//! Calling the decision service from inside a query.
//!
//! The functions themselves live in [`crate::prompt`]; this is what they
//! share: one client for the process, a runtime to wait on it from a
//! synchronous plan, and the batching that turns a column of rows into as few
//! calls as its distinct values allow.
//!
//! The service answers a whole question set in one call, but each row is its
//! own call. So deciding at read time over a large table is the expensive
//! shape, and deciding once where a row is written, then storing the answer,
//! is the cheap one. A materialising query reads only the rows that are new,
//! and writes down what it worked out.
use lance::deps::datafusion::{
    arrow::{
        array::{Array, StringArray},
        datatypes::DataType,
    },
    common::{DataFusionError, Result as DfResult},
};
use std::{collections::BTreeMap, sync::Arc, sync::OnceLock};
use walleye_typesafe::{Client, Question};

/// One process-wide client, so its concurrency limit and its answer cache are
/// shared by every query rather than rebuilt per statement.
pub(crate) fn client() -> Option<&'static Arc<Client>> {
    static CLIENT: OnceLock<Option<Arc<Client>>> = OnceLock::new();
    CLIENT
        .get_or_init(|| Client::from_env().map(Arc::new))
        .as_ref()
}

pub(crate) fn wait<F: std::future::Future>(future: F) -> F::Output {
    use tokio::runtime::{Handle, RuntimeFlavor};
    match Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| handle.block_on(future))
        }
        _ => aside().block_on(future),
    }
}

fn aside() -> &'static tokio::runtime::Runtime {
    static ASIDE: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    ASIDE.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("decision runtime")
    })
}

fn execution(message: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Execution(message.to_string())
}

pub(crate) fn to_strings(array: &dyn Array, function: &str) -> DfResult<StringArray> {
    arrow_cast::cast(array, &DataType::Utf8)
        .map_err(|error| execution(format!("{function} needs text: {error}")))
        .map(|cast| {
            cast.as_any()
                .downcast_ref::<StringArray>()
                .expect("cast to Utf8")
                .clone()
        })
}

/// Ask a whole question set of every distinct non-null row.
///
/// One call carries every question, which is the difference that matters:
/// the service evaluates them in parallel, so a row asked five things costs
/// about what a row asked one thing costs. Five separate function calls
/// would cost five times as much.
pub(crate) fn decisions_for(
    states: &StringArray,
    questions: &BTreeMap<String, Question>,
    function: &str,
) -> DfResult<Vec<Option<std::sync::Arc<walleye_typesafe::Decision>>>> {
    let Some(client) = client() else {
        return Err(execution(format!(
            "{function} needs a decision service: set TYPESAFE_API_KEY"
        )));
    };
    let mut distinct: Vec<String> = Vec::new();
    let mut seen: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    let mut slot: Vec<Option<usize>> = Vec::with_capacity(states.len());
    for index in 0..states.len() {
        if states.is_null(index) {
            slot.push(None);
            continue;
        }
        let state = states.value(index);
        let at = *seen.entry(state).or_insert_with(|| {
            distinct.push(state.to_owned());
            distinct.len() - 1
        });
        slot.push(Some(at));
    }

    let decided = wait(client.ask_many(&distinct, questions));
    let mut resolved: Vec<Option<std::sync::Arc<walleye_typesafe::Decision>>> =
        Vec::with_capacity(decided.len());
    let mut refusal: Option<String> = None;
    for outcome in decided {
        match outcome {
            Ok(decision) => resolved.push(Some(decision)),
            Err(error) => {
                // A rejected question is the caller's mistake and every row
                // will repeat it, so report it rather than return a column of
                // nulls. A busy service is not the caller's mistake.
                if matches!(error, walleye_typesafe::Error::Rejected { .. }) {
                    refusal.get_or_insert_with(|| error.to_string());
                }
                resolved.push(None);
            }
        }
    }
    if let Some(refusal) = refusal {
        return Err(execution(format!("{function}: {refusal}")));
    }
    Ok(slot
        .into_iter()
        .map(|at| at.and_then(|at| resolved[at].clone()))
        .collect())
}
