//! Typed decisions as SQL functions.
//!
//! The decision service answers every question about one row in a single
//! call, but each row is its own call. So `classify` over a large table at
//! read time is the expensive way to use it, and classifying a row once where
//! it is written, then storing the answer, is the cheap way. These functions
//! exist to make the second shape expressible: a materialising query reads
//! only the rows that are new, and the answers it computes are written down.
//!
//! Every function takes the row's text first, then the instructions, then the
//! taxonomy. Asking the same question of the same text twice inside one query
//! costs one call, because identical questions about identical state share an
//! answer.
use arrow_array::{
    Array, ArrayRef, Float64Array, StringArray, StructArray, builder::StringBuilder,
};
use arrow_schema::{DataType, Field, FieldRef, Fields};
use lance::deps::datafusion::{
    common::{DataFusionError, Result as DfResult, ScalarValue},
    logical_expr::{
        ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
        Volatility,
    },
    prelude::SessionContext,
};
use std::{collections::BTreeMap, sync::Arc, sync::OnceLock};
use walleye_typesafe::{Answer, Client, Question, parse_criteria, parse_levels, parse_spec};

/// The question key used for every single-question call. The service keys
/// answers by the name the caller chose, and these functions ask one thing.
const KEY: &str = "q";

/// One process-wide client, so its concurrency limit and its answer cache are
/// shared by every query rather than rebuilt per statement.
fn client() -> Option<&'static Arc<Client>> {
    static CLIENT: OnceLock<Option<Arc<Client>>> = OnceLock::new();
    CLIENT
        .get_or_init(|| Client::from_env().map(Arc::new))
        .as_ref()
}

/// Run an asynchronous call from inside a synchronous function evaluation.
///
/// On a multi-threaded runtime this hands the worker's other tasks to a
/// sibling thread first, which is what makes blocking here safe. Anywhere
/// else it borrows a dedicated runtime, because blocking the only thread of a
/// single-threaded runtime would deadlock against the very work being waited
/// on.
fn wait<F: std::future::Future>(future: F) -> F::Output {
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

/// Read an argument that must be one text value for the whole batch, which is
/// how instructions and a taxonomy are written: as literals.
fn constant(value: &ColumnarValue, name: &str, function: &str) -> DfResult<String> {
    match value {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(text)))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(text)))
        | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(text))) => Ok(text.clone()),
        ColumnarValue::Array(array) if array.len() == 1 => {
            let array = to_strings(array, function)?;
            Ok(array.value(0).to_owned())
        }
        _ => Err(execution(format!(
            "{function} needs a constant {name}; give it as a literal string"
        ))),
    }
}
fn to_strings(array: &dyn Array, function: &str) -> DfResult<StringArray> {
    arrow_cast::cast(array, &DataType::Utf8)
        .map_err(|error| execution(format!("{function} needs text: {error}")))
        .map(|cast| {
            cast.as_any()
                .downcast_ref::<StringArray>()
                .expect("cast to Utf8")
                .clone()
        })
}

/// Ask one question of every distinct non-null row in the batch.
///
/// Distinct is what makes a batch affordable: a column of a thousand rows
/// drawn from twenty phrasings costs twenty calls, not a thousand. A row the
/// service could not answer yields null rather than failing the query, so one
/// unanswerable row does not discard the batch.
fn answers_for(
    states: &StringArray,
    question: Question,
    function: &str,
) -> DfResult<Vec<Option<Answer>>> {
    question.validate().map_err(execution)?;
    let questions: BTreeMap<String, Question> = [(KEY.to_owned(), question)].into();
    Ok(decisions_for(states, &questions, function)?
        .into_iter()
        .map(|decision| decision.and_then(|d| d.answers.get(KEY).cloned()))
        .collect())
}

/// Ask a whole question set of every distinct non-null row.
///
/// One call carries every question, which is the difference that matters:
/// the service evaluates them in parallel, so a row asked five things costs
/// about what a row asked one thing costs. Five separate function calls
/// would cost five times as much.
fn decisions_for(
    states: &StringArray,
    questions: &BTreeMap<String, Question>,
    function: &str,
) -> DfResult<Vec<Option<std::sync::Arc<walleye_typesafe::Decision>>>> {
    let Some(client) = client() else {
        return Err(execution(format!(
            "{function} needs a decision service: set WALLEYE_TYPESAFE_API_KEY"
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

/// How a function turns one answer into one output value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Reads {
    /// The chosen label.
    Label,
    /// How sure the service is, from 0 to 1.
    Confidence,
    /// The answer's number: a weighted score, or the probability a
    /// proposition holds.
    Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Decide {
    name: &'static str,
    kind: Kind,
    reads: Reads,
    signature: Signature,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Kind {
    Choice,
    Score,
    Noul,
}
impl Decide {
    fn new(name: &'static str, kind: Kind, reads: Reads) -> Self {
        let arguments = match kind {
            Kind::Choice | Kind::Score => 3,
            Kind::Noul => 2,
        };
        Self {
            name,
            kind,
            reads,
            // Volatile keeps the planner from folding or reordering a call
            // that leaves the process and costs money.
            signature: Signature::any(arguments, Volatility::Volatile),
        }
    }
}
impl ScalarUDFImpl for Decide {
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arguments: &[DataType]) -> DfResult<DataType> {
        Ok(match self.reads {
            Reads::Label => DataType::Utf8,
            Reads::Confidence | Reads::Value => DataType::Float64,
        })
    }
    fn invoke_with_args(&self, arguments: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let rows = arguments.number_rows;
        let states = match &arguments.args[0] {
            ColumnarValue::Array(array) => to_strings(array, self.name)?,
            ColumnarValue::Scalar(scalar) => {
                let array = scalar.to_array_of_size(rows).map_err(execution)?;
                to_strings(&array, self.name)?
            }
        };
        let instructions = constant(&arguments.args[1], "instructions", self.name)?;
        let question = match self.kind {
            Kind::Choice => {
                let spec = constant(&arguments.args[2], "criteria", self.name)?;
                Question::choice(instructions, parse_criteria(&spec).map_err(execution)?)
            }
            Kind::Score => {
                let spec = constant(&arguments.args[2], "levels", self.name)?;
                Question::score(instructions, parse_levels(&spec))
            }
            Kind::Noul => Question::noul(instructions),
        };
        let answers = answers_for(&states, question, self.name)?;
        Ok(match self.reads {
            Reads::Label => {
                let mut built = StringBuilder::with_capacity(answers.len(), answers.len() * 16);
                for answer in &answers {
                    match answer.as_ref().and_then(Answer::label) {
                        Some(label) => built.append_value(label),
                        None => built.append_null(),
                    }
                }
                ColumnarValue::Array(Arc::new(built.finish()))
            }
            Reads::Confidence => ColumnarValue::Array(Arc::new(Float64Array::from(
                answers
                    .iter()
                    .map(|answer| answer.as_ref().map(Answer::confidence))
                    .collect::<Vec<_>>(),
            ))),
            Reads::Value => ColumnarValue::Array(Arc::new(Float64Array::from(
                answers
                    .iter()
                    .map(|answer| answer.as_ref().and_then(Answer::value))
                    .collect::<Vec<_>>(),
            ))),
        })
    }
}

/// One answer rendered as three columns, so every question kind reads the
/// same way regardless of what it asked.
fn answer_fields() -> Fields {
    Fields::from(vec![
        Field::new("label", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
        Field::new("confidence", DataType::Float64, true),
    ])
}

/// Read the question set from the second argument, which must be a literal so
/// the shape of the result is known while the query is still being planned.
fn spec_of(scalar: Option<&ScalarValue>) -> DfResult<BTreeMap<String, Question>> {
    let json = match scalar {
        Some(ScalarValue::Utf8(Some(text)))
        | Some(ScalarValue::LargeUtf8(Some(text)))
        | Some(ScalarValue::Utf8View(Some(text))) => text.clone(),
        _ => {
            return Err(execution(
                "decide needs its question set as a literal string, because the \
                 columns it returns are named by the questions it asks",
            ));
        }
    };
    parse_spec(&json).map_err(execution)
}

/// Ask a whole set of questions about a row in one call.
///
/// The set is written in the same shape the service takes, and each question
/// becomes a field of the returned struct:
///
/// ```sql
/// SELECT decide(body, '{"team":{"type":"choice","instructions":"Which team?",
///                                "criteria":{"returns":"...","billing":"..."}},
///                       "refund":{"type":"noul","instructions":"Wants money back"}}')
/// ```
///
/// This is the form to prefer whenever a row is asked more than one thing.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DecideAll {
    signature: Signature,
}
impl DecideAll {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Volatile),
        }
    }
    fn struct_type(questions: &BTreeMap<String, Question>) -> DataType {
        DataType::Struct(Fields::from(
            questions
                .keys()
                .map(|name| Field::new(name, DataType::Struct(answer_fields()), true))
                .collect::<Vec<_>>(),
        ))
    }
}
impl ScalarUDFImpl for DecideAll {
    fn name(&self) -> &str {
        "decide"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arguments: &[DataType]) -> DfResult<DataType> {
        Err(execution(
            "decide needs its question set as a literal string",
        ))
    }
    fn return_field_from_args(&self, arguments: ReturnFieldArgs) -> DfResult<FieldRef> {
        let questions = spec_of(arguments.scalar_arguments.get(1).copied().flatten())?;
        Ok(std::sync::Arc::new(Field::new(
            "decide",
            Self::struct_type(&questions),
            true,
        )))
    }
    fn invoke_with_args(&self, arguments: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let rows = arguments.number_rows;
        let states = match &arguments.args[0] {
            ColumnarValue::Array(array) => to_strings(array, "decide")?,
            ColumnarValue::Scalar(scalar) => {
                let array = scalar.to_array_of_size(rows).map_err(execution)?;
                to_strings(&array, "decide")?
            }
        };
        let json = constant(&arguments.args[1], "question set", "decide")?;
        let questions = parse_spec(&json).map_err(execution)?;
        let decided = decisions_for(&states, &questions, "decide")?;

        let mut columns: Vec<(FieldRef, ArrayRef)> = Vec::with_capacity(questions.len());
        for name in questions.keys() {
            let answers: Vec<Option<Answer>> = decided
                .iter()
                .map(|decision| {
                    decision
                        .as_ref()
                        .and_then(|decision| decision.answers.get(name).cloned())
                })
                .collect();
            let mut labels = StringBuilder::with_capacity(answers.len(), answers.len() * 16);
            for answer in &answers {
                match answer.as_ref().and_then(Answer::label) {
                    Some(label) => labels.append_value(label),
                    None => labels.append_null(),
                }
            }
            let inner: Vec<(FieldRef, ArrayRef)> = vec![
                (
                    std::sync::Arc::new(Field::new("label", DataType::Utf8, true)),
                    std::sync::Arc::new(labels.finish()) as ArrayRef,
                ),
                (
                    std::sync::Arc::new(Field::new("value", DataType::Float64, true)),
                    std::sync::Arc::new(Float64Array::from(
                        answers
                            .iter()
                            .map(|answer| answer.as_ref().and_then(Answer::value))
                            .collect::<Vec<_>>(),
                    )) as ArrayRef,
                ),
                (
                    std::sync::Arc::new(Field::new("confidence", DataType::Float64, true)),
                    std::sync::Arc::new(Float64Array::from(
                        answers
                            .iter()
                            .map(|answer| answer.as_ref().map(Answer::confidence))
                            .collect::<Vec<_>>(),
                    )) as ArrayRef,
                ),
            ];
            columns.push((
                std::sync::Arc::new(Field::new(name, DataType::Struct(answer_fields()), true)),
                std::sync::Arc::new(StructArray::from(inner)) as ArrayRef,
            ));
        }
        Ok(ColumnarValue::Array(std::sync::Arc::new(
            StructArray::from(columns),
        )))
    }
}

/// Add the decision functions to a session.
///
/// They are registered whether or not a key is configured, so a query that
/// uses one without a key fails saying that, rather than failing as though
/// the function did not exist.
pub fn register(context: &SessionContext) {
    for function in [
        Decide::new("classify", Kind::Choice, Reads::Label),
        Decide::new("classify_confidence", Kind::Choice, Reads::Confidence),
        Decide::new("rate", Kind::Score, Reads::Value),
        Decide::new("rate_label", Kind::Score, Reads::Label),
        Decide::new("rate_confidence", Kind::Score, Reads::Confidence),
        Decide::new("holds", Kind::Noul, Reads::Value),
        Decide::new("holds_confidence", Kind::Noul, Reads::Confidence),
    ] {
        context.register_udf(ScalarUDF::new_from_impl(function));
    }
    context.register_udf(ScalarUDF::new_from_impl(DecideAll::new()));
}

/// Whether a decision service is configured, which decides whether the
/// classification functions can answer.
pub fn available() -> bool {
    client().is_some()
}
