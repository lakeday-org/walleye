//! `prompt` and `prompt_jev`: a model and a decision service, as SQL
//! functions.
//!
//! Both take a row's text and return a column. They differ in what they are
//! for. `prompt` asks a language model in words and gets words back, or JSON
//! when given a schema: open-ended work, a sentence per row, priced and paced
//! like a language model. `prompt_jev` asks the decision service a typed
//! question -- one option from a set, a level on a scale, a yes or no -- and
//! gets back the answer with how sure it was.
//!
//! The difference matters at the scale a query runs at. A choice over five
//! options is not a paragraph, and asking for it as one costs a hundred times
//! more and cannot say how sure it is.
use crate::model::Model;
use lance::deps::datafusion::{
    arrow::{
        array::{Array, ArrayRef, Float64Builder, StringArray, StringBuilder, StructArray},
        datatypes::{DataType, Field, FieldRef, Fields},
    },
    common::Result as DfResult,
    error::DataFusionError,
    execution::context::SessionContext,
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    },
};
use serde_json::Value;
use std::sync::Arc;

fn execution(error: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Execution(error.to_string())
}

/// The strings of one argument, however it arrived.
fn strings(value: &ColumnarValue, rows: usize, function: &str) -> DfResult<StringArray> {
    let array: ArrayRef = match value {
        ColumnarValue::Array(array) => array.clone(),
        ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(rows).map_err(execution)?,
    };
    array
        .as_any()
        .downcast_ref::<StringArray>()
        .cloned()
        .ok_or_else(|| execution(format!("{function} takes text")))
}

/// An argument that must be the same for every row, because it configures the
/// call rather than describing a row.
fn constant(value: &ColumnarValue, what: &str, function: &str) -> DfResult<String> {
    match value {
        ColumnarValue::Scalar(scalar) => Ok(scalar.to_string().trim_matches('"').to_owned()),
        ColumnarValue::Array(_) => Err(execution(format!(
            "{function} needs one {what} for the whole query, not one per row"
        ))),
    }
}

/// Ask a language model, once per distinct prompt.
///
/// `prompt(text)` answers in prose. `prompt(text, options)` takes a JSON
/// object: `model` and `effort` choose how it is answered, and `schema` makes
/// the answer structured JSON rather than prose.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Prompt {
    signature: Signature,
}
impl Prompt {
    fn new() -> Self {
        Self {
            // Volatile: the planner must not fold, reorder or duplicate a call
            // that leaves the process and costs money.
            signature: Signature::one_of(
                vec![
                    lance::deps::datafusion::logical_expr::TypeSignature::Any(1),
                    lance::deps::datafusion::logical_expr::TypeSignature::Any(2),
                ],
                Volatility::Volatile,
            ),
        }
    }
}
impl ScalarUDFImpl for Prompt {
    fn name(&self) -> &str {
        "prompt"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arguments: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, arguments: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let rows = arguments.number_rows;
        let prompts = strings(&arguments.args[0], rows, "prompt")?;
        let options: Value = match arguments.args.get(1) {
            Some(argument) => {
                let raw = constant(argument, "set of options", "prompt")?;
                serde_json::from_str(&raw)
                    .map_err(|e| execution(format!("prompt options must be a JSON object: {e}")))?
            }
            None => Value::Null,
        };
        let Some(model) = Model::from_env() else {
            return Err(execution("prompt needs a model: set WALLEYE_MODEL_KEY"));
        };
        let model = model.with(
            options.get("model").and_then(Value::as_str),
            options.get("effort").and_then(Value::as_str),
        );
        let shape = options.get("schema").cloned();

        // The same prompt on many rows is one call. A column of a hundred
        // rows holding three distinct prompts costs three.
        let mut distinct: Vec<String> = Vec::new();
        let mut seen: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
        let mut slot: Vec<Option<usize>> = Vec::with_capacity(rows);
        for index in 0..prompts.len() {
            if prompts.is_null(index) {
                slot.push(None);
                continue;
            }
            let text = prompts.value(index);
            let at = *seen.entry(text).or_insert_with(|| {
                distinct.push(text.to_owned());
                distinct.len() - 1
            });
            slot.push(Some(at));
        }
        let answered = crate::classify::wait(async {
            use futures::stream::StreamExt;
            futures::stream::iter(
                distinct
                    .iter()
                    .map(|text| model.complete(text, shape.as_ref())),
            )
            .buffered(8)
            .collect::<Vec<_>>()
            .await
        });
        let mut failure = None;
        let resolved: Vec<Option<String>> = answered
            .into_iter()
            .map(|outcome| match outcome {
                Ok(text) => Some(text),
                Err(error) => {
                    failure.get_or_insert_with(|| error.to_string());
                    None
                }
            })
            .collect();
        if let Some(reason) = failure
            && resolved.iter().all(Option::is_none)
        {
            return Err(execution(format!("prompt failed: {reason}")));
        }
        let mut built = StringBuilder::with_capacity(rows, rows * 32);
        for at in slot {
            match at.and_then(|at| resolved.get(at).cloned().flatten()) {
                Some(text) => built.append_value(text),
                None => built.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(built.finish())))
    }
}

/// Ask the decision service a typed question about each row.
///
/// `prompt_jev(state, question)` where `question` is the same JSON the service
/// takes: `{"type": "choice", "instructions": ..., "criteria": {...}}`, or
/// `score` with a list of levels, or `noul` with none. It answers with the
/// chosen option and how sure the service was, so a caller can act on one and
/// gate on the other.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PromptJev {
    signature: Signature,
}
impl PromptJev {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Volatile),
        }
    }
    fn fields() -> Fields {
        Fields::from(vec![
            Field::new("answer", DataType::Utf8, true),
            Field::new("confidence", DataType::Float64, true),
        ])
    }
}
impl ScalarUDFImpl for PromptJev {
    fn name(&self) -> &str {
        "prompt_jev"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arguments: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Struct(Self::fields()))
    }
    fn invoke_with_args(&self, arguments: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let rows = arguments.number_rows;
        let states = strings(&arguments.args[0], rows, "prompt_jev")?;
        let raw = constant(&arguments.args[1], "question", "prompt_jev")?;
        let question: walleye_typesafe::Question = serde_json::from_str(&raw)
            .map_err(|e| execution(format!("prompt_jev question is not a valid question: {e}")))?;
        let answers = crate::classify::answers_for(&states, question, "prompt_jev")?;

        let mut labels = StringBuilder::with_capacity(answers.len(), answers.len() * 16);
        let mut sureness = Float64Builder::with_capacity(answers.len());
        for answer in &answers {
            match answer {
                Some(answer) => {
                    match answer.label() {
                        Some(label) => labels.append_value(label),
                        // A yes-or-no has a probability and no label; it is
                        // reported as the number it is.
                        None => labels.append_null(),
                    }
                    match answer.value() {
                        Some(value) => sureness.append_value(value),
                        None => sureness.append_value(answer.confidence()),
                    }
                }
                None => {
                    labels.append_null();
                    sureness.append_null();
                }
            }
        }
        let columns: Vec<ArrayRef> = vec![Arc::new(labels.finish()), Arc::new(sureness.finish())];
        Ok(ColumnarValue::Array(Arc::new(StructArray::new(
            Self::fields(),
            columns,
            None,
        ))))
    }
    fn return_field_from_args(
        &self,
        _arguments: lance::deps::datafusion::logical_expr::ReturnFieldArgs,
    ) -> DfResult<FieldRef> {
        Ok(Arc::new(Field::new(
            "prompt_jev",
            DataType::Struct(Self::fields()),
            true,
        )))
    }
}

/// Add both to a session.
pub fn register(context: &SessionContext) {
    context.register_udf(ScalarUDF::new_from_impl(Prompt::new()));
    context.register_udf(ScalarUDF::new_from_impl(PromptJev::new()));
}
