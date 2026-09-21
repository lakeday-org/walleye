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
    common::ScalarValue,
    error::DataFusionError,
    execution::context::SessionContext,
    logical_expr::{
        ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
        Volatility,
    },
};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};
use walleye_typesafe::Question;

fn execution(error: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Execution(error.to_string())
}

/// The strings of one argument, however it arrived.
fn strings(value: &ColumnarValue, rows: usize, function: &str) -> DfResult<StringArray> {
    let array: ArrayRef = match value {
        ColumnarValue::Array(array) => array.clone(),
        ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(rows).map_err(execution)?,
    };
    crate::decisions::to_strings(&array, function)
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
        let answered = crate::decisions::wait(async {
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

/// Ask the decision service about each row.
///
/// `prompt_jev(state, question)` takes the service's own JSON. One question
/// answers with one struct:
///
/// ```sql
/// prompt_jev(body, '{"type":"choice","instructions":"...","criteria":{...}}')
///   -> {answer: 'damage', value: NULL, confidence: 0.74}
/// ```
///
/// A set of questions answers with one struct per question, named by the keys
/// the caller chose, and costs the same single call:
///
/// ```sql
/// prompt_jev(body, '{"topic":{"type":"choice",...},"heat":{"type":"score",...}}')
///   -> {topic: {...}, heat: {...}}
/// ```
///
/// Asking several things about a row at once is the cheap way to do it: the
/// service answers them in parallel and charges by the call, so a set of six
/// costs what one costs.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PromptJev {
    signature: Signature,
}

/// What one answer looks like, whichever kind of question it came from: a
/// label where there is one, a number where there is one, and how sure.
fn answer_fields() -> Fields {
    Fields::from(vec![
        Field::new("answer", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
        Field::new("confidence", DataType::Float64, true),
    ])
}

/// The questions a literal spec asks. A spec naming a `type` at the top is one
/// question; anything else is a set of them.
fn questions_of(json: &str) -> DfResult<(BTreeMap<String, Question>, bool)> {
    let parsed: Value = serde_json::from_str(json)
        .map_err(|e| execution(format!("prompt_jev needs a question as JSON: {e}")))?;
    if parsed.get("type").is_some() {
        let question: Question = serde_json::from_value(parsed)
            .map_err(|e| execution(format!("prompt_jev question is not a valid question: {e}")))?;
        question.validate().map_err(execution)?;
        return Ok(([(SOLE.to_owned(), question)].into(), true));
    }
    let set = walleye_typesafe::parse_spec(json).map_err(execution)?;
    for question in set.values() {
        question.validate().map_err(execution)?;
    }
    Ok((set, false))
}

/// The key a single question is filed under, never seen by a caller.
const SOLE: &str = "answer";

fn literal(scalar: Option<&ScalarValue>) -> DfResult<String> {
    match scalar {
        Some(ScalarValue::Utf8(Some(text)))
        | Some(ScalarValue::LargeUtf8(Some(text)))
        | Some(ScalarValue::Utf8View(Some(text))) => Ok(text.clone()),
        _ => Err(execution(
            "prompt_jev needs its question as a literal string, because the columns it \
             returns are named by the questions it asks",
        )),
    }
}

impl PromptJev {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Volatile),
        }
    }
    fn shape(questions: &BTreeMap<String, Question>, sole: bool) -> DataType {
        if sole {
            return DataType::Struct(answer_fields());
        }
        DataType::Struct(Fields::from(
            questions
                .keys()
                .map(|name| Field::new(name, DataType::Struct(answer_fields()), true))
                .collect::<Vec<_>>(),
        ))
    }
}

/// One question's answers, as the three columns they are reported in.
fn answered(answers: &[Option<walleye_typesafe::Answer>]) -> ArrayRef {
    let mut labels = StringBuilder::with_capacity(answers.len(), answers.len() * 16);
    let mut values = Float64Builder::with_capacity(answers.len());
    let mut sureness = Float64Builder::with_capacity(answers.len());
    for answer in answers {
        match answer {
            Some(answer) => {
                match answer.label() {
                    Some(label) => labels.append_value(label),
                    None => labels.append_null(),
                }
                match answer.value() {
                    Some(value) => values.append_value(value),
                    None => values.append_null(),
                }
                sureness.append_value(answer.confidence());
            }
            None => {
                labels.append_null();
                values.append_null();
                sureness.append_null();
            }
        }
    }
    Arc::new(StructArray::new(
        answer_fields(),
        vec![
            Arc::new(labels.finish()),
            Arc::new(values.finish()),
            Arc::new(sureness.finish()),
        ],
        None,
    ))
}

impl ScalarUDFImpl for PromptJev {
    fn name(&self) -> &str {
        "prompt_jev"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arguments: &[DataType]) -> DfResult<DataType> {
        Err(execution(
            "prompt_jev needs its question as a literal string",
        ))
    }
    fn return_field_from_args(&self, arguments: ReturnFieldArgs) -> DfResult<FieldRef> {
        let json = literal(arguments.scalar_arguments.get(1).copied().flatten())?;
        let (questions, sole) = questions_of(&json)?;
        Ok(Arc::new(Field::new(
            "prompt_jev",
            Self::shape(&questions, sole),
            true,
        )))
    }
    fn invoke_with_args(&self, arguments: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let rows = arguments.number_rows;
        let states = strings(&arguments.args[0], rows, "prompt_jev")?;
        let json = match &arguments.args[1] {
            ColumnarValue::Scalar(scalar) => literal(Some(scalar))?,
            ColumnarValue::Array(_) => {
                return Err(execution(
                    "prompt_jev needs one question for the whole query, not one per row",
                ));
            }
        };
        let (questions, sole) = questions_of(&json)?;
        let decided = crate::decisions::decisions_for(&states, &questions, "prompt_jev")?;
        let pick = |name: &str| -> Vec<Option<walleye_typesafe::Answer>> {
            decided
                .iter()
                .map(|decision| decision.as_ref().and_then(|d| d.answers.get(name).cloned()))
                .collect()
        };
        if sole {
            return Ok(ColumnarValue::Array(answered(&pick(SOLE))));
        }
        let columns: Vec<ArrayRef> = questions.keys().map(|name| answered(&pick(name))).collect();
        let fields = match Self::shape(&questions, false) {
            DataType::Struct(fields) => fields,
            _ => unreachable!("a set of questions is a struct"),
        };
        Ok(ColumnarValue::Array(Arc::new(StructArray::new(
            fields, columns, None,
        ))))
    }
}

/// Add both to a session.
pub fn register(context: &SessionContext) {
    context.register_udf(ScalarUDF::new_from_impl(Prompt::new()));
    context.register_udf(ScalarUDF::new_from_impl(PromptJev::new()));
}
