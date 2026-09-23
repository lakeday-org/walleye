//! Turning text into vectors: the embedding model, and the two SQL functions
//! that let a query search by meaning.
//!
//! The ingest path uses the model to give text columns a vector beside them.
//! A query uses the same model, through `embed`, to turn what it is looking
//! for into a vector it can compare against those, with `cosine_distance`.
//! Both have to be the same model, or the distances mean nothing, which is
//! why this lives in one place.
//!
//! Configured on its own, apart from the model that answers questions in SQL,
//! because the two are chosen for different things and billed separately:
//!
//! - `WALLEYE_EMBEDDING_KEY`: required. Without it nothing is embedded, and no
//!   table grows a vector column.
//! - `WALLEYE_EMBEDDING_URL`: an OpenAI-shaped embeddings endpoint. Defaults
//!   to OpenAI's own.
//! - `WALLEYE_EMBEDDING_MODEL`: defaults to `text-embedding-3-small`.
//! - `WALLEYE_EMBEDDING_TIMEOUT`: seconds to wait for one call, default 20.
//!
//! The request is the OpenAI one - `{"model": ..., "input": [...]}` answered
//! by `{"data": [{"index": i, "embedding": [...]}]}` - so any gateway,
//! proxy or self-hosted server that speaks it will do.
use std::time::Duration;

type Error = Box<dyn std::error::Error + Send + Sync>;

const DEFAULT_URL: &str = "https://api.openai.com/v1/embeddings";
const DEFAULT_MODEL: &str = "text-embedding-3-small";
/// How many texts go in one call. OpenAI accepts 2048; fewer keeps a single
/// request small enough that one slow call does not hold a large batch.
const PER_CALL: usize = 256;
/// Texts are cut to this many characters before they are sent. Embedding
/// models take around 8000 tokens; at four characters a token this stays
/// well inside that, and a field longer than this is rarely searched for the
/// part that was cut.
pub const MAX_CHARS: usize = 24_000;

/// A configured embedding model.
#[derive(Clone, Debug)]
pub struct Embedder {
    url: String,
    key: String,
    pub model: String,
    client: reqwest::Client,
}

impl Embedder {
    /// The embedder the environment configures, if it configures one. Read on
    /// each use rather than once, so that turning embedding on or off does not
    /// need a restart and a test can point it somewhere of its own.
    pub fn from_env() -> Option<Self> {
        let key = std::env::var("WALLEYE_EMBEDDING_KEY")
            .ok()
            .filter(|key| !key.trim().is_empty())?;
        let url = std::env::var("WALLEYE_EMBEDDING_URL")
            .ok()
            .filter(|url| !url.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_URL.to_owned());
        let model = std::env::var("WALLEYE_EMBEDDING_MODEL")
            .ok()
            .filter(|model| !model.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_MODEL.to_owned());
        let seconds = std::env::var("WALLEYE_EMBEDDING_TIMEOUT")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(20);
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(seconds))
            .build()
            .ok()?;
        Some(Self {
            url,
            key,
            model,
            client,
        })
    }

    /// One vector per text, in order. All or nothing: a batch that fails part
    /// way is reported as failed, so no row is written with a vector from a
    /// different call than its neighbours think.
    pub async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, Error> {
        let mut vectors = Vec::with_capacity(texts.len());
        for chunk in texts.chunks(PER_CALL) {
            let input: Vec<String> = chunk
                .iter()
                .map(|text| text.chars().take(MAX_CHARS).collect())
                .collect();
            let response = self
                .client
                .post(&self.url)
                .bearer_auth(&self.key)
                .json(&serde_json::json!({"model": self.model, "input": input}))
                .send()
                .await
                .map_err(|e| format!("the embedding model is unreachable: {e}"))?;
            let status = response.status();
            if !status.is_success() {
                let body = response.text().await.unwrap_or_default();
                return Err(format!(
                    "the embedding model refused with {status}: {}",
                    body.chars().take(300).collect::<String>()
                )
                .into());
            }
            #[derive(serde::Deserialize)]
            struct Item {
                index: usize,
                embedding: Vec<f32>,
            }
            #[derive(serde::Deserialize)]
            struct Answer {
                data: Vec<Item>,
            }
            let answer: Answer = response
                .json()
                .await
                .map_err(|e| format!("the embedding model answered in an unexpected shape: {e}"))?;
            if answer.data.len() != chunk.len() {
                return Err(format!(
                    "asked for {} embeddings and got {}",
                    chunk.len(),
                    answer.data.len()
                )
                .into());
            }
            let mut ordered: Vec<Option<Vec<f32>>> = vec![None; chunk.len()];
            for item in answer.data {
                if let Some(slot) = ordered.get_mut(item.index) {
                    *slot = Some(item.embedding);
                }
            }
            for vector in ordered {
                vectors.push(vector.ok_or("the embedding model skipped a text")?);
            }
        }
        Ok(vectors)
    }

    /// How long this model's vectors are, learned by asking it once. A vector
    /// column's width is fixed when the table is made, so it has to be known
    /// before the first row is.
    pub async fn dimensions(&self) -> Result<usize, Error> {
        let probe = self.embed(&["dimension probe".to_owned()]).await?;
        let width = probe.first().map(Vec::len).unwrap_or(0);
        if width == 0 {
            return Err("the embedding model returned an empty vector".into());
        }
        Ok(width)
    }
}

// --- in SQL ------------------------------------------------------------------

use lance::deps::datafusion::{
    arrow::{
        array::{
            Array, ArrayRef, Float32Builder, Float64Array, ListArray, ListBuilder, StringArray,
        },
        datatypes::{DataType, Field},
    },
    common::Result as DfResult,
    error::DataFusionError,
    execution::context::SessionContext,
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    },
};
use std::sync::{Arc, Mutex, OnceLock};

fn execution(message: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Execution(message.to_string())
}

fn vector_type() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::Float32, true)))
}

/// Vectors already fetched, by model and text, so a query that embeds the
/// same phrase in every batch - or twice - pays once. Bounded; past the
/// limit it stops admitting rather than grow.
/// Vectors by the model that made them and the text they were made from.
type Remembered = Mutex<std::collections::HashMap<(String, String), Vec<f32>>>;

fn remembered() -> &'static Remembered {
    static CACHE: OnceLock<Remembered> = OnceLock::new();
    CACHE.get_or_init(Default::default)
}

/// `embed(text)`: the vector the configured embedding model gives a text.
///
/// The same model the ingest path used, so the vector can be compared with
/// the `_embedding` columns beside ingested text. Write
/// `ORDER BY cosine_distance(review_embedding, embed('arrived damaged'))` to
/// find the reviews that mean that, whatever words they used.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Embed {
    signature: Signature,
}

impl ScalarUDFImpl for Embed {
    fn name(&self) -> &str {
        "embed"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arguments: &[DataType]) -> DfResult<DataType> {
        Ok(vector_type())
    }
    fn invoke_with_args(&self, arguments: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let rows = arguments.number_rows;
        let texts = match &arguments.args[0] {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(rows)?,
        };
        let texts = arrow_cast::cast(&texts, &DataType::Utf8)
            .map_err(|e| execution(format!("embed needs text: {e}")))?;
        let texts = texts
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("cast to Utf8");
        let Some(model) = Embedder::from_env() else {
            return Err(execution(
                "embed needs an embedding model: set WALLEYE_EMBEDDING_KEY",
            ));
        };
        // Only the texts nobody has asked for yet go to the model, once each.
        let mut wanted: Vec<String> = Vec::new();
        {
            let cache = remembered().lock().unwrap_or_else(|p| p.into_inner());
            for index in 0..texts.len() {
                if texts.is_null(index) {
                    continue;
                }
                let text = texts.value(index);
                if !cache.contains_key(&(model.model.clone(), text.to_owned()))
                    && !wanted.iter().any(|w| w == text)
                {
                    wanted.push(text.to_owned());
                }
            }
        }
        // This query's vectors, whether or not the shared cache has room for
        // them.
        let mut fetched: std::collections::HashMap<String, Vec<f32>> = Default::default();
        if !wanted.is_empty() {
            let vectors = crate::decisions::wait(model.embed(&wanted))
                .map_err(|e| execution(format!("embed: {e}")))?;
            let mut cache = remembered().lock().unwrap_or_else(|p| p.into_inner());
            for (text, vector) in wanted.into_iter().zip(vectors) {
                if cache.len() < 4096 {
                    cache.insert((model.model.clone(), text.clone()), vector.clone());
                }
                fetched.insert(text, vector);
            }
        }
        let cache = remembered().lock().unwrap_or_else(|p| p.into_inner());
        let mut built = ListBuilder::new(Float32Builder::new()).with_field(Arc::new(Field::new(
            "item",
            DataType::Float32,
            true,
        )));
        for index in 0..texts.len() {
            let vector = (!texts.is_null(index))
                .then(|| texts.value(index))
                .and_then(|text| {
                    fetched
                        .get(text)
                        .or_else(|| cache.get(&(model.model.clone(), text.to_owned())))
                });
            match vector {
                Some(vector) => {
                    built.values().append_slice(vector);
                    built.append(true);
                }
                None => built.append(false),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(built.finish())))
    }
}

/// `cosine_distance(a, b)`: how far apart two vectors point, from 0 for the
/// same direction to 2 for opposite ones. Takes vector columns and the
/// output of `embed` alike. Null where either side is.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CosineDistance {
    signature: Signature,
}

/// A vector argument as a list of doubles, one per row.
fn as_lists(value: &ColumnarValue, rows: usize) -> DfResult<ListArray> {
    let array: ArrayRef = match value {
        ColumnarValue::Array(array) => array.clone(),
        ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(rows)?,
    };
    let target = DataType::List(Arc::new(Field::new("item", DataType::Float64, true)));
    let cast = arrow_cast::cast(&array, &target)
        .map_err(|e| execution(format!("cosine_distance needs two vectors: {e}")))?;
    Ok(cast
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("cast to a list")
        .clone())
}

impl ScalarUDFImpl for CosineDistance {
    fn name(&self) -> &str {
        "cosine_distance"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arguments: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Float64)
    }
    fn invoke_with_args(&self, arguments: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let rows = arguments.number_rows;
        let left = as_lists(&arguments.args[0], rows)?;
        let right = as_lists(&arguments.args[1], rows)?;
        let mut distances = Vec::with_capacity(rows);
        for index in 0..rows {
            if left.is_null(index) || right.is_null(index) {
                distances.push(None);
                continue;
            }
            let a = left.value(index);
            let b = right.value(index);
            let a = a.as_any().downcast_ref::<Float64Array>().expect("doubles");
            let b = b.as_any().downcast_ref::<Float64Array>().expect("doubles");
            if a.len() != b.len() {
                return Err(execution(format!(
                    "cosine_distance: vectors of {} and {} values cannot be compared; were they \
                     made by different models?",
                    a.len(),
                    b.len()
                )));
            }
            let (mut dot, mut na, mut nb) = (0.0, 0.0, 0.0);
            for i in 0..a.len() {
                let (x, y) = (a.value(i), b.value(i));
                dot += x * y;
                na += x * x;
                nb += y * y;
            }
            distances.push((na > 0.0 && nb > 0.0).then(|| 1.0 - dot / (na.sqrt() * nb.sqrt())));
        }
        Ok(ColumnarValue::Array(Arc::new(Float64Array::from(
            distances,
        ))))
    }
}

/// Add both to a session.
pub fn register(context: &SessionContext) {
    context.register_udf(ScalarUDF::new_from_impl(Embed {
        // Volatile: a call that leaves the process must not be folded into
        // the plan or run more often than the query asks.
        signature: Signature::any(1, Volatility::Volatile),
    }));
    context.register_udf(ScalarUDF::new_from_impl(CosineDistance {
        signature: Signature::any(2, Volatility::Immutable),
    }));
}
