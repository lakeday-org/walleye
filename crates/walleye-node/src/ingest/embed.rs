//! Turning text into vectors, for the columns worth searching by meaning.
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

/// Whether a text column looks like prose rather than a code, a name or a
/// label: long, and made of words. Only prose is worth a judge's time or an
/// embedding's cost.
pub fn prose(values: &[&str]) -> Prose {
    let seen: Vec<&str> = values
        .iter()
        .copied()
        .filter(|v| !v.trim().is_empty())
        .collect();
    if seen.is_empty() {
        return Prose::No;
    }
    let chars = seen.iter().map(|v| v.chars().count()).sum::<usize>() / seen.len();
    let words = seen
        .iter()
        .map(|v| v.split_whitespace().count())
        .sum::<usize>()
        / seen.len();
    let distinct = seen.iter().collect::<std::collections::BTreeSet<_>>().len();
    if distinct < 2 && seen.len() > 1 {
        return Prose::No;
    }
    if chars >= 60 && words >= 8 {
        Prose::Clearly
    } else if chars >= 24 && words >= 3 {
        Prose::Maybe
    } else {
        Prose::No
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Prose {
    /// Not free text: never embedded.
    No,
    /// Could be; a judge decides, and without one it is not embedded.
    Maybe,
    /// Long sentences. Embedded even without a judge to ask.
    Clearly,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_names_and_labels_are_not_prose() {
        assert_eq!(prose(&["SKU-1234", "SKU-9"]), Prose::No);
        assert_eq!(prose(&["Ada Lovelace", "Grace Hopper"]), Prose::No);
        assert_eq!(prose(&["pending", "shipped", "pending"]), Prose::No);
    }

    #[test]
    fn a_sentence_might_be_and_a_paragraph_is() {
        assert_eq!(
            prose(&["arrived late, box crushed", "fast shipping, happy overall"]),
            Prose::Maybe
        );
        assert_eq!(
            prose(&[
                "The package arrived three days late and the box was crushed on one side.",
                "Great product, fast shipping, and support answered my question within an hour."
            ]),
            Prose::Clearly
        );
    }

    #[test]
    fn the_same_sentence_every_time_is_a_label() {
        let same = "this is the default description text for every item";
        assert_eq!(prose(&[same, same, same]), Prose::No);
    }
}
