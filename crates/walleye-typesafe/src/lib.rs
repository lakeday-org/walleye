//! A client for the System One decision API, which answers typed questions
//! about a piece of state and reports how sure it is.
//!
//! The service answers many questions about one state in a single call and
//! evaluates them in parallel, so asking more questions about a row costs
//! roughly what asking one costs. Classifying more rows does not: each row is
//! its own call. That asymmetry is why classification belongs where a row is
//! first written rather than where it is read, and why this client is built
//! around bounded concurrency over rows and a cache that makes a repeated
//! question about unchanged state free.
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    hash::{Hash, Hasher},
    sync::{Arc, Mutex},
    time::Duration,
};

pub const DEFAULT_ENDPOINT: &str = "https://api.typesafe.ai/v1/systemone";
pub const DEFAULT_MODEL: &str = "jev-latest";
/// Concurrent requests one client will have in flight. The service answers in
/// about 150ms, so this is the difference between classifying a few rows a
/// second and a few hundred.
pub const DEFAULT_CONCURRENCY: usize = 16;
const MAX_ATTEMPTS: u32 = 5;

#[derive(Debug)]
pub enum Error {
    /// No API key was configured, so no question can be asked.
    NoCredentials,
    /// The service rejected the request. Retrying it unchanged will not help.
    Rejected { status: u16, message: String },
    /// The service was busy or unreachable after every retry.
    Unavailable(String),
    /// The service answered with something this client cannot read.
    Malformed(String),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoCredentials => write!(
                f,
                "no System One API key: set WALLEYE_TYPESAFE_API_KEY or TYPESAFE_API_KEY"
            ),
            Self::Rejected { status, message } => {
                write!(f, "System One rejected the request ({status}): {message}")
            }
            Self::Unavailable(detail) => write!(f, "System One unavailable: {detail}"),
            Self::Malformed(detail) => write!(f, "System One answered unreadably: {detail}"),
        }
    }
}
impl std::error::Error for Error {}

/// One typed question. The service answers every question in a request at
/// once, so a caller should ask everything it might want about a row here
/// rather than in a second call.
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Question {
    /// Pick one option. `criteria` maps each option to what it means.
    Choice {
        instructions: String,
        criteria: BTreeMap<String, String>,
    },
    /// Rate against ordered levels, lowest first.
    Score {
        instructions: String,
        criteria: Vec<String>,
    },
    /// Judge a yes or no proposition.
    Noul { instructions: String },
}
impl Question {
    pub fn choice(
        instructions: impl Into<String>,
        criteria: impl IntoIterator<Item = (String, String)>,
    ) -> Self {
        Self::Choice {
            instructions: instructions.into(),
            criteria: criteria.into_iter().collect(),
        }
    }
    pub fn score(
        instructions: impl Into<String>,
        levels: impl IntoIterator<Item = String>,
    ) -> Self {
        Self::Score {
            instructions: instructions.into(),
            criteria: levels.into_iter().collect(),
        }
    }
    pub fn noul(instructions: impl Into<String>) -> Self {
        Self::Noul {
            instructions: instructions.into(),
        }
    }
    /// Reject a question the service would reject, before paying for a round
    /// trip. A choice needs something to choose between; a score needs levels
    /// to rank against.
    pub fn validate(&self) -> Result<(), Error> {
        let bad = |message: &str| {
            Err(Error::Rejected {
                status: 422,
                message: message.to_owned(),
            })
        };
        match self {
            Self::Choice { criteria, .. } => {
                if criteria.len() < 2 {
                    return bad("a choice needs at least two options");
                }
                if criteria.len() > 255 {
                    return bad("a choice takes at most 255 options");
                }
                if criteria.keys().any(|option| option.is_empty()) {
                    return bad("every choice option needs a name");
                }
            }
            Self::Score { criteria, .. } => {
                if criteria.len() < 2 {
                    return bad("a score needs at least two levels");
                }
            }
            Self::Noul { instructions } => {
                if instructions.trim().is_empty() {
                    return bad("a noul needs a proposition to judge");
                }
            }
        }
        Ok(())
    }
}

/// One answer. Every kind reports how sure the service is, which is the
/// second decision axis: a confident narrow label and an unsure one are
/// different facts even when they name the same thing.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Answer {
    Choice {
        choice: String,
        confidence: f64,
        probabilities: HashMap<String, f64>,
    },
    Score {
        score: f64,
        confidence: f64,
        #[serde(default)]
        legend: HashMap<String, String>,
        probabilities: HashMap<String, f64>,
    },
    Noul {
        noul: f64,
    },
}
impl Answer {
    /// The answer as a label, where one exists. A score names the level it
    /// leans towards; a yes or no proposition has no label.
    pub fn label(&self) -> Option<String> {
        match self {
            Self::Choice { choice, .. } => Some(choice.clone()),
            Self::Score { score, legend, .. } => {
                // The reported score is probability-weighted and sits between
                // levels, so the level it is nearest is the one to name.
                legend.get(&(score.round() as i64).to_string()).cloned()
            }
            Self::Noul { .. } => None,
        }
    }
    /// How sure the service is, from 0 to 1. A yes or no answer carries its
    /// certainty in the value itself: a half is maximum doubt, and either end
    /// is conviction.
    pub fn confidence(&self) -> f64 {
        match self {
            Self::Choice { confidence, .. } | Self::Score { confidence, .. } => *confidence,
            Self::Noul { noul } => (noul - 0.5).abs() * 2.0,
        }
    }
    /// The answer as a number: the weighted level of a score, or the
    /// probability a proposition holds. A choice has no natural number.
    pub fn value(&self) -> Option<f64> {
        match self {
            Self::Score { score, .. } => Some(*score),
            Self::Noul { noul } => Some(*noul),
            Self::Choice { .. } => None,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Decision {
    pub model: String,
    pub answers: HashMap<String, Answer>,
    #[serde(default)]
    pub usage: Usage,
}
#[derive(Debug, Default, Deserialize)]
pub struct Usage {
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
}

#[derive(Serialize)]
struct Request<'a> {
    state: &'a str,
    model: &'a str,
    questions: &'a BTreeMap<String, Question>,
}

/// Asks typed questions about state, with bounded concurrency, retries that
/// respect the service's own backpressure, and a cache keyed by the exact
/// question and state.
pub struct Client {
    http: reqwest::Client,
    endpoint: String,
    key: String,
    model: String,
    gate: Arc<tokio::sync::Semaphore>,
    cache: Mutex<HashMap<u64, Arc<Decision>>>,
    cache_limit: usize,
}
impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never render the key.
        f.debug_struct("Client")
            .field("endpoint", &self.endpoint)
            .field("model", &self.model)
            .finish_non_exhaustive()
    }
}
impl Client {
    /// A client configured from the environment, or `None` when no key is
    /// set. Absence is a configuration state rather than a failure: a
    /// deployment that asks no questions needs no key.
    pub fn from_env() -> Option<Self> {
        let key = std::env::var("WALLEYE_TYPESAFE_API_KEY")
            .or_else(|_| std::env::var("TYPESAFE_API_KEY"))
            .ok()
            .filter(|key| !key.trim().is_empty())?;
        let endpoint =
            std::env::var("WALLEYE_TYPESAFE_URL").unwrap_or_else(|_| DEFAULT_ENDPOINT.to_owned());
        let model =
            std::env::var("WALLEYE_TYPESAFE_MODEL").unwrap_or_else(|_| DEFAULT_MODEL.to_owned());
        let concurrency = std::env::var("WALLEYE_TYPESAFE_CONCURRENCY")
            .ok()
            .and_then(|value| value.parse().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_CONCURRENCY);
        Some(Self::new(endpoint, key, model, concurrency))
    }
    pub fn new(
        endpoint: impl Into<String>,
        key: impl Into<String>,
        model: impl Into<String>,
        concurrency: usize,
    ) -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap_or_default(),
            endpoint: endpoint.into(),
            key: key.into(),
            model: model.into(),
            gate: Arc::new(tokio::sync::Semaphore::new(concurrency.max(1))),
            cache: Mutex::new(HashMap::new()),
            cache_limit: 50_000,
        }
    }
    pub fn model(&self) -> &str {
        &self.model
    }

    /// Ask one set of questions about one state.
    pub async fn ask(
        &self,
        state: &str,
        questions: &BTreeMap<String, Question>,
    ) -> Result<Arc<Decision>, Error> {
        if questions.is_empty() {
            return Err(Error::Rejected {
                status: 422,
                message: "ask at least one question".into(),
            });
        }
        for question in questions.values() {
            question.validate()?;
        }
        let key = cache_key(state, questions, &self.model);
        if let Some(hit) = self.cache.lock().ok().and_then(|c| c.get(&key).cloned()) {
            return Ok(hit);
        }
        let _permit = self
            .gate
            .acquire()
            .await
            .map_err(|_| Error::Unavailable("client shut down".into()))?;
        let decision = Arc::new(self.send(state, questions).await?);
        if let Ok(mut cache) = self.cache.lock() {
            // A cache that grows without bound is a leak, and this one holds
            // whole answers. Past the limit, stop admitting rather than evict:
            // the working set of a maintenance pass is its own batch.
            if cache.len() < self.cache_limit {
                cache.insert(key, Arc::clone(&decision));
            }
        }
        Ok(decision)
    }

    /// Ask the same questions about many states, up to the concurrency limit.
    /// Answers come back in the order the states were given. One state's
    /// failure is its own: the rest still answer.
    pub async fn ask_many(
        &self,
        states: &[String],
        questions: &BTreeMap<String, Question>,
    ) -> Vec<Result<Arc<Decision>, Error>> {
        use futures::stream::StreamExt;
        futures::stream::iter(states.iter().map(|state| self.ask(state, questions)))
            .buffered(self.gate.available_permits().max(1))
            .collect()
            .await
    }

    async fn send(
        &self,
        state: &str,
        questions: &BTreeMap<String, Question>,
    ) -> Result<Decision, Error> {
        let body = Request {
            state,
            model: &self.model,
            questions,
        };
        let mut last = String::new();
        for attempt in 0..MAX_ATTEMPTS {
            if attempt > 0 {
                // The service names its own backpressure with 429 and 529.
                // Backing off is the whole remedy; hammering it is not.
                tokio::time::sleep(Duration::from_millis(200 << (attempt - 1))).await;
            }
            let response = match self
                .http
                .post(&self.endpoint)
                .bearer_auth(&self.key)
                .json(&body)
                .send()
                .await
            {
                Ok(response) => response,
                Err(error) => {
                    last = error.to_string();
                    continue;
                }
            };
            let status = response.status().as_u16();
            if status == 429 || status == 529 || (500..600).contains(&status) {
                last = format!("status {status}");
                continue;
            }
            let text = response
                .text()
                .await
                .map_err(|error| Error::Unavailable(error.to_string()))?;
            if status != 200 {
                return Err(Error::Rejected {
                    status,
                    message: truncate(&text, 400),
                });
            }
            return serde_json::from_str(&text)
                .map_err(|error| Error::Malformed(format!("{error}: {}", truncate(&text, 200))));
        }
        Err(Error::Unavailable(format!(
            "{MAX_ATTEMPTS} attempts failed, last was {last}"
        )))
    }
}

fn truncate(text: &str, limit: usize) -> String {
    match text.char_indices().nth(limit) {
        Some((end, _)) => format!("{}...", &text[..end]),
        None => text.to_owned(),
    }
}

/// Identity of a question about a state. The state is the row's content, so
/// unchanged content asked the same thing is the same answer, and a
/// maintenance pass that reruns over rows it already classified pays nothing.
fn cache_key(state: &str, questions: &BTreeMap<String, Question>, model: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    state.hash(&mut hasher);
    model.hash(&mut hasher);
    for (name, question) in questions {
        name.hash(&mut hasher);
        match question {
            Question::Choice {
                instructions,
                criteria,
            } => {
                0u8.hash(&mut hasher);
                instructions.hash(&mut hasher);
                for (option, meaning) in criteria {
                    option.hash(&mut hasher);
                    meaning.hash(&mut hasher);
                }
            }
            Question::Score {
                instructions,
                criteria,
            } => {
                1u8.hash(&mut hasher);
                instructions.hash(&mut hasher);
                criteria.hash(&mut hasher);
            }
            Question::Noul { instructions } => {
                2u8.hash(&mut hasher);
                instructions.hash(&mut hasher);
            }
        }
    }
    hasher.finish()
}

/// Parse `option=meaning; other=meaning` into choice criteria, which is how a
/// SQL caller writes a taxonomy inline.
pub fn parse_criteria(spec: &str) -> Result<BTreeMap<String, String>, Error> {
    let mut criteria = BTreeMap::new();
    for clause in spec.split(';') {
        let clause = clause.trim();
        if clause.is_empty() {
            continue;
        }
        let Some((option, meaning)) = clause.split_once('=') else {
            return Err(Error::Rejected {
                status: 422,
                message: format!("expected 'option=meaning' in criteria, found '{clause}'"),
            });
        };
        criteria.insert(option.trim().to_owned(), meaning.trim().to_owned());
    }
    Ok(criteria)
}

/// Parse `first; second; third` into ordered score levels, lowest first.
pub fn parse_levels(spec: &str) -> Vec<String> {
    spec.split(';')
        .map(str::trim)
        .filter(|level| !level.is_empty())
        .map(str::to_owned)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn criteria_parse_from_the_inline_form_a_sql_caller_writes() {
        let criteria = parse_criteria("bug=Something is broken; billing=Charges and invoices")
            .expect("well formed");
        assert_eq!(criteria.len(), 2);
        assert_eq!(criteria["bug"], "Something is broken");
        assert_eq!(criteria["billing"], "Charges and invoices");
        assert_eq!(parse_levels("minor; moderate; severe").len(), 3);
    }
    #[test]
    fn criteria_without_a_meaning_are_refused_with_the_offending_clause() {
        let error = parse_criteria("bug").expect_err("no meaning given");
        assert!(error.to_string().contains("'bug'"), "{error}");
    }
    #[test]
    fn a_question_the_service_would_refuse_is_refused_before_the_round_trip() {
        let one = Question::choice("pick", [("only".to_owned(), "the only option".to_owned())]);
        assert!(one.validate().is_err(), "a choice of one is not a choice");
        assert!(
            Question::score("rate", ["only".to_owned()])
                .validate()
                .is_err()
        );
        assert!(Question::noul("  ").validate().is_err());
        assert!(
            Question::choice(
                "pick",
                [
                    ("a".to_owned(), "first".to_owned()),
                    ("b".to_owned(), "second".to_owned())
                ]
            )
            .validate()
            .is_ok()
        );
    }
    #[test]
    fn a_yes_or_no_answer_carries_its_own_certainty() {
        // A half is maximum doubt; either end is conviction.
        assert!((Answer::Noul { noul: 0.5 }.confidence() - 0.0).abs() < 1e-9);
        assert!((Answer::Noul { noul: 1.0 }.confidence() - 1.0).abs() < 1e-9);
        assert!((Answer::Noul { noul: 0.0 }.confidence() - 1.0).abs() < 1e-9);
        assert!((Answer::Noul { noul: 0.75 }.confidence() - 0.5).abs() < 1e-9);
    }
    #[test]
    fn the_same_question_about_the_same_state_is_the_same_cache_entry() {
        let questions: BTreeMap<String, Question> = [(
            "q".to_owned(),
            Question::choice(
                "pick",
                [
                    ("a".to_owned(), "first".to_owned()),
                    ("b".to_owned(), "second".to_owned()),
                ],
            ),
        )]
        .into();
        let other: BTreeMap<String, Question> = [(
            "q".to_owned(),
            Question::choice(
                "pick",
                [
                    ("a".to_owned(), "first".to_owned()),
                    ("b".to_owned(), "SECOND".to_owned()),
                ],
            ),
        )]
        .into();
        assert_eq!(
            cache_key("state", &questions, "m"),
            cache_key("state", &questions, "m")
        );
        assert_ne!(
            cache_key("state", &questions, "m"),
            cache_key("other", &questions, "m")
        );
        // A changed meaning is a changed question, so it must not reuse the
        // answer given for the old one.
        assert_ne!(
            cache_key("state", &questions, "m"),
            cache_key("state", &other, "m")
        );
        assert_ne!(
            cache_key("state", &questions, "m"),
            cache_key("state", &questions, "other-model")
        );
    }
    #[test]
    fn a_client_never_renders_its_key() {
        let client = Client::new("https://example.invalid", "secret-key-value", "m", 4);
        assert!(!format!("{client:?}").contains("secret-key-value"));
    }
}
