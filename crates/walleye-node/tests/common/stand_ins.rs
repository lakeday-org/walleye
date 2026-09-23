//! Stand-ins for the services walleye calls out to, so tests that exercise a
//! judgement, a drafted statement or an embedding are deterministic and run
//! in CI, rather than depending on a live service being up and quick.
//!
//! One server per test binary, on its own thread and runtime so it outlives
//! any one test, speaking each service's own wire format:
//!
//! - `/jev`: the decision service. A question is answered only if a test
//!   scripted it; anything else gets no answer at all, which the code treats
//!   exactly as it treats a judge that is unsure or not there. So a test that
//!   scripts nothing is still testing the safe defaults.
//! - `/responses`: an OpenAI Responses endpoint that drafts SQL from scripts.
//! - `/embeddings`: an OpenAI embeddings endpoint whose vectors hash a text's
//!   words, so a text is nearest itself and texts sharing words are near.
//!
//! Scripts are matched by a needle - a source name, a field name, a row's
//! text - so tests running in parallel in one binary do not answer each
//! other's questions.
#![allow(dead_code)]

use axum::{Json, Router, extract::State, routing::post};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

#[derive(Default)]
struct Scripts {
    answers: Mutex<Vec<(String, String, Value)>>,
    drafts: Mutex<Vec<(String, String)>>,
    jev_requests: Mutex<Vec<Value>>,
    model_requests: Mutex<Vec<Value>>,
    embedded: AtomicUsize,
    embeddings_down: AtomicBool,
}

static SCRIPTS: OnceLock<Arc<Scripts>> = OnceLock::new();
static STARTED: OnceLock<String> = OnceLock::new();

fn scripts() -> &'static Arc<Scripts> {
    SCRIPTS.get_or_init(Default::default)
}

/// Which services a binary wants pointed at the stand-ins.
#[derive(Clone, Copy)]
pub struct Services {
    pub jev: bool,
    pub model: bool,
    pub embeddings: bool,
}

pub const ALL: Services = Services {
    jev: true,
    model: true,
    embeddings: true,
};
pub const JEV: Services = Services {
    jev: true,
    model: false,
    embeddings: false,
};

/// Start the stand-ins once for this binary and point the environment at
/// them. Every test calls this first; the first call decides.
pub fn start(services: Services) -> &'static str {
    STARTED.get_or_init(|| {
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                sender
                    .send(format!("http://{}", listener.local_addr().unwrap()))
                    .unwrap();
                let app = Router::new()
                    .route("/jev", post(jev))
                    .route("/responses", post(responses))
                    .route("/embeddings", post(embeddings))
                    .with_state(scripts().clone());
                axum::serve(listener, app).await.unwrap();
            });
        });
        let base = receiver.recv().unwrap();
        // SAFETY: set once, before any client in this binary reads them.
        unsafe {
            for name in [
                "TYPESAFE_API_KEY",
                "TYPESAFE_URL",
                "WALLEYE_MODEL_KEY",
                "WALLEYE_MODEL_URL",
                "WALLEYE_EMBEDDING_KEY",
                "WALLEYE_EMBEDDING_URL",
            ] {
                std::env::remove_var(name);
            }
            if services.jev {
                std::env::set_var("TYPESAFE_API_KEY", "stand-in");
                std::env::set_var("TYPESAFE_URL", format!("{base}/jev"));
            }
            if services.model {
                std::env::set_var("WALLEYE_MODEL_KEY", "stand-in");
                std::env::set_var("WALLEYE_MODEL_URL", format!("{base}/responses"));
                std::env::set_var("WALLEYE_MODEL_NAME", "stand-in-model");
            }
            if services.embeddings {
                std::env::set_var("WALLEYE_EMBEDDING_KEY", "stand-in");
                std::env::set_var("WALLEYE_EMBEDDING_URL", format!("{base}/embeddings"));
                std::env::set_var("WALLEYE_EMBEDDING_MODEL", "stand-in-embedder");
            }
        }
        base
    })
}

// --- scripting ---------------------------------------------------------------

/// Answer a question whose name starts with `question`, asked about a state
/// or with instructions that contain `needle`.
pub fn answer(needle: &str, question: &str, answer: Value) {
    scripts()
        .answers
        .lock()
        .unwrap()
        .push((needle.to_owned(), question.to_owned(), answer));
}

/// Answer only where the state contains `state` and the instructions contain
/// `instructions`: two different questions about the same row.
pub fn answer_to(state: &str, instructions: &str, question: &str, answer: Value) {
    scripts().answers.lock().unwrap().push((
        format!("{state}\u{0}{instructions}"),
        question.to_owned(),
        answer,
    ));
}

/// A choice, with the probabilities a real answer carries.
pub fn choice(label: &str, confidence: f64, probabilities: &[(&str, f64)]) -> Value {
    let probabilities: serde_json::Map<String, Value> = probabilities
        .iter()
        .map(|(k, v)| ((*k).to_owned(), json!(v)))
        .collect();
    json!({"type": "choice", "choice": label, "confidence": confidence,
           "probabilities": probabilities})
}

/// A yes or no, as the probability that it holds.
pub fn noul(value: f64) -> Value {
    json!({"type": "noul", "noul": value})
}

/// A score, naming the level it leans towards.
pub fn score(level: &str, confidence: f64) -> Value {
    json!({"type": "score", "score": 1.0, "confidence": confidence,
           "legend": {"1": level}, "probabilities": {}})
}

/// Draft this SQL when the prompt contains `needle`.
pub fn draft(needle: &str, sql: &str) {
    scripts()
        .drafts
        .lock()
        .unwrap()
        .push((needle.to_owned(), sql.to_owned()));
}

/// Every request the decision service was sent whose state contains `needle`.
pub fn jev_requests(needle: &str) -> Vec<Value> {
    scripts()
        .jev_requests
        .lock()
        .unwrap()
        .iter()
        .filter(|r| r["state"].as_str().is_some_and(|s| s.contains(needle)))
        .cloned()
        .collect()
}

/// Every prompt the model was sent that contains `needle`.
pub fn model_prompts(needle: &str) -> Vec<String> {
    scripts()
        .model_requests
        .lock()
        .unwrap()
        .iter()
        .map(|r| r.to_string())
        .filter(|r| r.contains(needle))
        .collect()
}

pub fn embedded() -> usize {
    scripts().embedded.load(Ordering::SeqCst)
}

pub fn embeddings_down(down: bool) {
    scripts().embeddings_down.store(down, Ordering::SeqCst);
}

/// The stand-in embedder's vector for a text: its words hashed into sixteen
/// places and normalised.
pub fn vector(text: &str) -> Vec<f32> {
    use std::hash::{Hash, Hasher};
    let mut v = [0f32; 16];
    for word in text.split_whitespace() {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        word.to_lowercase()
            .trim_matches(|c: char| !c.is_alphanumeric())
            .hash(&mut h);
        v[(h.finish() as usize) % 16] += 1.0;
    }
    let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-6);
    v.iter().map(|x| x / norm).collect()
}

// --- the services ------------------------------------------------------------

async fn jev(State(s): State<Arc<Scripts>>, Json(body): Json<Value>) -> Json<Value> {
    s.jev_requests.lock().unwrap().push(body.clone());
    let state = body["state"].as_str().unwrap_or_default();
    let rules = s.answers.lock().unwrap().clone();
    let mut answers = serde_json::Map::new();
    for (name, question) in body["questions"].as_object().cloned().unwrap_or_default() {
        let instructions = question["instructions"].as_str().unwrap_or_default();
        if let Some((_, _, answer)) = rules.iter().rev().find(|(needle, prefix, _)| {
            name.starts_with(prefix.as_str())
                && match needle.split_once('\u{0}') {
                    Some((in_state, in_instructions)) => {
                        state.contains(in_state) && instructions.contains(in_instructions)
                    }
                    None => {
                        state.contains(needle.as_str()) || instructions.contains(needle.as_str())
                    }
                }
        }) {
            answers.insert(name, answer.clone());
        }
    }
    Json(json!({"model": "stand-in", "answers": answers, "usage": {}}))
}

async fn responses(State(s): State<Arc<Scripts>>, Json(body): Json<Value>) -> Json<Value> {
    s.model_requests.lock().unwrap().push(body.clone());
    let prompt = body.to_string();
    let drafts = s.drafts.lock().unwrap().clone();
    let sql = drafts
        .iter()
        .rev()
        .find(|(needle, _)| prompt.contains(needle.as_str()))
        .map(|(_, sql)| sql.clone())
        .unwrap_or_else(|| "SELECT 'no script matched this prompt' AS error".to_owned());
    Json(json!({
        "output": [{"type": "message", "role": "assistant",
                    "content": [{"type": "output_text", "text": sql}]}],
        "usage": {"input_tokens": 0, "output_tokens": 0}
    }))
}

async fn embeddings(
    State(s): State<Arc<Scripts>>,
    Json(body): Json<Value>,
) -> (axum::http::StatusCode, Json<Value>) {
    if s.embeddings_down.load(Ordering::SeqCst) {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "overloaded"})),
        );
    }
    let input: Vec<String> = body["input"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .filter_map(|t| t.as_str().map(str::to_owned))
        .collect();
    s.embedded.fetch_add(input.len(), Ordering::SeqCst);
    let data: Vec<Value> = input
        .iter()
        .enumerate()
        .map(|(index, text)| json!({"object": "embedding", "index": index, "embedding": vector(text)}))
        .collect();
    (
        axum::http::StatusCode::OK,
        Json(json!({"object": "list", "data": data})),
    )
}
