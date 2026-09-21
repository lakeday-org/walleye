//! A model that writes SQL, with the decision service beside it as a tool.
//!
//! Measured on Spider 2.0-lite's local split, giving the model a bounded-choice
//! classifier to call reached the same accuracy as the model alone on half the
//! reasoning tokens and 43% less wall time. It is not more accurate. It is
//! cheaper for the same answer, because the narrow decisions - which column
//! means what, which of two readings of a phrase is meant - move off the
//! expensive model onto one that answers them in about 150ms.
//!
//! The model only ever produces a statement. Running it is the engine's job,
//! and the rows are what the caller actually asked for.
use serde_json::{Value, json};
use std::time::Duration;

type Error = Box<dyn std::error::Error + Send + Sync>;

/// Where statements are written. Any endpoint speaking OpenAI's Responses
/// shape will do: point `WALLEYE_MODEL_URL` at a self-hosted gateway, a proxy,
/// or another vendor's compatible surface and nothing else changes.
pub const DEFAULT_ENDPOINT: &str = "https://api.openai.com/v1/responses";
pub const DEFAULT_MODEL: &str = "gpt-5.6-luna";
/// Measured across our question set and Spider 2.0-lite, reasoning buys no
/// accuracy here -- 52.6% against 51.9% at the highest setting -- and costs
/// time. None by default; raise it with `WALLEYE_MODEL_EFFORT` where a model
/// needs it.
pub const DEFAULT_EFFORT: &str = "none";
/// How long one statement may take to write.
pub const DEFAULT_TIMEOUT: u64 = 300;
/// The model may consult the classifier this many times before the draft is
/// abandoned. It averaged 1.6 calls per question and never needed more than 17.
const MAX_ROUNDS: u32 = 12;

/// One bounded question the model chose to ask, and what came back. Returned
/// with the answer so a caller can see what the reading rested on, and how
/// sure each part of it was.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Decision {
    pub question: String,
    pub answer: String,
    pub confidence: f64,
}

/// A statement, and what it cost to arrive at.
#[derive(Debug, Default, serde::Serialize)]
pub struct Draft {
    pub sql: String,
    pub decisions: Vec<Decision>,
    pub rounds: u32,
    pub input_tokens: u64,
    pub cached_tokens: u64,
    pub output_tokens: u64,
    pub reasoning_tokens: u64,
}

fn tool_spec() -> Value {
    json!({
        "type": "function",
        "name": "ask_jev",
        "description":
            "Ask a fast, cheap classifier one multiple-choice question and get back the \
             chosen option with a calibrated confidence between 0 and 1. It is far cheaper \
             and faster than reasoning it out yourself, and it is calibrated, so a low \
             confidence genuinely means the answer is doubtful. Use it to settle bounded \
             questions about this database: which table holds a thing, which column means \
             a thing, whether two columns refer to each other, which of several readings \
             of the user's question is meant. Ask as many as you need.",
        "parameters": {
            "type": "object",
            "properties": {
                "question": {"type": "string", "description": "The question to decide."},
                "options": {
                    "type": "array", "items": {"type": "string"},
                    "minItems": 2, "maxItems": 60,
                    "description": "The options it must choose between."
                }
            },
            "required": ["question", "options"],
            "additionalProperties": false
        }
    })
}

const SYSTEM: &str = "You are an expert analyst writing SQL for a DataFusion engine, which \
    accepts standard ANSI SQL. Reply with exactly one SELECT statement and nothing else: no \
    prose, no markdown fences, no commentary. Use only the tables and columns in the schema, \
    and quote every identifier with double quotes so that names holding capitals, spaces or \
    reserved words resolve. The statement must read only; never write, create or drop.";

/// Strip a markdown fence the model was asked not to produce, and a trailing
/// semicolon the engine will not take.
fn clean(text: &str) -> String {
    let text = text.trim();
    let body = match text.find("```") {
        Some(start) => {
            let after = &text[start + 3..];
            let after = after.strip_prefix("sql").unwrap_or(after);
            match after.find("```") {
                Some(end) => &after[..end],
                None => after,
            }
        }
        None => text,
    };
    body.trim().trim_end_matches(';').trim().to_owned()
}

#[derive(Debug)]
pub struct Model {
    endpoint: String,
    key: String,
    name: String,
    effort: Option<String>,
    http: reqwest::Client,
}

impl Model {
    /// One name for the key and no fallback, as with the decision service: a
    /// second name is a second place for a stale value to hide.
    pub fn from_env() -> Option<Self> {
        let key = std::env::var("WALLEYE_MODEL_KEY")
            .ok()
            .filter(|k| !k.trim().is_empty())?;
        let effort =
            std::env::var("WALLEYE_MODEL_EFFORT").unwrap_or_else(|_| DEFAULT_EFFORT.to_owned());
        let seconds = std::env::var("WALLEYE_MODEL_TIMEOUT")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_TIMEOUT);
        Some(Self {
            endpoint: std::env::var("WALLEYE_MODEL_URL")
                .unwrap_or_else(|_| DEFAULT_ENDPOINT.to_owned()),
            key,
            name: std::env::var("WALLEYE_MODEL_NAME").unwrap_or_else(|_| DEFAULT_MODEL.to_owned()),
            // "none" is a value the service takes, not an absence: omitting
            // the field lets a model reason by default, which is the opposite
            // of asking for none. An empty setting is how you omit it, for an
            // endpoint that does not know the field at all.
            effort: (!effort.is_empty()).then_some(effort),
            http: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(seconds))
                .build()
                .ok()?,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, body: &Value) -> Result<Value, Error> {
        let mut last = String::new();
        for attempt in 0..4u32 {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(500 << (attempt - 1))).await;
            }
            let response = match self
                .http
                .post(&self.endpoint)
                .bearer_auth(&self.key)
                .json(body)
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
            let text = response.text().await.unwrap_or_default();
            // 520-524 are a proxy's, not the service's, and are transient.
            if matches!(status, 429 | 500..=504 | 520 | 522 | 524 | 529) {
                last = format!("status {status}");
                continue;
            }
            if status != 200 {
                return Err(format!("model refused ({status}): {}", brief(&text)).into());
            }
            return Ok(serde_json::from_str(&text)?);
        }
        Err(format!("model unreachable after 4 attempts, last was {last}").into())
    }

    /// Answer one prompt with text.
    ///
    /// `shape` is an optional JSON schema; supplying one makes the answer
    /// structured JSON rather than prose, which is what a column of records
    /// usually wants.
    pub async fn complete(&self, prompt: &str, shape: Option<&Value>) -> Result<String, Error> {
        let mut body = json!({
            "model": self.name,
            "input": [{"role": "user", "content": prompt}],
            "max_output_tokens": 32000,
            "store": false,
        });
        if let Some(effort) = &self.effort {
            body["reasoning"] = json!({"effort": effort});
        }
        if let Some(schema) = shape {
            body["text"] = json!({"format": {
                "type": "json_schema",
                "name": schema.get("name").and_then(Value::as_str).unwrap_or("answer"),
                "schema": schema.get("schema").unwrap_or(schema),
                "strict": schema.get("strict").and_then(Value::as_bool).unwrap_or(true),
            }});
        }
        let payload = self.send(&body).await?;
        let mut text = String::new();
        for item in payload["output"].as_array().unwrap_or(&vec![]) {
            for part in item["content"].as_array().unwrap_or(&vec![]) {
                if part["type"] == "output_text"
                    && let Some(chunk) = part["text"].as_str()
                {
                    text.push_str(chunk);
                }
            }
        }
        Ok(text.trim().to_owned())
    }

    /// The same client, with this call's own model and effort.
    pub fn with(&self, name: Option<&str>, effort: Option<&str>) -> Self {
        Self {
            endpoint: self.endpoint.clone(),
            key: self.key.clone(),
            name: name.unwrap_or(&self.name).to_owned(),
            effort: match effort {
                Some("") => None,
                Some(other) => Some(other.to_owned()),
                None => self.effort.clone(),
            },
            http: self.http.clone(),
        }
    }

    /// Draft a statement for `phrase` against `schema`.
    ///
    /// `refused` carries statements an earlier draft produced that the planner
    /// would not accept, so a retry is a different question rather than the
    /// same one asked again.
    pub async fn draft(
        &self,
        schema: &str,
        phrase: &str,
        refused: &[(String, String)],
        jev: Option<&walleye_typesafe::Client>,
        state: &str,
    ) -> Result<Draft, Error> {
        // The model has no clock, so a question about last week has nothing to
        // count back from. Today is a fact, not a judgement: it is stated.
        let mut prompt = format!(
            "Today is {}.\n\nDatabase schema:\n\n{schema}\n\nQuestion: {phrase}",
            today()
        );
        if !refused.is_empty() {
            prompt.push_str(
                "\n\nEarlier statements for this same question were handed to the query \
                 planner, which refused them. Do not write them again:\n",
            );
            for (sql, reason) in refused {
                prompt.push_str(&format!("\n  {sql}\n    refused: {reason}\n"));
            }
        }
        prompt.push_str(
            "\n\nWrite one SELECT statement that answers the question. Return only the SQL.",
        );

        let mut input = vec![
            json!({"role": "system", "content": SYSTEM}),
            json!({"role": "user", "content": prompt}),
        ];
        let mut draft = Draft::default();

        for _round in 0..MAX_ROUNDS {
            let mut body = json!({
                "model": self.name,
                "input": input,
                "max_output_tokens": 32000,
                "store": false,
            });
            if let Some(effort) = &self.effort {
                body["reasoning"] = json!({"effort": effort});
            }
            if jev.is_some() {
                body["tools"] = json!([tool_spec()]);
            }
            let payload = self.send(&body).await?;
            draft.rounds += 1;

            let usage = &payload["usage"];
            draft.input_tokens += usage["input_tokens"].as_u64().unwrap_or(0);
            draft.cached_tokens += usage["input_tokens_details"]["cached_tokens"]
                .as_u64()
                .unwrap_or(0);
            draft.output_tokens += usage["output_tokens"].as_u64().unwrap_or(0);
            draft.reasoning_tokens += usage["output_tokens_details"]["reasoning_tokens"]
                .as_u64()
                .unwrap_or(0);

            let output = payload["output"].as_array().cloned().unwrap_or_default();
            let calls: Vec<&Value> = output
                .iter()
                .filter(|item| item["type"] == "function_call")
                .collect();
            if calls.is_empty() {
                let mut text = String::new();
                for item in &output {
                    for part in item["content"].as_array().unwrap_or(&vec![]) {
                        if part["type"] == "output_text"
                            && let Some(chunk) = part["text"].as_str()
                        {
                            text.push_str(chunk);
                        }
                    }
                }
                draft.sql = clean(&text);
                if draft.sql.is_empty() {
                    return Err("the model returned no statement".into());
                }
                return Ok(draft);
            }

            // A reasoning item explains the calls that follow it, and the
            // service rejects the calls if it is left behind.
            for item in &output {
                if matches!(
                    item["type"].as_str(),
                    Some("reasoning") | Some("function_call") | Some("message")
                ) {
                    input.push(item.clone());
                }
            }
            for call in calls {
                let reply = self.consult(call, jev, state, &mut draft).await;
                input.push(json!({
                    "type": "function_call_output",
                    "call_id": call["call_id"],
                    "output": reply.to_string(),
                }));
            }
        }
        Err(format!("the model did not settle on a statement in {MAX_ROUNDS} rounds").into())
    }

    /// Answer one `ask_jev` call. A failure is reported to the model rather
    /// than raised: it asked a question, and "that question did not work" is
    /// an answer it can act on.
    async fn consult(
        &self,
        call: &Value,
        jev: Option<&walleye_typesafe::Client>,
        state: &str,
        draft: &mut Draft,
    ) -> Value {
        let attempt = || -> Result<(String, Vec<String>), Error> {
            let arguments: Value =
                serde_json::from_str(call["arguments"].as_str().unwrap_or("{}"))?;
            let question = arguments["question"].as_str().unwrap_or("").to_owned();
            let options: Vec<String> = arguments["options"]
                .as_array()
                .map(|options| {
                    options
                        .iter()
                        .filter_map(|o| o.as_str().map(str::to_owned))
                        .take(60)
                        .collect()
                })
                .unwrap_or_default();
            if options.len() < 2 {
                return Err("a choice needs at least two options".into());
            }
            Ok((question, options))
        };
        let (question, options) = match attempt() {
            Ok(pair) => pair,
            Err(error) => return json!({"error": error.to_string()}),
        };
        let Some(jev) = jev else {
            return json!({"error": "no decision service is configured"});
        };
        let asked = std::collections::BTreeMap::from([(
            "q".to_owned(),
            walleye_typesafe::Question::choice(
                question.clone(),
                options.iter().map(|o| (o.clone(), o.clone())),
            ),
        )]);
        match jev.ask(state, &asked).await {
            Ok(decision) => {
                let answer = decision
                    .answers
                    .get("q")
                    .and_then(|a| a.label().map(|l| (l, a.confidence())));
                match answer {
                    Some((label, confidence)) => {
                        draft.decisions.push(Decision {
                            question,
                            answer: label.clone(),
                            confidence: (confidence * 1000.0).round() / 1000.0,
                        });
                        json!({"answer": label, "confidence": (confidence * 1000.0).round() / 1000.0})
                    }
                    None => json!({"error": "the classifier returned no label"}),
                }
            }
            Err(error) => json!({"error": error.to_string()}),
        }
    }
}

/// Today, as the model should read it.
fn today() -> String {
    let days = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| (since.as_secs() / 86_400) as i64)
        .unwrap_or(0);
    let (year, month, day) = civil_from_days(days);
    const NAMES: [&str; 7] = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ];
    let weekday = NAMES[(days + 3).rem_euclid(7) as usize];
    format!("{weekday}, {year:04}-{month:02}-{day:02}")
}

/// Howard Hinnant's civil-from-days.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if month <= 2 { year + 1 } else { year }, month, day)
}

fn brief(text: &str) -> String {
    match text.char_indices().nth(300) {
        Some((end, _)) => format!("{}...", &text[..end]),
        None => text.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The model is told not to fence its answer and sometimes does anyway.
    #[test]
    fn a_fenced_statement_is_unwrapped() {
        assert_eq!(clean("```sql\nSELECT 1\n```"), "SELECT 1");
        assert_eq!(clean("```\nSELECT 1\n```"), "SELECT 1");
        assert_eq!(clean("SELECT 1;"), "SELECT 1");
        assert_eq!(clean("  SELECT 1  "), "SELECT 1");
    }

    /// An unterminated fence still yields the statement rather than nothing.
    #[test]
    fn an_unclosed_fence_still_gives_the_statement() {
        assert_eq!(clean("```sql\nSELECT 1"), "SELECT 1");
    }
}
