//! Dispatches persisted stream work to bounded, stateless HTTP processors.
//! The processor owns its lease and acknowledgment in the source stream.
use crate::Service;
use futures::{StreamExt, stream::FuturesUnordered};
use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessorConfig {
    pub query: String,
    pub endpoint: String,
    #[serde(default)]
    pub headers: HashMap<String, String>,
    #[serde(default = "poll_ms")]
    pub poll_ms: u64,
    #[serde(default = "retry_ms")]
    pub retry_ms: u64,
}
fn poll_ms() -> u64 {
    5000
}
fn retry_ms() -> u64 {
    30000
}
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
#[derive(Deserialize)]
struct Work {
    key: String,
    available_at: i64,
}
impl Service {
    /// Reconstructs due work from stream state; no local queue is authoritative.
    pub async fn process(
        &self,
        config: ProcessorConfig,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let endpoint = reqwest::Url::parse(&config.endpoint)?;
        if !matches!(endpoint.scheme(), "http" | "https")
            || !endpoint.path().ends_with('/')
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || config.poll_ms == 0
            || config.retry_ms == 0
        {
            return Err("invalid stream processor configuration".into());
        }
        let engine = self
            .engine
            .as_ref()
            .ok_or("stream processor requires the stream API")?;
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(240))
            .build()?;
        let mut headers = reqwest::header::HeaderMap::new();
        for (key, value) in &config.headers {
            headers.insert(
                reqwest::header::HeaderName::from_bytes(key.as_bytes())?,
                value.parse()?,
            );
        }
        let mut running = FuturesUnordered::new();
        let mut active = HashSet::new();
        let mut retry = HashMap::new();
        loop {
            if self.quiescing.load(std::sync::atomic::Ordering::Acquire) {
                while running.next().await.is_some() {}
                return Ok(());
            }
            let now = now_ms();
            retry.retain(|_, until| *until > now);
            let sql = format!(
                "SELECT key, available_at FROM ({}) WHERE available_at <= {} ORDER BY available_at LIMIT 32",
                config.query, now
            );
            // A missing stream during first boot is expected; invalid queries remain visible.
            match engine.query(&sql).await {
                Ok(bytes) => {
                    for work in serde_json::from_slice::<Vec<Work>>(&bytes)? {
                        if active.len() >= 4 {
                            break;
                        }
                        if work.available_at > now as i64
                            || active.contains(&work.key)
                            || retry.contains_key(&work.key)
                        {
                            continue;
                        }
                        if work.key.is_empty()
                            || work.key.len() > 128
                            || !work
                                .key
                                .bytes()
                                .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
                        {
                            return Err("invalid stream processor key".into());
                        }
                        active.insert(work.key.clone());
                        let request = client
                            .post(endpoint.join(&work.key)?)
                            .headers(headers.clone());
                        running.push(async move {
                            let ok = match request.send().await {
                                Ok(response) => response.status().is_success(),
                                Err(_) => false,
                            };
                            (work.key, ok)
                        });
                    }
                }
                Err(error) => eprintln!("stream processor query failed: {error}"),
            }
            tokio::select! {
                result = running.next(), if !running.is_empty() => {
                    if let Some((key, ok)) = result {
                        active.remove(&key);
                        // A successful callback must advance its stream record. Back off even if
                        // it did not, so malformed processors cannot spin on an unacknowledged row.
                        retry.insert(key, now_ms() + if ok { config.poll_ms } else { config.retry_ms });
                    }
                },
                _ = self.changed.notified() => {},
                _ = tokio::time::sleep(Duration::from_millis(config.poll_ms)) => {},
            }
        }
    }
}
