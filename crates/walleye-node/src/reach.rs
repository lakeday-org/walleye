//! What a worker is allowed to reach, and nothing else.
//!
//! A worker gets one function out of its isolate. This decides what that
//! function will do. Today it does one thing: an outbound HTTP request to a
//! host somebody explicitly allowed. The default is that no host is allowed,
//! because a transform that can call anywhere is a transform that can
//! exfiltrate anywhere, and that should be a decision rather than an
//! accident.
use std::sync::Arc;

/// Hosts a worker may call, from `WALLEYE_WORKER_FETCH_ALLOW`. Empty means
/// none, and a worker that tries is told so rather than quietly failing.
#[derive(Clone, Debug, Default)]
pub struct Allowed(Vec<String>);
impl Allowed {
    pub fn from_env() -> Self {
        Self(
            std::env::var("WALLEYE_WORKER_FETCH_ALLOW")
                .unwrap_or_default()
                .split(',')
                .map(str::trim)
                .filter(|host| !host.is_empty())
                .map(str::to_ascii_lowercase)
                .collect(),
        )
    }
    fn permits(&self, host: &str) -> bool {
        let host = host.to_ascii_lowercase();
        self.0.iter().any(|allowed| {
            // A bare host allows itself and its subdomains, so one entry
            // covers an API that answers on more than one name.
            host == *allowed || host.ends_with(&format!(".{allowed}"))
        })
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    /// Only `fetch` today. Named so a second capability does not have to
    /// change the shape of every worker that already uses this one.
    #[serde(default = "fetch_verb")]
    kind: String,
    url: String,
    #[serde(default = "get")]
    method: String,
    #[serde(default)]
    headers: std::collections::HashMap<String, String>,
    #[serde(default)]
    body: Option<String>,
}
fn fetch_verb() -> String {
    "fetch".into()
}
fn get() -> String {
    "GET".into()
}
/// Replace every `{{env:NAME}}` with what the node holds under that name.
///
/// A missing one is refused rather than sent as the literal text, because a
/// request that quietly carries `{{env:TOKEN}}` as its credential fails
/// somewhere far away from the mistake.
pub(crate) fn substitute(value: &str) -> Result<String, String> {
    let mut out = String::with_capacity(value.len());
    let mut rest = value;
    while let Some(start) = rest.find("{{env:") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 6..];
        let Some(end) = after.find("}}") else {
            return Err("a secret reference is missing its closing braces".into());
        };
        let name = &after[..end];
        if name.is_empty() || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
            return Err(format!("{name} is not a usable secret name"));
        }
        let held =
            std::env::var(name).map_err(|_| format!("this node holds no secret called {name}"))?;
        out.push_str(&held);
        rest = &after[end + 2..];
    }
    out.push_str(rest);
    Ok(out)
}

/// The same substitution, for anything outside a worker request that also
/// needs a secret the node holds.
pub fn substitute_public(value: &str) -> Result<String, String> {
    substitute(value)
}

#[derive(serde::Serialize)]
struct Answer {
    status: u16,
    headers: std::collections::HashMap<String, String>,
    body: String,
}

/// The host a worker sees. Holds no credentials of its own: whatever a worker
/// sends in headers is what the request carries.
pub struct Reach {
    allowed: Allowed,
    client: reqwest::Client,
    runtime: tokio::runtime::Handle,
}
impl Reach {
    pub fn new(allowed: Allowed, runtime: tokio::runtime::Handle) -> Arc<Self> {
        Arc::new(Self {
            allowed,
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(20))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap_or_default(),
            runtime,
        })
    }
}
impl walleye_v8::Host for Reach {
    fn call(&self, request: &str) -> Result<String, String> {
        let request: Request =
            serde_json::from_str(request).map_err(|error| format!("bad request: {error}"))?;
        if request.kind != "fetch" {
            return Err(format!("a worker cannot {}", request.kind));
        }
        let url = reqwest::Url::parse(&request.url).map_err(|error| format!("bad url: {error}"))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err("a worker may only call http or https".into());
        }
        let host = url.host_str().unwrap_or_default().to_owned();
        if self.allowed.is_empty() {
            return Err(
                "no host is allowed; set WALLEYE_WORKER_FETCH_ALLOW to let workers call out".into(),
            );
        }
        if !self.allowed.permits(&host) {
            return Err(format!("{host} is not in WALLEYE_WORKER_FETCH_ALLOW"));
        }
        let method = reqwest::Method::from_bytes(request.method.to_uppercase().as_bytes())
            .map_err(|_| format!("{} is not a method", request.method))?;
        // A worker asks for a secret by name and never holds one. The value
        // comes from the node's own environment at the moment of the call, so
        // it is not in the worker, not in the view definition somebody stored,
        // and not in anything a query can read.
        let mut headers = std::collections::HashMap::with_capacity(request.headers.len());
        for (name, value) in request.headers {
            headers.insert(name, substitute(&value)?);
        }

        // The worker's thread waits here. It is a blocking-pool thread and
        // the deadline still applies, so a slow host costs the batch rather
        // than the node.
        let client = self.client.clone();
        let answer = tokio::task::block_in_place(|| {
            self.runtime.block_on(async move {
                let mut sending = client.request(method, url);
                for (name, value) in &headers {
                    sending = sending.header(name, value);
                }
                if let Some(body) = request.body {
                    sending = sending.body(body);
                }
                let response = sending.send().await.map_err(|error| error.to_string())?;
                let status = response.status().as_u16();
                let headers = response
                    .headers()
                    .iter()
                    .filter_map(|(name, value)| {
                        value
                            .to_str()
                            .ok()
                            .map(|value| (name.as_str().to_owned(), value.to_owned()))
                    })
                    .collect();
                let body = response.text().await.map_err(|error| error.to_string())?;
                Ok::<Answer, String>(Answer {
                    status,
                    headers,
                    body,
                })
            })
        })?;
        serde_json::to_string(&answer).map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_secret_is_named_by_a_worker_and_supplied_by_the_node() {
        // SAFETY: single-threaded test, set before it is read.
        unsafe { std::env::set_var("WALLEYE_TEST_SECRET", "s3cr3t") };
        assert_eq!(
            substitute("Bearer {{env:WALLEYE_TEST_SECRET}}").unwrap(),
            "Bearer s3cr3t"
        );
        assert_eq!(substitute("no secrets here").unwrap(), "no secrets here");
        unsafe { std::env::remove_var("WALLEYE_TEST_SECRET") };
    }
    #[test]
    fn a_secret_the_node_does_not_hold_is_refused_rather_than_sent_as_text() {
        let error = substitute("Bearer {{env:WALLEYE_ABSENT_SECRET}}").expect_err("not held");
        assert!(error.contains("WALLEYE_ABSENT_SECRET"), "{error}");
    }
    #[test]
    fn an_allowed_host_covers_its_subdomains_and_nothing_else() {
        let allowed = Allowed(vec!["example.com".into()]);
        assert!(allowed.permits("example.com"));
        assert!(allowed.permits("api.example.com"));
        assert!(!allowed.permits("notexample.com"));
        assert!(!allowed.permits("example.com.evil.net"));
    }
}
