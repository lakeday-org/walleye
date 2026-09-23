//! Kubernetes owns the cache pod list. EndpointSlice changes update disposable
//! cache placement; they never change Bitr's durable replication membership.
use crate::{ApiConfig, Config};
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    net::IpAddr,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use walleye_ring::{Membership, Node};
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiscoveryConfig {
    pub namespace: String,
    pub service: String,
}
impl DiscoveryConfig {
    fn validate(&self) -> Result<(), String> {
        for name in [&self.namespace, &self.service] {
            if name.is_empty()
                || name.len() > 63
                || !name
                    .bytes()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
                || name.starts_with('-')
                || name.ends_with('-')
            {
                return Err("invalid Kubernetes namespace or service name".into());
            }
        }
        Ok(())
    }
}
/// The StatefulSet supplies identity and resource settings, without a member list.
pub fn config_from_env() -> Result<Config, Box<dyn std::error::Error>> {
    let namespace = std::env::var("POD_NAMESPACE")?;
    let service = std::env::var("WALLEYE_DISCOVERY_SERVICE")?;
    let node_id = std::env::var("POD_NAME")?;
    let discovery = DiscoveryConfig { namespace, service };
    discovery.validate()?;
    let endpoint = format!(
        "http://{}.{}.{}.svc:8080",
        node_id, discovery.service, discovery.namespace
    );
    // Every pod serves the API and may own tables; the ring places cache
    // entries only, so no pod ordinal is special.
    let api = Some(ApiConfig {
        root_uri: std::env::var("WALLEYE_ROOT_URI")?,
        bitr_url: Some(std::env::var("WALLEYE_BITR_URL")?),
    });
    Ok(Config {
        node_id: node_id.clone(),
        listen: "0.0.0.0:8080".into(),
        directory: PathBuf::from("/data/cache"),
        memory_bytes: std::env::var("WALLEYE_CACHE_MEMORY_BYTES")?.parse()?,
        disk_bytes: std::env::var("WALLEYE_CACHE_DISK_BYTES")?.parse()?,
        token: std::env::var("WALLEYE_TOKEN")?,
        bitr: false,
        members: vec![Node::new(node_id, endpoint, 1.0)?],
        api,
        kubernetes: Some(discovery),
        processor: None,
        lease: crate::LeaseConfig::default(),
    })
}
#[derive(Deserialize)]
struct SliceList {
    items: Vec<Slice>,
    #[serde(default)]
    metadata: ListMeta,
}
#[derive(Default, Deserialize)]
struct ListMeta {
    #[serde(default, rename = "continue")]
    continuation: String,
}
#[derive(Deserialize)]
struct Slice {
    metadata: Meta,
    ports: Vec<Port>,
    endpoints: Vec<Endpoint>,
}
#[derive(Deserialize)]
struct Meta {
    #[serde(default)]
    namespace: String,
    #[serde(default)]
    labels: BTreeMap<String, String>,
}
#[derive(Deserialize)]
struct Port {
    name: Option<String>,
    port: Option<u16>,
    protocol: Option<String>,
}
#[derive(Deserialize)]
struct Endpoint {
    addresses: Vec<IpAddr>,
    #[serde(default)]
    conditions: Conditions,
    #[serde(rename = "targetRef")]
    target: Option<Target>,
}
#[derive(Default, Deserialize)]
struct Conditions {
    ready: Option<bool>,
    terminating: Option<bool>,
}
#[derive(Deserialize)]
struct Target {
    kind: String,
    name: String,
    namespace: Option<String>,
}
/// Merge every slice, remove unready/terminating pods, and deduplicate dual-stack
/// addresses deterministically. Stable pod names remain the rendezvous identity.
pub fn members_from_slices(
    config: &DiscoveryConfig,
    bytes: &[u8],
) -> Result<Vec<Node>, Box<dyn std::error::Error>> {
    let list: SliceList = serde_json::from_slice(bytes)?;
    if !list.metadata.continuation.is_empty() {
        return Err("incomplete EndpointSlice listing".into());
    }
    let mut nodes: BTreeMap<String, Node> = BTreeMap::new();
    for slice in list.items {
        if slice.metadata.namespace != config.namespace
            || slice.metadata.labels.get("kubernetes.io/service-name") != Some(&config.service)
        {
            continue;
        }
        let Some(port) = slice
            .ports
            .iter()
            .find(|p| {
                p.name.as_deref() == Some("cache")
                    && p.protocol.as_deref().is_none_or(|v| v == "TCP")
            })
            .and_then(|p| p.port)
            .filter(|p| *p > 0)
        else {
            continue;
        };
        for endpoint in slice.endpoints {
            if endpoint.conditions.ready == Some(false)
                || endpoint.conditions.terminating == Some(true)
            {
                continue;
            }
            let Some(target) = endpoint.target else {
                continue;
            };
            if target.kind != "Pod"
                || target
                    .namespace
                    .as_ref()
                    .is_some_and(|n| n != &config.namespace)
            {
                continue;
            }
            for address in endpoint.addresses {
                let authority = match address {
                    IpAddr::V4(ip) => format!("{ip}:{port}"),
                    IpAddr::V6(ip) => format!("[{ip}]:{port}"),
                };
                let node = Node::new(&target.name, format!("http://{authority}"), 1.0)?;
                match nodes.entry(target.name.clone()) {
                    std::collections::btree_map::Entry::Vacant(e) => {
                        e.insert(node);
                    }
                    std::collections::btree_map::Entry::Occupied(mut e) => {
                        if node.endpoint < e.get().endpoint {
                            e.insert(node);
                        }
                    }
                }
            }
        }
    }
    Ok(nodes.into_values().collect())
}
pub async fn follow(
    config: &DiscoveryConfig,
    ring: Arc<Membership>,
) -> Result<(), Box<dyn std::error::Error>> {
    config.validate()?;
    let service_account = PathBuf::from("/var/run/secrets/kubernetes.io/serviceaccount");
    let ca = reqwest::Certificate::from_pem(&std::fs::read(service_account.join("ca.crt"))?)?;
    let client = reqwest::Client::builder()
        .add_root_certificate(ca)
        .timeout(Duration::from_secs(4))
        .build()?;
    let host = std::env::var("KUBERNETES_SERVICE_HOST")?;
    let authority = if host.contains(':') {
        format!("[{host}]")
    } else {
        host
    };
    let url = format!(
        "https://{}:{}/apis/discovery.k8s.io/v1/namespaces/{}/endpointslices",
        authority,
        std::env::var("KUBERNETES_SERVICE_PORT_HTTPS").unwrap_or_else(|_| "443".into()),
        config.namespace
    );
    let selector = format!("kubernetes.io/service-name={}", config.service);
    let mut interval = tokio::time::interval(Duration::from_secs(5));
    loop {
        interval.tick().await;
        let result: Result<(), Box<dyn std::error::Error>> = async {
            // Projected service-account tokens rotate; do not retain the startup token.
            let token = tokio::fs::read_to_string(service_account.join("token")).await?;
            let mut response = client
                .get(&url)
                .query(&[("labelSelector", &selector)])
                .bearer_auth(token.trim())
                .send()
                .await?
                .error_for_status()?;
            let mut bytes = Vec::new();
            while let Some(chunk) = response.chunk().await? {
                if bytes.len() + chunk.len() > 1024 * 1024 {
                    return Err("EndpointSlice response exceeds 1 MiB".into());
                }
                bytes.extend_from_slice(&chunk);
            }
            let members = members_from_slices(config, &bytes)?;
            if !members.is_empty()
                && ring.update(members, Instant::now(), Duration::from_secs(30))?
            {
                eprintln!(
                    "cache membership updated: {} nodes",
                    ring.snapshot().members().len()
                );
            }
            Ok(())
        }
        .await;
        if let Err(error) = result {
            eprintln!("Kubernetes cache discovery failed; retaining last membership: {error}");
        }
    }
}
