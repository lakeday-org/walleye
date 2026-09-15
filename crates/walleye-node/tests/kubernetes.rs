//! Kubernetes membership is a cache hint, never an authority for WAL durability.
use serde_json::{Value, json};
use walleye_node::kubernetes::{DiscoveryConfig, members_from_slices};
fn endpoint(name: &str, ip: &str, ready: bool) -> Value {
    json!({"addresses":[ip],"conditions":{"ready":ready},"targetRef":{"kind":"Pod","namespace":"test","name":name}})
}
fn slice(endpoints: Vec<Value>) -> Value {
    json!({"metadata":{"namespace":"test","labels":{"kubernetes.io/service-name":"walleye-cache"}},"ports":[{"name":"cache","port":8080,"protocol":"TCP"}],"endpoints":endpoints})
}
fn config() -> DiscoveryConfig {
    DiscoveryConfig {
        namespace: "test".into(),
        service: "walleye-cache".into(),
    }
}
#[test]
fn slices_filter_unready_terminating_foreign_endpoints_and_deduplicate() {
    let ready = endpoint("cache-0", "10.0.0.1", true);
    let mut terminating = endpoint("cache-2", "10.0.0.3", true);
    terminating["conditions"]["terminating"] = json!(true);
    let mut foreign = slice(vec![endpoint("other", "10.0.0.4", true)]);
    foreign["metadata"]["labels"]["kubernetes.io/service-name"] = json!("other");
    let payload = json!({"items":[slice(vec![ready.clone(),endpoint("cache-1","10.0.0.2",false),terminating]),slice(vec![ready]),foreign]});
    let members = members_from_slices(&config(), &serde_json::to_vec(&payload).unwrap()).unwrap();
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].id, "cache-0");
    assert_eq!(members[0].endpoint, "http://10.0.0.1:8080");
}
#[test]
fn replacement_keeps_pod_identity_and_ipv6_urls_are_valid() {
    let before = json!({"items":[slice(vec![endpoint("cache-0","10.0.0.1",true)])]});
    let after = json!({"items":[slice(vec![endpoint("cache-0","fd00::2",true)])]});
    let a = members_from_slices(&config(), &serde_json::to_vec(&before).unwrap()).unwrap();
    let b = members_from_slices(&config(), &serde_json::to_vec(&after).unwrap()).unwrap();
    assert_eq!(a[0].id, b[0].id);
    assert_eq!(b[0].endpoint, "http://[fd00::2]:8080");
    assert!(
        members_from_slices(&config(), br#"{"metadata":{"continue":"more"},"items":[]}"#).is_err()
    );
    assert!(members_from_slices(&config(), b"invalid").is_err());
}
