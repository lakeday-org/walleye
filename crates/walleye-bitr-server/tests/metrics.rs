//! Focused tests for the authenticated autoscaling metrics contract.

use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use serde_json::json;
use tokio::net::TcpListener;
use walleye_bitr::EncryptedRecord;
use walleye_bitr_server::{
    DiskReplica, GatewayMetrics, INTERNAL_AUTH_HEADER, OpaqueArchive, ReplicaGateway, ReplicaNode,
    StorageNodeStatus, gateway_router, node_router,
};

const INTERNAL_TOKEN: &str = "metrics-internal-token";

fn archive() -> Result<Arc<OpaqueArchive>, Box<dyn std::error::Error>> {
    Ok(Arc::new(OpaqueArchive::new(
        Arc::new(object_store::memory::InMemory::new()),
        "replica",
        64,
    )?))
}

fn opaque(stream: &str, lsn: u64, committed_lsn: u64, marker: u8) -> EncryptedRecord {
    serde_json::from_value(json!({
        "stream": stream,
        "writer_epoch": 1,
        "lsn": lsn,
        "committed_lsn": committed_lsn,
        "nonce": vec![marker; 24],
        "ciphertext": vec![marker; 12],
        "authentication": vec![marker; 32],
    }))
    .expect("opaque test envelope")
}

struct RunningNode {
    member: ReplicaNode,
    task: tokio::task::JoinHandle<Result<(), std::io::Error>>,
}

async fn running_node(
    directory: &tempfile::TempDir,
    name: &str,
    root: &str,
) -> Result<RunningNode, Box<dyn std::error::Error>> {
    let disk = Arc::new(DiskReplica::open_with_config(
        directory.path().join(format!("{name}.log")),
        name,
        "hot",
        directory.path(),
    )?);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let app = node_router(Arc::clone(&disk), root, Some(INTERNAL_TOKEN))?;
    let task = tokio::spawn(async move { axum::serve(listener, app).await });
    Ok(RunningNode {
        member: ReplicaNode::new(name, format!("http://{address}")),
        task,
    })
}

#[tokio::test]
async fn storage_metrics_are_authenticated_and_report_the_data_volume()
-> Result<(), Box<dyn std::error::Error>> {
    let root = STANDARD.encode([71_u8; 32]);
    let directory = tempfile::tempdir()?;
    let disk = Arc::new(DiskReplica::open_with_config(
        directory.path().join("storage.log"),
        "storage-0",
        "hot",
        directory.path(),
    )?);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let app = node_router(disk, &root, Some(INTERNAL_TOKEN))?;
    let task = tokio::spawn(async move { axum::serve(listener, app).await });
    let client = reqwest::Client::new();

    let unauthorized = client
        .get(format!("http://{address}/internal/v1/metrics"))
        .send()
        .await?;
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    let response = client
        .get(format!("http://{address}/internal/v1/metrics"))
        .header(INTERNAL_AUTH_HEADER, INTERNAL_TOKEN)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let status: StorageNodeStatus = response.json().await?;
    assert_eq!(status.node_name, "storage-0");
    assert_eq!(status.tier, "hot");
    assert!(!status.boot_id.is_empty());
    assert!(status.timestamp_ms > 0);
    assert!(status.filesystem.total_bytes > 0);
    assert_eq!(
        status.filesystem.total_bytes,
        status.filesystem.free_bytes + status.filesystem.used_bytes
    );

    task.abort();
    Ok(())
}

#[tokio::test]
async fn gateway_metrics_have_monotonic_counters_and_fresh_storage_aggregate()
-> Result<(), Box<dyn std::error::Error>> {
    let root = STANDARD.encode([72_u8; 32]);
    let directory = tempfile::tempdir()?;
    let first = running_node(&directory, "storage-0", &root).await?;
    let second = running_node(&directory, "storage-1", &root).await?;
    let gateway = Arc::new(ReplicaGateway::new(
        vec![first.member.clone(), second.member.clone()],
        2,
        &root,
        INTERNAL_TOKEN,
        archive()?,
    )?);
    let record = opaque("tenant-a/catalog", 1, 0, 1);
    assert_eq!(gateway.append(record.clone()).await?, 2);
    assert_eq!(
        gateway.append(opaque("tenant-a/catalog", 3, 2, 2)).await,
        Err(walleye_bitr::ReplicaError::LsnConflict)
    );

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(async move { axum::serve(listener, gateway_router(gateway)).await });
    let response = reqwest::Client::new()
        .get(format!("http://{address}/internal/v1/metrics"))
        .header(INTERNAL_AUTH_HEADER, INTERNAL_TOKEN)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let metrics: GatewayMetrics = response.json().await?;
    assert!(!metrics.boot_id.is_empty());
    assert!(metrics.started_at_ms <= metrics.timestamp_ms);
    assert_eq!(metrics.append_attempts, 2);
    assert_eq!(metrics.append_acks, 1);
    assert_eq!(metrics.append_failures, 1);
    assert_eq!(metrics.acked_bytes, record.ciphertext().len() as u64);
    assert!(metrics.append_latency_nanos > 0);
    assert_eq!(metrics.active_requests, 0);
    assert_eq!(metrics.storage_nodes, 2);
    assert_eq!(metrics.healthy_storage, 2);
    assert_eq!(metrics.storage.len(), 2);
    assert_eq!(metrics.counters.append_acks, metrics.append_acks);

    task.abort();
    first.task.abort();
    second.task.abort();
    Ok(())
}

#[tokio::test]
async fn gateway_fails_closed_for_stale_node_metrics() -> Result<(), Box<dyn std::error::Error>> {
    let root = STANDARD.encode([73_u8; 32]);
    let directory = tempfile::tempdir()?;
    let first = running_node(&directory, "storage-0", &root).await?;
    let second = running_node(&directory, "storage-1", &root).await?;
    let gateway = Arc::new(
        ReplicaGateway::new(
            vec![first.member.clone(), second.member.clone()],
            2,
            &root,
            INTERNAL_TOKEN,
            archive()?,
        )?
        .with_metrics_max_age(Duration::ZERO),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(async move { axum::serve(listener, gateway_router(gateway)).await });
    let response = reqwest::Client::new()
        .get(format!("http://{address}/internal/v1/metrics"))
        .header(INTERNAL_AUTH_HEADER, INTERNAL_TOKEN)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    task.abort();
    first.task.abort();
    second.task.abort();
    Ok(())
}
