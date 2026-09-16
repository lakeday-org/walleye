//! Cold start from the archive: a cluster writes and archives a stream, every
//! volume is discarded, fresh members boot against the same bucket, and the
//! writer's next append at `archived_lsn + 1` must be accepted.
use std::sync::Arc;

use object_store::ObjectStore;
use serde_json::json;
use tokio::net::TcpListener;
use walleye_bitr::EncryptedRecord;
use walleye_bitr_server::{DiskReplica, OpaqueArchive, ReplicaGateway, ReplicaNode, node_router};

const INTERNAL_TOKEN: &str = "cold-start-internal-token";
const ROOT: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";

struct RunningNode {
    member: ReplicaNode,
    disk: Arc<DiskReplica>,
    task: tokio::task::JoinHandle<Result<(), std::io::Error>>,
}

fn record(stream: &str, lsn: u64, epoch: u64) -> EncryptedRecord {
    serde_json::from_value(json!({
        "stream": stream,
        "writer_epoch": epoch,
        "lsn": lsn,
        "committed_lsn": lsn.saturating_sub(1),
        "nonce": vec![lsn as u8; 24],
        "ciphertext": vec![lsn as u8; 64],
        "authentication": vec![lsn as u8; 32],
    }))
    .expect("record")
}

async fn group(
    directory: &tempfile::TempDir,
    names: &[&str],
) -> Result<Vec<RunningNode>, Box<dyn std::error::Error>> {
    let mut bound = Vec::new();
    for name in names {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        bound.push((
            *name,
            ReplicaNode::new(*name, format!("http://{}", listener.local_addr()?)),
            listener,
        ));
    }
    let members = bound
        .iter()
        .map(|(_, member, _)| member.clone())
        .collect::<Vec<_>>();
    let mut nodes = Vec::new();
    for (name, member, listener) in bound {
        let data = directory.path().join(name);
        let disk = Arc::new(DiskReplica::open_with_control(
            data.join("replica.log"),
            name,
            "hot",
            &data,
            data.join("control.json"),
            &members,
        )?);
        let app = node_router(Arc::clone(&disk), ROOT, Some(INTERNAL_TOKEN))?;
        let task = tokio::spawn(async move { axum::serve(listener, app).await });
        nodes.push(RunningNode { member, disk, task });
    }
    Ok(nodes)
}

fn gateway(
    directory: &tempfile::TempDir,
    nodes: &[RunningNode],
    archive: &Arc<OpaqueArchive>,
    name: &str,
) -> Result<ReplicaGateway, Box<dyn std::error::Error>> {
    Ok(ReplicaGateway::new_direct(
        nodes.iter().map(|n| n.member.clone()).collect(),
        2,
        ROOT,
        INTERNAL_TOKEN,
        directory.path().join(format!("{name}-control.json")),
        Arc::clone(archive),
    )?)
}

#[tokio::test]
async fn fresh_members_accept_the_append_after_the_archived_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    let first = tempfile::tempdir()?;
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let archive = Arc::new(OpaqueArchive::new(Arc::clone(&store), "bitr", 2)?);
    let stream = "walleye/acceptance";

    // Launch cluster writes four records; every member archives and trims.
    let old = group(&first, &["a", "b", "c"]).await?;
    let old_gateway = gateway(&first, &old, &archive, "old")?;
    let records = (1..=4)
        .map(|lsn| record(stream, lsn, 3))
        .collect::<Vec<_>>();
    assert_eq!(old_gateway.append_many(records).await?, 2);
    for node in &old {
        old_gateway.archive_local_commits(&node.disk).await?;
    }
    assert_eq!(archive.archived_lsn(stream).await?, 4);
    for node in old {
        node.task.abort();
    }

    // Machines and volumes are gone; fresh members boot against the bucket.
    let second = tempfile::tempdir()?;
    let fresh = group(&second, &["d", "e", "f"]).await?;
    let new_gateway = gateway(&second, &fresh, &archive, "new")?;

    // Without seeding, the writer's next append is refused for want of a
    // predecessor.
    let refused = new_gateway.append_many(vec![record(stream, 5, 4)]).await;
    assert!(refused.is_err(), "unseeded members must not accept LSN 5");

    for node in &fresh {
        assert_eq!(new_gateway.seed_from_archive(&node.disk).await?, 1);
        assert_eq!(
            new_gateway.seed_from_archive(&node.disk).await?,
            0,
            "idempotent"
        );
    }
    assert_eq!(
        new_gateway.append_many(vec![record(stream, 5, 4)]).await?,
        2
    );

    // A writer with the archived epoch or lower stays fenced after the restart.
    assert!(
        new_gateway
            .append_many(vec![record(stream, 6, 2)])
            .await
            .is_err()
    );

    // The full history, archive plus the new tail, recovers through the new gateway.
    let recovered = new_gateway.recover(stream, 0).await?;
    assert_eq!(
        recovered.iter().map(|r| r.lsn()).collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5]
    );
    for node in fresh {
        node.task.abort();
    }
    Ok(())
}

/// A gateway short of its quorum refuses readiness and names the members that
/// did not answer, so an operator sees which node is at fault.
#[tokio::test]
async fn readiness_names_the_unreachable_members() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let live = group(&directory, &["live"]).await?;
    // Two members at addresses nothing listens on: bind to claim a port, then
    // drop the listener so a probe is refused rather than left hanging.
    let mut dead = Vec::new();
    for name in ["gone-a", "gone-b"] {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        drop(listener);
        dead.push(ReplicaNode::new(name, format!("http://{address}")));
    }
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let archive = Arc::new(OpaqueArchive::new(store, "bitr", 2)?);
    let members = vec![live[0].member.clone(), dead[0].clone(), dead[1].clone()];
    let gateway = Arc::new(ReplicaGateway::new_direct(
        members,
        2,
        ROOT,
        INTERNAL_TOKEN,
        directory.path().join("gateway-control.json"),
        archive,
    )?);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let base = format!("http://{}", listener.local_addr()?);
    let app = walleye_bitr_server::gateway_router(Arc::clone(&gateway));
    let task = tokio::spawn(async move { axum::serve(listener, app).await });

    let response = reqwest::Client::new()
        .get(format!("{base}/readyz"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let body: serde_json::Value = response.json().await?;
    assert_eq!(body["ready"], serde_json::json!(false));
    assert_eq!(body["quorum"], serde_json::json!(2));
    assert_eq!(body["healthy"], serde_json::json!(["live"]));
    let mut unreachable = body["unreachable"]
        .as_array()
        .expect("unreachable members")
        .iter()
        .map(|id| id.as_str().unwrap_or_default().to_owned())
        .collect::<Vec<_>>();
    unreachable.sort();
    assert_eq!(unreachable, ["gone-a", "gone-b"]);
    assert!(
        body["reason"].as_str().unwrap_or_default().contains("2"),
        "{body}"
    );

    task.abort();
    for node in live {
        node.task.abort();
    }
    Ok(())
}
