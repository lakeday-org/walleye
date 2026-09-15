//! Process entry point for one cloud replica Machine.

use crate::cas_probe;

use std::env;
use std::net::IpAddr;
use std::sync::Arc;

use crate::{DiskReplica, OpaqueArchive, ReplicaGateway, gateway_router, node_router, parse_nodes};
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use tokio::net::TcpListener;

/// Runs one combined storage, gateway, and opaque-archive replica process.
pub async fn run_from_env() -> Result<(), Box<dyn std::error::Error>> {
    let arguments: Vec<String> = Vec::new();
    if arguments == ["--verify-control-head-cas"] {
        let archive = build_archive()?;
        let result = cas_probe::verify(archive.object_store(), archive.prefix())
            .await
            .map_err(|error| -> Box<dyn std::error::Error> { error })?;
        println!("{}", serde_json::to_string(&result)?);
        return Ok(());
    }
    if !arguments.is_empty() {
        return Err("unsupported replica arguments".into());
    }
    let root_key = required("LAKEDAY_DATAPLANE_ROOT_KEY")?;
    run_combined(&root_key).await
}

/// Runs the two listeners owned by every direct Fly Machine. Storage remains
/// private on port 9090 while every combined process exposes the same
/// stateless coordinator surface on port 30080.
async fn run_combined(root_key: &str) -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = env::var("LAKEDAY_REPLICA_DATA_DIR").unwrap_or_else(|_| "/data".to_owned());
    let data_dir = std::path::PathBuf::from(data_dir);
    let log_path = env::var("LAKEDAY_REPLICA_LOG")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| data_dir.join("replica.log"));
    let node_name = required_nonempty("LAKEDAY_REPLICA_NODE_NAME")?;
    let tier = required_nonempty("LAKEDAY_REPLICA_TIER")?;
    let initial_nodes = direct_nodes()?;
    let control_path = env::var("LAKEDAY_REPLICA_CONTROL_PATH")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| crate::control_state_path(&data_dir));
    let node = Arc::new(DiskReplica::open_with_control(
        &log_path,
        node_name.clone(),
        tier,
        &data_dir,
        &control_path,
        &initial_nodes,
    )?);
    let internal_token = required_nonempty("LAKEDAY_REPLICA_INTERNAL_TOKEN")?;
    let quorum = env::var("LAKEDAY_REPLICA_QUORUM")
        .unwrap_or_else(|_| "2".to_owned())
        .parse::<usize>()
        .map_err(|_| "LAKEDAY_REPLICA_QUORUM must be an integer")?;
    let admin_token = env::var("LAKEDAY_REPLICA_ADMIN_TOKEN").unwrap_or_default();
    let archive = Arc::new(build_archive()?);
    let gateway = Arc::new(
        ReplicaGateway::new_direct_with_control(
            initial_nodes,
            quorum,
            root_key,
            internal_token.clone(),
            node.control().ok_or("direct control state missing")?,
            Arc::clone(&archive),
        )?
        .with_admin_token(admin_token)
        .with_local_member_id(node_name),
    );
    // Existing quorum service remains available while the provider's
    // conditional writes are checked. The operator may select online
    // scaling only after this process has verified the actual archive
    // backend, and after every active node advertises the new protocol.
    let probe_archive = Arc::clone(&archive);
    let probe_gateway = Arc::clone(&gateway);
    tokio::spawn(async move {
        match cas_probe::verify(probe_archive.object_store(), probe_archive.prefix()).await {
            Ok(_) => {
                probe_gateway.mark_provider_cas_verified();
                eprintln!("replica_control_head_cas_verified");
            }
            Err(_) => {
                eprintln!("replica_control_head_cas_verification_failed");
            }
        }
    });
    let storage_bind =
        env::var("LAKEDAY_REPLICA_STORAGE_BIND").unwrap_or_else(|_| "0.0.0.0".to_owned());
    let storage_port =
        env::var("LAKEDAY_REPLICA_STORAGE_PORT").unwrap_or_else(|_| "9090".to_owned());
    let storage_address = format_address(&storage_bind, &storage_port);
    let gateway_address = gateway_listen_address()?;
    let storage_listener = TcpListener::bind(&storage_address).await?;
    let gateway_listener = TcpListener::bind(&gateway_address).await?;
    let storage_app = node_router(Arc::clone(&node), root_key, Some(&internal_token))?;
    let gateway_app = gateway_router(Arc::clone(&gateway));
    let result = tokio::select! {
        result = axum::serve(storage_listener, storage_app) => result.map_err(Into::into),
        result = axum::serve(gateway_listener, gateway_app) => result.map_err(Into::into),
        result = archive_loop(Arc::clone(&gateway), Arc::clone(&node)) => result,
        result = shutdown_signal() => result.map_err(Into::into),
    };
    result
}

fn direct_nodes() -> Result<Vec<crate::ReplicaNode>, Box<dyn std::error::Error>> {
    if let Ok(value) = env::var("LAKEDAY_REPLICA_MEMBERS")
        && !value.trim().is_empty()
    {
        return Ok(parse_nodes(&value)?);
    }
    // An existing control file is authoritative and can bootstrap a restarted
    // Machine without re-supplying the environment list. An empty list lets
    // the direct constructor load that file and rejects a first boot clearly.
    Ok(Vec::new())
}

fn gateway_listen_address() -> Result<String, Box<dyn std::error::Error>> {
    if let Ok(listen) = env::var("LAKEDAY_GATEWAY_LISTEN")
        && !listen.trim().is_empty()
    {
        return Ok(listen);
    }
    let bind = env::var("LAKEDAY_GATEWAY_BIND").unwrap_or_else(|_| "0.0.0.0".to_owned());
    let port = env::var("LAKEDAY_GATEWAY_PORT").unwrap_or_else(|_| "30080".to_owned());
    Ok(format_address(&bind, &port))
}

fn format_address(bind: &str, port: &str) -> String {
    if bind.starts_with('[') {
        format!("{bind}:{port}")
    } else if bind.parse::<IpAddr>().is_ok_and(|ip| ip.is_ipv6()) {
        format!("[{bind}]:{port}")
    } else {
        format!("{bind}:{port}")
    }
}

fn build_archive() -> Result<OpaqueArchive, Box<dyn std::error::Error>> {
    let bucket = required_nonempty("LAKEDAY_REPLICA_ARCHIVE_BUCKET")?;
    let prefix = required_nonempty("LAKEDAY_REPLICA_ARCHIVE_PREFIX")?;
    let batch_records = required_nonempty("LAKEDAY_REPLICA_ARCHIVE_BATCH_RECORDS")?
        .parse::<usize>()
        .map_err(|_| "LAKEDAY_REPLICA_ARCHIVE_BATCH_RECORDS must be a positive integer")?;
    let store = AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .build()?;
    Ok(OpaqueArchive::new(Arc::new(store), prefix, batch_records)?)
}

async fn archive_loop(
    gateway: Arc<ReplicaGateway>,
    node: Arc<DiskReplica>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
    loop {
        interval.tick().await;
        if let Err(error) = gateway.archive_local_commits(&node).await {
            eprintln!("replica archive pass failed: {error}");
        }
    }
}

/// Reads one required cloud-owned setting without a hidden default.
fn required(name: &str) -> Result<String, Box<dyn std::error::Error>> {
    env::var(name).map_err(|_| format!("{name} is required").into())
}

/// Private node/gateway traffic must always have an explicit credential.
fn required_nonempty(name: &str) -> Result<String, Box<dyn std::error::Error>> {
    let value = required(name)?;
    if value.trim().is_empty() {
        return Err(format!("{name} must not be empty").into());
    }
    Ok(value)
}

/// Kubernetes terminates containers with SIGTERM. Exit promptly after stopping
/// listeners; acknowledged replica appends have already crossed their fsync boundary.
async fn shutdown_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! { result = tokio::signal::ctrl_c() => result, _ = terminate.recv() => Ok(()) }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await
    }
}
