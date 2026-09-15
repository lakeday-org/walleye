//! Executable acceptance workload against actual S3 and optional three-node Bitr/Foyer services.
use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use base64::Engine;
use datafusion_execution::memory_pool::MemoryConsumer;
use futures::TryStreamExt;
use hmac::{Hmac, Mac};
use lance_io::object_store::{ObjectStoreParams, StorageOptionsAccessor};
use object_store::{
    ObjectStore, ObjectStoreExt,
    aws::{AmazonS3Builder, S3ConditionalPut},
};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, sync::Arc, time::Duration};
use walleye_bitr::{HttpReplica, QuorumWriter};
use walleye_cache::PeerConfig;
use walleye_lance::{BitrWalBackend, CachedStorage, LanceDurability, Table, TableConfig};
use walleye_ring::{Membership, Node};

type Error = Box<dyn std::error::Error>;
fn required(key: &str) -> Result<String, Error> {
    std::env::var(key).map_err(Into::into)
}
fn token(root: &[u8], tenant: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(root).expect("valid HMAC key");
    mac.update(
        format!("lakeday-cloud/deployment-identity/v1\0{tenant}\0replica-gateway-authentication")
            .as_bytes(),
    );
    hex::encode(mac.finalize().into_bytes())
}
fn assert_rows(batches: &[RecordBatch], expected: usize) -> Result<(), Error> {
    let mut ids = Vec::new();
    for batch in batches {
        let array = batch
            .column_by_name("id")
            .ok_or("missing id")?
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("id type")?;
        ids.extend(array.values().iter().copied());
    }
    ids.sort();
    if ids != (0..expected as i64).collect::<Vec<_>>() {
        return Err(format!("rows differ: expected {expected}, received {}", ids.len()).into());
    }
    Ok(())
}
#[tokio::main]
async fn main() -> Result<(), Error> {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "single".into());
    if mode != "single" && mode != "cluster" {
        return Err("usage: walleye-workload single|cluster".into());
    }
    let endpoint = required("AWS_ENDPOINT")?;
    let bucket = required("WALLEYE_BUCKET")?;
    let prefix = required("WALLEYE_RUN_ID")?;
    let options = HashMap::from([
        ("aws_access_key_id".into(), required("AWS_ACCESS_KEY_ID")?),
        (
            "aws_secret_access_key".into(),
            required("AWS_SECRET_ACCESS_KEY")?,
        ),
        ("aws_region".into(), "us-east-1".into()),
        ("aws_endpoint".into(), endpoint.clone()),
        (
            "allow_http".into(),
            endpoint.starts_with("http://").to_string(),
        ),
    ]);
    let params = ObjectStoreParams {
        storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
            options,
        ))),
        ..Default::default()
    };
    let object_store = AmazonS3Builder::from_env()
        .with_bucket_name(&bucket)
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .build()?;
    // Prove the actual bucket enforces both create and update CAS before testing the engine.
    let path = object_store::path::Path::from(format!("{prefix}/cas-probe"));
    let first = object_store
        .put_opts(
            &path,
            b"one".to_vec().into(),
            object_store::PutOptions {
                mode: object_store::PutMode::Create,
                ..Default::default()
            },
        )
        .await?;
    assert!(matches!(
        object_store
            .put_opts(
                &path,
                b"two".to_vec().into(),
                object_store::PutOptions {
                    mode: object_store::PutMode::Create,
                    ..Default::default()
                }
            )
            .await,
        Err(object_store::Error::AlreadyExists { .. })
    ));
    let old = object_store::UpdateVersion {
        e_tag: first.e_tag,
        version: first.version,
    };
    object_store
        .put_opts(
            &path,
            b"two".to_vec().into(),
            object_store::PutOptions {
                mode: object_store::PutMode::Update(old.clone()),
                ..Default::default()
            },
        )
        .await?;
    assert!(
        object_store
            .put_opts(
                &path,
                b"stale".to_vec().into(),
                object_store::PutOptions {
                    mode: object_store::PutMode::Update(old),
                    ..Default::default()
                }
            )
            .await
            .is_err()
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let nodes = if mode == "cluster" {
        required("WALLEYE_CACHE_NODES")?
            .split(',')
            .enumerate()
            .map(|(i, u)| Node::new(format!("node-{}", i + 1), u, 1.0))
            .collect::<Result<Vec<_>, _>>()?
    } else {
        vec![]
    };
    let tenant = "walleye";
    let peer_token =
        std::env::var("WALLEYE_CACHE_TOKEN").unwrap_or_else(|_| "acceptance-walleye-token".into());
    let http = reqwest::Client::new();
    let mut initial_node_stats = Vec::new();
    for node in &nodes {
        let stats: serde_json::Value = http
            .get(format!("{}/internal/cache/stats", node.endpoint))
            .bearer_auth(&peer_token)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        initial_node_stats.push(stats);
    }
    let peer = if mode == "cluster" {
        Some(PeerConfig {
            token: peer_token.clone(),
            ring: Arc::new(Membership::new(nodes.clone())?),
        })
    } else {
        None
    };
    let dir = std::env::var("WALLEYE_WORK_DIR").unwrap_or_else(|_| "/tmp/walleye-workload".into());
    let cache = CachedStorage::open(
        format!("{dir}/{prefix}/{mode}"),
        tenant,
        32 * 1024 * 1024,
        128 * 1024 * 1024,
        params.clone(),
        peer.clone(),
    )
    .await?;
    let config = TableConfig::new(
        format!("table_{}", prefix.replace('-', "_")),
        format!("s3://{bucket}/{prefix}/{mode}/table"),
        schema.clone(),
        vec!["id".into()],
    )?;
    let stream_id = config.stream.clone();
    let backend = if mode == "cluster" {
        let root = base64::engine::general_purpose::STANDARD
            .decode(required("LAKEDAY_DATAPLANE_ROOT_KEY")?)?;
        Some(Arc::new(BitrWalBackend::new(
            Arc::new(QuorumWriter::new(
                Arc::new(HttpReplica::new(
                    required("WALLEYE_BITR_URL")?,
                    token(&root, tenant),
                )),
                [7; 32],
            )),
            &config.stream,
            config.shard_id,
            1,
        )?))
    } else {
        None
    };
    let durability = backend
        .clone()
        .map(LanceDurability::Bitr)
        .unwrap_or(LanceDurability::ObjectStore);
    let mut table = Table::open(config.clone(), cache.storage.clone(), durability).await?;
    let expected = 8192usize;
    for start in (0..expected).step_by(1024) {
        let ids = Arc::new(Int64Array::from(
            (start as i64..(start + 1024) as i64).collect::<Vec<_>>(),
        ));
        let values = Arc::new(StringArray::from(
            (start..start + 1024)
                .map(|n| {
                    format!(
                        "row-{n:08}-{}",
                        (0..4)
                            .map(|part| format!("{:x}", Sha256::digest(format!("{n}:{part}"))))
                            .collect::<String>()
                    )
                })
                .collect::<Vec<_>>(),
        ));
        table
            .append(vec![RecordBatch::try_new(
                schema.clone(),
                vec![ids, values],
            )?])
            .await?;
    }
    assert_rows(
        &table
            .scan(None, expected + 1)
            .await
            .map_err(|e| format!("scan at workload line {}: {e}", line!()))?,
        expected,
    )?;
    let knowledge = if let Some(b) = &backend {
        let k = b.commit_knowledge().await?;
        assert!(k.committed_lsn >= 8 && k.certificate.as_ref().is_some_and(|s| !s.is_empty()));
        Some(k)
    } else {
        None
    };
    table.checkpoint().await?;
    if let Some(backend) = &backend {
        assert_eq!(
            backend.retained_wal_bytes().await,
            0,
            "checkpointed WAL payloads must leave the adapter's memory"
        );
    }
    assert_rows(
        &table
            .scan(None, expected + 1)
            .await
            .map_err(|e| format!("scan at workload line {}: {e}", line!()))?,
        expected,
    )?;
    table.close().await?;
    // Reopen after checkpoint to force reads of real S3 Lance files through Foyer.
    let durability = if let Some(b) = &backend {
        LanceDurability::Bitr(b.clone())
    } else {
        LanceDurability::ObjectStore
    };
    let mut table = Table::open(config, cache.storage.clone(), durability).await?;
    assert_rows(
        &table
            .scan(None, expected + 1)
            .await
            .map_err(|e| format!("scan at workload line {}: {e}", line!()))?,
        expected,
    )?;
    let before = cache.peers.as_ref().map(|s| s.snapshot());
    for _ in 0..3 {
        assert_rows(
            &table
                .scan(None, expected + 1)
                .await
                .map_err(|e| format!("scan at workload line {}: {e}", line!()))?,
            expected,
        )?;
    }
    let after = cache.peers.as_ref().map(|s| s.snapshot());
    if let (Some(b), Some(a)) = (&before, &after) {
        assert!(
            a["peer_hits"].as_u64().unwrap() > b["peer_hits"].as_u64().unwrap(),
            "warm query must hit actual peer Foyer entries"
        );
        assert!(a["peer_stores"].as_u64().unwrap() > 0);
    }
    table.close().await?;
    // Test live cache reclamation using the same resource manager as query execution.
    let runtime = cache.resources.runtime()?;
    let reservation = MemoryConsumer::new("acceptance-query").register(&runtime.memory_pool);
    let initial = cache.backend.memory_capacity();
    reservation.try_grow(24 * 1024 * 1024)?;
    assert!(cache.backend.memory_capacity() < initial);
    drop(reservation);
    assert_eq!(cache.backend.memory_capacity(), initial);
    let spill = runtime.disk_manager.create_tmp_file("acceptance")?;
    assert_eq!(
        cache.backend.persistent_capacity(),
        cache.backend.disk_floor()
    );
    drop(spill);
    assert_eq!(cache.backend.persistent_capacity(), 128 * 1024 * 1024);
    let mut node_stats = Vec::new();
    for (node, initial) in nodes.iter().zip(&initial_node_stats) {
        let base = &node.endpoint;
        let wrong = http
            .get(format!("{base}/internal/cache/stats"))
            .bearer_auth("incorrect-deployment-token")
            .send()
            .await?;
        assert_eq!(wrong.status(), reqwest::StatusCode::UNAUTHORIZED);
        http.post(format!("{base}/internal/cache/flush"))
            .bearer_auth(&peer_token)
            .send()
            .await?
            .error_for_status()?;
        let stats: serde_json::Value = http
            .get(format!("{base}/internal/cache/stats"))
            .bearer_auth(&peer_token)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert!(
            stats["hits"].as_u64().unwrap() > initial["hits"].as_u64().unwrap()
                && stats["stores"].as_u64().unwrap() > initial["stores"].as_u64().unwrap(),
            "each of three Foyer nodes must serve and store entries"
        );
        let mut stats = stats;
        stats["workload_hits"] =
            serde_json::json!(stats["hits"].as_u64().unwrap() - initial["hits"].as_u64().unwrap());
        stats["workload_stores"] = serde_json::json!(
            stats["stores"].as_u64().unwrap() - initial["stores"].as_u64().unwrap()
        );
        node_stats.push(stats);
    }
    let objects: Vec<_> = object_store
        .list(Some(&object_store::path::Path::from(prefix.clone())))
        .try_collect()
        .await?;
    assert!(
        objects
            .iter()
            .any(|o| o.location.as_ref().ends_with(".lance")),
        "checkpoint must write real Lance files to S3"
    );
    let mut archive_segments = 0usize;
    let mut archived_positions = std::collections::BTreeSet::new();
    if mode == "cluster" {
        let archive = AmazonS3Builder::from_env()
            .with_bucket_name(required("WALLEYE_ARCHIVE_BUCKET")?)
            .build()?;
        let archive_prefix = object_store::path::Path::from(format!(
            "commits/streams/{:x}/segments",
            Sha256::digest(stream_id.as_bytes())
        ));
        let committed = knowledge
            .as_ref()
            .ok_or("missing commit proof")?
            .committed_lsn;
        for _ in 0..30 {
            let objects: Vec<_> = archive.list(Some(&archive_prefix)).try_collect().await?;
            archive_segments = objects
                .iter()
                .filter(|o| o.location.as_ref().contains("/segments/"))
                .count();
            for object in &objects {
                let bytes = archive.get(&object.location).await?.bytes().await?;
                let segment: serde_json::Value = serde_json::from_slice(&bytes)?;
                assert_eq!(segment["stream"], stream_id);
                for record in segment["records"]
                    .as_array()
                    .ok_or("archive segment missing records")?
                {
                    assert_eq!(record["stream"], stream_id);
                    archived_positions.insert(record["lsn"].as_u64().ok_or("record missing LSN")?);
                }
            }
            if (1..=committed).all(|n| archived_positions.contains(&n)) {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        assert!(
            (1..=committed).all(|n| archived_positions.contains(&n)),
            "Bitr must archive acknowledged log records to the S3 bucket"
        );
    }
    cache.backend.flush().await;
    let memory_used = cache.backend.memory_usage();
    cache.backend.close().await?;
    let report = serde_json::json!({"mode":mode,"rows":expected,"s3_cas":true,"hot_read":true,"checkpoint_read":true,"checkpoint_releases_wal_buffers":mode=="cluster","reopen_read":true,"memory_reclamation":true,"disk_reclamation":true,"authentication":mode=="cluster","peer_before":before,"peer_after":after,"nodes":node_stats,"commit":knowledge,"archive_segments":archive_segments,"archived_positions":archived_positions,"lance_objects":objects.len(),"local_cache_memory":memory_used});
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
