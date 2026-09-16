//! Three env-style members on one root: every node serves the full API, each
//! stream has exactly one owner on the ring, non-owners forward, and writes
//! through any node are neither lost nor duplicated.
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use std::{collections::BTreeSet, sync::Arc};
use walleye_node::{ApiConfig, Config, Service, cluster, router};
use walleye_ring::{Node, Ring};

const TOKEN: &str = "deployment-secret-token";

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("node", DataType::Utf8, true),
    ]))
}
fn ipc(rows: &[(i64, &str)]) -> Vec<u8> {
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))) as ArrayRef,
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
        ],
    )
    .unwrap();
    let mut out = Vec::new();
    let mut w = arrow_ipc::writer::StreamWriter::try_new(&mut out, &schema()).unwrap();
    w.write(&batch).unwrap();
    w.finish().unwrap();
    out
}
fn ids(bytes: &[u8]) -> Vec<i64> {
    let reader = arrow_ipc::reader::FileReader::try_new(std::io::Cursor::new(bytes), None).unwrap();
    let mut out = Vec::new();
    for b in reader {
        let b = b.unwrap();
        let col = b.column_by_name("id").unwrap();
        out.extend(
            col.as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .flatten(),
        );
    }
    out
}

struct Member {
    id: String,
    base: String,
    service: Arc<Service>,
}

async fn start(root: &std::path::Path, count: usize) -> Vec<Member> {
    let mut listeners = Vec::new();
    for i in 0..count {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        listeners.push((format!("n{i}"), format!("http://{addr}"), listener));
    }
    let members: Vec<Node> = listeners
        .iter()
        .map(|(id, endpoint, _)| Node::new(id.clone(), endpoint.clone(), 1.0).unwrap())
        .collect();
    let mut out = Vec::new();
    for (id, base, listener) in listeners {
        let config = Config {
            node_id: id.clone(),
            listen: base.clone(),
            directory: root.join("cache").join(&id),
            memory_bytes: 1024 * 1024 * 1024,
            disk_bytes: 64 * 1024 * 1024,
            token: TOKEN.into(),
            bitr: false,
            members: members.clone(),
            kubernetes: None,
            processor: None,
            api: Some(ApiConfig {
                root_uri: format!("file://{}/store", root.display()),
                bitr_url: None,
            }),
        };
        let service = Service::open(config).await.unwrap();
        let app = router(service.clone());
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        out.push(Member { id, base, service });
    }
    out
}

#[tokio::test]
async fn every_member_serves_every_stream_with_one_owner() {
    let dir = tempfile::tempdir().unwrap();
    let members = start(dir.path(), 3).await;
    let client = reqwest::Client::new();
    let ring = Ring::new(
        members
            .iter()
            .map(|m| Node::new(m.id.clone(), m.base.clone(), 1.0).unwrap())
            .collect(),
    )
    .unwrap();
    let tables: Vec<String> = (0..6).map(|i| format!("t{i}")).collect();
    let owners: BTreeSet<String> = tables
        .iter()
        .map(|t| ring.owner(t.as_bytes()).id.clone())
        .collect();
    assert!(
        owners.len() >= 2,
        "six tables should spread over several owners: {owners:?}"
    );

    // Create every table through node 0, whoever owns it.
    for table in &tables {
        let r = client
            .post(format!(
                "{}/v1/table/{table}/create/?mode=create",
                members[0].base
            ))
            .header("x-api-key", TOKEN)
            .header("content-type", "application/vnd.apache.arrow.stream")
            .body(ipc(&[]))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200, "{}", r.text().await.unwrap());
    }

    // Write 30 rows per table, round-robin across members; every id is unique.
    for table in &tables {
        for i in 0..30i64 {
            let member = &members[(i as usize) % members.len()];
            let r = client
                .post(format!("{}/v1/table/{table}/insert/", member.base))
                .header("x-api-key", TOKEN)
                .header("content-type", "application/vnd.apache.arrow.stream")
                .body(ipc(&[(i, &member.id)]))
                .send()
                .await
                .unwrap();
            let expected_owner = ring.owner(table.as_bytes()).id.clone();
            let served_by = r
                .headers()
                .get(cluster::OWNER_HEADER)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            let status = r.status();
            let body = r.text().await.unwrap();
            assert_eq!(status, 200, "{table} via {}: {body}", member.id);
            assert_eq!(served_by, expected_owner, "{table} via {}", member.id);
        }
    }

    // Every member reports the same complete, duplicate-free contents.
    for table in &tables {
        for member in &members {
            let r = client
                .post(format!("{}/v1/table/{table}/count_rows/", member.base))
                .header("x-api-key", TOKEN)
                .json(&serde_json::json!({}))
                .send()
                .await
                .unwrap();
            assert_eq!(r.status(), 200);
            assert_eq!(
                r.json::<u64>().await.unwrap(),
                30,
                "{table} via {}",
                member.id
            );
            let r = client
                .post(format!("{}/v1/table/{table}/query/", member.base))
                .header("x-api-key", TOKEN)
                .json(&serde_json::json!({"k": i64::MAX as u64 - 1, "vector": [], "prefilter": true, "version": null}))
                .send()
                .await
                .unwrap();
            assert_eq!(r.status(), 200);
            let mut got = ids(&r.bytes().await.unwrap());
            got.sort();
            assert_eq!(
                got,
                (0..30).collect::<Vec<_>>(),
                "{table} via {}",
                member.id
            );
        }
    }

    // SQL over one table routes to its owner from any member; a join across
    // tables with different owners is refused rather than answered wrong.
    let (a, b) = {
        let mut by_owner = std::collections::BTreeMap::new();
        for t in &tables {
            by_owner
                .entry(ring.owner(t.as_bytes()).id.clone())
                .or_insert_with(Vec::new)
                .push(t.clone());
        }
        let mut groups = by_owner.into_values();
        (
            groups.next().unwrap()[0].clone(),
            groups.next().unwrap()[0].clone(),
        )
    };
    for member in &members {
        let r = client
            .post(format!("{}/v1/query", member.base))
            .header("authorization", format!("Bearer {TOKEN}"))
            .json(&serde_json::json!({"sql": format!("SELECT count(*) AS n FROM {a}")}))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
        assert_eq!(r.json::<serde_json::Value>().await.unwrap()[0]["n"], 30);
    }
    let r = client
        .post(format!("{}/v1/query", members[0].base))
        .header("authorization", format!("Bearer {TOKEN}"))
        .json(&serde_json::json!({"sql": format!("SELECT count(*) FROM {a}, {b}")}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);

    // A forward that carries a stale membership view is fenced, and a
    // forwarded request that lands on a non-owner is refused instead of
    // bouncing around.
    let table = &tables[0];
    let non_owner = members
        .iter()
        .find(|m| m.id != ring.owner(table.as_bytes()).id)
        .unwrap();
    let r = client
        .post(format!("{}/v1/table/{table}/count_rows/", non_owner.base))
        .header("x-api-key", TOKEN)
        .header(cluster::MEMBERS_HEADER, "0000000000000000")
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 409);
    let r = client
        .post(format!("{}/v1/table/{table}/count_rows/", non_owner.base))
        .header("x-api-key", TOKEN)
        .header(cluster::FORWARDED_HEADER, "1")
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 409);

    // Listing is served locally by every member from the shared catalog.
    let r = client
        .get(format!("{}/v1/table/", members[2].base))
        .header("x-api-key", TOKEN)
        .send()
        .await
        .unwrap();
    let listed = r.json::<serde_json::Value>().await.unwrap();
    assert_eq!(listed["tables"].as_array().unwrap().len(), 6);

    for member in members {
        member.service.close().await;
    }
}
