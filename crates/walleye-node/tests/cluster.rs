//! Three env-style members on one root: every node serves the full API, each
//! table has exactly one owner - the process that claimed it first - and
//! non-owners forward, so writes through any node are neither lost nor
//! duplicated.
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use std::{collections::BTreeSet, sync::Arc};
use walleye_node::{ApiConfig, Config, Service, cluster, router};
use walleye_ring::Node;

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

/// The ownership session a member runs under, which is what the owner header
/// names.
async fn session(member: &Member) -> String {
    reqwest::Client::new()
        .get(format!("{}/internal/ownership", member.base))
        .header("authorization", format!("Bearer {TOKEN}"))
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap()["node"]
        .as_str()
        .unwrap()
        .to_owned()
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
            lease: Default::default(),
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
    let tables: Vec<String> = (0..6).map(|i| format!("t{i}")).collect();
    let mut sessions = Vec::new();
    for member in &members {
        sessions.push(session(member).await);
    }
    // A table belongs to the process that claims it first, so creating table
    // i through member i % 3 spreads them over all three.
    let owner_of = |table: &str| -> String {
        let index: usize = table[1..].parse().unwrap();
        sessions[index % members.len()].clone()
    };
    let owners: BTreeSet<String> = tables.iter().map(|t| owner_of(t)).collect();
    assert_eq!(owners.len(), 3, "six tables over three owners: {owners:?}");

    for (index, table) in tables.iter().enumerate() {
        let r = client
            .post(format!(
                "{}/v1/table/{table}/create/?mode=create",
                members[index % members.len()].base
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
            let expected_owner = owner_of(table);
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

    // SQL over one table routes to its owner from any member. t0 and t1 have
    // different owners.
    let (a, b) = (tables[0].clone(), tables[1].clone());
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
    // SQL spanning members runs on the node that received it, which gathers
    // the rows it does not own from their owners. Every member gives the
    // same answer.
    for member in &members {
        let r = client
            .post(format!("{}/v1/query", member.base))
            .header("authorization", format!("Bearer {TOKEN}"))
            .json(&serde_json::json!({
                "sql": format!(
                    "SELECT (SELECT count(*) FROM {a}) + (SELECT count(*) FROM {b}) AS n"
                )
            }))
            .send()
            .await
            .unwrap();
        let status = r.status();
        let body = r.json::<serde_json::Value>().await.unwrap();
        assert_eq!(status, 200, "{body}");
        assert_eq!(body[0]["n"], 60, "via {}: {body}", member.id);
    }
    // A join across members sees every row of both tables.
    let r = client
        .post(format!("{}/v1/query", members[0].base))
        .header("authorization", format!("Bearer {TOKEN}"))
        .json(&serde_json::json!({
            "sql": format!("SELECT count(*) AS n FROM {a} JOIN {b} ON {a}.id = {b}.id")
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.json::<serde_json::Value>().await.unwrap()[0]["n"], 30);

    // A forwarded request that lands on a non-owner is refused, and says why,
    // instead of bouncing around.
    let table = &tables[0];
    let non_owner = &members[1];
    let r = client
        .post(format!("{}/v1/table/{table}/count_rows/", non_owner.base))
        .header("x-api-key", TOKEN)
        .header(cluster::FORWARDED_HEADER, "1")
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 409);
    assert_eq!(
        r.headers().get(cluster::ROUTE_ERROR_HEADER).unwrap(),
        "stale-owner"
    );

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

/// A write that reaches the wrong node is not the caller's mistake: the same
/// call to the owner works. It has to be answerable by retrying against the
/// owner, so it is a 409 rather than a 400, which nothing retries.
///
/// This is the status any handover protocol would be built on, and it used to
/// differ between the two write surfaces.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_to_a_non_owner_is_a_conflict_rather_than_a_bad_request() {
    let d = tempfile::tempdir().unwrap();
    let members = start(d.path(), 3).await;

    // Define through one member, which claims it, so the stream exists
    // everywhere and has an owner.
    let owning = &members[0];
    let defined = reqwest::Client::new()
        .post(format!("{}/v1/streams", owning.base))
        .header("authorization", format!("Bearer {TOKEN}"))
        .json(&serde_json::json!({
            "name": "journal",
            "primary_key": ["id"],
            "columns": [{"name":"id","type":"int64"},{"name":"node","type":"string"}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(defined.status(), reqwest::StatusCode::OK);

    // Ask a non-owner directly, with the forwarding middleware told this
    // request was already forwarded once so it refuses to hop again. That is
    // the path where the engine's own not-owner answer reaches the client.
    let other = &members[1];
    let answered = reqwest::Client::new()
        .post(format!("{}/v1/streams/journal/events", other.base))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("x-walleye-forwarded", "1")
        .json(&serde_json::json!({"rows":[{"id":1,"node":"x"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        answered.status(),
        reqwest::StatusCode::CONFLICT,
        "a caller sent to the wrong node should retry, not give up: {}",
        answered.text().await.unwrap_or_default()
    );

    for m in members {
        m.service.close().await;
    }
}
