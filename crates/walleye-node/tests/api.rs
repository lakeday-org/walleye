//! One deployment token gates both public streams and private peer operations.
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use tower::ServiceExt;
use walleye_node::{ApiConfig, Config, Service, router};
use walleye_ring::Node;
fn config(path: &std::path::Path, api: bool) -> Config {
    Config {
        node_id: "n".into(),
        listen: "127.0.0.1:0".into(),
        directory: path.join("cache"),
        memory_bytes: 1024 * 1024 * 1024,
        disk_bytes: 64 * 1024 * 1024,
        token: "deployment-secret-token".into(),
        bitr: false,
        members: vec![Node::new("n", "http://n", 1.0).unwrap()],
        kubernetes: None,
        processor: None,
        api: api.then(|| ApiConfig {
            root_uri: format!("file://{}/store", path.display()),
            bitr_url: None,
        }),
    }
}
async fn call(app: &axum::Router, path: &str, body: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .header("content-type", "application/json")
                .header("authorization", "Bearer deployment-secret-token")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 8 * 1024 * 1024)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}
#[tokio::test]
async fn invalid_token_rejected_before_cache_or_stream_access() {
    let d = tempfile::tempdir().unwrap();
    let service = Service::open(config(d.path(), false)).await.unwrap();
    let app = router(service.clone());
    for (method, path, body) in [
        ("GET", "/internal/cache/stats", ""),
        (
            "PUT",
            "/internal/cache/01010101010101010101010101010101",
            "payload",
        ),
        ("POST", "/v1/query", r#"{"sql":"SELECT 1"}"#),
    ] {
        let r = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .header("content-type", "application/json")
                    .header("authorization", "Bearer incorrect-token")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
    }
    service.close().await;
}
#[tokio::test]
async fn peer_miss_does_not_fill_and_persisted_entries_survive_restart() {
    let d = tempfile::tempdir().unwrap();
    let service = Service::open(config(d.path(), false)).await.unwrap();
    let key = lance_core::cache::InternalCacheKey::from_bytes([9; 16]);
    assert!(service.cache.export_entry(&key).await.is_none());
    let mut envelope = 1u64.to_le_bytes().to_vec();
    envelope.push(42);
    assert!(
        service
            .cache
            .import_entry(&key, envelope.clone().into())
            .await
    );
    service.cache.flush().await;
    service.close().await;
    drop(service);
    let service = Service::open(config(d.path(), false)).await.unwrap();
    assert_eq!(
        service.cache.export_entry(&key).await.unwrap().as_ref(),
        envelope
    );
    service.close().await;
}
#[tokio::test]
async fn define_ingest_sql_and_reopen_preserve_data_and_reject_invalid_requests() {
    let d = tempfile::tempdir().unwrap();
    let service = Service::open(config(d.path(), true)).await.unwrap();
    let app = router(service.clone());
    let definition = json!({"name":"events","columns":[{"name":"id","type":"int64"},{"name":"city","type":"string"},{"name":"value","type":"float64"}],"primary_key":["id"]});
    assert_eq!(
        call(&app, "/v1/streams", definition.clone()).await.0,
        StatusCode::OK
    );
    assert_eq!(
        call(&app, "/v1/streams", definition).await.0,
        StatusCode::OK
    );
    let ingest=call(&app,"/v1/streams/events/events",json!({"rows":[{"id":1,"city":"a","value":2.0},{"id":2,"city":"a","value":3.0},{"id":3,"city":"b","value":7.0}]})).await;
    assert_eq!(ingest.0, StatusCode::OK, "{ingest:?}");
    let result = call(
        &app,
        "/v1/query",
        json!({"sql":"SELECT city, sum(value) AS total FROM events WHERE id <= 2 GROUP BY city"}),
    )
    .await;
    assert_eq!(result.0, StatusCode::OK, "{result:?}");
    assert_eq!(result.1, json!([{"city":"a","total":5.0}]));
    for bad in [
        json!({"rows":[{"id":"wrong","city":"a","value":1}]}),
        json!({"rows":[{"id":4,"city":"a","value":1,"secret":5}]}),
    ] {
        assert_eq!(
            call(&app, "/v1/streams/events/events", bad).await.0,
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(
        call(
            &app,
            "/v1/query",
            json!({"sql":"CREATE TABLE nope AS SELECT 1"})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(&app, "/v1/query", json!({"sql":"SELECT * FROM missing"}))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    // SQL joins operate on independently captured stream snapshots.
    assert_eq!(call(&app, "/v1/streams", json!({"name":"cities","columns":[{"name":"city","type":"string"},{"name":"region","type":"string"}],"primary_key":["city"]})).await.0, StatusCode::OK);
    assert_eq!(
        call(
            &app,
            "/v1/streams/cities/events",
            json!({"rows":[{"city":"a","region":"west"},{"city":"b","region":"east"}]})
        )
        .await
        .0,
        StatusCode::OK
    );
    let joined=call(&app,"/v1/query",json!({"sql":"SELECT c.region, count(*) AS n FROM events e JOIN cities c ON e.city = c.city GROUP BY c.region ORDER BY c.region"})).await;
    assert_eq!(
        joined,
        (
            StatusCode::OK,
            json!([{"region":"east","n":1},{"region":"west","n":2}])
        )
    );
    service.close().await;
    drop(app);
    drop(service);
    let service = Service::open(config(d.path(), true)).await.unwrap();
    let app = router(service.clone());
    let result = call(
        &app,
        "/v1/query",
        json!({"sql":"SELECT count(*) AS n FROM events"}),
    )
    .await;
    assert_eq!(result, (StatusCode::OK, json!([{"n":3}])));
    service.close().await;
}

/// Concurrent stream processors cannot commit against a superseded read snapshot.
#[tokio::test]
async fn conditional_ingestion_rejects_stale_readers_and_previous_boots() {
    let d = tempfile::tempdir().unwrap();
    let service = Service::open(config(d.path(), true)).await.unwrap();
    let app = router(service.clone());
    let definition = json!({"name":"journal","columns":[{"name":"entry","type":"string"},{"name":"data","type":"string","nullable":true}],"primary_key":["entry"]});
    assert_eq!(
        call(&app, "/v1/streams", definition).await.0,
        StatusCode::OK
    );
    let snapshot = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/query")
                .header("authorization", "Bearer deployment-secret-token")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"sql":"SELECT * FROM journal"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let revision = snapshot
        .headers()
        .get("etag")
        .expect("snapshot revision")
        .to_str()
        .unwrap()
        .to_owned();
    let append = |value: &str| {
        Request::builder()
            .method("POST")
            .uri("/v1/streams/journal/events")
            .header("authorization", "Bearer deployment-secret-token")
            .header("content-type", "application/json")
            .header("if-match", &revision)
            .body(Body::from(
                json!({"rows":[{"entry":"job","data":value}]}).to_string(),
            ))
            .unwrap()
    };
    let (a, b) = tokio::join!(
        app.clone().oneshot(append("first")),
        app.clone().oneshot(append("second"))
    );
    let mut codes = [a.unwrap().status(), b.unwrap().status()];
    codes.sort();
    assert_eq!(codes, [StatusCode::OK, StatusCode::PRECONDITION_FAILED]);
    let rows = call(&app, "/v1/query", json!({"sql":"SELECT * FROM journal"}))
        .await
        .1;
    assert_eq!(rows.as_array().unwrap().len(), 1);
    service.close().await;
    drop(app);
    drop(service);
    let reopened = Service::open(config(d.path(), true)).await.unwrap();
    let app = router(reopened.clone());
    assert_eq!(
        app.clone()
            .oneshot(append("stale boot"))
            .await
            .unwrap()
            .status(),
        StatusCode::PRECONDITION_FAILED
    );
    assert_eq!(
        call(&app, "/v1/query", json!({"sql":"SELECT * FROM journal"}))
            .await
            .1,
        rows
    );
    reopened.close().await;
}

#[tokio::test]
async fn stream_processor_recovers_due_work_and_retries_failed_callbacks() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let d = tempfile::tempdir().unwrap();
    let service = Service::open(config(d.path(), true)).await.unwrap();
    let app = router(service.clone());
    assert_eq!(call(&app, "/v1/streams", json!({"name":"work","columns":[{"name":"key","type":"string"},{"name":"available_at","type":"int64"}],"primary_key":["key"]})).await.0, StatusCode::OK);
    assert_eq!(
        call(
            &app,
            "/v1/streams/work/events",
            json!({"rows":[{"key":"repository_123","available_at":0}]})
        )
        .await
        .0,
        StatusCode::OK
    );
    drop(app);
    service.close().await;
    drop(service);
    let service = Service::open(config(d.path(), true)).await.unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(tokio::sync::Notify::new());
    let callback_release = release.clone();
    let observed = count.clone();
    let callback_service = service.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/consume/", listener.local_addr().unwrap());
    let callback = axum::Router::new().route(
        "/consume/{key}",
        axum::routing::post(
            move |axum::extract::Path(key): axum::extract::Path<String>,
                  headers: axum::http::HeaderMap| {
                let release = callback_release.clone();
                let count = observed.clone();
                let service = callback_service.clone();
                async move {
                    assert_eq!(key, "repository_123");
                    assert_eq!(headers["x-lakeday-worker-ingress"], "host-only-token");
                    if count.fetch_add(1, Ordering::SeqCst) == 0 {
                        return StatusCode::SERVICE_UNAVAILABLE;
                    }
                    release.notified().await;
                    let result = call(
                        &router(service),
                        "/v1/streams/work/events",
                        json!({"rows":[{"key":key,"available_at":i64::MAX}]}),
                    )
                    .await;
                    assert_eq!(result.0, StatusCode::OK);
                    StatusCode::OK
                }
            },
        ),
    );
    let server = tokio::spawn(async move { axum::serve(listener, callback).await.unwrap() });
    let config: walleye_node::ProcessorConfig=serde_json::from_value(json!({"query":"SELECT key, available_at FROM work", "endpoint":endpoint,"headers":{"x-lakeday-worker-ingress":"host-only-token"},"retry_ms":20,"poll_ms":20})).unwrap();
    let task_service = service.clone();
    let task = tokio::spawn(async move { task_service.process(config).await.unwrap() });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while count.load(Ordering::SeqCst) < 2 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("persisted due work retried after restart");
    service.quiesce();
    assert!(
        !task.is_finished(),
        "active callbacks must drain before shutdown"
    );
    release.notify_one();
    assert_eq!(
        count.load(Ordering::SeqCst),
        2,
        "future work must not run repeatedly"
    );
    tokio::time::timeout(std::time::Duration::from_secs(1), task)
        .await
        .expect("processor stops on quiesce")
        .unwrap();
    server.abort();
    let _ = server.await;
    service.close().await;
}
