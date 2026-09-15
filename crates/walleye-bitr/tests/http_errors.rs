//! Gateway protocol failures retain bounded diagnostics on append and recovery.

use axum::{Json, Router, http::StatusCode, routing::any};
use serde_json::json;
use walleye_bitr::{EncryptedRecord, HttpReplica, ReplicaError, ReplicaGateway};

#[tokio::test]
async fn recognized_http_400_retains_the_gateway_diagnostic() {
    for code in ["invalid_watermark", "recovery_failed"] {
        let app = Router::new().fallback(any(move || async move {
            (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "code":code, "retryable":false,
                    "error":"record LSN 7 is not after committed LSN 9"
                })),
            )
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("server");
        });
        let gateway = HttpReplica::new(format!("http://{address}"), "test-token");
        let record: EncryptedRecord = serde_json::from_value(json!({
            "stream":"tenant-a/test", "writer_epoch":1, "lsn":7, "committed_lsn":6,
            "nonce":vec![3;24], "ciphertext":vec![5;16], "authentication":vec![7;32]
        }))
        .expect("record");
        let recovery = gateway
            .recover("tenant-a/test", 0)
            .await
            .expect_err("rejection");
        let append = gateway.append(record).await.expect_err("rejection");
        server.abort();
        for error in [recovery, append] {
            assert!(
                matches!(error, ReplicaError::Protocol(ref detail) if detail.contains("record LSN 7 is not after committed LSN 9")),
                "lost gateway diagnostic: {error}"
            );
        }
    }
}
