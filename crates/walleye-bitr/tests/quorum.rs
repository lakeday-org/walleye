//! Contract tests for the single quorum-gateway client.
//!
//! The client encrypts one envelope and makes one HTTP call. The gateway's
//! successful response is the quorum boundary; quorum fan-out and committed
//! tail selection stay on the gateway side. The default fixtures use
//! `MemoryReplica` or an in-process Axum router, so they do not prove cloud
//! deployment, physical replica durability, or cross-process recovery.

use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::routing::{get, post};
use serde::Deserialize;
use serde_json::json;
use walleye_bitr::{
    AppendRecord, CommitKnowledge, ENCRYPTED_RECORD_CONTENT_TYPE, EncryptedRecord,
    MAX_ENCRYPTED_RECORD_CIPHERTEXT_BYTES, MAX_ENCRYPTED_RECORD_PLAINTEXT_BYTES,
    MAX_ENCRYPTED_RECORD_STREAM_BYTES, MemoryReplica, QuorumWriter, Replica, ReplicaError,
    ReplicaGateway,
};

const KEY: [u8; 32] = [7; 32];

#[test]
fn encrypted_record_binary_codec_round_trips_and_rejects_malformed_lengths() {
    let record: EncryptedRecord = serde_json::from_value(json!({
        "stream": "tenant-a/catalog",
        "writer_epoch": 8,
        "lsn": 42,
        "committed_lsn": 41,
        "nonce": vec![3; 24],
        "ciphertext": vec![5; 512],
        "authentication": vec![7; 32],
    }))
    .expect("record fixture");

    let encoded = record.encode_binary().expect("binary encoding");
    assert_eq!(ENCRYPTED_RECORD_CONTENT_TYPE, "application/octet-stream");
    assert!(encoded.len() < serde_json::to_vec(&record).expect("json encoding").len());
    assert_eq!(EncryptedRecord::decode_binary(&encoded), Ok(record.clone()));

    let mut truncated = encoded.clone();
    truncated.pop();
    assert!(EncryptedRecord::decode_binary(&truncated).is_err());

    let mut trailing = encoded.clone();
    trailing.push(0);
    assert!(EncryptedRecord::decode_binary(&trailing).is_err());

    let mut oversized_stream = encoded;
    oversized_stream[..4]
        .copy_from_slice(&((MAX_ENCRYPTED_RECORD_STREAM_BYTES as u32) + 1).to_le_bytes());
    assert!(EncryptedRecord::decode_binary(&oversized_stream).is_err());

    let mut oversized_ciphertext = record.encode_binary().expect("binary encoding");
    let ciphertext_length_offset = 4 + record.stream().len() + 8 * 3 + 24;
    oversized_ciphertext[ciphertext_length_offset..ciphertext_length_offset + 4]
        .copy_from_slice(&((MAX_ENCRYPTED_RECORD_CIPHERTEXT_BYTES as u32) + 1).to_le_bytes());
    assert!(EncryptedRecord::decode_binary(&oversized_ciphertext).is_err());
}

#[test]
fn encrypted_record_batch_codec_preserves_framing_and_order() {
    let records = (1..=3)
        .map(|lsn| {
            serde_json::from_value(json!({
                "stream": "tenant-a/catalog",
                "writer_epoch": 8,
                "lsn": lsn,
                "committed_lsn": lsn - 1,
                "nonce": vec![lsn as u8; 24],
                "ciphertext": vec![lsn as u8; lsn as usize + 4],
                "authentication": vec![lsn as u8; 32],
            }))
            .expect("record fixture")
        })
        .collect::<Vec<EncryptedRecord>>();

    let encoded = EncryptedRecord::encode_binary_batch(&records).expect("batch encoding");
    assert_eq!(
        EncryptedRecord::decode_binary_batch(&encoded),
        Ok(records.clone())
    );

    let mut truncated = encoded.clone();
    truncated.pop();
    assert!(EncryptedRecord::decode_binary_batch(&truncated).is_err());

    let mut trailing = encoded;
    trailing.push(0);
    assert!(EncryptedRecord::decode_binary_batch(&trailing).is_err());
}

#[test]
fn encrypted_record_binary_codec_exposes_plaintext_ceiling_below_ciphertext_limit() {
    assert_eq!(
        MAX_ENCRYPTED_RECORD_PLAINTEXT_BYTES + 16,
        MAX_ENCRYPTED_RECORD_CIPHERTEXT_BYTES
    );
}

#[tokio::test]
async fn writer_batch_preserves_order_and_recovers_the_contiguous_tail() {
    let gateway = Arc::new(MemoryReplica::healthy());
    let writer = QuorumWriter::new(gateway, KEY);
    let records = (1..=3)
        .map(|lsn| AppendRecord::new("tenant-a/catalog", 8, lsn, lsn - 1, &[lsn as u8]))
        .collect::<Vec<_>>();

    let knowledge = writer
        .append_many(records.clone())
        .await
        .expect("batch gateway quorum acknowledgement");
    assert_eq!(
        knowledge,
        CommitKnowledge::replica("tenant-a/catalog", 8, 3)
    );

    let recovered = writer
        .recover("tenant-a/catalog", 0)
        .await
        .expect("ordered committed gateway tail");
    assert_eq!(
        recovered.iter().map(AppendRecord::lsn).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(
        recovered
            .iter()
            .map(|record| record.payload()[0])
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
}

#[tokio::test]
async fn one_gateway_acknowledges_an_encrypted_record_and_preserves_prior_commit() {
    let gateway = Arc::new(MemoryReplica::healthy());
    let writer = QuorumWriter::new(gateway, KEY);

    let knowledge = writer
        .append(AppendRecord::new(
            "tenant-a/do-7",
            3,
            41,
            40,
            b"sqlite delta",
        ))
        .await
        .expect("gateway quorum acknowledgement");

    assert_eq!(knowledge, CommitKnowledge::replica("tenant-a/do-7", 3, 41));
    let recovered = writer
        .recover("tenant-a/do-7", 40)
        .await
        .expect("committed gateway tail");
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].lsn(), 41);
    assert_eq!(recovered[0].committed_lsn(), 40);
    assert_eq!(recovered[0].payload(), b"sqlite delta");
}

#[tokio::test]
async fn a_gateway_failure_never_acknowledges_the_local_record() {
    let gateway = Arc::new(MemoryReplica::unavailable());
    let writer = QuorumWriter::new(gateway, KEY);

    let error = writer
        .append(AppendRecord::new("tenant-a/do-7", 3, 42, 41, b"delta"))
        .await
        .expect_err("an unavailable quorum gateway cannot acknowledge");

    assert_eq!(error, ReplicaError::NodeUnavailable);
}

#[tokio::test]
async fn stale_writer_epoch_is_fenced_by_the_gateway() {
    let gateway = Arc::new(MemoryReplica::healthy());
    let gateway_handle: Arc<dyn ReplicaGateway> = gateway.clone();
    let writer = QuorumWriter::new(gateway_handle, KEY);
    writer
        .append(AppendRecord::new("tenant-a/do-7", 9, 1, 0, b"new writer"))
        .await
        .expect("new writer commits");

    let error = writer
        .append(AppendRecord::new("tenant-a/do-7", 8, 2, 1, b"stale writer"))
        .await
        .expect_err("an older epoch is fenced");

    assert_eq!(error, ReplicaError::WriterFenced);
}

#[tokio::test]
async fn recovery_rejects_a_noncontiguous_gateway_tail() {
    let source = Arc::new(MemoryReplica::healthy());
    let source_handle: Arc<dyn ReplicaGateway> = source.clone();
    let seed = QuorumWriter::new(source_handle, KEY);
    seed.append(AppendRecord::new("tenant-a/do-7", 3, 6, 5, b"gap"))
        .await
        .expect("seed encrypted tail");
    let records = Replica::records(source.as_ref(), "tenant-a/do-7").await;
    let gateway = Arc::new(StaticGateway::new(records));
    let writer = QuorumWriter::new(gateway, KEY);

    let error = writer
        .recover("tenant-a/do-7", 4)
        .await
        .expect_err("a committed response cannot skip an LSN");

    assert_eq!(
        error,
        ReplicaError::RecoveryGap {
            expected_lsn: 5,
            received_lsn: 6,
        }
    );
}

#[tokio::test]
async fn watermark_recovery_rejects_a_tail_short_of_the_authenticated_commit() {
    let source = Arc::new(MemoryReplica::healthy());
    let source_handle: Arc<dyn ReplicaGateway> = source.clone();
    let seed = QuorumWriter::new(source_handle, KEY);
    seed.append(AppendRecord::new("tenant-a/do-7", 3, 5, 4, b"first"))
        .await
        .expect("seed encrypted tail");
    let records = Replica::records(source.as_ref(), "tenant-a/do-7").await;
    let gateway = Arc::new(StaticGateway::new(records));
    let writer = QuorumWriter::new(gateway, KEY);

    let error = writer
        .recover_with_watermark("tenant-a/do-7", 4, 6, "commit-cert")
        .await
        .expect_err("a watermark response must reach its authenticated commit");

    assert_eq!(
        error,
        ReplicaError::RecoveryIncomplete {
            expected_lsn: 6,
            committed_lsn: 6,
        }
    );
}

#[tokio::test]
async fn watermark_recovery_rejects_a_gap_before_the_authenticated_commit() {
    let source = Arc::new(MemoryReplica::healthy());
    let source_handle: Arc<dyn ReplicaGateway> = source.clone();
    let seed = QuorumWriter::new(source_handle, KEY);
    seed.append(AppendRecord::new("tenant-a/do-7", 3, 5, 4, b"first"))
        .await
        .expect("seed first encrypted record");
    seed.append(AppendRecord::new("tenant-a/do-7", 3, 7, 6, b"third"))
        .await
        .expect("seed third encrypted record");
    let records = Replica::records(source.as_ref(), "tenant-a/do-7").await;
    let gateway = Arc::new(StaticGateway::new(records));
    let writer = QuorumWriter::new(gateway, KEY);

    let error = writer
        .recover_with_watermark("tenant-a/do-7", 4, 7, "commit-cert")
        .await
        .expect_err("a watermark response must not skip an LSN");

    assert_eq!(
        error,
        ReplicaError::RecoveryGap {
            expected_lsn: 6,
            received_lsn: 7,
        }
    );
}

#[tokio::test]
async fn http_client_posts_once_and_recovers_from_the_one_gateway_endpoint()
-> Result<(), Box<dyn std::error::Error>> {
    let state = Arc::new(HttpState::default());
    let app = Router::new()
        .route("/internal/v1/bitr/activate", post(http_activate))
        .route("/v1/append", post(http_append))
        .route("/v1/records", get(http_records))
        .with_state(Arc::clone(&state));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    tokio::spawn(async move { axum::serve(listener, app).await });

    let gateway = Arc::new(
        walleye_bitr::HttpReplica::new(format!("http://{address}"), "gateway-secret")
            .with_activation(format!("http://{address}"), "operator-secret"),
    );
    let writer = QuorumWriter::new(gateway, KEY);
    let knowledge = writer
        .append(AppendRecord::new(
            "tenant-a/do-7",
            3,
            41,
            40,
            b"secret delta",
        ))
        .await?;
    assert_eq!(knowledge.certificate.as_deref(), Some("commit-cert-41"));
    let recovered = writer.recover("tenant-a/do-7", 40).await?;

    assert_eq!(state.append_calls.lock().expect("append lock").len(), 1);
    assert_eq!(
        state
            .activation_authorizations
            .lock()
            .expect("activation lock")
            .as_slice(),
        &[Some("Bearer operator-secret".to_owned())]
    );
    assert_eq!(
        state
            .append_content_types
            .lock()
            .expect("content type lock")
            .as_slice(),
        &[Some(ENCRYPTED_RECORD_CONTENT_TYPE.to_owned())]
    );
    let envelope = state
        .append_calls
        .lock()
        .expect("append lock")
        .first()
        .cloned()
        .ok_or("missing append envelope")?;
    assert_eq!(envelope.stream(), "tenant-a/do-7");
    assert_eq!(envelope.lsn(), 41);
    assert_eq!(envelope.committed_lsn(), 40);
    assert_ne!(envelope.ciphertext(), b"secret delta");
    assert_eq!(
        state
            .recovery_queries
            .lock()
            .expect("query lock")
            .as_slice(),
        &[("tenant-a/do-7".to_owned(), 40, None)]
    );
    assert_eq!(recovered[0].payload(), b"secret delta");
    Ok(())
}

#[tokio::test]
async fn http_client_posts_one_framed_batch_to_the_gateway()
-> Result<(), Box<dyn std::error::Error>> {
    let state = Arc::new(HttpState::default());
    let app = Router::new()
        .route("/v1/append-many", post(http_append_many))
        .with_state(Arc::clone(&state));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    tokio::spawn(async move { axum::serve(listener, app).await });

    let gateway = Arc::new(walleye_bitr::HttpReplica::new(
        format!("http://{address}"),
        "gateway-secret",
    ));
    let writer = QuorumWriter::new(gateway, KEY);
    let records = (1..=3)
        .map(|lsn| AppendRecord::new("tenant-a/do-7", 3, lsn, lsn - 1, &[lsn as u8]))
        .collect::<Vec<_>>();
    let knowledge = writer.append_many(records).await?;

    assert_eq!(knowledge.committed_lsn, 3);
    assert_eq!(knowledge.certificate.as_deref(), Some("commit-cert-3"));
    assert_eq!(state.batch_calls.lock().expect("batch lock").len(), 1);
    assert_eq!(
        state
            .batch_content_types
            .lock()
            .expect("batch content type lock")
            .as_slice(),
        &[Some(ENCRYPTED_RECORD_CONTENT_TYPE.to_owned())]
    );
    assert_eq!(
        state.batch_calls.lock().expect("batch lock")[0]
            .iter()
            .map(EncryptedRecord::lsn)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    Ok(())
}

#[tokio::test]
async fn http_client_sends_authenticated_committed_watermark_for_recovery()
-> Result<(), Box<dyn std::error::Error>> {
    let state = Arc::new(HttpState::default());
    let app = Router::new()
        .route("/v1/append", post(http_append))
        .route("/v1/records", get(http_records))
        .with_state(Arc::clone(&state));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    tokio::spawn(async move { axum::serve(listener, app).await });

    let gateway = Arc::new(walleye_bitr::HttpReplica::new(
        format!("http://{address}"),
        "gateway-secret",
    ));
    let writer = QuorumWriter::new(gateway, KEY);
    writer
        .append(AppendRecord::new(
            "tenant-a/do-7",
            3,
            41,
            40,
            b"secret delta",
        ))
        .await?;
    writer
        .recover_with_watermark("tenant-a/do-7", 40, 41, "commit-cert-41")
        .await?;

    assert_eq!(
        state
            .recovery_queries
            .lock()
            .expect("query lock")
            .as_slice(),
        &[("tenant-a/do-7".to_owned(), 40, Some(41))]
    );
    assert_eq!(
        state
            .recovery_certificates
            .lock()
            .expect("certificate lock")
            .as_slice(),
        &[Some("commit-cert-41".to_owned())]
    );
    Ok(())
}

#[tokio::test]
async fn http_gateway_503_is_not_treated_as_a_commit() -> Result<(), Box<dyn std::error::Error>> {
    let app = Router::new().route(
        "/v1/append",
        post(|| async { StatusCode::SERVICE_UNAVAILABLE }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    tokio::spawn(async move { axum::serve(listener, app).await });

    let writer = QuorumWriter::new(
        Arc::new(walleye_bitr::HttpReplica::new(
            format!("http://{address}"),
            "gateway-secret",
        )),
        KEY,
    );
    let error = writer
        .append(AppendRecord::new("tenant-a/do-7", 3, 1, 0, b"delta"))
        .await
        .expect_err("gateway did not establish a quorum");
    assert_eq!(error, ReplicaError::GatewayUnavailable);
    Ok(())
}

#[tokio::test]
async fn http_client_maps_replica_capacity_exceeded_response()
-> Result<(), Box<dyn std::error::Error>> {
    let app = Router::new().route(
        "/v1/append",
        post(|| async {
            (
                StatusCode::PAYLOAD_TOO_LARGE,
                axum::Json(json!({
                    "code": "replica_capacity_exceeded",
                    "retryable": false,
                    "requested_bytes": 4097,
                    "max_append_bytes": 4096,
                    "cohort_id": 7,
                })),
            )
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    tokio::spawn(async move { axum::serve(listener, app).await });

    let writer = QuorumWriter::new(
        Arc::new(walleye_bitr::HttpReplica::new(
            format!("http://{address}"),
            "gateway-secret",
        )),
        KEY,
    );
    let error = writer
        .append(AppendRecord::new("tenant-a/do-7", 3, 1, 0, b"oversized"))
        .await
        .expect_err("capacity response must not acknowledge the append");

    assert_eq!(
        error,
        ReplicaError::CapacityExceeded {
            cohort_id: 7,
            requested_bytes: 4097,
            max_append_bytes: 4096,
        }
    );
    Ok(())
}

#[tokio::test]
async fn http_client_requires_the_capacity_error_code() -> Result<(), Box<dyn std::error::Error>> {
    let app = Router::new().route(
        "/v1/append",
        post(|| async {
            (
                StatusCode::PAYLOAD_TOO_LARGE,
                axum::Json(json!({
                    "code": "some_other_error",
                    "requested_bytes": 4097,
                    "max_append_bytes": 4096,
                    "cohort_id": 7,
                })),
            )
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    tokio::spawn(async move { axum::serve(listener, app).await });

    let writer = QuorumWriter::new(
        Arc::new(walleye_bitr::HttpReplica::new(
            format!("http://{address}"),
            "gateway-secret",
        )),
        KEY,
    );
    let error = writer
        .append(AppendRecord::new("tenant-a/do-7", 3, 1, 0, b"oversized"))
        .await
        .expect_err("an unrelated 413 must not use the capacity error");

    assert_eq!(error, ReplicaError::GatewayRejected { status: 413 });
    Ok(())
}

#[derive(Default)]
struct HttpState {
    activation_authorizations: Mutex<Vec<Option<String>>>,
    append_calls: Mutex<Vec<EncryptedRecord>>,
    append_content_types: Mutex<Vec<Option<String>>>,
    batch_calls: Mutex<Vec<Vec<EncryptedRecord>>>,
    batch_content_types: Mutex<Vec<Option<String>>>,
    recovery_queries: Mutex<Vec<(String, u64, Option<u64>)>>,
    recovery_certificates: Mutex<Vec<Option<String>>>,
}

/// Captures the host-only wake lease. Payload bytes never cross this route.
async fn http_activate(State(state): State<Arc<HttpState>>, headers: HeaderMap) -> StatusCode {
    state
        .activation_authorizations
        .lock()
        .expect("activation lock")
        .push(
            headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
        );
    StatusCode::NO_CONTENT
}

/// Captures one gateway append envelope after its route decides to acknowledge.
async fn http_append(
    State(state): State<Arc<HttpState>>,
    headers: HeaderMap,
    body: Bytes,
) -> (StatusCode, HeaderMap) {
    let record = EncryptedRecord::decode_binary(&body).expect("binary record body");
    state
        .append_content_types
        .lock()
        .expect("content type lock")
        .push(
            headers
                .get("content-type")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
        );
    state.append_calls.lock().expect("append lock").push(record);
    let mut headers = HeaderMap::new();
    headers.insert(
        walleye_bitr::COMMIT_CERTIFICATE_HEADER,
        HeaderValue::from_static("commit-cert-41"),
    );
    (StatusCode::NO_CONTENT, headers)
}

/// Captures one framed gateway append-many envelope.
async fn http_append_many(
    State(state): State<Arc<HttpState>>,
    headers: HeaderMap,
    body: Bytes,
) -> (StatusCode, HeaderMap) {
    let records = EncryptedRecord::decode_binary_batch(&body).expect("binary batch body");
    state
        .batch_content_types
        .lock()
        .expect("batch content type lock")
        .push(
            headers
                .get("content-type")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
        );
    state.batch_calls.lock().expect("batch lock").push(records);
    let mut headers = HeaderMap::new();
    headers.insert(
        walleye_bitr::COMMIT_CERTIFICATE_HEADER,
        HeaderValue::from_static("commit-cert-3"),
    );
    (StatusCode::NO_CONTENT, headers)
}

#[derive(Deserialize)]
struct RecoveryQuery {
    stream: String,
    after_lsn: u64,
    committed_lsn: Option<u64>,
}

/// Returns only the committed tail captured by the append route.
async fn http_records(
    State(state): State<Arc<HttpState>>,
    headers: HeaderMap,
    Query(query): Query<RecoveryQuery>,
) -> axum::Json<Vec<EncryptedRecord>> {
    state.recovery_queries.lock().expect("query lock").push((
        query.stream.clone(),
        query.after_lsn,
        query.committed_lsn,
    ));
    state
        .recovery_certificates
        .lock()
        .expect("certificate lock")
        .push(
            headers
                .get(walleye_bitr::COMMIT_CERTIFICATE_HEADER)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
        );
    axum::Json(state.append_calls.lock().expect("append lock").clone())
}

struct StaticGateway {
    records: Vec<EncryptedRecord>,
}

impl StaticGateway {
    /// Creates a gateway fixture that returns the supplied committed tail.
    fn new(records: Vec<EncryptedRecord>) -> Self {
        Self { records }
    }
}

#[async_trait::async_trait]
impl ReplicaGateway for StaticGateway {
    /// Accepts appends because recovery tests do not exercise the append path.
    async fn append(&self, _record: EncryptedRecord) -> Result<Option<String>, ReplicaError> {
        Ok(None)
    }

    /// Returns the fixture tail without applying client-side filtering.
    async fn recover(
        &self,
        _stream: &str,
        _after_lsn: u64,
    ) -> Result<Vec<EncryptedRecord>, ReplicaError> {
        Ok(self.records.clone())
    }

    /// Returns the fixture tail for a bounded recovery request.
    async fn recover_with_watermark(
        &self,
        _stream: &str,
        _after_lsn: u64,
        _committed_lsn: u64,
        _certificate: &str,
    ) -> Result<Vec<EncryptedRecord>, ReplicaError> {
        Ok(self.records.clone())
    }
}
