//! Local rolling-upgrade acceptance tests.
//!
//! These tests intentionally use the same HTTP node and gateway routers as
//! the cloud binary.  They exercise the durability boundary over real TCP
//! sockets while keeping the storage volume local and disposable:
//!
//! * one storage member can restart while the other two continue to accept
//!   acknowledged writes;
//! * a replacement gateway reconstructs its writer fence from the durable
//!   node snapshots, so a process restart cannot fork the stream; and
//! * a successor writer epoch fences the old writer even after the gateway
//!   process was replaced.

use std::error::Error;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::serve;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use walleye_bitr::{
    AppendRecord, HttpReplica, QuorumWriter, ReplicaError, ReplicaGateway as ReplicaGatewayTrait,
};
use walleye_bitr_server::{
    DiskReplica, OpaqueArchive, ReplicaGateway as CloudReplicaGateway, ReplicaNode, gateway_router,
    node_router,
};

const VERSION: &str = "lakeday-cloud/deployment-identity/v1";
const INTERNAL_TOKEN: &str = "rolling-upgrade-node-token";
const TENANT: &str = "rolling-upgrade-tenant";
const STREAM: &str = "rolling-upgrade-tenant/entity-memory";
const WRITER_KEY: [u8; 32] = [41; 32];

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

fn tenant_token(root_key: &[u8; 32], tenant: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(root_key).expect("HMAC key");
    mac.update(format!("{VERSION}\0{tenant}\0replica-gateway-authentication").as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

fn archive(prefix: &str) -> TestResult<Arc<OpaqueArchive>> {
    Ok(Arc::new(OpaqueArchive::new(
        Arc::new(object_store::memory::InMemory::new()),
        prefix,
        64,
    )?))
}

/// One HTTP storage process and its durable local volume.
struct TestNode {
    id: String,
    addr: SocketAddr,
    log_path: PathBuf,
    data_dir: PathBuf,
    root: String,
    node: Arc<DiskReplica>,
    task: Option<JoinHandle<Result<(), std::io::Error>>>,
}

impl TestNode {
    async fn start(
        id: impl Into<String>,
        root: impl Into<String>,
        directory: &Path,
    ) -> TestResult<Self> {
        let id = id.into();
        let root = root.into();
        let data_dir = directory.join(&id);
        let log_path = data_dir.join("replica.log");
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let node = Arc::new(DiskReplica::open_with_config(
            &log_path, &id, "hot", &data_dir,
        )?);
        let app = node_router(Arc::clone(&node), &root, Some(INTERNAL_TOKEN))?;
        let task = tokio::spawn(async move { serve(listener, app).await });
        Ok(Self {
            id,
            addr,
            log_path,
            data_dir,
            root,
            node,
            task: Some(task),
        })
    }

    fn member(&self) -> ReplicaNode {
        ReplicaNode::new(self.id.clone(), format!("http://{}", self.addr))
    }

    async fn stop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }

    async fn restart(&mut self) -> TestResult<()> {
        self.stop().await;
        // Let the aborted serve task release its listener before rebinding
        // the same stable URL. This is deliberately bounded and local.
        let listener = bind_after_abort(self.addr).await?;
        let node = Arc::new(DiskReplica::open_with_config(
            &self.log_path,
            &self.id,
            "hot",
            &self.data_dir,
        )?);
        let app = node_router(Arc::clone(&node), &self.root, Some(INTERNAL_TOKEN))?;
        self.task = Some(tokio::spawn(async move { serve(listener, app).await }));
        self.node = node;
        Ok(())
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn bind_after_abort(addr: SocketAddr) -> std::io::Result<TcpListener> {
    let mut last_error = None;
    for _ in 0..20 {
        match TcpListener::bind(addr).await {
            Ok(listener) => return Ok(listener),
            Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {
                last_error = Some(error);
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_error.expect("at least one bind attempt"))
}

/// One gateway process. A fresh instance deliberately gets an empty
/// in-memory writer cache and must rebuild it from the node quorum.
struct TestGateway {
    addr: SocketAddr,
    nodes: Vec<ReplicaNode>,
    root: String,
    task: Option<JoinHandle<Result<(), std::io::Error>>>,
    gateway: Arc<CloudReplicaGateway>,
}

impl TestGateway {
    async fn start(nodes: Vec<ReplicaNode>, root: impl Into<String>) -> TestResult<Self> {
        Self::start_at(nodes, root.into(), None).await
    }

    async fn start_at(
        nodes: Vec<ReplicaNode>,
        root: String,
        addr: Option<SocketAddr>,
    ) -> TestResult<Self> {
        let gateway = Arc::new(CloudReplicaGateway::new(
            nodes.clone(),
            2,
            &root,
            INTERNAL_TOKEN,
            archive("rolling-upgrade")?,
        )?);
        let listener = match addr {
            Some(addr) => bind_after_abort(addr).await?,
            None => TcpListener::bind("127.0.0.1:0").await?,
        };
        let addr = listener.local_addr()?;
        let app = gateway_router(Arc::clone(&gateway));
        let task = tokio::spawn(async move { serve(listener, app).await });
        Ok(Self {
            addr,
            nodes,
            root,
            task: Some(task),
            gateway,
        })
    }

    fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    async fn stop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }

    async fn restart(&mut self) -> TestResult<()> {
        self.stop().await;
        let mut replacement =
            Self::start_at(self.nodes.clone(), self.root.clone(), Some(self.addr)).await?;
        self.task = replacement.task.take();
        self.gateway = Arc::clone(&replacement.gateway);
        Ok(())
    }
}

impl Drop for TestGateway {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Small client-side load balancer used to model two equivalent gateway
/// processes behind one endpoint. It fails over only transport/availability
/// errors; fencing and conflicts remain authoritative and are never hidden.
struct FailoverGateway {
    primary: Arc<HttpReplica>,
    secondary: Arc<HttpReplica>,
}

impl FailoverGateway {
    fn retry_on(error: &ReplicaError) -> bool {
        matches!(
            error,
            ReplicaError::GatewayUnavailable
                | ReplicaError::GatewayRejected { status: 502..=504 }
                | ReplicaError::QuorumUnavailable
                | ReplicaError::NodeUnavailable
        )
    }
}

#[async_trait]
impl ReplicaGatewayTrait for FailoverGateway {
    async fn append(
        &self,
        record: walleye_bitr::EncryptedRecord,
    ) -> Result<Option<String>, ReplicaError> {
        match self.primary.append(record.clone()).await {
            Err(error) if Self::retry_on(&error) => self.secondary.append(record).await,
            result => result,
        }
    }

    async fn append_many(
        &self,
        records: Vec<walleye_bitr::EncryptedRecord>,
    ) -> Result<Option<String>, ReplicaError> {
        match self.primary.append_many(records.clone()).await {
            Err(error) if Self::retry_on(&error) => self.secondary.append_many(records).await,
            result => result,
        }
    }

    async fn recover(
        &self,
        stream: &str,
        after_lsn: u64,
    ) -> Result<Vec<walleye_bitr::EncryptedRecord>, ReplicaError> {
        match self.primary.recover(stream, after_lsn).await {
            Err(error) if Self::retry_on(&error) => self.secondary.recover(stream, after_lsn).await,
            result => result,
        }
    }

    async fn recover_with_watermark(
        &self,
        stream: &str,
        after_lsn: u64,
        committed_lsn: u64,
        certificate: &str,
    ) -> Result<Vec<walleye_bitr::EncryptedRecord>, ReplicaError> {
        match self
            .primary
            .recover_with_watermark(stream, after_lsn, committed_lsn, certificate)
            .await
        {
            Err(error) if Self::retry_on(&error) => {
                self.secondary
                    .recover_with_watermark(stream, after_lsn, committed_lsn, certificate)
                    .await
            }
            result => result,
        }
    }
}

async fn nodes(directory: &Path, root: &str) -> TestResult<Vec<TestNode>> {
    let mut nodes = Vec::with_capacity(3);
    for index in 0..3 {
        nodes.push(
            TestNode::start(format!("rolling-node-{index}"), root.to_owned(), directory).await?,
        );
    }
    Ok(nodes)
}

fn writer(gateway_url: &str, token: &str) -> QuorumWriter {
    let gateway = Arc::new(HttpReplica::new(gateway_url.to_owned(), token.to_owned()));
    QuorumWriter::new(gateway, WRITER_KEY)
}

fn failover_writer(primary_url: &str, secondary_url: &str, token: &str) -> QuorumWriter {
    let gateway = Arc::new(FailoverGateway {
        primary: Arc::new(HttpReplica::new(primary_url.to_owned(), token.to_owned())),
        secondary: Arc::new(HttpReplica::new(secondary_url.to_owned(), token.to_owned())),
    });
    QuorumWriter::new(gateway, WRITER_KEY)
}

async fn append_range(
    writer: &QuorumWriter,
    first_lsn: u64,
    last_lsn: u64,
    epoch: u64,
) -> TestResult<()> {
    for lsn in first_lsn..=last_lsn {
        writer
            .append(AppendRecord::new(
                STREAM,
                epoch,
                lsn,
                lsn - 1,
                format!("payload-{lsn}").as_bytes(),
            ))
            .await?;
    }
    Ok(())
}

fn assert_payloads(records: &[AppendRecord], first_lsn: u64, last_lsn: u64, epoch: u64) {
    assert_eq!(
        records.iter().map(AppendRecord::lsn).collect::<Vec<_>>(),
        (first_lsn..=last_lsn).collect::<Vec<_>>()
    );
    assert!(
        records
            .iter()
            .all(|record| { record.writer_epoch() == epoch || record.writer_epoch() < epoch })
    );
    for record in records {
        assert_eq!(
            record.payload(),
            format!("payload-{}", record.lsn()).as_bytes()
        );
    }
}

/// One member can be restarted without interrupting the quorum path. Every
/// acknowledgement made while it is down is recovered from the two survivors,
/// then a fenced rebalance repairs the restarted volume from that evidence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn continuous_writes_survive_storage_restart_and_repair() -> TestResult {
    let root_key = [73_u8; 32];
    let encoded_root = STANDARD.encode(root_key);
    let token = tenant_token(&root_key, TENANT);
    let directory = tempfile::tempdir()?;
    let mut nodes = nodes(directory.path(), &encoded_root).await?;
    let members = nodes.iter().map(TestNode::member).collect::<Vec<_>>();
    let gateway = TestGateway::start(members, encoded_root.clone()).await?;
    let writer = writer(&gateway.url(), &token);

    append_range(&writer, 1, 4, 1).await?;
    let boot_before = nodes[0].node.boot_id().to_owned();
    nodes[0].stop().await;

    // This is the continuous-write window: node zero is unavailable, while
    // the remaining two members acknowledge the entire suffix.
    append_range(&writer, 5, 20, 1).await?;
    assert_eq!(nodes[1].node.snapshot().committed.len(), 20);
    assert_eq!(nodes[2].node.snapshot().committed.len(), 20);

    nodes[0].restart().await?;
    assert_ne!(nodes[0].node.boot_id(), boot_before);
    assert_eq!(nodes[0].node.snapshot().committed.len(), 4);

    // Repair runs under the gateway's durable maintenance fence. No new
    // writes are admitted during this manifest-changing operation, but all
    // previously acknowledged records remain available and are copied using
    // the exact idempotent append/commit protocol.
    // Static compatibility gateways use the first complete cohort for all
    // members; an all-member repair keeps the test on that public path while
    // still repairing the restarted volume from the two surviving copies.
    let report = gateway.gateway.rebalance(None).await?;
    assert!(report.complete(), "incomplete repair report: {report:?}");
    assert_eq!(nodes[0].node.snapshot().committed.len(), 20);

    let recovered = writer.recover(STREAM, 0).await?;
    assert_payloads(&recovered, 1, 20, 1);
    Ok(())
}

/// Two equivalent gateways provide the availability boundary during a gateway
/// process replacement. The replacement starts with no writer cache, rebuilds
/// the committed prefix from the node quorum, and still fences an old writer
/// after a successor epoch is admitted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gateway_replacement_preserves_writes_and_epoch_fencing() -> TestResult {
    let root_key = [79_u8; 32];
    let encoded_root = STANDARD.encode(root_key);
    let token = tenant_token(&root_key, TENANT);
    let directory = tempfile::tempdir()?;
    let nodes = nodes(directory.path(), &encoded_root).await?;
    let members = nodes.iter().map(TestNode::member).collect::<Vec<_>>();
    let mut primary = TestGateway::start(members.clone(), encoded_root.clone()).await?;
    let secondary = TestGateway::start(members, encoded_root.clone()).await?;
    let writer = Arc::new(failover_writer(&primary.url(), &secondary.url(), &token));

    append_range(&writer, 1, 4, 1).await?;
    let primary_boot_before = primary.gateway.metrics().await?.boot_id;
    primary.stop().await;

    let writer_during_replacement = Arc::clone(&writer);
    let (first_failover_tx, first_failover_rx) = tokio::sync::oneshot::channel();
    let writes = tokio::spawn(async move {
        let mut first_failover_tx = Some(first_failover_tx);
        for lsn in 5..=20 {
            writer_during_replacement
                .append(AppendRecord::new(
                    STREAM,
                    1,
                    lsn,
                    lsn - 1,
                    format!("payload-{lsn}").as_bytes(),
                ))
                .await
                .map_err(|error| format!("write {lsn} failed: {error}"))?;
            if lsn == 5
                && let Some(sender) = first_failover_tx.take()
            {
                let _ = sender.send(());
            }
            // Keep the replacement overlapped with the write stream rather
            // than merely placing it before or after a batch.
            tokio::time::sleep(Duration::from_millis(3)).await;
        }
        Ok::<_, String>(())
    });
    first_failover_rx.await?;
    primary.restart().await?;
    writes.await??;

    let primary_boot_after = primary.gateway.metrics().await?.boot_id;
    assert_ne!(primary_boot_before, primary_boot_after);
    let recovered = writer.recover(STREAM, 0).await?;
    assert_payloads(&recovered, 1, 20, 1);

    // The new process has reconstructed epoch one from durable node state.
    // A successor epoch can advance the stream, but the old writer cannot
    // append after that fence—even if it retained the old HTTP client.
    let successor = failover_writer(&primary.url(), &secondary.url(), &token);
    successor
        .append(AppendRecord::new(STREAM, 2, 21, 20, b"payload-21"))
        .await?;
    let stale = writer
        .append(AppendRecord::new(STREAM, 1, 22, 21, b"stale-payload-22"))
        .await
        .expect_err("the old writer epoch must be fenced");
    assert_eq!(stale, ReplicaError::WriterFenced);

    let recovered = successor.recover(STREAM, 0).await?;
    assert_payloads(&recovered[..20], 1, 20, 1);
    assert_eq!(recovered[20].lsn(), 21);
    assert_eq!(recovered[20].writer_epoch(), 2);
    assert_eq!(recovered[20].payload(), b"payload-21");
    Ok(())
}
