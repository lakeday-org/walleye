//! Read-only SQL over stable hot-log and Lance snapshots, using the shared query budget.
use crate::{LanceStorageOptions, ScanResult};
use arrow_schema::SchemaRef;
use futures::TryStreamExt;
use lance::deps::datafusion::{
    catalog::{Session, TableProvider},
    common::Result as DfResult,
    execution::{
        context::{SQLOptions, SessionContext},
        memory_pool::{FairSpillPool, MemoryConsumer},
        runtime_env::RuntimeEnvBuilder,
    },
    logical_expr::{Expr, TableType},
    physical_expr::expressions::Column,
    physical_plan::{ExecutionPlan, execute_stream, projection::ProjectionExec},
    prelude::SessionConfig,
};
use std::sync::Arc;
fn context(storage: &LanceStorageOptions) -> lance::Result<SessionContext> {
    let runtime = match storage.query_runtime() {
        Some(r) => r,
        None => RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(FairSpillPool::new(64 * 1024 * 1024)))
            .build_arc()
            .map_err(err)?,
    };
    Ok(SessionContext::new_with_config_rt(
        SessionConfig::new().with_target_partitions(storage.query_partitions()),
        runtime,
    ))
}
fn err(e: impl std::fmt::Display) -> lance::Error {
    lance::Error::io(e.to_string())
}
/// A stream supplies its schema without locking its writer. SQL captures an owned
/// snapshot only when that stream is used, then releases the writer before execution.
#[async_trait::async_trait]
pub trait SnapshotSource: Send + Sync {
    fn schema(&self) -> SchemaRef;
    async fn snapshot(&self) -> lance::Result<TableSnapshot>;
}

/// Queries can reference only the supplied stream snapshots. DDL, DML, and external
/// table registration are disabled. Ingestion occurs through the stream API.
pub async fn query(
    storage: &LanceStorageOptions,
    tables: &[(String, Arc<dyn SnapshotSource>)],
    sql: &str,
) -> lance::Result<ScanResult> {
    tokio::time::timeout(storage.query_timeout(), async {
        let ctx = context(storage)?;
        for (name, table) in tables {
            ctx.register_table(
                name.as_str(),
                Arc::new(StreamProvider {
                    source: table.clone(),
                    snapshot: tokio::sync::OnceCell::new(),
                }),
            )
            .map_err(err)?;
        }
        let frame = ctx
            .sql_with_options(
                sql,
                SQLOptions::new()
                    .with_allow_ddl(false)
                    .with_allow_dml(false)
                    .with_allow_statements(false),
            )
            .await
            .map_err(err)?;
        let plan = frame.create_physical_plan().await.map_err(err)?;
        collect(&ctx, plan).await
    })
    .await
    .map_err(|_| err("query deadline exceeded"))?
}
pub(crate) async fn execute(
    storage: &LanceStorageOptions,
    plan: Arc<dyn ExecutionPlan>,
) -> lance::Result<ScanResult> {
    collect(&context(storage)?, plan).await
}
async fn collect(ctx: &SessionContext, plan: Arc<dyn ExecutionPlan>) -> lance::Result<ScanResult> {
    let reservation =
        MemoryConsumer::new("Walleye query results").register(&ctx.runtime_env().memory_pool);
    let mut stream = execute_stream(plan, ctx.task_ctx()).map_err(err)?;
    let mut retained = crate::result_memory::ResultMemory::default();
    let mut batches = Vec::new();
    while let Some(batch) = stream.try_next().await.map_err(err)? {
        reservation.try_grow(retained.charge(&batch)).map_err(err)?;
        batches.push(batch);
    }
    Ok(ScanResult {
        batches,
        _memory: reservation,
    })
}
/// A captured stream view owns its plan and can outlive the writer lock.
#[derive(Clone, Debug)]
pub struct TableSnapshot(Arc<dyn ExecutionPlan>);
impl TableSnapshot {
    pub(crate) fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        Self(Arc::new(SnapshotExec(plan)))
    }
}
struct StreamProvider {
    source: Arc<dyn SnapshotSource>,
    // One snapshot per stream per query, including self-joins and repeated scans.
    snapshot: tokio::sync::OnceCell<TableSnapshot>,
}
impl std::fmt::Debug for StreamProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamProvider").finish_non_exhaustive()
    }
}
#[async_trait::async_trait]
impl TableProvider for StreamProvider {
    fn schema(&self) -> SchemaRef {
        self.source.schema()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let snapshot = self
            .snapshot
            .get_or_try_init(|| async {
                self.source.snapshot().await.map_err(|e| {
                    lance::deps::datafusion::common::DataFusionError::External(Box::new(e))
                })
            })
            .await?;
        snapshot.scan(state, projection, filters, limit).await
    }
}
#[async_trait::async_trait]
impl TableProvider for TableSnapshot {
    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if let Some(indices) = projection {
            let expressions: Vec<(
                Arc<dyn lance::deps::datafusion::physical_expr::PhysicalExpr>,
                String,
            )> = indices
                .iter()
                .map(|&i| {
                    (
                        Arc::new(Column::new(self.schema().field(i).name(), i)) as _,
                        self.schema().field(i).name().clone(),
                    )
                })
                .collect();
            Ok(Arc::new(ProjectionExec::try_new(
                expressions,
                self.0.clone(),
            )?))
        } else {
            Ok(self.0.clone())
        }
    }
}

/// Lance owns the internal LSM plan. Present it as a leaf so the SQL optimizer
/// cannot rewrite its merge semantics or depend on internal generation statistics.
#[derive(Debug)]
struct SnapshotExec(Arc<dyn ExecutionPlan>);
impl lance::deps::datafusion::physical_plan::DisplayAs for SnapshotExec {
    fn fmt_as(
        &self,
        _: lance::deps::datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "WalleyeStreamSnapshot")
    }
}
impl ExecutionPlan for SnapshotExec {
    fn name(&self) -> &str {
        "WalleyeStreamSnapshot"
    }
    fn properties(&self) -> &Arc<lance::deps::datafusion::physical_plan::PlanProperties> {
        self.0.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(lance::deps::datafusion::common::DataFusionError::Plan(
                "snapshot is a leaf".into(),
            ));
        }
        Ok(self)
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<lance::deps::datafusion::execution::TaskContext>,
    ) -> DfResult<lance::deps::datafusion::physical_plan::SendableRecordBatchStream> {
        self.0.execute(partition, context)
    }
}
