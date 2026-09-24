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
    logical_expr::{Expr, TableProviderFilterPushDown, TableType},
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
    let context = SessionContext::new_with_config_rt(
        SessionConfig::new().with_target_partitions(storage.query_partitions()),
        runtime,
    );
    // Typed decisions are ordinary functions to a query, so they are added
    // wherever a session is built rather than only on one path.
    // A model and a decision service, as functions a query can call.
    crate::prompt::register(&context);
    crate::embed::register(&context);
    Ok(context)
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

/// A point-in-time stream view which can build more than one physical plan.
///
/// A SQL query may scan the same table more than once (for example, a
/// self-join), and each scan may carry a different pushed-down predicate.  The
/// implementation captures the table's manifest and in-memory memtables once,
/// then builds each filtered plan from that captured state.  This keeps the
/// read consistent without forcing every query through an unfiltered full LSM
/// scan.
#[async_trait::async_trait]
pub(crate) trait SnapshotPlanSource: Send + Sync {
    fn schema(&self) -> SchemaRef;
    async fn plan(
        &self,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>>;
    /// Build a LanceDB-style search plan: optional SQL filter, projection,
    /// limit/offset, and optional nearest-neighbor query.
    async fn search_plan(
        &self,
        request: &crate::SearchRequest,
    ) -> lance::Result<Arc<dyn ExecutionPlan>>;
    async fn count(&self, filter: Option<&str>) -> lance::Result<u64>;
}

/// Queries can reference only the supplied stream snapshots. DDL, DML, and external
/// table registration are disabled. Ingestion occurs through the stream API.
pub async fn query(
    storage: &LanceStorageOptions,
    tables: &[(String, Arc<dyn SnapshotSource>)],
    sql: &str,
) -> lance::Result<ScanResult> {
    query_with_gathered(storage, tables, &[], sql).await
}

/// As [`query`], plus tables whose rows were gathered from the member that
/// owns them. A gathered table is a point-in-time copy taken by its owner, so
/// it carries that owner's unflushed rows; it is registered in memory for the
/// life of this query only.
/// Tables are registered under exactly the name the catalog holds.
///
/// `register_table` accepts anything that converts into a `TableReference`,
/// and converting from a string *parses* it, which folds an unquoted
/// identifier to lower case. Registering `"Pets"` that way filed it under
/// `pets`, so `FROM "Pets"` — the only form available to a name holding a
/// space or a reserved word — could never resolve, while the catalog, the
/// table API and cluster routing all still called it `Pets`.
///
/// `TableReference::bare` takes the name as given. Folding then happens only
/// where SQL says it should: on the reference in the statement, so `FROM
/// "Pets"` resolves and unquoted `FROM Pets` does not, as in any other
/// case-sensitive catalog.
pub async fn query_with_gathered(
    storage: &LanceStorageOptions,
    tables: &[(String, Arc<dyn SnapshotSource>)],
    gathered: &[(String, SchemaRef, Vec<arrow_array::RecordBatch>)],
    sql: &str,
) -> lance::Result<ScanResult> {
    use lance::deps::datafusion::{common::TableReference, datasource::MemTable};
    tokio::time::timeout(storage.query_timeout(), async {
        let ctx = context(storage)?;
        for (name, table) in tables {
            if gathered.iter().any(|(gathered, _, _)| gathered == name) {
                continue;
            }
            ctx.register_table(
                TableReference::bare(name.clone()),
                Arc::new(StreamProvider {
                    source: table.clone(),
                    snapshot: tokio::sync::OnceCell::new(),
                }),
            )
            .map_err(err)?;
        }
        for (name, schema, batches) in gathered {
            ctx.register_table(
                TableReference::bare(name.clone()),
                Arc::new(MemTable::try_new(schema.clone(), vec![batches.clone()]).map_err(err)?),
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
/// A captured stream view owns its point-in-time source and can outlive the
/// writer lock.
#[derive(Clone)]
pub struct TableSnapshot(Arc<dyn SnapshotPlanSource>);
impl TableSnapshot {
    pub(crate) fn from_source(source: Arc<dyn SnapshotPlanSource>) -> Self {
        Self(source)
    }
    /// Execute a LanceDB-style search against this point-in-time view.
    pub async fn search(
        &self,
        storage: &LanceStorageOptions,
        request: &crate::SearchRequest,
    ) -> lance::Result<ScanResult> {
        tokio::time::timeout(storage.query_timeout(), async {
            let plan = self.0.search_plan(request).await?;
            execute(storage, plan).await
        })
        .await
        .map_err(|_| err("query deadline exceeded"))?
    }
    /// Count visible rows, optionally under a SQL filter.
    pub async fn count(
        &self,
        storage: &LanceStorageOptions,
        filter: Option<&str>,
    ) -> lance::Result<u64> {
        tokio::time::timeout(storage.query_timeout(), self.0.count(filter))
            .await
            .map_err(|_| err("query deadline exceeded"))?
    }
}
impl std::fmt::Debug for TableSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableSnapshot").finish_non_exhaustive()
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

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        // The captured LSM scanner evaluates the pushed predicates before its
        // limit and preserves newest-per-primary-key semantics.  Returning
        // Exact makes DataFusion hand the predicates to `scan` instead of
        // inserting a FilterExec above an already materialized full snapshot.
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Exact)
            .collect())
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
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let plan = self.0.plan(filters, limit).await?;
        let plan = if let Some(indices) = projection {
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
            Arc::new(ProjectionExec::try_new(expressions, plan)?) as Arc<dyn ExecutionPlan>
        } else {
            plan
        };
        Ok(Arc::new(SnapshotExec(plan)))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Exact)
            .collect())
    }
}

/// Lance owns the internal LSM plan. Present it as a leaf after the source has
/// already applied the pushed filters so DataFusion cannot merge statistics
/// from internal generations or rewrite their newest-per-key semantics.
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

/// Table names a SQL statement reads, for routing a query to the node that
/// owns them. Names are returned as written (case preserved, unqualified).
pub fn sql_table_names(sql: &str) -> lance::Result<Vec<String>> {
    use lance::deps::datafusion::sql::{parser::DFParser, resolve::resolve_table_references};
    let statements = DFParser::parse_sql(sql).map_err(err)?;
    let mut names = Vec::new();
    for statement in &statements {
        let (tables, _ctes) = resolve_table_references(statement, true).map_err(err)?;
        for table in tables {
            let name = table.table().to_string();
            if !names.contains(&name) {
                names.push(name);
            }
        }
    }
    Ok(names)
}
