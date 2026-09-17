//! LanceDB remote protocol (`/v1/table/...`) so the stock `lancedb` SDKs work
//! against Walleye with `host_override`. Data travels as Arrow IPC; everything
//! else is JSON. Unsupported operations return 400 with a plain message.
// axum responses are the natural error type for handlers; boxing them buys nothing.
#![allow(clippy::result_large_err)]
use crate::{Service, api, engine};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::Deserialize;
use std::{io::Cursor, sync::Arc};
use walleye_lance::{JsonSchema, SearchRequest, VectorIndexSpec, VectorQuery};

const ARROW_FILE: &str = "application/vnd.apache.arrow.file";

pub fn routes() -> Router<Arc<Service>> {
    Router::new()
        .route("/v1/table/", get(list))
        // Walleye has no namespaces: every namespace id lists the root.
        .route(
            "/v1/namespace/{namespace}/table/list",
            get(list_in_namespace),
        )
        .route("/v1/table/{name}/create/", post(create))
        .route("/v1/table/{name}/describe/", post(describe))
        .route("/v1/table/{name}/drop/", post(drop))
        .route("/v1/table/{name}/insert/", post(insert))
        .route("/v1/table/{name}/query/", post(query))
        .route("/v1/table/{name}/count_rows/", post(count_rows))
        .route("/v1/table/{name}/create_index/", post(create_index))
        .route("/v1/table/{name}/index/list/", post(list_indices))
        .route("/v1/table/{name}/compact_lsm/", post(compact_lsm))
        .route("/v1/table/{name}/flush_lsm/", post(flush_lsm))
        .route("/v1/table/{name}/get_lsm_stats/", post(lsm_stats))
        .route("/v1/view/", get(list_views))
        .route("/v1/view/{name}/create/", post(create_view))
        .route("/v1/view/{name}/describe/", post(describe_view))
        .route("/v1/view/{name}/drop/", post(drop_view))
        .route("/v1/view/{name}/refresh/", post(refresh_view))
        .route(
            "/v1/worker/{name}/",
            get(worker_request)
                .post(worker_request)
                .put(worker_request)
                .delete(worker_request),
        )
        .layer(DefaultBodyLimit::max(512 * 1024 * 1024))
}

type Reply = Result<Response, Response>;

/// The SDK matches on status and, for create, on "already exists" in the body.
/// As [`error`], but for a write: a writer fenced by its own WAL persistence
/// failure leaves the outcome unknown rather than failed, and a client told
/// "failed" would retry a write that may already have been stored.
fn write_error(e: &(dyn std::error::Error + Send + Sync + 'static)) -> Response {
    if walleye_lance::writer_fence_reason(e) == Some(walleye_lance::FenceReason::PersistenceFailure)
    {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [("retry-after", "1")],
            format!(
                "the outcome of this write is unknown: {e}. Read the rows back before retrying; \
                 a retry of the same rows is safe when they carry a primary key."
            ),
        )
            .into_response();
    }
    error(e)
}
fn error(e: &(dyn std::error::Error + 'static)) -> Response {
    let status = if e.downcast_ref::<engine::TableNotFound>().is_some() {
        StatusCode::NOT_FOUND
    } else if e.downcast_ref::<crate::cluster::NotOwner>().is_some() {
        StatusCode::CONFLICT
    } else {
        StatusCode::BAD_REQUEST
    };
    (status, e.to_string()).into_response()
}
fn bad(message: impl Into<String>) -> Response {
    (StatusCode::BAD_REQUEST, message.into()).into_response()
}
fn engine<'a>(s: &'a Service, h: &HeaderMap) -> Result<&'a engine::Engine, Response> {
    api(s, h).map_err(|(status, body)| (status, body).into_response())
}
/// The engine for a durable write; 503 with `Retry-After` while this node's
/// replica quorum is unreachable.
fn writable_engine<'a>(s: &'a Service, h: &HeaderMap) -> Result<&'a engine::Engine, Response> {
    crate::writable(s, h)
        .map_err(|(status, body)| (status, [("retry-after", "1")], body).into_response())
}
/// An inbound Arrow body is held raw and decoded at once; reserve both before
/// decoding so an oversize insert is refused instead of exceeding the budget.
fn body_lease(
    engine: &engine::Engine,
    body: &Bytes,
) -> Result<walleye_cache::MemoryLease, Response> {
    engine
        .resources()
        .reserve_memory("request body", body.len().saturating_mul(2))
        .map_err(|e| (StatusCode::PAYLOAD_TOO_LARGE, e.to_string()).into_response())
}
fn read_ipc(body: &Bytes) -> Result<(Arc<Schema>, Vec<RecordBatch>), Response> {
    let reader = arrow_ipc::reader::StreamReader::try_new(Cursor::new(body.as_ref()), None)
        .map_err(|e| bad(format!("invalid Arrow IPC stream: {e}")))?;
    let schema = reader.schema();
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| bad(format!("invalid Arrow IPC stream: {e}")))?;
    Ok((schema, batches))
}
fn write_ipc(schema: &Arc<Schema>, batches: &[RecordBatch]) -> Result<Response, Response> {
    let mut out = Vec::new();
    {
        let mut writer = arrow_ipc::writer::FileWriter::try_new(&mut out, schema)
            .map_err(|e| bad(e.to_string()))?;
        for batch in batches {
            writer.write(batch).map_err(|e| bad(e.to_string()))?;
        }
        writer.finish().map_err(|e| bad(e.to_string()))?;
    }
    Ok(([("content-type", ARROW_FILE)], out).into_response())
}

#[derive(Deserialize)]
struct Page {
    limit: Option<usize>,
    page_token: Option<String>,
}
async fn list(State(s): State<Arc<Service>>, h: HeaderMap, Query(page): Query<Page>) -> Reply {
    let engine = engine(&s, &h)?;
    let mut names = engine.table_names().await.map_err(|e| error(e.as_ref()))?;
    if let Some(after) = page.page_token.filter(|t| !t.is_empty()) {
        names.retain(|n| n > &after);
    }
    let mut token = String::new();
    if let Some(limit) = page.limit
        && names.len() > limit
    {
        names.truncate(limit);
        token = names.last().cloned().unwrap_or_default();
    }
    Ok(Json(serde_json::json!({"tables": names, "page_token": token})).into_response())
}

async fn list_in_namespace(
    state: State<Arc<Service>>,
    Path(_namespace): Path<String>,
    h: HeaderMap,
    page: Query<Page>,
) -> Reply {
    list(state, h, page).await
}

#[derive(Deserialize)]
struct Mode {
    mode: Option<String>,
}
async fn create(
    State(s): State<Arc<Service>>,
    Path(name): Path<String>,
    h: HeaderMap,
    Query(mode): Query<Mode>,
    body: Bytes,
) -> Reply {
    let engine = writable_engine(&s, &h)?;
    let _lease = body_lease(engine, &body)?;
    let (schema, batches) = read_ipc(&body)?;
    let definition =
        engine::StreamDefinition::from_arrow(&name, &schema).map_err(|e| error(e.as_ref()))?;
    let mode = mode.mode.unwrap_or_else(|| "create".into());
    let exist_ok = match mode.as_str() {
        "create" => false,
        "exist_ok" => true,
        "overwrite" => {
            match engine.drop_table(&name).await {
                Ok(()) => {}
                Err(e) if e.downcast_ref::<engine::TableNotFound>().is_some() => {}
                Err(e) => return Err(error(e.as_ref())),
            }
            false
        }
        other => return Err(bad(format!("unknown create mode {other}"))),
    };
    let mut revision = s.revision.lock().await;
    *revision = format!("\"{}\"", uuid::Uuid::new_v4());
    engine
        .define_with(definition, exist_ok)
        .await
        .map_err(|e| error(e.as_ref()))?;
    let version = engine
        .append(&name, batches)
        .await
        .map_err(|e| write_error(e.as_ref()))?;
    Ok(Json(serde_json::json!({"version": version})).into_response())
}

async fn describe(State(s): State<Arc<Service>>, Path(name): Path<String>, h: HeaderMap) -> Reply {
    let engine = engine(&s, &h)?;
    let (version, schema) = engine
        .describe(&name)
        .await
        .map_err(|e| error(e.as_ref()))?;
    let schema = JsonSchema::try_from(&schema).map_err(|e| bad(e.to_string()))?;
    Ok(Json(serde_json::json!({"version": version, "schema": schema})).into_response())
}

async fn drop(State(s): State<Arc<Service>>, Path(name): Path<String>, h: HeaderMap) -> Reply {
    let engine = writable_engine(&s, &h)?;
    let mut revision = s.revision.lock().await;
    *revision = format!("\"{}\"", uuid::Uuid::new_v4());
    engine
        .drop_table(&name)
        .await
        .map_err(|e| error(e.as_ref()))?;
    Ok(Json(serde_json::json!({})).into_response())
}

async fn insert(
    State(s): State<Arc<Service>>,
    Path(name): Path<String>,
    h: HeaderMap,
    Query(mode): Query<Mode>,
    body: Bytes,
) -> Reply {
    let engine = writable_engine(&s, &h)?;
    if mode.mode.as_deref() == Some("overwrite") {
        return Err(bad(
            "insert mode=overwrite is not supported; drop and recreate the table",
        ));
    }
    let _lease = body_lease(engine, &body)?;
    let (_, batches) = read_ipc(&body)?;
    let mut revision = s.revision.lock().await;
    *revision = format!("\"{}\"", uuid::Uuid::new_v4());
    let version = engine
        .append(&name, batches)
        .await
        .map_err(|e| write_error(e.as_ref()))?;
    s.changed.notify_one();
    Ok(Json(serde_json::json!({"version": version})).into_response())
}

/// The SDK always sends `k`; a plain scan uses a huge sentinel.
fn limit_from_k(k: Option<u64>) -> Option<usize> {
    k.filter(|&k| k < (i64::MAX / 2) as u64).map(|k| k as usize)
}
fn vector_column(schema: &Schema, requested: Option<&str>) -> Result<String, Response> {
    let is_vector = |dt: &DataType| matches!(dt, DataType::FixedSizeList(inner, _) if inner.data_type() == &DataType::Float32);
    if let Some(column) = requested {
        let field = schema
            .field_with_name(column)
            .map_err(|_| bad(format!("unknown vector column {column}")))?;
        if !is_vector(field.data_type()) {
            return Err(bad(format!(
                "{column} is not a FixedSizeList<Float32> column"
            )));
        }
        return Ok(column.into());
    }
    let candidates: Vec<_> = schema
        .fields()
        .iter()
        .filter(|f| is_vector(f.data_type()))
        .map(|f| f.name().clone())
        .collect();
    match candidates.as_slice() {
        [one] => Ok(one.clone()),
        [] => Err(bad("table has no vector column")),
        _ => Err(bad("table has several vector columns; set vector_column")),
    }
}
async fn query(
    State(s): State<Arc<Service>>,
    Path(name): Path<String>,
    h: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Reply {
    let engine = engine(&s, &h)?;
    if body.get("full_text_query").is_some_and(|v| !v.is_null()) {
        return Err(bad("full-text search is not supported yet"));
    }
    if body.get("order_by").is_some_and(|v| !v.is_null()) {
        return Err(bad("order_by is not supported; use SQL via /v1/query"));
    }
    let mut request = SearchRequest {
        filter: body
            .get("filter")
            .and_then(|v| v.as_str())
            .map(str::to_string),
        limit: limit_from_k(body.get("k").and_then(|v| v.as_u64())),
        offset: body
            .get("offset")
            .and_then(|v| v.as_u64())
            .map(|o| o as usize),
        ..Default::default()
    };
    match body.get("columns") {
        None | Some(serde_json::Value::Null) => {}
        Some(serde_json::Value::Array(items)) => {
            request.columns = Some(
                items
                    .iter()
                    .map(|v| v.as_str().map(str::to_string))
                    .collect::<Option<Vec<_>>>()
                    .ok_or_else(|| bad("columns must be strings"))?,
            );
        }
        Some(_) => {
            return Err(bad(
                "column expressions are not supported; use SQL via /v1/query",
            ));
        }
    }
    let vector: Vec<f32> = match body.get("vector") {
        None | Some(serde_json::Value::Null) => Vec::new(),
        Some(serde_json::Value::Array(items)) => {
            if items.iter().any(|v| v.is_array()) {
                return Err(bad("multivector queries are not supported"));
            }
            items
                .iter()
                .map(|v| v.as_f64().map(|f| f as f32))
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| bad("vector must contain numbers"))?
        }
        Some(_) => return Err(bad("vector must be an array")),
    };
    if !vector.is_empty() {
        let (_, schema) = engine
            .describe(&name)
            .await
            .map_err(|e| error(e.as_ref()))?;
        let column = vector_column(&schema, body.get("vector_column").and_then(|v| v.as_str()))?;
        let k = request.limit.unwrap_or(10);
        // The metric belongs to the index. A query may restate it but not
        // change it: an index built for one metric answers nothing for another.
        let specs = engine
            .vector_indexes(&name)
            .await
            .map_err(|e| error(e.as_ref()))?;
        let indexed = specs
            .iter()
            .find(|s| s.column == column)
            .map(|s| s.metric.clone());
        let requested = body
            .get("distance_type")
            .and_then(|v| v.as_str())
            .map(|m| m.to_lowercase());
        let metric = match (indexed, requested) {
            (Some(index), Some(query)) if index != query => {
                return Err(bad(format!(
                    "column {column} is indexed with metric {index}; create_index with \
                     metric_type={query} to change it"
                )));
            }
            (Some(index), _) => Some(index),
            (None, query) => query,
        };
        request.vector = Some(VectorQuery {
            column,
            vector,
            k,
            nprobes: body
                .get("minimum_nprobes")
                .or_else(|| body.get("nprobes"))
                .and_then(|v| v.as_u64())
                .unwrap_or(20) as usize,
            refine_factor: body
                .get("refine_factor")
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as u32,
            metric,
            // The SDK sends null unless the caller set ef. A single large
            // graph needs a wider beam than Lance's default to keep recall.
            ef: Some(
                body.get("ef")
                    .and_then(|v| v.as_u64())
                    .map(|e| e as usize)
                    .unwrap_or(100)
                    .max(k),
            ),
        });
        // The nearest-neighbor plan applies k; a separate limit would double-apply.
        request.limit = None;
    }
    let batches = engine
        .search(&name, &request)
        .await
        .map_err(|e| error(e.as_ref()))?;
    let schema = match batches.first() {
        Some(b) => b.schema(),
        None => {
            let (_, schema) = engine
                .describe(&name)
                .await
                .map_err(|e| error(e.as_ref()))?;
            Arc::new(match &request.columns {
                Some(columns) => schema
                    .project(
                        &columns
                            .iter()
                            .map(|c| schema.index_of(c))
                            .collect::<Result<Vec<_>, _>>()
                            .map_err(|e| bad(e.to_string()))?,
                    )
                    .map_err(|e| bad(e.to_string()))?,
                None => schema,
            })
        }
    };
    write_ipc(&schema, &batches)
}

async fn count_rows(
    State(s): State<Arc<Service>>,
    Path(name): Path<String>,
    h: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Reply {
    let engine = engine(&s, &h)?;
    let filter = body.get("predicate").and_then(|v| v.as_str());
    let count = engine
        .count(&name, filter)
        .await
        .map_err(|e| error(e.as_ref()))?;
    Ok(Json(serde_json::json!(count)).into_response())
}

#[derive(Deserialize)]
struct CreateIndex {
    column: String,
    index_type: Option<String>,
    metric_type: Option<String>,
    name: Option<String>,
}
/// Every vector index type maps to the one layout Walleye maintains: an HNSW
/// graph on the memtable, IVF_HNSW_SQ on each generation.
async fn create_index(
    State(s): State<Arc<Service>>,
    Path(name): Path<String>,
    h: HeaderMap,
    Json(body): Json<CreateIndex>,
) -> Reply {
    let engine = writable_engine(&s, &h)?;
    let kind = body
        .index_type
        .unwrap_or_else(|| "IVF_PQ".into())
        .to_uppercase();
    match kind.as_str() {
        "FTS" => return Err(bad("full-text indexes are not supported yet")),
        k if k.starts_with("IVF") || k.starts_with("HNSW") => {}
        other => {
            return Err(bad(format!(
                "index type {other} is not supported; vector columns use IVF_HNSW_SQ"
            )));
        }
    }
    let spec = VectorIndexSpec {
        name: body.name.unwrap_or_else(|| format!("{}_idx", body.column)),
        column: body.column,
        metric: body
            .metric_type
            .unwrap_or_else(|| "l2".into())
            .to_lowercase(),
    };
    let mut revision = s.revision.lock().await;
    *revision = format!("\"{}\"", uuid::Uuid::new_v4());
    engine
        .configure_vector_index(&name, spec)
        .await
        .map_err(|e| error(e.as_ref()))?;
    Ok(Json(serde_json::json!({})).into_response())
}

async fn list_indices(
    State(s): State<Arc<Service>>,
    Path(name): Path<String>,
    h: HeaderMap,
) -> Reply {
    let engine = engine(&s, &h)?;
    let indexes: Vec<_> = engine
        .vector_indexes(&name)
        .await
        .map_err(|e| error(e.as_ref()))?
        .into_iter()
        .map(|i| {
            serde_json::json!({
                "index_name": i.name,
                "columns": [i.column],
                "index_type": "IVF_HNSW_SQ",
                "distance_type": i.metric,
            })
        })
        .collect();
    Ok(Json(serde_json::json!({"indexes": indexes})).into_response())
}

async fn compact_lsm(
    State(s): State<Arc<Service>>,
    Path(name): Path<String>,
    h: HeaderMap,
) -> Reply {
    let engine = writable_engine(&s, &h)?;
    let result = engine.compact(&name).await.map_err(|e| error(e.as_ref()))?;
    Ok(Json(match result {
        Some(r) => serde_json::json!({"merged": r.merged.len(), "rows": r.rows, "generation": r.output.generation}),
        None => serde_json::json!({"merged": 0}),
    })
    .into_response())
}

async fn flush_lsm(State(s): State<Arc<Service>>, Path(name): Path<String>, h: HeaderMap) -> Reply {
    let engine = writable_engine(&s, &h)?;
    engine.flush(&name).await.map_err(|e| error(e.as_ref()))?;
    Ok(Json(serde_json::json!({})).into_response())
}

async fn lsm_stats(State(s): State<Arc<Service>>, Path(name): Path<String>, h: HeaderMap) -> Reply {
    let engine = engine(&s, &h)?;
    let stats = engine
        .lsm_stats(&name)
        .await
        .map_err(|e| error(e.as_ref()))?;
    Ok(Json(serde_json::to_value(stats).map_err(|e| bad(e.to_string()))?).into_response())
}

/// How far to drive a view in one request. A refresh does bounded work so a
/// call cannot run away; a caller that wants a tier fully caught up asks
/// again until it reports nothing left.
#[derive(Deserialize)]
struct Passes {
    #[serde(default)]
    passes: Option<usize>,
}

async fn list_views(State(s): State<Arc<Service>>, h: HeaderMap) -> Reply {
    let engine = engine(&s, &h)?;
    let names = engine.view_names().await.map_err(|e| error(e.as_ref()))?;
    Ok(Json(serde_json::json!({ "views": names })).into_response())
}

async fn create_view(
    State(s): State<Arc<Service>>,
    Path(name): Path<String>,
    h: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Reply {
    let engine = writable_engine(&s, &h)?;
    let mut view: engine::ViewDefinition = serde_json::from_value(
        // The name lives in the path, so a body that repeats it is accepted
        // and a body that omits it is too.
        match body {
            serde_json::Value::Object(mut fields) => {
                fields.insert("name".into(), serde_json::Value::String(name.clone()));
                serde_json::Value::Object(fields)
            }
            other => other,
        },
    )
    .map_err(|e| bad(e.to_string()))?;
    view.name = name;
    engine
        .define_view(view)
        .await
        .map_err(|e| error(e.as_ref()))?;
    Ok(Json(serde_json::json!({ "created": true })).into_response())
}

async fn describe_view(
    State(s): State<Arc<Service>>,
    Path(name): Path<String>,
    h: HeaderMap,
) -> Reply {
    let engine = engine(&s, &h)?;
    let view = engine.view(&name).await.map_err(|e| error(e.as_ref()))?;
    let cursor = engine
        .cursor(&format!("view:{name}"))
        .await
        .map_err(|e| error(e.as_ref()))?;
    let mut described = serde_json::to_value(&view).map_err(|e| bad(e.to_string()))?;
    if let Some(fields) = described.as_object_mut() {
        fields.insert("cursor".into(), serde_json::json!(cursor));
    }
    Ok(Json(described).into_response())
}

async fn drop_view(State(s): State<Arc<Service>>, Path(name): Path<String>, h: HeaderMap) -> Reply {
    let engine = writable_engine(&s, &h)?;
    engine
        .drop_view(&name)
        .await
        .map_err(|e| error(e.as_ref()))?;
    Ok(Json(serde_json::json!({ "dropped": true })).into_response())
}

async fn refresh_view(
    State(s): State<Arc<Service>>,
    Path(name): Path<String>,
    h: HeaderMap,
    passes: Query<Passes>,
) -> Reply {
    let engine = writable_engine(&s, &h)?;
    let progress = engine
        .drain_view(&name, passes.passes.unwrap_or(1))
        .await
        .map_err(|e| write_error(e.as_ref()))?;
    let mut revision = s.revision.lock().await;
    *revision = format!("\"{}\"", uuid::Uuid::new_v4());
    Ok(Json(serde_json::to_value(progress).map_err(|e| bad(e.to_string()))?).into_response())
}

/// Hand an inbound request to a view's worker.
///
/// The worker sees the method, the headers and the body, and answers with a
/// status, headers and a body of its own. Anything it wrote is durable before
/// this returns, so a webhook that acknowledges has already stored what it
/// acknowledged.
async fn worker_request(
    State(s): State<Arc<Service>>,
    Path(name): Path<String>,
    method: axum::http::Method,
    h: HeaderMap,
    body: Bytes,
) -> Reply {
    let engine = writable_engine(&s, &h)?;
    let headers: std::collections::HashMap<String, String> = h
        .iter()
        // The deployment token is the node's business, not the worker's.
        .filter(|(name, _)| !matches!(name.as_str(), "authorization" | "x-api-key" | "cookie"))
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_owned(), value.to_owned()))
        })
        .collect();
    let request = serde_json::json!({
        "method": method.as_str(),
        "headers": headers,
        "body": String::from_utf8_lossy(&body),
    });
    let answered = engine
        .handle_request(&name, request)
        .await
        .map_err(|e| write_error(e.as_ref()))?;

    let status = answered
        .get("status")
        .and_then(serde_json::Value::as_u64)
        .and_then(|status| u16::try_from(status).ok())
        .and_then(|status| StatusCode::from_u16(status).ok())
        .unwrap_or(StatusCode::OK);
    let mut response = match answered.get("body") {
        Some(serde_json::Value::String(text)) => text.clone().into_response(),
        Some(other) => Json(other.clone()).into_response(),
        None => ().into_response(),
    };
    *response.status_mut() = status;
    if let Some(serde_json::Value::Object(fields)) = answered.get("headers") {
        for (name, value) in fields {
            if let (Ok(name), Some(value)) = (
                axum::http::HeaderName::try_from(name.as_str()),
                value.as_str().and_then(|v| v.parse().ok()),
            ) {
                response.headers_mut().insert(name, value);
            }
        }
    }
    Ok(response)
}
