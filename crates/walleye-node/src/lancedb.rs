//! LanceDB remote protocol (`/v1/table/...`) so the stock `lancedb` SDKs work
//! against Walleye with `host_override`. Data travels as Arrow IPC; everything
//! else is JSON. Unsupported operations return 400 with a plain message.
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
        .layer(DefaultBodyLimit::max(512 * 1024 * 1024))
}

type Reply = Result<Response, Response>;

/// The SDK matches on status and, for create, on "already exists" in the body.
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
    let engine = engine(&s, &h)?;
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
        .map_err(|e| error(e.as_ref()))?;
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
    let engine = engine(&s, &h)?;
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
    let engine = engine(&s, &h)?;
    if mode.mode.as_deref() == Some("overwrite") {
        return Err(bad(
            "insert mode=overwrite is not supported; drop and recreate the table",
        ));
    }
    let (_, batches) = read_ipc(&body)?;
    let mut revision = s.revision.lock().await;
    *revision = format!("\"{}\"", uuid::Uuid::new_v4());
    let version = engine
        .append(&name, batches)
        .await
        .map_err(|e| error(e.as_ref()))?;
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
    let engine = engine(&s, &h)?;
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
    let engine = engine(&s, &h)?;
    let result = engine.compact(&name).await.map_err(|e| error(e.as_ref()))?;
    Ok(Json(match result {
        Some(r) => serde_json::json!({"merged": r.merged.len(), "rows": r.rows, "generation": r.output.generation}),
        None => serde_json::json!({"merged": 0}),
    })
    .into_response())
}

async fn flush_lsm(State(s): State<Arc<Service>>, Path(name): Path<String>, h: HeaderMap) -> Reply {
    let engine = engine(&s, &h)?;
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
