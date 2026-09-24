//! Seeing data: from a question, a statement or a whole table to a dashboard.
//!
//! A dashboard is panels - each one query, with what it was asked as - and a
//! spec saying how to draw them, in the json-render format. Drawing a saved
//! dashboard runs its queries again and binds their rows into the spec by
//! `$state`, so it costs what the queries cost and nothing else.
//!
//! Choosing how to draw is composing, and it is split along what each side
//! knows. The engine has the rows, so it works out what each query could be
//! drawn as: a line for a measure over time, bars for a measure across
//! categories, one number for one row, and so on, each described in words.
//! Jev has judgement, so it picks among those for what was asked and lays
//! them out. The picking runs json-render's own composer in a V8 worker,
//! reaching Jev through the host, so the key stays here and the rows never
//! leave: the composer only ever sees descriptions of them.
//!
//! Without Jev configured the engine's own first choice for each query is
//! used, in the order the queries were given, and the dashboard says so.
use crate::engine::Engine;
use object_store::{ObjectStore as _, ObjectStoreExt as _};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

type Error = Box<dyn std::error::Error + Send + Sync>;

/// json-render's composer and the catalog, bundled into one worker module by
/// `composer/build.mjs`.
const COMPOSER: &str = include_str!("see/composer.js");
/// Rows a panel draws at most. A chart of more points than this is not read
/// point by point, and a table of more is a query, not a dashboard.
pub const PANEL_ROWS: usize = 1000;
/// Queries a table's own dashboard runs.
const TABLE_PANELS: usize = 8;
/// Series one line or bar chart shows before it stops being readable.
const SERIES: usize = 4;
/// Columns a table panel shows.
const TABLE_COLUMNS: usize = 12;
/// Categories a pie can show before its slices stop being comparable.
const SLICES: usize = 6;

/// One query on a dashboard.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Panel {
    /// Where its rows are bound in the spec: `/<id>`.
    pub id: String,
    pub title: String,
    pub sql: String,
    /// What it was asked as, when it was asked in words.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub question: Option<String>,
}

/// A saved dashboard: its panels and how to draw them.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Dashboard {
    pub name: String,
    pub title: String,
    /// What it was made for, which the composer read.
    pub prompt: String,
    pub panels: Vec<Panel>,
    /// A json-render spec without state: `root` and `elements`.
    pub spec: Value,
    /// What each element shows, in words, so a later edit can talk about it.
    #[serde(default)]
    pub descriptions: BTreeMap<String, String>,
    /// `jev` when Jev chose and laid out the panels; `rules` when the engine
    /// took its own first choice for each.
    pub composed_by: String,
    /// For a table's own dashboard, the table's columns when it was made; a
    /// table whose columns changed gets a new one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<String>>,
    pub updated_ms: u64,
}

/// A dashboard drawn: the spec with every panel's rows in its state.
#[derive(Debug, Serialize)]
pub struct Drawn {
    pub name: String,
    pub title: String,
    pub prompt: String,
    pub panels: Vec<Panel>,
    /// `root`, `elements` and `state`: ready for a json-render renderer.
    pub spec: Value,
    pub descriptions: BTreeMap<String, String>,
    pub composed_by: String,
    /// Panels whose query failed, and why. They draw with no rows.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub errors: BTreeMap<String, String>,
    pub milliseconds: u64,
}

// --- what a result could be drawn as ---------------------------------------

/// What a column of a result is, for drawing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Role {
    /// A point in time: an axis.
    Time,
    /// A number: something to plot.
    Measure,
    /// A label with few enough values to group by.
    Category,
    /// A label with too many values to group by.
    Text,
}

#[derive(Clone, Debug)]
struct Column {
    name: String,
    role: Role,
    distinct: usize,
}

/// A result, in its columns' order. JSON objects do not keep their keys'
/// order once parsed, and a statement's column order is part of what it
/// says, so rows are read as ordered pairs.
struct Frame {
    columns: Vec<String>,
    rows: Vec<Vec<(String, Value)>>,
}

impl Frame {
    fn parse(bytes: &[u8]) -> Result<Self, Error> {
        let rows: Vec<Ordered> = serde_json::from_slice(bytes)?;
        let mut columns: Vec<String> = Vec::new();
        let mut seen = BTreeSet::new();
        for row in &rows {
            for (name, _) in &row.0 {
                if seen.insert(name.clone()) {
                    columns.push(name.clone());
                }
            }
        }
        Ok(Self {
            columns,
            rows: rows.into_iter().map(|row| row.0).collect(),
        })
    }

    fn values<'a>(&'a self, column: &'a str) -> impl Iterator<Item = &'a Value> + 'a {
        self.rows.iter().filter_map(move |row| {
            row.iter()
                .find(|(name, _)| name == column)
                .map(|(_, value)| value)
                .filter(|value| !value.is_null())
        })
    }

    /// The rows as plain objects, which is what a renderer binds.
    fn records(&self) -> Value {
        Value::Array(
            self.rows
                .iter()
                .map(|row| Value::Object(row.iter().cloned().collect::<Map<_, _>>()))
                .collect(),
        )
    }

    fn profile(&self) -> Vec<Column> {
        self.columns
            .iter()
            .map(|name| {
                let values: Vec<&Value> = self.values(name).collect();
                let distinct = values
                    .iter()
                    .map(|value| value.to_string())
                    .collect::<BTreeSet<_>>()
                    .len();
                let role = if !values.is_empty() && values.iter().all(|v| v.is_number()) {
                    // A number that names a row is a label, not a quantity.
                    if is_identifier(name) {
                        Role::Text
                    } else {
                        Role::Measure
                    }
                } else if !values.is_empty()
                    && values.iter().all(|v| v.as_str().is_some_and(is_time))
                {
                    Role::Time
                } else if distinct <= 50 || distinct * 2 <= values.len() {
                    Role::Category
                } else {
                    Role::Text
                };
                Column {
                    name: name.clone(),
                    role,
                    distinct,
                }
            })
            .collect()
    }
}

struct Ordered(Vec<(String, Value)>);
impl<'de> Deserialize<'de> for Ordered {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visit;
        impl<'de> serde::de::Visitor<'de> for Visit {
            type Value = Ordered;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a row")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<Ordered, A::Error> {
                let mut pairs = Vec::new();
                while let Some(pair) = map.next_entry::<String, Value>()? {
                    pairs.push(pair);
                }
                Ok(Ordered(pairs))
            }
        }
        deserializer.deserialize_map(Visit)
    }
}

/// Whether text is a date or a time, as a result writes one.
fn is_time(text: &str) -> bool {
    chrono::DateTime::parse_from_rfc3339(text).is_ok()
        || chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f").is_ok()
        || chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f").is_ok()
        || chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d").is_ok()
}

/// A json-render composition candidate: an element the composer may place,
/// which it cannot change, described in words.
fn candidate(id: String, description: String, kind: &str, props: Value, resource: &str) -> Value {
    json!({
        "id": id,
        "description": description,
        "element": { "type": kind, "props": props },
        "resource": resource,
    })
}

/// What a panel's rows could be drawn as, best first. They share the panel
/// as a resource, so a dashboard draws each panel once.
fn candidates(panel: &Panel, frame: &Frame) -> Vec<Value> {
    let columns = frame.profile();
    let rows = frame.rows.len();
    let data = json!({ "$state": format!("/{}", panel.id) });
    let of = |role: Role| -> Vec<&Column> { columns.iter().filter(|c| c.role == role).collect() };
    let (times, measures, categories) = (of(Role::Time), of(Role::Measure), of(Role::Category));
    let names = |cols: &[&Column]| -> Vec<String> {
        cols.iter().take(SERIES).map(|c| c.name.clone()).collect()
    };
    let title = &panel.title;
    let id = &panel.id;
    let mut out = Vec::new();

    // A chart has one axis and measures along it. A result with anything
    // else in it - a second label, a name, an id - is records, which only a
    // table shows faithfully: a line through rows of mixed labels draws a
    // series that is not there.
    let only = |axis: usize| columns.len() == axis + measures.len();
    if rows == 1 && measures.len() == 1 && columns.len() == 1 {
        out.push(candidate(
            format!("{id}-metric"),
            format!("{title}: one number, {}", measures[0].name),
            "Metric",
            json!({ "title": title, "data": data, "value": measures[0].name }),
            id,
        ));
    }
    if let (Some(time), false, true) = (times.first(), measures.is_empty(), only(1)) {
        let span = span(frame, &time.name);
        out.push(candidate(
            format!("{id}-line"),
            format!(
                "{title}: a line of {} over {}, {rows} points{span}",
                names(&measures).join(", "),
                time.name
            ),
            "LineChart",
            json!({ "title": title, "data": data, "x": time.name, "y": names(&measures) }),
            id,
        ));
        out.push(candidate(
            format!("{id}-bars_over_time"),
            format!(
                "{title}: bars of {} for each {}, {rows} bars{span}",
                names(&measures).join(", "),
                time.name
            ),
            "BarChart",
            json!({ "title": title, "data": data, "x": time.name, "y": names(&measures) }),
            id,
        ));
    }
    if let (Some(category), false, true) = (categories.first(), measures.is_empty(), only(1)) {
        out.push(candidate(
            format!("{id}-bars"),
            format!(
                "{title}: bars comparing {} across {} {}",
                names(&measures).join(", "),
                category.distinct,
                category.name
            ),
            "BarChart",
            json!({ "title": title, "data": data, "x": category.name, "y": names(&measures) }),
            id,
        ));
        if category.distinct <= SLICES && rows <= SLICES && measures.len() == 1 {
            out.push(candidate(
                format!("{id}-pie"),
                format!(
                    "{title}: a pie of each {}'s share of {}",
                    category.name, measures[0].name
                ),
                "PieChart",
                json!({ "title": title, "data": data, "label": category.name, "value": measures[0].name }),
                id,
            ));
        }
    }
    if measures.len() >= 2 && only(0) && rows > 1 {
        out.push(candidate(
            format!("{id}-scatter"),
            format!(
                "{title}: {} against {}, one point per row, {rows} points",
                measures[1].name, measures[0].name
            ),
            "ScatterChart",
            json!({ "title": title, "data": data, "x": measures[0].name, "y": measures[1].name }),
            id,
        ));
    }
    let shown: Vec<String> = frame.columns.iter().take(TABLE_COLUMNS).cloned().collect();
    if !shown.is_empty() {
        out.push(candidate(
            format!("{id}-table"),
            format!(
                "{title}: the rows as a table, {rows} rows of {}",
                shown.join(", ")
            ),
            "Table",
            json!({ "title": title, "data": data, "columns": shown }),
            id,
        ));
    }
    out
}

/// ", from A to B" for a time column, or nothing.
fn span(frame: &Frame, column: &str) -> String {
    let mut times: Vec<&str> = frame.values(column).filter_map(Value::as_str).collect();
    times.sort_unstable();
    match (times.first(), times.last()) {
        (Some(first), Some(last)) if first != last => format!(", from {first} to {last}"),
        _ => String::new(),
    }
}

/// The candidate that holds the panels.
fn grid(panels: usize) -> Value {
    json!({
        "id": "grid",
        "description": "A grid holding the panels, two to a row",
        "element": { "type": "Grid", "props": { "columns": if panels > 1 { 2 } else { 1 } } },
        "maxUses": 1,
    })
}

// --- composing ----------------------------------------------------------------

fn jev() -> Option<&'static walleye_typesafe::Client> {
    static CLIENT: OnceLock<Option<walleye_typesafe::Client>> = OnceLock::new();
    CLIENT
        .get_or_init(walleye_typesafe::Client::from_env)
        .as_ref()
}

/// The composer's one way out: its questions, to Jev.
struct Judge {
    client: &'static walleye_typesafe::Client,
    runtime: tokio::runtime::Handle,
    rounds: AtomicUsize,
    failed: AtomicBool,
}

impl walleye_v8::Host for Judge {
    fn call(&self, request: &str) -> Result<String, String> {
        let request: Value = serde_json::from_str(request).map_err(|e| e.to_string())?;
        let asked = &request["jev"];
        let questions: BTreeMap<String, walleye_typesafe::Question> =
            serde_json::from_value(asked["questions"].clone()).map_err(|e| e.to_string())?;
        let state = asked["state"].to_string();
        self.rounds.fetch_add(1, Ordering::Relaxed);
        let decision = self
            .runtime
            .block_on(self.client.ask(&state, &questions))
            .map_err(|error| {
                self.failed.store(true, Ordering::Relaxed);
                error.to_string()
            })?;
        let mut answers = Map::new();
        for name in questions.keys() {
            let answer = decision
                .answers
                .get(name)
                .ok_or("Jev left a question unanswered")?;
            answers.insert(
                name.clone(),
                json!({ "choice": answer.label(), "confidence": answer.confidence() }),
            );
        }
        Ok(json!({ "answers": answers }).to_string())
    }
}

/// A spec and what each of its elements shows.
struct Composed {
    spec: Value,
    descriptions: BTreeMap<String, String>,
    by: &'static str,
}

/// Choose and lay out panels for `prompt` from `offered`, or edit `current`
/// to suit it. Jev decides when it is configured; otherwise, or if it fails,
/// each panel's first candidate is used, in order.
async fn compose(
    prompt: &str,
    offered: &[(Panel, Vec<Value>)],
    current: Option<(&Value, &BTreeMap<String, String>)>,
) -> Result<Composed, Error> {
    let mut candidates: Vec<Value> = offered.iter().flat_map(|(_, c)| c.clone()).collect();
    // The page is always a grid, even of one panel. Jev then picks each
    // panel's drawing by what the request needs, rather than as the page's
    // outermost element - a question about containers, which it answers
    // with whatever looks most like one - and a later edit has somewhere to
    // put a second panel beside the first instead of replacing it.
    for candidate in candidates.iter_mut() {
        candidate["root"] = json!(false);
    }
    if current.is_none() {
        candidates.insert(0, grid(offered.len()));
    }
    let described: HashMap<String, String> = candidates
        .iter()
        .map(|c| {
            (
                element_key(&c["element"]),
                c["description"].as_str().unwrap_or("").to_owned(),
            )
        })
        .collect();
    if let Some(client) = jev() {
        match compose_with(client, prompt, &candidates, current).await {
            Ok(spec) => {
                let mut descriptions = current.map(|(_, d)| d.clone()).unwrap_or_default();
                if let Some(elements) = spec["elements"].as_object() {
                    descriptions.retain(|id, _| elements.contains_key(id));
                    for (id, element) in elements {
                        if let Some(text) = described.get(&element_key(element)) {
                            descriptions.insert(id.clone(), text.clone());
                        }
                    }
                }
                return Ok(Composed {
                    spec,
                    descriptions,
                    by: "jev",
                });
            }
            Err(error) => {
                eprintln!("walleye.see compose outcome=rules error={error}");
            }
        }
    }
    Ok(by_rules(offered, current))
}

/// The key an element is recognised by: its type and props.
fn element_key(element: &Value) -> String {
    json!({ "type": element["type"], "props": element["props"] }).to_string()
}

async fn compose_with(
    client: &'static walleye_typesafe::Client,
    prompt: &str,
    candidates: &[Value],
    current: Option<(&Value, &BTreeMap<String, String>)>,
) -> Result<Value, Error> {
    // Each panel's rows as an empty list: the composer resolves bindings to
    // check props, and never shows state to Jev, so the rows stay here.
    let state: Map<String, Value> = candidates
        .iter()
        .filter_map(|c| c["resource"].as_str())
        .map(|panel| (panel.to_owned(), json!([])))
        .collect();
    let request = json!({
        "prompt": prompt,
        "candidates": candidates,
        "state": state,
        "spec": current.map(|(spec, _)| spec),
        "descriptions": current.map(|(_, descriptions)| descriptions),
        "maxElements": 16,
    })
    .to_string();
    let judge = Arc::new(Judge {
        client,
        runtime: tokio::runtime::Handle::current(),
        rounds: AtomicUsize::new(0),
        failed: AtomicBool::new(false),
    });
    let host: Arc<dyn walleye_v8::Host> = judge.clone();
    let limits = walleye_v8::Limits {
        heap_bytes: 128 * 1024 * 1024,
        deadline: std::time::Duration::from_secs(60),
    };
    let started = std::time::Instant::now();
    let outcome = tokio::task::spawn_blocking(move || {
        walleye_v8::run_request(COMPOSER, &request, limits, host, Default::default())
    })
    .await??;
    let answer: Value = serde_json::from_str(&outcome.returned)?;
    eprintln!(
        "walleye.see compose rounds={} stop={} valid={} elapsed_ms={}",
        judge.rounds.load(Ordering::Relaxed),
        answer["stopReason"],
        answer["valid"],
        started.elapsed().as_millis()
    );
    if answer["valid"] != json!(true) || !answer["spec"].is_object() {
        return Err(format!(
            "the composer could not draw that ({})",
            answer["stopReason"].as_str().unwrap_or("no reason")
        )
        .into());
    }
    Ok(answer["spec"].clone())
}

/// The engine's own choice: each panel's first candidate, in order, in a grid
/// when there are several. An edit keeps what is there and adds the new
/// panels after it.
fn by_rules(
    offered: &[(Panel, Vec<Value>)],
    current: Option<(&Value, &BTreeMap<String, String>)>,
) -> Composed {
    let mut elements = Map::new();
    let mut descriptions = BTreeMap::new();
    let mut children = Vec::new();
    let mut root = None;
    if let Some((spec, described)) = current {
        if let Some(existing) = spec["elements"].as_object() {
            elements = existing.clone();
        }
        descriptions = described.clone();
        root = spec["root"].as_str().map(str::to_owned);
    }
    let shown: BTreeSet<String> = elements
        .values()
        .filter_map(|element| {
            element["props"]["data"]["$state"]
                .as_str()
                .map(str::to_owned)
        })
        .collect();
    for (panel, candidates) in offered {
        if shown.contains(&format!("/{}", panel.id)) {
            continue;
        }
        let Some(first) = candidates.first() else {
            continue;
        };
        let id = format!("{}-{}", panel.id, elements.len());
        let mut element = first["element"].clone();
        element["children"] = json!([]);
        elements.insert(id.clone(), element);
        descriptions.insert(
            id.clone(),
            first["description"].as_str().unwrap_or("").to_owned(),
        );
        children.push(id);
    }
    let root = match root {
        Some(root) => {
            if let Some(list) = elements
                .get_mut(&root)
                .and_then(|r| r["children"].as_array_mut())
            {
                list.extend(children.into_iter().map(Value::String));
            }
            root
        }
        None => {
            elements.insert(
                "grid".into(),
                json!({ "type": "Grid", "props": { "columns": if children.len() > 1 { 2 } else { 1 } }, "children": children }),
            );
            descriptions.insert(
                "grid".into(),
                "A grid holding the panels, two to a row".into(),
            );
            "grid".into()
        }
    };
    Composed {
        spec: json!({ "root": root, "elements": elements }),
        descriptions,
        by: "rules",
    }
}

// --- running panels --------------------------------------------------------------

/// A panel's rows, at most [`PANEL_ROWS`] of them.
///
/// The limit goes on the end of the statement rather than around it: an
/// outer `SELECT * FROM (...) LIMIT n` is free to lose the inner ORDER BY,
/// and a panel's order is often its point. A statement that ends in a limit
/// of its own keeps it, and the rows are cut here either way.
async fn run(engine: &Engine, panel: &Panel) -> Result<Frame, Error> {
    let sql = panel.sql.trim().trim_end_matches(';').trim_end();
    let bounded = if ends_in_limit(sql) {
        sql.to_owned()
    } else {
        format!("{sql} LIMIT {PANEL_ROWS}")
    };
    let mut frame = Frame::parse(&engine.query(&bounded).await?)?;
    frame.rows.truncate(PANEL_ROWS);
    Ok(frame)
}

/// Whether a statement's last clause is `LIMIT n`, with or without an offset.
fn ends_in_limit(sql: &str) -> bool {
    let words: Vec<String> = sql
        .split_whitespace()
        .rev()
        .take(4)
        .map(str::to_ascii_uppercase)
        .collect();
    let number = |w: &String| !w.is_empty() && w.bytes().all(|b| b.is_ascii_digit());
    match words.as_slice() {
        [n, limit, ..] if number(n) && limit == "LIMIT" => true,
        [n, offset, m, limit] => number(n) && offset == "OFFSET" && number(m) && limit == "LIMIT",
        _ => false,
    }
}

async fn run_all(engine: &Engine, panels: &[Panel]) -> Vec<Result<Frame, Error>> {
    futures::future::join_all(panels.iter().map(|panel| run(engine, panel))).await
}

/// What each panel could be drawn as, from its rows. A panel whose query
/// fails is left out, with why.
async fn offer(
    engine: &Engine,
    panels: Vec<Panel>,
) -> (Vec<(Panel, Vec<Value>)>, BTreeMap<String, String>) {
    let mut offered = Vec::new();
    let mut errors = BTreeMap::new();
    for (panel, frame) in panels.iter().zip(run_all(engine, &panels).await) {
        match frame {
            Ok(frame) => {
                let candidates = candidates(panel, &frame);
                offered.push((panel.clone(), candidates));
            }
            Err(error) => {
                eprintln!(
                    "walleye.see panel={} outcome=error sql={:?} error={error}",
                    panel.id, panel.sql
                );
                errors.insert(panel.id.clone(), error.to_string());
            }
        }
    }
    (offered, errors)
}

/// Draw a dashboard: run its panels and bind their rows.
pub async fn draw(engine: &Engine, dashboard: Dashboard) -> Drawn {
    let started = std::time::Instant::now();
    let mut state = Map::new();
    let mut errors = BTreeMap::new();
    for (panel, frame) in dashboard
        .panels
        .iter()
        .zip(run_all(engine, &dashboard.panels).await)
    {
        match frame {
            Ok(frame) => {
                state.insert(panel.id.clone(), frame.records());
            }
            Err(error) => {
                state.insert(panel.id.clone(), json!([]));
                errors.insert(panel.id.clone(), error.to_string());
            }
        }
    }
    let mut spec = dashboard.spec.clone();
    spec["state"] = Value::Object(state);
    Drawn {
        name: dashboard.name,
        title: dashboard.title,
        prompt: dashboard.prompt,
        panels: dashboard.panels,
        spec,
        descriptions: dashboard.descriptions,
        composed_by: dashboard.composed_by,
        errors,
        milliseconds: started.elapsed().as_millis() as u64,
    }
}

fn now_ms() -> u64 {
    crate::engine::now_micros() / 1000
}

/// Only the panels some element draws.
fn keep_drawn(panels: Vec<Panel>, spec: &Value) -> Vec<Panel> {
    let bound: BTreeSet<String> = spec["elements"]
        .as_object()
        .map(|elements| {
            elements
                .values()
                .filter_map(|e| e["props"]["data"]["$state"].as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default();
    panels
        .into_iter()
        .filter(|panel| bound.contains(&format!("/{}", panel.id)))
        .collect()
}

/// A panel for a question, answered in words by text to SQL.
async fn asked(engine: &Engine, id: String, question: &str) -> Result<Panel, Error> {
    let answered = engine.answer(question).await?;
    Ok(Panel {
        id,
        title: title_of(question),
        sql: answered.sql,
        question: Some(question.trim().to_owned()),
    })
}

/// A short title from a question: its first sentence, capitalised, without
/// the question mark.
fn title_of(question: &str) -> String {
    let first = question
        .trim()
        .split(['?', '\n'])
        .next()
        .unwrap_or("")
        .trim();
    let mut chars = first.chars();
    let title: String = match chars.next() {
        Some(c) => c.to_uppercase().chain(chars).collect(),
        None => "Untitled".into(),
    };
    title.chars().take(80).collect()
}

/// What a dashboard is made from: questions to answer, statements to run, or
/// both, with titles.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    #[serde(default)]
    pub title: Option<String>,
    /// What it is for, when that is more than its questions.
    #[serde(default)]
    pub prompt: Option<String>,
    #[serde(default)]
    pub questions: Vec<String>,
    #[serde(default)]
    pub panels: Vec<PanelRequest>,
    /// A spec to keep as it is, for saving a dashboard already composed.
    #[serde(default)]
    pub spec: Option<Value>,
    #[serde(default)]
    pub descriptions: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PanelRequest {
    #[serde(default)]
    pub id: Option<String>,
    pub title: String,
    pub sql: String,
    #[serde(default)]
    pub question: Option<String>,
}

/// Make a dashboard from a request: answer its questions, take its
/// statements, and compose them - or keep the spec it brought.
pub async fn make(engine: &Engine, name: &str, request: Request) -> Result<Dashboard, Error> {
    if request.questions.is_empty() && request.panels.is_empty() {
        return Err("give it something to show: `questions`, `panels`, or both".into());
    }
    if request.questions.len() + request.panels.len() > 16 {
        return Err("a dashboard shows at most sixteen panels".into());
    }
    let mut panels = Vec::new();
    for given in request.panels {
        let id = given.id.unwrap_or_else(|| format!("q{}", panels.len()));
        if !crate::engine::valid_name(&id) || panels.iter().any(|p: &Panel| p.id == id) {
            return Err(format!("panel id `{id}` is not a usable name, or is taken").into());
        }
        panels.push(Panel {
            id,
            title: given.title,
            sql: given.sql,
            question: given.question,
        });
    }
    let answered = futures::future::join_all(
        request
            .questions
            .iter()
            .enumerate()
            .map(|(i, q)| asked(engine, format!("q{}", panels.len() + i), q)),
    )
    .await;
    for panel in answered {
        panels.push(panel?);
    }
    let prompt = request.prompt.unwrap_or_else(|| {
        let asked: Vec<String> = panels
            .iter()
            .map(|p| p.question.clone().unwrap_or_else(|| p.title.clone()))
            .collect();
        asked.join("; ")
    });
    let title = request
        .title
        .unwrap_or_else(|| title_of(panels.first().map(|p| p.title.as_str()).unwrap_or(name)));

    if let Some(spec) = request.spec {
        let panels = keep_drawn(panels, &spec);
        return Ok(Dashboard {
            name: name.to_owned(),
            title,
            prompt,
            panels,
            spec,
            descriptions: request.descriptions,
            composed_by: "given".into(),
            columns: None,
            updated_ms: now_ms(),
        });
    }
    let (offered, errors) = offer(engine, panels).await;
    if offered.is_empty() {
        return Err(format!("no panel could be run: {errors:?}").into());
    }
    let composed = compose(&prompt, &offered, None).await?;
    let panels = keep_drawn(
        offered.into_iter().map(|(p, _)| p).collect(),
        &composed.spec,
    );
    Ok(Dashboard {
        name: name.to_owned(),
        title,
        prompt,
        panels,
        spec: composed.spec,
        descriptions: composed.descriptions,
        composed_by: composed.by.into(),
        columns: None,
        updated_ms: now_ms(),
    })
}

/// Change a dashboard as `message` asks: show something it does not show
/// yet, or rearrange, restyle or remove what it does.
pub async fn chat(
    engine: &Engine,
    mut dashboard: Dashboard,
    message: &str,
) -> Result<(Dashboard, String), Error> {
    let message = message.trim();
    if message.is_empty() {
        return Err("say what to change".into());
    }
    // Without a judge, a message is read as asking for more to be shown,
    // which is the one thing that cannot be done by hand in a renderer.
    let wants_data = needs_new_data(&dashboard, message).await.unwrap_or(true);
    let mut panels = dashboard.panels.clone();
    let mut added = None;
    if wants_data {
        let id = (0..)
            .map(|n| format!("q{n}"))
            .find(|id| !panels.iter().any(|p| &p.id == id))
            .expect("an unused id");
        let panel = asked(engine, id, message).await?;
        added = Some(panel.title.clone());
        panels.push(panel);
    }
    let (offered, errors) = offer(engine, panels).await;
    if let Some(error) = errors.values().next() {
        return Err(format!("a panel could not be run: {error}").into());
    }
    let composed = compose(
        message,
        &offered,
        Some((&dashboard.spec, &dashboard.descriptions)),
    )
    .await?;
    dashboard.panels = keep_drawn(
        offered.into_iter().map(|(p, _)| p).collect(),
        &composed.spec,
    );
    dashboard.spec = composed.spec;
    dashboard.descriptions = composed.descriptions;
    dashboard.composed_by = composed.by.into();
    dashboard.updated_ms = now_ms();
    let did = match added {
        Some(title) => format!("added {title}"),
        None => "rearranged what it shows".into(),
    };
    Ok((dashboard, did))
}

/// Whether a message asks for something the dashboard does not show yet, as
/// Jev reads it. `None` when there is no judge or it failed.
async fn needs_new_data(dashboard: &Dashboard, message: &str) -> Option<bool> {
    let client = jev()?;
    let shown: Vec<&str> = dashboard
        .descriptions
        .values()
        .map(String::as_str)
        .collect();
    let state = format!(
        "A dashboard shows:\n- {}\n\nSomeone asks of it: {message}",
        shown.join("\n- ")
    );
    let questions = BTreeMap::from([(
        "needs".to_owned(),
        walleye_typesafe::Question::Choice {
            instructions: "What does the request need?".into(),
            criteria: BTreeMap::from([
                (
                    "new_data".to_owned(),
                    "Data the dashboard does not show yet: a new measure, breakdown, table or \
                     question"
                        .to_owned(),
                ),
                (
                    "arrange".to_owned(),
                    "Only a change to what it already shows: move, remove, resize, or draw a \
                     panel differently"
                        .to_owned(),
                ),
            ]),
        },
    )]);
    match client.ask(&state, &questions).await {
        Ok(decision) => decision
            .answers
            .get("needs")
            .and_then(|a| a.label())
            .map(|label| label == "new_data"),
        Err(error) => {
            eprintln!("walleye.see chat judge=unavailable error={error}");
            None
        }
    }
}

// --- a table's own dashboard ----------------------------------------------------

fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The panels that describe a table, from its columns and what they hold:
/// how many rows, how they arrive over time, how they break down by each
/// label with few values, how each measure moves, and the latest rows.
async fn table_panels(
    engine: &Engine,
    table: &str,
    schema: &arrow_schema::Schema,
) -> Result<Vec<Panel>, Error> {
    use arrow_schema::DataType;
    let fields: Vec<&arrow_schema::Field> = schema
        .fields()
        .iter()
        .map(|f| f.as_ref())
        .filter(|f| !crate::engine::hidden(f.name()) && !f.name().starts_with("walleye_"))
        .collect();
    let t = quote(table);
    let time = fields
        .iter()
        .find(|f| {
            matches!(
                f.data_type(),
                DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64
            )
        })
        .map(|f| f.name().clone());
    let labels: Vec<&str> = fields
        .iter()
        .filter(|f| {
            matches!(
                f.data_type(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Boolean
            )
        })
        .map(|f| f.name().as_str())
        .collect();
    let measures: Vec<&str> = fields
        .iter()
        .filter(|f| f.data_type().is_numeric())
        .map(|f| f.name().as_str())
        .filter(|name| !is_identifier(name))
        .collect();

    // One pass for what the rules need: the row count, the time span, and
    // how many values each label takes.
    let mut stats = vec!["count(*) AS walleye_rows".to_owned()];
    for (i, label) in labels.iter().enumerate() {
        stats.push(format!("approx_distinct({}) AS walleye_d{i}", quote(label)));
    }
    if let Some(time) = &time {
        stats.push(format!("min({}) AS walleye_from", quote(time)));
        stats.push(format!("max({}) AS walleye_to", quote(time)));
    }
    let stats: Value = serde_json::from_slice::<Vec<Value>>(
        &engine
            .query(&format!("SELECT {} FROM {t}", stats.join(", ")))
            .await?,
    )?
    .into_iter()
    .next()
    .unwrap_or_default();
    let rows = stats["walleye_rows"].as_u64().unwrap_or(0);

    let mut panels = vec![Panel {
        id: "rows".into(),
        title: format!("Rows in {table}"),
        sql: format!("SELECT count(*) AS rows FROM {t}"),
        question: None,
    }];
    let bucket = time.as_ref().map(|_| {
        let from = stats["walleye_from"].as_str().and_then(parse_time);
        let to = stats["walleye_to"].as_str().and_then(parse_time);
        match (from, to) {
            (Some(from), Some(to)) if (to - from).num_hours() <= 72 => "hour",
            (Some(from), Some(to)) if (to - from).num_days() > 365 => "month",
            (Some(from), Some(to)) if (to - from).num_days() > 90 => "week",
            _ => "day",
        }
    });
    if let (Some(time), Some(bucket)) = (&time, bucket) {
        panels.push(Panel {
            id: "over_time".into(),
            title: format!("Rows per {bucket}"),
            sql: format!(
                "SELECT date_trunc('{bucket}', {c}) AS {bucket}, count(*) AS rows FROM {t} \
                 WHERE {c} IS NOT NULL GROUP BY 1 ORDER BY 1",
                c = quote(time)
            ),
            question: None,
        });
        for (i, measure) in measures.iter().take(2).enumerate() {
            panels.push(Panel {
                id: format!("measure_{i}"),
                title: format!("Average {measure} per {bucket}"),
                sql: format!(
                    "SELECT date_trunc('{bucket}', {c}) AS {bucket}, avg({m}) AS {a} FROM {t} \
                     WHERE {c} IS NOT NULL GROUP BY 1 ORDER BY 1",
                    c = quote(time),
                    m = quote(measure),
                    a = quote(&format!("average_{measure}")),
                ),
                question: None,
            });
        }
    } else {
        for (i, measure) in measures.iter().take(2).enumerate() {
            panels.push(Panel {
                id: format!("measure_{i}"),
                title: measure.to_string(),
                sql: format!(
                    "SELECT min({m}) AS minimum, avg({m}) AS average, max({m}) AS maximum \
                     FROM {t}",
                    m = quote(measure)
                ),
                question: None,
            });
        }
    }
    for (i, label) in labels.iter().enumerate() {
        let distinct = stats[format!("walleye_d{i}")].as_u64().unwrap_or(0);
        // A label with one value says nothing, and one with nearly a value
        // per row is a name, not a grouping.
        if !(2..=50).contains(&distinct) || distinct * 2 > rows.max(1) {
            continue;
        }
        if panels.len() >= TABLE_PANELS - 1 {
            break;
        }
        panels.push(Panel {
            id: format!("by_{i}"),
            title: format!("Rows by {label}"),
            sql: format!(
                "SELECT {l} AS {l}, count(*) AS rows FROM {t} WHERE {l} IS NOT NULL \
                 GROUP BY 1 ORDER BY 2 DESC LIMIT 10",
                l = quote(label)
            ),
            question: None,
        });
    }
    let shown: Vec<String> = fields
        .iter()
        .filter(|f| {
            !matches!(
                f.data_type(),
                DataType::FixedSizeList(_, _) | DataType::List(_)
            )
        })
        .take(TABLE_COLUMNS)
        .map(|f| quote(f.name()))
        .collect();
    if !shown.is_empty() {
        panels.push(Panel {
            id: "latest".into(),
            title: format!("Latest rows in {table}"),
            sql: match &time {
                Some(time) => format!(
                    "SELECT {} FROM {t} ORDER BY {} DESC NULLS LAST LIMIT 20",
                    shown.join(", "),
                    quote(time)
                ),
                None => format!("SELECT {} FROM {t} LIMIT 20", shown.join(", ")),
            },
            question: None,
        });
    }
    Ok(panels)
}

fn parse_time(text: &str) -> Option<chrono::NaiveDateTime> {
    chrono::DateTime::parse_from_rfc3339(text)
        .map(|t| t.naive_utc())
        .ok()
        .or_else(|| chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f").ok())
        .or_else(|| chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f").ok())
}

/// A number that names a row rather than measures it.
fn is_identifier(name: &str) -> bool {
    name.eq_ignore_ascii_case("id")
        || name.to_ascii_lowercase().ends_with("_id")
        || (name.ends_with("Id") && name.len() > 2)
}

/// A table's own dashboard: the one saved, while the table's columns are
/// the ones it was made from, or a new one.
pub async fn for_table(engine: &Engine, table: &str, fresh: bool) -> Result<Dashboard, Error> {
    let schema = engine
        .schemas()
        .await?
        .into_iter()
        .find(|(name, _)| name == table)
        .map(|(_, schema)| schema)
        .ok_or_else(|| format!("no table named {table}"))?;
    let columns: Vec<String> = schema
        .fields()
        .iter()
        .filter(|f| !crate::engine::hidden(f.name()))
        .map(|f| format!("{}:{}", f.name(), f.data_type()))
        .collect();
    let name = format!("table.{table}");
    if !fresh
        && let Some(saved) = load(engine, &name).await?
        && saved.columns.as_ref() == Some(&columns)
    {
        return Ok(saved);
    }
    let panels = table_panels(engine, table, &schema).await?;
    let prompt = format!(
        "An overview of the table {table}: how much it holds, how that changes over time, how \
         its rows break down, and what the latest rows look like."
    );
    let (offered, errors) = offer(engine, panels).await;
    if offered.is_empty() {
        return Err(format!("nothing about {table} could be queried: {errors:?}").into());
    }
    let composed = compose(&prompt, &offered, None).await?;
    let dashboard = Dashboard {
        name: name.clone(),
        title: table.to_owned(),
        prompt,
        panels: keep_drawn(
            offered.into_iter().map(|(p, _)| p).collect(),
            &composed.spec,
        ),
        spec: composed.spec,
        descriptions: composed.descriptions,
        composed_by: composed.by.into(),
        columns: Some(columns),
        updated_ms: now_ms(),
    };
    save(engine, &dashboard).await?;
    Ok(dashboard)
}

/// A one-off view of one question or statement, not saved.
pub async fn look(
    engine: &Engine,
    question: Option<&str>,
    sql: Option<&str>,
    title: Option<String>,
) -> Result<Dashboard, Error> {
    let panel = match (question, sql) {
        (Some(question), None) => asked(engine, "q0".into(), question).await?,
        (None, Some(sql)) => Panel {
            id: "q0".into(),
            title: title.clone().unwrap_or_else(|| "Result".into()),
            sql: sql.to_owned(),
            question: None,
        },
        (Some(_), Some(_)) => return Err("give a question or a statement, not both".into()),
        (None, None) => return Err("give a question in `question` or a statement in `sql`".into()),
    };
    let prompt = panel
        .question
        .clone()
        .unwrap_or_else(|| panel.title.clone());
    let (offered, errors) = offer(engine, vec![panel]).await;
    if let Some(error) = errors.values().next() {
        return Err(error.clone().into());
    }
    let composed = compose(&prompt, &offered, None).await?;
    let panels = keep_drawn(
        offered.into_iter().map(|(p, _)| p).collect(),
        &composed.spec,
    );
    Ok(Dashboard {
        name: String::new(),
        title: title.unwrap_or_else(|| panels.first().map(|p| p.title.clone()).unwrap_or_default()),
        prompt,
        panels,
        spec: composed.spec,
        descriptions: composed.descriptions,
        composed_by: composed.by.into(),
        columns: None,
        updated_ms: now_ms(),
    })
}

// --- keeping them ---------------------------------------------------------------

/// Whether a dashboard can be saved under `name`: a name, or `table.<name>`
/// for a table's own.
pub fn valid(name: &str) -> bool {
    crate::engine::valid_name(name)
        || name
            .strip_prefix("table.")
            .is_some_and(crate::engine::valid_name)
}

fn path(engine: &Engine, name: &str) -> object_store::path::Path {
    let (_, prefix) = engine.dashboard_store();
    prefix.clone().join(format!("{name}.json"))
}

pub async fn load(engine: &Engine, name: &str) -> Result<Option<Dashboard>, Error> {
    let (store, _) = engine.dashboard_store();
    match store.inner.get(&path(engine, name)).await {
        Ok(object) => Ok(Some(serde_json::from_slice(&object.bytes().await?)?)),
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

pub async fn save(engine: &Engine, dashboard: &Dashboard) -> Result<(), Error> {
    let (store, _) = engine.dashboard_store();
    store
        .inner
        .put(
            &path(engine, &dashboard.name),
            serde_json::to_vec_pretty(dashboard)?.into(),
        )
        .await?;
    Ok(())
}

pub async fn remove(engine: &Engine, name: &str) -> Result<bool, Error> {
    if load(engine, name).await?.is_none() {
        return Ok(false);
    }
    let (store, _) = engine.dashboard_store();
    store.inner.delete(&path(engine, name)).await?;
    Ok(true)
}

/// Every saved dashboard, newest first: name, title, panels and when.
pub async fn list(engine: &Engine) -> Result<Vec<Value>, Error> {
    use futures::TryStreamExt;
    let (store, prefix) = engine.dashboard_store();
    let listed: Vec<_> = store.inner.list(Some(prefix)).try_collect().await?;
    let mut out = Vec::new();
    for object in listed {
        let bytes = store.inner.get(&object.location).await?.bytes().await?;
        if let Ok(d) = serde_json::from_slice::<Dashboard>(&bytes) {
            out.push((
                d.updated_ms,
                json!({
                    "name": d.name,
                    "title": d.title,
                    "panels": d.panels.len(),
                    "composed_by": d.composed_by,
                    "updated_ms": d.updated_ms,
                }),
            ));
        }
    }
    out.sort_by_key(|entry| std::cmp::Reverse(entry.0));
    Ok(out.into_iter().map(|(_, v)| v).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(json: &str) -> Frame {
        Frame::parse(json.as_bytes()).unwrap()
    }
    fn panel() -> Panel {
        Panel {
            id: "q0".into(),
            title: "Signups".into(),
            sql: "SELECT 1".into(),
            question: None,
        }
    }
    fn kinds(candidates: &[Value]) -> Vec<&str> {
        candidates
            .iter()
            .map(|c| c["element"]["type"].as_str().unwrap())
            .collect()
    }

    /// The bundle embedded is the one its sources build: an edit to the
    /// composer without `npm run build` fails here, not in production.
    #[test]
    fn the_composer_bundle_is_built_from_its_sources() {
        use sha2::{Digest, Sha256};
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("composer");
        let mut files = vec!["package-lock.json".to_owned()];
        let mut sources: Vec<String> = std::fs::read_dir(root.join("src"))
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .filter(|n| n.ends_with(".js"))
            .map(|n| format!("src/{n}"))
            .collect();
        sources.sort();
        files.extend(sources);
        let mut hash = Sha256::new();
        for file in files {
            hash.update(file.as_bytes());
            hash.update(b"\0");
            hash.update(std::fs::read(root.join(&file)).unwrap());
            hash.update(b"\0");
        }
        let expected = format!("// source-sha256: {}", hex::encode(hash.finalize()));
        assert_eq!(
            COMPOSER.lines().next().unwrap(),
            expected,
            "the composer changed; run `npm run build` in crates/walleye-node/composer"
        );
    }

    #[test]
    fn columns_keep_the_order_the_statement_gave() {
        let f = frame(r#"[{"zeta":1,"alpha":"a"},{"zeta":2,"mid":3,"alpha":"b"}]"#);
        assert_eq!(f.columns, ["zeta", "alpha", "mid"]);
    }

    #[test]
    fn a_measure_over_time_is_a_line_first() {
        let f = frame(
            r#"[{"day":"2026-09-01T00:00:00Z","signups":3},{"day":"2026-09-02T00:00:00Z","signups":5}]"#,
        );
        let c = candidates(&panel(), &f);
        assert_eq!(kinds(&c), ["LineChart", "BarChart", "Table"]);
        assert_eq!(c[0]["element"]["props"]["x"], "day");
        assert_eq!(c[0]["element"]["props"]["y"], json!(["signups"]));
        assert_eq!(c[0]["element"]["props"]["data"], json!({"$state": "/q0"}));
        assert!(
            c[0]["description"]
                .as_str()
                .unwrap()
                .contains("from 2026-09-01")
        );
        assert!(c.iter().all(|c| c["resource"] == "q0"));
    }

    #[test]
    fn one_number_is_a_metric() {
        let c = candidates(&panel(), &frame(r#"[{"total":340}]"#));
        assert_eq!(kinds(&c), ["Metric", "Table"]);
        assert_eq!(c[0]["element"]["props"]["value"], "total");
    }

    #[test]
    fn a_few_categories_can_be_bars_or_a_pie() {
        let c = candidates(
            &panel(),
            &frame(r#"[{"plan":"free","n":300},{"plan":"pro","n":40},{"plan":"team","n":9}]"#),
        );
        assert_eq!(kinds(&c), ["BarChart", "PieChart", "Table"]);
        assert_eq!(c[1]["element"]["props"]["label"], "plan");
    }

    #[test]
    fn records_are_a_table() {
        let c = candidates(
            &panel(),
            &frame(
                r#"[{"id":1,"at":"2026-09-01T00:00:00Z","plan":"free","paid":0},
                    {"id":2,"at":"2026-09-02T00:00:00Z","plan":"pro","paid":12}]"#,
            ),
        );
        assert_eq!(kinds(&c), ["Table"]);
        let c = candidates(
            &panel(),
            &frame(
                r#"[{"day":"2026-09-01","plan":"free","n":3},{"day":"2026-09-01","plan":"pro","n":1}]"#,
            ),
        );
        assert_eq!(
            kinds(&c),
            ["Table"],
            "a series per plan is not drawn as one line"
        );
    }

    #[test]
    fn two_measures_alone_are_a_scatter() {
        let c = candidates(
            &panel(),
            &frame(r#"[{"price":1,"sold":9},{"price":2,"sold":4}]"#),
        );
        assert_eq!(kinds(&c), ["ScatterChart", "Table"]);
    }

    #[test]
    fn rules_put_several_panels_in_a_grid_and_add_to_an_edit() {
        let a = Panel {
            id: "a".into(),
            ..panel()
        };
        let b = Panel {
            id: "b".into(),
            ..panel()
        };
        let one = candidates(&a, &frame(r#"[{"total":1}]"#));
        let two = candidates(&b, &frame(r#"[{"total":2}]"#));
        let first = by_rules(&[(a.clone(), one.clone()), (b.clone(), two.clone())], None);
        assert_eq!(first.spec["root"], "grid");
        assert_eq!(
            first.spec["elements"]["grid"]["children"]
                .as_array()
                .unwrap()
                .len(),
            2
        );

        let c = Panel {
            id: "c".into(),
            ..panel()
        };
        let three = candidates(&c, &frame(r#"[{"total":3}]"#));
        let edited = by_rules(
            &[(a, one), (b, two), (c, three)],
            Some((&first.spec, &first.descriptions)),
        );
        let children = edited.spec["elements"]["grid"]["children"]
            .as_array()
            .unwrap();
        assert_eq!(
            children.len(),
            3,
            "the two kept, one added: {}",
            edited.spec
        );
    }

    #[test]
    fn a_limit_is_added_only_where_there_is_none() {
        assert!(ends_in_limit("SELECT a FROM t ORDER BY a LIMIT 10"));
        assert!(ends_in_limit("select a from t limit 10 offset 5"));
        assert!(!ends_in_limit(
            "SELECT a FROM (SELECT a FROM t LIMIT 5) ORDER BY a"
        ));
        assert!(!ends_in_limit("SELECT limit FROM t"));
    }

    #[test]
    fn identifiers_are_named_as_such() {
        for id in ["id", "ID", "user_id", "userId"] {
            assert!(is_identifier(id), "{id}");
        }
        for measure in ["paid", "valid", "amount", "idle"] {
            assert!(!is_identifier(measure), "{measure}");
        }
    }

    #[test]
    fn titles_come_from_the_question() {
        assert_eq!(
            title_of("how many signups per day?"),
            "How many signups per day"
        );
    }
}
