//! Take any JSON, and turn it into tables.
//!
//! `POST /v1/ingest/{source}` accepts records of any shape. What happens to
//! them is decided in three layers, and the split is the point:
//!
//! **Bronze is what arrived.** Every record is written first, exactly as its
//! bytes came, to `ingest_bronze`, keyed on a hash of the source and the
//! record. Nothing is parsed into a number on the way; nothing is dropped.
//! Every later decision can be wrong, because every later decision can be
//! remade from here.
//!
//! **Rules route; a judge only decides once.** A source is mapped to a table
//! the first time it is seen, and from then on by that mapping alone. Jev is
//! asked when something genuinely needs judging - which table a new source
//! belongs to, what a column's values mean, whether a missing field is optional
//! or a broken record - and never per record. What it decides is written into
//! the table's rule, with the confidence it was decided at, and applied by code
//! thereafter.
//!
//! **Silver is rebuilt, never altered.** The LSM scanner refuses a user column
//! that an older generation does not have, so a live table cannot grow a
//! column. Instead, every non-key column is stored nullable from the start -
//! making a field optional is then a change to the rule and nothing else, and
//! ingestion never stops for it - and a change that does need a new column, or
//! a wider type, rebuilds the table from bronze. Until then a value that does
//! not fit is kept in `walleye_extra`, the record's catch-all, rather than
//! lost.
//!
//! Some limits are code rather than judgement, because a judge that is wrong
//! once must not be able to cause damage a rebuild cannot undo: a key column
//! is never optional, and a table never grows past [`MAX_COLUMNS`].
pub mod embed;
pub mod literal;

use crate::engine::{Column, Engine, StreamDefinition};
use arrow_array::{
    ArrayRef, RecordBatch,
    builder::{
        BooleanBuilder, Decimal128Builder, Float64Builder, Int64Builder, StringBuilder,
        TimestampMicrosecondBuilder,
    },
};
use arrow_schema::{Field, Schema};
use literal::{Kind, Literal, Value, candidates, convert, lossless};
use object_store::{ObjectStore as _, ObjectStoreExt as _};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{Arc, OnceLock},
};
use tokio::sync::Mutex;
use walleye_typesafe::{Answer, Question};

type Error = Box<dyn std::error::Error + Send + Sync>;

/// Every record as it arrived.
pub const BRONZE: &str = "ingest_bronze";
/// Records that were refused, and why. They stay in bronze as well.
pub const QUARANTINE: &str = "ingest_quarantine";
/// The bronze id of the record a silver row came from.
pub const ID: &str = "walleye_id";
/// Whatever a record carried that the table has no column for yet, or that
/// did not fit the column it was meant for, as a JSON object.
pub const EXTRA: &str = "walleye_extra";
/// The most columns a table may have, its own two included. One wrong call
/// about a record keyed by ids would otherwise mean thousands.
pub const MAX_COLUMNS: usize = 200;
/// How many new fields in one batch before it is worth asking whether the
/// keys are names at all, or data - user ids, dates - wearing a field's shape.
const MANY_FIELDS: usize = 24;
/// Below this, a judgement is not acted on and the safe choice is made
/// instead. Safe means recoverable: a lossless type, an optional field, a
/// value kept in the catch-all.
const CONFIDENT: f64 = 0.6;
/// How many records a judge is shown when deciding about a table.
const SHOWN: usize = 5;
/// The option meaning "none of these columns" when asking about a rename. No
/// column can be called this: a leading underscore is not a column name.
const NEW_FIELD: &str = "__new_field__";

/// Locks for routing a source and for changing a table, so two requests do
/// not decide the same thing twice. In-process only: two nodes ingesting the
/// same source at once would each decide, and the later rule would win.
#[derive(Default)]
pub struct State {
    sources: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    tables: Mutex<HashMap<String, Arc<Mutex<()>>>>,
}

impl State {
    async fn source(&self, name: &str) -> Arc<Mutex<()>> {
        self.sources
            .lock()
            .await
            .entry(name.to_owned())
            .or_default()
            .clone()
    }
    async fn table(&self, name: &str) -> Arc<Mutex<()>> {
        self.tables
            .lock()
            .await
            .entry(name.to_owned())
            .or_default()
            .clone()
    }
}

/// How one column is read out of a record.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RuleColumn {
    pub name: String,
    /// A [`Kind`] name.
    pub kind: String,
    /// Whether a record without it is broken rather than merely short. Only
    /// ever decides quarantine; the column is stored nullable regardless,
    /// unless it is the key.
    pub required: bool,
    /// Other field names that mean this column, from a rename.
    #[serde(default)]
    pub aliases: Vec<String>,
}

/// Everything decided about one table, and how.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableRule {
    pub table: String,
    /// Moves every time the table has to be rebuilt.
    pub version: u64,
    pub columns: Vec<RuleColumn>,
    /// The field that identifies a record, if one does. Without one, the
    /// bronze id is the key, so an identical record sent twice is one row.
    pub primary_key: Option<String>,
    /// The records' top-level keys are data rather than names, so no field
    /// ever becomes a column and everything is kept in the catch-all.
    pub keys_are_data: bool,
    pub sources: Vec<String>,
    /// What was decided, by whom, and how sure it was. Newest last.
    #[serde(default)]
    pub history: Vec<Change>,
    /// Text columns that carry a vector, for searching by meaning.
    #[serde(default)]
    pub embeddings: Vec<Embedding>,
    /// Text columns already considered for embedding, embedded or not. Each
    /// is decided once; after that the rule decides.
    #[serde(default)]
    pub embed_considered: Vec<String>,
}

/// One text column's vector.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Embedding {
    /// The text column the vector is made from.
    pub source: String,
    /// The vector column, beside it in the same table.
    pub column: String,
    /// The model that made it. Vectors from two models cannot be compared,
    /// so a table's vectors all come from this one.
    pub model: String,
    pub dimensions: usize,
}

/// One decision, as the rule remembers it and a response reports it.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Change {
    pub what: String,
    pub chose: String,
    /// `jev` when it was judged, `rule` when code decided because nothing
    /// needed judging or the judge was not sure enough.
    pub by: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub confidence: Option<f64>,
}

#[derive(Serialize, Deserialize)]
struct Route {
    source: String,
    table: String,
}

/// What one request did.
#[derive(Debug, Serialize)]
pub struct Report {
    pub source: String,
    pub table: String,
    /// The table did not exist before this request.
    pub created: bool,
    /// Rows now in silver from this request.
    pub accepted: usize,
    /// Records refused; each is in `ingest_quarantine` with its reason.
    pub quarantined: usize,
    /// Accepted rows that kept something in the catch-all.
    pub rescued: usize,
    /// The table was rebuilt from bronze to take a new column or type.
    pub rebuilt: bool,
    /// Rows given vectors by this request, including ones filled in that an
    /// earlier request had to write without.
    pub embedded: usize,
    /// Rows this request wrote whose text is still waiting for a vector,
    /// because the embedding model did not answer. The next request for the
    /// table fills them in.
    pub unembedded: usize,
    pub changes: Vec<Change>,
}

/// One record as it arrived: its bronze id, its bytes, and its fields when it
/// was an object at all.
struct Arrived {
    id: String,
    raw: String,
    fields: Option<BTreeMap<String, Literal>>,
}

impl Arrived {
    fn new(source: &str, raw: &RawValue) -> Self {
        use sha2::{Digest, Sha256};
        let text = raw.get().to_owned();
        let mut hash = Sha256::new();
        hash.update(source.as_bytes());
        hash.update([0]);
        hash.update(text.as_bytes());
        let id = hex::encode(&hash.finalize()[..16]);
        let fields = serde_json::from_str::<BTreeMap<String, Box<RawValue>>>(&text)
            .ok()
            .map(|map| {
                map.into_iter()
                    .map(|(key, value)| (key, Literal::read(&value)))
                    .collect()
            });
        Self {
            id,
            raw: text,
            fields,
        }
    }
}

/// A source name is the path segment a client chose, so it is checked before
/// it becomes part of an object key or a SQL literal.
fn valid_source(name: &str) -> bool {
    (1..=128).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
}

/// Whether a field name can be a column as it is. One that cannot is kept in
/// the catch-all rather than renamed, because a renamed column is one nobody
/// asked for and a query cannot find by the name they sent.
fn column_name(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= 64
        && !key.starts_with('_')
        && key != ID
        && key != EXTRA
        && !key.chars().any(char::is_control)
}

/// A table name made from something a client sent.
fn table_name(from: &str) -> Option<String> {
    let mut name = String::new();
    for c in from.chars() {
        if c.is_ascii_alphanumeric() {
            name.push(c.to_ascii_lowercase());
        } else if !name.ends_with('_') && !name.is_empty() {
            name.push('_');
        }
    }
    let name = name.trim_matches('_').to_owned();
    if name.is_empty() {
        return None;
    }
    let name = if name.as_bytes()[0].is_ascii_digit() {
        format!("t_{name}")
    } else {
        name
    };
    Some(name.chars().take(48).collect())
}

fn judge() -> Option<&'static walleye_typesafe::Client> {
    static CLIENT: OnceLock<Option<walleye_typesafe::Client>> = OnceLock::new();
    CLIENT
        .get_or_init(walleye_typesafe::Client::from_env)
        .as_ref()
}

/// Ask a question set once. A judge that is not configured, or that fails,
/// answers nothing, and every decision falls to its safe default: that is
/// what a low-confidence answer does too, so there is one path, not two.
async fn ask(state: &str, questions: BTreeMap<String, Question>) -> HashMap<String, Answer> {
    if questions.is_empty() {
        return HashMap::new();
    }
    let Some(client) = judge() else {
        return HashMap::new();
    };
    match client.ask(state, &questions).await {
        Ok(decision) => decision.answers.clone(),
        Err(error) => {
            eprintln!("walleye.ingest judge outcome=unavailable error={error}");
            HashMap::new()
        }
    }
}

/// A choice the judge was sure enough about.
fn chosen(answers: &HashMap<String, Answer>, key: &str) -> Option<(String, f64)> {
    let answer = answers.get(key)?;
    let label = answer.label()?;
    (answer.confidence() >= CONFIDENT).then(|| (label, answer.confidence()))
}

/// A yes or no the judge was sure enough about.
fn decided(answers: &HashMap<String, Answer>, key: &str) -> Option<(bool, f64)> {
    let answer = answers.get(key)?;
    let value = answer.value()?;
    (answer.confidence() >= CONFIDENT).then(|| (value >= 0.5, answer.confidence()))
}

fn by_judge(what: String, chose: String, confidence: f64) -> Change {
    Change {
        what,
        chose,
        by: "jev".into(),
        confidence: Some(confidence),
    }
}

fn by_rule(what: String, chose: String) -> Change {
    Change {
        what,
        chose,
        by: "rule".into(),
        confidence: None,
    }
}

/// What a judge is shown about a batch: a few records whole, then every field
/// with how often it appears and what it held.
fn describe(source: &str, records: &[&BTreeMap<String, Literal>]) -> String {
    let mut text = format!(
        "Records arriving on the stream \"{source}\". {} of them, the first few in full:\n",
        records.len()
    );
    for record in records.iter().take(SHOWN) {
        let shown: Vec<String> = record
            .iter()
            .map(|(key, value)| {
                format!(
                    "{}: {}",
                    serde_json::to_string(key).unwrap_or_default(),
                    value.sample()
                )
            })
            .collect();
        text.push_str(&format!("{{{}}}\n", shown.join(", ")));
    }
    text.push_str("\nEvery field, how many records have it, and example values:\n");
    let mut fields: BTreeMap<&str, (usize, Vec<String>)> = BTreeMap::new();
    for record in records {
        for (key, value) in record.iter() {
            let entry = fields.entry(key.as_str()).or_default();
            entry.0 += 1;
            if entry.1.len() < 4 && !value.is_null() {
                let sample = value.sample();
                if !entry.1.contains(&sample) {
                    entry.1.push(sample);
                }
            }
        }
    }
    for (key, (count, examples)) in fields {
        text.push_str(&format!(
            "- {key}: in {count} of {}; {}\n",
            records.len(),
            examples.join(", ")
        ));
    }
    text
}

/// A choice between column kinds, offered only the kinds every value admits.
fn type_question(field: &str, options: &[Kind]) -> Question {
    Question::choice(
        format!(
            "What does the field \"{field}\" hold? Choose how it should be stored. Every option \
             offered is one all of its values fit."
        ),
        options
            .iter()
            .map(|kind| (kind.name().to_owned(), kind.meaning().to_owned())),
    )
}

// --- persistence -----------------------------------------------------------

async fn load<T: serde::de::DeserializeOwned>(
    engine: &Engine,
    folder: &str,
    name: &str,
) -> Result<Option<T>, Error> {
    let (store, prefix, _) = engine.ingest_store();
    let path = prefix.clone().join(folder).join(format!("{name}.json"));
    match store.inner.get(&path).await {
        Ok(object) => Ok(Some(serde_json::from_slice(&object.bytes().await?)?)),
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

async fn save<T: Serialize>(
    engine: &Engine,
    folder: &str,
    name: &str,
    value: &T,
) -> Result<(), Error> {
    let (store, prefix, _) = engine.ingest_store();
    let path = prefix.clone().join(folder).join(format!("{name}.json"));
    store
        .inner
        .put(&path, serde_json::to_vec_pretty(value)?.into())
        .await?;
    Ok(())
}

async fn every_rule(engine: &Engine) -> Result<Vec<TableRule>, Error> {
    use futures::TryStreamExt;
    let (store, prefix, _) = engine.ingest_store();
    let folder = prefix.clone().join("tables");
    let listed: Vec<_> = store.inner.list(Some(&folder)).try_collect().await?;
    let mut rules = Vec::new();
    for object in listed {
        let bytes = store.inner.get(&object.location).await?.bytes().await?;
        if let Ok(rule) = serde_json::from_slice::<TableRule>(&bytes) {
            rules.push(rule);
        }
    }
    Ok(rules)
}

/// The two tables every ingest path writes, created once.
async fn ensure_system_tables(engine: &Engine) -> Result<(), Error> {
    let column = |name: &str, kind: &str, nullable: bool| Column {
        name: name.into(),
        kind: kind.into(),
        nullable,
    };
    for (name, columns) in [
        (
            BRONZE,
            vec![
                column(ID, "string", false),
                column("source", "string", false),
                column("received_at", "int64", false),
                column("record", "string", false),
            ],
        ),
        (
            QUARANTINE,
            vec![
                column(ID, "string", false),
                column("source", "string", false),
                column("table_name", "string", true),
                column("reason", "string", false),
                column("record", "string", false),
            ],
        ),
    ] {
        engine
            .define_with(
                StreamDefinition {
                    name: name.into(),
                    columns,
                    primary_key: vec![ID.into()],
                    schema: None,
                    vector_indexes: Vec::new(),
                    text_indexes: Vec::new(),
                },
                true,
            )
            .await?;
    }
    Ok(())
}

fn definition(rule: &TableRule) -> StreamDefinition {
    let key = rule.primary_key.clone().unwrap_or_else(|| ID.to_owned());
    let mut columns = vec![Column {
        name: ID.into(),
        kind: "string".into(),
        nullable: false,
    }];
    for column in &rule.columns {
        let kind = Kind::from_name(&column.kind).unwrap_or(Kind::String);
        columns.push(Column {
            name: column.name.clone(),
            kind: kind.storage().into(),
            nullable: column.name != key,
        });
    }
    for embedding in &rule.embeddings {
        columns.push(Column {
            name: embedding.column.clone(),
            kind: format!("vector:{}", embedding.dimensions),
            nullable: true,
        });
    }
    columns.push(Column {
        name: EXTRA.into(),
        kind: "json".into(),
        nullable: true,
    });
    StreamDefinition {
        name: rule.table.clone(),
        columns,
        primary_key: vec![key],
        schema: None,
        vector_indexes: rule
            .embeddings
            .iter()
            .map(|e| walleye_lance::VectorIndexSpec {
                name: format!("{}_idx", e.column),
                column: e.column.clone(),
                metric: "cosine".into(),
            })
            .collect(),
        text_indexes: Vec::new(),
    }
}

// --- the request ------------------------------------------------------------

/// Ingest a batch of records of any shape from `source`.
pub async fn ingest(
    engine: &Engine,
    source: &str,
    raws: Vec<Box<RawValue>>,
) -> Result<Report, Error> {
    if !valid_source(source) {
        return Err("a source is 1 to 128 letters, digits, '.', '_' or '-'".into());
    }
    if raws.is_empty() {
        return Err("send at least one record".into());
    }
    ensure_system_tables(engine).await?;
    let arrived: Vec<Arrived> = raws.iter().map(|raw| Arrived::new(source, raw)).collect();

    // Bronze first, before anything is decided, so nothing that follows can
    // lose a record.
    let received = crate::engine::now_micros();
    engine
        .ingest(
            BRONZE,
            arrived
                .iter()
                .map(|record| {
                    serde_json::json!({
                        ID: record.id,
                        "source": source,
                        "received_at": received,
                        "record": record.raw,
                    })
                })
                .collect(),
        )
        .await?;

    let objects: Vec<&Arrived> = arrived.iter().filter(|a| a.fields.is_some()).collect();
    let mut quarantine: Vec<(&Arrived, String)> = arrived
        .iter()
        .filter(|a| a.fields.is_none())
        .map(|a| (a, "not a JSON object".to_owned()))
        .collect();

    // Route: a source already mapped goes where it was mapped, decided by
    // nothing but that mapping.
    let source_lock = engine.ingest_store().2.source(source).await;
    let routing = source_lock.lock().await;
    let (mut rule, created, mut changes) = match load::<Route>(engine, "routes", source).await? {
        Some(route) => {
            let rule = load::<TableRule>(engine, "tables", &route.table)
                .await?
                .ok_or_else(|| {
                    format!(
                        "source {source} routes to {} which has no rule",
                        route.table
                    )
                })?;
            (rule, false, Vec::new())
        }
        None => {
            let records: Vec<&BTreeMap<String, Literal>> =
                objects.iter().filter_map(|a| a.fields.as_ref()).collect();
            let (rule, created, changes) = route(engine, source, &records).await?;
            save(engine, "tables", &rule.table, &rule).await?;
            save(
                engine,
                "routes",
                source,
                &Route {
                    source: source.to_owned(),
                    table: rule.table.clone(),
                },
            )
            .await?;
            (rule, created, changes)
        }
    };
    drop(routing);

    let table_lock = engine.ingest_store().2.table(&rule.table).await;
    let _changing = table_lock.lock().await;
    // Another request may have changed the rule while this one waited.
    if !created && let Some(latest) = load::<TableRule>(engine, "tables", &rule.table).await? {
        rule = latest;
    }
    if !rule.sources.iter().any(|s| s == source) {
        rule.sources.push(source.to_owned());
    }

    let records: Vec<&BTreeMap<String, Literal>> =
        objects.iter().filter_map(|a| a.fields.as_ref()).collect();
    let evolved = evolve(engine, &mut rule, source, &records).await;
    changes.extend(evolved.changes.iter().cloned());
    // Which text is worth searching by meaning, once the columns are settled.
    let embedder = embed::Embedder::from_env();
    let (planned, embedding_added) =
        plan_embeddings(embedder.as_ref(), &mut rule, source, &records).await;
    changes.extend(planned);
    for (index, reason) in &evolved.quarantine {
        quarantine.push((objects[*index], reason.clone()));
    }

    if !quarantine.is_empty() {
        engine
            .ingest(
                QUARANTINE,
                quarantine
                    .iter()
                    .map(|(record, reason)| {
                        serde_json::json!({
                            ID: record.id,
                            "source": source,
                            "table_name": rule.table,
                            "reason": reason,
                            "record": record.raw,
                        })
                    })
                    .collect(),
            )
            .await?;
    }

    rule.history.extend(changes.iter().cloned());
    let keep = rule.history.len().saturating_sub(200);
    rule.history.drain(..keep);

    let refused: BTreeSet<&str> = quarantine.iter().map(|(r, _)| r.id.as_str()).collect();
    let accepted: Vec<&Arrived> = objects
        .iter()
        .copied()
        .filter(|a| !refused.contains(a.id.as_str()))
        .collect();

    let rebuilt = created || evolved.reshaped || embedding_added;
    if rebuilt {
        rule.version += 1;
    }
    save(engine, "tables", &rule.table, &rule).await?;

    let (rescued, embedded, unembedded);
    if rebuilt {
        // The table is made again from bronze, which already holds this
        // request's records, so they arrive with everything else.
        rescued = accepted.iter().filter(|a| row(&rule, a).1).count();
        let built = rebuild(engine, &rule, embedder.as_ref()).await?;
        embedded = built.embedded;
        unembedded = built.awaiting;
    } else {
        // Vectors an earlier request could not get, first, so a newer
        // version of a key in this batch is the one that lands last.
        let repaired = repair(engine, &rule, embedder.as_ref()).await;
        let mut known = HashMap::new();
        if let Some(why) = vectors_for(
            embedder.as_ref(),
            &rule,
            texts(&rule, &accepted),
            &mut known,
        )
        .await
        {
            changes.push(by_rule("embedding".into(), format!("deferred: {why}")));
        }
        let built = batch(&rule, &accepted, &known)?;
        rescued = built.rescued;
        embedded = built.embedded + repaired;
        unembedded = built.awaiting;
        if built.batch.num_rows() > 0 {
            engine.append(&rule.table, vec![built.batch]).await?;
        }
    }

    Ok(Report {
        source: source.to_owned(),
        table: rule.table.clone(),
        created,
        accepted: accepted.len(),
        quarantined: quarantine.len(),
        rescued,
        rebuilt,
        embedded,
        unembedded,
        changes,
    })
}

/// Decide where a source nobody has seen goes: into a table that already
/// holds records like these, or into a new one, and if new, what it is
/// called and what its columns are.
async fn route(
    engine: &Engine,
    source: &str,
    records: &[&BTreeMap<String, Literal>],
) -> Result<(TableRule, bool, Vec<Change>), Error> {
    let state = describe(source, records);
    let fields: BTreeSet<&str> = records
        .iter()
        .flat_map(|r| r.keys().map(String::as_str))
        .collect();

    // A table is only offered if most of these fields are already its
    // columns, and every value that has a column fits it. Matching on shape
    // is what the judge is for; ruling out what cannot fit is not.
    let mut existing = Vec::new();
    for rule in every_rule(engine).await? {
        if rule.keys_are_data || fields.is_empty() {
            continue;
        }
        let known = |key: &str| {
            rule.columns
                .iter()
                .find(|c| c.name == key || c.aliases.iter().any(|a| a == key))
        };
        let overlap = fields.iter().filter(|key| known(key).is_some()).count();
        let fits = records.iter().all(|record| {
            record.iter().all(|(key, value)| match known(key) {
                Some(column) => {
                    value.is_null()
                        || Kind::from_name(&column.kind)
                            .is_some_and(|kind| convert(kind, value).is_some())
                }
                None => true,
            })
        });
        if fits && overlap * 10 >= fields.len() * 6 {
            existing.push(rule);
        }
    }

    let mut questions = BTreeMap::new();
    if !existing.is_empty() {
        let mut options: Vec<(String, String)> = existing
            .iter()
            .map(|rule| {
                let columns: Vec<&str> = rule.columns.iter().map(|c| c.name.as_str()).collect();
                (
                    rule.table.clone(),
                    format!(
                        "the same kind of record as the table {}, whose columns are {}",
                        rule.table,
                        columns.join(", ")
                    ),
                )
            })
            .collect();
        options.push((
            "new".into(),
            "a different kind of record that belongs in a table of its own".into(),
        ));
        questions.insert(
            "route".to_owned(),
            Question::choice(
                "Are these records the same kind of thing an existing table already holds?",
                options,
            ),
        );
    }
    let answers = ask(&state, questions).await;
    if let Some((table, confidence)) = chosen(&answers, "route")
        && table != "new"
        && let Some(mut rule) = existing.into_iter().find(|r| r.table == table)
    {
        rule.sources.push(source.to_owned());
        let change = by_judge(format!("route {source}"), table, confidence);
        return Ok((rule, false, vec![change]));
    }

    let (rule, changes) = design(engine, source, records, &state).await?;
    Ok((rule, true, changes))
}

/// Everything a new table needs decided: its name, its key, and each column's
/// kind and whether it is required. Asked as one question set, which the
/// judge answers in parallel for about the price of one question.
async fn design(
    engine: &Engine,
    source: &str,
    records: &[&BTreeMap<String, Literal>],
    state: &str,
) -> Result<(TableRule, Vec<Change>), Error> {
    let mut order: Vec<&str> = Vec::new();
    let mut values: HashMap<&str, Vec<&Literal>> = HashMap::new();
    for record in records {
        for (key, value) in record.iter() {
            if !values.contains_key(key.as_str()) {
                order.push(key);
            }
            values.entry(key).or_default().push(value);
        }
    }
    let present = |key: &str| records.iter().filter(|r| r.contains_key(key)).count();

    let taken: BTreeSet<String> = engine.table_names().await?.into_iter().collect();
    let mut raw_names: Vec<String> = table_name(source).into_iter().collect();
    // A field that says what kind of record this is makes a better name than
    // the stream it came down, when every record agrees on it.
    for key in [
        "type",
        "kind",
        "event",
        "event_type",
        "object",
        "entity",
        "resource",
    ] {
        if let Some(seen) = values.get(key) {
            let texts: BTreeSet<&str> = seen
                .iter()
                .filter_map(|v| match v {
                    Literal::Text(text) => Some(text.as_str()),
                    _ => None,
                })
                .collect();
            if texts.len() == 1
                && seen.len() == records.len()
                && let Some(name) = texts.into_iter().next().and_then(table_name)
            {
                raw_names.push(name);
            }
        }
    }
    if raw_names.is_empty() {
        raw_names.push("ingested".to_owned());
    }
    let mut names: Vec<String> = Vec::new();
    for base in raw_names {
        let mut name = base.clone();
        let mut n = 2;
        while taken.contains(&name) || name == BRONZE || name == QUARANTINE {
            name = format!("{base}_{n}");
            n += 1;
        }
        if !names.contains(&name) {
            names.push(name);
        }
    }

    let mut questions = BTreeMap::new();
    let mut changes = Vec::new();

    // Keys that are data rather than names make a column per id. Ask once
    // there are enough of them to matter; past the cap it is not a question.
    let named: Vec<&str> = order.iter().copied().filter(|k| column_name(k)).collect();
    if named.len() >= MANY_FIELDS {
        questions.insert(
            "keys_are_data".to_owned(),
            Question::noul(
                "The top-level keys of these records are themselves data - ids, dates, user \
                 names - rather than the names of fields every record shares.",
            ),
        );
    }
    if names.len() > 1 {
        questions.insert(
            "name".to_owned(),
            Question::choice(
                "What should the table holding these records be called?",
                names
                    .iter()
                    .map(|n| (n.clone(), format!("a table named {n}"))),
            ),
        );
    }

    let mut kinds: BTreeMap<&str, Vec<Kind>> = BTreeMap::new();
    for (index, key) in named.iter().enumerate() {
        let options = candidates(values[key].iter().copied());
        if options.len() > 1 {
            questions.insert(format!("type_{index}"), type_question(key, &options));
        }
        if present(key) == records.len() {
            questions.insert(
                format!("required_{index}"),
                Question::noul(format!(
                    "Every one of these records needs \"{key}\" to mean anything: a record \
                     without it would be broken, not just shorter."
                )),
            );
        }
        kinds.insert(key, options);
    }

    // A key candidate is in every record, never null, of a kind the LSM can
    // hash, and different in every record shown. Five records at least: two
    // records are unique by almost anything.
    let keys: Vec<&str> = named
        .iter()
        .copied()
        .filter(|key| {
            let seen = &values[key];
            records.len() >= 5
                && present(key) == records.len()
                && seen.iter().all(|v| !v.is_null())
                && kinds[key].iter().any(|k| k.keyable())
                && seen.iter().map(|v| v.json()).collect::<BTreeSet<_>>().len() == seen.len()
        })
        .collect();
    if !keys.is_empty() {
        let mut options: Vec<(String, String)> = keys
            .iter()
            .map(|k| ((*k).to_owned(), format!("\"{k}\" identifies the record")))
            .collect();
        options.push((
            "none".into(),
            "no field identifies a record; each one is an event of its own".into(),
        ));
        questions.insert(
            "key".to_owned(),
            Question::choice(
                "Which field identifies a record, so that a later record with the same value \
                 replaces it?",
                options,
            ),
        );
    }

    let answers = ask(state, questions).await;

    let table = match chosen(&answers, "name") {
        Some((name, confidence)) if names.contains(&name) => {
            changes.push(by_judge("table name".into(), name.clone(), confidence));
            name
        }
        _ => {
            changes.push(by_rule("table name".into(), names[0].clone()));
            names[0].clone()
        }
    };

    let keys_are_data = match decided(&answers, "keys_are_data") {
        Some((yes, confidence)) => {
            changes.push(by_judge(
                "keys are data".into(),
                yes.to_string(),
                confidence,
            ));
            yes
        }
        // Undecided, the cap decides: it is the one thing a judge cannot
        // overrule.
        None => {
            let over = named.len() + 2 > MAX_COLUMNS;
            if over {
                changes.push(by_rule("keys are data".into(), "true".into()));
            }
            over
        }
    };
    // Past the cap there is nothing to decide, whatever the judge said.
    let keys_are_data = keys_are_data || named.len() + 2 > MAX_COLUMNS;

    let mut columns = Vec::new();
    if !keys_are_data {
        for (index, key) in named.iter().enumerate() {
            if columns.len() + 2 >= MAX_COLUMNS {
                changes.push(by_rule(
                    format!("column {key}"),
                    "kept in the catch-all: the table is at its column limit".into(),
                ));
                continue;
            }
            let options = &kinds[key];
            let kind = if options.len() == 1 {
                options[0]
            } else {
                match chosen(&answers, &format!("type_{index}")).and_then(|(name, c)| {
                    Kind::from_name(&name)
                        .filter(|k| options.contains(k))
                        .map(|k| (k, c))
                }) {
                    Some((kind, confidence)) => {
                        changes.push(by_judge(
                            format!("type of {key}"),
                            kind.name().into(),
                            confidence,
                        ));
                        kind
                    }
                    None => {
                        let kind = lossless(options);
                        changes.push(by_rule(format!("type of {key}"), kind.name().into()));
                        kind
                    }
                }
            };
            // Lean optional. Wrongly optional costs nothing; wrongly required
            // turns a short record into a refused one.
            let required = match decided(&answers, &format!("required_{index}")) {
                Some((yes, confidence)) => {
                    changes.push(by_judge(
                        format!("{key} required"),
                        yes.to_string(),
                        confidence,
                    ));
                    yes
                }
                None => false,
            };
            columns.push(RuleColumn {
                name: (*key).to_owned(),
                kind: kind.name().into(),
                required,
                aliases: Vec::new(),
            });
        }
    }

    let primary_key = match chosen(&answers, "key") {
        Some((key, confidence)) if key != "none" && keys.contains(&key.as_str()) => {
            changes.push(by_judge("primary key".into(), key.clone(), confidence));
            Some(key)
        }
        _ => {
            changes.push(by_rule("primary key".into(), ID.into()));
            None
        }
    };
    // Whatever the judge said, a key is required and of a kind that hashes.
    if let Some(key) = &primary_key
        && let Some(column) = columns.iter_mut().find(|c| &c.name == key)
    {
        column.required = true;
        let options = &kinds[key.as_str()];
        if !Kind::from_name(&column.kind).is_some_and(Kind::keyable) {
            let kind = options
                .iter()
                .copied()
                .find(|k| k.keyable())
                .unwrap_or(Kind::String);
            column.kind = kind.name().into();
        }
    }

    Ok((
        TableRule {
            table,
            version: 0,
            columns,
            primary_key,
            keys_are_data,
            sources: vec![source.to_owned()],
            history: Vec::new(),
            embeddings: Vec::new(),
            embed_considered: Vec::new(),
        },
        changes,
    ))
}

struct Evolved {
    changes: Vec<Change>,
    /// The table's columns or kinds changed, so it must be rebuilt.
    reshaped: bool,
    /// Records to refuse, by position in the batch, and why.
    quarantine: BTreeMap<usize, String>,
}

/// What an existing table has to change to take this batch, decided once for
/// the batch rather than once per record.
///
/// In two rounds, because the second depends on the first. Whether a new
/// field is an old one renamed decides whether a record has its key at all:
/// a key that was renamed is still a key, and checking for it before asking
/// would refuse every record from the rename on. So renames - and whether new
/// keys are names at all - are settled first, and everything that depends on
/// which records are whole is asked after.
async fn evolve(
    engine: &Engine,
    rule: &mut TableRule,
    source: &str,
    records: &[&BTreeMap<String, Literal>],
) -> Evolved {
    let mut out = Evolved {
        changes: Vec::new(),
        reshaped: false,
        quarantine: BTreeMap::new(),
    };
    if records.is_empty() {
        return out;
    }
    fn find(rule: &TableRule, key: &str) -> Option<usize> {
        rule.columns
            .iter()
            .position(|c| c.name == key || c.aliases.iter().any(|a| a == key))
    }
    fn value_of(record: &BTreeMap<String, Literal>, column: &RuleColumn) -> Option<Literal> {
        std::iter::once(&column.name)
            .chain(column.aliases.iter())
            .find_map(|name| record.get(name).cloned())
    }
    fn unknown_fields<'a>(
        rule: &TableRule,
        records: impl Iterator<Item = &'a BTreeMap<String, Literal>>,
    ) -> (Vec<&'a str>, HashMap<&'a str, Vec<&'a Literal>>) {
        let mut order = Vec::new();
        let mut values: HashMap<&str, Vec<&Literal>> = HashMap::new();
        if rule.keys_are_data {
            return (order, values);
        }
        for record in records {
            for (key, value) in record.iter() {
                if find(rule, key).is_none()
                    && column_name(key)
                    && !rule.embeddings.iter().any(|e| &e.column == key)
                {
                    if !values.contains_key(key.as_str()) {
                        order.push(key.as_str());
                    }
                    values.entry(key.as_str()).or_default().push(value);
                }
            }
        }
        (order, values)
    }

    // --- round one: what the new fields are ------------------------------
    let (unknown, values) = unknown_fields(rule, records.iter().copied());
    let mut first = BTreeMap::new();
    if unknown.len() >= MANY_FIELDS {
        first.insert(
            "keys_are_data".to_owned(),
            Question::noul(format!(
                "These {} new top-level keys are data - ids, dates, names - rather than the \
                 names of new fields: {}",
                unknown.len(),
                unknown
                    .iter()
                    .take(12)
                    .copied()
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
        );
    }
    // A new field, and a column no record carrying it has, of a kind its
    // values fit, may be that column renamed. The key is a candidate like any
    // other column; a renamed key is still the key.
    //
    // One choice per new field, among every column it could be, rather than a
    // yes or no per pair: the judge compares candidates against each other,
    // which is the question that actually needs answering, and a field can
    // only become one column.
    let mut renames: Vec<(String, &str, Vec<usize>)> = Vec::new();
    let mut spelled: Vec<(String, usize)> = Vec::new();
    let mut evidence: HashMap<usize, Vec<String>> = HashMap::new();
    for key in &unknown {
        let carrying: Vec<&&BTreeMap<String, Literal>> =
            records.iter().filter(|r| r.contains_key(*key)).collect();
        let mut could_be = Vec::new();
        for (at, column) in rule.columns.iter().enumerate() {
            let absent = carrying.iter().all(|r| value_of(r, column).is_none());
            let kind = Kind::from_name(&column.kind).unwrap_or(Kind::String);
            let is_key = Some(&column.name) == rule.primary_key.as_ref();
            let fits = values[key].iter().all(|v| {
                if is_key {
                    convert(kind, v).is_some()
                } else {
                    v.is_null() || convert(kind, v).is_some()
                }
            });
            if !(absent && fits) {
                continue;
            }
            if spelled_alike(key, &column.name) {
                // Not a judgement: the same name in another case convention.
                spelled.push(((*key).to_owned(), at));
                could_be.clear();
                break;
            }
            could_be.push(at);
        }
        if could_be.is_empty() {
            continue;
        }
        // The judge cannot see the table, so show it what each candidate
        // column already holds, beside what the new field holds.
        let mut options: Vec<(String, String)> = Vec::new();
        for at in &could_be {
            let column = &rule.columns[*at];
            if !evidence.contains_key(at) {
                evidence.insert(*at, held(engine, &rule.table, &column.name).await);
            }
            let held = &evidence[at];
            options.push((
                column.name.clone(),
                format!(
                    "the existing column \"{}\" under a new name; it holds {}{}",
                    column.name,
                    Kind::from_name(&column.kind).map_or("text", |k| k.meaning()),
                    if held.is_empty() {
                        String::new()
                    } else {
                        format!(", with values like {}", held.join(", "))
                    }
                ),
            ));
        }
        options.push((
            NEW_FIELD.to_owned(),
            "a new field that is none of these columns".to_owned(),
        ));
        let now: Vec<String> = values[key]
            .iter()
            .filter(|v| !v.is_null())
            .take(4)
            .map(|v| v.sample())
            .collect();
        let question = format!("rename_{}", renames.len());
        first.insert(
            question.clone(),
            Question::choice(
                format!(
                    "These records have a field \"{key}\" the table has never seen, holding \
                     values like {}, and they do not have the columns offered here. Is \
                     \"{key}\" one of those columns under a new name - the same thing, now \
                     spelled differently - or a new field?",
                    now.join(", ")
                ),
                options,
            ),
        );
        renames.push((question, key, could_be));
    }
    let state = describe(source, records);
    let answers = ask(&state, first).await;

    if let Some((yes, confidence)) = decided(&answers, "keys_are_data") {
        out.changes.push(by_judge(
            "keys are data".into(),
            yes.to_string(),
            confidence,
        ));
        if yes {
            rule.keys_are_data = true;
        }
    }
    let mut renamed: BTreeSet<&str> = BTreeSet::new();
    if !rule.keys_are_data {
        for (key, at) in &spelled {
            let column = &mut rule.columns[*at];
            if !column.aliases.iter().any(|a| a == key) {
                column.aliases.push(key.clone());
                out.changes.push(by_rule(
                    format!("{key} is {} renamed", column.name),
                    "true".into(),
                ));
                out.reshaped = true;
            }
            if let Some(unknown_key) = unknown.iter().find(|u| **u == key.as_str()) {
                renamed.insert(unknown_key);
            }
        }
        // Merged on the lean, not on conviction. Both mistakes are recoverable
        // from bronze, and the one this avoids is the worse of the two: a
        // missed rename splits a field across two half-empty columns, and a
        // missed key rename refuses every record from the rename on. Where two
        // new fields lean towards the same column, the surer one has it.
        let mut leans: Vec<(f64, &str, usize)> = renames
            .iter()
            .filter_map(|(question, key, could_be)| {
                let answer = answers.get(question)?;
                let label = answer.label()?;
                let at = could_be
                    .iter()
                    .copied()
                    .find(|at| rule.columns[*at].name == label)?;
                Some((answer.confidence(), *key, at))
            })
            .collect();
        leans.sort_by(|a, b| b.0.total_cmp(&a.0));
        let mut taken: BTreeSet<usize> = BTreeSet::new();
        for (confidence, key, at) in leans {
            if renamed.contains(key) || !taken.insert(at) {
                continue;
            }
            let column = &mut rule.columns[at];
            column.aliases.push(key.to_owned());
            out.changes.push(by_judge(
                format!("{key} is {} renamed", column.name),
                "true".into(),
                confidence,
            ));
            renamed.insert(key);
            out.reshaped = true;
        }
        for (question, key, _) in &renames {
            if !renamed.contains(key)
                && let Some(answer) = answers.get(question)
            {
                out.changes.push(by_judge(
                    format!("{key} is a new field"),
                    "true".into(),
                    answer.confidence(),
                ));
            }
        }
    }

    // --- the key, with renames applied -----------------------------------
    // A key is required, and a record without one cannot be placed: refuse
    // it, whatever anybody would say about it.
    if let Some(key) = &rule.primary_key
        && let Some(column) = rule.columns.iter().find(|c| &c.name == key)
    {
        let kind = Kind::from_name(&column.kind).unwrap_or(Kind::String);
        for (index, record) in records.iter().enumerate() {
            match value_of(record, column) {
                Some(value) if convert(kind, &value).is_some() => {}
                Some(Literal::Null) | None => {
                    out.quarantine
                        .insert(index, format!("missing its key \"{key}\""));
                }
                Some(_) => {
                    out.quarantine
                        .insert(index, format!("its key \"{key}\" is not a {}", kind.name()));
                }
            }
        }
    }
    let live: Vec<(usize, &BTreeMap<String, Literal>)> = records
        .iter()
        .copied()
        .enumerate()
        .filter(|(index, _)| !out.quarantine.contains_key(index))
        .collect();
    if live.is_empty() {
        return out;
    }

    // --- round two: columns, gaps and misfits, over the whole records -----
    let (unknown, values) = unknown_fields(rule, live.iter().map(|(_, r)| *r));
    let mut second = BTreeMap::new();

    let mut new_kinds: BTreeMap<&str, Vec<Kind>> = BTreeMap::new();
    for (index, key) in unknown.iter().enumerate() {
        let options = candidates(values[key].iter().copied());
        if options.len() > 1 {
            second.insert(format!("type_{index}"), type_question(key, &options));
        }
        new_kinds.insert(key, options);
    }

    let mut missing: Vec<(usize, Vec<usize>)> = Vec::new();
    for (at, column) in rule.columns.iter().enumerate() {
        if !column.required || Some(&column.name) == rule.primary_key.as_ref() {
            continue;
        }
        let without: Vec<usize> = live
            .iter()
            .filter(|(_, r)| matches!(value_of(r, column), None | Some(Literal::Null)))
            .map(|(index, _)| *index)
            .collect();
        if !without.is_empty() {
            second.insert(
                format!("optional_{at}"),
                Question::noul(format!(
                    "\"{}\" was required, and {} of {} new records arrived without it. It is \
                     an optional field that these records simply do not have, rather than a \
                     sign the records are broken.",
                    column.name,
                    without.len(),
                    live.len()
                )),
            );
            missing.push((at, without));
        }
    }

    let mut misfits: Vec<(usize, Vec<Kind>)> = Vec::new();
    for (at, column) in rule.columns.iter().enumerate() {
        let kind = Kind::from_name(&column.kind).unwrap_or(Kind::String);
        let seen: Vec<Literal> = live
            .iter()
            .filter_map(|(_, r)| value_of(r, column))
            .collect();
        if seen
            .iter()
            .all(|v| v.is_null() || convert(kind, v).is_some())
        {
            continue;
        }
        let wider: Vec<Kind> = candidates(seen.iter())
            .into_iter()
            .filter(|k| *k != kind)
            .filter(|k| Some(&column.name) != rule.primary_key.as_ref() || k.keyable())
            .collect();
        let mut options: Vec<(String, String)> = wider
            .iter()
            .map(|k| {
                (
                    k.name().to_owned(),
                    format!("widen the column to {}: {}", k.name(), k.meaning()),
                )
            })
            .collect();
        options.push((
            "keep".into(),
            format!(
                "keep it {}; the values that do not fit are mistakes, kept aside with the record",
                kind.name()
            ),
        ));
        let examples: Vec<String> = seen
            .iter()
            .filter(|v| !v.is_null() && convert(kind, v).is_none())
            .take(4)
            .map(Literal::sample)
            .collect();
        second.insert(
            format!("widen_{at}"),
            Question::choice(
                format!(
                    "The column \"{}\" holds {}, and new records sent values that are not: {}. \
                     What should happen?",
                    column.name,
                    kind.name(),
                    examples.join(", ")
                ),
                options,
            ),
        );
        misfits.push((at, wider));
    }

    let state = describe(source, &live.iter().map(|(_, r)| *r).collect::<Vec<_>>());
    let answers = ask(&state, second).await;

    for (index, key) in unknown.iter().enumerate() {
        if rule.columns.len() + 2 >= MAX_COLUMNS {
            out.changes.push(by_rule(
                format!("column {key}"),
                "kept in the catch-all: the table is at its column limit".into(),
            ));
            continue;
        }
        let options = &new_kinds[key];
        let kind = if options.len() == 1 {
            options[0]
        } else {
            match chosen(&answers, &format!("type_{index}")).and_then(|(name, c)| {
                Kind::from_name(&name)
                    .filter(|k| options.contains(k))
                    .map(|k| (k, c))
            }) {
                Some((kind, confidence)) => {
                    out.changes.push(by_judge(
                        format!("type of {key}"),
                        kind.name().into(),
                        confidence,
                    ));
                    kind
                }
                None => {
                    let kind = lossless(options);
                    out.changes
                        .push(by_rule(format!("type of {key}"), kind.name().into()));
                    kind
                }
            }
        };
        out.changes
            .push(by_rule(format!("new column {key}"), kind.name().into()));
        rule.columns.push(RuleColumn {
            name: (*key).to_owned(),
            kind: kind.name().into(),
            // A field that appears later was not in every record before it,
            // which is as optional as a field gets.
            required: false,
            aliases: Vec::new(),
        });
        out.reshaped = true;
    }

    for (at, without) in &missing {
        let name = rule.columns[*at].name.clone();
        match decided(&answers, &format!("optional_{at}")) {
            Some((false, confidence)) => {
                out.changes.push(by_judge(
                    format!("{name} missing"),
                    "refuse the records".into(),
                    confidence,
                ));
                for index in without {
                    out.quarantine
                        .insert(*index, format!("missing the required field \"{name}\""));
                }
            }
            // Optional, or unsure. Relaxing costs nothing - every column but
            // the key is stored nullable already - and refusing a record the
            // judge was not sure about hides data somebody sent.
            answer => {
                rule.columns[*at].required = false;
                match answer {
                    Some((_, confidence)) => out.changes.push(by_judge(
                        format!("{name} missing"),
                        "now optional".into(),
                        confidence,
                    )),
                    None => out
                        .changes
                        .push(by_rule(format!("{name} missing"), "now optional".into())),
                }
            }
        }
    }

    for (at, wider) in &misfits {
        let name = rule.columns[*at].name.clone();
        match chosen(&answers, &format!("widen_{at}")).and_then(|(label, c)| {
            Kind::from_name(&label)
                .filter(|k| wider.contains(k))
                .map(|k| (k, c))
        }) {
            Some((kind, confidence)) => {
                rule.columns[*at].kind = kind.name().into();
                out.changes.push(by_judge(
                    format!("widen {name}"),
                    kind.name().into(),
                    confidence,
                ));
                out.reshaped = true;
            }
            // Kept, or unsure: the values that do not fit stay in the
            // catch-all, where a later rebuild can still promote them.
            None => out.changes.push(by_rule(
                format!("widen {name}"),
                "kept; values that do not fit are in the catch-all".into(),
            )),
        }
    }
    out
}

/// Two field names that are the same name written in a different case
/// convention: `user_id`, `userId`, `UserID` and `user-id` all read "userid".
/// The most common rename there is, and one code can see without a judge.
fn spelled_alike(a: &str, b: &str) -> bool {
    let fold = |name: &str| -> String {
        name.chars()
            .filter(|c| c.is_alphanumeric())
            .flat_map(char::to_lowercase)
            .collect()
    };
    a != b && !fold(a).is_empty() && fold(a) == fold(b)
}

/// A few values a column already holds, for a judge asked whether a new field
/// is that column renamed. Nothing is lost if it cannot be read: the question
/// is asked with less evidence, and an unsure answer changes nothing.
async fn held(engine: &Engine, table: &str, column: &str) -> Vec<String> {
    let quoted = |name: &str| format!("\"{}\"", name.replace('"', "\"\""));
    let sql = format!(
        "SELECT DISTINCT CAST({c} AS VARCHAR) AS v FROM {t} WHERE {c} IS NOT NULL LIMIT 4",
        c = quoted(column),
        t = quoted(table)
    );
    let Ok(bytes) = engine.query(&sql).await else {
        return Vec::new();
    };
    serde_json::from_slice::<Vec<serde_json::Value>>(&bytes)
        .unwrap_or_default()
        .iter()
        .filter_map(|row| row.get("v")?.as_str().map(|v| format!("\"{v}\"")))
        .collect()
}

// --- silver -----------------------------------------------------------------

/// One record read through a rule: a converted value per column, and whatever
/// was left over as a JSON object. The flag says whether anything was.
fn row(rule: &TableRule, arrived: &Arrived) -> (Vec<Option<Value>>, bool, Option<String>) {
    let Some(fields) = &arrived.fields else {
        return (Vec::new(), false, None);
    };
    let mut used: BTreeSet<&str> = BTreeSet::new();
    // Raw text, not parsed values, so a number keeps its digits.
    let mut extra: BTreeMap<String, Box<RawValue>> = BTreeMap::new();
    let mut values = Vec::with_capacity(rule.columns.len());
    for column in &rule.columns {
        let kind = Kind::from_name(&column.kind).unwrap_or(Kind::String);
        let found = std::iter::once(&column.name)
            .chain(column.aliases.iter())
            .find_map(|name| fields.get_key_value(name));
        match found {
            Some((name, value)) => {
                used.insert(name.as_str());
                let converted = convert(kind, value);
                if converted.is_none() && !value.is_null() {
                    extra.insert(name.clone(), raw_json(value));
                }
                values.push(converted);
            }
            None => values.push(None),
        }
    }
    for (key, value) in fields {
        if !used.contains(key.as_str()) {
            extra.insert(key.clone(), raw_json(value));
        }
    }
    let rescued = !extra.is_empty();
    let extra = rescued.then(|| {
        // Rebuilt by hand so a number keeps the digits it arrived with.
        let parts: Vec<String> = extra
            .iter()
            .map(|(k, v)| {
                format!(
                    "{}:{}",
                    serde_json::to_string(k).unwrap_or_default(),
                    v.get()
                )
            })
            .collect();
        format!("{{{}}}", parts.join(","))
    });
    (values, rescued, extra)
}

fn raw_json(value: &Literal) -> Box<RawValue> {
    RawValue::from_string(value.json())
        .unwrap_or_else(|_| RawValue::from_string("null".into()).expect("null is JSON"))
}

/// Records as one Arrow batch in the table's column order.
struct Built {
    batch: RecordBatch,
    /// Rows that kept something in the catch-all.
    rescued: usize,
    /// Rows with at least one vector.
    embedded: usize,
    /// Rows with text that has no vector yet.
    awaiting: usize,
}

/// The text a record gives each embedded column, where it gives one worth
/// embedding. Blank text is not: a model refuses it, and it means nothing.
fn embeddable(rule: &TableRule, values: &[Option<Value>]) -> Vec<Option<String>> {
    rule.embeddings
        .iter()
        .map(|embedding| {
            let at = rule
                .columns
                .iter()
                .position(|c| c.name == embedding.source)?;
            match values.get(at)? {
                Some(Value::Text(text)) if !text.trim().is_empty() => Some(text.clone()),
                _ => None,
            }
        })
        .collect()
}

/// Every distinct text these records would embed.
fn texts(rule: &TableRule, records: &[&Arrived]) -> BTreeSet<String> {
    if rule.embeddings.is_empty() {
        return BTreeSet::new();
    }
    records
        .iter()
        .flat_map(|record| embeddable(rule, &row(rule, record).0))
        .flatten()
        .collect()
}

fn batch(
    rule: &TableRule,
    records: &[&Arrived],
    vectors: &HashMap<String, Vec<f32>>,
) -> Result<Built, Error> {
    use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
    let definition = definition(rule);
    let mut ids = StringBuilder::new();
    let mut extras = StringBuilder::new();
    let mut builders: Vec<Box<dyn Fill>> = rule
        .columns
        .iter()
        .map(|c| fill_for(Kind::from_name(&c.kind).unwrap_or(Kind::String)))
        .collect();
    let mut lists: Vec<FixedSizeListBuilder<Float32Builder>> = rule
        .embeddings
        .iter()
        .map(|e| {
            FixedSizeListBuilder::new(Float32Builder::new(), e.dimensions as i32).with_field(
                Arc::new(Field::new("item", arrow_schema::DataType::Float32, true)),
            )
        })
        .collect();
    let (mut rescued, mut embedded, mut awaiting) = (0, 0, 0);
    for record in records {
        let (values, kept, extra) = row(rule, record);
        let wanted = embeddable(rule, &values);
        ids.append_value(&record.id);
        for (builder, value) in builders.iter_mut().zip(values) {
            builder.push(value);
        }
        let (mut has, mut lacks) = (false, false);
        for ((list, embedding), text) in lists.iter_mut().zip(&rule.embeddings).zip(wanted) {
            let vector = text
                .as_ref()
                .and_then(|t| vectors.get(t))
                .filter(|v| v.len() == embedding.dimensions);
            match vector {
                Some(vector) => {
                    list.values().append_slice(vector);
                    list.append(true);
                    has = true;
                }
                None => {
                    for _ in 0..embedding.dimensions {
                        list.values().append_null();
                    }
                    list.append(false);
                    lacks |= text.is_some();
                }
            }
        }
        extras.append_option(extra);
        rescued += kept as usize;
        embedded += has as usize;
        awaiting += lacks as usize;
    }
    let mut columns: Vec<ArrayRef> = vec![Arc::new(ids.finish())];
    columns.extend(builders.iter_mut().map(|b| b.finish()));
    columns.extend(lists.iter_mut().map(|l| Arc::new(l.finish()) as ArrayRef));
    columns.push(Arc::new(extras.finish()));
    let fields: Vec<Field> = definition
        .columns
        .iter()
        .map(|c| {
            Field::new(
                &c.name,
                crate::engine::storage_type(&c.kind).expect("an ingest column kind is storable"),
                c.nullable,
            )
        })
        .collect();
    Ok(Built {
        batch: RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?,
        rescued,
        embedded,
        awaiting,
    })
}

// --- embedding ----------------------------------------------------------------

/// Embed whatever of these texts is not already known. Returns why it could
/// not, when it could not: the rows are then written without their vectors,
/// and filled in by a later request, because ingestion does not wait on a
/// model that is slow or down.
async fn vectors_for(
    embedder: Option<&embed::Embedder>,
    rule: &TableRule,
    texts: BTreeSet<String>,
    known: &mut HashMap<String, Vec<f32>>,
) -> Option<String> {
    let embedder = embedder?;
    if rule.embeddings.is_empty() {
        return None;
    }
    if let Some(other) = rule.embeddings.iter().find(|e| e.model != embedder.model) {
        return Some(format!(
            "this table's vectors come from {}, and {} is configured; vectors from two \
             models cannot be compared",
            other.model, embedder.model
        ));
    }
    let missing: Vec<String> = texts
        .into_iter()
        .filter(|t| !known.contains_key(t))
        .collect();
    if missing.is_empty() {
        return None;
    }
    match embedder.embed(&missing).await {
        Ok(vectors) => {
            known.extend(missing.into_iter().zip(vectors));
            None
        }
        Err(error) => {
            eprintln!(
                "walleye.ingest embed table={} outcome=deferred error={error}",
                rule.table
            );
            Some(error.to_string())
        }
    }
}

/// Decide which text columns carry a vector. Each column is decided once and
/// remembered; after that the rule decides.
///
/// Code rules out what is not prose - codes, names, labels, short values -
/// and embeds what plainly is, paragraphs of words, without asking anybody.
/// What is left, a sentence that might be a note or might be a title, is the
/// judge's. Returns whether a vector column was added, which is a rebuild.
async fn plan_embeddings(
    embedder: Option<&embed::Embedder>,
    rule: &mut TableRule,
    source: &str,
    records: &[&BTreeMap<String, Literal>],
) -> (Vec<Change>, bool) {
    let mut changes = Vec::new();
    let Some(embedder) = embedder else {
        return (changes, false);
    };
    if rule.embeddings.iter().any(|e| e.model != embedder.model) || rule.keys_are_data {
        return (changes, false);
    }
    let mut clearly: Vec<String> = Vec::new();
    let mut maybe: Vec<String> = Vec::new();
    let mut considered: Vec<String> = Vec::new();
    for column in &rule.columns {
        if column.kind != Kind::String.name()
            || Some(&column.name) == rule.primary_key.as_ref()
            || rule.embed_considered.contains(&column.name)
        {
            continue;
        }
        let seen: Vec<&str> = records
            .iter()
            .filter_map(|record| {
                std::iter::once(&column.name)
                    .chain(column.aliases.iter())
                    .find_map(|name| match record.get(name) {
                        Some(Literal::Text(text)) => Some(text.as_str()),
                        _ => None,
                    })
            })
            .collect();
        // Nothing to judge by yet; decide when there is.
        if seen.is_empty() {
            continue;
        }
        match embed::prose(&seen) {
            embed::Prose::No => considered.push(column.name.clone()),
            embed::Prose::Clearly => clearly.push(column.name.clone()),
            embed::Prose::Maybe => maybe.push(column.name.clone()),
        }
    }

    let mut questions = BTreeMap::new();
    for (index, name) in maybe.iter().enumerate() {
        questions.insert(
            format!("embed_{index}"),
            Question::noul(format!(
                "The field \"{name}\" holds free text that someone would want to search by \
                 meaning - a description, a message, a review, a note - rather than a name, a \
                 title, a code or a label."
            )),
        );
    }
    let answers = if questions.is_empty() {
        HashMap::new()
    } else {
        ask(&describe(source, records), questions).await
    };

    let mut wanted: Vec<(String, Change)> = clearly
        .into_iter()
        .map(|name| {
            let change = by_rule(format!("embed {name}"), "true".into());
            (name, change)
        })
        .collect();
    for (index, name) in maybe.iter().enumerate() {
        let key = format!("embed_{index}");
        match decided(&answers, &key) {
            Some((true, confidence)) => {
                wanted.push((
                    name.clone(),
                    by_judge(format!("embed {name}"), "true".into(), confidence),
                ));
            }
            Some((false, confidence)) => {
                changes.push(by_judge(
                    format!("embed {name}"),
                    "false".into(),
                    confidence,
                ));
                considered.push(name.clone());
            }
            // Answered but unsure: not embedded, and not asked again. With no
            // answer at all the judge was not there, and it is asked next time.
            None if answers.contains_key(&key) => {
                changes.push(by_rule(format!("embed {name}"), "false".into()));
                considered.push(name.clone());
            }
            None => {}
        }
    }
    rule.embed_considered.extend(considered);
    if wanted.is_empty() {
        return (changes, false);
    }

    // The width of a vector column is fixed when the table is made, so learn
    // it now; a table already embedding with this model knows it.
    let dimensions = match rule.embeddings.first() {
        Some(existing) => existing.dimensions,
        None => match embedder.dimensions().await {
            Ok(width) => width,
            Err(error) => {
                // Not recorded as considered, so it is tried again next time.
                changes.push(by_rule("embedding".into(), format!("unavailable: {error}")));
                return (changes, false);
            }
        },
    };
    for (name, change) in wanted {
        let taken = |candidate: &str| {
            rule.columns.iter().any(|c| c.name == candidate)
                || rule.embeddings.iter().any(|e| e.column == candidate)
                || candidate == ID
                || candidate == EXTRA
        };
        let mut column = format!("{name}_embedding");
        let mut n = 2;
        while taken(&column) {
            column = format!("{name}_embedding_{n}");
            n += 1;
        }
        rule.embeddings.push(Embedding {
            source: name.clone(),
            column,
            model: embedder.model.clone(),
            dimensions,
        });
        rule.embed_considered.push(name);
        changes.push(change);
    }
    (changes, true)
}

fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Fill in vectors an earlier request had to write without, because the model
/// did not answer then. Bounded per request, so a long outage is caught up a
/// piece at a time rather than all at once on whoever writes first.
///
/// Rows are rewritten from their bronze records under the table's lock, so the
/// row replaced is the one that was read: no newer version of a key can have
/// arrived in between.
async fn repair(engine: &Engine, rule: &TableRule, embedder: Option<&embed::Embedder>) -> usize {
    if rule.embeddings.is_empty() || embedder.is_none() {
        return 0;
    }
    let gaps: Vec<String> = rule
        .embeddings
        .iter()
        .map(|e| {
            format!(
                "({v} IS NULL AND {t} IS NOT NULL AND trim({t}) <> '')",
                v = quote(&e.column),
                t = quote(&e.source)
            )
        })
        .collect();
    let sql = format!(
        "SELECT {ID} AS id FROM {} WHERE {} LIMIT 256",
        quote(&rule.table),
        gaps.join(" OR ")
    );
    let Ok(bytes) = engine.query(&sql).await else {
        return 0;
    };
    let ids: Vec<String> = serde_json::from_slice::<Vec<serde_json::Value>>(&bytes)
        .unwrap_or_default()
        .iter()
        .filter_map(|r| r.get("id")?.as_str().map(str::to_owned))
        .filter(|id| id.bytes().all(|b| b.is_ascii_hexdigit()))
        .collect();
    if ids.is_empty() {
        return 0;
    }
    let listed = ids
        .iter()
        .map(|id| format!("'{id}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("SELECT {ID} AS id, source, record FROM {BRONZE} WHERE {ID} IN ({listed})");
    let Ok(bytes) = engine.query(&sql).await else {
        return 0;
    };
    let arrived =
        arrivals(&serde_json::from_slice::<Vec<serde_json::Value>>(&bytes).unwrap_or_default());
    let refs: Vec<&Arrived> = arrived.iter().collect();
    let mut known = HashMap::new();
    if vectors_for(embedder, rule, texts(rule, &refs), &mut known)
        .await
        .is_some()
    {
        return 0;
    }
    let Ok(built) = batch(rule, &refs, &known) else {
        return 0;
    };
    if built.batch.num_rows() == 0 || engine.append(&rule.table, vec![built.batch]).await.is_err() {
        return 0;
    }
    built.embedded
}

/// Bronze rows as records, keeping the ids they were given when they arrived.
fn arrivals(rows: &[serde_json::Value]) -> Vec<Arrived> {
    rows.iter()
        .filter_map(|row| {
            let source = row.get("source")?.as_str()?;
            let record = row.get("record")?.as_str()?;
            let raw = RawValue::from_string(record.to_owned()).ok()?;
            let mut arrived = Arrived::new(source, &raw);
            arrived.id = row.get("id")?.as_str()?.to_owned();
            Some(arrived)
        })
        .filter(|a| a.fields.is_some())
        .collect()
}

/// A column builder that takes converted values.
trait Fill {
    fn push(&mut self, value: Option<Value>);
    fn finish(&mut self) -> ArrayRef;
}

fn fill_for(kind: Kind) -> Box<dyn Fill> {
    match kind {
        Kind::Boolean => Box::new(BooleanBuilder::new()),
        Kind::Int64 => Box::new(Int64Builder::new()),
        Kind::Float64 => Box::new(Float64Builder::new()),
        Kind::Decimal => Box::new(
            Decimal128Builder::new()
                .with_precision_and_scale(literal::DECIMAL_PRECISION, literal::DECIMAL_SCALE as i8)
                .expect("a valid decimal type"),
        ),
        Kind::TimestampIso
        | Kind::TimestampSeconds
        | Kind::TimestampMillis
        | Kind::TimestampMicros => {
            Box::new(TimestampMicrosecondBuilder::new().with_timezone("UTC"))
        }
        Kind::String | Kind::Json => Box::new(StringBuilder::new()),
    }
}

impl Fill for BooleanBuilder {
    fn push(&mut self, value: Option<Value>) {
        self.append_option(match value {
            Some(Value::Bool(v)) => Some(v),
            _ => None,
        });
    }
    fn finish(&mut self) -> ArrayRef {
        Arc::new(BooleanBuilder::finish(self))
    }
}
impl Fill for Int64Builder {
    fn push(&mut self, value: Option<Value>) {
        self.append_option(match value {
            Some(Value::Int(v)) => Some(v),
            _ => None,
        });
    }
    fn finish(&mut self) -> ArrayRef {
        Arc::new(Int64Builder::finish(self))
    }
}
impl Fill for Float64Builder {
    fn push(&mut self, value: Option<Value>) {
        self.append_option(match value {
            Some(Value::Float(v)) => Some(v),
            _ => None,
        });
    }
    fn finish(&mut self) -> ArrayRef {
        Arc::new(Float64Builder::finish(self))
    }
}
impl Fill for Decimal128Builder {
    fn push(&mut self, value: Option<Value>) {
        self.append_option(match value {
            Some(Value::Decimal(v)) => Some(v),
            _ => None,
        });
    }
    fn finish(&mut self) -> ArrayRef {
        Arc::new(Decimal128Builder::finish(self))
    }
}
impl Fill for TimestampMicrosecondBuilder {
    fn push(&mut self, value: Option<Value>) {
        self.append_option(match value {
            Some(Value::Micros(v)) => Some(v),
            _ => None,
        });
    }
    fn finish(&mut self) -> ArrayRef {
        Arc::new(TimestampMicrosecondBuilder::finish(self))
    }
}
impl Fill for StringBuilder {
    fn push(&mut self, value: Option<Value>) {
        self.append_option(match value {
            Some(Value::Text(v)) => Some(v),
            _ => None,
        });
    }
    fn finish(&mut self) -> ArrayRef {
        Arc::new(StringBuilder::finish(self))
    }
}

/// Make the table again from bronze under its current rule.
///
/// This is how a table takes a new column or a wider type: the LSM cannot add
/// a column to generations that already exist, and bronze holds every record
/// every source mapped to this table ever sent, byte for byte. Records that
/// were refused stay refused.
async fn rebuild(
    engine: &Engine,
    rule: &TableRule,
    embedder: Option<&embed::Embedder>,
) -> Result<Built, Error> {
    // Vectors the table already holds are kept, keyed by the text they were
    // made from, so a rebuild for a new column does not pay to embed every
    // row again. A column that is new to embedding has none to keep.
    let mut known: HashMap<String, Vec<f32>> = HashMap::new();
    for embedding in &rule.embeddings {
        let sql = format!(
            "SELECT {t} AS text, {v} AS vector FROM {table} WHERE {v} IS NOT NULL",
            t = quote(&embedding.source),
            v = quote(&embedding.column),
            table = quote(&rule.table)
        );
        let Ok(bytes) = engine.query(&sql).await else {
            continue;
        };
        for row in serde_json::from_slice::<Vec<serde_json::Value>>(&bytes).unwrap_or_default() {
            let (Some(text), Some(vector)) = (
                row.get("text").and_then(|t| t.as_str()),
                row.get("vector").and_then(|v| v.as_array()),
            ) else {
                continue;
            };
            let vector: Vec<f32> = vector
                .iter()
                .filter_map(|x| x.as_f64())
                .map(|x| x as f32)
                .collect();
            if vector.len() == embedding.dimensions {
                known.insert(text.to_owned(), vector);
            }
        }
    }

    let _ = engine.drop_table(&rule.table).await;
    engine.define_with(definition(rule), false).await?;
    let mut total = Built {
        batch: RecordBatch::new_empty(Arc::new(Schema::empty())),
        rescued: 0,
        embedded: 0,
        awaiting: 0,
    };
    let quoted: Vec<String> = rule
        .sources
        .iter()
        .filter(|s| valid_source(s))
        .map(|s| format!("'{s}'"))
        .collect();
    if quoted.is_empty() {
        return Ok(total);
    }
    let sources = quoted.join(", ");
    let table = rule.table.replace('\'', "''");
    let sql = format!(
        "SELECT b.{ID} AS id, b.source AS source, b.record AS record FROM {BRONZE} b \
         WHERE b.source IN ({sources}) \
         AND b.{ID} NOT IN (SELECT {ID} FROM {QUARANTINE} WHERE table_name = '{table}') \
         ORDER BY b.{seq}",
        seq = crate::engine::HIDDEN_SEQ
    );
    let bytes = engine.query(&sql).await?;
    let rows: Vec<serde_json::Value> = serde_json::from_slice(&bytes)?;
    for chunk in rows.chunks(2048) {
        let arrived = arrivals(chunk);
        let refs: Vec<&Arrived> = arrived.iter().collect();
        // A model that does not answer leaves these rows waiting for their
        // vectors, like any other write; the rebuild itself goes ahead.
        let _ = vectors_for(embedder, rule, texts(rule, &refs), &mut known).await;
        let built = batch(rule, &refs, &known)?;
        total.rescued += built.rescued;
        total.embedded += built.embedded;
        total.awaiting += built.awaiting;
        if built.batch.num_rows() > 0 {
            engine.append(&rule.table, vec![built.batch]).await?;
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_table_name_is_made_from_what_a_client_sent() {
        assert_eq!(
            table_name("Stripe-Webhooks"),
            Some("stripe_webhooks".into())
        );
        assert_eq!(table_name("invoice.paid"), Some("invoice_paid".into()));
        assert_eq!(table_name("2026-orders"), Some("t_2026_orders".into()));
        assert_eq!(table_name("---"), None);
    }

    #[test]
    fn a_field_that_cannot_be_a_column_is_kept_not_renamed() {
        assert!(column_name("user_id"));
        assert!(column_name("User ID"));
        assert!(!column_name("_internal"));
        assert!(!column_name(ID));
        assert!(!column_name(EXTRA));
        assert!(!column_name(""));
    }

    #[test]
    fn a_source_name_is_checked_before_it_becomes_a_key_or_a_literal() {
        assert!(valid_source("stripe.events-v2"));
        assert!(!valid_source("a'b"));
        assert!(!valid_source("a/b"));
        assert!(!valid_source(""));
    }

    #[test]
    fn a_name_in_another_case_convention_is_the_same_name() {
        assert!(spelled_alike("userId", "user_id"));
        assert!(spelled_alike("UserID", "user-id"));
        assert!(!spelled_alike("user_id", "user_id"));
        assert!(!spelled_alike("customer", "client"));
        assert!(!spelled_alike("user_id", "user_ids"));
    }

    #[test]
    fn leftovers_keep_their_digits() {
        let raw: Box<RawValue> =
            serde_json::from_str(r#"{"id": "a", "big": 9007199254740993, "note": "x"}"#).unwrap();
        let arrived = Arrived::new("s", &raw);
        let rule = TableRule {
            table: "t".into(),
            version: 1,
            columns: vec![RuleColumn {
                name: "id".into(),
                kind: "string".into(),
                required: true,
                aliases: Vec::new(),
            }],
            primary_key: Some("id".into()),
            keys_are_data: false,
            sources: vec!["s".into()],
            history: Vec::new(),
            embeddings: Vec::new(),
            embed_considered: Vec::new(),
        };
        let (_, rescued, extra) = row(&rule, &arrived);
        assert!(rescued);
        let extra = extra.unwrap();
        assert!(extra.contains("9007199254740993"), "{extra}");
    }
}
