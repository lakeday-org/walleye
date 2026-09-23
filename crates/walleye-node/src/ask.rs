//! Answering a question asked in words, with rows.
//!
//! A model writes one SELECT against this node's catalog, the planner checks
//! it before it runs, and the rows come back with the statement that produced
//! them. The statement is part of the answer rather than instead of it: it is
//! how the answer gets checked, corrected, saved as a view, or run again.
//!
//! What is deliberately absent: there is no second reader to fall back on when
//! the model is unreachable, and no guess when it is unconfigured. A question
//! is either answered by the model or refused, because a quietly worse answer
//! is harder to notice than no answer.
use crate::engine::Engine;
use arrow_schema::DataType;
use walleye_lance::model::Model;

type Error = Box<dyn std::error::Error + Send + Sync>;

/// A question, the statement it was read as, and the rows that statement
/// returned.
#[derive(Debug, serde::Serialize)]
pub struct Answered {
    pub sql: String,
    /// The rows, as records.
    pub rows: serde_json::Value,
    pub row_count: usize,
    /// What the model asked the decision service along the way, and how sure
    /// each answer was. Empty when it asked nothing.
    pub decisions: Vec<walleye_lance::model::Decision>,
    /// Statements the planner refused before this one was accepted.
    pub corrected: Vec<Refusal>,
    /// Which member wrote the statement, and which ran it.
    pub drafted_by: String,
    pub ran_on: String,
    pub milliseconds: u64,
    pub usage: Usage,
}

/// A statement the planner would not accept, and why.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Refusal {
    pub sql: String,
    pub reason: String,
}

#[derive(Debug, Default, serde::Serialize)]
pub struct Usage {
    pub rounds: u32,
    pub input_tokens: u64,
    pub cached_tokens: u64,
    pub output_tokens: u64,
    pub reasoning_tokens: u64,
    pub decisions: usize,
}

fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn describe(kind: &DataType) -> &'static str {
    match kind {
        DataType::Utf8 | DataType::LargeUtf8 => "text",
        DataType::Int64 | DataType::Int32 => "bigint",
        DataType::Float64 | DataType::Float32 => "double",
        DataType::Boolean => "boolean",
        DataType::FixedSizeList(_, _) => "vector",
        _ => "text",
    }
}

impl Engine {
    /// Every table this node knows, as DDL the model reads.
    async fn catalog_ddl(&self) -> Result<String, Error> {
        let mut out = String::new();
        let mut searchable = false;
        for (name, schema) in self.schemas().await? {
            let columns: Vec<String> = schema
                .fields()
                .iter()
                .filter(|f| !crate::engine::hidden(f.name()))
                .map(|f| format!("  {} {}", quote(f.name()), describe(f.data_type())))
                .collect();
            if columns.is_empty() {
                continue;
            }
            out.push_str(&format!(
                "CREATE TABLE {} (\n{}\n);\n",
                quote(&name),
                columns.join(",\n")
            ));
            // Say which vectors hold the meaning of which text, and how to use
            // them. Only where a query could: `embed` needs the same model
            // that made the vectors.
            if walleye_lance::embed::Embedder::from_env().is_some() {
                for field in schema.fields() {
                    let Some(text) = field.name().strip_suffix("_embedding") else {
                        continue;
                    };
                    if !matches!(field.data_type(), DataType::FixedSizeList(_, _))
                        || schema.field_with_name(text).is_err()
                    {
                        continue;
                    }
                    out.push_str(&format!(
                        "-- {v} holds the meaning of {t}. To find rows whose {t} is about \
                         something, whatever words it uses: ORDER BY cosine_distance({v}, \
                         embed('what to look for')) LIMIT n. Smaller is closer.\n",
                        v = quote(field.name()),
                        t = quote(text),
                    ));
                    searchable = true;
                }
            }
        }
        if out.is_empty() {
            return Err("this node has no tables to ask about".into());
        }
        if searchable {
            out.push_str(
                "-- When a question asks for rows by what their text means or is about, \
                 rather than for exact words, use the vector search above instead of LIKE.\n",
            );
        }
        Ok(out)
    }

    /// Plan a statement without running it.
    ///
    /// This is what makes the loop worth having: the planner knows things the
    /// model cannot, such as whether a column exists on the table it was put
    /// with, and it says so for the price of a plan rather than a scan.
    async fn rehearse(&self, sql: &str) -> Result<(), String> {
        match self.query(&format!("EXPLAIN {sql}")).await {
            Ok(_) => Ok(()),
            Err(error) => Err(error.to_string()),
        }
    }

    /// Answer a question with rows.
    pub async fn answer(&self, question: &str) -> Result<Answered, Error> {
        /// One statement, then two more chances with the planner's objection
        /// in hand. A question that cannot be read into a runnable statement
        /// in three goes is handed back rather than paid for again.
        const ATTEMPTS: usize = 3;

        let started = std::time::Instant::now();
        let question = question.trim();
        if question.is_empty() {
            return Err("say what you are looking for".into());
        }
        if question.len() > 2048 {
            return Err("that is longer than a question".into());
        }
        let Some(model) = Model::from_env() else {
            return Err("answering a question needs a model: set WALLEYE_MODEL_KEY".into());
        };
        let decider = walleye_typesafe::Client::from_env();
        let schema = self.catalog_ddl().await?;
        let me = self.ownership().node();

        let mut refused: Vec<Refusal> = Vec::new();
        let mut written = None;
        for _ in 0..ATTEMPTS {
            let pairs: Vec<(String, String)> = refused
                .iter()
                .map(|r| (r.sql.clone(), r.reason.clone()))
                .collect();
            let state = format!(
                "A question asked of a SQL database.\n\nSchema:\n{schema}\nQuestion: {question}"
            );
            let draft = model
                .draft(&schema, question, &pairs, decider.as_ref(), &state)
                .await?;
            match self.rehearse(&draft.sql).await {
                Ok(()) => {
                    written = Some(draft);
                    break;
                }
                Err(reason) => refused.push(Refusal {
                    sql: draft.sql,
                    reason,
                }),
            }
        }
        let Some(draft) = written else {
            let last = refused.last().expect("a failed attempt recorded a refusal");
            return Err(format!(
                "read that as `{}`, which this node will not plan: {}",
                last.sql, last.reason
            )
            .into());
        };

        // A statement over a single table runs where that table lives; one
        // spanning owners runs here and gathers what it does not own.
        let mut ran_on = me.clone();
        let named = walleye_lance::sql_table_names(&draft.sql).unwrap_or_default();
        let bytes = match named.first().filter(|_| named.len() == 1) {
            Some(table) => match self.route(table, false).await? {
                crate::ownership::Route::Remote { peer, .. } => {
                    ran_on = peer.node.clone();
                    self.cluster().run_sql(&peer, &draft.sql).await?
                }
                _ => bytes::Bytes::from(self.query(&draft.sql).await?),
            },
            None => bytes::Bytes::from(self.query(&draft.sql).await?),
        };
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&bytes)?;
        Ok(Answered {
            sql: draft.sql,
            row_count: rows.len(),
            rows: serde_json::Value::Array(rows),
            usage: Usage {
                rounds: draft.rounds,
                input_tokens: draft.input_tokens,
                cached_tokens: draft.cached_tokens,
                output_tokens: draft.output_tokens,
                reasoning_tokens: draft.reasoning_tokens,
                decisions: draft.decisions.len(),
            },
            decisions: draft.decisions,
            corrected: refused,
            drafted_by: me,
            ran_on,
            milliseconds: started.elapsed().as_millis() as u64,
        })
    }
}
