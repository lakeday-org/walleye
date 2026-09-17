//! Turning a phrase into a query, by choosing rather than by writing.
//!
//! The decision service does not produce text. It picks one option from a set
//! you give it and says how sure it is. That is a poor way to write SQL and an
//! excellent way to build it, because every part of a query is a choice from
//! something the database already knows: which table, which column, which
//! direction, how many rows.
//!
//! So the options come from the catalog and the statement is assembled by
//! code. A column that does not exist cannot be chosen, a table that was
//! dropped is not on the list, and the result is always a statement this node
//! can run. The model supplies judgement about which of the real options the
//! phrase meant; it never supplies an identifier.
//!
//! Every question goes in one call, because the service answers them in
//! parallel and charges by the call rather than the question. One phrase is
//! one round trip, which is what makes this usable while somebody types.
use crate::engine::Engine;
type Error = Box<dyn std::error::Error + Send + Sync>;
use arrow_schema::{DataType, Schema};
use std::collections::BTreeMap;
use walleye_typesafe::{Answer, Client, Question};

/// What the phrase was read as, and how sure each part of that reading is.
#[derive(Debug, serde::Serialize)]
pub struct Reading {
    /// The statement, ready to run.
    pub sql: String,
    /// Per-decision confidence, so a caller can show or withhold.
    pub confidence: BTreeMap<String, f64>,
    /// How sure the least certain decision was.
    pub weakest: f64,
    /// What each decision came out as, for a caller that wants to explain
    /// itself rather than just show a statement.
    pub reading: BTreeMap<String, String>,
    /// How many readings it took. More than one means an earlier reading was
    /// assembled, refused by the planner, and read again with the refusal.
    pub attempts: u8,
    /// The statements the planner refused, with why, in order. Empty for a
    /// reading that planned first time.
    pub corrected: Vec<Refusal>,
}

/// A statement the planner would not accept, and its reason.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Refusal {
    pub sql: String,
    pub reason: String,
}

const NONE: &str = "none";

/// Candidate literals in the phrase: quoted strings and bare numbers.
///
/// A value cannot be a choice over the catalog, because it is not in the
/// catalog. Taking it from the phrase and letting the model pick between the
/// candidates keeps the model out of the business of inventing one.
fn literals(phrase: &str, known: &[String]) -> Vec<String> {
    // Words that are doing grammar rather than naming a value.
    const GRAMMAR: &[&str] = &[
        "a",
        "all",
        "an",
        "and",
        "any",
        "are",
        "as",
        "at",
        "by",
        "count",
        "descending",
        "each",
        "every",
        "first",
        "for",
        "from",
        "group",
        "how",
        "in",
        "is",
        "it",
        "last",
        "least",
        "many",
        "me",
        "most",
        "much",
        "of",
        "on",
        "or",
        "order",
        "ordered",
        "per",
        "rows",
        "show",
        "some",
        "sort",
        "sorted",
        "than",
        "that",
        "the",
        "them",
        "there",
        "to",
        "top",
        "total",
        "up",
        "was",
        "were",
        "what",
        "when",
        "where",
        "which",
        "who",
        "with",
        "biggest",
        "smallest",
        "highest",
        "lowest",
        "newest",
        "oldest",
        "over",
        "under",
        "above",
        "below",
        "more",
        "less",
        "break",
        "down",
        "list",
        "give",
        // Counting words. "top ten" says how many rows, not what to match.
        "one",
        "two",
        "three",
        "four",
        "five",
        "six",
        "seven",
        "eight",
        "nine",
        "ten",
        "twenty",
        "fifty",
        "hundred",
        "handful",
        "few",
        "several",
        "dozen",
    ];
    let mut found = Vec::new();
    let mut rest = phrase;
    while let Some(open) = rest.find(['\'', '"']) {
        let quote = rest.as_bytes()[open] as char;
        let after = &rest[open + 1..];
        match after.find(quote) {
            Some(close) if close > 0 => {
                found.push(after[..close].to_owned());
                rest = &after[close + 1..];
            }
            _ => break,
        }
    }
    for word in phrase.split(|c: char| !c.is_ascii_alphanumeric() && c != '.' && c != '-') {
        if !word.is_empty()
            && word
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_digit() || c == '-')
            && word.parse::<f64>().is_ok()
            && !found.iter().any(|f| f == word)
        {
            found.push(word.to_owned());
        }
    }
    // A bare word can be a value too: a city, a status, a name. Anything the
    // catalog already names is an identifier rather than a value, and the
    // grammar words above are neither.
    for word in phrase.split(|c: char| !c.is_ascii_alphanumeric() && c != '_' && c != '-') {
        let lower = word.to_ascii_lowercase();
        if word.is_empty()
            || word.len() < 2
            || GRAMMAR.contains(&lower.as_str())
            || known.iter().any(|k| {
                k.eq_ignore_ascii_case(word)
                    || lower
                        .strip_suffix('s')
                        .is_some_and(|stem| k.eq_ignore_ascii_case(stem))
                    || k.to_ascii_lowercase().strip_suffix('s') == Some(lower.as_str())
            })
            || found.iter().any(|f| f.eq_ignore_ascii_case(word))
        {
            continue;
        }
        found.push(word.to_owned());
    }
    found.truncate(24);
    found
}

fn describe_type(kind: &DataType) -> &'static str {
    match kind {
        DataType::Utf8 | DataType::LargeUtf8 => "text",
        DataType::Int64 | DataType::Int32 => "a whole number",
        DataType::Float64 | DataType::Float32 => "a number",
        DataType::Boolean => "true or false",
        DataType::FixedSizeList(_, _) => "a vector, for nearest-neighbour search",
        _ => "a value",
    }
}

/// Every question about one phrase, built from what the catalog actually has.
fn questions(
    candidates: &[String],
    tables: &[(String, Schema)],
    columns: &Schema,
) -> BTreeMap<String, Question> {
    let mut set = BTreeMap::new();

    if tables.len() > 1 {
        let criteria = tables.iter().map(|(name, schema)| {
            let cols: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            (name.clone(), format!("a table of {}", cols.join(", ")))
        });
        set.insert(
            "table".to_owned(),
            Question::choice("FROM clause. Which table is this asking about?", criteria),
        );
    }

    set.insert(
        "shape".to_owned(),
        Question::choice(
            "SELECT clause. What is being asked for?",
            [
                (
                    "count".to_owned(),
                    "Only a number is wanted: it asks how many, or for a total or a tally of \
                     the whole table."
                        .to_owned(),
                ),
                (
                    "rows".to_owned(),
                    "The records themselves are wanted, including when the phrase narrows to \
                     some of them."
                        .to_owned(),
                ),
                (
                    "group".to_owned(),
                    "A count per distinct value of some column, like a breakdown or a tally."
                        .to_owned(),
                ),
            ],
        ),
    );

    let named = |extra: &str| {
        let mut options: Vec<(String, String)> = columns
            .fields()
            .iter()
            .map(|f| {
                (
                    f.name().clone(),
                    format!(
                        "the column {}, holding {}",
                        f.name(),
                        describe_type(f.data_type())
                    ),
                )
            })
            .collect();
        options.push((NONE.to_owned(), extra.to_owned()));
        options
    };

    set.insert(
        "group_column".to_owned(),
        Question::choice(
            "GROUP BY clause. If it asks for a breakdown, which column is it broken \
             down by?",
            named("It asks for no breakdown."),
        ),
    );
    set.insert(
        "order_column".to_owned(),
        Question::choice(
            "ORDER BY clause. Which column does it want the answer ordered by, if any?",
            named("It states no order."),
        ),
    );
    set.insert(
        "direction".to_owned(),
        Question::choice(
            "ORDER BY direction. Largest first or smallest first?",
            [
                (
                    "descending".to_owned(),
                    "Largest, newest, highest, worst first.".to_owned(),
                ),
                (
                    "ascending".to_owned(),
                    "Smallest, oldest, lowest, best first.".to_owned(),
                ),
            ],
        ),
    );
    set.insert(
        "limit".to_owned(),
        Question::choice(
            "LIMIT clause. How many rows does it want back?",
            [
                (
                    "ten".to_owned(),
                    "A handful: top ten, a few, some examples.".to_owned(),
                ),
                ("fifty".to_owned(), "A page of them.".to_owned()),
                (
                    "all".to_owned(),
                    "It does not say, or it wants everything.".to_owned(),
                ),
            ],
        ),
    );

    // One pair of questions per candidate word, rather than one question
    // asking which column and another asking which value. Asked separately
    // the two answers need not agree, and for "pending orders in portland"
    // they did not: the column was the one holding cities and the value was
    // the word for a status. Asked per word each question is answerable on
    // its own, and a phrase naming two values gets both.
    for (index, value) in candidates.iter().enumerate() {
        let n = index + 1;
        set.insert(
            format!("where_column_{n}"),
            Question::choice(
                format!(
                    "WHERE clause. The phrase contains \"{value}\". If the rows have to match \
                     that, which column is it matched against?"
                ),
                named(&format!(
                    "\"{value}\" is part of how the question is phrased, not a value any \
                     column has to match."
                )),
            ),
        );
        set.insert(
            format!("where_op_{n}"),
            Question::choice(
                format!(
                    "WHERE clause. If \"{value}\" is matched against a column, which \
                     comparison does the phrase ask for?"
                ),
                [
                    ("equals".to_owned(), "Exactly this value.".to_owned()),
                    ("above".to_owned(), "Greater than this value.".to_owned()),
                    ("below".to_owned(), "Less than this value.".to_owned()),
                    (
                        "contains".to_owned(),
                        "Text merely containing this value, for a fragment or a partial word."
                            .to_owned(),
                    ),
                ],
            ),
        );
    }

    set
}

fn chosen(answers: &BTreeMap<String, Answer>, key: &str) -> Option<(String, f64)> {
    let answer = answers.get(key)?;
    Some((answer.label()?, answer.confidence()))
}

fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// A literal, rendered so it cannot carry anything but itself.
fn literal(value: &str, kind: Option<&DataType>) -> String {
    let numeric = matches!(
        kind,
        Some(DataType::Int64 | DataType::Int32 | DataType::Float64 | DataType::Float32)
    );
    if numeric && value.parse::<f64>().is_ok() {
        return value.to_owned();
    }
    format!("'{}'", value.replace('\'', "''"))
}

/// The grammar the decisions are filling in, and the catalog they are chosen
/// from. This is the state the questions are asked against, so the service
/// sees the shape of the statement rather than only the phrase.
fn task(phrase: &str, tables: &[(String, Schema)], refused: &[Refusal]) -> String {
    let mut out = format!("Someone typed this into a database search box:\n\n  {phrase}\n\n");
    out.push_str(
        "It has to become exactly one SQL statement of this form, where any clause may be \
         absent:\n\n  SELECT <columns> FROM <table> WHERE <column> <op> <value> \
         GROUP BY <column> ORDER BY <column> <direction> LIMIT <n>\n\nEach question below \
         fills one slot in it. The statement is assembled from the answers, so an answer \
         that does not belong in its clause makes a statement that does not run.\n\n",
    );
    out.push_str("The tables it may run against:\n");
    for (name, schema) in tables {
        let cols: Vec<String> = schema
            .fields()
            .iter()
            .map(|f| format!("{} ({})", f.name(), describe_type(f.data_type())))
            .collect();
        out.push_str(&format!("  {name}: {}\n", cols.join(", ")));
    }
    if !refused.is_empty() {
        out.push_str(
            "\nEarlier readings of this same phrase were assembled and handed to the query \
             planner, which refused them. Do not choose the same way again:\n",
        );
        for Refusal { sql, reason } in refused {
            out.push_str(&format!("\n  {sql}\n    refused: {reason}\n"));
        }
    }
    out
}

/// Build the statement from one set of answers. Every identifier comes from
/// the schema, so this cannot emit a name the table does not have; what it can
/// emit is a combination the planner rejects, which is what the caller checks.
fn assemble(
    answers: &BTreeMap<String, Answer>,
    candidates: &[String],
    tables: &[(String, Schema)],
    single: Option<&(String, Schema)>,
) -> Result<Reading, Error> {
    let mut confidence = BTreeMap::new();
    let mut reading = BTreeMap::new();
    let mut take = |key: &str| -> Option<String> {
        let (label, sure) = chosen(answers, key)?;
        confidence.insert(key.to_owned(), (sure * 100.0).round() / 100.0);
        reading.insert(key.to_owned(), label.clone());
        Some(label)
    };

    let table = match single {
        Some((name, _)) => name.clone(),
        None => take("table").ok_or("could not tell which table")?,
    };
    let schema = tables
        .iter()
        .find(|(name, _)| name == &table)
        .map(|(_, schema)| schema.clone())
        .ok_or("could not tell which table")?;
    let has = |column: &str| schema.fields().iter().any(|f| f.name() == column);
    let kind = |column: &str| {
        schema
            .fields()
            .iter()
            .find(|f| f.name() == column)
            .map(|f| f.data_type().clone())
    };

    let shape = take("shape").unwrap_or_else(|| "rows".into());
    // Every word the phrase offered, against whichever column the service put
    // it with. A word it placed nowhere contributes nothing.
    let mut predicates: Vec<String> = Vec::new();
    let mut matched: Vec<String> = Vec::new();
    for (index, value) in candidates.iter().enumerate() {
        let n = index + 1;
        let (column_key, op_key) = (format!("where_column_{n}"), format!("where_op_{n}"));
        let Some(column) = take(&column_key).filter(|c| c != NONE && has(c)) else {
            continue;
        };
        let op = take(&op_key).unwrap_or_else(|| "equals".into());
        let numeric = matches!(
            kind(&column),
            Some(DataType::Int64 | DataType::Int32 | DataType::Float64 | DataType::Float32)
        );
        if numeric && (op == "contains" || value.parse::<f64>().is_err()) {
            continue;
        }
        let rendered = literal(value, kind(&column).as_ref());
        predicates.push(match op.as_str() {
            "above" => format!("{} > {rendered}", quote(&column)),
            "below" => format!("{} < {rendered}", quote(&column)),
            "contains" => format!("{} LIKE '%{}%'", quote(&column), value.replace('\'', "''")),
            _ => format!("{} = {rendered}", quote(&column)),
        });
        matched.extend([column_key, op_key]);
    }
    let group_column = take("group_column").filter(|c| c != NONE && has(c));
    let order_column = take("order_column").filter(|c| c != NONE && has(c));
    let direction = take("direction").unwrap_or_else(|| "descending".into());
    let limit = take("limit").unwrap_or_else(|| "all".into());

    let mut sql = String::new();
    let grouped = shape == "group" && group_column.is_some();
    if grouped {
        let column = group_column.clone().expect("checked");
        sql.push_str(&format!(
            "SELECT {}, count(*) AS n FROM {}",
            quote(&column),
            quote(&table)
        ));
    } else if shape == "count" {
        sql.push_str(&format!("SELECT count(*) AS n FROM {}", quote(&table)));
    } else {
        sql.push_str(&format!("SELECT * FROM {}", quote(&table)));
    }

    if !predicates.is_empty() {
        sql.push_str(&format!(" WHERE {}", predicates.join(" AND ")));
    }

    if grouped {
        let column = group_column.expect("checked");
        sql.push_str(&format!(" GROUP BY {}", quote(&column)));
        sql.push_str(" ORDER BY n DESC");
    } else if shape != "count"
        && let Some(column) = order_column
    {
        let way = if direction == "ascending" {
            "ASC"
        } else {
            "DESC"
        };
        sql.push_str(&format!(" ORDER BY {} {way}", quote(&column)));
    }

    if shape != "count" {
        match limit.as_str() {
            "ten" => sql.push_str(" LIMIT 10"),
            "fifty" => sql.push_str(" LIMIT 50"),
            _ => {}
        }
    }

    // Only the decisions that shaped the statement count. A question that did
    // not apply, asked speculatively because asking is free, is legitimately
    // unsure and saying so would make every reading look doubtful.
    let mut used: Vec<String> = vec!["shape".to_owned()];
    if single.is_none() {
        used.push("table".to_owned());
    }
    used.append(&mut matched);
    if grouped {
        used.push("group_column".to_owned());
    } else if sql.contains(" ORDER BY ") {
        used.extend(["order_column".to_owned(), "direction".to_owned()]);
    }
    if sql.contains(" LIMIT ") {
        used.push("limit".to_owned());
    }
    let weakest = used
        .iter()
        .filter_map(|key| confidence.get(key).copied())
        .fold(1.0_f64, f64::min);
    confidence.retain(|key, _| used.contains(key));
    reading.retain(|key, _| used.contains(key));
    Ok(Reading {
        sql,
        confidence,
        weakest,
        reading,
        attempts: 1,
        corrected: Vec::new(),
    })
}

impl Engine {
    /// Plan the statement without running it. This is what makes the loop
    /// worth having: the planner knows the things the questions cannot, like
    /// whether the chosen column can be compared against the chosen value, and
    /// it says so for the price of a plan rather than a scan.
    async fn rehearse(&self, sql: &str) -> Result<(), String> {
        match self.query(&format!("EXPLAIN {sql}")).await {
            Ok(_) => Ok(()),
            Err(error) => Err(error.to_string()),
        }
    }

    pub async fn read_query(&self, phrase: &str) -> Result<Reading, Error> {
        /// One reading, then two more chances with the planner's objection in
        /// hand. A phrase the service cannot read into a runnable statement in
        /// three goes is a phrase to hand back, not to keep paying for.
        const ATTEMPTS: u8 = 3;

        let phrase = phrase.trim();
        if phrase.is_empty() {
            return Err("say what you are looking for".into());
        }
        if phrase.len() > 512 {
            return Err("that is longer than a question".into());
        }
        let Some(client) = Client::from_env() else {
            return Err("reading a phrase needs a decision service: set TYPESAFE_API_KEY".into());
        };

        let mut tables = Vec::new();
        for name in self.table_names().await? {
            if let Ok((_, schema)) = self.describe(&name).await {
                tables.push((name, schema));
            }
        }
        if tables.is_empty() {
            return Err("this node has no tables to ask about".into());
        }

        // With one table there is nothing to decide, and asking would invite a
        // wrong answer where there is only one right one.
        let single = (tables.len() == 1).then(|| tables[0].clone());
        let columns = single
            .as_ref()
            .map(|(_, schema)| schema.clone())
            .unwrap_or_else(|| {
                // Before the table is known, offer every column there is; the
                // assembly above discards any that the chosen table lacks.
                let mut fields: Vec<arrow_schema::FieldRef> = Vec::new();
                for (_, schema) in &tables {
                    for field in schema.fields() {
                        if !fields.iter().any(|f| f.name() == field.name()) {
                            fields.push(field.clone());
                        }
                    }
                }
                Schema::new(fields)
            });

        // A phrase is allowed a few values, not a sentence of them: each one
        // costs two questions, and past a handful the phrase is prose rather
        // than a search.
        const VALUES: usize = 4;
        let mut known: Vec<String> = tables.iter().map(|(name, _)| name.clone()).collect();
        for (_, schema) in &tables {
            for field in schema.fields() {
                known.push(field.name().clone());
            }
        }
        let mut candidates = literals(phrase, &known);
        candidates.truncate(VALUES);

        let set = questions(&candidates, &tables, &columns);
        let mut corrected: Vec<Refusal> = Vec::new();
        for attempt in 1..=ATTEMPTS {
            // The refusals are part of the state, so each retry is a different
            // question to the service rather than a cached repeat of the last
            // one.
            let state = task(phrase, &tables, &corrected);
            let decided = client
                .ask(&state, &set)
                .await
                .map_err(|e| -> Error { e.to_string().into() })?;
            let answers: BTreeMap<String, Answer> = decided
                .answers
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            let mut reading = assemble(&answers, &candidates, &tables, single.as_ref())?;
            match self.rehearse(&reading.sql).await {
                Ok(()) => {
                    reading.attempts = attempt;
                    reading.corrected = corrected;
                    return Ok(reading);
                }
                Err(reason) => corrected.push(Refusal {
                    sql: reading.sql,
                    reason,
                }),
            }
        }
        let last = corrected
            .last()
            .expect("a failed attempt recorded a refusal");
        Err(format!(
            "read that as `{}`, which this node will not plan: {}",
            last.sql, last.reason
        )
        .into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;

    fn orders() -> (String, Schema) {
        (
            "orders".to_owned(),
            Schema::new(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("customer", DataType::Utf8, true),
                Field::new("city", DataType::Utf8, true),
                Field::new("status", DataType::Utf8, true),
                Field::new("total", DataType::Float64, true),
            ]),
        )
    }
    fn known_names(tables: &[(String, Schema)]) -> Vec<String> {
        let mut known: Vec<String> = tables.iter().map(|(n, _)| n.clone()).collect();
        for (_, schema) in tables {
            known.extend(schema.fields().iter().map(|f| f.name().clone()));
        }
        known
    }
    fn pick(choice: &str, confidence: f64) -> Answer {
        Answer::Choice {
            choice: choice.to_owned(),
            confidence,
            probabilities: Default::default(),
        }
    }
    fn answers(pairs: &[(&str, &str)]) -> BTreeMap<String, Answer> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), pick(v, 0.9)))
            .collect()
    }

    /// The words a phrase offers as values are the ones that are neither
    /// grammar nor something the catalog already names.
    #[test]
    fn a_phrase_offers_only_its_values() {
        let tables = [orders()];
        let known = known_names(&tables);
        assert_eq!(
            literals("pending orders in portland", &known),
            ["pending", "portland"]
        );
        assert_eq!(literals("orders over 100", &known), ["100"]);
        assert_eq!(literals("how many orders", &known), Vec::<String>::new());
    }

    /// "top ten" says how many rows to return. Read as a value it became
    /// `WHERE "total" > 'ten'`, which is a filter nobody asked for.
    #[test]
    fn a_counting_word_is_not_a_value() {
        let tables = [orders()];
        assert_eq!(
            literals("biggest orders first, top ten", &known_names(&tables)),
            Vec::<String>::new()
        );
    }

    /// A plural of a column is that column being named, not a value: "which
    /// carriers are late" is about `carrier`.
    #[test]
    fn a_plural_of_a_column_is_the_column() {
        let tables = [(
            "shipments".to_owned(),
            Schema::new(vec![
                Field::new("carrier", DataType::Utf8, true),
                Field::new("days_late", DataType::Int64, true),
            ]),
        )];
        let offered = literals("which carriers are slow", &known_names(&tables));
        assert!(!offered.iter().any(|w| w == "carriers"), "got {offered:?}");
    }

    /// Each value is placed by its own question, so a phrase naming two of
    /// them gets both, each against the column it was placed with. Asked as
    /// one column question and one value question, this produced the city
    /// column holding the status word.
    #[test]
    fn two_values_become_two_predicates() {
        let tables = [orders()];
        let candidates = vec!["pending".to_owned(), "portland".to_owned()];
        let reading = assemble(
            &answers(&[
                ("shape", "rows"),
                ("where_column_1", "status"),
                ("where_op_1", "equals"),
                ("where_column_2", "city"),
                ("where_op_2", "equals"),
                ("group_column", NONE),
                ("order_column", NONE),
                ("limit", "all"),
            ]),
            &candidates,
            &tables,
            Some(&tables[0]),
        )
        .unwrap();
        assert_eq!(
            reading.sql,
            r#"SELECT * FROM "orders" WHERE "status" = 'pending' AND "city" = 'portland'"#
        );
    }

    /// A number column compared against a word plans without complaint,
    /// because casting is legal, and then means nothing. The pairing is
    /// dropped rather than written down.
    #[test]
    fn a_word_is_not_compared_against_a_number_column() {
        let tables = [orders()];
        let reading = assemble(
            &answers(&[
                ("shape", "rows"),
                ("where_column_1", "total"),
                ("where_op_1", "above"),
                ("group_column", NONE),
                ("order_column", NONE),
                ("limit", "all"),
            ]),
            &["late".to_owned()],
            &tables,
            Some(&tables[0]),
        )
        .unwrap();
        assert_eq!(reading.sql, r#"SELECT * FROM "orders""#);
    }

    /// A column the chosen table does not have cannot reach the statement,
    /// whichever table's schema it came from.
    #[test]
    fn a_column_the_table_lacks_is_discarded() {
        let tables = [orders()];
        let reading = assemble(
            &answers(&[
                ("shape", "rows"),
                ("where_column_1", "days_late"),
                ("where_op_1", "above"),
                ("order_column", "carrier"),
                ("group_column", NONE),
                ("limit", "all"),
            ]),
            &["3".to_owned()],
            &tables,
            Some(&tables[0]),
        )
        .unwrap();
        assert_eq!(reading.sql, r#"SELECT * FROM "orders""#);
    }

    /// Confidence is reported over the decisions that shaped the statement.
    /// Minimising over every question asked, including the ones that did not
    /// apply, made a certain reading look like a doubtful one.
    #[test]
    fn only_the_decisions_that_shaped_it_count() {
        let tables = [orders()];
        let mut set = answers(&[("shape", "count")]);
        set.insert("order_column".to_owned(), pick(NONE, 0.2));
        set.insert("group_column".to_owned(), pick(NONE, 0.1));
        set.insert("limit".to_owned(), pick("all", 0.3));
        let reading = assemble(&set, &[], &tables, Some(&tables[0])).unwrap();
        assert_eq!(reading.sql, r#"SELECT count(*) AS n FROM "orders""#);
        assert_eq!(reading.weakest, 0.9);
        assert_eq!(reading.reading.keys().collect::<Vec<_>>(), ["shape"]);
    }

    /// Identifiers and values are escaped on the way in, so a table or a value
    /// carrying a quote closes nothing.
    #[test]
    fn quotes_in_names_and_values_are_escaped() {
        assert_eq!(quote(r#"we"ird"#), r#""we""ird""#);
        assert_eq!(literal("O'Hare", Some(&DataType::Utf8)), "'O''Hare'");
        assert_eq!(literal("100", Some(&DataType::Float64)), "100");
        assert_eq!(literal("100", Some(&DataType::Utf8)), "'100'");
    }

    /// The state carries the grammar being filled in and the catalog it is
    /// filled from, and on a retry the statements the planner already refused.
    #[test]
    fn a_retry_states_what_was_refused() {
        let tables = [orders()];
        let first = task("orders in seattle", &tables, &[]);
        assert!(first.contains("SELECT <columns> FROM <table>"), "{first}");
        assert!(first.contains("orders: id"), "{first}");
        let again = task(
            "orders in seattle",
            &tables,
            &[Refusal {
                sql: r#"SELECT * FROM "orders" WHERE "nope" = 1"#.to_owned(),
                reason: "No field named nope".to_owned(),
            }],
        );
        assert!(again.contains("No field named nope"), "{again}");
        assert!(
            again.contains("Do not choose the same way again"),
            "{again}"
        );
    }
}
