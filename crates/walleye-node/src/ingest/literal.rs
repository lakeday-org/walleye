//! What a field's value actually is, before anybody decides what it means.
//!
//! Everything here works on the text a client sent, never on a parsed number.
//! A JSON parser that reads `9007199254740993` into a float has already lost
//! the last digit, and nothing downstream can bring it back, so the value is
//! kept as its literal text until a column type has been chosen and the
//! conversion can be exact or refuse.
//!
//! Two jobs live here, and both are code rather than judgement:
//!
//! - which types a set of values could be stored as without loss
//!   ([`candidates`]), so that a model choosing between them can only choose
//!   among types the data admits;
//! - converting one value to one of those types ([`convert`]), exactly or not
//!   at all.
use serde_json::value::RawValue;

/// The types an ingested column can be. This is the whole list a model is
/// offered: nothing outside it can be chosen, and nothing in it is offered
/// unless every value seen converts to it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Kind {
    Boolean,
    Int64,
    Float64,
    Decimal,
    TimestampIso,
    TimestampSeconds,
    TimestampMillis,
    TimestampMicros,
    String,
    Json,
}

impl Kind {
    /// Every kind, in the order they are offered.
    pub const ALL: [Kind; 10] = [
        Kind::Boolean,
        Kind::Int64,
        Kind::Float64,
        Kind::Decimal,
        Kind::TimestampIso,
        Kind::TimestampSeconds,
        Kind::TimestampMillis,
        Kind::TimestampMicros,
        Kind::String,
        Kind::Json,
    ];

    /// The name a model is shown and a rule stores.
    pub fn name(self) -> &'static str {
        match self {
            Kind::Boolean => "boolean",
            Kind::Int64 => "int64",
            Kind::Float64 => "float64",
            Kind::Decimal => "decimal",
            Kind::TimestampIso => "timestamp_text",
            Kind::TimestampSeconds => "timestamp_seconds",
            Kind::TimestampMillis => "timestamp_millis",
            Kind::TimestampMicros => "timestamp_micros",
            Kind::String => "string",
            Kind::Json => "json",
        }
    }

    pub fn from_name(name: &str) -> Option<Kind> {
        Kind::ALL.into_iter().find(|kind| kind.name() == name)
    }

    /// What the option means, for the model choosing between them. Written
    /// about the data rather than the storage, because that is what the
    /// choice turns on.
    pub fn meaning(self) -> &'static str {
        match self {
            Kind::Boolean => "a true or false flag",
            Kind::Int64 => {
                "a whole number that is counted or compared, such as a quantity or a count"
            }
            Kind::Float64 => {
                "a measurement where tiny rounding does not matter, such as a temperature, a \
                 ratio or a score"
            }
            Kind::Decimal => {
                "an exact decimal amount that must never be rounded, such as money or a price"
            }
            Kind::TimestampIso => "a moment in time written out as a date or date and time",
            Kind::TimestampSeconds => "a moment in time given as seconds since 1970",
            Kind::TimestampMillis => "a moment in time given as milliseconds since 1970",
            Kind::TimestampMicros => "a moment in time given as microseconds since 1970",
            Kind::String => {
                "text, or an identifier or code that only looks like a number - a zip code, a \
                 phone number, an account number - where leading zeros matter and arithmetic \
                 means nothing"
            }
            Kind::Json => "a nested object or list, kept whole",
        }
    }

    /// The storage type a stream definition names for this kind.
    pub fn storage(self) -> &'static str {
        match self {
            Kind::Boolean => "boolean",
            Kind::Int64 => "int64",
            Kind::Float64 => "float64",
            Kind::Decimal => "decimal",
            Kind::TimestampIso
            | Kind::TimestampSeconds
            | Kind::TimestampMillis
            | Kind::TimestampMicros => "timestamp",
            Kind::String => "string",
            Kind::Json => "json",
        }
    }

    /// Whether a column of this kind can be part of a primary key. The LSM
    /// hashes keys, and only these hash the same way every time.
    pub fn keyable(self) -> bool {
        matches!(self, Kind::Int64 | Kind::String | Kind::Boolean)
    }
}

/// One value as it arrived, classified but not interpreted. Numbers keep
/// their text.
#[derive(Clone, Debug, PartialEq)]
pub enum Literal {
    Null,
    Bool(bool),
    Number(String),
    Text(String),
    Object(String),
    Array(String),
}

impl Literal {
    /// Read a raw value without losing anything it said.
    pub fn read(raw: &RawValue) -> Literal {
        let text = raw.get().trim();
        match text.as_bytes().first() {
            None | Some(b'n') => Literal::Null,
            Some(b't') => Literal::Bool(true),
            Some(b'f') => Literal::Bool(false),
            Some(b'"') => serde_json::from_str::<String>(text)
                .map(Literal::Text)
                .unwrap_or_else(|_| Literal::Text(text.to_owned())),
            Some(b'{') => Literal::Object(text.to_owned()),
            Some(b'[') => Literal::Array(text.to_owned()),
            _ => Literal::Number(text.to_owned()),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Literal::Null)
    }

    /// The value as JSON text, exactly as it would round-trip.
    pub fn json(&self) -> String {
        match self {
            Literal::Null => "null".to_owned(),
            Literal::Bool(value) => value.to_string(),
            Literal::Number(text) | Literal::Object(text) | Literal::Array(text) => text.clone(),
            Literal::Text(text) => serde_json::to_string(text).unwrap_or_default(),
        }
    }

    /// A short rendering for a model to read, bounded so one long blob does
    /// not crowd out the rest of a sample.
    pub fn sample(&self) -> String {
        let text = self.json();
        if text.chars().count() > 80 {
            let cut: String = text.chars().take(77).collect();
            format!("{cut}...")
        } else {
            text
        }
    }
}

/// A converted value, ready for an Arrow builder.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Bool(bool),
    Int(i64),
    Float(f64),
    /// Scaled by 10^[`DECIMAL_SCALE`].
    Decimal(i128),
    /// Microseconds since 1970, UTC.
    Micros(i64),
    Text(String),
}

/// Decimal columns are stored with this many places. Nine keeps every
/// currency exact and most rates; a value with more places is not offered
/// decimal at all rather than being rounded into it.
pub const DECIMAL_SCALE: u32 = 9;
/// And this many digits in total, the widest a 128-bit decimal holds.
pub const DECIMAL_PRECISION: u8 = 38;

/// Convert one value to one kind, exactly or not at all. `None` means the
/// value does not fit, never that it was approximated.
pub fn convert(kind: Kind, literal: &Literal) -> Option<Value> {
    match (kind, literal) {
        (_, Literal::Null) => None,
        (Kind::Boolean, Literal::Bool(value)) => Some(Value::Bool(*value)),
        (Kind::Int64, Literal::Number(text)) => whole(text).map(Value::Int),
        (Kind::Int64, Literal::Text(text)) => digits(text).and_then(whole).map(Value::Int),
        (Kind::Float64, Literal::Number(text)) => float(text).map(Value::Float),
        (Kind::Float64, Literal::Text(text)) => numeric(text).and_then(float).map(Value::Float),
        (Kind::Decimal, Literal::Number(text)) => decimal(text).map(Value::Decimal),
        (Kind::Decimal, Literal::Text(text)) => numeric(text).and_then(decimal).map(Value::Decimal),
        (Kind::TimestampIso, Literal::Text(text)) => iso(text).map(Value::Micros),
        (Kind::TimestampSeconds, value) => epoch(value, 1_000_000).map(Value::Micros),
        (Kind::TimestampMillis, value) => epoch(value, 1_000).map(Value::Micros),
        (Kind::TimestampMicros, value) => epoch(value, 1).map(Value::Micros),
        (Kind::String, Literal::Text(text)) => Some(Value::Text(text.clone())),
        (Kind::String, Literal::Number(text)) => Some(Value::Text(text.clone())),
        (Kind::String, Literal::Bool(value)) => Some(Value::Text(value.to_string())),
        (Kind::Json, value) => Some(Value::Text(value.json())),
        _ => None,
    }
}

/// Every kind all of these values convert to, most specific first. Nulls say
/// nothing about type and are ignored; a column seen only as null admits
/// every scalar kind, which is why an empty sample is decided as text.
pub fn candidates<'a>(values: impl IntoIterator<Item = &'a Literal>) -> Vec<Kind> {
    let seen: Vec<&Literal> = values.into_iter().filter(|v| !v.is_null()).collect();
    if seen.is_empty() {
        return vec![Kind::String];
    }
    // A nested object or list is stored whole. Offering it a scalar type
    // would only ever be refused value by value.
    if seen
        .iter()
        .any(|v| matches!(v, Literal::Object(_) | Literal::Array(_)))
    {
        return vec![Kind::Json];
    }
    Kind::ALL
        .into_iter()
        .filter(|kind| *kind != Kind::Json)
        .filter(|kind| seen.iter().all(|value| convert(*kind, value).is_some()))
        .collect()
}

/// The kind to use when nobody has judged: the narrowest one that loses
/// nothing. Floating point is never it, because floating point is the one
/// that rounds; a timestamp never is either, because reading a number as a
/// moment is an interpretation, and an interpretation needs a judge.
pub fn lossless(options: &[Kind]) -> Kind {
    for kind in [
        Kind::Boolean,
        Kind::Int64,
        Kind::Decimal,
        Kind::String,
        Kind::Json,
    ] {
        if options.contains(&kind) {
            return kind;
        }
    }
    Kind::Json
}

fn whole(text: &str) -> Option<i64> {
    let text = text.trim();
    if text.contains(['.', 'e', 'E']) {
        return None;
    }
    text.parse::<i64>().ok()
}

/// A string that is a plain integer, and stays one: no leading zero, because
/// "02134" becomes 2134 and that is not the same zip code.
fn digits(text: &str) -> Option<&str> {
    let body = text.strip_prefix('-').unwrap_or(text);
    if body.is_empty() || !body.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if body.len() > 1 && body.starts_with('0') {
        return None;
    }
    Some(text)
}

/// A string that reads as a number and nothing else. Words like "nan" and
/// "inf" parse as floats and are not numbers anybody sent on purpose.
fn numeric(text: &str) -> Option<&str> {
    let body = text.strip_prefix('-').unwrap_or(text);
    if body.is_empty() {
        return None;
    }
    let mut dot = false;
    for (index, byte) in body.bytes().enumerate() {
        match byte {
            b'0'..=b'9' => {}
            b'.' if !dot && index > 0 && index + 1 < body.len() => dot = true,
            _ => return None,
        }
    }
    // A leading zero before more digits is an identifier, not a quantity.
    if body.len() > 1 && body.starts_with('0') && !body.starts_with("0.") {
        return None;
    }
    Some(text)
}

fn float(text: &str) -> Option<f64> {
    text.trim()
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
}

/// A decimal literal scaled to [`DECIMAL_SCALE`] places, exactly. Exponents
/// are refused rather than expanded: they are how floats get written, and a
/// value that arrived as one was never exact.
fn decimal(text: &str) -> Option<i128> {
    let text = text.trim();
    if text.contains(['e', 'E']) {
        return None;
    }
    let (negative, body) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (integer, fraction) = body.split_once('.').unwrap_or((body, ""));
    if integer.is_empty()
        || !integer.bytes().all(|b| b.is_ascii_digit())
        || !fraction.bytes().all(|b| b.is_ascii_digit())
        || fraction.len() > DECIMAL_SCALE as usize
        || integer.len() > (DECIMAL_PRECISION as usize - DECIMAL_SCALE as usize)
    {
        return None;
    }
    let mut scaled: i128 = integer.parse().ok()?;
    let mut places = 0;
    for digit in fraction.bytes() {
        scaled = scaled
            .checked_mul(10)?
            .checked_add((digit - b'0') as i128)?;
        places += 1;
    }
    scaled = scaled.checked_mul(10_i128.checked_pow(DECIMAL_SCALE - places)?)?;
    Some(if negative { -scaled } else { scaled })
}

/// Seconds, milliseconds and microseconds since 1970 overlap as integers and
/// do not overlap as plausible moments. Each unit is only admitted for values
/// that land between 2000 and 2100 in it, so a value is usually admitted by
/// one unit at most, and the ambiguity a model would otherwise have to settle
/// mostly never arises.
fn epoch(literal: &Literal, per_unit: i64) -> Option<i64> {
    let text = match literal {
        Literal::Number(text) => text.as_str(),
        Literal::Text(text) => digits(text)?,
        _ => return None,
    };
    let value = whole(text)?;
    let micros = value.checked_mul(per_unit)?;
    const Y2000: i64 = 946_684_800_000_000;
    const Y2100: i64 = 4_102_444_800_000_000;
    (Y2000..Y2100).contains(&micros).then_some(micros)
}

fn iso(text: &str) -> Option<i64> {
    use chrono::{DateTime, NaiveDate, NaiveDateTime};
    let text = text.trim();
    // A date needs at least year, month and day; anything shorter is a
    // number or a code that happens to parse.
    if text.len() < 10 {
        return None;
    }
    if let Ok(moment) = DateTime::parse_from_rfc3339(text) {
        return Some(moment.timestamp_micros());
    }
    for format in ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S%.f"] {
        if let Ok(moment) = NaiveDateTime::parse_from_str(text, format) {
            return Some(moment.and_utc().timestamp_micros());
        }
    }
    if text.len() == 10
        && let Ok(date) = NaiveDate::parse_from_str(text, "%Y-%m-%d")
    {
        return Some(date.and_hms_opt(0, 0, 0)?.and_utc().timestamp_micros());
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lit(json: &str) -> Literal {
        let raw: Box<RawValue> = serde_json::from_str(json).unwrap();
        Literal::read(&raw)
    }

    #[test]
    fn a_big_integer_keeps_every_digit() {
        // 2^53 + 1: the first integer a float cannot hold.
        let value = lit("9007199254740993");
        assert_eq!(value, Literal::Number("9007199254740993".into()));
        assert_eq!(
            convert(Kind::Int64, &value),
            Some(Value::Int(9_007_199_254_740_993))
        );
        assert_eq!(
            convert(Kind::String, &value),
            Some(Value::Text("9007199254740993".into()))
        );
    }

    #[test]
    fn a_zip_code_is_never_offered_as_a_number() {
        let zip = lit(r#""02134""#);
        let offered = candidates([&zip]);
        assert!(!offered.contains(&Kind::Int64), "{offered:?}");
        assert!(!offered.contains(&Kind::Float64), "{offered:?}");
        assert!(!offered.contains(&Kind::Decimal), "{offered:?}");
        assert_eq!(offered, vec![Kind::String]);
    }

    #[test]
    fn money_converts_to_decimal_exactly() {
        let price = lit(r#""12.50""#);
        assert_eq!(
            convert(Kind::Decimal, &price),
            Some(Value::Decimal(12_500_000_000))
        );
        assert!(candidates([&price]).contains(&Kind::Decimal));
        // Too many places to hold exactly: refused, not rounded.
        assert_eq!(convert(Kind::Decimal, &lit("0.1234567891")), None);
        // An exponent is how a float gets written; refused.
        assert_eq!(convert(Kind::Decimal, &lit("1e3")), None);
    }

    #[test]
    fn an_epoch_is_admitted_by_the_one_unit_that_makes_it_plausible() {
        let seconds = lit("1700000000");
        let millis = lit("1700000000000");
        let offered = candidates([&seconds]);
        assert!(offered.contains(&Kind::TimestampSeconds));
        assert!(!offered.contains(&Kind::TimestampMillis));
        let offered = candidates([&millis]);
        assert!(offered.contains(&Kind::TimestampMillis));
        assert!(!offered.contains(&Kind::TimestampSeconds));
        assert_eq!(
            convert(Kind::TimestampSeconds, &seconds),
            convert(Kind::TimestampMillis, &millis)
        );
        // A small count is not a moment in any unit.
        assert!(
            !candidates([&lit("42")])
                .iter()
                .any(|k| k.name().starts_with("timestamp"))
        );
    }

    #[test]
    fn text_dates_are_timestamps_and_short_codes_are_not() {
        assert!(candidates([&lit(r#""2026-09-23T12:00:00Z""#)]).contains(&Kind::TimestampIso));
        assert!(candidates([&lit(r#""2026-09-23""#)]).contains(&Kind::TimestampIso));
        assert!(!candidates([&lit(r#""2026""#)]).contains(&Kind::TimestampIso));
    }

    #[test]
    fn one_value_that_does_not_fit_removes_the_kind() {
        let offered = candidates([&lit("3"), &lit("4.5")]);
        assert!(!offered.contains(&Kind::Int64), "{offered:?}");
        assert!(offered.contains(&Kind::Decimal), "{offered:?}");
        let offered = candidates([&lit("3"), &lit(r#""N/A""#)]);
        assert_eq!(offered, vec![Kind::String]);
    }

    #[test]
    fn nested_values_are_kept_whole() {
        assert_eq!(candidates([&lit(r#"{"a": 1}"#)]), vec![Kind::Json]);
        assert_eq!(candidates([&lit("[1, 2]")]), vec![Kind::Json]);
        assert_eq!(
            convert(Kind::Json, &lit(r#"{"a": 9007199254740993}"#)),
            Some(Value::Text(r#"{"a": 9007199254740993}"#.into()))
        );
    }

    #[test]
    fn nulls_say_nothing_about_type() {
        assert_eq!(
            candidates([&lit("null"), &lit("true")]),
            vec![Kind::Boolean, Kind::String]
        );
        assert_eq!(candidates([&lit("null")]), vec![Kind::String]);
    }

    #[test]
    fn without_a_judge_the_narrowest_lossless_kind_wins() {
        // Floating point is never chosen unasked: it is the one that rounds.
        assert_eq!(lossless(&candidates([&lit("4.5")])), Kind::Decimal);
        assert_eq!(lossless(&candidates([&lit("7")])), Kind::Int64);
        // A plausible epoch is still kept as the number it is.
        assert_eq!(lossless(&candidates([&lit("1700000000")])), Kind::Int64);
        assert_eq!(lossless(&candidates([&lit(r#""02134""#)])), Kind::String);
    }
}
