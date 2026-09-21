//! Turning what a phrase says into values a query can compare against.
//!
//! "thirteen" is thirteen, "last week" is a pair of dates, and "the 3rd" is a
//! day of the month you have to know today to resolve. None of that is a
//! judgement, so none of it is asked: code works it out and the decision
//! service is left the part that is a judgement, which is what column the
//! value belongs to and how it is compared.
//!
//! Today is passed in rather than read from the clock, so the same phrase
//! always resolves the same way in a test.

/// Howard Hinnant's civil-from-days and its inverse. Exact, table-free, and
/// the same pair the cron schedule uses.
pub fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let mp = if month > 2 { month - 3 } else { month + 9 } as i64;
    let doy = (153 * mp + 2) / 5 + day as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}
pub fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if month <= 2 { year + 1 } else { year }, month, day)
}

fn iso(days: i64) -> String {
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}")
}

/// What a value turned out to be.
#[derive(Debug, Clone, PartialEq)]
pub enum Kind {
    /// A quantity, however it was spelled.
    Number(f64),
    /// A stretch of days, half open: on or after the first, before the second.
    /// Every date phrase is one of these, because "last week" is not a day and
    /// neither is "March".
    Span(i64, i64),
    /// A word with no reading but itself: a name, a place, a status.
    Words,
}

/// One value the phrase offered, as typed and as resolved.
#[derive(Debug, Clone, PartialEq)]
pub struct Value {
    /// The span of the phrase this came from, for showing the reader what was
    /// understood.
    pub text: String,
    pub kind: Kind,
}

impl Value {
    /// How this value is written into a statement, and what it is called when
    /// it is offered as an option.
    pub fn label(&self) -> String {
        match &self.kind {
            Kind::Number(n) if n.fract() == 0.0 => format!("{}", *n as i64),
            Kind::Number(n) => format!("{n}"),
            Kind::Span(start, end) if end - start == 1 => iso(*start),
            Kind::Span(start, end) => format!("{} to {}", iso(*start), iso(*end - 1)),
            Kind::Words => self.text.clone(),
        }
    }
}

const ONES: [(&str, i64); 20] = [
    ("zero", 0),
    ("one", 1),
    ("two", 2),
    ("three", 3),
    ("four", 4),
    ("five", 5),
    ("six", 6),
    ("seven", 7),
    ("eight", 8),
    ("nine", 9),
    ("ten", 10),
    ("eleven", 11),
    ("twelve", 12),
    ("thirteen", 13),
    ("fourteen", 14),
    ("fifteen", 15),
    ("sixteen", 16),
    ("seventeen", 17),
    ("eighteen", 18),
    ("nineteen", 19),
];
const TENS: [(&str, i64); 8] = [
    ("twenty", 20),
    ("thirty", 30),
    ("forty", 40),
    ("fifty", 50),
    ("sixty", 60),
    ("seventy", 70),
    ("eighty", 80),
    ("ninety", 90),
];
const MONTHS: [&str; 12] = [
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
];

fn word_number(word: &str) -> Option<i64> {
    let word = word.trim_end_matches("st").trim_end_matches("nd");
    ONES.iter()
        .chain(TENS.iter())
        .find(|(name, _)| *name == word)
        .map(|(_, value)| *value)
}

/// A number however it is spelled: "13", "1,300", "thirteen", "twenty five".
/// Returns the value and how many words it consumed.
fn number_at(words: &[&str]) -> Option<(f64, usize)> {
    let first = words.first()?;
    let digits = first.replace([',', '$'], "");
    if let Ok(value) = digits.parse::<f64>() {
        return Some((value, 1));
    }
    // "3rd", "21st"
    let ordinal = first.trim_end_matches(|c: char| c.is_ascii_alphabetic());
    if ordinal.len() < first.len()
        && let Ok(value) = ordinal.parse::<f64>()
    {
        return Some((value, 1));
    }
    let lower = first.to_ascii_lowercase();
    let tens = TENS.iter().find(|(name, _)| *name == lower);
    if let Some((_, tens)) = tens {
        // "twenty five" is one number in two words.
        if let Some(next) = words.get(1)
            && let Some(unit) = word_number(&next.to_ascii_lowercase())
            && (1..10).contains(&unit)
        {
            return Some(((tens + unit) as f64, 2));
        }
        return Some((*tens as f64, 1));
    }
    word_number(&lower).map(|value| (value as f64, 1))
}

fn month_start(year: i64, month: u32) -> i64 {
    days_from_civil(year, month, 1)
}
fn month_after(year: i64, month: u32) -> i64 {
    if month == 12 {
        days_from_civil(year + 1, 1, 1)
    } else {
        days_from_civil(year, month + 1, 1)
    }
}

/// Every date phrase this understands, resolved against `today`. Returns the
/// span and how many words it used.
fn date_at(words: &[&str], today: i64) -> Option<(i64, i64, usize)> {
    let lower: Vec<String> = words.iter().map(|w| w.to_ascii_lowercase()).collect();
    let at = |i: usize| lower.get(i).map(String::as_str).unwrap_or("");
    let (year, month, _) = civil_from_days(today);

    match at(0) {
        "today" => return Some((today, today + 1, 1)),
        "yesterday" => return Some((today - 1, today, 1)),
        "tomorrow" => return Some((today + 1, today + 2, 1)),
        _ => {}
    }
    // "last week", "this month", "last year"
    if matches!(at(0), "last" | "this" | "past") {
        let back = at(0) != "this";
        match at(1) {
            "week" => {
                let start = today - 7 * i64::from(back);
                return Some((start, start + 7, 2));
            }
            "month" => {
                let (y, m) = if back && month == 1 {
                    (year - 1, 12)
                } else if back {
                    (year, month - 1)
                } else {
                    (year, month)
                };
                return Some((month_start(y, m), month_after(y, m), 2));
            }
            "year" => {
                let y = year - i64::from(back);
                return Some((days_from_civil(y, 1, 1), days_from_civil(y + 1, 1, 1), 2));
            }
            _ => {}
        }
        // "last 7 days", "past three weeks"
        if let Some((count, used)) = number_at(&words[1..]) {
            let unit = at(1 + used);
            let days = match unit.trim_end_matches('s') {
                "day" => 1,
                "week" => 7,
                "month" => 30,
                "year" => 365,
                _ => return None,
            };
            let span = (count as i64).max(1) * days;
            return Some((today - span, today + 1, 2 + used));
        }
    }
    // "on the 3rd", "the 3rd"
    if at(0) == "on"
        && at(1) == "the"
        && let Some((day, used)) = number_at(&words[2..])
        && (1..=31).contains(&(day as i64))
    {
        let start = days_from_civil(year, month, day as u32);
        return Some((start, start + 1, 2 + used));
    }
    // "in march", "march 2025"
    if let Some(index) = MONTHS.iter().position(|m| *m == at(0)) {
        let m = index as u32 + 1;
        let y = match number_at(&words[1..]) {
            Some((value, _)) if (1900.0..2200.0).contains(&value) => value as i64,
            _ => year,
        };
        let used = if y == year { 1 } else { 2 };
        return Some((month_start(y, m), month_after(y, m), used));
    }
    None
}

/// Every value the phrase offers, resolved. `today` is days since the epoch.
///
/// `known` names the catalog's own words, which are identifiers rather than
/// values, and `skip` the grammar words that carry no value at all.
pub fn extract(phrase: &str, today: i64, known: &[String], skip: &[&str]) -> Vec<Value> {
    // Split on spaces only. A comma inside a number belongs to it, and one
    // after a word is trimmed where the word is read.
    let words: Vec<&str> = phrase
        .split(|c: char| c.is_whitespace() || c == '?')
        .map(|w| w.trim_end_matches([',', '.', ';']))
        .filter(|w| !w.is_empty())
        .collect();
    let mut out: Vec<Value> = Vec::new();
    let mut index = 0;
    while index < words.len() {
        let rest = &words[index..];
        if let Some((start, end, used)) = date_at(rest, today) {
            out.push(Value {
                text: rest[..used].join(" "),
                kind: Kind::Span(start, end),
            });
            index += used;
            continue;
        }
        // A month or a weekday on its own is a date, so a number that follows
        // one is part of it and never a quantity of its own.
        if let Some((value, used)) = number_at(rest) {
            out.push(Value {
                text: rest[..used].join(" "),
                kind: Kind::Number(value),
            });
            index += used;
            continue;
        }
        let word = rest[0].trim_matches(|c: char| !c.is_alphanumeric());
        let lower = word.to_ascii_lowercase();
        let carries_meaning = word.len() > 1
            && !skip.contains(&lower.as_str())
            && !known.iter().any(|k| k.eq_ignore_ascii_case(word));
        if carries_meaning {
            out.push(Value {
                text: word.to_owned(),
                kind: Kind::Words,
            });
        }
        index += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 2026-09-18.
    fn today() -> i64 {
        days_from_civil(2026, 9, 18)
    }
    fn kinds(phrase: &str) -> Vec<(String, Kind)> {
        extract(
            phrase,
            today(),
            &[],
            &["orders", "the", "in", "with", "over"],
        )
        .into_iter()
        .map(|v| (v.label(), v.kind))
        .collect()
    }

    #[test]
    fn the_two_date_conversions_are_inverses() {
        for days in [-100_000, -1, 0, 1, 19_000, 100_000] {
            let (y, m, d) = civil_from_days(days);
            assert_eq!(days_from_civil(y, m, d), days, "{y}-{m}-{d}");
        }
    }

    /// A quantity is a quantity however it is spelled.
    #[test]
    fn numbers_are_read_however_they_are_written() {
        assert_eq!(kinds("13")[0].1, Kind::Number(13.0));
        assert_eq!(kinds("thirteen")[0].1, Kind::Number(13.0));
        assert_eq!(kinds("twenty five")[0].1, Kind::Number(25.0));
        assert_eq!(kinds("1,300")[0].1, Kind::Number(1300.0));
        assert_eq!(kinds("$45.50")[0].1, Kind::Number(45.5));
    }

    /// Every date phrase is a stretch of days, because "last week" is not a
    /// day and a query that treats it as one is wrong.
    #[test]
    fn date_phrases_become_spans() {
        assert_eq!(kinds("today")[0].0, "2026-09-18");
        assert_eq!(kinds("yesterday")[0].0, "2026-09-17");
        assert_eq!(kinds("last week")[0].0, "2026-09-11 to 2026-09-17");
        assert_eq!(kinds("last month")[0].0, "2026-08-01 to 2026-08-31");
        assert_eq!(kinds("this year")[0].0, "2026-01-01 to 2026-12-31");
        assert_eq!(kinds("last 7 days")[0].0, "2026-09-11 to 2026-09-18");
        assert_eq!(kinds("on the 3rd")[0].0, "2026-09-03");
        assert_eq!(kinds("march")[0].0, "2026-03-01 to 2026-03-31");
    }

    /// A date phrase is one value, not the words it is made of. "last 7 days"
    /// must not also offer 7.
    #[test]
    fn a_date_phrase_swallows_its_own_number() {
        let read = kinds("orders in the last 7 days");
        assert_eq!(read.len(), 1, "{read:?}");
        assert_eq!(read[0].0, "2026-09-11 to 2026-09-18");
    }

    /// A word that is neither a quantity nor a date is offered as itself, and
    /// the catalog's own words are not offered at all.
    #[test]
    fn plain_words_are_offered_and_catalog_names_are_not() {
        let found = extract(
            "acme orders in seattle",
            today(),
            &["orders".to_owned()],
            &["in"],
        );
        let labels: Vec<String> = found.iter().map(Value::label).collect();
        assert_eq!(labels, ["acme", "seattle"]);
    }
}
