//! Five-field cron, enough to say when a worker should run.
//!
//! Minute, hour, day of month, month, day of week. Each field takes `*`,
//! `*/n`, `a-b`, `a-b/n`, a list of those separated by commas, or a number.
//! Day of week is Sunday as both 0 and 7, which is what everybody's crontab
//! already assumes.
//!
//! When both day of month and day of week are restricted, a day matching
//! either one matches, which is the behaviour every cron has had since Vixie
//! and the one every existing expression was written against.

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schedule {
    minute: Field,
    hour: Field,
    day: Field,
    month: Field,
    weekday: Field,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Field {
    /// One bit per value, from `low`.
    allowed: Vec<bool>,
    low: u32,
    /// Whether the field was written as `*`, which decides how day of month
    /// and day of week combine.
    any: bool,
}
impl Field {
    fn parse(spec: &str, low: u32, high: u32, name: &str) -> Result<Self, String> {
        let width = (high - low + 1) as usize;
        let mut allowed = vec![false; width];
        let any = spec.trim() == "*";
        for part in spec.split(',') {
            let part = part.trim();
            if part.is_empty() {
                return Err(format!("{name} has an empty piece"));
            }
            let (range, step) = match part.split_once('/') {
                Some((range, step)) => (
                    range,
                    step.parse::<u32>()
                        .map_err(|_| format!("{name} has a step that is not a number"))?,
                ),
                None => (part, 1),
            };
            if step == 0 {
                return Err(format!("{name} has a step of zero"));
            }
            let (from, to) = if range == "*" {
                (low, high)
            } else if let Some((from, to)) = range.split_once('-') {
                (value(from, name)?, value(to, name)?)
            } else {
                let one = value(range, name)?;
                if step == 1 { (one, one) } else { (one, high) }
            };
            if from < low || to > high || from > to {
                return Err(format!("{name} must be between {low} and {high}"));
            }
            let mut at = from;
            while at <= to {
                allowed[(at - low) as usize] = true;
                at += step;
            }
        }
        if !allowed.iter().any(|on| *on) {
            return Err(format!("{name} matches nothing"));
        }
        Ok(Self { allowed, low, any })
    }
    fn matches(&self, value: u32) -> bool {
        value
            .checked_sub(self.low)
            .and_then(|at| self.allowed.get(at as usize).copied())
            .unwrap_or(false)
    }
}
fn value(text: &str, name: &str) -> Result<u32, String> {
    text.trim()
        .parse()
        .map_err(|_| format!("{name} has a value that is not a number"))
}

impl Schedule {
    pub fn parse(expression: &str) -> Result<Self, String> {
        let fields: Vec<&str> = expression.split_whitespace().collect();
        if fields.len() != 5 {
            return Err(format!(
                "a schedule has five fields, minute hour day month weekday, but {} were given",
                fields.len()
            ));
        }
        Ok(Self {
            minute: Field::parse(fields[0], 0, 59, "minute")?,
            hour: Field::parse(fields[1], 0, 23, "hour")?,
            day: Field::parse(fields[2], 1, 31, "day of month")?,
            month: Field::parse(fields[3], 1, 12, "month")?,
            weekday: Field::parse(&fields[4].replace('7', "0"), 0, 6, "day of week")?,
        })
    }

    /// Whether this minute is one the schedule names.
    fn matches(&self, at: Civil) -> bool {
        if !self.minute.matches(at.minute) || !self.hour.matches(at.hour) {
            return false;
        }
        if !self.month.matches(at.month) {
            return false;
        }
        match (self.day.any, self.weekday.any) {
            // Both restricted: either one matching is enough, which is what
            // every crontab written in the last forty years expects.
            (false, false) => self.day.matches(at.day) || self.weekday.matches(at.weekday),
            _ => self.day.matches(at.day) && self.weekday.matches(at.weekday),
        }
    }

    /// The first second of the next minute this schedule names, strictly
    /// after `after`. Seconds since the epoch, in UTC.
    ///
    /// Searching a minute at a time over four years covers every schedule
    /// that can ever match, and stops rather than looping for one that
    /// cannot, such as the thirtieth of February.
    pub fn next_after(&self, after: i64) -> Option<i64> {
        let mut at = (after / 60 + 1) * 60;
        let limit = at + 4 * 366 * 24 * 60 * 60;
        while at < limit {
            if self.matches(Civil::from_epoch(at)) {
                return Some(at);
            }
            at += 60;
        }
        None
    }
}

/// Enough of a calendar to answer a cron question, in UTC.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Civil {
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    weekday: u32,
}
impl Civil {
    fn from_epoch(seconds: i64) -> Self {
        let days = seconds.div_euclid(86_400);
        let rest = seconds.rem_euclid(86_400);
        // 1970-01-01 was a Thursday.
        let weekday = (days + 4).rem_euclid(7) as u32;
        let (_, month, day) = civil_from_days(days);
        Self {
            month,
            day,
            hour: (rest / 3600) as u32,
            minute: ((rest % 3600) / 60) as u32,
            weekday,
        }
    }
}

/// Howard Hinnant's days-to-civil, which is exact and has no table.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn at(expression: &str, after: i64) -> i64 {
        Schedule::parse(expression)
            .expect("a usable schedule")
            .next_after(after)
            .expect("something matches")
    }

    #[test]
    fn every_minute_is_the_next_minute() {
        // 2026-01-01T00:00:30Z
        let start = 1_767_225_630;
        assert_eq!(at("* * * * *", start), 1_767_225_660);
    }
    #[test]
    fn a_step_lands_on_its_multiples() {
        let start = 1_767_225_600; // 00:00:00
        let five = at("*/5 * * * *", start);
        assert_eq!(five, start + 5 * 60, "00:05");
        assert_eq!(at("*/5 * * * *", five), five + 5 * 60, "and then 00:10");
    }
    #[test]
    fn an_hour_and_minute_pick_one_moment_a_day() {
        let start = 1_767_225_600; // 2026-01-01T00:00:00Z
        let nine_thirty = at("30 9 * * *", start);
        assert_eq!(nine_thirty, start + 9 * 3600 + 30 * 60);
        // The next one is a day later, not an hour.
        assert_eq!(at("30 9 * * *", nine_thirty), nine_thirty + 86_400);
    }
    #[test]
    fn a_weekday_schedule_skips_the_weekend() {
        // 2026-01-02T10:00:00Z was a Friday.
        let friday_ten = 1_767_348_000;
        let next = at("0 10 * * 1-5", friday_ten);
        // Saturday and Sunday are skipped, so the next is Monday.
        assert_eq!(next, 1_767_607_200, "the following Monday at ten");
        assert_eq!((next - friday_ten) / 86_400, 3, "three days later");
    }
    #[test]
    fn sunday_is_both_zero_and_seven() {
        assert_eq!(
            Schedule::parse("0 0 * * 0").unwrap(),
            Schedule::parse("0 0 * * 7").unwrap()
        );
    }
    #[test]
    fn a_list_matches_each_of_its_values() {
        let start = 1_767_225_600;
        let first = at("0,30 * * * *", start);
        assert_eq!(first, start + 30 * 60);
        assert_eq!(at("0,30 * * * *", first), start + 3600);
    }
    #[test]
    fn nonsense_is_refused_with_the_field_that_is_wrong() {
        for (expression, wanted) in [
            ("* * * *", "five fields"),
            ("60 * * * *", "minute"),
            ("* 24 * * *", "hour"),
            ("* * 0 * *", "day of month"),
            ("* * * 13 *", "month"),
            ("*/0 * * * *", "step of zero"),
            ("x * * * *", "minute"),
        ] {
            let error = Schedule::parse(expression).expect_err(expression);
            assert!(error.contains(wanted), "{expression}: {error}");
        }
    }
    #[test]
    fn a_date_that_never_comes_gives_up_rather_than_looping() {
        // The thirtieth of February.
        assert_eq!(
            Schedule::parse("0 0 30 2 *")
                .unwrap()
                .next_after(1_767_225_600),
            None
        );
    }
}
