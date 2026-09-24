//! Durable alarms: when scheduled work runs, decided without I/O.
//!
//! An alarm belongs to an ownership key - a table, or a view with no source -
//! and lives in that key's ownership record in the bucket, so whoever owns the
//! key owns its alarms and a new owner finds them in the record it claims.
//! Only the owner changes the record, and every change is a compare-and-swap
//! on it, so two processes can never both begin the same firing.
//!
//! A firing has two steps. It begins by recording the attempt and moving the
//! alarm to the time it should be retried if nothing reports back; then the
//! handler runs; then it completes, which clears a one-shot alarm or moves a
//! schedule to its next occurrence. A process that dies between the two
//! leaves the alarm due at its retry time, and the next owner fires it again,
//! with the attempt count saying so. Delivery is therefore at least once.
//!
//! The retry policy and the schedule arithmetic follow celld's
//! (`crates/logic/alarm.rs` and `crates/logic/cron.rs` in denoland/celld):
//! backoff doubles from two seconds and stops doubling at the seventh try, a
//! bounded number of failures abandons one occurrence, and a retry never
//! delays the next occurrence of a schedule.
use serde::{Deserialize, Serialize};

/// The first retry waits this long; each later one twice the one before.
pub const BACKOFF_BASE_MS: u64 = 2_000;
/// The backoff stops doubling after this many doublings (2 s << 6 = 128 s).
const MAX_BACKOFF_SHIFT: u32 = 6;
/// Failed attempts at one occurrence before it is given up.
pub const MAX_ATTEMPTS: u32 = 7;
/// How many missed occurrences are walked before the walk gives up and the
/// schedule restarts from now. A year of a minutely cron is half of this.
const MAX_WALK: u64 = 1_000_000;

/// How a scheduled alarm comes round again.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Repeat {
    /// Five cron fields, UTC, minute resolution.
    Cron(String),
    /// Every this many seconds after the previous occurrence.
    Every(u64),
}

impl Repeat {
    /// The first occurrence strictly after `after_ms`.
    pub fn next_after(&self, after_ms: u64) -> Option<u64> {
        match self {
            Self::Cron(expression) => {
                let schedule = crate::cron::Schedule::parse(expression).ok()?;
                let after = (after_ms / 1000) as i64;
                schedule
                    .next_after(after)
                    .map(|secs| (secs.max(0) as u64) * 1000)
            }
            Self::Every(seconds) => after_ms.checked_add(seconds.max(&1) * 1000),
        }
    }

    /// The latest occurrence at or before `now_ms`, walking from the
    /// occurrence `from_ms`, and how many occurrences that passes over.
    /// A schedule that fell behind runs once, for the most recent time it
    /// missed, rather than once for every time it missed.
    pub fn catch_up(&self, from_ms: u64, now_ms: u64) -> (u64, u64) {
        if now_ms <= from_ms {
            return (from_ms, 0);
        }
        if let Self::Every(seconds) = self {
            let every = seconds.max(&1) * 1000;
            let steps = (now_ms - from_ms) / every;
            return (from_ms + steps * every, steps);
        }
        let mut at = from_ms;
        let mut skipped = 0;
        while skipped < MAX_WALK {
            match self.next_after(at) {
                Some(next) if next <= now_ms => {
                    at = next;
                    skipped += 1;
                }
                _ => return (at, skipped),
            }
        }
        (at, skipped)
    }
}

/// One alarm, as stored in its key's ownership record.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Alarm {
    /// When it is next due, in milliseconds since the Unix epoch. While a
    /// firing is in flight this is when to try again if it never reports.
    pub at_ms: u64,
    /// The occurrence the next firing stands for.
    pub scheduled_ms: u64,
    /// Firings begun for this occurrence.
    #[serde(default)]
    pub attempt: u32,
    /// Changes whenever the alarm is set, so a firing never completes an alarm
    /// that was replaced while its handler ran.
    #[serde(default)]
    pub id: u64,
    /// Set for a schedule, which re-arms itself; absent for a one-shot alarm.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repeat: Option<Repeat>,
    /// For a schedule whose firing is in flight or being retried, the first
    /// occurrence that firing stands for. An occurrence given up hands this
    /// range on to the next run rather than dropping it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub covers_from_ms: Option<u64>,
}

impl Alarm {
    /// A one-shot alarm at `at_ms`.
    pub fn once(at_ms: u64, id: u64) -> Self {
        Self {
            at_ms,
            scheduled_ms: at_ms,
            attempt: 0,
            id,
            repeat: None,
            covers_from_ms: None,
        }
    }

    /// A schedule whose first occurrence is `first_ms`.
    pub fn repeating(repeat: Repeat, first_ms: u64, id: u64) -> Self {
        Self {
            at_ms: first_ms,
            scheduled_ms: first_ms,
            attempt: 0,
            id,
            repeat: Some(repeat),
            covers_from_ms: None,
        }
    }
}

/// How long to wait after the `attempt`-th failure.
pub fn backoff_ms(attempt: u32) -> u64 {
    BACKOFF_BASE_MS << attempt.saturating_sub(1).min(MAX_BACKOFF_SHIFT)
}

/// What a firing is for, handed to the handler.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Firing {
    pub id: u64,
    /// The occurrence this run stands for.
    pub scheduled_ms: u64,
    /// 1 for the first try at this occurrence.
    pub attempt: u32,
    /// Occurrences a schedule missed that this one run also stands for.
    pub missed: u64,
}

/// Begin a firing of a due alarm at `now_ms`: record the attempt, and move the
/// alarm to its retry time in case this firing never reports back. `None`
/// when the alarm is not due.
pub fn begin(alarm: &mut Alarm, now_ms: u64) -> Option<Firing> {
    if alarm.at_ms > now_ms {
        return None;
    }
    let mut missed = 0;
    if let Some(repeat) = &alarm.repeat {
        let from = alarm.covers_from_ms.unwrap_or(alarm.scheduled_ms);
        if alarm.attempt == 0 {
            let (latest, skipped) = repeat.catch_up(alarm.scheduled_ms, now_ms);
            alarm.scheduled_ms = latest;
            alarm.covers_from_ms = Some(alarm.covers_from_ms.unwrap_or(from).min(from));
            missed = skipped;
        } else if let Some(from) = alarm.covers_from_ms {
            // A retry stands for the same occurrences as the first try.
            missed = repeat.catch_up(from, alarm.scheduled_ms).1;
        }
    }
    alarm.attempt += 1;
    alarm.at_ms = now_ms.saturating_add(backoff_ms(alarm.attempt));
    Some(Firing {
        id: alarm.id,
        scheduled_ms: alarm.scheduled_ms,
        attempt: alarm.attempt,
        missed,
    })
}

/// What becomes of an alarm once its firing reports.
#[derive(Debug, PartialEq, Eq)]
pub enum Settled {
    /// Leave it: it was replaced meanwhile, or it waits for its retry.
    Keep,
    /// Remove it: a one-shot that ran, or one given up.
    Remove,
}

/// Complete a firing. `ok` is whether the handler succeeded. `alarm` is the
/// alarm as it is now, which the handler may have replaced.
pub fn complete(alarm: &mut Alarm, firing: &Firing, ok: bool, now_ms: u64) -> Settled {
    if alarm.id != firing.id
        || alarm.attempt != firing.attempt
        || alarm.scheduled_ms != firing.scheduled_ms
    {
        return Settled::Keep;
    }
    let next = alarm
        .repeat
        .as_ref()
        .and_then(|repeat| repeat.next_after(firing.scheduled_ms));
    let move_on = |alarm: &mut Alarm| match next {
        Some(next) => {
            alarm.at_ms = next;
            alarm.scheduled_ms = next;
            alarm.attempt = 0;
            alarm.covers_from_ms = None;
            Settled::Keep
        }
        None => Settled::Remove,
    };
    if ok {
        return move_on(alarm);
    }
    // An occurrence that is given up is not dropped silently: the alarm moves
    // to the next occurrence but keeps pointing at the one that failed, so the
    // next run catches up over it and reports it among those it covers.
    let hand_over = |alarm: &mut Alarm| match next {
        Some(next) => {
            alarm.at_ms = next.max(now_ms.min(alarm.at_ms));
            alarm.scheduled_ms = alarm.covers_from_ms.take().unwrap_or(alarm.scheduled_ms);
            alarm.attempt = 0;
            Settled::Keep
        }
        None => Settled::Remove,
    };
    if firing.attempt >= MAX_ATTEMPTS {
        return hand_over(alarm);
    }
    // A retry never holds up the next occurrence: whichever is sooner wins.
    match next {
        Some(next) if next <= alarm.at_ms => hand_over(alarm),
        _ => Settled::Keep,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Occurrences a failed run stood for are not lost when a second run fails
    /// too: the run that finally succeeds stands for all of them.
    #[test]
    fn consecutive_given_up_occurrences_all_reach_the_next_run() {
        let mut alarm = Alarm::repeating(Repeat::Every(1), 0, 1);
        // Down until 2.5 s: 0, 1 and 2 are due, and the run for 2 fails.
        let first = begin(&mut alarm, 2_500).unwrap();
        assert_eq!((first.scheduled_ms, first.missed), (2_000, 2));
        complete(&mut alarm, &first, false, 2_600);
        // Its retry would wait 2 s; the occurrence at 3 s wins, and fails too.
        let second = begin(&mut alarm, 3_000).unwrap();
        assert_eq!((second.scheduled_ms, second.missed), (3_000, 3));
        complete(&mut alarm, &second, false, 3_100);
        let third = begin(&mut alarm, 4_000).unwrap();
        assert_eq!(
            (third.scheduled_ms, third.missed),
            (4_000, 4),
            "0, 1, 2 and 3 are all covered by the run for 4"
        );
        complete(&mut alarm, &third, true, 4_100);
        assert_eq!((alarm.scheduled_ms, alarm.covers_from_ms), (5_000, None));
    }

    #[test]
    fn backoff_doubles_and_then_holds() {
        let waits: Vec<u64> = (1..=9).map(backoff_ms).collect();
        assert_eq!(
            waits,
            [
                2_000, 4_000, 8_000, 16_000, 32_000, 64_000, 128_000, 128_000, 128_000
            ]
        );
    }

    #[test]
    fn a_one_shot_fires_once_and_is_removed() {
        let mut alarm = Alarm::once(1_000, 7);
        assert_eq!(begin(&mut alarm, 999), None, "not due yet");
        let firing = begin(&mut alarm, 5_000).expect("due, and past due fires");
        assert_eq!(
            firing,
            Firing {
                id: 7,
                scheduled_ms: 1_000,
                attempt: 1,
                missed: 0
            }
        );
        assert_eq!(alarm.at_ms, 7_000, "retry time while in flight");
        assert_eq!(begin(&mut alarm, 5_001), None, "in flight is not due");
        assert_eq!(complete(&mut alarm, &firing, true, 5_100), Settled::Remove);
    }

    #[test]
    fn a_failure_retries_with_backoff_until_it_gives_up() {
        let mut alarm = Alarm::once(0, 1);
        let mut now = 0;
        for attempt in 1..=MAX_ATTEMPTS {
            let firing = begin(&mut alarm, now).expect("due");
            assert_eq!(firing.attempt, attempt);
            let settled = complete(&mut alarm, &firing, false, now);
            if attempt < MAX_ATTEMPTS {
                assert_eq!(settled, Settled::Keep);
                assert_eq!(alarm.at_ms, now + backoff_ms(attempt));
                now = alarm.at_ms;
            } else {
                assert_eq!(settled, Settled::Remove, "abandoned after the last attempt");
            }
        }
    }

    #[test]
    fn a_replaced_alarm_is_not_completed_by_the_old_firing() {
        let mut alarm = Alarm::once(0, 1);
        let firing = begin(&mut alarm, 10).unwrap();
        alarm = Alarm::once(50_000, 2);
        assert_eq!(complete(&mut alarm, &firing, true, 20), Settled::Keep);
        assert_eq!(alarm.at_ms, 50_000);
    }

    #[test]
    fn missed_occurrences_coalesce_into_one_run_for_the_latest() {
        let mut alarm = Alarm::repeating(Repeat::Every(10), 100_000, 1);
        // Down from before 100 s until 135 s: 100, 110, 120 and 130 all passed.
        let firing = begin(&mut alarm, 135_000).unwrap();
        assert_eq!(
            firing.scheduled_ms, 130_000,
            "the run stands for the latest"
        );
        assert_eq!(firing.missed, 3, "and for the three before it");
        assert_eq!(complete(&mut alarm, &firing, true, 135_500), Settled::Keep);
        assert_eq!(alarm.at_ms, 140_000, "the next occurrence, on the grid");
        assert_eq!(alarm.attempt, 0);
    }

    #[test]
    fn a_cron_catches_up_to_the_minute_it_last_missed() {
        // 2026-09-23 00:00 UTC and every quarter hour.
        let midnight = 1_790_121_600_000;
        let repeat = Repeat::Cron("*/15 * * * *".into());
        let mut alarm = Alarm::repeating(repeat, midnight, 1);
        let firing = begin(&mut alarm, midnight + 50 * 60_000).unwrap();
        assert_eq!(firing.scheduled_ms, midnight + 45 * 60_000);
        assert_eq!(firing.missed, 3);
        complete(&mut alarm, &firing, true, midnight + 50 * 60_000);
        assert_eq!(alarm.at_ms, midnight + 60 * 60_000);
    }

    #[test]
    fn a_failing_schedule_retries_without_holding_up_its_next_occurrence() {
        let mut alarm = Alarm::repeating(Repeat::Every(3), 0, 1);
        let firing = begin(&mut alarm, 0).unwrap();
        // The retry would be at 2 s, before the next occurrence at 3 s.
        assert_eq!(complete(&mut alarm, &firing, false, 0), Settled::Keep);
        assert_eq!((alarm.at_ms, alarm.scheduled_ms), (2_000, 0));
        let retry = begin(&mut alarm, 2_000).unwrap();
        assert_eq!((retry.attempt, retry.scheduled_ms), (2, 0));
        // The next retry would be at 6 s; the occurrence at 3 s comes first,
        // and its run stands for the one that failed as well.
        assert_eq!(complete(&mut alarm, &retry, false, 2_000), Settled::Keep);
        let next = {
            let mut probe = alarm.clone();
            begin(&mut probe, 3_000).unwrap()
        };
        assert_eq!((next.scheduled_ms, next.missed), (3_000, 1));
        assert_eq!(
            (alarm.at_ms, alarm.scheduled_ms, alarm.attempt),
            (3_000, 0, 0)
        );
    }
}
