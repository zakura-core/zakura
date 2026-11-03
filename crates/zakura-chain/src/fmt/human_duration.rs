//! Human-readable durations, such as `1h 30m` or `250ms`.
//!
//! This is a port of the duration parser and formatter from
//! [`humantime` 2.4.0](https://github.com/chronotope/humantime), which is also licensed
//! `MIT OR Apache-2.0`. Config files store durations in this format, so parsing and formatting
//! stay compatible with that version, except that week, month, and year units aren't supported.
//! Durations of a month or more are written in days instead.

use std::{fmt, str::Chars, time::Duration};

#[cfg(test)]
mod tests;

const SECONDS_PER_DAY: u64 = 86_400;
const NANOS_PER_SECOND: u64 = 1_000_000_000;

/// Why [`parse_duration`] rejected a string.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ParseDurationError {
    /// A character that can't appear in a duration, at this byte offset.
    InvalidCharacter(usize),
    /// A number was expected at this byte offset.
    NumberExpected(usize),
    /// A missing or unknown unit.
    UnknownUnit,
    /// A value that overflows, or a fraction finer than a nanosecond.
    NumberOverflow,
    /// The string only contained whitespace.
    Empty,
}

/// Parses a duration such as `15s`, `1h 30m`, `2.5days`, or `0`.
///
/// Each number needs a unit, except a bare `0`. Supported units are:
/// `nanos`/`nsec`/`ns`, `usec`/`us`/`µs`, `millis`/`msec`/`ms`,
/// `seconds`/`second`/`secs`/`sec`/`s`, `minutes`/`minute`/`mins`/`min`/`m`,
/// `hours`/`hour`/`hrs`/`hr`/`h`, and `days`/`day`/`d`.
pub(crate) fn parse_duration(s: &str) -> Result<Duration, ParseDurationError> {
    if s == "0" {
        return Ok(Duration::ZERO);
    }

    Parser {
        iter: s.chars(),
        src: s,
    }
    .parse()
}

/// Formats `duration` with the largest units first, such as `1day 2h 3m 4s 5ms`.
///
/// Days are the largest unit, so a year is written as `365days 6h`.
///
/// [`parse_duration`] parses the result back to the same duration.
pub(crate) fn format_duration(duration: Duration) -> FormattedDuration {
    FormattedDuration(duration)
}

/// A [`Duration`] that displays as a human-readable string.
#[derive(Clone, Copy, Debug)]
pub(crate) struct FormattedDuration(Duration);

impl fmt::Display for FormattedDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let secs = self.0.as_secs();
        let nanos = self.0.subsec_nanos();

        if secs == 0 && nanos == 0 {
            return f.write_str("0s");
        }

        let days = secs / SECONDS_PER_DAY;
        let day_secs = secs % SECONDS_PER_DAY;

        let mut started = false;
        write_plural_unit(f, &mut started, days, "day")?;
        write_unit(f, &mut started, day_secs / 3600, "h")?;
        write_unit(f, &mut started, day_secs % 3600 / 60, "m")?;
        write_unit(f, &mut started, day_secs % 60, "s")?;
        write_unit(f, &mut started, u64::from(nanos / 1_000_000), "ms")?;
        write_unit(f, &mut started, u64::from(nanos / 1000 % 1000), "us")?;
        write_unit(f, &mut started, u64::from(nanos % 1000), "ns")
    }
}

/// Writes a non-zero `value` with `unit`, separated from any earlier unit by a space.
fn write_unit(
    f: &mut fmt::Formatter<'_>,
    started: &mut bool,
    value: u64,
    unit: &str,
) -> fmt::Result {
    if value == 0 {
        return Ok(());
    }
    if *started {
        f.write_str(" ")?;
    }
    *started = true;

    write!(f, "{value}{unit}")
}

/// Like [`write_unit`], but adds an `s` to `unit` when `value` is more than one.
fn write_plural_unit(
    f: &mut fmt::Formatter<'_>,
    started: &mut bool,
    value: u64,
    unit: &str,
) -> fmt::Result {
    write_unit(f, started, value, unit)?;
    if value > 1 {
        f.write_str("s")?;
    }

    Ok(())
}

/// The digits after a decimal point, as `numerator / denominator`.
#[derive(Clone, Copy, Debug)]
struct Fraction {
    numerator: u64,
    denominator: u64,
}

#[derive(Clone, Copy, Debug)]
enum Unit {
    Nanosecond,
    Microsecond,
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
}

impl Unit {
    fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "nanos" | "nsec" | "ns" => Self::Nanosecond,
            "usec" | "us" | "µs" => Self::Microsecond,
            "millis" | "msec" | "ms" => Self::Millisecond,
            "seconds" | "second" | "secs" | "sec" | "s" => Self::Second,
            "minutes" | "minute" | "min" | "mins" | "m" => Self::Minute,
            "hours" | "hour" | "hr" | "hrs" | "h" => Self::Hour,
            "days" | "day" | "d" => Self::Day,
            _ => return None,
        })
    }

    /// Returns `value` of this unit as whole seconds and extra nanoseconds.
    fn whole(self, value: u64) -> Result<(u64, u64), ParseDurationError> {
        Ok(match self {
            Self::Nanosecond => (0, value),
            Self::Microsecond => (0, checked_mul(value, 1000)?),
            Self::Millisecond => (0, checked_mul(value, 1_000_000)?),
            Self::Second => (value, 0),
            Self::Minute => (checked_mul(value, 60)?, 0),
            Self::Hour => (checked_mul(value, 3600)?, 0),
            Self::Day => (checked_mul(value, SECONDS_PER_DAY)?, 0),
        })
    }

    /// Returns `fraction` of this unit as whole seconds and extra nanoseconds.
    ///
    /// Units of an hour or more keep whole seconds, and smaller units keep whole nanoseconds.
    /// A fraction that doesn't divide exactly into those is rejected.
    fn fraction(self, fraction: Fraction) -> Result<(u64, u64), ParseDurationError> {
        let Fraction {
            numerator,
            denominator,
        } = fraction;
        let exact = |scale| exact_div(checked_mul(numerator, scale)?, denominator);

        Ok(match self {
            Self::Nanosecond => return Err(ParseDurationError::NumberOverflow),
            Self::Microsecond => (0, exact(1000)?),
            Self::Millisecond => (0, exact(1_000_000)?),
            Self::Second => (0, exact(NANOS_PER_SECOND)?),
            Self::Minute => (0, exact(60 * NANOS_PER_SECOND)?),
            Self::Hour => (exact(3600)?, 0),
            Self::Day => (exact(SECONDS_PER_DAY)?, 0),
        })
    }
}

fn checked_mul(a: u64, b: u64) -> Result<u64, ParseDurationError> {
    a.checked_mul(b).ok_or(ParseDurationError::NumberOverflow)
}

fn checked_add(a: u64, b: u64) -> Result<u64, ParseDurationError> {
    a.checked_add(b).ok_or(ParseDurationError::NumberOverflow)
}

/// Divides `a` by `b`, rejecting a remainder as a loss of precision.
fn exact_div(a: u64, b: u64) -> Result<u64, ParseDurationError> {
    if a.is_multiple_of(b) {
        Ok(a / b)
    } else {
        Err(ParseDurationError::NumberOverflow)
    }
}

/// Returns the value of an ASCII decimal digit.
fn digit_value(c: char) -> u64 {
    u64::from(c) - u64::from('0')
}

fn is_unit_char(c: char) -> bool {
    c.is_ascii_alphabetic() || c == 'µ'
}

/// Adds `secs` and `nanos` to `out`.
fn add_to(out: &mut Duration, secs: u64, nanos: u64) -> Result<(), ParseDurationError> {
    *out = Duration::from_secs(secs)
        .checked_add(Duration::from_nanos(nanos))
        .and_then(|added| out.checked_add(added))
        .ok_or(ParseDurationError::NumberOverflow)?;

    Ok(())
}

struct Parser<'a> {
    iter: Chars<'a>,
    src: &'a str,
}

impl Parser<'_> {
    fn parse(mut self) -> Result<Duration, ParseDurationError> {
        let mut value = self.parse_first_digit()?.ok_or(ParseDurationError::Empty)?;
        let mut out = Duration::ZERO;

        'values: loop {
            // The rest of the number. Whitespace between digits is ignored.
            let mut fraction = None;
            let mut offset = self.offset();
            while let Some(c) = self.iter.next() {
                match c {
                    '0'..='9' => {
                        value = checked_add(checked_mul(value, 10)?, digit_value(c))?;
                    }
                    c if c.is_whitespace() => {}
                    c if is_unit_char(c) => break,
                    '.' => {
                        fraction = Some(self.parse_fraction(&mut offset)?);
                        break;
                    }
                    _ => return Err(ParseDurationError::InvalidCharacter(offset)),
                }
                offset = self.offset();
            }

            // The unit, which ends at whitespace or the next number.
            let unit_start = offset;
            let mut unit_end = self.offset();
            while let Some(c) = self.iter.next() {
                match c {
                    '0'..='9' => {
                        self.add_value(value, fraction, unit_start, unit_end, &mut out)?;
                        value = digit_value(c);
                        continue 'values;
                    }
                    c if c.is_whitespace() => break,
                    c if is_unit_char(c) => {}
                    _ => return Err(ParseDurationError::InvalidCharacter(unit_end)),
                }
                unit_end = self.offset();
            }

            self.add_value(value, fraction, unit_start, unit_end, &mut out)?;
            value = match self.parse_first_digit()? {
                Some(value) => value,
                None => return Ok(out),
            };
        }
    }

    /// Skips whitespace, then reads the first digit of a number, or returns `None` at the end.
    fn parse_first_digit(&mut self) -> Result<Option<u64>, ParseDurationError> {
        let offset = self.offset();
        for c in self.iter.by_ref() {
            match c {
                '0'..='9' => return Ok(Some(digit_value(c))),
                c if c.is_whitespace() => {}
                _ => return Err(ParseDurationError::NumberExpected(offset)),
            }
        }

        Ok(None)
    }

    /// Reads the digits after a decimal point, updating `offset` as it goes.
    fn parse_fraction(&mut self, offset: &mut usize) -> Result<Fraction, ParseDurationError> {
        let mut numerator = 0_u64;
        let mut denominator = 1_u64;
        let mut leading_zeros = true;

        while let Some(c) = self.iter.next() {
            match c {
                '0' => {
                    denominator = checked_mul(denominator, 10)?;
                    if !leading_zeros {
                        numerator = checked_mul(numerator, 10)?;
                    }
                }
                '1'..='9' => {
                    leading_zeros = false;
                    denominator = checked_mul(denominator, 10)?;
                    numerator = checked_add(checked_mul(numerator, 10)?, digit_value(c))?;
                }
                c if c.is_whitespace() => {}
                c if is_unit_char(c) => break,
                _ => return Err(ParseDurationError::InvalidCharacter(*offset)),
            }
            *offset = self.offset();
        }

        if denominator == 1 {
            // A decimal point with no digits after it, such as `1.s`.
            return Err(ParseDurationError::InvalidCharacter(*offset));
        }

        Ok(Fraction {
            numerator,
            denominator,
        })
    }

    /// The byte offset of the next character.
    fn offset(&self) -> usize {
        self.src.len() - self.iter.as_str().len()
    }

    /// Adds `value` and `fraction` of the unit at `src[unit_start..unit_end]` to `out`.
    fn add_value(
        &self,
        value: u64,
        fraction: Option<Fraction>,
        unit_start: usize,
        unit_end: usize,
        out: &mut Duration,
    ) -> Result<(), ParseDurationError> {
        let unit = Unit::from_name(&self.src[unit_start..unit_end])
            .ok_or(ParseDurationError::UnknownUnit)?;

        let (secs, nanos) = unit.whole(value)?;
        add_to(out, secs, nanos)?;

        if let Some(fraction) = fraction {
            let (secs, nanos) = unit.fraction(fraction)?;
            add_to(out, secs, nanos)?;
        }

        Ok(())
    }
}
