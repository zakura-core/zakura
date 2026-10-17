//! Tests for human-readable durations, including the vectors from `humantime` 2.4.0.

use std::time::Duration;

use proptest::prelude::*;

use super::{format_duration, parse_duration, ParseDurationError::*};

fn parsed(s: &str) -> Duration {
    parse_duration(s).unwrap_or_else(|error| panic!("{s:?} should parse, but got {error:?}"))
}

#[test]
fn parses_every_unit_name() {
    for (input, expected) in [
        ("17nsec", Duration::new(0, 17)),
        ("17nanos", Duration::new(0, 17)),
        ("33ns", Duration::new(0, 33)),
        ("3usec", Duration::new(0, 3000)),
        ("78us", Duration::new(0, 78_000)),
        ("163µs", Duration::new(0, 163_000)),
        ("31msec", Duration::new(0, 31_000_000)),
        ("31millis", Duration::new(0, 31_000_000)),
        ("6ms", Duration::new(0, 6_000_000)),
        ("3000s", Duration::new(3000, 0)),
        ("300sec", Duration::new(300, 0)),
        ("300secs", Duration::new(300, 0)),
        ("50seconds", Duration::new(50, 0)),
        ("1second", Duration::new(1, 0)),
        ("100m", Duration::new(6000, 0)),
        ("12min", Duration::new(720, 0)),
        ("12mins", Duration::new(720, 0)),
        ("1minute", Duration::new(60, 0)),
        ("7minutes", Duration::new(420, 0)),
        ("2h", Duration::new(7200, 0)),
        ("7hr", Duration::new(25_200, 0)),
        ("7hrs", Duration::new(25_200, 0)),
        ("1hour", Duration::new(3600, 0)),
        ("24hours", Duration::new(86_400, 0)),
        ("1day", Duration::new(86_400, 0)),
        ("2days", Duration::new(172_800, 0)),
        ("365d", Duration::new(31_536_000, 0)),
        ("1week", Duration::new(604_800, 0)),
        ("7weeks", Duration::new(4_233_600, 0)),
        ("104wks", Duration::new(2 * 31_449_600, 0)),
        ("100wk", Duration::new(60_480_000, 0)),
        ("52w", Duration::new(31_449_600, 0)),
        ("1month", Duration::new(2_630_016, 0)),
        ("3months", Duration::new(3 * 2_630_016, 0)),
        ("12M", Duration::new(31_560_192, 0)),
        ("1year", Duration::new(31_557_600, 0)),
        ("7years", Duration::new(7 * 31_557_600, 0)),
        ("15yrs", Duration::new(15 * 31_557_600, 0)),
        ("10yr", Duration::new(10 * 31_557_600, 0)),
        ("17y", Duration::new(536_479_200, 0)),
    ] {
        assert_eq!(parsed(input), expected, "{input:?}");
    }
}

#[test]
fn parses_fractions_that_fit_the_unit() {
    for (input, expected) in [
        ("3.1usec", Duration::new(0, 3100)),
        ("3.01us", Duration::new(0, 3010)),
        ("3.001us", Duration::new(0, 3001)),
        ("31.1ms", Duration::new(0, 31_100_000)),
        ("31.000001ms", Duration::new(0, 31_000_001)),
        ("300.0s", Duration::new(300, 0)),
        ("0.0s", Duration::ZERO),
        ("0.2s", Duration::new(0, 200_000_000)),
        ("1.02s", Duration::new(1, 20_000_000)),
        ("1.000000002s", Duration::new(1, 2)),
        ("1.123456789s", Duration::new(1, 123_456_789)),
        ("100.0m", Duration::new(6000, 0)),
        ("12.1min", Duration::new(726, 0)),
        ("1.5minutes", Duration::new(90, 0)),
        ("2.5h", Duration::new(9000, 0)),
        ("0.5h", Duration::new(1800, 0)),
        ("1.5d", Duration::new(86_400 + 86_400 / 2, 0)),
    ] {
        assert_eq!(parsed(input), expected, "{input:?}");
    }
}

#[test]
fn rejects_fractions_finer_than_the_unit_allows() {
    for input in [
        "17.5nsec",
        "0.0005ns",
        "3.0001us",
        "0.123456us",
        "31.0000001ms",
        "1.0000000002s",
        "0.0000000002s",
        "0.000123456789s",
        "0.00000005d",
    ] {
        assert_eq!(parse_duration(input), Err(NumberOverflow), "{input:?}");
    }
}

#[test]
fn rejects_malformed_numbers_and_units() {
    assert!(matches!(parse_duration("1.s"), Err(InvalidCharacter(_))));
    assert!(matches!(parse_duration("1..s"), Err(InvalidCharacter(_))));
    assert!(matches!(parse_duration("1s!"), Err(InvalidCharacter(_))));
    assert!(matches!(parse_duration(".1s"), Err(NumberExpected(_))));
    assert!(matches!(parse_duration("."), Err(NumberExpected(_))));
    assert!(matches!(parse_duration("s"), Err(NumberExpected(_))));
    assert_eq!(parse_duration("15"), Err(UnknownUnit));
    assert_eq!(parse_duration("15 parsecs"), Err(UnknownUnit));
    assert_eq!(parse_duration(""), Err(Empty));
    assert_eq!(parse_duration("  "), Err(Empty));
    assert_eq!(parse_duration("18446744073709551616s"), Err(NumberOverflow));
    assert_eq!(parse_duration("18446744073709551615y"), Err(NumberOverflow));
}

#[test]
fn parses_combined_values_and_whitespace_like_humantime() {
    for (input, expected) in [
        ("0", Duration::ZERO),
        ("2h 15m", Duration::new(8100, 0)),
        ("20 min 17 nsec ", Duration::new(1200, 17)),
        ("7.120us 3ns", Duration::new(0, 7123)),
        ("1.234s 1.345ms 1.678us 1ns", Duration::new(1, 235_346_679)),
        ("1.234s0.345ms0.678us0ns", Duration::new(1, 234_345_678)),
        // Whitespace between digits is skipped, so this is twelve seconds.
        ("1 2s", Duration::new(12, 0)),
    ] {
        assert_eq!(parsed(input), expected, "{input:?}");
    }
}

#[test]
fn formats_largest_units_first() {
    for (duration, expected) in [
        (Duration::ZERO, "0s"),
        (Duration::new(15, 123), "15s 123ns"),
        (Duration::from_millis(1500), "1s 500ms"),
        (Duration::new(0, 1_002_003), "1ms 2us 3ns"),
        (Duration::from_secs(90), "1m 30s"),
        (Duration::from_secs(3600), "1h"),
        (Duration::from_secs(86_400), "1day"),
        (Duration::from_secs(2 * 86_400 + 1), "2days 1s"),
        (Duration::from_secs(2_630_016), "1month"),
        (Duration::from_secs(31_557_600), "1year"),
        (
            Duration::new(
                2 * 31_557_600 + 2 * 2_630_016 + 3 * 86_400 + 4 * 3600 + 5 * 60 + 6,
                7,
            ),
            "2years 2months 3days 4h 5m 6s 7ns",
        ),
    ] {
        assert_eq!(format_duration(duration).to_string(), expected);
    }
}

#[test]
fn every_second_of_a_day_round_trips() {
    for second in 0..86_400 {
        let duration = Duration::from_secs(second);
        assert_eq!(parsed(&format_duration(duration).to_string()), duration);
    }
}

proptest! {
    #[test]
    fn formatted_durations_round_trip(
        secs in 0..253_370_764_800_u64,
        nanos in 0..1_000_000_000_u32,
    ) {
        let duration = Duration::new(secs, nanos);
        prop_assert_eq!(parsed(&format_duration(duration).to_string()), duration);
    }

    #[test]
    fn parsing_arbitrary_strings_does_not_panic(input in "\\PC{0,24}") {
        let _ = parse_duration(&input);
    }
}
