//! Durations encoded as human-readable strings, such as `1h 30m`.

use std::{fmt, time::Duration};

use serde::{
    de::{self, Visitor},
    Deserializer, Serializer,
};

use crate::fmt::human_duration::{format_duration, parse_duration};

/// Serializes a [`Duration`] as a human-readable string.
pub fn serialize<S: Serializer>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&format_duration(*duration).to_string())
}

/// Deserializes a human-readable duration string.
pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
    struct DurationVisitor;

    impl<'de> Visitor<'de> for DurationVisitor {
        type Value = Duration;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a duration")
        }

        fn visit_str<E: de::Error>(self, value: &str) -> Result<Duration, E> {
            parse_duration(value).map_err(|_| E::invalid_value(de::Unexpected::Str(value), &self))
        }
    }

    deserializer.deserialize_str(DurationVisitor)
}
