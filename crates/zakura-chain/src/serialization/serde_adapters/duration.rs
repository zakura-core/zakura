//! Durations encoded as human-readable strings using [`humantime`].

use std::{fmt, time::Duration};

use serde::{
    de::{self, Visitor},
    Deserializer, Serializer,
};

/// Serializes a [`Duration`] using [`humantime::format_duration`].
pub fn serialize<S: Serializer>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&humantime::format_duration(*duration).to_string())
}

/// Deserializes a string using [`humantime::parse_duration`].
pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
    struct DurationVisitor;

    impl<'de> Visitor<'de> for DurationVisitor {
        type Value = Duration;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a duration")
        }

        fn visit_str<E: de::Error>(self, value: &str) -> Result<Duration, E> {
            humantime::parse_duration(value)
                .map_err(|_| E::invalid_value(de::Unexpected::Str(value), &self))
        }
    }

    deserializer.deserialize_str(DurationVisitor)
}
