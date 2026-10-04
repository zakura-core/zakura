//! Optional human-readable durations, with absent values encoded as `None`.

use std::time::Duration;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Deserialize)]
#[serde(transparent)]
struct HumanDuration(#[serde(with = "super::duration")] Duration);

/// Serializes an optional [`Duration`] as an optional human-readable string.
pub fn serialize<S: Serializer>(
    duration: &Option<Duration>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    duration
        .map(|value| humantime::format_duration(value).to_string())
        .serialize(serializer)
}

/// Deserializes an optional string using the shared duration adapter.
pub fn deserialize<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Duration>, D::Error> {
    Option::<HumanDuration>::deserialize(deserializer).map(|value| value.map(|duration| duration.0))
}
