//! Shared value validation for CSV readers.

use std::{collections::HashSet, fmt, io, sync::OnceLock};

use serde::{
    de::{self, MapAccess, SeqAccess, Visitor},
    Deserialize, Deserializer,
};
use serde_json::{Map, Value};

/// Shared valid and invalid CSV cases for independent readers.
pub const CSV_READER_FIXTURES: &str = include_str!("../tests/fixtures/csv.json");

/// Maximum bytes accepted from one imported capture.
pub const MAX_CAPTURE_BYTES: u64 = 256 * 1024 * 1024;
/// Maximum records retained from one imported capture.
pub const MAX_CAPTURE_ROWS: usize = 500_000;
/// Maximum files accepted from one imported capture.
pub const MAX_CAPTURE_FILES: usize = 256;
/// Maximum bytes in a decoded CSV field.
pub const MAX_FIELD_BYTES: usize = 64 * 1024;
/// Maximum bytes in a decoded CSV record.
pub const MAX_RECORD_BYTES: usize = 1024 * 1024;

/// Return the checked producer schema.
pub fn schema() -> &'static Value {
    static SCHEMA: OnceLock<Value> = OnceLock::new();
    SCHEMA.get_or_init(|| {
        serde_json::from_str(crate::SCHEMA_JSON).expect("embedded trace schema is valid JSON")
    })
}

/// Decode JSON without accepting duplicate object keys.
pub fn json(text: &str) -> io::Result<Value> {
    let mut depth = 0usize;
    let mut quoted = false;
    let mut escaped = false;
    for byte in text.bytes() {
        if escaped {
            escaped = false;
        } else if quoted && byte == b'\\' {
            escaped = true;
        } else if byte == b'"' {
            quoted = !quoted;
        } else if !quoted {
            if byte == b'[' || byte == b'{' {
                depth += 1;
                if depth > 64 {
                    return Err(invalid("JSON nesting budget exceeded"));
                }
            } else if byte == b']' || byte == b'}' {
                depth = depth.saturating_sub(1);
            }
        }
    }
    serde_json::from_str::<UniqueValue>(text)
        .map(|value| value.0)
        .map_err(|_| invalid("invalid or ambiguous JSON field"))
}

struct UniqueValue(Value);

impl<'de> Deserialize<'de> for UniqueValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct UniqueVisitor;
        impl<'de> Visitor<'de> for UniqueVisitor {
            type Value = UniqueValue;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("JSON with unique object keys")
            }
            fn visit_bool<E: de::Error>(self, value: bool) -> Result<Self::Value, E> {
                Ok(UniqueValue(value.into()))
            }
            fn visit_i64<E: de::Error>(self, value: i64) -> Result<Self::Value, E> {
                Ok(UniqueValue(value.into()))
            }
            fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
                Ok(UniqueValue(value.into()))
            }
            fn visit_f64<E: de::Error>(self, value: f64) -> Result<Self::Value, E> {
                serde_json::Number::from_f64(value)
                    .map(|number| UniqueValue(Value::Number(number)))
                    .ok_or_else(|| E::custom("nonfinite JSON number"))
            }
            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                Ok(UniqueValue(value.into()))
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Null))
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(UniqueValue(value)) = sequence.next_element()? {
                    values.push(value);
                }
                Ok(UniqueValue(Value::Array(values)))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut object: A) -> Result<Self::Value, A::Error> {
                let mut values = Map::new();
                while let Some((key, UniqueValue(value))) =
                    object.next_entry::<String, UniqueValue>()?
                {
                    if values.insert(key, value).is_some() {
                        return Err(de::Error::custom("duplicate JSON key"));
                    }
                }
                Ok(UniqueValue(Value::Object(values)))
            }
        }
        deserializer.deserialize_any(UniqueVisitor)
    }
}

/// Validate envelope and declared value types before consumers inspect a row.
pub fn row(row: &Map<String, Value>) -> io::Result<()> {
    static TYPES: OnceLock<[HashSet<&'static str>; 4]> = OnceLock::new();
    let [integers, booleans, json, declared] = TYPES.get_or_init(|| {
        let schema = schema();
        let fields = |name: &str| -> HashSet<&'static str> {
            schema[name]
                .as_array()
                .expect("schema field list is an array")
                .iter()
                .map(|field| field.as_str().expect("schema field is a string"))
                .collect()
        };
        let declared = schema["tables"]
            .as_object()
            .expect("schema tables form an object")
            .values()
            .flat_map(|columns| columns.as_array().expect("schema columns form an array"))
            .map(|column| column.as_str().expect("schema column is a string"))
            .chain(crate::ENVELOPE_COLUMNS.iter().copied())
            .collect();
        [
            fields("integer_fields"),
            fields("boolean_fields"),
            fields("json_fields"),
            declared,
        ]
    });
    for (key, value) in row {
        if integers.contains(key.as_str()) && !value.is_u64()
            || booleans.contains(key.as_str()) && !value.is_boolean()
            || declared.contains(key.as_str())
                && !integers.contains(key.as_str())
                && !booleans.contains(key.as_str())
                && !json.contains(key.as_str())
                && !value.is_string()
        {
            return Err(invalid("trace field has an invalid value type"));
        }
    }
    if row.get("trace_version").and_then(Value::as_u64) != Some(2) {
        return Err(invalid("trace requires version 2 process-wide clocks"));
    }
    for field in ["node", "process_trace_id", "event"] {
        if !row
            .get(field)
            .and_then(Value::as_str)
            .is_some_and(|value| !value.is_empty())
        {
            return Err(invalid("trace envelope/event is incomplete"));
        }
    }
    if !row.get("ts").is_some_and(Value::is_u64) {
        return Err(invalid("trace timestamp is missing"));
    }
    if row
        .get("wall_ts")
        .and_then(Value::as_str)
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .is_none()
    {
        return Err(invalid("wall_ts must be an absolute RFC 3339 timestamp"));
    }
    if let Some(required) = schema()["required_event_fields"]
        .get(row["event"].as_str().expect("event was validated"))
        .and_then(Value::as_array)
    {
        for field in required {
            if !row.contains_key(field.as_str().expect("schema field is a string")) {
                return Err(invalid("trace event is missing a required field"));
            }
        }
    }
    Ok(())
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
