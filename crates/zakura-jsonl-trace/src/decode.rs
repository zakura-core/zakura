//! Shared value validation for CSV readers.

use std::{collections::HashSet, io, sync::OnceLock};

use serde_json::{Map, Value};

/// Shared valid and invalid CSV cases for independent readers.
pub const CSV_READER_FIXTURES: &str = include_str!("../tests/fixtures/csv.json");

/// Return the checked producer schema.
pub fn schema() -> &'static Value {
    static SCHEMA: OnceLock<Value> = OnceLock::new();
    SCHEMA.get_or_init(|| {
        serde_json::from_str(crate::SCHEMA_JSON).expect("embedded trace schema is valid JSON")
    })
}

/// Decode one JSON-typed CSV field.
pub fn json(text: &str) -> io::Result<Value> {
    serde_json::from_str(text).map_err(|_| invalid("invalid JSON field"))
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
