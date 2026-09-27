//! Post-flush JSONL trace reader for Zakura tests.

use std::{
    fs,
    io::{self, BufRead},
    path::{Path, PathBuf},
};

use serde_json::Value;
use zakura_jsonl_trace::TRACE_START_EVENT;

/// Loaded Zakura trace tables.
#[derive(Clone, Debug, Default)]
pub struct TraceReader {
    rows: Vec<TraceRow>,
}

#[derive(Clone, Debug)]
struct TraceRow {
    table: String,
    source_node: Option<String>,
    /// The `node` from the latest header row preceding this row in its file.
    header_node: Option<String>,
    row: Value,
}

/// A filtered view over trace rows.
#[derive(Clone, Debug)]
pub struct TraceQuery<'a> {
    reader: &'a TraceReader,
    table: Option<&'a str>,
    node: Option<&'a str>,
}

/// Expected JSON value in a trace row.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum TraceValue<'a> {
    /// A string field.
    Str(&'a str),
    /// An unsigned integer field.
    U64(u64),
    /// A boolean field.
    Bool(bool),
    /// A null field.
    Null,
}

impl TraceReader {
    /// Load all `*.jsonl` files in `path` and one level of per-node
    /// subdirectories.
    pub fn load(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();
        let mut reader = Self::default();
        if !path.exists() {
            return Ok(reader);
        }

        reader.load_dir(path, None)?;
        let mut dirs = fs::read_dir(path)?.collect::<Result<Vec<_>, _>>()?;
        dirs.sort_by_key(|entry| entry.path());
        for entry in dirs {
            if entry.file_type()?.is_dir() {
                let source_node = source_node_from_dir(&entry.path());
                reader.load_dir(&entry.path(), source_node)?;
            }
        }

        Ok(reader)
    }

    /// Filter rows to a logical table.
    pub fn table<'a>(&'a self, table: &'a str) -> TraceQuery<'a> {
        TraceQuery {
            reader: self,
            table: Some(table),
            node: None,
        }
    }

    /// Filter rows to a node label.
    pub fn node<'a>(&'a self, node: &'a str) -> TraceQuery<'a> {
        TraceQuery {
            reader: self,
            table: None,
            node: Some(node),
        }
    }

    /// Return all loaded rows.
    pub fn rows(&self) -> Vec<&Value> {
        self.rows.iter().map(|row| &row.row).collect()
    }

    fn load_dir(&mut self, path: &Path, source_node: Option<String>) -> io::Result<()> {
        let mut files = fs::read_dir(path)?.collect::<Result<Vec<_>, _>>()?;
        files.sort_by_key(|entry| entry.path());

        for entry in files {
            let path = entry.path();
            if !entry.file_type()?.is_file()
                || path.extension().and_then(|ext| ext.to_str()) != Some("jsonl")
            {
                continue;
            }

            self.load_file(path, source_node.clone())?;
        }

        Ok(())
    }

    fn load_file(&mut self, path: PathBuf, source_node: Option<String>) -> io::Result<()> {
        let table = path
            .file_stem()
            .and_then(|name| name.to_str())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid trace file name"))?
            .to_string();
        let file = fs::File::open(path)?;
        let mut header_node = None;

        for line in io::BufReader::new(file).lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let row: Value = serde_json::from_str(&line)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            if row.get("event").and_then(Value::as_str) == Some(TRACE_START_EVENT) {
                header_node = row.get("node").and_then(Value::as_str).map(str::to_string);
                continue;
            }
            self.rows.push(TraceRow {
                table: table.clone(),
                source_node: source_node.clone(),
                header_node: header_node.clone(),
                row,
            });
        }

        Ok(())
    }
}

impl<'a> TraceQuery<'a> {
    /// Narrow this query to a logical table.
    pub fn table(mut self, table: &'a str) -> Self {
        self.table = Some(table);
        self
    }

    /// Narrow this query to a node label.
    pub fn node(mut self, node: &'a str) -> Self {
        self.node = Some(node);
        self
    }

    fn matching(&self) -> impl Iterator<Item = &'a TraceRow> + '_ {
        self.reader.rows.iter().filter(|row| {
            self.table.is_none_or(|table| row.table == table)
                && self.node.is_none_or(|node| row.matches_node(node))
        })
    }

    /// Return matching rows in file order.
    pub fn rows(&self) -> Vec<&'a Value> {
        self.matching().map(|row| &row.row).collect()
    }

    /// Count matching rows whose `event` field equals `event`.
    pub fn count(&self, event: &str) -> usize {
        self.matching()
            .map(|row| &row.row)
            .filter(|row| row.get("event").and_then(Value::as_str) == Some(event))
            .count()
    }

    /// Return the first matching row.
    pub fn first(&self) -> Option<&'a Value> {
        self.matching().map(|row| &row.row).next()
    }

    /// Return the last matching row.
    pub fn last(&self) -> Option<&'a Value> {
        self.matching().map(|row| &row.row).last()
    }

    /// Assert that `events` appears as an ordered subsequence in this query.
    ///
    /// Use this for one table and one node. Cross-table and cross-node ordering
    /// is intentionally not modeled by the batched JSONL writer.
    pub fn assert_sequence(&self, events: &[&str]) {
        let mut next = 0;

        for row in self.matching().map(|row| &row.row) {
            if next == events.len() {
                break;
            }
            if row.get("event").and_then(Value::as_str) == Some(events[next]) {
                next += 1;
            }
        }

        assert_eq!(
            next,
            events.len(),
            "trace did not contain expected event subsequence: {events:?}",
        );
    }

    /// Assert that this query contains an event row, ignoring row order.
    pub fn assert_event(&self, event: &str) {
        self.assert_row(event, &[]);
    }

    /// Assert that this query contains an event row with all expected fields.
    ///
    /// This is intentionally unordered: JSONL writers batch rows, and e2e
    /// tests should only assert ordering when it is part of the protocol.
    pub fn assert_row(&self, event: &str, fields: &[(&str, TraceValue<'_>)]) {
        let matched = self.matching().map(|row| &row.row).any(|row| {
            row.get("event").and_then(Value::as_str) == Some(event)
                && fields
                    .iter()
                    .all(|(field, value)| trace_value_matches(row.get(*field), *value))
        });

        assert!(
            matched,
            "trace did not contain event {event:?} with fields {fields:?}; matching rows: {:?}",
            self.rows()
        );
    }
}

fn trace_value_matches(actual: Option<&Value>, expected: TraceValue<'_>) -> bool {
    match expected {
        TraceValue::Str(expected) => actual.and_then(Value::as_str) == Some(expected),
        TraceValue::U64(expected) => actual.and_then(Value::as_u64) == Some(expected),
        TraceValue::Bool(expected) => actual.and_then(Value::as_bool) == Some(expected),
        TraceValue::Null => actual.is_some_and(Value::is_null),
    }
}

impl TraceRow {
    fn matches_node(&self, node: &str) -> bool {
        if let Some(source_node) = &self.source_node {
            return source_node == node;
        }

        // Rows written before the header format carry their node inline.
        let row_node = self.row.get("node").and_then(Value::as_str);
        row_node.or(self.header_node.as_deref()) == Some(node)
    }
}

fn source_node_from_dir(path: &Path) -> Option<String> {
    path.file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix("node-"))
        .filter(|node| !node.is_empty())
        .map(ToString::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(node: &str) -> String {
        format!(r#"{{"event":"{TRACE_START_EVENT}","node":"{node}","process_trace_id":"1-2"}}"#)
            + "\n"
    }

    #[test]
    fn reader_counts_and_matches_subsequences_within_a_table() {
        let dir = tempfile::tempdir().expect("tempdir");
        let node_dir = dir.path().join("node-01");
        fs::create_dir_all(&node_dir).expect("node dir");
        fs::write(
            node_dir.join("handshake.jsonl"),
            header("01")
                + r#"{"event":"control.started"}"#
                + "\n"
                + r#"{"event":"control.succeeded"}"#
                + "\n",
        )
        .expect("trace file");

        let reader = TraceReader::load(dir.path()).expect("reader");
        assert_eq!(
            reader
                .node("01")
                .table("handshake")
                .count("control.started"),
            1
        );
        assert_eq!(
            reader.table("handshake").rows().len(),
            2,
            "header rows are not returned as trace rows"
        );
        reader
            .node("01")
            .table("handshake")
            .assert_sequence(&["control.started", "control.succeeded"]);
    }

    #[test]
    fn reader_uses_node_subdir_before_header_node() {
        let dir = tempfile::tempdir().expect("tempdir");
        let node_dir = dir.path().join("node-01");
        fs::create_dir_all(&node_dir).expect("node dir");
        fs::write(
            node_dir.join("conn.jsonl"),
            header("wrong") + r#"{"event":"accepted"}"# + "\n",
        )
        .expect("trace file");

        let reader = TraceReader::load(dir.path()).expect("reader");
        assert_eq!(reader.node("01").table("conn").count("accepted"), 1);
        assert_eq!(reader.node("wrong").table("conn").count("accepted"), 0);
    }

    #[test]
    fn reader_attributes_rows_to_the_latest_header_node() {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::write(
            dir.path().join("conn.jsonl"),
            header("01")
                + r#"{"event":"accepted"}"#
                + "\n"
                + &header("02")
                + r#"{"event":"accepted"}"#
                + "\n"
                + r#"{"event":"accepted"}"#
                + "\n",
        )
        .expect("trace file");

        let reader = TraceReader::load(dir.path()).expect("reader");
        assert_eq!(reader.node("01").table("conn").count("accepted"), 1);
        assert_eq!(reader.node("02").table("conn").count("accepted"), 2);
    }

    #[test]
    fn reader_attributes_old_format_rows_by_their_node_field() {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::write(
            dir.path().join("conn.jsonl"),
            r#"{"node":"01","process_trace_id":"1-2","event":"accepted"}"#.to_string()
                + "\n"
                + r#"{"node":"02","process_trace_id":"1-2","event":"accepted"}"#
                + "\n",
        )
        .expect("trace file");

        let reader = TraceReader::load(dir.path()).expect("reader");
        assert_eq!(reader.node("01").table("conn").count("accepted"), 1);
        assert_eq!(reader.node("02").table("conn").count("accepted"), 1);
    }

    #[test]
    fn reader_handles_header_rows_appended_to_an_old_format_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::write(
            dir.path().join("conn.jsonl"),
            r#"{"node":"01","process_trace_id":"1-2","event":"accepted"}"#.to_string()
                + "\n"
                + &header("02")
                + r#"{"event":"accepted"}"#
                + "\n",
        )
        .expect("trace file");

        let reader = TraceReader::load(dir.path()).expect("reader");
        assert_eq!(reader.node("01").table("conn").count("accepted"), 1);
        assert_eq!(reader.node("02").table("conn").count("accepted"), 1);
        assert_eq!(reader.table("conn").rows().len(), 2);
    }

    #[test]
    fn reader_loads_node_subdirs_in_deterministic_order() {
        let dir = tempfile::tempdir().expect("tempdir");
        let node_b = dir.path().join("node-b");
        let node_a = dir.path().join("node-a");
        fs::create_dir_all(&node_b).expect("node-b dir");
        fs::create_dir_all(&node_a).expect("node-a dir");
        fs::write(
            node_b.join("conn.jsonl"),
            header("b") + r#"{"event":"from-b"}"# + "\n",
        )
        .expect("node-b trace file");
        fs::write(
            node_a.join("conn.jsonl"),
            header("a") + r#"{"event":"from-a"}"# + "\n",
        )
        .expect("node-a trace file");

        let reader = TraceReader::load(dir.path()).expect("reader");
        let events: Vec<_> = reader
            .table("conn")
            .rows()
            .into_iter()
            .filter_map(|row| row.get("event").and_then(Value::as_str))
            .collect();

        assert_eq!(events, ["from-a", "from-b"]);
    }
}
