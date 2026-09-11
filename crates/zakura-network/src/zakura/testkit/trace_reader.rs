//! Post-flush JSONL and CSV trace reader for Zakura tests.

use std::{
    collections::HashSet,
    fs,
    io::{self, BufRead},
    path::{Path, PathBuf},
};

use serde_json::Value;
use zakura_jsonl_trace::{ENVELOPE_COLUMNS, EXTRA_COLUMN};

/// Loaded Zakura trace tables.
#[derive(Clone, Debug, Default)]
pub struct TraceReader {
    rows: Vec<TraceRow>,
}

#[derive(Clone, Debug)]
struct TraceRow {
    table: String,
    source_node: Option<String>,
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
    /// Load all `*.jsonl` and `*.csv` files in `path` and one level of per-node
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
                || !matches!(
                    path.extension().and_then(|ext| ext.to_str()),
                    Some("jsonl" | "csv")
                )
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
        if path.extension().and_then(|ext| ext.to_str()) == Some("csv") {
            let mut reader = csv::Reader::from_path(path)?;
            let headers = reader.headers()?.clone();
            let header_columns = validate_csv_headers(&headers)?;
            for record in reader.records() {
                let record = record?;
                let mut row = serde_json::Map::new();
                let mut extra = None;
                for (column, value) in headers.iter().zip(record.iter()) {
                    if value.is_empty() {
                        continue;
                    }
                    if column == EXTRA_COLUMN {
                        extra = Some(
                            serde_json::from_str::<serde_json::Map<String, Value>>(value).map_err(
                                |error| io::Error::new(io::ErrorKind::InvalidData, error),
                            )?,
                        );
                    } else {
                        // CSV carries no type metadata. Keep identity and label columns textual.
                        let value = if matches!(
                            column,
                            "ts" | "request_id"
                                | "peer_id"
                                | "peer_start_height"
                                | "local_tip_height"
                                | "elapsed_ms"
                                | "hash_count"
                                | "inferred_start_height"
                                | "inferred_end_height"
                                | "returned_height"
                                | "range_start"
                                | "range_count"
                                | "height"
                                | "apply_token"
                                | "best_header_tip"
                                | "requested_count"
                                | "local_frontier"
                                | "queue_len"
                                | "in_flight_count"
                        ) {
                            serde_json::from_str(value).map_err(|error| {
                                io::Error::new(io::ErrorKind::InvalidData, error)
                            })?
                        } else {
                            Value::String(value.to_owned())
                        };
                        row.insert(column.to_owned(), value);
                    }
                }
                if let Some(extra) = extra {
                    if let Some(column) = extra
                        .keys()
                        .find(|column| header_columns.contains(column.as_str()))
                    {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("CSV extra field collides with column {column:?}"),
                        ));
                    }
                    row.extend(extra);
                }
                self.rows.push(TraceRow {
                    table: table.clone(),
                    source_node: source_node.clone(),
                    row: Value::Object(row),
                });
            }
            return Ok(());
        }
        let file = fs::File::open(path)?;

        for line in io::BufReader::new(file).lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let row = serde_json::from_str(&line)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            self.rows.push(TraceRow {
                table: table.clone(),
                source_node: source_node.clone(),
                row,
            });
        }

        Ok(())
    }
}

fn validate_csv_headers(headers: &csv::StringRecord) -> io::Result<HashSet<String>> {
    let columns: HashSet<_> = headers.iter().map(ToOwned::to_owned).collect();
    if columns.len() != headers.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "CSV header contains duplicate columns",
        ));
    }
    if !headers
        .iter()
        .take(ENVELOPE_COLUMNS.len())
        .eq(ENVELOPE_COLUMNS.iter().copied())
        || headers.iter().next_back() != Some(EXTRA_COLUMN)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "CSV header is missing the trace envelope or trailing extra column",
        ));
    }
    Ok(columns)
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

        self.row
            .get("node")
            .and_then(Value::as_str)
            .is_some_and(|row_node| row_node == node)
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

    #[test]
    fn reader_loads_csv_numbers_labels_and_quoted_fields() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut writer =
            csv::Writer::from_path(dir.path().join("commit_state.csv")).expect("CSV file");
        writer
            .write_record([
                "ts",
                "wall_ts",
                "node",
                "process_trace_id",
                "event",
                "height",
                "hash",
                "reason",
                "extra",
            ])
            .expect("header");
        writer
            .write_record([
                "1",
                "2026-09-11T00:00:00Z",
                "01",
                "process-1",
                "commit_finish",
                "42",
                "1234",
                "error, with\na newline",
                r#"{"new_count":7}"#,
            ])
            .expect("row");
        writer.flush().expect("flush");
        let reader = TraceReader::load(dir.path()).expect("CSV reader");
        reader.table("commit_state").assert_row(
            "commit_finish",
            &[
                ("node", TraceValue::Str("01")),
                ("height", TraceValue::U64(42)),
                ("hash", TraceValue::Str("1234")),
                ("reason", TraceValue::Str("error, with\na newline")),
                ("new_count", TraceValue::U64(7)),
            ],
        );
    }

    #[test]
    fn reader_rejects_duplicate_csv_columns() {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::write(
            dir.path().join("commit_state.csv"),
            "ts,wall_ts,node,process_trace_id,event,event,extra\n",
        )
        .expect("trace file");

        let error = TraceReader::load(dir.path()).expect_err("duplicate header must fail");
        assert!(error.to_string().contains("duplicate columns"));
    }

    #[test]
    fn reader_rejects_csv_extra_collisions() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut writer =
            csv::Writer::from_path(dir.path().join("commit_state.csv")).expect("CSV file");
        writer
            .write_record([
                "ts",
                "wall_ts",
                "node",
                "process_trace_id",
                "event",
                "extra",
            ])
            .expect("header");
        writer
            .write_record([
                "1",
                "2026-09-11T00:00:00Z",
                "01",
                "process-1",
                "commit_start",
                r#"{"event":"commit_finish"}"#,
            ])
            .expect("row");
        writer.flush().expect("flush");

        let error = TraceReader::load(dir.path()).expect_err("extra collision must fail");
        assert!(error.to_string().contains("collides with column"));
    }

    #[test]
    fn reader_counts_and_matches_subsequences_within_a_table() {
        let dir = tempfile::tempdir().expect("tempdir");
        let node_dir = dir.path().join("node-01");
        fs::create_dir_all(&node_dir).expect("node dir");
        fs::write(
            node_dir.join("handshake.jsonl"),
            r#"{"node":"01","event":"control.started"}"#.to_string()
                + "\n"
                + r#"{"node":"01","event":"control.succeeded"}"#
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
        reader
            .node("01")
            .table("handshake")
            .assert_sequence(&["control.started", "control.succeeded"]);
    }

    #[test]
    fn reader_uses_node_subdir_before_row_node_field() {
        let dir = tempfile::tempdir().expect("tempdir");
        let node_dir = dir.path().join("node-01");
        fs::create_dir_all(&node_dir).expect("node dir");
        fs::write(
            node_dir.join("conn.jsonl"),
            r#"{"node":"wrong","event":"accepted"}"#.to_string() + "\n",
        )
        .expect("trace file");

        let reader = TraceReader::load(dir.path()).expect("reader");
        assert_eq!(reader.node("01").table("conn").count("accepted"), 1);
        assert_eq!(reader.node("wrong").table("conn").count("accepted"), 0);
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
            r#"{"node":"b","event":"from-b"}"#.to_string() + "\n",
        )
        .expect("node-b trace file");
        fs::write(
            node_a.join("conn.jsonl"),
            r#"{"node":"a","event":"from-a"}"#.to_string() + "\n",
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
