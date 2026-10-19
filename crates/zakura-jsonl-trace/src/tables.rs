//! The registry of node JSONL trace tables, and table selections built from it.

use std::{fmt, sync::Arc};

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

use crate::JsonlTraceTable;

/// Legacy upgrade and control-handshake transitions.
pub const HANDSHAKE: JsonlTraceTable = JsonlTraceTable::new("handshake", "handshake.jsonl");

/// Connection admission and close transitions.
pub const CONN: JsonlTraceTable = JsonlTraceTable::new("conn", "conn.jsonl");

/// Per-connection stream admission transitions.
pub const STREAM: JsonlTraceTable = JsonlTraceTable::new("stream", "stream.jsonl");

/// Discovery dialer decisions and backoff classification.
pub const DISCOVERY: JsonlTraceTable = JsonlTraceTable::new("discovery", "discovery.jsonl");

/// Frame and message rate-limit decisions.
pub const RATELIMIT: JsonlTraceTable = JsonlTraceTable::new("ratelimit", "ratelimit.jsonl");

/// Header-sync policy, accounting, and frontier events.
pub const HEADER_SYNC: JsonlTraceTable = JsonlTraceTable::new("header_sync", "header_sync.jsonl");

/// Legacy compatibility request/response events.
pub const LEGACY_REQUEST: JsonlTraceTable =
    JsonlTraceTable::new("legacy_request", "legacy_request.jsonl");

/// Block-sync (stream-6) scheduling, download, submit, and commit events.
pub const BLOCK_SYNC: JsonlTraceTable = JsonlTraceTable::new("block_sync", "block_sync.jsonl");

/// Zakurad adapter boundary events for commits, state reads, and frontier mirrors.
pub const COMMIT_STATE: JsonlTraceTable =
    JsonlTraceTable::new("commit_state", "commit_state.jsonl");

/// Per-connection QUIC transport samples, every 10 s and on close
/// (zakura-quic OBS-3).
pub const QUIC_CONN: JsonlTraceTable = JsonlTraceTable::new("quic_conn", "quic_conn.jsonl");

/// Failed non-blocking outbound queue sends for Zakura wire messages.
pub const QUEUE_SEND: JsonlTraceTable = JsonlTraceTable::new("queue_send", "queue_send.jsonl");

/// Legacy TCP peer-set request outcomes.
pub const LEGACY_PEER_REQUEST: JsonlTraceTable =
    JsonlTraceTable::new("legacy_peer_request", "legacy_peer_request.jsonl");

/// Legacy block sync pipeline diagnostics.
pub const LEGACY_SYNC: JsonlTraceTable = JsonlTraceTable::new("legacy_sync", "legacy_sync.jsonl");

/// Mining RPC events: block templates served and mined blocks submitted.
pub const MINING: JsonlTraceTable = JsonlTraceTable::new("mining", "mining.jsonl");

/// Every table a node can write.
pub const ALL: &[JsonlTraceTable] = &[
    HANDSHAKE,
    CONN,
    STREAM,
    DISCOVERY,
    RATELIMIT,
    HEADER_SYNC,
    LEGACY_REQUEST,
    BLOCK_SYNC,
    COMMIT_STATE,
    QUIC_CONN,
    QUEUE_SEND,
    LEGACY_PEER_REQUEST,
    LEGACY_SYNC,
    MINING,
];

/// A set of registered trace tables that a tracer writes.
///
/// Deserializes from a list of table names. Unknown names are rejected, so a typo cannot
/// silently disable a table. The default selects every table in [`ALL`].
#[derive(Clone, Eq, PartialEq)]
pub struct JsonlTraceTables {
    names: Arc<[&'static str]>,
}

impl JsonlTraceTables {
    /// Selects every registered table.
    pub fn all() -> Self {
        Self {
            names: ALL.iter().map(|table| table.table()).collect(),
        }
    }

    /// Selects no tables.
    pub fn none() -> Self {
        Self {
            names: Arc::new([]),
        }
    }

    /// Selects the registered tables named in `names`.
    ///
    /// Returns the first name that is not a registered table as the error.
    pub fn from_names<'a>(names: impl IntoIterator<Item = &'a str>) -> Result<Self, String> {
        let mut selected = Vec::new();
        for name in names {
            let table = ALL
                .iter()
                .find(|table| table.table() == name)
                .ok_or_else(|| name.to_string())?;
            if !selected.contains(&table.table()) {
                selected.push(table.table());
            }
        }

        Ok(Self {
            names: selected.into(),
        })
    }

    /// Returns `true` if the selection writes `table`.
    pub fn contains(&self, table: &str) -> bool {
        self.names.contains(&table)
    }

    /// Returns `true` if the selection writes no tables.
    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }

    /// Returns the selected table names.
    pub fn names(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.names.iter().copied()
    }
}

impl Default for JsonlTraceTables {
    fn default() -> Self {
        Self::all()
    }
}

impl fmt::Debug for JsonlTraceTables {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.names()).finish()
    }
}

impl Serialize for JsonlTraceTables {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.names())
    }
}

impl<'de> Deserialize<'de> for JsonlTraceTables {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let names = Vec::<String>::deserialize(deserializer)?;
        Self::from_names(names.iter().map(String::as_str)).map_err(|unknown| {
            let known: Vec<_> = ALL.iter().map(|table| table.table()).collect();
            de::Error::custom(format!(
                "unknown trace table {unknown:?}; known tables: {}",
                known.join(", ")
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registered_table_names_and_files_are_unique() {
        for (index, table) in ALL.iter().enumerate() {
            assert!(
                ALL[index + 1..]
                    .iter()
                    .all(|other| other.table() != table.table()
                        && other.file_name() != table.file_name()),
                "{} is registered twice",
                table.table()
            );
        }
    }

    #[test]
    fn selection_deserializes_known_names_and_rejects_unknown_names() {
        let selected: JsonlTraceTables =
            serde_json::from_str(r#"["mining", "conn", "mining"]"#).expect("known names parse");
        assert_eq!(selected.names().collect::<Vec<_>>(), ["mining", "conn"]);
        assert!(selected.contains("mining"));
        assert!(!selected.contains("block_sync"));

        let error = serde_json::from_str::<JsonlTraceTables>(r#"["minnig"]"#)
            .expect_err("a misspelled table is rejected");
        assert!(error.to_string().contains("unknown trace table \"minnig\""));
    }

    #[test]
    fn default_selection_round_trips_every_table() {
        let all = JsonlTraceTables::default();
        assert!(ALL.iter().all(|table| all.contains(table.table())));

        let json = serde_json::to_string(&all).expect("selection serializes");
        let parsed: JsonlTraceTables = serde_json::from_str(&json).expect("selection parses");
        assert_eq!(parsed, all);
    }
}
