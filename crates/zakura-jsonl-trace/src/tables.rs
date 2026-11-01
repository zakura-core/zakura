//! The registry of node CSV trace tables, and table selections built from it.

use std::{fmt, sync::Arc};

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

use crate::JsonlTraceTable;

/// Legacy upgrade and control-handshake transitions.
pub const HANDSHAKE: JsonlTraceTable = JsonlTraceTable::csv(
    "handshake",
    "handshake.csv",
    &[
        "event",
        "conn",
        "stream",
        "payload_len",
        "frame_len",
        "max_frame_bytes",
        "peer",
        "role",
        "phase",
        "reason",
        "selected_protocol",
        "direction",
        "stream_kind",
        "network",
    ],
);

/// Connection admission and close transitions.
pub const CONN: JsonlTraceTable = JsonlTraceTable::csv(
    "conn",
    "conn.csv",
    &[
        "event",
        "conn",
        "stream",
        "payload_len",
        "frame_len",
        "max_frame_bytes",
        "peer",
        "role",
        "phase",
        "reason",
        "selected_protocol",
        "direction",
        "stream_kind",
        "network",
    ],
);

/// Per-connection stream admission transitions.
pub const STREAM: JsonlTraceTable = JsonlTraceTable::csv(
    "stream",
    "stream.csv",
    &[
        "event",
        "conn",
        "stream",
        "payload_len",
        "frame_len",
        "max_frame_bytes",
        "peer",
        "role",
        "phase",
        "reason",
        "selected_protocol",
        "direction",
        "stream_kind",
        "network",
    ],
);

/// Discovery dialer decisions and backoff classification.
pub const DISCOVERY: JsonlTraceTable =
    JsonlTraceTable::csv("discovery", "discovery.csv", &["event", "result", "peer"]);

/// Frame and message rate-limit decisions.
pub const RATELIMIT: JsonlTraceTable = JsonlTraceTable::csv(
    "ratelimit",
    "ratelimit.csv",
    &[
        "event",
        "conn",
        "stream",
        "payload_len",
        "frame_len",
        "max_frame_bytes",
        "peer",
        "role",
        "phase",
        "reason",
        "selected_protocol",
        "direction",
        "stream_kind",
        "network",
    ],
);

/// Header-sync policy, accounting, and frontier events.
pub const HEADER_SYNC: JsonlTraceTable = JsonlTraceTable::csv(
    "header_sync",
    "header_sync.csv",
    &[
        "event",
        "peer",
        "session_id",
        "direction",
        "reason",
        "boundary",
        "disposition",
        "state_version",
        "header_generation",
        "verified_generation",
        "branch_anchor",
        "branch_target",
        "request_id",
        "stream_version",
        "target_hash",
        "common_ancestor_height",
        "common_ancestor_hash",
        "locator_count",
        "locator_head",
        "header_count",
        "complete",
        "tree_aux_schema",
        "outcome",
        "inbound_count",
        "outbound_count",
        "stage",
        "category",
        "attribution",
        "cause",
        "operation",
        "work_anchor_height",
        "work_anchor_hash",
        "selected_tip_height",
        "selected_tip_hash",
        "max_headers_per_response",
        "max_inflight_requests",
        "max_message_bytes",
        "tree_aux_schema_mask",
        "old_selected_height",
        "old_selected_hash",
        "new_selected_height",
        "new_selected_hash",
        "height",
        "repair_generation",
        "phase",
        "supplier_count",
        "predecessor_height",
        "peers_considered",
        "rejected_height",
        "rejected_capacity",
        "rejected_schema",
        "rejected_busy",
        "rejected_tried",
        "supplier_status_wait",
        "best_peer_height",
        "best_peer_hash",
    ],
);

/// Legacy compatibility request/response events.
pub const LEGACY_REQUEST: JsonlTraceTable = JsonlTraceTable::csv(
    "legacy_request",
    "legacy_request.csv",
    &[
        "event",
        "peer",
        "request_id",
        "request",
        "message_type",
        "response",
        "item_count",
        "missing_count",
        "error",
    ],
);

/// Block-sync (stream-6) scheduling, download, submit, and commit events.
pub const BLOCK_SYNC: JsonlTraceTable = JsonlTraceTable::csv(
    "block_sync",
    "block_sync.csv",
    &[
        "event",
        "peer",
        "kind",
        "height",
        "hash",
        "range_start",
        "range_count",
        "servable_low",
        "servable_high",
        "expected_count",
        "estimated_bytes",
        "serialized_bytes",
        "decoded_attributed_memory_size_bytes",
        "sequencer_input_decoded_attributed_memory_bytes",
        "reorder_decoded_attributed_memory_bytes",
        "applying_decoded_attributed_memory_bytes",
        "active_pipeline_decoded_attributed_memory_bytes",
        "elapsed_ms",
        "prepare_elapsed_ms",
        "send_elapsed_ms",
        "result",
        "reason",
        "error",
        "apply_token",
        "request_floor",
        "body_download_floor",
        "floor_gap_height",
        "floor_gap_state",
        "floor_gap_servable_peers",
        "floor_gap_available_peers",
        "floor_gap_outstanding_peers",
        "floor_gap_oldest_outstanding_ms",
        "floor_gap_next_deadline_ms",
        "verified_block_tip",
        "best_header_tip",
        "body_lag",
        "applying",
        "submitted_applies",
        "reorder",
        "outstanding",
        "budget_available",
        "budget_reserved",
        "budget_reserved_after",
        "received_bytes_per_sec",
        "received_blocks_per_sec",
        "committed_bytes_per_sec",
        "committed_blocks_per_sec",
        "committed_count",
        "download_blocked_on_budget",
        "peers_wanting_slots",
        "peers",
        "active_connections",
        "peers_with_status",
        "needed_min",
        "needed_count",
        "queue_len",
        "queue_blocks",
        "queue_capacity",
        "queue_max_capacity",
        "queue_min_start",
        "assigned_len",
        "local_body_work",
        "refill_low_water",
        "covered_max_end",
        "fill_stop_reason",
        "fill_sent",
        "direction",
        "received_status",
        "released_bytes",
        "returned_count",
        "already_pending_count",
        "released_count",
        "missing_count",
        "pending_after",
        "in_flight_after",
        "sequencer_input_queued_bytes",
        "sequencer_input_queued_blocks",
        "sequencer_input_capacity",
        "sequencer_input_capacity_before",
        "sequencer_input_max_capacity",
        "reorder_buffered_bytes",
        "applying_buffered_bytes",
        "unsubmitted_applying_count",
        "in_flight_submission_count",
        "in_flight_submission_bytes",
        "retained_pipeline_wire_bytes",
        "inbound_peers",
        "outbound_peers",
        "inbound_peers_with_status",
        "outbound_peers_with_status",
        "request_slot_capacity",
        "request_slot_effective_window",
        "request_slot_available",
        "request_slot_saturated_peers",
        "available_slots",
        "peer_outstanding",
        "normal_slots",
        "floor_slots",
        "pending_work",
        "unreceived_count",
        "return_min_height",
        "return_max_height",
        "request_start",
        "request_range_count",
        "request_elapsed_ms",
        "sequencer_send_elapsed_us",
        "sequencer_queue_elapsed_us",
        "decode_permit_wait_us",
        "ok",
        "retry_attempt",
        "previous_verified_tip",
        "previous_download_floor",
        "preserve_active_successors",
        "peer_has_successor_after",
        "peer_outstanding_conflicts_at_tip",
        "reset_tip_matches_local_work",
        "has_local_successor_after",
        "requests_without_block_progress",
        "no_progress_request_cap",
        "block_progress_proven",
        "bbr_cwnd",
        "bbr_rtprop_ms",
        "bbr_btlbw_milliblocks_per_sec",
        "bbr_cwnd_bytes",
        "bbr_inflight_bytes",
        "bbr_btlbw_bytes_per_sec",
        "bbr_delivered",
        "bbr_phase",
        "bbr_smoothed_elapsed_ms",
        "bbr_delay_cap",
        "bbr_reliability_permille",
        "floor_bypass",
        "last_block_age_ms",
    ],
);

/// Zakurad adapter boundary events for commits, state reads, and frontier mirrors.
pub const COMMIT_STATE: JsonlTraceTable = JsonlTraceTable::csv(
    "commit_state",
    "commit_state.csv",
    &[
        "event",
        "source",
        "action",
        "peer",
        "reason",
        "range_start",
        "range_count",
        "height",
        "hash",
        "apply_token",
        "best_header_tip",
        "elapsed_ms",
        "result",
        "requested_count",
        "apply_class",
        "local_frontier",
        "queue_len",
        "in_flight_count",
    ],
);

/// Per-connection QUIC transport samples, every 10 s and on close
/// (zakura-quic OBS-3).
pub const QUIC_CONN: JsonlTraceTable = JsonlTraceTable::csv(
    "quic_conn",
    "quic_conn.csv",
    &[
        "event",
        "conn",
        "direction",
        "peer",
        "admitted_ip",
        "rtt_micros",
        "cwnd",
        "lost_packets",
        "congestion_events",
        "bytes_sent",
        "bytes_received",
        "current_mtu",
        "bytes_in_flight",
        "send_blocked_micros",
        "close_reason",
    ],
);

/// Failed non-blocking outbound queue sends for Zakura wire messages.
pub const QUEUE_SEND: JsonlTraceTable = JsonlTraceTable::csv(
    "queue_send",
    "queue_send.csv",
    &[
        "event",
        "service",
        "message",
        "peer",
        "error",
        "reason",
        "queue_capacity",
        "queue_max_capacity",
        "range_start",
        "range_count",
        "height",
        "hash",
        "session_id",
        "request_id",
    ],
);

/// Legacy TCP peer-set request outcomes.
pub const LEGACY_PEER_REQUEST: JsonlTraceTable = JsonlTraceTable::csv(
    "legacy_peer_request",
    "legacy_peer_request.csv",
    &[
        "event",
        "request_id",
        "peer_id",
        "peer",
        "peer_start_height",
        "local_tip_height",
        "elapsed_ms",
        "locator_tip",
        "stop",
        "requested_hash",
        "route",
        "result",
        "hash_count",
        "inferred_start_height",
        "inferred_end_height",
        "response",
        "error",
        "returned_hash",
        "returned_height",
        "missing_hash",
    ],
);

/// Legacy block sync pipeline diagnostics.
pub const LEGACY_SYNC: JsonlTraceTable = JsonlTraceTable::csv(
    "legacy_sync",
    "legacy_sync.csv",
    &[
        "event",
        "state_tip",
        "reason",
        "error",
        "checkpoint_height",
        "reserve",
        "prospective_tips",
        "discovered",
        "hash",
        "height",
        "result",
        "phase",
        "elapsed_ms",
        "phase_elapsed_ms",
        "previous_phase",
        "in_flight",
        "registry_retries",
        "tasks",
        "download_elapsed_ms",
        "peer",
    ],
);

/// Mining RPC events: block templates served and mined blocks submitted.
pub const MINING: JsonlTraceTable = JsonlTraceTable::csv(
    "mining",
    "mining.csv",
    &[
        "event",
        "height",
        "previous_block_hash",
        "transactions",
        "long_poll_id",
        "work_id",
        "submit_old",
        "cur_time",
        "client_long_poll_id",
        "elapsed_us",
        "error",
        "method",
        "hash",
        "result",
        "reason",
    ],
);

/// Legacy P2P wire messages: send attempt, write completion, decoded receipt, and, for block
/// bodies on Linux, the peer's acknowledgement of the body.
///
/// Rows share `conn` with `p2p_request` and `block_gossip`, and `seq` orders sends on one
/// connection. The `tcp_*` columns are the kernel's TCP state: on a block body's `send_done`
/// row when the write returned, and on its `body_acked` or `body_ack_timeout` row when the
/// delivery watch ended. `tcp_retrans` counts retransmitted segments during the delivery;
/// `tcp_total_retrans` counts them since the connection opened.
pub const P2P: JsonlTraceTable = JsonlTraceTable::csv(
    "p2p",
    "p2p.csv",
    &[
        "event",
        "conn",
        "direction",
        "peer",
        "seq",
        "msg",
        "hash",
        "height",
        "item_count",
        "block_hashes",
        "tx_count",
        "nonce",
        "body_bytes",
        "write_us",
        "tcp_notsent_bytes",
        "tcp_unacked",
        "tcp_cwnd",
        "tcp_rtt_us",
        "tcp_total_retrans",
        "tcp_delivery_rate",
        "ack_us",
        "acked_bytes",
        "tcp_retrans",
        "samples",
        "result",
        "error",
    ],
);

/// Legacy peer request routing: peer selection, connection queueing, and request writes.
pub const P2P_REQUEST: JsonlTraceTable = JsonlTraceTable::csv(
    "p2p_request",
    "p2p_request.csv",
    &[
        "event",
        "request_id",
        "conn",
        "direction",
        "peer",
        "request",
        "hash",
        "item_count",
        "route",
        "candidates",
        "active_request",
        "active_request_id",
        "active_age_us",
        "queued",
        "queue_us",
        "write_us",
        "elapsed_us",
        "result",
        "error",
    ],
);

/// Legacy block advertisement scheduling: broadcast, upload-slot, and inventory write events.
pub const BLOCK_GOSSIP: JsonlTraceTable = JsonlTraceTable::csv(
    "block_gossip",
    "block_gossip.csv",
    &[
        "event",
        "broadcast_id",
        "hash",
        "reason",
        "conn",
        "peer",
        "position",
        "peers",
        "concurrency",
        "available_slots",
        "wait_us",
        "held_us",
        "requested_us",
        "written_us",
        "elapsed_us",
        "release_reason",
        "succeeded",
        "succeeded_peers",
    ],
);

/// Gossip and sync block download, verification, and commit timing.
pub const BLOCK_VERIFY: JsonlTraceTable = JsonlTraceTable::csv(
    "block_verify",
    "block_verify.csv",
    &[
        "event",
        "source",
        "hash",
        "height",
        "parent_hash",
        "parent",
        "tip_height",
        "download_us",
        "wait_us",
        "verify_us",
        "total_us",
        "result",
        "error",
    ],
);

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
    P2P,
    P2P_REQUEST,
    BLOCK_GOSSIP,
    BLOCK_VERIFY,
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
    fn registered_tables_match_the_shared_schema() {
        let schema = crate::decode::schema()["tables"]
            .as_object()
            .expect("schema tables form an object");
        assert_eq!(schema.len(), ALL.len(), "every schema table is registered");
        for table in ALL {
            assert_eq!(table.file_name(), format!("{}.csv", table.table()));
            let columns: Vec<_> = schema[table.table()]
                .as_array()
                .expect("registered table is in the schema")
                .iter()
                .map(|column| column.as_str().expect("schema column is a string"))
                .collect();
            assert_eq!(table.header(), columns, "{}", table.table());
        }
    }

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
