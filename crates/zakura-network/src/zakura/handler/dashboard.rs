//! Address-free connection observations for the opt-in local dashboard.

use iroh::endpoint::Connection;

use super::ZakuraConnId;

/// Previous cumulative counters for one connection, including all of its paths.
#[derive(Default)]
pub(super) struct ConnectionCounters {
    rx_bytes: u64,
    tx_bytes: u64,
    lost_packets: u64,
    lost_bytes: u64,
}

impl ConnectionCounters {
    /// Capture transport counters and selected-path RTT without peer identity.
    pub(super) fn observe(
        &mut self,
        connection: &Connection,
        connection_id: ZakuraConnId,
        closed: bool,
    ) {
        let stats = connection.stats();
        metrics::counter!("zakura.p2p.quic.rx_bytes")
            .increment(stats.udp_rx.bytes.saturating_sub(self.rx_bytes));
        metrics::counter!("zakura.p2p.quic.tx_bytes")
            .increment(stats.udp_tx.bytes.saturating_sub(self.tx_bytes));
        metrics::counter!("zakura.p2p.quic.lost_packets")
            .increment(stats.lost_packets.saturating_sub(self.lost_packets));
        metrics::counter!("zakura.p2p.quic.lost_bytes")
            .increment(stats.lost_bytes.saturating_sub(self.lost_bytes));
        self.rx_bytes = stats.udp_rx.bytes;
        self.tx_bytes = stats.udp_tx.bytes;
        self.lost_packets = stats.lost_packets;
        self.lost_bytes = stats.lost_bytes;
        zakura_jsonl_trace::dashboard::emit(|| {
            let paths = connection.paths();
            let selected = paths.iter().find(|path| path.is_selected());
            let rtt_ms = selected
                .as_ref()
                .map(|path| path.rtt().as_secs_f64() * 1000.0);
            serde_json::json!({
                "event": "native_connection", "connection": connection_id, "closed": closed,
                "rx_bytes": stats.udp_rx.bytes, "tx_bytes": stats.udp_tx.bytes,
                "lost_packets": stats.lost_packets, "lost_bytes": stats.lost_bytes,
                "rtt_ms": rtt_ms,
            })
        });
    }
}
