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
            let (queues, queues_limited) = queue_pressure(connection_id, closed);
            serde_json::json!({
                "event": "native_connection", "connection": connection_id, "closed": closed,
                "rx_bytes": stats.udp_rx.bytes, "tx_bytes": stats.udp_tx.bytes,
                "lost_packets": stats.lost_packets, "lost_bytes": stats.lost_bytes,
                "rtt_ms": rtt_ms, "queues": queues, "queues_limited": queues_limited,
            })
        });
    }
}

use crate::zakura::transport::CapacityObserver;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex, OnceLock,
    },
};

// Fixed cardinality independent of peer-supplied frame contents.
type QueueKey = (u64, u64, u16, &'static str);
static QUEUES: OnceLock<Mutex<BTreeMap<QueueKey, CapacityObserver>>> = OnceLock::new();
static QUEUES_LIMITED: AtomicBool = AtomicBool::new(false);

pub(super) fn register_queue(
    connection: u64,
    stream: u64,
    kind: u16,
    direction: &'static str,
    observer: CapacityObserver,
) {
    let Ok(mut queues) = QUEUES.get_or_init(Default::default).lock() else {
        return;
    };
    if queues.len() >= 4096 {
        queues.retain(|_, observer| observer().is_some());
    }
    if queues.len() >= 4096 {
        QUEUES_LIMITED.store(true, Ordering::Relaxed);
        return;
    }
    queues.insert((connection, stream, kind, direction), observer);
}

#[derive(serde::Serialize)]
struct QueuePressure {
    stream: u64,
    kind: u16,
    kind_name: &'static str,
    direction: &'static str,
    occupied_slots: usize,
    capacity: usize,
}

fn queue_pressure(connection: u64, closed: bool) -> (Vec<QueuePressure>, bool) {
    let mut rows = Vec::new();
    let Some(registry) = QUEUES.get() else {
        return (rows, false);
    };
    let Ok(mut queues) = registry.lock() else {
        return (rows, true);
    };
    queues.retain(|&(conn, stream, kind, direction), observer| {
        if conn != connection {
            return true;
        }
        if closed {
            return false;
        }
        let Some((occupied_slots, capacity)) = observer() else {
            return false;
        };
        if rows.len() < 32 {
            rows.push(QueuePressure {
                stream,
                kind,
                kind_name: super::stream_kind_label(kind),
                direction,
                occupied_slots,
                capacity,
            });
        } else {
            QUEUES_LIMITED.store(true, Ordering::Relaxed);
        }
        true
    });
    (rows, QUEUES_LIMITED.load(Ordering::Relaxed))
}
