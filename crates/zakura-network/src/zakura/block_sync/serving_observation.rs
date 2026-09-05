//! Request correlation for optional serving-workload captures.

use std::sync::Arc;

use serde::Serialize;
use zakura_jsonl_trace::JsonlTraceEvent;

use super::{BlockRangeRequestId, ZakuraPeerId, ZakuraTrace};
use crate::zakura::trace::{peer_label, BLOCK_SYNC_TABLE};

/// Trace identity only. Clones must not retain sessions, permits, or budgets.
#[derive(Debug)]
pub(super) struct ServingObservation {
    trace: ZakuraTrace,
    peer: ZakuraPeerId,
    session_id: u64,
    message_sequence: u64,
}

#[derive(Serialize)]
struct ServingEvent {
    event: &'static str,
    capture_version: u64,
    peer: String,
    session_id: u64,
    message_sequence: u64,
    phase: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_id: Option<u64>,
}

impl JsonlTraceEvent for ServingEvent {
    const TABLE: zakura_jsonl_trace::JsonlTraceTable = BLOCK_SYNC_TABLE;
}

impl ServingObservation {
    /// Use the decode emitter's clock throughout this request's serving path.
    pub(super) fn for_request(
        trace: &ZakuraTrace,
        peer: &ZakuraPeerId,
        session_id: u64,
        message_sequence: u64,
    ) -> Option<Arc<Self>> {
        trace.is_enabled().then(|| {
            Arc::new(Self {
                trace: trace.clone(),
                peer: peer.clone(),
                session_id,
                message_sequence,
            })
        })
    }

    /// Count attempts before emission so a full trace queue cannot hide a lost row.
    pub(super) fn emit(&self, phase: &'static str, request_id: Option<BlockRangeRequestId>) {
        metrics::counter!("sync.block.capture.serving_events", "phase" => phase).increment(1);
        self.trace.emit_event(|| ServingEvent {
            event: "get_blocks_serving",
            capture_version: 1,
            peer: peer_label(&self.peer),
            session_id: self.session_id,
            message_sequence: self.message_sequence,
            phase,
            request_id: request_id.map(BlockRangeRequestId::get),
        });
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;
    use zakura_chain::block;
    use zakura_jsonl_trace::JsonlTracer;

    use super::*;
    use crate::zakura::block_sync::{
        serving_regulation::GetBlocksServingRegulator, ZakuraBlockSyncConfig,
    };

    #[test]
    fn repeated_ranges_keep_decode_identity_without_retaining_capacity() {
        let (sender, mut receiver) = mpsc::channel(16);
        let trace = ZakuraTrace::new(JsonlTracer::new(sender), "test");
        let peer = ZakuraPeerId::new(vec![7; 32]).unwrap();
        let regulator = GetBlocksServingRegulator::new(ZakuraBlockSyncConfig::default());
        let session = regulator.session(peer.clone(), 3);
        let mut retained_observations = Vec::new();

        for sequence in [20, 21] {
            let observation = ServingObservation::for_request(&trace, &peer, 3, sequence);
            retained_observations.push(observation.clone());
            let request = session
                .try_retain_input(block::Height(100), 1)
                .unwrap()
                .with_observation(observation);
            let mut attempt = session.try_admit(request.count()).unwrap();
            request.observe_admission(&mut attempt);
            assert_eq!(request.into_parts(), (block::Height(100), 1));
            let mut permit = attempt.commit();
            permit.bind_request_id(BlockRangeRequestId::new(sequence + 10).unwrap());
            assert_eq!(regulator.snapshot().node_active, 1);
            drop(permit);
        }

        // Keeping metadata for a report must not keep serving resources alive.
        assert_eq!(retained_observations.len(), 2);
        let snapshot = regulator.snapshot();
        assert_eq!(snapshot.node_active, 0);
        assert_eq!(snapshot.node_pending, 0);
        assert_eq!(snapshot.node_outstanding, 0);
        assert_eq!(snapshot.peer_outstanding, 0);

        let mut rows = Vec::new();
        while let Ok(event) = receiver.try_recv() {
            rows.push(serde_json::from_slice::<serde_json::Value>(&event.line).unwrap());
        }
        assert_eq!(rows.len(), 8);
        for (sequence, events) in [20, 21].into_iter().zip(rows.as_chunks::<4>().0) {
            let phases: Vec<_> = events
                .iter()
                .map(|row| row["phase"].as_str().unwrap())
                .collect();
            assert_eq!(
                phases,
                [
                    "input_retained",
                    "admission_reserved",
                    "committed",
                    "request_bound"
                ]
            );
            for row in events {
                assert_eq!(row["message_sequence"], sequence);
                assert_eq!(row["session_id"], 3);
                assert_eq!(row["peer"], rows[0]["peer"]);
            }
            assert_eq!(events[3]["request_id"], sequence + 10);
        }
    }
}
