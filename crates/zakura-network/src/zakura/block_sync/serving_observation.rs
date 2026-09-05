//! Request correlation for optional serving-workload captures.

use std::sync::Arc;

use serde::Serialize;
use zakura_jsonl_trace::JsonlTraceEvent;

use super::{BlockRangeRequestId, ZakuraPeerId, ZakuraTrace};
use crate::zakura::trace::{peer_label, BLOCK_SYNC_TABLE};
use crate::zakura::transport::{FrameObservation, FrameObserver};

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
        self.trace
            .emit_event(|| self.row("get_blocks_serving", phase, request_id));
    }

    fn row(
        &self,
        event: &'static str,
        phase: &'static str,
        request_id: Option<BlockRangeRequestId>,
    ) -> ServingEvent {
        ServingEvent {
            event,
            capture_version: 1,
            peer: peer_label(&self.peer),
            session_id: self.session_id,
            message_sequence: self.message_sequence,
            phase,
            request_id: request_id.map(BlockRangeRequestId::get),
        }
    }

    pub(super) fn frame_observer(
        self: &Arc<Self>,
        request_id: Option<BlockRangeRequestId>,
        frame_sequence: u64,
        payload_bytes: u64,
    ) -> Box<dyn FrameObserver> {
        Box::new(ServingFrameObservation {
            request: self.clone(),
            request_id,
            frame_sequence,
            payload_bytes,
            message_type: None,
            write_state: "queued",
        })
    }

    pub(super) fn start_settlement(
        self: &Arc<Self>,
        request_id: Option<BlockRangeRequestId>,
        request_overhead: u64,
        response_cap: u64,
        transferred: u64,
    ) -> SettlementObservation {
        let settlement = SettlementObservation {
            request: self.clone(),
            request_id,
            request_overhead,
            response_cap,
            transferred,
        };
        settlement.emit("release_started");
        settlement
    }
}

/// Must be dropped after the request's resource fields to close the release interval.
#[derive(Debug)]
pub(super) struct SettlementObservation {
    request: Arc<ServingObservation>,
    request_id: Option<BlockRangeRequestId>,
    request_overhead: u64,
    response_cap: u64,
    transferred: u64,
}

#[derive(Serialize)]
struct SettlementEvent {
    #[serde(flatten)]
    request: ServingEvent,
    request_overhead: u64,
    response_cap: u64,
    transferred: u64,
    unused_response_capacity: u64,
}

impl JsonlTraceEvent for SettlementEvent {
    const TABLE: zakura_jsonl_trace::JsonlTraceTable = BLOCK_SYNC_TABLE;
}

impl SettlementObservation {
    fn emit(&self, phase: &'static str) {
        metrics::counter!("sync.block.capture.settlement_events", "phase" => phase).increment(1);
        self.request.trace.emit_event(|| SettlementEvent {
            request: self
                .request
                .row("get_blocks_settlement", phase, self.request_id),
            request_overhead: self.request_overhead,
            response_cap: self.response_cap,
            transferred: self.transferred,
            unused_response_capacity: self.response_cap.saturating_sub(self.transferred),
        });
    }
}

impl Drop for SettlementObservation {
    fn drop(&mut self) {
        self.emit("release_finished");
    }
}

#[derive(Debug)]
struct ServingFrameObservation {
    request: Arc<ServingObservation>,
    request_id: Option<BlockRangeRequestId>,
    frame_sequence: u64,
    payload_bytes: u64,
    message_type: Option<u16>,
    write_state: &'static str,
}

#[derive(Serialize)]
struct ServingFrameEvent {
    #[serde(flatten)]
    request: ServingEvent,
    frame_sequence: u64,
    payload_bytes: u64,
    message_type: Option<u16>,
    write_state: &'static str,
}

impl JsonlTraceEvent for ServingFrameEvent {
    const TABLE: zakura_jsonl_trace::JsonlTraceTable = BLOCK_SYNC_TABLE;
}

impl FrameObserver for ServingFrameObservation {
    fn observe(&mut self, event: FrameObservation) {
        let phase = match event {
            FrameObservation::Queued { message_type } => {
                self.message_type = Some(message_type);
                "queued"
            }
            FrameObservation::WriteStarted => {
                self.write_state = "writing";
                "write_started"
            }
            FrameObservation::WriteReturned => {
                self.write_state = "returned";
                "write_returned"
            }
            FrameObservation::ReleaseStarted => "release_started",
            FrameObservation::ReleaseFinished => "release_finished",
        };
        metrics::counter!("sync.block.capture.frame_events", "phase" => phase).increment(1);
        self.request.trace.emit_event(|| ServingFrameEvent {
            request: self.request.row("get_blocks_frame", phase, self.request_id),
            frame_sequence: self.frame_sequence,
            payload_bytes: self.payload_bytes,
            message_type: self.message_type,
            write_state: self.write_state,
        });
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;
    use zakura_chain::block;
    use zakura_chain::serialization::ZcashDeserialize;
    use zakura_jsonl_trace::JsonlTracer;

    use super::*;
    use crate::zakura::block_sync::{
        serving_regulation::GetBlocksServingRegulator, BlockSyncPeerSession, ZakuraBlockSyncConfig,
    };
    use crate::zakura::transport::worker_framed_channel;

    #[tokio::test]
    async fn service_frames_keep_request_identity_after_the_permit_is_dropped() {
        let (sender, mut observations) = mpsc::channel(32);
        let trace = ZakuraTrace::new(JsonlTracer::new(sender), "test");
        let peer = ZakuraPeerId::new(vec![7; 32]).unwrap();
        let regulator = GetBlocksServingRegulator::new(ZakuraBlockSyncConfig::default());
        let session = regulator.session(peer.clone(), 3);
        let (send, mut receiver) = worker_framed_channel(2);
        let service = BlockSyncPeerSession::for_test_with_session_id(
            peer.clone(),
            3,
            send,
            CancellationToken::new(),
        );
        let (height, bytes) = zakura_test::vectors::MAINNET_BLOCKS.iter().next().unwrap();
        let body = Arc::new(block::Block::zcash_deserialize(*bytes).unwrap());
        let request = session
            .try_retain_input(block::Height(*height), 1)
            .unwrap()
            .with_observation(ServingObservation::for_request(&trace, &peer, 3, 20));
        let mut attempt = session.try_admit(request.count()).unwrap();
        request.observe_admission(&mut attempt);
        let (height, _) = request.into_parts();
        let mut permit = attempt.commit();
        permit.bind_request_id(BlockRangeRequestId::new(30).unwrap());
        service.try_send_regulated_block(body, &mut permit).unwrap();
        service
            .try_send_regulated_blocks_done(height, 1, &mut permit)
            .unwrap();
        let query = permit.query_lease();
        drop(permit);
        assert_eq!(regulator.snapshot().node_active, 1);
        let mut rows = Vec::new();
        while let Ok(event) = observations.try_recv() {
            let row: serde_json::Value = serde_json::from_slice(&event.line).unwrap();
            assert_ne!(row["event"], "get_blocks_settlement");
            rows.push(row);
        }
        // Ledger closure cannot settle a request still retained by its state query.
        drop(query);
        assert_eq!(regulator.snapshot().node_active, 0);
        assert!(regulator.snapshot().node_outstanding > 0);

        receiver
            .recv()
            .await
            .unwrap()
            .write_with(|_| async {})
            .await;
        // The terminal frame is still queued when the transport closes.
        drop(receiver);
        assert_eq!(regulator.snapshot().node_outstanding, 0);

        let mut frames = [Vec::new(), Vec::new()];
        while let Ok(event) = observations.try_recv() {
            rows.push(serde_json::from_slice(&event.line).unwrap());
        }
        let settlements: Vec<_> = rows
            .iter()
            .filter(|row| row["event"] == "get_blocks_settlement")
            .collect();
        assert_eq!(settlements.len(), 2);
        assert_eq!(settlements[0]["phase"], "release_started");
        assert_eq!(settlements[1]["phase"], "release_finished");
        let queued_bytes: u64 = rows
            .iter()
            .filter(|row| row["event"] == "get_blocks_frame" && row["phase"] == "queued")
            .map(|row| row["payload_bytes"].as_u64().unwrap())
            .sum();
        for row in settlements {
            assert_eq!(row["request_id"], 30);
            assert_eq!(row["transferred"], queued_bytes);
            assert_eq!(
                row["response_cap"].as_u64().unwrap(),
                queued_bytes + row["unused_response_capacity"].as_u64().unwrap()
            );
        }
        for row in rows {
            if row["event"] != "get_blocks_frame" {
                continue;
            }
            assert_eq!(row["session_id"], 3);
            assert_eq!(row["message_sequence"], 20);
            assert_eq!(row["request_id"], 30);
            assert!(row["payload_bytes"].as_u64().unwrap() > 0);
            assert!(row["message_type"].as_u64().is_some());
            let sequence = usize::try_from(row["frame_sequence"].as_u64().unwrap()).unwrap();
            frames[sequence].push(row["phase"].as_str().unwrap().to_owned());
        }
        assert_eq!(
            frames[0],
            [
                "queued",
                "write_started",
                "write_returned",
                "release_started",
                "release_finished"
            ]
        );
        assert_eq!(frames[1], ["queued", "release_started", "release_finished"]);
    }

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
        assert_eq!(rows.len(), 12);
        for (sequence, events) in [20, 21].into_iter().zip(rows.as_chunks::<6>().0) {
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
                    "request_bound",
                    "release_started",
                    "release_finished"
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
