//! Best-effort ingress milestones. No blocks, addresses, or error strings are retained.
use super::*;
mod discovery;
pub use discovery::{Discovery, DiscoveryInfo};
use std::{
    collections::hash_map::RandomState,
    hash::{BuildHasher, Hash},
};

/// Route that observed a block, not a claim that other routes did not see it.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Route {
    /// Inbound gossip downloader.
    Gossip,
    /// Legacy chain sync downloader.
    Sync,
    /// Legacy peer connection boundary.
    LegacyPeer,
    /// State dependency queue.
    State,
    /// Consensus router entry.
    Router,
    /// Header-driven block sync.
    BlockSync,
}

/// Fixed vocabulary keeps producer records bounded and avoids formatting hot-path errors.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    /// Downloader was asked to consider this hash.
    Discovered,
    /// An existing download or verification owns this hash.
    AlreadyQueued,
    /// The global inbound queue rejected this announcement.
    QueueFull,
    /// The per-source concurrency limit rejected this announcement.
    SourceFull,
    /// The download task first ran.
    TaskStarted,
    /// Existing-body lookup returned.
    StateLookupDone,
    /// Waiting for network service readiness.
    NetworkReadyWait,
    /// Request submitted to the network service, before routing and transport.
    NetworkRequest,
    /// Network readiness or body request failed.
    NetworkFailed,
    /// The downloader received the body response.
    BodyReceived,
    /// Waiting for the source ordering lock.
    SourceWait,
    /// Acquired the source ordering lock.
    SourceAcquired,
    /// Body ready for the verifier, including service readiness wait.
    VerifierReadyWait,
    /// Verifier service ready and request being submitted.
    VerifierSubmitted,
    /// Request entered the consensus router.
    RouterEntered,
    /// State cannot yet hand this block to the writer because its parent is unavailable.
    ParentUnavailable,
    /// State handed this block to the writer queue.
    WriterEnqueued,
    /// Download and verification returned success.
    Success,
    /// Download or verification returned an error.
    Failed,
    /// Task ended without a recorded result, including early rejection or cancellation.
    Incomplete,
    /// Selected peer connection began sending the request.
    PeerRequest,
    /// Request flushed to the local transport, not proof of remote receipt.
    PeerRequestFlushed,
    /// Decoded body delivered to the connection task.
    PeerBody,
    /// Decoded block inventory delivered to the connection task.
    PeerAnnouncement,
    /// Selected connection timed out waiting for the response.
    PeerTimeout,
    /// Peer explicitly reported the requested block unavailable.
    PeerNotFound,
    /// Inventory was consumed as a FindBlocks response.
    InventorySyncResponse,
    /// A singleton block inventory was routed to inbound gossip.
    InventoryGossip,
    /// Unsolicited inventory contained multiple blocks.
    InventoryIgnoredMultiBlock,
    /// Unsolicited inventory mixed blocks with other item types.
    InventoryIgnoredMixed,
    /// An obtain-tips discovery round started.
    SyncRoundStarted,
    /// Waiting for readiness to submit one discovery query.
    SyncRequestReadyWait,
    /// Discovery query submitted, before peer routing.
    SyncRequestSubmitted,
    /// Discovery query returned successfully.
    SyncRequestCompleted,
    /// Discovery query hit its service timeout.
    SyncRequestTimeout,
    /// Discovery query failed, without retaining its error text.
    SyncRequestFailed,
    /// Discovery query was cancelled or panicked.
    SyncRequestIncomplete,
    /// The round consumed one query result; pending counts unconsumed queries.
    SyncResponseHandled,
    /// Hashes returned by a discovery query, before filtering.
    SyncHashesReceived,
    /// Last response hash discarded by the legacy compatibility rule.
    SyncTrailingHashDiscarded,
    /// Discovery response exceeded its existing size guard.
    SyncResponseOversized,
    /// Response contained duplicate hashes.
    SyncResponseDuplicate,
    /// Response contained an already-known hash after an unknown hash.
    SyncResponseKnownSuffix,
    /// All remaining response hashes were already known.
    SyncHashesKnown,
    /// Hash skipped as an already-known response prefix.
    SyncHashKnownPrefix,
    /// Hash added to the combined download list.
    SyncHashAccepted,
    /// All discovery query results have been consumed.
    SyncFanoutComplete,
    /// Hash committed by another route before dispatch.
    SyncHashAlreadyCommitted,
    /// Combined list handed to the existing download admission path.
    SyncDownloadsDispatch,
    /// Discovery round returned successfully.
    SyncRoundCompleted,
    /// Discovery round ended early, including errors or cancellation.
    SyncRoundIncomplete,
    /// Selected connection failed while a block request was pending.
    PeerFailed,
}

/// A timestamp on the run's monotonic clock. Operation zero denotes an independent observation.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Record {
    /// Block hash in internal byte order.
    pub hash: [u8; 32],
    /// Download identity within the run, or zero for independent observations.
    pub operation: u64,
    /// Opaque run-local source key, never a raw address.
    pub source: Option<u64>,
    /// Component that observed the milestone.
    pub route: Route,
    /// Observed transition.
    pub phase: Phase,
    /// Microseconds since the recorder monotonic epoch.
    pub at_us: u64,
    /// Optional bounded sync-discovery context. Older records omit it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discovery: Option<DiscoveryInfo>,
}

/// At most 256 ingress records per monotonic second, independently of span capacity.
/// A try-lock never stalls validation when another producer is recording.
pub(super) struct Limiter {
    window: Mutex<(u64, u32)>,
    operations: AtomicU64,
    sources: RandomState,
}
impl Default for Limiter {
    fn default() -> Self {
        Self {
            window: Mutex::new((0, 0)),
            operations: AtomicU64::new(1),
            sources: RandomState::new(),
        }
    }
}
impl Limiter {
    fn admit(&self, now: u64) -> bool {
        let Ok(mut window) = self.window.try_lock() else {
            return false;
        };
        let second = now / 1_000_000;
        if window.0 != second {
            *window = (second, 0);
        }
        if window.1 >= 256 {
            return false;
        }
        window.1 += 1;
        true
    }
}

/// Run-local opaque source identifier. It cannot be used to recover a peer address.
pub fn source(key: &impl Hash) -> Option<u64> {
    RECORDER
        .get()
        .filter(|_| enabled())
        .map(|r| r.lifecycle.sources.hash_one(key))
}

/// One admitted download attempt. Dropping it before a result records an incomplete attempt.
/// This includes cancellation and early rejection; the last milestone identifies the phase.
pub struct Download {
    record: Option<Record>,
    finished: bool,
}
impl Download {
    /// Begin one download attempt without retaining any block data.
    pub fn new(hash: [u8; 32], route: Route, source: Option<u64>) -> Self {
        let record = RECORDER.get().filter(|_| enabled()).map(|r| Record {
            hash,
            route,
            source,
            operation: r.lifecycle.operations.fetch_add(1, Ordering::Relaxed),
            phase: Phase::Discovered,
            at_us: 0,
            discovery: None,
        });
        let trace = Self {
            record,
            finished: false,
        };
        trace.mark(Phase::Discovered);
        trace
    }
    /// Record a fixed-size milestone without waiting on the exporter.
    pub fn mark(&self, phase: Phase) {
        if let Some(mut record) = self.record {
            record.phase = phase;
            emit(record);
        }
    }
    /// Record the caller result and suppress the incomplete marker.
    pub fn finish(mut self, success: bool) {
        self.mark(if success {
            Phase::Success
        } else {
            Phase::Failed
        });
        self.finished = true;
    }
}
impl Drop for Download {
    fn drop(&mut self) {
        if !self.finished {
            self.mark(Phase::Incomplete);
        }
    }
}

/// A milestone outside a download task, correlated by exact block hash and run.
pub fn observe(hash: [u8; 32], route: Route, phase: Phase, source: Option<u64>) {
    if enabled() {
        emit(Record {
            hash,
            route,
            phase,
            source,
            operation: 0,
            at_us: 0,
            discovery: None,
        });
    }
}
fn emit(mut record: Record) {
    if let Some(r) = RECORDER.get().filter(|_| enabled()) {
        record.at_us = r.now();
        if r.lifecycle.admit(record.at_us) {
            r.emit(Event::Lifecycle(record), false);
        } else {
            r.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_and_nonblocking() {
        let limiter = Limiter::default();
        for _ in 0..256 {
            assert!(limiter.admit(1));
        }
        assert!(!limiter.admit(2));
        assert!(limiter.admit(1_000_000));
        let _lock = limiter.window.lock().unwrap();
        assert!(!limiter.admit(2_000_000));
    }
    #[test]
    fn historical_milestones_without_discovery_still_decode() {
        let old = serde_json::json!({
            "hash": vec![4; 32], "operation": 1, "source": null,
            "route": "legacy_peer", "phase": "peer_announcement", "at_us": 25
        });
        let decoded: Record = serde_json::from_value(old).unwrap();
        assert!(decoded.discovery.is_none());
    }
    #[test]
    fn record_roundtrips_without_peer_addresses() {
        let event = Event::Lifecycle(Record {
            hash: [4; 32],
            operation: 7,
            source: Some(42),
            route: Route::Gossip,
            phase: Phase::ParentUnavailable,
            at_us: 10,
            discovery: None,
        });
        let encoded = serde_json::to_string(&event).unwrap();
        assert!(matches!(
            serde_json::from_str::<Event>(&encoded).unwrap(),
            Event::Lifecycle(Record { operation: 7, .. })
        ));
    }
}
