//! Bounded discovery rounds linked to the hashes they considered.
use super::*;

/// Query slots identify local requests, not remote peers. Peer routing can retry a query.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct DiscoveryInfo {
    /// Unique round identity within this node run.
    pub round: u64,
    /// Zero-based query slot, absent on round-wide milestones.
    pub request: Option<u32>,
    /// Query results not yet consumed by the round, not necessarily still on the network.
    pub pending: Option<u32>,
    /// Full response or dispatch-list length, including hashes beyond the diagnostic cap.
    pub hashes: Option<u32>,
}

/// No-op without recording. Drop marks early returns and cancellation explicitly.
pub struct Discovery {
    info: Option<DiscoveryInfo>,
    finished: bool,
}

fn count(n: usize) -> u32 {
    u32::try_from(n).unwrap_or(u32::MAX)
}

impl Discovery {
    /// Start an obtain-tips round. No peer or block data is retained by the guard.
    pub fn new(queries: usize) -> Self {
        let scope = Self {
            info: RECORDER.get().filter(|_| enabled()).map(|r| DiscoveryInfo {
                round: r.lifecycle.operations.fetch_add(1, Ordering::Relaxed),
                request: None,
                pending: None,
                hashes: None,
            }),
            finished: false,
        };
        scope.mark(Phase::SyncRoundStarted, Some(queries), None);
        scope
    }

    /// Start one query before waiting for network-service readiness.
    pub fn request(&self, slot: usize) -> Self {
        let mut scope = self.query(slot);
        scope.finished = false;
        scope.mark(Phase::SyncRequestReadyWait, None, None);
        scope
    }

    /// Label processing of a query result without starting another request.
    pub fn query(&self, slot: usize) -> Self {
        Self {
            info: self.info.map(|mut info| {
                info.request = Some(count(slot));
                info
            }),
            finished: true,
        }
    }

    fn record(&self, hash: [u8; 32], phase: Phase, pending: Option<usize>, hashes: Option<usize>) {
        if let Some(mut info) = self.info {
            info.pending = pending.map(count);
            info.hashes = hashes.map(count);
            emit(Record {
                hash,
                operation: info.round,
                source: None,
                route: Route::Sync,
                phase,
                at_us: 0,
                discovery: Some(info),
            });
        }
    }

    /// Round/query event with no associated block. The collector links it by round ID.
    pub fn mark(&self, phase: Phase, pending: Option<usize>, hashes: Option<usize>) {
        self.record([0; 32], phase, pending, hashes);
    }

    /// Record only the first 64 hashes. The full count exposes bounded diagnostic coverage.
    pub fn hashes(&self, phase: Phase, hashes: &[[u8; 32]]) {
        if self.info.is_none() {
            return;
        }
        self.mark(phase, None, Some(hashes.len()));
        for hash in hashes.iter().take(64) {
            self.record(*hash, phase, None, Some(hashes.len()));
        }
    }

    /// Avoid allocating a second list just for diagnostic hashes.
    pub fn hash_iter(&self, phase: Phase, hashes: impl ExactSizeIterator<Item = [u8; 32]>) {
        if self.info.is_none() {
            return;
        }
        let len = hashes.len();
        self.mark(phase, None, Some(len));
        for hash in hashes.take(64) {
            self.record(hash, phase, None, Some(len));
        }
    }

    /// End this scope without emitting an incomplete marker on drop.
    pub fn finish(&mut self, phase: Phase, pending: Option<usize>, hashes: Option<usize>) {
        self.mark(phase, pending, hashes);
        self.finished = true;
    }
}

impl Drop for Discovery {
    fn drop(&mut self) {
        if !self.finished {
            let phase = if self.info.is_some_and(|i| i.request.is_some()) {
                Phase::SyncRequestIncomplete
            } else {
                Phase::SyncRoundIncomplete
            };
            self.mark(phase, None, None);
        }
    }
}
