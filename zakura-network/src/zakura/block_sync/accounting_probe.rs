//! Test-only accounting oracle for block-sync retained memory.
//!
//! The fuzz scenarios bound *peaks* sampled from a trace, which can only infer
//! correct accounting from the absence of a visible plateau breach. This probe reads
//! the live ledgers directly and states the conservation laws exactly:
//!
//! - the wire budget, the work queue's maintained reserved counter, and its scanned
//!   ground-truth recomputation all agree;
//! - the active decoded total equals the sum of its per-stage parts;
//! - at full teardown every ledger is zero, with no slack at all.
//!
//! It is built from the same [`RoutineWiring`] handles the production path threads to
//! each peer routine, so a snapshot observes the real counters rather than a copy.
//! Nothing here is compiled into release binaries.

use std::sync::{atomic::Ordering, Arc};

use super::{
    retained_memory::RetainedBodyMemoryTracker, state::RoutineWiring, work_queue::WorkQueue,
};
use crate::zakura::transport::ByteBudget;

/// One instantaneous read of every block-sync accounting ledger.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct BlockSyncAccounting {
    /// Bytes reserved in the shared wire budget.
    pub(crate) wire_budget_reserved: u64,
    /// The work queue's incrementally-maintained reserved-bytes counter.
    pub(crate) work_reserved: u64,
    /// Ground-truth recomputation of the same sum by scanning the queue.
    pub(crate) work_reserved_scanned: u64,
    /// Authoritative retained-memory total (reservations + live body charges).
    pub(crate) retained_total: u64,
    /// Bytes still held by unreconciled in-flight request reservations.
    pub(crate) in_flight_reservation_bytes: u64,
    /// Number of unreconciled in-flight request reservations.
    pub(crate) in_flight_reservation_count: u64,
    /// Wire bytes queued on the sequencer input channel.
    pub(crate) sequencer_input_bytes: u64,
    /// Decoded bytes attributed to bodies queued on the sequencer input channel.
    pub(crate) sequencer_input_decoded: u64,
    /// Decoded bytes attributed to the reorder buffer.
    pub(crate) reorder_decoded: u64,
    /// Decoded bytes attributed to applying bodies, including detached submissions.
    pub(crate) applying_decoded: u64,
    /// The aggregate the reactor publishes for the whole active body pipeline.
    pub(crate) active_pipeline_decoded: u64,
    /// Submitted bodies awaiting a matching completion.
    pub(crate) in_flight_submission_count: u64,
}

/// Reads the live block-sync ledgers.
///
/// Holds its own handles, so it stays readable after the reactor, the peer routines,
/// and the sequencer task have all been dropped — the state in which the zero-slack
/// teardown assertion is meaningful.
#[derive(Clone, Debug)]
pub(crate) struct BlockSyncAccountingProbe {
    budget: ByteBudget,
    work: Arc<WorkQueue>,
    retained_memory: RetainedBodyMemoryTracker,
    sequencer_input_bytes: Arc<std::sync::atomic::AtomicU64>,
    sequencer_input_decoded: Arc<std::sync::atomic::AtomicU64>,
    view: tokio::sync::watch::Receiver<super::sequencer_task::SequencerView>,
}

impl BlockSyncAccountingProbe {
    pub(super) fn new(wiring: &RoutineWiring) -> Self {
        Self {
            budget: wiring.budget.clone(),
            work: wiring.work.clone(),
            retained_memory: wiring.retained_memory.clone(),
            sequencer_input_bytes: wiring.sequencer_input_bytes.clone(),
            sequencer_input_decoded: wiring
                .sequencer_input_decoded_attributed_memory_bytes
                .clone(),
            view: wiring.view.clone(),
        }
    }

    /// Read every ledger once.
    ///
    /// The reads are not one atomic operation, so a snapshot taken while routines are
    /// running can catch a handoff mid-flight. Assert on it only at a quiescent
    /// barrier, where no transition is in progress.
    pub(crate) fn snapshot(&self) -> BlockSyncAccounting {
        let (in_flight_reservation_bytes, in_flight_reservation_count) =
            self.work.in_flight_memory_reservations_for_test();
        // The sequencer task zeroes its view on drop, so post-teardown these read as
        // zero — which is exactly the teardown expectation, not a masked leak: the
        // retained total is owned by the tracker and survives the task.
        let view = *self.view.borrow();

        BlockSyncAccounting {
            wire_budget_reserved: self.budget.reserved(),
            work_reserved: self.work.reserved_bytes(),
            work_reserved_scanned: self.work.reserved_bytes_scanned(),
            retained_total: self.retained_memory.used(),
            in_flight_reservation_bytes,
            in_flight_reservation_count,
            sequencer_input_bytes: self.sequencer_input_bytes.load(Ordering::Relaxed),
            sequencer_input_decoded: self.sequencer_input_decoded.load(Ordering::Relaxed),
            reorder_decoded: view.reorder_decoded_attributed_memory_bytes,
            applying_decoded: view.applying_decoded_attributed_memory_bytes,
            active_pipeline_decoded: view.active_pipeline_decoded_attributed_memory_bytes,
            in_flight_submission_count: view.in_flight_submission_count,
        }
    }

    pub(crate) fn retained_total(&self) -> u64 {
        self.retained_memory.used()
    }
}
