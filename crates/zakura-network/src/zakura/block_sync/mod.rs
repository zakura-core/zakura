//! Native Zakura block-sync stream messages and service scaffold.
//!
//! New to this subsystem? Start in [`pipe`] — it is the "start here" map of how a
//! peer's stream-6 frames flow through the per-peer routine, the work queue, the
//! commit pipeline, the registry, and the reactor.

use std::{collections::BTreeMap, io, sync::Arc, time::Duration};

use byteorder::WriteBytesExt;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use zakura_chain::{
    block,
    serialization::{SerializationError, ZcashDeserialize, ZcashSerialize},
};
use zakura_rt::{
    collections::{HashMap, HashSet},
    sync::{mpsc, watch, CancellationToken, Mutex as StdMutex},
    task::JoinHandle,
    time::{self, Instant},
};

use super::{
    trace::block_sync_trace as bs_trace, Frame, ServicePeerDirection, ServicePeerLimits,
    ZakuraPeerId, ZakuraTrace,
};

/// Reports the event that `$event` builds to the observer in `$slot`, an
/// [`ObserverSlot`](block_sync_observe::ObserverSlot), if one is installed.
/// `$event` is evaluated only then.
///
/// Every call site is itself compiled only in test and simulator builds.
#[cfg(any(test, zakura_sim))]
macro_rules! observe {
    ($slot:expr, $event:expr) => {
        if let Some(observer) = &$slot.0 {
            observer.observe($event)
        }
    };
}

mod admission;
mod bbr;
#[cfg(feature = "internal-bench")]
mod bench;
#[cfg(any(test, zakura_sim))]
pub mod block_sync_observe;
mod config;
mod error;
mod events;
mod peer_registry;
mod peer_routine;
mod pipe;
mod reactor;
mod regulated;
mod reorder;
mod request;
mod sequencer;
mod sequencer_task;
mod service;
mod source;
mod state;
mod status;
#[cfg(test)]
mod tests;
mod trace;
mod wire;
mod work_queue;

// The remaining consumers of these two internal constants are the same-crate
// fuzz-testkit test modules; the re-exports only exist for test builds.
#[cfg(test)]
pub(crate) use admission::DESERIALIZED_MEM_FACTOR;
#[cfg(feature = "internal-bench")]
pub use bench::{
    spawn_bench_sequencer, BenchBodyFeeder, BenchCommitter, BenchSequencerHandle, BenchSubmissions,
    BenchSubmit, SequencerProgress,
};
#[cfg(test)]
pub(crate) use config::MIN_BS_CHECKPOINT_SUBMITTED_BLOCK_APPLIES;
pub use config::{BlockSyncStatus, CwndUnit, ZakuraBlockSyncConfig, MAX_BS_RESPONSE_BYTES};
pub use error::{BlockSyncStartError, BlockSyncWireError};
pub use events::{
    BlockApplyOutcome, BlockApplyResult, BlockApplyToken, BlockSyncAction, BlockSyncBlockMeta,
    BlockSyncEvent, BlockSyncMisbehavior,
};
#[cfg(any(test, zakura_sim, feature = "zakura-testkit"))]
pub use peer_routine::routine_accepts_body;
pub use reactor::spawn_block_sync_reactor;
pub use request::BlockSizeEstimate;
#[cfg(test)]
pub(crate) use service::MAX_BS_FRAME_BYTES;
#[cfg(any(test, zakura_sim))]
pub use service::{block_sync_serving_limit, block_sync_streams};
pub use service::{BlockSyncPeerSession, BlockSyncService};
#[cfg(any(test, zakura_sim, feature = "zakura-testkit"))]
pub use source::DetachedReadSession;
pub use source::{BlockRangeRead, BlockRangeReadLease, BlockRangeReadResult, BlockRangeSource};
#[cfg(any(test, zakura_sim, feature = "zakura-testkit"))]
pub use state::DetachedHandleEvents;
pub use state::{BlockSyncFrontiers, BlockSyncHandle, BlockSyncStartup};
#[cfg(any(test, zakura_sim, feature = "zakura-testkit"))]
pub use wire::decode_block_payload_for_test;
pub use wire::{
    BlockSyncMessage, MAX_BS_BLOCKS_PER_REQUEST, MAX_BS_MESSAGE_BYTES, MSG_BS_BLOCK,
    MSG_BS_BLOCKS_DONE, MSG_BS_GET_BLOCKS, MSG_BS_RANGE_UNAVAILABLE, MSG_BS_STATUS,
    ZAKURA_CAP_BLOCK_SYNC_REGULATED, ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION,
    ZAKURA_STREAM_BLOCK_SYNC,
};

/// The current UTC time as chrono's type. Reads `zakura_rt::time::utc_now`,
/// so a simulated run sees the scenario clock.
fn utc_now() -> chrono::DateTime<chrono::Utc> {
    time::utc_now().into()
}

/// A header-chain [`Clock`](zakura_header_chain::Clock) that reads [`utc_now`], so
/// body-retry deadlines and their conversion to `Instant`s share one clock.
#[derive(Copy, Clone, Debug, Default)]
struct BlockSyncClock;

impl zakura_header_chain::Clock for BlockSyncClock {
    /// Returns [`utc_now`].
    fn now(&self) -> chrono::DateTime<chrono::Utc> {
        utc_now()
    }
}

#[cfg(test)]
fn test_work_scope() -> zakura_header_chain::BodyWorkAuthority {
    zakura_header_chain::BodyWorkAuthority {
        header: zakura_header_chain::HeaderWorkAuthority {
            header_generation: zakura_header_chain::HeaderGeneration::new(2),
            branch: zakura_header_chain::BranchId::new(block::Hash([4; 32]), block::Hash([5; 32])),
        },
        verified_generation: zakura_header_chain::VerifiedGeneration::new(3),
        body_work_epoch: zakura_header_chain::BodyWorkEpoch::default(),
    }
}

#[cfg(test)]
fn test_work_owner() -> zakura_header_chain::BodyWorkOwner {
    test_work_scope().bind(6, std::num::NonZeroU64::new(7).expect("seven is nonzero"))
}

#[cfg(test)]
pub(crate) fn test_block_apply_outcome(result: BlockApplyResult) -> BlockApplyOutcome {
    let evidence = zakura_header_chain::EvidenceId::from_digest([0xa5; 32]);
    let hash = zakura_chain::block::Hash([0x5a; 32]);
    match result {
        BlockApplyResult::Committed => {
            BlockApplyOutcome::committed(zakura_header_chain::VerifiedBodyEvidence {
                hash,
                evidence,
            })
        }
        BlockApplyResult::Duplicate => {
            BlockApplyOutcome::duplicate(zakura_header_chain::VerifiedBodyEvidence {
                hash,
                evidence,
            })
        }
        BlockApplyResult::Rejected => {
            BlockApplyOutcome::consensus_invalid(zakura_header_chain::ConsensusBodyInvalid {
                hash,
                evidence,
                rule: zakura_header_chain::BodyRuleId::new("test.consensus_invalid"),
                source: zakura_header_chain::SourceId::from_digest([0x3c; 32]),
            })
        }
        BlockApplyResult::Unavailable => {
            BlockApplyOutcome::retryable(zakura_header_chain::TransientBodyFailure {
                hash,
                evidence,
                kind: zakura_header_chain::TransientBodyFailureKind::VerifierUnavailable,
                availability: zakura_header_chain::BodyUnavailableSummary::default(),
            })
        }
        BlockApplyResult::TimedOut => {
            BlockApplyOutcome::retryable(zakura_header_chain::TransientBodyFailure {
                hash,
                evidence,
                kind: zakura_header_chain::TransientBodyFailureKind::Timeout,
                availability: zakura_header_chain::BodyUnavailableSummary::default(),
            })
        }
    }
}
