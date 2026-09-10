//! Native Zakura fork-aware header sync.

use thiserror::Error;
use zakura_chain::block;

use super::{Frame, ZakuraPeerId, FRAME_HEADER_BYTES};

mod config;
mod error;
mod events;
#[cfg(any(test, feature = "header-fuzz"))]
mod fuzz;
mod pipe;
mod reactor;
mod scheduler;
mod service;
#[cfg(test)]
mod tests;
mod wire;

pub use config::{header_sync_header_bytes_for_network, ZakuraHeaderSyncConfig};
pub use error::HeaderSyncStartError;
#[cfg(any(test, feature = "zakura-testkit"))]
pub use events::HeaderSyncAction;
pub use events::{
    Event, FullStateFrontiers, HeaderPathLease, HeaderPathLeaseResult, HeaderPathPage,
    HeaderPathPageResult, HeaderSyncFatalEvent, HeaderSyncHandle, HeaderSyncMisbehavior,
    HeaderSyncRequestId, HeaderSyncStartup, HeaderTargetAdmissionResult,
    HeaderTargetPreparationResult, VctRepairContextResult,
};
#[cfg(any(test, feature = "header-fuzz"))]
pub use fuzz::{replay_header_pursuit_bytes, HeaderPursuitReplaySummary, NoEffectsProbe};
pub use reactor::spawn_header_sync_reactor;
pub use scheduler::peer_work::{ActiveHeaderRequest, AdvertisedHeaderTarget, HeaderTargetPurpose};
pub use scheduler::retry::{
    BodyRetryEpisode, BodyRetryQueue, RetryJitter, RetryUpdate, SeededRetryJitter,
};
#[cfg(any(test, feature = "zakura-testkit"))]
pub(crate) use service::drive_header_sync_actions;
pub use service::PeerSession;
pub(crate) use service::{HeaderSyncPassthroughService, HeaderSyncService};
pub(crate) use wire::{headers_response_bytes, headers_response_capacity};
pub use wire::{
    AuxSchema, GetHeaders, HeaderEntry, HeaderServingLimits, HeaderSyncCodec,
    HeaderSyncDecodeContext, HeaderSyncMessage, HeaderSyncWireError, Headers, HeadersOutcome,
    HeadersOutcomeCode, Status, TreeAuxRecordV1, DEFAULT_HS_RANGE, MAX_HS_MESSAGE_BYTES,
    MAX_HS_RANGE, MSG_HS_GET_HEADERS, MSG_HS_HEADERS, MSG_HS_HEADERS_OUTCOME, MSG_HS_STATUS,
    TREE_AUX_SCHEMA_V1_BYTES, ZAKURA_HEADER_SYNC_STREAM_VERSION, ZAKURA_STREAM_HEADER_SYNC,
};
