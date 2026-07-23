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
mod wire;

pub use config::{header_sync_header_bytes_for_network, ZakuraHeaderSyncConfig};
pub use error::HeaderSyncStartError;
pub use events::{
    FullStateFrontiers, HeaderPathLease, HeaderPathLeaseResult, HeaderPathPage,
    HeaderPathPageResult, HeaderSyncAction, HeaderSyncEvent, HeaderSyncHandle,
    HeaderSyncMisbehavior, HeaderSyncRequestId, HeaderSyncStartup, HeaderTargetAdmissionResult,
    HeaderTargetPreparationResult, VctRepairContextResult,
};
#[cfg(any(test, feature = "header-fuzz"))]
pub use fuzz::{replay_header_pursuit_bytes, HeaderPursuitReplaySummary, NoEffectsProbe};
pub use reactor::spawn_header_sync_reactor;
pub use scheduler::coverage::BranchRange;
pub use scheduler::peer_work::{ActiveHeaderRequest, AdvertisedHeaderTarget};
pub use scheduler::repair::{RepairPhase, RepairTaskError, VctRepairQueue, VctRepairTask};
pub use scheduler::retry::{
    BodyRetryEpisode, BodyRetryQueue, RetryJitter, RetryUpdate, SeededRetryJitter,
};
pub use service::HeaderSyncPeerSession;
pub(crate) use service::{
    drive_header_sync_actions, HeaderSyncPassthroughService, HeaderSyncService,
};
pub use wire::{
    AuxSchema, GetHeaders, HeaderEntry, HeaderServingLimits, HeaderSyncCodec,
    HeaderSyncDecodeContext, HeaderSyncMessage, HeaderSyncWireError, Headers, HeadersOutcome,
    HeadersOutcomeCode, Status, TreeAuxRecordV1, DEFAULT_HS_RANGE, MAX_HS_MESSAGE_BYTES,
    MAX_HS_RANGE, MSG_HS_GET_HEADERS, MSG_HS_HEADERS, MSG_HS_HEADERS_OUTCOME, MSG_HS_STATUS,
    TREE_AUX_SCHEMA_V1_BYTES, ZAKURA_HEADER_SYNC_STREAM_VERSION, ZAKURA_STREAM_HEADER_SYNC,
};

#[cfg(test)]
mod ownership_tests {
    fn declaration<'a>(source: &'a str, marker: &str) -> &'a str {
        let start = source
            .find(marker)
            .unwrap_or_else(|| panic!("ownership inventory cannot find `{marker}`"));
        let rest = &source[start..];
        let end = rest
            .find("\n}")
            .unwrap_or_else(|| panic!("ownership inventory cannot bound `{marker}`"));
        &rest[..end]
    }

    fn requires(source: &str, marker: &str, fields: &[&str]) {
        let declaration = declaration(source, marker);
        for field in fields {
            assert!(
                declaration.contains(field),
                "`{marker}` must retain branch-sensitive field `{field}`"
            );
        }
    }

    #[test]
    fn all_work_types_require_generation_and_branch() {
        let ids = include_str!("../../../../zakura-header-chain/src/ids.rs");
        requires(
            ids,
            "pub struct BranchId",
            &["anchor_hash", "target_tip_hash"],
        );
        requires(
            ids,
            "pub struct WorkScope",
            &["state_version", "header_generation", "branch"],
        );
        requires(
            ids,
            "pub struct WorkOwner",
            &[
                "state_version",
                "header_generation",
                "branch",
                "session_id",
                "request_id",
            ],
        );

        let peer_work = include_str!("scheduler/peer_work.rs");
        requires(
            peer_work,
            "pub struct AdvertisedHeaderTarget",
            &["scope: WorkScope"],
        );
        requires(
            peer_work,
            "pub struct ActiveHeaderRequest",
            &["target: AdvertisedHeaderTarget", "owner: WorkOwner"],
        );

        let coverage = include_str!("scheduler/coverage.rs");
        requires(
            coverage,
            "struct CoverageKey",
            &["generation: HeaderGeneration", "branch: BranchId"],
        );
        let retry = include_str!("scheduler/retry.rs");
        for marker in ["pub struct BodyRetryEpisode", "struct BodyRetryKey"] {
            requires(
                retry,
                marker,
                &["generation: HeaderGeneration", "branch: BranchId"],
            );
        }
        let repair = include_str!("scheduler/repair.rs");
        requires(
            repair,
            "pub struct VctRepairTask",
            &["owner: WorkOwner", "range: BranchRange"],
        );

        let service = include_str!("service.rs");
        requires(
            service,
            "struct ExpectedHeadersResponse",
            &[
                "request_id: HeaderSyncRequestId",
                "scope: zakura_header_chain::WorkScope",
            ],
        );
        let events = include_str!("events.rs");
        requires(
            events,
            "pub struct HeaderPathLease",
            &["scope: zakura_header_chain::WorkScope"],
        );
        requires(
            events,
            "pub struct HeaderPathPage",
            &["scope: zakura_header_chain::WorkScope"],
        );
        let response_event = events
            .split_once("SessionResponse {")
            .and_then(|(_, rest)| rest.split_once("HeaderLocatorReady {"))
            .map(|(body, _)| body)
            .expect("the response completion has one inspectable event block");
        assert!(response_event.contains("scope: zakura_header_chain::WorkScope"));

        let reactor = include_str!("reactor.rs");
        let served = declaration(reactor, "enum ServedPathState");
        assert_eq!(
            served
                .matches("scope: zakura_header_chain::WorkScope")
                .count(),
            2,
            "acquiring and active retained-path work must both carry scope"
        );

        let state = include_str!("../../../../zakura-state/src/header_chain.rs");
        for marker in [
            "pub struct RetainedPathLease",
            "pub struct RetainedPathPage",
        ] {
            requires(state, marker, &["scope: WorkScope"]);
        }
        let transitions = include_str!("../../../../zakura-header-chain/src/transition/types.rs");
        requires(transitions, "pub struct AuxDelivery", &["owner: WorkOwner"]);
        requires(
            transitions,
            "pub struct InsertHeaders",
            &["owner: WorkOwner"],
        );

        let block_work = include_str!("../block_sync/work_queue.rs");
        requires(
            block_work,
            "struct WorkItem",
            &[
                "scope: zakura_header_chain::WorkScope",
                "owner: Option<zakura_header_chain::WorkOwner>",
            ],
        );
        let block_request = include_str!("../block_sync/request.rs");
        requires(
            block_request,
            "struct BlockRangeRequest",
            &["owner: zakura_header_chain::WorkOwner"],
        );
        let block_reactor = include_str!("../block_sync/reactor.rs");
        requires(
            block_reactor,
            "struct PendingNeededQuery",
            &["scope: zakura_header_chain::WorkScope"],
        );
        let reorder = include_str!("../block_sync/reorder.rs");
        for marker in ["struct DrainedBlock", "struct BufferedBlock"] {
            requires(reorder, marker, &["owner: zakura_header_chain::WorkOwner"]);
        }
        let sequencer = include_str!("../block_sync/sequencer.rs");
        for marker in [
            "struct ApplyingBlock",
            "struct SubmitItem",
            "struct InFlightSubmission",
        ] {
            requires(
                sequencer,
                marker,
                &["owner: zakura_header_chain::WorkOwner"],
            );
        }
        let driver =
            include_str!("../../../../zakurad/src/commands/start/zakura/block_sync_driver.rs");
        requires(
            driver,
            "struct PendingBlockApply",
            &["owner: zakura_header_chain::WorkOwner"],
        );
    }
}
