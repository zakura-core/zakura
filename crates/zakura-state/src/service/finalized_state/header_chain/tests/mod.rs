//! Tests for the durable header-chain store and runtime.

mod crash;
mod runtime;
mod startup;
mod transition;

use std::{
    num::{NonZeroU64, NonZeroUsize},
    sync::atomic::{AtomicBool, Ordering},
};

use super::*;
use crate::{
    constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
    service::finalized_state::{
        zakura_db::block::ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT, STATE_COLUMN_FAMILIES_IN_CODE,
    },
    service::{
        non_finalized_state::NonFinalizedState,
        write::{PreparedFullStateTransition, PreparedFullStateTransitionError},
    },
    Config,
};
use zakura_chain::{
    block::genesis::regtest_genesis_block,
    parameters::{testnet::RegtestParameters, Network},
};
use zakura_header_chain::{
    AlarmSet, BodyCommitmentKind, BodyEvidence, BodyPayloadMismatch, BodyRuleId,
    BodyUnavailableSummary, BodyValidationState, ChainScore, CheckpointSet, EligibilityReason,
    EngineConfig, EngineMode, FinalityEpoch, FrontierSet, FullStateEvidenceAuthority,
    HeaderBatchInput, HeaderChainDiskVersion, HeaderGeneration, HeaderRules, HeaderValidationState,
    InsertHeaders, OperatorInvalidate, OperatorInvalidationId, OperatorReconsider, SourceId,
    StateVersion, SuffixWork, SystemClock, TargetCompletion, TransientBodyFailure,
    TransientBodyFailureKind, TransitionEvent, TrustedAnchor, VerifiedBodyEvidence,
    VerifiedChainChanged, VerifiedChangeCause, VerifiedGeneration, WorkCoordinate,
};

fn header_owner(
    snapshot: &zakura_header_chain::EngineSnapshot,
    target: block::Hash,
    session_id: u64,
    request_id: u64,
) -> zakura_header_chain::HeaderSyncWorkOwner {
    zakura_header_chain::HeaderWorkAuthority::for_target(snapshot, target)
        .bind(
            session_id,
            NonZeroU64::new(request_id).expect("fixture request IDs are nonzero"),
        )
        .into()
}

fn body_owner(
    snapshot: &zakura_header_chain::EngineSnapshot,
    session_id: u64,
    request_id: u64,
) -> zakura_header_chain::BodyWorkOwner {
    zakura_header_chain::BodyWorkAuthority::for_snapshot(snapshot).bind(
        session_id,
        NonZeroU64::new(request_id).expect("fixture request IDs are nonzero"),
    )
}

struct Authority(EvidenceId);

impl FullStateEvidenceAuthority for Authority {
    fn authorizes_full_state(&self, event: &TransitionEvent) -> bool {
        event.idempotency_key() == Some(self.0)
    }

    fn authorizes_scheduler_retry(&self, retry: &zakura_header_chain::OperatorBodyRetry) -> bool {
        retry.evidence == self.0
    }
}

fn open(config: &Config, network: &Network) -> DiskDb {
    DiskDb::new(
        config,
        STATE_DATABASE_KIND,
        &state_database_format_version_in_code(),
        network,
        STATE_COLUMN_FAMILIES_IN_CODE
            .iter()
            .map(ToString::to_string),
        false,
    )
    .expect("the header-chain fixture database opens")
}

fn fixture() -> (EngineConfig, HeaderNode, EngineMetadata) {
    let network = Network::new_regtest(RegtestParameters::default());
    let block = regtest_genesis_block();
    let frontier = Frontier::new(block::Height(0), block.hash());
    let config = EngineConfig::new(
        EngineMode::Integrated,
        network,
        TrustedAnchor {
            frontier,
            header: block.header.clone(),
        },
        CheckpointSet::default(),
    )
    .expect("the regtest engine configuration is coherent");
    let work = block
        .header
        .difficulty_threshold
        .to_work()
        .expect("the regtest genesis target has exact work");
    let node = HeaderNode::from_durable_parts(
        block.header.clone(),
        frontier.hash,
        block.header.previous_block_hash,
        frontier.height,
        work,
        WorkCoordinate::new(frontier.hash, work.as_u256()),
        HeaderValidationState::Valid,
        zakura_header_chain::EligibilityState::default(),
        BodyValidationState::Unknown,
        Vec::new(),
    )
    .expect("the canonical genesis fields agree");
    let metadata = EngineMetadata {
        disk_format: HeaderChainDiskVersion(1),
        mode: EngineMode::Integrated,
        network_id: config.network.kind(),
        anchor_manifest_digest: config.trust_anchor_digest(),
        work_origin: frontier,
        state_version: StateVersion::new(1),
        header_generation: HeaderGeneration::new(1),
        verified_generation: VerifiedGeneration::new(1),
        finality_epoch: FinalityEpoch::new(0),
        headers_only_migration_epoch: None,
        frontiers: FrontierSet {
            finalized: frontier,
            header_best: frontier,
            verified_best: frontier,
        },
        header_best_score: ChainScore::new(SuffixWork::zero(), frontier.hash),
        oldest_retained_height: frontier.height,
        alarms: AlarmSet::default(),
        last_transition: None,
    };
    (config, node, metadata)
}

fn assert_transition_engine_matches_store(runtime: &HeaderChainRuntime) {
    let engine = runtime
        .transition_engine
        .lock()
        .expect("the transition engine mutex is not poisoned");
    let durable_nodes = runtime
        .store
        .all_header_nodes()
        .expect("the durable nodes are readable");
    assert_eq!(engine.graph().header_node_count(), durable_nodes.len());
    for node in durable_nodes {
        assert_eq!(engine.graph().header_node(node.hash), Some(&node));
    }
    assert_eq!(
        engine.metadata(),
        &runtime
            .store
            .metadata()
            .expect("the durable metadata is readable")
    );
    assert_eq!(
        engine.selected_projection(),
        runtime
            .store
            .selected_projection()
            .expect("the durable selected projection is readable")
    );
    assert_eq!(
        engine.verified_projection(),
        runtime
            .store
            .verified_projection()
            .expect("the durable verified projection is readable")
    );
    let mut durable_aux: HashMap<_, Vec<_>> = HashMap::new();
    for delivery in runtime
        .store
        .all_aux_deliveries()
        .expect("the durable auxiliary deliveries are readable")
    {
        durable_aux
            .entry(delivery.header_hash)
            .or_default()
            .push(delivery);
    }
    for deliveries in durable_aux.values_mut() {
        deliveries.sort_unstable_by_key(|delivery| delivery.delivery_id);
    }
    for (hash, deliveries) in durable_aux {
        assert_eq!(engine.aux_deliveries(hash), deliveries);
    }
}
