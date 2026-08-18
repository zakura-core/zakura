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
    parameters::{
        testnet::{ConfiguredActivationHeights, RegtestParameters},
        Network,
    },
    serialization::ZcashDeserializeInto,
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

fn stage_full_state_canonical_hash(
    store: &HeaderChainStore,
    batch: &mut DiskWriteBatch,
    frontier: Frontier,
) {
    store
        .put_raw(
            batch,
            "hash_by_height",
            frontier.height.as_bytes(),
            frontier.hash.0,
        )
        .expect("the full-state canonical hash stages");
}

fn fixture() -> (EngineConfig, HeaderNode, EngineMetadata) {
    let network = Network::new_regtest(RegtestParameters::default());
    let block = regtest_genesis_block();
    fixture_for_network(network, block)
}

fn mainnet_fixture() -> (EngineConfig, HeaderNode, EngineMetadata) {
    let block = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
        .zcash_deserialize_into()
        .expect("the Mainnet genesis fixture deserializes");
    fixture_for_network(Network::Mainnet, block)
}

fn fixture_for_network(
    network: Network,
    block: Arc<block::Block>,
) -> (EngineConfig, HeaderNode, EngineMetadata) {
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
    .expect("the engine configuration is coherent");
    let work = block
        .header
        .difficulty_threshold
        .to_work()
        .expect("the genesis target has exact work");
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
        disk_format: HeaderChainDiskVersion::CURRENT,
        mode: EngineMode::Integrated,
        network_id: config.network().kind(),
        network_policy_digest: config.network_policy_digest(),
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
        .load_header_nodes()
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
    let durable_engine = load_transition_engine(&runtime.store)
        .expect("the durable auxiliary rows pass recovery validation");
    let mut durable_headers = HashSet::new();
    for delivery in runtime
        .store
        .load_aux_deliveries()
        .expect("the durable auxiliary deliveries are readable")
    {
        durable_headers.insert(delivery.delivery().header_hash);
    }
    for hash in durable_headers {
        assert_eq!(
            engine.aux_deliveries(hash),
            durable_engine.aux_deliveries(hash)
        );
    }
}
