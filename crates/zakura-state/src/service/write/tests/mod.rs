//! Tests for the state write task.

mod attachment_and_vct_aux;
mod deferred;
mod failure_exit;
mod full_state_coherence;
mod selection_and_evidence;

use super::*;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use tokio::sync::{mpsc, oneshot, watch};
use zakura_chain::{
    block::{self, genesis::regtest_genesis_block, Block, ChainHistoryBlockTxAuthCommitmentHash},
    fmt::HexDebug,
    history_tree::HistoryTree,
    parameters::{
        testnet::{ConfiguredActivationHeights, ConfiguredCheckpoints, ParametersBuilder},
        Network, NetworkUpgrade, GENESIS_PREVIOUS_BLOCK_HASH,
    },
    serialization::ZcashDeserializeInto,
    transaction::{arbitrary::transaction_to_fake_v5, Transaction},
    transparent,
    work::{difficulty::ParameterDifficulty as _, equihash},
};

use crate::{
    arbitrary::Prepare,
    service::{
        finalized_state::{
            header_chain::{HeaderChainStore, HeaderChainStoreError},
            FinalizedState, VctAuxiliaryFailureAttribution, VctAuxiliaryWindow,
            VctSuccessorWitness,
        },
        non_finalized_state::NonFinalizedState,
        write::{
            classify_verified_change, commit_contextual_finalization, commit_operator_change,
            receive_until_deferred_deadline, recover_resource_stall, verified_request,
            BlockWriteSender, BlockWriteTaskExit, HeaderChainAttachmentError,
            HeaderChainMaintenance, HeaderChainObservers, HeaderChainWriter,
            NonFinalizedWriteMessage, PreparedFullStateTransition,
        },
        ChainTipSender,
    },
    tests::FakeChainHelper,
    CheckpointVerifiedBlock, Config,
};
use zakura_header_chain::{
    AdjustedDifficulty, AlarmSet, ApplyResult, BodyRuleId, BodyUnavailableSummary,
    BodyValidationState, BodyViolation, ChainScore, CheckpointSet, ConsensusBodyInvalid,
    EngineConfig, EngineMetadata, EngineMode, EngineSnapshot, EvidenceId, FinalityEpoch, Frontier,
    FrontierSet, HeaderBatchInput, HeaderChainDiskVersion, HeaderGeneration, HeaderNode,
    HeaderRules, HeaderValidationState, InsertHeaders, InvalidTransitionEvidence, SourceId,
    StateVersion, SuffixWork, SystemClock, TargetCompletion, TransientBodyFailure,
    TransientBodyFailureKind, TransitionContext, TransitionEvent, TransitionFailure,
    TransitionRequest, TrustedAnchor, VerifiedChangeCause, VerifiedGeneration, VerifiedHeaderRef,
    WorkCoordinate, POW_ADJUSTMENT_BLOCK_SPAN,
};

fn header_owner(
    snapshot: &EngineSnapshot,
    target: block::Hash,
    session_id: u64,
    request_id: u64,
) -> zakura_header_chain::HeaderSyncWorkOwner {
    zakura_header_chain::HeaderWorkAuthority::for_target(snapshot, target)
        .bind(
            session_id,
            std::num::NonZeroU64::new(request_id).expect("fixture request IDs are nonzero"),
        )
        .into()
}

struct TestDeferredMaintenance {
    deadlines: Mutex<VecDeque<chrono::DateTime<chrono::Utc>>>,
    sender: Mutex<Option<mpsc::UnboundedSender<NonFinalizedWriteMessage>>>,
    reevaluations: AtomicUsize,
}

impl HeaderChainMaintenance for TestDeferredMaintenance {
    fn earliest_deferred(
        &self,
    ) -> Result<Option<chrono::DateTime<chrono::Utc>>, HeaderChainStoreError> {
        Ok(self
            .deadlines
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?
            .front()
            .copied())
    }

    fn now(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }

    fn reevaluate_deferred(&self) -> Result<(), HeaderChainStoreError> {
        self.reevaluations.fetch_add(1, Ordering::SeqCst);
        let mut deadlines = self
            .deadlines
            .lock()
            .map_err(|_| HeaderChainStoreError::WriterPoisoned)?;
        deadlines.pop_front();
        if deadlines.is_empty() {
            self.sender
                .lock()
                .map_err(|_| HeaderChainStoreError::WriterPoisoned)?
                .take();
        }
        Ok(())
    }
}

fn header_writer(
    finalized_state: &FinalizedState,
    network: &Network,
    anchor_height: block::Height,
    anchor_block: &Arc<zakura_chain::block::Block>,
) -> HeaderChainWriter {
    let frontier = Frontier::new(anchor_height, anchor_block.hash());
    let config = EngineConfig::new(
        EngineMode::Integrated,
        network.clone(),
        TrustedAnchor {
            frontier,
            header: anchor_block.header.clone(),
        },
        CheckpointSet::default(),
    )
    .expect("the full-state fixture anchor is coherent");
    let work = anchor_block
        .header
        .difficulty_threshold
        .to_work()
        .expect("the fixture target has exact work");
    let anchor = HeaderNode::from_durable_parts(
        anchor_block.header.clone(),
        frontier.hash,
        anchor_block.header.previous_block_hash,
        frontier.height,
        work,
        WorkCoordinate::new(frontier.hash, work.as_u256()),
        HeaderValidationState::Valid,
        Default::default(),
        BodyValidationState::Verified {
            evidence: EvidenceId::from_digest([0x70; 32]),
        },
        Vec::new(),
    )
    .expect("the anchor node fields agree");
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
    let store = HeaderChainStore::new(finalized_state.db.db().clone());
    store
        .initialize(metadata, anchor)
        .expect("the fixture header store initializes");
    let (runtime, _) = store
        .startup(&config)
        .expect("the fixture header store audits");
    HeaderChainWriter::new(runtime, config)
}

#[test]
fn relay_requires_the_staged_winner_to_extend_the_selected_tip() {
    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;
    let finalized = FinalizedState::new(&Config::ephemeral(), &network)
        .expect("the ephemeral finalized state opens");
    let heartwood_height = NetworkUpgrade::Heartwood
        .activation_height(&network)
        .expect("Heartwood activates")
        .0;
    let root: Arc<Block> = network.block_map()[&(heartwood_height - 1)]
        .zcash_deserialize_into()
        .expect("the pre-Heartwood checkpoint block is valid");
    let mut current = NonFinalizedState::new(&network);
    current
        .commit_new_chain(root.clone().prepare(), &finalized.db)
        .expect("the root enters the non-finalized state");
    let activation = root.make_fake_child().set_block_commitment([0; 32]);
    current
        .commit_block(activation.clone().prepare(), &finalized.db)
        .expect("the Heartwood activation block enters the non-finalized state");
    let child_commitment: [u8; 32] = current
        .best_chain()
        .expect("the activation chain exists")
        .history_block_commitment_tree()
        .hash()
        .expect("the activation creates a history root")
        .into();

    let child = activation
        .make_fake_child()
        .set_block_commitment(child_commitment)
        .set_work(100);
    let child_height = child.coinbase_height().expect("the child has a height");
    let child_hash = child.hash();
    let mut staged_child = current.clone();
    staged_child
        .commit_block(child.clone().prepare(), &finalized.db)
        .expect("the tip child enters the staged state");
    assert!(staged_block_extends_selected_tip(
        finalized.db.finalized_tip_hash(),
        &current,
        &staged_child,
        activation.hash(),
        child_height,
        child_hash,
    ));

    current = staged_child;
    let sibling = activation
        .make_fake_child()
        .set_block_commitment(child_commitment)
        .set_work(1_000);
    let sibling_height = sibling.coinbase_height().expect("the sibling has a height");
    let sibling_hash = sibling.hash();
    let mut staged_sibling = current.clone();
    staged_sibling
        .commit_block(sibling.prepare(), &finalized.db)
        .expect("the harder sibling enters the staged state");
    assert_eq!(
        staged_sibling.best_tip(),
        Some((sibling_height, sibling_hash)),
        "the staged branch selection must choose the harder sibling"
    );
    assert!(!staged_block_extends_selected_tip(
        finalized.db.finalized_tip_hash(),
        &current,
        &staged_sibling,
        activation.hash(),
        sibling_height,
        sibling_hash,
    ));
}
