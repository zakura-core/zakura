//! Tests for header-time authentication of VCT auxiliary metadata.
//!
//! The fixture builds a deterministic chain across each configured network upgrade.
//! Each header carries its exact ZIP-221 commitment.
//! The fixture commits a body prefix.
//! The fixture admits the remaining headers with auxiliary deliveries.

use std::{num::NonZeroU64, sync::Arc};

use proptest::prelude::*;
use tokio::sync::watch;
use zakura_chain::{
    block::{self, merkle::AuthDataRoot, Block, ChainHistoryBlockTxAuthCommitmentHash, Height},
    fmt::HexDebug,
    history_tree::HistoryTree,
    ironwood, orchard,
    parameters::{
        testnet::{ConfiguredActivationHeights, ConfiguredCheckpoints, ParametersBuilder},
        Network, NetworkUpgrade, GENESIS_PREVIOUS_BLOCK_HASH,
    },
    sapling,
    transaction::{LockTime, Transaction},
    transparent,
    work::{difficulty::ParameterDifficulty as _, equihash},
};
use zakura_header_chain::{
    AuxDelivery, BodySizeHint, EvidenceId, HeaderBatchInput, HeaderRules, InsertHeaders, SourceId,
    SystemClock, TargetCompletion, TransitionContext, TransitionEvent, TransitionRequest,
    VctRootRepairState, VctRootRepairStatus,
};

use super::*;
use crate::{
    service::{
        finalized_state::commitment_aux_verify::{
            verify_commitment_roots, CommitmentRootVerification,
        },
        non_finalized_state::NonFinalizedState,
        write::{HeaderChainWriter, VctAuxiliaryWindowRead},
    },
    CheckpointVerifiedBlock, Config,
};

/// Heartwood activates here, so the running MMR is created at this height.
const HEARTWOOD: u32 = 5;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum TestAuxStatus {
    Unauthenticated,
    Authenticated,
    Rejected,
    Disputed,
}

fn aux_status(delivery: AuxDelivery) -> TestAuxStatus {
    if delivery.is_unauthenticated() {
        TestAuxStatus::Unauthenticated
    } else if delivery.is_authenticated() {
        TestAuxStatus::Authenticated
    } else if delivery.is_rejected() {
        TestAuxStatus::Rejected
    } else {
        TestAuxStatus::Disputed
    }
}
/// NU5 activates here, so headers from this height commit to an authorizing-data root and the
/// sweep's boundary check needs the successor delivery, not just the successor header.
const NU5: u32 = 10;
/// NU6 activates here.
const NU6: u32 = 14;
/// NU6.1 activates here.
const NU6_1: u32 = 16;
/// NU6.2 activates here.
const NU6_2: u32 = 18;
/// NU6.3 activates here.
const NU6_3: u32 = 20;
/// NU7 activates here.
const NU7: u32 = 22;
/// Highest generated height.
const TOP: u32 = 24;
/// Highest height committed as a body; every height above it is header-only.
const BODY_TIP: u32 = 12;

fn empty_sapling_root() -> sapling::tree::Root {
    sapling::tree::NoteCommitmentTree::default().root()
}

fn empty_orchard_root() -> orchard::tree::Root {
    orchard::tree::NoteCommitmentTree::default().root()
}

fn empty_ironwood_root() -> ironwood::tree::Root {
    ironwood::tree::NoteCommitmentTree::default().root()
}

fn parameters(checkpoint_blocks: Option<&[Arc<Block>]>) -> Network {
    let builder = ParametersBuilder::default()
        .with_activation_heights(ConfiguredActivationHeights {
            before_overwinter: Some(1),
            overwinter: Some(2),
            sapling: Some(3),
            blossom: Some(4),
            heartwood: Some(HEARTWOOD),
            canopy: Some(6),
            nu5: Some(NU5),
            nu6: Some(NU6),
            nu6_1: Some(NU6_1),
            nu6_2: Some(NU6_2),
            nu6_3: Some(NU6_3),
            nu7: Some(NU7),
            #[cfg(zcash_unstable = "nutachyon")]
            nu_tachyon: None,
        })
        .expect("the compressed activation schedule is ordered")
        .with_disable_pow(true)
        .extend_funding_streams();
    let builder = match checkpoint_blocks {
        Some(blocks) => builder
            .with_genesis_hash(
                blocks
                    .first()
                    .expect("the generated chain contains genesis")
                    .hash(),
            )
            .expect("the generated genesis hash is canonical")
            .with_checkpoints(ConfiguredCheckpoints::HeightsAndHashes(
                blocks
                    .iter()
                    .take(BODY_TIP as usize + 1)
                    .map(|block| {
                        (
                            block
                                .coinbase_height()
                                .expect("every generated checkpoint has a height"),
                            block.hash(),
                        )
                    })
                    .collect(),
            ))
            .expect("the generated checkpoints are ordered"),
        None => builder,
    };
    builder
        .to_network()
        .expect("the compressed custom network is valid")
}

/// A deterministic chain whose header commitments are exactly what the sweep re-derives.
fn generate_chain(network: &Network) -> Vec<Arc<Block>> {
    let sapling_root = empty_sapling_root();
    let orchard_root = empty_orchard_root();
    let ironwood_root = empty_ironwood_root();
    let mut history_tree = HistoryTree::default();
    let mut previous_hash = GENESIS_PREVIOUS_BLOCK_HASH;
    let mut blocks = Vec::new();

    for height in (0..=TOP).map(Height) {
        let upgrade = NetworkUpgrade::current(network, height);
        let input = transparent::Input::Coinbase {
            height,
            data: if height == Height(0) {
                transparent::GENESIS_COINBASE_SCRIPT_SIG.to_vec()
            } else {
                format!("vct-sweep {height:?}").into_bytes()
            },
            sequence: 0,
        };
        let transaction = match upgrade {
            NetworkUpgrade::Genesis | NetworkUpgrade::BeforeOverwinter => Transaction::V1 {
                inputs: vec![input],
                outputs: Vec::new(),
                lock_time: LockTime::unlocked(),
            },
            NetworkUpgrade::Overwinter => Transaction::V3 {
                inputs: vec![input],
                outputs: Vec::new(),
                lock_time: LockTime::unlocked(),
                expiry_height: height,
                joinsplit_data: None,
            },
            NetworkUpgrade::Sapling
            | NetworkUpgrade::Blossom
            | NetworkUpgrade::Heartwood
            | NetworkUpgrade::Canopy => Transaction::V4 {
                inputs: vec![input],
                outputs: Vec::new(),
                lock_time: LockTime::unlocked(),
                expiry_height: height,
                joinsplit_data: None,
                sapling_shielded_data: None,
            },
            NetworkUpgrade::Nu5
            | NetworkUpgrade::Nu6
            | NetworkUpgrade::Nu6_1
            | NetworkUpgrade::Nu6_2
            | NetworkUpgrade::Nu6_3
            | NetworkUpgrade::Nu7 => Transaction::V5 {
                network_upgrade: upgrade,
                lock_time: LockTime::unlocked(),
                expiry_height: height,
                inputs: vec![input],
                outputs: Vec::new(),
                sapling_shielded_data: None,
                orchard_shielded_data: None,
            },
            #[cfg(zcash_unstable = "nutachyon")]
            NetworkUpgrade::NuTachyon => Transaction::V5 {
                network_upgrade: upgrade,
                lock_time: LockTime::unlocked(),
                expiry_height: height,
                inputs: vec![input],
                outputs: Vec::new(),
                sapling_shielded_data: None,
                orchard_shielded_data: None,
            },
        };
        let transactions = vec![Arc::new(transaction)];
        let merkle_root = transactions.iter().cloned().collect();
        let time =
            chrono::DateTime::from_timestamp(1_700_000_000_i64 + i64::from(height.0) * 150, 0)
                .expect("the deterministic timestamp is in range");
        let header = block::Header {
            version: 4,
            previous_block_hash: previous_hash,
            merkle_root,
            commitment_bytes: HexDebug([0; 32]),
            time,
            difficulty_threshold: network.target_difficulty_limit().to_compact(),
            nonce: HexDebug([0; 32]),
            solution: equihash::Solution::for_proposal(),
        };
        let mut block = Arc::new(Block {
            header: Arc::new(header),
            transactions,
        });
        let commitment = match upgrade {
            NetworkUpgrade::Sapling | NetworkUpgrade::Blossom => <[u8; 32]>::from(sapling_root),
            NetworkUpgrade::Heartwood if height == Height(HEARTWOOD) => [0; 32],
            NetworkUpgrade::Heartwood | NetworkUpgrade::Canopy => history_tree
                .hash()
                .expect("the history tree exists after Heartwood activation")
                .into(),
            NetworkUpgrade::Nu5
            | NetworkUpgrade::Nu6
            | NetworkUpgrade::Nu6_1
            | NetworkUpgrade::Nu6_2
            | NetworkUpgrade::Nu6_3
            | NetworkUpgrade::Nu7 => ChainHistoryBlockTxAuthCommitmentHash::from_commitments(
                &history_tree
                    .hash()
                    .expect("the history tree exists after NU5 activation"),
                &block.auth_data_root(),
            )
            .into(),
            _ => [0; 32],
        };
        Arc::make_mut(&mut Arc::make_mut(&mut block).header).commitment_bytes = commitment.into();
        previous_hash = block.hash();
        history_tree
            .push(
                network,
                block.clone(),
                &sapling_root,
                &orchard_root,
                &ironwood_root,
                #[cfg(zcash_unstable = "nutachyon")]
                &Default::default(),
            )
            .expect("the deterministic history tree advances");
        blocks.push(block);
    }

    blocks
}

/// How one generated auxiliary delivery is corrupted before it reaches the header chain.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Corruption {
    /// A wrong ZIP-244 authorizing-data root, which NU5+ headers commit to directly.
    AuthDataRoot,
    /// A wrong end-of-block Sapling root, which only the successor's history leaf covers.
    SaplingRoot,
    /// A wrong shielded transaction count, which only the successor's history leaf covers.
    SaplingTxCount,
    /// A non-empty Orchard root below NU5, which this height's own pin rejects.
    PreActivationOrchardRoot,
}

impl Corruption {
    fn apply(self, aux: &mut zakura_header_chain::TreeAuxRecordV1) {
        match self {
            Self::AuthDataRoot => {
                let mut bytes = <[u8; 32]>::from(aux.auth_data_root);
                bytes[0] ^= 1;
                aux.auth_data_root = AuthDataRoot::from(bytes);
            }
            Self::SaplingRoot => {
                aux.sapling_root = sapling::tree::Root::try_from([7; 32])
                    .expect("the fixture root bytes are a valid field element");
            }
            Self::SaplingTxCount => aux.sapling_tx_count = 1,
            Self::PreActivationOrchardRoot => {
                aux.orchard_root = orchard::tree::Root::try_from([0; 32])
                    .expect("zero is a valid pallas base field element");
            }
        }
    }
}

struct Fixture {
    network: Network,
    chain: Vec<Arc<Block>>,
    finalized_state: FinalizedState,
    writer: HeaderChainWriter,
    repair: VctWriteRetryManager,
    repair_receiver: watch::Receiver<VctRootRepairStatus>,
    /// Height whose delivery was omitted from the admitted headers, if any.
    skipped_aux: Option<Height>,
    /// Height whose delivery was corrupted before admission, if any.
    corruption: Option<(Height, Corruption)>,
}

impl Fixture {
    /// Commit bodies through [`BODY_TIP`] and attach a header chain at that tip.
    fn new() -> Self {
        let preliminary = parameters(None);
        let preliminary_chain = generate_chain(&preliminary);
        let network = parameters(Some(&preliminary_chain));
        let chain = generate_chain(&network);
        assert_eq!(network.genesis_hash(), chain[0].hash());
        assert_eq!(
            chain.iter().map(|block| block.hash()).collect::<Vec<_>>(),
            preliminary_chain
                .iter()
                .map(|block| block.hash())
                .collect::<Vec<_>>(),
            "installing generated checkpoints must not change the generated chain"
        );

        let mut finalized_state = FinalizedState::new(&Config::ephemeral(), &network)
            .expect("the sweep fixture finalized state opens");
        for block in chain.iter().take(BODY_TIP as usize + 1) {
            finalized_state
                .commit_finalized_direct(
                    CheckpointVerifiedBlock::from(block.clone()).into(),
                    None,
                    None,
                    "vct sweep fixture body",
                )
                .expect("the generated body prefix commits");
        }
        // The fast path owns every generated height, so the sweep never stops on the handoff.
        finalized_state.enable_vct_exact_root_source_for_test(Height(TOP + 1));

        let live = NonFinalizedState::new(&network);
        let writer = HeaderChainWriter::attach_at_semantic_handoff(&finalized_state, &live)
            .expect("the header engine attaches at the committed body tip");
        assert_eq!(
            writer.runtime.publisher().snapshot().frontiers.finalized,
            Frontier::new(Height(BODY_TIP), chain[BODY_TIP as usize].hash())
        );

        let (repair_sender, repair_receiver) = watch::channel(VctRootRepairStatus::default());
        Self {
            network,
            chain,
            finalized_state,
            writer,
            repair: VctWriteRetryManager::new(repair_sender),
            repair_receiver,
            skipped_aux: None,
            corruption: None,
        }
    }

    /// The exact schema-1 record admitted for `height`, corruption included.
    fn aux_record(&self, height: Height) -> Option<zakura_header_chain::TreeAuxRecordV1> {
        if self.skipped_aux == Some(height) {
            return None;
        }
        let block = self.chain.get(height.0 as usize)?;
        let mut record = zakura_header_chain::TreeAuxRecordV1 {
            height,
            sapling_root: empty_sapling_root(),
            orchard_root: empty_orchard_root(),
            ironwood_root: empty_ironwood_root(),
            sapling_tx_count: block.sapling_transactions_count(),
            orchard_tx_count: block.orchard_transactions_count(),
            ironwood_tx_count: block.ironwood_transactions_count(),
            auth_data_root: block.auth_data_root(),
        };
        if let Some((corrupt_height, corruption)) = self.corruption {
            if corrupt_height == height {
                corruption.apply(&mut record);
            }
        }
        Some(record)
    }

    /// Admit headers `BODY_TIP + 1 ..= TOP` with one auxiliary delivery each.
    ///
    /// `skip_aux` omits a height's delivery entirely, modelling a peer that supplied headers
    /// without metadata. `corrupt` alters one height's delivery in place.
    fn insert_headers(&mut self, skip_aux: Option<Height>, corrupt: Option<(Height, Corruption)>) {
        self.skipped_aux = skip_aux;
        self.corruption = corrupt;
        let snapshot = self.writer.runtime.publisher().snapshot();
        let parent = self.chain[BODY_TIP as usize].clone();
        let headers: Vec<_> = self.chain[BODY_TIP as usize + 1..]
            .iter()
            .map(|block| block.header.clone())
            .collect();
        let lease = self
            .writer
            .runtime
            .reader()
            .validation_context(parent.hash())
            .expect("the committed parent context read succeeds")
            .expect("the committed parent is retained");
        let rules =
            HeaderRules::for_validation_lease(&lease).expect("the custom network waives PoW");
        let batch = zakura_header_chain::prepare_headers(
            HeaderBatchInput::new(&headers),
            lease.parent(),
            &rules,
            &SystemClock,
        )
        .expect("the generated headers pass production validation");

        let target_tip_hash = self.chain[TOP as usize].hash();
        let owner =
            zakura_header_chain::HeaderWorkAuthority::for_target(&snapshot, target_tip_hash)
                .bind(1, NonZeroU64::new(1).expect("one is nonzero"))
                .into();
        let source = SourceId::from_digest([0x51; 32]);
        let aux = self.chain[BODY_TIP as usize + 1..]
            .iter()
            .filter_map(|block| {
                let height = block
                    .coinbase_height()
                    .expect("every generated block has a coinbase height");
                let record = self.aux_record(height)?;
                let mut delivery_id = [0x60; 32];
                delivery_id[..4].copy_from_slice(&height.0.to_le_bytes());
                Some(AuxDelivery::new(
                    EvidenceId::from_digest(delivery_id),
                    block.hash(),
                    source,
                    owner,
                    BodySizeHint::Unknown,
                    Some(record),
                ))
            })
            .collect();

        let result = self
            .writer
            .runtime
            .apply(
                TransitionRequest {
                    expected_version: snapshot.state_version,
                    event: TransitionEvent::InsertHeaders(Box::new(InsertHeaders {
                        owner,
                        source,
                        parent_hash: parent.hash(),
                        target_tip_hash,
                        completion: TargetCompletion::TargetComplete {
                            common_ancestor: snapshot.frontiers.finalized,
                        },
                        batch,
                        aux,
                    })),
                },
                &TransitionContext {
                    config: &self.writer.config,
                    clock: &SystemClock,
                    full_state_authority: None,
                    retention_references: &[],
                },
            )
            .expect("the generated header batch commits");
        assert!(matches!(result, ApplyResult::Committed));
        assert_eq!(
            self.writer
                .runtime
                .publisher()
                .snapshot()
                .frontiers
                .header_best,
            Frontier::new(Height(TOP), target_tip_hash)
        );
    }

    fn redeliver(&mut self, height: Height, corruption: Option<Corruption>, marker: u8) {
        let snapshot = self.writer.runtime.publisher().snapshot();
        let parent = self.chain[height.0.saturating_sub(1) as usize].clone();
        let target = self.chain[height.0 as usize].clone();
        let lease = self
            .writer
            .runtime
            .reader()
            .validation_context(parent.hash())
            .expect("the selected repair parent context read succeeds")
            .expect("the selected repair parent is retained");
        let rules =
            HeaderRules::for_validation_lease(&lease).expect("the custom network waives PoW");
        let batch = zakura_header_chain::prepare_headers(
            HeaderBatchInput::new(std::slice::from_ref(&target.header)),
            lease.parent(),
            &rules,
            &SystemClock,
        )
        .expect("the replacement header passes production validation");
        let repair_owner = zakura_header_chain::BodyWorkAuthority::for_snapshot(&snapshot).bind(
            u64::from(marker),
            NonZeroU64::new(1).expect("one is nonzero"),
        );
        let repair_episode = self
            .writer
            .runtime
            .reader()
            .vct_repair_context(repair_owner, height)
            .expect("the replacement repair context is coherent")
            .expect("the replacement target remains selected")
            .episode;
        let owner = repair_owner.into();
        let source = SourceId::from_digest([marker; 32]);
        let mut record = zakura_header_chain::TreeAuxRecordV1 {
            height,
            sapling_root: empty_sapling_root(),
            orchard_root: empty_orchard_root(),
            ironwood_root: empty_ironwood_root(),
            sapling_tx_count: target.sapling_transactions_count(),
            orchard_tx_count: target.orchard_transactions_count(),
            ironwood_tx_count: target.ironwood_transactions_count(),
            auth_data_root: target.auth_data_root(),
        };
        if let Some(corruption) = corruption {
            corruption.apply(&mut record);
        }
        let mut delivery_id = [marker; 32];
        delivery_id[..4].copy_from_slice(&height.0.to_le_bytes());
        let delivery = AuxDelivery::new(
            EvidenceId::from_digest(delivery_id),
            target.hash(),
            source,
            owner,
            BodySizeHint::Unknown,
            Some(record),
        );
        let result = self
            .writer
            .runtime
            .apply(
                TransitionRequest {
                    expected_version: snapshot.state_version,
                    event: TransitionEvent::InsertHeaders(Box::new(InsertHeaders {
                        owner,
                        source,
                        parent_hash: parent.hash(),
                        target_tip_hash: target.hash(),
                        completion: TargetCompletion::SelectedAuxiliaryRepair {
                            common_ancestor: Frontier::new(
                                Height(height.0.saturating_sub(1)),
                                parent.hash(),
                            ),
                            selected_target: Frontier::new(height, target.hash()),
                            episode: repair_episode,
                        },
                        batch,
                        aux: vec![delivery],
                    })),
                },
                &TransitionContext {
                    config: &self.writer.config,
                    clock: &SystemClock,
                    full_state_authority: None,
                    retention_references: &[],
                },
            )
            .expect("the selected auxiliary replacement commits");
        assert!(matches!(result, ApplyResult::Committed));
    }

    fn authentications(&self, height: Height) -> Vec<TestAuxStatus> {
        let hash = self.chain[height.0 as usize].hash();
        let window = self
            .writer
            .runtime
            .selected_auxiliary_window(height, hash)
            .expect("the selected auxiliary window is coherent")
            .expect("the selected auxiliary header exists");
        window
            .delivery_header
            .auxiliary_deliveries
            .into_iter()
            .map(aux_status)
            .collect()
    }

    fn sweep(&mut self, sweeper: &mut VctAuthenticationSweeper) {
        for _ in 0..=TOP - BODY_TIP {
            sweeper.sweep(
                &self.finalized_state,
                &self.writer,
                &mut self.repair,
                || false,
            );
            if self.repair_state() != VctRootRepairState::Idle
                || sweeper
                    .verified_selected_prefix
                    .as_ref()
                    .is_some_and(|run| run.frontier.height >= Height(LAST_PROVABLE))
            {
                break;
            }
        }
    }

    /// The authentication state currently recorded for the selected delivery at `height`.
    fn outcome(&self, height: Height) -> Option<AuxDelivery> {
        let hash = self.chain[height.0 as usize].hash();
        match self.writer.vct_auxiliary_window(height, hash) {
            Ok(VctAuxiliaryWindowRead::Ready(window)) => Some(window.delivery),
            // Every delivery at this height is rejected, or none was ever supplied.
            Ok(VctAuxiliaryWindowRead::Missing { .. }) => None,
            Err(error) => panic!("the fixture auxiliary read is coherent: {error}"),
        }
    }

    fn authentication(&self, height: Height) -> Option<TestAuxStatus> {
        self.outcome(height).map(aux_status)
    }

    fn is_selected(&self, height: Height) -> bool {
        self.writer
            .runtime
            .reader()
            .selected_hash(height)
            .expect("the fixture selected read is coherent")
            == Some(self.chain[height.0 as usize].hash())
    }

    fn repair_state(&self) -> VctRootRepairState {
        self.repair_receiver.borrow().state
    }

    /// The ZIP-221 MMR folded through `height`, rebuilt from genesis.
    fn history_tree_through(&self, height: Height) -> HistoryTree {
        let mut tree = HistoryTree::default();
        for block in self.chain.iter().take(height.0 as usize + 1) {
            tree.push(
                &self.network,
                block.clone(),
                &empty_sapling_root(),
                &empty_orchard_root(),
                &empty_ironwood_root(),
                #[cfg(zcash_unstable = "nutachyon")]
                &Default::default(),
            )
            .expect("the generated history tree advances");
        }
        tree
    }

    /// Runs the committer's body-based verifier over the delivery at `height`.
    ///
    /// This is the trust boundary the sweep runs ahead of, reached through a different code
    /// path: [`commitment_aux_verify::verify_commitment_roots`] folds from a block body, while
    /// the sweep folds from schema-1 parts. Any disagreement is a defect in one of them.
    fn committer_accepts(&self, height: Height) -> bool {
        let (Some(record), Some(successor_record)) = (
            self.aux_record(height),
            height.next().ok().and_then(|next| self.aux_record(next)),
        ) else {
            return false;
        };
        let block = self.chain[height.0 as usize].clone();
        let successor = &self.chain[height.0 as usize + 1];
        let parent = Height(height.0 - 1);

        verify_commitment_roots(
            &self.network,
            self.history_tree_through(parent),
            #[cfg(zcash_unstable = "nutachyon")]
            Default::default(),
            vec![
                CommitmentRootVerification::with_roots(
                    block.clone(),
                    record.sapling_root,
                    record.orchard_root,
                    record.ironwood_root,
                    Some(block.auth_data_root()),
                    false,
                ),
                CommitmentRootVerification::header_only(
                    successor.header.clone(),
                    Height(height.0 + 1),
                    Some(successor_record.auth_data_root),
                ),
            ],
        )
        .is_ok()
    }
}

/// The highest height the sweep can prove: the tip has no successor to authenticate it.
const LAST_PROVABLE: u32 = TOP - 1;

#[test]
fn authenticates_every_supplied_delivery_ahead_of_its_body() {
    let _init_guard = zakura_test::init();
    let mut fixture = Fixture::new();
    fixture.insert_headers(None, None);
    let mut sweeper = VctAuthenticationSweeper::default();

    fixture.sweep(&mut sweeper);

    for height in (BODY_TIP + 1..=LAST_PROVABLE).map(Height) {
        assert!(
            matches!(
                fixture.authentication(height),
                Some(TestAuxStatus::Authenticated)
            ),
            "the sweep authenticates {height:?} from its successor header, far below its body"
        );
    }
    assert_eq!(
        fixture.authentication(Height(TOP)),
        Some(TestAuxStatus::Unauthenticated),
        "the header tip has no successor, so nothing proves its roots yet"
    );
    assert_eq!(
        fixture.repair_state(),
        VctRootRepairState::Idle,
        "a clean sweep asks for no metadata repair"
    );
    assert_eq!(
        fixture.finalized_state.db.tip().map(|(height, _)| height),
        Some(Height(BODY_TIP)),
        "the sweep commits no bodies"
    );
}

#[test]
fn queued_commit_work_yields_before_authentication() {
    let _init_guard = zakura_test::init();
    let mut fixture = Fixture::new();
    fixture.insert_headers(None, None);
    let before = fixture.writer.runtime.publisher().snapshot();
    let mut sweeper = VctAuthenticationSweeper::default();

    sweeper.sweep(
        &fixture.finalized_state,
        &fixture.writer,
        &mut fixture.repair,
        || true,
    );

    assert_eq!(fixture.writer.runtime.publisher().snapshot(), before);
    assert_eq!(
        fixture.authentication(Height(BODY_TIP + 1)),
        Some(TestAuxStatus::Unauthenticated)
    );
}

#[test]
fn stops_below_a_height_with_no_supplied_metadata() {
    let _init_guard = zakura_test::init();
    let hole = Height(BODY_TIP + 4);
    let mut fixture = Fixture::new();
    fixture.insert_headers(Some(hole), None);
    let mut sweeper = VctAuthenticationSweeper::default();

    fixture.sweep(&mut sweeper);

    // A delivery is proven by its successor's, so the hole also leaves its predecessor
    // unproven.
    for height in (BODY_TIP + 1..hole.0 - 1).map(Height) {
        assert!(
            matches!(
                fixture.authentication(height),
                Some(TestAuxStatus::Authenticated)
            ),
            "{height:?} is below the hole and still provable"
        );
    }
    assert_eq!(
        fixture.authentication(Height(hole.0 - 1)),
        Some(TestAuxStatus::Unauthenticated),
        "nothing proves the delivery directly below the hole"
    );
    assert_eq!(
        fixture.authentication(hole),
        None,
        "the hole has no delivery at all"
    );
    for height in (hole.0 + 1..=LAST_PROVABLE).map(Height) {
        assert_eq!(
            fixture.authentication(height),
            Some(TestAuxStatus::Unauthenticated),
            "the running history tree cannot skip {hole:?}, so {height:?} stays unproven"
        );
    }
}

#[test]
fn disputes_an_ambiguous_note_commitment_boundary_and_arms_repair() {
    let _init_guard = zakura_test::init();
    let bad = Height(BODY_TIP + 3);
    let mut fixture = Fixture::new();
    fixture.insert_headers(None, Some((bad, Corruption::SaplingRoot)));
    let mut sweeper = VctAuthenticationSweeper::default();

    fixture.sweep(&mut sweeper);

    assert!(matches!(
        fixture.authentication(bad),
        Some(TestAuxStatus::Disputed)
    ));
    assert!(
        fixture.is_selected(bad),
        "rejecting metadata must not invalidate or deselect its header"
    );
    assert_eq!(
        fixture.repair_state(),
        VctRootRepairState::Unavailable { height: bad },
        "the dispute asks for replacement metadata immediately"
    );
    assert!(
        fixture
            .finalized_state
            .db
            .tip()
            .is_some_and(|(height, _)| height.0 < bad.0),
        "the committer has not reached the corrupted height yet"
    );
}

#[test]
fn an_ambiguous_boundary_disputes_both_deliveries_without_rejecting_them() {
    let _init_guard = zakura_test::init();
    // A wrong authorizing-data root at H first shows up in H's own commitment check, which
    // mixes it with the roots folded for H - 1. Neither delivery can be cleared, so the
    // The sweep disputes both deliveries until a replacement identifies the bad delivery.
    let bad = Height(BODY_TIP + 3);
    let predecessor = Height(bad.0 - 1);
    let mut fixture = Fixture::new();
    fixture.insert_headers(None, Some((bad, Corruption::AuthDataRoot)));
    let mut sweeper = VctAuthenticationSweeper::default();

    fixture.sweep(&mut sweeper);

    let bad_authentication = fixture
        .outcome(bad)
        .expect("the corrupt successor remains available as disputed evidence");
    let predecessor_authentication = fixture
        .outcome(predecessor)
        .expect("the honest predecessor remains available as disputed evidence");
    assert_eq!(aux_status(bad_authentication), TestAuxStatus::Disputed);
    assert_eq!(
        aux_status(predecessor_authentication),
        TestAuxStatus::Disputed
    );
    assert_eq!(
        bad_authentication.observation_ids(),
        predecessor_authentication.observation_ids()
    );
    assert!(fixture.is_selected(bad) && fixture.is_selected(predecessor));
    assert_eq!(
        fixture.repair_state(),
        VctRootRepairState::Unavailable {
            height: predecessor
        },
        "repair restarts at the lower disputed height"
    );

    let generation = fixture.repair_receiver.borrow().generation;
    fixture.sweep(&mut sweeper);
    assert_eq!(
        fixture.repair_receiver.borrow().generation,
        generation,
        "re-reading durable dispute evidence must not replace in-flight repair work"
    );
}

#[test]
fn a_replacement_successor_preserves_and_authenticates_the_honest_predecessor() {
    let _init_guard = zakura_test::init();
    let bad = Height(BODY_TIP + 4);
    let predecessor = Height(bad.0 - 1);
    let mut fixture = Fixture::new();
    fixture.insert_headers(None, Some((bad, Corruption::AuthDataRoot)));
    let mut sweeper = VctAuthenticationSweeper::default();
    fixture.sweep(&mut sweeper);

    fixture.redeliver(bad, None, 0x71);
    fixture.sweep(&mut sweeper);

    assert!(matches!(
        fixture.authentication(predecessor),
        Some(TestAuxStatus::Authenticated)
    ));
    let successor_states = fixture.authentications(bad);
    assert!(successor_states.contains(&TestAuxStatus::Disputed));
    assert!(successor_states.contains(&TestAuxStatus::Authenticated));
    assert_eq!(fixture.repair_state(), VctRootRepairState::Idle);
}

#[test]
fn a_replacement_predecessor_preserves_the_honest_successor() {
    let _init_guard = zakura_test::init();
    let bad = Height(BODY_TIP + 3);
    let successor = Height(bad.0 + 1);
    let mut fixture = Fixture::new();
    fixture.insert_headers(None, Some((bad, Corruption::SaplingRoot)));
    let mut sweeper = VctAuthenticationSweeper::default();
    fixture.sweep(&mut sweeper);

    fixture.redeliver(bad, None, 0x73);
    fixture.sweep(&mut sweeper);

    let predecessor_states = fixture.authentications(bad);
    assert!(predecessor_states.contains(&TestAuxStatus::Disputed));
    assert!(predecessor_states.contains(&TestAuxStatus::Authenticated));
    assert!(matches!(
        fixture.authentication(successor),
        Some(TestAuxStatus::Authenticated)
    ));
    assert_eq!(fixture.repair_state(), VctRootRepairState::Idle);
}

#[test]
fn a_fresh_manager_recreates_repair_from_a_durable_dispute() {
    let _init_guard = zakura_test::init();
    let bad = Height(BODY_TIP + 3);
    let repair_height = Height(bad.0 - 1);
    let mut fixture = Fixture::new();
    fixture.insert_headers(None, Some((bad, Corruption::AuthDataRoot)));
    let mut sweeper = VctAuthenticationSweeper::default();
    fixture.sweep(&mut sweeper);

    let (repair_sender, repair_receiver) = watch::channel(VctRootRepairStatus::default());
    fixture.repair = VctWriteRetryManager::new(repair_sender);
    fixture.repair_receiver = repair_receiver;
    let mut restarted = VctAuthenticationSweeper::default();
    fixture.sweep(&mut restarted);

    assert_eq!(
        fixture.repair_state(),
        VctRootRepairState::Unavailable {
            height: repair_height
        }
    );
}

#[test]
fn a_transient_anchor_gate_preserves_an_existing_repair() {
    let _init_guard = zakura_test::init();
    let repair_height = Height(BODY_TIP + 2);
    let mut fixture = Fixture::new();
    fixture.insert_headers(None, None);
    fixture
        .repair
        .request_sweep_repair(repair_height, VctRepairTrigger::MissingRootObserved);
    fixture
        .finalized_state
        .enable_vct_exact_root_source_for_test(Height(BODY_TIP));
    let mut sweeper = VctAuthenticationSweeper::default();

    fixture.sweep(&mut sweeper);

    assert_eq!(
        fixture.repair_state(),
        VctRootRepairState::Unavailable {
            height: repair_height
        }
    );
}

#[test]
fn a_sweep_repair_need_survives_a_successful_commit() {
    let _init_guard = zakura_test::init();
    let bad = Height(BODY_TIP + 3);
    let mut fixture = Fixture::new();
    fixture.insert_headers(None, Some((bad, Corruption::SaplingRoot)));
    let mut sweeper = VctAuthenticationSweeper::default();
    fixture.sweep(&mut sweeper);
    assert_eq!(
        fixture.repair_state(),
        VctRootRepairState::Unavailable { height: bad }
    );

    // The committer remains below the sweep. A successful block commit must not clear the
    // sweep repair request.
    fixture.repair.on_commit_success();

    assert_eq!(
        fixture.repair_state(),
        VctRootRepairState::Unavailable { height: bad },
        "the committer clears only its repair request"
    );
}

#[test]
fn a_committer_stall_below_the_sweep_takes_priority() {
    let _init_guard = zakura_test::init();
    let bad = Height(BODY_TIP + 6);
    let mut fixture = Fixture::new();
    fixture.insert_headers(None, Some((bad, Corruption::SaplingRoot)));
    let mut sweeper = VctAuthenticationSweeper::default();
    fixture.sweep(&mut sweeper);

    let stalled = Height(BODY_TIP + 1);
    fixture.repair.request_committer_repair_for_test(stalled);

    assert_eq!(
        fixture.repair_state(),
        VctRootRepairState::Unavailable { height: stalled },
        "the committer repair must unblock the checkpoint queue before the sweep can resume"
    );

    fixture.repair.on_commit_success();
    assert_eq!(
        fixture.repair_state(),
        VctRootRepairState::Unavailable { height: bad },
        "clearing the committer request exposes the sweep request"
    );
}

#[test]
fn a_fresh_sweeper_resumes_from_the_durable_authentication_marks() {
    let _init_guard = zakura_test::init();
    let mut fixture = Fixture::new();
    fixture.insert_headers(None, None);
    let mut sweeper = VctAuthenticationSweeper::default();
    fixture.sweep(&mut sweeper);
    let after_first = fixture.writer.runtime.publisher().snapshot();

    // A restart drops the in-memory run but not the marks it wrote.
    let mut restarted = VctAuthenticationSweeper::default();
    fixture.sweep(&mut restarted);

    assert_eq!(
        fixture.writer.runtime.publisher().snapshot(),
        after_first,
        "re-verifying already authenticated deliveries commits no new transition"
    );
    for height in (BODY_TIP + 1..=LAST_PROVABLE).map(Height) {
        assert!(matches!(
            fixture.authentication(height),
            Some(TestAuxStatus::Authenticated)
        ));
    }
}

#[test]
fn does_nothing_outside_the_fast_path() {
    let _init_guard = zakura_test::init();
    let mut fixture = Fixture::new();
    fixture.insert_headers(None, None);
    // Move the handoff below every header-only height: those heights recompute from bodies,
    // so peer metadata there carries no authority and needs no early check.
    fixture
        .finalized_state
        .enable_vct_exact_root_source_for_test(Height(BODY_TIP));
    let before = fixture.writer.runtime.publisher().snapshot();
    let mut sweeper = VctAuthenticationSweeper::default();

    fixture.sweep(&mut sweeper);

    assert_eq!(fixture.writer.runtime.publisher().snapshot(), before);
    assert_eq!(
        fixture.authentication(Height(BODY_TIP + 1)),
        Some(TestAuxStatus::Unauthenticated)
    );
}

#[test]
fn fold_placement_is_checked_before_every_history_tree_push() {
    let _init_guard = zakura_test::init();
    let network = parameters(None);
    let chain = generate_chain(&network);
    let mut tree = HistoryTree::default();

    assert!(
        history_tree_accepts_height(&network, Height(1), &tree),
        "an empty tree is the only correct placement below Heartwood"
    );
    assert!(
        history_tree_accepts_height(&network, Height(HEARTWOOD), &tree),
        "the activation height creates the tree from any placement"
    );
    assert!(
        !history_tree_accepts_height(&network, Height(HEARTWOOD + 1), &tree),
        "an empty tree cannot be extended above Heartwood"
    );

    tree.push(
        &network,
        chain[HEARTWOOD as usize].clone(),
        &empty_sapling_root(),
        &empty_orchard_root(),
        &empty_ironwood_root(),
        #[cfg(zcash_unstable = "nutachyon")]
        &Default::default(),
    )
    .expect("the activation block creates the tree");

    assert!(history_tree_accepts_height(
        &network,
        Height(HEARTWOOD + 1),
        &tree
    ));
    assert!(
        !history_tree_accepts_height(&network, Height(HEARTWOOD + 2), &tree),
        "a tree one height behind would panic the fold"
    );
    assert!(
        !history_tree_accepts_height(&network, Height(HEARTWOOD - 1), &tree),
        "a tree that exists below Heartwood would panic the fold"
    );
}

#[test]
fn only_supplied_auxiliary_fields_are_blamed_on_a_delivery() {
    let _init_guard = zakura_test::init();
    for error in [
        CommitmentError::InvalidFinalSaplingRoot {
            expected: [0; 32],
            actual: [1; 32],
        },
        CommitmentError::InvalidPreSaplingSaplingTxCount {
            expected: 0,
            actual: 1,
        },
        CommitmentError::InvalidPreNu5OrchardRoot {
            expected: [0; 32],
            actual: [1; 32],
        },
        CommitmentError::InvalidPreNu5OrchardTxCount {
            expected: 0,
            actual: 1,
        },
        CommitmentError::InvalidPreNu6_3IronwoodRoot {
            expected: [0; 32],
            actual: [1; 32],
        },
        CommitmentError::InvalidPreNu6_3IronwoodTxCount {
            expected: 0,
            actual: 1,
        },
        CommitmentError::InvalidChainHistoryBlockTxAuthCommitment {
            expected: [0; 32],
            actual: [1; 32],
        },
    ] {
        assert!(
            commitment_error_implicates_delivery(&error),
            "{error} is a supplied-field pin"
        );
    }

    for error in [
        // The parent history root is proven by the predecessor step, never by a delivery here.
        CommitmentError::InvalidChainHistoryRoot {
            expected: [0; 32],
            actual: [1; 32],
        },
        // These report a header the header chain already validated.
        CommitmentError::InvalidChainHistoryActivationReserved { actual: [1; 32] },
        CommitmentError::InvalidAuthDataRoot {
            expected: [0; 32],
            actual: [1; 32],
        },
        CommitmentError::InvalidSapingRootBytes,
    ] {
        assert!(
            !commitment_error_implicates_delivery(&error),
            "{error} must never reject metadata"
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    /// The sweep never authenticates a corrupted delivery.
    /// The sweep never folds past a corrupted delivery.
    /// The sweep never rejects a delivery that the corruption does not implicate.
    ///
    /// A permanent rejection must identify the rejected delivery.
    #[test]
    fn never_authenticates_or_over_rejects_a_corrupted_delivery(
        offset in 1u32..(TOP - BODY_TIP),
        corruption in prop_oneof![
            Just(Corruption::AuthDataRoot),
            Just(Corruption::SaplingRoot),
            Just(Corruption::SaplingTxCount),
            Just(Corruption::PreActivationOrchardRoot),
        ],
    ) {
        let _init_guard = zakura_test::init();
        let bad = Height(BODY_TIP + offset);
        let mut fixture = Fixture::new();
        fixture.insert_headers(None, Some((bad, corruption)));
        let mut sweeper = VctAuthenticationSweeper::default();

        fixture.sweep(&mut sweeper);

        prop_assert!(
            !matches!(
                fixture.authentication(bad),
                Some(TestAuxStatus::Authenticated)
            ),
            "a corrupted delivery at {bad:?} must never be authenticated"
        );
        // The boundary for H combines roots from H and H - 1.
        // An ambiguous failure can dispute the honest delivery at H - 1.
        // The failure does not implicate any lower delivery.
        let lowest_affected = Height(bad.0.saturating_sub(1));
        for height in (BODY_TIP + 1..lowest_affected.0).map(Height) {
            prop_assert!(
                matches!(
                    fixture.authentication(height),
                    Some(TestAuxStatus::Authenticated)
                ),
                "{height:?} is below the corruption and must stay usable"
            );
        }
        // The sweep folds a ZIP-221 leaf from schema-1 parts. The committer folds the same leaf
        // from a block body. The sweep must not authenticate a delivery that the committer rejects.
        for height in (BODY_TIP + 1..=LAST_PROVABLE).map(Height) {
            if matches!(
                fixture.authentication(height),
                Some(TestAuxStatus::Authenticated)
            ) {
                prop_assert!(
                    fixture.committer_accepts(height),
                    "the sweep authenticated {height:?}, which the committer's body-based \
                     verifier rejects"
                );
            }
        }
        for height in (bad.0..=TOP).map(Height) {
            prop_assert!(
                !matches!(
                    fixture.authentication(height),
                    Some(TestAuxStatus::Authenticated)
                ),
                "the run stops at the corruption, so {height:?} cannot be proven"
            );
        }
        for height in (BODY_TIP + 1..=TOP).map(Height) {
            prop_assert!(
                fixture.is_selected(height),
                "authentication state changes never deselect {height:?}"
            );
        }
        prop_assert!(
            matches!(
                fixture.repair_state(),
                VctRootRepairState::Unavailable { height }
                    if height >= lowest_affected && height <= bad
            ),
            "repair is asked for at the corruption or the one neighbour it implicates"
        );
    }
}
