//! zakura-state's full-state glue, ported over the header-chain engine.
//!
//! The header side's [`EnginePort`](crate::header::adapter::EnginePort)
//! holds full state beside its engine, and the block side reads and commits
//! through it. Both depend on this module, and it depends on neither.
//!
//! zakurad's block driver asks zakura-state for the bodies block sync still
//! needs, commits verified bodies through it, and records body evidence with
//! it. zakura-state answers from its non-finalized chains and the
//! header-chain engine, and publishes a `CommittedHeaderChainView` after each
//! engine commit. This module ports what block sync can observe of that,
//! and each item names its zakura-state source:
//!
//! * [`VerifiedChain`], full state's committed blocks (`NonFinalizedState`);
//! * [`verified_request`], the verified-chain event a commit sends the engine
//!   (`verified_request` in `service/write.rs`);
//! * [`missing_block_body_metadata`], the missing-body query
//!   (`service.rs`), and [`query_needed_blocks`], the driver's chunking of it;
//! * [`read_block_range`], serving's bounded range read
//!   (`service/block_range.rs`);
//! * [`ViewPublisher`], the view publication (`Publisher` in
//!   `finalized_state/header_chain.rs`);
//! * [`PreparedAuthority`], [`SchedulerAuthority`] and
//!   [`StateIssuedAuthority`], the authorities the state writer hands the
//!   engine.
//!
//! Finality stays at the genesis block, as it does for headers: full state
//! commits no `FullStateFinalized` event, so every committed block stays in
//! the non-finalized tree. Production finalizes once the best chain is
//! longer than `MAX_BLOCK_REORG_HEIGHT`, so [`VerifiedChain::with`] refuses
//! to grow past it. Full state keeps at most `MAX_NON_FINALIZED_CHAIN_FORKS`
//! chains and evicts the lowest-ranked one past that, as production does.

use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
};

use sha2::{Digest, Sha256};
use zakura_chain::{
    block,
    parameters::constants::{MAX_BLOCK_REORG_HEIGHT, MAX_NON_FINALIZED_CHAIN_FORKS},
};
use zakura_header_chain::{
    AuxDelivery, AuxDelta, BodySizeHint, BodySizeHintBatch, BodyWorkEpoch, ChangeSet,
    CommittedHeaderChainView, CommittedTransitionError, EngineSnapshot, EvidenceId,
    FullStateEvidenceAuthority, HeaderChainEngine, OperatorBodyRetry, StateVersion, StoreError,
    TransitionEffect, TransitionEvent, TransitionFailure, TransitionFingerprint, TransitionRequest,
    ValidationLease, VerifiedBlockAccepted, VerifiedChainChanged, VerifiedChangeCause,
    VerifiedHeaderRef,
};
use zakura_network::zakura::{BlockSizeEstimate, BlockSyncBlockMeta};
use zakura_rt::sync::watch;

/// The most headers one page or one bounded path covers, and the most
/// heights one missing-body read covers, zakura-state's
/// `MAX_HEADER_SYNC_HEIGHT_RANGE`.
pub const MAX_HEADER_SYNC_HEIGHT_RANGE: u32 = 4_000;

/// Why a full-state operation failed. Each leaves the engine and full state
/// unchanged.
#[derive(Debug)]
pub enum StateError {
    /// The engine refused the transition, as `HeaderChainStoreError` wraps a
    /// planner failure.
    Transition(TransitionFailure),
    /// The engine refused to install a planned transition.
    Install(CommittedTransitionError),
    /// Full state and the engine disagree, or a read found an incoherence.
    Store(StoreError),
}

impl fmt::Display for StateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StateError::Transition(failure) => failure.fmt(f),
            StateError::Install(error) => error.fmt(f),
            StateError::Store(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for StateError {}

impl From<StoreError> for StateError {
    fn from(error: StoreError) -> Self {
        StateError::Store(error)
    }
}

impl From<TransitionFailure> for StateError {
    fn from(failure: TransitionFailure) -> Self {
        StateError::Transition(failure)
    }
}

impl From<CommittedTransitionError> for StateError {
    fn from(error: CommittedTransitionError) -> Self {
        StateError::Install(error)
    }
}

/// One committed block.
#[derive(Clone, Debug)]
struct Committed {
    block: Arc<block::Block>,
    height: block::Height,
    /// The verifier's receipt order, which breaks ties between equal-work
    /// chains.
    receipt: u64,
}

impl Committed {
    /// This block's rank as a chain tip, as `Chain`'s order ranks chains:
    /// more work, then the earlier receipt, then the greater hash. Fixture
    /// blocks all carry the Regtest genesis difficulty, so work is height.
    fn rank(&self) -> (block::Height, Reverse<u64>, [u8; 32]) {
        (self.height, Reverse(self.receipt), self.block.hash().0)
    }
}

/// Full state's committed blocks: the genesis block, finalized, and a tree
/// of non-finalized blocks on it.
///
/// `NonFinalizedState` keeps one chain per tip. This keeps the tree, and
/// each tip names a chain, ranked by [`Committed::rank`].
#[derive(Clone, Debug)]
pub struct VerifiedChain {
    genesis: Committed,
    /// Every non-finalized block, by hash.
    blocks: BTreeMap<[u8; 32], Committed>,
}

impl VerifiedChain {
    /// Full state holding only `genesis`.
    ///
    /// # Panics
    ///
    /// If `genesis` has no coinbase height.
    pub fn new(genesis: Arc<block::Block>) -> Self {
        let height = genesis
            .coinbase_height()
            .expect("a genesis block encodes its height");
        VerifiedChain {
            genesis: Committed {
                block: genesis,
                height,
                receipt: 0,
            },
            blocks: BTreeMap::new(),
        }
    }

    /// The finalized block, genesis.
    pub fn finalized(&self) -> zakura_header_chain::Frontier {
        frontier(&self.genesis)
    }

    /// Whether full state holds `hash`, finalized or not.
    pub fn contains(&self, hash: block::Hash) -> bool {
        self.get(hash).is_some()
    }

    /// The committed block `hash`, finalized or not.
    pub fn block(&self, hash: block::Hash) -> Option<&Arc<block::Block>> {
        self.get(hash).map(|committed| &committed.block)
    }

    /// How many non-finalized blocks full state holds.
    pub fn len(&self) -> usize {
        self.blocks.len()
    }

    /// Whether full state holds only the finalized block.
    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    fn get(&self, hash: block::Hash) -> Option<&Committed> {
        if hash == self.genesis.block.hash() {
            Some(&self.genesis)
        } else {
            self.blocks.get(&hash.0)
        }
    }

    /// Each non-finalized chain's tip, sorted by hash bytes, as
    /// `verified_tips` lists them. Empty when full state holds only genesis.
    pub fn tips(&self) -> Vec<block::Hash> {
        let parents: BTreeSet<[u8; 32]> = self
            .blocks
            .values()
            .map(|committed| committed.block.header.previous_block_hash.0)
            .collect();
        // The map is keyed by hash bytes, so this is already sorted.
        self.blocks
            .keys()
            .filter(|hash| !parents.contains(*hash))
            .map(|hash| block::Hash(*hash))
            .collect()
    }

    /// The best chain's tip, or genesis when full state holds no other
    /// block, as `verified_frontier` reads it.
    pub fn best_tip(&self) -> zakura_header_chain::Frontier {
        self.tips()
            .into_iter()
            .filter_map(|hash| self.blocks.get(&hash.0))
            .max_by_key(|committed| committed.rank())
            .map(frontier)
            .unwrap_or_else(|| self.finalized())
    }

    /// How many non-finalized blocks the best chain holds, as
    /// `best_chain_len` counts them.
    pub fn best_chain_len(&self) -> u32 {
        self.best_tip().height.0 - self.genesis.height.0
    }

    /// The non-finalized blocks `self` holds and `after` doesn't, sorted by
    /// hash bytes, as `evicted_blocks` lists them.
    pub fn evicted_blocks(&self, after: &VerifiedChain) -> Vec<block::Hash> {
        // The map is keyed by hash bytes, so this is already sorted.
        self.blocks
            .keys()
            .filter(|hash| !after.blocks.contains_key(*hash))
            .map(|hash| block::Hash(*hash))
            .collect()
    }

    /// The non-finalized path from genesis's child through `hash`, or
    /// `None` if full state doesn't hold `hash` above genesis.
    #[allow(clippy::unwrap_in_result)]
    fn path_to(&self, hash: block::Hash) -> Option<Vec<&Committed>> {
        let mut path = Vec::new();
        let mut current = self.blocks.get(&hash.0)?;
        loop {
            path.push(current);
            let parent = current.block.header.previous_block_hash;
            if parent == self.genesis.block.hash() {
                break;
            }
            current = self
                .blocks
                .get(&parent.0)
                .expect("full state commits a block only once its parent is committed");
        }
        path.reverse();
        Some(path)
    }

    /// The best chain's non-finalized blocks, in height order, as
    /// `verified_path` reads them.
    pub fn best_path(&self) -> Vec<VerifiedHeaderRef> {
        self.path_to(self.best_tip().hash)
            .unwrap_or_default()
            .into_iter()
            .map(header_ref)
            .collect()
    }

    /// The best chain's block hash at `height`, genesis included, as
    /// `read::hash_by_height` reads it.
    pub fn hash_at(&self, height: block::Height) -> Option<block::Hash> {
        if height == self.genesis.height {
            return Some(self.genesis.block.hash());
        }
        self.path_to(self.best_tip().hash)?
            .into_iter()
            .find(|committed| committed.height == height)
            .map(|committed| committed.block.hash())
    }

    /// The hash of `tip`'s ancestor at `height`, `tip` itself included, or
    /// `None` if full state doesn't hold `tip` or `height` is above it.
    pub fn ancestor(
        &self,
        tip: zakura_header_chain::Frontier,
        height: block::Height,
    ) -> Option<block::Hash> {
        let mut current = self.get(tip.hash).filter(|tip| tip.height >= height)?;
        while current.height > height {
            current = self.get(current.block.header.previous_block_hash)?;
        }
        Some(current.block.hash())
    }

    /// The serialized size of `hash` if it's on the best chain, as
    /// `read::block_info` reads it.
    fn best_chain_size(&self, hash: block::Hash) -> Option<u32> {
        let committed = self.get(hash)?;
        (self.hash_at(committed.height) == Some(hash)).then(|| serialized_size(&committed.block))
    }

    /// Full state after it commits `block` at `height`, which the verifier
    /// received `receipt`th, as `NonFinalizedState::insert_with` does: past
    /// `MAX_NON_FINALIZED_CHAIN_FORKS` chains, it evicts the lowest-ranked
    /// chain other than `block`'s. The parent must be committed.
    ///
    /// # Panics
    ///
    /// If the best chain would hold more than `MAX_BLOCK_REORG_HEIGHT`
    /// blocks. Production finalizes there, and full state here never does,
    /// so a scenario must stay within that window.
    pub(crate) fn with(
        &self,
        block: Arc<block::Block>,
        height: block::Height,
        receipt: u64,
    ) -> Self {
        let mut after = self.clone();
        let inserted = block.hash();
        after.blocks.insert(
            inserted.0,
            Committed {
                block,
                height,
                receipt,
            },
        );
        loop {
            let tips = after.tips();
            if tips.len() <= MAX_NON_FINALIZED_CHAIN_FORKS {
                break;
            }
            let evicted = tips
                .into_iter()
                .filter(|tip| *tip != inserted)
                .filter_map(|tip| after.blocks.get(&tip.0))
                .min_by_key(|committed| committed.rank())
                .map(|committed| committed.block.hash())
                .expect("the fork limit leaves room for the inserted chain");
            after.evict_branch(evicted);
        }
        assert!(
            after.best_chain_len() <= MAX_BLOCK_REORG_HEIGHT,
            "full state never finalizes, so a scenario stays within {MAX_BLOCK_REORG_HEIGHT} \
             non-finalized blocks",
        );
        after
    }

    /// Removes the chain whose tip is `tip`: each block from the tip down
    /// that no other chain holds.
    fn evict_branch(&mut self, tip: block::Hash) {
        let mut current = tip;
        while let Some(committed) = self.blocks.remove(&current.0) {
            let parent = committed.block.header.previous_block_hash;
            let shared = self
                .blocks
                .values()
                .any(|other| other.block.header.previous_block_hash == parent);
            if shared {
                break;
            }
            current = parent;
        }
    }
}

fn frontier(committed: &Committed) -> zakura_header_chain::Frontier {
    zakura_header_chain::Frontier::new(committed.height, committed.block.hash())
}

fn header_ref(committed: &Committed) -> VerifiedHeaderRef {
    VerifiedHeaderRef {
        height: committed.height,
        hash: committed.block.hash(),
        header: committed.block.header.clone(),
    }
}

fn serialized_size(block: &block::Block) -> u32 {
    use zakura_chain::serialization::ZcashSerialize;
    u32::try_from(block.zcash_serialized_size()).expect("a block fits MAX_BLOCK_BYTES")
}

/// The engine event that commits `accepted`, taking full state from
/// `before` to `after`, at `snapshot`'s version. A port of
/// `verified_request` in zakura-state's `service/write.rs`: a new best
/// chain that extends the old one grows it, any other new best chain resets
/// it, and a block off the best chain is accepted as a side path.
///
/// # Errors
///
/// If `before`'s best tip isn't the engine's verified tip, or `after`
/// doesn't hold `accepted`.
pub fn verified_request(
    before: &VerifiedChain,
    after: &VerifiedChain,
    accepted: zakura_header_chain::Frontier,
    snapshot: &EngineSnapshot,
) -> Result<TransitionRequest, StoreError> {
    let old_path = before.best_path();
    let new_path = after.best_path();
    let old_frontier = before.best_tip();
    if old_frontier != snapshot.frontiers.verified_best {
        return Err(StoreError::Incoherent(
            "full state's best tip differs from the engine's verified tip",
        ));
    }
    let best_changed =
        old_path.last().map(|header| header.hash) != new_path.last().map(|header| header.hash);
    let event = if best_changed {
        let (cause, changed_path) = classify_verified_change(&old_path, &new_path);
        let event_path = changed_path.to_vec();
        let evidence = full_state_evidence(
            match cause {
                VerifiedChangeCause::Grow => b"grow",
                VerifiedChangeCause::CheckpointFinalizedGrow => b"checkpoint-grow",
                VerifiedChangeCause::Reset => b"reset",
            },
            snapshot.state_version,
            accepted.hash,
            &event_path,
        );
        TransitionEvent::VerifiedChainChanged(VerifiedChainChanged {
            full_state_transition_id: evidence,
            old_tip: old_frontier,
            new_path: event_path,
            cause,
        })
    } else {
        let accepted_path: Vec<_> = after
            .path_to(accepted.hash)
            .filter(|path| path.last().map(|committed| committed.height) == Some(accepted.height))
            .ok_or(StoreError::Incoherent(
                "accepted full-state block is absent from its staged side path",
            ))?
            .into_iter()
            .map(header_ref)
            .collect();
        let evidence = full_state_evidence(
            b"verified-side-path",
            snapshot.state_version,
            accepted.hash,
            &accepted_path,
        );
        TransitionEvent::VerifiedBlockAccepted(VerifiedBlockAccepted {
            full_state_transition_id: evidence,
            path: accepted_path,
        })
    };
    Ok(TransitionRequest {
        expected_version: snapshot.state_version,
        event,
    })
}

/// zakura-state's `classify_verified_change`.
fn classify_verified_change<'a>(
    old_path: &[VerifiedHeaderRef],
    new_path: &'a [VerifiedHeaderRef],
) -> (VerifiedChangeCause, &'a [VerifiedHeaderRef]) {
    let grows = new_path.len() > old_path.len()
        && new_path
            .iter()
            .zip(old_path)
            .all(|(new, old)| new.hash == old.hash);
    if grows {
        (VerifiedChangeCause::Grow, &new_path[old_path.len()..])
    } else {
        (VerifiedChangeCause::Reset, new_path)
    }
}

/// zakura-state's `full_state_evidence`.
fn full_state_evidence(
    tag: &[u8],
    version: StateVersion,
    target: block::Hash,
    path: &[VerifiedHeaderRef],
) -> EvidenceId {
    let mut hasher = Sha256::new();
    hasher.update(b"zakura-full-state-header-transition-v1");
    hasher.update(tag);
    hasher.update(version.get().to_be_bytes());
    hasher.update(target.0);
    for header in path {
        hasher.update(header.height.0.to_be_bytes());
        hasher.update(header.hash.0);
    }
    EvidenceId::from_digest(hasher.finalize().into())
}

/// One missing-body read: zakura-state's `BlockSyncBodyMetadata`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BodyMetadata {
    /// The highest block on both full state's best chain and the selected
    /// header chain.
    pub anchor: zakura_header_chain::Frontier,
    /// Each selected header in the window whose body full state lacks, with
    /// a size if one is known.
    pub blocks: Vec<(block::Height, block::Hash, Option<u32>)>,
}

/// The selected headers from `from` whose bodies full state lacks, at most
/// `limit` of them and at most 4,000. A port of zakura-state's
/// `missing_block_body_metadata`, with the engine's selected projection and
/// `chain` read together, as `with_selected_projection` reads them.
///
/// A body full state holds on its best chain reports its real size. Any
/// other reports the size its header's auxiliary deliveries advertise.
///
/// # Errors
///
/// If the selected projection and full state share no block.
pub fn missing_block_body_metadata(
    engine: &HeaderChainEngine,
    chain: &VerifiedChain,
    from: block::Height,
    limit: u32,
) -> Result<BodyMetadata, StoreError> {
    let selected = engine.selected_projection();
    let verified_tip = chain.best_tip();
    let Some(best_header_tip) = selected.last().copied() else {
        return Err(StoreError::Incoherent(
            "block sync has no selected or full-state frontier",
        ));
    };
    let minimum_height = selected
        .first()
        .expect("the selected projection has a best header")
        .height;
    let selected_hash = |height: block::Height| {
        selected
            .binary_search_by_key(&height, |frontier| frontier.height)
            .ok()
            .map(|index| selected[index].hash)
    };
    let anchor = highest_common_body_header_frontier(
        verified_tip.height.min(best_header_tip.height),
        minimum_height,
        |height| chain.hash_at(height),
        selected_hash,
    )?;

    let first_selected = anchor.height.next().unwrap_or(anchor.height);
    let repairing_fork = verified_tip != anchor;
    let verified_successor = verified_tip.height.next().ok();
    let start = if repairing_fork && verified_successor == Some(from) {
        first_selected
    } else {
        first_selected.max(from)
    };
    if start > best_header_tip.height {
        return Ok(BodyMetadata {
            anchor,
            blocks: Vec::new(),
        });
    }
    let count = limit.min(MAX_HEADER_SYNC_HEIGHT_RANGE).min(
        best_header_tip
            .height
            .0
            .saturating_sub(start.0)
            .saturating_add(1),
    );

    let mut blocks = Vec::new();
    for offset in 0..count {
        let Some(height) = start.0.checked_add(offset).map(block::Height) else {
            break;
        };
        let Some(hash) = selected_hash(height) else {
            continue;
        };
        // Only the finalized database's bodies are skipped here, and
        // genesis is the only finalized block.
        if height <= chain.finalized().height && chain.hash_at(height) == Some(hash) {
            continue;
        }
        // Advisory sizes come from the retained deliveries of the selected
        // header's own hash, so a fork can't borrow another block's size.
        let size = chain.best_chain_size(hash).or_else(|| {
            AuxDelivery::advertised_body_size(engine.aux_deliveries(hash)).map(|size| size.get())
        });
        blocks.push((height, hash, size));
    }
    Ok(BodyMetadata { anchor, blocks })
}

/// zakura-state's `highest_common_body_header_frontier`: walks down from
/// `height` to the first height where full state and the selected headers
/// agree.
fn highest_common_body_header_frontier(
    mut height: block::Height,
    minimum_height: block::Height,
    mut body_hash: impl FnMut(block::Height) -> Option<block::Hash>,
    mut selected_hash: impl FnMut(block::Height) -> Option<block::Hash>,
) -> Result<zakura_header_chain::Frontier, StoreError> {
    loop {
        if let (Some(body_hash), Some(selected_hash)) = (body_hash(height), selected_hash(height)) {
            if body_hash == selected_hash {
                return Ok(zakura_header_chain::Frontier::new(height, body_hash));
            }
        }
        if height <= minimum_height {
            return Err(StoreError::Incoherent(
                "selected headers and full state have no common ancestor",
            ));
        }
        height = block::Height(height.0.saturating_sub(1));
    }
}

/// Why [`query_needed_blocks`] failed.
#[derive(Debug)]
pub enum NeededBlocksError {
    /// The query's limit was 0.
    ZeroLimit,
    /// Two chunks of the query read different anchors.
    AnchorChanged,
    /// A chunk's state read failed.
    State(StoreError),
}

impl fmt::Display for NeededBlocksError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NeededBlocksError::ZeroLimit => {
                f.write_str("block-sync needed-body query limit must be nonzero")
            }
            NeededBlocksError::AnchorChanged => {
                f.write_str("block-sync body anchor changed across one chunked state query")
            }
            NeededBlocksError::State(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for NeededBlocksError {}

/// The driver's needed-body query: `limit` heights from `from`, read in
/// chunks of up to 4,000 by `chunk`, each failing the query if its anchor
/// differs from the first. A port of `query_block_sync_needed_blocks` and
/// `block_sync_needed_blocks_from_state` in zakurad's block driver: a
/// positive size is an advertised estimate, and anything else is unknown.
///
/// # Errors
///
/// If `limit` is 0, a chunk fails, or the anchor moves between chunks.
pub fn query_needed_blocks(
    from: block::Height,
    limit: u32,
    mut chunk: impl FnMut(block::Height, u32) -> Result<BodyMetadata, StoreError>,
) -> Result<(zakura_header_chain::Frontier, Vec<BlockSyncBlockMeta>), NeededBlocksError> {
    if limit == 0 {
        return Err(NeededBlocksError::ZeroLimit);
    }
    let mut needed = Vec::new();
    let mut body_anchor = None;
    let mut next_from = from;
    let mut remaining = limit;
    while remaining > 0 {
        let chunk_limit = remaining.min(MAX_HEADER_SYNC_HEIGHT_RANGE);
        let metadata = chunk(next_from, chunk_limit).map_err(NeededBlocksError::State)?;
        if body_anchor
            .replace(metadata.anchor)
            .is_some_and(|anchor| anchor != metadata.anchor)
        {
            return Err(NeededBlocksError::AnchorChanged);
        }
        needed.extend(metadata.blocks.into_iter().map(|(height, hash, size)| {
            BlockSyncBlockMeta {
                height,
                hash,
                size: size
                    .filter(|size| *size > 0)
                    .map(BlockSizeEstimate::Advertised)
                    .unwrap_or(BlockSizeEstimate::Unknown),
            }
        }));
        remaining = remaining.saturating_sub(chunk_limit);
        let Some(after_chunk) = next_from.0.checked_add(chunk_limit).map(block::Height) else {
            break;
        };
        next_from = after_chunk;
    }
    let body_anchor = body_anchor.expect("a nonzero limit reads at least one chunk");
    Ok((body_anchor, needed))
}

/// The bounded range read serving makes: the contiguous blocks from `start`,
/// at most `count` of them and `max_body_bytes` in all, from the full-state
/// chain that holds the highest selected header with a committed body. A
/// port of `read_owned_block_range`, `block_range_snapshot` and
/// `serialized_block_within` in zakura-state's `service/block_range.rs`.
///
/// `cancelled` is checked before each lookup, and a cancelled read returns
/// what it already read.
///
/// # Errors
///
/// If the selected projection disagrees with the engine's frontiers.
#[allow(clippy::unwrap_in_result)]
pub fn read_block_range(
    engine: &HeaderChainEngine,
    chain: &VerifiedChain,
    start: block::Height,
    count: u32,
    max_body_bytes: u32,
    mut cancelled: impl FnMut() -> bool,
) -> Result<Vec<(block::Height, Vec<u8>)>, StoreError> {
    use zakura_chain::serialization::ZcashSerialize;

    if cancelled() {
        return Ok(Vec::new());
    }
    let snapshot = engine.snapshot();
    let selected = engine.selected_projection();
    if selected.first().copied() != Some(snapshot.frontiers.finalized)
        || selected.last().copied() != Some(snapshot.frontiers.header_best)
    {
        return Err(StoreError::Incoherent(
            "selected projection disagrees with its published bounds",
        ));
    }
    // The overlap ends at full state's highest chain tip.
    let overlap_tip = chain
        .tips()
        .into_iter()
        .filter_map(|hash| chain.get(hash).map(|committed| committed.height))
        .max()
        .unwrap_or(chain.finalized().height);
    // The projection is ordered by height.
    let overlap = selected.partition_point(|frontier| frontier.height <= overlap_tip);
    let tip = selected[..overlap]
        .iter()
        .rev()
        .find(|frontier| chain.contains(frontier.hash))
        .copied()
        .unwrap_or(chain.finalized());
    let branch: Vec<&Committed> = std::iter::once(&chain.genesis)
        .chain(chain.path_to(tip.hash).unwrap_or_default())
        .collect();

    // `collect_bounded_height_range`.
    // Widening u32 to usize is lossless on supported targets.
    let mut remaining = max_body_bytes as usize;
    let mut blocks = Vec::new();
    for offset in 0..count {
        let Some(height) = start.0.checked_add(offset).map(block::Height) else {
            break;
        };
        if height > tip.height || cancelled() {
            break;
        }
        // Widening u32 to usize is lossless on supported targets.
        let Some(committed) = branch.get(height.0 as usize) else {
            break;
        };
        // Full state records each size, so a block that can't fit stops
        // the read before it's serialized.
        // Widening u32 to usize is lossless on supported targets.
        let size = serialized_size(&committed.block) as usize;
        if size > remaining {
            break;
        }
        let bytes = committed
            .block
            .zcash_serialize_to_vec()
            .expect("a committed block serializes because writing to a Vec is infallible");
        remaining -= size;
        blocks.push((height, bytes));
    }
    Ok(blocks)
}

/// The headers whose scheduling size `change_set` changes, read before the
/// engine installs it. A port of the hint-hash step in zakura-state's
/// `apply_combined_inner`, sorted by hash bytes so every process lists them
/// in one order.
pub(crate) fn hint_hashes(engine: &HeaderChainEngine, change_set: &ChangeSet) -> Vec<block::Hash> {
    let hashes: BTreeSet<[u8; 32]> = change_set
        .aux_changes
        .iter()
        .filter_map(|change| {
            let AuxDelta::Put(delivery) = change else {
                return None;
            };
            (delivery.scheduling_body_size().is_some()
                && engine
                    .aux_deliveries(delivery.header_hash)
                    .iter()
                    .find(|old| old.delivery_id == delivery.delivery_id)
                    .is_none_or(|old| old.effective_body_size() != delivery.effective_body_size()))
            .then_some(delivery.header_hash.0)
        })
        .collect();
    hashes.into_iter().map(block::Hash).collect()
}

/// The size-hint updates for `hashes`, read after the engine installed
/// their change, in height order.
pub(crate) fn hint_updates(
    engine: &HeaderChainEngine,
    hashes: Vec<block::Hash>,
) -> Vec<(block::Height, block::Hash, BodySizeHint)> {
    let mut updates: Vec<_> = hashes
        .into_iter()
        .filter_map(|hash| {
            let node = engine.graph().header_node(hash)?;
            let size = AuxDelivery::advertised_body_size(engine.aux_deliveries(hash))?;
            Some((node.height, hash, BodySizeHint::Known(size)))
        })
        .collect();
    updates.sort_by_key(|(height, hash, _)| (*height, hash.0));
    updates
}

/// The committed-view publisher: zakura-state's `Publisher` for views. It
/// keeps the last view and the watch that carries it.
#[derive(Debug)]
pub(crate) struct ViewPublisher {
    view: CommittedHeaderChainView,
    /// `None` once shutdown closed the watch.
    sender: Option<watch::Sender<Option<CommittedHeaderChainView>>>,
}

/// How many hint batches a view keeps, `RETAINED_HINT_BATCHES` in
/// zakura-state.
const RETAINED_HINT_BATCHES: usize = 16;

impl ViewPublisher {
    /// A publisher whose first view is `snapshot` at the first body-work
    /// epoch, as `Publisher::new` builds it.
    pub(crate) fn new(snapshot: EngineSnapshot) -> Self {
        let view = CommittedHeaderChainView::new(snapshot, BodyWorkEpoch::default());
        let (sender, _) = watch::channel(Some(view.clone()));
        ViewPublisher {
            view,
            sender: Some(sender),
        }
    }

    /// The last view.
    pub(crate) fn view(&self) -> CommittedHeaderChainView {
        self.view.clone()
    }

    /// A clone of the watch's sender, or `None` once it's closed.
    pub(crate) fn sender(&self) -> Option<watch::Sender<Option<CommittedHeaderChainView>>> {
        self.sender.clone()
    }

    /// Takes the watch's sender, so the watch closes once its clones drop.
    pub(crate) fn close(&mut self) -> Option<watch::Sender<Option<CommittedHeaderChainView>>> {
        self.sender.take()
    }

    /// The next view, for a committed `snapshot` whose transition had
    /// `effect` and changed `updates`' size hints. A port of
    /// `publish_with_hint_changes`: an effect that invalidates body work
    /// starts the next epoch, and nonempty updates start the next hint
    /// revision and add a batch. The caller sends the view.
    pub(crate) fn next(
        &mut self,
        snapshot: EngineSnapshot,
        effect: TransitionEffect,
        updates: Vec<(block::Height, block::Hash, BodySizeHint)>,
    ) -> CommittedHeaderChainView {
        let body_work_epoch = if effect.invalidates_body_work() {
            self.view
                .body_work_epoch
                .checked_next()
                .expect("a process cannot commit u64::MAX body-work invalidations")
        } else {
            self.view.body_work_epoch
        };
        let mut view = CommittedHeaderChainView::new(snapshot, body_work_epoch);
        view.body_size_hint_batches = self.view.body_size_hint_batches.clone();
        view.body_size_hint_revision = self.view.body_size_hint_revision;
        if !updates.is_empty() {
            view.body_size_hint_revision = view
                .body_size_hint_revision
                .checked_add(1)
                .expect("a process cannot commit u64::MAX hint corrections");
            let mut batches = view
                .body_size_hint_batches
                .as_deref()
                .unwrap_or_default()
                .to_vec();
            if batches.len() == RETAINED_HINT_BATCHES {
                batches.remove(0);
            }
            batches.push(BodySizeHintBatch {
                revision: view.body_size_hint_revision,
                updates: updates.into(),
            });
            view.body_size_hint_batches = Some(batches.into());
        }
        self.view = view.clone();
        view
    }
}

/// zakura-state's `PreparedAuthority`: authorizes exactly one staged event,
/// with full state's tip, its evicted bodies and its chain tips.
#[derive(Debug)]
pub(crate) struct PreparedAuthority {
    transition: TransitionFingerprint,
    pub(crate) retention_references: Vec<block::Hash>,
    pub(crate) verified_tip: Option<zakura_header_chain::Frontier>,
    pub(crate) evicted_bodies: Vec<block::Hash>,
}

impl PreparedAuthority {
    /// `PreparedAuthority::for_event`.
    pub(crate) fn for_event(event: &TransitionEvent) -> Result<Self, StoreError> {
        event
            .fingerprint()
            .map(|transition| PreparedAuthority {
                transition,
                retention_references: Vec::new(),
                verified_tip: None,
                evicted_bodies: Vec::new(),
            })
            .ok_or(StoreError::Incoherent(
                "prepared full-state event has no stable identity",
            ))
    }
}

impl FullStateEvidenceAuthority for PreparedAuthority {
    fn authorizes_full_state(&self, event: &TransitionEvent) -> bool {
        event.fingerprint() == Some(self.transition)
    }

    fn verified_tip(&self, event: &TransitionEvent) -> Option<zakura_header_chain::Frontier> {
        self.authorizes_full_state(event)
            .then_some(self.verified_tip)
            .flatten()
    }

    fn evicted_bodies(&self, event: &TransitionEvent) -> &[block::Hash] {
        if self.authorizes_full_state(event) {
            &self.evicted_bodies
        } else {
            &[]
        }
    }

    fn authorizes_retention_reference(&self, reference: block::Hash) -> bool {
        self.retention_references.contains(&reference)
    }
}

/// zakura-state's `PreparedSchedulerAuthority`: authorizes exactly one
/// scheduler retry.
#[derive(Debug)]
pub(crate) struct SchedulerAuthority(pub(crate) OperatorBodyRetry);

impl FullStateEvidenceAuthority for SchedulerAuthority {
    fn authorizes_full_state(&self, _event: &TransitionEvent) -> bool {
        false
    }

    fn authorizes_scheduler_retry(&self, retry: &OperatorBodyRetry) -> bool {
        *retry == self.0
    }
}

/// zakura-state's `StateIssuedAuthority`: adds the validation leases the
/// writer issued for one apply, full state's chain tips, and the version it
/// read to `inner`.
pub(crate) struct StateIssuedAuthority<'a> {
    pub(crate) inner: Option<&'a dyn FullStateEvidenceAuthority>,
    pub(crate) validation_leases: &'a [ValidationLease],
    pub(crate) active_retention_references: &'a [block::Hash],
    pub(crate) full_state_authorization_version: Option<StateVersion>,
}

impl FullStateEvidenceAuthority for StateIssuedAuthority<'_> {
    fn evicted_bodies(&self, event: &TransitionEvent) -> &[block::Hash] {
        self.inner.map_or(&[], |inner| inner.evicted_bodies(event))
    }

    fn verified_tip(&self, event: &TransitionEvent) -> Option<zakura_header_chain::Frontier> {
        self.inner.and_then(|inner| inner.verified_tip(event))
    }

    fn authorizes_full_state(&self, event: &TransitionEvent) -> bool {
        self.inner
            .is_some_and(|inner| inner.authorizes_full_state(event))
    }

    fn full_state_authorization_version(&self, event: &TransitionEvent) -> Option<StateVersion> {
        if self.authorizes_full_state(event) {
            self.full_state_authorization_version
        } else {
            None
        }
    }

    fn authorizes_scheduler_retry(&self, retry: &OperatorBodyRetry) -> bool {
        self.inner
            .is_some_and(|inner| inner.authorizes_scheduler_retry(retry))
    }

    fn authorizes_header_completion(&self, insert: &zakura_header_chain::InsertHeaders) -> bool {
        self.inner
            .is_some_and(|inner| inner.authorizes_header_completion(insert))
    }

    fn authorizes_validation_lease(&self, lease: &ValidationLease) -> bool {
        self.validation_leases.contains(lease)
    }

    fn authorizes_retention_reference(&self, reference: block::Hash) -> bool {
        self.active_retention_references.contains(&reference)
            || self
                .inner
                .is_some_and(|inner| inner.authorizes_retention_reference(reference))
    }
}

/// zakura-state's `combined_retention_references`: the context's references
/// and the active ones, sorted by hash bytes and deduplicated.
pub(crate) fn combined_retention_references(
    context: &[block::Hash],
    active: &[block::Hash],
) -> Vec<block::Hash> {
    let mut references: Vec<_> = context.iter().chain(active).copied().collect();
    references.sort_unstable_by_key(|hash| hash.0);
    references.dedup();
    references
}
