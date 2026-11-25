//! Pure node-wide block coordinator (`BlockTable`).
//!
//! `BlockTable` owns `top`, bounded pending metadata, admitted blocks, stripe
//! bitsets and verified parts, arrival-ordered pull planning (`PeerPaths`),
//! decode/check scheduling, the monotonic recovery ladder (`Normal -> Repair
//! -> Fallback`), and retention retirement.
//!
//! It holds values only: no locks, channels, tasks, or I/O handles. In
//! production the IO layer wraps `BlockTable` in a short non-awaited `Mutex`.

use std::{sync::Arc, time::Duration};

use zakura_chain::block;
use zakura_quic::NodeId;
use zakura_rt::{
    collections::{HashMap, HashSet},
    time::Instant,
};

use crate::peer::block_gossip::ADVERT_RANDOM_INTERVAL;

use super::{
    close::Bpv1CloseRow,
    commitment::CoinbaseBinding,
    config::HavePacing,
    paths::{arrival_duration, Arrival, PathDirection, PathEstimate, PeerPaths, RateSample},
    peer::{PartAuth, SLOW_X},
    profile::{
        Coding, Profile, W2_MAX_COINBASE_BYTES, W2_MAX_COMMITMENT_PATH_DEPTH, W2_MAX_HEADER_BYTES,
    },
    tree::{encoding_id, VerifiedPart},
    wire::{Bpv1Message, WantBitmap, PROPAGATION_DEPTH},
};

#[cfg(any(test, feature = "p2p-fuzz"))]
mod fuzz;
#[cfg(any(test, feature = "p2p-fuzz"))]
mod invariants;

#[cfg(any(test, feature = "p2p-fuzz"))]
pub use fuzz::{replay_bpv1_block_bytes, BlockReplaySummary};

/// The stall interval before the first repair round: `SLOW_X` × one part's
/// estimated arrival through the median path, at least 1 ms (decision-22).
/// Round `r` waits this × `2^r` after the previous one; rounds repeat until
/// the deadline (decision-17).
pub fn stall_base(paths: &PeerPaths, coding: &Coding) -> Duration {
    arrival_duration(
        paths.median_rtt(),
        u64::from(coding.part_bytes),
        paths.median_rate(PathDirection::Pull),
    )
    .max(Duration::from_millis(1))
    .saturating_mul(SLOW_X)
}

/// Monotonic reconstruction deadline from admission (`W2` baseline, `bpv1-57`).
/// It is the only recovery bound (decision-17).
pub const RECONSTRUCTION_DEADLINE: Duration = Duration::from_secs(15);

/// Monotonic verification deadline from a block's first verifier submission
/// (`bpv1-82`).
///
/// The body claim lasts until this deadline while the block verifies
/// (`bpv1-44`). Past it, the block falls back to block sync and frees its slot.
/// It matches the verifier's 2-minute wait for another delivery's pending
/// commit, and the node's verifier adapter cancels its router call at the same
/// bound.
pub const VERIFY_DEADLINE: Duration = Duration::from_secs(120);

/// Maximum metadata entries in header admission or the commitment check.
/// Pending metadata never counts here (`bpv1-75`).
pub const MAX_ADMITTING_META_COUNT: usize = 64;

/// Maximum bytes held in metadata in header admission or the commitment check.
pub const MAX_ADMITTING_META_BYTES: usize = 8 * 1024 * 1024;

/// Maximum metadata entries waiting on missing parents (`bpv1-11`).
pub const MAX_PENDING_META_COUNT: usize = 64;

/// Maximum bytes held in metadata waiting on missing parents (`bpv1-11`).
pub const MAX_PENDING_META_BYTES: usize = 8 * 1024 * 1024;

/// Maximum metadata entries one peer keeps waiting on missing parents
/// (`bpv1-11`). Header sync normally delivers a parent before its child's
/// metadata arrives, so an honest peer rarely has more than one waiting; a
/// block whose metadata does not fit arrives through block sync instead.
pub const MAX_PENDING_META_PER_PEER: usize = 4;

// decision-23: each metadata budget holds at least four maximum `HeaderMeta`
// envelopes, whose coinbase may reach `MAX_BLOCK_BYTES`.
const _: () = assert!(MAX_ADMITTING_META_BYTES >= 4 * MetaEnvelope::MAX_ESTIMATED_BYTES);
const _: () = assert!(MAX_PENDING_META_BYTES >= 4 * MetaEnvelope::MAX_ESTIMATED_BYTES);

/// Maximum age of an unresolved pending metadata entry (`bpv1-11`).
pub const PENDING_META_AGE: Duration = Duration::from_secs(15);

/// Default maximum number of concurrently active collecting/decoding blocks.
pub const DEFAULT_MAX_CONCURRENT_BLOCKS: usize = 4;

/// Default budget for peer part bytes charged before proof.
pub const DEFAULT_PART_BUFFER_BYTES: u64 = 32 * 1024 * 1024;

/// Unique monotonic work identity carried across async port/worker boundaries (`bpv1-63`).
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct WorkId(pub u64);

/// Configuration knobs for [`BlockTable`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct BlockConfig {
    /// Active BPv1 coding profile (`Profile::W2` or `Profile::TINY`).
    pub profile: Profile,
    /// Maximum concurrently active (non-retired) blocks admitted for collection.
    pub max_concurrent_blocks: usize,
    /// Budget for peer part bytes charged before proof, in flight or retained
    /// (`bpv1-52`).
    pub part_buffer_bytes: u64,
    /// Main's advertisement pacing for unflagged `Have`s (decision-26).
    pub have_pacing: HavePacing,
}

impl BlockConfig {
    /// Returns defaults for `profile`.
    pub fn for_profile(profile: Profile) -> Self {
        Self {
            profile,
            max_concurrent_blocks: DEFAULT_MAX_CONCURRENT_BLOCKS,
            part_buffer_bytes: DEFAULT_PART_BUFFER_BYTES,
            have_pacing: HavePacing::default(),
        }
    }
}

/// Why [`BlockTable::charge_part`] refused a part before proof. The IO shell
/// drops the part without faulting the peer.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PartRefusal {
    /// The block is unknown or retired.
    NotAdmitted,
    /// The part buffer budget is full (`LocalCapacity`).
    LocalCapacity,
}

/// Terminal ladder outcome of a block this node collected (`zakura.p2p.bpv1.ladder.outcome`).
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum BlockOutcome {
    /// Committed without a repair round.
    Normal,
    /// Committed after at least one repair round.
    Repair,
    /// Handed to block-sync fallback.
    Fallback,
}

/// Lifecycle phase of an admitted block.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum BlockPhase {
    /// Collecting parts across stripes.
    Collecting,
    /// Assembled block handed to the consensus verifier.
    Verifying,
    /// Committed / completed.
    Done,
    /// Handed off to native block sync fallback (`bpv1-41`, `bpv1-43`, `bpv1-57`).
    Fallback,
    /// Retired; buffers released (`bpv1-46`).
    Retired,
}

/// Why a block entered fallback.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum FallbackReason {
    /// Monotonic reconstruction deadline elapsed.
    Deadline,
    /// Stripe re-encode check did not match committed `stripe_root` (`bpv1-41`).
    ReencodeFailed,
    /// Assembled block's tx or auth-data root differed from header (`bpv1-43`).
    RootMismatch,
    /// Local decoded-block pool capacity or retryable verifier backpressure.
    LocalCapacity,
    /// The verification deadline elapsed before a verdict (`bpv1-82`).
    VerifyDeadline,
}

/// How a received part was requested (`part.received{source}`).
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PartSource {
    /// A proposer seed flagged for relay: a pushed seed (decision-24).
    Seed,
    /// A proposer seed flagged for push only: loss cover or a direct copy
    /// (decision-19).
    Direct,
    /// A holder's push under a push slot (decision-24).
    Push,
    /// A `Want` while the block's ladder is `Normal`.
    Pull,
    /// A `Want` after the block entered repair.
    Repair,
}

/// Result of `HeaderAdmission::admit`.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum HeaderAdmissionResult {
    /// Exact header admitted into headerchain at `height`.
    Admitted {
        /// Consensus height of the admitted header.
        height: u32,
        /// Hash of the header's parent.
        parent_hash: block::Hash,
    },
    /// Parent header is not yet in headerchain.
    MissingParent {
        /// Hash of the missing parent header.
        parent_hash: block::Hash,
    },
    /// Header is invalid by consensus/headerchain rules (`close-3`).
    PeerInvalid,
    /// Local storage/service error; never faults peer.
    LocalError,
}

/// Verdict returned by `BlockVerifier::submit`.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum VerifierVerdict {
    /// Block committed as valid.
    Valid,
    /// Body transaction or auth-data root differs from header (`decision-8`, `bpv1-43`).
    RootMismatch,
    /// Local decoded-block pool capacity or retryable verifier backpressure.
    LocalCapacity,
    /// Consensus-invalid with matching roots (`bpv1-45`).
    Invalid,
    /// Another path already delivered the block.
    Duplicate,
}

/// Reason a body claim is released to `BlockFallback`.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ClaimRelease {
    /// Block committed (or duplicate).
    Committed,
    /// Handed off to native block sync fallback.
    Fallback,
    /// The first repair round lets native block sync race (decision-22).
    Repair,
    /// Retired / consensus-invalid / evicted.
    Retired,
}

/// Decoded `HeaderMeta` envelope presented to [`BlockTable`].
#[derive(Clone, Debug)]
pub struct MetaEnvelope {
    /// Block consensus hash.
    pub block_id: block::Hash,
    /// Serialized full header bytes.
    pub header_bytes: Vec<u8>,
    /// Coinbase transaction + commitment proofs.
    pub binding: CoinbaseBinding,
    /// Validated body coding parameters.
    pub coding: Coding,
    /// Committed Merkle root of all coded parts.
    pub part_root: [u8; 32],
    /// Proposer's transport identity.
    pub proposer_node: NodeId,
}

impl MetaEnvelope {
    /// Fixed charge for the envelope's small fields.
    const FIXED_BYTES: usize = 128;

    /// The largest [`Self::estimated_bytes`] a decoded `HeaderMeta` yields.
    pub const MAX_ESTIMATED_BYTES: usize = W2_MAX_HEADER_BYTES
        + W2_MAX_COINBASE_BYTES
        + 2 * W2_MAX_COMMITMENT_PATH_DEPTH * 32
        + Self::FIXED_BYTES;

    /// Approximate memory charge of this envelope against the pending-meta budget.
    pub fn estimated_bytes(&self) -> usize {
        self.header_bytes.len()
            + self.binding.coinbase.len()
            + (self.binding.txid_siblings.len() + self.binding.auth_siblings.len()) * 32
            + Self::FIXED_BYTES
    }

    /// The `HeaderMeta` to forward. It is the same on every connection.
    pub fn header_meta(&self) -> Box<Bpv1Message> {
        Box::new(Bpv1Message::HeaderMeta {
            header: self.header_bytes.clone(),
            binding: self.binding.clone(),
            coding: self.coding,
            part_root: self.part_root,
            proposer_node: self.proposer_node,
        })
    }
}

/// An admitted block descriptor that every live `PeerState` learns.
#[derive(Clone, Debug)]
pub struct AdmittedBlock {
    /// Block hash.
    pub block_id: block::Hash,
    /// Block coding layout.
    pub coding: Coding,
    /// Proposer node identity.
    pub proposer_node: NodeId,
    /// Wire `HeaderMeta` to send to peers that did not send it.
    pub header_meta: Box<Bpv1Message>,
    /// Whether this node is the proposer.
    pub as_proposer: bool,
    /// Peers that hear of each verified part at once (decision-26).
    pub have_group: Vec<NodeId>,
    /// When every other peer may hear of parts: admission plus the
    /// advertisement delay. `None` for this node's own block, which it
    /// never announces (decision-20).
    pub have_deadline: Option<Instant>,
}

/// Why a peer's pull claims are released (`BlockInput::ReleaseClaims`).
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ReleaseReason {
    /// `WantLate`: the peer may still deliver. Each slot is released this way
    /// at most once per block (`bpv1-64`). The peer counts as tried.
    Late,
    /// The peer failed the slot: `WantEnd` arrived without it, the `Want`
    /// deadline passed, or the peer already owes it on a live `Want`. The peer
    /// counts as tried.
    Failed,
    /// A local refusal (parked, window full, mailbox closed) or session end.
    /// The peer never failed the slot, so it stays eligible.
    Refused,
}

/// Snapshot of one peer's pull capacity supplied to the pull planner.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct PullPeerState {
    /// Remote peer identity.
    pub peer: NodeId,
    /// Bytes this peer currently owes from live `Want`s (including late ones).
    pub ahead_bytes: u64,
    /// Whether this peer is open, unparked, and below `MAX_WANTS_INFLIGHT`.
    pub can_pull: bool,
    /// Remaining per-peer pull window slots available.
    pub window_remaining: u32,
}

/// Synchronous input to [`BlockTable::step`].
#[derive(Clone, Debug)]
pub enum BlockInput {
    /// The verified chain tip moved (`bpv1-7`, `bpv1-46`, `bpv1-76`).
    TipHeight {
        /// Height of the new verified tip.
        height: u32,
        /// Hash of the new verified tip.
        hash: block::Hash,
    },
    /// A parent header was admitted into headerchain, waking pending metadata (`bpv1-11`).
    ParentAdmitted {
        /// Hash of the newly admitted parent header.
        hash: block::Hash,
        /// Height of the admitted parent.
        height: u32,
    },
    /// Validated-layout `HeaderMeta` received from a peer (`bpv1-1`, `bpv1-3`, `bpv1-7`).
    MetaReceived {
        /// Decoded metadata envelope.
        envelope: Box<MetaEnvelope>,
        /// Peer that sent this metadata.
        from_peer: NodeId,
        /// Session generation of `from_peer`.
        session_gen: u64,
    },
    /// Completion of `HeaderAdmission::admit` (`bpv1-1`, `bpv1-63`).
    HeaderAdmitted {
        /// Work identity returned by `AdmitHeader`.
        work_id: WorkId,
        /// Admission outcome.
        result: HeaderAdmissionResult,
    },
    /// Completion of coinbase commitment verification (`bpv1-2`, `bpv1-6`, `bpv1-63`).
    CommitmentChecked {
        /// Work identity returned by `VerifyCommitment`.
        work_id: WorkId,
        /// Whether the commitment verified cleanly.
        ok: bool,
        /// Live peers, among which admission picks the `Have` initial group
        /// (decision-26).
        peers: Vec<NodeId>,
        /// Seed for the group's random slots, drawn by the IO shell.
        rng_seed: u64,
    },
    /// Local proposer solved a block: admit directly as `Done` with all parts held (`bpv1-9`).
    BlockAdmittedLocal {
        /// Block height.
        height: u32,
        /// Metadata envelope for the solved block.
        envelope: Box<MetaEnvelope>,
        /// All `stripes * n` verified parts.
        parts: Vec<VerifiedPart>,
    },
    /// `peer` flagged `indices` for push (decision-24). They count as pending
    /// claims held by `peer` until they arrive or turn late (bpv1-64).
    PushFlagged {
        /// The pushing connection.
        peer: NodeId,
        /// Target block.
        block_id: block::Hash,
        /// Flagged wire indices.
        indices: Vec<u32>,
        /// Whether the proposer flagged them for relay: this node's pushed
        /// seeds, which it pushes onward once it holds them (decision-24).
        relay: bool,
    },
    /// Path estimate update from `PathSource` (`decision-12`, `bpv1-40`, `bpv1-65`).
    PathObserved {
        /// Live peer connection.
        peer: NodeId,
        /// Measured path estimate.
        estimate: PathEstimate,
    },
    /// A delivery-rate sample from `peer`'s answer to one `Want` (decision-21).
    RateSample {
        /// Serving peer.
        peer: NodeId,
        /// The sample's times and bytes.
        sample: RateSample,
    },
    /// Peer session ended: drop path entry and release its pull claims (`bpv1-62`, `bpv1-65`).
    SessionEnded {
        /// Disconnected peer.
        peer: NodeId,
        /// Remaining live peers for immediate re-planning.
        peer_states: Vec<PullPeerState>,
    },
    /// `peer` sent `BlockDone` for `block_id`, so it holds every part. Only
    /// repair rounds ask it for parts it did not announce (decision-20).
    PeerBlockDone {
        /// Announcing peer.
        peer: NodeId,
        /// Target block.
        block_id: block::Hash,
        /// Snapshot of all live peers' pull capacities.
        peer_states: Vec<PullPeerState>,
    },
    /// Peer announced holding `indices` of `block_id` with `Have`.
    PeerAnnounced {
        /// Announcing peer.
        peer: NodeId,
        /// Target block.
        block_id: block::Hash,
        /// Part indices announced by `peer`.
        indices: Vec<u32>,
        /// Snapshot of all live peers' pull capacities.
        peer_states: Vec<PullPeerState>,
    },
    /// Explicit pull-planning pass for `(block_id, stripe)` (e.g., after `WantEnd` or part arrival).
    PlanPulls {
        /// Target block.
        block_id: block::Hash,
        /// Target stripe.
        stripe: u32,
        /// Snapshot of live peers' pull capacities.
        peer_states: Vec<PullPeerState>,
    },
    /// Release node-wide pull claims held by `peer`.
    ReleaseClaims {
        /// Peer whose claims are being released.
        peer: NodeId,
        /// Target block.
        block_id: block::Hash,
        /// Target stripe.
        stripe: u32,
        /// Stripe-relative slots to release.
        slots: Vec<u32>,
        /// Why the claims are released; decides whether `peer` counts as tried.
        reason: ReleaseReason,
        /// Snapshot of live peers' pull capacities for immediate re-plan.
        peer_states: Vec<PullPeerState>,
    },
    /// Verified part delivered by a peer (`bpv1-22`, `bpv1-23`, `decision-10`).
    ///
    /// The part's bytes must already be charged by [`BlockTable::charge_part`]
    /// for `(index, from_peer)`; an uncharged part is not stored. Storing the
    /// part keeps the charge; a duplicate or stale part releases it.
    InsertPart {
        /// Target block.
        block_id: block::Hash,
        /// Proof-verified part.
        part: VerifiedPart,
        /// Peer that supplied the part.
        from_peer: NodeId,
        /// How the session authorized the part.
        auth: PartAuth,
        /// Snapshot of live peers' pull capacities (used if a stripe still needs parts).
        peer_states: Vec<PullPeerState>,
    },
    /// Proof verification failed for a part charged by [`BlockTable::charge_part`]:
    /// release its charge.
    ReleasePartCharge {
        /// Target block.
        block_id: block::Hash,
        /// Wire part index.
        index: u32,
        /// Peer that sent the part.
        from_peer: NodeId,
    },
    /// This node uploaded `bytes` of `block_id` part payloads to a peer (`upload.bytes_over_body`).
    PartUploaded {
        /// Block hash.
        block_id: block::Hash,
        /// Uploaded part bytes.
        bytes: u64,
    },
    /// Completion of a stripe decode or re-encode check (`bpv1-41`, `bpv1-63`).
    StripeJobDone {
        /// Work identity from `StartStripeJob`.
        work_id: WorkId,
        /// Target block.
        block_id: block::Hash,
        /// Target stripe.
        stripe: u32,
        /// `Ok(all_n_parts)` on root match, `Err(())` on re-encode failure.
        result: Result<Vec<VerifiedPart>, ()>,
    },
    /// Canonical block assembly succeeded in IO before verifier submission.
    BlockAssembled {
        /// Work identity from `SubmitVerifier`.
        work_id: WorkId,
        /// Target block.
        block_id: block::Hash,
    },
    /// Verdict from `BlockVerifier::submit` (`bpv1-43`, `bpv1-45`, `bpv1-63`).
    VerifierVerdict {
        /// Work identity from `SubmitVerifier`.
        work_id: WorkId,
        /// Target block.
        block_id: block::Hash,
        /// Consensus verdict.
        verdict: VerifierVerdict,
    },
    /// Native block sync committed this block; ends it if it is in fallback (`bpv1-57`).
    FallbackDone {
        /// Committed block.
        block_id: block::Hash,
    },
    /// The local commit of a block this node proposed failed after BPv1
    /// published it; retires the block so it is no longer served (`bpv1-45`).
    ProposedCommitFailed {
        /// The proposed block.
        block_id: block::Hash,
    },
    /// Periodic / timer tick: ages pending metadata, advances stall/repair ladder, enforces deadlines.
    Tick {
        /// Snapshot of live peers' pull capacities for repair pulls.
        peer_states: Vec<PullPeerState>,
    },
}

/// Effect emitted by [`BlockTable::step`] for IO to execute.
#[derive(Clone, Debug)]
pub enum BlockOutput {
    /// Submit exact header to `HeaderAdmission`.
    AdmitHeader {
        /// Work identity for coalescing & staleness filtering.
        work_id: WorkId,
        /// Block hash.
        block_id: block::Hash,
        /// Serialized header bytes.
        header_bytes: Vec<u8>,
        /// Peer that sent `HeaderMeta`.
        from_peer: NodeId,
        /// Session generation of `from_peer`.
        session_gen: u64,
    },
    /// Verify the coinbase commitment for an admitted header on a worker (`bpv1-2`).
    VerifyCommitment {
        /// Work identity.
        work_id: WorkId,
        /// Block hash.
        block_id: block::Hash,
        /// Admitted height from `HeaderAdmission`.
        height: u32,
        /// Metadata envelope to verify.
        envelope: Box<MetaEnvelope>,
    },
    /// Close the offending peer session with a cited close row (`bpv1-61`).
    ClosePeer {
        /// Offending peer.
        peer: NodeId,
        /// Session generation that sent the bad message.
        session_gen: u64,
        /// Cited close row (3 or 4).
        row: Bpv1CloseRow,
    },
    /// Claim body ownership in `BlockFallback` so native block sync skips it.
    ClaimBody {
        /// Claimed block hash.
        block_id: block::Hash,
        /// Monotonic expiry deadline.
        deadline: Instant,
    },
    /// Move BPv1's live body claim to the later `deadline` (`bpv1-44`). An
    /// expired or released claim stays gone, because block sync may already
    /// own the body.
    ExtendBodyClaim {
        /// Claimed block hash.
        block_id: block::Hash,
        /// Monotonic expiry deadline.
        deadline: Instant,
    },
    /// Release body claim in `BlockFallback`.
    ReleaseBodyClaim {
        /// Block hash.
        block_id: block::Hash,
        /// Why the claim is released.
        reason: ClaimRelease,
    },
    /// Publish an admitted block to every live `PeerState`.
    PublishAdmitted {
        /// The admitted block descriptor.
        admitted: AdmittedBlock,
        /// Peer that originally delivered `HeaderMeta` (`None` if local proposer).
        from_peer: Option<NodeId>,
    },
    /// Grant an arrival-ordered pull to `peer`'s `PeerState`.
    GrantPull {
        /// Chosen pull server.
        peer: NodeId,
        /// Block hash.
        block_id: block::Hash,
        /// Target stripe.
        stripe: u32,
        /// Stripe-relative bitmap of parts to request.
        requested: WantBitmap,
        /// Arrival estimate for the last requested part (arms `WantLate`).
        arrival: Arrival,
    },
    /// Notify peer routines that `indices` of `block_id` are verified and servable (`opt-5`).
    PartReady {
        /// Block hash.
        block_id: block::Hash,
        /// Newly servable wire indices.
        indices: Vec<u32>,
        /// Whether `indices` are this node's pushed seeds, which routines
        /// push onward from whichever source delivered them (decision-24,
        /// bpv1-71).
        push: bool,
    },
    /// Notify peer routines that this node holds `k` parts of `stripe` (`StripeDone`).
    NotifyStripeDone {
        /// Block hash.
        block_id: block::Hash,
        /// Completed stripe.
        stripe: u32,
    },
    /// Notify peer routines that this node assembled `block_id` (`BlockDone`).
    NotifyBlockDone {
        /// Block hash.
        block_id: block::Hash,
    },
    /// Decode a stripe or re-check its encoding on a worker (`bpv1-41`, `decision-10`).
    ///
    /// Both kinds run the same decode and re-encode check; `Check` is the
    /// low-priority pass after the all-systematic fast path.
    StartStripeJob {
        /// Work identity.
        work_id: WorkId,
        /// Decode or check.
        kind: StripeJob,
        /// Block hash.
        block_id: block::Hash,
        /// Target stripe.
        stripe: u32,
        /// Coding tuple.
        coding: Coding,
        /// `encoding_id = T("encoding", coding)`.
        enc_id: [u8; 32],
        /// Length-`n` slot vector containing the `k` held verified parts.
        held: Vec<Option<VerifiedPart>>,
    },
    /// Assemble the canonical block from all stripes' `k` source parts and submit to `BlockVerifier` (`bpv1-42`, `bpv1-44`).
    SubmitVerifier {
        /// Work identity.
        work_id: WorkId,
        /// Block hash.
        block_id: block::Hash,
        /// Serialized header bytes.
        header_bytes: Vec<u8>,
        /// Serialized coinbase bytes.
        coinbase_bytes: Vec<u8>,
        /// Coding tuple.
        coding: Coding,
        /// Each stripe's `k` padded source payloads in slot order, one entry
        /// per stripe. Padding parts (decision-18) are zeros. An empty body
        /// has no stripes.
        stripe_sources: Vec<Vec<Arc<[u8]>>>,
    },
    /// Publish block retirement to every live `PeerState`.
    PublishRetire {
        /// Retired block hash.
        block_id: block::Hash,
        /// Stripe count (for retired counters).
        stripes: u32,
        /// Parts per stripe (for retired counters).
        n: u32,
    },
    /// Notify the peer that delivered a `HeaderMeta` that it was neutrally
    /// rejected/expired (clearing `meta_received_pending`).
    MetaRejected {
        /// Block hash.
        block_id: block::Hash,
        /// Peer that sent the metadata.
        peer: NodeId,
        /// Session generation.
        session_gen: u64,
    },
    /// Evict a long-retired block's per-connection counters once safely below
    /// the propagation window.
    PublishEvictRetired {
        /// Evicted block hash.
        block_id: block::Hash,
    },
}

/// Worker job on a stripe that reached `k` parts.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum StripeJob {
    /// Some held parts are parity: decode before the stripe is assemblable.
    Decode,
    /// All `k` source parts are held: the stripe is already assemblable, and
    /// the re-encode check runs at low priority (`decision-10`).
    Check,
}

/// Typed observability event emitted by [`BlockTable::step`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum BlockEvent {
    /// Pending metadata aged out and dropped.
    MetaPendingAged {
        /// Block hash.
        block_id: block::Hash,
    },
    /// Pending metadata was dropped, or refused a place, to keep the pending
    /// budget's per-peer and node-wide bounds (`bpv1-11`).
    MetaPendingDropped {
        /// Block hash.
        block_id: block::Hash,
    },
    /// Coinbase commitment checked and valid.
    CommitmentVerified {
        /// Block hash.
        block_id: block::Hash,
    },
    /// Block admitted into active `BlockTable` state.
    BlockAdmitted {
        /// Block hash.
        block_id: block::Hash,
    },
    /// Verified part arrived at `BlockTable`.
    PartInserted {
        /// Block hash.
        block_id: block::Hash,
        /// Wire part index.
        index: u32,
        /// True when this was the first copy stored (`bpv1-22`).
        first: bool,
        /// Peer that supplied the part.
        from_peer: NodeId,
        /// How the part was requested (`part.received{source}`).
        source: PartSource,
    },
    /// Stripe reached `k` verified parts.
    StripeReachedK {
        /// Block hash.
        block_id: block::Hash,
        /// Stripe index.
        stripe: u32,
    },
    /// All stripes became assemblable.
    Assembled {
        /// Block hash.
        block_id: block::Hash,
        /// Time since `HeaderMeta` arrived (`assembly.seconds`).
        since_meta: Duration,
    },
    /// The block stored its first peer part (`meta_to_first_part.seconds`,
    /// `first_part_to_verified.seconds`).
    FirstPartStored {
        /// Block hash.
        block_id: block::Hash,
        /// From `HeaderMeta` arrival to the first part's charge, before proof.
        meta_to_first_part: Duration,
        /// From that charge to storing the first verified part.
        first_part_to_stored: Duration,
    },
    /// Terminal ladder outcome, emitted once per collected block.
    Outcome {
        /// Block hash.
        block_id: block::Hash,
        /// How the block ended.
        outcome: BlockOutcome,
    },
    /// Upload total of a retired block this node did not propose (`upload.bytes_over_body`).
    Uploaded {
        /// Block hash.
        block_id: block::Hash,
        /// Part bytes uploaded to peers.
        uploaded_bytes: u64,
        /// The block's `body_bytes`.
        body_bytes: u64,
    },
    /// Assembled block handed to `BlockVerifier`.
    Handoff {
        /// Block hash.
        block_id: block::Hash,
    },
    /// A retryable verdict parked the assembled block until its parent commits
    /// (`bpv1-84`).
    Parked {
        /// Block hash.
        block_id: block::Hash,
    },
    /// Verdict received from `BlockVerifier`.
    Verdict {
        /// Block hash.
        block_id: block::Hash,
        /// The verifier's verdict.
        verdict: VerifierVerdict,
    },
    /// Recovery ladder entered repair round `round`.
    RepairRound {
        /// Block hash.
        block_id: block::Hash,
        /// 1-based repair round number.
        round: u8,
    },
    /// Block transitioned to fallback (`bpv1-57`).
    FallbackStarted {
        /// Block hash.
        block_id: block::Hash,
        /// Reason for fallback.
        reason: FallbackReason,
    },
    /// Block retired and buffers released (`bpv1-46`).
    Retired {
        /// Block hash.
        block_id: block::Hash,
    },
    /// At retirement: how many parts of the block this node received again
    /// after it held them, such as a holder push crossing a direct copy
    /// (`part.duplicates`, decision-19).
    PartDuplicates {
        /// Block hash.
        block_id: block::Hash,
        /// Second copies received.
        count: u32,
    },
    /// A holder's flagged pushes turned late; the late rule released them
    /// (decision-24, bpv1-64).
    PushLate {
        /// The late holder.
        peer: NodeId,
        /// Block hash.
        block_id: block::Hash,
    },
    /// Late pull released a slot claim once (`bpv1-64`).
    PullLate {
        /// Slow peer whose claim was released.
        peer: NodeId,
        /// Block hash.
        block_id: block::Hash,
        /// Stripe index.
        stripe: u32,
        /// Stripe-relative slot released.
        slot: u32,
    },
}

/// One peer's grant while [`BlockTable`] plans a stripe.
struct PlannedPull {
    requested: WantBitmap,
    /// Arrival estimate of the last part granted to this peer.
    arrival: Arrival,
}

/// Result of one [`BlockTable::step`] transition.
#[derive(Clone, Debug, Default)]
pub struct BlockStep {
    /// Effects for IO to execute.
    pub outputs: Vec<BlockOutput>,
    /// Typed events for traces, metrics, and tests.
    pub events: Vec<BlockEvent>,
}

#[derive(Clone, Debug)]
struct AdmittingEntry {
    work_id: WorkId,
    envelope: Box<MetaEnvelope>,
    /// When `HeaderMeta` first arrived.
    arrived_at: Instant,
    from_peer: NodeId,
    session_gen: u64,
    /// Other sessions that sent the same `HeaderMeta` while this entry waited.
    /// Each session holds a received marker until the table rejects or admits
    /// the block.
    also_from: Vec<(NodeId, u64)>,
    height: Option<u32>,
    /// The header's parent, once headerchain admits the header.
    parent_hash: Option<block::Hash>,
    stage: AdmittingStage,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum AdmittingStage {
    WaitingHeader,
    WaitingCommitment,
}

#[derive(Clone, Debug)]
struct PendingMetaEntry {
    envelope: Box<MetaEnvelope>,
    /// When `HeaderMeta` first arrived.
    arrived_at: Instant,
    from_peer: NodeId,
    session_gen: u64,
    /// See [`AdmittingEntry::also_from`].
    also_from: Vec<(NodeId, u64)>,
    parent_hash: block::Hash,
    received_at: Instant,
}

/// Picks a block's `Have` initial group the way main's block gossip picks
/// its advertisement group (`peer/block_gossip.rs` `select_peers`,
/// decision-26). Peers rank by the send-direction arrival of one part,
/// earliest first, with ties in random order. Every
/// [`ADVERT_RANDOM_INTERVAL`]th slot takes a random remaining peer, so the
/// group never depends only on the estimate.
fn have_group(
    paths: &PeerPaths,
    peers: &[NodeId],
    part_bytes: u64,
    window: usize,
    rng_seed: u64,
) -> Vec<NodeId> {
    use rand_10::{rngs::StdRng, seq::SliceRandom, RngExt, SeedableRng};
    let mut rng = StdRng::seed_from_u64(rng_seed);
    let mut ranked: Vec<(Duration, NodeId)> = peers
        .iter()
        .map(|&peer| (paths.seed_time(&peer, part_bytes), peer))
        .collect();
    ranked.shuffle(&mut rng);
    ranked.sort_by_key(|(arrival, _)| *arrival);
    let mut group = Vec::with_capacity(window.min(ranked.len()));
    for position in 1..=window.min(ranked.len()) {
        let index = if position % ADVERT_RANDOM_INTERVAL == 0 {
            rng.random_range(0..ranked.len())
        } else {
            0
        };
        group.push(ranked.remove(index).1);
    }
    group
}

#[derive(Clone, Debug)]
struct StripeState {
    /// Held verified parts indexed by stripe-relative slot (`0..n`).
    held: Vec<Option<VerifiedPart>>,
    /// Source slots at or above this one hold only padding (decision-18).
    /// They count as held from admission but have no part in `held`.
    padding_from: u32,
    /// Number of held parts (`0..=n`), padding included.
    held_count: u32,
    /// Number of held source parts (`0..=k`).
    source_held_count: u32,
    /// Active pull claim owner per stripe-relative slot.
    claims: HashMap<u32, NodeId>,
    /// Slots a peer sends unasked and has not delivered yet, with the sender:
    /// the proposer's flagged seeds (decision-19) or a holder's flagged
    /// pushes (decision-24). The first sender keeps the slot. They count as
    /// pending, apart from `claims`, so a neighbour seeded beyond `k` keeps
    /// `held + claims <= k` for pulls.
    incoming: HashMap<u32, Incoming>,
    /// Slots the proposer flagged for relay: this node's pushed seeds
    /// (decision-24).
    pushed: HashSet<u32>,
    /// Slots whose claim has already been released once via `WantLate` (`bpv1-64`).
    /// Never cleared, so the late rule holds once per block across repair rounds.
    late_released: HashSet<u32>,
    /// Announcers known to hold each stripe-relative slot.
    announcers: Vec<HashSet<NodeId>>,
    /// Peers that were late on or failed each slot. The planner never grants a
    /// slot to a peer in its `tried` set. Repair rounds clear it.
    tried: Vec<HashSet<NodeId>>,
    /// Active decode or check worker job, if any.
    job: Option<(WorkId, StripeJob)>,
    /// True once all `k` source parts are held (directly or after decode).
    assemblable: bool,
}

impl StripeState {
    fn decoding(&self) -> bool {
        matches!(self.job, Some((_, StripeJob::Decode)))
    }

    /// A stripe with no part held but its padding (decision-18).
    fn new(coding: &Coding, stripe: u32) -> Self {
        let len = coding.n as usize;
        let padding_from = (0..coding.k)
            .find(|&slot| {
                coding
                    .wire_index(stripe, slot)
                    .is_some_and(|index| coding.is_padding(index))
            })
            .unwrap_or(coding.k);
        let padding = coding.k - padding_from;
        Self {
            held: vec![None; len],
            padding_from,
            held_count: padding,
            source_held_count: padding,
            claims: HashMap::new(),
            incoming: HashMap::new(),
            pushed: HashSet::new(),
            late_released: HashSet::new(),
            announcers: (0..len).map(|_| HashSet::new()).collect(),
            tried: (0..len).map(|_| HashSet::new()).collect(),
            job: None,
            assemblable: false,
        }
    }

    /// Whether `slot` holds only padding (decision-18).
    fn is_padding(&self, slot: u32, k: u32) -> bool {
        (self.padding_from..k).contains(&slot)
    }

    /// Whether `slot` is held: a stored part, or padding.
    fn has(&self, slot: u32, k: u32) -> bool {
        self.is_padding(slot, k) || self.held[slot as usize].is_some()
    }

    /// Stores `part` at `slot` unless the slot is already held. Returns
    /// whether the part was stored.
    fn store(&mut self, slot: u32, k: u32, part: VerifiedPart) -> bool {
        if self.has(slot, k) {
            return false;
        }
        self.held[slot as usize] = Some(part);
        self.incoming.remove(&slot);
        self.held_count += 1;
        if slot < k {
            self.source_held_count += 1;
        }
        true
    }

    /// Unheld slots that a pull claim or a pending seed or push covers.
    fn pending_count(&self) -> u32 {
        let incoming_only = self
            .incoming
            .keys()
            .filter(|slot| !self.claims.contains_key(slot))
            .count();
        // A stripe has at most `MAX_N` slots, so the count fits u32.
        (self.claims.len() + incoming_only) as u32
    }

    fn clear_buffers(&mut self) {
        self.held.clear();
        self.claims.clear();
        self.incoming.clear();
        self.announcers.clear();
        self.tried.clear();
        self.job = None;
    }
}

/// A slot a peer sends unasked (decision-19, decision-24).
#[derive(Copy, Clone, Debug)]
struct Incoming {
    sender: NodeId,
    /// When the slot turns late: `SLOW_X` × the estimated arrival of the
    /// batch that listed or flagged it (bpv1-64). A later batch from the
    /// same sender never moves it.
    late_at: Instant,
}

#[derive(Clone, Debug)]
struct ActiveBlock {
    /// The admitted `HeaderMeta`, stored once. Retirement drops its byte fields.
    meta: Box<MetaEnvelope>,
    height: u32,
    as_proposer: bool,
    /// `encoding_id(meta.coding)`, cached for part proofs.
    enc_id: [u8; 32],
    stripes: Vec<StripeState>,
    phase: BlockPhase,
    /// Recovery ladder (`bpv1-57`): 0 while collecting normally, then repair
    /// round `1..` until the reconstruction deadline. It only grows, and
    /// resets to 0 when the block falls back.
    repair_round: u8,
    /// Monotonic reconstruction deadline; NEVER moved after admission (`bpv1-57`).
    reconstruction_deadline: Instant,
    /// Last part arrival (or repair-round start) the stall interval counts from.
    last_part_at: Instant,
    /// Peers that sent `BlockDone`, which repair may ask for any part (decision-20).
    done_peers: HashSet<NodeId>,
    /// Work id of the submission in flight. `None` while `Verifying` means a
    /// retryable verdict parked the assembled block until its parent commits
    /// (`bpv1-84`).
    verify_work_id: Option<WorkId>,
    /// Monotonic verification deadline, set at the first submission
    /// (`bpv1-82`).
    verify_deadline: Option<Instant>,
    /// The header's parent. `None` for a local solved block.
    parent_hash: Option<block::Hash>,
    /// Whether this block currently holds a body claim in `BlockFallback`.
    body_claimed: bool,
    /// True once `BlockDone` has been announced to routines.
    block_done_notified: bool,
    /// Peer part bytes charged before proof and not yet stored or released,
    /// keyed by `(index, peer)`.
    part_charges: HashMap<(u32, NodeId), u64>,
    /// Charged bytes of stored peer parts, held until retirement.
    stored_charge: u64,
    /// When `HeaderMeta` arrived, or when the proposer admitted the block.
    meta_arrived_at: Instant,
    /// When the first peer part was charged, before proof.
    first_part_at: Option<Instant>,
    /// True once the first peer part was stored.
    first_part_stored: bool,
    /// Part bytes this node uploaded to peers.
    uploaded_bytes: u64,
    /// Peers that hear of each verified part at once (decision-26).
    have_group: Vec<NodeId>,
    /// When every other peer may hear of parts (decision-26).
    have_deadline: Option<Instant>,
    /// Parts received again after they were held.
    duplicates: u32,
}

impl ActiveBlock {
    /// A freshly admitted block in `Collecting` with empty stripes.
    fn from_envelope(
        env: Box<MetaEnvelope>,
        height: u32,
        now: Instant,
        meta_arrived_at: Instant,
        as_proposer: bool,
    ) -> Self {
        let coding = env.coding;
        Self {
            meta: env,
            height,
            as_proposer,
            enc_id: encoding_id(&coding),
            stripes: (0..coding.stripes)
                .map(|stripe| StripeState::new(&coding, stripe))
                .collect(),
            phase: BlockPhase::Collecting,
            repair_round: 0,
            reconstruction_deadline: now + RECONSTRUCTION_DEADLINE,
            last_part_at: now,
            done_peers: HashSet::new(),
            verify_work_id: None,
            verify_deadline: None,
            parent_hash: None,
            body_claimed: false,
            block_done_notified: false,
            part_charges: HashMap::new(),
            stored_charge: 0,
            meta_arrived_at,
            first_part_at: None,
            first_part_stored: false,
            uploaded_bytes: 0,
            have_group: Vec::new(),
            have_deadline: None,
            duplicates: 0,
        }
    }

    /// Peer part bytes this block holds against the part buffer budget.
    fn charged_bytes(&self) -> u64 {
        self.part_charges.values().sum::<u64>() + self.stored_charge
    }

    /// The descriptor that `PublishAdmitted` and session rejoin send to peers.
    fn admitted(&self) -> AdmittedBlock {
        AdmittedBlock {
            block_id: self.meta.block_id,
            coding: self.meta.coding,
            proposer_node: self.meta.proposer_node,
            header_meta: self.meta.header_meta(),
            as_proposer: self.as_proposer,
            have_group: self.have_group.clone(),
            have_deadline: self.have_deadline,
        }
    }

    /// Announces `BlockDone` once, after canonical assembly or commit.
    fn notify_done(&mut self, now: Instant, step: &mut BlockStep) {
        if !self.block_done_notified {
            self.block_done_notified = true;
            step.outputs.push(BlockOutput::NotifyBlockDone {
                block_id: self.meta.block_id,
            });
            step.events.push(BlockEvent::Assembled {
                block_id: self.meta.block_id,
                since_meta: now.saturating_duration_since(self.meta_arrived_at),
            });
        }
    }

    fn all_stripes_assemblable(&self) -> bool {
        self.stripes.iter().all(|s| s.assemblable)
    }

    /// `base × 2^r` after repair round `r`; `base` before the first round.
    fn stall_interval(&self, base: Duration) -> Duration {
        let shift = u32::from(self.repair_round);
        let mult = 1u32.checked_shl(shift.min(16)).unwrap_or(u32::MAX);
        base.saturating_mul(mult)
    }

    fn retained_part_count(&self) -> usize {
        self.stripes
            .iter()
            .map(|s| s.held.iter().filter(|p| p.is_some()).count())
            .sum()
    }
}

/// Tells the peer that delivered `block_id`'s `HeaderMeta` that it was
/// rejected or expired without fault.
fn reject_meta(step: &mut BlockStep, block_id: block::Hash, peer: NodeId, session_gen: u64) {
    step.outputs.push(BlockOutput::MetaRejected {
        block_id,
        peer,
        session_gen,
    });
}

/// Rejects the metadata for every session in `also_from`.
fn reject_also(step: &mut BlockStep, block_id: block::Hash, also_from: &[(NodeId, u64)]) {
    for &(peer, session_gen) in also_from {
        reject_meta(step, block_id, peer, session_gen);
    }
}

/// Returns the next work identity from `counter`, skipping zero.
fn alloc_work_id(counter: &mut u64) -> WorkId {
    let id = WorkId(*counter);
    *counter = counter.wrapping_add(1).max(1);
    id
}

/// Pure node-wide BPv1 block coordinator.
#[derive(Clone, Debug)]
pub struct BlockTable {
    config: BlockConfig,
    next_work_id: u64,
    top: u32,
    /// Height of the verified tip. Metadata more than `PROPAGATION_DEPTH` above it belongs to
    /// block sync (`bpv1-76`).
    verified: u32,
    /// Hash of the verified tip. A block whose parent it is commits next
    /// (`bpv1-83`, `bpv1-84`).
    verified_hash: Option<block::Hash>,
    paths: PeerPaths,
    /// Metadata currently in `HeaderAdmission` or `VerifyCommitment`.
    admitting: HashMap<block::Hash, AdmittingEntry>,
    /// Index from `WorkId` to `block_id` for O(1) stale-work filtering.
    work_to_block: HashMap<WorkId, block::Hash>,
    /// Pending metadata waiting on missing parents (`bpv1-11`).
    pending: HashMap<block::Hash, PendingMetaEntry>,
    /// Current byte charge of `pending`.
    pending_bytes: usize,
    /// Admitted (and retired-in-window) blocks.
    blocks: HashMap<block::Hash, ActiveBlock>,
}

impl BlockTable {
    /// Creates a fresh empty block table.
    pub fn new(config: BlockConfig) -> Self {
        Self {
            config,
            next_work_id: 1,
            top: 0,
            verified: 0,
            verified_hash: None,
            paths: PeerPaths::new(),
            admitting: HashMap::new(),
            work_to_block: HashMap::new(),
            pending: HashMap::new(),
            pending_bytes: 0,
            blocks: HashMap::new(),
        }
    }

    /// Current known chain tip height.
    pub fn top(&self) -> u32 {
        self.top
    }

    /// Shared read-only view of [`PeerPaths`].
    pub fn paths(&self) -> &PeerPaths {
        &self.paths
    }

    /// Number of pending metadata entries waiting on missing parents.
    pub fn pending_meta_len(&self) -> usize {
        self.pending.len()
    }

    /// Number of metadata entries waiting on header or commitment admission.
    pub fn admitting_meta_len(&self) -> usize {
        self.admitting.len()
    }

    /// Current lifecycle phase of `block_id`, if admitted or retired.
    pub fn block_phase(&self, block_id: &block::Hash) -> Option<BlockPhase> {
        self.blocks.get(block_id).map(|b| b.phase)
    }

    /// Returns the held [`VerifiedPart`] for `(block_id, index)`, if present.
    pub fn get_part(&self, block_id: &block::Hash, index: u32) -> Option<&VerifiedPart> {
        let block = self.blocks.get(block_id)?;
        if block.phase == BlockPhase::Retired {
            return None;
        }
        let (stripe, slot) = block.meta.coding.split_index(index)?;
        block
            .stripes
            .get(stripe as usize)?
            .held
            .get(slot as usize)?
            .as_ref()
    }

    /// Returns the immutable admitted `(Coding, part_root)` descriptor for an active
    /// (non-retired) block.
    ///
    /// Peer verification workers obtain their verification root strictly from this
    /// descriptor so an unadmitted or competing `HeaderMeta` can never substitute a root.
    pub fn admitted_descriptor(&self, block_id: &block::Hash) -> Option<(Coding, [u8; 32])> {
        let block = self.blocks.get(block_id)?;
        if block.phase == BlockPhase::Retired {
            return None;
        }
        Some((block.meta.coding, block.meta.part_root))
    }

    /// Snapshot of currently active (non-retired) admitted blocks so a newly joined
    /// or reconnected peer session learns active descriptors.
    pub fn active_admitted_blocks(&self) -> Vec<(AdmittedBlock, Vec<u32>)> {
        self.blocks
            .values()
            .filter(|b| b.phase != BlockPhase::Retired)
            .map(|b| {
                let mut ready = Vec::new();
                for (s, st) in b.stripes.iter().enumerate() {
                    for (slot, part) in st.held.iter().enumerate() {
                        if part.is_some() {
                            if let Some(idx) = b.meta.coding.wire_index(s as u32, slot as u32) {
                                ready.push(idx);
                            }
                        }
                    }
                }
                (b.admitted(), ready)
            })
            .collect()
    }

    /// Charges `bytes` of a peer part frame, plus the zeros that pad its
    /// payload to `S`, against the part buffer budget before
    /// proof and returns the block's admitted
    /// `(coding, part_root)` to verify the part against.
    ///
    /// The IO shell calls this when it accepts a part prefix, because it needs
    /// the answer before it starts proof work. A later `InsertPart` stores the
    /// part under the charge or releases it; `ReleasePartCharge` releases it
    /// after a failed proof; retirement releases every charge of the block. A
    /// repeated charge for the same `(index, peer)` replaces the earlier one.
    pub fn charge_part(
        &mut self,
        now: Instant,
        block_id: &block::Hash,
        index: u32,
        from_peer: NodeId,
        bytes: u64,
    ) -> Result<(Coding, [u8; 32]), PartRefusal> {
        let charged = self.part_buffer_bytes();
        let budget = self.config.part_buffer_bytes;
        let block = self
            .blocks
            .get_mut(block_id)
            .filter(|b| b.phase != BlockPhase::Retired)
            .ok_or(PartRefusal::NotAdmitted)?;
        // 03 §4: the charge covers the part padded to `S`, not only its wire bytes.
        let padding = block
            .meta
            .coding
            .wire_len(index)
            .map_or(0, |len| block.meta.coding.part_bytes.saturating_sub(len));
        let bytes = bytes.saturating_add(u64::from(padding));
        let replaced = block
            .part_charges
            .get(&(index, from_peer))
            .copied()
            .unwrap_or(0);
        if charged - replaced + bytes > budget {
            return Err(PartRefusal::LocalCapacity);
        }
        block.part_charges.insert((index, from_peer), bytes);
        block.first_part_at.get_or_insert(now);
        Ok((block.meta.coding, block.meta.part_root))
    }

    /// Peer part bytes charged against the part buffer budget, in flight or stored.
    pub fn part_buffer_bytes(&self) -> u64 {
        self.blocks.values().map(ActiveBlock::charged_bytes).sum()
    }

    /// Returns the number of retained parts across all non-retired blocks.
    pub fn retained_part_count(&self) -> usize {
        self.blocks
            .values()
            .map(ActiveBlock::retained_part_count)
            .sum()
    }

    /// Returns the earliest timer deadline across pending metadata and active blocks.
    pub fn next_deadline(&self) -> Option<Instant> {
        let pending = self
            .pending
            .values()
            .map(|entry| entry.received_at + PENDING_META_AGE);
        let verifying = self
            .blocks
            .values()
            .filter(|b| b.phase == BlockPhase::Verifying)
            .filter_map(|b| b.verify_deadline);
        let blocks = self
            .blocks
            .values()
            .filter(|b| b.phase == BlockPhase::Collecting)
            .flat_map(|b| {
                [
                    Some(b.reconstruction_deadline),
                    Some(
                        b.last_part_at + b.stall_interval(stall_base(&self.paths, &b.meta.coding)),
                    ),
                    b.stripes
                        .iter()
                        .flat_map(|st| st.incoming.values().map(|p| p.late_at))
                        .min(),
                ]
                .into_iter()
                .flatten()
            });
        pending.chain(verifying).chain(blocks).min()
    }

    /// Executes one synchronous state transition.
    pub fn step(&mut self, now: Instant, input: BlockInput) -> BlockStep {
        let mut step = BlockStep::default();
        match input {
            BlockInput::TipHeight { height, hash } => {
                self.verified = height;
                self.verified_hash = Some(hash);
                self.raise_top(height, &mut step);
                self.resubmit_parked(now, &mut step);
            }
            BlockInput::ParentAdmitted { hash, height } => {
                self.raise_top(height, &mut step);
                self.wake_pending_for_parent(hash, &mut step);
            }
            BlockInput::MetaReceived {
                envelope,
                from_peer,
                session_gen,
            } => {
                self.handle_meta_received(
                    now,
                    envelope,
                    from_peer,
                    session_gen,
                    Vec::new(),
                    &mut step,
                );
            }
            BlockInput::HeaderAdmitted { work_id, result } => {
                self.handle_header_admitted(now, work_id, result, &mut step);
            }
            BlockInput::CommitmentChecked {
                work_id,
                ok,
                peers,
                rng_seed,
            } => {
                self.handle_commitment_checked(now, work_id, ok, &peers, rng_seed, &mut step);
            }
            BlockInput::BlockAdmittedLocal {
                height,
                envelope,
                parts,
            } => {
                self.handle_block_admitted_local(now, height, envelope, parts, &mut step);
            }
            BlockInput::PushFlagged {
                peer,
                block_id,
                indices,
                relay,
            } => self.handle_push_flagged(now, peer, block_id, &indices, relay, &mut step),
            BlockInput::PathObserved { peer, estimate } => self.paths.observe(peer, estimate),
            BlockInput::RateSample { peer, sample } => {
                self.paths.record_sample(peer, sample);
            }
            BlockInput::SessionEnded { peer, peer_states } => {
                self.handle_session_ended(now, peer, &peer_states, &mut step);
            }
            BlockInput::PeerBlockDone {
                peer,
                block_id,
                peer_states,
            } => {
                self.handle_peer_block_done(now, peer, block_id, &peer_states, &mut step);
            }
            BlockInput::PeerAnnounced {
                peer,
                block_id,
                indices,
                peer_states,
            } => {
                self.handle_peer_announced(now, peer, block_id, &indices, &peer_states, &mut step);
            }
            BlockInput::PlanPulls {
                block_id,
                stripe,
                peer_states,
            } => {
                self.plan_stripe(now, block_id, stripe, &peer_states, &mut step);
            }
            BlockInput::ReleaseClaims {
                peer,
                block_id,
                stripe,
                slots,
                reason,
                peer_states,
            } => {
                self.handle_release_claims(
                    now,
                    peer,
                    block_id,
                    stripe,
                    &slots,
                    reason,
                    &peer_states,
                    &mut step,
                );
            }
            BlockInput::InsertPart {
                block_id,
                part,
                from_peer,
                auth,
                peer_states,
            } => {
                self.handle_insert_part(
                    now,
                    block_id,
                    part,
                    from_peer,
                    auth,
                    &peer_states,
                    &mut step,
                );
            }
            BlockInput::ReleasePartCharge {
                block_id,
                index,
                from_peer,
            } => {
                if let Some(block) = self.blocks.get_mut(&block_id) {
                    block.part_charges.remove(&(index, from_peer));
                }
            }
            BlockInput::PartUploaded { block_id, bytes } => {
                if let Some(block) = self
                    .blocks
                    .get_mut(&block_id)
                    .filter(|b| b.phase != BlockPhase::Retired)
                {
                    block.uploaded_bytes = block.uploaded_bytes.saturating_add(bytes);
                }
            }
            BlockInput::StripeJobDone {
                work_id,
                block_id,
                stripe,
                result,
            } => {
                self.handle_stripe_checked(now, work_id, block_id, stripe, result, &mut step);
            }
            BlockInput::BlockAssembled { work_id, block_id } => {
                self.handle_block_assembled(now, work_id, block_id, &mut step);
            }
            BlockInput::VerifierVerdict {
                work_id,
                block_id,
                verdict,
            } => {
                self.handle_verifier_verdict(now, work_id, block_id, verdict, &mut step);
            }
            BlockInput::FallbackDone { block_id } => {
                self.handle_fallback_done(block_id, &mut step);
            }
            BlockInput::ProposedCommitFailed { block_id } => {
                self.handle_proposed_commit_failed(block_id, &mut step);
            }
            BlockInput::Tick { peer_states } => {
                self.handle_tick(now, &peer_states, &mut step);
            }
        }
        step
    }

    /// Raises `top` to `height` and retires the blocks that leave the window.
    fn raise_top(&mut self, height: u32, step: &mut BlockStep) {
        if height > self.top {
            self.top = height;
            self.retire_out_of_window(step);
        }
    }

    fn in_propagation_window(&self, height: u32) -> bool {
        // bpv1-7: strictly above `top - PROPAGATION_DEPTH` (excludes equality).
        height.saturating_add(PROPAGATION_DEPTH) > self.top
    }

    /// Whether BPv1 may admit a block at `height`: inside the propagation window
    /// (`bpv1-7`) and at most `PROPAGATION_DEPTH` above the verified tip (`bpv1-76`).
    ///
    /// While a node catches up, header sync runs near the network tip and raises
    /// `top` far above the verified tip. A block that high cannot commit until block
    /// sync delivers its ancestors, so block sync owns it, as legacy gossip's
    /// verified-tip lookahead does.
    fn admissible_height(&self, height: u32) -> bool {
        self.in_propagation_window(height)
            && height <= self.verified.saturating_add(PROPAGATION_DEPTH)
    }

    fn handle_meta_received(
        &mut self,
        arrived_at: Instant,
        envelope: Box<MetaEnvelope>,
        from_peer: NodeId,
        session_gen: u64,
        mut also_from: Vec<(NodeId, u64)>,
        step: &mut BlockStep,
    ) {
        let block_id = envelope.block_id;
        // An admitted or retired block's admission already cleared every
        // session's received marker. A block still admitting or pending records
        // this sender, so whatever ends the entry also answers this session.
        if self.blocks.contains_key(&block_id) {
            return;
        }
        let waiting = match self.admitting.get_mut(&block_id) {
            Some(entry) => Some(&mut entry.also_from),
            None => self
                .pending
                .get_mut(&block_id)
                .map(|entry| &mut entry.also_from),
        };
        if let Some(waiting) = waiting {
            waiting.push((from_peer, session_gen));
            waiting.append(&mut also_from);
            return;
        }
        // bpv1-10: validate canonical coding tuple against the local
        // profile BEFORE scheduling any headerchain admission work!
        if self
            .config
            .profile
            .validate_coding(
                &envelope.coding,
                envelope.header_bytes.len(),
                envelope.binding.coinbase.len(),
            )
            .is_err()
        {
            step.outputs.push(BlockOutput::ClosePeer {
                peer: from_peer,
                session_gen,
                row: Bpv1CloseRow::Row4BadMetaOrCommitment,
            });
            reject_also(step, block_id, &also_from);
            return;
        }
        // Bound admitting count and bytes before queueing headerchain
        // admission. Pending metadata has its own budget, so headers with
        // unknown parents never take space from viable metadata (bpv1-75).
        if self.admitting.len() >= MAX_ADMITTING_META_COUNT
            || self
                .admitting_bytes()
                .saturating_add(envelope.estimated_bytes())
                > MAX_ADMITTING_META_BYTES
        {
            reject_meta(step, block_id, from_peer, session_gen);
            reject_also(step, block_id, &also_from);
            return;
        }
        let work_id = alloc_work_id(&mut self.next_work_id);
        let header_bytes = envelope.header_bytes.clone();
        self.admitting.insert(
            block_id,
            AdmittingEntry {
                work_id,
                envelope,
                arrived_at,
                from_peer,
                session_gen,
                also_from,
                height: None,
                parent_hash: None,
                stage: AdmittingStage::WaitingHeader,
            },
        );
        self.work_to_block.insert(work_id, block_id);
        step.outputs.push(BlockOutput::AdmitHeader {
            work_id,
            block_id,
            header_bytes,
            from_peer,
            session_gen,
        });
    }

    fn handle_header_admitted(
        &mut self,
        now: Instant,
        work_id: WorkId,
        result: HeaderAdmissionResult,
        step: &mut BlockStep,
    ) {
        let Some(block_id) = self.work_to_block.remove(&work_id) else {
            return;
        };
        let Some(entry) = self.admitting.get(&block_id) else {
            return;
        };
        if entry.work_id != work_id || entry.stage != AdmittingStage::WaitingHeader {
            return;
        }

        match result {
            HeaderAdmissionResult::Admitted {
                height,
                parent_hash,
            } => {
                // An admitted header need not be selected: headerchain also
                // retains forks and future-dated headers. Only the verified tip,
                // the best header-valid height, and local solved blocks raise `top`.
                if !self.admissible_height(height) {
                    if let Some(removed) = self.admitting.remove(&block_id) {
                        reject_meta(step, block_id, removed.from_peer, removed.session_gen);
                        reject_also(step, block_id, &removed.also_from);
                    }
                    return;
                }
                let next_work = alloc_work_id(&mut self.next_work_id);
                let entry = self
                    .admitting
                    .get_mut(&block_id)
                    .expect("admitting entry verified present");
                entry.height = Some(height);
                entry.parent_hash = Some(parent_hash);
                entry.work_id = next_work;
                entry.stage = AdmittingStage::WaitingCommitment;
                let envelope = entry.envelope.clone();
                self.work_to_block.insert(next_work, block_id);

                // bpv1-1: commitment verification starts ONLY after HeaderAdmitted.
                step.outputs.push(BlockOutput::VerifyCommitment {
                    work_id: next_work,
                    block_id,
                    height,
                    envelope,
                });
            }
            HeaderAdmissionResult::MissingParent { parent_hash } => {
                let entry = self
                    .admitting
                    .remove(&block_id)
                    .expect("admitting entry verified present");
                self.insert_pending(
                    block_id,
                    PendingMetaEntry {
                        envelope: entry.envelope,
                        arrived_at: entry.arrived_at,
                        from_peer: entry.from_peer,
                        session_gen: entry.session_gen,
                        also_from: entry.also_from,
                        parent_hash,
                        received_at: now,
                    },
                    step,
                );
            }
            HeaderAdmissionResult::PeerInvalid => {
                let entry = self
                    .admitting
                    .remove(&block_id)
                    .expect("admitting entry verified present");
                // `block_id` is the hash of the header bytes, so every session
                // in `also_from` sent this same invalid header.
                for (peer, session_gen) in
                    std::iter::once((entry.from_peer, entry.session_gen)).chain(entry.also_from)
                {
                    step.outputs.push(BlockOutput::ClosePeer {
                        peer,
                        session_gen,
                        row: Bpv1CloseRow::Row3InvalidHeader,
                    });
                }
            }
            HeaderAdmissionResult::LocalError => {
                if let Some(removed) = self.admitting.remove(&block_id) {
                    reject_meta(step, block_id, removed.from_peer, removed.session_gen);
                    reject_also(step, block_id, &removed.also_from);
                }
            }
        }
    }

    /// Bytes held in metadata in header admission or the commitment check.
    fn admitting_bytes(&self) -> usize {
        self.admitting
            .values()
            .map(|e| e.envelope.estimated_bytes())
            .sum()
    }

    /// Pending entries and bytes that `peer` holds.
    fn pending_held_by(&self, peer: NodeId) -> (usize, usize) {
        self.pending
            .values()
            .filter(|e| e.from_peer == peer)
            .fold((0, 0), |(count, bytes), e| {
                (count + 1, bytes + e.envelope.estimated_bytes())
            })
    }

    /// Parks metadata whose parent header is unknown (`bpv1-11`).
    ///
    /// A peer keeps at most [`MAX_PENDING_META_PER_PEER`] entries waiting. When
    /// the node-wide budget is full, the peer holding the most pending entries,
    /// then bytes, loses its oldest entry; if no other peer holds more than the
    /// sender would, the new entry is refused instead. So a peer flooding
    /// headers with unknown parents displaces only its own entries, and every
    /// dropped entry falls back to header sync and block sync.
    fn insert_pending(
        &mut self,
        block_id: block::Hash,
        entry: PendingMetaEntry,
        step: &mut BlockStep,
    ) {
        let bytes = entry.envelope.estimated_bytes();
        let (held, _) = self.pending_held_by(entry.from_peer);
        let mut admit = held < MAX_PENDING_META_PER_PEER;
        while admit
            && (self.pending.len() >= MAX_PENDING_META_COUNT
                || self.pending_bytes.saturating_add(bytes) > MAX_PENDING_META_BYTES)
        {
            let (own_count, own_bytes) = self.pending_held_by(entry.from_peer);
            let after_insert = (own_count + 1, own_bytes.saturating_add(bytes));
            let heaviest = self
                .pending
                .values()
                .map(|e| e.from_peer)
                .filter(|&peer| peer != entry.from_peer)
                .map(|peer| (self.pending_held_by(peer), peer))
                .max();
            match heaviest {
                Some((weight, peer)) if weight > after_insert => {
                    let oldest = self
                        .pending
                        .iter()
                        .filter(|(_, e)| e.from_peer == peer)
                        .min_by_key(|(id, e)| (e.received_at, id.0))
                        .map(|(id, _)| *id)
                        .expect("the heaviest peer holds at least one pending entry");
                    self.drop_pending(oldest, step);
                }
                _ => admit = false,
            }
        }
        if !admit {
            step.events
                .push(BlockEvent::MetaPendingDropped { block_id });
            reject_meta(step, block_id, entry.from_peer, entry.session_gen);
            reject_also(step, block_id, &entry.also_from);
            return;
        }
        self.pending_bytes = self.pending_bytes.saturating_add(bytes);
        self.pending.insert(block_id, entry);
    }

    /// Drops a pending entry for the pending budget and rejects it for each
    /// session that sent it.
    fn drop_pending(&mut self, block_id: block::Hash, step: &mut BlockStep) {
        if let Some(entry) = self.pending.remove(&block_id) {
            self.pending_bytes = self
                .pending_bytes
                .saturating_sub(entry.envelope.estimated_bytes());
            step.events
                .push(BlockEvent::MetaPendingDropped { block_id });
            reject_meta(step, block_id, entry.from_peer, entry.session_gen);
            reject_also(step, block_id, &entry.also_from);
        }
    }

    fn wake_pending_for_parent(&mut self, parent_hash: block::Hash, step: &mut BlockStep) {
        let ready: Vec<block::Hash> = self
            .pending
            .iter()
            .filter(|(_, e)| e.parent_hash == parent_hash)
            .map(|(id, _)| *id)
            .collect();
        for block_id in ready {
            if let Some(pending) = self.pending.remove(&block_id) {
                self.pending_bytes = self
                    .pending_bytes
                    .saturating_sub(pending.envelope.estimated_bytes());
                self.handle_meta_received(
                    pending.arrived_at,
                    pending.envelope,
                    pending.from_peer,
                    pending.session_gen,
                    pending.also_from,
                    step,
                );
            }
        }
    }

    fn handle_commitment_checked(
        &mut self,
        now: Instant,
        work_id: WorkId,
        ok: bool,
        peers: &[NodeId],
        rng_seed: u64,
        step: &mut BlockStep,
    ) {
        let Some(block_id) = self.work_to_block.remove(&work_id) else {
            return;
        };
        let Some(entry) = self.admitting.get(&block_id) else {
            return;
        };
        if entry.work_id != work_id || entry.stage != AdmittingStage::WaitingCommitment {
            return;
        }
        let entry = self
            .admitting
            .remove(&block_id)
            .expect("admitting entry verified present");

        if !ok {
            step.outputs.push(BlockOutput::ClosePeer {
                peer: entry.from_peer,
                session_gen: entry.session_gen,
                row: Bpv1CloseRow::Row4BadMetaOrCommitment,
            });
            // Another sender's envelope may differ under the same hash, so it is
            // not faulted for this one's commitment.
            reject_also(step, block_id, &entry.also_from);
            return;
        }
        // This node admitted the block itself while the check ran: its miner
        // solved the block that a peer's copy also carries. That admission
        // published the block to every session, so this copy ends here.
        // Admitting it again would replace the solved block's entry.
        if self.blocks.contains_key(&block_id) {
            return;
        }

        let height = entry.height.unwrap_or(self.top);
        // Recheck the window after async commitment work
        // in case `top` advanced while commitment verification was running.
        if !self.admissible_height(height) {
            reject_meta(step, block_id, entry.from_peer, entry.session_gen);
            reject_also(step, block_id, &entry.also_from);
            return;
        }
        // All concurrent slots actively collecting: drop cleanly without faulting the peer.
        let next = self.is_next_block(height, entry.parent_hash);
        if !self.evict_for_slot(false, step)
            && !(next && self.displace_for_next_block(height, step))
        {
            reject_meta(step, block_id, entry.from_peer, entry.session_gen);
            reject_also(step, block_id, &entry.also_from);
            return;
        }

        let deadline = now + RECONSTRUCTION_DEADLINE;
        let mut active =
            ActiveBlock::from_envelope(entry.envelope, height, now, entry.arrived_at, false);
        active.body_claimed = true;
        active.parent_hash = entry.parent_hash;
        // decision-26: pick the `Have` initial group once, at admission.
        let pacing = self.config.have_pacing;
        let window = pacing.window(active.meta.coding.body_bytes);
        let candidates: Vec<NodeId> = peers
            .iter()
            .copied()
            .filter(|peer| *peer != active.meta.proposer_node)
            .collect();
        active.have_group = have_group(
            &self.paths,
            &candidates,
            u64::from(active.meta.coding.part_bytes),
            window,
            rng_seed,
        );
        active.have_deadline = Some(now + pacing.delay);
        let admitted = active.admitted();
        self.blocks.insert(block_id, active);

        step.events
            .push(BlockEvent::CommitmentVerified { block_id });
        step.events.push(BlockEvent::BlockAdmitted { block_id });
        // 06 §6: claim the body before publishing `BlockAdmitted`. The shell
        // runs outputs in order, so no peer sees `BlockAdmitted` first. Native
        // block sync may have queued the height from its header already. Its
        // selection and writer-start checks skip the claimed body.
        step.outputs
            .push(BlockOutput::ClaimBody { block_id, deadline });
        step.outputs.push(BlockOutput::PublishAdmitted {
            admitted,
            from_peer: Some(entry.from_peer),
        });
        // An empty body has no stripes, so the block assembles from
        // `HeaderMeta` alone and no part crosses the wire for it.
        self.maybe_assemble_and_verify(now, block_id, step);
    }

    /// Frees a concurrent-block slot if every slot is taken.
    ///
    /// Retires the lowest `Done` block, else the lowest `Fallback` block so a
    /// stalled fallback never wedges admission. With `force`, retires the lowest
    /// non-retired block as a last resort. Returns whether a slot is free.
    fn evict_for_slot(&mut self, force: bool, step: &mut BlockStep) -> bool {
        let lowest = |phases: &[BlockPhase]| {
            self.blocks
                .values()
                .filter(|b| phases.contains(&b.phase))
                .min_by_key(|b| b.height)
                .map(|b| b.meta.block_id)
        };
        let active = self
            .blocks
            .values()
            .filter(|b| b.phase != BlockPhase::Retired)
            .count();
        if active < self.config.max_concurrent_blocks {
            return true;
        }
        let victim = lowest(&[BlockPhase::Done])
            .or_else(|| lowest(&[BlockPhase::Fallback]))
            .or_else(|| {
                force
                    .then(|| lowest(&[BlockPhase::Collecting, BlockPhase::Verifying]))
                    .flatten()
            });
        match victim {
            Some(id) => {
                self.retire_block(id, step);
                true
            }
            None => false,
        }
    }

    /// Whether a block at `height` with `parent_hash` extends the verified tip,
    /// so it is the next block the chain can commit (`bpv1-83`, `bpv1-84`).
    ///
    /// Headerchain also admits retained forks, so the height alone does not
    /// show that the parent is committed.
    fn is_next_block(&self, height: u32, parent_hash: Option<block::Hash>) -> bool {
        height == self.verified.saturating_add(1)
            && parent_hash.is_some()
            && parent_hash == self.verified_hash
    }

    /// Frees a slot for the next block, the one whose parent is the verified
    /// tip (`bpv1-83`).
    ///
    /// Every block above that height waits on it. When every slot is busy, it
    /// retires the highest block above it, and block sync fetches that one
    /// instead. Returns whether a slot is free.
    fn displace_for_next_block(&mut self, height: u32, step: &mut BlockStep) -> bool {
        let victim = self
            .blocks
            .values()
            .filter(|b| {
                matches!(b.phase, BlockPhase::Collecting | BlockPhase::Verifying)
                    && b.height > height
            })
            .max_by_key(|b| b.height)
            .map(|b| b.meta.block_id);
        match victim {
            Some(id) => {
                self.retire_block(id, step);
                true
            }
            None => false,
        }
    }

    /// Resubmits each parked block whose parent is now the verified tip
    /// (`bpv1-84`).
    ///
    /// A parked block whose height the verified tip reached on another branch
    /// is no longer the next block, so it falls back to block sync.
    fn resubmit_parked(&mut self, now: Instant, step: &mut BlockStep) {
        let mut ready = Vec::new();
        let mut passed = Vec::new();
        for b in self.blocks.values() {
            if b.phase != BlockPhase::Verifying
                || b.verify_work_id.is_some()
                || !b.verify_deadline.is_some_and(|deadline| now < deadline)
            {
                continue;
            }
            if self.is_next_block(b.height, b.parent_hash) {
                ready.push(b.meta.block_id);
            } else if b.height <= self.verified.saturating_add(1) {
                passed.push(b.meta.block_id);
            }
        }
        for block_id in ready {
            self.submit_assembled(block_id, step);
        }
        for block_id in passed {
            self.enter_fallback(block_id, FallbackReason::LocalCapacity, step);
        }
    }

    fn handle_block_admitted_local(
        &mut self,
        now: Instant,
        height: u32,
        envelope: Box<MetaEnvelope>,
        parts: Vec<VerifiedPart>,
        step: &mut BlockStep,
    ) {
        let block_id = envelope.block_id;
        // Make local solved-block admission idempotent by hash. A peer's copy
        // cannot come first: the coinbase commitment names this node as
        // proposer, and only this node's prepared template publishes it, after
        // this admission. A copy relayed from before a restart has no prepared
        // template behind it, so it never reaches here.
        if self.blocks.contains_key(&block_id) {
            return;
        }
        self.raise_top(height, step);
        // A solved block always takes a slot, evicting the lowest block if needed.
        self.evict_for_slot(true, step);
        let coding = envelope.coding;
        let mut active = ActiveBlock::from_envelope(envelope, height, now, now, true);
        active.phase = BlockPhase::Done;
        active.block_done_notified = true;
        for part in parts {
            if let Some((s, slot)) = coding.split_index(part.index()) {
                active.stripes[s as usize].store(slot, coding.k, part);
            }
        }
        for st in &mut active.stripes {
            st.assemblable = st.source_held_count == coding.k;
        }
        let admitted = active.admitted();
        self.blocks.insert(block_id, active);
        step.events.push(BlockEvent::BlockAdmitted { block_id });
        step.outputs.push(BlockOutput::PublishAdmitted {
            admitted,
            from_peer: None,
        });
        // decision-20: the proposer announces nothing. Its `BlockDone` tells
        // repair that it holds every part.
    }

    /// Records that `peer` holds every part of `block_id` (decision-20). A
    /// block already in repair re-plans its stripes at once.
    fn handle_peer_block_done(
        &mut self,
        now: Instant,
        peer: NodeId,
        block_id: block::Hash,
        peer_states: &[PullPeerState],
        step: &mut BlockStep,
    ) {
        let Some(block) = self
            .blocks
            .get_mut(&block_id)
            .filter(|b| b.phase == BlockPhase::Collecting)
        else {
            return;
        };
        if !block.done_peers.insert(peer) || block.repair_round == 0 {
            return;
        }
        for stripe in 0..block.meta.coding.stripes {
            self.plan_stripe(now, block_id, stripe, peer_states, step);
        }
    }

    /// Records `peer`'s flagged pushes as pending claims held by `peer` and
    /// arms their late timer (decision-24, bpv1-64). The planner never asks
    /// `peer` for a part it is pushing. Relay-flagged indices, which only the
    /// proposer sends, become this node's pushed seeds: it pushes each onward
    /// once it holds it, and one already held goes out now (bpv1-71).
    fn handle_push_flagged(
        &mut self,
        now: Instant,
        peer: NodeId,
        block_id: block::Hash,
        indices: &[u32],
        relay: bool,
        step: &mut BlockStep,
    ) {
        let Some(block) = self
            .blocks
            .get_mut(&block_id)
            .filter(|b| b.phase == BlockPhase::Collecting)
        else {
            return;
        };
        let mut held: Vec<u32> = Vec::new();
        for &index in indices {
            if let Some((stripe, slot)) = block.meta.coding.split_index(index) {
                let st = &mut block.stripes[stripe as usize];
                st.tried[slot as usize].insert(peer);
                if relay && peer == block.meta.proposer_node {
                    st.pushed.insert(slot);
                    if st.held[slot as usize].is_some() {
                        held.push(index);
                    }
                }
            }
        }
        if !held.is_empty() {
            step.outputs.push(BlockOutput::PartReady {
                block_id,
                indices: held,
                push: true,
            });
        }
        self.expect_incoming(now, peer, block_id, indices);
    }

    /// Marks each of `indices` that the block lacks and that no claim or
    /// earlier sender covers as pending from `peer`. They turn late at
    /// `SLOW_X` × the estimated arrival of this batch. A later batch from
    /// `peer` keeps its own late time, so it never delays this one (bpv1-64).
    fn expect_incoming(
        &mut self,
        now: Instant,
        peer: NodeId,
        block_id: block::Hash,
        indices: &[u32],
    ) {
        let Some(block) = self.blocks.get_mut(&block_id) else {
            return;
        };
        let coding = block.meta.coding;
        let mut bytes = 0u64;
        let mut batch: Vec<(u32, u32)> = Vec::new();
        for &index in indices {
            let (Some((stripe, slot)), Some(wire_len)) =
                (coding.split_index(index), coding.wire_len(index))
            else {
                continue;
            };
            let st = &block.stripes[stripe as usize];
            if st.assemblable
                || st.decoding()
                || st.has(slot, coding.k)
                || st.claims.contains_key(&slot)
                || st.incoming.contains_key(&slot)
            {
                continue;
            }
            batch.push((stripe, slot));
            bytes = bytes.saturating_add(u64::from(wire_len));
        }
        if batch.is_empty() {
            return;
        }
        let arrival = self.paths.pull_arrival(&peer, 0, bytes, now);
        let estimate = arrival
            .at
            .saturating_duration_since(now)
            .max(Duration::from_millis(1));
        let late_at = now + estimate.saturating_mul(SLOW_X);
        for (stripe, slot) in batch {
            block.stripes[stripe as usize].incoming.insert(
                slot,
                Incoming {
                    sender: peer,
                    late_at,
                },
            );
        }
    }

    fn handle_session_ended(
        &mut self,
        now: Instant,
        peer: NodeId,
        peer_states: &[PullPeerState],
        step: &mut BlockStep,
    ) {
        // bpv1-65: drop path entry with connection.
        self.paths.remove(&peer);

        let mut replan: Vec<(block::Hash, u32)> = Vec::new();
        for (&block_id, block) in &mut self.blocks {
            if block.phase != BlockPhase::Collecting {
                continue;
            }
            block.done_peers.remove(&peer);
            // A sender's pending seeds and pushes end with its connection.
            for (s_idx, stripe) in block.stripes.iter_mut().enumerate() {
                for set in &mut stripe.announcers {
                    set.remove(&peer);
                }
                let before = stripe.pending_count();
                stripe.claims.retain(|_, owner| *owner != peer);
                stripe.incoming.retain(|_, pending| pending.sender != peer);
                if stripe.pending_count() < before {
                    replan.push((block_id, s_idx as u32));
                }
            }
        }
        for (block_id, stripe) in replan {
            self.plan_stripe(now, block_id, stripe, peer_states, step);
        }
    }

    fn handle_peer_announced(
        &mut self,
        now: Instant,
        peer: NodeId,
        block_id: block::Hash,
        indices: &[u32],
        peer_states: &[PullPeerState],
        step: &mut BlockStep,
    ) {
        let mut touched_stripes: Vec<u32> = Vec::new();
        {
            let Some(block) = self.blocks.get_mut(&block_id) else {
                return;
            };
            if block.phase != BlockPhase::Collecting {
                return;
            }
            for &idx in indices {
                let Some((s, slot)) = block.meta.coding.split_index(idx) else {
                    continue;
                };
                if let Some(st) = block.stripes.get_mut(s as usize) {
                    if let Some(ann) = st.announcers.get_mut(slot as usize) {
                        ann.insert(peer);
                    }
                    if !touched_stripes.contains(&s) {
                        touched_stripes.push(s);
                    }
                }
            }
        }
        touched_stripes.sort_unstable();
        for s in touched_stripes {
            self.plan_stripe(now, block_id, s, peer_states, step);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_release_claims(
        &mut self,
        now: Instant,
        peer: NodeId,
        block_id: block::Hash,
        stripe: u32,
        slots: &[u32],
        reason: ReleaseReason,
        peer_states: &[PullPeerState],
        step: &mut BlockStep,
    ) {
        let mut released_any = false;
        {
            let Some(block) = self.blocks.get_mut(&block_id) else {
                return;
            };
            if block.phase != BlockPhase::Collecting {
                return;
            }
            let Some(st) = block.stripes.get_mut(stripe as usize) else {
                return;
            };
            for &slot in slots {
                if st.claims.get(&slot) != Some(&peer) {
                    continue;
                }
                if reason == ReleaseReason::Late {
                    // bpv1-64: late release happens at most ONCE per index per block.
                    if !st.late_released.insert(slot) {
                        continue;
                    }
                    step.events.push(BlockEvent::PullLate {
                        peer,
                        block_id,
                        stripe,
                        slot,
                    });
                }
                st.claims.remove(&slot);
                if reason != ReleaseReason::Refused {
                    st.tried[slot as usize].insert(peer);
                }
                released_any = true;
            }
        }
        if released_any {
            self.plan_stripe(now, block_id, stripe, peer_states, step);
        }
    }

    /// Arrival-ordered pull planner (`decision-12`).
    ///
    /// Fills `k - held - pending` one part at a time:
    /// - Picks the announcer with the earliest `arrival(P, ahead + part_bytes)`
    ///   among announcers with room under the 1-RTT transfer cap.
    /// - Prefers source slots (`0..k`) when arrival times tie (encouraging the
    ///   all-systematic fast path, `decision-10`).
    /// - Leaves parts unclaimed if no announcer has room.
    fn plan_stripe(
        &mut self,
        now: Instant,
        block_id: block::Hash,
        stripe: u32,
        peer_states: &[PullPeerState],
        step: &mut BlockStep,
    ) {
        let Some(block) = self.blocks.get_mut(&block_id) else {
            return;
        };
        if block.phase != BlockPhase::Collecting {
            return;
        }
        let coding = block.meta.coding;
        let part_bytes = u64::from(coding.part_bytes);
        // decision-20: a repair round may also ask any `BlockDone` peer.
        let mut done_peers: Vec<NodeId> = if block.repair_round > 0 {
            block.done_peers.iter().copied().collect()
        } else {
            Vec::new()
        };
        done_peers.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        let Some(st) = block.stripes.get_mut(stripe as usize) else {
            return;
        };
        if st.assemblable || st.decoding() {
            return;
        }
        let already = st.held_count.saturating_add(st.pending_count());
        if already >= coding.k {
            return;
        }
        let mut needed = coding.k - already;

        // Working `ahead` bytes and window per eligible peer, updated as this plan grants.
        let mut room: HashMap<NodeId, (u64, u32)> = peer_states
            .iter()
            .filter(|ps| ps.can_pull && ps.window_remaining > 0)
            .map(|ps| (ps.peer, (ps.ahead_bytes, ps.window_remaining)))
            .collect();
        if room.is_empty() {
            return;
        }

        let mut grants: HashMap<NodeId, PlannedPull> = HashMap::new();

        while needed > 0 {
            // Find the best `(slot, peer)` pair across all unclaimed, unheld slots:
            // earliest arrival, then source before parity (`decision-10`), then slot and peer.
            let mut best: Option<(Arrival, bool /* is_parity */, u32, NodeId)> = None;

            for slot in 0..coding.n {
                if st.has(slot, coding.k)
                    || st.claims.contains_key(&slot)
                    || st.incoming.contains_key(&slot)
                {
                    continue;
                }
                let tried = &st.tried[slot as usize];
                let announcers = &st.announcers[slot as usize];
                let sources = announcers
                    .iter()
                    .chain(done_peers.iter().filter(|peer| !announcers.contains(*peer)));
                for &peer in sources {
                    if tried.contains(&peer) {
                        continue;
                    }
                    let Some(&(ahead, window)) = room.get(&peer) else {
                        continue;
                    };
                    if window == 0 {
                        continue;
                    }
                    let Some(arrival) = self.paths.arrival_with_room(&peer, ahead, part_bytes, now)
                    else {
                        continue;
                    };
                    let is_parity = slot >= coding.k;
                    let better = best.is_none_or(|(b, b_parity, b_slot, b_peer)| {
                        (arrival.at, is_parity, slot, peer.as_bytes())
                            < (b.at, b_parity, b_slot, b_peer.as_bytes())
                    });
                    if better {
                        best = Some((arrival, is_parity, slot, peer));
                    }
                }
            }

            let Some((arrival, _, slot, peer)) = best else {
                break;
            };

            st.claims.insert(slot, peer);
            if let Some((ahead, window)) = room.get_mut(&peer) {
                *ahead = ahead.saturating_add(part_bytes);
                *window = window.saturating_sub(1);
            }
            let grant = grants.entry(peer).or_insert(PlannedPull {
                requested: WantBitmap::EMPTY,
                arrival,
            });
            grant.requested.set(slot);
            if arrival.at > grant.arrival.at {
                grant.arrival = arrival;
            }
            needed -= 1;
        }

        // Emit grants in deterministic peer order.
        let mut grant_list: Vec<(NodeId, PlannedPull)> = grants.into_iter().collect();
        grant_list.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));

        for (peer, grant) in grant_list {
            step.outputs.push(BlockOutput::GrantPull {
                peer,
                block_id,
                stripe,
                requested: grant.requested,
                arrival: grant.arrival,
            });
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_insert_part(
        &mut self,
        now: Instant,
        block_id: block::Hash,
        part: VerifiedPart,
        from_peer: NodeId,
        auth: PartAuth,
        peer_states: &[PullPeerState],
        step: &mut BlockStep,
    ) {
        let index = part.index();
        let mut replan_stripe: Option<u32> = None;

        {
            let Some(block) = self.blocks.get_mut(&block_id) else {
                return;
            };
            // 03 §4: every peer part is charged before proof. Removing the charge
            // here releases it on every early return; storing the part keeps it.
            let Some(charge) = block.part_charges.remove(&(index, from_peer)) else {
                return;
            };
            if matches!(block.phase, BlockPhase::Retired | BlockPhase::Fallback) {
                return;
            }
            // Defense-in-depth check that the part was verified
            // against this block's admitted `part_root`.
            if part.part_root() != block.meta.part_root {
                return;
            }
            let coding = block.meta.coding;
            let Some((stripe, slot)) = coding.split_index(index) else {
                return;
            };
            let st = &mut block.stripes[stripe as usize];
            let source = match auth {
                PartAuth::Seed if st.pushed.contains(&slot) => PartSource::Seed,
                PartAuth::Seed => PartSource::Direct,
                PartAuth::Push => PartSource::Push,
                PartAuth::Want(_) if block.repair_round > 0 => PartSource::Repair,
                PartAuth::Want(_) => PartSource::Pull,
            };
            if !st.store(slot, coding.k, part) {
                // bpv1-22: first valid copy wins; duplicate drops cleanly.
                block.duplicates = block.duplicates.saturating_add(1);
                step.events.push(BlockEvent::PartInserted {
                    block_id,
                    index,
                    first: false,
                    from_peer,
                    source,
                });
                return;
            }
            st.claims.remove(&slot);
            // A part outside the stripe's claims (a seed, or a part that a
            // released claim still delivered) can leave more claims than the
            // stripe needs. Drop the excess, highest slot first, so held +
            // claims stays within `k` (bpv1-28). Their `Want`s stay live, and
            // any part they deliver is still stored.
            // A stripe has at most `MAX_N` slots, so the count fits u32.
            while st.held_count.saturating_add(st.claims.len() as u32) > coding.k {
                let Some(&excess) = st.claims.keys().max() else {
                    break;
                };
                st.claims.remove(&excess);
            }
            // Progress resets stall timer, NEVER reconstruction_deadline (bpv1-57).
            block.last_part_at = now;
            block.stored_charge += charge;
            if !std::mem::replace(&mut block.first_part_stored, true) {
                let first_part_at = block.first_part_at.unwrap_or(now);
                step.events.push(BlockEvent::FirstPartStored {
                    block_id,
                    meta_to_first_part: first_part_at
                        .saturating_duration_since(block.meta_arrived_at),
                    first_part_to_stored: now.saturating_duration_since(first_part_at),
                });
            }

            step.events.push(BlockEvent::PartInserted {
                block_id,
                index,
                first: true,
                from_peer,
                source,
            });
            // bpv1-71: a pushed seed goes on from whichever source delivered
            // it first; no other part ever does.
            step.outputs.push(BlockOutput::PartReady {
                block_id,
                indices: vec![index],
                push: st.pushed.contains(&slot),
            });

            if st.held_count == coding.k && st.job.is_none() {
                st.claims.clear();
                st.incoming.clear();
                step.events
                    .push(BlockEvent::StripeReachedK { block_id, stripe });
                step.outputs
                    .push(BlockOutput::NotifyStripeDone { block_id, stripe });

                // decision-10: with all `k` source parts the stripe is assemblable
                // now and only needs a low-priority re-encode check.
                let kind = if st.source_held_count == coding.k {
                    st.assemblable = true;
                    StripeJob::Check
                } else {
                    StripeJob::Decode
                };
                let work_id = alloc_work_id(&mut self.next_work_id);
                st.job = Some((work_id, kind));
                step.outputs.push(BlockOutput::StartStripeJob {
                    work_id,
                    kind,
                    block_id,
                    stripe,
                    coding,
                    enc_id: block.enc_id,
                    held: st.held.clone(),
                });
            } else if st.held_count < coding.k {
                replan_stripe = Some(stripe);
            }
        }

        // Check if every stripe is now assemblable (e.g., all-systematic fast path).
        self.maybe_assemble_and_verify(now, block_id, step);

        if let Some(stripe) = replan_stripe {
            self.plan_stripe(now, block_id, stripe, peer_states, step);
        }
    }

    fn handle_stripe_checked(
        &mut self,
        now: Instant,
        work_id: WorkId,
        block_id: block::Hash,
        stripe: u32,
        result: Result<Vec<VerifiedPart>, ()>,
        step: &mut BlockStep,
    ) {
        let mut should_fallback = false;
        {
            let Some(block) = self.blocks.get_mut(&block_id) else {
                return;
            };
            if matches!(block.phase, BlockPhase::Retired | BlockPhase::Fallback) {
                return;
            }
            let coding = block.meta.coding;
            let Some(st) = block.stripes.get_mut(stripe as usize) else {
                return;
            };
            // bpv1-63: ignore stale completions.
            if st.job.map(|(id, _)| id) != Some(work_id) {
                return;
            }
            st.job = None;

            match result {
                Ok(all_parts) => {
                    st.assemblable = true;

                    // Regenerated parts become servable ONLY after check passes
                    // (bpv1-41). A regenerated pushed seed goes on (bpv1-71).
                    let mut newly_ready = Vec::new();
                    let mut newly_pushed = Vec::new();
                    for part in all_parts {
                        let index = part.index();
                        if let Some((s, slot)) = coding.split_index(index) {
                            if s == stripe && st.store(slot, coding.k, part) {
                                if st.pushed.contains(&slot) {
                                    newly_pushed.push(index);
                                } else {
                                    newly_ready.push(index);
                                }
                            }
                        }
                    }
                    for (indices, push) in [(newly_ready, false), (newly_pushed, true)] {
                        if !indices.is_empty() {
                            step.outputs.push(BlockOutput::PartReady {
                                block_id,
                                indices,
                                push,
                            });
                        }
                    }
                }
                Err(()) => {
                    should_fallback = true;
                }
            }
        }

        if should_fallback {
            // bpv1-41: a re-encode failure stops coded propagation. Before commit
            // the block falls back, which retires it. A committed block needs no
            // fallback, but its encoding is retired too, so its parts stop
            // circulating.
            self.enter_fallback(block_id, FallbackReason::ReencodeFailed, step);
            self.retire_block(block_id, step);
        } else {
            self.maybe_assemble_and_verify(now, block_id, step);
        }
    }

    fn handle_block_assembled(
        &mut self,
        now: Instant,
        work_id: WorkId,
        block_id: block::Hash,
        step: &mut BlockStep,
    ) {
        let Some(block) = self.blocks.get_mut(&block_id) else {
            return;
        };
        if block.verify_work_id != Some(work_id)
            || matches!(block.phase, BlockPhase::Fallback | BlockPhase::Retired)
        {
            return;
        }
        block.notify_done(now, step);
    }

    fn maybe_assemble_and_verify(
        &mut self,
        now: Instant,
        block_id: block::Hash,
        step: &mut BlockStep,
    ) {
        let Some(block) = self.blocks.get_mut(&block_id) else {
            return;
        };
        // bpv1-44: `Verifying` and every later phase are past collection.
        if block.phase != BlockPhase::Collecting
            || !block.all_stripes_assemblable()
            || !self.submit_assembled(block_id, step)
        {
            return;
        }
        let block = self
            .blocks
            .get_mut(&block_id)
            .expect("the block exists because it was just submitted");
        let deadline = now + VERIFY_DEADLINE;
        block.phase = BlockPhase::Verifying;
        block.verify_deadline = Some(deadline);
        // bpv1-44: the body claim covers verification, so block sync fetches no
        // second copy while the verifier holds this one. The verification
        // deadline bounds it, as the reconstruction deadline bounds collection.
        if block.body_claimed {
            step.outputs
                .push(BlockOutput::ExtendBodyClaim { block_id, deadline });
        }
        step.events.push(BlockEvent::Handoff { block_id });
    }

    /// Submits an assembled block's body to the verifier, and returns whether
    /// it held every source part.
    ///
    /// `BlockTable` keeps every source part of an assembled block, so a parked
    /// block resubmits without fetching anything (`bpv1-84`).
    fn submit_assembled(&mut self, block_id: block::Hash, step: &mut BlockStep) -> bool {
        let Some(block) = self.blocks.get_mut(&block_id) else {
            return false;
        };
        let coding = block.meta.coding;
        let mut stripe_sources = Vec::with_capacity(block.stripes.len());
        let mut zeros: Option<Arc<[u8]>> = None;
        for st in &block.stripes {
            let mut sources = Vec::with_capacity(coding.k as usize);
            for slot in 0..coding.k {
                let payload = match &st.held[slot as usize] {
                    Some(part) => part.payload_arc(),
                    None if st.is_padding(slot, coding.k) => zeros
                        .get_or_insert_with(|| vec![0u8; coding.part_bytes as usize].into())
                        .clone(),
                    None => return false,
                };
                sources.push(payload);
            }
            stripe_sources.push(sources);
        }

        let work_id = alloc_work_id(&mut self.next_work_id);
        block.verify_work_id = Some(work_id);

        // `NotifyBlockDone` waits for `BlockAssembled` or a committed verdict.
        step.outputs.push(BlockOutput::SubmitVerifier {
            work_id,
            block_id,
            header_bytes: block.meta.header_bytes.clone(),
            coinbase_bytes: block.meta.binding.coinbase.clone(),
            coding,
            stripe_sources,
        });
        true
    }

    fn handle_verifier_verdict(
        &mut self,
        now: Instant,
        work_id: WorkId,
        block_id: block::Hash,
        verdict: VerifierVerdict,
        step: &mut BlockStep,
    ) {
        let Some(block) = self.blocks.get_mut(&block_id) else {
            return;
        };
        if block.verify_work_id != Some(work_id) {
            // bpv1-63: ignore stale verdict.
            return;
        }
        block.verify_work_id = None;
        if block.phase == BlockPhase::Retired {
            return;
        }
        if matches!(verdict, VerifierVerdict::Valid | VerifierVerdict::Duplicate) {
            block.notify_done(now, step);
        }
        step.events.push(BlockEvent::Verdict { block_id, verdict });
        match verdict {
            VerifierVerdict::Valid | VerifierVerdict::Duplicate => {
                block.phase = BlockPhase::Done;
                let outcome = if block.repair_round > 0 {
                    BlockOutcome::Repair
                } else {
                    BlockOutcome::Normal
                };
                step.events.push(BlockEvent::Outcome { block_id, outcome });
                if std::mem::take(&mut block.body_claimed) {
                    step.outputs.push(BlockOutput::ReleaseBodyClaim {
                        block_id,
                        reason: ClaimRelease::Committed,
                    });
                }
            }
            // decision-8 / bpv1-43: fallback; NEVER mark the header invalid.
            VerifierVerdict::RootMismatch => {
                self.enter_fallback(block_id, FallbackReason::RootMismatch, step);
            }
            // bpv1-84: a retryable verdict says nothing about the body. While the
            // parent is not committed, the block cannot commit through any path,
            // so it keeps its body and claim and resubmits once the parent
            // commits. With the parent committed, it falls back.
            VerifierVerdict::LocalCapacity if block.height > self.verified.saturating_add(1) => {
                step.events.push(BlockEvent::Parked { block_id });
            }
            VerifierVerdict::LocalCapacity => {
                self.enter_fallback(block_id, FallbackReason::LocalCapacity, step);
            }
            // bpv1-45: consensus-invalid with matching roots -> retire and stop serving.
            VerifierVerdict::Invalid => self.retire_block(block_id, step),
        }
    }

    fn enter_fallback(
        &mut self,
        block_id: block::Hash,
        reason: FallbackReason,
        step: &mut BlockStep,
    ) {
        let Some(block) = self.blocks.get_mut(&block_id) else {
            return;
        };
        // bpv1-57: fallback starts at most once per block, and never after commit.
        if !matches!(block.phase, BlockPhase::Collecting | BlockPhase::Verifying) {
            return;
        }
        block.repair_round = 0;
        block.phase = BlockPhase::Fallback;
        // bpv1-63: fallback ends the submission, so a later verdict is stale.
        block.verify_work_id = None;
        for st in &mut block.stripes {
            st.claims.clear();
            st.job = None;
        }
        step.events
            .push(BlockEvent::FallbackStarted { block_id, reason });
        step.events.push(BlockEvent::Outcome {
            block_id,
            outcome: BlockOutcome::Fallback,
        });
        if std::mem::take(&mut block.body_claimed) {
            step.outputs.push(BlockOutput::ReleaseBodyClaim {
                block_id,
                reason: ClaimRelease::Fallback,
            });
        }
        // A bad encoding retires at once, so `get_part` stops serving it.
        if matches!(
            reason,
            FallbackReason::ReencodeFailed | FallbackReason::RootMismatch
        ) {
            self.retire_block(block_id, step);
        }
    }

    /// Native block sync committed `block_id`. A `Fallback` block holds no
    /// complete body to relay, so it retires at once and frees its slot.
    fn handle_fallback_done(&mut self, block_id: block::Hash, step: &mut BlockStep) {
        if self.block_phase(&block_id) == Some(BlockPhase::Fallback) {
            self.retire_block(block_id, step);
        }
    }

    fn handle_proposed_commit_failed(&mut self, block_id: block::Hash, step: &mut BlockStep) {
        // Only the proposer's own copy: a peer's copy of the same block has
        // its own verdict.
        if self
            .blocks
            .get(&block_id)
            .is_some_and(|block| block.as_proposer)
        {
            self.retire_block(block_id, step);
        }
    }

    fn handle_tick(&mut self, now: Instant, peer_states: &[PullPeerState], step: &mut BlockStep) {
        // 1. Age out expired pending metadata (bpv1-11).
        let max_age = PENDING_META_AGE;
        let aged: Vec<block::Hash> = self
            .pending
            .iter()
            .filter(|(_, e)| now.saturating_duration_since(e.received_at) >= max_age)
            .map(|(id, _)| *id)
            .collect();
        for id in aged {
            if let Some(entry) = self.pending.remove(&id) {
                self.pending_bytes = self
                    .pending_bytes
                    .saturating_sub(entry.envelope.estimated_bytes());
                step.events
                    .push(BlockEvent::MetaPendingAged { block_id: id });
                reject_meta(step, id, entry.from_peer, entry.session_gen);
                reject_also(step, id, &entry.also_from);
            }
        }

        // 2. Late seeds and pushes go to other announcers once, like a late
        //    `Want` (bpv1-64).
        let mut late_seeds: Vec<(block::Hash, u32)> = Vec::new();
        for (&block_id, block) in &mut self.blocks {
            if block.phase != BlockPhase::Collecting {
                continue;
            }
            // Each slot turns late at its own time (bpv1-64).
            let mut late: Vec<NodeId> = block
                .stripes
                .iter()
                .flat_map(|st| st.incoming.values())
                .filter(|pending| now >= pending.late_at)
                .map(|pending| pending.sender)
                .collect();
            late.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
            late.dedup();
            for sender in late {
                let mut released = false;
                for (s_idx, st) in block.stripes.iter_mut().enumerate() {
                    let mut slots: Vec<u32> = st
                        .incoming
                        .iter()
                        .filter(|(_, p)| p.sender == sender && now >= p.late_at)
                        .map(|(&slot, _)| slot)
                        .collect();
                    if slots.is_empty() {
                        continue;
                    }
                    slots.sort_unstable();
                    // A stripe has at most `MAX_N` slots, so the index fits u32.
                    let stripe = s_idx as u32;
                    for slot in slots {
                        st.incoming.remove(&slot);
                        if st.late_released.insert(slot) {
                            step.events.push(BlockEvent::PullLate {
                                peer: sender,
                                block_id,
                                stripe,
                                slot,
                            });
                        }
                        st.tried[slot as usize].insert(sender);
                    }
                    released = true;
                    late_seeds.push((block_id, stripe));
                }
                if released && sender != block.meta.proposer_node {
                    step.events.push(BlockEvent::PushLate {
                        peer: sender,
                        block_id,
                    });
                }
            }
        }
        for (block_id, stripe) in late_seeds {
            self.plan_stripe(now, block_id, stripe, peer_states, step);
        }

        // 3. Repair rounds repeat on stall; the monotonic deadline alone ends them (bpv1-57, decision-17).
        let mut to_fallback: Vec<block::Hash> = Vec::new();
        let mut to_repair: Vec<(block::Hash, u32)> = Vec::new();

        let mut verify_expired: Vec<block::Hash> = Vec::new();

        for (&block_id, block) in &mut self.blocks {
            // bpv1-82: verification ends by its deadline, verdict or not.
            if block.phase == BlockPhase::Verifying
                && block
                    .verify_deadline
                    .is_some_and(|deadline| now >= deadline)
            {
                verify_expired.push(block_id);
                continue;
            }
            if block.phase != BlockPhase::Collecting {
                continue;
            }
            if now >= block.reconstruction_deadline {
                to_fallback.push(block_id);
                continue;
            }
            let stall_interval = block.stall_interval(stall_base(&self.paths, &block.meta.coding));
            if now >= block.last_part_at + stall_interval {
                let round = block.repair_round.saturating_add(1);
                block.repair_round = round;
                block.last_part_at = now;
                step.events
                    .push(BlockEvent::RepairRound { block_id, round });
                // decision-22: the first round releases the body claim, so
                // native block sync races the stalled block.
                if std::mem::take(&mut block.body_claimed) {
                    step.outputs.push(BlockOutput::ReleaseBodyClaim {
                        block_id,
                        reason: ClaimRelease::Repair,
                    });
                }
                // Release stalled claims on incomplete stripes so repair can re-plan.
                for (s_idx, st) in block.stripes.iter_mut().enumerate() {
                    if !st.assemblable && !st.decoding() {
                        st.claims.clear();
                        st.incoming.clear();
                        for tried in &mut st.tried {
                            tried.clear();
                        }
                        to_repair.push((block_id, s_idx as u32));
                    }
                }
            }
        }

        for block_id in to_fallback {
            self.enter_fallback(block_id, FallbackReason::Deadline, step);
        }
        for block_id in verify_expired {
            self.enter_fallback(block_id, FallbackReason::VerifyDeadline, step);
        }
        for (block_id, stripe) in to_repair {
            self.plan_stripe(now, block_id, stripe, peer_states, step);
        }

        // 4. Retire blocks whose height left the retention window.
        self.retire_out_of_window(step);
    }

    fn retire_out_of_window(&mut self, step: &mut BlockStep) {
        // bpv1-7: retire exactly the heights the propagation window refuses.
        // The verified-tip bound (bpv1-76) gates admission only.
        let to_retire: Vec<block::Hash> = self
            .blocks
            .values()
            .filter(|b| b.phase != BlockPhase::Retired && !self.in_propagation_window(b.height))
            .map(|b| b.meta.block_id)
            .collect();
        for id in to_retire {
            self.retire_block(id, step);
        }
        // Drop retired tombstones once well past the retention window and notify
        // peer routines (`EvictRetired`) so long-lived sessions drain.
        let purge_below = self.top.saturating_sub(PROPAGATION_DEPTH.saturating_mul(2));
        let to_evict: Vec<block::Hash> = self
            .blocks
            .values()
            .filter(|b| b.phase == BlockPhase::Retired && b.height < purge_below)
            .map(|b| b.meta.block_id)
            .collect();
        for block_id in to_evict {
            self.blocks.remove(&block_id);
            step.outputs
                .push(BlockOutput::PublishEvictRetired { block_id });
        }
    }

    fn retire_block(&mut self, block_id: block::Hash, step: &mut BlockStep) {
        let Some(block) = self.blocks.get_mut(&block_id) else {
            return;
        };
        if block.phase == BlockPhase::Retired {
            return;
        }
        block.phase = BlockPhase::Retired;
        let stripes = block.meta.coding.stripes;
        let n = block.meta.coding.n;
        let release_claim = std::mem::take(&mut block.body_claimed);
        block.verify_work_id = None;
        for st in &mut block.stripes {
            st.clear_buffers();
        }
        block.part_charges.clear();
        block.stored_charge = 0;
        if !block.as_proposer {
            step.events.push(BlockEvent::Uploaded {
                block_id,
                uploaded_bytes: block.uploaded_bytes,
                body_bytes: block.meta.coding.body_bytes,
            });
            step.events.push(BlockEvent::PartDuplicates {
                block_id,
                count: block.duplicates,
            });
        }
        // A retired block keeps its identity and coding but no longer holds header bytes.
        block.meta.header_bytes = Vec::new();
        block.meta.binding.coinbase = Vec::new();
        block.meta.binding.txid_siblings = Vec::new();
        block.meta.binding.auth_siblings = Vec::new();

        step.events.push(BlockEvent::Retired { block_id });
        if release_claim {
            step.outputs.push(BlockOutput::ReleaseBodyClaim {
                block_id,
                reason: ClaimRelease::Retired,
            });
        }
        step.outputs.push(BlockOutput::PublishRetire {
            block_id,
            stripes,
            n,
        });
    }
}
