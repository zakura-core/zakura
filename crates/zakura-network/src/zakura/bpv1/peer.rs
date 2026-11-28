//! Pure per-session `PeerState` state machine (stack-2).
//!
//! `PeerState` holds values only — no channels, locks, tasks, timers, or
//! runtime handles. Every transition is `step(&mut self, now, input) -> PeerStep`,
//! returning outputs for IO to execute and typed [`PeerEvent`]s for tracing,
//! metrics, and property tests.

use std::{collections::VecDeque, time::Duration};

use zakura_chain::block;
use zakura_quic::NodeId;
use zakura_rt::{collections::HashMap, time::Instant};

use super::{
    blocks::{ReleaseReason, MAX_PENDING_META_COUNT},
    close::{Bpv1CloseCause, Bpv1CloseRow},
    paths::{Arrival, RateSample},
    profile::{Coding, Profile, W2_MAX_HAVE_INDICES},
    tree::VerifiedPart,
    wire::{
        part_payload_len, Bpv1Message, HaveFlags, HaveForm, WantBitmap, WantEndReason,
        BPV1_CONTROL_KIND, BPV1_DATA_KIND, HAVE_FLUSH_INTERVAL, MAX_WANTS_INFLIGHT, PART_RULE,
    },
};

#[cfg(any(test, feature = "p2p-fuzz"))]
mod fuzz;
#[cfg(any(test, feature = "p2p-fuzz"))]
mod invariants;

#[cfg(any(test, feature = "p2p-fuzz"))]
pub use fuzz::{replay_bpv1_peer_bytes, PeerReplaySummary};

/// Per-peer pull window ceiling: the most parts outstanding from one
/// neighbour.
pub const PULL_WINDOW: u32 = 64;

/// Base request deadline (`8 s`, decision-3).
pub const REQUEST_BASE_TIMEOUT: Duration = Duration::from_secs(8);

/// Minimum transfer rate for deadline sizing (`100 KB/s`, decision-3).
pub const REQUEST_MIN_RATE_BPS: u64 = 100_000;

/// Buffered `Have` indices that trigger an immediate flush (opt-11).
pub const HAVE_FLUSH_COUNT: usize = 32;

/// Default early-record age limit (`10 s`).
pub const DEFAULT_EARLY_AGE: Duration = Duration::from_secs(10);

/// Default per-peer data early-record byte cap: two maximum W2 `Part`
/// frames.
pub const DEFAULT_EARLY_PEER_BYTES_CAP: usize = 2 * PART_RULE.payload.max();

/// Control-stream early-record entry cap. At the cap the oldest entry leaves.
pub const EARLY_CONTROL_CAP: usize = 64;

/// Session park cooldown on first stall (`30 s`, bpv1-58).
pub const LIVENESS_COOLDOWN: Duration = Duration::from_secs(30);

/// Fixed late-pull multiplier (`SLOW_X = 2`, decision-12).
pub const SLOW_X: u32 = 2;

/// Direct copies the proposer releases to an idle data writer at once, each
/// batch behind its own flagged `Have` (decision-19). Small, so the recheck
/// before each batch sees the peer's latest `Have`s and `StripeDone`s.
pub const COPY_BATCH: usize = 8;

/// Compact growable bitset allocated strictly from admitted layout dimensions.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BitSet {
    words: Vec<u64>,
    bits: usize,
}

impl BitSet {
    /// Allocates a zeroed bitset of `bits` bits.
    pub fn with_bits(bits: usize) -> Self {
        Self {
            words: vec![0u64; bits.div_ceil(64)],
            bits,
        }
    }

    /// Returns whether bit `index` is set (`false` if out of bounds).
    pub fn get(&self, index: usize) -> bool {
        if index >= self.bits {
            return false;
        }
        (self.words[index / 64] & (1u64 << (index % 64))) != 0
    }

    /// Sets bit `index`, returning `true` if it transitioned from 0 to 1.
    pub fn set(&mut self, index: usize) -> bool {
        if index >= self.bits {
            return false;
        }
        let word = &mut self.words[index / 64];
        let mask = 1u64 << (index % 64);
        let was_set = (*word & mask) != 0;
        *word |= mask;
        !was_set
    }

    /// Total set bits.
    pub fn count_ones(&self) -> u32 {
        self.words.iter().map(|w| w.count_ones()).sum()
    }
}

/// Configuration for one [`PeerState`] session machine.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct PeerConfig {
    /// Coding profile (`W2` in production, `TINY` in tests).
    pub profile: Profile,
    /// Per-peer data early-record byte cap.
    pub early_peer_bytes_cap: usize,
    /// Age after which unresolved early entries expire (`Drop`, never close).
    pub early_age: Duration,
}

impl PeerConfig {
    /// Production defaults for `profile`.
    pub const fn for_profile(profile: Profile) -> Self {
        Self {
            profile,
            early_peer_bytes_cap: DEFAULT_EARLY_PEER_BYTES_CAP,
            early_age: DEFAULT_EARLY_AGE,
        }
    }
}

/// Completion token binding a verify worker job to its exact session generation
/// and part (bpv1-63).
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct PartToken {
    /// Session generation that accepted the part prefix.
    pub session_gen: u64,
    /// Block consensus hash.
    pub block_id: block::Hash,
    /// Wire part index.
    pub index: u32,
    /// Monotonic attempt counter within this session.
    pub attempt: u64,
}

/// How an incoming `Part` prefix was authorized.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PartAuth {
    /// Authorized by a push flag on the connection authenticated as the
    /// block's `proposer_node`: a proposer seed (bpv1-16).
    Seed,
    /// Authorized by a push slot the peer opened with a flagged `Have`
    /// (decision-24, bpv1-69).
    Push,
    /// Authorized by live outgoing `Want` with `want_id` (bpv1-13).
    Want(u32),
}

/// Exhaustive drop reasons for a part.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum DropReason {
    /// Arrived after our `StripeDone` or `BlockDone`, still authorized (bpv1-20).
    AfterStop,
    /// Arrived for a retired block within the peer's retired counter (bpv1-21).
    RetiredLate,
    /// Early-replayed part whose authorization no longer holds.
    Unwanted,
    /// Aged out of the early record before block admission (bpv1-14, -48).
    EarlyAged,
    /// Refused because the early record had no room beside held entries (bpv1-48).
    EarlyFull,
    /// Refused because the frame alone exceeds the early record's cap (bpv1-48).
    EarlyOversize,
    /// Refused before proof because the block table's part buffer was full (bpv1-52).
    LocalCapacity,
    /// Refused before proof because the block table no longer holds the block.
    NotAdmitted,
    /// Arrived for a block whose `HeaderMeta` crossed but whose state this
    /// session dropped: the table rejected the peer's copy, or evicted the
    /// retired block (bpv1-14).
    MetaDropped,
}

/// Block admitted locally in `BlockTable`; triggers `HeaderMeta` send (unless
/// it already crossed this connection) and replays held early entries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockAdmitted {
    /// Consensus block hash.
    pub block_id: block::Hash,
    /// Admitted coding tuple.
    pub coding: Coding,
    /// Committed proposer transport identity.
    pub proposer_node: NodeId,
    /// Full `HeaderMeta` message, which the session forwards unless the
    /// metadata already crossed its connection. A new session of the peer
    /// that sent it first still gets it. Every admitted block carries it,
    /// so the metadata crosses each connection before any block traffic
    /// (bpv1-8, bpv1-14).
    pub header_meta: Box<Bpv1Message>,
    /// Whether this local node is the block's proposer (queues `BlockDone` right after `HeaderMeta`, bpv1-9).
    pub as_proposer: bool,
    /// Outside the block's `Have` initial group: when the peer may first
    /// hear of parts, admission plus the advertisement delay. `None` in the
    /// group (decision-26).
    pub have_held_until: Option<Instant>,
}

/// Inbound `Part` prefix `(block_id, index)` read before proof/payload (bpv1-13).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecvPartPrefix {
    /// Block consensus hash.
    pub block_id: block::Hash,
    /// Wire part index.
    pub index: u32,
    /// Whole `Part` frame payload. Its length is the frame's
    /// early-record charge.
    pub payload: Vec<u8>,
}

/// Inbound `Have` decoded on the data stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecvHave {
    /// Block consensus hash.
    pub block_id: block::Hash,
    /// Push and relay flags (decision-24).
    pub flags: HaveFlags,
    /// The form the indices arrived in. A bitmap's length must fit the
    /// admitted geometry (decision-33, close-16).
    pub form: HaveForm,
    /// Strictly increasing wire indices.
    pub indices: Vec<u32>,
    /// Encoded frame bytes (for early-record accounting).
    pub frame_bytes: usize,
}

/// `BlockTable` granted a pull plan for this peer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PullGranted {
    /// Block consensus hash.
    pub block_id: block::Hash,
    /// Stripe index.
    pub stripe: u32,
    /// Exact stripe-relative indices to request.
    pub requested: WantBitmap,
    /// `PeerPaths`' arrival estimate for the last requested part (decision-12).
    pub arrival: Arrival,
}

/// One proposer seed for this peer (decision-19).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Seed {
    /// Verified part to push.
    pub part: VerifiedPart,
    /// Whether the peer pushes the part onward as its holder: a deal-1
    /// pushed seed (decision-24).
    pub relay: bool,
}

/// Proposer queues seeds of its own block to this peer (decision-19).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Seeds {
    /// Block consensus hash.
    pub block_id: block::Hash,
    /// deal-1 and deal-2 seeds, in queue order. They queue at once.
    pub seeds: Vec<Seed>,
    /// deal-3 direct copies, in release order. They wait until the data
    /// writer is idle ([`PeerInput::DataIdle`]), so they follow the seeds.
    pub copies: Vec<VerifiedPart>,
}

/// Inbound `Want` decoded on the control stream (server role).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecvWant {
    /// Request id (`!= 0`).
    pub want_id: u32,
    /// Block consensus hash.
    pub block_id: block::Hash,
    /// Stripe index.
    pub stripe: u32,
    /// Exact requested indices.
    pub requested: WantBitmap,
}

/// Server queued a part in answer to an admitted `RecvWant`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServePartQueued {
    /// Request id.
    pub want_id: u32,
    /// Block consensus hash.
    pub block_id: block::Hash,
    /// Wire part index.
    pub index: u32,
}

/// Server finished producing parts for `want_id` and queues `WantEnd`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServeDone {
    /// Request id.
    pub want_id: u32,
    /// Total parts sent for this `Want`.
    pub sent: u16,
    /// End reason.
    pub reason: WantEndReason,
}

/// Inbound `WantEnd` decoded on the data stream (requester role).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecvWantEnd {
    /// Request id.
    pub want_id: u32,
    /// Number of parts server reports sending.
    pub sent: u16,
}

/// Local `BlockTable` retired `block_id`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Retire {
    /// Block consensus hash.
    pub block_id: block::Hash,
    /// Layout stripes (used if peer had no active record for `block_id`).
    pub stripes: u32,
    /// Layout `n`.
    pub n: u32,
}

/// Inputs accepted by [`PeerState::step`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PeerInput {
    /// Block admitted locally in `BlockTable`; triggers `HeaderMeta` send (unless
    /// the peer already sent it) and replays held early entries.
    BlockAdmitted(BlockAdmitted),
    /// Inbound `HeaderMeta` decoded on the data stream.
    RecvHeaderMeta {
        /// Consensus hash of the header carried in `HeaderMeta`.
        block_id: block::Hash,
        /// Decoded `HeaderMeta` message.
        message: Box<Bpv1Message>,
    },
    /// Inbound `Part` prefix `(block_id, index)` read before proof/payload (bpv1-13).
    RecvPartPrefix(RecvPartPrefix),
    /// Proof verification worker succeeded for `token`.
    PartVerified {
        /// Completion token.
        token: PartToken,
        /// Verified part.
        part: VerifiedPart,
    },
    /// Proof verification worker failed for `token`.
    PartFailed {
        /// Completion token.
        token: PartToken,
    },
    /// The block table refused the part for `token` before proof. The peer
    /// did nothing wrong.
    PartRefused {
        /// Completion token.
        token: PartToken,
        /// [`DropReason::LocalCapacity`] or [`DropReason::NotAdmitted`].
        reason: DropReason,
    },
    /// Local `BlockTable` has verified/checked parts ready to announce to this peer.
    Ready {
        /// Block consensus hash.
        block_id: block::Hash,
        /// Verified part indices now held locally.
        indices: Vec<u32>,
        /// Whether `indices` are this node's pushed seeds, which it pushes
        /// to every peer but the proposer (decision-24, bpv1-71).
        push: bool,
    },
    /// Inbound `Have` decoded on the data stream.
    RecvHave(RecvHave),
    /// `BlockTable` granted a pull plan for this peer.
    PullGranted(PullGranted),
    /// Proposer queues seeds of its own block to this peer (decision-19).
    Seeds(Seeds),
    /// Inbound `Want` decoded on the control stream (server role).
    RecvWant(RecvWant),
    /// Server queued a part in answer to an admitted `RecvWant`.
    ServePartQueued(ServePartQueued),
    /// Server finished producing parts for `want_id` and queues `WantEnd`.
    ServeDone(ServeDone),
    /// The data writer handed one queued `WantEnd` to the transport.
    WantEndWritten,
    /// Inbound `WantEnd` decoded on the data stream (requester role).
    RecvWantEnd(RecvWantEnd),
    /// Local node reached `k` verified parts for `(block_id, stripe)`.
    LocalStripeDone {
        /// Block consensus hash.
        block_id: block::Hash,
        /// Completed stripe index.
        stripe: u32,
    },
    /// Local node assembled `block_id`.
    LocalBlockDone {
        /// Block consensus hash.
        block_id: block::Hash,
    },
    /// Inbound `StripeDone` on the control stream.
    RecvStripeDone {
        /// Block consensus hash.
        block_id: block::Hash,
        /// Completed stripe index.
        stripe: u32,
    },
    /// Inbound `BlockDone` on the control stream.
    RecvBlockDone {
        /// Block consensus hash.
        block_id: block::Hash,
    },
    /// Local `BlockTable` retired `block_id`.
    Retire(Retire),
    /// Local `BlockTable` neutrally dropped or expired a pending `HeaderMeta`
    /// without admitting it, clearing `meta_received_pending`.
    MetaRejected {
        /// Block consensus hash.
        block_id: block::Hash,
    },
    /// Block left the `PROPAGATION_DEPTH` window; drop retired counters.
    EvictRetired {
        /// Block consensus hash.
        block_id: block::Hash,
    },
    /// Periodic timer tick driving early expiry, late pulls, deadlines, liveness, and `Have` flush.
    Tick,
    /// The data writer has nothing queued or in flight, and
    /// [`PeerState::has_copies`] holds: release the next batch of direct
    /// copies (decision-19).
    DataIdle,
    /// The control frame in flight has waited [`stall_timeout`] for its bytes:
    /// the peer stopped reading the control stream (bpv1-58).
    ControlStalled,
    /// Session ended; drain all per-peer state.
    SessionEnd,
}

/// Outputs emitted by [`PeerState::step`] for IO to execute.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PeerOutput {
    /// Enqueue a control-stream message (`Want`, `StripeDone`, `BlockDone`).
    SendControl(Box<Bpv1Message>),
    /// Enqueue a `HeaderMeta` on the data stream ahead of every queued item
    /// but earlier `HeaderMeta`s, so it precedes its block's traffic (bpv1-8).
    SendHeaderMeta(Box<Bpv1Message>),
    /// Enqueue a data-stream `Have` message behind queued data. A flagged
    /// `Have` precedes the [`PeerOutput::PushPart`]s it flags (bpv1-72).
    SendHave {
        /// Block consensus hash.
        block_id: block::Hash,
        /// Push and relay flags (decision-24).
        flags: HaveFlags,
        /// The smaller index form for the block's geometry (decision-33).
        form: HaveForm,
        /// Strictly increasing wire indices.
        indices: Vec<u32>,
    },
    /// Enqueue held part `index` on the data stream behind its flagged
    /// `Have` (decision-24).
    PushPart {
        /// Block consensus hash.
        block_id: block::Hash,
        /// Wire part index.
        index: u32,
    },
    /// Enqueue a proposer seed `Part` on the data stream behind its flagged
    /// `Have` (decision-19).
    SendSeedPart {
        /// Block consensus hash.
        block_id: block::Hash,
        /// Verified part to write.
        part: VerifiedPart,
    },
    /// The peer flagged `indices` for push; `BlockTable` counts them as
    /// pending claims held by the peer (decision-24, bpv1-69).
    PushFlagged {
        /// Block consensus hash.
        block_id: block::Hash,
        /// Flagged wire indices.
        indices: Vec<u32>,
        /// Whether the proposer flagged them for relay: this node pushes
        /// them onward as their holder (decision-24).
        relay: bool,
    },
    /// Enqueue a reserved `WantEnd` on the data stream behind its parts.
    SendWantEnd {
        /// Request id.
        want_id: u32,
        /// Parts written for this `Want`.
        sent: u16,
        /// End reason.
        reason: WantEndReason,
    },
    /// Forward a decoded `HeaderMeta` to `BlockTable` for headerchain admission.
    SubmitHeaderMeta {
        /// Block consensus hash.
        block_id: block::Hash,
        /// Decoded `HeaderMeta`.
        message: Box<Bpv1Message>,
    },
    /// Read proof+payload and verify part `(block_id, index)` under `token`.
    VerifyPart {
        /// Completion token.
        token: PartToken,
        /// Whole `Part` frame payload to parse and verify.
        payload: Vec<u8>,
    },
    /// Hand a verified part to `BlockTable`.
    InsertPart {
        /// Block consensus hash.
        block_id: block::Hash,
        /// Verified part.
        part: VerifiedPart,
        /// Generation of the session that accepted the part, which owns its
        /// charge.
        session_gen: u64,
        /// How the part was authorized.
        auth: PartAuth,
    },
    /// Notify `BlockTable` that this peer announced `indices` for `block_id`.
    PeerHas {
        /// Block consensus hash.
        block_id: block::Hash,
        /// Newly announced wire indices.
        indices: Vec<u32>,
    },
    /// The peer sent `BlockDone`: it holds every part, so repair may ask it
    /// (decision-20).
    PeerBlockDone {
        /// Block consensus hash.
        block_id: block::Hash,
    },
    /// Dispatch an admitted inbound `Want` to `Serve` for the requested parts
    /// that have not already been sent to this peer (bpv1-17).
    ServeWant {
        /// Request id.
        want_id: u32,
        /// Block consensus hash.
        block_id: block::Hash,
        /// Stripe index.
        stripe: u32,
        /// Eligible stripe-relative slots (filtered for send-once & peer view).
        eligible_slots: Vec<u32>,
    },
    /// Cancel the queued unsent push of part `index` of `block_id`: the peer
    /// announced or sent the part meanwhile.
    CancelQueuedPush {
        /// Block consensus hash.
        block_id: block::Hash,
        /// Wire part index.
        index: u32,
    },
    /// Cancel queued unsent parts of `(block_id, stripe)` in the data writer (bpv1-38).
    CancelQueuedStripe {
        /// Block consensus hash.
        block_id: block::Hash,
        /// Stripe index.
        stripe: u32,
    },
    /// Purge queued unsent parts (and optionally `Have`s) of `block_id` (bpv1-38, -46).
    PurgeQueuedBlock {
        /// Block consensus hash.
        block_id: block::Hash,
        /// Whether to also purge queued `Have`s (true on retirement).
        include_haves: bool,
    },
    /// Release this session's pull claims for undelivered indices (late pull, deadline, or refused grant).
    ReleaseClaims {
        /// Block consensus hash.
        block_id: block::Hash,
        /// Stripe index.
        stripe: u32,
        /// Stripe-relative slots whose node-wide claims are released.
        slots: Vec<u32>,
        /// Why the claims are released.
        reason: ReleaseReason,
    },
    /// A delivery-rate sample from a `Want` that received parts (decision-21).
    RateSample(RateSample),
    /// Notify `BlockTable` that an outgoing `Want` ended so remaining unreceived
    /// slots can be re-planned.
    WantCompleted {
        /// Request id.
        want_id: u32,
        /// Block consensus hash.
        block_id: block::Hash,
        /// Stripe index.
        stripe: u32,
        /// Requested slots that never arrived before `WantEnd`.
        unreceived_slots: Vec<u32>,
    },
    /// Pause reading this peer's data stream while early record is full (bpv1-48).
    PauseDataReads,
    /// Resume reading this peer's data stream once early record has room.
    ResumeDataReads,
    /// Close the connection due to an unambiguous protocol violation (bpv1-61).
    CloseProtocol(Bpv1CloseCause),
    /// Close the connection neutrally after a repeat liveness stall (bpv1-58).
    CloseNeutralStall,
}

/// A [`PeerOutput::SendHave`] of `indices` in the smaller form for `coding`
/// (decision-33).
fn send_have(
    block_id: block::Hash,
    coding: &Coding,
    flags: HaveFlags,
    indices: Vec<u32>,
) -> PeerOutput {
    PeerOutput::SendHave {
        block_id,
        flags,
        form: HaveForm::smaller(coding.total_parts().unwrap_or(0), indices.len()),
        indices,
    }
}

/// How long a transfer of `bytes` may make no progress before the session
/// stalls: `4 × (REQUEST_BASE_TIMEOUT + bytes / REQUEST_MIN_RATE_BPS)`
/// (decision-3, bpv1-58).
pub fn stall_timeout(bytes: u64) -> Duration {
    let transfer_ms = bytes.saturating_mul(1000).div_ceil(REQUEST_MIN_RATE_BPS);
    REQUEST_BASE_TIMEOUT
        .saturating_add(Duration::from_millis(transfer_ms))
        .saturating_mul(4)
}

/// Typed events emitted beside [`PeerOutput`]s.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PeerEvent {
    /// `HeaderMeta` received from this peer.
    MetaReceived(block::Hash),
    /// The peer opened `count` push slots with a flagged `Have` (decision-24).
    PushFlagged {
        /// Block consensus hash.
        block_id: block::Hash,
        /// Flagged indices.
        count: usize,
    },
    /// This node queued an unflagged `Have` of `count` indices (decision-26).
    HaveSent {
        /// Block consensus hash.
        block_id: block::Hash,
        /// Announced indices.
        count: usize,
        /// Whether the peer is outside the block's initial group.
        delayed: bool,
    },
    /// This node announced `count` verified indices to the block's proposer
    /// (decision-32).
    HaveSentToProposer {
        /// Block consensus hash.
        block_id: block::Hash,
        /// Announced indices.
        count: usize,
    },
    /// This node proposed the block, and a neighbour announced `indices`
    /// (decision-32). The proposer's trace records when each neighbour got
    /// each part.
    ProposerHeardHave {
        /// Block consensus hash.
        block_id: block::Hash,
        /// Announced indices.
        indices: Vec<u32>,
    },
    /// The advertisement deadline dropped `count` held indices that the
    /// skip rules no longer allow (decision-26).
    HaveSkippedAtDeadline {
        /// Block consensus hash.
        block_id: block::Hash,
        /// Dropped indices.
        count: usize,
    },
    /// The proposer released a batch of direct copies behind their flagged
    /// `Have` (decision-19).
    CopiesQueued {
        /// Block consensus hash.
        block_id: block::Hash,
        /// Copies released.
        count: usize,
        /// Their payload bytes.
        bytes: u64,
    },
    /// `Part` prefix authorized.
    PartPrefix {
        /// Block consensus hash.
        block_id: block::Hash,
        /// Wire part index.
        index: u32,
        /// Attribution source.
        auth: PartAuth,
    },
    /// `Part` dropped without faulting the peer.
    PartDropped {
        /// Block consensus hash.
        block_id: block::Hash,
        /// Wire part index.
        index: u32,
        /// Exhaustive reason.
        reason: DropReason,
    },
    /// Early entry buffered awaiting block admission or `HeaderMeta` exchange.
    EarlyHeld {
        /// Block consensus hash.
        block_id: block::Hash,
        /// Bytes held.
        bytes: usize,
    },
    /// Early entry aged out (`Drop`).
    EarlyAged {
        /// Block consensus hash.
        block_id: block::Hash,
        /// Bytes freed.
        bytes: usize,
    },
    /// Outgoing `Want` ended by `WantEnd`.
    WantEnded {
        /// Request id.
        want_id: u32,
        /// Parts server reported sending.
        sent: u16,
        /// Parts receiver attributed to this `Want`.
        attributed: u16,
    },
    /// Outgoing `Want` exceeded `SLOW_X * estimated_time` (bpv1-64).
    WantLate {
        /// Request id.
        want_id: u32,
        /// Released slot count.
        released: usize,
    },
    /// Outgoing `Want` passed its hard request deadline (bpv1-31).
    WantDeadline {
        /// Request id.
        want_id: u32,
        /// Released slot count.
        released: usize,
    },
    /// Session parked after first liveness timeout (bpv1-58).
    Parked,
    /// Session unparked after cooldown.
    Unparked,
}

/// Result of one synchronous [`PeerState::step`] transition.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PeerStep {
    /// Effects for IO to execute in order.
    pub outputs: Vec<PeerOutput>,
    /// Typed observability events.
    pub events: Vec<PeerEvent>,
}

#[derive(Clone, Debug)]
struct PeerBlockState {
    coding: Coding,
    proposer_node: NodeId,
    /// This node proposed the block: it admitted the block from its own
    /// solved template (bpv1-9). The miner picks `proposer_node`, so a block
    /// can name a node that never proposed it; only this flag says a node did.
    as_proposer: bool,
    /// Push slots the peer opened with flagged `Have`s (decision-24). A slot
    /// stays set after its part arrives, so it also counts toward the
    /// per-stripe bound (bpv1-69).
    peer_flagged: BitSet,
    /// The proposer's relay flags on this connection: this node's pushed
    /// seeds (decision-24).
    peer_relayed: BitSet,
    /// The early record dropped a flagged `Have` of the peer before
    /// admission. A part that nothing else authorizes then opens a push slot
    /// itself while its stripe has fewer than the connection's push bound,
    /// and no `Want` goes to the peer for such a stripe (bpv1-60, bpv1-69).
    flags_lost: bool,
    /// Indices this node flagged and pushed to the peer: its pushed seeds,
    /// or as the proposer, its seeds (bpv1-71).
    local_flagged: BitSet,
    /// As the proposer: direct copies waiting for an idle data writer, in
    /// release order (decision-19).
    copies: VecDeque<VerifiedPart>,
    /// This session wrote the block's `HeaderMeta`. A repeated
    /// `BlockAdmitted` never writes it twice (bpv1-4).
    meta_sent: bool,
    /// The peer sent the block's `HeaderMeta` on this connection. Admission
    /// sets `meta_sent` or this, so the metadata crossed before any block
    /// traffic (bpv1-8, bpv1-14).
    meta_received: bool,
    send_mark: BitSet,
    recv_record: BitSet,
    peer_have: BitSet,
    local_have_sent: BitSet,
    peer_stripe_done: BitSet,
    local_stripe_done_sent: BitSet,
    peer_block_done: bool,
    local_block_done_sent: bool,
    /// Indices waiting for the next plain `Have`.
    pending_have: Vec<u32>,
    pending_have_since: Option<Instant>,
    /// Outside the initial group: the pending batch waits until this
    /// deadline (decision-26). `None` once it has gone out, or in the group.
    have_held_until: Option<Instant>,
    /// Whether the peer is outside the block's initial group.
    have_delayed: bool,
}

impl PeerBlockState {
    fn new(coding: Coding, proposer_node: NodeId, as_proposer: bool) -> Self {
        let total = coding.total_parts().unwrap_or(0) as usize;
        let stripes = coding.stripes as usize;
        Self {
            coding,
            proposer_node,
            as_proposer,
            peer_flagged: BitSet::with_bits(total),
            peer_relayed: BitSet::with_bits(total),
            flags_lost: false,
            local_flagged: BitSet::with_bits(total),
            copies: VecDeque::new(),
            meta_sent: false,
            meta_received: false,
            send_mark: BitSet::with_bits(total),
            recv_record: BitSet::with_bits(total),
            peer_have: BitSet::with_bits(total),
            local_have_sent: BitSet::with_bits(total),
            peer_stripe_done: BitSet::with_bits(stripes),
            local_stripe_done_sent: BitSet::with_bits(stripes),
            peer_block_done: false,
            local_block_done_sent: false,
            pending_have: Vec::new(),
            pending_have_since: None,
            have_held_until: None,
            have_delayed: false,
        }
    }

    /// Set bits of `set` in `stripe`.
    fn stripe_count(&self, set: &BitSet, stripe: u32) -> u32 {
        // Widening u32 to usize is lossless on supported targets.
        let n = self.coding.n as usize;
        let start = stripe as usize * n;
        // A stripe has at most `MAX_N` slots, so the count fits u32.
        (start..start + n).filter(|&i| set.get(i)).count() as u32
    }

    /// The most indices of one stripe that a sender may flag for push on
    /// this connection (bpv1-69): `n` from the block's proposer, whose
    /// seeds carry its proof of work and which flags each index once
    /// (bpv1-25), and `n − k` from anyone else.
    fn push_bound(&self, from: NodeId) -> u32 {
        if from == self.proposer_node {
            self.coding.n
        } else {
            self.relay_bound()
        }
    }

    /// The most indices of one stripe that the proposer may flag for relay
    /// on one connection, and that a holder pushes: `n − k` (bpv1-69).
    fn relay_bound(&self) -> u32 {
        self.coding.n.saturating_sub(self.coding.k)
    }

    /// Whether a part of `stripe` from `remote` may still be a push whose
    /// flag the early record dropped (bpv1-60).
    fn lost_push_open(&self, stripe: u32, remote: NodeId) -> bool {
        self.flags_lost && self.stripe_count(&self.peer_flagged, stripe) < self.push_bound(remote)
    }

    /// Whether the peer may still want `index`: the index is in range, the
    /// peer has not finished its stripe, announced it, or sent it, and this
    /// node has not announced it. With `or_decodable`, a stripe the peer can
    /// already decode also counts as finished.
    fn peer_may_want(&self, index: u32, or_decodable: bool) -> bool {
        let Some((stripe, _)) = self.coding.split_index(index) else {
            return false;
        };
        // Widening u32 to usize is lossless on supported targets.
        let i = index as usize;
        !self.peer_stripe_done.get(stripe as usize)
            && !self.peer_have.get(i)
            && !self.recv_record.get(i)
            && !self.local_have_sent.get(i)
            && !(or_decodable && self.peer_can_decode(stripe))
    }

    /// Records that this node flagged `index` in a `Have` and queued its part.
    fn mark_flagged(&mut self, index: u32) {
        // Widening u32 to usize is lossless on supported targets.
        let i = index as usize;
        self.local_flagged.set(i);
        self.local_have_sent.set(i);
        self.send_mark.set(i);
    }

    /// Whether the peer already holds or has been sent at least `k` distinct parts of `stripe` (opt-7).
    ///
    /// Counts the union of `peer_have` and `send_mark` so an index that appears in
    /// both (due to crossed `Have` and part write) is never double-counted.
    fn peer_can_decode(&self, stripe: u32) -> bool {
        let s = stripe as usize;
        let n = self.coding.n as usize;
        let start = s * n;
        let end = start + n;
        let distinct = (start..end)
            .filter(|&i| self.peer_have.get(i) || self.send_mark.get(i))
            .count() as u32;
        distinct >= self.coding.k
    }
}

/// The peer traffic this session still accepts, and drops, for a block it
/// keeps no state for: a retired block, or one whose metadata it dropped
/// (bpv1-21). Spending past either counter closes with row 15.
#[derive(Copy, Clone, Debug)]
struct RetiredCounters {
    part_remaining: u32,
    have_remaining: u32,
    /// Whether the peer sent the block's `HeaderMeta` on this connection, so
    /// a repeat closes with row 5 while the block stays retired (bpv1-4).
    meta_received: bool,
}

impl RetiredCounters {
    /// Counters for a block whose `HeaderMeta` the peer sent and this
    /// session dropped before it saw any of the block's traffic.
    fn for_dropped_meta(total_parts: u32) -> Self {
        Self {
            part_remaining: total_parts,
            have_remaining: total_parts,
            meta_received: true,
        }
    }

    /// Spends one part. Returns `false` if none remained.
    fn spend_part(&mut self) -> bool {
        let Some(remaining) = self.part_remaining.checked_sub(1) else {
            return false;
        };
        self.part_remaining = remaining;
        true
    }

    /// Spends `count` announced indices. Returns `false` if fewer remained.
    fn spend_have(&mut self, count: usize) -> bool {
        let count = u32::try_from(count).unwrap_or(u32::MAX);
        let Some(remaining) = self.have_remaining.checked_sub(count) else {
            return false;
        };
        self.have_remaining = remaining;
        true
    }
}

#[derive(Clone, Debug)]
struct OutgoingWant {
    block_id: block::Hash,
    stripe: u32,
    requested: WantBitmap,
    /// Requested slots delivered under this `Want`. Its count is the parts
    /// attributed to the `Want`.
    received: WantBitmap,
    /// When this node queued the `Want`, and the first and last attributed
    /// arrivals with their bytes, for the delivery-rate sample (decision-21).
    sent_at: Instant,
    arrived: Option<(Instant, Instant)>,
    arrived_bytes: u64,
    late_at: Instant,
    deadline_at: Instant,
    late_fired: bool,
    deadline_fired: bool,
}

impl OutgoingWant {
    /// Requested slots not yet delivered.
    fn unreceived_slots(&self) -> Vec<u32> {
        self.requested
            .iter()
            .filter(|&slot| !self.received.contains(slot))
            .collect()
    }
}

#[derive(Clone, Debug)]
struct IncomingWant {
    block_id: block::Hash,
    stripe: u32,
    requested: WantBitmap,
}

#[derive(Clone, Debug)]
enum EarlyDataKind {
    Part(RecvPartPrefix),
    Have(RecvHave),
}

impl EarlyDataKind {
    fn block_id(&self) -> block::Hash {
        match self {
            Self::Part(prefix) => prefix.block_id,
            Self::Have(have) => have.block_id,
        }
    }

    /// Bytes charged to the early record: a part's payload, or a `Have`'s
    /// frame or its block id plus decoded indices, whichever is larger. A
    /// bitmap byte decodes to up to eight `u32` indices (decision-33), so the
    /// frame alone undercounts what the record holds.
    fn held_bytes(&self) -> usize {
        match self {
            Self::Part(prefix) => prefix.payload.len(),
            Self::Have(have) => have.frame_bytes.max(
                have.indices
                    .len()
                    .saturating_mul(std::mem::size_of::<u32>())
                    .saturating_add(std::mem::size_of::<block::Hash>()),
            ),
        }
    }
}

#[derive(Clone, Debug)]
struct EarlyDataEntry {
    received_at: Instant,
    kind: EarlyDataKind,
}

#[derive(Clone, Debug)]
enum EarlyControlKind {
    StripeDone(u32),
    BlockDone,
}

#[derive(Clone, Debug)]
struct EarlyControlEntry {
    block_id: block::Hash,
    kind: EarlyControlKind,
}

#[derive(Copy, Clone, Debug)]
struct InflightVerify {
    block_id: block::Hash,
    index: u32,
    auth: PartAuth,
}

/// Pure per-connection BPv1 session machine.
#[derive(Debug)]
pub struct PeerState {
    config: PeerConfig,
    remote_node: NodeId,
    session_gen: u64,
    next_want_id: u32,
    next_attempt: u64,

    blocks: HashMap<block::Hash, PeerBlockState>,
    /// Blocks whose `HeaderMeta` this peer sent before local admission, with
    /// each block's part count from that `HeaderMeta`.
    meta_received_pending: HashMap<block::Hash, u32>,
    /// Blocks whose `HeaderMeta` crossed this connection but whose state this
    /// session no longer keeps: the table rejected the peer's copy, or
    /// evicted the retired block. Their parts and `Have`s drop while the
    /// block's counters last (bpv1-14, bpv1-21). Holds at most
    /// `MAX_PENDING_META_COUNT`, oldest first. An honest peer sends traffic
    /// only for blocks in its propagation window, far fewer.
    meta_dropped: VecDeque<(block::Hash, RetiredCounters)>,
    /// Retired block counters kept while inside `PROPAGATION_DEPTH` (bpv1-21).
    retired: HashMap<block::Hash, RetiredCounters>,

    wants_out: HashMap<u32, OutgoingWant>,
    wants_in: HashMap<u32, IncomingWant>,
    /// `WantEnd`s emitted but not yet written to the data stream, oldest
    /// first, with the `Want` each ends. Each `Want` is still live at the
    /// peer, which cannot have received its end (bpv1-28, bpv1-49).
    unwritten_ends: VecDeque<(u32, IncomingWant)>,
    inflight_verify: HashMap<u64, InflightVerify>,

    early_data: VecDeque<EarlyDataEntry>,
    early_data_bytes: usize,
    early_control: VecDeque<EarlyControlEntry>,
    data_paused: bool,
    /// Unadmitted blocks whose flagged `Have` from this peer the early record
    /// dropped, oldest first. Holds at most `MAX_PENDING_META_COUNT`, like
    /// pending metadata (bpv1-60).
    flags_lost: VecDeque<block::Hash>,

    /// Last instant a `Part` or `WantEnd` arrived while `wants_out` was non-empty.
    last_progress: Option<Instant>,
    /// Whether this connection already stalled once and parked (bpv1-58).
    stalled_once: bool,
    /// When parked on first stall, stays parked until this instant.
    parked_until: Option<Instant>,
    /// How the session closed, if it did.
    closed: Option<SessionClose>,
}

/// How a [`PeerState`] session closed.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum SessionClose {
    /// Protocol violation with its cited close row.
    Protocol(Bpv1CloseCause),
    /// Neutral close on a repeat liveness stall (bpv1-58).
    NeutralStall,
}

impl PeerState {
    /// Creates a clean session machine for one authenticated peer connection.
    pub fn new(config: PeerConfig, remote_node: NodeId, session_gen: u64) -> Self {
        Self {
            config,
            remote_node,
            session_gen,
            next_want_id: 1,
            next_attempt: 1,
            blocks: HashMap::new(),
            meta_received_pending: HashMap::new(),
            meta_dropped: VecDeque::new(),
            retired: HashMap::new(),
            wants_out: HashMap::new(),
            wants_in: HashMap::new(),
            unwritten_ends: VecDeque::new(),
            inflight_verify: HashMap::new(),
            early_data: VecDeque::new(),
            early_data_bytes: 0,
            early_control: VecDeque::new(),
            data_paused: false,
            flags_lost: VecDeque::new(),
            last_progress: None,
            stalled_once: false,
            parked_until: None,
            closed: None,
        }
    }

    /// Session generation identifier.
    pub fn session_gen(&self) -> u64 {
        self.session_gen
    }

    /// Remote peer's authenticated node identity.
    pub fn remote_node(&self) -> NodeId {
        self.remote_node
    }

    /// Whether the session has been closed for a protocol violation or neutral stall.
    pub fn is_closed(&self) -> bool {
        self.closed.is_some()
    }

    /// Cited protocol close cause, if closed for a protocol violation.
    pub fn close_cause(&self) -> Option<Bpv1CloseCause> {
        match self.closed? {
            SessionClose::Protocol(cause) => Some(cause),
            SessionClose::NeutralStall => None,
        }
    }

    /// Whether the session was closed neutrally on a repeat stall.
    pub fn is_closed_neutral(&self) -> bool {
        self.closed == Some(SessionClose::NeutralStall)
    }

    /// Whether the session is currently parked for liveness cooldown.
    pub fn is_parked(&self, now: Instant) -> bool {
        self.parked_until.is_some_and(|until| now < until)
    }

    /// Whether data-stream reads are paused due to a full early record.
    pub fn is_data_paused(&self) -> bool {
        self.data_paused
    }

    /// Parts a new grant may still request from this peer: [`PULL_WINDOW`]
    /// minus the parts outstanding on live outgoing `Want`s.
    pub fn window_remaining(&self) -> u32 {
        PULL_WINDOW.saturating_sub(self.outstanding_parts())
    }

    /// Number of live outgoing `Want` reservations.
    pub fn live_wants_out(&self) -> usize {
        self.wants_out.len()
    }

    /// Number of live incoming `Want` commitments being served.
    pub fn live_wants_in(&self) -> usize {
        self.wants_in.len()
    }

    /// Requested parts not yet received across all live outgoing `Want`s,
    /// including late ones whose wire reservation is still live.
    fn outstanding_parts(&self) -> u32 {
        self.wants_out
            .values()
            .map(|w| u32::from(w.requested.count().saturating_sub(w.received.count())))
            .sum()
    }

    /// Part size `S` of `block_id`, or the profile's `S_MAX` once the block
    /// is no longer tracked.
    pub fn part_bytes_of(&self, block_id: &block::Hash) -> u64 {
        u64::from(
            self.blocks
                .get(block_id)
                .map_or(self.config.profile.s_max, |b| b.coding.part_bytes),
        )
    }

    /// Total outstanding expected bytes across all live outgoing `Want`s
    /// (including late ones whose wire reservation is still live).
    pub fn outstanding_bytes(&self) -> u64 {
        self.wants_out
            .values()
            .map(|w| {
                u64::from(w.requested.count().saturating_sub(w.received.count()))
                    .saturating_mul(self.part_bytes_of(&w.block_id))
            })
            .sum()
    }

    /// Bytes currently held in the per-peer data early record.
    pub fn early_data_bytes(&self) -> usize {
        self.early_data_bytes
    }

    /// Whether any live outgoing `Want` overlaps `requested` on `(block_id, stripe)`.
    fn has_overlapping_want_out(
        &self,
        block_id: &block::Hash,
        stripe: u32,
        requested: &WantBitmap,
    ) -> bool {
        self.wants_out.values().any(|w| {
            w.block_id == *block_id && w.stripe == stripe && w.requested.intersects(requested)
        })
    }

    /// Whether this session is currently eligible to send a new `Want`.
    pub fn can_pull(&self, now: Instant) -> bool {
        !self.is_closed()
            && !self.is_parked(now)
            && (self.wants_out.len() as u32) < MAX_WANTS_INFLIGHT
    }

    /// Current liveness stall timeout for the bytes this session waits for.
    fn liveness_timeout(&self) -> Duration {
        stall_timeout(self.outstanding_bytes())
    }

    /// Earliest future timer deadline for this session (`Tick` wakeup).
    pub fn next_deadline(&self) -> Option<Instant> {
        let mut next: Option<Instant> = None;
        let mut consider = |t: Instant| {
            next = Some(match next {
                Some(cur) => cur.min(t),
                None => t,
            });
        };
        if let Some(until) = self.parked_until {
            consider(until);
        }
        if let Some(front) = self.early_data.front() {
            consider(front.received_at + self.config.early_age);
        }
        for b in self.blocks.values() {
            match (b.have_held_until, b.pending_have_since) {
                (Some(until), _) => consider(until),
                (None, Some(since)) if !b.pending_have.is_empty() => {
                    consider(since + HAVE_FLUSH_INTERVAL)
                }
                _ => {}
            }
        }
        for w in self.wants_out.values() {
            if !w.late_fired {
                consider(w.late_at);
            }
            if !w.deadline_fired {
                consider(w.deadline_at);
            }
        }
        if !self.wants_out.is_empty() {
            if let Some(last) = self.last_progress {
                consider(last + self.liveness_timeout());
            }
        }
        next
    }

    /// Executes one synchronous state transition.
    pub fn step(&mut self, now: Instant, input: PeerInput) -> PeerStep {
        let mut step = PeerStep::default();
        if matches!(input, PeerInput::SessionEnd) {
            self.handle_session_end();
            return step;
        }
        if self.is_closed() {
            return step;
        }

        match input {
            PeerInput::BlockAdmitted(admitted) => {
                self.handle_block_admitted(now, admitted, &mut step);
            }
            PeerInput::RecvHeaderMeta { block_id, message } => {
                self.handle_recv_header_meta(now, block_id, message, &mut step);
            }
            PeerInput::RecvPartPrefix(prefix) => {
                self.handle_recv_part_prefix(now, prefix, false, &mut step);
            }
            PeerInput::PartVerified { token, part } => {
                self.handle_part_verified(token, part, &mut step);
            }
            PeerInput::PartFailed { token } => self.handle_part_failed(token, &mut step),
            PeerInput::PartRefused { token, reason } => {
                self.handle_part_refused(token, reason, &mut step);
            }
            PeerInput::Ready {
                block_id,
                indices,
                push,
            } => {
                // decision-32: the proposer gets a plain `Have` for a pushed seed too.
                let to_proposer = self
                    .blocks
                    .get(&block_id)
                    .is_some_and(|block| self.remote_node == block.proposer_node);
                if push && !to_proposer {
                    self.push_seeds(block_id, indices, &mut step);
                } else {
                    self.handle_ready(now, block_id, indices, &mut step);
                }
            }
            PeerInput::RecvHave(have) => self.handle_recv_have(now, have, &mut step),
            PeerInput::PullGranted(grant) => self.handle_pull_granted(now, grant, &mut step),
            PeerInput::Seeds(seeds) => self.handle_seeds(seeds, &mut step),
            PeerInput::RecvWant(want) => self.handle_recv_want(want, &mut step),
            PeerInput::ServePartQueued(queued) => self.handle_serve_part_queued(queued),
            PeerInput::ServeDone(done) => self.handle_serve_done(done, &mut step),
            // The data writer writes in FIFO order and never drops a `WantEnd`,
            // so the oldest unwritten end is the one written.
            PeerInput::WantEndWritten => {
                self.unwritten_ends.pop_front();
            }
            PeerInput::RecvWantEnd(end) => self.handle_recv_want_end(now, end, &mut step),
            PeerInput::LocalStripeDone { block_id, stripe } => {
                self.handle_local_stripe_done(block_id, stripe, &mut step);
            }
            PeerInput::LocalBlockDone { block_id } => {
                self.handle_local_block_done(block_id, &mut step);
            }
            PeerInput::RecvStripeDone { block_id, stripe } => {
                self.handle_recv_stripe_done(block_id, stripe, &mut step);
            }
            PeerInput::RecvBlockDone { block_id } => {
                self.handle_recv_block_done(block_id, &mut step);
            }
            PeerInput::Retire(retire) => self.handle_retire(retire, &mut step),
            PeerInput::MetaRejected { block_id } => {
                if let Some(total_parts) = self.meta_received_pending.remove(&block_id) {
                    self.note_meta_dropped(
                        block_id,
                        RetiredCounters::for_dropped_meta(total_parts),
                    );
                }
                // Another peer's copy can still admit the block, so its held
                // `StripeDone`s and `BlockDone` stay.
                self.drop_early_data_for_block(block_id, &mut step);
            }
            PeerInput::EvictRetired { block_id } => {
                if let Some(counters) = self.retired.remove(&block_id) {
                    self.note_meta_dropped(block_id, counters);
                }
                if let Some(total_parts) = self.meta_received_pending.remove(&block_id) {
                    // The peer's metadata crossed, so its later data drops.
                    self.note_meta_dropped(
                        block_id,
                        RetiredCounters::for_dropped_meta(total_parts),
                    );
                    self.drop_early_for_block(block_id, &mut step);
                }
                self.flags_lost.retain(|b| *b != block_id);
            }
            PeerInput::Tick => self.handle_tick(now, &mut step),
            PeerInput::DataIdle => self.handle_data_idle(&mut step),
            PeerInput::ControlStalled => {
                // A peer that never reads control gets the repeat-stall close:
                // nothing it sent is a protocol violation.
                self.closed = Some(SessionClose::NeutralStall);
                step.outputs.push(PeerOutput::CloseNeutralStall);
            }
            PeerInput::SessionEnd => unreachable!("handled above"),
        }
        step
    }

    /// Queues the `WantEnd` of the peer's `Want` `want_id`. The `Want` stays
    /// live until the data writer writes its end.
    fn end_want(
        &mut self,
        want_id: u32,
        want: IncomingWant,
        sent: u16,
        reason: WantEndReason,
        step: &mut PeerStep,
    ) {
        self.unwritten_ends.push_back((want_id, want));
        step.outputs.push(PeerOutput::SendWantEnd {
            want_id,
            sent,
            reason,
        });
    }

    /// The peer's live `Want`s: those being served and those whose end is
    /// not yet written.
    fn live_wants_in_iter(&self) -> impl Iterator<Item = (u32, &IncomingWant)> {
        self.wants_in
            .iter()
            .map(|(&id, want)| (id, want))
            .chain(self.unwritten_ends.iter().map(|(id, want)| (*id, want)))
    }

    /// Closes for `row`, read on the stream of `stream_kind`.
    fn close_protocol(&mut self, stream_kind: u16, row: Bpv1CloseRow, step: &mut PeerStep) {
        if self.closed.is_some() {
            return;
        }
        let cause = row.cause(stream_kind);
        self.closed = Some(SessionClose::Protocol(cause));
        step.outputs.push(PeerOutput::CloseProtocol(cause));
    }

    fn handle_block_admitted(
        &mut self,
        now: Instant,
        admitted: BlockAdmitted,
        step: &mut PeerStep,
    ) {
        let BlockAdmitted {
            block_id,
            coding,
            proposer_node,
            header_meta,
            as_proposer,
            have_held_until,
        } = admitted;
        if self.retired.contains_key(&block_id) {
            return;
        }
        let received = self.meta_received_pending.remove(&block_id).is_some();
        self.meta_dropped.retain(|(b, _)| *b != block_id);
        let flags_lost = self.take_flags_lost(block_id);
        let first = !self.blocks.contains_key(&block_id);
        let state = self
            .blocks
            .entry(block_id)
            .or_insert_with(|| PeerBlockState::new(coding, proposer_node, as_proposer));
        // decision-26: the first admission fixes the pacing; a repeat changes
        // nothing.
        if first {
            state.have_held_until = have_held_until;
            state.have_delayed = have_held_until.is_some();
        }
        state.flags_lost |= flags_lost;
        if received {
            state.meta_received = true;
        }

        // bpv1-4: send HeaderMeta at most once per connection, and never back
        // on a connection that sent it. The session's own record decides,
        // because a peer's later session never received this copy.
        if !state.meta_received && !state.meta_sent {
            state.meta_sent = true;
            step.outputs.push(PeerOutput::SendHeaderMeta(header_meta));
        }

        // bpv1-9: proposer queues BlockDone right after HeaderMeta so no peer
        // pushes or announces parts back.
        if as_proposer && !state.local_block_done_sent {
            state.local_block_done_sent = true;
            step.outputs
                .push(PeerOutput::SendControl(Box::new(Bpv1Message::BlockDone {
                    block_id,
                })));
        }

        // Replay held control and data early entries for this block.
        self.replay_early_for_block(now, block_id, step);
    }

    fn handle_recv_header_meta(
        &mut self,
        now: Instant,
        block_id: block::Hash,
        message: Box<Bpv1Message>,
        step: &mut PeerStep,
    ) {
        // bpv1-4 / close-5: a second HeaderMeta for one block on one connection
        // closes 5 while this node keeps the block pending, admitted, or retired.
        let duplicate = self.meta_received_pending.contains_key(&block_id)
            || self.blocks.get(&block_id).is_some_and(|b| b.meta_received)
            || self.retired.get(&block_id).is_some_and(|r| r.meta_received);
        if duplicate {
            self.close_protocol(BPV1_DATA_KIND, Bpv1CloseRow::Row5DuplicateHeaderMeta, step);
            return;
        }
        // A dropped mark stays until admission, so a repeated `HeaderMeta`
        // cannot refill the counters its rejection leaves.
        if let Some(block) = self.blocks.get_mut(&block_id) {
            block.meta_received = true;
            // Parts that waited for this `HeaderMeta` (bpv1-14) can now be accepted.
            self.replay_early_for_block(now, block_id, step);
        } else if let Some(retired) = self.retired.get_mut(&block_id) {
            retired.meta_received = true;
        } else {
            let total_parts = match &*message {
                Bpv1Message::HeaderMeta { coding, .. } => coding.total_parts().unwrap_or(0),
                _ => 0,
            };
            self.meta_received_pending.insert(block_id, total_parts);
        }
        step.events.push(PeerEvent::MetaReceived(block_id));
        if !self.retired.contains_key(&block_id) {
            step.outputs
                .push(PeerOutput::SubmitHeaderMeta { block_id, message });
        }
    }

    fn handle_recv_part_prefix(
        &mut self,
        now: Instant,
        prefix: RecvPartPrefix,
        is_replay: bool,
        step: &mut PeerStep,
    ) {
        let block_id = prefix.block_id;
        let index = prefix.index;
        // 1. Retired block -> spend retired part counter without allocating/verifying (bpv1-21).
        if let Some(retired) = self.retired.get_mut(&block_id) {
            if !retired.spend_part() {
                self.close_protocol(
                    BPV1_DATA_KIND,
                    Bpv1CloseRow::Row15RetiredCounterExhausted,
                    step,
                );
            } else {
                step.events.push(PeerEvent::PartDropped {
                    block_id,
                    index,
                    reason: DropReason::RetiredLate,
                });
            }
            return;
        }

        // 2. Unadmitted block -> hold, drop, or close 21 (bpv1-14).
        let Some(block) = self.blocks.get(&block_id) else {
            self.recv_unadmitted(now, EarlyDataKind::Part(prefix), step);
            return;
        };

        // 3. Range and exact-length check -> close 13 (bpv1-66). The frame
        //    length fixes the payload length before any payload byte is read.
        let Some((stripe, slot)) = block.coding.split_index(index) else {
            self.close_protocol(BPV1_DATA_KIND, Bpv1CloseRow::Row13BadPartIndexOrProof, step);
            return;
        };
        if part_payload_len(&block.coding, index) != Some(prefix.payload.len()) {
            self.close_protocol(BPV1_DATA_KIND, Bpv1CloseRow::Row13BadPartIndexOrProof, step);
            return;
        }

        // 4. Receive-once check -> close 14 (bpv1-18).
        if block.recv_record.get(index as usize) {
            self.close_protocol(
                BPV1_DATA_KIND,
                Bpv1CloseRow::Row14UnwantedOrDuplicatePart,
                step,
            );
            return;
        }

        // 5. Attribution: a push slot first (bpv1-69), a seed when the
        //    proposer opened it (bpv1-16), then live Want (bpv1-13).
        // Widening u32 to usize is lossless on supported targets.
        let flagged = block.peer_flagged.get(index as usize);
        // A part whose flag the early record dropped opens its push slot on
        // arrival (bpv1-60). No `Want` names such a stripe, so the part
        // cannot be a `Want`'s.
        let lost_push = !flagged && block.lost_push_open(stripe, self.remote_node);
        let auth = if flagged || lost_push {
            Some(if self.remote_node == block.proposer_node {
                PartAuth::Seed
            } else {
                PartAuth::Push
            })
        } else {
            self.wants_out
                .iter()
                .find(|(_, w)| {
                    w.block_id == block_id
                        && w.stripe == stripe
                        && w.requested.contains(slot)
                        && !w.received.contains(slot)
                })
                .map(|(&want_id, _)| PartAuth::Want(want_id))
        };

        let Some(auth) = auth else {
            if is_replay {
                // Early replay whose authorization disappeared returns Drop, never close.
                step.events.push(PeerEvent::PartDropped {
                    block_id,
                    index,
                    reason: DropReason::Unwanted,
                });
            } else {
                self.close_protocol(
                    BPV1_DATA_KIND,
                    Bpv1CloseRow::Row14UnwantedOrDuplicatePart,
                    step,
                );
            }
            return;
        };

        // Record receipt & update Want attribution.
        let block = self.blocks.get_mut(&block_id).expect("block checked above");
        block.recv_record.set(index as usize);
        if lost_push {
            block.peer_flagged.set(index as usize);
        }
        // The peer sent a part this node pushes to it: skip the write if it
        // is still queued (03 §5.4).
        if block.local_flagged.get(index as usize) {
            step.outputs
                .push(PeerOutput::CancelQueuedPush { block_id, index });
        }
        let after_stop =
            block.local_block_done_sent || block.local_stripe_done_sent.get(stripe as usize);

        if let PartAuth::Want(want_id) = auth {
            if let Some(want) = self.wants_out.get_mut(&want_id) {
                want.received.set(slot);
                let first = want.arrived.map_or(now, |(first, _)| first);
                want.arrived = Some((first, now));
                // Widening usize to u64 is lossless on supported targets.
                want.arrived_bytes = want
                    .arrived_bytes
                    .saturating_add(prefix.payload.len() as u64);
            }
            // A part delivered under a live `Want` counts as progress for
            // liveness (bpv1-58); a pushed part does not.
            if !self.wants_out.is_empty() {
                self.last_progress = Some(now);
            }
        }

        step.events.push(PeerEvent::PartPrefix {
            block_id,
            index,
            auth,
        });

        // bpv1-20: authorized part arriving after our StripeDone/BlockDone is
        // dropped as surplus without verify work and never closes.
        if after_stop {
            step.events.push(PeerEvent::PartDropped {
                block_id,
                index,
                reason: DropReason::AfterStop,
            });
            return;
        }

        let attempt = self.next_attempt;
        self.next_attempt = self.next_attempt.wrapping_add(1);
        let token = PartToken {
            session_gen: self.session_gen,
            block_id,
            index,
            attempt,
        };
        self.inflight_verify.insert(
            attempt,
            InflightVerify {
                block_id,
                index,
                auth,
            },
        );
        step.outputs.push(PeerOutput::VerifyPart {
            token,
            payload: prefix.payload,
        });
    }

    /// Removes `token`'s in-flight verification if `token` is from this
    /// session and names the part that verification started for.
    fn take_inflight(&mut self, token: &PartToken) -> Option<InflightVerify> {
        if token.session_gen != self.session_gen {
            return None;
        }
        self.inflight_verify
            .remove(&token.attempt)
            .filter(|inflight| inflight.block_id == token.block_id && inflight.index == token.index)
    }

    fn handle_part_verified(&mut self, token: PartToken, part: VerifiedPart, step: &mut PeerStep) {
        let Some(inflight) = self.take_inflight(&token) else {
            return;
        };
        if part.index() != token.index || !self.blocks.contains_key(&token.block_id) {
            return;
        }
        step.outputs.push(PeerOutput::InsertPart {
            block_id: token.block_id,
            part,
            session_gen: token.session_gen,
            auth: inflight.auth,
        });
    }

    fn handle_part_failed(&mut self, token: PartToken, step: &mut PeerStep) {
        // A stale failure after retirement is a no-op (`bpv1-63`).
        if self.take_inflight(&token).is_none() || !self.blocks.contains_key(&token.block_id) {
            return;
        }
        // bpv1-12 / bpv1-22 / close-13: invalid proof closes only this sender.
        self.close_protocol(BPV1_DATA_KIND, Bpv1CloseRow::Row13BadPartIndexOrProof, step);
    }

    fn handle_part_refused(&mut self, token: PartToken, reason: DropReason, step: &mut PeerStep) {
        if self.take_inflight(&token).is_some() {
            step.events.push(PeerEvent::PartDropped {
                block_id: token.block_id,
                index: token.index,
                reason,
            });
        }
    }

    fn handle_ready(
        &mut self,
        now: Instant,
        block_id: block::Hash,
        indices: Vec<u32>,
        step: &mut PeerStep,
    ) {
        let Some(block) = self.blocks.get_mut(&block_id) else {
            return;
        };
        // bpv1-9 / decision-20: the proposer never announces its own block.
        if block.as_proposer {
            return;
        }
        // decision-32: the proposer hears of every part this node verifies,
        // except the parts it sent here, at once and outside decision-26's
        // pacing. The proposer holds every part, so it never asks for one.
        if self.remote_node == block.proposer_node {
            let announced: Vec<u32> = indices
                .into_iter()
                .filter(|&index| {
                    // Widening u32 to usize is lossless on supported targets.
                    let i = index as usize;
                    !block.recv_record.get(i) && !block.local_have_sent.get(i)
                })
                .collect();
            for &index in &announced {
                block.local_have_sent.set(index as usize);
            }
            for chunk in announced.chunks(W2_MAX_HAVE_INDICES) {
                let mut indices = chunk.to_vec();
                indices.sort_unstable();
                step.events.push(PeerEvent::HaveSentToProposer {
                    block_id,
                    count: indices.len(),
                });
                step.outputs.push(send_have(
                    block_id,
                    &block.coding,
                    HaveFlags::Plain,
                    indices,
                ));
            }
            return;
        }
        if block.peer_block_done {
            return;
        }

        for index in indices {
            if !block.peer_may_want(index, true) {
                continue;
            }
            if !block.pending_have.contains(&index) {
                block.pending_have.push(index);
                block.pending_have_since.get_or_insert(now);
            }
        }

        // bpv1-73: outside the initial group, the batch waits for the deadline.
        match block.have_held_until {
            Some(until) if now >= until => self.release_held_haves(block_id, step),
            None if block.pending_have.len() >= HAVE_FLUSH_COUNT => {
                self.flush_block_have(block_id, step)
            }
            _ => {}
        }
    }

    /// At the advertisement deadline, sends the held indices that the skip
    /// rules still allow in one batch (decision-26, bpv1-73).
    fn release_held_haves(&mut self, block_id: block::Hash, step: &mut PeerStep) {
        let Some(block) = self.blocks.get_mut(&block_id) else {
            return;
        };
        block.have_held_until = None;
        let held_count = block.pending_have.len();
        if held_count == 0 {
            return;
        }
        let before = block.local_have_sent.count_ones();
        self.flush_block_have(block_id, step);
        let Some(block) = self.blocks.get(&block_id) else {
            return;
        };
        // The flush announced only held indices, so the rest were skipped.
        // Widening u32 to usize is lossless on supported targets.
        let sent = (block.local_have_sent.count_ones() - before) as usize;
        if sent < held_count {
            step.events.push(PeerEvent::HaveSkippedAtDeadline {
                block_id,
                count: held_count - sent,
            });
        }
    }

    /// Pushes this node's verified pushed seeds `indices` to the peer
    /// (decision-24, bpv1-71). One flagged `Have` per `W2_MAX_HAVE_INDICES`
    /// goes first, then each of its parts behind it on the data stream
    /// (bpv1-72). The proposer, a peer that
    /// announced or sent a part, and a peer that finished its stripe or the
    /// block get nothing. The flagged `Have` is the only `Have` for those
    /// indices on this connection.
    fn push_seeds(&mut self, block_id: block::Hash, indices: Vec<u32>, step: &mut PeerStep) {
        let Some(block) = self.blocks.get_mut(&block_id) else {
            return;
        };
        if block.peer_block_done || self.remote_node == block.proposer_node || block.as_proposer {
            return;
        }
        let bound = block.coding.n.saturating_sub(block.coding.k);
        let mut flagged: Vec<u32> = Vec::new();
        for index in indices {
            // Widening u32 to usize is lossless on supported targets.
            if !block.peer_may_want(index, true)
                || block.send_mark.get(index as usize)
                || block.stripe_count(&block.local_flagged, index / block.coding.n) >= bound
            {
                continue;
            }
            block.mark_flagged(index);
            flagged.push(index);
        }
        if flagged.is_empty() {
            return;
        }
        // The flagged `Have` is the only `Have` for these indices (bpv1-72).
        block
            .pending_have
            .retain(|&i| !block.local_flagged.get(i as usize));
        flagged.sort_unstable();
        // A `Have` lists at most `W2_MAX_HAVE_INDICES`, so each chunk goes
        // out as its own flagged `Have` ahead of its parts.
        for chunk in flagged.chunks(W2_MAX_HAVE_INDICES) {
            step.outputs.push(send_have(
                block_id,
                &block.coding,
                HaveFlags::Push,
                chunk.to_vec(),
            ));
            for &index in chunk {
                step.outputs.push(PeerOutput::PushPart { block_id, index });
            }
        }
    }

    fn flush_block_have(&mut self, block_id: block::Hash, step: &mut PeerStep) {
        let Some(block) = self.blocks.get_mut(&block_id) else {
            return;
        };
        if block.pending_have.is_empty() {
            return;
        }
        let mut batch = std::mem::take(&mut block.pending_have);
        block.pending_have_since = None;
        batch.sort_unstable();
        batch.dedup();
        // Rerun the skip rules of `handle_ready`: the peer may have announced,
        // sent, or finished meanwhile (bpv1-73).
        batch.retain(|&idx| !block.peer_block_done && block.peer_may_want(idx, true));
        for &idx in &batch {
            block.local_have_sent.set(idx as usize);
        }
        let delayed = block.have_delayed;
        for chunk in batch.chunks(W2_MAX_HAVE_INDICES) {
            if !chunk.is_empty() {
                step.events.push(PeerEvent::HaveSent {
                    block_id,
                    count: chunk.len(),
                    delayed,
                });
                step.outputs.push(send_have(
                    block_id,
                    &block.coding,
                    HaveFlags::Plain,
                    chunk.to_vec(),
                ));
            }
        }
    }

    fn handle_recv_have(&mut self, now: Instant, have: RecvHave, step: &mut PeerStep) {
        let block_id = have.block_id;
        if let Some(retired) = self.retired.get_mut(&block_id) {
            if !retired.spend_have(have.indices.len()) {
                self.close_protocol(
                    BPV1_DATA_KIND,
                    Bpv1CloseRow::Row15RetiredCounterExhausted,
                    step,
                );
            }
            return;
        }

        let Some(block) = self.blocks.get_mut(&block_id) else {
            self.recv_unadmitted(now, EarlyDataKind::Have(have), step);
            return;
        };
        let indices = have.indices;

        let total = block.coding.total_parts().unwrap_or(0);
        // Validate the form and every index before mutating peer_have
        // (bpv1-25, close-16). A bitmap sized for another geometry, or a
        // padding bit, which names an index at or past `total`, closes. The
        // decoder already rejected indices that are not strictly increasing.
        if !have.form.fits(total)
            || indices
                .iter()
                .any(|&idx| idx >= total || block.peer_have.get(idx as usize))
        {
            self.close_protocol(BPV1_DATA_KIND, Bpv1CloseRow::Row16BadHaveIndex, step);
            return;
        }
        // bpv1-69 / close-20: per stripe, a peer flags at most its push
        // bound, and only the proposer flags relay, on at most `n − k`.
        let flags = have.flags;
        if flags.push() {
            let from_proposer = self.remote_node == block.proposer_node;
            let push_bound = block.push_bound(self.remote_node);
            let relay_bound = block.relay_bound();
            let over = (0..block.coding.stripes).any(|stripe| {
                let added = indices
                    .iter()
                    .filter(|&&idx| block.coding.split_index(idx).map(|(s, _)| s) == Some(stripe))
                    .count();
                // A `Have` lists at most 4,096 indices, so the count fits u32.
                let added = added as u32;
                block.stripe_count(&block.peer_flagged, stripe) + added > push_bound
                    || (flags.relay()
                        && block.stripe_count(&block.peer_relayed, stripe) + added > relay_bound)
            });
            if over || (flags.relay() && !from_proposer) {
                self.close_protocol(BPV1_DATA_KIND, Bpv1CloseRow::Row20BadPushFlags, step);
                return;
            }
        }
        for &idx in &indices {
            block.peer_have.set(idx as usize);
            if flags.push() {
                block.peer_flagged.set(idx as usize);
            }
            if flags.relay() {
                block.peer_relayed.set(idx as usize);
            }
            // The peer holds a part this node pushes to it: skip the write
            // if it is still queued (03 §5.4).
            if block.local_flagged.get(idx as usize) {
                step.outputs.push(PeerOutput::CancelQueuedPush {
                    block_id,
                    index: idx,
                });
            }
        }
        if indices.is_empty() {
            return;
        }
        if flags.push() {
            step.events.push(PeerEvent::PushFlagged {
                block_id,
                count: indices.len(),
            });
            step.outputs.push(PeerOutput::PushFlagged {
                block_id,
                indices,
                relay: flags.relay(),
            });
        } else if block.as_proposer {
            step.events
                .push(PeerEvent::ProposerHeardHave { block_id, indices });
        } else {
            step.outputs.push(PeerOutput::PeerHas { block_id, indices });
        }
    }

    fn handle_pull_granted(&mut self, now: Instant, grant: PullGranted, step: &mut PeerStep) {
        let PullGranted {
            block_id,
            stripe,
            requested,
            arrival,
        } = grant;
        let reject_with = |step: &mut PeerStep, reason: ReleaseReason| {
            let slots: Vec<u32> = requested.iter().collect();
            if !slots.is_empty() {
                step.outputs.push(PeerOutput::ReleaseClaims {
                    block_id,
                    stripe,
                    slots,
                    reason,
                });
            }
        };
        let reject = |step: &mut PeerStep| reject_with(step, ReleaseReason::Refused);
        if self.is_parked(now) || requested.is_empty() {
            reject(step);
            return;
        }
        // `PULL_WINDOW` bounds the parts outstanding from this peer.
        if u32::from(requested.count()) > self.window_remaining()
            || self.wants_out.len() >= MAX_WANTS_INFLIGHT as usize
        {
            reject(step);
            return;
        }
        let Some(block) = self.blocks.get(&block_id) else {
            reject(step);
            return;
        };
        if stripe >= block.coding.stripes || !requested.all_below(block.coding.n) {
            reject(step);
            return;
        }
        // bpv1-69: never ask the peer for a part it is pushing, or for a
        // stripe where a push whose flag the early record dropped may still
        // come (bpv1-60). A failed grant marks the peer tried for those slots.
        let pushing = block.lost_push_open(stripe, self.remote_node)
            || requested.iter().any(|slot| {
                block.coding.wire_index(stripe, slot).is_some_and(|index| {
                    block.peer_flagged.get(index as usize) && !block.recv_record.get(index as usize)
                })
            });
        if pushing {
            reject_with(step, ReleaseReason::Failed);
            return;
        }
        let part_bytes = u64::from(block.coding.part_bytes);
        // bpv1-28: live Wants on one connection must have disjoint requested sets per (block, stripe).
        if self.has_overlapping_want_out(&block_id, stripe, &requested) {
            // The peer already owes an overlapping slot on a live `Want`.
            reject_with(step, ReleaseReason::Failed);
            return;
        }

        let want_id = self.alloc_want_id();
        let parts = requested.count();
        let expected_bytes = u64::from(parts).saturating_mul(part_bytes);
        let total_outstanding = self.outstanding_bytes().saturating_add(expected_bytes);

        // decision-3: deadline = 8s + outstanding bytes / 100 KB/s.
        let transfer_ms = total_outstanding
            .saturating_mul(1000)
            .div_ceil(REQUEST_MIN_RATE_BPS);
        let deadline_dur = REQUEST_BASE_TIMEOUT.saturating_add(Duration::from_millis(transfer_ms));
        let deadline_at = now + deadline_dur;

        // decision-12: late = SLOW_X * estimated_time (clamped below by 1 ms).
        let est_dur = arrival
            .at
            .saturating_duration_since(now)
            .max(Duration::from_millis(1));
        let late_at = now + est_dur.saturating_mul(SLOW_X);

        if self.wants_out.is_empty() {
            self.last_progress = Some(now);
        }

        // bpv1-27: create reservation BEFORE emitting the Want control write.
        self.wants_out.insert(
            want_id,
            OutgoingWant {
                block_id,
                stripe,
                requested,
                received: WantBitmap::EMPTY,
                sent_at: now,
                arrived: None,
                arrived_bytes: 0,
                late_at,
                deadline_at,
                late_fired: false,
                deadline_fired: false,
            },
        );

        step.outputs
            .push(PeerOutput::SendControl(Box::new(Bpv1Message::Want {
                want_id,
                block_id,
                stripe,
                requested,
            })));
    }

    /// Queues the proposer's `seeds` of its own block to the peer
    /// (decision-19, decision-24). The relay seeds go first: one flagged
    /// `Have` with push and relay per `W2_MAX_HAVE_INDICES`, then its parts.
    /// The other seeds follow the same way with push only. A seed the peer
    /// announced, sent, or no longer needs, or one already flagged or
    /// announced on this connection, is skipped, so no index is announced
    /// twice (bpv1-25).
    fn handle_seeds(&mut self, seeds: Seeds, step: &mut PeerStep) {
        let Seeds {
            block_id,
            seeds,
            copies,
        } = seeds;
        let Some(block) = self.blocks.get_mut(&block_id) else {
            return;
        };
        // bpv1-9: only the proposer seeds.
        if block.peer_block_done || !block.as_proposer {
            return;
        }
        let mut relay: Vec<VerifiedPart> = Vec::new();
        let mut other: Vec<VerifiedPart> = Vec::new();
        for seed in seeds {
            let index = seed.part.index();
            // bpv1-17 / bpv1-25: each index is flagged and sent once.
            // Widening u32 to usize is lossless on supported targets.
            if !block.peer_may_want(index, false) || block.send_mark.get(index as usize) {
                continue;
            }
            block.mark_flagged(index);
            if seed.relay {
                relay.push(seed.part);
            } else {
                other.push(seed.part);
            }
        }
        for (flags, parts) in [(HaveFlags::Relay, relay), (HaveFlags::Push, other)] {
            for chunk in parts.chunks(W2_MAX_HAVE_INDICES) {
                let mut indices: Vec<u32> = chunk.iter().map(VerifiedPart::index).collect();
                indices.sort_unstable();
                step.outputs
                    .push(send_have(block_id, &block.coding, flags, indices));
                for part in chunk {
                    step.outputs.push(PeerOutput::SendSeedPart {
                        block_id,
                        part: part.clone(),
                    });
                }
            }
        }
        block.copies.extend(copies);
    }

    /// Whether direct copies wait for an idle data writer
    /// ([`PeerInput::DataIdle`]).
    pub fn has_copies(&self) -> bool {
        self.blocks.values().any(|block| !block.copies.is_empty())
    }

    /// Releases up to [`COPY_BATCH`] direct copies of one block behind one
    /// push-flagged `Have`. A copy the peer no longer needs drops here: it
    /// announced, sent, or was sent the part, finished its stripe or the
    /// block, or holds or was sent `k` parts of the stripe (decision-19).
    fn handle_data_idle(&mut self, step: &mut PeerStep) {
        let Some((&block_id, block)) = self
            .blocks
            .iter_mut()
            .filter(|(_, block)| !block.copies.is_empty())
            .min_by_key(|(block_id, _)| block_id.0)
        else {
            return;
        };
        let mut batch: Vec<VerifiedPart> = Vec::new();
        while batch.len() < COPY_BATCH {
            let Some(part) = block.copies.pop_front() else {
                break;
            };
            let index = part.index();
            // Widening u32 to usize is lossless on supported targets.
            if block.peer_block_done
                || !block.peer_may_want(index, true)
                || block.send_mark.get(index as usize)
            {
                continue;
            }
            block.mark_flagged(index);
            batch.push(part);
        }
        if batch.is_empty() {
            return;
        }
        let mut indices: Vec<u32> = batch.iter().map(VerifiedPart::index).collect();
        indices.sort_unstable();
        step.events.push(PeerEvent::CopiesQueued {
            block_id,
            count: batch.len(),
            bytes: batch
                .iter()
                // Widening usize to u64 is lossless on supported targets.
                .map(|part| part.wire_payload().len() as u64)
                .sum(),
        });
        step.outputs
            .push(send_have(block_id, &block.coding, HaveFlags::Push, indices));
        for part in batch {
            step.outputs
                .push(PeerOutput::SendSeedPart { block_id, part });
        }
    }

    fn handle_recv_want(&mut self, want: RecvWant, step: &mut PeerStep) {
        let RecvWant {
            want_id,
            block_id,
            stripe,
            requested,
        } = want;
        // The decoder already rejected a zero `want_id` and an empty bitmap.
        // bpv1-28 / close-9: reused live want_id. A `Want` whose end this
        // node has not yet written is still live at the peer.
        if self.live_wants_in_iter().any(|(id, _)| id == want_id) {
            self.close_protocol(
                BPV1_CONTROL_KIND,
                Bpv1CloseRow::Row9WantReservationViolation,
                step,
            );
            return;
        }
        // bpv1-49 / close-9: close only above 2 × MAX_WANTS_INFLIGHT. An
        // unwritten `WantEnd` still holds its `Want`'s place, so a peer that
        // stops reading data cannot grow this node's data queue.
        let hard_limit = MAX_WANTS_INFLIGHT.saturating_mul(2) as usize;
        if self.wants_in.len() + self.unwritten_ends.len() >= hard_limit {
            self.close_protocol(
                BPV1_CONTROL_KIND,
                Bpv1CloseRow::Row9WantReservationViolation,
                step,
            );
            return;
        }
        // bpv1-28 / close-9: overlapping live requested set for same (block, stripe).
        let overlaps = self.live_wants_in_iter().any(|(_, w)| {
            w.block_id == block_id && w.stripe == stripe && w.requested.intersects(&requested)
        });
        if overlaps {
            self.close_protocol(
                BPV1_CONTROL_KIND,
                Bpv1CloseRow::Row9WantReservationViolation,
                step,
            );
            return;
        }

        let want = IncomingWant {
            block_id,
            stripe,
            requested,
        };
        // Reservation messages never enter early record: unknown or retired blocks
        // end immediately with sent = 0 (bpv1-29).
        if self.retired.contains_key(&block_id) {
            self.end_want(want_id, want, 0, WantEndReason::Retired, step);
            return;
        }
        let Some(block) = self.blocks.get(&block_id) else {
            self.end_want(want_id, want, 0, WantEndReason::UnknownBlock, step);
            return;
        };

        // close-8: stripe or requested bit out of range for admitted layout.
        if stripe >= block.coding.stripes || !requested.all_below(block.coding.n) {
            self.close_protocol(BPV1_CONTROL_KIND, Bpv1CloseRow::Row8BadWantRange, step);
            return;
        }

        // If the peer already sent BlockDone/StripeDone, serve 0 parts and
        // end cleanly.
        if block.peer_block_done || block.peer_stripe_done.get(stripe as usize) {
            self.end_want(want_id, want, 0, WantEndReason::Served, step);
            return;
        }

        // Filter requested slots by send-once, never-send-back, and source-before-parity order.
        let mut eligible_slots = Vec::new();
        for slot in requested.iter() {
            let Some(idx) = block.coding.wire_index(stripe, slot) else {
                continue;
            };
            if block.send_mark.get(idx as usize)
                || block.recv_record.get(idx as usize)
                || block.peer_have.get(idx as usize)
            {
                continue;
            }
            eligible_slots.push(slot);
        }

        self.wants_in.insert(want_id, want);
        step.outputs.push(PeerOutput::ServeWant {
            want_id,
            block_id,
            stripe,
            eligible_slots,
        });
    }

    fn handle_serve_part_queued(&mut self, queued: ServePartQueued) {
        let ServePartQueued {
            want_id,
            block_id,
            index,
        } = queued;
        let Some(want) = self.wants_in.get(&want_id) else {
            return;
        };
        if want.block_id != block_id {
            return;
        }
        if let Some(block) = self.blocks.get_mut(&block_id) {
            if block.coding.split_index(index).is_some() {
                block.send_mark.set(index as usize);
            }
        }
    }

    fn handle_serve_done(&mut self, done: ServeDone, step: &mut PeerStep) {
        let ServeDone {
            want_id,
            sent,
            reason,
        } = done;
        let Some(want) = self.wants_in.remove(&want_id) else {
            return;
        };
        let capped_sent = sent.min(want.requested.count());
        self.end_want(want_id, want, capped_sent, reason, step);
    }

    fn handle_recv_want_end(&mut self, now: Instant, end: RecvWantEnd, step: &mut PeerStep) {
        let RecvWantEnd { want_id, sent } = end;
        // bpv1-29 / bpv1-30 / close-18: must match live Want, sent <= popcount, sent >= attributed.
        let Some(want) = self.wants_out.remove(&want_id) else {
            self.close_protocol(BPV1_DATA_KIND, Bpv1CloseRow::Row18BadWantEnd, step);
            return;
        };
        let popcount = want.requested.count();
        let attributed = want.received.count();
        if sent > popcount || sent < attributed {
            self.close_protocol(BPV1_DATA_KIND, Bpv1CloseRow::Row18BadWantEnd, step);
            return;
        }

        if self.wants_out.is_empty() {
            self.last_progress = None;
        } else {
            self.last_progress = Some(now);
        }

        let unreceived_slots = want.unreceived_slots();

        step.events.push(PeerEvent::WantEnded {
            want_id,
            sent,
            attributed,
        });
        // decision-21: a `Want` that received parts yields one rate sample,
        // ahead of the re-plan its end triggers.
        if let Some((first_at, last_at)) = want.arrived {
            step.outputs.push(PeerOutput::RateSample(RateSample {
                bytes: want.arrived_bytes,
                sent_at: want.sent_at,
                first_at,
                last_at,
            }));
        }
        step.outputs.push(PeerOutput::WantCompleted {
            want_id,
            block_id: want.block_id,
            stripe: want.stripe,
            unreceived_slots,
        });
    }

    fn handle_local_stripe_done(
        &mut self,
        block_id: block::Hash,
        stripe: u32,
        step: &mut PeerStep,
    ) {
        let Some(block) = self.blocks.get_mut(&block_id) else {
            return;
        };
        if stripe >= block.coding.stripes {
            return;
        }
        // bpv1-39: announce StripeDone at most once per (block, stripe) per connection.
        if block.local_stripe_done_sent.set(stripe as usize) {
            step.outputs
                .push(PeerOutput::SendControl(Box::new(Bpv1Message::StripeDone {
                    block_id,
                    stripe,
                })));
        }
    }

    fn handle_local_block_done(&mut self, block_id: block::Hash, step: &mut PeerStep) {
        let Some(block) = self.blocks.get_mut(&block_id) else {
            return;
        };
        // bpv1-39: announce BlockDone at most once per block per connection.
        if !block.local_block_done_sent {
            block.local_block_done_sent = true;
            step.outputs
                .push(PeerOutput::SendControl(Box::new(Bpv1Message::BlockDone {
                    block_id,
                })));
        }
    }

    fn handle_recv_stripe_done(&mut self, block_id: block::Hash, stripe: u32, step: &mut PeerStep) {
        if self.retired.contains_key(&block_id) {
            return;
        }
        let Some(block) = self.blocks.get_mut(&block_id) else {
            self.buffer_early_control(block_id, EarlyControlKind::StripeDone(stripe));
            return;
        };
        // close-10: out-of-range stripe or repeat.
        if stripe >= block.coding.stripes || !block.peer_stripe_done.set(stripe as usize) {
            self.close_protocol(BPV1_CONTROL_KIND, Bpv1CloseRow::Row10BadStripeDone, step);
            return;
        }
        // bpv1-38: cancel queued unsent parts of stripe; send marks stay set.
        step.outputs
            .push(PeerOutput::CancelQueuedStripe { block_id, stripe });
    }

    fn handle_recv_block_done(&mut self, block_id: block::Hash, step: &mut PeerStep) {
        if self.retired.contains_key(&block_id) {
            return;
        }
        let Some(block) = self.blocks.get_mut(&block_id) else {
            self.buffer_early_control(block_id, EarlyControlKind::BlockDone);
            return;
        };
        // close-11: repeat BlockDone.
        if block.peer_block_done {
            self.close_protocol(
                BPV1_CONTROL_KIND,
                Bpv1CloseRow::Row11DuplicateBlockDone,
                step,
            );
            return;
        }
        block.peer_block_done = true;
        // A held batch stays, so the deadline counts it as skipped.
        if block.have_held_until.is_none() {
            block.pending_have.clear();
            block.pending_have_since = None;
        }
        block.copies.clear();
        step.outputs.push(PeerOutput::PurgeQueuedBlock {
            block_id,
            include_haves: false,
        });
        step.outputs.push(PeerOutput::PeerBlockDone { block_id });
    }

    fn handle_retire(&mut self, retire: Retire, step: &mut PeerStep) {
        let Retire {
            block_id,
            stripes,
            n,
        } = retire;
        let meta_pending = self.meta_received_pending.remove(&block_id).is_some();
        self.inflight_verify.retain(|_, v| v.block_id != block_id);
        let total = stripes.saturating_mul(n);

        // Replace per-block state with two bounded retired counters (bpv1-21).
        let counters = if let Some(block) = self.blocks.remove(&block_id) {
            let total_parts = block.coding.total_parts().unwrap_or(total);
            RetiredCounters {
                part_remaining: total_parts.saturating_sub(block.recv_record.count_ones()),
                have_remaining: total_parts.saturating_sub(block.peer_have.count_ones()),
                meta_received: block.meta_received || meta_pending,
            }
        } else {
            RetiredCounters {
                part_remaining: total,
                have_remaining: total,
                meta_received: meta_pending,
            }
        };
        self.retired.insert(block_id, counters);

        // Drop any held early entries for this retired block. Its retired
        // counters bound every later part, so lost flags no longer matter.
        self.drop_early_for_block(block_id, step);
        self.flags_lost.retain(|b| *b != block_id);

        // Purge queued parts & Haves for this block (bpv1-46).
        step.outputs.push(PeerOutput::PurgeQueuedBlock {
            block_id,
            include_haves: true,
        });

        // End every live incoming Want for this block with `Retired` (bpv1-29, -46).
        let ending_wants: Vec<u32> = self
            .wants_in
            .iter()
            .filter(|(_, w)| w.block_id == block_id)
            .map(|(&id, _)| id)
            .collect();
        for want_id in ending_wants {
            if let Some(want) = self.wants_in.remove(&want_id) {
                self.end_want(want_id, want, 0, WantEndReason::Retired, step);
            }
        }
    }

    fn handle_tick(&mut self, now: Instant, step: &mut PeerStep) {
        // 1. Age out unresolved early entries (bpv1-14, -48).
        let early_age = self.config.early_age;
        let (aged, kept): (VecDeque<_>, _) = std::mem::take(&mut self.early_data)
            .into_iter()
            .partition(|e| now.saturating_duration_since(e.received_at) >= early_age);
        self.early_data = kept;
        for entry in &aged {
            self.note_dropped_early(&entry.kind);
            step.events.push(PeerEvent::EarlyAged {
                block_id: entry.kind.block_id(),
                bytes: entry.kind.held_bytes(),
            });
        }
        self.release_early(aged.iter().map(|e| e.kind.held_bytes()).sum(), step);

        // 2. Unpark after cooldown expires.
        if let Some(until) = self.parked_until {
            if now >= until {
                self.parked_until = None;
                step.events.push(PeerEvent::Unparked);
                if !self.wants_out.is_empty() {
                    self.last_progress = Some(now);
                }
            }
        }

        // 3. Flush batched Haves that reached `have_flush_interval`.
        let flush_interval = HAVE_FLUSH_INTERVAL;
        let due_haves: Vec<block::Hash> = self
            .blocks
            .iter()
            .filter(|(_, b)| {
                b.have_held_until.is_none()
                    && !b.pending_have.is_empty()
                    && b.pending_have_since
                        .is_some_and(|since| now.saturating_duration_since(since) >= flush_interval)
            })
            .map(|(id, _)| *id)
            .collect();
        for id in due_haves {
            self.flush_block_have(id, step);
        }
        // bpv1-73: release held indices at each block's advertisement deadline.
        let deadlines: Vec<block::Hash> = self
            .blocks
            .iter()
            .filter(|(_, b)| b.have_held_until.is_some_and(|until| now >= until))
            .map(|(id, _)| *id)
            .collect();
        for id in deadlines {
            self.release_held_haves(id, step);
        }

        // 4. Fire late pulls (bpv1-64) and hard deadlines (bpv1-31) on outgoing Wants.
        let want_ids: Vec<u32> = self.wants_out.keys().copied().collect();
        for want_id in want_ids {
            let Some(want) = self.wants_out.get_mut(&want_id) else {
                continue;
            };
            // Late pull at SLOW_X * estimated arrival time: release claims once,
            // keep wire reservation, and never punish (bpv1-64).
            if !want.late_fired && !want.deadline_fired && now >= want.late_at {
                want.late_fired = true;
                let slots = want.unreceived_slots();
                if !slots.is_empty() {
                    step.events.push(PeerEvent::WantLate {
                        want_id,
                        released: slots.len(),
                    });
                    step.outputs.push(PeerOutput::ReleaseClaims {
                        block_id: want.block_id,
                        stripe: want.stripe,
                        slots,
                        reason: ReleaseReason::Late,
                    });
                }
            }

            // Hard deadline (8 s + outstanding bytes at 100 KB/s): release node-wide
            // claims and keep the wire reservation live (bpv1-31). The parts
            // stay outstanding, so they keep counting against `PULL_WINDOW`.
            if !want.deadline_fired && now >= want.deadline_at {
                want.deadline_fired = true;
                let slots = want.unreceived_slots();
                step.events.push(PeerEvent::WantDeadline {
                    want_id,
                    released: slots.len(),
                });
                if !slots.is_empty() {
                    step.outputs.push(PeerOutput::ReleaseClaims {
                        block_id: want.block_id,
                        stripe: want.stripe,
                        slots,
                        reason: ReleaseReason::Failed,
                    });
                }
            }
        }

        // 5. Liveness stall check (decision-3, bpv1-58):
        //    4 × request_timeout (including transfer time of outstanding bytes)
        //    with no Part and no WantEnd while wants_out is non-empty.
        if !self.wants_out.is_empty() && !self.is_parked(now) {
            if let Some(last) = self.last_progress {
                if now.saturating_duration_since(last) >= self.liveness_timeout() {
                    if !self.stalled_once {
                        // First stall: park session without retiring (keeps WriterFence happy).
                        self.stalled_once = true;
                        self.parked_until = Some(now + LIVENESS_COOLDOWN);
                        self.last_progress = Some(now);
                        step.events.push(PeerEvent::Parked);
                    } else {
                        // Repeat stall: close connection neutrally.
                        self.closed = Some(SessionClose::NeutralStall);
                        step.outputs.push(PeerOutput::CloseNeutralStall);
                    }
                }
            }
        }
    }

    /// Clears every per-session record. The table's `SessionEnded` releases
    /// this session's pull claims and pending pushes (bpv1-62), including
    /// grants that never reached the session, so the end emits nothing.
    fn handle_session_end(&mut self) {
        self.blocks.clear();
        self.meta_received_pending.clear();
        self.meta_dropped.clear();
        self.retired.clear();
        self.wants_out.clear();
        self.wants_in.clear();
        self.unwritten_ends.clear();
        self.inflight_verify.clear();
        self.early_data.clear();
        self.early_data_bytes = 0;
        self.early_control.clear();
        self.data_paused = false;
        self.flags_lost.clear();
        self.last_progress = None;
        self.parked_until = None;
    }

    /// Holds a `StripeDone` or `BlockDone` for a block this session has not
    /// admitted. It never pauses control (bpv1-54). The peer's `HeaderMeta`
    /// for the block is ahead of its traffic on the data stream, which can
    /// lag the control stream by any backlog, so the entry waits without an
    /// age limit. It leaves when the
    /// block admits, when the block retires, when newer entries push it out
    /// at the cap, or when the session ends.
    fn buffer_early_control(&mut self, block_id: block::Hash, kind: EarlyControlKind) {
        if self.early_control.len() >= EARLY_CONTROL_CAP {
            self.early_control.pop_front();
        }
        self.early_control
            .push_back(EarlyControlEntry { block_id, kind });
    }

    /// Handles a `Part` or `Have` for a block this session has not admitted
    /// (bpv1-14). The peer's `HeaderMeta` precedes the block's traffic on the
    /// data stream (bpv1-8), so the metadata crossed unless the peer broke
    /// that order. Data behind metadata under local admission waits in the
    /// early record (bpv1-48). Data behind metadata that this session dropped
    /// spends the block's counters and drops (bpv1-21). Data whose metadata
    /// never crossed closes 21.
    fn recv_unadmitted(&mut self, now: Instant, kind: EarlyDataKind, step: &mut PeerStep) {
        let block_id = kind.block_id();
        if self.meta_received_pending.contains_key(&block_id) {
            self.buffer_early_data(now, kind, step);
        } else if let Some(counters) = self.dropped_counters_mut(&block_id) {
            let spent = match &kind {
                EarlyDataKind::Part(_) => counters.spend_part(),
                EarlyDataKind::Have(have) => counters.spend_have(have.indices.len()),
            };
            if !spent {
                self.close_protocol(
                    BPV1_DATA_KIND,
                    Bpv1CloseRow::Row15RetiredCounterExhausted,
                    step,
                );
                return;
            }
            // A later admission still opens the push slots of a dropped
            // flagged `Have` (bpv1-60).
            self.note_dropped_early(&kind);
            if let EarlyDataKind::Part(prefix) = kind {
                step.events.push(PeerEvent::PartDropped {
                    block_id,
                    index: prefix.index,
                    reason: DropReason::MetaDropped,
                });
            }
        } else {
            self.close_protocol(
                BPV1_DATA_KIND,
                Bpv1CloseRow::Row21DataBeforeHeaderMeta,
                step,
            );
        }
    }

    /// Remembers that this session dropped the state of `block_id` after its
    /// `HeaderMeta` crossed, with the traffic it still accepts for the block.
    /// The oldest mark leaves when the set is full.
    fn note_meta_dropped(&mut self, block_id: block::Hash, counters: RetiredCounters) {
        if self.dropped_counters(&block_id).is_some() {
            return;
        }
        if self.meta_dropped.len() >= MAX_PENDING_META_COUNT {
            self.meta_dropped.pop_front();
        }
        self.meta_dropped.push_back((block_id, counters));
    }

    fn dropped_counters(&self, block_id: &block::Hash) -> Option<&RetiredCounters> {
        self.meta_dropped
            .iter()
            .find_map(|(b, counters)| (b == block_id).then_some(counters))
    }

    fn dropped_counters_mut(&mut self, block_id: &block::Hash) -> Option<&mut RetiredCounters> {
        self.meta_dropped
            .iter_mut()
            .find_map(|(b, counters)| (b == block_id).then_some(counters))
    }

    fn buffer_early_data(&mut self, now: Instant, kind: EarlyDataKind, step: &mut PeerStep) {
        let block_id = kind.block_id();
        let bytes = kind.held_bytes();
        // A single frame larger than the entire per-peer early cap can never fit:
        // drop it explicitly without pausing an empty queue.
        let refused = if bytes > self.config.early_peer_bytes_cap {
            Some(DropReason::EarlyOversize)
        } else if self.early_data_bytes.saturating_add(bytes) > self.config.early_peer_bytes_cap {
            if !self.data_paused {
                self.data_paused = true;
                step.outputs.push(PeerOutput::PauseDataReads);
            }
            Some(DropReason::EarlyFull)
        } else {
            None
        };
        if let Some(reason) = refused {
            self.note_dropped_early(&kind);
            if let EarlyDataKind::Part(prefix) = kind {
                step.events.push(PeerEvent::PartDropped {
                    block_id,
                    index: prefix.index,
                    reason,
                });
            }
            return;
        }
        self.early_data_bytes += bytes;
        self.early_data.push_back(EarlyDataEntry {
            received_at: now,
            kind,
        });
        step.events.push(PeerEvent::EarlyHeld { block_id, bytes });
        if self.early_data_bytes >= self.config.early_peer_bytes_cap && !self.data_paused {
            self.data_paused = true;
            step.outputs.push(PeerOutput::PauseDataReads);
        }
    }

    /// Frees `freed` early-record bytes and resumes data reads once the record
    /// has room again.
    fn release_early(&mut self, freed: usize, step: &mut PeerStep) {
        self.early_data_bytes = self.early_data_bytes.saturating_sub(freed);
        if freed > 0 && self.data_paused && self.early_data_bytes < self.config.early_peer_bytes_cap
        {
            self.data_paused = false;
            step.outputs.push(PeerOutput::ResumeDataReads);
        }
    }

    /// Removes and returns the early entries held for `block_id`.
    fn take_early_for_block(
        &mut self,
        block_id: block::Hash,
        step: &mut PeerStep,
    ) -> (VecDeque<EarlyControlEntry>, VecDeque<EarlyDataEntry>) {
        let (control, kept) = std::mem::take(&mut self.early_control)
            .into_iter()
            .partition(|e| e.block_id == block_id);
        self.early_control = kept;
        let (data, kept): (VecDeque<_>, _) = std::mem::take(&mut self.early_data)
            .into_iter()
            .partition(|e| e.kind.block_id() == block_id);
        self.early_data = kept;
        self.release_early(data.iter().map(|e| e.kind.held_bytes()).sum(), step);
        (control, data)
    }

    fn replay_early_for_block(&mut self, now: Instant, block_id: block::Hash, step: &mut PeerStep) {
        let (control, data) = self.take_early_for_block(block_id, step);
        for entry in control {
            if self.is_closed() {
                return;
            }
            match entry.kind {
                EarlyControlKind::StripeDone(stripe) => {
                    self.handle_recv_stripe_done(block_id, stripe, step);
                }
                EarlyControlKind::BlockDone => self.handle_recv_block_done(block_id, step),
            }
        }
        for entry in data {
            if self.is_closed() {
                return;
            }
            match entry.kind {
                EarlyDataKind::Part(prefix) => {
                    self.handle_recv_part_prefix(now, prefix, true, step);
                }
                EarlyDataKind::Have(have) => self.handle_recv_have(now, have, step),
            }
        }
    }

    fn drop_early_for_block(&mut self, block_id: block::Hash, step: &mut PeerStep) {
        self.early_control.retain(|e| e.block_id != block_id);
        self.drop_early_data_for_block(block_id, step);
    }

    fn drop_early_data_for_block(&mut self, block_id: block::Hash, step: &mut PeerStep) {
        let (data, kept): (VecDeque<_>, _) = std::mem::take(&mut self.early_data)
            .into_iter()
            .partition(|e| e.kind.block_id() == block_id);
        self.early_data = kept;
        self.release_early(data.iter().map(|e| e.kind.held_bytes()).sum(), step);
        for entry in &data {
            self.note_dropped_early(&entry.kind);
        }
    }

    /// Remembers a flagged `Have` that the early record dropped, so the
    /// parts behind it never close the peer after admission (bpv1-60). The
    /// oldest mark leaves when the set is full.
    fn note_dropped_early(&mut self, kind: &EarlyDataKind) {
        let EarlyDataKind::Have(have) = kind else {
            return;
        };
        if !have.flags.push() || self.flags_lost.contains(&have.block_id) {
            return;
        }
        if self.flags_lost.len() >= MAX_PENDING_META_COUNT {
            self.flags_lost.pop_front();
        }
        self.flags_lost.push_back(have.block_id);
    }

    /// Removes the lost-flag mark of `block_id`, returning whether it was set.
    fn take_flags_lost(&mut self, block_id: block::Hash) -> bool {
        let before = self.flags_lost.len();
        self.flags_lost.retain(|b| *b != block_id);
        self.flags_lost.len() < before
    }

    fn alloc_want_id(&mut self) -> u32 {
        loop {
            let id = self.next_want_id;
            self.next_want_id = self.next_want_id.wrapping_add(1);
            if self.next_want_id == 0 {
                self.next_want_id = 1;
            }
            if id != 0 && !self.wants_out.contains_key(&id) {
                return id;
            }
        }
    }
}
