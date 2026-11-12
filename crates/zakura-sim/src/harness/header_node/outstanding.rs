//! The tap's half of `header-sync/outstanding-work-bounded` (hdr-9d.1): the
//! reactor's growable containers, as its [`ReactorEvent::Sizes`] reports
//! them at the end of each unit of work, stay within their bounds, and
//! every key of a map kept for a peer or a call names one that still has
//! what the entry is for.
//!
//! The bounds restate production's constants (Rule 6): the queue's 16
//! slots ([`MAX_STAGED_TARGETS`]), the 16 locator permits, the 4,000-header
//! chunk budget, and the admitted peer limit the harness configures. The
//! membership rules join the reactor's keys to its own facts, never to
//! another of its containers (Rule A):
//!
//! - Request deadlines and staged-capacity leases: every key holds an
//!   active slot, as the queue's last `Slots` lists it. Each peer's lease
//!   count is the number of `PageStaged` facts with headers on that slot's
//!   round, so a lease kept twice or past its round breaks.
//! - Request reservations: each is open by its `ReservationOpened` with no
//!   `ReservationEnded`, for its peer, and backs that peer's active round,
//!   by the round its opening or its `ReservationBound` names, so one kept
//!   past its round breaks even when the peer's next round reuses it.
//! - Work slots: at most the queue's 16, and every unstarted slot's peer
//!   has an open locator read, which is all an unstarted slot waits for:
//!   every other wait removes it. A slot's lifetime belongs to the
//!   round-life properties, by the plan's rule partition: a round retired
//!   while the queue still holds it is its retirement reason's property's,
//!   `reset-invalidates-work` for an obsolete one and
//!   `peer-retirement-progress` for a departure, timeout or shutdown. So
//!   this row breaks on an active slot whose round the queue released
//!   (`RoundReleased`) only when its `Retired` gave any other reason.
//! - Status cadences (hdr-9d.2): listed, not checked. `ServiceSizes`
//!   reports the service's cadence keys at each insert and removal, and
//!   the report carries their peak. The map's only removal is the
//!   transport's `remove_peer`, and this harness has no transport, so no
//!   entry is ever removed here. The peak equals the connections the run
//!   opened and says nothing about production. Whether a closed
//!   connection's entry goes is the transport cleanup's contract
//!   (`RegisteredPeerCleanupGuard`). See Open coverage in the hdr-9 to
//!   hdr-13 implementation plan.
//! - Publisher tasks (hdr-9d.2): at most one runs per session, by
//!   `PublisherStarted` and `PublisherEnded`, and a run that comes to rest
//!   has none running whose session's token is cancelled, whatever
//!   cancelled it, or any at all once the reactor's `Shutdown` unit ran,
//!   which ends every session without cancelling their tokens. Each
//!   publisher hands its session's token to the tap as it's spawned, before
//!   the service queues the session's admission
//!   (`HeaderSyncObserver::session_cancel`), so no session ends unknown.
//!   Conversely, every session the reactor ends (`SessionEnded`,
//!   `SessionRefused`, `SessionAtCapacity`, or a replacement's
//!   `SessionAdmitted`) has its token cancelled by the end of that unit's
//!   `Sizes`. A publisher returns as soon as its session is cancelled.
//! - Subscriber cursors (hdr-9d.2): each subscription's unaccepted
//!   cursors, as `SubscriberServing` lists them after every claim and
//!   acceptance, are exactly the pages' last headers `CursorClaimed` gave
//!   it past the last `CursorsAccepted`, at most 4,000. Cursors are joined
//!   by hash alone: their heights and totals are commit-only-authorized's. After every open, terminal
//!   outcome and retraction too, a session's subscriber holds at most one
//!   live subscription and remembers at most one ended key, and after an
//!   outcome or a retraction the subscription is out of the table.
//! - Publisher state and serving budgets (hdr-9d.2), as `PublisherServing`
//!   reports them right after each operation that can grow them, under
//!   the lock that orders them: an `Open` the ledger applied, the ending of
//!   every subscription that was due, a committed page, and each loop pass,
//!   right after the capacity waits' pruning. At every site the ledger's live and closing
//!   subscriptions, and the subscriptions the publisher's state tracks, are
//!   each exactly the ones the ledger's own `PublicationChanged` facts
//!   opened and haven't ended, never judged against each other (Rule A).
//!   At most 2 of those, tombstones, unwritten terminal outcomes and
//!   publication slots, since each holds a slot, and each subscription's
//!   sent-cursor ring within 4,096. At a pass, at most 2 kept waits, every
//!   one a subscription is stalled on. The capacity-waits witness registers
//!   one: a replaced session's acquire finds its peer's lease still live,
//!   and a selected-chain change, before or after the subscription stalls
//!   on it, leaves no stall referencing it. At most 8 node
//!   and 2 peer execution slots, and at most 64 MiB of a peer's output.
//!   The node output bytes
//!   and the response slots are listed, not checked: their limits follow
//!   the node's configured session count through the toolkit's sizing.
//! - Serving peer budgets (hdr-9d.2): no size bound, since production has
//!   none (Rule 6), but each lookup's pruning removes every peer whose
//!   budgets had expired just before it, and none with any budget still
//!   alive, and no lookup replaces budgets any of which is still alive.
//!   Either would give the peer's next session budgets apart from its live
//!   ones, as `ServingPeers` names them. An expired entry never comes alive
//!   again, so an entry that expires during the pruning only adds to what
//!   it removes.
//! - Serving budgets at acquisition (hdr-9d.2): every push's acquisition
//!   (`ServingAcquired`) leaves at most 8 node and 2 peer execution slots
//!   and 64 MiB of the peer's output in use, so an excess that's acquired
//!   and released between publisher passes still breaks.
//! - Retained-path leases (hdr-9d.2): the node's storage holds at most 16
//!   open leases at once, by its `Leased` and `CapabilityEnded` facts, and
//!   at most 16 paths in its continuation cache, by `RetentionCached` and
//!   `RetentionEnded`, since a lease whose capability ends can leave its
//!   path cached.
//! - Staged entries: a receiving round holds exactly the headers its
//!   `PageStaged` facts carried, and a preparing or applying one holds
//!   none, since preparation takes them.
//! - Repair episodes: each one's owner is the owner its `Started` gave an
//!   active repair slot's round.
//! - Close deadlines and pending closes: every key is a peer admitted with
//!   the session the entry closes, by `SessionAdmitted` and
//!   `SessionEnded`, so an entry kept across a replacement breaks.
//! - Cached offers and the VCT supplier order: every key is a peer with an
//!   admitted session. The supplier order holds each peer at most once.
//! - Departed strikes and readmission cooldowns: retention only. Both maps
//!   stay within the admitted peer limit, restated here. Every strike entry
//!   holds at least one strike, for a peer with no admitted session. A
//!   maintenance unit ends with no entry past its own stored deadline. Stored
//!   counts and deadlines are production's values, read as they are. This
//!   property doesn't judge whether a count is what the session earned,
//!   whether a hold or cooldown has the configured length, whether a cooldown
//!   was owed, which entry the cap evicts, or whether admission resumes the
//!   right strikes. Those are penalty rules (Rule 5).
//!   `header-fault-penalized` owns a header fault's cooldown, in the wire
//!   harness. `bounded-adversary-progress` owns a pause that never lifts
//!   (hdr-13). An unproductive peer's strikes, hold and cooldown have no
//!   owner; see Open coverage in the hdr-9 to hdr-13 implementation plan.
//! - Session handles and scheduling state: exactly the admitted peers.
//! - Locator reads: every entry names a `Dispatched` locator call for its
//!   peer with no `CallEnded`.
//! - Storage calls: every key is a call with a `Dispatched` and no
//!   `CallEnded`, and every pending prepare or apply holds headers by a
//!   `HeadersHeld` with no `HeadersEnded`. The reactor polls exactly one
//!   port future per pending call (every direct call is accepted into
//!   `StorageCalls` and pushed as a future together, dispatch.rs:555-558,
//!   and settled before any unit ends). Open calls are bounded by kind:
//!   locator reads by the 16 permits, and prepares and applies by the
//!   headers they hold, which together stay within the 4,000-header chunk
//!   budget, so at most 4,000 of them.
//! - Completed targets: every key has the adopted generation and the
//!   adopted finalized anchor, is one a `TargetCompleted` named, and has a
//!   tip the node's engine holds, by the storage owner's own state. Both
//!   ways to complete a target, an admitted apply and a locator that
//!   already held it, leave its tip held, so the set is bounded by the
//!   engine's headers above the anchor, and a check that took an unknown
//!   target as known breaks.
//!
//! - VCT repair sources (#2298): the repair requirement's departed
//!   suppliers stay within the departed limit, restated here as the
//!   configured `max_inbound_peers + max_outbound_peers` the reactor prunes
//!   them to, and none is an admitted peer's source. Every key of its
//!   tried, excluded-input, busy and target-refusal sets is an admitted
//!   peer's source or a departed one, so each set stays within the
//!   admitted peers plus the departed limit. A source is its peer's
//!   digest, as production derives it. Whether a supplier earned its entry,
//!   or how long its hold lasts, is the repair policy's, not judged here.
//! - VCT repair context reads (#2298): by `Dispatched` and `CallEnded`,
//!   at most one open per repair owner, and, while the reactor runs, none
//!   still open, or ending, past the reactor's 30 s request timeout after
//!   its dispatch, in virtual time. A reactor whose VCT local operation
//!   passed its fatal deadline stops polling its calls and only waits for
//!   shutdown, so a read its shutdown unit still holds, or its teardown
//!   drops, isn't judged by age.
//!   The port times each read out at that timeout, and a requirement asks
//!   again only once its read is answered or past that deadline. A
//!   replaced episode's read can still be open beside its successor's,
//!   under another owner. An episode is replaced when storage publishes a
//!   new repair status, or when the node adopts committed state that moves
//!   the repair's generation or branch, and production caps neither that
//!   rate nor the node-wide count. So the node-wide count is listed, not
//!   checked, beyond these two rules.
//!
//! Cooldowns are capped by #2131, and the pruned-peer map by #2209, each at
//! the admitted peer limit.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    time::Duration,
};

use zakura_chain::block;
use zakura_header_chain::{
    BodyWorkOwner, BranchId, Frontier, HeaderGeneration, HeaderSyncWorkOwner, SourceId,
};
use zakura_network::zakura::{
    observe::{
        CallEnd, HeaderRequestTerminal, HeaderTargetPhase, PortCall, PublicationChange,
        PublisherSite, ReactorCause, ReactorEvent, ReactorSizes, RepairSources, ServeInUse,
        StorageCallKind, SubscriberChange, WorkSlot,
    },
    ZakuraPeerId,
};
use zakura_rt::sync::CancellationToken;

/// The reactor's request timeout, `HeaderSyncStartup`'s 30 s default, which
/// the storage harness keeps. The port times each VCT repair context read
/// out at it.
pub const REPAIR_CONTEXT_TIMEOUT: Duration = Duration::from_secs(30);

/// The queue's slot limit, `MAX_STAGED_TARGETS_V1`.
pub const MAX_STAGED_TARGETS: usize = 16;

/// The reactor's locator permits.
pub const LOCATOR_PERMITS: usize = 16;

/// The header chunk budget's capacity. It also bounds a subscription's
/// unaccepted cursors, since every page carries at least one header and
/// the window is at most `MAX_HS_PUSH_CREDIT_HEADERS`, 4,000.
pub const CHUNK_HEADERS: usize = 4_000;

/// A session's live subscriptions, and the ended keys its subscriber
/// remembers: `MAX_HS_SUBSCRIPTIONS`.
pub const MAX_SUBSCRIPTIONS: usize = 1;

/// A session's publication slots, `2 × MAX_HS_SUBSCRIPTIONS`, which also
/// bound its publisher's live and closing subscriptions, its tombstones,
/// its queued terminal outcomes and the capacity waits it keeps, at most
/// one per subscription.
pub const PUBLICATION_SLOTS: usize = 2;

/// Each subscription's sent-cursor ring, `HS_SENT_CURSOR_RING`.
pub const SENT_CURSOR_RING: usize = 4_096;

/// The retained-path leases the node's storage holds at once,
/// `MAX_RETAINED_PATH_LEASES`.
pub const MAX_RETAINED_PATH_LEASES: usize = 16;

/// Page reads that may run at once across the node, `NODE_EXECUTION`.
pub const NODE_EXECUTION: usize = 8;

/// Page reads that may run at once for one peer, across its replacement
/// sessions, `PEER_EXECUTION`.
pub const PEER_EXECUTION: usize = 2;

/// Unsent page bytes one peer may hold, `SERVE_PEER_OUTPUT_BYTES`, the
/// toolkit's 64 MiB default.
pub const SERVE_PEER_OUTPUT_BYTES: u64 = 64 << 20;

/// The peers the reactor admits at once, as production derives it from
/// `max_inbound_peers + max_outbound_peers`, capped by the node's
/// connection limit, and never below 8 (`MIN_PEER_IDENTITY_STATE`).
pub fn admitted_peer_limit(max_inbound: usize, max_outbound: usize, connections: usize) -> usize {
    max_inbound
        .saturating_add(max_outbound)
        .min(connections)
        .max(8)
}

/// Each row's largest size so far, shared by the tap and whoever reads it.
/// Cloning shares the store.
#[derive(Clone, Debug)]
pub struct OutstandingEvidence(Registered<Store>);

#[derive(Debug, Default)]
struct Store {
    /// Each row's peak, by name.
    peaks: BTreeMap<&'static str, usize>,
    /// The first step whose drain judged a `Sizes`.
    applicable: Option<u64>,
    /// Whether a `Sizes` was judged since the last drain.
    pending: bool,
    /// The publisher tasks running, by peer and session, each with the
    /// token that cancels its session, once its start handed it over.
    running: Vec<(ZakuraPeerId, u64, Option<CancellationToken>)>,
    /// Whether the reactor began its shutdown, which ends every session
    /// without cancelling their tokens.
    shut_down: bool,
    /// Whether the run came to rest: it ended idle, or at its budget after
    /// a drain that ran out of ready tasks.
    at_rest: bool,
    /// The publishers a quiescent step already found running under a
    /// cancelled token ([`OutstandingEvidence::lingering_cancelled`]).
    reported: Vec<(ZakuraPeerId, u64)>,
}

impl Snapshot for Store {
    /// Everything it holds, with each running publisher's token as whether
    /// it has one: a token is a live handle, which the scheduler's shutdown
    /// rightly cancels, not evidence. The running publishers are sorted,
    /// since the check lists them from a hashed set.
    fn snapshot(&self) -> serde_json::Value {
        let Store {
            peaks,
            applicable,
            pending,
            running,
            shut_down,
            at_rest,
            reported,
        } = self;
        let running = sorted_snapshot(
            running
                .iter()
                .map(|(peer, session_id, token)| (peer, session_id, token.is_some())),
        );
        serde_json::json!({
            "fields": debug_snapshot(&(peaks, applicable, pending, shut_down, at_rest, reported)),
            "running": running,
        })
    }
}

impl OutstandingEvidence {
    /// An empty store in `stores`, the node's.
    pub fn new(stores: &EvidenceStores) -> OutstandingEvidence {
        OutstandingEvidence(stores.register("outstanding", Store::default()))
    }

    /// Runs `read` on the store. Nothing else is called while the lock is
    /// held.
    fn with<R>(&self, read: impl FnOnce(&Store) -> R) -> R {
        self.0.read(read)
    }

    /// Runs `change` on the store, unless it's frozen, which drops the
    /// change.
    fn change<R>(&self, change: impl FnOnce(&mut Store) -> R) -> Option<R> {
        self.0.write(change)
    }

    /// Each row's largest size so far, by name.
    pub fn peaks(&self) -> BTreeMap<&'static str, usize> {
        self.with(|store| store.peaks.clone())
    }

    /// Whether a drain up to and including `step`'s judged a `Sizes`.
    pub fn applicable_by(&self, step: u64) -> bool {
        self.with(|store| store.applicable.is_some_and(|found| found <= step))
    }

    /// Records how the run ended, as [`super::adoption::settled`] judges it.
    pub fn end(&self, end: &crate::RunEnd) {
        let at_rest = super::adoption::settled(end);
        self.change(|store| store.at_rest = at_rest);
    }

    /// The publisher tasks running now under a cancelled session token that
    /// no earlier call returned, sorted, which this marks returned. A
    /// publisher returns as soon as its session is cancelled, and the
    /// cancellation wakes it, so after a step that leaves no task able to
    /// run there's none. A checker reads it there: a drain that finishes
    /// the work a publisher waits on can end one that ignored its
    /// cancellation before the run comes to rest. The tokens are read with
    /// no preemption point.
    pub fn lingering_cancelled(&self) -> Vec<(ZakuraPeerId, u64)> {
        let lingering = self.with(|store| {
            let mut lingering: Vec<(ZakuraPeerId, u64)> = store
                .running
                .iter()
                .filter(|(peer, session_id, cancel)| {
                    cancel
                        .as_ref()
                        .is_some_and(CancellationToken::peek_cancelled)
                        && !store.reported.contains(&(peer.clone(), *session_id))
                })
                .map(|(peer, session_id, _)| (peer.clone(), *session_id))
                .collect();
            lingering
                .sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()).then(a.1.cmp(&b.1)));
            lingering
        });
        self.change(|store| store.reported.extend(lingering.iter().cloned()));
        lingering
    }

    /// The publisher tasks still running when the run came to rest, if it
    /// did, whose session's token is cancelled, or every one once the
    /// reactor began its shutdown, but for the ones a quiescent step
    /// already found ([`Self::lingering_cancelled`]). A publisher returns as
    /// soon as its session is cancelled, so a run at rest has none. The
    /// tokens are read with no preemption point, after the run.
    pub fn lingering_at_rest(&self) -> Vec<(ZakuraPeerId, u64)> {
        self.with(|store| {
            if !store.at_rest {
                return Vec::new();
            }
            let mut lingering: Vec<(ZakuraPeerId, u64)> = store
                .running
                .iter()
                .filter(|(peer, session_id, cancel)| {
                    (store.shut_down
                        || cancel
                            .as_ref()
                            .is_some_and(CancellationToken::peek_cancelled))
                        && !store.reported.contains(&(peer.clone(), *session_id))
                })
                .map(|(peer, session_id, _)| (peer.clone(), *session_id))
                .collect();
            lingering
                .sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()).then(a.1.cmp(&b.1)));
            lingering
        })
    }

    /// Whether any drain judged a `Sizes`.
    pub fn any(&self) -> bool {
        self.with(|store| store.applicable.is_some())
    }
}

/// Reads the hashes of every header the node's engine holds, or `None` once
/// the engine is gone. The rule calls it only from [`Outstanding::drain`],
/// on the driver between steps, since the engine's lock is the simulator's
/// and a task that took it while the tap holds this rule's lock could be
/// parked there, and only for completed targets whose tips it hasn't seen
/// held yet.
pub(super) struct HeldHeaders(Box<dyn Fn() -> Option<HashSet<block::Hash>> + Send>);

impl HeldHeaders {
    /// Reads the held headers with `read`.
    pub(super) fn new(read: impl Fn() -> Option<HashSet<block::Hash>> + Send + 'static) -> Self {
        HeldHeaders(Box::new(read))
    }
}

impl std::fmt::Debug for HeldHeaders {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HeldHeaders")
    }
}

/// A dispatched call that hasn't ended: what it asks for, and for which
/// peer when it's a locator read or which owner when it's a repair context
/// read.
#[derive(Clone, Debug)]
enum Open {
    Locator(ZakuraPeerId),
    /// A repair context read, with its owner and the virtual time it was
    /// dispatched at, when the run had a clock.
    VctContext(BodyWorkOwner, Option<Duration>),
    Prepare,
    Apply,
}

/// The rule's state: what the reactor's facts say it should hold, and the
/// breaks not yet drained.
#[derive(Debug)]
pub(super) struct Outstanding {
    evidence: OutstandingEvidence,
    /// The admitted peer limit the harness configured.
    peer_limit: usize,
    /// The departed VCT repair suppliers the reactor keeps history for.
    departed_limit: usize,
    /// The virtual time the facts being observed came at, since the run's
    /// start, when the run has a clock.
    now: Option<Duration>,
    /// Each admitted peer's current session.
    admitted: HashMap<ZakuraPeerId, u64>,
    /// Each dispatched call with no `CallEnded`.
    calls: HashMap<u64, Open>,
    /// Each call holding headers by a `HeadersHeld` with no `HeadersEnded`,
    /// with how many.
    holding: HashMap<u64, usize>,
    /// How many `PageStaged` facts with headers each round has, and the
    /// headers they carried, by peer and incarnation.
    pages: HashMap<(ZakuraPeerId, u64), (usize, usize)>,
    /// Every completion a `TargetCompleted` named.
    completed: HashSet<(HeaderGeneration, BranchId)>,
    /// Reads the engine's held headers.
    held: HeldHeaders,
    /// The completed targets' tips seen held. Nothing removes a held
    /// header in any fixture, so a tip seen held stays held.
    held_tips: HashSet<block::Hash>,
    /// Completed targets' tips a `Sizes` showed that aren't yet seen held,
    /// for the next drain to look up.
    unseen_tips: HashSet<block::Hash>,
    /// The hashes of each subscription's claimed pages' last headers past
    /// its accepted cursor, by peer, session and subscription, from
    /// `CursorClaimed` and `CursorsAccepted`.
    claimed: HashMap<(ZakuraPeerId, u64, u64), Vec<block::Hash>>,
    /// The node's open retained-path leases, by peer, session and lease ID,
    /// from its storage's `Leased` and `CapabilityEnded`.
    leases: HashSet<(String, u64, u64)>,
    /// The retained paths in the node's continuation cache, by the lease
    /// they came from, from `RetentionCached` and `RetentionEnded`.
    cached: HashSet<(String, u64, u64)>,
    /// The publisher tasks running, by peer and session, from
    /// `PublisherStarted` and `PublisherEnded`.
    publishers: HashSet<(ZakuraPeerId, u64)>,
    /// The subscriptions each session's publication ledger opened and hasn't
    /// ended, by peer and session, from `PublicationChanged`.
    publications: HashMap<(ZakuraPeerId, u64), BTreeSet<u64>>,
    /// The token that cancels each session, by peer and session, as its
    /// publisher's start handed it over.
    cancels: HashMap<(ZakuraPeerId, u64), CancellationToken>,
    /// The sessions the reactor ended since its last `Sizes`: by
    /// `SessionEnded`, `SessionRefused`, `SessionAtCapacity`, or a
    /// replacement's `SessionAdmitted`. Each one's token must be cancelled by the end of
    /// the unit that ended it.
    ended_unchecked: Vec<(ZakuraPeerId, u64)>,
    /// Each round the queue released, by peer and incarnation, with the
    /// reason its `Retired` gave, once it did.
    released: HashMap<(ZakuraPeerId, u64), Option<HeaderRequestTerminal>>,
    /// Whether the reactor began its shutdown, which ends every session.
    shut_down: bool,
    /// The unit of work the reactor is in, by its last `CauseSet`.
    cause: Option<ReactorCause>,
    /// The owner each started round's `Started` gave it, by peer and
    /// incarnation, until its `RoundReleased`.
    rounds: HashMap<(ZakuraPeerId, u64), HeaderSyncWorkOwner>,
    /// Each open reservation's peer and the round it backs, once known.
    reserved: HashMap<u64, (ZakuraPeerId, Option<u64>)>,
    /// The queue's slots, as its last `Slots` listed them.
    slots: Vec<WorkSlot>,
    /// The adopted generation and finalized anchor.
    adopted: Option<(HeaderGeneration, Frontier)>,
    breaks: Vec<String>,
}

impl Outstanding {
    /// The rule for a node whose admitted peer limit is `peer_limit` and
    /// that keeps history for at most `departed_limit` departed VCT repair
    /// suppliers, keeping its peaks in `evidence` and reading its engine's
    /// headers with `held`.
    pub(super) fn new(
        evidence: OutstandingEvidence,
        peer_limit: usize,
        departed_limit: usize,
        held: HeldHeaders,
    ) -> Self {
        Outstanding {
            evidence,
            peer_limit,
            departed_limit,
            now: None,
            admitted: HashMap::new(),
            calls: HashMap::new(),
            holding: HashMap::new(),
            pages: HashMap::new(),
            completed: HashSet::new(),
            held,
            held_tips: HashSet::new(),
            unseen_tips: HashSet::new(),
            rounds: HashMap::new(),
            cause: None,
            released: HashMap::new(),
            shut_down: false,
            publishers: HashSet::new(),
            leases: HashSet::new(),
            cached: HashSet::new(),
            claimed: HashMap::new(),
            publications: HashMap::new(),
            cancels: HashMap::new(),
            ended_unchecked: Vec::new(),
            reserved: HashMap::new(),
            slots: Vec::new(),
            adopted: None,
            breaks: Vec::new(),
        }
    }

    /// Notes the reactor's `event`, judging a `Slots` list's length and
    /// every `Sizes`.
    pub(super) fn observe(&mut self, event: &ReactorEvent) {
        match event {
            ReactorEvent::SessionAdmitted {
                peer,
                session_id,
                replaced,
                ..
            } => {
                self.admitted.insert(peer.clone(), *session_id);
                if let Some(replaced) = replaced {
                    self.ended_unchecked.push((peer.clone(), *replaced));
                }
            }
            ReactorEvent::SessionEnded {
                peer, session_id, ..
            } => {
                if self.admitted.get(peer) == Some(session_id) {
                    self.admitted.remove(peer);
                }
                self.ended_unchecked.push((peer.clone(), *session_id));
            }
            ReactorEvent::SessionRefused { peer, session_id }
            | ReactorEvent::SessionAtCapacity { peer, session_id } => {
                self.ended_unchecked.push((peer.clone(), *session_id));
            }
            // Joined by the page's own last header, not the stored cursor,
            // which commit-only-authorized judges, as are the heights.
            ReactorEvent::CursorClaimed {
                peer,
                session_id,
                subscription,
                page_last,
                ..
            } => self
                .claimed
                .entry((peer.clone(), *session_id, *subscription))
                .or_default()
                .push(page_last.hash),
            ReactorEvent::CursorsAccepted {
                peer,
                session_id,
                subscription,
                through,
                ..
            } => {
                if let Some(claimed) =
                    self.claimed
                        .get_mut(&(peer.clone(), *session_id, *subscription))
                {
                    if let Some(index) = claimed.iter().position(|hash| *hash == through.hash) {
                        claimed.drain(..=index);
                    }
                }
            }
            ReactorEvent::SubscriberServing {
                peer,
                session_id,
                subscription,
                change,
                unaccepted,
                live,
                ended,
            } => {
                self.peak("subscriber live subscriptions", *live);
                self.peak("subscriber ended keys", *ended);
                let key = (peer.clone(), *session_id, *subscription);
                if unaccepted.is_some()
                    && matches!(
                        change,
                        SubscriberChange::Outcome | SubscriberChange::Retract
                    )
                {
                    self.breaks.push(format!(
                        "OutstandingWork: {peer:?} session {session_id}'s subscriber keeps \
                         subscription {subscription} after its {change:?}"
                    ));
                }
                let held = match unaccepted {
                    Some(unaccepted) => {
                        let expected = self.claimed.get(&key).map_or(&[][..], Vec::as_slice);
                        if !unaccepted.iter().map(|cursor| &cursor.hash).eq(expected) {
                            self.breaks.push(format!(
                                "OutstandingWork: {peer:?} session {session_id} subscription \
                                 {subscription} holds unaccepted cursors {unaccepted:?}, but its \
                                 claims past its acceptance are {expected:?}"
                            ));
                        }
                        unaccepted.len()
                    }
                    // The subscription left the table, and its cursors with it.
                    None => {
                        self.claimed.remove(&key);
                        0
                    }
                };
                self.peak("subscriber unaccepted cursors", held);
                for (row, size, bound) in [
                    ("unaccepted cursors", held, CHUNK_HEADERS),
                    ("live subscriptions", *live, MAX_SUBSCRIPTIONS),
                    ("ended keys", *ended, MAX_SUBSCRIPTIONS),
                ] {
                    if size > bound {
                        self.breaks.push(format!(
                            "OutstandingWork: {peer:?} session {session_id}'s subscriber holds \
                             {size} {row}, past its {bound}"
                        ));
                    }
                }
            }
            ReactorEvent::PublicationChanged {
                peer,
                session_id,
                subscription,
                change,
            } => {
                let live = self
                    .publications
                    .entry((peer.clone(), *session_id))
                    .or_default();
                match change {
                    PublicationChange::Opened => live.insert(*subscription),
                    PublicationChange::Ended => live.remove(subscription),
                };
            }
            ReactorEvent::PublisherServing {
                peer,
                session_id,
                site,
                live,
                subscriptions,
                tombstones,
                terminals,
                job: _,
                slots,
                waits,
                stalls,
                in_use,
            } => {
                // Each container against the ledger's own open and end
                // facts, never against the other container (Rule A).
                let opened: Vec<u64> = self
                    .publications
                    .get(&(peer.clone(), *session_id))
                    .map(|live| live.iter().copied().collect())
                    .unwrap_or_default();
                let ledger: Vec<u64> = live.iter().map(|(key, _)| *key).collect();
                for (container, keys) in [("ledger", &ledger), ("state", subscriptions)] {
                    if *keys != opened {
                        self.breaks.push(format!(
                            "OutstandingWork: {peer:?} session {session_id}'s publisher {container} \
                             holds subscriptions {keys:?} at its {site:?}, but its ledger opened \
                             and hasn't ended {opened:?}"
                        ));
                    }
                }
                let ring = live.iter().map(|(_, sent)| *sent).max().unwrap_or(0);
                for (row, size, bound) in [
                    ("publisher subscriptions", live.len(), PUBLICATION_SLOTS),
                    ("publisher tombstones", *tombstones, PUBLICATION_SLOTS),
                    ("publisher terminal outcomes", *terminals, PUBLICATION_SLOTS),
                    ("publication slots", *slots, PUBLICATION_SLOTS),
                    ("sent-cursor ring", ring, SENT_CURSOR_RING),
                ] {
                    self.publisher_bound(peer, *session_id, row, size, bound);
                }
                // The waits are pruned only at a pass, so only a pass's are
                // judged, and the budgets are read only there.
                if *site == PublisherSite::Pass {
                    self.publisher_pass(peer, *session_id, waits, stalls, in_use.as_ref());
                }
            }
            ReactorEvent::ServingPeers {
                expired,
                removed,
                removed_live,
            } => {
                self.peak("expired serving peer budgets", expired.len());
                if !removed_live.is_empty() {
                    self.breaks.push(format!(
                        "OutstandingWork: a serving lookup removed or replaced the live budgets \
                         of {removed_live:?}, so their next session gets budgets apart from them"
                    ));
                }
                let kept: Vec<&ZakuraPeerId> = expired
                    .iter()
                    .filter(|peer| !removed.contains(peer))
                    .collect();
                if !kept.is_empty() {
                    self.breaks.push(format!(
                        "OutstandingWork: a serving lookup's pruning kept the expired budgets of \
                         {kept:?}"
                    ));
                }
            }
            ReactorEvent::ServingAcquired { peer, in_use } => {
                let peer_output = usize::try_from(in_use.peer_output).unwrap_or(usize::MAX);
                for (row, size, bound) in [
                    (
                        "node execution slots",
                        in_use.node_execution,
                        NODE_EXECUTION,
                    ),
                    (
                        "peer execution slots",
                        in_use.peer_execution,
                        PEER_EXECUTION,
                    ),
                    (
                        "peer output bytes",
                        peer_output,
                        usize::try_from(SERVE_PEER_OUTPUT_BYTES).unwrap_or(usize::MAX),
                    ),
                ] {
                    self.peak(row, size);
                    if size > bound {
                        self.breaks.push(format!(
                            "OutstandingWork: a serving acquisition for {peer:?} leaves {size} \
                             {row} in use, past its {bound}"
                        ));
                    }
                }
            }
            ReactorEvent::PublisherStarted { peer, session_id } => {
                let key = (peer.clone(), *session_id);
                if !self.publishers.insert(key) {
                    self.breaks.push(format!(
                        "OutstandingWork: the service runs a second publisher for {peer:?} \
                         session {session_id}, whose first hasn't ended"
                    ));
                }
                self.peak("publisher tasks", self.publishers.len());
                self.note_running();
            }
            ReactorEvent::PublisherEnded { peer, session_id } => {
                self.publishers.remove(&(peer.clone(), *session_id));
                self.note_running();
            }
            ReactorEvent::Dispatched { call, op } => {
                let open = match op {
                    PortCall::Locator { peer, .. } => Open::Locator(peer.clone()),
                    PortCall::VctContext { owner, .. } => Open::VctContext(*owner, self.now),
                    PortCall::Prepare { .. } => Open::Prepare,
                    PortCall::Apply { .. } => Open::Apply,
                };
                self.calls.insert(*call, open);
            }
            ReactorEvent::CallEnded { call, end } => {
                let open = self.calls.remove(call);
                if let (Some(Open::VctContext(owner, dispatched)), false) =
                    (open, *end == CallEnd::Dropped)
                {
                    self.context_read_ended(owner, dispatched);
                }
            }
            ReactorEvent::Started { request, .. } => {
                self.rounds
                    .insert((request.peer.clone(), request.incarnation), request.owner);
            }
            ReactorEvent::CauseSet { cause } => {
                self.cause = Some(*cause);
                if *cause == ReactorCause::Shutdown && !self.shut_down {
                    self.shut_down = true;
                    self.evidence.change(|store| store.shut_down = true);
                }
            }
            ReactorEvent::Retired {
                request, reason, ..
            } => {
                self.released
                    .insert((request.peer.clone(), request.incarnation), Some(*reason));
            }
            ReactorEvent::TargetCompleted {
                header_generation,
                branch,
                ..
            } => {
                self.completed.insert((*header_generation, *branch));
            }
            ReactorEvent::HeadersHeld {
                call,
                kind,
                headers,
                ..
            } => {
                if matches!(kind, StorageCallKind::Prepare | StorageCallKind::Apply) {
                    self.holding.insert(*call, *headers);
                }
            }
            ReactorEvent::PageStaged {
                request, headers, ..
            } => {
                if *headers > 0 {
                    let (pages, staged) = self
                        .pages
                        .entry((request.peer.clone(), request.incarnation))
                        .or_default();
                    *pages += 1;
                    *staged += headers;
                }
            }
            ReactorEvent::ReservationOpened {
                reservation,
                peer,
                request,
                ..
            } => {
                self.reserved.insert(
                    *reservation,
                    (
                        peer.clone(),
                        request.as_ref().map(|round| round.incarnation),
                    ),
                );
            }
            ReactorEvent::ReservationBound {
                reservation,
                request,
                ..
            } => {
                if let Some((_, round)) = self.reserved.get_mut(reservation) {
                    *round = Some(request.incarnation);
                }
            }
            ReactorEvent::ReservationEnded { reservation, .. } => {
                self.reserved.remove(reservation);
            }
            ReactorEvent::RoundReleased {
                peer, incarnation, ..
            } => {
                self.pages.remove(&(peer.clone(), *incarnation));
                self.rounds.remove(&(peer.clone(), *incarnation));
                self.released
                    .entry((peer.clone(), *incarnation))
                    .or_insert(None);
            }
            ReactorEvent::HeadersEnded { call, .. } => {
                self.holding.remove(call);
            }
            ReactorEvent::SnapshotAdopted {
                header_generation,
                finalized,
                ..
            } => self.adopted = Some((*header_generation, *finalized)),
            ReactorEvent::Slots { slots } => {
                self.slots = slots.clone();
                self.peak("work slots", slots.len());
                if slots.len() > MAX_STAGED_TARGETS {
                    self.breaks.push(format!(
                        "OutstandingWork: the queue holds {} slots, past its {MAX_STAGED_TARGETS}",
                        slots.len()
                    ));
                }
            }
            ReactorEvent::Sizes { sizes } => {
                self.judge_cancels();
                self.judge(sizes);
            }
            // Listed, not checked: see the module doc.
            ReactorEvent::ServiceSizes { cadences } => {
                self.peak("status cadences", cadences.len());
            }
            _ => {}
        }
    }

    /// Notes the node's storage `fact`: a retained-path lease beginning or
    /// its handle ending. More open leases than storage's 16 break the rule.
    pub(super) fn storage(&mut self, fact: &crate::observe::StorageFact) {
        use crate::observe::StorageFact;
        match fact {
            StorageFact::Leased { lease, .. } => {
                self.leases
                    .insert((lease.peer.clone(), lease.session_id, lease.lease_id));
                self.peak("retained-path leases", self.leases.len());
                if self.leases.len() > MAX_RETAINED_PATH_LEASES {
                    self.breaks.push(format!(
                        "OutstandingWork: the node's storage holds {} retained-path leases, past \
                         its {MAX_RETAINED_PATH_LEASES}",
                        self.leases.len()
                    ));
                }
            }
            StorageFact::CapabilityEnded { lease, .. } => {
                self.leases
                    .remove(&(lease.peer.clone(), lease.session_id, lease.lease_id));
            }
            StorageFact::RetentionCached { lease, .. } => {
                self.cached
                    .insert((lease.peer.clone(), lease.session_id, lease.lease_id));
                self.peak("cached retained paths", self.cached.len());
                if self.cached.len() > MAX_RETAINED_PATH_LEASES {
                    self.breaks.push(format!(
                        "OutstandingWork: the node's continuation cache holds {} retained paths, \
                         past its {MAX_RETAINED_PATH_LEASES}",
                        self.cached.len()
                    ));
                }
            }
            StorageFact::RetentionEnded { lease, .. } => {
                self.cached
                    .remove(&(lease.peer.clone(), lease.session_id, lease.lease_id));
            }
            _ => {}
        }
    }

    /// Keeps `cancel`, the token that cancels `peer`'s session
    /// `session_id`, which its publisher handed over as it started.
    pub(super) fn session_cancel(
        &mut self,
        peer: &ZakuraPeerId,
        session_id: u64,
        cancel: CancellationToken,
    ) {
        self.cancels.insert((peer.clone(), session_id), cancel);
        self.note_running();
    }

    /// Breaks for each session the reactor ended since its last `Sizes`
    /// whose token isn't cancelled, read with no preemption point. The
    /// reactor cancels a refused or replaced session's token in the unit
    /// that ends it, and the transport cancels a departed one's before the
    /// reactor hears of it. A session whose token isn't known yet waits for
    /// it, and is judged at the first `Sizes` after.
    fn judge_cancels(&mut self) {
        let mut unknown = Vec::new();
        for (peer, session_id) in std::mem::take(&mut self.ended_unchecked) {
            let Some(cancel) = self.cancels.get(&(peer.clone(), session_id)) else {
                unknown.push((peer, session_id));
                continue;
            };
            if !cancel.peek_cancelled() {
                self.breaks.push(format!(
                    "OutstandingWork: the reactor ended {peer:?} session {session_id}, but its \
                     token isn't cancelled, so its publisher can run on"
                ));
            }
        }
        self.ended_unchecked = unknown;
    }

    /// Keeps the running publishers, with their sessions' tokens, in the
    /// evidence, for the run's end to judge.
    fn note_running(&self) {
        let running = self
            .publishers
            .iter()
            .map(|(peer, session_id)| {
                let cancel = self.cancels.get(&(peer.clone(), *session_id)).cloned();
                (peer.clone(), *session_id, cancel)
            })
            .collect();
        self.evidence.change(|store| store.running = running);
    }

    /// Judges a publisher pass's kept waits, right after their pruning, and
    /// the serving budgets `in_use` then.
    fn publisher_pass(
        &mut self,
        peer: &ZakuraPeerId,
        session_id: u64,
        waits: &[u64],
        stalls: &[u64],
        in_use: Option<&ServeInUse>,
    ) {
        self.publisher_bound(
            peer,
            session_id,
            "publisher capacity waits",
            waits.len(),
            PUBLICATION_SLOTS,
        );
        let stray: Vec<&u64> = waits.iter().filter(|wait| !stalls.contains(wait)).collect();
        if !stray.is_empty() {
            self.breaks.push(format!(
                "OutstandingWork: {peer:?} session {session_id}'s publisher keeps \
                 capacity waits {stray:?} after pruning, which no subscription is \
                 stalled on"
            ));
        }
        let Some(in_use) = in_use else {
            return;
        };
        self.publisher_bound(
            peer,
            session_id,
            "node execution slots",
            in_use.node_execution,
            NODE_EXECUTION,
        );
        self.publisher_bound(
            peer,
            session_id,
            "peer execution slots",
            in_use.peer_execution,
            PEER_EXECUTION,
        );
        let peer_output = usize::try_from(in_use.peer_output).unwrap_or(usize::MAX);
        self.peak("peer output bytes", peer_output);
        if in_use.peer_output > SERVE_PEER_OUTPUT_BYTES {
            self.breaks.push(format!(
                "OutstandingWork: {peer:?}'s serving holds {} output bytes, past its \
                 {SERVE_PEER_OUTPUT_BYTES}",
                in_use.peer_output
            ));
        }
        // Listed, not checked: their limits follow the node's configured
        // session count through the toolkit's sizing.
        self.peak(
            "node output bytes",
            usize::try_from(in_use.node_output).unwrap_or(usize::MAX),
        );
        self.peak("node response slots", in_use.node_responses);
        self.peak("peer response slots", in_use.peer_responses);
    }

    /// Notes `size` as `peer` session `session_id`'s publisher's `row`, and
    /// breaks if it's past `bound`.
    fn publisher_bound(
        &mut self,
        peer: &ZakuraPeerId,
        session_id: u64,
        row: &'static str,
        size: usize,
        bound: usize,
    ) {
        self.peak(row, size);
        if size > bound {
            self.breaks.push(format!(
                "OutstandingWork: {peer:?} session {session_id}'s publisher holds {size} {row}, \
                 past its {bound}"
            ));
        }
    }

    /// Notes `size` as `row`'s size now.
    fn peak(&self, row: &'static str, size: usize) {
        self.evidence.change(|store| {
            let peak = store.peaks.entry(row).or_default();
            *peak = (*peak).max(size);
        });
    }

    /// Breaks if a repair context read for `owner`, dispatched at
    /// `dispatched`, ends now, past the request timeout, which a `Sizes`
    /// report between its dispatch and its end might not have seen.
    fn context_read_ended(&mut self, owner: BodyWorkOwner, dispatched: Option<Duration>) {
        if let Some(age) = self
            .now
            .zip(dispatched)
            .map(|(now, at)| now.saturating_sub(at))
            .filter(|age| *age > REPAIR_CONTEXT_TIMEOUT)
        {
            self.breaks.push(format!(
                "OutstandingWork: the reactor ended a repair context read for {owner:?} \
                 {age:?} after its dispatch, past the {REPAIR_CONTEXT_TIMEOUT:?} request timeout"
            ));
        }
    }

    /// Sets the virtual time, since the run's start, that the next facts
    /// come at, or `None` outside a run's clock.
    pub(super) fn at(&mut self, now: Option<Duration>) {
        self.now = now;
    }

    /// Judges the VCT repair rows, the requirement's `sources` and the open
    /// context reads, into `found`.
    fn judge_repair(&self, sources: &RepairSources, found: &mut Vec<String>) {
        let RepairSources {
            tried,
            excluded_input,
            busy,
            target_refusals,
            departed,
        } = sources;
        let admitted: HashSet<SourceId> = self
            .admitted
            .keys()
            .map(|peer| SourceId::from_digest(peer.digest()))
            .collect();
        self.peak("departed repair sources", departed.len());
        if departed.len() > self.departed_limit {
            found.push(format!(
                "holds {} departed repair sources, past its {}",
                departed.len(),
                self.departed_limit
            ));
        }
        let connected: Vec<&SourceId> = departed
            .iter()
            .filter(|source| admitted.contains(source))
            .collect();
        if !connected.is_empty() {
            found.push(format!(
                "holds departed repair history for {connected:?}, which are admitted peers"
            ));
        }
        // Sets have no duplicate keys, so membership bounds each one by the
        // admitted peers plus the departed limit.
        for (row, keys) in [
            ("tried repair sources", tried),
            ("excluded repair sources", excluded_input),
            ("busy repair sources", busy),
            ("repair target refusals", target_refusals),
        ] {
            self.peak(row, keys.len());
            let stray: Vec<&SourceId> = keys
                .iter()
                .filter(|source| !admitted.contains(source) && !departed.contains(source))
                .collect();
            if !stray.is_empty() {
                found.push(format!(
                    "holds {row} {stray:?}, which are neither admitted peers nor held \
                     departures"
                ));
            }
        }
        let mut reads: HashMap<&BodyWorkOwner, usize> = HashMap::new();
        for open in self.calls.values() {
            let Open::VctContext(owner, dispatched) = open else {
                continue;
            };
            *reads.entry(owner).or_default() += 1;
            // See the module docs: the shutdown unit's reads aren't judged
            // by age.
            if self.shut_down {
                continue;
            }
            let age = self
                .now
                .zip(*dispatched)
                .map(|(now, at)| now.saturating_sub(at));
            if let Some(age) = age.filter(|age| *age > REPAIR_CONTEXT_TIMEOUT) {
                found.push(format!(
                    "holds a repair context read for {owner:?} open {age:?} after its \
                     dispatch, past the {REPAIR_CONTEXT_TIMEOUT:?} request timeout"
                ));
            }
        }
        self.peak("repair context reads", reads.values().sum());
        for (owner, count) in reads {
            if count > 1 {
                found.push(format!(
                    "holds {count} open repair context reads for {owner:?}, past its 1"
                ));
            }
        }
    }

    /// Judges one `Sizes` report against every row.
    fn judge(&mut self, sizes: &ReactorSizes) {
        let ReactorSizes {
            sessions,
            scheduled,
            cached_offers,
            reservations,
            staged_capacity,
            staged_entries,
            repair_episodes,
            request_deadlines,
            close_deadlines,
            pending_closes,
            locator_reads,
            storage_calls,
            port_futures,
            supplier_order,
            departed_strikes,
            cooldowns,
            pruned_peers,
            completed_targets,
            repair_sources,
        } = sizes;
        self.evidence.change(|store| store.pending = true);
        let limit = self.peer_limit;
        let mut found = Vec::new();
        let bounds = [
            ("session handles", sessions.len(), limit),
            ("scheduling state", scheduled.len(), limit),
            ("cached offers", cached_offers.len(), limit),
            ("reservations", reservations.len(), MAX_STAGED_TARGETS),
            ("staged capacity", staged_capacity.len(), MAX_STAGED_TARGETS),
            (
                "staged entries",
                staged_entries
                    .iter()
                    .fold(0, |total, (_, headers)| total.saturating_add(*headers)),
                CHUNK_HEADERS,
            ),
            ("repair episodes", repair_episodes.len(), MAX_STAGED_TARGETS),
            (
                "request deadlines",
                request_deadlines.len(),
                MAX_STAGED_TARGETS,
            ),
            ("close deadlines", close_deadlines.len(), limit),
            ("pending closes", pending_closes.len(), limit),
            ("locator reads", locator_reads.len(), LOCATOR_PERMITS),
            ("supplier order", supplier_order.len(), limit),
            ("departed strikes", departed_strikes.len(), limit),
            ("cooldowns", cooldowns.len(), limit),
            ("pruned peers", pruned_peers.len(), limit),
        ];
        for (row, size, bound) in bounds {
            self.peak(row, size);
            if size > bound {
                found.push(format!("holds {size} {row}, past its {bound}"));
            }
        }
        self.peak("storage calls", storage_calls.len());
        self.peak("port futures", *port_futures);
        self.peak("completed targets", completed_targets.len());

        let admitted = |peer: &ZakuraPeerId| self.admitted.contains_key(peer);
        let mut expected: Vec<&ZakuraPeerId> = self.admitted.keys().collect();
        expected.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        for (row, peers) in [
            ("session handles", sessions),
            ("scheduling state", scheduled),
        ] {
            if peers.iter().collect::<Vec<_>>() != expected {
                found.push(format!(
                    "holds {row} for {peers:?}, but the admitted peers are {expected:?}"
                ));
            }
        }
        for (row, closes) in [
            ("close deadlines", close_deadlines),
            ("pending closes", pending_closes),
        ] {
            let stray: Vec<&(ZakuraPeerId, u64)> = closes
                .iter()
                .filter(|(peer, session_id)| self.admitted.get(peer) != Some(session_id))
                .collect();
            if !stray.is_empty() {
                found.push(format!(
                    "holds {row} for {stray:?}, which aren't admitted sessions"
                ));
            }
        }
        // A slot's lifetime is the round-life properties' (the plan's rule
        // partition): a round retired while the queue holds it is its
        // retirement reason's. An obsolete one's is reset-invalidates-work's,
        // and a departure's, timeout's or shutdown's peer-retirement-
        // progress's, so a slot kept past its round's release breaks here
        // only for any other reason.
        for slot in self.slots.iter().filter(|slot| slot.phase.is_some()) {
            let Some(Some(reason)) = self.released.get(&(slot.peer.clone(), slot.incarnation))
            else {
                continue;
            };
            let owned_elsewhere = matches!(
                reason,
                HeaderRequestTerminal::SnapshotObsolete
                    | HeaderRequestTerminal::Disconnected
                    | HeaderRequestTerminal::SessionReplaced
                    | HeaderRequestTerminal::TimedOut
                    | HeaderRequestTerminal::Shutdown
            );
            if !owned_elsewhere {
                found.push(format!(
                    "holds a slot for {:?}'s round {}, which the queue released as {reason:?}",
                    slot.peer, slot.incarnation
                ));
            }
        }
        for slot in self.slots.iter().filter(|slot| slot.phase.is_none()) {
            let reading = self
                .calls
                .values()
                .any(|open| matches!(open, Open::Locator(asked) if *asked == slot.peer));
            if !reading {
                found.push(format!(
                    "holds an unstarted slot for {:?}, which has no open locator read",
                    slot.peer
                ));
            }
        }
        let departed: Vec<&ZakuraPeerId> = departed_strikes
            .iter()
            .map(|(peer, _, _)| peer)
            .filter(|peer| self.admitted.contains_key(*peer))
            .collect();
        if !departed.is_empty() {
            found.push(format!(
                "holds departed strikes for {departed:?}, which have an admitted session"
            ));
        }
        let strikeless: Vec<&ZakuraPeerId> = departed_strikes
            .iter()
            .filter(|(_, strikes, _)| *strikes == 0)
            .map(|(peer, _, _)| peer)
            .collect();
        if !strikeless.is_empty() {
            found.push(format!(
                "holds departed strikes for {strikeless:?} with no strikes"
            ));
        }
        if self.cause == Some(ReactorCause::Maintenance) {
            let strikes: Vec<(ZakuraPeerId, std::time::Duration)> = departed_strikes
                .iter()
                .map(|(peer, _, left)| (peer.clone(), *left))
                .collect();
            for (row, entries) in [("departed strikes", &strikes), ("cooldowns", cooldowns)] {
                let expired: Vec<&ZakuraPeerId> = entries
                    .iter()
                    .filter(|(_, left)| left.is_zero())
                    .map(|(peer, _)| peer)
                    .collect();
                if !expired.is_empty() {
                    found.push(format!(
                        "ends maintenance holding expired {row} for {expired:?}"
                    ));
                }
            }
        }
        self.judge_repair(repair_sources, &mut found);
        for (row, peers) in [
            ("cached offers", cached_offers),
            ("supplier order", supplier_order),
        ] {
            let stray: Vec<&ZakuraPeerId> = peers.iter().filter(|peer| !admitted(peer)).collect();
            if !stray.is_empty() {
                found.push(format!(
                    "holds {row} for {stray:?}, which have no admitted session"
                ));
            }
        }
        let distinct: HashSet<&ZakuraPeerId> = supplier_order.iter().collect();
        if distinct.len() != supplier_order.len() {
            found.push(format!(
                "holds the supplier order {supplier_order:?}, with a peer more than once"
            ));
        }
        let active: HashMap<&ZakuraPeerId, u64> = self
            .slots
            .iter()
            .filter(|slot| slot.phase.is_some())
            .map(|slot| (&slot.peer, slot.incarnation))
            .collect();
        let leased: Vec<&ZakuraPeerId> = staged_capacity.iter().map(|(peer, _)| peer).collect();
        for (row, peers) in [
            (
                "request deadlines",
                request_deadlines.iter().collect::<Vec<_>>(),
            ),
            ("staged capacity", leased),
        ] {
            let stray: Vec<&ZakuraPeerId> = peers
                .into_iter()
                .filter(|peer| !active.contains_key(peer))
                .collect();
            if !stray.is_empty() {
                found.push(format!(
                    "holds {row} for {stray:?}, which hold no active slot"
                ));
            }
        }
        let phases: HashMap<&ZakuraPeerId, (u64, HeaderTargetPhase)> = self
            .slots
            .iter()
            .filter_map(|slot| Some((&slot.peer, (slot.incarnation, slot.phase?))))
            .collect();
        for (peer, headers) in staged_entries {
            let expected = match phases.get(peer) {
                Some((incarnation, HeaderTargetPhase::Receiving)) => self
                    .pages
                    .get(&(peer.clone(), *incarnation))
                    .map_or(0, |(_, staged)| *staged),
                Some(_) => 0,
                None => {
                    found.push(format!(
                        "holds {headers} staged entries for {peer:?}, which holds no active slot"
                    ));
                    continue;
                }
            };
            if *headers != expected {
                found.push(format!(
                    "holds {headers} staged entries for {peer:?}, whose round should hold \
                     {expected}"
                ));
            }
        }
        for (peer, leases) in staged_capacity {
            let Some(incarnation) = active.get(peer) else {
                continue;
            };
            let (staged, _) = self
                .pages
                .get(&(peer.clone(), *incarnation))
                .copied()
                .unwrap_or_default();
            if *leases != staged {
                found.push(format!(
                    "holds {leases} leases for {peer:?}, whose round {incarnation} staged \
                     {staged} pages"
                ));
            }
        }
        for (peer, reservation) in reservations {
            let backs = reservation.and_then(|id| self.reserved.get(&id));
            let current = active.get(peer).copied();
            if !matches!(backs, Some((owner, Some(round))) if owner == peer && Some(*round) == current)
            {
                found.push(format!(
                    "holds reservation {reservation:?} for {peer:?}, which backs {backs:?}, \
                     not its active round {current:?}"
                ));
            }
        }
        let repairs: HashSet<HeaderSyncWorkOwner> = self
            .slots
            .iter()
            .filter(|slot| slot.phase.is_some() && slot.repair)
            .filter_map(|slot| {
                self.rounds
                    .get(&(slot.peer.clone(), slot.incarnation))
                    .copied()
            })
            .collect();
        let stray: Vec<&HeaderSyncWorkOwner> = repair_episodes
            .iter()
            .filter(|owner| !repairs.contains(owner))
            .collect();
        if !stray.is_empty() {
            found.push(format!(
                "holds repair episodes for {stray:?}, which own no active repair round"
            ));
        }
        let stray: Vec<&(ZakuraPeerId, u64)> = locator_reads
            .iter()
            .filter(|(peer, call)| {
                !matches!(self.calls.get(call), Some(Open::Locator(asked)) if asked == peer)
            })
            .collect();
        if !stray.is_empty() {
            found.push(format!(
                "holds locator reads {stray:?} with no pending locator call for their peer"
            ));
        }
        let ended: Vec<u64> = storage_calls
            .iter()
            .filter(|call| !self.calls.contains_key(call))
            .copied()
            .collect();
        if !ended.is_empty() {
            found.push(format!(
                "holds storage calls {ended:?}, which ended or never dispatched"
            ));
        }
        let unheld: Vec<u64> = storage_calls
            .iter()
            .filter(|call| {
                matches!(self.calls.get(call), Some(Open::Prepare | Open::Apply))
                    && !self.holding.get(call).is_some_and(|headers| *headers > 0)
            })
            .copied()
            .collect();
        if !unheld.is_empty() {
            found.push(format!(
                "holds prepare or apply calls {unheld:?} that hold no headers"
            ));
        }
        let locators = storage_calls
            .iter()
            .filter(|call| matches!(self.calls.get(call), Some(Open::Locator(_))))
            .count();
        if locators > LOCATOR_PERMITS {
            found.push(format!(
                "holds {locators} open locator calls, past its {LOCATOR_PERMITS} permits"
            ));
        }
        let held: usize = storage_calls
            .iter()
            .filter_map(|call| self.holding.get(call))
            .fold(0, |total, headers| total.saturating_add(*headers));
        if held > CHUNK_HEADERS {
            found.push(format!(
                "holds prepare and apply calls holding {held} headers, past its {CHUNK_HEADERS}"
            ));
        }
        if *port_futures != storage_calls.len() {
            found.push(format!(
                "polls {port_futures} port futures for {} pending calls",
                storage_calls.len()
            ));
        }
        let unnamed: Vec<_> = completed_targets
            .iter()
            .filter(|key| !self.completed.contains(key))
            .collect();
        if !unnamed.is_empty() {
            found.push(format!(
                "holds completed targets {unnamed:?} that no TargetCompleted named"
            ));
        }
        self.unseen_tips.extend(
            completed_targets
                .iter()
                .map(|(_, branch)| branch.target_tip_hash)
                .filter(|tip| !self.held_tips.contains(tip)),
        );
        if let Some((generation, finalized)) = self.adopted {
            let stale: Vec<_> = completed_targets
                .iter()
                .filter(|(held, branch)| {
                    *held != generation || branch.anchor_hash != finalized.hash
                })
                .collect();
            if !stale.is_empty() {
                found.push(format!(
                    "holds completed targets {stale:?} outside the adopted generation \
                     {generation:?} and anchor {finalized:?}"
                ));
            }
        }
        for detail in found {
            self.breaks
                .push(format!("OutstandingWork: the reactor {detail}"));
        }
    }

    /// The breaks found since the last call, after looking up the
    /// completed targets' tips the `Sizes` since showed. Marks the rule
    /// applicable at `step` if a `Sizes` was judged since the last call.
    pub(super) fn drain(&mut self, step: u64) -> Vec<String> {
        if !self.unseen_tips.is_empty() {
            let unseen = std::mem::take(&mut self.unseen_tips);
            if let Some(held) = (self.held.0)() {
                for tip in unseen {
                    if held.contains(&tip) {
                        self.held_tips.insert(tip);
                    } else {
                        self.breaks.push(format!(
                            "OutstandingWork: the reactor holds a completed target for {tip:?}, \
                             which the node's engine doesn't hold"
                        ));
                    }
                }
            }
        }
        self.evidence.change(|store| {
            if std::mem::take(&mut store.pending) {
                store.applicable.get_or_insert(step);
            }
        });
        std::mem::take(&mut self.breaks)
    }
}

#[cfg(test)]
mod tests;
use crate::stores::{debug_snapshot, sorted_snapshot, EvidenceStores, Registered, Snapshot};
