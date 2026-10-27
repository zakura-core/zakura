//! What a run observed, step by step.
//!
//! Checkers read only the [`ObservationLog`]. Each entry carries the step
//! that appended it. Step 0 is setup, before the first pick.

use serde::Serialize;

use crate::model::Label;

/// One thing a step made happen.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "obs", rename_all = "kebab-case")]
#[non_exhaustive]
pub enum Observation {
    /// A task the run hadn't seen before became ready. A task spawned while
    /// every worker is busy shows up at the step that first offers it.
    TaskSpawned {
        /// The task's name.
        task: String,
    },
    /// A task stopped at a preemption point and stays ready.
    TaskReached {
        /// The task's name.
        task: String,
        /// The point, such as `lock#2`.
        at: String,
        /// The point's source location, as `file:line`.
        #[serde(skip_serializing_if = "Option::is_none")]
        site: Option<String>,
    },
    /// A task's poll returned `Pending`.
    TaskWaiting {
        /// The task's name.
        task: String,
    },
    /// A task parked on a synchronous wait, keeping its worker.
    TaskParked {
        /// The task's name.
        task: String,
    },
    /// A task ended.
    TaskExited {
        /// The task's name.
        task: String,
        /// How it ended.
        exit: Exit,
    },
    /// Virtual time moved.
    TimeAdvanced {
        /// The new time, in nanoseconds since the run's start.
        to_ns: u64,
        /// How many timers fired.
        fired: usize,
    },
    /// One fact a storage service published.
    Storage {
        /// The service, such as `header`.
        service: String,
        /// The storage instance's registration index in its run.
        instance: u64,
        /// The fact's sequence number, from 1 per service.
        seq: u64,
        /// Its transaction's number, from 1 per service.
        txn: u64,
        /// What changed.
        fact: StorageFact,
    },
    /// A storage service's owner state, read after the facts before it.
    StorageState {
        /// The service.
        service: String,
        /// The storage instance's registration index in its run.
        instance: u64,
        /// The last fact the state reflects.
        through_seq: u64,
        /// The state.
        state: StorageState,
    },
    /// One event a header-sync reactor reported through its tap, in the
    /// order the reactor reported it.
    Reactor {
        /// The tap's registration index among the run's taps, an index space
        /// apart from storage's.
        instance: u64,
        /// The event's number, from 1 per tap.
        seq: u64,
        /// The last fact the node's storage had published when the event
        /// happened, 0 before any.
        storage_seq: u64,
        /// The node's name, a display label only.
        node: String,
        /// What happened.
        event: ReactorFact,
    },
    /// A peer sent a frame on a tabled stream. The peer's role models
    /// labelled it when it was sent.
    PeerFrame {
        /// The connection.
        connection: u64,
        /// The stream.
        stream: StreamId,
        /// The frame's position among the peer's frames on this stream,
        /// from 0.
        seq: u64,
        /// The row whose rule `label` enforces, as [`crate::table::rule_row`]
        /// picks it. `None` for a type the stream doesn't declare. A declared
        /// type sent to the wrong reader keeps its row, labelled
        /// `UndeclaredType`.
        row: Option<String>,
        /// The stream whose table holds `row`: `stream`, except where
        /// [`crate::table::rule_row`] picks a row on another stream of the
        /// layout.
        rule_stream: StreamId,
        /// The frame's label.
        label: Label,
        /// True if charging the frame to its row's cadence bucket found it
        /// empty: [`crate::model::CadenceModel`]'s charge returned
        /// [`crate::model::Charge::Exhausted`]. Such a frame is still
        /// conformant.
        exhausted: bool,
        /// The exchange or subscription the frame belongs to, as the peer's
        /// role models key it: a request's own key, or the key a response,
        /// page or update names. `None` for an announcement or a frame with
        /// no key.
        exchange: Option<u64>,
        /// The payload's length in bytes, frame header excluded.
        payload: u64,
    },
    /// A frame the node sent on a tabled stream reached a peer, whose role
    /// models labelled it on arrival under the sender's rules: `OverMargin`
    /// once the node's open requests exceed the highest limit the peer
    /// advertised, and `OverLive` once the node's live subscriptions exceed
    /// `max_live`, without the receiver's margin of twice those. A model
    /// input, like [`Observation::CreditGranted`].
    NodeFrame {
        /// The connection.
        connection: u64,
        /// The stream.
        stream: StreamId,
        /// The frame's position among the node's frames on this stream,
        /// from 0.
        seq: u64,
        /// The row whose rule `label` enforces, as for
        /// [`Observation::PeerFrame`].
        row: Option<String>,
        /// The stream whose table holds `row`.
        rule_stream: StreamId,
        /// The frame's label.
        label: Label,
        /// True if the peer's [`crate::model::CadenceModel`] charged the
        /// frame to an empty bucket: its charge returned
        /// [`crate::model::Charge::Exhausted`].
        exhausted: bool,
    },
    /// The node's reader reached the bytes it needs to judge the peer's
    /// frame `seq`. A frame the reader hasn't reached has no judgement.
    FrameJudged {
        /// The connection.
        connection: u64,
        /// The stream.
        stream: StreamId,
        /// The frame, as its [`Observation::PeerFrame`] numbers it.
        seq: u64,
    },
    /// The node decided to close a connection.
    ConnectionClosed {
        /// The connection.
        connection: u64,
        /// The first cause the node recorded, such as
        /// `service_protocol_reject`.
        cause: String,
    },
    /// The node asked storage to punish the peer on a connection.
    Misbehavior {
        /// The connection.
        connection: u64,
    },
    /// The node granted credit on a subscription it opened, as the peer's
    /// subscription model applied the update. A model input: the peer's
    /// harness emits it, never the node's.
    CreditGranted {
        /// The connection.
        connection: u64,
        /// The subscription row.
        subscription: RowAt,
        /// The subscription's key.
        exchange: u64,
        /// The objects the update added.
        objects: u64,
        /// The payload bytes the update added.
        bytes: u64,
        /// True if the update opened the subscription. A key the peer's
        /// model has forgotten may open again, and its credit starts over.
        opens: bool,
    },
    /// The peer opened an exchange with the node, owed this much, as the
    /// peer's exchange model recorded it. A model input, like
    /// [`Observation::CreditGranted`].
    ExchangeOwed {
        /// The connection.
        connection: u64,
        /// The request row.
        request: RowAt,
        /// The exchange's key.
        exchange: u64,
        /// Frames the node may send before the exchange's ending.
        frames: u32,
        /// Payload bytes the node may send, the ending's included.
        bytes: u64,
    },
    /// The node retained, forwarded or handed storage work from one of the
    /// peer's response or page frames: an actual effect, which the family's
    /// adapter reads at the storage or forwarding boundary, whatever code
    /// path produced it. Work counts once, at the first boundary it crosses:
    /// an adapter that sees the same work retained and then forwarded
    /// reports it once.
    WorkAdmitted {
        /// The connection.
        connection: u64,
        /// The row of the frame the work came from.
        frame: RowAt,
        /// That frame, as its [`Observation::PeerFrame`] numbers it on
        /// `frame`'s stream: the frame whose body the work holds.
        seq: u64,
        /// The request or subscription row those frames answer.
        request: RowAt,
        /// The exchange or subscription key.
        exchange: u64,
        /// Objects, such as headers or blocks.
        objects: u64,
        /// Payload bytes.
        bytes: u64,
        /// True if `frame`'s row ends the exchange, as a subscription's
        /// terminal outcome does.
        ends: bool,
    },
    /// The node installed an owner for serving the peer's request: an actual
    /// effect, read where the owner is created.
    Serving {
        /// The connection.
        connection: u64,
        /// The request row.
        request: RowAt,
        /// The request's key.
        exchange: u64,
    },
    /// A serving owner from [`Observation::Serving`] retired: an actual
    /// effect, read where the owner is dropped.
    ServingEnded {
        /// The connection.
        connection: u64,
        /// The request row.
        request: RowAt,
        /// The request's key.
        exchange: u64,
    },
    /// The node wrote a response frame to the peer: an actual effect, read
    /// at the writer.
    NodeOutput {
        /// The connection.
        connection: u64,
        /// The frame's row.
        frame: RowAt,
        /// The request row it answers.
        request: RowAt,
        /// The exchange's key.
        exchange: u64,
        /// The payload's length in bytes.
        payload: u64,
        /// True if the row ends the exchange.
        ends: bool,
    },
    /// An event from a test-only fake action source.
    Fake {
        /// The source's name.
        source: String,
        /// What happened.
        event: String,
    },
}

/// A storage fact in plain fields: hashes and peers in hex, counters as
/// numbers, and names where the port has them.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "fact", rename_all = "kebab-case")]
pub enum StorageFact {
    /// A call's first poll submitted operation `id`.
    Submitted {
        /// The operation's ID.
        id: u64,
        /// What it asks for.
        request: StorageRequest,
        /// A digest of the request.
        digest: String,
    },
    /// The caller dropped its call while the operation was pending.
    CallerDropped {
        /// The operation.
        id: u64,
    },
    /// The caller took its reply.
    ReplyTaken {
        /// The operation.
        id: u64,
    },
    /// The caller dropped its delivered reply without taking it.
    ReplyDropped {
        /// The operation.
        id: u64,
    },
    /// The run completed the operation.
    Completed {
        /// The operation.
        id: u64,
        /// The outcome, as in its action identity.
        outcome: String,
        /// What it returned.
        result: StorageResult,
        /// Whether the reply reached a waiting caller.
        delivered: bool,
        /// The engine's header generation after it.
        generation: u64,
        /// A digest of the service's state after it.
        state: String,
    },
    /// A locator's permit moved into storage.
    PermitTaken {
        /// The operation.
        id: u64,
    },
    /// Storage's locator permit left it.
    PermitDropped {
        /// The operation.
        id: u64,
    },
    /// A lease began.
    Leased {
        /// The lease.
        lease: StorageLease,
        /// Its work scope.
        scope: WorkScope,
        /// Its target.
        target: StoragePoint,
        /// Where its next page starts.
        common_ancestor: StoragePoint,
        /// The cursor's index into its retained hashes, or `None` once every
        /// header is served.
        next: Option<u64>,
        /// Its slot class, `general` or `bounded`.
        capacity: String,
        /// The cached lease it continued from.
        #[serde(skip_serializing_if = "Option::is_none")]
        continued_from: Option<u64>,
    },
    /// A lease's handle stopped working.
    CapabilityEnded {
        /// The lease.
        lease: StorageLease,
        /// Why, such as `lease-released`.
        release: String,
    },
    /// A consumed lease's path moved into the continuation cache.
    RetentionCached {
        /// The lease.
        lease: StorageLease,
        /// The last header served.
        last: StoragePoint,
        /// The cursor's index into its retained hashes, or `None` once every
        /// header is served.
        next: Option<u64>,
    },
    /// A retained path stopped being held.
    RetentionEnded {
        /// The lease the path belonged to.
        lease: StorageLease,
        /// Why, such as `with-capability` or `continued`.
        why: String,
        /// For `continued`, the new lease's ID.
        #[serde(skip_serializing_if = "Option::is_none")]
        into: Option<u64>,
    },
    /// A refusal set a capacity signal.
    WaiterSet {
        /// The signal's class, `peer`, `general` or `fallback`.
        class: String,
        /// For `peer`, the lease it waits on.
        #[serde(skip_serializing_if = "Option::is_none")]
        lease: Option<StorageLease>,
    },
    /// A capacity signal fired.
    WaiterFired {
        /// The signal's class.
        class: String,
        /// For `peer`, the lease it waited on.
        #[serde(skip_serializing_if = "Option::is_none")]
        lease: Option<StorageLease>,
    },
    /// The service shut down, dropping these operations.
    ShutDown {
        /// The operations dropped.
        dropped: Vec<u64>,
    },
}

/// A work owner with every field of its typed form.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct WorkOwner {
    /// `header` or `body-repair`.
    pub kind: String,
    /// The scope it works in.
    #[serde(flatten)]
    pub scope: WorkScope,
    /// A body owner's verified generation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verified_generation: Option<u64>,
    /// A body owner's body work epoch.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body_work_epoch: Option<u64>,
    /// Its session.
    pub session_id: u64,
    /// Its request.
    pub request_id: u64,
}

/// A header work scope: a generation and the branch it pursues.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct WorkScope {
    /// The header generation.
    pub header_generation: u64,
    /// The branch.
    pub branch: StorageBranch,
}

/// A branch, by its anchor and target hashes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StorageBranch {
    /// The anchor's hash.
    pub anchor: String,
    /// The target's hash.
    pub target: String,
}

/// A header's height and hash.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StoragePoint {
    /// The height.
    pub height: u32,
    /// The hash.
    pub hash: String,
}

/// A lease's peer, session and ID.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StorageLease {
    /// The peer.
    pub peer: String,
    /// The peer's session.
    pub session_id: u64,
    /// The lease ID.
    pub lease_id: u64,
}

/// What an operation asked for.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum StorageRequest {
    /// `continuation_locator`.
    ContinuationLocator,
    /// `vct_repair_context`.
    VctRepairContext {
        /// The repair owner.
        owner: WorkOwner,
        /// The repaired height.
        height: u32,
    },
    /// `acquire_header_path`.
    AcquireHeaderPath {
        /// The serving peer.
        peer: String,
        /// Its session.
        session_id: u64,
        /// The lease's scope.
        scope: WorkScope,
        /// The target.
        target: String,
        /// The locator, nearest first.
        locators: Vec<String>,
    },
    /// `read_header_path`.
    ReadHeaderPath {
        /// The lease the handle names, if this port issued it.
        lease: Option<StorageLease>,
        /// The handle's scope.
        scope: WorkScope,
        /// Where the page starts.
        after: String,
        /// The most headers asked for.
        max_headers: u32,
    },
    /// `release_header_path`.
    ReleaseHeaderPath {
        /// The lease the handle names, if this port issued it.
        lease: Option<StorageLease>,
        /// The handle's scope.
        scope: WorkScope,
    },
    /// `prepare_header_target`.
    PrepareHeaderTarget {
        /// The work owner.
        owner: WorkOwner,
        /// The supplier.
        source: String,
        /// The parent of the first header.
        common_ancestor: StoragePoint,
        /// The last header.
        target: StoragePoint,
        /// How many headers.
        headers: u32,
    },
    /// `apply_header_target`.
    ApplyHeaderTarget {
        /// The work owner.
        owner: WorkOwner,
        /// The supplier.
        source: String,
        /// The parent, if this port sealed the target.
        parent: Option<String>,
        /// The target.
        target: String,
    },
    /// `selected_chain_contains`.
    SelectedChainContains {
        /// The header asked about.
        hash: String,
    },
}

/// What an operation returned.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "result", rename_all = "kebab-case")]
pub enum StorageResult {
    /// The run chose a fault, such as `timeout`.
    Fault {
        /// The fault.
        fault: String,
    },
    /// The adapter rejected the request.
    Rejected,
    /// The engine path returned an error.
    PortError,
    /// A locator.
    Locator {
        /// Its first entry, the selected tip.
        tip: StoragePoint,
        /// The header generation it was read at.
        header_generation: u64,
        /// The finalized anchor it was read at.
        finalized: StoragePoint,
    },
    /// No locator.
    NoLocator,
    /// A resolved VCT repair context.
    VctResolved,
    /// A stale VCT repair context.
    VctStale,
    /// A leased path.
    Acquired {
        /// The lease.
        lease: StorageLease,
        /// Where its path starts.
        common_ancestor: StoragePoint,
        /// Its target.
        target: StoragePoint,
    },
    /// `CapacityBusy`, with its signal's class.
    CapacityBusy {
        /// `peer`, `general` or `fallback`.
        class: String,
    },
    /// `Busy`.
    Busy,
    /// `TargetNotRetained`.
    TargetNotRetained,
    /// `NoLocatorIntersection`.
    NoLocatorIntersection,
    /// `HistoryPruned`.
    HistoryPruned,
    /// A served page.
    Page {
        /// The last header served, or the page's start.
        last: StoragePoint,
        /// How many headers.
        count: u32,
        /// Whether it reached the target.
        complete: bool,
    },
    /// The read's `Unavailable`.
    PageUnavailable,
    /// A release.
    Released,
    /// A sealed target.
    Prepared,
    /// A failed preparation, by error category.
    PrepareFailed {
        /// The category.
        category: String,
    },
    /// `Applied`.
    Applied,
    /// `ResourceStalled`.
    ResourceStalled,
    /// A failed apply, by error category.
    ApplyFailed {
        /// The category.
        category: String,
    },
    /// On the selected chain.
    Selected {
        /// Where.
        at: StoragePoint,
    },
    /// Retained but not selected.
    NotSelected,
    /// Not retained.
    NotRetained,
}

/// A storage service's actual owner state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StorageState {
    /// Every operation not yet taken or dropped.
    pub operations: Vec<StorageOperation>,
    /// The operations whose permit storage holds.
    pub permits: Vec<u64>,
    /// Every live lease.
    pub leases: Vec<StorageLeaseState>,
    /// Every cached continuation.
    pub continuations: Vec<StorageContinuation>,
    /// Whether the ordinary class has a waiting signal.
    pub general_waiter: bool,
    /// Whether the reserve class has a waiting signal.
    pub fallback_waiter: bool,
    /// The last lease ID issued.
    pub last_lease_id: u64,
    /// How many acquires continued from a cached path.
    pub continuation_hits: u64,
}

/// One operation in a service's bookkeeping.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StorageOperation {
    /// Its ID.
    pub id: u64,
    /// What it asked for.
    pub request: StorageRequest,
    /// Where it is: `pending`, `running` or `replied`.
    pub stage: String,
    /// For a pending operation, whether the port holds its caller's waker.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub caller_waiting: Option<bool>,
    /// For a replied operation, the stored reply.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<StorageResult>,
    /// Whether the caller's call still exists.
    pub caller_alive: bool,
}

/// One live lease.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StorageLeaseState {
    /// The lease.
    pub lease: StorageLease,
    /// Its scope.
    pub scope: WorkScope,
    /// Its target.
    pub target: StoragePoint,
    /// The last header served, or where its path starts.
    pub last: StoragePoint,
    /// The cursor's index into its retained hashes, or `None` once every
    /// header is served.
    pub next: Option<u64>,
    /// Its slot class.
    pub capacity: String,
    /// Whether its peer has a refused acquire waiting on it.
    pub waiter: bool,
    /// The transaction that created it, if a controlled one did.
    pub leased_txn: Option<u64>,
}

/// One cached continuation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StorageContinuation {
    /// The consumed lease.
    pub lease: StorageLease,
    /// Its scope.
    pub scope: WorkScope,
    /// Its target.
    pub target: StoragePoint,
    /// The last header served.
    pub last: StoragePoint,
    /// The cursor's index into its retained hashes, or `None` once every
    /// header is served.
    pub next: Option<u64>,
    /// How many hashes its path retains.
    pub retained: u64,
}

/// A header-sync reactor event in plain fields: peers, sources and hashes
/// in hex, owners as storage writes them, and names for every enum.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "event", rename_all = "kebab-case")]
pub enum ReactorFact {
    /// The reactor pushed a port call's future.
    Dispatched {
        /// The call's number, from 1 per reactor.
        call: u64,
        /// What the call asks storage for.
        port: ReactorCall,
    },
    /// A port call's future ended.
    CallEnded {
        /// The call's number.
        call: u64,
        /// How it ended: `replied`, `port-error`, `timed-out`,
        /// `not-submitted` or `port-panicked`.
        end: String,
        /// The storage operation the call is paired with. The tap doesn't
        /// pair calls, so it always logs `None`.
        op: Option<u64>,
    },
    /// The reactor finished processing a call's completion.
    Handled {
        /// The call's number.
        call: u64,
        /// Whether the call's future was caught panicking.
        panicked: bool,
    },
    /// An active request started.
    Started {
        /// The request.
        request: ReactorRequest,
        /// Why it started.
        purpose: ReactorPurpose,
    },
    /// An active request was retired.
    Retired {
        /// The request.
        request: ReactorRequest,
        /// Its phase then: `receiving`, `preparing` or `applying`.
        phase: String,
        /// Why it started.
        purpose: ReactorPurpose,
        /// Why it was retired, as the reactor's traces name it, such as
        /// `snapshot_obsolete`.
        reason: String,
    },
    /// An offer was refused for local capacity and stays unspent.
    OfferWaiting {
        /// The offering peer.
        peer: String,
        /// The offering session.
        session_id: u64,
        /// The offered target.
        target: String,
        /// Which capacity refused it: `staging-full` or `locator-capacity`.
        cause: String,
    },
}

/// What a dispatched port call asks storage for.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ReactorCall {
    /// A continuation locator read.
    Locator {
        /// The peer whose offer needs it.
        peer: String,
        /// The offering session.
        session_id: u64,
        /// The offered target.
        target: String,
        /// The header authority it was read under.
        scope: WorkScope,
    },
    /// A VCT repair context read.
    VctContext {
        /// The repair's owner.
        owner: WorkOwner,
        /// The height that needs repair.
        height: u32,
    },
    /// A target preparation.
    Prepare {
        /// The supplying peer.
        peer: String,
        /// The supplier's source identity.
        source: String,
        /// The request that owns the target.
        owner: WorkOwner,
    },
    /// A prepared target's application.
    Apply {
        /// The supplying peer.
        peer: String,
        /// The supplier's source identity.
        source: String,
        /// The request that owns the target.
        owner: WorkOwner,
    },
}

/// An active request's identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ReactorRequest {
    /// The peer whose session owns it.
    pub peer: String,
    /// The owning session.
    pub session_id: u64,
    /// Its correlation ID.
    pub request_id: u64,
    /// Its work owner.
    pub owner: WorkOwner,
    /// The advertised target it pursues.
    pub target: String,
}

/// Why an active request started.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "purpose", rename_all = "kebab-case")]
pub enum ReactorPurpose {
    /// To admit a complete branch target.
    Normal,
    /// To redeliver auxiliary metadata for a selected range.
    SelectedAuxiliaryRepair {
        /// The selected target the repair context fixes.
        selected_target: StoragePoint,
        /// The repair-signal generation that owns the attempt.
        repair_generation: u64,
    },
}

/// The events that reactor tap `instance` logged in `observations`, in
/// order.
pub(crate) fn reactor_entries<'o>(
    observations: impl IntoIterator<Item = &'o Observation>,
    instance: u64,
) -> Vec<&'o Observation> {
    observations
        .into_iter()
        .filter(|obs| {
            matches!(
                obs,
                Observation::Reactor { instance: index, .. } if *index == instance
            )
        })
        .collect()
}

/// The facts and states that storage `instance` of `service` logged in
/// `observations`, in order.
pub(crate) fn storage_entries<'o>(
    observations: impl IntoIterator<Item = &'o Observation>,
    service: &str,
    instance: u64,
) -> Vec<&'o Observation> {
    observations
        .into_iter()
        .filter(|obs| match obs {
            Observation::Storage {
                service: logged,
                instance: index,
                ..
            }
            | Observation::StorageState {
                service: logged,
                instance: index,
                ..
            } => logged == service && *index == instance,
            _ => false,
        })
        .collect()
}

/// A stream kind at one version, as property IDs write it: `s5v9`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct StreamId {
    /// The stream kind.
    pub kind: u16,
    /// The stream version.
    pub version: u16,
}

impl std::fmt::Display for StreamId {
    /// Writes `s<kind>v<version>`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "s{}v{}", self.kind, self.version)
    }
}

/// A row of a stream's message table.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct RowAt {
    /// The stream.
    pub stream: StreamId,
    /// The row's name.
    pub row: String,
}

impl RowAt {
    /// Row `row` of `stream`.
    pub fn new(stream: StreamId, row: impl Into<String>) -> Self {
        RowAt {
            stream,
            row: row.into(),
        }
    }
}

impl std::fmt::Display for RowAt {
    /// Writes `s<kind>v<version>/<row>`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.stream, self.row)
    }
}

/// How a task ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Exit {
    /// The future returned `Ready`.
    Completed,
    /// The task was aborted.
    Cancelled,
    /// The task panicked.
    Panicked,
}

/// An observation and the step that appended it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    /// The step, from 1. Step 0 is setup.
    pub step: u64,
    /// What happened.
    pub obs: Observation,
}

/// Every observation of a run, in order.
#[derive(Clone, Debug, Default)]
pub struct ObservationLog {
    entries: Vec<Entry>,
    step: u64,
    frozen: bool,
    /// The run the log belongs to, 0 for one no run made.
    run: u64,
}

impl ObservationLog {
    /// Appends `obs` to the current step. Does nothing once the log is
    /// frozen, so cleanup at shutdown leaves no trace in it.
    pub fn push(&mut self, obs: Observation) {
        if !self.frozen {
            self.entries.push(Entry {
                step: self.step,
                obs,
            });
        }
    }

    /// Every entry, in order.
    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }

    /// The observations `step` appended.
    pub fn of_step(&self, step: u64) -> impl Iterator<Item = &Observation> {
        // Entries are appended in step order.
        let start = self.entries.partition_point(|e| e.step < step);
        self.entries[start..]
            .iter()
            .take_while(move |e| e.step == step)
            .map(|e| &e.obs)
    }

    /// The step being appended to, 0 during setup.
    pub fn step(&self) -> u64 {
        self.step
    }

    /// The run the log belongs to, 0 for one no run made.
    pub(crate) fn run(&self) -> u64 {
        self.run
    }

    /// Marks the log as `run`'s.
    pub(crate) fn set_run(&mut self, run: u64) {
        self.run = run;
    }

    /// Starts appending to `step`.
    pub(crate) fn begin(&mut self, step: u64) {
        self.step = step;
    }

    /// Stops recording. Live observations end before cleanup starts.
    pub(crate) fn freeze(&mut self) {
        self.frozen = true;
    }
}
