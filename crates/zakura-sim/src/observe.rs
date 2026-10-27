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
        /// True if the frame emptied its row's cadence bucket. Such a frame
        /// is still conformant.
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
