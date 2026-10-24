//! The header storage contract: what a simulated header-chain [`Port`] may do.
//!
//! The simulator implements [`Port`] over the real header-chain engine. Each
//! call becomes a pending operation, and the run completes it later with one
//! [`Outcome`]. This module says which outcomes each operation may have, and
//! when. It also names the deadlines around each operation and the events that
//! release what it owns. It mirrors production at the revision it was written
//! against, and the source references below name the code it follows.
//!
//! [`Port`]: zakura_node_services::header_chain::Port
//!
//! # Operations
//!
//! [`HeaderOp`] lists the eight asynchronous [`Port`] methods. `selected_chain`
//! isn't one of them. It returns a watch receiver synchronously, so it has no
//! pending state and no faults. In header scenarios only an apply's
//! completion sends a new snapshot on it, and receivers may see several
//! commits coalesced into one change. The reactor's `Misbehavior` and `DropPeer` actions aren't storage
//! calls either. The direct dispatch path logs them and returns.
//!
//! | Operation | Production caller | Engine results | Faults |
//! |:--|:--|:--|:--|
//! | `continuation_locator` | reactor, `QueryHeaderLocator` | `Some(locator)`, or `None` before handoff | `unavailable`, `timeout` after 30 s |
//! | `vct_repair_context` | reactor, `QueryVctRepairContext` | `Resolved`, `Stale` | `unavailable`, `timeout` after 30 s |
//! | `acquire_header_path` | v9 publisher page job | `Acquired`, `TargetNotRetained`, `NoLocatorIntersection`, `HistoryPruned`, `CapacityBusy` or `Busy` per [`AcquireRefusal`]. An empty or oversized locator gives `Err(Unavailable)` | `unavailable`, `timeout` after 30 s |
//! | `read_header_path` | v9 publisher page job | `Page`, `Unavailable`. A foreign path or an out-of-bounds page count gives `Err(Unavailable)` | `unavailable`, `timeout` after 30 s |
//! | `release_header_path` | v9 publisher page job | `Ok(())`, also on repeats. A foreign path gives `Err(Unavailable)` | `unavailable`, `timeout` after 30 s |
//! | `prepare_header_target` | reactor, `PrepareHeaderTarget` | a sealed target, `stale_target`, `unknown_anchor`, validation and body-size errors | `local-resource` |
//! | `apply_header_target` | reactor, `ApplyHeaderTarget` | `Applied`, `ResourceStalled`, `stale_target`, `invalid_header`, `auxiliary_capacity`, `local_resource`. A target another adapter sealed gets only `stale_target` | `local-resource` |
//! | `selected_chain_contains` | v9 publisher | `Selected`, `NotSelected`, `NotRetained` | `unavailable`, `timeout` after 30 s |
//!
//! # Outcomes
//!
//! An [`Outcome`] or [`HeaderReply`] can be built freely, so the contract only
//! holds if ST3 calls [`HeaderRequest::check`] at its completion boundary,
//! before it runs or replies.
//!
//! Input checks the adapter runs itself come first. A request that fails one,
//! such as a read with another adapter's path handle or an apply of a target
//! another adapter sealed, gets the adapter's rejection before any state call
//! or deadline starts. So its only legal
//! outcome is [`Outcome::Rejected`], and [`HeaderRequest::enabled`] offers no
//! engine result or fault for it. Checks that state runs, like the locator and
//! page counts, run under the adapter's deadline, so they are engine results.
//!
//! [`Outcome::Engine`] runs the operation against the engine and returns what
//! it computes. Every engine result in the table comes only from the engine.
//! For example, the run can't make a still-current target `stale_target`.
//!
//! [`Outcome::Fault`] returns a typed fault that the production adapter can
//! also return without engine state deciding it, through
//! [`HeaderRequest::fault_reply`]:
//!
//! * `unavailable` is `PortError::Unavailable`, as when the state service
//!   fails. It's legal at any time.
//! * `timeout` is `PortError::Timeout`. The production adapter gives it only
//!   when a state read outlives [`ADAPTER_DEADLINE`], so it's legal only once
//!   that long has passed since submission.
//! * `local-resource` is `HeaderChainError::local_resource` for the request
//!   owner's branch. It's prepare's and apply's only fault. The adapter makes
//!   it when a state call fails or times out, or the writer closes, so no
//!   engine decision is needed. It's legal at any time. For apply it means
//!   nothing was committed.
//!
//! A fault means the backing work never ran, so it has no effect on state.
//!
//! # Excluded histories
//!
//! Each operation's production stages run atomically at its completion.
//! Production runs some stages earlier, and other operations can see their
//! effects before the reply. Any production history that depends on another
//! operation seeing such an intermediate stage is excluded, as is the API
//! draft's Ambiguous outcome. Covering them would need an operation split
//! into separate execute and reply stages. The known instances:
//!
//! * A reservation held while an acquire resolves its path. Say acquire A
//!   reserves its peer's slot at 1 s. Acquire B, from the same peer, gets
//!   `CapacityBusy` at 2 s. Then A replies `NoLocatorIntersection` at 3 s and
//!   fires B's signal. Under atomic completion A holds no reservation before
//!   it completes, so B can't be refused this way.
//! * A lease consumed long before a late successful reply. Say the lease is
//!   acquired at 0 s, the path read consumes it at 1 s and frees its slot,
//!   and the body-size read times out at 31 s. Production still replies
//!   `Page` at 31 s. A single completion at 31 s would find the lease expired
//!   and reply `Unavailable`, and would never free the slot at 1 s.
//! * A lease consumed before a tree-root read fails, so the read replies
//!   `timeout` or `unavailable` with its lease already gone.
//! * Work that ran anyway after a fault. A state read keeps running after its
//!   adapter times out, so a read can time out at 30 s and still consume its
//!   lease a moment later, and a release can time out after releasing.
//! * A commit visible before its apply replies. The writer commits and
//!   publishes the snapshot, then sends the reply, so production readers can
//!   see the commit in between. Here both happen in one completion. Readers
//!   still see the commit before the caller handles its reply, but no other
//!   storage operation completes between the publish and the reply.
//!
//! ST3's single-completion model covers none of these. ST3 treats any new
//! history of this shape as covered by this rule. It needs no contract
//! change.
//!
//! The body-size fallback needs no outcome of its own. Header fixtures commit
//! no block bodies, so the engine adapter serves every size as unknown, which
//! is the same page the fallback gives. Only its timing is lost, as the
//! second instance shows.
//!
//! # Execution
//!
//! Every operation runs in the one completion action the run picks, which is
//! its linearization point. Reads capture committed state there. An acquire
//! creates its lease, a read consumes it and a release drops it there. An
//! apply commits and publishes its snapshot there, before its reply.
//!
//! Caller cancellation never stops accepted work. A reactor timeout drops its
//! future, and a publisher page job outlives its session, but production state
//! still runs the read on a blocking thread or the apply on the writer. So a
//! pending operation keeps its inputs and owned capacity until it completes,
//! and only its reply receiver goes absent. A completion with no receiver
//! drops its reply and still counts as done. Only service shutdown removes
//! pending operations, and a dropped reply never rolls back a commit.
//!
//! # Deadlines
//!
//! [`Deadline`] keeps three kinds apart:
//!
//! * The reactor wraps only the locator and VCT-context calls in its request
//!   timeout ([`REACTOR_REQUEST_TIMEOUT`] by default). When it fires the
//!   reactor drops its future and treats the result as missing. The operation
//!   stays pending.
//! * The production adapter wraps each state read in its own fresh
//!   [`ADAPTER_DEADLINE`], counted from that read's start. That bounds the
//!   whole operation for the locator, VCT context, acquire, membership and
//!   release calls, which make one read each. A page read makes up to five
//!   reads, in order:
//!   1. The path read, which consumes the lease when it returns a page.
//!   2. The committed body sizes of a nonempty page's headers. If this read
//!      fails or times out, the reply is still a successful `Page` with every
//!      size unknown.
//!   3. The finalized tip, when the caller asked for tree roots.
//!   4. The selected hash at the page's last finalized height, when the page
//!      reaches the finalized range.
//!   5. That finalized range's roots, when the hash matches the page.
//!
//!   A failure or timeout in reads 3 to 5 fails the page read with `timeout`
//!   or `unavailable`. Since each read has its own deadline, a page read whose
//!   five reads each take 20 s succeeds after 100 s.
//!
//!   For prepare it bounds only the validation-lease read, and the result is
//!   `local-resource`. Apply has no adapter deadline. Its 30 s loop only logs
//!   diagnostics.
//! * The 30 min VCT repair deadline ([`VCT_REPAIR_WATCHDOG`]) is a fatal
//!   watchdog, not a timeout. It covers prepare and apply for a selected
//!   auxiliary repair. When it passes the reactor reports a fatal event and the
//!   node shuts down. The operation is never cancelled, and it produces no
//!   reply.
//!
//! # Releases
//!
//! [`Release`] names each event that ends owned capacity. A locator's permit
//! ends when its operation finishes, not when its caller times out. A
//! retained-path lease ends when one of four things happens:
//!
//! * a successful read consumes it
//! * an explicit release drops it
//! * an `Acquired` reply nobody receives drops it
//! * its 30 s idle deadline passes, and some later acquire or read sweeps it
//!
//! The sweep isn't per peer. Each acquire and read whose [`InputCheck`]s
//! pass first ends every lease past its idle deadline, whoever holds it, and
//! fires the waiting capacity signals. One that fails an input check returns
//! `Err(Unavailable)` before reaching the lease registry, so it sweeps
//! nothing. So if peer A leaves a lease
//! idle for 30 s, peer B's next acquire ends it and can take the freed slot.
//! A then sees `Unavailable` on its next read. Shutdown ends everything.
//!
//! A peer holds at most one lease. An acquire while that peer's lease or
//! earlier acquire is still live gets `CapacityBusy`, whose signal fires when
//! that lease ends, so the publisher waits and retries. `Busy` is a different
//! refusal that sends the publisher to a local stall. [`AcquireRefusal`] maps
//! each refusal to its reply.
//!
//! # Source references
//!
//! * `zakura-network`: `HeaderSyncReactor::dispatch_direct_port_operation`
//!   and `VctLocalOperation` in `header_sync/reactor.rs`, and `run_job` and
//!   `start_page` in `header_sync/v9/publisher.rs`.
//! * `zakurad`: `HeaderChainServicePort` and `ZAKURA_HEADER_SYNC_DRIVER_TIMEOUT`
//!   in `commands/start/zakura`.
//! * `zakura-state`: `PendingRetainedPathAcquisition`,
//!   `RetainedPathLeaseRegistry` and `RETAINED_PATH_LEASE_IDLE` in
//!   `service/finalized_state/header_chain.rs`.

use std::{fmt, sync::Arc, time::Duration};

use zakura_chain::block;
use zakura_header_chain::{
    BodyWorkOwner, ErrorSubject, HeaderChainError, HeaderLocator, HeaderSyncWorkOwner,
};
use zakura_node_services::header_chain::{
    AcquirePath, AcquirePathReply, AdapterKey, ApplyHeaderTargetReply, LocatorReadPermit,
    PortError, PrepareHeaderTarget, PrepareHeaderTargetReply, PreparedHeaderTarget, ReadPath,
    ReadPathReply, RetainedHeaderPath, SelectedChainMembership, VctRepairContextReply,
};

pub mod adapter;
pub mod fixture;
#[cfg(test)]
mod tests;

/// The production adapter's deadline on each state read, which
/// `ZAKURA_HEADER_SYNC_DRIVER_TIMEOUT` sets in `zakurad`.
pub const ADAPTER_DEADLINE: Duration = Duration::from_secs(30);

/// The reactor's default request timeout around the locator and VCT-context
/// calls. A node can configure another value.
pub const REACTOR_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// The reactor's fatal watchdog on prepare and apply for a VCT repair,
/// `VCT_LOCAL_OPERATION_FATAL_AFTER` in `zakura-network`.
pub const VCT_REPAIR_WATCHDOG: Duration = Duration::from_secs(30 * 60);

/// One asynchronous [`Port`](zakura_node_services::header_chain::Port)
/// operation. Variants follow the trait's method order, which is also the
/// canonical order.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum HeaderOp {
    /// `continuation_locator`.
    ContinuationLocator,
    /// `vct_repair_context`.
    VctRepairContext,
    /// `acquire_header_path`.
    AcquireHeaderPath,
    /// `read_header_path`.
    ReadHeaderPath,
    /// `release_header_path`.
    ReleaseHeaderPath,
    /// `prepare_header_target`.
    PrepareHeaderTarget,
    /// `apply_header_target`.
    ApplyHeaderTarget,
    /// `selected_chain_contains`.
    SelectedChainContains,
}

/// How a pending operation completes.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Outcome {
    /// Run the operation against the engine and return its result.
    Engine,
    /// Return a typed fault without running the operation.
    Fault(Fault),
    /// Return the adapter's rejection of a request that failed one of its
    /// own input checks. The only legal outcome for such a request.
    Rejected,
}

/// A fault the run may choose instead of running an operation. The module
/// doc says what each one returns.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Fault {
    /// `PortError::Timeout`.
    Timeout,
    /// `PortError::Unavailable` with no source.
    Unavailable,
    /// `HeaderChainError::local_resource` for the request owner's branch.
    LocalResource,
}

/// When a fault becomes legal.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum FaultGate {
    /// From submission on.
    Any,
    /// Once [`ADAPTER_DEADLINE`] has passed since submission.
    AfterAdapterDeadline,
}

/// The state read an adapter deadline bounds.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum AdapterStage {
    /// The operation's own state read, which bounds the whole operation
    /// unless more stages follow it.
    StateRead,
    /// The body-size read a page read makes for a nonempty page. Its failure
    /// degrades the page instead of failing it.
    FinalizedBodySizesRead,
    /// The finalized tip read, the first tree-root read of a page read that
    /// asked for tree roots.
    FinalizedTipRead,
    /// The selected-hash read that checks the page's last finalized header
    /// before roots are attached.
    FinalizedHashRead,
    /// The read of the finalized range's roots.
    FinalizedRootsRead,
    /// Prepare's validation-lease read. The rest of preparation has no
    /// deadline.
    ValidationLeaseRead,
}

impl AdapterStage {
    /// Whether a failure or timeout at this stage still gives a successful
    /// reply. Only the body-size read does: it serves every size as unknown.
    pub const fn degrades(self) -> bool {
        matches!(self, AdapterStage::FinalizedBodySizesRead)
    }
}

/// A deadline around an operation. The module doc explains each kind.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Deadline {
    /// The caller drops its future at `after`. The operation stays pending.
    Reactor {
        /// The default timeout.
        after: Duration,
    },
    /// The adapter fails one state read, `stage`, `after` that read starts.
    /// Each stage gets its own deadline, so an operation's stages can take
    /// longer than `after` in total and still succeed.
    Adapter {
        /// The state read it bounds.
        stage: AdapterStage,
        /// The deadline, counted from the read's start.
        after: Duration,
    },
    /// The reactor reports a fatal event at `after` and keeps the operation.
    /// Only for a VCT repair's prepare or apply.
    FatalWatchdog {
        /// The deadline.
        after: Duration,
    },
}

/// An event that ends capacity an operation owns.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Release {
    /// The operation's `LocatorReadPermit` clone drops when the operation
    /// finishes, not when its caller times out.
    LocatorPermitOnFinish,
    /// A successful page read consumes its lease.
    LeaseConsumedByRead,
    /// An explicit release drops the lease. A repeat does nothing.
    LeaseReleased,
    /// An `Acquired` reply with no receiver drops its lease.
    LeaseAbandonedAcquire,
    /// The lease's 30 s idle deadline has passed, and a later operation that
    /// sweeps idle leases ends it. That operation can belong to any peer. See
    /// [`OpContract::sweeps_idle_leases`].
    LeaseIdleExpired,
    /// Service shutdown drops everything still owned.
    Shutdown,
}

/// Why state refuses an acquire before leasing a path, following
/// `RetainedPathLeaseRegistry::reserve` and `acquire_retained_path`.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum AcquireRefusal {
    /// The peer already holds a lease, or has an acquire in flight. The
    /// signal fires when that ends.
    PeerHoldsLease,
    /// Every slot the request's capacity class may use is taken. The signal
    /// fires when one frees.
    CapacityFull,
    /// The path needs an ordinary slot, but only the reserve slot was free.
    /// The signal fires when an ordinary slot frees.
    BeyondReserve,
    /// The scope names another target, or a newer header generation than
    /// state has.
    ScopeAhead,
    /// The reservation or lease ID counter ran out.
    IdsExhausted,
}

impl AcquireRefusal {
    /// Every refusal, in the order state checks them. `IdsExhausted` sits
    /// where state checks the reservation ID. State checks the lease ID last,
    /// after `BeyondReserve`.
    pub const ALL: [AcquireRefusal; 5] = [
        AcquireRefusal::PeerHoldsLease,
        AcquireRefusal::CapacityFull,
        AcquireRefusal::IdsExhausted,
        AcquireRefusal::ScopeAhead,
        AcquireRefusal::BeyondReserve,
    ];

    /// Whether the refusal is `CapacityBusy`, with a signal the caller waits
    /// on. Otherwise it's `Busy`.
    pub const fn waits_for_capacity(self) -> bool {
        match self {
            AcquireRefusal::PeerHoldsLease
            | AcquireRefusal::CapacityFull
            | AcquireRefusal::BeyondReserve => true,
            AcquireRefusal::ScopeAhead | AcquireRefusal::IdsExhausted => false,
        }
    }
}

/// The contract for one [`HeaderOp`].
#[derive(Debug)]
pub struct OpContract {
    /// The operation.
    pub op: HeaderOp,
    /// The faults the run may choose, in canonical order, with when each
    /// becomes legal.
    pub faults: &'static [(Fault, FaultGate)],
    /// Every deadline around the operation.
    pub deadlines: &'static [Deadline],
    /// The events that end capacity the operation creates or holds. Empty if
    /// it owns none.
    pub releases: &'static [Release],
    /// The checks on the request that run before any lease state is
    /// touched. A failed check gives `Err(Unavailable)`.
    pub input_checks: &'static [InputCheck],
    /// Whether the operation first ends every peer's lease that's past its
    /// idle deadline, as `RetainedPathLeaseRegistry::expire` does, once its
    /// input checks pass. See [`OpContract::sweeps`].
    pub sweeps_idle_leases: bool,
}

/// A request check that runs before the operation touches the state it
/// changes, so a request that fails it changes no state or lease and fires no
/// capacity signal.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum InputCheck {
    /// An acquire's locator has between 1 and `MAX_HEADER_LOCATOR_HASHES`
    /// hashes (`acquire_retained_path`).
    LocatorCount,
    /// The adapter issued the path's handle (`RetainedHeaderPath::adapter_identity`).
    AdapterHandle,
    /// A read asks for between 1 and `MAX_HEADER_SYNC_HEIGHT_RANGE` headers
    /// (`read_retained_path`).
    PageCount,
    /// The adapter sealed the target it's asked to apply
    /// (`PreparedHeaderTarget::into_insert`). A foreign seal gives
    /// `stale_target` for the owner's branch.
    PreparedTargetSeal,
}

impl InputCheck {
    /// Whether the adapter runs this check itself, before any state read and
    /// so before any deadline. Only these checks decide a request's outcome
    /// on their own. State runs the others under the adapter's deadline.
    pub const fn in_adapter(self) -> bool {
        matches!(
            self,
            InputCheck::AdapterHandle | InputCheck::PreparedTargetSeal
        )
    }
}

impl OpContract {
    /// Whether the operation sweeps idle leases, given the input check it
    /// failed, if any.
    pub fn sweeps(&self, failed: Option<InputCheck>) -> bool {
        self.sweeps_idle_leases && failed.is_none()
    }
}

/// The faults of the operations that return `PortError`.
const PORT_FAULTS: &[(Fault, FaultGate)] = &[
    (Fault::Timeout, FaultGate::AfterAdapterDeadline),
    (Fault::Unavailable, FaultGate::Any),
];

/// The only fault of prepare and apply.
const TARGET_FAULTS: &[(Fault, FaultGate)] = &[(Fault::LocalResource, FaultGate::Any)];

/// The deadline of an operation whose state read bounds it whole.
const STATE_READ_DEADLINE: Deadline = Deadline::Adapter {
    stage: AdapterStage::StateRead,
    after: ADAPTER_DEADLINE,
};

/// The deadlines of the reactor's locator and VCT-context calls.
const REACTOR_READ_DEADLINES: &[Deadline] = &[
    Deadline::Reactor {
        after: REACTOR_REQUEST_TIMEOUT,
    },
    STATE_READ_DEADLINE,
];

/// The contracts, indexed by [`HeaderOp`] discriminant.
static CONTRACTS: [OpContract; 8] = [
    OpContract {
        op: HeaderOp::ContinuationLocator,
        faults: PORT_FAULTS,
        deadlines: REACTOR_READ_DEADLINES,
        releases: &[Release::LocatorPermitOnFinish, Release::Shutdown],
        input_checks: &[],
        sweeps_idle_leases: false,
    },
    OpContract {
        op: HeaderOp::VctRepairContext,
        faults: PORT_FAULTS,
        deadlines: REACTOR_READ_DEADLINES,
        releases: &[],
        input_checks: &[],
        sweeps_idle_leases: false,
    },
    OpContract {
        op: HeaderOp::AcquireHeaderPath,
        faults: PORT_FAULTS,
        deadlines: &[STATE_READ_DEADLINE],
        releases: &[
            Release::LeaseAbandonedAcquire,
            Release::LeaseIdleExpired,
            Release::Shutdown,
        ],
        input_checks: &[InputCheck::LocatorCount],
        sweeps_idle_leases: true,
    },
    OpContract {
        op: HeaderOp::ReadHeaderPath,
        faults: PORT_FAULTS,
        deadlines: &[
            STATE_READ_DEADLINE,
            Deadline::Adapter {
                stage: AdapterStage::FinalizedBodySizesRead,
                after: ADAPTER_DEADLINE,
            },
            Deadline::Adapter {
                stage: AdapterStage::FinalizedTipRead,
                after: ADAPTER_DEADLINE,
            },
            Deadline::Adapter {
                stage: AdapterStage::FinalizedHashRead,
                after: ADAPTER_DEADLINE,
            },
            Deadline::Adapter {
                stage: AdapterStage::FinalizedRootsRead,
                after: ADAPTER_DEADLINE,
            },
        ],
        releases: &[
            Release::LeaseConsumedByRead,
            Release::LeaseIdleExpired,
            Release::Shutdown,
        ],
        input_checks: &[InputCheck::AdapterHandle, InputCheck::PageCount],
        sweeps_idle_leases: true,
    },
    OpContract {
        op: HeaderOp::ReleaseHeaderPath,
        faults: PORT_FAULTS,
        deadlines: &[STATE_READ_DEADLINE],
        releases: &[
            Release::LeaseReleased,
            Release::LeaseIdleExpired,
            Release::Shutdown,
        ],
        input_checks: &[InputCheck::AdapterHandle],
        sweeps_idle_leases: false,
    },
    OpContract {
        op: HeaderOp::PrepareHeaderTarget,
        faults: TARGET_FAULTS,
        deadlines: &[
            Deadline::Adapter {
                stage: AdapterStage::ValidationLeaseRead,
                after: ADAPTER_DEADLINE,
            },
            Deadline::FatalWatchdog {
                after: VCT_REPAIR_WATCHDOG,
            },
        ],
        releases: &[],
        input_checks: &[],
        sweeps_idle_leases: false,
    },
    OpContract {
        op: HeaderOp::ApplyHeaderTarget,
        faults: TARGET_FAULTS,
        deadlines: &[Deadline::FatalWatchdog {
            after: VCT_REPAIR_WATCHDOG,
        }],
        releases: &[],
        input_checks: &[InputCheck::PreparedTargetSeal],
        sweeps_idle_leases: false,
    },
    OpContract {
        op: HeaderOp::SelectedChainContains,
        faults: PORT_FAULTS,
        deadlines: &[STATE_READ_DEADLINE],
        releases: &[],
        input_checks: &[],
        sweeps_idle_leases: false,
    },
];

impl HeaderOp {
    /// Every operation, in canonical order.
    pub const ALL: [HeaderOp; 8] = [
        HeaderOp::ContinuationLocator,
        HeaderOp::VctRepairContext,
        HeaderOp::AcquireHeaderPath,
        HeaderOp::ReadHeaderPath,
        HeaderOp::ReleaseHeaderPath,
        HeaderOp::PrepareHeaderTarget,
        HeaderOp::ApplyHeaderTarget,
        HeaderOp::SelectedChainContains,
    ];

    /// The `Port` method name, such as `read_header_path`.
    pub const fn name(self) -> &'static str {
        match self {
            HeaderOp::ContinuationLocator => "continuation_locator",
            HeaderOp::VctRepairContext => "vct_repair_context",
            HeaderOp::AcquireHeaderPath => "acquire_header_path",
            HeaderOp::ReadHeaderPath => "read_header_path",
            HeaderOp::ReleaseHeaderPath => "release_header_path",
            HeaderOp::PrepareHeaderTarget => "prepare_header_target",
            HeaderOp::ApplyHeaderTarget => "apply_header_target",
            HeaderOp::SelectedChainContains => "selected_chain_contains",
        }
    }

    /// This operation's contract.
    pub fn contract(self) -> &'static OpContract {
        // Discriminants are 0..8 in declaration order, matching `CONTRACTS`.
        &CONTRACTS[self as usize]
    }

    /// The outcomes legal `since_submit` after submission for a request that
    /// passes its adapter-side input checks, in canonical order:
    /// [`Outcome::Engine`] first, then the legal faults in contract order.
    /// [`HeaderRequest::enabled`] applies those checks.
    pub fn enabled(self, since_submit: Duration) -> impl Iterator<Item = Outcome> {
        let faults = self
            .contract()
            .faults
            .iter()
            .filter(move |(_, gate)| match gate {
                FaultGate::Any => true,
                FaultGate::AfterAdapterDeadline => since_submit >= ADAPTER_DEADLINE,
            })
            .map(|(fault, _)| Outcome::Fault(*fault));
        std::iter::once(Outcome::Engine).chain(faults)
    }

    /// Whether [`Self::enabled`] offers `outcome` at `since_submit`, for a
    /// request that passes its adapter-side input checks. ST3 calls
    /// [`HeaderRequest::check`], which also applies those.
    pub fn check(self, outcome: Outcome, since_submit: Duration) -> Result<(), IllegalOutcome> {
        if self.enabled(since_submit).any(|legal| legal == outcome) {
            Ok(())
        } else {
            Err(IllegalOutcome {
                op: self,
                outcome,
                since_submit,
            })
        }
    }
}

impl Outcome {
    /// The name in a storage action identity,
    /// `storage:header:<seq>:<name>`.
    pub const fn name(self) -> &'static str {
        match self {
            Outcome::Engine => "engine",
            Outcome::Fault(Fault::Timeout) => "timeout",
            Outcome::Fault(Fault::Unavailable) => "unavailable",
            Outcome::Fault(Fault::LocalResource) => "local-resource",
            Outcome::Rejected => "rejected",
        }
    }
}

/// An outcome the contract doesn't allow for an operation at that time.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct IllegalOutcome {
    /// The operation.
    pub op: HeaderOp,
    /// The refused outcome.
    pub outcome: Outcome,
    /// How long after submission it was asked for.
    pub since_submit: Duration,
}

impl fmt::Display for IllegalOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} can't complete with {} {:?} after submission",
            self.op.name(),
            self.outcome.name(),
            self.since_submit,
        )
    }
}

impl std::error::Error for IllegalOutcome {}

/// An operation's accepted inputs, which own what the caller handed over.
#[derive(Debug)]
pub enum HeaderRequest {
    /// `continuation_locator`. The permit stays owned until the operation
    /// finishes.
    ContinuationLocator {
        /// The caller's permit clone.
        permit: LocatorReadPermit,
    },
    /// `vct_repair_context`.
    VctRepairContext {
        /// The repair owner.
        owner: BodyWorkOwner,
        /// The repaired height.
        height: block::Height,
    },
    /// `acquire_header_path`.
    AcquireHeaderPath(AcquirePath),
    /// `read_header_path`.
    ReadHeaderPath {
        /// The leased path.
        path: RetainedHeaderPath,
        /// The page to read.
        request: ReadPath,
    },
    /// `release_header_path`.
    ReleaseHeaderPath {
        /// The leased path.
        path: RetainedHeaderPath,
    },
    /// `prepare_header_target`.
    PrepareHeaderTarget(PrepareHeaderTarget),
    /// `apply_header_target`.
    ApplyHeaderTarget {
        /// The sealed target.
        target: PreparedHeaderTarget,
    },
    /// `selected_chain_contains`.
    SelectedChainContains {
        /// The header asked about.
        hash: block::Hash,
    },
}

/// An operation's reply, in the `Port` method's own reply type.
#[derive(Debug)]
pub enum HeaderReply {
    /// `continuation_locator`.
    ContinuationLocator(Result<Option<HeaderLocator>, PortError>),
    /// `vct_repair_context`.
    VctRepairContext(Result<VctRepairContextReply, PortError>),
    /// `acquire_header_path`.
    AcquireHeaderPath(Result<AcquirePathReply, PortError>),
    /// `read_header_path`.
    ReadHeaderPath(Result<ReadPathReply, PortError>),
    /// `release_header_path`.
    ReleaseHeaderPath(Result<(), PortError>),
    /// `prepare_header_target`.
    PrepareHeaderTarget(PrepareHeaderTargetReply),
    /// `apply_header_target`.
    ApplyHeaderTarget(ApplyHeaderTargetReply),
    /// `selected_chain_contains`.
    SelectedChainContains(Result<SelectedChainMembership, PortError>),
}

impl HeaderRequest {
    /// The operation this request is for.
    pub fn op(&self) -> HeaderOp {
        match self {
            HeaderRequest::ContinuationLocator { .. } => HeaderOp::ContinuationLocator,
            HeaderRequest::VctRepairContext { .. } => HeaderOp::VctRepairContext,
            HeaderRequest::AcquireHeaderPath(_) => HeaderOp::AcquireHeaderPath,
            HeaderRequest::ReadHeaderPath { .. } => HeaderOp::ReadHeaderPath,
            HeaderRequest::ReleaseHeaderPath { .. } => HeaderOp::ReleaseHeaderPath,
            HeaderRequest::PrepareHeaderTarget(_) => HeaderOp::PrepareHeaderTarget,
            HeaderRequest::ApplyHeaderTarget { .. } => HeaderOp::ApplyHeaderTarget,
            HeaderRequest::SelectedChainContains { .. } => HeaderOp::SelectedChainContains,
        }
    }

    /// The work owner of a prepare or apply, which their records carry.
    /// `None` for other operations.
    pub fn owner(&self) -> Option<HeaderSyncWorkOwner> {
        match self {
            HeaderRequest::PrepareHeaderTarget(request) => Some(request.owner),
            HeaderRequest::ApplyHeaderTarget { target } => Some(target.owner()),
            _ => None,
        }
    }

    /// The first adapter-side input check this request fails, for the
    /// adapter holding `key`. The contract's `input_checks` are the source.
    pub fn rejected_by(&self, key: &AdapterKey) -> Option<InputCheck> {
        self.op()
            .contract()
            .input_checks
            .iter()
            .copied()
            .filter(|check| check.in_adapter())
            .find(|check| match check {
                InputCheck::AdapterHandle => match self {
                    HeaderRequest::ReadHeaderPath { path, .. }
                    | HeaderRequest::ReleaseHeaderPath { path } => {
                        path.adapter_identity(key).is_none()
                    }
                    _ => false,
                },
                InputCheck::PreparedTargetSeal => match self {
                    // `into_insert` consumes the target, so this checks a
                    // clone and keeps the request's own.
                    HeaderRequest::ApplyHeaderTarget { target } => {
                        target.clone().into_insert(key).is_err()
                    }
                    _ => false,
                },
                InputCheck::LocatorCount | InputCheck::PageCount => false,
            })
    }

    /// The outcomes legal for this request `since_submit` after submission,
    /// for the adapter holding `key`. Only [`Outcome::Rejected`] if an
    /// adapter-side input check fails, and [`HeaderOp::enabled`] otherwise.
    pub fn enabled(&self, key: &AdapterKey, since_submit: Duration) -> Vec<Outcome> {
        if self.rejected_by(key).is_some() {
            vec![Outcome::Rejected]
        } else {
            self.op().enabled(since_submit).collect()
        }
    }

    /// Whether [`Self::enabled`] offers `outcome`.
    pub fn check(
        &self,
        key: &AdapterKey,
        outcome: Outcome,
        since_submit: Duration,
    ) -> Result<(), IllegalOutcome> {
        if self.enabled(key, since_submit).contains(&outcome) {
            Ok(())
        } else {
            Err(IllegalOutcome {
                op: self.op(),
                outcome,
                since_submit,
            })
        }
    }

    /// The adapter's rejection, if this request fails an adapter-side input
    /// check, as the production adapter returns it. A foreign path handle
    /// gives `PortError::Unavailable` with no source. A foreign target seal
    /// gives `stale_target` for the owner's branch.
    pub fn rejection_reply(&self, key: &AdapterKey) -> Option<HeaderReply> {
        let check = self.rejected_by(key)?;
        let foreign_handle = || PortError::Unavailable { source: None };
        Some(match (check, self) {
            (InputCheck::AdapterHandle, HeaderRequest::ReadHeaderPath { .. }) => {
                HeaderReply::ReadHeaderPath(Err(foreign_handle()))
            }
            (InputCheck::AdapterHandle, HeaderRequest::ReleaseHeaderPath { .. }) => {
                HeaderReply::ReleaseHeaderPath(Err(foreign_handle()))
            }
            (InputCheck::PreparedTargetSeal, HeaderRequest::ApplyHeaderTarget { target }) => {
                HeaderReply::ApplyHeaderTarget(Err(Arc::new(HeaderChainError::stale_target(
                    ErrorSubject::Branch(target.owner().header_authority().branch),
                ))))
            }
            _ => unreachable!("rejected_by fails each adapter-side check only on its own request"),
        })
    }

    /// The reply the production adapter gives for `fault`, or
    /// [`IllegalOutcome`] if [`Self::check`] refuses it.
    pub fn fault_reply(
        &self,
        key: &AdapterKey,
        fault: Fault,
        since_submit: Duration,
    ) -> Result<HeaderReply, IllegalOutcome> {
        self.check(key, Outcome::Fault(fault), since_submit)?;
        let port_error = || match fault {
            Fault::Timeout => PortError::Timeout,
            Fault::Unavailable => PortError::Unavailable { source: None },
            Fault::LocalResource => {
                unreachable!("the contract gives local-resource only to prepare and apply")
            }
        };
        let local_resource = |owner: HeaderSyncWorkOwner| {
            Arc::new(HeaderChainError::local_resource(
                ErrorSubject::Branch(owner.header_authority().branch),
                None,
            ))
        };
        Ok(match self {
            HeaderRequest::ContinuationLocator { .. } => {
                HeaderReply::ContinuationLocator(Err(port_error()))
            }
            HeaderRequest::VctRepairContext { .. } => {
                HeaderReply::VctRepairContext(Err(port_error()))
            }
            HeaderRequest::AcquireHeaderPath(_) => {
                HeaderReply::AcquireHeaderPath(Err(port_error()))
            }
            HeaderRequest::ReadHeaderPath { .. } => HeaderReply::ReadHeaderPath(Err(port_error())),
            HeaderRequest::ReleaseHeaderPath { .. } => {
                HeaderReply::ReleaseHeaderPath(Err(port_error()))
            }
            HeaderRequest::PrepareHeaderTarget(request) => {
                HeaderReply::PrepareHeaderTarget(Err(local_resource(request.owner)))
            }
            HeaderRequest::ApplyHeaderTarget { target } => {
                HeaderReply::ApplyHeaderTarget(Err(local_resource(target.owner())))
            }
            HeaderRequest::SelectedChainContains { .. } => {
                HeaderReply::SelectedChainContains(Err(port_error()))
            }
        })
    }
}

impl HeaderReply {
    /// The operation this reply answers.
    pub fn op(&self) -> HeaderOp {
        match self {
            HeaderReply::ContinuationLocator(_) => HeaderOp::ContinuationLocator,
            HeaderReply::VctRepairContext(_) => HeaderOp::VctRepairContext,
            HeaderReply::AcquireHeaderPath(_) => HeaderOp::AcquireHeaderPath,
            HeaderReply::ReadHeaderPath(_) => HeaderOp::ReadHeaderPath,
            HeaderReply::ReleaseHeaderPath(_) => HeaderOp::ReleaseHeaderPath,
            HeaderReply::PrepareHeaderTarget(_) => HeaderOp::PrepareHeaderTarget,
            HeaderReply::ApplyHeaderTarget(_) => HeaderOp::ApplyHeaderTarget,
            HeaderReply::SelectedChainContains(_) => HeaderOp::SelectedChainContains,
        }
    }
}
