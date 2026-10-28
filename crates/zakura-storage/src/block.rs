//! The block storage contract: what a simulated block service may do.
//!
//! Block sync has no storage trait. The reactor sends [`BlockSyncAction`]s
//! on a channel, and the node's driver answers through the
//! [`BlockSyncHandle`](zakura_network::zakura::BlockSyncHandle): a
//! needed-blocks answer or failure for each query, and a
//! `BlockApplyFinished` event for each submitted body. Serving reads
//! committed blocks through a
//! [`BlockRangeSource`](zakura_network::zakura::BlockRangeSource). The
//! simulator's block service answers both. Each action the driver takes,
//! and each range read, becomes a pending operation, and the run completes
//! it later with one [`BlockOutcome`]. This module says which outcomes each
//! operation may have, and when. It mirrors production at the revision it
//! was written against, and the source references below name the code it
//! follows.
//!
//! [`controlled`] holds the block service that keeps this contract,
//! [`evidence`] the facts it publishes and the models that check them, and
//! [`range`] the range source serving reads through. [`fixture`] builds the
//! blocks it serves.
//!
//! # Operations
//!
//! [`BlockOp`] lists one operation per action, plus the range read. A
//! submission has two stages: it waits in the driver's queue until the apply
//! limits admit it, then waits for the verifier's verdict.
//!
//! | Operation | Stage | Outcomes | Reply | Production source |
//! |:--|:--|:--|:--|:--|
//! | `query_needed_blocks` | pending | `answer`, `fail` | `ScopedNeededBlocks`, or a query failure | `drive_block_sync_actions`, `query_block_sync_needed_blocks`, and zakura-state's `missing_block_body_metadata` |
//! | `submit_block` | queued | `admit` | none | `drain_pending_block_applies` |
//! | `submit_block` | admitted | one [`Verdict`] | `BlockApplyFinished` | `apply_block_sync_body`, `block_commit_outcome` and `block_verify_error_class` |
//! | `record_body_unavailable` | pending | `apply`, `fail` | none | the driver's `RecordBodyUnavailable` arm |
//! | `record_body_invalid` | pending | `apply` | none | `persist_consensus_body_invalid` |
//! | `restart_body_availability` | pending | `apply`, `fail` | none | the driver's `RestartBodyAvailability` arm |
//! | `retry_body_availability` | pending | `apply`, `fail` | none | the driver's `RetryBodyAvailability` arm |
//! | `misbehavior` | none | none | none | the driver's `Misbehavior` arm |
//! | `read_range` | pending | `serve`, `error` | the read's result | `StateBlockRangeSource` and zakura-state's `read_owned_block_range` |
//!
//! The driver's sources are in `zakurad`'s
//! `commands/start/zakura/block_sync_driver.rs` and
//! `block_range_source.rs`. A misbehavior report has no outcome: the
//! service records it when the driver takes it, since the driver's only
//! effect is the supervisor's enforcement, which the simulator doesn't run.
//!
//! # Outcomes
//!
//! An outcome can be built freely, so the contract only holds because the
//! service calls [`BlockOp::check`] before it completes an operation.
//!
//! * `answer` runs the missing-body query against the engine and the
//!   service's committed chain and replies with what it computes, or with a
//!   failure if that query errs, as zakura-state's does.
//! * `fail` is a state error. A query replies with a failure. A record
//!   changes nothing: the driver logs the error and moves on.
//!   `record_body_invalid` has no `fail`, since the driver shuts down when
//!   that record isn't durable, and the simulator excludes that shutdown.
//! * `apply` runs the record's transition on the engine, which decides
//!   whether it's recorded or stale. A stale `record_body_invalid` retries
//!   with the engine's current version, up to
//!   [`BODY_INVALID_STALE_REFRESH_LIMIT`] times, as
//!   `persist_consensus_body_invalid` does.
//! * `admit` moves a queued submission to the verifier.
//! * A [`Verdict`] is the verifier's answer. Only the verifier is modeled,
//!   so the run picks it, among the verdicts its [`Gate`]s allow.
//! * `serve` reads the committed chain's contiguous prefix from the
//!   requested height, within the request's count and byte budget, as
//!   zakura-state's bounded read does. A missing first block gives an empty
//!   prefix. `error` is the read's `Err`.
//!
//! # Gates
//!
//! The driver takes one action at a time from its channel, and the block
//! service takes them as the driver does (`coalesce_ready_needed_block_queries`
//! at the top of its loop, `coalesce_stale_needed_block_queries` when it
//! takes a query). A query or record is pending only once the driver has
//! taken it, and the driver takes nothing else until it completes, so its
//! outcomes need no gate. A [`Gate`] says when the other outcomes are legal,
//! from the [`Conditions`] the service computes for each pending operation:
//!
//! * The driver admits a body and reports a verdict only at the top of
//!   its loop or in its select, never while it awaits a query or record
//!   ([`Stage::needs_free_driver`]). A verdict commits and replies at
//!   once, so this leaves out a commit that lands while the driver awaits.
//!   The driver also admits in its `SubmitBlock` arm, right after it
//!   queues the body. The run admits lazily instead: a queued submission
//!   waits until the run picks `admit`, which only delays admissions the
//!   driver makes at once.
//! * The driver admits queued submissions in arrival order, skipping one
//!   whose class is at its limit ([`Gate::AdmitNext`]). The limits are
//!   [`ApplyLimits`].
//! * The verifier commits a body only once its parent is committed. Full
//!   state queues a body whose parent is missing
//!   (`QueuedBlocks` in zakura-state), so `committed` and `ambiguous` wait
//!   for the parent ([`Gate::ParentCommitted`]).
//! * `duplicate` needs state to know the hash ([`Gate::KnownToState`]): it
//!   is committed, a later admission of it replaced this one in full
//!   state's queue, or an older admission of it is still in flight. The
//!   verifier waits on a hash in the write channel and then reports a
//!   duplicate (`KnownBlock` in zakura-consensus's block verifier), and
//!   state refuses a hash it has already sent to the writer
//!   (`non_finalized_block_write_sent_hashes`). The older submission's own
//!   verdict may still fail. The two are either/or: state replaces a
//!   queued older copy or refuses the newer one
//!   (`queue_and_commit_to_non_finalized_state`), so a newer copy that
//!   reports a duplicate replaced nothing, and the older copy must reach a
//!   verdict of its own.
//! * State checks a body's header commitments against its parent's trees,
//!   after it checks for a duplicate, so `payload_mismatch` waits for the
//!   parent too ([`Gate::ParentCommitted`]). Full state checks them once
//!   `QueuedBlocks` releases the body, and finalized state once the body's
//!   parent is its tip.
//! * State decides a `Storage` retry after it checks for a duplicate, so it
//!   needs the hash not to be committed ([`Gate::NotCommitted`]). It doesn't
//!   wait for the parent: when a body fails without proving itself invalid,
//!   full state fails its queued and later descendants with a header-chain
//!   error (`failed_ancestor_error` in zakura-state).
//! * Both verifiers check for a committed hash before any consensus rule.
//!   The full verifier asks state for the hash first and reports a
//!   committed one as a duplicate (`KnownBlock` in zakura-consensus's block
//!   verifier). The checkpoint verifier refuses a height at or below its
//!   verified progress, which covers every committed block, before its body
//!   checks (`check_height`'s `AlreadyVerified`). So `consensus_invalid`
//!   needs the hash not to be committed when the verifier received it
//!   ([`Gate::NewAtAdmission`]). The run admits lazily, so admission is
//!   when the verifier receives the body. A commit by another submission
//!   after admission doesn't close the gate: the verifier has passed its
//!   check by then.
//! * The other retryable kinds stay legal after admission
//!   ([`RetryKind::gate`]). A failed duplicate check gives a retryable
//!   verdict even for a committed hash (`Depth`), and the contract doesn't
//!   narrow the kinds by their source.
//!
//! # Verdicts
//!
//! [`Verdict::outcome`] builds each verdict's `BlockApplyOutcome` as
//! `block_commit_outcome` does, with the same evidence identities. The
//! driver fidelity test in `zakurad` compares them with the driver's.
//!
//! * `committed` commits the body: the service grows its committed chain and
//!   runs the verified-chain transition on the engine.
//! * `payload_mismatch` reports one of state's header-commitment
//!   mismatches, [`Commitment`]. The routine checks the transaction merkle
//!   root and the coinbase height before it submits a body, so the
//!   verifier's checks of those can't fail. State's commitment checks still
//!   can, and block sync disconnects the supplier only for the kinds that
//!   prove tampering.
//! * `consensus_invalid` reports a fixed rule per class.
//! * `retryable` reports one [`RetryKind`]. [`ApplyClass::retry_kinds`]
//!   lists the kinds each verifier can produce.
//! * `ambiguous` commits the body and reports it retryable with
//!   `VerifierUnavailable`. Full state's writer commits independently of
//!   the response path, so a dropped or failed response can follow a commit.
//!   It's legal only after admission: a queued submission never reached the
//!   verifier.
//!
//! # Excluded histories
//!
//! Each operation's production stages run atomically at its completion, as
//! in the header contract. Any production history that depends on another
//! operation seeing an intermediate stage is excluded. The known instances:
//!
//! * A commit visible before its reply. Full state commits and publishes the
//!   new view, then the driver sends `BlockApplyFinished`. Here both happen in
//!   one completion.
//! * A commit while the driver awaits a query or record. Production's
//!   verifier can commit then, and the driver replies once it's free. Here
//!   a verdict commits and replies at once, so it waits for a free driver
//!   ([`Stage::needs_free_driver`]).
//! * Chunks of one query reading different states. The driver reads up to
//!   4,000 heights per state read, and fails the query if the anchor moves
//!   between reads. Here every chunk reads one state, so that failure only
//!   comes from `fail`.
//! * A short range read. Production returns an empty or short prefix when
//!   its read is cancelled before or during the read
//!   (`read_owned_block_range`). Here `serve` always returns the whole
//!   prefix, and `error` is the only other answer.
//!
//! These parts of the driver don't run here, so no outcome covers them:
//!
//! * the legacy handoff and `SyncCoordinator` phases, including the
//!   abandonment of applies when the node yields to legacy sync. A node that
//!   runs only the Zakura stack never yields after bootstrap;
//! * the debug throughput probe and the stall traces;
//! * the driver's shutdown when a `record_body_invalid` isn't durable;
//! * the driver's 30 s deadlines on state calls;
//! * the checkpoint verifier holding a body until its checkpoint range is
//!   complete. A body at or below [`ApplyLimits::max_checkpoint_height`] is
//!   checkpoint-class: it counts against the checkpoint limits and has the
//!   checkpoint verifier's retry kinds, but its verdict never waits for the
//!   rest of its range.

use std::{fmt, num::NonZeroU64};

use sha2::{Digest, Sha256};
use zakura_chain::block;
use zakura_header_chain::{
    BodyCommitmentKind, BodyPayloadMismatch, BodyRuleId, BodyUnavailableSummary, BodyWorkAuthority,
    BodyWorkOwner, ConsensusBodyInvalid, EvidenceId, Frontier, SourceId, TransientBodyFailure,
    TransientBodyFailureKind, VerifiedBodyEvidence,
};
use zakura_network::zakura::{
    BlockApplyOutcome, BlockApplyToken, BlockRangeRead, BlockSyncAction, BlockSyncBlockMeta,
    BlockSyncEvent,
};
use zakura_node_services::constants::MAX_CHECKPOINT_HEIGHT_GAP;

pub mod controlled;
pub mod evidence;
pub mod fixture;
pub mod range;

/// How many times `persist_consensus_body_invalid` refreshes a stale
/// version before it gives up.
pub const BODY_INVALID_STALE_REFRESH_LIMIT: u8 = 3;

/// The driver's floor on its checkpoint apply limit, zakurad's
/// `MIN_CHECKPOINT_CONCURRENCY_LIMIT` (`components/sync.rs`). It equals
/// the ceiling, so the checkpoint limit is always this.
pub const MIN_CHECKPOINT_APPLY_LIMIT: usize = MAX_CHECKPOINT_HEIGHT_GAP;

/// The driver's floor on its full and combined apply limits, zakurad's
/// `MIN_CONCURRENCY_LIMIT` (`components/sync.rs`).
pub const MIN_APPLY_LIMIT: usize = 1;

/// One block-service operation. Variants follow [`BlockSyncAction`]'s order,
/// then the range read, which is also the canonical order.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum BlockOp {
    /// `QueryNeededBlocks`.
    QueryNeededBlocks,
    /// `SubmitBlock`.
    SubmitBlock,
    /// `RecordBodyUnavailable`.
    RecordBodyUnavailable,
    /// `RecordBodyInvalid`.
    RecordBodyInvalid,
    /// `RestartBodyAvailability`.
    RestartBodyAvailability,
    /// `RetryBodyAvailability`.
    RetryBodyAvailability,
    /// `Misbehavior`.
    Misbehavior,
    /// `BlockRangeSource::read`.
    ReadRange,
}

/// Where a pending operation is. Only a submission has two stages.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Stage {
    /// Waiting for its one completion.
    Pending,
    /// A submission in the driver's queue, before the apply limits admit it.
    Queued,
    /// A submission the verifier is working on.
    Admitted,
}

impl Stage {
    /// Whether an operation at this stage completes only while the driver
    /// awaits nothing: it admits a body and takes a verdict only at the top
    /// of its loop or in its select.
    pub const fn needs_free_driver(self) -> bool {
        matches!(self, Stage::Queued | Stage::Admitted)
    }
}

/// How a pending operation completes, or moves to its next stage.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum BlockOutcome {
    /// Run the missing-body query and reply with its result.
    Answer,
    /// A state error: a query replies with a failure, a record changes
    /// nothing.
    Fail,
    /// Admit a queued submission to the verifier.
    Admit,
    /// The verifier's verdict on an admitted submission.
    Verdict(Verdict),
    /// Run a record's transition on the engine.
    Apply,
    /// Serve a range read from the committed chain.
    Serve,
    /// Fail a range read.
    Error,
}

/// The verifier's answer for one admitted body.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Verdict {
    /// The body committed.
    Committed,
    /// State already knew the body's hash.
    Duplicate,
    /// The body disagrees with a commitment in its header.
    PayloadMismatch(Commitment),
    /// The body failed a consensus rule.
    ConsensusInvalid,
    /// Verification reached no conclusion.
    Retryable(RetryKind),
    /// The body committed, but the reply says `VerifierUnavailable`.
    Ambiguous,
}

/// A header commitment that state finds a body disagrees with, a subset of
/// zakura-state's `InvalidBlockCommitment` kinds (`ValidateContextError::
/// body_verification_class`).
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Commitment {
    /// The ZIP-244 auth-data root, which proves tampering.
    AuthDataRoot,
    /// The NU5 chain-history and auth-data commitment, which proves
    /// tampering.
    ChainHistoryBlockTxAuthCommitment,
    /// The final Sapling root, which depends on local trees and doesn't
    /// prove tampering.
    FinalSaplingRoot,
}

impl Commitment {
    /// Every commitment, in canonical order.
    pub const ALL: [Commitment; 3] = [
        Commitment::AuthDataRoot,
        Commitment::ChainHistoryBlockTxAuthCommitment,
        Commitment::FinalSaplingRoot,
    ];

    /// The production kind.
    pub const fn kind(self) -> BodyCommitmentKind {
        match self {
            Commitment::AuthDataRoot => BodyCommitmentKind::AuthDataRoot,
            Commitment::ChainHistoryBlockTxAuthCommitment => {
                BodyCommitmentKind::CHAIN_HISTORY_BLOCK_TX_AUTH_COMMITMENT
            }
            Commitment::FinalSaplingRoot => BodyCommitmentKind::Other("final_sapling_root"),
        }
    }

    /// The driver's `body_commitment_kind_label`, also the name in an
    /// outcome name.
    pub const fn label(self) -> &'static str {
        match self {
            Commitment::AuthDataRoot => "auth_data_root",
            Commitment::ChainHistoryBlockTxAuthCommitment => {
                "chain_history_block_tx_auth_commitment"
            }
            Commitment::FinalSaplingRoot => "final_sapling_root",
        }
    }
}

/// A retryable verifier failure, `TransientBodyFailureKind` without the
/// kinds no verifier returns.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum RetryKind {
    /// `MissingContext`.
    MissingContext,
    /// `Canceled`.
    Canceled,
    /// `Storage`.
    Storage,
    /// `VerifierUnavailable`.
    VerifierUnavailable,
    /// `ResourceExhausted`.
    ResourceExhausted,
}

impl RetryKind {
    /// The production kind.
    pub const fn kind(self) -> TransientBodyFailureKind {
        match self {
            RetryKind::MissingContext => TransientBodyFailureKind::MissingContext,
            RetryKind::Canceled => TransientBodyFailureKind::Canceled,
            RetryKind::Storage => TransientBodyFailureKind::Storage,
            RetryKind::VerifierUnavailable => TransientBodyFailureKind::VerifierUnavailable,
            RetryKind::ResourceExhausted => TransientBodyFailureKind::ResourceExhausted,
        }
    }

    /// The name in an outcome name, such as `missing-context`.
    pub const fn name(self) -> &'static str {
        match self {
            RetryKind::MissingContext => "missing-context",
            RetryKind::Canceled => "canceled",
            RetryKind::Storage => "storage",
            RetryKind::VerifierUnavailable => "verifier-unavailable",
            RetryKind::ResourceExhausted => "resource-exhausted",
        }
    }

    /// When this kind is legal. Only state's commit reports `Storage`, after
    /// its duplicate check. The verifier can report the others before state
    /// sees the body.
    pub const fn gate(self) -> Gate {
        match self {
            RetryKind::Storage => Gate::NotCommitted,
            RetryKind::MissingContext
            | RetryKind::Canceled
            | RetryKind::VerifierUnavailable
            | RetryKind::ResourceExhausted => Gate::Always,
        }
    }
}

/// The verifier a body goes to, `BlockApplyClass` in the driver.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ApplyClass {
    /// At or below the last checkpoint height.
    Checkpoint,
    /// Above it.
    Full,
}

impl ApplyClass {
    /// The class of a body at `height`, as `block_apply_class` decides it.
    pub fn of(height: block::Height, max_checkpoint_height: block::Height) -> Self {
        if height <= max_checkpoint_height {
            ApplyClass::Checkpoint
        } else {
            ApplyClass::Full
        }
    }

    /// The retryable kinds this class's verifier can produce, in canonical
    /// order.
    ///
    /// * Full: `MissingContext` from a failed state read or a missing parent
    ///   or tree (`Depth`, `ParentUnavailable`, `NotReadyToBeCommitted`),
    ///   `Canceled` from an invalidated or orphaned ancestor, `Storage` from
    ///   a header-chain or tree failure, and `VerifierUnavailable` from a
    ///   full queue, an exited writer or a failed state call
    ///   (`VerifyBlockError` and `CommitBlockError::body_verification_class`).
    /// * Checkpoint: `MissingContext` from an unavailable supplied VCT root
    ///   (`VctSuppliedRootUnavailable` in finalized state), `Canceled` from a
    ///   finished or shutting-down verifier, `Storage` from a failed
    ///   checkpoint commit, `VerifierUnavailable` from a dropped request, and
    ///   `ResourceExhausted` from the queued limit
    ///   (`VerifyCheckpointError::body_verification_class`).
    ///
    /// No verifier returns `Timeout`, and only legacy-yield abandonment,
    /// which the simulator excludes, returns `Canceled` for a full body
    /// without the verifier.
    pub const fn retry_kinds(self) -> &'static [RetryKind] {
        match self {
            ApplyClass::Full => &[
                RetryKind::MissingContext,
                RetryKind::Canceled,
                RetryKind::Storage,
                RetryKind::VerifierUnavailable,
            ],
            ApplyClass::Checkpoint => &[
                RetryKind::MissingContext,
                RetryKind::Canceled,
                RetryKind::Storage,
                RetryKind::VerifierUnavailable,
                RetryKind::ResourceExhausted,
            ],
        }
    }

    /// The rule a `consensus_invalid` verdict reports.
    pub const fn invalid_rule(self) -> &'static str {
        match self {
            ApplyClass::Full => "block.subsidy",
            ApplyClass::Checkpoint => "checkpoint.subsidy",
        }
    }
}

/// The driver's apply limits after it clamps its configuration, as
/// `drive_block_sync_actions` and `drain_pending_block_applies` apply them.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ApplyLimits {
    /// The checkpoint verifier's concurrency limit.
    pub checkpoint: usize,
    /// The full verifier's concurrency limit.
    pub full: usize,
    /// The limit on both together.
    pub combined: usize,
    /// The last checkpoint height, which decides each body's class.
    pub max_checkpoint_height: block::Height,
}

impl ApplyLimits {
    /// The limits the driver uses for configured `checkpoint`, `full` and
    /// `combined` limits. It clamps the checkpoint limit to
    /// `[MIN_CHECKPOINT_APPLY_LIMIT, MAX_CHECKPOINT_HEIGHT_GAP]`, which is
    /// one value, and raises the others to at least [`MIN_APPLY_LIMIT`].
    pub fn clamped(
        checkpoint: usize,
        full: usize,
        combined: usize,
        max_checkpoint_height: block::Height,
    ) -> Self {
        ApplyLimits {
            checkpoint: checkpoint.clamp(MIN_CHECKPOINT_APPLY_LIMIT, MAX_CHECKPOINT_HEIGHT_GAP),
            full: full.max(MIN_APPLY_LIMIT),
            combined: combined.max(MIN_APPLY_LIMIT),
            max_checkpoint_height,
        }
    }

    /// Whether one more `class` apply may start while `checkpoint` and
    /// `full` applies are in flight. A checkpoint range can sit in the
    /// verifier until its checkpoint arrives, so checkpoint applies get room
    /// for two ranges.
    pub fn has_room(self, class: ApplyClass, checkpoint: usize, full: usize) -> bool {
        let in_flight = checkpoint.saturating_add(full);
        match class {
            ApplyClass::Checkpoint => {
                let pipeline = self.checkpoint.saturating_mul(2);
                in_flight < self.combined.max(pipeline) && checkpoint < pipeline
            }
            ApplyClass::Full => in_flight < self.combined && full < self.full,
        }
    }
}

/// When an outcome is legal. The module doc explains each.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Gate {
    /// Always.
    Always,
    /// The submission is the oldest queued one whose class has room.
    AdmitNext,
    /// The body's parent is committed and the body isn't.
    ParentCommitted,
    /// The body's hash is committed, a later admission of it replaced this
    /// one, or an older admission of it is in flight.
    KnownToState,
    /// The body's hash isn't committed.
    NotCommitted,
    /// The body's hash wasn't committed when the verifier received it.
    NewAtAdmission,
}

/// What the service knows about a pending operation when it lists its
/// outcomes. Fields an operation's gates don't read are ignored.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct Conditions {
    /// The driver is awaiting a query or record.
    pub driver_awaiting: bool,
    /// The submission is the oldest queued one whose class has room.
    pub oldest_with_room: bool,
    /// The body's parent is committed.
    pub parent_committed: bool,
    /// The body's hash is committed.
    pub committed: bool,
    /// The body's hash was committed when the run admitted it.
    pub committed_at_admission: bool,
    /// A later admission of the same hash replaced this one and didn't
    /// report a duplicate itself.
    pub replaced: bool,
    /// An older admission of the same hash is in flight.
    pub older_in_flight: bool,
}

impl Gate {
    /// Whether this gate is open under `conditions`.
    pub const fn open(self, conditions: &Conditions) -> bool {
        match self {
            Gate::Always => true,
            Gate::AdmitNext => conditions.oldest_with_room,
            Gate::ParentCommitted => conditions.parent_committed && !conditions.committed,
            Gate::KnownToState => {
                conditions.committed || conditions.replaced || conditions.older_in_flight
            }
            Gate::NotCommitted => !conditions.committed,
            Gate::NewAtAdmission => !conditions.committed_at_admission,
        }
    }
}

/// A row of a stage's contract: one outcome with its gate, or a family that
/// expands per class or per commitment.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum OutcomeFamily {
    /// One outcome.
    One(BlockOutcome, Gate),
    /// `Verdict(PayloadMismatch(commitment))` for each [`Commitment`], with
    /// [`Gate::ParentCommitted`].
    PayloadMismatch,
    /// `Verdict(Retryable(kind))` for each of the class's kinds, with
    /// [`RetryKind::gate`].
    Retryable,
}

/// The outcomes of one stage of an operation.
#[derive(Debug)]
pub struct StageContract {
    /// The stage.
    pub stage: Stage,
    /// The outcome families, in canonical order.
    pub outcomes: &'static [OutcomeFamily],
}

/// The contract for one [`BlockOp`].
#[derive(Debug)]
pub struct OpContract {
    /// The operation.
    pub op: BlockOp,
    /// Its stages, in order. Empty for an operation the service records
    /// when the driver takes it.
    pub stages: &'static [StageContract],
}

/// The stages of a query.
const QUERY_STAGES: &[StageContract] = &[StageContract {
    stage: Stage::Pending,
    outcomes: &[
        OutcomeFamily::One(BlockOutcome::Answer, Gate::Always),
        OutcomeFamily::One(BlockOutcome::Fail, Gate::Always),
    ],
}];

/// The stages of a record that a state error leaves unrecorded.
const RECORD_STAGES: &[StageContract] = &[StageContract {
    stage: Stage::Pending,
    outcomes: &[
        OutcomeFamily::One(BlockOutcome::Apply, Gate::Always),
        OutcomeFamily::One(BlockOutcome::Fail, Gate::Always),
    ],
}];

/// The contracts, indexed by [`BlockOp`] discriminant.
static CONTRACTS: [OpContract; 8] = [
    OpContract {
        op: BlockOp::QueryNeededBlocks,
        stages: QUERY_STAGES,
    },
    OpContract {
        op: BlockOp::SubmitBlock,
        stages: &[
            StageContract {
                stage: Stage::Queued,
                outcomes: &[OutcomeFamily::One(BlockOutcome::Admit, Gate::AdmitNext)],
            },
            StageContract {
                stage: Stage::Admitted,
                outcomes: &[
                    OutcomeFamily::One(
                        BlockOutcome::Verdict(Verdict::Committed),
                        Gate::ParentCommitted,
                    ),
                    OutcomeFamily::One(
                        BlockOutcome::Verdict(Verdict::Duplicate),
                        Gate::KnownToState,
                    ),
                    OutcomeFamily::PayloadMismatch,
                    OutcomeFamily::One(
                        BlockOutcome::Verdict(Verdict::ConsensusInvalid),
                        Gate::NewAtAdmission,
                    ),
                    OutcomeFamily::Retryable,
                    OutcomeFamily::One(
                        BlockOutcome::Verdict(Verdict::Ambiguous),
                        Gate::ParentCommitted,
                    ),
                ],
            },
        ],
    },
    OpContract {
        op: BlockOp::RecordBodyUnavailable,
        stages: RECORD_STAGES,
    },
    OpContract {
        op: BlockOp::RecordBodyInvalid,
        stages: &[StageContract {
            stage: Stage::Pending,
            outcomes: &[OutcomeFamily::One(BlockOutcome::Apply, Gate::Always)],
        }],
    },
    OpContract {
        op: BlockOp::RestartBodyAvailability,
        stages: RECORD_STAGES,
    },
    OpContract {
        op: BlockOp::RetryBodyAvailability,
        stages: RECORD_STAGES,
    },
    OpContract {
        op: BlockOp::Misbehavior,
        stages: &[],
    },
    OpContract {
        op: BlockOp::ReadRange,
        stages: &[StageContract {
            stage: Stage::Pending,
            outcomes: &[
                OutcomeFamily::One(BlockOutcome::Serve, Gate::Always),
                OutcomeFamily::One(BlockOutcome::Error, Gate::Always),
            ],
        }],
    },
];

impl BlockOp {
    /// Every operation, in canonical order.
    pub const ALL: [BlockOp; 8] = [
        BlockOp::QueryNeededBlocks,
        BlockOp::SubmitBlock,
        BlockOp::RecordBodyUnavailable,
        BlockOp::RecordBodyInvalid,
        BlockOp::RestartBodyAvailability,
        BlockOp::RetryBodyAvailability,
        BlockOp::Misbehavior,
        BlockOp::ReadRange,
    ];

    /// The operation an action becomes.
    pub const fn of(action: &BlockSyncAction) -> Self {
        match action {
            BlockSyncAction::QueryNeededBlocks { .. } => BlockOp::QueryNeededBlocks,
            BlockSyncAction::SubmitBlock { .. } => BlockOp::SubmitBlock,
            BlockSyncAction::RecordBodyUnavailable { .. } => BlockOp::RecordBodyUnavailable,
            BlockSyncAction::RecordBodyInvalid { .. } => BlockOp::RecordBodyInvalid,
            BlockSyncAction::RestartBodyAvailability { .. } => BlockOp::RestartBodyAvailability,
            BlockSyncAction::RetryBodyAvailability { .. } => BlockOp::RetryBodyAvailability,
            BlockSyncAction::Misbehavior { .. } => BlockOp::Misbehavior,
        }
    }

    /// A short name for records, such as `submit_block`.
    pub const fn name(self) -> &'static str {
        match self {
            BlockOp::QueryNeededBlocks => "query_needed_blocks",
            BlockOp::SubmitBlock => "submit_block",
            BlockOp::RecordBodyUnavailable => "record_body_unavailable",
            BlockOp::RecordBodyInvalid => "record_body_invalid",
            BlockOp::RestartBodyAvailability => "restart_body_availability",
            BlockOp::RetryBodyAvailability => "retry_body_availability",
            BlockOp::Misbehavior => "misbehavior",
            BlockOp::ReadRange => "read_range",
        }
    }

    /// Whether the driver awaits this operation before it takes its next
    /// action: a query or a record. It queues a submission, and awaits a
    /// misbehavior report's enforcement only, which the simulator doesn't
    /// run.
    pub const fn awaited(self) -> bool {
        matches!(
            self,
            BlockOp::QueryNeededBlocks
                | BlockOp::RecordBodyUnavailable
                | BlockOp::RecordBodyInvalid
                | BlockOp::RestartBodyAvailability
                | BlockOp::RetryBodyAvailability
        )
    }

    /// This operation's contract.
    pub fn contract(self) -> &'static OpContract {
        // Discriminants are 0..8 in declaration order, matching `CONTRACTS`.
        &CONTRACTS[self as usize]
    }

    /// The stage a new operation starts in, or `None` for one the service
    /// records when the driver takes it.
    pub fn first_stage(self) -> Option<Stage> {
        self.contract().stages.first().map(|stage| stage.stage)
    }

    /// Every outcome this operation may ever have at `stage` for a `class`
    /// body, in canonical order, with its gate. `class` matters only to an
    /// admitted submission.
    pub fn outcomes(self, stage: Stage, class: ApplyClass) -> Vec<(BlockOutcome, Gate)> {
        self.contract()
            .stages
            .iter()
            .filter(|contract| contract.stage == stage)
            .flat_map(|contract| contract.outcomes.iter())
            .flat_map(|family| match family {
                OutcomeFamily::One(outcome, gate) => vec![(*outcome, *gate)],
                OutcomeFamily::PayloadMismatch => Commitment::ALL
                    .iter()
                    .map(|commitment| {
                        (
                            BlockOutcome::Verdict(Verdict::PayloadMismatch(*commitment)),
                            Gate::ParentCommitted,
                        )
                    })
                    .collect(),
                OutcomeFamily::Retryable => class
                    .retry_kinds()
                    .iter()
                    .map(|kind| {
                        (
                            BlockOutcome::Verdict(Verdict::Retryable(*kind)),
                            kind.gate(),
                        )
                    })
                    .collect(),
            })
            .collect()
    }

    /// The outcomes legal now for an operation at `stage` with a `class`
    /// body, under `conditions`, in canonical order.
    pub fn enabled(
        self,
        stage: Stage,
        class: ApplyClass,
        conditions: &Conditions,
    ) -> Vec<BlockOutcome> {
        if stage.needs_free_driver() && conditions.driver_awaiting {
            return Vec::new();
        }
        self.outcomes(stage, class)
            .into_iter()
            .filter(|(_, gate)| gate.open(conditions))
            .map(|(outcome, _)| outcome)
            .collect()
    }

    /// Whether [`Self::enabled`] offers `outcome`.
    pub fn check(
        self,
        stage: Stage,
        class: ApplyClass,
        conditions: &Conditions,
        outcome: BlockOutcome,
    ) -> Result<(), IllegalOutcome> {
        if self.enabled(stage, class, conditions).contains(&outcome) {
            Ok(())
        } else {
            Err(IllegalOutcome {
                op: self,
                stage,
                outcome,
            })
        }
    }
}

impl BlockOutcome {
    /// The name in a storage action identity,
    /// `storage:block:<id>:<name>`.
    pub fn name(self) -> String {
        match self {
            BlockOutcome::Answer => "answer".into(),
            BlockOutcome::Fail => "fail".into(),
            BlockOutcome::Admit => "admit".into(),
            BlockOutcome::Apply => "apply".into(),
            BlockOutcome::Serve => "serve".into(),
            BlockOutcome::Error => "error".into(),
            BlockOutcome::Verdict(verdict) => match verdict {
                Verdict::Committed => "committed".into(),
                Verdict::Duplicate => "duplicate".into(),
                Verdict::PayloadMismatch(commitment) => {
                    format!("payload-mismatch-{}", commitment.label().replace('_', "-"))
                }
                Verdict::ConsensusInvalid => "consensus-invalid".into(),
                Verdict::Retryable(kind) => format!("retryable-{}", kind.name()),
                Verdict::Ambiguous => "ambiguous".into(),
            },
        }
    }
}

/// An outcome the contract doesn't allow for an operation now.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct IllegalOutcome {
    /// The operation.
    pub op: BlockOp,
    /// Its stage.
    pub stage: Stage,
    /// The refused outcome.
    pub outcome: BlockOutcome,
}

impl fmt::Display for IllegalOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} can't complete with {} at stage {:?}",
            self.op.name(),
            self.outcome.name(),
            self.stage,
        )
    }
}

impl std::error::Error for IllegalOutcome {}

/// An operation's accepted inputs, which own what the caller handed over.
#[derive(Debug)]
pub enum BlockRequest {
    /// An action from the reactor.
    Action(BlockSyncAction),
    /// A range read from serving, with the lease it must give back.
    ReadRange(BlockRangeRead),
}

impl BlockRequest {
    /// The operation this request is for.
    pub fn op(&self) -> BlockOp {
        match self {
            BlockRequest::Action(action) => BlockOp::of(action),
            BlockRequest::ReadRange(_) => BlockOp::ReadRange,
        }
    }

    /// The submitted body's height and class, for a submission whose body
    /// has a coinbase height. A routine never submits one without.
    pub fn submitted(&self, limits: &ApplyLimits) -> Option<(block::Height, ApplyClass)> {
        match self {
            BlockRequest::Action(BlockSyncAction::SubmitBlock { block, .. }) => block
                .coinbase_height()
                .map(|height| (height, ApplyClass::of(height, limits.max_checkpoint_height))),
            _ => None,
        }
    }
}

/// One event the service sends block sync through its handle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HandleEvent {
    /// `BlockSyncEvent::ScopedNeededBlocks`, a query's answer.
    NeededBlocks {
        /// The query.
        query_id: NonZeroU64,
        /// Its scope, echoed.
        scope: BodyWorkAuthority,
        /// The highest committed block on the selected header chain.
        body_anchor: Frontier,
        /// The missing bodies.
        blocks: Vec<BlockSyncBlockMeta>,
    },
    /// `send_needed_blocks_query_failure`.
    NeededBlocksFailed {
        /// The query.
        query_id: NonZeroU64,
        /// Its scope, echoed.
        scope: BodyWorkAuthority,
    },
    /// `BlockSyncEvent::BlockApplyFinished`, a submission's verdict.
    ApplyFinished {
        /// The submission's owner.
        owner: BodyWorkOwner,
        /// Its supplier.
        source: SourceId,
        /// Its token.
        token: BlockApplyToken,
        /// The body's height.
        height: block::Height,
        /// The body's hash.
        hash: block::Hash,
        /// The verdict.
        outcome: BlockApplyOutcome,
    },
}

impl HandleEvent {
    /// The control event this sends, or `None` for a query failure, which
    /// has its own channel.
    pub fn into_control(self) -> Option<BlockSyncEvent> {
        match self {
            HandleEvent::NeededBlocks {
                query_id,
                scope,
                body_anchor,
                blocks,
            } => Some(BlockSyncEvent::ScopedNeededBlocks {
                query_id,
                scope,
                body_anchor,
                blocks,
            }),
            HandleEvent::NeededBlocksFailed { .. } => None,
            HandleEvent::ApplyFinished {
                owner,
                source,
                token,
                height,
                hash,
                outcome,
            } => Some(BlockSyncEvent::BlockApplyFinished {
                owner,
                source,
                token,
                height,
                hash,
                outcome,
            }),
        }
    }

    /// The event a control event is, or `None` for one the service never
    /// sends.
    pub fn from_control(event: BlockSyncEvent) -> Option<Self> {
        match event {
            BlockSyncEvent::ScopedNeededBlocks {
                query_id,
                scope,
                body_anchor,
                blocks,
            } => Some(HandleEvent::NeededBlocks {
                query_id,
                scope,
                body_anchor,
                blocks,
            }),
            BlockSyncEvent::BlockApplyFinished {
                owner,
                source,
                token,
                height,
                hash,
                outcome,
            } => Some(HandleEvent::ApplyFinished {
                owner,
                source,
                token,
                height,
                hash,
                outcome,
            }),
            _ => None,
        }
    }
}

impl Verdict {
    /// The outcome the driver reports for this verdict on a `class` body
    /// `hash`, which `owner` requested from `source`, as
    /// `block_commit_outcome` builds it.
    pub fn outcome(
        self,
        owner: BodyWorkOwner,
        source: SourceId,
        hash: block::Hash,
        class: ApplyClass,
    ) -> BlockApplyOutcome {
        match self {
            Verdict::Committed => BlockApplyOutcome::committed(VerifiedBodyEvidence {
                hash,
                evidence: body_outcome_evidence(b"committed", owner, source, hash, &[]),
            }),
            Verdict::Duplicate => BlockApplyOutcome::duplicate(VerifiedBodyEvidence {
                hash,
                evidence: body_outcome_evidence(b"duplicate", owner, source, hash, &[]),
            }),
            Verdict::PayloadMismatch(commitment) => {
                BlockApplyOutcome::payload_mismatch(BodyPayloadMismatch {
                    evidence: body_outcome_evidence(
                        b"payload-mismatch",
                        owner,
                        source,
                        hash,
                        commitment.label().as_bytes(),
                    ),
                    requested: hash,
                    delivered: hash,
                    kind: commitment.kind(),
                    source,
                })
            }
            Verdict::ConsensusInvalid => {
                let rule = BodyRuleId::new(class.invalid_rule());
                BlockApplyOutcome::consensus_invalid(ConsensusBodyInvalid {
                    hash,
                    evidence: intrinsic_body_invalid_evidence(hash, &rule),
                    rule,
                    source,
                })
            }
            Verdict::Retryable(kind) => retryable_outcome(owner, source, hash, kind),
            Verdict::Ambiguous => {
                retryable_outcome(owner, source, hash, RetryKind::VerifierUnavailable)
            }
        }
    }

    /// Whether this verdict commits the body.
    pub const fn commits(self) -> bool {
        matches!(self, Verdict::Committed | Verdict::Ambiguous)
    }
}

/// A retryable outcome of `kind`, as `retryable_body_outcome` builds it.
fn retryable_outcome(
    owner: BodyWorkOwner,
    source: SourceId,
    hash: block::Hash,
    kind: RetryKind,
) -> BlockApplyOutcome {
    BlockApplyOutcome::retryable(TransientBodyFailure {
        hash,
        evidence: body_outcome_evidence(
            b"retryable",
            owner,
            source,
            hash,
            transient_kind_label(kind.kind()).as_bytes(),
        ),
        kind: kind.kind(),
        availability: BodyUnavailableSummary::default(),
    })
}

/// The driver's `body_outcome_evidence`.
fn body_outcome_evidence(
    kind: &[u8],
    owner: BodyWorkOwner,
    source: SourceId,
    hash: block::Hash,
    detail: &[u8],
) -> EvidenceId {
    let mut hasher = Sha256::new();
    hasher.update(b"zakura-body-apply-outcome-v1");
    hash_bytes(&mut hasher, kind);
    hasher.update(owner.header_generation.get().to_le_bytes());
    hasher.update(owner.verified_generation.get().to_le_bytes());
    hasher.update(owner.branch.anchor_hash.0);
    hasher.update(owner.branch.target_tip_hash.0);
    hasher.update(owner.session_id.to_le_bytes());
    hasher.update(owner.request_id.get().to_le_bytes());
    hasher.update(source.digest());
    hasher.update(hash.0);
    hash_bytes(&mut hasher, detail);
    EvidenceId::from_digest(hasher.finalize().into())
}

/// The driver's `intrinsic_body_invalid_evidence`.
fn intrinsic_body_invalid_evidence(hash: block::Hash, rule: &BodyRuleId) -> EvidenceId {
    let mut hasher = Sha256::new();
    hasher.update(b"zakura-consensus-body-invalid-v1");
    hasher.update(hash.0);
    hash_bytes(&mut hasher, rule.as_str().as_bytes());
    EvidenceId::from_digest(hasher.finalize().into())
}

/// The driver's `hash_bytes`: a length prefix, then the bytes.
fn hash_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    let length = u64::try_from(bytes.len()).expect("a slice length fits u64 on supported targets");
    hasher.update(length.to_le_bytes());
    hasher.update(bytes);
}

/// The driver's `transient_failure_kind_label`.
const fn transient_kind_label(kind: TransientBodyFailureKind) -> &'static str {
    match kind {
        TransientBodyFailureKind::MissingContext => "missing_context",
        TransientBodyFailureKind::Canceled => "canceled",
        TransientBodyFailureKind::Storage => "storage",
        TransientBodyFailureKind::VerifierUnavailable => "verifier_unavailable",
        TransientBodyFailureKind::Timeout => "timeout",
        TransientBodyFailureKind::ResourceExhausted => "resource_exhausted",
    }
}
