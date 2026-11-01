//! Protocol-state features: the abstract states of the G1–G3 plan's feature
//! table, as exact [`Feature`] indices (explorer implementation plan, E2b).
//!
//! [`collector`] reads a run's observations, the registry's events and the
//! run's end, and hits a [`StateFeature`] for each state it sees. It tracks
//! full identities, such as operation IDs and work owners, to decide whether
//! two events relate. A feature keeps only the relationship: no hash, ID,
//! height or time reaches an index.
//!
//! # Index layout
//!
//! [`FEEDBACK_VERSION`] 2 fixes this layout. Each [`Family`] owns 4,096
//! indices from `family × 4096`, in [`Family::ALL`]'s order. A family splits
//! its span into blocks, one per [`StateFeature`] variant. A block's index is
//! mixed-radix over the variant's fields, in field order, each field by its
//! position in its vocabulary's `ALL`.
//!
//! | Family | First index | Block | Variant: fields |
//! |--------|-------------|-------|-----------------|
//! | `request-reset` | 0 | +0 | `HeaderRetired`: purpose × phase × reason |
//! | | | +512 | `BlockRequestStep`: last progress × step |
//! | | | +1024 | `BlockOpCompleted`: operation × stage × epoch |
//! | `completion-cancellation` | 4096 | +0 | `Completion`: service × operation × timing |
//! | | | +512 | `Disposition`: service × operation × disposition |
//! | | | +1024 | `CallEnded`: call × end |
//! | `competing-forks` | 8192 | +0 | `Fork`: state |
//! | `replacement` | 12288 | +0 | `Replacement`: kind × service × event |
//! | `budget` | 16384 | +0 | `BlockBudget`: change × level before × level after |
//! | | | +512 | `HeaderCapacity`: event × class |
//! | | | +1024 | `CadenceEmptied`: row × direction |
//! | `regulation` | 20480 | +0 | `Frame`: row × direction × label × stage |
//! | | | +2048 | `Transition`: transition |
//! | `hold` | 24576 | | none yet |
//! | `reorg-stage` | 28672 | +0 | `ReorgStage`: stage |
//! | `recovery` | 32768 | +0 | `Obligation`: namespace × point |
//! | `teardown` | 36864 | +0 | `Retired`: what |
//! | | | +16 | `Acknowledged`: service × how × dropped |
//! | | | +32 | `Baseline`: baseline |
//! | | | +48 | `IncompleteDrain` |
//!
//! A vocabulary of production names, such as [`RetireReason`], ends with
//! `Unlisted`: a name this version doesn't list. Adding a value to a
//! vocabulary moves existing indices: it moves `Unlisted`, and it changes the
//! radix of every block that uses the vocabulary. Adding a value or a block
//! therefore bumps [`FEEDBACK_VERSION`]. The layout tests pin every
//! vocabulary's size, one index per block and [`FEEDBACK_VERSION`], so such
//! a change fails them until the version and the pins move together.
//!
//! [`REQUIRED`] is the exact required-state matrix: every state the feature
//! table requires, and the features that show it, or why none does yet.

mod collector;

pub use collector::collector;

use super::feedback::Feature;
#[cfg(doc)]
use super::feedback::FEEDBACK_VERSION;
use crate::{
    model::Label,
    observe::{StorageRequest, StreamId},
};

/// How many indices each family owns.
pub const SPAN: u16 = 4096;

/// Declares a vocabulary: an enum of names, in index order, with `ALL` and
/// `name`.
macro_rules! vocabulary {
    (
        $(#[$meta:meta])*
        $name:ident {
            $( $(#[$vmeta:meta])* $variant:ident = $text:literal, )*
        }
    ) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub enum $name {
            $( $(#[$vmeta])* $variant, )*
        }

        impl $name {
            /// Every value, in index order.
            pub const ALL: &'static [$name] = &[$($name::$variant,)*];

            /// The value's name.
            pub const fn name(self) -> &'static str {
                match self {
                    $($name::$variant => $text,)*
                }
            }

            /// The value whose name is `text`, if any.
            pub fn named(text: &str) -> Option<$name> {
                $name::ALL.iter().copied().find(|value| value.name() == text)
            }

            /// Its position in [`Self::ALL`].
            #[allow(dead_code)]
            const fn position(self) -> u16 {
                self as u16
            }

            /// How many values there are.
            #[allow(dead_code)]
            const fn count() -> u16 {
                // A vocabulary has a few dozen names at most.
                $name::ALL.len() as u16
            }
        }
    };
}

/// Declares a vocabulary of production names that ends with `Unlisted`, and
/// `of`, which maps a name this version doesn't list to `Unlisted`.
macro_rules! open_vocabulary {
    (
        $(#[$meta:meta])*
        $name:ident {
            $( $(#[$vmeta:meta])* $variant:ident = $text:literal, )*
        }
    ) => {
        vocabulary! {
            $(#[$meta])*
            $name {
                $( $(#[$vmeta])* $variant = $text, )*
                /// A name this version doesn't list.
                Unlisted = "unlisted",
            }
        }

        impl $name {
            /// The value named `text`, or `Unlisted`.
            pub fn of(text: &str) -> $name {
                $name::named(text)
                    .filter(|value| *value != $name::Unlisted)
                    .unwrap_or($name::Unlisted)
            }
        }
    };
}

vocabulary! {
    /// A feature family: one row of the G1–G3 plan's feature table.
    Family {
        /// Operation kind × pending stage × reset or replacement overlap.
        RequestReset = "feature/request-reset",
        /// Completion against cancellation and owner retirement, and the
        /// result's disposition.
        CompletionCancellation = "feature/completion-cancellation",
        /// Competing offers, a shared ancestor, the winner and the reorg.
        CompetingForks = "feature/competing-forks",
        /// Connection or stream replacement, the old session's work and its
        /// late traffic.
        Replacement = "feature/replacement",
        /// A budget's level, change, denial and release.
        Budget = "feature/budget",
        /// A frame's row, direction, label and consequence, and the
        /// regulation table's transitions.
        Regulation = "feature/regulation",
        /// A held operation, its release and the resumed operation.
        Hold = "feature/hold",
        /// The stages of syncing a new branch after a reorg.
        ReorgStage = "feature/reorg-stage",
        /// An obligation's life, and the conditions it assumes.
        Recovery = "feature/recovery",
        /// Service retirement, its acknowledgement, the settled baseline
        /// and the drain.
        Teardown = "feature/teardown",
    }
}

impl Family {
    /// The family's first index.
    pub const fn first(self) -> u16 {
        self.position() * SPAN
    }
}

vocabulary! {
    /// Why a header request started: `ReactorPurpose`.
    Purpose {
        /// To admit a branch target.
        Normal = "normal",
        /// To redeliver a selected range's auxiliary metadata.
        AuxiliaryRepair = "selected-auxiliary-repair",
    }
}

open_vocabulary! {
    /// A header request's phase when it was retired.
    RetiredPhase {
        /// Receiving headers.
        Receiving = "receiving",
        /// Preparing its target.
        Preparing = "preparing",
        /// Applying its target.
        Applying = "applying",
    }
}

open_vocabulary! {
    /// Why the reactor retired a header request, as its traces name it
    /// (`HeaderRequestTerminal::label`).
    RetireReason {
        /// `target_not_retained`.
        TargetNotRetained = "target_not_retained",
        /// `no_locator_intersection`.
        NoLocatorIntersection = "no_locator_intersection",
        /// `history_pruned`.
        HistoryPruned = "history_pruned",
        /// `target_not_selected`.
        TargetNotSelected = "target_not_selected",
        /// `subscription_superseded`.
        SubscriptionSuperseded = "subscription_superseded",
        /// `subscription_closed`.
        SubscriptionClosed = "subscription_closed",
        /// `disconnected`.
        Disconnected = "disconnected",
        /// `local_error`.
        LocalError = "local_error",
        /// `malformed_response`.
        MalformedResponse = "malformed_response",
        /// `excluded_auxiliary_input`.
        ExcludedAuxiliaryInput = "excluded_auxiliary_input",
        /// `repair_obsolete`.
        RepairObsolete = "repair_obsolete",
        /// `resource_stalled`.
        ResourceStalled = "resource_stalled",
        /// `send_error`.
        SendError = "send_error",
        /// `session_replaced`.
        SessionReplaced = "session_replaced",
        /// `shutdown`.
        Shutdown = "shutdown",
        /// `snapshot_obsolete`.
        SnapshotObsolete = "snapshot_obsolete",
        /// `staging_refused`.
        StagingRefused = "staging_refused",
        /// `target_admitted`.
        TargetAdmitted = "target_admitted",
        /// `target_rejected`.
        TargetRejected = "target_rejected",
        /// `timed_out`.
        TimedOut = "timed_out",
    }
}

vocabulary! {
    /// The last progress step a block request recorded.
    RequestProgress {
        /// None yet.
        None = "none",
        /// `published`.
        Published = "published",
        /// `started`.
        Started = "started",
        /// `written`.
        Written = "written",
    }
}

open_vocabulary! {
    /// A block request step that isn't progress: a reset, an expiry, a
    /// failed delivery or a drop, by the phase it dropped in.
    RequestEvent {
        /// `reset`.
        Reset = "reset",
        /// `expired`.
        Expired = "expired",
        /// `delivery-failed`.
        DeliveryFailed = "delivery-failed",
        /// `dropped-unpublished`.
        DroppedUnpublished = "dropped-unpublished",
        /// `dropped-queued`.
        DroppedQueued = "dropped-queued",
        /// `dropped-started`.
        DroppedStarted = "dropped-started",
        /// `dropped-written`.
        DroppedWritten = "dropped-written",
        /// `dropped-skipped`.
        DroppedSkipped = "dropped-skipped",
    }
}

vocabulary! {
    /// A block operation that names a body-work epoch.
    EpochWork {
        /// `QueryNeededBlocks`, by its scope's epoch.
        Query = "query-needed-blocks",
        /// `SubmitBlock`, by its owner's epoch.
        Submit = "submit-block",
    }
}

open_vocabulary! {
    /// A block operation's last stage in storage before it completed.
    BlockStage {
        /// Pending.
        Pending = "pending",
        /// Queued for the apply limits.
        Queued = "queued",
        /// Admitted by the apply limits.
        Admitted = "admitted",
    }
}

vocabulary! {
    /// How a piece of work's body-work epoch compares with the newest one
    /// block sync has seen.
    Epoch {
        /// The newest.
        Current = "current",
        /// An older one.
        Stale = "stale",
    }
}

vocabulary! {
    /// A storage service.
    Service {
        /// Header storage.
        Header = "header",
        /// Block storage.
        Block = "block",
    }
}

vocabulary! {
    /// A storage operation's kind: [`StorageRequest`]'s variant.
    Operation {
        /// `continuation_locator`.
        ContinuationLocator = "continuation_locator",
        /// `vct_repair_context`.
        VctRepairContext = "vct_repair_context",
        /// `acquire_header_path`.
        AcquireHeaderPath = "acquire_header_path",
        /// `read_header_path`.
        ReadHeaderPath = "read_header_path",
        /// `release_header_path`.
        ReleaseHeaderPath = "release_header_path",
        /// `prepare_header_target`.
        PrepareHeaderTarget = "prepare_header_target",
        /// `apply_header_target`.
        ApplyHeaderTarget = "apply_header_target",
        /// `selected_chain_contains`.
        SelectedChainContains = "selected_chain_contains",
        /// `query_needed_blocks`.
        QueryNeededBlocks = "query_needed_blocks",
        /// `submit_block`.
        SubmitBlock = "submit_block",
        /// `record_body`.
        RecordBody = "record_body",
        /// `misbehavior`.
        Misbehavior = "misbehavior",
        /// `read_range`.
        ReadRange = "read_range",
    }
}

impl Operation {
    /// `request`'s kind.
    pub fn of(request: &StorageRequest) -> Operation {
        match request {
            StorageRequest::ContinuationLocator => Operation::ContinuationLocator,
            StorageRequest::VctRepairContext { .. } => Operation::VctRepairContext,
            StorageRequest::AcquireHeaderPath { .. } => Operation::AcquireHeaderPath,
            StorageRequest::ReadHeaderPath { .. } => Operation::ReadHeaderPath,
            StorageRequest::ReleaseHeaderPath { .. } => Operation::ReleaseHeaderPath,
            StorageRequest::PrepareHeaderTarget { .. } => Operation::PrepareHeaderTarget,
            StorageRequest::ApplyHeaderTarget { .. } => Operation::ApplyHeaderTarget,
            StorageRequest::SelectedChainContains { .. } => Operation::SelectedChainContains,
            StorageRequest::QueryNeededBlocks { .. } => Operation::QueryNeededBlocks,
            StorageRequest::SubmitBlock { .. } => Operation::SubmitBlock,
            StorageRequest::RecordBody { .. } => Operation::RecordBody,
            StorageRequest::Misbehavior { .. } => Operation::Misbehavior,
            StorageRequest::ReadRange { .. } => Operation::ReadRange,
        }
    }
}

vocabulary! {
    /// What preceded a storage operation's completion.
    Timing {
        /// Neither a cancellation request nor its owner's retirement: the
        /// completion came first.
        CallerWaiting = "caller-waiting",
        /// Its caller dropped the call first.
        AfterCancelRequest = "after-cancel-request",
        /// The reactor retired the request that owns it first.
        AfterOwnerRetired = "after-owner-retired",
    }
}

vocabulary! {
    /// What became of a storage operation's reply, or of an operation that
    /// never completed.
    Disposition {
        /// The caller took the reply.
        Taken = "taken",
        /// The caller dropped the delivered reply without taking it.
        Dropped = "dropped",
        /// The completion found its caller gone: a header operation's, or a
        /// block range read's.
        Undelivered = "undelivered",
        /// The reply reached block sync's handle.
        Delivered = "delivered",
        /// Block sync's handle was closed, so the reply was dropped.
        HandleClosed = "handle-closed",
        /// The driver took a later query, so this one gets no reply.
        Coalesced = "coalesced",
        /// The driver exited and dropped it uncompleted.
        DriverExited = "driver-exited",
        /// The service shut down and dropped it uncompleted.
        ShutDown = "shut-down",
    }
}

vocabulary! {
    /// What a header reactor's port call asks storage for.
    Call {
        /// A continuation locator.
        Locator = "locator",
        /// A VCT repair context.
        VctContext = "vct-context",
        /// A target's preparation.
        Prepare = "prepare",
        /// A prepared target's application.
        Apply = "apply",
    }
}

open_vocabulary! {
    /// How a header reactor's port call ended.
    CallEnd {
        /// `replied`.
        Replied = "replied",
        /// `port-error`.
        PortError = "port-error",
        /// `timed-out`.
        TimedOut = "timed-out",
        /// `not-submitted`.
        NotSubmitted = "not-submitted",
        /// `port-panicked`.
        PortPanicked = "port-panicked",
    }
}

vocabulary! {
    /// A competing-forks state.
    Fork {
        /// The header reactor held active requests from two peers for two
        /// different targets at once.
        DistinctOffersActive = "distinct-offers-active",
        /// Header storage prepared two different targets from the same
        /// common ancestor.
        SharedAncestorPrepared = "shared-ancestor-prepared",
        /// Header storage applied a target while the reactor held an active
        /// request for another.
        WinnerApplied = "winner-applied",
        /// Header storage applied a target whose common ancestor lies below
        /// the selected tip it last reported, and a later locator reported
        /// that target as the selected tip: a header reorg.
        HeaderReorgApplied = "header-reorg-applied",
        /// Block storage reset the verified chain onto another branch.
        VerifiedReset = "verified-reset",
    }
}

vocabulary! {
    /// What a replacement replaced.
    ReplacementKind {
        /// A peer's connection: a new connection from the same peer.
        Connection = "connection",
        /// A service stream: a new session of the same kind on the same
        /// connection.
        Stream = "stream",
    }
}

vocabulary! {
    /// The service a session serves, by its stream kind.
    StreamService {
        /// Header sync's stream.
        HeaderSync = "header-sync",
        /// Block sync's stream.
        BlockSync = "block-sync",
        /// Any other stream.
        Other = "other",
    }
}

vocabulary! {
    /// What a replacement found of the old session, or what followed it.
    ReplacementEvent {
        /// The old session had already ended.
        OldEnded = "old-ended",
        /// The old session still ran, with no outstanding request.
        OldLiveIdle = "old-live-idle",
        /// The old session still ran, with an outstanding request.
        OldLiveBusy = "old-live-busy",
        /// The node judged a frame of the old session after the
        /// replacement.
        OldTrafficAfter = "old-traffic-after",
    }
}

open_vocabulary! {
    /// A change to block sync's shared byte budget.
    BudgetChange {
        /// `reserved`.
        Reserved = "reserved",
        /// `charged`: the floor's documented overdraft.
        Charged = "charged",
        /// `released`.
        Released = "released",
    }
}

vocabulary! {
    /// A budget counter's level against its limit.
    Level {
        /// Zero.
        Empty = "empty",
        /// Above zero, below the limit.
        Below = "below",
        /// Exactly the limit.
        AtLimit = "at-limit",
        /// Above the limit.
        OverLimit = "over-limit",
    }
}

impl Level {
    /// `value`'s level against `limit`.
    pub fn of(value: u64, limit: u64) -> Level {
        match value {
            0 => Level::Empty,
            value if value < limit => Level::Below,
            value if value == limit => Level::AtLimit,
            _ => Level::OverLimit,
        }
    }
}

vocabulary! {
    /// A header storage capacity event.
    CapacityEvent {
        /// Storage refused a path for capacity: `CapacityBusy`.
        Denied = "denied",
        /// A capacity signal fired for a refused caller.
        WaiterFired = "waiter-fired",
    }
}

open_vocabulary! {
    /// A header storage capacity class.
    CapacityClass {
        /// `peer`.
        Peer = "peer",
        /// `general`.
        General = "general",
        /// `fallback`.
        Fallback = "fallback",
    }
}

vocabulary! {
    /// A tabled row, as a frame's rule names it: the row that sets its
    /// label's rule.
    Row {
        /// Block sync's `status`.
        BlockStatus = "s6v3/status",
        /// Block sync's `get-blocks`.
        BlockGetBlocks = "s6v3/get-blocks",
        /// Block sync's `block`.
        BlockBlock = "s6v3/block",
        /// Block sync's `blocks-done`.
        BlockBlocksDone = "s6v3/blocks-done",
        /// Block sync's `range-unavailable`.
        BlockRangeUnavailable = "s6v3/range-unavailable",
        /// A type block sync's stream doesn't declare.
        BlockUndeclared = "s6v3/undeclared",
        /// Header sync's `status`.
        HeaderStatus = "s5v9/status",
        /// Header sync's `subscribe-headers`.
        HeaderSubscribeHeaders = "s5v9/subscribe-headers",
        /// Header sync's `headers`.
        HeaderHeaders = "s5v9/headers",
        /// Header sync's `headers-outcome`.
        HeaderHeadersOutcome = "s5v9/headers-outcome",
        /// A type header sync's stream doesn't declare.
        HeaderUndeclared = "s5v9/undeclared",
        /// A stream or row this version doesn't list.
        Unlisted = "unlisted",
    }
}

impl Row {
    /// The row `row` of `stream` names, or the stream's undeclared row for
    /// `None`.
    pub fn of(stream: StreamId, row: Option<&str>) -> Row {
        let name = format!("{stream}/{}", row.unwrap_or("undeclared"));
        Row::named(&name)
            .filter(|row| *row != Row::Unlisted)
            .unwrap_or(Row::Unlisted)
    }
}

vocabulary! {
    /// Which way a frame travels.
    Direction {
        /// From a peer to the node.
        ToNode = "to-node",
        /// From the node to a peer.
        FromNode = "from-node",
    }
}

vocabulary! {
    /// A frame's label: [`Label`], in table order.
    FrameLabel {
        /// `undeclared-type`.
        UndeclaredType = "undeclared-type",
        /// `reserved-flags`.
        ReservedFlags = "reserved-flags",
        /// `short-payload`.
        ShortPayload = "short-payload",
        /// `long-payload`.
        LongPayload = "long-payload",
        /// `over-margin`.
        OverMargin = "over-margin",
        /// `unsolicited`.
        Unsolicited = "unsolicited",
        /// `over-owed`.
        OverOwed = "over-owed",
        /// `over-live`.
        OverLive = "over-live",
        /// `over-credit`.
        OverCredit = "over-credit",
        /// `after-close`.
        AfterClose = "after-close",
        /// `bad-cursor`.
        BadCursor = "bad-cursor",
        /// `bad-content`.
        BadContent = "bad-content",
        /// `conformant`.
        Conformant = "conformant",
    }
}

impl FrameLabel {
    /// `label`'s value.
    pub fn of(label: Label) -> FrameLabel {
        FrameLabel::named(label.name()).expect("FrameLabel lists every label by name")
    }
}

vocabulary! {
    /// How far a frame got, and the consequence that followed it.
    FrameStage {
        /// The sender's role models labelled it.
        Sent = "sent",
        /// The node's reader reached the bytes that decide its label.
        Judged = "judged",
        /// The node closed the connection with a fault cause after judging
        /// it, before judging another frame there.
        FaultCloseAfter = "fault-close-after",
        /// The node closed the connection with another cause after judging
        /// it, before judging another frame there.
        OtherCloseAfter = "other-close-after",
        /// The node asked storage to punish the peer after judging it,
        /// before judging another frame there.
        MisbehaviorAfter = "misbehavior-after",
        /// The run ended with its connection open.
        Unpunished = "unpunished",
    }
}

vocabulary! {
    /// A regulation table's state change on a header-sync stream.
    Transition {
        /// The node recorded its own `Open`.
        OpenIssued = "open-issued",
        /// The node removed an `Open` it never wrote.
        OpenRetracted = "open-retracted",
        /// The node recorded its own `Grant`.
        GrantIssued = "grant-issued",
        /// The publisher applied the peer's `Open`.
        OpenApplied = "open-applied",
        /// The publisher applied the peer's `Close`.
        CloseApplied = "close-applied",
        /// The publisher applied the peer's `Grant`.
        GrantApplied = "grant-applied",
        /// The service claimed a reserved page.
        PageClaimed = "page-claimed",
        /// A subscription the node opened ended.
        EndClaimed = "end-claimed",
        /// A subscription the peer opened ended.
        PublicationEnded = "publication-ended",
        /// The precheck reserved a page.
        HeaderReserved = "header-reserved",
        /// The precheck admitted a terminal outcome.
        HeaderTerminal = "header-terminal",
        /// The precheck refused a header: `unknown_message_type`.
        RefusedUnknownMessageType = "refused/unknown_message_type",
        /// The precheck refused a header: `reserved_flags`.
        RefusedReservedFlags = "refused/reserved_flags",
        /// The precheck refused a header: `payload_too_short`.
        RefusedPayloadTooShort = "refused/payload_too_short",
        /// The precheck refused a header: `unsolicited`.
        RefusedUnsolicited = "refused/unsolicited",
        /// The precheck refused a header: `above_reservation`.
        RefusedAboveReservation = "refused/above_reservation",
        /// The precheck refused a header: `above_owed`.
        RefusedAboveOwed = "refused/above_owed",
        /// The precheck refused a header for a reason this version doesn't
        /// list.
        RefusedUnlisted = "refused/unlisted",
    }
}

impl Transition {
    /// The refusal for `reason` (`FrameRejection::label`).
    pub fn refused(reason: &str) -> Transition {
        Transition::named(&format!("refused/{reason}"))
            .filter(|transition| *transition != Transition::RefusedUnlisted)
            .unwrap_or(Transition::RefusedUnlisted)
    }
}

vocabulary! {
    /// A stage of block sync's work on a new branch, after its body-work
    /// epoch first advanced, for work of the newest epoch.
    ReorgStage {
        /// Block sync asked storage which bodies it needs.
        Query = "query",
        /// Block sync published a body request.
        Request = "request",
        /// The node judged a peer's frame carrying a body its session
        /// requested.
        Receive = "receive",
        /// A routine handed a body to the sequencer.
        Handoff = "handoff",
        /// The sequencer submitted a body to storage.
        Submit = "submit",
        /// Storage committed a submission.
        VerdictCommitted = "verdict-committed",
        /// Storage gave a submission another verdict.
        VerdictOther = "verdict-other",
    }
}

open_vocabulary! {
    /// A property ID's first segment.
    Namespace {
        /// `sim`.
        Sim = "sim",
        /// `harness`.
        Harness = "harness",
        /// `discovery`.
        Discovery = "discovery",
        /// `header-sync`.
        HeaderSync = "header-sync",
        /// `block-sync`.
        BlockSync = "block-sync",
        /// `session`.
        Session = "session",
        /// `race`.
        Race = "race",
    }
}

vocabulary! {
    /// A point in an obligation's life: `Lifecycle` without `Applicable`.
    ObligationPoint {
        /// A check opened an obligation.
        Opened = "opened",
        /// An observation discharged one: progress.
        Discharged = "discharged",
        /// One expired undischarged: its deadline passed.
        Expired = "expired",
    }
}

vocabulary! {
    /// A service that retired.
    Retirement {
        /// A block-sync routine returned its work and ended.
        RoutineEnded = "routine-ended",
        /// Block sync's sequencer closed.
        SequencerClosed = "sequencer-closed",
        /// A connection's workers drained and its peer was unregistered.
        ConnectionTornDown = "connection-torn-down",
    }
}

vocabulary! {
    /// How a storage service acknowledged its caller's retirement.
    Acknowledgement {
        /// A reply found the caller's handle closed, so the driver exited.
        DriverExited = "driver-exited",
        /// The service shut down.
        ShutDown = "shut-down",
    }
}

vocabulary! {
    /// Whether an acknowledgement dropped operations.
    Dropped {
        /// None.
        None = "none",
        /// Some operation or deferred action.
        Some = "some",
    }
}

vocabulary! {
    /// Block sync's owner state at run end, once its sequencer closed.
    Baseline {
        /// No budget byte reserved and no body held.
        Settled = "settled",
        /// Some budget byte reserved or body held.
        Unsettled = "unsettled",
    }
}

/// One protocol state: a feature of one family, with every field from a
/// bounded vocabulary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StateFeature {
    /// The header reactor retired a request of `purpose` in `phase` for
    /// `reason`.
    HeaderRetired {
        /// Why it started.
        purpose: Purpose,
        /// Its phase.
        phase: RetiredPhase,
        /// Why it was retired.
        reason: RetireReason,
    },
    /// A block request whose last progress step was `last` recorded `step`.
    BlockRequestStep {
        /// Its last progress step.
        last: RequestProgress,
        /// The step.
        step: RequestEvent,
    },
    /// Block storage completed `op` from `stage`, for work of `epoch`.
    BlockOpCompleted {
        /// The operation.
        op: EpochWork,
        /// Its stage before the completion.
        stage: BlockStage,
        /// Its work's epoch then.
        epoch: Epoch,
    },
    /// `service` completed `op`, with `timing` before it.
    Completion {
        /// The service.
        service: Service,
        /// The operation's kind.
        op: Operation,
        /// What preceded the completion.
        timing: Timing,
    },
    /// What became of `op`'s reply, or of `op`.
    Disposition {
        /// The service.
        service: Service,
        /// The operation's kind.
        op: Operation,
        /// What became of it.
        disposition: Disposition,
    },
    /// A header reactor's port call for `call` ended with `end`.
    CallEnded {
        /// What it asked for.
        call: Call,
        /// How it ended.
        end: CallEnd,
    },
    /// A competing-forks state.
    Fork(Fork),
    /// `kind` replacement of a `service` session, and `event`.
    Replacement {
        /// What was replaced.
        kind: ReplacementKind,
        /// The session's service.
        service: StreamService,
        /// What the replacement found, or what followed it.
        event: ReplacementEvent,
    },
    /// Block sync's byte budget made `change` from `before` to `after`.
    BlockBudget {
        /// The change.
        change: BudgetChange,
        /// The counter's level before it.
        before: Level,
        /// The counter's level after it.
        after: Level,
    },
    /// Header storage capacity `event` for `class`.
    HeaderCapacity {
        /// What happened.
        event: CapacityEvent,
        /// The capacity class.
        class: CapacityClass,
    },
    /// A `row` frame travelling `direction` emptied its cadence bucket.
    CadenceEmptied {
        /// The frame's row.
        row: Row,
        /// Which way it travelled.
        direction: Direction,
    },
    /// A `row` frame labelled `label`, travelling `direction`, reached
    /// `stage`.
    Frame {
        /// Its rule's row.
        row: Row,
        /// Which way it travelled.
        direction: Direction,
        /// Its label.
        label: FrameLabel,
        /// How far it got, or what followed.
        stage: FrameStage,
    },
    /// A regulation table changed state.
    Transition(Transition),
    /// After block sync's body-work epoch first advanced, its newest-epoch
    /// work reached `stage`.
    ReorgStage(ReorgStage),
    /// An obligation of a property in `namespace` reached `point`.
    Obligation {
        /// The property ID's first segment.
        namespace: Namespace,
        /// The point.
        point: ObligationPoint,
    },
    /// A service retired.
    Retired(Retirement),
    /// `service` acknowledged its caller's retirement by `how`.
    Acknowledged {
        /// The storage service.
        service: Service,
        /// How.
        how: Acknowledgement,
        /// Whether it dropped any operation.
        dropped: Dropped,
    },
    /// Block sync's owner state at run end, once its sequencer closed.
    Baseline(Baseline),
    /// The drain after the step budget stopped at its bound with tasks
    /// still ready.
    IncompleteDrain,
}

/// The mixed-radix index of `digits`, each a (value, radix) pair, most
/// significant first.
const fn mix(digits: &[(u16, u16)]) -> u16 {
    let mut index = 0;
    let mut at = 0;
    while at < digits.len() {
        index = index * digits[at].1 + digits[at].0;
        at += 1;
    }
    index
}

/// A digit for `value` of vocabulary `$v`.
macro_rules! digit {
    ($value:expr, $v:ty) => {
        ($value.position(), <$v>::count())
    };
}

impl StateFeature {
    /// The family the feature belongs to.
    pub const fn family(self) -> Family {
        match self {
            StateFeature::HeaderRetired { .. }
            | StateFeature::BlockRequestStep { .. }
            | StateFeature::BlockOpCompleted { .. } => Family::RequestReset,
            StateFeature::Completion { .. }
            | StateFeature::Disposition { .. }
            | StateFeature::CallEnded { .. } => Family::CompletionCancellation,
            StateFeature::Fork(_) => Family::CompetingForks,
            StateFeature::Replacement { .. } => Family::Replacement,
            StateFeature::BlockBudget { .. }
            | StateFeature::HeaderCapacity { .. }
            | StateFeature::CadenceEmptied { .. } => Family::Budget,
            StateFeature::Frame { .. } | StateFeature::Transition(_) => Family::Regulation,
            StateFeature::ReorgStage(_) => Family::ReorgStage,
            StateFeature::Obligation { .. } => Family::Recovery,
            StateFeature::Retired(_)
            | StateFeature::Acknowledged { .. }
            | StateFeature::Baseline(_)
            | StateFeature::IncompleteDrain => Family::Teardown,
        }
    }

    /// The first index of the feature's block within its family's span.
    pub const fn block(self) -> u16 {
        match self {
            StateFeature::HeaderRetired { .. }
            | StateFeature::Completion { .. }
            | StateFeature::Fork(_)
            | StateFeature::Replacement { .. }
            | StateFeature::BlockBudget { .. }
            | StateFeature::Frame { .. }
            | StateFeature::ReorgStage(_)
            | StateFeature::Obligation { .. }
            | StateFeature::Retired(_) => 0,
            StateFeature::Acknowledged { .. } => 16,
            StateFeature::Baseline(_) => 32,
            StateFeature::IncompleteDrain => 48,
            StateFeature::BlockRequestStep { .. }
            | StateFeature::Disposition { .. }
            | StateFeature::HeaderCapacity { .. } => 512,
            StateFeature::BlockOpCompleted { .. }
            | StateFeature::CallEnded { .. }
            | StateFeature::CadenceEmptied { .. } => 1024,
            StateFeature::Transition(_) => 2048,
        }
    }

    /// The feature's position within its block.
    fn offset(self) -> u16 {
        match self {
            StateFeature::HeaderRetired {
                purpose,
                phase,
                reason,
            } => mix(&[
                digit!(purpose, Purpose),
                digit!(phase, RetiredPhase),
                digit!(reason, RetireReason),
            ]),
            StateFeature::BlockRequestStep { last, step } => {
                mix(&[digit!(last, RequestProgress), digit!(step, RequestEvent)])
            }
            StateFeature::BlockOpCompleted { op, stage, epoch } => mix(&[
                digit!(op, EpochWork),
                digit!(stage, BlockStage),
                digit!(epoch, Epoch),
            ]),
            StateFeature::Completion {
                service,
                op,
                timing,
            } => mix(&[
                digit!(service, Service),
                digit!(op, Operation),
                digit!(timing, Timing),
            ]),
            StateFeature::Disposition {
                service,
                op,
                disposition,
            } => mix(&[
                digit!(service, Service),
                digit!(op, Operation),
                digit!(disposition, Disposition),
            ]),
            StateFeature::CallEnded { call, end } => {
                mix(&[digit!(call, Call), digit!(end, CallEnd)])
            }
            StateFeature::Fork(fork) => fork.position(),
            StateFeature::Replacement {
                kind,
                service,
                event,
            } => mix(&[
                digit!(kind, ReplacementKind),
                digit!(service, StreamService),
                digit!(event, ReplacementEvent),
            ]),
            StateFeature::BlockBudget {
                change,
                before,
                after,
            } => mix(&[
                digit!(change, BudgetChange),
                digit!(before, Level),
                digit!(after, Level),
            ]),
            StateFeature::HeaderCapacity { event, class } => {
                mix(&[digit!(event, CapacityEvent), digit!(class, CapacityClass)])
            }
            StateFeature::CadenceEmptied { row, direction } => {
                mix(&[digit!(row, Row), digit!(direction, Direction)])
            }
            StateFeature::Frame {
                row,
                direction,
                label,
                stage,
            } => mix(&[
                digit!(row, Row),
                digit!(direction, Direction),
                digit!(label, FrameLabel),
                digit!(stage, FrameStage),
            ]),
            StateFeature::Transition(transition) => transition.position(),
            StateFeature::ReorgStage(stage) => stage.position(),
            StateFeature::Obligation { namespace, point } => {
                mix(&[digit!(namespace, Namespace), digit!(point, ObligationPoint)])
            }
            StateFeature::Retired(retirement) => retirement.position(),
            StateFeature::Acknowledged {
                service,
                how,
                dropped,
            } => mix(&[
                digit!(service, Service),
                digit!(how, Acknowledgement),
                digit!(dropped, Dropped),
            ]),
            StateFeature::Baseline(baseline) => baseline.position(),
            StateFeature::IncompleteDrain => 0,
        }
    }

    /// The feature's index.
    pub fn feature(self) -> Feature {
        Feature::new(self.family().first() + self.block() + self.offset())
    }

    /// Every state feature, for the layout tests and for decoding.
    pub fn all() -> Vec<StateFeature> {
        let mut all = Vec::new();
        for &purpose in Purpose::ALL {
            for &phase in RetiredPhase::ALL {
                for &reason in RetireReason::ALL {
                    all.push(StateFeature::HeaderRetired {
                        purpose,
                        phase,
                        reason,
                    });
                }
            }
        }
        for &last in RequestProgress::ALL {
            for &step in RequestEvent::ALL {
                all.push(StateFeature::BlockRequestStep { last, step });
            }
        }
        for &op in EpochWork::ALL {
            for &stage in BlockStage::ALL {
                for &epoch in Epoch::ALL {
                    all.push(StateFeature::BlockOpCompleted { op, stage, epoch });
                }
            }
        }
        for &service in Service::ALL {
            for &op in Operation::ALL {
                for &timing in Timing::ALL {
                    all.push(StateFeature::Completion {
                        service,
                        op,
                        timing,
                    });
                }
                for &disposition in Disposition::ALL {
                    all.push(StateFeature::Disposition {
                        service,
                        op,
                        disposition,
                    });
                }
            }
        }
        for &call in Call::ALL {
            for &end in CallEnd::ALL {
                all.push(StateFeature::CallEnded { call, end });
            }
        }
        all.extend(Fork::ALL.iter().copied().map(StateFeature::Fork));
        for &kind in ReplacementKind::ALL {
            for &service in StreamService::ALL {
                for &event in ReplacementEvent::ALL {
                    all.push(StateFeature::Replacement {
                        kind,
                        service,
                        event,
                    });
                }
            }
        }
        for &change in BudgetChange::ALL {
            for &before in Level::ALL {
                for &after in Level::ALL {
                    all.push(StateFeature::BlockBudget {
                        change,
                        before,
                        after,
                    });
                }
            }
        }
        for &event in CapacityEvent::ALL {
            for &class in CapacityClass::ALL {
                all.push(StateFeature::HeaderCapacity { event, class });
            }
        }
        for &row in Row::ALL {
            for &direction in Direction::ALL {
                all.push(StateFeature::CadenceEmptied { row, direction });
                for &label in FrameLabel::ALL {
                    for &stage in FrameStage::ALL {
                        all.push(StateFeature::Frame {
                            row,
                            direction,
                            label,
                            stage,
                        });
                    }
                }
            }
        }
        all.extend(
            Transition::ALL
                .iter()
                .copied()
                .map(StateFeature::Transition),
        );
        all.extend(
            ReorgStage::ALL
                .iter()
                .copied()
                .map(StateFeature::ReorgStage),
        );
        for &namespace in Namespace::ALL {
            for &point in ObligationPoint::ALL {
                all.push(StateFeature::Obligation { namespace, point });
            }
        }
        all.extend(Retirement::ALL.iter().copied().map(StateFeature::Retired));
        for &service in Service::ALL {
            for &how in Acknowledgement::ALL {
                for &dropped in Dropped::ALL {
                    all.push(StateFeature::Acknowledged {
                        service,
                        how,
                        dropped,
                    });
                }
            }
        }
        all.extend(Baseline::ALL.iter().copied().map(StateFeature::Baseline));
        all.push(StateFeature::IncompleteDrain);
        all
    }

    /// The state feature at `feature`'s index, if any.
    pub fn decode(feature: Feature) -> Option<StateFeature> {
        StateFeature::all()
            .into_iter()
            .find(|state| state.feature() == feature)
    }
}

/// Where a required state stands.
#[derive(Clone, Copy, Debug)]
pub enum Coverage {
    /// The collector hits a feature for it, and a deterministic witness
    /// reaches it.
    Collected {
        /// The harnesses whose witnesses reach it.
        harnesses: &'static [&'static str],
        /// True if a harness's fuzz decoder draws a witness scenario, so a
        /// fuzz target over the harness reaches it. False if only an
        /// explicit scenario does, until a generator draws it.
        drawn: bool,
        /// Whether a state feature shows it.
        shows: fn(&StateFeature) -> bool,
    },
    /// Nothing collects it yet.
    Waiting {
        /// The chunk or source that must land first.
        on: &'static str,
        /// What's missing.
        why: &'static str,
    },
}

/// One required state of the feature table.
#[derive(Clone, Copy, Debug)]
pub struct RequiredState {
    /// Its family.
    pub family: Family,
    /// The state, in the feature table's words.
    pub state: &'static str,
    /// Where it stands.
    pub coverage: Coverage,
}

/// The block-sync storage harness.
const BLOCK: &str = "block-sync/storage";

/// The header-sync storage harness.
const HEADER_STORAGE: &str = "header-sync/storage";

/// The header-sync wire harness.
const HEADER_WIRE: &str = "header-sync/wire";

/// A required state that `shows` collects, reached by `harnesses`'
/// witnesses.
const fn collected(
    family: Family,
    state: &'static str,
    harnesses: &'static [&'static str],
    drawn: bool,
    shows: fn(&StateFeature) -> bool,
) -> RequiredState {
    RequiredState {
        family,
        state,
        coverage: Coverage::Collected {
            harnesses,
            drawn,
            shows,
        },
    }
}

/// A required state that waits `on` a chunk or source, because of `why`.
const fn waiting(
    family: Family,
    state: &'static str,
    on: &'static str,
    why: &'static str,
) -> RequiredState {
    RequiredState {
        family,
        state,
        coverage: Coverage::Waiting { on, why },
    }
}

/// The exact required-state matrix: every state the G1–G3 plan's feature
/// table requires, in table order. `tests/explore_state.rs` runs each
/// collected state's witness.
pub const REQUIRED: &[RequiredState] = &[
    collected(
        Family::RequestReset,
        "header request retired at its pending stage for a reset or replacement",
        &[HEADER_STORAGE, HEADER_WIRE],
        true,
        |f| {
            matches!(
                f,
                StateFeature::HeaderRetired {
                    reason: RetireReason::SnapshotObsolete
                        | RetireReason::SessionReplaced
                        | RetireReason::Disconnected,
                    ..
                }
            )
        },
    ),
    collected(
        Family::RequestReset,
        "block request reset at its pending stage",
        &[BLOCK],
        true,
        |f| {
            matches!(
                f,
                StateFeature::BlockRequestStep {
                    step: RequestEvent::Reset,
                    ..
                }
            )
        },
    ),
    collected(
        Family::RequestReset,
        "block operation pending across an epoch reset, completed stale",
        &[BLOCK],
        true,
        |f| {
            matches!(
                f,
                StateFeature::BlockOpCompleted {
                    epoch: Epoch::Stale,
                    ..
                }
            )
        },
    ),
    collected(
        Family::CompletionCancellation,
        "completion before cancellation",
        &[BLOCK, HEADER_STORAGE, HEADER_WIRE],
        true,
        |f| {
            matches!(
                f,
                StateFeature::Completion {
                    timing: Timing::CallerWaiting,
                    ..
                }
            )
        },
    ),
    collected(
        Family::CompletionCancellation,
        "completion after cancellation request",
        &[HEADER_STORAGE],
        false,
        |f| {
            matches!(
                f,
                StateFeature::Completion {
                    timing: Timing::AfterCancelRequest,
                    ..
                }
            )
        },
    ),
    waiting(
        Family::CompletionCancellation,
        "completion after cancellation acknowledgement",
        "M6a and G2a",
        "no storage port acknowledges a cancellation on main",
    ),
    collected(
        Family::CompletionCancellation,
        "completion after owner retirement",
        &[HEADER_STORAGE],
        false,
        |f| {
            matches!(
                f,
                StateFeature::Completion {
                    timing: Timing::AfterOwnerRetired,
                    ..
                }
            )
        },
    ),
    collected(
        Family::CompletionCancellation,
        "result disposition",
        &[BLOCK, HEADER_STORAGE, HEADER_WIRE],
        true,
        |f| matches!(f, StateFeature::Disposition { .. }),
    ),
    collected(
        Family::CompetingForks,
        "two distinct peer offers",
        &[HEADER_STORAGE],
        false,
        |f| *f == StateFeature::Fork(Fork::DistinctOffersActive),
    ),
    collected(
        Family::CompetingForks,
        "shared ancestor",
        &[HEADER_STORAGE],
        false,
        |f| *f == StateFeature::Fork(Fork::SharedAncestorPrepared),
    ),
    waiting(
        Family::CompetingForks,
        "stronger, weaker or equal work",
        "O2 and G2a",
        "no observation carries chain work, and every fixture header has the Regtest genesis \
         difficulty, so competing branches differ only in length; an observation must report \
         each prepared target's suffix work, and G2a's controls must offer branches of more, \
         less and equal work",
    ),
    collected(
        Family::CompetingForks,
        "selected winner",
        &[HEADER_STORAGE],
        false,
        |f| *f == StateFeature::Fork(Fork::WinnerApplied),
    ),
    collected(
        Family::CompetingForks,
        "reorg",
        &[HEADER_STORAGE, BLOCK],
        true,
        |f| {
            matches!(
                f,
                StateFeature::Fork(Fork::HeaderReorgApplied | Fork::VerifiedReset)
            )
        },
    ),
    collected(
        Family::Replacement,
        "connection replacement by work stage",
        &[BLOCK],
        false,
        |f| {
            matches!(
                f,
                StateFeature::Replacement {
                    kind: ReplacementKind::Connection,
                    event: ReplacementEvent::OldLiveIdle | ReplacementEvent::OldLiveBusy,
                    ..
                }
            )
        },
    ),
    waiting(
        Family::Replacement,
        "stream replacement",
        "M6a and G2b",
        "no harness replaces a service stream; G2b adds the action once M6a supplies \
         its identities",
    ),
    waiting(
        Family::Replacement,
        "old-session traffic delivered",
        "G2b",
        "no witness delivers a replaced session's frame; G2b's replacement actions \
         reach it",
    ),
    collected(Family::Budget, "empty", &[BLOCK], true, |f| {
        matches!(
            f,
            StateFeature::BlockBudget {
                before: Level::Empty,
                ..
            }
        )
    }),
    collected(Family::Budget, "below limit", &[BLOCK], true, |f| {
        matches!(
            f,
            StateFeature::BlockBudget {
                after: Level::Below,
                ..
            }
        )
    }),
    waiting(
        Family::Budget,
        "exactly limit",
        "G2a",
        "no witness on main reserves the budget to its limit exactly; G2a's boundary \
         fixtures size the bodies to it",
    ),
    collected(Family::Budget, "released", &[BLOCK], true, |f| {
        matches!(
            f,
            StateFeature::BlockBudget {
                change: BudgetChange::Released,
                ..
            }
        )
    }),
    collected(
        Family::Budget,
        "documented allowance: the floor's overdraft",
        &[BLOCK],
        true,
        |f| {
            matches!(
                f,
                StateFeature::BlockBudget {
                    change: BudgetChange::Charged,
                    ..
                }
            )
        },
    ),
    collected(
        Family::Budget,
        "documented allowance: a cadence bucket emptied",
        &[HEADER_WIRE],
        false,
        |f| matches!(f, StateFeature::CadenceEmptied { .. }),
    ),
    waiting(
        Family::Budget,
        "attempted over-limit and denied",
        "G2a",
        "block sync's tap logs no refused reservation, and no header witness fills a \
         capacity class; G2a's pressure fixtures reach them",
    ),
    collected(
        Family::Regulation,
        "family, row and direction × model label × consequence",
        &[BLOCK, HEADER_WIRE],
        true,
        |f| {
            matches!(
                f,
                StateFeature::Frame {
                    stage: FrameStage::FaultCloseAfter
                        | FrameStage::OtherCloseAfter
                        | FrameStage::MisbehaviorAfter
                        | FrameStage::Unpunished,
                    ..
                }
            )
        },
    ),
    collected(
        Family::Regulation,
        "a model label's frame followed by a fault close",
        &[HEADER_WIRE],
        true,
        |f| {
            matches!(
                f,
                StateFeature::Frame {
                    stage: FrameStage::FaultCloseAfter,
                    ..
                }
            )
        },
    ),
    collected(
        Family::Regulation,
        "regulation table transitions",
        &[HEADER_WIRE],
        true,
        |f| matches!(f, StateFeature::Transition(_)),
    ),
    waiting(
        Family::Regulation,
        "conformant crossed updates",
        "G2b",
        "no peer script crosses an update with an outcome; G2b's protocol actions add it",
    ),
    waiting(
        Family::Hold,
        "operation kind × blocked × release × resumed operation",
        "G2a",
        "the run logs no hold or release; G2a's storage controls add them",
    ),
    collected(Family::ReorgStage, "query", &[BLOCK], true, |f| {
        *f == StateFeature::ReorgStage(ReorgStage::Query)
    }),
    collected(Family::ReorgStage, "request", &[BLOCK], true, |f| {
        *f == StateFeature::ReorgStage(ReorgStage::Request)
    }),
    collected(Family::ReorgStage, "receive", &[BLOCK], true, |f| {
        *f == StateFeature::ReorgStage(ReorgStage::Receive)
    }),
    collected(
        Family::ReorgStage,
        "decode, as its handoff",
        &[BLOCK],
        true,
        |f| *f == StateFeature::ReorgStage(ReorgStage::Handoff),
    ),
    collected(Family::ReorgStage, "submit", &[BLOCK], true, |f| {
        *f == StateFeature::ReorgStage(ReorgStage::Submit)
    }),
    collected(Family::ReorgStage, "verdict", &[BLOCK], true, |f| {
        matches!(
            f,
            StateFeature::ReorgStage(ReorgStage::VerdictCommitted | ReorgStage::VerdictOther)
        )
    }),
    waiting(
        Family::ReorgStage,
        "header locator, prepare and apply stages",
        "P8",
        "no harness runs header and block sync together",
    ),
    waiting(
        Family::Recovery,
        "conditions established",
        "G2a",
        "no harness has a recovery phase; G2a's recovery contract adds its boundary",
    ),
    collected(
        Family::Recovery,
        "obligation opened",
        &[HEADER_WIRE],
        true,
        |f| {
            matches!(
                f,
                StateFeature::Obligation {
                    point: ObligationPoint::Opened,
                    ..
                }
            )
        },
    ),
    collected(
        Family::Recovery,
        "progress observed",
        &[HEADER_WIRE],
        true,
        |f| {
            matches!(
                f,
                StateFeature::Obligation {
                    point: ObligationPoint::Discharged,
                    ..
                }
            )
        },
    ),
    waiting(
        Family::Recovery,
        "deadline expired",
        "G2a",
        "no witness on main expires an obligation without a planted bug; G2a's recovery \
         witnesses reach it",
    ),
    waiting(
        Family::Recovery,
        "assumption lost",
        "G2a",
        "the registry emits no event when a property's assumptions stop holding",
    ),
    collected(
        Family::Teardown,
        "service retirement",
        &[BLOCK, HEADER_WIRE],
        true,
        |f| matches!(f, StateFeature::Retired(_)),
    ),
    waiting(
        Family::Teardown,
        "external acknowledgement",
        "G2a",
        "no witness on main leaves a storage call pending at teardown, so no driver \
         exits or shuts down with one; G2a's storage holds keep the call pending",
    ),
    collected(Family::Teardown, "settled baseline", &[BLOCK], false, |f| {
        *f == StateFeature::Baseline(Baseline::Settled)
    }),
    waiting(
        Family::Teardown,
        "incomplete drain",
        "G2a",
        "no witness on main stops its drain at the bound",
    ),
];
