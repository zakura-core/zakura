//! Every place a header check or the registry constructs `NotApplicable`
//! for a header ID (C20, item 6).
//!
//! [`NA_REASONS`] names each producer by its file and function, and each
//! construction in it by its exact source line, in order, with what it
//! means. `tests/header_acceptance.rs` parses every file under `src/` and
//! finds each `Verdict::NotApplicable` and `PropertyOutcome::NotApplicable`
//! in expression position, inside known assertion and pattern macros too,
//! leaving out comparisons and patterns. Each one must be listed here at
//! its line, with its producer's fingerprint, or sit in a file [`NOT_HEADER`] says no
//! header check reaches, so a new producer fails until it's classified.
//!
//! A [`NaKind::Step`] or [`NaKind::Run`] construction is a reason a report
//! can end `NotApplicable` for, and each owes a test showing it at its
//! trigger. Under C1 a report names its reason, so those tests wait for E1.
//! A [`NaKind::MissingEvidence`] construction follows `missing_evidence`,
//! which fails the run as a harness failure, so it's never a reason.
//!
//! The scan and its pins catch honest edits (Adam, 2026-11-09). Rust written
//! to hide a construction from them, such as raw identifiers, `include!`
//! inside another macro, alias chains to the verdict types, a shadowed
//! standard macro or `#[path]` games, is out of model. Once E1 types the
//! reason `NotApplicable` carries, its variants become the inventory.

/// What a `NotApplicable` construction means.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NaKind {
    /// The check's trigger didn't occur at this step. A run where it never
    /// occurs reports the property `NotApplicable` for this reason.
    Step,
    /// The registry's run-level outcome.
    Run,
    /// The value returned after `missing_evidence`, which fails the run.
    MissingEvidence,
}

/// One construction: what it means, and its source line, trimmed.
#[derive(Clone, Copy, Debug)]
pub struct NaConstruction {
    /// What it means.
    pub kind: NaKind,
    /// Why, in words.
    pub meaning: &'static str,
    /// The trimmed source line it's on, which the scan compares exactly,
    /// in order. What triggers it is its producer's whole text, which
    /// [`NaProducer::fingerprint`] pins.
    pub line: &'static str,
}

/// One function's `NotApplicable` constructions.
#[derive(Clone, Copy, Debug)]
pub struct NaProducer {
    /// The source file, relative to the crate.
    pub file: &'static str,
    /// The function, qualified by its `impl` type or inline module where it
    /// has one, such as `check` or `Type::check`.
    pub function: &'static str,
    /// The fingerprint of its text, signature to closing brace, as a token
    /// stream: the first 16 hex digits of its SHA-256, which the scan
    /// recomputes. Any edit to the function but a comment or whitespace
    /// changes it, so the meanings below were read against exactly this
    /// text (Adam's C20 fingerprint rule, 2026-11-09). A function it calls
    /// is out of scope, that function's owner's contract (Rule 23).
    pub fingerprint: &'static str,
    /// Its constructions, in source order.
    pub constructions: &'static [NaConstruction],
}

/// The fingerprint, as [`NaProducer::fingerprint`] computes one, of the
/// verdict types' definitions, `Verdict`'s and `PropertyOutcome`'s,
/// attributes included, and every `impl` block for them in the crate, in
/// file and line order. A derive, a `cfg_attr` or a hand-written impl could
/// construct `NotApplicable` where the scan can't see it, so any such change
/// fails until it's reread. The scan refuses an alias of either type and
/// an `include!`, which could hide one.
pub const VERDICT_FINGERPRINT: &str = "800d01a646674cb4";

/// A file whose `NotApplicable` constructions no header ID's check reaches.
#[derive(Clone, Copy, Debug)]
pub struct NotHeader {
    /// The file, relative to the crate.
    pub file: &'static str,
    /// Its module's name.
    pub module: &'static str,
    /// Why no header check reaches it.
    pub why: &'static str,
    /// What the scan checks of that.
    pub basis: Basis,
}

/// How the scan checks a [`NotHeader`] file is out of header reach.
#[derive(Clone, Copy, Debug)]
pub enum Basis {
    /// Exactly these `(file, trimmed line)` references name its module's
    /// identifier, in a path or an import, once per reference. A new
    /// reference anywhere, in a header file through a grouped import, in a
    /// new helper, or a re-export or call added to a file already listed,
    /// fails the scan.
    NamedOnlyBy(&'static [(&'static str, &'static str)]),
    /// It names neither `Verdict` nor `PropertyOutcome`, so its
    /// `NotApplicable` is another type's.
    NamesNoVerdict,
}

/// The files whose `NotApplicable` constructions no header ID's check
/// reaches. Every other file's constructions must be in [`NA_REASONS`].
/// The scan checks each one's [`Basis`], so a header check that starts
/// using one of these, directly or through a new helper, needs it
/// reclassified.
pub static NOT_HEADER: &[NotHeader] = &[
    NotHeader {
        file: "src/domain/block_sync.rs",
        module: "block_sync",
        why: "block sync's checks",
        basis: Basis::NamedOnlyBy(&[
            ("src/harness/block_node/tap.rs", "domain::block_sync::OwnerInput,"),
            ("src/harness/block_node/view.rs", "use crate::domain::block_sync::OwnerModel;"),
            ("src/harness/block_semantic.rs", "block_sync::{BlockScenario, MirrorSpec, SequencerPause},"),
            ("src/harness/block_sync.rs", "block_sync::{resource_properties, Limits, OwnerModel, OwnerView},"),
            ("src/harness/coverage.rs", "property: crate::domain::block_sync::BUDGET_LEDGERS_AGREE,"),
            ("src/harness/coverage.rs", "property: crate::domain::block_sync::LOOKAHEAD_OVERSHOOT_BOUND,"),
            ("src/harness/coverage.rs", "property: crate::domain::block_sync::RESERVED_BYTE_BOUND,"),
            ("src/harness/coverage.rs", "property: crate::domain::block_sync::RETAINED_BYTE_BOUND,"),
            ("src/harness/coverage.rs", "property: crate::domain::block_sync::TEARDOWN_RETURNS_BASELINE,"),
            ("src/harness/coverage.rs", "property: super::block_sync::OPERATION_IDS,"),
            ("src/harness/coverage.rs", "property: super::super::domain::block_sync::PAYLOAD_INVENTORY_AGREES,"),
            ("src/harness/mod.rs", "entry::<block_sync::Storage>(),"),
        ]),
    },
    NotHeader {
        file: "src/domain/block_sync_semantic.rs",
        module: "block_sync_semantic",
        why: "block sync's semantic checks",
        basis: Basis::NamedOnlyBy(&[
            ("src/domain/block_sync_semantic/reactor_queries.rs", "domain::block_sync_semantic::SemanticModel,"),
            ("src/harness/block_node/view.rs", "use crate::domain::block_sync_semantic::SemanticModel;"),
            ("src/harness/block_sync.rs", "block_sync_semantic::{semantic_properties, SemanticModel, SemanticView},"),
            ("src/harness/coverage.rs", "property: crate::domain::block_sync_semantic::CAPACITY_RELEASE_WAKES,"),
            ("src/harness/coverage.rs", "property: crate::domain::block_sync_semantic::DETACHED_SUBMISSION_BOUNDED,"),
            ("src/harness/coverage.rs", "property: crate::domain::block_sync_semantic::INPUT_DECODED_BOUND,"),
            ("src/harness/coverage.rs", "property: crate::domain::block_sync_semantic::REACTOR_QUERY_LIFECYCLE,"),
            ("src/harness/coverage.rs", "property: crate::domain::block_sync_semantic::STALE_COMPLETION_INERT,"),
            ("src/harness/coverage.rs", "property: crate::domain::block_sync_semantic::SUBMIT_ONLY_REQUESTED,"),
        ]),
    },
    NotHeader {
        file: "src/domain/shared/bounds.rs",
        module: "bounds",
        why: "the shared bound check, which only block sync's bounds use",
        basis: Basis::NamedOnlyBy(&[
            ("src/domain/block_sync.rs", "use super::shared::bounds::{bound, Bound, Measured, Reading};"),
        ]),
    },
    NotHeader {
        file: "src/domain/discovery.rs",
        module: "discovery",
        why: "discovery's checks",
        basis: Basis::NamedOnlyBy(&[
            ("src/harness/coverage.rs", "property: crate::domain::discovery::VERIFICATION_CAPACITY,"),
            ("src/harness/coverage.rs", "property: crate::domain::discovery::WAIT_COMPLETE_WAKES,"),
            ("src/harness/discovery.rs", "domain::discovery::{verification_capacity, wait_complete_wakes, VerificationView},"),
            ("src/harness/discovery.rs", "peer::discovery::{self as fixture, record, unix_now, FIXTURE_ADDR},"),
            ("src/harness/discovery_service.rs", "discovery::{discovery_stream, Answer, DiscoveryAct, DiscoveryRow, DiscoveryScript, Hold},"),
            ("src/harness/discovery_service.rs", "testkit::discovery::{"),
            ("src/harness/mod.rs", "entry::<discovery::VerificationCapacity>(),"),
            ("src/harness/mod.rs", "entry::<discovery::WaitComplete>(),"),
            ("src/peer/discovery/content.rs", "use crate::peer::discovery::{record, services, unix_now, FIXTURE_ADDR};"),
            ("src/peer/discovery/sender.rs", "use crate::peer::discovery::{record, unix_now, Request, FIXTURE_ADDR};"),
            ("src/peer/evidence.rs", "discovery::DiscoverySender,"),
            ("src/peer/mod.rs", "discovery::{DiscoveryAct, DiscoveryPeer, DiscoveryScript, Request},"),
        ]),
    },
    NotHeader {
        file: "src/harness/fixtures.rs",
        module: "fixtures",
        why: "the race fixtures' checks",
        basis: Basis::NamedOnlyBy(&[
            ("src/harness/mod.rs", "entry::<fixtures::All>(),"),
            ("src/harness/mod.rs", "entry::<fixtures::Fixed>(),"),
            ("src/harness/mod.rs", "entry::<fixtures::Race<fixtures::AbBa>>(),"),
            ("src/harness/mod.rs", "entry::<fixtures::Race<fixtures::AbBa>>(),"),
            ("src/harness/mod.rs", "entry::<fixtures::Race<fixtures::AbortWithoutJoin>>(),"),
            ("src/harness/mod.rs", "entry::<fixtures::Race<fixtures::AbortWithoutJoin>>(),"),
            ("src/harness/mod.rs", "entry::<fixtures::Race<fixtures::LostUpdate>>(),"),
            ("src/harness/mod.rs", "entry::<fixtures::Race<fixtures::LostUpdate>>(),"),
            ("src/harness/mod.rs", "entry::<fixtures::Race<fixtures::MissedNotify>>(),"),
            ("src/harness/mod.rs", "entry::<fixtures::Race<fixtures::MissedNotify>>(),"),
            ("src/harness/mod.rs", "entry::<fixtures::Race<fixtures::SnapshotReserve>>(),"),
            ("src/harness/mod.rs", "entry::<fixtures::Race<fixtures::SnapshotReserve>>(),"),
            ("src/harness/mod.rs", "entry::<fixtures::Race<fixtures::TimerCancel>>(),"),
            ("src/harness/mod.rs", "entry::<fixtures::Race<fixtures::TimerCancel>>(),"),
            ("src/suite.rs", "use crate::harness::{entry, fixtures::All};"),
        ]),
    },
    NotHeader {
        file: "src/harness/header_acceptance/evaluate.rs",
        module: "evaluate",
        why: "the acceptance evaluator's own report type, not a check",
        basis: Basis::NamesNoVerdict,
    },
    NotHeader {
        file: "src/harness/header_acceptance/tool.rs",
        module: "tool",
        why: "the acceptance tool reading a trace's report into the evaluator's type, not a check",
        basis: Basis::NamesNoVerdict,
    },
];

/// A [`NaKind::Step`] construction on `line`, meaning `meaning`.
const fn step(meaning: &'static str, line: &'static str) -> NaConstruction {
    NaConstruction {
        kind: NaKind::Step,
        meaning,
        line,
    }
}

/// A [`NaKind::MissingEvidence`] construction on `line`.
const fn missing(line: &'static str) -> NaConstruction {
    NaConstruction {
        kind: NaKind::MissingEvidence,
        meaning: "returned after missing_evidence, which fails the run",
        line,
    }
}

/// `return Verdict::NotApplicable;`.
const RETURN: &str = "return Verdict::NotApplicable;";
/// A tail `Verdict::NotApplicable`.
const TAIL: &str = "Verdict::NotApplicable";
/// A verdict that starts `NotApplicable` until something applies.
const START: &str = "let mut verdict = Verdict::NotApplicable;";

/// Every header-reachable `NotApplicable` producer, by file and function.
pub static NA_REASONS: &[NaProducer] = &[
    NaProducer {
        file: "src/harness/header_sync.rs",
        function: "check_service_operation_ids",
        fingerprint: "cfa6092c996c0c66",
        constructions: &[step(
            "no storage submission or completion of the service at this step",
            RETURN,
        )],
    },
    NaProducer {
        file: "src/harness/header_sync.rs",
        function: "check_header_fault_penalized",
        fingerprint: "6780360ca559f5cc",
        constructions: &[step(
            "the fault evidence holds no header fault",
            "FaultCheck::Nothing => Verdict::NotApplicable,",
        )],
    },
    NaProducer {
        file: "src/domain/header_sync.rs",
        function: "check_chunk_budget_bound",
        fingerprint: "1f73830ca98b2e78",
        constructions: &[step(
            "no capacity boundary was read at this step",
            "_ => Verdict::NotApplicable,",
        )],
    },
    NaProducer {
        file: "src/domain/header_sync.rs",
        function: "check_watch_read",
        fingerprint: "509343a9decd20fe",
        constructions: &[step(
            "nothing was published for the flooded queue's watch rule yet",
            "WatchRead::Nothing => Verdict::NotApplicable,",
        )],
    },
    NaProducer {
        file: "src/domain/header_sync.rs",
        function: "check_teardown",
        fingerprint: "8f59ba6f6c2c8413",
        constructions: &[step(
            "no round was released or settled, and the node isn't settled",
            TAIL,
        )],
    },
    NaProducer {
        file: "src/domain/header_sync.rs",
        function: "check_stale_completion",
        fingerprint: "b92fb8d4fcb31b7a",
        constructions: &[step("no stale call has been found by this step", TAIL)],
    },
    NaProducer {
        file: "src/domain/header_sync.rs",
        function: "check_release_wake",
        fingerprint: "58b16f4764d21e63",
        constructions: &[step("no release wake has opened", TAIL)],
    },
    NaProducer {
        file: "src/domain/header_sync_progress.rs",
        function: "check_honest_progress",
        fingerprint: "55a7629dc8fe3ca6",
        constructions: &[
            step("no frame was read at this step", RETURN),
            step(
                "this step's read isn't the first honest Status read",
                RETURN,
            ),
            missing(RETURN),
        ],
    },
    NaProducer {
        file: "src/domain/session.rs",
        function: "check_honest_never_punished",
        fingerprint: "e7fc75e475d7c45b",
        constructions: &[
            step("no honest peer's transport fact at this step", START),
            missing(RETURN),
        ],
    },
    NaProducer {
        file: "src/table.rs",
        function: "check_generated",
        fingerprint: "08aba84074181da9",
        constructions: &[missing(RETURN)],
    },
    NaProducer {
        file: "src/table.rs",
        function: "check_closes",
        fingerprint: "4ca175cbfcd9df18",
        constructions: &[
            missing(RETURN),
            step(
                "no frame with the ID's label, stream and row (any row for UndeclaredType) was \
                 judged at this step on a connection still open",
                RETURN,
            ),
        ],
    },
    NaProducer {
        file: "src/table.rs",
        function: "check_never_closes",
        fingerprint: "41f4c3cfb98aafa4",
        constructions: &[
            step(
                "no frame of the ID's stream that it applies to was judged at this step",
                START,
            ),
            missing(RETURN),
            missing(RETURN),
        ],
    },
    NaProducer {
        file: "src/downstream.rs",
        function: "check_with",
        fingerprint: "6e56136a16bc992f",
        constructions: &[step(
            "no frame at this step carries the effect a sender rule judges",
            START,
        )],
    },
    NaProducer {
        file: "src/harness/machine.rs",
        function: "check",
        fingerprint: "31897515d1375ad1",
        constructions: &[
            missing(RETURN),
            step(
                "the run hasn't reached the machine's resource-bearing transition",
                TAIL,
            ),
            missing(RETURN),
        ],
    },
    NaProducer {
        file: "src/harness/mod.rs",
        function: "live_step",
        fingerprint: "706bf84041ce9a35",
        constructions: &[step(
            "the drain after the budget, where livelock isn't judged",
            "Phase::Drain => Verdict::NotApplicable,",
        )],
    },
    NaProducer {
        file: "src/property/registry.rs",
        function: "outcome",
        fingerprint: "526e30fa0647023a",
        constructions: &[
            NaConstruction {
                kind: NaKind::Run,
                meaning: "an assumption the property needs was never declared",
                line: "return PropertyOutcome::NotApplicable;",
            },
            NaConstruction {
                kind: NaKind::Run,
                meaning: "its tier excludes a source it needs",
                line: "return PropertyOutcome::NotApplicable;",
            },
            NaConstruction {
                kind: NaKind::Run,
                meaning: "the check never applied at any step",
                line: "None => PropertyOutcome::NotApplicable,",
            },
        ],
    },
];
