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
//! A header check reaches an excluded file only by naming its module, which
//! [`Basis::NotNamedByHeader`] refuses in header-side text
//! ([`HEADER_SIDE`]), so other teams' references to their own modules don't
//! touch the scan.
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
    /// No header-side text names its module's identifier, in a path or a
    /// `use` item. Header-side text is every file [`HEADER_SIDE`] covers,
    /// whole, and, for each [`NA_REASONS`] producer in a shared file, its
    /// own text (what its fingerprint covers) and the `use` items of the
    /// module scope it sits in. Any identifier of that name counts, so an
    /// alias, a grouped import or an import of its parent stays refused,
    /// and so does an unrelated module of the same name, which only header
    /// code can write.
    ///
    /// It doesn't follow calls. A helper in a shared file that a producer
    /// already calls, edited later to reach an excluded module, or a
    /// re-export chain in another file, isn't seen. A callee is its owner's
    /// contract (Rule 23), as for the fingerprint.
    NotNamedByHeader,
    /// It names neither `Verdict` nor `PropertyOutcome`, so its
    /// `NotApplicable` is another type's.
    NamesNoVerdict,
}

/// The path prefixes, relative to the crate, of the files that are header
/// sync's own: every file under them is header-side text whole
/// ([`Basis::NotNamedByHeader`]).
pub const HEADER_SIDE: &[&str] = &["src/harness/header_", "src/domain/header_sync"];

/// The files whose `NotApplicable` constructions no header ID's check
/// reaches. Every other file's constructions must be in [`NA_REASONS`].
/// The scan checks each one's [`Basis`], so header-side text that starts
/// naming one of these needs it reclassified.
pub static NOT_HEADER: &[NotHeader] = &[
    NotHeader {
        file: "src/domain/block_sync.rs",
        module: "block_sync",
        why: "block sync's checks",
        basis: Basis::NotNamedByHeader,
    },
    NotHeader {
        file: "src/domain/block_sync_semantic.rs",
        module: "block_sync_semantic",
        why: "block sync's semantic checks",
        basis: Basis::NotNamedByHeader,
    },
    NotHeader {
        file: "src/domain/shared/bounds.rs",
        module: "bounds",
        why: "the shared bound check, which only block sync's bounds use",
        basis: Basis::NotNamedByHeader,
    },
    NotHeader {
        file: "src/domain/cross.rs",
        module: "cross",
        why: "the sync/* checks across header sync and block sync, which call header sync's \
              checks but which no header check calls",
        basis: Basis::NotNamedByHeader,
    },
    NotHeader {
        file: "src/domain/discovery.rs",
        module: "discovery",
        why: "discovery's checks",
        basis: Basis::NotNamedByHeader,
    },
    NotHeader {
        file: "src/harness/fixtures.rs",
        module: "fixtures",
        why: "the race fixtures' checks",
        basis: Basis::NotNamedByHeader,
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
        fingerprint: "ea28e1a97e82f73d",
        constructions: &[step(
            "no round a claim left untainted was released or settled, and the node isn't settled",
            TAIL,
        )],
    },
    NaProducer {
        file: "src/domain/header_sync.rs",
        function: "check_reset_invalidates_work",
        fingerprint: "0e882cb7004ed34b",
        constructions: &[step(
            "the reactor hasn't adopted a reset or started a round under it by this step",
            TAIL,
        )],
    },
    NaProducer {
        file: "src/domain/header_sync.rs",
        function: "check_stale_chain_effect",
        fingerprint: "17d096f30924cecf",
        constructions: &[step(
            "no chain effect has been judged against a reset by this step",
            TAIL,
        )],
    },
    NaProducer {
        file: "src/domain/header_sync.rs",
        function: "check_hold_release_wake",
        fingerprint: "385d74bc1223da8e",
        constructions: &[step(
            "no held reply reached a reactor call, and no held serving read served its page, before the harness's shutdown by this step",
            TAIL,
        )],
    },
    NaProducer {
        file: "src/domain/header_sync.rs",
        function: "check_peer_retirement",
        fingerprint: "50c476d2ab2cdeec",
        constructions: &[step(
            "no drain has judged a departed session's work by this step, and no hand-off or obliging deadline arm opened before the harness's shutdown",
            TAIL,
        )],
    },
    NaProducer {
        file: "src/domain/header_sync.rs",
        function: "check_expected_chain_selected",
        fingerprint: "f73227e6ef4f55d5",
        constructions: &[
            step(
                "a corrupt engine state, which commit-only-authorized owns, tainted the fork-choice evidence by this step",
                RETURN,
            ),
            step(
                "no fork-choice step had applied by this step and the node hadn't settled",
                TAIL,
            ),
        ],
    },
    NaProducer {
        file: "src/domain/header_sync.rs",
        function: "check_state_publication",
        fingerprint: "f336b39a795850ad",
        constructions: &[
            step(
                "no commit, adoption, status debt or served page had applied by this step, or a corrupt engine state tainted the publication evidence",
                RETURN,
            ),
        ],
    },
    NaProducer {
        file: "src/domain/header_sync.rs",
        function: "check_outstanding_work_bounded",
        fingerprint: "4722d816dec8d8c8",
        constructions: &[step(
            "no drain has judged the reactor's container sizes by this step",
            TAIL,
        )],
    },
    NaProducer {
        file: "src/domain/header_sync.rs",
        function: "check_commit_only_authorized",
        fingerprint: "e869cc220b2cc8ff",
        constructions: &[step(
            "the authority check hasn't judged a round's work by this step",
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
        function: "check_bounded_adversary_progress",
        fingerprint: "6d9098d79cff89e0",
        constructions: &[
            step("no recovery has recorded a `recovered` pick that owes a successor", RETURN),
            step("this step isn't the `recovered` pick's", RETURN),
        ],
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
        fingerprint: "26f09b8f3475e4e3",
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
        fingerprint: "595a51c68b4af91b",
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
    NaProducer {
        file: "src/domain/connection.rs",
        function: "verdict",
        fingerprint: "88499ebb3405adb6",
        constructions: &[
            missing(RETURN),
            step(
                "the wire tap judged nothing the property covers at this step",
                "None => Verdict::NotApplicable,",
            ),
        ],
    },
    NaProducer {
        file: "src/domain/connection.rs",
        function: "check_credit_isolated",
        fingerprint: "f8b83e34be11d3f0",
        constructions: &[step(
            "the run samples no connection loads",
            "None => Verdict::NotApplicable,",
        )],
    },
    NaProducer {
        file: "src/domain/connection.rs",
        function: "check_send_isolated",
        fingerprint: "98ad14c1908c58a9",
        constructions: &[step(
            "the run samples no connection loads",
            "None => Verdict::NotApplicable,",
        )],
    },
    NaProducer {
        file: "src/domain/connection.rs",
        function: "check_connection_shares_bound",
        fingerprint: "e1d5acaf3ead2d01",
        constructions: &[step(
            "the run samples no connection loads",
            "None => Verdict::NotApplicable,",
        )],
    },
    NaProducer {
        file: "src/domain/session.rs",
        function: "check_replacement_preserves_authorization",
        fingerprint: "77b0406fa875f002",
        constructions: &[
            step(
                "no session row or stale registry write at this step",
                RETURN,
            ),
            step(
                "no session at this step replaced an earlier connection, and no stale registry \
                 write was attempted",
                "None => Verdict::NotApplicable,",
            ),
        ],
    },
    NaProducer {
        file: "src/domain/session.rs",
        function: "check_stalled_service_isolated",
        fingerprint: "c27e64b13b2136ff",
        constructions: &[
            step(
                "no block-sync request or handoff, close with a cause, or session exit at this \
                 step",
                RETURN,
            ),
            step(
                "no wait ended, close was decided, or session exited at this step",
                START,
            ),
            missing(RETURN),
        ],
    },
];
