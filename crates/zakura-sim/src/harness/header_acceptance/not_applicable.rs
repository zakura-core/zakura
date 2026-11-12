//! The `NotApplicable` reports each node planted case expects on its
//! counted runs, beside its target.
//!
//! The evaluator holds every property of a counted run outside its target
//! to `Held`, or to the `NotApplicable` reason its row lists for it
//! ([`super::evaluate`]). Each case runs a focused input that applies only
//! some of its harness's IDs, as its recorded baseline reports them. For a
//! generated ID's case, and the wire harness's hand-written cases,
//! [`CASES`] names the IDs its run applies and the IDs whose assumptions
//! its scenario doesn't declare, and every other ID [`harness_matrix`]
//! registers on its harness never applied there. The other harnesses'
//! cases apply nearly every ID, so [`SHARED`] names, per group of cases,
//! the IDs they never apply and those they don't declare. [`expected`]
//! spells either out as the row's list.
//!
//! The patched build's completed runs, its `Draft` run and a patched run
//! found at the end, go on past the break, so they can apply other IDs, or
//! never reach some the baseline applies. Where a case's recorded runs
//! there do, [`PATCHED`] names how, and [`patched_expected`] spells out
//! their own list. Three cases break a broader ID on their `Draft` runs
//! after their own break: on `outcome-window-unchecked`'s,
//! `headers-outcome/unsolicited-closes`; on `page-past-credit`'s,
//! `sim/no-livelock`; and on `open-repeated`'s, `conformant-never-closes`
//! and `session/honest-never-punished`. Each names those as its set, judged
//! on that `Draft` run ([`super::Found::AtStepSetOnDraft`]), and the
//! last two of those `Draft` runs report otherwise than their baselines,
//! as [`PATCHED`] says.

use std::{collections::BTreeMap, sync::OnceLock};

use super::{rows::harness_matrix, CaseRef, ExpectedNa};
use crate::NaReason;

/// `header-sync/wire`.
const WIRE: &str = "header-sync/wire";
/// `header-sync/storage`.
const STORAGE: &str = "header-sync/storage";
/// `header-sync/liveness`.
const LIVENESS: &str = "header-sync/liveness";
/// `header-sync/flood`.
const FLOOD: &str = "header-sync/flood";
/// `header-sync/release-wake`.
const RELEASE_WAKE: &str = "header-sync/release-wake";
/// `header-sync/repair`.
const REPAIR: &str = "header-sync/repair";

/// The IDs a run of one frame from a peer the node doesn't subscribe to
/// applies, beside its target.
const FRAME: &[&str] = &[
    "sim/no-task-panic",
    "sim/no-livelock",
    "harness/direct-dispatch",
    "header-sync/chunk-budget-bound",
    "header-sync/commit-only-authorized",
    "header-sync/s5v9/conformant-never-closes",
    "header-sync/s5v9/status/sender-cadence",
];

/// The IDs a run in which the node subscribes to the peer applies, beside
/// its target: [`FRAME`]'s, and the subscription's work and the node's
/// updates.
const SUBSCRIBED: &[&str] = &[
    "sim/no-task-panic",
    "sim/no-livelock",
    "harness/direct-dispatch",
    "header-sync/chunk-budget-bound",
    "header-sync/commit-only-authorized",
    "header-sync/s5v9/conformant-never-closes",
    "header-sync/s5v9/status/sender-cadence",
    "header-sync/storage-operation-ids",
    "header-sync/teardown-returns-baseline",
    "header-sync/reset-invalidates-work",
    "header-sync/peer-retirement-progress",
    "header-sync/s5v9/subscribe-headers/sender-within-live",
    "header-sync/s5v9/subscribe-headers/sender-within-credit",
];

/// [`SUBSCRIBED`], and the node's page answering the peer's `Open`.
const ANSWERED: &[&str] = &[
    "sim/no-task-panic",
    "sim/no-livelock",
    "harness/direct-dispatch",
    "header-sync/chunk-budget-bound",
    "header-sync/commit-only-authorized",
    "header-sync/s5v9/conformant-never-closes",
    "header-sync/s5v9/status/sender-cadence",
    "header-sync/storage-operation-ids",
    "header-sync/teardown-returns-baseline",
    "header-sync/reset-invalidates-work",
    "header-sync/peer-retirement-progress",
    "header-sync/s5v9/subscribe-headers/sender-within-live",
    "header-sync/s5v9/subscribe-headers/sender-within-credit",
    "header-sync/s5v9/headers/sender-answers-open",
];

/// The IDs a run in which the node judges a subscribed adversary's page
/// applies, beside its target.
const PAGE_JUDGED: &[&str] = &[
    "sim/no-task-panic",
    "sim/no-livelock",
    "harness/direct-dispatch",
    "header-sync/chunk-budget-bound",
    "header-sync/s5v9/conformant-never-closes",
    "header-sync/s5v9/status/sender-cadence",
    "header-sync/storage-operation-ids",
    "header-sync/teardown-returns-baseline",
    "header-sync/reset-invalidates-work",
    "header-sync/peer-retirement-progress",
    "header-sync/s5v9/subscribe-headers/sender-within-live",
    "header-sync/s5v9/subscribe-headers/sender-within-credit",
    "header-sync/s5v9/headers/bad-content-closes",
];

/// The IDs a run in which the node penalizes a peer's faulted header
/// applies, beside its target.
const PENALTY: &[&str] = &[
    "sim/no-task-panic",
    "harness/direct-dispatch",
    "header-sync/chunk-budget-bound",
    "header-sync/commit-only-authorized",
    "header-sync/s5v9/conformant-never-closes",
    "header-sync/s5v9/status/sender-cadence",
    "header-sync/storage-operation-ids",
    "header-sync/teardown-returns-baseline",
    "header-sync/reset-invalidates-work",
    "header-sync/peer-retirement-progress",
    "header-sync/stale-completion-inert",
    "header-sync/s5v9/subscribe-headers/sender-within-live",
    "header-sync/s5v9/subscribe-headers/sender-within-credit",
];

/// The IDs a run in which the node syncs from an honest peer applies,
/// beside its target and `session/honest-never-punished`.
const HONEST_SYNC: &[&str] = &[
    "sim/no-task-panic",
    "harness/direct-dispatch",
    "header-sync/chunk-budget-bound",
    "header-sync/commit-only-authorized",
    "header-sync/s5v9/conformant-never-closes",
    "header-sync/s5v9/status/sender-cadence",
    "header-sync/storage-operation-ids",
    "header-sync/teardown-returns-baseline",
    "header-sync/reset-invalidates-work",
    "header-sync/stale-completion-inert",
    "header-sync/stale-chain-effect-inert",
    "header-sync/s5v9/subscribe-headers/sender-within-live",
    "header-sync/s5v9/subscribe-headers/sender-within-credit",
];

/// What an adversary's scenario doesn't declare: no peer is honest, and
/// the node isn't expected to progress.
const ADVERSARY: &[&str] = &[
    "session/honest-never-punished",
    "header-sync/honest-progress",
];

/// What the scenario of the node serving an honest subscriber, or syncing
/// from honest peers, doesn't declare: no livelock bound, and the node
/// isn't expected to progress.
const SERVING: &[&str] = &["sim/no-livelock", "header-sync/honest-progress"];

/// What a penalty scenario doesn't declare: [`ADVERSARY`]'s, and no
/// livelock bound.
const PENALTY_UNDECLARED: &[&str] = &[
    "session/honest-never-punished",
    "header-sync/honest-progress",
    "sim/no-livelock",
];

/// What the storage harness's scenarios don't declare: no livelock bound,
/// and no adversary whose progress is bounded.
const STORAGE_UNDECLARED: &[&str] = &["sim/no-livelock", "header-sync/bounded-adversary-progress"];

/// What a scenario with no livelock bound doesn't declare.
const NO_LIVELOCK: &[&str] = &["sim/no-livelock"];

/// What a liveness scenario with no bounded adversary doesn't declare.
const BOUNDED: &[&str] = &["header-sync/bounded-adversary-progress"];

/// A case's target.
enum Target {
    /// `header-sync/s5v9/<suffix>`, one of header sync's generated IDs, on
    /// the wire harness.
    Generated(&'static str),
    /// A hand-written ID, and the harness the case runs on.
    Id(&'static str, &'static str),
}

/// One case's runs: its target and name, the IDs beside the target that
/// apply, and those whose assumptions its scenario doesn't declare.
struct CaseNa {
    /// The target.
    property: Target,
    /// The case.
    name: &'static str,
    /// The IDs beside the target that apply and hold.
    applies: &'static [&'static str],
    /// The IDs that are `NotApplicable` as `undeclared`.
    undeclared: &'static [&'static str],
}

/// A [`CaseNa`] of `header-sync/s5v9/<suffix>`'s case `name`.
const fn generated(
    suffix: &'static str,
    name: &'static str,
    applies: &'static [&'static str],
    undeclared: &'static [&'static str],
) -> CaseNa {
    CaseNa {
        property: Target::Generated(suffix),
        name,
        applies,
        undeclared,
    }
}

/// A [`CaseNa`] of hand-written ID `property`'s case `name` on the wire
/// harness.
const fn wire(
    property: &'static str,
    name: &'static str,
    applies: &'static [&'static str],
    undeclared: &'static [&'static str],
) -> CaseNa {
    CaseNa {
        property: Target::Id(property, WIRE),
        name,
        applies,
        undeclared,
    }
}

/// Each case's runs, as its recorded baseline reports them.
const CASES: &[CaseNa] = &[
    CaseNa {
        property: Target::Id("header-sync/commit-only-authorized", WIRE),
        name: "staged-despite-close",
        applies: &[
            "sim/no-task-panic",
            "sim/no-livelock",
            "harness/direct-dispatch",
            "header-sync/chunk-budget-bound",
            "header-sync/s5v9/conformant-never-closes",
            "header-sync/s5v9/status/sender-cadence",
            "header-sync/storage-operation-ids",
            "header-sync/teardown-returns-baseline",
            "header-sync/reset-invalidates-work",
            "header-sync/peer-retirement-progress",
            "header-sync/s5v9/subscribe-headers/sender-within-live",
            "header-sync/s5v9/subscribe-headers/sender-within-credit",
            "header-sync/s5v9/headers/bad-content-closes",
        ],
        undeclared: ADVERSARY,
    },
    CaseNa {
        property: Target::Id("header-sync/reset-invalidates-work", STORAGE),
        name: "stale-locator-round",
        applies: &[
            "sim/no-task-panic",
            "harness/direct-dispatch",
            "header-sync/storage-operation-ids",
            "header-sync/chunk-budget-bound",
            "header-sync/teardown-returns-baseline",
            "header-sync/stale-completion-inert",
            "header-sync/stale-chain-effect-inert",
            "header-sync/commit-only-authorized",
            "header-sync/outstanding-work-bounded",
            "header-sync/expected-chain-selected",
            "header-sync/state-publication-consistent",
        ],
        undeclared: STORAGE_UNDECLARED,
    },
    wire(
        "header-sync/commit-only-authorized",
        "lower-locator-allowed",
        PAGE_JUDGED,
        ADVERSARY,
    ),
    wire(
        "header-sync/commit-only-authorized",
        "off-path-page",
        PAGE_JUDGED,
        ADVERSARY,
    ),
    wire(
        "header-sync/commit-only-authorized",
        "overcredit-guard",
        &[
            "sim/no-task-panic",
            "sim/no-livelock",
            "harness/direct-dispatch",
            "header-sync/chunk-budget-bound",
            "header-sync/s5v9/conformant-never-closes",
            "header-sync/s5v9/status/sender-cadence",
            "header-sync/storage-operation-ids",
            "header-sync/teardown-returns-baseline",
            "header-sync/reset-invalidates-work",
            "header-sync/peer-retirement-progress",
            "header-sync/s5v9/subscribe-headers/sender-within-live",
            "header-sync/s5v9/subscribe-headers/sender-within-credit",
            "header-sync/s5v9/subscribe-headers/over-credit-closes",
        ],
        ADVERSARY,
    ),
    wire(
        "header-sync/header-fault-penalized",
        "cooldown-session-gate",
        PENALTY,
        PENALTY_UNDECLARED,
    ),
    wire(
        "header-sync/header-fault-penalized",
        "idle-exit-drops-fault",
        PENALTY,
        PENALTY_UNDECLARED,
    ),
    wire(
        "header-sync/header-fault-penalized",
        "late-fault-unpenalized",
        PENALTY,
        PENALTY_UNDECLARED,
    ),
    wire(
        "header-sync/header-fault-penalized",
        "short-cooldown",
        PENALTY,
        PENALTY_UNDECLARED,
    ),
    wire(
        "header-sync/honest-progress",
        "dropped-honest-peer",
        &[
            "sim/no-task-panic",
            "harness/direct-dispatch",
            "header-sync/chunk-budget-bound",
            "header-sync/commit-only-authorized",
            "header-sync/s5v9/conformant-never-closes",
            "header-sync/s5v9/status/sender-cadence",
            "header-sync/storage-operation-ids",
            "header-sync/teardown-returns-baseline",
            "header-sync/reset-invalidates-work",
            "header-sync/stale-completion-inert",
            "header-sync/stale-chain-effect-inert",
            "header-sync/s5v9/subscribe-headers/sender-within-live",
            "header-sync/s5v9/subscribe-headers/sender-within-credit",
            "session/honest-never-punished",
        ],
        NO_LIVELOCK,
    ),
    wire(
        "header-sync/honest-progress",
        "skipped-apply",
        &[
            "sim/no-task-panic",
            "harness/direct-dispatch",
            "header-sync/chunk-budget-bound",
            "header-sync/commit-only-authorized",
            "header-sync/s5v9/conformant-never-closes",
            "header-sync/s5v9/status/sender-cadence",
            "header-sync/storage-operation-ids",
            "header-sync/teardown-returns-baseline",
            "header-sync/reset-invalidates-work",
            "header-sync/stale-completion-inert",
            "header-sync/stale-chain-effect-inert",
            "header-sync/s5v9/subscribe-headers/sender-within-live",
            "header-sync/s5v9/subscribe-headers/sender-within-credit",
            "session/honest-never-punished",
        ],
        NO_LIVELOCK,
    ),
    wire(
        "session/honest-never-punished",
        "target-page-refused",
        HONEST_SYNC,
        SERVING,
    ),
    generated(
        "headers/bad-content-closes",
        "unlinked-page-dropped",
        SUBSCRIBED,
        ADVERSARY,
    ),
    generated(
        "status/sender-cadence",
        "pacing-floors-cut",
        &[
            "sim/no-task-panic",
            "harness/direct-dispatch",
            "header-sync/chunk-budget-bound",
            "header-sync/commit-only-authorized",
            "header-sync/s5v9/conformant-never-closes",
            "session/honest-never-punished",
        ],
        SERVING,
    ),
    generated(
        "subscribe-headers/sender-within-live",
        "open-repeated",
        &[
            "sim/no-task-panic",
            "harness/direct-dispatch",
            "header-sync/chunk-budget-bound",
            "header-sync/commit-only-authorized",
            "header-sync/s5v9/conformant-never-closes",
            "header-sync/s5v9/status/sender-cadence",
            "header-sync/storage-operation-ids",
            "header-sync/teardown-returns-baseline",
            "header-sync/reset-invalidates-work",
            "header-sync/stale-completion-inert",
            "header-sync/stale-chain-effect-inert",
            "header-sync/s5v9/subscribe-headers/sender-within-credit",
            "session/honest-never-punished",
        ],
        SERVING,
    ),
    generated(
        "conformant-never-closes",
        "target-page-refused",
        &[
            "sim/no-task-panic",
            "sim/no-livelock",
            "harness/direct-dispatch",
            "header-sync/chunk-budget-bound",
            "header-sync/commit-only-authorized",
            "header-sync/s5v9/status/sender-cadence",
            "header-sync/storage-operation-ids",
            "header-sync/teardown-returns-baseline",
            "header-sync/reset-invalidates-work",
            "header-sync/stale-completion-inert",
            "header-sync/stale-chain-effect-inert",
            "header-sync/s5v9/subscribe-headers/sender-within-live",
            "header-sync/s5v9/subscribe-headers/sender-within-credit",
        ],
        ADVERSARY,
    ),
    generated(
        "headers-outcome/bad-content-closes",
        "outcome-window-unchecked",
        SUBSCRIBED,
        ADVERSARY,
    ),
    generated(
        "headers-outcome/long-payload-closes",
        "oversize-skipped",
        SUBSCRIBED,
        ADVERSARY,
    ),
    generated(
        "headers-outcome/reserved-flags-closes",
        "reserved-flags-skipped",
        SUBSCRIBED,
        ADVERSARY,
    ),
    generated(
        "headers-outcome/sender-ends-once",
        "outcome-misnamed",
        &[
            "sim/no-task-panic",
            "harness/direct-dispatch",
            "header-sync/chunk-budget-bound",
            "header-sync/commit-only-authorized",
            "header-sync/s5v9/conformant-never-closes",
            "header-sync/s5v9/status/sender-cadence",
            "header-sync/storage-operation-ids",
            "header-sync/teardown-returns-baseline",
            "header-sync/reset-invalidates-work",
            "header-sync/s5v9/subscribe-headers/sender-within-live",
            "header-sync/s5v9/subscribe-headers/sender-within-credit",
            "header-sync/s5v9/headers/sender-answers-open",
            "session/honest-never-punished",
        ],
        SERVING,
    ),
    generated(
        "headers-outcome/short-payload-closes",
        "short-payload-skipped",
        SUBSCRIBED,
        ADVERSARY,
    ),
    generated(
        "headers-outcome/unsolicited-closes",
        "unsolicited-skipped",
        FRAME,
        ADVERSARY,
    ),
    generated(
        "headers/reserved-flags-closes",
        "reserved-flags-skipped",
        SUBSCRIBED,
        ADVERSARY,
    ),
    generated(
        "headers/sender-answers-open",
        "page-misnamed",
        &[
            "sim/no-task-panic",
            "harness/direct-dispatch",
            "header-sync/chunk-budget-bound",
            "header-sync/commit-only-authorized",
            "header-sync/s5v9/conformant-never-closes",
            "header-sync/s5v9/status/sender-cadence",
            "header-sync/storage-operation-ids",
            "header-sync/teardown-returns-baseline",
            "header-sync/reset-invalidates-work",
            "header-sync/s5v9/subscribe-headers/sender-within-live",
            "header-sync/s5v9/subscribe-headers/sender-within-credit",
            "header-sync/s5v9/headers-outcome/sender-ends-once",
            "session/honest-never-punished",
        ],
        SERVING,
    ),
    generated(
        "headers/short-payload-closes",
        "short-payload-skipped",
        SUBSCRIBED,
        ADVERSARY,
    ),
    generated(
        "headers/unsolicited-closes",
        "unsolicited-skipped",
        FRAME,
        ADVERSARY,
    ),
    generated(
        "status/bad-content-closes",
        "work-anchor-unchecked",
        FRAME,
        ADVERSARY,
    ),
    generated(
        "status/cadence-never-closes",
        "exhausted-cadence-throttled",
        FRAME,
        ADVERSARY,
    ),
    generated(
        "status/long-payload-closes",
        "oversize-skipped",
        FRAME,
        ADVERSARY,
    ),
    generated(
        "status/reserved-flags-closes",
        "reserved-flags-skipped",
        FRAME,
        ADVERSARY,
    ),
    generated(
        "status/short-payload-closes",
        "short-payload-skipped",
        FRAME,
        ADVERSARY,
    ),
    generated(
        "subscribe-headers/after-close-closes",
        "after-close-dropped",
        &[
            "sim/no-task-panic",
            "sim/no-livelock",
            "harness/direct-dispatch",
            "header-sync/chunk-budget-bound",
            "header-sync/commit-only-authorized",
            "header-sync/s5v9/conformant-never-closes",
            "header-sync/s5v9/status/sender-cadence",
            "header-sync/storage-operation-ids",
            "header-sync/reset-invalidates-work",
            "header-sync/s5v9/headers/sender-answers-open",
        ],
        ADVERSARY,
    ),
    generated(
        "subscribe-headers/bad-content-closes",
        "repeated-locator-unchecked",
        FRAME,
        ADVERSARY,
    ),
    generated(
        "subscribe-headers/bad-cursor-closes",
        "unknown-acknowledgement-dropped",
        ANSWERED,
        ADVERSARY,
    ),
    generated(
        "subscribe-headers/long-payload-closes",
        "oversize-skipped",
        FRAME,
        ADVERSARY,
    ),
    generated(
        "subscribe-headers/over-credit-closes",
        "above-reservation-skipped",
        SUBSCRIBED,
        ADVERSARY,
    ),
    generated(
        "subscribe-headers/over-live-closes",
        "no-slot-dropped",
        ANSWERED,
        ADVERSARY,
    ),
    generated(
        "subscribe-headers/reserved-flags-closes",
        "reserved-flags-skipped",
        FRAME,
        ADVERSARY,
    ),
    generated(
        "subscribe-headers/sender-within-credit",
        "page-past-credit",
        ANSWERED,
        ADVERSARY,
    ),
    generated(
        "subscribe-headers/short-payload-closes",
        "short-payload-skipped",
        FRAME,
        ADVERSARY,
    ),
    generated(
        "undeclared-type-closes",
        "undeclared-type-skipped",
        FRAME,
        ADVERSARY,
    ),
];

/// Hand-written IDs' cases on one harness other than the wire harness
/// whose baselines leave the same IDs `NotApplicable`.
struct Shared {
    /// The harness.
    harness: &'static str,
    /// The IDs the runs never apply.
    never: &'static [&'static str],
    /// The IDs that are `NotApplicable` as `undeclared`.
    undeclared: &'static [&'static str],
    /// The cases, as their targets and names.
    cases: &'static [(&'static str, &'static str)],
}

/// The groups of cases, as their recorded baselines report them.
const SHARED: &[Shared] = &[
    Shared {
        harness: STORAGE,
        never: &["header-sync/peer-retirement-progress"],
        undeclared: STORAGE_UNDECLARED,
        cases: &[
            ("header-sync/chunk-budget-bound", "release-relocks-budget"),
            (
                "header-sync/commit-only-authorized",
                "accepted-before-apply",
            ),
            ("header-sync/commit-only-authorized", "accepted-not-stored"),
            ("header-sync/commit-only-authorized", "acknowledged-bytes"),
            (
                "header-sync/commit-only-authorized",
                "advanced-verified-best",
            ),
            ("header-sync/commit-only-authorized", "applied-without-tip"),
            ("header-sync/commit-only-authorized", "apply-source-changed"),
            ("header-sync/commit-only-authorized", "byte-overgrant"),
            (
                "header-sync/commit-only-authorized",
                "changed-held-validation",
            ),
            (
                "header-sync/commit-only-authorized",
                "cleared-preferred-tip",
            ),
            ("header-sync/commit-only-authorized", "corrupted-best-score"),
            (
                "header-sync/commit-only-authorized",
                "corrupted-projection-entry",
            ),
            (
                "header-sync/commit-only-authorized",
                "corrupted-side-eligibility",
            ),
            (
                "header-sync/commit-only-authorized",
                "corrupted-side-parent",
            ),
            (
                "header-sync/commit-only-authorized",
                "corrupted-verified-projection",
            ),
            (
                "header-sync/commit-only-authorized",
                "dropped-payload-header",
            ),
            (
                "header-sync/commit-only-authorized",
                "failed-projection-entry",
            ),
            (
                "header-sync/commit-only-authorized",
                "failed-side-eligibility",
            ),
            (
                "header-sync/commit-only-authorized",
                "failure-after-mutation",
            ),
            (
                "header-sync/commit-only-authorized",
                "flattened-side-coordinate",
            ),
            ("header-sync/commit-only-authorized", "foreign-leases"),
            ("header-sync/commit-only-authorized", "forwarded-length"),
            (
                "header-sync/commit-only-authorized",
                "grant-after-cancellation",
            ),
            ("header-sync/commit-only-authorized", "grant-other-target"),
            ("header-sync/commit-only-authorized", "later-ancestor"),
            ("header-sync/commit-only-authorized", "locator-mutates"),
            ("header-sync/commit-only-authorized", "locator-swapped"),
            ("header-sync/commit-only-authorized", "mischarged-page"),
            ("header-sync/commit-only-authorized", "miscounted-cursor"),
            ("header-sync/commit-only-authorized", "miscounted-page"),
            ("header-sync/commit-only-authorized", "misdated-admission"),
            ("header-sync/commit-only-authorized", "new-node-body-state"),
            ("header-sync/commit-only-authorized", "open-drops-locator"),
            ("header-sync/commit-only-authorized", "open-other-target"),
            ("header-sync/commit-only-authorized", "overgrant"),
            ("header-sync/commit-only-authorized", "owner-swap"),
            (
                "header-sync/commit-only-authorized",
                "projection-to-side-tip",
            ),
            ("header-sync/commit-only-authorized", "reordered-payload"),
            (
                "header-sync/commit-only-authorized",
                "sealed-batch-truncated",
            ),
            ("header-sync/commit-only-authorized", "session-id-changed"),
            ("header-sync/commit-only-authorized", "skewed-remaining"),
            ("header-sync/commit-only-authorized", "skipped-sequence"),
            ("header-sync/commit-only-authorized", "skipped-staging"),
            ("header-sync/commit-only-authorized", "stored-excess-credit"),
            ("header-sync/commit-only-authorized", "stored-order"),
            (
                "header-sync/commit-only-authorized",
                "submitted-source-changed",
            ),
            ("header-sync/commit-only-authorized", "swapped-payload"),
            ("header-sync/commit-only-authorized", "swapped-side-header"),
            ("header-sync/commit-only-authorized", "truncated-projection"),
            (
                "header-sync/commit-only-authorized",
                "unrelated-branch-deleted",
            ),
            ("header-sync/commit-only-authorized", "unstored-cursor"),
            (
                "header-sync/commit-only-authorized",
                "wrong-acknowledgement",
            ),
            ("header-sync/commit-only-authorized", "wrong-anchor"),
            ("header-sync/commit-only-authorized", "wrong-close"),
            ("header-sync/commit-only-authorized", "wrong-peer"),
            ("header-sync/commit-only-authorized", "wrong-request"),
            ("header-sync/commit-only-authorized", "wrong-source"),
            (
                "header-sync/expected-chain-selected",
                "every-session-refused",
            ),
            (
                "header-sync/expected-chain-selected",
                "later-sessions-refused",
            ),
            (
                "header-sync/expected-chain-selected",
                "missing-cooldown-refuses",
            ),
            ("header-sync/expected-chain-selected", "offer-by-height"),
            ("header-sync/expected-chain-selected", "score-corrupted"),
            (
                "header-sync/expected-chain-selected",
                "selector-prefers-length",
            ),
            (
                "header-sync/expected-chain-selected",
                "service-rejects-valid-streams",
            ),
            (
                "header-sync/outstanding-work-bounded",
                "accepted-cursors-kept",
            ),
            ("header-sync/outstanding-work-bounded", "call-kept"),
            (
                "header-sync/outstanding-work-bounded",
                "completed-targets-kept",
            ),
            ("header-sync/outstanding-work-bounded", "ended-kept"),
            ("header-sync/outstanding-work-bounded", "entries-cloned"),
            ("header-sync/outstanding-work-bounded", "extra-future"),
            ("header-sync/outstanding-work-bounded", "marked-at-prepare"),
            ("header-sync/outstanding-work-bounded", "outcome-kept"),
            ("header-sync/outstanding-work-bounded", "retention-skipped"),
            ("header-sync/outstanding-work-bounded", "spare-publisher"),
            ("header-sync/outstanding-work-bounded", "staged-key-kept"),
            ("header-sync/outstanding-work-bounded", "unbounded-slots"),
            ("header-sync/outstanding-work-bounded", "unknown-as-known"),
            ("header-sync/reset-invalidates-work", "anchor-only-locator"),
            ("header-sync/reset-invalidates-work", "awaiting-kept"),
            ("header-sync/reset-invalidates-work", "bump-skipped"),
            (
                "header-sync/reset-invalidates-work",
                "cross-generation-locator",
            ),
            ("header-sync/reset-invalidates-work", "entry-kept"),
            (
                "header-sync/reset-invalidates-work",
                "insertion-bump-skipped",
            ),
            (
                "header-sync/reset-invalidates-work",
                "insertion-bump-skipped-held",
            ),
            ("header-sync/reset-invalidates-work", "no-locator"),
            ("header-sync/reset-invalidates-work", "old-hashes"),
            ("header-sync/reset-invalidates-work", "old-locator"),
            ("header-sync/reset-invalidates-work", "owner-refreshed"),
            (
                "header-sync/reset-invalidates-work",
                "skipped-locator-entry",
            ),
            ("header-sync/reset-invalidates-work", "skipped-retirement"),
            ("header-sync/reset-invalidates-work", "stale-read"),
            ("header-sync/reset-invalidates-work", "tip-only-locator"),
            ("header-sync/reset-invalidates-work", "unreleased-obsolete"),
            ("header-sync/reset-invalidates-work", "wrong-locator-height"),
            ("header-sync/stale-chain-effect-inert", "stale-adoption"),
            ("header-sync/stale-chain-effect-inert", "stale-commit"),
            ("header-sync/stale-completion-inert", "stale-acceptance"),
            ("header-sync/state-publication-consistent", "store-dropped"),
            (
                "header-sync/state-publication-consistent",
                "stored-version-kept",
            ),
            ("header-sync/state-publication-consistent", "unsent-tip"),
        ],
    },
    Shared {
        harness: STORAGE,
        never: &[],
        undeclared: STORAGE_UNDECLARED,
        cases: &[
            (
                "header-sync/chunk-budget-bound",
                "uncapped-secondary-reservation",
            ),
            (
                "header-sync/outstanding-work-bounded",
                "cancellation-ignored",
            ),
            (
                "header-sync/outstanding-work-bounded",
                "close-kept-on-replace",
            ),
            ("header-sync/outstanding-work-bounded", "deadline-kept"),
            ("header-sync/outstanding-work-bounded", "locator-entry-kept"),
            ("header-sync/outstanding-work-bounded", "pruning-skipped"),
            (
                "header-sync/outstanding-work-bounded",
                "publisher-token-detached",
            ),
            ("header-sync/outstanding-work-bounded", "reuse-skipped"),
            (
                "header-sync/outstanding-work-bounded",
                "secondary-reservation",
            ),
            ("header-sync/outstanding-work-bounded", "supplier-twice"),
            ("header-sync/stale-completion-inert", "dropped-owner-check"),
            ("header-sync/stale-completion-inert", "stale-charge"),
            ("header-sync/stale-completion-inert", "stale-close"),
            ("header-sync/stale-completion-inert", "stale-credit"),
            ("header-sync/stale-completion-inert", "stale-release"),
            ("header-sync/stale-completion-inert", "stale-retirement"),
            (
                "header-sync/teardown-returns-baseline",
                "forgotten-completion",
            ),
            (
                "header-sync/teardown-returns-baseline",
                "forgotten-retained-headers",
            ),
            ("header-sync/teardown-returns-baseline", "kept-queue-lease"),
            (
                "header-sync/teardown-returns-baseline",
                "leaked-lease-clone",
            ),
            (
                "header-sync/teardown-returns-baseline",
                "leaked-reservation-entry",
            ),
            (
                "header-sync/teardown-returns-baseline",
                "skipped-round-release",
            ),
        ],
    },
    Shared {
        harness: STORAGE,
        never: &[
            "header-sync/peer-retirement-progress",
            "header-sync/stale-chain-effect-inert",
            "header-sync/stale-completion-inert",
        ],
        undeclared: STORAGE_UNDECLARED,
        cases: &[
            (
                "header-sync/outstanding-work-bounded",
                "cancellation-ignored-at-capacity",
            ),
            (
                "header-sync/outstanding-work-bounded",
                "execution-admitted-twice",
            ),
            ("header-sync/state-publication-consistent", "ancestor-field"),
            ("header-sync/state-publication-consistent", "page-order"),
            (
                "header-sync/teardown-returns-baseline",
                "consumed-locator-permit",
            ),
            (
                "header-sync/teardown-returns-baseline",
                "forgotten-locator-permit",
            ),
            (
                "header-sync/teardown-returns-baseline",
                "shutdown-kept-queue",
            ),
        ],
    },
    Shared {
        harness: STORAGE,
        never: &[
            "header-sync/peer-retirement-progress",
            "header-sync/stale-completion-inert",
            "header-sync/storage-operation-ids",
            "header-sync/teardown-returns-baseline",
        ],
        undeclared: STORAGE_UNDECLARED,
        cases: &[
            ("header-sync/commit-only-authorized", "added-reason"),
            ("header-sync/commit-only-authorized", "wrong-inheritance"),
            (
                "header-sync/commit-only-authorized",
                "wrong-invalidation-id",
            ),
            ("header-sync/reset-invalidates-work", "bump-skipped-quiet"),
            ("header-sync/state-publication-consistent", "mixed-adoption"),
            (
                "header-sync/state-publication-consistent",
                "publication-skipped",
            ),
        ],
    },
    Shared {
        harness: STORAGE,
        never: &[
            "header-sync/peer-retirement-progress",
            "header-sync/stale-chain-effect-inert",
            "header-sync/stale-completion-inert",
            "header-sync/teardown-returns-baseline",
        ],
        undeclared: STORAGE_UNDECLARED,
        cases: &[
            (
                "header-sync/state-publication-consistent",
                "first-locator-dropped",
            ),
            (
                "header-sync/state-publication-consistent",
                "first-page-height",
            ),
            (
                "header-sync/state-publication-consistent",
                "first-page-target",
            ),
            (
                "header-sync/state-publication-consistent",
                "target-as-ancestor",
            ),
            (
                "header-sync/state-publication-consistent",
                "wrong-fork-served",
            ),
        ],
    },
    Shared {
        harness: STORAGE,
        never: &[
            "header-sync/peer-retirement-progress",
            "header-sync/stale-completion-inert",
        ],
        undeclared: STORAGE_UNDECLARED,
        cases: &[
            ("harness/direct-dispatch", "locator-to-action-channel"),
            (
                "header-sync/state-publication-consistent",
                "old-fields-after-reset",
            ),
            (
                "header-sync/state-publication-consistent",
                "watch-ignored-in-storage",
            ),
        ],
    },
    Shared {
        harness: STORAGE,
        never: &[
            "header-sync/peer-retirement-progress",
            "header-sync/stale-completion-inert",
            "header-sync/teardown-returns-baseline",
        ],
        undeclared: STORAGE_UNDECLARED,
        cases: &[
            (
                "header-sync/commit-only-authorized",
                "corrupted-best-score-by-commit",
            ),
            (
                "header-sync/expected-chain-selected",
                "score-corrupted-by-commit",
            ),
            (
                "header-sync/reset-invalidates-work",
                "insertion-bump-skipped-by-commit",
            ),
        ],
    },
    Shared {
        harness: STORAGE,
        never: &["header-sync/stale-completion-inert"],
        undeclared: STORAGE_UNDECLARED,
        cases: &[("header-sync/outstanding-work-bounded", "waits-kept")],
    },
    Shared {
        harness: LIVENESS,
        never: &[],
        undeclared: BOUNDED,
        cases: &[
            (
                "header-sync/peer-retirement-progress",
                "kept-repair-widened",
            ),
            ("header-sync/peer-retirement-progress", "lost-timeout"),
            ("header-sync/peer-retirement-progress", "retained-owner"),
            ("header-sync/peer-retirement-progress", "session-view"),
            ("header-sync/peer-retirement-progress", "spent-on-refusal"),
            (
                "header-sync/peer-retirement-progress",
                "unreleased-retirement",
            ),
        ],
    },
    Shared {
        harness: LIVENESS,
        never: &[
            "header-sync/peer-retirement-progress",
            "header-sync/stale-chain-effect-inert",
            "header-sync/stale-completion-inert",
        ],
        undeclared: BOUNDED,
        cases: &[
            ("header-sync/hold-release-wakes", "committed-page-dropped"),
            ("header-sync/hold-release-wakes", "completion-arm-removed"),
            ("header-sync/hold-release-wakes", "exits-on-reply"),
            ("header-sync/hold-release-wakes", "lost-notification"),
            ("header-sync/hold-release-wakes", "page-job-wake-lost"),
        ],
    },
    Shared {
        harness: LIVENESS,
        never: &[
            "header-sync/hold-release-wakes",
            "header-sync/peer-retirement-progress",
            "header-sync/stale-completion-inert",
        ],
        undeclared: BOUNDED,
        cases: &[
            (
                "header-sync/state-publication-consistent",
                "postponed-while-changing",
            ),
            (
                "header-sync/state-publication-consistent",
                "stale-status-cache",
            ),
        ],
    },
    Shared {
        harness: LIVENESS,
        never: &[
            "header-sync/hold-release-wakes",
            "header-sync/stale-completion-inert",
        ],
        undeclared: &[],
        cases: &[
            (
                "header-sync/bounded-adversary-progress",
                "pause-outlasts-cooldown",
            ),
            (
                "header-sync/bounded-adversary-progress",
                "refusal-never-lifts",
            ),
        ],
    },
    Shared {
        harness: LIVENESS,
        never: &[
            "header-sync/hold-release-wakes",
            "header-sync/peer-retirement-progress",
            "header-sync/reset-invalidates-work",
            "header-sync/stale-chain-effect-inert",
            "header-sync/stale-completion-inert",
            "header-sync/storage-operation-ids",
        ],
        undeclared: BOUNDED,
        cases: &[(
            "header-sync/state-publication-consistent",
            "permanent-refusal",
        )],
    },
    Shared {
        harness: LIVENESS,
        never: &[
            "header-sync/hold-release-wakes",
            "header-sync/peer-retirement-progress",
            "header-sync/stale-completion-inert",
        ],
        undeclared: &[],
        cases: &[("header-sync/bounded-adversary-progress", "skipped-apply")],
    },
    Shared {
        harness: LIVENESS,
        never: &[
            "header-sync/hold-release-wakes",
            "header-sync/peer-retirement-progress",
            "header-sync/stale-completion-inert",
            "header-sync/storage-operation-ids",
        ],
        undeclared: BOUNDED,
        cases: &[("header-sync/state-publication-consistent", "watch-ignored")],
    },
    Shared {
        harness: LIVENESS,
        never: &[
            "header-sync/peer-retirement-progress",
            "header-sync/stale-completion-inert",
        ],
        undeclared: &[],
        cases: &[(
            "header-sync/bounded-adversary-progress",
            "faulted-target-remembered",
        )],
    },
    Shared {
        harness: LIVENESS,
        never: &["header-sync/stale-completion-inert"],
        undeclared: &[],
        cases: &[(
            "header-sync/bounded-adversary-progress",
            "stalled-successor",
        )],
    },
    Shared {
        harness: FLOOD,
        never: &[
            "header-sync/peer-retirement-progress",
            "header-sync/stale-completion-inert",
        ],
        undeclared: NO_LIVELOCK,
        cases: &[
            (
                "header-sync/watch-observed-under-flood",
                "watch-check-revert",
            ),
            ("header-sync/watch-observed-under-flood", "wedged-run-loop"),
        ],
    },
    Shared {
        harness: FLOOD,
        never: &[
            "header-sync/peer-retirement-progress",
            "header-sync/stale-completion-inert",
            "header-sync/teardown-returns-baseline",
        ],
        undeclared: NO_LIVELOCK,
        cases: &[
            (
                "header-sync/watch-observed-under-lifecycle-flood",
                "watch-check-revert",
            ),
            (
                "header-sync/watch-observed-under-lifecycle-flood",
                "wedged-run-loop",
            ),
        ],
    },
    Shared {
        harness: RELEASE_WAKE,
        never: &["header-sync/peer-retirement-progress"],
        undeclared: NO_LIVELOCK,
        cases: &[
            ("header-sync/capacity-release-wakes", "dropped-waiting-peer"),
            (
                "header-sync/capacity-release-wakes",
                "missed-refill-deadline",
            ),
            ("header-sync/capacity-release-wakes", "missed-release-wake"),
            ("header-sync/capacity-release-wakes", "refill-adds-nothing"),
        ],
    },
    Shared {
        harness: REPAIR,
        never: &[],
        undeclared: NO_LIVELOCK,
        cases: &[(
            "header-sync/commit-only-authorized",
            "v1-record-charged-twice",
        )],
    },
    Shared {
        harness: REPAIR,
        never: &["header-sync/stale-chain-effect-inert"],
        undeclared: NO_LIVELOCK,
        cases: &[(
            "header-sync/outstanding-work-bounded",
            "departed-source-unpruned",
        )],
    },
];

/// How a case's patched completed runs report otherwise than its baseline.
struct PatchedNa {
    /// The target.
    property: &'static str,
    /// The case.
    name: &'static str,
    /// The IDs the baseline never applies that apply there.
    applies: &'static [&'static str],
    /// The IDs the baseline applies that never apply there.
    never: &'static [&'static str],
    /// The IDs left `NotApplicable` there for another reason.
    other: &'static [(&'static str, NaReason)],
}

/// The cases whose recorded patched completed runs, every `Draft` run and
/// each patched run found at the end, report otherwise than their
/// baselines.
const PATCHED: &[PatchedNa] = &[
    // Its recorded Draft run, past the over-credit page, applies the
    // bad-cursor close, which its baseline never does, and never applies
    // the answers-open rule its baseline applies.
    PatchedNa {
        property: "header-sync/s5v9/subscribe-headers/sender-within-credit",
        name: "page-past-credit",
        applies: &["header-sync/s5v9/subscribe-headers/bad-cursor-closes"],
        never: &["header-sync/s5v9/headers/sender-answers-open"],
        other: &[],
    },
    // Its recorded Draft run, past the repeated `Open`, closes the honest
    // peer and never applies the stale-completion rule its baseline
    // applies.
    PatchedNa {
        property: "header-sync/s5v9/subscribe-headers/sender-within-live",
        name: "open-repeated",
        applies: &[],
        never: &["header-sync/stale-completion-inert"],
        other: &[],
    },
    // Past the refused page, the node closes the peer, so it retires it and
    // never reaches a stale completion or chain effect.
    PatchedNa {
        property: "header-sync/s5v9/conformant-never-closes",
        name: "target-page-refused",
        applies: &["header-sync/peer-retirement-progress"],
        never: &[
            "header-sync/stale-completion-inert",
            "header-sync/stale-chain-effect-inert",
        ],
        other: &[],
    },
    // The same patch, on the honest-sync input.
    PatchedNa {
        property: "session/honest-never-punished",
        name: "target-page-refused",
        applies: &["header-sync/peer-retirement-progress"],
        never: &[
            "header-sync/stale-completion-inert",
            "header-sync/stale-chain-effect-inert",
        ],
        other: &[],
    },
    // Past the misnamed page, no outcome ends a page the peer took.
    PatchedNa {
        property: "header-sync/s5v9/headers/sender-answers-open",
        name: "page-misnamed",
        applies: &[],
        never: &["header-sync/s5v9/headers-outcome/sender-ends-once"],
        other: &[],
    },
    // Past the dropped frame, the node ends its page with an outcome.
    PatchedNa {
        property: "header-sync/s5v9/subscribe-headers/after-close-closes",
        name: "after-close-dropped",
        applies: &["header-sync/s5v9/headers-outcome/sender-ends-once"],
        never: &[],
        other: &[],
    },
    // Past the repeated locator, the node subscribes the peer and reads
    // storage for it.
    PatchedNa {
        property: "header-sync/s5v9/subscribe-headers/bad-content-closes",
        name: "repeated-locator-unchecked",
        applies: &["header-sync/storage-operation-ids"],
        never: &[],
        other: &[],
    },
    // Found at the end: the forgotten completion never goes stale.
    PatchedNa {
        property: "header-sync/teardown-returns-baseline",
        name: "forgotten-completion",
        applies: &[],
        never: &[
            "header-sync/stale-completion-inert",
            "header-sync/stale-chain-effect-inert",
        ],
        other: &[],
    },
    // Found at the end: the wedged loop never tears down or reaches a stale
    // chain effect. The other flood's watch rule breaks at the same settled
    // end, on the same publication, so it's in the case's set.
    PatchedNa {
        property: "header-sync/watch-observed-under-flood",
        name: "wedged-run-loop",
        applies: &[],
        never: &[
            "header-sync/stale-chain-effect-inert",
            "header-sync/teardown-returns-baseline",
        ],
        other: &[],
    },
    // Found at the end: the wedged loop never touches storage, and breaks
    // the other flood's watch rule too, as above.
    PatchedNa {
        property: "header-sync/watch-observed-under-lifecycle-flood",
        name: "wedged-run-loop",
        applies: &[],
        never: &["header-sync/storage-operation-ids"],
        other: &[],
    },
    // Found at the end: the reactor returns before it takes the late
    // failure, so no completion ends stale.
    PatchedNa {
        property: "header-sync/header-fault-penalized",
        name: "idle-exit-drops-fault",
        applies: &[],
        never: &["header-sync/stale-completion-inert"],
        other: &[],
    },
    // Found at the end: with nothing applied, nothing goes stale.
    PatchedNa {
        property: "header-sync/honest-progress",
        name: "skipped-apply",
        applies: &[],
        never: &[
            "header-sync/stale-completion-inert",
            "header-sync/stale-chain-effect-inert",
        ],
        other: &[],
    },
    // Found at the end: the dropped peer retires, and nothing goes stale.
    PatchedNa {
        property: "header-sync/honest-progress",
        name: "dropped-honest-peer",
        applies: &["header-sync/peer-retirement-progress"],
        never: &[
            "header-sync/stale-completion-inert",
            "header-sync/stale-chain-effect-inert",
        ],
        other: &[],
    },
];

/// A case's expected `NotApplicable` reports: its counted runs', and its
/// patched completed runs' where they differ.
type Lists = (Vec<ExpectedNa>, Option<Vec<ExpectedNa>>);

/// Every case [`CASES`] or [`SHARED`] holds, by target and name, with
/// [`PATCHED`]'s changes.
fn lists() -> &'static BTreeMap<(&'static str, &'static str), Lists> {
    static LISTS: OnceLock<BTreeMap<(&'static str, &'static str), Lists>> = OnceLock::new();
    LISTS.get_or_init(|| {
        // The reasons' names and the generated IDs, built once, so the
        // leak is bounded.
        let name =
            |reason: NaReason| -> &'static str { Box::leak(reason.name().into_owned().into()) };
        let never = name(NaReason::NeverApplied);
        let undeclared = name(NaReason::Undeclared);
        let reason = |id, undeclared_ids: &[&str]| {
            if undeclared_ids.contains(&id) {
                undeclared
            } else {
                never
            }
        };
        let matrix = harness_matrix();
        let mut lists = BTreeMap::new();
        for entry in CASES {
            let (property, on): (&'static str, _) = match entry.property {
                Target::Generated(suffix) => (
                    Box::leak(format!("header-sync/s5v9/{suffix}").into_boxed_str()),
                    WIRE,
                ),
                Target::Id(id, harness) => (id, harness),
            };
            let list = matrix
                .iter()
                .filter(|(harness, id, _)| {
                    *harness == on && *id != property && !entry.applies.contains(id)
                })
                .map(|&(_, id, _)| ExpectedNa {
                    property: id,
                    reason: reason(id, entry.undeclared),
                })
                .collect();
            let fresh = lists.insert((property, entry.name), (list, None)).is_none();
            assert!(fresh, "{property}/{} is listed once", entry.name);
        }
        for group in SHARED {
            let list: Vec<ExpectedNa> = matrix
                .iter()
                .filter(|(harness, id, _)| {
                    *harness == group.harness
                        && (group.never.contains(id) || group.undeclared.contains(id))
                })
                .map(|&(_, id, _)| ExpectedNa {
                    property: id,
                    reason: reason(id, group.undeclared),
                })
                .collect();
            for &(property, case) in group.cases {
                let fresh = lists
                    .insert((property, case), (list.clone(), None))
                    .is_none();
                assert!(fresh, "{property}/{case} is listed once");
            }
        }
        for change in PATCHED {
            let (counted, patched) = lists
                .get_mut(&(change.property, change.name))
                .unwrap_or_else(|| panic!("{}/{} has a list", change.property, change.name));
            let mut list: Vec<ExpectedNa> = counted
                .iter()
                .filter(|na| !change.applies.contains(&na.property))
                .copied()
                .collect();
            assert_eq!(
                counted.len() - list.len(),
                change.applies.len(),
                "{}/{}: each newly applied ID was NotApplicable",
                change.property,
                change.name
            );
            let added = change
                .never
                .iter()
                .map(|&id| (id, never))
                .chain(change.other.iter().map(|&(id, why)| (id, name(why))));
            for (id, why) in added {
                assert!(
                    !counted.iter().any(|na| na.property == id),
                    "{}/{}: {id} applied in the baseline",
                    change.property,
                    change.name
                );
                list.push(ExpectedNa {
                    property: id,
                    reason: why,
                });
            }
            *patched = Some(list);
        }
        lists
    })
}

/// The `NotApplicable` reports `case`'s counted runs expect beside its
/// target, if [`CASES`] or [`SHARED`] holds it: each ID of its harness it
/// doesn't apply, as `undeclared` where its scenario doesn't declare the
/// ID's assumptions and `never-applied` otherwise. A case neither holds
/// expects none, so a catch declared without its runs' reports can't
/// verify.
pub fn expected(case: CaseRef) -> &'static [ExpectedNa] {
    lists()
        .get(&(case.property, case.name))
        .map_or(&[], |(counted, _)| counted.as_slice())
}

/// The `NotApplicable` reports `case`'s patched completed runs expect beside
/// its set, its `Draft` run and a patched run found at the end, where
/// [`PATCHED`] says they differ from [`expected`]'s.
pub fn patched_expected(case: CaseRef) -> Option<&'static [ExpectedNa]> {
    lists()
        .get(&(case.property, case.name))
        .and_then(|(_, patched)| patched.as_deref())
}
