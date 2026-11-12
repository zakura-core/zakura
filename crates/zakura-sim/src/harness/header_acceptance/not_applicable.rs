//! The `NotApplicable` reports the generated IDs' planted cases and the
//! despite-close case expect on their counted runs, beside their targets.
//!
//! The evaluator holds every property of a counted run outside its target
//! to `Held`, or to the `NotApplicable` reason its row lists for it
//! ([`super::evaluate`]). Each of these cases runs a focused
//! `header-sync/wire` input that applies only a few of the harness's IDs, so
//! [`CASES`] names, per case, the IDs its run applies and the IDs whose
//! assumptions its scenario doesn't declare, as its recorded baseline
//! reports them. Every other wire ID in [`harness_matrix`] never applied
//! there. [`expected`] spells that out as the row's list.
//!
//! Seven cases' `Draft` runs, which go on past the patched break, report
//! otherwise, so no one list fits both runs: `target-page-refused`,
//! `outcome-window-unchecked`, `page-misnamed`, `after-close-dropped` and
//! `repeated-locator-unchecked` apply other IDs, `page-past-credit` also
//! breaks `sim/no-livelock`, and `exhausted-cadence-throttled` ends at a
//! harness failure. Their lists are their baselines'.

use std::{collections::BTreeMap, sync::OnceLock};

use super::{rows::harness_matrix, CaseRef, ExpectedNa};
use crate::NaReason;

/// `header-sync/wire`.
const WIRE: &str = "header-sync/wire";

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

/// What an adversary's scenario doesn't declare: no peer is honest, and
/// the node isn't expected to progress.
const ADVERSARY: &[&str] = &[
    "session/honest-never-punished",
    "header-sync/honest-progress",
];

/// What the scenario of the node serving an honest subscriber doesn't
/// declare: no livelock bound, and the node isn't expected to progress.
const SERVING: &[&str] = &["sim/no-livelock", "header-sync/honest-progress"];

/// A case's target.
enum Target {
    /// `header-sync/s5v9/<suffix>`, one of header sync's generated IDs.
    Generated(&'static str),
    /// A hand-written ID.
    Id(&'static str),
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

/// Each case's runs, as its recorded baseline reports them.
const CASES: &[CaseNa] = &[
    CaseNa {
        property: Target::Id("header-sync/commit-only-authorized"),
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

/// The `NotApplicable` reports `case`'s counted runs expect beside its
/// target, if [`CASES`] holds it: each wire ID it doesn't apply, as
/// `undeclared` where its scenario doesn't declare the ID's assumptions and
/// `never-applied` otherwise. A case it doesn't hold expects none, so a
/// catch declared without its runs' reports can't verify.
pub fn expected(case: CaseRef) -> &'static [ExpectedNa] {
    static LISTS: OnceLock<BTreeMap<(String, &'static str), Vec<ExpectedNa>>> = OnceLock::new();
    let lists = LISTS.get_or_init(|| {
        // Two names, built once, so the leak is bounded.
        let never: &'static str = Box::leak(NaReason::NeverApplied.name().into_owned().into());
        let undeclared: &'static str = Box::leak(NaReason::Undeclared.name().into_owned().into());
        let wire: Vec<&'static str> = harness_matrix()
            .into_iter()
            .filter(|(harness, _, _)| *harness == WIRE)
            .map(|(_, id, _)| id)
            .collect();
        CASES
            .iter()
            .map(|entry| {
                let property = match entry.property {
                    Target::Generated(suffix) => format!("header-sync/s5v9/{suffix}"),
                    Target::Id(id) => id.to_owned(),
                };
                let list = wire
                    .iter()
                    .filter(|id| **id != property && !entry.applies.contains(id))
                    .map(|&id| ExpectedNa {
                        property: id,
                        reason: if entry.undeclared.contains(&id) {
                            undeclared
                        } else {
                            never
                        },
                    })
                    .collect();
                ((property, entry.name), list)
            })
            .collect()
    });
    lists
        .get(&(case.property.to_owned(), case.name))
        .map_or(&[], Vec::as_slice)
}
