//! The obligation keys acceptance requires (Rule 21), derived from
//! inventories the manifest doesn't control: the registry's IDs and
//! statuses, each property's `needs`, the planted and native-mutation
//! directories, `COVERAGE`'s named tests, the contracts and narrowing
//! entries, [`GATE_REQUIREMENTS`](super::GATE_REQUIREMENTS) and
//! [`COMPOSITION_CHECKS`](super::COMPOSITION_CHECKS). Each key's gates are
//! fixed here, by [`gate_rule`], not by the manifest.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    contracts::{contract_test_key, Contract, ContractId, ContractState, Narrowing},
    reasons::NaEntry,
    sim_test, Approval, CaseRun, Expected, ExpectedResult, Gate, HistoryRef, Key, RunId, TestRef,
    Tier, PLAN, TIERS,
};
use crate::property::Status;

/// One `(harness, ID)` registration, as the registry reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Registered {
    /// The harness.
    pub harness: String,
    /// The property.
    pub property: String,
    /// Its status there.
    pub status: Status,
    /// Whether the harness is a machine.
    pub machine: bool,
}

/// What [`required_keys`] reads.
pub struct Inventories<'a> {
    /// Every header registration.
    pub registered: &'a [Registered],
    /// Every `(harness, source)` a header property there needs, by the
    /// source's `Debug` name.
    pub needs: &'a [(String, String)],
    /// Every planted case under `qa/planted/header-sync/` and
    /// `qa/planted/header/`, as `(property directory, case name)`.
    pub planted: &'a [(String, String)],
    /// Each planted case's harness, by `(property directory, case name)`,
    /// read from its committed input's sidecar
    /// ([`planted_harness`](super::planted_harness)) apart from the rows.
    /// A case with no readable harness counts as a node case.
    pub planted_harnesses: &'a BTreeMap<(String, String), String>,
    /// Every directory under `qa/native-mutations/header-sync/`, retired
    /// ones included.
    pub native: &'a [String],
    /// Every `(file, test, property)` `COVERAGE` names for a header row,
    /// all in the `zakura-sim` package.
    pub coverage_tests: &'a [(String, String, String)],
    /// The contracts table.
    pub contracts: &'a [Contract],
    /// The contracts that must each have a row.
    pub required_contracts: &'a [ContractId],
    /// The narrowing entries.
    pub narrowing: &'a [Narrowing],
    /// The gate obligations no row owns.
    pub requirements: &'a [GateObligation],
    /// The composition checks.
    pub composition: &'a [&'a str],
    /// The `NotApplicable` reasons a header ID can carry.
    pub reasons: &'a [NaEntry],
    /// The manifest's expected results, read only for the planted runs they
    /// count, each of which owes a replay and, when it completes, a cleanup
    /// comparison.
    pub expected: &'a [ExpectedResult],
    /// Every history a counted witness test runs, with the property its
    /// pass judges, each of which owes a whole-report pass, a replay, a
    /// cleanup comparison and, on a node harness, its expected
    /// `NotApplicable` list.
    pub histories: &'a [(HistoryRef, &'a str)],
    /// Whether a harness is a machine.
    pub machine: fn(&str) -> bool,
}

/// What may satisfy a gate obligation, written apart from the result that
/// does ([`GATE_REQUIREMENTS`](super::GATE_REQUIREMENTS)).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Permit {
    /// Exactly this test's `Passes`.
    Test(TestRef),
    /// A transitional `Open` until exactly this PR, with exactly this
    /// reason, so another obligation's `Open` for the same PR doesn't
    /// satisfy it.
    Until {
        /// Who owns the missing evidence.
        owner: &'static str,
        /// The PR that builds it.
        pr: &'static str,
        /// Why it's open.
        reason: &'static str,
    },
    /// A standing `Open` with exactly this reason and approval, which counts
    /// only once approved.
    Standing {
        /// Who owns the claim.
        owner: &'static str,
        /// Its reason.
        reason: &'static str,
        /// Its approval.
        approval: Approval,
    },
}

/// One gate obligation no row owns.
#[derive(Clone, Copy, Debug)]
pub struct GateObligation {
    /// Its name, its [`Key::Requirement`].
    pub name: &'static str,
    /// The gates it serves.
    pub gates: &'static [Gate],
    /// What may satisfy it.
    pub permit: Permit,
}

impl Permit {
    /// Whether `expected` is what this permits.
    pub fn allows(self, expected: &Expected) -> bool {
        match (self, expected) {
            (Permit::Test(test), Expected::Passes { test: t }) => test == *t,
            (
                Permit::Until { owner, pr, reason },
                Expected::Open {
                    until,
                    reason: r,
                    owner: o,
                    ..
                },
            ) => *until == Some(pr) && *r == reason && *o == owner,
            (
                Permit::Standing {
                    owner,
                    reason,
                    approval,
                },
                Expected::Open {
                    until,
                    approval: given,
                    reason: r,
                    owner: o,
                },
            ) => until.is_none() && *given == approval && *r == reason && *o == owner,
            _ => false,
        }
    }
}

/// Why `gate-1/a-a-and-a-b-a` is open until hdr-14.7b, whose fresh-process
/// recordings and replays it compares.
pub const GATE_1_A_A_AND_A_B_A: &str =
    "the acceptance tool runs each history A twice and between a distinct B in \
                     one process, and compares traces, reports and store snapshots";

/// Why `gate-1/topology-freshness` is open until hf-7.
pub const GATE_1_TOPOLOGY_FRESHNESS: &str =
    "a topology change with no reactor event shows in the next view";

/// Why `gate-1/store-inventory` is open until hf-6.
pub const GATE_1_STORE_INVENTORY: &str =
    "every store a header check reads is registered, behind a checkpoint and \
                     has a producer test";

/// Why `gate-1/replay-store-control` is open until hdr-14.7b.
pub const GATE_1_REPLAY_STORE_CONTROL: &str =
    "a changed stored fact with every report the same must fail the replay \
                     comparison";

/// Why `gate-2/end-boundary-preservation` is open until hdr-14.8.
pub const GATE_2_END_BOUNDARY_PRESERVATION: &str =
    "every distinct end finding at one boundary survives, which C12 and C13 \
                     deliver";

/// Why `gate-3/crossing/unretained-open-then-outcome` is open until hdr-14.6.
pub const GATE_3_CROSSING_UNRETAINED_OPEN_THEN_OUTCOME: &str =
    "Act::UnretainedOpen then Act::OutcomeCrossing, labelled conformant with no \
                     punishment";

/// Why `gate-3/crossing/crossing-close` is open until hdr-14.6.
pub const GATE_3_CROSSING_CROSSING_CLOSE: &str =
    "a subscription Close that crosses the node's ending, labelled conformant \
                     with no punishment";

/// Why `gate-3/crossing/over-window-grant` is open until hdr-14.6.
pub const GATE_3_CROSSING_OVER_WINDOW_GRANT: &str =
    "a crossed Grant above the window, labelled bad-cursor and closed";

/// Why `gate-5/bound-table` is open until hdr-9d.1.
pub const GATE_5_BOUND_TABLE: &str =
    "each row of outstanding-work-bounded's bound table: its at-limit, \
                     one-over-denied and release-then-reuse fixture, its cleanup or \
                     conservation witness, or its exclusion";

/// Why `gate-5/mixed-witness` is open until hdr-14.8.
pub const GATE_5_MIXED_WITNESS: &str =
    "one storage scenario running all four Sim resource IDs together";

/// Every gate obligation no row owns, with its gates and what may satisfy
/// it, written from the spec's gate table.
pub static GATE_OBLIGATIONS: &[GateObligation] = &[
    GateObligation {
        name: "gate-1/a-a-and-a-b-a",
        gates: &[Gate::G1],
        permit: Permit::Until {
            owner: "Adam's side",
            pr: "hdr-14.7b",
            reason: GATE_1_A_A_AND_A_B_A,
        },
    },
    GateObligation {
        name: "gate-1/topology-freshness",
        gates: &[Gate::G1],
        permit: Permit::Until {
            owner: "Adam's harness",
            pr: "hf-7",
            reason: GATE_1_TOPOLOGY_FRESHNESS,
        },
    },
    GateObligation {
        name: "gate-1/store-inventory",
        gates: &[Gate::G1],
        permit: Permit::Until {
            owner: "Adam's harness",
            pr: "hf-6",
            reason: GATE_1_STORE_INVENTORY,
        },
    },
    GateObligation {
        name: "gate-1/replay-store-control",
        gates: &[Gate::G1],
        permit: Permit::Until {
            owner: "Adam's side",
            pr: "hdr-14.7b",
            reason: GATE_1_REPLAY_STORE_CONTROL,
        },
    },
    GateObligation {
        name: "gate-2/conformant-control-per-fault",
        gates: &[Gate::G2],
        permit: Permit::Test(sim_test(
            "tests/header_wire_witness.rs",
            "each_invalid_label_witness_has_a_conformant_control",
        )),
    },
    GateObligation {
        name: "gate-2/despite-close/caught",
        gates: &[Gate::G2],
        permit: Permit::Test(sim_test(
            "tests/header_despite_close.rs",
            "the_path_break_needs_a_retained_violation",
        )),
    },
    GateObligation {
        name: "gate-2/despite-close/close-companion",
        gates: &[Gate::G2],
        permit: Permit::Test(sim_test(
            "tests/header_despite_close.rs",
            "the_fault_close_holds_on_the_unpatched_rejection",
        )),
    },
    GateObligation {
        name: "gate-2/despite-close/staging-companion",
        gates: &[Gate::G2],
        permit: Permit::Test(sim_test(
            "tests/header_despite_close.rs",
            "the_staging_companion_needs_the_rejected_page_staged",
        )),
    },
    GateObligation {
        name: "gate-2/bad-content-fault-close/status",
        gates: &[Gate::G2],
        permit: Permit::Test(sim_test(
            "tests/header_wire_table.rs",
            "status_bad_content_closes_with_a_fault",
        )),
    },
    GateObligation {
        name: "gate-2/bad-content-fault-close/subscribe-headers",
        gates: &[Gate::G2],
        permit: Permit::Test(sim_test(
            "tests/header_wire_table.rs",
            "subscribe_headers_bad_content_closes_with_a_fault",
        )),
    },
    GateObligation {
        name: "gate-2/bad-content-fault-close/headers",
        gates: &[Gate::G2],
        permit: Permit::Test(sim_test(
            "tests/header_wire_table.rs",
            "headers_bad_content_closes_with_a_fault",
        )),
    },
    GateObligation {
        name: "gate-2/bad-content-fault-close/headers-outcome",
        gates: &[Gate::G2],
        permit: Permit::Test(sim_test(
            "tests/header_wire_table.rs",
            "headers_outcome_bad_content_closes_with_a_fault",
        )),
    },
    GateObligation {
        name: "gate-2/bad-content-close-claim",
        gates: &[Gate::G2],
        permit: Permit::Standing {
            owner: "the table-derived properties",
            reason: "no property fails a node that answers header bad content with a \
                     Misbehavior and leaves the connection open; the table-derived properties \
                     own that claim (ask 5)",
            approval: PLAN,
        },
    },
    GateObligation {
        name: "gate-2/downstream-templates",
        gates: &[Gate::G2],
        permit: Permit::Standing {
            owner: "hdr-9 to hdr-13",
            reason: "header sync judges downstream effects at the owner boundary (Rule 10), so \
                     the generic templates stay unregistered (decision 3)",
            approval: Approval::Adam {
                record: "hdr-14 plan, decision 3",
            },
        },
    },
    GateObligation {
        name: "gate-2/same-step-preservation",
        gates: &[Gate::G2],
        permit: Permit::Test(sim_test(
            "tests/registry.rs",
            "known_and_active_failures_at_one_boundary",
        )),
    },
    GateObligation {
        name: "gate-2/end-boundary-preservation",
        gates: &[Gate::G2],
        permit: Permit::Until {
            owner: "Evan (E9) and Adam's harness (hf-1 to hf-3)",
            pr: "hdr-14.8",
            reason: GATE_2_END_BOUNDARY_PRESERVATION,
        },
    },
    GateObligation {
        name: "gate-3/crossing/unretained-open-then-outcome",
        gates: &[Gate::G3],
        permit: Permit::Until {
            owner: "Adam's side",
            pr: "hdr-14.6",
            reason: GATE_3_CROSSING_UNRETAINED_OPEN_THEN_OUTCOME,
        },
    },
    GateObligation {
        name: "gate-3/crossing/crossing-close",
        gates: &[Gate::G3],
        permit: Permit::Until {
            owner: "Adam's side",
            pr: "hdr-14.6",
            reason: GATE_3_CROSSING_CROSSING_CLOSE,
        },
    },
    GateObligation {
        name: "gate-3/crossing/over-window-grant",
        gates: &[Gate::G3],
        permit: Permit::Until {
            owner: "Adam's side",
            pr: "hdr-14.6",
            reason: GATE_3_CROSSING_OVER_WINDOW_GRANT,
        },
    },
    GateObligation {
        name: "gate-3/no-request-row",
        gates: &[Gate::G3],
        permit: Permit::Standing {
            owner: "hdr-14",
            reason: "header sync has no request row, so within-limit, over-margin, \
                     receiver-serving-bounded and sender-within-response-quantity aren't \
                     generated for it",
            approval: PLAN,
        },
    },
    GateObligation {
        name: "gate-3/credit-fits-window",
        gates: &[Gate::G3],
        permit: Permit::Standing {
            owner: "the table-derived properties",
            reason: "on-wire credit against the receive window belongs to the table-derived \
                     properties (Rule 10, ask 5)",
            approval: Approval::Pending,
        },
    },
    GateObligation {
        name: "gate-4/send-interval",
        gates: &[Gate::G4],
        permit: Permit::Standing {
            owner: "the table toolkit's sender cadence, or hdr-12b",
            reason: "CadenceModel doesn't model send_interval and the status floor is unjudged \
                     (ask 5)",
            approval: Approval::Pending,
        },
    },
    GateObligation {
        name: "gate-5/bound-table",
        gates: &[Gate::G5],
        permit: Permit::Until {
            owner: "Adam's side",
            pr: "hdr-9d.1",
            reason: GATE_5_BOUND_TABLE,
        },
    },
    GateObligation {
        name: "gate-5/mixed-witness",
        gates: &[Gate::G5],
        permit: Permit::Until {
            owner: "Adam's side",
            pr: "hdr-14.8",
            reason: GATE_5_MIXED_WITNESS,
        },
    },
];

/// The gates a header ID's keys serve, written from the spec's gate table.
/// `None` for an ID no rule names, which the static check refuses.
pub fn gate_rule(property: &str) -> Option<Vec<Gate>> {
    use Gate::*;
    if let Some(suffix) = property.strip_prefix("header-sync/s5v9/") {
        let subscription_close = [
            "over-live-closes",
            "over-credit-closes",
            "after-close-closes",
            "bad-cursor-closes",
            "over-owed-closes",
        ];
        return Some(if suffix == "status/cadence-never-closes" {
            vec![G2, G4]
        } else if suffix == "status/sender-cadence" {
            vec![G3, G4]
        } else if suffix.contains("/sender-") {
            vec![G3]
        } else if subscription_close
            .iter()
            .any(|label| suffix.ends_with(label))
        {
            vec![G2, G3]
        } else if suffix.ends_with("-closes") {
            vec![G2]
        } else {
            return None;
        });
    }
    Some(match property {
        // A watch rule is a freshness claim: the published state is read
        // before the flooded queue's third event.
        "header-sync/storage-operation-ids"
        | "harness/direct-dispatch"
        | "header-sync/watch-observed-under-flood"
        | "header-sync/watch-observed-under-lifecycle-flood" => vec![G1],
        "header-sync/header-fault-penalized" | "session/honest-never-punished" => vec![G2],
        // Downstream effects judged at the owner boundary (Rule 10): which
        // authority survives a reset, and what stages, prepares and
        // commits, whose credit and update rules are exchange rules too.
        "header-sync/reset-invalidates-work" | "header-sync/stale-chain-effect-inert" => vec![G2],
        "header-sync/commit-only-authorized" => vec![G2, G3],
        // Which tip a settled node selects, judged at the engine that owns
        // the choice.
        "header-sync/expected-chain-selected" => vec![G2],
        // A committed state's publication, adoption and advertisement in
        // order, each served page as an exchange with what was read, and a
        // timely status.
        "header-sync/state-publication-consistent" => vec![G1, G3, G4],
        "header-sync/chunk-budget-bound"
        | "header-sync/teardown-returns-baseline"
        | "header-sync/stale-completion-inert"
        | "header-sync/capacity-release-wakes"
        | "header-sync/outstanding-work-bounded"
        // A held reply's wake, like a capacity release's.
        | "header-sync/hold-release-wakes"
        // Departed sessions' retained work and its hand-off.
        | "header-sync/peer-retirement-progress"
        | "header/reactor-policy"
        | "header/publisher-policy" => vec![G5],
        // Liveness and the step loop's own claims serve no single gate. The
        // run's overall acceptance still counts them.
        "header-sync/honest-progress"
        | "header-sync/bounded-adversary-progress"
        | "sim/no-task-panic"
        | "sim/no-livelock" => Vec::new(),
        _ => return None,
    })
}

/// The executions an expected result counts, each of which owes a replay,
/// and the completed node-harness ones among them, each of which owes a
/// cleanup comparison. A machine's runs own no store (see
/// [`counted_results`](super::rows::counted_results)).
pub fn counted_runs(expected: &Expected) -> (Vec<RunId>, Vec<RunId>) {
    match *expected {
        Expected::Holds { history, .. } | Expected::StaysNotApplicable { history, .. } => {
            let run = RunId::from(history);
            (vec![run.clone()], vec![run])
        }
        // A patched run found at a step stops there, so only a completed
        // one, found at the end, owes a cleanup comparison with the baseline
        // and Draft runs.
        Expected::Caught { case, found, .. } => {
            let mut completed = vec![
                RunId::case(case, CaseRun::Baseline),
                RunId::case(case, CaseRun::Draft),
            ];
            if found == super::Found::AtEnd {
                completed.push(RunId::case(case, CaseRun::Patched));
            }
            (
                vec![
                    RunId::case(case, CaseRun::Baseline),
                    RunId::case(case, CaseRun::Patched),
                    RunId::case(case, CaseRun::Draft),
                ],
                completed,
            )
        }
        Expected::MachineCaught { case, .. } => (
            vec![
                RunId::case(case, CaseRun::Baseline),
                RunId::case(case, CaseRun::Patched),
            ],
            Vec::new(),
        ),
        _ => (Vec::new(), Vec::new()),
    }
}

/// Every key acceptance requires, with the gates it serves.
///
/// A header ID owes a passing key, and an `Active` one a catch key too.
/// Each planted case owes its own case key, each native mutation a key,
/// each needed source a missing-source key, each `COVERAGE` test a witness
/// key, each `Satisfied` contract's proving test and each narrowing entry
/// a test key, each requirement and composition check its key, and each
/// counted execution a replay and, for a history, a cleanup comparison.
/// Each `NotApplicable` reason owes its test, and each ID an entry for every
/// tier other than its own.
/// An ID [`gate_rule`] doesn't know gets no gates here, and the static
/// check names it.
pub fn required_keys(inv: &Inventories<'_>) -> BTreeMap<Key, BTreeSet<Gate>> {
    let mut keys = BTreeMap::new();
    let gates_of = |property: &str| -> BTreeSet<Gate> {
        gate_rule(property)
            .unwrap_or_default()
            .into_iter()
            .collect()
    };
    for registration in inv.registered {
        let gates = gates_of(&registration.property);
        keys.insert(Key::passing(&registration.property), gates.clone());
        if registration.status == Status::Active {
            keys.insert(Key::catch(&registration.property), gates);
        }
    }
    for (property, name) in inv.planted {
        keys.insert(
            Key::Case {
                property: property.clone(),
                name: name.clone(),
            },
            gates_of(property),
        );
    }
    for patch in inv.native {
        keys.insert(
            Key::Native {
                patch: patch.clone(),
            },
            BTreeSet::from([Gate::G6]),
        );
    }
    for (harness, source) in inv.needs {
        keys.insert(
            Key::MissingSource {
                harness: harness.clone(),
                source: source.clone(),
            },
            BTreeSet::from([Gate::G6]),
        );
    }
    // A witness test serves the gates of every property it witnesses.
    for (file, test, property) in inv.coverage_tests {
        keys.entry(Key::Witness {
            package: "zakura-sim".to_owned(),
            file: file.clone(),
            test: test.clone(),
        })
        .or_insert_with(BTreeSet::new)
        .extend(gate_rule(property).unwrap_or_default());
    }
    for &id in inv.required_contracts {
        for contract in inv.contracts.iter().filter(|contract| contract.id == id) {
            if contract.state == ContractState::Satisfied {
                for &test in contract.proving {
                    keys.insert(contract_test_key(id, test), BTreeSet::new());
                }
            }
        }
    }
    for entry in inv.narrowing {
        keys.insert(
            Key::NarrowingTest {
                entry: entry.entry.to_owned(),
            },
            BTreeSet::new(),
        );
    }
    for obligation in inv.requirements {
        keys.insert(
            Key::Requirement {
                name: obligation.name.to_owned(),
            },
            obligation.gates.iter().copied().collect(),
        );
    }
    for &test in inv.composition {
        keys.insert(
            Key::Composition {
                test: test.to_owned(),
            },
            BTreeSet::new(),
        );
    }
    for &(history, target) in inv.histories {
        let run = RunId::from(history);
        keys.insert(
            Key::History {
                run: run.clone(),
                target: target.to_owned(),
            },
            gate_rule(target).unwrap_or_default().into_iter().collect(),
        );
        keys.insert(Key::Replay { run: run.clone() }, BTreeSet::from([Gate::G1]));
        keys.insert(
            Key::Cleanup { run: run.clone() },
            BTreeSet::from([Gate::G1]),
        );
        if !(inv.machine)(history.harness) {
            keys.insert(Key::NaList { run }, BTreeSet::from([Gate::G6]));
        }
    }
    // Every planted case's baseline and patched runs owe a replay, and a
    // node case's `Draft` run does too, its baseline and Draft runs a
    // cleanup comparison as well, from the planted inventory itself. Only
    // whether a node case's patched run completes, so owes a cleanup too,
    // comes from its row's `found`, below.
    for (property, name) in inv.planted {
        let case = |run| RunId::Case {
            property: property.clone(),
            name: name.clone(),
            run,
        };
        let machine = inv
            .planted_harnesses
            .get(&(property.clone(), name.clone()))
            .is_some_and(|harness| (inv.machine)(harness));
        for run in [CaseRun::Baseline, CaseRun::Patched] {
            keys.insert(Key::Replay { run: case(run) }, BTreeSet::from([Gate::G1]));
        }
        if !machine {
            keys.insert(
                Key::Replay {
                    run: case(CaseRun::Draft),
                },
                BTreeSet::from([Gate::G1]),
            );
            for run in [CaseRun::Baseline, CaseRun::Draft] {
                keys.insert(Key::Cleanup { run: case(run) }, BTreeSet::from([Gate::G1]));
            }
        }
    }
    for entry in inv.reasons {
        keys.insert(
            Key::NaReason {
                reason: entry.reason,
            },
            BTreeSet::from([Gate::G6]),
        );
    }
    for registration in inv.registered {
        let own = if registration.machine {
            Tier::Machine
        } else {
            Tier::Sim
        };
        for tier in TIERS.into_iter().filter(|tier| *tier != own) {
            keys.insert(
                Key::Tier {
                    property: registration.property.clone(),
                    tier: format!("{tier:?}"),
                },
                BTreeSet::from([Gate::G6]),
            );
        }
    }
    for result in inv.expected {
        let (replayed, cleaned) = counted_runs(&result.expected);
        for run in replayed {
            keys.insert(Key::Replay { run }, BTreeSet::from([Gate::G1]));
        }
        for run in cleaned {
            keys.insert(Key::Cleanup { run }, BTreeSet::from([Gate::G1]));
        }
    }
    keys
}
