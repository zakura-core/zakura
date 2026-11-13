//! The obligation keys acceptance requires (Rule 21), derived from
//! inventories the manifest doesn't control: the registry's IDs and
//! statuses, each property's `needs`, the planted and native-mutation
//! directories, `COVERAGE`'s named tests, the contracts and narrowing
//! entries, [`GATE_REQUIREMENTS`](super::GATE_REQUIREMENTS) and
//! [`COMPOSITION_CHECKS`](super::COMPOSITION_CHECKS). Each key's gates are
//! fixed here, by [`gate_rule`], not by the manifest.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    contracts::{
        contract_catch_key, contract_test_key, Contract, ContractId, ContractState, Narrowing,
    },
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
    /// Every planted case whose `.toml` names `missing_evidence`, a
    /// harness-caught case with no `Draft` run
    /// ([`planted_harness_caught`](super::planted_harness_caught)), read
    /// apart from the rows.
    pub harness_caught: &'a BTreeSet<(String, String)>,
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

/// Gate 1's topology freshness test (hf-7): a link the harness closes,
/// with no reactor event between, shows in the next view.
pub const TOPOLOGY_FRESHNESS_TEST: TestRef = sim_test(
    "tests/header_node_session_ends.rs",
    "a_link_the_harness_closes_shows_in_the_next_view",
);

/// Gate 1's store inventory, its first half (hf-6): every field holding
/// shared state in the header harness's sources is a registered store or
/// an allowed live resource.
pub const STORE_INVENTORY_REGISTERED_TEST: TestRef = sim_test(
    "tests/evidence_inventory.rs",
    "every_shared_field_is_a_registered_store_or_allowed",
);

/// Gate 1's store inventory, its second half (hf-6, hf-7): every
/// registered store names its checkpoint and a producer test that exists.
pub const STORE_INVENTORY_PRODUCER_TEST: TestRef = sim_test(
    "tests/evidence_inventory.rs",
    "every_registered_store_names_its_checkpoint_and_producer",
);

/// Gate 2's end-boundary preservation in the registry (C12): every
/// distinct end finding at one boundary is kept.
pub const END_BOUNDARY_REGISTRY_TEST: TestRef =
    sim_test("tests/registry.rs", "every_end_finding_is_kept");

/// Gate 2's end-boundary preservation in the fault store (C13): a settled
/// end reports every open fault, not just the first.
pub const END_BOUNDARY_FAULT_STORE_TEST: TestRef = sim_test(
    "src/harness/header_node/fault.rs",
    "harness::header_node::fault::tests::the_end_reports_every_open_fault",
);

/// Gate 5's bound-table limit fixtures (hdr-9d.1): work slots, locator
/// reads, and node and peer execution slots each reach their limit and no
/// further while one more party waits, the departed strikes and cooldowns
/// reach theirs, and the slot fixture's per-round reservations, staged
/// capacity and request deadlines reach 16 with them.
pub const BOUND_LIMIT_TEST: TestRef = sim_test(
    "tests/header_outstanding.rs",
    "the_limit_fixtures_reach_and_hold_their_bounds",
);

/// Gate 5's bound-table release and reuse: in each limit fixture's log the
/// waiting party gets its first unit only after a release, with fewer than
/// the limit open. The departed strikes and cooldowns take more drops in
/// one cooldown than they hold, then fall as entries end and rise again.
pub const BOUND_REUSE_TEST: TestRef = sim_test(
    "tests/header_outstanding.rs",
    "the_limit_fixtures_release_and_reuse_their_bounds",
);

/// Gate 5's bound-table membership and cleanup witness: every membership
/// or cleanup row held keys while the rule judged it in a committed
/// witness that holds the property.
pub const BOUND_MEMBERSHIP_TEST: TestRef = sim_test(
    "tests/header_outstanding.rs",
    "each_membership_row_holds_keys_in_a_committed_witness",
);

/// Gate 5's capacity-wait cleanup witness: two refusals, and the publisher
/// keeps one wait at a time, so the pruning drops the stale one.
pub const BOUND_WAITS_TEST: TestRef = sim_test(
    "tests/header_outstanding.rs",
    "the_capacity_waits_fixture_refuses_twice_and_holds",
);

/// Who owns a bound-table row's missing evidence.
pub const BOUND_OWNER: &str = "hdr-9d";

/// A bound-table row's obligation `name`, met by `test`'s pass.
const fn bound_test(name: &'static str, test: TestRef) -> GateObligation {
    GateObligation {
        name,
        gates: &[Gate::G5],
        permit: Permit::Test(test),
    }
}

/// Adam's approval of the [`bound_unshown`] rows' standing exclusions,
/// with their fixtures owed by a follow-up PR.
pub const BOUND_UNSHOWN_RULING: Approval = Approval::Adam {
    record: "Adam, 2026-11-12, gate-5 bound-table rows with no fixture evidence, fixtures in a follow-up",
};

/// A bound-table row's obligation `name` with no fixture evidence, met
/// by its standing exclusion for `reason` under [`BOUND_UNSHOWN_RULING`].
const fn bound_unshown(name: &'static str, reason: &'static str) -> GateObligation {
    GateObligation {
        name,
        gates: &[Gate::G5],
        permit: Permit::Standing {
            owner: BOUND_OWNER,
            reason,
            approval: BOUND_UNSHOWN_RULING,
        },
    }
}

/// The bound-table rows with no fixture evidence, each with the reason its
/// exclusion would carry. `pub` so the manifest's rows write the same
/// standing `Open`s, under [`BOUND_UNSHOWN_RULING`].
pub mod bound_unshown {
    /// The ordinary per-identity reservation.
    pub const RESERVATION: &str = "no fixture reaches the 250-header per-identity \
        reservation: the limit fixtures' mirrors advertise at most 235 headers per response";
    /// The per-peer locator bound.
    pub const LOCATOR_PER_PEER: &str = "no fixture asks for a second locator read for one \
        peer while its first is in flight, so the one-per-peer bound is never at its limit";
    /// One prepare and one apply per round.
    pub const CALLS_PER_ROUND: &str = "no fixture offers a round a second prepare or apply \
        while its first is pending, so the one-per-round bound is never at its limit";
    /// Detached `RetainedHeaders`.
    pub const DETACHED: &str = "detached RetainedHeaders count inside the chunk budget, which \
        chunk-budget-bound judges, and no fixture holds one past its round";
    /// Pending closes.
    pub const PENDING_CLOSES: &str = "no committed storage fixture fills a session's queue so a \
        Close is refused and waits to retry, so the map stays empty";
    /// Status cadences.
    pub const CADENCES: &str = "status cadences are listed, not checked: the storage harness \
        has no transport, so no connection close removes an entry, and the removal is the \
        transport cleanup's contract (RegisteredPeerCleanupGuard)";
    /// Publisher tombstones.
    pub const TOMBSTONES: &str = "no fixture keeps two ended subscriptions' tombstones on one \
        session at once, so the 2-tombstone bound is never at its limit";
    /// Publisher jobs.
    pub const JOBS: &str = "no fixture asks a session's publisher for a second page job while \
        one runs, so the one-job bound is never at its limit";
    /// Publisher subscription slots.
    pub const SUBSCRIPTION_SLOTS: &str = "no fixture opens a second subscription while the \
        first's terminal write finishes, so the 2-slot margin is never used";
    /// The sent-cursor ring.
    pub const SENT_RING: &str = "the sent-cursor ring is structurally unreachable: every page \
        spends at least one header of credit, and a subscription holds at most 4,000 headers \
        of credit past its acknowledged cursor, so reserve_page's credit check refuses before \
        the 4,096-entry ring fills";
    /// Node serving output.
    pub const NODE_OUTPUT: &str = "the node's output grants are listed, not checked: their \
        limit follows the configured session count through the toolkit's sizing";
    /// Per-peer serving output.
    pub const PEER_OUTPUT: &str = "no fixture queues anywhere near 64 MiB of one peer's output, \
        so the per-peer output bound never nears its limit";
    /// Serving response counts.
    pub const RESPONSE_COUNTS: &str = "response counts are listed, not checked: their limits \
        follow the configured session count through the toolkit's sizing";
    /// Retained-path leases.
    pub const LEASES: &str = "the 8 node execution slots bound concurrent serving leases first, \
        so no fixture reaches storage's 16 retained-path leases";
    /// Pruned peers.
    pub const PRUNED_PEERS: &str = "pruned peers are capped at the admitted peer limit \
        (#2209), but no committed storage fixture prunes a peer's history, so the cap is never \
        exercised";
    /// VCT repair rows.
    pub const VCT_REPAIR: &str = "no committed storage fixture starts a VCT repair, so its \
        sources, episodes and context reads stay empty; the repair harness's witnesses judge \
        stale completion and outstanding work, not these rows' limits";
}

/// Why the repair reservation has no row result: the bound table marks it
/// not judged, since the unclaimed chunk budget already bounds it, and no
/// repair round is reachable in these fixtures.
pub const BOUND_REPAIR_RESERVATION: &str = "the repair reservation is the unclaimed chunk \
    budget, which chunk-budget-bound already bounds, so the bound table doesn't judge it, and \
    no repair round is reachable in these fixtures";

/// Every gate obligation no row owns, with its gates and what may satisfy
/// it, written from the spec's gate table.
pub static GATE_OBLIGATIONS: &[GateObligation] = &[
    GateObligation {
        name: "gate-1/a-a-and-a-b-a",
        gates: &[Gate::G1],
        permit: Permit::Test(sim_test(
            "tests/header_acceptance_tool.rs",
            "each_history_is_the_same_alone_and_after_another",
        )),
    },
    GateObligation {
        name: "gate-1/topology-freshness",
        gates: &[Gate::G1],
        permit: Permit::Test(TOPOLOGY_FRESHNESS_TEST),
    },
    GateObligation {
        name: "gate-1/store-inventory/registered",
        gates: &[Gate::G1],
        permit: Permit::Test(STORE_INVENTORY_REGISTERED_TEST),
    },
    GateObligation {
        name: "gate-1/store-inventory/checkpoint-and-producer",
        gates: &[Gate::G1],
        permit: Permit::Test(STORE_INVENTORY_PRODUCER_TEST),
    },
    GateObligation {
        name: "gate-1/replay-store-control",
        gates: &[Gate::G1],
        permit: Permit::Test(sim_test(
            "tests/header_acceptance_tool.rs",
            "a_changed_stored_fact_fails_the_replay_comparison",
        )),
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
        name: "gate-2/end-boundary-preservation/registry",
        gates: &[Gate::G2],
        permit: Permit::Test(END_BOUNDARY_REGISTRY_TEST),
    },
    GateObligation {
        name: "gate-2/end-boundary-preservation/fault-store",
        gates: &[Gate::G2],
        permit: Permit::Test(END_BOUNDARY_FAULT_STORE_TEST),
    },
    GateObligation {
        name: "gate-3/crossing/unretained-open-then-outcome",
        gates: &[Gate::G3],
        permit: Permit::Test(sim_test(
            "tests/header_wire_crossing.rs",
            "an_unretained_open_then_outcome_crossing_is_conformant_and_unpunished",
        )),
    },
    GateObligation {
        name: "gate-3/crossing/crossing-close",
        gates: &[Gate::G3],
        permit: Permit::Test(sim_test(
            "tests/header_wire_crossing.rs",
            "a_crossing_close_is_conformant_and_unpunished",
        )),
    },
    GateObligation {
        name: "gate-3/crossing/over-window-grant",
        gates: &[Gate::G3],
        permit: Permit::Test(sim_test(
            "tests/header_wire_crossing.rs",
            "an_over_window_crossing_grant_is_bad_cursor_and_closed",
        )),
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
    // `outstanding-work-bounded`'s bound table (hdr-9d.1), one obligation
    // per row and kind of evidence: an admission row's at-limit and
    // one-over-denied fixture and its release-then-reuse fixture, a
    // membership, cleanup or conservation row's witness, or the exclusion
    // a row with neither awaits.
    bound_test("gate-5/bound-table/work-slots/at-limit", BOUND_LIMIT_TEST),
    bound_test(
        "gate-5/bound-table/work-slots/release-reuse",
        BOUND_REUSE_TEST,
    ),
    bound_test(
        "gate-5/bound-table/reservation-maps/at-limit",
        BOUND_LIMIT_TEST,
    ),
    bound_test(
        "gate-5/bound-table/reservation-maps/release-reuse",
        BOUND_REUSE_TEST,
    ),
    bound_test(
        "gate-5/bound-table/request-deadlines/at-limit",
        BOUND_LIMIT_TEST,
    ),
    bound_test(
        "gate-5/bound-table/request-deadlines/membership",
        BOUND_MEMBERSHIP_TEST,
    ),
    bound_test(
        "gate-5/bound-table/locator-reads/at-limit",
        BOUND_LIMIT_TEST,
    ),
    bound_test(
        "gate-5/bound-table/locator-reads/release-reuse",
        BOUND_REUSE_TEST,
    ),
    bound_test(
        "gate-5/bound-table/locator-reads/membership",
        BOUND_MEMBERSHIP_TEST,
    ),
    bound_test(
        "gate-5/bound-table/node-execution-slots/at-limit",
        BOUND_LIMIT_TEST,
    ),
    bound_test(
        "gate-5/bound-table/node-execution-slots/release-reuse",
        BOUND_REUSE_TEST,
    ),
    bound_test(
        "gate-5/bound-table/peer-execution-slots/at-limit",
        BOUND_LIMIT_TEST,
    ),
    bound_test(
        "gate-5/bound-table/peer-execution-slots/release-reuse",
        BOUND_REUSE_TEST,
    ),
    bound_test(
        "gate-5/bound-table/departed-strikes/at-limit",
        BOUND_LIMIT_TEST,
    ),
    bound_test(
        "gate-5/bound-table/departed-strikes/release-reuse",
        BOUND_REUSE_TEST,
    ),
    bound_test(
        "gate-5/bound-table/readmission-cooldowns/at-limit",
        BOUND_LIMIT_TEST,
    ),
    bound_test(
        "gate-5/bound-table/readmission-cooldowns/release-reuse",
        BOUND_REUSE_TEST,
    ),
    bound_test(
        "gate-5/bound-table/publisher-capacity-waits/cleanup",
        BOUND_WAITS_TEST,
    ),
    bound_test(
        "gate-5/bound-table/close-deadlines/membership",
        BOUND_MEMBERSHIP_TEST,
    ),
    bound_test(
        "gate-5/bound-table/storage-calls/membership",
        BOUND_MEMBERSHIP_TEST,
    ),
    bound_test(
        "gate-5/bound-table/port-futures/conservation",
        BOUND_MEMBERSHIP_TEST,
    ),
    bound_test(
        "gate-5/bound-table/cached-offers/membership",
        BOUND_MEMBERSHIP_TEST,
    ),
    bound_test(
        "gate-5/bound-table/supplier-order/membership",
        BOUND_MEMBERSHIP_TEST,
    ),
    bound_test(
        "gate-5/bound-table/completed-targets/membership",
        BOUND_MEMBERSHIP_TEST,
    ),
    bound_test(
        "gate-5/bound-table/subscriber-cursors/membership",
        BOUND_MEMBERSHIP_TEST,
    ),
    bound_test(
        "gate-5/bound-table/subscriber-subscriptions/membership",
        BOUND_MEMBERSHIP_TEST,
    ),
    bound_test(
        "gate-5/bound-table/publisher-tasks/cleanup",
        BOUND_MEMBERSHIP_TEST,
    ),
    bound_test(
        "gate-5/bound-table/sessions/membership",
        BOUND_MEMBERSHIP_TEST,
    ),
    bound_test(
        "gate-5/bound-table/serving-peer-budgets/cleanup",
        BOUND_MEMBERSHIP_TEST,
    ),
    GateObligation {
        name: "gate-5/bound-table/repair-reservation",
        gates: &[Gate::G5],
        permit: Permit::Standing {
            owner: BOUND_OWNER,
            reason: BOUND_REPAIR_RESERVATION,
            approval: PLAN,
        },
    },
    bound_unshown("gate-5/bound-table/reservation", bound_unshown::RESERVATION),
    bound_unshown(
        "gate-5/bound-table/locator-reads/per-peer",
        bound_unshown::LOCATOR_PER_PEER,
    ),
    bound_unshown(
        "gate-5/bound-table/calls-per-round",
        bound_unshown::CALLS_PER_ROUND,
    ),
    bound_unshown(
        "gate-5/bound-table/detached-headers",
        bound_unshown::DETACHED,
    ),
    bound_unshown(
        "gate-5/bound-table/pending-closes",
        bound_unshown::PENDING_CLOSES,
    ),
    bound_unshown(
        "gate-5/bound-table/status-cadences",
        bound_unshown::CADENCES,
    ),
    bound_unshown(
        "gate-5/bound-table/publisher-tombstones",
        bound_unshown::TOMBSTONES,
    ),
    bound_unshown("gate-5/bound-table/publisher-jobs", bound_unshown::JOBS),
    bound_unshown(
        "gate-5/bound-table/subscription-slots",
        bound_unshown::SUBSCRIPTION_SLOTS,
    ),
    bound_unshown(
        "gate-5/bound-table/sent-cursor-ring",
        bound_unshown::SENT_RING,
    ),
    bound_unshown("gate-5/bound-table/node-output", bound_unshown::NODE_OUTPUT),
    bound_unshown("gate-5/bound-table/peer-output", bound_unshown::PEER_OUTPUT),
    bound_unshown(
        "gate-5/bound-table/response-counts",
        bound_unshown::RESPONSE_COUNTS,
    ),
    bound_unshown(
        "gate-5/bound-table/retained-path-leases",
        bound_unshown::LEASES,
    ),
    bound_unshown(
        "gate-5/bound-table/pruned-peers",
        bound_unshown::PRUNED_PEERS,
    ),
    bound_unshown("gate-5/bound-table/vct-repair", bound_unshown::VCT_REPAIR),
    // The one storage history running all four Sim resource IDs together,
    // with its Shutdown and StopStorage picked and the node settled at its
    // reactor's exit. Its four whole-report passes are history keys.
    GateObligation {
        name: "gate-5/mixed-witness",
        gates: &[Gate::G5],
        permit: Permit::Test(sim_test(
            "tests/header_mixed_resources.rs",
            "the_mixed_fixture_holds_every_resource_rule",
        )),
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
        // Its patched run ends at a harness failure, so only the baseline
        // owes a cleanup comparison.
        Expected::HarnessCaught { case, .. } => (
            vec![
                RunId::case(case, CaseRun::Baseline),
                RunId::case(case, CaseRun::Patched),
            ],
            vec![RunId::case(case, CaseRun::Baseline)],
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
/// a test key, each such contract's proving catch a catch key, each
/// requirement and composition check its key, and each
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
                for &case in contract.proving_catches {
                    keys.insert(contract_catch_key(id, case), BTreeSet::new());
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
    // cleanup comparison as well, from the planted inventory itself. A
    // harness-caught case has no `Draft` run, so only its baseline owes a
    // cleanup. Only whether a node case's patched run completes, so owes a
    // cleanup too, comes from its row's `found`, below.
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
        if inv
            .harness_caught
            .contains(&(property.clone(), name.clone()))
        {
            keys.insert(
                Key::Cleanup {
                    run: case(CaseRun::Baseline),
                },
                BTreeSet::from([Gate::G1]),
            );
        } else if !machine {
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
