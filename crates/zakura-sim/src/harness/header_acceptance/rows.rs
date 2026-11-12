//! The manifest's rows: one per header ID on `main`, the gate requirements
//! no ID owns alone, the native mutations' classification, and the
//! committed constants the static checks compare against.
//!
//! Each row is written by hand from the plan and the code, not from the
//! registry or [`COVERAGE`](crate::harness::coverage::COVERAGE), so the
//! static checks in `tests/header_acceptance.rs` compare two independent
//! lists.

use super::required as obligation;
use super::{
    sim_test, Approval, CaseRef, Expected, ExpectedNa, ExpectedResult, Found, Gate, HistoryRef,
    Key, Promotion, Row, RunId, TestRef, Tier, PLAN,
};
use crate::property::Status;

/// `header-sync/smoke`.
const SMOKE: &str = "header-sync/smoke";
/// `header-sync/storage`.
const STORAGE: &str = "header-sync/storage";
/// `header-sync/release-wake`.
const RELEASE_WAKE: &str = "header-sync/release-wake";
/// `header-sync/wire`.
const WIRE: &str = "header-sync/wire";
/// `header-sync/flood`.
const FLOOD: &str = "header-sync/flood";
/// `header-sync/repair`.
const REPAIR: &str = "header-sync/repair";
/// `header-sync/liveness`.
const LIVENESS: &str = "header-sync/liveness";
/// The header reactor machine.
const REACTOR: &str = "header/reactor-policy";
/// The header publisher machine.
const PUBLISHER: &str = "header/publisher-policy";

/// Every harness this manifest covers: the header node harnesses and the
/// two header machines.
pub const HEADER_HARNESSES: &[&str] = &[
    SMOKE,
    STORAGE,
    RELEASE_WAKE,
    WIRE,
    FLOOD,
    REPAIR,
    LIVENESS,
    REACTOR,
    PUBLISHER,
];

/// `header-sync/storage-operation-ids`.
const OPERATION_IDS: &str = "header-sync/storage-operation-ids";
/// `harness/direct-dispatch`.
const DIRECT_DISPATCH: &str = "harness/direct-dispatch";
/// `header-sync/chunk-budget-bound`.
const CHUNK: &str = "header-sync/chunk-budget-bound";
/// `header-sync/teardown-returns-baseline`.
const TEARDOWN: &str = "header-sync/teardown-returns-baseline";
/// `header-sync/stale-completion-inert`.
const STALE: &str = "header-sync/stale-completion-inert";
/// `header-sync/capacity-release-wakes`.
const WAKES: &str = "header-sync/capacity-release-wakes";
/// `header-sync/reset-invalidates-work`.
const RESET: &str = "header-sync/reset-invalidates-work";
/// `header-sync/stale-chain-effect-inert`.
const CHAIN: &str = "header-sync/stale-chain-effect-inert";
/// `header-sync/commit-only-authorized`.
const COMMIT: &str = "header-sync/commit-only-authorized";
/// `header-sync/outstanding-work-bounded`.
const OUTSTANDING: &str = "header-sync/outstanding-work-bounded";
/// `header-sync/hold-release-wakes`.
const HOLD: &str = "header-sync/hold-release-wakes";
/// `header-sync/peer-retirement-progress`.
const RETIREMENT: &str = "header-sync/peer-retirement-progress";
/// `header-sync/expected-chain-selected`.
const SELECTED: &str = "header-sync/expected-chain-selected";
/// `header-sync/state-publication-consistent`.
const PUBLICATION: &str = "header-sync/state-publication-consistent";
/// `header-sync/honest-progress`.
const PROGRESS_ID: &str = "header-sync/honest-progress";
/// `header-sync/bounded-adversary-progress`.
const RECOVERY_ID: &str = "header-sync/bounded-adversary-progress";
/// `header-sync/watch-observed-under-flood`.
const WATCH_WIRE: &str = "header-sync/watch-observed-under-flood";
/// `header-sync/watch-observed-under-lifecycle-flood`.
const WATCH_LIFECYCLE: &str = "header-sync/watch-observed-under-lifecycle-flood";

/// The step loop's two properties, which every node harness registers.
const SIM_WIDE: [&str; 2] = ["sim/no-task-panic", "sim/no-livelock"];
/// `header-sync/header-fault-penalized`.
const PENALIZED: &str = "header-sync/header-fault-penalized";
/// `session/honest-never-punished`.
const HONEST: &str = "session/honest-never-punished";
/// `header-sync/s5v9/conformant-never-closes`, the broader claim
/// [`NESTS`] lists over [`HONEST`].
const CONFORMANT: &str = "header-sync/s5v9/conformant-never-closes";

/// What the storage harness registers, which the release-wake, flood and
/// repair harnesses register too, and the wire harness all but its last,
/// `outstanding-work-bounded`.
const STORAGE_REGISTERS: [&str; 14] = [
    SIM_WIDE[0],
    SIM_WIDE[1],
    OPERATION_IDS,
    DIRECT_DISPATCH,
    CHUNK,
    TEARDOWN,
    STALE,
    RESET,
    CHAIN,
    COMMIT,
    RETIREMENT,
    SELECTED,
    PUBLICATION,
    OUTSTANDING,
];

/// Every `(harness, ID, status)` the header harnesses must register, written
/// apart from the registry. `tests/header_acceptance.rs` requires the
/// registrations to equal it exactly, so a harness that stops registering
/// an ID fails even while another harness still registers it.
pub fn harness_matrix() -> Vec<(&'static str, &'static str, Status)> {
    let mut matrix = Vec::new();
    let mut add = |harness: &'static str, ids: &[&'static str]| {
        matrix.extend(ids.iter().map(|id| (harness, *id, Status::Active)));
    };
    add(SMOKE, &SIM_WIDE);
    add(STORAGE, &STORAGE_REGISTERS);
    add(RELEASE_WAKE, &STORAGE_REGISTERS);
    add(RELEASE_WAKE, &[WAKES]);
    add(WIRE, &STORAGE_REGISTERS[..STORAGE_REGISTERS.len() - 1]);
    add(WIRE, &[PENALIZED, HONEST]);
    add(WIRE, generated_ids());
    add(FLOOD, &STORAGE_REGISTERS);
    add(FLOOD, &[WATCH_WIRE, WATCH_LIFECYCLE]);
    add(REPAIR, &STORAGE_REGISTERS);
    add(LIVENESS, &STORAGE_REGISTERS);
    add(LIVENESS, &[HOLD]);
    add(REACTOR, &[REACTOR]);
    add(PUBLISHER, &[PUBLISHER]);
    matrix.push((WIRE, PROGRESS_ID, Status::Draft));
    matrix.push((STORAGE, RECOVERY_ID, Status::Active));
    matrix.push((LIVENESS, RECOVERY_ID, Status::Active));
    matrix
}

/// The file of `header-sync/wire`'s generated witnesses.
const WIRE_WITNESS: &str = "tests/header_wire_witness.rs";

/// The file of the despite-close case's companion tests.
const DESPITE_CLOSE_TESTS: &str = "tests/header_despite_close.rs";
/// The acceptance tool's integration tests: gate 1's A/A and A/B/A
/// comparison and its replay store control.
const TOOL_TESTS: &str = "tests/header_acceptance_tool.rs";

/// The file of `header-sync/wire`'s receiver-direction table checks, whose
/// per-row content tests carry the fault close a content rejection owes.
const TABLE_TESTS: &str = "tests/header_wire_table.rs";

/// The file of gate 3's crossing histories on `header-sync/wire`, each a
/// `subscribe-headers` update that crosses the node's ending.
const CROSSING_TESTS: &str = "tests/header_wire_crossing.rs";

/// One generated `s5v9` ID's row data: its suffix, the witness test that
/// names its passing history, the gates it serves, and its catch.
struct Generated {
    suffix: &'static str,
    witness: &'static str,
    gates: &'static [Gate],
    catch: GeneratedCatch,
}

/// How a generated ID's catch is settled (decision 1): by its planted case
/// under `qa/planted/header-sync/s5v9/`, or by an `Open` saying why no
/// production patch breaks the ID alone.
#[derive(Clone, Copy)]
enum GeneratedCatch {
    /// The case's name. Its set is the ID alone, and it's found at a step.
    Case(&'static str),
    /// [`Case`](GeneratedCatch::Case)'s case, and a second case of the ID,
    /// caught the same way, that isn't its catch but proves something
    /// beside it, such as a contract's.
    Cases(&'static str, &'static str),
    /// The `Open`'s reason and its approval.
    Open(&'static str, Approval),
}

/// The honest witness, and the check that the witnesses list every ID.
const EVERY: &str = "every_generated_property_applies_in_its_witness";
/// The `undeclared-type` and `status` fault witnesses.
const STATUS: &str = "status_faults_apply_in_their_witnesses";
/// The `subscribe-headers` fault witnesses.
const SUBSCRIBE: &str = "subscribe_headers_faults_apply_in_their_witnesses";
/// The `headers` fault witnesses.
const HEADERS: &str = "headers_faults_apply_in_their_witnesses";
/// The `headers-outcome` fault witnesses.
const OUTCOME: &str = "headers_outcome_faults_apply_in_their_witnesses";

/// Gate 2 alone.
const G2: &[Gate] = &[Gate::G2];
/// Gates 2 and 3: a subscription close.
const G23: &[Gate] = &[Gate::G2, Gate::G3];
/// Gates 2 and 4: cadence exhaustion.
const G24: &[Gate] = &[Gate::G2, Gate::G4];
/// Gate 3 alone: a sender rule.
const G3: &[Gate] = &[Gate::G3];
/// Gates 3 and 4: sender cadence.
const G34: &[Gate] = &[Gate::G3, Gate::G4];
/// Gates 1, 3 and 4.
const G134: &[Gate] = &[Gate::G1, Gate::G3, Gate::G4];
/// Gate 5 alone.
const G5: &[Gate] = &[Gate::G5];
/// Gate 1 alone.
const G1: &[Gate] = &[Gate::G1];
/// No gate: counted only by the run's overall acceptance.
const NONE: &[Gate] = &[];

/// Header sync's 32 generated IDs on `main`.
const GENERATED: [Generated; 32] = {
    use GeneratedCatch::{Case, Cases, Open};
    [
        gen(
            "conformant-never-closes",
            EVERY,
            G2,
            Case("target-page-refused"),
        ),
        gen(
            "status/sender-cadence",
            EVERY,
            G34,
            Case("pacing-floors-cut"),
        ),
        gen(
            "subscribe-headers/sender-within-live",
            EVERY,
            G3,
            Case("open-repeated"),
        ),
        gen(
            "subscribe-headers/sender-within-credit",
            EVERY,
            G3,
            Case("page-past-credit"),
        ),
        gen(
            "headers/sender-answers-open",
            EVERY,
            G3,
            Case("page-misnamed"),
        ),
        gen(
            "headers-outcome/sender-ends-once",
            EVERY,
            G3,
            Case("outcome-misnamed"),
        ),
        gen(
            "undeclared-type-closes",
            STATUS,
            G2,
            Case("undeclared-type-skipped"),
        ),
        gen(
            "status/reserved-flags-closes",
            STATUS,
            G2,
            // The second is C21's proof for the generated receiver checks.
            Cases("reserved-flags-skipped", "two-reserved-flags-skipped"),
        ),
        gen(
            "status/short-payload-closes",
            STATUS,
            G2,
            Case("short-payload-skipped"),
        ),
        gen(
            "status/long-payload-closes",
            STATUS,
            G2,
            Case("oversize-skipped"),
        ),
        gen(
            "status/bad-content-closes",
            STATUS,
            G2,
            Case("work-anchor-unchecked"),
        ),
        gen(
            "status/cadence-never-closes",
            STATUS,
            G24,
            Case("exhausted-cadence-throttled"),
        ),
        gen(
            "subscribe-headers/reserved-flags-closes",
            SUBSCRIBE,
            G2,
            Case("reserved-flags-skipped"),
        ),
        gen(
            "subscribe-headers/short-payload-closes",
            SUBSCRIBE,
            G2,
            Case("short-payload-skipped"),
        ),
        gen(
            "subscribe-headers/long-payload-closes",
            SUBSCRIBE,
            G2,
            Case("oversize-skipped"),
        ),
        gen(
            "subscribe-headers/over-live-closes",
            SUBSCRIBE,
            G23,
            Case("no-slot-dropped"),
        ),
        gen(
            "subscribe-headers/over-credit-closes",
            SUBSCRIBE,
            G23,
            Case("above-reservation-skipped"),
        ),
        gen(
            "subscribe-headers/after-close-closes",
            SUBSCRIBE,
            G23,
            Case("after-close-dropped"),
        ),
        gen(
            "subscribe-headers/bad-cursor-closes",
            SUBSCRIBE,
            G23,
            Case("unknown-acknowledgement-dropped"),
        ),
        gen(
            "subscribe-headers/bad-content-closes",
            SUBSCRIBE,
            G2,
            Case("repeated-locator-unchecked"),
        ),
        gen(
            "headers/reserved-flags-closes",
            HEADERS,
            G2,
            Case("reserved-flags-skipped"),
        ),
        gen(
            "headers/short-payload-closes",
            HEADERS,
            G2,
            Case("short-payload-skipped"),
        ),
        gen(
            "headers/long-payload-closes",
            HEADERS,
            G2,
            Open(LONG_PAGE_UNREACHED, HDR_14_5_RULING),
        ),
        gen(
            "headers/unsolicited-closes",
            HEADERS,
            G2,
            Case("unsolicited-skipped"),
        ),
        gen(
            "headers/bad-content-closes",
            HEADERS,
            G2,
            Case("unlinked-page-dropped"),
        ),
        gen(
            "headers/over-owed-closes",
            HEADERS,
            G23,
            Open(NOT_OWED, Approval::Pending),
        ),
        gen(
            "headers-outcome/reserved-flags-closes",
            OUTCOME,
            G2,
            Case("reserved-flags-skipped"),
        ),
        gen(
            "headers-outcome/short-payload-closes",
            OUTCOME,
            G2,
            Case("short-payload-skipped"),
        ),
        gen(
            "headers-outcome/long-payload-closes",
            OUTCOME,
            G2,
            Case("oversize-skipped"),
        ),
        gen(
            "headers-outcome/unsolicited-closes",
            OUTCOME,
            G2,
            Case("unsolicited-skipped"),
        ),
        gen(
            "headers-outcome/bad-content-closes",
            OUTCOME,
            G2,
            Case("outcome-window-unchecked"),
        ),
        gen(
            "headers-outcome/over-owed-closes",
            OUTCOME,
            G23,
            Open(NOT_OWED, Approval::Pending),
        ),
    ]
};

/// A [`Generated`] entry.
const fn gen(
    suffix: &'static str,
    witness: &'static str,
    gates: &'static [Gate],
    catch: GeneratedCatch,
) -> Generated {
    Generated {
        suffix,
        witness,
        gates,
        catch,
    }
}

/// Adam's hdr-14.5 ruling on the generated IDs' open catches.
const HDR_14_5_RULING: Approval = Approval::Adam {
    record: "Adam, 2026-11-12, hdr-14.5 ruling",
};

/// Why `headers/long-payload-closes` has no catch: no patch to one path
/// breaks it alone.
const LONG_PAGE_UNREACHED: &str =
    "on the simulator's Regtest headers the node grants a subscription at most 4,000 smallest \
     pages of byte credit, about 1 MiB, which is less than the headers row's 2 MiB maximum, so a \
     page past that maximum is past its credit too, as the act's two labels say, and the \
     subscriber's precheck refuses it before the frame filter's cap does; a patch that has the \
     reader skip both refusals drains the page at 0 ns and breaks sim/no-livelock first";

/// The generated IDs no header-sync scenario reaches, and why.
const NOT_OWED: &str = "header sync's responses answer a subscription, whose pages spend credit, \
                        so the models label an overrun over-credit, never over-owed";

/// The honest two-way history of the generated witnesses.
const BOTH_WAYS: HistoryRef = HistoryRef {
    harness: WIRE,
    witness: sim_test(WIRE_WITNESS, EVERY),
    case: "both-ways",
};

/// The history of `witness` named `case` on the wire harness.
const fn wire_history(witness: &'static str, case: &'static str) -> HistoryRef {
    HistoryRef {
        harness: WIRE,
        witness: sim_test(WIRE_WITNESS, witness),
        case,
    }
}

/// The conformant-control witness.
const CONTROL: &str = "each_invalid_label_witness_has_a_conformant_control";
/// Its six control histories, each a row's conformant boundary act.
const CONTROL_CASES: [&str; 6] = [
    "status/min-payload",
    "subscribe-headers/min-payload",
    "subscribe-headers/second-open",
    "headers/min-payload",
    "headers/max-payload",
    "headers-outcome/min-payload",
];

/// The committed close-relink input on the storage harness, as the capacity
/// witness runs it.
const CLOSE_RELINK_CAPACITY: HistoryRef = HistoryRef {
    harness: STORAGE,
    witness: sim_test(
        "tests/header_node_capacity.rs",
        "the_close_relink_input_holds_chunk_budget_bound",
    ),
    case: "committed close-relink input",
};

/// The first of the storage witness's 32 plans on the default scenario,
/// which `COVERAGE` names for the storage harness's operation and dispatch
/// properties.
const NODE_SYNC: HistoryRef = HistoryRef {
    harness: STORAGE,
    witness: sim_test(
        "tests/header_node.rs",
        "node_syncs_from_a_mirror_on_controlled_storage",
    ),
    case: "plan 0 of inputs(32, 256)",
};

/// The committed close-relink input, as the teardown witness runs it.
const CLOSE_RELINK_TEARDOWN: HistoryRef = HistoryRef {
    harness: STORAGE,
    witness: sim_test(
        "tests/header_teardown.rs",
        "the_close_relink_input_holds_teardown_returns_baseline",
    ),
    case: "committed close-relink input",
};

/// The committed stale-prepare input.
const STALE_PREPARE: HistoryRef = HistoryRef {
    harness: STORAGE,
    witness: sim_test(
        "tests/header_stale.rs",
        "the_stale_prepare_input_holds_stale_completion_inert",
    ),
    case: "committed stale-prepare input",
};

/// The mixed resource witness's test (gate 5), which runs its committed
/// fixture once.
const MIXED_TEST: TestRef = sim_test(
    "tests/header_mixed_resources.rs",
    "the_mixed_fixture_holds_every_resource_rule",
);

/// Gate 5's mixed resource witness: the one storage history where close and
/// relink churn, a reset, a reply hold, a held prepare, a `Shutdown` and a
/// `StopStorage` run together. It's counted once per resource rule in
/// [`MIXED_TARGETS`], each with its own whole-report pass, and its test's
/// own pass is `gate-5/mixed-witness`, which also requires the control
/// picks in order and the node settled at its reactor's exit. Like every
/// `Holds`, these count only in the frozen-head run's record, which runs on
/// `main` once hdr-14 has merged.
const MIXED_RESOURCES: HistoryRef = HistoryRef {
    harness: STORAGE,
    witness: MIXED_TEST,
    case: "header-resources-mixed",
};

/// The four `Sim` resource rules [`MIXED_RESOURCES`] judges.
const MIXED_TARGETS: [&str; 4] = [CHUNK, TEARDOWN, STALE, OUTSTANDING];

/// What stays `NotApplicable` on [`MIXED_RESOURCES`], each with the reason
/// its report carries: no storage scenario declares a livelock assumption
/// or a recovery. Every other ID the storage harness registers must hold.
const MIXED_NA: &[ExpectedNa] = &[
    ExpectedNa {
        property: SIM_WIDE[1],
        reason: "undeclared",
    },
    ExpectedNa {
        property: RECOVERY_ID,
        reason: "undeclared",
    },
];

/// `property`'s `Holds` on [`MIXED_RESOURCES`]. It takes its key, the
/// history's pass for `property`, from [`attach_history_passes`].
fn mixed(property: &'static str) -> ExpectedResult {
    result(
        Vec::new(),
        G5,
        Expected::Holds {
            property,
            history: MIXED_RESOURCES,
            not_applicable: MIXED_NA,
        },
    )
}

/// Each reset witness's name, as its witness test prefixes its executions.
const RESET_CASES: [&str; 10] = [
    "held-locator ",
    "held-prepare ",
    "held-apply ",
    "held-reply ",
    "two-forks ",
    "session-replaced-read ",
    "tip-reuse ",
    "completion-before-adoption ",
    "quiet-reset ",
    "quiet-reset-deep ",
];
/// Each stale-chain witness's name, as its witness test prefixes its executions.
const CHAIN_CASES: [&str; 4] = [
    "pre-reset-commit ",
    "refused-across-reset ",
    "fork-stale-apply ",
    "reset-inside-status-floor ",
];
/// Each authority witness's name, as its witness test prefixes its executions.
const AUTHORITY_CASES: [&str; 4] = [
    "forks-sharing-a-header ",
    "multi-page-sync ",
    "relinked-mid-round ",
    "harness-commit ",
];
/// Each fork-choice witness's name, as its witness test prefixes its
/// executions.
const SELECTED_CASES: [&str; 7] = [
    "stronger-first ",
    "stronger-last ",
    "three-forks ",
    "equal-work ",
    "heavier-offered-to-longer ",
    "invalidated-then-reconsidered ",
    "harness-commit ",
];
/// Each publication witness's name, as its witness test prefixes its
/// executions.
const PUBLICATION_CASES: [&str; 13] = [
    "startup-only ",
    "quiet-reset ",
    "invalidate-only ",
    "grow ",
    "reset-inside-status-floor ",
    "harness-commit ",
    "sync ",
    "served ",
    "served-one-header ",
    "served-from-a-lower-locator ",
    "served-in-pages ",
    "served-after-growth ",
    "served-across-a-reset ",
];
/// Each committed outstanding-work witness, by its fixture stem, which its
/// witness test runs under its committed Plan.
const OUTSTANDING_CASES: [&str; 10] = [
    "header-authority-forks-sharing-a-header",
    "header-authority-relinked-mid-round",
    "header-reset-held-apply",
    "header-reset-held-reply",
    "header-reset-quiet-reset",
    "header-reset-quiet-reset-deep",
    "header-outstanding-churn",
    "header-outstanding-serving-churn",
    "header-outstanding-serving-replaced",
    "header-outstanding-capacity-waits",
];

/// The committed reset witnesses, each holding and applying
/// `reset-invalidates-work`, the first one's first plan.
const RESET_WITNESSES: HistoryRef = HistoryRef {
    harness: STORAGE,
    witness: sim_test(
        "tests/header_reset_invalidates.rs",
        "every_witness_holds_and_applies",
    ),
    case: "held-locator plan 0 of inputs(8, 256)",
};

/// The committed stale-chain witnesses, each holding and applying
/// `stale-chain-effect-inert`.
const CHAIN_WITNESSES: HistoryRef = HistoryRef {
    harness: STORAGE,
    witness: sim_test(
        "tests/header_stale_chain.rs",
        "every_witness_holds_and_applies",
    ),
    case: "pre-reset-commit plan 0 of inputs(8, 256)",
};

/// The committed authority witnesses, each holding and applying
/// `commit-only-authorized`.
const AUTHORITY_WITNESSES: HistoryRef = HistoryRef {
    harness: STORAGE,
    witness: sim_test(
        "tests/header_authority.rs",
        "every_witness_holds_and_applies",
    ),
    case: "forks-sharing-a-header plan 0 of inputs(8, 256)",
};

/// The fork-choice witnesses, each holding and applying
/// `expected-chain-selected`.
const SELECTED_WITNESSES: HistoryRef = HistoryRef {
    harness: STORAGE,
    witness: sim_test(
        "tests/header_chain_selected.rs",
        "every_witness_holds_and_applies",
    ),
    case: "stronger-first plan 0 of inputs(8, 256)",
};

/// The publication witnesses, each holding and applying
/// `state-publication-consistent`.
const PUBLICATION_WITNESSES: HistoryRef = HistoryRef {
    harness: STORAGE,
    witness: sim_test(
        "tests/header_publication.rs",
        "every_witness_holds_and_applies",
    ),
    case: "startup-only plan 0 of inputs(8, 256)",
};

/// The committed outstanding-work witnesses, each holding and applying
/// `outstanding-work-bounded` under its committed Plan.
const OUTSTANDING_WITNESSES: HistoryRef = HistoryRef {
    harness: STORAGE,
    witness: sim_test(
        "tests/header_outstanding.rs",
        "the_committed_witnesses_hold_outstanding_work_bounded",
    ),
    case: "header-authority-forks-sharing-a-header",
};

/// Each committed repair input, as its witness test names its execution.
const REPAIR_OUTSTANDING_CASES: [&str; 4] = [
    "committed header-repair-applied input",
    "committed header-repair-prepare-departs input",
    "committed header-repair-apply-departs input",
    "committed header-repair-supplier-pruned input",
];

/// The committed repair inputs, each holding and applying
/// `outstanding-work-bounded` under its committed Plan.
const REPAIR_OUTSTANDING: HistoryRef = HistoryRef {
    harness: REPAIR,
    witness: sim_test(
        "tests/header_repair.rs",
        "the_committed_repair_inputs_hold_outstanding_work_bounded",
    ),
    case: "committed header-repair-applied input",
};

/// Each committed hold-release fixture, by its fixture stem, which its
/// witness test runs under the fixed Plan.
const HOLD_CASES: [&str; 4] = [
    "header-hold-held-locator",
    "header-hold-held-prepare",
    "header-hold-held-apply",
    "header-hold-serving-read",
];

/// The committed hold-release fixtures, each holding and applying
/// `hold-release-wakes` under the fixed Plan.
const HOLD_WITNESSES: HistoryRef = HistoryRef {
    harness: LIVENESS,
    witness: sim_test(
        "tests/header_hold_release.rs",
        "the_committed_fixtures_match_their_names",
    ),
    case: "header-hold-held-locator",
};

/// Each committed retirement fixture, by its fixture stem, which its
/// witness test runs under the fixed Plan.
const RETIRE_CASES: [&str; 8] = [
    "header-retire-timeout",
    "header-retire-late-page",
    "header-retire-awaiting-close",
    "header-retire-preparing-close",
    "header-retire-receiving-close",
    "header-retire-receiving-relink",
    "header-retire-preparing-relink",
    "header-retire-applying-relink",
];

/// The committed retirement fixtures, each holding and applying
/// `peer-retirement-progress` under the fixed Plan.
const RETIRE_WITNESSES: HistoryRef = HistoryRef {
    harness: LIVENESS,
    witness: sim_test(
        "tests/header_peer_retirement.rs",
        "the_committed_fixtures_match_their_names",
    ),
    case: "header-retire-timeout",
};

/// Each committed recovery fixture, by its fixture stem, which its witness
/// test runs under the fixed Plan: hdr-11's control and hdr-13's close,
/// fault and departure classes.
const RECOVERY_CASES: [&str; 4] = [
    "header-recovery-successor",
    "header-recovery-closes",
    "header-recovery-faults",
    "header-recovery-departure",
];

/// The committed recovery fixtures, each holding and applying
/// `bounded-adversary-progress` under the fixed Plan.
const RECOVERY_WITNESS: HistoryRef = HistoryRef {
    harness: LIVENESS,
    witness: sim_test(
        "tests/header_recovery_progress.rs",
        "the_committed_fixtures_match_their_names",
    ),
    case: "header-recovery-successor",
};

/// The flood witness test, which runs both committed flood inputs.
const FLOOD_WITNESS: TestRef = sim_test(
    "tests/header_flood_watch.rs",
    "the_committed_flood_inputs_hold_their_watch_rules",
);

/// The committed wire flood input, judged on the wire queue's rule.
const FLOOD_WIRE: HistoryRef = HistoryRef {
    harness: FLOOD,
    witness: FLOOD_WITNESS,
    case: "committed wire flood input",
};

/// The committed lifecycle flood input, judged on the lifecycle queue's
/// rule.
const FLOOD_LIFECYCLE: HistoryRef = HistoryRef {
    harness: FLOOD,
    witness: FLOOD_WITNESS,
    case: "committed lifecycle flood input",
};

/// The committed release-wake input.
const RELEASE_INPUT: HistoryRef = HistoryRef {
    harness: RELEASE_WAKE,
    witness: sim_test(
        "tests/header_release_wake.rs",
        "the_release_wake_input_holds_capacity_release_wakes",
    ),
    case: "committed release input",
};

/// The disconnect-mid-prepare history.
const MID_PREPARE: HistoryRef = HistoryRef {
    harness: WIRE,
    witness: sim_test(
        "tests/header_wire_prepare_fault.rs",
        "a_disconnect_mid_prepare_is_still_penalized",
    ),
    case: "steered plan 0",
};

/// The honest-progress fixture.
const PROGRESS: HistoryRef = HistoryRef {
    harness: WIRE,
    witness: sim_test(
        "tests/header_wire_progress.rs",
        "the_progress_fixture_holds_honest_progress",
    ),
    case: "committed plan",
};

/// The honest-peer witness.
const HONEST_PEER: HistoryRef = HistoryRef {
    harness: WIRE,
    witness: sim_test(WIRE_WITNESS, "honest_peer_never_punished"),
    case: "both ways",
};

/// The smoke harness's clean run.
const SMOKE_RUN: HistoryRef = HistoryRef {
    harness: SMOKE,
    witness: sim_test("tests/harness.rs", "a_clean_run_holds_no_task_panic"),
    case: "empty input",
};

/// The reactor machine's recorded history.
const REACTOR_RUN: HistoryRef = HistoryRef {
    harness: REACTOR,
    witness: sim_test(
        "tests/replay_exact.rs",
        "reactor_machine_records_and_replays_without_transport",
    ),
    case: "REACTOR_INPUT",
};

/// The publisher machine's recorded history.
const PUBLISHER_RUN: HistoryRef = HistoryRef {
    harness: PUBLISHER,
    witness: sim_test(
        "tests/replay_exact.rs",
        "publisher_machine_records_and_replays_without_transport",
    ),
    case: "PUBLISHER_INPUT",
};

/// A planted case under `property`.
const fn case(property: &'static str, name: &'static str) -> CaseRef {
    CaseRef { property, name }
}

/// An expected result covering `keys`, for `gates`.
fn result(keys: Vec<Key>, gates: &[Gate], expected: Expected) -> ExpectedResult {
    ExpectedResult {
        keys,
        gates: gates.to_vec(),
        expected,
    }
}

/// A transitional `Open` until `until`.
fn transitional(reason: &'static str, owner: &'static str, until: &'static str) -> Expected {
    Expected::Open {
        reason,
        owner,
        approval: Approval::Pending,
        until: Some(until),
    }
}

/// `Holds` on `history` for `property`'s passing key.
fn holds(property: &'static str, gates: &[Gate], history: HistoryRef) -> Vec<ExpectedResult> {
    vec![result(
        vec![Key::passing(property)],
        gates,
        Expected::Holds {
            property,
            history,
            not_applicable: &[],
        },
    )]
}

/// Each counted execution's replay and each counted history's cleanup
/// comparison, once per execution however many rows count it. A machine
/// history's cleanup key is an approved standing `Open`, and a node
/// history's waits for hf-6's stores.
pub fn counted_results(expected: &[ExpectedResult]) -> Vec<ExpectedResult> {
    let mut replayed = std::collections::BTreeSet::new();
    let mut cleaned = std::collections::BTreeSet::new();
    let mut listed = std::collections::BTreeSet::new();
    for history in counted_histories() {
        let run = RunId::from(history);
        replayed.insert(run.clone());
        cleaned.insert(run.clone());
        if !is_machine(history.harness) {
            listed.insert(run);
        }
    }
    for result in expected {
        let (runs, completed) = super::required::counted_runs(&result.expected);
        replayed.extend(runs);
        cleaned.extend(completed);
    }
    let mut results: Vec<ExpectedResult> = replayed
        .into_iter()
        .map(|run| {
            result(
                vec![Key::Replay { run: run.clone() }],
                G1,
                Expected::Replays { run },
            )
        })
        .collect();
    for run in cleaned {
        // A planted case's cleanup comes only from a node `Caught`.
        let machine = matches!(
            &run,
            RunId::History { harness, .. }
                if crate::harness::find(harness).is_ok_and(|entry| entry.machine)
        );
        let cleanup = if machine {
            MACHINE_CLEANUP
        } else {
            NODE_CLEANUP
        };
        results.push(result(vec![Key::Cleanup { run }], G1, cleanup));
    }
    for run in listed {
        results.push(result(vec![Key::NaList { run }], &[Gate::G6], NA_LIST));
    }
    results
}

/// Gives each counted history its whole-report key: on the row's own
/// result when a row's `Holds` already judges that history for its target,
/// or as a `Holds` of its target otherwise.
pub fn attach_history_passes(expected: &mut Vec<ExpectedResult>) {
    for (history, target) in history_targets() {
        let key = Key::History {
            run: history.into(),
            target: target.to_owned(),
        };
        let row = expected.iter_mut().find(|result| {
            matches!(result.expected, Expected::Holds { property, history: h, .. }
                if h == history && property == target)
        });
        match row {
            Some(result) => result.keys.push(key),
            None => {
                let gates = super::required::gate_rule(target).unwrap_or_default();
                expected.push(result(
                    vec![key],
                    &gates,
                    Expected::Holds {
                        property: target,
                        history,
                        not_applicable: &[],
                    },
                ));
            }
        }
    }
}

/// A machine history's cleanup key: it runs a transport-free loop outside
/// `Sim::run` and owns no store.
pub const MACHINE_CLEANUP: Expected = Expected::Open {
    reason: "a machine history runs a transport-free loop outside Sim::run and owns no store, \
             by HarnessEntry::machine",
    owner: "hdr-14",
    approval: PLAN,
    until: None,
};

/// A node execution's cleanup key, until hf-6's stores can be snapshotted.
pub const NODE_CLEANUP: Expected = Expected::Open {
    reason: "the cleanup comparison snapshots the stores hf-6 registers",
    owner: "Adam's harness",
    approval: Approval::Pending,
    until: Some("hf-6"),
};

/// A node history's expected `NotApplicable` list, until C1's reasons let
/// each one be written with the reason its report must carry.
pub const NA_LIST: Expected = Expected::Open {
    reason: "the history's expected NotApplicable reports, each with the reason C1 makes a \
             report carry",
    owner: "Adam's side",
    approval: Approval::Pending,
    until: Some("E1"),
};

/// Whether `harness` is a machine, by
/// [`HarnessEntry::machine`](crate::harness::HarnessEntry::machine).
pub fn is_machine(harness: &str) -> bool {
    crate::harness::find(harness).is_ok_and(|entry| entry.machine)
}

/// `count` cases named `plan <i> of <inputs>`, built once per name.
fn plan_cases(prefix: &'static str, inputs: &'static str, count: usize) -> Vec<&'static str> {
    // Built once for a fixed set, so the leak is bounded.
    static CASES: std::sync::OnceLock<
        std::sync::Mutex<std::collections::BTreeMap<String, &'static str>>,
    > = std::sync::OnceLock::new();
    let cases = CASES.get_or_init(Default::default);
    let mut cases = cases
        .lock()
        .expect("no test panics while holding the case names");
    (0..count)
        .map(|index| {
            let name = format!("{prefix}plan {index} of {inputs}");
            *cases
                .entry(name.clone())
                .or_insert_with(|| Box::leak(name.into_boxed_str()))
        })
        .collect()
}

/// Every history a counted witness test runs, one per execution, written
/// from the tests. Each owes its replay, its cleanup comparison and, on a
/// node harness, its expected `NotApplicable` list, whether or not a row's
/// `Holds` names it. Tests that run several, such as the honest-peer
/// witness's five, list each. A history [`history_targets`] counts for
/// several targets is listed once.
pub fn counted_histories() -> Vec<HistoryRef> {
    let mut seen = std::collections::BTreeSet::new();
    history_targets()
        .into_iter()
        .map(|(history, _)| history)
        .filter(|history| seen.insert(*history))
        .collect()
}

/// Every counted history with the property its whole-report pass judges as
/// its target: the ID the witness test runs it for. The mixed resource
/// witness is run for each of its four rules, so it's listed once per rule.
pub fn history_targets() -> Vec<(HistoryRef, &'static str)> {
    let mut histories = vec![(BOTH_WAYS, generated_ids()[0])];
    for (generated, id) in GENERATED.iter().zip(generated_ids()) {
        if generated.witness != EVERY && !generated.suffix.ends_with("over-owed-closes") {
            histories.push((wire_history(generated.witness, generated.suffix), *id));
        }
    }
    let honest = HONEST_PEER.witness;
    let mut honest_cases = vec!["both ways", "redial", "bystander"];
    honest_cases.extend(plan_cases("lossy ", "inputs(2, 16384)", 2));
    for case in honest_cases {
        histories.push((
            HistoryRef {
                harness: WIRE,
                witness: honest,
                case,
            },
            HONEST,
        ));
    }
    for case in plan_cases("", "inputs(32, 256)", 32) {
        histories.push((HistoryRef { case, ..NODE_SYNC }, OPERATION_IDS));
    }
    let mut progress_cases = vec!["committed plan", "empty plan"];
    progress_cases.extend(plan_cases("", "inputs(4, 16384)", 4));
    for case in progress_cases {
        histories.push((HistoryRef { case, ..PROGRESS }, PROGRESS_ID));
    }
    // The six conformant controls, one per distinct row boundary a fault
    // witness crosses, each its own execution.
    for case in CONTROL_CASES {
        histories.push((wire_history(CONTROL, case), generated_ids()[0]));
    }
    // Each witness of hdr-9's IDs under each of eight generated plans,
    // and outstanding-work-bounded's under its committed Plan.
    for (cases, history, id) in [
        (&RESET_CASES[..], RESET_WITNESSES, RESET),
        (&CHAIN_CASES[..], CHAIN_WITNESSES, CHAIN),
        (&AUTHORITY_CASES[..], AUTHORITY_WITNESSES, COMMIT),
        (&SELECTED_CASES[..], SELECTED_WITNESSES, SELECTED),
        (&PUBLICATION_CASES[..], PUBLICATION_WITNESSES, PUBLICATION),
    ] {
        for prefix in cases {
            for case in plan_cases(prefix, "inputs(8, 256)", 8) {
                histories.push((HistoryRef { case, ..history }, id));
            }
        }
    }
    for case in OUTSTANDING_CASES {
        histories.push((
            HistoryRef {
                case,
                ..OUTSTANDING_WITNESSES
            },
            OUTSTANDING,
        ));
    }
    for case in REPAIR_OUTSTANDING_CASES {
        histories.push((
            HistoryRef {
                case,
                ..REPAIR_OUTSTANDING
            },
            OUTSTANDING,
        ));
    }
    // Each committed hdr-10 liveness fixture under the fixed Plan.
    for (cases, history, id) in [
        (&HOLD_CASES[..], HOLD_WITNESSES, HOLD),
        (&RETIRE_CASES[..], RETIRE_WITNESSES, RETIREMENT),
    ] {
        for &case in cases {
            histories.push((HistoryRef { case, ..history }, id));
        }
    }
    for case in RECOVERY_CASES {
        histories.push((
            HistoryRef {
                case,
                ..RECOVERY_WITNESS
            },
            RECOVERY_ID,
        ));
    }
    histories.extend(MIXED_TARGETS.map(|target| (MIXED_RESOURCES, target)));
    histories.extend([
        (CLOSE_RELINK_CAPACITY, CHUNK),
        (CLOSE_RELINK_TEARDOWN, TEARDOWN),
        (STALE_PREPARE, STALE),
        (RELEASE_INPUT, WAKES),
        (FLOOD_WIRE, WATCH_WIRE),
        (FLOOD_LIFECYCLE, WATCH_LIFECYCLE),
        (SMOKE_RUN, SIM_WIDE[0]),
        (REACTOR_RUN, REACTOR),
        (PUBLISHER_RUN, PUBLISHER),
    ]);
    histories
}

/// A node planted case's `Caught`, covering its own case key and, for the
/// case named `primary`, its property's catch key. Its replays come from
/// [`counted_results`].
fn caught(
    harness: &'static str,
    case: CaseRef,
    set: &'static [&'static str],
    found: Found,
    gates: &[Gate],
    primary: bool,
) -> Vec<ExpectedResult> {
    let mut keys = vec![Key::Case {
        property: case.property.to_owned(),
        name: case.name.to_owned(),
    }];
    if primary {
        keys.push(Key::catch(case.property));
    }
    vec![result(
        keys,
        gates,
        Expected::Caught {
            case,
            harness,
            set,
            found,
            companions: &[],
            not_applicable: super::not_applicable::expected(case),
            patched_not_applicable: super::not_applicable::patched_expected(case),
        },
    )]
}

/// A machine planted case's `MachineCaught`. Its replays come from
/// [`counted_results`].
fn machine_caught(harness: &'static str, case: CaseRef, primary: bool) -> Vec<ExpectedResult> {
    let mut keys = vec![Key::Case {
        property: case.property.to_owned(),
        name: case.name.to_owned(),
    }];
    if primary {
        keys.push(Key::catch(case.property));
    }
    vec![result(keys, G5, Expected::MachineCaught { case, harness })]
}

/// A native mutation's transitional `Open` until `until`, the PR that moves
/// it to a typed planted case. [`Key::covers`] takes no other.
pub fn native_open(until: &'static str) -> Expected {
    transitional(
        "a native mutation whose rule is a claim moves to a typed planted case",
        "Adam's side",
        until,
    )
}

/// `key`'s expected result with the gates `gates`: the transitional `Open`
/// [`owed_open`] writes for it.
fn owed(key: Key, gates: &[Gate]) -> ExpectedResult {
    let open = owed_open(&key).expect("every key a row calls owed for has an owed_open entry");
    result(vec![key], gates, open)
}

/// The witness and bypass tests `COVERAGE` names for a header row, each an
/// executed `Passes`. Listed here by hand, so the static check compares them
/// with `COVERAGE`.
pub fn witness_tests() -> Vec<TestRef> {
    let mut tests: Vec<TestRef> = [EVERY, STATUS, SUBSCRIBE, HEADERS, OUTCOME]
        .into_iter()
        .map(|test| sim_test(WIRE_WITNESS, test))
        .collect();
    tests.extend([
        sim_test(
            WIRE_WITNESS,
            "scripted_bypasses_violate_every_generated_header_id",
        ),
        sim_test(WIRE_WITNESS, "honest_peer_never_punished"),
        sim_test(
            WIRE_WITNESS,
            "scripted_punishments_of_an_honest_peer_violate_honest_never_punished",
        ),
        sim_test(
            "tests/header_node.rs",
            "node_syncs_from_a_mirror_on_controlled_storage",
        ),
        sim_test(
            "tests/header_node.rs",
            "planted_operation_ids_violate_the_property",
        ),
        sim_test(
            "tests/header_node.rs",
            "direct_dispatch_guard_reports_a_violation",
        ),
        CLOSE_RELINK_CAPACITY.witness,
        CLOSE_RELINK_TEARDOWN.witness,
        STALE_PREPARE.witness,
        RESET_WITNESSES.witness,
        CHAIN_WITNESSES.witness,
        AUTHORITY_WITNESSES.witness,
        OUTSTANDING_WITNESSES.witness,
        HOLD_WITNESSES.witness,
        RETIRE_WITNESSES.witness,
        SELECTED_WITNESSES.witness,
        PUBLICATION_WITNESSES.witness,
        RECOVERY_WITNESS.witness,
        RELEASE_INPUT.witness,
        FLOOD_WITNESS,
        MID_PREPARE.witness,
        sim_test(
            "tests/header_wire_prepare_fault.rs",
            "hidden_cooldowns_violate_the_penalty_rule",
        ),
        PROGRESS.witness,
        REACTOR_RUN.witness,
        PUBLISHER_RUN.witness,
        SMOKE_RUN.witness,
        sim_test("tests/harness.rs", "task_panic_violates_no_task_panic"),
        sim_test("tests/harness.rs", "a_moving_clock_holds_no_livelock"),
        sim_test("tests/harness.rs", "livelock_fires_before_budget"),
    ]);
    tests
}

/// The IDs whose `COVERAGE` witness runs a test-only harness, which the
/// executable can't replay, so their passing history comes from a header
/// harness instead. Each keeps its `COVERAGE` test as a required `Passes`.
pub static TEST_ONLY_WITNESSES: &[(&str, TestRef, &str)] = &[
    (
        "sim/no-livelock",
        sim_test("tests/harness.rs", "a_moving_clock_holds_no_livelock"),
        "a_moving_clock_holds_no_livelock runs test/probe, which the executable can't replay",
    ),
    (
        "header-sync/header-fault-penalized",
        sim_test(
            "tests/header_wire_prepare_fault.rs",
            "a_disconnect_mid_prepare_is_still_penalized",
        ),
        "a_disconnect_mid_prepare_is_still_penalized runs test/header-wire-watched, which the \
         executable can't replay",
    ),
];

/// One approved standing exclusion of a passing, catch or missing-source
/// key: the key it's granted for and the exact `Open` that grants it.
#[derive(Clone, Copy, Debug)]
pub struct StandingExclusion {
    /// The key's kind, subject and, for a missing-source key, source, such
    /// as `("catch", "header-sync/...", "")` or
    /// `("missing-source", "header-sync/storage", "View")`.
    pub key: (&'static str, &'static str, &'static str),
    /// Who owns the excluded claim.
    pub owner: &'static str,
    /// The exclusion's reason.
    pub reason: &'static str,
    /// Its approval.
    pub approval: Approval,
}

/// The approved standing exclusions of passing, catch and missing-source
/// keys: `headers/long-payload-closes`' catch, by Adam's hdr-14.5 ruling.
/// The two `over-owed-closes` catch exclusions still await approval, so
/// they block.
pub static STANDING_EXCLUSIONS: &[StandingExclusion] = &[StandingExclusion {
    key: ("catch", "header-sync/s5v9/headers/long-payload-closes", ""),
    owner: "hdr-14",
    reason: LONG_PAGE_UNREACHED,
    approval: HDR_14_5_RULING,
}];

/// Whether `exclusions` grants `key` exactly the standing `Open` `expected`.
pub fn exclusion_granted(exclusions: &[StandingExclusion], key: &Key, expected: &Expected) -> bool {
    let (kind, subject, source) = match key {
        Key::Passing { property } => ("passing", property.as_str(), ""),
        Key::Catch { property } => ("catch", property.as_str(), ""),
        Key::MissingSource { harness, source } => {
            ("missing-source", harness.as_str(), source.as_str())
        }
        _ => return false,
    };
    let Expected::Open {
        reason,
        owner,
        approval,
        until: None,
    } = expected
    else {
        return false;
    };
    exclusions.iter().any(|exclusion| {
        exclusion.key == (kind, subject, source)
            && exclusion.owner == *owner
            && exclusion.reason == *reason
            && exclusion.approval == *approval
    })
}

/// Why `storage-operation-ids` has no catch: its facts are written only by
/// simulator code, which no production patch reaches.
const OPERATION_IDS_UNREACHED: &str =
    "only the controlled storage port, zakura-storage's ControlledPort, numbers the node's \
     storage operations and logs their submissions and completions, so only storage-fake code \
     can break the rule and no production patch can";

/// Why `header-fault-penalized`'s counted history is still owed.
const PENALIZED_PASSING: &str = "a header-sync/wire history for the penalty, since its \
                                 COVERAGE test runs test/header-wire-watched";
/// Why a sim-wide ID's catch is still owed.
const SIM_WIDE_CATCH: &str = "the sim-wide IDs' bypasses run on test-only harnesses, so a \
                              header-harness catch or an approved Open is still owed";
/// Why `sim/no-livelock`'s counted history is still owed.
const LIVELOCK_PASSING: &str = "its witness runs the test-only test/probe harness, which the \
                                executable can't replay, so a header-harness history or an \
                                approved Open is owed";

/// The transitional `Open` each transitional passing or catch key owes,
/// whole: its reason, owner and the PR it waits for, written from the
/// plan's PR table. The sim-wide IDs' catches and the two test-only
/// witnesses' passing histories are hdr-14.4's. [`Key::covers`] takes a
/// transitional `Open` on such a key only if it's exactly this one.
pub fn owed_open(key: &Key) -> Option<Expected> {
    let (reason, until) = match key {
        Key::Catch { property } if SIM_WIDE.contains(&property.as_str()) => {
            (SIM_WIDE_CATCH, "hdr-14.4")
        }
        Key::Passing { property } if property == PENALIZED => (PENALIZED_PASSING, "hdr-14.4"),
        Key::Passing { property } if property == SIM_WIDE[1] => (LIVELOCK_PASSING, "hdr-14.4"),
        _ => return None,
    };
    Some(transitional(reason, "Adam's side", until))
}

/// Every native mutation directory there has been, written apart from
/// `NATIVE_MUTATIONS`, so a mutation whose directory and classification are
/// both removed still has a record that must resolve.
pub static NATIVE_INVENTORY: &[&str] = &[
    "consumed-locator-permit",
    "cooldown-session-gate",
    "dropped-owner-check",
    "foreign-leases",
    "forgotten-locator-permit",
    "late-fault-unpenalized",
    "misstored-page",
    "own9-join",
    "owner-swap",
    "raw-budget-lease",
    "raw-budget-reservation",
    "readmission-unchecked",
    "reordered-payload",
    "secondary-reservation",
    "session-view",
    "short-cooldown",
    "short-payload",
    "shutdown-kept-queue",
    "skewed-remaining",
    "skipped-retention",
    "skipped-staging",
    "stale-locator-round",
    "stale-status-cache",
    "swapped-payload",
    "unreleased-retirement",
    "unsent-tip",
];

/// Every row, one per header ID on `main`.
pub fn rows() -> Vec<Row> {
    let mut rows = Vec::new();
    for (generated, property) in GENERATED.iter().zip(generated_ids()) {
        rows.push(generated_row(generated, property));
    }
    rows.extend(hand_written_rows());
    rows
}

/// The full IDs of [`GENERATED`], in its order, built once.
fn generated_ids() -> &'static [&'static str] {
    static IDS: std::sync::OnceLock<Vec<&'static str>> = std::sync::OnceLock::new();
    IDS.get_or_init(|| {
        GENERATED
            .iter()
            .map(|generated| {
                // Built once for a fixed set of 32, so the leak is bounded.
                &*Box::leak(format!("header-sync/s5v9/{}", generated.suffix).into_boxed_str())
            })
            .collect()
    })
}

/// A generated ID's row: its passing history, or its approval-pending
/// `StaysNotApplicable`, and its catch: its planted case's `Caught`, or its
/// `Open` under the approval its entry names. A second case's `Caught`
/// covers that case's key alone.
fn generated_row(generated: &Generated, property: &'static str) -> Row {
    let mut expected = if generated.suffix.ends_with("over-owed-closes") {
        let results = vec![result(
            vec![Key::passing(property)],
            generated.gates,
            Expected::StaysNotApplicable {
                property,
                history: if generated.witness == HEADERS {
                    wire_history(HEADERS, "headers/unsolicited-closes")
                } else {
                    wire_history(OUTCOME, "headers-outcome/unsolicited-closes")
                },
                reason: NOT_OWED,
                approval: Approval::Pending,
                not_applicable: &[],
            },
        )];
        results
    } else {
        let history = if generated.witness == EVERY {
            BOTH_WAYS
        } else {
            wire_history(generated.witness, generated.suffix)
        };
        holds(property, generated.gates, history)
    };
    let caught_by = |name, primary| {
        // Built once per case for a fixed set of 30, so the leak is
        // bounded.
        let set: &'static [&'static str] = Box::leak(Box::new([property]));
        caught(
            WIRE,
            case(property, name),
            set,
            Found::AtStep,
            generated.gates,
            primary,
        )
    };
    match generated.catch {
        GeneratedCatch::Case(name) => expected.extend(caught_by(name, true)),
        GeneratedCatch::Cases(name, second) => {
            expected.extend(caught_by(name, true));
            expected.extend(caught_by(second, false));
        }
        GeneratedCatch::Open(reason, approval) => expected.push(result(
            vec![Key::catch(property)],
            generated.gates,
            Expected::Open {
                reason,
                owner: "hdr-14",
                approval,
                until: None,
            },
        )),
    }
    Row {
        property,
        tier: Tier::Sim,
        harnesses: &[WIRE],
        status: Status::Active,
        promotion: None,
        expected,
    }
}

/// The 13 hand-written header IDs' rows.
fn hand_written_rows() -> Vec<Row> {
    let storage_harnesses: &'static [&'static str] =
        &[STORAGE, RELEASE_WAKE, WIRE, FLOOD, REPAIR, LIVENESS];
    let mut rows = Vec::new();

    let mut operation_ids = holds(OPERATION_IDS, G1, NODE_SYNC);
    operation_ids.push(result(
        vec![Key::catch(OPERATION_IDS)],
        G1,
        Expected::Open {
            reason: OPERATION_IDS_UNREACHED,
            owner: "hdr-14",
            approval: Approval::Pending,
            until: None,
        },
    ));
    rows.push(sim_row(OPERATION_IDS, storage_harnesses, operation_ids));

    let mut direct = holds(DIRECT_DISPATCH, G1, NODE_SYNC);
    direct.extend(caught(
        STORAGE,
        case(DIRECT_DISPATCH, "locator-to-action-channel"),
        &[DIRECT_DISPATCH],
        Found::AtStep,
        G1,
        true,
    ));
    rows.push(sim_row(DIRECT_DISPATCH, storage_harnesses, direct));

    let mut chunk = holds(CHUNK, G5, CLOSE_RELINK_CAPACITY);
    chunk.push(mixed(CHUNK));
    chunk.extend(caught(
        STORAGE,
        case(CHUNK, "uncapped-secondary-reservation"),
        &[CHUNK],
        Found::AtStep,
        G5,
        true,
    ));
    // C19's bug-only half (Adam's ruling on question 7): a release that
    // re-takes the budget's lock leaves a boundary unread at a dry end.
    let relocks = case(CHUNK, "release-relocks-budget");
    chunk.push(result(
        vec![Key::Case {
            property: CHUNK.to_owned(),
            name: relocks.name.to_owned(),
        }],
        G5,
        Expected::HarnessCaught {
            case: relocks,
            harness: STORAGE,
            detail: UNREAD_BOUNDARY,
            not_applicable: super::not_applicable::expected(relocks),
        },
    ));
    rows.push(sim_row(CHUNK, storage_harnesses, chunk));

    let mut teardown = holds(TEARDOWN, G5, CLOSE_RELINK_TEARDOWN);
    teardown.push(mixed(TEARDOWN));
    // `forgotten-completion`'s leak shows only at the end, as an end finding.
    for (index, (name, found)) in [
        ("forgotten-completion", Found::AtEnd),
        ("forgotten-retained-headers", Found::AtStep),
        ("kept-queue-lease", Found::AtStep),
        ("leaked-lease-clone", Found::AtStep),
        ("leaked-reservation-entry", Found::AtStep),
        ("skipped-round-release", Found::AtStep),
        // hdr-14.4 moved the node-settlement native mutations here, each on
        // the committed stop-storage seed, which shuts the node down.
        ("shutdown-kept-queue", Found::AtStep),
        ("consumed-locator-permit", Found::AtStep),
        ("forgotten-locator-permit", Found::AtStep),
    ]
    .into_iter()
    .enumerate()
    {
        teardown.extend(caught(
            STORAGE,
            case(TEARDOWN, name),
            &[TEARDOWN],
            found,
            G5,
            index == 0,
        ));
    }
    rows.push(sim_row(TEARDOWN, storage_harnesses, teardown));

    let mut stale = holds(STALE, G5, STALE_PREPARE);
    stale.push(mixed(STALE));
    for (index, name) in [
        "stale-charge",
        "stale-credit",
        "stale-release",
        "stale-retirement",
        "stale-acceptance",
        "stale-close",
        // hdr-14.4 moved this native mutation here, on the churn witness.
        "dropped-owner-check",
    ]
    .into_iter()
    .enumerate()
    {
        stale.extend(caught(
            STORAGE,
            case(STALE, name),
            &[STALE],
            Found::AtStep,
            G5,
            index == 0,
        ));
    }
    rows.push(sim_row(STALE, storage_harnesses, stale));

    let mut reset = holds(RESET, G2, RESET_WITNESSES);
    // `entry-kept` keeps a round's entry past its reset, which also leaves
    // outstanding work, so its case lists both.
    for (index, (name, harness, set, found)) in [
        (
            "anchor-only-locator",
            STORAGE,
            &[RESET] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "awaiting-kept",
            STORAGE,
            &[RESET] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "bump-skipped",
            STORAGE,
            &[RESET] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "bump-skipped-quiet",
            STORAGE,
            &[RESET] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "cross-generation-locator",
            STORAGE,
            &[RESET] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "entry-kept",
            STORAGE,
            &[RESET, OUTSTANDING] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "insertion-bump-skipped",
            STORAGE,
            &[RESET] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "insertion-bump-skipped-by-commit",
            STORAGE,
            &[RESET] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "insertion-bump-skipped-held",
            STORAGE,
            &[RESET] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "no-locator",
            STORAGE,
            &[RESET] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "old-hashes",
            STORAGE,
            &[RESET] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "old-locator",
            STORAGE,
            &[RESET] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "owner-refreshed",
            STORAGE,
            &[RESET] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "skipped-locator-entry",
            STORAGE,
            &[RESET] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "skipped-retirement",
            STORAGE,
            &[RESET] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "stale-locator-round",
            STORAGE,
            &[RESET] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "stale-read",
            STORAGE,
            &[RESET] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "tip-only-locator",
            STORAGE,
            &[RESET] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "unreleased-obsolete",
            STORAGE,
            &[RESET] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "wrong-locator-height",
            STORAGE,
            &[RESET] as &'static [&'static str],
            Found::AtStep,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        reset.extend(caught(
            harness,
            case(RESET, name),
            set,
            found,
            G2,
            index == 0,
        ));
    }
    rows.push(sim_row(RESET, storage_harnesses, reset));

    let mut chain = holds(CHAIN, G2, CHAIN_WITNESSES);
    for (index, (name, harness, set, found)) in [
        (
            "stale-adoption",
            STORAGE,
            &[CHAIN] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "stale-commit",
            STORAGE,
            &[CHAIN] as &'static [&'static str],
            Found::AtStep,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        chain.extend(caught(
            harness,
            case(CHAIN, name),
            set,
            found,
            G2,
            index == 0,
        ));
    }
    rows.push(sim_row(CHAIN, storage_harnesses, chain));

    let mut commit = holds(COMMIT, G23, AUTHORITY_WITNESSES);
    // Three cases run a saved wire Plan, on the wire harness.
    for (index, (name, harness, set, found)) in [
        (
            "accepted-before-apply",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "accepted-not-stored",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "acknowledged-bytes",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "added-reason",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "advanced-verified-best",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "applied-without-tip",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "apply-source-changed",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "byte-overgrant",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "changed-held-validation",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "cleared-preferred-tip",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "corrupted-best-score",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "corrupted-best-score-by-commit",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "corrupted-projection-entry",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "corrupted-side-eligibility",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "corrupted-side-parent",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "corrupted-verified-projection",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "dropped-payload-header",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "failed-projection-entry",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "failed-side-eligibility",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "failure-after-mutation",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "flattened-side-coordinate",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "foreign-leases",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "forwarded-length",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "grant-after-cancellation",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "grant-other-target",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "later-ancestor",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "locator-mutates",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "locator-swapped",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "lower-locator-allowed",
            WIRE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "mischarged-page",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "miscounted-cursor",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "miscounted-page",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "misdated-admission",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "new-node-body-state",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "off-path-page",
            WIRE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "open-drops-locator",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "open-other-target",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "overcredit-guard",
            WIRE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "overgrant",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "owner-swap",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "projection-to-side-tip",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "reordered-payload",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "sealed-batch-truncated",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "session-id-changed",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "skewed-remaining",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "skipped-sequence",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "skipped-staging",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "stored-excess-credit",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "stored-order",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "submitted-source-changed",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "swapped-payload",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "swapped-side-header",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "truncated-projection",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "unrelated-branch-deleted",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "unstored-cursor",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "v1-record-charged-twice",
            REPAIR,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "wrong-acknowledgement",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "wrong-anchor",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "wrong-close",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "wrong-inheritance",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "wrong-invalidation-id",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "wrong-peer",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "wrong-request",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "wrong-source",
            STORAGE,
            &[COMMIT] as &'static [&'static str],
            Found::AtStep,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        commit.extend(caught(
            harness,
            case(COMMIT, name),
            set,
            found,
            G23,
            index == 0,
        ));
    }
    // Gap 6: a page staged despite its connection's fault close, judged on
    // the case's Draft run by its three companions (`companions`).
    let despite = super::companions::DESPITE_CLOSE;
    commit.push(result(
        vec![Key::Case {
            property: despite.property.to_owned(),
            name: despite.name.to_owned(),
        }],
        G23,
        Expected::Caught {
            case: despite,
            harness: WIRE,
            set: &[COMMIT],
            found: Found::AtStep,
            companions: super::companions::COMPANIONS,
            not_applicable: super::not_applicable::expected(despite),
            patched_not_applicable: super::not_applicable::patched_expected(despite),
        },
    ));
    rows.push(sim_row(COMMIT, storage_harnesses, commit));

    let mut outstanding = holds(OUTSTANDING, G5, OUTSTANDING_WITNESSES);
    outstanding.push(mixed(OUTSTANDING));
    // The two cancellation cases' publishers linger at rest, which the end
    // check finds after the drain. The run records that as its stopping
    // `Violation`, so both are found at a step.
    for (index, (name, harness, set, found)) in [
        (
            "accepted-cursors-kept",
            STORAGE,
            &[OUTSTANDING] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "call-kept",
            STORAGE,
            &[OUTSTANDING] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "cancellation-ignored",
            STORAGE,
            &[OUTSTANDING] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "cancellation-ignored-at-capacity",
            STORAGE,
            &[OUTSTANDING] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "close-kept-on-replace",
            STORAGE,
            &[OUTSTANDING] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "completed-targets-kept",
            STORAGE,
            &[OUTSTANDING] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "deadline-kept",
            STORAGE,
            &[OUTSTANDING] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "departed-source-unpruned",
            REPAIR,
            &[OUTSTANDING] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "ended-kept",
            STORAGE,
            &[OUTSTANDING] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "entries-cloned",
            STORAGE,
            &[OUTSTANDING] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "execution-admitted-twice",
            STORAGE,
            &[OUTSTANDING] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "extra-future",
            STORAGE,
            &[OUTSTANDING] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "locator-entry-kept",
            STORAGE,
            &[OUTSTANDING] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "marked-at-prepare",
            STORAGE,
            &[OUTSTANDING] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "outcome-kept",
            STORAGE,
            &[OUTSTANDING] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "pruning-skipped",
            STORAGE,
            &[OUTSTANDING] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "publisher-token-detached",
            STORAGE,
            &[OUTSTANDING] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "retention-skipped",
            STORAGE,
            &[OUTSTANDING] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "reuse-skipped",
            STORAGE,
            &[OUTSTANDING] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "secondary-reservation",
            STORAGE,
            &[OUTSTANDING] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "spare-publisher",
            STORAGE,
            &[OUTSTANDING] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "staged-key-kept",
            STORAGE,
            &[OUTSTANDING] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "supplier-twice",
            STORAGE,
            &[OUTSTANDING] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "unbounded-slots",
            STORAGE,
            &[OUTSTANDING] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "unknown-as-known",
            STORAGE,
            &[OUTSTANDING] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "waits-kept",
            STORAGE,
            &[OUTSTANDING] as &'static [&'static str],
            Found::AtStep,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        outstanding.extend(caught(
            harness,
            case(OUTSTANDING, name),
            set,
            found,
            G5,
            index == 0,
        ));
    }
    rows.push(sim_row(
        OUTSTANDING,
        &[STORAGE, RELEASE_WAKE, FLOOD, REPAIR, LIVENESS],
        outstanding,
    ));

    let mut hold = holds(HOLD, G5, HOLD_WITNESSES);
    for (index, name) in [
        "committed-page-dropped",
        "completion-arm-removed",
        "exits-on-reply",
        "lost-notification",
        "page-job-wake-lost",
    ]
    .into_iter()
    .enumerate()
    {
        hold.extend(caught(
            LIVENESS,
            case(HOLD, name),
            &[HOLD],
            Found::AtStep,
            G5,
            index == 0,
        ));
    }
    rows.push(sim_row(HOLD, &[LIVENESS], hold));

    let mut retirement = holds(RETIREMENT, G5, RETIRE_WITNESSES);
    // `session-view` and `unreleased-retirement` are hdr-5's native
    // mutations, moved here as cases of their own names.
    for (index, name) in [
        "kept-repair-widened",
        "lost-timeout",
        "retained-owner",
        "session-view",
        "spent-on-refusal",
        "unreleased-retirement",
    ]
    .into_iter()
    .enumerate()
    {
        retirement.extend(caught(
            LIVENESS,
            case(RETIREMENT, name),
            &[RETIREMENT],
            Found::AtStep,
            G5,
            index == 0,
        ));
    }
    rows.push(sim_row(
        RETIREMENT,
        &[STORAGE, RELEASE_WAKE, WIRE, FLOOD, REPAIR, LIVENESS],
        retirement,
    ));

    let mut selected = holds(SELECTED, G2, SELECTED_WITNESSES);
    for (index, (name, harness, set, found)) in [
        (
            "every-session-refused",
            STORAGE,
            &[SELECTED] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "later-sessions-refused",
            STORAGE,
            &[SELECTED],
            Found::AtStep,
        ),
        (
            "missing-cooldown-refuses",
            STORAGE,
            &[SELECTED],
            Found::AtStep,
        ),
        (
            "service-rejects-valid-streams",
            STORAGE,
            &[SELECTED],
            Found::AtStep,
        ),
        (
            "offer-by-height",
            STORAGE,
            &[SELECTED] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "score-corrupted",
            STORAGE,
            &[SELECTED] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "score-corrupted-by-commit",
            STORAGE,
            &[SELECTED] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "selector-prefers-length",
            STORAGE,
            &[SELECTED] as &'static [&'static str],
            Found::AtStep,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        selected.extend(caught(
            harness,
            case(SELECTED, name),
            set,
            found,
            G2,
            index == 0,
        ));
    }
    rows.push(sim_row(SELECTED, storage_harnesses, selected));

    let mut publication = holds(PUBLICATION, G134, PUBLICATION_WITNESSES);
    // `stale-status-cache` and `unsent-tip` are the adopted-state native
    // mutations, moved here as cases of their own names.
    for (index, (name, harness, set, found)) in [
        (
            "ancestor-field",
            STORAGE,
            &[PUBLICATION] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "first-locator-dropped",
            STORAGE,
            &[PUBLICATION] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "first-page-height",
            STORAGE,
            &[PUBLICATION] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "first-page-target",
            STORAGE,
            &[PUBLICATION] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "mixed-adoption",
            STORAGE,
            &[PUBLICATION] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "old-fields-after-reset",
            STORAGE,
            &[PUBLICATION] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "page-order",
            STORAGE,
            &[PUBLICATION] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "permanent-refusal",
            LIVENESS,
            &[PUBLICATION] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "postponed-while-changing",
            LIVENESS,
            &[PUBLICATION] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "publication-skipped",
            STORAGE,
            &[PUBLICATION] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "stale-status-cache",
            LIVENESS,
            &[PUBLICATION] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "store-dropped",
            STORAGE,
            &[PUBLICATION] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "stored-version-kept",
            STORAGE,
            &[PUBLICATION] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "target-as-ancestor",
            STORAGE,
            &[PUBLICATION] as &'static [&'static str],
            Found::AtStep,
        ),
        // The unsent tip shows only when the run comes to rest, as the
        // property's end finding.
        (
            "unsent-tip",
            STORAGE,
            &[PUBLICATION] as &'static [&'static str],
            Found::AtEnd,
        ),
        (
            "watch-ignored",
            LIVENESS,
            &[PUBLICATION] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "watch-ignored-in-storage",
            STORAGE,
            &[PUBLICATION] as &'static [&'static str],
            Found::AtStep,
        ),
        (
            "wrong-fork-served",
            STORAGE,
            &[PUBLICATION] as &'static [&'static str],
            Found::AtStep,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        publication.extend(caught(
            harness,
            case(PUBLICATION, name),
            set,
            found,
            G134,
            index == 0,
        ));
    }
    rows.push(sim_row(PUBLICATION, storage_harnesses, publication));

    let wakes = WAKES;
    let mut wake = holds(wakes, G5, RELEASE_INPUT);
    for (index, name) in [
        "missed-refill-deadline",
        "missed-release-wake",
        "refill-adds-nothing",
        "dropped-waiting-peer",
    ]
    .into_iter()
    .enumerate()
    {
        wake.extend(caught(
            RELEASE_WAKE,
            case(wakes, name),
            &[WAKES],
            Found::AtStep,
            G5,
            index == 0,
        ));
    }
    rows.push(sim_row(wakes, &[RELEASE_WAKE], wake));

    // Each watch rule's history is its own committed flood input, and both
    // patches break each rule on its own input (each case's
    // `also_violates` covers only the campaign's other-queue inputs).
    for (id, set, history) in [
        (WATCH_WIRE, &[WATCH_WIRE], FLOOD_WIRE),
        (WATCH_LIFECYCLE, &[WATCH_LIFECYCLE], FLOOD_LIFECYCLE),
    ] {
        let mut watch = holds(id, G1, history);
        for (name, found) in [
            // The revert's starved watch stops the run at a step. The wedged
            // loop leaves a publication unread when the run settles, which
            // its end check finds.
            ("watch-check-revert", Found::AtStep),
            ("wedged-run-loop", Found::AtEnd),
        ] {
            watch.extend(caught(
                FLOOD,
                case(id, name),
                set,
                found,
                G1,
                name == "watch-check-revert",
            ));
        }
        rows.push(sim_row(id, &[FLOOD], watch));
    }

    let penalized = PENALIZED;
    // Its COVERAGE test runs the test-only test/header-wire-watched
    // harness (`TEST_ONLY_WITNESSES`), so its counted history waits for a
    // registered one.
    let mut fault = vec![owed(Key::passing(penalized), G2)];
    // hdr-14.4 moved its three native mutations here, each a typed case on
    // the scripted late-fault fixture, and each is found at a step.
    for name in [
        "late-fault-unpenalized",
        "cooldown-session-gate",
        "short-cooldown",
    ] {
        fault.extend(caught(
            WIRE,
            case(penalized, name),
            &[PENALIZED],
            Found::AtStep,
            G2,
            name == "late-fault-unpenalized",
        ));
    }
    // C2's owed penalty concluding violated at the drain's end, through a
    // node-side abandonment: the reactor's own exit drops the call holding
    // the failed reply, and the end replay keeps it as a `DroppedFault`.
    fault.extend(caught(
        WIRE,
        case(penalized, "idle-exit-drops-fault"),
        &[PENALIZED],
        Found::AtEnd,
        G2,
        false,
    ));
    rows.push(sim_row(penalized, &[WIRE], fault));

    let honest = HONEST;
    let mut never = holds(honest, G2, HONEST_PEER);
    // Any production punishment of an honest peer also breaks
    // conformant-never-closes, so the catch is nested (decision 2).
    never.extend(caught(
        WIRE,
        case(honest, "target-page-refused"),
        &[HONEST, CONFORMANT],
        Found::AtStep,
        G2,
        true,
    ));
    rows.push(sim_row(honest, &[WIRE], never));

    let progress = PROGRESS_ID;
    let mut progressing = holds(progress, NONE, PROGRESS);
    progressing.extend(caught(
        WIRE,
        case(progress, "skipped-apply"),
        &[PROGRESS_ID],
        Found::AtEnd,
        NONE,
        false,
    ));
    progressing.extend(caught(
        WIRE,
        case(progress, "dropped-honest-peer"),
        &[PROGRESS_ID],
        Found::AtEnd,
        NONE,
        false,
    ));
    // Still Draft at hdr-14.8: its promotion needs Evan's agreement, so the
    // frozen-head run's record lists it under `drafts` until he gives it.
    rows.push(Row {
        property: progress,
        tier: Tier::Sim,
        harnesses: &[WIRE],
        status: Status::Draft,
        promotion: Some(Promotion {
            condition: "Evan agrees to promote it (the hdr-6–8 plan's hdr-8)",
            owner: "Evan",
        }),
        expected: progressing,
    });

    let recovery = RECOVERY_ID;
    let mut recovering = holds(recovery, NONE, RECOVERY_WITNESS);
    // hdr-11's control bypass, then hdr-13's: each violates only this ID
    // on its committed fixture (a case's `also_violates` covers only its
    // campaign's departure inputs).
    for name in [
        "skipped-apply",
        "stalled-successor",
        "faulted-target-remembered",
        "refusal-never-lifts",
        "pause-outlasts-cooldown",
    ] {
        recovering.extend(caught(
            LIVENESS,
            case(recovery, name),
            &[RECOVERY_ID],
            Found::AtStep,
            NONE,
            name == "stalled-successor",
        ));
    }
    rows.push(sim_row(recovery, &[STORAGE, LIVENESS], recovering));

    let mut reactor = vec![result(
        vec![Key::passing(REACTOR)],
        G5,
        Expected::Holds {
            property: REACTOR,
            history: REACTOR_RUN,
            not_applicable: &[],
        },
    )];
    reactor.extend(machine_caught(
        REACTOR,
        case(REACTOR, "locator-call-settles-without-removal"),
        true,
    ));
    rows.push(machine_row(REACTOR, reactor));

    let mut publisher = vec![result(
        vec![Key::passing(PUBLISHER)],
        G5,
        Expected::Holds {
            property: PUBLISHER,
            history: PUBLISHER_RUN,
            not_applicable: &[],
        },
    )];
    for (index, name) in ["lost-credit", "lost-job", "lost-terminal"]
        .into_iter()
        .enumerate()
    {
        publisher.extend(machine_caught(PUBLISHER, case(PUBLISHER, name), index == 0));
    }
    rows.push(machine_row(PUBLISHER, publisher));

    let node_harnesses: &'static [&'static str] =
        &[SMOKE, STORAGE, RELEASE_WAKE, WIRE, FLOOD, REPAIR, LIVENESS];
    let mut panic = holds(SIM_WIDE[0], NONE, SMOKE_RUN);
    panic.push(owed(Key::catch(SIM_WIDE[0]), NONE));
    rows.push(sim_row(SIM_WIDE[0], node_harnesses, panic));

    let livelock = vec![
        owed(Key::passing(SIM_WIDE[1]), NONE),
        owed(Key::catch(SIM_WIDE[1]), NONE),
    ];
    rows.push(sim_row(SIM_WIDE[1], node_harnesses, livelock));

    rows
}

/// An `Active` row in the `Sim` tier.
fn sim_row(
    property: &'static str,
    harnesses: &'static [&'static str],
    expected: Vec<ExpectedResult>,
) -> Row {
    Row {
        property,
        tier: Tier::Sim,
        harnesses,
        status: Status::Active,
        promotion: None,
        expected,
    }
}

/// A machine harness's row.
fn machine_row(property: &'static str, expected: Vec<ExpectedResult>) -> Row {
    Row {
        property,
        tier: Tier::Machine,
        harnesses: if property == REACTOR {
            &[REACTOR]
        } else {
            &[PUBLISHER]
        },
        status: Status::Active,
        promotion: None,
        expected,
    }
}

/// What `chunk-budget-bound`'s missing evidence says of a boundary still
/// unread at a dry end: the registry's unread-boundary detail.
pub const UNREAD_BOUNDARY: &str = "with a boundary unread";

/// One broader claim a planted case's set may add to its own ID.
#[derive(Clone, Copy, Debug)]
pub struct Nest {
    /// The broader claim.
    pub broader: &'static str,
    /// The narrower claims it contains.
    pub over: &'static [&'static str],
    /// When the broader claim is broken too. The static check joins only
    /// the two IDs. The case's own trace must show this, which C6's
    /// set-aware judge checks (E2).
    pub when: &'static str,
}

/// Which IDs a planted case's set may add to its own, written from the
/// claims and not from any case. `conformant-never-closes` covers every
/// `-never-closes` ID and any punishment no fault label excuses, which
/// includes punishing a peer marked honest. `session/honest-never-punished`
/// covers those only for a peer marked honest.
pub static NESTS: &[Nest] = &[
    Nest {
        broader: "header-sync/outstanding-work-bounded",
        over: &["header-sync/reset-invalidates-work"],
        when: "the reactor keeps a container entry for a round the reset retired",
    },
    Nest {
        broader: "header-sync/s5v9/conformant-never-closes",
        over: &[
            "header-sync/s5v9/status/cadence-never-closes",
            "session/honest-never-punished",
        ],
        when: "the punishment answers a frame no fault label excuses",
    },
    Nest {
        broader: "session/honest-never-punished",
        over: &[
            "header-sync/s5v9/status/cadence-never-closes",
            "header-sync/s5v9/conformant-never-closes",
        ],
        when: "the punished peer is marked honest",
    },
];

/// A gate obligation no single ID owns (item 0's `GATE_REQUIREMENTS`),
/// written from the spec's gate table.
#[derive(Clone, Debug)]
pub struct GateRequirement {
    /// Its name, its [`Key::Requirement`].
    pub name: &'static str,
    /// The gates it serves.
    pub gates: &'static [Gate],
    /// What satisfies it on `main`.
    pub expected: Expected,
}

/// What satisfies each gate obligation no row owns. On `main` most are
/// transitional until the hdr-14 PR that builds them. The obligations
/// themselves are listed apart, in
/// [`GATE_OBLIGATIONS`](super::required::GATE_OBLIGATIONS), so an entry
/// deleted here still leaves its key required.
pub static GATE_REQUIREMENTS: &[GateRequirement] = &[
    GateRequirement {
        name: "gate-1/a-a-and-a-b-a",
        gates: G1,
        expected: Expected::Passes {
            test: sim_test(
                TOOL_TESTS,
                "each_history_is_the_same_alone_and_after_another",
            ),
        },
    },
    GateRequirement {
        name: "gate-1/topology-freshness",
        gates: G1,
        expected: Expected::Open {
            reason: obligation::GATE_1_TOPOLOGY_FRESHNESS,
            owner: "Adam's harness",
            approval: Approval::Pending,
            until: Some("hf-7"),
        },
    },
    GateRequirement {
        name: "gate-1/store-inventory",
        gates: G1,
        expected: Expected::Open {
            reason: obligation::GATE_1_STORE_INVENTORY,
            owner: "Adam's harness",
            approval: Approval::Pending,
            until: Some("hf-6"),
        },
    },
    GateRequirement {
        name: "gate-1/replay-store-control",
        gates: G1,
        expected: Expected::Passes {
            test: sim_test(
                TOOL_TESTS,
                "a_changed_stored_fact_fails_the_replay_comparison",
            ),
        },
    },
    GateRequirement {
        name: "gate-2/conformant-control-per-fault",
        gates: G2,
        expected: Expected::Passes {
            test: sim_test(
                WIRE_WITNESS,
                "each_invalid_label_witness_has_a_conformant_control",
            ),
        },
    },
    GateRequirement {
        name: "gate-2/despite-close/caught",
        gates: G2,
        expected: Expected::Passes {
            test: sim_test(
                DESPITE_CLOSE_TESTS,
                "the_path_break_needs_a_retained_violation",
            ),
        },
    },
    GateRequirement {
        name: "gate-2/despite-close/close-companion",
        gates: G2,
        expected: Expected::Passes {
            test: sim_test(
                DESPITE_CLOSE_TESTS,
                "the_fault_close_holds_on_the_unpatched_rejection",
            ),
        },
    },
    GateRequirement {
        name: "gate-2/despite-close/staging-companion",
        gates: G2,
        expected: Expected::Passes {
            test: sim_test(
                DESPITE_CLOSE_TESTS,
                "the_staging_companion_needs_the_rejected_page_staged",
            ),
        },
    },
    GateRequirement {
        name: "gate-2/bad-content-fault-close/status",
        gates: G2,
        expected: Expected::Passes {
            test: sim_test(TABLE_TESTS, "status_bad_content_closes_with_a_fault"),
        },
    },
    GateRequirement {
        name: "gate-2/bad-content-fault-close/subscribe-headers",
        gates: G2,
        expected: Expected::Passes {
            test: sim_test(
                TABLE_TESTS,
                "subscribe_headers_bad_content_closes_with_a_fault",
            ),
        },
    },
    GateRequirement {
        name: "gate-2/bad-content-fault-close/headers",
        gates: G2,
        expected: Expected::Passes {
            test: sim_test(TABLE_TESTS, "headers_bad_content_closes_with_a_fault"),
        },
    },
    GateRequirement {
        name: "gate-2/bad-content-fault-close/headers-outcome",
        gates: G2,
        expected: Expected::Passes {
            test: sim_test(
                TABLE_TESTS,
                "headers_outcome_bad_content_closes_with_a_fault",
            ),
        },
    },
    GateRequirement {
        name: "gate-2/bad-content-close-claim",
        gates: G2,
        expected: Expected::Open {
            reason: "no property fails a node that answers header bad content with a \
                     Misbehavior and leaves the connection open; the table-derived properties \
                     own that claim (ask 5)",
            owner: "the table-derived properties",
            approval: PLAN,
            until: None,
        },
    },
    GateRequirement {
        name: "gate-2/downstream-templates",
        gates: G2,
        expected: Expected::Open {
            reason: "header sync judges downstream effects at the owner boundary (Rule 10), so \
                     the generic templates stay unregistered (decision 3)",
            owner: "hdr-9 to hdr-13",
            approval: Approval::Adam {
                record: "hdr-14 plan, decision 3",
            },
            until: None,
        },
    },
    GateRequirement {
        name: "gate-2/same-step-preservation",
        gates: G2,
        expected: Expected::Passes {
            test: sim_test(
                "tests/registry.rs",
                "known_and_active_failures_at_one_boundary",
            ),
        },
    },
    // C12 (E9) and C13 (hf-1 to hf-3) have landed, but both contracts are
    // still Blocked in the contracts table, so this stays open until their
    // states move with their proving tests. The frozen-head run then
    // records it.
    GateRequirement {
        name: "gate-2/end-boundary-preservation",
        gates: G2,
        expected: Expected::Open {
            reason: obligation::GATE_2_END_BOUNDARY_PRESERVATION,
            owner: "Evan (E9) and Adam's harness (hf-1 to hf-3)",
            approval: Approval::Pending,
            until: Some("hdr-14.8"),
        },
    },
    GateRequirement {
        name: "gate-3/crossing/unretained-open-then-outcome",
        gates: G3,
        expected: Expected::Passes {
            test: sim_test(
                CROSSING_TESTS,
                "an_unretained_open_then_outcome_crossing_is_conformant_and_unpunished",
            ),
        },
    },
    GateRequirement {
        name: "gate-3/crossing/crossing-close",
        gates: G3,
        expected: Expected::Passes {
            test: sim_test(
                CROSSING_TESTS,
                "a_crossing_close_is_conformant_and_unpunished",
            ),
        },
    },
    GateRequirement {
        name: "gate-3/crossing/over-window-grant",
        gates: G3,
        expected: Expected::Passes {
            test: sim_test(
                CROSSING_TESTS,
                "an_over_window_crossing_grant_is_bad_cursor_and_closed",
            ),
        },
    },
    GateRequirement {
        name: "gate-3/no-request-row",
        gates: G3,
        expected: Expected::Open {
            reason: "header sync has no request row, so within-limit, over-margin, \
                     receiver-serving-bounded and sender-within-response-quantity aren't \
                     generated for it",
            owner: "hdr-14",
            approval: PLAN,
            until: None,
        },
    },
    GateRequirement {
        name: "gate-3/credit-fits-window",
        gates: G3,
        expected: Expected::Open {
            reason: "on-wire credit against the receive window belongs to the table-derived \
                     properties (Rule 10, ask 5)",
            owner: "the table-derived properties",
            approval: Approval::Pending,
            until: None,
        },
    },
    GateRequirement {
        name: "gate-4/send-interval",
        gates: &[Gate::G4],
        expected: Expected::Open {
            reason: "CadenceModel doesn't model send_interval and the status floor is unjudged \
                     (ask 5)",
            owner: "the table toolkit's sender cadence, or hdr-12b",
            approval: Approval::Pending,
            until: None,
        },
    },
    GateRequirement {
        name: "gate-5/bound-table",
        gates: G5,
        expected: Expected::Open {
            reason: obligation::GATE_5_BOUND_TABLE,
            owner: "Adam's side",
            approval: Approval::Pending,
            until: Some("hdr-9d.1"),
        },
    },
    GateRequirement {
        name: "gate-5/mixed-witness",
        gates: G5,
        expected: Expected::Passes { test: MIXED_TEST },
    },
];

/// The [`GATE_REQUIREMENTS`] as expected results.
pub fn requirement_results() -> Vec<ExpectedResult> {
    GATE_REQUIREMENTS
        .iter()
        .map(|requirement| {
            result(
                vec![Key::Requirement {
                    name: requirement.name.to_owned(),
                }],
                requirement.gates,
                requirement.expected.clone(),
            )
        })
        .collect()
}

/// A retired native mutation's witness test outside the simulator, and the
/// failure its README requires of the patched run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnitWitness {
    /// The test, by its full libtest name.
    pub test: TestRef,
    /// The assertion message the patched run must fail with.
    pub failure: &'static str,
    /// What that assertion's `left` value must show, in order.
    pub shows: &'static [&'static str],
}

/// What a native mutation under `qa/native-mutations/header-sync/` is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeClass {
    /// It moves to a typed planted case under `property` in `until`.
    ToMove {
        /// The property it moves under.
        property: &'static str,
        /// The PR that moves it.
        until: &'static str,
    },
    /// It's detected only as integrity: an `IntegrityCaught` for gate 6.
    Integrity {
        /// The history it runs.
        history: HistoryRef,
        /// The diagnostic it must fail with.
        diagnostic: &'static str,
        /// The history's expected `NotApplicable` properties on its clean
        /// runs, written once C1's reasons exist.
        not_applicable: &'static [ExpectedNa],
    },
    /// It moved to the planted case `case` under `property`, whose catch
    /// a row must keep naming once the directory is gone.
    /// The planted case keeps the mutation's name, which the static check
    /// requires, so an unrelated existing case can't stand in for it.
    Moved {
        /// The property it moved under.
        property: &'static str,
        /// The planted case it became.
        case: &'static str,
    },
    /// It stays outside the simulator, caught by `witness`, a test run
    /// unpatched, patched and restored as a plain build: a `UnitCaught`
    /// for gate 6.
    Retired {
        /// Why.
        reason: &'static str,
        /// The test that catches it and the failure it must show.
        witness: UnitWitness,
    },
    /// It's a duplicate of the existing planted case `case` under
    /// `property`, an approved retirement. Its directory stays, so its key
    /// stays required on gate 6, and that key shares the case's `Caught`:
    /// it verifies only once that case is caught.
    Duplicate {
        /// The covering case's property.
        property: &'static str,
        /// The covering case.
        case: &'static str,
    },
}

/// One native mutation's classification.
#[derive(Clone, Copy, Debug)]
pub struct NativeMutation {
    /// Its directory's name.
    pub patch: &'static str,
    /// What it is.
    pub class: NativeClass,
}

/// The capacity witness of the probe's accounting, which the integrity
/// mutations run.
const PROBE_ACCOUNTING: HistoryRef = HistoryRef {
    harness: STORAGE,
    witness: sim_test(
        "tests/header_node_capacity.rs",
        "the_probe_matches_the_accounting_through_close_relink_and_shutdown",
    ),
    case: "plan 0 of inputs(16, 256)",
};

/// A [`NativeClass::ToMove`].
const fn to_move(
    patch: &'static str,
    property: &'static str,
    until: &'static str,
) -> NativeMutation {
    NativeMutation {
        patch,
        class: NativeClass::ToMove { property, until },
    }
}

/// Every native mutation on `main`, classified. The ledger rules each one
/// breaks move to the ID and PR the hdr-9–13 plan's evidence-rule
/// classification gives them.
pub static NATIVE_MUTATIONS: &[NativeMutation] = &[
    NativeMutation {
        patch: "consumed-locator-permit",
        class: NativeClass::Moved {
            property: TEARDOWN,
            case: "consumed-locator-permit",
        },
    },
    NativeMutation {
        patch: "cooldown-session-gate",
        class: NativeClass::Moved {
            property: PENALIZED,
            case: "cooldown-session-gate",
        },
    },
    NativeMutation {
        patch: "dropped-owner-check",
        class: NativeClass::Moved {
            property: STALE,
            case: "dropped-owner-check",
        },
    },
    NativeMutation {
        patch: "foreign-leases",
        class: NativeClass::Moved {
            property: COMMIT,
            case: "foreign-leases",
        },
    },
    NativeMutation {
        patch: "forgotten-locator-permit",
        class: NativeClass::Moved {
            property: TEARDOWN,
            case: "forgotten-locator-permit",
        },
    },
    NativeMutation {
        patch: "late-fault-unpenalized",
        class: NativeClass::Moved {
            property: PENALIZED,
            case: "late-fault-unpenalized",
        },
    },
    // Duplicates of a planted case: the same mutation one call apart,
    // first failing with the same finding at the same step.
    NativeMutation {
        patch: "misstored-page",
        class: NativeClass::Duplicate {
            property: COMMIT,
            case: "stored-order",
        },
    },
    to_move("own9-join", COMMIT, "hdr-9c.1a"),
    NativeMutation {
        patch: "owner-swap",
        class: NativeClass::Moved {
            property: COMMIT,
            case: "owner-swap",
        },
    },
    NativeMutation {
        patch: "raw-budget-lease",
        class: NativeClass::Integrity {
            history: PROBE_ACCOUNTING,
            diagnostic: "OwnedMismatch",
            not_applicable: &[],
        },
    },
    NativeMutation {
        patch: "raw-budget-reservation",
        class: NativeClass::Integrity {
            history: PROBE_ACCOUNTING,
            diagnostic: "ReservedMismatch",
            not_applicable: &[],
        },
    },
    NativeMutation {
        patch: "readmission-unchecked",
        class: NativeClass::Retired {
            reason: "its README says the simulator can't reach the readmission check, so it \
                     stays a zakura-network unit test",
            // The README requires the 60 s cooldown's event assertion to
            // fail, seeing `SessionAdmitted` for session 2 after
            // `CooldownStarted`.
            witness: UnitWitness {
                test: TestRef {
                    package: "zakura-network",
                    file: "src/zakura/header_sync/reactor/tests/observer.rs",
                    test: "zakura::header_sync::reactor::tests::observer::\
                           a_late_fault_cools_down_the_peer_and_refuses_its_reconnect",
                },
                failure: "assertion `left == right` failed: cooldown 60s",
                shows: &["CooldownStarted", "SessionAdmitted", "session_id: 2"],
            },
        },
    },
    NativeMutation {
        patch: "reordered-payload",
        class: NativeClass::Moved {
            property: COMMIT,
            case: "reordered-payload",
        },
    },
    NativeMutation {
        patch: "secondary-reservation",
        class: NativeClass::Moved {
            property: OUTSTANDING,
            case: "secondary-reservation",
        },
    },
    NativeMutation {
        patch: "session-view",
        class: NativeClass::Moved {
            property: "header-sync/peer-retirement-progress",
            case: "session-view",
        },
    },
    NativeMutation {
        patch: "short-cooldown",
        class: NativeClass::Moved {
            property: PENALIZED,
            case: "short-cooldown",
        },
    },
    NativeMutation {
        patch: "short-payload",
        class: NativeClass::Duplicate {
            property: COMMIT,
            case: "dropped-payload-header",
        },
    },
    NativeMutation {
        patch: "shutdown-kept-queue",
        class: NativeClass::Moved {
            property: TEARDOWN,
            case: "shutdown-kept-queue",
        },
    },
    NativeMutation {
        patch: "skewed-remaining",
        class: NativeClass::Moved {
            property: COMMIT,
            case: "skewed-remaining",
        },
    },
    NativeMutation {
        patch: "skipped-retention",
        class: NativeClass::Duplicate {
            property: OUTSTANDING,
            case: "retention-skipped",
        },
    },
    NativeMutation {
        patch: "skipped-staging",
        class: NativeClass::Moved {
            property: COMMIT,
            case: "skipped-staging",
        },
    },
    NativeMutation {
        patch: "stale-locator-round",
        class: NativeClass::Moved {
            property: RESET,
            case: "stale-locator-round",
        },
    },
    NativeMutation {
        patch: "stale-status-cache",
        class: NativeClass::Moved {
            property: "header-sync/state-publication-consistent",
            case: "stale-status-cache",
        },
    },
    NativeMutation {
        patch: "swapped-payload",
        class: NativeClass::Moved {
            property: COMMIT,
            case: "swapped-payload",
        },
    },
    NativeMutation {
        patch: "unreleased-retirement",
        class: NativeClass::Moved {
            property: "header-sync/peer-retirement-progress",
            case: "unreleased-retirement",
        },
    },
    NativeMutation {
        patch: "unsent-tip",
        class: NativeClass::Moved {
            property: "header-sync/state-publication-consistent",
            case: "unsent-tip",
        },
    },
];

/// The native mutations' expected results: an `IntegrityCaught` for gate 6,
/// a transitional `Open` until the PR that moves it, for a retired one the
/// `UnitCaught` of its witness, or, for a duplicate, the `Caught` among
/// `expected` of the planted case covering it, on gate 6. A moved one has
/// no key, and nor has a duplicate whose case no `Caught` names, so its key
/// blocks.
pub fn native_results(expected: &[ExpectedResult]) -> Vec<ExpectedResult> {
    NATIVE_MUTATIONS
        .iter()
        .filter_map(|mutation| {
            let key = vec![Key::Native {
                patch: mutation.patch.to_owned(),
            }];
            let expected = match mutation.class {
                NativeClass::ToMove { until, .. } => native_open(until),
                NativeClass::Integrity {
                    history,
                    diagnostic,
                    not_applicable,
                } => Expected::IntegrityCaught {
                    patch: mutation.patch,
                    history,
                    diagnostic,
                    not_applicable,
                },
                NativeClass::Retired { witness, .. } => Expected::UnitCaught {
                    patch: mutation.patch,
                    witness,
                },
                NativeClass::Duplicate { property, case } => expected
                    .iter()
                    .find(|result| {
                        matches!(result.expected, Expected::Caught { case: c, .. }
                            if c.property == property && c.name == case)
                    })?
                    .expected
                    .clone(),
                NativeClass::Moved { .. } => return None,
            };
            Some(result(key, &[Gate::G6], expected))
        })
        .collect()
}

/// The file of the static checks and the evaluator's controls.
pub const ACCEPTANCE_TESTS: &str = "tests/header_acceptance.rs";

/// The static checks and the evaluator's tests, each a required `Passes` at
/// the subject commit, since the nightly job that also runs them isn't bound
/// to it. Kept apart from the tests themselves, so a deleted test fails the
/// static check that every listed test exists.
pub static COMPOSITION_CHECKS: &[&str] = &[
    "the_rows_equal_the_registered_header_ids",
    "the_header_harness_list_is_complete",
    "the_registrations_equal_the_harness_matrix",
    "the_matrix_check_refuses_one_id_moved_between_harnesses",
    "every_required_key_is_covered_once_with_its_gates",
    "the_rows_are_consistent_with_coverage",
    "every_history_names_its_case",
    "every_planted_case_is_named_by_one_row_of_its_kind",
    "a_planted_case_s_harness_is_read_as_the_runner_reads_it",
    "a_test_s_module_path_is_counted_from_its_target",
    "the_planted_scan_reads_nested_properties",
    "every_native_mutation_is_classified",
    "standing_exclusions_bind_to_their_key",
    "scripted_bypasses_never_satisfy_a_catch",
    "every_referenced_test_exists",
    "every_nested_set_is_allowed_by_nests",
    "the_contracts_table_has_one_row_per_required_contract",
    "every_narrowing_entry_has_a_test_or_its_pr",
    "every_transitional_row_names_an_open_pr",
    "the_gate_requirements_match_their_obligations",
    "every_header_harness_has_its_needs_read",
    "every_row_judges_a_pinned_history",
    "each_result_identity_names_one_expectation",
    "the_composition_checks_are_every_static_check",
    "the_evaluator_refuses_each_single_defect",
    "the_evaluator_refuses_manifest_defects",
    "the_evaluator_accepts_genuine_results",
    "the_evaluator_applies_contracts_and_narrowing",
    "the_evaluator_judges_a_harness_caught_case",
    "the_evaluator_joins_on_full_identity",
    "the_evaluator_decides_contracts_through_their_keys",
    "the_evaluator_binds_runners_and_traces",
    "the_evaluator_accepts_real_storage_and_release_wake_histories",
    "the_not_applicable_inventory_lists_every_reason",
    "the_narrowing_rules_match_their_pin",
];

/// The [`COMPOSITION_CHECKS`] as expected results.
pub fn composition_results() -> Vec<ExpectedResult> {
    COMPOSITION_CHECKS
        .iter()
        .map(|&test| {
            result(
                vec![Key::Composition {
                    test: test.to_owned(),
                }],
                NONE,
                Expected::Passes {
                    test: sim_test(ACCEPTANCE_TESTS, test),
                },
            )
        })
        .collect()
}

/// The `COVERAGE` witness tests as expected results, one per test.
pub fn witness_results(rows: &[Row]) -> Vec<ExpectedResult> {
    witness_tests()
        .into_iter()
        .map(|test| {
            // A witness test serves the gates of every row whose COVERAGE
            // entry names it, by those rows' own tags.
            let mut gates: Vec<Gate> = Vec::new();
            for coverage in crate::harness::coverage::COVERAGE {
                // A COVERAGE witness is a zakura-sim test, joined on its
                // whole TestRef.
                let witness = coverage.passing.witness();
                let named = sim_test(witness.file, witness.test) == test
                    || matches!(coverage.bypass, crate::harness::coverage::Bypass::Test(w)
                        if sim_test(w.file, w.test) == test);
                if !named {
                    continue;
                }
                if let Some(row) = rows.iter().find(|row| row.property == coverage.property) {
                    for result in &row.expected {
                        if result.keys.contains(&Key::passing(row.property)) {
                            gates.extend(result.gates.iter().copied());
                        }
                    }
                }
            }
            gates.sort();
            gates.dedup();
            result(
                vec![Key::Witness {
                    package: test.package.to_owned(),
                    file: test.file.to_owned(),
                    test: test.test.to_owned(),
                }],
                &gates,
                Expected::Passes { test },
            )
        })
        .collect()
}

/// The file of the missing-source tests.
const SOURCE_TESTS: &str = "tests/header_acceptance_sources.rs";

/// The test that drops each source `harness`'s properties need, one at a
/// time, or `None` for a machine, which builds its view in its own driver
/// with no source to drop.
pub fn missing_source_test(harness: &str) -> Option<TestRef> {
    let test = match harness {
        SMOKE => "smoke_fails_without_each_source",
        STORAGE => "storage_fails_without_each_source",
        RELEASE_WAKE => "release_wake_fails_without_each_source",
        WIRE => "wire_fails_without_each_source",
        FLOOD => "flood_fails_without_each_source",
        REPAIR => "repair_fails_without_each_source",
        _ => return None,
    };
    Some(sim_test(SOURCE_TESTS, test))
}

/// The missing-source tests' expected results. Each node harness's test
/// requires a harness failure naming each dropped source with no property
/// `Held`, so it covers each of its harness's keys. A machine's keys are a
/// standing `Open` awaiting approval.
pub fn missing_source_results(keys: &[(String, String)]) -> Vec<ExpectedResult> {
    let mut by_harness: std::collections::BTreeMap<&str, Vec<Key>> = Default::default();
    for (harness, source) in keys {
        by_harness
            .entry(harness)
            .or_default()
            .push(Key::MissingSource {
                harness: harness.clone(),
                source: source.clone(),
            });
    }
    by_harness
        .into_iter()
        .map(|(harness, keys)| {
            let expected = match missing_source_test(harness) {
                Some(test) => Expected::Passes { test },
                None => Expected::Open {
                    reason: "a machine builds its one property's view in its own driver, so it \
                             has no source a test could drop",
                    owner: "hdr-14",
                    approval: Approval::Pending,
                    until: None,
                },
            };
            result(keys, &[Gate::G6], expected)
        })
        .collect()
}

/// The `NotApplicable` reasons' expected results, one per reason a header
/// ID can carry ([`super::reasons::NA_REASONS`]).
pub fn reason_results() -> Vec<ExpectedResult> {
    super::reasons::NA_REASONS
        .iter()
        .map(|entry| {
            result(
                vec![Key::NaReason {
                    reason: entry.reason,
                }],
                &[Gate::G6],
                REASON_TEST,
            )
        })
        .collect()
}

/// A `NotApplicable` reason's test, until E1's tests show each at its
/// trigger.
pub const REASON_TEST: Expected = Expected::Open {
    reason: "a test that shows this reason at its trigger",
    owner: "Adam's side",
    approval: Approval::Pending,
    until: Some("E1"),
};

/// Each non-`Real` tier exclusion Adam has approved, by its own
/// `(property, tier)` key, so approving one rule's exclusion approves no
/// other. None is approved on `main`.
pub static TIER_APPROVALS: &[(&str, Tier, Approval)] = &[];

/// Each result that covers a header rule in a tier other than its own, by
/// `(property, tier)`, such as an owning crate's table suite that checks
/// the whole rule. None exists on `main`, so every such key is an
/// exclusion.
pub static TIER_TESTS: &[(&str, Tier, TestRef)] = &[];

/// Why `tier` has no result for `property` yet. Real has the plan's
/// approved exclusion. The others await approval, rule by rule.
pub fn tier_exclusion(property: &str, tier: Tier) -> Expected {
    tier_exclusion_in(TIER_APPROVALS, property, tier)
}

/// [`tier_exclusion`] with `approvals` in place of [`TIER_APPROVALS`]. The
/// `Real` exclusion is the plan's, for every rule; any other is approved
/// only by its own entry.
pub fn tier_exclusion_in(
    approvals: &[(&str, Tier, Approval)],
    property: &str,
    tier: Tier,
) -> Expected {
    let own = approvals
        .iter()
        .find(|(p, t, _)| *p == property && *t == tier)
        .map_or(Approval::Pending, |(_, _, approval)| *approval);
    let (reason, approval) = match tier {
        Tier::Real => (
            "properties.md allows only safety properties over peer-visible events on Real, and \
             nothing runs header properties there yet",
            PLAN,
        ),
        Tier::Table if property.starts_with("header-sync/s5v9/") => (
            "model_agreement compares the regulation primitives' admit-or-refuse answers with \
             the models, but checks no content validation, closure or punishment, so it's \
             supporting evidence only",
            own,
        ),
        Tier::Table => (
            "no table suite checks this rule whole, consequence included",
            own,
        ),
        Tier::Machine => ("no machine harness checks this rule", own),
        Tier::Sim => (
            "this machine's model has no node-harness property of its own",
            own,
        ),
    };
    Expected::Open {
        reason,
        owner: "hdr-14",
        approval,
        until: None,
    }
}

/// Every header ID's entry for each tier other than its own.
pub fn tier_results(registered: &[super::Registered]) -> Vec<ExpectedResult> {
    let mut seen = std::collections::BTreeSet::new();
    let mut results = Vec::new();
    for registration in registered {
        if !seen.insert(registration.property.clone()) {
            continue;
        }
        let own = if registration.machine {
            Tier::Machine
        } else {
            Tier::Sim
        };
        for tier in super::TIERS.into_iter().filter(|tier| *tier != own) {
            results.push(result(
                vec![Key::Tier {
                    property: registration.property.clone(),
                    tier: format!("{tier:?}"),
                }],
                &[Gate::G6],
                tier_exclusion(&registration.property, tier),
            ));
        }
    }
    results
}
