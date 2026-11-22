//! Header-sync fuzz inputs that tests build in code or find by a bounded
//! search, instead of reading a committed seed.
//!
//! A searched input is a scenario's prefix, then the first Plan of a fixed
//! stream whose run reaches what a test needs: each named property's
//! trigger history, with the property held. The stream is
//! [`byte_inputs`](super::byte_inputs), so every search finds the same
//! input on every run. A search that finds none panics, naming what it
//! looked for.

use std::sync::OnceLock;

use zakura_sim::{
    domain::{
        header_sync::{
            CHUNK_BUDGET_BOUND, COMMIT_ONLY_AUTHORIZED, OUTSTANDING_WORK_BOUNDED,
            STALE_COMPLETION_INERT, TEARDOWN_RETURNS_BASELINE,
        },
        session::HONEST_NEVER_PUNISHED,
    },
    harness::{
        self,
        header_sync::{
            grammar, HeaderWire, RepairHarness, RepairScenario, Storage, StorageScenario,
        },
        wire_plans::SavedPlan,
        HarnessEntry, Subsystem,
    },
    witness::Expect,
    NullSink, Plan,
};

use super::byte_inputs;

/// How many inputs the [`byte_inputs`] stream behind [`storage_plans`]
/// holds.
const STORAGE_STREAM_INPUTS: usize = 64;

/// The longest input of that stream, and the length of each storage Plan.
pub const STORAGE_PLAN_BYTES: usize = 1_024;

/// The `header-sync/storage` Plans a search tries, in order: the
/// [`byte_inputs`] stream of [`STORAGE_STREAM_INPUTS`] inputs, joined and
/// cut into [`STORAGE_PLAN_BYTES`]-byte slices.
pub fn storage_plans() -> Vec<Vec<u8>> {
    byte_inputs(STORAGE_STREAM_INPUTS, STORAGE_PLAN_BYTES)
        .concat()
        .as_chunks::<STORAGE_PLAN_BYTES>()
        .0
        .iter()
        .map(|plan| plan.to_vec())
        .collect()
}

/// `scenario`'s canonical `header-sync/storage` prefix, then `plan`.
pub fn storage_input(scenario: &StorageScenario, plan: &[u8]) -> Vec<u8> {
    grammar::input(scenario, plan).unwrap_or_else(|error| panic!("{error}"))
}

/// Whether `entry`'s witness of each of `properties` holds on the full fuzz
/// `input`, reaching the property's trigger history.
pub fn holds(entry: &HarnessEntry, input: &[u8], properties: &[&str]) -> bool {
    let plan = &input[input.len().min(entry.scenario_bytes)..];
    let scenario = entry.scenario(input);
    properties.iter().all(|property| {
        (entry.witness)(
            &scenario,
            Plan::from_fuzz(plan),
            property,
            Expect::Holds,
            &[],
            &mut NullSink,
        )
        .is_ok()
    })
}

/// The first of `candidates` that `accept` takes. Panics, naming `what`, if
/// none does.
pub fn first(
    what: &str,
    candidates: impl IntoIterator<Item = Vec<u8>>,
    mut accept: impl FnMut(&[u8]) -> bool,
) -> Vec<u8> {
    candidates
        .into_iter()
        .find(|input| accept(input))
        .unwrap_or_else(|| panic!("no searched input reaches {what}"))
}

/// `scenario` under the first of [`storage_plans`] on which every one of
/// `properties` holds and reaches its trigger history.
pub fn search_storage(what: &str, scenario: &StorageScenario, properties: &[&str]) -> Vec<u8> {
    let entry = harness::find(Storage::NAME).expect("the storage harness is registered");
    first(
        what,
        storage_plans()
            .iter()
            .map(|plan| storage_input(scenario, plan)),
        |input| holds(entry, input, properties),
    )
}

/// The close-and-relink input: [`StorageScenario::close_relink`], whose
/// node reserves headers from both mirrors and releases rounds as its
/// mirrors close and finish, under the first Plan on which
/// `chunk-budget-bound`, `teardown-returns-baseline` and
/// `outstanding-work-bounded` all hold and reach their trigger histories.
/// The process keeps it.
pub fn close_relink_input() -> &'static [u8] {
    static INPUT: OnceLock<Vec<u8>> = OnceLock::new();
    INPUT.get_or_init(|| {
        search_storage(
            "the close-and-relink histories",
            &StorageScenario::close_relink(),
            &[
                CHUNK_BUDGET_BOUND,
                TEARDOWN_RETURNS_BASELINE,
                OUTSTANDING_WORK_BOUNDED,
            ],
        )
    })
}

/// The stale-prepare input: [`StorageScenario::triggers`] under the first
/// Plan whose node ends a call stale, so `stale-completion-inert` applies
/// and holds. Its node ends a prepare after its round retired, while the
/// same peer's next round is active. The process keeps it.
pub fn stale_prepare_input() -> &'static [u8] {
    static INPUT: OnceLock<Vec<u8>> = OnceLock::new();
    INPUT.get_or_init(|| {
        search_storage(
            "a stale call",
            &StorageScenario::triggers(),
            &[STALE_COMPLETION_INERT],
        )
    })
}

/// How many Plans [`wire_input`]'s honest-sync search tries after the empty
/// one.
const WIRE_SEARCH_PLANS: usize = 8;

/// The longest Plan [`wire_input`]'s honest-sync search tries.
const WIRE_PLAN_BYTES: usize = 16_384;

/// Saved Plan `plan`'s `header-sync/wire` input: its tag, then the empty
/// Plan, which the saved scenario's script needs no more than. The honest
/// sync instead takes the first of the empty Plan and
/// [`WIRE_SEARCH_PLANS`] drawn ones on which `session/honest-never-punished`
/// holds and applies.
pub fn wire_input(plan: SavedPlan) -> Vec<u8> {
    if plan != SavedPlan::HonestSync {
        return plan.tag().to_vec();
    }
    static INPUT: OnceLock<Vec<u8>> = OnceLock::new();
    INPUT
        .get_or_init(|| {
            let entry = harness::find(HeaderWire::NAME).expect("the wire harness is registered");
            let plans =
                std::iter::once(Vec::new()).chain(byte_inputs(WIRE_SEARCH_PLANS, WIRE_PLAN_BYTES));
            first(
                "an honest sync",
                plans.map(|bytes| [&plan.tag()[..], &bytes].concat()),
                |input| holds(entry, input, &[HONEST_NEVER_PUNISHED]),
            )
        })
        .clone()
}

/// How many Plans [`repair_input`]'s search tries.
const REPAIR_SEARCH_PLANS: usize = 16;

/// The longest Plan [`repair_input`]'s search tries.
const REPAIR_PLAN_BYTES: usize = 256;

/// `scenario`'s scenario byte on `header-sync/repair`.
pub fn repair_byte(scenario: RepairScenario) -> u8 {
    match scenario {
        RepairScenario::Applied => 0,
        RepairScenario::PrepareDeparts => 1,
        RepairScenario::ApplyDeparts => 2,
        RepairScenario::SupplierPruned => 3,
    }
}

/// The properties a `header-sync/repair` input must reach in `scenario`:
/// `outstanding-work-bounded` in every fixture, and in each repairing one
/// `stale-completion-inert` and `commit-only-authorized` too.
pub fn repair_properties(scenario: RepairScenario) -> &'static [&'static str] {
    match scenario {
        RepairScenario::SupplierPruned => &[OUTSTANDING_WORK_BOUNDED],
        _ => &[
            STALE_COMPLETION_INERT,
            COMMIT_ONLY_AUTHORIZED,
            OUTSTANDING_WORK_BOUNDED,
        ],
    }
}

/// `scenario`'s `header-sync/repair` input: its scenario byte, then the
/// first of [`REPAIR_SEARCH_PLANS`] Plans on which each of
/// [`repair_properties`] holds and reaches its trigger history. Each
/// fixture's history waits on control steps with no deadline, such as the
/// stale peer's close, so a Plan that keeps one waiting until the step
/// budget never reaches the rest of it. The process keeps each input.
pub fn repair_input(scenario: RepairScenario) -> Vec<u8> {
    static INPUTS: [OnceLock<Vec<u8>>; 4] = [
        OnceLock::new(),
        OnceLock::new(),
        OnceLock::new(),
        OnceLock::new(),
    ];
    let byte = repair_byte(scenario);
    INPUTS[usize::from(byte)]
        .get_or_init(|| {
            let entry =
                harness::find(RepairHarness::NAME).expect("the repair harness is registered");
            first(
                &format!("{scenario:?}'s repair history"),
                byte_inputs(REPAIR_SEARCH_PLANS, REPAIR_PLAN_BYTES)
                    .into_iter()
                    .map(|plan| [&[byte][..], &plan].concat()),
                |input| holds(entry, input, repair_properties(scenario)),
            )
        })
        .clone()
}
