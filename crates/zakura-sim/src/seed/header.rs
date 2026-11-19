//! The `header` seeds: `header-sync/storage`'s scenario seeds and witness
//! seeds, `header-sync/wire`'s saved Plans, the one-byte fixtures of
//! `header-sync/release-wake`, `header-sync/repair` and `header-sync/flood`,
//! and the two header machines' op tapes.

use super::{draws, Seed};
use crate::{
    domain::{
        header_sync::{
            CAPACITY_RELEASE_WAKES, CHUNK_BUDGET_BOUND, COMMIT_ONLY_AUTHORIZED,
            OUTSTANDING_WORK_BOUNDED, STALE_COMPLETION_INERT, TEARDOWN_RETURNS_BASELINE,
            WATCH_OBSERVED_UNDER_FLOOD, WATCH_OBSERVED_UNDER_LIFECYCLE_FLOOD,
        },
        header_sync_progress::HONEST_PROGRESS,
    },
    harness::{
        header_sync::{
            grammar, storage_witnesses as witnesses, Flood, FloodScenario, HeaderWire, ReleaseWake,
            RepairHarness, RepairScenario, Storage, StorageScenario, WakeScenario,
        },
        wire_plans::SavedPlan,
        Subsystem,
    },
    peer::adversary::{Act, ContentRule},
    witness::Expect,
};

/// Every `header` seed.
pub(super) fn seeds() -> Vec<Seed> {
    let mut seeds = storage_seeds();
    seeds.extend(witness_seeds());
    seeds.extend(wire_seeds());
    seeds.extend(one_byte_seeds());
    seeds.extend(machine_seeds());
    seeds
}

/// The test that runs the scenario seeds.
const STORAGE_SOURCE: &str = "crates/zakura-sim/tests/header_storage_seeds.rs";

/// The Plan bytes after each storage seed's prefix.
const STORAGE_PLAN_BYTES: usize = 1_024;

/// Slice `index` of the [`draws::byte_inputs`] stream, cut into
/// [`STORAGE_PLAN_BYTES`]-byte slices: a storage seed's Plan.
fn storage_plan(index: usize) -> Vec<u8> {
    let stream = draws::byte_inputs(64, 1_024).concat();
    stream[index * STORAGE_PLAN_BYTES..(index + 1) * STORAGE_PLAN_BYTES].to_vec()
}

/// A `header-sync/storage` seed: `scenario`'s canonical prefix, then `plan`.
fn storage(name: &str, source: &str, scenario: &StorageScenario, plan: &[u8]) -> Seed {
    let input = grammar::input(scenario, plan).unwrap_or_else(|error| panic!("{name}: {error}"));
    Seed::new(name, Storage::NAME, source, input)
}

/// `header-sync/storage`'s scenario seeds, each a scenario under a slice of
/// the stream, so a fuzz campaign starts from inputs that use the whole
/// action grammar. `header-storage-stale-prepare` is the trigger scenario
/// under the slice whose run ends a prepare after its round retired, while
/// the same peer's next round is active. The locator-dropped, apply-at-close
/// and competing-mirrors seeds are witnesses `tests/explore_state.rs` draws.
/// `header-sync-storage-default` is the default scenario's bare prefix.
fn storage_seeds() -> Vec<Seed> {
    let scenarios: [(&str, StorageScenario, usize); 9] = [
        ("header-storage-default", StorageScenario::default(), 0),
        (
            "header-storage-close-relink",
            StorageScenario::close_relink(),
            1,
        ),
        ("header-storage-grow", StorageScenario::grow(), 2),
        ("header-storage-holds", StorageScenario::holds(), 3),
        ("header-storage-triggers", StorageScenario::triggers(), 4),
        (
            "header-storage-competing-mirrors",
            witnesses::competing_mirrors(),
            10,
        ),
        (
            "header-storage-stale-prepare",
            StorageScenario::triggers(),
            11,
        ),
        (
            "header-storage-locator-dropped",
            witnesses::dropped_locator(),
            12,
        ),
        (
            "header-storage-apply-at-close",
            witnesses::apply_at_close(),
            13,
        ),
    ];
    let mut seeds: Vec<Seed> = scenarios
        .into_iter()
        .map(|(name, scenario, slice)| {
            let seed = storage(name, STORAGE_SOURCE, &scenario, &storage_plan(slice));
            match name {
                "header-storage-close-relink" => seed
                    .witness(CHUNK_BUDGET_BOUND, Expect::Holds)
                    .witness(TEARDOWN_RETURNS_BASELINE, Expect::Holds),
                "header-storage-stale-prepare" => {
                    seed.witness(STALE_COMPLETION_INERT, Expect::Holds)
                }
                _ => seed,
            }
        })
        .collect();
    seeds.push(storage(
        "header-sync-storage-default",
        STORAGE_SOURCE,
        &StorageScenario::default(),
        &[],
    ));
    seeds
}

/// fuzz-12 item 7's witness seeds, `header-storage-w-*`: each action and
/// overlap the fuzz-12 controls added, under the slice whose run shows it.
/// Most use slice 16. The hold, the reconsider, the release and two forks
/// use a slice whose run orders their overlap that way.
fn witness_seeds() -> Vec<Seed> {
    type Named = (&'static str, fn() -> StorageScenario, usize);
    let named: [Named; 15] = [
        ("hold-succeeds", StorageScenario::holds, 2),
        ("reset-locator", witnesses::reset_held_locator, 16),
        ("reset-prepare", witnesses::reset_held_prepare, 16),
        ("reset-apply", witnesses::reset_held_apply, 16),
        ("reset-reply", witnesses::reset_reply_hold, 16),
        ("reconsider", witnesses::invalidate_reconsider, 20),
        ("release", witnesses::release_after_apply, 17),
        ("reply-hold", witnesses::reply_hold, 16),
        ("reply-after-shutdown", witnesses::reply_after_shutdown, 16),
        ("reply-after-close", witnesses::reply_after_close, 16),
        ("stop-storage", witnesses::stop_with_held_apply, 16),
        ("peer-lease-denied", witnesses::peer_lease_denied, 16),
        ("equal-forks", witnesses::equal_forks, 16),
        ("longer-fork", witnesses::longer_fork, 17),
        ("weighted-fork", witnesses::weighted_fork, 17),
    ];
    let mut scenarios: Vec<(String, StorageScenario, usize)> = named
        .into_iter()
        .map(|(name, scenario, slice)| (format!("header-storage-w-{name}"), scenario(), slice))
        .collect();
    for (kind, allow, outcome, after) in witnesses::fault_witnesses() {
        scenarios.push((
            witnesses::fault_stem(kind, outcome),
            witnesses::fault_witness(kind, allow, after),
            16,
        ));
    }
    for (prefix, classes) in [
        ("recover", witnesses::recovery_witnesses()),
        ("owed", witnesses::owed_recovery_witnesses()),
    ] {
        for (class, scenario) in classes {
            let name = format!("header-storage-w-{prefix}-{}", class.replace(' ', "-"));
            scenarios.push((name, scenario, 16));
        }
    }
    let source = "crates/zakura-sim/tests/header_storage_witness_seeds.rs";
    scenarios
        .into_iter()
        .map(|(name, scenario, slice)| storage(&name, source, &scenario, &storage_plan(slice)))
        .collect()
}

/// `header-sync/wire`'s saved Plans, `header-wire-<name>`: each Plan's tag,
/// then the honest sync's 16 KiB draw, or no bytes for the rest, which runs
/// the default Plan. The progress fixture and the three refused-page acts
/// are planted cases' inputs, so each carries a witness expecting a hold.
fn wire_seeds() -> Vec<Seed> {
    SavedPlan::ALL
        .into_iter()
        .map(|plan| {
            let mut input = plan.tag().to_vec();
            if plan == SavedPlan::HonestSync {
                input.extend(draws::byte_input(1, 16_384, 0));
            }
            let seed = Seed::new(
                format!("header-wire-{}", plan.name()),
                HeaderWire::NAME,
                "crates/zakura-sim/tests/header_wire_lifecycle.rs",
                input,
            );
            match plan {
                SavedPlan::HonestProgress => seed.witness(HONEST_PROGRESS, Expect::Holds),
                SavedPlan::Act(
                    Act::BadContent {
                        rule: ContentRule::Linkage | ContentRule::LowerLocator,
                    }
                    | Act::OverCreditObjects,
                ) => seed.witness(COMMIT_ONLY_AUTHORIZED, Expect::Holds),
                _ => seed,
            }
        })
        .collect()
}

/// A one-byte-prefix seed: `byte`, then input `index` of
/// [`draws::byte_inputs`]`(count, max_len)`.
fn one_byte(name: &str, harness: &str, source: &str, byte: u8, draw: [usize; 3]) -> Seed {
    let [count, max_len, index] = draw;
    let mut input = vec![byte];
    input.extend(draws::byte_input(count, max_len, index));
    Seed::new(name, harness, source, input)
}

/// The release-wake, repair and flood seeds: each fixture's scenario byte,
/// then a drawn Plan.
///
/// - **Release wake.** The release Plan is the first tested one whose
///   release is drained at the top of the reactor's loop. Any Plan defers
///   in the deferred fixture.
/// - **Repair.** Each fixture's history waits on control steps with no
///   deadline, and the one Plan reaches every fixture's history.
/// - **Flood.** Each Plan is one whose flooded queue competes with the
///   commit's watch read.
fn one_byte_seeds() -> Vec<Seed> {
    let wake = |name, scenario: WakeScenario, draw| {
        let byte = match scenario {
            WakeScenario::Release => 0,
            WakeScenario::Deferred => 1,
        };
        let source = "crates/zakura-sim/tests/header_release_wake.rs";
        one_byte(name, ReleaseWake::NAME, source, byte, draw)
            .witness(CAPACITY_RELEASE_WAKES, Expect::Holds)
    };
    let repair = |name, scenario: RepairScenario| {
        let byte = match scenario {
            RepairScenario::Applied => 0,
            RepairScenario::PrepareDeparts => 1,
            RepairScenario::ApplyDeparts => 2,
            RepairScenario::SupplierPruned => 3,
        };
        let source = "crates/zakura-sim/tests/header_repair.rs";
        let seed = one_byte(name, RepairHarness::NAME, source, byte, [4, 256, 0]);
        // The pruned supplier never repairs, so only the bound applies.
        let seed = match scenario {
            RepairScenario::SupplierPruned => seed,
            _ => seed
                .witness(STALE_COMPLETION_INERT, Expect::Holds)
                .witness(COMMIT_ONLY_AUTHORIZED, Expect::Holds),
        };
        seed.witness(OUTSTANDING_WORK_BOUNDED, Expect::Holds)
    };
    let flood = |name, scenario: FloodScenario, index| {
        let (byte, property) = match scenario {
            FloodScenario::Wire => (0, WATCH_OBSERVED_UNDER_FLOOD),
            FloodScenario::Lifecycle => (1, WATCH_OBSERVED_UNDER_LIFECYCLE_FLOOD),
        };
        let source = "crates/zakura-sim/tests/header_flood_watch.rs";
        one_byte(name, Flood::NAME, source, byte, [24, 256, index]).witness(property, Expect::Holds)
    };
    vec![
        wake(
            "header-release-wake",
            WakeScenario::Release,
            [400, 512, 155],
        ),
        wake(
            "header-release-wake-deferred",
            WakeScenario::Deferred,
            [16, 256, 0],
        ),
        repair("header-repair-applied", RepairScenario::Applied),
        repair(
            "header-repair-prepare-departs",
            RepairScenario::PrepareDeparts,
        ),
        repair("header-repair-apply-departs", RepairScenario::ApplyDeparts),
        repair(
            "header-repair-supplier-pruned",
            RepairScenario::SupplierPruned,
        ),
        flood("header-flood-wire", FloodScenario::Wire, 14),
        flood("header-flood-lifecycle", FloodScenario::Lifecycle, 2),
    ]
}

/// The header publisher's and reactor's op tapes, each four bytes per
/// operation, as the machines' own tests in `zakura-network` write them.
fn machine_seeds() -> Vec<Seed> {
    let source = "crates/zakura-sim/tests/replay_exact.rs";
    // The publisher machine's `WITNESS` in `publisher/machine/tests.rs`:
    // open, grant, read a page, close, end, take and write the outcome, then
    // a stale page.
    let publisher = vec![
        0, 0, 4, 35, 10, 0, 0, 0, 15, 0, 0, 0, 8, 0, 0, 0, 23, 0, 0, 0, 24, 0, 0, 0, 25, 0, 0, 0,
        15, 0, 0, 128,
    ];
    // The reactor machine's `WITNESS` in `reactor/machine_tests.rs`: two
    // admissions, reads started and refused, stale completions, a close
    // refused, retried and expired, a cached pass, a resource stall, a
    // commit, a repair and its stall, refresh stalls and a rotation.
    let reactor = vec![
        0, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 1, 3, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 2, 3, 0,
        0, 0, 4, 0, 0, 0, 4, 1, 0, 0, 7, 0, 0, 1, 6, 0, 232, 3, 8, 0, 0, 0, 7, 0, 1, 1, 6, 0, 136,
        19, 8, 0, 0, 0, 9, 0, 0, 0, 10, 2, 0, 0, 11, 3, 0, 0, 13, 0, 0, 0, 14, 0, 0, 0, 15, 0, 0,
        0, 6, 0, 96, 234, 15, 0, 0, 0, 15, 0, 0, 0, 12, 0, 0, 0,
    ];
    let publisher_policy = "header/publisher-policy";
    let reactor_policy = "header/reactor-policy";
    vec![
        Seed::new(
            "header-publisher-policy",
            publisher_policy,
            source,
            publisher,
        )
        .witness(publisher_policy, Expect::Holds),
        Seed::new("header-reactor-policy", reactor_policy, source, reactor)
            .witness(reactor_policy, Expect::Holds),
    ]
}
