//! The `sync` seeds: `sync/wire`'s and `sync/storage`'s saved scenarios and
//! curated combinations, each its tag and the empty Plan, the
//! `sync/storage` seeds whose Plan a search finds or is a fixed draw a
//! search picked, and the `sync/composed` prefixes, each under the empty
//! Plan.

use std::sync::OnceLock;

use super::{draws::byte_input, Seed};
use crate::{
    check::PropertyOutcome,
    domain::cross::{BLOCK_REQUESTS_FOLLOW_HEADERS, HEADER_REORG_RETIRES_BLOCK_WORK},
    explore::generate::{reach, ReachSearch},
    harness::{
        self,
        sync::{SavedSync, SyncStorage},
        sync_wire::{
            SavedWire, SyncComposed, SyncWire, CURATED, SYNC_COMPOSED_BYTES, SYNC_COMPOSED_VERSION,
        },
        Subsystem,
    },
    observe::BlockSyncFact,
    witness::Expect,
    Observation, RunReport,
};

/// The test that runs the `sync/wire` seeds.
const WIRE: &str = "crates/zakura-sim/tests/sync_wire.rs";

/// The test that runs the `sync/storage` seeds.
const STORAGE: &str = "crates/zakura-sim/tests/sync_harness.rs";

/// How many fuzz Plans the `reorg-verdict` search drew from
/// [`super::draws::byte_inputs`], each up to [`SEARCH_PLAN_BYTES`] long.
const SEARCH_CANDIDATES: usize = 64;

/// The longest Plan the `sync/storage` searches draw.
const SEARCH_PLAN_BYTES: usize = 16_384;

/// How many Plans [`reorg_stale_body`]'s search may run. It found its Plan
/// within its first nine runs when this was set, so 256 leaves a wide
/// margin.
const STALE_BODY_RUNS: u64 = 256;

/// `sync-storage-reorg-stale-body`'s full input: the saved `reorg`
/// scenario under the first Plan a search finds whose run
/// [`refuses_a_stale_body`]. `accept-stale-scope-bodies.patch` buffers that
/// body instead.
///
/// The search runs on the unpatched build, so a production change that
/// moves the old Plan off that order finds a new one instead of weakening
/// the planted case. The first call runs it, and later calls in the process
/// reuse its bytes.
fn reorg_stale_body() -> &'static [u8] {
    static INPUT: OnceLock<Vec<u8>> = OnceLock::new();
    INPUT.get_or_init(|| {
        let entry = harness::find(SyncStorage::NAME).expect("sync/storage is registered");
        let search = ReachSearch {
            prefix: SavedSync::Reorg.tag().to_vec(),
            max_bytes: SEARCH_PLAN_BYTES,
            steer: None,
            ..ReachSearch::new(0, STALE_BODY_RUNS)
        };
        let reached = reach(entry, &search, refuses_a_stale_body).unwrap_or_else(|| {
            panic!("no Plan of {STALE_BODY_RUNS} makes the sequencer refuse a stale body")
        });
        reached.input
    })
}

/// Whether `report`'s run fills the stale-body input's role: it violates
/// no property, applies [`HEADER_REORG_RETIRES_BLOCK_WORK`], and has the
/// sequencer refuse a body of an older epoch than the one it applied.
fn refuses_a_stale_body(report: &RunReport) -> Option<()> {
    let violated = report.end.violation.is_some()
        || report
            .properties
            .iter()
            .any(|property| matches!(property.outcome, PropertyOutcome::Violated { .. }));
    let applied = report.properties.iter().any(|property| {
        property.property == HEADER_REORG_RETIRES_BLOCK_WORK
            && matches!(property.outcome, PropertyOutcome::Held { applicable } if applicable > 0)
    });
    let refused = report.log.entries().iter().any(|entry| {
        matches!(
            &entry.obs,
            Observation::BlockSync {
                event: BlockSyncFact::StaleScope { body, scope_epoch },
                ..
            } if body.owner.body_work_epoch.zip(*scope_epoch).is_some_and(|(own, applied)| own < applied)
        )
    });
    (!violated && applied && refused).then_some(())
}

/// `sync-storage-reorg-verdict-republish`'s full input: the saved
/// `reorg-verdict` scenario under the first search candidate that, under
/// `keep-work-queue-authority.patch`, has a request under the first new
/// epoch drop before the second one, and a routine publish its returned
/// work under the first epoch after the sequencer applied the second. Only
/// a patched run shows that order, so the search ran under the patch.
fn reorg_verdict_republish() -> Vec<u8> {
    let plan = byte_input(SEARCH_CANDIDATES, SEARCH_PLAN_BYTES, 8);
    [&SavedSync::ReorgVerdict.tag()[..], &plan].concat()
}

/// A `sync/composed` prefix: two mirrors, the second dialed by the node,
/// on the default chain, then `events`, three bytes each, then zeros.
fn composed(events: &[[u8; 3]]) -> Vec<u8> {
    let mut prefix = vec![SYNC_COMPOSED_VERSION, 0b0000_1001, 0];
    prefix.extend(events.iter().flatten());
    prefix.resize(SYNC_COMPOSED_BYTES, 0);
    prefix
}

/// The `sync/composed` seeds, `sync-composed-<name>`:
///
/// - `honest`: two mirrors serve one chain with no control step.
/// - `close-growth-stall`: mirror 0's connection closes after the second
///   admission, the chain grows, and each mirror range read stalls for
///   3 s.
/// - `reset`: the mirrors reset to a fork at height 3 that ends one above
///   the tip.
/// - `every-step`: every control kind once: two closes, growth, a reset, a
///   held range read and a teardown.
fn composed_seeds() -> Vec<Seed> {
    let targets = "crates/zakura-sim/tests/targets.rs";
    let source = "crates/zakura-sim/tests/sync_composed.rs";
    let every_step = [
        [1, 1, 0b0000_0100],
        [2, 1, 0],
        [3, 1, 0],
        [4, 9, 0],
        [5, 0b0000_0100, 2],
        [7, 0, 0],
        [3, 3, 0],
    ];
    let close_growth_stall = [[1, 0, 0b0000_0101], [3, 0, 0], [5, 0b0000_0100, 1]];
    [
        ("honest", targets, composed(&[])),
        ("close-growth-stall", source, composed(&close_growth_stall)),
        ("reset", targets, composed(&[[4, 9, 0]])),
        ("every-step", targets, composed(&every_step)),
    ]
    .into_iter()
    .map(|(name, source, prefix)| {
        Seed::new(
            format!("sync-composed-{name}"),
            SyncComposed::NAME,
            source,
            prefix,
        )
    })
    .collect()
}

/// `seed` with each of `witnesses`.
fn witnessed(seed: Seed, witnesses: &[(&str, Expect)]) -> Seed {
    witnesses.iter().fold(seed, |seed, (property, expect)| {
        seed.witness(property, *expect)
    })
}

/// Every `sync` seed.
pub(super) fn seeds() -> Vec<Seed> {
    let wire = SavedWire::ALL.into_iter().map(|saved| {
        let seed = Seed::new(
            format!("sync-wire-{}", saved.name()),
            SyncWire::NAME,
            WIRE,
            saved.tag().to_vec(),
        );
        witnessed(seed, saved.witnesses())
    });
    let curated = CURATED.into_iter().map(|(name, prefix)| {
        Seed::new(
            format!("sync-wire-{name}"),
            SyncWire::NAME,
            WIRE,
            prefix.to_vec(),
        )
    });
    let storage = SavedSync::ALL.into_iter().map(|saved| {
        let seed = Seed::new(
            format!("sync-storage-{}", saved.name()),
            SyncStorage::NAME,
            STORAGE,
            saved.tag().to_vec(),
        );
        witnessed(seed, saved.witnesses())
    });
    let searched = [
        Seed::built(
            "sync-storage-reorg-stale-body",
            SyncStorage::NAME,
            STORAGE,
            reorg_stale_body,
        )
        .witness(HEADER_REORG_RETIRES_BLOCK_WORK, Expect::Holds),
        Seed::new(
            "sync-storage-reorg-verdict-republish",
            SyncStorage::NAME,
            STORAGE,
            reorg_verdict_republish(),
        )
        .witness(BLOCK_REQUESTS_FOLLOW_HEADERS, Expect::Holds),
    ];
    wire.chain(curated)
        .chain(storage)
        .chain(searched)
        .chain(composed_seeds())
        .collect()
}
