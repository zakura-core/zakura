//! The `sync` seeds: `sync/wire`'s and `sync/storage`'s saved scenarios and
//! curated combinations, each its tag and the empty Plan, the
//! `sync/storage` seeds whose Plan is a fixed draw a search picked, and
//! zakura#2378's known failure, whose Plan a steered run records.

use std::{collections::BTreeMap, sync::OnceLock};

use super::{draws::byte_input, Seed, SeedSet};
use crate::{
    domain::{
        cross::{BLOCK_REQUESTS_FOLLOW_HEADERS, HEADER_REORG_RETIRES_BLOCK_WORK},
        session::STALLED_SERVICE_ISOLATED,
    },
    harness::{
        run_steered,
        sync::{SavedSync, SyncStorage},
        sync_wire::{SavedWire, SyncWire, COMBINED_TAG, CURATED},
        Subsystem,
    },
    observe::BlockSyncFact,
    witness::Expect,
    Category, Identity, NullSink, Observation, ObservationLog, Plan, Steer,
};

/// The test that runs the `sync/wire` seeds.
const WIRE: &str = "crates/zakura-sim/tests/sync_wire.rs";

/// The test that runs the `sync/storage` seeds.
const STORAGE: &str = "crates/zakura-sim/tests/sync_harness.rs";

/// How many fuzz Plans the `sync/storage` searches drew from
/// [`super::draws::byte_inputs`], each up to [`SEARCH_PLAN_BYTES`] long.
const SEARCH_CANDIDATES: usize = 64;

/// The longest Plan the `sync/storage` searches drew.
const SEARCH_PLAN_BYTES: usize = 16_384;

/// `sync-storage-reorg-stale-body`'s full input: the saved `reorg`
/// scenario under the first search candidate whose run delivers an
/// old-epoch body after the sequencer applied the new epoch, which the
/// sequencer refuses as stale. `accept-stale-scope-bodies.patch` buffers
/// that body instead.
fn reorg_stale_body() -> Vec<u8> {
    let plan = byte_input(SEARCH_CANDIDATES, SEARCH_PLAN_BYTES, 7);
    [&SavedSync::Reorg.tag()[..], &plan].concat()
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

/// `sync-wire-reset-mid-write`'s prefix: fuzz-13's combined actions `FB 14`,
/// the fork while storage holds a submission, with every read stalled.
const RESET_MID_WRITE_PREFIX: [u8; 2] = [COMBINED_TAG, 0b0001_0100];

/// The [`xorshift_plan`] seed whose run under [`WriterLast`] reaches
/// zakura#2378: the first of 200 a search tried that does.
const RESET_MID_WRITE_SEED: u64 = 23;

/// `sync-wire-reset-mid-write`'s full input, which reaches zakura#2378:
/// [`RESET_MID_WRITE_PREFIX`], then the Plan bytes that replay, with no
/// steer, the run of [`RESET_MID_WRITE_SEED`]'s Plan under [`WriterLast`].
/// The steer is what reaches the reset: random Plans alone rarely leave a
/// request mid-write when the fork's reset lands. The fork's tip reset then
/// retires a block request while its frame is partly written, which cancels
/// the whole block-sync session, and the writer fence closes the honest
/// connection with `unfinished_exchange` while the session's other requests
/// wait.
///
/// The steered run takes a few seconds, so the first call runs it and later
/// calls in the process reuse its bytes.
fn reset_mid_write() -> Vec<u8> {
    static INPUT: OnceLock<Vec<u8>> = OnceLock::new();
    INPUT
        .get_or_init(|| {
            let scenario = SyncWire::draw(&RESET_MID_WRITE_PREFIX);
            let ports = SyncWire::prepare(&scenario);
            let input = Plan::from_fuzz(&xorshift_plan(RESET_MID_WRITE_SEED));
            let steer = Box::new(WriterLast::default());
            let (_, plan) = run_steered::<SyncWire>(&scenario, ports, input, &mut NullSink, steer)
                .unwrap_or_else(|error| panic!("the steered zakura#2378 run fails: {error}"));
            [&RESET_MID_WRITE_PREFIX[..], &plan].concat()
        })
        .clone()
}

/// Seed `seed`'s Plan bytes: enough xorshift64 output that no `sync/wire`
/// run exhausts it.
fn xorshift_plan(seed: u64) -> Vec<u8> {
    // An odd state is never zero, and xorshift64 never reaches zero from a
    // nonzero state.
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..400_000)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            // Truncation keeps the low byte, which is all a byte needs.
            state as u8
        })
        .collect()
}

/// A steer that picks each block request's writer last among the ready
/// actions while its frame write is under way, as a slow thread would.
/// Otherwise it lets the Plan pick.
///
/// A writer is the task whose step logged the request's `started` step, and
/// its write is under way until the request's next step. Among the other
/// ready actions, the steer takes the first that sorts after its previous
/// pick, wrapping to the start.
#[derive(Default)]
struct WriterLast {
    /// How many log entries the steer has read.
    read: usize,
    /// Each block request, as `(session, request)`, that started at the step
    /// being read and has no writer yet.
    started: Vec<(u64, u64)>,
    /// Each block request whose write is under way, and its writer's name.
    writing: BTreeMap<(u64, u64), String>,
    /// The steer's previous pick.
    last: Option<Identity>,
}

impl WriterLast {
    /// Reads the log entries after the ones already read.
    fn catch_up(&mut self, log: &ObservationLog) {
        for entry in &log.entries()[self.read..] {
            if let Some(task) = entry.obs.ran_task() {
                for request in self.started.drain(..) {
                    self.writing.insert(request, task.to_owned());
                }
            }
            if let Observation::BlockSync {
                event: BlockSyncFact::Request { owner, step, .. },
                ..
            } = &entry.obs
            {
                let request = (owner.session_id, owner.request_id);
                if step == "started" {
                    self.started.push(request);
                } else {
                    self.writing.remove(&request);
                }
            }
        }
        self.read = log.entries().len();
    }
}

impl Steer for WriterLast {
    /// The first ready action after the previous pick that isn't a writer
    /// mid-write, if any writer is mid-write and ready.
    fn pick(&mut self, options: &[Identity], log: &ObservationLog) -> Option<usize> {
        self.catch_up(log);
        let writing = |option: &Identity| {
            option.category == Category::Task && self.writing.values().any(|w| *w == option.key)
        };
        if !options.iter().any(writing) {
            return None;
        }
        let rest: Vec<usize> = (0..options.len())
            .filter(|&index| !writing(&options[index]))
            .collect();
        let after_last = |&&index: &&usize| {
            self.last.as_ref().is_none_or(|last| {
                (options[index].category, &options[index].key) > (last.category, &last.key)
            })
        };
        let index = *rest.iter().find(after_last).or(rest.first())?;
        self.last = Some(options[index].clone());
        Some(index)
    }
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
        Seed::new(
            "sync-storage-reorg-stale-body",
            SyncStorage::NAME,
            STORAGE,
            reorg_stale_body(),
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
    // zakura#2378 is open, so the seed fails by design.
    let known_failure = Seed::new(
        "sync-wire-reset-mid-write",
        SyncWire::NAME,
        WIRE,
        reset_mid_write(),
    )
    .witness(STALLED_SERVICE_ISOLATED, Expect::Violated)
    .in_set(SeedSet::KnownFailure);
    wire.chain(curated)
        .chain(storage)
        .chain(searched)
        .chain([known_failure])
        .collect()
}
