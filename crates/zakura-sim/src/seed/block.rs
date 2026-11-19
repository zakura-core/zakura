//! The `block` seeds: the block machines' op tapes, and `block-sync/storage`'s
//! scenario tags, alone or with a fixed draw of Plan bytes.

use super::{draws::byte_input, Seed, SeedSet};
use crate::{
    domain::{
        block_sync::{
            BUDGET_LEDGERS_AGREE, LOOKAHEAD_OVERSHOOT_BOUND, PAYLOAD_INVENTORY_AGREES,
            RESERVED_BYTE_BOUND, RETAINED_BYTE_BOUND, TEARDOWN_RETURNS_BASELINE,
        },
        block_sync_semantic::{
            CAPACITY_RELEASE_WAKES, DETACHED_SUBMISSION_BOUNDED, INPUT_DECODED_BOUND,
            STALE_COMPLETION_INERT, SUBMIT_ONLY_REQUESTED,
        },
    },
    harness::{
        block_semantic::{
            CAPACITY_RELEASE_TAG, DECODED_INPUT_STALL_TAG, RESET_MID_FLIGHT_TAG, STALLED_RESET_TAG,
        },
        block_sync::{Storage, ABOVE_WINDOW_TAG, PAUSED_HANDOFFS_TAG, PEERS_LEAVE_TAG},
        block_work::{DECODED_RETENTION_ENTRY, ENTRY, FUNDING_ENTRY, REACTOR_ENTRY},
        Subsystem,
    },
    witness::Expect,
};

/// The test that replays the block machines' op tapes.
const REPLAY_EXACT: &str = "crates/zakura-sim/tests/replay_exact.rs";

/// The test that runs the resource-bound seeds.
const RESOURCE: &str = "crates/zakura-sim/tests/block_resource_properties.rs";

/// The test that runs the semantic seeds.
const SEMANTIC: &str = "crates/zakura-sim/tests/block_semantic_properties.rs";

/// How many fuzz Plans the semantic seeds' searches drew from
/// [`super::draws::byte_inputs`], each up to [`SEARCH_PLAN_BYTES`] long.
const SEARCH_CANDIDATES: usize = 64;

/// The longest Plan the semantic seeds' searches drew.
const SEARCH_PLAN_BYTES: usize = 16_384;

/// The longest Plan the stalled-storage seeds draw.
const STALL_PLAN_BYTES: usize = 256;

/// `block/work-ownership`'s op tape: two owners take, publish and receive
/// heights, then return them.
const WORK_OWNERSHIP: [u8; 40] = [
    0, 0, 0, 9, 1, 0, 0, 1, 2, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 9, 1, 0, 0, 1, 2, 1, 0, 0, 5, 0, 0, 0,
    4, 1, 0, 0, 5, 1, 0, 0,
];

/// `block/reactor-policy`'s op tape that keeps a query's identity across a
/// committed view.
const REACTOR_IDENTITY: [u8; 16] = [0, 0, 1, 200, 1, 0, 1, 0, 4, 0, 30, 0, 0, 9, 1, 200];

/// `block/reactor-policy`'s op tape that offers, refuses and retries
/// queries across refills and committed views.
const REACTOR_POLICY: [u8; 60] = [
    0, 0, 1, 200, 0, 8, 2, 200, 7, 0, 200, 0, 3, 0, 0, 0, 1, 0, 0, 1, 6, 2, 1, 0, 6, 3, 0, 0, 5, 0,
    1, 0, 1, 0, 0, 0, 0, 0, 1, 200, 2, 0, 1, 0, 7, 0, 200, 0, 3, 0, 0, 0, 4, 0, 30, 0, 6, 0, 3, 0,
];

/// `block/work-funding`'s op tape: it retains a receipt through a reset,
/// then admits, publishes and returns a replacement. zakura-network's
/// funding tests replay the same bytes as their machine target's witness.
const WORK_FUNDING: [u8; 56] = [
    0, 0, 1, 99, 1, 0, 9, 3, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0, 8, 0, 0, 0, 17, 0, 0,
    0, 0, 0, 1, 99, 1, 0, 9, 3, 2, 1, 0, 0, 3, 1, 0, 0, 12, 0, 0, 1, 11, 0, 0, 0,
];

/// `block/work-funding`'s op tape: one session publishes 1,000 above-floor
/// bytes, overdraws 500 more with a floor request, and its next floor
/// request would pass the owner cap's floor total of 1,500 bytes.
/// zakura-network's funding tests replay the same bytes.
const WORK_OWNER_CAP: [u8; 40] = [
    0, 0, 7, 249, 1, 2, 31, 10, 2, 0, 0, 0, 3, 0, 0, 0, 1, 6, 15, 5, 2, 1, 6, 0, 3, 1, 0, 0, 0, 8,
    1, 249, 1, 8, 7, 3, 2, 2, 8, 0,
];

/// `block-sync-stale-beside-current`'s input, from a seeded search no
/// builder here reproduces. Its tag, `ff 06`, picks the stale-beside-current
/// scenario: a reset detaches a held submission, whose late retryable verdict
/// reaches block sync while the fork's first submission waits in storage.
/// No other seed catches the stale-completion native mutations.
const STALE_BESIDE_CURRENT: [u8; 127] = [
    255, 6, 175, 205, 29, 123, 57, 168, 32, 226, 244, 101, 185, 161, 106, 158, 120, 110, 79, 69, 9,
    128, 24, 93, 196, 6, 236, 129, 76, 114, 168, 184, 139, 248, 155, 116, 168, 81, 106, 137, 57,
    27, 234, 162, 126, 116, 12, 159, 203, 83, 225, 50, 69, 31, 190, 154, 130, 44, 60, 171, 22, 201,
    58, 19, 132, 197, 195, 138, 201, 65, 144, 120, 229, 62, 166, 176, 140, 54, 140, 72, 184, 243,
    9, 61, 177, 60, 221, 236, 126, 101, 246, 222, 91, 5, 224, 38, 211, 194, 123, 219, 187, 224, 63,
    160, 33, 134, 47, 169, 58, 152, 85, 117, 31, 142, 25, 77, 204, 0, 22, 15, 78, 181, 171, 128,
    29, 151, 151,
];

/// `block-sync-floor-lane-race`'s input, the first Plan of a seeded random
/// search (mut-7.4) whose run caught `qa/native-mutations/bounds/wide-floor-lane`.
/// Its tag, `ff 03`, picks the paused-handoffs scenario: a routine probes the
/// floor's next height, another takes it first, and the first routine's floor
/// take starts above it. No other seed catches that mutation.
const FLOOR_LANE_RACE: [u8; 83] = [
    255, 3, 72, 157, 55, 128, 198, 186, 82, 16, 91, 161, 242, 199, 158, 147, 254, 163, 134, 5, 107,
    70, 131, 14, 247, 96, 31, 152, 223, 11, 47, 96, 255, 213, 221, 88, 112, 123, 38, 43, 17, 154,
    26, 78, 33, 200, 226, 183, 52, 43, 8, 119, 26, 124, 7, 255, 100, 19, 200, 114, 254, 113, 150,
    45, 245, 124, 218, 125, 212, 209, 10, 67, 73, 66, 87, 4, 152, 33, 86, 248, 255, 43, 185,
];

/// `block-sync-stuck-floor`'s input. Its tag, `ff 02`, picks the stuck-floor
/// scenario: one mirror serves 840 bodies but withholds height 3, so the
/// commit window fills. No other seed catches the wide-commit-window patch.
const STUCK_FLOOR: [u8; 34] = [
    255, 2, 242, 137, 179, 73, 195, 5, 191, 247, 140, 235, 116, 0, 74, 225, 188, 83, 173, 107, 30,
    102, 38, 172, 207, 44, 9, 31, 114, 46, 216, 227, 57, 216,
];

/// `block/decoded-retention`'s op tape. Each op is `[opcode, source slot,
/// destination slot, pool]`, and the opcode modulo 7 picks admit, clone,
/// retain header, retain transaction, drop, nothing or close. Pools 0 and 1
/// hold three blocks each, and pool 2 holds two.
const DECODED_RETENTION: [u8; 80] = [
    0, 0, 0, 0, // admit into slot 0 from pool 0
    0, 0, 4, 1, // admit into slot 4 from pool 1
    1, 0, 3, 0, // clone slot 0 into slot 3
    2, 0, 1, 0, // retain slot 0's header in slot 1
    3, 0, 2, 0, // retain slot 0's transaction in slot 2
    4, 0, 0, 0, // drop slot 0
    5, 1, 0, 2, // nothing
    4, 3, 0, 0, // drop slot 3
    4, 1, 0, 0, // drop slot 1
    6, 0, 0, 2, // close pool 2
    5, 2, 0, 1, // nothing
    4, 2, 0, 0, // drop slot 2
    4, 4, 0, 0, // drop slot 4
    0, 0, 0, 2, // admit from pool 2, which is closed
    0, 0, 0, 0, // admit into slot 0 from pool 0
    0, 0, 1, 0, // admit into slot 1 from pool 0
    0, 0, 2, 0, // admit into slot 2 from pool 0, its third block
    0, 0, 3, 0, // admit a fourth block from pool 0, past its limit
    5, 0, 0, 1, // nothing
    6, 0, 0, 0, // close pool 0
];

/// `tag`, then Plan bytes.
fn tagged(tag: [u8; 2], plan: &[u8]) -> Vec<u8> {
    [&tag[..], plan].concat()
}

/// Search candidate `index`: the Plan a semantic seed's search picked.
fn candidate(index: usize) -> Vec<u8> {
    byte_input(SEARCH_CANDIDATES, SEARCH_PLAN_BYTES, index)
}

/// The first short Plan, which the stalled-storage seeds run.
fn short_plan() -> Vec<u8> {
    byte_input(1, STALL_PLAN_BYTES, 0)
}

/// Every `block` seed.
pub(super) fn seeds() -> Vec<Seed> {
    let storage = Storage::NAME;
    vec![
        Seed::new(
            "block-work-ownership",
            ENTRY.name,
            REPLAY_EXACT,
            WORK_OWNERSHIP.to_vec(),
        )
        .witness(ENTRY.name, Expect::Holds),
        Seed::new(
            "block-reactor-identity",
            REACTOR_ENTRY.name,
            REPLAY_EXACT,
            REACTOR_IDENTITY.to_vec(),
        )
        .witness(REACTOR_ENTRY.name, Expect::Holds),
        Seed::new(
            "block-reactor-policy",
            REACTOR_ENTRY.name,
            REPLAY_EXACT,
            REACTOR_POLICY.to_vec(),
        )
        .witness(REACTOR_ENTRY.name, Expect::Holds),
        Seed::new(
            "block-work-funding",
            FUNDING_ENTRY.name,
            REPLAY_EXACT,
            WORK_FUNDING.to_vec(),
        )
        .witness(FUNDING_ENTRY.name, Expect::Holds),
        Seed::new(
            "block-work-owner-cap",
            FUNDING_ENTRY.name,
            "crates/zakura-network/src/zakura/block_sync/work_queue/state/tests/funding.rs",
            WORK_OWNER_CAP.to_vec(),
        )
        .witness(FUNDING_ENTRY.name, Expect::Holds),
        Seed::new(
            "block-decoded-retention",
            DECODED_RETENTION_ENTRY.name,
            REPLAY_EXACT,
            DECODED_RETENTION.to_vec(),
        )
        .witness(DECODED_RETENTION_ENTRY.name, Expect::Holds),
        // Four mirrors serve sixteen bodies of unknown size against a byte
        // budget of `1.5·B`, and each routine pauses in its handoff from
        // height 5. The empty Plan picks round-robin.
        Seed::new(
            "block-sync-paused-handoffs",
            storage,
            RESOURCE,
            PAUSED_HANDOFFS_TAG.to_vec(),
        )
        .witness(RESERVED_BYTE_BOUND, Expect::Holds),
        Seed::new(
            "block-sync-peers-leave",
            storage,
            RESOURCE,
            PEERS_LEAVE_TAG.to_vec(),
        )
        .witness(TEARDOWN_RETURNS_BASELINE, Expect::Holds),
        Seed::new(
            "block-sync-above-window",
            storage,
            RESOURCE,
            ABOVE_WINDOW_TAG.to_vec(),
        )
        .witness(LOOKAHEAD_OVERSHOOT_BOUND, Expect::Holds)
        .in_set(SeedSet::Slow),
        // `regenerate_reset_mid_flight_input` picked candidate 6: its run
        // submits bodies of an old and a new body-work epoch, credits one
        // through first-completion-wins, and has the sequencer refuse an
        // old-epoch body after it applied a newer epoch.
        Seed::new(
            "block-sync-reset-mid-flight",
            storage,
            SEMANTIC,
            tagged(RESET_MID_FLIGHT_TAG, &candidate(6)),
        )
        .witness(SUBMIT_ONLY_REQUESTED, Expect::Holds),
        // `regenerate_capacity_frontier_first_input` picked candidate 9: block
        // sync handles the detached commit's tip before its reply.
        Seed::new(
            "block-sync-capacity-frontier-first",
            storage,
            SEMANTIC,
            tagged(CAPACITY_RELEASE_TAG, &candidate(9)),
        )
        .witness(CAPACITY_RELEASE_WAKES, Expect::Holds)
        .in_set(SeedSet::Slow),
        Seed::new(
            "block-sync-stalled-reset",
            storage,
            SEMANTIC,
            tagged(STALLED_RESET_TAG, &short_plan()),
        )
        .witness(DETACHED_SUBMISSION_BOUNDED, Expect::Holds)
        .witness(INPUT_DECODED_BOUND, Expect::Holds)
        .witness(PAYLOAD_INVENTORY_AGREES, Expect::Holds)
        .witness(BUDGET_LEDGERS_AGREE, Expect::Holds),
        Seed::new(
            "block-sync-decoded-input-stall",
            storage,
            SEMANTIC,
            tagged(DECODED_INPUT_STALL_TAG, &short_plan()),
        )
        .witness(INPUT_DECODED_BOUND, Expect::Holds)
        .in_set(SeedSet::Slow),
        Seed::new(
            "block-sync-stale-beside-current",
            storage,
            SEMANTIC,
            STALE_BESIDE_CURRENT.to_vec(),
        )
        .witness(STALE_COMPLETION_INERT, Expect::Holds)
        .witness(DETACHED_SUBMISSION_BOUNDED, Expect::Holds),
        Seed::new(
            "block-sync-floor-lane-race",
            storage,
            RESOURCE,
            FLOOR_LANE_RACE.to_vec(),
        )
        .witness(RESERVED_BYTE_BOUND, Expect::Holds),
        // A run takes over 20 minutes in a target's AddressSanitizer build,
        // too long for a fuzz input.
        Seed::new(
            "block-sync-stuck-floor",
            storage,
            RESOURCE,
            STUCK_FLOOR.to_vec(),
        )
        .witness(RETAINED_BYTE_BOUND, Expect::Holds)
        .in_set(SeedSet::Slow),
    ]
}
