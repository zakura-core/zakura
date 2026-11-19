//! `sync/composed` (fuzz-11 for the combined wire tier): a prefix composes
//! `sync/wire`'s topology, chain, control steps and block holds. Each slot
//! decodes to its step, a close waits for its mirror's live connection, a
//! reorg forks below the tip and ends above it, and a composed run with a
//! close, a growth and stalled reads holds every property.
#![cfg(zakura_sim)]

use zakura_sim::{
    explore::{classify, run_bytes, ExploreOutcome},
    harness::{
        self,
        block_node::{
            control::Fork,
            policy::{BlockKind, Select, Trigger},
        },
        sync::SyncAction,
        sync_wire::{SyncComposed, SYNC_COMPOSED_VERSION},
        Subsystem,
    },
    NullSink,
};

/// Two mirrors, the second dialed by the node, on 0 to 100 ms links, and
/// six 2,000-byte blocks.
const TOPOLOGY: u8 = 0b0000_1001;
const CHAIN: u8 = 0;

/// A prefix of `events`, three bytes each, after the version, topology and
/// chain bytes.
fn prefix(events: &[[u8; 3]]) -> Vec<u8> {
    let mut prefix = vec![SYNC_COMPOSED_VERSION, TOPOLOGY, CHAIN];
    prefix.extend(events.iter().flatten());
    prefix.resize(SyncComposed::SCENARIO_BYTES, 0);
    prefix
}

#[test]
fn each_slot_decodes_to_its_step() {
    let scenario = SyncComposed::draw(&prefix(&[
        // Close mirror 1 once storage admits its first body.
        [1, 1, 0b0000_0100],
        // Close it again, with no trigger of its own.
        [2, 1, 0],
        // Grow the chain by two blocks.
        [3, 1, 0],
        // Reset to a fork at height 1 + 9 % 7 = 3, ending one above the tip.
        [4, 9, 0],
        // Hold every mirror range read for 6 s after its submission.
        [5, 0b0000_0100, 2],
        // Tear down; the rest is never read.
        [7, 0, 0],
        [3, 3, 0],
    ]));
    let sync = &scenario.sync;
    assert_eq!(sync.mirrors.len(), 2);
    assert_eq!(sync.blocks, 6);
    let linked = |count| Trigger::Count(count, Box::new(Trigger::Linked("mirror-1".to_owned())));
    let close = |when| {
        (
            SyncAction::CloseIfLinked {
                peer: "mirror-1".to_owned(),
            },
            Some(when),
        )
    };
    let steps: Vec<_> = sync
        .control
        .iter()
        .map(|step| (step.action.clone(), step.when.clone()))
        .collect();
    assert_eq!(
        steps,
        [
            close(Trigger::All(vec![linked(1), harness::sync::admitted(1)])),
            close(linked(2)),
            (SyncAction::Grow { len: 2 }, None),
            (
                SyncAction::Reset {
                    fork: Fork {
                        branch: 1,
                        at: 3,
                        len: 6,
                    },
                },
                None
            ),
            (SyncAction::Teardown, None),
        ]
    );
    let holds = &sync.block_policy.holds;
    let hold = holds.last().unwrap();
    assert_eq!(
        hold.select,
        Select {
            kind: Some(BlockKind::MirrorRead),
            ..Select::default()
        }
    );
    assert_eq!(hold.until, Trigger::VirtualTime(6_000));
    let mut other = prefix(&[]);
    other[0] ^= 0xff;
    assert!(!SyncComposed::accepts(&other));
}

/// A mirror takes only as many closes as its dialer makes connections. A
/// fifth close of mirror 0 would wait forever for a fifth link and hold
/// back the growth after it, so it is skipped.
#[test]
fn closes_past_the_dialers_connections_are_skipped() {
    let close = [1, 0, 0];
    let scenario = SyncComposed::draw(&prefix(&[close, close, close, close, close, [3, 0, 0]]));
    let steps: Vec<_> = scenario
        .sync
        .control
        .iter()
        .map(|step| step.action.clone())
        .collect();
    let close = SyncAction::CloseIfLinked {
        peer: "mirror-0".to_owned(),
    };
    assert_eq!(
        steps,
        [
            close.clone(),
            close.clone(),
            close.clone(),
            close,
            SyncAction::Grow { len: 1 }
        ]
    );
}

/// Mirror 0's connection closes after the second admission, the chain
/// grows by one block, and each mirror range read stalls for 3 s. Both
/// steps run, in order, and the run violates nothing and reaches no
/// harness failure.
#[test]
fn a_composed_close_growth_and_stall_holds() {
    let input = prefix(&[[1, 0, 0b0000_0101], [3, 0, 0], [5, 0b0000_0100, 1]]);
    let entry = harness::find("sync/composed").unwrap();
    let run = run_bytes::<SyncComposed>(&input, &mut NullSink, None);
    let steps: Vec<_> = run
        .report()
        .expect("the harness runs the scenario")
        .picks
        .iter()
        .map(|pick| pick.identity.as_str())
        .filter(|identity| identity.ends_with(":close-if-linked") || identity.ends_with(":grow"))
        .collect();
    assert_eq!(steps, ["peer:mirror-0:close-if-linked", "peer:chain:grow"]);
    let classified = classify(entry, &run);
    assert_eq!(classified.outcome, ExploreOutcome::Clean, "{classified:?}");
}
