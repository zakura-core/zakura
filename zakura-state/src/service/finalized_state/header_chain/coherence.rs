//! Model-based coherence checks for the durable fork-aware header store.

#[path = "coherence/fabricate.rs"]
mod fabricate;
#[path = "coherence/harness.rs"]
mod harness;
#[path = "coherence/prop.rs"]
mod prop;

use harness::{Anchor, Harness, Op, Source};

#[test]
fn fork_aware_write_path_upholds_invariants_across_forks_rejections_and_reopen() {
    let _init_guard = zakura_test::init();
    let mut harness = Harness::new();

    harness.run_all(&[
        Op::InsertHeaders {
            source: Source::Trunk,
            offset: 0,
            len: 50,
            anchor: Anchor::Natural,
        },
        Op::InsertHeaders {
            source: Source::Branch(0),
            offset: 0,
            len: 26,
            anchor: Anchor::Natural,
        },
        Op::Reopen,
        Op::InsertHeaders {
            source: Source::Branch(1),
            offset: 0,
            len: 30,
            anchor: Anchor::Natural,
        },
        Op::InsertHeaders {
            source: Source::Branch(2),
            offset: 0,
            len: 4,
            anchor: Anchor::Natural,
        },
        Op::InsertHeaders {
            source: Source::Branch(2),
            offset: 4,
            len: 64,
            anchor: Anchor::Natural,
        },
        Op::InsertHeaders {
            source: Source::Branch(3),
            offset: 0,
            len: 5,
            anchor: Anchor::Natural,
        },
        Op::InsertHeaders {
            source: Source::Trunk,
            offset: 50,
            len: 10,
            anchor: Anchor::Genesis,
        },
        Op::InsertHeaders {
            source: Source::Branch(1),
            offset: 7,
            len: 5,
            anchor: Anchor::TrunkAt(50),
        },
        Op::Verify {
            source: Source::Branch(2),
            index: 20,
        },
        Op::Finalize { count: 10 },
        Op::Verify {
            source: Source::Branch(3),
            index: 4,
        },
        Op::Reopen,
    ]);
}

#[test]
fn unlinked_anchor_range_upholds_invariants() {
    let _init_guard = zakura_test::init();
    let mut harness = Harness::new();
    harness.run_all(&[
        Op::InsertHeaders {
            source: Source::Trunk,
            offset: 0,
            len: 50,
            anchor: Anchor::Natural,
        },
        Op::InsertHeaders {
            source: Source::Trunk,
            offset: 50,
            len: 10,
            anchor: Anchor::Genesis,
        },
    ]);
    assert_eq!(harness.rejections(), 1);
}

#[test]
fn redelivery_over_verified_bodies_upholds_invariants() {
    let _init_guard = zakura_test::init();
    let mut harness = Harness::new();
    harness.run_all(&[
        Op::InsertHeaders {
            source: Source::Trunk,
            offset: 0,
            len: 20,
            anchor: Anchor::Natural,
        },
        Op::Verify {
            source: Source::Trunk,
            index: 19,
        },
        Op::InsertHeaders {
            source: Source::Trunk,
            offset: 0,
            len: 20,
            anchor: Anchor::Natural,
        },
        Op::InsertHeaders {
            source: Source::Trunk,
            offset: 5,
            len: 10,
            anchor: Anchor::Natural,
        },
        Op::Reopen,
    ]);
    assert_eq!(harness.rejections(), 0);
}

#[test]
fn unlinked_full_state_seed_upholds_invariants() {
    let _init_guard = zakura_test::init();
    let mut harness = Harness::new();
    harness.run_all(&[Op::MalformedVerify {
        source: Source::Branch(0),
        index: 0,
    }]);
    assert_eq!(harness.rejections(), 1);
}
