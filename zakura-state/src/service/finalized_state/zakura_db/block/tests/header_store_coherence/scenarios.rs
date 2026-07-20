//! Scripted scenarios: production event shapes replayed against the store.
//!
//! Every scenario runs with a per-op audit (via `Harness::run`), so any
//! passing scenario is a regression gate on the whole write sequence, not
//! just its final assertions.
//!
//! The `*_upholds_invariants` tests at the bottom are the permanent
//! regression gates for the three write-path corruption bugs this suite
//! originally proved with `corruption_repro_*` twins (removed together with
//! the write-path fixes; see the module README for the bug histories).

use zakura_chain::block::Height;

use super::{
    fabricate::{BRANCH_A, BRANCH_B, BRANCH_B_EXT, BRANCH_C, FORK_HEIGHT, TRUNK_LEN},
    ops::{universe, Anchor, Harness, Op, OpOutcome, Source},
};
use crate::error::CommitHeaderRangeError;

/// The whole trunk in one range from the genesis anchor.
fn commit_trunk() -> Op {
    Op::CommitHeaderRange {
        source: Source::Trunk,
        offset: 0,
        len: TRUNK_LEN,
        anchor: Anchor::Auto,
    }
}

/// A full branch in one range from its fork parent.
fn commit_branch(branch: usize) -> Op {
    Op::CommitHeaderRange {
        source: Source::Branch(branch),
        offset: 0,
        len: usize::MAX / 2,
        anchor: Anchor::Auto,
    }
}

fn branch_tip(branch: usize) -> (Height, zakura_chain::block::Hash) {
    let fab = universe().branches[branch]
        .headers
        .last()
        .expect("branches are non-empty");
    (fab.height, fab.hash)
}

fn assert_accepted(outcome: &OpOutcome) {
    assert!(
        matches!(outcome, OpOutcome::Accepted),
        "expected acceptance, got {outcome:?}"
    );
}

/// s01: extend with the trunk, finalize its base, reorg to the higher-work
/// branch A, and survive a restart.
#[test]
fn s01_extend_then_simple_reorg() {
    let _init_guard = zakura_test::init();
    let mut harness = Harness::new();

    let outcomes = harness
        .run_all(&[
            commit_trunk(),
            Op::Finalize { count: 5 },
            commit_branch(BRANCH_A),
            Op::Reopen,
        ])
        .expect("clean sequence has no violations");
    outcomes.iter().for_each(assert_accepted);

    assert_eq!(
        harness.state().best_header_tip(),
        Some(branch_tip(BRANCH_A)),
    );
    assert_eq!(harness.oracle.body_tip(), Height(5));
}

/// s02: losing a work comparison once is not a
/// lasting verdict. Branch B first loses to A (`LowerWorkConflict`, store
/// untouched — checked by the harness), then B's extended version wins and
/// becomes canonical at the same fork point.
#[test]
fn s02_lower_work_conflict_non_terminal() {
    let _init_guard = zakura_test::init();
    let mut harness = Harness::new();

    harness
        .run_all(&[commit_trunk(), commit_branch(BRANCH_A)])
        .expect("setup is clean");

    // B is longer but carries less work: rejected, side-effect free.
    let outcome = harness
        .run(&commit_branch(BRANCH_B))
        .expect("rejection leaves the store coherent");
    match outcome.header_range_error() {
        CommitHeaderRangeError::LowerWorkConflict {
            height,
            existing_work,
            new_work,
        } => {
            assert_eq!(*height, Height(FORK_HEIGHT + 1));
            assert!(
                new_work <= existing_work,
                "the rejected suffix must not out-work the incumbent"
            );
        }
        other => panic!("expected LowerWorkConflict, got {other:?}"),
    }
    assert_eq!(
        harness.state().best_header_tip(),
        Some(branch_tip(BRANCH_A)),
        "A stays canonical after the rejection",
    );

    // B_ext re-includes B's rejected prefix and now out-works A: the same
    // fork point switches the moment the work balance flips.
    let outcome = harness
        .run(&commit_branch(BRANCH_B_EXT))
        .expect("the winning delivery is clean");
    assert_accepted(&outcome);
    assert_eq!(
        harness.state().best_header_tip(),
        Some(branch_tip(BRANCH_B_EXT)),
    );
}

/// s02b: the winning branch arrives split into two ranges. The first range
/// must already out-work A on its own (a losing prefix is rejected — that
/// shape is s03's walk-back).
#[test]
fn s02b_lower_work_then_win_split_ranges() {
    let _init_guard = zakura_test::init();
    let mut harness = Harness::new();

    harness
        .run_all(&[commit_trunk(), commit_branch(BRANCH_A)])
        .expect("setup is clean");
    harness
        .run(&commit_branch(BRANCH_B))
        .expect("rejection leaves the store coherent");

    // B_ext minus its two margin headers still out-works A by construction.
    let b_ext_len = universe().branches[BRANCH_B_EXT].headers.len();
    let outcomes = harness
        .run_all(&[
            Op::CommitHeaderRange {
                source: Source::Branch(BRANCH_B_EXT),
                offset: 0,
                len: b_ext_len - 2,
                anchor: Anchor::Auto,
            },
            Op::CommitHeaderRange {
                source: Source::Branch(BRANCH_B_EXT),
                offset: b_ext_len - 2,
                len: 2,
                anchor: Anchor::Auto,
            },
        ])
        .expect("split delivery is clean");
    outcomes.iter().for_each(assert_accepted);
    assert_eq!(
        harness.state().best_header_tip(),
        Some(branch_tip(BRANCH_B_EXT)),
    );
}

/// s02d: a restart between the losing delivery and the winning one must not
/// change the outcome (nothing about the rejection may persist).
#[test]
fn s02d_lower_work_then_win_with_reopen_between() {
    let _init_guard = zakura_test::init();
    let mut harness = Harness::new();

    harness
        .run_all(&[commit_trunk(), commit_branch(BRANCH_A)])
        .expect("setup is clean");
    harness
        .run(&commit_branch(BRANCH_B))
        .expect("rejection leaves the store coherent");
    let outcomes = harness
        .run_all(&[Op::Reopen, commit_branch(BRANCH_B_EXT), Op::Reopen])
        .expect("post-restart winning delivery is clean");
    outcomes.iter().for_each(assert_accepted);
    assert_eq!(
        harness.state().best_header_tip(),
        Some(branch_tip(BRANCH_B_EXT)),
    );
}

/// s03: the walk-back re-commit shape. The upper half of the winning branch
/// arrives first and fails with `UnknownAnchor`; the lower half lands (a
/// reorg); the upper half is re-delivered and extends it.
#[test]
fn s03_walkback_split_recommit() {
    let _init_guard = zakura_test::init();
    let mut harness = Harness::new();

    harness
        .run_all(&[commit_trunk(), commit_branch(BRANCH_A)])
        .expect("setup is clean");

    let b_ext_len = universe().branches[BRANCH_B_EXT].headers.len();
    let split = b_ext_len - 2;

    // Upper half first: its anchor is not committed yet.
    let outcome = harness
        .run(&Op::CommitHeaderRange {
            source: Source::Branch(BRANCH_B_EXT),
            offset: split,
            len: 2,
            anchor: Anchor::Auto,
        })
        .expect("rejection leaves the store coherent");
    assert!(
        matches!(
            outcome.header_range_error(),
            CommitHeaderRangeError::UnknownAnchor { .. }
        ),
        "expected UnknownAnchor, got {outcome:?}"
    );

    // Walk back: lower half (a winning reorg), then the upper half again.
    let outcomes = harness
        .run_all(&[
            Op::CommitHeaderRange {
                source: Source::Branch(BRANCH_B_EXT),
                offset: 0,
                len: split,
                anchor: Anchor::Auto,
            },
            Op::CommitHeaderRange {
                source: Source::Branch(BRANCH_B_EXT),
                offset: split,
                len: 2,
                anchor: Anchor::Auto,
            },
        ])
        .expect("walk-back re-commit is clean");
    outcomes.iter().for_each(assert_accepted);
    assert_eq!(
        harness.state().best_header_tip(),
        Some(branch_tip(BRANCH_B_EXT)),
    );
}

/// s04: a body commit racing a header reorg. After headers reorg to branch A,
/// body sync (still sequential on the old chain) commits the trunk block at
/// the fork's first height: the verified body wins and truncates A's rows.
/// Re-delivering A over the now-body-backed height must be refused.
#[test]
fn s04_body_commit_racing_header_reorg() {
    let _init_guard = zakura_test::init();
    let mut harness = Harness::new();

    harness
        .run_all(&[
            commit_trunk(),
            Op::Finalize {
                count: FORK_HEIGHT as usize,
            },
            commit_branch(BRANCH_A),
        ])
        .expect("setup is clean");
    assert_eq!(harness.oracle.body_tip(), Height(FORK_HEIGHT));

    // The racing body: the old-branch block right above the fork.
    let outcome = harness
        .run(&Op::CommitBody {
            source: Source::Trunk,
            index: FORK_HEIGHT as usize, // trunk row at height FORK_HEIGHT + 1
        })
        .expect("the verified body wins cleanly");
    assert_accepted(&outcome);
    assert_eq!(
        harness.state().best_header_tip(),
        Some((
            Height(FORK_HEIGHT + 1),
            universe().trunk_at(FORK_HEIGHT + 1).hash
        )),
        "the body truncated A's provisional rows",
    );

    // A cannot displace a committed body through the header path.
    let outcome = harness
        .run(&commit_branch(BRANCH_A))
        .expect("rejection leaves the store coherent");
    assert!(
        matches!(
            outcome.header_range_error(),
            CommitHeaderRangeError::ImmutableConflict { .. }
                | CommitHeaderRangeError::ConflictingFullBlockHeader { .. }
        ),
        "expected a body-conflict rejection, got {outcome:?}"
    );
}

/// s05: the release path's frontier trim. Sequential body commits release
/// matching provisional rows one by one; header re-delivery *above* the body
/// tip is idempotent; restarts change nothing. (Re-delivery *over* body-backed
/// heights skips those heights — `redelivery_over_bodies_upholds_invariants`.)
#[test]
fn s05_release_trim_then_redelivery() {
    let _init_guard = zakura_test::init();
    let mut harness = Harness::new();

    let outcomes = harness
        .run_all(&[
            commit_trunk(),
            Op::Finalize { count: 5 },
            Op::Reopen,
            Op::Finalize { count: 5 },
            // Re-delivery anchored at the body tip, over header-only heights.
            Op::CommitHeaderRange {
                source: Source::Trunk,
                offset: 10,
                len: 10,
                anchor: Anchor::Auto,
            },
            Op::Reopen,
        ])
        .expect("release trim and redelivery are clean");
    outcomes.iter().for_each(assert_accepted);
    assert_eq!(harness.oracle.body_tip(), Height(10));

    let trunk_tip = universe().trunk_at(TRUNK_LEN as u32);
    assert_eq!(
        harness.state().best_header_tip(),
        Some((trunk_tip.height, trunk_tip.hash)),
    );
}

/// s06: a reorg to a *lower* height. The long low-work branch B is canonical;
/// the short high-work branch A replaces it, so the tip height decreases and
/// every one of B's rows above A's tip must be gone.
#[test]
fn s06_reorg_to_lower_height() {
    let _init_guard = zakura_test::init();
    let mut harness = Harness::new();

    let outcomes = harness
        .run_all(&[commit_trunk(), commit_branch(BRANCH_B)])
        .expect("setup is clean");
    outcomes.iter().for_each(assert_accepted);
    let old_tip = branch_tip(BRANCH_B).0;
    assert_eq!(
        harness.state().best_header_tip(),
        Some(branch_tip(BRANCH_B)),
    );

    let outcomes = harness
        .run_all(&[commit_branch(BRANCH_A), Op::Reopen])
        .expect("the lower-height reorg strands nothing");
    outcomes.iter().for_each(assert_accepted);
    assert_eq!(
        harness.state().best_header_tip(),
        Some(branch_tip(BRANCH_A)),
        "the tip height decreased to A's tip",
    );
    let new_tip = branch_tip(BRANCH_A).0;
    for height in (new_tip.0 + 1..=old_tip.0).map(Height) {
        assert!(
            harness.state().commitment_roots(height).is_none(),
            "the reorg removes stale commitment roots at {height:?}",
        );
    }
}

/// s07: double reorg at the same fork point (B → A → B_ext), then the losing
/// branch's re-delivery is refused without side effects.
#[test]
fn s07_double_reorg_same_fork_point() {
    let _init_guard = zakura_test::init();
    let mut harness = Harness::new();

    let outcomes = harness
        .run_all(&[
            commit_trunk(),
            commit_branch(BRANCH_B),
            commit_branch(BRANCH_A),
            commit_branch(BRANCH_B_EXT),
        ])
        .expect("double reorg is clean");
    outcomes.iter().for_each(assert_accepted);
    assert_eq!(
        harness.state().best_header_tip(),
        Some(branch_tip(BRANCH_B_EXT)),
    );

    let outcome = harness
        .run(&commit_branch(BRANCH_A))
        .expect("rejection leaves the store coherent");
    assert!(
        matches!(
            outcome.header_range_error(),
            CommitHeaderRangeError::LowerWorkConflict { .. }
        ),
        "expected LowerWorkConflict, got {outcome:?}"
    );
}

/// s08: activity across the DAA window edge. After a reorg whose fork point is
/// more than `POW_ADJUSTMENT_BLOCK_SPAN` below the tip, re-deliveries whose
/// difficulty context spans both the branch and the trunk must validate, and
/// a branch whose ancestry lost the reorg must be unknown.
#[test]
fn s08_reorg_across_daa_window_edge() {
    let _init_guard = zakura_test::init();
    let mut harness = Harness::new();

    harness
        .run_all(&[
            commit_trunk(),
            commit_branch(BRANCH_A),
            commit_branch(BRANCH_B_EXT),
        ])
        .expect("setup is clean");

    // The fork is deeper than the 28-height DAA window below the tip.
    let (tip_height, _) = branch_tip(BRANCH_B_EXT);
    assert!(tip_height.0 - FORK_HEIGHT > 28);

    // Idempotent re-delivery anchored mid-branch: its context spans the
    // branch/trunk boundary.
    let outcome = harness
        .run(&Op::CommitHeaderRange {
            source: Source::Branch(BRANCH_B_EXT),
            offset: 10,
            len: 10,
            anchor: Anchor::Auto,
        })
        .expect("re-delivery across the window edge is clean");
    assert_accepted(&outcome);

    // C forks off A, which lost the reorg: its anchor is unknown now.
    let outcome = harness
        .run(&commit_branch(BRANCH_C))
        .expect("rejection leaves the store coherent");
    assert!(
        matches!(
            outcome.header_range_error(),
            CommitHeaderRangeError::UnknownAnchor { .. }
        ),
        "expected UnknownAnchor, got {outcome:?}"
    );
}

/// s09: s03 with a restart at every boundary — pins the crash-recovery
/// property that recovery is never a special code path.
#[test]
fn s09_restart_between_walkback_and_recommit() {
    let _init_guard = zakura_test::init();
    let mut harness = Harness::new();

    harness
        .run_all(&[commit_trunk(), commit_branch(BRANCH_A)])
        .expect("setup is clean");

    let b_ext_len = universe().branches[BRANCH_B_EXT].headers.len();
    let split = b_ext_len - 2;

    let outcome = harness
        .run(&Op::CommitHeaderRange {
            source: Source::Branch(BRANCH_B_EXT),
            offset: split,
            len: 2,
            anchor: Anchor::Auto,
        })
        .expect("rejection leaves the store coherent");
    assert!(matches!(
        outcome.header_range_error(),
        CommitHeaderRangeError::UnknownAnchor { .. }
    ));

    let outcomes = harness
        .run_all(&[
            Op::Reopen,
            Op::CommitHeaderRange {
                source: Source::Branch(BRANCH_B_EXT),
                offset: 0,
                len: split,
                anchor: Anchor::Auto,
            },
            Op::Reopen,
            Op::CommitHeaderRange {
                source: Source::Branch(BRANCH_B_EXT),
                offset: split,
                len: 2,
                anchor: Anchor::Auto,
            },
            Op::Reopen,
        ])
        .expect("restart-interleaved walk-back is clean");
    outcomes.iter().for_each(assert_accepted);
    assert_eq!(
        harness.state().best_header_tip(),
        Some(branch_tip(BRANCH_B_EXT)),
    );
}

/// s10: the seed path (non-finalized best-chain commits) interleaved with
/// header ranges: seeds switch the canonical row at a height, truncating
/// conflicting suffixes, and header sync extends on top of seeded rows.
#[test]
fn s10_seed_interplay() {
    let _init_guard = zakura_test::init();
    let mut harness = Harness::new();

    let outcomes = harness
        .run_all(&[
            commit_trunk(),
            // The non-finalized best chain switches to A: seed its first rows.
            Op::Seed {
                source: Source::Branch(BRANCH_A),
                index: 0,
            },
            Op::Seed {
                source: Source::Branch(BRANCH_A),
                index: 1,
            },
            // Header sync extends A on top of the seeded rows.
            Op::CommitHeaderRange {
                source: Source::Branch(BRANCH_A),
                offset: 2,
                len: usize::MAX / 2,
                anchor: Anchor::Auto,
            },
            Op::Reopen,
            // The non-finalized best chain switches to B: the seed truncates A.
            Op::Seed {
                source: Source::Branch(BRANCH_B),
                index: 0,
            },
            Op::CommitHeaderRange {
                source: Source::Branch(BRANCH_B),
                offset: 1,
                len: usize::MAX / 2,
                anchor: Anchor::Auto,
            },
            Op::Reopen,
        ])
        .expect("seed interplay is clean");
    outcomes.iter().for_each(assert_accepted);
    assert_eq!(
        harness.state().best_header_tip(),
        Some(branch_tip(BRANCH_B)),
    );
}

/// s11: a refused (unlinked) seed converges through header-range sync. The
/// zakura store follows branch A (seeded at the fork height); the
/// non-finalized best chain switches to branch B and its new best tip (B's
/// *second* row) is seeded. The store refuses the unlinked seed as a no-op —
/// the header store briefly lags the nf chain — and the later linked range
/// delivery of B converges it onto the new branch.
#[test]
fn s11_refused_seed_converges_via_range_delivery() {
    let _init_guard = zakura_test::init();
    let mut harness = Harness::new();

    let outcomes = harness
        .run_all(&[
            commit_trunk(),
            Op::Seed {
                source: Source::Branch(BRANCH_A),
                index: 0,
            },
        ])
        .expect("setup is clean");
    outcomes.iter().for_each(assert_accepted);
    let a_first = &universe().branches[BRANCH_A].headers[0];
    assert_eq!(
        harness.state().best_header_tip(),
        Some((a_first.height, a_first.hash)),
    );

    // The nf best tip jumps to B[1]; its parent row is not stored, so the
    // seed is refused without touching the store (checked by the harness).
    let outcome = harness
        .run(&Op::Seed {
            source: Source::Branch(BRANCH_B),
            index: 1,
        })
        .expect("the refused seed leaves the store coherent");
    assert_accepted(&outcome);
    assert_eq!(
        harness.state().best_header_tip(),
        Some((a_first.height, a_first.hash)),
        "the refused seed must not move the header tip",
    );

    // Header sync catches up with the nf chain: B arrives as a linked range
    // and out-works the stored A suffix, converging the store onto B.
    let outcome = harness
        .run(&commit_branch(BRANCH_B))
        .expect("the converging range delivery is clean");
    assert_accepted(&outcome);
    assert_eq!(
        harness.state().best_header_tip(),
        Some(branch_tip(BRANCH_B)),
    );
}

// ---------------------------------------------------------------------------
// Regression gates for the three proven write-path corruption bugs. Each was
// originally pinned by a `corruption_repro_*` test that demonstrated the
// violation; those were removed with the write-path fixes, and these twins
// now assert the fixed behavior over the exact same op sequences.
// ---------------------------------------------------------------------------

/// The unlinked-anchor commit (bug 1): a range of branch-A headers anchored at
/// the same-height *trunk* hash passes contextual difficulty validation — the
/// two fast chains have identical (time, threshold) sequences — so before the
/// linkage check in `prepare_header_range_batch_with_roots`, it committed a
/// suffix that did not link to the row below it: an on-disk I2 violation
/// reachable from a single untrusted peer response.
fn unlinked_anchor_ops() -> Vec<Op> {
    vec![
        commit_trunk(),
        Op::CommitHeaderRange {
            source: Source::Branch(BRANCH_A),
            offset: 1,
            len: usize::MAX / 2,
            // The trunk hash at A[0]'s height: same height, wrong chain.
            anchor: Anchor::TrunkAt(FORK_HEIGHT + 1),
        },
    ]
}

/// The store rejects unlinked ranges with `UnlinkedRange` and stays coherent.
#[test]
fn unlinked_anchor_commit_upholds_invariants() {
    let _init_guard = zakura_test::init();
    let mut harness = Harness::new();
    let outcomes = harness
        .run_all(&unlinked_anchor_ops())
        .expect("the store rejects unlinked ranges and stays coherent");
    assert!(
        matches!(
            outcomes[1].header_range_error(),
            CommitHeaderRangeError::UnlinkedRange { .. }
        ),
        "expected UnlinkedRange, got {:?}",
        outcomes[1]
    );
}

/// Re-delivery over committed bodies (bug 2): before the committed-height
/// gate covered the header/hash/height/body-size writes (it originally gated
/// only the *roots* write), a header range re-delivered over heights whose
/// bodies had since been committed re-inserted zakura rows *below* the body
/// tip — rows the release trim (which already ran at body-commit time) never
/// removes.
fn redelivery_over_bodies_ops() -> Vec<Op> {
    vec![
        commit_trunk(),
        Op::Finalize { count: 10 },
        // Re-delivery spanning body-backed heights 6..=10 and header-only
        // heights above: accepted, but the body-backed heights are skipped.
        Op::CommitHeaderRange {
            source: Source::Trunk,
            offset: 5,
            len: 15,
            anchor: Anchor::Auto,
        },
    ]
}

/// Re-delivery over bodies leaves no zakura rows below the body tip.
#[test]
fn redelivery_over_bodies_upholds_invariants() {
    let _init_guard = zakura_test::init();
    let mut harness = Harness::new();
    let outcomes = harness
        .run_all(&redelivery_over_bodies_ops())
        .expect("re-delivery over bodies leaves no zakura rows below the body tip");
    outcomes.iter().for_each(assert_accepted);
}

/// Seed above a gap (bug 3, minimal shape — found by the discovery proptest
/// and shrunk to a single op): before the parent-linkage refusal in
/// `prepare_zakura_header_from_committed_block`, seeding a block whose parent
/// row is absent left a row the chain walk cannot reach.
fn seed_above_gap_ops() -> Vec<Op> {
    vec![Op::Seed {
        source: Source::Trunk,
        index: 1, // trunk height 2; nothing exists at height 1
    }]
}

/// A seed above a gap is refused as a no-op instead of stranding a row.
#[test]
fn seed_above_gap_upholds_invariants() {
    let _init_guard = zakura_test::init();
    let mut harness = Harness::new();
    harness
        .run_all(&seed_above_gap_ops())
        .expect("a seed above a gap must not strand an unreachable row");

    let genesis_hash = universe().genesis.hash();
    assert_eq!(
        harness.state().best_header_tip(),
        Some((Height(0), genesis_hash)),
        "the refused seed must not move the header tip",
    );
}

/// Seed fork switch (bug 3, production shape): the zakura store follows
/// branch A; the non-finalized best chain switches to branch B and its new
/// best tip (B's *second* row) is seeded. Before the parent-linkage refusal,
/// the seed's non-conflict arm inserted the row directly over A's truncated
/// parent — broken linkage on disk, the poisoned-DAA-window generator from
/// the production incident table. (`s11` proves the refused seed converges
/// later through range delivery.)
fn seed_fork_switch_ops() -> Vec<Op> {
    vec![
        commit_trunk(),
        Op::Seed {
            source: Source::Branch(BRANCH_A),
            index: 0,
        },
        Op::Seed {
            source: Source::Branch(BRANCH_B),
            index: 1,
        },
    ]
}

/// A fork-switch seed keeps the store linked (the unlinked seed is refused).
#[test]
fn seed_fork_switch_upholds_invariants() {
    let _init_guard = zakura_test::init();
    let mut harness = Harness::new();
    harness
        .run_all(&seed_fork_switch_ops())
        .expect("a fork-switch seed must keep the store linked");

    let a_first = &universe().branches[BRANCH_A].headers[0];
    assert_eq!(
        harness.state().best_header_tip(),
        Some((a_first.height, a_first.hash)),
        "the store stays on A until a linked delivery of B arrives",
    );
}
