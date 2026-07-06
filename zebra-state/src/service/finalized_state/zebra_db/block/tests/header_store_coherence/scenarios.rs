//! Scripted scenarios: production event shapes replayed against the store.
//!
//! Every scenario runs with a per-op audit (via `Harness::run`), so any
//! passing scenario is a regression gate on the whole write sequence, not
//! just its final assertions.
//!
//! The `corruption_repro_*` tests at the bottom deliberately demonstrate
//! store-invariant violations that exist today: they PASS while the bug
//! exists and fail loudly once the write path is fixed, forcing re-triage.
//! Their `#[ignore]`d twins assert the true invariant, to be un-ignored
//! together with the write-path fix.

use zebra_chain::block::Height;

use super::{
    audit::Violation,
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

fn branch_tip(branch: usize) -> (Height, zebra_chain::block::Hash) {
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
    let _init_guard = zebra_test::init();
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
    let _init_guard = zebra_test::init();
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
    let _init_guard = zebra_test::init();
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
    let _init_guard = zebra_test::init();
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
    let _init_guard = zebra_test::init();
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
    let _init_guard = zebra_test::init();
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
/// heights strands rows — that is `corruption_repro_redelivery_over_bodies`.)
#[test]
fn s05_release_trim_then_redelivery() {
    let _init_guard = zebra_test::init();
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
    let _init_guard = zebra_test::init();
    let mut harness = Harness::new();

    let outcomes = harness
        .run_all(&[commit_trunk(), commit_branch(BRANCH_B)])
        .expect("setup is clean");
    outcomes.iter().for_each(assert_accepted);
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
}

/// s07: double reorg at the same fork point (B → A → B_ext), then the losing
/// branch's re-delivery is refused without side effects.
#[test]
fn s07_double_reorg_same_fork_point() {
    let _init_guard = zebra_test::init();
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
    let _init_guard = zebra_test::init();
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
    let _init_guard = zebra_test::init();
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
    let _init_guard = zebra_test::init();
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

// ---------------------------------------------------------------------------
// Corruption reproductions: deterministic sequences that violate the store
// invariants today. See the module doc for the pass/ignore convention.
// ---------------------------------------------------------------------------

/// The unlinked-anchor commit: `prepare_header_range_batch_with_roots` never
/// checks that `headers[0].previous_block_hash == anchor` (or any intra-range
/// linkage). A range of branch-A headers anchored at the same-height *trunk*
/// hash passes contextual difficulty validation — the two fast chains have
/// identical (time, threshold) sequences — and commits a suffix that does not
/// link to the row below it: an on-disk I2 violation. Production reach: a
/// header range is untrusted peer input; nothing upstream of the store
/// re-checks the anchor linkage.
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

/// PASSES while the bug exists (proves the repro); flips when the writer is
/// fixed, forcing this pair to be re-triaged.
#[test]
fn corruption_repro_unlinked_anchor_commit() {
    let _init_guard = zebra_test::init();
    let mut harness = Harness::new();

    let report = harness
        .run_all(&unlinked_anchor_ops())
        .expect_err("the unlinked-anchor commit corrupts the store today");

    assert!(
        report
            .violations
            .iter()
            .any(|violation| matches!(violation, Violation::BrokenLinkage { height, .. } if *height == Height(FORK_HEIGHT + 2))),
        "expected BrokenLinkage right above the spliced anchor: {report:?}"
    );
    assert!(
        !report.mismatches.is_empty(),
        "the oracle rejects this range; the store accepted it: {report:?}"
    );
}

/// The true invariant. Un-ignore when the write-path fix lands.
#[test]
#[ignore = "known zakura header-store corruption: un-ignore with the write-path fix"]
fn unlinked_anchor_commit_upholds_invariants() {
    let _init_guard = zebra_test::init();
    let mut harness = Harness::new();
    harness
        .run_all(&unlinked_anchor_ops())
        .expect("the store rejects unlinked ranges and stays coherent");
}

/// Re-delivery over committed bodies: the range insert loop
/// (`prepare_header_range_batch_with_roots`) gates only its *roots* write on
/// `contains_body_at_height` — the header/hash/height/body-size writes are
/// unconditional. A header range re-delivered over heights whose bodies have
/// since been committed (a header store behind the body store, or a late range
/// response racing body sync — the exact scenario the roots gate's own comment
/// describes) re-inserts zakura rows *below* the body tip. That breaks the
/// frontier-overlay invariant ("the Zakura header store only ever holds
/// heights with no committed body", block.rs release-path doc): the release
/// trim already ran at body-commit time, so nothing ever removes these rows.
fn redelivery_over_bodies_ops() -> Vec<Op> {
    vec![
        commit_trunk(),
        Op::Finalize { count: 10 },
        // Re-delivery spanning body-backed heights 6..=10 and header-only
        // heights above. The rows match the canonical chain, so the store
        // accepts — but the write strands zakura rows under the body tip.
        Op::CommitHeaderRange {
            source: Source::Trunk,
            offset: 5,
            len: 15,
            anchor: Anchor::Auto,
        },
    ]
}

/// PASSES while the bug exists (proves the repro); flips when the writer is
/// fixed, forcing this pair to be re-triaged.
#[test]
fn corruption_repro_redelivery_over_bodies() {
    let _init_guard = zebra_test::init();
    let mut harness = Harness::new();

    let report = harness
        .run_all(&redelivery_over_bodies_ops())
        .expect_err("re-delivery over committed bodies strands zakura rows today");

    assert!(
        report
            .violations
            .iter()
            .any(|violation| matches!(violation, Violation::ZakuraRowAtBodyHeight { height, .. } if *height <= Height(10))),
        "expected zakura rows stranded at body-backed heights: {report:?}"
    );
    assert!(
        report.mismatches.is_empty(),
        "acceptance itself is correct chain selection; the write effects are the bug: {report:?}"
    );
}

/// The true invariant. Un-ignore when the write-path fix lands.
#[test]
#[ignore = "known zakura header-store corruption: un-ignore with the write-path fix"]
fn redelivery_over_bodies_upholds_invariants() {
    let _init_guard = zebra_test::init();
    let mut harness = Harness::new();
    harness
        .run_all(&redelivery_over_bodies_ops())
        .expect("re-delivery over bodies leaves no zakura rows below the body tip");
}

/// Seed above a gap — found by `prop_random_sequences_uphold_invariants` and
/// shrunk to a single op. The seed path
/// (`prepare_zakura_header_from_committed_block`) writes header/hash/height
/// rows at its height with **no linkage or anchor precondition**: seeding a
/// block whose parent row is absent leaves a row the chain walk cannot reach.
/// Production shape: seeds fire only at non-finalized best-*tip* commits, so
/// any nf best-tip jump — a fork switch between nf chains, or a restart that
/// restores the nf backup and then commits on top — seeds a height whose
/// parent row was never written.
fn seed_above_gap_ops() -> Vec<Op> {
    vec![Op::Seed {
        source: Source::Trunk,
        index: 1, // trunk height 2; nothing exists at height 1
    }]
}

/// PASSES while the bug exists (proves the repro); flips when the writer is
/// fixed, forcing this pair to be re-triaged.
#[test]
fn corruption_repro_seed_above_gap() {
    let _init_guard = zebra_test::init();
    let mut harness = Harness::new();

    let report = harness
        .run_all(&seed_above_gap_ops())
        .expect_err("seeding above a gap strands an unreachable row today");

    assert!(
        report
            .violations
            .iter()
            .any(|violation| matches!(violation, Violation::RowAboveLastLinked { height, .. } if *height == Height(2))),
        "expected the seeded row to be unreachable from the finalized tip: {report:?}"
    );
    assert!(
        report
            .violations
            .iter()
            .any(|violation| matches!(violation, Violation::BestHeaderTipMismatch { .. })),
        "expected best_header_tip to point above the linked chain: {report:?}"
    );
}

/// The true invariant. Un-ignore when the write-path fix lands.
#[test]
#[ignore = "known zakura header-store corruption: un-ignore with the write-path fix"]
fn seed_above_gap_upholds_invariants() {
    let _init_guard = zebra_test::init();
    let mut harness = Harness::new();
    harness
        .run_all(&seed_above_gap_ops())
        .expect("a seed above a gap must not strand an unreachable row");
}

/// Seed fork switch — the production-shaped variant of the seed linkage hole.
/// The zakura store follows branch A (seeded at the fork height); the
/// non-finalized best chain switches to branch B and its new best tip (B's
/// *second* row) is seeded. The seed's non-conflict arm inserts the row
/// directly — B's parent row was truncated, so the store now holds
/// `A[0]` at the fork height and `B[1]` right above it: broken linkage on
/// disk, exactly the poisoned-DAA-window generator from the incident table.
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

/// PASSES while the bug exists (proves the repro); flips when the writer is
/// fixed, forcing this pair to be re-triaged.
#[test]
fn corruption_repro_seed_fork_switch() {
    let _init_guard = zebra_test::init();
    let mut harness = Harness::new();

    let report = harness
        .run_all(&seed_fork_switch_ops())
        .expect_err("a fork-switch seed breaks linkage today");

    assert!(
        report
            .violations
            .iter()
            .any(|violation| matches!(violation, Violation::BrokenLinkage { height, .. } if *height == Height(FORK_HEIGHT + 2))),
        "expected broken linkage right above the fork height: {report:?}"
    );
}

/// The true invariant. Un-ignore when the write-path fix lands.
#[test]
#[ignore = "known zakura header-store corruption: un-ignore with the write-path fix"]
fn seed_fork_switch_upholds_invariants() {
    let _init_guard = zebra_test::init();
    let mut harness = Harness::new();
    harness
        .run_all(&seed_fork_switch_ops())
        .expect("a fork-switch seed must keep the store linked");
}
