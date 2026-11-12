//! The fork-choice check's rules, on engines built from fixture chains.

use chrono::{DateTime, Utc};
use zakura_chain::block;
use zakura_header_chain::{ChainScore, Frontier, SuffixWork};
use zakura_node_services::header_chain::Port as _;
use zakura_storage::header::{adapter::EnginePort, controlled::OpId, evidence::ResultIdentity};

use super::*;
use crate::harness::header_node::{preload, ready, Chain, Fork};

/// A fixture with branch 2 at weight 2, and an engine holding five
/// unweighted headers on branch 1 and three on branch 2, both forked from
/// genesis, so the shorter branch 2 has more work.
fn weighted() -> (HeaderFixture, EnginePort) {
    let fixture = HeaderFixture::regtest().with_weight(2, 2);
    let port = EnginePort::new(&fixture);
    for (branch, len) in [(1, 5), (2, 3)] {
        preload(
            &port,
            &fixture,
            &Chain {
                main: 0,
                fork: Some(Fork { branch, len }),
            },
        );
    }
    (fixture, port)
}

/// The tip of `len` headers on `branch` from genesis.
fn tip(fixture: &HeaderFixture, branch: u8, len: u32) -> Frontier {
    let headers = fixture.extend(fixture.anchor(), len, branch);
    Frontier::new(block::Height(len), headers.last().expect("len > 0").hash())
}

/// The work of `len` headers on `branch`.
fn work(fixture: &HeaderFixture, branch: u8, len: u32) -> U256 {
    U256::from(fixture.target(branch).to_work().expect("valid")) * U256::from(len)
}

/// `snapshot` selecting `tip` with suffix work `work`.
fn selecting(mut snapshot: EngineSnapshot, tip: Frontier, work: U256) -> EngineSnapshot {
    snapshot.frontiers.header_best = tip;
    snapshot.header_best_score = ChainScore::new(SuffixWork::new(work), tip.hash);
    snapshot
}

/// A check on `fixture`'s branches 1 and 2, seeded with `port`'s engine and
/// `snapshot`, with what its first drain found.
fn seeded(
    fixture: &HeaderFixture,
    port: &EnginePort,
    snapshot: &EngineSnapshot,
) -> (ForkChoice, ForkChoiceEvidence, Result<Vec<String>, String>) {
    let evidence = ForkChoiceEvidence::new(&EvidenceStores::default());
    let mut check = ForkChoice::new(
        evidence.clone(),
        fixture.clone(),
        [1, 2],
        &port.durable_state(),
        snapshot,
        now(),
    );
    let drained = check.drain(1);
    (check, evidence, drained)
}

/// The clock now.
fn now() -> DateTime<Utc> {
    zakura_rt::time::utc_now().into()
}

/// A reset run that left `port`'s engine as it is, giving `snapshot`.
fn reset_run(port: &EnginePort, snapshot: EngineSnapshot) -> EngineRun {
    EngineRun {
        kind: RunKind::Reset,
        clock: now(),
        before: port.durable_state(),
        after: port.durable_state(),
        snapshot,
    }
}

/// A `Reset` fact naming `target`.
fn reset_fact(invalidate: bool, target: block::Hash) -> StorageFact {
    StorageFact::Reset {
        kind: if invalidate {
            "invalidate"
        } else {
            "reconsider"
        }
        .to_owned(),
        target: hash(target),
        generation: 0,
    }
}

/// The engine's own selection of the shorter heavier branch holds, and the
/// record applies.
#[test]
fn the_heavier_shorter_tip_holds() {
    let (fixture, port) = weighted();
    let snapshot = port.snapshot();
    assert_eq!(snapshot.frontiers.header_best, tip(&fixture, 2, 3));
    let (_, evidence, drained) = seeded(&fixture, &port, &snapshot);
    assert_eq!(drained, Ok(Vec::new()));
    assert!(evidence.applicable_by(1));
}

/// Selecting the longer branch's tip, with its own work as the score,
/// breaks the selection rule.
#[test]
fn the_longer_lighter_tip_breaks() {
    let (fixture, port) = weighted();
    let snapshot = selecting(port.snapshot(), tip(&fixture, 1, 5), work(&fixture, 1, 5));
    let (_, _, drained) = seeded(&fixture, &port, &snapshot);
    let breaks = drained.expect("no evidence is missing");
    assert_eq!(breaks.len(), 1, "{breaks:?}");
    assert!(
        breaks[0].contains("not one of the max-work eligible tips"),
        "{breaks:?}"
    );
}

/// The right tip with a wrong score breaks only the score.
#[test]
fn a_wrong_score_breaks() {
    let (fixture, port) = weighted();
    let snapshot = selecting(port.snapshot(), tip(&fixture, 2, 3), U256::zero());
    let (_, _, drained) = seeded(&fixture, &port, &snapshot);
    let breaks = drained.expect("no evidence is missing");
    assert_eq!(breaks.len(), 1, "{breaks:?}");
    assert!(breaks[0].contains("scored the selected tip"), "{breaks:?}");
}

/// A wrong score a run's snapshot copies from the metadata row the
/// commit-contents check judged is the row's, which another property owns,
/// so it doesn't break this rule.
#[test]
fn a_wrong_score_copied_from_a_judged_row_is_not_this_rules() {
    let (fixture, port) = weighted();
    let (mut check, _, _) = seeded(&fixture, &port, &port.snapshot());
    let snapshot = selecting(port.snapshot(), tip(&fixture, 2, 3), U256::zero());
    let mut run = EngineRun {
        kind: RunKind::Apply(OpId(1), ResultIdentity::Applied),
        ..reset_run(&port, snapshot.clone())
    };
    run.after.metadata.header_best_score = snapshot.header_best_score;
    check.run(&run, false);
    assert_eq!(check.drain(2), Ok(Vec::new()));
}

/// No run judged the seed's metadata row, so a wrong score the seed's
/// snapshot copies from it breaks this rule (review round 6).
#[test]
fn the_seeds_copied_score_is_judged() {
    let (fixture, port) = weighted();
    let snapshot = selecting(port.snapshot(), tip(&fixture, 2, 3), U256::zero());
    let mut state = port.durable_state();
    state.metadata.header_best_score = snapshot.header_best_score;
    let mut check = ForkChoice::new(
        ForkChoiceEvidence::new(&EvidenceStores::default()),
        fixture,
        [1, 2],
        &state,
        &snapshot,
        now(),
    );
    let breaks = check.drain(1).expect("no evidence is missing");
    assert_eq!(breaks.len(), 1, "{breaks:?}");
}

/// With two tied tips, a snapshot naming the other tip while copying the
/// row's score for its own breaks: the score names another tip than the
/// snapshot selects, which no row defect explains.
#[test]
fn a_tied_tip_with_the_rows_score_breaks() {
    let fixture = HeaderFixture::regtest();
    let port = EnginePort::new(&fixture);
    for branch in [1, 2] {
        preload(
            &port,
            &fixture,
            &Chain {
                main: 0,
                fork: Some(Fork { branch, len: 4 }),
            },
        );
    }
    let row = port.snapshot();
    let other = [tip(&fixture, 1, 4), tip(&fixture, 2, 4)]
        .into_iter()
        .find(|tip| *tip != row.frontiers.header_best)
        .expect("two tips");
    let mut snapshot = row.clone();
    snapshot.frontiers.header_best = other;
    let (_, _, drained) = seeded(&fixture, &port, &snapshot);
    let breaks = drained.expect("no evidence is missing");
    assert_eq!(breaks.len(), 1, "{breaks:?}");
    assert!(breaks[0].contains("scored the selected tip"), "{breaks:?}");
}

/// Equal work allows either tip.
#[test]
fn equal_work_allows_either_tip() {
    let fixture = HeaderFixture::regtest();
    let port = EnginePort::new(&fixture);
    for branch in [1, 2] {
        preload(
            &port,
            &fixture,
            &Chain {
                main: 0,
                fork: Some(Fork { branch, len: 4 }),
            },
        );
    }
    for branch in [1, 2] {
        let snapshot = selecting(
            port.snapshot(),
            tip(&fixture, branch, 4),
            work(&fixture, branch, 4),
        );
        let (_, _, drained) = seeded(&fixture, &port, &snapshot);
        assert_eq!(drained, Ok(Vec::new()), "branch {branch}");
    }
}

/// Once a reset invalidates the heavier branch's first header, an engine
/// still on its tip breaks the rule, and one on the other branch holds.
/// Reconsidering it brings the heavier tip back.
#[test]
fn a_reset_moves_the_expected_tip() {
    let (fixture, port) = weighted();
    let heavier = port.snapshot();
    let (mut check, _, drained) = seeded(&fixture, &port, &heavier);
    assert_eq!(drained, Ok(Vec::new()));
    let first = fixture.extend(fixture.anchor(), 1, 2)[0].hash();

    check.storage(&reset_fact(true, first));
    check.run(&reset_run(&port, heavier.clone()), false);
    let breaks = check.drain(2).expect("no evidence is missing");
    assert_eq!(breaks.len(), 1, "{breaks:?}");
    assert!(breaks[0].contains("eligible false"), "{breaks:?}");

    let lighter = selecting(heavier.clone(), tip(&fixture, 1, 5), work(&fixture, 1, 5));
    check.run(
        &EngineRun {
            kind: RunKind::Apply(OpId(1), ResultIdentity::Applied),
            ..reset_run(&port, lighter.clone())
        },
        false,
    );
    assert_eq!(check.drain(3), Ok(Vec::new()));

    check.storage(&reset_fact(false, first));
    check.run(&reset_run(&port, lighter), false);
    assert_eq!(check.drain(4).expect("no evidence is missing").len(), 1);
    check.run(
        &EngineRun {
            kind: RunKind::Apply(OpId(2), ResultIdentity::Applied),
            ..reset_run(&port, heavier)
        },
        false,
    );
    assert_eq!(check.drain(5), Ok(Vec::new()));
}

/// A record holding a header the fixture tree doesn't give isn't judged.
#[test]
fn a_header_outside_the_fixture_is_not_judged() {
    let (fixture, port) = weighted();
    let mut state = port.durable_state();
    let (key, node) = state
        .graph
        .nodes
        .iter()
        .find(|(key, _)| **key != fixture.anchor().hash)
        .map(|(key, node)| (*key, node.clone()))
        .expect("the engine holds headers above the anchor");
    let mut foreign = key.0;
    foreign[0] ^= 1;
    state.graph.nodes.insert(block::Hash(foreign), node);
    let evidence = ForkChoiceEvidence::new(&EvidenceStores::default());
    let snapshot = selecting(port.snapshot(), tip(&fixture, 1, 5), work(&fixture, 1, 5));
    let mut check = ForkChoice::new(evidence.clone(), fixture, [1, 2], &state, &snapshot, now());
    assert_eq!(check.drain(1), Ok(Vec::new()));
    assert!(!evidence.any());
}

/// A reset run with no `Reset` fact is missing evidence.
#[test]
fn a_reset_run_without_its_fact_is_missing_evidence() {
    let (fixture, port) = weighted();
    let snapshot = port.snapshot();
    let (mut check, _, _) = seeded(&fixture, &port, &snapshot);
    check.run(&reset_run(&port, snapshot), false);
    assert!(check.drain(2).is_err());
}

/// A locator read makes no record.
#[test]
fn a_locator_read_is_not_judged() {
    let (fixture, port) = weighted();
    let (mut check, evidence, _) = seeded(&fixture, &port, &port.snapshot());
    let wrong = selecting(port.snapshot(), tip(&fixture, 1, 5), work(&fixture, 1, 5));
    check.run(
        &EngineRun {
            kind: RunKind::Locator(OpId(1), None),
            ..reset_run(&port, wrong)
        },
        false,
    );
    assert_eq!(check.drain(2), Ok(Vec::new()));
    assert!(evidence.applicable_by(1));
}

/// A run the commit-contents check found corrupt isn't judged, and neither
/// is any later record, so a corrupt metadata score, which the snapshot
/// copies, fails only `commit-only-authorized`.
#[test]
fn a_corrupt_run_taints_every_later_record() {
    let (fixture, port) = weighted();
    let (mut check, evidence, _) = seeded(&fixture, &port, &port.snapshot());
    let wrong = selecting(port.snapshot(), tip(&fixture, 2, 3), U256::zero());
    let apply = |id| EngineRun {
        kind: RunKind::Apply(OpId(id), ResultIdentity::Applied),
        ..reset_run(&port, wrong.clone())
    };
    check.run(&apply(1), true);
    assert_eq!(check.drain(2), Ok(Vec::new()));
    check.run(&apply(2), false);
    assert_eq!(check.drain(3), Ok(Vec::new()));
    assert!(evidence.applicable_by(1));
    assert!(!evidence.applicable_by(0));
    assert!(evidence.tainted_by(2));
    assert!(!evidence.tainted_by(1));
}

/// A harness commit's engine run is a record like an apply's, judged on
/// the engine it left whatever its snapshot says (review rounds 2 and 3):
/// here the engine holds a heavier branch 1 while the run's snapshot still
/// names the seed's tip, with the seed's state version.
#[test]
fn a_harness_commit_run_is_judged_whatever_its_snapshot_says() {
    let (fixture, port) = weighted();
    let seed = port.snapshot();
    let (mut check, _, drained) = seeded(&fixture, &port, &seed);
    assert_eq!(drained, Ok(Vec::new()));
    // Twelve more headers on branch 1 make it the heaviest.
    let parent = tip(&fixture, 1, 5);
    let headers = fixture.extend(parent, 12, 1);
    let request = fixture.target_request(&port.snapshot(), parent, &headers, 9);
    let prepared = ready(port.prepare_header_target(request)).expect("fixture headers prepare");
    ready(port.apply_header_target(prepared)).expect("fixture headers apply");
    assert_eq!(port.snapshot().frontiers.header_best, tip(&fixture, 1, 17));
    let commit = |snapshot| EngineRun {
        kind: RunKind::HarnessCommit(headers.iter().map(|header| header.hash()).collect()),
        ..reset_run(&port, snapshot)
    };
    check.run(&commit(seed.clone()), false);
    let breaks = check.drain(2).expect("no evidence is missing");
    assert_eq!(breaks.len(), 1, "{breaks:?}");
    assert!(breaks[0].contains("after a harness commit"), "{breaks:?}");
    check.run(&commit(port.snapshot()), false);
    assert_eq!(check.drain(3), Ok(Vec::new()));
}

/// A harness commit the commit-contents check finds corrupt taints fork
/// choice as a port run's does, so a wrong row it wrote fails only that
/// check's ID (review rounds 4 and 5).
#[test]
fn a_corrupt_harness_commit_taints() {
    let (fixture, port) = weighted();
    let (mut check, evidence, _) = seeded(&fixture, &port, &port.snapshot());
    let wrong = selecting(port.snapshot(), tip(&fixture, 1, 5), work(&fixture, 1, 5));
    check.run(
        &EngineRun {
            kind: RunKind::HarnessCommit(Vec::new()),
            ..reset_run(&port, wrong)
        },
        true,
    );
    assert_eq!(check.drain(2), Ok(Vec::new()));
    assert!(evidence.tainted_by(2));
}

/// A header admitted more than two hours ahead of its record's clock is
/// deferred, so it and its descendants are ineligible. With the seed's
/// clock just before the first fixture header comes due, every header
/// above the anchor is deferred, so the anchor is the only eligible tip,
/// and stays so at a later record's clock, since nothing promotes a
/// deferred header.
#[test]
fn a_deferred_admission_stays_ineligible() {
    let (fixture, port) = weighted();
    let first = fixture.extend(fixture.anchor(), 1, 2).remove(0);
    let early = DateTime::<Utc>::from_timestamp(first.time.timestamp() - 2 * 60 * 60 - 1, 0)
        .expect("a valid time");
    let lighter = selecting(port.snapshot(), tip(&fixture, 1, 5), work(&fixture, 1, 5));
    let anchor = selecting(port.snapshot(), fixture.anchor(), U256::zero());
    let evidence = ForkChoiceEvidence::new(&EvidenceStores::default());
    let mut check = ForkChoice::new(
        evidence,
        fixture.clone(),
        [1, 2],
        &port.durable_state(),
        &anchor,
        early,
    );
    assert_eq!(check.drain(1), Ok(Vec::new()));
    check.run(
        &EngineRun {
            clock: now(),
            kind: RunKind::Apply(OpId(1), ResultIdentity::Applied),
            ..reset_run(&port, lighter)
        },
        false,
    );
    assert_eq!(check.drain(2).expect("no evidence is missing").len(), 1);
}

/// The setup observation's drain at step 0 holds the seed's breaks and
/// applicability for step 1's, which a property reads.
#[test]
fn the_seed_waits_for_step_one() {
    let (fixture, port) = weighted();
    let wrong = selecting(port.snapshot(), tip(&fixture, 2, 3), U256::zero());
    let evidence = ForkChoiceEvidence::new(&EvidenceStores::default());
    let mut check = ForkChoice::new(
        evidence.clone(),
        fixture,
        [1, 2],
        &port.durable_state(),
        &wrong,
        now(),
    );
    assert_eq!(check.drain(0), Ok(Vec::new()));
    assert!(!evidence.any());
    assert_eq!(check.drain(1).expect("no evidence is missing").len(), 1);
    assert!(evidence.applicable_by(1));
}

/// A settled point counted after the node's stores froze is dropped.
#[test]
fn a_frozen_fork_choice_store_drops_a_settled_point() {
    crate::stores::assert_freeze_drops_writes(
        ForkChoiceEvidence::new,
        |evidence| {
            evidence.change(|store| store.settled_points += 1);
        },
        ForkChoiceEvidence::settled_points,
    );
}

/// A claims store and fork-choice evidence with no tap behind them.
#[derive(Clone)]
struct Forks {
    claims: crate::harness::header_node::claim::ClaimEvidence,
    fork: ForkChoiceEvidence,
}

impl Default for Forks {
    /// Empty stores in one handle of their own.
    fn default() -> Self {
        let stores = EvidenceStores::default();
        Forks {
            claims: crate::harness::header_node::claim::ClaimEvidence::new(&stores),
            fork: ForkChoiceEvidence::new(&stores),
        }
    }
}

impl crate::domain::header_sync::ForkView for Forks {
    fn claims(&self) -> &crate::harness::header_node::claim::ClaimEvidence {
        &self.claims
    }

    fn fork_choice(&self) -> &ForkChoiceEvidence {
        &self.fork
    }
}

impl crate::property::HasView for Forks {
    type View = Forks;

    /// A copy sharing the stores.
    fn view(&self) -> Forks {
        self.clone()
    }

    /// The stores, as a node harness reports its own.
    fn snapshot(&self) -> Option<serde_json::Value> {
        Some(self.fork.0.stores().report_snapshot())
    }
}

/// The claim break [`Forks`]' one boundary keeps.
const UNSELECTED: &str = "ExpectedChainSelected: the engine's tip isn't a max-work eligible one";

/// The convergence break [`Forks`]' one boundary keeps at its settled point.
const UNCONVERGED: &str = "the settled engine's tip isn't a heaviest mirror tip";

impl crate::harness::scripted::Script for Forks {
    const NAME: &'static str = "test/fork-choice-findings";
    type Case = ();

    /// Only `expected-chain-selected`.
    fn properties() -> Vec<crate::property::Property<crate::harness::scripted::ScriptRun<Forks>>> {
        vec![crate::domain::header_sync::expected_chain_selected()]
    }

    /// The schedule and the view.
    fn sources() -> crate::property::Sources {
        use crate::property::Source;
        crate::property::Sources::observed(&[Source::Schedule, Source::View])
    }

    /// At step 1, after which no task can run: a selection break the drain
    /// found, and a settled point whose convergence broke, with the check
    /// applicable, as the tap keeps them.
    fn observe(&self, (): &(), log: &mut crate::ObservationLog) {
        let step = log.step();
        if step != 1 {
            return;
        }
        self.claims
            .record(crate::harness::header_node::claim::ClaimRecord {
                step,
                claim: crate::harness::header_node::claim::Claim::ExpectedChainSelected,
                detail: UNSELECTED.to_owned(),
                attribution: None,
            });
        self.fork.change(|store| {
            store.applicable = Some(step);
            store.settled = Some((step, Some(UNCONVERGED.to_owned())));
            store.settled_points += 1;
        });
    }
}

/// C21 through the sidecar: a step whose drain found a selection break
/// and a settled point off the heaviest mirror chain, with no task able to
/// run after it, is two `expected-chain-selected` violations there, both
/// in the full report recorded beside the trace and its fresh replay.
#[test]
fn a_selection_break_beside_a_settled_break_reaches_the_sidecar() {
    use crate::{
        domain::header_sync::EXPECTED_CHAIN_SELECTED,
        harness::scripted::{findings, sidecar},
    };
    let full = sidecar::<Forks>(&(), &[]);
    let found = findings(&full, EXPECTED_CHAIN_SELECTED);
    let actual: Vec<(u64, &str, &str)> = found
        .violations
        .iter()
        .map(|violation| {
            (
                violation.step,
                violation.expected.as_str(),
                violation.actual.as_str(),
            )
        })
        .collect();
    assert_eq!(
        actual,
        [
            (
                1,
                "the engine selects a max-work eligible tip, scored with its work",
                UNSELECTED
            ),
            (
                1,
                "a settled node in the fork fixture is on the heaviest mirror chain",
                UNCONVERGED
            ),
        ]
    );
    assert_eq!(found.end_found, 0, "both at their step: {found:?}");
    assert_eq!(
        found.outcome,
        crate::PropertyOutcome::Violated { witness: vec![1] }
    );
}
