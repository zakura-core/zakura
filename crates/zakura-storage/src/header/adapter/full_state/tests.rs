//! Full state on the engine port: commits, the views they publish, the
//! missing-body query and range reads.

use std::{
    future::Future,
    num::NonZeroUsize,
    pin::pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use zakura_chain::{
    block,
    parameters::constants::{MAX_BLOCK_REORG_HEIGHT, MAX_NON_FINALIZED_CHAIN_FORKS},
};
use zakura_header_chain::{
    ApplyResult, BodyUnavailableSummary, EvidenceId, Frontier, OperatorBodyRetry, StateVersion,
    TransitionEvent, TransitionRequest, VerifiedChangeCause,
};
use zakura_node_services::header_chain::{ApplyHeaderTargetOutcome, Port};

use super::{Applied, EnginePort};
use crate::block::fixture::{BlockFixture, BodySpec, FixtureBlock};
use crate::header::controlled::ControlledPort;

/// Polls `future` once. Every engine-port operation runs when first polled.
fn ready<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("the engine port completes each operation on its first poll"),
    }
}

/// Inserts `blocks`' headers on `parent`.
fn insert(port: &EnginePort, parent: Frontier, blocks: &[FixtureBlock]) {
    let request = BlockFixture::regtest().target_request(&port.snapshot(), parent, blocks, 1);
    let prepared = ready(port.prepare_header_target(request)).expect("fixture headers prepare");
    assert_eq!(
        ready(port.apply_header_target(prepared)).expect("fixture headers apply"),
        ApplyHeaderTargetOutcome::Applied
    );
}

/// Commits `block` with `receipt`, expecting the engine to commit it.
fn commit(port: &EnginePort, block: &FixtureBlock, receipt: u64) -> Applied {
    let applied = port
        .commit_block(block.block.clone(), receipt)
        .expect("a fixture body commits");
    assert_eq!(applied.result, ApplyResult::Committed, "{}", block.height.0);
    applied
}

fn verified_best(port: &EnginePort) -> Frontier {
    port.snapshot().frontiers.verified_best
}

/// ST4.3: a commit on the best chain grows it, a longer fork resets it and
/// starts the next body-work epoch, and a shorter fork is a side path. Each
/// publishes the view it installed.
#[test]
fn commits_grow_reset_and_publish_views() {
    let fixture = BlockFixture::regtest();
    let port = EnginePort::new(fixture.headers());
    let anchor = fixture.anchor();
    let main = fixture.uniform(anchor, 2, BodySpec::exact(2_000), 0);
    let fork = fixture.uniform(anchor, 3, BodySpec::exact(2_000), 1);
    insert(&port, anchor, &main);
    insert(&port, anchor, &fork);
    let epoch = port.view().body_work_epoch;

    let grown = commit(&port, &main[0], 1);
    assert_eq!(grown.cause, Some(VerifiedChangeCause::Grow));
    let installed = grown.installed.expect("a grow installs");
    assert!(!installed.effect.invalidates_body_work());
    assert_eq!(installed.view.body_work_epoch, epoch);
    assert_eq!(installed.view.snapshot, port.snapshot());
    assert_eq!(port.view().snapshot, port.snapshot());
    assert_eq!(verified_best(&port), main[0].frontier());

    // A one-block fork is a side path: the verified tip stays.
    let side = commit(&port, &fork[0], 2);
    assert_eq!(side.cause, None);
    assert_eq!(verified_best(&port), main[0].frontier());

    // A longer fork resets the verified chain and starts the next epoch.
    let reset = commit(&port, &fork[1], 3);
    assert_eq!(reset.cause, Some(VerifiedChangeCause::Reset));
    assert_eq!(verified_best(&port), fork[1].frontier());
    let installed = reset.installed.expect("a reset installs");
    assert!(installed.effect.invalidates_body_work());
    let next = epoch.checked_next().expect("the epoch is small");
    assert_eq!(installed.view.body_work_epoch, next);
    assert_eq!(port.view(), installed.view);

    let grown = commit(&port, &fork[2], 4);
    assert_eq!(grown.cause, Some(VerifiedChangeCause::Grow));
    assert_eq!(port.view().body_work_epoch, next);
    port.with_full_state(|chain| {
        assert_eq!(chain.best_tip(), fork[2].frontier());
        assert_eq!(chain.len(), 4);
    });
}

/// ST4.3: between equal-work chains, the body the verifier received first
/// wins, whichever commits first, as `Chain`'s order breaks the tie.
#[test]
fn an_earlier_receipt_wins_an_equal_work_tie() {
    let fixture = BlockFixture::regtest();
    let port = EnginePort::new(fixture.headers());
    let anchor = fixture.anchor();
    let a = fixture.block(anchor, 0, BodySpec::exact(2_000));
    let b = fixture.block(anchor, 1, BodySpec::exact(2_000));
    insert(&port, anchor, std::slice::from_ref(&a));
    insert(&port, anchor, std::slice::from_ref(&b));
    let epoch = port.view().body_work_epoch;

    // A was received first, B committed first.
    let first = commit(&port, &b, 2);
    assert_eq!(first.cause, Some(VerifiedChangeCause::Grow));
    assert_eq!(verified_best(&port), b.frontier());
    let second = commit(&port, &a, 1);
    assert_eq!(second.cause, Some(VerifiedChangeCause::Reset));
    assert_eq!(verified_best(&port), a.frontier());
    let installed = second.installed.expect("a reset installs");
    assert!(installed.effect.invalidates_body_work());
    assert_eq!(
        installed.view.body_work_epoch,
        epoch.checked_next().expect("the epoch is small")
    );
}

/// ST4.3: past `MAX_NON_FINALIZED_CHAIN_FORKS` chains, full state evicts
/// the lowest-ranked chain other than the one it just extended, and the
/// engine accepts the eviction.
#[test]
fn full_state_evicts_the_lowest_ranked_fork() {
    let fixture = BlockFixture::regtest();
    let port = EnginePort::new(fixture.headers());
    let anchor = fixture.anchor();
    let forks: Vec<_> = (0..=MAX_NON_FINALIZED_CHAIN_FORKS)
        .map(|branch| {
            let branch = u8::try_from(branch).expect("the fork limit is small");
            fixture.block(anchor, branch, BodySpec::exact(2_000))
        })
        .collect();
    for block in &forks {
        insert(&port, anchor, std::slice::from_ref(block));
    }
    for (receipt, block) in (1..).zip(&forks) {
        commit(&port, block, receipt);
    }
    // Every chain has one block, so the latest receipt ranks lowest. The
    // last commit is exempt, so the one before it goes.
    let evicted = forks[MAX_NON_FINALIZED_CHAIN_FORKS - 1].hash();
    port.with_full_state(|chain| {
        assert_eq!(chain.tips().len(), MAX_NON_FINALIZED_CHAIN_FORKS);
        assert!(!chain.contains(evicted));
        for block in forks.iter().filter(|block| block.hash() != evicted) {
            assert!(chain.contains(block.hash()), "{:?}", block.hash());
        }
        assert_eq!(chain.best_tip(), forks[0].frontier());
    });
}

/// ST4.3: a commit the engine stalls on its resource limits installs the
/// stall alarm and publishes its view, but leaves full state unchanged.
#[test]
fn a_resource_stall_installs_its_alarm_and_leaves_full_state() {
    let fixture = BlockFixture::regtest();
    let mut port = EnginePort::new(fixture.headers());
    let anchor = fixture.anchor();
    let main = fixture.uniform(anchor, 3, BodySpec::exact(2_000), 0);
    let fork = fixture.block(anchor, 1, BodySpec::exact(2_000));
    insert(&port, anchor, &main);
    for (receipt, block) in (1..).zip(&main) {
        commit(&port, block, receipt);
    }
    // The graph holds the main chain's three headers. A verified fork adds
    // its own header, and both chains' tips protect their paths, which is
    // four nodes.
    port.config.limits.max_non_finalized_nodes = NonZeroUsize::new(3).expect("3 is nonzero");
    let before = port.snapshot();

    let stalled = port
        .commit_block(fork.block.clone(), 4)
        .expect("a stall is a result");
    let ApplyResult::ResourceStalled(receipt) = stalled.result else {
        panic!("the commit stalls: {:?}", stalled.result);
    };
    assert!(receipt.alarm_changed);
    let installed = stalled.installed.expect("the alarm change installs");
    assert!(installed.view.snapshot.alarms.resource_stalled);
    assert_eq!(port.view(), installed.view);
    assert_eq!(port.snapshot().frontiers, before.frontiers);
    port.with_full_state(|chain| {
        assert!(!chain.contains(fork.hash()));
        assert_eq!(chain.best_tip(), main[2].frontier());
    });
}

/// ST4.3: a stalled reset installs only the stall alarm, so it reports no
/// verified-chain change and keeps the body-work epoch.
#[test]
fn a_stalled_reset_reports_no_verified_change() {
    let fixture = BlockFixture::regtest();
    let mut port = EnginePort::new(fixture.headers());
    let anchor = fixture.anchor();
    let a = fixture.block(anchor, 0, BodySpec::exact(2_000));
    let b = fixture.block(anchor, 1, BodySpec::exact(2_000));
    insert(&port, anchor, std::slice::from_ref(&b));
    commit(&port, &b, 2);
    // A was received first, so its commit would reset to it. Its header
    // goes to the engine with the commit, past a one-node graph.
    port.config.limits.max_non_finalized_nodes = NonZeroUsize::new(1).expect("1 is nonzero");
    let epoch = port.view().body_work_epoch;

    let stalled = port
        .commit_block(a.block.clone(), 1)
        .expect("a stall is a result");
    assert!(matches!(stalled.result, ApplyResult::ResourceStalled(_)));
    assert_eq!(stalled.cause, None);
    let installed = stalled.installed.expect("the alarm change installs");
    assert!(!installed.effect.invalidates_body_work());
    assert_eq!(installed.view.body_work_epoch, epoch);
    assert_eq!(verified_best(&port), b.frontier());
}

/// ST4.3: the missing-body query lists the selected headers above the
/// committed anchor, with their advertised sizes, and nothing for an
/// unadvertised one.
#[test]
fn the_missing_body_query_lists_uncommitted_bodies_with_hints() {
    let fixture = BlockFixture::regtest();
    let port = EnginePort::new(fixture.headers());
    let anchor = fixture.anchor();
    let hinted = BodySpec {
        size: 2_000,
        hint: 500,
    };
    let unhinted = BodySpec {
        size: 2_000,
        hint: 0,
    };
    let main = fixture.extend(anchor, &[hinted, hinted, hinted, unhinted], 0);
    insert(&port, anchor, &main);
    commit(&port, &main[0], 1);
    commit(&port, &main[1], 2);

    let metadata = port
        .missing_block_body_metadata(block::Height(1), 10)
        .expect("the query reads");
    assert_eq!(metadata.anchor, main[1].frontier());
    assert_eq!(
        metadata.blocks,
        [
            (main[2].height, main[2].hash(), Some(500)),
            (main[3].height, main[3].hash(), None),
        ]
    );
    // The limit bounds the window.
    let one = port
        .missing_block_body_metadata(block::Height(3), 1)
        .expect("the query reads");
    assert_eq!(one.blocks.len(), 1);
}

/// ST4.3: a range read serves the committed prefix from its start, within
/// its count and byte budget, and nothing once cancelled.
#[test]
fn range_reads_serve_the_committed_prefix_within_budget() {
    let fixture = BlockFixture::regtest();
    let port = EnginePort::new(fixture.headers());
    let anchor = fixture.anchor();
    let main = fixture.uniform(anchor, 4, BodySpec::exact(2_000), 0);
    insert(&port, anchor, &main);
    for (receipt, block) in (1..).zip(&main[..3]) {
        commit(&port, block, receipt);
    }
    let bytes = |blocks: &[FixtureBlock]| -> Vec<_> {
        blocks
            .iter()
            .map(|block| (block.height, block.bytes()))
            .collect()
    };

    let all = port
        .read_block_range(block::Height(1), 10, u32::MAX, || false)
        .expect("the read succeeds");
    assert_eq!(all, bytes(&main[..3]), "the read stops at the last body");
    let budget = port
        .read_block_range(block::Height(1), 10, 2 * 2_000 + 1_999, || false)
        .expect("the read succeeds");
    assert_eq!(budget, bytes(&main[..2]), "a body past the budget stops it");
    let counted = port
        .read_block_range(block::Height(2), 1, u32::MAX, || false)
        .expect("the read succeeds");
    assert_eq!(counted, bytes(&main[1..2]));
    let missing = port
        .read_block_range(block::Height(4), 10, u32::MAX, || false)
        .expect("the read succeeds");
    assert!(missing.is_empty());
    let cancelled = port
        .read_block_range(block::Height(1), 10, u32::MAX, || true)
        .expect("the read succeeds");
    assert!(cancelled.is_empty());
}

/// ST4.3: full state never finalizes, so a commit past production's
/// non-finalized window panics instead of diverging from production.
#[test]
#[should_panic(expected = "non-finalized blocks")]
fn a_commit_past_the_non_finalized_window_panics() {
    let fixture = BlockFixture::regtest();
    let anchor = fixture.anchor();
    let chain = fixture.uniform(
        anchor,
        MAX_BLOCK_REORG_HEIGHT + 1,
        BodySpec::exact(2_000),
        0,
    );
    let mut full_state = crate::full_state::VerifiedChain::new(fixture.headers().genesis_block());
    for (receipt, block) in (1..).zip(&chain) {
        full_state = full_state.with(Arc::clone(&block.block), block.height, receipt);
    }
}

/// ST4.3: a body record reaches the engine, which finds a record at an old
/// version stale, and the port refuses a record of any other event.
#[test]
fn body_records_reach_the_engine() {
    let fixture = BlockFixture::regtest();
    let port = EnginePort::new(fixture.headers());
    let block = fixture.block(fixture.anchor(), 0, BodySpec::exact(2_000));
    insert(&port, fixture.anchor(), std::slice::from_ref(&block));
    let current = port.snapshot().state_version;
    let retry = OperatorBodyRetry {
        hash: block.hash(),
        evidence: EvidenceId::from_digest([7; 32]),
        availability: BodyUnavailableSummary::default(),
    };
    let stale = port
        .record_body_evidence(TransitionRequest {
            expected_version: StateVersion::new(current.get() - 1),
            event: TransitionEvent::OperatorBodyRetry(retry),
        })
        .expect("the engine decides a record");
    assert!(
        matches!(stale.result, ApplyResult::Stale(receipt) if receipt.current_version == current),
        "{:?}",
        stale.result
    );
    assert!(stale.installed.is_none());
    assert_eq!(port.snapshot().state_version, current);

    let snapshot = port.snapshot();
    let other = crate::full_state::verified_request(
        &crate::full_state::VerifiedChain::new(fixture.headers().genesis_block()),
        &crate::full_state::VerifiedChain::new(fixture.headers().genesis_block()).with(
            block.block.clone(),
            block.height,
            1,
        ),
        block.frontier(),
        &snapshot,
    )
    .expect("a commit's request builds");
    assert!(port.record_body_evidence(other).is_err());
}

/// ST4.3: closing the view watch closes every subscription, and a later
/// subscription starts closed.
#[test]
fn closing_the_views_closes_their_watch() {
    let fixture = BlockFixture::regtest();
    let port = EnginePort::new(fixture.headers());
    let subscribed = port.subscribe_views();
    assert_eq!(*subscribed.borrow(), Some(port.view()));
    port.close_views();
    assert!(subscribed.has_changed().is_err());
    let late = port.subscribe_views();
    assert!(late.has_changed().is_err());
    assert_eq!(*late.borrow(), None);
}

/// ST4.3: a controlled header port over a shared engine port sees the
/// block side's commits, as header sync and block sync share zakura-state.
#[test]
fn a_controlled_header_port_shares_the_engine() {
    let fixture = BlockFixture::regtest();
    let port = Arc::new(EnginePort::new(fixture.headers()));
    let controlled = ControlledPort::over(Arc::clone(&port));
    assert!(Arc::ptr_eq(&controlled.shared_engine(), &port));
    let block = fixture.block(fixture.anchor(), 0, BodySpec::exact(2_000));
    insert(&port, fixture.anchor(), std::slice::from_ref(&block));
    commit(&port, &block, 1);
    assert_eq!(
        controlled.engine().snapshot().frontiers.verified_best,
        block.frontier()
    );
}
