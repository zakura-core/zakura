//! Deterministic-ish local fuzzer / scenario simulator for the **real** Zakura
//! block-sync reactor.
//!
//! Phase 1 (this module): a real-time, scenario-scripted harness that drives the
//! real `spawn_block_sync_reactor` through synthetic peers (`SyntheticBlockSyncPeers`,
//! the same `service::add_peer` → real `PeerRoutine` seam production uses) and a
//! mock commit pipeline (`MockApplyFrontier`), emitting the standard JSONL traces so
//! the existing analysis scripts work unchanged. Nothing here reimplements reactor
//! logic — the node side is the real WorkQueue / ByteBudget / per-peer routine /
//! Sequencer path.
//!
//! Phase 2 (later) threads a `Clock` for bit-exact replay; the harness is written to
//! take a tokio flavor + clock so that is a config flip, not a rewrite.

use std::{sync::Arc, time::Duration};

use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use zakura_chain::block;

use super::mock_blocksync::{
    mainnet_genesis_hash, MockApplyFrontier, SyntheticBlockCorpus, SyntheticBlockShape,
};
use super::{SyntheticBlockSyncPeers, TraceCapture};
use crate::zakura::{
    BlockApplyResult, BlockSyncAction, BlockSyncEvent, BlockSyncFrontiers, BlockSyncHandle,
    ZakuraTrace,
};
use crate::BoxError;

mod invariants;
mod peer;
mod scenario;
#[cfg(test)]
mod tests;

pub(crate) use invariants::{
    assert_core as assert_core_invariants, report as invariant_report, InvariantReport,
};

pub(crate) use scenario::*;

/// Run one scenario against the real reactor with the given trace sink, returning the
/// outcome. Drives to the corpus target or the scenario deadline.
pub(crate) async fn run_scenario(
    scenario: &Scenario,
    trace: ZakuraTrace,
) -> Result<FuzzOutcome, BoxError> {
    let corpus = SyntheticBlockCorpus::generate(
        scenario.blocks,
        scenario.seed,
        SyntheticBlockShape {
            target_block_bytes: scenario.target_block_bytes,
        },
    );
    let target = corpus.target_height();
    let genesis_hash = mainnet_genesis_hash();

    let initial_header = scenario.initial_best_header.min(target);
    let initial_header_hash = corpus_hash(&corpus, initial_header);

    // One shared mock commit frontier: the commit driver advances it as bodies apply;
    // the timeline driver rolls it back on a verified reset so the node's reorg and
    // the committer stay consistent.
    let apply = MockApplyFrontier::new(corpus.clone());
    let initial = fuzz_snapshot(
        1,
        1,
        1,
        zakura_header_chain::Frontier::new(block::Height(0), genesis_hash),
        zakura_header_chain::Frontier::new(block::Height(0), genesis_hash),
        zakura_header_chain::Frontier::new(initial_header, initial_header_hash),
    );
    let (snapshots, committed_snapshots) = watch::channel(Some(initial));

    let shutdown = CancellationToken::new();
    let mut startup = crate::zakura::BlockSyncStartup::new_with_committed_snapshots(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: genesis_hash,
        },
        (initial_header, initial_header_hash),
        committed_snapshots,
        scenario.config.clone(),
    );
    startup.trace = trace.clone();
    startup.shutdown = shutdown.clone();

    let (handle, actions, reactor_task) = crate::zakura::spawn_block_sync_reactor(startup);

    let (committed_tx, mut committed_rx) = watch::channel(block::Height(0));

    let mut tasks = Vec::new();
    tasks.push(spawn_action_driver(
        handle.clone(),
        actions,
        corpus.clone(),
        target,
        apply.clone(),
        scenario.commit,
        committed_tx,
        shutdown.clone(),
    ));
    if !scenario.timeline.is_empty() {
        tasks.push(spawn_timeline_driver(
            snapshots.clone(),
            corpus.clone(),
            apply.clone(),
            scenario.timeline.clone(),
            shutdown.clone(),
        ));
    }

    // Attach synthetic peers through the real add_peer path; each owns its own
    // connect/serve/disconnect lifecycle (peer churn).
    let peers = Arc::new(SyntheticBlockSyncPeers::new(
        scenario.config.clone(),
        handle.clone(),
        scenario.transport_queue_depth.unwrap_or(1024),
    ));
    for spec in &scenario.peers {
        tasks.push(peer::spawn_peer_lifecycle(
            peers.clone(),
            corpus.clone(),
            *spec,
            scenario.seed,
            corpus_hash(&corpus, spec.servable_high),
            shutdown.clone(),
        ));
    }

    let running = RunningHarness {
        shutdown: shutdown.clone(),
        reactor_task,
        tasks,
        _peers: peers,
    };

    // Wait for the committed tip to reach the target, or the deadline. Map the watch
    // `Ref` to an owned (Copy) height so its borrow of `committed_rx` is released
    // before the fallback re-borrows it.
    let reached = tokio::time::timeout(
        scenario.deadline,
        committed_rx.wait_for(|height| *height >= target),
    )
    .await
    .ok()
    .and_then(|result| result.ok())
    .map(|height| *height);
    let committed_tip = reached.unwrap_or_else(|| *committed_rx.borrow());
    running.stop().await;

    Ok(FuzzOutcome {
        committed_tip,
        target,
    })
}

/// Owns the running reactor + driver tasks for one scenario and tears them down on
/// drop. Holds `SyntheticBlockSyncPeers` (the `BlockSyncService`) alive so the
/// per-peer routines keep running.
struct RunningHarness {
    shutdown: CancellationToken,
    reactor_task: JoinHandle<()>,
    tasks: Vec<JoinHandle<()>>,
    _peers: Arc<SyntheticBlockSyncPeers>,
}

impl Drop for RunningHarness {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.reactor_task.abort();
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl RunningHarness {
    async fn stop(mut self) {
        self.shutdown.cancel();
        stop_task(&mut self.reactor_task).await;
        for task in &mut self.tasks {
            stop_task(task).await;
        }
    }
}

async fn stop_task(task: &mut JoinHandle<()>) {
    if tokio::time::timeout(Duration::from_secs(2), &mut *task)
        .await
        .is_err()
    {
        task.abort();
        let _ = task.await;
    }
}

/// Answers the reactor's actions from the corpus and mock apply frontier.
// A fuzz-harness driver that wires up many independent channels/knobs; grouping
// them into a struct would not make the test setup clearer.
#[allow(clippy::too_many_arguments)]
fn spawn_action_driver(
    handle: BlockSyncHandle,
    mut actions: mpsc::Receiver<BlockSyncAction>,
    corpus: SyntheticBlockCorpus,
    target: block::Height,
    apply: MockApplyFrontier,
    commit: CommitProfile,
    committed_tx: watch::Sender<block::Height>,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut applied = 0u64;
        loop {
            let action = tokio::select! {
                _ = shutdown.cancelled() => break,
                action = actions.recv() => match action {
                    Some(action) => action,
                    None => break,
                },
            };
            match action {
                BlockSyncAction::QueryNeededBlocks {
                    query_id,
                    from,
                    limit,
                    best_header_tip,
                    scope,
                } => {
                    let start = from;
                    let metas = if limit == 0 {
                        Vec::new()
                    } else {
                        let end = (start + i64::from(limit.saturating_sub(1)))
                            .unwrap_or(block::Height::MAX)
                            .min(best_header_tip)
                            .min(target);
                        if start <= end {
                            corpus.metas_between(start, end)
                        } else {
                            Vec::new()
                        }
                    };
                    if handle
                        .send(BlockSyncEvent::ScopedNeededBlocks {
                            query_id,
                            scope,
                            blocks: metas,
                        })
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                BlockSyncAction::QueryBlocksByHeightRange { peer, start, count } => {
                    let blocks = corpus.blocks_in_range(start, count, target);
                    if handle
                        .send(BlockSyncEvent::BlockRangeResponseReady {
                            peer,
                            start_height: start,
                            requested_count: count,
                            blocks,
                        })
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                BlockSyncAction::SubmitBlock {
                    owner,
                    source,
                    token,
                    block,
                } => {
                    // Model a slow/bursty commit drain: hold the submitted body before
                    // applying so its reserved bytes stay held until
                    // `BlockApplyFinished`, letting the apply backlog build against the
                    // byte budget.
                    if !commit.per_commit_delay.is_zero()
                        && sleep_or_cancel(&shutdown, commit.per_commit_delay).await
                    {
                        break;
                    }
                    let height = block
                        .coinbase_height()
                        .expect("synthetic submitted block has height");
                    let outcome = apply.apply(block.as_ref());
                    if outcome.result == BlockApplyResult::Committed {
                        let _ = committed_tx.send(outcome.frontiers.verified_block_tip);
                    }
                    if handle
                        .send(BlockSyncEvent::BlockApplyFinished {
                            owner,
                            source,
                            token,
                            height,
                            hash: block.hash(),
                            outcome: crate::zakura::block_sync::test_block_apply_outcome(
                                outcome.result,
                            ),
                            local_frontier: Some(outcome.frontiers),
                        })
                        .await
                        .is_err()
                    {
                        break;
                    }
                    applied = applied.saturating_add(1);
                    if let Some(burst) = commit.burst {
                        if burst.every_commits > 0
                            && applied.is_multiple_of(burst.every_commits)
                            && !burst.duration.is_zero()
                            && sleep_or_cancel(&shutdown, burst.duration).await
                        {
                            break;
                        }
                    }
                }
                BlockSyncAction::RecordBodyUnavailable { .. }
                | BlockSyncAction::RestartBodyAvailability { .. }
                | BlockSyncAction::RetryBodyAvailability { .. } => {}
                BlockSyncAction::Misbehavior { .. } => {}
            }
        }
    })
}

/// Publishes the scenario's timed committed snapshots, driving the node's download target.
fn spawn_timeline_driver(
    snapshots: watch::Sender<Option<zakura_header_chain::EngineSnapshot>>,
    corpus: SyntheticBlockCorpus,
    apply: MockApplyFrontier,
    mut timeline: Vec<TipEvent>,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        timeline.sort_by_key(|event| event.at);
        let mut elapsed = Duration::ZERO;
        for event in timeline {
            let wait = event.at.saturating_sub(elapsed);
            if sleep_or_cancel(&shutdown, wait).await {
                return;
            }
            elapsed = event.at;
            // Roll the mock committer back first so re-downloaded blocks above the
            // reset re-commit cleanly once the node resets.
            let apply_frontiers = if let TipEventKind::VerifiedReset(height) = event.kind {
                apply.reset_to(height)
            } else {
                apply.frontiers()
            };
            let mut current = snapshots
                .borrow()
                .clone()
                .expect("the fuzz harness starts after semantic handoff");
            current.state_version =
                zakura_header_chain::StateVersion::new(current.state_version.get() + 1);
            current.frontiers.finalized = zakura_header_chain::Frontier::new(
                apply_frontiers.finalized_height,
                apply_frontiers.verified_block_hash,
            );
            current.frontiers.verified_best = zakura_header_chain::Frontier::new(
                apply_frontiers.verified_block_tip,
                apply_frontiers.verified_block_hash,
            );
            apply_tip_event(&corpus, &mut current, event.kind);
            snapshots
                .send(Some(current))
                .expect("the fuzz reactor keeps its committed-snapshot receiver");
        }
    })
}

fn apply_tip_event(
    corpus: &SyntheticBlockCorpus,
    snapshot: &mut zakura_header_chain::EngineSnapshot,
    kind: TipEventKind,
) {
    match kind {
        TipEventKind::GrowTo(height) => {
            snapshot.header_generation =
                zakura_header_chain::HeaderGeneration::new(snapshot.header_generation.get() + 1);
            snapshot.frontiers.header_best =
                zakura_header_chain::Frontier::new(height, corpus_hash(corpus, height));
        }
        TipEventKind::HeaderReanchor(height) => {
            snapshot.header_generation =
                zakura_header_chain::HeaderGeneration::new(snapshot.header_generation.get() + 1);
            snapshot.frontiers.header_best =
                zakura_header_chain::Frontier::new(height, corpus_hash(corpus, height));
        }
        TipEventKind::VerifiedReset(height) => {
            snapshot.verified_generation = zakura_header_chain::VerifiedGeneration::new(
                snapshot.verified_generation.get() + 1,
            );
            snapshot.frontiers.verified_best =
                zakura_header_chain::Frontier::new(height, corpus_hash(corpus, height));
        }
    }
    snapshot.header_best_score = zakura_header_chain::ChainScore::new(
        zakura_header_chain::SuffixWork::zero(),
        snapshot.frontiers.header_best.hash,
    );
}

fn fuzz_snapshot(
    state_version: u64,
    header_generation: u64,
    verified_generation: u64,
    finalized: zakura_header_chain::Frontier,
    verified_best: zakura_header_chain::Frontier,
    header_best: zakura_header_chain::Frontier,
) -> zakura_header_chain::EngineSnapshot {
    zakura_header_chain::EngineSnapshot {
        mode: zakura_header_chain::EngineMode::Integrated,
        state_version: zakura_header_chain::StateVersion::new(state_version),
        header_generation: zakura_header_chain::HeaderGeneration::new(header_generation),
        verified_generation: zakura_header_chain::VerifiedGeneration::new(verified_generation),
        frontiers: zakura_header_chain::FrontierSet {
            finalized,
            header_best,
            verified_best,
        },
        header_best_score: zakura_header_chain::ChainScore::new(
            zakura_header_chain::SuffixWork::zero(),
            header_best.hash,
        ),
        oldest_retained_height: finalized.height,
        alarms: zakura_header_chain::AlarmSet::default(),
    }
}

fn corpus_hash(corpus: &SyntheticBlockCorpus, height: block::Height) -> block::Hash {
    if height == block::Height(0) {
        mainnet_genesis_hash()
    } else {
        corpus
            .block_at(height)
            .map(|block| block.hash())
            .unwrap_or_else(mainnet_genesis_hash)
    }
}

/// Sleep `duration`, returning `true` if `shutdown` fired first.
pub(crate) async fn sleep_or_cancel(shutdown: &CancellationToken, duration: Duration) -> bool {
    if duration.is_zero() {
        return shutdown.is_cancelled();
    }
    tokio::select! {
        _ = shutdown.cancelled() => true,
        _ = tokio::time::sleep(duration) => false,
    }
}

/// Build a `ZakuraTrace` writing into a per-run dir under `target/zakura-traces/`,
/// returning the capture guard (flush + persist) alongside it.
pub(crate) fn run_trace(name: &str) -> std::io::Result<(TraceCapture, ZakuraTrace)> {
    let mut capture = TraceCapture::for_test(name)?;
    let trace = ZakuraTrace::new(capture.tracer_for_node(0), "00");
    Ok((capture, trace))
}
