//! Controlled block storage on the step loop (storage.md ST4.7). The `Plan`
//! picks when and how each block operation completes, between the tasks
//! that submitted them.
#![cfg(zakura_sim)]

mod support;

use std::{
    future::Future,
    num::NonZeroU64,
    pin::pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use sha2::{Digest, Sha256};
use support::{config, inputs, picks, run};
use zakura_chain::block;
use zakura_header_chain::{
    BodyRuleId, BodyWorkAuthority, ConsensusBodyInvalid, EvidenceId, SourceId, StateVersion,
};
use zakura_network::zakura::{
    BlockRangeRead, BlockRangeReadLease, BlockRangeSource, BlockSyncAction,
};
use zakura_node_services::header_chain::{ApplyHeaderTargetOutcome, Port};
use zakura_rt::sync::mpsc;
use zakura_scheduler::{SchedulerConfig, SimInstant};
use zakura_sim::{
    observe::{StorageFact, StorageResult, StorageState},
    storage::{BlockStorage, HeaderStorage},
    ActionSource, Boundary, HarnessFailure, Identity, Observation, ObservationLog, Outcome,
    PlanInput, RunConfig, RunReport,
};
use zakura_storage::{
    block::{
        controlled::{ControlledBlockService, FactKind, ReplyLog},
        fixture::{BlockFixture, BodySpec, FixtureBlock},
        ApplyLimits,
    },
    header::{adapter::EnginePort, controlled::ControlledPort, fixture::HeaderFixture},
};

/// A run config with 2 workers and `budget` steps, and the default stack,
/// since the engine runs on task stacks.
fn storage_config(budget: u64) -> RunConfig {
    let mut config = config(2, budget);
    config.scheduler.stack_size = SchedulerConfig::default().stack_size;
    config
}

/// Polls `future` once and returns its output.
fn ready<F: Future>(future: F) -> F::Output {
    match pin!(future).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("the engine port completes each operation on its first poll"),
    }
}

/// Apply limits that admit two full bodies at once.
const LIMITS: ApplyLimits = ApplyLimits {
    checkpoint: 2,
    full: 2,
    combined: 3,
    max_checkpoint_height: block::Height(0),
};

/// A block service over an engine that holds a 3-block chain's headers.
struct Blocks {
    engine: Arc<EnginePort>,
    service: Arc<ControlledBlockService>,
    chain: Vec<FixtureBlock>,
}

impl Blocks {
    fn new() -> Self {
        let fixture = BlockFixture::regtest();
        let engine = Arc::new(EnginePort::new(fixture.headers()));
        let chain = fixture.extend(fixture.anchor(), &[BodySpec::exact(900); 3], 0);
        let request = fixture.target_request(&engine.snapshot(), fixture.anchor(), &chain, 1);
        let prepared =
            ready(engine.prepare_header_target(request)).expect("fixture headers prepare");
        assert_eq!(
            ready(engine.apply_header_target(prepared)).expect("fixture headers apply"),
            ApplyHeaderTargetOutcome::Applied
        );
        let service = Arc::new(ControlledBlockService::new(
            engine.clone(),
            Arc::new(ReplyLog::default()),
            LIMITS,
        ));
        Blocks {
            engine,
            service,
            chain,
        }
    }

    /// A digest of the engine's committed state and full state's tips.
    fn state_digest(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(self.engine.state_digest());
        for tip in self.engine.with_full_state(|chain| chain.tips()) {
            hasher.update(tip.0);
        }
        hasher.finalize().into()
    }
}

/// Spawns block sync's stand-in, which sends a query, every body, a
/// record and another query through the intake task, and a server that
/// reads the committed range until a read serves a body, at most eight
/// times.
fn spawn_sync(sim: &mut zakura_sim::Sim, blocks: &Blocks) {
    let service = blocks.service.clone();
    let chain: Vec<_> = blocks
        .chain
        .iter()
        .map(|block| block.block.clone())
        .collect();
    let engine = blocks.engine.clone();
    sim.scheduler().spawn_root("sync", async move {
        let (sender, actions) = mpsc::channel(8);
        let _intake = service.spawn(actions);
        let scope = BodyWorkAuthority::for_view(&engine.view());
        let query = |id: u64| BlockSyncAction::QueryNeededBlocks {
            query_id: NonZeroU64::new(id).expect("query IDs count from 1"),
            from: block::Height(1),
            limit: 16,
            best_header_tip: block::Height(3),
            scope,
        };
        let mut sent = vec![query(1)];
        for (token, block) in (1..).zip(chain.iter()) {
            sent.push(BlockSyncAction::SubmitBlock {
                owner: scope.bind(1, NonZeroU64::MIN),
                source: SourceId::from_digest([7; 32]),
                token,
                block: block.clone(),
            });
        }
        sent.push(BlockSyncAction::RecordBodyInvalid {
            expected_version: StateVersion::new(1),
            invalid: ConsensusBodyInvalid {
                hash: chain[2].hash(),
                evidence: EvidenceId::from_digest(chain[2].hash().0),
                rule: BodyRuleId::new("block.subsidy"),
                source: SourceId::from_digest([7; 32]),
            },
        });
        sent.push(query(2));
        for action in sent {
            if sender.send(action).await.is_err() {
                return;
            }
        }
    });

    let source = blocks.service.range_source();
    sim.scheduler().spawn_root("server", async move {
        for _ in 0..8 {
            let (lease, _session) = BlockRangeReadLease::detached();
            let read = source
                .read(BlockRangeRead {
                    start_height: block::Height(1),
                    count: 3,
                    max_body_bytes: 10_000,
                    lease,
                })
                .await;
            if read.is_ok_and(|read| !read.blocks.is_empty()) {
                return;
            }
        }
    });
}

/// Runs the scenario under `plan`, and returns the report and the blocks.
fn block_run(plan: PlanInput, budget: u64) -> (RunReport, Blocks) {
    wrapped_block_run(plan, budget, |storage| Box::new(storage))
}

/// [`block_run`] with the block storage source wrapped by `wrap`.
fn wrapped_block_run(
    plan: PlanInput,
    budget: u64,
    wrap: impl FnOnce(BlockStorage) -> Box<dyn ActionSource>,
) -> (RunReport, Blocks) {
    let blocks = Blocks::new();
    let report = run(
        storage_config(budget),
        plan,
        |sim| {
            spawn_sync(sim, &blocks);
            let storage = BlockStorage::new(sim, blocks.service.clone());
            sim.add_source(wrap(storage));
        },
        &mut zakura_sim::NoChecks,
    );
    (report, blocks)
}

/// A digest of a run's block storage observations, with their steps, and
/// of the engine's final state.
fn history(plan: PlanInput) -> String {
    let (report, blocks) = block_run(plan, 2_000);
    let mut hasher = Sha256::new();
    for entry in report.log.entries() {
        if let Observation::Storage { .. } | Observation::StorageState { .. } = &entry.obs {
            hasher.update(entry.step.to_le_bytes());
            hasher.update(serde_json::to_vec(&entry.obs).unwrap());
        }
    }
    hasher.update(blocks.state_digest());
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// ST4.7: the run picks block outcomes as `storage:block:<id>:<outcome>`
/// actions, some plans commit past the first body, and some serve a
/// non-empty range.
#[test]
fn the_plan_completes_block_operations() {
    let mut committed_past_first = false;
    let mut served = false;
    for plan in inputs(16, 256) {
        let (report, blocks) = block_run(plan, 2_000);
        assert!(
            !matches!(report.end.outcome, Outcome::Harness),
            "{:?}",
            report.end
        );
        for pick in picks(&report) {
            if let Some(rest) = pick.strip_prefix("storage:") {
                assert!(rest.starts_with("block:"), "{pick}");
            }
        }
        served |= report.log.entries().iter().any(|entry| {
            matches!(
                &entry.obs,
                Observation::Storage {
                    fact: StorageFact::BlockCompleted {
                        result: StorageResult::Served { heights, .. },
                        ..
                    },
                    ..
                } if !heights.is_empty()
            )
        });
        committed_past_first |= blocks.engine.snapshot().frontiers.verified_best
            == blocks.chain[1].frontier()
            || blocks.engine.snapshot().frontiers.verified_best == blocks.chain[2].frontier();
    }
    assert!(
        committed_past_first,
        "some plan commits past the first body"
    );
    assert!(served, "some plan serves a non-empty range");
}

/// ST4.7: a plan's block storage history and final state repeat exactly,
/// and an unrelated run in between changes nothing.
#[test]
fn block_storage_histories_repeat_a_a_and_a_b_a() {
    let plans = inputs(2, 256);
    let a = history(plans[0].clone());
    assert_eq!(history(plans[0].clone()), a);
    let b = history(plans[1].clone());
    assert_ne!(b, a, "the two plans should make different histories");
    assert_eq!(history(plans[0].clone()), a);
}

/// ST4.7: a run that ends with block operations held releases them when
/// the source drops at shutdown, and nothing from shutdown reaches the
/// frozen log.
#[test]
fn block_shutdown_releases_everything_and_logs_nothing() {
    let (report, blocks) = block_run(PlanInput::Bytes(Vec::new()), 12);
    assert_eq!(report.end.outcome, Outcome::Budget);
    assert!(report.log.entries().iter().any(|entry| matches!(
        &entry.obs,
        Observation::Storage {
            fact: StorageFact::BlockSubmitted { .. },
            ..
        }
    )));
    assert!(!report.log.entries().iter().any(|entry| matches!(
        &entry.obs,
        Observation::Storage {
            fact: StorageFact::ShutDown { .. },
            ..
        }
    )));
    assert!(blocks.service.owner_state().operations.is_empty());
    let kinds: Vec<_> = blocks
        .service
        .take_facts()
        .into_iter()
        .map(|fact| fact.kind)
        .collect();
    assert!(
        matches!(kinds.last(), Some(FactKind::ShutDown { dropped, .. }) if !dropped.is_empty()),
        "shutdown dropped held operations: {kinds:?}"
    );
}

/// ST4.7: each storage service numbers its own instances, so block
/// storage registered after header storage still names its actions
/// `storage:block:<id>:<outcome>`.
#[test]
fn block_actions_keep_their_service_name_beside_header_storage() {
    let mut picked_block = false;
    for plan in inputs(8, 256) {
        let blocks = Blocks::new();
        let report = run(
            storage_config(400),
            plan,
            |sim| {
                let port = Arc::new(ControlledPort::new(&HeaderFixture::regtest()));
                let caller = port.clone();
                sim.scheduler().spawn_root("caller", async move {
                    let anchor = HeaderFixture::regtest().anchor();
                    let _ = caller.selected_chain_contains(anchor.hash).await;
                });
                let header = HeaderStorage::new(sim, port);
                sim.add_source(Box::new(header));
                spawn_sync(sim, &blocks);
                let storage = BlockStorage::new(sim, blocks.service.clone());
                sim.add_source(Box::new(storage));
            },
            &mut zakura_sim::NoChecks,
        );
        assert!(
            !matches!(report.end.outcome, Outcome::Harness),
            "{:?}",
            report.end
        );
        for pick in picks(&report) {
            if let Some(rest) = pick.strip_prefix("storage:") {
                assert!(
                    rest.starts_with("block:") || rest.starts_with("header:"),
                    "{pick}"
                );
                picked_block |= rest.starts_with("block:");
            }
        }
    }
    assert!(picked_block, "some plan completes a block operation");
}

/// A storage source that forwards everything to `inner`, but at step `at`
/// observes into a copy of the log and logs what it delivered there after
/// `edit` changes it, as a wrapper that normalizes or redacts might.
struct Rewrite<S> {
    inner: S,
    at: u64,
    edit: fn(&mut [Observation]),
}

impl<S: ActionSource> ActionSource for Rewrite<S> {
    fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        self.inner.enabled(now, out);
    }

    fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>) {
        self.inner.boundaries(now, out);
    }

    fn execute(&mut self, local: u64, log: &mut ObservationLog) {
        self.inner.execute(local, log);
    }

    fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
        if log.step() != self.at {
            return self.inner.observe(log);
        }
        let mut copy = log.clone();
        let cursor = copy.entries().len();
        self.inner.observe(&mut copy)?;
        let mut delivered: Vec<Observation> = copy.entries()[cursor..]
            .iter()
            .map(|entry| entry.obs.clone())
            .collect();
        (self.edit)(&mut delivered);
        for obs in delivered {
            log.push(obs);
        }
        Ok(())
    }
}

/// Leaves the delivery as it was.
fn unchanged(_: &mut [Observation]) {}

/// Changes the first fact's payload, keeping its sequence number.
fn alter_fact(delivered: &mut [Observation]) {
    if let Some(fact) = delivered.iter_mut().find_map(|obs| match obs {
        Observation::Storage { fact, .. } => Some(fact),
        _ => None,
    }) {
        *fact = StorageFact::CallerDropped { id: 999 };
    }
}

/// Changes the state's first operation, keeping its watermark.
fn alter_state(delivered: &mut [Observation]) {
    if let Some(operation) = delivered.iter_mut().find_map(|obs| match obs {
        Observation::StorageState {
            state: StorageState::Block(state),
            ..
        } => state.operations.first_mut(),
        _ => None,
    }) {
        operation.id += 1_000;
    }
}

/// Moves the state ahead of the fact before it.
fn swap_fact_and_state(delivered: &mut [Observation]) {
    let state = delivered
        .iter()
        .position(|obs| matches!(obs, Observation::StorageState { .. }));
    if let Some(state) = state.filter(|state| *state > 0) {
        delivered.swap(state - 1, state);
    }
}

/// The run log must hold exactly what block storage delivered, so a
/// wrapper that relogs a delivery with a fact's payload or the state
/// changed, keeping their numbers, or with a fact and the state swapped,
/// fails the run with `Undelivered`. Relogging it unchanged passes.
#[test]
fn block_storage_entries_must_reach_the_log_unchanged() {
    let plan = inputs(1, 256).remove(0);
    let (whole, _) = block_run(plan.clone(), 2_000);
    // A step after setup whose delivery holds a fact, then a state with
    // an operation, so each edit changes it.
    let at = whole
        .log
        .entries()
        .windows(2)
        .find(|pair| {
            pair[0].step > 0
                && pair[0].step == pair[1].step
                && matches!(pair[0].obs, Observation::Storage { .. })
                && matches!(
                    &pair[1].obs,
                    Observation::StorageState {
                        state: StorageState::Block(state),
                        ..
                    } if !state.operations.is_empty()
                )
        })
        .expect("block storage delivers a fact and a state after setup")[0]
        .step;
    let relogged = |edit| {
        wrapped_block_run(plan.clone(), 2_000, move |inner| {
            Box::new(Rewrite { inner, at, edit })
        })
        .0
    };
    assert_eq!(relogged(unchanged).end, whole.end);
    for (name, edit) in [
        ("fact", alter_fact as fn(&mut [Observation])),
        ("state", alter_state),
        ("order", swap_fact_and_state),
    ] {
        let report = relogged(edit);
        assert_eq!(report.end.outcome, Outcome::Harness, "{name}");
        assert_eq!(report.end.steps, at, "{name}");
        let Some(HarnessFailure::SourceEvidence { detail, .. }) = &report.end.harness else {
            panic!("expected a source evidence failure, got {:?}", report.end);
        };
        assert!(detail.starts_with("Undelivered"), "{name}: {detail}");
    }
}
