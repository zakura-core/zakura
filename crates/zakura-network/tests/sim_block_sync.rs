//! The block-sync reactor and sequencer on the deterministic scheduler. These
//! only build with `RUSTFLAGS="--cfg zakura_sim"`, in a separate
//! `CARGO_TARGET_DIR`.
//!
//! Block sync runs with no peers and the default empty block source, fed one
//! committed header-chain view. A small seeded `Plan` stands in for
//! `zakura-sim`: it picks which ready task runs and answers every random
//! draw.
#![cfg(zakura_sim)]

use std::{
    cell::RefCell,
    collections::BTreeMap,
    rc::Rc,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use zakura_chain::{block, parameters::Network};
use zakura_header_chain::{
    AlarmSet, BodyUnavailableSummary, BodyWorkEpoch, ChainScore, CommittedHeaderChainView,
    EngineMode, EngineSnapshot, Frontier, FrontierSet, HeaderGeneration, StateVersion, SuffixWork,
    VerifiedGeneration,
};
use zakura_network::zakura::{
    spawn_block_sync_reactor, BlockSyncFrontiers, BlockSyncHandle, BlockSyncStartup,
    ZakuraBlockSyncConfig,
};
use zakura_rt::sync::{watch, CancellationToken};
use zakura_scheduler::{RunOutcome, Scheduler, SchedulerConfig, SimInstant, Status};

/// A run that takes more steps than this is stuck.
const MAX_STEPS: usize = 100_000;

/// Block sync's `NEEDED_BLOCK_QUERY_RETRY_DELAY`, which is private.
const NEEDED_BLOCK_QUERY_RETRY_DELAY: Duration = Duration::from_millis(250);

/// The gauge that reports how long the body-unavailable alarm has been up.
const ALARM_AGE_GAUGE: &str = "sync.header_chain.body_unavailable.age_seconds";

/// A seeded stream of choices: which ready task runs, and every draw's
/// answer.
struct Plan(u64);

impl Plan {
    /// The next 64 bits (SplitMix64).
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A choice below `bound`, or any `u64` when `bound` is 0.
    fn below(&mut self, bound: u64) -> u64 {
        match bound {
            0 => self.next(),
            bound => self.next() % bound,
        }
    }
}

/// Everything a run lets a test see, in order.
#[derive(Debug, PartialEq, Eq)]
struct Observations {
    /// Each step as `task:kind#n`, `advance-time`, `shutdown`, or a panic.
    steps: Vec<String>,
    /// Each draw as `kind/bound`.
    draws: Vec<String>,
    /// The earliest timer deadline, from virtual time zero, each time the
    /// run settled waiting on time.
    deadlines: Vec<Duration>,
    /// Where the run stood the first time it settled.
    settled: Status,
    /// How the reactor's task ended.
    exit: String,
}

/// What block sync starts from.
struct Scenario {
    /// The committed header-chain view block sync starts from.
    view: CommittedHeaderChainView,
    /// Whether a task drains block sync's actions. Without one, the action
    /// receiver is dropped, so every action fails to dispatch.
    drain_actions: bool,
    /// UTC at virtual time zero.
    utc_origin: Option<SystemTime>,
}

impl Scenario {
    /// Regtest at genesis, with every action drained.
    fn genesis() -> Self {
        let genesis = genesis();
        Scenario {
            view: committed_view(genesis, genesis, AlarmSet::default()),
            drain_actions: true,
            utc_origin: None,
        }
    }
}

/// The Regtest genesis block as a frontier.
fn genesis() -> Frontier {
    Frontier::new(
        block::Height(0),
        Network::new_regtest(Default::default()).genesis_hash(),
    )
}

/// A committed view with `verified` as the finalized and verified tip,
/// `header` as the best header, and `alarms`.
fn committed_view(
    verified: Frontier,
    header: Frontier,
    alarms: AlarmSet,
) -> CommittedHeaderChainView {
    let snapshot = EngineSnapshot {
        mode: EngineMode::Integrated,
        state_version: StateVersion::new(1),
        header_generation: HeaderGeneration::new(1),
        verified_generation: VerifiedGeneration::new(1),
        frontiers: FrontierSet {
            finalized: verified,
            header_best: header,
            verified_best: verified,
        },
        header_best_score: ChainScore::new(SuffixWork::zero(), header.hash),
        oldest_retained_height: verified.height,
        alarms,
    };
    CommittedHeaderChainView::new(snapshot, BodyWorkEpoch::default())
}

/// Block-sync startup from `view`, with state queries on and the default
/// block source.
fn startup(
    view: &CommittedHeaderChainView,
    views: watch::Receiver<Option<CommittedHeaderChainView>>,
    shutdown: CancellationToken,
) -> BlockSyncStartup {
    let frontiers = &view.frontiers;
    let mut startup = BlockSyncStartup::new_with_committed_views(
        BlockSyncFrontiers {
            finalized_height: frontiers.finalized.height,
            verified_block_tip: frontiers.verified_best.height,
            verified_block_hash: frontiers.verified_best.hash,
        },
        (frontiers.header_best.height, frontiers.header_best.hash),
        views,
        ZakuraBlockSyncConfig::default(),
    );
    assert!(startup.state_queries_enabled, "the constructor's default");
    startup.shutdown = shutdown;
    startup
}

/// One block-sync run on the scheduler, from start through shutdown.
struct Run {
    sched: Scheduler,
    plan: Rc<RefCell<Plan>>,
    draws: Rc<RefCell<Vec<String>>>,
    steps: Vec<String>,
    deadlines: Vec<Duration>,
    shutdown: CancellationToken,
    /// Block sync's handle and the committed-view sender, until shutdown
    /// drops them. The handle owns the routine wiring, which keeps the
    /// sequencer's body input open.
    inputs: Option<(
        BlockSyncHandle,
        watch::Sender<Option<CommittedHeaderChainView>>,
    )>,
    exit: Rc<RefCell<Option<String>>>,
}

impl Run {
    /// Starts block sync from `scenario`, with `seed` answering every choice.
    /// Nothing runs yet.
    fn start(scenario: Scenario, seed: u64) -> Self {
        let plan = Rc::new(RefCell::new(Plan(seed)));
        let draws = Rc::new(RefCell::new(Vec::new()));
        let config = SchedulerConfig {
            utc_origin: scenario.utc_origin,
            ..SchedulerConfig::default()
        };
        let mut sched = Scheduler::new(config, SimInstant::ZERO);
        {
            let (plan, draws) = (Rc::clone(&plan), Rc::clone(&draws));
            sched.set_draws(move |draw| {
                draws
                    .borrow_mut()
                    .push(format!("{:?}/{}", draw.kind, draw.bound));
                plan.borrow_mut().below(draw.bound)
            });
        }

        let shutdown = CancellationToken::new();
        let (views, views_rx) = watch::channel(Some(scenario.view.clone()));
        let (handle, mut actions, reactor) =
            spawn_block_sync_reactor(startup(&scenario.view, views_rx, shutdown.clone()))
                .expect("block sync starts");
        if scenario.drain_actions {
            sched.spawn_root(
                "actions",
                async move { while actions.recv().await.is_some() {} },
            );
        } else {
            drop(actions);
        }
        let exit = Rc::new(RefCell::new(None));
        {
            let exit = Rc::clone(&exit);
            sched.spawn_root("join", async move {
                *exit.borrow_mut() = Some(match reactor.await {
                    Ok(()) => "completed".to_owned(),
                    Err(error) => format!("{error}"),
                });
            });
        }

        Run {
            sched,
            plan,
            draws,
            steps: Vec::new(),
            deadlines: Vec::new(),
            shutdown,
            inputs: Some((handle, views)),
            exit,
        }
    }

    /// Runs ready tasks, picked by the plan, until none is ready or `stop`
    /// holds. Records each step, and the earliest deadline if the run now
    /// waits on time.
    fn settle_until(&mut self, stop: impl Fn(&Self) -> bool) -> Status {
        let status = loop {
            assert!(
                self.steps.len() < MAX_STEPS,
                "the run took {MAX_STEPS} steps"
            );
            let status = self.sched.status();
            if status != Status::Running || stop(self) {
                break status;
            }
            let ready = self.sched.ready();
            let index = self.plan.borrow_mut().below(ready.len() as u64); // a Vec's length fits in u64
            let pick = ready[index as usize]; // below `ready.len()`, a usize
            self.steps
                .push(format!("{}:{}", self.sched.task_name(pick.task), pick.at));
            if let RunOutcome::Panicked(message) = self.sched.run(pick.task) {
                self.steps.push(format!("panicked: {message}"));
            }
        };
        if status == Status::WaitingOnTime {
            let next = self
                .sched
                .next_deadline()
                .expect("WaitingOnTime has a timer");
            self.deadlines.push(next.since_start());
        }
        status
    }

    /// Runs ready tasks until none is ready.
    fn settle(&mut self) -> Status {
        self.settle_until(|_| false)
    }

    /// Moves virtual time to the earliest deadline, then settles.
    fn advance(&mut self) -> Status {
        let next = self
            .sched
            .next_deadline()
            .expect("only a run waiting on time advances");
        self.sched.advance_to(next);
        self.steps.push("advance-time".to_owned());
        self.settle()
    }

    /// Shuts block sync down and runs every task to its end.
    ///
    /// The handle's routine wiring keeps the sequencer's body input open, and
    /// the sequencer only exits once both of its inputs close. So the
    /// reactor has to end before the handle drops, and only then can the
    /// rest finish.
    fn finish(mut self, settled: Status) -> Observations {
        self.steps.push("shutdown".to_owned());
        self.shutdown.cancel();
        let reactor_ended = |run: &Self| run.exit.borrow().is_some();
        while !reactor_ended(&self) {
            match self.settle_until(reactor_ended) {
                Status::WaitingOnTime if !reactor_ended(&self) => {
                    self.advance();
                }
                status => assert!(
                    reactor_ended(&self),
                    "the reactor ends after shutdown, but the run is {status:?}"
                ),
            }
        }
        drop(self.inputs.take());
        let end = loop {
            match self.settle() {
                Status::WaitingOnTime => {
                    self.advance();
                }
                other => break other,
            }
        };
        assert_eq!(end, Status::Quiescent, "every task ends after shutdown");
        let report = self.sched.shutdown();
        assert_eq!(report, Default::default(), "nothing was left to clean up");

        let exit = self
            .exit
            .borrow_mut()
            .take()
            .expect("the join task saw the reactor end");
        Observations {
            steps: self.steps,
            draws: self.draws.take(),
            deadlines: self.deadlines,
            settled,
            exit,
        }
    }
}

/// Starts block sync from `scenario`, settles, moves time forward
/// `advances` times, then shuts down.
fn run(scenario: Scenario, seed: u64, advances: usize) -> Observations {
    let mut run = Run::start(scenario, seed);
    let settled = run.settle();
    for _ in 0..advances {
        assert_eq!(run.advance(), Status::WaitingOnTime);
    }
    run.finish(settled)
}

/// Asserts that no step panicked.
fn assert_no_panic(observed: &Observations) {
    assert!(
        !observed
            .steps
            .iter()
            .any(|step| step.starts_with("panicked")),
        "{:?}",
        observed.steps
    );
}

#[test]
fn block_sync_reactor_starts_waits_on_time_and_shuts_down() {
    let observed = run(Scenario::genesis(), 1, 0);
    assert_eq!(observed.settled, Status::WaitingOnTime);
    assert_eq!(observed.exit, "completed");
    for task in ["block_sync_reactor.1:", "block_sync_sequencer.1:"] {
        assert!(
            observed.steps.iter().any(|step| step.starts_with(task)),
            "{task} ran under its spawn name: {:?}",
            observed.steps
        );
    }
    assert_no_panic(&observed);
}

#[test]
fn the_same_plan_replays_the_same_block_sync_run() {
    let first = run(Scenario::genesis(), 7, 2);
    assert_eq!(
        first,
        run(Scenario::genesis(), 7, 2),
        "a second run with the same plan"
    );
    let other = run(Scenario::genesis(), 8, 2);
    assert_eq!(
        first,
        run(Scenario::genesis(), 7, 2),
        "a run after an unrelated one"
    );
    // Plan 8 orders the steps differently, so the comparisons above would
    // catch a run that ignored its plan.
    assert_ne!(first.steps, other.steps);
}

#[test]
fn block_sync_timers_ignore_the_host_clock() {
    // The header tip is ahead of the verified tip, but by less than the
    // empty-state quiet period's lag, so the reactor asks for the missing
    // bodies at once. With the action receiver dropped, that query fails to
    // dispatch, and the reactor retries it after a delay.
    let behind = || {
        let genesis = genesis();
        Scenario {
            view: committed_view(
                genesis,
                Frontier::new(block::Height(10), block::Hash([0x11; 32])),
                AlarmSet::default(),
            ),
            drain_actions: false,
            utc_origin: None,
        }
    };

    let first = run(behind(), 3, 2);
    std::thread::sleep(Duration::from_millis(50));
    let second = run(behind(), 3, 2);

    assert_eq!(
        first.deadlines[..3],
        [
            NEEDED_BLOCK_QUERY_RETRY_DELAY,
            NEEDED_BLOCK_QUERY_RETRY_DELAY * 2,
            NEEDED_BLOCK_QUERY_RETRY_DELAY * 3,
        ],
        "each failed query retries one delay after the last, in virtual time"
    );
    assert_eq!(
        first, second,
        "host time passing between runs changes nothing"
    );
    assert_no_panic(&first);
}

/// A recorder that keeps each gauge's last value by name.
#[derive(Clone, Default)]
struct GaugeRecorder(Arc<Mutex<BTreeMap<String, f64>>>);

/// One gauge of a [`GaugeRecorder`].
struct RecordedGauge {
    name: String,
    values: Arc<Mutex<BTreeMap<String, f64>>>,
}

impl GaugeRecorder {
    /// The gauge's last value, if it was ever set.
    fn get(&self, name: &str) -> Option<f64> {
        self.0
            .lock()
            .expect("no gauge write panics")
            .get(name)
            .copied()
    }
}

impl metrics::GaugeFn for RecordedGauge {
    /// Adds `value` to the recorded value.
    fn increment(&self, value: f64) {
        *self
            .values
            .lock()
            .expect("no gauge write panics")
            .entry(self.name.clone())
            .or_default() += value;
    }

    /// Subtracts `value` from the recorded value.
    fn decrement(&self, value: f64) {
        self.increment(-value);
    }

    /// Replaces the recorded value.
    fn set(&self, value: f64) {
        self.values
            .lock()
            .expect("no gauge write panics")
            .insert(self.name.clone(), value);
    }
}

impl metrics::Recorder for GaugeRecorder {
    /// Ignores descriptions.
    fn describe_counter(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }

    /// Ignores descriptions.
    fn describe_gauge(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }

    /// Ignores descriptions.
    fn describe_histogram(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }

    /// Drops counter updates.
    fn register_counter(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        metrics::Counter::noop()
    }

    /// Records the gauge under its name, ignoring labels.
    fn register_gauge(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        metrics::Gauge::from_arc(Arc::new(RecordedGauge {
            name: key.name().to_owned(),
            values: Arc::clone(&self.0),
        }))
    }

    /// Drops histogram samples.
    fn register_histogram(
        &self,
        _: &metrics::Key,
        _: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}

#[test]
fn block_sync_alarm_age_follows_scenario_utc() {
    const ALARM_AGE: Duration = Duration::from_secs(10);
    let origin = SystemTime::UNIX_EPOCH + Duration::from_secs(1_750_000_000);
    let started_at = chrono::DateTime::<chrono::Utc>::from(origin - ALARM_AGE);
    let genesis = genesis();
    let alarms = AlarmSet {
        header_best_body_unavailable: Some(BodyUnavailableSummary {
            started_at,
            attempts: 3,
            suppliers: 0,
            supplier_set_digest: [0x51; 32],
            alarmed: true,
            next_probe_at: started_at + chrono::Duration::minutes(10),
        }),
        ..AlarmSet::default()
    };
    let scenario = Scenario {
        view: committed_view(genesis, genesis, alarms),
        drain_actions: true,
        utc_origin: Some(origin),
    };

    let gauges = GaugeRecorder::default();
    let observed = metrics::with_local_recorder(&gauges, || {
        let mut run = Run::start(scenario, 5);
        let settled = run.settle();
        assert_eq!(
            gauges.get(ALARM_AGE_GAUGE),
            Some(ALARM_AGE.as_secs_f64()),
            "the age at virtual time zero"
        );

        std::thread::sleep(Duration::from_millis(50));
        let tick = ZakuraBlockSyncConfig::default().request_timeout;
        while run.sched.now().since_start() < tick {
            run.advance();
        }
        let elapsed = run.sched.now().since_start();
        assert_eq!(
            gauges.get(ALARM_AGE_GAUGE),
            Some((ALARM_AGE + elapsed).as_secs_f64()),
            "the age after the next metrics tick follows virtual time only"
        );
        run.finish(settled)
    });
    assert_no_panic(&observed);
}
