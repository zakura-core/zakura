//! The block-sync reactor and sequencer on the deterministic scheduler. These
//! only build with `RUSTFLAGS="--cfg zakura_sim"`, in a separate
//! `CARGO_TARGET_DIR`.
//!
//! Each node runs block sync fed one committed header-chain view, with a
//! fixture block source and an action task that answers block queries and
//! records submissions. Linked nodes talk over in-memory streams. A small
//! seeded `Plan` stands in for `zakura-sim`: it picks which ready task runs
//! and answers every random draw.
#![cfg(zakura_sim)]

use std::{
    cell::RefCell,
    collections::BTreeMap,
    rc::Rc,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use futures::future::BoxFuture;
use zakura_chain::{
    block,
    parameters::Network,
    serialization::{ZcashDeserializeInto, ZcashSerialize},
    transaction::Transaction,
    transparent,
};
use zakura_header_chain::{
    AlarmSet, BodyUnavailableSummary, BodyWorkEpoch, ChainScore, CommittedHeaderChainView,
    EngineMode, EngineSnapshot, Frontier, FrontierSet, HeaderGeneration, StateVersion, SuffixWork,
    VerifiedGeneration,
};
use zakura_network::{
    zakura::{
        block_sync_observe::{
            BlockEvent, BlockSyncObserver, BudgetChange, BudgetChangeKind, HeldBody, Holder,
            HolderChange, LeaseStep, ObserverSlot, RequestStep, RequestWritePhase, RoutineSnapshot,
            WorkItemPlace,
        },
        framed_channel, spawn_block_sync_reactor, BlockRangeRead, BlockRangeReadResult,
        BlockRangeSource, BlockSizeEstimate, BlockSyncAction, BlockSyncBlockMeta, BlockSyncEvent,
        BlockSyncFrontiers, BlockSyncHandle, BlockSyncMessage, BlockSyncService, BlockSyncStartup,
        CloseCauseReader, FramedRecv, FramedSend, Peer, Service, ServicePeerDirection,
        ZakuraBlockSyncConfig, ZakuraPeerId, ZAKURA_CAP_BLOCK_SYNC_REGULATED,
        ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION, ZAKURA_STREAM_BLOCK_SYNC,
    },
    BoxError,
};
use zakura_quic::NodeSecretKey;
use zakura_rt::sync::{watch, CancellationToken};
use zakura_scheduler::{RunOutcome, Scheduler, SchedulerConfig, SimInstant, Status};
use zakura_test::vectors::BLOCK_MAINNET_1_BYTES;

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

/// Mainnet block 1 with its coinbase rewritten to claim `height`, and its
/// merkle root recomputed. Each such block has its own hash, decodes, and
/// passes the routine's coinbase-height and merkle checks.
fn fixture_block(height: u32) -> Arc<block::Block> {
    let mut block: block::Block = BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .expect("mainnet block 1 decodes");
    let coinbase = Arc::make_mut(&mut block.transactions[0]);
    let input = match coinbase {
        Transaction::V1 { inputs, .. }
        | Transaction::V2 { inputs, .. }
        | Transaction::V3 { inputs, .. }
        | Transaction::V4 { inputs, .. }
        | Transaction::V5 { inputs, .. }
        | Transaction::V6 { inputs, .. } => &mut inputs[0],
    };
    let transparent::Input::Coinbase {
        height: coinbase_height,
        ..
    } = input
    else {
        panic!("block 1 starts with a coinbase input");
    };
    *coinbase_height = block::Height(height);
    let mut header = *block.header;
    header.merkle_root = block.transactions.iter().collect();
    block.header = Arc::new(header);
    Arc::new(block)
}

/// Serves the fixture blocks at heights 1 and up, as committed storage would.
#[derive(Debug)]
struct FixtureSource(Vec<Arc<block::Block>>);

impl BlockRangeSource for FixtureSource {
    /// Returns the stored blocks from `start_height`, up to the count and
    /// byte limits.
    fn read(
        &self,
        request: BlockRangeRead,
    ) -> BoxFuture<'static, Result<BlockRangeReadResult, BoxError>> {
        let mut blocks = Vec::new();
        let mut bytes = 0;
        for height in (request.start_height.0..).take(request.count as usize) {
            let Some(block) = height
                .checked_sub(1)
                .and_then(|index| self.0.get(index as usize))
            else {
                break;
            };
            let body = block.zcash_serialize_to_vec().expect("a block serializes");
            bytes += body.len();
            if bytes > request.max_body_bytes as usize {
                break;
            }
            blocks.push((block::Height(height), body));
        }
        let lease = request.lease;
        Box::pin(async move { Ok(BlockRangeReadResult { blocks, lease }) })
    }
}

/// Fails every read, as storage that can't read a stored block would.
#[derive(Debug)]
struct FailingSource;

impl BlockRangeSource for FailingSource {
    fn read(
        &self,
        _request: BlockRangeRead,
    ) -> BoxFuture<'static, Result<BlockRangeReadResult, BoxError>> {
        Box::pin(async { Err("the stored block can't be read".into()) })
    }
}

/// How long a run of linked nodes may take in virtual time.
const MAX_NET_TIME: Duration = Duration::from_secs(600);

/// One block-sync node: its reactor's handle, view sender and service, and
/// the heights its sequencer submitted.
struct Node {
    /// Shared with the node's action task, which replies through it. Shutdown
    /// takes it, so the sequencer's body input can close.
    handle: Rc<RefCell<Option<BlockSyncHandle>>>,
    _views: watch::Sender<Option<CommittedHeaderChainView>>,
    service: BlockSyncService,
    submitted: Rc<RefCell<Vec<block::Height>>>,
    /// Each height submitted before its sequencer reported the submission as
    /// a holder.
    unreported_submissions: Rc<RefCell<Vec<block::Height>>>,
    events: Recorder,
}

/// Block-sync nodes on one scheduler, linked by in-memory streams.
struct Net {
    sched: Scheduler,
    plan: Rc<RefCell<Plan>>,
    steps: Vec<String>,
    shutdown: CancellationToken,
    nodes: Vec<Node>,
    exits: Rc<RefCell<Vec<String>>>,
    /// Each linked connection's token and close-cause reader.
    connections: Vec<(CancellationToken, CloseCauseReader)>,
}

impl Net {
    fn new(seed: u64) -> Self {
        let plan = Rc::new(RefCell::new(Plan(seed)));
        let mut sched = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
        {
            let plan = Rc::clone(&plan);
            sched.set_draws(move |draw| plan.borrow_mut().below(draw.bound));
        }
        Net {
            sched,
            plan,
            steps: Vec::new(),
            shutdown: CancellationToken::new(),
            nodes: Vec::new(),
            exits: Rc::default(),
            connections: Vec::new(),
        }
    }

    /// Starts a node named `name` from `view`. Its storage holds `chain` up
    /// to the view's verified tip, and its action task answers every query
    /// with the rest of `chain`. It records each submission and replies to
    /// none.
    fn spawn(
        &mut self,
        name: &'static str,
        view: CommittedHeaderChainView,
        chain: &[Arc<block::Block>],
    ) -> usize {
        let verified = view.frontiers.verified_best.height.0 as usize;
        let source = Arc::new(FixtureSource(chain[..verified].to_vec()));
        self.spawn_with_source(name, view, chain, source)
    }

    /// [`Net::spawn`], serving from `source` instead of the stored chain.
    fn spawn_with_source(
        &mut self,
        name: &'static str,
        view: CommittedHeaderChainView,
        chain: &[Arc<block::Block>],
        source: Arc<dyn BlockRangeSource>,
    ) -> usize {
        let (views, views_rx) = watch::channel(Some(view.clone()));
        let events = Recorder::default();
        let mut startup = startup(&view, views_rx, self.shutdown.clone()).with_range_source(source);
        startup.observer = ObserverSlot(Some(Arc::new(events.clone())));
        let (handle, mut actions, reactor) =
            spawn_block_sync_reactor(startup).expect("block sync starts");
        let service = BlockSyncService::new_with_handle(handle.clone());

        let exits = Rc::clone(&self.exits);
        self.sched.spawn_root(name, async move {
            let exit = match reactor.await {
                Ok(()) => "completed".to_owned(),
                Err(error) => format!("{error}"),
            };
            exits.borrow_mut().push(exit);
        });

        let handle = Rc::new(RefCell::new(Some(handle)));
        let submitted = Rc::new(RefCell::new(Vec::new()));
        let unreported_submissions = Rc::new(RefCell::new(Vec::new()));
        let metas: Vec<_> = chain
            .iter()
            .map(|block| BlockSyncBlockMeta {
                height: block
                    .coinbase_height()
                    .expect("a fixture block has a height"),
                hash: block.hash(),
                size: BlockSizeEstimate::Confirmed(
                    block.zcash_serialized_size() as u32, // a block is below 2 MB
                ),
            })
            .collect();
        {
            let (handle, submitted) = (Rc::clone(&handle), Rc::clone(&submitted));
            let (unreported, events) = (Rc::clone(&unreported_submissions), events.clone());
            // The scheduler takes a `'static` label; the test leaks one per node.
            let label: &'static str = Box::leak(format!("{name}-actions").into_boxed_str());
            self.sched.spawn_root(label, async move {
                while let Some(action) = actions.recv().await {
                    match action {
                        BlockSyncAction::QueryNeededBlocks {
                            query_id,
                            from,
                            limit,
                            scope,
                            ..
                        } => {
                            let blocks = metas
                                .iter()
                                .filter(|meta| meta.height >= from)
                                .take(limit as usize)
                                .copied()
                                .collect();
                            if let Some(handle) = &*handle.borrow() {
                                let _ = handle.send_control(BlockSyncEvent::ScopedNeededBlocks {
                                    query_id,
                                    scope,
                                    body_anchor: genesis(),
                                    blocks,
                                });
                            }
                        }
                        BlockSyncAction::SubmitBlock { block, token, .. } => {
                            let height = block
                                .coinbase_height()
                                .expect("a submitted block has a height");
                            // The sequencer reports a submission's holder before
                            // it sends the action.
                            let reported = events.events().iter().any(|event| {
                                matches!(
                                    event,
                                    BlockEvent::Holder {
                                        body,
                                        holder: Holder::Submitted { token: held },
                                        change: HolderChange::Retained,
                                    } if *held == token && body.height == height
                                )
                            });
                            if !reported {
                                unreported.borrow_mut().push(height);
                            }
                            submitted.borrow_mut().push(height);
                        }
                        _ => {}
                    }
                }
            });
        }

        self.nodes.push(Node {
            handle,
            _views: views,
            service,
            submitted,
            unreported_submissions,
            events,
        });
        self.nodes.len() - 1
    }

    /// Opens a session on node `node` over `streams`, from `remote` in
    /// `direction`, on connection `conn_id`.
    fn add_peer(
        &mut self,
        node: usize,
        remote: ZakuraPeerId,
        direction: ServicePeerDirection,
        conn_id: u64,
        streams: (FramedRecv, FramedSend),
    ) {
        let connection = CancellationToken::new();
        let peer = Peer::new_versioned(
            conn_id,
            remote,
            ZAKURA_CAP_BLOCK_SYNC_REGULATED,
            direction,
            ZAKURA_STREAM_BLOCK_SYNC,
            ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION,
            1,
            streams,
            connection.clone(),
        );
        self.connections
            .push((connection, peer.close_cause_reader()));
        self.nodes[node].service.add_peer(peer);
    }

    /// Runs ready tasks, picked by the plan, until none is ready or `stop`
    /// holds.
    fn settle_until(&mut self, stop: impl Fn(&Self) -> bool) -> Status {
        loop {
            assert!(
                self.steps.len() < MAX_STEPS,
                "the run took {MAX_STEPS} steps"
            );
            let status = self.sched.status();
            if status != Status::Running || stop(self) {
                return status;
            }
            let ready = self.sched.ready();
            let index = self.plan.borrow_mut().below(ready.len() as u64); // a Vec's length fits in u64
            let pick = ready[index as usize]; // below `ready.len()`, a usize
            self.steps
                .push(format!("{}:{}", self.sched.task_name(pick.task), pick.at));
            if let RunOutcome::Panicked(message) = self.sched.run(pick.task) {
                self.steps.push(format!("panicked: {message}"));
            }
        }
    }

    /// Settles, advancing time whenever the run only waits on it, until
    /// `stop` holds, the run stops for another reason, or virtual time
    /// passes [`MAX_NET_TIME`].
    fn run_until(&mut self, stop: impl Fn(&Self) -> bool) -> Status {
        loop {
            match self.settle_until(&stop) {
                Status::WaitingOnTime
                    if !stop(self) && self.sched.now().since_start() < MAX_NET_TIME =>
                {
                    let next = self
                        .sched
                        .next_deadline()
                        .expect("WaitingOnTime has a timer");
                    self.sched.advance_to(next);
                    self.steps.push("advance-time".to_owned());
                }
                status => return status,
            }
        }
    }

    /// Shuts every node down and closes every connection. Each reactor ends
    /// before its handle drops, as [`Run::finish`] explains.
    fn finish(mut self) -> Vec<String> {
        self.shutdown.cancel();
        for (connection, _) in &self.connections {
            connection.cancel();
        }
        let count = self.nodes.len();
        self.run_until(|net| net.exits.borrow().len() == count);
        assert_eq!(self.exits.borrow().len(), count, "every reactor ends");
        for node in self.nodes.drain(..) {
            node.handle.take();
        }
        assert_eq!(
            self.run_until(|_| false),
            Status::Quiescent,
            "every task ends after shutdown"
        );
        assert_eq!(
            self.sched.shutdown(),
            Default::default(),
            "no task was left"
        );
        assert!(
            !self.steps.iter().any(|step| step.starts_with("panicked")),
            "{:?}",
            self.steps
        );
        self.exits.take()
    }
}

/// The peer identity of the node whose key is `seed` repeated.
fn peer_id(seed: u8) -> ZakuraPeerId {
    let node = NodeSecretKey::from_bytes(&[seed; 32]).public();
    ZakuraPeerId::new(node.as_bytes().to_vec()).expect("a node id is a valid peer id")
}

/// The fixture chain at heights 1 through `count`.
fn fixture_chain(count: u32) -> Vec<Arc<block::Block>> {
    (1..=count).map(fixture_block).collect()
}

/// The heights the download fixture serves.
const DOWNLOAD_HEIGHTS: u32 = 4;

/// Node `b` stores the fixture chain. Node `a` has its headers but no bodies,
/// so it downloads them from `b` over a pair of in-memory streams, one each
/// way. Runs until `a` submitted every height, and returns the net with `a`
/// at index 0 and `b` at index 1.
fn download(seed: u64) -> Net {
    let chain = fixture_chain(DOWNLOAD_HEIGHTS);
    let tip = Frontier::new(
        block::Height(DOWNLOAD_HEIGHTS),
        chain[chain.len() - 1].hash(),
    );
    let mut net = Net::new(seed);
    let a = net.spawn(
        "a",
        committed_view(genesis(), tip, AlarmSet::default()),
        &chain,
    );
    let b = net.spawn("b", committed_view(tip, tip, AlarmSet::default()), &chain);

    // `a` writes what `b` reads, and the other way round.
    let (a_send, b_recv) = framed_channel(8);
    let (b_send, a_recv) = framed_channel(8);
    net.add_peer(
        a,
        peer_id(2),
        ServicePeerDirection::Outbound,
        1,
        (a_recv, a_send),
    );
    net.add_peer(
        b,
        peer_id(1),
        ServicePeerDirection::Inbound,
        1,
        (b_recv, b_send),
    );

    let expected: Vec<_> = (1..=DOWNLOAD_HEIGHTS).map(block::Height).collect();
    let done = |net: &Net| *net.nodes[a].submitted.borrow() == expected;
    net.run_until(done);
    assert!(
        done(&net),
        "`a` submits every body in height order: {:?}",
        net.nodes[a].submitted.borrow()
    );
    // Let `a` handle every frame still in flight, including `b`'s last
    // ending, before the caller checks the run.
    net.settle_until(|_| false);
    net
}

/// `a` downloads every body from `b` and submits each in height order.
#[test]
fn block_sync_serves_a_range_over_memory_streams() {
    let net = download(3);
    assert!(
        net.nodes[1].submitted.borrow().is_empty(),
        "`b` already had every body"
    );
    for (connection, cause) in &net.connections {
        assert!(
            !connection.is_cancelled(),
            "neither service closed its connection"
        );
        assert_eq!(cause.get(), None, "no path recorded a close cause");
    }
    assert_eq!(net.finish(), ["completed", "completed"]);
}

/// A peer that sends `BlocksDone` for a range nobody asked for breaks the
/// stream-6 table. The node's routine rejects the frame, which closes the
/// connection and records why.
#[test]
fn an_unsolicited_ending_closes_the_connection_with_its_cause() {
    let genesis = genesis();
    let mut net = Net::new(4);
    let a = net.spawn(
        "a",
        committed_view(genesis, genesis, AlarmSet::default()),
        &[],
    );

    let (a_send, mut probe_recv) = framed_channel(8);
    let (probe_send, a_recv) = framed_channel(8);
    net.add_peer(
        a,
        peer_id(2),
        ServicePeerDirection::Outbound,
        1,
        (a_recv, a_send),
    );
    let frame = BlockSyncMessage::BlocksDone {
        start_height: block::Height(1),
        returned: 1,
    }
    .encode_frame()
    .expect("a valid ending encodes");
    net.sched.spawn_root("probe", async move {
        probe_send.send(frame).await.expect("`a` reads the stream");
        // Drains `a`'s status until the connection closes.
        while probe_recv.recv().await.is_some() {}
    });

    let (connection, cause) = net.connections[0].clone();
    net.run_until(|_| connection.is_cancelled());
    assert!(connection.is_cancelled(), "{:?}", net.steps);
    assert_eq!(cause.get(), Some("service_protocol_reject"));
    assert_eq!(net.finish(), ["completed"]);
}

/// A node's block-sync events, in the order its tasks reported them.
#[derive(Clone, Debug, Default)]
struct Recorder(Arc<Mutex<Vec<BlockEvent>>>);

impl Recorder {
    fn events(&self) -> Vec<BlockEvent> {
        self.0
            .lock()
            .expect("no observer call panics while it holds the log")
            .clone()
    }
}

impl BlockSyncObserver for Recorder {
    fn observe(&self, event: BlockEvent) {
        self.0
            .lock()
            .expect("no observer call panics while it holds the log")
            .push(event);
    }
}

/// Each routine's judged frames count up from 1 with no gap.
fn check_frame_sequence(events: &[BlockEvent]) -> Result<(), String> {
    let mut last = std::collections::HashMap::new();
    for event in events {
        if let BlockEvent::FrameJudged {
            peer,
            conn_id,
            generation,
            seq,
            ..
        } = event
        {
            let previous = last.insert((peer.clone(), *conn_id, *generation), *seq);
            if *seq != previous.unwrap_or(0) + 1 {
                return Err(format!("frame {seq} after {previous:?}"));
            }
        }
    }
    Ok(())
}

/// Each budget change starts where the previous one ended, from an empty
/// budget, and the budget ends empty.
fn check_budget_chain(events: &[BlockEvent]) -> Result<(), String> {
    let mut reserved = 0;
    for event in events {
        if let BlockEvent::Budget(BudgetChange {
            kind,
            before,
            after,
            ..
        }) = event
        {
            if *before != reserved {
                return Err(format!("{kind:?} from {before}, but {reserved} reserved"));
            }
            reserved = *after;
        }
    }
    match reserved {
        0 => Ok(()),
        left => Err(format!("{left} bytes left reserved")),
    }
}

/// Every published request reaches `Written`, after `Started`.
fn check_request_steps(events: &[BlockEvent]) -> Result<(), String> {
    let mut steps: Vec<(_, Vec<RequestStep>)> = Vec::new();
    for event in events {
        if let BlockEvent::Request { owner, step, .. } = event {
            match steps.iter_mut().find(|(known, _)| known == owner) {
                Some((_, list)) => list.push(*step),
                None => steps.push((*owner, vec![*step])),
            }
        }
    }
    for (owner, list) in &steps {
        let position = |wanted| list.iter().position(|step| *step == wanted);
        match (
            position(RequestStep::Published),
            position(RequestStep::Started),
            position(RequestStep::Written),
        ) {
            (Some(0), Some(started), Some(written)) if started < written => {}
            _ => return Err(format!("{owner:?} took {list:?}")),
        }
    }
    Ok(())
}

/// Each handoff starts before the routine retains its body's `Queued`
/// holder, so the handoff covers every moment the sequencer's input holds
/// the body. Each ends with the same body and charge. The budget's release
/// of exactly that charge comes just before the end, because the routine
/// reports the end after the counter changes and before the release wakes
/// any waiter. Returns the handoffs that ended.
fn check_handoffs(events: &[BlockEvent]) -> Result<usize, String> {
    // Each open handoff, and whether its body's `Queued` holder followed.
    let mut open = Vec::new();
    let mut queued = Vec::new();
    let mut ended = 0;
    for (index, event) in events.iter().enumerate() {
        match event {
            BlockEvent::HandoffStarted {
                peer,
                owner,
                height,
                charge,
            } => {
                open.push((peer.clone(), *owner, *height, *charge));
                queued.push(false);
            }
            BlockEvent::Holder {
                body,
                holder: Holder::Queued { .. },
                change: HolderChange::Retained,
            } => {
                if let Some(position) =
                    open.iter()
                        .zip(&queued)
                        .position(|((_, owner, height, _), queued)| {
                            !queued && *owner == body.owner && *height == body.height
                        })
                {
                    queued[position] = true;
                }
            }
            BlockEvent::HandoffEnded {
                peer,
                owner,
                height,
                charge,
            } => {
                let key = (peer.clone(), *owner, *height, *charge);
                let Some(position) = open.iter().position(|started| *started == key) else {
                    return Err(format!("{height:?} ended a handoff it never started"));
                };
                open.remove(position);
                if !queued.remove(position) {
                    return Err(format!(
                        "{height:?}'s handoff started after its body was queued"
                    ));
                }
                match index.checked_sub(1).map(|previous| &events[previous]) {
                    Some(BlockEvent::Budget(BudgetChange {
                        kind: BudgetChangeKind::Released,
                        before,
                        after,
                        ..
                    })) if before - after == *charge => ended += 1,
                    other => return Err(format!("{height:?} ended after {other:?}")),
                }
            }
            _ => {}
        }
    }
    match open.as_slice() {
        [] => Ok(ended),
        unended => Err(format!("{unended:?} never ended")),
    }
}

/// The write phase each request step moves its request to, if it moves it.
/// `Dropped` reports the phase its request ended in without moving it.
fn phase_after(step: RequestStep) -> Option<RequestWritePhase> {
    match step {
        RequestStep::Published => Some(RequestWritePhase::Queued),
        RequestStep::Started => Some(RequestWritePhase::Started),
        RequestStep::Written => Some(RequestWritePhase::Written),
        RequestStep::Expired => Some(RequestWritePhase::Skipped),
        RequestStep::Dropped(_) | RequestStep::DeliveryFailed | RequestStep::Reset => None,
    }
}

/// Folds each request's steps into the phases it passed through, from
/// `Unpublished`. Every request in a routine snapshot shows a phase its
/// steps already reached. A snapshot can lag its request's steps, because
/// the routine reads each phase before it reports the snapshot, but it can
/// never lead them: a phase no step reported means a missed step. Each
/// `Dropped` step reports the last phase the steps reached.
///
/// The check relies on the sim's single thread. The status reports
/// `Expired` after its compare-exchange, so on a multi-threaded runtime the
/// handle's drop on another thread can read the expired phase and report
/// `Dropped(Skipped)` first. This check would then fail a correct run.
fn check_request_snapshots(events: &[BlockEvent]) -> Result<(), String> {
    let mut reached: Vec<(_, Vec<RequestWritePhase>)> = Vec::new();
    for event in events {
        match event {
            BlockEvent::Request { owner, step, .. } => {
                let index = match reached.iter().position(|(known, _)| known == owner) {
                    Some(index) => index,
                    None => {
                        reached.push((*owner, vec![RequestWritePhase::Unpublished]));
                        reached.len() - 1
                    }
                };
                let phases = &mut reached[index].1;
                if let RequestStep::Dropped(phase) = step {
                    if phases.last() != Some(phase) {
                        return Err(format!(
                            "{owner:?} dropped in {phase:?} after reaching {phases:?}"
                        ));
                    }
                }
                phases.extend(phase_after(*step));
            }
            BlockEvent::Routine(snapshot) => {
                for request in &snapshot.requests {
                    let phases = reached
                        .iter()
                        .find(|(owner, _)| *owner == request.owner)
                        .map_or(&[RequestWritePhase::Unpublished][..], |(_, phases)| phases);
                    if !phases.contains(&request.write) {
                        return Err(format!(
                            "{:?} shows {:?}, but its steps reached only {phases:?}",
                            request.owner, request.write
                        ));
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Folds the holder events. At each sequencer snapshot, the fold's reorder,
/// `applying` and submission holders equal the snapshot's. Every queued
/// body leaves the queue, and every body that leaves it before the
/// sequencer closes enters a sequencer holder by the same allocation. After
/// [`BlockEvent::SequencerClosed`], a queued body's release has no next
/// holder. Returns the heights the sequencer held, in the order they
/// arrived.
///
/// The sequencer reports its `Reorder`, `Applying`, `Submitted` and
/// `Detached` holders by diffing its own snapshots, so the fold can't
/// disagree with those snapshots. Real runs check those holders
/// independently in two places: a dequeued body must enter one by its
/// allocation, and the action task checks that each `SubmitBlock` follows
/// its `Submitted` holder.
fn check_holders(events: &[BlockEvent]) -> Result<Vec<block::Height>, String> {
    let mut held: Vec<(Holder, HeldBody)> = Vec::new();
    let mut dequeued = Vec::new();
    let mut heights = Vec::new();
    let mut closed = false;
    for event in events {
        match event {
            BlockEvent::Holder {
                body,
                holder,
                change: HolderChange::Retained,
            } => {
                if let Holder::Reorder { allocation } | Holder::Applying { allocation } = holder {
                    dequeued.retain(|queued| queued != allocation);
                    if !heights.contains(&body.height) {
                        heights.push(body.height);
                    }
                }
                held.push((*holder, *body));
            }
            BlockEvent::Holder {
                body,
                holder,
                change: HolderChange::Released,
            } => {
                let Some(position) = held.iter().position(|entry| *entry == (*holder, *body))
                else {
                    return Err(format!("{holder:?} released {body:?} it never held"));
                };
                held.remove(position);
                match holder {
                    Holder::Queued { allocation } if !closed => dequeued.push(*allocation),
                    _ => {}
                }
            }
            BlockEvent::SequencerClosed => closed = true,
            BlockEvent::Sequencer(snapshot) => {
                let mut expected = snapshot.holders();
                for entry in held
                    .iter()
                    .filter(|(holder, _)| !matches!(holder, Holder::Queued { .. }))
                {
                    let Some(position) = expected.iter().position(|other| other == entry) else {
                        return Err(format!("{entry:?} folded but not in {snapshot:?}"));
                    };
                    expected.remove(position);
                }
                if !expected.is_empty() {
                    return Err(format!("{expected:?} in the snapshot but not folded"));
                }
            }
            _ => {}
        }
    }
    if let Some(queued) = held
        .iter()
        .find(|(holder, _)| matches!(holder, Holder::Queued { .. }))
    {
        return Err(format!("{queued:?} never left the queue"));
    }
    match dequeued.as_slice() {
        [] => Ok(heights),
        lost => Err(format!("allocations {lost:?} left the queue to no holder")),
    }
}

/// Teardown returns every owner to its baseline: each routine's last
/// snapshot holds no request or claim, the work queue's last snapshot holds
/// no provisional item or reservation, no body has a holder, and the
/// sequencer's last snapshot is empty.
///
/// A received item stays in flight until the state commits its block. The
/// fixture never commits, so the queue keeps those items, and the snapshot
/// can't tell them from unreceived ones. The routine snapshot covers the
/// unreceived ones: its requests and claims are empty only after the
/// routine returned their heights.
fn check_teardown(events: &[BlockEvent]) -> Result<(), String> {
    let mut routines: Vec<(_, &RoutineSnapshot)> = Vec::new();
    let mut held: Vec<(Holder, HeldBody)> = Vec::new();
    let (mut queue, mut sequencer) = (None, None);
    for event in events {
        match event {
            BlockEvent::Routine(snapshot) => {
                let key = (snapshot.peer.clone(), snapshot.conn_id, snapshot.generation);
                match routines.iter_mut().find(|(known, _)| *known == key) {
                    Some((_, last)) => *last = snapshot,
                    None => routines.push((key, snapshot)),
                }
            }
            BlockEvent::WorkQueue(items) => queue = Some(items),
            BlockEvent::Sequencer(snapshot) => sequencer = Some(snapshot),
            BlockEvent::Holder {
                body,
                holder,
                change: HolderChange::Retained,
            } => held.push((*holder, *body)),
            BlockEvent::Holder {
                body,
                holder,
                change: HolderChange::Released,
            } => held.retain(|entry| *entry != (*holder, *body)),
            _ => {}
        }
    }
    if let Some((key, last)) = routines
        .iter()
        .find(|(_, last)| !last.requests.is_empty() || !last.claims.is_empty())
    {
        return Err(format!("routine {key:?} ended with {last:?}"));
    }
    if let Some(item) = queue
        .into_iter()
        .flatten()
        .find(|item| item.place == WorkItemPlace::Provisional || item.reserved_bytes > 0)
    {
        return Err(format!("the queue ended with {item:?}"));
    }
    if !held.is_empty() {
        return Err(format!("{held:?} still held"));
    }
    match sequencer {
        Some(last) if *last != Default::default() => {
            Err(format!("the sequencer ended with {last:?}"))
        }
        _ => Ok(()),
    }
}

/// One serving response, as [`check_serving`] folds it.
struct Response {
    /// The serving session: its peer, connection and routine generation.
    session: (ZakuraPeerId, u64, u64),
    start: block::Height,
    count: u32,
    released: bool,
    blocks: Vec<u64>,
    /// Whether the ending came after the release, if it came.
    ending_after_release: Option<bool>,
}

/// Each serving response takes its lease, queues block frames only while it
/// holds the lease, and queues exactly one ending. A produced response ends
/// before its release. Serve's failure ending comes after the release.
/// Returns the block frames served and the failure endings.
fn check_serving(events: &[BlockEvent]) -> Result<(usize, usize), String> {
    let mut responses: Vec<Response> = Vec::new();
    for event in events {
        match event {
            BlockEvent::ServingLease {
                peer,
                conn_id,
                generation,
                start,
                count,
                step: LeaseStep::Acquired,
            } => responses.push(Response {
                session: (peer.clone(), *conn_id, *generation),
                start: *start,
                count: *count,
                released: false,
                blocks: Vec::new(),
                ending_after_release: None,
            }),
            BlockEvent::ServingLease {
                peer,
                conn_id,
                generation,
                start,
                count,
                step: LeaseStep::Released,
            } => {
                let session = (peer.clone(), *conn_id, *generation);
                let Some(response) = responses.iter_mut().find(|response| {
                    response.session == session
                        && response.start == *start
                        && response.count == *count
                        && !response.released
                }) else {
                    return Err(format!("{start:?} released a lease it never acquired"));
                };
                response.released = true;
            }
            BlockEvent::ServingFrame {
                peer,
                conn_id,
                generation,
                start,
                body_bytes,
                ..
            } => {
                let session = (peer.clone(), *conn_id, *generation);
                let Some(response) = responses.iter_mut().find(|response| {
                    response.session == session
                        && response.start == *start
                        && response.ending_after_release.is_none()
                }) else {
                    return Err(format!("frame for {start:?} outside a response"));
                };
                match (*body_bytes, response.released) {
                    (0, released) => response.ending_after_release = Some(released),
                    (_, false) => response.blocks.push(*body_bytes),
                    (_, true) => return Err(format!("{start:?} sent a block after its release")),
                }
            }
            _ => {}
        }
    }
    let (mut served, mut failures) = (0, 0);
    for response in &responses {
        let start = response.start;
        match response.ending_after_release {
            _ if !response.released => return Err(format!("{start:?} never released")),
            None => return Err(format!("{start:?} never ended")),
            Some(_) if response.blocks.len() > response.count as usize => {
                return Err(format!("{start:?} sent {} blocks", response.blocks.len()))
            }
            Some(after) => {
                served += response.blocks.len();
                failures += usize::from(after);
            }
        }
    }
    Ok((served, failures))
}

/// Block sync reports each effect of a download in an order that agrees
/// with its owners' snapshots, under each of 16 plans.
#[test]
fn block_sync_reports_each_effect_of_a_download() {
    for seed in 0..16 {
        check_download_events(seed);
    }
}

/// Runs the download under plan `seed`, shuts it down, and checks both
/// nodes' events.
fn check_download_events(seed: u64) {
    let net = download(seed);
    let unreported = net.nodes[0].unreported_submissions.borrow().clone();
    let recorders = [net.nodes[0].events.clone(), net.nodes[1].events.clone()];
    assert_eq!(net.finish(), ["completed", "completed"]);
    let [a, b] = recorders.map(|recorder| recorder.events());

    let admitted = |events: &[BlockEvent]| -> Vec<_> {
        events
            .iter()
            .filter_map(|event| match event {
                BlockEvent::SessionAdmitted {
                    peer,
                    conn_id,
                    direction,
                    ..
                } => Some((peer.clone(), *conn_id, *direction)),
                _ => None,
            })
            .collect()
    };
    assert_eq!(
        admitted(&a),
        [(peer_id(2), 1, ServicePeerDirection::Outbound)]
    );
    assert_eq!(
        admitted(&b),
        [(peer_id(1), 1, ServicePeerDirection::Inbound)]
    );

    for events in [&a, &b] {
        check_frame_sequence(events).unwrap();
        check_budget_chain(events).unwrap();
        check_request_steps(events).unwrap();
        check_request_snapshots(events).unwrap();
        check_teardown(events).unwrap();
    }
    let judged = |events: &[BlockEvent]| {
        events
            .iter()
            .filter(|event| matches!(event, BlockEvent::FrameJudged { .. }))
            .count()
    };
    assert!(
        judged(&a) > DOWNLOAD_HEIGHTS as usize,
        "`a` judged each body and ending"
    );
    assert!(judged(&b) > 0, "`b` judged `a`'s requests");
    assert!(
        a.iter().any(|event| matches!(
            event,
            BlockEvent::Request {
                step: RequestStep::Written,
                ..
            }
        )),
        "`a` wrote its requests"
    );

    assert_eq!(check_handoffs(&a), Ok(DOWNLOAD_HEIGHTS as usize));
    let mut held = check_holders(&a).unwrap();
    held.sort();
    assert_eq!(
        held,
        (1..=DOWNLOAD_HEIGHTS)
            .map(block::Height)
            .collect::<Vec<_>>()
    );
    assert!(
        unreported.is_empty(),
        "submitted before their holder: {unreported:?}"
    );
    assert_eq!(check_serving(&b), Ok((DOWNLOAD_HEIGHTS as usize, 0)));
    assert_eq!(check_serving(&a), Ok((0, 0)), "`a` serves nothing");

    for events in [&a, &b] {
        assert!(events
            .iter()
            .any(|event| matches!(event, BlockEvent::Routine(_))));
        assert!(events
            .iter()
            .any(|event| matches!(event, BlockEvent::WorkQueue(_))));
    }
    assert!(
        a.iter()
            .any(|event| matches!(event, BlockEvent::Sequencer(_))),
        "`a`'s sequencer held bodies"
    );
}

/// Removing any one event a check reads from a download's log makes that
/// check fail. The hooks themselves are covered by the real runs above and
/// the deliberate breaks in the commit message.
#[test]
fn each_check_catches_a_suppressed_event() {
    let net = download(5);
    let recorders = [net.nodes[0].events.clone(), net.nodes[1].events.clone()];
    assert_eq!(net.finish(), ["completed", "completed"]);
    let [a, b] = recorders.map(|recorder| recorder.events());

    fn without(events: &[BlockEvent], suppressed: impl Fn(&BlockEvent) -> bool) -> Vec<BlockEvent> {
        let index = events
            .iter()
            .position(suppressed)
            .expect("the download reports the event");
        let mut events = events.to_vec();
        events.remove(index);
        events
    }

    let first_frame = without(&a, |event| matches!(event, BlockEvent::FrameJudged { .. }));
    assert!(check_frame_sequence(&first_frame).is_err());
    let first_budget = without(&a, |event| matches!(event, BlockEvent::Budget(_)));
    assert!(check_budget_chain(&first_budget).is_err());
    let first_written = without(&a, |event| {
        matches!(
            event,
            BlockEvent::Request {
                step: RequestStep::Written,
                ..
            }
        )
    });
    assert!(check_request_steps(&first_written).is_err());
    // Remove the `Written` step of the first request a snapshot shows
    // written, so only the snapshot can tell.
    let shown_written = a
        .iter()
        .find_map(|event| match event {
            BlockEvent::Routine(snapshot) => snapshot
                .requests
                .iter()
                .find(|request| request.write == RequestWritePhase::Written)
                .map(|request| request.owner),
            _ => None,
        })
        .expect("a snapshot shows a written request");
    let unwritten = without(&a, |event| {
        matches!(
            event,
            BlockEvent::Request {
                owner,
                step: RequestStep::Written,
                ..
            } if *owner == shown_written
        )
    });
    assert!(check_request_snapshots(&unwritten).is_err());
    let first_ended = without(&a, |event| matches!(event, BlockEvent::HandoffEnded { .. }));
    assert!(check_handoffs(&first_ended).is_err());
    let first_retained = without(&a, |event| {
        matches!(
            event,
            BlockEvent::Holder {
                holder: Holder::Reorder { .. } | Holder::Applying { .. },
                change: HolderChange::Retained,
                ..
            }
        )
    });
    assert!(check_holders(&first_retained).is_err());
    let first_dequeue = without(&a, |event| {
        matches!(
            event,
            BlockEvent::Holder {
                holder: Holder::Queued { .. },
                change: HolderChange::Released,
                ..
            }
        )
    });
    assert!(check_holders(&first_dequeue).is_err());
    assert!(check_teardown(&first_dequeue).is_err());
    let first_lease_end = without(&b, |event| {
        matches!(
            event,
            BlockEvent::ServingLease {
                step: LeaseStep::Released,
                ..
            }
        )
    });
    assert!(check_serving(&first_lease_end).is_err());
}

/// Links `a`, which needs the fixture chain's bodies, to `b`, which serves
/// them from `source`. `b` reads what `a` writes only when `b_reads`; else
/// `a`'s frames stay queued. Returns the net and the streams the test keeps
/// alive.
fn linked(
    seed: u64,
    source: Option<Arc<dyn BlockRangeSource>>,
    b_reads: bool,
) -> (Net, Vec<FramedRecv>, Vec<FramedSend>) {
    let chain = fixture_chain(DOWNLOAD_HEIGHTS);
    let tip = Frontier::new(
        block::Height(DOWNLOAD_HEIGHTS),
        chain[chain.len() - 1].hash(),
    );
    let mut net = Net::new(seed);
    let a = net.spawn(
        "a",
        committed_view(genesis(), tip, AlarmSet::default()),
        &chain,
    );
    let b_view = committed_view(tip, tip, AlarmSet::default());
    let b = match source {
        Some(source) => net.spawn_with_source("b", b_view, &chain, source),
        None => net.spawn("b", b_view, &chain),
    };
    let (a_send, a_to_b) = framed_channel(8);
    let (b_send, a_recv) = framed_channel(8);
    let (mut kept_recv, mut kept_send) = (Vec::new(), Vec::new());
    let b_recv = if b_reads {
        a_to_b
    } else {
        let (feed, b_recv) = framed_channel(8);
        kept_recv.push(a_to_b);
        kept_send.push(feed);
        b_recv
    };
    net.add_peer(
        a,
        peer_id(2),
        ServicePeerDirection::Outbound,
        1,
        (a_recv, a_send),
    );
    net.add_peer(
        b,
        peer_id(1),
        ServicePeerDirection::Inbound,
        1,
        (b_recv, b_send),
    );
    (net, kept_recv, kept_send)
}

/// `b` never reads what `a` writes, so transport never takes `a`'s request
/// frames. Each request's timeout expires it while its frame is still
/// queued, and the expiry reports its step.
#[test]
fn a_request_that_times_out_unwritten_reports_its_expiry() {
    let (mut net, kept_recv, kept_send) = linked(6, None, false);
    let expired = |net: &Net| {
        net.nodes[0].events.events().iter().any(|event| {
            matches!(
                event,
                BlockEvent::Request {
                    step: RequestStep::Expired,
                    ..
                }
            )
        })
    };
    net.run_until(expired);
    assert!(expired(&net), "{:?}", net.steps.last());
    // Run on until the routine holds a request again, so the shutdown
    // below finds requests to return.
    let holding = |net: &Net| {
        net.nodes[0]
            .events
            .events()
            .iter()
            .rev()
            .find_map(|event| match event {
                BlockEvent::Routine(snapshot) => Some(!snapshot.requests.is_empty()),
                _ => None,
            })
            .unwrap_or(false)
    };
    net.run_until(holding);
    assert!(holding(&net), "{:?}", net.steps.last());
    let a = net.nodes[0].events.events();
    let owner = a
        .iter()
        .find_map(|event| match event {
            BlockEvent::Request {
                owner,
                step: RequestStep::Expired,
                ..
            } => Some(*owner),
            _ => None,
        })
        .expect("a request expired");
    let steps: Vec<_> = a
        .iter()
        .filter_map(|event| match event {
            BlockEvent::Request {
                owner: stepped,
                step,
                ..
            } if *stepped == owner => Some(*step),
            _ => None,
        })
        .collect();
    assert_eq!(
        steps[..2],
        [RequestStep::Published, RequestStep::Expired],
        "the request expired while queued, before any reset or drop"
    );
    check_request_snapshots(&a).unwrap();
    let recorder = net.nodes[0].events.clone();
    assert_eq!(net.finish(), ["completed", "completed"]);
    drop((kept_recv, kept_send));

    // `a` still had requests out when the run shut down. Its routine's
    // drop snapshot shows them returned.
    let a = recorder.events();
    check_request_snapshots(&a).unwrap();
    check_teardown(&a).unwrap();
    let index = a
        .iter()
        .rposition(|event| matches!(event, BlockEvent::Routine(_)))
        .expect("the routine reports its snapshots");
    let BlockEvent::Routine(before_drop) = &a[..index]
        .iter()
        .rev()
        .find(|event| matches!(event, BlockEvent::Routine(_)))
        .expect("the routine reported before its drop")
    else {
        unreachable!("found above")
    };
    assert!(
        !before_drop.requests.is_empty(),
        "the routine held requests until its drop"
    );
    let mut without_drop = a.clone();
    without_drop.remove(index);
    assert!(check_teardown(&without_drop).is_err());
}

/// `b` can't read the blocks it stores. Serve releases each response's
/// lease, then queues the failure ending, and reports both.
#[test]
fn a_serving_read_failure_reports_its_failure_ending() {
    let (mut net, kept_recv, kept_send) = linked(7, Some(Arc::new(FailingSource)), true);
    let failed = |net: &Net| {
        net.nodes[1]
            .events
            .events()
            .iter()
            .any(|event| matches!(event, BlockEvent::ServingFrame { body_bytes: 0, .. }))
    };
    net.run_until(failed);
    assert!(failed(&net), "{:?}", net.steps.last());
    let b = net.nodes[1].events.events();
    let ending = b
        .iter()
        .position(|event| matches!(event, BlockEvent::ServingFrame { body_bytes: 0, .. }))
        .expect("serve queued an ending");
    assert_eq!(
        check_serving(&b[..=ending]).map(|(served, _)| served),
        Ok(0),
        "no block was served"
    );
    let BlockEvent::ServingFrame { start, .. } = &b[ending] else {
        unreachable!("found above")
    };
    assert!(
        b[..ending].iter().any(|event| matches!(
            event,
            BlockEvent::ServingLease {
                start: released,
                step: LeaseStep::Released,
                ..
            } if released == start
        )),
        "the failure ending follows the lease's release"
    );
    assert_eq!(net.finish(), ["completed", "completed"]);
    drop((kept_recv, kept_send));
}
