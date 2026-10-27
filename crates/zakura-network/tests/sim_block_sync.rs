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
            HolderChange, LeaseStep, ObserverSlot, RequestStep,
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
        let (views, views_rx) = watch::channel(Some(view.clone()));
        let source = FixtureSource(chain[..verified].to_vec());
        let events = Recorder::default();
        let mut startup =
            startup(&view, views_rx, self.shutdown.clone()).with_range_source(Arc::new(source));
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
                        BlockSyncAction::SubmitBlock { block, .. } => submitted.borrow_mut().push(
                            block
                                .coinbase_height()
                                .expect("a submitted block has a height"),
                        ),
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

/// Each handoff ends with the same body and charge, and the budget releases
/// exactly that charge between the handoff's start and end. Another task can
/// run between the release and the end, because the release wakes budget
/// waiters.
fn check_handoffs(events: &[BlockEvent]) -> Result<usize, String> {
    let mut open = Vec::new();
    let mut claimed = Vec::new();
    let mut ended = 0;
    for (index, event) in events.iter().enumerate() {
        match event {
            BlockEvent::HandoffStarted {
                peer,
                owner,
                height,
                charge,
            } => open.push(((peer.clone(), *owner, *height, *charge), index)),
            BlockEvent::HandoffEnded {
                peer,
                owner,
                height,
                charge,
            } => {
                let key = (peer.clone(), *owner, *height, *charge);
                let Some(position) = open.iter().position(|(started, _)| *started == key) else {
                    return Err(format!("{height:?} ended a handoff it never started"));
                };
                let (_, started) = open.remove(position);
                let release = (started..index).find(|candidate| {
                    !claimed.contains(candidate)
                        && matches!(
                            events[*candidate],
                            BlockEvent::Budget(BudgetChange {
                                kind: BudgetChangeKind::Released,
                                before,
                                after,
                            }) if before - after == *charge
                        )
                });
                match release {
                    Some(release) => claimed.push(release),
                    None => return Err(format!("{height:?} ended with no release of {charge}")),
                }
                ended += 1;
            }
            _ => {}
        }
    }
    match open.as_slice() {
        [] => Ok(ended),
        unended => Err(format!("{unended:?} never ended")),
    }
}

/// Folds the holder events. At each sequencer snapshot, the fold's reorder,
/// `applying` and submission holders equal the snapshot's. Every queued
/// body leaves the queue, and every body that leaves it enters a sequencer
/// holder by the same allocation. Returns the heights the sequencer held, in
/// the order they arrived.
fn check_holders(events: &[BlockEvent]) -> Result<Vec<block::Height>, String> {
    let mut held: Vec<(Holder, HeldBody)> = Vec::new();
    let mut dequeued = Vec::new();
    let mut heights = Vec::new();
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
                if let Holder::Queued { allocation } = holder {
                    dequeued.push(*allocation);
                }
            }
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

/// Each serving lease is released after its frames: one block frame per
/// height it returns, then one ending. Returns the heights served.
fn check_serving(events: &[BlockEvent]) -> Result<u32, String> {
    let mut leases: Vec<(ZakuraPeerId, block::Height, u32, Vec<u64>)> = Vec::new();
    let mut served = 0;
    for event in events {
        match event {
            BlockEvent::ServingLease {
                peer,
                start,
                count,
                step: LeaseStep::Acquired,
            } => leases.push((peer.clone(), *start, *count, Vec::new())),
            BlockEvent::ServingFrame {
                peer,
                start,
                body_bytes,
                ..
            } => {
                let Some(lease) = leases
                    .iter_mut()
                    .find(|lease| lease.0 == *peer && lease.1 == *start)
                else {
                    return Err(format!("frame for {start:?} outside a lease"));
                };
                lease.3.push(*body_bytes);
            }
            BlockEvent::ServingLease {
                peer,
                start,
                count,
                step: LeaseStep::Released,
            } => {
                let Some(position) = leases
                    .iter()
                    .position(|lease| lease.0 == *peer && lease.1 == *start && lease.2 == *count)
                else {
                    return Err(format!("{start:?} released a lease it never acquired"));
                };
                let (.., frames) = leases.remove(position);
                let Some((0, blocks)) = frames.split_last() else {
                    return Err(format!("{start:?} ended with frames {frames:?}"));
                };
                if blocks.contains(&0) || blocks.len() > *count as usize {
                    return Err(format!("{start:?} of {count} sent {frames:?}"));
                }
                served += blocks.len() as u32; // at most `count`, a u32
            }
            _ => {}
        }
    }
    match leases.as_slice() {
        [] => Ok(served),
        held => Err(format!("{held:?} never released")),
    }
}

/// Block sync reports each effect of a download in an order that agrees
/// with its owners' snapshots, under each of 16 plans.
#[test]
fn block_sync_reports_each_effect_of_a_download() {
    for seed in 0..16 {
        check_download_events(seed);
    }
}

/// Runs the download under plan `seed` and checks both nodes' events.
fn check_download_events(seed: u64) {
    let net = download(seed);
    let (a, b) = (net.nodes[0].events.events(), net.nodes[1].events.events());

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
    assert_eq!(check_serving(&b), Ok(DOWNLOAD_HEIGHTS));
    assert_eq!(check_serving(&a), Ok(0), "`a` serves nothing");

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
    assert_eq!(net.finish(), ["completed", "completed"]);
}

/// Removing any one event a check reads from a download's log makes that
/// check fail.
#[test]
fn each_check_catches_a_suppressed_event() {
    let net = download(5);
    let (a, b) = (net.nodes[0].events.events(), net.nodes[1].events.events());
    assert_eq!(net.finish(), ["completed", "completed"]);

    let without = |events: &[BlockEvent], suppressed: fn(&BlockEvent) -> bool| {
        let index = events
            .iter()
            .position(suppressed)
            .expect("the download reports the event");
        let mut events = events.to_vec();
        events.remove(index);
        events
    };

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
