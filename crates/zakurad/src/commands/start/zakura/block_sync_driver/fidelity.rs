//! ST4.6: the real block-sync driver passes zakura-storage's shared cases,
//! and block sync sees the same events from it as from the simulator's
//! controlled block service.
//!
//! A case with no verdict runs the driver over a real ephemeral zakura-state
//! holding the case's headers. Its state reads and records go to that
//! state, and an error from a record is its refusal. A case with verdicts runs the driver over fakes that share one
//! zakura-storage engine. State reads answer from the engine's full state,
//! and records go to the engine. In both, the verifier waits for each case's
//! verdict, committing the body to the engine, in the order the verifier
//! received it, when the verdict commits it. Each verdict is an error the
//! real verifier returns, wrapped by its router, so
//! `block_verify_error_class` classifies it.
//! `zakura_storage::block::cases` lists the driver behavior the cases leave
//! out. The engine fakes share the service's commits, fork choice and
//! queries, so `committed_state.rs` checks those against real zakura-state.

use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use tokio::sync::oneshot;
use tower::service_fn;
use zakura_chain::parameters::subsidy::SubsidyError;
use zakura_header_chain::{ConsensusBodyInvalid, Frontier, StateVersion};
use zakura_network::zakura::DetachedHandleEvents;
use zakura_node_services::constants::MAX_CHECKPOINT_HEIGHT_GAP;
use zakura_storage::{
    block::{
        cases::{cases, insert_headers, run_controlled, Case, Observed, Script, Step},
        controlled::record_request,
        evidence::RecordResult,
        Commitment, HandleEvent, RetryKind, Verdict, MIN_APPLY_LIMIT, MIN_CHECKPOINT_APPLY_LIMIT,
    },
    header::adapter::EnginePort,
};

use super::{super::header_sync_driver::engine_comparison::ProductionState, *};

/// A verifier reply.
type Reply = Result<block::Hash, zakura_consensus::BoxError>;

/// What the fakes share with the test.
#[derive(Default)]
struct Shared {
    /// Each body the verifier was asked to commit, in order.
    calls: Mutex<Vec<block::Hash>>,
    /// The waiting verifier calls, oldest first, by hash, each with the
    /// order the verifier received it in.
    waiting: Mutex<HashMap<block::Hash, VecDeque<(oneshot::Sender<Reply>, u64)>>>,
    /// The last verifier receipt handed out.
    receipts: AtomicU64,
    /// Each record attempt's result, in order.
    records: Mutex<Vec<RecordResult>>,
    /// Every invalid body block sync has recorded, to unseal records with.
    invalid: Mutex<Vec<ConsensusBodyInvalid>>,
    /// Whether a query's later chunks see a moved anchor.
    anchor_moves: AtomicBool,
    /// The state calls zakura-state hasn't answered yet.
    in_flight: AtomicUsize,
}

/// Where the driver's state reads and records go.
enum Backend {
    /// The case's engine, through fakes.
    Engine(Arc<EnginePort>),
    /// A real zakura-state holding the case's headers.
    State(Box<ProductionState>),
}

impl Backend {
    /// The backend for `case`, over `script`'s chain.
    async fn for_case(case: &Case, script: &Script) -> Self {
        if case
            .steps
            .iter()
            .any(|(step, _)| matches!(step, Step::Verdict(..)))
        {
            return Backend::Engine(script.engine.clone());
        }
        let state = ProductionState::start().await;
        insert_headers(&*state.port(), &script.blocks).await;
        Backend::State(Box::new(state))
    }

    /// The latest view of the state the driver runs on.
    fn view(&self) -> zakura_header_chain::CommittedHeaderChainView {
        match self {
            Backend::Engine(engine) => engine.view(),
            Backend::State(state) => state
                .read_state
                .subscribe_header_chain_views()
                .borrow()
                .clone()
                .expect("an attached header runtime publishes its view"),
        }
    }
}

/// Locks `mutex`. A poisoned lock means a fake already panicked.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().expect("no fake panics while it holds a lock")
}

/// The driver over fakes, for one case.
struct DriverRun {
    actions: mpsc::Sender<BlockSyncAction>,
    events: DetachedHandleEvents,
    shared: Arc<Shared>,
    shutdown: oneshot::Sender<()>,
    driver: tokio::task::JoinHandle<()>,
}

impl DriverRun {
    /// Spawns the driver for `case`, over `backend`.
    fn spawn(case: &Case, backend: &Backend) -> Self {
        let shared = Arc::new(Shared::default());
        let (block_sync, events) = BlockSyncHandle::detached();
        let (actions, action_rx) = mpsc::channel(64);
        let (shutdown, shutdown_rx) = oneshot::channel();
        let (read_state, writer, authority) = match backend {
            Backend::Engine(engine) => {
                let authority = zakura_state::HeaderChainBodyEvidenceAuthority::new_test();
                (
                    read_state(engine.clone(), shared.clone()),
                    writer(engine.clone(), shared.clone(), authority.clone()),
                    authority,
                )
            }
            Backend::State(state) => (
                state_reads(state, shared.clone()),
                state_writes(state, shared.clone()),
                state.authority.clone(),
            ),
        };
        let driver = tokio::spawn(drive_block_sync_actions(
            action_rx,
            zakura_network::zakura::ZakuraSupervisorHandle::new(1),
            None,
            block_sync,
            zakura_chain::chain_tip::NoChainTip,
            read_state,
            Some(writer),
            Some(authority),
            verifier(shared.clone()),
            block::Height(case.limits.max_checkpoint_height),
            case.limits.checkpoint,
            case.limits.full,
            case.limits.combined,
            ZakuraTrace::noop(),
            None,
            super::super::SyncCoordinator::new(),
            async move {
                let _ = shutdown_rx.await;
            },
        ));
        DriverRun {
            actions,
            events,
            shared,
            shutdown,
            driver,
        }
    }

    /// Lets the driver run until it waits on something only the test can
    /// give it. The runtime has one thread, so the driver runs only here.
    /// zakura-state answers on its own threads, so this also waits until
    /// it has answered every call.
    ///
    /// # Panics
    ///
    /// If zakura-state still has calls in flight after 10 s.
    async fn settle(&self) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            for _ in 0..256 {
                tokio::task::yield_now().await;
            }
            if self.shared.in_flight.load(Ordering::SeqCst) == 0 {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "zakura-state answers every call within 10 s"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// What block sync saw since the last call. `unadmitted` holds each
    /// submitted hash's tokens that haven't reached the verifier.
    fn observe(
        &mut self,
        seen: &mut Observed,
        unadmitted: &mut HashMap<block::Hash, VecDeque<BlockApplyToken>>,
    ) {
        for hash in lock(&self.shared.calls).drain(..) {
            let token = unadmitted
                .get_mut(&hash)
                .and_then(VecDeque::pop_front)
                .expect("the driver verifies only submitted bodies");
            seen.admitted.push(token);
        }
        seen.records.extend(lock(&self.shared.records).drain(..));
        let mut events = Vec::new();
        while let Some(event) = self.events.try_next_control() {
            events.push(
                HandleEvent::from_control(event).expect("the driver sends only block-sync replies"),
            );
        }
        while let Some((query_id, scope)) = self.events.try_next_query_failure() {
            events.push(HandleEvent::NeededBlocksFailed { query_id, scope });
        }
        seen.push_events(events);
    }

    /// Shuts the driver down, failing each verifier call it still makes.
    async fn shut_down(self) {
        let _ = self.shutdown.send(());
        let shared = self.shared.clone();
        let fail_calls = async move {
            loop {
                // A finished apply can admit a queued one before the driver
                // sees its shutdown.
                lock(&shared.waiting).clear();
                tokio::task::yield_now().await;
            }
        };
        tokio::select! {
            finished = self.driver => finished.expect("the driver doesn't panic"),
            _ = tokio::time::timeout(Duration::from_secs(5), fail_calls) => {
                panic!("the driver exits once its applies finish")
            }
        }
    }
}

/// State reads, from the engine's full state.
fn read_state(
    engine: Arc<EnginePort>,
    shared: Arc<Shared>,
) -> BoxCloneService<zakura_state::ReadRequest, zakura_state::ReadResponse, zakura_state::BoxError>
{
    BoxCloneService::new(service_fn(move |request: zakura_state::ReadRequest| {
        let engine = engine.clone();
        let shared = shared.clone();
        async move {
            let zakura_state::ReadRequest::MissingBlockBodyMetadata { from, limit } = request
            else {
                panic!("unexpected read request: {request:?}");
            };
            let metadata = engine
                .missing_block_body_metadata(from, limit)
                .map_err(|error| format!("{error:?}"))?;
            Ok(zakura_state::ReadResponse::MissingBlockBodyMetadata(
                moved_anchor(
                    &shared,
                    from,
                    zakura_state::BlockSyncBodyMetadata {
                        anchor: metadata.anchor,
                        blocks: metadata.blocks,
                    },
                ),
            ))
        }
    }))
}

/// `metadata`, read from `from`, with its anchor moved if the case moves
/// the anchor of a query's later chunks.
fn moved_anchor(
    shared: &Shared,
    from: block::Height,
    mut metadata: zakura_state::BlockSyncBodyMetadata,
) -> zakura_state::BlockSyncBodyMetadata {
    if shared.anchor_moves.load(Ordering::SeqCst)
        && from.0 > zakura_state::constants::MAX_HEADER_SYNC_HEIGHT_RANGE
    {
        metadata.anchor = Frontier {
            height: metadata.anchor.height,
            hash: block::Hash([0xff; 32]),
        };
    }
    metadata
}

/// Counts `call` in flight until zakura-state answers it or the driver
/// drops it, as its deadline does.
async fn counted<T>(shared: &Shared, call: impl std::future::Future<Output = T>) -> T {
    /// Ends one call's count when dropped.
    struct InFlight<'a>(&'a AtomicUsize);

    impl Drop for InFlight<'_> {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    shared.in_flight.fetch_add(1, Ordering::SeqCst);
    let _in_flight = InFlight(&shared.in_flight);
    call.await
}

/// State reads, from zakura-state.
fn state_reads(
    state: &ProductionState,
    shared: Arc<Shared>,
) -> BoxCloneService<zakura_state::ReadRequest, zakura_state::ReadResponse, zakura_state::BoxError>
{
    let read_state = state.read_state.clone();
    BoxCloneService::new(service_fn(move |request: zakura_state::ReadRequest| {
        let read_state = read_state.clone();
        let shared = shared.clone();
        async move {
            let zakura_state::ReadRequest::MissingBlockBodyMetadata { from, .. } = request else {
                panic!("unexpected read request: {request:?}");
            };
            match counted(&shared, read_state.oneshot(request)).await? {
                zakura_state::ReadResponse::MissingBlockBodyMetadata(metadata) => {
                    Ok(zakura_state::ReadResponse::MissingBlockBodyMetadata(
                        moved_anchor(&shared, from, metadata),
                    ))
                }
                response => panic!("unexpected read response: {response:?}"),
            }
        }
    }))
}

/// State writes: block sync's records, to zakura-state. Each answer is a
/// record attempt's result, and an error is a refusal.
fn state_writes(
    state: &ProductionState,
    shared: Arc<Shared>,
) -> BoxCloneService<zakura_state::Request, zakura_state::Response, zakura_state::BoxError> {
    let writer = state.writer();
    BoxCloneService::new(service_fn(move |request: zakura_state::Request| {
        let writer = writer.clone();
        let shared = shared.clone();
        async move {
            assert!(
                matches!(
                    request,
                    zakura_state::Request::RecordHeaderChainBodyInvalid { .. }
                        | zakura_state::Request::RecordHeaderChainBodyUnavailable { .. }
                        | zakura_state::Request::RestartHeaderChainBodyAvailability { .. }
                        | zakura_state::Request::RetryHeaderChainBodyAvailability { .. }
                ),
                "unexpected write request: {request:?}"
            );
            let response = match counted(&shared, writer.oneshot(request)).await {
                Ok(response) => response,
                Err(error) => {
                    lock(&shared.records).push(RecordResult::Refused);
                    return Err(error);
                }
            };
            let result = match &response {
                zakura_state::Response::HeaderChainBodyInvalidRecorded(result)
                | zakura_state::Response::HeaderChainBodyUnavailableRecorded(result)
                | zakura_state::Response::HeaderChainBodyAvailabilityRestarted(result)
                | zakura_state::Response::HeaderChainBodyAvailabilityRetried(result) => result,
                response => panic!("unexpected write response: {response:?}"),
            };
            lock(&shared.records).push(RecordResult::of(result));
            Ok(response)
        }
    }))
}

/// State writes: invalid-body records, applied to the engine.
fn writer(
    engine: Arc<EnginePort>,
    shared: Arc<Shared>,
    authority: zakura_state::HeaderChainBodyEvidenceAuthority,
) -> BoxCloneService<zakura_state::Request, zakura_state::Response, zakura_state::BoxError> {
    BoxCloneService::new(service_fn(move |request: zakura_state::Request| {
        let engine = engine.clone();
        let shared = shared.clone();
        let authority = authority.clone();
        async move {
            let zakura_state::Request::RecordHeaderChainBodyInvalid { prepared } = request else {
                panic!("a case with verdicts records only invalid bodies: {request:?}");
            };
            // The sealed request hides its fields, so find the record and
            // version that seal to it.
            let current = engine.snapshot().state_version.get();
            let (expected_version, invalid) = lock(&shared.invalid)
                .iter()
                .flat_map(|invalid| {
                    (1..=current).map(move |version| (StateVersion::new(version), invalid))
                })
                .find(|(version, invalid)| {
                    authority.from_full_verifier(*version, (*invalid).clone()) == prepared
                })
                .map(|(version, invalid)| (version, invalid.clone()))
                .expect("the driver records only what block sync sent");
            let applied = engine
                .record_body_evidence_for_driver_test(record_request(
                    BlockSyncAction::RecordBodyInvalid {
                        expected_version,
                        invalid,
                    },
                ))
                .map_err(|error| format!("{error:?}"))?;
            lock(&shared.records).push(RecordResult::of(&applied.result));
            Ok(zakura_state::Response::HeaderChainBodyInvalidRecorded(
                applied.result,
            ))
        }
    }))
}

/// A verifier whose calls wait for the case's verdicts.
fn verifier(
    shared: Arc<Shared>,
) -> BoxCloneService<zakura_consensus::Request, block::Hash, zakura_consensus::BoxError> {
    BoxCloneService::new(service_fn(move |request: zakura_consensus::Request| {
        let shared = shared.clone();
        async move {
            let zakura_consensus::Request::Commit(block) = request else {
                panic!("unexpected consensus request: {request:?}");
            };
            let hash = block.hash();
            let (reply, verdict) = oneshot::channel();
            let receipt = shared.receipts.fetch_add(1, Ordering::SeqCst) + 1;
            lock(&shared.calls).push(hash);
            lock(&shared.waiting)
                .entry(hash)
                .or_default()
                .push_back((reply, receipt));
            verdict
                .await
                .unwrap_or_else(|_| Err("the case ended before a verdict".into()))
        }
    }))
}

/// The reply the real verifier gives for `verdict` on `block`.
fn verifier_reply(verdict: Verdict, block: &block::Block) -> Reply {
    use zakura_consensus::VerifyBlockError;
    use zakura_state::{CommitBlockError, ValidateContextError};

    let hash = block.hash();
    let router = |error: VerifyBlockError| -> Reply {
        Err(zakura_consensus::RouterError::from(error).into())
    };
    let commit = |error: CommitBlockError| router(VerifyBlockError::Commit(error));
    let context = |error: ValidateContextError| {
        commit(CommitBlockError::ValidateContextError(Box::new(error)))
    };
    match verdict {
        Verdict::Committed => Ok(hash),
        Verdict::Duplicate => router(VerifyBlockError::Block {
            source: zakura_consensus::BlockError::AlreadyInChain(
                hash,
                zakura_state::KnownBlock::BestChain,
            ),
        }),
        Verdict::PayloadMismatch(commitment) => {
            let (expected, actual) = ([1; 32], [2; 32]);
            context(ValidateContextError::InvalidBlockCommitment(
                match commitment {
                    Commitment::AuthDataRoot => {
                        block::CommitmentError::InvalidAuthDataRoot { expected, actual }
                    }
                    Commitment::ChainHistoryBlockTxAuthCommitment => {
                        block::CommitmentError::InvalidChainHistoryBlockTxAuthCommitment {
                            expected,
                            actual,
                        }
                    }
                    Commitment::FinalSaplingRoot => {
                        block::CommitmentError::InvalidFinalSaplingRoot { expected, actual }
                    }
                },
            ))
        }
        Verdict::ConsensusInvalid => router(VerifyBlockError::Subsidy(SubsidyError::NoCoinbase)),
        Verdict::Retryable(RetryKind::MissingContext) => {
            commit(CommitBlockError::MissingMinedParent)
        }
        Verdict::Retryable(RetryKind::Canceled) => {
            context(ValidateContextError::InvalidAncestorBlock(hash))
        }
        Verdict::Retryable(RetryKind::Storage) => commit(CommitBlockError::HeaderChainError {
            error: "storage unavailable".to_string(),
        }),
        Verdict::Retryable(RetryKind::VerifierUnavailable) | Verdict::Ambiguous => {
            Err("verifier unavailable".into())
        }
        Verdict::Retryable(RetryKind::ResourceExhausted) => {
            unreachable!("only the checkpoint verifier exhausts its resources")
        }
    }
}

/// Runs `case` on the real driver, and returns what block sync saw during
/// each step.
async fn run_driver(case: &Case) -> Vec<Observed> {
    let mut script = Script::new(case);
    let backend = Backend::for_case(case, &script).await;
    let mut run = DriverRun::spawn(case, &backend);
    // The driver waits on its channel before each step, as between steps.
    run.settle().await;
    let mut unadmitted: HashMap<block::Hash, VecDeque<BlockApplyToken>> = HashMap::new();
    let mut observed = Vec::new();
    for (step, _) in &case.steps {
        let mut seen = Observed::default();
        if let Step::Verdict(index, verdict) = step {
            let block = script.blocks[*index].block.clone();
            let (reply, receipt) = lock(&run.shared.waiting)
                .get_mut(&block.hash())
                .and_then(VecDeque::pop_front)
                .unwrap_or_else(|| panic!("{}: block {index} isn't admitted", case.name));
            if verdict.commits() {
                script
                    .engine
                    .commit_block_for_driver_test(block.clone(), receipt)
                    .expect("a case commits a body only after its parent");
            }
            reply
                .send(verifier_reply(*verdict, &block))
                .expect("the driver awaits its admitted applies");
        } else {
            run.shared.anchor_moves.store(
                matches!(step, Step::QueryAnchorMoves { .. }),
                Ordering::SeqCst,
            );
            // Every action goes out before the driver runs, so a burst
            // arrives together.
            for action in script.actions_in(step, &backend.view()) {
                match &action {
                    BlockSyncAction::SubmitBlock { token, block, .. } => unadmitted
                        .entry(block.hash())
                        .or_default()
                        .push_back(*token),
                    BlockSyncAction::RecordBodyInvalid { invalid, .. } => {
                        lock(&run.shared.invalid).push(invalid.clone())
                    }
                    _ => {}
                }
                run.actions
                    .try_send(action)
                    .expect("the action channel has room for every step");
            }
        }
        run.settle().await;
        run.observe(&mut seen, &mut unadmitted);
        observed.push(seen);
    }
    // The driver waits on its channel after the last step, with nothing
    // more to send.
    run.settle().await;
    let mut after = Observed::default();
    run.observe(&mut after, &mut unadmitted);
    assert_eq!(
        after,
        Observed::default(),
        "{}: after the last step",
        case.name
    );
    run.shut_down().await;
    observed
}

/// The block contract clamps limits with the driver's floors and ceiling,
/// and refreshes a stale invalid-body record as often as the driver.
#[test]
fn the_contract_uses_the_drivers_constants() {
    assert_eq!(MIN_APPLY_LIMIT, sync::MIN_CONCURRENCY_LIMIT);
    assert_eq!(
        MIN_CHECKPOINT_APPLY_LIMIT,
        sync::MIN_CHECKPOINT_CONCURRENCY_LIMIT
    );
    assert_eq!(
        MAX_CHECKPOINT_HEIGHT_GAP,
        zakura_consensus::MAX_CHECKPOINT_HEIGHT_GAP
    );
    assert_eq!(
        zakura_storage::block::BODY_INVALID_STALE_REFRESH_LIMIT,
        BODY_INVALID_STALE_REFRESH_LIMIT
    );
}

/// ST4.6: on every shared case, the real driver gives block sync what the
/// case states, and exactly what the controlled service gives it.
#[tokio::test]
async fn the_driver_and_the_controlled_service_agree_on_every_case() {
    for case in cases() {
        let driver = run_driver(&case).await;
        let controlled = run_controlled(&case);
        for (index, (step, seen)) in case.steps.iter().enumerate() {
            assert_eq!(
                &driver[index].summary(),
                seen,
                "{}: the driver at step {index}, {step:?}",
                case.name
            );
            assert_eq!(
                driver[index], controlled[index],
                "{}: the driver and the controlled service at step {index}, {step:?}",
                case.name
            );
        }
    }
}

/// A state call the driver drops, as its deadline does, ends its count, so
/// `settle` doesn't wait for it.
#[test]
fn a_dropped_state_call_ends_its_count() {
    use futures::FutureExt;

    let shared = Shared::default();
    let mut call = Box::pin(counted(&shared, std::future::pending::<()>()));
    assert!((&mut call).now_or_never().is_none());
    assert_eq!(shared.in_flight.load(Ordering::SeqCst), 1);
    drop(call);
    assert_eq!(shared.in_flight.load(Ordering::SeqCst), 0);
}
