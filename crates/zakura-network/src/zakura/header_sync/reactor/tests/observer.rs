//! The reactor reports port calls, request starts and retirements, and
//! refused offers to an installed [`observe::HeaderSyncObserver`].

use std::{
    cell::Cell,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex as StdMutex,
    },
};

use futures::{FutureExt, StreamExt};
use tokio::sync::Semaphore as TokioSemaphore;
use zakura_node_services::header_chain as port;

use super::*;
use crate::zakura::{
    testkit::{TraceCapture, TraceReader},
    CloseCause, FramedRecv,
};
use observe::{CallEnd, HeaderSyncObserver, ObserverSlot, PortCall, RequestRef, WaitCause};

/// Records every event in order.
#[derive(Debug, Default)]
struct Recorder(StdMutex<Vec<ReactorEvent>>);

impl HeaderSyncObserver for Recorder {
    fn observe(&self, event: ReactorEvent) {
        self.0
            .lock()
            .expect("no test panics while holding it")
            .push(event);
    }
}

impl Recorder {
    /// The events recorded since the last call.
    fn take(&self) -> Vec<ReactorEvent> {
        std::mem::take(&mut *self.0.lock().expect("no test panics while holding it"))
    }
}

/// Where a [`ScriptedPort`] call panics.
#[derive(Copy, Clone, Debug)]
enum PanicAt {
    /// In the port method, before it returns its future.
    Method,
    /// Inside the returned future, once it is polled.
    Future,
}

/// A port whose replies the test controls. Prepares and applies fail with a
/// local resource error, locator reads return a continuation locator at the
/// genesis anchor, and VCT context reads report a stale repair.
#[derive(Debug, Default)]
struct ScriptedPort {
    /// The port methods called, in order.
    calls: StdMutex<Vec<&'static str>>,
    /// When set, every prepare and apply waits for one permit before it answers.
    gate: Option<Arc<TokioSemaphore>>,
    /// Locator reads never answer.
    silent_locator: bool,
    /// Prepares and applies panic here.
    panic: Option<PanicAt>,
    /// Prepares the port answered.
    prepares: AtomicUsize,
}

impl ScriptedPort {
    /// Records `method`, then panics there if the port panics in its methods.
    fn enter(&self, method: &'static str) {
        self.calls
            .lock()
            .expect("no test panics while holding it")
            .push(method);
        if matches!(self.panic, Some(PanicAt::Method)) {
            panic!("the scripted port panics before returning its future");
        }
    }

    /// Waits for the gate, then panics if the port panics in its futures.
    async fn answer(&self) {
        if let Some(gate) = &self.gate {
            gate.acquire()
                .await
                .expect("the test never closes the gate")
                .forget();
        }
        if matches!(self.panic, Some(PanicAt::Future)) {
            panic!("the scripted port panics inside its future");
        }
    }
}

impl port::Port for ScriptedPort {
    fn continuation_locator(
        &self,
        _permit: port::LocatorReadPermit,
    ) -> port::HeaderChainFuture<
        '_,
        Result<Option<zakura_header_chain::HeaderLocator>, port::PortError>,
    > {
        self.calls
            .lock()
            .expect("no test panics while holding it")
            .push("continuation_locator");
        if self.silent_locator {
            return Box::pin(std::future::pending());
        }
        let anchor = zakura_header_chain::Frontier::new(
            block::Height(0),
            Network::new_regtest(Default::default()).genesis_hash(),
        );
        Box::pin(async move {
            Ok(Some(zakura_header_chain::HeaderLocator::for_continuation(
                anchor,
            )))
        })
    }

    fn vct_repair_context(
        &self,
        _owner: zakura_header_chain::BodyWorkOwner,
        _height: block::Height,
    ) -> port::HeaderChainFuture<'_, Result<port::VctRepairContextReply, port::PortError>> {
        Box::pin(async { Ok(port::VctRepairContextReply::Stale) })
    }

    fn acquire_header_path(
        &self,
        _request: port::AcquirePath,
    ) -> port::HeaderChainFuture<'_, Result<port::AcquirePathReply, port::PortError>> {
        Box::pin(async { Ok(port::AcquirePathReply::TargetNotRetained) })
    }

    fn read_header_path(
        &self,
        _path: port::RetainedHeaderPath,
        _request: port::ReadPath,
    ) -> port::HeaderChainFuture<'_, Result<port::ReadPathReply, port::PortError>> {
        Box::pin(async { Ok(port::ReadPathReply::Unavailable) })
    }

    fn release_header_path(
        &self,
        _path: port::RetainedHeaderPath,
    ) -> port::HeaderChainFuture<'_, Result<(), port::PortError>> {
        Box::pin(async { Ok(()) })
    }

    fn prepare_header_target(
        &self,
        request: port::PrepareHeaderTarget,
    ) -> port::HeaderChainFuture<'_, port::PrepareHeaderTargetReply> {
        self.enter("prepare_header_target");
        Box::pin(async move {
            self.answer().await;
            self.prepares.fetch_add(1, Ordering::SeqCst);
            Err(local_failure(request.owner))
        })
    }

    fn apply_header_target(
        &self,
        target: port::PreparedHeaderTarget,
    ) -> port::HeaderChainFuture<'_, port::ApplyHeaderTargetReply> {
        self.enter("apply_header_target");
        let owner = target.owner();
        Box::pin(async move {
            self.answer().await;
            Err(local_failure(owner))
        })
    }

    fn selected_chain_contains(
        &self,
        _hash: block::Hash,
    ) -> port::HeaderChainFuture<'_, Result<port::SelectedChainMembership, port::PortError>> {
        Box::pin(async { Ok(port::SelectedChainMembership::NotRetained) })
    }
}

/// A direct-port reactor on `port` with a committed genesis snapshot and,
/// when `recorder` is set, an installed observer.
fn observed_reactor(
    port: Arc<dyn port::Port>,
    recorder: Option<Arc<Recorder>>,
    trace: crate::zakura::ZakuraTrace,
) -> (HeaderSyncReactor, zakura_header_chain::EngineSnapshot) {
    let mut startup = startup(CancellationToken::new());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let snapshot = committed_snapshot(anchor);
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot.clone()));
    startup.committed_snapshots = Some(snapshots_rx);
    startup.header_chain_port = port;
    startup.use_direct_port();
    startup.trace = trace;
    startup.observer = recorder.map_or_else(ObserverSlot::default, |recorder| {
        ObserverSlot::new(recorder)
    });
    let (_, _, reactor) = build_header_sync_reactor(startup).expect("the observed reactor builds");
    (reactor, snapshot)
}

/// Connects `peer` as session `session_id`, returning its outbound frames.
fn connect(reactor: &mut HeaderSyncReactor, peer: &ZakuraPeerId, session_id: u64) -> FramedRecv {
    let (send, outbound) = framed_channel(8);
    reactor.handle_peer_connected(PeerSession::from_parts_with_connection(
        peer.clone(),
        session_id,
        send,
        CancellationToken::new(),
        CancellationToken::new(),
        CloseCause::new(),
    ));
    outbound
}

/// Waits for the next port completion and processes it, as the reactor loop does.
async fn complete_next(reactor: &mut HeaderSyncReactor) {
    let completion = reactor
        .pending_port_operations
        .next()
        .await
        .expect("a port call is pending");
    reactor.handle_port_completion(completion);
}

/// `peer`'s active request.
fn active(reactor: &HeaderSyncReactor, peer: &ZakuraPeerId) -> ActiveHeaderRequest {
    reactor
        .peer_work_queue
        .active(peer)
        .expect("the peer has an active request")
        .clone()
}

/// The identity the observer reports for `active`.
fn request_ref(active: &ActiveHeaderRequest) -> RequestRef {
    RequestRef {
        peer: active.peer.clone(),
        session_id: active.owner.session_id(),
        request_id: active.request_id.get(),
        owner: active.owner,
        target_tip_hash: active.target.status.selected_tip_hash,
    }
}

/// The `Retired` event for `active`, retired in its current phase.
fn retired(active: &ActiveHeaderRequest, reason: HeaderRequestTerminal) -> ReactorEvent {
    ReactorEvent::Retired {
        request: request_ref(active),
        phase: active.phase,
        purpose: active.purpose.clone(),
        reason,
    }
}

/// The apply call for `peer`'s active request, sealed as its own preparation.
fn apply_operation(reactor: &HeaderSyncReactor, peer: &ZakuraPeerId) -> HeaderPortOperation {
    let active = active(reactor, peer);
    let common_ancestor = active
        .common_ancestor
        .expect("the seeded request has a fixed common ancestor");
    let headers: Vec<_> = active
        .entries
        .iter()
        .map(|entry| entry.header.clone())
        .collect();
    let lease = zakura_header_chain::ValidationLease::new(
        common_ancestor,
        vec![zakura_header_chain::HeaderContextFact {
            frontier: common_ancestor,
            header: regtest_genesis_block().header.clone(),
        }],
        reactor.startup.network.clone(),
        [9; 32],
    );
    let rules = zakura_header_chain::HeaderRules::for_validation_lease(&lease)
        .expect("the authenticated regtest policy is valid");
    let batch = zakura_header_chain::prepare_headers(
        zakura_header_chain::HeaderBatchInput::new(&headers),
        lease.parent(),
        &rules,
        &zakura_header_chain::SystemClock,
    )
    .expect("the seeded header prepares");
    let target = port::PreparedHeaderTarget::from_insert(
        &port::AdapterKey::new(),
        Box::new(zakura_header_chain::InsertHeaders {
            owner: active.owner,
            source: active.source,
            parent_hash: common_ancestor.hash,
            target_tip_hash: active.target.status.selected_tip_hash,
            completion: zakura_header_chain::TargetCompletion::TargetComplete { common_ancestor },
            batch,
            aux: Vec::new(),
        }),
    );
    HeaderPortOperation::ApplyHeaderTarget {
        purpose: active.purpose,
        peer: active.peer,
        source: active.source,
        owner: active.owner,
        target,
    }
}

/// The prepare call for `peer`'s active request, with `entries` in place of
/// its staged entries and `purpose` in place of its own.
fn prepare_operation(
    reactor: &HeaderSyncReactor,
    peer: &ZakuraPeerId,
    purpose: HeaderTargetPurpose,
    entries: Vec<HeaderEntry>,
) -> HeaderPortOperation {
    let active = active(reactor, peer);
    let common_ancestor = active
        .common_ancestor
        .expect("the seeded request has a fixed common ancestor");
    HeaderPortOperation::PrepareHeaderTarget {
        purpose,
        peer: active.peer,
        source: active.source,
        owner: active.owner,
        common_ancestor,
        target: zakura_header_chain::Frontier::new(
            active.target.status.selected_tip_height,
            active.target.status.selected_tip_hash,
        ),
        completion: zakura_header_chain::TargetCompletion::TargetComplete { common_ancestor },
        entries,
    }
}

/// Every retirement reason, in declaration order.
const EVERY_REASON: [HeaderRequestTerminal; 20] = [
    HeaderRequestTerminal::TargetNotRetained,
    HeaderRequestTerminal::NoLocatorIntersection,
    HeaderRequestTerminal::HistoryPruned,
    HeaderRequestTerminal::TargetNotSelected,
    HeaderRequestTerminal::SubscriptionSuperseded,
    HeaderRequestTerminal::SubscriptionClosed,
    HeaderRequestTerminal::Disconnected,
    HeaderRequestTerminal::LocalError,
    HeaderRequestTerminal::MalformedResponse,
    HeaderRequestTerminal::ExcludedAuxiliaryInput,
    HeaderRequestTerminal::RepairObsolete,
    HeaderRequestTerminal::ResourceStalled,
    HeaderRequestTerminal::SendError,
    HeaderRequestTerminal::SessionReplaced,
    HeaderRequestTerminal::Shutdown,
    HeaderRequestTerminal::SnapshotObsolete,
    HeaderRequestTerminal::StagingRefused,
    HeaderRequestTerminal::TargetAdmitted,
    HeaderRequestTerminal::TargetRejected,
    HeaderRequestTerminal::TimedOut,
];

/// `reason`'s index in [`EVERY_REASON`]. A new reason fails to compile here
/// until it is listed.
fn reason_index(reason: HeaderRequestTerminal) -> usize {
    use HeaderRequestTerminal::*;
    match reason {
        TargetNotRetained => 0,
        NoLocatorIntersection => 1,
        HistoryPruned => 2,
        TargetNotSelected => 3,
        SubscriptionSuperseded => 4,
        SubscriptionClosed => 5,
        Disconnected => 6,
        LocalError => 7,
        MalformedResponse => 8,
        ExcludedAuxiliaryInput => 9,
        RepairObsolete => 10,
        ResourceStalled => 11,
        SendError => 12,
        SessionReplaced => 13,
        Shutdown => 14,
        SnapshotObsolete => 15,
        StagingRefused => 16,
        TargetAdmitted => 17,
        TargetRejected => 18,
        TimedOut => 19,
    }
}

/// Every reason reaches the observer with the retired request's exact
/// identity, including the five that the request-terminal trace skips.
#[test]
fn every_active_retirement_reason_reaches_the_observer() {
    for (index, reason) in EVERY_REASON.into_iter().enumerate() {
        assert_eq!(reason_index(reason), index, "{reason:?} is listed once");
        let recorder = Arc::new(Recorder::default());
        let (mut reactor, snapshot) = observed_reactor(
            Arc::new(ScriptedPort::default()),
            Some(recorder.clone()),
            crate::zakura::ZakuraTrace::noop(),
        );
        let peer = peer();
        let _outbound = connect(&mut reactor, &peer, 7);
        seed_applying_request(&mut reactor, &snapshot, peer.clone(), 7);
        let seeded = active(&reactor, &peer);
        recorder.take();

        reactor.retire_peer_work(&peer, reason);

        assert_eq!(
            recorder.take(),
            vec![ReactorEvent::Retired {
                request: RequestRef {
                    peer: peer.clone(),
                    session_id: 7,
                    request_id: 9,
                    owner: seeded.owner,
                    target_tip_hash: seeded.target.status.selected_tip_hash,
                },
                phase: HeaderTargetPhase::Applying,
                purpose: HeaderTargetPurpose::Normal,
                reason,
            }],
            "{reason:?}"
        );
    }
}

/// A committed snapshot that obsoletes an active request retires it without
/// going through `retire_peer_work`, and the observer still sees it.
#[test]
fn snapshot_retirement_is_observed() {
    let recorder = Arc::new(Recorder::default());
    let (mut reactor, snapshot) = observed_reactor(
        Arc::new(ScriptedPort::default()),
        Some(recorder.clone()),
        crate::zakura::ZakuraTrace::noop(),
    );
    let peer = peer();
    let _outbound = connect(&mut reactor, &peer, 7);
    seed_applying_request(&mut reactor, &snapshot, peer.clone(), 7);
    let seeded = active(&reactor, &peer);
    recorder.take();
    let mut newer = snapshot;
    newer.header_generation = newer
        .header_generation
        .checked_next()
        .expect("the fixture generation advances");

    reactor.retire_obsolete_work(&newer);

    assert_eq!(
        recorder.take(),
        vec![retired(&seeded, HeaderRequestTerminal::SnapshotObsolete)]
    );
    assert!(reactor.peer_work_queue.active(&peer).is_none());
}

/// A repair retry retires the repair's active request without going through
/// `retire_peer_work`, and the observer still sees it.
#[test]
fn repair_retry_retirement_is_observed() {
    let recorder = Arc::new(Recorder::default());
    let (mut reactor, snapshot) = observed_reactor(
        Arc::new(ScriptedPort::default()),
        Some(recorder.clone()),
        crate::zakura::ZakuraTrace::noop(),
    );
    let peer = peer();
    let _outbound = connect(&mut reactor, &peer, 7);
    let (source, owner, _) = super::timeouts::seed_vct_active_request(
        &mut reactor,
        &snapshot,
        peer.clone(),
        7,
        HeaderTargetPhase::Applying,
    );
    let seeded = active(&reactor, &peer);
    recorder.take();

    reactor.retry_vct_repair(
        owner
            .body_owner()
            .expect("the seeded repair has body authority"),
        VctRepairRetry::local(source, HeaderRequestTerminal::RepairObsolete),
    );

    let events = recorder.take();
    assert_eq!(
        events.first(),
        Some(&retired(&seeded, HeaderRequestTerminal::RepairObsolete))
    );
    assert!(
        matches!(
            seeded.purpose,
            HeaderTargetPurpose::SelectedAuxiliaryRepair { .. }
        ),
        "the seeded request is a repair"
    );
}

/// `Handled` follows every effect of the apply's callback.
#[tokio::test]
async fn handled_follows_the_callbacks_effects() {
    let recorder = Arc::new(Recorder::default());
    let (mut reactor, snapshot) = observed_reactor(
        Arc::new(ScriptedPort::default()),
        Some(recorder.clone()),
        crate::zakura::ZakuraTrace::noop(),
    );
    let peer = peer();
    let _outbound = connect(&mut reactor, &peer, 7);
    let (source, owner, _) = seed_applying_request(&mut reactor, &snapshot, peer.clone(), 7);
    let seeded = active(&reactor, &peer);
    recorder.take();

    assert!(reactor.dispatch_action(apply_operation(&reactor, &peer)));
    complete_next(&mut reactor).await;

    assert_eq!(
        recorder.take(),
        vec![
            ReactorEvent::Dispatched {
                call: 1,
                op: PortCall::Apply {
                    peer: peer.clone(),
                    source,
                    owner,
                },
            },
            ReactorEvent::CallEnded {
                call: 1,
                end: CallEnd::Replied,
            },
            retired(&seeded, HeaderRequestTerminal::TargetRejected),
            ReactorEvent::Handled {
                call: 1,
                panicked: false,
            },
        ]
    );
}

/// Forwards every event to its recorder except a port future's
/// `CallEnded`, which it drops, as a buggy observer might.
#[derive(Debug, Default)]
struct DropsCallEnds {
    recorder: Recorder,
}

impl HeaderSyncObserver for DropsCallEnds {
    fn observe(&self, event: ReactorEvent) {
        if !matches!(event, ReactorEvent::CallEnded { .. }) {
            self.recorder.observe(event);
        }
    }
}

/// The slot counts each event before the observer sees it, from the
/// reactor's own events and from inside a port future alike, so an event
/// the observer drops still shows in the count, and the count outlives the
/// reactor.
///
/// This fails if a port future's `CallEnded` skips the counted path.
#[tokio::test]
async fn the_slot_counts_events_the_observer_drops() {
    let observer = Arc::new(DropsCallEnds::default());
    let (mut reactor, snapshot) = observed_reactor(
        Arc::new(ScriptedPort::default()),
        None,
        crate::zakura::ZakuraTrace::noop(),
    );
    let slot = ObserverSlot::new(observer.clone());
    reactor.startup.observer = slot.clone();
    let peer = peer();
    let _outbound = connect(&mut reactor, &peer, 7);
    seed_applying_request(&mut reactor, &snapshot, peer.clone(), 7);
    let before = slot.emitted();
    observer.recorder.take();

    assert!(reactor.dispatch_action(apply_operation(&reactor, &peer)));
    complete_next(&mut reactor).await;

    let recorded = observer.recorder.take();
    assert!(
        matches!(
            recorded.first(),
            Some(ReactorEvent::Dispatched { call: 1, .. })
        ) && matches!(recorded.last(), Some(ReactorEvent::Handled { call: 1, .. })),
        "the call was dispatched and handled: {recorded:?}"
    );
    assert!(
        !recorded
            .iter()
            .any(|event| matches!(event, ReactorEvent::CallEnded { .. })),
        "the observer dropped the call's end"
    );
    drop(reactor);
    assert_eq!(
        slot.emitted() - before,
        recorded.len() as u64 + 1, // a test's handful of events fits u64
        "the count has the dropped `CallEnded` too"
    );
}

/// The events strictly between `CallEnded { call }` and `Handled { call }`.
fn bracket(events: &[ReactorEvent], call: u64) -> &[ReactorEvent] {
    let ended = events
        .iter()
        .position(|event| matches!(event, ReactorEvent::CallEnded { call: c, .. } if *c == call))
        .expect("the call ended");
    let handled = events
        .iter()
        .position(|event| matches!(event, ReactorEvent::Handled { call: c, .. } if *c == call))
        .expect("the call was handled");
    assert!(ended < handled, "a call ends before it is handled");
    &events[ended + 1..handled]
}

/// Replaces `peer`'s session 1 request, held at its `phase` port call, with
/// a session 2 request in the same phase, then lets the old call answer.
/// Asserts the old call's whole history and that its bracket is empty.
async fn assert_stale_call_leaves_the_replacement_alone(phase: HeaderTargetPhase) {
    let gate = Arc::new(TokioSemaphore::new(0));
    let recorder = Arc::new(Recorder::default());
    let (mut reactor, snapshot) = observed_reactor(
        Arc::new(ScriptedPort {
            gate: Some(gate.clone()),
            ..ScriptedPort::default()
        }),
        Some(recorder.clone()),
        crate::zakura::ZakuraTrace::noop(),
    );
    let peer = peer();
    let _old_outbound = connect(&mut reactor, &peer, 1);
    let (source, owner, _) = seed_applying_request(&mut reactor, &snapshot, peer.clone(), 1);
    reactor
        .peer_work_queue
        .active_mut(&peer)
        .expect("the old request is active")
        .phase = phase;
    let old = active(&reactor, &peer);
    let (operation, op) = if phase == HeaderTargetPhase::Applying {
        (
            apply_operation(&reactor, &peer),
            PortCall::Apply {
                peer: peer.clone(),
                source,
                owner,
            },
        )
    } else {
        (
            prepare_operation(
                &reactor,
                &peer,
                HeaderTargetPurpose::Normal,
                old.entries.clone(),
            ),
            PortCall::Prepare {
                peer: peer.clone(),
                source,
                owner,
            },
        )
    };
    recorder.take();
    assert!(reactor.dispatch_action(operation));

    let _new_outbound = connect(&mut reactor, &peer, 2);
    seed_applying_request(&mut reactor, &snapshot, peer.clone(), 2);
    reactor
        .peer_work_queue
        .active_mut(&peer)
        .expect("the replacement is active")
        .phase = phase;
    let replacement = active(&reactor, &peer);
    gate.add_permits(1);
    complete_next(&mut reactor).await;

    let events = recorder.take();
    assert_eq!(bracket(&events, 1), &[], "{phase:?}");
    assert_eq!(
        events,
        vec![
            ReactorEvent::Dispatched { call: 1, op },
            retired(&old, HeaderRequestTerminal::SessionReplaced),
            ReactorEvent::CallEnded {
                call: 1,
                end: CallEnd::Replied,
            },
            ReactorEvent::Handled {
                call: 1,
                panicked: false,
            },
        ],
        "{phase:?}"
    );
    assert_eq!(active(&reactor, &peer), replacement, "{phase:?}");
}

/// A stale apply for a replaced session completes while its replacement
/// applies. Its bracket retires nothing, and the replacement stays active.
#[tokio::test]
async fn stale_apply_bracket_leaves_the_replacement_alone() {
    assert_stale_call_leaves_the_replacement_alone(HeaderTargetPhase::Applying).await;
}

/// A stale prepare for a replaced session completes while its replacement
/// prepares. Its bracket retires nothing, and the replacement stays active.
#[tokio::test]
async fn stale_prepare_bracket_leaves_the_replacement_alone() {
    assert_stale_call_leaves_the_replacement_alone(HeaderTargetPhase::Preparing).await;
}

/// A locator read that never answers ends `TimedOut` at the request timeout.
#[tokio::test(start_paused = true)]
async fn timed_out_locator_ends_timed_out() {
    let recorder = Arc::new(Recorder::default());
    let (mut reactor, snapshot) = observed_reactor(
        Arc::new(ScriptedPort {
            silent_locator: true,
            ..ScriptedPort::default()
        }),
        Some(recorder.clone()),
        crate::zakura::ZakuraTrace::noop(),
    );
    let peer = peer();
    let _outbound = connect(&mut reactor, &peer, 0);
    let target = block::Hash([0x81; 32]);
    let scope = zakura_header_chain::HeaderWorkAuthority::for_target(&snapshot, target);
    recorder.take();

    let started = Instant::now();
    assert!(
        reactor.dispatch_action(HeaderPortOperation::QueryHeaderLocator {
            peer: peer.clone(),
            session_id: 0,
            target_tip_hash: target,
            scope,
        })
    );
    complete_next(&mut reactor).await;

    assert_eq!(started.elapsed(), reactor.startup.request_timeout);
    assert_eq!(
        recorder.take(),
        vec![
            ReactorEvent::Dispatched {
                call: 1,
                op: PortCall::Locator {
                    peer,
                    session_id: 0,
                    target_tip_hash: target,
                    scope,
                },
            },
            ReactorEvent::CallEnded {
                call: 1,
                end: CallEnd::TimedOut,
            },
            ReactorEvent::Handled {
                call: 1,
                panicked: false,
            },
        ]
    );
}

/// A prepare that the reactor answers itself ends `NotSubmitted`, and storage
/// never sees it. One names a repair that its completion contradicts, and one
/// has more entries than a transition allows.
#[tokio::test]
async fn synthetic_prepare_ends_not_submitted() {
    let recorder = Arc::new(Recorder::default());
    let port = Arc::new(ScriptedPort::default());
    let (mut reactor, snapshot) = observed_reactor(
        port.clone(),
        Some(recorder.clone()),
        crate::zakura::ZakuraTrace::noop(),
    );
    let peer = peer();
    let _outbound = connect(&mut reactor, &peer, 7);
    let (source, owner, _) = seed_applying_request(&mut reactor, &snapshot, peer.clone(), 7);
    let seeded = active(&reactor, &peer);
    let selected_target = zakura_header_chain::Frontier::new(
        seeded.target.status.selected_tip_height,
        seeded.target.status.selected_tip_hash,
    );
    let mismatched_repair = prepare_operation(
        &reactor,
        &peer,
        HeaderTargetPurpose::SelectedAuxiliaryRepair {
            selected_target,
            repair_generation: 11,
        },
        seeded.entries.clone(),
    );
    let oversized = prepare_operation(
        &reactor,
        &peer,
        HeaderTargetPurpose::Normal,
        vec![seeded.entries[0].clone(); zakura_header_chain::MAX_HEADERS_PER_TRANSITION_V1 + 1],
    );
    recorder.take();

    for (call, operation) in [(1, mismatched_repair), (2, oversized)] {
        assert!(reactor.dispatch_action(operation));
        complete_next(&mut reactor).await;
        assert_eq!(
            recorder.take(),
            vec![
                ReactorEvent::Dispatched {
                    call,
                    op: PortCall::Prepare {
                        peer: peer.clone(),
                        source,
                        owner,
                    },
                },
                ReactorEvent::CallEnded {
                    call,
                    end: CallEnd::NotSubmitted,
                },
                ReactorEvent::Handled {
                    call,
                    panicked: false,
                },
            ]
        );
    }
    assert!(port.calls.lock().expect("unpoisoned").is_empty());
    assert_eq!(port.prepares.load(Ordering::SeqCst), 0);
}

/// An apply that panics, either before its port method returns a future or
/// inside that future, ends `PortPanicked`, retires the matching owner, and
/// is handled as panicked.
#[tokio::test]
async fn port_panic_before_and_after_submission() {
    for panic in [PanicAt::Method, PanicAt::Future] {
        let recorder = Arc::new(Recorder::default());
        let (mut reactor, snapshot) = observed_reactor(
            Arc::new(ScriptedPort {
                panic: Some(panic),
                ..ScriptedPort::default()
            }),
            Some(recorder.clone()),
            crate::zakura::ZakuraTrace::noop(),
        );
        let peer = peer();
        let _outbound = connect(&mut reactor, &peer, 7);
        let (source, owner, _) = seed_applying_request(&mut reactor, &snapshot, peer.clone(), 7);
        let seeded = active(&reactor, &peer);
        recorder.take();

        assert!(reactor.dispatch_action(apply_operation(&reactor, &peer)));
        complete_next(&mut reactor).await;

        assert_eq!(
            recorder.take(),
            vec![
                ReactorEvent::Dispatched {
                    call: 1,
                    op: PortCall::Apply {
                        peer: peer.clone(),
                        source,
                        owner,
                    },
                },
                ReactorEvent::CallEnded {
                    call: 1,
                    end: CallEnd::PortPanicked,
                },
                retired(&seeded, HeaderRequestTerminal::LocalError),
                ReactorEvent::Handled {
                    call: 1,
                    panicked: true,
                },
            ],
            "{panic:?}"
        );
    }
}

/// Records every event, and panics once just after recording the first
/// `CallEnded`.
#[derive(Debug, Default)]
struct PanicsAfterCallEnded {
    recorder: Recorder,
    panicked: std::sync::atomic::AtomicBool,
}

impl HeaderSyncObserver for PanicsAfterCallEnded {
    fn observe(&self, event: ReactorEvent) {
        let ended = matches!(event, ReactorEvent::CallEnded { .. });
        self.recorder.observe(event);
        if ended && !self.panicked.swap(true, Ordering::SeqCst) {
            panic!("observer boom");
        }
    }
}

/// An observer that panics inside a port future, after recording the call's
/// end, unwinds the reactor rather than having the panic reported as the
/// port's. The call never gets a second `CallEnded` or a `Handled`.
#[tokio::test]
async fn observer_panic_in_a_port_future_unwinds_the_reactor() {
    let observer = Arc::new(PanicsAfterCallEnded::default());
    let (mut reactor, snapshot) = observed_reactor(
        Arc::new(ScriptedPort::default()),
        None,
        crate::zakura::ZakuraTrace::noop(),
    );
    reactor.startup.observer = ObserverSlot::new(observer.clone());
    let peer = peer();
    let _outbound = connect(&mut reactor, &peer, 7);
    let (source, owner, _) = seed_applying_request(&mut reactor, &snapshot, peer.clone(), 7);
    observer.recorder.take();

    assert!(reactor.dispatch_action(apply_operation(&reactor, &peer)));
    let polled = AssertUnwindSafe(reactor.pending_port_operations.next())
        .catch_unwind()
        .await;

    assert!(
        polled.is_err(),
        "the observer panic unwinds out of the reactor's poll"
    );
    assert_eq!(
        observer.recorder.take(),
        vec![
            ReactorEvent::Dispatched {
                call: 1,
                op: PortCall::Apply {
                    peer: peer.clone(),
                    source,
                    owner,
                },
            },
            ReactorEvent::CallEnded {
                call: 1,
                end: CallEnd::Replied,
            },
        ]
    );
}

/// An observer panic unwinds the reactor with the observer's own payload,
/// whether it panics on a `Replied` end inside the port future or on a
/// `PortPanicked` end after the future's catch.
#[tokio::test]
async fn observer_panic_unwinds_with_its_own_payload() {
    for panic in [None, Some(PanicAt::Future)] {
        let observer = Arc::new(PanicsAfterCallEnded::default());
        let (mut reactor, snapshot) = observed_reactor(
            Arc::new(ScriptedPort {
                panic,
                ..ScriptedPort::default()
            }),
            None,
            crate::zakura::ZakuraTrace::noop(),
        );
        reactor.startup.observer = ObserverSlot::new(observer.clone());
        let peer = peer();
        let _outbound = connect(&mut reactor, &peer, 7);
        seed_applying_request(&mut reactor, &snapshot, peer.clone(), 7);

        assert!(reactor.dispatch_action(apply_operation(&reactor, &peer)));
        let payload = AssertUnwindSafe(reactor.pending_port_operations.next())
            .catch_unwind()
            .await
            .err()
            .expect("the observer panic unwinds out of the reactor's poll");

        assert_eq!(
            payload.downcast_ref::<&str>(),
            Some(&"observer boom"),
            "{panic:?}"
        );
    }
}

/// An offer refused because staging is full, or because every locator permit
/// is taken, is reported as waiting.
#[tokio::test]
async fn offer_waiting_on_staging_full_and_locator_capacity() {
    let recorder = Arc::new(Recorder::default());
    let (mut reactor, snapshot) = observed_reactor(
        Arc::new(ScriptedPort::default()),
        Some(recorder.clone()),
        crate::zakura::ZakuraTrace::noop(),
    );
    for index in 0..zakura_header_chain::MAX_STAGED_TARGETS_V1 {
        let byte = u8::try_from(index).expect("the staged target cap fits u8");
        let filler =
            ZakuraPeerId::new(vec![byte; 32]).expect("the test peer ID has the required length");
        let target = block::Hash([byte; 32]);
        assert_eq!(
            reactor.peer_work_queue.stage(
                filler,
                AdvertisedHeaderTarget {
                    scope: zakura_header_chain::HeaderWorkAuthority::for_target(&snapshot, target),
                    session_id: 0,
                    status: super::status_offers::status_for(&snapshot, target),
                },
                PeerWorkPriority::HigherComparableWork,
            ),
            QueueWorkResult::NeedsLocator
        );
    }
    let held_back =
        ZakuraPeerId::new(vec![0x72; 32]).expect("the test peer ID has the required length");
    let _held_back_outbound = connect(&mut reactor, &held_back, 0);
    let target = block::Hash([0x42; 32]);
    recorder.take();

    reactor.handle_status(
        held_back.clone(),
        0,
        super::status_offers::status_for(&snapshot, target),
    );

    assert_eq!(
        recorder.take(),
        vec![ReactorEvent::OfferWaiting {
            peer: held_back,
            session_id: 0,
            target_tip_hash: target,
            cause: WaitCause::StagingFull,
        }]
    );

    let (mut reactor, snapshot) = observed_reactor(
        Arc::new(ScriptedPort::default()),
        Some(recorder.clone()),
        crate::zakura::ZakuraTrace::noop(),
    );
    let peer = peer();
    let _outbound = connect(&mut reactor, &peer, 0);
    let _permits: Vec<_> = (0..zakura_header_chain::MAX_STAGED_TARGETS_V1)
        .map(|_| {
            reactor
                .locator_capacity
                .clone()
                .try_acquire_owned()
                .expect("the test takes every locator permit")
        })
        .collect();
    let scope = zakura_header_chain::HeaderWorkAuthority::for_target(&snapshot, target);
    recorder.take();

    assert!(
        reactor.dispatch_action(HeaderPortOperation::QueryHeaderLocator {
            peer: peer.clone(),
            session_id: 0,
            target_tip_hash: target,
            scope,
        })
    );

    assert_eq!(
        recorder.take(),
        vec![ReactorEvent::OfferWaiting {
            peer,
            session_id: 0,
            target_tip_hash: target,
            cause: WaitCause::LocatorCapacity,
        }]
    );
    assert!(reactor.pending_port_operations.is_empty());
}

/// What one scripted history did, for comparing runs.
#[derive(Debug, Eq, PartialEq)]
struct Transcript {
    /// The port methods called, in order.
    port_calls: Vec<&'static str>,
    /// Every frame sent to the peer, in order.
    frames: Vec<String>,
    /// The work queue's final state.
    queue: String,
    /// The header-sync trace rows, without their envelope fields.
    trace: Vec<serde_json::Value>,
}

/// The header-sync trace rows in `reader`, without the time, node and process
/// fields the emitter adds to every row.
fn trace_rows(reader: &TraceReader) -> Vec<serde_json::Value> {
    reader
        .table(HEADER_SYNC_TABLE.table())
        .rows()
        .into_iter()
        .map(|row| {
            let mut row = row.clone();
            let fields = row.as_object_mut().expect("a trace row is an object");
            for envelope in ["ts", "node", "process_trace_id"] {
                fields.remove(envelope);
            }
            row
        })
        .collect()
}

/// Runs one history: a status, its locator read, the subscription's one page,
/// and a prepare that storage rejects. `recorder`, when set, observes it.
async fn scripted_history(recorder: Option<Arc<Recorder>>) -> Transcript {
    let mut capture =
        TraceCapture::for_test("observer_does_not_change_behaviour").expect("trace capture starts");
    let port = Arc::new(ScriptedPort::default());
    let (mut reactor, snapshot) = observed_reactor(
        port.clone(),
        recorder,
        crate::zakura::ZakuraTrace::new(capture.tracer(), "observer-test"),
    );
    let peer = peer();
    let (send, mut outbound) = framed_channel(8);
    reactor.handle_peer_connected(PeerSession::from_parts(
        peer.clone(),
        send,
        CancellationToken::new(),
    ));
    let anchor = snapshot.frontiers.finalized;
    let mut header = *regtest_genesis_block().header;
    header.previous_block_hash = anchor.hash;
    header.time += chrono::Duration::seconds(1);
    let header = Arc::new(header);
    let target = header.hash();
    reactor.handle_status(
        peer.clone(),
        0,
        Status {
            work_anchor_height: anchor.height,
            work_anchor_hash: anchor.hash,
            selected_tip_height: block::Height(1),
            selected_tip_hash: target,
            suffix_cumulative_work: zakura_chain::work::difficulty::U256::from(2_u8),
            oldest_retained_height: anchor.height,
            max_headers_per_response: 1,
            max_subscriptions: 1,
            max_message_bytes: 2_000_000,
            tree_aux_schema_mask: 0,
        },
    );
    reactor.schedule_cached_statuses();
    complete_next(&mut reactor).await;
    reactor.schedule_cached_statuses();
    let mut frames = Vec::new();
    while let Ok(frame) = outbound.try_recv() {
        frames.push(frame);
    }
    let open = update(
        frames
            .last()
            .expect("the locator opens a subscription")
            .clone(),
    );
    reactor.deliver_page(
        peer.clone(),
        0,
        Headers {
            subscription_id: open.subscription_id,
            initial_target_tip_hash: target,
            common_ancestor_height: anchor.height,
            common_ancestor_hash: anchor.hash,
            reaches_initial_target: true,
            tree_aux_schema: AuxSchema::None,
            entries: vec![HeaderEntry {
                header,
                body_size: 0,
                tree_aux: None,
            }],
        },
    );
    reactor.schedule_cached_statuses();
    complete_next(&mut reactor).await;
    reactor.schedule_cached_statuses();
    while let Ok(frame) = outbound.try_recv() {
        frames.push(frame);
    }
    let queue = format!("{:?}", reactor.peer_work_queue);
    drop(reactor);
    capture.flush().await;
    let trace = trace_rows(&capture.reader().expect("the trace reloads"));
    let _ = capture.finish().await.expect("trace capture finishes");
    let port_calls = port.calls.lock().expect("unpoisoned").clone();
    Transcript {
        port_calls,
        frames: frames.iter().map(|frame| format!("{frame:?}")).collect(),
        queue,
        trace,
    }
}

/// One scripted history behaves the same with and without an observer.
#[tokio::test(start_paused = true)]
async fn observer_does_not_change_behaviour() {
    let recorder = Arc::new(Recorder::default());
    let observed = scripted_history(Some(recorder.clone())).await;
    let unobserved = scripted_history(None).await;

    assert_eq!(observed, unobserved);
    assert_eq!(observed.frames.len(), 3, "a status, an Open and a Close");
    assert!(!observed.trace.is_empty());
    assert_eq!(
        observed.port_calls,
        ["continuation_locator", "prepare_header_target"]
    );
    let events = recorder.take();
    assert!(
        matches!(
            events.as_slice(),
            [
                ReactorEvent::Settled {
                    turn: 0,
                    last: false,
                    ..
                },
                ReactorEvent::Dispatched {
                    call: 1,
                    op: PortCall::Locator { .. }
                },
                ReactorEvent::CallEnded {
                    call: 1,
                    end: CallEnd::Replied
                },
                ReactorEvent::Started { .. },
                ReactorEvent::Handled {
                    call: 1,
                    panicked: false
                },
                ReactorEvent::Dispatched {
                    call: 2,
                    op: PortCall::Prepare { .. }
                },
                ReactorEvent::CallEnded {
                    call: 2,
                    end: CallEnd::Replied
                },
                ReactorEvent::Retired {
                    reason: HeaderRequestTerminal::TargetRejected,
                    ..
                },
                ReactorEvent::Handled {
                    call: 2,
                    panicked: false
                },
            ]
        ),
        "the history exercises every hook it passes: {events:#?}"
    );
}

/// Without an observer, `observe!` never evaluates its event.
#[test]
fn no_observer_evaluates_nothing() {
    let (mut reactor, _) = observed_reactor(
        Arc::new(ScriptedPort::default()),
        None,
        crate::zakura::ZakuraTrace::noop(),
    );
    let evaluated = Cell::new(0);
    let event = || {
        evaluated.set(evaluated.get() + 1);
        ReactorEvent::Handled {
            call: 1,
            panicked: false,
        }
    };

    observe!(reactor, event());
    assert_eq!(evaluated.get(), 0);

    let recorder = Arc::new(Recorder::default());
    reactor.startup.observer = ObserverSlot::new(recorder.clone());
    observe!(reactor, event());
    assert_eq!(evaluated.get(), 1);
    assert_eq!(
        recorder.take(),
        vec![ReactorEvent::Handled {
            call: 1,
            panicked: false,
        }]
    );
}

/// The `Misbehavior` events in `events`.
fn misbehaviors(events: Vec<ReactorEvent>) -> Vec<ReactorEvent> {
    events
        .into_iter()
        .filter(|event| matches!(event, ReactorEvent::Misbehavior { .. }))
        .collect()
}

/// A page from session `session_id` whose parent is no sent locator, so the
/// reactor's own response check reports it.
fn page_off_the_locator(
    reactor: &mut HeaderSyncReactor,
    snapshot: &zakura_header_chain::EngineSnapshot,
    peer: &ZakuraPeerId,
    owner: zakura_header_chain::HeaderSyncWorkOwner,
) -> Headers {
    let active = reactor
        .peer_work_queue
        .active_mut(peer)
        .expect("the seeded request is active");
    let wrong_ancestor = zakura_header_chain::Frontier::new(
        snapshot.frontiers.finalized.height,
        block::Hash([0x41; 32]),
    );
    let mut header = *active.entries[0].header;
    header.previous_block_hash = wrong_ancestor.hash;
    active.phase = HeaderTargetPhase::Receiving;
    active.common_ancestor = None;
    active.entries.clear();
    Headers {
        subscription_id: owner.request_id().get(),
        initial_target_tip_hash: owner.header_authority().branch.target_tip_hash,
        common_ancestor_height: wrong_ancestor.height,
        common_ancestor_hash: wrong_ancestor.hash,
        reaches_initial_target: false,
        tree_aux_schema: AuxSchema::None,
        entries: vec![HeaderEntry {
            header: Arc::new(header),
            body_size: 0,
            tree_aux: None,
        }],
    }
}

/// A fault in the live session's message or work names that session as both
/// the origin and the current session, whether the reactor's own check found
/// it or storage did.
#[test]
fn current_session_failures_name_their_origin() {
    let peer = peer();

    // The reactor's own check of a page from session 1.
    let recorder = Arc::new(Recorder::default());
    let (mut reactor, snapshot) = observed_reactor(
        Arc::new(ScriptedPort::default()),
        Some(recorder.clone()),
        crate::zakura::ZakuraTrace::noop(),
    );
    let _outbound = connect(&mut reactor, &peer, 1);
    let (_, owner, _) = seed_applying_request(&mut reactor, &snapshot, peer.clone(), 1);
    let page = page_off_the_locator(&mut reactor, &snapshot, &peer, owner);
    recorder.take();
    reactor.deliver_page(peer.clone(), 1, page);
    assert_eq!(
        misbehaviors(recorder.take()),
        vec![ReactorEvent::Misbehavior {
            peer: peer.clone(),
            reason: HeaderSyncMisbehavior::MalformedMessage,
            origin: Some(1),
            current: Some(1),
        }],
        "the reported page names its session"
    );

    // A typed prepare failure for session 1's work.
    let recorder = Arc::new(Recorder::default());
    let (mut reactor, snapshot) = observed_reactor(
        Arc::new(ScriptedPort::default()),
        Some(recorder.clone()),
        crate::zakura::ZakuraTrace::noop(),
    );
    let _outbound = connect(&mut reactor, &peer, 1);
    let (source, owner, _) = seed_applying_request(&mut reactor, &snapshot, peer.clone(), 1);
    reactor
        .peer_work_queue
        .active_mut(&peer)
        .expect("the seeded request is active")
        .phase = HeaderTargetPhase::Preparing;
    recorder.take();
    reactor.handle_header_target_prepared(
        peer.clone(),
        source,
        owner,
        HeaderTargetPreparationResult::Failed(invalid_header_failure(source, owner)),
    );
    assert_eq!(
        misbehaviors(recorder.take()),
        vec![ReactorEvent::Misbehavior {
            peer: peer.clone(),
            reason: HeaderSyncMisbehavior::InvalidHeader,
            origin: Some(1),
            current: Some(1),
        }],
        "the typed failure names its owner's session"
    );
}

/// A prepare for session 1 that fails after session 2 replaced it is
/// discarded before any dispatch, so it reports no misbehavior.
#[test]
fn stale_prepare_emits_no_misbehavior() {
    let recorder = Arc::new(Recorder::default());
    let (mut reactor, snapshot) = observed_reactor(
        Arc::new(ScriptedPort::default()),
        Some(recorder.clone()),
        crate::zakura::ZakuraTrace::noop(),
    );
    let peer = peer();
    let _old_outbound = connect(&mut reactor, &peer, 1);
    let (source, old_owner, _) = seed_applying_request(&mut reactor, &snapshot, peer.clone(), 1);
    let _new_outbound = connect(&mut reactor, &peer, 2);
    seed_applying_request(&mut reactor, &snapshot, peer.clone(), 2);
    reactor
        .peer_work_queue
        .active_mut(&peer)
        .expect("the replacement is active")
        .phase = HeaderTargetPhase::Preparing;
    let replacement = active(&reactor, &peer);
    recorder.take();

    reactor.handle_header_target_prepared(
        peer.clone(),
        source,
        old_owner,
        HeaderTargetPreparationResult::Failed(invalid_header_failure(source, old_owner)),
    );
    assert_eq!(misbehaviors(recorder.take()), vec![]);
    assert_eq!(active(&reactor, &peer), replacement);
}

/// A dispatch for session 1's work while session 2 is current reports both
/// sessions, so the harness never charges the replacement.
#[test]
fn dispatch_reports_distinct_origin_and_current() {
    let recorder = Arc::new(Recorder::default());
    let (mut reactor, snapshot) = observed_reactor(
        Arc::new(ScriptedPort::default()),
        Some(recorder.clone()),
        crate::zakura::ZakuraTrace::noop(),
    );
    let peer = peer();
    let _old_outbound = connect(&mut reactor, &peer, 1);
    let (_, old_owner, _) = seed_applying_request(&mut reactor, &snapshot, peer.clone(), 1);
    let _new_outbound = connect(&mut reactor, &peer, 2);
    recorder.take();

    reactor.dispatch_misbehavior(
        peer.clone(),
        HeaderSyncMisbehavior::InvalidHeader,
        Some(old_owner),
        None,
    );
    assert_eq!(
        misbehaviors(recorder.take()),
        vec![ReactorEvent::Misbehavior {
            peer,
            reason: HeaderSyncMisbehavior::InvalidHeader,
            origin: Some(1),
            current: Some(2),
        }]
    );
}

/// Reports the reactor's state as it stands, and returns the snapshot it
/// reported.
fn settle(reactor: &HeaderSyncReactor, recorder: &Recorder) -> observe::ReactorSnapshot {
    recorder.take();
    reactor.settled(false);
    match recorder.take().as_slice() {
        [ReactorEvent::Settled { snapshot, .. }] => (**snapshot).clone(),
        other => panic!("one Settled, got {other:?}"),
    }
}

/// The test peer with ID bytes `byte`.
fn peer_of(byte: u8) -> ZakuraPeerId {
    ZakuraPeerId::new(vec![byte; 32]).expect("32 bytes is a valid peer ID")
}

/// `peer` offers target `target` on `session_id`, and the reactor stages it
/// and dispatches its locator read, which a silent port never answers.
fn offer(
    reactor: &mut HeaderSyncReactor,
    snapshot: &zakura_header_chain::EngineSnapshot,
    peer: &ZakuraPeerId,
    session_id: u64,
    target: block::Hash,
) {
    reactor.handle_status(
        peer.clone(),
        session_id,
        super::status_offers::status_for(snapshot, target),
    );
    reactor.schedule_cached_statuses();
}

/// A reactor on a port whose locator reads never answer, observed by
/// `recorder`.
fn silent_reactor(
    recorder: &Arc<Recorder>,
) -> (HeaderSyncReactor, zakura_header_chain::EngineSnapshot) {
    observed_reactor(
        Arc::new(ScriptedPort {
            silent_locator: true,
            ..ScriptedPort::default()
        }),
        Some(recorder.clone()),
        crate::zakura::ZakuraTrace::noop(),
    )
}

/// The awaiting entry `peer`'s offer of `target` on `session_id` makes.
fn awaiting(
    peer: &ZakuraPeerId,
    session_id: u64,
    target: block::Hash,
    snapshot: &zakura_header_chain::EngineSnapshot,
) -> observe::WorkEntry {
    observe::WorkEntry {
        peer: peer.clone(),
        session_id,
        target_tip_hash: target,
        phase: observe::WorkPhase::AwaitingLocator {
            scope: zakura_header_chain::HeaderWorkAuthority::for_target(snapshot, target),
        },
    }
}

/// A settled snapshot lists a cached offer waiting for its locator and
/// another peer's active request, each with its identity and phase, with
/// both peers' sessions, the locator read in flight and its unhandled call.
///
/// This fails if the snapshot leaves out work that waits for its locator.
#[test]
fn the_snapshot_lists_awaiting_and_active_work() {
    let recorder = Arc::new(Recorder::default());
    let (mut reactor, snapshot) = silent_reactor(&recorder);
    let (waiting, active_peer) = (peer_of(1), peer_of(2));
    let _waiting = connect(&mut reactor, &waiting, 3);
    let _active = connect(&mut reactor, &active_peer, 7);
    let target = block::Hash([0x78; 32]);
    offer(&mut reactor, &snapshot, &waiting, 3, target);
    let (_, owner, _) = seed_applying_request(&mut reactor, &snapshot, active_peer.clone(), 7);
    assert!(
        reactor.peer_work_queue.awaiting_target(&waiting).is_some()
            && reactor.peer_work_queue.active(&active_peer).is_some(),
        "one offer waits for its locator and one request is active"
    );

    let settled = settle(&reactor, &recorder);
    let seeded = active(&reactor, &active_peer);
    assert_eq!(
        settled.work,
        vec![
            awaiting(&waiting, 3, target, &snapshot),
            observe::WorkEntry {
                peer: active_peer.clone(),
                session_id: 7,
                target_tip_hash: seeded.target.status.selected_tip_hash,
                phase: observe::WorkPhase::Active {
                    request_id: 9,
                    owner,
                    purpose: HeaderTargetPurpose::Normal,
                    phase: HeaderTargetPhase::Applying,
                    held: 1,
                    cursor: None,
                },
            },
        ]
    );
    assert_eq!(
        settled
            .sessions
            .iter()
            .map(|session| (session.peer.clone(), session.session_id))
            .collect::<Vec<_>>(),
        [(waiting.clone(), 3), (active_peer, 7)]
    );
    assert_eq!(settled.pending_locators, [waiting]);
    assert_eq!(settled.unhandled_calls, [1]);
    assert_eq!(
        settled.adopted,
        Some(observe::AdoptedSnapshot {
            header_generation: snapshot.header_generation.get(),
            finalized: snapshot.frontiers.finalized,
            header_best: snapshot.frontiers.header_best,
        })
    );
}

/// An obsolete committed snapshot drops a target still waiting for its
/// locator without any retirement event, and the next snapshot shows it
/// gone, since it reads the queue itself.
///
/// This fails if the snapshot follows the reactor's retirement events.
#[test]
fn unstarted_removal_shows_without_a_retirement_event() {
    let recorder = Arc::new(Recorder::default());
    let (mut reactor, snapshot) = silent_reactor(&recorder);
    let peer = peer();
    let _outbound = connect(&mut reactor, &peer, 3);
    let target = block::Hash([0x78; 32]);
    offer(&mut reactor, &snapshot, &peer, 3, target);
    assert_eq!(
        settle(&reactor, &recorder).work,
        [awaiting(&peer, 3, target, &snapshot)],
        "the target waits for its locator"
    );

    let mut obsolete = snapshot.clone();
    obsolete.header_generation = snapshot.header_generation.checked_next().unwrap();
    reactor.retire_obsolete_work(&obsolete);
    assert!(
        !recorder
            .take()
            .iter()
            .any(|event| matches!(event, ReactorEvent::Retired { .. })),
        "nothing reports the removal"
    );
    assert!(reactor.peer_work_queue.awaiting_target(&peer).is_none());
    assert_eq!(settle(&reactor, &recorder).work, []);
}

/// Each way the queue drops a peer's work leaves the next snapshot without
/// it: `remove` through `retire_peer_work`, `remove_all` through
/// `remove_unstarted` and `retire_obsolete_unstarted`,
/// `retire_obsolete_active`, and `remove_owner`. Each case first shows the
/// work listed, then that its path ran.
///
/// This fails if any removal path leaves the queue's entry in place.
#[test]
fn each_removal_path_empties_the_snapshot() {
    let target = block::Hash([0x78; 32]);
    /// `snapshot` at the next header generation, which obsoletes its work.
    fn newer(
        snapshot: &zakura_header_chain::EngineSnapshot,
    ) -> zakura_header_chain::EngineSnapshot {
        let mut newer = snapshot.clone();
        newer.header_generation = snapshot.header_generation.checked_next().unwrap();
        newer
    }
    type Path = fn(&mut HeaderSyncReactor, &zakura_header_chain::EngineSnapshot, &ZakuraPeerId);
    let unstarted: [(&str, Path); 2] = [
        ("remove_unstarted", |reactor, _, peer| {
            reactor.defer_ordinary_target(peer)
        }),
        ("retire_obsolete_unstarted", |reactor, snapshot, _| {
            reactor.retire_obsolete_work(&newer(snapshot))
        }),
    ];
    for (path, remove) in unstarted {
        let recorder = Arc::new(Recorder::default());
        let (mut reactor, snapshot) = silent_reactor(&recorder);
        let peer = peer();
        let _outbound = connect(&mut reactor, &peer, 3);
        offer(&mut reactor, &snapshot, &peer, 3, target);
        assert_eq!(settle(&reactor, &recorder).work.len(), 1, "{path}");
        remove(&mut reactor, &snapshot, &peer);
        assert!(
            reactor.peer_work_queue.awaiting_target(&peer).is_none(),
            "{path} ran"
        );
        assert_eq!(settle(&reactor, &recorder).work, [], "{path}");
    }

    let active: [(&str, Path); 3] = [
        ("remove", |reactor, _, peer| {
            reactor.retire_peer_work(peer, HeaderRequestTerminal::Disconnected)
        }),
        ("retire_obsolete_active", |reactor, snapshot, _| {
            reactor.retire_obsolete_work(&newer(snapshot))
        }),
        ("remove_owner", |reactor, _, peer| {
            let owner = active(reactor, peer).owner;
            assert!(reactor.peer_work_queue.remove_owner(owner).is_some());
        }),
    ];
    for (path, remove) in active {
        let recorder = Arc::new(Recorder::default());
        let (mut reactor, snapshot) = silent_reactor(&recorder);
        let peer = peer();
        let _outbound = connect(&mut reactor, &peer, 7);
        seed_applying_request(&mut reactor, &snapshot, peer.clone(), 7);
        assert_eq!(settle(&reactor, &recorder).work.len(), 1, "{path}");
        remove(&mut reactor, &snapshot, &peer);
        assert!(
            reactor.peer_work_queue.active(&peer).is_none(),
            "{path} ran"
        );
        assert_eq!(settle(&reactor, &recorder).work, [], "{path}");
    }
}

/// The snapshot shows the committed snapshot the reactor adopted, not the
/// watch's latest: one published while the reactor isn't running a turn
/// shows only once the reactor reads it.
///
/// This fails if the snapshot reads the watch.
#[test]
fn the_adopted_state_is_what_the_reactor_schedules_from() {
    let recorder = Arc::new(Recorder::default());
    let mut startup = startup(CancellationToken::new());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let snapshot = committed_snapshot(anchor);
    let (snapshots, snapshots_rx) = watch::channel(Some(snapshot.clone()));
    startup.committed_snapshots = Some(snapshots_rx);
    startup.use_direct_port();
    startup.observer = ObserverSlot::new(recorder.clone());
    let (_, _, mut reactor) = build_header_sync_reactor(startup).expect("the reactor builds");
    let generation = |settled: observe::ReactorSnapshot| {
        settled
            .adopted
            .expect("the reactor adopted a snapshot")
            .header_generation
    };
    let g = snapshot.header_generation.get();
    assert_eq!(generation(settle(&reactor, &recorder)), g);

    let mut next = snapshot.clone();
    next.state_version = snapshot.state_version.checked_next().unwrap();
    next.header_generation = snapshot.header_generation.checked_next().unwrap();
    snapshots.send(Some(next)).unwrap();
    assert_eq!(
        generation(settle(&reactor, &recorder)),
        g,
        "the published snapshot isn't the reactor's yet"
    );
    reactor.refresh_committed_snapshot();
    assert_eq!(generation(settle(&reactor, &recorder)), g + 1);
}

/// Every turn's markers, in order, from a running reactor: each turn
/// starts with its `Resumed`, numbered one past the last `Settled`, and
/// ends with a `Settled` of its number. Only a port future's `CallEnded`,
/// reported while the loop polls its futures, sits between a `Settled` and
/// the next `Resumed`, which is then the completion's.
fn assert_turns(events: &[ReactorEvent]) -> Vec<observe::ResumeArm> {
    let mut arms = Vec::new();
    let mut settled = None;
    let mut resumed = None;
    let mut call_ended = false;
    let mut last = false;
    for event in events {
        assert!(!last, "nothing follows the final Settled: {event:?}");
        match event {
            ReactorEvent::Settled {
                turn, last: end, ..
            } => {
                match (settled, resumed) {
                    (None, None) => assert_eq!(*turn, 0, "the build settles turn 0"),
                    (_, Some(open)) => assert_eq!(*turn, open, "a turn settles its own number"),
                    _ => panic!("Settled{{turn: {turn}}} with no Resumed before it"),
                }
                settled = Some(*turn);
                resumed = None;
                last = *end;
            }
            ReactorEvent::Resumed { turn, arm } => {
                let previous = settled.expect("a Settled comes first");
                assert_eq!(resumed, None, "two Resumed in one turn");
                assert_eq!(*turn, previous + 1, "Resumed starts the next turn");
                if std::mem::take(&mut call_ended) {
                    assert_eq!(*arm, observe::ResumeArm::PortCompletion);
                }
                resumed = Some(*turn);
                arms.push(*arm);
            }
            ReactorEvent::CallEnded { .. } if resumed.is_none() => call_ended = true,
            other => assert!(
                resumed.is_some(),
                "{other:?} came before its turn's Resumed"
            ),
        }
    }
    assert!(last, "the loop ends with its final Settled");
    arms
}

/// Lets a spawned reactor run until it waits.
async fn idle() {
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
}

/// A running reactor reports a `Resumed` first in every turn: its first,
/// each of the loop's ten arms, and the wait that follows a fatal storage
/// operation, before any other event of the turn.
///
/// This fails if an arm reports its `Resumed` after an event of its turn.
#[tokio::test(start_paused = true)]
async fn resume_markers_lead_each_turn() {
    use observe::ResumeArm as Arm;

    let recorder = Arc::new(Recorder::default());
    let gate = Arc::new(TokioSemaphore::new(0));
    let shutdown = CancellationToken::new();
    let mut startup = startup(shutdown.clone());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let snapshot = committed_snapshot(anchor);
    let (snapshots, snapshots_rx) = watch::channel(Some(snapshot.clone()));
    let (repairs, repairs_rx) = watch::channel(Default::default());
    startup.committed_snapshots = Some(snapshots_rx);
    startup.vct_root_repairs = Some(repairs_rx);
    startup.header_chain_port = Arc::new(ScriptedPort {
        gate: Some(gate.clone()),
        ..ScriptedPort::default()
    });
    startup.use_direct_port();
    startup.observer = ObserverSlot::new(recorder.clone());
    let (handle, _actions, mut reactor) =
        build_header_sync_reactor(startup).expect("the reactor builds");
    let peer = peer();
    let _outbound = connect(&mut reactor, &peer, 7);
    seed_applying_request(&mut reactor, &snapshot, peer.clone(), 7);
    // An apply the gate holds, and a waiter for a locator permit, which is
    // free, so the first wait ends at the permit arm.
    assert!(reactor.dispatch_action(apply_operation(&reactor, &peer)));
    reactor.locator_capacity_waiting = true;
    // The test's own setup ran outside any turn, so it settles it.
    recorder.take();
    reactor.settled(false);
    let running = tokio::spawn(reactor.run());
    idle().await;

    // The apply answers, its retirement releases capacity, and two queued
    // events leave the next turn to yield.
    gate.add_permits(1);
    idle().await;
    for _ in 0..2 {
        handle
            .try_send(Event::Status {
                peer: peer.clone(),
                session_id: 7,
                status: super::status_offers::status_for(&snapshot, block::Hash([0x78; 32])),
            })
            .expect("the event queue has room");
    }
    idle().await;
    let (send, _second) = framed_channel(8);
    handle
        .send_lifecycle(Event::PeerConnected(
            PeerSession::from_parts_with_connection(
                peer_of(2),
                9,
                send,
                CancellationToken::new(),
                CancellationToken::new(),
                CloseCause::new(),
            ),
        ))
        .expect("the lifecycle queue has room");
    idle().await;
    let mut next = snapshot.clone();
    next.state_version = snapshot.state_version.checked_next().unwrap();
    snapshots.send(Some(next)).unwrap();
    idle().await;
    repairs
        .send(zakura_header_chain::VctRootRepairStatus {
            state: zakura_header_chain::VctRootRepairState::Unavailable {
                height: block::Height(1),
            },
            generation: 7,
        })
        .unwrap();
    idle().await;
    tokio::time::advance(std::time::Duration::from_secs(60)).await;
    idle().await;
    shutdown.cancel();
    running.await.expect("the reactor stops");
    let mut arms = assert_turns(&recorder.take());

    // A fatal storage operation parks the loop on shutdown alone.
    let shutdown = CancellationToken::new();
    let (mut reactor, snapshot) = observed_reactor(
        Arc::new(ScriptedPort::default()),
        Some(recorder.clone()),
        crate::zakura::ZakuraTrace::noop(),
    );
    reactor.startup.shutdown = shutdown.clone();
    let (_, owner, _) = seed_applying_request(&mut reactor, &snapshot, peer.clone(), 7);
    let now = Instant::now();
    reactor.vct_local_operation = Some(VctLocalOperation {
        phase: HeaderTargetPhase::Applying,
        owner,
        repair_generation: 0,
        target: snapshot.frontiers.header_best,
        started_at: now,
        deadline: now,
        fatal_sent: true,
    });
    let running = tokio::spawn(reactor.run());
    idle().await;
    shutdown.cancel();
    running.await.expect("the reactor stops");
    arms.extend(assert_turns(&recorder.take()));

    for arm in [
        Arm::Start,
        Arm::FatalWait,
        Arm::Shutdown,
        Arm::PortCompletion,
        Arm::LocatorPermit,
        Arm::CapacityReleased,
        Arm::Maintenance,
        Arm::Lifecycle,
        Arm::Event,
        Arm::Yield,
        Arm::CommittedSnapshot,
        Arm::VctRepairs,
    ] {
        assert!(arms.contains(&arm), "{arm:?} never resumed: {arms:?}");
    }
}

/// A reactor with no work that shuts down reports its final `Settled`,
/// though its snapshot equals the one before it.
///
/// This fails if an unchanged final snapshot isn't reported.
#[tokio::test(start_paused = true)]
async fn an_idle_shutdown_keeps_its_final_marker() {
    let recorder = Arc::new(Recorder::default());
    let shutdown = CancellationToken::new();
    let (mut reactor, _) = observed_reactor(
        Arc::new(ScriptedPort::default()),
        Some(recorder.clone()),
        crate::zakura::ZakuraTrace::noop(),
    );
    reactor.startup.shutdown = shutdown.clone();
    let running = tokio::spawn(reactor.run());
    idle().await;
    shutdown.cancel();
    running.await.expect("the reactor stops");

    let events = recorder.take();
    assert_turns(&events);
    let settled: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            ReactorEvent::Settled { last, snapshot, .. } => Some((*last, snapshot)),
            _ => None,
        })
        .collect();
    let [.., (false, before), (true, last)] = settled.as_slice() else {
        panic!("a settled turn, then the final Settled: {events:?}");
    };
    assert_eq!(before, last, "nothing changed at shutdown");
    assert_eq!(
        ***last,
        observe::ReactorSnapshot {
            adopted: last.adopted,
            ..Default::default()
        },
        "the reactor holds nothing but its adopted snapshot"
    );
}
