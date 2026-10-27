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
    startup.observer = ObserverSlot(recorder.map(|recorder| recorder as _));
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
    reactor.startup.observer = ObserverSlot(Some(observer.clone()));
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
        reactor.startup.observer = ObserverSlot(Some(observer.clone()));
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
    reactor.startup.observer = ObserverSlot(Some(recorder.clone()));
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
