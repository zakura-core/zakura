//! The controlled header port: submission, enumeration, completion,
//! cancellation, timeout gates, lease ends, records and shutdown
//! (storage.md ST3.1 to ST3.4 and ST3.6).

use std::{
    future::Future,
    pin::pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use zakura_chain::block;
use zakura_header_chain::{ErrorCategory, Frontier, HeaderWorkAuthority, SourceId};
use zakura_node_services::header_chain::{
    AcquirePath, AcquirePathReply, AdapterKey, ApplyHeaderTargetOutcome, LocatorReadPermit, Port,
    PortError, PreparedHeaderTarget, ReadPath, ReadPathReply, RetainedHeaderPath,
    SelectedChainMembership,
};
use zakura_rt::sync::Semaphore;
use zakura_storage::header::{
    controlled::{CompleteError, Completion, ControlledPort, OpId, StorageEvent},
    evidence::{LeaseKey, RequestIdentity, ResultIdentity, SignalClass},
    fixture::HeaderFixture,
    lease::{LeaseEnd, MAX_RETAINED_PATH_LEASES},
    Fault, HeaderOp, Outcome, Release,
};

/// The engine outcome.
const ENGINE: Outcome = Outcome::Engine;
/// The `timeout` fault.
const TIMEOUT: Outcome = Outcome::Fault(Fault::Timeout);
/// The `unavailable` fault.
const UNAVAILABLE: Outcome = Outcome::Fault(Fault::Unavailable);
/// The `local-resource` fault.
const LOCAL_RESOURCE: Outcome = Outcome::Fault(Fault::LocalResource);

/// Counts its wakes.
#[derive(Default)]
struct Wakes(AtomicUsize);

impl Wake for Wakes {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl Wakes {
    /// How many times it was woken.
    fn count(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

/// Polls `future` once with `waker`.
fn poll<F: Future + Unpin>(future: &mut F, waker: &Waker) -> Poll<F::Output> {
    pin!(future).poll(&mut Context::from_waker(waker))
}

/// Polls `future` once, as a submission, and checks it waits.
fn submit<F: Future + Unpin>(future: &mut F) {
    assert!(
        poll(future, Waker::noop()).is_pending(),
        "a controlled call waits for its completion"
    );
}

/// Polls `future` once and returns its output.
fn ready<F: Future>(future: F) -> F::Output {
    match pin!(future).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("expected the future to be ready"),
    }
}

/// A controlled port whose engine holds `count` headers on the anchor,
/// admitted through the engine port directly. Returns the port and the
/// headers' frontiers.
fn chain(count: u32) -> (ControlledPort, Vec<Frontier>) {
    let fixture = HeaderFixture::regtest();
    let port = ControlledPort::new(&fixture);
    let anchor = fixture.anchor();
    let headers = fixture.extend(anchor, count, 0);
    let prepared = prepared(&port, anchor, &headers, 1);
    assert_eq!(
        ready(port.engine().apply_header_target(prepared)).expect("fixture headers apply"),
        ApplyHeaderTargetOutcome::Applied
    );
    let frontiers = (1..=count)
        .zip(&headers)
        .map(|(height, header)| Frontier::new(block::Height(height), header.hash()))
        .collect();
    (port, frontiers)
}

/// `headers` on `parent`, prepared by the engine port directly.
fn prepared(
    port: &ControlledPort,
    parent: Frontier,
    headers: &[Arc<block::Header>],
    request_id: u64,
) -> PreparedHeaderTarget {
    let request = HeaderFixture::regtest().target_request(
        &port.engine().snapshot(),
        parent,
        headers,
        request_id,
    );
    ready(port.engine().prepare_header_target(request)).expect("fixture headers prepare")
}

/// A locator permit from `semaphore`.
fn permit(semaphore: &Arc<Semaphore>) -> LocatorReadPermit {
    LocatorReadPermit::new(
        semaphore
            .clone()
            .try_acquire_owned()
            .expect("the semaphore has a permit"),
    )
}

/// A path handle another adapter issued.
fn foreign_path(port: &ControlledPort, target: Frontier) -> RetainedHeaderPath {
    RetainedHeaderPath::from_adapter(
        &AdapterKey::new(),
        1,
        SourceId::from_digest([1; 32]),
        1,
        HeaderFixture::regtest().anchor(),
        target,
        HeaderWorkAuthority::for_target(&port.engine().snapshot(), target.hash),
    )
}

/// The pending operations' IDs, operations and enabled outcomes.
fn listed(port: &ControlledPort) -> Vec<(u64, HeaderOp, Vec<Outcome>)> {
    port.pending()
        .into_iter()
        .map(|op| (op.id.0, op.op, op.enabled))
        .collect()
}

/// D2: a call reaches storage on its first poll, not when it's made, so a
/// call dropped unpolled leaves nothing pending.
#[test]
fn a_call_is_submitted_on_its_first_poll() {
    let (port, chain) = chain(1);
    let unpolled = port.selected_chain_contains(chain[0].hash);
    assert!(port.pending().is_empty());
    drop(unpolled);
    assert!(port.pending().is_empty());

    let mut call = port.selected_chain_contains(chain[0].hash);
    submit(&mut call);
    assert_eq!(
        listed(&port),
        [(
            1,
            HeaderOp::SelectedChainContains,
            vec![ENGINE, UNAVAILABLE]
        )]
    );
}

/// ST3.1: IDs count up in submission order and are never reused, and the
/// listing is in ID order with each operation's contract outcomes. A request
/// that fails an adapter-side check offers only its rejection.
#[test]
fn ids_and_enumeration_are_canonical() {
    let (port, chain) = chain(2);
    let semaphore = Arc::new(Semaphore::new(1));
    let mut membership = port.selected_chain_contains(chain[1].hash);
    let mut locator = port.continuation_locator(permit(&semaphore));
    let mut foreign = port.read_header_path(
        foreign_path(&port, chain[1]),
        ReadPath {
            after_hash: HeaderFixture::regtest().anchor().hash,
            max_header_count: 1,
            want_tree_aux: false,
        },
    );
    let mut prepare = port.prepare_header_target(HeaderFixture::regtest().target_request(
        &port.engine().snapshot(),
        chain[1],
        &HeaderFixture::regtest().extend(chain[1], 1, 0),
        2,
    ));
    // Polled out of creation order: IDs follow submission.
    submit(&mut locator);
    submit(&mut membership);
    submit(&mut prepare);
    submit(&mut foreign);
    let expected = vec![
        (1, HeaderOp::ContinuationLocator, vec![ENGINE, UNAVAILABLE]),
        (
            2,
            HeaderOp::SelectedChainContains,
            vec![ENGINE, UNAVAILABLE],
        ),
        (
            3,
            HeaderOp::PrepareHeaderTarget,
            vec![ENGINE, LOCAL_RESOURCE],
        ),
        (4, HeaderOp::ReadHeaderPath, vec![Outcome::Rejected]),
    ];
    assert_eq!(listed(&port), expected);
    assert_eq!(listed(&port), expected, "listing has no side effects");

    port.complete(OpId(2), ENGINE)
        .expect("membership completes");
    // Taking the reply ends operation 2, and its ID stays used.
    ready(membership).expect("membership reads");
    let mut again = port.selected_chain_contains(chain[0].hash);
    submit(&mut again);
    let ids: Vec<_> = port.pending().iter().map(|op| op.id.0).collect();
    assert_eq!(ids, [1, 3, 4, 5]);

    assert!(matches!(
        port.complete(OpId(4), ENGINE),
        Err(CompleteError::Illegal(_))
    ));
    port.complete(OpId(4), Outcome::Rejected)
        .expect("a foreign handle gets its rejection");
    assert!(matches!(
        ready(foreign),
        Err(PortError::Unavailable { source: None })
    ));
}

/// ST3.1: an illegal outcome or an absent operation is refused and changes
/// nothing. The legal outcome then commits and wakes the caller once, and
/// a second completion of the same operation is refused.
#[test]
fn completing_twice_or_illegally_changes_nothing() {
    let (port, chain) = chain(1);
    let before = port.engine().snapshot();
    let digest = port.engine().state_digest();
    let next = HeaderFixture::regtest().extend(chain[0], 1, 0);
    let mut apply = port.apply_header_target(prepared(&port, chain[0], &next, 2));
    let wakes = Arc::new(Wakes::default());
    let waker = Waker::from(wakes.clone());
    assert!(poll(&mut apply, &waker).is_pending());

    for illegal in [TIMEOUT, UNAVAILABLE, Outcome::Rejected] {
        assert!(matches!(
            port.complete(OpId(1), illegal),
            Err(CompleteError::Illegal(_))
        ));
    }
    assert_eq!(
        port.complete(OpId(2), ENGINE),
        Err(CompleteError::NotPending(OpId(2)))
    );
    assert_eq!(port.engine().snapshot(), before);
    assert_eq!(port.engine().state_digest(), digest);
    assert_eq!(
        listed(&port),
        [(1, HeaderOp::ApplyHeaderTarget, vec![ENGINE, LOCAL_RESOURCE])]
    );
    assert!(poll(&mut apply, &waker).is_pending());
    assert_eq!(wakes.count(), 0);

    let completion = port.complete(OpId(1), ENGINE).expect("apply completes");
    assert!(completion.delivered);
    assert_eq!(wakes.count(), 1);
    assert_eq!(
        port.engine().snapshot().frontiers.header_best.hash,
        next[0].hash()
    );
    assert_eq!(
        port.complete(OpId(1), ENGINE),
        Err(CompleteError::NotPending(OpId(1)))
    );
    assert!(matches!(
        poll(&mut apply, &waker),
        Poll::Ready(Ok(ApplyHeaderTargetOutcome::Applied))
    ));
    assert!(port.pending().is_empty());
}

/// ST3.2: the reactor drops its own permit clone and its future when its
/// locator request times out. The pending operation still holds its clone,
/// so the permit comes back only when the operation completes, whatever
/// the outcome.
#[test]
fn a_locator_permit_frees_at_completion_not_at_caller_cancel() {
    let (port, _) = chain(1);
    let semaphore = Arc::new(Semaphore::new(1));
    for outcome in [ENGINE, UNAVAILABLE] {
        let reactor_permit = permit(&semaphore);
        let mut call = port.continuation_locator(reactor_permit.clone());
        submit(&mut call);
        drop(reactor_permit);
        drop(call);
        assert_eq!(semaphore.available_permits(), 0);
        let pending = port.pending();
        assert_eq!(pending.len(), 1);
        assert!(!pending[0].caller_waiting);

        let completion = port
            .complete(pending[0].id, outcome)
            .expect("the locator completes");
        assert!(!completion.delivered, "nobody waits for the reply");
        assert_eq!(semaphore.available_permits(), 1);
        assert!(port.pending().is_empty());
    }
}

/// A delivered reply reaches its caller with the engine's result.
#[test]
fn a_completion_delivers_the_engine_result() {
    let (port, chain) = chain(2);
    let mut call = port.selected_chain_contains(chain[1].hash);
    submit(&mut call);
    port.complete(OpId(1), ENGINE)
        .expect("membership completes");
    assert!(matches!(
        ready(call),
        Ok(SelectedChainMembership::Selected(frontier)) if frontier == chain[1]
    ));
}

/// ST3.6: `timeout` opens exactly 30 s after submission, counted from the
/// first poll, and the gate tells the run when.
#[tokio::test(start_paused = true)]
async fn timeout_opens_at_the_adapter_deadline() {
    let (port, chain) = chain(1);
    let mut call = port.selected_chain_contains(chain[0].hash);
    tokio::time::advance(Duration::from_secs(10)).await;
    submit(&mut call);
    assert_eq!(port.next_gate(), Some(Duration::from_secs(30)));

    tokio::time::advance(Duration::from_secs(29)).await;
    assert_eq!(
        listed(&port),
        [(
            1,
            HeaderOp::SelectedChainContains,
            vec![ENGINE, UNAVAILABLE]
        )]
    );
    assert_eq!(port.next_gate(), Some(Duration::from_secs(1)));
    assert!(matches!(
        port.complete(OpId(1), TIMEOUT),
        Err(CompleteError::Illegal(_))
    ));

    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(
        listed(&port),
        [(
            1,
            HeaderOp::SelectedChainContains,
            vec![ENGINE, TIMEOUT, UNAVAILABLE]
        )]
    );
    assert_eq!(port.next_gate(), None);
    port.complete(OpId(1), TIMEOUT).expect("the gate is open");
    assert!(matches!(ready(call), Err(PortError::Timeout)));
}

/// ST3.6: prepare and apply have no adapter deadline, so even past the
/// 30 min VCT watchdog they never offer a timeout, and they open no gate.
#[tokio::test(start_paused = true)]
async fn prepare_and_apply_never_time_out() {
    let (port, chain) = chain(1);
    let next = HeaderFixture::regtest().extend(chain[0], 1, 0);
    let mut prepare = port.prepare_header_target(HeaderFixture::regtest().target_request(
        &port.engine().snapshot(),
        chain[0],
        &next,
        2,
    ));
    let mut apply = port.apply_header_target(prepared(&port, chain[0], &next, 3));
    submit(&mut prepare);
    submit(&mut apply);
    assert_eq!(port.next_gate(), None);
    tokio::time::advance(Duration::from_secs(31 * 60)).await;
    assert_eq!(
        listed(&port),
        [
            (
                1,
                HeaderOp::PrepareHeaderTarget,
                vec![ENGINE, LOCAL_RESOURCE]
            ),
            (2, HeaderOp::ApplyHeaderTarget, vec![ENGINE, LOCAL_RESOURCE]),
        ]
    );
    assert_eq!(port.next_gate(), None);

    let before = port.engine().snapshot();
    port.complete(OpId(2), LOCAL_RESOURCE)
        .expect("local-resource is legal at any time");
    assert!(matches!(
        ready(apply),
        Err(error) if error.category == ErrorCategory::LocalResourceOrStorage
    ));
    assert_eq!(
        port.engine().snapshot(),
        before,
        "a fault never runs the work"
    );
}

/// The peer numbered `n`.
fn peer(n: u8) -> SourceId {
    SourceId::from_digest([n; 32])
}

/// `peer`'s acquire in `session` of the path from the anchor to `target`.
fn acquire(
    port: &ControlledPort,
    peer: SourceId,
    session_id: u64,
    target: Frontier,
) -> AcquirePath {
    AcquirePath {
        source: peer,
        session_id,
        scope: HeaderWorkAuthority::for_target(&port.engine().snapshot(), target.hash),
        target_tip_hash: target.hash,
        locator_hashes: vec![HeaderFixture::regtest().anchor().hash],
    }
}

/// A read of one page of `path` after the anchor.
fn first_page() -> ReadPath {
    ReadPath {
        after_hash: HeaderFixture::regtest().anchor().hash,
        max_header_count: 1,
        want_tree_aux: false,
    }
}

/// Submits `call`, completes it with the engine outcome, and returns its
/// reply and the completion.
fn run<F: Future + Unpin>(port: &ControlledPort, mut call: F) -> (F::Output, Completion) {
    submit(&mut call);
    let id = port.pending().last().expect("the call is pending").id;
    let completion = port
        .complete(id, ENGINE)
        .expect("the engine outcome is legal");
    (ready(call), completion)
}

/// The leased path in `reply`.
fn acquired(reply: Result<AcquirePathReply, PortError>) -> RetainedHeaderPath {
    match reply {
        Ok(AcquirePathReply::Acquired(path)) => *path,
        other => panic!("expected a lease, got {other:?}"),
    }
}

/// The lease end of `peer`'s `path` for `release`.
fn ended(peer: SourceId, path: &RetainedHeaderPath, release: Release) -> LeaseEnd {
    LeaseEnd {
        peer,
        lease_id: path.handle_id(),
        release,
    }
}

/// ST3.3: an apply whose caller is gone still commits at its completion and
/// publishes on the watch. Only the reply is dropped.
#[test]
fn a_cancelled_apply_still_commits() {
    let (port, chain) = chain(1);
    let mut watch = port.selected_chain();
    watch.borrow_and_update();
    let before = port.engine().state_digest();
    let next = HeaderFixture::regtest().extend(chain[0], 1, 0);
    let target = prepared(&port, chain[0], &next, 2);
    let owner = target.owner();
    let mut apply = port.apply_header_target(target);
    submit(&mut apply);
    drop(apply);

    let completion = port.complete(OpId(1), ENGINE).expect("apply completes");
    assert!(!completion.delivered);
    assert!(matches!(
        completion.request,
        RequestIdentity::ApplyHeaderTarget { owner: applied, .. } if applied == owner
    ));
    assert_eq!(completion.result, ResultIdentity::Applied);
    assert_eq!(
        port.engine().snapshot().frontiers.header_best.hash,
        next[0].hash()
    );
    assert!(watch
        .has_changed()
        .expect("the engine port holds the sender"));
    assert_ne!(completion.state_digest, before);
    assert_eq!(completion.state_digest, port.engine().state_digest());
    let events = port.take_events();
    assert!(matches!(
        events.as_slice(),
        [
            StorageEvent::Submitted {
                id: OpId(1),
                request: RequestIdentity::ApplyHeaderTarget { .. },
                ..
            },
            StorageEvent::CallerDropped { id: OpId(1) },
            StorageEvent::Completed(Completion {
                id: OpId(1),
                delivered: false,
                ..
            }),
        ]
    ));
}

/// ST3.3: a read and a release from an old session, completing after the
/// same peer's new session took a lease, leave the new lease alone.
#[test]
fn late_work_from_an_old_session_leaves_the_new_lease() {
    let (port, chain) = chain(2);
    let target = chain[1];
    let (reply, _) = run(
        &port,
        port.acquire_header_path(acquire(&port, peer(1), 1, target)),
    );
    let old = acquired(reply);
    let mut late_read = port.read_header_path(old.clone(), first_page());
    submit(&mut late_read);
    let (released, _) = run(&port, port.release_header_path(old.clone()));
    released.expect("the release succeeds");
    let (reply, _) = run(
        &port,
        port.acquire_header_path(acquire(&port, peer(1), 2, target)),
    );
    let new = acquired(reply);

    let id = port.pending()[0].id;
    let completion = port.complete(id, ENGINE).expect("the late read completes");
    assert!(matches!(ready(late_read), Ok(ReadPathReply::Unavailable)));
    assert!(completion.lease_ends.is_empty());
    let (released, completion) = run(&port, port.release_header_path(old));
    released.expect("a stale release still succeeds");
    assert!(completion.lease_ends.is_empty());
    assert_eq!(port.engine().live_leases(), 1);

    let (page, _) = run(&port, port.read_header_path(new, first_page()));
    assert!(matches!(page, Ok(ReadPathReply::Page(_))));
}

/// ST3.4: releasing a path twice succeeds both times and ends the lease
/// once.
#[test]
fn a_repeated_release_ends_the_lease_once() {
    let (port, chain) = chain(1);
    let (reply, _) = run(
        &port,
        port.acquire_header_path(acquire(&port, peer(1), 1, chain[0])),
    );
    let path = acquired(reply);
    let (first, completion) = run(&port, port.release_header_path(path.clone()));
    first.expect("the release succeeds");
    assert_eq!(
        completion.lease_ends,
        [ended(peer(1), &path, Release::LeaseReleased)]
    );
    let (second, completion) = run(&port, port.release_header_path(path));
    second.expect("a repeat succeeds");
    assert!(completion.lease_ends.is_empty());
}

/// ST3.4 and D3: an `Acquired` reply nobody takes ends its lease, whether
/// the caller left before the completion or dropped the reply unread. The
/// peer can then acquire again instead of waiting on itself.
#[test]
fn an_abandoned_acquire_ends_its_lease() {
    let (port, chain) = chain(1);
    let mut gone = port.acquire_header_path(acquire(&port, peer(1), 1, chain[0]));
    submit(&mut gone);
    drop(gone);
    let completion = port
        .complete(OpId(1), ENGINE)
        .expect("the acquire completes");
    assert!(!completion.delivered);
    assert!(matches!(
        completion.lease_ends.as_slice(),
        [LeaseEnd {
            release: Release::LeaseAbandonedAcquire,
            ..
        }]
    ));
    assert_eq!(port.engine().live_leases(), 0);

    let mut unread = port.acquire_header_path(acquire(&port, peer(1), 1, chain[0]));
    submit(&mut unread);
    let completion = port
        .complete(OpId(2), ENGINE)
        .expect("the acquire completes");
    assert!(completion.delivered);
    assert_eq!(port.engine().live_leases(), 1);
    port.take_events();
    drop(unread);
    assert_eq!(port.engine().live_leases(), 0);
    assert!(matches!(
        port.take_events().as_slice(),
        [StorageEvent::ReplyDropped {
            id: OpId(2),
            lease_ends,
        }] if matches!(
            lease_ends.as_slice(),
            [LeaseEnd { release: Release::LeaseAbandonedAcquire, .. }]
        )
    ));

    let (reply, _) = run(
        &port,
        port.acquire_header_path(acquire(&port, peer(1), 1, chain[0])),
    );
    acquired(reply);
}

/// ST3.4: a read whose caller is gone still consumes its lease, so the slot
/// frees instead of leaking.
#[test]
fn a_cancelled_read_still_consumes_its_lease() {
    let (port, chain) = chain(1);
    let (reply, _) = run(
        &port,
        port.acquire_header_path(acquire(&port, peer(1), 1, chain[0])),
    );
    let path = acquired(reply);
    let mut read = port.read_header_path(path.clone(), first_page());
    submit(&mut read);
    drop(read);
    let completion = port.complete(OpId(2), ENGINE).expect("the read completes");
    assert_eq!(
        completion.lease_ends,
        [ended(peer(1), &path, Release::LeaseConsumedByRead)]
    );
    assert_eq!(port.engine().live_leases(), 0);
}

/// ST3.4: a released handle, and a handle forged with this port's key but
/// another lease ID, read nothing, and the forged read leaves the live
/// lease alone.
#[test]
fn released_and_forged_handles_cannot_read() {
    let (port, chain) = chain(1);
    let (reply, _) = run(
        &port,
        port.acquire_header_path(acquire(&port, peer(1), 1, chain[0])),
    );
    let released = acquired(reply);
    run(&port, port.release_header_path(released.clone()))
        .0
        .expect("the release succeeds");
    let (read, _) = run(&port, port.read_header_path(released, first_page()));
    assert!(matches!(read, Ok(ReadPathReply::Unavailable)));

    let (reply, _) = run(
        &port,
        port.acquire_header_path(acquire(&port, peer(1), 1, chain[0])),
    );
    let live = acquired(reply);
    let forged = RetainedHeaderPath::from_adapter(
        port.engine().adapter_key(),
        live.handle_id() + 1,
        peer(1),
        1,
        live.common_ancestor,
        live.target,
        live.scope,
    );
    let (read, completion) = run(&port, port.read_header_path(forged, first_page()));
    assert!(matches!(read, Ok(ReadPathReply::Unavailable)));
    assert!(completion.lease_ends.is_empty());
    assert_eq!(port.engine().live_leases(), 1);
}

/// The idle sweep's lease ends belong to the operation that swept them,
/// whichever peer's it is.
#[tokio::test(start_paused = true)]
async fn an_idle_sweep_is_recorded_on_the_sweeping_acquire() {
    let (port, chain) = chain(1);
    let (reply, _) = run(
        &port,
        port.acquire_header_path(acquire(&port, peer(1), 1, chain[0])),
    );
    let idle = acquired(reply);
    tokio::time::advance(Duration::from_secs(30)).await;
    let (reply, completion) = run(
        &port,
        port.acquire_header_path(acquire(&port, peer(2), 1, chain[0])),
    );
    acquired(reply);
    assert_eq!(
        completion.lease_ends,
        [ended(peer(1), &idle, Release::LeaseIdleExpired)]
    );
}

/// Records name whose work an operation is. Request digests tell apart
/// reads of different leases, which `Debug` alone can't, and state digests
/// repeat on a fresh port with the same history.
#[test]
fn records_carry_attribution_and_digests() {
    let histories: Vec<_> = (0..2)
        .map(|_| {
            let (port, chain) = chain(1);
            let mut acquire_call = port.acquire_header_path(acquire(&port, peer(3), 9, chain[0]));
            submit(&mut acquire_call);
            port.complete(OpId(1), ENGINE)
                .expect("the acquire completes");
            let path = acquired(ready(acquire_call));
            let forged = RetainedHeaderPath::from_adapter(
                port.engine().adapter_key(),
                path.handle_id() + 1,
                peer(3),
                9,
                path.common_ancestor,
                path.target,
                path.scope,
            );
            let mut reads = [
                port.read_header_path(path, first_page()),
                port.read_header_path(forged, first_page()),
            ];
            for read in &mut reads {
                submit(read);
            }
            let events = port.take_events();
            let read_digests: Vec<_> = events
                .iter()
                .filter_map(|event| match event {
                    StorageEvent::Submitted {
                        request: RequestIdentity::ReadHeaderPath { lease, .. },
                        request_digest,
                        ..
                    } => {
                        let lease = lease.expect("both handles carry this port's seal");
                        assert_eq!((lease.peer, lease.session_id), (peer(3), 9));
                        Some(*request_digest)
                    }
                    _ => None,
                })
                .collect();
            assert_eq!(read_digests.len(), 2);
            assert_ne!(read_digests[0], read_digests[1]);
            let StorageEvent::Completed(acquired) = &events[1] else {
                panic!("expected the acquire's completion, got {:?}", events[1]);
            };
            assert!(matches!(
                acquired.request,
                RequestIdentity::AcquireHeaderPath { peer: acquirer, session_id: 9, .. }
                    if acquirer == peer(3)
            ));
            (read_digests, acquired.state_digest)
        })
        .collect();
    assert_eq!(histories[0], histories[1]);
}

/// D10: shutdown drops pending work unrun, with its inputs, drops untaken
/// replies, and ends every lease. A call polled afterwards never reaches
/// storage. A repeat does nothing.
#[test]
fn shutdown_ends_everything_without_running_work() {
    let (port, chain) = chain(1);
    let snapshot = port.engine().snapshot();
    let semaphore = Arc::new(Semaphore::new(1));
    let next = HeaderFixture::regtest().extend(chain[0], 1, 0);
    let mut apply = port.apply_header_target(prepared(&port, chain[0], &next, 2));
    let mut locator = port.continuation_locator(permit(&semaphore));
    let mut unread = port.acquire_header_path(acquire(&port, peer(1), 1, chain[0]));
    submit(&mut apply);
    drop(apply);
    submit(&mut locator);
    submit(&mut unread);
    port.complete(OpId(3), ENGINE)
        .expect("the acquire completes");
    assert_eq!(port.engine().live_leases(), 1);
    assert_eq!(semaphore.available_permits(), 0);
    port.take_events();

    port.shut_down();
    assert_eq!(port.engine().snapshot(), snapshot, "the apply never ran");
    assert_eq!(semaphore.available_permits(), 1);
    assert_eq!(port.engine().live_leases(), 0);
    assert!(port.pending().is_empty());
    assert!(matches!(
        port.take_events().as_slice(),
        [StorageEvent::ShutDown { dropped, lease_ends }]
            if dropped == &[OpId(1), OpId(2), OpId(3)]
                && matches!(
                    lease_ends.as_slice(),
                    [LeaseEnd { release: Release::Shutdown, .. }]
                )
    ));

    assert!(poll(&mut locator, Waker::noop()).is_pending());
    assert!(poll(&mut unread, Waker::noop()).is_pending());
    let mut after = port.selected_chain_contains(chain[0].hash);
    assert!(poll(&mut after, Waker::noop()).is_pending());
    drop((locator, unread, after));
    port.shut_down();
    assert!(port.pending().is_empty());
    assert!(port.take_events().is_empty());
}

/// Shutdown closes the selected-chain watch, so a receiver taken before it
/// sees the watch closed, and one taken after it starts closed.
#[test]
fn shutdown_closes_the_selected_chain_watch() {
    let (port, _) = chain(1);
    let before = port.selected_chain();
    assert!(
        before.has_changed().is_ok(),
        "the watch is open until shutdown"
    );

    port.shut_down();
    assert!(before.has_changed().is_err());
    assert!(port.selected_chain().has_changed().is_err());
}

/// #1721 review P2 (Astra): two applies with the same owner, prepared from
/// the same headers with different body sizes, carry different auxiliary
/// deliveries, so their request digests differ. Apply records also name
/// the supplier.
#[test]
fn apply_digests_cover_the_sealed_insertion() {
    let (port, chain) = chain(1);
    let next = HeaderFixture::regtest().extend(chain[0], 1, 0);
    let targets: Vec<_> = [0, 1_000]
        .into_iter()
        .map(|body_size| {
            let mut request = HeaderFixture::regtest().target_request(
                &port.engine().snapshot(),
                chain[0],
                &next,
                2,
            );
            let mut entries = request.entries.into_vec();
            entries[0].body_size = body_size;
            request.entries = entries.try_into().expect("one entry fits");
            ready(port.engine().prepare_header_target(request)).expect("fixture headers prepare")
        })
        .collect();
    assert_eq!(targets[0].owner(), targets[1].owner());
    let source = targets[0].source();
    let mut applies: Vec<_> = targets
        .into_iter()
        .map(|target| port.apply_header_target(target))
        .collect();
    for apply in &mut applies {
        submit(apply);
    }
    let submitted: Vec<_> = port
        .take_events()
        .into_iter()
        .filter_map(|event| match event {
            StorageEvent::Submitted {
                request: RequestIdentity::ApplyHeaderTarget { source, .. },
                request_digest,
                ..
            } => Some((Some(source), request_digest)),
            _ => None,
        })
        .collect();
    assert_eq!(submitted.len(), 2);
    assert_eq!(submitted[0].0, Some(source));
    assert_eq!(submitted[1].0, Some(source));
    assert_ne!(submitted[0].1, submitted[1].1);
}

/// Two applies with the same owner, supplier and target, prepared on
/// different parents, cover different ranges, so their typed identities
/// differ by the parent their sealed insertions name.
#[test]
fn apply_identities_name_their_parent() {
    let (port, chain) = chain(1);
    let anchor = HeaderFixture::regtest().anchor();
    let headers = HeaderFixture::regtest().extend(anchor, 3, 0);
    assert_eq!(headers[0].hash(), chain[0].hash);
    let targets = [
        prepared(&port, anchor, &headers, 2),
        prepared(&port, chain[0], &headers[1..], 2),
    ];
    let named =
        |target: &PreparedHeaderTarget| (target.owner(), target.source(), target.target_tip_hash());
    assert_eq!(named(&targets[0]), named(&targets[1]));
    for target in targets {
        submit(&mut port.apply_header_target(target));
    }
    let identities: Vec<_> = port
        .take_events()
        .into_iter()
        .filter_map(|event| match event {
            StorageEvent::Submitted { request, .. } => Some(request),
            _ => None,
        })
        .collect();
    let parents: Vec<_> = identities
        .iter()
        .map(|identity| match identity {
            RequestIdentity::ApplyHeaderTarget { parent_hash, .. } => *parent_hash,
            other => panic!("expected an apply, got {other:?}"),
        })
        .collect();
    assert_eq!(parents, [Some(anchor.hash), Some(chain[0].hash)]);
    assert_ne!(identities[0], identities[1]);
}

/// D12: a locator submitted after shutdown never reaches storage, and its
/// permit comes back as soon as the call is polled.
#[test]
fn a_locator_after_shutdown_returns_its_permit() {
    let (port, _) = chain(1);
    port.shut_down();
    let semaphore = Arc::new(Semaphore::new(1));
    let mut call = port.continuation_locator(permit(&semaphore));
    assert_eq!(semaphore.available_permits(), 0);
    assert!(poll(&mut call, Waker::noop()).is_pending());
    assert_eq!(semaphore.available_permits(), 1);
    assert!(port.pending().is_empty());
}

/// `count` headers on the anchor, admitted in batches no longer than the
/// transition cap through the engine port directly. Returns the port and
/// the headers' frontiers.
fn long_chain(count: u32) -> (ControlledPort, Vec<Frontier>) {
    let fixture = HeaderFixture::regtest();
    let port = ControlledPort::new(&fixture);
    let cap = u32::try_from(zakura_header_chain::MAX_HEADERS_PER_TRANSITION_V1)
        .expect("the cap fits u32");
    let (mut parent, mut frontiers, mut request_id) = (fixture.anchor(), Vec::new(), 1);
    while frontiers.len() < count as usize {
        // Widening a header count to usize is lossless on supported targets.
        let batch = fixture.extend(parent, (count - frontiers.len() as u32).min(cap), 0);
        let target = prepared(&port, parent, &batch, request_id);
        assert_eq!(
            ready(port.engine().apply_header_target(target)).expect("fixture headers apply"),
            ApplyHeaderTargetOutcome::Applied
        );
        for header in &batch {
            parent = Frontier::new(block::Height(parent.height.0 + 1), header.hash());
            frontiers.push(parent);
        }
        request_id += 1;
    }
    (port, frontiers)
}

/// A read of `count` headers after `after`.
fn page_after(after: Frontier, count: u32) -> ReadPath {
    ReadPath {
        after_hash: after.hash,
        max_header_count: count,
        want_tree_aux: false,
    }
}

/// B2: each operation's typed request and result identity, from the
/// locator's generation and anchor to a page's last header and a lease's
/// key, with the owner each request carries.
///
/// Its `PortError` read is input validation only: a page count of zero. A
/// read finding the engine incoherent needs a cursor that disagrees with
/// the graph, which no sequence of port calls produces, so nothing here
/// covers that branch.
#[test]
fn completions_carry_typed_identities() {
    let (port, chain) = chain(3);
    let anchor = HeaderFixture::regtest().anchor();
    let snapshot = port.engine().snapshot();
    let semaphore = Arc::new(Semaphore::new(1));

    let (_, locator) = run(&port, port.continuation_locator(permit(&semaphore)));
    assert_eq!(locator.request, RequestIdentity::ContinuationLocator);
    assert_eq!(
        locator.result,
        ResultIdentity::Locator {
            tip: chain[2],
            header_generation: snapshot.header_generation,
            finalized: anchor,
        }
    );

    let (reply, acquire_completion) = run(
        &port,
        port.acquire_header_path(acquire(&port, peer(1), 5, chain[2])),
    );
    let path = acquired(reply);
    let lease = LeaseKey {
        peer: peer(1),
        session_id: 5,
        lease_id: path.handle_id(),
    };
    assert_eq!(
        acquire_completion.request,
        RequestIdentity::AcquireHeaderPath {
            peer: peer(1),
            session_id: 5,
            scope: path.scope,
            target_tip_hash: chain[2].hash,
            locators: vec![anchor.hash],
        }
    );
    assert_eq!(
        acquire_completion.result,
        ResultIdentity::Acquired {
            lease,
            common_ancestor: anchor,
            target: chain[2],
        }
    );

    let (_, read) = run(
        &port,
        port.read_header_path(path.clone(), page_after(anchor, 2)),
    );
    assert_eq!(
        read.request,
        RequestIdentity::ReadHeaderPath {
            lease: Some(lease),
            scope: path.scope,
            after_hash: anchor.hash,
            max_header_count: 2,
        }
    );
    assert_eq!(
        read.result,
        ResultIdentity::Page {
            last: chain[1],
            count: 2,
            complete: false,
        }
    );
    let mut continued = acquire(&port, peer(1), 5, chain[2]);
    continued.locator_hashes = vec![chain[1].hash];
    let (reply, _) = run(&port, port.acquire_header_path(continued));
    let rest = acquired(reply);
    let (_, read) = run(
        &port,
        port.read_header_path(rest.clone(), page_after(chain[1], 2)),
    );
    assert_eq!(
        read.result,
        ResultIdentity::Page {
            last: chain[2],
            count: 1,
            complete: true,
        }
    );
    let (_, release) = run(&port, port.release_header_path(rest.clone()));
    assert_eq!(release.result, ResultIdentity::Released);
    let (_, stale) = run(
        &port,
        port.read_header_path(rest.clone(), page_after(chain[2], 1)),
    );
    assert_eq!(stale.result, ResultIdentity::PageUnavailable);
    // A zero page count fails input validation before the registry.
    let (_, bad) = run(&port, port.read_header_path(rest, page_after(chain[2], 0)));
    assert_eq!(bad.result, ResultIdentity::PortError);

    let (_, selected) = run(&port, port.selected_chain_contains(chain[1].hash));
    assert_eq!(selected.result, ResultIdentity::Selected(chain[1]));
    let (_, unknown) = run(&port, port.selected_chain_contains(block::Hash([0xee; 32])));
    assert_eq!(unknown.result, ResultIdentity::NotRetained);

    let next = HeaderFixture::regtest().extend(chain[2], 1, 0);
    let request =
        HeaderFixture::regtest().target_request(&port.engine().snapshot(), chain[2], &next, 9);
    let owner = request.owner;
    let (target, prepare) = run(&port, port.prepare_header_target(request));
    assert_eq!(
        prepare.request,
        RequestIdentity::PrepareHeaderTarget {
            owner,
            source: SourceId::from_digest([7; 32]),
            common_ancestor: chain[2],
            target: Frontier::new(block::Height(4), next[0].hash()),
            headers: 1,
        }
    );
    assert_eq!(prepare.result, ResultIdentity::Prepared);
    let (_, apply) = run(
        &port,
        port.apply_header_target(target.expect("fixture headers prepare")),
    );
    assert_eq!(apply.result, ResultIdentity::Applied);

    let body_owner = zakura_header_chain::BodyWorkAuthority {
        header: path.scope,
        verified_generation: zakura_header_chain::VerifiedGeneration::new(3),
        body_work_epoch: zakura_header_chain::BodyWorkEpoch::default(),
    }
    .bind(8, std::num::NonZeroU64::new(9).expect("nine is nonzero"));
    let (_, vct) = run(&port, port.vct_repair_context(body_owner, block::Height(2)));
    assert_eq!(
        vct.request,
        RequestIdentity::VctRepairContext {
            owner: body_owner,
            height: block::Height(2),
        }
    );
    assert_eq!(vct.result, ResultIdentity::VctStale);

    let mut faulted = port.selected_chain_contains(chain[0].hash);
    submit(&mut faulted);
    let id = port.pending()[0].id;
    let fault = port
        .complete(id, UNAVAILABLE)
        .expect("unavailable is legal");
    assert_eq!(fault.result, ResultIdentity::Fault(Fault::Unavailable));
    let mut foreign = port.read_header_path(foreign_path(&port, chain[1]), first_page());
    submit(&mut foreign);
    let id = port.pending()[0].id;
    let rejected = port
        .complete(id, Outcome::Rejected)
        .expect("a foreign handle is rejected");
    assert_eq!(rejected.result, ResultIdentity::Rejected);
}

/// B2: a `CapacityBusy` refusal names the signal it handed out, read from
/// the registry: the peer's own lease, the ordinary class for a path
/// beyond the reserve slot, and the reserve class when every slot is taken.
#[test]
fn refusals_name_the_signal_they_hand_out() {
    let (port, chain) = long_chain(4_001);
    let short = |n| {
        let mut request = acquire(&port, peer(n), 1, chain[1]);
        request.locator_hashes = vec![chain[0].hash];
        request
    };
    for n in 1..u8::try_from(MAX_RETAINED_PATH_LEASES).expect("16 fits u8") {
        acquired(run(&port, port.acquire_header_path(short(n))).0);
    }
    let (_, again) = run(&port, port.acquire_header_path(short(1)));
    assert_eq!(
        again.result,
        ResultIdentity::CapacityBusy(SignalClass::Peer)
    );
    let (_, beyond) = run(
        &port,
        port.acquire_header_path(acquire(&port, peer(100), 1, chain[4_000])),
    );
    assert_eq!(
        beyond.result,
        ResultIdentity::CapacityBusy(SignalClass::General)
    );
    acquired(
        run(
            &port,
            port.acquire_header_path(acquire(&port, peer(101), 1, chain[3_999])),
        )
        .0,
    );
    let (_, full) = run(&port, port.acquire_header_path(short(102)));
    assert_eq!(
        full.result,
        ResultIdentity::CapacityBusy(SignalClass::Fallback)
    );
}

/// Two applies of the same headers with the same owner, supplier and
/// auxiliary deliveries, sealed for different targets, have different
/// request digests: one may commit while the other is stale.
#[test]
fn apply_digests_cover_the_target() {
    let (port, chain) = chain(1);
    let next = HeaderFixture::regtest().extend(chain[0], 1, 0);
    let targets: Vec<_> = [None, Some(block::Hash([0xee; 32]))]
        .into_iter()
        .map(|target_hash| {
            let mut request = HeaderFixture::regtest().target_request(
                &port.engine().snapshot(),
                chain[0],
                &next,
                2,
            );
            if let Some(hash) = target_hash {
                request.target.hash = hash;
            }
            ready(port.engine().prepare_header_target(request)).expect("fixture headers prepare")
        })
        .collect();
    assert_eq!(targets[0].owner(), targets[1].owner());
    assert_ne!(targets[0].target_tip_hash(), targets[1].target_tip_hash());
    let mut applies: Vec<_> = targets
        .into_iter()
        .map(|target| port.apply_header_target(target))
        .collect();
    for apply in &mut applies {
        submit(apply);
    }
    let digests: Vec<_> = port
        .take_events()
        .into_iter()
        .filter_map(|event| match event {
            StorageEvent::Submitted { request_digest, .. } => Some(request_digest),
            _ => None,
        })
        .collect();
    assert_eq!(digests.len(), 2);
    assert_ne!(digests[0], digests[1]);
}

/// A caller that polls again with a different waker, as a task does when a
/// `select!` moves the call, gets the completion's wake on the new waker.
/// The old one is never woken.
#[test]
fn a_completion_wakes_the_latest_waker() {
    let (port, chain) = chain(1);
    let mut call = port.selected_chain_contains(chain[0].hash);
    let (first, second) = (Arc::new(Wakes::default()), Arc::new(Wakes::default()));
    assert!(poll(&mut call, &Waker::from(first.clone())).is_pending());
    assert!(poll(&mut call, &Waker::from(second.clone())).is_pending());
    port.complete(OpId(1), ENGINE)
        .expect("membership completes");
    assert_eq!((first.count(), second.count()), (0, 1));
}

/// A completion with nobody waiting drops its reply rather than keeping
/// it in the port. A prepared target holds its headers, so once the
/// prepare completes for a caller that's gone, the test's copy is the only
/// one left.
#[test]
fn a_reply_nobody_waits_for_is_dropped() {
    let (port, chain) = chain(1);
    let next = HeaderFixture::regtest().extend(chain[0], 1, 0);
    let request =
        HeaderFixture::regtest().target_request(&port.engine().snapshot(), chain[0], &next, 2);
    let mut prepare = port.prepare_header_target(request);
    submit(&mut prepare);
    drop(prepare);
    assert!(
        Arc::strong_count(&next[0]) > 1,
        "the pending request holds it"
    );
    let completion = port.complete(OpId(1), ENGINE).expect("prepare completes");
    assert!(!completion.delivered);
    assert_eq!(Arc::strong_count(&next[0]), 1, "the reply was dropped");
}
