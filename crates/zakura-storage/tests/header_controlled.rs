//! The controlled header port: submission, enumeration, completion,
//! cancellation and timeout gates (storage.md ST3.1, ST3.2 and ST3.6).

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
    AdapterKey, ApplyHeaderTargetOutcome, LocatorReadPermit, Port, PortError, PreparedHeaderTarget,
    ReadPath, RetainedHeaderPath, SelectedChainMembership,
};
use zakura_rt::sync::Semaphore;
use zakura_storage::header::{
    controlled::{CompleteError, ControlledPort, OpId},
    fixture::HeaderFixture,
    Fault, HeaderOp, Outcome,
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
