//! Serving ownership suite: six cases that every [`Serve`] user runs.
//!
//! A service adds an adapter that implements [`ServingUnderTest`] and invokes
//! [`serving_ownership_suite!`]. The cases check capacity counters and
//! delivered frames only; they use no reference model.

use std::{future::Future, sync::Arc, time::Duration};

use tokio_util::sync::CancellationToken;

use crate::zakura::{
    framed_channel,
    regulation::{PeerServeLimits, Serve, ServeCapacity, ServeSession},
    Frame, FramedRecv, ZakuraPeerId,
};

/// Node execution slots in every case.
pub(crate) const NODE_SLOTS: usize = 2;

/// How long a case waits for a response. Time is paused, so this costs no
/// wall-clock time once the work is done.
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);

/// One serving implementation and the controls the suite needs.
pub(crate) trait ServingUnderTest: Send + Sync + Sized + 'static {
    /// The service under test.
    type Serve: Serve;
    /// Holding this value blocks `produce`; dropping it releases the work.
    type Stall: Send;

    /// A fresh adapter.
    fn new() -> impl Future<Output = Self> + Send;
    /// The shared service instance.
    fn serve(&self) -> Arc<Self::Serve>;
    /// The `seq`-th valid request.
    fn request(&self, seq: u32) -> <Self::Serve as Serve>::Request;
    /// The largest response frame, used as each peer's output budget.
    fn max_response_bytes(&self) -> u32;
    /// Block every later `produce` until the returned value is dropped.
    fn stall(&self) -> impl Future<Output = Self::Stall> + Send;
    /// Make the next `produce` fail locally. Returns `false` if the service
    /// has no local failure to inject; the failure case then only checks
    /// that the next request still gets a response.
    fn fail_next(&self) -> bool;
    /// Check that `frame` answers the `seq`-th request.
    fn assert_response(&self, frame: &Frame, seq: u32);
}

/// One adapter with serving capacity: one execution slot and one response of
/// output per peer, and [`NODE_SLOTS`] node slots.
pub(crate) struct Harness<T> {
    pub(crate) adapter: T,
    pub(crate) capacity: ServeCapacity,
}

/// A served session with its peer-side reader and cancellation.
pub(crate) struct PeerLink<S> {
    pub(crate) session: ServeSession<S>,
    pub(crate) peer_recv: FramedRecv,
    pub(crate) cancel: CancellationToken,
}

pub(crate) fn peer(seed: u8) -> ZakuraPeerId {
    ZakuraPeerId::new(vec![seed; 32]).expect("test peer IDs have the required length")
}

impl<T: ServingUnderTest> Harness<T> {
    pub(crate) async fn new() -> Self {
        let adapter = T::new().await;
        let capacity = ServeCapacity::new(
            NODE_SLOTS,
            PeerServeLimits {
                execution_slots: 1,
                output_bytes: adapter.max_response_bytes(),
            },
        );
        Self { adapter, capacity }
    }

    pub(crate) fn link(&self, seed: u8) -> PeerLink<T::Serve> {
        let (send, peer_recv) = framed_channel(4);
        let cancel = CancellationToken::new();
        let session = ServeSession::new(
            self.adapter.serve(),
            &self.capacity,
            &peer(seed),
            send,
            cancel.clone(),
        );
        PeerLink {
            session,
            peer_recv,
            cancel,
        }
    }

    /// Wait until the peer's and the node's execution and output are released.
    pub(crate) async fn await_released(&self, seed: u8) {
        await_until(|| {
            self.capacity.node_slots_held() == 0 && self.capacity.peer_held(&peer(seed)) == (0, 0)
        })
        .await;
    }
}

/// Poll `condition` while spawned and blocking work runs.
pub(crate) async fn await_until(mut condition: impl FnMut() -> bool) {
    for _ in 0..10_000 {
        if condition() {
            return;
        }
        tokio::task::yield_now().await;
        // Blocking threads run on real time; give them a moment.
        std::thread::sleep(Duration::from_micros(50));
    }
    panic!("condition did not hold");
}

/// Whether `future` is still pending after spawned and blocking work has had
/// time to run.
///
/// This polls instead of using a timer: a paused clock does not advance while
/// a blocking task runs, and a stalled case keeps one running.
pub(crate) async fn stays_pending(future: impl Future) -> bool {
    tokio::pin!(future);
    for _ in 0..200 {
        if futures::poll!(&mut future).is_ready() {
            return false;
        }
        tokio::task::yield_now().await;
        std::thread::sleep(Duration::from_micros(50));
    }
    true
}

async fn next_frame(peer_recv: &mut FramedRecv) -> Frame {
    tokio::time::timeout(RESPONSE_TIMEOUT, peer_recv.recv())
        .await
        .expect("the response arrives")
        .expect("the stream stays open")
}

pub(crate) async fn cancel_mid_production_keeps_execution_until_the_work_ends<
    T: ServingUnderTest,
>() {
    let harness = Harness::<T>::new().await;
    let stall = harness.adapter.stall().await;
    let mut link = harness.link(1);
    link.session.serve(harness.adapter.request(1)).await;
    await_until(|| harness.capacity.node_slots_held() == 1).await;

    link.cancel.cancel();
    assert!(stays_pending(std::future::pending::<()>()).await);
    assert_eq!(
        harness.capacity.node_slots_held(),
        1,
        "cancellation does not release capacity held by running work"
    );

    drop(stall);
    harness.await_released(1).await;
    assert!(
        stays_pending(link.peer_recv.recv()).await,
        "a cancelled request sends nothing"
    );
}

pub(crate) async fn reconnect_shares_peer_capacity<T: ServingUnderTest>() {
    let harness = Harness::<T>::new().await;
    let stall = harness.adapter.stall().await;
    let mut first = harness.link(1);
    let reconnect = harness.link(1);
    first.session.serve(harness.adapter.request(1)).await;

    let second = reconnect.session.serve(harness.adapter.request(2));
    tokio::pin!(second);
    assert!(
        stays_pending(&mut second).await,
        "a reconnect waits for the peer's busy execution slot"
    );
    drop(stall);
    // The first response shares the peer's output budget until it is read.
    let frame = next_frame(&mut first.peer_recv).await;
    harness.adapter.assert_response(&frame, 1);
    tokio::time::timeout(RESPONSE_TIMEOUT, second)
        .await
        .expect("the reconnect is admitted once the first response is written");
}

pub(crate) async fn blocked_output_stops_at_the_byte_bound_without_node_slots<
    T: ServingUnderTest,
>() {
    let harness = Harness::<T>::new().await;
    let link = harness.link(1);
    link.session.serve(harness.adapter.request(1)).await;
    await_until(|| harness.capacity.node_slots_held() == 0).await;

    let second = link.session.serve(harness.adapter.request(2));
    tokio::pin!(second);
    assert!(
        stays_pending(&mut second).await,
        "an unread response holds the peer's whole output budget"
    );
    let (execution, output) = harness.capacity.peer_held(&peer(1));
    assert_eq!(execution, 1, "the waiting request holds only its peer slot");
    let output_bound = usize::try_from(harness.adapter.max_response_bytes()).expect("fits usize");
    assert!(output > 0 && output <= output_bound);
    assert_eq!(harness.capacity.node_slots_held(), 0);
}

pub(crate) async fn failure_releases_everything<T: ServingUnderTest>() {
    let harness = Harness::<T>::new().await;
    let mut link = harness.link(1);
    if harness.adapter.fail_next() {
        link.session.serve(harness.adapter.request(1)).await;
        harness.await_released(1).await;
        assert!(
            stays_pending(link.peer_recv.recv()).await,
            "a failed request sends nothing"
        );
    }

    link.session.serve(harness.adapter.request(2)).await;
    let frame = next_frame(&mut link.peer_recv).await;
    harness.adapter.assert_response(&frame, 2);
}

pub(crate) async fn non_reading_peers_do_not_starve_an_honest_peer<T: ServingUnderTest>() {
    let harness = Harness::<T>::new().await;
    let mut stuck = Vec::new();
    let non_readers = u8::try_from(NODE_SLOTS + 2).expect("a few peers fit u8");
    for seed in 1..=non_readers {
        let link = harness.link(seed);
        link.session.serve(harness.adapter.request(1)).await;
        // A second request per peer waits for output that never frees.
        let request = harness.adapter.request(2);
        stuck.push(tokio::spawn(async move {
            link.session.serve(request).await;
            link
        }));
    }
    await_until(|| harness.capacity.node_slots_held() == 0).await;

    let mut honest = harness.link(100);
    honest.session.serve(harness.adapter.request(7)).await;
    let frame = next_frame(&mut honest.peer_recv).await;
    harness.adapter.assert_response(&frame, 7);
    for task in stuck {
        task.abort();
    }
}

pub(crate) async fn exactly_one_response_per_request<T: ServingUnderTest>() {
    let harness = Harness::<T>::new().await;
    let mut link = harness.link(1);
    for seq in 0..5 {
        link.session.serve(harness.adapter.request(seq)).await;
        let frame = next_frame(&mut link.peer_recv).await;
        harness.adapter.assert_response(&frame, seq);
    }
    harness.await_released(1).await;
    assert!(
        stays_pending(link.peer_recv.recv()).await,
        "no request answers twice"
    );
}

/// Add the six ownership cases for adapter `$adapter` in a module `$name`.
macro_rules! serving_ownership_suite {
    ($name:ident, $adapter:ty) => {
        mod $name {
            use super::*;
            use $crate::zakura::regulation::serving_kit as kit;

            #[tokio::test(start_paused = true)]
            async fn cancel_mid_production_keeps_execution_until_the_work_ends() {
                kit::cancel_mid_production_keeps_execution_until_the_work_ends::<$adapter>().await;
            }

            #[tokio::test(start_paused = true)]
            async fn reconnect_shares_peer_capacity() {
                kit::reconnect_shares_peer_capacity::<$adapter>().await;
            }

            #[tokio::test(start_paused = true)]
            async fn blocked_output_stops_at_the_byte_bound_without_node_slots() {
                kit::blocked_output_stops_at_the_byte_bound_without_node_slots::<$adapter>().await;
            }

            #[tokio::test(start_paused = true)]
            async fn failure_releases_everything() {
                kit::failure_releases_everything::<$adapter>().await;
            }

            #[tokio::test(start_paused = true)]
            async fn non_reading_peers_do_not_starve_an_honest_peer() {
                kit::non_reading_peers_do_not_starve_an_honest_peer::<$adapter>().await;
            }

            #[tokio::test(start_paused = true)]
            async fn exactly_one_response_per_request() {
                kit::exactly_one_response_per_request::<$adapter>().await;
            }
        }
    };
}

pub(crate) use serving_ownership_suite;
