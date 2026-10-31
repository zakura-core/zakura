//! Node-wide owners for work that legacy TCP and compatibility peers can start.
//!
//! # Capacity and progress contract
//!
//! Nothing in this module waits, so no caller can wait for capacity while it holds a
//! lock or an earlier reservation. Each limit acts only on work that a conformant
//! peer cannot be waiting for, or on a count that conformant callers cannot reach:
//!
//! | Owner | Refusal |
//! | --- | --- |
//! | [`InboundBudget::try_charge`] | Unsolicited work drops, as it does when a queue is full. |
//! | [`InboundBudget::charge_committed`] | Never refuses. A response the node asked for cannot wait or drop without timing out a conformant peer. |
//! | [`InboundRowAdmission`] | Sized for every caller at once, so a refusal means a caller exceeded its own limit. |
//! | [`TcpHandshakeOwner`] | Sized for every connection slot, so a refusal means a dialer exceeded its own limit. |

use std::{
    sync::Arc,
    task::{Context, Poll},
};

use futures::future::{self, BoxFuture, FutureExt};
use thiserror::Error;
use tower::Service;
use zakura_rt::sync::{
    atomic::{AtomicU64, Ordering},
    OwnedSemaphorePermit, Semaphore,
};

use crate::{BoxError, Request, Response};

/// Decoded `block` and `tx` bytes that unsolicited TCP work may retain across this node.
///
/// A connection handles one message at a time, so this bound applies only to a node
/// whose connections all hold large transactions at once. This is an unmeasured choice,
/// and it matches the compatibility partition.
pub(crate) const TCP_INBOUND_RETAINED_BYTES: u64 = 128 * 1024 * 1024;

/// The heap bytes of a vector's own allocation.
pub(crate) fn vec_retained_bytes<T>(items: &Vec<T>) -> u64 {
    // Widening usize to u64 is lossless on supported targets.
    items.capacity().saturating_mul(std::mem::size_of::<T>()) as u64
}

/// Decoded bytes one budget's unsolicited work may retain.
///
/// This is one partition of the node's decoded ingress. Each budget has one owner, so
/// a flood on one protocol cannot refuse a conformant request on the other.
#[derive(Clone, Debug)]
pub(crate) struct InboundBudget {
    used: Arc<AtomicU64>,
    capacity: u64,
}

impl InboundBudget {
    /// Create one budget. Clones share its capacity.
    pub(crate) fn new(capacity: u64) -> Self {
        Self {
            used: Arc::new(AtomicU64::new(0)),
            capacity,
        }
    }

    /// Charge `bytes` now, or nothing. A larger item clamps to the whole budget, so
    /// it can still enter an idle budget. An empty item always enters.
    pub(crate) fn try_charge(&self, bytes: u64) -> Option<InboundCharge> {
        let bytes = bytes.min(self.capacity);
        if bytes == 0 {
            return Some(self.charge_of(0));
        }
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes)
                    .filter(|next| *next <= self.capacity)
            })
            .ok()
            .map(|_| self.charge_of(bytes))
    }

    /// Charge the exact `bytes` of work the node already committed to, even above the
    /// capacity. Unsolicited work then refuses until this charge ends.
    ///
    /// The caller bounds this work itself. A TCP connection has one outstanding
    /// request and serves one request at a time.
    pub(crate) fn charge_committed(&self, bytes: u64) -> InboundCharge {
        let _ = self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                Some(used.saturating_add(bytes))
            });
        self.charge_of(bytes)
    }

    fn charge_of(&self, bytes: u64) -> InboundCharge {
        InboundCharge {
            used: self.used.clone(),
            bytes,
        }
    }

    /// Bytes currently charged.
    #[cfg(test)]
    pub(crate) fn used(&self) -> u64 {
        self.used.load(Ordering::Acquire)
    }

    /// Bytes that unsolicited work can still charge.
    #[cfg(test)]
    pub(crate) fn available(&self) -> u64 {
        self.capacity.saturating_sub(self.used())
    }
}

/// Bytes charged to one [`InboundBudget`] until this charge drops.
#[derive(Debug)]
pub(crate) struct InboundCharge {
    used: Arc<AtomicU64>,
    bytes: u64,
}

impl InboundCharge {
    /// Combine `other`, which must come from this charge's budget.
    pub(crate) fn merge(&mut self, mut other: InboundCharge) {
        debug_assert!(Arc::ptr_eq(&self.used, &other.used));
        self.bytes = self.bytes.saturating_add(other.bytes);
        other.bytes = 0;
    }
}

impl Drop for InboundCharge {
    fn drop(&mut self) {
        let _ = self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                Some(used.saturating_sub(self.bytes))
            });
    }
}

/// The inventory rows that reach the backend through the inbound service.
///
/// A slot covers one request's backend work. Its follow-up work has its own owner.
/// `zakurad`'s block `Downloads` and mempool `Downloads` own advertised fetches, and
/// the state's read workers and read result pools own lookups.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InboundRow {
    /// C.1 and a single-block TCP `inv`.
    AdvertiseBlock,
    /// C.2 and a transaction TCP `inv`.
    AdvertiseTransactionIds,
    /// C.3 and a block TCP `getdata`.
    BlocksByHash,
    /// C.4 and a transaction TCP `getdata`.
    TransactionsById,
    /// C.9 and TCP `getblocks`.
    FindBlocks,
    /// C.10 and TCP `getheaders`.
    FindHeaders,
    /// C.11 and L.mempool.
    MempoolTransactionIds,
}

impl InboundRow {
    const COUNT: usize = 7;

    /// The row that owns `request`, or `None` for work with another owner.
    ///
    /// The peer cache answers `Peers`, and the inbound byte budget funds a pushed
    /// transaction. The remaining requests never reach the inbound service.
    pub(crate) fn of(request: &Request) -> Option<Self> {
        match request {
            Request::AdvertiseBlock(..) => Some(Self::AdvertiseBlock),
            Request::AdvertiseTransactionIds(..) => Some(Self::AdvertiseTransactionIds),
            Request::BlocksByHash(_) | Request::BlocksByHashFrom { .. } => Some(Self::BlocksByHash),
            Request::TransactionsById(_) | Request::TransactionsByIdFrom { .. } => {
                Some(Self::TransactionsById)
            }
            Request::FindBlocks { .. } => Some(Self::FindBlocks),
            Request::FindHeaders { .. } => Some(Self::FindHeaders),
            Request::MempoolTransactionIds => Some(Self::MempoolTransactionIds),
            Request::Peers
            | Request::Ping(_)
            | Request::PushTransaction(..)
            | Request::AdvertiseBlockToAll(_)
            | Request::AdvertiseMinedBlock(_)
            | Request::DisconnectPeer(_) => None,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::AdvertiseBlock => "advertise_block",
            Self::AdvertiseTransactionIds => "advertise_transaction_ids",
            Self::BlocksByHash => "blocks_by_hash",
            Self::TransactionsById => "transactions_by_id",
            Self::FindBlocks => "find_blocks",
            Self::FindHeaders => "find_headers",
            Self::MempoolTransactionIds => "mempool_transaction_ids",
        }
    }

    fn index(self) -> usize {
        // A fieldless enum's discriminant is below `COUNT`, so it fits `usize`.
        self as usize
    }
}

/// The error a saturated row returns. It does not fault the peer.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("inbound row {0:?} is at capacity")]
pub(crate) struct InboundRowRefused(pub(crate) InboundRow);

/// Admits each request to its row's own slots before the backend sees it.
///
/// Clones share their slots. Every row has one slot per possible caller, because a
/// smaller row would refuse a conformant peer whenever enough other peers sync at once.
/// Each caller runs one request at a time, so these slots cannot refuse unless a
/// caller exceeds its own limit. This wrapper therefore states each row's aggregate
/// bound, and one row's saturation never spends another row's slots.
#[derive(Clone, Debug)]
pub(crate) struct InboundRowAdmission<S> {
    inner: S,
    rows: Arc<[Arc<Semaphore>; InboundRow::COUNT]>,
}

impl<S> InboundRowAdmission<S> {
    /// Wrap `inner` with `slots_per_row` slots in each row.
    pub(crate) fn new(inner: S, slots_per_row: usize) -> Self {
        Self {
            inner,
            rows: Arc::new(std::array::from_fn(|_| {
                Arc::new(Semaphore::new(slots_per_row))
            })),
        }
    }

    fn try_admit(&self, row: InboundRow) -> Option<OwnedSemaphorePermit> {
        self.rows[row.index()].clone().try_acquire_owned().ok()
    }
}

impl<S> Service<Request> for InboundRowAdmission<S>
where
    S: Service<Request, Response = Response, Error = BoxError> + Clone,
    S::Future: Send + 'static,
{
    type Response = Response;
    type Error = BoxError;
    type Future = BoxFuture<'static, Result<Response, BoxError>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), BoxError>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let slot = match InboundRow::of(&request) {
            Some(row) => match self.try_admit(row) {
                Some(slot) => Some(slot),
                None => {
                    metrics::counter!("zcash.net.in.row.refused", "row" => row.label())
                        .increment(1);
                    // `poll_ready` may have reserved a buffer slot in `inner`, and a
                    // refused request will not spend it. A fresh clone has no reservation.
                    self.inner = self.inner.clone();
                    return future::ready(Err(InboundRowRefused(row).into())).boxed();
                }
            },
            None => None,
        };
        let response = self.inner.call(request);
        async move {
            let response = response.await;
            drop(slot);
            response
        }
        .boxed()
    }
}

/// Aggregate owner of TCP handshakes that have not finished.
///
/// Each handshake holds one slot from its first byte until it ends or times out. A
/// slot bounds the handshake's codec buffer by the pre-handshake body limit, its
/// decoded `version` by the user-agent limit, and its phase metadata by one task.
/// The inbound and outbound connection counters reserve a connection before its
/// handshake starts, so a slot per connection cannot refuse a conformant peer.
#[derive(Clone, Debug)]
pub(crate) struct TcpHandshakeOwner {
    slots: Arc<Semaphore>,
}

impl TcpHandshakeOwner {
    /// Create an owner with one slot per connection, and at least one slot.
    pub(crate) fn new(connection_limit: usize) -> Self {
        Self {
            slots: Arc::new(Semaphore::new(connection_limit.max(1))),
        }
    }

    /// Take a slot now, or nothing.
    pub(crate) fn try_admit(&self) -> Option<OwnedSemaphorePermit> {
        self.slots.clone().try_acquire_owned().ok()
    }

    /// Slots no handshake holds.
    #[cfg(test)]
    pub(crate) fn available(&self) -> usize {
        self.slots.available_permits()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::atomic::AtomicUsize};

    use tokio::sync::oneshot;
    use tower::ServiceExt;
    use zakura_chain::block;

    use super::*;

    /// A budget admits within its capacity and refuses without changing its charge.
    #[test]
    fn a_saturated_budget_refuses_unsolicited_work() {
        let budget = InboundBudget::new(10);
        let held = budget.try_charge(6).expect("the budget is idle");
        assert!(budget.try_charge(5).is_none());
        assert_eq!(budget.used(), 6);
        drop(held);
        assert_eq!(budget.used(), 0);
    }

    /// A committed charge exceeds the capacity instead of refusing or waiting, and
    /// unsolicited work refuses until the committed charge ends.
    #[test]
    fn committed_work_is_charged_above_the_capacity() {
        let budget = InboundBudget::new(10);
        let mut committed = budget.charge_committed(25);
        committed.merge(budget.charge_committed(5));
        assert_eq!(budget.used(), 30);
        assert_eq!(budget.available(), 0);
        assert!(budget.try_charge(1).is_none());
        assert!(
            budget.try_charge(0).is_some(),
            "an empty item always enters"
        );
        drop(committed);
        assert_eq!(budget.used(), 0);
        assert!(budget.try_charge(1).is_some());
    }

    /// One request of each row with its inventory ID.
    fn rows() -> Vec<(&'static str, InboundRow, Request)> {
        let hash = block::Hash([1; 32]);
        vec![
            (
                "C.1",
                InboundRow::AdvertiseBlock,
                Request::AdvertiseBlock(hash, None),
            ),
            (
                "C.2",
                InboundRow::AdvertiseTransactionIds,
                Request::AdvertiseTransactionIds(HashSet::new(), None),
            ),
            (
                "C.3",
                InboundRow::BlocksByHash,
                Request::BlocksByHash([hash].into_iter().collect()),
            ),
            (
                "C.4",
                InboundRow::TransactionsById,
                Request::TransactionsById(HashSet::new()),
            ),
            (
                "C.9",
                InboundRow::FindBlocks,
                Request::FindBlocks {
                    known_blocks: vec![hash],
                    stop: None,
                },
            ),
            (
                "C.10",
                InboundRow::FindHeaders,
                Request::FindHeaders {
                    known_blocks: vec![hash],
                    stop: None,
                },
            ),
            (
                "C.11 and L.mempool",
                InboundRow::MempoolTransactionIds,
                Request::MempoolTransactionIds,
            ),
        ]
    }

    /// An inner service that answers `Nil` and counts its calls.
    fn counting_service(
        calls: Arc<AtomicUsize>,
    ) -> impl Service<
        Request,
        Response = Response,
        Error = BoxError,
        Future = future::Ready<Result<Response, BoxError>>,
    > + Clone {
        tower::service_fn(move |_request: Request| {
            calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            future::ready(Ok::<_, BoxError>(Response::Nil))
        })
    }

    /// Each row refuses with its own typed error once its slots run out, the backend
    /// never sees the refused request, and every other row stays open.
    #[tokio::test]
    async fn each_row_has_its_own_slots() {
        for (id, saturated, request) in rows() {
            let calls = Arc::new(AtomicUsize::new(0));
            let service = InboundRowAdmission::new(counting_service(calls.clone()), 1);
            let held = service.try_admit(saturated).expect("the row is idle");

            let refused = service.clone().oneshot(request).await.unwrap_err();
            assert_eq!(
                refused.downcast_ref::<InboundRowRefused>(),
                Some(&InboundRowRefused(saturated)),
                "{id}"
            );
            assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0, "{id}");

            for (other_id, other, other_request) in rows() {
                if other != saturated {
                    assert!(
                        service.clone().oneshot(other_request).await.is_ok(),
                        "{other_id} stays open while {id} is saturated"
                    );
                }
            }
            drop(held);
        }
    }

    /// Requests with another owner take no row slot.
    #[test]
    fn peers_and_pushed_transactions_have_other_owners() {
        assert_eq!(InboundRow::of(&Request::Peers), None);
        assert_eq!(InboundRow::of(&Request::Ping(Default::default())), None);
    }

    /// A slot lasts until the backend's response resolves, including for a request
    /// whose result is empty, such as a mempool snapshot of an empty mempool.
    #[tokio::test]
    async fn a_row_slot_lasts_until_the_response_resolves() {
        let (respond, response) = oneshot::channel::<()>();
        let response = Arc::new(std::sync::Mutex::new(Some(response)));
        let inner = tower::service_fn(move |_request: Request| {
            let response = response.lock().expect("the test does not panic").take();
            async move {
                response.expect("the test sends one request").await?;
                Ok::<_, BoxError>(Response::Nil)
            }
        });
        let service = InboundRowAdmission::new(inner, 1);
        let row = service.rows[InboundRow::MempoolTransactionIds.index()].clone();

        let pending = tokio::spawn(service.clone().oneshot(Request::MempoolTransactionIds));
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while row.available_permits() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the request takes its slot");

        respond.send(()).expect("the service is waiting");
        assert!(matches!(pending.await, Ok(Ok(Response::Nil))));
        assert_eq!(row.available_permits(), 1);
    }

    /// An inner service that holds a reservation from `poll_ready` to `call`, like a
    /// `Buffer`. Its clones hold no reservation.
    #[derive(Debug)]
    struct Reserving {
        capacity: Arc<Semaphore>,
        reservation: Option<OwnedSemaphorePermit>,
    }

    impl Clone for Reserving {
        fn clone(&self) -> Self {
            Self {
                capacity: self.capacity.clone(),
                reservation: None,
            }
        }
    }

    impl Service<Request> for Reserving {
        type Response = Response;
        type Error = BoxError;
        type Future = future::Ready<Result<Response, BoxError>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), BoxError>> {
            self.reservation = self.capacity.clone().try_acquire_owned().ok();
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _request: Request) -> Self::Future {
            self.reservation = None;
            future::ready(Ok(Response::Nil))
        }
    }

    /// A refusal returns the inner service's ready reservation, so an idle connection
    /// cannot keep a shared buffer slot.
    #[tokio::test]
    async fn a_refusal_returns_the_inner_reservation() {
        let capacity = Arc::new(Semaphore::new(1));
        let inner = Reserving {
            capacity: capacity.clone(),
            reservation: None,
        };
        let mut service = InboundRowAdmission::new(inner, 1);
        let _held = service
            .try_admit(InboundRow::MempoolTransactionIds)
            .expect("the row is idle");

        service.ready().await.expect("the inner service is ready");
        assert_eq!(capacity.available_permits(), 0);
        assert!(service.call(Request::MempoolTransactionIds).await.is_err());
        assert_eq!(capacity.available_permits(), 1);
    }

    /// L.handshake: the owner counts every unfinished handshake across its clones,
    /// and a zero connection limit still leaves one slot for isolated use.
    #[test]
    fn the_handshake_owner_bounds_handshakes_in_aggregate() {
        let owner = TcpHandshakeOwner::new(2);
        let clone = owner.clone();
        let first = owner.try_admit().expect("one of two slots");
        let _second = clone.try_admit().expect("two of two slots");
        assert!(owner.try_admit().is_none());
        assert_eq!(clone.available(), 0);
        drop(first);
        assert_eq!(owner.available(), 1);

        assert_eq!(TcpHandshakeOwner::new(0).available(), 1);
    }
}
