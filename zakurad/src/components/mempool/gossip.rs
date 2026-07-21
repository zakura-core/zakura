//! A task that gossips any [`zakura_chain::transaction::UnminedTxId`] that enters the mempool to peers.
//!
//! This module is just a function [`run_mempool_transaction_id_gossip`] that
//! treats mempool insertion events received in a channel as wakeup signals,
//! takes the transaction IDs that still need gossip from the mempool service,
//! and advertises them to peers. Failed advertisements restore IDs that remain
//! in the mempool, allowing retries without reintroducing removed transactions.

use std::collections::HashSet;

use tokio::sync::broadcast::{
    self,
    error::{RecvError, TryRecvError},
};
use tower::{timeout::Timeout, Service, ServiceExt};

use zakura_network::MAX_TX_INV_IN_SENT_MESSAGE;

use zakura_chain::transaction::UnminedTxId;
use zakura_network as zn;
use zakura_node_services::mempool::{MempoolChange, Request, Response};

use crate::{
    components::sync::{PEER_GOSSIP_DELAY, TIPS_RESPONSE_TIMEOUT},
    BoxError,
};

/// The maximum number of channel messages we will combine into a single peer broadcast.
pub const MAX_CHANGES_BEFORE_SEND: usize = 10;

// Safe because the protocol limit of 25,000 fits in usize on all targets.
const MAX_TX_INV_IN_SENT_MESSAGE_USIZE: usize = MAX_TX_INV_IN_SENT_MESSAGE as usize;

/// The number of mempool change notifications buffered for gossip subscribers.
///
/// Keep this close to the number of changes the gossip task can drain in one
/// broadcast, so sustained overload triggers lag recovery instead of building
/// up a large backlog of stale notifications.
pub(super) const MEMPOOL_CHANGE_CHANNEL_CAPACITY: usize = MAX_CHANGES_BEFORE_SEND * 4;

/// Runs continuously, gossiping new [`UnminedTxId`](zakura_chain::transaction::UnminedTxId) to peers.
///
/// Broadcasts any new [`UnminedTxId`](zakura_chain::transaction::UnminedTxId)s that
/// are stored in the mempool to multiple ready peers.
pub(crate) async fn run_mempool_transaction_id_gossip<ZN, ZM>(
    mut receiver: broadcast::Receiver<MempoolChange>,
    broadcast_network: ZN,
    mut mempool: ZM,
) -> Result<(), BoxError>
where
    ZN: Service<zn::Request, Response = zn::Response, Error = BoxError> + Send + Clone + 'static,
    ZN::Future: Send,
    ZM: Service<Request, Response = Response, Error = BoxError> + Send + Clone + 'static,
    ZM::Future: Send + 'static,
{
    info!("initializing transaction gossip task");

    // use the same timeout as tips requests,
    // so broadcasts don't delay the syncer too long
    let mut broadcast_network = Timeout::new(broadcast_network, TIPS_RESPONSE_TIMEOUT);
    let mut drain_pending_without_wakeup = false;

    loop {
        // This count is only used in logs. It is zero for a pending-set drain
        // that runs without consuming a channel notification.
        let mut combined_changes = 0;

        if !drain_pending_without_wakeup {
            combined_changes = 1;

            // once we get new data in the channel, drain pending transaction IDs
            // from the mempool service and broadcast them to peers.
            //
            // The channel is a wakeup signal. The mempool service keeps the
            // authoritative pending gossip set, so lagged wakeups can recover
            // without re-advertising the entire mempool.
            loop {
                match receiver.recv().await {
                    Ok(mempool_change) if mempool_change.is_added() => break,
                    Ok(_) => {
                        // ignore other changes, we only want to gossip added transactions
                        continue;
                    }
                    Err(RecvError::Lagged(skip_count)) => {
                        info!(
                            ?skip_count,
                            "dropped mempool changes before gossiping, draining pending transaction IDs"
                        );
                        metrics::counter!("mempool.gossip.lagged.events.total").increment(1);
                        metrics::counter!("mempool.gossip.lagged.messages.total")
                            .increment(skip_count);
                        // Exit the wait loop to re-advertise the pending transaction IDs.
                        break;
                    }
                    Err(closed @ RecvError::Closed) => Err(closed)?,
                }
            }

            // also consume wakeups that arrived shortly after this one,
            // but limit the number of changes so the loop terminates.
            while combined_changes <= MAX_CHANGES_BEFORE_SEND {
                match receiver.try_recv() {
                    Ok(mempool_change) if mempool_change.is_added() => {}
                    Ok(_) => {
                        // ignore other changes, we only want to gossip added transactions
                        continue;
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Lagged(skip_count)) => {
                        info!(
                            ?skip_count,
                            "dropped mempool changes before gossiping, draining pending transaction IDs"
                        );
                        metrics::counter!("mempool.gossip.lagged.events.total").increment(1);
                        metrics::counter!("mempool.gossip.lagged.messages.total")
                            .increment(skip_count);
                    }
                    Err(closed @ TryRecvError::Closed) => Err(closed)?,
                }

                combined_changes += 1;
            }
        } else {
            drain_pending_without_wakeup = false;
        }

        let (attempted_count, should_retry) = advertise_pending_mempool_transaction_ids(
            &mut mempool,
            &mut broadcast_network,
            combined_changes,
        )
        .await?;

        if attempted_count == 0 {
            continue;
        }

        // Retry failed advertisements, and keep draining after a full batch
        // because more transaction IDs may still be pending.
        drain_pending_without_wakeup = should_retry;

        // wait for at least the network timeout between gossips
        //
        // in practice, transactions arrive every 1-20 seconds,
        // so waiting 6 seconds can delay transaction propagation, in order to reduce peer load
        tokio::time::sleep(PEER_GOSSIP_DELAY).await;
    }
}

/// Advertise transaction IDs waiting in the mempool's pending gossip set.
async fn advertise_pending_mempool_transaction_ids<ZN, ZM>(
    mempool: &mut ZM,
    broadcast_network: &mut Timeout<ZN>,
    combined_changes: usize,
) -> Result<(u64, bool), BoxError>
where
    ZN: Service<zn::Request, Response = zn::Response, Error = BoxError> + Send + Clone + 'static,
    ZN::Future: Send,
    ZM: Service<Request, Response = Response, Error = BoxError> + Send + Clone + 'static,
    ZM::Future: Send + 'static,
{
    let Response::PendingGossipTransactionIds(tx_ids) = mempool
        .ready()
        .await?
        .call(Request::TakePendingGossipTransactionIds {
            limit: MAX_TX_INV_IN_SENT_MESSAGE_USIZE,
        })
        .await?
    else {
        return Err(std::io::Error::other(
            "mempool pending gossip request returned a different response variant",
        )
        .into());
    };

    if tx_ids.is_empty() {
        return Ok((0, false));
    }

    if tx_ids.len() > MAX_TX_INV_IN_SENT_MESSAGE_USIZE {
        return Err(std::io::Error::other(
            "mempool returned more pending gossip IDs than requested",
        )
        .into());
    }

    let txs_len: u64 = tx_ids
        .len()
        .try_into()
        .expect("bounded pending transaction ID count fits in u64");
    let retry_tx_ids = tx_ids.clone();
    let request = zn::Request::AdvertiseTransactionIds(tx_ids, None);

    info!(%request, changes = %combined_changes, "sending pending mempool transaction broadcast");
    debug!(
        ?request,
        changes = ?combined_changes,
        "full list of pending mempool transactions in broadcast"
    );

    let network = match broadcast_network.ready().await {
        Ok(network) => network,
        Err(error) => {
            requeue_pending_mempool_transaction_ids(mempool, retry_tx_ids).await?;
            return Err(error);
        }
    };

    if let Err(error) = network.call(request).await {
        warn!(
            ?error,
            transactions = txs_len,
            "failed to advertise pending mempool transactions, retrying"
        );
        metrics::counter!("mempool.gossip.failed.broadcasts.total").increment(1);

        requeue_pending_mempool_transaction_ids(mempool, retry_tx_ids).await?;
        return Ok((txs_len, true));
    }

    metrics::counter!("mempool.gossip.pending.transactions.total").increment(txs_len);
    metrics::counter!("mempool.gossiped.transactions.total").increment(txs_len);

    Ok((txs_len, txs_len == MAX_TX_INV_IN_SENT_MESSAGE))
}

/// Restore a failed advertisement batch to the mempool's pending gossip set.
async fn requeue_pending_mempool_transaction_ids<ZM>(
    mempool: &mut ZM,
    tx_ids: HashSet<UnminedTxId>,
) -> Result<(), BoxError>
where
    ZM: Service<Request, Response = Response, Error = BoxError> + Send + Clone + 'static,
    ZM::Future: Send + 'static,
{
    let response = mempool
        .ready()
        .await?
        .call(Request::RequeuePendingGossipTransactionIds(tx_ids))
        .await?;
    if !matches!(response, Response::RequeuedPendingGossipTransactionIds) {
        return Err(std::io::Error::other(
            "requeue pending transaction IDs request returned a different response variant",
        )
        .into());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashSet, VecDeque},
        sync::{Arc, Mutex},
        time::Duration,
    };

    use tokio::sync::{broadcast, mpsc};
    use tower::service_fn;

    use zakura_chain::transaction::{self, UnminedTxId};

    use super::*;

    fn test_tx_ids(count: usize, seed: u8) -> HashSet<UnminedTxId> {
        (0..count)
            .map(|index| {
                let index: u64 = index
                    .try_into()
                    .expect("test transaction ID index fits in u64");
                let mut bytes = [seed; 32];
                bytes[..8].copy_from_slice(&index.to_le_bytes());

                UnminedTxId::Legacy(transaction::Hash(bytes))
            })
            .collect()
    }

    fn mempool_service(
        pending_batches: Vec<HashSet<UnminedTxId>>,
    ) -> (
        impl Service<Request, Response = Response, Error = BoxError, Future: Send> + Clone,
        mpsc::Receiver<usize>,
    ) {
        let pending_batches = Arc::new(Mutex::new(VecDeque::from(pending_batches)));
        let (limit_sender, limit_receiver) = mpsc::channel(16);
        let service = service_fn(move |request| {
            let pending_batches = pending_batches.clone();
            let limit_sender = limit_sender.clone();

            async move {
                match request {
                    Request::TakePendingGossipTransactionIds { limit } => {
                        assert_eq!(
                            limit, MAX_TX_INV_IN_SENT_MESSAGE_USIZE,
                            "gossip task should bound each pending take to one inv"
                        );
                        limit_sender
                            .send(limit)
                            .await
                            .expect("limit receiver should be open");

                        let tx_ids = pending_batches
                            .lock()
                            .expect("pending batch mutex should not be poisoned")
                            .pop_front()
                            .unwrap_or_default();

                        Ok(Response::PendingGossipTransactionIds(tx_ids))
                    }
                    Request::RequeuePendingGossipTransactionIds(tx_ids) => {
                        let mut pending_batches = pending_batches
                            .lock()
                            .expect("pending batch mutex should not be poisoned");
                        pending_batches.push_front(tx_ids);

                        Ok(Response::RequeuedPendingGossipTransactionIds)
                    }
                    unexpected_request => {
                        panic!("unexpected mempool request: {unexpected_request:?}")
                    }
                }
            }
        });

        (service, limit_receiver)
    }

    fn peer_set_service() -> (
        impl Service<zn::Request, Response = zn::Response, Error = BoxError, Future: Send> + Clone,
        mpsc::Receiver<zn::Request>,
    ) {
        let (advertised_sender, advertised_receiver) = mpsc::channel(16);

        let service = service_fn(move |request| {
            let advertised_sender = advertised_sender.clone();

            async move {
                advertised_sender
                    .send(request)
                    .await
                    .expect("advertised request receiver should be open");

                Ok(zn::Response::Nil)
            }
        });

        (service, advertised_receiver)
    }

    async fn expect_advertised_transaction_ids(
        advertised_receiver: &mut mpsc::Receiver<zn::Request>,
    ) -> HashSet<UnminedTxId> {
        let advertised_request =
            tokio::time::timeout(Duration::from_secs(1), advertised_receiver.recv())
                .await
                .expect("gossip task should advertise pending mempool txids")
                .expect("peer set should advertise a request before the task exits");

        let zn::Request::AdvertiseTransactionIds(advertised_tx_ids, None) = advertised_request
        else {
            panic!("unexpected advertised request: {advertised_request:?}");
        };

        advertised_tx_ids
    }

    #[tokio::test]
    async fn added_mempool_gossip_drains_pending_transaction_ids() {
        let _init_guard = zakura_test::init();

        let pending_tx_ids = test_tx_ids(2, 1);
        let (mempool, mut limit_receiver) = mempool_service(vec![pending_tx_ids.clone()]);
        let (peer_set, mut advertised_receiver) = peer_set_service();
        let (sender, receiver) = broadcast::channel(MEMPOOL_CHANGE_CHANNEL_CAPACITY);

        sender
            .send(MempoolChange::added(test_tx_ids(1, 2)))
            .expect("receiver should be subscribed");

        let gossip_task = tokio::spawn(run_mempool_transaction_id_gossip(
            receiver, peer_set, mempool,
        ));

        assert_eq!(
            limit_receiver
                .recv()
                .await
                .expect("gossip task should request pending txids"),
            MAX_TX_INV_IN_SENT_MESSAGE_USIZE
        );
        assert_eq!(
            expect_advertised_transaction_ids(&mut advertised_receiver).await,
            pending_tx_ids,
            "happy path should advertise the pending mempool txids",
        );

        gossip_task.abort();
    }

    #[tokio::test]
    async fn lagged_mempool_gossip_drains_pending_transaction_ids() {
        let _init_guard = zakura_test::init();

        let pending_tx_ids = test_tx_ids(2, 1);
        let dropped_tx_ids = test_tx_ids(2, 2);
        let (mempool, mut limit_receiver) = mempool_service(vec![pending_tx_ids.clone()]);
        let (peer_set, mut advertised_receiver) = peer_set_service();
        let (sender, receiver) = broadcast::channel(1);

        let mut lagged_events = dropped_tx_ids
            .into_iter()
            .map(|tx_id| MempoolChange::added([tx_id].into_iter().collect()));

        sender
            .send(
                lagged_events
                    .next()
                    .expect("first lagged mempool change should exist"),
            )
            .expect("receiver should be subscribed");
        sender
            .send(
                lagged_events
                    .next()
                    .expect("second lagged mempool change should exist"),
            )
            .expect("receiver should be subscribed");

        let gossip_task = tokio::spawn(run_mempool_transaction_id_gossip(
            receiver, peer_set, mempool,
        ));

        assert_eq!(
            limit_receiver
                .recv()
                .await
                .expect("gossip task should request pending txids"),
            MAX_TX_INV_IN_SENT_MESSAGE_USIZE
        );
        assert_eq!(
            expect_advertised_transaction_ids(&mut advertised_receiver).await,
            pending_tx_ids,
            "lag recovery should advertise pending txids, not dropped channel payloads",
        );

        gossip_task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn lagged_mempool_gossip_recovers_pending_transaction_ids_in_bounded_cycles() {
        let _init_guard = zakura_test::init();

        let first_batch = test_tx_ids(MAX_TX_INV_IN_SENT_MESSAGE_USIZE, 1);
        let second_batch = test_tx_ids(2, 2);
        let (mempool, mut limit_receiver) =
            mempool_service(vec![first_batch.clone(), second_batch.clone()]);
        let (peer_set, mut advertised_receiver) = peer_set_service();
        let (sender, receiver) = broadcast::channel(1);

        sender
            .send(MempoolChange::added(
                [UnminedTxId::Legacy(transaction::Hash([42; 32]))]
                    .into_iter()
                    .collect(),
            ))
            .expect("receiver should be subscribed");
        sender
            .send(MempoolChange::added(
                [UnminedTxId::Legacy(transaction::Hash([43; 32]))]
                    .into_iter()
                    .collect(),
            ))
            .expect("receiver should be subscribed");

        let gossip_task = tokio::spawn(run_mempool_transaction_id_gossip(
            receiver, peer_set, mempool,
        ));

        assert_eq!(
            limit_receiver
                .recv()
                .await
                .expect("first drain should request pending txids"),
            MAX_TX_INV_IN_SENT_MESSAGE_USIZE
        );
        let advertised_tx_ids = expect_advertised_transaction_ids(&mut advertised_receiver).await;
        assert_eq!(advertised_tx_ids, first_batch);
        assert_eq!(
            advertised_tx_ids.len(),
            MAX_TX_INV_IN_SENT_MESSAGE_USIZE,
            "first recovery cycle should be bounded to one inv-sized batch",
        );

        tokio::time::advance(PEER_GOSSIP_DELAY).await;

        assert_eq!(
            limit_receiver
                .recv()
                .await
                .expect("second drain should happen without another wakeup"),
            MAX_TX_INV_IN_SENT_MESSAGE_USIZE
        );
        let advertised_tx_ids = expect_advertised_transaction_ids(&mut advertised_receiver).await;
        assert_eq!(advertised_tx_ids, second_batch);
        assert!(
            advertised_tx_ids.len() < MAX_TX_INV_IN_SENT_MESSAGE_USIZE,
            "second recovery cycle should only advertise the remaining txids",
        );

        gossip_task.abort();
    }

    #[tokio::test]
    async fn failed_broadcast_keeps_transaction_ids_pending_for_retry() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let _init_guard = zakura_test::init();

        let pending_tx_ids = test_tx_ids(2, 1);
        let (mut mempool, _limit_receiver) = mempool_service(vec![pending_tx_ids.clone()]);
        let attempts = Arc::new(AtomicUsize::new(0));
        let peer_set = service_fn({
            let attempts = Arc::clone(&attempts);
            move |_request| {
                let attempts = Arc::clone(&attempts);
                async move {
                    if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        Err(std::io::Error::other("transient peer-set failure").into())
                    } else {
                        Ok(zn::Response::Nil)
                    }
                }
            }
        });
        let mut peer_set = Timeout::new(peer_set, Duration::from_secs(1));
        let pending_count: u64 = pending_tx_ids
            .len()
            .try_into()
            .expect("test transaction count fits in u64");

        let (attempted_count, should_retry) =
            advertise_pending_mempool_transaction_ids(&mut mempool, &mut peer_set, 1)
                .await
                .expect("failed advertisements should remain retryable");
        assert_eq!(attempted_count, pending_count);
        assert!(should_retry, "failed advertisements should be retried");

        let (advertised_count, should_retry) =
            advertise_pending_mempool_transaction_ids(&mut mempool, &mut peer_set, 0)
                .await
                .expect("the retry should succeed");
        assert_eq!(advertised_count, pending_count);
        assert!(
            !should_retry,
            "a successful partial batch should wait for another wakeup"
        );

        let response = mempool
            .ready()
            .await
            .expect("mempool should be ready")
            .call(Request::TakePendingGossipTransactionIds {
                limit: MAX_TX_INV_IN_SENT_MESSAGE_USIZE,
            })
            .await
            .expect("pending gossip query should succeed");
        let Response::PendingGossipTransactionIds(remaining_tx_ids) = response else {
            panic!("pending gossip query returned a different response variant");
        };
        assert!(
            remaining_tx_ids.is_empty(),
            "successfully advertised transaction IDs should be acknowledged"
        );
    }

    #[tokio::test]
    async fn empty_pending_mempool_gossip_wakeup_does_not_advertise() {
        let _init_guard = zakura_test::init();

        let (mempool, mut limit_receiver) = mempool_service(vec![HashSet::new()]);
        let (peer_set, mut advertised_receiver) = peer_set_service();
        let (sender, receiver) = broadcast::channel(MEMPOOL_CHANGE_CHANNEL_CAPACITY);

        sender
            .send(MempoolChange::added(test_tx_ids(1, 1)))
            .expect("receiver should be subscribed");

        let gossip_task = tokio::spawn(run_mempool_transaction_id_gossip(
            receiver, peer_set, mempool,
        ));

        assert_eq!(
            limit_receiver
                .recv()
                .await
                .expect("gossip task should request pending txids"),
            MAX_TX_INV_IN_SENT_MESSAGE_USIZE
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), advertised_receiver.recv())
                .await
                .is_err(),
            "empty pending gossip wakeups should not advertise to peers",
        );
        assert!(
            !gossip_task.is_finished(),
            "gossip task should remain alive after an empty pending drain",
        );

        gossip_task.abort();
    }
}
