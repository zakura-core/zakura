//! Pace legacy block advertisements using the corresponding block writes.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

use futures::{
    channel::{mpsc, oneshot},
    SinkExt,
};
use tokio::{
    sync::{watch, Semaphore},
    time::{sleep_until, timeout, Instant},
};
use zakura_chain::block;

use crate::{
    constants::REQUEST_TIMEOUT, peer::ClientRequest, peer_registry::PeerRegistryUpdater, Request,
};

/// Maximum outstanding legacy block advertisements across broadcasts.
pub(crate) const BLOCK_GOSSIP_CONCURRENCY: usize = 32;
/// Allow the kernel send queue to drain after the framed block write completes.
const BLOCK_WRITE_GRACE: Duration = Duration::from_millis(750);
/// Peers that already have a block might never request it.
const BLOCK_REQUEST_WAIT: Duration = Duration::from_secs(3);
/// Bound both inbound service work and the subsequent block write.
const BLOCK_UPLOAD_WAIT: Duration = Duration::from_secs(2 * REQUEST_TIMEOUT.as_secs());

#[derive(Clone, Copy, Debug)]
enum UploadState {
    Waiting,
    Requested(Instant),
    Written(Instant),
    Finished(bool),
}

/// Observers exist only while local broadcasts await this connection's uploads.
#[derive(Clone, Debug, Default)]
pub(crate) struct BlockUploads(Arc<Mutex<HashMap<block::Hash, Weak<watch::Sender<UploadState>>>>>);

impl BlockUploads {
    fn register(&self, hash: block::Hash) -> (watch::Receiver<UploadState>, Option<UploadClaim>) {
        let mut uploads = self.0.lock().expect("block upload mutex is never poisoned");
        uploads.retain(|_, upload| upload.strong_count() > 0);
        if let Some(upload) = uploads.get(&hash).and_then(Weak::upgrade) {
            return (upload.subscribe(), None);
        }
        let (sender, receiver) = watch::channel(UploadState::Waiting);
        let sender = Arc::new(sender);
        uploads.insert(hash, Arc::downgrade(&sender));
        (receiver, Some(UploadClaim(sender)))
    }

    fn update(&self, hash: block::Hash, state: UploadState) {
        let uploads = self.0.lock().expect("block upload mutex is never poisoned");
        if let Some(upload) = uploads.get(&hash).and_then(Weak::upgrade) {
            upload.send_if_modified(|current| {
                let replace = matches!(*current, UploadState::Waiting)
                    || matches!(
                        (*current, state),
                        (
                            UploadState::Requested(_),
                            UploadState::Written(_) | UploadState::Finished(_)
                        )
                    );
                if replace {
                    *current = state;
                }
                replace
            });
        }
    }

    pub(crate) fn requested(&self, hash: block::Hash) {
        self.update(hash, UploadState::Requested(Instant::now()));
    }

    pub(crate) fn written(&self, hash: block::Hash) {
        self.update(hash, UploadState::Written(Instant::now()));
    }

    pub(crate) fn unavailable(&self, hash: block::Hash) {
        self.update(hash, UploadState::Finished(false));
    }

    pub(crate) fn disconnected(&self) {
        for upload in self
            .0
            .lock()
            .expect("block upload mutex is never poisoned")
            .values()
            .filter_map(Weak::upgrade)
        {
            upload.send_replace(UploadState::Finished(false));
        }
    }
}

struct UploadClaim(Arc<watch::Sender<UploadState>>);

impl Drop for UploadClaim {
    fn drop(&mut self) {
        self.0.send_if_modified(|state| {
            if matches!(state, UploadState::Finished(_)) {
                false
            } else {
                *state = UploadState::Finished(false);
                true
            }
        });
    }
}

/// A bounded request sender that remains usable while the peer set marks a client busy.
#[derive(Clone, Debug)]
pub(crate) struct BlockGossipPeer {
    pub(crate) sender: mpsc::Sender<ClientRequest>,
    pub(crate) uploads: BlockUploads,
    pub(crate) registry: Option<PeerRegistryUpdater>,
}

impl BlockGossipPeer {
    pub(crate) fn rtt(&self) -> Option<Duration> {
        self.registry.as_ref().and_then(PeerRegistryUpdater::rtt)
    }

    /// Keep the permit through getdata, the block write, and the drain allowance.
    pub(crate) async fn advertise(mut self, hash: block::Hash, slots: Arc<Semaphore>) -> bool {
        let (mut progress, claim) = self.uploads.register(hash);
        let Some(claim) = claim else {
            // Early and committed announcements join an existing upload for this peer.
            loop {
                if let UploadState::Finished(succeeded) = *progress.borrow_and_update() {
                    return succeeded;
                }
                if progress.changed().await.is_err() {
                    return false;
                }
            }
        };
        let Ok(_permit) = slots.acquire().await else {
            return false;
        };
        let (tx, rx) = oneshot::channel();
        let request = ClientRequest {
            request: Request::AdvertiseBlock(hash, None),
            tx,
            inv_collector: None,
            transient_addr: None,
            span: tracing::Span::current(),
        };
        // A busy connection can finish its current request before writing this inventory.
        let advertised = timeout(BLOCK_UPLOAD_WAIT, async {
            self.sender.send(request).await.ok()?;
            rx.await.ok()?.ok()
        })
        .await;
        if !matches!(advertised, Ok(Some(_))) {
            return false;
        }

        let request_deadline = Instant::now() + BLOCK_REQUEST_WAIT;
        let succeeded = loop {
            let state = *progress.borrow_and_update();
            let deadline = match state {
                UploadState::Waiting => request_deadline,
                UploadState::Requested(started) => started + BLOCK_UPLOAD_WAIT,
                UploadState::Written(written) => {
                    sleep_until(written + BLOCK_WRITE_GRACE).await;
                    break true;
                }
                UploadState::Finished(succeeded) => break succeeded,
            };
            tokio::select! {
                biased;
                changed = progress.changed() => if changed.is_err() { break false; },
                _ = sleep_until(deadline) => {
                    let reason = if matches!(state, UploadState::Waiting) { "no_request" } else { "upload_timeout" };
                    metrics::counter!("peer.block_gossip.slot_timeout", "reason" => reason).increment(1);
                    // Advertisement completion does not promise that the peer requested the block.
                    break true;
                }
            }
        };
        claim.0.send_replace(UploadState::Finished(succeeded));
        succeeded
    }
}

/// Randomize relays and equal RTTs; prioritize configured sidecars within the cap.
pub(crate) fn order_peers(
    peers: Vec<(bool, BlockGossipPeer)>,
    mined: bool,
    rng: &mut impl rand::Rng,
) -> Vec<BlockGossipPeer> {
    use rand::seq::SliceRandom;
    // Snapshot RTT once: heartbeat updates must not change comparisons during sorting.
    let mut peers: Vec<_> = peers
        .into_iter()
        .map(|(sidecar, peer)| {
            let rtt = if mined {
                peer.rtt().unwrap_or(Duration::MAX)
            } else {
                Duration::ZERO
            };
            (sidecar, rtt, peer)
        })
        .collect();
    peers.shuffle(rng);
    peers.sort_by_key(|(sidecar, rtt, _)| (!sidecar, *rtt));
    peers.into_iter().map(|(_, _, peer)| peer).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{peer_registry::PeerRegistry, ConnectedPeer, Response};
    use futures::{FutureExt, StreamExt};
    use rand::{rngs::StdRng, SeedableRng};

    fn peer() -> (BlockGossipPeer, mpsc::Receiver<ClientRequest>) {
        let (sender, receiver) = mpsc::channel(0);
        (
            BlockGossipPeer {
                sender,
                uploads: BlockUploads::default(),
                registry: None,
            },
            receiver,
        )
    }

    async fn accept(receiver: &mut mpsc::Receiver<ClientRequest>, hash: block::Hash) {
        let request = receiver
            .next()
            .await
            .expect("peer receives an advertisement");
        assert_eq!(request.request, Request::AdvertiseBlock(hash, None));
        request.tx.send(Ok(Response::Nil)).unwrap();
        tokio::task::yield_now().await;
    }

    #[tokio::test(start_paused = true)]
    async fn inventory_does_not_release_slot_and_upload_holds_it_through_grace() {
        let hash = block::Hash([1; 32]);
        let slots = Arc::new(Semaphore::new(1));
        let (first, mut first_rx) = peer();
        let uploads = first.uploads.clone();
        let (second, mut second_rx) = peer();
        let first_task = tokio::spawn(first.advertise(hash, slots.clone()));
        accept(&mut first_rx, hash).await;
        let second_task = tokio::spawn(second.advertise(hash, slots.clone()));
        tokio::task::yield_now().await;
        assert!(second_rx.next().now_or_never().is_none());
        uploads.requested(hash);
        tokio::time::advance(BLOCK_REQUEST_WAIT + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert!(
            second_rx.next().now_or_never().is_none(),
            "getdata keeps the slot occupied beyond the no-request timeout"
        );
        uploads.written(hash);
        tokio::task::yield_now().await;
        tokio::time::advance(BLOCK_WRITE_GRACE - Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert!(second_rx.next().now_or_never().is_none());
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(first_task.await.unwrap());
        accept(&mut second_rx, hash).await;
        assert!(second_task.await.unwrap());
        assert_eq!(slots.available_permits(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn no_request_timeout_releases_a_slot() {
        let hash = block::Hash([2; 32]);
        let slots = Arc::new(Semaphore::new(1));
        let (first, mut first_rx) = peer();
        let task = tokio::spawn(first.advertise(hash, slots.clone()));
        accept(&mut first_rx, hash).await;
        tokio::time::advance(BLOCK_REQUEST_WAIT - Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        assert_eq!(slots.available_permits(), 0);
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(task.await.unwrap());
        assert_eq!(slots.available_permits(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn duplicate_announcements_share_upload_and_slot() {
        let hash = block::Hash([3; 32]);
        let slots = Arc::new(Semaphore::new(1));
        let (peer, mut receiver) = peer();
        let first = tokio::spawn(peer.clone().advertise(hash, slots.clone()));
        accept(&mut receiver, hash).await;
        let second = tokio::spawn(peer.clone().advertise(hash, slots.clone()));
        tokio::task::yield_now().await;
        assert!(receiver.next().now_or_never().is_none());
        peer.uploads.requested(hash);
        peer.uploads.written(hash);
        assert!(first.await.unwrap());
        assert!(second.await.unwrap());
        assert_eq!(slots.available_permits(), 1);
        assert!(receiver.next().now_or_never().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_wakes_duplicate_and_returns_permit() {
        let hash = block::Hash([4; 32]);
        let slots = Arc::new(Semaphore::new(1));
        let (peer, mut receiver) = peer();
        let first = tokio::spawn(peer.clone().advertise(hash, slots.clone()));
        accept(&mut receiver, hash).await;
        let duplicate = tokio::spawn(peer.advertise(hash, slots.clone()));
        tokio::task::yield_now().await;
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        assert!(!duplicate.await.unwrap());
        assert_eq!(slots.available_permits(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn upload_timeout_and_disconnect_release_slots() {
        for disconnect in [false, true] {
            let hash = block::Hash([5; 32]);
            let slots = Arc::new(Semaphore::new(1));
            let (peer, mut receiver) = peer();
            let uploads = peer.uploads.clone();
            let task = tokio::spawn(peer.advertise(hash, slots.clone()));
            accept(&mut receiver, hash).await;
            uploads.requested(hash);
            tokio::task::yield_now().await;
            if disconnect {
                uploads.disconnected();
            } else {
                tokio::time::advance(BLOCK_UPLOAD_WAIT).await;
            }
            assert_eq!(task.await.unwrap(), !disconnect);
            assert_eq!(slots.available_permits(), 1);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_hashes_share_the_cap() {
        let slots = Arc::new(Semaphore::new(BLOCK_GOSSIP_CONCURRENCY));
        let mut tasks = Vec::new();
        let mut receivers = Vec::new();
        for i in 0..BLOCK_GOSSIP_CONCURRENCY + 1 {
            let (peer, receiver) = peer();
            tasks.push(tokio::spawn(peer.advertise(
                block::Hash([u8::try_from(i).unwrap(); 32]),
                slots.clone(),
            )));
            receivers.push(receiver);
        }
        tokio::task::yield_now().await;
        for (i, receiver) in receivers
            .iter_mut()
            .take(BLOCK_GOSSIP_CONCURRENCY)
            .enumerate()
        {
            accept(receiver, block::Hash([u8::try_from(i).unwrap(); 32])).await;
        }
        assert!(receivers
            .last_mut()
            .unwrap()
            .next()
            .now_or_never()
            .is_none());
        assert_eq!(slots.available_permits(), 0);
        for task in tasks {
            task.abort();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn unavailable_early_block_can_be_advertised_again_after_commit() {
        let hash = block::Hash([6; 32]);
        let slots = Arc::new(Semaphore::new(1));
        let (peer, mut receiver) = peer();
        let early = tokio::spawn(peer.clone().advertise(hash, slots.clone()));
        accept(&mut receiver, hash).await;
        peer.uploads.requested(hash);
        peer.uploads.unavailable(hash);
        assert!(!early.await.unwrap());
        let committed = tokio::spawn(peer.clone().advertise(hash, slots.clone()));
        accept(&mut receiver, hash).await;
        peer.uploads.requested(hash);
        peer.uploads.written(hash);
        assert!(committed.await.unwrap());
        assert_eq!(slots.available_permits(), 1);
    }

    #[test]
    fn miners_use_rtt_and_relays_use_random_order() {
        let registry = PeerRegistry::default();
        let mut guards = Vec::new();
        let peers: Vec<_> = [Some(80), Some(10), None, Some(30), Some(10)]
            .into_iter()
            .enumerate()
            .map(|(i, millis)| {
                let (mut peer, _receiver) = peer();
                let (guard, updater) = registry.register_legacy(ConnectedPeer {
                    addr: format!("127.0.0.1:{}", i + 1).parse().unwrap(),
                    user_agent: "test".into(),
                    version: crate::constants::CURRENT_NETWORK_PROTOCOL_VERSION,
                    is_inbound: false,
                    rtt: millis.map(Duration::from_millis),
                    ping_sent_at: None,
                });
                guards.push(guard);
                peer.registry = Some(updater);
                (false, peer)
            })
            .collect();
        let mined = order_peers(peers.clone(), true, &mut StdRng::seed_from_u64(9));
        let measured: Vec<_> = mined.iter().map(BlockGossipPeer::rtt).collect();
        assert_eq!(
            measured,
            [Some(10), Some(10), Some(30), Some(80), None]
                .map(|value| value.map(Duration::from_millis))
        );
        let relay = order_peers(peers, false, &mut StdRng::seed_from_u64(9));
        assert_ne!(
            relay.iter().map(BlockGossipPeer::rtt).collect::<Vec<_>>(),
            measured
        );
    }
}
