//! Block advertisements to Zakura peers, with the contract the gossip producer relies on.
//!
//! The legacy advertiser is ready only when a peer can take an advertisement, reports an
//! advertisement that reached nobody as [`NoEligibleBlockPeers`] so the producer keeps the block
//! and waits for the next peer, and reports any other error as a failed delivery. This service
//! gives the native side the same contract on top of the legacy gossip sessions, so a node with
//! only Zakura peers announces its tip and its own mined block to the first peer that connects
//! instead of recording them as sent.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures::FutureExt;
use tower::Service;
use zakura_chain::block;

use crate::{
    peer::{block_gossip::Readiness, NoEligibleBlockPeers},
    protocol::internal::{Request, Response},
    BoxError,
};

use super::{BlockFanout, LegacyGossipFrame, ZakuraGossipBroadcast, ZakuraSupervisorHandle};

use super::BLOCK_QUEUE_WAIT as QUEUE_WAIT;

/// Tower service that announces blocks to the Zakura peers with open gossip sessions.
///
/// Ready once a session can take a frame, which may be a session with a full queue, like a busy
/// legacy peer: a full queue gets [`QUEUE_WAIT`] to make room before the session counts as
/// having refused the block. A block is recorded as seen, and kept for replay to later peers,
/// only once a session queued it; a block already seen, sent earlier or relayed from a peer, is
/// not sent again. A submitted block that the chain has not committed yet is never kept for
/// replay, since the chain can still reject it; once it commits, its announcement keeps it for
/// replay even though it is not sent again. A session added while a full queue was making room
/// is offered the block once it is delivered, since its replay on joining came too early for
/// it, unless a later block has been remembered since. Requests other than block
/// advertisements fail.
pub(crate) struct ZakuraBlockAdvertiser {
    broadcast: ZakuraGossipBroadcast,
    readiness: Readiness,
    /// How long a full session queue gets to make room.
    queue_wait: Duration,
}

impl ZakuraBlockAdvertiser {
    /// Returns an advertiser for the gossip sessions of the given supervisor's peers.
    pub(crate) fn new(supervisor: ZakuraSupervisorHandle) -> Self {
        Self::from_broadcast(ZakuraGossipBroadcast::new(supervisor), QUEUE_WAIT)
    }

    fn from_broadcast(broadcast: ZakuraGossipBroadcast, queue_wait: Duration) -> Self {
        Self {
            broadcast,
            readiness: Readiness::Idle,
            queue_wait,
        }
    }

    /// Resolves once a session can take a frame.
    fn wait_for_session(&self) -> Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send>> {
        let outbound = self.broadcast.outbound.clone();
        // Subscribed before the first check, so a session inserted between the check and the
        // wait still wakes it.
        let mut session_count = outbound.subscribe_session_count();
        Box::pin(async move {
            loop {
                if outbound.has_open_session() {
                    return Ok(());
                }
                session_count
                    .changed()
                    .await
                    .map_err(|_| "the Zakura gossip session set was dropped")?;
            }
        })
    }

    /// Sends the block to the open sessions, keeping a committed block for replay to later peers.
    async fn advertise(
        broadcast: ZakuraGossipBroadcast,
        hash: block::Hash,
        committed: bool,
        queue_wait: Duration,
    ) -> Result<Response, BoxError> {
        let frame = LegacyGossipFrame::AdvertiseBlock(hash);
        let advertisement = broadcast.outbound.issue_advertisement();
        if broadcast.unseen(&frame).await.is_none() {
            // The block was already sent, as this node's submission or relayed from a peer. Now
            // that it has committed, it is still the block a later peer should be sent.
            if committed {
                broadcast
                    .outbound
                    .remember_latest_block(advertisement, &frame);
            }
            return Ok(Response::Nil);
        }

        let BlockFanout { reached, offered } = broadcast
            .outbound
            .fan_out_block_within(hash, queue_wait)
            .await;
        if reached.delivered > 0 {
            broadcast.mark_seen(&frame).await;
            if committed {
                broadcast
                    .outbound
                    .remember_latest_block(advertisement, &frame);
            }
            // A session added while a full queue was making room was replayed the block before
            // this one, or nothing, and a later advertisement of this block is not sent again.
            // Unless a later advertisement's block has been remembered since: then that is what
            // the session was replayed, and this block is older than what it has.
            if !broadcast.outbound.is_superseded(advertisement) {
                broadcast
                    .outbound
                    .send_block_to_sessions_added_since(&offered, hash, queue_wait)
                    .await;
            }
            return Ok(Response::Nil);
        }
        if reached.attempted == 0
            || reached.reached_only_closed_sessions()
            || !broadcast.outbound.has_open_session()
        {
            // Nobody received it: the same as having had nobody to send to. A session added
            // meanwhile was not offered the block; it makes the next advertisement ready at once.
            return Err(Box::new(NoEligibleBlockPeers) as BoxError);
        }
        let refusal = reached
            .first_error
            .expect("a session that was attempted but did not deliver recorded its refusal");
        Err(refusal.into())
    }
}

impl Clone for ZakuraBlockAdvertiser {
    /// Clones the handle without its wait: each handle establishes its own readiness.
    fn clone(&self) -> Self {
        Self::from_broadcast(self.broadcast.clone(), self.queue_wait)
    }
}

impl fmt::Debug for ZakuraBlockAdvertiser {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ZakuraBlockAdvertiser")
            .field("sessions", &self.broadcast.outbound.session_count())
            .field("readiness", &self.readiness.name())
            .finish()
    }
}

impl Service<Request> for ZakuraBlockAdvertiser {
    type Response = Response;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Response, BoxError>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), BoxError>> {
        loop {
            match &mut self.readiness {
                // Latched, so repeated readiness polls do not restart the wait.
                Readiness::Granted => return Poll::Ready(Ok(())),
                Readiness::Idle => self.readiness = Readiness::Waiting(self.wait_for_session()),
                Readiness::Waiting(waiting) => match waiting.poll_unpin(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(())) => self.readiness = Readiness::Granted,
                    Poll::Ready(Err(error)) => {
                        self.readiness = Readiness::Idle;
                        return Poll::Ready(Err(error));
                    }
                },
            }
        }
    }

    fn call(&mut self, request: Request) -> Self::Future {
        // A grant covers one advertisement: the next one waits for its own session.
        self.readiness = Readiness::Idle;
        let block = match request {
            Request::AdvertiseBlock(block, _)
            | Request::AdvertiseBlockToAll(block)
            | Request::AdvertiseMinedBlock(block) => block,
            other => {
                return futures::future::ready(Err(format!(
                    "block advertiser cannot send {}",
                    other.command()
                )
                .into()))
                .boxed()
            }
        };
        let broadcast = self.broadcast.clone();
        Box::pin(Self::advertise(
            broadcast,
            block.hash,
            !block.submitted,
            self.queue_wait,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::timeout;
    use tower::{Service, ServiceExt};
    use zakura_chain::{block, transaction::UnminedTxId};

    use crate::{
        peer::NoEligibleBlockPeers,
        protocol::internal::{AdvertisedBlock, Request, Response},
        zakura::{
            framed_channel,
            legacy_gossip::{
                FirstSeenCache, LegacyGossipFrame, LegacyGossipOutbound, LegacyGossipPeerSession,
                ZakuraGossipBroadcast, DEFAULT_FIRST_SEEN_CAPACITY, DEFAULT_FIRST_SEEN_TTL,
            },
            FramedRecv, ZakuraPeerId,
        },
        BoxError,
    };

    use super::ZakuraBlockAdvertiser;

    const SHORT_WAIT: Duration = Duration::from_millis(50);
    const RECEIVE_TIMEOUT: Duration = Duration::from_secs(1);

    fn block_hash(byte: u8) -> block::Hash {
        block::Hash([byte; 32])
    }

    /// How long a full queue gets to make room in these tests.
    const TEST_QUEUE_WAIT: Duration = Duration::from_millis(200);

    fn advertiser() -> (ZakuraBlockAdvertiser, ZakuraGossipBroadcast) {
        let broadcast = ZakuraGossipBroadcast {
            first_seen: FirstSeenCache::new(DEFAULT_FIRST_SEEN_CAPACITY, DEFAULT_FIRST_SEEN_TTL),
            outbound: LegacyGossipOutbound::default(),
        };
        (
            ZakuraBlockAdvertiser::from_broadcast(broadcast.clone(), TEST_QUEUE_WAIT),
            broadcast,
        )
    }

    /// Inserts a session for peer `byte` with a queue of `capacity` frames.
    fn insert_session(
        outbound: &LegacyGossipOutbound,
        byte: u8,
        capacity: usize,
    ) -> (LegacyGossipPeerSession, FramedRecv) {
        let peer_id = ZakuraPeerId::new(vec![byte; 32]).expect("test peer id is within bounds");
        let (sender, receiver) = framed_channel(capacity);
        let session = LegacyGossipPeerSession::new(peer_id, 0, 0, sender);
        assert!(
            outbound.insert(session.clone()),
            "fresh session is accepted"
        );
        (session, receiver)
    }

    async fn recv_advert(receiver: &mut FramedRecv) -> Result<block::Hash, BoxError> {
        let frame = timeout(RECEIVE_TIMEOUT, receiver.recv())
            .await?
            .ok_or("gossip session closed")?;
        match LegacyGossipFrame::decode_frame(frame)? {
            LegacyGossipFrame::AdvertiseBlock(hash) => Ok(hash),
            frame => Err(format!("unexpected frame {frame:?}").into()),
        }
    }

    fn latest_block(outbound: &LegacyGossipOutbound) -> Option<block::Hash> {
        outbound
            .latest_block
            .lock()
            .expect("legacy gossip latest-block mutex is never poisoned")
            .hash
    }

    async fn is_unseen(broadcast: &ZakuraGossipBroadcast, hash: block::Hash) -> bool {
        broadcast
            .unseen(&LegacyGossipFrame::AdvertiseBlock(hash))
            .await
            .is_some()
    }

    #[tokio::test]
    async fn native_block_advertiser_is_pending_until_a_session_exists() -> Result<(), BoxError> {
        let (mut advertiser, broadcast) = advertiser();

        assert!(
            timeout(SHORT_WAIT, advertiser.ready()).await.is_err(),
            "no session, so the advertiser is not ready"
        );

        let (_session, mut receiver) = insert_session(&broadcast.outbound, 1, 1);
        let hash = block_hash(1);
        timeout(RECEIVE_TIMEOUT, advertiser.ready())
            .await??
            .call(Request::AdvertiseBlockToAll(hash.into()))
            .await?;

        assert_eq!(recv_advert(&mut receiver).await?, hash);
        Ok(())
    }

    #[tokio::test]
    async fn native_block_advertiser_fails_with_no_eligible_peers_after_its_session_left(
    ) -> Result<(), BoxError> {
        let (mut advertiser, broadcast) = advertiser();
        let (session, _receiver) = insert_session(&broadcast.outbound, 2, 1);
        timeout(RECEIVE_TIMEOUT, advertiser.ready()).await??;
        broadcast.outbound.remove(session.peer_id(), 0);

        let hash = block_hash(2);
        let error = advertiser
            .call(Request::AdvertiseBlockToAll(hash.into()))
            .await
            .expect_err("nobody received the advertisement");
        assert!(
            error.is::<NoEligibleBlockPeers>(),
            "an advertisement that reached nobody is a missing peer, got {error}"
        );
        assert!(
            is_unseen(&broadcast, hash).await,
            "an undelivered block is not recorded as seen"
        );
        assert_eq!(
            latest_block(&broadcast.outbound),
            None,
            "an undelivered block is not kept for replay"
        );
        Ok(())
    }

    #[tokio::test]
    async fn native_block_advertiser_fails_with_no_eligible_peers_when_its_only_session_closed(
    ) -> Result<(), BoxError> {
        let (mut advertiser, broadcast) = advertiser();
        let (_session, receiver) = insert_session(&broadcast.outbound, 3, 1);
        timeout(RECEIVE_TIMEOUT, advertiser.ready()).await??;
        // The writer is gone but the session is still in the set, as it is until its stream
        // reports the close.
        drop(receiver);

        let error = advertiser
            .call(Request::AdvertiseBlockToAll(block_hash(3).into()))
            .await
            .expect_err("a closed session cannot receive the advertisement");
        assert!(
            error.is::<NoEligibleBlockPeers>(),
            "a session whose writer closed is no peer, got {error}"
        );
        assert!(
            timeout(SHORT_WAIT, advertiser.ready()).await.is_err(),
            "a closed session does not grant readiness"
        );
        Ok(())
    }

    #[tokio::test]
    async fn native_block_advertiser_reports_a_send_error_while_a_session_remains(
    ) -> Result<(), BoxError> {
        let (mut advertiser, broadcast) = advertiser();
        let (_session, mut receiver) = insert_session(&broadcast.outbound, 4, 1);
        let earlier = block_hash(40);
        timeout(RECEIVE_TIMEOUT, advertiser.ready())
            .await??
            .call(Request::AdvertiseBlockToAll(earlier.into()))
            .await?;

        // The queue of one stays full for longer than the wait: the session is there but
        // refuses the next frame.
        let hash = block_hash(4);
        let error = timeout(RECEIVE_TIMEOUT, advertiser.ready())
            .await??
            .call(Request::AdvertiseBlockToAll(hash.into()))
            .await
            .expect_err("a full queue refuses the advertisement");
        assert!(
            !error.is::<NoEligibleBlockPeers>(),
            "a refusal by a connected session is a failed delivery, got {error}"
        );
        assert!(
            is_unseen(&broadcast, hash).await,
            "a refused block is not recorded as seen"
        );
        assert_eq!(latest_block(&broadcast.outbound), Some(earlier));

        assert_eq!(recv_advert(&mut receiver).await?, earlier);
        timeout(RECEIVE_TIMEOUT, advertiser.ready())
            .await??
            .call(Request::AdvertiseBlockToAll(hash.into()))
            .await?;
        assert_eq!(
            recv_advert(&mut receiver).await?,
            hash,
            "the retry of a refused block is sent"
        );
        Ok(())
    }

    #[tokio::test]
    async fn native_block_advertiser_waits_for_a_full_session_to_make_room() -> Result<(), BoxError>
    {
        let (mut advertiser, broadcast) = advertiser();
        let (_session, mut receiver) = insert_session(&broadcast.outbound, 12, 1);
        let earlier = block_hash(120);
        let hash = block_hash(12);
        timeout(RECEIVE_TIMEOUT, advertiser.ready())
            .await??
            .call(Request::AdvertiseBlockToAll(earlier.into()))
            .await?;

        // The queue of one is full, and drains while the advertisement is waiting for room.
        let drain = async {
            tokio::time::sleep(TEST_QUEUE_WAIT / 4).await;
            recv_advert(&mut receiver).await
        };
        let advertise = async {
            timeout(RECEIVE_TIMEOUT, advertiser.ready())
                .await??
                .call(Request::AdvertiseBlockToAll(hash.into()))
                .await
        };
        let (drained, advertised) = tokio::join!(drain, advertise);
        assert_eq!(drained?, earlier);
        advertised?;

        assert_eq!(
            recv_advert(&mut receiver).await?,
            hash,
            "the block is queued once the session made room"
        );
        assert!(!is_unseen(&broadcast, hash).await);
        assert_eq!(latest_block(&broadcast.outbound), Some(hash));
        Ok(())
    }

    #[tokio::test]
    async fn native_block_advertiser_marks_seen_and_remembers_replay_only_after_delivery(
    ) -> Result<(), BoxError> {
        let (mut advertiser, broadcast) = advertiser();
        let (_session, mut receiver) = insert_session(&broadcast.outbound, 5, 1);
        let hash = block_hash(5);
        assert_eq!(latest_block(&broadcast.outbound), None);

        timeout(RECEIVE_TIMEOUT, advertiser.ready())
            .await??
            .call(Request::AdvertiseMinedBlock(hash.into()))
            .await?;
        assert_eq!(recv_advert(&mut receiver).await?, hash);
        assert!(
            !is_unseen(&broadcast, hash).await,
            "a delivered block is recorded as seen"
        );
        assert_eq!(latest_block(&broadcast.outbound), Some(hash));

        let (later, mut later_receiver) = insert_session(&broadcast.outbound, 6, 1);
        broadcast
            .outbound
            .replay_latest_block_to_peer(later, TEST_QUEUE_WAIT)
            .await?;
        assert_eq!(
            recv_advert(&mut later_receiver).await?,
            hash,
            "a later peer gets the delivered block replayed"
        );
        Ok(())
    }

    #[tokio::test]
    async fn native_block_advertiser_does_not_resend_a_relayed_block() -> Result<(), BoxError> {
        let (mut advertiser, broadcast) = advertiser();
        let hash = block_hash(7);
        // The inbound relay recorded the block when a peer announced it.
        broadcast
            .mark_seen(&LegacyGossipFrame::AdvertiseBlock(hash))
            .await;
        let (_session, mut receiver) = insert_session(&broadcast.outbound, 7, 1);

        let response = timeout(RECEIVE_TIMEOUT, advertiser.ready())
            .await??
            .call(Request::AdvertiseBlockToAll(hash.into()))
            .await?;
        assert!(matches!(response, Response::Nil));
        assert!(
            receiver.try_recv().is_err(),
            "a block the peers already announced is not sent again"
        );
        Ok(())
    }

    #[tokio::test]
    async fn native_block_advertiser_rejects_other_requests() -> Result<(), BoxError> {
        let (mut advertiser, broadcast) = advertiser();
        let (_session, mut receiver) = insert_session(&broadcast.outbound, 8, 1);
        let tx_id = UnminedTxId::from_legacy_id(zakura_chain::transaction::Hash([8; 32]));

        let error = timeout(RECEIVE_TIMEOUT, advertiser.ready())
            .await??
            .call(Request::AdvertiseTransactionIds([tx_id].into(), None))
            .await
            .expect_err("only block advertisements are sent");
        assert!(
            error.to_string().contains("AdvertiseTransactionIds"),
            "the error names the rejected request, got {error}"
        );
        assert!(receiver.try_recv().is_err());
        Ok(())
    }

    #[tokio::test]
    async fn native_block_advertiser_clone_starts_idle() -> Result<(), BoxError> {
        let (mut advertiser, broadcast) = advertiser();
        let (_session, _receiver) = insert_session(&broadcast.outbound, 9, 1);
        timeout(RECEIVE_TIMEOUT, advertiser.ready()).await??;

        let clone = advertiser.clone();
        assert!(format!("{advertiser:?}").contains("granted"));
        assert!(
            format!("{clone:?}").contains("idle"),
            "a clone establishes its own readiness"
        );
        Ok(())
    }

    #[tokio::test]
    async fn native_block_advertiser_does_not_keep_a_submitted_block_for_replay(
    ) -> Result<(), BoxError> {
        let (mut advertiser, broadcast) = advertiser();
        let (_session, mut receiver) = insert_session(&broadcast.outbound, 10, 1);
        let hash = block_hash(10);

        timeout(RECEIVE_TIMEOUT, advertiser.ready())
            .await??
            .call(Request::AdvertiseMinedBlock(AdvertisedBlock::submitted(
                hash, 1_000,
            )))
            .await?;
        assert_eq!(
            recv_advert(&mut receiver).await?,
            hash,
            "a submitted block is announced to the peers connected now"
        );
        assert!(
            !is_unseen(&broadcast, hash).await,
            "a delivered submission is recorded as seen"
        );
        assert_eq!(
            latest_block(&broadcast.outbound),
            None,
            "a block the chain can still reject is not kept for later peers"
        );

        let (later, mut later_receiver) = insert_session(&broadcast.outbound, 11, 1);
        broadcast
            .outbound
            .replay_latest_block_to_peer(later, TEST_QUEUE_WAIT)
            .await?;
        assert!(
            later_receiver.try_recv().is_err(),
            "a later peer is not sent the submitted block"
        );
        Ok(())
    }

    #[tokio::test]
    async fn native_block_advertiser_keeps_a_submitted_block_for_replay_once_it_commits(
    ) -> Result<(), BoxError> {
        let (mut advertiser, broadcast) = advertiser();
        let (_session, mut receiver) = insert_session(&broadcast.outbound, 12, 2);
        let hash = block_hash(12);

        timeout(RECEIVE_TIMEOUT, advertiser.ready())
            .await??
            .call(Request::AdvertiseMinedBlock(AdvertisedBlock::submitted(
                hash, 1_000,
            )))
            .await?;
        assert_eq!(recv_advert(&mut receiver).await?, hash);
        assert_eq!(latest_block(&broadcast.outbound), None);

        timeout(RECEIVE_TIMEOUT, advertiser.ready())
            .await??
            .call(Request::AdvertiseMinedBlock(AdvertisedBlock::new(
                hash, 1_000,
            )))
            .await?;
        assert!(
            receiver.try_recv().is_err(),
            "a block already announced as a submission is not sent again when it commits"
        );
        assert_eq!(
            latest_block(&broadcast.outbound),
            Some(hash),
            "the committed block is kept for later peers"
        );

        let (later, mut later_receiver) = insert_session(&broadcast.outbound, 13, 1);
        broadcast
            .outbound
            .replay_latest_block_to_peer(later, TEST_QUEUE_WAIT)
            .await?;
        assert_eq!(
            recv_advert(&mut later_receiver).await?,
            hash,
            "a later peer is sent the committed block"
        );
        Ok(())
    }

    #[tokio::test]
    async fn native_block_advertiser_offers_a_delivered_block_to_a_session_added_while_waiting(
    ) -> Result<(), BoxError> {
        let (mut advertiser, broadcast) = advertiser();
        let (_busy, mut busy_receiver) = insert_session(&broadcast.outbound, 14, 1);
        let earlier = block_hash(140);
        let hash = block_hash(14);
        timeout(RECEIVE_TIMEOUT, advertiser.ready())
            .await??
            .call(Request::AdvertiseBlockToAll(earlier.into()))
            .await?;

        // While the full queue is making room, another peer's session joins, then the queue
        // drains.
        let join_then_drain = async {
            tokio::time::sleep(TEST_QUEUE_WAIT / 4).await;
            let joined = insert_session(&broadcast.outbound, 15, 1);
            let drained = recv_advert(&mut busy_receiver).await;
            (joined, drained)
        };
        let advertise = async {
            timeout(RECEIVE_TIMEOUT, advertiser.ready())
                .await??
                .call(Request::AdvertiseBlockToAll(hash.into()))
                .await
        };
        let (((joined, mut joined_receiver), drained), advertised) =
            tokio::join!(join_then_drain, advertise);
        assert_eq!(drained?, earlier);
        advertised?;

        assert_eq!(recv_advert(&mut busy_receiver).await?, hash);
        assert_eq!(
            recv_advert(&mut joined_receiver).await?,
            hash,
            "the session that joined while the block was waiting is sent it"
        );

        // The replay on joining runs after the block was remembered, and finds it queued.
        broadcast
            .outbound
            .replay_latest_block_to_peer(joined, TEST_QUEUE_WAIT)
            .await?;
        assert!(
            joined_receiver.try_recv().is_err(),
            "the joined session is not queued the block a second time"
        );
        Ok(())
    }

    #[tokio::test]
    async fn native_block_advertiser_reports_no_eligible_peers_when_every_offered_session_closed(
    ) -> Result<(), BoxError> {
        let (mut advertiser, broadcast) = advertiser();
        let (_gone, gone_receiver) = insert_session(&broadcast.outbound, 16, 1);
        let earlier = block_hash(160);
        let hash = block_hash(16);
        timeout(RECEIVE_TIMEOUT, advertiser.ready())
            .await??
            .call(Request::AdvertiseBlockToAll(earlier.into()))
            .await?;

        // While the full queue is making room, another peer's session joins and the full one's
        // transport goes away.
        let join_then_close = async {
            tokio::time::sleep(TEST_QUEUE_WAIT / 4).await;
            let joined = insert_session(&broadcast.outbound, 17, 1);
            drop(gone_receiver);
            joined
        };
        let advertise = async {
            timeout(RECEIVE_TIMEOUT, advertiser.ready())
                .await??
                .call(Request::AdvertiseBlockToAll(hash.into()))
                .await
        };
        let ((_joined, mut joined_receiver), advertised) = tokio::join!(join_then_close, advertise);
        let error = advertised.expect_err("the block reached nobody");
        assert!(
            error.is::<NoEligibleBlockPeers>(),
            "a session that joined after the offer is not a refusal, got {error}"
        );
        assert!(is_unseen(&broadcast, hash).await);
        assert_eq!(latest_block(&broadcast.outbound), Some(earlier));

        // The joined session makes the next advertisement ready, and is sent the block.
        timeout(RECEIVE_TIMEOUT, advertiser.ready())
            .await??
            .call(Request::AdvertiseBlockToAll(hash.into()))
            .await?;
        assert_eq!(recv_advert(&mut joined_receiver).await?, hash);
        Ok(())
    }

    #[tokio::test]
    async fn native_block_advertiser_keeps_the_later_block_for_replay_when_an_earlier_one_finishes_last(
    ) -> Result<(), BoxError> {
        let (mut advertiser, broadcast) = advertiser();
        let (_session, mut receiver) = insert_session(&broadcast.outbound, 18, 1);
        let earlier = block_hash(180);
        let older = block_hash(18);
        let newer = block_hash(19);
        timeout(RECEIVE_TIMEOUT, advertiser.ready())
            .await??
            .call(Request::AdvertiseBlockToAll(earlier.into()))
            .await?;

        // While the older block waits for the full queue, the newer block, relayed from a peer
        // already, takes the seen path at once; then the queue drains.
        let mut later_advertiser = advertiser.clone();
        let newer_then_drain = async {
            tokio::time::sleep(TEST_QUEUE_WAIT / 4).await;
            broadcast
                .mark_seen(&LegacyGossipFrame::AdvertiseBlock(newer))
                .await;
            timeout(RECEIVE_TIMEOUT, later_advertiser.ready())
                .await??
                .call(Request::AdvertiseBlockToAll(newer.into()))
                .await?;
            assert_eq!(latest_block(&broadcast.outbound), Some(newer));
            recv_advert(&mut receiver).await
        };
        let advertise_older = async {
            timeout(RECEIVE_TIMEOUT, advertiser.ready())
                .await??
                .call(Request::AdvertiseBlockToAll(older.into()))
                .await
        };
        let (drained, advertised) = tokio::join!(newer_then_drain, advertise_older);
        assert_eq!(drained?, earlier);
        advertised?;

        assert_eq!(recv_advert(&mut receiver).await?, older);
        assert_eq!(
            latest_block(&broadcast.outbound),
            Some(newer),
            "the block advertised later stays the one replayed to later peers"
        );
        Ok(())
    }

    #[tokio::test]
    async fn native_block_advertiser_does_not_catch_a_joined_session_up_with_an_older_block(
    ) -> Result<(), BoxError> {
        let (mut advertiser, broadcast) = advertiser();
        let (_busy, mut busy_receiver) = insert_session(&broadcast.outbound, 20, 1);
        let earlier = block_hash(200);
        let older = block_hash(20);
        let newer = block_hash(21);
        timeout(RECEIVE_TIMEOUT, advertiser.ready())
            .await??
            .call(Request::AdvertiseBlockToAll(earlier.into()))
            .await?;

        // While the older block waits for the full queue: a newer, already relayed block is
        // remembered, a session joins and is replayed it, then the queue drains.
        let mut later_advertiser = advertiser.clone();
        let newer_then_join_then_drain = async {
            tokio::time::sleep(TEST_QUEUE_WAIT / 4).await;
            broadcast
                .mark_seen(&LegacyGossipFrame::AdvertiseBlock(newer))
                .await;
            timeout(RECEIVE_TIMEOUT, later_advertiser.ready())
                .await??
                .call(Request::AdvertiseBlockToAll(newer.into()))
                .await?;
            let (joined, mut joined_receiver) = insert_session(&broadcast.outbound, 22, 2);
            broadcast
                .outbound
                .replay_latest_block_to_peer(joined, TEST_QUEUE_WAIT)
                .await?;
            assert_eq!(recv_advert(&mut joined_receiver).await?, newer);
            let drained = recv_advert(&mut busy_receiver).await?;
            Ok::<_, BoxError>((joined_receiver, drained))
        };
        let advertise_older = async {
            timeout(RECEIVE_TIMEOUT, advertiser.ready())
                .await??
                .call(Request::AdvertiseBlockToAll(older.into()))
                .await
        };
        let (joined, advertised) = tokio::join!(newer_then_join_then_drain, advertise_older);
        let (mut joined_receiver, drained) = joined?;
        assert_eq!(drained, earlier);
        advertised?;

        assert_eq!(recv_advert(&mut busy_receiver).await?, older);
        assert!(
            joined_receiver.try_recv().is_err(),
            "a session that has the newer block is not sent the older one"
        );
        Ok(())
    }

    #[tokio::test]
    async fn native_block_advertiser_replay_waits_for_a_full_session_to_make_room(
    ) -> Result<(), BoxError> {
        let (mut advertiser, broadcast) = advertiser();
        let (_session, mut receiver) = insert_session(&broadcast.outbound, 23, 1);
        let hash = block_hash(23);
        timeout(RECEIVE_TIMEOUT, advertiser.ready())
            .await??
            .call(Request::AdvertiseBlockToAll(hash.into()))
            .await?;

        // A session that joins with a full queue of its own is replayed the block once it
        // drains.
        let (joined, mut joined_receiver) = insert_session(&broadcast.outbound, 24, 1);
        let filler = block_hash(240);
        joined.try_send_advertise_block(filler)?;
        let drain = async {
            tokio::time::sleep(TEST_QUEUE_WAIT / 4).await;
            recv_advert(&mut joined_receiver).await
        };
        let replay = broadcast
            .outbound
            .replay_latest_block_to_peer(joined.clone(), TEST_QUEUE_WAIT);
        let (drained, replayed) = tokio::join!(drain, replay);
        assert_eq!(drained?, filler);
        replayed?;
        assert_eq!(recv_advert(&mut joined_receiver).await?, hash);
        assert_eq!(recv_advert(&mut receiver).await?, hash);
        Ok(())
    }
}
