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
};

use futures::FutureExt;
use tower::Service;

use crate::{
    peer::{block_gossip::Readiness, NoEligibleBlockPeers},
    protocol::internal::{Request, Response},
    BoxError,
};

use super::{LegacyGossipFrame, ZakuraGossipBroadcast, ZakuraSupervisorHandle};

/// Tower service that announces blocks to the Zakura peers with open gossip sessions.
///
/// Ready once a session can take a frame, which may be a session with a full queue, like a busy
/// legacy peer. A block is recorded as seen, and kept for replay to later peers, only once a
/// session queued it; a block already seen, sent earlier or relayed from a peer, is not sent
/// again. Requests other than block advertisements fail.
pub(crate) struct ZakuraBlockAdvertiser {
    broadcast: ZakuraGossipBroadcast,
    readiness: Readiness,
}

impl ZakuraBlockAdvertiser {
    /// Returns an advertiser for the gossip sessions of the given supervisor's peers.
    pub(crate) fn new(supervisor: ZakuraSupervisorHandle) -> Self {
        Self::from_broadcast(ZakuraGossipBroadcast::new(supervisor))
    }

    fn from_broadcast(broadcast: ZakuraGossipBroadcast) -> Self {
        Self {
            broadcast,
            readiness: Readiness::Idle,
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

    async fn advertise(
        broadcast: ZakuraGossipBroadcast,
        frame: LegacyGossipFrame,
    ) -> Result<Response, BoxError> {
        if broadcast.unseen(&frame).await.is_none() {
            return Ok(Response::Nil);
        }

        let fanout = broadcast.outbound.fan_out_to_peers(&frame, None);
        if fanout.delivered > 0 {
            broadcast.mark_seen(&frame).await;
            broadcast.outbound.remember_latest_block(&frame);
            return Ok(Response::Nil);
        }
        if fanout.attempted == 0 || !broadcast.outbound.has_open_session() {
            // Nobody received it: the same as having had nobody to send to.
            return Err(Box::new(NoEligibleBlockPeers) as BoxError);
        }
        let refusal = fanout
            .first_error
            .expect("a session that was attempted but did not deliver recorded its refusal");
        Err(refusal.into())
    }
}

impl Clone for ZakuraBlockAdvertiser {
    /// Clones the handle without its wait: each handle establishes its own readiness.
    fn clone(&self) -> Self {
        Self::from_broadcast(self.broadcast.clone())
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
            LegacyGossipFrame::AdvertiseBlock(block.hash),
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
        protocol::internal::{Request, Response},
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

    fn advertiser() -> (ZakuraBlockAdvertiser, ZakuraGossipBroadcast) {
        let broadcast = ZakuraGossipBroadcast {
            first_seen: FirstSeenCache::new(DEFAULT_FIRST_SEEN_CAPACITY, DEFAULT_FIRST_SEEN_TTL),
            outbound: LegacyGossipOutbound::default(),
        };
        (
            ZakuraBlockAdvertiser::from_broadcast(broadcast.clone()),
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
        *outbound
            .latest_block
            .lock()
            .expect("legacy gossip latest-block mutex is never poisoned")
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

        // The queue of one is full: the session is there but refuses the next frame.
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
            .replay_latest_block_to_peer(later)
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
}
