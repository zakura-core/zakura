//! Replace real service sessions while requests are prepared, queued or writing.

use super::*;

use super::super::work_queue::{RequestWrite, WorkQueue};
use crate::zakura::transport::{
    worker_framed_channel, ByteBudget, FrameWriteClaim, FramedWorkerRecv,
};

struct PreparedRequest {
    authorization: ResponseAuthorization,
    write: Arc<RequestWrite>,
    work: Arc<WorkQueue>,
    budget: ByteBudget,
}

impl PreparedRequest {
    fn new(session: &BlockSyncPeerSession) -> Self {
        let authorization = session.authorize_response().unwrap();
        let work = Arc::new(WorkQueue::new(block::Height(0)));
        work.set_estimate_floor_for_tests(1);
        work.extend(
            test_work_scope(),
            [(
                block::Height(1),
                block::Hash([1; 32]),
                BlockSizeEstimate::Confirmed(100),
            )],
        );
        let items = work.take_for_request(
            block::Height(1),
            block::Height(1),
            1,
            100,
            session.session_id(),
            std::num::NonZeroU64::new(1).unwrap(),
        );
        assert_eq!(items.len(), 1);
        let mut budget = ByteBudget::new(100);
        assert!(budget.try_reserve(100));
        let write = RequestWrite::new(
            items[0].1.owner.unwrap(),
            items,
            work.clone(),
            budget.clone(),
            session.cancel_token(),
            authorization.write_permission(),
        );
        Self {
            authorization,
            write,
            work,
            budget,
        }
    }

    fn queue(&self, session: &BlockSyncPeerSession) {
        let sender = session.request_sender();
        let slot = sender.try_reserve_guarded().unwrap();
        let frame = BlockSyncMessage::GetBlocks {
            start_height: block::Height(1),
            count: 1,
        }
        .encode_frame()
        .unwrap();
        assert!(self
            .write
            .publish(|| assert!(slot.send_request(frame, self.write.clone()))));
    }
}

/// Build the download-only peer a fencing test admits, keeping both stream ends.
fn fence_peer(
    peer: &ZakuraPeerId,
    conn_id: ZakuraConnId,
    cancel: CancellationToken,
) -> (Peer, FramedSend, FramedWorkerRecv) {
    let (input, recv) = crate::zakura::framed_channel(4);
    let (send, output) = worker_framed_channel(4);
    let peer = crate::zakura::testkit::DownloadOnlyPeer::create_with_conn_id_and_direction(
        conn_id,
        peer.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (recv, send))]),
        cancel,
    );
    (peer, input, output)
}

fn add_fence_peer(
    service: &BlockSyncService,
    peer: &ZakuraPeerId,
    conn_id: ZakuraConnId,
    cancel: CancellationToken,
) -> (FramedSend, FramedWorkerRecv) {
    let (peer, input, output) = fence_peer(peer, conn_id, cancel);
    service.add_peer(peer);
    (input, output)
}

#[tokio::test]
async fn replacement_fences_old_publication_and_queued_first_write() {
    for queued in [false, true] {
        let service = BlockSyncService::new_for_test(ZakuraBlockSyncConfig::default());
        let peer = ZakuraPeerId::new(vec![71; 32]).unwrap();
        let connection = CancellationToken::new();
        let (_old_input, mut old_output) = add_fence_peer(&service, &peer, 1, connection.clone());
        let old = service.current_sessions_for_test().snapshot()[&peer].clone();
        let request = PreparedRequest::new(&old);
        if queued {
            request.queue(&old);
        }
        let (_new_input, mut new_output) = add_fence_peer(&service, &peer, 1, connection.clone());
        let new = service.current_sessions_for_test().snapshot()[&peer].clone();
        assert_ne!(old.session_id(), new.session_id());
        assert!(old.authorize_response().is_none());
        if queued {
            let mut wrote = false;
            old_output
                .recv()
                .await
                .unwrap()
                .write_with(|_| {
                    wrote = true;
                    async { Ok::<(), ()>(()) }
                })
                .await
                .unwrap();
            assert!(!wrote, "retired queued GetBlocks must emit no bytes");
            assert!(request.write.status().was_skipped());
        } else {
            assert!(!request
                .write
                .publish(|| panic!("retired GetBlocks must not publish")));
        }
        let PreparedRequest {
            authorization,
            write,
            work,
            budget,
        } = request;
        drop(write);
        drop(authorization);
        assert_eq!(work.pending_len(), 1);
        assert_eq!(budget.reserved(), 0);
        assert!(!connection.is_cancelled());
        let mut next = PreparedRequest::new(&new);
        next.queue(&new);
        let mut bytes = 0;
        new_output
            .recv()
            .await
            .unwrap()
            .write_with(|frame| {
                bytes = frame.payload.len();
                async { Ok::<(), ()>(()) }
            })
            .await
            .unwrap();
        // GetBlocks carries one message tag, a starting height and a count.
        let request_bytes = std::mem::size_of::<u8>() + 2 * std::mem::size_of::<u32>();
        assert_eq!(bytes, request_bytes);
        next.authorization.finish();
        assert!(!connection.is_cancelled());
        service.remove_peer(&peer, 1);
    }
}

#[tokio::test]
async fn replacement_closes_started_exchange_even_after_request_write_finishes() {
    for written in [false, true] {
        let service = BlockSyncService::new_for_test(ZakuraBlockSyncConfig::default());
        let peer = ZakuraPeerId::new(vec![72; 32]).unwrap();
        let connection = CancellationToken::new();
        let (_old_input, mut output) = add_fence_peer(&service, &peer, 1, connection.clone());
        let old = service.current_sessions_for_test().snapshot()[&peer].clone();
        let request = PreparedRequest::new(&old);
        request.queue(&old);
        let queued = output.recv().await.unwrap();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (complete_tx, complete_rx) = tokio::sync::oneshot::channel();
        let writer = tokio::spawn(queued.write_with(|_| async move {
            started_tx.send(()).unwrap();
            complete_rx.await.unwrap();
            Ok::<(), ()>(())
        }));
        time::timeout(Duration::from_secs(1), started_rx)
            .await
            .unwrap()
            .unwrap();
        let completion = if written {
            complete_tx.send(()).unwrap();
            time::timeout(Duration::from_secs(1), writer)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            None
        } else {
            Some((complete_tx, writer))
        };
        let (_new_input, mut new_output) = add_fence_peer(&service, &peer, 1, connection.clone());
        assert!(connection.is_cancelled());
        assert_eq!(
            service.current_sessions_for_test().snapshot()[&peer].session_id(),
            old.session_id()
        );
        assert!(time::timeout(Duration::from_secs(1), new_output.recv())
            .await
            .unwrap()
            .is_none());
        if let Some((complete_tx, writer)) = completion {
            complete_tx.send(()).unwrap();
            time::timeout(Duration::from_secs(1), writer)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
        assert_eq!(
            old.close_cause.get_or("unset"),
            "unfinished_response_authorization"
        );
    }
}

#[tokio::test]
async fn finished_exchange_and_new_connection_allow_replacement() {
    for finished in [false, true] {
        let service = BlockSyncService::new_for_test(ZakuraBlockSyncConfig::default());
        let peer = ZakuraPeerId::new(vec![73; 32]).unwrap();
        let old_connection = CancellationToken::new();
        let (_old_input, mut output) = add_fence_peer(&service, &peer, 1, old_connection.clone());
        let old = service.current_sessions_for_test().snapshot()[&peer].clone();
        let mut request = PreparedRequest::new(&old);
        request.queue(&old);
        output
            .recv()
            .await
            .unwrap()
            .write_with(|_| async { Ok::<(), ()>(()) })
            .await
            .unwrap();
        let (next_id, next_connection) = if finished {
            request.authorization.finish();
            (1, old_connection.clone())
        } else {
            (2, CancellationToken::new())
        };
        let (_new_input, _new_output) =
            add_fence_peer(&service, &peer, next_id, next_connection.clone());
        let new = service.current_sessions_for_test().snapshot()[&peer].clone();
        assert_ne!(old.session_id(), new.session_id());
        assert_eq!(old_connection.is_cancelled(), !finished);
        assert!(!next_connection.is_cancelled());
        assert!(new.authorize_response().is_some());
        service.remove_peer(&peer, next_id);
    }
}

#[tokio::test]
async fn removing_a_session_fences_writers_before_erasing_its_record() {
    for teardown in [false, true] {
        for started in [false, true] {
            let service = BlockSyncService::new_for_test(ZakuraBlockSyncConfig::default());
            let peer = ZakuraPeerId::new(vec![74; 32]).unwrap();
            let connection = CancellationToken::new();
            let (_input, _output) = add_fence_peer(&service, &peer, 1, connection.clone());
            let session = service.current_sessions_for_test().snapshot()[&peer].clone();
            let request = PreparedRequest::new(&session);
            request.queue(&session);
            if started {
                assert!(request.write.try_start());
            }
            if teardown {
                assert!(service.inner.finish_session(&peer, 1, session.session_id()));
            } else {
                service.remove_peer(&peer, 1);
            }
            assert!(service.current_sessions_for_test().snapshot().is_empty());
            assert!(!request.write.try_start());
            assert!(session.authorize_response().is_none());
            assert_eq!(connection.is_cancelled(), started);
        }
    }
}

#[tokio::test]
async fn a_session_closed_by_its_own_fence_leaves_no_gap_claim() {
    let service = BlockSyncService::new_for_test(ZakuraBlockSyncConfig::default());
    let peer = ZakuraPeerId::new(vec![75; 32]).unwrap();
    let connection = CancellationToken::new();
    let (_input, _output) = add_fence_peer(&service, &peer, 1, connection.clone());
    let session = service.current_sessions_for_test().snapshot()[&peer].clone();
    let request = PreparedRequest::new(&session);
    request.queue(&session);
    assert!(request.write.try_start());

    assert!(service.inner.finish_session(&peer, 1, session.session_id()));

    assert!(
        connection.is_cancelled(),
        "retiring an unfinished started response closes the connection"
    );
    assert!(
        !service.owns_connection_for_peer(&peer, 1),
        "no gap claim survives for a connection retire already closed"
    );
}

/// A park that lands after `add_peer`'s entry-point check is only honored at
/// admission, so this drives one into that window: the peer-map lock is the
/// single wait between the two checks, and holding it stalls an `add_peer` that
/// has already passed the entry check.
#[tokio::test]
async fn parked_admission_leaves_the_incumbent_session_untouched() {
    // `new_for_test` has no registry wiring, so admission can never return Parked
    // there; the production constructor spawns an inert reactor with wiring.
    let service = BlockSyncService::new(ZakuraBlockSyncConfig::default(), mainnet_decoder());
    let peer = ZakuraPeerId::new(vec![0x5a; 32]).unwrap();
    let _first = add_fence_peer(&service, &peer, 1, CancellationToken::new());
    let incumbent = service.current_sessions_for_test().snapshot()[&peer].clone();
    let registry = service
        .inner
        .routine_wiring
        .as_ref()
        .expect("BlockSyncService::new wires a registry")
        .registry
        .clone();

    // The entry-point check collects expired connection-less parks, so this
    // probe disappearing means the replacement is past that check. Sampling it
    // against an earlier instant keeps the probe itself from collecting it.
    let probe = ZakuraPeerId::new(vec![0x5b; 32]).unwrap();
    let expired = Instant::now();
    let before_expiry = expired - Duration::from_millis(1);
    registry.park_peer_until(&probe, expired);
    assert!(registry.peer_park_deadline(&probe, before_expiry).is_some());

    let (replacement, _second_input, _second_output) =
        fence_peer(&peer, 2, CancellationToken::new());
    std::thread::scope(|scope| {
        let admitted = service.inner.sessions.active.lock().unwrap();
        let admitting = scope.spawn(|| service.add_peer(replacement));
        let give_up = Instant::now() + Duration::from_secs(10);
        while registry.peer_park_deadline(&probe, before_expiry).is_some() {
            assert!(
                Instant::now() < give_up,
                "the replacement never reached the entry-point park check"
            );
            std::thread::yield_now();
        }
        assert!(registry.park_session(
            &peer,
            1,
            incumbent.session_id(),
            Instant::now() + Duration::from_secs(60)
        ));
        drop(admitted);
        admitting.join().unwrap();
    });

    assert!(
        !incumbent.cancel_token().is_cancelled(),
        "a parked admission must not cancel the incumbent"
    );
    // `authorize_response` also covers the incumbent's connection staying open.
    assert!(
        incumbent.authorize_response().is_some(),
        "a parked admission must not retire the incumbent"
    );
    let active = service.inner.sessions.active.lock().unwrap();
    assert_eq!(active[&peer].conn_id, 1, "the incumbent record survives");
    assert_eq!(
        active[&peer].session_id,
        incumbent.session_id(),
        "the parked replacement is declined"
    );
}
