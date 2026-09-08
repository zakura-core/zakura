use super::*;
use crate::zakura::{
    block_sync::serving_regulation::{GetBlocksServingPermit, GetBlocksServingRegulator},
    transport::{worker_framed_channel, FramedWorkerRecv},
};

fn setup() -> (
    BlockSyncPeerSession,
    FramedWorkerRecv,
    GetBlocksServingRegulator,
    GetBlocksServingPermit,
) {
    let peer = ZakuraPeerId::new(vec![1; 32]).unwrap();
    let regulator = GetBlocksServingRegulator::new(ZakuraBlockSyncConfig::default());
    let permit = regulator
        .session(peer.clone(), 1)
        .try_admit(1)
        .unwrap()
        .commit();
    let (sender, receiver) = worker_framed_channel(1);
    let session = BlockSyncPeerSession::for_test(peer, sender, CancellationToken::new());
    (session, receiver, regulator, permit)
}

fn invalid_terminal() -> BlockSyncMessage {
    BlockSyncMessage::BlocksDone {
        start_height: block::Height(1),
        returned: u32::MAX,
    }
}

#[test]
fn full_or_closed_queue_is_checked_before_encoding() {
    let (session, receiver, regulator, mut permit) = setup();
    assert!(BlockSyncPeerSession::encode_regulated_message(invalid_terminal(), &permit).is_err());
    session
        .try_send_range_unavailable(block::Height(1), 1)
        .unwrap();
    // This terminator cannot encode. Full, rather than Encode, proves that a
    // congested queue rejects the attempt before serialization starts.
    assert!(matches!(
        session.try_send_regulated_message(invalid_terminal(), &mut permit),
        Err(OrderedSendError::Full)
    ));
    drop(receiver);
    assert!(matches!(
        session.try_send_regulated_message(invalid_terminal(), &mut permit),
        Err(OrderedSendError::Closed)
    ));
    drop(permit);
    assert_eq!(regulator.snapshot().node_active, 0);
}

#[tokio::test]
async fn encode_failure_returns_queue_space_without_sharing_the_producer() {
    let (session, mut receiver, regulator, mut permit) = setup();
    assert!(matches!(
        session.try_send_regulated_message(invalid_terminal(), &mut permit),
        Err(OrderedSendError::Encode(_))
    ));
    assert_eq!(session.send.capacity(), 1);
    session
        .try_send_regulated_message(
            BlockSyncMessage::RangeUnavailable {
                start_height: block::Height(1),
                count: 1,
            },
            &mut permit,
        )
        .unwrap();
    drop(permit);
    assert_eq!(regulator.snapshot().node_active, 1);
    drop(receiver.recv().await.unwrap());
    assert_eq!(regulator.snapshot().node_active, 0);
}

#[tokio::test]
async fn cancelled_queue_wait_does_not_encode_or_share_the_producer() {
    let (session, mut receiver, regulator, mut permit) = setup();
    session
        .try_send_range_unavailable(block::Height(1), 1)
        .unwrap();
    let mut pending = Box::pin(session.send_regulated_message(invalid_terminal(), &mut permit));
    assert!(futures::poll!(&mut pending).is_pending());
    drop(pending);
    drop(receiver.recv().await.unwrap());
    assert_eq!(session.send.capacity(), 1);
    assert!(matches!(
        session
            .send_regulated_message(invalid_terminal(), &mut permit)
            .await,
        Err(OrderedSendError::Encode(_))
    ));
    assert_eq!(session.send.capacity(), 1);
    drop(receiver);
    assert!(matches!(
        session
            .send_regulated_message(invalid_terminal(), &mut permit)
            .await,
        Err(OrderedSendError::Closed)
    ));
    drop(permit);
    assert_eq!(regulator.snapshot().node_active, 0);
}
