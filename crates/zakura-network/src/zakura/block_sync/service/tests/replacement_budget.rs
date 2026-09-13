//! Denying memory for a replacement must leave the current receiver usable.

use super::*;
use crate::zakura::transport::worker_framed_channel;

#[tokio::test]
async fn metadata_full_replacement_keeps_the_previous_receiver_active() {
    use crate::zakura::regulation::ResponseMemory;

    let setup = ResponseMemory::node_setup_bytes_for_test()
        + ResponseMemory::setup_bytes_for_test()
        + ResponseScope::setup_bytes_for_test();
    let node = ResponseMemory::new(setup + 4096, setup + 4096);
    let memory = node.connection();
    let service = BlockSyncService::new_for_test(ZakuraBlockSyncConfig::default());
    let peer = ZakuraPeerId::new(vec![75; 32]).unwrap();
    let connection = CancellationToken::new();
    let add = || {
        let (input, recv) = crate::zakura::framed_channel(4);
        let (send, output) = worker_framed_channel(4);
        let candidate =
            crate::zakura::testkit::DownloadOnlyPeer::create_with_conn_id_and_direction(
                1,
                peer.clone(),
                None,
                ZAKURA_CAP_BLOCK_SYNC,
                ServicePeerDirection::Outbound,
                HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (recv, send))]),
                connection.clone(),
            )
            .with_response_memory(memory.clone());
        let cancel = candidate.service_cancel_token();
        service.add_peer(candidate);
        (input, output, cancel)
    };
    let (_input, _output, old_cancel) = add();
    let old = service.current_sessions_for_test().snapshot()[&peer].clone();
    let held = memory.try_reserve(4096).unwrap();
    let (_new_input, _new_output, rejected_cancel) = add();
    assert!(rejected_cancel.is_cancelled());
    assert!(!old_cancel.is_cancelled());
    assert!(!connection.is_cancelled());
    assert_eq!(node.reserved_for_test(), setup + 4096);
    assert_eq!(
        service.current_sessions_for_test().snapshot()[&peer].session_id(),
        old.session_id()
    );
    drop(held);
    let mut owner = old.authorize_response().unwrap();
    let write = owner.write_permission();
    assert!(write.publish(|| {}));
    assert!(write.try_start(|| true));
    owner.finish();
    service.remove_peer(&peer, 1);
}
