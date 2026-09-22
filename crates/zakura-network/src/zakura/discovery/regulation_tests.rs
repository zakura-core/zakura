//! Hostile discovery scripts over real connections.
//!
//! The shared suites check reservations and serving in process. These scripts
//! check that discovery wires them to the connection: a duplicate response
//! closes it, and peers that never read do not block an honest request.

use std::time::Duration;

use super::protocol::DiscoveryMessage;
use crate::{
    zakura::{
        testkit::{HostilePeer, ZakuraTestNode},
        Frame, ZAKURA_CAP_DISCOVERY, ZAKURA_STREAM_DISCOVERY,
    },
    BoxError,
};

fn discovery_frame(message: DiscoveryMessage) -> Frame {
    Frame {
        message_type: 1,
        flags: 0,
        payload: message.encode().expect("test discovery messages encode"),
    }
}

fn get_peers() -> Frame {
    discovery_frame(DiscoveryMessage::GetPeers {
        limit: 8,
        wanted_services: Vec::new(),
        exclude_node_ids: Vec::new(),
    })
}

fn empty_peers() -> Frame {
    discovery_frame(DiscoveryMessage::Peers {
        records: Vec::new(),
    })
}

/// Open the discovery stream and read until the victim asks for peers.
async fn await_victim_get_peers(peer: &HostilePeer) -> Result<(), BoxError> {
    peer.send_raw_frame(ZAKURA_STREAM_DISCOVERY, get_peers())
        .await?;
    loop {
        let frame = tokio::time::timeout(
            Duration::from_secs(5),
            peer.recv_ordered_frame(ZAKURA_STREAM_DISCOVERY),
        )
        .await??;
        if matches!(
            DiscoveryMessage::decode(&frame.payload)?,
            DiscoveryMessage::GetPeers { .. }
        ) {
            return Ok(());
        }
    }
}

#[tokio::test]
async fn a_duplicate_peers_response_closes_the_connection() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let victim = ZakuraTestNode::builder(61).spawn().await?;
    let control =
        HostilePeer::connect_native_with_capabilities(&victim, 62, ZAKURA_CAP_DISCOVERY).await?;
    let hostile =
        HostilePeer::connect_native_with_capabilities(&victim, 63, ZAKURA_CAP_DISCOVERY).await?;

    for peer in [&control, &hostile] {
        await_victim_get_peers(peer).await?;
        peer.send_raw_frame(ZAKURA_STREAM_DISCOVERY, empty_peers())
            .await?;
    }
    hostile
        .send_raw_frame(ZAKURA_STREAM_DISCOVERY, empty_peers())
        .await?;

    // The victim's initial exchange waits two seconds for Hello and Services,
    // which neither peer sends, so a close within one second is the reject.
    hostile
        .wait_for_connection_close(Duration::from_secs(1))
        .await?;
    assert!(
        control
            .wait_for_connection_close(Duration::from_secs(1))
            .await
            .is_err(),
        "one answer per request keeps the connection"
    );

    control.shutdown().await;
    hostile.shutdown().await;
    victim.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn peers_that_never_read_do_not_block_an_honest_get_peers() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let victim = ZakuraTestNode::builder(64).spawn().await?;

    // More silent requesters than the node has serving slots.
    let mut silent = Vec::new();
    for seed in 65..71 {
        let peer =
            HostilePeer::connect_native_with_capabilities(&victim, seed, ZAKURA_CAP_DISCOVERY)
                .await?;
        peer.send_raw_frame(ZAKURA_STREAM_DISCOVERY, get_peers())
            .await?;
        silent.push(peer);
    }

    let honest =
        HostilePeer::connect_native_with_capabilities(&victim, 71, ZAKURA_CAP_DISCOVERY).await?;
    honest
        .send_raw_frame(ZAKURA_STREAM_DISCOVERY, get_peers())
        .await?;
    let answered = async {
        loop {
            let frame = honest.recv_ordered_frame(ZAKURA_STREAM_DISCOVERY).await?;
            if matches!(
                DiscoveryMessage::decode(&frame.payload)?,
                DiscoveryMessage::Peers { .. }
            ) {
                return Ok::<_, BoxError>(());
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(5), answered).await??;

    for peer in silent {
        peer.shutdown().await;
    }
    honest.shutdown().await;
    victim.shutdown().await;
    Ok(())
}
