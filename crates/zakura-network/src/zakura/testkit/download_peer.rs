//! Download-only unit fixtures. Both outgoing roles share an observation queue;
//! the QUIC integration tests exercise independent transport queues and writes.

use std::{collections::HashMap, net::IpAddr};

use tokio_util::sync::CancellationToken;

use crate::zakura::{
    framed_channel, transport::ServiceStream, CloseCause, FramedRecv, FramedSend, Peer,
    ServicePeerDirection, ZakuraConnId, ZakuraPeerId, ZAKURA_BLOCK_SYNC_STREAM_VERSION,
    ZAKURA_STREAM_BLOCK_REQUESTS, ZAKURA_STREAM_BLOCK_SYNC,
};

pub(crate) struct DownloadOnlyPeer;

impl DownloadOnlyPeer {
    pub(crate) fn create(
        id: ZakuraPeerId,
        remote_ip: Option<IpAddr>,
        negotiated: u64,
        streams: HashMap<u16, (FramedRecv, FramedSend)>,
        cancel: CancellationToken,
    ) -> Peer {
        Self::create_with_direction(
            id,
            remote_ip,
            negotiated,
            ServicePeerDirection::Inbound,
            streams,
            cancel,
        )
    }

    pub(crate) fn create_with_direction(
        id: ZakuraPeerId,
        remote_ip: Option<IpAddr>,
        negotiated: u64,
        direction: ServicePeerDirection,
        streams: HashMap<u16, (FramedRecv, FramedSend)>,
        cancel: CancellationToken,
    ) -> Peer {
        Self::create_with_conn_id_and_direction(
            0, id, remote_ip, negotiated, direction, streams, cancel,
        )
    }

    pub(crate) fn create_with_conn_id_and_direction(
        conn_id: ZakuraConnId,
        id: ZakuraPeerId,
        remote_ip: Option<IpAddr>,
        negotiated: u64,
        direction: ServicePeerDirection,
        mut streams: HashMap<u16, (FramedRecv, FramedSend)>,
        cancel: CancellationToken,
    ) -> Peer {
        let (data, send) = streams.remove(&ZAKURA_STREAM_BLOCK_SYNC).unwrap();
        assert!(streams.is_empty());
        let (requests_keepalive, requests) = framed_channel(1);
        let session_cancel = cancel.child_token();
        let keepalive_cancel = session_cancel.clone();
        tokio::spawn(async move {
            keepalive_cancel.cancelled().await;
            drop(requests_keepalive);
        });
        Peer::new_with_service_streams(
            conn_id,
            id,
            remote_ip,
            negotiated,
            direction,
            HashMap::from([
                (
                    ZAKURA_STREAM_BLOCK_SYNC,
                    ServiceStream::new(
                        0,
                        ZAKURA_BLOCK_SYNC_STREAM_VERSION,
                        data,
                        send.clone(),
                        session_cancel.clone(),
                    ),
                ),
                (
                    ZAKURA_STREAM_BLOCK_REQUESTS,
                    ServiceStream::new(0, 1, requests, send, session_cancel),
                ),
            ]),
            cancel,
            CloseCause::new(),
        )
    }
}
