//! Process boundary probe for independently pinned Iroh dependency graphs.

use std::{io::Write as _, time::Duration};

use super::{LocalEndpointFactory, ZakuraTestNode};
use crate::{
    zakura::{
        run_native_initiator_handshake, Frame, StreamPrelude, ZakuraHandshakeConfig,
        ZakuraLocalLimits, ZakuraPeerId, LEGACY_GOSSIP_VERSION, P2P_V2_ALPN, STREAM_PRELUDE_MAGIC,
        ZAKURA_CAP_LEGACY_GOSSIP, ZAKURA_STREAM_GOSSIP,
    },
    BoxError, Config,
};

/// Run through scripts/test_iroh_interop.py with separately built test binaries.
#[allow(clippy::print_stdout)] // Machine-readable protocol for the process runner.
#[tokio::test]
#[ignore = "requires a separately started peer process"]
async fn native_process_peer() -> Result<(), BoxError> {
    tokio::time::timeout(Duration::from_secs(20), async {
        let payload = vec![0x5a; 16 * 1024];
        match std::env::var("ZAKURA_INTEROP_PEER") {
            Ok(peer) => {
                let (id, socket) = peer.split_once('@').ok_or("invalid peer address")?;
                let addr = iroh::EndpointAddr::new(id.parse()?).with_ip_addr(socket.parse()?);
                let limits = ZakuraLocalLimits::from_config(&Config::default());
                let endpoint =
                    LocalEndpointFactory::with_transport_config(limits.transport_config())
                        .endpoint(92)
                        .await?;
                let connection = endpoint.connect(addr, P2P_V2_ALPN).await?;
                let mut config = ZakuraHandshakeConfig::for_network(&Config::default().network);
                config.supported_capabilities = ZAKURA_CAP_LEGACY_GOSSIP;
                let peer_id = ZakuraPeerId::new(endpoint.id().as_bytes().to_vec())?;
                run_native_initiator_handshake(&connection, &limits, &config, &peer_id).await?;
                let (mut send, _recv) = connection.open_bi().await?;
                let prelude = StreamPrelude {
                    magic: STREAM_PRELUDE_MAGIC,
                    stream_kind: ZAKURA_STREAM_GOSSIP,
                    stream_version: LEGACY_GOSSIP_VERSION,
                    request_id: None,
                    max_frame_bytes: limits.max_frame_bytes,
                };
                send.write_all(&prelude.encode()?).await?;
                let frame = Frame {
                    message_type: 1,
                    flags: 0,
                    payload,
                };
                let encoded = frame.encode(limits.max_frame_bytes)?;
                for _ in 0..64 {
                    send.write_all(&encoded).await?;
                }
                // The receiver shuts down only after checking the complete payload.
                connection.closed().await;
                endpoint.close().await;
            }
            Err(std::env::VarError::NotPresent) => {
                let node = ZakuraTestNode::builder(91).spawn().await?;
                let addr = node.node_addr().await;
                let socket = addr
                    .ip_addrs()
                    .find(|addr| addr.is_ipv4() && addr.ip().is_loopback())
                    .ok_or("missing loopback socket")?;
                println!("ZAKURA_INTEROP_READY={}@{socket}", addr.id);
                std::io::stdout().flush()?;
                let expected_peer = ZakuraPeerId::new(
                    LocalEndpointFactory::secret_key(92)
                        .public()
                        .as_bytes()
                        .to_vec(),
                )?;
                let mut received_frames = 0;
                while received_frames < 64 {
                    for received in node.recorder().drain() {
                        assert_eq!(received.peer_id, expected_peer);
                        assert_eq!(received.stream_kind, ZAKURA_STREAM_GOSSIP);
                        assert_eq!(received.frame.payload, payload);
                        received_frames += 1;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                node.shutdown().await;
            }
            Err(error) => return Err(error.into()),
        }
        println!("ZAKURA_INTEROP_OK");
        Ok::<_, BoxError>(())
    })
    .await?
}
