//! Discovery serving runs the shared ownership suite.

use std::sync::Arc;

use iroh::SecretKey;
use tokio::sync::watch;

use super::*;
use crate::zakura::{
    discovery::pipe::decode_discovery_frame,
    regulation::serving_kit::{serving_ownership_suite, ServingUnderTest},
    ZakuraDiscoveryConfig, ZakuraDiscoveryLocalConfig, ZakuraHandshakeConfig,
};
use zakura_chain::parameters::Network;

fn discovery_handle() -> ZakuraDiscoveryHandle {
    let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
    // The handle keeps its own receiver; the sender can go.
    let (_connected_tx, connected_rx) = watch::channel(Vec::new());
    ZakuraDiscoveryHandle::new(
        ZakuraDiscoveryLocalConfig {
            secret_key: SecretKey::from_bytes(&[31u8; 32]),
            direct_addrs: Vec::new(),
            services: vec![ZakuraServiceId::discovery()],
            zakura_protocol_min: handshake.zakura_protocol_min,
            zakura_protocol_max: handshake.zakura_protocol_max,
            network_id: handshake.network_id,
            chain_id: handshake.chain_id,
            last_authored_sequence: None,
        },
        ZakuraDiscoveryConfig::default(),
        connected_rx,
    )
    .expect("the test discovery config is valid")
}

/// `GetPeers` stalls while the test holds the address-book lock.
struct GetPeersUnderTest(Arc<GetPeersServe>);

impl ServingUnderTest for GetPeersUnderTest {
    type Serve = GetPeersServe;
    type Stall = Box<dyn std::any::Any + Send>;

    async fn new() -> Self {
        Self(Arc::new(GetPeersServe {
            handle: discovery_handle(),
            peer_node_id: SecretKey::from_bytes(&[32u8; 32]).public(),
        }))
    }

    fn serve(&self) -> Arc<GetPeersServe> {
        self.0.clone()
    }

    fn request(&self, seq: u32) -> GetPeersRequest {
        GetPeersRequest {
            limit: u16::try_from(seq % 8).expect("small limits fit u16"),
            wanted_services: Vec::new(),
            exclude_node_ids: Vec::new(),
        }
    }

    fn max_response_bytes(&self) -> u32 {
        MAX_DISCOVERY_RESPONSE_FRAME
    }

    async fn stall(&self) -> Self::Stall {
        self.0.handle.hold_book_for_test().await
    }

    fn fail_next(&self) -> bool {
        false
    }

    fn assert_response(&self, frame: &Frame, _seq: u32) {
        assert!(matches!(
            decode_discovery_frame(frame),
            Ok(DiscoveryMessage::Peers { .. })
        ));
    }
}

/// `GetServices` stalls while the test holds the address-book lock.
struct GetServicesUnderTest(Arc<GetServicesServe>);

impl ServingUnderTest for GetServicesUnderTest {
    type Serve = GetServicesServe;
    type Stall = Box<dyn std::any::Any + Send>;

    async fn new() -> Self {
        Self(Arc::new(GetServicesServe {
            handle: discovery_handle(),
            header_sync: None,
            block_sync: None,
        }))
    }

    fn serve(&self) -> Arc<GetServicesServe> {
        self.0.clone()
    }

    fn request(&self, _seq: u32) -> GetServices {
        GetServices {
            wanted_services: Vec::new(),
        }
    }

    fn max_response_bytes(&self) -> u32 {
        MAX_DISCOVERY_RESPONSE_FRAME
    }

    async fn stall(&self) -> Self::Stall {
        self.0.handle.hold_book_for_test().await
    }

    fn fail_next(&self) -> bool {
        false
    }

    fn assert_response(&self, frame: &Frame, _seq: u32) {
        assert!(matches!(
            decode_discovery_frame(frame),
            Ok(DiscoveryMessage::Services(_))
        ));
    }
}

serving_ownership_suite!(get_peers_ownership, GetPeersUnderTest);
serving_ownership_suite!(get_services_ownership, GetServicesUnderTest);
