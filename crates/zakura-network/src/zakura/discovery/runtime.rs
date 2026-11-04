//! Wiring helpers for constructing the native discovery runtime handle.

use std::net::SocketAddr;

use zakura_quic::NodeSecretKey;
use zakura_rt::sync::watch;

use crate::zakura::{ZakuraHandshakeConfig, ZakuraPeerId};

use super::protocol::{
    DiscoveryWireError, ZakuraDiscoveryConfig, ZakuraDiscoveryHandle, ZakuraDiscoveryLocalConfig,
    ZakuraServiceId, DEFAULT_DISCOVERY_CONNECTION_HEADROOM,
};
use super::sequence_file::AuthoredSequenceFile;

/// Services advertised in this node's discovery self-record.
pub(crate) fn default_advertised_services() -> Vec<ZakuraServiceId> {
    vec![
        ZakuraServiceId::discovery(),
        ZakuraServiceId::block_sync(),
        ZakuraServiceId::header_sync(),
        ZakuraServiceId::legacy_gossip(),
        ZakuraServiceId::legacy_requests(),
        ZakuraServiceId::service_discovery(),
    ]
}

/// Reserves at least one connection slot per configured bootstrap peer so
/// bootstrap dials are never starved by discovered-peer dials.
pub(crate) fn effective_discovery_connection_headroom(bootstrap_peer_count: usize) -> usize {
    DEFAULT_DISCOVERY_CONNECTION_HEADROOM.max(bootstrap_peer_count)
}

/// Builds a discovery runtime handle from the local node identity and the
/// negotiated network parameters. With a `sequence_file`, the handle seeds its
/// first self-record sequence from the stored one and stores each sequence it
/// authors.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_discovery_handle(
    secret_key: NodeSecretKey,
    sequence_file: Option<AuthoredSequenceFile>,
    direct_addrs: Vec<SocketAddr>,
    advertised_services: Vec<ZakuraServiceId>,
    handshake: &ZakuraHandshakeConfig,
    max_zakura_connections: usize,
    bootstrap_peer_count: usize,
    connected: watch::Receiver<Vec<ZakuraPeerId>>,
) -> Result<ZakuraDiscoveryHandle, DiscoveryWireError> {
    let local_config = ZakuraDiscoveryLocalConfig {
        secret_key,
        direct_addrs,
        services: advertised_services,
        zakura_protocol_min: handshake.zakura_protocol_min,
        zakura_protocol_max: handshake.zakura_protocol_max,
        network_id: handshake.network_id,
        chain_id: handshake.chain_id,
        last_authored_sequence: sequence_file.as_ref().and_then(AuthoredSequenceFile::load),
    };
    let config = ZakuraDiscoveryConfig {
        max_zakura_connections,
        discovery_connection_headroom: effective_discovery_connection_headroom(
            bootstrap_peer_count,
        ),
        ..ZakuraDiscoveryConfig::default()
    };
    let handle = ZakuraDiscoveryHandle::new(local_config, config, connected)?;
    Ok(match sequence_file {
        Some(file) => handle.with_sequence_file(file),
        None => handle,
    })
}
