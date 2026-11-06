//! Production's discovery startup behind narrow handles, for the
//! simulator's discovery node (disc-2.1).
//!
//! Each handle calls the crate-private entry point that production's
//! endpoint builder calls, and changes nothing in it. Each one passes the
//! caller's arguments through, except that [`build_discovery_handle`] passes
//! no sequence file. They exist only in the simulator build.

use std::net::SocketAddr;

use zakura_quic::NodeSecretKey;
use zakura_rt::{sync::watch, task::JoinHandle};

use crate::zakura::{
    discovery, DiscoveryWireError, ZakuraDiscoveryHandle, ZakuraEndpoint, ZakuraHandshakeConfig,
    ZakuraLocalLimits, ZakuraPeerId, ZakuraServiceId,
};

/// Builds a discovery handle as production's endpoint builder does, with no
/// sequence file, so the first self-record's sequence comes from the run's
/// clock.
#[allow(clippy::too_many_arguments)]
pub fn build_discovery_handle(
    secret_key: NodeSecretKey,
    direct_addrs: Vec<SocketAddr>,
    advertised_services: Vec<ZakuraServiceId>,
    handshake: &ZakuraHandshakeConfig,
    max_zakura_connections: usize,
    bootstrap_peer_count: usize,
    connected: watch::Receiver<Vec<ZakuraPeerId>>,
) -> Result<ZakuraDiscoveryHandle, DiscoveryWireError> {
    discovery::build_discovery_handle(
        secret_key,
        None,
        direct_addrs,
        advertised_services,
        handshake,
        max_zakura_connections,
        bootstrap_peer_count,
        connected,
    )
}

/// Seeds `discovery`'s book with `bootstrap_peers` as static dial
/// candidates.
pub async fn insert_static_bootstrap_candidates(
    discovery: &ZakuraDiscoveryHandle,
    bootstrap_peers: &[String],
) {
    discovery::insert_static_bootstrap_candidates(discovery, bootstrap_peers).await;
}

/// Spawns one maintained `bootstrap_dial` task per bootstrap peer on
/// `endpoint`.
pub fn spawn_native_bootstrap_dialer(
    endpoint: ZakuraEndpoint,
    bootstrap_peers: Vec<String>,
    limits: ZakuraLocalLimits,
) -> Vec<JoinHandle<()>> {
    discovery::spawn_native_bootstrap_dialer(endpoint, bootstrap_peers, limits)
}

/// Spawns the `discovery_dialer` task, which dials `discovery`'s
/// candidates on `endpoint`.
pub fn spawn_native_discovery_dialer(
    endpoint: ZakuraEndpoint,
    discovery: ZakuraDiscoveryHandle,
    limits: ZakuraLocalLimits,
    sought_services: Vec<ZakuraServiceId>,
) -> JoinHandle<()> {
    discovery::spawn_native_discovery_dialer(endpoint, discovery, limits, sought_services)
}
