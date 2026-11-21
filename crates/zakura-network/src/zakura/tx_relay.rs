//! `tx-relay`, the v2 service that moves transactions between peers and
//! hands them to the pool core.
//!
//! [`machine`] is the service's pure state machine, whose module docs state
//! its rules, and [`service`] the IO that drives it against the pool and the
//! wire.

// The adapter, cluster, conformance and service tests and their node
// stand-ins need the testkit's QUIC nodes, which the simulator's build
// leaves out.
#[cfg(all(test, not(zakura_sim)))]
mod adapter_tests;
#[cfg(all(test, not(zakura_sim)))]
mod cluster_tests;
pub(crate) mod config;
#[cfg(all(test, not(zakura_sim)))]
mod conformance_tests;
pub(crate) mod machine;
pub(crate) mod service;
#[cfg(all(test, not(zakura_sim)))]
mod service_tests;
pub(crate) mod sessions;
#[cfg(zakura_sim)]
pub mod sim;
#[cfg(all(test, not(zakura_sim)))]
mod test_node;
pub(crate) mod wire;
#[cfg(test)]
mod wire_tests;

pub use config::ZakuraTxRelayConfig;
pub use service::{MayCool, TxRelayService};
pub use sessions::TxRelaySessions;

use std::sync::Arc;

use zakura_node_services::mempool::port::PoolHandle;

use super::{CustomService, ZakuraServiceId};

/// The service id `tx-relay` advertises and seeks in discovery.
pub fn tx_relay_service_id() -> ZakuraServiceId {
    ZakuraServiceId::try_from("tx-relay")
        .expect("the tx-relay service id is non-empty bounded ASCII")
}

/// Starts `tx-relay` on `pool` with `config`'s fan-out and returns it as a
/// service to register. `may_cool` says whether a verdict at a height may
/// start a cooldown. A session whose address is in `unconditional` is never
/// charged for a body, as `network.unconditional_peers` promises.
///
/// The service carries its live-session record, so the legacy adapter stops
/// moving transactions for each peer that has a live `tx-relay` session.
pub fn tx_relay_service(
    pool: PoolHandle,
    may_cool: MayCool,
    config: &ZakuraTxRelayConfig,
    unconditional: crate::UnconditionalPeers,
) -> CustomService {
    let sessions = TxRelaySessions::default();
    let service: Arc<TxRelayService> =
        TxRelayService::spawn(pool, may_cool, config, unconditional, sessions.clone());
    CustomService {
        service,
        provides: vec![tx_relay_service_id()],
        seeks: vec![tx_relay_service_id()],
        tx_relay_sessions: Some(sessions),
    }
}
