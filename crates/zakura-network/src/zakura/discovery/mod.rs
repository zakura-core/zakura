//! Native Zakura discovery service and dial supervision.

mod candidate_dialer;
#[cfg(test)]
mod conformance_tests;
mod dialer;
mod pipe;
mod protocol;
mod redial;
mod requester;
mod runtime;
mod service;
mod serving;
mod trace;
pub(crate) mod wire;
#[cfg(test)]
mod wire_tests;

#[cfg(any(test, feature = "zakura-testkit"))]
pub(crate) use candidate_dialer::run_native_discovery_dialer;
pub(crate) use candidate_dialer::{
    insert_static_bootstrap_candidates, spawn_native_discovery_dialer,
};
pub(crate) use dialer::{parse_bootstrap_peer, spawn_native_bootstrap_dialer};
#[cfg(test)]
pub(crate) use protocol::discovery_dial_slot_limit;
pub use protocol::*;
pub(crate) use redial::{native_dial_supervised, RedialPolicy};
pub(crate) use runtime::{
    build_discovery_handle, default_advertised_services, effective_discovery_connection_headroom,
};
pub use service::{DiscoveryPeerSession, DiscoveryService};
