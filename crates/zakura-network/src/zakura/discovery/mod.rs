//! Native Zakura discovery service and dial supervision.

mod candidate_dialer;
#[cfg(test)]
mod conformance_tests;
mod dialer;
mod peer;
mod pipe;
mod protocol;
mod redial;
mod requester;
mod runtime;
mod service;
#[cfg(zakura_sim)]
pub(crate) use service::discovery_streams;
#[cfg(any(test, feature = "p2p-fuzz"))]
mod fuzz;
mod serving;
mod state;
#[cfg(any(test, feature = "p2p-fuzz"))]
pub use fuzz::{
    trace_discovery_book_bytes, trace_discovery_state_bytes, AdmittedFact, BookEntryFact, BookFact,
    BookStep, BookView, DiscoverySessionStep, DriverFailure, EvictionFact, HintFact, ImportFact,
    InstallFact, SessionView,
};
mod trace;
mod verified;
pub(crate) mod wire;
#[cfg(test)]
mod wire_tests;

#[cfg(any(test, feature = "zakura-testkit"))]
pub(crate) use candidate_dialer::run_native_discovery_dialer;
pub(crate) use candidate_dialer::{
    insert_static_bootstrap_candidates, spawn_native_discovery_dialer,
};
pub(crate) use dialer::{parse_bootstrap_peer, spawn_native_bootstrap_dialer};
#[cfg(any(test, feature = "p2p-fuzz"))]
pub use peer::{replay_discovery_session_bytes, DiscoverySessionReplaySummary};
#[cfg(test)]
pub(crate) use protocol::discovery_dial_slot_limit;
pub use protocol::*;
pub(crate) use redial::{native_dial_supervised, RedialPolicy};
#[cfg(any(test, feature = "p2p-fuzz"))]
pub use redial::{replay_redial_bytes, RedialReplaySummary};
pub(crate) use runtime::{
    build_discovery_handle, default_advertised_services, effective_discovery_connection_headroom,
};
pub use service::{DiscoveryPeerSession, DiscoveryService, DISCOVERY_BUDGET};
pub(crate) use serving::node_output_bytes as discovery_serving_output_bytes;

#[cfg(any(test, feature = "p2p-fuzz"))]
pub use requester::{
    replay_discovery_query_bytes, replay_discovery_settlement_bytes, DiscoveryQueryReplaySummary,
    DiscoverySettlementReplaySummary,
};

#[cfg(any(test, feature = "p2p-fuzz"))]
pub use candidate_dialer::{replay_candidate_dial_bytes, CandidateDialReplaySummary};
