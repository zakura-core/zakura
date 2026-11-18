#![no_main]

zakura_sim_fuzz::sim_target!(
    zakura_sim::harness::discovery_service::DiscoverySpam,
    zakura_sim::explore::state::collector
);
