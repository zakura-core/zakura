#![no_main]

zakura_sim_fuzz::sim_target!(
    zakura_sim::harness::sync::SyncStorage,
    zakura_sim::explore::state::collector
);
