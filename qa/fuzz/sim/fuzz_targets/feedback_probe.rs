#![no_main]

zakura_sim_fuzz::sim_target!(
    zakura_sim::explore::feedback::Probe,
    zakura_sim::explore::feedback::probe_collector
);
