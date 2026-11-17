#![no_main]

use libfuzzer_sys::fuzz_target;
use zakura_network::zakura::{replay_discovery_query_bytes, replay_discovery_settlement_bytes};

// Every input replays under both decoders, so a corpus keeps its decoder-v1 meaning.
fuzz_target!(|data: &[u8]| {
    replay_discovery_query_bytes(data);
    replay_discovery_settlement_bytes(data);
});
