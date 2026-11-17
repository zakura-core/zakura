#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    zakura_network::zakura::replay_discovery_session_bytes(data);
});
