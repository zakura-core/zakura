#![no_main]

use libfuzzer_sys::fuzz_target;
use zakura_network::zakura::replay_service_session_bytes;

fuzz_target!(|data: &[u8]| {
    let _ = replay_service_session_bytes(data);
});
