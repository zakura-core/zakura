#![no_main]

use libfuzzer_sys::fuzz_target;
use zakura_network::zakura::replay_header_pursuit_bytes;

fuzz_target!(|data: &[u8]| {
    let _ = replay_header_pursuit_bytes(data);
});
