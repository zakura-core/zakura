#![no_main]

use libfuzzer_sys::fuzz_target;
use zakura_network::zakura::replay_request_lifecycle_bytes;

fuzz_target!(|data: &[u8]| { replay_request_lifecycle_bytes(data); });
