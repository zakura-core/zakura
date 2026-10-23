#![no_main]

use libfuzzer_sys::fuzz_target;
use zakura_network::zakura::replay_header_reactor_bytes;

fuzz_target!(|data: &[u8]| { replay_header_reactor_bytes(data); });
