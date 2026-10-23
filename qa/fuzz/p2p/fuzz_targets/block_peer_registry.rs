#![no_main]

use libfuzzer_sys::fuzz_target;
use zakura_network::zakura::replay_peer_registry_bytes;

fuzz_target!(|data: &[u8]| { replay_peer_registry_bytes(data); });
