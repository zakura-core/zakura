#![no_main]

use libfuzzer_sys::fuzz_target;
use zakura_network::zakura::replay_discovery_book_bytes;

fuzz_target!(|data: &[u8]| { replay_discovery_book_bytes(data); });
