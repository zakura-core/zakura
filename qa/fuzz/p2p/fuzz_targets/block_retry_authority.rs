#![no_main]

use libfuzzer_sys::fuzz_target;
use zakura_network::zakura::replay_retry_authority_bytes;

fuzz_target!(|data: &[u8]| { replay_retry_authority_bytes(data); });
