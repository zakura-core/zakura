#![no_main]

use libfuzzer_sys::fuzz_target;
use zakura_network::zakura::replay_candidate_dial_bytes;

fuzz_target!(|data: &[u8]| {
    replay_candidate_dial_bytes(data);
});
