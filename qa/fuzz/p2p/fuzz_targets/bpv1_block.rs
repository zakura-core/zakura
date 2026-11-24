#![no_main]

use libfuzzer_sys::fuzz_target;
use zakura_network::zakura::bpv1::replay_bpv1_block_bytes;

// The replay asserts every `BlockTable` invariant after each step.
fuzz_target!(|data: &[u8]| {
    replay_bpv1_block_bytes(data);
});
