#![no_main]

use libfuzzer_sys::fuzz_target;
use zakura_network::zakura::bpv1::fuzz::replay_bpv1_mesh_bytes;

// Four nodes of production BPv1 machines with fuzz-chosen delivery order and
// an adversary. The replay asserts every machine invariant after each step,
// that no honest connection closes with a protocol row, and that every
// ended session drains.
fuzz_target!(|data: &[u8]| {
    replay_bpv1_mesh_bytes(data);
});
