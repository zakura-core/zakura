#![no_main]

use libfuzzer_sys::fuzz_target;
use zakura_network::zakura::bpv1::replay_bpv1_peer_bytes;

// The replay asserts every `PeerState` invariant after each step, and the
// drain on `SessionEnd`.
fuzz_target!(|data: &[u8]| {
    replay_bpv1_peer_bytes(data);
});
