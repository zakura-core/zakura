#![no_main]

use libfuzzer_sys::fuzz_target;
use zakura_network::zakura::bpv1::fuzz::replay_bpv1_codec_bytes;

// Encoding, any-`k` decoding, the re-encode check, part proofs and
// canonical assembly on an arbitrary body.
fuzz_target!(|data: &[u8]| {
    replay_bpv1_codec_bytes(data);
});
