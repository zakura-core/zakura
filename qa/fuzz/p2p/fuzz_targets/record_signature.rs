#![no_main]

use libfuzzer_sys::fuzz_target;
use zakura_network::zakura::replay_record_signature_bytes;

fuzz_target!(|data: &[u8]| {
    replay_record_signature_bytes(data);
});
