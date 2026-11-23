#![no_main]

use libfuzzer_sys::fuzz_target;
use zakura_quic::fuzz::replay_key_list_bytes;

fuzz_target!(|data: &[u8]| {
    replay_key_list_bytes(data);
});
