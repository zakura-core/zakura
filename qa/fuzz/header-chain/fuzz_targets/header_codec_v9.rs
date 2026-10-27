#![no_main]

use libfuzzer_sys::fuzz_target;
use zakura_network::zakura::replay_header_sync_v9_codec_bytes;

fuzz_target!(|data: &[u8]| {
    let _ = replay_header_sync_v9_codec_bytes(data);
});
