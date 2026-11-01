#![no_main]

use libfuzzer_sys::fuzz_target;
use zakura_network::zakura::replay_work_ownership_bytes;

fuzz_target!(|data: &[u8]| {
    replay_work_ownership_bytes(data);
});
