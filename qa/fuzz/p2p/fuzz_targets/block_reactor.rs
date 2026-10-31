#![no_main]

use libfuzzer_sys::fuzz_target;
use zakura_network::zakura::replay_block_reactor_bytes;

fuzz_target!(|data: &[u8]| {
    replay_block_reactor_bytes(data);
});
