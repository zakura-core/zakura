#![no_main]

use libfuzzer_sys::fuzz_target;
use zakura_network::zakura::replay_sequencer_order_bytes;

fuzz_target!(|data: &[u8]| { replay_sequencer_order_bytes(data); });
