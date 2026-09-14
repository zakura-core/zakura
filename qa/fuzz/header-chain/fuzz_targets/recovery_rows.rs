#![no_main]

use libfuzzer_sys::fuzz_target;
use zakura_state::replay_recovery_rows_bytes;

fuzz_target!(|data: &[u8]| {
    let _ = replay_recovery_rows_bytes(data);
});
