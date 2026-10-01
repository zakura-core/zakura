#![no_main]

use libfuzzer_sys::fuzz_target;
use zakura_script_oracle::{check_p2sh_sigops, check_script_sigops};

fuzz_target!(|bytes: &[u8]| {
    check_script_sigops(bytes);
    check_p2sh_sigops(bytes);
});
