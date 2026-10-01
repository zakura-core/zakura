#![no_main]

use arbitrary::Unstructured;
use libfuzzer_sys::fuzz_target;
use zakura_script_oracle::generate::ScriptCase;

fuzz_target!(|bytes: &[u8]| {
    if let Ok(case) = ScriptCase::arbitrary(&mut Unstructured::new(bytes)) {
        case.check();
    }
});
