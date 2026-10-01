#![no_main]

use arbitrary::Unstructured;
use libfuzzer_sys::fuzz_target;
use zakura_script_oracle::generate::TransactionCase;

fuzz_target!(|bytes: &[u8]| {
    if let Ok(case) = TransactionCase::arbitrary(&mut Unstructured::new(bytes)) {
        case.check();
    }
});
