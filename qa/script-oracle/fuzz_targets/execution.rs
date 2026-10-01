#![no_main]
use libfuzzer_sys::fuzz_target;
use libzcash_script::{CxxInterpreter, ZcashScript};
use zcash_script::{
    interpreter::{CallbackTransactionSignatureChecker, Flags},
    script::{Code, Raw},
};

fuzz_target!(|data: (u32, bool, Vec<u8>, Vec<u8>)| {
    let (lock_time, is_final, sig, pub_key) = data;
    if sig.len() > 10_001 || pub_key.len() > 10_001 {
        return;
    }
    let sighash = |_: &Code, _: &zcash_script::signature::HashType| Some([0x42; 32]);
    let flags = Flags::P2SH | Flags::CHECKLOCKTIMEVERIFY;
    let script = Raw::from_raw_parts(sig, pub_key);
    let rust = script.eval(
        flags,
        &CallbackTransactionSignatureChecker {
            sighash: &sighash,
            lock_time: i64::from(lock_time),
            is_final,
        },
    );
    let cpp = CxxInterpreter {
        sighash: &sighash,
        lock_time,
        is_final,
    }
    .verify_callback(&script, flags);
    assert_eq!(
        rust.is_ok_and(|v| v),
        cpp.is_ok_and(|v| v),
        "script={script}"
    );
});
