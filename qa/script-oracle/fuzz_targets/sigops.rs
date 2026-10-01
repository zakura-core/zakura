#![no_main]
use libfuzzer_sys::fuzz_target;
use libzcash_script::{CxxInterpreter, ZcashScript};
use zakura_script_oracle::{cxx_p2sh_sigops, cxx_sigops};
use zcash_script::script::Code;

fuzz_target!(|bytes: &[u8]| {
    if bytes.len() > 2_000_000 {
        return;
    }
    let code = Code(bytes.to_vec());
    for accurate in [false, true] {
        assert_eq!(code.sig_op_count(accurate), cxx_sigops(bytes, accurate));
    }
    let mut key = vec![0xa9, 0x14];
    key.extend([0; 20]);
    key.push(0x87);
    assert_eq!(
        Code(key.clone()).p2sh_sig_op_count(&code),
        cxx_p2sh_sigops(&key, bytes)
    );
    let cpp = CxxInterpreter {
        sighash: &|_, _| Some([0x42; 32]),
        lock_time: 0,
        is_final: true,
    };
    assert_eq!(
        code.sig_op_count(false),
        cpp.legacy_sigop_count_script(&code).unwrap()
    );
});
