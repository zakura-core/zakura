use libzcash_script::{CxxInterpreter, ZcashScript};
use zcash_script::{
    interpreter::{CallbackTransactionSignatureChecker, Flags},
    script::{Code, Raw},
    test_vectors::test_vectors,
};

fn compare(sig: Vec<u8>, pub_key: Vec<u8>, lock_time: u32, is_final: bool) {
    // A fixed successful callback avoids the original C++ missing-callback defect.
    // Transaction tests use the production adapter's real transaction/UTXO hashes.
    let sighash = |_: &Code, _: &zcash_script::signature::HashType| Some([0x42; 32]);
    let cxx = CxxInterpreter {
        sighash: &sighash,
        lock_time,
        is_final,
    };
    let checker = CallbackTransactionSignatureChecker {
        sighash: &sighash,
        lock_time: i64::from(lock_time),
        is_final,
    };
    let script = Raw::from_raw_parts(sig, pub_key);
    let flags = Flags::P2SH | Flags::CHECKLOCKTIMEVERIFY;
    let rust = script.eval(flags, &checker);
    let cpp = cxx.verify_callback(&script, flags);
    assert_eq!(
        rust.as_ref().is_ok_and(|v| *v),
        cpp.as_ref().is_ok_and(|v| *v),
        "script={script}, Rust={rust:?}, C++={cpp:?}"
    );
}

#[test]
fn upstream_execution_vectors() {
    for vector in test_vectors() {
        let sighash = zcash_script::testing::missing_sighash;
        let cxx = CxxInterpreter {
            sighash: &sighash,
            lock_time: 0,
            is_final: true,
        };
        zcash_script::testing::run_test_vector(
            &vector,
            true,
            &|script, flags| {
                cxx.verify_callback(script, flags)
                    .map_err(|(component, error)| match error {
                        libzcash_script::Error::Script(error) => (component, error),
                        error => panic!("C++ oracle failed: {error}"),
                    })
            },
            &|code| cxx.legacy_sigop_count_script(code),
        );
    }
}

#[test]
fn counting_boundaries() {
    let cxx = CxxInterpreter {
        sighash: &|_, _| Some([0x42; 32]),
        lock_time: 0,
        is_final: true,
    };
    for size in [0u32, 1, 75, 76, 255, 256, 520, 521, 65535, 65536] {
        for prefix in [0x4c, 0x4d, 0x4e] {
            let width = match prefix {
                0x4c => 1,
                0x4d => 2,
                _ => 4,
            };
            if (width == 1 && size > 255) || (width == 2 && size > 65535) {
                continue;
            }
            let mut bytes = vec![0xac, prefix];
            bytes.extend(&size.to_le_bytes()[..width]);
            bytes.resize(bytes.len() + usize::try_from(size).unwrap(), 0xac);
            bytes.extend([0x51, 0xae, 0xab, 0xac]);
            for truncated in [false, true] {
                let code = if truncated {
                    Code(bytes[..bytes.len() - 5].to_vec())
                } else {
                    Code(bytes.clone())
                };
                for accurate in [false, true] {
                    assert_eq!(
                        code.sig_op_count(accurate),
                        crate::cxx_sigops(&code.0, accurate)
                    );
                }
                assert_eq!(
                    code.sig_op_count(false),
                    cxx.legacy_sigop_count_script(&code).unwrap(),
                    "size={size}, prefix={prefix}, truncated={truncated}"
                );
            }
        }
    }
}

#[test]
fn deterministic_raw_script_corpus() {
    let cxx = CxxInterpreter {
        sighash: &|_, _| Some([0x42; 32]),
        lock_time: 0,
        is_final: true,
    };
    let mut seed = 0x2d58_39c1_u64;
    for case in 0..20_000 {
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed.to_le_bytes()[0]
        };
        let length = usize::from(next());
        let bytes: Vec<u8> = (0..length).map(|_| next()).collect();
        let code = Code(bytes.clone());
        assert_eq!(
            code.sig_op_count(false),
            cxx.legacy_sigop_count_script(&code).unwrap(),
            "case={case}, bytes={bytes:?}"
        );
        for accurate in [false, true] {
            assert_eq!(
                code.sig_op_count(accurate),
                crate::cxx_sigops(&bytes, accurate),
                "case={case}"
            );
        }
        let mut key = vec![0xa9, 0x14];
        key.extend([0; 20]);
        key.push(0x87);
        assert_eq!(
            Code(key.clone()).p2sh_sig_op_count(&code),
            crate::cxx_p2sh_sigops(&key, &bytes),
            "case={case}"
        );
        let split = if bytes.is_empty() {
            0
        } else {
            usize::from(next()) % bytes.len()
        };
        compare(
            bytes[..split].to_vec(),
            bytes[split..].to_vec(),
            u32::from(next()),
            next() & 1 == 0,
        );
    }
}

#[test]
fn lock_time_boundaries() {
    for tx_lock_time in [0u32, 499_999_999, 500_000_000, 500_000_001, u32::MAX] {
        for operand in [0u32, 499_999_999, 500_000_000, 500_000_001, u32::MAX] {
            let mut value = operand.to_le_bytes().to_vec();
            while value.last() == Some(&0) {
                value.pop();
            }
            if value.last().is_some_and(|byte| byte & 0x80 != 0) {
                value.push(0);
            }
            let mut script = vec![u8::try_from(value.len()).unwrap()];
            script.extend(value);
            script.extend([0xb1, 0x75, 0x51]);
            for final_sequence in [false, true] {
                compare(vec![], script.clone(), tx_lock_time, final_sequence);
            }
        }
    }
}

#[test]
fn p2sh_oversized_and_truncated_pushes() {
    let mut key = vec![0xa9, 0x14];
    key.extend([0; 20]);
    key.push(0x87);
    for size in [520u16, 521, 1000] {
        let mut sig = vec![0x4d];
        sig.extend(size.to_le_bytes());
        sig.resize(sig.len() + usize::from(size), 0xac);
        sig.extend([0x02, 0x51, 0xae]);
        for suffix in [vec![], vec![0x50], vec![0x51], vec![0x61], vec![0x4d, 0x01]] {
            let sig = [sig.clone(), suffix].concat();
            assert_eq!(
                Code(key.clone()).p2sh_sig_op_count(&Code(sig.clone())),
                crate::cxx_p2sh_sigops(&key, &sig)
            );
        }
    }
}
