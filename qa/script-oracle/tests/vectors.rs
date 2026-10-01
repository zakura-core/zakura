//! The upstream `zcash_script` test vectors, compared under Zakura's flags.

use zakura_script_oracle::{check_script, check_script_sigops};
use zcash_script::{
    script,
    test_vectors::test_vectors,
    testing::{invalid_sighash, missing_sighash, sighash},
};

#[test]
fn upstream_vectors_agree() {
    for vector in test_vectors() {
        // `run` hands over the vector's scripts. Each vector's own flags and expected result are
        // ignored: the comparison uses Zakura's flags and the C++ result.
        let _ = vector.run(
            &|raw: &script::Raw, _| {
                for callback in [sighash, invalid_sighash, missing_sighash] {
                    for (lock_time, is_final) in [(0, true), (0, false), (500_000_000, false)] {
                        check_script(raw, lock_time, is_final, &callback);
                    }
                }
                check_script_sigops(&raw.sig.0);
                Ok(true)
            },
            &|code: &script::Code| {
                check_script_sigops(&code.0);
                0
            },
        );
    }
}
