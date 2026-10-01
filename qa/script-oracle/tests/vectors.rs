//! Fixed vectors, compared under Zakura's flags.

use zakura_script_oracle::{check_script, check_script_sigops, Verdict};
use zcash_script::{
    script,
    signature::HashType,
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

/// A DER signature (low S) by secret key `0x11` × 32 over the digest `0x42` × 32.
const SIGNATURE: &str = "304402200f394e2439520511a6bc9c9abdfc2238e845bd8b6d57a555a319fbac502d51de022025ba594ec4bf19aad4391a95ecffd6f08f7cffbc721637bae3c37f9b16862b2f";
/// The same signature with S replaced by n - S.
const HIGH_S_SIGNATURE: &str = "304502200f394e2439520511a6bc9c9abdfc2238e845bd8b6d57a555a319fbac502d51de022100da45a6b13b40e6552bc6e56a1300290e2b31dd2a3d326880dc0edef1b9b01612";
/// The same signature with a 33-byte R whose first byte is 1, so R overflows the group order.
const OVERFLOWING_R_SIGNATURE: &str = "30450221010f394e2439520511a6bc9c9abdfc2238e845bd8b6d57a555a319fbac502d51de022025ba594ec4bf19aad4391a95ecffd6f08f7cffbc721637bae3c37f9b16862b2f";
/// The compressed public key of secret key `0x11` × 32.
const PUBLIC_KEY: &str = "034f355bdcb7cc0af728ef3cceb9615d90684bb5b2ca5f859ab0f0b704075871aa";
/// The coordinates of [`PUBLIC_KEY`]. Y is odd.
const COORDINATES: &str = "4f355bdcb7cc0af728ef3cceb9615d90684bb5b2ca5f859ab0f0b704075871aa385b6b1b8ead809ca67454d9683fcf2ba03456d6fe2c4abe2b07f0fbdbb2f1c1";

/// Returns `0x42` × 32 for the six canonical hash types and rejects the rest, like ZIP-244.
fn zip244_sighash(_: &script::Code, hash_type: &HashType) -> Option<[u8; 32]> {
    matches!(hash_type.raw_bits(), 0x01..=0x03 | 0x81..=0x83).then_some([0x42; 32])
}

/// Concatenates hex strings.
fn bytes(parts: &[&str]) -> Vec<u8> {
    hex::decode(parts.concat()).expect("valid hex")
}

/// Vectors from the C++/Rust semantic audit, one per rule the interpreters must share.
#[test]
fn audit_vectors_agree() {
    use Verdict::{Accepted, Rejected};
    let (sig, pk) = (SIGNATURE, PUBLIC_KEY);
    let nops = |n: usize| "61".repeat(n);
    let push_521 = format!("4d0902{}", "07".repeat(521));
    let push_520 = format!("4d0802{}", "07".repeat(520));
    let multisig_after_nops = |n: usize| bytes(&["0000", &nops(n), &"00".repeat(20), "0114ae"]);
    let cases = vec![
        (
            "OP_VERIF in a dead branch",
            vec![],
            bytes(&["0063656851"]),
            0,
            false,
            Rejected,
        ),
        (
            "OP_VER in a dead branch",
            vec![],
            bytes(&["0063626851"]),
            0,
            false,
            Accepted,
        ),
        (
            "invalid 0xba in a dead branch",
            vec![],
            bytes(&["0063ba6851"]),
            0,
            false,
            Accepted,
        ),
        (
            "invalid 0xff in a dead branch",
            vec![],
            bytes(&["0063ff6851"]),
            0,
            false,
            Accepted,
        ),
        (
            "OP_RESERVED in a dead branch",
            vec![],
            bytes(&["0063506851"]),
            0,
            false,
            Accepted,
        ),
        (
            "OP_RETURN in a dead branch",
            vec![],
            bytes(&["00636a6851"]),
            0,
            false,
            Accepted,
        ),
        (
            "OP_CAT in a dead branch",
            vec![],
            bytes(&["00637e6851"]),
            0,
            false,
            Rejected,
        ),
        (
            "OP_CODESEPARATOR in a dead branch",
            vec![],
            bytes(&["0063ab6851"]),
            0,
            false,
            Rejected,
        ),
        (
            "521-byte push in a dead branch",
            vec![],
            bytes(&["0063", &push_521, "6851"]),
            0,
            false,
            Rejected,
        ),
        (
            "520-byte push",
            vec![],
            bytes(&[&push_520, "7551"]),
            0,
            false,
            Accepted,
        ),
        (
            "201 counted opcodes",
            vec![],
            bytes(&[&nops(201), "51"]),
            0,
            false,
            Accepted,
        ),
        (
            "202 counted opcodes",
            vec![],
            bytes(&[&nops(202), "51"]),
            0,
            false,
            Rejected,
        ),
        (
            "202 opcodes with 200 in a dead branch",
            vec![],
            bytes(&["0063", &nops(200), "6851"]),
            0,
            false,
            Rejected,
        ),
        (
            "180 opcodes and a 20-key multisig",
            vec![],
            multisig_after_nops(180),
            0,
            false,
            Accepted,
        ),
        (
            "181 opcodes and a 20-key multisig",
            vec![],
            multisig_after_nops(181),
            0,
            false,
            Rejected,
        ),
        (
            "1000 stack elements",
            vec![],
            bytes(&[&"51".repeat(1000)]),
            0,
            false,
            Accepted,
        ),
        (
            "1001 stack elements",
            vec![],
            bytes(&[&"51".repeat(1001)]),
            0,
            false,
            Rejected,
        ),
        (
            "CLTV 2^32 - 1, not final",
            vec![],
            bytes(&["05ffffffff00b17551"]),
            u32::MAX,
            false,
            Accepted,
        ),
        (
            "CLTV 2^32 - 1, final",
            vec![],
            bytes(&["05ffffffff00b17551"]),
            u32::MAX,
            true,
            Rejected,
        ),
        (
            "CLTV time against a height",
            vec![],
            bytes(&["040065cd1db17551"]),
            499_999_999,
            false,
            Rejected,
        ),
        (
            "CLTV negative zero",
            vec![],
            bytes(&["0180b17551"]),
            0,
            false,
            Accepted,
        ),
        (
            "CLTV non-minimal negative zero",
            vec![],
            bytes(&["020180b17551"]),
            0,
            false,
            Rejected,
        ),
        (
            "P2PK",
            bytes(&["47", sig, "01"]),
            bytes(&["21", pk, "ac"]),
            0,
            false,
            Accepted,
        ),
        (
            "P2PK with high S",
            bytes(&["48", HIGH_S_SIGNATURE, "01"]),
            bytes(&["21", pk, "ac"]),
            0,
            false,
            Accepted,
        ),
        (
            "P2PK with a hybrid key",
            bytes(&["47", sig, "01"]),
            bytes(&["4107", COORDINATES, "ac"]),
            0,
            false,
            Accepted,
        ),
        (
            "hybrid key with the wrong parity",
            bytes(&["47", sig, "01"]),
            bytes(&["4106", COORDINATES, "ac91"]),
            0,
            false,
            Accepted,
        ),
        (
            "overflowing R",
            bytes(&["48", OVERFLOWING_R_SIGNATURE, "01"]),
            bytes(&["21", pk, "ac91"]),
            0,
            false,
            Accepted,
        ),
        // The C++ checker reuses an uninitialized sighash buffer when the callback writes
        // nothing, so it accepted these before the baseline adapter wrote a random hash.
        (
            "rejected hash type after a valid check",
            bytes(&["47", sig, "04", "47", sig, "01"]),
            bytes(&["21", pk, "ad", "21", pk, "ac"]),
            0,
            false,
            Rejected,
        ),
        (
            "rejected hash type in a 2-of-2 multisig",
            bytes(&["00", "47", sig, "04", "47", sig, "01"]),
            bytes(&["52", "21", pk, "21", pk, "52ae"]),
            0,
            false,
            Rejected,
        ),
    ];
    for (name, script_sig, script_pubkey, lock_time, is_final, expected) in cases {
        let raw = script::Raw::from_raw_parts(script_sig, script_pubkey);
        assert_eq!(
            check_script(&raw, lock_time, is_final, &zip244_sighash),
            expected,
            "{name}"
        );
    }
}
