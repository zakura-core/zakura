//! Fixed encoding boundaries under the adapter's P2SH | CHECKLOCKTIMEVERIFY flags.
//!
//! The replaced libzcash_script 0.1.0 interpreter checks DER unconditionally;
//! STRICTENC and LOW_S are not enabled. These are not Bitcoin policy tests.

use zcash_script::{
    interpreter::{CallbackTransactionSignatureChecker, Flags},
    script,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    Valid,
    Invalid,
    Abort,
}

fn evaluate(raw: &script::Raw) -> Outcome {
    match raw.eval(
        Flags::P2SH | Flags::CHECKLOCKTIMEVERIFY,
        &CallbackTransactionSignatureChecker {
            sighash: &|_, _| Some([0x42; 32]),
            lock_time: 0,
            is_final: true,
        },
    ) {
        Ok(true) => Outcome::Valid,
        Ok(false) => Outcome::Invalid,
        Err(_) => Outcome::Abort,
    }
}

fn push(script: &mut Vec<u8>, bytes: &[u8]) {
    let len = u8::try_from(bytes.len()).expect("encoding fixtures fit in PUSHDATA1");
    if len > 75 {
        script.push(0x4c);
    }
    script.push(len);
    script.extend(bytes);
}

fn integer(value: &[u8]) -> Vec<u8> {
    let first = value
        .iter()
        .position(|&byte| byte != 0)
        .unwrap_or(value.len());
    let mut encoded = value[first..].to_vec();
    if encoded.first().is_none_or(|byte| byte & 0x80 != 0) {
        encoded.insert(0, 0);
    }
    encoded
}

fn der(r: &[u8], s: &[u8]) -> Vec<u8> {
    let len = |v: &[u8]| u8::try_from(v.len()).expect("fixture scalar fits one DER length byte");
    [
        vec![0x30, len(r) + len(s) + 4, 2, len(r)],
        r.to_vec(),
        vec![2, len(s)],
        s.to_vec(),
    ]
    .concat()
}

/// Shared with the archived C++ differential harness; see the qualification document.
fn encoding_cases(evaluate: impl Fn(&script::Raw) -> Outcome) {
    use Outcome::{Abort, Invalid, Valid};

    let decode = |s| hex::decode(s).expect("fixture hex is valid");
    // Secret key 0x11 * 32, digest 0x42 * 32. Keep the vectors independent of signing code.
    let signature = decode("304402200f394e2439520511a6bc9c9abdfc2238e845bd8b6d57a555a319fbac502d51de022025ba594ec4bf19aad4391a95ecffd6f08f7cffbc721637bae3c37f9b16862b2f");
    let high_s = decode("304502200f394e2439520511a6bc9c9abdfc2238e845bd8b6d57a555a319fbac502d51de022100da45a6b13b40e6552bc6e56a1300290e2b31dd2a3d326880dc0edef1b9b01612");
    let key = decode("034f355bdcb7cc0af728ef3cceb9615d90684bb5b2ca5f859ab0f0b704075871aa");
    let coordinates = decode("4f355bdcb7cc0af728ef3cceb9615d90684bb5b2ca5f859ab0f0b704075871aa385b6b1b8ead809ca67454d9683fcf2ba03456d6fe2c4abe2b07f0fbdbb2f1c1");
    let order = decode("fffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141");
    let prime = decode("fffffffffffffffffffffffffffffffffffffffffffffffffffffffefffffc2f");
    let r = &signature[4..36];
    let s = &signature[38..70];
    let signed = |mut bytes: Vec<u8>| {
        bytes.push(1);
        bytes
    };
    let mut cases: Vec<(String, Vec<u8>, Vec<u8>, Outcome)> = Vec::new();
    let mut sig_case = |name: &str, sig: Vec<u8>, outcome| {
        cases.push((name.to_owned(), signed(sig), key.clone(), outcome));
    };
    sig_case("low S", signature.clone(), Valid);
    sig_case("high S", high_s, Valid);
    sig_case("hash byte without DER", vec![], Abort);
    sig_case("zero-length R", der(&[], s), Abort);
    sig_case("zero-length S", der(r, &[]), Abort);
    sig_case("negative R", der(&[0x80], s), Abort);
    sig_case("negative S", der(r, &[0x80]), Abort);
    sig_case(
        "redundant zero in R",
        der(&[&[0][..], r].concat(), s),
        Abort,
    );
    sig_case(
        "redundant zero in S",
        der(r, &[&[0][..], s].concat()),
        Abort,
    );
    let mut long_length = signature.clone();
    long_length.insert(1, 0x81);
    sig_case("long-form sequence length", long_length, Abort);
    let mut trailing = signature.clone();
    trailing.push(0);
    sig_case("trailing DER byte", trailing, Abort);
    for (name, index, replacement) in [
        ("sequence tag", 0, 0x31),
        ("sequence length", 1, 0x43),
        ("R tag", 2, 3),
        ("R length overrun", 3, 0xff),
        ("S tag", 36, 3),
        ("S length overrun", 37, 0xff),
    ] {
        let mut malformed = signature.clone();
        malformed[index] = replacement;
        sig_case(name, malformed, Abort);
    }
    for len in 1..signature.len() {
        sig_case(
            &format!("truncated DER at {len}"),
            signature[..len].to_vec(),
            Abort,
        );
    }
    let mut below_order = order.clone();
    below_order[31] -= 1;
    let mut above_order = order.clone();
    above_order[31] += 1;
    for (name, scalar) in [
        ("zero", vec![0]),
        ("one", vec![1]),
        ("n-1", below_order),
        ("n", order),
        ("n+1", above_order),
        ("p", prime.clone()),
        ("2^256-1", vec![0xff; 32]),
        ("33-byte overflow", vec![1; 33]),
    ] {
        let encoded = integer(&scalar);
        sig_case(&format!("R {name}"), der(&encoded, s), Invalid);
        sig_case(&format!("S {name}"), der(r, &encoded), Invalid);
    }
    cases.push(("empty signature".into(), vec![], key.clone(), Invalid));
    let uncompressed = [&[4][..], &coordinates].concat();
    let mut off_curve = uncompressed.clone();
    off_curve[64] ^= 1;
    // secp256k1 has cofactor 1: there is no nontrivial on-curve small subgroup
    // fixture. The relevant boundary is rejecting infinity/non-curve encodings.
    for (name, public_key, outcome) in [
        ("uncompressed", uncompressed.clone(), Valid),
        (
            "hybrid matching parity",
            [&[7][..], &coordinates].concat(),
            Valid,
        ),
        (
            "hybrid wrong parity",
            [&[6][..], &coordinates].concat(),
            Invalid,
        ),
        ("uncompressed off curve", off_curve, Invalid),
        ("compressed x zero", [&[2][..], &[0; 32]].concat(), Invalid),
        ("x equals field prime", [&[2][..], &prime].concat(), Invalid),
        (
            "y equals field prime",
            [&uncompressed[..33], &prime].concat(),
            Invalid,
        ),
        ("infinity", vec![0], Invalid),
        ("empty key", vec![], Invalid),
        ("truncated key", key[..32].to_vec(), Invalid),
        ("trailing key byte", [&key[..], &[0]].concat(), Invalid),
        (
            "invalid compressed prefix",
            [&[5][..], &key[1..]].concat(),
            Invalid,
        ),
    ] {
        if outcome == Invalid {
            assert!(
                secp256k1::PublicKey::from_slice(&public_key).is_err(),
                "{name} must fail point parsing"
            );
        }
        cases.push((name.into(), signed(signature.clone()), public_key, outcome));
    }
    for (name, signature, public_key, expected) in cases {
        for multisig in [false, true] {
            for suffix in [0, 0x91, 0x69] {
                let mut sig = Vec::new();
                let mut pk = Vec::new();
                if multisig {
                    sig.push(0);
                    pk.push(0x51);
                }
                push(&mut sig, &signature);
                push(&mut pk, &public_key);
                if multisig {
                    pk.push(0x51);
                }
                pk.push(if multisig { 0xae } else { 0xac });
                let outcome = match (expected, suffix) {
                    (Abort, _) | (Invalid, 0x69) => Abort,
                    (Valid, 0x91) => Invalid,
                    (Invalid, 0x91) => Valid,
                    _ => expected,
                };
                if suffix != 0 {
                    pk.push(suffix);
                }
                if suffix == 0x69 {
                    pk.push(0x51);
                }
                assert_eq!(
                    evaluate(&script::Raw::from_raw_parts(sig, pk)),
                    outcome,
                    "{name}, multisig={multisig}, suffix={suffix:#x}"
                );
            }
        }
    }
}

#[test]
fn signature_encoding_consensus_matrix() {
    encoding_cases(evaluate);
}
