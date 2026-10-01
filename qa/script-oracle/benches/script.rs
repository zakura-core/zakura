use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use libzcash_script::{CxxInterpreter, ZcashScript};
use ripemd::{Digest, Ripemd160};
use secp256k1::{Message, PublicKey, Secp256k1, SecretKey};
use sha2::Sha256;
use std::hint::black_box;
use zcash_script::{
    interpreter::{CallbackTransactionSignatureChecker, Flags},
    script::{Code, Raw},
};

fn push(data: &[u8]) -> Vec<u8> {
    assert!(data.len() <= 75);
    let mut bytes = vec![u8::try_from(data.len()).unwrap()];
    bytes.extend(data);
    bytes
}

fn p2sh(sig: &[u8], redeem: &[u8]) -> Raw {
    let mut script_sig = sig.to_vec();
    if redeem.len() > 75 {
        script_sig.extend([0x4c, u8::try_from(redeem.len()).unwrap()]);
        script_sig.extend(redeem);
    } else {
        script_sig.extend(push(redeem));
    }
    let hash = Ripemd160::digest(Sha256::digest(redeem));
    let mut script_pub_key = vec![0xa9];
    script_pub_key.extend(push(&hash));
    script_pub_key.push(0x87);
    Raw::from_raw_parts(script_sig, script_pub_key)
}

fn bench(c: &mut Criterion) {
    let secp = Secp256k1::new();
    let key = SecretKey::from_slice(&[0xcd; 32]).unwrap();
    let pub_key = PublicKey::from_secret_key(&secp, &key).serialize();
    let mut sig = secp
        .sign_ecdsa(&Message::from_digest([0x42; 32]), &key)
        .serialize_der()
        .to_vec();
    sig.push(1);
    let hash = Ripemd160::digest(Sha256::digest(pub_key));
    let mut p2pkh_pub = vec![0x76, 0xa9];
    p2pkh_pub.extend(push(&hash));
    p2pkh_pub.extend([0x88, 0xac]);
    let mut p2pkh_sig = push(&sig);
    p2pkh_sig.extend(push(&pub_key));
    let p2pkh = Raw::from_raw_parts(p2pkh_sig.clone(), p2pkh_pub.clone());
    let p2sh_p2pkh = p2sh(&p2pkh_sig, &p2pkh_pub);
    let mut multisig_pub = vec![0x52];
    for _ in 0..3 {
        multisig_pub.extend(push(&pub_key));
    }
    multisig_pub.extend([0x53, 0xae]);
    let mut multisig_sig = vec![0x00];
    multisig_sig.extend(push(&sig));
    multisig_sig.extend(push(&sig));
    let multisig = p2sh(&multisig_sig, &multisig_pub);
    let malformed = Raw::from_raw_parts(vec![], vec![0xac]);
    let late_fail = Raw::from_raw_parts(p2pkh_sig, [p2pkh_pub, vec![0x69, 0x00]].concat());
    let sighash = |_: &Code, _: &zcash_script::signature::HashType| Some([0x42; 32]);
    let cpp = CxxInterpreter {
        sighash: &sighash,
        lock_time: 0,
        is_final: true,
    };
    let rust = CallbackTransactionSignatureChecker {
        sighash: &sighash,
        lock_time: 0,
        is_final: true,
    };
    let flags = Flags::P2SH | Flags::CHECKLOCKTIMEVERIFY;
    let mut group = c.benchmark_group("execution");
    group.throughput(Throughput::Elements(1));
    for (name, script, valid) in [
        ("p2pkh", p2pkh, true),
        ("p2sh-p2pkh", p2sh_p2pkh, true),
        ("p2sh-multisig", multisig, true),
        ("malformed", malformed, false),
        ("late-fail", late_fail, false),
    ] {
        assert_eq!(script.eval(flags, &rust).is_ok_and(|v| v), valid);
        assert_eq!(cpp.verify_callback(&script, flags).is_ok_and(|v| v), valid);
        group.bench_with_input(BenchmarkId::new("rust", name), &script, |b, script| {
            b.iter(|| black_box(black_box(script).eval(flags, &rust)))
        });
        group.bench_with_input(BenchmarkId::new("cxx", name), &script, |b, script| {
            b.iter(|| black_box(cpp.verify_callback(black_box(script), flags)))
        });
    }
    group.finish();
    let mut group = c.benchmark_group("sigops");
    for size in [520u16, 521, 10_000] {
        let mut code = vec![0x4d];
        code.extend(size.to_le_bytes());
        code.resize(code.len() + usize::from(size), 0xac);
        code.push(0xac);
        let code = Code(code);
        assert_eq!(code.sig_op_count(false), 1);
        assert_eq!(cpp.legacy_sigop_count_script(&code).unwrap(), 1);
        group.bench_with_input(BenchmarkId::new("rust", size), &code, |b, code| {
            b.iter(|| black_box(black_box(code).sig_op_count(false)))
        });
        group.bench_with_input(BenchmarkId::new("cxx", size), &code, |b, code| {
            b.iter(|| black_box(cpp.legacy_sigop_count_script(black_box(code))))
        });
    }
    group.finish();
}

criterion_group! { name = benches; config = Criterion::default().sample_size(30).warm_up_time(std::time::Duration::from_secs(1)).measurement_time(std::time::Duration::from_secs(2)); targets = bench }
criterion_main!(benches);
