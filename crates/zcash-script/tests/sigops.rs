use zcash_script::{
    interpreter::{Flags, NullSignatureChecker},
    script::{Code, Raw},
};

fn push(prefix: u8, size: usize) -> Vec<u8> {
    let size_u32 = u32::try_from(size).unwrap();
    let mut bytes = vec![prefix];
    match prefix {
        0x01..=0x4b => assert_eq!(size, usize::from(prefix)),
        0x4c => bytes.push(u8::try_from(size).unwrap()),
        0x4d => bytes.extend(u16::try_from(size).unwrap().to_le_bytes()),
        0x4e => bytes.extend(size_u32.to_le_bytes()),
        _ => panic!("invalid push prefix"),
    }
    // Signature opcodes inside push data must never contribute to the count.
    bytes.resize(bytes.len() + size, 0xac);
    bytes
}

#[test]
fn complete_pushes_count_following_sigops() {
    for (prefix, sizes) in [
        (0x01, &[1][..]),
        (0x4b, &[75][..]),
        (0x4c, &[0, 75, 76, 255][..]),
        (0x4d, &[0, 255, 256, 520, 521, 65535][..]),
        (0x4e, &[0, 520, 521, 65536][..]),
    ] {
        for &size in sizes {
            let mut bytes = push(prefix, size);
            bytes.extend([0xac, 0xad, 0x52, 0xae, 0xaf]);
            assert_eq!(Code(bytes.clone()).sig_op_count(false), 42);
            assert_eq!(Code(bytes).sig_op_count(true), 24);
        }
    }
}

#[test]
fn truncated_pushes_stop_counting() {
    for bytes in [
        vec![0xac, 0x02, 0xac],
        vec![0xac, 0x4c],
        vec![0xac, 0x4c, 2, 0xac],
        vec![0xac, 0x4d, 0xac],
        vec![0xac, 0x4d, 2, 0, 0xac],
        vec![0xac, 0x4e, 0xac, 0xac, 0xac],
        vec![0xac, 0x4e, 0xff, 0xff, 0xff, 0xff, 0xac],
    ] {
        for accurate in [false, true] {
            assert_eq!(Code(bytes.clone()).sig_op_count(accurate), 1);
        }
    }
}

#[test]
fn multisig_uses_only_immediately_preceding_small_integer() {
    for n in 1u8..=16 {
        for multisig in [0xae, 0xaf] {
            assert_eq!(
                Code(vec![0x50 + n, multisig]).sig_op_count(true),
                u32::from(n)
            );
            assert_eq!(Code(vec![0x50 + n, multisig]).sig_op_count(false), 20);
        }
    }
    for separator in [0x00, 0x4f, 0x50, 0x61, 0x7e, 0xab, 0xff] {
        assert_eq!(Code(vec![0x51, separator, 0xae]).sig_op_count(true), 20);
    }
    assert_eq!(Code(vec![0x51, 0x4c, 0x00, 0xae]).sig_op_count(true), 20);
}

#[test]
fn execution_still_rejects_oversized_pushes() {
    let script = Raw::from_raw_parts(vec![], push(0x4d, 521));
    assert!(script
        .eval(Flags::empty(), &NullSignatureChecker())
        .is_err());
    let script = Raw::from_raw_parts(vec![], push(0x4d, 520));
    assert!(script.eval(Flags::empty(), &NullSignatureChecker()).is_ok());
}

#[test]
fn p2sh_extraction_uses_raw_pushes_and_clears_small_integers() {
    let mut key = vec![0xa9, 0x14];
    key.extend([0; 20]);
    key.push(0x87);
    let key = Code(key);
    let mut sig = push(0x4d, 521);
    sig.extend([0x02, 0x51, 0xae]);
    assert_eq!(key.p2sh_sig_op_count(&Code(sig.clone())), 1);
    for suffix in [0x00, 0x50, 0x51, 0x60, 0x61, 0x4c, 0x4d, 0x4e] {
        assert_eq!(
            key.p2sh_sig_op_count(&Code([sig.clone(), vec![suffix]].concat())),
            0
        );
    }
    let mut oversized_redeem = push(0x4d, 521);
    oversized_redeem.push(0xac);
    let mut sig = vec![0x4d];
    sig.extend(u16::try_from(oversized_redeem.len()).unwrap().to_le_bytes());
    sig.extend(oversized_redeem);
    assert_eq!(key.p2sh_sig_op_count(&Code(sig)), 1);
}
