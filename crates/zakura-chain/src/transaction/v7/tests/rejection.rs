//! Encodings that the v7 decoder rejects, each of which `zcash_primitives` also rejects.

use crate::transaction::Transaction;

use super::{
    common_read, compact_size, decode, encode, transparent_auth, transparent_envelope,
    vectors::TEST_VECTORS, Envelope, OUTPOINT,
};

#[track_caller]
fn assert_rejected(bytes: &[u8], case: &str) {
    assert!(decode(bytes).is_err(), "zakura accepts: {case}");
    assert!(
        common_read(bytes).is_err(),
        "zcash_primitives accepts: {case}"
    );
}

#[track_caller]
fn assert_accepted(bytes: &[u8], case: &str) -> Transaction {
    let tx = decode(bytes).unwrap_or_else(|error| panic!("zakura rejects {case}: {error}"));
    assert_eq!(encode(&tx), bytes, "{case} re-encodes");
    common_read(bytes).unwrap_or_else(|error| panic!("zcash_primitives rejects {case}: {error}"));
    tx
}

/// The vector with the transparent, Sapling, Orchard, and Ironwood bundles.
fn full_vector() -> Envelope {
    let full = TEST_VECTORS
        .iter()
        .map(|vector| Envelope::parse(&hex::decode(vector.tx).expect("vectors are hex")))
        .find(|envelope| {
            [0, 2, 3, 4]
                .iter()
                .all(|bundle_type| envelope.effects.iter().any(|(t, _, _)| t == bundle_type))
        })
        .expect("a vector has the transparent and every shielded bundle");
    assert_accepted(&full.encode(), "the full vector");
    full
}

#[test]
fn rejects_header_fields() {
    let _init_guard = zakura_test::init();

    let base = transparent_envelope(OUTPOINT);
    assert_accepted(&base.encode(), "the base transaction");

    let mut not_overwintered = base.clone();
    not_overwintered.header[3] &= 0x7f;
    assert_rejected(
        &not_overwintered.encode(),
        "a v7 header without fOverwintered",
    );

    let mut group = base.clone();
    group.header[4] ^= 1;
    assert_rejected(&group.encode(), "a wrong version group ID");

    let mut branch = base.clone();
    let nu6_3 = u32::from(zcash_protocol::consensus::BranchId::Nu6_3);
    branch.header[8..12].copy_from_slice(&nu6_3.to_le_bytes());
    assert_rejected(&branch.encode(), "a pre-NU7.1 consensus branch ID");
}

#[test]
fn rejects_parsing_rule_violations() {
    let _init_guard = zakura_test::init();

    let base = transparent_envelope(OUTPOINT);

    let mut zero = base.clone();
    zero.deltas.push((0, 0, 0));
    assert_rejected(&zero.encode(), "a zero value pool delta");

    let mut unordered = base.clone();
    unordered.deltas = vec![(5, 0, 10), (0, 0, -10)];
    assert_rejected(&unordered.encode(), "deltas out of type order");

    let mut duplicate = base.clone();
    duplicate.deltas = vec![(0, 0, 10), (0, 0, -10)];
    assert_rejected(&duplicate.encode(), "two deltas of one type");

    let mut unordered_effects = base.clone();
    unordered_effects.deltas = vec![(1, 0, 10)];
    unordered_effects
        .effects
        .insert(0, (1, 0, coinbase_payload(1, 10, 0, &[], &[], None)));
    assert_rejected(&unordered_effects.encode(), "effects out of type order");

    let mut variants = base.clone();
    variants.deltas.push((0, 1, 10));
    assert_rejected(&variants.encode(), "two variants of one type");

    let mut orphan_auth = base.clone();
    orphan_auth.effects.clear();
    assert_rejected(
        &orphan_auth.encode(),
        "authorizing data without effecting data",
    );
}

#[test]
fn rejects_unregistered_and_misplaced_bundles() {
    let _init_guard = zakura_test::init();

    let base = transparent_envelope(OUTPOINT);

    for (case, edit) in [
        ("an unregistered type", (7, 0)),
        ("an unregistered variant", (5, 1)),
    ] {
        let mut envelope = base.clone();
        envelope.deltas.push((edit.0, edit.1, 10));
        assert_rejected(&envelope.encode(), case);
    }

    let mut coinbase_auth = base.clone();
    coinbase_auth
        .effects
        .push((1, 0, coinbase_payload(1, 10, 0, &[], &[], None)));
    coinbase_auth.auths.push((1, 0, Vec::new()));
    assert_rejected(&coinbase_auth.encode(), "coinbase authorizing data");

    for bundle_type in [5, 6] {
        let mut envelope = base.clone();
        envelope.effects.push((bundle_type, 0, Vec::new()));
        assert_rejected(&envelope.encode(), "fee or NSM effecting data");
    }

    let mut large = base.encode();
    // Replace the first effect's one-byte type with a type above MAX_COMPACT_SIZE.
    let effects_start = 20 + 1;
    assert_eq!(large[effects_start..effects_start + 2], [1, 0]);
    large.splice(
        effects_start + 1..effects_start + 2,
        compact_size(0x0200_0001),
    );
    assert_rejected(&large, "a type above MAX_COMPACT_SIZE");

    let mut non_canonical = base.encode();
    non_canonical.splice(effects_start + 1..effects_start + 2, [0xfd, 0x00, 0x00]);
    assert_rejected(&non_canonical, "a non-canonical compactSize type");
}

#[test]
fn rejects_shielded_deltas_without_bundles() {
    let _init_guard = zakura_test::init();

    for bundle_type in [2, 3, 4] {
        let mut envelope = transparent_envelope(OUTPOINT);
        envelope.deltas.push((bundle_type, 0, 10));
        assert_rejected(&envelope.encode(), "a shielded delta without its bundle");
    }

    // The NSM bundle has only a delta, which decodes, and consensus rejects.
    let mut nsm = transparent_envelope(OUTPOINT);
    nsm.deltas.push((6, 0, 10));
    let tx = assert_accepted(&nsm.encode(), "an NSM delta");
    let fields = tx.v7_fields().expect("the transaction is v7");
    assert_eq!(i64::from(fields.value_deltas.nsm), 10);

    let mut out_of_range = transparent_envelope(OUTPOINT);
    out_of_range.deltas.push((5, 0, i64::MAX));
    assert_rejected(&out_of_range.encode(), "a delta above MAX_MONEY");
}

#[test]
fn rejects_non_canonical_transparent_bundles() {
    let _init_guard = zakura_test::init();

    let mut empty = super::Envelope::empty();
    empty.effects.push((0, 0, vec![0, 0]));
    assert_rejected(&empty.encode(), "an empty transparent bundle");

    let mut missing_auth = transparent_envelope(OUTPOINT);
    missing_auth.auths.clear();
    assert_rejected(&missing_auth.encode(), "inputs without authorizing data");

    let mut outputs_only = transparent_envelope(OUTPOINT);
    let effect = outputs_only.effect_mut(0);
    effect.drain(..41);
    effect.insert(0, 0);
    let outputs_only_with_auth = outputs_only.clone();
    outputs_only.auths.clear();
    assert_accepted(&outputs_only.encode(), "outputs without inputs");
    assert_rejected(
        &outputs_only_with_auth.encode(),
        "authorizing data without inputs",
    );

    let mut trailing = transparent_envelope(OUTPOINT);
    trailing.effect_mut(0).push(0);
    assert_rejected(&trailing.encode(), "trailing effecting bytes");

    let mut trailing_auth = transparent_envelope(OUTPOINT);
    trailing_auth.auth_mut(0).push(0);
    assert_rejected(&trailing_auth.encode(), "trailing authorizing bytes");

    let mut short = transparent_envelope(OUTPOINT);
    short.auth_mut(0).pop();
    assert_rejected(&short.encode(), "a truncated payload");
}

#[test]
fn rejects_sighash_info_other_than_variant_0() {
    let _init_guard = zakura_test::init();

    for (case, info) in [
        ("sighash variant 1", vec![0x01, 0x01]),
        ("sighash data", vec![0x02, 0x00, 0x00]),
        ("an empty sighashInfo", vec![0x00]),
    ] {
        let mut envelope = transparent_envelope(OUTPOINT);
        let auth = envelope.auth_mut(0);
        auth.splice(..2, info);
        assert_rejected(&envelope.encode(), case);
    }
    assert_eq!(
        transparent_auth()[..2],
        super::super::codec::SIGHASH_INFO_V0
    );

    // A shielded signature's sighashInfo: the binding signature ends every shielded
    // authorizing payload.
    for bundle_type in [2, 3, 4] {
        let mut envelope = full_vector();
        let auth = envelope.auth_mut(bundle_type);
        let info = auth.len() - 65;
        auth[info] = 0x01;
        assert_rejected(&envelope.encode(), "a binding signature with variant 1");
    }
}

#[test]
fn rejects_missing_shielded_authorizing_data() {
    let _init_guard = zakura_test::init();

    for bundle_type in [2, 3, 4] {
        let mut envelope = full_vector();
        envelope.auths.retain(|(t, _, _)| *t != bundle_type);
        assert_rejected(
            &envelope.encode(),
            "a shielded bundle without authorization",
        );
    }
}

#[test]
fn checks_orchard_flags_per_pool() {
    let _init_guard = zakura_test::init();

    const ENABLE_CROSS_ADDRESS: u8 = 0b0000_0100;
    const RESERVED: u8 = 0b1000_0000;

    let mut orchard = full_vector();
    *orchard.effect_mut(3).last_mut().expect("flags") |= ENABLE_CROSS_ADDRESS;
    assert_rejected(
        &orchard.encode(),
        "an Orchard bundle with enableCrossAddress",
    );

    let mut ironwood = full_vector();
    *ironwood.effect_mut(4).last_mut().expect("flags") |= ENABLE_CROSS_ADDRESS;
    assert_accepted(
        &ironwood.encode(),
        "an Ironwood bundle with enableCrossAddress",
    );

    for bundle_type in [3, 4] {
        let mut envelope = full_vector();
        *envelope.effect_mut(bundle_type).last_mut().expect("flags") |= RESERVED;
        assert_rejected(&envelope.encode(), "a reserved flag bit");
    }
}

#[test]
fn rejects_non_canonical_proof_sizes() {
    let _init_guard = zakura_test::init();

    for bundle_type in [3, 4] {
        let mut envelope = full_vector();
        let auth = envelope.auth_mut(bundle_type);
        // The proof follows the 32-byte anchor and its compactSize length.
        let (len, prefix) = match auth[32] {
            0xfd => (usize::from(u16::from_le_bytes([auth[33], auth[34]])), 3),
            n => (usize::from(n), 1),
        };
        let mut longer = auth[..32].to_vec();
        longer.extend(compact_size(super::usize_to_u64(len + 1)));
        longer.extend_from_slice(&auth[32 + prefix..32 + prefix + len]);
        longer.push(0);
        longer.extend_from_slice(&auth[32 + prefix + len..]);
        *auth = longer;
        assert_rejected(&envelope.encode(), "a proof one byte too long");
    }
}

/// Encodes the effecting data of a coinbase bundle.
pub(super) fn coinbase_payload(
    height: u32,
    block_subsidy: u64,
    lockbox_value: u64,
    coinbase_data: &[u8],
    signals: &[(u8, u8)],
    poll_digest: Option<[u8; 16]>,
) -> Vec<u8> {
    let mut payload = height.to_le_bytes().to_vec();
    payload.extend_from_slice(&block_subsidy.to_le_bytes());
    payload.extend_from_slice(&lockbox_value.to_le_bytes());
    payload.extend(compact_size(super::usize_to_u64(coinbase_data.len())));
    payload.extend_from_slice(coinbase_data);
    payload.extend(compact_size(super::usize_to_u64(signals.len())));
    for (code, level) in signals {
        payload.extend_from_slice(&[*code, *level]);
    }
    if let Some(poll_digest) = poll_digest {
        payload.extend_from_slice(&poll_digest);
    }
    payload
}

#[test]
fn checks_coinbase_fields() {
    let _init_guard = zakura_test::init();

    let envelope = |payload: Vec<u8>| {
        let mut envelope = Envelope::empty();
        envelope.effects.push((1, 0, payload));
        envelope.encode()
    };

    for (case, payload) in [
        (
            "the lowest height",
            coinbase_payload(1, 10, 10, &[], &[], None),
        ),
        (
            "the highest height",
            coinbase_payload(499_999_999, 10, 0, &[], &[], None),
        ),
        (
            "94 bytes of coinbaseData",
            coinbase_payload(1, 10, 0, &[7; 94], &[], None),
        ),
        (
            "a miner poll",
            coinbase_payload(1, 10, 0, &[], &[(1, 0), (2, 9)], Some([3; 16])),
        ),
    ] {
        let tx = assert_accepted(&envelope(payload), case);
        assert!(tx.is_coinbase(), "{case}");
    }

    for (case, payload) in [
        ("height 0", coinbase_payload(0, 10, 0, &[], &[], None)),
        (
            "a height of 500000000",
            coinbase_payload(500_000_000, 10, 0, &[], &[], None),
        ),
        (
            "a lockbox value above the subsidy",
            coinbase_payload(1, 10, 11, &[], &[], None),
        ),
        (
            "95 bytes of coinbaseData",
            coinbase_payload(1, 10, 0, &[7; 95], &[], None),
        ),
        (
            "descending signal codes",
            coinbase_payload(1, 10, 0, &[], &[(2, 0), (1, 0)], Some([3; 16])),
        ),
        (
            "a repeated signal code",
            coinbase_payload(1, 10, 0, &[], &[(1, 0), (1, 0)], Some([3; 16])),
        ),
        (
            "signals without a poll digest",
            coinbase_payload(1, 10, 0, &[], &[(1, 0)], None),
        ),
        (
            "a poll digest without signals",
            coinbase_payload(1, 10, 0, &[], &[], Some([3; 16])),
        ),
        (
            "a subsidy above MAX_MONEY",
            coinbase_payload(1, u64::MAX, 0, &[], &[], None),
        ),
    ] {
        assert_rejected(&envelope(payload), case);
    }
}

/// The decoder rejects a coinbase transaction's Sapling spends before reading them, as it
/// does for v5 and v6 (GHSA-rgwx-8r98-p34c). `zcash_primitives` rejects such a
/// transaction too, as a bundle-local rule.
#[test]
fn rejects_coinbase_sapling_spends_early() {
    let _init_guard = zakura_test::init();

    // The full vector's Sapling spends, in a coinbase transaction without the other
    // spending bundles.
    let mut envelope = full_vector()
        .without_bundle(0)
        .without_bundle(3)
        .without_bundle(4);
    envelope.deltas.retain(|(t, _, _)| *t != 5);
    envelope
        .effects
        .insert(0, (1, 0, coinbase_payload(1, 10, 0, &[], &[], None)));
    let bytes = envelope.encode();
    assert!(super::is_coinbase_sapling_spends(&decode(&bytes)));
    let error = common_read(&bytes).expect_err("zcash_primitives rejects the spends");
    assert!(super::is_bundle_rule_error(&error), "{error}");

    // A spend count that the payload cannot hold gets the coinbase error, so the check
    // runs before the count is used to read or allocate spends.
    let mut envelope = Envelope::empty();
    envelope
        .effects
        .push((1, 0, coinbase_payload(1, 10, 0, &[], &[], None)));
    envelope.effects.push((2, 0, compact_size(0xffff)));
    envelope.auths.push((2, 0, Vec::new()));
    assert!(super::is_coinbase_sapling_spends(&decode(
        &envelope.encode()
    )));
}
