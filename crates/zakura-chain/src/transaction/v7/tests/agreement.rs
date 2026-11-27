//! Deterministic byte mutations of the known-answer vectors, which the v7 decoder and
//! `zcash_primitives` must accept or reject alike, apart from two known divergences:
//!
//! - The decoder keeps an Orchard or Ironwood action's `rk` as bytes and rejects only the
//!   identity, as for v5 and v6, while `zcash_primitives` rejects any encoding that is
//!   not a valid point. The difference is harmless: spend authorization signature
//!   verification fails for such an `rk`.
//! - The decoder accepts a transaction that breaks a ZIP 248 bundle-local rule, which
//!   consensus rejects, while `zcash_primitives` rejects it when decoding. The rules are
//!   a transparent or coinbase delta without its bundle, and a coinbase transaction that
//!   spends from a pool, except Sapling spends, which both decoders reject.

use crate::transaction::Transaction;

use super::{
    breaks_bundle_local_rule, coinbase::v7_coinbase_envelope, common_read, decode,
    is_bundle_rule_error, transparent_envelope, usize_to_u64, vectors::TEST_VECTORS, Envelope,
    OUTPOINT,
};

/// A small deterministic generator, so the mutations are the same on every run.
struct XorShift(u64);

impl XorShift {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, bound: usize) -> usize {
        usize::try_from(self.next() % usize_to_u64(bound)).expect("below a usize bound")
    }
}

/// Applies mutation `kind` at `position`, returning the result and a description.
fn mutate(base: &[u8], position: usize, kind: usize, rng: &mut XorShift) -> (Vec<u8>, String) {
    let mut bytes = base.to_vec();
    let description = match kind {
        0 => {
            let bit = rng.below(8);
            bytes[position] ^= 1 << bit;
            format!("flip bit {bit}")
        }
        1 => {
            bytes[position] = 0;
            "set 0x00".into()
        }
        2 => {
            bytes[position] = 0xff;
            "set 0xff".into()
        }
        3 => {
            bytes[position] = bytes[position].wrapping_add(1);
            "increment".into()
        }
        4 => {
            bytes[position] = bytes[position].wrapping_sub(1);
            "decrement".into()
        }
        5 => {
            bytes.remove(position);
            "delete".into()
        }
        6 => {
            let byte = u8::try_from(rng.below(256)).expect("below 256");
            bytes.insert(position, byte);
            format!("insert {byte:#04x}")
        }
        _ => {
            bytes.truncate(position);
            "truncate".into()
        }
    };
    (bytes, format!("{description} at {position}"))
}

/// Returns `true` if an Orchard or Ironwood action's `rk` is not a valid point.
fn has_invalid_rk(tx: &Transaction) -> bool {
    let invalid = |rk| reddsa::VerificationKey::<reddsa::orchard::SpendAuth>::try_from(rk).is_err();
    tx.orchard_actions().any(|action| invalid(action.rk))
        || tx.ironwood_actions().any(|action| invalid(action.rk))
}

/// Returns `true` if `error` is `zcash_primitives` rejecting an action's `rk` as not a
/// valid point.
fn is_invalid_rk_error(error: &std::io::Error) -> bool {
    error
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<::orchard::ActionDecodeError>())
        == Some(&::orchard::ActionDecodeError::InvalidRk)
}

/// How the two decoders treated one encoding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    BothAccept,
    BothReject,
    /// The first known divergence: an `rk` that is not a valid point.
    InvalidRk,
    /// The second known divergence: a ZIP 248 bundle-local rule.
    BundleLocalRule,
}

/// Decodes `bytes` with both decoders, returning a description of any divergence other
/// than the two known ones.
fn compare(bytes: &[u8]) -> Result<Outcome, String> {
    let zakura = decode(bytes);
    let common = common_read(bytes);
    match (&zakura, &common) {
        (Ok(_), Ok(_)) => Ok(Outcome::BothAccept),
        (Err(_), Err(_)) => Ok(Outcome::BothReject),
        (Ok(tx), Err(error)) if has_invalid_rk(tx) && is_invalid_rk_error(error) => {
            Ok(Outcome::InvalidRk)
        }
        (Ok(tx), Err(error)) if breaks_bundle_local_rule(tx) && is_bundle_rule_error(error) => {
            Ok(Outcome::BundleLocalRule)
        }
        _ => Err(format!(
            "zakura {:?}, zcash_primitives {:?}",
            zakura.as_ref().err(),
            common.as_ref().err()
        )),
    }
}

/// The encodings that the mutations start from: the known-answer vectors.
fn bases() -> Vec<Envelope> {
    TEST_VECTORS
        .iter()
        .map(|vector| Envelope::parse(&hex::decode(vector.tx).expect("vectors are hex")))
        .collect()
}

#[test]
fn mutated_vectors_decode_alike() {
    let _init_guard = zakura_test::init();

    let mut rng = XorShift(0x2480_0007_5eed_cafe);
    let mut outcomes = Vec::new();
    let mut divergences = Vec::new();
    let mut record = |bytes: &[u8], description: String| match compare(bytes) {
        Ok(outcome) => outcomes.push(outcome),
        Err(divergence) => divergences.push(format!("{description}: {divergence}")),
    };

    for (index, base) in bases().iter().enumerate() {
        let base = base.encode();
        // Every byte of the common fields and the first entries, where the structure is
        // densest, then positions across the whole transaction.
        let mut positions: Vec<usize> = (0..base.len().min(160)).collect();
        positions.extend((0..800).map(|_| rng.below(base.len())));
        for position in positions {
            let (bytes, description) = mutate(&base, position, rng.below(8), &mut rng);
            record(&bytes, format!("base {index}, {description}"));
        }
    }
    let random = outcomes.len();

    // Every byte of each coinbase bundle that both decoders accept, where the coinbase
    // field rules apply.
    let mut coinbase_outcomes = Vec::new();
    for (index, base) in bases().into_iter().enumerate() {
        let Some(position) = base.effects.iter().position(|(t, _, _)| *t == 1) else {
            continue;
        };
        if compare(&base.encode()) != Ok(Outcome::BothAccept) {
            continue;
        }
        let payload = base.effects[position].2.clone();
        for byte in 0..payload.len() {
            for kind in 0..6 {
                let (mutated, description) = mutate(&payload, byte, kind, &mut rng);
                let mut envelope = base.clone();
                envelope.effects[position].2 = mutated;
                match compare(&envelope.encode()) {
                    Ok(outcome) => coinbase_outcomes.push(outcome),
                    Err(divergence) => divergences.push(format!(
                        "base {index}, coinbase payload {description}: {divergence}"
                    )),
                }
            }
        }
    }

    // An `rk` that is not a valid point, nor the identity, in each Orchard-protocol pool.
    let full = bases().remove(0);
    for bundle_type in [3, 4] {
        let mut envelope = full.clone();
        // The rk follows the action count's one byte, the cv, and the nullifier.
        envelope.effect_mut(bundle_type)[65..97].copy_from_slice(&[0xff; 32]);
        assert_eq!(
            compare(&envelope.encode()),
            Ok(Outcome::InvalidRk),
            "bundle type {bundle_type}"
        );
    }

    assert!(
        divergences.is_empty(),
        "{} divergences:\n{}",
        divergences.len(),
        divergences.join("\n")
    );
    // The mutations exercise both outcomes, not only rejections.
    assert!(
        count(&outcomes, Outcome::BothAccept) > 100,
        "{random} random mutations"
    );
    assert!(
        count(&outcomes, Outcome::BothReject) > 1000,
        "{random} random mutations"
    );
    assert!(count(&coinbase_outcomes, Outcome::BothAccept) > 10);
    assert!(count(&coinbase_outcomes, Outcome::BothReject) > 10);
}

/// Each ZIP 248 bundle-local rule is the known divergence, apart from a coinbase
/// transaction's Sapling spends, which both decoders reject.
#[test]
fn bundle_local_rules_are_the_known_divergence() {
    let _init_guard = zakura_test::init();

    let mut transparent_delta = Envelope::empty();
    transparent_delta.deltas = vec![(0, 0, 10), (5, 0, -10)];

    let mut coinbase_delta = transparent_envelope(OUTPOINT);
    coinbase_delta.deltas = vec![(1, 0, 10), (5, 0, -10)];

    // The coinbase transaction's outputs-only transparent bundle, with an input.
    let mut coinbase_input = v7_coinbase_envelope(1);
    let transparent = transparent_envelope(OUTPOINT);
    coinbase_input.effects[0] = transparent.effects[0].clone();
    coinbase_input.auths = transparent.auths;

    let full = bases()
        .into_iter()
        .find(|envelope| {
            [3, 4]
                .iter()
                .all(|bundle_type| envelope.effects.iter().any(|(t, _, _)| t == bundle_type))
        })
        .expect("a vector has Orchard and Ironwood bundles");
    let mut cases = vec![
        (
            "a transparent delta without a transparent bundle",
            transparent_delta,
        ),
        ("a coinbase delta without a coinbase bundle", coinbase_delta),
        (
            "a coinbase transaction with a transparent input",
            coinbase_input,
        ),
    ];
    for (bundle_type, case) in [
        (3, "a coinbase transaction with Orchard spends enabled"),
        (4, "a coinbase transaction with Ironwood spends enabled"),
    ] {
        const ENABLE_SPENDS: u8 = 0b0000_0001;
        // Only that bundle, with spends enabled, and a coinbase bundle.
        let mut envelope = full.clone();
        envelope.deltas.retain(|(t, _, _)| *t == bundle_type);
        envelope.effects.retain(|(t, _, _)| *t == bundle_type);
        envelope.auths.retain(|(t, _, _)| *t == bundle_type);
        *envelope.effect_mut(bundle_type).last_mut().expect("flags") |= ENABLE_SPENDS;
        envelope
            .effects
            .insert(0, v7_coinbase_envelope(1).effects[1].clone());
        cases.push((case, envelope));
    }

    for (case, envelope) in cases {
        assert_eq!(
            compare(&envelope.encode()),
            Ok(Outcome::BundleLocalRule),
            "{case}"
        );
    }
}

fn count(outcomes: &[Outcome], outcome: Outcome) -> usize {
    outcomes.iter().filter(|o| **o == outcome).count()
}
