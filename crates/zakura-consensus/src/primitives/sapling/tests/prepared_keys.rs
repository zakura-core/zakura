//! Soundness tests for Sapling validation under the process-wide prepared verifying keys.
//!
//! The verifier used to build new verifying keys for every batch; the tests call that the "cold"
//! path, and it is still what [`validate_cold`] does. It now validates under one shared pair of
//! keys whose prepared G2 pairing terms outlive each batch, and the same `sapling-crypto` release
//! routes a batch of exactly one Spend and one Output proof through a new joint verifier and
//! packs nullifier public inputs with new code.
//!
//! None of that may change a single accept or reject decision. So every test here decides the
//! same batch both ways and requires the two to agree with each other and with the expected
//! answer:
//!
//! - over every real Sapling bundle in the mainnet and testnet test vectors,
//! - over mutations of those bundles that reach the Groth16 verifier (proofs, and every public
//!   input the Spend and Output circuits take), mutations caught by signatures, and mutations
//!   that must still be accepted,
//! - in every batch shape `validate_prepared` dispatches on: Spend proofs only, Output proofs
//!   only, one of each (the joint verifier), and more (the multicore verifier),
//! - across repeated, interleaved and concurrent use of the same keys, including their first use,
//! - and through the batch service, its single-item fallback, and its drop-time flush.

use std::sync::{Barrier, Mutex};

use bls12_381::Scalar;
use futures::future::join_all;
use once_cell::sync::Lazy;
use proptest::prelude::*;
use tower::{Service, ServiceExt};
use tower_batch_control::BatchControl;

use sapling_crypto::{
    bundle::{GrothProofBytes, OutputDescription, SpendDescription},
    circuit::{OutputVerifyingKey, SpendVerifyingKey},
    note::ExtractedNoteCommitment,
    BatchValidator, Nullifier, PreparedBatchVerifyingKeys,
};

use zakura_chain::transaction::Transaction;

use crate::error::TransactionError;

use super::{
    super::{validate, Verifier, VERIFYING_KEYS},
    item, mined_sapling_transactions, mutated_output, mutated_spend, sapling_prover,
    uncached_verification_behind_a_fresh_cache, Authorized, Block, Bundle, Height, Network,
    NetworkUpgrade, ZatBalance, ZcashDeserializeInto,
};

/// A Sapling bundle and the sighash its signatures are checked against.
#[derive(Clone)]
struct Fixture {
    /// Where the bundle came from and what was done to it, for assertion messages.
    name: String,
    bundle: Bundle<Authorized, ZatBalance>,
    sighash: [u8; 32],
}

impl Fixture {
    fn spends(&self) -> usize {
        self.bundle.shielded_spends().len()
    }

    fn outputs(&self) -> usize {
        self.bundle.shielded_outputs().len()
    }

    /// Returns this fixture with a different bundle, named after `change`.
    fn with_bundle(&self, change: &str, bundle: Bundle<Authorized, ZatBalance>) -> Self {
        Self {
            name: format!("{} with {change}", self.name),
            bundle,
            sighash: self.sighash,
        }
    }
}

/// Every transparent-input-free Sapling bundle in the mainnet and testnet test vectors, with the
/// sighash of the upgrade its block was mined under.
///
/// Transactions with transparent inputs are skipped because their sighash commits to the outputs
/// they spend, which are not in the test vectors.
static FIXTURES: Lazy<Vec<Fixture>> = Lazy::new(|| {
    let mut fixtures = Vec::new();

    for (network, blocks) in [
        (
            Network::Mainnet,
            zakura_test::vectors::MAINNET_BLOCKS.iter(),
        ),
        (
            Network::new_default_testnet(),
            zakura_test::vectors::TESTNET_BLOCKS.iter(),
        ),
    ] {
        for (height, bytes) in blocks {
            let block: Block = bytes
                .zcash_deserialize_into()
                .expect("hard-coded test vector must deserialize");
            let nu = NetworkUpgrade::current(&network, Height(*height));

            for tx in &block.transactions {
                if !tx.inputs().is_empty() {
                    continue;
                }

                let Some(item) = item(tx, nu) else {
                    continue;
                };

                fixtures.push(Fixture {
                    name: format!(
                        "{network} height {height} v{} tx {} ({} spends, {} outputs)",
                        tx.version(),
                        tx.hash(),
                        item.bundle.shielded_spends().len(),
                        item.bundle.shielded_outputs().len(),
                    ),
                    bundle: item.bundle,
                    sighash: item.sighash.into(),
                });
            }
        }
    }

    fixtures
});

/// One fixture of each distinct (version, spends, outputs) shape.
///
/// The mutation tests run over these rather than every fixture: bundles of the same shape reach
/// the same verifier code, and cold validation re-prepares both keys every time.
fn representative_fixtures() -> Vec<&'static Fixture> {
    let mut seen = Vec::new();

    FIXTURES
        .iter()
        .filter(|fixture| {
            let version = fixture.name.split(' ').find(|word| word.starts_with('v'));
            let shape = (version, fixture.spends(), fixture.outputs());
            let is_new = !seen.contains(&shape);
            seen.push(shape);
            is_new
        })
        .collect()
}

/// Returns the first fixture with exactly `spends` spends and `outputs` outputs.
fn fixture_with_shape(spends: usize, outputs: usize) -> &'static Fixture {
    FIXTURES
        .iter()
        .find(|fixture| fixture.spends() == spends && fixture.outputs() == outputs)
        .unwrap_or_else(|| {
            panic!(
                "the test vectors must contain a bundle with {spends} spends and {outputs} outputs"
            )
        })
}

/// Queues `fixtures` into one new batch.
///
/// Returns the batch, and whether every bundle passed the synchronous checks. A bundle that fails
/// them may still have queued some of its proofs and signatures, just as in the verifier.
fn queue(fixtures: &[&Fixture]) -> (BatchValidator, bool) {
    let mut batch = BatchValidator::new();
    let mut all_passed = true;

    for fixture in fixtures {
        all_passed &= batch.check_bundle(fixture.bundle.clone(), fixture.sighash);
    }

    (batch, all_passed)
}

/// Validates `batch` the way the verifier did before prepared keys: under keys built for this
/// batch alone, so every G2 term is prepared from scratch.
fn validate_cold(batch: BatchValidator) -> bool {
    let (spend_vk, output_vk) = sapling_prover().verifying_keys();
    batch.validate(&spend_vk, &output_vk, rand_10::rng())
}

/// Validates `batch` under `keys`, which the caller shares across batches.
fn validate_with(keys: &PreparedBatchVerifyingKeys<'_>, batch: BatchValidator) -> bool {
    batch.validate_prepared(keys, rand_10::rng())
}

/// How one batch was decided.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Decision {
    /// Whether every bundle passed the synchronous checks.
    queued: bool,
    /// Whether the batch was accepted under new, unprepared keys.
    cold: bool,
    /// Whether the batch was accepted under the verifier's shared prepared keys.
    prepared: bool,
}

/// Decides `fixtures` as one batch under both new keys and the verifier's shared keys, and
/// asserts that the two agree and give `expected`.
///
/// A batch is accepted when every bundle passes the synchronous checks and batch validation
/// accepts. Batch validation is compared even when a synchronous check failed, because the
/// verifier still validates whatever that bundle queued alongside the rest of its batch.
fn assert_decided(fixtures: &[&Fixture], expected: bool) -> Decision {
    let (cold_batch, queued) = queue(fixtures);
    let (prepared_batch, _) = queue(fixtures);

    let decision = Decision {
        queued,
        cold: validate_cold(cold_batch),
        prepared: validate(prepared_batch),
    };

    let names = || {
        fixtures
            .iter()
            .map(|fixture| fixture.name.as_str())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        decision.cold,
        decision.prepared,
        "prepared keys must decide a batch exactly like new keys: {decision:?} for {:?}",
        names(),
    );
    assert_eq!(
        decision.queued && decision.prepared,
        expected,
        "unexpected decision {decision:?} for {:?}",
        names(),
    );

    decision
}

// Proof and public-input mutations.
//
// A Groth16 proof is serialized as a compressed G1 point A (48 bytes), a compressed G2 point B
// (96 bytes), and a compressed G1 point C (48 bytes). In both encodings the third-highest bit of
// the first byte selects the sign of y, so flipping it replaces the point with its negation: the
// proof stays well-formed and passes the synchronous checks, and only the pairing check can
// reject it.

const A: usize = 0;
const B: usize = 48;
const C: usize = 144;
const SIGN_BIT: u8 = 0x20;

/// The proof mutations that keep a proof well-formed but make it invalid.
fn proof_mutations() -> Vec<(&'static str, fn(GrothProofBytes) -> GrothProofBytes)> {
    vec![
        ("A and C swapped", |mut proof| {
            let (a, rest) = proof.split_at_mut(B);
            a.swap_with_slice(&mut rest[C - B..]);
            proof
        }),
        ("A negated", |mut proof| {
            proof[A] ^= SIGN_BIT;
            proof
        }),
        ("B negated", |mut proof| {
            proof[B] ^= SIGN_BIT;
            proof
        }),
        ("C negated", |mut proof| {
            proof[C] ^= SIGN_BIT;
            proof
        }),
    ]
}

/// Returns `bundle` with its spend at `index` replaced by `mutate`'s result.
fn map_spend(
    bundle: &Bundle<Authorized, ZatBalance>,
    index: usize,
    mutate: impl FnOnce(&SpendDescription<Authorized>) -> SpendDescription<Authorized>,
) -> Bundle<Authorized, ZatBalance> {
    let mut spends = bundle.shielded_spends().to_vec();
    spends[index] = mutate(&spends[index]);

    Bundle::from_parts(
        spends,
        bundle.shielded_outputs().to_vec(),
        *bundle.value_balance(),
        Authorized {
            binding_sig: bundle.authorization().binding_sig,
        },
    )
    .expect("replacing a spend keeps the bundle non-empty")
}

/// Returns `bundle` with its output at `index` replaced by `mutate`'s result.
fn map_output(
    bundle: &Bundle<Authorized, ZatBalance>,
    index: usize,
    mutate: impl FnOnce(&OutputDescription<GrothProofBytes>) -> OutputDescription<GrothProofBytes>,
) -> Bundle<Authorized, ZatBalance> {
    let mut outputs = bundle.shielded_outputs().to_vec();
    outputs[index] = mutate(&outputs[index]);

    Bundle::from_parts(
        bundle.shielded_spends().to_vec(),
        outputs,
        *bundle.value_balance(),
        Authorized {
            binding_sig: bundle.authorization().binding_sig,
        },
    )
    .expect("replacing an output keeps the bundle non-empty")
}

/// Rebuilds `spend` with the given fields replaced.
fn spend_with(
    spend: &SpendDescription<Authorized>,
    anchor: Option<Scalar>,
    nullifier: Option<Nullifier>,
    zkproof: Option<GrothProofBytes>,
) -> SpendDescription<Authorized> {
    SpendDescription::from_parts(
        spend.cv().clone(),
        anchor.unwrap_or(*spend.anchor()),
        nullifier.unwrap_or(*spend.nullifier()),
        *spend.rk(),
        zkproof.unwrap_or(*spend.zkproof()),
        *spend.spend_auth_sig(),
    )
}

/// Rebuilds `output` with the given fields replaced.
fn output_with(
    output: &OutputDescription<GrothProofBytes>,
    cmu: Option<ExtractedNoteCommitment>,
    ephemeral_key: Option<[u8; 32]>,
    enc_ciphertext_flip: Option<usize>,
    zkproof: Option<GrothProofBytes>,
) -> OutputDescription<GrothProofBytes> {
    let mut epk = output.ephemeral_key().clone();
    if let Some(bytes) = ephemeral_key {
        epk.0 = bytes;
    }

    let mut enc_ciphertext = *output.enc_ciphertext();
    if let Some(byte) = enc_ciphertext_flip {
        enc_ciphertext[byte] ^= 1;
    }

    OutputDescription::from_parts(
        output.cv().clone(),
        cmu.unwrap_or(*output.cmu()),
        epk,
        enc_ciphertext,
        *output.out_ciphertext(),
        zkproof.unwrap_or(*output.zkproof()),
    )
}

/// Returns `nullifier` with bit `bit` flipped, counting from the least significant bit of its
/// little-endian encoding.
fn flip_nullifier_bit(nullifier: &Nullifier, bit: usize) -> Nullifier {
    let mut bytes = nullifier.0;
    bytes[bit / 8] ^= 1 << (bit % 8);
    Nullifier(bytes)
}

/// Returns `cmu` plus one, as a field element.
fn next_cmu(cmu: &ExtractedNoteCommitment) -> ExtractedNoteCommitment {
    let value = Option::<Scalar>::from(Scalar::from_bytes(&cmu.to_bytes()))
        .expect("a parsed note commitment is a canonical field element");
    Option::from(ExtractedNoteCommitment::from_bytes(
        &(value + Scalar::one()).to_bytes(),
    ))
    .expect("a field element is a valid note commitment")
}

/// A mutated bundle and the decision the verifier must reach on it.
struct Mutation {
    fixture: Fixture,
    /// Whether the mutated bundle must be accepted.
    accepted: bool,
    /// Whether the mutated bundle must pass the synchronous checks and have valid signatures, so
    /// that only the Groth16 proof verifier — the code the prepared keys feed — can reject it.
    reaches_proofs: bool,
}

/// Mutations of `fixture` that the proof verifier alone must reject.
///
/// Signatures are checked against the fixture's unchanged sighash, and every change is to a
/// proof or to a public input of its circuit, so each mutation passes the synchronous checks and
/// the signature batch and is rejected by the pairing check under the prepared keys.
fn proof_verifier_mutations(fixture: &Fixture) -> Vec<Mutation> {
    let bundle = &fixture.bundle;
    let mut mutations = Vec::new();
    let mut push = |change: String, bundle| {
        mutations.push(Mutation {
            fixture: fixture.with_bundle(&change, bundle),
            accepted: false,
            reaches_proofs: true,
        })
    };

    let spend_indexes = [0, fixture.spends().saturating_sub(1)];
    for index in spend_indexes.into_iter().take(fixture.spends()) {
        for (change, mutate) in proof_mutations() {
            push(
                format!("spend {index} proof {change}"),
                map_spend(bundle, index, |spend| {
                    spend_with(spend, None, None, Some(mutate(*spend.zkproof())))
                }),
            );
        }

        if let Some(output) = bundle.shielded_outputs().first() {
            push(
                format!("spend {index} proof replaced by an output proof"),
                map_spend(bundle, index, |spend| {
                    spend_with(spend, None, None, Some(*output.zkproof()))
                }),
            );
        }

        let other = (index + 1) % fixture.spends();
        if other != index {
            let other_proof = *bundle.shielded_spends()[other].zkproof();
            push(
                format!("spend {index} proof replaced by spend {other}'s"),
                map_spend(bundle, index, |spend| {
                    spend_with(spend, None, None, Some(other_proof))
                }),
            );
        }

        push(
            format!("spend {index} anchor incremented"),
            map_spend(bundle, index, |spend| {
                spend_with(spend, Some(spend.anchor() + Scalar::one()), None, None)
            }),
        );

        // The nullifier is packed into two public inputs: its low 254 bits, and its top two.
        // Cover both inputs and both sides of the split.
        for bit in [0, 1, 7, 8, 127, 128, 252, 253, 254, 255] {
            push(
                format!("spend {index} nullifier bit {bit} flipped"),
                map_spend(bundle, index, |spend| {
                    spend_with(
                        spend,
                        None,
                        Some(flip_nullifier_bit(spend.nullifier(), bit)),
                        None,
                    )
                }),
            );
        }
    }

    let output_indexes = [0, fixture.outputs().saturating_sub(1)];
    for index in output_indexes.into_iter().take(fixture.outputs()) {
        for (change, mutate) in proof_mutations() {
            push(
                format!("output {index} proof {change}"),
                map_output(bundle, index, |output| {
                    output_with(output, None, None, None, Some(mutate(*output.zkproof())))
                }),
            );
        }

        if let Some(spend) = bundle.shielded_spends().first() {
            push(
                format!("output {index} proof replaced by a spend proof"),
                map_output(bundle, index, |output| {
                    output_with(output, None, None, None, Some(*spend.zkproof()))
                }),
            );
        }

        let other = (index + 1) % fixture.outputs();
        if other != index {
            let other_output = &bundle.shielded_outputs()[other];
            push(
                format!("output {index} proof replaced by output {other}'s"),
                map_output(bundle, index, |output| {
                    output_with(output, None, None, None, Some(*other_output.zkproof()))
                }),
            );
            push(
                format!("output {index} note commitment replaced by output {other}'s"),
                map_output(bundle, index, |output| {
                    output_with(output, Some(*other_output.cmu()), None, None, None)
                }),
            );
            push(
                format!("output {index} ephemeral key replaced by output {other}'s"),
                map_output(bundle, index, |output| {
                    output_with(
                        output,
                        None,
                        Some(other_output.ephemeral_key().0),
                        None,
                        None,
                    )
                }),
            );
        }

        push(
            format!("output {index} note commitment incremented"),
            map_output(bundle, index, |output| {
                output_with(output, Some(next_cmu(output.cmu())), None, None, None)
            }),
        );

        // A compressed Jubjub point keeps the sign of x in its top bit, so flipping it negates the
        // ephemeral key: still a valid point, but a different public input.
        push(
            format!("output {index} ephemeral key negated"),
            map_output(bundle, index, |output| {
                let mut epk = output.ephemeral_key().0;
                epk[31] ^= 0x80;
                output_with(output, None, Some(epk), None, None)
            }),
        );
    }

    mutations
}

/// Mutations of `fixture` that signatures or the synchronous checks must reject, and changes that
/// verification does not cover and must still accept.
///
/// These keep the test honest in both directions: a harness that rejected everything would pass
/// the proof mutations above, but not the accepted changes here.
fn other_mutations(fixture: &Fixture) -> Vec<Mutation> {
    let bundle = &fixture.bundle;
    let binding_sig = bundle.authorization().binding_sig;
    let rebuild = |spends: Vec<_>, outputs: Vec<_>, value_balance, binding_sig| {
        Bundle::from_parts(spends, outputs, value_balance, Authorized { binding_sig })
            .expect("the fixture's spends and outputs are not both empty")
    };
    let spends = || bundle.shielded_spends().to_vec();
    let outputs = || bundle.shielded_outputs().to_vec();

    let rejected = |change: &str, bundle| Mutation {
        fixture: fixture.with_bundle(change, bundle),
        accepted: false,
        reaches_proofs: false,
    };
    let accepted = |change: &str, bundle| Mutation {
        fixture: fixture.with_bundle(change, bundle),
        accepted: true,
        reaches_proofs: true,
    };

    let mut flipped_binding_sig = <[u8; 64]>::from(binding_sig);
    flipped_binding_sig[0] ^= 1;

    let mut mutations = vec![
        rejected(
            "the binding signature changed",
            rebuild(
                spends(),
                outputs(),
                *bundle.value_balance(),
                flipped_binding_sig.into(),
            ),
        ),
        rejected(
            "the value balance incremented",
            rebuild(
                spends(),
                outputs(),
                (*bundle.value_balance() + ZatBalance::const_from_i64(1))
                    .expect("the fixture's value balance is far from the maximum"),
                binding_sig,
            ),
        ),
    ];

    let mut wrong_sighash = fixture.clone();
    wrong_sighash.name = format!("{} with the sighash changed", fixture.name);
    wrong_sighash.sighash[0] ^= 1;
    mutations.push(Mutation {
        fixture: wrong_sighash,
        accepted: false,
        reaches_proofs: false,
    });

    if fixture.spends() > 0 {
        mutations.push(rejected(
            "spend 0 authorization signature changed",
            map_spend(bundle, 0, |spend| {
                let mut sig = <[u8; 64]>::from(*spend.spend_auth_sig());
                sig[0] ^= 1;
                SpendDescription::from_parts(
                    spend.cv().clone(),
                    *spend.anchor(),
                    *spend.nullifier(),
                    *spend.rk(),
                    *spend.zkproof(),
                    sig.into(),
                )
            }),
        ));
    }

    if fixture.spends() > 1 {
        let rk = *bundle.shielded_spends()[1].rk();
        mutations.push(rejected(
            "spend 0 randomized key replaced by spend 1's",
            map_spend(bundle, 0, |spend| {
                SpendDescription::from_parts(
                    spend.cv().clone(),
                    *spend.anchor(),
                    *spend.nullifier(),
                    rk,
                    *spend.zkproof(),
                    *spend.spend_auth_sig(),
                )
            }),
        ));

        let mut reversed = spends();
        reversed.reverse();
        mutations.push(accepted(
            "its spends reversed",
            rebuild(reversed, outputs(), *bundle.value_balance(), binding_sig),
        ));
    }

    if fixture.outputs() > 0 {
        mutations.push(accepted(
            "output 0 note ciphertext changed",
            map_output(bundle, 0, |output| {
                output_with(output, None, None, Some(0), None)
            }),
        ));
    }

    if fixture.outputs() > 1 {
        let mut reversed = outputs();
        reversed.reverse();
        mutations.push(accepted(
            "its outputs reversed",
            rebuild(spends(), reversed, *bundle.value_balance(), binding_sig),
        ));

        let mut dropped = outputs();
        dropped.pop();
        mutations.push(rejected(
            "its last output removed",
            rebuild(spends(), dropped, *bundle.value_balance(), binding_sig),
        ));

        if fixture.spends() > 0 {
            let cv = bundle.shielded_outputs()[1].cv().clone();
            mutations.push(rejected(
                "spend 0 value commitment replaced by output 1's",
                map_spend(bundle, 0, |spend| {
                    SpendDescription::from_parts(
                        cv,
                        *spend.anchor(),
                        *spend.nullifier(),
                        *spend.rk(),
                        *spend.zkproof(),
                        *spend.spend_auth_sig(),
                    )
                }),
            ));
        }
    }

    mutations
}

/// Returns a well-formed but invalid version of `fixture`, whose first proof fails the pairing
/// check and nothing else.
fn with_invalid_proof(fixture: &Fixture) -> Fixture {
    if fixture.spends() > 0 {
        fixture.with_bundle(
            "spend 0 proof A negated",
            map_spend(&fixture.bundle, 0, |spend| {
                let mut proof = *spend.zkproof();
                proof[A] ^= SIGN_BIT;
                spend_with(spend, None, None, Some(proof))
            }),
        )
    } else {
        fixture.with_bundle(
            "output 0 proof A negated",
            map_output(&fixture.bundle, 0, |output| {
                let mut proof = *output.zkproof();
                proof[A] ^= SIGN_BIT;
                output_with(output, None, None, None, Some(proof))
            }),
        )
    }
}

#[test]
fn the_verifier_shares_one_pair_of_keys() {
    let first: *const (SpendVerifyingKey, OutputVerifyingKey) = &*VERIFYING_KEYS;
    let second: *const (SpendVerifyingKey, OutputVerifyingKey) = &*VERIFYING_KEYS;
    assert!(
        std::ptr::eq(first, second),
        "the keys must be shared, or their prepared terms are rebuilt for every batch"
    );
}

/// The fixtures reach every path `validate_prepared` dispatches on, so the tests that loop over
/// them cannot silently stop covering one.
#[test]
fn fixtures_reach_every_batch_validation_path() {
    let _init_guard = zakura_test::init();

    // Spend proofs only, one and several.
    fixture_with_shape(1, 0);
    assert!(FIXTURES
        .iter()
        .any(|fixture| fixture.spends() > 1 && fixture.outputs() == 0));

    // Output proofs only.
    fixture_with_shape(0, 1);

    // Exactly one of each, which the joint verifier takes.
    fixture_with_shape(1, 1);

    // More than one of either kind alongside the other, which the multicore verifier takes.
    assert!(FIXTURES
        .iter()
        .any(|fixture| fixture.spends() > 1 && fixture.outputs() > 0));
    assert!(FIXTURES
        .iter()
        .any(|fixture| fixture.spends() > 0 && fixture.outputs() > 1));

    // Both transaction versions that carry Sapling, and both networks.
    for needle in ["v4", "v5", "Mainnet", "Testnet"] {
        assert!(
            FIXTURES.iter().any(|fixture| fixture.name.contains(needle)),
            "the test vectors must contain a {needle} Sapling bundle"
        );
    }
}

#[test]
fn an_empty_batch_is_decided_alike() {
    let _init_guard = zakura_test::init();

    assert_eq!(
        assert_decided(&[], true),
        Decision {
            queued: true,
            cold: true,
            prepared: true,
        }
    );
}

#[test]
fn every_real_bundle_is_accepted_alone_cold_and_prepared() {
    let _init_guard = zakura_test::init();

    for fixture in FIXTURES.iter() {
        // Repeat the prepared validation, so later rounds run on terms prepared by earlier ones.
        for _ in 0..3 {
            assert_decided(&[fixture], true);
        }
    }
}

#[test]
fn every_real_bundle_is_accepted_in_one_batch() {
    let _init_guard = zakura_test::init();

    let fixtures: Vec<_> = FIXTURES.iter().collect();
    assert_decided(&fixtures, true);
}

/// Every mutation that only the proof verifier can catch is rejected, alone and in a larger batch,
/// cold and prepared.
///
/// "Alone" puts each mutation through the verifier its bundle's shape selects, including the joint
/// verifier for a one-spend, one-output bundle. "Beside a neighbor" adds another valid bundle, so
/// the same invalid proof is also caught by the multicore verifier.
#[test]
fn every_proof_mutation_is_rejected_cold_and_prepared() {
    let _init_guard = zakura_test::init();

    let neighbor = fixture_with_shape(1, 2);
    let mut checked = 0;

    for fixture in representative_fixtures() {
        for mutation in proof_verifier_mutations(fixture) {
            let decision = assert_decided(&[&mutation.fixture], mutation.accepted);
            assert!(
                decision.queued,
                "{} must pass the synchronous checks, so the proof verifier is what rejects it",
                mutation.fixture.name,
            );
            assert!(mutation.reaches_proofs);

            assert_decided(&[neighbor, &mutation.fixture], mutation.accepted);
            assert_decided(&[&mutation.fixture, neighbor], mutation.accepted);
            checked += 1;
        }
    }

    assert!(
        checked > 100,
        "expected well over a hundred proof mutations, got {checked}"
    );
}

#[test]
fn every_other_mutation_is_decided_alike_cold_and_prepared() {
    let _init_guard = zakura_test::init();

    let neighbor = fixture_with_shape(1, 2);
    let (mut accepted, mut rejected) = (0, 0);

    for fixture in representative_fixtures() {
        for mutation in other_mutations(fixture) {
            let decision = assert_decided(&[&mutation.fixture], mutation.accepted);
            if mutation.reaches_proofs {
                assert!(
                    decision.queued,
                    "{} must pass the synchronous checks",
                    mutation.fixture.name,
                );
            }

            assert_decided(&[neighbor, &mutation.fixture], mutation.accepted);

            if mutation.accepted {
                accepted += 1;
            } else {
                rejected += 1;
            }
        }
    }

    assert!(accepted > 0, "the controls must include accepted changes");
    assert!(rejected > 0, "the controls must include rejected changes");
}

/// Every bit of a nullifier is bound by the Spend proof, on both sides of the split between its
/// two packed public inputs.
///
/// The nullifier packing is new in this `sapling-crypto` release, and a bit it dropped would let a
/// spend reveal a nullifier its proof never committed to.
#[test]
fn every_nullifier_bit_is_bound_by_the_spend_proof() {
    let _init_guard = zakura_test::init();

    for fixture in [fixture_with_shape(1, 0), fixture_with_shape(1, 1)] {
        for bit in 0..256 {
            let mutated = fixture.with_bundle(
                &format!("nullifier bit {bit} flipped"),
                map_spend(&fixture.bundle, 0, |spend| {
                    spend_with(
                        spend,
                        None,
                        Some(flip_nullifier_bit(spend.nullifier(), bit)),
                        None,
                    )
                }),
            );

            let (batch, queued) = queue(&[&mutated]);
            assert!(queued, "any 32 bytes are a well-formed nullifier");
            assert!(
                !validate(batch),
                "{} must be rejected under prepared keys",
                mutated.name
            );
        }

        // Compare against new keys at the split and at the ends, where packing errors would be.
        for bit in [0, 247, 248, 253, 254, 255] {
            let mutated = fixture.with_bundle(
                &format!("nullifier bit {bit} flipped"),
                map_spend(&fixture.bundle, 0, |spend| {
                    spend_with(
                        spend,
                        None,
                        Some(flip_nullifier_bit(spend.nullifier(), bit)),
                        None,
                    )
                }),
            );
            assert_decided(&[&mutated], false);
        }
    }
}

/// Spend and Output proofs from different bundles still go through the joint verifier when the
/// batch holds exactly one of each, and it rejects either one being invalid.
#[test]
fn the_joint_verifier_decides_proofs_from_different_bundles() {
    let _init_guard = zakura_test::init();

    let spend_only = fixture_with_shape(1, 0);
    let output_only = fixture_with_shape(0, 1);
    let invalid_spend = with_invalid_proof(spend_only);
    let invalid_output = with_invalid_proof(output_only);

    assert_decided(&[spend_only, output_only], true);
    assert_decided(&[output_only, spend_only], true);
    assert_decided(&[&invalid_spend, output_only], false);
    assert_decided(&[spend_only, &invalid_output], false);
    assert_decided(&[&invalid_spend, &invalid_output], false);
}

/// One invalid bundle anywhere in a large batch rejects the whole batch.
#[test]
fn one_invalid_bundle_rejects_a_large_batch() {
    let _init_guard = zakura_test::init();

    for invalid_index in [0, FIXTURES.len() / 2, FIXTURES.len() - 1] {
        let invalid = with_invalid_proof(&FIXTURES[invalid_index]);
        let batch: Vec<_> = FIXTURES
            .iter()
            .enumerate()
            .map(|(index, fixture)| {
                if index == invalid_index {
                    &invalid
                } else {
                    fixture
                }
            })
            .collect();

        assert_decided(&batch, false);
    }
}

/// Keys that have prepared their terms on valid batches still reject the next invalid one, in
/// every order and shape.
#[test]
fn warm_keys_still_reject_invalid_batches() {
    let _init_guard = zakura_test::init();

    let (spend_vk, output_vk) = sapling_prover().verifying_keys();
    let keys = PreparedBatchVerifyingKeys::new(&spend_vk, &output_vk);

    for round in 0..3 {
        for fixture in FIXTURES.iter() {
            let invalid = with_invalid_proof(fixture);

            let (batch, _) = queue(&[fixture]);
            assert!(
                validate_with(&keys, batch),
                "round {round}: {}",
                fixture.name
            );

            let (batch, queued) = queue(&[&invalid]);
            assert!(queued);
            assert!(
                !validate_with(&keys, batch),
                "round {round}: {}",
                invalid.name
            );
        }
    }
}

/// Whatever batch first prepares a key, the key decides every later batch correctly.
///
/// A key's terms are prepared lazily by the first batch that needs them. This starts from new keys
/// each time and varies that first batch: invalid proofs, invalid signatures (which return before
/// any preparation), and one proof kind only (which prepares one key and not the other).
#[test]
fn the_first_batch_under_new_keys_does_not_change_later_decisions() {
    let _init_guard = zakura_test::init();

    let spend_only = fixture_with_shape(1, 0);
    let output_only = fixture_with_shape(0, 1);
    let both = fixture_with_shape(1, 1);
    let many = fixture_with_shape(1, 2);

    let mut wrong_sighash = both.clone();
    wrong_sighash.name = format!("{} with the sighash changed", both.name);
    wrong_sighash.sighash[0] ^= 1;

    let invalid_spend_only = with_invalid_proof(spend_only);
    let invalid_output_only = with_invalid_proof(output_only);
    let invalid_both = with_invalid_proof(both);
    let invalid_many = with_invalid_proof(many);

    let first_batches: [&[&Fixture]; 8] = [
        &[&invalid_spend_only],
        &[&invalid_output_only],
        &[&invalid_both],
        &[&invalid_many],
        &[&wrong_sighash],
        &[spend_only],
        &[output_only],
        &[many],
    ];

    let later_batches: [(&[&Fixture], bool); 8] = [
        (&[spend_only], true),
        (&[&invalid_spend_only], false),
        (&[output_only], true),
        (&[&invalid_output_only], false),
        (&[both], true),
        (&[&invalid_both], false),
        (&[many, both], true),
        (&[many, &invalid_both], false),
    ];

    for first in first_batches {
        let (spend_vk, output_vk) = sapling_prover().verifying_keys();
        let keys = PreparedBatchVerifyingKeys::new(&spend_vk, &output_vk);

        let expected_first = first.iter().all(|fixture| {
            let (batch, queued) = queue(&[fixture]);
            queued && validate_cold(batch)
        });
        let (batch, _) = queue(first);
        assert_eq!(validate_with(&keys, batch), expected_first);

        for (later, expected) in later_batches {
            let (batch, queued) = queue(later);
            assert!(queued);
            assert_eq!(
                validate_with(&keys, batch),
                expected,
                "after first batch {:?}, batch {:?}",
                first
                    .iter()
                    .map(|fixture| &fixture.name)
                    .collect::<Vec<_>>(),
                later
                    .iter()
                    .map(|fixture| &fixture.name)
                    .collect::<Vec<_>>(),
            );
        }
    }
}

/// Threads that race to prepare the same new keys, and then keep sharing them, each get the right
/// decision for every batch.
#[test]
fn concurrent_validation_under_shared_keys_is_consistent() {
    let _init_guard = zakura_test::init();

    const THREADS: usize = 8;
    const BATCHES_PER_THREAD: usize = 12;

    let invalid: Vec<_> = FIXTURES.iter().map(with_invalid_proof).collect();

    let (spend_vk, output_vk) = sapling_prover().verifying_keys();
    let fresh_keys = PreparedBatchVerifyingKeys::new(&spend_vk, &output_vk);

    // Race on new keys, whose terms are not yet prepared, and on the verifier's shared keys.
    for use_production_keys in [false, true] {
        let start = Barrier::new(THREADS);
        let failures = Mutex::new(Vec::new());

        std::thread::scope(|scope| {
            for thread in 0..THREADS {
                let (start, failures, invalid, fresh_keys) =
                    (&start, &failures, &invalid, &fresh_keys);

                scope.spawn(move || {
                    start.wait();

                    for batch_index in 0..BATCHES_PER_THREAD {
                        let index = (thread * BATCHES_PER_THREAD + batch_index) % FIXTURES.len();
                        let expected = (thread + batch_index) % 2 == 0;
                        let fixture = if expected {
                            &FIXTURES[index]
                        } else {
                            &invalid[index]
                        };

                        let (batch, queued) = queue(&[fixture]);
                        assert!(queued);
                        let actual = if use_production_keys {
                            validate(batch)
                        } else {
                            validate_with(fresh_keys, batch)
                        };

                        if actual != expected {
                            failures
                                .lock()
                                .expect("no thread panics while holding the lock")
                                .push(fixture.name.clone());
                        }
                    }
                });
            }
        });

        let failures = failures
            .into_inner()
            .expect("no thread panics while holding the lock");
        assert!(
            failures.is_empty(),
            "concurrent validation misjudged: {failures:?}"
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    /// Any single-bit change to any byte of any proof in a real bundle is rejected, cold and
    /// prepared alike.
    ///
    /// Almost every such change breaks a point's encoding or subgroup membership, so this mostly
    /// exercises the synchronous checks; the sign-bit flips that stay well-formed are covered
    /// directly by the proof mutations above, and random well-formed statements by
    /// `any_other_public_input_is_rejected`. Either way the bundle must be rejected, and the two
    /// keys must agree on what batch validation says about whatever it queued.
    #[test]
    fn any_proof_bit_flip_is_rejected(
        fixture_index in any::<prop::sample::Index>(),
        in_spend in any::<bool>(),
        description_index in any::<prop::sample::Index>(),
        byte in 0..192usize,
        bit in 0..8u8,
    ) {
        let _init_guard = zakura_test::init();

        let fixture = &FIXTURES[fixture_index.index(FIXTURES.len())];
        let flip = |mut proof: GrothProofBytes| {
            proof[byte] ^= 1 << bit;
            proof
        };

        let use_spend = (in_spend && fixture.spends() > 0) || fixture.outputs() == 0;
        let mutated = if use_spend {
            let index = description_index.index(fixture.spends());
            fixture.with_bundle(
                &format!("spend {index} proof byte {byte} bit {bit} flipped"),
                map_spend(&fixture.bundle, index, |spend| {
                    spend_with(spend, None, None, Some(flip(*spend.zkproof())))
                }),
            )
        } else {
            let index = description_index.index(fixture.outputs());
            fixture.with_bundle(
                &format!("output {index} proof byte {byte} bit {bit} flipped"),
                map_output(&fixture.bundle, index, |output| {
                    output_with(output, None, None, None, Some(flip(*output.zkproof())))
                }),
            )
        };

        assert_decided(&[&mutated], false);
    }

    /// A valid proof does not verify against any other statement: a random nullifier, anchor, or
    /// note commitment in place of the real one is rejected, cold and prepared alike.
    ///
    /// Every value drawn here is well-formed, so each case passes the synchronous checks and the
    /// signature batch, and only the pairing check under the prepared keys can reject it.
    #[test]
    fn any_other_public_input_is_rejected(
        fixture_index in any::<prop::sample::Index>(),
        description_index in any::<prop::sample::Index>(),
        input in 0..3u8,
        random in prop::array::uniform32(any::<u8>()),
    ) {
        let _init_guard = zakura_test::init();

        let with_spends: Vec<_> = FIXTURES.iter().filter(|fixture| fixture.spends() > 0).collect();
        let with_outputs: Vec<_> = FIXTURES.iter().filter(|fixture| fixture.outputs() > 0).collect();

        let mut wide = [0; 64];
        wide[..32].copy_from_slice(&random);
        let scalar = Scalar::from_bytes_wide(&wide);

        let mutated = match input {
            0 | 1 => {
                let fixture = with_spends[fixture_index.index(with_spends.len())];
                let index = description_index.index(fixture.spends());
                let spend = &fixture.bundle.shielded_spends()[index];
                let (change, anchor, nullifier) = if input == 0 {
                    prop_assume!(spend.nullifier().0 != random);
                    ("nullifier", None, Some(Nullifier(random)))
                } else {
                    prop_assume!(*spend.anchor() != scalar);
                    ("anchor", Some(scalar), None)
                };
                fixture.with_bundle(
                    &format!("spend {index} {change} replaced by {}", hex::encode(random)),
                    map_spend(&fixture.bundle, index, |spend| {
                        spend_with(spend, anchor, nullifier, None)
                    }),
                )
            }
            _ => {
                let fixture = with_outputs[fixture_index.index(with_outputs.len())];
                let index = description_index.index(fixture.outputs());
                let cmu = Option::from(ExtractedNoteCommitment::from_bytes(&scalar.to_bytes()))
                    .expect("a field element is a valid note commitment");
                prop_assume!(*fixture.bundle.shielded_outputs()[index].cmu() != cmu);
                fixture.with_bundle(
                    &format!("output {index} note commitment replaced by {}", hex::encode(random)),
                    map_output(&fixture.bundle, index, |output| {
                        output_with(output, Some(cmu), None, None, None)
                    }),
                )
            }
        };

        let decision = assert_decided(&[&mutated], false);
        prop_assert!(decision.queued, "{} must reach the proof verifier", mutated.name);
    }
}

/// The batch service, with its single-item fallback, decides each of a mixed set of transactions
/// exactly as they were mined or corrupted.
///
/// The batch holds every valid transaction and several invalid ones, so it fails as a whole and
/// the fallback re-verifies each item alone. Both paths validate under the shared prepared keys.
#[tokio::test(flavor = "multi_thread")]
async fn the_batch_service_decides_each_transaction_correctly() {
    let _init_guard = zakura_test::init();

    let mut items = Vec::new();

    for (nu, tx) in mined_sapling_transactions() {
        items.push((
            item(&tx, nu).expect("the transaction has a bundle"),
            true,
            format!("mined {}", tx.hash()),
        ));

        if !matches!(tx, Transaction::V4 { .. }) {
            continue;
        }

        if tx.sapling_spends_per_anchor().next().is_some() {
            let corrupted = mutated_spend(&tx, |spend| spend.zkproof.0[A] ^= SIGN_BIT);
            items.push((
                item(&corrupted, nu).expect("the transaction has a bundle"),
                false,
                format!("{} with spend 0 proof A negated", tx.hash()),
            ));

            if tx.sapling_outputs().next().is_some() {
                let corrupted = mutated_output(&tx, |output| output.zkproof.0[C] ^= SIGN_BIT);
                items.push((
                    item(&corrupted, nu).expect("the transaction has a bundle"),
                    false,
                    format!("{} with output 0 proof C negated", tx.hash()),
                ));
            }
        }
    }

    assert!(items.iter().any(|(_, valid, _)| *valid));
    assert!(items.iter().any(|(_, valid, _)| !*valid));

    let verifier = uncached_verification_behind_a_fresh_cache();
    let results = join_all(
        items
            .iter()
            .map(|(item, _, _)| verifier.clone().oneshot(item.clone())),
    )
    .await;

    for ((_, valid, name), result) in items.iter().zip(results) {
        match result {
            Ok(()) => assert!(*valid, "{name} must be rejected"),
            Err(error) => {
                assert!(!*valid, "{name} must be accepted, got {error}");
                let error = error
                    .downcast::<TransactionError>()
                    .expect("the verifier reports a typed transaction error");
                assert_eq!(
                    *error,
                    TransactionError::SaplingVerificationFailed,
                    "{name}"
                );
            }
        }
    }
}

/// A verifier dropped with pending items validates them under the shared prepared keys.
#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_verifier_decides_its_pending_batch() {
    let _init_guard = zakura_test::init();

    let (nu, tx) = super::mined_v4_sapling_transaction_with_spends();
    let valid = item(&tx, nu).expect("the transaction has a bundle");
    let invalid = item(
        &mutated_spend(&tx, |spend| spend.zkproof.0[A] ^= SIGN_BIT),
        nu,
    )
    .expect("the transaction has a bundle");

    let mut verifier = Verifier::default();
    let pending = verifier.call(BatchControl::Item(valid.clone()));
    drop(verifier);
    pending
        .await
        .expect("a valid bundle flushed on drop must be accepted");

    let mut verifier = Verifier::default();
    let pending = verifier.call(BatchControl::Item(invalid));
    drop(verifier);
    let error = pending
        .await
        .expect_err("an invalid bundle flushed on drop must be rejected");
    assert_eq!(
        *error
            .downcast::<TransactionError>()
            .expect("the verifier reports a typed transaction error"),
        TransactionError::SaplingVerificationFailed
    );
}
