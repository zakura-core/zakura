//! Tests of v7 transaction verification ([ZIP 248]).
//!
//! [ZIP 248]: https://zips.z.cash/zip-0248

use std::{
    collections::{HashMap, HashSet},
    convert::Infallible,
    sync::{Arc, LazyLock},
};

use chrono::{DateTime, Utc};
use tower::{service_fn, ServiceExt};
use zcash_primitives::transaction::{
    builder::{BuildConfig, Builder, BundlePadding},
    TxVersion,
};

use zakura_chain::{
    amount::{Amount, NegativeAllowed, NonNegative},
    block::Height,
    parameters::{
        testnet::{ConfiguredActivationHeights, Parameters},
        Network, NetworkUpgrade,
    },
    serialization::ZcashDeserializeInto,
    transaction::{
        v7::{BundleKind, CoinbaseBundle, ShieldedBundle, V7Fields, ValueDeltas},
        LockTime, SigHash, SigHasher, Transaction,
    },
    transparent,
};

use crate::{
    error::TransactionError,
    primitives::{halo2, BundleSighashes},
    transaction::{check, zip248, Request, Verifier},
};

use super::{mock_transparent_transfer, TestVerifier};

/// The height at which [`network`] activates NU7.1.
const NU7_1_HEIGHT: Height = Height(13);

/// A network that activates every upgrade by height 12, and NU7.1 at [`NU7_1_HEIGHT`].
fn network() -> Network {
    Parameters::build()
        .with_activation_heights(ConfiguredActivationHeights {
            before_overwinter: Some(1),
            overwinter: Some(2),
            sapling: Some(3),
            blossom: Some(4),
            heartwood: Some(5),
            canopy: Some(6),
            nu5: Some(7),
            nu6: Some(8),
            nu6_1: Some(9),
            nu6_2: Some(10),
            nu6_3: Some(11),
            nu7: Some(12),
            nu7_1: Some(NU7_1_HEIGHT.0),
            #[cfg(zcash_unstable = "zfuture")]
            zfuture: None,
        })
        .expect("activation heights are ordered")
        .clear_funding_streams()
        .to_network()
        .expect("the configured network parameters are valid")
}

fn amount(zatoshis: i64) -> Amount<NegativeAllowed> {
    zatoshis.try_into().expect("test amounts are valid")
}

fn v7(
    inputs: Vec<transparent::Input>,
    outputs: Vec<transparent::Output>,
    value_deltas: ValueDeltas,
    coinbase: Option<CoinbaseBundle>,
) -> Transaction {
    Transaction::V7 {
        network_upgrade: NetworkUpgrade::Nu7_1,
        lock_time: LockTime::unlocked(),
        expiry_height: Height(0),
        inputs,
        outputs,
        sapling_shielded_data: None,
        orchard_shielded_data: None,
        ironwood_shielded_data: None,
        v7_fields: Box::new(V7Fields {
            value_deltas,
            coinbase,
        }),
    }
}

/// A transparent v7 transfer that spends 10_000 zatoshis, pays 1, and states a fee of
/// 9_999, with the output it spends.
fn transfer(value_deltas: ValueDeltas) -> (Transaction, Vec<transparent::Output>) {
    let (input, output, known_utxos) =
        mock_transparent_transfer(Height(1), true, 0, 10_000.try_into().expect("valid"));
    let spent = known_utxos
        .into_values()
        .map(|utxo| utxo.utxo.output)
        .collect();
    (v7(vec![input], vec![output], value_deltas, None), spent)
}

fn balanced_transfer_deltas() -> ValueDeltas {
    ValueDeltas {
        transparent: amount(9_999),
        fee: amount(-9_999),
        ..ValueDeltas::default()
    }
}

/// A v7 coinbase transaction that pays 100 zatoshis of subsidy and collects no fees.
fn coinbase(value_deltas: ValueDeltas) -> Transaction {
    let bundle = CoinbaseBundle::new(
        NU7_1_HEIGHT,
        100.try_into().expect("valid"),
        Amount::zero(),
        b"miner".to_vec(),
        None,
    )
    .expect("the coinbase fields are in range");
    let output = transparent::Output {
        value: 100.try_into().expect("valid"),
        lock_script: transparent::Script::new(&[0x51]),
    };
    v7(Vec::new(), vec![output], value_deltas, Some(bundle))
}

/// Runs both phases of the ZIP 248 checks in the verifier's order.
fn zip248_check(
    tx: &Transaction,
    network_upgrade: NetworkUpgrade,
    spent_outputs: &[transparent::Output],
) -> Result<(), TransactionError> {
    zip248::check_bundles_and_deltas(tx, network_upgrade)?;
    zip248::check_spent_outputs(tx, spent_outputs)
}

fn check_at_nu7_1(
    tx: &Transaction,
    spent_outputs: &[transparent::Output],
) -> Result<(), TransactionError> {
    zip248_check(tx, NetworkUpgrade::Nu7_1, spent_outputs)
}

#[test]
fn balanced_transfer_passes() {
    let _init_guard = zakura_test::init();
    let (tx, spent) = transfer(balanced_transfer_deltas());
    assert_eq!(check_at_nu7_1(&tx, &spent), Ok(()));
}

#[test]
fn coinbase_balancing_its_subsidy_passes() {
    let _init_guard = zakura_test::init();
    let tx = coinbase(ValueDeltas {
        transparent: amount(-100),
        coinbase: amount(100),
        ..ValueDeltas::default()
    });
    assert_eq!(check_at_nu7_1(&tx, &[]), Ok(()));
}

/// NU7.1 activates every bundle except ZIP 233's NSM bundle, and no bundle before
/// NU7.1.
#[test]
fn bundles_must_be_active() {
    let _init_guard = zakura_test::init();
    assert_eq!(zip248::activation(BundleKind::Nsm), None);

    let (with_nsm, spent) = transfer(ValueDeltas {
        nsm: amount(-1),
        fee: amount(-9_998),
        ..balanced_transfer_deltas()
    });
    assert_eq!(
        check_at_nu7_1(&with_nsm, &spent),
        Err(TransactionError::V7BundleNotActive(BundleKind::Nsm))
    );

    let (tx, spent) = transfer(balanced_transfer_deltas());
    assert_eq!(
        zip248_check(&tx, NetworkUpgrade::Nu7, &spent),
        Err(TransactionError::V7BundleNotActive(BundleKind::Transparent))
    );
}

#[test]
fn deltas_need_their_effecting_bundles() {
    let _init_guard = zakura_test::init();
    let no_transparent_bundle = v7(
        Vec::new(),
        Vec::new(),
        ValueDeltas {
            transparent: amount(5),
            fee: amount(-5),
            ..ValueDeltas::default()
        },
        None,
    );
    assert_eq!(
        check_at_nu7_1(&no_transparent_bundle, &[]),
        Err(TransactionError::V7DeltaWithoutBundle(
            BundleKind::Transparent
        ))
    );

    let (no_coinbase_bundle, spent) = transfer(ValueDeltas {
        coinbase: amount(1),
        fee: amount(-10_000),
        ..balanced_transfer_deltas()
    });
    assert_eq!(
        check_at_nu7_1(&no_coinbase_bundle, &spent),
        Err(TransactionError::V7DeltaWithoutBundle(BundleKind::Coinbase))
    );
}

#[test]
fn fee_sign_depends_on_the_transaction_kind() {
    let _init_guard = zakura_test::init();
    let (adds_value, spent) = transfer(ValueDeltas {
        transparent: amount(-1),
        fee: amount(1),
        ..ValueDeltas::default()
    });
    assert_eq!(
        check_at_nu7_1(&adds_value, &spent),
        Err(TransactionError::V7FeeAddsValue)
    );

    let removes_value = coinbase(ValueDeltas {
        transparent: amount(-100),
        coinbase: amount(101),
        fee: amount(-1),
        ..ValueDeltas::default()
    });
    assert_eq!(
        check_at_nu7_1(&removes_value, &[]),
        Err(TransactionError::V7CoinbaseFeeRemovesValue)
    );
}

/// ZIP 233's NSM delta removes value from circulation, so a positive one is rejected.
#[test]
fn nsm_delta_must_not_add_value() {
    let _init_guard = zakura_test::init();
    let with_nsm = |nsm: i64| {
        transfer(ValueDeltas {
            nsm: amount(nsm),
            fee: amount(-9_999 - nsm),
            ..balanced_transfer_deltas()
        })
        .0
    };
    let deltas = |tx: &Transaction| tx.v7_fields().expect("the fixture is v7").value_deltas;

    let adds_value = with_nsm(1);
    assert_eq!(
        zip248::delta_signs_are_valid(&adds_value, &deltas(&adds_value)),
        Err(TransactionError::V7NsmAddsValue)
    );
    let removes_value = with_nsm(-1);
    assert_eq!(
        zip248::delta_signs_are_valid(&removes_value, &deltas(&removes_value)),
        Ok(())
    );

    // NU7.1 does not activate the NSM bundle, so the full check rejects it first.
    assert_eq!(
        zip248::check_bundles_and_deltas(&adds_value, NetworkUpgrade::Nu7_1),
        Err(TransactionError::V7BundleNotActive(BundleKind::Nsm))
    );
}

#[test]
fn deltas_must_sum_to_zero() {
    let _init_guard = zakura_test::init();
    let (tx, spent) = transfer(ValueDeltas {
        fee: amount(-9_998),
        ..balanced_transfer_deltas()
    });
    assert_eq!(
        check_at_nu7_1(&tx, &spent),
        Err(TransactionError::V7UnbalancedValuePoolDeltas)
    );
}

/// A transaction that balances its own deltas still cannot misstate the value it spends.
#[test]
fn transparent_delta_must_match_spent_outputs() {
    let _init_guard = zakura_test::init();
    let (tx, spent) = transfer(ValueDeltas {
        transparent: amount(9_000),
        fee: amount(-9_000),
        ..ValueDeltas::default()
    });
    assert_eq!(
        check_at_nu7_1(&tx, &spent),
        Err(TransactionError::V7TransparentDeltaMismatch)
    );
}

#[test]
fn earlier_versions_are_not_checked() {
    let _init_guard = zakura_test::init();
    let (
        Transaction::V7 {
            inputs, outputs, ..
        },
        spent,
    ) = transfer(balanced_transfer_deltas())
    else {
        unreachable!("the fixture is a v7 transaction");
    };
    let v5 = Transaction::V5 {
        network_upgrade: NetworkUpgrade::Nu6_3,
        lock_time: LockTime::unlocked(),
        expiry_height: Height(0),
        inputs,
        outputs,
        sapling_shielded_data: None,
        orchard_shielded_data: None,
    };
    assert_eq!(zip248_check(&v5, NetworkUpgrade::Nu5, &spent), Ok(()));
}

/// ZIP 248 counts a coinbase bundle as an input and a ZIP 233 amount as an output.
#[test]
fn inputs_and_outputs_include_v7_bundles() {
    let _init_guard = zakura_test::init();
    let tx = coinbase(ValueDeltas::default());
    assert!(!tx.has_transparent_or_shielded_inputs());
    assert_eq!(check::has_inputs_and_outputs(&tx), Ok(()));

    let (input, _, _) = mock_transparent_transfer(Height(1), true, 0, Amount::zero());
    let burns = v7(
        vec![input.clone()],
        Vec::new(),
        ValueDeltas {
            nsm: amount(-1),
            ..ValueDeltas::default()
        },
        None,
    );
    assert_eq!(check::has_inputs_and_outputs(&burns), Ok(()));

    let pays_nothing = v7(vec![input], Vec::new(), ValueDeltas::default(), None);
    assert_eq!(
        check::has_inputs_and_outputs(&pays_nothing),
        Err(TransactionError::NoOutputs)
    );
}

#[test]
fn v7_coinbase_must_not_spend_transparent_outputs() {
    let _init_guard = zakura_test::init();
    let (input, _, _) = mock_transparent_transfer(Height(1), true, 0, Amount::zero());
    let Transaction::V7 {
        inputs, v7_fields, ..
    } = &mut coinbase(ValueDeltas::default())
    else {
        unreachable!("the fixture is a v7 transaction");
    };
    inputs.push(input);
    let tx = v7(
        inputs.clone(),
        Vec::new(),
        ValueDeltas::default(),
        v7_fields.coinbase.clone(),
    );
    assert!(tx.is_coinbase());
    assert_eq!(
        check::coinbase_tx_no_prevout_joinsplit_spend(&tx),
        Err(TransactionError::CoinbaseHasTransparentInput)
    );
}

/// A v7 transfer whose second input spends the null previous output, which a v7
/// transaction decodes as a spend, with the known output its first input spends.
fn spends_the_null_prevout() -> (
    Transaction,
    HashMap<transparent::OutPoint, transparent::OrderedUtxo>,
) {
    let (input, output, known_utxos) =
        mock_transparent_transfer(Height(1), true, 0, 10_000.try_into().expect("valid"));
    let null_prevout = transparent::Input::PrevOut {
        outpoint: transparent::OutPoint::NULL,
        unlock_script: transparent::Script::new(&[]),
        sequence: u32::MAX,
    };
    let tx = v7(
        vec![input, null_prevout],
        vec![output],
        balanced_transfer_deltas(),
        None,
    );
    (tx, known_utxos)
}

#[test]
fn v7_non_coinbase_must_not_spend_the_null_prevout() {
    let _init_guard = zakura_test::init();
    let (tx, _) = spends_the_null_prevout();
    assert!(tx.is_valid_non_coinbase());
    assert_eq!(
        check::non_coinbase_has_no_null_prevout(&tx),
        Err(TransactionError::NonCoinbaseHasNullPrevout)
    );

    let (tx, _) = transfer(balanced_transfer_deltas());
    assert_eq!(check::non_coinbase_has_no_null_prevout(&tx), Ok(()));
}

/// The verifier rejects a null prevout before it looks up any spent output: the state
/// services here panic on any request.
#[tokio::test]
async fn verifier_rejects_a_v7_null_prevout_before_any_lookup() {
    let _init_guard = zakura_test::init();
    let (tx, known_utxos) = spends_the_null_prevout();
    assert_eq!(
        verify_in_block(tx.clone(), known_utxos, NU7_1_HEIGHT).await,
        Err(TransactionError::NonCoinbaseHasNullPrevout)
    );

    let verifier = Verifier::new_for_tests(
        &network(),
        service_fn(|_| async { unreachable!("the null prevout fails before any lookup") }),
    );
    let result = verifier
        .oneshot(Request::Mempool {
            transaction: tx.into(),
            height: NU7_1_HEIGHT,
        })
        .await;
    assert_eq!(
        result.map(|_| ()),
        Err(TransactionError::NonCoinbaseHasNullPrevout)
    );
}

#[test]
fn v7_has_a_verifier() {
    let _init_guard = zakura_test::init();
    let (tx, _) = transfer(balanced_transfer_deltas());
    assert_eq!(check::version_has_a_verifier(&tx), Ok(()));
}

#[test]
fn v7_is_valid_only_from_nu7_1() {
    let _init_guard = zakura_test::init();
    let (tx, _) = transfer(balanced_transfer_deltas());
    assert_eq!(
        TestVerifier::verify_v7_transaction_network_upgrade(&tx, NetworkUpgrade::Nu7),
        Err(TransactionError::UnsupportedByNetworkUpgrade(
            7,
            NetworkUpgrade::Nu7
        ))
    );
    assert_eq!(
        TestVerifier::verify_v7_transaction_network_upgrade(&tx, NetworkUpgrade::Nu7_1),
        Ok(())
    );
}

/// Verifies `tx` as a transaction of a block at `height`, spending `known_utxos`.
async fn verify_in_block(
    tx: Transaction,
    known_utxos: HashMap<transparent::OutPoint, transparent::OrderedUtxo>,
    height: Height,
) -> Result<Option<Amount<NonNegative>>, TransactionError> {
    let verifier = Verifier::new_for_tests(
        &network(),
        service_fn(|_| async { unreachable!("block transactions do not query the state") }),
    );
    let response = verifier
        .oneshot(Request::Block {
            transaction_id: tx.unmined_id(),
            transaction: Arc::new(tx).into(),
            known_utxos: Arc::new(known_utxos),
            parent_utxos: Arc::new(HashMap::new()),
            known_outpoint_hashes: Arc::new(HashSet::new()),
            height,
            time: DateTime::<Utc>::MAX_UTC,
        })
        .await?;
    Ok(response.miner_fee())
}

/// The fee a v7 transaction pays is the fee it states.
#[tokio::test]
async fn verifier_accepts_a_v7_transfer_and_reads_its_stated_fee() {
    let _init_guard = zakura_test::init();
    let (input, output, known_utxos) =
        mock_transparent_transfer(Height(1), true, 0, 10_000.try_into().expect("valid"));
    let tx = v7(vec![input], vec![output], balanced_transfer_deltas(), None);

    assert_eq!(
        verify_in_block(tx, known_utxos, NU7_1_HEIGHT).await,
        Ok(Some(9_999.try_into().expect("valid")))
    );
}

#[tokio::test]
async fn verifier_applies_the_zip_248_rules() {
    let _init_guard = zakura_test::init();
    let (input, output, known_utxos) =
        mock_transparent_transfer(Height(1), true, 0, 10_000.try_into().expect("valid"));
    let misstated = ValueDeltas {
        transparent: amount(9_000),
        fee: amount(-9_000),
        ..ValueDeltas::default()
    };
    let tx = v7(vec![input], vec![output], misstated, None);

    assert_eq!(
        verify_in_block(tx, known_utxos, NU7_1_HEIGHT).await,
        Err(TransactionError::V7TransparentDeltaMismatch)
    );
}

#[tokio::test]
async fn verifier_rejects_v7_before_nu7_1() {
    let _init_guard = zakura_test::init();
    let (input, output, known_utxos) =
        mock_transparent_transfer(Height(1), true, 0, 10_000.try_into().expect("valid"));
    let tx = v7(vec![input], vec![output], balanced_transfer_deltas(), None);
    let before = (NU7_1_HEIGHT - 1).expect("NU7.1 activates above genesis");

    // The transaction commits to the NU7.1 branch ID, which is not the branch ID of a
    // block before NU7.1.
    assert_eq!(
        verify_in_block(tx, known_utxos, before).await,
        Err(TransactionError::WrongConsensusBranchId)
    );
}

/// A v7 transaction built by the `zcash_primitives` builder, with the outputs its
/// transparent inputs spend.
struct Signed {
    tx: Transaction,
    spent_outputs: Vec<transparent::Output>,
}

/// A P2PKH input of 100_000 zatoshis, signed with its v7 digest, pays an Ironwood output
/// of 85_000 zatoshis with real proofs, and the transaction states the 15_000 zatoshi
/// ZIP 317 fee.
static SHIELDING_TO_IRONWOOD: LazyLock<Signed> = LazyLock::new(build_ironwood_shielding);

/// An Orchard note of 100_000 zatoshis pays a transparent output of 85_000 zatoshis, with
/// real proofs, and the transaction states the 15_000 zatoshi ZIP 317 fee. NU7.1 accepts
/// no new Orchard outputs, so the bundle spends.
static SPENDING_FROM_ORCHARD: LazyLock<Signed> = LazyLock::new(build_orchard_spending);

fn orchard_spending_key() -> ::orchard::keys::SpendingKey {
    ::orchard::keys::SpendingKey::from_bytes([0; 32]).expect("valid spending key")
}

fn orchard_address() -> ::orchard::Address {
    ::orchard::keys::FullViewingKey::from(&orchard_spending_key()).address(
        ::orchard::keys::Diversifier::from_bytes([0; 11]),
        ::orchard::keys::Scope::External,
    )
}

fn build_ironwood_shielding() -> Signed {
    use zcash_protocol::{memo::MemoBytes, value::Zatoshis};
    use zcash_transparent::{
        address::{Script, TransparentAddress},
        builder::TransparentSigningSet,
        bundle::{OutPoint, TxOut},
    };

    let mut builder = v7_builder(BuildConfig::Standard {
        sapling_anchor: None,
        orchard_anchor: None,
        ironwood_anchor: Some(::orchard::Anchor::empty_tree()),
        orchard_padding: BundlePadding::DEFAULT,
        ironwood_padding: BundlePadding::DEFAULT,
    });

    let mut signing_set = TransparentSigningSet::new();
    let pubkey = signing_set
        .add_key(secp256k1::SecretKey::from_secret_bytes([0xcd; 32]).expect("valid secret key"));
    let script: Script = TransparentAddress::from_pubkey(&pubkey).script().into();
    builder
        .add_transparent_p2pkh_input(
            pubkey,
            OutPoint::new([1; 32], 0),
            TxOut::new(Zatoshis::const_from_u64(100_000), script.clone()),
        )
        .expect("the input is a P2PKH input of the added key");
    builder
        .add_ironwood_output::<Infallible>(
            None,
            orchard_address(),
            Zatoshis::const_from_u64(85_000),
            MemoBytes::empty(),
        )
        .expect("NU7.1 accepts Ironwood outputs");

    Signed {
        tx: build(builder, &signing_set, &[], &[]),
        spent_outputs: vec![transparent::Output {
            value: 100_000.try_into().expect("valid"),
            lock_script: transparent::Script::new(&script.0 .0),
        }],
    }
}

fn build_orchard_spending() -> Signed {
    use ::orchard::{
        note::{NoteVersion, RandomSeed, Rho},
        tree::MerkleHashOrchard,
        value::NoteValue,
        Note,
    };
    use incrementalmerkletree::{frontier::CommitmentTree, witness::IncrementalWitness};
    use zcash_protocol::value::Zatoshis;
    use zcash_transparent::{address::TransparentAddress, builder::TransparentSigningSet};

    let rho = Rho::from_bytes(&[1; 32]).expect("a canonical field element");
    let note = Note::from_parts(
        orchard_address(),
        NoteValue::from_raw(100_000),
        rho,
        RandomSeed::from_bytes([2; 32], &rho).expect("a valid seed"),
        NoteVersion::V2,
    )
    .expect("the note has a commitment");
    let mut tree = CommitmentTree::<MerkleHashOrchard, 32>::empty();
    tree.append(MerkleHashOrchard::from_cmx(&note.commitment().into()))
        .expect("the tree has room");
    let witness = IncrementalWitness::from_tree(tree).expect("the tree is nonempty");

    let mut builder = v7_builder(BuildConfig::Standard {
        sapling_anchor: None,
        orchard_anchor: Some(witness.root().into()),
        ironwood_anchor: None,
        orchard_padding: BundlePadding::DEFAULT,
        ironwood_padding: BundlePadding::DEFAULT,
    });
    builder
        .add_orchard_spend::<Infallible>(
            ::orchard::keys::FullViewingKey::from(&orchard_spending_key()),
            note,
            witness.path().expect("the witness has a path").into(),
        )
        .expect("the note is in the anchor's tree");
    builder
        .add_transparent_output(
            &TransparentAddress::PublicKeyHash([0; 20]),
            Zatoshis::const_from_u64(85_000),
        )
        .expect("NU7.1 accepts transparent outputs");

    Signed {
        tx: build(
            builder,
            &TransparentSigningSet::new(),
            &[],
            &[::orchard::keys::SpendAuthorizingKey::from(
                &orchard_spending_key(),
            )],
        ),
        spent_outputs: Vec::new(),
    }
}

/// A v7 transaction built by the `zcash_primitives` builder that spends two Sapling notes
/// of 40_000 and 30_000 zatoshis to a transparent output of 55_000 zatoshis, and states
/// the 15_000 zatoshi ZIP 317 fee. Its proofs are all zeros: this host has no Sapling
/// proving parameters, so tests of it check signatures only.
static SAPLING_SPENDING: LazyLock<Transaction> = LazyLock::new(build_sapling_spending_transaction);

fn build_sapling_spending_transaction() -> Transaction {
    use incrementalmerkletree::{frontier::CommitmentTree, witness::IncrementalWitness};
    use sapling_crypto::{value::NoteValue, zip32::ExtendedSpendingKey, Node, Rseed};
    use zcash_protocol::value::Zatoshis;
    use zcash_transparent::{address::TransparentAddress, builder::TransparentSigningSet};

    let extsk = ExtendedSpendingKey::master(&[]);
    let dfvk = extsk.to_diversifiable_full_viewing_key();
    // Not every diversifier gives an address, so take the first that does.
    let to = (0..=u8::MAX)
        .find_map(|first| {
            let mut diversifier = [0; 11];
            diversifier[0] = first;
            dfvk.fvk()
                .vk
                .to_payment_address(sapling_crypto::Diversifier(diversifier))
        })
        .expect("about half of all diversifiers are valid");
    let notes = [(40_000, [1; 32]), (30_000, [2; 32])].map(|(value, rseed)| {
        to.create_note(NoteValue::from_raw(value), Rseed::AfterZip212(rseed))
    });
    let mut tree = CommitmentTree::<Node, 32>::empty();
    tree.append(Node::from_cmu(&notes[0].cmu()))
        .expect("the tree has room");
    let mut first = IncrementalWitness::from_tree(tree.clone()).expect("the tree is nonempty");
    tree.append(Node::from_cmu(&notes[1].cmu()))
        .expect("the tree has room");
    first
        .append(Node::from_cmu(&notes[1].cmu()))
        .expect("the witness has room");
    let second = IncrementalWitness::from_tree(tree).expect("the tree is nonempty");

    let mut builder = v7_builder(BuildConfig::Standard {
        sapling_anchor: Some(first.root().into()),
        orchard_anchor: None,
        ironwood_anchor: None,
        orchard_padding: BundlePadding::DEFAULT,
        ironwood_padding: BundlePadding::DEFAULT,
    });
    for (note, witness) in notes.iter().zip([&first, &second]) {
        builder
            .add_sapling_spend::<Infallible>(
                dfvk.fvk().clone(),
                note.clone(),
                witness.path().expect("the witness has a path"),
            )
            .expect("the note is in the anchor's tree");
    }
    builder
        .add_transparent_output(
            &TransparentAddress::PublicKeyHash([0; 20]),
            Zatoshis::const_from_u64(55_000),
        )
        .expect("NU7.1 accepts transparent outputs");

    build(builder, &TransparentSigningSet::new(), &[extsk], &[])
}

/// A `zcash_primitives` builder for a v7 transaction at [`NU7_1_HEIGHT`].
fn v7_builder(config: BuildConfig) -> Builder<Network, ()> {
    let mut builder = Builder::new(network(), NU7_1_HEIGHT.0.into(), config);
    builder
        .propose_version::<Infallible>(TxVersion::V7)
        .expect("NU7.1 accepts v7 transactions");
    builder
}

/// Builds, signs, and proves the transaction, then decodes it with Zakura.
fn build(
    builder: Builder<Network, ()>,
    signing_set: &zcash_transparent::builder::TransparentSigningSet,
    sapling_keys: &[sapling_crypto::zip32::ExtendedSpendingKey],
    orchard_keys: &[::orchard::keys::SpendAuthorizingKey],
) -> Transaction {
    let built = builder
        .build(
            signing_set,
            sapling_keys,
            orchard_keys,
            rand_10::rng(),
            &ZeroSaplingProofs,
            &ZeroSaplingProofs,
            &zcash_primitives::transaction::fees::zip317::FeeRule::standard(),
        )
        .expect("the transaction balances to its ZIP 317 fee");
    let mut bytes = Vec::new();
    built
        .transaction()
        .write(&mut bytes)
        .expect("writing to a vector succeeds");
    bytes
        .zcash_deserialize_into()
        .expect("Zakura decodes the transactions zcash_primitives builds")
}

/// A Sapling prover that makes all-zero proofs, for tests that check only signatures.
struct ZeroSaplingProofs;

/// The size of a Groth16 proof: two G1 points and one G2 point, compressed.
const GROTH_PROOF_SIZE: usize = 48 + 96 + 48;

impl sapling_crypto::prover::SpendProver for ZeroSaplingProofs {
    type Proof = sapling_crypto::bundle::GrothProofBytes;

    fn prepare_circuit(
        _: sapling_crypto::ProofGenerationKey,
        _: sapling_crypto::Diversifier,
        _: sapling_crypto::Rseed,
        _: sapling_crypto::value::NoteValue,
        _: jubjub::Fr,
        _: sapling_crypto::value::ValueCommitTrapdoor,
        _: bls12_381::Scalar,
        _: sapling_crypto::MerklePath,
    ) -> Option<sapling_crypto::circuit::Spend> {
        Some(sapling_crypto::circuit::Spend {
            value_commitment_opening: None,
            proof_generation_key: None,
            payment_address: None,
            commitment_randomness: None,
            ar: None,
            auth_path: Vec::new(),
            anchor: None,
        })
    }

    fn create_proof<R: rand_core_10::Rng>(
        &self,
        _: sapling_crypto::circuit::Spend,
        _: &mut R,
    ) -> Self::Proof {
        [0; GROTH_PROOF_SIZE]
    }

    fn encode_proof(proof: Self::Proof) -> sapling_crypto::bundle::GrothProofBytes {
        proof
    }
}

impl sapling_crypto::prover::OutputProver for ZeroSaplingProofs {
    type Proof = sapling_crypto::bundle::GrothProofBytes;

    fn prepare_circuit(
        _: &sapling_crypto::keys::EphemeralSecretKey,
        _: sapling_crypto::PaymentAddress,
        _: jubjub::Fr,
        _: sapling_crypto::value::NoteValue,
        _: sapling_crypto::value::ValueCommitTrapdoor,
    ) -> sapling_crypto::circuit::Output {
        sapling_crypto::circuit::Output {
            value_commitment_opening: None,
            payment_address: None,
            commitment_randomness: None,
            esk: None,
        }
    }

    fn create_proof<R: rand_core_10::Rng>(
        &self,
        _: sapling_crypto::circuit::Output,
        _: &mut R,
    ) -> Self::Proof {
        [0; GROTH_PROOF_SIZE]
    }

    fn encode_proof(proof: Self::Proof) -> sapling_crypto::bundle::GrothProofBytes {
        proof
    }
}

/// The verifier checks the transparent signature against its v7 digest and each
/// Ironwood signature against its own digest, and pays the stated fee.
#[tokio::test]
async fn verifier_accepts_a_signed_v7_shielding_transaction() {
    let _init_guard = zakura_test::init();
    let Signed { tx, spent_outputs } = &*SHIELDING_TO_IRONWOOD;
    let transparent::Input::PrevOut { outpoint, .. } = tx.inputs()[0] else {
        unreachable!("the fixture spends a transparent output");
    };
    let known_utxos = HashMap::from([(
        outpoint,
        transparent::OrderedUtxo::new(spent_outputs[0].clone(), Height(1), 1),
    )]);

    assert_eq!(
        verify_in_block(tx.clone(), known_utxos, NU7_1_HEIGHT).await,
        Ok(Some(15_000.try_into().expect("valid")))
    );
}

/// An Orchard or Ironwood bundle of a v7 transaction verifies against its per-signature
/// digests, and not against one digest shared by every signature.
#[test]
fn orchard_style_bundles_verify_only_against_their_per_signature_digests() {
    let _init_guard = zakura_test::init();
    for (pool, fixture) in [
        (ShieldedBundle::Orchard, &SPENDING_FROM_ORCHARD),
        (ShieldedBundle::Ironwood, &SHIELDING_TO_IRONWOOD),
    ] {
        let Signed { tx, spent_outputs } = &**fixture;
        let sighasher = SigHasher::new(tx, NetworkUpgrade::Nu7_1, Arc::new(spent_outputs.clone()))
            .expect("the transaction converts");
        let bundle = match pool {
            ShieldedBundle::Orchard => sighasher.orchard_bundle(),
            _ => sighasher.ironwood_bundle(),
        }
        .expect("the transaction has a bundle in the pool");
        let sighashes = sighasher
            .v7_shielded_sighashes(pool)
            .expect("the spent output is known");
        assert!(
            bundle.actions().len() >= 2,
            "padding adds a dummy action, whose signature has its own digest"
        );
        let shared = SigHash(sighashes.binding());

        assert!(
            halo2::Item::new(
                bundle.clone(),
                BundleSighashes::PerSignature(Arc::new(sighashes))
            )
            .verify_single(&halo2::VERIFYING_KEY_NU6_3_ONWARD),
            "{pool:?}"
        );
        assert!(
            !halo2::Item::new(bundle, shared).verify_single(&halo2::VERIFYING_KEY_NU6_3_ONWARD),
            "{pool:?}"
        );
    }
}

/// Each Sapling signature of a v7 transaction verifies against the digest that
/// [`BundleSighashes`] supplies for it, and a spend authorization signature does not
/// verify against the digest of the binding signature.
#[test]
fn sapling_signatures_verify_only_against_their_per_signature_digests() {
    use sapling_crypto::{bundle::SignatureDigests, value::CommitmentSum};

    let _init_guard = zakura_test::init();
    let sighasher = SigHasher::new(
        &SAPLING_SPENDING,
        NetworkUpgrade::Nu7_1,
        Arc::new(Vec::new()),
    )
    .expect("the transaction converts");
    let bundle = sighasher
        .sapling_bundle()
        .expect("the transaction has a Sapling bundle");
    let sighashes = BundleSighashes::PerSignature(Arc::new(
        sighasher
            .v7_shielded_sighashes(ShieldedBundle::Sapling)
            .expect("the transaction has no transparent inputs"),
    ));
    let shared = BundleSighashes::Shared(SigHash(SignatureDigests::binding(&sighashes)));
    assert_eq!(bundle.shielded_spends().len(), 2);

    for (index, spend) in bundle.shielded_spends().iter().enumerate() {
        let own = SignatureDigests::spend_auth(&sighashes, index).expect("the spend exists");
        assert!(spend.rk().verify(&own, spend.spend_auth_sig()).is_ok());
        let other =
            SignatureDigests::spend_auth(&shared, index).expect("a shared digest covers it");
        assert!(spend.rk().verify(&other, spend.spend_auth_sig()).is_err());
    }

    let spends = CommitmentSum::try_sum(bundle.shielded_spends().iter().map(|spend| spend.cv()))
        .expect("the value commitments are valid");
    let outputs =
        CommitmentSum::try_sum(bundle.shielded_outputs().iter().map(|output| output.cv()))
            .expect("the value commitments are valid");
    let bvk = (spends - outputs).into_bvk(i64::from(*bundle.value_balance()));
    assert!(bvk
        .verify(
            &SignatureDigests::binding(&sighashes),
            &bundle.authorization().binding_sig
        )
        .is_ok());
}
