use self::crypto::SigningKey;
// Local PQ activation through the real UTXO-bound transaction and script validators.

use std::{
    collections::HashMap,
    sync::{Arc, LazyLock},
};

use chrono::{DateTime, Utc};
use tower::{service_fn, ServiceExt};
use zakura_chain::{
    block::Height,
    parameters::{
        testnet::{ConfiguredActivationHeights, RegtestParameters},
        Network, NetworkUpgrade,
    },
    serialization::{ZcashDeserializeInto, ZcashSerialize},
    transaction::{Hash, HashType, LockTime, Transaction},
    transparent::{Input, OutPoint, Output, Script, Utxo},
};
use zakura_script::{CachedFfiTransaction, Error as ScriptError};
use zcash_script::{pq as script_pq, script::Code};
use zcash_transparent::{
    pq::p2sh,
    util::hash160,
};

use crate::{
    error::TransactionError,
    transaction::{check, Request, Response, Verifier},
    BoxError,
};

const ACTIVATION: Height = Height(10);
const PQ_INPUT: usize = 1;
static KEY: LazyLock<SigningKey> = LazyLock::new(|| SigningKey::from_seed(&[7; crypto::SEED_BYTES]));
static SPEND: LazyLock<Spend> =
    LazyLock::new(|| Spend::signed(single_signer(), HashType::ALL, p2sh::signing_context(PARAMETERS), 1));

fn network(activation: Option<Height>) -> Network {
    Network::new_regtest(RegtestParameters {
        pq_p2sh_activation_height: activation,
        activation_heights: ConfiguredActivationHeights {
            nu5: Some(1),
            ..Default::default()
        },
        ..Default::default()
    })
}

fn push(bytes: &[u8]) -> Vec<u8> {
    let mut result = vec![0x4d]; // PUSHDATA2, including for small fixture values.
    result.extend_from_slice(&u16::try_from(bytes.len()).unwrap().to_le_bytes());
    result.extend_from_slice(bytes);
    result
}

fn single_signer() -> Code {
    let mut bytes = vec![u8::try_from(crypto::PUBLIC_KEY_BYTES).unwrap()];
    bytes.extend_from_slice(&KEY.public_key());
    bytes.push(0xac); // CHECKSIG
    Code(bytes)
}

fn multisig(required: u8, keys: &[[u8; crypto::PUBLIC_KEY_BYTES]], verify: bool) -> Code {
    let number = |value: u8| {
        if value <= 16 {
            vec![0x50 + value]
        } else {
            push(&[value])
        }
    };
    let mut bytes = number(required);
    for key in keys {
        bytes.push(u8::try_from(crypto::PUBLIC_KEY_BYTES).unwrap());
        bytes.extend(key);
    }
    bytes.extend(number(u8::try_from(keys.len()).unwrap()));
    bytes.push(if verify { 0xaf } else { 0xae });
    if verify {
        bytes.push(0x51);
    }
    Code(bytes)
}

fn p2sh_output(redeem: &Code) -> Output {
    let mut bytes = vec![0xa9, 20];
    bytes.extend_from_slice(&hash160::hash(&redeem.0));
    bytes.push(0x87);
    Output {
        value: 100_000u64.try_into().unwrap(),
        lock_script: Script::new(&bytes),
    }
}

#[derive(Clone)]
struct Spend {
    transaction: Transaction,
    previous_outputs: Vec<Output>,
    redeem: Code,
}

impl Spend {
    fn signed(body: Code, mode: HashType, context: &[u8], signatures: usize) -> Self {
        Self::signed_at_locktime(body, mode, context, signatures, LockTime::unlocked())
    }

    fn signed_at_locktime(
        body: Code,
        mode: HashType,
        context: &[u8],
        signatures: usize,
        lock_time: LockTime,
    ) -> Self {
        let mut spend = Self::unsigned(body, lock_time);
        let signature = spend.signature(&KEY, mode, context);
        spend.install_arguments(&vec![signature; signatures]);
        spend
    }

    fn unsigned(body: Code, lock_time: LockTime) -> Self {
        let redeem = p2sh::redeem_script(PARAMETERS, &body).unwrap();
        let previous_outputs = vec![p2sh_output(&Code(vec![0x51])), p2sh_output(&redeem)];
        let transaction = Transaction::V5 {
            network_upgrade: NetworkUpgrade::Nu5,
            inputs: vec![
                Input::PrevOut {
                    outpoint: OutPoint {
                        hash: Hash([7; 32]),
                        index: 0,
                    },
                    unlock_script: Script::new(&push(&[0x51])),
                    sequence: 0,
                },
                Input::PrevOut {
                    outpoint: OutPoint {
                        hash: Hash([8; 32]),
                        index: 0,
                    },
                    unlock_script: Script::new(&[]),
                    sequence: 0,
                },
            ],
            outputs: vec![
                Output {
                    value: 90_000u64.try_into().unwrap(),
                    lock_script: Script::new(&[0x51])
                };
                2
            ],
            lock_time,
            expiry_height: Height(100),
            sapling_shielded_data: None,
            orchard_shielded_data: None,
        };
        Self {
            transaction,
            previous_outputs,
            redeem,
        }
    }

    fn signature(&self, key: &SigningKey, mode: HashType, context: &[u8]) -> Vec<u8> {
        let digest = self
            .transaction
            .sighasher(NetworkUpgrade::Nu5, Arc::new(self.previous_outputs.clone()))
            .unwrap()
            .sighash(mode, Some((PQ_INPUT, self.redeem.0.clone())));
        let mut signature = key.sign(&digest.0, context).unwrap();
        signature.push(u8::try_from(mode.bits()).unwrap());
        signature
    }

    fn install_arguments(&mut self, arguments: &[Vec<u8>]) {
        let mut input = Vec::new();
        for argument in arguments {
            input.extend(push(argument));
        }
        input.extend(push(&self.redeem.0));
        *self.input_mut() = Script::new(&input);
        // Exercise the received-transaction reader for every installed authorization.
        self.transaction = self
            .transaction
            .zcash_serialize_to_vec()
            .unwrap()
            .as_slice()
            .zcash_deserialize_into::<Transaction>()
            .unwrap();
    }

    fn cached(
        &self,
        network: &Network,
        height: Height,
    ) -> Result<CachedFfiTransaction, ScriptError> {
        CachedFfiTransaction::new_for_network(
            Arc::new(self.transaction.clone()),
            Arc::new(self.previous_outputs.clone()),
            network,
            height,
        )
    }

    fn check(&self) -> Result<(), ScriptError> {
        self.cached(&network(Some(ACTIVATION)), ACTIVATION)?
            .is_valid(PQ_INPUT)
    }

    fn input_mut(&mut self) -> &mut Script {
        let Transaction::V5 { inputs, .. } = &mut self.transaction else {
            unreachable!()
        };
        let Input::PrevOut { unlock_script, .. } = &mut inputs[PQ_INPUT] else {
            unreachable!()
        };
        unlock_script
    }

    fn replace_redeem(&mut self, redeem: Code, update_commitment: bool) {
        let old_redeem_push_len = push(&self.redeem.0).len();
        let bytes = self.input_mut().as_raw_bytes();
        let mut input = bytes[..bytes.len() - old_redeem_push_len].to_vec();
        input.extend(push(&redeem.0));
        *self.input_mut() = Script::new(&input);
        if update_commitment {
            self.previous_outputs[PQ_INPUT] = p2sh_output(&redeem);
        }
        self.redeem = redeem;
    }
}

/// Resolve previous outputs through the state service, rather than injecting them into a checker.
async fn verify_block(
    spend: &Spend,
    network: &Network,
    height: Height,
) -> Result<Response, TransactionError> {
    let utxos: HashMap<_, _> = spend
        .transaction
        .inputs()
        .iter()
        .zip(&spend.previous_outputs)
        .map(|(input, output)| {
            let Input::PrevOut { outpoint, .. } = input else {
                unreachable!()
            };
            (*outpoint, Utxo::new(output.clone(), Height(1), false))
        })
        .collect();
    let state = service_fn(move |request| {
        let zakura_state::Request::AwaitUtxo(outpoint) = request else {
            panic!("unexpected state request: {request:?}")
        };
        let output = utxos[&outpoint].clone();
        async move {
            // Deliberately let the second input finish first; digest ordering must follow vin.
            if outpoint.hash == Hash([7; 32]) {
                tokio::task::yield_now().await;
            }
            Ok::<_, BoxError>(zakura_state::Response::Utxo(output))
        }
    });
    let mempool = service_fn(|_: zakura_node_services::mempool::Request| async {
        Err::<zakura_node_services::mempool::Response, BoxError>(
            "block verification must not query mempool".into(),
        )
    });
    let (sender, receiver) = tokio::sync::oneshot::channel();
    assert!(sender.send(mempool).is_ok());
    let verifier = Verifier::new(network, state, receiver);
    verifier
        .oneshot(Request::Block {
            transaction_hash: spend.transaction.hash(),
            transaction: Arc::new(spend.transaction.clone()),
            known_utxos: Arc::new(HashMap::new()),
            known_outpoint_hashes: Arc::default(),
            height,
            time: DateTime::<Utc>::MAX_UTC,
        })
        .await
}

#[tokio::test]
async fn distinct_multisig_pairs_and_failed_matches_reach_transaction_validation() {
    let _guard = zakura_test::init();
    let bob = SigningKey::from_seed(&[8; crypto::SEED_BYTES]);
    let carol = SigningKey::from_seed(&[9; crypto::SEED_BYTES]);
    let keys = [&*KEY, &bob, &carol];
    for verify in [false, true] {
        let body = multisig(2, &keys.map(SigningKey::public_key), verify);
        let mut spend = Spend::unsigned(body, LockTime::unlocked());
        let signatures: Vec<_> = keys
            .iter()
            .map(|key| spend.signature(key, HashType::ALL, p2sh::signing_context(PARAMETERS)))
            .collect();
        // Pair [0, 1] must first skip the non-signing key at the top of the stack.
        for pair in [[0, 1], [0, 2], [1, 2]] {
            spend.install_arguments(&[
                vec![],
                signatures[pair[0]].clone(),
                signatures[pair[1]].clone(),
            ]);
            let response = verify_block(&spend, &network(Some(ACTIVATION)), Height(11))
                .await
                .unwrap();
            assert_eq!(response.tx_id(), spend.transaction.unmined_id());
            assert_eq!(
                response.sigops(),
                3,
                "multisig accounting charges the keys, including missed matches"
            );
        }
        let mut forged = signatures[0].clone();
        forged[0] ^= 1;
        for arguments in [
            vec![vec![], signatures[2].clone(), signatures[0].clone()],
            vec![vec![], forged, signatures[2].clone()],
        ] {
            spend.install_arguments(&arguments);
            let error = verify_block(&spend, &network(Some(ACTIVATION)), Height(11))
                .await
                .unwrap_err();
            if verify {
                assert!(matches!(
                    error,
                    TransactionError::Script(ScriptError::Pq(p2sh::Error::Pq(
                        script_pq::Error::Script(zcash_script::script::Error::Interpreter(
                            _,
                            zcash_script::interpreter::Error::Verify
                        ))
                    )))
                ));
            } else {
                assert!(matches!(
                    error,
                    TransactionError::Script(ScriptError::ScriptInvalid)
                ));
            }
        }
    }
}

#[tokio::test]
async fn twenty_signature_capacity_survives_node_parsing_and_sigop_accounting() {
    let _guard = zakura_test::init();
    // Duplicate keys are legal in legacy Script. This is a transport/capacity
    // fixture; the separate distinct-key test establishes signer matching.
    let mut spend = Spend::unsigned(
        multisig(20, &[KEY.public_key(); 20], false),
        LockTime::unlocked(),
    );
    let signature = spend.signature(&KEY, HashType::ALL, p2sh::signing_context(PARAMETERS));
    let mut arguments = vec![vec![]];
    arguments.extend(vec![signature; 20]);
    spend.install_arguments(&arguments);
    let input_bytes = spend.input_mut().as_raw_bytes().len();
    assert!(input_bytes > 32 * 1024);
    assert!(input_bytes <= script_pq::MAX_INPUT_BYTES);
    let response = verify_block(&spend, &network(Some(ACTIVATION)), Height(11))
        .await
        .unwrap();
    assert_eq!(response.tx_id(), spend.transaction.unmined_id());
    assert_eq!(response.sigops(), 20);
    assert!(matches!(
        check::mempool_standard_input_scripts(&spend.transaction, &spend.previous_outputs),
        Err(TransactionError::NonStandardScriptSigSize {
            input_index: PQ_INPUT,
            ..
        })
    ));
}

#[tokio::test]
async fn serialized_pq_spend_reaches_transaction_validation_and_accounting() {
    let _guard = zakura_test::init();
    let response = verify_block(&SPEND, &network(Some(ACTIVATION)), Height(11))
        .await
        .unwrap();
    assert_eq!(response.tx_id(), SPEND.transaction.unmined_id());
    assert_eq!(
        response.sigops(),
        1,
        "large signature pushes must not hide the redeem script's CHECKSIG"
    );
    let cached = SPEND
        .cached(&network(Some(ACTIVATION)), ACTIVATION)
        .unwrap();
    assert_eq!(
        cached.is_valid(0),
        Ok(()),
        "the ordinary legacy P2SH input still executes"
    );
    assert_eq!(cached.p2sh_sigops(), 1);
}

#[tokio::test]
async fn local_activation_is_enforced_by_transaction_validation() {
    let _guard = zakura_test::init();
    for (network, height) in [
        (network(Some(ACTIVATION)), Height(9)),
        (network(None), Height(11)),
    ] {
        assert!(matches!(
            verify_block(&SPEND, &network, height).await,
            Err(TransactionError::Script(_))
        ));
    }
    assert!(verify_block(&SPEND, &network(Some(ACTIVATION)), ACTIVATION)
        .await
        .is_ok());
}

#[tokio::test]
async fn isolated_custom_testnet_enforces_pq_activation() {
    let _guard = zakura_test::init();
    let custom = zakura_chain::parameters::testnet::Parameters::build()
        .with_network_name("PqActivationFixture").unwrap()
        .with_network_magic(zakura_chain::parameters::Magic([80, 81, 84, 50])).unwrap()
        .with_activation_heights(ConfiguredActivationHeights {
            nu5: Some(1), ..Default::default()
        }).unwrap()
        .with_funding_streams(Vec::new())
        .with_pq_p2sh_activation_height(ACTIVATION)
        .to_network().unwrap();
    assert!(matches!(verify_block(&SPEND, &custom, Height(9)).await,
        Err(TransactionError::Script(_))));
    assert!(verify_block(&SPEND, &custom, ACTIVATION).await.is_ok());
    assert!(verify_block(&SPEND, &custom, Height(11)).await.is_ok());
}

#[test]
fn public_networks_and_legacy_constructor_cannot_select_pq() {
    for network in [
        Network::Mainnet,
        Network::new_default_testnet(),
        Network::new_configured_testnet(zakura_chain::parameters::testnet::Parameters::default()),
    ] {
        assert_eq!(network.pq_p2sh_activation_height(), None);
        let height = NetworkUpgrade::Nu5.activation_height(&network).unwrap();
        assert!(SPEND
            .cached(&network, height)
            .unwrap()
            .is_valid(PQ_INPUT)
            .is_err());
    }
    assert!(CachedFfiTransaction::new(
        Arc::new(SPEND.transaction.clone()),
        Arc::new(SPEND.previous_outputs.clone()),
        NetworkUpgrade::Nu5
    )
    .unwrap()
    .is_valid(PQ_INPUT)
    .is_err());
}

#[test]
fn only_v5_can_use_local_pq_rules() {
    let mut spend = SPEND.clone();
    // No signature is needed: an accidentally enabled PQ path would succeed in any
    // version, so a digest mismatch cannot conceal a broken version gate.
    spend.replace_redeem(p2sh::redeem_script(PARAMETERS, &Code(vec![0x51])).unwrap(), true);
    let input = push(&spend.redeem.0);
    *spend.input_mut() = Script::new(&input);
    assert_eq!(spend.check(), Ok(()));
    spend.transaction = Transaction::V4 {
        inputs: spend.transaction.inputs().to_vec(),
        outputs: spend.transaction.outputs().to_vec(),
        lock_time: LockTime::unlocked(),
        expiry_height: Height(100),
        joinsplit_data: None,
        sapling_shielded_data: None,
    };
    assert!(spend.check().is_err());
    spend.transaction = Transaction::V6 {
        network_upgrade: NetworkUpgrade::Nu7,
        inputs: spend.transaction.inputs().to_vec(),
        outputs: spend.transaction.outputs().to_vec(),
        lock_time: LockTime::unlocked(),
        expiry_height: Height(100),
        sapling_shielded_data: None,
        orchard_shielded_data: None,
        ironwood_shielded_data: None,
    };
    let network = Network::new_regtest(RegtestParameters {
        pq_p2sh_activation_height: Some(ACTIVATION),
        activation_heights: ConfiguredActivationHeights {
            nu7: Some(1),
            ..Default::default()
        },
        ..Default::default()
    });
    assert!(spend
        .cached(&network, ACTIVATION)
        .unwrap()
        .is_valid(PQ_INPUT)
        .is_err());
}

#[tokio::test]
async fn real_previous_outputs_and_transaction_fields_are_bound() {
    let _guard = zakura_test::init();
    for (name, mutate) in [
        (
            "PQ input amount",
            (|s: &mut Spend| s.previous_outputs[1].value = 100_001u64.try_into().unwrap())
                as fn(&mut Spend),
        ),
        ("other input amount", |s| {
            s.previous_outputs[0].value = 100_001u64.try_into().unwrap()
        }),
        ("previous output order", |s| s.previous_outputs.swap(0, 1)),
        ("outpoint", |s| {
            if let Transaction::V5 { inputs, .. } = &mut s.transaction {
                if let Input::PrevOut { outpoint, .. } = &mut inputs[1] {
                    outpoint.index += 1;
                }
            }
        }),
        ("output value", |s| {
            if let Transaction::V5 { outputs, .. } = &mut s.transaction {
                outputs[1].value = 89_999u64.try_into().unwrap();
            }
        }),
        ("expiry height", |s| {
            if let Transaction::V5 { expiry_height, .. } = &mut s.transaction {
                *expiry_height = Height(101);
            }
        }),
        ("locktime", |s| {
            if let Transaction::V5 { lock_time, .. } = &mut s.transaction {
                *lock_time = LockTime::Height(Height(9));
            }
        }),
        ("signature", |s| {
            let mut input = s.input_mut().as_raw_bytes().to_vec();
            input[3] ^= 1;
            *s.input_mut() = Script::new(&input);
        }),
    ] {
        let mut changed = SPEND.clone();
        mutate(&mut changed);
        assert!(
            matches!(
                verify_block(&changed, &network(Some(ACTIVATION)), Height(11)).await,
                Err(TransactionError::Script(_))
            ),
            "{name}"
        );
    }
    let mut changed = SPEND.clone();
    changed.previous_outputs.pop();
    assert!(matches!(
        changed.cached(&network(Some(ACTIVATION)), ACTIVATION),
        Err(ScriptError::TxIndex)
    ));

    // Change both the trusted upgrade and the transaction's branch ID, so a branch
    // mismatch check cannot conceal a missing upgrade binding in the signed digest.
    let mut changed = SPEND.clone();
    if let Transaction::V5 {
        network_upgrade, ..
    } = &mut changed.transaction
    {
        *network_upgrade = NetworkUpgrade::Nu6;
    }
    let upgraded_network = Network::new_regtest(RegtestParameters {
        pq_p2sh_activation_height: Some(ACTIVATION),
        activation_heights: ConfiguredActivationHeights {
            nu6: Some(1),
            ..Default::default()
        },
        ..Default::default()
    });
    assert!(matches!(
        verify_block(&changed, &upgraded_network, Height(11)).await,
        Err(TransactionError::Script(ScriptError::ScriptInvalid))
    ));
}

#[tokio::test]
async fn existing_signing_modes_use_the_node_digest() {
    let _guard = zakura_test::init();
    for mode in [
        HashType::ALL,
        HashType::NONE,
        HashType::SINGLE,
        HashType::ALL_ANYONECANPAY,
        HashType::NONE_ANYONECANPAY,
        HashType::SINGLE_ANYONECANPAY,
    ] {
        let spend = Spend::signed(single_signer(), mode, p2sh::signing_context(PARAMETERS), 1);
        assert!(
            verify_block(&spend, &network(Some(ACTIVATION)), Height(11))
                .await
                .is_ok(),
            "{mode:?}"
        );
        if mode == HashType::SINGLE || mode == HashType::SINGLE_ANYONECANPAY {
            let mut changed = spend;
            if let Transaction::V5 { outputs, .. } = &mut changed.transaction {
                outputs.pop();
            }
            assert_eq!(changed.check(), Err(ScriptError::ScriptInvalid));
        }
    }
}

#[test]
fn authenticated_identifiers_and_input_bounds_are_enforced() {
    let mut changed = SPEND.clone();
    let mut redeem = changed.redeem.clone();
    redeem.0[5] = 3;
    changed.replace_redeem(redeem.clone(), false);
    assert_eq!(
        changed.check(),
        Err(ScriptError::Pq(p2sh::Error::CommitmentMismatch))
    );
    changed.previous_outputs[1] = p2sh_output(&redeem);
    assert_eq!(
        changed.check(),
        Err(ScriptError::Pq(p2sh::Error::UnsupportedIdentifier(3)))
    );

    let mut changed = SPEND.clone();
    *changed.input_mut() = Script::new(&vec![0; script_pq::MAX_INPUT_BYTES + 1]);
    assert_eq!(
        changed.check(),
        Err(ScriptError::Pq(p2sh::Error::Pq(
            script_pq::Error::InputSize
        )))
    );
    let mut changed = SPEND.clone();
    let mut bytes = changed.input_mut().as_raw_bytes().to_vec();
    bytes.pop();
    *changed.input_mut() = Script::new(&bytes);
    assert!(changed.check().is_err());
}

#[test]
fn signing_context_locktime_and_multiple_checks_use_shared_rules() {
    let wrong_context = Spend::signed(single_signer(), HashType::ALL, b"wrong protocol", 1);
    assert_eq!(wrong_context.check(), Err(ScriptError::ScriptInvalid));

    let mut timelocked = Code(vec![0x5a, 0xb1, 0x75]); // 10 CHECKLOCKTIMEVERIFY DROP
    timelocked.0.extend(single_signer().0);
    let spend = Spend::signed_at_locktime(
        timelocked,
        HashType::ALL,
        p2sh::signing_context(PARAMETERS),
        1,
        LockTime::Height(Height(10)),
    );
    assert_eq!(spend.check(), Ok(()));
    for final_sequence in [false, true] {
        let mut changed = spend.clone();
        if let Transaction::V5 {
            lock_time, inputs, ..
        } = &mut changed.transaction
        {
            if final_sequence {
                if let Input::PrevOut { sequence, .. } = &mut inputs[PQ_INPUT] {
                    *sequence = u32::MAX;
                }
            } else {
                *lock_time = LockTime::Height(Height(9));
            }
        }
        assert!(matches!(
            changed.check(),
            Err(ScriptError::Pq(p2sh::Error::Pq(script_pq::Error::Script(
                zcash_script::script::Error::Interpreter(
                    _,
                    zcash_script::interpreter::Error::UnsatisfiedLockTime
                )
            ))))
        ));
    }

    let mut twice = single_signer();
    *twice.0.last_mut().unwrap() = 0xad; // CHECKSIGVERIFY
    twice.0.extend(single_signer().0);
    let spend = Spend::signed(twice, HashType::ALL, p2sh::signing_context(PARAMETERS), 2);
    assert_eq!(spend.check(), Ok(()));
    assert_eq!(
        spend
            .cached(&network(Some(ACTIVATION)), ACTIVATION)
            .unwrap()
            .p2sh_sigops(),
        2
    );
}

#[test]
fn relay_policy_still_rejects_pq_sized_inputs() {
    assert!(matches!(
        check::mempool_standard_input_scripts(&SPEND.transaction, &SPEND.previous_outputs),
        Err(TransactionError::NonStandardScriptSigSize {
            input_index: PQ_INPUT,
            ..
        })
    ));
}

#[test]
fn relay_policy_uses_authenticated_pq_and_trusted_activation() {
    let active = network(Some(ACTIVATION));
    let policy = |spend: &Spend, net: &Network, height| {
        check::mempool_standard_input_scripts_for_network(
            &spend.transaction,
            &spend.previous_outputs,
            net,
            height,
        )
    };
    assert_eq!(policy(&SPEND, &active, ACTIVATION), Ok(()));
    for net in [
        network(None),
        Network::Mainnet,
        Network::new_default_testnet(),
    ] {
        assert!(policy(&SPEND, &net, ACTIVATION).is_err());
    }
    assert!(policy(&SPEND, &active, Height(9)).is_err());
    let mut changed = SPEND.clone();
    changed.previous_outputs[PQ_INPUT] = p2sh_output(&Code(vec![0x51]));
    assert!(policy(&changed, &active, ACTIVATION).is_err());

    let mut changed = SPEND.clone();
    changed.redeem.0[5] = 3;
    changed.previous_outputs[PQ_INPUT] = p2sh_output(&changed.redeem);
    changed.install_arguments(&[vec![0; crypto::SIGNATURE_BYTES + 1]]);
    assert!(policy(&changed, &active, ACTIVATION).is_err());

    let mut multiple = Spend::unsigned(
        multisig(2, &[KEY.public_key(); 3], false),
        LockTime::unlocked(),
    );
    let signature = multiple.signature(&KEY, HashType::ALL, p2sh::signing_context(PARAMETERS));
    multiple.install_arguments(&[vec![], signature.clone(), signature]);
    assert_eq!(policy(&multiple, &active, ACTIVATION), Ok(()));
    let mut too_many = Spend::unsigned(
        multisig(16, &[KEY.public_key(); 16], false),
        LockTime::unlocked(),
    );
    too_many.install_arguments(&[vec![], vec![0; crypto::SIGNATURE_BYTES + 1]]);
    assert_eq!(
        policy(&too_many, &active, ACTIVATION),
        Err(TransactionError::NonStandardInputs)
    );

    let mut oversized = SPEND.clone();
    *oversized.input_mut() = Script::new(&vec![0; script_pq::MAX_INPUT_BYTES + 1]);
    assert!(policy(&oversized, &active, ACTIVATION).is_err());
}
