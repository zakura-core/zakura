// Post-quantum transparent outputs, through the node's real UTXO-bound transaction and
// script validators. Included once per signature scheme; `crypto` and `PARAMETERS` name the
// scheme under test.
use self::crypto::SigningKey;

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
use zcash_protocol::consensus::NetworkType;
use zcash_script::{pq as script_pq, script::Code};
use zcash_transparent::{
    pq::{output, template},
    util::hash160,
};

use crate::{
    error::TransactionError,
    transaction::{check, Request, Response, Verifier},
    BoxError,
};

const ACTIVATION: Height = Height(10);
/// The input that spends the post-quantum output. Input 0 spends an ordinary one, so every
/// fixture also exercises a transaction with both kinds of input side by side.
const PQ_INPUT: usize = 1;

static KEY: LazyLock<SigningKey> = LazyLock::new(|| SigningKey::from_seed(&[7; crypto::SEED_BYTES]));
static SPEND: LazyLock<Spend> =
    LazyLock::new(|| Spend::signed(single_signer(), HashType::ALL, context(), 1));

/// The context a signature for this scheme is made under on a Regtest chain. A signature
/// made under any other context, including the same scheme on another network, fails.
fn context() -> &'static [u8] {
    output::signing_context(NetworkType::Regtest, PARAMETERS)
}

fn network(activation: Option<Height>) -> Network {
    Network::new_regtest(RegtestParameters {
        pq_activation_height: activation,
        activation_heights: ConfiguredActivationHeights {
            nu5: Some(1),
            ..Default::default()
        },
        ..Default::default()
    })
}

/// Pushes `bytes` with the shortest opcode that can carry them, which is the only encoding a
/// post-quantum input may use: the scanner rejects a longer one, so an input cannot be padded.
fn push(bytes: &[u8]) -> Vec<u8> {
    let mut result = match bytes.len() {
        0 => vec![0x00],
        length @ 1..=0x4b => vec![u8::try_from(length).unwrap()],
        length @ 0x4c..=0xff => vec![0x4c, u8::try_from(length).unwrap()],
        length => {
            let mut prefix = vec![0x4d];
            prefix.extend_from_slice(&u16::try_from(length).unwrap().to_le_bytes());
            prefix
        }
    };
    if let [byte] = bytes {
        assert!(
            *byte != 0x81 && !(1..=16).contains(byte),
            "a one-byte value in this range has its own opcode, which the fixtures do not need"
        );
    }
    result.extend_from_slice(bytes);
    result
}

/// The canonical body for one signer, which is what a wallet writes.
fn single_signer() -> Code {
    template::single_key_body(PARAMETERS, &KEY.public_key()).unwrap()
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

fn output_with(bytes: &[u8]) -> Output {
    Output {
        value: 100_000u64.try_into().unwrap(),
        lock_script: Script::new(bytes),
    }
}

/// An ordinary pay-to-script-hash output, for the legacy input beside the post-quantum one.
fn p2sh_output(redeem: &Code) -> Output {
    let mut bytes = vec![0xa9, 20];
    bytes.extend_from_slice(&hash160::hash(&redeem.0));
    bytes.push(0x87);
    output_with(&bytes)
}

/// The output that commits to `body`: the format, the scheme, and the body's hash.
fn pq_output(body: &Code) -> Output {
    output_with(&output::output_script(PARAMETERS, body).unwrap().0)
}

/// A script in the post-quantum namespace with these header bytes and commitment length,
/// for the shapes no rules define.
fn namespace_script(format: u8, scheme: u8, commitment_bytes: usize) -> Vec<u8> {
    let mut bytes = vec![format, scheme, u8::try_from(commitment_bytes).unwrap()];
    bytes.extend(std::iter::repeat_n(0x11, commitment_bytes));
    bytes
}

#[derive(Clone)]
struct Spend {
    transaction: Transaction,
    previous_outputs: Vec<Output>,
    /// The script the post-quantum output commits to, and all that its spend executes.
    body: Code,
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
        let previous_outputs = vec![p2sh_output(&Code(vec![0x51])), pq_output(&body)];
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
            body,
        }
    }

    /// Signs the digest the node computes for this input, over the committed body.
    fn signature(&self, key: &SigningKey, mode: HashType, context: &[u8]) -> Vec<u8> {
        let digest = self
            .transaction
            .sighasher(NetworkUpgrade::Nu5, Arc::new(self.previous_outputs.clone()))
            .unwrap()
            .sighash(mode, Some((PQ_INPUT, self.body.0.clone())));
        let mut signature = key.sign(&digest.0, context).unwrap();
        signature.push(u8::try_from(mode.bits()).unwrap());
        signature
    }

    fn install_arguments(&mut self, arguments: &[Vec<u8>]) {
        let mut input = Vec::new();
        for argument in arguments {
            input.extend(push(argument));
        }
        input.extend(push(&self.body.0));
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
        let inputs = match &mut self.transaction {
            Transaction::V5 { inputs, .. }
            | Transaction::V6 { inputs, .. }
            | Transaction::V4 { inputs, .. } => inputs,
            _ => unreachable!("the fixtures are v4 and later"),
        };
        let Input::PrevOut { unlock_script, .. } = &mut inputs[PQ_INPUT] else {
            unreachable!()
        };
        unlock_script
    }

    fn output_mut(&mut self, index: usize) -> &mut Output {
        let outputs = match &mut self.transaction {
            Transaction::V5 { outputs, .. }
            | Transaction::V6 { outputs, .. }
            | Transaction::V4 { outputs, .. } => outputs,
            _ => unreachable!("the fixtures are v4 and later"),
        };
        &mut outputs[index]
    }

    /// Replaces the revealed body, optionally moving the output's commitment with it.
    fn replace_body(&mut self, body: Code, update_commitment: bool) {
        let old_body_push_len = push(&self.body.0).len();
        let bytes = self.input_mut().as_raw_bytes();
        let mut input = bytes[..bytes.len() - old_body_push_len].to_vec();
        input.extend(push(&body.0));
        *self.input_mut() = Script::new(&input);
        if update_commitment {
            self.previous_outputs[PQ_INPUT] = pq_output(&body);
        }
        self.body = body;
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
            transaction_id: spend.transaction.unmined_id(),
            transaction: Arc::new(spend.transaction.clone()).into(),
            known_utxos: Arc::new(HashMap::new()),
            known_outpoint_hashes: Arc::default(),
            parent_utxos: Arc::new(HashMap::new()),
            height,
            time: DateTime::<Utc>::MAX_UTC,
        })
        .await
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
        "large signature pushes must not hide the committed body's CHECKSIG"
    );
    let cached = SPEND
        .cached(&network(Some(ACTIVATION)), ACTIVATION)
        .unwrap();
    assert_eq!(
        cached.is_valid(0),
        Ok(()),
        "the ordinary legacy input beside it still executes"
    );
    assert_eq!(cached.p2sh_sigops(), 1);
}

#[tokio::test]
async fn a_pq_output_is_claimable_before_activation_and_refused_after() {
    let _guard = zakura_test::init();
    // The output is three pushes, so the legacy rules leave its commitment on the stack as a
    // non-zero value: before activation an empty input claims it, which is why a node that
    // has activated these rules must refuse to run one under the legacy interpreter. This is
    // the whole argument for the dedicated output, and it was not testable while the format
    // hid inside a pay-to-script-hash redeem script.
    let mut spend = SPEND.clone();
    *spend.input_mut() = Script::new(&[]);

    assert_eq!(
        spend
            .cached(&network(None), Height(11))
            .unwrap()
            .is_valid(PQ_INPUT),
        Ok(()),
        "a chain that has not activated these rules hands the output to anyone"
    );
    assert_eq!(
        spend
            .cached(&network(Some(ACTIVATION)), ACTIVATION)
            .unwrap()
            .is_valid(PQ_INPUT),
        Err(ScriptError::Pq(output::Error::Pq(
            script_pq::Error::MissingScript
        ))),
        "once the rules apply, the input has to reveal the committed script and satisfy it"
    );
}

/// A transaction that pays to `output_script` and needs no signature to spend its own input,
/// so that changing what it pays to cannot invalidate one. The creation rule reads only the
/// outputs.
fn paying_to(output_script: &[u8]) -> Spend {
    let mut spend = Spend::unsigned(Code(vec![0x51]), LockTime::unlocked());
    spend.output_mut(0).lock_script = Script::new(output_script);
    spend.install_arguments(&[]);
    spend
}

#[tokio::test]
async fn creating_an_output_no_rules_define_is_rejected_at_creation() {
    let _guard = zakura_test::init();
    let active = network(Some(ACTIVATION));

    // A defined format and scheme is an ordinary payment.
    let defined = paying_to(
        &template::single_key_output_script(PARAMETERS, &KEY.public_key())
            .unwrap()
            .0,
    );
    assert!(verify_block(&defined, &active, Height(11)).await.is_ok());

    // Everything else inside the namespace is restricted: an undefined format, a scheme no
    // parameter set defines, and a commitment that is not the length format 0 fixes. Each is
    // refused in the transaction that creates it, while the sender still holds the coins.
    for (name, script) in [
        ("undefined format", namespace_script(0x51, 0x51, 32)),
        ("undefined scheme", namespace_script(0x00, 0x60, 32)),
        ("short commitment", namespace_script(0x00, 0x51, 20)),
    ] {
        let spend = paying_to(&script);

        assert!(
            matches!(
                verify_block(&spend, &active, Height(11)).await,
                Err(TransactionError::RestrictedPqOutput { output_index: 0, .. })
            ),
            "{name} must be rejected at creation"
        );
        assert!(
            verify_block(&spend, &network(None), Height(11)).await.is_ok(),
            "{name} is an ordinary non-standard output before activation"
        );
    }

    // A near miss of the namespace is not in it. This script pushes the same commitment with
    // a longer push opcode than it needs, so it stays an ordinary output: it really is
    // spendable under the legacy rules and may already have an owner.
    let mut bytes = vec![0x00, 0x51, 0x4c, 32];
    bytes.extend(std::iter::repeat_n(0x11, 32));
    let near_miss = paying_to(&bytes);
    assert!(verify_block(&near_miss, &active, Height(11)).await.is_ok());
}

#[tokio::test]
async fn an_output_no_rules_define_cannot_be_spent_either() {
    let _guard = zakura_test::init();
    let mut spend = SPEND.clone();
    spend.previous_outputs[PQ_INPUT] = output_with(&namespace_script(0x00, 0x60, 32));
    *spend.input_mut() = Script::new(&[]);

    assert_eq!(
        spend.check(),
        Err(ScriptError::Pq(output::Error::UnsupportedScheme(16))),
    );
    assert_eq!(
        spend
            .cached(&network(None), Height(11))
            .unwrap()
            .is_valid(PQ_INPUT),
        Ok(()),
        "before activation the same output is claimable, which is what restricting it prevents"
    );
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
            .map(|key| spend.signature(key, HashType::ALL, context()))
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
                    TransactionError::Script(ScriptError::Pq(output::Error::Pq(
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
    // Duplicate keys are legal in legacy Script. This is a transport and capacity fixture;
    // the separate distinct-key test establishes signer matching.
    let mut spend = Spend::unsigned(
        multisig(20, &[KEY.public_key(); 20], false),
        LockTime::unlocked(),
    );
    let signature = spend.signature(&KEY, HashType::ALL, context());
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

#[test]
fn public_networks_and_the_heightless_constructor_cannot_select_pq() {
    for network in [
        Network::Mainnet,
        Network::new_default_testnet(),
        Network::new_configured_testnet(zakura_chain::parameters::testnet::Parameters::default()),
    ] {
        assert_eq!(network.pq_activation_height(), None);
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
fn only_v5_and_later_can_spend_a_pq_output() {
    // No signature is needed here: an accidentally enabled post-quantum path would succeed
    // in any version, so a digest mismatch cannot conceal a broken version gate.
    let mut spend = SPEND.clone();
    spend.replace_body(Code(vec![0x51]), true);
    let input = push(&spend.body.0);
    *spend.input_mut() = Script::new(&input);
    assert_eq!(spend.check(), Ok(()));

    let v5 = spend.transaction.clone();
    spend.transaction = Transaction::V4 {
        inputs: v5.inputs().to_vec(),
        outputs: v5.outputs().to_vec(),
        lock_time: LockTime::unlocked(),
        expiry_height: Height(100),
        joinsplit_data: None,
        sapling_shielded_data: None,
    };
    assert!(
        matches!(spend.check(), Err(ScriptError::Interpreter { .. })),
        "a v4 digest does not commit to the output being spent, so the namespace rule \
         refuses the output rather than the post-quantum rules accepting it"
    );

    // A v6 transaction retains the digest the signature needs, so it can spend one.
    spend.transaction = Transaction::V6 {
        network_upgrade: NetworkUpgrade::Nu7,
        inputs: v5.inputs().to_vec(),
        outputs: v5.outputs().to_vec(),
        lock_time: LockTime::unlocked(),
        expiry_height: Height(100),
        sapling_shielded_data: None,
        orchard_shielded_data: None,
        ironwood_shielded_data: None,
    };
    let nu7_network = Network::new_regtest(RegtestParameters {
        pq_activation_height: Some(ACTIVATION),
        activation_heights: ConfiguredActivationHeights {
            nu7: Some(1),
            ..Default::default()
        },
        ..Default::default()
    });
    assert_eq!(
        spend
            .cached(&nu7_network, ACTIVATION)
            .unwrap()
            .is_valid(PQ_INPUT),
        Ok(())
    );
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
            s.output_mut(1).value = 89_999u64.try_into().unwrap()
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
        pq_activation_height: Some(ACTIVATION),
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
        let spend = Spend::signed(single_signer(), mode, context(), 1);
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
fn the_commitment_and_the_input_bounds_are_enforced() {
    // A body that is not the one the output commits to is refused before anything runs.
    let mut changed = SPEND.clone();
    let mut body = changed.body.clone();
    body.0[1] ^= 1;
    changed.replace_body(body.clone(), false);
    assert_eq!(
        changed.check(),
        Err(ScriptError::Pq(output::Error::CommitmentMismatch))
    );
    // Moving the commitment to match it makes the input authentic again, and then the
    // signature, which was made over the original body, is what fails.
    changed.previous_outputs[PQ_INPUT] = pq_output(&body);
    assert_eq!(changed.check(), Err(ScriptError::ScriptInvalid));

    let mut changed = SPEND.clone();
    *changed.input_mut() = Script::new(&vec![0; script_pq::MAX_INPUT_BYTES + 1]);
    assert_eq!(
        changed.check(),
        Err(ScriptError::Pq(output::Error::Pq(
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
fn the_signing_context_binds_the_network_the_scheme_and_the_protocol() {
    for wrong in [
        b"wrong protocol".as_slice(),
        // The same scheme, signed for another network. The transaction digest says nothing
        // about which network it is for, so only this context separates them.
        output::signing_context(NetworkType::Main, PARAMETERS),
        output::signing_context(NetworkType::Test, PARAMETERS),
    ] {
        let spend = Spend::signed(single_signer(), HashType::ALL, wrong, 1);
        assert_eq!(spend.check(), Err(ScriptError::ScriptInvalid));
    }
    assert_eq!(
        Spend::signed(single_signer(), HashType::ALL, context(), 1).check(),
        Ok(())
    );
}

#[test]
fn locktime_and_multiple_checks_use_the_shared_script_rules() {
    let mut timelocked = Code(vec![0x5a, 0xb1, 0x75]); // 10 CHECKLOCKTIMEVERIFY DROP
    timelocked.0.extend(single_signer().0);
    let spend = Spend::signed_at_locktime(
        timelocked,
        HashType::ALL,
        context(),
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
            Err(ScriptError::Pq(output::Error::Pq(
                script_pq::Error::Script(zcash_script::script::Error::Interpreter(
                    _,
                    zcash_script::interpreter::Error::UnsatisfiedLockTime
                ))
            )))
        ));
    }

    let mut twice = single_signer();
    *twice.0.last_mut().unwrap() = 0xad; // CHECKSIGVERIFY
    twice.0.extend(single_signer().0);
    let spend = Spend::signed(twice, HashType::ALL, context(), 2);
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
fn relay_policy_rejects_pq_sized_inputs_until_the_rules_apply() {
    assert!(matches!(
        check::mempool_standard_input_scripts(&SPEND.transaction, &SPEND.previous_outputs),
        Err(TransactionError::NonStandardScriptSigSize {
            input_index: PQ_INPUT,
            ..
        })
    ));
}

#[test]
fn relay_policy_uses_the_authenticated_body_and_the_chain_activation() {
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

    // The exemption follows the output: an input this size spending an ordinary output gets
    // nothing from it.
    let mut changed = SPEND.clone();
    changed.previous_outputs[PQ_INPUT] = p2sh_output(&Code(vec![0x51]));
    assert!(policy(&changed, &active, ACTIVATION).is_err());

    // Nor does an input that reveals a body the output does not commit to.
    let mut changed = SPEND.clone();
    changed.body.0[1] ^= 1;
    changed.install_arguments(&[vec![0; crypto::SIGNATURE_BYTES]]);
    assert!(policy(&changed, &active, ACTIVATION).is_err());

    let mut multiple = Spend::unsigned(
        multisig(2, &[KEY.public_key(); 3], false),
        LockTime::unlocked(),
    );
    let signature = multiple.signature(&KEY, HashType::ALL, context());
    multiple.install_arguments(&[vec![], signature.clone(), signature]);
    assert_eq!(policy(&multiple, &active, ACTIVATION), Ok(()));

    // Policy still caps how much checking one input may ask a relay to do.
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
