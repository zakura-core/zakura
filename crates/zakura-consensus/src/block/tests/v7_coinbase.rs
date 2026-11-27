//! The block rules on a v7 coinbase transaction's coinbase bundle and value pool deltas
//! ([ZIP 248]).
//!
//! [ZIP 248]: https://zips.z.cash/zip-0248

use zakura_chain::{
    amount::NonNegative,
    parameters::{
        subsidy::{funding_stream_values, FundingStreamReceiver},
        testnet::{ConfiguredFundingStreamRecipient, ConfiguredFundingStreams, RegtestParameters},
    },
    transaction::v7::{CoinbaseBundle, V7Fields, ValueDeltas},
};

use super::*;

/// The height of the tested block, which is after NU7 and NU7.1.
const HEIGHT: Height = Height(7);

/// The fees of the block's other transactions, whichever their versions: v7
/// transactions state theirs, and earlier ones leave theirs implicitly.
const FEES: i64 = 10_000;

/// The ZIP 235 contribution of `fees`: 60% from NU7, rounded down.
fn nsm_fee_contribution(fees: i64) -> i64 {
    fees * 6 / 10
}

/// A Regtest network with NU7 at height 5, NU7.1 at height 6, and a lockbox funding
/// stream.
fn network() -> Network {
    Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu6_3: Some(1),
            nu7: Some(5),
            nu7_1: Some(6),
            ..Default::default()
        },
        funding_streams: Some(vec![ConfiguredFundingStreams {
            height_range: Some(Height(1)..Height(100)),
            recipients: Some(vec![ConfiguredFundingStreamRecipient {
                receiver: FundingStreamReceiver::Deferred,
                numerator: 12,
                addresses: None,
            }]),
        }]),
        ..Default::default()
    })
}

struct Expected {
    subsidy: Amount<NonNegative>,
    lockbox: Amount<NonNegative>,
}

fn expected(network: &Network) -> Expected {
    let subsidy = block_subsidy(HEIGHT, network, Some(MAX_MONEY.try_into().expect("valid")))
        .expect("the block has a subsidy");
    let lockbox = funding_stream_values(HEIGHT, network, subsidy)
        .expect("the stream values are valid")
        .remove(&FundingStreamReceiver::Deferred)
        .expect("the network has a lockbox stream");
    Expected { subsidy, lockbox }
}

struct CoinbaseFields {
    height: Height,
    subsidy: Amount<NonNegative>,
    lockbox: Amount<NonNegative>,
    coinbase_delta: i64,
    fee_delta: i64,
}

/// A v7 coinbase transaction for a block at [`HEIGHT`] with `fees`, whose fields and deltas
/// are set by `edit`, starting from the valid ones.
fn coinbase(network: &Network, fees: i64, edit: impl FnOnce(&mut CoinbaseFields)) -> Transaction {
    let Expected { subsidy, lockbox } = expected(network);
    let mut fields = CoinbaseFields {
        height: HEIGHT,
        subsidy,
        lockbox,
        coinbase_delta: i64::from(subsidy) - i64::from(lockbox) - nsm_fee_contribution(fees),
        fee_delta: fees,
    };
    edit(&mut fields);
    v7_coinbase(&fields)
}

/// A v7 coinbase transaction with `fields`, whose transparent output pays what its deltas
/// release, so the transaction balances.
fn v7_coinbase(fields: &CoinbaseFields) -> Transaction {
    let paid = fields.coinbase_delta + fields.fee_delta;
    let amount = |zatoshis: i64| -> Amount<NegativeAllowed> { zatoshis.try_into().expect("valid") };
    Transaction::V7 {
        network_upgrade: NetworkUpgrade::Nu7_1,
        lock_time: LockTime::unlocked(),
        expiry_height: fields.height,
        inputs: Vec::new(),
        outputs: vec![transparent::Output {
            value: paid.try_into().expect("valid"),
            lock_script: transparent::Script::new(&[0x51]),
        }],
        sapling_shielded_data: None,
        orchard_shielded_data: None,
        ironwood_shielded_data: None,
        v7_fields: Box::new(V7Fields {
            value_deltas: ValueDeltas {
                transparent: amount(-paid),
                coinbase: amount(fields.coinbase_delta),
                fee: amount(fields.fee_delta),
                nsm: Amount::zero(),
            },
            coinbase: Some(
                CoinbaseBundle::new(
                    fields.height,
                    fields.subsidy,
                    fields.lockbox,
                    b"miner".to_vec(),
                    None,
                )
                .expect("the coinbase fields are in range"),
            ),
        }),
    }
}

fn miner_fees_are_valid(
    network: &Network,
    fees: i64,
    coinbase: &Transaction,
) -> Result<(), BlockError> {
    let Expected { subsidy, lockbox } = expected(network);
    check::miner_fees_are_valid(
        coinbase,
        HEIGHT,
        fees.try_into().expect("valid"),
        subsidy,
        check::LockboxFlows {
            deposit: lockbox,
            disbursement: Amount::zero(),
        },
        network,
    )
}

fn subsidy_error(error: SubsidyError) -> Result<(), BlockError> {
    Err(BlockError::Transaction(TransactionError::Subsidy(error)))
}

#[test]
fn v7_coinbase_matching_its_block_is_valid() {
    let _init_guard = zakura_test::init();
    let network = network();
    assert!(!expected(&network).lockbox.is_zero());
    assert_eq!(
        miner_fees_are_valid(&network, FEES, &coinbase(&network, FEES, |_| {})),
        Ok(())
    );
}

/// When the ZIP 235 fee contribution exceeds the subsidy less the lockbox deposit, the
/// coinbase delta is negative, and the block is still valid.
#[test]
fn v7_coinbase_delta_can_be_negative() {
    let _init_guard = zakura_test::init();
    let network = network();
    let fees = 10_000_000_000; // 100 ZEC
    let Expected { subsidy, lockbox } = expected(&network);
    assert!(nsm_fee_contribution(fees) > i64::from(subsidy) - i64::from(lockbox));

    let coinbase = coinbase(&network, fees, |_| {});
    let deltas = coinbase
        .v7_fields()
        .expect("the fixture is a v7 transaction")
        .value_deltas;
    assert!(i64::from(deltas.coinbase) < 0);
    assert_eq!(miner_fees_are_valid(&network, fees, &coinbase), Ok(()));
}

#[test]
fn v7_coinbase_fields_must_match_the_block() {
    let _init_guard = zakura_test::init();
    let network = network();

    let cases: [(fn(&mut CoinbaseFields), SubsidyError); 5] = [
        (
            |f| f.height = Height(8),
            SubsidyError::V7CoinbaseHeightMismatch,
        ),
        (
            |f| f.subsidy = (f.subsidy + Amount::<NonNegative>::try_from(1).unwrap()).unwrap(),
            SubsidyError::V7CoinbaseSubsidyMismatch,
        ),
        (
            |f| f.lockbox = (f.lockbox - Amount::<NonNegative>::try_from(1).unwrap()).unwrap(),
            SubsidyError::V7CoinbaseLockboxMismatch,
        ),
        // A miner that keeps the ZIP 235 fee contribution through its coinbase delta.
        (
            |f| f.coinbase_delta += nsm_fee_contribution(FEES),
            SubsidyError::V7CoinbaseDeltaMismatch,
        ),
        // A miner that claims more fees than the block's transactions pay.
        (|f| f.fee_delta += 1, SubsidyError::V7CoinbaseFeesMismatch),
    ];
    for (edit, error) in cases {
        assert_eq!(
            miner_fees_are_valid(&network, FEES, &coinbase(&network, FEES, edit)),
            subsidy_error(error)
        );
    }
}

/// A coinbase that moves value between its coinbase and fee deltas still pays the
/// same total, but each delta must state its own amount.
#[test]
fn v7_coinbase_deltas_are_checked_separately() {
    let _init_guard = zakura_test::init();
    let network = network();
    let shifted = coinbase(&network, FEES, |f| {
        f.coinbase_delta += 1;
        f.fee_delta -= 1;
    });
    assert_eq!(
        miner_fees_are_valid(&network, FEES, &shifted),
        subsidy_error(SubsidyError::V7CoinbaseDeltaMismatch)
    );
}

/// ZIP 248 has no bundle that pays out of the lockbox, so at a ZIP 271 disbursement
/// height only a coinbase transaction before v7 can pay the disbursement.
#[test]
fn lockbox_disbursements_need_a_coinbase_before_v7() {
    let _init_guard = zakura_test::init();
    let network = network();
    let Expected { subsidy, lockbox } = expected(&network);
    let disbursement = Amount::<NonNegative>::try_from(1_000).expect("valid");
    let flows = check::LockboxFlows {
        deposit: lockbox,
        disbursement,
    };
    let fees = Amount::<NonNegative>::try_from(FEES).expect("valid");
    let miner_share = FEES - nsm_fee_contribution(FEES);
    let check = |coinbase: &Transaction| {
        check::miner_fees_are_valid(coinbase, HEIGHT, fees, subsidy, flows, &network)
    };

    // A v6 coinbase pays the miner and the disbursement, and the lockbox funds the
    // disbursement.
    let output = |value: i64| transparent::Output {
        value: value.try_into().expect("valid"),
        lock_script: transparent::Script::new(&[0x51]),
    };
    let v6 = Transaction::V6 {
        network_upgrade: NetworkUpgrade::Nu7_1,
        lock_time: LockTime::unlocked(),
        expiry_height: HEIGHT,
        inputs: vec![transparent::Input::Coinbase {
            height: HEIGHT,
            data: Vec::new(),
            sequence: u32::MAX,
        }],
        outputs: vec![
            output(i64::from(subsidy) - i64::from(lockbox) + miner_share),
            output(i64::from(disbursement)),
        ],
        sapling_shielded_data: None,
        orchard_shielded_data: None,
        ironwood_shielded_data: None,
    };
    assert_eq!(check(&v6), Ok(()));

    // A v7 coinbase's deltas balance only without the disbursement output, and then the
    // total output falls short of the disbursement.
    assert_eq!(
        check(&coinbase(&network, FEES, |_| {})),
        subsidy_error(SubsidyError::InvalidMinerFees)
    );
}

/// A v7 coinbase transaction that spends a transparent output fails with the coinbase
/// rule, before the block looks the output up.
#[test]
fn v7_coinbase_with_a_transparent_input_fails_the_coinbase_check() {
    let _init_guard = zakura_test::init();
    let network = network();
    let mut coinbase = coinbase(&network, FEES, |_| {});
    let Transaction::V7 { inputs, .. } = &mut coinbase else {
        unreachable!("the fixture is a v7 transaction");
    };
    inputs.push(transparent::Input::PrevOut {
        outpoint: transparent::OutPoint {
            hash: zakura_chain::transaction::Hash([1; 32]),
            index: 0,
        },
        unlock_script: transparent::Script::new(&[]),
        sequence: u32::MAX,
    });
    let mut block =
        Block::zcash_deserialize(&zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES[..])
            .expect("the genesis block deserializes");
    block.transactions = vec![Arc::new(coinbase)];

    assert_eq!(
        check::coinbase_is_first(&block).map(|_| ()),
        Err(BlockError::Transaction(
            TransactionError::CoinbaseHasTransparentInput
        ))
    );
}

/// The height of the mixed-version block, at which NU7 and NU7.1 activate.
const MIXED_HEIGHT: Height = Height(2);

/// A Regtest network that activates NU6.3 at height 1 and NU7 and NU7.1 at
/// [`MIXED_HEIGHT`], without funding streams, and with ZIP 234 reissuance far away.
fn mixed_network() -> Network {
    Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu6_3: Some(1),
            nu7: Some(MIXED_HEIGHT.0),
            nu7_1: Some(MIXED_HEIGHT.0),
            ..Default::default()
        },
        test_nsm_reissuance_height: Some(Height(1_000)),
        ..Default::default()
    })
}

/// A P2SH output whose redeem script is `OP_TRUE`, so that spends need no signature.
fn anyone_can_spend(value: i64) -> transparent::Output {
    let mut lock_script = vec![0xa9, 0x14];
    lock_script.extend_from_slice(&[
        0xda, 0x17, 0x45, 0xe9, 0xb5, 0x49, 0xbd, 0x0b, 0xfa, 0x1a, 0x56, 0x99, 0x71, 0xc7, 0x7e,
        0xba, 0x30, 0xcd, 0x5a, 0x4b,
    ]);
    lock_script.push(0x87);
    transparent::Output {
        value: value.try_into().expect("valid"),
        lock_script: transparent::Script::new(&lock_script),
    }
}

fn spend(index: u32) -> transparent::Input {
    transparent::Input::PrevOut {
        outpoint: parent_outpoint(index),
        // Pushes the redeem script `OP_TRUE`.
        unlock_script: transparent::Script::new(&[0x01, 0x51]),
        sequence: u32::MAX,
    }
}

fn parent_outpoint(index: u32) -> transparent::OutPoint {
    transparent::OutPoint {
        hash: zakura_chain::transaction::Hash([7; 32]),
        index,
    }
}

/// Each transaction spends one parent output of 10_000 zatoshis: the v5 transaction
/// leaves a fee of 1_000 and the v6 transaction 2_000, implicitly, and the v7 transaction
/// states 3_000.
fn mixed_block(network: &Network, coinbase_fee_delta: i64) -> Block {
    let fees = 6_000;
    let subsidy = block_subsidy(MIXED_HEIGHT, network, None).expect("the block has a subsidy");
    let coinbase = v7_coinbase(&CoinbaseFields {
        height: MIXED_HEIGHT,
        subsidy,
        lockbox: Amount::zero(),
        coinbase_delta: i64::from(subsidy) - nsm_fee_contribution(fees),
        fee_delta: coinbase_fee_delta,
    });
    let v5 = Transaction::V5 {
        network_upgrade: NetworkUpgrade::Nu7_1,
        lock_time: LockTime::unlocked(),
        expiry_height: Height(0),
        inputs: vec![spend(0)],
        outputs: vec![anyone_can_spend(9_000)],
        sapling_shielded_data: None,
        orchard_shielded_data: None,
    };
    let v6 = Transaction::V6 {
        network_upgrade: NetworkUpgrade::Nu7_1,
        lock_time: LockTime::unlocked(),
        expiry_height: Height(0),
        inputs: vec![spend(1)],
        outputs: vec![anyone_can_spend(8_000)],
        sapling_shielded_data: None,
        orchard_shielded_data: None,
        ironwood_shielded_data: None,
    };
    let v7 = Transaction::V7 {
        network_upgrade: NetworkUpgrade::Nu7_1,
        lock_time: LockTime::unlocked(),
        expiry_height: Height(0),
        inputs: vec![spend(2)],
        outputs: vec![anyone_can_spend(7_000)],
        sapling_shielded_data: None,
        orchard_shielded_data: None,
        ironwood_shielded_data: None,
        v7_fields: Box::new(V7Fields {
            value_deltas: ValueDeltas {
                transparent: 3_000.try_into().expect("valid"),
                fee: (-3_000).try_into().expect("valid"),
                ..ValueDeltas::default()
            },
            coinbase: None,
        }),
    };

    let mut block =
        Block::zcash_deserialize(&zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES[..])
            .expect("the genesis block deserializes");
    block.transactions = [coinbase, v5, v6, v7].map(Arc::new).to_vec();
    Arc::make_mut(&mut block.header).merkle_root = block.transactions.iter().collect();
    block
}

/// Checks a block proposal with the real transaction verifier, against a state whose
/// parent has the outputs that [`mixed_block`] spends.
async fn check_proposal(network: &Network, block: Block) -> Result<block::Hash, VerifyBlockError> {
    let parent_utxos: Arc<HashMap<_, _>> = Arc::new(
        (0..3)
            .map(|index| {
                (
                    parent_outpoint(index),
                    transparent::Utxo::new(anyone_can_spend(10_000), Height(1), false),
                )
            })
            .collect(),
    );
    let state = service_fn(move |request: zs::Request| {
        let parent_utxos = parent_utxos.clone();
        async move {
            Ok::<_, BoxError>(match request {
                // Only the parent, the genesis block's null hash, is known.
                zs::Request::KnownBlock(hash) => zs::Response::KnownBlock(
                    (hash == block::Hash([0; 32])).then_some(zs::KnownBlock::Finalized),
                ),
                zs::Request::CheckParentInputs { .. } => {
                    zs::Response::ParentInputs(zs::ParentInputs::Unspent(parent_utxos))
                }
                zs::Request::CheckBlockProposalValidity(_) => zs::Response::ValidBlockProposal,
                _ => panic!("unexpected state request: {request:?}"),
            })
        }
    });
    let transactions = crate::transaction::Verifier::new_for_tests(
        network,
        service_fn(|_| async { unreachable!("block transactions do not query the state") }),
    );
    SemanticBlockVerifier::new(
        network,
        crate::transaction::boxed_state(state),
        crate::transaction::boxed_transaction_verifier(Buffer::new(transactions, 4)),
    )
    .oneshot(Request::CheckProposal(Arc::new(block).into()))
    .await
}

/// A v7 coinbase transaction collects the fees of v5 and v6 transactions, which leave
/// theirs implicitly, and of v7 transactions, which state theirs.
#[tokio::test]
async fn v7_coinbase_collects_the_fees_of_every_transaction_version() {
    let _init_guard = zakura_test::init();
    let network = mixed_network();

    let block = mixed_block(&network, 6_000);
    assert_eq!(
        check_proposal(&network, block.clone())
            .await
            .map_err(|error| error.to_string()),
        Ok(block.hash())
    );

    let result = check_proposal(&network, mixed_block(&network, 6_001)).await;
    assert!(
        matches!(
            result,
            Err(VerifyBlockError::Block {
                source: BlockError::Transaction(TransactionError::Subsidy(
                    SubsidyError::V7CoinbaseFeesMismatch
                )),
            })
        ),
        "{result:?}"
    );
}

/// A v7 transaction decodes a null prevout as a spend. The block's quick checks reject
/// it, and they run before the block looks up its parent's outputs, which cannot contain
/// the null previous output.
#[tokio::test]
async fn v7_null_prevout_fails_the_block_quick_checks() {
    let _init_guard = zakura_test::init();
    let network = mixed_network();
    let mut block = mixed_block(&network, 6_000);
    let v7 = Arc::make_mut(
        block
            .transactions
            .last_mut()
            .expect("the mixed block ends with its v7 transaction"),
    );
    let Transaction::V7 { inputs, .. } = v7 else {
        unreachable!("the mixed block ends with its v7 transaction");
    };
    let transparent::Input::PrevOut { outpoint, .. } = &mut inputs[0] else {
        unreachable!("the v7 transaction spends a parent output");
    };
    *outpoint = transparent::OutPoint::NULL;
    Arc::make_mut(&mut block.header).merkle_root = block.transactions.iter().collect();

    assert_eq!(
        check::coinbase_is_first(&block).map(|_| ()),
        Err(BlockError::Transaction(
            TransactionError::NonCoinbaseHasNullPrevout
        ))
    );
    let result = check_proposal(&network, block).await;
    assert!(
        matches!(
            result,
            Err(VerifyBlockError::Block {
                source: BlockError::Transaction(TransactionError::NonCoinbaseHasNullPrevout),
            })
        ),
        "{result:?}"
    );
}
