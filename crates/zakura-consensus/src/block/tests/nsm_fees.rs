//! Coinbase fee allocation at NU7 and the later reissuance boundary.

use super::*;

#[test]
fn nsm_fee_claims_at_activation_and_reissuance() {
    use zakura_chain::parameters::testnet::RegtestParameters;

    let network = Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu6_3: Some(1),
            nu7: Some(5),
            ..Default::default()
        },
        test_nsm_reissuance_height: Some(Height(10)),
        ..Default::default()
    });
    for height in [Height(4), Height(5), Height(9), Height(10)] {
        let subsidy =
            block_subsidy(height, &network, Some(Amount::try_from(1_000_000).unwrap())).unwrap();
        for (fees, miner_share) in [(0, 0), (1, 1), (2, 1), (1_000, 400), (1_001, 401)] {
            let miner_share = if height >= Height(5) {
                miner_share
            } else {
                fees
            };
            for adjustment in [-1, 0, 1] {
                let mut coinbase = v5_coinbase_transaction(
                    NetworkUpgrade::current(&network, height),
                    height,
                    &network,
                );
                let Transaction::V5 { outputs, .. } = &mut coinbase else {
                    unreachable!()
                };
                outputs[0].value =
                    Amount::try_from(i64::from(subsidy) + miner_share + adjustment).unwrap();
                let result = check::miner_fees_are_valid(
                    &coinbase,
                    height,
                    Amount::try_from(fees).unwrap(),
                    subsidy,
                    DeferredPoolBalanceChange::new(Amount::zero()),
                    &network,
                );
                assert_eq!(
                    result.is_ok(),
                    adjustment == 0,
                    "height={height:?}, fees={fees}"
                );
            }
        }
    }
}

#[tokio::test]
async fn nsm_fee_semantic_verification_rounds_the_block_total() {
    use zakura_chain::{
        block_info::BlockInfo,
        transparent::{Input, OutPoint, Script},
    };

    let _init_guard = zakura_test::init();
    let network = zip234_test_network(Height(4));
    // Two one-zatoshi fees must contribute one zatoshi from activation onward.
    for height in [Height(1), Height(2), Height(4)] {
        let subsidy = block_subsidy(
            height,
            &network,
            Some(Amount::try_from(ZIP234_TEST_DEFICIT).unwrap()),
        )
        .unwrap();
        for miner_share in [0, 1, 2] {
            let mut block = zip234_test_block(
                &network,
                height,
                (subsidy + Amount::try_from(miner_share).unwrap()).unwrap(),
            );
            for index in [1u8, 2] {
                let mut transaction =
                    v5_coinbase_transaction(NetworkUpgrade::Nu7, height, &network);
                *transaction.inputs_mut() = vec![Input::PrevOut {
                    outpoint: OutPoint {
                        hash: zakura_chain::transaction::Hash([index; 32]),
                        index: 0,
                    },
                    unlock_script: Script::new(&[]),
                    sequence: u32::MAX,
                }];
                block.transactions.push(Arc::new(transaction));
            }
            Arc::make_mut(&mut block.header).merkle_root = block.transactions.iter().collect();
            let mut parent_pools = zakura_chain::value_balance::ValueBalance::zero();
            parent_pools
                .set_nsm_value_balance_amount(Amount::try_from(ZIP234_TEST_DEFICIT).unwrap());
            let state = service_fn(move |request: zs::Request| async move {
                Ok::<_, BoxError>(match request {
                    zs::Request::KnownBlock(_) => zs::Response::KnownBlock(None),
                    zs::Request::CheckParentInputs { .. } => {
                        zs::Response::ParentInputs(zs::ParentInputs::Inconclusive)
                    }
                    zs::Request::AwaitBlockInfo(_) => {
                        zs::Response::BlockInfo(Some(BlockInfo::new(parent_pools, 0)))
                    }
                    zs::Request::CommitSemanticallyVerifiedBlock(block) => {
                        zs::Response::Committed(block.hash)
                    }
                    _ => panic!("unexpected state request: {request:?}"),
                })
            });
            // NU7 has no production branch ID yet. Mock only transaction verification;
            // the real block verifier must aggregate the returned fees before splitting.
            let transaction = service_fn(|request| async move {
                let mut response = accept_block_transaction(request);
                let tx::Response::Block {
                    miner_fee: Some(fee),
                    ..
                } = &mut response
                else {
                    return Ok::<_, BoxError>(response);
                };
                *fee = Amount::try_from(1).unwrap();
                Ok(response)
            });
            let verifier = SemanticBlockVerifier::new(&network, state, transaction);
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                verifier.oneshot(Request::Commit(Arc::new(block))),
            )
            .await
            .unwrap();
            if miner_share == 1 {
                result.expect("the miner must receive exactly one of the two fee zatoshi");
            } else {
                assert!(
                    matches!(
                        result,
                        Err(VerifyBlockError::Block {
                            source: BlockError::Transaction(TransactionError::Subsidy(
                                SubsidyError::InvalidMinerFees
                            )),
                        })
                    ),
                    "{result:?}"
                );
            }
        }
    }
}
