//! Opt-in acceptance gate for a separately built production artifact.
//! No transaction-verifier or state-service mocks are used here.

use super::{
    config::{os_assigned_rpc_port_config, read_listen_addr_from_logs, testdir},
    launch::{ZakuradTestDirExt, EXTENDED_LAUNCH_DELAY},
};
use color_eyre::eyre::{eyre, Result};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tempfile::TempDir;
use zakura_chain::{
    amount::Amount,
    block::{Block, ChainHistoryBlockTxAuthCommitmentHash, Height},
    parameters::{
        subsidy::halving_block_subsidy,
        testnet::{ConfiguredActivationHeights, RegtestParameters},
        Network, NetworkUpgrade,
    },
    serialization::{BytesInDisplayOrder, ZcashSerialize},
    transaction::{HashType, LockTime, SigHasher, Transaction},
    transparent::{Input, OutPoint, Output, Script},
};
use zakura_node_services::rpc_client::RpcRequestClient;
use zakura_rpc::{
    client::{BlockTemplateResponse, BlockTemplateTimeSource},
    proposal_block_from_template,
    server::OPENED_RPC_ENDPOINT_MSG,
};
use zakura_test::{
    args,
    command::{TestChild, TestDirExt},
};

const NU7: u32 = 104;

struct Node {
    child: TestChild<TempDir>,
    rpc: RpcRequestClient,
    p2p: std::net::SocketAddr,
    backup: std::path::PathBuf,
}
impl Node {
    fn start(dir: TempDir) -> Result<Self> {
        let binary = std::env::var("NSM_RELEASE_NODE")?;
        let config = dir.path().join("zakura.toml");
        let settings: zakurad::config::ZakuradConfig =
            toml::from_str(&std::fs::read_to_string(&config)?)?;
        let p2p = settings.network.listen_addr;
        let backup = settings
            .state
            .non_finalized_state_backup_dir(&settings.network.network)
            .expect("persistent fixture enables backups");
        let mut child = dir
            .spawn_child_with_command(
                &binary,
                args!["-c": config.to_str().expect("test path is Unicode"), "start"],
            )?
            .with_timeout(EXTENDED_LAUNCH_DELAY);
        let addr = read_listen_addr_from_logs(&mut child, OPENED_RPC_ENDPOINT_MSG)?;
        Ok(Self {
            child,
            rpc: RpcRequestClient::new(addr),
            p2p,
            backup,
        })
    }
    async fn call(&self, method: &str, params: Value) -> Result<Value> {
        tokio::time::timeout(
            Duration::from_secs(30),
            self.rpc.json_result_from_call(method, params.to_string()),
        )
        .await
        .map_err(|_| eyre!("{method} timed out"))?
        .map_err(|error| eyre!("{method}: {error}"))
    }
    async fn wait_ready(&self) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(30), async {
            while self.call("getbestblockhash", json!([])).await.is_err() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .map_err(|_| eyre!("node did not initialize its chain tip"))?;
        Ok(())
    }
    async fn wait_backup(&self) -> Result<()> {
        let hash = self.call("getbestblockhash", json!([])).await?;
        let raw = self.call("getblock", json!([hash, 0])).await?;
        let expected = u64::try_from(
            raw.as_str()
                .ok_or_else(|| eyre!("block hex missing"))?
                .len()
                / 2,
        )?;
        let path = self
            .backup
            .join(hash.as_str().ok_or_else(|| eyre!("block hash missing"))?);
        tokio::time::timeout(Duration::from_secs(30), async {
            while !std::fs::metadata(&path).is_ok_and(|metadata| metadata.len() >= expected) {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .map_err(|_| eyre!("non-finalized backup did not persist the tip"))?;
        Ok(())
    }
    fn stop(mut self) -> Result<TempDir> {
        let dir = self
            .child
            .dir
            .take()
            .expect("the running node owns its directory");
        self.child.kill(false)?;
        self.child
            .wait_with_output()?
            .assert_failure()?
            .assert_was_killed()?;
        Ok(dir)
    }
}

fn roots(block: &mut Block, template: &BlockTemplateResponse, network: &Network) {
    let auth = block.auth_data_root();
    let merkle = block.transactions.iter().collect();
    let header = Arc::make_mut(&mut block.header);
    header.merkle_root = merkle;
    if NetworkUpgrade::current(network, Height(template.height())) >= NetworkUpgrade::Nu5 {
        header.commitment_bytes = ChainHistoryBlockTxAuthCommitmentHash::from_commitments(
            &template.default_roots().chain_history_root(),
            &auth,
        )
        .bytes_in_serialized_order()
        .into();
    }
}

fn encode(block: &Block) -> Result<String> {
    Ok(hex::encode(block.zcash_serialize_to_vec()?))
}

fn authorize(tx: &mut Arc<Transaction>, upgrade: NetworkUpgrade, previous: Output) -> Result<()> {
    let script = previous.lock_script.as_raw_bytes().to_vec();
    let digest: [u8; 32] = SigHasher::new(tx, upgrade, Arc::new(vec![previous]))?
        .sighash(HashType::ALL, Some((0, script)))
        .into();
    // Public test key, matching the fixture's coinbases.
    let key = secp256k1::SecretKey::from_slice(&[1; 32])?;
    let signature = secp256k1::Secp256k1::new()
        .sign_ecdsa(&secp256k1::Message::from_digest(digest), &key)
        .serialize_der();
    let mut unlock = vec![u8::try_from(signature.len() + 1)?];
    unlock.extend_from_slice(&signature);
    unlock.push(1);
    let Input::PrevOut { unlock_script, .. } = &mut Arc::make_mut(tx).inputs_mut()[0] else {
        unreachable!()
    };
    *unlock_script = Script::new(&unlock);
    Ok(())
}

async fn wait_mempool(node: &Node, txid: &str, present: bool) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let entries = node.call("getrawmempool", json!([])).await?;
            if entries
                .as_array()
                .ok_or_else(|| eyre!("mempool response is not an array"))?
                .iter()
                .any(|entry| entry.as_str() == Some(txid))
                == present
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .map_err(|_| eyre!("mempool failed to revalidate across activation"))?
}

/// Cross NU7 with actual spends, validate proposals, reject malformed
/// rewards, independently submit to a second node, then restart and replay.
pub async fn run() -> Result<()> {
    if std::env::var("NSM_RELEASE_ACCEPTANCE").as_deref() != Ok("1") {
        return Ok(());
    }
    if std::env::var_os("NSM_RELEASE_NODE").is_none() {
        return Err(eyre!("release gate requires NSM_RELEASE_NODE pointing to a separately built production binary"));
    }
    let _guard = zakura_test::init();
    let network = Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu5: Some(2),
            nu7: Some(NU7),
            ..Default::default()
        },
        // This schedule co-activates NU6.1 at NU7. Its lockbox is empty, but
        // consensus still requires an explicitly configured disbursement output.
        lockbox_disbursements: Some(vec![
            zakura_chain::parameters::testnet::ConfiguredLockboxDisbursement {
                address: "t2RnBRiqrN1nW4ecZs1Fj3WWjNdnSs4kiX8".to_string(),
                amount: Amount::new(0),
            },
        ]),
        ..Default::default()
    });
    let reissuance = zakura_chain::parameters::subsidy::nsm_reissuance_height(&network);
    assert!(
        reissuance.is_none_or(|height| height.0 > 110),
        "short production fixture must not override the reference crossover"
    );
    let make_node = || -> Result<Node> {
        let mut config = os_assigned_rpc_port_config(false, &network)?;
        config.network.listen_addr =
            std::net::SocketAddr::from(([127, 0, 0, 1], zakura_test::net::random_known_port()));
        config.state.ephemeral = false;
        config.mempool.debug_enable_at_height = Some(0);
        Node::start(testdir()?.with_config(&mut config)?)
    };
    let node = make_node()?;
    let peer = make_node()?;
    node.wait_ready().await?;
    peer.wait_ready().await?;
    // Public, deterministic test key; it never holds funds outside this private chain.
    let signing = secp256k1::Secp256k1::new();
    let key = secp256k1::SecretKey::from_slice(&[1; 32])?;
    let public = secp256k1::PublicKey::from_secret_key(&signing, &key).serialize();
    let mut lock_bytes = vec![33];
    lock_bytes.extend_from_slice(&public);
    lock_bytes.push(0xac); // OP_CHECKSIG
    let lock_script = Script::new(&lock_bytes);
    let mut funding: Vec<(OutPoint, i64)> = Vec::new();
    let mut pending = None;
    let mut blocks = Vec::new();
    for h in 1..=110 {
        if h == NU7 {
            let tx: &Arc<Transaction> = pending.as_ref().expect("queued before activation");
            wait_mempool(&node, &tx.hash().to_string(), false).await?;
        }
        let raw_template = node.call("getblocktemplate", json!([])).await?;
        let template: BlockTemplateResponse = serde_json::from_value(raw_template)?;
        assert_eq!(template.height(), h);
        let mut block =
            proposal_block_from_template(&template, BlockTemplateTimeSource::default(), &network)?;
        let scheduled = i64::from(halving_block_subsidy(Height(h), &network)?);
        // Large fees exercise recycling; small fees detect rounding per tx.
        let fees: [i64; 2] = if h < 103 {
            [0, 0]
        } else if h == 105 {
            [7_000_000, 7_000_000]
        } else {
            [10_001, 10_001]
        };
        let total = fees.iter().sum::<i64>();
        let recycled = if h >= NU7 { total * 3 / 5 } else { 0 };
        let expected = scheduled + total - recycled;
        let coinbase = Arc::make_mut(&mut block.transactions[0]);
        let outputs = coinbase.outputs_mut();
        assert_eq!(outputs.len(), if h == NU7 { 2 } else { 1 });
        let miner_index = outputs
            .iter()
            .position(|output| i64::from(output.value) > 0)
            .expect("the miner receives a positive subsidy");
        assert_eq!(
            i64::from(outputs[miner_index].value),
            scheduled,
            "empty template reward at {h}"
        );
        outputs[miner_index] = Output::new(Amount::try_from(expected)?, lock_script.clone());
        if h <= 3 {
            funding.push((
                OutPoint {
                    hash: coinbase.hash(),
                    index: 0,
                },
                expected,
            ));
        }
        if h >= 103 {
            for (index, fee) in fees.into_iter().enumerate() {
                let (input, amount) = funding[index];
                let mut tx = Arc::new(Transaction::V5 {
                    network_upgrade: NetworkUpgrade::current(&network, Height(h)),
                    lock_time: LockTime::unlocked(),
                    expiry_height: Height(h),
                    inputs: vec![Input::PrevOut {
                        outpoint: input,
                        unlock_script: Script::new(&[]),
                        sequence: u32::MAX,
                    }],
                    outputs: vec![Output::new(
                        Amount::try_from(amount - fee)?,
                        lock_script.clone(),
                    )],
                    sapling_shielded_data: None,
                    orchard_shielded_data: None,
                });
                authorize(
                    &mut tx,
                    NetworkUpgrade::current(&network, Height(h)),
                    Output::new(Amount::try_from(amount)?, lock_script.clone()),
                )?;
                funding[index] = (
                    OutPoint {
                        hash: tx.hash(),
                        index: 0,
                    },
                    amount - fee,
                );
                block.transactions.push(tx);
            }
        }
        roots(&mut block, &template, &network);
        if h == 103 {
            // Queue a still-valid V4 transaction AFTER taking this template, so
            // block 103 leaves it unmined as the next-block rules change to NU7.
            let (input, amount) = funding[2];
            let mut tx = Arc::new(Transaction::V4 {
                inputs: vec![Input::PrevOut {
                    outpoint: input,
                    unlock_script: Script::new(&[]),
                    sequence: u32::MAX,
                }],
                outputs: vec![Output::new(
                    Amount::try_from(amount - 10_000)?,
                    lock_script.clone(),
                )],
                lock_time: LockTime::unlocked(),
                expiry_height: Height(0),
                joinsplit_data: None,
                sapling_shielded_data: None,
            });
            authorize(
                &mut tx,
                NetworkUpgrade::Nu5,
                Output::new(Amount::try_from(amount)?, lock_script.clone()),
            )?;
            node.call(
                "sendrawtransaction",
                json!([hex::encode(tx.zcash_serialize_to_vec()?)]),
            )
            .await?;
            wait_mempool(&node, &tx.hash().to_string(), true).await?;
            pending = Some(tx);
        }
        if h >= NU7 {
            for adjustment in [-1i64, 1] {
                let mut invalid = block.clone();
                Arc::make_mut(&mut invalid.transactions[0]).outputs_mut()[miner_index].value =
                    Amount::try_from(expected + adjustment)?;
                roots(&mut invalid, &template, &network);
                let response = node
                    .call(
                        "getblocktemplate",
                        json!([{"mode":"proposal", "data":encode(&invalid)?}]),
                    )
                    .await?;
                assert!(
                    !response.is_null(),
                    "invalid payout proposal accepted at {h}"
                );
                let response = node.call("submitblock", json!([encode(&invalid)?])).await?;
                assert!(!response.is_null(), "invalid payout committed at {h}");
                assert_eq!(node.call("getblockcount", json!([])).await?, json!(h - 1));
            }
        }
        if h >= 103 {
            for wrong_branch in [false, true] {
                let mut invalid = block.clone();
                let tx = Arc::make_mut(&mut invalid.transactions[1]);
                if wrong_branch {
                    let Transaction::V5 {
                        network_upgrade, ..
                    } = tx
                    else {
                        unreachable!()
                    };
                    *network_upgrade = NetworkUpgrade::Nu5;
                    // NU5 is the pre-activation context here, so this mutation is
                    // only invalid once the upgrade has occurred.
                    if h < NU7 {
                        continue;
                    }
                } else {
                    let Input::PrevOut { unlock_script, .. } = &mut tx.inputs_mut()[0] else {
                        unreachable!()
                    };
                    let mut bytes = unlock_script.as_raw_bytes().to_vec();
                    let last_signature_byte = bytes.len() - 2;
                    bytes[last_signature_byte] ^= 1;
                    *unlock_script = Script::new(&bytes);
                }
                roots(&mut invalid, &template, &network);
                assert!(
                    !node
                        .call("submitblock", json!([encode(&invalid)?]))
                        .await?
                        .is_null(),
                    "invalid signature/branch accepted at {h}"
                );
                assert_eq!(node.call("getblockcount", json!([])).await?, json!(h - 1));
            }
        }
        let data = encode(&block)?;
        assert!(
            node.call(
                "getblocktemplate",
                json!([{"mode":"proposal", "data":data}])
            )
            .await?
            .is_null(),
            "valid proposal at {h}"
        );
        assert!(
            node.call("submitblock", json!([data])).await?.is_null(),
            "valid submission at {h}"
        );
        assert!(
            peer.call("submitblock", json!([encode(&block)?]))
                .await?
                .is_null(),
            "independent verification at {h}"
        );
        assert_eq!(
            node.call("getbestblockhash", json!([])).await?,
            peer.call("getbestblockhash", json!([])).await?
        );
        blocks.push(block);
    }
    let before = node.call("getblockchaininfo", json!([])).await?;
    node.wait_backup().await?;
    let node = Node::start(node.stop()?)?;
    node.wait_ready().await?;
    assert_eq!(
        node.call("getbestblockhash", json!([])).await?,
        before["bestblockhash"]
    );
    // Roll the active chain back across NU7 using RPC invalidation.
    let root = blocks[usize::try_from(NU7 - 2)?].hash();
    let old_template = node.call("getblocktemplate", json!([])).await?;
    let long_poll_id = old_template["longpollid"]
        .as_str()
        .ok_or_else(|| eyre!("missing long-poll ID"))?
        .to_owned();
    let rpc = node.rpc.clone();
    let pending_template = tokio::spawn(async move {
        rpc.json_result_from_call::<Value>(
            "getblocktemplate",
            json!([{"longpollid":long_poll_id}]).to_string(),
        )
        .await
    });
    tokio::task::yield_now().await;
    node.call("invalidateblock", json!([root.to_string()]))
        .await?;
    let refreshed = tokio::time::timeout(Duration::from_secs(30), pending_template)
        .await??
        .map_err(|error| eyre!("long-poll refresh: {error}"))?;
    assert_eq!(
        refreshed["previousblockhash"],
        json!(blocks[usize::try_from(NU7 - 3)?].hash().to_string())
    );
    assert_eq!(node.call("getblockcount", json!([])).await?, json!(NU7 - 2));
    let pending = pending.expect("a pre-activation transaction was queued");
    node.call(
        "sendrawtransaction",
        json!([hex::encode(pending.zcash_serialize_to_vec()?)]),
    )
    .await?;
    wait_mempool(&node, &pending.hash().to_string(), true).await?;
    node.call("reconsiderblock", json!([root.to_string()]))
        .await?;
    assert_eq!(
        node.call("getbestblockhash", json!([])).await?,
        before["bestblockhash"]
    );
    wait_mempool(&node, &pending.hash().to_string(), false).await?;
    assert_eq!(
        node.call("getblocksubsidy", json!([111])).await?,
        peer.call("getblocksubsidy", json!([111])).await?
    );
    // Partitioned nodes now mine competing histories; reconnect only after the
    // new branch is longer, so convergence requires real P2P fork synchronization.
    node.call("invalidateblock", json!([root.to_string()]))
        .await?;
    node.call("generate", json!([9])).await?;
    assert_eq!(node.call("getblockcount", json!([])).await?, json!(111));
    let winning_tip = node.call("getbestblockhash", json!([])).await?;
    assert_ne!(winning_tip, peer.call("getbestblockhash", json!([])).await?);
    peer.call("addnode", json!([node.p2p.to_string(), "add"]))
        .await?;
    tokio::time::timeout(Duration::from_secs(60), async {
        while peer.call("getbestblockhash", json!([])).await? != winning_tip {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok::<(), color_eyre::Report>(())
    })
    .await
    .map_err(|_| eyre!("nodes did not converge after reconnecting competing NU7 histories"))??;
    assert_eq!(
        node.call("getblocksubsidy", json!([112])).await?,
        peer.call("getblocksubsidy", json!([112])).await?
    );
    node.stop()?;
    peer.stop()?;
    Ok(())
}

/// Exercise real shielded coinbase proofs on both sides of NU7. These are output
/// proofs; contextual shielded-spend and reissuance coverage lives in zakura-state.
pub async fn run_shielded(address: zakura_rpc::config::mining::MinerAddressType) -> Result<()> {
    use zakurad::components::With;

    if std::env::var("NSM_RELEASE_ACCEPTANCE").as_deref() != Ok("1") {
        return Ok(());
    }
    let _guard = zakura_test::init();
    let network = Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu6_3: Some(1),
            nu7: Some(4),
            ..Default::default()
        },
        funding_streams: Some(vec![
            zakura_chain::parameters::testnet::ConfiguredFundingStreams {
                height_range: Some(Height(1)..Height(100)),
                recipients: Some(vec![
                    zakura_chain::parameters::testnet::ConfiguredFundingStreamRecipient {
                        receiver:
                            zakura_chain::parameters::subsidy::FundingStreamReceiver::Deferred,
                        numerator: 1,
                        addresses: None,
                    },
                ]),
            },
        ]),
        lockbox_disbursements: Some(vec![
            zakura_chain::parameters::testnet::ConfiguredLockboxDisbursement {
                address: "t2RnBRiqrN1nW4ecZs1Fj3WWjNdnSs4kiX8".to_string(),
                amount: Amount::new(6_250_000),
            },
        ]),
        ..Default::default()
    });
    let expect_ironwood = address == zakura_rpc::config::mining::MinerAddressType::Unified;
    let mut config = os_assigned_rpc_port_config(false, &network)?.with(address);
    config.state.ephemeral = false;
    config.mempool.debug_enable_at_height = Some(0);
    let node = Node::start(testdir()?.with_config(&mut config)?)?;
    node.wait_ready().await?;
    for height in 1..=8 {
        let template: BlockTemplateResponse =
            serde_json::from_value(node.call("getblocktemplate", json!([])).await?)?;
        assert_eq!(template.height(), height);
        let block =
            proposal_block_from_template(&template, BlockTemplateTimeSource::default(), &network)?;
        assert_eq!(
            block.transactions[0].ironwood_shielded_data().is_some(),
            expect_ironwood,
            "unified mining address must route to Ironwood after NU6.3"
        );
        let mut invalid = block.clone();
        let coinbase = Arc::make_mut(&mut invalid.transactions[0]);
        match coinbase {
            Transaction::V5 {
                sapling_shielded_data: Some(data),
                ..
            }
            | Transaction::V6 {
                sapling_shielded_data: Some(data),
                ..
            } => {
                let zakura_chain::sapling::TransferData::JustOutputs { outputs } =
                    &mut data.transfers
                else {
                    return Err(eyre!("coinbase unexpectedly contains Sapling spends"));
                };
                let mut changed = outputs.as_vec().clone();
                changed[0].zkproof.0[0] ^= 1;
                *outputs = changed.try_into().expect("coinbase contains an output");
            }
            Transaction::V6 {
                ironwood_shielded_data: Some(data),
                ..
            } => data.proof.0[0] ^= 1,
            Transaction::V5 {
                orchard_shielded_data: Some(data),
                ..
            }
            | Transaction::V6 {
                orchard_shielded_data: Some(data),
                ..
            } => data.proof.0[0] ^= 1,
            _ => return Err(eyre!("shielded mining address produced no shielded output")),
        }
        assert_eq!(
            invalid.transactions[0].hash(),
            block.transactions[0].hash(),
            "proof mutation must preserve the transaction effect digest"
        );
        roots(&mut invalid, &template, &network);
        let before = node.call("getbestblockhash", json!([])).await?;
        assert!(
            !node
                .call(
                    "getblocktemplate",
                    json!([{"mode":"proposal", "data":encode(&invalid)?}])
                )
                .await?
                .is_null(),
            "corrupt shielded proof proposal accepted at {height}"
        );
        assert!(
            !node
                .call("submitblock", json!([encode(&invalid)?]))
                .await?
                .is_null(),
            "corrupt shielded proof block accepted at {height}"
        );
        assert_eq!(before, node.call("getbestblockhash", json!([])).await?);
        let proposal = node
            .call(
                "getblocktemplate",
                json!([{"mode":"proposal", "data":encode(&block)?}]),
            )
            .await?;
        assert!(
            proposal.is_null(),
            "valid shielded proposal rejected at {height}: {proposal}"
        );
        assert!(
            node.call("submitblock", json!([encode(&block)?]))
                .await?
                .is_null(),
            "valid shielded coinbase rejected at {height}"
        );
    }
    node.stop()?;
    Ok(())
}
