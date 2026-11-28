//! End-to-end Regtest tests of [ZIP 248] v7 transactions through `zakurad`'s RPC entry
//! points: `getblocktemplate` (template and proposal modes), `submitblock`,
//! `sendrawtransaction`, and the verbose transaction and UTXO RPCs.
//!
//! [ZIP 248]: https://zips.z.cash/zip-0248

use std::{
    collections::BTreeMap,
    net::{SocketAddr, UdpSocket},
    sync::Arc,
    time::Duration,
};

use eyre::{eyre, Result};
use serde_json::Value;
use tempfile::TempDir;
use zcash_primitives::transaction::TxVersion;

use zakura_chain::{
    amount::{Amount, NegativeAllowed, NonNegative},
    block::{merkle, Block, ChainHistoryBlockTxAuthCommitmentHash, Height},
    parameters::{
        subsidy::FundingStreamReceiver,
        testnet::{
            ConfiguredActivationHeights, ConfiguredFundingStreamRecipient,
            ConfiguredFundingStreams, ConfiguredLockboxDisbursement, RegtestParameters,
        },
        Network, NetworkUpgrade,
    },
    serialization::{BytesInDisplayOrder, ZcashSerialize},
    transaction::{
        self,
        v7::{CoinbaseBundle, ValueDeltas},
        Transaction,
    },
    transparent::{OutPoint, Output},
};
use zakura_network::P2pStack;
use zakura_node_services::rpc_client::RpcRequestClient;
use zakura_quic::NodeSecretKey;
use zakura_rpc::{
    client::{
        BlockProposalResponse, BlockTemplateResponse, BlockTemplateTimeSource,
        SubmitBlockErrorResponse, SubmitBlockResponse,
    },
    config::mining::MinerAddressType,
    proposal_block_from_template,
    server::OPENED_RPC_ENDPOINT_MSG,
};
use zakura_test::{args, command::TestChild};
use zakurad::{components::With, config::ZakuradConfig};

use super::{
    config::{os_assigned_rpc_port_config, read_listen_addr_from_logs, testdir},
    launch::{ZakuradTestDirExt, EXTENDED_LAUNCH_DELAY},
    regtest::MiningRpcMethods,
};

mod wallet;

use wallet::{
    output, p2sh, push_script, v7_transfer, with_unlock_script, Coin, CommonBuilt, TestKey,
    IRONWOOD_FEE, SIGHASH_ALL, SIGHASH_NONE, TRANSPARENT_FEE,
};

/// The height at which [`v7_transactions_across_nu7_1_test`] activates NU7.1.
///
/// The coinbase at height 1 matures at height 101, so the chain has a mature output to
/// spend in a v7 transaction before NU7.1, at heights 101 and 102.
const LATE_NU7_1: u32 = 103;

/// The percentage of each block subsidy that the funding streams deposit into the lockbox.
const LOCKBOX_PERCENT: u64 = 1;

/// The ZIP 235 share of fees that a block removes from circulation, in tenths.
const NSM_FEE_TENTHS: i64 = 6;

/// The fee that the v7 transactions assembled from Zakura's types state and pay.
const STATED_FEE: u64 = 20_000;

/// The `valuePoolDeltas` bundle types of ZIP 248's registry.
const TRANSPARENT: u64 = 0;
const COINBASE: u64 = 1;
const IRONWOOD: u64 = 4;
const FEE: u64 = 5;

/// How long each test may take, including node startup and proving.
const TEST_TIMEOUT: Duration = Duration::from_secs(10 * 60);

/// Mines through NU7.1 activation and spends, rejects, and reloads v7 transactions.
///
/// - Coinbases are v6 before NU7.1 and v7 from it, and a v7 transaction is rejected
///   before NU7.1.
/// - One block mixes a v6 and two v7 transactions with its v7 coinbase, whose fee bundle
///   collects their fees.
/// - The mempool rejects v7 transactions with a wrong transparent delta, unbalanced
///   deltas, an NSM delta, or a `CHECKSIG` or `CHECKMULTISIG` signature without
///   `SIGHASH_ALL`, including one whose failure a script would otherwise negate.
/// - `sendrawtransaction` fails to decode a v7 encoding with a pre-NU7.1 branch ID, so
///   it never reaches the mempool.
/// - Block proposals are rejected for a v7 coinbase whose fee bundle, subsidy, lockbox
///   value, coinbase delta, or expiry height does not match its block, or whose fee
///   bundle removes value. A coinbase that states the next height is rejected as out of
///   sequence with its parent.
/// - A mature v7 coinbase output is spent by a v7 transaction whose second input spends
///   the second output of another transaction.
/// - Every v7 transaction reads back the same after a restart.
pub(crate) async fn v7_transactions_across_nu7_1_test() -> Result<()> {
    let _init_guard = zakura_test::init();
    tokio::time::timeout(TEST_TIMEOUT, async {
        let key = TestKey::new();
        let mut chain = V7Chain::start(nu7_1_regtest(LATE_NU7_1), &key).await?;

        let (activation_coinbase, early) = chain.mine_through_nu7_1(&key).await?;
        let second_output = Coin::of(&early.tx, 1)?;
        let mixed = chain.mine_mixed_block(&key, early).await?;
        let pays_scripts = mixed.last().expect("the mixed block has v7 transactions");
        let pending = chain
            .reject_invalid_transactions(&key, pays_scripts)
            .await?;
        let v7_txs = [
            vec![activation_coinbase],
            mixed,
            chain.reject_invalid_coinbases(&key, pending).await?,
            chain.spend_v7_coinbase(&key, &second_output).await?,
        ]
        .concat();

        let chain = chain.restart_and_compare(&v7_txs).await?;
        chain.node.stop()?;
        Ok(())
    })
    .await?
}

/// Mines a v7 coinbase that pays the default unified miner address through Ironwood,
/// after a v6 coinbase at a ZIP 271 lockbox disbursement height, and rejects a proposal
/// whose coinbase enables Ironwood spends.
pub(crate) async fn v7_coinbase_pays_ironwood_test() -> Result<()> {
    let _init_guard = zakura_test::init();
    tokio::time::timeout(TEST_TIMEOUT, async {
        let net = nu7_1_regtest(1);
        let config = node_config(&net)?.with(MinerAddressType::Unified);
        let node = Node::start(testdir()?, config).await?;

        // A v7 coinbase cannot pay a lockbox disbursement, so the template falls back.
        let first = node.rpc.mine_proposed_block(&net).await?;
        let disbursement = net.lockbox_disbursements(Height(1));
        assert_eq!(disbursement.len(), 1, "height 1 pays one disbursement");
        assert_eq!(first.transactions[0].version(), 6);
        assert!(first.transactions[0]
            .outputs()
            .iter()
            .any(|output| output.lock_script == disbursement[0].0.script()));

        let block = node.rpc.mine_proposed_block(&net).await?;
        let coinbase = &block.transactions[0];
        assert_eq!(coinbase.version(), 7);
        let shielded = coinbase
            .ironwood_shielded_data()
            .ok_or_else(|| eyre!("a unified miner address is paid through Ironwood"))?;

        let json = node.verbose_tx(coinbase.hash()).await?;
        let deltas = assert_v7_coinbase(&json, Height(2), 0)?;
        assert_eq!(
            deltas.get(&IRONWOOD).copied(),
            Some(i64::from(shielded.value_balance)),
            "the Ironwood delta is the bundle's balance",
        );
        assert!(
            deltas[&IRONWOOD] < 0,
            "the reward moves value into the Ironwood pool"
        );

        let template = node.template().await?;
        let block = template_block_with_coinbase(&template, &net, |coinbase| {
            let Transaction::V7 {
                ironwood_shielded_data: Some(shielded),
                ..
            } = coinbase
            else {
                return Err(eyre!("the template's v7 coinbase pays through Ironwood"));
            };
            shielded.flags |= zakura_chain::orchard::Flags::ENABLE_SPENDS;
            Ok(())
        })?;
        let response = node.rpc.propose(&block).await?;
        assert!(
            matches!(&response, BlockProposalResponse::Rejected(rejection)
                if rejection.contains("coinbasehasenablespendsironwood")),
            "a proposal whose coinbase enables Ironwood spends is rejected: {response:?}",
        );

        node.stop()?;
        Ok(())
    })
    .await?
}

/// Shields a mature v7 coinbase output into Ironwood and spends the note back to a
/// transparent output, with v7 transactions that the `zcash_primitives` builder proves.
pub(crate) async fn v7_ironwood_round_trip_test() -> Result<()> {
    let _init_guard = zakura_test::init();
    tokio::time::timeout(TEST_TIMEOUT, async {
        let key = TestKey::new();
        let net = nu7_1_regtest(1);
        let mut config = node_config(&net)?;
        config.mining.miner_address = Some(key.address().to_string().parse()?);
        let node = Node::start(testdir()?, config).await?;

        // The height 2 coinbase is the first v7 one and matures at height 102.
        node.rpc.generate(101).await?;
        let coinbase = node.block(2).await?.transactions[0].clone();
        assert_eq!(coinbase.version(), 7);
        let coin = key.coin_in(&coinbase)?;

        let (shield, note) = wallet::shield_to_ironwood(&net, Height(102), &key, &coin)?;
        shield.assert_digests_agree();
        node.mine_with(&net, &[&shield.tx]).await?;
        let deltas = value_pool_deltas(&node.verbose_tx(shield.tx.hash()).await?)?;
        assert_eq!(
            deltas,
            BTreeMap::from([
                (TRANSPARENT, signed(coin.value())),
                (IRONWOOD, -signed(note.value())),
                (FEE, -signed(IRONWOOD_FEE)),
            ]),
        );

        let unshield = wallet::unshield_from_ironwood(&net, Height(103), &note, &key)?;
        unshield.assert_digests_agree();
        node.mine_with(&net, &[&unshield.tx]).await?;
        let paid = note.value() - IRONWOOD_FEE;
        let json = node.verbose_tx(unshield.tx.hash()).await?;
        assert_eq!(
            value_pool_deltas(&json)?,
            BTreeMap::from([
                (TRANSPARENT, -signed(paid)),
                (IRONWOOD, signed(note.value())),
                (FEE, -signed(IRONWOOD_FEE)),
            ]),
        );
        assert_eq!(json["vout"][0]["valueZat"], signed(paid));
        let unspent = node.tx_out(Coin::of(&unshield.tx, 0)?.outpoint).await?;
        assert_eq!(
            unspent["value"], json["vout"][0]["value"],
            "the unshielded output is unspent",
        );

        node.stop()?;
        Ok(())
    })
    .await?
}

/// Relays v7 blocks and a v7 transaction between two nodes over Zakura P2P v2 with BPv1.
///
/// A consumer syncs the producer's chain past NU7.1. A v7 transfer submitted to the
/// consumer reaches the producer's mempool by transaction relay, which keys it by its
/// wtxid, and the producer mines it. The consumer receives that block by BPv1, which
/// checks the commitment in its v7 coinbase.
pub(crate) async fn v7_transactions_relay_between_nodes_test() -> Result<()> {
    let _init_guard = zakura_test::init();
    tokio::time::timeout(TEST_TIMEOUT, async {
        let key = TestKey::new();
        let net = nu7_1_regtest(1);

        let producer_id = NodeSecretKey::from_bytes(&[1; 32]);
        let producer_addr = free_udp_addr()?;
        let (dir, mut config) = v2_node(&net, &producer_id, producer_addr, Vec::new())?;
        config.mining.miner_address = Some(key.address().to_string().parse()?);
        let producer = Node::start(dir, config).await?;

        let bootstrap = format!("{}@{producer_addr}", producer_id.public());
        let (dir, mut config) = v2_node(
            &net,
            &NodeSecretKey::from_bytes(&[2; 32]),
            free_udp_addr()?,
            vec![bootstrap],
        )?;
        config.mining.miner_address = None;
        let consumer = Node::start(dir, config).await?;

        // The height 2 coinbase is the first v7 one and matures at height 102.
        producer.rpc.generate(101).await?;
        consumer.wait_for_tip_of(&producer).await?;
        let coinbase = consumer.block(2).await?.transactions[0].clone();
        assert_eq!(coinbase.version(), 7, "the consumer synced v7 coinbases");

        let transfer = wallet::transparent_transfer(
            &net,
            Height(102),
            TxVersion::V7,
            &key,
            &[key.coin_in(&coinbase)?],
            1,
        )?;
        consumer.rpc.send_raw_transaction(&transfer.tx).await?;
        producer.wait_for_mempool(transfer.tx.hash()).await?;

        let block = producer.rpc.mine_proposed_block(&net).await?;
        assert!(block
            .transactions
            .iter()
            .any(|tx| tx.hash() == transfer.tx.hash()));
        consumer.wait_for_tip_of(&producer).await?;
        let json = consumer.verbose_tx(transfer.tx.hash()).await?;
        assert_v7_transfer(&json, &transfer.tx, TRANSPARENT_FEE)?;
        assert_eq!(json["height"], 102);

        let Node { child, rpc } = consumer;
        let mut consumer = Node {
            child: child.with_timeout(RELAY_TIMEOUT),
            rpc,
        };
        consumer
            .child
            .expect_stdout_line_matches(format!("commitment_verified.*{}", block.hash()))?;

        consumer.stop()?;
        producer.stop()?;
        Ok(())
    })
    .await?
}

/// How long a two-node test waits for a block or transaction to reach the other node.
const RELAY_TIMEOUT: Duration = Duration::from_secs(120);

/// Returns a test directory and a config for a node on `net` that uses only Zakura P2P
/// v2, with BPv1, identity `node_key`, listening on `listen_addr`.
fn v2_node(
    net: &Network,
    node_key: &NodeSecretKey,
    listen_addr: SocketAddr,
    bootstrap_peers: Vec<String>,
) -> Result<(TempDir, ZakuradConfig)> {
    let dir = testdir()?;
    let identity_dir = dir.path().join("identity");
    std::fs::create_dir_all(&identity_dir)?;
    std::fs::write(
        identity_dir.join("regtest.zakura-iroh-secret-key"),
        node_key.to_hex(),
    )?;

    let mut config = node_config(net)?;
    config.network.p2p_stack = P2pStack::Zakura;
    config.network.identity_dir = identity_dir;
    config.network.zakura.listen_addr = Some(listen_addr);
    config.network.zakura.bootstrap_peers = bootstrap_peers;
    config.network.zakura.bpv1.enabled = true;
    // The test reads only these lines, and nothing drains the rest of stdout.
    config.tracing.filter = Some(
        "warn,zakura_rpc::server=info,zakurad::components::mempool=info,bpv1_stage=info".to_owned(),
    );
    Ok((dir, config))
}

/// Returns a local UDP address that is free now.
fn free_udp_addr() -> Result<SocketAddr> {
    Ok(UdpSocket::bind("127.0.0.1:0")?.local_addr()?)
}

/// A Regtest network whose upgrades through NU7 activate at height 1, and NU7.1 at
/// `nu7_1`.
///
/// Funding streams deposit [`LOCKBOX_PERCENT`] of each subsidy into the lockbox, so v7
/// coinbases state a nonzero lockbox value. Height 1 is the NU6.1 activation block, whose
/// coinbase pays a ZIP 271 disbursement that fits in its own lockbox deposit.
fn nu7_1_regtest(nu7_1: u32) -> Network {
    Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu6_3: Some(1),
            nu7: Some(1),
            nu7_1: Some(nu7_1),
            ..Default::default()
        },
        funding_streams: Some(vec![ConfiguredFundingStreams {
            height_range: Some(Height(1)..Height(1_000)),
            recipients: Some(vec![ConfiguredFundingStreamRecipient {
                receiver: FundingStreamReceiver::Deferred,
                numerator: LOCKBOX_PERCENT,
                addresses: None,
            }]),
        }]),
        // NU7's subsidy puts 2,083,333 zatoshis into the first block's lockbox.
        lockbox_disbursements: Some(vec![ConfiguredLockboxDisbursement {
            address: "t2RnBRiqrN1nW4ecZs1Fj3WWjNdnSs4kiX8".to_string(),
            amount: Amount::new(2_000_000),
        }]),
        ..Default::default()
    })
}

/// A node config for `net` whose mempool is active from genesis and whose state
/// survives restarts.
fn node_config(net: &Network) -> Result<ZakuradConfig> {
    let mut config = os_assigned_rpc_port_config(false, net)?;
    config.mempool.debug_enable_at_height = Some(0);
    config.state.ephemeral = false;
    config.consensus.checkpoint_sync = false;
    // Restores every non-finalized block after a restart.
    config.state.debug_skip_non_finalized_state_backup_task = true;
    Ok(config)
}

/// How long a killed node may take to exit.
const NODE_EXIT_TIMEOUT: Duration = Duration::from_secs(30);

/// A running `zakurad` and its RPC client.
struct Node {
    child: TestChild<TempDir>,
    rpc: RpcRequestClient,
}

impl Node {
    async fn start(dir: TempDir, mut config: ZakuradConfig) -> Result<Self> {
        let dir = dir.with_config(&mut config)?;
        Self::spawn(dir).await
    }

    async fn spawn(dir: TempDir) -> Result<Self> {
        let mut child = dir
            .spawn_child(args!["start"])?
            .with_timeout(EXTENDED_LAUNCH_DELAY);
        let rpc = RpcRequestClient::new(read_listen_addr_from_logs(
            &mut child,
            OPENED_RPC_ENDPOINT_MSG,
        )?);
        child.expect_stdout_line_matches("mempool activated")?;
        Ok(Self { child, rpc })
    }

    /// Kills the node, waits for it to exit, and returns its directory.
    ///
    /// The OS releases the node's database lock when the process exits, so a new node
    /// can open the directory as soon as this returns.
    fn stop(mut self) -> Result<TempDir> {
        self.child.kill(false)?;
        let mut output = self
            .child
            .wait_with_output_or_timeout(NODE_EXIT_TIMEOUT)?
            .assert_failure()?;
        output.assert_was_killed()?;
        output
            .dir
            .take()
            .ok_or_else(|| eyre!("the test directory is kept"))
    }

    async fn block(&self, height: i32) -> Result<Block> {
        self.rpc
            .get_block(height)
            .await
            .map_err(|error| eyre!(error))?
            .map(Arc::unwrap_or_clone)
            .ok_or_else(|| eyre!("the chain has a block at {height}"))
    }

    async fn call(&self, method: &str, params: String) -> Result<Value> {
        self.rpc
            .json_result_from_call(method, params)
            .await
            .map_err(|error| eyre!("{method}: {error}"))
    }

    async fn verbose_tx(&self, hash: transaction::Hash) -> Result<Value> {
        self.call("getrawtransaction", format!(r#"["{hash}", 1]"#))
            .await
    }

    /// Returns `gettxout` for `outpoint`, which is `null` once it is spent.
    async fn tx_out(&self, outpoint: OutPoint) -> Result<Value> {
        self.call(
            "gettxout",
            format!(r#"["{}", {}, false]"#, outpoint.hash, outpoint.index),
        )
        .await
    }

    /// Waits until this node's best block is `other`'s.
    async fn wait_for_tip_of(&self, other: &Node) -> Result<()> {
        let target = other.call("getbestblockhash", "[]".to_owned()).await?;
        self.wait_until(|node| {
            let target = target.clone();
            async move { Ok(node.call("getbestblockhash", "[]".to_owned()).await? == target) }
        })
        .await
        .map_err(|error| eyre!("the node reaches the best block {target}: {error}"))
    }

    /// Waits until this node's mempool holds `hash`.
    async fn wait_for_mempool(&self, hash: transaction::Hash) -> Result<()> {
        let hash = hash.to_string();
        self.wait_until(|node| {
            let hash = hash.clone();
            async move {
                let mempool = node.call("getrawmempool", "[]".to_owned()).await?;
                Ok(mempool
                    .as_array()
                    .is_some_and(|ids| ids.iter().any(|id| id.as_str() == Some(hash.as_str()))))
            }
        })
        .await
        .map_err(|error| eyre!("the mempool receives {hash}: {error}"))
    }

    /// Polls `done` until it returns true, for at most [`RELAY_TIMEOUT`].
    async fn wait_until<'a, F, Fut>(&'a self, done: F) -> Result<()>
    where
        F: Fn(&'a Node) -> Fut,
        Fut: std::future::Future<Output = Result<bool>>,
    {
        tokio::time::timeout(RELAY_TIMEOUT, async {
            while !done(self).await? {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Ok(())
        })
        .await?
    }

    async fn template(&self) -> Result<BlockTemplateResponse> {
        self.rpc
            .json_result_from_call("getblocktemplate", "[]")
            .await
            .map_err(|error| eyre!(error))
    }

    /// Submits `tx` and returns the message of the error it must be rejected with.
    async fn send_rejected(&self, tx: &Transaction) -> Result<String> {
        self.send_rejected_bytes(&tx.zcash_serialize_to_vec()?)
            .await
    }

    /// Submits the transaction encoding `bytes` and returns the message of the error it
    /// must be rejected with.
    async fn send_rejected_bytes(&self, bytes: &[u8]) -> Result<String> {
        let data = hex::encode(bytes);
        let response: Value = serde_json::from_str(
            &self
                .rpc
                .text_from_call("sendrawtransaction", format!(r#"["{data}"]"#))
                .await?,
        )?;
        response
            .pointer("/error/message")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| eyre!("the transaction is rejected: {response}"))
    }

    /// Submits `txs`, checks that the next template includes them, and mines it.
    ///
    /// Returns the block and its transaction fees.
    async fn mine_with(&self, net: &Network, txs: &[&Transaction]) -> Result<(Block, i64)> {
        for tx in txs {
            self.rpc.send_raw_transaction(tx).await?;
        }
        let mempool = self.call("getrawmempool", "[]".to_owned()).await?;
        let template = self.template().await?;
        for tx in txs {
            let hash = tx.hash().to_string();
            assert!(
                mempool
                    .as_array()
                    .is_some_and(|ids| ids.iter().any(|id| id.as_str() == Some(hash.as_str()))),
                "the mempool holds {hash}",
            );
            assert!(
                template
                    .transactions()
                    .iter()
                    .any(|template_tx| template_tx.hash() == tx.hash()),
                "the template includes {hash}",
            );
        }
        let fees: i64 = template
            .transactions()
            .iter()
            .map(|tx| i64::from(tx.fee()))
            .sum();
        assert_eq!(
            -i64::from(template.coinbase_txn().fee()),
            fees - fees * NSM_FEE_TENTHS / 10,
            "the template's coinbase pays the miner the fees that ZIP 235 leaves",
        );

        let block = self.rpc.mine_proposed_block(net).await?;
        assert_eq!(
            block.transactions.len(),
            template.transactions().len() + 1,
            "the mined block has the template's transactions",
        );
        for tx in txs {
            assert!(block
                .transactions
                .iter()
                .any(|mined| mined.hash() == tx.hash()));
        }
        Ok((block, fees))
    }
}

/// The Regtest chain of [`v7_transactions_across_nu7_1_test`], mined to the test key.
struct V7Chain {
    net: Network,
    node: Node,
    tip: u32,
}

impl V7Chain {
    async fn start(net: Network, key: &TestKey) -> Result<Self> {
        let mut config = node_config(&net)?;
        config.mining.miner_address = Some(key.address().to_string().parse()?);
        Ok(Self {
            node: Node::start(testdir()?, config).await?,
            net,
            tip: 0,
        })
    }

    fn next_height(&self) -> Height {
        Height(self.tip + 1)
    }

    /// Mines `count` blocks with the `generate` RPC.
    async fn generate(&mut self, count: u32) -> Result<()> {
        self.node.rpc.generate(count).await?;
        self.tip += count;
        Ok(())
    }

    /// Submits `txs` and mines them in a block from a template, checked in proposal mode
    /// first. Returns the block and its transaction fees.
    async fn mine(&mut self, txs: &[&Transaction]) -> Result<(Block, i64)> {
        let (block, fees) = self.node.mine_with(&self.net, txs).await?;
        self.tip += 1;
        assert_eq!(block.coinbase_height(), Some(Height(self.tip)));
        Ok((block, fees))
    }

    /// Returns the test key's output in the coinbase at `height`.
    async fn coinbase_coin(&self, key: &TestKey, height: u32) -> Result<Coin> {
        let block = self.node.block(i32::try_from(height)?).await?;
        key.coin_in(&block.transactions[0])
    }

    /// Mines v6 coinbases up to NU7.1 and the first v7 coinbase at it, and checks that a
    /// v7 transaction is rejected before NU7.1.
    ///
    /// Returns the v7 coinbase, and the rejected v7 transaction, which becomes valid at
    /// NU7.1 and pays the test key two outputs.
    async fn mine_through_nu7_1(&mut self, key: &TestKey) -> Result<(Transaction, CommonBuilt)> {
        self.generate(LATE_NU7_1 - 3).await?;
        let coin = self.coinbase_coin(key, 1).await?;
        let early = wallet::transparent_transfer(
            &self.net,
            Height(LATE_NU7_1),
            TxVersion::V7,
            key,
            &[coin],
            2,
        )?;
        early.assert_digests_agree();
        let message = self.node.send_rejected(&early.tx).await?;
        assert!(
            message.contains("incorrect consensus branch id"),
            "a v7 transaction is rejected before NU7.1: {message}",
        );

        while self.next_height().0 < LATE_NU7_1 {
            let (block, _) = self.mine(&[]).await?;
            let json = self.node.verbose_tx(block.transactions[0].hash()).await?;
            assert_eq!(json["version"], 6, "coinbases are v6 before NU7.1");
            assert!(json.get("valuePoolDeltas").is_none());
            assert!(json.get("coinbase").is_none());
        }

        let (block, fees) = self.mine(&[]).await?;
        assert_eq!(fees, 0);
        let coinbase = Transaction::clone(&block.transactions[0]);
        assert_v7_coinbase(
            &self.node.verbose_tx(coinbase.hash()).await?,
            Height(LATE_NU7_1),
            0,
        )?;
        Ok((coinbase, early))
    }

    /// Mines a block with a v7 coinbase, a v6 transaction, the v7 transaction rejected
    /// before NU7.1, and a v7 transaction assembled from Zakura's types, which pays a
    /// `CHECKSIG NOT` script in its first output and a 1-of-1 `CHECKMULTISIG` script in
    /// its second.
    ///
    /// Returns the block's v7 transactions, ending with the one that pays the scripts.
    async fn mine_mixed_block(
        &mut self,
        key: &TestKey,
        early: CommonBuilt,
    ) -> Result<Vec<Transaction>> {
        let height = self.next_height();
        let v6 = wallet::transparent_transfer(
            &self.net,
            height,
            TxVersion::V6,
            key,
            &[self.coinbase_coin(key, 2).await?],
            1,
        )?;
        v6.assert_digests_agree();

        let coin = self.coinbase_coin(key, 3).await?;
        let paid = coin.value() - STATED_FEE;
        let pays_scripts = signed_transfer(
            key,
            &coin,
            vec![
                output(paid / 2, p2sh(&checksig_not(key)))?,
                output(paid - paid / 2, p2sh(&one_of_one_multisig(key)))?,
            ],
            stated_fee_deltas(),
            SIGHASH_ALL,
        )?;

        let (block, fees) = self.mine(&[&v6.tx, &early.tx, &pays_scripts]).await?;
        assert_eq!(fees, signed(2 * TRANSPARENT_FEE + STATED_FEE));
        let versions: Vec<_> = block.transactions.iter().map(|tx| tx.version()).collect();
        assert_eq!(versions[0], 7);
        assert_eq!(versions[1..].iter().filter(|&&v| v == 6).count(), 1);
        assert_eq!(versions[1..].iter().filter(|&&v| v == 7).count(), 2);
        assert_v7_coinbase(
            &self.node.verbose_tx(block.transactions[0].hash()).await?,
            height,
            fees,
        )?;

        let json = self.node.verbose_tx(v6.tx.hash()).await?;
        assert_eq!(json["version"], 6);
        assert!(json.get("valuePoolDeltas").is_none());
        for (tx, fee) in [(&early.tx, TRANSPARENT_FEE), (&pays_scripts, STATED_FEE)] {
            let json = self.node.verbose_tx(tx.hash()).await?;
            assert_v7_transfer(&json, tx, fee)?;
        }
        assert_eq!(
            self.node
                .tx_out(self.coinbase_coin(key, 1).await?.outpoint)
                .await?,
            Value::Null,
            "the v7 transaction spent the v6 coinbase output",
        );
        assert!(self
            .node
            .tx_out(Coin::of(&early.tx, 0)?.outpoint)
            .await?
            .is_object());

        Ok(vec![
            Transaction::clone(&block.transactions[0]),
            early.tx,
            pays_scripts,
        ])
    }

    /// Checks that the mempool, or for a pre-NU7.1 branch ID the decoder, rejects v7
    /// transactions that break one v7 rule each.
    ///
    /// Returns a valid spend of the `CHECKSIG NOT` output of `pays_scripts`, which the
    /// mempool has not seen yet.
    async fn reject_invalid_transactions(
        &mut self,
        key: &TestKey,
        pays_scripts: &Transaction,
    ) -> Result<Transaction> {
        let coin = self.coinbase_coin(key, 4).await?;
        let fee = signed(STATED_FEE);
        let amount = |zatoshis: i64| -> Amount<NegativeAllowed> {
            zatoshis.try_into().expect("test amounts are valid")
        };
        // The output of a transfer of `coin` that pays the test key all but STATED_FEE.
        let to_key_less_fee = |coin: &Coin| -> Result<Vec<Output>> {
            Ok(vec![output(
                coin.value() - STATED_FEE,
                key.address().script(),
            )?])
        };

        for (description, deltas, hash_type, error) in [
            (
                "a transparent delta that is not its inputs less its outputs",
                ValueDeltas {
                    transparent: amount(fee + 1),
                    fee: amount(-fee - 1),
                    ..ValueDeltas::default()
                },
                SIGHASH_ALL,
                "transparent value pool delta of a v7 transaction MUST equal",
            ),
            (
                "deltas that do not sum to zero",
                ValueDeltas {
                    transparent: amount(fee),
                    fee: amount(-fee - 1),
                    ..ValueDeltas::default()
                },
                SIGHASH_ALL,
                "value pool deltas of a v7 transaction MUST sum to zero",
            ),
            (
                "an NSM delta, whose bundle is not active at NU7.1",
                ValueDeltas {
                    transparent: amount(fee),
                    fee: amount(-fee + 1),
                    nsm: amount(-1),
                    ..ValueDeltas::default()
                },
                SIGHASH_ALL,
                "v7 bundle Nsm is not active",
            ),
            (
                "a SIGHASH_NONE signature",
                stated_fee_deltas(),
                SIGHASH_NONE,
                SIGHASH_NOT_ALL,
            ),
        ] {
            let tx = signed_transfer(key, &coin, to_key_less_fee(&coin)?, deltas, hash_type)?;
            let message = self.node.send_rejected(&tx).await?;
            assert!(
                message.contains(error),
                "a v7 transaction with {description} is rejected with {error:?}: {message}",
            );
        }

        // A v7 transaction encodes its consensus branch ID, and v7 starts at NU7.1, so
        // the v7 decoder rejects an earlier one before the transaction reaches the
        // mempool. Zakura's encoder refuses to write one, so this patches the branch ID
        // of a valid encoding.
        let valid = signed_transfer(
            key,
            &coin,
            to_key_less_fee(&coin)?,
            stated_fee_deltas(),
            SIGHASH_ALL,
        )?;
        let mut bytes = valid.zcash_serialize_to_vec()?;
        let nu7 = NetworkUpgrade::Nu7
            .branch_id()
            .ok_or_else(|| eyre!("NU7 has a consensus branch ID"))?;
        // The 4-byte header and version group ID precede the branch ID.
        bytes[8..12].copy_from_slice(&u32::from(nu7).to_le_bytes());
        let message = self.node.send_rejected_bytes(&bytes).await?;
        assert!(
            message.contains("NU7.1 or later consensus branch ID"),
            "a v7 encoding with the NU7 branch ID is rejected: {message}",
        );

        // CHECKMULTISIG checks the signatures it is given, so the rule covers them too.
        let coin = Coin::of(pays_scripts, 1)?;
        let unsigned = wallet::v7_transfer(
            std::slice::from_ref(&coin),
            to_key_less_fee(&coin)?,
            stated_fee_deltas(),
        );
        let signature = key.sign_v7(&unsigned, std::slice::from_ref(&coin), 0, SIGHASH_NONE);
        // CHECKMULTISIG pops one item more than it uses, which the empty push supplies.
        let multisig = with_unlock_script(
            unsigned,
            0,
            push_script(&[&[], &signature, &one_of_one_multisig(key)]),
        );
        let message = self.node.send_rejected(&multisig).await?;
        assert!(
            message.contains(SIGHASH_NOT_ALL),
            "a CHECKMULTISIG signature without SIGHASH_ALL is rejected: {message}",
        );

        // The redeem script negates the signature check, so a signature that merely failed
        // would spend it: a signature without SIGHASH_ALL must fail the script instead.
        let coin = Coin::of(pays_scripts, 0)?;
        let redeem_script = checksig_not(key);
        let unsigned = wallet::v7_transfer(
            std::slice::from_ref(&coin),
            to_key_less_fee(&coin)?,
            stated_fee_deltas(),
        );
        let signature = key.sign_v7(&unsigned, std::slice::from_ref(&coin), 0, SIGHASH_NONE);
        let negated = with_unlock_script(
            unsigned.clone(),
            0,
            push_script(&[&signature, &redeem_script]),
        );
        let message = self.node.send_rejected(&negated).await?;
        assert!(
            message.contains(SIGHASH_NOT_ALL),
            "a negated signature without SIGHASH_ALL is rejected: {message}",
        );

        // An empty signature is not a signature that the rule covers: CHECKSIG pushes
        // false, and NOT makes the script succeed.
        let empty = with_unlock_script(unsigned, 0, push_script(&[&[], &redeem_script]));
        // The two spends differ only in authorizing data, so rejecting one must not reject
        // the other by txid.
        assert_eq!(empty.hash(), negated.hash());
        assert_ne!(empty.auth_digest(), negated.auth_digest());
        Ok(empty)
    }

    /// Checks that proposals with each [`CoinbaseFault`] are rejected, then mines the
    /// template with `pending`, which pays a fee, so that the template's fee bundle and
    /// ZIP 235 contribution are nonzero.
    ///
    /// Returns the block's v7 transactions.
    async fn reject_invalid_coinbases(
        &mut self,
        key: &TestKey,
        pending: Transaction,
    ) -> Result<Vec<Transaction>> {
        self.node.rpc.send_raw_transaction(&pending).await?;
        let template = self.node.template().await?;
        assert!(
            i64::from(template.coinbase_txn().fee()) < 0,
            "the template has fees, so its fee bundle can be lowered",
        );

        for (fault, reason) in [
            (CoinbaseFault::FeeBelowBlockFees, "v7coinbasefeesmismatch"),
            (
                CoinbaseFault::SubsidyBelowSchedule,
                "v7coinbasesubsidymismatch",
            ),
            (
                CoinbaseFault::LockboxAboveDeposit,
                "v7coinbaselockboxmismatch",
            ),
            (
                CoinbaseFault::DeltaIgnoresNsmContribution,
                "v7coinbasedeltamismatch",
            ),
            (CoinbaseFault::ExpiryAhead, "coinbaseexpiryblockheight"),
            (CoinbaseFault::FeeRemovesValue, "v7coinbasefeeremovesvalue"),
            (CoinbaseFault::HeightAhead, NONSEQUENTIAL_BLOCK_REJECTION),
        ] {
            let block = faulty_block(&template, &self.net, key, fault)?;
            let response = self.node.rpc.propose(&block).await?;
            assert!(
                matches!(&response, BlockProposalResponse::Rejected(rejection) if rejection.contains(reason)),
                "a proposal whose coinbase has {fault:?} is rejected with {reason:?}: {response:?}",
            );
        }
        let block = faulty_block(&template, &self.net, key, CoinbaseFault::FeeBelowBlockFees)?;
        assert_eq!(
            self.node.rpc.submit_block_response(&block).await?,
            SubmitBlockResponse::ErrorResponse(SubmitBlockErrorResponse::Rejected),
            "submitblock rejects a v7 coinbase whose fee bundle is short",
        );

        let (block, fees) = self.mine(&[]).await?;
        assert!(block
            .transactions
            .iter()
            .any(|tx| tx.hash() == pending.hash()));
        assert_eq!(fees, signed(STATED_FEE));
        let height = Height(self.tip);
        assert_v7_coinbase(
            &self.node.verbose_tx(block.transactions[0].hash()).await?,
            height,
            fees,
        )?;
        assert_v7_transfer(
            &self.node.verbose_tx(pending.hash()).await?,
            &pending,
            STATED_FEE,
        )?;

        Ok(vec![Transaction::clone(&block.transactions[0]), pending])
    }

    /// Spends the first v7 coinbase output once it matures, with a builder-made v7
    /// transaction whose second input spends `second_output`, an output at index 1.
    ///
    /// Every other transparent spend that these tests mine is of input 0 and output index
    /// 0, so this is the mined spend whose v7 digests and signature commit to nonzero
    /// positions.
    ///
    /// Returns the v7 transactions of the spending block.
    async fn spend_v7_coinbase(
        &mut self,
        key: &TestKey,
        second_output: &Coin,
    ) -> Result<Vec<Transaction>> {
        assert_eq!(second_output.outpoint.index, 1);
        let coin = self.coinbase_coin(key, LATE_NU7_1).await?;
        let maturity = LATE_NU7_1 + zakura_chain::transparent::MIN_TRANSPARENT_COINBASE_MATURITY;

        self.generate(maturity - 2 - self.tip).await?;
        let immature = wallet::transparent_transfer(
            &self.net,
            self.next_height(),
            TxVersion::V7,
            key,
            std::slice::from_ref(&coin),
            1,
        )?;
        let message = self.node.send_rejected(&immature.tx).await?;
        assert!(
            message.contains("immature"),
            "a v7 coinbase output is immature until height {maturity}: {message}",
        );

        self.generate(1).await?;
        assert_eq!(self.next_height(), Height(maturity));
        let spend = wallet::transparent_transfer(
            &self.net,
            self.next_height(),
            TxVersion::V7,
            key,
            &[coin.clone(), second_output.clone()],
            1,
        )?;
        spend.assert_digests_agree();
        let (block, fees) = self.mine(&[&spend.tx]).await?;
        assert_eq!(fees, signed(TRANSPARENT_FEE));
        assert_v7_coinbase(
            &self.node.verbose_tx(block.transactions[0].hash()).await?,
            Height(maturity),
            fees,
        )?;
        let json = self.node.verbose_tx(spend.tx.hash()).await?;
        assert_v7_transfer(&json, &spend.tx, TRANSPARENT_FEE)?;
        assert_eq!(json["vin"][0]["txid"], coin.outpoint.hash.to_string());
        assert_eq!(
            json["vin"][1]["txid"],
            second_output.outpoint.hash.to_string()
        );
        assert_eq!(json["vin"][1]["vout"], 1);
        assert_eq!(self.node.tx_out(coin.outpoint).await?, Value::Null);
        assert_eq!(self.node.tx_out(second_output.outpoint).await?, Value::Null);
        assert!(self
            .node
            .tx_out(Coin::of(&spend.tx, 0)?.outpoint)
            .await?
            .is_object());

        Ok(vec![Transaction::clone(&block.transactions[0]), spend.tx])
    }

    /// Restarts the node on its state, and checks that it reads back each of `v7_txs`
    /// as before and keeps mining.
    async fn restart_and_compare(self, v7_txs: &[Transaction]) -> Result<Self> {
        let mut before = Vec::new();
        for tx in v7_txs {
            before.push(self.node.verbose_tx(tx.hash()).await?);
        }
        let best_hash = self.node.call("getbestblockhash", "[]".to_owned()).await?;

        let Self { net, node, tip } = self;
        let dir = node.stop()?;
        let mut chain = Self {
            node: Node::spawn(dir).await?,
            net,
            tip,
        };

        assert_eq!(
            chain.node.call("getbestblockhash", "[]".to_owned()).await?,
            best_hash
        );
        for (tx, before) in v7_txs.iter().zip(before) {
            let after = chain.node.verbose_tx(tx.hash()).await?;
            assert_eq!(after, before, "the restarted node reads back {}", tx.hash());
            assert_eq!(after["txid"], tx.hash().to_string());
            assert_eq!(
                after["authdigest"],
                tx.auth_digest()
                    .ok_or_else(|| eyre!("v7 transactions have an auth digest"))?
                    .to_string(),
            );
        }

        let (block, _) = chain.mine(&[]).await?;
        assert_eq!(block.transactions[0].version(), 7);
        Ok(chain)
    }
}

/// The script error of a v7 signature without `SIGHASH_ALL`.
const SIGHASH_NOT_ALL: &str = "signature hash type is not SIGHASH_ALL";

/// The proposal rejection of a block that is not its parent's child.
///
/// A coinbase that states the next block's height gets this rejection, not a coinbase
/// height mismatch: block verification reads the block's height from that same field.
const NONSEQUENTIAL_BLOCK_REJECTION: &str = "nonsequentialblock";

/// The deltas of a transfer that pays [`STATED_FEE`] and states it.
fn stated_fee_deltas() -> ValueDeltas {
    let fee = signed(STATED_FEE);
    ValueDeltas {
        transparent: fee.try_into().expect("valid amount"),
        fee: (-fee).try_into().expect("valid amount"),
        ..ValueDeltas::default()
    }
}

/// A redeem script that succeeds when `key`'s signature check fails.
fn checksig_not(key: &TestKey) -> Vec<u8> {
    const OP_CHECKSIG: u8 = 0xac;
    const OP_NOT: u8 = 0x91;
    let mut script = vec![33];
    script.extend_from_slice(&key.pubkey());
    script.extend_from_slice(&[OP_CHECKSIG, OP_NOT]);
    script
}

/// A 1-of-1 `CHECKMULTISIG` redeem script of `key`.
fn one_of_one_multisig(key: &TestKey) -> Vec<u8> {
    const OP_1: u8 = 0x51;
    const OP_CHECKMULTISIG: u8 = 0xae;
    let mut script = vec![OP_1, 33];
    script.extend_from_slice(&key.pubkey());
    script.extend_from_slice(&[OP_1, OP_CHECKMULTISIG]);
    script
}

/// A v7 transfer of `coin`, a P2PKH output of `key`, to `outputs`, which states `deltas`
/// and is signed with `hash_type`.
fn signed_transfer(
    key: &TestKey,
    coin: &Coin,
    outputs: Vec<Output>,
    deltas: ValueDeltas,
    hash_type: u8,
) -> Result<Transaction> {
    let coins = std::slice::from_ref(coin);
    let unsigned = v7_transfer(coins, outputs, deltas);
    let signature = key.sign_v7(&unsigned, coins, 0, hash_type);
    Ok(with_unlock_script(
        unsigned,
        0,
        push_script(&[&signature, &key.pubkey()]),
    ))
}

fn signed(zatoshis: u64) -> i64 {
    i64::try_from(zatoshis).expect("test amounts fit in i64")
}

/// Returns the `valuePoolDeltas` of a verbose transaction by bundle type, checking that
/// they use variant 0, are nonzero, and are in increasing bundle type order.
fn value_pool_deltas(json: &Value) -> Result<BTreeMap<u64, i64>> {
    let mut deltas = BTreeMap::new();
    let mut last = None;
    for delta in json["valuePoolDeltas"]
        .as_array()
        .ok_or_else(|| eyre!("a v7 transaction lists its value pool deltas: {json}"))?
    {
        let bundle_type = delta["bundleType"]
            .as_u64()
            .ok_or_else(|| eyre!("bundleType: {delta}"))?;
        let value = delta["valueZat"]
            .as_i64()
            .ok_or_else(|| eyre!("valueZat: {delta}"))?;
        assert_eq!(delta["bundleVariant"], 0);
        assert_ne!(value, 0, "deltas are nonzero");
        assert!(last < Some(bundle_type), "deltas are in bundle type order");
        last = Some(bundle_type);
        deltas.insert(bundle_type, value);
    }
    Ok(deltas)
}

/// Checks the verbose output of the v7 coinbase at `height`, whose block's other
/// transactions pay `fees`, and returns its value pool deltas.
fn assert_v7_coinbase(json: &Value, height: Height, fees: i64) -> Result<BTreeMap<u64, i64>> {
    assert_eq!(json["version"], 7, "{json}");
    assert_eq!(
        json["vin"],
        Value::Array(Vec::new()),
        "a v7 coinbase has no inputs"
    );

    let coinbase = &json["coinbase"];
    assert_eq!(coinbase["blockHeight"], height.0);
    let subsidy = coinbase["blockSubsidyZat"]
        .as_i64()
        .ok_or_else(|| eyre!("blockSubsidyZat: {coinbase}"))?;
    let lockbox = coinbase["lockboxValueZat"]
        .as_i64()
        .ok_or_else(|| eyre!("lockboxValueZat: {coinbase}"))?;
    assert_eq!(
        lockbox,
        subsidy * signed(LOCKBOX_PERCENT) / 100,
        "the funding streams' lockbox deposit",
    );

    let deltas = value_pool_deltas(json)?;
    assert_eq!(deltas.values().sum::<i64>(), 0, "the deltas balance");
    assert_eq!(
        deltas.get(&FEE).copied().unwrap_or_default(),
        fees,
        "the fee bundle collects the block's fees",
    );
    assert_eq!(
        deltas.get(&COINBASE).copied(),
        Some(subsidy - lockbox - fees * NSM_FEE_TENTHS / 10),
        "the coinbase delta is the subsidy less the lockbox deposit and the ZIP 235 \
         contribution",
    );
    let outputs: i64 = json["vout"]
        .as_array()
        .ok_or_else(|| eyre!("vout: {json}"))?
        .iter()
        .map(|output| output["valueZat"].as_i64().unwrap_or_default())
        .sum();
    assert_eq!(
        deltas.get(&TRANSPARENT).copied().unwrap_or_default(),
        -outputs,
        "the transparent delta pays the outputs",
    );
    Ok(deltas)
}

/// Checks the verbose output of a v7 transparent transfer `tx` that pays `fee`.
fn assert_v7_transfer(json: &Value, tx: &Transaction, fee: u64) -> Result<()> {
    assert_eq!(json["version"], 7);
    assert_eq!(json["txid"], tx.hash().to_string());
    assert_eq!(
        json["authdigest"],
        tx.auth_digest()
            .ok_or_else(|| eyre!("v7 transactions have an auth digest"))?
            .to_string(),
    );
    assert!(json.get("coinbase").is_none());
    assert_eq!(
        value_pool_deltas(json)?,
        BTreeMap::from([(TRANSPARENT, signed(fee)), (FEE, -signed(fee))]),
    );
    Ok(())
}

/// A v7 coinbase field that breaks a coinbase rule, with the deltas and outputs kept
/// balanced so that only that field is wrong.
///
/// Each fault but [`CoinbaseFault::HeightAhead`] fails the coinbase rule it names.
#[derive(Clone, Copy, Debug)]
enum CoinbaseFault {
    /// The fee bundle collects one zatoshi less than the block's fees, and the miner is
    /// paid one zatoshi less.
    FeeBelowBlockFees,
    /// The coinbase bundle and expiry height state the height after the block's.
    ///
    /// Block verification reads the block's height from the coinbase bundle, so the
    /// coinbase height rule cannot fail. The block fails as out of sequence with its
    /// parent instead.
    HeightAhead,
    /// The expiry height is the height after the block's, and the coinbase bundle states
    /// the block's height.
    ExpiryAhead,
    /// `blockSubsidy` is one zatoshi short, and so are the coinbase delta and the miner's
    /// output.
    SubsidyBelowSchedule,
    /// `lockboxValue` is one zatoshi more than the lockbox deposit, and the coinbase delta
    /// and the miner's output are one zatoshi less.
    LockboxAboveDeposit,
    /// The coinbase delta is the subsidy less the lockbox deposit, without the ZIP 235
    /// fee contribution, and the miner is paid that contribution too.
    DeltaIgnoresNsmContribution,
    /// The fee bundle removes one zatoshi instead of collecting the block's fees, and the
    /// miner is paid that much less.
    FeeRemovesValue,
}

/// Returns the block of `template`, with `fault` in its v7 coinbase and the header roots
/// recomputed for the changed coinbase.
fn faulty_block(
    template: &BlockTemplateResponse,
    net: &Network,
    key: &TestKey,
    fault: CoinbaseFault,
) -> Result<Block> {
    template_block_with_coinbase(template, net, |coinbase| {
        add_coinbase_fault(coinbase, key, fault)
    })
}

/// Puts `fault` in the v7 `coinbase`, which pays the miner `key`.
fn add_coinbase_fault(
    coinbase: &mut Transaction,
    key: &TestKey,
    fault: CoinbaseFault,
) -> Result<()> {
    let Transaction::V7 {
        expiry_height,
        outputs,
        v7_fields,
        ..
    } = coinbase
    else {
        return Err(eyre!("the template's coinbase is v7"));
    };
    let bundle = v7_fields
        .coinbase
        .as_ref()
        .ok_or_else(|| eyre!("a v7 coinbase has a coinbase bundle"))?;
    let mut height = bundle.height();
    let mut subsidy = bundle.block_subsidy();
    let mut lockbox = bundle.lockbox_value();
    let (data, poll) = (
        bundle.coinbase_data().to_vec(),
        bundle.miner_poll().cloned(),
    );

    let one = Amount::<NonNegative>::try_from(1)?;
    let deltas = &mut v7_fields.value_deltas;
    let delta = |zatoshis: i64| Amount::<NegativeAllowed>::try_from(zatoshis);
    // The zatoshis that the fault adds to the miner's output.
    let miner_change = match fault {
        CoinbaseFault::FeeBelowBlockFees => {
            deltas.fee = (deltas.fee - one.constrain()?)?;
            -1
        }
        CoinbaseFault::HeightAhead => {
            // A coinbase expires at its own height, so the expiry moves with it.
            height = Height(height.0 + 1);
            *expiry_height = height;
            0
        }
        CoinbaseFault::ExpiryAhead => {
            *expiry_height = Height(height.0 + 1);
            0
        }
        CoinbaseFault::SubsidyBelowSchedule => {
            subsidy = (subsidy - one)?;
            deltas.coinbase = (deltas.coinbase - one.constrain()?)?;
            -1
        }
        CoinbaseFault::LockboxAboveDeposit => {
            lockbox = (lockbox + one)?;
            deltas.coinbase = (deltas.coinbase - one.constrain()?)?;
            -1
        }
        CoinbaseFault::DeltaIgnoresNsmContribution => {
            let ignored = delta(i64::from(subsidy) - i64::from(lockbox))?;
            let contribution = i64::from((ignored - deltas.coinbase)?);
            assert!(
                contribution > 0,
                "the block's fees make a ZIP 235 contribution"
            );
            deltas.coinbase = ignored;
            contribution
        }
        CoinbaseFault::FeeRemovesValue => {
            let removed = delta(-1)?;
            let change = i64::from((removed - deltas.fee)?);
            deltas.fee = removed;
            change
        }
    };
    if miner_change != 0 {
        // The transparent delta pays the coinbase's outputs.
        deltas.transparent = (deltas.transparent - delta(miner_change)?)?;
        let miner = outputs
            .iter_mut()
            .find(|output| output.lock_script == key.address().script())
            .ok_or_else(|| eyre!("the coinbase pays the test key"))?;
        let value = Amount::<NonNegative>::try_from(i64::from(miner.value()) + miner_change)?;
        *miner = Output::new(value, miner.lock_script.clone());
    }
    v7_fields.coinbase = Some(
        CoinbaseBundle::new(height, subsidy, lockbox, data, poll)
            .map_err(|error| eyre!("{error:?}"))?,
    );
    Ok(())
}

/// Returns the block of `template` with its coinbase changed by `edit`, and the header
/// roots recomputed for the changed coinbase.
fn template_block_with_coinbase(
    template: &BlockTemplateResponse,
    net: &Network,
    edit: impl FnOnce(&mut Transaction) -> Result<()>,
) -> Result<Block> {
    let mut block =
        proposal_block_from_template(template, BlockTemplateTimeSource::default(), net)?;
    edit(Arc::make_mut(&mut block.transactions[0]))?;

    let merkle_root: merkle::Root = block.transactions.iter().collect();
    let commitment = ChainHistoryBlockTxAuthCommitmentHash::from_commitments(
        &template.default_roots().chain_history_root(),
        &block.auth_data_root(),
    );
    let header = Arc::make_mut(&mut block.header);
    header.merkle_root = merkle_root;
    header.commitment_bytes = commitment.bytes_in_serialized_order().into();
    Ok(block)
}
