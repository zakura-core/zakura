//! Full block verification for the transparent and mixed workloads that a larger block size
//! makes possible.
//!
//! Each case builds one block from wallet-built transactions, stages the UTXOs the block spends
//! in an on-disk state, and times the block's commit through the production block verifier router
//! until the block is the non-finalized tip. The timed span covers transaction execution (scripts,
//! signatures, and proofs), UTXO reads, the contextual checks, and the commit. The bench network
//! disables proof of work, so the span includes no Equihash or difficulty work.
//!
//! # Workloads
//!
//! - `many_small`: payments with one P2PKH input, one P2PKH payment, and P2PKH change.
//! - `output_heavy`: payout batches with one P2PKH input and many P2SH outputs.
//! - `signature_heavy`: consolidations with many P2PKH inputs and one P2PKH output.
//! - `mixed`: shielding transactions that fill the ZIP 218 Ironwood action limit, with
//!   `many_small` payments in the remaining space.
//!
//! The 20,000 signature-operation limit counts each P2PKH output, but no P2PKH input. A block of
//! P2PKH payments reaches that limit before 3 MB, so `many_small` blocks can stop short of the
//! target size. Each case reports the limit that ended it. `output_heavy` pays P2SH addresses,
//! which have no signature operations, so its blocks reach the target size.
//!
//! # Setup
//!
//! Funding blocks enter the state through the checkpoint path, which does not verify their
//! contents. A funding transaction without inputs creates every UTXO that a workload spends.
//! The state uses pruned storage with checkpoint sync, which stops the optional transparent
//! address and spender indexes.
//!
//! A state keeps its database open until its process exits, so the bench runs each step of a
//! sample in a child process of its own binary. One child commits the setup blocks, and dropping
//! its state flushes RocksDB to disk. A second child opens that state, so RocksDB caches start
//! cold, and times the bench block. The operating system page cache stays warm.
//!
//! `MAX_BLOCK_BYTES` limits decoding blocks from the network and disk. Block verification has no
//! size check, so this bench can verify blocks built in memory that exceed 2 MB.
//!
//! # Configuration
//!
//! ```sh
//! ZAKURA_BENCH_WORKLOADS=many_small,signature_heavy \
//! ZAKURA_BENCH_BLOCK_BYTES=2000000,3000000 \
//! ZAKURA_BENCH_SAMPLES=5 \
//! ZAKURA_BENCH_BACKGROUND_UTXOS=1000000 \
//!   cargo bench -p zakura-consensus --bench block_verification_workloads
//! ```
//!
//! `ZAKURA_BENCH_BACKGROUND_UTXOS` adds unrelated UTXOs to the state, so UTXO reads search a
//! database closer to Mainnet size. It defaults to zero.

// Benchmark results are printed in machine-readable lines.
#![allow(clippy::print_stdout)]

use std::{
    convert::Infallible,
    env,
    hint::black_box,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{Arc, LazyLock},
    time::{Duration, Instant},
};

use chrono::Utc;
use rand_10::Rng;
use sapling_crypto::{
    bundle::GrothProofBytes,
    circuit,
    keys::EphemeralSecretKey,
    prover::{OutputProver, SpendProver},
    value::{NoteValue, ValueCommitTrapdoor},
    Diversifier, MerklePath, PaymentAddress, ProofGenerationKey, Rseed,
};
use tokio::sync::oneshot;
use tower::{Service, ServiceExt};

use zakura_chain::{
    amount::{Amount, NonNegative, MAX_MONEY},
    block::{self, Block, ChainHistoryBlockTxAuthCommitmentHash, Height},
    parameters::{
        subsidy::{block_subsidy, miner_fee_share, miner_subsidy, parent_nsm_value_balance},
        testnet::{ConfiguredActivationHeights, ConfiguredCheckpoints, Parameters},
        Network, NetworkUpgrade, ORCHARD_PROTOCOL_BLOCK_ACTION_LIMIT,
    },
    serialization::{ZcashDeserialize, ZcashSerialize},
    transaction::{LockTime, Transaction},
    transparent,
};
use zakura_consensus::{router, MAX_BLOCK_SIGOPS};
use zakura_node_services::mempool;
use zakura_script::Sigops;
use zakura_state as zs;
use zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES;

use zcash_primitives::transaction::{
    builder::{BuildConfig, Builder, BundlePadding},
    fees::zip317::FeeRule,
};
use zcash_protocol::{consensus::BlockHeight, memo::MemoBytes, value::Zatoshis};
use zcash_transparent::{
    address::TransparentAddress,
    builder::TransparentSigningSet,
    bundle::{OutPoint, TxOut},
};

/// The block size limit proposed by the 3 MB ZIP, and the current limit.
const DEFAULT_BLOCK_BYTES: [usize; 2] = [2_000_000, 3_000_000];
const DEFAULT_SAMPLES: usize = 5;

/// Bytes reserved for the header, the coinbase transaction, and the transaction count.
///
/// The header with its Equihash solution is 1,487 bytes. A transparent coinbase is under 200
/// bytes. The bench asserts the finished block is no larger than its target.
const BLOCK_OVERHEAD_BYTES: usize = 2_000;

/// Transparent outputs in one `output_heavy` payout transaction.
const PAYOUT_OUTPUTS: usize = 200;
/// P2PKH inputs in one `signature_heavy` consolidation transaction.
const CONSOLIDATION_INPUTS: usize = 50;
/// Ironwood actions in a `mixed` block: the ZIP 218 limit, which applies from NU7.
// The limit is 330, so the cast to usize is exact.
const BLOCK_IRONWOOD_ACTIONS: usize = ORCHARD_PROTOCOL_BLOCK_ACTION_LIMIT as usize;
/// A shielding transaction with one Ironwood output pads its bundle to two actions.
const SHIELDING_TX_ACTIONS: usize = 2;

/// The value of each funding UTXO: one ZEC.
const FUNDING_UTXO_ZATOSHIS: u64 = 100_000_000;
/// The value of each payment or payout output.
const PAYMENT_ZATOSHIS: u64 = 10_000;
/// Background UTXOs created by one funding transaction, which keeps funding blocks under 2 MB.
const BACKGROUND_UTXOS_PER_BLOCK: usize = 50_000;

/// Target spacing between setup blocks. The bench block's time follows the last setup block.
const SETUP_BLOCK_SPACING: Duration = Duration::from_secs(75);

/// Bounds waiting for a state or verifier response.
const SERVICE_TIMEOUT: Duration = Duration::from_secs(600);

type Router = tower::buffer::Buffer<
    zakura_consensus::Request,
    futures::future::BoxFuture<'static, Result<block::Hash, zakura_consensus::RouterError>>,
>;
type BoxState = tower::util::BoxService<zs::Request, zs::Response, zakura_consensus::BoxError>;
type StateService = tower::buffer::Buffer<
    zs::Request,
    futures::future::BoxFuture<'static, Result<zs::Response, zakura_consensus::BoxError>>,
>;
type ClosedMempool = tower::buffer::Buffer<
    mempool::Request,
    futures::future::BoxFuture<'static, Result<mempool::Response, zakura_consensus::BoxError>>,
>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Workload {
    ManySmall,
    OutputHeavy,
    SignatureHeavy,
    Mixed,
}

impl Workload {
    const ALL: [Workload; 4] = [
        Workload::ManySmall,
        Workload::OutputHeavy,
        Workload::SignatureHeavy,
        Workload::Mixed,
    ];

    fn name(self) -> &'static str {
        match self {
            Workload::ManySmall => "many_small",
            Workload::OutputHeavy => "output_heavy",
            Workload::SignatureHeavy => "signature_heavy",
            Workload::Mixed => "mixed",
        }
    }

    fn parse(name: &str) -> Self {
        Workload::ALL
            .into_iter()
            .find(|workload| workload.name() == name)
            .unwrap_or_else(|| panic!("unknown workload {name:?}"))
    }

    /// Funding UTXOs that fill a block of `block_bytes` with this workload, rounded up.
    ///
    /// A P2PKH input is at least 148 bytes, so no workload spends more inputs than this.
    fn funding_utxos(self, block_bytes: usize) -> usize {
        let inputs = block_bytes.div_ceil(148);
        match self {
            Workload::Mixed => inputs + BLOCK_IRONWOOD_ACTIONS / SHIELDING_TX_ACTIONS,
            _ => inputs,
        }
    }
}

/// The limit that stopped a block from growing.
#[derive(Clone, Copy, Debug)]
enum FillLimit {
    BlockBytes,
    Sigops,
}

/// A transparent key that signs every input the bench spends.
struct Wallet {
    signing_set: TransparentSigningSet,
    public_key: secp256k1::PublicKey,
    address: TransparentAddress,
}

impl Wallet {
    fn new() -> Self {
        let secret_key = secp256k1::SecretKey::from_secret_bytes([0xcd; 32])
            .expect("a constant nonzero key below the curve order is valid");
        let mut signing_set = TransparentSigningSet::new();
        let public_key = signing_set.add_key(secret_key);
        let address = TransparentAddress::from_pubkey(&public_key);
        Self {
            signing_set,
            public_key,
            address,
        }
    }

    fn lock_script(&self) -> transparent::Script {
        let TransparentAddress::PublicKeyHash(hash) = self.address else {
            unreachable!("the wallet address is derived from a public key");
        };
        // OP_DUP OP_HASH160 <hash> OP_EQUALVERIFY OP_CHECKSIG
        let mut script = vec![0x76, 0xa9, 0x14];
        script.extend_from_slice(&hash);
        script.extend_from_slice(&[0x88, 0xac]);
        transparent::Script::new(&script)
    }
}

/// An unspent output that the wallet controls.
#[derive(Clone, Copy)]
struct Coin {
    outpoint: transparent::OutPoint,
    value: Zatoshis,
}

/// A transaction and the facts the block builder needs about it.
struct BuiltTransaction {
    transaction: Arc<Transaction>,
    bytes: usize,
    sigops: u32,
    fee: Zatoshis,
}

/// What a finished block contains.
struct BlockContents {
    transactions: Vec<BuiltTransaction>,
    fill_limit: FillLimit,
    transparent_inputs: usize,
    transparent_outputs: usize,
    ironwood_actions: usize,
    sigops: u32,
}

/// Selects the step a child process runs. A state keeps its database open until its process
/// exits, so every step that opens a state runs in its own child of the bench binary.
const STEP_ENV: &str = "ZAKURA_BENCH_STEP";
/// The directory where a case's steps exchange blocks and keep their states.
const CASE_DIR_ENV: &str = "ZAKURA_BENCH_CASE_DIR";
/// The state directory of one sample.
const STATE_DIR_ENV: &str = "ZAKURA_BENCH_STATE_DIR";
const WORKLOAD_ENV: &str = "ZAKURA_BENCH_WORKLOAD";
const TARGET_BYTES_ENV: &str = "ZAKURA_BENCH_TARGET_BYTES";
const BACKGROUND_UTXOS_ENV: &str = "ZAKURA_BENCH_BACKGROUND_UTXOS";

/// The file that holds the setup blocks followed by the bench block.
const BLOCKS_FILE: &str = "blocks.bin";
/// The prefix of the line that the measure step prints its result on.
const MEASURED_PREFIX: &str = "verify_commit_seconds=";

fn main() {
    let step = env::var(STEP_ENV);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("the benchmark runtime starts");
    match step.as_deref() {
        Err(_) => run_cases(),
        Ok("template") => runtime.block_on(template_step()),
        Ok("setup") => runtime.block_on(setup_step()),
        Ok("measure") => runtime.block_on(measure_step()),
        Ok(step) => panic!("unknown bench step {step:?}"),
    }
}

/// Runs every configured case and prints one result line for each.
fn run_cases() {
    let workloads = env::var("ZAKURA_BENCH_WORKLOADS").map_or_else(
        |_| Workload::ALL.to_vec(),
        |names| names.split(',').map(Workload::parse).collect(),
    );
    let block_sizes = env::var("ZAKURA_BENCH_BLOCK_BYTES").map_or_else(
        |_| DEFAULT_BLOCK_BYTES.to_vec(),
        |sizes| sizes.split(',').map(parse_env_number).collect(),
    );
    let samples =
        env::var("ZAKURA_BENCH_SAMPLES").map_or(DEFAULT_SAMPLES, |n| parse_env_number(&n));
    let background_utxos = env::var(BACKGROUND_UTXOS_ENV).map_or(0, |n| parse_env_number(&n));
    assert!(samples > 0, "ZAKURA_BENCH_SAMPLES must be positive");

    let scratch = tempfile::Builder::new()
        .prefix("zakura-block-verification-workloads")
        .tempdir_in(env::temp_dir())
        .expect("the scratch directory is created in TMPDIR");

    for workload in workloads {
        for &block_bytes in &block_sizes {
            let case_dir = scratch
                .path()
                .join(format!("{}-{block_bytes}", workload.name()));
            std::fs::create_dir(&case_dir).expect("the case directory is created");
            let case_env = [
                (WORKLOAD_ENV, workload.name().to_string()),
                (TARGET_BYTES_ENV, block_bytes.to_string()),
                (BACKGROUND_UTXOS_ENV, background_utxos.to_string()),
            ];
            let summary = run_step("template", &case_dir, &case_env);

            let mut durations = Vec::with_capacity(samples);
            for sample in 0..samples {
                let state_dir = case_dir.join(format!("sample-{sample}"));
                let sample_env = [(STATE_DIR_ENV, state_dir.display().to_string())];
                run_step("setup", &case_dir, &sample_env);
                let measured = run_step("measure", &case_dir, &sample_env);
                let seconds = measured
                    .lines()
                    .find_map(|line| line.strip_prefix(MEASURED_PREFIX))
                    .expect("the measure step prints its duration")
                    .parse::<f64>()
                    .expect("the measure step prints a number of seconds");
                durations.push(Duration::from_secs_f64(seconds));
                std::fs::remove_dir_all(&state_dir).expect("the sample state is removed");
            }
            std::fs::remove_dir_all(&case_dir).expect("the case directory is removed");

            print_result(summary.trim(), samples, &mut durations);
        }
    }
}

fn parse_env_number(value: &str) -> usize {
    value
        .trim()
        .replace('_', "")
        .parse()
        .unwrap_or_else(|error| panic!("{value:?} is not a number: {error}"))
}

fn env_path(name: &str) -> PathBuf {
    PathBuf::from(env::var(name).unwrap_or_else(|_| panic!("the parent sets {name}")))
}

/// Runs `step` in a child process and returns its standard output.
fn run_step(step: &str, case_dir: &Path, step_env: &[(&str, String)]) -> String {
    let exe = env::current_exe().expect("the bench binary knows its own path");
    let output = Command::new(exe)
        .env(STEP_ENV, step)
        .env(CASE_DIR_ENV, case_dir)
        .envs(step_env.iter().map(|(name, value)| (name, value)))
        .stderr(Stdio::inherit())
        .output()
        .expect("the bench binary starts a child process");
    assert!(
        output.status.success(),
        "the {step} step failed: {}",
        output.status
    );
    String::from_utf8(output.stdout).expect("bench steps print UTF-8")
}

/// Builds the setup blocks and the bench block for one case and writes them to the case
/// directory, then prints the bench block's contents.
///
/// Each header commits to the chain history before it, so this step builds the blocks on a
/// template state that holds their parents. Every sample reuses the blocks.
async fn template_step() {
    let case_dir = env_path(CASE_DIR_ENV);
    let workload = Workload::parse(&env::var(WORKLOAD_ENV).expect("the parent sets the workload"));
    let block_bytes =
        parse_env_number(&env::var(TARGET_BYTES_ENV).expect("the parent sets the size"));
    let background_utxos = parse_env_number(
        &env::var(BACKGROUND_UTXOS_ENV).expect("the parent sets the background UTXOs"),
    );
    let network = bench_network();
    let wallet = Wallet::new();

    let funding_blocks = funding_block_outputs(
        &wallet,
        workload.funding_utxos(block_bytes),
        background_utxos,
    );
    let setup_tip_height = Height(
        u32::try_from(funding_blocks.len()).expect("setup has far fewer blocks than u32::MAX"),
    );
    let template = open_state(&network, &case_dir.join("template"), setup_tip_height).await;
    let read_state = template.read_state;
    let state = buffered(template.state);
    let setup = setup_blocks(&network, &state, &read_state, funding_blocks).await;
    let height = setup_tip_height
        .next()
        .expect("the setup tip is far below the maximum height");

    let build_start = Instant::now();
    let contents = fill_block(
        &network,
        &wallet,
        &setup.coins,
        height,
        workload,
        block_bytes,
    );
    let build_elapsed = build_start.elapsed();
    let block = assemble_block(&network, &state, &read_state, &setup, height, &contents).await;

    let actual_bytes = block.zcash_serialized_size();
    assert!(
        actual_bytes <= block_bytes,
        "the block is {actual_bytes} bytes, more than its {block_bytes} byte target"
    );
    let mut blocks = setup.blocks;
    blocks.push(Arc::new(block));
    write_blocks(&case_dir.join(BLOCKS_FILE), &blocks);

    println!(
        "workload={} target_block_bytes={} actual_block_bytes={} fill_limit={:?} \
         transactions={} transparent_inputs={} transparent_outputs={} ironwood_actions={} \
         sigops={} background_utxos={} build_seconds={:.3}",
        workload.name(),
        block_bytes,
        actual_bytes,
        contents.fill_limit,
        contents.transactions.len() + 1,
        contents.transparent_inputs,
        contents.transparent_outputs,
        contents.ironwood_actions,
        contents.sigops,
        background_utxos,
        build_elapsed.as_secs_f64(),
    );
}

/// Commits the setup blocks to a new state, then drops the state, which flushes RocksDB.
async fn setup_step() {
    let network = bench_network();
    let blocks = read_blocks(&env_path(CASE_DIR_ENV).join(BLOCKS_FILE));
    let (bench_block, setup_blocks) = blocks.split_last().expect("the blocks file is not empty");
    let state = open_state(&network, &env_path(STATE_DIR_ENV), setup_tip(bench_block)).await;
    commit_setup_blocks(state.state, setup_blocks).await;
}

/// Opens the sample's state with cold RocksDB caches, and times the bench block's verification
/// and commit.
async fn measure_step() {
    let network = bench_network();
    let blocks = read_blocks(&env_path(CASE_DIR_ENV).join(BLOCKS_FILE));
    let bench_block = blocks.last().expect("the blocks file is not empty").clone();
    let state = open_state(&network, &env_path(STATE_DIR_ENV), setup_tip(&bench_block)).await;
    let verifier = block_verifier(&network, buffered(state.state), state.read_state).await;

    // A running node has built its verifying key before it verifies a shielded block.
    tokio::task::spawn_blocking(|| {
        LazyLock::force(&zakura_consensus::halo2::VERIFYING_KEY_NU6_3_ONWARD);
    })
    .await
    .expect("building the verifying key does not panic");

    let elapsed = verify_and_commit(verifier, bench_block).await;
    println!("{MEASURED_PREFIX}{:.9}", elapsed.as_secs_f64());
}

/// The height of the setup tip, which is the bench block's parent.
fn setup_tip(bench_block: &Block) -> Height {
    bench_block
        .coinbase_height()
        .and_then(|height| height.previous().ok())
        .expect("the bench block is above genesis")
}

/// Writes `blocks` as headers and length-prefixed transactions.
///
/// Block deserialization stops at `MAX_BLOCK_BYTES`, so a 3 MB block cannot be read back whole.
fn write_blocks(path: &Path, blocks: &[Arc<Block>]) {
    let mut bytes = Vec::new();
    for block in blocks {
        block
            .header
            .zcash_serialize(&mut bytes)
            .expect("writing a header to memory succeeds");
        let count = u32::try_from(block.transactions.len()).expect("a block has few transactions");
        bytes.extend_from_slice(&count.to_le_bytes());
        for transaction in &block.transactions {
            let encoded = transaction
                .zcash_serialize_to_vec()
                .expect("writing a transaction to memory succeeds");
            let len = u32::try_from(encoded.len()).expect("a transaction is under 4 GiB");
            bytes.extend_from_slice(&len.to_le_bytes());
            bytes.extend_from_slice(&encoded);
        }
    }
    std::fs::write(path, bytes).expect("the blocks file is written");
}

fn read_blocks(path: &Path) -> Vec<Arc<Block>> {
    let bytes = std::fs::read(path).expect("the template step wrote the blocks file");
    let mut reader = &bytes[..];
    let mut blocks = Vec::new();
    let read_u32 = |reader: &mut &[u8]| {
        let (value, rest) = reader.split_at(4);
        *reader = rest;
        u32::from_le_bytes(value.try_into().expect("split_at returns four bytes"))
    };
    while !reader.is_empty() {
        let header = block::Header::zcash_deserialize(&mut reader)
            .expect("the template step wrote a valid header");
        let count = read_u32(&mut reader);
        let transactions = (0..count)
            .map(|_| {
                let len = usize::try_from(read_u32(&mut reader)).expect("u32 fits in usize");
                let (encoded, rest) = reader.split_at(len);
                reader = rest;
                Arc::new(
                    Transaction::zcash_deserialize(encoded)
                        .expect("the template step wrote valid transactions"),
                )
            })
            .collect();
        blocks.push(Arc::new(Block {
            header: Arc::new(header),
            transactions,
        }));
    }
    blocks
}

/// A Testnet-like network where every upgrade through NU7 is active from height 1.
///
/// Proof of work, funding streams, and the slow start are disabled, so the coinbase pays the
/// whole block subsidy to the miner and setup blocks need no Equihash solutions.
fn bench_network() -> Network {
    let genesis = genesis_block();
    let target_difficulty_limit = genesis
        .header
        .difficulty_threshold
        .to_expanded()
        .expect("the Mainnet genesis difficulty threshold is valid");

    Parameters::build()
        .with_genesis_hash(genesis.hash())
        .expect("the Mainnet genesis hash is a valid genesis hash")
        .with_checkpoints(ConfiguredCheckpoints::HeightsAndHashes(vec![(
            Height(0),
            genesis.hash(),
        )]))
        .expect("a genesis-only checkpoint list is valid")
        .with_activation_heights(ConfiguredActivationHeights {
            canopy: Some(1),
            nu5: Some(1),
            nu6: Some(1),
            nu6_1: Some(1),
            nu6_2: Some(1),
            nu6_3: Some(1),
            nu7: Some(1),
            ..Default::default()
        })
        .expect("upgrades activating together at height 1 are valid")
        .clear_funding_streams()
        .with_slow_start_interval(Height::MIN)
        .with_disable_pow(true)
        .with_target_difficulty_limit(target_difficulty_limit)
        .expect("the Mainnet genesis difficulty is a valid limit")
        // Funding outputs issue new money, which NU7 takes from the NSM value balance.
        .with_initial_nsm_value_balance(
            Amount::try_from(MAX_MONEY).expect("the money supply is a valid amount"),
        )
        .to_network()
        .expect("the bench network parameters are consistent")
}

fn genesis_block() -> Block {
    Block::zcash_deserialize(&BLOCK_MAINNET_GENESIS_BYTES[..])
        .expect("the Mainnet genesis test vector deserializes")
}

/// The funding blocks after genesis, and the coins the workload can spend.
struct Setup {
    blocks: Vec<Arc<Block>>,
    coins: Vec<Coin>,
}

/// The outputs of each funding block: background blocks first, then the workload's coins.
fn funding_block_outputs(
    wallet: &Wallet,
    funding_utxos: usize,
    background_utxos: usize,
) -> Vec<(usize, transparent::Script)> {
    let background_script = transparent::Script::new(&[0x51]);
    let mut blocks = Vec::new();
    let mut remaining = background_utxos;
    while remaining > 0 {
        let count = remaining.min(BACKGROUND_UTXOS_PER_BLOCK);
        blocks.push((count, background_script.clone()));
        remaining -= count;
    }
    blocks.push((funding_utxos, wallet.lock_script()));
    blocks
}

/// Builds and commits genesis and the funding blocks, and returns the last block's coins.
///
/// Setup blocks enter through the checkpoint path, so a funding transaction needs no inputs.
/// Their times end one spacing before now, so the bench block's time is after them.
async fn setup_blocks(
    network: &Network,
    state: &StateService,
    read_state: &zs::ReadStateService,
    funding_blocks: Vec<(usize, transparent::Script)>,
) -> Setup {
    let genesis = Arc::new(genesis_block());
    commit_checkpoint_block(state, genesis.clone()).await;

    let setup_span = SETUP_BLOCK_SPACING
        * u32::try_from(funding_blocks.len() + 1).expect("setup has few funding blocks");
    let first_time = Utc::now() - setup_span;

    let mut blocks = vec![genesis];
    let mut coins = Vec::new();
    for (index, (count, lock_script)) in funding_blocks.into_iter().enumerate() {
        let height = Height(u32::try_from(index + 1).expect("setup has few funding blocks"));
        let funding = Arc::new(Transaction::V6 {
            network_upgrade: NetworkUpgrade::current(network, height),
            lock_time: LockTime::unlocked(),
            expiry_height: Height(0),
            inputs: Vec::new(),
            outputs: vec![
                transparent::Output {
                    value: Amount::try_from(FUNDING_UTXO_ZATOSHIS)
                        .expect("one ZEC is a valid amount"),
                    lock_script,
                };
                count
            ],
            sapling_shielded_data: None,
            orchard_shielded_data: None,
            ironwood_shielded_data: None,
        });
        let coinbase = Arc::new(coinbase_transaction(
            network,
            height,
            Amount::zero(),
            format!("bench setup {index}").into_bytes(),
        ));
        let parent = blocks.last().expect("setup starts with genesis");
        let mut block = child_block(parent, vec![coinbase, funding.clone()]);
        complete_header(read_state, parent, &mut block).await;
        Arc::make_mut(&mut block.header).time =
            first_time + SETUP_BLOCK_SPACING * u32::try_from(index + 1).expect("few blocks");
        let block = Arc::new(block);
        commit_checkpoint_block(state, block.clone()).await;

        let funding_hash = funding.hash();
        coins = (0..count)
            .map(|output_index| Coin {
                outpoint: transparent::OutPoint {
                    hash: funding_hash,
                    index: u32::try_from(output_index)
                        .expect("a block has fewer than u32::MAX outputs"),
                },
                value: Zatoshis::const_from_u64(FUNDING_UTXO_ZATOSHIS),
            })
            .collect();
        blocks.push(block);
    }

    Setup { blocks, coins }
}

/// Returns a block on `parent` whose header commits to `transactions`.
///
/// [`complete_header`] sets the fields that depend on the chain, and the caller sets the time.
fn child_block(parent: &Block, transactions: Vec<Arc<Transaction>>) -> Block {
    let mut header = *parent.header;
    header.previous_block_hash = parent.hash();
    header.merkle_root = transactions.iter().collect();
    Block {
        header: Arc::new(header),
        transactions,
    }
}

/// Sets the difficulty and block commitment that the chain after `parent` expects, and returns
/// the chain info they came from.
///
/// The commitment covers the chain history and the block's authorizing data, so the block's
/// transactions must be final. It does not cover the block time.
async fn complete_header(
    read_state: &zs::ReadStateService,
    parent: &Block,
    block: &mut Block,
) -> zs::GetBlockTemplateChainInfo {
    let zs::ReadResponse::ChainInfo(chain_info) = read_state
        .clone()
        .oneshot(zs::ReadRequest::ChainInfo)
        .await
        .expect("the state answers chain info requests")
    else {
        unreachable!("the state answers ChainInfo with ChainInfo");
    };
    assert_eq!(
        chain_info.tip_hash,
        parent.hash(),
        "the block extends the state tip"
    );

    let history_root = chain_info
        .chain_history_root
        .unwrap_or_else(|| block::CHAIN_HISTORY_ACTIVATION_RESERVED.into());
    let commitment = ChainHistoryBlockTxAuthCommitmentHash::from_commitments(
        &history_root,
        &block.auth_data_root(),
    );
    let header = Arc::make_mut(&mut block.header);
    header.difficulty_threshold = chain_info.expected_difficulty;
    header.commitment_bytes = <[u8; 32]>::from(commitment).into();
    chain_info
}

/// A coinbase transaction that pays `value` to an unspendable script.
fn coinbase_transaction(
    network: &Network,
    height: Height,
    value: Amount<NonNegative>,
    data: Vec<u8>,
) -> Transaction {
    Transaction::V6 {
        network_upgrade: NetworkUpgrade::current(network, height),
        lock_time: LockTime::unlocked(),
        expiry_height: height,
        inputs: vec![transparent::Input::Coinbase {
            height,
            data,
            sequence: u32::MAX,
        }],
        outputs: vec![transparent::Output {
            value,
            lock_script: transparent::Script::new(&[0x6a]),
        }],
        sapling_shielded_data: None,
        orchard_shielded_data: None,
        ironwood_shielded_data: None,
    }
}

/// Builds transactions for `workload` until the next one would exceed a block limit.
fn fill_block(
    network: &Network,
    wallet: &Wallet,
    coins: &[Coin],
    height: Height,
    workload: Workload,
    block_bytes: usize,
) -> BlockContents {
    let budget = block_bytes - BLOCK_OVERHEAD_BYTES;
    let mut coins = coins.iter().copied();
    let mut contents = BlockContents {
        transactions: Vec::new(),
        fill_limit: FillLimit::BlockBytes,
        transparent_inputs: 0,
        transparent_outputs: 0,
        ironwood_actions: 0,
        sigops: 0,
    };
    let mut bytes = 0;

    // The coinbase pays an OP_RETURN script, which has no sigops, so only these count.
    let mut try_add = |contents: &mut BlockContents, built: BuiltTransaction| {
        if bytes + built.bytes > budget {
            return Err(FillLimit::BlockBytes);
        }
        if contents.sigops + built.sigops > MAX_BLOCK_SIGOPS {
            return Err(FillLimit::Sigops);
        }
        bytes += built.bytes;
        contents.sigops += built.sigops;
        contents.transparent_inputs += built.transaction.inputs().len();
        contents.transparent_outputs += built.transaction.outputs().len();
        contents.ironwood_actions +=
            usize::try_from(built.transaction.shielded_action_counts().ironwood_actions)
                .expect("action counts fit in usize");
        contents.transactions.push(built);
        Ok(())
    };

    let mut shielding_txs = match workload {
        Workload::Mixed => BLOCK_IRONWOOD_ACTIONS / SHIELDING_TX_ACTIONS,
        _ => 0,
    };
    loop {
        let shield = shielding_txs > 0;
        shielding_txs = shielding_txs.saturating_sub(1);
        let (inputs, payments) = match workload {
            _ if shield => (1, Vec::new()),
            Workload::ManySmall | Workload::Mixed => (1, vec![wallet.address]),
            Workload::OutputHeavy => (1, vec![p2sh_address(); PAYOUT_OUTPUTS]),
            Workload::SignatureHeavy => (CONSOLIDATION_INPUTS, Vec::new()),
        };
        let inputs: Vec<Coin> = coins.by_ref().take(inputs).collect();
        assert!(
            !inputs.is_empty(),
            "the workload ran out of funding UTXOs before filling the block"
        );
        let built = build_transaction(network, wallet, height, &inputs, &payments, shield);
        if let Err(limit) = try_add(&mut contents, built) {
            contents.fill_limit = limit;
            return contents;
        }
    }
}

fn p2sh_address() -> TransparentAddress {
    TransparentAddress::ScriptHash([0x42; 20])
}

/// Builds and signs a V6 transaction that spends `inputs`, pays each of `payments`, and returns
/// the rest to the wallet after the ZIP 317 fee. With `shield`, the rest goes to an Ironwood
/// address instead of the wallet.
fn build_transaction(
    network: &Network,
    wallet: &Wallet,
    height: Height,
    inputs: &[Coin],
    payments: &[TransparentAddress],
    shield: bool,
) -> BuiltTransaction {
    let fee_rule = FeeRule::standard();
    let payment_value = Zatoshis::const_from_u64(PAYMENT_ZATOSHIS);
    let input_value = inputs
        .iter()
        .map(|coin| coin.value)
        .sum::<Option<Zatoshis>>()
        .expect("the bench spends far less than the money supply");

    // The fee depends on the number and size of inputs and outputs, not on their values, so a
    // builder with a placeholder change value reports the fee for the real transaction.
    let builder_with_change = |change: Zatoshis| {
        let mut builder = Builder::new(
            network,
            BlockHeight::from(height),
            BuildConfig::Standard {
                sapling_anchor: None,
                orchard_anchor: None,
                ironwood_anchor: shield.then(orchard::Anchor::empty_tree),
                orchard_padding: BundlePadding::DEFAULT,
                ironwood_padding: BundlePadding::DEFAULT,
            },
        );
        for coin in inputs {
            builder
                .add_transparent_p2pkh_input(
                    wallet.public_key,
                    OutPoint::new(coin.outpoint.hash.0, coin.outpoint.index),
                    TxOut::new(coin.value, wallet.address.script().into()),
                )
                .expect("the wallet key controls every P2PKH coin");
        }
        for address in payments {
            builder
                .add_transparent_output(address, payment_value)
                .expect("payment outputs are valid");
        }
        if shield {
            builder
                .add_ironwood_output::<Infallible>(
                    None,
                    ironwood_recipient(),
                    change,
                    MemoBytes::empty(),
                )
                .expect("the bench network supports Ironwood outputs at the bench height");
        } else {
            builder
                .add_transparent_output(&wallet.address, change)
                .expect("the change output is valid");
        }
        builder
    };

    let fee = builder_with_change(Zatoshis::const_from_u64(1))
        .get_fee(&fee_rule)
        .expect("the ZIP 317 fee of a small transaction is computable");
    let payments_value = (payment_value * u64::try_from(payments.len()).expect("few payments"))
        .expect("payments are far below the money supply");
    let change = ((input_value - payments_value).and_then(|rest| rest - fee))
        .expect("funding coins cover the payments and the fee");

    let result = builder_with_change(change)
        .build(
            &wallet.signing_set,
            &[],
            &[],
            rand_10::rng(),
            &NoSaplingProver,
            &NoSaplingProver,
            &fee_rule,
        )
        .expect("the wallet builds and signs the transaction");
    let built = result.transaction();
    let sigops = built.sigops();
    let mut bytes = Vec::new();
    built
        .write(&mut bytes)
        .expect("writing a transaction to memory succeeds");
    let transaction = Transaction::zcash_deserialize(&bytes[..])
        .expect("a transaction from the zcash_primitives builder deserializes");

    BuiltTransaction {
        transaction: Arc::new(transaction),
        bytes: bytes.len(),
        sigops,
        fee,
    }
}

fn ironwood_recipient() -> orchard::Address {
    let spending_key = orchard::keys::SpendingKey::from_bytes([7; 32])
        .expect("the constant spending key is valid");
    orchard::keys::FullViewingKey::from(&spending_key).address(
        orchard::keys::Diversifier::from_bytes([0; 11]),
        orchard::keys::Scope::External,
    )
}

/// Adds the coinbase and a header that commits to the chain after the setup blocks.
async fn assemble_block(
    network: &Network,
    state: &StateService,
    read_state: &zs::ReadStateService,
    setup: &Setup,
    height: Height,
    contents: &BlockContents,
) -> Block {
    let parent = setup.blocks.last().expect("setup starts with genesis");

    let zs::Response::BlockInfo(Some(parent_info)) = state
        .clone()
        .oneshot(zs::Request::BlockInfo(parent.hash()))
        .await
        .expect("the state answers block info requests")
    else {
        unreachable!("the committed parent has block info");
    };
    let nsm_value_balance =
        parent_nsm_value_balance(parent_info.value_pools().nsm_value_balance_amount())
            .expect("the setup chain leaves a valid NSM value balance");

    let fees: u64 = contents
        .transactions
        .iter()
        .map(|tx| tx.fee.into_u64())
        .sum();
    let fees =
        Amount::<NonNegative>::try_from(fees).expect("block fees are far below the money supply");
    let subsidy = block_subsidy(height, network, Some(nsm_value_balance))
        .expect("the bench height has a block subsidy");
    let miner_reward = (miner_subsidy(height, network, subsidy)
        .expect("funding streams are disabled, so the miner receives the subsidy")
        + miner_fee_share(height, network, fees))
    .expect("the miner reward is far below the money supply");

    let coinbase = Arc::new(coinbase_transaction(
        network,
        height,
        miner_reward,
        b"block verification workloads".to_vec(),
    ));
    let transactions = std::iter::once(coinbase)
        .chain(
            contents
                .transactions
                .iter()
                .map(|tx| tx.transaction.clone()),
        )
        .collect();

    let mut block = child_block(parent, transactions);
    let chain_info = complete_header(read_state, parent, &mut block).await;
    let time = chain_info
        .cur_time
        .max(chain_info.min_time)
        .min(chain_info.max_time);
    Arc::make_mut(&mut block.header).time = time.into();
    block
}

async fn open_state(
    network: &Network,
    path: &Path,
    max_checkpoint_height: Height,
) -> zs::InitializedState<BoxState> {
    let config = zs::Config {
        cache_dir: path.to_path_buf(),
        ephemeral: false,
        checkpoint_sync: true,
        storage_mode: zs::StorageMode::Pruned(zs::PruningConfig::default()),
        ..zs::Config::default()
    };
    zs::init(config, network, max_checkpoint_height, 0)
        .await
        .expect("the bench state opens in its scratch directory")
}

fn buffered(state: BoxState) -> StateService {
    tower::buffer::Buffer::new(state, 1)
}

async fn commit_checkpoint_block(state: &StateService, block: Arc<Block>) {
    tokio::time::timeout(
        SERVICE_TIMEOUT,
        state
            .clone()
            .oneshot(zs::Request::CommitCheckpointVerifiedBlock(block.into())),
    )
    .await
    .expect("setup blocks commit before the timeout")
    .expect("the checkpoint path commits setup blocks");
}

/// Commits the setup blocks, then drops the state, which flushes its database to disk.
async fn commit_setup_blocks(mut state: BoxState, blocks: &[Arc<Block>]) {
    let mut commits = Vec::with_capacity(blocks.len());
    for block in blocks {
        let commit = state
            .ready()
            .await
            .expect("the state service is ready")
            .call(zs::Request::CommitCheckpointVerifiedBlock(
                block.clone().into(),
            ));
        commits.push(commit);
    }
    for commit in commits {
        tokio::time::timeout(SERVICE_TIMEOUT, commit)
            .await
            .expect("setup blocks commit before the timeout")
            .expect("the checkpoint path commits setup blocks");
    }
}

/// Builds the production verifier router over `state`, with checkpoint sync ending at genesis.
async fn block_verifier(
    network: &Network,
    state: StateService,
    read_state: zs::ReadStateService,
) -> Router {
    let config = zakura_consensus::Config {
        checkpoint_sync: true,
        ..zakura_consensus::Config::default()
    };
    let (router, _transaction_verifier, _tasks, _max_checkpoint_height) =
        router::init_with_read_state(
            config,
            network,
            state,
            read_state,
            oneshot::channel::<ClosedMempool>().1,
        )
        .await;
    router
}

/// Returns the time from submitting `block` until it is the non-finalized tip.
async fn verify_and_commit(verifier: Router, block: Arc<Block>) -> Duration {
    let start = Instant::now();
    let hash = tokio::time::timeout(
        SERVICE_TIMEOUT,
        verifier.oneshot(zakura_consensus::Request::Commit(block.into())),
    )
    .await
    .expect("the block verifies before the timeout")
    .expect("the workload block is valid");
    let elapsed = start.elapsed();
    black_box(hash);
    elapsed
}

fn print_result(summary: &str, samples: usize, durations: &mut [Duration]) {
    durations.sort();
    println!(
        "block_verification_workloads: {summary} samples={samples} \
         verify_commit_median_seconds={:.6} verify_commit_min_seconds={:.6} \
         verify_commit_max_seconds={:.6}",
        durations[durations.len() / 2].as_secs_f64(),
        durations[0].as_secs_f64(),
        durations[durations.len() - 1].as_secs_f64(),
    );
}

/// Satisfies the transaction builder's prover bounds without Sapling proving parameters.
///
/// The bench builds only transparent and Ironwood transactions, so the builder never calls these
/// methods. The uninhabited proof type keeps this adapter from returning a placeholder proof.
struct NoSaplingProver;

impl SpendProver for NoSaplingProver {
    type Proof = Infallible;

    fn prepare_circuit(
        _proof_generation_key: ProofGenerationKey,
        _diversifier: Diversifier,
        _rseed: Rseed,
        _value: NoteValue,
        _alpha: jubjub::Fr,
        _rcv: ValueCommitTrapdoor,
        _anchor: bls12_381::Scalar,
        _merkle_path: MerklePath,
    ) -> Option<circuit::Spend> {
        unreachable!("the bench never adds Sapling spends")
    }

    fn create_proof<R: Rng>(&self, _circuit: circuit::Spend, _rng: &mut R) -> Self::Proof {
        unreachable!("the bench never adds Sapling spends")
    }

    fn encode_proof(proof: Self::Proof) -> GrothProofBytes {
        match proof {}
    }
}

impl OutputProver for NoSaplingProver {
    type Proof = Infallible;

    fn prepare_circuit(
        _esk: &EphemeralSecretKey,
        _payment_address: PaymentAddress,
        _rcm: jubjub::Fr,
        _value: NoteValue,
        _rcv: ValueCommitTrapdoor,
    ) -> circuit::Output {
        unreachable!("the bench never adds Sapling outputs")
    }

    fn create_proof<R: Rng>(&self, _circuit: circuit::Output, _rng: &mut R) -> Self::Proof {
        unreachable!("the bench never adds Sapling outputs")
    }

    fn encode_proof(proof: Self::Proof) -> GrothProofBytes {
        match proof {}
    }
}
