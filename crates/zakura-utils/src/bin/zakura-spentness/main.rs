//! Generate and audit external spentness artifacts from immutable archive states.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::{
    fs::{self, File},
    path::{Path, PathBuf},
};

use clap::{Parser, Subcommand};
use color_eyre::eyre::{ensure, eyre, Context, Result};
use rand::RngCore;
use sha2::{Digest, Sha256};
use zakura_chain::{
    block::{self, Height},
    common::atomic_write,
    parameters::{
        spentness_hints::{Commitment, Encoder, ParsedArtifact, VerifiedArtifact},
        Network,
    },
    serialization::{ZcashDeserializeInto, ZcashSerialize},
    transparent::{OrderedUtxo, OutPoint, MIN_TRANSPARENT_COINBASE_MATURITY},
};
use zakura_state::{Config, FinalizedState, OutputLocation, ZakuraDb};

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Provision a local artifact only when this binary recognizes its release commitment.
    Install {
        #[arg(long)]
        artifact: PathBuf,
        #[arg(long)]
        cache: PathBuf,
    },
    /// Build or advance a separate ordinary archive state to exactly H.
    Replay {
        #[arg(long)]
        source: PathBuf,
        #[arg(long)]
        destination: PathBuf,
        #[arg(long)]
        height: u32,
        #[arg(long)]
        block_hash: block::Hash,
    },
    /// Generate membership by merging canonical outputs with the exact-H UTXO set.
    Generate {
        #[arg(long)]
        state: PathBuf,
        #[arg(long)]
        height: u32,
        #[arg(long)]
        block_hash: block::Hash,
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        commitment: PathBuf,
    },
    /// Compare complete entries with a separately validated exact-H archive state.
    Verify {
        #[arg(long)]
        state: PathBuf,
        #[arg(long)]
        artifact: PathBuf,
        #[arg(long)]
        commitment: PathBuf,
        /// Machine-readable verification evidence for release tooling.
        #[arg(long)]
        report: Option<PathBuf>,
    },
}

fn config(path: &Path) -> Config {
    Config {
        cache_dir: path.to_owned(),
        delete_old_database: false,
        vct_fast_sync: false,
        ..Config::default()
    }
}

fn open(path: &Path) -> Result<ZakuraDb> {
    let (_, db, _) = zakura_state::init_read_only(config(path), &Network::Mainnet)?;
    Ok(db)
}

fn exact_boundary(db: &ZakuraDb, height: u32, hash: block::Hash) -> Result<[u8; 32]> {
    ensure!(
        db.tip() == Some((Height(height), hash)),
        "archive tip must equal the requested H/hash"
    );
    let genesis = db
        .hash(Height(0))
        .ok_or_else(|| eyre!("archive has no genesis"))?;
    ensure!(
        Some(genesis) == Network::Mainnet.checkpoint_list().hash(Height(0)),
        "archive chain identity differs from Mainnet"
    );
    Ok(genesis.0)
}

fn write(path: PathBuf, bytes: &[u8]) -> Result<()> {
    atomic_write(path, bytes)??;
    Ok(())
}

fn replay(source: &Path, destination: &Path, height: u32, hash: block::Hash) -> Result<()> {
    fs::create_dir_all(destination)?;
    ensure!(
        fs::canonicalize(source)? != fs::canonicalize(destination)?,
        "replay destination must differ from source"
    );
    let source = open(source)?;
    ensure!(
        source.hash(Height(height)) == Some(hash),
        "source does not contain the selected H/hash"
    );
    let mut state = FinalizedState::new(&config(destination), &Network::Mainnet)?;
    let start = match state.db.tip() {
        Some((tip, tip_hash)) => {
            ensure!(
                tip.0 <= height && source.hash(tip) == Some(tip_hash),
                "replay state has an incompatible boundary; choose a fresh destination"
            );
            tip.0
                .checked_add(1)
                .ok_or_else(|| eyre!("replay height overflow"))?
        }
        None => 0,
    };
    let mut trees = None;
    for h in start..=height {
        let block = source
            .block(Height(h).into())
            .ok_or_else(|| eyre!("source lacks retained block {h}"))?;
        ensure!(
            source.hash(Height(h)) == Some(block.hash()),
            "source block hash mismatch at {h}"
        );
        let (_, next_trees) =
            state.commit_finalized_direct(block.into(), trees, None, "spentness archive replay")?;
        trees = Some(next_trees);
        if h % 10_000 == 0 {
            eprintln!("replayed {h}/{height}");
        }
    }
    exact_boundary(&state.db, height, hash)?;
    Ok(())
}

fn generate(db: &ZakuraDb, height: u32, hash: block::Hash) -> Result<(Vec<u8>, Commitment, u64)> {
    let identity = exact_boundary(db, height, hash)?;
    let mut encoder = Encoder::new(identity, height, hash.0);
    let mut utxos = db.utxos_by_location().peekable();
    let mut survivors = 0u64;
    for h in 0..=height {
        let block = db
            .block(Height(h).into())
            .ok_or_else(|| eyre!("missing block {h}"))?;
        ensure!(
            db.hash(Height(h)) == Some(block.hash()),
            "canonical hash mismatch at {h}"
        );
        for (tx_index, tx) in block.transactions.iter().enumerate() {
            for (output_index, output) in tx.outputs().iter().enumerate() {
                let location = OutputLocation::from_usize(Height(h), tx_index, output_index);
                ensure!(
                    utxos.peek().is_none_or(|(next, _)| *next >= location),
                    "unmatched UTXO before {location:?}"
                );
                let retained = utxos.peek().is_some_and(|(next, _)| *next == location);
                if retained {
                    let (_, entry) = utxos.next().ok_or_else(|| eyre!("UTXO iterator changed"))?;
                    ensure!(h != 0, "genesis outputs must not enter the UTXO set");
                    ensure!(
                        entry == OrderedUtxo::new(output.clone(), Height(h), tx_index),
                        "UTXO entry differs from its creating transaction"
                    );
                    survivors = survivors
                        .checked_add(1)
                        .ok_or_else(|| eyre!("survivor count overflow"))?;
                }
                encoder.push(retained)?;
            }
        }
    }
    ensure!(
        utxos.next().is_none(),
        "UTXO set contains unmatched entries beyond canonical outputs"
    );
    let bytes = encoder.finish();
    let pin = ParsedArtifact::read(bytes.as_slice())?.commitment().clone();
    Ok((bytes, pin, survivors))
}

/// Addition modulo 2^256 preserves duplicate multiplicity, unlike XOR.
fn add(sum: &mut [u8; 32], value: [u8; 32]) {
    let mut carry = 0u16;
    for (byte, value) in sum.iter_mut().zip(value) {
        let next = u16::from(*byte) + u16::from(value) + carry;
        *byte = next.to_le_bytes()[0];
        carry = next >> 8;
    }
}

fn outpoint_hash(salt: &[u8; 32], outpoint: OutPoint) -> Result<[u8; 32]> {
    let mut hash = Sha256::new();
    hash.update(b"Zakura spentness audit v1\0");
    hash.update(salt);
    hash.update(outpoint.zcash_serialize_to_vec()?);
    Ok(hash.finalize().into())
}

fn entry_bytes(entry: &OrderedUtxo) -> Result<Vec<u8>> {
    let mut bytes = entry.utxo.height.0.to_le_bytes().to_vec();
    bytes.push(u8::from(entry.utxo.from_coinbase));
    bytes.extend_from_slice(&entry.utxo.output.zcash_serialize_to_vec()?);
    Ok(bytes)
}

fn replay_transaction(
    replay: &rocksdb::DB,
    height: Height,
    tx_index: usize,
    tx: &zakura_chain::transaction::Transaction,
) -> Result<()> {
    for input in tx.inputs() {
        if let Some(outpoint) = input.outpoint() {
            let key = outpoint.zcash_serialize_to_vec()?;
            let entry = replay.get(&key)?.ok_or_else(|| {
                eyre!("oracle found a missing, future, or duplicate spend: {outpoint:?}")
            })?;
            let creation_height = u32::from_le_bytes(entry[..4].try_into()?);
            ensure!(
                entry[4] == 0
                    || creation_height
                        .checked_add(MIN_TRANSPARENT_COINBASE_MATURITY)
                        .is_some_and(|mature| height.0 >= mature),
                "oracle found an immature coinbase spend"
            );
            replay.delete(key)?;
        }
    }
    let hash = tx.hash();
    for (index, output) in tx.outputs().iter().enumerate() {
        let outpoint = OutPoint {
            hash,
            index: u32::try_from(index)?,
        };
        let key = outpoint.zcash_serialize_to_vec()?;
        ensure!(
            replay.get(&key)?.is_none(),
            "oracle found a duplicate output"
        );
        replay.put(
            key,
            entry_bytes(&OrderedUtxo::new(output.clone(), height, tx_index))?,
        )?;
    }
    Ok(())
}

/// Independent transparent replay: no state write helpers or artifact bits.
fn replay_transparent(db: &ZakuraDb, height: u32, replay: &rocksdb::DB) -> Result<()> {
    for h in 1..=height {
        let block = db
            .block(Height(h).into())
            .ok_or_else(|| eyre!("oracle lacks block {h}"))?;
        for (tx_index, tx) in block.transactions.iter().enumerate() {
            replay_transaction(replay, Height(h), tx_index, tx)?;
        }
    }
    let mut count = 0usize;
    for row in replay.iterator(rocksdb::IteratorMode::Start) {
        let (key, expected) = row?;
        let outpoint = key.as_ref().zcash_deserialize_into::<OutPoint>()?;
        let entry = db
            .utxo(&outpoint)
            .ok_or_else(|| eyre!("ordinary state omitted an oracle survivor"))?;
        ensure!(
            entry_bytes(&entry)?.as_slice() == expected.as_ref(),
            "ordinary state differs from complete oracle UTXO entry"
        );
        count = count
            .checked_add(1)
            .ok_or_else(|| eyre!("oracle UTXO count overflow"))?;
    }
    ensure!(
        db.utxos_by_location().count() == count,
        "ordinary state has unmatched UTXOs"
    );
    Ok(())
}

fn verify(db: &ZakuraDb, artifact: &VerifiedArtifact) -> Result<u64> {
    let pin = artifact.commitment();
    ensure!(
        exact_boundary(
            db,
            pin.terminal_height,
            block::Hash(pin.terminal_block_hash)
        )? == pin.chain_identity,
        "chain identity mismatch"
    );
    let scratch_root = std::env::var_os("TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| Config::default().cache_dir.join("spentness-audit"));
    fs::create_dir_all(&scratch_root)?;
    let scratch = tempfile::Builder::new()
        .prefix("spentness-audit-")
        .tempdir_in(scratch_root)?;
    let mut options = rocksdb::Options::default();
    options.create_if_missing(true);
    options.set_write_buffer_size(16 * 1024 * 1024);
    options.set_max_write_buffer_number(2);
    let replay = rocksdb::DB::open(&options, scratch.path())?;
    replay_transparent(db, pin.terminal_height, &replay)?;
    let mut salt = [0; 32];
    rand::rngs::OsRng.fill_bytes(&mut salt);
    let mut absent = [0; 32];
    let mut inputs = [0; 32];
    let mut ordinal = 0u64;
    let mut survivors = 0u64;
    for h in 0..=pin.terminal_height {
        let block = db
            .block(Height(h).into())
            .ok_or_else(|| eyre!("oracle lacks block {h}"))?;
        ensure!(
            db.hash(Height(h)) == Some(block.hash()),
            "oracle canonical hash mismatch at {h}"
        );
        for (tx_index, tx) in block.transactions.iter().enumerate() {
            let tx_hash = tx.hash();
            for input in tx.inputs() {
                if let Some(outpoint) = input.outpoint() {
                    add(&mut inputs, outpoint_hash(&salt, outpoint)?);
                }
            }
            for (index, output) in tx.outputs().iter().enumerate() {
                let outpoint = OutPoint {
                    hash: tx_hash,
                    index: u32::try_from(index)?,
                };
                // This oracle uses outpoint lookups, independently of the generator's merge.
                let entry = db.utxo(&outpoint);
                let retained = artifact.retains(ordinal)?;
                ensure!(
                    retained == entry.is_some(),
                    "wrong survivor bit at ordinal {ordinal}"
                );
                if let Some(entry) = entry {
                    ensure!(
                        h != 0 && entry == OrderedUtxo::new(output.clone(), Height(h), tx_index),
                        "oracle UTXO entry differs at {outpoint:?}"
                    );
                    survivors += 1;
                } else if h != 0 {
                    add(&mut absent, outpoint_hash(&salt, outpoint)?);
                }
                ordinal = ordinal
                    .checked_add(1)
                    .ok_or_else(|| eyre!("ordinal overflow"))?;
            }
        }
    }
    ensure!(
        ordinal == pin.output_count,
        "output count differs from artifact"
    );
    ensure!(
        u64::try_from(db.utxos_by_location().count())? == survivors,
        "oracle contains unmatched UTXOs"
    );
    ensure!(
        absent == inputs,
        "salted spent-output/input multiset mismatch"
    );
    Ok(survivors)
}

fn main() -> Result<()> {
    color_eyre::install()?;
    zakura_utils::init_tracing();
    match Args::parse().command {
        Command::Install { artifact, cache } => {
            let parsed = ParsedArtifact::read(File::open(artifact)?)?;
            let pin = zakura_chain::parameters::spentness_hints::MAINNET_COMMITMENTS.iter()
                .find(|pin| *pin == parsed.commitment()).ok_or_else(|| eyre!("this binary does not recognize the artifact; install a release with its reviewed commitment"))?;
            let verified = parsed.verify(pin)?;
            write(
                cache.join(format!("{}.bin", hex::encode(pin.sha256))),
                verified.bytes(),
            )?;
            Ok(())
        }
        Command::Replay {
            source,
            destination,
            height,
            block_hash,
        } => replay(&source, &destination, height, block_hash),
        Command::Generate {
            state,
            height,
            block_hash,
            output,
            commitment,
        } => {
            let (bytes, pin, survivors) = generate(&open(&state)?, height, block_hash)?;
            write(output, &bytes)?;
            write(commitment, &serde_json::to_vec_pretty(&pin)?)?;
            println!(
                "outputs={} survivors={survivors} bytes={} sha256={}",
                pin.output_count,
                pin.byte_len,
                hex::encode(pin.sha256)
            );
            Ok(())
        }
        Command::Verify {
            state,
            artifact,
            commitment,
            report,
        } => {
            let pin: Commitment = serde_json::from_reader(File::open(commitment)?)?;
            let artifact = VerifiedArtifact::read(File::open(artifact)?, &pin)
                .wrap_err("authenticating artifact")?;
            let survivors = verify(&open(&state)?, &artifact)?;
            if let Some(report) = report {
                write(
                    report,
                    &serde_json::to_vec_pretty(&serde_json::json!({
                        "schema_version": 1,
                        "commitment": pin,
                        "survivor_count": survivors,
                        "oracle": "transparent-replay-v1",
                        "complete_entries": true,
                        "salted_multiset": true,
                    }))?,
                )?;
            }
            println!(
                "verified outputs={} survivors={survivors} sha256={}",
                pin.output_count,
                hex::encode(pin.sha256)
            );
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zakura_chain::serialization::ZcashDeserializeInto;

    #[test]
    fn oracle_enforces_spend_order_maturity_and_complete_entries() -> Result<()> {
        use zakura_chain::{
            transaction::{LockTime, Transaction},
            transparent::{Input, Output, Script},
        };
        let directory = tempfile::tempdir()?;
        let replay = rocksdb::DB::open_default(directory.path())?;
        let output = Output::new(1u64.try_into()?, Script::new(&[0x6a]));
        let funding = Transaction::V1 {
            inputs: vec![Input::Coinbase {
                height: Height(1),
                data: vec![],
                sequence: 0,
            }],
            outputs: vec![output.clone()],
            lock_time: LockTime::unlocked(),
        };
        let outpoint = OutPoint {
            hash: funding.hash(),
            index: 0,
        };
        let spend = Transaction::V1 {
            inputs: vec![Input::PrevOut {
                outpoint,
                unlock_script: Script::new(&[]),
                sequence: 0,
            }],
            outputs: vec![output.clone()],
            lock_time: LockTime::unlocked(),
        };
        ensure!(
            replay_transaction(&replay, Height(101), 1, &spend).is_err(),
            "future output was accepted"
        );
        replay_transaction(&replay, Height(1), 0, &funding)?;
        ensure!(
            replay.get(outpoint.zcash_serialize_to_vec()?)?.is_some(),
            "non-address scripts must remain in the UTXO set"
        );
        ensure!(
            replay_transaction(&replay, Height(100), 1, &spend).is_err(),
            "immature coinbase was accepted"
        );
        replay_transaction(&replay, Height(101), 1, &spend)?;
        ensure!(
            replay_transaction(&replay, Height(101), 2, &spend).is_err(),
            "duplicate spend was accepted"
        );
        let same_block_outpoint = OutPoint {
            hash: spend.hash(),
            index: 0,
        };
        let entry = replay
            .get(same_block_outpoint.zcash_serialize_to_vec()?)?
            .ok_or_else(|| eyre!("survivor missing"))?;
        ensure!(
            entry == entry_bytes(&OrderedUtxo::new(output, Height(101), 1))?,
            "value, script, creation height, or coinbase flag differs"
        );
        let same_block_spend = Transaction::V1 {
            inputs: vec![Input::PrevOut {
                outpoint: same_block_outpoint,
                unlock_script: Script::new(&[]),
                sequence: 0,
            }],
            outputs: vec![],
            lock_time: LockTime::unlocked(),
        };
        replay_transaction(&replay, Height(101), 2, &same_block_spend)?;
        ensure!(
            replay
                .iterator(rocksdb::IteratorMode::Start)
                .next()
                .is_none(),
            "same-block spend left an output"
        );
        Ok(())
    }

    #[test]
    fn ordinary_state_generation_verification_and_exact_boundary() -> Result<()> {
        let _guard = zakura_test::init();
        let source_dir = tempfile::tempdir()?;
        let replay_dir = tempfile::tempdir()?;
        let mut source = FinalizedState::new(&config(source_dir.path()), &Network::Mainnet)?;
        let mut trees = None;
        for h in 0..=10 {
            let block = zakura_test::vectors::MAINNET_BLOCKS[&h]
                .zcash_deserialize_into::<block::Block>()?;
            let (_, next) = source.commit_finalized_direct(
                std::sync::Arc::new(block).into(),
                trees,
                None,
                "spentness fixture",
            )?;
            trees = Some(next);
        }
        let hash = source
            .db
            .hash(Height(10))
            .ok_or_else(|| eyre!("fixture tip missing"))?;
        let (bytes, pin, count) = generate(&source.db, 10, hash)?;
        let (again, _, _) = generate(&source.db, 10, hash)?;
        ensure!(bytes == again, "generation must be deterministic");
        let verified = VerifiedArtifact::read(bytes.as_slice(), &pin)?;
        ensure!(
            verify(&source.db, &verified)? == count,
            "oracle survivor mismatch"
        );
        ensure!(!verified.retains(0)?, "genesis was retained");
        let old_hash = source
            .db
            .hash(Height(5))
            .ok_or_else(|| eyre!("fixture height missing"))?;
        ensure!(
            generate(&source.db, 5, old_hash).is_err(),
            "must reject newer UTXO sets"
        );
        // Freeze a historical state without rolling back the source.
        replay(source_dir.path(), replay_dir.path(), 5, old_hash)?;
        let historical = open(replay_dir.path())?;
        let (historical_bytes, historical_pin, _) = generate(&historical, 5, old_hash)?;
        verify(
            &historical,
            &VerifiedArtifact::read(historical_bytes.as_slice(), &historical_pin)?,
        )?;
        drop(historical);
        replay(source_dir.path(), replay_dir.path(), 10, hash)?;
        let oracle = open(replay_dir.path())?;
        verify(&oracle, &verified)?;
        // Re-pin a false assertion: file authentication alone must not satisfy the oracle.
        let mut wrong = bytes;
        wrong[86] ^= 2;
        let parsed = ParsedArtifact::read(wrong.as_slice())?;
        let false_pin = parsed.commitment().clone();
        ensure!(
            verify(&oracle, &parsed.verify(&false_pin)?).is_err(),
            "oracle accepted a false survivor bit"
        );
        ensure!(
            source.db.tip() == Some((Height(10), hash)),
            "replay changed its source"
        );
        Ok(())
    }

    #[test]
    fn accumulator_preserves_multiplicity_and_wraps() {
        let mut sum = [255; 32];
        let mut one = [0; 32];
        one[0] = 1;
        add(&mut sum, one);
        assert_eq!(sum, [0; 32]);
        add(&mut sum, one);
        add(&mut sum, one);
        assert_eq!(sum[0], 2);
    }
}
