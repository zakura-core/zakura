//! Generate and audit external spentness artifacts from immutable archive states.
//!
//! - `replay`: advance a separate ordinary archive state to exactly H.
//! - `generate`: merge canonical output order with the exact-H UTXO set.
//! - `verify`: check the artifact against an independent transparent replay.
#![allow(clippy::print_stdout, clippy::print_stderr)]

mod generate;
mod replay;
mod verify;

use std::{
    collections::HashSet,
    fs::File,
    num::NonZeroUsize,
    ops::RangeInclusive,
    path::{Path, PathBuf},
    sync::{mpsc, Arc},
    thread,
};

use clap::{Parser, Subcommand};
use color_eyre::eyre::{ensure, eyre, Result};
use zakura_chain::{
    block::{self, merkle, Block, Height},
    common::atomic_write,
    parameters::{
        spentness_hints::{supported_commitments, ParsedArtifact},
        Network,
    },
    transaction,
};
use zakura_state::{Config, ZakuraDb};

/// Reviewed artifacts exist only for Mainnet.
const NETWORK: Network = Network::Mainnet;
/// The most threads that read and deserialize blocks ahead of a pass.
const MAX_BLOCK_READERS: usize = 8;
/// Blocks each reader may hold before the pass consumes them.
const BLOCKS_READ_AHEAD: usize = 16;
/// Log pass progress every this many blocks.
const PROGRESS_INTERVAL: u32 = 10_000;

#[derive(Parser)]
#[command(version)]
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

/// Archive state settings: keep the existing database and never skip VCT history.
fn state_config(path: &Path) -> Config {
    Config {
        cache_dir: path.to_owned(),
        delete_old_database: false,
        vct_fast_sync: false,
        ..Config::default()
    }
}

fn open(path: &Path) -> Result<ZakuraDb> {
    let (_, db, _) = zakura_state::init_read_only(state_config(path), &NETWORK)?;
    Ok(db)
}

/// Require the archive tip to equal H/hash, and return the Mainnet genesis hash.
fn exact_boundary(db: &ZakuraDb, height: u32, hash: block::Hash) -> Result<[u8; 32]> {
    ensure!(
        db.tip() == Some((Height(height), hash)),
        "archive tip must equal the requested H/hash"
    );
    let genesis = db
        .hash(Height(0))
        .ok_or_else(|| eyre!("archive has no genesis"))?;
    ensure!(
        Some(genesis) == NETWORK.checkpoint_list().hash(Height(0)),
        "archive chain identity differs from Mainnet"
    );
    Ok(genesis.0)
}

/// A retained canonical block and its transaction hashes.
struct CanonicalBlock {
    height: Height,
    block: Arc<Block>,
    tx_hashes: Vec<transaction::Hash>,
}

/// Read a retained block and check that it is the canonical block at `height`.
///
/// The Merkle root check binds the transactions to the checked header.
fn canonical_block(db: &ZakuraDb, height: Height) -> Result<CanonicalBlock> {
    let block = db
        .block(height.into())
        .ok_or_else(|| eyre!("archive lacks retained block {}", height.0))?;
    ensure!(
        db.hash(height) == Some(block.hash()),
        "canonical hash mismatch at {}",
        height.0
    );
    let tx_hashes: Vec<_> = block.transactions.iter().map(|tx| tx.hash()).collect();
    ensure!(
        !tx_hashes.is_empty()
            && block.header.merkle_root == tx_hashes.iter().copied().collect::<merkle::Root>(),
        "transaction Merkle root mismatch at {}",
        height.0
    );
    // Duplicate transactions can reproduce a Merkle root (CVE-2012-2459).
    ensure!(
        tx_hashes.iter().collect::<HashSet<_>>().len() == tx_hashes.len(),
        "duplicate transaction at {}",
        height.0
    );
    Ok(CanonicalBlock {
        height,
        block,
        tx_hashes,
    })
}

/// Pass each canonical block in `heights` to `visit`, in height order.
///
/// Worker threads read, deserialize, and check blocks ahead of `visit`.
/// `label` names the pass in progress logs.
fn for_each_block(
    db: &ZakuraDb,
    heights: RangeInclusive<u32>,
    label: &str,
    mut visit: impl FnMut(CanonicalBlock) -> Result<()>,
) -> Result<()> {
    let readers = thread::available_parallelism()
        .map_or(1, NonZeroUsize::get)
        .min(MAX_BLOCK_READERS);
    let end = *heights.end();
    thread::scope(|scope| {
        // Reader `i` sends every `readers`-th height, starting at offset `i`.
        let receivers: Vec<_> = (0..readers)
            .map(|reader| {
                let (sender, receiver) = mpsc::sync_channel(BLOCKS_READ_AHEAD);
                let assigned = heights.clone().skip(reader).step_by(readers);
                scope.spawn(move || {
                    for h in assigned {
                        // A closed channel means the pass has stopped.
                        if sender.send(canonical_block(db, Height(h))).is_err() {
                            break;
                        }
                    }
                });
                receiver
            })
            .collect();
        for (h, receiver) in heights.clone().zip(receivers.iter().cycle()) {
            let block = receiver
                .recv()
                .map_err(|_| eyre!("block reader stopped before {h}"))??;
            visit(block)?;
            if h.is_multiple_of(PROGRESS_INTERVAL) {
                eprintln!("{label} {h}/{end}");
            }
        }
        Ok(())
    })
}

fn write(path: PathBuf, bytes: &[u8]) -> Result<()> {
    atomic_write(path, bytes)??;
    Ok(())
}

fn install(artifact_path: &Path, cache: &Path) -> Result<()> {
    let parsed = ParsedArtifact::read(File::open(artifact_path)?)?;
    let commitment = supported_commitments(&NETWORK)
        .into_iter()
        .find(|commitment| commitment == parsed.commitment())
        .ok_or_else(|| {
            eyre!(
                "this binary does not recognize the artifact, or its commitment is revoked; \
                 install a release with its reviewed commitment"
            )
        })?;
    let verified = parsed.verify(&commitment)?;
    write(cache.join(commitment.file_name()), verified.bytes())
}

fn run(command: Command) -> Result<()> {
    match command {
        Command::Install { artifact, cache } => install(&artifact, &cache),
        Command::Replay {
            source,
            destination,
            height,
            block_hash,
        } => replay::replay(&source, &destination, height, block_hash),
        Command::Generate {
            state,
            height,
            block_hash,
            output,
            commitment,
        } => generate::run(&state, height, block_hash, output, commitment),
        Command::Verify {
            state,
            artifact,
            commitment,
            report,
        } => verify::run(&state, &artifact, &commitment, report),
    }
}

fn main() -> Result<()> {
    color_eyre::install()?;
    zakura_utils::init_tracing();
    run(Args::parse().command)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use zakura_chain::{
        parameters::spentness_hints::{VerifiedArtifact, HEADER_LEN},
        serialization::ZcashDeserializeInto,
        transparent::Output,
    };
    use zakura_state::{FinalizedState, OutputLocation, TypedColumnFamily};

    use super::*;

    /// Release tooling fixtures that the Python validator tests also read.
    const GOLDEN_DIR: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../.github/scripts/testdata/spentness"
    );
    /// Set to rewrite the golden files after an intended format change.
    const UPDATE_GOLDEN_ENV: &str = "ZAKURA_UPDATE_SPENTNESS_GOLDEN";

    /// Commit Mainnet blocks 0 through 10 to an ordinary archive state.
    fn fixture_archive(path: &Path) -> Result<FinalizedState> {
        let mut state = FinalizedState::new(&state_config(path), &NETWORK)?;
        let mut trees = None;
        for height in 0..=10 {
            let block =
                zakura_test::vectors::MAINNET_BLOCKS[&height].zcash_deserialize_into::<Block>()?;
            let (_, next) = state.commit_finalized_direct(
                Arc::new(block).into(),
                trees,
                None,
                "spentness fixture",
            )?;
            trees = Some(next);
        }
        Ok(state)
    }

    /// Replace one ordinary UTXO row, or delete it when `output` is `None`.
    fn set_ordinary_utxo(
        db: &ZakuraDb,
        location: OutputLocation,
        output: Option<&Output>,
    ) -> Result<()> {
        let utxos = TypedColumnFamily::<OutputLocation, Output>::new(db, "utxo_by_out_loc")
            .ok_or_else(|| eyre!("state lacks utxo_by_out_loc"))?
            .new_batch_for_writing();
        match output {
            Some(output) => utxos.zs_insert(&location, output),
            None => utxos.zs_delete(&location),
        }
        .write_batch()?;
        Ok(())
    }

    #[test]
    fn oracle_rejects_corrupted_extra_and_missing_ordinary_utxos() -> Result<()> {
        let _guard = zakura_test::init();
        let dir = tempfile::tempdir()?;
        let state = fixture_archive(dir.path())?;
        let db = &state.db;
        let tip = db
            .hash(Height(10))
            .ok_or_else(|| eyre!("fixture tip missing"))?;
        let generated = generate::generate(db, 10, tip)?;
        let artifact = VerifiedArtifact::read(generated.bytes.as_slice(), &generated.commitment)?;
        let (survivor, entry) = db
            .utxos_by_location()
            .last()
            .ok_or_else(|| eyre!("fixture has no UTXOs"))?;
        let output = entry.utxo.output;
        let mut corrupted = output.clone();
        corrupted.value = (u64::from(output.value) + 1).try_into()?;

        let rejection = |artifact: &VerifiedArtifact| match verify::verify(db, artifact) {
            Ok(_) => "accepted".to_string(),
            Err(error) => error.to_string(),
        };
        for (location, row, expected) in [
            (survivor, &corrupted, "ordinary UTXO entry differs"),
            (
                OutputLocation::from_usize(Height(5), 0, 99),
                &output,
                "outside canonical outputs",
            ),
            (
                OutputLocation::from_usize(Height(0), 0, 0),
                &output,
                "wrong survivor bit at ordinal 0",
            ),
        ] {
            let original = db.utxo_by_location(location).map(|entry| entry.utxo.output);
            set_ordinary_utxo(db, location, Some(row))?;
            let rejection = rejection(&artifact);
            ensure!(
                rejection.contains(expected),
                "{location:?}: expected {expected:?}, got {rejection:?}"
            );
            set_ordinary_utxo(db, location, original.as_ref())?;
        }

        // Generation trusts the ordinary state, so only the oracle can reject this artifact.
        set_ordinary_utxo(db, survivor, None)?;
        ensure!(
            rejection(&artifact).contains("wrong survivor bit"),
            "a missing survivor kept its bit"
        );
        let damaged = generate::generate(db, 10, tip)?;
        let damaged = VerifiedArtifact::read(damaged.bytes.as_slice(), &damaged.commitment)?;
        ensure!(
            rejection(&damaged).contains("artifact omits oracle survivor"),
            "oracle accepted an artifact generated from a state missing a survivor"
        );

        set_ordinary_utxo(db, survivor, Some(&output))?;
        ensure!(
            verify::verify(db, &artifact)? == generated.survivors,
            "restored state failed verification"
        );
        Ok(())
    }

    /// Pin the files that `.github/scripts/test_spentness_release.py` validates.
    #[test]
    fn release_files_match_python_golden_files() -> Result<()> {
        let _guard = zakura_test::init();
        let dir = tempfile::tempdir()?;
        let state = fixture_archive(dir.path())?;
        let tip = state
            .db
            .hash(Height(10))
            .ok_or_else(|| eyre!("fixture tip missing"))?;
        let generated = generate::generate(&state.db, 10, tip)?;
        let artifact = VerifiedArtifact::read(generated.bytes.as_slice(), &generated.commitment)?;
        let survivors = verify::verify(&state.db, &artifact)?;
        let files = [
            ("mainnet-spentness-hints.bin", generated.bytes.clone()),
            (
                "mainnet-spentness-hints.commitment.json",
                generate::commitment_json(&generated.commitment)?,
            ),
            (
                "mainnet-spentness-hints.verification.json",
                verify::report(&generated.commitment, survivors)?,
            ),
        ];
        let golden = Path::new(GOLDEN_DIR);
        for (name, bytes) in files {
            let path = golden.join(name);
            if std::env::var_os(UPDATE_GOLDEN_ENV).is_some() {
                fs::create_dir_all(golden)?;
                fs::write(&path, &bytes)?;
            }
            ensure!(
                fs::read(&path)? == bytes,
                "{name} differs from its golden file; set {UPDATE_GOLDEN_ENV}=1 after an intended change"
            );
        }
        Ok(())
    }

    #[test]
    fn generation_verification_and_exact_boundary() -> Result<()> {
        let _guard = zakura_test::init();
        let source_dir = tempfile::tempdir()?;
        let replay_dir = tempfile::tempdir()?;
        let source = fixture_archive(source_dir.path())?;
        let tip_hash = source
            .db
            .hash(Height(10))
            .ok_or_else(|| eyre!("fixture tip missing"))?;

        let generated = generate::generate(&source.db, 10, tip_hash)?;
        let again = generate::generate(&source.db, 10, tip_hash)?;
        ensure!(
            generated.bytes == again.bytes,
            "generation must be deterministic"
        );
        let verified = VerifiedArtifact::read(generated.bytes.as_slice(), &generated.commitment)?;
        ensure!(
            verify::verify(&source.db, &verified)? == generated.survivors,
            "oracle survivor mismatch"
        );
        ensure!(!verified.retains(0)?, "genesis was retained");

        let old_hash = source
            .db
            .hash(Height(5))
            .ok_or_else(|| eyre!("fixture height missing"))?;
        ensure!(
            generate::generate(&source.db, 5, old_hash).is_err(),
            "must reject newer UTXO sets"
        );

        // Freeze a historical state without rolling back the source.
        replay::replay(source_dir.path(), replay_dir.path(), 5, old_hash)?;
        let historical = open(replay_dir.path())?;
        let old = generate::generate(&historical, 5, old_hash)?;
        verify::verify(
            &historical,
            &VerifiedArtifact::read(old.bytes.as_slice(), &old.commitment)?,
        )?;
        drop(historical);

        replay::replay(source_dir.path(), replay_dir.path(), 10, tip_hash)?;
        let oracle = open(replay_dir.path())?;
        verify::verify(&oracle, &verified)?;

        // Re-pin a false assertion: file authentication alone must not satisfy the oracle.
        let mut wrong = generated.bytes;
        wrong[HEADER_LEN] ^= 2;
        let parsed = ParsedArtifact::read(wrong.as_slice())?;
        let false_commitment = parsed.commitment().clone();
        ensure!(
            verify::verify(&oracle, &parsed.verify(&false_commitment)?).is_err(),
            "oracle accepted a false survivor bit"
        );
        ensure!(
            source.db.tip() == Some((Height(10), tip_hash)),
            "replay changed its source"
        );
        Ok(())
    }
}
