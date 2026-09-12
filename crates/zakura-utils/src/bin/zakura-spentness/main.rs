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
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};

use clap::{Parser, Subcommand};
use color_eyre::eyre::{ensure, eyre, Result};
use zakura_chain::{
    block::{self, Block, Height},
    common::atomic_write,
    parameters::{
        spentness_hints::{release_commitments, ParsedArtifact},
        Network,
    },
};
use zakura_state::{Config, ZakuraDb};

/// Reviewed artifacts exist only for Mainnet.
const NETWORK: Network = Network::Mainnet;

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Diagnose an incomplete state's cursor by re-enumerating retained bodies.
    AuditProgress {
        #[arg(long)]
        state: PathBuf,
    },
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

/// Read a retained block and check that it is the canonical block at `height`.
fn canonical_block(db: &ZakuraDb, height: Height) -> Result<Arc<Block>> {
    let block = db
        .block(height.into())
        .ok_or_else(|| eyre!("archive lacks retained block {}", height.0))?;
    ensure!(
        db.hash(height) == Some(block.hash()),
        "canonical hash mismatch at {}",
        height.0
    );
    Ok(block)
}

fn write(path: PathBuf, bytes: &[u8]) -> Result<()> {
    atomic_write(path, bytes)??;
    Ok(())
}

/// Print the cursor audit, and fail when retained bodies disagree with the cursor.
///
/// The audit never changes the database.
fn audit_progress(state: &Path) -> Result<()> {
    let audit = zakura_state::audit_spentness_progress(&state_config(state), &NETWORK)?;
    println!("{}", serde_json::to_string_pretty(&audit)?);
    ensure!(
        audit.cursor_matches,
        "retained bodies differ from the construction cursor; audit made no changes"
    );
    Ok(())
}

fn install(artifact_path: &Path, cache: &Path) -> Result<()> {
    let parsed = ParsedArtifact::read(File::open(artifact_path)?)?;
    let commitment = release_commitments(&NETWORK)
        .iter()
        .find(|commitment| *commitment == parsed.commitment())
        .ok_or_else(|| {
            eyre!(
                "this binary does not recognize the artifact; \
                 install a release with its reviewed commitment"
            )
        })?;
    let verified = parsed.verify(commitment)?;
    write(cache.join(commitment.file_name()), verified.bytes())
}

fn run(command: Command) -> Result<()> {
    match command {
        Command::AuditProgress { state } => audit_progress(&state),
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
    use zakura_chain::{
        parameters::spentness_hints::{VerifiedArtifact, HEADER_LEN},
        serialization::ZcashDeserializeInto,
    };
    use zakura_state::FinalizedState;

    use super::*;

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
