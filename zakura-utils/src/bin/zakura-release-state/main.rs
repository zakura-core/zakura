//! Produces and verifies the coupled Mainnet checkpoint and VCT frontier release state.

#![allow(clippy::print_stdout, clippy::unwrap_in_result)]

use std::{
    collections::BTreeSet,
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
};

use chrono::{DateTime, SecondsFormat, Utc};
use color_eyre::eyre::{ensure, eyre, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use structopt::StructOpt;
use tempfile::NamedTempFile;

use zakura_chain::{
    block::{self, Height, MAX_BLOCK_BYTES},
    parameters::Network,
};
use zakura_node_services::constants::{MAX_CHECKPOINT_BYTE_COUNT, MAX_CHECKPOINT_HEIGHT_GAP};
use zakura_state::{produce_final_frontiers_bytes, validate_final_frontiers_bytes};
use zakura_utils::init_tracing;

const SCHEMA_VERSION: u32 = 1;
const NETWORK: &str = "Mainnet";
const MANIFEST_FILE: &str = "manifest.json";
const BLOCK_METADATA_FILE: &str = "block-metadata.bin";
const FRONTIER_FILE: &str = "mainnet-frontier.bin";
const METADATA_MAGIC: &[u8; 8] = b"ZKRSB001";
const METADATA_HEADER_LEN: usize = 20;
const METADATA_RECORD_LEN: usize = 36;
const MAX_METADATA_RECORDS: usize = 5_000_000;
const MAX_MANIFEST_BYTES: u64 = 64 * 1024;
const MAX_PROVENANCE_BYTES: u64 = 64 * 1024;
const MAX_CHECKPOINT_BYTES: u64 = 16 * 1024 * 1024;
const MAX_FRONTIER_BYTES: u64 = 64 * 1024 * 1024;
// These bounded constants total 180,000,020, which fits in u64 on every supported target.
const MAX_METADATA_BYTES: u64 =
    (METADATA_HEADER_LEN + MAX_METADATA_RECORDS * METADATA_RECORD_LEN) as u64;
const MIN_CHECKPOINT_HEIGHT_GAP: u32 = 17;
const RELEASE_STATE_BASE_HEIGHT: u32 = 3_358_006;
const RELEASE_STATE_BASE_HASH: &str =
    "0000000000a4bc547a096ef2bac1f31842d514e2109c540b02384c9e62532ccf";
const RELEASE_STATE_BASE_CHECKPOINTS_SHA256: &str =
    "e6e3558b59e22c0761c5f7ce5716089d9edae273c66802bc5069f69b450d52f9";
const _: () = assert!(
    MAX_CHECKPOINT_BYTE_COUNT.div_ceil(MAX_BLOCK_BYTES) == 17,
    "update the minimum checkpoint gap when checkpoint or block size limits change"
);

const CHECKPOINT_SOURCE_PATH: &str = "zakura-chain/src/parameters/checkpoint/main-checkpoints.txt";
const FRONTIER_SOURCE_PATH: &str =
    "zakura-state/src/service/finalized_state/vct/mainnet-frontier.bin";
const PROVENANCE_SOURCE_PATH: &str =
    "zakura-state/src/service/finalized_state/vct/mainnet-frontier.json";

#[derive(Debug, StructOpt)]
#[structopt(
    name = "zakura-release-state",
    about = "Produce and verify Mainnet checkpoint/frontier release-state bundles"
)]
enum Command {
    /// Export a bundle from an existing Mainnet finalized-state database.
    Export {
        /// Zakura cache directory containing the Mainnet state database.
        #[structopt(long, parse(from_os_str))]
        cache_dir: PathBuf,

        /// New directory where the complete bundle will be created.
        #[structopt(long, parse(from_os_str))]
        output_dir: PathBuf,

        /// Publication timestamp in RFC 3339 format.
        #[structopt(long)]
        generated_at: String,
    },

    /// Verify a bundle without accessing a state database or source checkout.
    Verify {
        /// Directory containing manifest.json and its two artifacts.
        #[structopt(long, parse(from_os_str))]
        bundle_dir: PathBuf,
    },

    /// Import a verified bundle into a Zakura source checkout.
    Import {
        /// Directory containing manifest.json and its two artifacts.
        #[structopt(long, parse(from_os_str))]
        bundle_dir: PathBuf,

        /// Root of the Zakura source checkout to update.
        #[structopt(long, parse(from_os_str))]
        source_dir: PathBuf,
    },

    /// Verify the checkpoint/frontier/provenance files committed in a source checkout.
    VerifySource {
        /// Root of the Zakura source checkout to verify.
        #[structopt(long, parse(from_os_str))]
        source_dir: PathBuf,

        /// Reject legacy bootstrap provenance that did not come from an imported bundle.
        #[structopt(long)]
        require_bundle_provenance: bool,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    schema_version: u32,
    network: String,
    generated_at: String,
    finalized_height: u32,
    finalized_hash: String,
    base_checkpoint_height: u32,
    base_checkpoint_hash: String,
    base_checkpoints_sha256: String,
    artifacts: Artifacts,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct Artifacts {
    block_metadata: Artifact,
    frontier: Artifact,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct Artifact {
    file: String,
    sha256: String,
    size: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct Provenance {
    schema_version: u32,
    network: String,
    source: ProvenanceSource,
    generated_at: String,
    finalized_height: u32,
    finalized_hash: String,
    checkpoints_sha256: String,
    frontier_sha256: String,
    frontier_size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    block_metadata_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    bundle_manifest_sha256: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum ProvenanceSource {
    LegacyBootstrap,
    ReleaseStateBundle,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Checkpoint {
    height: Height,
    hash: block::Hash,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BlockMetadata {
    height: Height,
    hash: block::Hash,
    size: u32,
}

#[derive(Debug)]
struct VerifiedBundle {
    manifest: Manifest,
    manifest_bytes: Vec<u8>,
    metadata: Vec<BlockMetadata>,
    frontier_bytes: Vec<u8>,
}

fn main() -> Result<()> {
    init_tracing();
    color_eyre::install()?;

    match Command::from_args() {
        Command::Export {
            cache_dir,
            output_dir,
            generated_at,
        } => export(&cache_dir, &output_dir, &generated_at),
        Command::Verify { bundle_dir } => {
            verify_bundle(&bundle_dir)?;
            println!("release-state bundle is valid");
            Ok(())
        }
        Command::Import {
            bundle_dir,
            source_dir,
        } => import(&bundle_dir, &source_dir),
        Command::VerifySource {
            source_dir,
            require_bundle_provenance,
        } => verify_source(&source_dir, require_bundle_provenance),
    }
}

fn export(cache_dir: &Path, output_dir: &Path, generated_at: &str) -> Result<()> {
    let generated_at = canonical_timestamp(generated_at)?;
    ensure!(
        !output_dir.exists(),
        "output directory already exists: {}",
        output_dir.display()
    );
    let output_parent = output_dir
        .parent()
        .ok_or_else(|| eyre!("output directory must have a parent"))?;
    let output_parent = if output_parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        output_parent
    };
    fs::create_dir_all(output_parent).wrap_err("creating output parent directory")?;

    let embedded_checkpoint_bytes = embedded_mainnet_checkpoint_bytes()?;
    let checkpoints = parse_checkpoints(&embedded_checkpoint_bytes)?;
    validate_complete_checkpoint_list(&checkpoints)?;
    let base = validate_canonical_base(&embedded_checkpoint_bytes, &checkpoints)?;

    let state_config = zakura_state::Config {
        cache_dir: cache_dir.to_path_buf(),
        delete_old_database: false,
        // Read-only export must opt into pruned mode or the state resume guard correctly rejects
        // the pruned publisher database as an archive configuration.
        storage_mode: zakura_state::StorageMode::Pruned(zakura_state::PruningConfig::default()),
        ..zakura_state::Config::default()
    };
    let (_, db, _) = zakura_state::init_read_only(state_config, &Network::Mainnet)
        .wrap_err("opening the Mainnet state database read-only")?;

    let (finalized_height, finalized_hash) = db
        .tip()
        .ok_or_else(|| eyre!("Mainnet state database has no finalized tip"))?;
    ensure!(
        finalized_height.0.saturating_sub(base.height.0) >= MIN_CHECKPOINT_HEIGHT_GAP,
        "finalized tip {} is too close to base checkpoint {} to create a valid terminal checkpoint",
        finalized_height.0,
        base.height.0
    );
    let record_count = usize::try_from(finalized_height.0 - base.height.0)
        .expect("u32 height difference fits in usize on supported targets");
    ensure!(
        record_count <= MAX_METADATA_RECORDS,
        "bundle would contain {record_count} block metadata records, exceeding the safety limit of {MAX_METADATA_RECORDS}; update the exporter base"
    );

    let mut metadata = Vec::with_capacity(record_count);
    for raw_height in (base.height.0 + 1)..=finalized_height.0 {
        let height = Height(raw_height);
        let hash = db
            .hash(height)
            .ok_or_else(|| eyre!("missing retained finalized hash at height {raw_height}"))?;
        ensure!(
            db.height(hash) == Some(height),
            "finalized hash indexes disagree at height {raw_height}"
        );
        let info = db
            .block_info(height.into())
            .ok_or_else(|| eyre!("missing retained BlockInfo at height {raw_height}"))?;
        ensure!(
            u64::from(info.size()) <= MAX_BLOCK_BYTES && info.size() > 0,
            "invalid retained block size {} at height {raw_height}",
            info.size()
        );
        metadata.push(BlockMetadata {
            height,
            hash,
            size: info.size(),
        });
    }
    ensure!(
        metadata.last().map(|record| record.hash) == Some(finalized_hash),
        "final metadata record does not match the captured finalized tip"
    );

    let frontier_bytes = produce_final_frontiers_bytes(&db, finalized_height)
        .wrap_err("producing finalized Mainnet frontiers")?;
    ensure!(
        db.tip() == Some((finalized_height, finalized_hash)),
        "finalized tip changed during release-state export; retry generation"
    );

    // Exercise checkpoint selection during export, so an unusable tail never reaches R2.
    let selected = select_checkpoints(base, &metadata)?;
    validate_checkpoint_gaps(base, &selected, finalized_height)?;

    let metadata_bytes = encode_metadata(base.height, finalized_height, &metadata)?;
    let manifest = Manifest {
        schema_version: SCHEMA_VERSION,
        network: NETWORK.to_string(),
        generated_at,
        finalized_height: finalized_height.0,
        finalized_hash: finalized_hash.to_string(),
        base_checkpoint_height: base.height.0,
        base_checkpoint_hash: base.hash.to_string(),
        base_checkpoints_sha256: RELEASE_STATE_BASE_CHECKPOINTS_SHA256.to_string(),
        artifacts: Artifacts {
            block_metadata: artifact(BLOCK_METADATA_FILE, &metadata_bytes),
            frontier: artifact(FRONTIER_FILE, &frontier_bytes),
        },
    };
    let manifest_bytes = pretty_json(&manifest)?;

    let staging = tempfile::Builder::new()
        .prefix(".zakura-release-state-")
        .tempdir_in(output_parent)
        .wrap_err("creating bundle staging directory")?;
    fs::write(staging.path().join(BLOCK_METADATA_FILE), &metadata_bytes)?;
    fs::write(staging.path().join(FRONTIER_FILE), &frontier_bytes)?;
    fs::write(staging.path().join(MANIFEST_FILE), &manifest_bytes)?;
    verify_bundle(staging.path()).wrap_err("verifying staged release-state bundle")?;

    let staging_path = staging.path().to_path_buf();
    fs::rename(&staging_path, output_dir).wrap_err_with(|| {
        format!(
            "publishing staged bundle {} to {}",
            staging_path.display(),
            output_dir.display()
        )
    })?;

    println!(
        "exported Mainnet release state at height {} ({}) to {}",
        finalized_height.0,
        finalized_hash,
        output_dir.display()
    );
    Ok(())
}

fn verify_bundle(bundle_dir: &Path) -> Result<VerifiedBundle> {
    ensure_exact_bundle_files(bundle_dir)?;
    let manifest_bytes = read_bounded_file(
        &bundle_dir.join(MANIFEST_FILE),
        MAX_MANIFEST_BYTES,
        "release-state manifest",
    )?;
    let manifest: Manifest =
        serde_json::from_slice(&manifest_bytes).wrap_err("parsing release-state manifest")?;
    validate_manifest(&manifest)?;

    let metadata_bytes = read_artifact(
        bundle_dir,
        &manifest.artifacts.block_metadata,
        BLOCK_METADATA_FILE,
    )?;
    let frontier_bytes = read_artifact(bundle_dir, &manifest.artifacts.frontier, FRONTIER_FILE)?;

    let base_height = Height(manifest.base_checkpoint_height);
    let finalized_height = Height(manifest.finalized_height);
    let metadata = decode_metadata(&metadata_bytes, base_height, finalized_height)?;
    let finalized_hash = parse_hash(&manifest.finalized_hash, "finalized_hash")?;
    ensure!(
        metadata.last().map(|record| record.hash) == Some(finalized_hash),
        "final metadata hash does not match manifest finalized_hash"
    );
    validate_final_frontiers_bytes(&frontier_bytes, finalized_height)
        .wrap_err("validating final frontier artifact")?;

    let base = Checkpoint {
        height: base_height,
        hash: parse_hash(&manifest.base_checkpoint_hash, "base_checkpoint_hash")?,
    };
    let selected = select_checkpoints(base, &metadata)?;
    validate_checkpoint_gaps(base, &selected, finalized_height)?;

    Ok(VerifiedBundle {
        manifest,
        manifest_bytes,
        metadata,
        frontier_bytes,
    })
}

fn import(bundle_dir: &Path, source_dir: &Path) -> Result<()> {
    let bundle = verify_bundle(bundle_dir)?;
    let checkpoint_path = source_dir.join(CHECKPOINT_SOURCE_PATH);
    let frontier_path = source_dir.join(FRONTIER_SOURCE_PATH);
    let provenance_path = source_dir.join(PROVENANCE_SOURCE_PATH);

    let checkpoint_bytes =
        read_bounded_file(&checkpoint_path, MAX_CHECKPOINT_BYTES, "source checkpoints")?;
    let checkpoints = parse_checkpoints(&checkpoint_bytes)?;
    validate_complete_checkpoint_list(&checkpoints)?;
    let canonical_base = validate_canonical_base(&checkpoint_bytes, &checkpoints)?;
    let source_tip = *checkpoints
        .last()
        .ok_or_else(|| eyre!("source Mainnet checkpoint list is empty"))?;
    if source_tip.height.0 == bundle.manifest.finalized_height {
        let existing_provenance_bytes = read_bounded_file(
            &provenance_path,
            MAX_PROVENANCE_BYTES,
            "source frontier provenance",
        )
        .wrap_err("source is already at the bundle height but has no readable provenance")?;
        let existing: Provenance = serde_json::from_slice(&existing_provenance_bytes)
            .wrap_err("source is already at the bundle height but its provenance is invalid")?;
        let manifest_sha256 = sha256_hex(&bundle.manifest_bytes);
        ensure!(
            source_tip.hash.to_string() == bundle.manifest.finalized_hash
                && existing.source == ProvenanceSource::ReleaseStateBundle
                && existing.finalized_height == bundle.manifest.finalized_height
                && existing.finalized_hash == bundle.manifest.finalized_hash
                && existing.frontier_sha256 == bundle.manifest.artifacts.frontier.sha256
                && existing.block_metadata_sha256.as_deref()
                    == Some(bundle.manifest.artifacts.block_metadata.sha256.as_str())
                && existing.bundle_manifest_sha256.as_deref() == Some(manifest_sha256.as_str()),
            "source terminal checkpoint conflicts with the bundle at the same height"
        );
        verify_source(source_dir, true)?;
        println!(
            "source already contains Mainnet release state at height {} ({})",
            bundle.manifest.finalized_height, bundle.manifest.finalized_hash
        );
        return Ok(());
    }
    ensure!(
        source_tip.height.0 < bundle.manifest.finalized_height,
        "refusing to import release state below the source terminal checkpoint"
    );
    validate_existing_checkpoint_suffix(canonical_base, &checkpoints, &bundle.metadata)?;
    let next_record = usize::try_from(source_tip.height.0 - canonical_base.height.0)
        .expect("u32 metadata index fits in usize on supported targets");
    let remaining_metadata = bundle
        .metadata
        .get(next_record..)
        .ok_or_else(|| eyre!("source checkpoint tip is outside bundle metadata"))?;
    let selected = select_checkpoints(source_tip, remaining_metadata)?;
    validate_checkpoint_gaps(
        source_tip,
        &selected,
        Height(bundle.manifest.finalized_height),
    )?;
    let updated_checkpoints = append_checkpoints(&checkpoint_bytes, &selected)?;
    let provenance = Provenance {
        schema_version: SCHEMA_VERSION,
        network: NETWORK.to_string(),
        source: ProvenanceSource::ReleaseStateBundle,
        generated_at: bundle.manifest.generated_at.clone(),
        finalized_height: bundle.manifest.finalized_height,
        finalized_hash: bundle.manifest.finalized_hash.clone(),
        checkpoints_sha256: sha256_hex(&updated_checkpoints),
        frontier_sha256: bundle.manifest.artifacts.frontier.sha256.clone(),
        frontier_size: bundle.manifest.artifacts.frontier.size,
        block_metadata_sha256: Some(bundle.manifest.artifacts.block_metadata.sha256.clone()),
        bundle_manifest_sha256: Some(sha256_hex(&bundle.manifest_bytes)),
    };
    let provenance_bytes = pretty_json(&provenance)?;

    replace_source_files(&[
        (&checkpoint_path, updated_checkpoints.as_slice()),
        (&frontier_path, bundle.frontier_bytes.as_slice()),
        (&provenance_path, provenance_bytes.as_slice()),
    ])?;
    verify_source(source_dir, true).wrap_err("verifying imported source release state")?;

    println!(
        "imported Mainnet release state at height {} ({})",
        bundle.manifest.finalized_height, bundle.manifest.finalized_hash
    );
    Ok(())
}

fn verify_source(source_dir: &Path, require_bundle_provenance: bool) -> Result<()> {
    let checkpoint_path = source_dir.join(CHECKPOINT_SOURCE_PATH);
    let frontier_path = source_dir.join(FRONTIER_SOURCE_PATH);
    let provenance_path = source_dir.join(PROVENANCE_SOURCE_PATH);

    let provenance_bytes = read_bounded_file(
        &provenance_path,
        MAX_PROVENANCE_BYTES,
        "source frontier provenance",
    )?;
    let provenance: Provenance =
        serde_json::from_slice(&provenance_bytes).wrap_err("parsing frontier provenance")?;
    ensure!(
        provenance.schema_version == SCHEMA_VERSION,
        "unsupported provenance schema_version"
    );
    ensure!(
        provenance.network == NETWORK,
        "provenance network must be Mainnet"
    );
    canonical_timestamp(&provenance.generated_at)?;
    let finalized_hash = parse_hash(&provenance.finalized_hash, "finalized_hash")?;

    let checkpoint_bytes =
        read_bounded_file(&checkpoint_path, MAX_CHECKPOINT_BYTES, "source checkpoints")?;
    ensure!(
        sha256_hex(&checkpoint_bytes) == provenance.checkpoints_sha256,
        "source checkpoint digest does not match frontier provenance"
    );
    let checkpoints = parse_checkpoints(&checkpoint_bytes)?;
    validate_complete_checkpoint_list(&checkpoints)?;
    validate_canonical_base(&checkpoint_bytes, &checkpoints)?;
    let final_checkpoint = checkpoints
        .last()
        .ok_or_else(|| eyre!("source Mainnet checkpoint list is empty"))?;
    ensure!(
        final_checkpoint.height.0 == provenance.finalized_height
            && final_checkpoint.hash == finalized_hash,
        "source terminal checkpoint does not match frontier provenance"
    );

    let frontier_bytes =
        read_bounded_file(&frontier_path, MAX_FRONTIER_BYTES, "source final frontier")?;
    ensure!(
        u64::try_from(frontier_bytes.len()).expect("usize fits in u64") == provenance.frontier_size,
        "source final frontier size does not match provenance"
    );
    ensure!(
        sha256_hex(&frontier_bytes) == provenance.frontier_sha256,
        "source final frontier digest does not match provenance"
    );
    validate_final_frontiers_bytes(&frontier_bytes, final_checkpoint.height)
        .wrap_err("validating source final frontier")?;
    match provenance.source {
        ProvenanceSource::LegacyBootstrap => {
            ensure!(
                provenance.block_metadata_sha256.is_none()
                    && provenance.bundle_manifest_sha256.is_none(),
                "legacy bootstrap provenance must not claim release-state bundle digests"
            );
            ensure!(
                !require_bundle_provenance,
                "release requires provenance imported from a verified release-state bundle"
            );
        }
        ProvenanceSource::ReleaseStateBundle => {
            parse_sha256(
                provenance
                    .block_metadata_sha256
                    .as_deref()
                    .ok_or_else(|| eyre!("bundle provenance is missing block_metadata_sha256"))?,
                "block_metadata_sha256",
            )?;
            parse_sha256(
                provenance
                    .bundle_manifest_sha256
                    .as_deref()
                    .ok_or_else(|| eyre!("bundle provenance is missing bundle_manifest_sha256"))?,
                "bundle_manifest_sha256",
            )?;
        }
    }

    println!(
        "source release state is valid at height {} ({})",
        final_checkpoint.height.0, final_checkpoint.hash
    );
    Ok(())
}

fn validate_manifest(manifest: &Manifest) -> Result<()> {
    ensure!(
        manifest.schema_version == SCHEMA_VERSION,
        "unsupported manifest schema_version"
    );
    ensure!(
        manifest.network == NETWORK,
        "manifest network must be Mainnet"
    );
    canonical_timestamp(&manifest.generated_at)?;
    parse_hash(&manifest.finalized_hash, "finalized_hash")?;
    parse_hash(&manifest.base_checkpoint_hash, "base_checkpoint_hash")?;
    parse_sha256(&manifest.base_checkpoints_sha256, "base_checkpoints_sha256")?;
    ensure!(
        manifest.finalized_height > manifest.base_checkpoint_height,
        "manifest finalized height must be above its base checkpoint"
    );
    ensure!(
        manifest.base_checkpoint_height == RELEASE_STATE_BASE_HEIGHT
            && manifest.base_checkpoint_hash == RELEASE_STATE_BASE_HASH
            && manifest.base_checkpoints_sha256 == RELEASE_STATE_BASE_CHECKPOINTS_SHA256,
        "manifest does not use the canonical Mainnet release-state base"
    );
    Ok(())
}

fn ensure_exact_bundle_files(bundle_dir: &Path) -> Result<()> {
    let actual: BTreeSet<String> = fs::read_dir(bundle_dir)
        .wrap_err("reading bundle directory")?
        .map(|entry| {
            let entry = entry?;
            ensure!(
                entry.file_type()?.is_file(),
                "bundle entries must be regular files"
            );
            entry
                .file_name()
                .into_string()
                .map_err(|_| eyre!("bundle filenames must be UTF-8"))
        })
        .collect::<Result<_>>()?;
    let expected = BTreeSet::from([
        MANIFEST_FILE.to_string(),
        BLOCK_METADATA_FILE.to_string(),
        FRONTIER_FILE.to_string(),
    ]);
    ensure!(
        actual == expected,
        "bundle must contain exactly {expected:?}, found {actual:?}"
    );
    Ok(())
}

fn read_artifact(bundle_dir: &Path, artifact: &Artifact, expected_file: &str) -> Result<Vec<u8>> {
    ensure!(
        artifact.file == expected_file,
        "manifest artifact filename must be {expected_file}"
    );
    parse_sha256(&artifact.sha256, &format!("{expected_file} sha256"))?;
    let max_size = match expected_file {
        BLOCK_METADATA_FILE => MAX_METADATA_BYTES,
        FRONTIER_FILE => MAX_FRONTIER_BYTES,
        _ => return Err(eyre!("unsupported release-state artifact {expected_file}")),
    };
    ensure!(
        artifact.size > 0 && artifact.size <= max_size,
        "{expected_file} manifest size exceeds its safety limit"
    );
    let bytes = read_bounded_file(
        &bundle_dir.join(expected_file),
        max_size,
        &format!("bundle artifact {expected_file}"),
    )?;
    ensure!(
        u64::try_from(bytes.len()).expect("usize fits in u64") == artifact.size,
        "{expected_file} size does not match manifest"
    );
    ensure!(
        sha256_hex(&bytes) == artifact.sha256,
        "{expected_file} digest does not match manifest"
    );
    Ok(bytes)
}

fn read_bounded_file(path: &Path, max_size: u64, description: &str) -> Result<Vec<u8>> {
    let file = fs::File::open(path)
        .wrap_err_with(|| format!("opening {description} at {}", path.display()))?;
    let metadata = file
        .metadata()
        .wrap_err_with(|| format!("reading {description} metadata at {}", path.display()))?;
    ensure!(metadata.is_file(), "{description} must be a regular file");
    ensure!(
        metadata.len() > 0 && metadata.len() <= max_size,
        "{description} size {} is outside the allowed range 1..={max_size}",
        metadata.len()
    );
    let mut bytes = Vec::with_capacity(
        usize::try_from(metadata.len())
            .expect("bounded file size fits in usize on supported targets"),
    );
    file.take(max_size + 1)
        .read_to_end(&mut bytes)
        .wrap_err_with(|| format!("reading {description} at {}", path.display()))?;
    ensure!(
        u64::try_from(bytes.len()).expect("usize fits in u64") <= max_size,
        "{description} grew beyond its safety limit while it was being read"
    );
    ensure!(
        u64::try_from(bytes.len()).expect("usize fits in u64") == metadata.len(),
        "{description} changed size while it was being read"
    );
    Ok(bytes)
}

fn artifact(file: &str, bytes: &[u8]) -> Artifact {
    Artifact {
        file: file.to_string(),
        sha256: sha256_hex(bytes),
        size: u64::try_from(bytes.len()).expect("usize fits in u64"),
    }
}

fn encode_metadata(base: Height, finalized: Height, records: &[BlockMetadata]) -> Result<Vec<u8>> {
    let expected = usize::try_from(finalized.0.saturating_sub(base.0))
        .expect("u32 height difference fits in usize on supported targets");
    ensure!(
        records.len() == expected,
        "metadata does not cover every height after the base checkpoint"
    );
    ensure!(
        records.len() <= MAX_METADATA_RECORDS,
        "metadata exceeds the record safety limit"
    );

    let mut bytes = Vec::with_capacity(METADATA_HEADER_LEN + records.len() * METADATA_RECORD_LEN);
    bytes.extend_from_slice(METADATA_MAGIC);
    bytes.extend_from_slice(&base.0.to_le_bytes());
    bytes.extend_from_slice(&finalized.0.to_le_bytes());
    bytes.extend_from_slice(
        &u32::try_from(records.len())
            .expect("bounded record count fits in u32")
            .to_le_bytes(),
    );
    for (index, record) in records.iter().enumerate() {
        let expected_height = base.0 + 1 + u32::try_from(index).expect("bounded index fits in u32");
        ensure!(
            record.height.0 == expected_height,
            "metadata heights must be contiguous"
        );
        ensure!(
            record.size > 0 && u64::from(record.size) <= MAX_BLOCK_BYTES,
            "metadata block size is invalid at height {expected_height}"
        );
        bytes.extend_from_slice(&record.hash.0);
        bytes.extend_from_slice(&record.size.to_le_bytes());
    }
    Ok(bytes)
}

fn decode_metadata(
    bytes: &[u8],
    expected_base: Height,
    expected_finalized: Height,
) -> Result<Vec<BlockMetadata>> {
    ensure!(
        bytes.len() >= METADATA_HEADER_LEN,
        "block metadata header is truncated"
    );
    ensure!(
        &bytes[..8] == METADATA_MAGIC,
        "unsupported block metadata format"
    );
    let base = read_u32(bytes, 8)?;
    let finalized = read_u32(bytes, 12)?;
    let count = usize::try_from(read_u32(bytes, 16)?)
        .expect("u32 record count fits in usize on supported targets");
    ensure!(
        base == expected_base.0,
        "block metadata base height does not match manifest"
    );
    ensure!(
        finalized == expected_finalized.0,
        "block metadata finalized height does not match manifest"
    );
    ensure!(
        count <= MAX_METADATA_RECORDS,
        "block metadata exceeds the record safety limit"
    );
    let expected_count = usize::try_from(finalized.saturating_sub(base))
        .expect("u32 height difference fits in usize on supported targets");
    ensure!(
        count == expected_count,
        "block metadata record count does not cover its height range"
    );
    let expected_len = METADATA_HEADER_LEN
        .checked_add(
            count
                .checked_mul(METADATA_RECORD_LEN)
                .ok_or_else(|| eyre!("block metadata length overflows"))?,
        )
        .ok_or_else(|| eyre!("block metadata length overflows"))?;
    ensure!(
        bytes.len() == expected_len,
        "block metadata length does not match its record count"
    );

    let mut records = Vec::with_capacity(count);
    for index in 0..count {
        let offset = METADATA_HEADER_LEN + index * METADATA_RECORD_LEN;
        let hash_bytes: [u8; 32] = bytes[offset..offset + 32]
            .try_into()
            .expect("validated metadata record length");
        let size = read_u32(bytes, offset + 32)?;
        let height = Height(base + 1 + u32::try_from(index).expect("bounded index fits in u32"));
        ensure!(
            size > 0 && u64::from(size) <= MAX_BLOCK_BYTES,
            "invalid metadata block size at height {}",
            height.0
        );
        records.push(BlockMetadata {
            height,
            hash: block::Hash(hash_bytes),
            size,
        });
    }
    Ok(records)
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32> {
    let raw: [u8; 4] = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| eyre!("truncated u32 at byte {offset}"))?
        .try_into()
        .expect("slice length was checked");
    Ok(u32::from_le_bytes(raw))
}

fn select_checkpoints(base: Checkpoint, metadata: &[BlockMetadata]) -> Result<Vec<Checkpoint>> {
    let finalized = metadata
        .last()
        .ok_or_else(|| eyre!("block metadata is empty"))?;
    ensure!(
        metadata.first().map(|record| record.height.0) == Some(base.height.0 + 1),
        "metadata must start immediately after the base checkpoint"
    );

    let mut selected = Vec::new();
    let mut cumulative_bytes = 0u64;
    let mut last_height = base.height;
    for record in metadata {
        cumulative_bytes = cumulative_bytes
            .checked_add(u64::from(record.size))
            .ok_or_else(|| eyre!("checkpoint cumulative byte count overflowed"))?;
        let gap = record.height.0 - last_height.0;
        if cumulative_bytes >= MAX_CHECKPOINT_BYTE_COUNT
            || usize::try_from(gap).expect("u32 fits in usize") >= MAX_CHECKPOINT_HEIGHT_GAP
        {
            selected.push(Checkpoint {
                height: record.height,
                hash: record.hash,
            });
            cumulative_bytes = 0;
            last_height = record.height;
        }
    }

    if selected.last().map(|checkpoint| checkpoint.height) != Some(finalized.height) {
        let tail_gap = finalized.height.0 - last_height.0;
        if tail_gap >= MIN_CHECKPOINT_HEIGHT_GAP {
            selected.push(Checkpoint {
                height: finalized.height,
                hash: finalized.hash,
            });
        } else {
            if selected.last().is_some() {
                selected.pop();
            }
            let previous = selected.last().copied().unwrap_or(base);
            let merged_gap = finalized.height.0 - previous.height.0;
            ensure!(
                merged_gap >= MIN_CHECKPOINT_HEIGHT_GAP,
                "finalized height is too close to the base checkpoint for a valid terminal checkpoint"
            );
            if usize::try_from(merged_gap).expect("u32 fits in usize") > MAX_CHECKPOINT_HEIGHT_GAP {
                let bridge_height = Height(finalized.height.0 - MIN_CHECKPOINT_HEIGHT_GAP);
                let bridge = metadata
                    .get(
                        usize::try_from(bridge_height.0 - base.height.0 - 1)
                            .expect("u32 metadata index fits in usize"),
                    )
                    .ok_or_else(|| eyre!("missing metadata for terminal checkpoint bridge"))?;
                selected.push(Checkpoint {
                    height: bridge.height,
                    hash: bridge.hash,
                });
            }
            selected.push(Checkpoint {
                height: finalized.height,
                hash: finalized.hash,
            });
        }
    }

    Ok(selected)
}

fn validate_checkpoint_gaps(
    base: Checkpoint,
    selected: &[Checkpoint],
    finalized: Height,
) -> Result<()> {
    ensure!(
        selected.last().map(|checkpoint| checkpoint.height) == Some(finalized),
        "terminal checkpoint must equal the finalized bundle height"
    );
    let mut previous = base.height;
    for checkpoint in selected {
        let gap = checkpoint.height.0 - previous.0;
        ensure!(
            gap >= MIN_CHECKPOINT_HEIGHT_GAP,
            "checkpoint gap {gap} is below {MIN_CHECKPOINT_HEIGHT_GAP}"
        );
        ensure!(
            usize::try_from(gap).expect("u32 fits in usize") <= MAX_CHECKPOINT_HEIGHT_GAP,
            "checkpoint gap {gap} exceeds {MAX_CHECKPOINT_HEIGHT_GAP}"
        );
        previous = checkpoint.height;
    }
    Ok(())
}

fn validate_complete_checkpoint_list(checkpoints: &[Checkpoint]) -> Result<()> {
    let first = checkpoints
        .first()
        .ok_or_else(|| eyre!("Mainnet checkpoint list is empty"))?;
    ensure!(
        first.height == Height::MIN,
        "Mainnet checkpoint list must start at genesis"
    );
    for pair in checkpoints.windows(2) {
        let gap = pair[1].height.0 - pair[0].height.0;
        ensure!(
            gap >= MIN_CHECKPOINT_HEIGHT_GAP,
            "checkpoint gap {gap} is below {MIN_CHECKPOINT_HEIGHT_GAP}"
        );
        ensure!(
            usize::try_from(gap).expect("u32 fits in usize") <= MAX_CHECKPOINT_HEIGHT_GAP,
            "checkpoint gap {gap} exceeds {MAX_CHECKPOINT_HEIGHT_GAP}"
        );
    }
    Ok(())
}

fn validate_canonical_base(
    checkpoint_bytes: &[u8],
    checkpoints: &[Checkpoint],
) -> Result<Checkpoint> {
    let base_index = checkpoints
        .iter()
        .position(|checkpoint| checkpoint.height.0 == RELEASE_STATE_BASE_HEIGHT)
        .ok_or_else(|| {
            eyre!("Mainnet checkpoint list is missing the canonical release-state base")
        })?;
    let base = checkpoints[base_index];
    ensure!(
        base.hash.to_string() == RELEASE_STATE_BASE_HASH,
        "canonical release-state base checkpoint hash does not match"
    );

    // Parsing requires canonical one-line records, so rendering this slice reproduces the exact
    // source prefix while excluding later, moving checkpoints.
    let prefix = render_checkpoints(&checkpoints[..=base_index])?;
    ensure!(
        checkpoint_bytes.starts_with(&prefix),
        "source checkpoint bytes do not contain the parsed canonical prefix"
    );
    ensure!(
        sha256_hex(&prefix) == RELEASE_STATE_BASE_CHECKPOINTS_SHA256,
        "canonical release-state checkpoint prefix digest does not match"
    );
    Ok(base)
}

fn validate_existing_checkpoint_suffix(
    base: Checkpoint,
    checkpoints: &[Checkpoint],
    metadata: &[BlockMetadata],
) -> Result<()> {
    let base_index = checkpoints
        .iter()
        .position(|checkpoint| *checkpoint == base)
        .ok_or_else(|| eyre!("checkpoint list is missing its validated release-state base"))?;
    for checkpoint in &checkpoints[base_index + 1..] {
        let metadata_index = usize::try_from(checkpoint.height.0 - base.height.0 - 1)
            .expect("u32 metadata index fits in usize on supported targets");
        let record = metadata.get(metadata_index).ok_or_else(|| {
            eyre!(
                "source checkpoint {} is above the bundle",
                checkpoint.height.0
            )
        })?;
        ensure!(
            record.height == checkpoint.height && record.hash == checkpoint.hash,
            "source checkpoint {} conflicts with bundle block metadata",
            checkpoint.height.0
        );
    }
    Ok(())
}

fn parse_checkpoints(bytes: &[u8]) -> Result<Vec<Checkpoint>> {
    let text = std::str::from_utf8(bytes).wrap_err("checkpoint file must be UTF-8")?;
    ensure!(
        text.ends_with('\n'),
        "checkpoint file must end with a newline"
    );
    let mut checkpoints: Vec<Checkpoint> = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let mut fields = line.split(' ');
        let height_text = fields.next().unwrap_or_default();
        let hash_text = fields.next().unwrap_or_default();
        ensure!(
            fields.next().is_none() && !height_text.is_empty() && !hash_text.is_empty(),
            "invalid checkpoint line {}",
            index + 1
        );
        let height: u32 = height_text
            .parse()
            .wrap_err_with(|| format!("invalid checkpoint height on line {}", index + 1))?;
        ensure!(
            height.to_string() == height_text,
            "checkpoint height is not canonical on line {}",
            index + 1
        );
        let hash = parse_hash(hash_text, &format!("checkpoint hash on line {}", index + 1))?;
        if let Some(previous) = checkpoints.last() {
            ensure!(
                height > previous.height.0,
                "checkpoint heights must be strictly increasing"
            );
        }
        checkpoints.push(Checkpoint {
            height: Height(height),
            hash,
        });
    }
    Ok(checkpoints)
}

fn append_checkpoints(base_bytes: &[u8], selected: &[Checkpoint]) -> Result<Vec<u8>> {
    ensure!(
        base_bytes.ends_with(b"\n"),
        "checkpoint file must end with a newline"
    );
    let mut updated = base_bytes.to_vec();
    for checkpoint in selected {
        writeln!(&mut updated, "{} {}", checkpoint.height.0, checkpoint.hash)?;
    }
    Ok(updated)
}

fn render_checkpoints(checkpoints: &[Checkpoint]) -> Result<Vec<u8>> {
    let mut rendered = Vec::new();
    for checkpoint in checkpoints {
        writeln!(&mut rendered, "{} {}", checkpoint.height.0, checkpoint.hash)?;
    }
    Ok(rendered)
}

fn embedded_mainnet_checkpoint_bytes() -> Result<Vec<u8>> {
    let checkpoints: Vec<_> = Network::Mainnet
        .checkpoint_list()
        .iter_cloned()
        .map(|(height, hash)| Checkpoint { height, hash })
        .collect();
    render_checkpoints(&checkpoints)
}

fn replace_source_files(files: &[(&Path, &[u8])]) -> Result<()> {
    let originals: Vec<Option<Vec<u8>>> = files
        .iter()
        .map(|(path, _)| match fs::read(path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        })
        .collect::<std::io::Result<_>>()?;
    let mut staged = Vec::with_capacity(files.len());
    for (path, bytes) in files {
        let parent = path
            .parent()
            .ok_or_else(|| eyre!("source path must have a parent: {}", path.display()))?;
        let mut temporary = NamedTempFile::new_in(parent)?;
        temporary.write_all(bytes)?;
        temporary.as_file().sync_all()?;
        staged.push(temporary);
    }

    for index in 0..files.len() {
        if let Err(error) = staged.remove(0).persist(files[index].0) {
            for rollback_index in 0..index {
                match &originals[rollback_index] {
                    Some(bytes) => atomic_write(files[rollback_index].0, bytes)?,
                    None => {
                        let _ = fs::remove_file(files[rollback_index].0);
                    }
                }
            }
            return Err(error.error)
                .wrap_err_with(|| format!("atomically replacing {}", files[index].0.display()));
        }
    }
    Ok(())
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| eyre!("path must have a parent: {}", path.display()))?;
    let mut temporary = NamedTempFile::new_in(parent)?;
    temporary.write_all(bytes)?;
    temporary.as_file().sync_all()?;
    temporary.persist(path).map_err(|error| error.error)?;
    Ok(())
}

fn parse_hash(value: &str, field: &str) -> Result<block::Hash> {
    ensure!(
        value.len() == 64,
        "{field} must contain 64 lowercase hexadecimal characters"
    );
    let hash: block::Hash = value.parse().wrap_err_with(|| format!("parsing {field}"))?;
    ensure!(
        hash.to_string() == value,
        "{field} must use canonical lowercase display order"
    );
    Ok(hash)
}

fn parse_sha256(value: &str, field: &str) -> Result<[u8; 32]> {
    ensure!(
        value.len() == 64,
        "{field} must contain 64 lowercase hexadecimal characters"
    );
    let bytes = hex::decode(value).wrap_err_with(|| format!("parsing {field}"))?;
    let parsed: [u8; 32] = bytes
        .try_into()
        .map_err(|_| eyre!("{field} must contain 32 bytes"))?;
    ensure!(
        hex::encode(parsed) == value,
        "{field} must use lowercase hexadecimal"
    );
    Ok(parsed)
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn canonical_timestamp(value: &str) -> Result<String> {
    let parsed: DateTime<Utc> = DateTime::parse_from_rfc3339(value)
        .wrap_err("generated_at must be an RFC 3339 timestamp")?
        .with_timezone(&Utc);
    let canonical = parsed.to_rfc3339_opts(SecondsFormat::Secs, true);
    ensure!(
        canonical == value,
        "generated_at must use canonical UTC seconds, for example 2026-07-18T12:34:56Z"
    );
    Ok(canonical)
}

fn pretty_json<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn hash(height: u32) -> block::Hash {
        let mut bytes = [0; 32];
        bytes[..4].copy_from_slice(&height.to_le_bytes());
        block::Hash(bytes)
    }

    fn metadata(base: u32, sizes: &[u32]) -> Vec<BlockMetadata> {
        sizes
            .iter()
            .enumerate()
            .map(|(index, size)| {
                let height = base + 1 + u32::try_from(index).expect("test index fits in u32");
                BlockMetadata {
                    height: Height(height),
                    hash: hash(height),
                    size: *size,
                }
            })
            .collect()
    }

    fn write_test_bundle(root: &Path, finalized_height: u32) -> PathBuf {
        let bundle_dir = root.join(format!("bundle-{finalized_height}"));
        fs::create_dir(&bundle_dir).expect("bundle directory is created");
        let record_count = usize::try_from(finalized_height - RELEASE_STATE_BASE_HEIGHT)
            .expect("test record count fits in usize");
        let records = metadata(RELEASE_STATE_BASE_HEIGHT, &vec![1; record_count]);
        let metadata_bytes = encode_metadata(
            Height(RELEASE_STATE_BASE_HEIGHT),
            Height(finalized_height),
            &records,
        )
        .expect("test metadata encodes");
        let mut frontier_bytes = hex::decode(include_str!("test-frontier.hex").trim())
            .expect("packaged frontier fixture is hexadecimal");
        frontier_bytes[..4].copy_from_slice(&finalized_height.to_le_bytes());
        let manifest = Manifest {
            schema_version: SCHEMA_VERSION,
            network: NETWORK.to_string(),
            generated_at: "2026-07-18T12:34:56Z".to_string(),
            finalized_height,
            finalized_hash: hash(finalized_height).to_string(),
            base_checkpoint_height: RELEASE_STATE_BASE_HEIGHT,
            base_checkpoint_hash: RELEASE_STATE_BASE_HASH.to_string(),
            base_checkpoints_sha256: RELEASE_STATE_BASE_CHECKPOINTS_SHA256.to_string(),
            artifacts: Artifacts {
                block_metadata: artifact(BLOCK_METADATA_FILE, &metadata_bytes),
                frontier: artifact(FRONTIER_FILE, &frontier_bytes),
            },
        };
        fs::write(bundle_dir.join(BLOCK_METADATA_FILE), metadata_bytes)
            .expect("metadata file is written");
        fs::write(bundle_dir.join(FRONTIER_FILE), frontier_bytes)
            .expect("frontier file is written");
        fs::write(
            bundle_dir.join(MANIFEST_FILE),
            pretty_json(&manifest).expect("manifest serializes"),
        )
        .expect("manifest file is written");
        bundle_dir
    }

    fn write_test_source(root: &Path) {
        for relative in [CHECKPOINT_SOURCE_PATH, FRONTIER_SOURCE_PATH] {
            fs::create_dir_all(
                root.join(relative)
                    .parent()
                    .expect("source path has a parent"),
            )
            .expect("source directory is created");
        }
        let embedded_checkpoint_bytes =
            embedded_mainnet_checkpoint_bytes().expect("embedded checkpoints render");
        let embedded_checkpoints =
            parse_checkpoints(&embedded_checkpoint_bytes).expect("embedded checkpoints parse");
        let base_index = embedded_checkpoints
            .iter()
            .position(|checkpoint| checkpoint.height.0 == RELEASE_STATE_BASE_HEIGHT)
            .expect("embedded checkpoints contain the fixed release-state base");
        let fixed_prefix = render_checkpoints(&embedded_checkpoints[..=base_index])
            .expect("fixed checkpoint prefix renders");
        fs::write(root.join(CHECKPOINT_SOURCE_PATH), fixed_prefix)
            .expect("checkpoint source is written");
        fs::write(
            root.join(FRONTIER_SOURCE_PATH),
            hex::decode(include_str!("test-frontier.hex").trim())
                .expect("packaged frontier fixture is hexadecimal"),
        )
        .expect("frontier source is written");
    }

    fn source_snapshot(root: &Path) -> [Option<Vec<u8>>; 3] {
        [
            fs::read(root.join(CHECKPOINT_SOURCE_PATH)).ok(),
            fs::read(root.join(FRONTIER_SOURCE_PATH)).ok(),
            fs::read(root.join(PROVENANCE_SOURCE_PATH)).ok(),
        ]
    }

    fn rewrite_bundle_final_hash(bundle_dir: &Path) {
        let metadata_path = bundle_dir.join(BLOCK_METADATA_FILE);
        let mut metadata_bytes = fs::read(&metadata_path).expect("metadata is readable");
        let hash_offset = metadata_bytes.len() - METADATA_RECORD_LEN;
        metadata_bytes[hash_offset] ^= 1;
        fs::write(&metadata_path, &metadata_bytes).expect("conflicting metadata is written");

        let manifest_path = bundle_dir.join(MANIFEST_FILE);
        let mut manifest: Manifest =
            serde_json::from_slice(&fs::read(&manifest_path).expect("manifest is readable"))
                .expect("manifest parses");
        let hash_bytes: [u8; 32] = metadata_bytes[hash_offset..hash_offset + 32]
            .try_into()
            .expect("metadata hash has 32 bytes");
        manifest.finalized_hash = block::Hash(hash_bytes).to_string();
        manifest.artifacts.block_metadata = artifact(BLOCK_METADATA_FILE, &metadata_bytes);
        fs::write(
            manifest_path,
            pretty_json(&manifest).expect("manifest serializes"),
        )
        .expect("conflicting manifest is written");
    }

    proptest! {
        #[test]
        fn terminal_checkpoint_selection_keeps_every_gap_in_range(
            sizes in prop::collection::vec(1u32..=2_000_000, 17..2000)
        ) {
            let base = Checkpoint { height: Height(10_000), hash: hash(10_000) };
            let records = metadata(base.height.0, &sizes);
            let finalized = records.last().expect("non-empty generated metadata").height;
            let selected = select_checkpoints(base, &records).expect("valid metadata selects checkpoints");
            validate_checkpoint_gaps(base, &selected, finalized).expect("all selected gaps are valid");
            prop_assert_eq!(selected.last().map(|checkpoint| checkpoint.height), Some(finalized));
        }
    }

    #[test]
    fn terminal_tail_rebalances_across_max_gap() {
        let base = Checkpoint {
            height: Height(1_000),
            hash: hash(1_000),
        };
        for length in 17..=817 {
            let records = metadata(base.height.0, &vec![1; length]);
            let finalized = records.last().expect("non-empty metadata").height;
            let selected = select_checkpoints(base, &records).expect("tail selection succeeds");
            validate_checkpoint_gaps(base, &selected, finalized).expect("tail gaps are valid");
        }
    }

    #[test]
    fn metadata_round_trip_is_deterministic() {
        let records = metadata(100, &[1, 2_000_000, 42]);
        let encoded =
            encode_metadata(Height(100), Height(103), &records).expect("encoding succeeds");
        let decoded =
            decode_metadata(&encoded, Height(100), Height(103)).expect("decoding succeeds");
        assert_eq!(decoded, records);
        assert_eq!(
            encode_metadata(Height(100), Height(103), &decoded).expect("re-encoding succeeds"),
            encoded
        );
    }

    #[test]
    fn metadata_rejects_trailing_bytes() {
        let records = metadata(100, &[1]);
        let mut encoded =
            encode_metadata(Height(100), Height(101), &records).expect("encoding succeeds");
        encoded.push(0);
        assert!(decode_metadata(&encoded, Height(100), Height(101)).is_err());
    }

    #[test]
    fn import_is_idempotent_and_reuses_the_fixed_bundle_base() {
        let temp = tempfile::tempdir().expect("temporary directory is created");
        let source = temp.path().join("source");
        write_test_source(&source);

        let first_height = RELEASE_STATE_BASE_HEIGHT + 401;
        let first_bundle = write_test_bundle(temp.path(), first_height);
        import(&first_bundle, &source).expect("first fixed-base bundle imports");
        verify_source(&source, true).expect("first import has strict bundle provenance");
        let first_checkpoints =
            fs::read(source.join(CHECKPOINT_SOURCE_PATH)).expect("checkpoints are readable");
        let first_provenance =
            fs::read(source.join(PROVENANCE_SOURCE_PATH)).expect("provenance is readable");

        import(&first_bundle, &source).expect("reimport is an idempotent success");
        assert_eq!(
            fs::read(source.join(CHECKPOINT_SOURCE_PATH)).expect("checkpoints are readable"),
            first_checkpoints
        );
        assert_eq!(
            fs::read(source.join(PROVENANCE_SOURCE_PATH)).expect("provenance is readable"),
            first_provenance
        );

        let next_height = first_height + MIN_CHECKPOINT_HEIGHT_GAP;
        let next_bundle = write_test_bundle(temp.path(), next_height);
        import(&next_bundle, &source)
            .expect("later bundle with the same fixed publisher base imports");
        verify_source(&source, true).expect("later import has strict bundle provenance");
        let next_checkpoint_bytes =
            fs::read(source.join(CHECKPOINT_SOURCE_PATH)).expect("checkpoints are readable");
        assert!(next_checkpoint_bytes.starts_with(&first_checkpoints));
        let checkpoints =
            parse_checkpoints(&next_checkpoint_bytes).expect("updated checkpoints parse");
        assert_eq!(
            checkpoints.last().map(|checkpoint| checkpoint.height),
            Some(Height(next_height))
        );
    }

    #[test]
    fn rejected_imports_leave_source_files_unchanged() {
        let temp = tempfile::tempdir().expect("temporary directory is created");
        let source = temp.path().join("source");
        write_test_source(&source);
        let current_height = RELEASE_STATE_BASE_HEIGHT + 401;
        let current_bundle = write_test_bundle(temp.path(), current_height);
        import(&current_bundle, &source).expect("initial bundle imports");

        let before = source_snapshot(&source);
        let lower_root = temp.path().join("lower");
        fs::create_dir(&lower_root).expect("lower bundle root is created");
        let lower_bundle = write_test_bundle(&lower_root, current_height - 1);
        assert!(import(&lower_bundle, &source).is_err());
        assert_eq!(source_snapshot(&source), before);

        let conflict_root = temp.path().join("conflict");
        fs::create_dir(&conflict_root).expect("conflict bundle root is created");
        let conflict_bundle = write_test_bundle(&conflict_root, current_height);
        rewrite_bundle_final_hash(&conflict_bundle);
        assert!(import(&conflict_bundle, &source).is_err());
        assert_eq!(source_snapshot(&source), before);

        let frontier_root = temp.path().join("frontier-tamper");
        fs::create_dir(&frontier_root).expect("frontier bundle root is created");
        let frontier_bundle =
            write_test_bundle(&frontier_root, current_height + MIN_CHECKPOINT_HEIGHT_GAP);
        let frontier_path = frontier_bundle.join(FRONTIER_FILE);
        let mut frontier = fs::read(&frontier_path).expect("frontier is readable");
        frontier[10] ^= 1;
        fs::write(frontier_path, frontier).expect("tampered frontier is written");
        assert!(import(&frontier_bundle, &source).is_err());
        assert_eq!(source_snapshot(&source), before);

        let metadata_root = temp.path().join("metadata-tamper");
        fs::create_dir(&metadata_root).expect("metadata bundle root is created");
        let metadata_bundle =
            write_test_bundle(&metadata_root, current_height + MIN_CHECKPOINT_HEIGHT_GAP);
        let metadata_path = metadata_bundle.join(BLOCK_METADATA_FILE);
        let mut metadata_bytes = fs::read(&metadata_path).expect("metadata is readable");
        metadata_bytes[METADATA_HEADER_LEN] ^= 1;
        fs::write(metadata_path, metadata_bytes).expect("tampered metadata is written");
        assert!(import(&metadata_bundle, &source).is_err());
        assert_eq!(source_snapshot(&source), before);

        let checkpoint_path = source.join(CHECKPOINT_SOURCE_PATH);
        let checkpoints =
            String::from_utf8(fs::read(&checkpoint_path).expect("source checkpoints are readable"))
                .expect("source checkpoints are UTF-8");
        let conflicting_base = format!("1{}", &RELEASE_STATE_BASE_HASH[1..]);
        let checkpoints = checkpoints.replacen(RELEASE_STATE_BASE_HASH, &conflicting_base, 1);
        fs::write(&checkpoint_path, checkpoints).expect("conflicting base is written");
        let before_base_conflict = source_snapshot(&source);
        let valid_root = temp.path().join("valid-later");
        fs::create_dir(&valid_root).expect("valid bundle root is created");
        let valid_bundle =
            write_test_bundle(&valid_root, current_height + MIN_CHECKPOINT_HEIGHT_GAP);
        assert!(import(&valid_bundle, &source).is_err());
        assert_eq!(source_snapshot(&source), before_base_conflict);
    }
}
