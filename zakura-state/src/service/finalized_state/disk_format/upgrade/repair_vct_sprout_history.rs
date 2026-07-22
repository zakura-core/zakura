//! Backfill Sprout historical anchors omitted by the original VCT fast path.
//!
//! The migration is deliberately one RocksDB write batch: a crash leaves the
//! old format version in place, so startup replays the validated artifact again.

use std::sync::Arc;

use crossbeam_channel::{Receiver, TryRecvError};
use semver::Version;
use thiserror::Error;
use zakura_chain::{
    block::{self, Height},
    parameters::Network,
    sprout,
};

use crate::service::finalized_state::{
    vct::artifact::{embedded_mainnet, mainnet_artifact_identity, Artifact},
    DiskWriteBatch, ZakuraDb,
};

use super::{CancelFormatChange, DiskFormatUpgrade};

pub(crate) const REPAIR_VERSION: Version = Version::new(28, 0, 1);

/// Replays the reviewed Mainnet artifact into pre-28.0.1 VCT databases.
pub struct Upgrade {
    prepared_input: Option<Arc<RepairInput>>,
}

impl Upgrade {
    pub(super) fn new(prepared_input: Option<Arc<RepairInput>>) -> Self {
        Self { prepared_input }
    }
}

/// A successful read-only audit of VCT Sprout history.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VctSproutHistoryValidationSummary {
    /// The finalized database tip checked by the audit.
    pub finalized_tip: Height,
    /// The Sprout commitment root at the finalized database tip.
    pub sprout_root_at_finalized_tip: sprout::tree::Root,
    /// The persisted VCT handoff marker.
    pub vct_marker: Height,
    /// The handoff authenticated by the embedded artifact.
    pub artifact_handoff: Height,
    /// The empty anchor plus artifact-record anchors checked by the audit.
    pub checked_anchor_count: usize,
}

/// Errors returned by the read-only VCT Sprout-history audit.
#[derive(Debug, Error)]
pub enum VctSproutHistoryValidationError {
    /// The state database could not be opened read-only.
    #[error("could not open the state database read-only")]
    OpenDatabase(#[source] crate::StateInitError),
    /// The audit only has canonical inputs for Mainnet.
    #[error("VCT Sprout-history validation is only supported on Mainnet")]
    UnsupportedNetwork,
    /// The database was not created by VCT fast sync.
    #[error("the database does not have a persisted VCT marker")]
    NotVctSynced,
    /// The database has not durably completed the repair format.
    #[error(
        "database format {actual} predates required VCT Sprout-history repair format {required}"
    )]
    RepairNotCompleted {
        /// The version stored on disk, or `None` if no version was stored.
        actual: String,
        /// The first format version containing the repair.
        required: Version,
    },
    /// The database format version could not be read.
    #[error("could not read the database format version: {reason}")]
    FormatVersion {
        /// The underlying version-file error.
        reason: String,
    },
    /// The artifact, canonical indexes, or repaired records failed validation.
    #[error("VCT Sprout-history validation failed: {reason}")]
    Invalid {
        /// The failed artifact, index, anchor, or frontier check.
        reason: String,
    },
}

#[derive(Debug, Error)]
pub(crate) enum RepairValidationError {
    #[error(
        "the current checkpoint list has no canonical hash at reached database marker {height:?}"
    )]
    MissingMarkerCheckpoint { height: Height },
    #[error(transparent)]
    InvalidArtifact(#[from] crate::service::finalized_state::vct::artifact::Error),
    #[error("repair eligibility requires a persisted VCT marker")]
    MissingVctMarker,
    #[error("VCT metadata exists without a finalized database tip")]
    MissingFinalizedTip,
    #[error("the finalized database hash at reached VCT marker {height:?} is not canonical")]
    MarkerHashMismatch { height: Height },
}

pub(crate) struct RepairInput {
    artifact_last_checkpoint: Height,
    artifact_last_checkpoint_hash: block::Hash,
    artifact_sprout_root: sprout::tree::Root,
    artifact: Artifact,
}

pub(crate) fn is_repair_eligible(db: &ZakuraDb, disk_version: Option<&Version>) -> bool {
    db.network() == Network::Mainnet
        && db.is_vct_synced()
        && disk_version.is_some_and(|version| version < &REPAIR_VERSION)
}

pub(crate) fn prepare_startup_repair(
    db: &ZakuraDb,
) -> Result<Arc<RepairInput>, RepairValidationError> {
    let marker = db
        .vct_synced_below()
        .ok_or(RepairValidationError::MissingVctMarker)?;
    validated_repair_input(db, marker).map(Arc::new)
}

/// Audits a completed VCT Sprout-history repair without modifying the database.
pub(crate) fn validate_completed_repair(
    db: &ZakuraDb,
) -> Result<VctSproutHistoryValidationSummary, VctSproutHistoryValidationError> {
    if db.network() != Network::Mainnet {
        return Err(VctSproutHistoryValidationError::UnsupportedNetwork);
    }

    let marker = db
        .vct_synced_below()
        .ok_or(VctSproutHistoryValidationError::NotVctSynced)?;
    let disk_version = db.format_version_on_disk().map_err(|error| {
        VctSproutHistoryValidationError::FormatVersion {
            reason: error.to_string(),
        }
    })?;

    let input = validated_repair_input(db, marker).map_err(|error| {
        VctSproutHistoryValidationError::Invalid {
            reason: error.to_string(),
        }
    })?;
    let finalized_tip =
        db.finalized_tip_height()
            .ok_or_else(|| VctSproutHistoryValidationError::Invalid {
                reason: RepairValidationError::MissingFinalizedTip.to_string(),
            })?;
    let sprout_root_at_finalized_tip =
        db.sprout_tree_for_tip()
            .map(|tree| tree.root())
            .map_err(|error| VctSproutHistoryValidationError::Invalid {
                reason: error.to_string(),
            })?;
    let checked_anchor_count = input
        .artifact
        .records_through(finalized_tip.min(marker))
        .count()
        .saturating_add(1);
    let (_cancel_sender, cancel_receiver) = crossbeam_channel::bounded(1);
    validate_repaired_records(db, marker, &input, true, &cancel_receiver)
        .map_err(|_| VctSproutHistoryValidationError::Invalid {
            reason: "validation was unexpectedly cancelled".to_string(),
        })?
        .map_err(|reason| VctSproutHistoryValidationError::Invalid { reason })?;

    if disk_version
        .as_ref()
        .is_none_or(|version| version < &REPAIR_VERSION)
    {
        return Err(VctSproutHistoryValidationError::RepairNotCompleted {
            actual: disk_version
                .map_or_else(|| "unknown".to_string(), |version| version.to_string()),
            required: REPAIR_VERSION,
        });
    }

    Ok(VctSproutHistoryValidationSummary {
        finalized_tip,
        sprout_root_at_finalized_tip,
        vct_marker: marker,
        artifact_handoff: input.artifact_last_checkpoint,
        checked_anchor_count,
    })
}

impl DiskFormatUpgrade for Upgrade {
    fn version(&self) -> Version {
        REPAIR_VERSION
    }

    fn description(&self) -> &'static str {
        "repair historical Sprout anchors omitted by verified commitment tree sync"
    }

    #[allow(clippy::unwrap_in_result)]
    fn run(
        &self,
        _initial_tip_height: Height,
        db: &ZakuraDb,
        cancel_receiver: &Receiver<CancelFormatChange>,
    ) -> Result<(), CancelFormatChange> {
        check_cancelled(cancel_receiver)?;
        let disk_version = db
            .format_version_on_disk()
            .expect("database format version must remain readable during repair");
        if !is_repair_eligible(db, disk_version.as_ref()) {
            return Ok(());
        }

        let Some(last_vct_height) = db.vct_synced_below() else {
            unreachable!("repair eligibility requires the VCT handoff marker");
        };

        let input = self
            .prepared_input
            .as_deref()
            .expect("writable startup preflight provides the validated VCT Sprout repair input");
        repair_records(
            db,
            last_vct_height,
            input.artifact_last_checkpoint,
            input.artifact_last_checkpoint_hash,
            input.artifact_sprout_root,
            &input.artifact,
            cancel_receiver,
        )
    }

    fn validate(
        &self,
        db: &ZakuraDb,
        cancel_receiver: &Receiver<CancelFormatChange>,
    ) -> Result<Result<(), String>, CancelFormatChange> {
        check_cancelled(cancel_receiver)?;
        let disk_version = match db.format_version_on_disk() {
            Ok(version) => version,
            Err(error) => return Ok(Err(error.to_string())),
        };
        if !is_repair_eligible(db, disk_version.as_ref()) {
            return Ok(Ok(()));
        }

        let marker = db
            .vct_synced_below()
            .expect("a VCT-synced database has a persisted handoff marker");
        let input = self
            .prepared_input
            .as_deref()
            .expect("repair validation reuses the startup-prepared VCT Sprout repair input");

        validate_repaired_records(db, marker, input, false, cancel_receiver)
    }
}

fn repair_input(
    _db: &ZakuraDb,
) -> Result<(Height, block::Hash, sprout::tree::Root, Artifact), RepairValidationError> {
    #[cfg(test)]
    if let Some(input) = load_test_repair_input(_db) {
        let input = input?;
        return Ok((
            input.handoff,
            input.handoff_hash,
            input.handoff_sprout_root,
            input.artifact,
        ));
    }

    let artifact = embedded_mainnet()?;
    let (artifact_last_checkpoint, artifact_last_checkpoint_hash, artifact_sprout_root) =
        mainnet_artifact_identity();
    Ok((
        artifact_last_checkpoint,
        artifact_last_checkpoint_hash,
        artifact_sprout_root,
        artifact,
    ))
}

fn validated_repair_input(
    db: &ZakuraDb,
    marker: Height,
) -> Result<RepairInput, RepairValidationError> {
    let (artifact_last_checkpoint, artifact_last_checkpoint_hash, artifact_sprout_root, artifact) =
        repair_input(db)?;
    artifact.validate_last_checkpoint(
        artifact_last_checkpoint,
        artifact_last_checkpoint_hash,
        artifact_sprout_root,
    )?;

    let tip = db
        .finalized_tip_height()
        .ok_or(RepairValidationError::MissingFinalizedTip)?;
    if tip >= marker {
        let marker_hash = expected_marker_hash(
            db,
            marker,
            artifact_last_checkpoint,
            artifact_last_checkpoint_hash,
        )
        .ok_or(RepairValidationError::MissingMarkerCheckpoint { height: marker })?;
        if db.hash(marker) != Some(marker_hash) {
            return Err(RepairValidationError::MarkerHashMismatch { height: marker });
        }
    }
    artifact.validate_canonical_through(tip, |height| db.hash(height), |hash| db.height(hash))?;

    Ok(RepairInput {
        artifact_last_checkpoint,
        artifact_last_checkpoint_hash,
        artifact_sprout_root,
        artifact,
    })
}

#[allow(clippy::unwrap_in_result)]
fn repair_records(
    db: &ZakuraDb,
    marker: Height,
    artifact_handoff: Height,
    artifact_handoff_hash: block::Hash,
    artifact_sprout_root: sprout::tree::Root,
    artifact: &Artifact,
    cancel_receiver: &Receiver<CancelFormatChange>,
) -> Result<(), CancelFormatChange> {
    check_cancelled(cancel_receiver)?;
    artifact
        .validate_last_checkpoint(
            artifact_handoff,
            artifact_handoff_hash,
            artifact_sprout_root,
        )
        .expect("startup preflight validated the build artifact identity");
    let tip = db
        .finalized_tip_height()
        .expect("startup preflight validated the finalized database tip");
    if tip >= marker {
        let marker_hash = expected_marker_hash(db, marker, artifact_handoff, artifact_handoff_hash)
            .expect("startup preflight validated the reached database marker checkpoint");
        assert_eq!(
            db.hash(marker),
            Some(marker_hash),
            "startup preflight validated the reached database marker hash"
        );
    }
    artifact
        .validate_canonical_through(tip, |height| db.hash(height), |hash| db.height(hash))
        .expect("startup preflight validated canonical artifact records through the local tip");

    let replay_until = tip.min(marker);
    let mut tree = sprout::tree::NoteCommitmentTree::default();
    let mut batch = DiskWriteBatch::new();
    batch.insert_sprout_anchor(db, &tree);

    for record in artifact.records_through(replay_until) {
        check_cancelled(cancel_receiver)?;
        for commitment in &record.commitments {
            tree.append(*commitment)
                .expect("validated Sprout history record must fit in the Sprout tree");
        }
        debug_assert_eq!(tree.root(), record.resulting_root);
        batch.insert_sprout_anchor(db, &tree);
    }

    // At and above the marker, post-marker commits may have advanced the
    // current frontier. Only repair the stale tip when it is a prefix.
    if tip < marker {
        batch.update_sprout_tip(db, &tree);
    }

    check_cancelled(cancel_receiver)?;
    #[cfg(test)]
    if let Some(before_write) = test_repair_input(db).and_then(|input| input.before_write) {
        before_write();
    }
    check_cancelled(cancel_receiver)?;
    db.write_batch_sync(batch)
        .expect("atomic Sprout anchor repair batch should be writable");
    Ok(())
}

fn validate_repaired_records(
    db: &ZakuraDb,
    marker: Height,
    input: &RepairInput,
    check_tip_at_marker: bool,
    cancel_receiver: &Receiver<CancelFormatChange>,
) -> Result<Result<(), String>, CancelFormatChange> {
    let tip = db
        .finalized_tip_height()
        .expect("repair input validation requires a finalized database tip");
    let replay_until = tip.min(marker);
    let mut tree = sprout::tree::NoteCommitmentTree::default();

    if db.sprout_tree_by_anchor(&tree.root()).as_deref() != Some(&tree) {
        return Ok(Err(
            "the database is missing the empty Sprout anchor".to_string()
        ));
    }

    for record in input.artifact.records_through(replay_until) {
        check_cancelled(cancel_receiver)?;
        for commitment in &record.commitments {
            if tree.append(*commitment).is_err() {
                return Ok(Err(format!(
                    "the Sprout history tree is full at {:?}",
                    record.height
                )));
            }
        }

        if db.sprout_tree_by_anchor(&tree.root()).as_deref() != Some(&tree) {
            return Ok(Err(format!(
                "the database is missing the Sprout anchor at {:?}",
                record.height
            )));
        }
    }

    if tip < marker || (check_tip_at_marker && tip == marker) {
        let tip_tree = db.sprout_tree_for_tip().map_err(|error| error.to_string());
        if tip_tree.as_deref() != Ok(&tree) {
            return Ok(Err(format!(
                "the Sprout tip does not match the artifact at {tip:?}"
            )));
        }
    }

    Ok(Ok(()))
}

#[cfg(test)]
fn repair(
    db: &ZakuraDb,
    handoff: Height,
    handoff_hash: block::Hash,
    handoff_sprout_root: sprout::tree::Root,
    artifact: &Artifact,
    cancel_receiver: &Receiver<CancelFormatChange>,
) -> Result<(), CancelFormatChange> {
    repair_records(
        db,
        handoff,
        handoff,
        handoff_hash,
        handoff_sprout_root,
        artifact,
        cancel_receiver,
    )
}

fn expected_marker_hash(
    db: &ZakuraDb,
    marker: Height,
    artifact_handoff: Height,
    artifact_handoff_hash: block::Hash,
) -> Option<block::Hash> {
    #[cfg(test)]
    {
        if let Some(marker_hash) = test_repair_input(db).and_then(|input| input.marker_hash) {
            return Some(marker_hash);
        }
        if marker == artifact_handoff {
            return Some(artifact_handoff_hash);
        }
    }

    #[cfg(not(test))]
    let _ = (artifact_handoff, artifact_handoff_hash);

    db.network().checkpoint_list().hash(marker)
}

fn check_cancelled(
    cancel_receiver: &Receiver<CancelFormatChange>,
) -> Result<(), CancelFormatChange> {
    match cancel_receiver.try_recv() {
        Err(TryRecvError::Empty) => Ok(()),
        _ => Err(CancelFormatChange),
    }
}

#[cfg(test)]
#[derive(Clone)]
struct TestRepairInput {
    handoff: Height,
    handoff_hash: zakura_chain::block::Hash,
    handoff_sprout_root: sprout::tree::Root,
    artifact: Artifact,
    marker_hash: Option<block::Hash>,
    before_write: Option<std::sync::Arc<dyn Fn() + Send + Sync>>,
}

#[cfg(test)]
#[derive(Clone)]
enum TestRepairSource {
    Input(Box<TestRepairInput>),
    Error(crate::service::finalized_state::vct::artifact::Error),
}

#[cfg(test)]
struct TestRepairEntry {
    source: TestRepairSource,
    load_count: usize,
}

#[cfg(test)]
fn test_repair_inputs(
) -> &'static std::sync::Mutex<std::collections::HashMap<std::path::PathBuf, TestRepairEntry>> {
    static INPUTS: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<std::path::PathBuf, TestRepairEntry>>,
    > = std::sync::OnceLock::new();
    INPUTS.get_or_init(Default::default)
}

#[cfg(test)]
fn test_repair_input(db: &ZakuraDb) -> Option<TestRepairInput> {
    test_repair_inputs()
        .lock()
        .expect("test repair input mutex is not poisoned")
        .get(db.path())
        .and_then(|entry| match &entry.source {
            TestRepairSource::Input(input) => Some(input.as_ref().clone()),
            TestRepairSource::Error(_) => None,
        })
}

#[cfg(test)]
fn load_test_repair_input(
    db: &ZakuraDb,
) -> Option<Result<TestRepairInput, crate::service::finalized_state::vct::artifact::Error>> {
    let mut inputs = test_repair_inputs()
        .lock()
        .expect("test repair input mutex is not poisoned");
    let entry = inputs.get_mut(db.path())?;
    entry.load_count += 1;
    Some(match &entry.source {
        TestRepairSource::Input(input) => Ok(input.as_ref().clone()),
        TestRepairSource::Error(error) => Err(error.clone()),
    })
}

#[cfg(test)]
fn test_repair_input_load_count(path: &std::path::Path) -> usize {
    test_repair_inputs()
        .lock()
        .expect("test repair input mutex is not poisoned")
        .get(path)
        .map_or(0, |entry| entry.load_count)
}

#[cfg(test)]
pub(super) fn has_test_repair_input(db: &ZakuraDb) -> bool {
    test_repair_input(db).is_some()
}

#[cfg(test)]
struct TestRepairInputGuard {
    path: std::path::PathBuf,
}

#[cfg(test)]
impl Drop for TestRepairInputGuard {
    fn drop(&mut self) {
        test_repair_inputs()
            .lock()
            .expect("test repair input mutex is not poisoned")
            .remove(&self.path);
    }
}

#[cfg(test)]
fn inject_test_repair_input(
    path: std::path::PathBuf,
    input: TestRepairInput,
) -> TestRepairInputGuard {
    assert!(
        test_repair_inputs()
            .lock()
            .expect("test repair input mutex is not poisoned")
            .insert(
                path.clone(),
                TestRepairEntry {
                    source: TestRepairSource::Input(Box::new(input)),
                    load_count: 0,
                },
            )
            .is_none(),
        "only one repair input can be injected per database"
    );
    TestRepairInputGuard { path }
}

#[cfg(test)]
fn inject_test_repair_error(
    path: std::path::PathBuf,
    error: crate::service::finalized_state::vct::artifact::Error,
) -> TestRepairInputGuard {
    assert!(
        test_repair_inputs()
            .lock()
            .expect("test repair input mutex is not poisoned")
            .insert(
                path.clone(),
                TestRepairEntry {
                    source: TestRepairSource::Error(error),
                    load_count: 0,
                },
            )
            .is_none(),
        "only one repair input can be injected per database"
    );
    TestRepairInputGuard { path }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use crossbeam_channel::{bounded, RecvTimeoutError};
    use tempfile::TempDir;
    use zakura_chain::{
        block,
        parameters::Network,
        primitives::{ed25519, x25519, Groth16Proof},
        serialization::ZcashDeserializeInto,
        sprout::{self, tree::NoteCommitmentTree},
        transaction::{LockTime, Transaction},
        transparent,
    };

    use crate::{
        constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
        service::finalized_state::{
            commitment_aux::{FinalFrontiers, FixtureSource},
            disk_db::WriteDisk,
            vct::artifact::{Artifact, Error as ArtifactError, Record},
            CheckpointVerifiedBlock, DiskWriteBatch, FinalizedState, ZakuraDb,
            STATE_COLUMN_FAMILIES_IN_CODE,
        },
        Config, MissingSproutTipTree,
    };

    use super::*;

    fn persistent_config() -> (TempDir, Config) {
        let cache = tempfile::tempdir().expect("temporary cache directory is created");
        let config = Config {
            cache_dir: cache.path().to_path_buf(),
            ephemeral: false,
            ..Config::default()
        };
        (cache, config)
    }

    fn ephemeral_db(network: &Network) -> ZakuraDb {
        ZakuraDb::new(
            &Config::ephemeral(),
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            network,
            true,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            false,
        )
        .expect("ephemeral database opens")
    }

    fn mark_vct(db: &ZakuraDb, handoff: Height) {
        let mut batch = DiskWriteBatch::new();
        batch.update_vct_sync_marker(db, handoff);
        db.write_batch(batch).expect("VCT marker write succeeds");
    }

    fn seed_canonical(db: &ZakuraDb, entries: &[(Height, block::Hash)]) {
        let hash_by_height = db.db().cf_handle("hash_by_height").unwrap();
        let height_by_hash = db.db().cf_handle("height_by_hash").unwrap();
        let mut batch = DiskWriteBatch::new();
        for (height, hash) in entries {
            batch.zs_insert(&hash_by_height, height, hash);
            batch.zs_insert(&height_by_hash, hash, height);
        }
        db.write_batch(batch)
            .expect("canonical block index writes succeed");
    }

    fn fixture_artifact(
        handoff: Height,
        handoff_hash: block::Hash,
        record_height: Height,
        record_hash: block::Hash,
        value: u8,
    ) -> (Artifact, sprout::tree::Root) {
        let mut tree = NoteCommitmentTree::default();
        let commitment = sprout::commitment::NoteCommitment::from([value; 32]);
        tree.append(commitment).expect("fixture tree has capacity");
        let root = tree.root();
        let bytes = Artifact::encode(
            handoff,
            handoff_hash,
            [Record {
                height: record_height,
                block_hash: record_hash,
                commitments: vec![commitment],
                resulting_root: root,
            }],
        )
        .expect("fixture artifact encodes");
        (
            Artifact::decode(&bytes).expect("fixture artifact decodes"),
            root,
        )
    }

    fn fixture_artifact_with_records(
        handoff: Height,
        handoff_hash: block::Hash,
        records: &[(Height, block::Hash, u8)],
    ) -> (Artifact, Vec<sprout::tree::Root>) {
        let mut tree = NoteCommitmentTree::default();
        let mut roots = Vec::new();
        let records = records.iter().map(|(height, hash, value)| {
            let commitment = sprout::commitment::NoteCommitment::from([*value; 32]);
            tree.append(commitment).expect("fixture tree has capacity");
            let resulting_root = tree.root();
            roots.push(resulting_root);
            Record {
                height: *height,
                block_hash: *hash,
                commitments: vec![commitment],
                resulting_root,
            }
        });
        let bytes =
            Artifact::encode(handoff, handoff_hash, records).expect("fixture artifact encodes");
        (
            Artifact::decode(&bytes).expect("fixture artifact decodes"),
            roots,
        )
    }

    fn fixed_sprout_transaction(value: u8) -> Arc<Transaction> {
        let joinsplit = sprout::JoinSplit::<Groth16Proof> {
            vpub_old: Default::default(),
            vpub_new: Default::default(),
            anchor: sprout::tree::Root::default(),
            nullifiers: [
                sprout::note::Nullifier::from([value; 32]),
                sprout::note::Nullifier::from([value.wrapping_add(1); 32]),
            ],
            commitments: [
                sprout::commitment::NoteCommitment::from([value.wrapping_add(2); 32]),
                sprout::commitment::NoteCommitment::from([value.wrapping_add(3); 32]),
            ],
            ephemeral_key: x25519::PublicKey::from([value.wrapping_add(4); 32]),
            random_seed: sprout::RandomSeed::from([value.wrapping_add(5); 32]),
            vmacs: [
                sprout::note::Mac::from([value.wrapping_add(6); 32]),
                sprout::note::Mac::from([value.wrapping_add(7); 32]),
            ],
            zkproof: Groth16Proof::from([0; 192]),
            enc_ciphertexts: [
                sprout::note::EncryptedNote([0; 601]),
                sprout::note::EncryptedNote([0; 601]),
            ],
        };

        Arc::new(Transaction::V4 {
            inputs: Vec::new(),
            outputs: Vec::new(),
            lock_time: LockTime::unlocked(),
            expiry_height: Height(0),
            joinsplit_data: Some(zakura_chain::transaction::JoinSplitData {
                first: joinsplit,
                rest: Vec::new(),
                pub_key: ed25519::VerificationKeyBytes::from([value.wrapping_add(8); 32]),
                sig: ed25519::Signature::from([0; 64]),
            }),
            sapling_shielded_data: None,
        })
    }

    fn fixed_child_block(
        parent: &block::Block,
        height: Height,
        sprout_value: Option<u8>,
    ) -> Arc<block::Block> {
        let mut block: block::Block = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into()
            .expect("Mainnet block 1 deserializes");
        let coinbase = Arc::make_mut(
            block
                .transactions
                .first_mut()
                .expect("Mainnet block 1 has a coinbase transaction"),
        );
        match coinbase {
            Transaction::V1 { inputs, .. } => match &mut inputs[0] {
                transparent::Input::Coinbase {
                    height: coinbase_height,
                    ..
                } => *coinbase_height = height,
                _ => panic!("Mainnet block 1 transaction 0 is a coinbase"),
            },
            _ => panic!("Mainnet block 1 has a V1 coinbase transaction"),
        }
        if let Some(value) = sprout_value {
            block.transactions.push(fixed_sprout_transaction(value));
        }
        Arc::make_mut(&mut block.header).previous_block_hash = parent.hash();
        Arc::new(block)
    }

    fn height_index(height: Height) -> usize {
        usize::try_from(height.0).expect("small deterministic test heights fit in usize")
    }

    fn seed_repair_db(
        config: &Config,
        network: &Network,
        handoff: Height,
        entries: &[(Height, block::Hash)],
    ) -> std::path::PathBuf {
        seed_repair_db_with_tip(
            config,
            network,
            handoff,
            entries,
            &NoteCommitmentTree::default(),
        )
    }

    fn seed_repair_db_with_tip(
        config: &Config,
        network: &Network,
        handoff: Height,
        entries: &[(Height, block::Hash)],
        tip_tree: &NoteCommitmentTree,
    ) -> std::path::PathBuf {
        let db = ZakuraDb::new(
            config,
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            network,
            true,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            false,
        )
        .expect("repair fixture database opens");
        seed_canonical(&db, entries);
        mark_vct(&db, handoff);
        let mut tip_batch = DiskWriteBatch::new();
        tip_batch.update_sprout_tree(&db, tip_tree);
        db.write_batch(tip_batch)
            .expect("fixture Sprout tip write succeeds");
        db.update_format_version_on_disk(&Version::new(28, 0, 0))
            .expect("fixture old version write succeeds");
        let path = db.path().to_path_buf();
        drop(db);
        path
    }

    fn open_normally(config: &Config, network: &Network) -> ZakuraDb {
        ZakuraDb::new(
            config,
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            network,
            false,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            false,
        )
        .expect("normal startup succeeds")
    }

    #[test]
    fn eligibility_is_mainnet_vct_and_pre_repair_only() {
        let mainnet = ephemeral_db(&Network::Mainnet);
        let regtest = ephemeral_db(&Network::new_regtest(Default::default()));
        let handoff = Height(2);
        let old = Version::new(28, 0, 0);

        assert!(!is_repair_eligible(&mainnet, Some(&old)));
        mark_vct(&mainnet, handoff);
        mark_vct(&regtest, handoff);

        assert!(is_repair_eligible(&mainnet, Some(&old)));
        assert!(!is_repair_eligible(&mainnet, Some(&REPAIR_VERSION)));
        assert!(!is_repair_eligible(&mainnet, None));
        assert!(!is_repair_eligible(&regtest, Some(&old)));
    }

    #[test]
    fn repair_is_atomic_repeatable_and_preserves_post_handoff_tip() {
        let db = ephemeral_db(&Network::Mainnet);
        let record_height = Height(1);
        let handoff = Height(2);
        let record_hash = block::Hash([1; 32]);
        let handoff_hash = block::Hash([2; 32]);
        let old_version = Version::new(28, 0, 0);
        db.update_format_version_on_disk(&old_version)
            .expect("fixture version write succeeds");
        seed_canonical(
            &db,
            &[(record_height, record_hash), (handoff, handoff_hash)],
        );
        let (artifact, repaired_root) =
            fixture_artifact(handoff, handoff_hash, record_height, record_hash, 7);
        mark_vct(&db, handoff);
        let _injection = inject_test_repair_input(
            db.path().to_path_buf(),
            TestRepairInput {
                handoff,
                handoff_hash,
                handoff_sprout_root: repaired_root,
                artifact: artifact.clone(),
                marker_hash: None,
                before_write: None,
            },
        );

        let mut existing_tip = NoteCommitmentTree::default();
        existing_tip
            .append(sprout::commitment::NoteCommitment::from([9; 32]))
            .expect("fixture tree has capacity");
        let mut tip_batch = DiskWriteBatch::new();
        tip_batch.update_sprout_tree(&db, &existing_tip);
        db.write_batch(tip_batch).expect("tip write succeeds");

        let (_cancel_tx, cancel_rx) = bounded(1);
        repair(
            &db,
            handoff,
            handoff_hash,
            repaired_root,
            &artifact,
            &cancel_rx,
        )
        .expect("first repair succeeds");
        repair(
            &db,
            handoff,
            handoff_hash,
            repaired_root,
            &artifact,
            &cancel_rx,
        )
        .expect("repeated repair succeeds");

        assert!(db.contains_sprout_anchor(&repaired_root));
        assert_eq!(
            db.sprout_tree_for_tip()
                .expect("fixture has a Sprout tip tree")
                .root(),
            existing_tip.root()
        );
        assert_eq!(
            db.format_version_on_disk()
                .expect("fixture version remains readable"),
            Some(old_version),
            "the durable repair batch must complete before the upgrade framework updates the version"
        );

        let upgrade = Upgrade::new(Some(
            prepare_startup_repair(&db).expect("fixture repair input validates"),
        ));
        assert_eq!(upgrade.validate(&db, &cancel_rx), Ok(Ok(())));
        let mut delete_batch = DiskWriteBatch::new();
        delete_batch.delete_sprout_anchor(&db, &repaired_root);
        db.write_batch(delete_batch)
            .expect("corrupting the repaired fixture succeeds");
        assert!(matches!(
            upgrade.validate(&db, &cancel_rx),
            Ok(Err(reason)) if reason.contains("Sprout anchor at")
        ));
    }

    #[test]
    fn registered_startup_repairs_completed_pruned_history_before_returning() {
        let (_cache, config) = persistent_config();
        let network = Network::Mainnet;
        let first_height = Height(1);
        let handoff = Height(2);
        let first_hash = block::Hash([1; 32]);
        let handoff_hash = block::Hash([2; 32]);
        let (artifact, roots) = fixture_artifact_with_records(
            handoff,
            handoff_hash,
            &[(first_height, first_hash, 11), (handoff, handoff_hash, 12)],
        );
        let mut tip_tree = NoteCommitmentTree::default();
        for value in [11, 12] {
            tip_tree
                .append(sprout::commitment::NoteCommitment::from([value; 32]))
                .expect("fixture tree has capacity");
        }
        let path = seed_repair_db_with_tip(
            &config,
            &network,
            handoff,
            &[(first_height, first_hash), (handoff, handoff_hash)],
            &tip_tree,
        );
        let _injection = inject_test_repair_input(
            path.clone(),
            TestRepairInput {
                handoff,
                handoff_hash,
                handoff_sprout_root: roots[1],
                artifact,
                marker_hash: None,
                before_write: None,
            },
        );

        let db = open_normally(&config, &network);

        assert!(
            roots.iter().all(|root| db.contains_sprout_anchor(root)),
            "startup must restore every historical anchor before returning the database"
        );
        assert_eq!(
            db.format_version_on_disk()
                .expect("version remains readable"),
            Some(state_database_format_version_in_code())
        );
        assert_eq!(db.hash(first_height), Some(first_hash));
        assert_eq!(db.height(first_hash), Some(first_height));
        assert_eq!(
            test_repair_input_load_count(&path),
            1,
            "startup preparation must load the repair artifact exactly once"
        );
        assert!(
            db.block(first_height.into()).is_none(),
            "repair uses retained canonical indexes and does not require pruned bodies"
        );

        drop(db);
        let db = open_normally(&config, &network);
        assert_eq!(
            test_repair_input_load_count(&path),
            1,
            "reopening the completed repair format must not load the artifact"
        );

        let summary =
            validate_completed_repair(&db).expect("the completed pruned repair validates");
        assert_eq!(
            summary,
            VctSproutHistoryValidationSummary {
                finalized_tip: handoff,
                sprout_root_at_finalized_tip: tip_tree.root(),
                vct_marker: handoff,
                artifact_handoff: handoff,
                checked_anchor_count: 3,
            }
        );

        let mut corrupt_batch = DiskWriteBatch::new();
        corrupt_batch.delete_sprout_anchor(&db, &roots[0]);
        db.write_batch(corrupt_batch)
            .expect("fixture anchor corruption succeeds");
        assert!(matches!(
            validate_completed_repair(&db),
            Err(VctSproutHistoryValidationError::Invalid { reason })
                if reason.contains("Sprout anchor at")
        ));
    }

    #[test]
    fn startup_preserves_artifact_validation_error_without_writes() {
        let (_cache, config) = persistent_config();
        let network = Network::Mainnet;
        let handoff = Height(1);
        let hash = block::Hash([1; 32]);
        let path = seed_repair_db(&config, &network, handoff, &[(handoff, hash)]);
        let _injection = inject_test_repair_error(path.clone(), ArtifactError::DigestMismatch);

        let open = ZakuraDb::new(
            &config,
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            &network,
            false,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            false,
        );
        assert!(matches!(
            open,
            Err(crate::StateInitError::VctSproutHistoryRepairInvalid { reason })
                if reason.contains("artifact payload digest does not match")
        ));
        assert_eq!(
            test_repair_input_load_count(&path),
            1,
            "startup reports the first artifact preparation failure directly"
        );

        let db = ZakuraDb::new_for_vct_sprout_history_validation(
            &config,
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            &network,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
        )
        .expect("read-only audit open bypasses the normal repair guard");
        assert_eq!(
            db.format_version_on_disk()
                .expect("fixture version remains readable"),
            Some(Version::new(28, 0, 0)),
            "failed artifact preparation must not advance the format version"
        );
    }

    #[test]
    fn audit_opens_pre_repair_database_and_reports_missing_history() {
        let (_cache, config) = persistent_config();
        let network = Network::Mainnet;
        let handoff = Height(1);
        let hash = block::Hash([1; 32]);
        let (artifact, roots) =
            fixture_artifact_with_records(handoff, hash, &[(handoff, hash, 17)]);
        let mut tip_tree = NoteCommitmentTree::default();
        tip_tree
            .append(sprout::commitment::NoteCommitment::from([17; 32]))
            .expect("fixture tree has capacity");
        let path =
            seed_repair_db_with_tip(&config, &network, handoff, &[(handoff, hash)], &tip_tree);
        let _injection = inject_test_repair_input(
            path,
            TestRepairInput {
                handoff,
                handoff_hash: hash,
                handoff_sprout_root: roots[0],
                artifact,
                marker_hash: None,
                before_write: None,
            },
        );

        let db = ZakuraDb::new_for_vct_sprout_history_validation(
            &config,
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            &network,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
        )
        .expect("the explicit audit can open an old database read-only");

        assert!(matches!(
            validate_completed_repair(&db),
            Err(VctSproutHistoryValidationError::Invalid { reason })
                if reason.contains("empty Sprout anchor")
        ));
    }

    #[test]
    fn completed_repair_audit_detects_canonical_reverse_index_corruption() {
        let (_cache, config) = persistent_config();
        let network = Network::Mainnet;
        let record_height = Height(1);
        let handoff = Height(2);
        let record_hash = block::Hash([1; 32]);
        let handoff_hash = block::Hash([2; 32]);
        let (artifact, roots) =
            fixture_artifact(handoff, handoff_hash, record_height, record_hash, 13);
        let mut tip_tree = NoteCommitmentTree::default();
        tip_tree
            .append(sprout::commitment::NoteCommitment::from([13; 32]))
            .expect("fixture tree has capacity");
        let path = seed_repair_db_with_tip(
            &config,
            &network,
            handoff,
            &[(record_height, record_hash), (handoff, handoff_hash)],
            &tip_tree,
        );
        let _injection = inject_test_repair_input(
            path,
            TestRepairInput {
                handoff,
                handoff_hash,
                handoff_sprout_root: roots,
                artifact,
                marker_hash: None,
                before_write: None,
            },
        );
        let db = open_normally(&config, &network);
        validate_completed_repair(&db).expect("the completed repair initially validates");

        let height_by_hash = db.db().cf_handle("height_by_hash").unwrap();
        let mut corrupt_batch = DiskWriteBatch::new();
        corrupt_batch.zs_delete(&height_by_hash, record_hash);
        db.write_batch(corrupt_batch)
            .expect("fixture reverse-index corruption succeeds");

        assert!(matches!(
            validate_completed_repair(&db),
            Err(VctSproutHistoryValidationError::Invalid { reason })
                if reason.contains("reverse block index")
        ));
    }

    #[test]
    fn startup_accepts_build_handoff_above_db_marker_and_preserves_post_marker_tip() {
        let (_cache, config) = persistent_config();
        let network = Network::Mainnet;
        let marker = Height(2);
        let tip = Height(3);
        let artifact_handoff = Height(4);
        let hashes = [
            block::Hash([1; 32]),
            block::Hash([2; 32]),
            block::Hash([3; 32]),
            block::Hash([4; 32]),
        ];
        let (artifact, roots) = fixture_artifact_with_records(
            artifact_handoff,
            hashes[3],
            &[
                (Height(1), hashes[0], 71),
                (marker, hashes[1], 72),
                (tip, hashes[2], 73),
                (artifact_handoff, hashes[3], 74),
            ],
        );
        let mut truthful_tip = NoteCommitmentTree::default();
        for value in [71, 72, 73] {
            truthful_tip
                .append(sprout::commitment::NoteCommitment::from([value; 32]))
                .expect("fixture tree has capacity");
        }
        assert_eq!(truthful_tip.root(), roots[2]);

        let path = seed_repair_db_with_tip(
            &config,
            &network,
            marker,
            &[
                (Height(1), hashes[0]),
                (marker, hashes[1]),
                (tip, hashes[2]),
            ],
            &truthful_tip,
        );
        let _injection = inject_test_repair_input(
            path,
            TestRepairInput {
                handoff: artifact_handoff,
                handoff_hash: hashes[3],
                handoff_sprout_root: roots[3],
                artifact,
                marker_hash: Some(hashes[1]),
                before_write: None,
            },
        );

        let db = open_normally(&config, &network);

        assert_eq!(db.vct_synced_below(), Some(marker));
        assert!(db.contains_sprout_anchor(&roots[0]));
        assert!(db.contains_sprout_anchor(&roots[1]));
        assert_eq!(
            db.sprout_tree_for_tip()
                .expect("post-marker tip remains available")
                .root(),
            roots[2],
            "repair through the marker must not replace truthful post-marker state"
        );
        assert_eq!(
            db.format_version_on_disk()
                .expect("version remains readable"),
            Some(state_database_format_version_in_code())
        );
    }

    #[test]
    fn startup_marker_mismatch_is_a_typed_initialization_error() {
        let (_cache, config) = persistent_config();
        let network = Network::Mainnet;
        let marker = Height(1);
        let artifact_handoff = Height(2);
        let canonical_marker_hash = block::Hash([1; 32]);
        let local_marker_hash = block::Hash([9; 32]);
        let artifact_handoff_hash = block::Hash([2; 32]);
        let path = seed_repair_db(&config, &network, marker, &[(marker, local_marker_hash)]);
        let (artifact, roots) = fixture_artifact_with_records(
            artifact_handoff,
            artifact_handoff_hash,
            &[
                (marker, canonical_marker_hash, 81),
                (artifact_handoff, artifact_handoff_hash, 82),
            ],
        );
        let _injection = inject_test_repair_input(
            path,
            TestRepairInput {
                handoff: artifact_handoff,
                handoff_hash: artifact_handoff_hash,
                handoff_sprout_root: roots[1],
                artifact,
                marker_hash: Some(canonical_marker_hash),
                before_write: None,
            },
        );

        let result = ZakuraDb::new(
            &config,
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            &network,
            false,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            false,
        );

        assert!(matches!(
            result,
            Err(crate::StateInitError::VctSproutHistoryRepairInvalid { reason })
                if reason.contains("reached VCT marker")
        ));
    }

    #[test]
    fn interrupted_prefix_tip_repair_is_repeatable_and_reruns_before_version_completion() {
        let (_cache, config) = persistent_config();
        let network = Network::Mainnet;
        let tip = Height(1);
        let tip_hash = block::Hash([1; 32]);
        let handoff = network.checkpoint_list().max_height();
        let handoff_hash = network
            .checkpoint_list()
            .hash(handoff)
            .expect("maximum checkpoint has a hash");
        let db = ZakuraDb::new(
            &config,
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            &network,
            true,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            false,
        )
        .expect("repair fixture database opens");
        seed_canonical(&db, &[(tip, tip_hash)]);
        mark_vct(&db, handoff);
        db.update_format_version_on_disk(&Version::new(28, 0, 0))
            .expect("fixture old version write succeeds");
        let path = db.path().to_path_buf();
        let (artifact, roots) = fixture_artifact_with_records(
            handoff,
            handoff_hash,
            &[(tip, tip_hash, 21), (Height(2), block::Hash([2; 32]), 22)],
        );
        let input = TestRepairInput {
            handoff,
            handoff_hash,
            handoff_sprout_root: roots[1],
            artifact,
            marker_hash: None,
            before_write: None,
        };
        let _injection = inject_test_repair_input(path, input.clone());

        let (_cancel_tx, cancel_rx) = bounded(1);
        let upgrade = Upgrade::new(Some(
            prepare_startup_repair(&db).expect("fixture repair input validates"),
        ));
        upgrade
            .run(tip, &db, &cancel_rx)
            .expect("simulated pre-version repair succeeds");
        upgrade
            .run(tip, &db, &cancel_rx)
            .expect("same old-version repair is idempotent");
        assert_eq!(
            db.sprout_tree_for_tip()
                .expect("prefix repair restores the current tip tree")
                .root(),
            roots[0]
        );
        assert_eq!(
            db.format_version_on_disk()
                .expect("old version remains readable"),
            Some(Version::new(28, 0, 0)),
            "a crash after the data batch but before framework completion leaves repair eligible"
        );
        drop(db);

        let db = open_normally(&config, &network);
        assert!(db.contains_sprout_anchor(&roots[0]));
        assert!(!db.contains_sprout_anchor(&roots[1]));
        assert_eq!(
            db.format_version_on_disk()
                .expect("version remains readable"),
            Some(state_database_format_version_in_code())
        );
    }

    #[test]
    fn interrupted_repair_reopens_and_commits_multiple_blocks_through_handoff() {
        let (_cache, config) = persistent_config();
        let network = Network::Mainnet;
        let genesis: Arc<block::Block> = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
            .zcash_deserialize_into()
            .expect("Mainnet genesis deserializes");
        let tip = Height(1);
        let handoff = Height(4);
        let mut blocks = vec![genesis];
        for (height, sprout_value) in [(1, Some(10)), (2, None), (3, Some(30)), (4, Some(40))] {
            blocks.push(fixed_child_block(
                blocks.last().expect("the fixed chain always has a parent"),
                Height(height),
                sprout_value,
            ));
        }
        let handoff_hash = blocks[height_index(handoff)].hash();

        let mut golden = FinalizedState::new(
            &Config::ephemeral(),
            &network,
            #[cfg(feature = "elasticsearch")]
            false,
        )
        .expect("golden state opens");
        let mut golden_trees = Vec::new();
        for block in &blocks {
            let (_, trees) = golden
                .commit_finalized_direct(
                    CheckpointVerifiedBlock::from(block.clone()).into(),
                    None,
                    None,
                    "interrupted Sprout repair golden chain",
                )
                .expect("the deterministic golden chain commits");
            golden_trees.push(trees);
        }
        let handoff_trees = golden_trees[height_index(handoff)].clone();

        let records = [Height(1), Height(3), handoff].map(|height| Record {
            height,
            block_hash: blocks[height_index(height)].hash(),
            commitments: blocks[height_index(height)]
                .sprout_note_commitments()
                .copied()
                .collect(),
            resulting_root: golden_trees[height_index(height)].sprout.root(),
        });
        let artifact = Artifact::decode(
            &Artifact::encode(handoff, handoff_hash, records).expect("fixture artifact encodes"),
        )
        .expect("fixture artifact globally replays");

        let mut prefix = FinalizedState::new(
            &config,
            &network,
            #[cfg(feature = "elasticsearch")]
            false,
        )
        .expect("repair fixture state opens");
        for block in blocks.iter().take(height_index(tip) + 1) {
            prefix
                .commit_finalized_direct(
                    CheckpointVerifiedBlock::from(block.clone()).into(),
                    None,
                    None,
                    "interrupted Sprout repair prefix",
                )
                .expect("the deterministic prefix commits");
        }
        let prefix_root = golden_trees[height_index(tip)].sprout.root();
        assert_ne!(
            prefix_root,
            NoteCommitmentTree::default().root(),
            "the interrupted prefix must already contain Sprout commitments"
        );

        let mut stale_trees = DiskWriteBatch::new();
        stale_trees.delete_sprout_anchor(&prefix.db, &prefix_root);
        stale_trees.update_sprout_tip(&prefix.db, &NoteCommitmentTree::default());
        stale_trees.update_vct_sync_marker(&prefix.db, handoff);
        prefix
            .db
            .write_batch(stale_trees)
            .expect("stale interrupted frontiers are seeded");
        prefix
            .db
            .update_format_version_on_disk(&Version::new(28, 0, 0))
            .expect("fixture old version write succeeds");
        let path = prefix.db.path().to_path_buf();
        drop(prefix);

        let _injection = inject_test_repair_input(
            path,
            TestRepairInput {
                handoff,
                handoff_hash,
                handoff_sprout_root: handoff_trees.sprout.root(),
                artifact: artifact.clone(),
                marker_hash: None,
                before_write: None,
            },
        );

        let repaired = FinalizedState::new(
            &config,
            &network,
            #[cfg(feature = "elasticsearch")]
            false,
        )
        .expect("restart synchronously repairs the interrupted prefix");
        assert_eq!(repaired.db.finalized_tip_height(), Some(tip));
        assert_eq!(
            repaired
                .db
                .sprout_tree_for_tip()
                .expect("repair restores a current Sprout frontier")
                .root(),
            prefix_root
        );
        assert!(
            repaired.db.sprout_tree_by_anchor(&prefix_root).is_some(),
            "startup repair restores the non-empty interrupted historical frontier"
        );
        assert_eq!(
            repaired
                .db
                .format_version_on_disk()
                .expect("version remains readable"),
            Some(state_database_format_version_in_code())
        );
        drop(repaired);

        let mut state = FinalizedState::new(
            &config,
            &network,
            #[cfg(feature = "elasticsearch")]
            false,
        )
        .expect("the version-complete repaired state reopens");
        assert_eq!(
            state
                .db
                .sprout_tree_for_tip()
                .expect("the repaired frontier survives the second reopen")
                .root(),
            prefix_root
        );
        state.enable_vct_fast_source(
            Box::new(FixtureSource::new(
                ((tip.0 + 1)..=handoff.0)
                    .map(|height| {
                        let trees = &golden_trees[height_index(Height(height))];
                        (
                            height,
                            (
                                trees.sapling.root(),
                                trees.orchard.root(),
                                trees.ironwood.root(),
                            ),
                        )
                    })
                    .collect(),
                FinalFrontiers {
                    height: handoff,
                    sapling: handoff_trees.sapling.clone(),
                    orchard: handoff_trees.orchard.clone(),
                    sprout: handoff_trees.sprout.clone(),
                    ironwood: handoff_trees.ironwood.clone(),
                },
            )),
            false,
        );

        for height in (tip.0 + 1)..=handoff.0 {
            let next = (height < handoff.0).then(|| {
                let block = &blocks[height_index(Height(height + 1))];
                crate::service::finalized_state::NextVctBlock::from_header(
                    block.header.clone(),
                    Height(height + 1),
                    block.auth_data_root(),
                )
            });
            state
                .commit_finalized_direct(
                    CheckpointVerifiedBlock::from(blocks[height_index(Height(height))].clone())
                        .into(),
                    None,
                    next,
                    "interrupted Sprout repair continuation",
                )
                .expect("the normal fast-path committer resumes through the handoff");

            let expected_root = golden_trees[height_index(Height(height))].sprout.root();
            assert_eq!(
                state
                    .db
                    .sprout_tree_for_tip()
                    .expect("every continuation commit has a current Sprout frontier")
                    .root(),
                expected_root,
                "the current Sprout frontier matches after height {height}"
            );
            for historical_height in [Height(1), Height(3), handoff]
                .into_iter()
                .filter(|historical_height| historical_height.0 <= height)
            {
                let historical_root = golden_trees[height_index(historical_height)].sprout.root();
                assert_eq!(
                    state
                        .db
                        .sprout_tree_by_anchor(&historical_root)
                        .expect("every reached Sprout anchor has its historical frontier")
                        .root(),
                    historical_root,
                    "the historical Sprout anchor at {historical_height:?} survives continuation"
                );
            }
        }

        assert_eq!(
            state.vct_fast_count(),
            u64::from(handoff.0 - tip.0),
            "every post-repair continuation block used the normal VCT fast path"
        );
        assert_eq!(state.db.finalized_tip_height(), Some(handoff));
        assert_eq!(state.db.hash(handoff), Some(handoff_hash));
        assert_eq!(
            state
                .db
                .sprout_tree_for_tip()
                .expect("the resumed commit writes the handoff Sprout frontier")
                .root(),
            handoff_trees.sprout.root()
        );
        assert!(
            state
                .db
                .contains_sprout_anchor(&handoff_trees.sprout.root()),
            "the resumed handoff commit persists its Sprout anchor"
        );
        artifact
            .validate_canonical(|height| state.db.hash(height), |hash| state.db.height(hash))
            .expect("the completed chain matches every artifact record and handoff hash");
        assert_eq!(
            state
                .db
                .format_version_on_disk()
                .expect("version is readable"),
            Some(state_database_format_version_in_code()),
            "normal continuation preserves the current database format version"
        );
    }

    #[test]
    fn old_read_only_state_is_rejected_and_repaired_state_is_accepted() {
        let (_cache, config) = persistent_config();
        let network = Network::Mainnet;
        let handoff = Height(1);
        let hash = block::Hash([1; 32]);
        let path = seed_repair_db(&config, &network, handoff, &[(handoff, hash)]);
        let (artifact, roots) =
            fixture_artifact_with_records(handoff, hash, &[(handoff, hash, 31)]);
        let _injection = inject_test_repair_input(
            path,
            TestRepairInput {
                handoff,
                handoff_hash: hash,
                handoff_sprout_root: roots[0],
                artifact,
                marker_hash: None,
                before_write: None,
            },
        );

        let old_read_only = ZakuraDb::new(
            &config,
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            &network,
            false,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            true,
        );
        assert!(matches!(
            old_read_only,
            Err(crate::StateInitError::VctSproutHistoryRepairRequired {
                mode: "read-only",
                ..
            })
        ));

        drop(open_normally(&config, &network));
        let repaired_read_only = ZakuraDb::new(
            &config,
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            &network,
            false,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            true,
        );
        assert!(
            repaired_read_only.is_ok(),
            "read-only startup accepts the completed repair"
        );
    }

    #[test]
    fn custom_network_does_not_consume_mainnet_repair_input() {
        let (_cache, config) = persistent_config();
        let network = Network::new_regtest(Default::default());
        let handoff = Height(1);
        let hash = block::Hash([1; 32]);
        let path = seed_repair_db(&config, &network, handoff, &[(handoff, hash)]);
        let (artifact, roots) =
            fixture_artifact_with_records(handoff, hash, &[(handoff, hash, 51)]);
        let _injection = inject_test_repair_input(
            path,
            TestRepairInput {
                handoff,
                handoff_hash: hash,
                handoff_sprout_root: roots[0],
                artifact,
                marker_hash: None,
                before_write: None,
            },
        );

        let db = open_normally(&config, &network);

        assert_eq!(
            db.format_version_on_disk()
                .expect("version remains readable"),
            Some(state_database_format_version_in_code())
        );
        assert!(!db.contains_sprout_anchor(&roots[0]));
    }

    #[test]
    fn startup_cannot_return_database_while_repair_is_blocked() {
        let (_cache, config) = persistent_config();
        let network = Network::Mainnet;
        let handoff = Height(1);
        let hash = block::Hash([1; 32]);
        let path = seed_repair_db(&config, &network, handoff, &[(handoff, hash)]);
        let (artifact, roots) =
            fixture_artifact_with_records(handoff, hash, &[(handoff, hash, 41)]);
        let (entered_tx, entered_rx) = bounded(1);
        let (release_tx, release_rx) = bounded(1);
        let before_write = Arc::new(move || {
            entered_tx
                .send(())
                .expect("startup visibility test receiver remains open");
            release_rx
                .recv()
                .expect("startup visibility test releases repair");
        });
        let _injection = inject_test_repair_input(
            path,
            TestRepairInput {
                handoff,
                handoff_hash: hash,
                handoff_sprout_root: roots[0],
                artifact,
                marker_hash: None,
                before_write: Some(before_write),
            },
        );
        let (returned_tx, returned_rx) = bounded(1);
        let open_config = config.clone();
        let open_network = network.clone();
        let open_thread = std::thread::spawn(move || {
            let db = open_normally(&open_config, &open_network);
            returned_tx
                .send(db.contains_sprout_anchor(&roots[0]))
                .expect("startup result receiver remains open");
        });

        entered_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("repair reaches the blocking pre-write seam");
        assert_eq!(
            returned_rx.recv_timeout(Duration::from_millis(100)),
            Err(RecvTimeoutError::Timeout),
            "ZakuraDb::new must not expose state while repair is incomplete"
        );
        release_tx.send(()).expect("blocked repair is released");
        assert!(
            returned_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("startup returns after repair"),
            "the returned database observes the repaired anchor"
        );
        open_thread.join().expect("startup thread does not panic");
    }

    #[test]
    fn missing_sprout_tip_fails_closed() {
        let db = ephemeral_db(&Network::Mainnet);
        let tip = Height(1);
        seed_canonical(&db, &[(tip, block::Hash([1; 32]))]);
        mark_vct(&db, Height(2));

        assert_eq!(db.sprout_tree_for_tip(), Err(MissingSproutTipTree { tip }));
    }

    #[test]
    fn cancellation_and_canonical_failure_write_nothing() {
        let db = ephemeral_db(&Network::Mainnet);
        let record_height = Height(1);
        let handoff = Height(2);
        let record_hash = block::Hash([1; 32]);
        let handoff_hash = block::Hash([2; 32]);
        seed_canonical(
            &db,
            &[(record_height, record_hash), (handoff, handoff_hash)],
        );
        let (artifact, root) =
            fixture_artifact(handoff, handoff_hash, record_height, record_hash, 8);

        let (cancel_tx, cancel_rx) = bounded(1);
        cancel_tx
            .send(CancelFormatChange)
            .expect("cancellation channel is open");
        assert_eq!(
            repair(&db, handoff, handoff_hash, root, &artifact, &cancel_rx),
            Err(CancelFormatChange)
        );
        assert!(!db.contains_sprout_anchor(&root));

        let bad_hash = block::Hash([3; 32]);
        let (bad_artifact, bad_root) =
            fixture_artifact(handoff, handoff_hash, record_height, bad_hash, 10);
        let (_cancel_tx, cancel_rx) = bounded(1);
        let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            repair(
                &db,
                handoff,
                handoff_hash,
                bad_root,
                &bad_artifact,
                &cancel_rx,
            )
        }));
        assert!(failure.is_err(), "canonical mismatch must reject repair");
        assert!(!db.contains_sprout_anchor(&bad_root));

        assert_eq!(
            bad_artifact.validate_canonical(|height| db.hash(height), |hash| db.height(hash)),
            Err(ArtifactError::CanonicalHashMismatch {
                height: record_height
            })
        );
    }

    #[test]
    fn reached_handoff_hash_mismatch_writes_nothing() {
        let db = ephemeral_db(&Network::Mainnet);
        let handoff = Height(2);
        let expected_handoff_hash = block::Hash([2; 32]);
        let local_handoff_hash = block::Hash([9; 32]);
        seed_canonical(&db, &[(handoff, local_handoff_hash)]);
        let (artifact, root) = fixture_artifact(
            handoff,
            expected_handoff_hash,
            Height(1),
            block::Hash([1; 32]),
            61,
        );
        let (_cancel_tx, cancel_rx) = bounded(1);

        let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            repair(
                &db,
                handoff,
                expected_handoff_hash,
                root,
                &artifact,
                &cancel_rx,
            )
        }));

        assert!(
            failure.is_err(),
            "a locally present handoff hash must match the globally pinned identity"
        );
        assert!(
            !db.contains_sprout_anchor(&root),
            "handoff validation must finish before the atomic repair batch is written"
        );
    }
}
