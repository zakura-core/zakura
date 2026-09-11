//! Select, authenticate, and resume a hinted run as the database opens.
//!
//! A fresh run starts only on an empty database. An interrupted run resumes with
//! its original commitment, even when a newer release selects a later one.

use std::{fs::File, io, path::PathBuf, sync::Arc};

use semver::Version;
use zakura_chain::{
    amount::{Amount, NonNegative},
    block::{Hash, Height},
    common::atomic_write,
    parameters::{
        spentness_hints::{Commitment, Mode, VerifiedArtifact},
        Network,
    },
    value_balance::ValueBalance,
};

use super::{
    record::{read_progress, Progress},
    ApplyingRun, SpentnessConfig, SpentnessError, SpentnessSetup, SpentnessStatus,
};
use crate::{
    config::database_format_version_on_disk,
    constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
    service::finalized_state::{
        disk_db::DiskDb, zakura_db::ZakuraDb, STATE_COLUMN_FAMILIES_IN_CODE,
    },
    Config, StateInitError,
};

/// The state-owned artifact directory: `<cache_dir>/spentness`.
pub fn spentness_cache_dir(config: &Config) -> PathBuf {
    config.cache_dir.join("spentness")
}

/// Return the immutable artifact location used for restart recovery.
pub fn artifact_cache_path(config: &Config, commitment: &Commitment) -> PathBuf {
    spentness_cache_dir(config).join(commitment.file_name())
}

/// Artifact selected by release authority before the node opens writable state.
#[derive(Clone, Debug)]
pub struct SpentnessArtifactRequirement {
    /// Exact release commitment for this run.
    pub commitment: Commitment,
    /// A durable applying record forbids ordinary fallback.
    pub recovery: bool,
}

/// Decide which artifact the node must supply before state opens, if any.
///
/// This reads only the progress record and never exposes an incomplete database.
/// In `auto` mode, an unusable boundary for a fresh run falls back to ordinary sync.
pub fn spentness_artifact_requirement(
    config: &Config,
    spentness: &SpentnessConfig,
    network: &Network,
) -> Result<Option<SpentnessArtifactRequirement>, StateInitError> {
    let setup = SpentnessSetup::new(spentness.clone(), network);
    let existing = probe_existing_state(config, network)?;
    let Some(requirement) = select_requirement(&setup, existing)? else {
        return Ok(None);
    };
    match check_requirement(config, &setup, &requirement) {
        Ok(()) => Ok(Some(requirement)),
        Err(error) if requirement.recovery || setup.config.mode == Mode::Require => {
            Err(error.into())
        }
        Err(error) => {
            tracing::warn!(%error, "spentness boundary unavailable; using ordinary sync");
            Ok(None)
        }
    }
}

/// What an existing database on disk contains.
enum ExistingState {
    Empty,
    Ordinary,
    Hinted(Progress),
}

fn probe_existing_state(
    config: &Config,
    network: &Network,
) -> Result<ExistingState, StateInitError> {
    let Some((db, _)) = open_existing_state(config, network)? else {
        return Ok(ExistingState::Empty);
    };
    if let Some(progress) = read_progress(&db)? {
        return Ok(ExistingState::Hinted(progress));
    }
    let hashes = db
        .cf_handle("hash_by_height")
        .expect("block hash column family is declared");
    let has_blocks = db
        .zs_forward_range_iter::<_, Height, Hash, _>(&hashes, ..)
        .next()
        .is_some();
    Ok(if has_blocks {
        ExistingState::Ordinary
    } else {
        ExistingState::Empty
    })
}

/// Open an existing database read-only, without format changes or spentness gates.
///
/// Returns the database and its on-disk format version, or `None` when no database exists.
/// Callers must not expose monetary state from it.
pub(super) fn open_existing_state(
    config: &Config,
    network: &Network,
) -> Result<Option<(DiskDb, Version)>, StateInitError> {
    if config.ephemeral {
        return Ok(None);
    }
    let version = state_database_format_version_in_code();
    let disk_version =
        database_format_version_on_disk(config, STATE_DATABASE_KIND, version.major, network)
            .map_err(|source| StateInitError::DatabaseFormatVersion {
                path: config.version_file_path(STATE_DATABASE_KIND, version.major, network),
                source,
            })?;
    let Some(disk_version) = disk_version else {
        return Ok(None);
    };
    let db = DiskDb::new(
        config,
        STATE_DATABASE_KIND,
        &version,
        network,
        STATE_COLUMN_FAMILIES_IN_CODE
            .iter()
            .map(ToString::to_string),
        true,
    )?;
    Ok(Some((db, disk_version)))
}

fn select_requirement(
    setup: &SpentnessSetup,
    existing: ExistingState,
) -> Result<Option<SpentnessArtifactRequirement>, SpentnessError> {
    let mode = setup.config.mode;
    let progress = match existing {
        ExistingState::Ordinary => return Ok(None),
        ExistingState::Empty if mode == Mode::Off => return Ok(None),
        ExistingState::Empty => {
            return match setup.authority.newest() {
                Ok(commitment) => Ok(Some(SpentnessArtifactRequirement {
                    commitment: commitment.clone(),
                    recovery: false,
                })),
                Err(_) if mode == Mode::Auto => Ok(None),
                Err(error) => Err(error),
            };
        }
        ExistingState::Hinted(progress) => progress,
    };

    match progress {
        Progress::Complete { commitment, .. } => {
            setup.authority.check(&commitment)?;
            Ok(None)
        }
        // Rebuilding needs retained bodies, not the artifact.
        Progress::Rebuilding { commitment, .. } => {
            setup.authority.check(&commitment)?;
            require_enabled(mode)?;
            Ok(None)
        }
        Progress::Applying { commitment, .. } => {
            require_enabled(mode)?;
            Ok(Some(SpentnessArtifactRequirement {
                commitment,
                recovery: true,
            }))
        }
    }
}

fn check_requirement(
    config: &Config,
    setup: &SpentnessSetup,
    requirement: &SpentnessArtifactRequirement,
) -> Result<(), SpentnessError> {
    setup.authority.check(&requirement.commitment)?;
    require_fast_sync(config)?;
    setup.authority.handoff_frontiers(&requirement.commitment)?;
    Ok(())
}

fn require_enabled(mode: Mode) -> Result<(), SpentnessError> {
    if mode == Mode::Off {
        return Err(SpentnessError::Incomplete);
    }
    Ok(())
}

fn require_fast_sync(config: &Config) -> Result<(), SpentnessError> {
    if !config.checkpoint_sync || !config.vct_fast_sync {
        return Err(SpentnessError::RequiresFastSync);
    }
    Ok(())
}

/// Verify the configured or cached artifact, and keep a durable recovery copy.
///
/// A restart can then resume from the state cache even if the configured file moves.
fn load_artifact(
    config: &Config,
    settings: &SpentnessConfig,
    commitment: &Commitment,
) -> Result<VerifiedArtifact, SpentnessError> {
    let recovery_path = artifact_cache_path(config, commitment);
    let source = settings.artifact.as_ref().unwrap_or(&recovery_path);
    let file = File::open(source).map_err(|error| match error.kind() {
        io::ErrorKind::NotFound => SpentnessError::MissingArtifact {
            digest: commitment.digest_hex(),
            path: recovery_path.clone(),
        },
        _ => error.into(),
    })?;
    let artifact = VerifiedArtifact::read(file, commitment)?;
    atomic_write(recovery_path, artifact.bytes())?.map_err(|error| error.error)?;
    Ok(artifact)
}

impl ZakuraDb {
    /// Resume a recorded run, or start a fresh one on an empty database.
    ///
    /// Called while the database opens, before format changes run.
    pub(crate) fn initialize_spentness(&mut self, read_only: bool) -> Result<(), SpentnessError> {
        match self.spentness_progress()? {
            Some(progress) => self.resume_spentness(progress, read_only),
            None => self.start_spentness(read_only),
        }
    }

    fn resume_spentness(
        &mut self,
        progress: Progress,
        read_only: bool,
    ) -> Result<(), SpentnessError> {
        self.check_progress(&progress)?;
        let resumable = !read_only && self.spentness.setup.config.mode != Mode::Off;
        match &progress {
            Progress::Complete { .. } => {}
            _ if !resumable => return Err(SpentnessError::Incomplete),
            Progress::Rebuilding { commitment, .. } => {
                self.check_recovery_environment(commitment)?;
            }
            Progress::Applying { commitment, .. } => {
                self.check_recovery_environment(commitment)?;
                self.spentness.applying = Some(Arc::new(self.prepare_run(commitment)?));
            }
        }
        self.publish_spentness_status(&progress);
        Ok(())
    }

    fn start_spentness(&mut self, read_only: bool) -> Result<(), SpentnessError> {
        let mode = self.spentness.setup.config.mode;
        if read_only || mode == Mode::Off {
            return Ok(());
        }
        if self.tip().is_some() {
            return match mode {
                Mode::Require => Err(SpentnessError::NonEmptyDatabase),
                _ => Ok(()),
            };
        }
        match self.prepare_fresh_run() {
            Ok(run) => {
                let terminal_height = Height(run.artifact.commitment().terminal_height);
                self.spentness.applying = Some(Arc::new(run));
                self.set_spentness_status(SpentnessStatus::Applying { terminal_height });
            }
            Err(error) if mode == Mode::Auto => {
                tracing::warn!(%error, "spentness unavailable before construction; using ordinary sync");
            }
            Err(error) => return Err(error),
        }
        Ok(())
    }

    fn prepare_fresh_run(&self) -> Result<ApplyingRun, SpentnessError> {
        require_fast_sync(&self.config)?;
        let commitment = self.spentness.setup.authority.newest()?;
        self.spentness.setup.authority.check(commitment)?;
        self.prepare_run(commitment)
    }

    /// Resolve the VCT handoff and load the verified artifact for an applying run.
    fn prepare_run(&self, commitment: &Commitment) -> Result<ApplyingRun, SpentnessError> {
        let handoff_frontiers = self
            .spentness
            .setup
            .authority
            .handoff_frontiers(commitment)?;
        let artifact = load_artifact(&self.config, &self.spentness.setup.config, commitment)?;
        Ok(ApplyingRun {
            artifact,
            handoff_frontiers,
        })
    }

    /// Check that the record agrees with the finalized tip and the release authority.
    fn check_progress(&self, progress: &Progress) -> Result<(), SpentnessError> {
        let commitment = progress.commitment();
        self.spentness.setup.authority.check(commitment)?;
        let tip = self.tip().ok_or(SpentnessError::Inconsistent(
            "progress without a finalized tip",
        ))?;
        let terminal = (
            Height(commitment.terminal_height),
            Hash(commitment.terminal_block_hash),
        );
        match progress {
            Progress::Applying {
                height,
                block_hash,
                next_ordinal,
                survivor_value,
                ..
            } => {
                if tip != (Height(*height), Hash(*block_hash))
                    || *height >= commitment.terminal_height
                    || *next_ordinal > commitment.output_count
                {
                    return Err(SpentnessError::Inconsistent(
                        "applying progress disagrees with the finalized boundary",
                    ));
                }
                Amount::<NonNegative>::try_from(*survivor_value)?;
            }
            Progress::Rebuilding {
                indexed_height,
                replay_accounting,
                survivor_value,
                ..
            } => {
                if tip != terminal || indexed_height.is_some_and(|height| height > terminal.0 .0) {
                    return Err(SpentnessError::Inconsistent(
                        "rebuild progress disagrees with the terminal boundary",
                    ));
                }
                ValueBalance::<NonNegative>::try_from(*replay_accounting)?;
                Amount::<NonNegative>::try_from(*survivor_value)?;
            }
            Progress::Complete { rollback_floor, .. } => {
                if Height(*rollback_floor) != terminal.0
                    || tip.0 < terminal.0
                    || self.hash(terminal.0) != Some(terminal.1)
                {
                    return Err(SpentnessError::Inconsistent(
                        "completed rollback boundary is inconsistent",
                    ));
                }
            }
        }
        Ok(())
    }

    /// An interrupted run resumes only with its original format, sync settings, and VCT boundary.
    fn check_recovery_environment(&self, commitment: &Commitment) -> Result<(), SpentnessError> {
        let disk_version = self
            .format_version_on_disk()
            .map_err(|_| SpentnessError::Inconsistent("cannot read the database format version"))?;
        if disk_version != Some(self.format_version_in_code()) {
            return Err(SpentnessError::FormatChanged);
        }
        require_fast_sync(&self.config)?;
        if self.vct_synced_below() != Some(Height(commitment.terminal_height)) {
            return Err(SpentnessError::Inconsistent(
                "spentness and VCT recovery boundaries differ",
            ));
        }
        Ok(())
    }
}
