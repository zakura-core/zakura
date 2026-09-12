//! Spentness settings resolved at startup.
//!
//! Construction resolves the release-selected artifact before writable state opens.
//! Distribution serves and downloads supported artifacts over Zakura peers.

use std::{fs::File, path::PathBuf, sync::Arc};

use color_eyre::{eyre::eyre, Report};
use tokio_util::sync::CancellationToken;
use zakura_chain::parameters::{
    spentness_hints::{release_commitments, Mode, VerifiedArtifact},
    Network,
};
use zakura_network::zakura::{spentness, CustomService, ZakuraSupervisorHandle};
use zakura_state::{BoxError, SpentnessArtifactRequirement};

use super::ZakuradConfig;

/// The artifact cache: the configured directory, or the state-owned artifact directory.
fn cache_dir(config: &ZakuradConfig) -> PathBuf {
    config
        .spentness
        .cache_dir
        .clone()
        .unwrap_or_else(|| zakura_state::spentness_cache_dir(&config.state))
}

/// Select construction settings, and verify the required artifact before state opens.
///
/// `auto` falls back to ordinary sync when the artifact is unavailable. `require` and
/// an interrupted run fail instead.
pub(super) async fn prepare_construction(
    config: &ZakuradConfig,
    state: &zakura_state::Config,
    shutdown: CancellationToken,
) -> Result<zakura_state::SpentnessConfig, Report> {
    let mode = config.spentness.mode;
    let mut construction = zakura_state::SpentnessConfig {
        mode,
        artifact: None,
    };
    if mode == Mode::Off {
        return Ok(construction);
    }

    let requirement = {
        let state = state.clone();
        let construction = construction.clone();
        let network = config.network.network.clone();
        tokio::task::spawn_blocking(move || {
            zakura_state::spentness_artifact_requirement(&state, &construction, &network)
        })
        .await?
        .map_err(|error| eyre!(error))?
    };
    let Some(requirement) = requirement else {
        return Ok(construction);
    };

    let commitment = &requirement.commitment;
    match resolve_artifact(config, &requirement, shutdown).await {
        Ok(path) => {
            tracing::info!(
                digest = %commitment.digest_hex(),
                height = commitment.terminal_height,
                "verified spentness artifact before opening state"
            );
            construction.artifact = Some(path);
        }
        Err(error) if requirement.recovery || mode == Mode::Require => {
            return Err(eyre!(
                "cannot acquire required spentness artifact {}: {error}; \
                 restore identical bytes with spentness.artifact_file",
                commitment.digest_hex()
            ));
        }
        Err(error) => {
            tracing::warn!(
                %error,
                "spentness acquisition failed before construction; using ordinary sync"
            );
            construction.mode = Mode::Off;
        }
    }
    Ok(construction)
}

/// Return a verified artifact file in the cache.
///
/// An explicit `artifact_file` supplies bytes only, and never falls back to peers.
/// Otherwise a cache hit avoids starting the temporary peer endpoint.
async fn resolve_artifact(
    config: &ZakuradConfig,
    requirement: &SpentnessArtifactRequirement,
    shutdown: CancellationToken,
) -> Result<PathBuf, BoxError> {
    let cache = cache_dir(config);
    let commitment = requirement.commitment.clone();

    if let Some(file) = config.spentness.artifact_file.clone() {
        return tokio::task::spawn_blocking(move || {
            let artifact = VerifiedArtifact::read(File::open(file)?, &commitment)?;
            spentness::publish(&cache, &artifact)
        })
        .await?;
    }

    let cached = {
        let cache = cache.clone();
        let commitment = commitment.clone();
        tokio::task::spawn_blocking(move || spentness::load(&cache, &commitment)).await?
    };
    if cached.is_err() {
        tracing::info!(
            digest = %commitment.digest_hex(),
            height = commitment.terminal_height,
            recovery = requirement.recovery,
            "acquiring spentness artifact before opening state"
        );
        spentness::acquire_before_state(&config.network, &cache, &commitment, shutdown).await?;
    }
    Ok(spentness::artifact_path(&cache, &commitment))
}

/// A prepared artifact service and the cache it downloads into.
pub(super) struct Distribution {
    cache: PathBuf,
    network: Network,
    service: Arc<spentness::ArtifactService>,
}

/// Distribution runs when the operator configured a cache, or enabled hints on Mainnet.
fn distribution_enabled(config: &ZakuradConfig) -> bool {
    config.spentness.cache_dir.is_some()
        || (config.spentness.mode != Mode::Off && config.network.network == Network::Mainnet)
}

/// Register the artifact service when distribution is enabled.
///
/// Distribution only serves and downloads artifacts; it never enables hinted state writes.
pub(super) async fn prepare_distribution(
    config: &ZakuradConfig,
    custom_services: &mut Vec<CustomService>,
) -> Result<Option<Distribution>, Report> {
    if !distribution_enabled(config) {
        return Ok(None);
    }
    let network = config.network.network.clone();
    if network != Network::Mainnet {
        return Err(eyre!(
            "spentness distribution currently requires reviewed Mainnet commitments"
        ));
    }
    let cache = cache_dir(config);
    let (service, custom) = spentness::prepare(cache.clone(), release_commitments(&network))
        .await
        .map_err(|error| eyre!(error))?;
    custom_services.push(custom);
    Ok(Some(Distribution {
        cache,
        network,
        service,
    }))
}

impl Distribution {
    /// Download missing supported artifacts in the background once peers connect.
    pub(super) fn spawn_downloads(
        self,
        supervisor: ZakuraSupervisorHandle,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(spentness::download_missing(
            self.cache,
            release_commitments(&self.network),
            self.service,
            supervisor,
        ))
    }
}
