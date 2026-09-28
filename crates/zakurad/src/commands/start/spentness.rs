//! Spentness artifact distribution over Zakura peers.

use std::{path::PathBuf, sync::Arc};

use color_eyre::{eyre::eyre, Report};
use zakura_chain::parameters::{
    spentness_hints::{supported_commitments, Commitment},
    Network,
};
use zakura_network::zakura::{spentness, CustomService, ZakuraSupervisorHandle};

use super::ZakuradConfig;

/// A prepared artifact service and the cache it downloads into.
pub(super) struct Distribution {
    cache: PathBuf,
    supported: Vec<Commitment>,
    service: Arc<spentness::ArtifactService>,
}

/// Register the artifact service when the operator configured a distribution cache.
///
/// Distribution only serves and downloads artifacts; it never enables hinted state writes.
/// It never downloads, serves, or advertises a revoked artifact.
pub(super) async fn prepare_distribution(
    config: &ZakuradConfig,
    custom_services: &mut Vec<CustomService>,
) -> Result<Option<Distribution>, Report> {
    let Some(cache) = config.spentness.cache_dir.clone() else {
        return Ok(None);
    };
    let network = config.network.network.clone();
    if network != Network::Mainnet {
        return Err(eyre!(
            "spentness distribution currently requires reviewed Mainnet commitments"
        ));
    }
    let supported = supported_commitments(&network);
    let (service, custom) = spentness::prepare(cache.clone(), &supported)
        .await
        .map_err(|error| eyre!(error))?;
    custom_services.push(custom);
    Ok(Some(Distribution {
        cache,
        supported,
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
            self.supported,
            self.service,
            supervisor,
        ))
    }
}
