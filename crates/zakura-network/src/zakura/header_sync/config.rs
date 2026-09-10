use std::time::Duration;

use serde::{Deserialize, Serialize};
use zakura_chain::{block, parameters::Network};

use super::{wire::*, HeaderSyncStartError};
use crate::zakura::ServicePeerLimits;

const COMMON_HEADER_BYTES: usize = 1_487;
const REGTEST_HEADER_BYTES: usize = 177;
const LOCAL_MAX_HS_INFLIGHT_PER_PEER: u16 = 1;
const DEFAULT_HS_STATUS_REFRESH_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_HS_MAX_UNPRODUCTIVE_REQUESTS: u32 = 3;
const DEFAULT_HS_UNPRODUCTIVE_PEER_COOLDOWN: Duration = Duration::from_secs(60);

/// Header-sync configuration nested under the Zakura P2P-v2 config.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ZakuraHeaderSyncConfig {
    /// Maximum headers this node advertises per `GetHeaders` response.
    pub max_headers_per_response: u32,
    /// How often this node sends unsolicited status refreshes after local frontier changes.
    #[serde(with = "humantime_serde")]
    pub status_refresh_interval: Duration,
    /// Header-sync peer caps and queue limits owned by this reactor.
    pub peer_limits: ServicePeerLimits,
    /// Consecutive unproductive header requests before this node drops the peer's session.
    ///
    /// A deadline marks its request as unproductive.
    /// Do not charge a peer that reports the selected tip.
    /// Set this value to `0` to keep all peers.
    pub max_unproductive_header_requests: u32,
    /// How long the node refuses header-sync readmission to an unproductive peer.
    ///
    /// Discovery applies dial backoff only after a failed dial.
    /// The cooldown prevents an immediate redial after a successful dial ends in eviction.
    /// Set this value to zero to allow immediate readmission.
    #[serde(with = "humantime_serde")]
    pub unproductive_peer_cooldown: Duration,
    /// Optional trusted header-sync anchor height.
    ///
    /// When unset, header sync starts from genesis. When set, [`anchor_hash`](Self::anchor_hash)
    /// must also be set and must match genesis or a configured checkpoint.
    pub anchor_height: Option<block::Height>,
    /// Optional trusted header-sync anchor hash.
    ///
    /// When unset, header sync starts from genesis. When set, [`anchor_height`](Self::anchor_height)
    /// must also be set and must match genesis or a configured checkpoint.
    pub anchor_hash: Option<block::Hash>,
}

impl Default for ZakuraHeaderSyncConfig {
    fn default() -> Self {
        Self {
            max_headers_per_response: DEFAULT_HS_RANGE,
            status_refresh_interval: DEFAULT_HS_STATUS_REFRESH_INTERVAL,
            peer_limits: ServicePeerLimits::default(),
            max_unproductive_header_requests: DEFAULT_HS_MAX_UNPRODUCTIVE_REQUESTS,
            unproductive_peer_cooldown: DEFAULT_HS_UNPRODUCTIVE_PEER_COOLDOWN,
            anchor_height: None,
            anchor_hash: None,
        }
    }
}

impl ZakuraHeaderSyncConfig {
    /// Return the clamped served-range advertisement for wire status messages.
    pub fn advertised_max_headers_per_response(&self) -> u32 {
        self.max_headers_per_response.clamp(1, MAX_HS_RANGE)
    }

    /// Return the locally capped in-flight advertisement for status messages.
    pub fn advertised_max_inflight_requests(&self) -> u16 {
        LOCAL_MAX_HS_INFLIGHT_PER_PEER
    }

    /// Return the status refresh interval, clamped to the publication-rate floor.
    pub fn effective_status_refresh_interval(&self) -> Duration {
        self.status_refresh_interval.max(Duration::from_secs(1))
    }

    /// Return the configured trusted anchor, or genesis when no override is configured.
    pub fn anchor(
        &self,
        network: &Network,
    ) -> Result<(block::Height, block::Hash), HeaderSyncStartError> {
        match (self.anchor_height, self.anchor_hash) {
            (Some(height), Some(hash)) if network.checkpoint_list().hash(height) == Some(hash) => {
                Ok((height, hash))
            }
            (Some(height), Some(hash)) => Err(HeaderSyncStartError::InvalidAnchor {
                anchor: (height, hash),
            }),
            (None, None) => Ok((block::Height(0), network.genesis_hash())),
            _ => Err(HeaderSyncStartError::IncompleteAnchor),
        }
    }
}

/// Returns the serialized byte length of a header-sync header on `network`.
pub fn header_sync_header_bytes_for_network(network: &Network) -> usize {
    if network
        .parameters()
        .is_some_and(|parameters| parameters.is_regtest())
    {
        REGTEST_HEADER_BYTES
    } else {
        COMMON_HEADER_BYTES
    }
}
