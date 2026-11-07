use std::time::Duration;

use serde::{Deserialize, Serialize};
use zakura_chain::{block, parameters::Network};

use super::{wire::*, HeaderSyncStartError};
use crate::zakura::ServicePeerLimits;

pub(super) const COMMON_HEADER_BYTES: usize = 1_487;
pub(super) const REGTEST_HEADER_BYTES: usize = 177;
const DEFAULT_HS_STATUS_REFRESH_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_HS_MAX_UNPRODUCTIVE_REQUESTS: u32 = 3;
const DEFAULT_HS_UNPRODUCTIVE_PEER_COOLDOWN: Duration = Duration::from_secs(60);

/// Header-sync configuration nested under the Zakura P2P-v2 config.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ZakuraHeaderSyncConfig {
    /// Maximum headers this node advertises per `Headers` page.
    pub max_headers_per_response: u32,
    /// How often this node sends unsolicited status refreshes after local frontier changes.
    #[serde(with = "zakura_chain::serialization::serde_adapters::duration")]
    pub status_refresh_interval: Duration,
    /// Header-sync peer caps and queue limits owned by this reactor.
    pub peer_limits: ServicePeerLimits,
    /// Consecutive unproductive header requests before this node drops the peer's session.
    ///
    /// A deadline marks its request as unproductive.
    /// Do not charge a peer that reports the selected tip.
    /// Set this value to `0` to keep all peers.
    pub max_unproductive_header_requests: u32,
    /// How long the node refuses header-sync readmission to an unproductive peer,
    /// or to a peer it disconnected for headers that failed validation.
    ///
    /// Discovery applies dial backoff only after a failed dial.
    /// The cooldown prevents an immediate redial after a successful dial ends in eviction.
    /// Set this value to zero to allow immediate readmission.
    #[serde(with = "zakura_chain::serialization::serde_adapters::duration")]
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
    /// The most unsent header-sync page bytes this node holds across every
    /// serving session.
    ///
    /// Unset uses the derived budget: the inbound plus outbound `peer_limits`
    /// sessions, clamped to the admitted connections, times one session's output
    /// budget. One session's budget is twice the default QUIC send window or the
    /// largest page, whichever is larger: 64 MiB at defaults, so the node budget is
    /// 92 × 64 MiB = 5.75 GiB. Every session can then hold its whole budget at once,
    /// so a fast reader never pauses another peer's serving. These
    /// bytes are reserved before each page is produced and held until its last frame
    /// is written; idle sessions hold nothing.
    ///
    /// A smaller value bounds this memory more tightly, but lets peers that read
    /// at full speed pause each other's serving. A value below one session's budget
    /// rises to that budget.
    pub node_serve_output_bytes: Option<u64>,
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
            node_serve_output_bytes: None,
        }
    }
}

impl ZakuraHeaderSyncConfig {
    /// The header-sync sessions that can serve at once on a node that admits
    /// `max_connections`: the inbound plus outbound session slots, clamped to
    /// `max_connections`.
    pub(crate) fn serving_sessions(&self, max_connections: usize) -> usize {
        self.peer_limits
            .max_inbound_peers
            .saturating_add(self.peer_limits.max_outbound_peers)
            .min(max_connections)
    }

    /// This config for a node that admits `max_connections`: an unset serving
    /// output budget becomes one session's budget on each of
    /// [`Self::serving_sessions`]. A configured value stays.
    pub fn for_node_connections(mut self, max_connections: usize) -> Self {
        if self.node_serve_output_bytes.is_none() {
            self.node_serve_output_bytes = Some(
                super::v9::serving_limits(
                    super::v9::SERVE_PEER_OUTPUT_BYTES,
                    self.serving_sessions(max_connections),
                    None,
                )
                .node_output_bytes,
            );
        }
        self
    }

    /// Return the clamped served-range advertisement for wire status messages.
    pub fn advertised_max_headers_per_response(&self) -> u32 {
        self.max_headers_per_response.clamp(1, MAX_HS_RANGE)
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
