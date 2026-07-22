use std::time::Duration;

use serde::{Deserialize, Serialize};
use zakura_chain::{block, parameters::Network};

use super::{wire::*, HeaderSyncStartError};
use crate::zakura::ServicePeerLimits;

const COMMON_HEADER_BYTES: usize = 1_487;
const REGTEST_HEADER_BYTES: usize = 177;
const LOCAL_MAX_HS_INFLIGHT_PER_PEER: u16 = 16;
const DEFAULT_HS_STATUS_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

/// Header-sync configuration nested under the Zakura P2P-v2 config.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ZakuraHeaderSyncConfig {
    /// Maximum headers this node advertises per `GetHeaders` response.
    pub max_headers_per_response: u32,
    /// Maximum concurrent `GetHeaders` requests per peer.
    ///
    /// This is both the inbound limit advertised to remote peers and the local
    /// outbound requester ceiling. The effective requester limit is the minimum
    /// of this value, the peer's advertisement, and the hard protocol cap of 16.
    pub max_inflight_requests: u16,
    /// How often this node sends unsolicited status refreshes after local frontier changes.
    #[serde(with = "humantime_serde")]
    pub status_refresh_interval: Duration,
    /// Header-sync peer caps and queue limits owned by this reactor.
    pub peer_limits: ServicePeerLimits,
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
            max_inflight_requests: DEFAULT_HS_MAX_INFLIGHT,
            status_refresh_interval: DEFAULT_HS_STATUS_REFRESH_INTERVAL,
            peer_limits: ServicePeerLimits::default(),
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
        self.max_inflight_requests
            .clamp(1, LOCAL_MAX_HS_INFLIGHT_PER_PEER)
    }

    /// Return the configured trusted anchor, or genesis when no override is configured.
    pub fn anchor(
        &self,
        network: &Network,
    ) -> Result<(block::Height, block::Hash), HeaderSyncStartError> {
        match (self.anchor_height, self.anchor_hash) {
            (Some(height), Some(hash)) => Ok((height, hash)),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_accept_new_blocks_setting_is_rejected() {
        let error = toml::from_str::<ZakuraHeaderSyncConfig>("accept_new_blocks = true")
            .expect_err("the removed block-relay setting must not be silently ignored");

        assert!(error
            .to_string()
            .contains("unknown field `accept_new_blocks`"));
    }
}
