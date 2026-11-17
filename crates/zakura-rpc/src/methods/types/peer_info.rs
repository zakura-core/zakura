//! An array of [`PeerInfo`] is the output of the `getpeerinfo` RPC method.

use getset::{CopyGetters, Getters};
use zakura_network::{types::MetaAddr, ConnectedPeer, PeerSocketAddr};

/// Item of the `getpeerinfo` response
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, Getters, CopyGetters)]
pub struct PeerInfo {
    /// The IP address and port of the peer
    #[getset(get_copy = "pub")]
    pub(crate) addr: PeerSocketAddr,

    /// The peer's user agent string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[getset(get = "pub")]
    pub(crate) subver: Option<String>,

    /// The protocol version advertised by the peer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[getset(get = "pub")]
    pub(crate) version: Option<u32>,

    /// Inbound (true) or Outbound (false)
    #[getset(get_copy = "pub")]
    pub(crate) inbound: bool,

    /// The best chain height the peer advertised when it connected, or -1 if unknown.
    #[getset(get_copy = "pub")]
    pub(crate) startingheight: i64,

    /// The round-trip ping time in seconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[getset(get = "pub")]
    pub(crate) pingtime: Option<f64>,

    /// The wait time on a ping response in seconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[getset(get = "pub")]
    pub(crate) pingwait: Option<f64>,
}

/// Response type for the `getpeerinfo` RPC method.
pub type GetPeerInfoResponse = Vec<PeerInfo>;

/// The `startingheight` zcashd reports for a peer whose handshake height is unknown.
const UNKNOWN_STARTING_HEIGHT: i64 = -1;

impl PeerInfo {
    /// Creates peer information without optional handshake metadata.
    pub fn new(
        addr: PeerSocketAddr,
        inbound: bool,
        pingtime: Option<f64>,
        pingwait: Option<f64>,
    ) -> Self {
        Self {
            addr,
            subver: None,
            version: None,
            inbound,
            startingheight: UNKNOWN_STARTING_HEIGHT,
            pingtime,
            pingwait,
        }
    }
}

impl From<MetaAddr> for PeerInfo {
    fn from(meta_addr: MetaAddr) -> Self {
        Self {
            addr: meta_addr.addr(),
            subver: None,
            version: None,
            inbound: meta_addr.is_inbound(),
            startingheight: UNKNOWN_STARTING_HEIGHT,
            pingtime: meta_addr.rtt().map(|d| d.as_secs_f64()),
            pingwait: meta_addr.ping_sent_at().map(|t| t.elapsed().as_secs_f64()),
        }
    }
}

impl From<ConnectedPeer> for PeerInfo {
    fn from(peer: ConnectedPeer) -> Self {
        Self {
            addr: peer.addr,
            subver: Some(peer.user_agent.to_string()),
            version: Some(peer.version.0),
            inbound: peer.is_inbound,
            startingheight: i64::from(peer.start_height.0),
            pingtime: peer.rtt.map(|duration| duration.as_secs_f64()),
            pingwait: peer
                .ping_sent_at
                .map(|instant| instant.elapsed().as_secs_f64()),
        }
    }
}

impl Default for PeerInfo {
    fn default() -> Self {
        Self {
            addr: PeerSocketAddr::unspecified(),
            subver: None,
            version: None,
            inbound: false,
            startingheight: UNKNOWN_STARTING_HEIGHT,
            pingtime: None,
            pingwait: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use zakura_network::Version;

    use super::*;

    #[test]
    fn connected_peer_conversion_includes_handshake_metadata() {
        let peer = ConnectedPeer {
            addr: "127.0.0.1:8233".parse().expect("test address parses"),
            user_agent: Arc::from("/Zakura:1.0.3/"),
            version: Version(170_160),
            start_height: zakura_chain::block::Height(3_500_000),
            is_inbound: true,
            rtt: None,
            ping_sent_at: None,
        };

        let peer_info = PeerInfo::from(peer);

        assert_eq!(peer_info.subver.as_deref(), Some("/Zakura:1.0.3/"));
        assert_eq!(peer_info.version, Some(170_160));
        assert!(peer_info.inbound);
        assert_eq!(peer_info.startingheight, 3_500_000);
    }
}
