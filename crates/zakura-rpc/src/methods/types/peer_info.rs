//! An array of [`PeerInfo`] is the output of the `getpeerinfo` RPC method.

use derive_getters::Getters;
use zakura_network::{types::MetaAddr, ConnectedPeer, PeerSocketAddr};

/// Item of the `getpeerinfo` response
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, Getters)]
pub struct PeerInfo {
    /// The IP address and port of the peer
    #[getter(copy)]
    pub(crate) addr: PeerSocketAddr,

    /// The peer's user agent string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) subver: Option<String>,

    /// The protocol version advertised by the peer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) version: Option<u32>,

    /// Inbound (true) or Outbound (false)
    pub(crate) inbound: bool,

    /// The round-trip ping time in seconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) pingtime: Option<f64>,

    /// The wait time on a ping response in seconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) pingwait: Option<f64>,
}

/// Response type for the `getpeerinfo` RPC method.
pub type GetPeerInfoResponse = Vec<PeerInfo>;

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
            is_inbound: true,
            rtt: None,
            ping_sent_at: None,
        };

        let peer_info = PeerInfo::from(peer);

        assert_eq!(peer_info.subver.as_deref(), Some("/Zakura:1.0.3/"));
        assert_eq!(peer_info.version, Some(170_160));
        assert!(peer_info.inbound);
    }
}
