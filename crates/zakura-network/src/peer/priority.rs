//! Validating peer listener and outbound connection addresses.

use std::net::SocketAddr;

use zakura_chain::parameters::Network;

use crate::PeerSocketAddr;

/// A validated peer address, retained as the return type of the published preference API.
///
/// All valid addresses compare equally: listening ports do not determine preference.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct PeerPreference {
    private: (),
}

impl PeerPreference {
    /// Validate `peer_addr` and return the same preference for every valid address.
    ///
    /// Returns an error when the address is invalid for outbound connections.
    pub fn new(
        peer_addr: impl Into<PeerSocketAddr>,
        network: impl Into<Option<Network>>,
    ) -> Result<PeerPreference, &'static str> {
        address_is_valid_for_outbound_connections(peer_addr.into(), network)?;
        Ok(PeerPreference { private: () })
    }
}

/// Is the [`PeerSocketAddr`] we have for this peer valid for outbound
/// connections?
///
/// Since the addresses in the address book are unique, this check can be
/// used to permanently reject entire [`MetaAddr`]s.
///
/// [`MetaAddr`]: crate::meta_addr::MetaAddr
pub fn address_is_valid_for_outbound_connections(
    peer_addr: PeerSocketAddr,
    network: impl Into<Option<Network>>,
) -> Result<(), &'static str> {
    // TODO: make private IP addresses an error unless a debug config is set (#3117)

    if peer_addr.ip().is_unspecified() {
        return Err("invalid peer IP address: unspecified addresses can not be used for outbound connections");
    }

    // 0 is an invalid outbound port.
    if peer_addr.port() == 0 {
        return Err(
            "invalid peer port: unspecified ports can not be used for outbound connections",
        );
    }

    address_is_valid_for_inbound_listeners(*peer_addr, network)
}

/// Is the supplied [`SocketAddr`] valid for inbound listeners on `network`?
///
/// This is used to check Zebra's configured Zcash listener port.
pub fn address_is_valid_for_inbound_listeners(
    listener_addr: SocketAddr,
    network: impl Into<Option<Network>>,
) -> Result<(), &'static str> {
    // TODO: make private IP addresses an error unless a debug config is set (#3117)

    // A port does not identify the network; message framing checks network magic.
    if network
        .into()
        .is_some_and(|network| listener_addr.port() == network.default_port())
    {
        return Ok(());
    }

    // Ignore ports used by potentially compatible nodes: other coins and unsupported Zcash regtest.
    if listener_addr.port() == 18344 {
        return Err(
            "invalid peer port: port is for Regtest, but Zakura does not support that network",
        );
    } else if [8033, 18033, 16125, 26125].contains(&listener_addr.port()) {
        // These coins use the same network magic numbers as Zcash, so we have to reject them by port:
        // - ZClassic: 8033/18033
        //   https://github.com/ZclassicCommunity/zclassic/blob/504362bbf72400f51acdba519e12707da44138c2/src/chainparams.cpp#L130
        // - Flux/ZelCash: 16125/26125
        return Err("invalid peer port: port is for a non-Zcash coin");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Network identity is checked by the protocol, not inferred from the default port.
    #[test]
    fn peer_cache_accepts_default_and_custom_ports_on_either_network() {
        for network in [Network::Mainnet, Network::new_default_testnet()] {
            for port in [8233, 18233, 9000] {
                let addr = SocketAddr::from(([127, 0, 0, 1], port));
                assert!(
                    address_is_valid_for_outbound_connections(addr.into(), network.clone()).is_ok()
                );
                assert!(address_is_valid_for_inbound_listeners(addr, network.clone()).is_ok());
            }
            for addr in ["0.0.0.0:8233", "127.0.0.1:0", "127.0.0.1:8033"] {
                assert!(address_is_valid_for_outbound_connections(
                    addr.parse().unwrap(),
                    network.clone()
                )
                .is_err());
            }
        }
    }
}
