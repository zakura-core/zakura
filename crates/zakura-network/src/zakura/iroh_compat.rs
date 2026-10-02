//! Conversions between Zakura's identity types and Iroh's.
//!
//! Zakura code holds [`NodeId`], [`NodeAddr`] and [`NodeSecretKey`]. The
//! transport still runs on Iroh, so the transport converts at each Iroh call.
//! Both sides validate the Ed25519 point, so a valid key converts without error.
//! This module goes away when the transport moves to `zakura-quic`.

use zakura_quic::{NodeAddr, NodeId, NodeSecretKey};

/// Converts a Zakura node ID into an Iroh endpoint ID.
pub(crate) fn to_iroh_id(id: &NodeId) -> iroh::EndpointId {
    iroh::EndpointId::from_bytes(id.as_bytes())
        .expect("NodeId construction already checked that the bytes are an Ed25519 point")
}

/// Converts an Iroh endpoint ID into a Zakura node ID.
pub(crate) fn from_iroh_id(id: &iroh::EndpointId) -> NodeId {
    NodeId::from_bytes(id.as_bytes())
        .expect("Iroh EndpointId construction already checked that the bytes are an Ed25519 point")
}

/// Converts a Zakura node address into an Iroh endpoint address.
pub(crate) fn to_iroh_addr(addr: &NodeAddr) -> iroh::EndpointAddr {
    iroh::EndpointAddr::new(to_iroh_id(&addr.id))
        .with_addrs(addr.direct.iter().copied().map(iroh::TransportAddr::Ip))
}

/// Converts an Iroh endpoint address into a Zakura node address.
///
/// Iroh stores addresses in a `BTreeSet`, so the direct addresses come out
/// sorted and deduplicated. Relay and custom addresses are dropped: Zakura
/// disables relays and address lookup.
pub(crate) fn from_iroh_addr(addr: &iroh::EndpointAddr) -> NodeAddr {
    NodeAddr::with_addrs(from_iroh_id(&addr.id), addr.ip_addrs().copied())
}

/// Converts a Zakura node secret key into an Iroh secret key.
pub(crate) fn to_iroh_secret(key: &NodeSecretKey) -> iroh::SecretKey {
    iroh::SecretKey::from_bytes(&key.to_bytes())
}
