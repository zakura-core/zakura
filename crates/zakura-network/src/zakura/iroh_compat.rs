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

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, str::FromStr};

    use super::*;

    #[test]
    fn keys_and_ids_round_trip_and_derive_the_same_node_id() {
        for seed in [[0u8; 32], [1; 32], [0xff; 32]] {
            let secret = NodeSecretKey::from_bytes(&seed);
            let iroh_secret = to_iroh_secret(&secret);
            assert_eq!(iroh_secret.to_bytes(), seed);
            assert_eq!(from_iroh_id(&iroh_secret.public()), secret.public());
            assert_eq!(to_iroh_id(&secret.public()), iroh_secret.public());
        }

        let generated = NodeSecretKey::generate();
        assert_eq!(
            from_iroh_id(&to_iroh_secret(&generated).public()),
            generated.public()
        );
    }

    #[test]
    fn key_text_parses_to_the_same_identity_as_iroh() {
        // RFC 8032 §7.1 test 1 seed, as lowercase hex and as RFC 4648 base32.
        let hex = "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60";
        let base32 = "TVQ3DHPP7VNGBOUEJL2JF3BMYRCETRLJPMZGSGLQHOWAGHFOP5QA";

        // Key files and config overrides use lowercase hex; Iroh also accepted
        // base32 in either case.
        for text in [hex.to_owned(), base32.to_owned(), base32.to_lowercase()] {
            let ours: NodeSecretKey = text.parse().expect("Zakura parses the key text");
            let theirs: iroh::SecretKey = text.parse().expect("Iroh parses the key text");
            assert_eq!(ours.to_bytes(), theirs.to_bytes());
            assert_eq!(from_iroh_id(&theirs.public()), ours.public());
            assert_eq!(ours.to_hex(), hex);
        }

        let iroh_id = iroh::SecretKey::from_str(hex)
            .expect("Iroh parses the key text")
            .public();
        assert_eq!(
            iroh_id
                .to_string()
                .parse::<NodeId>()
                .expect("hex node ID parses"),
            from_iroh_id(&iroh_id)
        );
    }

    #[test]
    fn addresses_round_trip_as_a_sorted_set() {
        let id = NodeSecretKey::from_bytes(&[3; 32]).public();
        let high: SocketAddr = "127.0.0.1:9000".parse().expect("valid address");
        let low: SocketAddr = "127.0.0.1:8000".parse().expect("valid address");
        let addr = NodeAddr::with_addrs(id, [high, low, high]);

        let iroh_addr = to_iroh_addr(&addr);
        assert_eq!(iroh_addr.id, to_iroh_id(&id));
        assert_eq!(
            from_iroh_addr(&iroh_addr),
            NodeAddr::with_addrs(id, [low, high])
        );
    }
}
