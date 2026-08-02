//! Canonical IP identities used by Zakura connection admission and discovery.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// Returns one identity for an IPv4 address and its unambiguous IPv6 transition encodings.
///
/// Admission accounting must not let a peer choose a separate bucket merely by encoding the same
/// IPv4 identity as IPv4-mapped, IPv4-compatible, 6to4, Teredo, or well-known-prefix NAT64 IPv6.
/// Network-specific NAT64 prefixes cannot be decoded without local prefix configuration, so they
/// remain IPv6 identities and discovery rejects the reserved local-use prefix separately.
pub(crate) fn canonical_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V4(_) => ip,
        IpAddr::V6(ipv6) => ipv4_embedded_in_ipv6(ipv6).map_or(ip, IpAddr::V4),
    }
}

fn ipv4_embedded_in_ipv6(ip: Ipv6Addr) -> Option<Ipv4Addr> {
    let octets = ip.octets();

    // RFC 4291 IPv4-mapped IPv6: ::ffff:192.0.2.1.
    if octets[..10] == [0; 10] && octets[10..12] == [0xff; 2] {
        return Some(ipv4_from_octets(&octets[12..16]));
    }

    // Deprecated RFC 4291 IPv4-compatible IPv6: ::192.0.2.1. Keep the native IPv6
    // unspecified and loopback addresses distinct from the former transition format.
    if octets[..12] == [0; 12] && octets[12..16] != [0, 0, 0, 0] && octets[12..16] != [0, 0, 0, 1] {
        return Some(ipv4_from_octets(&octets[12..16]));
    }

    // RFC 3056 6to4: 2002:V4ADDR::/48.
    if octets[..2] == [0x20, 0x02] {
        return Some(ipv4_from_octets(&octets[2..6]));
    }

    // RFC 4380 Teredo: the client IPv4 address is bitwise-inverted in the final 32 bits.
    if octets[..4] == [0x20, 0x01, 0, 0] {
        return Some(Ipv4Addr::new(
            !octets[12],
            !octets[13],
            !octets[14],
            !octets[15],
        ));
    }

    // RFC 6052 well-known NAT64 prefix: 64:ff9b::/96.
    if octets[..12] == [0x00, 0x64, 0xff, 0x9b, 0, 0, 0, 0, 0, 0, 0, 0] {
        return Some(ipv4_from_octets(&octets[12..16]));
    }

    None
}

fn ipv4_from_octets(octets: &[u8]) -> Ipv4Addr {
    Ipv4Addr::new(octets[0], octets[1], octets[2], octets[3])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonicalizes_unambiguous_ipv4_transition_encodings() {
        let ipv4: IpAddr = "93.184.216.34".parse().expect("IPv4 test address parses");
        for encoded in [
            "::ffff:93.184.216.34",
            "::93.184.216.34",
            "2002:5db8:d822::",
            "2001:0:c000:22d::a247:27dd",
            "64:ff9b::5db8:d822",
        ] {
            let encoded = encoded.parse().expect("IPv6 test address parses");
            assert_eq!(canonical_ip(encoded), ipv4, "failed to decode {encoded}");
        }
    }

    #[test]
    fn preserves_native_and_ambiguous_ipv6_addresses() {
        for address in ["::", "::1", "2001:db8::1", "64:ff9b:1::5db8:d822"] {
            let address = address.parse().expect("IPv6 test address parses");
            assert_eq!(canonical_ip(address), address);
        }
    }
}
