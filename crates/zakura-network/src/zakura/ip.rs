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

/// Returns the address group that connection admission charges as one host.
///
/// That's `ip` after [`canonical_ip`] folding, with a global unicast IPv6
/// address (2000::/3) cut to its /64. RFC 4291 interface IDs are 64 bits, so a
/// host that holds a /64 can pick any of its 2^64 addresses per connection, and
/// an exact-address cap would never bind it. Other IPv6 addresses, such as
/// unique local, link-local and loopback, stay exact, so containers or hosts
/// sharing one local subnet keep their own limits. Every per-IP admission
/// count and discovery's per-IP dial reservation key peers through this.
pub(crate) fn admission_host(ip: IpAddr) -> IpAddr {
    match canonical_ip(ip) {
        IpAddr::V6(ipv6) if ipv6.segments()[0] & 0xe000 == 0x2000 => {
            let mut octets = ipv6.octets();
            octets[8..].fill(0);
            IpAddr::V6(Ipv6Addr::from(octets))
        }
        ip => ip,
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
    fn admission_host_groups_global_ipv6_by_64() {
        let host = |ip: &str| admission_host(ip.parse().expect("test address parses"));
        let ip = |ip: &str| -> IpAddr { ip.parse().expect("test address parses") };

        // Global unicast IPv6 groups by /64, so every interface ID is one host.
        assert_eq!(host("2001:db8:1:2::1"), ip("2001:db8:1:2::"));
        assert_eq!(
            host("2001:db8:1:2:ffff:ffff:ffff:ffff"),
            ip("2001:db8:1:2::")
        );
        assert_eq!(host("3fff:ffff:ffff:ffff::9"), ip("3fff:ffff:ffff:ffff::"));
        assert_ne!(host("2001:db8:1:2::1"), host("2001:db8:1:3::1"));

        // Transition encodings fold to their IPv4 host before any masking.
        assert_eq!(host("2002:5db8:d822::1"), ip("93.184.216.34"));
        assert_eq!(host("2001:0:c000:22d::a247:27dd"), ip("93.184.216.34"));
        assert_eq!(host("::ffff:93.184.216.34"), ip("93.184.216.34"));
        assert_eq!(host("93.184.216.34"), ip("93.184.216.34"));

        // Non-global IPv6 stays exact.
        for exact in [
            "::1",
            "::",
            "fe80::1:2",
            "fd00:1:2:3::4",
            "ff02::1",
            "64:ff9b:1::5db8:d822",
        ] {
            assert_eq!(host(exact), ip(exact), "{exact}");
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
