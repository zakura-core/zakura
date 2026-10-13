//! Operating-system helpers: kernel drop counters (SOCK-7) and interface
//! listing (SOCK-10, SOCK-12).

use std::net::{IpAddr, SocketAddr};

/// Canonicalizes an address at the crate boundary (SOCK-11).
///
/// Only IPv4-mapped addresses change. Other IPv6 addresses keep their scope
/// ID, which a link-local address needs to be dialable.
pub fn canonical_addr(addr: SocketAddr) -> SocketAddr {
    match addr {
        SocketAddr::V6(v6) => v6
            .ip()
            .to_ipv4_mapped()
            .map_or(addr, |v4| SocketAddr::new(IpAddr::V4(v4), v6.port())),
        v4 => v4,
    }
}

/// Maps an IPv4-mapped IPv6 address to IPv4 (SOCK-11).
pub fn canonical_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V6(v6)),
        v4 => v4,
    }
}

/// Lists the IP addresses of the host's interfaces that are up.
///
/// Zakura uses this for wildcard binds, where the bound address isn't
/// advertisable, and the endpoint polls it to notice network changes. Returns
/// an empty list where the platform has no `getifaddrs`.
pub fn local_interface_ips() -> Vec<IpAddr> {
    #[cfg(unix)]
    let mut ips = unix_interface_ips();
    #[cfg(not(unix))]
    let mut ips = Vec::new();
    ips.sort();
    ips.dedup();
    ips
}

/// Walks `getifaddrs(3)` and keeps the IPv4 and IPv6 addresses of interfaces
/// that are up.
#[cfg(unix)]
#[allow(unsafe_code)]
fn unix_interface_ips() -> Vec<IpAddr> {
    use std::{
        net::{Ipv4Addr, Ipv6Addr},
        ptr,
    };

    let mut head: *mut libc::ifaddrs = ptr::null_mut();
    // SAFETY: `head` is a valid out-pointer. On success the kernel's list
    // stays valid until `freeifaddrs`, which runs below on every path.
    if unsafe { libc::getifaddrs(&mut head) } != 0 {
        return Vec::new();
    }
    let mut ips = Vec::new();
    let mut cursor = head;
    while !cursor.is_null() {
        // SAFETY: `cursor` is a non-null node of the list `getifaddrs` returned,
        // which isn't freed until after the loop. A non-null `ifa_addr` points
        // to a sockaddr whose `sa_family` names its concrete type; each arm
        // reads only that type, which the kernel sized the allocation for.
        let (next, ip) = unsafe {
            let entry = &*cursor;
            // The flag constant is a small positive `c_int`; widening it to the
            // field's unsigned type keeps its value.
            let up = entry.ifa_flags & libc::IFF_UP as libc::c_uint != 0;
            let ip = match (up, entry.ifa_addr.is_null()) {
                (true, false) => match i32::from((*entry.ifa_addr).sa_family) {
                    libc::AF_INET => {
                        let v4 = &*entry.ifa_addr.cast::<libc::sockaddr_in>();
                        Some(IpAddr::V4(Ipv4Addr::from(u32::from_be(v4.sin_addr.s_addr))))
                    }
                    libc::AF_INET6 => {
                        let v6 = &*entry.ifa_addr.cast::<libc::sockaddr_in6>();
                        Some(IpAddr::V6(Ipv6Addr::from(v6.sin6_addr.s6_addr)))
                    }
                    _ => None,
                },
                _ => None,
            };
            (entry.ifa_next, ip)
        };
        ips.extend(ip);
        cursor = next;
    }
    // SAFETY: `head` came from a successful `getifaddrs` and is freed once.
    unsafe { libc::freeifaddrs(head) };
    ips
}

/// Returns the inode of a socket, which keys its `/proc/net/udp` row.
#[cfg(target_os = "linux")]
pub(crate) fn socket_inode(socket: &impl std::os::fd::AsRawFd) -> Option<u64> {
    let link = std::fs::read_link(format!("/proc/self/fd/{}", socket.as_raw_fd())).ok()?;
    let link = link.to_str()?;
    link.strip_prefix("socket:[")?
        .strip_suffix(']')?
        .parse()
        .ok()
}

/// Reads the `drops` column for the socket with `inode` (SOCK-7).
#[cfg(target_os = "linux")]
pub(crate) fn kernel_drops(inode: u64, ipv6: bool) -> Option<u64> {
    let table = std::fs::read_to_string(if ipv6 {
        "/proc/net/udp6"
    } else {
        "/proc/net/udp"
    })
    .ok()?;
    parse_udp_drops(&table, inode)
}

/// Parses one `/proc/net/udp{,6}` table. Column 10 is the inode and the last
/// column is `drops`.
#[cfg(any(target_os = "linux", test))]
fn parse_udp_drops(table: &str, inode: u64) -> Option<u64> {
    table.lines().skip(1).find_map(|line| {
        let fields: Vec<&str> = line.split_whitespace().collect();
        let row_inode: u64 = fields.get(9)?.parse().ok()?;
        (row_inode == inode)
            .then(|| fields.last()?.parse().ok())
            .flatten()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mapped_v6_canonicalizes_to_v4() {
        let mapped: SocketAddr = "[::ffff:192.0.2.1]:8234".parse().unwrap();
        assert_eq!(canonical_addr(mapped), "192.0.2.1:8234".parse().unwrap());
        let v6: SocketAddr = "[2001:db8::1]:8234".parse().unwrap();
        assert_eq!(canonical_addr(v6), v6);
    }

    #[test]
    fn link_local_v6_keeps_its_scope() {
        let SocketAddr::V6(scoped) = canonical_addr("[fe80::1%3]:8234".parse().unwrap()) else {
            panic!("an IPv6 address stays IPv6");
        };
        assert_eq!(scoped.scope_id(), 3);
    }

    #[test]
    fn udp_table_drops_parse_by_inode() {
        let table = "\
   sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode ref pointer drops
  123: 00000000:202A 00000000:0000 07 00000000:00000000 00:00000000 00000000  1000        0 4242 2 0000000000000000 17
  124: 00000000:202B 00000000:0000 07 00000000:00000000 00:00000000 00000000  1000        0 4343 2 0000000000000000 0
";
        assert_eq!(parse_udp_drops(table, 4242), Some(17));
        assert_eq!(parse_udp_drops(table, 4343), Some(0));
        assert_eq!(parse_udp_drops(table, 1), None);
    }

    #[cfg(unix)]
    #[test]
    fn interface_list_includes_loopback() {
        let ips = local_interface_ips();
        assert!(
            ips.iter().any(|ip| ip.is_loopback()),
            "no loopback address in {ips:?}"
        );
        let mut sorted = ips.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(ips, sorted);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn bound_socket_has_a_drop_counter() {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let inode = socket_inode(&socket).unwrap();
        assert_eq!(kernel_drops(inode, false), Some(0));
    }
}
