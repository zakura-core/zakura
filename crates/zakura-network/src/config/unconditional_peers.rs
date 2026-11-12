//! Unconditional peer configuration for zakura-network.

use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

use ipnet::IpNet;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    protocol::external::{canonical_ip, canonical_peer_addr},
    PeerSocketAddr,
};

/// A list of peers that Zakura never scores or bans, and keeps connected where it can.
///
/// Each entry is an IP address, such as `192.0.2.1`, an IP address and port, such as
/// `192.0.2.1:8233` or `[2001:db8::1]:8233`, or a CIDR range, such as `2001:db8::/32`.
/// An IPv4 entry also matches the IPv4-mapped IPv6 form of the same address, and the
/// reverse.
///
/// Every entry is exempt from scoring and bans. An entry that names one address is also
/// dialed, see [`dial_addrs`](Self::dial_addrs). A range has no single address to dial.
///
/// The list is cheap to clone, so every component that needs it can hold a copy.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UnconditionalPeers {
    /// The configured entries, in config order.
    entries: Arc<[Entry]>,
}

/// One configured unconditional peer entry.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
struct Entry {
    /// The addresses this entry matches. A single address is a full-length range.
    net: IpNet,

    /// The port to dial, if the entry gives one. Only a single address can have a port.
    port: Option<u16>,
}

impl Entry {
    /// Returns whether this entry names one address, rather than a range of them.
    fn is_single_address(&self) -> bool {
        self.net.prefix_len() == self.net.max_prefix_len()
    }
}

impl UnconditionalPeers {
    /// Returns whether `ip` is in any configured address or range.
    pub fn contains(&self, ip: IpAddr) -> bool {
        let ip = canonical_ip(ip);
        let mapped_ip = match ip {
            IpAddr::V4(ipv4) => Some(IpAddr::V6(ipv4.to_ipv6_mapped())),
            IpAddr::V6(_) => None,
        };

        self.entries.iter().any(|entry| {
            entry.net.contains(&ip)
                || mapped_ip.is_some_and(|mapped_ip| entry.net.contains(&mapped_ip))
        })
    }

    /// Returns the number of configured entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether no entries are configured.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the configured addresses and ranges, without their ports.
    pub fn iter(&self) -> impl Iterator<Item = &IpNet> {
        self.entries.iter().map(|entry| &entry.net)
    }

    /// Returns the most addresses that [`dial_addrs`](Self::dial_addrs) can return.
    ///
    /// This counts every entry that names one address, so it never allocates.
    pub fn dial_limit(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.is_single_address())
            .count()
    }

    /// Returns the addresses Zakura dials to stay connected to its unconditional peers.
    ///
    /// Every entry that names one address is dialed, on `default_port` unless the entry
    /// gives its own port. A range is never dialed. Addresses are canonical, in config
    /// order, with duplicates removed.
    pub fn dial_addrs(&self, default_port: u16) -> Vec<PeerSocketAddr> {
        let mut dial_addrs = Vec::with_capacity(self.dial_limit());

        for entry in self
            .entries
            .iter()
            .filter(|entry| entry.is_single_address())
        {
            let addr = canonical_peer_addr(SocketAddr::new(
                entry.net.addr(),
                entry.port.unwrap_or(default_port),
            ));

            if !dial_addrs.contains(&addr) {
                dial_addrs.push(addr);
            }
        }

        dial_addrs
    }
}

impl FromIterator<IpNet> for UnconditionalPeers {
    fn from_iter<T: IntoIterator<Item = IpNet>>(iter: T) -> Self {
        Self {
            entries: iter
                .into_iter()
                .map(|net| Entry {
                    net: net.trunc(),
                    port: None,
                })
                .collect(),
        }
    }
}

/// Parses an IP address, an IP address with a non-zero port, or a CIDR range.
fn parse_entry(entry: &str) -> Option<Entry> {
    if let Ok(addr) = entry.parse::<SocketAddr>() {
        return (addr.port() != 0).then(|| Entry {
            net: IpNet::from(addr.ip()),
            port: Some(addr.port()),
        });
    }

    entry
        .parse::<IpNet>()
        .ok()
        .or_else(|| entry.parse::<IpAddr>().ok().map(IpNet::from))
        .filter(|net| *net == net.trunc())
        .map(|net| Entry { net, port: None })
}

/// Formats an entry, writing a full-length range as a plain IP address.
fn format_entry(entry: &Entry) -> String {
    match entry.port {
        Some(port) => SocketAddr::new(entry.net.addr(), port).to_string(),
        None if entry.is_single_address() => entry.net.addr().to_string(),
        None => entry.net.to_string(),
    }
}

impl Serialize for UnconditionalPeers {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_seq(self.entries.iter().map(format_entry))
    }
}

impl<'de> Deserialize<'de> for UnconditionalPeers {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let entries = Vec::<String>::deserialize(deserializer)?
            .iter()
            .map(|entry| {
                parse_entry(entry).ok_or_else(|| {
                    de::Error::custom(format!(
                        "invalid network.unconditional_peers entry {entry:?}: expected an IP \
                         address, an IP address with a port, or a canonical CIDR range without \
                         host bits, such as 192.0.2.1, 192.0.2.1:8233 or 2001:db8::/32"
                    ))
                })
            })
            .collect::<Result<_, _>>()?;

        Ok(Self { entries })
    }
}
