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
/// `192.0.2.1:8233` or `[2001:db8::1]:8233`, a DNS name and port, such as
/// `peer.example.com:8233`, or a CIDR range, such as `2001:db8::/32`.
/// An IPv4 entry also matches the IPv4-mapped IPv6 form of the same address, and the
/// reverse.
///
/// Every entry is exempt from scoring and bans. An entry that names one address or one
/// DNS name is also dialed, see [`dial_addrs`](Self::dial_addrs) and
/// [`dial_hosts`](Self::dial_hosts). A range of several addresses has no single address
/// to dial.
///
/// This type holds the configured entries only. A DNS name's exemption covers the
/// addresses it resolves to while the node runs, which
/// [`UnconditionalPeerSet`](crate::UnconditionalPeerSet) tracks.
///
/// The list is cheap to clone, so every component that needs it can hold a copy.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UnconditionalPeers {
    /// The configured entries, in config order.
    entries: Arc<[Entry]>,
}

/// One configured unconditional peer entry.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Entry {
    /// An IP address or a range of them.
    Net {
        /// The addresses this entry matches. A single address is a full-length range.
        net: IpNet,

        /// The port to dial, if the entry gives one. Only a single address can have
        /// a port.
        port: Option<u16>,
    },

    /// A DNS name, which is resolved again on every dial.
    Host {
        /// The name, in lower case.
        name: Arc<str>,

        /// The port to dial.
        port: u16,
    },
}

impl Entry {
    /// Returns the range this entry matches, if it is an address or a range.
    fn net(&self) -> Option<&IpNet> {
        match self {
            Entry::Net { net, .. } => Some(net),
            Entry::Host { .. } => None,
        }
    }

    /// Returns whether Zakura dials this entry: it names one address or one DNS name,
    /// rather than a range of addresses.
    fn is_dialed(&self) -> bool {
        match self {
            Entry::Net { net, .. } => net.prefix_len() == net.max_prefix_len(),
            Entry::Host { .. } => true,
        }
    }
}

impl UnconditionalPeers {
    /// Returns whether `ip` is in any configured address or range.
    ///
    /// The addresses that configured DNS names resolve to are not known here, see
    /// [`UnconditionalPeerSet::contains`](crate::UnconditionalPeerSet::contains).
    pub fn contains(&self, ip: IpAddr) -> bool {
        let ip = canonical_ip(ip);
        let mapped_ip = match ip {
            IpAddr::V4(ipv4) => Some(IpAddr::V6(ipv4.to_ipv6_mapped())),
            IpAddr::V6(_) => None,
        };

        self.iter()
            .any(|net| net.contains(&ip) || mapped_ip.is_some_and(|ip| net.contains(&ip)))
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
        self.entries.iter().filter_map(Entry::net)
    }

    /// Returns the most connections Zakura opens to stay connected to these peers: one
    /// for each entry that [`dial_addrs`](Self::dial_addrs) or
    /// [`dial_hosts`](Self::dial_hosts) can return.
    ///
    /// This counts entries, so it never allocates.
    pub fn dial_limit(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.is_dialed())
            .count()
    }

    /// Returns the addresses Zakura dials to stay connected to its unconditional peers.
    ///
    /// Every entry that names one address is dialed, on `default_port` unless the entry
    /// gives its own port. A range of several addresses is never dialed. Addresses are
    /// canonical, in config order, with duplicates removed.
    pub fn dial_addrs(&self, default_port: u16) -> Vec<PeerSocketAddr> {
        let mut dial_addrs = Vec::new();

        for entry in self.entries.iter().filter(|entry| entry.is_dialed()) {
            let Entry::Net { net, port } = entry else {
                continue;
            };
            let addr =
                canonical_peer_addr(SocketAddr::new(net.addr(), port.unwrap_or(default_port)));

            if !dial_addrs.contains(&addr) {
                dial_addrs.push(addr);
            }
        }

        dial_addrs
    }

    /// Returns the DNS names Zakura dials to stay connected to its unconditional peers,
    /// each with its port.
    ///
    /// Names are in lower case, in config order, with duplicates removed.
    pub fn dial_hosts(&self) -> Vec<(Arc<str>, u16)> {
        let mut dial_hosts = Vec::new();

        for entry in self.entries.iter() {
            let Entry::Host { name, port } = entry else {
                continue;
            };
            let host = (name.clone(), *port);

            if !dial_hosts.contains(&host) {
                dial_hosts.push(host);
            }
        }

        dial_hosts
    }
}

impl FromIterator<IpNet> for UnconditionalPeers {
    fn from_iter<T: IntoIterator<Item = IpNet>>(iter: T) -> Self {
        Self {
            entries: iter
                .into_iter()
                .map(|net| Entry::Net {
                    net: net.trunc(),
                    port: None,
                })
                .collect(),
        }
    }
}

/// Parses an IP address or a DNS name with a non-zero port, an IP address, or a CIDR
/// range.
fn parse_entry(entry: &str) -> Option<Entry> {
    if let Ok(addr) = entry.parse::<SocketAddr>() {
        return (addr.port() != 0).then(|| Entry::Net {
            net: IpNet::from(addr.ip()),
            port: Some(addr.port()),
        });
    }

    let net = entry
        .parse::<IpNet>()
        .ok()
        .or_else(|| entry.parse::<IpAddr>().ok().map(IpNet::from));
    if let Some(net) = net {
        return (net == net.trunc()).then_some(Entry::Net { net, port: None });
    }

    let (name, port) = entry.rsplit_once(':')?;
    let port = port.parse::<u16>().ok().filter(|port| *port != 0)?;

    is_dns_name(name).then(|| Entry::Host {
        name: name.to_ascii_lowercase().into(),
        port,
    })
}

/// Returns whether `name` is a DNS name: dot-separated labels of ASCII letters, digits
/// and inner hyphens.
///
/// The last label must not be all digits. No top-level domain is, and the check stops
/// a mistyped IPv4 address, such as `192.0.2.300`, from loading as a name that never
/// resolves.
fn is_dns_name(name: &str) -> bool {
    /// The longest DNS name, in its text form.
    const MAX_NAME_LEN: usize = 253;
    /// The longest label of a DNS name.
    const MAX_LABEL_LEN: usize = 63;

    let is_label = |label: &str| {
        (1..=MAX_LABEL_LEN).contains(&label.len())
            && label
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            && !label.starts_with('-')
            && !label.ends_with('-')
    };

    name.len() <= MAX_NAME_LEN
        && name.split('.').all(is_label)
        && name
            .rsplit('.')
            .next()
            .is_some_and(|last| !last.bytes().all(|byte| byte.is_ascii_digit()))
}

/// Formats an entry, writing a full-length range as a plain IP address.
fn format_entry(entry: &Entry) -> String {
    match entry {
        Entry::Net {
            net,
            port: Some(port),
        } => SocketAddr::new(net.addr(), *port).to_string(),
        Entry::Net { net, port: None } if entry.is_dialed() => net.addr().to_string(),
        Entry::Net { net, port: None } => net.to_string(),
        Entry::Host { name, port } => format!("{name}:{port}"),
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
                         address, an IP address or DNS name with a port, or a canonical CIDR \
                         range without host bits, such as 192.0.2.1, 192.0.2.1:8233, \
                         peer.example.com:8233 or 2001:db8::/32"
                    ))
                })
            })
            .collect::<Result<_, _>>()?;

        Ok(Self { entries })
    }
}
