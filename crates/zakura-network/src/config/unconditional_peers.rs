//! Unconditional peer configuration for zakura-network.

use std::{
    net::{IpAddr, SocketAddr},
    sync::{Arc, RwLock},
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
/// Every entry is exempt from scoring and bans. An entry that names one address is also
/// dialed, see [`dial_addrs`](Self::dial_addrs), and so is a DNS name, see
/// [`dial_names`](Self::dial_names). A range of several addresses has no single address
/// to dial.
///
/// A DNS name stands for the one address that its dialer last resolved it to, see
/// [`set_resolved_ip`](Self::set_resolved_ip). That address is treated like a listed
/// single address until the name resolves to another one.
///
/// The list is cheap to clone, so every component that needs it can hold a copy. Every
/// copy sees the latest resolved addresses.
#[derive(Clone, Debug, Default)]
pub struct UnconditionalPeers {
    /// The configured addresses and ranges, in config order.
    entries: Arc<[Entry]>,

    /// The configured DNS names and their ports, in config order.
    names: Arc<[(Arc<str>, u16)]>,

    /// The canonical address that each name in `names` last resolved to, if any. It is
    /// only read when `names` is not empty.
    resolved_ips: Arc<RwLock<Vec<Option<IpAddr>>>>,
}

/// Two lists are equal when they have the same configured entries, whatever their names
/// have resolved to.
impl PartialEq for UnconditionalPeers {
    fn eq(&self, other: &Self) -> bool {
        self.entries == other.entries && self.names == other.names
    }
}

impl Eq for UnconditionalPeers {}

/// One configured unconditional peer entry.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
struct Entry {
    /// The addresses this entry matches. A single address is a full-length range.
    net: IpNet,

    /// The port to dial, if the entry gives one. Only a single address can have a port.
    port: Option<u16>,

    /// Whether this node can dial the entry, see
    /// [`UnconditionalPeers::leave_out_undialable`]. A configured entry starts out
    /// dialable.
    is_dialable: bool,
}

impl Entry {
    /// Returns whether this entry names one address, rather than a range of them.
    fn is_single_address(&self) -> bool {
        self.net.prefix_len() == self.net.max_prefix_len()
    }

    /// Returns whether Zakura dials this entry: it names one address, which this node
    /// can dial.
    fn is_dialed(&self) -> bool {
        self.is_single_address() && self.is_dialable
    }

    /// Returns the canonical address Zakura dials for this entry, on `default_port`
    /// unless the entry gives its own port.
    fn dial_addr(&self, default_port: u16) -> PeerSocketAddr {
        canonical_peer_addr(SocketAddr::new(
            self.net.addr(),
            self.port.unwrap_or(default_port),
        ))
    }
}

impl UnconditionalPeers {
    /// Returns whether `ip` is in any configured address or range, or is the address
    /// that a configured DNS name last resolved to.
    pub fn contains(&self, ip: IpAddr) -> bool {
        self.contains_in(ip, |_entry| true) || self.is_resolved_ip(ip)
    }

    /// Returns whether `ip` is an address that Zakura dials: the address of an entry
    /// that names one address, or the address that a configured DNS name last resolved
    /// to. An address that is only in a configured range is not, and nor is one that
    /// [`leave_out_undialable`](Self::leave_out_undialable) left out.
    pub fn is_dial_address(&self, ip: IpAddr) -> bool {
        self.contains_in(ip, Entry::is_dialed) || self.is_resolved_ip(ip)
    }

    /// Returns whether `ip` is the address that a configured DNS name last resolved to.
    ///
    /// A list without DNS names returns false without taking the lock.
    fn is_resolved_ip(&self, ip: IpAddr) -> bool {
        !self.names.is_empty()
            && self
                .resolved_ips
                .read()
                .expect("resolved address lock should not be poisoned")
                .contains(&Some(canonical_ip(ip)))
    }

    /// Records that the DNS name at `name_index` in [`dial_names`](Self::dial_names)
    /// now resolves to `ip`. Every copy of this list then treats `ip` like a listed
    /// single address, and no longer treats the name's previous address that way.
    pub(crate) fn set_resolved_ip(&self, name_index: usize, ip: IpAddr) {
        let mut resolved_ips = self
            .resolved_ips
            .write()
            .expect("resolved address lock should not be poisoned");

        if let Some(resolved_ip) = resolved_ips.get_mut(name_index) {
            *resolved_ip = Some(canonical_ip(ip));
        }
    }

    /// Returns whether `ip` is in a configured address or range whose entry
    /// `is_included` accepts.
    fn contains_in(&self, ip: IpAddr, is_included: impl Fn(&Entry) -> bool) -> bool {
        let ip = canonical_ip(ip);
        let mapped_ip = match ip {
            IpAddr::V4(ipv4) => Some(IpAddr::V6(ipv4.to_ipv6_mapped())),
            IpAddr::V6(_) => None,
        };

        self.entries
            .iter()
            .filter(|entry| is_included(entry))
            .any(|entry| {
                entry.net.contains(&ip)
                    || mapped_ip.is_some_and(|mapped_ip| entry.net.contains(&mapped_ip))
            })
    }

    /// Returns the number of configured entries.
    pub fn len(&self) -> usize {
        self.entries.len() + self.names.len()
    }

    /// Returns whether no entries are configured.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.names.is_empty()
    }

    /// Returns the configured addresses and ranges, without their ports.
    pub fn iter(&self) -> impl Iterator<Item = &IpNet> {
        self.entries.iter().map(|entry| &entry.net)
    }

    /// Returns the most peers that Zakura dials: the most addresses that
    /// [`dial_addrs`](Self::dial_addrs) can return, plus one for each DNS name.
    ///
    /// This counts every entry that Zakura dials, so it never allocates.
    pub fn dial_limit(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.is_dialed())
            .count()
            + self.names.len()
    }

    /// Returns the DNS names that Zakura dials to stay connected to its unconditional
    /// peers, with their ports, in config order.
    ///
    /// Zakura looks a name up before each dial and dials the first address it can, see
    /// [`set_resolved_ip`](Self::set_resolved_ip).
    pub fn dial_names(&self) -> impl Iterator<Item = (Arc<str>, u16)> + '_ {
        self.names.iter().cloned()
    }

    /// Returns the addresses Zakura dials to stay connected to its unconditional peers.
    ///
    /// Every entry that names one address is dialed, on `default_port` unless the entry
    /// gives its own port, except the ones that
    /// [`leave_out_undialable`](Self::leave_out_undialable) left out. A range of several
    /// addresses is never dialed. Addresses are canonical, in config order, with
    /// duplicates removed.
    pub fn dial_addrs(&self, default_port: u16) -> Vec<PeerSocketAddr> {
        let mut dial_addrs = Vec::with_capacity(self.dial_limit());

        for entry in self.entries.iter().filter(|entry| entry.is_dialed()) {
            let addr = entry.dial_addr(default_port);
            if !dial_addrs.contains(&addr) {
                dial_addrs.push(addr);
            }
        }

        dial_addrs
    }

    /// Returns these peers with every entry that names one address `is_dialable` rejects,
    /// such as this node's own listener, left out of dialing.
    ///
    /// Such an entry stays exempt from scoring and bans, see [`contains`](Self::contains),
    /// but [`is_dial_address`](Self::is_dial_address), [`dial_addrs`](Self::dial_addrs)
    /// and [`dial_limit`](Self::dial_limit) leave it out, so nothing keeps room for a
    /// peer that Zakura never dials. The config serializes as before.
    pub(crate) fn leave_out_undialable(
        &self,
        default_port: u16,
        is_dialable: impl Fn(PeerSocketAddr) -> bool,
    ) -> Self {
        Self {
            entries: self
                .entries
                .iter()
                .map(|entry| Entry {
                    is_dialable: !entry.is_single_address()
                        || is_dialable(entry.dial_addr(default_port)),
                    ..*entry
                })
                .collect(),
            names: self.names.clone(),
            resolved_ips: self.resolved_ips.clone(),
        }
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
                    is_dialable: true,
                })
                .collect(),
            ..Self::default()
        }
    }
}

/// Parses a DNS name with a non-zero port, such as `peer.example.com:8233`.
fn parse_name(entry: &str) -> Option<(Arc<str>, u16)> {
    let (name, port) = entry.rsplit_once(':')?;
    let port = port.parse::<u16>().ok().filter(|port| *port != 0)?;

    is_dns_name(name).then(|| (name.into(), port))
}

/// Returns whether `name` is a DNS name: dot-separated labels of ASCII letters, digits
/// and inner hyphens, optionally followed by the dot that marks an absolute name.
///
/// The last label must not be all digits. No top-level domain is, and the check stops
/// a mistyped IPv4 address, such as `192.0.2.300`, from loading as a name that never
/// resolves.
fn is_dns_name(name: &str) -> bool {
    /// The longest DNS name, in its text form.
    const MAX_NAME_LEN: usize = 253;
    /// The longest label of a DNS name.
    const MAX_LABEL_LEN: usize = 63;

    let name = name.strip_suffix('.').unwrap_or(name);

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

/// Parses an IP address, an IP address with a non-zero port, or a CIDR range.
fn parse_entry(entry: &str) -> Option<Entry> {
    if let Ok(addr) = entry.parse::<SocketAddr>() {
        return (addr.port() != 0).then(|| Entry {
            net: IpNet::from(addr.ip()),
            port: Some(addr.port()),
            is_dialable: true,
        });
    }

    entry
        .parse::<IpNet>()
        .ok()
        .or_else(|| entry.parse::<IpAddr>().ok().map(IpNet::from))
        .filter(|net| *net == net.trunc())
        .map(|net| Entry {
            net,
            port: None,
            is_dialable: true,
        })
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
        // Addresses and ranges come first, then DNS names.
        serializer.collect_seq(
            self.entries.iter().map(format_entry).chain(
                self.names
                    .iter()
                    .map(|(name, port)| format!("{name}:{port}")),
            ),
        )
    }
}

impl<'de> Deserialize<'de> for UnconditionalPeers {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut entries = Vec::new();
        let mut names = Vec::new();

        for entry in Vec::<String>::deserialize(deserializer)? {
            if let Some(entry) = parse_entry(&entry) {
                entries.push(entry);
            } else if let Some(name) = parse_name(&entry) {
                names.push(name);
            } else {
                return Err(de::Error::custom(format!(
                    "invalid network.unconditional_peers entry {entry:?}: expected an IP \
                     address, an IP address or DNS name with a port, or a canonical CIDR \
                     range without host bits, such as 192.0.2.1, 192.0.2.1:8233, \
                     peer.example.com:8233 or 2001:db8::/32"
                )));
            }
        }

        Ok(Self {
            resolved_ips: Arc::new(RwLock::new(vec![None; names.len()])),
            entries: entries.into(),
            names: names.into(),
        })
    }
}
