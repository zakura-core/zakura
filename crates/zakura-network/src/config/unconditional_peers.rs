//! Unconditional peer configuration for zakura-network.

use std::{net::IpAddr, sync::Arc};

use ipnet::IpNet;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

use crate::protocol::external::canonical_ip;

/// A list of peer IP addresses and CIDR ranges that Zakura never scores or bans.
///
/// Each entry is an IP address, such as `192.0.2.1`, or a CIDR range, such as
/// `2001:db8::/32`. An IPv4 entry also matches the IPv4-mapped IPv6 form of the
/// same address, and the reverse.
///
/// The list is cheap to clone, so every component that needs it can hold a copy.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UnconditionalPeers {
    /// The configured ranges. A plain IP address is stored as a full-length range.
    nets: Arc<[IpNet]>,
}

impl UnconditionalPeers {
    /// Returns whether `ip` is in any configured address or range.
    pub fn contains(&self, ip: IpAddr) -> bool {
        let ip = canonical_ip(ip);
        let mapped_ip = match ip {
            IpAddr::V4(ipv4) => Some(IpAddr::V6(ipv4.to_ipv6_mapped())),
            IpAddr::V6(_) => None,
        };

        self.nets.iter().any(|net| {
            net.contains(&ip) || mapped_ip.is_some_and(|mapped_ip| net.contains(&mapped_ip))
        })
    }

    /// Returns the number of configured addresses and ranges.
    pub fn len(&self) -> usize {
        self.nets.len()
    }

    /// Returns whether no addresses or ranges are configured.
    pub fn is_empty(&self) -> bool {
        self.nets.is_empty()
    }

    /// Returns the configured addresses and ranges.
    pub fn iter(&self) -> impl Iterator<Item = &IpNet> {
        self.nets.iter()
    }
}

impl FromIterator<IpNet> for UnconditionalPeers {
    fn from_iter<T: IntoIterator<Item = IpNet>>(iter: T) -> Self {
        Self {
            nets: iter.into_iter().map(|net| net.trunc()).collect(),
        }
    }
}

/// Parses an IP address or CIDR range.
fn parse_entry(entry: &str) -> Option<IpNet> {
    entry
        .parse::<IpNet>()
        .ok()
        .or_else(|| entry.parse::<IpAddr>().ok().map(IpNet::from))
}

/// Formats a range, writing a full-length range as a plain IP address.
fn format_entry(net: &IpNet) -> String {
    if net.prefix_len() == net.max_prefix_len() {
        net.addr().to_string()
    } else {
        net.to_string()
    }
}

impl Serialize for UnconditionalPeers {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_seq(self.nets.iter().map(format_entry))
    }
}

impl<'de> Deserialize<'de> for UnconditionalPeers {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Vec::<String>::deserialize(deserializer)?
            .iter()
            .map(|entry| {
                parse_entry(entry).ok_or_else(|| {
                    de::Error::custom(format!(
                        "invalid network.unconditional_peers entry {entry:?}: expected an IP \
                         address or CIDR range, such as 192.0.2.1 or 2001:db8::/32"
                    ))
                })
            })
            .collect()
    }
}
