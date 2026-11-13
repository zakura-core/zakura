//! The unconditional peers of a running node.

use std::{collections::HashSet, net::IpAddr, sync::Arc};

use tokio::sync::watch;

use crate::{protocol::external::canonical_ip, UnconditionalPeers};

/// The peers that a running node never scores or bans.
///
/// These are the addresses and ranges in the configured [`UnconditionalPeers`], plus
/// the addresses that its DNS names currently resolve to. Zakura resolves a name each
/// time it dials that peer, so the resolved addresses change while the node runs.
///
/// The set is cheap to clone, and every clone sees the latest resolved addresses.
#[derive(Clone, Debug)]
pub struct UnconditionalPeerSet {
    /// The configured addresses and ranges.
    configured: UnconditionalPeers,

    /// The canonical addresses that the configured DNS names currently resolve to.
    resolved: watch::Receiver<Arc<HashSet<IpAddr>>>,
}

/// The publishing half of an [`UnconditionalPeerSet`], held by the task that resolves
/// its DNS names.
#[derive(Debug)]
pub(crate) struct ResolvedUnconditionalPeers(watch::Sender<Arc<HashSet<IpAddr>>>);

impl UnconditionalPeerSet {
    /// Returns a set for `configured`, and the handle that publishes the addresses its
    /// DNS names resolve to.
    pub(crate) fn new(configured: UnconditionalPeers) -> (Self, ResolvedUnconditionalPeers) {
        let (sender, resolved) = watch::channel(Arc::default());

        (
            Self {
                configured,
                resolved,
            },
            ResolvedUnconditionalPeers(sender),
        )
    }

    /// Returns whether `ip` is a configured address, is in a configured range, or is an
    /// address that a configured DNS name currently resolves to.
    pub fn contains(&self, ip: IpAddr) -> bool {
        self.configured.contains(ip) || self.resolved.borrow().contains(&canonical_ip(ip))
    }

    /// Returns whether `ip` is the address of a peer that Zakura keeps connected: a
    /// configured single address, or an address that a configured DNS name currently
    /// resolves to. An address that is only in a configured range is not.
    pub fn is_kept_connected(&self, ip: IpAddr) -> bool {
        self.configured.is_dial_address(ip) || self.resolved.borrow().contains(&canonical_ip(ip))
    }
}

impl From<UnconditionalPeers> for UnconditionalPeerSet {
    /// Returns a set that only holds the configured addresses and ranges. Nothing
    /// resolves its DNS names.
    fn from(configured: UnconditionalPeers) -> Self {
        Self::new(configured).0
    }
}

impl Default for UnconditionalPeerSet {
    fn default() -> Self {
        UnconditionalPeers::default().into()
    }
}

impl ResolvedUnconditionalPeers {
    /// Replaces the resolved addresses with `ips`. Every clone of the set sees them at
    /// once.
    pub(crate) fn publish(&self, ips: impl IntoIterator<Item = IpAddr>) {
        self.0
            .send_replace(Arc::new(ips.into_iter().map(canonical_ip).collect()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parses `ip` as an IP address.
    fn ip(ip: &str) -> IpAddr {
        ip.parse().expect("test address parses")
    }

    /// The set holds the configured addresses and ranges, plus the latest resolved
    /// addresses, and every clone sees a new resolution at once.
    #[test]
    fn set_follows_the_latest_resolved_addresses() {
        let configured: UnconditionalPeers =
            ["192.0.2.0/24".parse().unwrap()].into_iter().collect();
        let (set, resolved_peers) = UnconditionalPeerSet::new(configured);
        let clone = set.clone();

        assert!(set.contains(ip("192.0.2.7")));
        assert!(!set.contains(ip("198.51.100.1")));

        resolved_peers.publish([ip("198.51.100.1"), ip("2001:db8::1")]);
        for set in [&set, &clone] {
            assert!(
                set.contains(ip("192.0.2.7")),
                "configured ranges stay exempt"
            );
            assert!(set.contains(ip("198.51.100.1")));
            assert!(set.contains(ip("2001:db8::1")));
            assert!(!set.contains(ip("198.51.100.2")));
        }

        // A new resolution replaces the last one.
        resolved_peers.publish([ip("198.51.100.2")]);
        for set in [&set, &clone] {
            assert!(set.contains(ip("198.51.100.2")));
            assert!(!set.contains(ip("198.51.100.1")));
            assert!(!set.contains(ip("2001:db8::1")));
        }

        // The last resolution outlives the task that published it.
        drop(resolved_peers);
        assert!(set.contains(ip("198.51.100.2")));
    }

    /// A resolved IPv4 address matches its IPv4-mapped IPv6 form, and the reverse.
    #[test]
    fn resolved_addresses_match_ipv4_mapped_forms() {
        let (set, resolved_peers) = UnconditionalPeerSet::new(UnconditionalPeers::default());

        resolved_peers.publish([ip("192.0.2.1"), ip("::ffff:192.0.2.2")]);

        assert!(set.contains(ip("::ffff:192.0.2.1")));
        assert!(set.contains(ip("192.0.2.2")));
        assert!(!set.contains(ip("192.0.2.3")));
    }

    /// Zakura keeps a peer connected when it is listed as one address, or is an address
    /// that a listed name currently resolves to. A range is not enough.
    #[test]
    fn kept_connected_peers_are_single_addresses_and_resolved_names() {
        let configured: UnconditionalPeers = ["192.0.2.1/32", "198.51.100.0/24"]
            .into_iter()
            .map(|net| net.parse().unwrap())
            .collect();
        let (set, resolved_peers) = UnconditionalPeerSet::new(configured);

        assert!(set.is_kept_connected(ip("192.0.2.1")));
        assert!(set.is_kept_connected(ip("::ffff:192.0.2.1")));
        assert!(set.contains(ip("198.51.100.7")));
        assert!(!set.is_kept_connected(ip("198.51.100.7")));
        assert!(!set.is_kept_connected(ip("203.0.113.1")));

        resolved_peers.publish([ip("203.0.113.1")]);
        assert!(set.is_kept_connected(ip("203.0.113.1")));
        assert!(set.is_kept_connected(ip("::ffff:203.0.113.1")));

        // A new resolution replaces the last one.
        resolved_peers.publish([ip("203.0.113.2")]);
        assert!(!set.is_kept_connected(ip("203.0.113.1")));
        assert!(set.is_kept_connected(ip("192.0.2.1")));
    }

    /// A set made from a config alone holds its addresses and ranges, and nothing else.
    #[test]
    fn set_from_config_holds_the_configured_addresses() {
        let configured: UnconditionalPeers =
            ["192.0.2.1/32".parse().unwrap()].into_iter().collect();
        let set = UnconditionalPeerSet::from(configured);

        assert!(set.contains(ip("192.0.2.1")));
        assert!(!set.contains(ip("192.0.2.2")));
        assert!(!UnconditionalPeerSet::default().contains(ip("192.0.2.1")));
    }
}
