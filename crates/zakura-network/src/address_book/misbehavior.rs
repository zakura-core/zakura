//! Per-IP peer misbehavior scores, and the bans they lead to.

use std::{
    collections::{HashMap, VecDeque},
    net::IpAddr,
    sync::{Arc, Mutex},
};

use crate::{
    config::UnconditionalPeers, constants, protocol::external::canonical_ip, BannedIps,
    PeerSocketAddr,
};

/// Records peer misbehavior and bans an IP address once its score reaches
/// [`MAX_PEER_MISBEHAVIOR_SCORE`](constants::MAX_PEER_MISBEHAVIOR_SCORE).
///
/// A report updates the score in place and returns: it never waits, and it is never
/// dropped. Only the report that crosses the threshold does more work, by adding the
/// IP address to the shared [`BannedIps`] list. The peer set watches that list and
/// disconnects the IP's peers, and the address book stops dialing and gossiping it.
///
/// Scores are kept per IP address, like bans, so a peer can't reset its score by
/// reconnecting from another port. Configured unconditional peers are never scored.
///
/// The handle is cheap to clone, so every component that reports misbehavior can hold
/// a copy.
#[derive(Clone, Debug)]
pub struct MisbehaviorReporter {
    /// Scores of IP addresses that have misbehaved but are not banned yet.
    scores: Arc<Mutex<ScoreTable>>,

    /// The ban list shared with the address book and the peer set.
    bans: BannedIps,

    /// Peers that are never scored or banned.
    unconditional_peers: UnconditionalPeers,
}

impl MisbehaviorReporter {
    /// Returns a reporter that bans IP addresses in `bans`, and never scores
    /// `unconditional_peers`.
    pub fn new(bans: BannedIps, unconditional_peers: UnconditionalPeers) -> Self {
        Self {
            scores: Default::default(),
            bans,
            unconditional_peers,
        }
    }

    /// Adds `score_increment` to the misbehavior score of `addr`'s IP address, and bans
    /// the IP address if its score reaches the ban threshold.
    pub fn report(&self, addr: PeerSocketAddr, score_increment: u32) {
        let ip = canonical_ip(addr.ip());

        if score_increment == 0 || self.bans.contains(ip) {
            return;
        }

        if self.unconditional_peers.contains(ip) {
            tracing::debug!(
                score_increment,
                "ignored misbehavior report for an unconditional peer"
            );
            metrics::counter!("address_book.misbehavior.unconditional_ignored.total").increment(1);
            return;
        }

        let reached_ban_threshold = self
            .scores
            .lock()
            .expect("score table lock should not be poisoned")
            .add(ip, score_increment);

        if reached_ban_threshold {
            // The ban list is the record of a banned IP from now on.
            self.bans.insert(ip);
            metrics::counter!("address_book.misbehavior.banned.total").increment(1);
            tracing::warn!(
                score_increment,
                "banned the IP address of a misbehaving peer"
            );
        }
    }

    /// Returns the misbehavior score of `ip`, or zero if it has none.
    ///
    /// A banned IP address has no score: its ban replaces it.
    pub fn score(&self, ip: IpAddr) -> u32 {
        self.scores
            .lock()
            .expect("score table lock should not be poisoned")
            .get(canonical_ip(ip))
    }

    /// Returns the ban list this reporter bans IP addresses in.
    pub(crate) fn bans(&self) -> BannedIps {
        self.bans.clone()
    }

    /// Returns whether `ip` is banned.
    pub fn is_banned(&self, ip: IpAddr) -> bool {
        self.bans.contains(ip)
    }
}

/// A bounded table of misbehavior scores below the ban threshold.
///
/// Peers choose their IP addresses, so the table keeps at most
/// [`MAX_BANNED_IPS`](constants::MAX_BANNED_IPS) of them, and forgets the IP address
/// that was scored first when it is full.
#[derive(Debug, Default)]
struct ScoreTable {
    /// The score of each IP address.
    scores: HashMap<IpAddr, u32>,

    /// The scored IP addresses in the order they were first scored.
    insertion_order: VecDeque<IpAddr>,
}

impl ScoreTable {
    /// Adds `score_increment` to the score of `ip`.
    ///
    /// Returns `true` if this reaches the ban threshold. The IP address is then removed
    /// from the table, so it reaches the threshold only once.
    fn add(&mut self, ip: IpAddr, score_increment: u32) -> bool {
        let score = self.scores.entry(ip).or_insert_with(|| {
            self.insertion_order.push_back(ip);
            0
        });
        *score = score.saturating_add(score_increment);

        if *score >= constants::MAX_PEER_MISBEHAVIOR_SCORE {
            self.remove(ip);
            return true;
        }

        if self.scores.len() > constants::MAX_BANNED_IPS {
            let oldest = self
                .insertion_order
                .pop_front()
                .expect("the insertion order has an entry for every scored IP");
            self.scores.remove(&oldest);
        }

        false
    }

    /// Returns the score of `ip`, or zero if it has none.
    fn get(&self, ip: IpAddr) -> u32 {
        self.scores.get(&ip).copied().unwrap_or(0)
    }

    /// Removes `ip` and its score.
    fn remove(&mut self, ip: IpAddr) {
        if self.scores.remove(&ip).is_some() {
            self.insertion_order.retain(|scored_ip| *scored_ip != ip);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};

    use super::*;

    fn reporter() -> MisbehaviorReporter {
        MisbehaviorReporter::new(BannedIps::default(), UnconditionalPeers::default())
    }

    fn peer_addr(ip: [u8; 4], port: u16) -> PeerSocketAddr {
        SocketAddr::from((ip, port)).into()
    }

    /// Scores add up per IP address across ports, and the report that reaches the
    /// threshold bans the IP and clears its score.
    #[test]
    fn scores_add_up_per_ip_until_the_ban() {
        let reporter = reporter();
        let first_port = peer_addr([127, 0, 0, 1], 8233);
        let second_port = peer_addr([127, 0, 0, 1], 51234);
        let other_ip = peer_addr([127, 0, 0, 2], 8233);

        reporter.report(first_port, 40);
        reporter.report(second_port, 40);
        reporter.report(other_ip, 40);
        assert_eq!(reporter.score(first_port.ip()), 80);
        assert!(!reporter.is_banned(first_port.ip()));

        reporter.report(second_port, 20);
        assert!(reporter.is_banned(first_port.ip()));
        assert_eq!(reporter.score(first_port.ip()), 0);

        assert_eq!(reporter.score(other_ip.ip()), 40);
        assert!(!reporter.is_banned(other_ip.ip()));
    }

    /// An IPv4-mapped IPv6 report scores and bans the canonical IPv4 address that the
    /// handshake and the address book use.
    #[test]
    fn ipv4_mapped_reports_score_the_canonical_ip() {
        let reporter = reporter();
        let mapped = PeerSocketAddr::from(SocketAddr::new(
            Ipv4Addr::LOCALHOST.to_ipv6_mapped().into(),
            8233,
        ));

        reporter.report(mapped, 1);
        assert_eq!(reporter.score(Ipv4Addr::LOCALHOST.into()), 1);

        reporter.report(mapped, constants::MAX_PEER_MISBEHAVIOR_SCORE);
        assert!(reporter.is_banned(Ipv4Addr::LOCALHOST.into()));
    }

    /// Reports from many threads at once all count: none of them is dropped.
    #[test]
    fn concurrent_reports_are_not_dropped() {
        // 99 reports in total: one short of the ban threshold, which would clear the score.
        const THREADS: u32 = 9;
        const REPORTS_PER_THREAD: u32 = 11;
        const _: () = assert!(THREADS * REPORTS_PER_THREAD < constants::MAX_PEER_MISBEHAVIOR_SCORE);

        let reporter = reporter();
        let addr = peer_addr([127, 0, 0, 1], 8233);

        std::thread::scope(|scope| {
            for _ in 0..THREADS {
                scope.spawn(|| {
                    for _ in 0..REPORTS_PER_THREAD {
                        reporter.report(addr, 1);
                    }
                });
            }
        });

        assert_eq!(reporter.score(addr.ip()), THREADS * REPORTS_PER_THREAD);
    }

    /// The table forgets the IP address scored first once it holds too many.
    #[test]
    fn score_table_is_bounded() {
        let mut table = ScoreTable::default();
        let ip = |index: usize| -> IpAddr {
            let index = u32::try_from(index).expect("the table bound fits in a u32");
            Ipv4Addr::from(index).into()
        };

        for index in 0..=constants::MAX_BANNED_IPS {
            assert!(!table.add(ip(index), 1));
        }

        assert_eq!(table.scores.len(), constants::MAX_BANNED_IPS);
        assert_eq!(table.insertion_order.len(), constants::MAX_BANNED_IPS);
        assert_eq!(table.get(ip(0)), 0, "the first scored IP is forgotten");
        assert_eq!(table.get(ip(constants::MAX_BANNED_IPS)), 1);
    }
}
