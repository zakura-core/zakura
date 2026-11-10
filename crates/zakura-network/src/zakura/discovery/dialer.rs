//! Bootstrap and candidate dial entry points for native discovery.

use std::{net::SocketAddr, str::FromStr};

use zakura_node_services::task::spawn_erased_named;
use zakura_quic::{NodeAddr, NodeId};
use zakura_rt::{
    collections::HashMap,
    sync::{oneshot, CancellationToken},
    task::JoinHandle,
};

use super::{native_dial_supervised, RedialPolicy};
use crate::zakura::{
    handler::{DialRegistration, DialReport},
    ZakuraEndpoint, ZakuraHandlerError, ZakuraLocalLimits, DEFAULT_ZAKURA_REDIAL_INITIAL_BACKOFF,
    DEFAULT_ZAKURA_REDIAL_MAX_BACKOFF,
};

/// Spawn supervised dials for configured native bootstrap peers.
///
/// Returns one task per unique remote identity after merging its configured addresses and removing
/// the local identity, so the caller can track each maintained dial under the endpoint shutdown
/// owner. Every dial also observes the shutdown token directly via [`native_dial_supervised`].
pub(crate) fn spawn_native_bootstrap_dialer(
    endpoint: ZakuraEndpoint,
    bootstrap_peers: Vec<String>,
    limits: ZakuraLocalLimits,
) -> Vec<JoinHandle<()>> {
    if bootstrap_peers.is_empty() {
        return Vec::new();
    }

    // Configured bootstrap peers are maintained: keep re-dialing forever so a
    // node whose only peers are over Zakura (`p2p_stack = "zakura"`) tolerates the
    // seed not being up yet at startup and recovers when a peer later drops. The
    // legacy crawler is absent on such a node, so this loop is the only healing
    // path for its seeds.
    let policy = RedialPolicy::maintain(
        DEFAULT_ZAKURA_REDIAL_INITIAL_BACKOFF,
        DEFAULT_ZAKURA_REDIAL_MAX_BACKOFF,
    );

    let node_addrs = grouped_remote_bootstrap_peers(bootstrap_peers, endpoint.local_node_id());
    let mut tasks = Vec::with_capacity(node_addrs.len());
    for node_addr in node_addrs {
        let endpoint = endpoint.clone();
        let limits = limits.clone();
        tasks.push(spawn_erased_named("bootstrap_dial", async move {
            native_dial_supervised(
                endpoint,
                node_addr,
                limits,
                policy,
                CancellationToken::new(),
            )
            .await;
        }));
    }
    tasks
}

/// Refuses a remote bootstrap identity, grouped as the bootstrap dialer groups
/// them, whose dial would make more address attempts than `dial_reserve`.
/// `attempts` counts the attempts a dial to an address set makes.
///
/// A maintained dial claims a transport slot for each attempt at once
/// (zakura-quic ADM-12), so a dial needing more could never start while
/// inbound peers and discovery fill every slot outside the reserve.
pub(crate) fn check_bootstrap_dials_fit(
    bootstrap_peers: &[String],
    local_node_id: NodeId,
    dial_reserve: usize,
    attempts: impl Fn(&NodeAddr) -> usize,
) -> Result<(), String> {
    for node_addr in grouped_remote_bootstrap_peers(bootstrap_peers.to_vec(), local_node_id) {
        let attempts = attempts(&node_addr);
        if attempts > dial_reserve {
            return Err(format!(
                "zakura.bootstrap_peers gives node {} {attempts} addresses this node can dial, \
                 but its connection limits reserve {dial_reserve} dial slots, so its \
                 maintained dial could fail whenever inbound peers fill the table; list at \
                 most {dial_reserve} for it or raise max_pending_handshakes",
                node_addr.id,
            ));
        }
    }
    Ok(())
}

fn grouped_remote_bootstrap_peers(
    bootstrap_peers: Vec<String>,
    local_node_id: NodeId,
) -> Vec<NodeAddr> {
    let mut direct_addrs_by_node = HashMap::<NodeId, Vec<SocketAddr>>::new();
    for entry in bootstrap_peers {
        let node_addr = match parse_bootstrap_peer(&entry) {
            Ok(node_addr) => node_addr,
            Err(error) => {
                tracing::warn!(?error, ?entry, "invalid Zakura bootstrap peer");
                continue;
            }
        };
        if node_addr.id == local_node_id {
            tracing::debug!(?entry, "ignoring local Zakura bootstrap peer");
            continue;
        }
        direct_addrs_by_node
            .entry(node_addr.id)
            .or_default()
            .extend(node_addr.direct);
    }

    direct_addrs_by_node
        .into_iter()
        .map(|(node_id, mut direct_addrs)| {
            direct_addrs.sort_unstable();
            direct_addrs.dedup();
            NodeAddr::with_addrs(node_id, direct_addrs)
        })
        .collect()
}

/// Dials and serves a maintained connection, and reports on `registration`
/// how its registration ended.
pub(crate) async fn native_bootstrap_dial(
    endpoint: &ZakuraEndpoint,
    node_addr: NodeAddr,
    limits: &ZakuraLocalLimits,
    registration: oneshot::Sender<DialRegistration>,
) -> Result<(), ZakuraHandlerError> {
    // Only discovery uses the admitted address, to settle per-IP reservations
    // and confirm the record address the dial reached.
    let (admitted_addr, _) = oneshot::channel();
    let report = DialReport {
        admitted_addr,
        registration,
    };
    crate::zakura::handler::serve_reported_native_dial_connection(
        endpoint,
        node_addr,
        limits,
        Some(report),
        crate::zakura::handler::NativeDialKind::Maintained,
    )
    .await
}

/// Dials a discovery candidate and reports on `report` the address whose
/// handshake TLS authenticated and how registration ended.
pub(crate) async fn native_discovery_dial(
    endpoint: &ZakuraEndpoint,
    node_addr: NodeAddr,
    limits: &ZakuraLocalLimits,
    report: DialReport,
) -> Result<(), ZakuraHandlerError> {
    crate::zakura::handler::serve_reported_native_dial_connection(
        endpoint,
        node_addr,
        limits,
        Some(report),
        crate::zakura::handler::NativeDialKind::Discovery,
    )
    .await
}

pub(crate) fn parse_bootstrap_peer(entry: &str) -> Result<NodeAddr, ZakuraHandlerError> {
    let Some((node_id, direct_addr)) = entry.split_once('@') else {
        return Err(ZakuraHandlerError::InvalidBootstrapPeer);
    };
    let node_id =
        NodeId::from_str(node_id).map_err(|_| ZakuraHandlerError::InvalidBootstrapPeer)?;
    let direct_addr = direct_addr
        .parse::<SocketAddr>()
        .map_err(|_| ZakuraHandlerError::InvalidBootstrapPeer)?;
    Ok(NodeAddr::with_addrs(node_id, [direct_addr]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use crate::{BoxError, Config};

    use crate::zakura::{DEFAULT_TESTNET_ZAKURA_BOOTSTRAP_PEERS, DEFAULT_ZAKURA_BOOTSTRAP_PEERS};

    /// Startup refuses a remote bootstrap identity whose distinct addresses
    /// outnumber the dial reserve its limits give, at the defaults and at
    /// small caps, and ignores the local identity.
    #[test]
    fn bootstrap_dials_must_fit_the_dial_reserve() {
        let remote = zakura_quic::NodeSecretKey::generate().public();
        let local = zakura_quic::NodeSecretKey::generate().public();
        let peers = |node_id: NodeId, count: u16| {
            (0..count)
                .map(|port| format!("{node_id}@127.0.0.1:{}", 8233 + port))
                // A repeated address counts once.
                .chain([format!("{node_id}@127.0.0.1:8233")])
                .collect::<Vec<_>>()
        };
        let reserve = |connections: usize, pending: usize| {
            let mut config = Config::default();
            config.zakura.max_connections = connections;
            config.zakura.max_pending_handshakes = pending;
            ZakuraLocalLimits::from_config(&config)
                .quic_bind_config(Vec::new())
                .dial_reserve
        };

        // Every distinct address is dialable here; the transport's own count
        // is `QuicEndpoint::dial_attempt_count`.
        let every = |node_addr: &NodeAddr| node_addr.direct.len();

        let defaults = reserve(92, 32);
        assert_eq!(defaults, 8);
        for list in [
            DEFAULT_ZAKURA_BOOTSTRAP_PEERS,
            DEFAULT_TESTNET_ZAKURA_BOOTSTRAP_PEERS,
        ] {
            let list = list.iter().map(|peer| peer.to_string()).collect::<Vec<_>>();
            check_bootstrap_dials_fit(&list, local, 1, every)
                .expect("default peers list one address each");
        }
        check_bootstrap_dials_fit(&peers(remote, 8), local, defaults, every)
            .expect("a full record fits the default reserve");
        check_bootstrap_dials_fit(&peers(remote, 9), local, defaults, every)
            .expect_err("a ninth address can't fit the default reserve");

        let small = reserve(16, 4);
        assert_eq!(small, 5);
        check_bootstrap_dials_fit(&peers(remote, 5), local, small, every)
            .expect("five addresses fit");
        let error = check_bootstrap_dials_fit(&peers(remote, 8), local, small, every)
            .expect_err("a full record can't fit a five-slot reserve");
        assert!(error.contains("zakura.bootstrap_peers"), "{error}");

        check_bootstrap_dials_fit(&peers(local, 9), local, small, every)
            .expect("the local identity is never dialed");
        check_bootstrap_dials_fit(&peers(remote, 8), local, small, |_| small)
            .expect("only the addresses the transport would dial count");
    }

    /// The discovery caller uses the lower transport ceiling and reports a
    /// local limit when a small configuration has room only for inbound peers.
    #[tokio::test]
    async fn discovery_dial_uses_the_inbound_preserving_limit() -> Result<(), BoxError> {
        use crate::zakura::testkit::ZakuraTestNode;
        use crate::zakura::ZakuraLocalLimits;

        tokio::time::timeout(Duration::from_secs(10), async {
            let limits = ZakuraLocalLimits {
                max_connections: 1,
                max_pending_handshakes: 1,
                ..ZakuraLocalLimits::from_config(&Config::default())
            };
            let node = ZakuraTestNode::builder(151_508)
                .limits(limits.clone())
                .spawn()
                .await?;
            let silent = std::net::UdpSocket::bind("127.0.0.1:0")?;
            let target = NodeAddr::with_addrs(
                zakura_quic::NodeSecretKey::generate().public(),
                [silent.local_addr()?],
            );
            let endpoint = node.endpoint();
            assert_eq!(endpoint.discovery_admission_capacity(), 0);
            let (admitted_addr, _admitted_rx) = tokio::sync::oneshot::channel();
            let (registration, _registration_rx) = tokio::sync::oneshot::channel();
            let report = DialReport {
                admitted_addr,
                registration,
            };
            let result = tokio::time::timeout(
                Duration::from_millis(200),
                native_discovery_dial(&endpoint, target, &limits, report),
            )
            .await?;
            assert!(matches!(result, Err(ref error) if error.is_pre_dial_limit()));
            node.shutdown().await;
            Ok::<_, BoxError>(())
        })
        .await?
    }

    #[test]
    fn bootstrap_peer_requires_node_id_and_direct_address() {
        assert!(parse_bootstrap_peer("missing-address").is_err());
        assert!(parse_bootstrap_peer("not-a-node@127.0.0.1:8233").is_err());
    }

    #[test]
    fn default_bootstrap_peers_parse() {
        for peer in DEFAULT_ZAKURA_BOOTSTRAP_PEERS
            .iter()
            .chain(DEFAULT_TESTNET_ZAKURA_BOOTSTRAP_PEERS)
        {
            parse_bootstrap_peer(peer).expect("default Zakura bootstrap peer should parse");
        }
    }

    #[test]
    fn bootstrap_peers_skip_self_and_merge_duplicate_remote_identities() {
        let local_id = zakura_quic::NodeSecretKey::from_bytes(&[1; 32]).public();
        let remote_id = zakura_quic::NodeSecretKey::from_bytes(&[2; 32]).public();
        let first_addr: SocketAddr = "127.0.0.1:8234".parse().expect("test address parses");
        let second_addr: SocketAddr = "127.0.0.1:8235".parse().expect("test address parses");
        let grouped = grouped_remote_bootstrap_peers(
            vec![
                format!("{local_id}@{first_addr}"),
                format!("{remote_id}@{first_addr}"),
                format!("{remote_id}@{second_addr}"),
                format!("{remote_id}@{second_addr}"),
            ],
            local_id,
        );

        assert_eq!(grouped.len(), 1);
        assert_eq!(grouped[0].id, remote_id);
        assert_eq!(grouped[0].direct, vec![first_addr, second_addr]);
    }
}
