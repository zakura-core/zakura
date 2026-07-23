//! Bootstrap and candidate dial entry points for native discovery.

use std::{collections::HashMap, net::SocketAddr, str::FromStr};

use iroh::{NodeAddr, NodeId};

use super::{native_dial_supervised, RedialPolicy};
use crate::zakura::{
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
) -> Vec<tokio::task::JoinHandle<()>> {
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
        tasks.push(tokio::spawn(async move {
            native_dial_supervised(endpoint, node_addr, limits, policy).await
        }));
    }
    tasks
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
        if node_addr.node_id == local_node_id {
            tracing::debug!(?entry, "ignoring local Zakura bootstrap peer");
            continue;
        }
        direct_addrs_by_node
            .entry(node_addr.node_id)
            .or_default()
            .extend(node_addr.direct_addresses().copied());
    }

    direct_addrs_by_node
        .into_iter()
        .map(|(node_id, mut direct_addrs)| {
            direct_addrs.sort_unstable();
            direct_addrs.dedup();
            NodeAddr::new(node_id).with_direct_addresses(direct_addrs)
        })
        .collect()
}

pub(crate) async fn native_bootstrap_dial(
    endpoint: &ZakuraEndpoint,
    node_addr: NodeAddr,
    limits: &ZakuraLocalLimits,
) -> Result<(), ZakuraHandlerError> {
    crate::zakura::handler::serve_native_dial_connection(endpoint, node_addr, limits).await
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
    Ok(NodeAddr::new(node_id).with_direct_addresses([direct_addr]))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::zakura::{DEFAULT_TESTNET_ZAKURA_BOOTSTRAP_PEERS, DEFAULT_ZAKURA_BOOTSTRAP_PEERS};

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
        let local_id = iroh::SecretKey::from_bytes(&[1; 32]).public();
        let remote_id = iroh::SecretKey::from_bytes(&[2; 32]).public();
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
        assert_eq!(grouped[0].node_id, remote_id);
        assert_eq!(
            grouped[0].direct_addresses().copied().collect::<Vec<_>>(),
            vec![first_addr, second_addr]
        );
    }
}
