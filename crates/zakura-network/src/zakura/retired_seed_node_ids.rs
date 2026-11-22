//! The default seeds' node IDs from before p2p-v2/4, and their replacement in
//! `zakura.bootstrap_peers`.
//!
//! p2p-v2/4 derives node IDs from the TLS credential, so every seed's ID changed, and a
//! config written before then names the seeds by IDs that now fail the identity check.
//! Drop this module once releases before p2p-v2/4 are no longer supported.

use std::str::FromStr;

use zakura_quic::NodeId;

use super::ZakuraConfig;

/// Each default seed's protocol 3 node ID, its Ed25519 key, paired with its current ID.
pub(crate) const RETIRED_ZAKURA_SEED_NODE_IDS: &[(&str, &str)] = &[
    // asia-0
    (
        "1398f62c6d1a457c51ba6a4b5f3dbd2f69fca93216218dc8997e416bd17d93ca",
        "a5f148b592404c5fcdc27f06919681c1dceddb274336abf5c91e70780471bc12",
    ),
    // us-0
    (
        "fd1724385aa0c75b64fb78cd602fa1d991fdebf76b13c58ed702eac835e9f618",
        "10e8b19b6aabb5ccd80d2df14fabc991602aea0cfb82783ebdabb3b1b699e9b7",
    ),
    // us-east-0
    (
        "9ec67ad6834bc2ca0d659c240e042d3446c37cabcc092b527d459c87d938b4a4",
        "ca22584f4392a26c46f9553e45db00fc8de06f1ff8f2c48f5d28b5cd58f134b6",
    ),
    // us-west-0
    (
        "bd3dc5d2a3d44c6bf90e364bf446231dbf9737e38a562ccf9e91ea631ea59b22",
        "27fec1ed7f52ca389f8ad09e79e896fe6f0097dea6d74b71628dce992cb50b57",
    ),
    // canada-0
    (
        "14ab98fa0c4b07d40119e1dbc9f3c36d20c8f226ae5ba4216218a2034f148e57",
        "a4ef400514ea1eaceea73e2b1a86f4102264e88cfc8b3ace9da73d43c7ccb7c2",
    ),
    // europe-west-0
    (
        "681d21b18644cd82ec13256a97f92bec1fff815683ef6f65dc7c993f098a4fe5",
        "9addeca7e4ea8aced48fcf9e3a9f8ce21056fecf9c0bbce368b61ec35b4e5bdc",
    ),
    // europe-central-0
    (
        "058b3f20dc9bef7bb447f94d7663d793cfbc036720f97e52d7f13661b21818e1",
        "5f0cc2933a058472140348ebe708ff7a10167cd7e9cd96fa19263ecdf62a3fbe",
    ),
    // asia-south-0
    (
        "291323d78eb7186c3fa225ef5e305e95363e0ef06d42dca91bd4ef0254aed1ae",
        "6a13975563f5883fda0b8b0b1cee54163dedf786dc8f1b4990d2130e7cc56b51",
    ),
    // asia-pacific-0
    (
        "85e425233a68697d4be91dd5d542305a8a327cd06d992d53c0913cef2fa75084",
        "a8a2de1c199100dcd1e7985a30a6a07858d45ee3703794d9d214827724067431",
    ),
    // zakura-testnet-1
    (
        "57ad39fad4f0bca46cf1ea831772a99d5027b372fef2be5a0ea68e1b5bb4da49",
        "672cb8d35831d6020f14e35215de0f7fc1ee771ecd3c9d5fe67c32acb0b53fab",
    ),
    // zakura-testnet-eu
    (
        "2bbb907b5d90598ef49f2e637066586b311a64587479be6ed43e8388587fcd2a",
        "3783033db00b137ed3d11cf2d30853fce65db4967d3910bb66ff4d14191764c9",
    ),
    // zakura-testnet-as
    (
        "50999835f48f4a048c0e9042e5332844c9673943d7fab1f7e993bae698c27ea3",
        "fe1f7911e1675f0f3cfb17033a71ff2221d03d9f170b0209a73deba439886efe",
    ),
];

/// Returns `entry`, a `node_id@direct_addr` bootstrap peer, with a default seed's retired
/// node ID replaced by its current one. The address stays as written, since a node ID
/// names a key rather than an address.
///
/// Returns `None` for any other entry, including one that does not parse.
#[doc(hidden)]
pub fn replace_retired_seed_node_id(entry: &str) -> Option<String> {
    let (node_id, direct_addr) = entry.split_once('@')?;
    let node_id = NodeId::from_str(node_id).ok()?;
    RETIRED_ZAKURA_SEED_NODE_IDS
        .iter()
        .find(|(retired, _)| NodeId::from_str(retired).ok() == Some(node_id))
        .map(|(_, current)| format!("{current}@{direct_addr}"))
}

impl ZakuraConfig {
    /// Replaces each bootstrap peer that names a default seed by its retired node ID with
    /// the seed's current ID, see [`replace_retired_seed_node_id`].
    ///
    /// Returns the `(old, new)` entries it replaced, in list order.
    pub fn replace_retired_seed_node_ids(&mut self) -> Vec<(String, String)> {
        let mut replaced = Vec::new();
        for entry in &mut self.bootstrap_peers {
            if let Some(current) = replace_retired_seed_node_id(entry) {
                replaced.push((std::mem::replace(entry, current.clone()), current));
            }
        }
        replaced
    }
}
