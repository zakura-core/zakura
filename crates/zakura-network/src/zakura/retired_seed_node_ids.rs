//! Replaces default seed IDs from p2p-v2/3 with their p2p-v2/5 IDs in
//! `zakura.bootstrap_peers`.
//!
//! A config from p2p-v2/3 names seeds by their Ed25519 public keys. A p2p-v2/5 dial
//! expects the ID derived from the seed's TLS credential.

use std::str::FromStr;

use zakura_quic::NodeId;

use super::ZakuraConfig;

/// Each default seed's protocol 3 node ID, its Ed25519 key, paired with its protocol 5 ID.
const RETIRED_ZAKURA_SEED_NODE_IDS: &[(&str, &str)] = &[
    // asia-0
    (
        "1398f62c6d1a457c51ba6a4b5f3dbd2f69fca93216218dc8997e416bd17d93ca",
        "078a60720f0d474743a51674b152f49f575704a77905c16475683cff9d18f246",
    ),
    // us-0
    (
        "fd1724385aa0c75b64fb78cd602fa1d991fdebf76b13c58ed702eac835e9f618",
        "1bb8f2f70bdbf9d44ef2ccc3de1a5cbbba56cea2cf371e227d501ce19a8bdeb9",
    ),
    // us-east-0
    (
        "9ec67ad6834bc2ca0d659c240e042d3446c37cabcc092b527d459c87d938b4a4",
        "a98d73e0114f9f8ae3d5a0b4ec0b8cef20b51d715de6898a39fc3f23948196b7",
    ),
    // us-west-0
    (
        "bd3dc5d2a3d44c6bf90e364bf446231dbf9737e38a562ccf9e91ea631ea59b22",
        "02ef9b05d81ad2f6176a68fd047bda5e4e277d69ac51b3b2536d43ea8f38de1c",
    ),
    // canada-0
    (
        "14ab98fa0c4b07d40119e1dbc9f3c36d20c8f226ae5ba4216218a2034f148e57",
        "0018a0f4fc1f3cc3b4a84e8ebcb77008343c9e54fb80f77fd03271c7a33063b7",
    ),
    // europe-west-0
    (
        "681d21b18644cd82ec13256a97f92bec1fff815683ef6f65dc7c993f098a4fe5",
        "4607c5353e2c07ecd10d73c6c19d0b0de092939328f33e8f04d5ced382fadf7b",
    ),
    // europe-central-0
    (
        "058b3f20dc9bef7bb447f94d7663d793cfbc036720f97e52d7f13661b21818e1",
        "8ac35544d02141e7a3cc3c8b8661a3e726985a266673b60cf45f5b33c3f36e73",
    ),
    // asia-south-0
    (
        "291323d78eb7186c3fa225ef5e305e95363e0ef06d42dca91bd4ef0254aed1ae",
        "42623a00c2632e0cf99d1c10cec024b0217d12d5fecc91947a3a040f142df06b",
    ),
    // asia-pacific-0
    (
        "85e425233a68697d4be91dd5d542305a8a327cd06d992d53c0913cef2fa75084",
        "ba06d75332e3a430d4e838b5838a2356338124cf2930bb0f8e2962e01223ed95",
    ),
    // zakura-testnet-1
    (
        "57ad39fad4f0bca46cf1ea831772a99d5027b372fef2be5a0ea68e1b5bb4da49",
        "884eb8c4e0d6005bd3a6e086df064efc12b866632dababa4249b2ceb42d43982",
    ),
    // zakura-testnet-eu
    (
        "2bbb907b5d90598ef49f2e637066586b311a64587479be6ed43e8388587fcd2a",
        "1d0dd618bb699637322dcbf20a0830fe9078733ba13c3ee01768bfb518653064",
    ),
    // zakura-testnet-as
    (
        "50999835f48f4a048c0e9042e5332844c9673943d7fab1f7e993bae698c27ea3",
        "b5410aedf93d00bbc9f93405a4c054b30289da9b76269432a5e6e0e132f0480c",
    ),
];

/// Returns, for each of `entries`, the `node_id@direct_addr` bootstrap peers of one list,
/// the entry with a default seed's retired node ID replaced by the seed's current ID, or
/// `None` to keep the entry as it is. The address stays as written, since a node ID names
/// a key rather than an address.
///
/// An entry is kept when the list already names that seed by its current ID. Its
/// addresses would otherwise join the current ID's into one wider bootstrap dial, which
/// startup can refuse; the kept entry only fails its identity check, as it already does.
/// Entries that do not parse are kept.
#[doc(hidden)]
pub fn retired_seed_replacements(entries: &[&str]) -> Vec<Option<String>> {
    let node_id = |entry: &str| NodeId::from_str(entry.split_once('@')?.0).ok();
    let listed: Vec<NodeId> = entries.iter().filter_map(|entry| node_id(entry)).collect();

    entries
        .iter()
        .map(|entry| {
            let id = node_id(entry)?;
            let (_, direct_addr) = entry.split_once('@')?;
            let current = RETIRED_ZAKURA_SEED_NODE_IDS
                .iter()
                .find(|(retired, _)| NodeId::from_str(retired).ok() == Some(id))
                .map(|(_, current)| *current)?;
            let current_id = NodeId::from_str(current).ok()?;
            (!listed.contains(&current_id)).then(|| format!("{current}@{direct_addr}"))
        })
        .collect()
}

impl ZakuraConfig {
    /// Replaces each bootstrap peer that names a default seed by its retired node ID with
    /// the seed's current ID, see [`retired_seed_replacements`].
    ///
    /// Returns the `(old, new)` entries it replaced, in list order.
    pub fn replace_retired_seed_node_ids(&mut self) -> Vec<(String, String)> {
        let entries: Vec<&str> = self.bootstrap_peers.iter().map(String::as_str).collect();
        let replacements = retired_seed_replacements(&entries);
        let mut replaced = Vec::new();
        for (entry, replacement) in self.bootstrap_peers.iter_mut().zip(replacements) {
            if let Some(current) = replacement {
                replaced.push((std::mem::replace(entry, current.clone()), current));
            }
        }
        replaced
    }
}
