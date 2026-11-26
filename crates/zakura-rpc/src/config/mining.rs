//! Mining config

use std::{collections::HashMap, ops::Deref, sync::LazyLock};

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

use zakura_chain::{miner_mesh::GatewayId, parameters::NetworkKind};
use zcash_address::ZcashAddress;
use zcash_transparent::coinbase::{MAX_COINBASE_HEIGHT_LEN, MAX_COINBASE_SCRIPT_LEN};

/// The maximum length of the optional, arbitrary data in the script sig field of
/// a coinbase tx.
pub(crate) const MAX_MINER_DATA_LEN: usize = MAX_COINBASE_SCRIPT_LEN - MAX_COINBASE_HEIGHT_LEN;

/// The marker Zakura prepends to the coinbase input of every block it builds.
///
/// The sakura flower emoji (`U+1F338`), 4 UTF-8 bytes.
pub(crate) const ZAKURA_COINBASE_MARKER: &str = "🌸";

/// Separates [`ZAKURA_COINBASE_MARKER`] from `extra_coinbase_data`.
///
/// Present only when that is set.
pub(crate) const ZAKURA_COINBASE_SEPARATOR: &str = ": ";

/// The maximum length of the user-configurable `extra_coinbase_data`.
///
/// The coinbase data is the marker, separator, and user data in a single push,
/// so the user portion is [`MAX_MINER_DATA_LEN`] minus the marker, separator,
/// and the 2-byte `OP_PUSHDATA1` opcode (for pushes over 75 bytes).
pub(crate) const MAX_USER_COINBASE_DATA_LEN: usize =
    MAX_MINER_DATA_LEN - ZAKURA_COINBASE_MARKER.len() - ZAKURA_COINBASE_SEPARATOR.len() - 2;

/// Mining configuration section.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    /// Address for receiving miner subsidy and tx fees.
    ///
    /// Used in coinbase tx constructed in `getblocktemplate` RPC. Sapling-only
    /// addresses are unsupported. Unified addresses use their Orchard receiver
    /// for Ironwood rewards after NU6.3, or their transparent receiver otherwise.
    #[serde(with = "miner_address")]
    pub miner_address: Option<ZcashAddress>,

    /// Optional tag that Zakura appends to the coinbase input of every block it
    /// builds, after the Zakura `🌸` marker and a `: ` separator.
    ///
    /// Limited to `MAX_USER_COINBASE_DATA_LEN` bytes.
    pub extra_coinbase_data: Option<ExtraCoinbaseData>,

    /// Optional shielded memo that Zakura will include in the output of a shielded coinbase
    /// transaction. Limited to 512 bytes.
    ///
    /// Applies only if [`Self::miner_address`] contains a shielded component.
    pub miner_memo: Option<String>,

    /// Mine blocks using Zakura's internal miner, without an external mining pool or equihash solver.
    ///
    /// Requires a `zakurad` built with the experimental `internal-miner` feature, which solves
    /// Equihash with one low-priority thread. Miners that share a `miner_address` should set
    /// distinct `extra_coinbase_data`, so they do not search identical work.
    ///
    /// The internal miner is off by default.
    #[serde(default)]
    pub internal_miner: bool,

    /// Advertise prepared mined block hashes after expected-work validation and state admission,
    /// but before contextual commit completes.
    pub optimistic_block_inventory: bool,

    /// Name the miner's gateway in the coinbase of every block Zakura builds: this node, or
    /// the node [`Self::miner_mesh_gateways`] names.
    ///
    /// The coinbase gets one extra zero-value output that carries the first 20 bytes of that
    /// node's v2 node ID, so other mining nodes can find it and keep a direct connection to
    /// it. The output takes 34 bytes of the block.
    ///
    /// A node with this set also prefers to stay connected to the gateways that recent blocks
    /// name, with or without a miner address. A relay node that other pools should connect to
    /// sets this without a miner address.
    ///
    /// Needs the v2 P2P stack and a node key that stays the same across restarts: `zakurad`
    /// refuses to start with this set unless it has both. When [`Self::miner_mesh_gateways`]
    /// names another node, this node needs neither.
    ///
    /// Off by default.
    pub miner_mesh: bool,

    /// The gateway the coinbases this node builds name instead of this node, such as the
    /// pool's public relay node.
    ///
    /// Each entry is the gateway's v2 node ID in hex, as its `getminermeshinfo` reports it, or
    /// the first 40 hex digits of it. The coinbase names the gateway by its first 20 bytes.
    ///
    /// The list is empty by default, and then the coinbases name this node. For now it may hold
    /// at most one entry: `zakurad` refuses to start with more. It needs
    /// [`Self::miner_mesh`].
    #[serde(
        serialize_with = "gateway_ids::serialize",
        deserialize_with = "gateway_ids::deserialize_gateways"
    )]
    pub miner_mesh_gateways: Vec<GatewayId>,

    /// The template nodes this relay node keeps connection places for: the pool's internal
    /// nodes whose coinbases name this node through their own [`Self::miner_mesh_gateways`].
    ///
    /// Listed nodes rank ahead of gateways chosen by block count. Within the funded places,
    /// an unambiguous template ID keeps its place even while offline. Once authenticated, it
    /// can use that place when ordinary inbound slots are full. Normal handshake limits apply.
    ///
    /// Each entry is a template node's v2 node ID in hex, as its `getminermeshinfo` reports
    /// it, or the first 40 hex digits of it.
    ///
    /// The list is empty by default. It needs [`Self::miner_mesh`] on a node that is its own
    /// gateway, so [`Self::miner_mesh_gateways`] must be empty; a node with a miner address
    /// may set it too. `zakurad` refuses to start when it lists this node, repeats an entry,
    /// or has more than [`MAX_MINER_MESH_TEMPLATE_NODES`] entries.
    #[serde(
        serialize_with = "gateway_ids::serialize",
        deserialize_with = "gateway_ids::deserialize_template_nodes"
    )]
    pub miner_mesh_template_nodes: Vec<GatewayId>,
}

/// The most template nodes [`Config::miner_mesh_template_nodes`] may list.
///
/// Each takes one of the places a node holds, ahead of the gateways recent blocks name. Four
/// covers a pool's template nodes and leaves at least six of the ten places the default
/// budget funds to those gateways. A smaller peer memory budget, or a larger
/// `network.zakura.max_connections`, funds fewer places, and the template nodes may then take
/// all of them; `zakurad` warns at startup when they do.
pub const MAX_MINER_MESH_TEMPLATE_NODES: usize = 4;

mod miner_address {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use zcash_address::ZcashAddress;

    pub(super) fn serialize<S>(
        address: &Option<ZcashAddress>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        address
            .as_ref()
            .map(ToString::to_string)
            .serialize(serializer)
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<Option<ZcashAddress>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<String>::deserialize(deserializer)?
            .map(|encoded| encoded.parse())
            .transpose()
            .map_err(serde::de::Error::custom)
    }
}

/// Reads and writes lists of gateway IDs as hex strings. This is where the config checks each
/// entry's format.
mod gateway_ids {
    use serde::{de, Deserialize, Deserializer, Serializer};
    use zakura_chain::miner_mesh::GatewayId;

    pub(super) fn serialize<S>(gateways: &[GatewayId], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_seq(gateways.iter().map(ToString::to_string))
    }

    /// Reads `miner_mesh_gateways`.
    pub(super) fn deserialize_gateways<'de, D>(deserializer: D) -> Result<Vec<GatewayId>, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserialize(deserializer, "miner_mesh_gateways")
    }

    /// Reads `miner_mesh_template_nodes`.
    pub(super) fn deserialize_template_nodes<'de, D>(
        deserializer: D,
    ) -> Result<Vec<GatewayId>, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserialize(deserializer, "miner_mesh_template_nodes")
    }

    /// Reads the list of gateway IDs in `field`, naming the field and the entry when an entry
    /// names no gateway ID.
    fn deserialize<'de, D>(deserializer: D, field: &str) -> Result<Vec<GatewayId>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Vec::<String>::deserialize(deserializer)?
            .iter()
            .map(|entry| {
                entry
                    .parse()
                    .map_err(|error| de::Error::custom(format!("{field} entry {entry:?}: {error}")))
            })
            .collect()
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            miner_address: None,
            extra_coinbase_data: None,
            miner_memo: None,
            internal_miner: false,
            optimistic_block_inventory: true,
            miner_mesh: false,
            miner_mesh_gateways: Vec::new(),
            miner_mesh_template_nodes: Vec::new(),
        }
    }
}

impl Config {
    /// Is the internal miner enabled using at least one thread?
    #[cfg(feature = "internal-miner")]
    pub fn is_internal_miner_enabled(&self) -> bool {
        // TODO: Changed to return always false so internal miner is never started. Part of https://github.com/ZcashFoundation/zebra/issues/8180
        // Find the removed code at https://github.com/ZcashFoundation/zebra/blob/v1.5.1/zakura-rpc/src/config/mining.rs#L83
        // Restore the code when conditions are met. https://github.com/ZcashFoundation/zebra/issues/8183
        self.internal_miner
    }
}

/// Operator-configured data appended to the coinbase input of every block Zakura
/// builds, after Zakura's `🌸` marker and `: ` separator.
///
/// Validated on construction to fit within the coinbase data budget, so an
/// oversized value can't be represented, and an oversized
/// `mining.extra_coinbase_data` in the config makes Zakura fail to start.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtraCoinbaseData(String);

impl Deref for ExtraCoinbaseData {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// The error returned when [`ExtraCoinbaseData`] is constructed from too many
/// bytes.
#[derive(Clone, Debug, thiserror::Error)]
#[error("extra_coinbase_data is {0} bytes, but the maximum is {MAX_USER_COINBASE_DATA_LEN}")]
pub struct ExtraCoinbaseDataTooLong(usize);

impl TryFrom<String> for ExtraCoinbaseData {
    type Error = ExtraCoinbaseDataTooLong;

    fn try_from(data: String) -> Result<Self, Self::Error> {
        if data.len() > MAX_USER_COINBASE_DATA_LEN {
            Err(ExtraCoinbaseDataTooLong(data.len()))
        } else {
            Ok(Self(data))
        }
    }
}

impl<'de> Deserialize<'de> for ExtraCoinbaseData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::try_from(String::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

impl Serialize for ExtraCoinbaseData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

/// The desired address type for the `mining.miner_address` field in the config.
#[derive(Eq, PartialEq, Default, Hash)]
pub enum MinerAddressType {
    /// A unified address, containing the components of all the other address types.
    Unified,
    /// A Sapling address.
    Sapling,
    /// A transparent address.
    #[default]
    Transparent,
}

/// Returns the hard-coded default miner address string for a given network and address type.
///
/// All addresses come from a single address:
///
/// - addresses for different networks are only different encodings of the same address;
/// - addresses of different types are components of the same unified address.
pub fn default_miner_address(kind: NetworkKind, addr_type: &MinerAddressType) -> &'static str {
    MINER_ADDRESS[&kind][addr_type]
}

static MINER_ADDRESS: LazyLock<HashMap<NetworkKind, HashMap<MinerAddressType, &'static str>>> =
    LazyLock::new(|| {
        [
    (NetworkKind::Mainnet, [
        (MinerAddressType::Unified, "u1cymdny2u2vllkx7t5jnelp0kde0dgnwu0jzmggzguxvxj6fe7gpuqehywejndlrjwgk9snr6g69azs8jfet78s9zy60uepx6tltk7ee57jlax49dezkhkgvjy2puuue6dvaevt53nah7t2cc2k4p0h0jxmlu9sx58m2xdm5f9sy2n89jdf8llflvtml2ll43e334avu2fwytuna404a"),
        // (MinerAddressType::Orchard, "u1hmfjpqdxaec3mvqypl7fkqcy53u438csydljpuepsfs7jx6sjwyznuzlna8qsslj3tg6sn9ua4q653280aqv4m2fjd4csptwxq3fjpwy"),
        (MinerAddressType::Sapling, "zs1xl84ekz6stprmvrp39s77mf9t953nqjndwlcjtzfrr3cgjjez87639xm4u9pfuvylrhec3uryy5"),
        (MinerAddressType::Transparent, "t1T92bmyoPM7PTSWUnaWnLGcxkF6Jp1AwMY"),
    ].into()),
    (NetworkKind::Testnet, [
        (MinerAddressType::Unified, "utest10a8k6aw5w33kvyt7x6fryzu7vvsjru5vgcfnvr288qx2zm6p63ygcajtaze0px08t583dyrgr42vasazjhhnntus2tqrpkzu0dm2l4cgf3ld6wdqdrf3jv8mvfx9c80e73syer9l2wlgawjtf7yvj0eqwdf354trtelxnr0fhpw9792eaf49ghstkyftc9lwqqwy4ye0cleagp4nzyt"),
        // (MinerAddressType::Orchard, "utest10zg6frxk32ma8980kdv9473e4aclw7clq9hydzcj6l349pkqzxk2mmj3cn7j5x38w6l4wyryv50whnlrw0k9agzpdf5fxyj7kq96ukcp"),
        (MinerAddressType::Sapling, "ztestsapling1xl84ekz6stprmvrp39s77mf9t953nqjndwlcjtzfrr3cgjjez87639xm4u9pfuvylrhecet38rq"),
        (MinerAddressType::Transparent, "tmJymvcUCn1ctbghvTJpXBwHiMEB8P6wxNV"),
    ].into()),
    (NetworkKind::Regtest, [
        (MinerAddressType::Unified, "uregtest1efxggx6lduhm2fx5lnrhxv7h7kpztlpa3ahf3n4w0q0zj5epj4av9xjq6ljsja3xk8z7rzd067kc7mgpy9448rdfzpfjz5gq389zdmpgnk6rp4ykk0xk6cmqw6zqcrnmsuaxv3yzsvcwsd4gagtalh0uzrdvy03nhmltjz2eu0232qlcs0zvxuqyut73yucd9gy5jaudnyt7yqhgpqv"),
        // (MinerAddressType::Orchard, "uregtest1pszqlgxaf5w8mu2yd9uygg8cswp0ec4f7eejqnqc35tztw4tk0sxnt3pym2f3s2872cy2ruuc5n8y9cen5q6ngzlmzu8ztrjesv8zm9j"),
        (MinerAddressType::Sapling, "zregtestsapling1xl84ekz6stprmvrp39s77mf9t953nqjndwlcjtzfrr3cgjjez87639xm4u9pfuvylrhecx0c2j8"),
        (MinerAddressType::Transparent, "tmJymvcUCn1ctbghvTJpXBwHiMEB8P6wxNV"),
    ].into()),
].into()
    });

#[cfg(test)]
mod tests {
    use zakura_chain::miner_mesh::GatewayId;

    use super::Config;

    /// A v2 node ID, and the gateway ID it starts with.
    const NODE_ID: &str = "4d70dccee88cea83ef71f9f870489b10f5c6c112d9c49725dc38d2979f6db690";
    const GATEWAY_ID: &str = "4d70dccee88cea83ef71f9f870489b10f5c6c112";

    #[test]
    fn a_gateway_reads_from_a_node_id_or_a_gateway_id_and_writes_as_a_gateway_id() {
        let expected: GatewayId = GATEWAY_ID.parse().unwrap();
        for entry in [NODE_ID, GATEWAY_ID] {
            let config: Config =
                toml::from_str(&format!("miner_mesh_gateways = [{entry:?}]")).unwrap();
            assert_eq!(config.miner_mesh_gateways, vec![expected]);

            let serialized = toml::to_string(&config).unwrap();
            assert!(serialized.contains(&format!("miner_mesh_gateways = [\"{GATEWAY_ID}\"]")));
            assert_eq!(toml::from_str::<Config>(&serialized).unwrap(), config);
        }
    }

    #[test]
    fn a_gateway_entry_of_the_wrong_length_or_not_hex_fails_with_the_entry_named() {
        for entry in [
            &NODE_ID[..63],
            &GATEWAY_ID[..38],
            "",
            &format!("{}zz", &NODE_ID[2..]),
        ] {
            let error = toml::from_str::<Config>(&format!("miner_mesh_gateways = [{entry:?}]"))
                .expect_err("the entry names no gateway");
            assert!(
                error
                    .to_string()
                    .contains(&format!("miner_mesh_gateways entry {entry:?}")),
                "unexpected error: {error}"
            );
        }
        assert!(toml::from_str::<Config>(&format!(
            "miner_mesh_gateways = [{GATEWAY_ID:?}, \"nothex\"]"
        ))
        .is_err());
    }

    #[test]
    fn no_gateway_is_named_by_default() {
        assert!(Config::default().miner_mesh_gateways.is_empty());
        let config: Config = toml::from_str("").unwrap();
        assert!(config.miner_mesh_gateways.is_empty());
    }

    #[test]
    fn a_template_node_reads_like_a_gateway_and_writes_as_a_gateway_id() {
        let expected: GatewayId = GATEWAY_ID.parse().unwrap();
        for entry in [NODE_ID, GATEWAY_ID] {
            let config: Config =
                toml::from_str(&format!("miner_mesh_template_nodes = [{entry:?}]")).unwrap();
            assert_eq!(config.miner_mesh_template_nodes, vec![expected]);
            assert!(config.miner_mesh_gateways.is_empty());

            let serialized = toml::to_string(&config).unwrap();
            assert!(serialized.contains(&format!("miner_mesh_template_nodes = [\"{GATEWAY_ID}\"]")));
            assert_eq!(toml::from_str::<Config>(&serialized).unwrap(), config);
        }
    }

    #[test]
    fn a_bad_template_node_entry_fails_with_the_setting_and_entry_named() {
        let entry = &NODE_ID[..63];
        let error = toml::from_str::<Config>(&format!("miner_mesh_template_nodes = [{entry:?}]"))
            .expect_err("the entry names no node");
        assert!(
            error
                .to_string()
                .contains(&format!("miner_mesh_template_nodes entry {entry:?}")),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn no_template_node_is_listed_by_default() {
        assert!(Config::default().miner_mesh_template_nodes.is_empty());
        let config: Config = toml::from_str("").unwrap();
        assert!(config.miner_mesh_template_nodes.is_empty());
    }

    #[test]
    fn miner_address_preserves_string_encoding_and_optional_defaults() {
        let encoded = "t1bmMa1wJDFdbc2TiURQP5BbBz6jHjUBuHq";
        let config: Config = toml::from_str(&format!("miner_address = {encoded:?}")).unwrap();
        assert_eq!(config.miner_address.as_ref().unwrap().to_string(), encoded);
        let serialized = toml::to_string(&config).unwrap();
        assert_eq!(toml::from_str::<Config>(&serialized).unwrap(), config);
        assert_eq!(
            serde_json::to_value(&config).unwrap()["miner_address"],
            encoded
        );

        for json in [
            serde_json::json!({}),
            serde_json::json!({"miner_address": null}),
        ] {
            assert!(serde_json::from_value::<Config>(json)
                .unwrap()
                .miner_address
                .is_none());
        }
        assert!(toml::from_str::<Config>("miner_address = 'invalid'").is_err());
        assert!(toml::from_str::<Config>("miner_address = 42").is_err());
    }
}
