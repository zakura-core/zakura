//! JSON Schemas for the RPC parameter and result types.
//!
//! Each schema describes the type's serde wire format, so a change to a type's
//! fields or serde attributes must update its schema here. The descriptions
//! repeat the types' doc comments. The OpenRPC fixture test catches changes to
//! the rendered schemas.

use serde_json::{json, Value};

use zakura_chain::subtree::NoteCommitmentSubtreeIndex;
use zakura_network::PeerSocketAddr;

use super::{Generator, RpcSchema};
use crate::methods::{
    hex_data::HexData,
    types::{
        get_block_template::parameters::{
            GetBlockTemplateCapability, GetBlockTemplateParameters, GetBlockTemplateRequestMode,
        },
        long_poll::LongPollId,
        submit_block::{SubmitBlockParameters, SubmitSolutionParameters},
    },
    AddNodeCommand, GetAddressBalanceRequest, GetAddressTxIdsRequest, GetAddressUtxosRequest,
};

/// Implements [`RpcSchema`] for types with a fixed inline schema.
macro_rules! inline_schema {
    ($($ty:ty => $schema:tt,)*) => {
        $(
            impl RpcSchema for $ty {
                fn schema(_: &mut Generator) -> Value {
                    json!($schema)
                }
            }
        )*
    };
}

/// Implements [`RpcSchema`] for a component type with a fixed schema.
macro_rules! component_schema {
    ($($ty:ty => $schema:tt,)*) => {
        $(
            impl RpcSchema for $ty {
                const COMPONENT: Option<&'static str> = Some(stringify!($ty));

                fn schema(_: &mut Generator) -> Value {
                    json!($schema)
                }
            }
        )*
    };
}

inline_schema! {
    () => { "type": "null" },
    bool => { "type": "boolean" },
    String => { "type": "string" },
    u8 => { "type": "integer", "format": "uint8", "minimum": 0, "maximum": 255 },
    u32 => { "type": "integer", "format": "uint32", "minimum": 0 },
    i32 => { "type": "integer", "format": "int32" },
}

impl<T: RpcSchema> RpcSchema for Vec<T> {
    fn schema(generator: &mut Generator) -> Value {
        json!({ "type": "array", "items": generator.subschema_for::<T>() })
    }
}

/// Serde writes `None` as `null`.
impl<T: RpcSchema> RpcSchema for Option<T> {
    fn schema(generator: &mut Generator) -> Value {
        let mut schema = generator.subschema_for::<T>();
        match schema.get_mut("type") {
            Some(Value::String(ty)) => {
                let ty = Value::String(std::mem::take(ty));
                schema["type"] = json!([ty, "null"]);
                schema
            }
            _ => json!({ "anyOf": [schema, { "type": "null" }] }),
        }
    }
}

/// Returns `T`'s schema with `description` added.
fn described<T: RpcSchema>(generator: &mut Generator, description: &str) -> Value {
    let mut schema = generator.subschema_for::<T>();
    schema["description"] = description.into();
    schema
}

/// Returns the schema of a unit-only enum serialized as variant name strings.
fn string_enum(description: &str, variants: &[(&str, &str)]) -> Value {
    let variants: Vec<_> = variants
        .iter()
        .map(|(value, description)| {
            json!({ "type": "string", "const": value, "description": description })
        })
        .collect();
    json!({ "description": description, "oneOf": variants })
}

component_schema! {
    HexData => {
        "description": "Deserialize hex-encoded strings to bytes.",
        "type": "string",
    },
    LongPollId => {
        "description": "The encoded long poll ID, generated from the [`LongPollInput`].\n\n\
            `zcashd` IDs are currently 69 hex/decimal digits long.\n\
            IDs use 46 hex/decimal digits, or 62 after the first template withdrawal.",
        "type": "string",
    },
    NoteCommitmentSubtreeIndex => {
        "description": "A note commitment subtree index, used to identify a subtree in a shielded pool.\n\
            Also used to count subtrees.",
        "type": "integer",
        "format": "uint16",
        "minimum": 0,
        "maximum": 65535,
    },
    PeerSocketAddr => {
        "description": "A thin wrapper for [`SocketAddr`] which hides peer IP addresses by default.",
        "type": "string",
    },
}

impl RpcSchema for AddNodeCommand {
    const COMPONENT: Option<&'static str> = Some("AddNodeCommand");

    fn schema(_: &mut Generator) -> Value {
        string_enum(
            "Commands for the `addnode` RPC method.",
            &[("add", "Add a node to the address book.")],
        )
    }
}

impl RpcSchema for GetBlockTemplateRequestMode {
    const COMPONENT: Option<&'static str> = Some("GetBlockTemplateRequestMode");

    fn schema(_: &mut Generator) -> Value {
        string_enum(
            "Defines whether the RPC method should generate a block template or attempt to \
             validate a block\nproposal.",
            &[
                ("template", "Indicates a request for a block template."),
                ("proposal", "Indicates a request to validate block data."),
            ],
        )
    }
}

impl RpcSchema for GetBlockTemplateCapability {
    const COMPONENT: Option<&'static str> = Some("GetBlockTemplateCapability");

    fn schema(_: &mut Generator) -> Value {
        const IGNORED: &str = "Currently ignored by zcashd and zebra.";
        string_enum(
            "Valid `capabilities` values that indicate client-side support.",
            &[
                (
                    "longpoll",
                    "Long Polling support.\nCurrently ignored by zebra.",
                ),
                (
                    "coinbasetxn",
                    "Information for coinbase transaction, default template data with the \
                     `coinbasetxn` field.\nCurrently ignored by zebra.",
                ),
                (
                    "coinbasevalue",
                    "Coinbase value, template response provides a `coinbasevalue` field and \
                     omits `coinbasetxn` field.\nCurrently ignored by zebra.",
                ),
                (
                    "coinbaseaux",
                    "Components of the coinbase transaction.\nCurrently ignored by zebra.",
                ),
                ("proposal", IGNORED),
                ("serverlist", IGNORED),
                ("workid", IGNORED),
                (
                    "unknowncapability",
                    "Unknown capability to fill in for mutations.",
                ),
            ],
        )
    }
}

/// Deserialized from a single address string or an object with an address list.
impl RpcSchema for GetAddressBalanceRequest {
    const COMPONENT: Option<&'static str> = Some("GetAddressBalanceRequest");

    fn schema(generator: &mut Generator) -> Value {
        json!({
            "description": "A request for [`RpcServer::get_address_balance`].",
            "anyOf": [
                {
                    "description": "A list of address strings.",
                    "type": "object",
                    "properties": { "addresses": generator.subschema_for::<Vec<String>>() },
                    "required": ["addresses"],
                },
                { "description": "A single address string.", "type": "string" },
            ],
        })
    }
}

/// Deserialized from a single address string or an object with an address list
/// and optional height range.
impl RpcSchema for GetAddressTxIdsRequest {
    const COMPONENT: Option<&'static str> = Some("GetAddressTxIdsRequest");

    fn schema(generator: &mut Generator) -> Value {
        json!({
            "description": "Parameters of [`RpcServer::get_address_tx_ids`] RPC method.\n\n\
                See [`RpcServer::get_address_tx_ids`] for more details.",
            "anyOf": [
                { "description": "A single address string.", "type": "string" },
                {
                    "description": "A full request object with address list and optional height range.",
                    "type": "object",
                    "properties": {
                        "addresses": described::<Vec<String>>(
                            generator,
                            "A list of addresses to get transactions from.",
                        ),
                        "start": described::<Option<u32>>(
                            generator,
                            "The height to start looking for transactions.",
                        ),
                        "end": described::<Option<u32>>(
                            generator,
                            "The height to end looking for transactions.",
                        ),
                    },
                    "required": ["addresses"],
                },
            ],
        })
    }
}

/// Deserialized from a single address string or an object with an address list
/// and `chainInfo` flag.
impl RpcSchema for GetAddressUtxosRequest {
    const COMPONENT: Option<&'static str> = Some("GetAddressUtxosRequest");

    fn schema(generator: &mut Generator) -> Value {
        let mut chain_info =
            described::<bool>(generator, "The height to start looking for transactions.");
        chain_info["default"] = false.into();
        json!({
            "description": "Parameters of [`RpcServer::get_address_utxos`] RPC method.",
            "anyOf": [
                { "description": "A single address string.", "type": "string" },
                {
                    "description": "A full request object with address list and chainInfo flag.",
                    "type": "object",
                    "properties": {
                        "addresses": described::<Vec<String>>(
                            generator,
                            "A list of addresses to get transactions from.",
                        ),
                        "chainInfo": chain_info,
                    },
                    "required": ["addresses"],
                },
            ],
        })
    }
}

impl RpcSchema for GetBlockTemplateParameters {
    const COMPONENT: Option<&'static str> = Some("GetBlockTemplateParameters");

    fn schema(generator: &mut Generator) -> Value {
        let mut mode = described::<GetBlockTemplateRequestMode>(
            generator,
            "Defines whether the RPC method should generate a block template or attempt to\n\
             validate block data, checking against all of the server's usual acceptance rules\n\
             (excluding the check for a valid proof-of-work).",
        );
        mode["default"] = "template".into();
        json!({
            "description": "Optional parameter `jsonrequestobject` for `getblocktemplate` RPC request.\n\n\
                The `data` field must be provided in `proposal` mode, and must be omitted in \
                `template` mode.\nAll other fields are optional.",
            "type": "object",
            "properties": {
                "mode": mode,
                "data": described::<Option<HexData>>(
                    generator,
                    "Must be omitted when `getblocktemplate` RPC is called in \"template\" mode \
                     (or when `mode` is omitted).\nMust be provided when `getblocktemplate` RPC \
                     is called in \"proposal\" mode.\n\nHex-encoded block data to be validated \
                     and checked against the server's usual acceptance rules\n(excluding the \
                     check for a valid proof-of-work).",
                ),
                "capabilities": described::<Vec<GetBlockTemplateCapability>>(
                    generator,
                    "A list of client-side supported capability features",
                ),
                "longpollid": described::<Option<LongPollId>>(
                    generator,
                    "An ID that delays the RPC response until the template changes.\n\n\
                     In Zebra, the ID represents the chain tip, max time, and mempool contents.",
                ),
                "workid": described::<Option<String>>(
                    generator,
                    "The workid for the block template.",
                ),
            },
        })
    }
}

impl RpcSchema for SubmitBlockParameters {
    const COMPONENT: Option<&'static str> = Some("SubmitBlockParameters");

    fn schema(generator: &mut Generator) -> Value {
        json!({
            "description": "Optional argument `jsonparametersobject` for `submitblock` RPC request\n\n\
                See the notes for the [`submit_block`](crate::methods::RpcServer::submit_block) RPC.",
            "type": "object",
            "properties": {
                "workid": described::<Option<String>>(
                    generator,
                    "The workid for the block template.\n\n\
                     > If the server provided a workid, it MUST be included with submissions,\n\n\
                     Rationale:\n\n\
                     > If servers allow all mutations, it may be hard to identify which job it is \
                     based on.\n\
                     > While it may be possible to verify the submission by its content, it is \
                     much easier\n\
                     > to compare it to the job issued. It is very easy for the miner to keep \
                     track of this.\n\
                     > Therefore, using a \"workid\" is a very cheap solution to enable more \
                     mutations.\n\n\
                     <https://en.bitcoin.it/wiki/BIP_0022#Rationale>\n\n\
                     Zakura accepts this field but does not read it. The verifier identifies a \
                     prepared\ncandidate by its content, so a submission reuses the prepared work \
                     whether or not the\nminer echoes the work ID back.",
                ),
            },
        })
    }
}

impl RpcSchema for SubmitSolutionParameters {
    const COMPONENT: Option<&'static str> = Some("SubmitSolutionParameters");

    fn schema(generator: &mut Generator) -> Value {
        json!({
            "description": "A compact mined-block submission.",
            "type": "object",
            "properties": {
                "workid": described::<String>(
                    generator,
                    "The prepared candidate identifier returned by `getblocktemplate`.",
                ),
                "header": described::<HexData>(
                    generator,
                    "The complete hex-encoded solved block header.",
                ),
            },
            "required": ["workid", "header"],
        })
    }
}
