//! Tests for JSON-RPC parameter decoding.

use super::super::jsonrpc::param_name_matches;

/// Every RPC parameter name, with the snake case and lower camel case aliases that `jsonrpsee`
/// accepted for it, as `heck` 0.5 computes them.
const PARAM_NAMES: &[(&str, &str, &str)] = &[
    ("address_strings", "address_strings", "addressStrings"),
    (
        "raw_transaction_hex",
        "raw_transaction_hex",
        "rawTransactionHex",
    ),
    ("_allow_high_fees", "allow_high_fees", "allowHighFees"),
    ("hash_or_height", "hash_or_height", "hashOrHeight"),
    ("verbosity", "verbosity", "verbosity"),
    ("verbose", "verbose", "verbose"),
    ("pool", "pool", "pool"),
    ("start_index", "start_index", "startIndex"),
    ("limit", "limit", "limit"),
    ("txid", "txid", "txid"),
    ("block_hash", "block_hash", "blockHash"),
    ("request", "request", "request"),
    ("index", "index", "index"),
    ("parameters", "parameters", "parameters"),
    ("hex_data", "hex_data", "hexData"),
    ("_parameters", "parameters", "parameters"),
    ("num_blocks", "num_blocks", "numBlocks"),
    ("height", "height", "height"),
    ("address", "address", "address"),
    ("n", "n", "n"),
    ("include_mempool", "include_mempool", "includeMempool"),
    ("addr", "addr", "addr"),
    ("command", "command", "command"),
];

#[test]
fn param_names_match_their_aliases() {
    for &(name, snake_case, lower_camel_case) in PARAM_NAMES {
        for key in [name, snake_case, lower_camel_case] {
            assert!(
                param_name_matches(key, name),
                "{key:?} should name {name:?}"
            );
        }
    }
}

#[test]
fn param_names_do_not_match_other_keys() {
    for (key, name) in [
        ("hashorheight", "hash_or_height"),
        ("HashOrHeight", "hash_or_height"),
        ("hash_or_heights", "hash_or_height"),
        ("hash__or_height", "hash_or_height"),
        ("_hash_or_height", "hash_or_height"),
        ("hashOr", "hash_or_height"),
        ("__allow_high_fees", "_allow_high_fees"),
        ("AllowHighFees", "_allow_high_fees"),
        ("", "n"),
        ("N", "n"),
        ("raw_address", "address"),
    ] {
        assert!(
            !param_name_matches(key, name),
            "{key:?} should not name {name:?}"
        );
    }
}
