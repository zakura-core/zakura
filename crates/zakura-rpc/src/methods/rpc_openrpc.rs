/// Lookup table for JSON-RPC methods.
#[allow(unused_qualifications)]
pub static METHODS: ::phf::Map<&str, openrpsee::openrpc::RpcMethod> = ::phf::phf_map! {
"getinfo" => openrpsee::openrpc::RpcMethod {
    description: "Returns software information from the RPC server, as a\n[`GetInfoResponse`] JSON struct.\n\nzcashd reference: [`getinfo`](https://zcash.github.io/rpc/getinfo.html)\nmethod: post\ntags: control\n\n# Notes\n\n[The zcashd reference](https://zcash.github.io/rpc/getinfo.html) might not show some fields\nin Zebra\'s [`GetInfoResponse`]. Zebra uses the field names and formats\nfrom the [zcashd\ncode](https://github.com/zcash/zcash/blob/v4.6.0-1/src/rpc/misc.cpp#L86-L87).\n\nSome fields from the zcashd reference are missing from Zebra\'s\n[`GetInfoResponse`]. It only contains the fields\n[required for lightwalletd support.](https://github.com/zcash/lightwalletd/blob/v0.4.9/common/common.go#L91-L95)\n",
    params: |_g| vec![
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("getinfo_result"),
    deprecated: false,
},
"getdeprecationinfo" => openrpsee::openrpc::RpcMethod {
    description: "Returns end-of-support information for this node release, as a\n[`GetDeprecationInfoResponse`] JSON struct.\n\nzcashd reference:\n[`getdeprecationinfo`](https://zcash.github.io/rpc/getdeprecationinfo.html)\nmethod: post\ntags: network\n\n# Notes\n\nAs in zcashd, the `end_of_service` object is only present on Mainnet,\nwhere end of support is enforced. Zakura reports the estimated last\nheight this release supports in `end_of_service.block_height`; the node\nhalts when the tip goes past it.\n\nSome fields from the zcashd response are missing from Zakura\'s response:\n`version` and `subversion` are available from `getinfo`,\n`deprecationheight` is deprecated in zcashd, and Zakura does not have\nzcashd\'s feature deprecation framework.\n\nThe estimate assumes the node is synced to the network tip; during\ninitial sync it is significantly overestimated.\n",
    params: |_g| vec![
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("getdeprecationinfo_result"),
    deprecated: false,
},
"getblockchaininfo" => openrpsee::openrpc::RpcMethod {
    description: "Returns blockchain state information, as a [`GetBlockchainInfoResponse`] JSON struct.\n\nzcashd reference: [`getblockchaininfo`](https://zcash.github.io/rpc/getblockchaininfo.html)\nmethod: post\ntags: blockchain\n\n# Notes\n\nSome fields from the zcashd reference are missing from Zebra\'s [`GetBlockchainInfoResponse`]. It only contains the fields\n[required for lightwalletd support.](https://github.com/zcash/lightwalletd/blob/v0.4.9/common/common.go#L72-L89)\n\nZebra includes an `ironwood` value pool entry. Like other value pool\nentries, it can be present with a zero balance.\n",
    params: |_g| vec![
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("getblockchaininfo_result"),
    deprecated: false,
},
"getaddressbalance" => openrpsee::openrpc::RpcMethod {
    description: "Returns the total balance of provided `addresses` in a\n[`GetAddressBalanceResponse`] instance.\n\nzcashd reference: [`getaddressbalance`](https://zcash.github.io/rpc/getaddressbalance.html)\nmethod: post\ntags: address\n\n# Parameters\n\n- `address_strings`: (object, example={\"addresses\": [\"tmYXBYJj1K7vhejSec5osXK2QsGa5MTisUQ\"]}) A JSON map with a single entry\n    - `addresses`: (array of strings) A list of base-58 encoded addresses.\n\n# Notes\n\nzcashd also accepts a single string parameter instead of an array of strings, but Zebra\ndoesn\'t because lightwalletd always calls this RPC with an array of addresses.\n\nzcashd also returns the total amount of Zatoshis received by the addresses, but Zebra\ndoesn\'t because lightwalletd doesn\'t use that information.\n\nThe RPC documentation says that the returned object has a string `balance` field, but\nzcashd actually [returns an\ninteger](https://github.com/zcash/lightwalletd/blob/bdaac63f3ee0dbef62bde04f6817a9f90d483b00/common/common.go#L128-L130).\n",
    params: |_g| vec![
        _g.param::<GetAddressBalanceRequest>("address_strings", crate::methods::PARAM_ADDRESS_STRINGS_DESC, true),
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("getaddressbalance_result"),
    deprecated: false,
},
"sendrawtransaction" => openrpsee::openrpc::RpcMethod {
    description: "Sends the raw bytes of a signed transaction to the local node\'s mempool, if the transaction is valid.\nReturns the [`SendRawTransactionResponse`] for the transaction, as a\nJSON string.\n\nzcashd reference: [`sendrawtransaction`](https://zcash.github.io/rpc/sendrawtransaction.html)\nmethod: post\ntags: transaction\n\n# Parameters\n\n- `raw_transaction_hex`: (string, required, example=\"signedhex\") The hex-encoded raw transaction bytes.\n- `allow_high_fees`: (bool, optional) A legacy parameter accepted by zcashd but ignored by Zebra.\n\n# Notes\n\nzcashd accepts an optional `allowhighfees` parameter. Zebra doesn\'t support this parameter,\nbecause lightwalletd doesn\'t use it.\n",
    params: |_g| vec![
        _g.param::<String>("raw_transaction_hex", crate::methods::PARAM_RAW_TRANSACTION_HEX_DESC, true),
        _g.param::<bool>("_allow_high_fees", crate::methods::PARAM__ALLOW_HIGH_FEES_DESC, false),
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("sendrawtransaction_result"),
    deprecated: false,
},
"getblock" => openrpsee::openrpc::RpcMethod {
    description: "Returns the requested block by hash or height, as a\n[`GetBlockResponse`] JSON string.\nIf the block is not in Zebra\'s state, returns\n[error code `-8`.](https://github.com/zcash/zcash/issues/5758) if a height was\npassed or -5 if a hash was passed.\n\nzcashd reference: [`getblock`](https://zcash.github.io/rpc/getblock.html)\nmethod: post\ntags: blockchain\n\n# Parameters\n\n- `hash_or_height`: (string, required, example=\"1\") The hash or height for the block to be returned.\n- `verbosity`: (number, optional, default=1, example=1) 0 for hex encoded data, 1 for a json object, and 2 for json object with transaction data.\n\n# Notes\n\nThe `size` field is only returned with verbosity=2.\n\nThe undocumented `chainwork` field is not returned.\n\nWith verbosity=2, transaction objects include `ironwood` only when the\ntransaction contains Ironwood shielded data. Block `trees` can include an\nIronwood tree entry when the state has one for the requested block.\n",
    params: |_g| vec![
        _g.param::<String>("hash_or_height", crate::methods::PARAM_HASH_OR_HEIGHT_DESC, true),
        _g.param::<u8>("verbosity", crate::methods::PARAM_VERBOSITY_DESC, false),
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("getblock_result"),
    deprecated: false,
},
"getblockheader" => openrpsee::openrpc::RpcMethod {
    description: "Returns the requested block header by hash or height, as a\n[`GetBlockHeaderResponse`] JSON string.\nIf the block is not in Zebra\'s state,\nreturns [error code `-8`.](https://github.com/zcash/zcash/issues/5758)\nif a height was passed or -5 if a hash was passed.\n\nzcashd reference: [`getblockheader`](https://zcash.github.io/rpc/getblockheader.html)\nmethod: post\ntags: blockchain\n\n# Parameters\n\n- `hash_or_height`: (string, required, example=\"1\") The hash or height for the block to be returned.\n- `verbose`: (bool, optional, default=false, example=true) false for hex encoded data, true for a json object\n\n# Notes\n\nThe undocumented `chainwork` field is not returned.\n",
    params: |_g| vec![
        _g.param::<String>("hash_or_height", crate::methods::PARAM_HASH_OR_HEIGHT_DESC, true),
        _g.param::<bool>("verbose", crate::methods::PARAM_VERBOSE_DESC, false),
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("getblockheader_result"),
    deprecated: false,
},
"getbestblockhash" => openrpsee::openrpc::RpcMethod {
    description: "Returns the hash of the current best blockchain tip block, as a\n[`GetBlockHashResponse`] JSON string.\n\nzcashd reference: [`getbestblockhash`](https://zcash.github.io/rpc/getbestblockhash.html)\nmethod: post\ntags: blockchain\n",
    params: |_g| vec![
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("getbestblockhash_result"),
    deprecated: false,
},
"getbestblockheightandhash" => openrpsee::openrpc::RpcMethod {
    description: "Returns the height and hash of the current best blockchain tip block, as a [`GetBlockHeightAndHashResponse`] JSON struct.\n\nzcashd reference: none\nmethod: post\ntags: blockchain\n",
    params: |_g| vec![
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("getbestblockheightandhash_result"),
    deprecated: false,
},
"getchaintips" => openrpsee::openrpc::RpcMethod {
    description: "Returns information about every tip in the block tree that this node still\ntracks, including the best chain and orphaned branches.\n\nzcashd reference: [`getchaintips`](https://zcash.github.io/rpc/getchaintips.html)\nmethod: post\ntags: blockchain\n\n# Notes\n\nzcashd answers this call by scanning its entire block index under `cs_main`,\nwhich costs seconds once the index holds millions of entries and blocks every\nother RPC for that whole time. Zakura reads only the chains it holds in\nmemory, so the cost is bounded by the number of tracked forks rather than by\nthe height of the chain.\n\nThe two nodes therefore report different tips. zcashd\'s block index is never\npruned, so it lists every stale tip it has ever seen. Zakura drops a fork once\nit falls below the finalized tip, so it lists the tips that are still live:\nthe best chain, the non-finalized forks, recently invalidated branches, and\nthe selected header chain when some block bodies are unavailable.\n\nZakura never returns zcashd\'s `valid-headers` or `unknown` statuses. Every\nblock in its non-finalized state is contextually verified, so a tip is either\nfully valid, invalidated, or known only by its header.\n\n`branchlen` can be short for an `invalid` tip. Zakura tracks a limited number\nof forks, and it can drop the chain that an invalidated branch forked from.\nThe branch is still reported, but its length is then measured from the deepest\nblock the node still tracks.\n",
    params: |_g| vec![
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("getchaintips_result"),
    deprecated: false,
},
"getmempoolinfo" => openrpsee::openrpc::RpcMethod {
    description: "Returns details on the active state of the TX memory pool.\n\nzcash reference: [`getmempoolinfo`](https://zcash.github.io/rpc/getmempoolinfo.html)\n",
    params: |_g| vec![
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("getmempoolinfo_result"),
    deprecated: false,
},
"getrawmempool" => openrpsee::openrpc::RpcMethod {
    description: "Returns all transaction ids in the memory pool, as a JSON array.\n\n# Parameters\n\n- `verbose`: (boolean, optional, default=false) true for a json object, false for array of transaction ids.\n\nzcashd reference: [`getrawmempool`](https://zcash.github.io/rpc/getrawmempool.html)\nmethod: post\ntags: blockchain\n",
    params: |_g| vec![
        _g.param::<bool>("verbose", crate::methods::PARAM_VERBOSE_DESC, false),
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("getrawmempool_result"),
    deprecated: false,
},
"z_gettreestate" => openrpsee::openrpc::RpcMethod {
    description: "Returns information about the given block\'s Sapling, Orchard, and when\navailable Ironwood tree state.\n\nzcashd reference: [`z_gettreestate`](https://zcash.github.io/rpc/z_gettreestate.html)\nmethod: post\ntags: blockchain\n\n# Parameters\n\n- `hash | height`: (string, required, example=\"00000000febc373a1da2bd9f887b105ad79ddc26ac26c2b28652d64e5207c5b5\") The block hash or height.\n\n# Notes\n\nThe zcashd doc reference above says that the parameter \"`height` can be\nnegative where -1 is the last known valid block\". On the other hand,\n`lightwalletd` only uses positive heights, so Zebra does not support\nnegative heights.\n\nThe `ironwood` field contains empty commitments unless Ironwood tree\nstate is available for the requested block.\n",
    params: |_g| vec![
        _g.param::<String>("hash_or_height", crate::methods::PARAM_HASH_OR_HEIGHT_DESC, true),
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("z_gettreestate_result"),
    deprecated: false,
},
"z_getsubtreesbyindex" => openrpsee::openrpc::RpcMethod {
    description: "Returns information about a range of shielded subtrees.\n\nzcashd reference: [`z_getsubtreesbyindex`](https://zcash.github.io/rpc/z_getsubtreesbyindex.html) - TODO: fix link\nmethod: post\ntags: blockchain\n\n# Parameters\n\n- `pool`: (string, required) The pool from which subtrees should be returned.\n  Either \"sapling\", \"orchard\", or \"ironwood\".\n- `start_index`: (number, required) The index of the first 2^16-leaf subtree to return.\n- `limit`: (number, optional) The maximum number of subtree values to return.\n\n# Notes\n\nWhile Zebra is doing its initial subtree index rebuild, subtrees will become available\nstarting at the chain tip. This RPC will return an empty list if the `start_index` subtree\nexists, but has not been rebuilt yet. This matches `zcashd`\'s behaviour when subtrees aren\'t\navailable yet. (But `zcashd` does its rebuild before syncing any blocks.)\n",
    params: |_g| vec![
        _g.param::<String>("pool", crate::methods::PARAM_POOL_DESC, true),
        _g.param::<NoteCommitmentSubtreeIndex>("start_index", crate::methods::PARAM_START_INDEX_DESC, true),
        _g.param::<NoteCommitmentSubtreeIndex>("limit", crate::methods::PARAM_LIMIT_DESC, false),
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("z_getsubtreesbyindex_result"),
    deprecated: false,
},
"getrawtransaction" => openrpsee::openrpc::RpcMethod {
    description: "Returns the raw transaction data, as a [`GetRawTransactionResponse`]\nJSON string or structure.\n\nzcashd reference: [`getrawtransaction`](https://zcash.github.io/rpc/getrawtransaction.html)\nmethod: post\ntags: transaction\n\n# Parameters\n\n- `txid`: (string, required, example=\"mytxid\") The transaction ID of the transaction to be returned.\n- `verbose`: (number, optional, default=0, example=1) If 0, return a string of hex-encoded data, otherwise return a JSON object.\n- `blockhash` (string, optional) The block in which to look for the transaction\n\n# Notes\n\nVerbose transaction output includes `ironwood` only when the transaction\ncontains Ironwood shielded data.\n",
    params: |_g| vec![
        _g.param::<String>("txid", crate::methods::PARAM_TXID_DESC, true),
        _g.param::<u8>("verbose", crate::methods::PARAM_VERBOSE_DESC, false),
        _g.param::<String>("block_hash", crate::methods::PARAM_BLOCK_HASH_DESC, false),
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("getrawtransaction_result"),
    deprecated: false,
},
"getaddresstxids" => openrpsee::openrpc::RpcMethod {
    description: "Returns the transaction ids made by the provided transparent addresses.\n\nzcashd reference: [`getaddresstxids`](https://zcash.github.io/rpc/getaddresstxids.html)\nmethod: post\ntags: address\n\n# Parameters\n\n- `request`: (required) Either:\n    - A single address string (e.g., `\"tmYXBYJj1K7vhejSec5osXK2QsGa5MTisUQ\"`), or\n    - An object with the following named fields:\n        - `addresses`: (array of strings, required) The addresses to get transactions from.\n        - `start`: (numeric, optional) The lower height to start looking for transactions (inclusive).\n        - `end`: (numeric, optional) The upper height to stop looking for transactions (inclusive).\n\nExample of the object form:\n```json\n{\n  \"addresses\": [\"tmYXBYJj1K7vhejSec5osXK2QsGa5MTisUQ\"],\n  \"start\": 1000,\n  \"end\": 2000\n}\n```\n\n# Notes\n\n- Only the multi-argument format is used by lightwalletd and this is what we currently support:\n<https://github.com/zcash/lightwalletd/blob/631bb16404e3d8b045e74a7c5489db626790b2f6/common/common.go#L97-L102>\n- It is recommended that users call the method with start/end heights such that the response can\'t be too large.\n",
    params: |_g| vec![
        _g.param::<GetAddressTxIdsRequest>("request", crate::methods::PARAM_REQUEST_DESC, true),
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("getaddresstxids_result"),
    deprecated: false,
},
"getaddressutxos" => openrpsee::openrpc::RpcMethod {
    description: "Returns all unspent outputs for a list of addresses.\n\nzcashd reference: [`getaddressutxos`](https://zcash.github.io/rpc/getaddressutxos.html)\nmethod: post\ntags: address\n\n# Parameters\n\n- `request`: (required) Either:\n    - A single address string (e.g., `\"tmYXBYJj1K7vhejSec5osXK2QsGa5MTisUQ\"`), or\n    - An object with the following named fields:\n        - `addresses`: (array, required, example=[\\\"tmYXBYJj1K7vhejSec5osXK2QsGa5MTisUQ\\\"]) The addresses to get outputs from.\n        - `chaininfo`: (boolean, optional, default=false) Include chain info with results\n\n# Notes\n\nlightwalletd always uses the multi-address request, without chaininfo:\n<https://github.com/zcash/lightwalletd/blob/master/frontend/service.go#L402>\n",
    params: |_g| vec![
        _g.param::<GetAddressUtxosRequest>("request", crate::methods::PARAM_REQUEST_DESC, true),
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("getaddressutxos_result"),
    deprecated: false,
},
"stop" => openrpsee::openrpc::RpcMethod {
    description: "Stop the running zakurad process.\n\n# Notes\n\n- Works for non windows targets only.\n- Works only if the network of the running zakurad process is `Regtest`.\n\nzcashd reference: [`stop`](https://zcash.github.io/rpc/stop.html)\nmethod: post\ntags: control\n",
    params: |_g| vec![
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("stop_result"),
    deprecated: false,
},
"getblockcount" => openrpsee::openrpc::RpcMethod {
    description: "Returns the height of the most recent block in the best valid block chain (equivalently,\nthe number of blocks in this chain excluding the genesis block).\n\nzcashd reference: [`getblockcount`](https://zcash.github.io/rpc/getblockcount.html)\nmethod: post\ntags: blockchain\n",
    params: |_g| vec![
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("getblockcount_result"),
    deprecated: false,
},
"getblockhash" => openrpsee::openrpc::RpcMethod {
    description: "Returns the hash of the block of a given height iff the index argument correspond\nto a block in the best chain.\n\nzcashd reference: [`getblockhash`](https://zcash-rpc.github.io/getblockhash.html)\nmethod: post\ntags: blockchain\n\n# Parameters\n\n- `index`: (numeric, required, example=1) The block index.\n\n# Notes\n\n- If `index` is positive then index = block height.\n- If `index` is negative then -1 is the last known valid block.\n",
    params: |_g| vec![
        _g.param::<i32>("index", crate::methods::PARAM_INDEX_DESC, true),
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("getblockhash_result"),
    deprecated: false,
},
"getblocktemplate" => openrpsee::openrpc::RpcMethod {
    description: "Returns a block template for mining new Zcash blocks.\n\n# Parameters\n\n- `jsonrequestobject`: (string, optional) A JSON object containing arguments.\n\nzcashd reference: [`getblocktemplate`](https://zcash-rpc.github.io/getblocktemplate.html)\nmethod: post\ntags: mining\n\n# Notes\n\nArguments to this RPC are currently ignored.\nLong polling, block proposals, server lists, and work IDs are not supported.\n\nMiners can make arbitrary changes to blocks, as long as:\n- the data sent to `submitblock` is a valid Zcash block, and\n- the parent block is a valid block that Zebra already has, or will receive soon.\n\nZebra verifies blocks in parallel, and keeps recent chains in parallel,\nso moving between chains and forking chains is very cheap.\n",
    params: |_g| vec![
        _g.param::<GetBlockTemplateParameters>("parameters", crate::methods::PARAM_PARAMETERS_DESC, false),
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("getblocktemplate_result"),
    deprecated: false,
},
"submitblock" => openrpsee::openrpc::RpcMethod {
    description: "Submits block to the node to be validated and committed.\nReturns the [`SubmitBlockResponse`] for the operation, as a JSON string.\n\nzcashd reference: [`submitblock`](https://zcash.github.io/rpc/submitblock.html)\nmethod: post\ntags: mining\n\n# Parameters\n\n- `hexdata`: (string, required)\n- `jsonparametersobject`: (string, optional) - currently ignored\n\n# Notes\n\n - `jsonparametersobject` holds a single field, workid, that must be included in submissions if provided by the server.\n",
    params: |_g| vec![
        _g.param::<HexData>("hex_data", crate::methods::PARAM_HEX_DATA_DESC, true),
        _g.param::<SubmitBlockParameters>("_parameters", crate::methods::PARAM__PARAMETERS_DESC, false),
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("submitblock_result"),
    deprecated: false,
},
"getmininginfo" => openrpsee::openrpc::RpcMethod {
    description: "Returns mining-related information.\n\nzcashd reference: [`getmininginfo`](https://zcash.github.io/rpc/getmininginfo.html)\nmethod: post\ntags: mining\n",
    params: |_g| vec![
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("getmininginfo_result"),
    deprecated: false,
},
"getnetworksolps" => openrpsee::openrpc::RpcMethod {
    description: "Returns the estimated network solutions per second based on the last `num_blocks` before\n`height`.\n\nIf `num_blocks` is not supplied, uses 120 blocks. If it is 0 or -1, uses the difficulty\naveraging window.\nIf `height` is not supplied or is -1, uses the tip height.\n\nzcashd reference: [`getnetworksolps`](https://zcash.github.io/rpc/getnetworksolps.html)\nmethod: post\ntags: mining\n",
    params: |_g| vec![
        _g.param::<i32>("num_blocks", crate::methods::PARAM_NUM_BLOCKS_DESC, false),
        _g.param::<i32>("height", crate::methods::PARAM_HEIGHT_DESC, false),
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("getnetworksolps_result"),
    deprecated: false,
},
"getnetworkhashps" => openrpsee::openrpc::RpcMethod {
    description: "Returns the estimated network solutions per second based on the last `num_blocks` before\n`height`.\n\nThis method name is deprecated, use [`getnetworksolps`](Self::get_network_sol_ps) instead.\nSee that method for details.\n\nzcashd reference: [`getnetworkhashps`](https://zcash.github.io/rpc/getnetworkhashps.html)\nmethod: post\ntags: mining\n",
    params: |_g| vec![
        _g.param::<i32>("num_blocks", crate::methods::PARAM_NUM_BLOCKS_DESC, false),
        _g.param::<i32>("height", crate::methods::PARAM_HEIGHT_DESC, false),
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("getnetworkhashps_result"),
    deprecated: false,
},
"getnetworkinfo" => openrpsee::openrpc::RpcMethod {
    description: "Returns an object containing various state info regarding P2P networking.\n\nzcashd reference: [`getnetworkinfo`](https://zcash.github.io/rpc/getnetworkinfo.html)\nmethod: post\ntags: network\n",
    params: |_g| vec![
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("getnetworkinfo_result"),
    deprecated: false,
},
"getpeerinfo" => openrpsee::openrpc::RpcMethod {
    description: "Returns data about each connected network node.\n\nzcashd reference: [`getpeerinfo`](https://zcash.github.io/rpc/getpeerinfo.html)\nmethod: post\ntags: network\n",
    params: |_g| vec![
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("getpeerinfo_result"),
    deprecated: false,
},
"ping" => openrpsee::openrpc::RpcMethod {
    description: "Requests that a ping be sent to all other nodes, to measure ping time.\n\nResults provided in getpeerinfo, pingtime and pingwait fields are decimal seconds.\nPing command is handled in queue with all other commands, so it measures processing backlog, not just network ping.\n\nzcashd reference: [`ping`](https://zcash.github.io/rpc/ping.html)\nmethod: post\ntags: network\n",
    params: |_g| vec![
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("ping_result"),
    deprecated: false,
},
"validateaddress" => openrpsee::openrpc::RpcMethod {
    description: "Checks if a zcash transparent address of type P2PKH, P2SH or TEX is valid.\nReturns information about the given address if valid.\n\nzcashd reference: [`validateaddress`](https://zcash.github.io/rpc/validateaddress.html)\nmethod: post\ntags: util\n\n# Parameters\n\n- `address`: (string, required) The zcash address to validate.\n",
    params: |_g| vec![
        _g.param::<String>("address", crate::methods::PARAM_ADDRESS_DESC, true),
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("validateaddress_result"),
    deprecated: false,
},
"z_validateaddress" => openrpsee::openrpc::RpcMethod {
    description: "Checks if a zcash address of type P2PKH, P2SH, TEX, SAPLING or UNIFIED is valid.\nReturns information about the given address if valid.\n\nzcashd reference: [`z_validateaddress`](https://zcash.github.io/rpc/z_validateaddress.html)\nmethod: post\ntags: util\n\n# Parameters\n\n- `address`: (string, required) The zcash address to validate.\n\n# Notes\n\n- No notes\n",
    params: |_g| vec![
        _g.param::<String>("address", crate::methods::PARAM_ADDRESS_DESC, true),
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("z_validateaddress_result"),
    deprecated: false,
},
"getblocksubsidy" => openrpsee::openrpc::RpcMethod {
    description: "Returns the block subsidy reward of the block at `height`, taking into account the mining slow start.\nReturns an error if `height` is less than the height of the first halving for the current network.\n\nzcashd reference: [`getblocksubsidy`](https://zcash.github.io/rpc/getblocksubsidy.html)\nmethod: post\ntags: mining\n\n# Parameters\n\n- `height`: (numeric, optional, example=1) Can be any valid current or future height.\n\n# Notes\n\nIf `height` is not supplied, uses the tip height.\n",
    params: |_g| vec![
        _g.param::<u32>("height", crate::methods::PARAM_HEIGHT_DESC, false),
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("getblocksubsidy_result"),
    deprecated: false,
},
"getdifficulty" => openrpsee::openrpc::RpcMethod {
    description: "Returns the proof-of-work difficulty as a multiple of the minimum difficulty.\n\nzcashd reference: [`getdifficulty`](https://zcash.github.io/rpc/getdifficulty.html)\nmethod: post\ntags: blockchain\n",
    params: |_g| vec![
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("getdifficulty_result"),
    deprecated: false,
},
"z_listunifiedreceivers" => openrpsee::openrpc::RpcMethod {
    description: "Returns the list of individual payment addresses given a unified address.\n\nzcashd reference: [`z_listunifiedreceivers`](https://zcash.github.io/rpc/z_listunifiedreceivers.html)\nmethod: post\ntags: wallet\n\n# Parameters\n\n- `address`: (string, required) The zcash unified address to get the list from.\n\n# Notes\n\n- No notes\n",
    params: |_g| vec![
        _g.param::<String>("address", crate::methods::PARAM_ADDRESS_DESC, true),
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("z_listunifiedreceivers_result"),
    deprecated: false,
},
"invalidateblock" => openrpsee::openrpc::RpcMethod {
    description: "Invalidates a block if it is not yet finalized, removing it from the non-finalized\nstate if it is present and rejecting it during contextual validation if it is submitted.\n\n# Parameters\n\n- `block_hash`: (hex-encoded block hash, required) The block hash to invalidate.\n",
    params: |_g| vec![
        _g.param::<String>("block_hash", crate::methods::PARAM_BLOCK_HASH_DESC, true),
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("invalidateblock_result"),
    deprecated: false,
},
"reconsiderblock" => openrpsee::openrpc::RpcMethod {
    description: "Reconsiders a previously invalidated block if it exists in the cache of previously invalidated blocks.\n\n# Parameters\n\n- `block_hash`: (hex-encoded block hash, required) The block hash to reconsider.\n",
    params: |_g| vec![
        _g.param::<String>("block_hash", crate::methods::PARAM_BLOCK_HASH_DESC, true),
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("reconsiderblock_result"),
    deprecated: false,
},
"generate" => openrpsee::openrpc::RpcMethod {
    description: "Mine blocks immediately. Returns the block hashes of the generated blocks.\n\n# Parameters\n\n- `num_blocks`: (numeric, required, example=1) Number of blocks to be generated.\n\n# Notes\n\nOnly works if the network of the running zakurad process is `Regtest`.\n\nzcashd reference: [`generate`](https://zcash.github.io/rpc/generate.html)\nmethod: post\ntags: generating\n",
    params: |_g| vec![
        _g.param::<u32>("num_blocks", crate::methods::PARAM_NUM_BLOCKS_DESC, true),
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("generate_result"),
    deprecated: false,
},
"addnode" => openrpsee::openrpc::RpcMethod {
    description: "Add or remove a node from the address book.\n\n# Parameters\n\n- `addr`: (string, required) The address of the node to add or remove.\n- `command`: (string, required) The command to execute, either \"add\", \"onetry\", or \"remove\".\n\n# Notes\n\nOnly the \"add\" command is currently supported.\n\nzcashd reference: [`addnode`](https://zcash.github.io/rpc/addnode.html)\nmethod: post\ntags: network\n",
    params: |_g| vec![
        _g.param::<PeerSocketAddr>("addr", crate::methods::PARAM_ADDR_DESC, true),
        _g.param::<AddNodeCommand>("command", crate::methods::PARAM_COMMAND_DESC, true),
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("addnode_result"),
    deprecated: false,
},
"rpc.discover" => openrpsee::openrpc::RpcMethod {
    description: "Returns an OpenRPC schema as a description of this service.\n",
    params: |_g| vec![
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("rpc.discover_result"),
    deprecated: false,
},
"gettxout" => openrpsee::openrpc::RpcMethod {
    description: "Returns details about an unspent transaction output.\n\nzcashd reference: [`gettxout`](https://zcash.github.io/rpc/gettxout.html)\nmethod: post\ntags: transaction\n\n# Parameters\n\n- `txid`: (string, required, example=\"mytxid\") The transaction ID that contains the output.\n- `n`: (number, required) The output index number.\n- `include_mempool` (bool, optional, default=true) Whether to include the mempool in the search.\n",
    params: |_g| vec![
        _g.param::<String>("txid", crate::methods::PARAM_TXID_DESC, true),
        _g.param::<u32>("n", crate::methods::PARAM_N_DESC, true),
        _g.param::<bool>("include_mempool", crate::methods::PARAM_INCLUDE_MEMPOOL_DESC, false),
    ],
    result: |g| g.result::<openrpsee::openrpc::ResultType>("gettxout_result"),
    deprecated: false,
},
};