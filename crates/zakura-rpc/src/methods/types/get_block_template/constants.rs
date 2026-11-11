//! Constant values used in mining rpcs methods.

use crate::server::jsonrpc::ErrorCode;

use zakura_chain::parameters::subsidy::FundingStreamReceiver::{self, *};

/// When long polling, the amount of time we wait between mempool queries.
/// (And sync status queries, which we do right before mempool queries.)
///
/// State tip changes make long polling return immediately. But miners can re-use old work
/// with an old set of transactions, so they don't need to know about mempool changes immediately.
///
/// Sync status changes are rare, and the blocks they download cause a chain tip change anyway.
///
/// `zcashd` waits 10 seconds between checking the state
/// <https://github.com/zcash/zcash/blob/420f8dfe38fd6b2465a665324366c2ae14aa98f4/src/rpc/mining.cpp#L626>
pub const MEMPOOL_LONG_POLL_INTERVAL: u64 = 5;

/// How long a long poll answered on a new tip waits for the mempool's transactions before it
/// falls back to the coinbase-only template.
///
/// The bound keeps a lagging mempool from holding back work on the new tip. On a Mainnet node
/// running this code, over 46 new blocks with up to 7 transactions waiting, we have not
/// observed worse than 3.5 ms from the node seeing a new block to returning the full template.
/// The mempool's part of that was at most 0.3 ms.
pub const NEW_TIP_MEMPOOL_WAIT: std::time::Duration = std::time::Duration::from_millis(10);

/// How many long polls answered on a new tip may build a full template at the same time.
///
/// Each one may ask the mempool, selects from its transactions and queues a template build.
/// Without a limit, every long poll woken by one tip change would do that at once. A long
/// poll over the limit answers with the coinbase-only template instead of waiting.
///
/// Three covers a pool that keeps a few long polls open on one node. The limit stays small
/// because templates are built one at a time: a larger one queues more builds, some for a tip
/// a reorg has already replaced, ahead of work on the newest tip.
pub const MAX_FULL_FIRST_ANSWERS: usize = 3;

/// The maximum number of times `getblocktemplate` rebuilds a template that was
/// superseded by a tip, parent selection, or rejection revision change.
///
/// Past the bound the RPC returns the transient "template parent changed; retry"
/// error so the miner can decide what to do.
pub const MAX_TEMPLATE_REBUILDS: usize = 4;

/// A range of valid block template nonces, that goes from `u32::MIN` to `u32::MAX` as a string.
pub const NONCE_RANGE_FIELD: &str = "00000000ffffffff";

/// A hardcoded list of fields that the miner can change from the block template.
///
/// <https://en.bitcoin.it/wiki/BIP_0023#Mutations>
pub const MUTABLE_FIELD: &[&str] = &[
    // Standard mutations, copied from zcashd
    "time",
    "transactions",
    "prevblock",
];

/// A hardcoded list of Zebra's getblocktemplate RPC capabilities.
///
/// <https://en.bitcoin.it/wiki/BIP_0023#Block_Proposal>
pub const CAPABILITIES_FIELD: &[&str] = &["proposal", "submitsolution"];

/// The maximum age of the local best tip's block time accepted by the mining RPCs.
///
/// This is a duration rather than a block count, so it is unaffected by target-spacing
/// changes such as NU7.
///
/// Six hours leaves a wide margin over the network's clock tolerance and over realistic
/// mining stalls:
/// > A full validator MUST NOT accept blocks with nTime more than two hours in the future
/// > according to its clock. This is not strictly a consensus rule because it is nondeterministic,
/// > and clock time varies between nodes.
/// >
/// > <https://zips.z.cash/protocol/protocol.pdf#blockheader>
pub const MAX_TIME_SINCE_CHAIN_TIP: chrono::Duration = chrono::Duration::hours(6);

/// The RPC error code used by `zcashd` for when it's still downloading initial blocks.
///
/// `s-nomp` mining pool expects error code `-10` when the node is not synced:
/// <https://github.com/s-nomp/node-stratum-pool/blob/d86ae73f8ff968d9355bb61aac05e0ebef36ccb5/lib/pool.js#L142>
pub const NOT_SYNCED_ERROR_CODE: ErrorCode = ErrorCode::ServerError(-10);

/// The default window size specifying how many blocks to check when estimating the chain's solution rate.
///
/// Based on default value in zcashd.
pub const DEFAULT_SOLUTION_RATE_WINDOW_SIZE: i32 = 120;

/// The funding stream order in `zcashd` RPC responses.
///
/// [`zcashd`]: https://github.com/zcash/zcash/blob/3f09cfa00a3c90336580a127e0096d99e25a38d6/src/consensus/funding.cpp#L13-L32
pub const ZCASHD_FUNDING_STREAM_ORDER: &[FundingStreamReceiver] =
    &[Ecc, ZcashFoundation, MajorGrants];
