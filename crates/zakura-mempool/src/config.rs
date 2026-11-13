//! The pool core's settings.
//!
//! These are only what the machine and its storage read. The node builds them
//! from its own user config, so this crate does not depend on it.

use std::time::Duration;

/// What the pool core's storage is limited by.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Config {
    /// The most ZIP-401 cost the pool holds. A full pool evicts the
    /// transactions that pay the lowest fee per unit of cost, but only for a
    /// transaction that pays more. This is ZIP-401's `mempooltxcostlimit`.
    pub tx_cost_limit: u64,

    /// How long the ids of expired, mined and double-spent transactions are
    /// remembered. This is ZIP-401's `mempoolevictionmemoryminutes`.
    pub eviction_memory_time: Duration,

    /// The largest standard `OP_RETURN` output script, in bytes, including the
    /// opcode and the push overhead.
    pub max_datacarrier_bytes: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            // [ZIP-401] Consensus rules:
            //
            // > There MUST be a configuration option mempooltxcostlimit,
            // > which SHOULD default to 80000000.
            // >
            // > There MUST be a configuration option mempoolevictionmemoryminutes,
            // > which SHOULD default to 60 [minutes].
            //
            // [ZIP-401]: https://zips.z.cash/zip-0401#specification
            tx_cost_limit: 80_000_000,
            eviction_memory_time: Duration::from_secs(60 * 60),
            max_datacarrier_bytes: DEFAULT_MAX_DATACARRIER_BYTES,
        }
    }
}

/// The default largest standard `OP_RETURN` output script, in bytes.
///
/// Equivalent to zcashd's `MAX_OP_RETURN_RELAY`:
/// <https://github.com/zcash/zcash/blob/v6.10.0/src/script/standard.h#L22-L26>
pub const DEFAULT_MAX_DATACARRIER_BYTES: u32 = 83;
