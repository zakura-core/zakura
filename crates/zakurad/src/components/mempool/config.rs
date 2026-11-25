//! User-configurable mempool parameters.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Mempool configuration section.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    /// The mempool transaction cost limit.
    ///
    /// This limits the total ZIP-401 cost of all transactions in the mempool. When the mempool
    /// is full, it evicts the transactions that pay the lowest fee per unit of cost, but only
    /// for a transaction that pays more.
    ///
    /// Consensus rule:
    /// > There MUST be a configuration option mempooltxcostlimit, which SHOULD default to 80000000.
    ///
    /// This corresponds to `mempooltxcostlimit` from [ZIP-401](https://zips.z.cash/zip-0401#specification).
    pub tx_cost_limit: u64,

    /// Maximum serialized size of an individual transaction accepted into the mempool.
    ///
    /// Transactions exactly at this limit are accepted. Larger transactions are rejected before
    /// semantic and contextual verification. This local policy does not affect block validation.
    pub max_transaction_bytes: u64,

    /// The mempool rejection age limit.
    ///
    /// This limits the maximum amount of time the IDs of expired, mined, and double-spent
    /// transactions stay in the mempool rejection lists. The mempool forgets evicted
    /// transactions at the next block instead, because blocks free mempool space.
    ///
    /// This corresponds to `mempoolevictionmemoryminutes` from
    /// [ZIP-401](https://zips.z.cash/zip-0401#specification).
    #[serde(with = "zakura_chain::serialization::serde_adapters::duration")]
    pub eviction_memory_time: Duration,

    /// If the state's best chain tip has reached this height, always enable the mempool,
    /// regardless of Zebra's sync status.
    ///
    /// Set to `None` by default: Zebra always checks the sync status before enabling the mempool.
    //
    // TODO:
    // - allow the mempool to be enabled before the genesis block is committed?
    //   we could replace `Option` with an enum that has an `AlwaysEnable` variant
    pub debug_enable_at_height: Option<u32>,

    /// Maximum size in bytes of an OP_RETURN script that is considered standard.
    ///
    /// If unset, defaults to [`DEFAULT_MAX_DATACARRIER_BYTES`]. This size includes the OP_RETURN
    /// opcode and pushdata overhead. Matches zcashd's `-datacarriersize` default behavior.
    pub max_datacarrier_bytes: Option<u32>,

    /// Which pool the node runs.
    ///
    /// `default` is the node's choice. In this release that is `core` when the
    /// P2P stack runs v2 (`dual` or `zakura`), and `classic` on a legacy-only
    /// stack. What `default` means can change in a later release. `classic`
    /// is the pool nodes have run so far, and setting it is the rollback.
    /// `core` runs the pool core on any stack.
    pub implementation: Implementation,
}

/// Which pool a node runs. See [`Config::implementation`].
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Implementation {
    /// The node's choice for this release.
    #[default]
    Default,
    /// The pool nodes have run so far.
    Classic,
    /// The pool core.
    Core,
}

impl Implementation {
    /// The pool this setting runs on a node whose P2P stack runs v2 exactly
    /// when `v2_p2p` is true. `Default` resolves to `Core` on a v2 stack,
    /// where `tx-relay` can carry the pool's transactions, and to `Classic`
    /// on a legacy-only one.
    pub fn resolve(self, v2_p2p: bool) -> Self {
        match self {
            Self::Default if v2_p2p => Self::Core,
            Self::Default | Self::Classic => Self::Classic,
            Self::Core => Self::Core,
        }
    }

    /// The setting's name in the config file.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Classic => "classic",
            Self::Core => "core",
        }
    }
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
            max_transaction_bytes: DEFAULT_MAX_TRANSACTION_BYTES,
            eviction_memory_time: Duration::from_secs(60 * 60),

            debug_enable_at_height: None,

            max_datacarrier_bytes: Some(DEFAULT_MAX_DATACARRIER_BYTES),

            implementation: Implementation::Default,
        }
    }
}

/// Default maximum serialized size of an individual mempool transaction, in bytes.
pub const DEFAULT_MAX_TRANSACTION_BYTES: u64 = 250_000;

/// Default maximum size of data carrier scripts (OP_RETURN), in bytes.
///
/// Equivalent to zcashd's `MAX_OP_RETURN_RELAY`:
/// <https://github.com/zcash/zcash/blob/v6.10.0/src/script/standard.h#L22-L26>
pub const DEFAULT_MAX_DATACARRIER_BYTES: u32 = 83;

#[cfg(test)]
mod tests {
    use zakura_chain::parameters::Network;
    use zakura_network::{Config as NetworkConfig, P2pStack};

    use super::Implementation;

    /// The pool each setting runs, with each network's default stack and with
    /// each stack set explicitly.
    #[test]
    fn default_runs_the_core_exactly_where_v2_runs() {
        let networks = [
            Network::Mainnet,
            Network::new_default_testnet(),
            Network::new_regtest(Default::default()),
        ];
        for network in networks {
            for stack in [
                P2pStack::Default,
                P2pStack::Legacy,
                P2pStack::Dual,
                P2pStack::Zakura,
            ] {
                let v2_p2p = NetworkConfig {
                    network: network.clone(),
                    p2p_stack: stack,
                    ..NetworkConfig::default()
                }
                .v2_p2p();
                let legacy_only = matches!(stack, P2pStack::Legacy)
                    || (matches!(stack, P2pStack::Default) && network == Network::Mainnet);
                let expected_default = if legacy_only {
                    Implementation::Classic
                } else {
                    Implementation::Core
                };

                for (setting, expected) in [
                    (Implementation::Default, expected_default),
                    (Implementation::Classic, Implementation::Classic),
                    (Implementation::Core, Implementation::Core),
                ] {
                    assert_eq!(
                        setting.resolve(v2_p2p),
                        expected,
                        "{setting:?} on {network:?} with {stack:?}",
                    );
                }
            }
        }
    }
}
