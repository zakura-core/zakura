//! Startup replacement of default seeds' retired v2 node IDs.

use tracing::warn;

use crate::Config;

/// Switches each `zakura.bootstrap_peers` entry that names a default seed by its
/// retired node ID to the seed's current ID, see
/// [`ZakuraConfig::replace_retired_seed_node_ids`], and warns about each one.
///
/// Zakura does this before the v2 endpoint reads the list, and after tracing starts so
/// the warnings reach the operator. It covers every config source, including
/// environment overrides that the config file rewrite cannot see.
///
/// [`ZakuraConfig::replace_retired_seed_node_ids`]: crate::zakura::ZakuraConfig::replace_retired_seed_node_ids
pub(super) fn replace_retired_seed_node_ids(config: &mut Config) {
    for (retired, current) in config.zakura.replace_retired_seed_node_ids() {
        warn!(
            %retired,
            %current,
            "zakura.bootstrap_peers names a default seed by the node ID it had before \
             p2p-v2/4; dialing its current ID instead. Update the entry in your config"
        );
    }
}
