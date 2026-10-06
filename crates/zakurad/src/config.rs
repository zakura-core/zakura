//! Zakurad Config
//!
//! See instructions in `commands.rs` to specify the path to your
//! application's configuration file and/or command-line options
//! for specifying it.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use zakura_rpc::config::mining::{default_miner_address, MinerAddressType};

use crate::{components::With, BoxError};

mod loader;

/// Centralized, case-insensitive suffix-based deny-list to ban setting config fields with
/// environment variables if those config field names end with any of these suffixes.
const DENY_CONFIG_KEY_SUFFIX_LIST: [&str; 5] = [
    "password",
    "secret",
    "token",
    // Block raw cookies only if a field is literally named "cookie".
    // (Paths like cookie_dir are not affected.)
    "cookie",
    // Only raw private keys; paths like *_private_key_path are not affected.
    "private_key",
];

/// Returns true if a leaf key name should be considered sensitive and blocked
/// from environment variable overrides.
fn is_sensitive_leaf_key(leaf_key: &str) -> bool {
    let key = leaf_key.to_ascii_lowercase();
    DENY_CONFIG_KEY_SUFFIX_LIST
        .iter()
        .any(|deny_suffix| key.ends_with(deny_suffix))
}

/// Configuration for `zakurad`.
///
/// The `zakurad` config is a TOML-encoded version of this structure. The meaning
/// of each field is described in the documentation, although it may be necessary
/// to click through to the sub-structures for each section.
///
/// The path to the configuration file can also be specified with the `--config` flag when running Zakura.
///
/// The default path to the `zakurad` config uses the platform's preferences
/// directory:
///
/// | Platform | Value                                 | Example                                        |
/// | -------- | ------------------------------------- | ---------------------------------------------- |
/// | Linux    | `$XDG_CONFIG_HOME` or `$HOME/.config` | `/home/alice/.config/zakura.toml`              |
/// | macOS    | `$HOME/Library/Preferences`           | `/Users/Alice/Library/Preferences/zakura.toml` |
/// | Windows  | `{FOLDERID_LocalAppData}`           | `C:\Users\Alice\AppData\Local\zakura.toml`     |
#[derive(Clone, Default, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct ZakuradConfig {
    /// Consensus configuration
    //
    // These configs use full paths to avoid a rustdoc link bug (#7048).
    pub consensus: zakura_consensus::config::Config,

    /// Metrics configuration
    pub metrics: crate::components::metrics::Config,

    /// Networking configuration
    pub network: zakura_network::config::Config,

    /// State configuration
    pub state: zakura_state::config::Config,

    /// Tracing configuration
    pub tracing: crate::components::tracing::Config,

    /// Sync configuration
    pub sync: crate::components::sync::Config,

    /// Mempool configuration
    pub mempool: crate::components::mempool::Config,

    /// RPC configuration
    pub rpc: zakura_rpc::config::rpc::Config,

    /// Mining configuration
    pub mining: zakura_rpc::config::mining::Config,

    /// Health check HTTP server configuration.
    ///
    /// See the Zebra Book for details and examples:
    /// <https://zebra.zfnd.org/user/health.html>
    pub health: crate::components::health::Config,

    /// zcashd-compat mode configuration.
    pub zcashd_compat: crate::components::zcashd_compat::Config,
}

impl ZakuradConfig {
    /// Loads the configuration from the conventional sources.
    ///
    /// Configuration is loaded from four sources, in order of precedence:
    /// 1. Environment variables with `ZAKURA_` prefix (highest precedence)
    /// 2. Environment variables with deprecated `ZEBRA_` prefix
    /// 3. TOML configuration file (if provided)
    /// 4. Hard-coded defaults (lowest precedence)
    ///
    /// Environment variables use the format `ZAKURA_SECTION__KEY` where:
    /// - `SECTION` is the configuration section (e.g., `network`, `rpc`)
    /// - `KEY` is the configuration key within that section
    /// - Double underscores (`__`) separate nested keys
    ///
    /// # Security
    ///
    /// Environment variables whose leaf key names end with sensitive suffixes (case-insensitive)
    /// will cause configuration loading to fail with an error: `password`, `secret`, `token`, `cookie`, `private_key`.
    /// This prevents both silent misconfigurations and process table exposure of sensitive values.
    ///
    /// See [`DENY_CONFIG_KEY_SUFFIX_LIST`] and [`is_sensitive_leaf_key()`] above
    ///
    /// # Platform behavior
    ///
    /// Prefix matching is case-sensitive; field names are lowercased.
    /// Environment values undergo legacy scalar coercions. Quotes and
    /// whitespace are retained; home and shell variable expressions are not
    /// expanded. Non-Unicode names and unrelated non-Unicode values are ignored.
    /// A matching variable with a non-Unicode value returns an error.
    ///
    /// # Examples
    /// - `ZAKURA_NETWORK__NETWORK=Testnet` sets `network.network = "Testnet"`
    /// - `ZAKURA_RPC__LISTEN_ADDR=127.0.0.1:8232` sets `rpc.listen_addr = "127.0.0.1:8232"`
    pub fn load(config_path: Option<PathBuf>) -> Result<Self, BoxError> {
        Self::load_with_env_prefixes(config_path, &["ZEBRA", "ZAKURA"])
    }

    /// Loads configuration using caller-provided environment variable prefixes.
    ///
    /// Prefixes are applied in order, so later prefixes override earlier prefixes.
    pub(crate) fn load_with_env_prefixes(
        config_path: Option<PathBuf>,
        env_prefixes: &[&str],
    ) -> Result<Self, BoxError> {
        loader::load(config_path, env_prefixes)
    }
}

impl With<MinerAddressType> for ZakuradConfig {
    fn with(mut self, miner_address_type: MinerAddressType) -> Self {
        self.mining.miner_address = Some(
            default_miner_address(self.network.network.kind(), &miner_address_type)
                .parse()
                .expect("valid hard-coded address"),
        );

        self
    }
}
