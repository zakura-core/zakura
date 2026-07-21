//! `generate` subcommand - generates a default `zakura.toml` config.

use crate::config::ZakuradConfig;
use abscissa_core::{Command, Runnable};
use clap::Parser;
use zakura_chain::common::atomic_write;

/// Generate a default `zakura.toml` configuration
#[derive(Command, Debug, Default, Parser)]
pub struct GenerateCmd {
    /// The file to write the generated config to.
    //
    // TODO: use PathBuf here instead, to support non-UTF-8 paths
    #[clap(
        long,
        short,
        help = "The file to write the generated config to (stdout if unspecified)"
    )]
    output_file: Option<String>,
}

impl Runnable for GenerateCmd {
    /// Start the application.
    #[allow(clippy::print_stdout)]
    fn run(&self) {
        let default_config = ZakuradConfig::default();
        let mut output = r"# Default configuration for zakurad.
#
# This file can be used as a skeleton for custom configs.
#
# Unspecified fields use default values. Optional fields are Some(field) if the
# field is present and None if it is absent.
#
# This file is generated as an example using zakurad's current defaults.
# You should set only the config options you want to keep, and delete the rest.
# Only a subset of fields are present in the skeleton, since optional values
# whose default is None are omitted.
#
# The config format (including a complete list of sections and fields) is
# documented here:
# https://docs.rs/zakura/latest/zakurad/config/struct.ZakuradConfig.html
#
# CONFIGURATION SOURCES (in order of precedence, highest to lowest):
#
# 1. Environment variables with ZAKURA_ prefix (highest precedence)
#    - Format: ZAKURA_SECTION__KEY (double underscore for nested keys)
#    - Examples:
#      - ZAKURA_NETWORK__NETWORK=Testnet
#      - ZAKURA_RPC__LISTEN_ADDR=127.0.0.1:8232
#      - ZAKURA_STATE__CACHE_DIR=/path/to/cache
#      - ZAKURA_TRACING__FILTER=debug
#      - ZAKURA_METRICS__ENDPOINT_ADDR=0.0.0.0:9999
#
# 2. Environment variables with deprecated ZEBRA_ prefix
#
# 3. Configuration file (TOML format)
#    - At the path specified via -c flag, e.g. `zakurad -c myconfig.toml start`, or
#    - At the default path in the user's preference directory (platform-dependent, see below)
#
# 4. Hard-coded defaults (lowest precedence)
#
# The user's preference directory and the default path to the `zakurad` config are platform dependent,
# based on `dirs::preference_dir`, see https://docs.rs/dirs/latest/dirs/fn.preference_dir.html :
#
# | Platform | Value                                 | Example                                        |
# | -------- | ------------------------------------- | ---------------------------------------------- |
# | Linux    | `$XDG_CONFIG_HOME` or `$HOME/.config` | `/home/alice/.config/zakura.toml`              |
# | macOS    | `$HOME/Library/Preferences`           | `/Users/Alice/Library/Preferences/zakura.toml` |
# | Windows  | `{FOLDERID_RoamingAppData}`           | `C:\Users\Alice\AppData\Local\zakura.toml`     |

"
        .to_owned();

        // this avoids a ValueAfterTable error
        // https://github.com/alexcrichton/toml-rs/issues/145
        let mut conf = toml::Value::try_from(default_config).unwrap();
        remove_experimental_sync_config(&mut conf);
        let conf = toml::to_string_pretty(&conf).expect("default config should be serializable");
        output += &document_network_p2p_config(&conf);
        match self.output_file {
            Some(ref output_file) => {
                atomic_write(output_file.as_str().into(), output.as_bytes())
                    .expect("must be able to write output atomically")
                    .expect("must be able to replace output file atomically");
            }
            None => {
                println!("{output}");
            }
        }
    }
}

/// Omit unstable native sync tuning knobs from the generated config skeleton.
///
/// The fields remain deserializable for advanced overrides, while absent
/// sections use their defaults from code.
fn remove_experimental_sync_config(config: &mut toml::Value) {
    let Some(zakura) = config
        .get_mut("network")
        .and_then(toml::Value::as_table_mut)
        .and_then(|network| network.get_mut("zakura"))
        .and_then(toml::Value::as_table_mut)
    else {
        return;
    };

    zakura.remove("block_sync");
    zakura.remove("header_sync");
}

fn document_network_p2p_config(config: &str) -> String {
    let had_trailing_newline = config.ends_with('\n');
    let mut lines = config.lines().map(ToString::to_string).collect::<Vec<_>>();

    let Some(network_start) = lines.iter().position(|line| line == "[network]") else {
        return config.to_string();
    };
    let network_end = lines
        .iter()
        .enumerate()
        .skip(network_start + 1)
        .find_map(|(index, line)| line.starts_with('[').then_some(index))
        .unwrap_or(lines.len());

    let Some(p2p_stack_index) = lines[network_start + 1..network_end]
        .iter()
        .position(|line| line.starts_with("p2p_stack = "))
        .map(|index| index + network_start + 1)
    else {
        return config.to_string();
    };

    let comments = [
        "# The peer-to-peer stack to run:",
        "# - \"legacy\": the legacy TCP Zcash P2P stack only.",
        "# - \"zakura\": the experimental native Zakura P2P v2 stack only.",
        "# - \"dual\": both stacks, enabling experimental v2 with legacy fallback.",
        "# - \"default\": Zakura's default for this network, which can change between",
        "#   releases. Currently \"legacy\" on Mainnet, and \"dual\" everywhere else.",
    ]
    .map(ToString::to_string);

    lines.splice(p2p_stack_index..p2p_stack_index, comments);

    let mut output = lines.join("\n");
    if had_trailing_newline {
        output.push('\n');
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_config_omits_experimental_sync_config_and_uses_defaults() {
        let default_config = ZakuradConfig::default();
        let mut config = toml::Value::try_from(&default_config).unwrap();

        remove_experimental_sync_config(&mut config);

        let zakura = config
            .get("network")
            .and_then(|network| network.get("zakura"))
            .expect("default config contains the native Zakura section");
        assert!(zakura.get("block_sync").is_none());
        assert!(zakura.get("header_sync").is_none());
        assert!(zakura.get("bootstrap_peers").is_some());

        let config = toml::to_string_pretty(&config).unwrap();
        let parsed: ZakuradConfig = toml::from_str(&config).unwrap();
        assert_eq!(
            parsed.network.zakura.block_sync,
            default_config.network.zakura.block_sync
        );
        assert_eq!(
            parsed.network.zakura.header_sync,
            default_config.network.zakura.header_sync
        );
    }
}
