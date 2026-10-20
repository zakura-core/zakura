//! `generate` subcommand - generates a default `zakura.toml` config.

use crate::config::ZakuradConfig;
use clap::Parser;
use eyre::Report;

/// Generate a default `zakura.toml` configuration
#[derive(Debug, Default, Parser)]
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

impl GenerateCmd {
    /// `generate` sub-command entrypoint.
    ///
    /// # Panics
    ///
    /// If the output file can't be created or written.
    //
    // A file error panics, as it always has: the panic report names the failed step and
    // exits with status 101. Returning it would only exit with status 1, because the
    // caller turns errors into statuses without printing them.
    #[allow(clippy::print_stdout, clippy::unwrap_in_result)]
    pub(crate) fn run(&self) -> Result<(), Report> {
        let output = default_config_toml(&ZakuradConfig::default());
        match self.output_file {
            Some(ref output_file) => {
                use std::{fs::File, io::Write};
                File::create(output_file)
                    .expect("must be able to open output file")
                    .write_all(output.as_bytes())
                    .expect("must be able to write output");
            }
            None => {
                println!("{output}");
            }
        }

        Ok(())
    }
}

/// The generated config, written by hand so `zakurad` does not carry a TOML
/// serializer for this one command. A unit test checks it against
/// `ZakuradConfig::default()`.
const DEFAULT_CONFIG_TEMPLATE: &str = include_str!("default_config.toml");

/// Fill the template's platform-dependent default paths.
fn default_config_toml(config: &ZakuradConfig) -> String {
    [
        ("\"@NETWORK_IDENTITY_DIR@\"", &config.network.identity_dir),
        ("\"@RPC_COOKIE_DIR@\"", &config.rpc.cookie_dir),
        ("\"@STATE_CACHE_DIR@\"", &config.state.cache_dir),
    ]
    .into_iter()
    .fold(
        DEFAULT_CONFIG_TEMPLATE.to_owned(),
        |toml, (placeholder, path)| {
            toml.replace(placeholder, &toml_basic_string(&path.to_string_lossy()))
        },
    )
}

/// Quote `value` as a TOML basic string.
fn toml_basic_string(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('"');
    for character in value.chars() {
        match character {
            '"' => quoted.push_str("\\\""),
            '\\' => quoted.push_str("\\\\"),
            '\u{0}'..='\u{1f}' | '\u{7f}' => {
                quoted.push_str(&format!("\\u{:04X}", u32::from(character)))
            }
            character => quoted.push(character),
        }
    }
    quoted.push('"');
    quoted
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    /// Native sync tuning stays out of the generated skeleton; absent sections
    /// use their defaults from code.
    fn expected_default_config(config: &ZakuradConfig) -> toml::Value {
        let mut expected = toml::Value::try_from(config).expect("the default config serializes");
        let zakura = expected
            .get_mut("network")
            .and_then(|network| network.get_mut("zakura"))
            .and_then(toml::Value::as_table_mut)
            .expect("the default config has a native Zakura section");
        zakura.remove("block_sync");
        zakura.remove("header_sync");
        expected
    }

    #[test]
    fn template_matches_default_config() {
        let default_config = ZakuradConfig::default();
        let generated = default_config_toml(&default_config);
        let generated_value: toml::Value =
            toml::from_str(&generated).expect("the generated config is valid TOML");
        let expected = expected_default_config(&default_config);

        assert!(
            generated_value == expected,
            "crates/zakurad/src/commands/default_config.toml is out of date with \
             ZakuradConfig::default(). Update the changed values to match:\n{}",
            toml::to_string_pretty(&expected).expect("the expected config serializes"),
        );

        let parsed: ZakuradConfig =
            toml::from_str(&generated).expect("zakurad accepts the generated config");
        assert_eq!(
            parsed.network.zakura.block_sync,
            default_config.network.zakura.block_sync
        );
        assert_eq!(
            parsed.network.zakura.header_sync,
            default_config.network.zakura.header_sync
        );
    }

    #[test]
    fn template_paths_are_toml_escaped() {
        let mut config = ZakuradConfig::default();
        let path = PathBuf::from("C:\\Users\\\"Alice\"\n\u{7f}");
        config.network.identity_dir = path.clone();
        config.rpc.cookie_dir = path.clone();
        config.state.cache_dir = path.clone();

        let parsed: ZakuradConfig = toml::from_str(&default_config_toml(&config))
            .expect("escaped paths are valid TOML strings");

        assert_eq!(parsed.network.identity_dir, path);
        assert_eq!(parsed.rpc.cookie_dir, path);
        assert_eq!(parsed.state.cache_dir, path);
    }
}
