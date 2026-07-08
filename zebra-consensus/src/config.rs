//! Configuration for semantic verification which is run in parallel.

use serde::{Deserialize, Serialize};

/// Configuration for parallel semantic verification:
/// <https://zebra.zfnd.org/dev/rfcs/0002-parallel-verification.html#definitions>
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(
    deny_unknown_fields,
    default,
    from = "InnerConfig",
    into = "InnerConfig"
)]
pub struct Config {
    /// Should Zebra make sure that it follows the consensus chain while syncing?
    /// This is a developer-only option.
    ///
    /// # Security
    ///
    /// Disabling this option leaves your node vulnerable to some kinds of chain-based attacks.
    /// Zebra regularly updates its checkpoints to ensure nodes are following the best chain.
    ///
    /// # Details
    ///
    /// This option is `true` by default, because it prevents some kinds of chain attacks.
    ///
    /// Disabling this option makes Zebra start full validation earlier.
    /// It is slower and less secure.
    /// To keep checkpoint sync enabled but opt out of the initial VCT fast-sync rollout, set
    /// [`vct_fast_sync`](Self::vct_fast_sync) to `false`.
    ///
    /// Zebra requires some checkpoints to simplify validation of legacy network upgrades.
    /// Required checkpoints are always active, even when this option is `false`.
    ///
    /// # Deprecation
    ///
    /// For security reasons, this option might be deprecated or ignored in a future Zebra
    /// release.
    pub checkpoint_sync: bool,

    /// Use the verified-commitment-trees fast sync path.
    ///
    /// Unset (the default) means enabled: checkpoint sync folds in verified
    /// Sapling/Orchard/Ironwood roots and skips the per-block tree recompute on networks with
    /// embedded handoff frontiers. Set to `false` to keep
    /// [`checkpoint_sync`](Self::checkpoint_sync) enabled while forcing the legacy per-block
    /// recompute in both Archive and Pruned storage modes.
    ///
    /// The fast path only runs under `checkpoint_sync = true`; when checkpoint sync is disabled
    /// this option is unused, and *explicitly* setting it to `true` is rejected at startup.
    pub vct_fast_sync: Option<bool>,
}

impl Config {
    /// Whether the verified-commitment-trees fast sync knob is enabled
    /// (`true` unless explicitly disabled).
    pub fn vct_fast_sync_enabled(&self) -> bool {
        self.vct_fast_sync.unwrap_or(true)
    }

    /// Validate relationships between consensus configuration options.
    pub fn validate(&self) -> Result<(), &'static str> {
        if !self.checkpoint_sync && self.vct_fast_sync == Some(true) {
            return Err("consensus.vct_fast_sync = true requires consensus.checkpoint_sync = true");
        }

        Ok(())
    }
}

impl From<InnerConfig> for Config {
    fn from(
        InnerConfig {
            checkpoint_sync,
            vct_fast_sync,
            ..
        }: InnerConfig,
    ) -> Self {
        Self {
            checkpoint_sync,
            vct_fast_sync,
        }
    }
}

impl From<Config> for InnerConfig {
    fn from(
        Config {
            checkpoint_sync,
            vct_fast_sync,
        }: Config,
    ) -> Self {
        Self {
            checkpoint_sync,
            vct_fast_sync,
            _debug_skip_parameter_preload: false,
        }
    }
}

/// Inner consensus configuration for backwards compatibility with older `zakura.toml` files,
/// which contain fields that have been removed.
///
/// Rust API callers should use [`Config`].
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct InnerConfig {
    /// See [`Config`] for more details.
    pub checkpoint_sync: bool,

    /// See [`Config`] for more details.
    ///
    /// Serialized only when explicitly set, so configs written by this version stay readable by
    /// zebrad versions that predate the option.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vct_fast_sync: Option<bool>,

    #[serde(skip_serializing, rename = "debug_skip_parameter_preload")]
    /// Unused config field for backwards compatibility.
    pub _debug_skip_parameter_preload: bool,
}

// we like our default configs to be explicit
#[allow(unknown_lints)]
#[allow(clippy::derivable_impls)]
impl Default for Config {
    fn default() -> Self {
        Self {
            checkpoint_sync: true,
            vct_fast_sync: None,
        }
    }
}

impl Default for InnerConfig {
    fn default() -> Self {
        Self {
            checkpoint_sync: Config::default().checkpoint_sync,
            vct_fast_sync: Config::default().vct_fast_sync,
            _debug_skip_parameter_preload: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vct_fast_sync_defaults_enabled_and_converts_through_inner_config() {
        assert!(Config::default().vct_fast_sync_enabled());
        assert_eq!(Config::default().vct_fast_sync, None);

        let force_disabled = Config::from(InnerConfig {
            checkpoint_sync: true,
            vct_fast_sync: Some(false),
            _debug_skip_parameter_preload: false,
        });

        assert!(force_disabled.checkpoint_sync);
        assert!(!force_disabled.vct_fast_sync_enabled());

        let inner = InnerConfig::from(force_disabled);
        assert_eq!(inner.vct_fast_sync, Some(false));
    }

    #[test]
    fn vct_fast_sync_requires_checkpoint_sync_only_when_explicit() {
        let valid_default = Config {
            checkpoint_sync: true,
            vct_fast_sync: None,
        };
        assert!(valid_default.validate().is_ok());

        let valid_explicit = Config {
            checkpoint_sync: true,
            vct_fast_sync: Some(true),
        };
        assert!(valid_explicit.validate().is_ok());

        let valid_legacy_recompute = Config {
            checkpoint_sync: true,
            vct_fast_sync: Some(false),
        };
        assert!(valid_legacy_recompute.validate().is_ok());

        // A pre-VCT config that disables checkpoint sync must keep working: the
        // unset option defaults to enabled but is not a contradiction.
        let valid_full_verification = Config {
            checkpoint_sync: false,
            vct_fast_sync: None,
        };
        assert!(valid_full_verification.validate().is_ok());

        let valid_full_verification_explicit_off = Config {
            checkpoint_sync: false,
            vct_fast_sync: Some(false),
        };
        assert!(valid_full_verification_explicit_off.validate().is_ok());

        let invalid = Config {
            checkpoint_sync: false,
            vct_fast_sync: Some(true),
        };
        assert_eq!(
            invalid.validate(),
            Err("consensus.vct_fast_sync = true requires consensus.checkpoint_sync = true")
        );
    }

    #[test]
    fn unset_vct_fast_sync_is_not_serialized() {
        let serialized =
            toml::to_string(&Config::default()).expect("default config serializes to TOML");
        assert!(
            !serialized.contains("vct_fast_sync"),
            "unset vct_fast_sync must not appear in generated configs: {serialized}"
        );

        let round_trip: Config =
            toml::from_str(&serialized).expect("serialized config deserializes");
        assert_eq!(round_trip, Config::default());

        let explicit: Config =
            toml::from_str("vct_fast_sync = false\n").expect("explicit vct_fast_sync deserializes");
        assert_eq!(explicit.vct_fast_sync, Some(false));
        assert!(!explicit.vct_fast_sync_enabled());
    }
}
