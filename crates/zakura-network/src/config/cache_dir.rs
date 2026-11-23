//! Cache directory configuration for zakura-network.

use std::path::{Path, PathBuf};

use zakura_chain::{
    common::{default_cache_dir, home_dir},
    parameters::Network,
};

/// The directory, relative to the user's home directory, where Zebra stores
/// long-term network identity secrets.
const DEFAULT_NETWORK_IDENTITY_DIR: &str = ".zakura";

/// A cache directory config field.
///
/// This cache directory configuration field is optional.
/// It defaults to being enabled with the default config path,
/// but also allows a custom path to be set.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(untagged)]
pub enum CacheDir {
    /// Whether the cache directory is enabled with the default path (`true`),
    /// or disabled (`false`).
    IsEnabled(bool),

    /// Enable the cache directory and use a custom path.
    CustomPath(PathBuf),
}

impl<'de> serde::Deserialize<'de> for CacheDir {
    /// Dispatches on the value's type instead of buffering it like `#[serde(untagged)]`.
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct CacheDirVisitor;

        impl serde::de::Visitor<'_> for CacheDirVisitor {
            type Value = CacheDir;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a boolean or a cache directory path")
            }

            fn visit_bool<E: serde::de::Error>(self, enabled: bool) -> Result<CacheDir, E> {
                Ok(CacheDir::IsEnabled(enabled))
            }

            fn visit_str<E: serde::de::Error>(self, path: &str) -> Result<CacheDir, E> {
                Ok(CacheDir::CustomPath(path.into()))
            }
        }

        deserializer.deserialize_any(CacheDirVisitor)
    }
}

impl From<bool> for CacheDir {
    fn from(value: bool) -> Self {
        CacheDir::IsEnabled(value)
    }
}

impl From<PathBuf> for CacheDir {
    fn from(value: PathBuf) -> Self {
        CacheDir::CustomPath(value)
    }
}

impl CacheDir {
    /// Returns a `CacheDir` enabled with the default path.
    pub fn default_path() -> Self {
        Self::IsEnabled(true)
    }

    /// Returns a disabled `CacheDir`.
    pub fn disabled() -> Self {
        Self::IsEnabled(false)
    }

    /// Returns a custom `CacheDir` enabled with `path`.
    pub fn custom_path(path: impl AsRef<Path>) -> Self {
        Self::CustomPath(path.as_ref().to_owned())
    }

    /// Returns `true` if this `CacheDir` is enabled with the default or a custom path.
    pub fn is_enabled(&self) -> bool {
        match self {
            CacheDir::IsEnabled(is_enabled) => *is_enabled,
            CacheDir::CustomPath(_) => true,
        }
    }

    /// Returns the peer cache file path for `network`, if enabled.
    pub fn peer_cache_file_path(&self, network: &Network) -> Option<PathBuf> {
        Some(
            self.cache_dir()?
                .join("network")
                .join(format!("{}.peers", network.lowercase_name())),
        )
    }

    /// Returns this node's native discovery cache file when peer caching is enabled.
    pub(crate) fn discovery_book_file_path(
        &self,
        network: &Network,
        node_id: &zakura_quic::NodeId,
    ) -> Option<PathBuf> {
        Some(self.cache_dir()?.join("network").join(format!(
            "{}.{node_id}.zakura-discovery-book",
            network.lowercase_name()
        )))
    }

    /// Returns the default persistent Zakura iroh secret-key file path for `network`.
    ///
    /// This path is intentionally independent from the peer cache directory so
    /// state or cache snapshots do not clone a node's long-term iroh identity.
    pub fn zakura_node_secret_key_file_path(&self, network: &Network) -> PathBuf {
        zakura_node_secret_key_file_path(&default_network_identity_dir(), network)
    }

    /// Returns the `zakura-network` base cache directory, if enabled.
    pub fn cache_dir(&self) -> Option<PathBuf> {
        match self {
            Self::IsEnabled(is_enabled) => is_enabled.then(default_cache_dir),
            Self::CustomPath(cache_dir) => Some(cache_dir.to_owned()),
        }
    }
}

impl Default for CacheDir {
    fn default() -> Self {
        Self::default_path()
    }
}

/// Returns the default directory for network identity secrets.
pub(crate) fn default_network_identity_dir() -> PathBuf {
    home_dir()
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")))
        .join(DEFAULT_NETWORK_IDENTITY_DIR)
}

/// Returns the persistent Zakura iroh secret-key file path for `network` under
/// `identity_dir`.
pub(crate) fn zakura_node_secret_key_file_path(identity_dir: &Path, network: &Network) -> PathBuf {
    identity_dir.join(format!(
        "{}.zakura-iroh-secret-key",
        network.lowercase_name()
    ))
}

/// Returns the file under `identity_dir` that holds the last discovery
/// self-record sequence that `node_id` authored on `network`. The name
/// carries the node ID because the sequence belongs to that identity: nodes
/// with configured keys can share one `identity_dir`.
pub(crate) fn zakura_discovery_sequence_file_path(
    identity_dir: &Path,
    network: &Network,
    node_id: &zakura_quic::NodeId,
) -> PathBuf {
    identity_dir.join(format!(
        "{}.{node_id}.zakura-discovery-sequence",
        network.lowercase_name()
    ))
}

/// Moves the discovery sequence file that an older release wrote for the
/// node with the Ed25519 key `key` to the name this release reads, the one
/// carrying `node_id`. Returns whether it moved a file.
///
/// Older releases named a node by its key, so their file carries the key's
/// hex. The move happens only when the old file exists and the new one does
/// not, so a second start changes nothing and a file this release wrote is
/// never replaced.
pub(crate) fn move_legacy_zakura_discovery_sequence_file(
    identity_dir: &Path,
    network: &Network,
    key: &zakura_quic::NodeKey,
    node_id: &zakura_quic::NodeId,
) -> std::io::Result<bool> {
    let legacy = identity_dir.join(format!(
        "{}.{key}.zakura-discovery-sequence",
        network.lowercase_name()
    ));
    let current = zakura_discovery_sequence_file_path(identity_dir, network, node_id);
    if current.exists() || !legacy.exists() {
        return Ok(false);
    }
    std::fs::rename(&legacy, &current)?;
    Ok(true)
}
