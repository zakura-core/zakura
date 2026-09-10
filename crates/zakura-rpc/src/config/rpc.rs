//! RPC config

use std::{net::SocketAddr, path::PathBuf};

use serde::{Deserialize, Serialize};

use zakura_chain::common::default_cache_dir;

/// RPC configuration section.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    /// IP address and port for the RPC server.
    ///
    /// Note: The RPC server is disabled by default.
    /// To enable the RPC server, set a listen address in the config:
    /// ```toml
    /// [rpc]
    /// listen_addr = '127.0.0.1:8232'
    /// ```
    ///
    /// The recommended ports for the RPC server are:
    /// - Mainnet: 127.0.0.1:8232
    /// - Testnet: 127.0.0.1:18232
    ///
    /// # Security
    ///
    /// On Mainnet and Testnet, disabling [`Self::enable_cookie_auth`] restricts
    /// this listener to the explicitly classified unauthenticated compatibility
    /// methods. Clients can still query chain state, submit transactions, and
    /// use the mining RPCs, but they cannot call administrative methods such as
    /// `invalidateblock` or `reconsiderblock`.
    /// Set [`Self::enable_cookie_auth`] when every method should require
    /// credentials.
    ///
    /// The restricted method set is defense in depth, not Internet hardening.
    /// Allowed methods can expose node metadata or consume significant
    /// resources. Keep unauthenticated RPC behind a firewall, private network,
    /// or gateway that limits access to trusted clients.
    ///
    /// Do not expose a cookie-authenticated listener over an untrusted plaintext
    /// network. HTTP Basic credentials are reusable; keep authenticated RPC on
    /// loopback or configure [`Self::tls`] on the primary listener.
    pub listen_addr: Option<SocketAddr>,

    /// Optional loopback-only listener for authenticated administrative RPCs.
    ///
    /// This listener exposes the full RPC method set and always requires the
    /// cookie in [`Self::cookie_dir`] and [`Self::cookie_file_name`]. It is a
    /// companion to a restricted unauthenticated [`Self::listen_addr`]:
    /// ```toml
    /// [rpc]
    /// listen_addr = '0.0.0.0:8232'
    /// enable_cookie_auth = false
    /// admin_listen_addr = '127.0.0.1:8231'
    /// ```
    ///
    /// The admin listener uses plaintext HTTP on loopback and does not inherit
    /// [`Self::tls`]. Use SSH or another secure local access mechanism to call
    /// it from a different machine. It cannot bind to a non-loopback address.
    pub admin_listen_addr: Option<SocketAddr>,

    /// IP address and port for the indexer RPC server.
    ///
    /// Note: The indexer RPC server is disabled by default.
    /// To enable the indexer RPC server, compile `zakurad` with the
    /// `indexer` feature flag and set a listen address in the config:
    /// ```toml
    /// [rpc]
    /// indexer_listen_addr = '127.0.0.1:8230'
    /// ```
    ///
    /// A non-loopback listener must configure mutual TLS:
    /// ```toml
    /// [rpc]
    /// indexer_listen_addr = '0.0.0.0:8230'
    ///
    /// [rpc.indexer_tls]
    /// cert_file = '/etc/zakura/indexer-server.pem'
    /// key_file = '/etc/zakura/indexer-server-key.pem'
    /// client_ca_file = '/etc/zakura/indexer-client-ca.pem'
    /// ```
    ///
    /// # Security
    ///
    /// Plaintext indexer RPC is restricted to loopback addresses. Configuring
    /// a non-loopback address requires [`Self::indexer_tls`].
    ///
    /// Loopback does not authenticate the peer: every process and user on the
    /// host can connect to the listener or impersonate it after it stops. Only
    /// use plaintext loopback between trusted processes on a single-tenant
    /// host. Configure [`Self::indexer_tls`] on multi-user or otherwise
    /// untrusted hosts, even when using a loopback address.
    pub indexer_listen_addr: Option<SocketAddr>,

    /// Mutual TLS configuration for the indexer RPC listener.
    ///
    /// This is required when [`Self::indexer_listen_addr`] is not a loopback
    /// address. Clients must present a certificate signed by `client_ca_file`.
    pub indexer_tls: Option<IndexerTlsConfig>,

    /// The number of threads used to process RPC requests and responses.
    ///
    /// This field is deprecated and could be removed in a future release.
    /// We keep it just for backward compatibility but it actually do nothing.
    /// It was something configurable when the RPC server was based in the jsonrpc-core crate,
    /// not anymore since we migrated to jsonrpsee.
    // TODO: Prefix this field name with an underscore so it's clear that it's now unused, and
    //       use serde(rename) to continue successfully deserializing old configs.
    pub parallel_cpu_threads: usize,

    /// Test-only option that makes Zebra say it is at the chain tip,
    /// no matter what the estimated height or local clock is.
    pub debug_force_finished_sync: bool,

    /// The directory where Zebra stores RPC cookies.
    pub cookie_dir: PathBuf,

    /// The cookie file name used in `cookie_dir`.
    #[serde(default = "default_cookie_file_name")]
    pub cookie_file_name: String,

    /// Enable cookie-based authentication for the primary RPC listener.
    ///
    /// Authenticated listeners expose the full RPC method set. On Mainnet and
    /// Testnet, unauthenticated listeners expose only the restricted
    /// compatibility method set. Regtest keeps the full method set so its local
    /// test-control RPCs remain available.
    pub enable_cookie_auth: bool,

    /// The maximum size of the response body in bytes.
    pub max_response_body_size: usize,

    /// Optional TLS configuration for the primary RPC listener.
    pub tls: Option<TlsConfig>,
}

/// TLS configuration for an RPC listener.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// PEM certificate chain served to RPC clients.
    pub cert_file: PathBuf,

    /// PEM private key for the served certificate.
    pub key_file: PathBuf,
}

/// Mutual TLS configuration for the indexer RPC listener.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerTlsConfig {
    /// PEM certificate chain served to indexer RPC clients.
    pub cert_file: PathBuf,

    /// PEM private key for the indexer RPC server certificate.
    pub key_file: PathBuf,

    /// PEM certificate authority used to authenticate indexer RPC clients.
    pub client_ca_file: PathBuf,
}

// This impl isn't derivable because it depends on features.
#[allow(clippy::derivable_impls)]
impl Default for Config {
    fn default() -> Self {
        Self {
            // Disable RPCs by default.
            listen_addr: None,
            admin_listen_addr: None,

            // Disable indexer RPCs by default.
            indexer_listen_addr: None,
            indexer_tls: None,

            // Use multiple threads, because we pause requests during getblocktemplate long polling
            parallel_cpu_threads: 0,

            // Debug options are always off by default.
            debug_force_finished_sync: false,

            // Use the default cache dir for the auth cookie.
            cookie_dir: default_cache_dir(),
            cookie_file_name: default_cookie_file_name(),

            // Enable cookie-based authentication by default.
            enable_cookie_auth: true,

            // 50 MiB
            max_response_body_size: 52_428_800,

            // Serve plain HTTP unless a caller wires TLS explicitly.
            tls: None,
        }
    }
}

impl Config {
    /// Validates the relationship between the primary and admin RPC listeners.
    pub fn validate(&self) -> Result<(), String> {
        let Some(admin_listen_addr) = self.admin_listen_addr else {
            return Ok(());
        };

        if !admin_listen_addr.ip().is_loopback() {
            return Err("rpc.admin_listen_addr must use a loopback address".to_string());
        }

        let Some(listen_addr) = self.listen_addr else {
            return Err(
                "rpc.admin_listen_addr requires rpc.listen_addr; use rpc.listen_addr with cookie authentication for an admin-only server"
                    .to_string(),
            );
        };

        if self.enable_cookie_auth {
            return Err(
                "rpc.admin_listen_addr requires rpc.enable_cookie_auth = false; an authenticated primary listener already exposes the full RPC method set"
                    .to_string(),
            );
        }

        if admin_listen_addr.port() != 0 && admin_listen_addr.port() == listen_addr.port() {
            return Err(
                "rpc.admin_listen_addr must use a different port from rpc.listen_addr".to_string(),
            );
        }

        Ok(())
    }
}

fn default_cookie_file_name() -> String {
    ".cookie".to_string()
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::Config;

    #[test]
    fn deserialize_defaults_cookie_file_name_when_missing() {
        let config: Config = toml::from_str(
            r#"
            listen_addr = "127.0.0.1:8232"
            "#,
        )
        .expect("partial rpc config should deserialize");

        assert_eq!(
            config.cookie_file_name,
            super::default_cookie_file_name(),
            "missing cookie file names should use the default value"
        );
    }

    #[test]
    fn validates_segmented_rpc_listeners() {
        let config: Config = toml::from_str(
            r#"
            listen_addr = "0.0.0.0:8232"
            admin_listen_addr = "127.0.0.1:8231"
            enable_cookie_auth = false
            "#,
        )
        .expect("segmented RPC config should deserialize");

        config
            .validate()
            .expect("a loopback admin listener should be valid");
    }

    #[test]
    fn rejects_non_loopback_admin_listener() {
        let config: Config = toml::from_str(
            r#"
            listen_addr = "0.0.0.0:8232"
            admin_listen_addr = "0.0.0.0:8231"
            enable_cookie_auth = false
            "#,
        )
        .expect("RPC config should deserialize before validation");

        assert_eq!(
            config.validate(),
            Err("rpc.admin_listen_addr must use a loopback address".to_string())
        );
    }

    #[test]
    fn rejects_redundant_admin_listener_for_authenticated_rpc() {
        let config: Config = toml::from_str(
            r#"
            listen_addr = "127.0.0.1:8232"
            admin_listen_addr = "127.0.0.1:8231"
            enable_cookie_auth = true
            "#,
        )
        .expect("RPC config should deserialize before validation");

        assert!(config
            .validate()
            .expect_err("an authenticated primary listener makes the admin listener redundant")
            .contains("enable_cookie_auth = false"));
    }

    #[test]
    fn rejects_admin_port_overlapped_by_primary_wildcard_listener() {
        let config: Config = toml::from_str(
            r#"
            listen_addr = "0.0.0.0:8232"
            admin_listen_addr = "127.0.0.1:8232"
            enable_cookie_auth = false
            "#,
        )
        .expect("RPC config should deserialize before validation");

        assert!(config
            .validate()
            .expect_err("a wildcard primary listener would overlap the admin listener")
            .contains("different port"));
    }

    #[test]
    fn deserializes_indexer_mtls_config() {
        let config: Config = toml::from_str(
            r#"
            indexer_listen_addr = "0.0.0.0:8230"

            [indexer_tls]
            cert_file = "/etc/zakura/indexer-server.pem"
            key_file = "/etc/zakura/indexer-server-key.pem"
            client_ca_file = "/etc/zakura/indexer-client-ca.pem"
            "#,
        )
        .expect("indexer mTLS config should deserialize");

        let tls = config
            .indexer_tls
            .expect("the configured indexer mTLS settings should be present");
        assert_eq!(
            tls.client_ca_file,
            PathBuf::from("/etc/zakura/indexer-client-ca.pem")
        );
    }
}
