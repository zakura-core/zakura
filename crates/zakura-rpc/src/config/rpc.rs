//! RPC config

use std::{
    net::{IpAddr, Ipv6Addr, SocketAddr},
    path::PathBuf,
};

use ipnet::IpNet;
use serde::{Deserialize, Serialize};

use zakura_chain::common::default_cache_dir;
use zakura_chain::parameters::NetworkKind;

const MAINNET_TRANSACTION_SUBMISSION_PORT: u16 = 8237;
const TESTNET_TRANSACTION_SUBMISSION_PORT: u16 = 18237;

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
    /// If you bind Zebra's RPC port to a public IP address,
    /// anyone on the internet can send transactions via your node.
    /// They can also query your node's state.
    pub listen_addr: Option<SocketAddr>,

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
    /// # Security
    ///
    /// If you bind Zebra's indexer RPC port to a public IP address,
    /// anyone on the internet can query your node's state.
    pub indexer_listen_addr: Option<SocketAddr>,

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

    /// Enable cookie-based authentication for RPCs.
    pub enable_cookie_auth: bool,

    /// The maximum size of the response body in bytes.
    pub max_response_body_size: usize,

    /// Optional TLS configuration for this RPC listener.
    pub tls: Option<TlsConfig>,

    /// Public transaction submission listener configuration.
    ///
    /// This listener exposes only `sendrawtransaction`. It is independent from
    /// [`Self::listen_addr`], so the full RPC interface can remain disabled.
    pub transaction_submission: TransactionSubmissionConfig,
}

/// Configuration for the public `sendrawtransaction` listener.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct TransactionSubmissionConfig {
    /// Whether the public transaction submission listener is enabled.
    pub enabled: bool,

    /// Optional listen address override.
    ///
    /// When unset, the listener binds to `[::]:8237` on Mainnet and
    /// `[::]:18237` on Testnet or Regtest.
    pub listen_addr: Option<SocketAddr>,

    /// Maximum HTTP requests per second across all clients.
    pub requests_per_second: u32,

    /// Maximum global rate limit burst.
    pub request_burst: u32,

    /// Maximum HTTP requests per minute from one IPv4 address or IPv6 /64.
    pub requests_per_minute_per_ip: u32,

    /// Maximum per-IP rate limit burst.
    pub request_burst_per_ip: u32,

    /// Maximum number of transaction submissions being verified at once.
    pub max_in_flight: usize,

    /// Maximum transaction submissions being verified for one IPv4 address or IPv6 /64.
    pub max_in_flight_per_ip: usize,

    /// Maximum number of open TCP connections.
    pub max_connections: usize,

    /// Maximum open TCP connections from one directly connected IPv4 address or IPv6 /64.
    pub max_connections_per_ip: usize,

    /// Proxy networks whose `X-Forwarded-For` headers are trusted.
    ///
    /// Leave this empty unless the listener is behind a trusted reverse proxy.
    pub trusted_proxies: Vec<IpNet>,

    /// Optional TLS configuration for this listener.
    pub tls: Option<TlsConfig>,
}

impl TransactionSubmissionConfig {
    /// Returns the configured address or the default address for `network`.
    pub(crate) fn resolved_listen_addr(&self, network: NetworkKind) -> SocketAddr {
        self.listen_addr.unwrap_or_else(|| {
            let port = match network {
                NetworkKind::Mainnet => MAINNET_TRANSACTION_SUBMISSION_PORT,
                NetworkKind::Testnet | NetworkKind::Regtest => TESTNET_TRANSACTION_SUBMISSION_PORT,
            };

            SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), port)
        })
    }

    /// Validates resource and rate limit settings before the listener starts.
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.requests_per_second == 0 {
            return Err(
                "rpc.transaction_submission.requests_per_second must be greater than zero"
                    .to_string(),
            );
        }
        if self.request_burst == 0 {
            return Err(
                "rpc.transaction_submission.request_burst must be greater than zero".to_string(),
            );
        }
        if self.requests_per_minute_per_ip == 0 {
            return Err(
                "rpc.transaction_submission.requests_per_minute_per_ip must be greater than zero"
                    .to_string(),
            );
        }
        if self.request_burst_per_ip == 0 {
            return Err(
                "rpc.transaction_submission.request_burst_per_ip must be greater than zero"
                    .to_string(),
            );
        }
        if u64::from(self.requests_per_minute_per_ip) > u64::from(self.requests_per_second) * 60 {
            return Err(
                "rpc.transaction_submission.requests_per_minute_per_ip must not exceed the global request rate"
                    .to_string(),
            );
        }
        if self.request_burst_per_ip > self.request_burst {
            return Err(
                "rpc.transaction_submission.request_burst_per_ip must not exceed request_burst"
                    .to_string(),
            );
        }
        if self.max_in_flight == 0 {
            return Err(
                "rpc.transaction_submission.max_in_flight must be greater than zero".to_string(),
            );
        }
        if self.max_in_flight > 500 {
            return Err("rpc.transaction_submission.max_in_flight must not exceed 500".to_string());
        }
        if self.max_in_flight_per_ip == 0 {
            return Err(
                "rpc.transaction_submission.max_in_flight_per_ip must be greater than zero"
                    .to_string(),
            );
        }
        if self.max_in_flight_per_ip > self.max_in_flight {
            return Err(
                "rpc.transaction_submission.max_in_flight_per_ip must not exceed max_in_flight"
                    .to_string(),
            );
        }
        if self.max_connections == 0 {
            return Err(
                "rpc.transaction_submission.max_connections must be greater than zero".to_string(),
            );
        }
        if self.max_connections > 100_000 {
            return Err(
                "rpc.transaction_submission.max_connections must not exceed 100000".to_string(),
            );
        }
        if self.max_connections_per_ip == 0 {
            return Err(
                "rpc.transaction_submission.max_connections_per_ip must be greater than zero"
                    .to_string(),
            );
        }
        if self.max_connections_per_ip > self.max_connections {
            return Err(
                "rpc.transaction_submission.max_connections_per_ip must not exceed max_connections"
                    .to_string(),
            );
        }
        if self.trusted_proxies.len() > 256 {
            return Err(
                "rpc.transaction_submission.trusted_proxies must not contain more than 256 networks"
                    .to_string(),
            );
        }
        if self
            .trusted_proxies
            .iter()
            .any(|network| network.prefix_len() == 0)
        {
            return Err(
                "rpc.transaction_submission.trusted_proxies must not trust every address"
                    .to_string(),
            );
        }

        Ok(())
    }
}

impl Default for TransactionSubmissionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            listen_addr: None,
            requests_per_second: 10,
            request_burst: 20,
            requests_per_minute_per_ip: 60,
            request_burst_per_ip: 4,
            max_in_flight: 16,
            max_in_flight_per_ip: 4,
            max_connections: 100,
            max_connections_per_ip: 20,
            trusted_proxies: Vec::new(),
            tls: None,
        }
    }
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

// This impl isn't derivable because it depends on features.
#[allow(clippy::derivable_impls)]
impl Default for Config {
    fn default() -> Self {
        Self {
            // Disable RPCs by default.
            listen_addr: None,

            // Disable indexer RPCs by default.
            indexer_listen_addr: None,

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

            transaction_submission: TransactionSubmissionConfig::default(),
        }
    }
}

fn default_cookie_file_name() -> String {
    ".cookie".to_string()
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv6Addr, SocketAddr};

    use zakura_chain::parameters::NetworkKind;

    use super::{Config, TransactionSubmissionConfig};

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
    fn transaction_submission_defaults_are_public_and_bounded() {
        let config = TransactionSubmissionConfig::default();

        assert!(config.enabled);
        assert_eq!(config.requests_per_minute_per_ip, 60);
        assert_eq!(config.request_burst_per_ip, 4);
        assert!(
            u64::from(config.requests_per_minute_per_ip)
                < u64::from(config.requests_per_second) * 60
        );
        assert!(config.request_burst_per_ip < config.request_burst);
        assert_eq!(
            config.resolved_listen_addr(NetworkKind::Mainnet),
            SocketAddr::new(Ipv6Addr::UNSPECIFIED.into(), 8237)
        );
        assert_eq!(
            config.resolved_listen_addr(NetworkKind::Testnet),
            SocketAddr::new(Ipv6Addr::UNSPECIFIED.into(), 18237)
        );
        config.validate().expect("default limits should be valid");
    }

    #[test]
    fn transaction_submission_rejects_unsafe_limits() {
        let config = TransactionSubmissionConfig {
            requests_per_second: 0,
            ..TransactionSubmissionConfig::default()
        };

        assert!(config.validate().is_err());

        let config = TransactionSubmissionConfig {
            requests_per_minute_per_ip: 601,
            ..TransactionSubmissionConfig::default()
        };

        assert!(config.validate().is_err());

        let config = TransactionSubmissionConfig {
            request_burst_per_ip: 21,
            ..TransactionSubmissionConfig::default()
        };

        assert!(config.validate().is_err());

        let config = TransactionSubmissionConfig {
            trusted_proxies: vec!["0.0.0.0/0".parse().expect("valid network")],
            ..TransactionSubmissionConfig::default()
        };

        assert!(config.validate().is_err());
    }
}
