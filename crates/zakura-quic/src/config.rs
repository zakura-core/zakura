//! Transport configuration (SPEC §9) and its mapping to noq.
//!
//! Only this module calls noq config setters (CTRL-0).

use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use noq::{
    congestion::{Bbr3Config, CubicConfig, NewRenoConfig},
    AckFrequencyConfig, IdleTimeout, MtuDiscoveryConfig, VarInt,
};
use serde::{Deserialize, Serialize};

const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;
const GIB: u64 = 1024 * MIB;

/// Multipath path budget, matching Iroh 1.1 (CTRL-30, WIRE-3).
pub const MAX_MULTIPATH_PATHS: u32 = 8;
/// Default handshake deadline in seconds (CTRL-17).
pub const DEFAULT_HANDSHAKE_TIMEOUT_SECS: u32 = 10;
/// Default pending handshakes above which unvalidated sources get a Retry (CTRL-22).
pub const DEFAULT_RETRY_THRESHOLD: u32 = 8;

/// Congestion controller choice (CTRL-3).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CongestionController {
    /// CUBIC, noq's and today's default.
    #[default]
    Cubic,
    /// NewReno.
    NewReno,
    /// BBRv3. Changing the default needs a fleet A/B (SPEC §17, P1).
    Bbr3,
}

/// `[network.zakura.quic]`: every QUIC setting Zakura controls (SPEC §9.1).
///
/// Defaults reproduce the Iroh backend's behavior, except the handshake
/// deadline (CTRL-17) and the Retry threshold (CTRL-22) (CTRL-0).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct QuicConfig {
    /// Requested `SO_RCVBUF` in bytes (CTRL-1).
    pub recv_buffer_bytes: u32,
    /// Requested `SO_SNDBUF` in bytes (CTRL-2).
    pub send_buffer_bytes: u32,
    /// Congestion controller (CTRL-3).
    pub congestion_controller: CongestionController,
    /// Initial congestion window in bytes; unset keeps the controller's default (CTRL-4).
    pub initial_window_bytes: Option<u64>,
    /// Per-stream receive window in bytes (CTRL-5).
    pub stream_receive_window_bytes: u32,
    /// Connection receive window in bytes (CTRL-6).
    pub receive_window_bytes: u32,
    /// Connection send window in bytes (CTRL-7).
    pub send_window_bytes: u64,
    /// Per-connection send-rate cap in bytes per second; unset means no cap (CTRL-8).
    pub max_send_rate_bytes_per_second: Option<u64>,
    /// Negotiate the ACK-frequency extension (CTRL-9, WIRE-9).
    pub ack_frequency: bool,
    /// Connection idle timeout in seconds (CTRL-10).
    pub idle_timeout_secs: u32,
    /// Connection keepalive interval in seconds (CTRL-11).
    pub keep_alive_interval_secs: u32,
    /// Generic segmentation and receive offload (CTRL-12).
    pub gso: bool,
    /// Initial MTU in bytes (CTRL-13).
    pub initial_mtu: u16,
    /// Path MTU discovery (CTRL-14).
    pub mtu_discovery: bool,
    /// Per-path keepalive interval in seconds (CTRL-15).
    pub path_keep_alive_interval_secs: u32,
    /// Per-path idle timeout in seconds (CTRL-16).
    pub path_idle_timeout_secs: u32,
    /// Deadline for a QUIC handshake in seconds, 10 by default; unset means
    /// only the idle timeout bounds it (CTRL-17).
    pub handshake_timeout_secs: Option<u32>,
    /// Maximum connection attempts waiting for a decision (CTRL-18).
    pub max_incoming: u32,
    /// Bytes buffered per connection attempt before a decision (CTRL-19).
    pub incoming_buffer_bytes: u64,
    /// Bytes buffered across all connection attempts (CTRL-20).
    pub incoming_buffer_total_bytes: u64,
    /// Handshakes in progress allowed per source IP; unset means no limit (CTRL-21).
    pub max_pending_per_ip: Option<u32>,
    /// Pending handshakes at which unvalidated sources get a Retry, 8 by
    /// default; unset means never (CTRL-22).
    pub retry_threshold: Option<u32>,
    /// Delay between dial attempts to a node's addresses (CTRL-23).
    pub dial_stagger_ms: u32,
    /// Kernel drop counter poll interval in seconds, Linux only (CTRL-24).
    pub kernel_drop_poll_secs: u32,
    /// Directory for per-connection qlog files; needs the `qlog` feature (CTRL-25).
    pub qlog_dir: Option<PathBuf>,
}

impl Default for QuicConfig {
    fn default() -> Self {
        Self {
            recv_buffer_bytes: 7 * MIB as u32,
            send_buffer_bytes: 7 * MIB as u32,
            congestion_controller: CongestionController::Cubic,
            initial_window_bytes: None,
            stream_receive_window_bytes: 16 * MIB as u32,
            receive_window_bytes: 32 * MIB as u32,
            send_window_bytes: 32 * MIB,
            max_send_rate_bytes_per_second: None,
            ack_frequency: false,
            idle_timeout_secs: 150,
            keep_alive_interval_secs: 10,
            gso: true,
            initial_mtu: 1200,
            mtu_discovery: true,
            path_keep_alive_interval_secs: 5,
            path_idle_timeout_secs: 15,
            handshake_timeout_secs: Some(DEFAULT_HANDSHAKE_TIMEOUT_SECS),
            max_incoming: 65_536,
            incoming_buffer_bytes: 10 * MIB,
            incoming_buffer_total_bytes: 100 * MIB,
            max_pending_per_ip: None,
            retry_threshold: Some(DEFAULT_RETRY_THRESHOLD),
            dial_stagger_ms: 250,
            kernel_drop_poll_secs: 10,
            qlog_dir: None,
        }
    }
}

/// A configuration value is out of range (CTRL-0).
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("invalid [network.zakura.quic] {key}: {reason}")]
pub struct ConfigError {
    /// The failing key.
    pub key: &'static str,
    /// Why the value is invalid.
    pub reason: String,
}

fn check<T: PartialOrd + std::fmt::Display + Copy>(
    key: &'static str,
    value: T,
    min: T,
    max: T,
) -> Result<(), ConfigError> {
    if value < min || value > max {
        return Err(ConfigError {
            key,
            reason: format!("{value} is outside {min}..={max}"),
        });
    }
    Ok(())
}

impl QuicConfig {
    /// Checks every range in SPEC §9.1.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let buffer = (64 * KIB as u32, GIB as u32);
        check(
            "recv_buffer_bytes",
            self.recv_buffer_bytes,
            buffer.0,
            buffer.1,
        )?;
        check(
            "send_buffer_bytes",
            self.send_buffer_bytes,
            buffer.0,
            buffer.1,
        )?;
        if let Some(window) = self.initial_window_bytes {
            check("initial_window_bytes", window, 14_720, 16 * MIB)?;
        }
        check(
            "stream_receive_window_bytes",
            u64::from(self.stream_receive_window_bytes),
            64 * KIB,
            256 * MIB,
        )?;
        check(
            "receive_window_bytes",
            u64::from(self.receive_window_bytes),
            u64::from(self.stream_receive_window_bytes),
            GIB,
        )?;
        check("send_window_bytes", self.send_window_bytes, 64 * KIB, GIB)?;
        if let Some(rate) = self.max_send_rate_bytes_per_second {
            check("max_send_rate_bytes_per_second", rate, 125_000, u64::MAX)?;
        }
        check("idle_timeout_secs", self.idle_timeout_secs, 30, 600)?;
        check(
            "keep_alive_interval_secs",
            self.keep_alive_interval_secs,
            1,
            60,
        )?;
        if self.idle_timeout_secs <= self.keep_alive_interval_secs.saturating_mul(3) {
            return Err(ConfigError {
                key: "idle_timeout_secs",
                reason: "must exceed keep_alive_interval_secs × 3".into(),
            });
        }
        check("initial_mtu", self.initial_mtu, 1200, 1500)?;
        check(
            "path_keep_alive_interval_secs",
            self.path_keep_alive_interval_secs,
            1,
            60,
        )?;
        check(
            "path_idle_timeout_secs",
            self.path_idle_timeout_secs,
            5,
            self.idle_timeout_secs,
        )?;
        if let Some(timeout) = self.handshake_timeout_secs {
            check("handshake_timeout_secs", timeout, 2, 60)?;
        }
        check("max_incoming", self.max_incoming, 16, 65_536)?;
        check(
            "incoming_buffer_bytes",
            self.incoming_buffer_bytes,
            4096,
            10 * MIB,
        )?;
        check(
            "incoming_buffer_total_bytes",
            self.incoming_buffer_total_bytes,
            self.incoming_buffer_bytes,
            100 * MIB,
        )?;
        if let Some(limit) = self.max_pending_per_ip {
            check("max_pending_per_ip", limit, 1, 64)?;
        }
        if let Some(threshold) = self.retry_threshold {
            check("retry_threshold", threshold, 0, self.max_incoming)?;
        }
        check("dial_stagger_ms", self.dial_stagger_ms, 0, 5000)?;
        check("kernel_drop_poll_secs", self.kernel_drop_poll_secs, 1, 300)?;
        if self.qlog_dir.is_some() && !cfg!(feature = "qlog") {
            return Err(ConfigError {
                key: "qlog_dir",
                reason: "this build lacks the zakura-quic `qlog` feature".into(),
            });
        }
        Ok(())
    }

    /// The connection idle timeout (CTRL-10).
    pub fn idle_timeout(&self) -> Duration {
        Duration::from_secs(u64::from(self.idle_timeout_secs))
    }

    /// The connection keepalive interval (CTRL-11).
    pub fn keep_alive_interval(&self) -> Duration {
        Duration::from_secs(u64::from(self.keep_alive_interval_secs))
    }

    /// The handshake deadline, if set (ADM-6, DIAL-4).
    pub fn handshake_timeout(&self) -> Option<Duration> {
        self.handshake_timeout_secs
            .map(|secs| Duration::from_secs(u64::from(secs)))
    }

    /// Delay between dial attempts (DIAL-3).
    pub fn dial_stagger(&self) -> Duration {
        Duration::from_millis(u64::from(self.dial_stagger_ms))
    }

    /// Kernel drop counter poll interval (SOCK-7).
    pub fn kernel_drop_poll_interval(&self) -> Duration {
        Duration::from_secs(u64::from(self.kernel_drop_poll_secs))
    }

    /// Builds the noq transport config for both directions.
    ///
    /// `max_bidi_streams` is the handshake config's `max_open_streams` (WIRE-8).
    pub(crate) fn transport_config(&self, max_bidi_streams: u32) -> noq::TransportConfig {
        let mut config = noq::TransportConfig::default();

        // Wire profile (§5). WIRE-4: the NAT-traversal parameter stays absent
        // because noq leaves `max_remote_nat_traversal_addresses` unset.
        config
            .max_concurrent_multipath_paths(MAX_MULTIPATH_PATHS)
            .max_concurrent_bidi_streams(VarInt::from_u32(max_bidi_streams))
            .max_concurrent_uni_streams(VarInt::from_u32(0))
            .datagram_receive_buffer_size(None)
            .datagram_send_buffer_size(0);
        // WIRE-10: noq neither sends nor requests observed-address reports by default.

        // Windows and timers.
        config
            .stream_receive_window(VarInt::from_u32(self.stream_receive_window_bytes))
            .receive_window(VarInt::from_u32(self.receive_window_bytes))
            .send_window(self.send_window_bytes)
            .max_idle_timeout(Some(
                IdleTimeout::try_from(Duration::from_secs(u64::from(self.idle_timeout_secs)))
                    .expect("validate() bounds idle_timeout_secs to 600, a valid QUIC VarInt"),
            ))
            .keep_alive_interval(Some(Duration::from_secs(u64::from(
                self.keep_alive_interval_secs,
            ))))
            .default_path_keep_alive_interval(Some(Duration::from_secs(u64::from(
                self.path_keep_alive_interval_secs,
            ))))
            .default_path_max_idle_timeout(Some(Duration::from_secs(u64::from(
                self.path_idle_timeout_secs,
            ))));

        // Congestion control: always set explicitly (CTRL-3).
        let initial_window = self.initial_window_bytes;
        let factory: Arc<dyn noq::congestion::ControllerFactory + Send + Sync> =
            match self.congestion_controller {
                CongestionController::Cubic => {
                    let mut cc = CubicConfig::default();
                    if let Some(window) = initial_window {
                        cc.initial_window(window);
                    }
                    Arc::new(cc)
                }
                CongestionController::NewReno => {
                    let mut cc = NewRenoConfig::default();
                    if let Some(window) = initial_window {
                        cc.initial_window(window);
                    }
                    Arc::new(cc)
                }
                CongestionController::Bbr3 => {
                    let mut cc = Bbr3Config::default();
                    if let Some(window) = initial_window {
                        cc.initial_window(window);
                    }
                    Arc::new(cc)
                }
            };
        config.congestion_controller_factory(factory);

        if let Some(rate) = self.max_send_rate_bytes_per_second {
            config.max_outgoing_bytes_per_second(Some(rate));
        }
        if self.ack_frequency {
            config.ack_frequency_config(Some(AckFrequencyConfig::default()));
        }
        config
            .enable_segmentation_offload(self.gso)
            .initial_mtu(self.initial_mtu)
            .mtu_discovery_config(self.mtu_discovery.then(MtuDiscoveryConfig::default));

        #[cfg(feature = "qlog")]
        if let Some(dir) = &self.qlog_dir {
            config.qlog_from_path(dir, "zakura");
        }

        config
    }

    /// Applies the incoming-attempt limits to a server config (ADM-5).
    pub(crate) fn apply_server_limits(&self, server: &mut noq::ServerConfig) {
        server
            .max_incoming(self.max_incoming as usize)
            .incoming_buffer_size(self.incoming_buffer_bytes)
            .incoming_buffer_size_total(self.incoming_buffer_total_bytes)
            // WIRE-11: allow migration; PATH-3 re-checks every new address.
            .migration(true);
    }

    /// Builds the endpoint config shared by every socket.
    pub(crate) fn endpoint_config(&self) -> noq::EndpointConfig {
        // ADM-10: noq's default is a random ring HMAC reset key per startup.
        let mut config = noq::EndpointConfig::default();
        // WIRE-5.
        config.grease_quic_bit(false);
        config
    }
}

/// Where and how to bind the endpoint's sockets (API-2).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuicBindConfig {
    /// Socket addresses to bind, one noq endpoint each (SOCK-1).
    pub addrs: Vec<SocketAddr>,
    /// Concurrent bidirectional streams per connection (WIRE-8).
    pub max_bidi_streams: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid_and_match_today() {
        let config = QuicConfig::default();
        config.validate().unwrap();
        assert_eq!(config.recv_buffer_bytes, 7_340_032);
        assert_eq!(config.max_incoming, 65_536);
        assert_eq!(config.incoming_buffer_bytes, 10 * MIB);
        assert_eq!(config.incoming_buffer_total_bytes, 100 * MIB);
        assert_eq!(config.path_keep_alive_interval_secs, 5);
        assert_eq!(config.path_idle_timeout_secs, 15);
        assert_eq!(config.handshake_timeout_secs, Some(10));
        assert_eq!(config.retry_threshold, Some(8));
        assert_eq!(config.congestion_controller, CongestionController::Cubic);
    }

    #[test]
    fn out_of_range_values_name_the_key() {
        let config = QuicConfig {
            recv_buffer_bytes: 10,
            ..QuicConfig::default()
        };
        assert_eq!(config.validate().unwrap_err().key, "recv_buffer_bytes");

        let config = QuicConfig {
            idle_timeout_secs: 30,
            keep_alive_interval_secs: 10,
            ..QuicConfig::default()
        };
        assert_eq!(config.validate().unwrap_err().key, "idle_timeout_secs");

        let config = QuicConfig {
            receive_window_bytes: 1024 * 1024,
            ..QuicConfig::default()
        };
        assert_eq!(config.validate().unwrap_err().key, "receive_window_bytes");

        let config = QuicConfig {
            retry_threshold: Some(70_000),
            ..QuicConfig::default()
        };
        assert_eq!(config.validate().unwrap_err().key, "retry_threshold");
    }

    #[test]
    fn toml_parses_and_refuses_unknown_keys() {
        let config: QuicConfig =
            toml::from_str("congestion_controller = \"bbr3\"\nretry_threshold = 0\n").unwrap();
        assert_eq!(config.congestion_controller, CongestionController::Bbr3);
        assert_eq!(config.retry_threshold, Some(0));
        config.validate().unwrap();

        assert!(toml::from_str::<QuicConfig>("transport = \"iroh\"\n").is_err());
    }
}
