//! Error types (API-6).

use std::net::SocketAddr;

use noq::{ConnectionError, TransportErrorCode};

use crate::{config::ConfigError, tls::TlsConfigError};

/// TLS alert `no_application_protocol` (RFC 8446 §6), carried as a QUIC crypto error.
const ALERT_NO_APPLICATION_PROTOCOL: u8 = 120;
/// TLS alerts a raw-public-key verifier sends when it rejects the peer's key.
const ALERTS_WRONG_IDENTITY: [u8; 4] = [
    42, // bad_certificate
    46, // certificate_unknown
    48, // unknown_ca
    51, // decrypt_error (signature check)
];

/// A dial failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ConnectError {
    /// The dialed node ID is this endpoint's own (DIAL-1).
    #[error("refusing to dial this node's own ID")]
    SelfDial,
    /// No address shares a family with a bound socket (DIAL-2).
    #[error("no dialable address: none matches a bound socket's address family")]
    NoUsableAddress,
    /// The peer speaks none of the offered ALPNs (DIAL-5).
    #[error("the peer doesn't speak the requested ALPN")]
    AlpnMismatch,
    /// The handshake missed its deadline (DIAL-4).
    #[error("QUIC handshake timed out")]
    HandshakeTimeout,
    /// The peer refused the connection attempt.
    #[error("the peer refused the connection")]
    Refused,
    /// The peer's key isn't the dialed node ID (TLS-7).
    #[error("the peer presented a different identity than the dialed node ID")]
    WrongIdentity,
    /// The endpoint is shut down or refused the remote address.
    #[error("endpoint can't dial: {0}")]
    Endpoint(#[from] noq::ConnectError),
    /// Building the TLS client config failed.
    #[error(transparent)]
    Tls(#[from] TlsConfigError),
    /// The connection failed for another transport reason.
    #[error(transparent)]
    Transport(ConnectionError),
}

impl ConnectError {
    /// Classifies a handshake failure.
    pub(crate) fn from_handshake(error: ConnectionError) -> Self {
        let code = match &error {
            ConnectionError::ConnectionClosed(close) => Some(close.error_code),
            ConnectionError::TransportError(error) => Some(error.code),
            _ => None,
        };
        match code {
            Some(code) if code == TransportErrorCode::CONNECTION_REFUSED => Self::Refused,
            Some(code) if code == TransportErrorCode::crypto(ALERT_NO_APPLICATION_PROTOCOL) => {
                Self::AlpnMismatch
            }
            Some(code)
                if ALERTS_WRONG_IDENTITY
                    .iter()
                    .any(|alert| code == TransportErrorCode::crypto(*alert)) =>
            {
                Self::WrongIdentity
            }
            _ => Self::Transport(error),
        }
    }

    /// Ranks errors so a multi-address dial reports the most informative one.
    pub(crate) fn rank(&self) -> u8 {
        match self {
            Self::AlpnMismatch => 6,
            Self::WrongIdentity => 5,
            Self::Refused => 4,
            Self::Transport(_) => 3,
            Self::HandshakeTimeout => 2,
            Self::Endpoint(_) | Self::Tls(_) => 1,
            Self::SelfDial | Self::NoUsableAddress => 0,
        }
    }
}

/// Binding the endpoint failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BindError {
    /// The configuration is out of range.
    #[error(transparent)]
    Config(#[from] ConfigError),
    /// No bind address was given.
    #[error("no bind address configured")]
    NoAddress,
    /// A socket failed to bind or configure.
    #[error("failed to bind {addr}: {source}")]
    Socket {
        /// The failing address.
        addr: SocketAddr,
        /// The I/O error.
        source: std::io::Error,
    },
    /// Building the TLS config failed.
    #[error(transparent)]
    Tls(#[from] TlsConfigError),
}
