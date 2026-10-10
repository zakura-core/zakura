//! Direct QUIC transport for Zakura's native P2P stack.
//!
//! `zakura-quic` runs Zakura's `p2p-v2/*` protocols over [`quinn_proto`], the
//! sans-I/O QUIC state machine, with its own socket layer and async driver.
//! `docs/specs/zakura-quic.md` is authoritative for its behavior; code
//! comments cite requirement IDs such as `ADM-3`.
//!
//! The crate provides:
//! - node identity types ([`NodeId`], [`NodeSecretKey`], [`NodeAddr`]);
//! - the TLS raw-public-key profile Iroh 1.1 uses;
//! - [`QuicEndpoint`], which binds UDP sockets, admits connection attempts
//!   before the handshake, dials, and shuts down in order;
//! - [`Conn`], an authenticated connection, and its stream handles.
//!
//! One endpoint task owns each socket's `quinn_proto::Endpoint`. One
//! connection task owns each `quinn_proto::Connection`. Handles reach them only
//! through ordered message channels, so no lock guards a QUIC state machine.

mod key;
mod tls;

pub use key::{KeyParsingError, NodeAddr, NodeId, NodeSecretKey, SignatureError, KEY_LENGTH};
pub use tls::TlsConfigError;

mod config;
mod congestion;
mod conn;
mod driver;
mod endpoint;
mod error;
mod socket;
mod stream;
pub mod sys;

pub use config::{ConfigError, CongestionController, QuicBindConfig, QuicConfig};
pub use conn::{BanCheck, CongestionStats, Conn, ConnObserver, ConnSample, ConnStats, DriverStats};
pub use endpoint::{Acceptor, Admit, IncomingInfo, QuicEndpoint, SocketStats};
pub use error::{
    BindError, ClosedStream, ConnectError, ReadError, ReadExactError, ReadToEndError, WriteError,
};
pub use socket::SocketBuffers;
pub use stream::{RecvStream, SendStream};

// API-5: quinn-proto types that appear in this crate's API.
pub use quinn_proto::{ConnectionError, ConnectionStats, FrameStats, PathStats, UdpStats, VarInt};

#[cfg(test)]
mod tests;
