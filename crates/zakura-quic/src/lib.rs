//! Direct QUIC transport for Zakura's native P2P stack.
//!
//! `zakura-quic` runs Zakura's `p2p-v2/*` protocols over [noq] without Iroh's
//! socket layer. `docs/specs/zakura-quic.md` is authoritative for its behavior;
//! code comments cite requirement IDs such as `ADM-3`.
//!
//! The crate provides:
//! - node identity types ([`NodeId`], [`NodeSecretKey`], [`NodeAddr`]);
//! - the TLS raw-public-key profile Iroh 1.1 uses;
//! - [`QuicEndpoint`], which binds noq-udp sockets, admits connection attempts
//!   before the handshake, dials, and shuts down in order;
//! - [`Conn`], an authenticated connection.

mod key;
mod tls;

pub use key::{KeyParsingError, NodeAddr, NodeId, NodeSecretKey, SignatureError, KEY_LENGTH};
pub use tls::TlsConfigError;

mod admission;
mod config;
mod conn;
mod endpoint;
mod error;
mod socket;
pub mod sys;

pub use config::{
    ConfigError, CongestionController, QuicBindConfig, QuicConfig, MAX_MULTIPATH_PATHS,
};
pub use conn::{BanCheck, Conn, ConnObserver, ConnSample, ConnStats};
pub use endpoint::{Acceptor, Admit, IncomingInfo, QuicEndpoint, SocketStats};
pub use error::{BindError, ConnectError};
pub use socket::SocketBuffers;

// API-5: stream types and the errors Zakura matches on.
pub use noq::{
    ClosedStream, ConnectionError, PathId, ReadError, ReadExactError, ReadToEndError, RecvStream,
    SendStream, VarInt, WriteError,
};

#[cfg(test)]
mod tests;
