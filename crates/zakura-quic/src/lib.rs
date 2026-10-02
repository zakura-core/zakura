//! Direct QUIC transport for Zakura's native P2P stack.
//!
//! `zakura-quic` runs Zakura's `p2p-v2/*` protocols over [noq] without Iroh's
//! socket layer. `docs/zakura-quic/SPEC.md` is authoritative for its behavior;
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
