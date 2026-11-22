//! Handshakes between this tree's `zakura-quic` and the release deployed
//! nodes run, in both directions, over loopback.
//!
//! The released endpoint has no hybrid group, so these pairs must complete
//! on X25519. The group choice and the absence of a HelloRetryRequest are
//! checked in `zakura-quic`'s own TLS tests; here the released code itself
//! has to accept the current dialer's two-datagram hello, authenticate both
//! nodes and carry data, and dial the current listener.

#[cfg(test)]
mod tests;
