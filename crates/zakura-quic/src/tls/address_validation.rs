//! Observe QUIC address validation without parsing or authenticating packets a
//! second time. RFC 9000 §8.1 permits a server to validate the client's address
//! when it receives a packet protected with Handshake keys.

use std::{any::Any, sync::Arc};

use bytes::BytesMut;
use quinn_proto::{
    crypto::{
        CryptoError, ExportKeyingMaterialError, HeaderKey, KeyPair, Keys, PacketKey, ServerConfig,
        Session, UnsupportedVersion,
    },
    transport_parameters::TransportParameters,
    ConnectionId, Side, TransportError,
};
use zakura_rt::sync::atomic::{AtomicBool, Ordering};

/// Attaches a validation signal to this accepted connection's TLS config.
pub(crate) fn observe(config: &mut quinn_proto::ServerConfig) -> Arc<AtomicBool> {
    let validated = Arc::new(AtomicBool::new(false));
    config.crypto = Arc::new(ObservedServer {
        inner: config.crypto.clone(),
        validated: validated.clone(),
    });
    validated
}

struct ObservedServer {
    inner: Arc<dyn ServerConfig>,
    validated: Arc<AtomicBool>,
}

impl ServerConfig for ObservedServer {
    fn initial_keys(&self, version: u32, dst: &ConnectionId) -> Result<Keys, UnsupportedVersion> {
        self.inner.initial_keys(version, dst)
    }

    fn retry_tag(&self, version: u32, dst: &ConnectionId, packet: &[u8]) -> [u8; 16] {
        self.inner.retry_tag(version, dst, packet)
    }

    fn start_session(
        self: Arc<Self>,
        version: u32,
        params: &TransportParameters,
    ) -> Box<dyn Session> {
        Box::new(ObservedSession {
            inner: self.inner.clone().start_session(version, params),
            handshake: Some(self.validated.clone()),
        })
    }
}

struct ObservedSession {
    inner: Box<dyn Session>,
    handshake: Option<Arc<AtomicBool>>,
}

impl Session for ObservedSession {
    fn initial_keys(&self, dst: &ConnectionId, side: Side) -> Keys {
        self.inner.initial_keys(dst, side)
    }

    fn handshake_data(&self) -> Option<Box<dyn Any>> {
        self.inner.handshake_data()
    }

    fn peer_identity(&self) -> Option<Box<dyn Any>> {
        self.inner.peer_identity()
    }

    fn early_crypto(&self) -> Option<(Box<dyn HeaderKey>, Box<dyn PacketKey>)> {
        self.inner.early_crypto()
    }

    fn early_data_accepted(&self) -> Option<bool> {
        self.inner.early_data_accepted()
    }

    fn is_handshaking(&self) -> bool {
        self.inner.is_handshaking()
    }

    fn read_handshake(&mut self, bytes: &[u8]) -> Result<bool, TransportError> {
        self.inner.read_handshake(bytes)
    }

    fn transport_parameters(&self) -> Result<Option<TransportParameters>, TransportError> {
        self.inner.transport_parameters()
    }

    fn write_handshake(&mut self, buf: &mut Vec<u8>) -> Option<Keys> {
        let mut keys = self.inner.write_handshake(buf)?;
        // Initial keys come from initial_keys(). TLS's first key change
        // supplies Handshake keys. Subsequent 1-RTT keys pass straight through.
        if let Some(validated) = self.handshake.take() {
            keys.packet.remote = Box::new(HandshakeReadKey {
                inner: keys.packet.remote,
                validated,
            });
        }
        Some(keys)
    }

    fn next_1rtt_keys(&mut self) -> Option<KeyPair<Box<dyn PacketKey>>> {
        self.inner.next_1rtt_keys()
    }

    fn is_valid_retry(&self, dst: &ConnectionId, header: &[u8], payload: &[u8]) -> bool {
        self.inner.is_valid_retry(dst, header, payload)
    }

    fn export_keying_material(
        &self,
        out: &mut [u8],
        label: &[u8],
        context: &[u8],
    ) -> Result<(), ExportKeyingMaterialError> {
        self.inner.export_keying_material(out, label, context)
    }
}

struct HandshakeReadKey {
    inner: Box<dyn PacketKey>,
    validated: Arc<AtomicBool>,
}

impl PacketKey for HandshakeReadKey {
    fn encrypt(&self, packet: u64, buf: &mut [u8], header_len: usize) {
        self.inner.encrypt(packet, buf, header_len);
    }

    fn decrypt(
        &self,
        packet: u64,
        header: &[u8],
        payload: &mut BytesMut,
    ) -> Result<(), CryptoError> {
        self.inner.decrypt(packet, header, payload)?;
        self.validated.store(true, Ordering::Relaxed);
        Ok(())
    }

    fn tag_len(&self) -> usize {
        self.inner.tag_len()
    }
    fn confidentiality_limit(&self) -> u64 {
        self.inner.confidentiality_limit()
    }
    fn integrity_limit(&self) -> u64 {
        self.inner.integrity_limit()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{tls::TlsConfig, NodeSecretKey};

    #[test]
    fn only_an_authenticated_packet_releases_the_send_allowance() {
        let server = TlsConfig::new(&NodeSecretKey::from_bytes(&[1; 32]))
            .server_config(vec![b"test".to_vec()])
            .unwrap();
        let keys = server.initial_keys(1, &ConnectionId::new(&[2; 8])).unwrap();
        let validated = Arc::new(AtomicBool::new(false));
        let key = HandshakeReadKey {
            inner: keys.packet.remote,
            validated: validated.clone(),
        };

        let mut packet = vec![0; 2 + key.tag_len()];
        key.encrypt(1, &mut packet, 1);
        let (header, payload) = packet.split_at(1);
        assert!(key
            .decrypt(2, header, &mut BytesMut::from(payload))
            .is_err());
        assert!(!validated.load(Ordering::Relaxed));
        key.decrypt(1, header, &mut BytesMut::from(payload))
            .unwrap();
        assert!(validated.load(Ordering::Relaxed));
    }
}
