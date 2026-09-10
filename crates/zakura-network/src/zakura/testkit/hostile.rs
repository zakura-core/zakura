//! Raw peer harness for adversarial Zakura tests.

use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use iroh::endpoint::{Connection, Endpoint, RecvStream, SendStream, VarInt};
use tokio::sync::Mutex;

use super::{LocalEndpointFactory, ZakuraTestNode};
use crate::{
    zakura::{
        legacy_gossip::ZAKURA_STREAM_GOSSIP, run_native_initiator_handshake, Frame,
        HeaderSyncCodec, HeaderSyncMessage, StreamPrelude, ZakuraHandshakeConfig,
        ZakuraLocalLimits, ZakuraPeerId, FRAME_HEADER_BYTES, LEGACY_GOSSIP_VERSION, P2P_V2_ALPN,
        STREAM_PRELUDE_MAGIC, ZAKURA_BLOCK_SYNC_STREAM_VERSION, ZAKURA_CAP_HEADER_SYNC,
        ZAKURA_CAP_LEGACY_GOSSIP, ZAKURA_DISCOVERY_STREAM_VERSION,
        ZAKURA_HEADER_SYNC_STREAM_VERSION, ZAKURA_STREAM_BLOCK_REQUESTS, ZAKURA_STREAM_BLOCK_SYNC,
        ZAKURA_STREAM_DISCOVERY, ZAKURA_STREAM_HEADER_SYNC,
    },
    BoxError, Config,
};

/// Raw Iroh peer that can violate Zakura stream rules by hand.
#[derive(Debug)]
pub struct HostilePeer {
    endpoint: Endpoint,
    connection: Connection,
    limits: ZakuraLocalLimits,
    opens_block_pair: bool,
    next_pair_id: AtomicU64,
    held_streams: Vec<SendStream>,
    ordered_streams: Mutex<HashMap<(u16, u16), (SendStream, RecvStream)>>,
}

impl HostilePeer {
    /// Connect to `victim` with a valid native control handshake.
    pub async fn connect_native(victim: &ZakuraTestNode, seed: u64) -> Result<Self, BoxError> {
        Self::connect_native_with_capabilities(
            victim,
            seed,
            ZAKURA_CAP_LEGACY_GOSSIP | ZAKURA_CAP_HEADER_SYNC,
        )
        .await
    }

    /// Connect to `victim` with an explicit optional capability mask.
    pub async fn connect_native_with_capabilities(
        victim: &ZakuraTestNode,
        seed: u64,
        capabilities: u64,
    ) -> Result<Self, BoxError> {
        let limits = victim.limits().clone();
        let endpoint = LocalEndpointFactory::with_transport_config(limits.transport_config())
            .endpoint(seed)
            .await?;
        let victim_addr = victim.node_addr().await;
        let opens_block_pair = endpoint.node_id() < victim_addr.node_id;
        endpoint.add_node_addr(victim_addr.clone())?;
        let connection = endpoint.connect(victim_addr, P2P_V2_ALPN).await?;
        let mut config = ZakuraHandshakeConfig::for_network(&Config::default().network);
        config.supported_capabilities = capabilities;
        let local_peer_id = ZakuraPeerId::new(endpoint.node_id().as_bytes().to_vec())?;
        run_native_initiator_handshake(&connection, &limits, &config, &local_peer_id).await?;

        Ok(Self {
            endpoint,
            connection,
            limits,
            opens_block_pair,
            next_pair_id: AtomicU64::new(1),
            held_streams: Vec::new(),
            ordered_streams: Mutex::new(HashMap::new()),
        })
    }

    /// Return this peer's authenticated Iroh id as Zakura sees it.
    pub fn id(&self) -> Result<ZakuraPeerId, BoxError> {
        Ok(ZakuraPeerId::new(
            self.endpoint.node_id().as_bytes().to_vec(),
        )?)
    }

    /// Encode and send one canonical protocol-v8 header-sync message.
    pub async fn send_header_sync_message(
        &self,
        codec: &HeaderSyncCodec,
        message: &HeaderSyncMessage,
    ) -> Result<(), BoxError> {
        let frame = codec.encode_frame(message)?;
        self.send_raw_frame(ZAKURA_STREAM_HEADER_SYNC, frame).await
    }

    /// Receive and decode one canonical protocol-v8 header-sync message.
    pub async fn recv_header_sync_message(
        &self,
        codec: &HeaderSyncCodec,
    ) -> Result<HeaderSyncMessage, BoxError> {
        let frame = self.recv_ordered_frame(ZAKURA_STREAM_HEADER_SYNC).await?;
        Ok(codec.decode_frame(frame, None)?)
    }

    /// Wait until the victim closes this QUIC connection.
    pub async fn wait_for_connection_close(
        &self,
        timeout: std::time::Duration,
    ) -> Result<(), BoxError> {
        tokio::time::timeout(timeout, self.connection.closed())
            .await
            .map_err(|_| -> BoxError {
                "timed out waiting for hostile connection closure".into()
            })?;
        Ok(())
    }

    /// Open one stream and send a valid prelude and frame.
    pub async fn send_frame(&self, stream_kind: u16, payload: Vec<u8>) -> Result<(), BoxError> {
        self.send_raw_frame(
            stream_kind,
            Frame {
                message_type: 1,
                flags: 0,
                payload,
            },
        )
        .await
    }

    /// Open one stream and send a valid prelude followed by `frame`.
    pub async fn send_raw_frame(&self, stream_kind: u16, frame: Frame) -> Result<(), BoxError> {
        if matches!(
            stream_kind,
            ZAKURA_STREAM_GOSSIP
                | ZAKURA_STREAM_DISCOVERY
                | ZAKURA_STREAM_HEADER_SYNC
                | ZAKURA_STREAM_BLOCK_SYNC
                | ZAKURA_STREAM_BLOCK_REQUESTS
        ) {
            return self.send_ordered_raw_frame(stream_kind, frame).await;
        }

        let (mut send, _recv) = self.connection.open_bi().await?;
        self.write_prelude(&mut send, stream_kind).await?;
        send.write_all(&frame.encode(self.limits.max_frame_bytes)?)
            .await?;
        let _ = send.finish();
        Ok(())
    }

    async fn send_ordered_raw_frame(&self, stream_kind: u16, frame: Frame) -> Result<(), BoxError> {
        self.send_ordered_raw_frame_with_version(
            stream_kind,
            Self::stream_version(stream_kind),
            frame,
        )
        .await
    }

    /// Send `frame` on a persistent ordered stream at an explicit version.
    pub async fn send_ordered_raw_frame_with_version(
        &self,
        stream_kind: u16,
        stream_version: u16,
        frame: Frame,
    ) -> Result<(), BoxError> {
        let mut streams = self.ordered_streams.lock().await;
        if Self::is_block_pair_role(stream_kind, stream_version) {
            self.ensure_block_pair(&mut streams).await?;
        }
        let (send, _recv) = match streams.entry((stream_kind, stream_version)) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let (mut send, recv) = self.connection.open_bi().await?;
                self.write_prelude_with_version(&mut send, stream_kind, stream_version)
                    .await?;
                entry.insert((send, recv))
            }
        };
        send.write_all(&frame.encode(self.limits.max_frame_bytes)?)
            .await?;
        Ok(())
    }

    /// Receive the next frame written by the victim on this ordered stream.
    pub async fn recv_ordered_frame(&self, stream_kind: u16) -> Result<Frame, BoxError> {
        self.recv_ordered_frame_with_version(stream_kind, Self::stream_version(stream_kind))
            .await
    }

    /// Receive the next frame on a persistent ordered stream at an explicit version.
    pub async fn recv_ordered_frame_with_version(
        &self,
        stream_kind: u16,
        stream_version: u16,
    ) -> Result<Frame, BoxError> {
        let mut streams = self.ordered_streams.lock().await;
        if Self::is_block_pair_role(stream_kind, stream_version) {
            self.ensure_block_pair(&mut streams).await?;
        }
        let (_send, recv) = match streams.entry((stream_kind, stream_version)) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let (mut send, recv) = self.connection.open_bi().await?;
                self.write_prelude_with_version(&mut send, stream_kind, stream_version)
                    .await?;
                entry.insert((send, recv))
            }
        };
        Self::read_frame(recv, self.limits.max_frame_bytes).await
    }

    fn is_block_pair_role(kind: u16, version: u16) -> bool {
        (kind == ZAKURA_STREAM_BLOCK_SYNC && version == ZAKURA_BLOCK_SYNC_STREAM_VERSION)
            || (kind == ZAKURA_STREAM_BLOCK_REQUESTS && version == 1)
    }

    // Use the same node-id opener as production. Raw fixtures do not run the
    // handler's collision-selection loop, so only one endpoint offers a pair.
    async fn ensure_block_pair(
        &self,
        streams: &mut HashMap<(u16, u16), (SendStream, RecvStream)>,
    ) -> Result<(), BoxError> {
        let data = (ZAKURA_STREAM_BLOCK_SYNC, ZAKURA_BLOCK_SYNC_STREAM_VERSION);
        let requests = (ZAKURA_STREAM_BLOCK_REQUESTS, 1);
        if streams.contains_key(&data) && streams.contains_key(&requests) {
            return Ok(());
        }
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            if self.opens_block_pair {
                let pair_id = self.next_pair_id.fetch_add(1, Ordering::Relaxed);
                for role in [data, requests] {
                    let (mut send, recv) = self.connection.open_bi().await?;
                    self.write_prelude_with_version(&mut send, role.0, role.1)
                        .await?;
                    send.write_all(&pair_id.to_le_bytes()).await?;
                    streams.insert(role, (send, recv));
                }
                return Ok(());
            }
            let mut pair_id = None;
            for _ in 0..16 {
                let (mut send, mut recv) = self.connection.accept_bi().await?;
                let prelude = Self::read_prelude(&mut recv).await?;
                let role = (prelude.stream_kind, prelude.stream_version);
                if role != data && role != requests {
                    send.reset(0u32.into())?;
                    recv.stop(0u32.into())?;
                    continue;
                }
                let mut id = [0; 8];
                recv.read_exact(&mut id).await?;
                let id = u64::from_le_bytes(id);
                if id == 0
                    || pair_id.is_some_and(|previous| previous != id)
                    || streams.contains_key(&role)
                {
                    return Err("victim opened an invalid block-sync pair".into());
                }
                pair_id = Some(id);
                streams.insert(role, (send, recv));
                if streams.contains_key(&data) && streams.contains_key(&requests) {
                    return Ok(());
                }
            }
            Err("victim did not open both block-sync roles".into())
        })
        .await?
    }

    /// Finish a persistent stream, or both roles of its block-sync pair.
    pub async fn finish_ordered_stream(&self, kind: u16, version: u16) -> Result<(), BoxError> {
        self.retire_ordered_stream(kind, version, false).await
    }

    /// Reset a persistent stream, or both roles of its block-sync pair.
    pub async fn reset_ordered_stream(&self, kind: u16, version: u16) -> Result<(), BoxError> {
        self.retire_ordered_stream(kind, version, true).await
    }

    async fn retire_ordered_stream(
        &self,
        kind: u16,
        version: u16,
        reset: bool,
    ) -> Result<(), BoxError> {
        let mut streams = self.ordered_streams.lock().await;
        let roles = if Self::is_block_pair_role(kind, version) {
            vec![
                (ZAKURA_STREAM_BLOCK_SYNC, ZAKURA_BLOCK_SYNC_STREAM_VERSION),
                (ZAKURA_STREAM_BLOCK_REQUESTS, 1),
            ]
        } else {
            vec![(kind, version)]
        };
        for role in roles {
            if let Some((mut send, mut recv)) = streams.remove(&role) {
                if reset {
                    send.reset(0u32.into())?;
                    recv.stop(0u32.into())?;
                } else {
                    send.finish()?;
                }
            }
        }
        Ok(())
    }

    /// Replace a persistent stream or pair with a fresh physical generation.
    pub async fn reopen_ordered_stream(&self, kind: u16, version: u16) -> Result<(), BoxError> {
        self.reset_ordered_stream(kind, version).await?;
        let mut streams = self.ordered_streams.lock().await;
        if Self::is_block_pair_role(kind, version) {
            return self.ensure_block_pair(&mut streams).await;
        }
        let (mut send, recv) = self.connection.open_bi().await?;
        self.write_prelude_with_version(&mut send, kind, version)
            .await?;
        streams.insert((kind, version), (send, recv));
        Ok(())
    }

    /// Send a valid frame header with a payload shorter than its declared
    /// length.
    pub async fn send_truncated_frame(&self, stream_kind: u16) -> Result<(), BoxError> {
        let (mut send, _recv) = self.connection.open_bi().await?;
        self.write_prelude(&mut send, stream_kind).await?;
        let mut header = Vec::with_capacity(FRAME_HEADER_BYTES);
        WriteBytesExt::write_u16::<LittleEndian>(&mut header, 1)?;
        WriteBytesExt::write_u16::<LittleEndian>(&mut header, 0)?;
        WriteBytesExt::write_u32::<LittleEndian>(&mut header, 8)?;
        send.write_all(&header).await?;
        send.write_all(&[1, 2, 3]).await?;
        let _ = send.finish();
        Ok(())
    }

    /// Open one stream whose prelude names the given kind and version, then send
    /// a frame. Used to exercise the unknown-kind/unsupported-version rejection
    /// path (FLUP-015): the victim must reset the stream before the frame is
    /// ever delivered to the inbound sink.
    pub async fn send_frame_with_version(
        &self,
        stream_kind: u16,
        stream_version: u16,
        payload: Vec<u8>,
    ) -> Result<(), BoxError> {
        let (mut send, _recv) = self.connection.open_bi().await?;
        let prelude = StreamPrelude {
            magic: STREAM_PRELUDE_MAGIC,
            stream_kind,
            stream_version,
            request_id: None,
            max_frame_bytes: self.limits.max_frame_bytes,
        };
        send.write_all(&prelude.encode()?).await?;
        let frame = Frame {
            message_type: 1,
            flags: 0,
            payload,
        };
        send.write_all(&frame.encode(self.limits.max_frame_bytes)?)
            .await?;
        let _ = send.finish();
        Ok(())
    }

    /// Open one stream whose prelude has a request id, then send a frame.
    pub async fn send_frame_with_request_id(
        &self,
        stream_kind: u16,
        request_id: u64,
        frame: Frame,
    ) -> Result<(), BoxError> {
        let (mut send, _recv) = self.connection.open_bi().await?;
        let prelude = StreamPrelude {
            magic: STREAM_PRELUDE_MAGIC,
            stream_kind,
            stream_version: 1,
            request_id: Some(request_id),
            max_frame_bytes: self.limits.max_frame_bytes,
        };
        send.write_all(&prelude.encode()?).await?;
        send.write_all(&frame.encode(self.limits.max_frame_bytes)?)
            .await?;
        let _ = send.finish();
        Ok(())
    }

    /// Open a request stream, send one frame, and read its first response frame.
    pub async fn request_frame(
        &self,
        stream_kind: u16,
        request_id: u64,
        frame: Frame,
    ) -> Result<Frame, BoxError> {
        let (mut send, mut recv) = self.connection.open_bi().await?;
        let prelude = StreamPrelude {
            magic: STREAM_PRELUDE_MAGIC,
            stream_kind,
            stream_version: 1,
            request_id: Some(request_id),
            max_frame_bytes: self.limits.max_frame_bytes,
        };
        send.write_all(&prelude.encode()?).await?;
        send.write_all(&frame.encode(self.limits.max_frame_bytes)?)
            .await?;
        send.finish()?;
        Self::read_frame(&mut recv, self.limits.max_frame_bytes).await
    }

    /// Accept the next bidi stream opened by the victim and respond with raw frames.
    pub async fn respond_to_next_request(&self, frames: Vec<Frame>) -> Result<(), BoxError> {
        let (mut send, _recv) = self.connection.accept_bi().await?;
        for frame in frames {
            send.write_all(&frame.encode(self.limits.max_frame_bytes)?)
                .await?;
        }
        let _ = send.finish();
        Ok(())
    }

    /// Accept the next request stream and build a response from its request id.
    pub async fn respond_to_next_request_with(
        &self,
        build_frames: impl FnOnce(u64) -> Vec<Frame>,
    ) -> Result<(), BoxError> {
        let (mut send, mut recv) = self.connection.accept_bi().await?;
        let prelude = Self::read_prelude(&mut recv).await?;
        let request_id = prelude
            .request_id
            .ok_or_else(|| BoxError::from("request stream did not include a request id"))?;
        for frame in build_frames(request_id) {
            send.write_all(&frame.encode(self.limits.max_frame_bytes)?)
                .await?;
        }
        let _ = send.finish();
        Ok(())
    }

    /// Accept the next request stream and keep the response side open.
    pub async fn accept_next_request_without_response(&mut self) -> Result<(), BoxError> {
        let (send, mut recv) = self.connection.accept_bi().await?;
        let prelude = Self::read_prelude(&mut recv).await?;
        prelude
            .request_id
            .ok_or_else(|| BoxError::from("request stream did not include a request id"))?;
        self.held_streams.push(send);
        Ok(())
    }

    /// Open one stream of `stream_kind` and send `count` valid frames on it.
    ///
    /// Unlike [`send_frame`](Self::send_frame), every frame travels on the SAME
    /// stream, so two calls produce exactly two long-lived same-kind streams.
    /// Used to prove the per-connection, per-kind message-rate bucket is shared
    /// across streams (FLUP-014) rather than re-created per stream. Each payload
    /// is unique so the inbound recorder can be inspected by content.
    pub async fn flood_stream(
        &self,
        stream_kind: u16,
        label: char,
        count: usize,
    ) -> Result<(), BoxError> {
        let (mut send, _recv) = self.connection.open_bi().await?;
        self.write_prelude(&mut send, stream_kind).await?;
        for index in 0..count {
            let frame = Frame {
                message_type: 1,
                flags: 0,
                payload: format!("{label}-{index}").into_bytes(),
            };
            send.write_all(&frame.encode(self.limits.max_frame_bytes)?)
                .await?;
        }
        let _ = send.finish();
        Ok(())
    }

    /// Send a frame header whose declared length exceeds the victim cap.
    pub async fn oversize_frame_declared_len(&self, stream_kind: u16) -> Result<(), BoxError> {
        self.send_frame_header_with_declared_payload_len(
            stream_kind,
            self.limits.max_frame_bytes.saturating_add(1),
        )
        .await
    }

    /// Send a frame header with an explicit declared payload length and no payload.
    pub async fn send_frame_header_with_declared_payload_len(
        &self,
        stream_kind: u16,
        declared_payload_len: u32,
    ) -> Result<(), BoxError> {
        if stream_kind == ZAKURA_STREAM_BLOCK_SYNC {
            let mut streams = self.ordered_streams.lock().await;
            self.ensure_block_pair(&mut streams).await?;
            let (send, _) = streams
                .get_mut(&(stream_kind, Self::stream_version(stream_kind)))
                .unwrap();
            let mut header = Vec::with_capacity(FRAME_HEADER_BYTES);
            WriteBytesExt::write_u16::<LittleEndian>(&mut header, 3)?;
            WriteBytesExt::write_u16::<LittleEndian>(&mut header, 0)?;
            WriteBytesExt::write_u32::<LittleEndian>(&mut header, declared_payload_len)?;
            send.write_all(&header).await?;
            send.finish()?;
            return Ok(());
        }
        let (mut send, _recv) = self.connection.open_bi().await?;
        self.write_prelude(&mut send, stream_kind).await?;
        let mut header = Vec::with_capacity(FRAME_HEADER_BYTES);
        WriteBytesExt::write_u16::<LittleEndian>(&mut header, 1)?;
        WriteBytesExt::write_u16::<LittleEndian>(&mut header, 0)?;
        WriteBytesExt::write_u32::<LittleEndian>(&mut header, declared_payload_len)?;
        send.write_all(&header).await?;
        let _ = send.finish();
        Ok(())
    }

    /// Open a stream and hold it past the victim's prelude timeout.
    pub async fn open_and_never_send_prelude(&mut self) -> Result<(), BoxError> {
        let (send, _recv) = self.connection.open_bi().await?;
        self.held_streams.push(send);
        tokio::time::sleep(self.limits.prelude_timeout + std::time::Duration::from_millis(20))
            .await;
        Ok(())
    }

    /// Open at most `count` streams quickly, sending valid preludes.
    pub async fn churn_streams(&mut self, stream_kind: u16, count: usize) -> Result<(), BoxError> {
        for _ in 0..count.min(4096) {
            let (mut send, _recv) = self.connection.open_bi().await?;
            self.write_prelude(&mut send, stream_kind).await?;
            self.held_streams.push(send);
        }
        Ok(())
    }

    /// Close the raw endpoint.
    pub async fn shutdown(self) {
        self.connection.close(VarInt::from_u32(0), b"hostile done");
        self.endpoint.close().await;
    }

    async fn write_prelude(&self, send: &mut SendStream, stream_kind: u16) -> Result<(), BoxError> {
        self.write_prelude_with_version(send, stream_kind, Self::stream_version(stream_kind))
            .await
    }

    async fn write_prelude_with_version(
        &self,
        send: &mut SendStream,
        stream_kind: u16,
        stream_version: u16,
    ) -> Result<(), BoxError> {
        let prelude = StreamPrelude {
            magic: STREAM_PRELUDE_MAGIC,
            stream_kind,
            stream_version,
            request_id: None,
            max_frame_bytes: self.limits.max_frame_bytes,
        };
        send.write_all(&prelude.encode()?).await?;
        Ok(())
    }

    fn stream_version(stream_kind: u16) -> u16 {
        match stream_kind {
            ZAKURA_STREAM_GOSSIP => LEGACY_GOSSIP_VERSION,
            ZAKURA_STREAM_DISCOVERY => ZAKURA_DISCOVERY_STREAM_VERSION,
            ZAKURA_STREAM_HEADER_SYNC => ZAKURA_HEADER_SYNC_STREAM_VERSION,
            ZAKURA_STREAM_BLOCK_SYNC => ZAKURA_BLOCK_SYNC_STREAM_VERSION,
            ZAKURA_STREAM_BLOCK_REQUESTS => 1,
            _ => 1,
        }
    }

    /// Selected header-sync stream version for an explicit capability mask.
    pub fn selected_header_sync_version(capabilities: u64) -> Option<u16> {
        if capabilities & ZAKURA_CAP_HEADER_SYNC != 0 {
            Some(ZAKURA_HEADER_SYNC_STREAM_VERSION)
        } else {
            (capabilities & ZAKURA_CAP_HEADER_SYNC != 0)
                .then_some(ZAKURA_HEADER_SYNC_STREAM_VERSION)
        }
    }

    async fn read_prelude(recv: &mut RecvStream) -> Result<StreamPrelude, BoxError> {
        let mut bytes = vec![0; 4 + 2 + 2 + 1];
        recv.read_exact(&mut bytes).await?;
        match bytes[8] {
            0 => {}
            1 => {
                let mut request_id = [0; 8];
                recv.read_exact(&mut request_id).await?;
                bytes.extend_from_slice(&request_id);
            }
            flag => return Err(format!("invalid request id flag: {flag}").into()),
        }
        let mut cap = [0; 4];
        recv.read_exact(&mut cap).await?;
        bytes.extend_from_slice(&cap);
        Ok(StreamPrelude::decode(&bytes)?)
    }

    async fn read_frame(recv: &mut RecvStream, max_frame_bytes: u32) -> Result<Frame, BoxError> {
        let mut header = vec![0; FRAME_HEADER_BYTES];
        recv.read_exact(&mut header).await?;
        let mut reader = std::io::Cursor::new(&header);
        let _message_type = reader.read_u16::<LittleEndian>()?;
        let _flags = reader.read_u16::<LittleEndian>()?;
        let payload_len = usize::try_from(reader.read_u32::<LittleEndian>()?)?;
        // Mirror production `read_frame`/`Frame::decode`: reject a declared frame
        // larger than the cap BEFORE allocating the payload buffer. Without this
        // a malicious or fuzzed responder can declare a huge payload_len and force
        // an unbounded allocation (OOM/hang) in the harness response reader before
        // the bounded `Frame::decode` rejection is ever reached.
        let frame_len = FRAME_HEADER_BYTES.saturating_add(payload_len);
        if frame_len > usize::try_from(max_frame_bytes)? {
            return Err(format!(
                "frame payload length {payload_len} exceeds max_frame_bytes {max_frame_bytes}"
            )
            .into());
        }
        let mut payload = vec![0; payload_len];
        recv.read_exact(&mut payload).await?;
        header.extend_from_slice(&payload);
        Ok(Frame::decode(&header, max_frame_bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use iroh::protocol::{AcceptError, ProtocolHandler, Router};

    const TEST_ALPN: &[u8] = b"/zakura/testkit/hostile-read-frame/0";
    const MAX_FRAME_BYTES: u32 = 4096;
    /// Declared payload length far exceeding the cap. The unfixed reader
    /// allocates this many bytes (then blocks reading a payload that never
    /// arrives) before the `max_frame_bytes` check; a correct reader rejects on
    /// the declared length first and never allocates it.
    const DECLARED_PAYLOAD_LEN: u32 = 64 * 1024 * 1024;

    /// Malicious responder: opens a stream, writes only a frame header that
    /// declares a payload far larger than the receiver's cap, then holds the
    /// stream open without ever sending the payload.
    #[derive(Clone, Debug)]
    struct OversizeResponder;

    impl ProtocolHandler for OversizeResponder {
        async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
            if let Ok((mut send, _recv)) = connection.open_bi().await {
                let mut header = Vec::with_capacity(FRAME_HEADER_BYTES);
                header.extend_from_slice(&1u16.to_le_bytes()); // message_type
                header.extend_from_slice(&0u16.to_le_bytes()); // flags
                header.extend_from_slice(&DECLARED_PAYLOAD_LEN.to_le_bytes()); // payload_len
                let _ = send.write_all(&header).await;
                // Never send the declared payload; keep the stream open so a
                // reader that allocates/reads the payload before the cap check
                // blocks here.
                tokio::time::sleep(Duration::from_secs(8)).await;
            }
            Ok(())
        }
    }

    /// `read_frame` must reject a frame whose declared length exceeds
    /// `max_frame_bytes` BEFORE allocating/reading the payload, so a malicious or
    /// fuzzed responder cannot drive an unbounded allocation (or block) in the
    /// harness response reader.
    #[tokio::test]
    async fn read_frame_rejects_oversize_declared_len_before_allocating_payload(
    ) -> Result<(), BoxError> {
        let server = LocalEndpointFactory::new().endpoint(4040).await?;
        let router = Router::builder(server)
            .accept(TEST_ALPN, OversizeResponder)
            .spawn();
        let server_addr = LocalEndpointFactory::node_addr(router.endpoint()).await;

        let client = LocalEndpointFactory::new().endpoint(4041).await?;
        client.add_node_addr(server_addr.clone())?;
        let connection = client.connect(server_addr, TEST_ALPN).await?;

        let (_send, mut recv) = connection.accept_bi().await?;

        // A correct reader compares the declared length to `max_frame_bytes` and
        // rejects before allocating/reading the payload, so this returns promptly.
        // The unfixed reader allocates `DECLARED_PAYLOAD_LEN` bytes and then blocks
        // reading a payload the responder never sends, so it times out here.
        let outcome = tokio::time::timeout(
            Duration::from_secs(2),
            HostilePeer::read_frame(&mut recv, MAX_FRAME_BYTES),
        )
        .await
        .expect(
            "read_frame must reject the oversize declared length before allocating/reading the \
             payload; a timeout here means payload_len was allocated/read before the \
             max_frame_bytes check",
        );

        let err = outcome.expect_err("oversize declared frame length must be rejected");
        let message = err.to_string();
        assert!(
            message.contains("max_frame_bytes"),
            "expected a max_frame_bytes rejection, got: {message}",
        );

        connection.close(0u32.into(), b"done");
        client.close().await;
        Ok(())
    }
}
