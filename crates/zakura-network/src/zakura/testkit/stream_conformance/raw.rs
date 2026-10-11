//! A raw peer that opens any layout by hand and can break any rule.
//!
//! Generalized from #945's `ensure_block_pair`. The peer runs the native
//! handshake, then opens the layout itself or accepts it from the victim,
//! following the transport's opener tiebreak. A multi-stream layout carries
//! its eight-byte session wire id after each member's prelude.

use iroh::{
    endpoint::{Connection, RecvStream, SendStream},
    Endpoint, EndpointAddr,
};
use zakura_chain::parameters::Network;

use super::CONFORMANCE_DEADLINE;
use crate::{
    zakura::{
        handler::{i_open_collision_winner, run_native_initiator_handshake_without_trace},
        testkit::{HostilePeer, LocalEndpointFactory},
        Frame, Stream, StreamPrelude, ZakuraHandshakeConfig, ZakuraLocalLimits, ZakuraPeerId,
        P2P_V2_ALPN, STREAM_PRELUDE_MAGIC,
    },
    BoxError,
};

/// A raw connection to a victim node, with the members of one layout.
#[derive(Debug)]
pub(crate) struct RawLayoutPeer {
    endpoint: Endpoint,
    pub(crate) connection: Connection,
    limits: ZakuraLocalLimits,
    /// The layout's members, in layout order, once opened.
    members: Vec<(SendStream, RecvStream)>,
    /// Sibling streams, by kind, once opened.
    siblings: std::collections::HashMap<u16, (SendStream, RecvStream)>,
}

async fn within<T>(
    what: &'static str,
    future: impl std::future::Future<Output = Result<T, BoxError>>,
) -> Result<T, BoxError> {
    tokio::time::timeout(CONFORMANCE_DEADLINE, future)
        .await
        .map_err(|_| format!("timed out: {what}"))?
}

impl RawLayoutPeer {
    /// Connect to `victim` and run the native handshake, offering
    /// `capabilities`.
    pub(crate) async fn connect(
        victim: EndpointAddr,
        limits: &ZakuraLocalLimits,
        seed: u64,
        capabilities: u64,
    ) -> Result<Self, BoxError> {
        let endpoint = LocalEndpointFactory::with_transport_config(limits.transport_config())
            .endpoint(seed)
            .await?;
        let connection = within("connect", async {
            Ok(endpoint.connect(victim, P2P_V2_ALPN).await?)
        })
        .await?;
        let mut config = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        config.supported_capabilities = capabilities;
        let local = ZakuraPeerId::new(endpoint.id().as_bytes().to_vec())?;
        within("handshake", async {
            run_native_initiator_handshake_without_trace(&connection, limits, &config, &local)
                .await?;
            Ok(())
        })
        .await?;
        Ok(Self {
            endpoint,
            connection,
            limits: limits.clone(),
            members: Vec::new(),
            siblings: std::collections::HashMap::new(),
        })
    }

    /// This peer's identity.
    pub(crate) fn id(&self) -> ZakuraPeerId {
        ZakuraPeerId::new(self.endpoint.id().as_bytes().to_vec())
            .expect("an endpoint id is a valid peer id")
    }

    /// Open a stream with a prelude for `kind` at `version`.
    pub(crate) async fn open_stream(
        &self,
        kind: u16,
        version: u16,
    ) -> Result<(SendStream, RecvStream), BoxError> {
        within("open a stream", async {
            let (mut send, recv) = self.connection.open_bi().await?;
            let prelude = StreamPrelude {
                magic: STREAM_PRELUDE_MAGIC,
                stream_kind: kind,
                stream_version: version,
                request_id: None,
                max_frame_bytes: self.limits.max_frame_bytes,
            };
            send.write_all(&prelude.encode()?).await?;
            Ok((send, recv))
        })
        .await
    }

    /// Open `layout`, or accept it from the victim if the victim wins the
    /// opener tiebreak. Any earlier members are dropped.
    pub(crate) async fn open_layout(&mut self, layout: &[Stream]) -> Result<(), BoxError> {
        self.members.clear();
        let multi = layout.len() > 1;
        if i_open_collision_winner(&self.endpoint.id(), &self.connection.remote_id()) {
            let wire_id: u64 = rand::random::<u64>().max(1);
            for member in layout {
                let (mut send, recv) = self.open_stream(member.kind, member.version).await?;
                if multi {
                    send.write_all(&wire_id.to_le_bytes()).await?;
                }
                self.members.push((send, recv));
            }
            return Ok(());
        }
        let mut accepted: Vec<Option<(SendStream, RecvStream)>> =
            layout.iter().map(|_| None).collect();
        let mut wire_id = None;
        within("accept the layout", async {
            while accepted.iter().any(Option::is_none) {
                let (mut send, mut recv) = self.connection.accept_bi().await?;
                let prelude = HostilePeer::read_prelude(&mut recv).await?;
                let Some(index) = layout.iter().position(|member| {
                    member.kind == prelude.stream_kind && member.version == prelude.stream_version
                }) else {
                    // Another service's stream.
                    send.reset(0u32.into())?;
                    recv.stop(0u32.into())?;
                    continue;
                };
                if multi {
                    let mut id = [0; 8];
                    recv.read_exact(&mut id).await?;
                    let id = u64::from_le_bytes(id);
                    if id == 0 || wire_id.is_some_and(|previous| previous != id) {
                        return Err("the victim opened members with different wire ids".into());
                    }
                    wire_id = Some(id);
                }
                accepted[index] = Some((send, recv));
            }
            Ok(())
        })
        .await?;
        self.members = accepted.into_iter().flatten().collect();
        Ok(())
    }

    fn member(&mut self, member: usize) -> Result<&mut (SendStream, RecvStream), BoxError> {
        self.members
            .get_mut(member)
            .ok_or_else(|| "the layout is not open".into())
    }

    /// Write raw bytes on a member.
    pub(crate) async fn write(&mut self, member: usize, bytes: &[u8]) -> Result<(), BoxError> {
        let (send, _) = self.member(member)?;
        within("write", async { Ok(send.write_all(bytes).await?) }).await
    }

    /// Send one frame on a member.
    pub(crate) async fn send(&mut self, member: usize, frame: &Frame) -> Result<(), BoxError> {
        let bytes = frame.encode(self.limits.max_frame_bytes)?;
        self.write(member, &bytes).await
    }

    /// Receive the next frame on a member.
    pub(crate) async fn recv(&mut self, member: usize) -> Result<Frame, BoxError> {
        let max_frame_bytes = self.limits.max_frame_bytes;
        let (_, recv) = self.member(member)?;
        within(
            "receive a frame",
            HostilePeer::read_frame(recv, max_frame_bytes),
        )
        .await
    }

    /// Reset a member in both directions.
    pub(crate) fn reset(&mut self, member: usize) -> Result<(), BoxError> {
        let (send, recv) = self.member(member)?;
        send.reset(0u32.into())?;
        recv.stop(0u32.into())?;
        Ok(())
    }

    /// Finish a member's send direction.
    pub(crate) fn finish(&mut self, member: usize) -> Result<(), BoxError> {
        let (send, _) = self.member(member)?;
        send.finish()?;
        Ok(())
    }

    /// Wait until a member's receive direction ends, by FIN or reset.
    pub(crate) async fn ended(&mut self, member: usize) -> Result<(), BoxError> {
        let (_, recv) = self.member(member)?;
        within("a member's end", async {
            let mut buffer = [0; 1024];
            loop {
                match recv.read(&mut buffer).await {
                    Ok(Some(_)) => {}
                    Ok(None) | Err(_) => return Ok(()),
                }
            }
        })
        .await
    }

    /// Whether the victim has closed the connection.
    pub(crate) fn is_closed(&self) -> bool {
        self.connection.close_reason().is_some()
    }

    /// Wait until the victim closes the connection.
    pub(crate) async fn closed(&self) -> Result<(), BoxError> {
        within("the connection's close", async {
            self.connection.closed().await;
            Ok(())
        })
        .await
    }

    /// Exchange one probe with the victim's sibling `sibling`.
    pub(crate) async fn probe_sibling(&mut self, sibling: &Stream) -> Result<(), BoxError> {
        if !self.siblings.contains_key(&sibling.kind) {
            let stream = self.open_stream(sibling.kind, sibling.version).await?;
            self.siblings.insert(sibling.kind, stream);
        }
        let (send, recv) = self
            .siblings
            .get_mut(&sibling.kind)
            .expect("the sibling stream was just opened");
        let probe = Frame {
            message_type: super::sibling::PROBE,
            flags: 0,
            payload: 7u64.to_le_bytes().to_vec(),
        };
        let max_frame_bytes = self.limits.max_frame_bytes;
        within("the sibling's echo", async {
            send.write_all(&probe.encode(max_frame_bytes)?).await?;
            let reply = HostilePeer::read_frame(recv, max_frame_bytes).await?;
            if reply.message_type != super::sibling::REPLY || reply.payload != probe.payload {
                return Err("the sibling did not echo the probe".into());
            }
            Ok(())
        })
        .await
    }

    /// Close the connection and the endpoint.
    pub(crate) async fn shutdown(self) {
        self.connection.close(0u32.into(), b"done");
        self.endpoint.close().await;
    }
}
