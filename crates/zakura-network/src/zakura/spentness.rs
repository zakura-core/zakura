//! Bounded peer transfer of whole-file authenticated spentness artifacts.

use std::{
    collections::BTreeMap,
    fs::File,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::Duration,
};

use tokio::{
    io::AsyncWriteExt,
    sync::Semaphore,
    time::{sleep, timeout},
};
use zakura_chain::{
    common::atomic_write,
    parameters::spentness_hints::{Commitment, VerifiedArtifact},
};

use super::{
    BoxRunFuture, CustomService, Frame, Peer, RequestResponseService, Service, SinkReject, Stream,
    StreamMode, ZakuraConnId, ZakuraPeerHandle, ZakuraPeerId, ZakuraServiceId,
    ZakuraSupervisorHandle,
};
use crate::BoxError;

/// Spentness artifact request/response stream.
pub const STREAM_KIND: u16 = 8;
/// Negotiated support for the artifact protocol, independent of artifact availability.
pub const CAPABILITY: u64 = 1 << 6;
/// Largest range payload (256 KiB).
pub const RANGE_BYTES: u32 = 256 * 1024;
/// Request message type: digest, offset, and length in little-endian encoding.
pub const GET_RANGE: u16 = 1;
/// Response message type. Status 0 is unavailable; status 1 includes a range.
pub const RANGE: u16 = 2;
const RESPONSE_HEADER: usize = 45;
const STREAMS: &[Stream] = &[Stream {
    kind: STREAM_KIND,
    version: 1,
    frame_cap: RANGE_BYTES + 64,
    capability: CAPABILITY,
    mode: StreamMode::RequestResponse,
}];
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Load supported cache entries and prepare protocol negotiation and discovery.
pub async fn prepare(
    cache: PathBuf,
    pins: &'static [Commitment],
) -> Result<(Arc<ArtifactService>, CustomService), BoxError> {
    let artifacts = tokio::task::spawn_blocking(move || {
        pins.iter().filter_map(|pin| match load(&cache, pin) {
            Ok(artifact) => Some(Arc::new(artifact)),
            Err(error) => { tracing::debug!(%error, digest = %hex::encode(pin.sha256), "spentness cache entry unavailable"); None }
        }).collect::<Vec<_>>()
    }).await?;
    let service = Arc::new(ArtifactService::new(artifacts));
    let id = ZakuraServiceId::new("zakura.spentness.v1")?;
    let provides = if service.available().is_empty() {
        Vec::new()
    } else {
        vec![id.clone()]
    };
    let custom = CustomService {
        service: service.clone(),
        provides,
        seeks: if pins.is_empty() {
            Vec::new()
        } else {
            vec![id]
        },
    };
    Ok((service, custom))
}

/// Attempt acquisition after bootstrap. Ordinary sync can continue if peers lack the artifact.
pub async fn download_missing(
    cache: PathBuf,
    pins: &'static [Commitment],
    service: Arc<ArtifactService>,
    supervisor: ZakuraSupervisorHandle,
) {
    for pin in pins.iter().rev() {
        if service.available().contains(&pin.sha256) {
            continue;
        }
        let mut changes = supervisor.subscribe();
        let peers = timeout(Duration::from_secs(60), async {
            loop {
                let peers = supervisor
                    .outbound_peer_handles_for_capability(CAPABILITY)
                    .await;
                if !peers.is_empty() {
                    return peers;
                }
                if changes.changed().await.is_err() {
                    return Vec::new();
                }
            }
        })
        .await
        .unwrap_or_default();
        match acquire(&cache, pin, &peers).await {
            Ok(artifact) => {
                tracing::info!(digest = %hex::encode(pin.sha256), bytes = pin.byte_len, "verified spentness artifact from peers");
                service.insert(artifact);
            }
            Err(error) => {
                tracing::warn!(%error, digest = %hex::encode(pin.sha256), "spentness artifact acquisition failed")
            }
        }
    }
}

/// Immutable verified artifacts with bounded aggregate serving concurrency and rate.
#[derive(Debug)]
pub struct ArtifactService {
    artifacts: RwLock<BTreeMap<[u8; 32], Arc<VerifiedArtifact>>>,
    serving: Semaphore,
}

impl ArtifactService {
    /// Register only artifacts whose owned bytes passed commitment verification.
    pub fn new(artifacts: impl IntoIterator<Item = Arc<VerifiedArtifact>>) -> Self {
        Self {
            artifacts: RwLock::new(
                artifacts
                    .into_iter()
                    .map(|artifact| (artifact.commitment().sha256, artifact))
                    .collect(),
            ),
            serving: Semaphore::new(4),
        }
    }

    /// Digests this node can serve. Advertisements must use this set.
    pub fn available(&self) -> Vec<[u8; 32]> {
        self.artifacts
            .read()
            .expect("artifact map lock is not poisoned")
            .keys()
            .copied()
            .collect()
    }

    /// Make newly verified owned bytes available for onward serving.
    pub fn insert(&self, artifact: Arc<VerifiedArtifact>) {
        self.artifacts
            .write()
            .expect("artifact map lock is not poisoned")
            .insert(artifact.commitment().sha256, artifact);
    }
}

impl Service for ArtifactService {
    fn name(&self) -> &'static str {
        "spentness"
    }
    fn streams(&self) -> &'static [Stream] {
        STREAMS
    }
    fn add_peer(&self, _peer: Peer) {}
    fn remove_peer(&self, _peer: &ZakuraPeerId, _conn: ZakuraConnId) {}
    fn as_request_response(&self) -> Option<&dyn RequestResponseService> {
        Some(self)
    }
}

fn request_fields(frame: &Frame) -> Result<([u8; 32], u64, u32), BoxError> {
    if frame.message_type != GET_RANGE || frame.flags != 0 || frame.payload.len() != 44 {
        return Err("invalid spentness range request".into());
    }
    let digest = frame.payload[..32].try_into()?;
    let offset = u64::from_le_bytes(frame.payload[32..40].try_into()?);
    let length = u32::from_le_bytes(frame.payload[40..44].try_into()?);
    if length == 0 || length > RANGE_BYTES || offset.checked_add(u64::from(length)).is_none() {
        return Err("spentness range exceeds limits".into());
    }
    Ok((digest, offset, length))
}

impl RequestResponseService for ArtifactService {
    fn request_frame<'a>(
        &'a self,
        _peer: ZakuraPeerId,
        _kind: u16,
        _id: u64,
        max_frame: u32,
        max_message: u32,
        frame: Frame,
    ) -> BoxRunFuture<'a, Result<Vec<Frame>, SinkReject>> {
        Box::pin(async move {
            let (digest, offset, length) = request_fields(&frame).map_err(SinkReject::protocol)?;
            let permit = self.serving.try_acquire().ok();
            let artifact = permit.as_ref().and_then(|_| {
                self.artifacts
                    .read()
                    .expect("artifact map lock is not poisoned")
                    .get(&digest)
                    .cloned()
            });
            let cap = max_frame.saturating_sub(8).min(max_message);
            if cap < u32::try_from(RESPONSE_HEADER).expect("response header fits u32") {
                return Err(SinkReject::local(
                    "negotiated frame cap cannot carry a spentness response",
                ));
            }
            let mut payload = vec![0];
            payload.extend_from_slice(&digest);
            payload.extend_from_slice(&offset.to_le_bytes());
            payload.extend_from_slice(&0u32.to_le_bytes());
            if let Some(artifact) = artifact {
                let end = offset + u64::from(length);
                if end > artifact.commitment().byte_len {
                    return Err(SinkReject::protocol(
                        "spentness range exceeds artifact length",
                    ));
                }
                if u64::from(length) + u64::try_from(RESPONSE_HEADER).expect("header fits u64")
                    <= u64::from(cap)
                {
                    payload[0] = 1;
                    payload[41..45].copy_from_slice(&length.to_le_bytes());
                    payload.extend_from_slice(
                        &artifact.bytes()[usize::try_from(offset).map_err(SinkReject::protocol)?
                            ..usize::try_from(end).map_err(SinkReject::protocol)?],
                    );
                    // Four slots, each at most 256 KiB per 250 ms: aggregate <= 4 MiB/s.
                    sleep(Duration::from_millis(250)).await;
                }
            }
            Ok(vec![Frame {
                message_type: RANGE,
                flags: 0,
                payload,
            }])
        })
    }
}

/// Validate the single bounded response before the transport stores it.
pub(crate) fn validate_response(frame: &Frame) -> Result<(), BoxError> {
    if frame.message_type != RANGE || frame.flags != 0 || frame.payload.len() < RESPONSE_HEADER {
        return Err("invalid spentness response".into());
    }
    let length = u32::from_le_bytes(frame.payload[41..45].try_into()?);
    if length > RANGE_BYTES
        || frame.payload.len() != RESPONSE_HEADER + usize::try_from(length)?
        || !matches!((frame.payload[0], length), (0, 0) | (1, 1..))
    {
        return Err("invalid spentness response status or length".into());
    }
    Ok(())
}

/// Load and reverify an exact content-addressed cache entry.
pub fn load(cache: &Path, pin: &Commitment) -> Result<VerifiedArtifact, BoxError> {
    Ok(VerifiedArtifact::read(
        File::open(cache.join(format!("{}.bin", hex::encode(pin.sha256))))?,
        pin,
    )?)
}

/// Durably publish verified owned bytes, including the parent directory entry.
pub fn publish(cache: &Path, artifact: &VerifiedArtifact) -> Result<PathBuf, BoxError> {
    let path = cache.join(format!("{}.bin", hex::encode(artifact.commitment().sha256)));
    atomic_write(path.clone(), artifact.bytes())??;
    Ok(path)
}

/// Acquire from at most three single sources. Resume only bytes from the same peer.
///
/// The caller selects a release-authorized commitment. Peer responses cannot replace it.
pub async fn acquire(
    cache: &Path,
    pin: &Commitment,
    peers: &[ZakuraPeerHandle],
) -> Result<Arc<VerifiedArtifact>, BoxError> {
    pin.validate()?;
    let owned_cache = cache.to_owned();
    let owned_pin = pin.clone();
    if let Ok(artifact) =
        tokio::task::spawn_blocking(move || load(&owned_cache, &owned_pin)).await?
    {
        return Ok(Arc::new(artifact));
    }
    tokio::fs::create_dir_all(cache).await?;
    for peer in peers.iter().take(3) {
        if let Ok(artifact) = acquire_from_peer(cache, pin, peer).await {
            return Ok(Arc::new(artifact));
        }
    }
    Err("no peer supplied the required spentness artifact; retry or provision its exact verified cache entry".into())
}

async fn acquire_from_peer(
    cache: &Path,
    pin: &Commitment,
    peer: &ZakuraPeerHandle,
) -> Result<VerifiedArtifact, BoxError> {
    let path = cache.join(format!(
        "{}.{}.part",
        hex::encode(pin.sha256),
        hex::encode(peer.peer_id().digest())
    ));
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await?;
    let mut offset = file.metadata().await?.len();
    if offset > pin.byte_len {
        file.set_len(0).await?;
        offset = 0;
    }
    while offset < pin.byte_len {
        let length = u32::try_from((pin.byte_len - offset).min(u64::from(RANGE_BYTES)))?;
        let mut request = pin.sha256.to_vec();
        request.extend_from_slice(&offset.to_le_bytes());
        request.extend_from_slice(&length.to_le_bytes());
        let frames = timeout(
            REQUEST_TIMEOUT,
            peer.request(STREAM_KIND, offset, GET_RANGE, 0, request),
        )
        .await??;
        if frames.len() != 1 {
            return Err("spentness peer returned an invalid response count".into());
        }
        let frame = &frames[0];
        validate_response(frame)?;
        if frame.payload[0] == 0 {
            return Err("spentness artifact is unavailable at this peer".into());
        }
        if frame.payload[1..33] != pin.sha256
            || frame.payload[33..41] != offset.to_le_bytes()
            || frame.payload[41..45] != length.to_le_bytes()
        {
            return Err("spentness peer returned a different range".into());
        }
        file.write_all(&frame.payload[RESPONSE_HEADER..]).await?;
        file.sync_data().await?;
        offset += u64::from(length);
    }
    drop(file);
    let source = path.clone();
    let expected = pin.clone();
    let verified = tokio::task::spawn_blocking(move || -> Result<_, BoxError> {
        Ok(VerifiedArtifact::read(File::open(source)?, &expected)?)
    })
    .await?;
    let artifact = match verified {
        Ok(artifact) => artifact,
        Err(error) => {
            tokio::fs::remove_file(path).await?;
            return Err(error);
        }
    };
    let cache = cache.to_owned();
    let artifact = tokio::task::spawn_blocking(move || -> Result<_, BoxError> {
        publish(&cache, &artifact)?;
        Ok(artifact)
    })
    .await??;
    tokio::fs::remove_file(path).await?;
    Ok(artifact)
}

#[cfg(test)]
mod tests {
    use super::*;
    use zakura_chain::parameters::spentness_hints::{encode, ParsedArtifact};

    #[derive(Debug)]
    struct Noop;
    impl Service for Noop {
        fn name(&self) -> &'static str {
            "noop"
        }
        fn streams(&self) -> &[Stream] {
            &[]
        }
        fn add_peer(&self, _peer: Peer) {}
        fn remove_peer(&self, _peer: &ZakuraPeerId, _conn: ZakuraConnId) {}
    }

    #[tokio::test]
    async fn spentness_seed_only_transfer_resumes_and_serves_verified_bytes() -> Result<(), BoxError>
    {
        let _guard = zakura_test::init();
        let bytes = encode(
            [1; 32],
            10,
            [2; 32],
            (0..(u64::from(RANGE_BYTES) * 8 + 9)).map(|n| n != 0 && n % 2 == 0),
        )?;
        let parsed = ParsedArtifact::read(bytes.as_slice())?;
        let pin = parsed.commitment().clone();
        let server_service = Arc::new(ArtifactService::new([Arc::new(parsed.verify(&pin)?)]));
        let server_identity = tempfile::tempdir()?;
        let client_identity = tempfile::tempdir()?;
        let cache = tempfile::tempdir()?;
        let service_id = ZakuraServiceId::new("zakura.spentness.v1")?;
        let mut server_config = crate::Config::for_test(crate::P2pStack::Dual);
        server_config.identity_dir = server_identity.path().to_owned();
        server_config.zakura.listen_addr = Some("127.0.0.1:0".parse()?);
        server_config.zakura.bootstrap_peers.clear();
        let server = super::super::spawn_zakura_endpoint_with_services(
            &server_config,
            |_, _| Arc::new(Noop),
            None,
            vec![CustomService {
                service: server_service,
                provides: vec![service_id.clone()],
                seeks: Vec::new(),
            }],
        )
        .await?
        .ok_or("server endpoint missing")?;
        let server_addr = server.node_addr().await;
        let direct = server_addr
            .direct_addresses()
            .copied()
            .find(|addr| addr.ip().is_loopback())
            .ok_or("server has no loopback address")?;
        let mut client_config = server_config;
        client_config.identity_dir = client_identity.path().to_owned();
        client_config.zakura.bootstrap_peers = vec![format!("{}@{direct}", server_addr.node_id)];
        let client_service = Arc::new(ArtifactService::new([]));
        let client = super::super::spawn_zakura_endpoint_with_services(
            &client_config,
            |_, _| Arc::new(Noop),
            None,
            vec![CustomService {
                service: client_service.clone(),
                provides: Vec::new(),
                seeks: vec![service_id],
            }],
        )
        .await?
        .ok_or("client endpoint missing")?;
        let result = timeout(Duration::from_secs(20), async {
            let supervisor = client.supervisor();
            let mut changes = supervisor.subscribe();
            let peer = loop {
                if let Some(peer) = supervisor
                    .outbound_peer_handles_for_capability(CAPABILITY)
                    .await
                    .into_iter()
                    .next()
                {
                    break peer;
                }
                changes.changed().await?;
            };
            let partial = cache.path().join(format!(
                "{}.{}.part",
                hex::encode(pin.sha256),
                hex::encode(peer.peer_id().digest())
            ));
            tokio::fs::write(&partial, &bytes[..128]).await?;
            let artifact = acquire(cache.path(), &pin, std::slice::from_ref(&peer)).await?;
            assert_eq!(artifact.bytes(), bytes);
            assert!(!partial.exists());
            assert_eq!(load(cache.path(), &pin)?.bytes(), bytes);
            client_service.insert(artifact);
            assert_eq!(client_service.available(), vec![pin.sha256]);
            let mut request = [3; 32].to_vec();
            request.extend_from_slice(&0u64.to_le_bytes());
            request.extend_from_slice(&1u32.to_le_bytes());
            let response = peer.request(STREAM_KIND, 0, GET_RANGE, 0, request).await?;
            assert_eq!(
                response[0].payload[0], 0,
                "missing artifacts are availability failures"
            );
            Ok::<_, BoxError>(())
        })
        .await;
        client.shutdown().await;
        server.shutdown().await;
        result??;
        Ok(())
    }

    #[tokio::test]
    async fn spentness_serving_bounds_and_busy_are_nonfatal() -> Result<(), BoxError> {
        let bytes = encode([1; 32], 1, [2; 32], [false, true])?;
        let parsed = ParsedArtifact::read(bytes.as_slice())?;
        let pin = parsed.commitment().clone();
        let service = ArtifactService::new([Arc::new(parsed.verify(&pin)?)]);
        let peer = ZakuraPeerId::new(vec![1; 32])?;
        let mut request = pin.sha256.to_vec();
        request.extend_from_slice(&0u64.to_le_bytes());
        request.extend_from_slice(&1u32.to_le_bytes());
        let frame = Frame {
            message_type: GET_RANGE,
            flags: 0,
            payload: request,
        };
        let permit = service.serving.acquire_many(4).await?;
        let response = service
            .request_frame(
                peer.clone(),
                STREAM_KIND,
                0,
                RANGE_BYTES + 64,
                RANGE_BYTES + 64,
                frame.clone(),
            )
            .await?;
        assert_eq!(response[0].payload[0], 0);
        drop(permit);
        let mut bad_range = frame.clone();
        bad_range.payload[32..40].copy_from_slice(&pin.byte_len.to_le_bytes());
        assert!(matches!(
            service
                .request_frame(
                    peer.clone(),
                    STREAM_KIND,
                    0,
                    RANGE_BYTES + 64,
                    RANGE_BYTES + 64,
                    bad_range
                )
                .await,
            Err(SinkReject::Protocol(_))
        ));
        let response = service
            .request_frame(peer, STREAM_KIND, 0, 53, 45, frame)
            .await?;
        assert_eq!(response[0].payload.len(), RESPONSE_HEADER);
        assert_eq!(response[0].payload[0], 0);
        Ok(())
    }

    #[test]
    fn cache_reverification_rejects_corruption() {
        let directory = tempfile::tempdir().unwrap();
        let bytes = encode([1; 32], 1, [2; 32], [false, true]).unwrap();
        let parsed = ParsedArtifact::read(bytes.as_slice()).unwrap();
        let pin = parsed.commitment().clone();
        let verified = parsed.verify(&pin).unwrap();
        let path = publish(directory.path(), &verified).unwrap();
        assert!(load(directory.path(), &pin).unwrap().retains(1).unwrap());
        std::fs::write(path, b"corrupt").unwrap();
        assert!(load(directory.path(), &pin).is_err());
    }

    #[test]
    fn range_bounds() {
        let mut payload = vec![0; 40];
        payload.extend_from_slice(&RANGE_BYTES.to_le_bytes());
        let mut frame = Frame {
            message_type: GET_RANGE,
            flags: 0,
            payload,
        };
        assert!(request_fields(&frame).is_ok());
        frame.payload[32..40].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(request_fields(&frame).is_err());
        frame.payload.truncate(43);
        assert!(request_fields(&frame).is_err());
    }

    fn scripted_peer(
        identity: u8,
        bytes: Vec<u8>,
    ) -> (ZakuraPeerHandle, tokio::task::JoinHandle<()>) {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let peer =
            ZakuraPeerHandle::new_for_tests(ZakuraPeerId::new(vec![identity; 32]).unwrap(), sender);
        let task = tokio::spawn(async move {
            while let Some(super::super::ZakuraOutboundFrame::Request {
                payload,
                completion,
                ..
            }) = receiver.recv().await
            {
                let offset = u64::from_le_bytes(payload[32..40].try_into().unwrap());
                let length = u32::from_le_bytes(payload[40..44].try_into().unwrap());
                let mut response = vec![1];
                response.extend_from_slice(&payload);
                let start = usize::try_from(offset).unwrap();
                response.extend_from_slice(&bytes[start..start + usize::try_from(length).unwrap()]);
                let _ = completion.send(Ok(vec![Frame {
                    message_type: RANGE,
                    flags: 0,
                    payload: response,
                }]));
            }
        });
        (peer, task)
    }

    #[tokio::test]
    async fn spentness_retries_single_sources_after_whole_file_mismatch() -> Result<(), BoxError> {
        let bytes = encode([1; 32], 1, [2; 32], [false, true, false])?;
        let pin = ParsedArtifact::read(bytes.as_slice())?.commitment().clone();
        let mut corrupt = bytes.clone();
        corrupt[86] ^= 4;
        let (bad, bad_task) = scripted_peer(1, corrupt);
        let (good, good_task) = scripted_peer(2, bytes.clone());
        let cache = tempfile::tempdir()?;
        let result = timeout(Duration::from_secs(5), async {
            assert!(acquire(cache.path(), &pin, std::slice::from_ref(&bad))
                .await
                .is_err());
            assert!(
                load(cache.path(), &pin).is_err(),
                "unverified data must not enter the cache"
            );
            let verified = acquire(cache.path(), &pin, &[bad, good]).await?;
            assert_eq!(verified.bytes(), bytes);
            assert_eq!(
                std::fs::read_dir(cache.path())?.count(),
                1,
                "discard corrupt partials and retain only the verified file"
            );
            Ok::<_, BoxError>(())
        })
        .await;
        bad_task.abort();
        good_task.abort();
        let _ = bad_task.await;
        let _ = good_task.await;
        result??;
        Ok(())
    }
}
