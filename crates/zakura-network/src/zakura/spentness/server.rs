//! Serve verified artifacts under node and per-peer byte rates.
//!
//! A request that would exceed either rate gets a busy reply at once. The
//! server never delays a reply, so an idle server answers at transport speed
//! and a waiting requester never holds a stream open.

use std::{
    collections::{BTreeMap, HashMap},
    ops::Range,
    sync::{Arc, Mutex, RwLock, RwLockReadGuard},
};

use tokio::time::Instant;
use zakura_chain::parameters::spentness_hints::VerifiedArtifact;

use super::{
    wire::{RangeRequest, RangeResponse, DIGEST_LEN, RESPONSE_HEADER_LEN},
    STREAMS,
};
use crate::zakura::{
    BoxRunFuture, Frame, Peer, RequestResponseService, Service, SinkReject, Stream, ZakuraConnId,
    ZakuraPeerId, FRAME_HEADER_BYTES,
};

/// Artifact bytes this node serves per second across all peers (8 MiB/s).
const NODE_BYTES_PER_SECOND: u64 = 8 * 1024 * 1024;
/// Artifact bytes this node serves per second to one peer (2 MiB/s).
///
/// One peer can take at most a quarter of the node rate.
const PEER_BYTES_PER_SECOND: u64 = 2 * 1024 * 1024;

type ArtifactMap = BTreeMap<[u8; DIGEST_LEN], Arc<VerifiedArtifact>>;

/// Immutable verified artifacts served under node and per-peer byte rates.
#[derive(Debug)]
pub struct ArtifactService {
    artifacts: RwLock<ArtifactMap>,
    rate: Mutex<ServeRate>,
}

impl ArtifactService {
    /// Register only artifacts whose owned bytes passed commitment verification.
    pub fn new(artifacts: impl IntoIterator<Item = Arc<VerifiedArtifact>>) -> Self {
        let artifacts = artifacts
            .into_iter()
            .map(|artifact| (artifact.commitment().sha256, artifact))
            .collect();
        Self {
            artifacts: RwLock::new(artifacts),
            rate: Mutex::new(ServeRate::new(Instant::now())),
        }
    }

    /// Digests this node can serve. Advertisements must use this set.
    pub fn available(&self) -> Vec<[u8; 32]> {
        self.read().keys().copied().collect()
    }

    /// Make newly verified owned bytes available for onward serving.
    pub fn insert(&self, artifact: Arc<VerifiedArtifact>) {
        self.artifacts
            .write()
            .expect("artifact map lock is not poisoned because no holder panics")
            .insert(artifact.commitment().sha256, artifact);
    }

    pub(super) fn contains(&self, digest: &[u8; DIGEST_LEN]) -> bool {
        self.read().contains_key(digest)
    }

    pub(super) fn is_empty(&self) -> bool {
        self.read().is_empty()
    }

    fn get(&self, digest: &[u8; DIGEST_LEN]) -> Option<Arc<VerifiedArtifact>> {
        self.read().get(digest).cloned()
    }

    fn read(&self) -> RwLockReadGuard<'_, ArtifactMap> {
        self.artifacts
            .read()
            .expect("artifact map lock is not poisoned because no holder panics")
    }

    /// Answer one range request from `peer`.
    ///
    /// Busy, missing, or oversized responses are availability failures, not peer faults.
    /// A bounded request past the artifact end is also an availability failure.
    fn serve(
        &self,
        peer: &ZakuraPeerId,
        request: RangeRequest,
        response_cap: usize,
    ) -> Result<Frame, SinkReject> {
        let Some(artifact) = self.get(&request.digest) else {
            return Ok(RangeResponse::unavailable(request));
        };
        let Some(range) = artifact_range(&artifact, request)? else {
            return Ok(RangeResponse::out_of_range(request));
        };
        if RESPONSE_HEADER_LEN + range.len() > response_cap {
            return Ok(RangeResponse::too_large(request));
        }
        let admitted = self
            .rate
            .lock()
            .expect("serve rate lock is not poisoned because no holder panics")
            .try_take(peer, u64::from(request.length), Instant::now());
        if !admitted {
            return Ok(RangeResponse::busy(request));
        }
        Ok(RangeResponse::available(request, &artifact.bytes()[range]))
    }
}

/// Byte buckets for the node and for each recently served peer.
///
/// A full peer bucket is equivalent to no entry, so the map keeps only peers
/// served within the last second.
#[derive(Debug)]
struct ServeRate {
    node: ByteBucket,
    peers: HashMap<ZakuraPeerId, ByteBucket>,
}

impl ServeRate {
    fn new(now: Instant) -> Self {
        Self {
            node: ByteBucket::full(NODE_BYTES_PER_SECOND, now),
            peers: HashMap::new(),
        }
    }

    /// Take `bytes` from the node and peer buckets, or take nothing.
    fn try_take(&mut self, peer: &ZakuraPeerId, bytes: u64, now: Instant) -> bool {
        self.node.refill(now);
        if !self.peers.contains_key(peer) {
            self.peers.retain(|_, bucket| {
                bucket.refill(now);
                !bucket.is_full()
            });
        }
        let peer_bucket = self
            .peers
            .entry(peer.clone())
            .or_insert_with(|| ByteBucket::full(PEER_BYTES_PER_SECOND, now));
        peer_bucket.refill(now);
        if self.node.tokens < bytes || peer_bucket.tokens < bytes {
            return false;
        }
        self.node.tokens -= bytes;
        peer_bucket.tokens -= bytes;
        true
    }
}

/// A token bucket over bytes that holds at most one second of its rate.
#[derive(Debug)]
struct ByteBucket {
    bytes_per_second: u64,
    tokens: u64,
    refilled: Instant,
}

impl ByteBucket {
    fn full(bytes_per_second: u64, now: Instant) -> Self {
        Self {
            bytes_per_second,
            tokens: bytes_per_second,
            refilled: now,
        }
    }

    fn is_full(&self) -> bool {
        self.tokens == self.bytes_per_second
    }

    fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.refilled);
        let earned = elapsed
            .as_nanos()
            .saturating_mul(u128::from(self.bytes_per_second))
            / 1_000_000_000;
        if earned == 0 {
            return;
        }
        let earned = u64::try_from(earned).unwrap_or(u64::MAX);
        self.tokens = self
            .tokens
            .saturating_add(earned)
            .min(self.bytes_per_second);
        self.refilled = now;
    }
}

/// Resolve a request to byte indexes inside the artifact.
fn artifact_range(
    artifact: &VerifiedArtifact,
    request: RangeRequest,
) -> Result<Option<Range<usize>>, SinkReject> {
    let end = request.checked_end().map_err(SinkReject::protocol)?;
    if end > artifact.commitment().byte_len {
        return Ok(None);
    }
    let start = usize::try_from(request.offset).map_err(SinkReject::protocol)?;
    let end = usize::try_from(end).map_err(SinkReject::protocol)?;
    Ok(Some(start..end))
}

/// Largest response payload the negotiated frame and message limits allow.
fn response_capacity(max_frame: u32, max_message: u32) -> Result<usize, SinkReject> {
    let max_frame = usize::try_from(max_frame).unwrap_or(usize::MAX);
    let max_message = usize::try_from(max_message).unwrap_or(usize::MAX);
    Some(
        max_frame
            .saturating_sub(FRAME_HEADER_BYTES)
            .min(max_message),
    )
    .filter(|capacity| *capacity >= RESPONSE_HEADER_LEN)
    .ok_or_else(|| SinkReject::local("negotiated frame cap cannot carry a spentness response"))
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

impl RequestResponseService for ArtifactService {
    fn request_frame<'a>(
        &'a self,
        peer: ZakuraPeerId,
        _kind: u16,
        _id: u64,
        max_frame: u32,
        max_message: u32,
        frame: Frame,
    ) -> BoxRunFuture<'a, Result<Vec<Frame>, SinkReject>> {
        Box::pin(async move {
            let request = RangeRequest::parse(&frame).map_err(SinkReject::protocol)?;
            let response_cap = response_capacity(max_frame, max_message)?;
            Ok(vec![self.serve(&peer, request, response_cap)?])
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use zakura_chain::parameters::spentness_hints::{encode, ParsedArtifact};

    use super::*;
    use crate::{
        zakura::spentness::{
            wire::{GET_RANGE, RANGE_BYTES},
            FRAME_CAP, STREAM_KIND,
        },
        BoxError,
    };

    #[tokio::test]
    async fn serving_bounds_are_nonfatal() -> Result<(), BoxError> {
        let bytes = encode([1; 32], 1, [2; 32], [false, true])?;
        let parsed = ParsedArtifact::read(bytes.as_slice())?;
        let commitment = parsed.commitment().clone();
        let service = ArtifactService::new([Arc::new(parsed.verify(&commitment)?)]);
        let peer = ZakuraPeerId::new(vec![1; 32])?;
        let request = RangeRequest {
            digest: commitment.sha256,
            offset: 0,
            length: 1,
        };
        let request_frame = |request: RangeRequest| Frame {
            message_type: GET_RANGE,
            flags: 0,
            payload: request.payload(),
        };
        let call = |max_frame, max_message, frame| {
            service.request_frame(peer.clone(), STREAM_KIND, 0, max_frame, max_message, frame)
        };

        // A bounded range past the artifact end does not blame the peer.
        let past_end = RangeRequest {
            offset: commitment.byte_len,
            ..request
        };
        let response = call(FRAME_CAP, FRAME_CAP, request_frame(past_end)).await?;
        assert!(matches!(
            RangeResponse::parse(&response[0])?,
            RangeResponse::OutOfRange(_)
        ));

        // A frame cap that fits only the response header yields a too-large response.
        let response = call(
            u32::try_from(RESPONSE_HEADER_LEN + FRAME_HEADER_BYTES)?,
            u32::try_from(RESPONSE_HEADER_LEN)?,
            request_frame(request),
        )
        .await?;
        assert_eq!(response[0].payload.len(), RESPONSE_HEADER_LEN);
        assert!(matches!(
            RangeResponse::parse(&response[0])?,
            RangeResponse::TooLarge(_)
        ));
        Ok(())
    }

    /// Status of one full-range request from `peer`.
    fn status(
        service: &ArtifactService,
        peer: u8,
        digest: [u8; DIGEST_LEN],
    ) -> Result<&'static str, BoxError> {
        let request = RangeRequest {
            digest,
            offset: 0,
            length: RANGE_BYTES,
        };
        let frame = service.serve(
            &ZakuraPeerId::new(vec![peer; 32])?,
            request,
            RESPONSE_HEADER_LEN + usize::try_from(RANGE_BYTES)?,
        )?;
        Ok(match RangeResponse::parse(&frame)? {
            RangeResponse::Available { .. } => "available",
            RangeResponse::Busy(_) => "busy",
            _ => "other",
        })
    }

    fn full_range_service() -> Result<(ArtifactService, [u8; DIGEST_LEN]), BoxError> {
        let bytes = encode(
            [1; 32],
            1,
            [2; 32],
            (0..u64::from(RANGE_BYTES) * 8).map(|_| false),
        )?;
        let parsed = ParsedArtifact::read(bytes.as_slice())?;
        let commitment = parsed.commitment().clone();
        let digest = commitment.sha256;
        Ok((
            ArtifactService::new([Arc::new(parsed.verify(&commitment)?)]),
            digest,
        ))
    }

    #[tokio::test(start_paused = true)]
    async fn one_peer_cannot_take_the_node_rate() -> Result<(), BoxError> {
        let (service, digest) = full_range_service()?;
        let ranges_per_peer = PEER_BYTES_PER_SECOND / u64::from(RANGE_BYTES);

        // An idle node serves a peer's full burst at once, then answers busy.
        for _ in 0..ranges_per_peer {
            assert_eq!(status(&service, 1, digest)?, "available");
        }
        assert_eq!(status(&service, 1, digest)?, "busy");

        // Another peer still gets its share while the first waits.
        assert_eq!(status(&service, 2, digest)?, "available");

        // The first peer's bucket refills at its own rate.
        tokio::time::advance(Duration::from_secs(1) / u32::try_from(ranges_per_peer)?).await;
        assert_eq!(status(&service, 1, digest)?, "available");
        assert_eq!(status(&service, 1, digest)?, "busy");
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn the_node_rate_bounds_all_peers() -> Result<(), BoxError> {
        let (service, digest) = full_range_service()?;
        let mut served = 0;
        let mut peer = 0;
        loop {
            peer += 1;
            if status(&service, peer, digest)? == "busy" {
                break;
            }
            served += u64::from(RANGE_BYTES);
            while status(&service, peer, digest)? == "available" {
                served += u64::from(RANGE_BYTES);
            }
        }
        assert_eq!(served, NODE_BYTES_PER_SECOND);

        // The node bucket refills, and the busy replies charged nothing.
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(status(&service, peer, digest)?, "available");
        Ok(())
    }
}
