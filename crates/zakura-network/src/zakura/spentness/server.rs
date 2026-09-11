//! Serve verified artifacts with bounded aggregate concurrency and rate.

use std::{
    collections::BTreeMap,
    ops::Range,
    sync::{Arc, RwLock, RwLockReadGuard},
    time::Duration,
};

use tokio::{sync::Semaphore, time::sleep};
use zakura_chain::parameters::spentness_hints::VerifiedArtifact;

use super::{
    wire::{RangeRequest, RangeResponse, DIGEST_LEN, RESPONSE_HEADER_LEN},
    STREAMS,
};
use crate::zakura::{
    BoxRunFuture, Frame, Peer, RequestResponseService, Service, SinkReject, Stream, ZakuraConnId,
    ZakuraPeerId,
};

/// Range preparations that may run at once across all peers.
pub(super) const MAX_CONCURRENT_SERVES: usize = 4;
/// Each successful preparation holds its slot this long.
///
/// Four slots, each at most 256 KiB per 250 ms, limit aggregate data to 4 MiB/s.
const SERVE_DELAY: Duration = Duration::from_millis(250);
/// Bytes the transport adds to each frame beyond the message payload.
pub(super) const TRANSPORT_FRAME_OVERHEAD: u32 = 8;

type ArtifactMap = BTreeMap<[u8; DIGEST_LEN], Arc<VerifiedArtifact>>;

/// Immutable verified artifacts with bounded aggregate serving concurrency and rate.
#[derive(Debug)]
pub struct ArtifactService {
    artifacts: RwLock<ArtifactMap>,
    pub(super) serving: Semaphore,
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
            serving: Semaphore::new(MAX_CONCURRENT_SERVES),
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

    /// Answer one range request.
    ///
    /// Busy, missing, or oversized responses are availability failures, not peer faults.
    /// Only a range past the artifact end is a protocol violation.
    async fn serve(&self, request: RangeRequest, response_cap: usize) -> Result<Frame, SinkReject> {
        let Ok(_permit) = self.serving.try_acquire() else {
            return Ok(RangeResponse::unavailable(request));
        };
        let Some(artifact) = self.get(&request.digest) else {
            return Ok(RangeResponse::unavailable(request));
        };
        let range = artifact_range(&artifact, request)?;
        if RESPONSE_HEADER_LEN + range.len() > response_cap {
            return Ok(RangeResponse::unavailable(request));
        }

        let response = RangeResponse::available(request, &artifact.bytes()[range]);
        sleep(SERVE_DELAY).await;
        Ok(response)
    }
}

/// Resolve a request to byte indexes inside the artifact.
fn artifact_range(
    artifact: &VerifiedArtifact,
    request: RangeRequest,
) -> Result<Range<usize>, SinkReject> {
    let end = request.checked_end().map_err(SinkReject::protocol)?;
    if end > artifact.commitment().byte_len {
        return Err(SinkReject::protocol(
            "spentness range exceeds artifact length",
        ));
    }
    let start = usize::try_from(request.offset).map_err(SinkReject::protocol)?;
    let end = usize::try_from(end).map_err(SinkReject::protocol)?;
    Ok(start..end)
}

/// Largest response payload the negotiated frame and message limits allow.
fn response_capacity(max_frame: u32, max_message: u32) -> Result<usize, SinkReject> {
    let capacity = max_frame
        .saturating_sub(TRANSPORT_FRAME_OVERHEAD)
        .min(max_message);
    usize::try_from(capacity)
        .ok()
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
        _peer: ZakuraPeerId,
        _kind: u16,
        _id: u64,
        max_frame: u32,
        max_message: u32,
        frame: Frame,
    ) -> BoxRunFuture<'a, Result<Vec<Frame>, SinkReject>> {
        Box::pin(async move {
            let request = RangeRequest::parse(&frame).map_err(SinkReject::protocol)?;
            let response_cap = response_capacity(max_frame, max_message)?;
            Ok(vec![self.serve(request, response_cap).await?])
        })
    }
}

#[cfg(test)]
mod tests {
    use zakura_chain::parameters::spentness_hints::{encode, ParsedArtifact};

    use super::*;
    use crate::{
        zakura::spentness::{wire::GET_RANGE, FRAME_CAP, STREAM_KIND},
        BoxError,
    };

    #[tokio::test]
    async fn serving_bounds_and_busy_are_nonfatal() -> Result<(), BoxError> {
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

        // Every serving slot is busy.
        let permit = service
            .serving
            .acquire_many(u32::try_from(MAX_CONCURRENT_SERVES)?)
            .await?;
        let response = call(FRAME_CAP, FRAME_CAP, request_frame(request)).await?;
        assert!(matches!(
            RangeResponse::parse(&response[0])?,
            RangeResponse::Unavailable(_)
        ));
        drop(permit);

        // A range past the artifact end is a protocol violation.
        let past_end = RangeRequest {
            offset: commitment.byte_len,
            ..request
        };
        assert!(matches!(
            call(FRAME_CAP, FRAME_CAP, request_frame(past_end)).await,
            Err(SinkReject::Protocol(_))
        ));

        // A frame cap that fits only the header yields an unavailable response.
        let header_only = u32::try_from(RESPONSE_HEADER_LEN)?;
        let response = call(
            header_only + TRANSPORT_FRAME_OVERHEAD,
            header_only,
            request_frame(request),
        )
        .await?;
        assert_eq!(response[0].payload.len(), RESPONSE_HEADER_LEN);
        assert!(matches!(
            RangeResponse::parse(&response[0])?,
            RangeResponse::Unavailable(_)
        ));
        Ok(())
    }
}
