//! Acquire an artifact from peers, one source at a time.
//!
//! Each source writes to its own resumable partial file. The downloader checks
//! every range, verifies the complete file, and only then publishes it to the cache.
//! A whole-file mismatch discards that source's partial file without blaming the peer.

use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use tokio::{
    io::AsyncWriteExt,
    time::{sleep, timeout},
};
use zakura_chain::parameters::spentness_hints::{Commitment, VerifiedArtifact};

use tokio_util::sync::CancellationToken;

use super::{
    cache::{load, partial_path, publish},
    wire::{RangeRequest, RangeResponse, GET_RANGE, RANGE_BYTES},
    ArtifactService, CAPABILITY, SERVICE_ID, STREAM_KIND,
};
use crate::{
    zakura::{
        spawn_zakura_endpoint_with_services, CustomService, Frame, Peer, Service, Stream,
        ZakuraConnId, ZakuraPeerHandle, ZakuraPeerId, ZakuraServiceId, ZakuraSupervisorHandle,
    },
    BoxError,
};

/// Sources tried per acquisition round.
const MAX_ACQUISITION_PEERS: usize = 3;
/// Minimum pause between failed acquisition rounds.
const RETRY_DELAY: Duration = Duration::from_secs(60);
/// Bound one source's total occupancy, including a slow sequence of ranges.
const SOURCE_TIMEOUT: Duration = Duration::from_secs(10 * 60);
/// Deadline for one range request.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// How long to wait for the first peer that negotiated the capability.
const PEER_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(60);
/// How long the temporary pre-state endpoint may take to shut down.
const BOOTSTRAP_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
/// Bound startup retries so Auto mode can fall back to ordinary sync.
const BOOTSTRAP_ACQUISITION_TIMEOUT: Duration = Duration::from_secs(60 * 60);

/// Continue after the last source, so unavailable peers cannot pin the first cohort.
#[derive(Clone, Default)]
struct SourceCursor(Option<ZakuraPeerId>);

impl SourceCursor {
    fn next(&mut self, mut peers: Vec<ZakuraPeerHandle>) -> Vec<ZakuraPeerHandle> {
        peers.sort_by(|a, b| a.peer_id().as_bytes().cmp(b.peer_id().as_bytes()));
        if peers.is_empty() {
            return peers;
        }
        if let Some(last) = &self.0 {
            let start = peers.partition_point(|peer| peer.peer_id().as_bytes() <= last.as_bytes())
                % peers.len();
            peers.rotate_left(start);
        }
        peers.truncate(MAX_ACQUISITION_PEERS);
        self.0 = peers.last().map(|peer| peer.peer_id().clone());
        peers
    }
}

/// Wait until at least one outbound peer negotiated the capability, or time out empty.
pub(super) async fn wait_for_capable_peers(
    supervisor: &ZakuraSupervisorHandle,
) -> Vec<ZakuraPeerHandle> {
    let mut changes = supervisor.subscribe();
    let discovery = async {
        loop {
            let peers = supervisor
                .outbound_peer_handles_for_capability(CAPABILITY)
                .await;
            if !peers.is_empty() || changes.changed().await.is_err() {
                return peers;
            }
        }
    };
    timeout(PEER_DISCOVERY_TIMEOUT, discovery)
        .await
        .unwrap_or_default()
}

/// Attempt acquisition after bootstrap. Ordinary sync can continue if peers lack the artifact.
pub async fn download_missing(
    cache: PathBuf,
    commitments: &'static [Commitment],
    service: Arc<ArtifactService>,
    supervisor: ZakuraSupervisorHandle,
) {
    let shutdown = supervisor.shutdown_token();
    let acquisition = async {
        let mut cursors = vec![SourceCursor::default(); commitments.len()];
        loop {
            let mut missing = false;
            for (index, commitment) in commitments.iter().enumerate().rev() {
                if service.contains(&commitment.sha256) {
                    continue;
                }
                missing = true;
                let peers = cursors[index].next(wait_for_capable_peers(&supervisor).await);
                match acquire(&cache, commitment, &peers).await {
                    Ok(artifact) => {
                        tracing::info!(
                            digest = %commitment.digest_hex(),
                            bytes = commitment.byte_len,
                            "verified spentness artifact from peers"
                        );
                        service.insert(artifact);
                    }
                    Err(error) => tracing::warn!(
                        %error,
                        digest = %commitment.digest_hex(),
                        "spentness artifact acquisition failed; retrying after delay"
                    ),
                }
            }
            if !missing || commitments.iter().all(|pin| service.contains(&pin.sha256)) {
                break;
            }
            sleep(RETRY_DELAY).await;
        }
    };
    tokio::select! {
        _ = acquisition => {},
        _ = shutdown.cancelled() => {},
    }
}

/// The temporary pre-state endpoint runs no node services; it only seeks artifacts.
#[derive(Debug)]
struct NoNodeServices;

impl Service for NoNodeServices {
    fn name(&self) -> &'static str {
        "spentness-bootstrap"
    }
    fn streams(&self) -> &[Stream] {
        &[]
    }
    fn add_peer(&self, _peer: Peer) {}
    fn remove_peer(&self, _peer: &ZakuraPeerId, _conn: ZakuraConnId) {}
}

/// Acquire a release-selected artifact over a temporary endpoint before writable state opens.
///
/// The endpoint shuts down before this returns, so the node can start its normal endpoint.
pub async fn acquire_before_state(
    config: &crate::Config,
    cache: &Path,
    commitment: &Commitment,
    shutdown: CancellationToken,
) -> Result<Arc<VerifiedArtifact>, BoxError> {
    commitment.validate()?;
    let endpoint = spawn_zakura_endpoint_with_services(
        config,
        |_, _| Arc::new(NoNodeServices),
        None,
        vec![CustomService {
            service: Arc::new(ArtifactService::new([])),
            provides: Vec::new(),
            seeks: vec![ZakuraServiceId::new(SERVICE_ID)?],
        }],
    )
    .await?
    .ok_or("peer spentness acquisition requires the Zakura transport or a local artifact file")?;

    let supervisor = endpoint.supervisor();
    let acquisition = async {
        let mut cursor = SourceCursor::default();
        loop {
            let peers = cursor.next(wait_for_capable_peers(&supervisor).await);
            match acquire(cache, commitment, &peers).await {
                Ok(artifact) => return Ok(artifact),
                Err(error) => {
                    tracing::warn!(%error, "pre-state spentness acquisition failed; retrying after delay")
                }
            }
            sleep(RETRY_DELAY).await;
        }
    };
    let result = tokio::select! {
        result = timeout(BOOTSTRAP_ACQUISITION_TIMEOUT, acquisition) =>
            result.unwrap_or_else(|_| Err("spentness startup acquisition exceeded its one-hour deadline".into())),
        _ = shutdown.cancelled() => Err("spentness acquisition cancelled during shutdown".into()),
    };
    timeout(BOOTSTRAP_SHUTDOWN_TIMEOUT, endpoint.shutdown())
        .await
        .map_err(|_| "spentness bootstrap shutdown timed out")?;
    result
}

/// Return a cached artifact, or acquire it from at most three single sources.
///
/// The caller selects a release-authorized commitment. Peer responses cannot replace it.
pub async fn acquire(
    cache: &Path,
    commitment: &Commitment,
    peers: &[ZakuraPeerHandle],
) -> Result<Arc<VerifiedArtifact>, BoxError> {
    commitment.validate()?;
    let cached = {
        let cache = cache.to_owned();
        let commitment = commitment.clone();
        tokio::task::spawn_blocking(move || load(&cache, &commitment)).await?
    };
    if let Ok(artifact) = cached {
        return Ok(Arc::new(artifact));
    }

    tokio::fs::create_dir_all(cache).await?;
    for peer in peers.iter().take(MAX_ACQUISITION_PEERS) {
        match timeout(SOURCE_TIMEOUT, acquire_from_peer(cache, commitment, peer)).await {
            Ok(Ok(artifact)) => return Ok(Arc::new(artifact)),
            Ok(Err(error)) => tracing::debug!(%error, "spentness source failed"),
            Err(error) => tracing::debug!(%error, "spentness source timed out"),
        }
    }
    Err("no peer supplied the required spentness artifact; \
         retry or provision its exact verified cache entry"
        .into())
}

async fn acquire_from_peer(
    cache: &Path,
    commitment: &Commitment,
    peer: &ZakuraPeerHandle,
) -> Result<VerifiedArtifact, BoxError> {
    let partial = partial_path(cache, commitment, peer.peer_id());
    download_partial(&partial, commitment, peer).await?;

    let artifact = verify_or_discard_partial(&partial, commitment).await?;
    let artifact = {
        let cache = cache.to_owned();
        tokio::task::spawn_blocking(move || -> Result<_, BoxError> {
            publish(&cache, &artifact)?;
            Ok(artifact)
        })
        .await??
    };
    tokio::fs::remove_file(partial).await?;
    Ok(artifact)
}

/// Fill the partial file from `peer`, resuming after any bytes already present.
async fn download_partial(
    partial: &Path,
    commitment: &Commitment,
    peer: &ZakuraPeerHandle,
) -> Result<(), BoxError> {
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(partial)
        .await?;
    let mut offset = file.metadata().await?.len();
    if offset > commitment.byte_len {
        file.set_len(0).await?;
        offset = 0;
    }

    let mut range_limit = RANGE_BYTES;
    while offset < commitment.byte_len {
        let length = (commitment.byte_len - offset).min(u64::from(range_limit));
        let request = RangeRequest {
            digest: commitment.sha256,
            offset,
            length: u32::try_from(length)?,
        };
        let frames = timeout(
            REQUEST_TIMEOUT,
            peer.request(STREAM_KIND, offset, GET_RANGE, 0, request.payload()),
        )
        .await??;
        if let [frame] = frames.as_slice() {
            if let RangeResponse::TooLarge(echoed) = RangeResponse::parse(frame)? {
                check_negative_echo(echoed, request)?;
                if request.length > 1 {
                    range_limit = request.length / 2;
                    continue;
                }
            }
        }
        file.write_all(requested_bytes(&frames, request)?).await?;
        file.sync_data().await?;
        offset = request.checked_end()?;
    }
    Ok(())
}

/// Return the range bytes from a single response that echoes `request` exactly.
fn requested_bytes(frames: &[Frame], request: RangeRequest) -> Result<&[u8], BoxError> {
    let [frame] = frames else {
        return Err("spentness peer returned an invalid response count".into());
    };
    match RangeResponse::parse(frame)? {
        RangeResponse::Available {
            request: echoed,
            bytes,
        } if echoed == request => Ok(bytes),
        RangeResponse::Available { .. } => Err("spentness peer returned a different range".into()),
        RangeResponse::Unavailable(echoed) => {
            check_negative_echo(echoed, request)?;
            Err("spentness artifact is unavailable at this peer".into())
        }
        RangeResponse::Busy(echoed) => {
            check_negative_echo(echoed, request)?;
            Err("spentness peer is busy; retry later".into())
        }
        RangeResponse::TooLarge(echoed) => {
            check_negative_echo(echoed, request)?;
            Err("spentness response cap is too small".into())
        }
        RangeResponse::OutOfRange(echoed) => {
            check_negative_echo(echoed, request)?;
            Err("spentness range is outside the peer's artifact".into())
        }
    }
}

fn check_negative_echo(echoed: RangeRequest, request: RangeRequest) -> Result<(), BoxError> {
    if echoed.digest != request.digest || echoed.offset != request.offset {
        return Err("spentness peer rejected a different range".into());
    }
    Ok(())
}

/// Verify the complete partial file against the commitment.
///
/// A file that fails verification is deleted, so the next attempt starts from zero.
async fn verify_or_discard_partial(
    partial: &Path,
    commitment: &Commitment,
) -> Result<VerifiedArtifact, BoxError> {
    let verified = {
        let partial = partial.to_owned();
        let commitment = commitment.clone();
        tokio::task::spawn_blocking(move || -> Result<_, BoxError> {
            Ok(VerifiedArtifact::read(File::open(partial)?, &commitment)?)
        })
        .await?
    };
    if verified.is_err() {
        tokio::fs::remove_file(partial).await?;
    }
    verified
}

#[cfg(test)]
mod tests {
    use zakura_chain::parameters::spentness_hints::{encode, ParsedArtifact, HEADER_LEN};

    use super::*;
    use crate::zakura::{ZakuraOutboundFrame, ZakuraPeerId};

    /// A peer that answers every range request from `bytes`.
    fn scripted_peer(
        identity: u8,
        bytes: Vec<u8>,
    ) -> (ZakuraPeerHandle, tokio::task::JoinHandle<()>) {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let peer =
            ZakuraPeerHandle::new_for_tests(ZakuraPeerId::new(vec![identity; 32]).unwrap(), sender);
        let task = tokio::spawn(async move {
            while let Some(ZakuraOutboundFrame::Request {
                payload,
                completion,
                ..
            }) = receiver.recv().await
            {
                let request = RangeRequest::parse_payload(&payload).unwrap();
                let start = usize::try_from(request.offset).unwrap();
                let end = start + usize::try_from(request.length).unwrap();
                let response = RangeResponse::available(request, &bytes[start..end]);
                let _ = completion.send(Ok(vec![response]));
            }
        });
        (peer, task)
    }

    #[tokio::test]
    async fn rotating_rounds_reach_peers_beyond_the_first_three() -> Result<(), BoxError> {
        let bytes = encode([1; 32], 1, [2; 32], [false, true, false])?;
        let commitment = ParsedArtifact::read(bytes.as_slice())?.commitment().clone();
        let mut corrupt = bytes.clone();
        corrupt[HEADER_LEN] ^= 4;
        let mut peers = Vec::new();
        let mut tasks = Vec::new();
        for identity in 1..=4 {
            let (peer, task) = scripted_peer(
                identity,
                if identity == 4 {
                    bytes.clone()
                } else {
                    corrupt.clone()
                },
            );
            peers.push(peer);
            tasks.push(task);
        }
        let mut cursor = SourceCursor::default();
        assert!(cursor.next(Vec::new()).is_empty());
        let cache = tempfile::tempdir()?;
        let first = cursor.next(peers.clone());
        assert_eq!(first.len(), MAX_ACQUISITION_PEERS);
        assert!(acquire(cache.path(), &commitment, &first).await.is_err());
        let next = cursor.next(peers);
        let result = acquire(cache.path(), &commitment, &next).await;
        for task in tasks {
            task.abort();
            let _ = task.await;
        }
        assert_eq!(result?.bytes(), bytes);
        Ok(())
    }

    #[tokio::test]
    async fn downloader_reduces_ranges_for_small_response_caps() -> Result<(), BoxError> {
        let bytes = encode([1; 32], 1, [2; 32], [false, true, false])?;
        let commitment = ParsedArtifact::read(bytes.as_slice())?.commitment().clone();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let peer = ZakuraPeerHandle::new_for_tests(ZakuraPeerId::new(vec![1; 32])?, sender);
        let served = bytes.clone();
        let task = tokio::spawn(async move {
            while let Some(ZakuraOutboundFrame::Request {
                payload,
                completion,
                ..
            }) = receiver.recv().await
            {
                let request = RangeRequest::parse_payload(&payload).unwrap();
                let response = if request.length > 8 {
                    RangeResponse::too_large(request)
                } else {
                    let start = usize::try_from(request.offset).unwrap();
                    let end = start + usize::try_from(request.length).unwrap();
                    RangeResponse::available(request, &served[start..end])
                };
                let _ = completion.send(Ok(vec![response]));
            }
        });
        let cache = tempfile::tempdir()?;
        let result = timeout(
            Duration::from_secs(5),
            acquire(cache.path(), &commitment, &[peer]),
        )
        .await;
        task.abort();
        let _ = task.await;
        assert_eq!(result??.bytes(), bytes);
        Ok(())
    }

    #[tokio::test]
    async fn retries_single_sources_after_whole_file_mismatch() -> Result<(), BoxError> {
        let bytes = encode([1; 32], 1, [2; 32], [false, true, false])?;
        let commitment = ParsedArtifact::read(bytes.as_slice())?.commitment().clone();
        let mut corrupt = bytes.clone();
        corrupt[HEADER_LEN] ^= 4;
        let (bad, bad_task) = scripted_peer(1, corrupt);
        let (good, good_task) = scripted_peer(2, bytes.clone());
        let cache = tempfile::tempdir()?;
        let result = timeout(Duration::from_secs(5), async {
            assert!(
                acquire(cache.path(), &commitment, std::slice::from_ref(&bad))
                    .await
                    .is_err()
            );
            assert!(
                load(cache.path(), &commitment).is_err(),
                "unverified data must not enter the cache"
            );
            let verified = acquire(cache.path(), &commitment, &[bad, good]).await?;
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
