//! Acquire an artifact from peers, one source at a time.
//!
//! Each source writes to its own resumable partial file. The downloader checks
//! every range, verifies the complete file, and only then publishes it to the cache.
//! A whole-file mismatch discards that source's partial file without blaming the peer.
//! A busy source is retried after a short pause; it keeps its place as the source.

use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use rand::Rng;
use tokio::{
    io::AsyncWriteExt,
    time::{sleep, timeout},
};
use zakura_chain::parameters::spentness_hints::{self, Commitment, VerifiedArtifact};

use super::{
    cache::{load, make_room_for_partial, partial_path, publish, remove_partials},
    wire::{RangeRequest, RangeResponse, GET_RANGE, RANGE_BYTES},
    ArtifactService, CAPABILITY, STREAM_KIND,
};
use crate::{
    zakura::{Frame, ZakuraPeerHandle, ZakuraPeerId, ZakuraSupervisorHandle},
    BoxError,
};

/// Sources per acquisition round that may transfer bytes.
///
/// A source that lacks the artifact does not count against this limit.
const MAX_ACQUISITION_PEERS: usize = 3;
/// Minimum pause between failed acquisition rounds.
const RETRY_DELAY: Duration = Duration::from_secs(60);
/// Bound one source's total occupancy, including a slow sequence of ranges and busy pauses.
const SOURCE_TIMEOUT: Duration = Duration::from_secs(10 * 60);
/// Deadline for one range request.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// How long to wait for the first peer that negotiated the capability.
const PEER_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(60);
/// Pause after the first busy reply. Each further busy reply doubles it.
const BUSY_BACKOFF_MIN: Duration = Duration::from_millis(250);
/// Longest pause between busy replies from one source.
const BUSY_BACKOFF_MAX: Duration = Duration::from_secs(2);

/// Continue after the last tried source, so unavailable peers cannot pin the first cohort.
#[derive(Clone, Default)]
struct SourceCursor(Option<ZakuraPeerId>);

impl SourceCursor {
    /// Order `peers` by id, starting after the last tried source.
    fn order(&self, mut peers: Vec<ZakuraPeerHandle>) -> Vec<ZakuraPeerHandle> {
        peers.sort_by(|a, b| a.peer_id().as_bytes().cmp(b.peer_id().as_bytes()));
        if let (Some(last), false) = (&self.0, peers.is_empty()) {
            let start = peers.partition_point(|peer| peer.peer_id().as_bytes() <= last.as_bytes())
                % peers.len();
            peers.rotate_left(start);
        }
        peers
    }
}

/// Wait until at least one peer negotiated the capability, or time out empty.
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
///
/// The first failure for each artifact logs a warning; later failures log at debug level.
pub async fn download_missing(
    cache: PathBuf,
    commitments: &'static [Commitment],
    service: Arc<ArtifactService>,
    supervisor: ZakuraSupervisorHandle,
) {
    let shutdown = supervisor.shutdown_token();
    let acquisition = async {
        let mut cursors = vec![SourceCursor::default(); commitments.len()];
        let mut warned = vec![false; commitments.len()];
        loop {
            let mut missing = false;
            for (index, commitment) in commitments.iter().enumerate().rev() {
                if service.contains(&commitment.sha256) {
                    continue;
                }
                let peers = cursors[index].order(wait_for_capable_peers(&supervisor).await);
                match acquire_round(&cache, commitment, &peers, &mut cursors[index]).await {
                    Ok(artifact) => {
                        tracing::info!(
                            digest = %commitment.digest_hex(),
                            bytes = commitment.byte_len,
                            "verified spentness artifact from peers"
                        );
                        service.insert(artifact);
                    }
                    Err(error) if !warned[index] => {
                        warned[index] = true;
                        missing = true;
                        tracing::warn!(
                            %error,
                            digest = %commitment.digest_hex(),
                            "spentness artifact acquisition failed; retrying after delay"
                        );
                    }
                    Err(error) => {
                        missing = true;
                        tracing::debug!(
                            %error,
                            digest = %commitment.digest_hex(),
                            "spentness artifact acquisition failed again"
                        );
                    }
                }
            }
            if !missing {
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

/// Return a cached artifact, or acquire it from peers in order.
///
/// At most three sources transfer bytes. Sources that lack the artifact are skipped.
/// The caller selects a release-authorized commitment. Peer responses cannot replace it.
pub async fn acquire(
    cache: &Path,
    commitment: &Commitment,
    peers: &[ZakuraPeerHandle],
) -> Result<Arc<VerifiedArtifact>, BoxError> {
    acquire_round(cache, commitment, peers, &mut SourceCursor::default()).await
}

/// Acquire from `peers` in order, and record each tried source in `cursor`.
async fn acquire_round(
    cache: &Path,
    commitment: &Commitment,
    peers: &[ZakuraPeerHandle],
    cursor: &mut SourceCursor,
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
    let mut sources = 0;
    for peer in peers {
        if sources == MAX_ACQUISITION_PEERS {
            break;
        }
        cursor.0 = Some(peer.peer_id().clone());
        match timeout(SOURCE_TIMEOUT, acquire_from_peer(cache, commitment, peer)).await {
            Ok(Ok(artifact)) => return Ok(Arc::new(artifact)),
            Ok(Err(SourceError::Absent)) => {
                tracing::debug!("spentness source lacks the artifact");
                continue;
            }
            Ok(Err(SourceError::Failed(error))) => {
                tracing::debug!(%error, "spentness source failed")
            }
            Err(error) => tracing::debug!(%error, "spentness source timed out"),
        }
        sources += 1;
    }
    Err("no peer supplied the required spentness artifact; \
         retry or provision its exact verified cache entry"
        .into())
}

/// Why one source did not supply the artifact.
#[derive(Debug)]
enum SourceError {
    /// The source replied unavailable before it transferred any bytes.
    Absent,
    /// The source failed after it could have transferred bytes.
    Failed(BoxError),
}

impl<E: Into<BoxError>> From<E> for SourceError {
    fn from(error: E) -> Self {
        Self::Failed(error.into())
    }
}

async fn acquire_from_peer(
    cache: &Path,
    commitment: &Commitment,
    peer: &ZakuraPeerHandle,
) -> Result<VerifiedArtifact, SourceError> {
    let partial = partial_path(cache, commitment, peer.peer_id());
    {
        let (cache, commitment, partial) = (cache.to_owned(), commitment.clone(), partial.clone());
        tokio::task::spawn_blocking(move || make_room_for_partial(&cache, &commitment, &partial))
            .await??;
    }
    download_partial(&partial, commitment, peer).await?;

    let artifact = verify_or_discard_partial(&partial, commitment).await?;
    let (cache, commitment) = (cache.to_owned(), commitment.clone());
    let artifact = tokio::task::spawn_blocking(move || -> Result<_, BoxError> {
        publish(&cache, &artifact)?;
        remove_partials(&cache, &commitment)?;
        Ok(artifact)
    })
    .await??;
    Ok(artifact)
}

/// Fill the partial file from `peer`, resuming after any bytes already present.
async fn download_partial(
    partial: &Path,
    commitment: &Commitment,
    peer: &ZakuraPeerHandle,
) -> Result<(), SourceError> {
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

    let mut transferred = false;
    let mut range_limit = RANGE_BYTES;
    let mut busy_backoff = BUSY_BACKOFF_MIN;
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
        match reply(&frames, request)? {
            Reply::Bytes(bytes) => {
                file.write_all(bytes).await?;
                file.sync_data().await?;
                offset = request.checked_end()?;
                transferred = true;
                busy_backoff = BUSY_BACKOFF_MIN;
            }
            Reply::Busy => {
                // Pause for a random time in the upper half of the backoff.
                let pause = rand::thread_rng().gen_range(busy_backoff / 2..=busy_backoff);
                sleep(pause).await;
                busy_backoff = (busy_backoff * 2).min(BUSY_BACKOFF_MAX);
            }
            Reply::TooLarge if request.length > 1 => range_limit = request.length / 2,
            Reply::TooLarge => return Err("spentness response cap is too small".into()),
            Reply::Unavailable if !transferred => return Err(SourceError::Absent),
            Reply::Unavailable => {
                return Err("spentness artifact became unavailable at this peer".into())
            }
            Reply::OutOfRange => {
                return Err("spentness range is outside the peer's artifact".into())
            }
        }
    }
    Ok(())
}

/// A checked reply to one range request.
#[derive(Debug)]
enum Reply<'a> {
    Bytes(&'a [u8]),
    Unavailable,
    Busy,
    TooLarge,
    OutOfRange,
}

/// Parse the single response and check that it echoes `request`.
fn reply(frames: &[Frame], request: RangeRequest) -> Result<Reply<'_>, BoxError> {
    let [frame] = frames else {
        return Err("spentness peer returned an invalid response count".into());
    };
    let (echoed, reply) = match RangeResponse::parse(frame)? {
        RangeResponse::Available {
            request: echoed,
            bytes,
        } => {
            if echoed != request {
                return Err("spentness peer returned a different range".into());
            }
            return Ok(Reply::Bytes(bytes));
        }
        RangeResponse::Unavailable(echoed) => (echoed, Reply::Unavailable),
        RangeResponse::Busy(echoed) => (echoed, Reply::Busy),
        RangeResponse::TooLarge(echoed) => (echoed, Reply::TooLarge),
        RangeResponse::OutOfRange(echoed) => (echoed, Reply::OutOfRange),
    };
    if echoed.digest != request.digest || echoed.offset != request.offset {
        return Err("spentness peer rejected a different range".into());
    }
    Ok(reply)
}

/// Verify the complete partial file against the commitment.
///
/// A file whose content fails verification is deleted, so the next attempt
/// starts from zero. A local read failure keeps the file.
async fn verify_or_discard_partial(
    partial: &Path,
    commitment: &Commitment,
) -> Result<VerifiedArtifact, BoxError> {
    let verified = {
        let partial = partial.to_owned();
        let commitment = commitment.clone();
        tokio::task::spawn_blocking(move || {
            std::fs::File::open(partial).map(|file| VerifiedArtifact::read(file, &commitment))
        })
        .await??
    };
    let error = match verified {
        Ok(artifact) => return Ok(artifact),
        Err(error) => error,
    };
    let corrupt = match &error {
        spentness_hints::Error::Io(error) => error.kind() == io::ErrorKind::UnexpectedEof,
        spentness_hints::Error::Format(_) | spentness_hints::Error::CommitmentMismatch => true,
        spentness_hints::Error::Ordinal => false,
    };
    if corrupt {
        tokio::fs::remove_file(partial).await?;
    }
    Err(error.into())
}

#[cfg(test)]
mod tests {
    use zakura_chain::parameters::spentness_hints::{encode, ParsedArtifact, HEADER_LEN};

    use super::*;
    use crate::zakura::{ZakuraOutboundFrame, ZakuraPeerId};

    /// A peer that answers each range request with `respond`.
    fn scripted_peer(
        identity: u8,
        mut respond: impl FnMut(RangeRequest) -> Frame + Send + 'static,
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
                let _ = completion.send(Ok(vec![respond(request)]));
            }
        });
        (peer, task)
    }

    /// A peer that answers every range request from `bytes`.
    fn serving_peer(
        identity: u8,
        bytes: Vec<u8>,
    ) -> (ZakuraPeerHandle, tokio::task::JoinHandle<()>) {
        scripted_peer(identity, move |request| {
            let start = usize::try_from(request.offset).unwrap();
            let end = start + usize::try_from(request.length).unwrap();
            RangeResponse::available(request, &bytes[start..end])
        })
    }

    async fn stop(tasks: impl IntoIterator<Item = tokio::task::JoinHandle<()>>) {
        for task in tasks {
            task.abort();
            let _ = task.await;
        }
    }

    fn artifact() -> Result<(Vec<u8>, Commitment), BoxError> {
        let bytes = encode([1; 32], 1, [2; 32], [false, true, false])?;
        let commitment = ParsedArtifact::read(bytes.as_slice())?.commitment().clone();
        Ok((bytes, commitment))
    }

    #[tokio::test]
    async fn rotating_rounds_reach_peers_beyond_the_first_three() -> Result<(), BoxError> {
        let (bytes, commitment) = artifact()?;
        let mut corrupt = bytes.clone();
        corrupt[HEADER_LEN] ^= 4;
        let (peers, tasks): (Vec<_>, Vec<_>) = (1..=4)
            .map(|identity| {
                serving_peer(
                    identity,
                    if identity == 4 {
                        bytes.clone()
                    } else {
                        corrupt.clone()
                    },
                )
            })
            .unzip();
        let mut cursor = SourceCursor::default();
        assert!(cursor.order(Vec::new()).is_empty());
        let cache = tempfile::tempdir()?;
        let first = cursor.order(peers.clone());
        let failed = acquire_round(cache.path(), &commitment, &first, &mut cursor).await;
        assert!(failed.is_err());
        assert_eq!(
            cursor.0.as_ref(),
            Some(first[MAX_ACQUISITION_PEERS - 1].peer_id())
        );
        let next = cursor.order(peers);
        let result = acquire_round(cache.path(), &commitment, &next, &mut cursor).await;
        stop(tasks).await;
        assert_eq!(result?.bytes(), bytes);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn busy_sources_are_retried_after_a_bounded_pause() -> Result<(), BoxError> {
        let (bytes, commitment) = artifact()?;
        let (peer, task) = scripted_peer(1, {
            let mut busy_replies = 6;
            move |request| {
                if busy_replies > 0 {
                    busy_replies -= 1;
                    return RangeResponse::busy(request);
                }
                let start = usize::try_from(request.offset).unwrap();
                let end = start + usize::try_from(request.length).unwrap();
                RangeResponse::available(request, &bytes[start..end])
            }
        });
        let cache = tempfile::tempdir()?;
        let started = tokio::time::Instant::now();
        let result = acquire(cache.path(), &commitment, &[peer]).await;
        stop([task]).await;
        assert!(result.is_ok(), "busy replies must not drop the source");
        assert!(started.elapsed() <= 6 * BUSY_BACKOFF_MAX);
        Ok(())
    }

    #[tokio::test]
    async fn sources_without_the_artifact_do_not_count() -> Result<(), BoxError> {
        let (bytes, commitment) = artifact()?;
        let (mut peers, mut tasks): (Vec<_>, Vec<_>) = (1..=5)
            .map(|identity| scripted_peer(identity, RangeResponse::unavailable))
            .unzip();
        let (peer, task) = serving_peer(6, bytes.clone());
        peers.push(peer);
        tasks.push(task);
        let cache = tempfile::tempdir()?;
        let result = acquire(cache.path(), &commitment, &peers).await;
        stop(tasks).await;
        assert_eq!(result?.bytes(), bytes);
        Ok(())
    }

    #[tokio::test]
    async fn success_removes_every_partial_for_the_digest() -> Result<(), BoxError> {
        let (bytes, commitment) = artifact()?;
        let cache = tempfile::tempdir()?;
        for identity in [7, 8] {
            let peer = ZakuraPeerId::new(vec![identity; 32])?;
            std::fs::write(partial_path(cache.path(), &commitment, &peer), [1])?;
        }
        let (peer, task) = serving_peer(1, bytes.clone());
        let result = acquire(cache.path(), &commitment, &[peer]).await;
        stop([task]).await;
        assert_eq!(result?.bytes(), bytes);
        let names: Vec<_> = std::fs::read_dir(cache.path())?
            .map(|entry| entry.map(|entry| entry.file_name()))
            .collect::<Result<_, _>>()?;
        assert_eq!(names, [std::ffi::OsString::from(commitment.file_name())]);
        Ok(())
    }

    #[tokio::test]
    async fn downloader_reduces_ranges_for_small_response_caps() -> Result<(), BoxError> {
        let (bytes, commitment) = artifact()?;
        let served = bytes.clone();
        let (peer, task) = scripted_peer(1, move |request| {
            if request.length > 8 {
                return RangeResponse::too_large(request);
            }
            let start = usize::try_from(request.offset).unwrap();
            let end = start + usize::try_from(request.length).unwrap();
            RangeResponse::available(request, &served[start..end])
        });
        let cache = tempfile::tempdir()?;
        let result = timeout(
            Duration::from_secs(5),
            acquire(cache.path(), &commitment, &[peer]),
        )
        .await;
        stop([task]).await;
        assert_eq!(result??.bytes(), bytes);
        Ok(())
    }

    #[tokio::test]
    async fn retries_single_sources_after_whole_file_mismatch() -> Result<(), BoxError> {
        let (bytes, commitment) = artifact()?;
        let mut corrupt = bytes.clone();
        corrupt[HEADER_LEN] ^= 4;
        let (bad, bad_task) = serving_peer(1, corrupt);
        let (good, good_task) = serving_peer(2, bytes.clone());
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
            assert_eq!(
                std::fs::read_dir(cache.path())?.count(),
                0,
                "a whole-file mismatch discards the source's partial file"
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
        stop([bad_task, good_task]).await;
        result??;
        Ok(())
    }
}
