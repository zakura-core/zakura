//! Bounded peer transfer of whole-file authenticated spentness artifacts.
//!
//! - `wire`: range request and response encoding.
//! - `server`: serving verified artifacts under a shared rate limit.
//! - `cache`: content-addressed files that are reverified on every load.
//! - `download`: resumable single-source acquisition with whole-file verification.
//!
//! The capability advertises protocol support. Discovery advertises availability
//! only when startup loaded at least one verified artifact.

use std::{path::PathBuf, sync::Arc};

use zakura_chain::parameters::spentness_hints::Commitment;

use super::{CustomService, Frame, Stream, StreamMode, ZakuraServiceId};
use crate::BoxError;

mod cache;
mod download;
mod server;
mod wire;

pub use cache::{artifact_path, load, publish};
pub use download::{acquire, download_missing};
pub use server::ArtifactService;
pub use wire::{GET_RANGE, RANGE, RANGE_BYTES};

/// Spentness artifact request/response stream.
pub const STREAM_KIND: u16 = 8;
/// Negotiated support for the artifact protocol, independent of artifact availability.
pub const CAPABILITY: u64 = 1 << 6;
const SERVICE_ID: &str = "zakura.spentness.v1";
const PROTOCOL_VERSION: u16 = 1;
/// Frame limit: one full range plus room for the response header.
const FRAME_CAP: u32 = RANGE_BYTES + 64;
const STREAMS: &[Stream] = &[Stream {
    kind: STREAM_KIND,
    version: PROTOCOL_VERSION,
    frame_cap: FRAME_CAP,
    capability: CAPABILITY,
    mode: StreamMode::RequestResponse,
}];

/// Load supported cache entries and prepare protocol negotiation and discovery.
///
/// The node seeks the service when it recognizes any commitment, and provides
/// it only when the cache already holds a verified artifact.
pub async fn prepare(
    cache: PathBuf,
    commitments: &'static [Commitment],
) -> Result<(Arc<ArtifactService>, CustomService), BoxError> {
    let artifacts =
        tokio::task::spawn_blocking(move || cache::load_supported(&cache, commitments)).await?;
    let service = Arc::new(ArtifactService::new(artifacts));
    let id = ZakuraServiceId::new(SERVICE_ID)?;
    let custom = CustomService {
        service: service.clone(),
        provides: (!service.is_empty())
            .then(|| id.clone())
            .into_iter()
            .collect(),
        seeks: (!commitments.is_empty())
            .then_some(id)
            .into_iter()
            .collect(),
    };
    Ok((service, custom))
}

/// Validate the single bounded response before the transport stores it.
pub(crate) fn validate_response(frame: &Frame) -> Result<(), BoxError> {
    wire::RangeResponse::parse(frame).map(|_| ())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::timeout;
    use zakura_chain::parameters::spentness_hints::{encode, ParsedArtifact};

    use super::{
        cache::partial_path,
        download::wait_for_capable_peers,
        wire::{RangeRequest, RangeResponse, DIGEST_LEN},
        *,
    };
    use crate::zakura::{
        spawn_zakura_endpoint_with_services, Peer, Service, ZakuraConnId, ZakuraPeerId,
    };

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
    async fn seed_only_transfer_resumes_and_serves_verified_bytes() -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        let bytes = encode(
            [1; 32],
            10,
            [2; 32],
            (0..(u64::from(RANGE_BYTES) * 8 + 9)).map(|n| n != 0 && n % 2 == 0),
        )?;
        let parsed = ParsedArtifact::read(bytes.as_slice())?;
        let commitment = parsed.commitment().clone();
        let server_service = Arc::new(ArtifactService::new([Arc::new(
            parsed.verify(&commitment)?,
        )]));
        let server_identity = tempfile::tempdir()?;
        let client_identity = tempfile::tempdir()?;
        let cache = tempfile::tempdir()?;
        let service_id = ZakuraServiceId::new(SERVICE_ID)?;

        let mut server_config = crate::Config::for_test(crate::P2pStack::Dual);
        server_config.identity_dir = server_identity.path().to_owned();
        server_config.zakura.listen_addr = Some("127.0.0.1:0".parse()?);
        server_config.zakura.bootstrap_peers.clear();
        let server = spawn_zakura_endpoint_with_services(
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
            .ip_addrs()
            .copied()
            .find(|addr| addr.ip().is_loopback())
            .ok_or("server has no loopback address")?;

        let mut client_config = server_config;
        client_config.identity_dir = client_identity.path().to_owned();
        client_config.zakura.bootstrap_peers = vec![format!("{}@{direct}", server_addr.id)];
        let client_service = Arc::new(ArtifactService::new([]));
        let client = spawn_zakura_endpoint_with_services(
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
            let peer = wait_for_capable_peers(&client.supervisor())
                .await
                .into_iter()
                .next()
                .ok_or("no capable peer connected")?;

            // Resume from a partial file left by an earlier attempt against the same peer.
            let partial = partial_path(cache.path(), &commitment, peer.peer_id());
            tokio::fs::write(&partial, &bytes[..128]).await?;
            let artifact = acquire(cache.path(), &commitment, std::slice::from_ref(&peer)).await?;
            assert_eq!(artifact.bytes(), bytes);
            assert!(!partial.exists());
            assert_eq!(load(cache.path(), &commitment)?.bytes(), bytes);
            client_service.insert(artifact);
            assert_eq!(client_service.available(), vec![commitment.sha256]);

            let unknown = RangeRequest {
                digest: [3; DIGEST_LEN],
                offset: 0,
                length: 1,
            };
            let response = peer
                .request(STREAM_KIND, 0, GET_RANGE, 0, unknown.payload())
                .await?;
            assert!(
                matches!(
                    RangeResponse::parse(&response[0])?,
                    RangeResponse::Unavailable(_)
                ),
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
}
