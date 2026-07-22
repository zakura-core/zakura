//! Native Zakura fork-aware header sync.

use thiserror::Error;
use zakura_chain::block;

use super::{Frame, ZakuraPeerId, FRAME_HEADER_BYTES};

mod config;
mod error;
mod events;
mod pipe;
mod reactor;
mod scheduler;
mod service;
mod wire;

pub use config::{header_sync_header_bytes_for_network, ZakuraHeaderSyncConfig};
pub use error::HeaderSyncStartError;
pub use events::{
    HeaderSyncAction, HeaderSyncEvent, HeaderSyncFrontiers, HeaderSyncHandle,
    HeaderSyncMisbehavior, HeaderSyncRequestId, HeaderSyncStartup,
};
pub use reactor::spawn_header_sync_reactor;
pub use scheduler::target::{PeerTargetAdvertisement, TargetPursuit};
pub use service::HeaderSyncPeerSession;
pub(crate) use service::{
    drive_header_sync_actions, HeaderSyncPassthroughService, HeaderSyncService,
};
pub use wire::{
    AuxSchema, GetHeaders, HeaderEntry, HeaderSyncCodec, HeaderSyncDecodeContext,
    HeaderSyncMessage, HeaderSyncWireError, Headers, HeadersOutcome, HeadersOutcomeCode,
    ServeCapabilities, Status, TreeAuxRecordV1, DEFAULT_HS_MAX_INFLIGHT, DEFAULT_HS_RANGE,
    MAX_HS_MESSAGE_BYTES, MAX_HS_RANGE, MSG_HS_GET_HEADERS, MSG_HS_HEADERS, MSG_HS_HEADERS_OUTCOME,
    MSG_HS_STATUS, TREE_AUX_SCHEMA_V1_BYTES, ZAKURA_HEADER_SYNC_STREAM_VERSION,
    ZAKURA_STREAM_HEADER_SYNC,
};
