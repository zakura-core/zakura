//! Per-connection compact block relay state, from the draft Compact Block Relay ZIP.
//!
//! Only connections that negotiated compact blocks (`early_block_relay`) use this state.
//!
//! A connection announces a block with `cmpctblock` when the peer asked for version 1
//! announcements, and then holds the block for [`RETENTION`] to serve the peer's
//! `getblocktxn`.
//!
//! A connection that receives a `cmpctblock` screens its header, holds it, and starts the
//! block's download from this peer. The download rebuilds the block from mempool
//! transactions, asks the peer for the missing ones with `getblocktxn`, and falls back to a
//! full `getdata` on any failure. A failure never scores the peer.
//!
//! A connection holds at most one sent, one received, and one reconstructed block, each for
//! [`RETENTION`]. Each held block takes a permit from the global [`SLOTS`].

use std::{
    collections::HashSet,
    sync::{Arc, LazyLock},
    time::Duration,
};

use futures::Sink;
use indexmap::IndexSet;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::{sleep, sleep_until, timeout, timeout_at, Instant},
};
use tower::{Service, ServiceExt};

use zakura_chain::{
    block::{self, MAX_BLOCK_BYTES},
    chain_tip::ChainTip,
    serialization::{CompactSizeMessage, SerializationError, ZcashSerialize},
};
use zakura_node_services::block_memory::BlockHandle;

use super::{Connection, Handler, State};
use crate::{
    constants::REQUEST_TIMEOUT,
    protocol::{
        external::{
            compact::{BlockTransactions, BlockTransactionsRequest, CompactBlock, Reconstruction},
            Message,
        },
        internal::InventoryResponse,
    },
    BoxError, PeerError, Request, Response,
};

/// The compact block encoding version that the draft Compact Block Relay ZIP defines.
const COMPACT_VERSION: u64 = 1;

/// Bounds the decoded block bodies that compact block relay holds across all connections.
///
/// A decoded body can retain much more memory than its 2 MB wire encoding. Six slots cover
/// three sent and three received bodies.
static SLOTS: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(6)));

/// How long a connection holds a sent, received, or reconstructed block.
///
/// A sent block serves the peer's `getblocktxn`. A received `cmpctblock` waits for the
/// downloader's request. A reconstructed block answers a second downloader of the same block.
/// A connection also screens at most one received `cmpctblock` per [`RETENTION`].
pub(super) const RETENTION: Duration = Duration::from_secs(15);

/// How long a connection waits for the peer's `blocktxn` before it downloads the full block.
pub(super) const RECOVERY_TIMEOUT: Duration = Duration::from_secs(2);

/// The `blocktxn` bytes a connection sends for one `cmpctblock` announcement.
// 2,000,000 fits in the usize of every supported target.
const RECOVERY_BYTES: usize = MAX_BLOCK_BYTES as usize;

/// A block this connection announced with `cmpctblock`, held to serve the peer's `getblocktxn`.
#[derive(Debug)]
struct SentBlock {
    block: BlockHandle,
    hash: block::Hash,
    /// The serialized size of each transaction of `block`, in block order.
    ///
    /// The connection measures them once, so a repeated or rejected `getblocktxn` costs no
    /// serialization.
    transaction_sizes: Vec<usize>,
    /// The connection serves `getblocktxn` for this block until this time.
    until: Instant,
    /// The global [`SLOTS`] permit this block holds.
    _slot: OwnedSemaphorePermit,
}

/// A `cmpctblock` this connection received and screened, held until the downloader requests
/// its block.
#[derive(Debug)]
struct ReceivedBlock {
    compact: Arc<CompactBlock>,
    hash: block::Hash,
    /// The connection drops this announcement at this time.
    until: Instant,
    /// The global [`SLOTS`] permit this announcement holds. Its reconstruction keeps it.
    slot: OwnedSemaphorePermit,
}

/// A block this connection rebuilt from a received `cmpctblock` and checked, held to answer
/// another request for it.
#[derive(Debug)]
struct ReconstructedBlock {
    block: BlockHandle,
    hash: block::Hash,
    /// The connection drops this block at this time.
    until: Instant,
    /// The global [`SLOTS`] permit this block holds.
    _slot: OwnedSemaphorePermit,
}

/// A `cmpctblock` built for a block advert, and what the connection holds once it writes it.
#[derive(Debug)]
pub(super) struct CompactAdvert {
    /// The `cmpctblock` message to write.
    pub(super) message: Message,
    block: BlockHandle,
    hash: block::Hash,
    transaction_sizes: Vec<usize>,
    slot: OwnedSemaphorePermit,
}

/// The compact block relay state of one connection.
#[derive(Debug)]
pub(super) struct CompactRelay {
    /// The peer's most recent `sendcmpct` asked for version 1 `cmpctblock` announcements.
    pub(super) send: bool,

    /// This connection's high-bandwidth receive slot.
    ///
    /// The connection holds it from its `sendcmpct(1, 1)` write until its `sendcmpct(0, 1)`
    /// write completes, or the connection fails.
    pub(super) receive_permit: Option<OwnedSemaphorePermit>,

    /// The slots that bound decoded bodies across connections, by default the global
    /// [`SLOTS`].
    pub(super) slots: Arc<Semaphore>,

    /// The block this connection last announced with `cmpctblock`, until [`RETENTION`] ends.
    sent: Option<SentBlock>,

    /// The `blocktxn` bytes left for the peer's `getblocktxn` requests for the sent block.
    ///
    /// Every request charges its overhead, so repeated or empty requests use up the budget.
    recovery_bytes_left: usize,

    /// The `cmpctblock` this connection last received and screened, until the downloader
    /// requests its block or [`RETENTION`] ends.
    received: Option<ReceivedBlock>,

    /// The block this connection last rebuilt and checked, until [`RETENTION`] ends.
    reconstructed: Option<ReconstructedBlock>,

    /// When this connection last screened a received `cmpctblock`.
    last_received: Option<Instant>,
}

impl Default for CompactRelay {
    fn default() -> Self {
        Self {
            send: false,
            receive_permit: None,
            slots: SLOTS.clone(),
            sent: None,
            recovery_bytes_left: 0,
            received: None,
            reconstructed: None,
            last_received: None,
        }
    }
}

impl CompactRelay {
    /// Records the peer's `sendcmpct` message. Any version other than 1 turns announcements
    /// off. Neither case is misbehavior.
    pub(super) fn received_send_compact(&mut self, announce: bool, version: u64) {
        self.send = announce && version == COMPACT_VERSION;
    }

    /// Holds the block of a written `cmpctblock` for [`RETENTION`], and gives the peer a
    /// full block's worth of `blocktxn` bytes.
    pub(super) fn mark_sent(&mut self, advert: CompactAdvert) {
        let CompactAdvert {
            message: _,
            block,
            hash,
            transaction_sizes,
            slot,
        } = advert;
        self.sent = Some(SentBlock {
            block,
            hash,
            transaction_sizes,
            until: Instant::now() + RETENTION,
            _slot: slot,
        });
        self.recovery_bytes_left = RECOVERY_BYTES;
    }

    /// Releases the sent block and its slot.
    pub(super) fn clear_sent(&mut self) {
        self.sent = None;
    }

    /// Releases every held block and its slot.
    pub(super) fn clear_held(&mut self) {
        self.sent = None;
        self.received = None;
        self.reconstructed = None;
    }

    /// Releases each held block and its slot once its [`RETENTION`] has ended.
    pub(super) fn expire(&mut self) {
        let now = Instant::now();
        if self.sent.as_ref().is_some_and(|sent| sent.until <= now) {
            self.sent = None;
        }
        if self
            .received
            .as_ref()
            .is_some_and(|received| received.until <= now)
        {
            self.received = None;
        }
        if self
            .reconstructed
            .as_ref()
            .is_some_and(|reconstructed| reconstructed.until <= now)
        {
            self.reconstructed = None;
        }
    }

    /// Returns when the first held block expires, if this connection holds any.
    pub(super) fn expiry(&self) -> Option<Instant> {
        [
            self.sent.as_ref().map(|sent| sent.until),
            self.received.as_ref().map(|received| received.until),
            self.reconstructed
                .as_ref()
                .map(|reconstructed| reconstructed.until),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    /// Returns whether this connection screens a received `cmpctblock`: it holds no received
    /// block, and it screened none in the last [`RETENTION`].
    ///
    /// Call [`Self::expire`] first.
    fn may_receive(&self) -> bool {
        self.received.is_none()
            && self
                .last_received
                .is_none_or(|last| last.elapsed() >= RETENTION)
    }

    /// Returns the block that a request for `hashes` downloads through compact block relay:
    /// the one hash, if it names the received or reconstructed block.
    ///
    /// The match can name an expired block. The download expires held blocks first, and then
    /// requests an expired block with `getdata`.
    pub(super) fn held_download(&self, hashes: &IndexSet<block::Hash>) -> Option<block::Hash> {
        if hashes.len() != 1 {
            return None;
        }
        let hash = hashes.first()?;
        let received = self.received.as_ref().map(|received| received.hash);
        let reconstructed = self
            .reconstructed
            .as_ref()
            .map(|reconstructed| reconstructed.hash);
        (received == Some(*hash) || reconstructed == Some(*hash)).then_some(*hash)
    }
}

/// Returns the request timeout for `handler`: [`RECOVERY_TIMEOUT`] while it waits for
/// `blocktxn`, and [`REQUEST_TIMEOUT`] otherwise.
pub(super) fn response_timeout(handler: &Handler) -> Duration {
    if matches!(handler, Handler::CompactBlock { .. }) {
        RECOVERY_TIMEOUT
    } else {
        REQUEST_TIMEOUT
    }
}

/// Returns the handler that waits for the full block `hash` after a `getdata`.
fn full_block_handler(hash: block::Hash) -> Handler {
    Handler::BlocksByHash {
        pending_hashes: HashSet::from([hash]),
        blocks: Vec::with_capacity(1),
    }
}

/// Waits until `expiry`, or forever if it is `None`.
pub(super) async fn sleep_until_expiry(expiry: Option<Instant>) {
    match expiry {
        Some(expiry) => sleep_until(expiry).await,
        None => futures::future::pending().await,
    }
}

/// Returns the serialized size of each transaction of `block`, or `None` if the
/// `cmpctblock` that `compact` encodes is not smaller than `block`.
///
/// Serializes the whole block, so call it on a blocking thread.
fn smaller_compact_sizes(block: &BlockHandle, compact: &CompactBlock) -> Option<Vec<usize>> {
    (compact.zcash_serialized_size() < block.zcash_serialized_size()).then(|| {
        block
            .view()
            .transactions()
            .map(|transaction| transaction.zcash_serialized_size())
            .collect()
    })
}

impl<S, Tx, C> Connection<S, Tx, C>
where
    S: Service<Request, Response = Response, Error = BoxError>,
    C: ChainTip,
    Tx: Sink<Message, Error = SerializationError> + Unpin,
{
    /// Takes a free high-bandwidth slot, if the peer set selected this connection and a slot
    /// is free.
    pub(super) fn try_take_receive_slot(&mut self) -> Option<OwnedSemaphorePermit> {
        self.compact_receive_rx
            .borrow_and_update()
            .as_ref()
            .and_then(|slots| Arc::clone(slots).try_acquire_owned().ok())
    }

    /// Writes `sendcmpct(announce = permit.is_some(), 1)`, then keeps `permit` as this
    /// connection's receive slot. A demotion therefore releases the old slot only after its
    /// write completes.
    ///
    /// A failed write releases `permit` and fails the connection.
    pub(super) async fn set_compact_receive(&mut self, permit: Option<OwnedSemaphorePermit>) {
        let result = self
            .peer_tx
            .send(Message::SendCompact {
                announce: permit.is_some(),
                version: COMPACT_VERSION,
            })
            .await;
        match result {
            Ok(()) => self.compact.receive_permit = permit,
            Err(error) => {
                drop(permit);
                self.fail_with(error).await;
            }
        }
    }

    /// Returns a `cmpctblock` that announces the block `hash`, or `None` if this connection
    /// announces it with `inv`.
    ///
    /// Returns `None` unless:
    /// - the connection negotiated compact blocks, and the peer asked for `cmpctblock`;
    /// - the connection holds no sent block, so it can still serve the last one;
    /// - a global slot is free;
    /// - the inbound service returns the block for an early-relay request; and
    /// - the `cmpctblock` is smaller than the block.
    ///
    /// The returned advert holds the slot. Dropping it releases the slot.
    pub(super) async fn compact_advert(&mut self, hash: block::Hash) -> Option<CompactAdvert> {
        if !self.connection_info.early_block_relay || !self.compact.send {
            return None;
        }
        self.compact.expire();
        if self.compact.sent.is_some() {
            return None;
        }
        let slot = Arc::clone(&self.compact.slots).try_acquire_owned().ok()?;
        let block = self.early_block(hash).await?;
        let nonce = rand_10::random();
        let (block, built) = tokio::task::spawn_blocking(move || {
            let built = CompactBlock::from_block(&block, nonce)
                .ok()
                .and_then(|compact| {
                    smaller_compact_sizes(&block, &compact).map(|sizes| (compact, sizes))
                });
            (block, built)
        })
        .await
        .ok()?;
        let (compact, transaction_sizes) = built?;
        Some(CompactAdvert {
            message: Message::CompactBlock(Arc::new(compact)),
            block,
            hash,
            transaction_sizes,
            slot,
        })
    }

    /// Returns the block `hash` if the inbound service answers an early-relay request for it
    /// within [`REQUEST_TIMEOUT`].
    ///
    /// Any error, timeout, or missing block returns `None`, because the advert can still go
    /// as an `inv`.
    async fn early_block(&mut self, hash: block::Hash) -> Option<BlockHandle> {
        self.compact_query(Request::EarlyBlocksByHash(IndexSet::from([hash])))
            .await?
            .block_statuses()?
            .iter()
            .find_map(|status| match status {
                InventoryResponse::Available((block, _)) if block.hash() == hash => {
                    Some(block.clone())
                }
                _ => None,
            })
    }

    /// Answers the peer's `getblocktxn` with the requested transactions of the sent block.
    ///
    /// Ignores the request, without a penalty, if:
    /// - the connection holds no sent block, or its [`RETENTION`] has ended;
    /// - the request is for another block;
    /// - an index is out of range, or the indexes do not strictly increase; or
    /// - the response would exceed the bytes left for this announcement.
    ///
    /// The write must finish before the sent block expires. A failed or late write fails the
    /// connection.
    pub(super) async fn serve_compact_transactions(&mut self, request: &BlockTransactionsRequest) {
        if !self.connection_info.early_block_relay {
            return;
        }
        self.compact.expire();
        let Some(sent) = &self.compact.sent else {
            debug!("ignoring getblocktxn: no sent compact block is held");
            return;
        };
        if sent.hash != request.block_hash {
            debug!("ignoring getblocktxn for a block this connection did not just announce");
            return;
        }
        if request
            .indexes
            .last()
            .is_some_and(|index| *index >= sent.transaction_sizes.len())
            || request.indexes.windows(2).any(|pair| pair[0] >= pair[1])
        {
            debug!("ignoring getblocktxn with out-of-range or unordered indexes");
            return;
        }

        let overhead = request.block_hash.zcash_serialized_size().saturating_add(
            CompactSizeMessage::try_from(request.indexes.len())
                .expect("the indexes are fewer than the sent block's transactions")
                .zcash_serialized_size(),
        );
        let Some(remaining) = self
            .compact
            .recovery_bytes_left
            .checked_sub(overhead)
            .and_then(|remaining| {
                request
                    .indexes
                    .iter()
                    .try_fold(remaining, |remaining, index| {
                        remaining.checked_sub(sent.transaction_sizes[*index])
                    })
            })
        else {
            debug!("ignoring getblocktxn that exceeds the announcement's byte budget");
            return;
        };
        self.compact.recovery_bytes_left = remaining;

        let transactions = request
            .indexes
            .iter()
            .map(|index| {
                let transaction = sent
                    .block
                    .view()
                    .transaction_at(*index)
                    .expect("the index is less than the sent block's transaction count");
                Arc::new(transaction.clone())
            })
            .collect();
        let response = Message::BlockTransactions(BlockTransactions {
            block_hash: request.block_hash,
            transactions,
        });
        let until = sent.until;

        self.block_uploads.requested(request.block_hash);
        // Repeated requests share the byte budget and the sent block's deadline.
        let result = timeout_at(until, self.peer_tx.send(response))
            .await
            .unwrap_or(Err(PeerError::ConnectionSendTimeout));
        match result {
            Ok(()) => self.block_uploads.written(request.block_hash),
            Err(error) => {
                self.compact.clear_sent();
                self.fail_with(error).await;
            }
        }
    }

    /// Sends a compact block relay `request` to the inbound service, and returns its response
    /// if it answers within [`REQUEST_TIMEOUT`].
    ///
    /// Returns `None` on an error or timeout. Neither one scores the peer, because compact
    /// block relay falls back to an `inv` or a full block download.
    async fn compact_query(&mut self, request: Request) -> Option<Response> {
        timeout(REQUEST_TIMEOUT, async {
            self.svc.ready().await.ok()?.call(request).await.ok()
        })
        .await
        .ok()
        .flatten()
    }

    /// Handles the peer's `cmpctblock`, then starts the block's download with an inbound
    /// `AdvertiseBlock`, as for an `inv`.
    ///
    /// The connection holds the `cmpctblock` for the download if:
    /// - it holds no received block, and screened none in the last [`RETENTION`];
    /// - the inbound service passes the header screen; and
    /// - a global slot is free.
    ///
    /// Every screen starts the cooldown, whatever its result. A screening failure is not
    /// misbehavior. A connection that did not negotiate compact blocks ignores the message.
    pub(super) async fn receive_compact(&mut self, compact: Arc<CompactBlock>) {
        if !self.connection_info.early_block_relay {
            debug!("ignoring cmpctblock: compact blocks were not negotiated");
            return;
        }
        self.compact.expire();
        let hash = compact.block_hash();
        if self.compact.may_receive() {
            self.compact.last_received = Some(Instant::now());
            let screened = matches!(
                self.compact_query(Request::CheckCompactHeader(compact.clone()))
                    .await,
                Some(Response::Nil)
            );
            if !screened {
                debug!("not holding cmpctblock: its header failed screening");
            } else if let Ok(slot) = Arc::clone(&self.compact.slots).try_acquire_owned() {
                self.compact.received = Some(ReceivedBlock {
                    compact,
                    hash,
                    until: Instant::now() + RETENTION,
                    slot,
                });
            } else {
                debug!("not holding cmpctblock: no decoded body slot is free");
            }
        } else {
            debug!("not screening cmpctblock: this peer sent one within the retention time");
        }

        let source = self
            .connection_info
            .connected_addr
            .get_transient_addr()
            .map(Into::into);
        self.drive_peer_request(Request::AdvertiseBlock(hash.into(), source))
            .await;
    }

    /// Returns the handler for a request for the block `hash`, which
    /// [`CompactRelay::held_download`] matched.
    ///
    /// Answers at once from a reconstructed block. Rebuilds a received block from mempool
    /// transactions, and writes `getblocktxn` if some are missing. Writes a full `getdata` in
    /// any other case, or if reconstruction fails.
    ///
    /// Fails only if a write fails, or if the peer lost its authorization.
    pub(super) async fn download_block(&mut self, hash: block::Hash) -> Result<Handler, PeerError> {
        self.compact.expire();
        if let Some(reconstructed) = self
            .compact
            .reconstructed
            .as_ref()
            .filter(|reconstructed| reconstructed.hash == hash)
        {
            let block = reconstructed.block.clone();
            return Ok(self.compact_response(block));
        }
        if let Some(received) = self
            .compact
            .received
            .take_if(|received| received.hash == hash)
        {
            if let Some(handler) = self.reconstruct(received).await? {
                return Ok(handler);
            }
        }
        self.send_download_request(Message::GetData(vec![hash.into()]))
            .await?;
        Ok(full_block_handler(hash))
    }

    /// Rebuilds the `received` block from mempool transactions.
    ///
    /// Returns the response handler if the block is complete, or the handler that waits for
    /// `blocktxn` after writing `getblocktxn` for the missing transactions. Returns `None` if
    /// the inbound service fails, or if reconstruction or its checks fail.
    async fn reconstruct(&mut self, received: ReceivedBlock) -> Result<Option<Handler>, PeerError> {
        let ReceivedBlock {
            compact,
            hash,
            until: _,
            slot,
        } = received;
        let Some(Response::CompactBlockTransactions(candidates)) = self
            .compact_query(Request::CompactBlockTransactions(compact.clone()))
            .await
        else {
            debug!("downloading the full block: no compact block candidates");
            return Ok(None);
        };
        let Ok((Ok(reconstruction), slot)) =
            tokio::task::spawn_blocking(move || (compact.reconstruct(candidates), slot)).await
        else {
            debug!("downloading the full block: the cmpctblock could not be rebuilt");
            return Ok(None);
        };

        let indexes = reconstruction.missing_indexes();
        if indexes.is_empty() {
            let complete = BlockTransactions {
                block_hash: hash,
                transactions: Vec::new(),
            };
            let block = self.finish_compact(reconstruction, complete, slot).await;
            return Ok(block.map(|block| self.compact_response(block)));
        }

        self.send_download_request(Message::GetBlockTransactions(BlockTransactionsRequest {
            block_hash: hash,
            indexes,
        }))
        .await?;
        Ok(Some(Handler::CompactBlock {
            reconstruction,
            slot,
        }))
    }

    /// Writes `message`, a request for a block's transactions, after rechecking the peer's
    /// live authorization immediately before the write.
    ///
    /// Fails if the write fails, or if the peer's version is now below the minimum or its IP
    /// is now banned.
    async fn send_download_request(&mut self, message: Message) -> Result<(), PeerError> {
        let address = self.connection_info.connected_addr.get_transient_addr();
        let version = self.connection_info.remote.version;
        let minimum = &mut self.minimum_peer_version;
        let bans = &self.bans;
        let authorized = self
            .peer_tx
            .send_authorized(message, || {
                version >= minimum.current()
                    && !address.is_some_and(|addr| bans.contains(addr.ip()))
            })
            .await?;
        if authorized {
            Ok(())
        } else {
            Err(PeerError::ConnectionDropped)
        }
    }

    /// Returns the handler that answers a block request with the reconstructed `block`, and
    /// charges its decoded bytes until the client takes the response.
    ///
    /// The delivery marks the response as early relay, so the block verifier does not score
    /// this peer for a later transaction or contextual failure.
    fn compact_response(&mut self, block: BlockHandle) -> Handler {
        self.charge_response_payload(block.decoded_bytes());
        let source = self.connection_info.connected_addr.get_transient_addr();
        Handler::Finished(Ok(Response::Blocks(vec![InventoryResponse::Available((
            block, source,
        ))])))
    }

    /// Fills the missing transactions of `reconstruction` from `response`, and checks the
    /// rebuilt block with the inbound service.
    ///
    /// On success, holds the block for [`RETENTION`] with `slot`, so another request for it is
    /// answered at once, and returns it. Returns `None` if the block does not match the
    /// announcement. A short ID can match the wrong transaction, so a mismatch is not
    /// misbehavior.
    async fn finish_compact(
        &mut self,
        reconstruction: Reconstruction,
        response: BlockTransactions,
        slot: OwnedSemaphorePermit,
    ) -> Option<BlockHandle> {
        let hash = reconstruction.block_hash();
        let (block, slot) =
            tokio::task::spawn_blocking(move || (reconstruction.finish(response), slot))
                .await
                .ok()?;
        let Ok(block) = block else {
            debug!("downloading the full block: the blocktxn does not fill the cmpctblock");
            return None;
        };
        if !matches!(
            self.compact_query(Request::CheckCompactBlock(block.clone()))
                .await,
            Some(Response::Nil)
        ) {
            debug!("downloading the full block: the rebuilt block failed its checks");
            return None;
        }

        let block = BlockHandle::from(block);
        self.compact.reconstructed = Some(ReconstructedBlock {
            block: block.clone(),
            hash,
            until: Instant::now() + RETENTION,
            _slot: slot,
        });
        Some(block)
    }

    /// Handles the peer's `blocktxn`, which finishes the reconstruction the pending request
    /// waits for.
    ///
    /// Ignores a `blocktxn` unless the pending request waits for the transactions of its
    /// block. Downloads the full block if the transactions do not finish a block that passes
    /// its checks.
    pub(super) async fn receive_compact_transactions(&mut self, response: BlockTransactions) {
        let hash = response.block_hash;
        let State::AwaitingResponse { handler, .. } = &mut self.state else {
            debug!("ignoring blocktxn: no request is pending");
            return;
        };
        if !matches!(
            handler,
            Handler::CompactBlock { reconstruction, .. } if reconstruction.block_hash() == hash
        ) {
            debug!("ignoring blocktxn: the pending request does not wait for it");
            return;
        }
        let Handler::CompactBlock {
            reconstruction,
            slot,
        } = std::mem::replace(handler, Handler::Finished(Ok(Response::Nil)))
        else {
            unreachable!("the pending request waits for this block's transactions");
        };
        self.clear_compact_recovery();

        match self.finish_compact(reconstruction, response, slot).await {
            Some(block) => {
                let response = self.compact_response(block);
                if let State::AwaitingResponse { handler, .. } = &mut self.state {
                    *handler = response;
                }
            }
            None => self.fallback_compact(hash).await,
        }
    }

    /// Writes a full `getdata` for the block `hash` after a failed or late recovery, and waits
    /// for the block for [`REQUEST_TIMEOUT`]. A failed write fails the connection.
    pub(super) async fn fallback_compact(&mut self, hash: block::Hash) {
        self.clear_compact_recovery();
        if let Err(error) = self
            .send_download_request(Message::GetData(vec![hash.into()]))
            .await
        {
            self.fail_with(error).await;
            return;
        }
        if let State::AwaitingResponse { handler, .. } = &mut self.state {
            *handler = full_block_handler(hash);
            self.request_timer = Some(Box::pin(sleep(REQUEST_TIMEOUT)));
        }
    }
}

impl<S, Tx, C> Connection<S, Tx, C>
where
    Tx: Sink<Message, Error = SerializationError> + Unpin,
{
    /// Tells the codec that no `blocktxn` is expected, before the connection drops or replaces
    /// a handler that waits for one.
    pub(super) fn clear_compact_recovery(&self) {
        if let Some(recovery) = &self.compact_recovery {
            recovery.send_replace(None);
        }
    }

    /// Tells the codec to accept a `blocktxn` for `hash` until `deadline`.
    pub(super) fn expect_compact_recovery(&self, hash: block::Hash, deadline: Instant) {
        if let Some(recovery) = &self.compact_recovery {
            recovery.send_replace(Some((hash, deadline)));
        }
    }
}

/// Waits for a high-bandwidth receive slot from `slots`, or forever if `slots` is `None`.
pub(super) async fn acquire_receive_slot(slots: Option<Arc<Semaphore>>) -> OwnedSemaphorePermit {
    match slots {
        Some(slots) => slots
            .acquire_owned()
            .await
            .expect("the inventory registry owns the receive slots and never closes them"),
        None => futures::future::pending().await,
    }
}
