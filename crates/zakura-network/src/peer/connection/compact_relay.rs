//! Per-connection compact block relay state, from the draft Compact Block Relay ZIP.
//!
//! Only connections that negotiated compact blocks (`early_block_relay`) use this state.
//!
//! A connection announces a block with `cmpctblock` when the peer asked for version 1
//! announcements, and then holds the block for [`RETENTION`] to serve the peer's
//! `getblocktxn`. It holds at most one sent block, and each held block takes a permit from
//! the global [`SLOTS`].

use std::{
    sync::{Arc, LazyLock},
    time::Duration,
};

use futures::Sink;
use indexmap::IndexSet;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::{sleep_until, timeout, timeout_at, Instant},
};
use tower::{Service, ServiceExt};

use zakura_chain::{
    block::{self, MAX_BLOCK_BYTES},
    chain_tip::ChainTip,
    serialization::{CompactSizeMessage, SerializationError, ZcashSerialize},
};
use zakura_node_services::block_memory::BlockHandle;

use super::Connection;
use crate::{
    constants::REQUEST_TIMEOUT,
    protocol::{
        external::{
            compact::{BlockTransactions, BlockTransactionsRequest, CompactBlock},
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

/// How long a connection holds a block it announced with `cmpctblock`, to serve the peer's
/// `getblocktxn`.
pub(super) const RETENTION: Duration = Duration::from_secs(15);

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
}

impl Default for CompactRelay {
    fn default() -> Self {
        Self {
            send: false,
            receive_permit: None,
            slots: SLOTS.clone(),
            sent: None,
            recovery_bytes_left: 0,
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

    /// Releases the sent block and its slot once [`RETENTION`] has ended.
    pub(super) fn expire(&mut self) {
        if self.expiry().is_some_and(|until| until <= Instant::now()) {
            self.clear_sent();
        }
    }

    /// Returns when the sent block expires, if this connection holds one.
    pub(super) fn expiry(&self) -> Option<Instant> {
        self.sent.as_ref().map(|sent| sent.until)
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
        let request = Request::EarlyBlocksByHash(IndexSet::from([hash]));
        let response = timeout(REQUEST_TIMEOUT, async {
            self.svc.ready().await.ok()?.call(request).await.ok()
        })
        .await
        .ok()
        .flatten()?;
        response
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
