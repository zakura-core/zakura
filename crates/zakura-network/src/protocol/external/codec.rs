//! A Tokio codec mapping byte streams to Bitcoin message streams.

use std::{
    fmt,
    io::{BufWriter, Cursor, Read, Write},
    panic::{self, AssertUnwindSafe},
    sync::{Arc, Mutex, OnceLock},
};

#[cfg(test)]
use std::cell::Cell;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use bytes::{BufMut, BytesMut};
use chrono::{TimeZone, Utc};
use indexmap::IndexMap;
use tokio_util::codec::{Decoder, Encoder};

use zakura_chain::{
    block::{self, Block},
    parameters::{Magic, Network},
    serialization::{
        sha256d, zcash_deserialize_string_external_count, CompactSizeMessage, FakeWriter,
        ReadZcashExt, SerializationError as Error, ZcashDeserialize, ZcashDeserializeInto,
        ZcashReader, ZcashSerialize, MAX_HEADERS_PER_MESSAGE, MAX_PROTOCOL_MESSAGE_LEN,
    },
    transaction::Transaction,
};

use zakura_node_services::block_memory::BlockHandle;

use crate::constants;

use super::{
    addr::{AddrInVersion, AddrV1, AddrV2},
    message::{
        Message, RejectReason, VersionMessage, MAX_REJECT_MESSAGE_LENGTH, MAX_REJECT_REASON_LENGTH,
        MAX_USER_AGENT_LENGTH,
    },
    types::*,
};

#[cfg(test)]
mod tests;

/// The length of a Bitcoin message header.
const HEADER_LEN: usize = 24usize;

/// Bloom filter commands accepted and discarded for peer compatibility.
const BLOOM_FILTER_COMMANDS: [[u8; 12]; 3] =
    [*b"filterload\0\0", *b"filteradd\0\0\0", *b"filterclear\0"];

/// The most body bytes the decoder reserves before they arrive.
///
/// A peer names a body length in a 24-byte header. Reserving only this much keeps a
/// hostile length from allocating a whole body. The buffer then grows with the bytes
/// that actually arrive, and TCP flow control bounds those.
const MAX_UNRECEIVED_BODY_RESERVATION: usize = 64 * 1024;

/// The most bytes one connection's decoder can hold for an incomplete message: one
/// header and one body of the protocol's maximum size.
pub(crate) const MAX_INCOMPLETE_MESSAGE_BYTES: usize = HEADER_LEN + MAX_PROTOCOL_MESSAGE_LEN;

/// The maximum body length allowed before the handshake completes.
///
/// Version messages are ~344 bytes max (including a 256-byte user agent);
/// verack is 0 bytes. 1 KB provides headroom for future protocol changes.
const MAX_HANDSHAKE_BODY_LEN: usize = 1024;

/// The smallest `block` message body whose checksum is shared between connections.
///
/// Sharing avoids hashing the body on each send, at the cost of a shared cache lookup. The
/// message already carries its block hash, so the lookup does not hash the header again.
/// The `block_checksum_threshold` benchmark compares the lookup and body-hashing costs;
/// this conservative threshold keeps small messages off the shared checksum path.
pub(crate) const MIN_SHARED_CHECKSUM_BODY_LEN: usize = 8 * 1024;

/// The number of blocks whose checksums are remembered.
///
/// Peers request the newest few blocks in bursts. The rest of the room keeps those entries
/// while syncing peers request older large blocks in between.
const SHARED_CHECKSUM_BLOCKS: usize = 32;

/// The parse error returned when a legacy message body parser panics.
const PANICKED_MESSAGE_BODY_PARSE_ERROR: &str = "panic while parsing peer message body";

/// Returns the message header `command` bytes escaped as printable ASCII.
///
/// # Security
///
/// The command field is attacker-controlled, so it must be escaped before
/// logging. Otherwise, a peer can inject newlines, ANSI escape sequences, or
/// other control characters into log output.
fn escape_command(command: &[u8; 12]) -> String {
    command
        .iter()
        .copied()
        .flat_map(std::ascii::escape_default)
        .map(char::from)
        .collect()
}

/// Writes `count` zero bytes to `writer`.
fn write_zeros<W: Write + ?Sized>(writer: &mut W, count: u32) -> Result<(), Error> {
    const ZEROS: [u8; 4096] = [0; 4096];
    // Widening u32 to usize is lossless on supported targets.
    let mut remaining = count as usize;
    while remaining > 0 {
        let len = remaining.min(ZEROS.len());
        writer.write_all(&ZEROS[..len])?;
        remaining -= len;
    }
    Ok(())
}

#[cfg(test)]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum BodyDecodePanic {
    CommandDispatch,
    AfterBlockConversion,
    AfterTransactionConversion,
}

/// Checksums of the large `block` messages this node sent or is about to send.
///
/// Sending the same block body to multiple peers uses the same message checksum.
/// Hashing a full block takes milliseconds, and many peers request a new block at the same
/// moment. Connections that share one `BlockChecksums` hash each block once: concurrent sends
/// of a block wait for the one send that is hashing it, then reuse its checksum. Block
/// advertisement can also [`precompute`](Self::precompute) a checksum before peers request
/// the block.
///
/// A checksum is found by the block's hash. Bodies using this cache must come from committed
/// state, or from trusted `submitblock` callers after semantic verification. Before contextual
/// verification checks `hashBlockCommitments`, different v5 authorization data can produce
/// bodies with the same block hash and length, even when their proofs and signatures verify.
/// Untrusted bodies served before that check must not use this cache: they could leave a
/// checksum that is later reused for a different body. Cache hits refresh the entry's recency,
/// so repeated requests can keep such an entry present.
///
/// The body length is checked as well, so a body of another length is hashed again. This check
/// does not distinguish different bodies of the same length or replace commitment validation.
#[derive(Clone, Debug, Default)]
pub(crate) struct BlockChecksums(Arc<Mutex<IndexMap<block::Hash, (usize, ChecksumSlot)>>>);

/// The checksum of one block body, set once by whichever send or precomputation hashes it
/// first. Others that need it meanwhile wait for that hash instead of repeating it.
type ChecksumSlot = Arc<OnceLock<sha256d::Checksum>>;

impl BlockChecksums {
    /// Returns the body length and checksum remembered for the block `hash`, if hashing has
    /// finished.
    ///
    /// A block that is found becomes the last one to be forgotten.
    pub(crate) fn get(&self, hash: block::Hash) -> Option<(usize, sha256d::Checksum)> {
        let mut checksums = self
            .0
            .lock()
            .expect("the lock is only held for map operations, which do not panic");
        let (remembered_len, slot) = checksums.shift_remove(&hash)?;
        let remembered = slot
            .get()
            .copied()
            .map(|checksum| (remembered_len, checksum));
        checksums.insert(hash, (remembered_len, slot));
        remembered
    }

    /// Returns the checksum of the `body_len` byte body of the block `hash`.
    ///
    /// Calls `compute` only if no other caller has hashed this body or is hashing it. Callers
    /// that arrive while another one hashes the body block until it finishes, which takes no
    /// longer than hashing the body themselves. The map lock is never held while hashing.
    ///
    /// The block becomes the last one to be forgotten. If there is no room, the block that was
    /// used least recently is forgotten.
    pub(crate) fn checksum_with(
        &self,
        hash: block::Hash,
        body_len: usize,
        compute: impl FnOnce() -> sha256d::Checksum,
    ) -> sha256d::Checksum {
        let slot = self.slot(hash, body_len);
        *slot.get_or_init(compute)
    }

    /// Hashes the `block` message body of `block` and remembers its checksum, unless the body
    /// is too small to share or its checksum is already known or being calculated.
    ///
    /// The body is hashed as it is serialized, so the full body is never held in memory. This
    /// takes milliseconds for a large block, so async callers must run it on a blocking thread.
    pub(crate) fn precompute(&self, block: &BlockHandle) {
        let body_len = block.zcash_serialized_size();
        if body_len < MIN_SHARED_CHECKSUM_BODY_LEN {
            return;
        }

        let mut precomputed = false;
        self.checksum_with(block.hash(), body_len, || {
            precomputed = true;
            serialized_block_checksum(block)
        });
        if precomputed {
            metrics::counter!("peer.block_checksum.precomputed").increment(1);
        }
    }

    /// Returns the slot for the `body_len` byte body of the block `hash`, adding an empty one
    /// if the block is unknown or was remembered with another body length.
    fn slot(&self, hash: block::Hash, body_len: usize) -> ChecksumSlot {
        let mut checksums = self
            .0
            .lock()
            .expect("the lock is only held for map operations, which do not panic");
        let slot = match checksums.shift_remove(&hash) {
            Some((remembered_len, slot)) if remembered_len == body_len => slot,
            _ => ChecksumSlot::default(),
        };
        if checksums.len() >= SHARED_CHECKSUM_BLOCKS {
            checksums.shift_remove_index(0);
        }
        checksums.insert(hash, (body_len, slot.clone()));
        slot
    }
}

/// Returns the message checksum of `block`'s serialized body, without buffering the body.
fn serialized_block_checksum(block: &BlockHandle) -> sha256d::Checksum {
    /// Feeds written bytes into a SHA-256 digest.
    struct DigestWriter(ring::digest::Context);

    impl Write for DigestWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    // Serialization writes many small fields. Buffering them keeps each digest update large.
    let mut writer = BufWriter::with_capacity(
        MIN_SHARED_CHECKSUM_BODY_LEN,
        DigestWriter(ring::digest::Context::new(&ring::digest::SHA256)),
    );
    block
        .zcash_serialize(&mut writer)
        .expect("writing to a digest does not fail");
    let first = writer
        .into_inner()
        .map_err(|error| error.into_error())
        .expect("flushing to a digest does not fail")
        .0
        .finish();
    sha256d_checksum_of_digest(first)
}

/// The command of a `block` message, as it appears in a message header.
const BLOCK_COMMAND: &[u8; 12] = b"block\0\0\0\0\0\0\0";

/// The first matching frame publishes its size and wakes the peer set holding the hedge.
#[derive(Debug, Default)]
pub(crate) struct BlockReplySize {
    bytes: OnceLock<u64>,
    waker: futures::task::AtomicWaker,
}

impl BlockReplySize {
    pub(crate) fn publish(&self, bytes: u64) {
        if self.bytes.set(bytes).is_ok() {
            self.waker.wake();
        }
    }

    pub(crate) fn get(&self) -> Option<u64> {
        self.bytes.get().copied()
    }

    /// Register before reading so that a size learned during a routing decision wakes it.
    pub(crate) fn read_for_routing(&self, cx: &std::task::Context<'_>) -> Option<u64> {
        self.waker.register(cx.waker());
        self.get()
    }
}

/// A request's destination for the size declared by its matching block message.
#[derive(Clone, Debug)]
struct RequestedBlockSize {
    hash: block::Hash,
    bytes: Arc<BlockReplySize>,
    /// Set if this watch was active when the decoder first saw a fragmented transport header.
    saw_header_start: bool,
}

/// Shares the active primary request between the connection and its decoder.
#[derive(Clone, Debug, Default)]
pub(crate) struct InboundBlockSize(Arc<Mutex<Option<RequestedBlockSize>>>);

impl InboundBlockSize {
    /// Called after the connection writes a single-block request.
    pub(crate) fn watch(&self, hash: block::Hash, bytes: Arc<BlockReplySize>) {
        *self.0.lock().expect("block size lock is never poisoned") = Some(RequestedBlockSize {
            hash,
            bytes,
            saw_header_start: false,
        });
    }

    /// Mark the current watch without capturing a size destination before the command and length are known.
    fn mark_header_start(&self) {
        if let Some(request) = self
            .0
            .lock()
            .expect("block size lock is never poisoned")
            .as_mut()
        {
            request.saw_header_start = true;
        }
    }

    /// A watch installed after a partial header arrived cannot own that frame.
    fn snapshot(&self, fragmented_header: bool) -> Option<RequestedBlockSize> {
        self.0
            .lock()
            .expect("block size lock is never poisoned")
            .as_ref()
            .filter(|request| !fragmented_header || request.saw_header_start)
            .cloned()
    }

    /// Stop observing this request when the connection becomes idle.
    pub(crate) fn clear(&self) {
        self.0
            .lock()
            .expect("block size lock is never poisoned")
            .take();
    }
}

/// A codec which produces Bitcoin messages from byte streams and vice versa.
pub struct Codec {
    builder: Builder,
    state: DecodeState,
    /// The request to observe when the next block message starts.
    inbound_block_size: InboundBlockSize,
    #[cfg(test)]
    body_decode_panic: Cell<Option<BodyDecodePanic>>,
}

/// A builder for specifying [`Codec`] options.
pub struct Builder {
    /// The network magic to use in encoding.
    network: Network,
    /// The protocol version to speak when encoding/decoding.
    version: Version,
    /// The maximum allowable message length.
    max_len: usize,
    /// The block checksums this codec reuses. A codec has its own unless it is given shared ones.
    block_checksums: BlockChecksums,
}

impl Codec {
    /// Return a builder for constructing a [`Codec`].
    pub fn builder() -> Builder {
        Builder {
            network: Network::Mainnet,
            version: constants::CURRENT_NETWORK_PROTOCOL_VERSION,
            max_len: MAX_HANDSHAKE_BODY_LEN,
            block_checksums: BlockChecksums::default(),
        }
    }

    /// Connect the request handler to this decoder before splitting the framed transport.
    pub(crate) fn inbound_block_size(&self) -> InboundBlockSize {
        self.inbound_block_size.clone()
    }

    /// Reconfigure the version used by the codec, e.g., after completing a handshake.
    pub fn reconfigure_version(&mut self, version: Version) {
        self.builder.version = version;
    }

    /// Raise the maximum accepted body length to the full protocol limit.
    ///
    /// Called after a successful handshake so post-handshake messages (blocks,
    /// transactions) can use the full `MAX_PROTOCOL_MESSAGE_LEN`.
    pub fn reconfigure_full_body_len(&mut self) {
        self.builder.max_len = MAX_PROTOCOL_MESSAGE_LEN;
    }
}

impl Builder {
    /// Finalize the builder and return a [`Codec`].
    pub fn finish(self) -> Codec {
        Codec {
            builder: self,
            state: DecodeState::Head { fragmented: false },
            inbound_block_size: InboundBlockSize::default(),
            #[cfg(test)]
            body_decode_panic: Cell::new(None),
        }
    }

    /// Configure the codec for the given [`Network`].
    pub fn for_network(mut self, network: &Network) -> Self {
        self.network = network.clone();
        self
    }

    /// Configure the codec's maximum accepted payload size, in bytes.
    #[allow(dead_code)]
    pub fn with_max_body_len(mut self, len: usize) -> Self {
        self.max_len = len;
        self
    }

    /// Share `block_checksums` with the other codecs that were given a clone of it, so later
    /// sends can reuse a block's remembered checksum.
    pub(crate) fn with_block_checksums(mut self, block_checksums: BlockChecksums) -> Self {
        self.block_checksums = block_checksums;
        self
    }
}

/// Returns the checksum in a message header: the first four bytes of SHA-256 applied twice to
/// `body`.
///
/// This is the same value as [`sha256d::Checksum::from`]. It is calculated with `ring`, whose
/// SHA-256 uses assembly for the instructions each CPU has. On CPUs without SHA instructions it
/// hashes about twice as fast as `sha2`. Every message sent or received is hashed on its
/// connection's task, and blocks are the largest messages, so this hash bounds how fast one
/// connection can send or receive blocks.
fn message_checksum(body: &[u8]) -> sha256d::Checksum {
    sha256d_checksum_of_digest(ring::digest::digest(&ring::digest::SHA256, body))
}

/// Returns the message checksum whose first SHA-256 pass produced `first`.
fn sha256d_checksum_of_digest(first: ring::digest::Digest) -> sha256d::Checksum {
    let second = ring::digest::digest(&ring::digest::SHA256, first.as_ref());
    let mut checksum = [0; 4];
    checksum.copy_from_slice(&second.as_ref()[..4]);
    sha256d::Checksum(checksum)
}

// ======== Encoding =========

impl Encoder<Message> for Codec {
    type Error = Error;

    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        use Error::Parse;
        use Message::*;
        // Note: because all match arms must have
        // the same type, and the array length is
        // part of the type, having at least one
        // of length 12 checks that they are all
        // of length 12, as they must be &[u8; 12].
        let command = match item {
            Version { .. } => b"version\0\0\0\0\0",
            Verack => b"verack\0\0\0\0\0\0",
            Ping { .. } | PaddedPing { .. } => b"ping\0\0\0\0\0\0\0\0",
            Pong { .. } => b"pong\0\0\0\0\0\0\0\0",
            Reject { .. } => b"reject\0\0\0\0\0\0",
            P2pV2Upgrade(_) => crate::zakura::P2P_V2_UPGRADE_COMMAND_BYTES,
            Addr { .. } => b"addr\0\0\0\0\0\0\0\0",
            GetAddr => b"getaddr\0\0\0\0\0",
            Block { .. } => BLOCK_COMMAND,
            GetBlocks { .. } => b"getblocks\0\0\0",
            Headers { .. } => b"headers\0\0\0\0\0",
            GetHeaders { .. } => b"getheaders\0\0",
            Inv { .. } => b"inv\0\0\0\0\0\0\0\0\0",
            GetData { .. } => b"getdata\0\0\0\0\0",
            NotFound { .. } => b"notfound\0\0\0\0",
            Tx { .. } => b"tx\0\0\0\0\0\0\0\0\0\0",
            Mempool => b"mempool\0\0\0\0\0",
        };

        // Reserve the whole frame before writing: growing `dst` while the body is written
        // copies it again at each step, which costs more than measuring it first. A block
        // that was sent before has its body length remembered with its checksum, so it is
        // serialized only once; every other message is measured first.
        let block_hash = match &item {
            Message::Block(block) => Some(block.hash()),
            _ => None,
        };
        // One lookup gives both the length to reserve and, once the body is written, its
        // checksum. A remembered length only sizes the reservation; the size limit is checked
        // against the bytes actually written below.
        let remembered = block_hash.and_then(|hash| self.builder.block_checksums.get(hash));
        let expected_len = match remembered {
            Some((len, _)) => len.min(self.builder.max_len),
            None => {
                let len = self.body_length(&item)?;
                if len > self.builder.max_len {
                    return Err(Parse("body length exceeded maximum size"));
                }
                len
            }
        };

        // The header's length and checksum are zero until the body is written, then filled
        // in from the bytes actually written.
        let start_len = dst.len();
        dst.reserve(HEADER_LEN + expected_len);
        let written = (|| {
            let dst = &mut dst.writer();
            dst.write_all(&self.builder.network.magic().0[..])?;
            dst.write_all(command)?;
            dst.write_u32::<LittleEndian>(0)?;
            dst.write_u32::<LittleEndian>(0)?;
            self.write_body(&item, dst)
        })();

        let body_length = dst.len() - start_len - HEADER_LEN;
        if let Err(error) = written {
            dst.truncate(start_len);
            return Err(error);
        }
        if body_length > self.builder.max_len {
            dst.truncate(start_len);
            return Err(Parse("body length exceeded maximum size"));
        }

        trace!(?item, len = body_length);
        // Aggregate totals only: per-addr labels are never pruned by the Prometheus
        // exporter and grow without bound on public listeners.
        metrics::counter!("zcash.net.out.bytes.total").increment((body_length + HEADER_LEN) as u64);

        // Production codecs cap bodies at `MAX_PROTOCOL_MESSAGE_LEN`, which fits in a u32.
        dst[start_len + 16..][..4].copy_from_slice(&(body_length as u32).to_le_bytes());
        let checksum = self.body_checksum(block_hash, remembered, &dst[start_len + HEADER_LEN..]);
        dst[start_len + 20..][..4].copy_from_slice(&checksum.0);

        Ok(())
    }
}

impl Codec {
    /// Obtain the size of the body of a given message. This will match the
    /// number of bytes written to the writer provided to `write_body` for the
    /// same message.
    ///
    /// Returns the error `write_body` would return for a message it refuses to write.
    fn body_length(&self, msg: &Message) -> Result<usize, Error> {
        let mut writer = FakeWriter(0);
        self.write_body(msg, &mut writer)?;
        Ok(writer.0)
    }

    /// Returns the checksum of `body`, the serialized body of a message, where `block_hash`
    /// is the hash of the block the message carries, if it carries one, and `remembered` is
    /// what [`BlockChecksums`] held for that block before the body was written.
    ///
    /// A large block's remembered checksum is used when its body length matches. Otherwise
    /// the send shares any calculation already in progress, or hashes and remembers the body.
    fn body_checksum(
        &self,
        block_hash: Option<block::Hash>,
        remembered: Option<(usize, sha256d::Checksum)>,
        body: &[u8],
    ) -> sha256d::Checksum {
        let hash = match block_hash {
            Some(hash) if body.len() >= MIN_SHARED_CHECKSUM_BODY_LEN => hash,
            _ => return message_checksum(body),
        };

        if let Some((remembered_len, checksum)) = remembered {
            if remembered_len == body.len() {
                metrics::counter!("peer.block_checksum.hit").increment(1);
                return checksum;
            }
        }

        let mut hashed_here = false;
        let checksum = self
            .builder
            .block_checksums
            .checksum_with(hash, body.len(), || {
                hashed_here = true;
                message_checksum(body)
            });
        if hashed_here {
            metrics::counter!("peer.block_checksum.miss").increment(1);
        } else {
            metrics::counter!("peer.block_checksum.hit").increment(1);
        }
        checksum
    }

    /// Write the body of the message into the given writer. This allows writing
    /// the message body prior to writing the header, so that the header can
    /// contain a checksum of the message body.
    fn write_body<W: Write + ?Sized>(&self, msg: &Message, writer: &mut W) -> Result<(), Error> {
        match msg {
            Message::Version(VersionMessage {
                version,
                services,
                timestamp,
                address_recv,
                address_from,
                nonce,
                user_agent,
                start_height,
                relay,
            }) => {
                writer.write_u32::<LittleEndian>(version.0)?;
                writer.write_u64::<LittleEndian>(services.bits())?;
                // # Security
                // DateTime<Utc>::timestamp has a smaller range than i64, so
                // serialization can not error.
                writer.write_i64::<LittleEndian>(timestamp.timestamp())?;

                address_recv.zcash_serialize(writer)?;
                address_from.zcash_serialize(writer)?;

                writer.write_u64::<LittleEndian>(nonce.0)?;

                if user_agent.len() > MAX_USER_AGENT_LENGTH {
                    // zcashd won't accept this version message
                    return Err(Error::Parse(
                        "user agent too long: must be 256 bytes or less",
                    ));
                }

                user_agent.zcash_serialize(writer)?;
                writer.write_u32::<LittleEndian>(start_height.0)?;
                writer.write_u8(*relay as u8)?;
            }
            Message::Verack => { /* Empty payload -- no-op */ }
            Message::Ping(nonce) => {
                writer.write_u64::<LittleEndian>(nonce.0)?;
            }
            Message::PaddedPing { nonce, padding } => {
                writer.write_u64::<LittleEndian>(nonce.0)?;
                write_zeros(writer, *padding)?;
            }
            Message::Pong(nonce) => {
                writer.write_u64::<LittleEndian>(nonce.0)?;
            }
            Message::Reject {
                message,
                ccode,
                reason,
                data,
            } => {
                if message.len() > MAX_REJECT_MESSAGE_LENGTH {
                    // zcashd won't accept this reject message
                    return Err(Error::Parse(
                        "reject message too long: must be 12 bytes or less",
                    ));
                }

                message.zcash_serialize(writer)?;

                writer.write_u8(*ccode as u8)?;

                if reason.len() > MAX_REJECT_REASON_LENGTH {
                    return Err(Error::Parse(
                        "reject reason too long: must be 111 bytes or less",
                    ));
                }

                reason.zcash_serialize(writer)?;
                if let Some(data) = data {
                    writer.write_all(data)?;
                }
            }
            Message::P2pV2Upgrade(payload) => {
                if payload.len() > crate::zakura::MAX_PRELUDE_PAYLOAD_BYTES {
                    return Err(Error::Parse(
                        "Zakura p2pv2up payload exceeds the hard prelude cap",
                    ));
                }
                writer.write_all(payload)?;
            }
            Message::Addr(addrs) => {
                assert!(
                    addrs.len() <= constants::MAX_ADDRS_IN_MESSAGE,
                    "unexpectedly large Addr message: greater than MAX_ADDRS_IN_MESSAGE addresses"
                );

                // Regardless of the way we received the address,
                // Zebra always sends `addr` messages
                let v1_addrs: Vec<AddrV1> = addrs.iter().map(|addr| AddrV1::from(*addr)).collect();
                v1_addrs.zcash_serialize(writer)?
            }
            Message::GetAddr => { /* Empty payload -- no-op */ }
            Message::Block(block) => block.zcash_serialize(writer)?,
            Message::GetBlocks { known_blocks, stop } => {
                writer.write_u32::<LittleEndian>(self.builder.version.0)?;
                known_blocks.zcash_serialize(writer)?;
                stop.unwrap_or(block::Hash([0; 32]))
                    .zcash_serialize(writer)?;
            }
            Message::GetHeaders { known_blocks, stop } => {
                writer.write_u32::<LittleEndian>(self.builder.version.0)?;
                known_blocks.zcash_serialize(writer)?;
                stop.unwrap_or(block::Hash([0; 32]))
                    .zcash_serialize(writer)?;
            }
            Message::Headers(headers) => headers.zcash_serialize(writer)?,
            Message::Inv(hashes) => hashes.zcash_serialize(writer)?,
            Message::GetData(hashes) => hashes.zcash_serialize(writer)?,
            Message::NotFound(hashes) => hashes.zcash_serialize(writer)?,
            Message::Tx(transaction) => transaction.transaction().zcash_serialize(writer)?,
            Message::Mempool => { /* Empty payload -- no-op */ }
        }
        Ok(())
    }
}

// ======== Decoding =========

enum DecodeState {
    Head {
        fragmented: bool,
    },
    Body {
        body_len: usize,
        command: [u8; 12],
        checksum: sha256d::Checksum,
        requested_size: Option<RequestedBlockSize>,
    },
}

impl fmt::Debug for DecodeState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DecodeState::Head { .. } => write!(f, "DecodeState::Head"),
            DecodeState::Body {
                body_len,
                command,
                checksum,
                ..
            } => f
                .debug_struct("DecodeState::Body")
                .field("body_len", &body_len)
                .field("command", &String::from_utf8_lossy(command))
                .field("checksum", &checksum)
                .finish(),
        }
    }
}

impl Decoder for Codec {
    type Item = Message;
    type Error = Error;

    #[allow(clippy::unwrap_in_result)]
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        use Error::Parse;

        // Ignored Bloom and unknown commands continue to the next message in the same
        // call. Returning `Ok(None)` with a complete message buffered would stall the
        // reader until the peer sends more bytes. Each discarded body is within
        // `max_len`, and one call only discards bytes already buffered, so the work
        // per call stays bounded without a separate count.
        'messages: loop {
            match self.state {
                DecodeState::Head { fragmented } => {
                    // First check that the src buffer contains an entire header.
                    if src.len() < HEADER_LEN {
                        if !fragmented && !src.is_empty() {
                            self.inbound_block_size.mark_header_start();
                            self.state = DecodeState::Head { fragmented: true };
                        }
                        trace!(?self.state, "src buffer does not have an entire header, waiting");
                        // Signal that decoding requires more data.
                        return Ok(None);
                    }

                    // Now that we know that src contains a header, split off the header section.
                    let header = src.split_to(HEADER_LEN);

                    // Create a cursor over the header and parse its fields.
                    let mut header_reader = Cursor::new(&header);
                    let magic = Magic(header_reader.read_4_bytes()?);
                    let command = header_reader.read_12_bytes()?;
                    let declared_len = header_reader.read_u32::<LittleEndian>()?;
                    let body_len = declared_len as usize; // Supported targets have at least 32-bit usize.
                    let checksum = sha256d::Checksum(header_reader.read_4_bytes()?);
                    trace!(
                        ?self.state,
                        ?magic,
                        command = %escape_command(&command),
                        body_len,
                        ?checksum,
                        "read header from src buffer"
                    );

                    if magic != self.builder.network.magic() {
                        return Err(Parse("supplied magic did not meet expectations"));
                    }
                    if body_len > self.builder.max_len {
                        return Err(Parse("body length exceeded maximum size"));
                    }
                    if command == *crate::zakura::P2P_V2_UPGRADE_COMMAND_BYTES
                        && body_len > crate::zakura::MAX_PRELUDE_PAYLOAD_BYTES
                    {
                        return Err(Parse("Zakura p2pv2up payload exceeds the hard prelude cap"));
                    }

                    metrics::counter!("zcash.net.in.bytes.total")
                        .increment((body_len + HEADER_LEN) as u64);

                    // Reserve buffer space for the next arriving bytes, not the claimed body.
                    src.reserve(body_len.min(MAX_UNRECEIVED_BODY_RESERVATION) + HEADER_LEN);

                    self.state = DecodeState::Body {
                        body_len,
                        command,
                        checksum,
                        requested_size: if command == *BLOCK_COMMAND
                            && u64::from(declared_len) <= block::MAX_BLOCK_BYTES
                        {
                            self.inbound_block_size.snapshot(fragmented)
                        } else {
                            None
                        },
                    };

                    // Now that the state is updated, attempt body decoding.
                    continue 'messages;
                }
                DecodeState::Body {
                    body_len,
                    command,
                    checksum,
                    ref mut requested_size,
                } => {
                    // Inspect at most one bounded header per observed frame, even if it arrives
                    // one byte at a time. The full decoder still checks the checksum and body.
                    let prefix_len = body_len.min(block::MAX_SERIALIZED_HEADER_BYTES);
                    if src.len() >= prefix_len {
                        if let Some(requested) = requested_size.take() {
                            if block::Header::zcash_deserialize(&src[..prefix_len])
                                .is_ok_and(|header| header.hash() == requested.hash)
                            {
                                // The transport length was read from a u32.
                                requested.bytes.publish(body_len as u64);
                            }
                        }
                    }
                    if src.len() < body_len {
                        // Need to wait for the full body
                        trace!(?self.state, len = src.len(), "src buffer does not have an entire body, waiting");
                        return Ok(None);
                    }

                    // Now that we know we have the full body, split off the body,
                    // and reset the decoder state for the next message. Otherwise
                    // we will attempt to read the next header as the current body.
                    let body = src.split_to(body_len);
                    self.state = DecodeState::Head { fragmented: false };

                    if checksum != message_checksum(&body[..]) {
                        return Err(Parse(
                            "supplied message checksum does not match computed checksum",
                        ));
                    }

                    if BLOOM_FILTER_COMMANDS.contains(&command) {
                        metrics::counter!("zcash.net.in.discarded", "reason" => "bloom")
                            .increment(1);
                        debug!(
                            command = %escape_command(&command),
                            "ignored unsupported bloom filter message"
                        );
                        continue 'messages;
                    }

                    let mut body_reader = &body[..];

                    // The body bytes are owned locally and the decoder state was
                    // reset above, before entering this unwind boundary. Body
                    // readers do not mutate shared services or connection state.
                    // A caught panic returns an error that the network treats as
                    // terminal, dropping this codec and its peer transport.
                    let decode_result = panic::catch_unwind(AssertUnwindSafe(|| {
                        #[cfg(test)]
                        self.panic_at_body_decode_point(BodyDecodePanic::CommandDispatch);

                        let msg = match &command {
                            b"version\0\0\0\0\0" => self.read_version(&mut body_reader),
                            b"verack\0\0\0\0\0\0" => self.read_verack(&mut body_reader),
                            b"ping\0\0\0\0\0\0\0\0" => self.read_ping(&mut body_reader),
                            b"pong\0\0\0\0\0\0\0\0" => self.read_pong(&mut body_reader),
                            b"reject\0\0\0\0\0\0" => self.read_reject(&mut body_reader),
                            crate::zakura::P2P_V2_UPGRADE_COMMAND_BYTES => {
                                self.read_p2p_v2_upgrade(&mut body_reader, body_len)
                            }
                            b"addr\0\0\0\0\0\0\0\0" => self.read_addr(&mut body_reader),
                            b"addrv2\0\0\0\0\0\0" => self.read_addrv2(&mut body_reader),
                            b"getaddr\0\0\0\0\0" => self.read_getaddr(&mut body_reader),
                            BLOCK_COMMAND => self.read_block(&mut body_reader),
                            b"getblocks\0\0\0" => self.read_getblocks(&mut body_reader),
                            b"headers\0\0\0\0\0" => self.read_headers(&mut body_reader),
                            b"getheaders\0\0" => self.read_getheaders(&mut body_reader),
                            b"inv\0\0\0\0\0\0\0\0\0" => self.read_inv(&mut body_reader),
                            b"getdata\0\0\0\0\0" => self.read_getdata(&mut body_reader),
                            b"notfound\0\0\0\0" => self.read_notfound(&mut body_reader),
                            b"tx\0\0\0\0\0\0\0\0\0\0" => self.read_tx(&mut body_reader),
                            b"mempool\0\0\0\0\0" => self.read_mempool(&mut body_reader),
                            _ => {
                                // # Security
                                //
                                // The command bytes are attacker-controlled, so they are
                                // escaped before logging to stop peers injecting control
                                // characters into log output.
                                let command_string = escape_command(&command);

                                // # Security
                                //
                                // Zcash connections are not authenticated, so malicious nodes can
                                // send fake messages, with connected peers' IP addresses
                                // in the IP header.
                                //
                                // Since we can't verify their source, Zebra needs to ignore
                                // unexpected messages, because closing the connection could
                                // cause a denial of service or eclipse attack.
                                metrics::counter!("zcash.net.in.discarded", "reason" => "unknown")
                                    .increment(1);
                                debug!(?command, %command_string, "unknown message command from peer");
                                return Ok(None);
                            }
                        }?;

                        // Bitcoin allows extra data at the end of most messages,
                        // so old nodes can read newer formats and ignore extra fields.
                        let extra_bytes = body_reader.len();
                        if extra_bytes == 0 {
                            trace!(?extra_bytes, %msg, "finished message decoding");
                        } else {
                            // log when there are extra bytes, so we know when we need to
                            // upgrade message formats
                            debug!(?extra_bytes, %msg, "extra data after decoding message");
                        }

                        Ok(Some(msg))
                    }));

                    match decode_result {
                        // An unknown command decodes to nothing; continue to the next one.
                        Ok(Ok(None)) => continue 'messages,
                        Ok(result) => return result,
                        Err(_panic_payload) => {
                            // Escaped for consistency with the other command logs:
                            // reaching a body parser requires an exact known-command
                            // match, so this is defense in depth, not a live risk.
                            let command = escape_command(&command)
                                .trim_end_matches(r"\x00")
                                .to_owned();
                            metrics::counter!(
                                "peer.message.parse.panics",
                                "parser" => "legacy_body"
                            )
                            .increment(1);
                            tracing::error!(
                                %command,
                                "peer-controlled message parser panicked; disconnecting legacy peer"
                            );

                            return Err(Parse(PANICKED_MESSAGE_BODY_PARSE_ERROR));
                        }
                    }
                }
            }
        }
    }
}

impl Codec {
    /// Deserializes a version message.
    ///
    /// The `relay` field is optional, as defined in <https://developer.bitcoin.org/reference/p2p_networking.html#version>
    ///
    /// Note: zcashd only requires fields up to `address_recv`, but everything up to `relay` is required in Zebra.
    ///       see <https://github.com/zcash/zcash/blob/11d563904933e889a11d9685c3b249f1536cfbe7/src/main.cpp#L6490-L6507>
    fn read_version<R: Read>(&self, mut reader: R) -> Result<Message, Error> {
        Ok(VersionMessage {
            version: Version(reader.read_u32::<LittleEndian>()?),
            // Use from_bits_truncate to discard unknown service bits.
            services: PeerServices::from_bits_truncate(reader.read_u64::<LittleEndian>()?),
            timestamp: Utc
                .timestamp_opt(reader.read_i64::<LittleEndian>()?, 0)
                .single()
                .ok_or(Error::Parse(
                    "version timestamp is out of range for DateTime",
                ))?,
            address_recv: AddrInVersion::zcash_deserialize(&mut reader)?,
            address_from: AddrInVersion::zcash_deserialize(&mut reader)?,
            nonce: Nonce(reader.read_u64::<LittleEndian>()?),
            user_agent: {
                let byte_count: CompactSizeMessage = (&mut reader).zcash_deserialize_into()?;
                let byte_count: usize = byte_count.into();

                // # Security
                //
                // Limit peer set memory usage, Zebra stores an `Arc<VersionMessage>` per
                // connected peer.
                //
                // Without this check, we can use `200 peers * 2 MB message size limit = 400 MB`.
                if byte_count > MAX_USER_AGENT_LENGTH {
                    return Err(Error::Parse(
                        "user agent too long: must be 256 bytes or less",
                    ));
                }

                zcash_deserialize_string_external_count(byte_count, &mut reader)?
            },
            start_height: block::Height(reader.read_u32::<LittleEndian>()?),
            relay: match reader.read_u8() {
                Ok(val @ 0..=1) => val == 1,
                Ok(_) => return Err(Error::Parse("non-bool value supplied in relay field")),
                Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => true,
                Err(err) => Err(err)?,
            },
        }
        .into())
    }

    fn read_verack<R: Read>(&self, mut _reader: R) -> Result<Message, Error> {
        Ok(Message::Verack)
    }

    fn read_ping<R: Read>(&self, mut reader: R) -> Result<Message, Error> {
        Ok(Message::Ping(Nonce(reader.read_u64::<LittleEndian>()?)))
    }

    fn read_pong<R: Read>(&self, mut reader: R) -> Result<Message, Error> {
        Ok(Message::Pong(Nonce(reader.read_u64::<LittleEndian>()?)))
    }

    fn read_reject<R: Read>(&self, mut reader: R) -> Result<Message, Error> {
        Ok(Message::Reject {
            message: {
                let byte_count: CompactSizeMessage = (&mut reader).zcash_deserialize_into()?;
                let byte_count: usize = byte_count.into();

                // # Security
                //
                // Limit log size on disk, Zebra might print large reject messages to disk.
                if byte_count > MAX_REJECT_MESSAGE_LENGTH {
                    return Err(Error::Parse(
                        "reject message too long: must be 12 bytes or less",
                    ));
                }

                zcash_deserialize_string_external_count(byte_count, &mut reader)?
            },
            ccode: match reader.read_u8()? {
                0x01 => RejectReason::Malformed,
                0x10 => RejectReason::Invalid,
                0x11 => RejectReason::Obsolete,
                0x12 => RejectReason::Duplicate,
                0x40 => RejectReason::Nonstandard,
                0x41 => RejectReason::Dust,
                0x42 => RejectReason::InsufficientFee,
                0x43 => RejectReason::Checkpoint,
                0x50 => RejectReason::Other,
                _ => return Err(Error::Parse("invalid RejectReason value in ccode field")),
            },
            reason: {
                let byte_count: CompactSizeMessage = (&mut reader).zcash_deserialize_into()?;
                let byte_count: usize = byte_count.into();

                // # Security
                //
                // Limit log size on disk, Zebra might print large reject messages to disk.
                if byte_count > MAX_REJECT_REASON_LENGTH {
                    return Err(Error::Parse(
                        "reject reason too long: must be 111 bytes or less",
                    ));
                }

                zcash_deserialize_string_external_count(byte_count, &mut reader)?
            },
            // Sometimes there's data, sometimes there isn't. There's no length
            // field, this is just implicitly encoded by the body_len.
            // Apparently all existing implementations only supply 32 bytes of
            // data (hash identifying the rejected object) or none (and we model
            // the Reject message that way), so instead of passing in the
            // body_len separately and calculating remaining bytes, just try to
            // read 32 bytes and ignore any failures. (The caller will log and
            // ignore any trailing bytes.)
            data: reader.read_32_bytes().ok(),
        })
    }

    fn read_p2p_v2_upgrade<R: Read>(
        &self,
        mut reader: R,
        body_len: usize,
    ) -> Result<Message, Error> {
        if body_len > crate::zakura::MAX_PRELUDE_PAYLOAD_BYTES {
            return Err(Error::Parse(
                "Zakura p2pv2up payload exceeds the hard prelude cap",
            ));
        }

        let mut payload = vec![0; body_len];
        reader.read_exact(&mut payload)?;
        Ok(Message::P2pV2Upgrade(payload))
    }

    /// Deserialize an `addr` (v1) message into a list of `MetaAddr`s.
    pub(super) fn read_addr<R: Read>(&self, reader: R) -> Result<Message, Error> {
        let addrs: Vec<AddrV1> = reader.zcash_deserialize_into()?;

        if addrs.len() > constants::MAX_ADDRS_IN_MESSAGE {
            return Err(Error::Parse(
                "more than MAX_ADDRS_IN_MESSAGE in addr message",
            ));
        }

        // Convert the received address format to Zebra's internal `MetaAddr`.
        let addrs = addrs.into_iter().map(Into::into).collect();
        Ok(Message::Addr(addrs))
    }

    /// Deserialize an `addrv2` message into a list of `MetaAddr`s.
    ///
    /// Currently, Zebra parses received `addrv2`s, ignoring some address types.
    /// Zebra never sends `addrv2` messages.
    pub(super) fn read_addrv2<R: Read>(&self, reader: R) -> Result<Message, Error> {
        let addrs: Vec<AddrV2> = reader.zcash_deserialize_into()?;

        if addrs.len() > constants::MAX_ADDRS_IN_MESSAGE {
            return Err(Error::Parse(
                "more than MAX_ADDRS_IN_MESSAGE in addrv2 message",
            ));
        }

        // Convert the received address format to Zebra's internal `MetaAddr`,
        // ignoring unsupported network IDs.
        let addrs = addrs
            .into_iter()
            .filter_map(|addr| addr.try_into().ok())
            .collect();
        Ok(Message::Addr(addrs))
    }

    fn read_getaddr<R: Read>(&self, mut _reader: R) -> Result<Message, Error> {
        Ok(Message::GetAddr)
    }

    fn read_block(&self, reader: &mut &[u8]) -> Result<Message, Error> {
        let result = self.deserialize_block_spawning(reader);
        let block: std::sync::Arc<Block> = result?.into();

        #[cfg(test)]
        self.panic_at_body_decode_point(BodyDecodePanic::AfterBlockConversion);

        Ok(Message::Block(block.into()))
    }

    fn read_getblocks<R: Read>(&self, mut reader: R) -> Result<Message, Error> {
        if self.builder.version == Version(reader.read_u32::<LittleEndian>()?) {
            let known_blocks = Vec::zcash_deserialize(&mut reader)?;
            let stop_hash = block::Hash::zcash_deserialize(&mut reader)?;
            let stop = if stop_hash != block::Hash([0; 32]) {
                Some(stop_hash)
            } else {
                None
            };
            Ok(Message::GetBlocks { known_blocks, stop })
        } else {
            Err(Error::Parse("getblocks version did not match negotiation"))
        }
    }

    /// Deserialize a `headers` message.
    ///
    /// See [Zcash block header] for the enumeration of these fields.
    ///
    /// [Zcash block header](https://zips.z.cash/protocol/protocol.pdf#page=84)
    fn read_headers(&self, bytes: &mut &[u8]) -> Result<Message, Error> {
        let mut reader = ZcashReader::from_slice(bytes);
        // CompactSizeMessage is bounded to MAX_PROTOCOL_MESSAGE_LEN on deserialization.
        let count: CompactSizeMessage = reader.read_value()?;
        // Infallible: CompactSizeMessage wraps u32, which always fits in usize.
        let count: usize = count.into();
        if count > MAX_HEADERS_PER_MESSAGE {
            return Err(Error::Parse(
                "headers message exceeds the protocol limit of 160 entries",
            ));
        }
        Ok(Message::Headers(reader.read_external_count(count)?))
    }

    fn read_getheaders<R: Read>(&self, mut reader: R) -> Result<Message, Error> {
        if self.builder.version == Version(reader.read_u32::<LittleEndian>()?) {
            let known_blocks = Vec::zcash_deserialize(&mut reader)?;
            let stop_hash = block::Hash::zcash_deserialize(&mut reader)?;
            let stop = if stop_hash != block::Hash([0; 32]) {
                Some(stop_hash)
            } else {
                None
            };
            Ok(Message::GetHeaders { known_blocks, stop })
        } else {
            Err(Error::Parse("getblocks version did not match negotiation"))
        }
    }

    fn read_inv<R: Read>(&self, reader: R) -> Result<Message, Error> {
        Ok(Message::Inv(Vec::zcash_deserialize(reader)?))
    }

    fn read_getdata<R: Read>(&self, reader: R) -> Result<Message, Error> {
        Ok(Message::GetData(Vec::zcash_deserialize(reader)?))
    }

    fn read_notfound<R: Read>(&self, reader: R) -> Result<Message, Error> {
        Ok(Message::NotFound(Vec::zcash_deserialize(reader)?))
    }

    fn read_tx(&self, reader: &mut &[u8]) -> Result<Message, Error> {
        let result = self.deserialize_transaction_spawning(reader);
        let transaction = result?.into();

        #[cfg(test)]
        self.panic_at_body_decode_point(BodyDecodePanic::AfterTransactionConversion);

        Ok(Message::Tx(transaction))
    }

    fn read_mempool<R: Read>(&self, mut _reader: R) -> Result<Message, Error> {
        Ok(Message::Mempool)
    }

    #[cfg(test)]
    fn inject_body_decode_panic(&self, panic_point: BodyDecodePanic) {
        self.body_decode_panic.set(Some(panic_point));
    }

    #[cfg(test)]
    fn panic_at_body_decode_point(&self, panic_point: BodyDecodePanic) {
        if self.body_decode_panic.get() == Some(panic_point) {
            self.body_decode_panic.set(None);
            panic!("injected legacy message body parser panic at {panic_point:?}");
        }
    }

    /// Given the reader, deserialize the transaction in the rayon thread pool.
    #[allow(clippy::unwrap_in_result)]
    fn deserialize_transaction_spawning(&self, reader: &mut &[u8]) -> Result<Transaction, Error> {
        let mut result = None;

        // Correctness: Do CPU-intensive work on a dedicated thread, to avoid blocking other futures.
        //
        // Since we use `block_in_place()`, other futures running on the connection task will be blocked:
        // https://docs.rs/tokio/latest/tokio/task/fn.block_in_place.html
        //
        // We can't use `spawn_blocking()` because:
        // - The `reader` has a lifetime (but we could replace it with a `Vec` of message data)
        // - There is no way to check the blocking task's future for panics
        tokio::task::block_in_place(|| {
            rayon::in_place_scope_fifo(|s| {
                s.spawn_fifo(|_s| result = Some(Transaction::zcash_deserialize_bounded(reader)))
            })
        });

        result.expect("scope has already finished")
    }

    /// Given the reader, deserialize the block in the rayon thread pool.
    #[allow(clippy::unwrap_in_result)]
    fn deserialize_block_spawning(&self, reader: &mut &[u8]) -> Result<Block, Error> {
        let mut result = None;

        // Correctness: Do CPU-intensive work on a dedicated thread, to avoid blocking other futures.
        //
        // Since we use `block_in_place()`, other futures running on the connection task will be blocked:
        // https://docs.rs/tokio/latest/tokio/task/fn.block_in_place.html
        //
        // We can't use `spawn_blocking()` because:
        // - The `reader` has a lifetime (but we could replace it with a `Vec` of message data)
        // - There is no way to check the blocking task's future for panics
        tokio::task::block_in_place(|| {
            rayon::in_place_scope_fifo(|s| {
                s.spawn_fifo(|_s| result = Some(Block::zcash_deserialize_bounded(reader)))
            })
        });

        result.expect("scope has already finished")
    }
}

/// Benchmark access to the two ways a `block` message body's checksum is obtained, used to
/// choose [`MIN_SHARED_CHECKSUM_BODY_LEN`]. Not part of the production API.
#[cfg(feature = "internal-bench")]
pub mod checksum_bench {
    use super::*;

    /// The smallest `block` message body whose checksum is shared between connections.
    pub const MIN_SHARED_BODY_LEN: usize = MIN_SHARED_CHECKSUM_BODY_LEN;

    /// Hashes `body` the way every unshared message body is hashed.
    pub fn hash_body(body: &[u8]) -> sha256d::Checksum {
        message_checksum(body)
    }

    /// A full shared checksum cache that remembers `block`'s checksum.
    pub struct CachedBlock {
        checksums: BlockChecksums,
    }

    impl CachedBlock {
        /// Fills a cache to capacity with other blocks, then remembers `block`'s checksum.
        pub fn new(block: &Block, body_len: usize) -> Self {
            let checksums = BlockChecksums::default();
            for i in 0..SHARED_CHECKSUM_BLOCKS {
                let mut other = [0; 32];
                // Widening usize to u64 is lossless on supported targets.
                other[..8].copy_from_slice(&(i as u64).to_le_bytes());
                checksums.checksum_with(block::Hash(other), body_len, || sha256d::Checksum([0; 4]));
            }
            checksums.checksum_with(block.hash(), body_len, || sha256d::Checksum([1; 4]));
            Self { checksums }
        }

        /// Finds the checksum using the hash already carried by an outgoing block message.
        pub fn lookup(&self, hash: block::Hash) -> sha256d::Checksum {
            self.checksums
                .get(hash)
                .expect("the benchmark block's checksum is remembered")
                .1
        }
    }
}
