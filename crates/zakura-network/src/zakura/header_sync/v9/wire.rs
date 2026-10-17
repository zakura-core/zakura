//! The version 9 message table and codec.
//!
//! Header bytes differ by network, so the codec is generic over
//! `HEADER_BYTES`: [`CommonMessage`] serves Mainnet and Testnet, and
//! [`RegtestMessage`] serves Regtest. Each has its own `Headers` row.
//!
//! Decoding checks the rules that need only the payload and the network. The
//! subscriber checks the rest after the reservation match.

use std::{mem::size_of, sync::Arc, time::Duration};

use zakura_chain::{
    block,
    serialization::{ZcashDeserialize, ZcashSerialize},
    work::difficulty::U256,
};

use super::super::{
    config::{COMMON_HEADER_BYTES, REGTEST_HEADER_BYTES},
    wire::{
        AuxSchema, HeaderEntry, TreeAuxRecordV1, TreeAuxWire, KNOWN_TREE_AUX_SCHEMA_MASK,
        MAX_HS_MESSAGE_BYTES, MAX_HS_RANGE, TREE_AUX_SCHEMA_V1_BYTES, ZAKURA_STREAM_HEADER_SYNC,
    },
};
use crate::zakura::{
    wire_codec::{
        BlockHash, Bool, BoundedReader, HeightLe, LeU16, LeU32, LeU64, List, Wire, WireError,
        WireMessage, U8,
    },
    Cadence, Credit, MessageRole, MessageRule, PayloadLen, Stream, FRAME_HEADER_BYTES,
};

/// The header sync stream version this module encodes.
pub(crate) const STREAM_VERSION: u16 = 9;

/// Capability bit 10, which identifies version 9.
pub(crate) const CAPABILITY: u64 = 1 << 10;

/// Most header credit a subscription holds beyond its acknowledged cursor.
pub(crate) const MAX_HS_PUSH_CREDIT_HEADERS: u32 = 4_000;

/// Most byte credit a subscription holds beyond its acknowledged cursor.
pub(crate) const MAX_HS_PUSH_CREDIT_BYTES: u32 = 8 * 1024 * 1024;

/// Live or closing subscriptions a subscriber holds per peer. `Status`
/// advertises exactly this value.
pub(crate) const MAX_HS_SUBSCRIPTIONS: u16 = 1;

/// Sent cursors a publisher remembers per subscription. It covers
/// [`MAX_HS_PUSH_CREDIT_HEADERS`], because every page spends a header.
pub(crate) const HS_SENT_CURSOR_RING: u32 = 4_096;

/// Bytes of a `Headers` page before its entries.
pub(crate) const HEADERS_RESPONSE_FIXED_BYTES: usize = <PageFixed as Wire>::MAX_LEN;

/// Most locator hashes an `Open` carries.
pub(crate) const MAX_LOCATOR_HASHES: usize = 13;

/// Bytes of one body-size hint.
const BODY_SIZE_BYTES: usize = 4;

/// The frame cap of the version 9 stream: the version 8 cap, which every
/// network's largest page fits.
// The sum is a small protocol constant, far below u32::MAX.
const FRAME_CAP: u32 = (MAX_HS_MESSAGE_BYTES + FRAME_HEADER_BYTES) as u32;

/// Frame message types.
mod message_type {
    pub(crate) const STATUS: u16 = 1;
    pub(crate) const SUBSCRIBE_HEADERS: u16 = 2;
    pub(crate) const HEADERS: u16 = 3;
    pub(crate) const HEADERS_OUTCOME: u16 = 4;
}

/// Cumulative work: a 256-bit little-endian integer.
#[derive(Debug)]
pub(crate) enum Work {}

impl Wire for Work {
    type Value = U256;
    const MIN_LEN: usize = 32;
    const MAX_LEN: usize = 32;

    fn max_heap_bytes(_input_len: usize) -> usize {
        0
    }

    fn encode(value: &U256, out: &mut Vec<u8>) -> Result<(), WireError> {
        out.extend_from_slice(&value.to_little_endian());
        Ok(())
    }

    fn decode(reader: &mut BoundedReader<'_>) -> Result<U256, WireError> {
        Ok(U256::from_little_endian(&reader.array::<32>("work")?))
    }
}

/// `Status`: the work anchor, the selected tip, the suffix work, the retention
/// floor, and the serving caps.
pub(crate) type StatusPayload = (
    (HeightLe, BlockHash),
    (HeightLe, BlockHash),
    Work,
    HeightLe,
    StatusCaps,
);

/// Maximum headers per response, maximum subscriptions, maximum message bytes,
/// and the auxiliary-schema mask.
pub(crate) type StatusCaps = (LeU32, LeU16, LeU32, LeU32);

/// `SubscribeHeaders`: the operation, subscription ID, update sequence,
/// initial target, acknowledged cursor, locators, added header and byte
/// credit, and auxiliary schema.
pub(crate) type UpdatePayload = (U8, LeU64, LeU32, BlockHash, CursorPayload, UpdateTail);

/// A cursor's height, hash, and cumulative header and byte counts.
pub(crate) type CursorPayload = (HeightLe, BlockHash, LeU64, LeU64);

/// The locators, the added header and byte credit, and the auxiliary schema.
pub(crate) type UpdateTail = (Locators, LeU32, LeU32, U8);

/// Up to 13 locator hashes. A CompactSize count below `0xfd` is one byte, so
/// this is the specification's locator-count byte followed by the hashes.
pub(crate) type Locators = List<BlockHash, 0, MAX_LOCATOR_HASHES>;

/// A `Headers` page's fixed fields: the subscription ID, initial target,
/// parent height and hash, header count, target marker, and auxiliary schema.
/// The entries follow in three parallel sections: headers, body-size hints,
/// and auxiliary records.
pub(crate) type PageFixed = (LeU64, BlockHash, HeightLe, BlockHash, LeU32, (Bool, U8));

/// `HeadersOutcome`: the subscription ID, initial target, and outcome code.
pub(crate) type OutcomePayload = (LeU64, BlockHash, U8);

/// Each side's frontier and serving caps, sent at most once a second.
const STATUS: MessageRule = MessageRule {
    message_type: message_type::STATUS,
    payload: PayloadLen::of::<StatusPayload>(),
    role: MessageRole::Announcement {
        cadence: Cadence {
            capacity: 602,
            refill_interval: Duration::from_millis(500),
            send_interval: Duration::from_secs(1),
        },
    },
};

/// Opens, renews, and closes a subscription.
pub(crate) const SUBSCRIBE_HEADERS: MessageRule = MessageRule {
    message_type: message_type::SUBSCRIBE_HEADERS,
    payload: PayloadLen::of::<UpdatePayload>(),
    role: MessageRole::Subscription {
        // Widening u16 to u32 is lossless.
        max_live: MAX_HS_SUBSCRIPTIONS as u32,
        credit: Credit {
            objects: MAX_HS_PUSH_CREDIT_HEADERS,
            bytes: MAX_HS_PUSH_CREDIT_BYTES,
        },
        cursor_history: HS_SENT_CURSOR_RING,
        cadence: None,
    },
};

/// Ends a subscription. It spends no credit.
const HEADERS_OUTCOME: MessageRule = MessageRule {
    message_type: message_type::HEADERS_OUTCOME,
    payload: PayloadLen::of::<OutcomePayload>(),
    role: MessageRole::Response {
        request: SUBSCRIBE_HEADERS.message_type,
        ends_exchange: true,
    },
};

/// One pushed page of headers, on a network whose headers take
/// `header_bytes`. Pages do not end the subscription.
const fn headers_row(header_bytes: usize) -> MessageRule {
    MessageRule {
        message_type: message_type::HEADERS,
        payload: PayloadLen::between(
            smallest_page(header_bytes, AuxSchema::None),
            largest_page(header_bytes),
        ),
        role: MessageRole::Response {
            request: SUBSCRIBE_HEADERS.message_type,
            ends_exchange: false,
        },
    }
}

/// Bytes of one page entry: a header, its body-size hint, and its auxiliary
/// record under `schema`.
pub(crate) const fn entry_bytes(header_bytes: usize, schema: AuxSchema) -> usize {
    header_bytes
        + BODY_SIZE_BYTES
        + match schema {
            AuxSchema::None => 0,
            AuxSchema::V1 => TREE_AUX_SCHEMA_V1_BYTES,
        }
}

/// Bytes of a one-header page under `schema`. Without auxiliary records, this
/// is the specification's `P`, the smallest legal page on the network.
pub(crate) const fn smallest_page(header_bytes: usize, schema: AuxSchema) -> usize {
    HEADERS_RESPONSE_FIXED_BYTES + entry_bytes(header_bytes, schema)
}

/// The `Headers` row's maximum: the largest page under either schema that
/// fits [`MAX_HS_MESSAGE_BYTES`] and [`MAX_HS_RANGE`] headers.
pub(crate) const fn largest_page(header_bytes: usize) -> usize {
    let plain = largest_page_under(header_bytes, AuxSchema::None);
    let with_records = largest_page_under(header_bytes, AuxSchema::V1);
    if plain > with_records {
        plain
    } else {
        with_records
    }
}

/// The largest page under `schema` that fits [`MAX_HS_MESSAGE_BYTES`] and
/// [`MAX_HS_RANGE`] headers.
const fn largest_page_under(header_bytes: usize, schema: AuxSchema) -> usize {
    let entry = entry_bytes(header_bytes, schema);
    let fitting = (MAX_HS_MESSAGE_BYTES - HEADERS_RESPONSE_FIXED_BYTES) / entry;
    // Widening u32 to usize is lossless on every supported target.
    let count = if fitting < MAX_HS_RANGE as usize {
        fitting
    } else {
        MAX_HS_RANGE as usize
    };
    HEADERS_RESPONSE_FIXED_BYTES + count * entry
}

/// Mainnet and Testnet messages, whose headers take 1,487 bytes.
pub(crate) type CommonMessage = Message<COMMON_HEADER_BYTES>;

/// Regtest messages, whose headers take 177 bytes.
pub(crate) type RegtestMessage = Message<REGTEST_HEADER_BYTES>;

const _: () = Stream::validate_layout(&CommonMessage::LAYOUT);
const _: () = Stream::validate_layout(&RegtestMessage::LAYOUT);

/// The version 9 layout for `network`, chosen by its header size.
pub(crate) fn layout(network: &zakura_chain::parameters::Network) -> &'static [Stream] {
    if super::super::header_sync_header_bytes_for_network(network) == REGTEST_HEADER_BYTES {
        &RegtestMessage::LAYOUT
    } else {
        &CommonMessage::LAYOUT
    }
}

/// A `Status` advertisement: the sender's frontier and serving caps.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Status {
    /// Height of the sender's finality anchor, excluded from the suffix work.
    pub(crate) work_anchor_height: block::Height,
    /// Hash of the work anchor.
    pub(crate) work_anchor_hash: block::Hash,
    /// Height of the sender's selected header tip.
    pub(crate) selected_tip_height: block::Height,
    /// Hash of the sender's selected header tip.
    pub(crate) selected_tip_hash: block::Hash,
    /// Work after the anchor through the selected tip.
    pub(crate) suffix_cumulative_work: U256,
    /// Lowest height for which the sender retains headers.
    pub(crate) oldest_retained_height: block::Height,
    /// Most headers the sender puts in one page, `1..=MAX_HS_RANGE`.
    pub(crate) max_headers_per_response: u32,
    /// Live subscriptions the sender holds per peer, always
    /// [`MAX_HS_SUBSCRIPTIONS`].
    pub(crate) max_subscriptions: u16,
    /// Most bytes in one of the sender's pages. It fits a one-header page
    /// under every advertised schema and stays within 2 MiB.
    pub(crate) max_message_bytes: u32,
    /// Bit `n - 1` advertises auxiliary schema `n`.
    pub(crate) tree_aux_schema_mask: u32,
}

/// A `SubscribeHeaders` operation.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum Operation {
    /// Opens a subscription with its locators and first credit.
    Open = 0,
    /// Acknowledges a cursor and adds credit.
    Grant = 1,
    /// Stops new pages. The publisher ends the subscription after the pages
    /// it already queued.
    Close = 2,
}

/// A subscription cursor.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct Cursor {
    /// Height of the last accepted header, or of the first locator.
    pub(crate) height: block::Height,
    /// Hash of that header.
    pub(crate) hash: block::Hash,
    /// Header credit the subscription's pages consumed through this cursor.
    pub(crate) headers: u64,
    /// Byte credit the subscription's pages consumed through this cursor.
    pub(crate) bytes: u64,
}

/// One subscription update.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SubscribeHeaders {
    /// What the update does.
    pub(crate) operation: Operation,
    /// Nonzero subscription identifier.
    pub(crate) subscription_id: u64,
    /// Zero for `Open`, then one more for each update.
    pub(crate) update_sequence: u32,
    /// The target the subscription's first pages reach.
    pub(crate) initial_target_tip_hash: block::Hash,
    /// The start cursor for `Open`, then the last cursor the subscriber
    /// accepted.
    pub(crate) acknowledged: Cursor,
    /// `1..=13` unique locator hashes for `Open`, none otherwise.
    pub(crate) locator_hashes: Vec<block::Hash>,
    /// Header credit (`objects`) and byte credit the update adds.
    pub(crate) added_credit: Credit,
    /// The auxiliary schema the subscriber asks for.
    pub(crate) tree_aux_schema: AuxSchema,
}

/// One pushed page of headers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Headers {
    /// Nonzero subscription identifier.
    pub(crate) subscription_id: u64,
    /// The subscription's initial target.
    pub(crate) initial_target_tip_hash: block::Hash,
    /// Height of the header the page's first header extends.
    pub(crate) common_ancestor_height: block::Height,
    /// Hash of the header the page's first header extends.
    pub(crate) common_ancestor_hash: block::Hash,
    /// Whether the page's last header is the initial target.
    pub(crate) reaches_initial_target: bool,
    /// The auxiliary schema of every entry.
    pub(crate) tree_aux_schema: AuxSchema,
    /// `1..=MAX_HS_RANGE` headers in ascending order, with their hints and
    /// records.
    pub(crate) entries: Vec<HeaderEntry>,
}

/// Why a subscription ended.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum HeadersOutcomeCode {
    /// The publisher no longer retains the initial target.
    TargetNotRetained = 1,
    /// No locator hash lies on the target path.
    NoLocatorIntersection = 2,
    /// The publisher pruned history on the target path.
    HistoryPruned = 3,
    /// The publisher's selected chain changed between `Status` and `Open`.
    TargetNotSelected = 5,
    /// The publisher's selected chain stopped extending the cursor.
    SubscriptionSuperseded = 6,
    /// The publisher received `Close`.
    SubscriptionClosed = 7,
}

impl HeadersOutcomeCode {
    /// The code for a wire byte, if version 9 defines one.
    fn from_wire(value: u8) -> Option<Self> {
        Some(match value {
            1 => Self::TargetNotRetained,
            2 => Self::NoLocatorIntersection,
            3 => Self::HistoryPruned,
            5 => Self::TargetNotSelected,
            6 => Self::SubscriptionSuperseded,
            7 => Self::SubscriptionClosed,
            _ => return None,
        })
    }
}

/// A subscription's terminal outcome.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct HeadersOutcome {
    /// Nonzero subscription identifier.
    pub(crate) subscription_id: u64,
    /// The subscription's initial target.
    pub(crate) initial_target_tip_hash: block::Hash,
    /// Why the subscription ended.
    pub(crate) outcome: HeadersOutcomeCode,
}

/// Every version 9 message on a network whose headers take `HEADER_BYTES`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Message<const HEADER_BYTES: usize> {
    Status(Status),
    SubscribeHeaders(SubscribeHeaders),
    Headers(Headers),
    HeadersOutcome(HeadersOutcome),
}

impl<const HEADER_BYTES: usize> Message<HEADER_BYTES> {
    /// The rows of this network's version 9 stream.
    pub(crate) const ROWS: [MessageRule; 4] = [
        STATUS,
        SUBSCRIBE_HEADERS,
        headers_row(HEADER_BYTES),
        HEADERS_OUTCOME,
    ];

    /// This network's version 9 stream. Activation registers it in place of
    /// the version 8 stream.
    pub(crate) const LAYOUT: [Stream; 1] = [Stream {
        kind: ZAKURA_STREAM_HEADER_SYNC,
        version: STREAM_VERSION,
        frame_cap: FRAME_CAP,
        capability: CAPABILITY,
        messages: Some(&Self::ROWS),
        ..Stream::PERSISTENT
    }];

    /// Check the rules that the payload items cannot express.
    fn check(&self) -> Result<(), WireError> {
        match self {
            Self::Status(status) => Self::check_status(status),
            Self::SubscribeHeaders(update) => Self::check_update(update),
            Self::Headers(page) => Self::check_page(page),
            Self::HeadersOutcome(outcome) => check_subscription_id(outcome.subscription_id),
        }
    }

    /// Check a `Status` against the protocol limits and its own heights.
    fn check_status(status: &Status) -> Result<(), WireError> {
        if status.work_anchor_height > status.selected_tip_height {
            return Err(WireError::OutOfRange("status work anchor"));
        }
        if status.oldest_retained_height > status.selected_tip_height {
            return Err(WireError::OutOfRange("status retention floor"));
        }
        if !(1..=MAX_HS_RANGE).contains(&status.max_headers_per_response) {
            return Err(WireError::OutOfRange("status header cap"));
        }
        if status.max_subscriptions != MAX_HS_SUBSCRIPTIONS {
            return Err(WireError::OutOfRange("status subscription cap"));
        }
        if status.tree_aux_schema_mask & !KNOWN_TREE_AUX_SCHEMA_MASK != 0 {
            return Err(WireError::OutOfRange("status schema mask"));
        }
        // V1 is the only known schema, and the largest.
        let largest_schema = if status.tree_aux_schema_mask & AuxSchema::V1.mask_bit() != 0 {
            AuxSchema::V1
        } else {
            AuxSchema::None
        };
        let smallest = smallest_page(HEADER_BYTES, largest_schema);
        // Widening u32 to usize is lossless on every supported target.
        if !(smallest..=MAX_HS_MESSAGE_BYTES).contains(&(status.max_message_bytes as usize)) {
            return Err(WireError::OutOfRange("status message cap"));
        }
        Ok(())
    }

    /// Check an update's shape: the rules for its operation, and the credit
    /// limits that every update shares. The publisher takes an `Open`'s start
    /// height as given.
    fn check_update(update: &SubscribeHeaders) -> Result<(), WireError> {
        check_subscription_id(update.subscription_id)?;
        let added = update.added_credit;
        if added.objects > MAX_HS_PUSH_CREDIT_HEADERS || added.bytes > MAX_HS_PUSH_CREDIT_BYTES {
            return Err(WireError::OutOfRange("update credit"));
        }
        // Widening u32 to usize is lossless on every supported target.
        let added_bytes = added.bytes as usize;
        match update.operation {
            Operation::Open => {
                if update.update_sequence != 0 {
                    return Err(WireError::OutOfRange("update sequence"));
                }
                let locators = &update.locator_hashes;
                let Some(first) = locators.first() else {
                    return Err(WireError::OutOfRange("locator count"));
                };
                // At most 13 locators, so the pairwise check stays small.
                if locators
                    .iter()
                    .enumerate()
                    .any(|(index, hash)| locators[..index].contains(hash))
                {
                    return Err(WireError::OutOfRange("locator hashes"));
                }
                let cursor = update.acknowledged;
                if cursor.hash != *first || cursor.headers != 0 || cursor.bytes != 0 {
                    return Err(WireError::OutOfRange("open cursor"));
                }
                if added.objects == 0
                    || added_bytes < smallest_page(HEADER_BYTES, update.tree_aux_schema)
                {
                    return Err(WireError::OutOfRange("open credit"));
                }
            }
            Operation::Grant | Operation::Close => {
                if update.update_sequence == 0 {
                    return Err(WireError::OutOfRange("update sequence"));
                }
                if !update.locator_hashes.is_empty() {
                    return Err(WireError::OutOfRange("update locators"));
                }
                if update.operation == Operation::Grant
                    && added.objects == 0
                    && added_bytes < smallest_page(HEADER_BYTES, AuxSchema::None)
                {
                    return Err(WireError::OutOfRange("grant minimum"));
                }
                if update.operation == Operation::Close && (added.objects != 0 || added.bytes != 0)
                {
                    return Err(WireError::OutOfRange("close credit"));
                }
            }
        }
        Ok(())
    }

    /// Check a page's fields and entries, but not its headers' encodings,
    /// which only encoding and decoding see.
    fn check_page(page: &Headers) -> Result<(), WireError> {
        check_subscription_id(page.subscription_id)?;
        let count = page.entries.len();
        check_page_heights(page.common_ancestor_height, count)?;
        let records = page.tree_aux_schema == AuxSchema::V1;
        for entry in &page.entries {
            check_body_size(entry.body_size)?;
            if entry.tree_aux.is_some() != records {
                return Err(WireError::OutOfRange("tree-aux records"));
            }
        }
        Ok(())
    }

    /// Append a page: its fixed fields, then its headers, hints, and records.
    fn encode_page(page: &Headers, out: &mut Vec<u8>) -> Result<(), WireError> {
        PageFixed::encode(
            &(
                page.subscription_id,
                page.initial_target_tip_hash,
                page.common_ancestor_height,
                page.common_ancestor_hash,
                u32::try_from(page.entries.len())
                    .map_err(|_| WireError::OutOfRange("header count"))?,
                (
                    page.reaches_initial_target,
                    page.tree_aux_schema.wire_value(),
                ),
            ),
            out,
        )?;
        for entry in &page.entries {
            let start = out.len();
            entry
                .header
                .zcash_serialize(&mut *out)
                .map_err(|_| WireError::OutOfRange("block header"))?;
            if out.len() - start != HEADER_BYTES {
                return Err(WireError::OutOfRange("header solution size"));
            }
        }
        for entry in &page.entries {
            LeU32::encode(&entry.body_size, out)?;
        }
        for record in page.entries.iter().filter_map(|entry| entry.tree_aux) {
            record
                .encode_to(out)
                .map_err(|_| WireError::OutOfRange("tree-aux record"))?;
        }
        Ok(())
    }

    /// Decode a page's entries after checking that the payload holds exactly
    /// `count` of them, so the entry buffer never outgrows the payload.
    fn decode_page(reader: &mut BoundedReader<'_>) -> Result<Headers, WireError> {
        let (
            subscription_id,
            initial_target_tip_hash,
            common_ancestor_height,
            common_ancestor_hash,
            count,
            (reaches_initial_target, schema),
        ) = reader.read::<PageFixed>()?;
        check_subscription_id(subscription_id)?;
        let tree_aux_schema = schema_from_wire(schema)?;
        // Widening u32 to usize is lossless on every supported target.
        let count = count as usize;
        check_page_heights(common_ancestor_height, count)?;
        let sections = reader.take_remaining();
        // At most MAX_HS_RANGE entries of a few kilobytes each, far below usize::MAX.
        if sections.len() != count * entry_bytes(HEADER_BYTES, tree_aux_schema) {
            return Err(WireError::OutOfRange("header entries"));
        }
        let (headers, rest) = sections.split_at(count * HEADER_BYTES);
        let (hints, records) = rest.split_at(count * BODY_SIZE_BYTES);

        let mut entries = Vec::with_capacity(count);
        for header in headers.as_chunks::<HEADER_BYTES>().0 {
            let mut bytes = &header[..];
            let header = block::Header::zcash_deserialize_from_slice(&mut bytes)
                .map_err(|_| WireError::OutOfRange("block header"))?;
            // Every network has one solution size, so its headers have one length.
            if !bytes.is_empty() {
                return Err(WireError::OutOfRange("header solution size"));
            }
            entries.push(HeaderEntry {
                header: Arc::new(header),
                body_size: 0,
                tree_aux: None,
            });
        }
        for (entry, hint) in entries
            .iter_mut()
            .zip(hints.as_chunks::<BODY_SIZE_BYTES>().0)
        {
            entry.body_size = u32::from_le_bytes(*hint);
            check_body_size(entry.body_size)?;
        }
        if tree_aux_schema == AuxSchema::V1 {
            let records = records.as_chunks::<TREE_AUX_SCHEMA_V1_BYTES>().0;
            for (entry, record) in entries.iter_mut().zip(records) {
                entry.tree_aux = Some(
                    TreeAuxRecordV1::decode_from(&mut &record[..])
                        .map_err(|_| WireError::OutOfRange("tree-aux record"))?,
                );
            }
        }
        Ok(Headers {
            subscription_id,
            initial_target_tip_hash,
            common_ancestor_height,
            common_ancestor_hash,
            reaches_initial_target,
            tree_aux_schema,
            entries,
        })
    }

    /// Most heap that decoding a `payload_len`-byte page requests: the entry
    /// buffer, one shared header per entry, and one solution buffer at a time.
    fn page_heap_bytes(payload_len: usize) -> usize {
        // Widening u32 to usize is lossless on every supported target.
        let count = (payload_len.saturating_sub(HEADERS_RESPONSE_FIXED_BYTES)
            / entry_bytes(HEADER_BYTES, AuxSchema::None))
        .min(MAX_HS_RANGE as usize);
        // An `Arc` adds two counters and at most one word of padding.
        let shared_header = size_of::<block::Header>() + 3 * size_of::<usize>();
        count
            .saturating_mul(size_of::<HeaderEntry>() + shared_header)
            .saturating_add(HEADER_BYTES)
    }
}

impl<const HEADER_BYTES: usize> WireMessage for Message<HEADER_BYTES> {
    type Error = WireError;
    const RULES: &'static [MessageRule] = &Self::ROWS;

    fn message_type(&self) -> u16 {
        match self {
            Self::Status(_) => message_type::STATUS,
            Self::SubscribeHeaders(_) => message_type::SUBSCRIBE_HEADERS,
            Self::Headers(_) => message_type::HEADERS,
            Self::HeadersOutcome(_) => message_type::HEADERS_OUTCOME,
        }
    }

    fn encode_payload(&self, out: &mut Vec<u8>) -> Result<(), WireError> {
        self.check()?;
        match self {
            Self::Status(status) => StatusPayload::encode(
                &(
                    (status.work_anchor_height, status.work_anchor_hash),
                    (status.selected_tip_height, status.selected_tip_hash),
                    status.suffix_cumulative_work,
                    status.oldest_retained_height,
                    (
                        status.max_headers_per_response,
                        status.max_subscriptions,
                        status.max_message_bytes,
                        status.tree_aux_schema_mask,
                    ),
                ),
                out,
            ),
            // A fieldless enum's discriminant is its declared `u8` value.
            Self::SubscribeHeaders(update) => UpdatePayload::encode(
                &(
                    update.operation as u8,
                    update.subscription_id,
                    update.update_sequence,
                    update.initial_target_tip_hash,
                    (
                        update.acknowledged.height,
                        update.acknowledged.hash,
                        update.acknowledged.headers,
                        update.acknowledged.bytes,
                    ),
                    (
                        update.locator_hashes.clone(),
                        update.added_credit.objects,
                        update.added_credit.bytes,
                        update.tree_aux_schema.wire_value(),
                    ),
                ),
                out,
            ),
            Self::Headers(page) => Self::encode_page(page, out),
            // A fieldless enum's discriminant is its declared `u8` value.
            Self::HeadersOutcome(outcome) => OutcomePayload::encode(
                &(
                    outcome.subscription_id,
                    outcome.initial_target_tip_hash,
                    outcome.outcome as u8,
                ),
                out,
            ),
        }
    }

    fn decode_payload(
        message_type: u16,
        reader: &mut BoundedReader<'_>,
    ) -> Result<Self, WireError> {
        let message = match message_type {
            message_type::STATUS => {
                let (
                    (work_anchor_height, work_anchor_hash),
                    (selected_tip_height, selected_tip_hash),
                    suffix_cumulative_work,
                    oldest_retained_height,
                    (
                        max_headers_per_response,
                        max_subscriptions,
                        max_message_bytes,
                        tree_aux_schema_mask,
                    ),
                ) = reader.read::<StatusPayload>()?;
                Self::Status(Status {
                    work_anchor_height,
                    work_anchor_hash,
                    selected_tip_height,
                    selected_tip_hash,
                    suffix_cumulative_work,
                    oldest_retained_height,
                    max_headers_per_response,
                    max_subscriptions,
                    max_message_bytes,
                    tree_aux_schema_mask,
                })
            }
            message_type::SUBSCRIBE_HEADERS => {
                let (
                    operation,
                    subscription_id,
                    update_sequence,
                    initial_target_tip_hash,
                    (height, hash, headers, bytes),
                    (locator_hashes, objects, added_bytes, schema),
                ) = reader.read::<UpdatePayload>()?;
                let operation = match operation {
                    0 => Operation::Open,
                    1 => Operation::Grant,
                    2 => Operation::Close,
                    _ => return Err(WireError::OutOfRange("update operation")),
                };
                Self::SubscribeHeaders(SubscribeHeaders {
                    operation,
                    subscription_id,
                    update_sequence,
                    initial_target_tip_hash,
                    acknowledged: Cursor {
                        height,
                        hash,
                        headers,
                        bytes,
                    },
                    locator_hashes,
                    added_credit: Credit {
                        objects,
                        bytes: added_bytes,
                    },
                    tree_aux_schema: schema_from_wire(schema)?,
                })
            }
            message_type::HEADERS => Self::Headers(Self::decode_page(reader)?),
            message_type::HEADERS_OUTCOME => {
                let (subscription_id, initial_target_tip_hash, outcome) =
                    reader.read::<OutcomePayload>()?;
                Self::HeadersOutcome(HeadersOutcome {
                    subscription_id,
                    initial_target_tip_hash,
                    outcome: HeadersOutcomeCode::from_wire(outcome)
                        .ok_or(WireError::OutOfRange("headers outcome"))?,
                })
            }
            _ => return Err(WireError::UnknownMessageType(message_type)),
        };
        message.check()?;
        Ok(message)
    }

    fn max_heap_bytes(message_type: u16, payload_len: usize) -> usize {
        match message_type {
            message_type::STATUS => StatusPayload::max_heap_bytes(payload_len),
            message_type::SUBSCRIBE_HEADERS => UpdatePayload::max_heap_bytes(payload_len),
            message_type::HEADERS => Self::page_heap_bytes(payload_len),
            message_type::HEADERS_OUTCOME => OutcomePayload::max_heap_bytes(payload_len),
            _ => 0,
        }
    }
}

/// The auxiliary schema for a wire byte.
fn schema_from_wire(value: u8) -> Result<AuxSchema, WireError> {
    match value {
        0 => Ok(AuxSchema::None),
        1 => Ok(AuxSchema::V1),
        _ => Err(WireError::OutOfRange("tree-aux schema")),
    }
}

/// Reject the zero subscription ID, which no subscription uses.
fn check_subscription_id(subscription_id: u64) -> Result<(), WireError> {
    if subscription_id == 0 {
        return Err(WireError::OutOfRange("subscription id"));
    }
    Ok(())
}

/// Reject a body-size hint above the consensus block-size ceiling.
fn check_body_size(body_size: u32) -> Result<(), WireError> {
    if u64::from(body_size) > block::MAX_BLOCK_BYTES {
        return Err(WireError::OutOfRange("body size hint"));
    }
    Ok(())
}

/// Reject an empty or oversized page, or one whose last header would sit
/// above [`block::Height::MAX`].
fn check_page_heights(parent: block::Height, count: usize) -> Result<(), WireError> {
    // Widening u32 to usize is lossless on every supported target.
    if count == 0 || count > MAX_HS_RANGE as usize {
        return Err(WireError::OutOfRange("header count"));
    }
    let last = u32::try_from(count)
        .ok()
        .and_then(|count| parent.0.checked_add(count));
    if last.is_none_or(|last| last > block::Height::MAX.0) {
        return Err(WireError::OutOfRange("page heights"));
    }
    Ok(())
}
