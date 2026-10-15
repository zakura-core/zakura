//! The version 9 codec's tests.
//!
//! The generated item, message, and frame suites run on both header sizes.
//! Hand-written fixtures pin each layout independently of the encoder, and the
//! cap tests pin the fixed sizes and every network's page bounds.

use std::sync::Arc;

use chrono::{TimeZone, Utc};
use proptest::{collection::hash_set, prelude::*};
use zakura_chain::{
    block::{self, genesis::regtest_genesis_block, merkle::AuthDataRoot},
    ironwood, orchard,
    parameters::Network,
    sapling,
    serialization::{ZcashDeserialize, ZcashSerialize},
    work::{difficulty::U256, equihash::Solution},
};

use super::{replay_header_sync_v9_codec_bytes, wire::*};
use crate::zakura::{
    check_frame_filter,
    header_sync::{
        config::{COMMON_HEADER_BYTES, REGTEST_HEADER_BYTES},
        header_sync_header_bytes_for_network,
        wire::{
            AuxSchema, HeaderEntry, TreeAuxRecordV1, TreeAuxWire, MAX_HS_MESSAGE_BYTES,
            MAX_HS_RANGE,
        },
    },
    wire_codec::{
        encode_frame,
        item_suite::item_suite,
        message_suite::{
            check_layout_carries_family, decode_checked, message_suite, MessageSample, Violation,
        },
        sample::WireSample,
        BlockHash, HeightLe, WireError, WireMessage,
    },
    Cadence, Credit, Frame, MessageRole, MessageRule, PayloadLen,
};

/// The three networks. Mainnet and Testnet share a header size.
fn networks() -> [Network; 3] {
    [
        Network::Mainnet,
        Network::new_default_testnet(),
        Network::new_regtest(Default::default()),
    ]
}

/// Run `$check::<HEADER_BYTES>(&network)` on every network, with the family
/// for its header size.
macro_rules! on_every_network {
    ($check:ident) => {
        for network in networks() {
            match header_sync_header_bytes_for_network(&network) {
                COMMON_HEADER_BYTES => $check::<COMMON_HEADER_BYTES>(&network),
                REGTEST_HEADER_BYTES => $check::<REGTEST_HEADER_BYTES>(&network),
                other => panic!("no version 9 family has {other}-byte headers"),
            }
        }
    };
}

/// A hash of 32 copies of `byte`.
fn hash(byte: u8) -> block::Hash {
    block::Hash([byte; 32])
}

/// `P` on a network with `HEADER_BYTES`-byte headers.
fn p<const HEADER_BYTES: usize>() -> u32 {
    u32::try_from(smallest_page(HEADER_BYTES, AuxSchema::None)).expect("a page fits u32")
}

/// The genesis header of `network`.
fn genesis_header(network: &Network) -> Arc<block::Header> {
    let bytes: &[u8] = match network {
        Network::Mainnet => &zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES,
        _ if network.is_regtest() => return regtest_genesis_block().header.clone(),
        _ => &zakura_test::vectors::BLOCK_TESTNET_GENESIS_BYTES,
    };
    block::Block::zcash_deserialize(bytes)
        .expect("the genesis vector decodes")
        .header
}

/// A real header that encodes to `HEADER_BYTES`.
fn sample_header<const HEADER_BYTES: usize>() -> Arc<block::Header> {
    let header = if HEADER_BYTES == REGTEST_HEADER_BYTES {
        genesis_header(&Network::new_regtest(Default::default()))
    } else {
        genesis_header(&Network::Mainnet)
    };
    assert_eq!(header_bytes(&header).len(), HEADER_BYTES);
    header
}

/// The Zcash encoding of `header`.
fn header_bytes(header: &block::Header) -> Vec<u8> {
    header
        .zcash_serialize_to_vec()
        .expect("a valid header encodes")
}

/// A schema-1 record with every root and count at its empty default.
fn empty_aux(height: block::Height) -> TreeAuxRecordV1 {
    TreeAuxRecordV1 {
        height,
        sapling_root: sapling::tree::Root::default(),
        orchard_root: orchard::tree::NoteCommitmentTree::default().root(),
        ironwood_root: ironwood::tree::NoteCommitmentTree::default().root(),
        sapling_tx_count: 0,
        orchard_tx_count: 0,
        ironwood_tx_count: 0,
        auth_data_root: AuthDataRoot::from([0; 32]),
    }
}

/// A legal `Status` with every height and cap at its minimum.
fn smallest_status<const HEADER_BYTES: usize>() -> Status {
    Status {
        work_anchor_height: block::Height(0),
        work_anchor_hash: hash(0x11),
        selected_tip_height: block::Height(0),
        selected_tip_hash: hash(0x22),
        suffix_cumulative_work: U256::zero(),
        oldest_retained_height: block::Height(0),
        max_headers_per_response: 1,
        max_subscriptions: MAX_HS_SUBSCRIPTIONS,
        max_message_bytes: p::<HEADER_BYTES>(),
        tree_aux_schema_mask: 0,
    }
}

/// An `Open` with `locators` distinct locators, starting at the first.
fn open(locators: u8, added_credit: Credit, schema: AuxSchema) -> SubscribeHeaders {
    let locator_hashes: Vec<_> = (0..locators).map(|index| hash(0x40 + index)).collect();
    SubscribeHeaders {
        operation: Operation::Open,
        subscription_id: 7,
        update_sequence: 0,
        initial_target_tip_hash: hash(0x22),
        acknowledged: Cursor {
            height: block::Height(5),
            hash: locator_hashes[0],
            headers: 0,
            bytes: 0,
        },
        locator_hashes,
        added_credit,
        tree_aux_schema: schema,
    }
}

/// A `Grant` or `Close` with no locators.
fn update(operation: Operation, update_sequence: u32, added_credit: Credit) -> SubscribeHeaders {
    SubscribeHeaders {
        operation,
        subscription_id: 7,
        update_sequence,
        initial_target_tip_hash: hash(0x22),
        acknowledged: Cursor {
            height: block::Height(6),
            hash: hash(0x33),
            headers: 1,
            bytes: 1_573,
        },
        locator_hashes: Vec::new(),
        added_credit,
        tree_aux_schema: AuxSchema::None,
    }
}

/// Header and byte credit.
fn credit(objects: u32, bytes: u32) -> Credit {
    Credit { objects, bytes }
}

/// A page of `count` copies of a real header, with the largest body-size
/// hint and, under schema 1, an empty record each.
fn page<const HEADER_BYTES: usize>(count: u32, schema: AuxSchema) -> Headers {
    let header = sample_header::<HEADER_BYTES>();
    let record = empty_aux(block::Height(0));
    Headers {
        subscription_id: 7,
        initial_target_tip_hash: hash(0x22),
        common_ancestor_height: block::Height(10),
        common_ancestor_hash: hash(0x33),
        reaches_initial_target: false,
        tree_aux_schema: schema,
        entries: (1..=count)
            .map(|offset| HeaderEntry {
                header: header.clone(),
                body_size: 2_000_000,
                tree_aux: (schema == AuxSchema::V1).then_some(TreeAuxRecordV1 {
                    height: block::Height(10 + offset),
                    ..record
                }),
            })
            .collect(),
    }
}

/// The most headers a page holds under `schema`: the 2 MiB cap binds on
/// Mainnet and Testnet, and `MAX_HS_RANGE` binds on Regtest.
fn most_headers<const HEADER_BYTES: usize>(schema: AuxSchema) -> u32 {
    match (HEADER_BYTES, schema) {
        (COMMON_HEADER_BYTES, AuxSchema::None) => 1_406,
        (COMMON_HEADER_BYTES, AuxSchema::V1) => 1_273,
        (REGTEST_HEADER_BYTES, _) => 4_000,
        _ => unreachable!("no network has {HEADER_BYTES}-byte headers"),
    }
}

/// An outcome for the samples' initial target.
fn outcome(subscription_id: u64, code: HeadersOutcomeCode) -> HeadersOutcome {
    HeadersOutcome {
        subscription_id,
        initial_target_tip_hash: hash(0x22),
        outcome: code,
    }
}

const OUTCOME_CODES: [HeadersOutcomeCode; 6] = [
    HeadersOutcomeCode::TargetNotRetained,
    HeadersOutcomeCode::NoLocatorIntersection,
    HeadersOutcomeCode::HistoryPruned,
    HeadersOutcomeCode::TargetNotSelected,
    HeadersOutcomeCode::SubscriptionSuperseded,
    HeadersOutcomeCode::SubscriptionClosed,
];

impl WireSample for Work {
    fn boundary_values() -> Vec<U256> {
        vec![U256::zero(), U256::MAX]
    }

    fn arbitrary() -> BoxedStrategy<U256> {
        any::<[u8; 32]>()
            .prop_map(|bytes| U256::from_little_endian(&bytes))
            .boxed()
    }
}

item_suite!(work, Work);
item_suite!(locators, Locators);
item_suite!(status_payload, StatusPayload);
item_suite!(update_payload, UpdatePayload);
item_suite!(page_fixed_fields, PageFixed);
item_suite!(outcome_payload, OutcomePayload);

/// Valid statuses: heights in order, caps within the protocol limits.
fn arbitrary_status<const HEADER_BYTES: usize>() -> BoxedStrategy<Status> {
    let max_message_bytes = u32::try_from(MAX_HS_MESSAGE_BYTES).expect("2 MiB fits u32");
    (0..=block::Height::MAX.0, any::<bool>())
        .prop_flat_map(move |(tip, records)| {
            let schema = if records {
                AuxSchema::V1
            } else {
                AuxSchema::None
            };
            let smallest =
                u32::try_from(smallest_page(HEADER_BYTES, schema)).expect("a page fits u32");
            (
                (Just(tip), 0..=tip, 0..=tip),
                (
                    BlockHash::arbitrary(),
                    BlockHash::arbitrary(),
                    Work::arbitrary(),
                ),
                (
                    1..=MAX_HS_RANGE,
                    smallest..=max_message_bytes,
                    Just(u32::from(records)),
                ),
            )
        })
        .prop_map(
            |(
                (tip, anchor, retained),
                (work_anchor_hash, selected_tip_hash, suffix_cumulative_work),
                (max_headers_per_response, max_message_bytes, tree_aux_schema_mask),
            )| Status {
                work_anchor_height: block::Height(anchor),
                work_anchor_hash,
                selected_tip_height: block::Height(tip),
                selected_tip_hash,
                suffix_cumulative_work,
                oldest_retained_height: block::Height(retained),
                max_headers_per_response,
                max_subscriptions: MAX_HS_SUBSCRIPTIONS,
                max_message_bytes,
                tree_aux_schema_mask,
            },
        )
        .boxed()
}

/// Either known auxiliary schema.
fn arbitrary_schema() -> BoxedStrategy<AuxSchema> {
    prop_oneof![Just(AuxSchema::None), Just(AuxSchema::V1)].boxed()
}

/// Valid updates of every operation.
fn arbitrary_update<const HEADER_BYTES: usize>() -> BoxedStrategy<SubscribeHeaders> {
    let opens = arbitrary_schema()
        .prop_flat_map(|schema| {
            let smallest =
                u32::try_from(smallest_page(HEADER_BYTES, schema)).expect("a page fits u32");
            (
                (1..=u64::MAX, BlockHash::arbitrary(), HeightLe::arbitrary()),
                hash_set(any::<[u8; 32]>(), 1..=13),
                (
                    1..=MAX_HS_PUSH_CREDIT_HEADERS,
                    smallest..=MAX_HS_PUSH_CREDIT_BYTES,
                    Just(schema),
                ),
            )
        })
        .prop_map(
            |((subscription_id, target, height), locators, (objects, bytes, schema))| {
                let locator_hashes: Vec<_> = locators.into_iter().map(block::Hash).collect();
                SubscribeHeaders {
                    operation: Operation::Open,
                    subscription_id,
                    update_sequence: 0,
                    initial_target_tip_hash: target,
                    acknowledged: Cursor {
                        height,
                        hash: locator_hashes[0],
                        headers: 0,
                        bytes: 0,
                    },
                    locator_hashes,
                    added_credit: credit(objects, bytes),
                    tree_aux_schema: schema,
                }
            },
        );
    let later = (
        (
            prop_oneof![Just(Operation::Grant), Just(Operation::Close)],
            1..=u64::MAX,
            1..=u32::MAX,
        ),
        (
            BlockHash::arbitrary(),
            HeightLe::arbitrary(),
            BlockHash::arbitrary(),
            any::<(u64, u64)>(),
        ),
        (
            0..=MAX_HS_PUSH_CREDIT_HEADERS,
            0..=MAX_HS_PUSH_CREDIT_BYTES,
            arbitrary_schema(),
        ),
    )
        .prop_map(
            |(
                (operation, subscription_id, update_sequence),
                (target, height, hash, (headers, bytes)),
                (objects, added_bytes, schema),
            )| {
                let added_credit = match operation {
                    Operation::Close => credit(0, 0),
                    // Lift a grant below the minimum to one header.
                    _ if objects == 0 && added_bytes < p::<HEADER_BYTES>() => {
                        credit(1, added_bytes)
                    }
                    _ => credit(objects, added_bytes),
                };
                SubscribeHeaders {
                    operation,
                    subscription_id,
                    update_sequence,
                    initial_target_tip_hash: target,
                    acknowledged: Cursor {
                        height,
                        hash,
                        headers,
                        bytes,
                    },
                    locator_hashes: Vec::new(),
                    added_credit,
                    tree_aux_schema: schema,
                }
            },
        );
    prop_oneof![opens, later].boxed()
}

/// Headers of `HEADER_BYTES` with arbitrary links, times, nonces, and
/// solutions.
fn arbitrary_header<const HEADER_BYTES: usize>() -> BoxedStrategy<Arc<block::Header>> {
    let base = *sample_header::<HEADER_BYTES>();
    let solution_bytes = match base.solution {
        Solution::Common(bytes) => bytes.len(),
        Solution::Regtest(bytes) => bytes.len(),
    };
    (
        any::<[u8; 32]>(),
        any::<u32>(),
        any::<[u8; 32]>(),
        proptest::collection::vec(any::<u8>(), solution_bytes),
    )
        .prop_map(move |(previous, time, nonce, solution)| {
            Arc::new(block::Header {
                previous_block_hash: block::Hash(previous),
                time: Utc
                    .timestamp_opt(time.into(), 0)
                    .single()
                    .expect("every u32 is a valid timestamp"),
                nonce: nonce.into(),
                solution: Solution::from_bytes(&solution).expect("the network's solution size"),
                ..base
            })
        })
        .boxed()
}

/// Valid pages of one to four entries.
fn arbitrary_page<const HEADER_BYTES: usize>() -> BoxedStrategy<Headers> {
    let record = empty_aux(block::Height(0));
    let entry = (
        arbitrary_header::<HEADER_BYTES>(),
        0..=2_000_000u32,
        (
            HeightLe::arbitrary(),
            any::<(u64, u64, u64)>(),
            any::<[u8; 32]>(),
        ),
    );
    (
        (1..=u64::MAX, BlockHash::arbitrary(), BlockHash::arbitrary()),
        (
            0..=block::Height::MAX.0 - 4,
            any::<bool>(),
            arbitrary_schema(),
        ),
        proptest::collection::vec(entry, 1..=4),
    )
        .prop_map(
            move |(
                (subscription_id, target, parent_hash),
                (parent_height, reaches_initial_target, schema),
                entries,
            )| Headers {
                subscription_id,
                initial_target_tip_hash: target,
                common_ancestor_height: block::Height(parent_height),
                common_ancestor_hash: parent_hash,
                reaches_initial_target,
                tree_aux_schema: schema,
                entries: entries
                    .into_iter()
                    .map(|(header, body_size, (height, counts, auth))| HeaderEntry {
                        header,
                        body_size,
                        tree_aux: (schema == AuxSchema::V1).then_some(TreeAuxRecordV1 {
                            height,
                            sapling_tx_count: counts.0,
                            orchard_tx_count: counts.1,
                            ironwood_tx_count: counts.2,
                            auth_data_root: AuthDataRoot::from(auth),
                            ..record
                        }),
                    })
                    .collect(),
            },
        )
        .boxed()
}

/// A frame without flags.
fn frame(message_type: u16, payload: Vec<u8>) -> Frame {
    Frame {
        message_type,
        flags: 0,
        payload,
    }
}

/// `Status` fields laid out by hand, so a fixture can break any rule.
#[derive(Clone)]
struct RawStatus {
    anchor: u32,
    tip: u32,
    retained: u32,
    header_cap: u32,
    subscriptions: u16,
    message_bytes: u32,
    mask: u32,
}

impl RawStatus {
    /// A legal status whose heights and caps sit inside every bound.
    fn valid() -> Self {
        Self {
            anchor: 5,
            tip: 9,
            retained: 3,
            header_cap: 1_000,
            subscriptions: 1,
            message_bytes: 2_097_152,
            mask: 1,
        }
    }

    /// The payload, field by field.
    fn payload(&self) -> Vec<u8> {
        [
            &self.anchor.to_le_bytes()[..],
            &[0x11; 32],
            &self.tip.to_le_bytes(),
            &[0x22; 32],
            &(0..32).collect::<Vec<u8>>(),
            &self.retained.to_le_bytes(),
            &self.header_cap.to_le_bytes(),
            &self.subscriptions.to_le_bytes(),
            &self.message_bytes.to_le_bytes(),
            &self.mask.to_le_bytes(),
        ]
        .concat()
    }

    /// The status a legal fixture decodes to.
    fn value(&self) -> Status {
        Status {
            work_anchor_height: block::Height(self.anchor),
            work_anchor_hash: hash(0x11),
            selected_tip_height: block::Height(self.tip),
            selected_tip_hash: hash(0x22),
            suffix_cumulative_work: U256::from_little_endian(&(0..32).collect::<Vec<u8>>()),
            oldest_retained_height: block::Height(self.retained),
            max_headers_per_response: self.header_cap,
            max_subscriptions: self.subscriptions,
            max_message_bytes: self.message_bytes,
            tree_aux_schema_mask: self.mask,
        }
    }
}

/// `SubscribeHeaders` fields laid out by hand, so a fixture can break any
/// rule.
#[derive(Clone)]
struct RawUpdate {
    operation: u8,
    subscription_id: u64,
    sequence: u32,
    cursor: (u32, [u8; 32], u64, u64),
    locator_count: u8,
    locators: Vec<[u8; 32]>,
    header_credit: u32,
    byte_credit: u32,
    schema: u8,
}

impl RawUpdate {
    /// An `Open` with `locators` locators and the whole credit window.
    fn open(locators: u8) -> Self {
        Self {
            operation: 0,
            subscription_id: 7,
            sequence: 0,
            cursor: (5, [0x40; 32], 0, 0),
            locator_count: locators,
            locators: (0..locators).map(|index| [0x40 + index; 32]).collect(),
            header_credit: MAX_HS_PUSH_CREDIT_HEADERS,
            byte_credit: MAX_HS_PUSH_CREDIT_BYTES,
            schema: 0,
        }
    }

    /// A `Grant` that adds the given credit.
    fn grant(header_credit: u32, byte_credit: u32) -> Self {
        Self {
            operation: 1,
            sequence: 1,
            cursor: (6, [0x33; 32], 1, 1_573),
            locator_count: 0,
            locators: Vec::new(),
            header_credit,
            byte_credit,
            ..Self::open(1)
        }
    }

    /// A `Close`, which adds no credit.
    fn close() -> Self {
        Self {
            operation: 2,
            sequence: 2,
            ..Self::grant(0, 0)
        }
    }

    /// The same update with one more locator, counted.
    fn with_locator(mut self, locator: [u8; 32]) -> Self {
        self.locator_count += 1;
        self.locators.push(locator);
        self
    }

    /// The payload, field by field.
    fn payload(&self) -> Vec<u8> {
        let (height, hash, headers, bytes) = self.cursor;
        [
            &[self.operation][..],
            &self.subscription_id.to_le_bytes(),
            &self.sequence.to_le_bytes(),
            &[0x22; 32],
            &height.to_le_bytes(),
            &hash,
            &headers.to_le_bytes(),
            &bytes.to_le_bytes(),
            &[self.locator_count],
            &self.locators.concat(),
            &self.header_credit.to_le_bytes(),
            &self.byte_credit.to_le_bytes(),
            &[self.schema],
        ]
        .concat()
    }

    /// The value of a fixture whose fields are legal.
    fn value(&self) -> SubscribeHeaders {
        let (height, cursor_hash, headers, bytes) = self.cursor;
        SubscribeHeaders {
            operation: match self.operation {
                0 => Operation::Open,
                1 => Operation::Grant,
                _ => Operation::Close,
            },
            subscription_id: self.subscription_id,
            update_sequence: self.sequence,
            initial_target_tip_hash: hash(0x22),
            acknowledged: Cursor {
                height: block::Height(height),
                hash: block::Hash(cursor_hash),
                headers,
                bytes,
            },
            locator_hashes: self.locators.iter().copied().map(block::Hash).collect(),
            added_credit: credit(self.header_credit, self.byte_credit),
            tree_aux_schema: if self.schema == 1 {
                AuxSchema::V1
            } else {
                AuxSchema::None
            },
        }
    }
}

/// `Headers` fields laid out by hand, so a fixture can break any rule.
#[derive(Clone)]
struct RawPage {
    subscription_id: u64,
    parent_height: u32,
    count: u32,
    reaches: u8,
    schema: u8,
    headers: Vec<u8>,
    hints: Vec<u32>,
    records: Vec<u8>,
}

impl RawPage {
    /// One real header with no records.
    fn valid(header: &block::Header) -> Self {
        Self {
            subscription_id: 7,
            parent_height: 10,
            count: 1,
            reaches: 1,
            schema: 0,
            headers: header_bytes(header),
            hints: vec![2_000_000],
            records: Vec::new(),
        }
    }

    /// One real header with an empty schema-1 record.
    fn with_record(header: &block::Header) -> Self {
        let mut records = Vec::new();
        empty_aux(block::Height(11))
            .encode_to(&mut records)
            .expect("a record encodes");
        Self {
            schema: 1,
            records,
            ..Self::valid(header)
        }
    }

    /// The payload, field by field.
    fn payload(&self) -> Vec<u8> {
        [
            &self.subscription_id.to_le_bytes()[..],
            &[0x22; 32],
            &self.parent_height.to_le_bytes(),
            &[0x33; 32],
            &self.count.to_le_bytes(),
            &[self.reaches, self.schema],
            &self.headers,
            &self
                .hints
                .iter()
                .flat_map(|hint| hint.to_le_bytes())
                .collect::<Vec<_>>(),
            &self.records,
        ]
        .concat()
    }
}

/// A `HeadersOutcome` payload laid out by hand, with any code byte.
fn outcome_payload(subscription_id: u64, code: u8) -> Vec<u8> {
    [&subscription_id.to_le_bytes()[..], &[0x22; 32], &[code]].concat()
}

impl<const HEADER_BYTES: usize> MessageSample for Message<HEADER_BYTES> {
    fn samples() -> Vec<Self> {
        let p = p::<HEADER_BYTES>();
        let mut samples = vec![
            Self::Status(smallest_status::<HEADER_BYTES>()),
            Self::Status(Status {
                work_anchor_height: block::Height::MAX,
                selected_tip_height: block::Height::MAX,
                suffix_cumulative_work: U256::MAX,
                oldest_retained_height: block::Height::MAX,
                max_headers_per_response: MAX_HS_RANGE,
                max_message_bytes: 2_097_152,
                tree_aux_schema_mask: 1,
                ..smallest_status::<HEADER_BYTES>()
            }),
            Self::SubscribeHeaders(open(1, credit(1, p), AuxSchema::None)),
            Self::SubscribeHeaders(open(
                13,
                credit(MAX_HS_PUSH_CREDIT_HEADERS, MAX_HS_PUSH_CREDIT_BYTES),
                AuxSchema::V1,
            )),
            Self::SubscribeHeaders(update(Operation::Grant, 1, credit(1, 0))),
            Self::SubscribeHeaders(update(Operation::Grant, u32::MAX, credit(0, p))),
            Self::SubscribeHeaders(update(Operation::Close, 2, credit(0, 0))),
            Self::Headers(page::<HEADER_BYTES>(1, AuxSchema::None)),
            Self::Headers(page::<HEADER_BYTES>(1, AuxSchema::V1)),
            Self::Headers(page::<HEADER_BYTES>(
                most_headers::<HEADER_BYTES>(AuxSchema::V1),
                AuxSchema::V1,
            )),
        ];
        samples.extend(
            OUTCOME_CODES
                .into_iter()
                .map(|code| Self::HeadersOutcome(outcome(u64::MAX, code))),
        );
        samples
    }

    fn arbitrary_valid() -> BoxedStrategy<Self> {
        prop_oneof![
            arbitrary_status::<HEADER_BYTES>().prop_map(Self::Status),
            arbitrary_update::<HEADER_BYTES>().prop_map(Self::SubscribeHeaders),
            arbitrary_page::<HEADER_BYTES>().prop_map(Self::Headers),
            (
                1..=u64::MAX,
                BlockHash::arbitrary(),
                proptest::sample::select(&OUTCOME_CODES[..]),
            )
                .prop_map(|(subscription_id, initial_target_tip_hash, outcome)| {
                    Self::HeadersOutcome(HeadersOutcome {
                        subscription_id,
                        initial_target_tip_hash,
                        outcome,
                    })
                }),
        ]
        .boxed()
    }

    fn violations() -> Vec<Violation<Self>> {
        violations::<HEADER_BYTES>()
    }
}

/// `raw` after `change`.
fn with<T>(mut raw: T, change: impl FnOnce(&mut T)) -> T {
    change(&mut raw);
    raw
}

/// Violations from `label: name => frame;` rows. Each frame must fail with
/// `WireError::OutOfRange(label)`.
macro_rules! out_of_range {
    ($($label:literal: $name:literal => $frame:expr;)*) => {
        vec![$(Violation {
            name: $name,
            frame: $frame,
            rejected_by: |error| *error == WireError::OutOfRange($label),
        }),*]
    };
}

/// Fixtures that each break one domain rule on a network with
/// `HEADER_BYTES`-byte headers. Lengths stay within each row, so the rule is
/// what rejects them.
fn violations<const HEADER_BYTES: usize>() -> Vec<Violation<Message<HEADER_BYTES>>> {
    let p = p::<HEADER_BYTES>();
    let record = u32::try_from(smallest_page(HEADER_BYTES, AuxSchema::V1)).expect("fits u32");
    let header = sample_header::<HEADER_BYTES>();
    let status =
        |change: &dyn Fn(&mut RawStatus)| frame(1, with(RawStatus::valid(), change).payload());
    let (open, grant, close) = (RawUpdate::open, RawUpdate::grant, RawUpdate::close);
    let update = |raw: RawUpdate| frame(2, raw.payload());
    let page =
        |change: &dyn Fn(&mut RawPage)| frame(3, with(RawPage::valid(&header), change).payload());
    let recorded = |change: &dyn Fn(&mut RawPage)| {
        frame(3, with(RawPage::with_record(&header), change).payload())
    };
    let outcome = |subscription_id, code| frame(4, outcome_payload(subscription_id, code));
    // A header of the other network's size, padded or cut to this network's slot.
    let mut foreign = if HEADER_BYTES == REGTEST_HEADER_BYTES {
        header_bytes(&sample_header::<COMMON_HEADER_BYTES>())
    } else {
        header_bytes(&sample_header::<REGTEST_HEADER_BYTES>())
    };
    foreign.resize(HEADER_BYTES, 0);
    let foreign = page(&|g| g.headers = foreign.clone());

    let mut violations = out_of_range! {
        "status work anchor": "anchor above the tip" => status(&|s| s.anchor = 10);
        "status retention floor": "retention floor above the tip" => status(&|s| s.retained = 10);
        "status header cap": "no headers per response" => status(&|s| s.header_cap = 0);
        "status header cap": "one header above MAX_HS_RANGE" => status(&|s| s.header_cap = 4_001);
        "status subscription cap": "no subscriptions" => status(&|s| s.subscriptions = 0);
        "status subscription cap": "two subscriptions" => status(&|s| s.subscriptions = 2);
        "status schema mask": "unknown schema bit" => status(&|s| s.mask = 0b11);
        "status schema mask": "top schema bit" => status(&|s| s.mask = 0x8000_0001);
        "status message cap": "one byte above 2 MiB" => status(&|s| s.message_bytes = 2_097_153);
        "status message cap": "one byte below a page" =>
            status(&|s| (s.mask, s.message_bytes) = (0, p - 1));
        "status message cap": "one byte below a page with a record" =>
            status(&|s| s.message_bytes = record - 1);
        "update operation": "operation 3" => update(with(grant(1, 0), |u| u.operation = 3));
        "subscription id": "update for subscription 0" =>
            update(with(grant(1, 0), |u| u.subscription_id = 0));
        "tree-aux schema": "update asks for schema 2" =>
            update(with(grant(1, 0), |u| u.schema = 2));
        "update credit": "header credit one above the window" => update(grant(4_001, 0));
        "update credit": "byte credit one above the window" =>
            update(grant(0, MAX_HS_PUSH_CREDIT_BYTES + 1));
        "update sequence": "open with sequence 1" => update(with(open(1), |u| u.sequence = 1));
        "locator count": "open with no locators" => update(open(0));
        "locator hashes": "open with a repeated locator" =>
            update(open(2).with_locator([0x40; 32]));
        "open cursor": "open away from its first locator" =>
            update(with(open(2), |u| u.cursor.1 = [0x41; 32]));
        "open cursor": "open with a consumed header" => update(with(open(1), |u| u.cursor.2 = 1));
        "open cursor": "open with consumed bytes" => update(with(open(1), |u| u.cursor.3 = 1));
        "open credit": "open without header credit" =>
            update(with(open(1), |u| u.header_credit = 0));
        "open credit": "open one byte below a page" =>
            update(with(open(1), |u| u.byte_credit = p - 1));
        "open credit": "open one byte below a page with a record" =>
            update(with(open(1), |u| (u.schema, u.byte_credit) = (1, record - 1)));
        "update sequence": "grant with sequence 0" => update(with(grant(1, 0), |u| u.sequence = 0));
        "update sequence": "close with sequence 0" => update(with(close(), |u| u.sequence = 0));
        "update locators": "grant with a locator" => update(grant(1, 0).with_locator([0x40; 32]));
        "update locators": "close with a locator" => update(close().with_locator([0x40; 32]));
        "grant minimum": "grant one byte below a page" => update(grant(0, p - 1));
        "close credit": "close with header credit" =>
            update(with(close(), |u| u.header_credit = 1));
        "close credit": "close with byte credit" => update(with(close(), |u| u.byte_credit = 1));
        "subscription id": "page for subscription 0" => page(&|g| g.subscription_id = 0);
        // The zero-count page keeps one entry, so it reaches the row's minimum length.
        "header count": "page with no headers" => page(&|g| g.count = 0);
        "header count": "page one header above MAX_HS_RANGE" => page(&|g| g.count = 4_001);
        "boolean": "target marker 2" => page(&|g| g.reaches = 2);
        "tree-aux schema": "page with schema 2" => page(&|g| g.schema = 2);
        "header entries": "page that counts two headers and holds one" => page(&|g| g.count = 2);
        "header entries": "page that names records and holds none" => page(&|g| g.schema = 1);
        "page heights": "header above the maximum height" =>
            page(&|g| g.parent_height = block::Height::MAX.0);
        "body size hint": "hint above the block-size ceiling" =>
            page(&|g| g.hints = vec![2_000_001]);
        "block header": "header version 3" =>
            page(&|g| g.headers[..4].copy_from_slice(&3u32.to_le_bytes()));
        // The Orchard root follows the height and the Sapling root. All ones is
        // above the Pallas base field modulus.
        "tree-aux record": "non-canonical Orchard root" =>
            recorded(&|g| g.records[36..68].fill(0xff));
        "subscription id": "outcome for subscription 0" => outcome(0, 1);
        "headers outcome": "outcome 0" => outcome(1, 0);
        "headers outcome": "retired Busy outcome" => outcome(1, 4);
        "headers outcome": "outcome 8" => outcome(1, 8);
    };
    violations.push(Violation {
        name: "open that counts 14 locators and holds 13",
        frame: update(with(open(13), |u| u.locator_count = 14)),
        rejected_by: |error| matches!(error, WireError::CountOutOfRange { count: 14, .. }),
    });
    violations.extend(if HEADER_BYTES == REGTEST_HEADER_BYTES {
        // A Mainnet solution cannot fit a Regtest slot.
        out_of_range! { "block header": "Mainnet header" => foreign; }
    } else {
        // A Regtest header leaves the rest of a Mainnet slot unread.
        out_of_range! { "header solution size": "Regtest header" => foreign; }
    });
    violations
}

message_suite!(mainnet_and_testnet_family, CommonMessage);
message_suite!(regtest_family, RegtestMessage);

/// Each layout carries exactly its family's rows.
#[test]
fn layouts_carry_exactly_their_family_rows() {
    check_layout_carries_family::<CommonMessage>(&CommonMessage::LAYOUT);
    check_layout_carries_family::<RegtestMessage>(&RegtestMessage::LAYOUT);
}

/// The frame suite: every reader of both layouts follows its rows.
#[test]
fn readers_follow_both_layouts() {
    check_frame_filter(&CommonMessage::LAYOUT);
    check_frame_filter(&RegtestMessage::LAYOUT);
}

/// Each network selects the layout for its header size, on stream 5 at
/// version 9 under capability bit 9.
#[test]
fn every_network_selects_the_layout_for_its_header_size() {
    /// Check `network`'s layout against the `HEADER_BYTES` family.
    fn check<const HEADER_BYTES: usize>(network: &Network) {
        let layout = layout(network);
        assert_eq!(layout, &Message::<HEADER_BYTES>::LAYOUT[..]);
        assert_eq!(
            (layout[0].kind, layout[0].version, layout[0].capability),
            (5, 9, 1 << 9)
        );
    }
    on_every_network!(check);
}

/// The rows hold the specification's values on every network.
#[test]
fn rows_hold_the_specified_limits() {
    /// Check the rows of the `HEADER_BYTES` family.
    fn check<const HEADER_BYTES: usize>(_network: &Network) {
        let [status, subscribe, headers, outcome] = Message::<HEADER_BYTES>::ROWS;
        assert_eq!(status.message_type, 1);
        assert_eq!(status.payload, PayloadLen::exact(122));
        assert_eq!(
            status.role,
            MessageRole::Announcement {
                cadence: Cadence {
                    capacity: 602,
                    refill_interval: std::time::Duration::from_millis(500),
                    send_interval: std::time::Duration::from_secs(1),
                },
            }
        );
        // The bucket holds exactly the one-second sender's outage burst.
        assert_eq!(
            Cadence::min_capacity(std::time::Duration::from_secs(1)),
            602
        );

        assert_eq!(subscribe.message_type, 2);
        assert_eq!(subscribe.payload, PayloadLen::between(107, 523));
        assert_eq!(
            subscribe.role,
            MessageRole::Subscription {
                max_live: 1,
                credit: Credit {
                    objects: 4_000,
                    bytes: 8 * 1024 * 1024,
                },
                cursor_history: 4_096,
                cadence: None,
            }
        );

        let (smallest, largest) = match HEADER_BYTES {
            COMMON_HEADER_BYTES => (1_573, 2_096_713),
            REGTEST_HEADER_BYTES => (263, 1_348_082),
            _ => unreachable!("no network has {HEADER_BYTES}-byte headers"),
        };
        assert_eq!(headers.message_type, 3);
        assert_eq!(headers.payload, PayloadLen::between(smallest, largest));
        assert_eq!(
            headers.role,
            MessageRole::Response {
                request: 2,
                ends_exchange: false
            }
        );

        assert_eq!(outcome.message_type, 4);
        assert_eq!(outcome.payload, PayloadLen::exact(41));
        assert_eq!(
            outcome.role,
            MessageRole::Response {
                request: 2,
                ends_exchange: true
            }
        );
    }
    on_every_network!(check);
}

/// The cap test: the fixed sizes and the page bounds are what the codec
/// produces.
///
/// A one-header page is the fixed fields, the header, and its hint, so it pins
/// `HEADERS_RESPONSE_FIXED_BYTES`. Under each schema, the most headers that fit
/// encode, and one more does not, so the `Headers` row's maximum is the
/// longest page the codec produces, not the 2 MiB cap.
#[test]
fn caps_match_the_codec_on_every_network() {
    /// Check the caps of the `HEADER_BYTES` family on `network`.
    fn check<const HEADER_BYTES: usize>(network: &Network) {
        assert_eq!(HEADERS_RESPONSE_FIXED_BYTES, 82);
        assert_eq!(HEADERS_OUTCOME_BYTES, 41);
        let header = genesis_header(network);
        let one = Headers {
            entries: vec![HeaderEntry {
                header: header.clone(),
                body_size: 0,
                tree_aux: None,
            }],
            ..page::<HEADER_BYTES>(1, AuxSchema::None)
        };
        let payload = encode_frame(&Message::<HEADER_BYTES>::Headers(one))
            .expect("a one-header page encodes")
            .payload;
        assert_eq!(
            payload.len(),
            HEADERS_RESPONSE_FIXED_BYTES + HEADER_BYTES + 4
        );
        assert_eq!(
            &payload[HEADERS_RESPONSE_FIXED_BYTES..][..HEADER_BYTES],
            &header_bytes(&header)[..],
            "the header follows the fixed fields"
        );
        let row = Message::<HEADER_BYTES>::ROWS[2].payload;
        assert_eq!(payload.len(), row.min());

        let outcome = encode_frame(&Message::<HEADER_BYTES>::HeadersOutcome(outcome(
            1,
            HeadersOutcomeCode::SubscriptionClosed,
        )))
        .expect("an outcome encodes");
        assert_eq!(outcome.payload.len(), HEADERS_OUTCOME_BYTES);

        let mut longest = 0;
        for schema in [AuxSchema::None, AuxSchema::V1] {
            let most = most_headers::<HEADER_BYTES>(schema);
            let full = encode_frame(&Message::<HEADER_BYTES>::Headers(page::<HEADER_BYTES>(
                most, schema,
            )))
            .expect("the fullest page encodes");
            longest = longest.max(full.payload.len());
            assert!(
                encode_frame(&Message::<HEADER_BYTES>::Headers(page::<HEADER_BYTES>(
                    most + 1,
                    schema
                )))
                .is_err(),
                "one more header under {schema:?} breaks the 2 MiB or MAX_HS_RANGE cap"
            );
        }
        assert_eq!(longest, row.max());
        assert!(row.max() <= MAX_HS_MESSAGE_BYTES);
    }
    on_every_network!(check);
}

/// A legal fixture, laid out by hand, and the message it must decode to.
type Fixture<const HEADER_BYTES: usize> = (&'static str, Frame, Message<HEADER_BYTES>);

/// Legal fixtures at each boundary, independent of the encoder.
fn fixtures<const HEADER_BYTES: usize>(network: &Network) -> Vec<Fixture<HEADER_BYTES>> {
    let p = p::<HEADER_BYTES>();
    let record = u32::try_from(smallest_page(HEADER_BYTES, AuxSchema::V1)).expect("fits u32");
    let header = genesis_header(network);
    let status = |raw: RawStatus| (frame(1, raw.payload()), Message::Status(raw.value()));
    let update = |raw: RawUpdate| {
        (
            frame(2, raw.payload()),
            Message::SubscribeHeaders(raw.value()),
        )
    };
    let entry = |tree_aux| HeaderEntry {
        header: header.clone(),
        body_size: 2_000_000,
        tree_aux,
    };
    let page = |raw: &RawPage, entries| {
        (
            frame(3, raw.payload()),
            Message::Headers(Headers {
                subscription_id: 7,
                initial_target_tip_hash: hash(0x22),
                common_ancestor_height: block::Height(10),
                common_ancestor_hash: hash(0x33),
                reaches_initial_target: true,
                tree_aux_schema: if raw.schema == 1 {
                    AuxSchema::V1
                } else {
                    AuxSchema::None
                },
                entries,
            }),
        )
    };
    let mut fixtures = vec![
        ("status", status(RawStatus::valid())),
        (
            "status cap at a one-header page",
            status(RawStatus {
                mask: 0,
                message_bytes: p,
                ..RawStatus::valid()
            }),
        ),
        (
            "status cap at a one-header page with a record",
            status(RawStatus {
                message_bytes: record,
                ..RawStatus::valid()
            }),
        ),
        (
            "status at the edges",
            status(RawStatus {
                anchor: 9,
                retained: 9,
                header_cap: 4_000,
                ..RawStatus::valid()
            }),
        ),
        ("open with one locator", update(RawUpdate::open(1))),
        ("open with 13 locators", update(RawUpdate::open(13))),
        (
            "open with the least credit",
            update(RawUpdate {
                header_credit: 1,
                byte_credit: p,
                ..RawUpdate::open(1)
            }),
        ),
        (
            "open with the least credit for records",
            update(RawUpdate {
                header_credit: 1,
                byte_credit: record,
                schema: 1,
                ..RawUpdate::open(1)
            }),
        ),
        ("grant of one header", update(RawUpdate::grant(1, 0))),
        ("grant of one page of bytes", update(RawUpdate::grant(0, p))),
        (
            "grant of the whole window",
            update(RawUpdate::grant(
                MAX_HS_PUSH_CREDIT_HEADERS,
                MAX_HS_PUSH_CREDIT_BYTES,
            )),
        ),
        ("close", update(RawUpdate::close())),
        (
            "page of one header",
            page(&RawPage::valid(&header), vec![entry(None)]),
        ),
        (
            "page of one header with a record",
            page(
                &RawPage::with_record(&header),
                vec![entry(Some(empty_aux(block::Height(11))))],
            ),
        ),
    ];
    for code in OUTCOME_CODES {
        // A fieldless enum's discriminant is its declared `u8` value.
        let wire = code as u8;
        fixtures.push((
            "outcome",
            (
                frame(4, outcome_payload(u64::MAX, wire)),
                Message::HeadersOutcome(outcome(u64::MAX, code)),
            ),
        ));
    }
    fixtures
        .into_iter()
        .map(|(name, (frame, message))| (name, frame, message))
        .collect()
}

/// Hand-written fixtures decode to their values and re-encode to their bytes.
#[test]
fn fixtures_decode_and_reencode_on_every_network() {
    /// Check the fixtures of the `HEADER_BYTES` family on `network`.
    fn check<const HEADER_BYTES: usize>(network: &Network) {
        for (name, frame, message) in fixtures::<HEADER_BYTES>(network) {
            let decoded = decode_checked::<Message<HEADER_BYTES>>(&frame)
                .unwrap_or_else(|error| panic!("{name} is legal: {error}"));
            assert_eq!(decoded, message, "{name} decodes to its value");
            assert_eq!(
                encode_frame(&message).expect("legal fixtures encode"),
                frame,
                "{name} re-encodes to its bytes"
            );
        }
    }
    on_every_network!(check);
}

/// The fuzz replay accepts every sample on both header sizes, and rejects
/// flags, undeclared types, and empty input.
#[test]
fn fuzz_replay_round_trips_samples_and_rejects_the_rest() {
    /// Replay the samples of the `HEADER_BYTES` family.
    fn check<const HEADER_BYTES: usize>(network: &Network) {
        let regtest = if network.is_regtest() { 0x80 } else { 0 };
        for sample in Message::<HEADER_BYTES>::samples() {
            let frame = encode_frame(&sample).expect("samples encode");
            let selector = regtest | u8::try_from(frame.message_type).expect("types fit u8");
            let bytes = [&[selector][..], &frame.payload].concat();
            assert!(replay_header_sync_v9_codec_bytes(&bytes));
            let flagged = [&[selector | 0x08][..], &frame.payload].concat();
            assert!(!replay_header_sync_v9_codec_bytes(&flagged));
            let undeclared = [&[regtest | 0x05][..], &frame.payload].concat();
            assert!(!replay_header_sync_v9_codec_bytes(&undeclared));
        }
    }
    on_every_network!(check);
    assert!(!replay_header_sync_v9_codec_bytes(&[]));
}
