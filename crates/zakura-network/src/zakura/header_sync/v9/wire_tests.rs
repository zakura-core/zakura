//! The version 9 codec's tests.
//!
//! The generated item, message, and frame suites run on both header sizes.
//! Hand-written fixtures pin each layout independently of the encoder.

use std::{sync::Arc, time::Duration};

use proptest::{collection::hash_set, prelude::*};
use zakura_chain::{
    block::{self, genesis::regtest_genesis_block, merkle::AuthDataRoot},
    parameters::Network,
    serialization::{ZcashDeserialize, ZcashSerialize},
    work::difficulty::U256,
};

use super::{
    replay_header_sync_v9_codec_bytes,
    test_support::{p, record, smallest_status, OUTCOME_CODES},
    wire::*,
};
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
        message_suite::{decode_checked, message_suite, MessageSample, Violation},
        sample::WireSample,
        BlockHash, HeightLe, WireError,
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

/// A page of `count` copies of a real header, with the largest body-size
/// hint and, under schema 1, an empty record each.
fn page<const HEADER_BYTES: usize>(count: u32, schema: AuxSchema) -> Headers {
    let header = sample_header::<HEADER_BYTES>();
    let record = record(block::Height(0));
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

/// The most headers a page with records holds: the 2 MiB cap binds on
/// Mainnet and Testnet, and `MAX_HS_RANGE` binds on Regtest.
fn most_headers<const HEADER_BYTES: usize>() -> u32 {
    if HEADER_BYTES == REGTEST_HEADER_BYTES {
        4_000
    } else {
        1_273
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

item_suite!(status_payload, StatusPayload);
item_suite!(update_payload, UpdatePayload);
item_suite!(page_fixed_fields, PageFixed);
item_suite!(outcome_payload, OutcomePayload);

/// Valid statuses: heights put in order and the message cap moved into
/// range for the advertised schema.
fn arbitrary_status<const HEADER_BYTES: usize>() -> BoxedStrategy<Status> {
    (
        (
            HeightLe::arbitrary(),
            HeightLe::arbitrary(),
            HeightLe::arbitrary(),
        ),
        (
            BlockHash::arbitrary(),
            BlockHash::arbitrary(),
            Work::arbitrary(),
        ),
        (1..=MAX_HS_RANGE, 0..=1u32, any::<prop::sample::Index>()),
    )
        .prop_map(
            |(
                (a, b, c),
                (work_anchor_hash, selected_tip_hash, suffix_cumulative_work),
                (max_headers_per_response, tree_aux_schema_mask, cap),
            )| {
                let schema = if tree_aux_schema_mask == 1 {
                    AuxSchema::V1
                } else {
                    AuxSchema::None
                };
                let smallest = smallest_page(HEADER_BYTES, schema);
                let max_message_bytes = smallest + cap.index(MAX_HS_MESSAGE_BYTES - smallest + 1);
                Status {
                    work_anchor_height: a.min(b),
                    work_anchor_hash,
                    selected_tip_height: a.max(b).max(c),
                    selected_tip_hash,
                    suffix_cumulative_work,
                    oldest_retained_height: b.min(c),
                    max_headers_per_response,
                    max_subscriptions: MAX_HS_SUBSCRIPTIONS,
                    max_message_bytes: u32::try_from(max_message_bytes).expect("2 MiB fits u32"),
                    tree_aux_schema_mask,
                }
            },
        )
        .boxed()
}

/// Valid updates: each operation's fields fixed up to its shape.
fn arbitrary_update<const HEADER_BYTES: usize>() -> BoxedStrategy<SubscribeHeaders> {
    (
        (0..3u8, 1..=u64::MAX, any::<u32>(), BlockHash::arbitrary()),
        (
            HeightLe::arbitrary(),
            BlockHash::arbitrary(),
            any::<(u64, u64)>(),
        ),
        hash_set(any::<[u8; 32]>(), 1..=13),
        (
            0..=MAX_HS_PUSH_CREDIT_HEADERS,
            0..=MAX_HS_PUSH_CREDIT_BYTES,
            any::<bool>(),
        ),
    )
        .prop_map(
            |(
                (operation, subscription_id, sequence, target),
                (height, hash, (headers, bytes)),
                locators,
                (objects, added_bytes, records),
            )| {
                let tree_aux_schema = if records {
                    AuxSchema::V1
                } else {
                    AuxSchema::None
                };
                let mut update = SubscribeHeaders {
                    operation: Operation::Grant,
                    subscription_id,
                    update_sequence: sequence.max(1),
                    initial_target_tip_hash: target,
                    acknowledged: Cursor {
                        height,
                        hash,
                        headers,
                        bytes,
                    },
                    locator_hashes: Vec::new(),
                    added_credit: Credit {
                        objects,
                        bytes: added_bytes,
                    },
                    tree_aux_schema,
                };
                match operation {
                    0 => {
                        let least = u32::try_from(smallest_page(HEADER_BYTES, tree_aux_schema))
                            .expect("a page fits u32");
                        update.operation = Operation::Open;
                        update.update_sequence = 0;
                        update.locator_hashes = locators.into_iter().map(block::Hash).collect();
                        update.acknowledged = Cursor {
                            hash: update.locator_hashes[0],
                            headers: 0,
                            bytes: 0,
                            ..update.acknowledged
                        };
                        update.added_credit = Credit {
                            objects: objects.max(1),
                            bytes: added_bytes.max(least),
                        };
                    }
                    // Lift a grant below the minimum to one header.
                    1 if objects == 0 && added_bytes < p::<HEADER_BYTES>() => {
                        update.added_credit.objects = 1;
                    }
                    1 => {}
                    _ => {
                        update.operation = Operation::Close;
                        update.added_credit = Credit {
                            objects: 0,
                            bytes: 0,
                        };
                    }
                }
                update
            },
        )
        .boxed()
}

/// Valid pages of one to four copies of a real header.
fn arbitrary_page<const HEADER_BYTES: usize>() -> BoxedStrategy<Headers> {
    let header = sample_header::<HEADER_BYTES>();
    let record = record(block::Height(0));
    let entry = (
        0..=2_000_000u32,
        HeightLe::arbitrary(),
        any::<(u64, u64, u64)>(),
        any::<[u8; 32]>(),
    );
    (
        (1..=u64::MAX, BlockHash::arbitrary(), BlockHash::arbitrary()),
        (0..=block::Height::MAX.0 - 4, any::<bool>(), any::<bool>()),
        proptest::collection::vec(entry, 1..=4),
    )
        .prop_map(
            move |(
                (subscription_id, target, parent_hash),
                (parent_height, reaches_initial_target, records),
                entries,
            )| Headers {
                subscription_id,
                initial_target_tip_hash: target,
                common_ancestor_height: block::Height(parent_height),
                common_ancestor_hash: parent_hash,
                reaches_initial_target,
                tree_aux_schema: if records {
                    AuxSchema::V1
                } else {
                    AuxSchema::None
                },
                entries: entries
                    .into_iter()
                    .map(|(body_size, height, counts, auth)| HeaderEntry {
                        header: header.clone(),
                        body_size,
                        tree_aux: records.then_some(TreeAuxRecordV1 {
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
            added_credit: Credit {
                objects: self.header_credit,
                bytes: self.byte_credit,
            },
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
        record(block::Height(11))
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
        let record = u32::try_from(smallest_page(HEADER_BYTES, AuxSchema::V1)).expect("fits u32");
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
            Self::Status(
                RawStatus {
                    message_bytes: record,
                    ..RawStatus::valid()
                }
                .value(),
            ),
        ];
        let open = RawUpdate::open(1);
        samples.extend(
            [
                RawUpdate {
                    header_credit: 1,
                    byte_credit: p,
                    ..open.clone()
                },
                RawUpdate {
                    header_credit: 1,
                    byte_credit: record,
                    schema: 1,
                    ..open
                },
                RawUpdate {
                    schema: 1,
                    ..RawUpdate::open(13)
                },
                RawUpdate::grant(1, 0),
                RawUpdate {
                    sequence: u32::MAX,
                    ..RawUpdate::grant(0, p)
                },
                RawUpdate::close(),
            ]
            .map(|raw| Self::SubscribeHeaders(raw.value())),
        );
        samples.extend(
            [
                page::<HEADER_BYTES>(1, AuxSchema::None),
                page::<HEADER_BYTES>(1, AuxSchema::V1),
                page::<HEADER_BYTES>(most_headers::<HEADER_BYTES>(), AuxSchema::V1),
            ]
            .map(Self::Headers),
        );
        samples.extend(OUTCOME_CODES.map(|code| Self::HeadersOutcome(outcome(u64::MAX, code))));
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

/// The frame suite: every reader of both layouts follows its rows.
#[test]
fn readers_follow_both_layouts() {
    check_frame_filter(&CommonMessage::LAYOUT);
    check_frame_filter(&RegtestMessage::LAYOUT);
}

/// Each network selects the layout for its header size, on stream 5 at
/// version 9 under capability bit 10.
#[test]
fn every_network_selects_the_layout_for_its_header_size() {
    /// Check `network`'s layout against the `HEADER_BYTES` family.
    fn check<const HEADER_BYTES: usize>(network: &Network) {
        let layout = layout(network);
        assert_eq!(layout, &Message::<HEADER_BYTES>::LAYOUT[..]);
        assert_eq!(
            (layout[0].kind, layout[0].version, layout[0].capability),
            (5, 9, 1 << 10)
        );
    }
    on_every_network!(check);
}

/// The rows and constants hold the specification's values.
#[test]
fn rows_hold_the_specified_limits() {
    let rows = |rows: [MessageRule; 4]| rows.map(|row| (row.message_type, row.payload));
    let with_page = |page| {
        [
            (1, PayloadLen::exact(122)),
            (2, PayloadLen::between(107, 523)),
            (3, page),
            (4, PayloadLen::exact(41)),
        ]
    };
    assert_eq!(
        rows(CommonMessage::ROWS),
        with_page(PayloadLen::between(1_573, 2_096_713))
    );
    assert_eq!(
        rows(RegtestMessage::ROWS),
        with_page(PayloadLen::between(263, 1_348_082))
    );
    assert_eq!(
        (
            MAX_HS_PUSH_CREDIT_HEADERS,
            MAX_HS_PUSH_CREDIT_BYTES,
            MAX_HS_SUBSCRIPTIONS,
            HS_SENT_CURSOR_RING,
            HEADERS_RESPONSE_FIXED_BYTES,
        ),
        (4_000, 8 * 1024 * 1024, 1, 4_096, 82)
    );
    // A fieldless enum's discriminant is its declared `u8` value.
    assert_eq!(OUTCOME_CODES.map(|code| code as u8), [1, 2, 3, 5, 6, 7]);

    // The bucket holds a one-second sender's outage burst and refills twice
    // as fast as the sender.
    let MessageRole::Announcement { cadence } = CommonMessage::ROWS[0].role else {
        panic!("Status is an announcement");
    };
    assert_eq!(
        cadence,
        Cadence {
            capacity: Cadence::min_capacity(Duration::from_secs(1)),
            refill_interval: Duration::from_millis(500),
            send_interval: Duration::from_secs(1),
        }
    );
}

/// One legal payload per layout, laid out by hand, and the message it must
/// decode to.
fn fixtures<const HEADER_BYTES: usize>(network: &Network) -> Vec<(Frame, Message<HEADER_BYTES>)> {
    let header = genesis_header(network);
    let update = |raw: RawUpdate| {
        (
            frame(2, raw.payload()),
            Message::SubscribeHeaders(raw.value()),
        )
    };
    let page = |raw: RawPage, tree_aux: Option<TreeAuxRecordV1>| {
        (
            frame(3, raw.payload()),
            Message::Headers(Headers {
                subscription_id: 7,
                initial_target_tip_hash: hash(0x22),
                common_ancestor_height: block::Height(10),
                common_ancestor_hash: hash(0x33),
                reaches_initial_target: true,
                tree_aux_schema: if tree_aux.is_some() {
                    AuxSchema::V1
                } else {
                    AuxSchema::None
                },
                entries: vec![HeaderEntry {
                    header: header.clone(),
                    body_size: 2_000_000,
                    tree_aux,
                }],
            }),
        )
    };
    vec![
        (
            frame(1, RawStatus::valid().payload()),
            Message::Status(RawStatus::valid().value()),
        ),
        update(RawUpdate::open(13)),
        update(RawUpdate::grant(
            MAX_HS_PUSH_CREDIT_HEADERS,
            MAX_HS_PUSH_CREDIT_BYTES,
        )),
        update(RawUpdate::close()),
        page(RawPage::valid(&header), None),
        page(
            RawPage::with_record(&header),
            Some(record(block::Height(11))),
        ),
        (
            frame(4, outcome_payload(u64::MAX, 7)),
            Message::HeadersOutcome(outcome(u64::MAX, HeadersOutcomeCode::SubscriptionClosed)),
        ),
    ]
}

/// Hand-written fixtures decode to their values and re-encode to their bytes.
#[test]
fn fixtures_decode_and_reencode_on_every_network() {
    /// Check the fixtures of the `HEADER_BYTES` family on `network`.
    fn check<const HEADER_BYTES: usize>(network: &Network) {
        for (index, (frame, message)) in fixtures::<HEADER_BYTES>(network).into_iter().enumerate() {
            let decoded = decode_checked::<Message<HEADER_BYTES>>(&frame)
                .unwrap_or_else(|error| panic!("fixture {index} is legal: {error}"));
            assert_eq!(decoded, message, "fixture {index} decodes to its value");
            assert_eq!(
                encode_frame(&message).expect("legal fixtures encode"),
                frame,
                "fixture {index} re-encodes to its bytes"
            );
        }
    }
    on_every_network!(check);
}

/// The fuzz replay accepts every sample on both header sizes.
#[test]
fn fuzz_replay_round_trips_samples() {
    /// Replay the samples of the `HEADER_BYTES` family.
    fn check<const HEADER_BYTES: usize>(network: &Network) {
        let regtest = if network.is_regtest() { 0x80 } else { 0 };
        for sample in Message::<HEADER_BYTES>::samples() {
            let frame = encode_frame(&sample).expect("samples encode");
            let selector = regtest | u8::try_from(frame.message_type - 1).expect("types fit u8");
            let bytes = [&[selector][..], &frame.payload].concat();
            assert!(replay_header_sync_v9_codec_bytes(&bytes));
        }
    }
    on_every_network!(check);
    assert!(!replay_header_sync_v9_codec_bytes(&[]));
}
