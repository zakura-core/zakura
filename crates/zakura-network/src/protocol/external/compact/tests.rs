//! Compact block relay message encodings, limits, and short IDs.

use zakura_chain::{
    block::Block,
    transaction::{self, AuthDigest, WtxId},
    transparent,
};
use zakura_test::vectors::{
    BLOCK_MAINNET_1687107_BYTES, BLOCK_MAINNET_1687108_BYTES, BLOCK_MAINNET_419201_BYTES,
};

use super::*;

fn block(bytes: &[u8]) -> Block {
    Block::zcash_deserialize(bytes).unwrap()
}

/// Returns `transaction` with one transparent input whose unlock script is `script`.
fn with_unlock_script(transaction: &Transaction, script: &[u8]) -> Arc<Transaction> {
    let mut transaction = transaction.clone();
    *transaction.inputs_mut() = vec![transparent::Input::PrevOut {
        outpoint: transparent::OutPoint {
            hash: transaction::Hash([7; 32]),
            index: 0,
        },
        unlock_script: transparent::Script::new(script),
        sequence: u32::MAX,
    }];
    Arc::new(transaction)
}

fn decode_compact(bytes: &[u8]) -> Result<CompactBlock, SerializationError> {
    CompactBlock::zcash_deserialize_bounded(&mut &bytes[..])
}

fn decode_response(bytes: &[u8]) -> Result<BlockTransactions, SerializationError> {
    BlockTransactions::zcash_deserialize_bounded(&mut &bytes[..])
}

fn decode_request(bytes: &[u8]) -> Result<BlockTransactionsRequest, SerializationError> {
    BlockTransactionsRequest::zcash_deserialize_from_slice(&mut &bytes[..])
}

fn compact_size(value: usize) -> Vec<u8> {
    CompactSizeMessage::try_from(value)
        .unwrap()
        .zcash_serialize_to_vec()
        .unwrap()
}

/// Encodes a `cmpctblock` body without checking it: `short_id_count` zero short IDs, then the
/// prefilled transactions with their differential indexes.
fn raw_compact(
    header: &block::Header,
    short_id_count: usize,
    prefilled: &[(usize, &Transaction)],
) -> Vec<u8> {
    let mut bytes = header.zcash_serialize_to_vec().unwrap();
    bytes.extend_from_slice(&42u64.to_le_bytes());
    bytes.extend(compact_size(short_id_count));
    bytes.resize(bytes.len() + short_id_count * size_of::<ShortId>(), 0);
    bytes.extend(compact_size(prefilled.len()));
    for (delta, transaction) in prefilled {
        bytes.extend(compact_size(*delta));
        transaction.zcash_serialize(&mut bytes).unwrap();
    }
    bytes
}

fn assert_parse_error<T: std::fmt::Debug>(result: Result<T, SerializationError>, message: &str) {
    match result {
        Err(SerializationError::Parse(error)) => {
            assert!(
                error.contains(message),
                "{error:?} does not mention {message:?}"
            )
        }
        other => panic!("expected a parse error mentioning {message:?}, got {other:?}"),
    }
}

/// The literal vectors that Zebra's implementation pins, which this module's own SipHash-2-4
/// and SHA-256 key derivation must reproduce.
///
/// An independent Python implementation of the ZIP's definition gives the same values: SHA-256
/// of the 1,487-byte header followed by the little-endian nonce, the first two little-endian
/// words of that hash as keys, then the first six little-endian bytes of SipHash-2-4.
#[test]
fn short_id_literal_vectors() {
    let block_bytes = &BLOCK_MAINNET_1687108_BYTES[..];
    let mut compact = CompactBlock::from_block(&block(block_bytes), 0x8877_6655_4433_2211).unwrap();
    let keys = compact.keys();
    assert_eq!(
        keys,
        ShortIdKeys {
            k0: 0x1f23_5750_38c4_9917,
            k1: 0x555b_c930_792d_bafe,
        }
    );

    // The txid of ZIP 243 test vector 0, a v4 transaction.
    let v4_txid = transaction::Hash([
        0x76, 0x8b, 0xd1, 0x68, 0x80, 0xc7, 0x70, 0xbb, 0xf0, 0x15, 0x92, 0x73, 0x87, 0xf9, 0xdc,
        0x7e, 0x2e, 0x8d, 0xfe, 0xcc, 0xef, 0x55, 0xc3, 0x57, 0x4a, 0xb4, 0x04, 0xdf, 0x99, 0x23,
        0xbf, 0x80,
    ]);
    assert_eq!(
        keys.short_id(UnminedTxId::Legacy(v4_txid)),
        [0x9b, 0x7c, 0xe3, 0x31, 0x28, 0x95],
    );

    // The txid and auth digest of ZIP 244 test vector 0, a v5 transaction.
    let v5_wtxid = WtxId {
        id: transaction::Hash([
            0x55, 0x2c, 0x96, 0xbd, 0x33, 0x83, 0x4b, 0xa1, 0xa8, 0xa3, 0xec, 0xd8, 0x0a, 0x2c,
            0x9c, 0xb4, 0x11, 0x87, 0x55, 0x3a, 0x3d, 0xcf, 0xe7, 0x92, 0x83, 0x16, 0xbb, 0x70,
            0x70, 0x4b, 0x85, 0xd0,
        ]),
        auth_digest: AuthDigest([
            0x12, 0x76, 0x7e, 0x5f, 0x67, 0x85, 0x67, 0x36, 0x0f, 0xb3, 0xa1, 0xcb, 0x9c, 0xf8,
            0x58, 0x61, 0x3f, 0xfe, 0x22, 0x63, 0xb6, 0x53, 0xc6, 0xa3, 0x70, 0xee, 0x1f, 0x68,
            0x20, 0xab, 0xdc, 0x57,
        ]),
    };
    assert_eq!(
        keys.short_id(UnminedTxId::Witnessed(v5_wtxid)),
        [0xdf, 0x54, 0x2d, 0x79, 0x0c, 0xbd],
    );

    // SipHash-2-4's published vector for the 32-byte message 00..1f under the key 00..0f.
    let published = ShortIdKeys {
        k0: 0x0706_0504_0302_0100,
        k1: 0x0f0e_0d0c_0b0a_0908,
    };
    let message = transaction::Hash(std::array::from_fn(|index| u8::try_from(index).unwrap()));
    assert_eq!(
        published.short_id(UnminedTxId::Legacy(message)),
        [0xce, 0x7c, 0xf2, 0x72, 0x2f, 0x51],
    );

    // The keys hash the whole header, through the last byte of its Equihash solution.
    let mut header_bytes = compact.header.zcash_serialize_to_vec().unwrap();
    assert_eq!(header_bytes, block_bytes[..1487]);
    header_bytes[1486] ^= 1;
    compact.header = Arc::new(block::Header::zcash_deserialize(&header_bytes[..]).unwrap());
    assert_ne!(compact.keys(), keys);
}

/// A compact block prefills only the coinbase transaction, and gives every other transaction
/// the short ID of its txid before v5 and of its wtxid from v5.
#[test]
fn from_block_hashes_txids_before_v5_and_wtxids_from_v5() {
    // The pre-NU5 block has only v4 transactions; the NU5 block has v4 and v5 transactions.
    for (bytes, has_v5) in [
        (&BLOCK_MAINNET_419201_BYTES[..], false),
        (&BLOCK_MAINNET_1687107_BYTES[..], true),
    ] {
        let block = block(bytes);
        let compact = CompactBlock::from_block(&block, 7).unwrap();
        let keys = compact.keys();

        assert_eq!(compact.block_hash(), block.hash());
        assert_eq!(compact.transaction_count(), block.transactions.len());
        assert_eq!(
            compact.prefilled,
            [PrefilledTransaction {
                index: 0,
                transaction: block.transactions[0].clone(),
            }]
        );

        let mut witnessed = 0;
        for (transaction, short_id) in block.transactions[1..].iter().zip(&compact.short_ids) {
            let id = UnminedTxId::from(transaction.as_ref());
            assert_eq!(keys.short_id(id), *short_id);
            match id {
                UnminedTxId::Legacy(_) => assert!(transaction.version() <= 4),
                UnminedTxId::Witnessed(wtxid) => {
                    assert!(transaction.version() >= 5);
                    // The auth digest changes the short ID.
                    assert_ne!(keys.short_id(UnminedTxId::Legacy(wtxid.id)), *short_id);
                    witnessed += 1;
                }
            }
        }
        assert_eq!(compact.short_ids.len(), block.transactions.len() - 1);
        assert!(block.transactions.len() - 1 > witnessed);
        assert_eq!(witnessed > 0, has_v5);

        let bytes = compact.zcash_serialize_to_vec().unwrap();
        assert_eq!(decode_compact(&bytes).unwrap(), compact);
    }
}

/// A compact block with more prefilled transactions round trips, and its differential
/// indexes are encoded as the ZIP says.
#[test]
fn prefilled_transactions_round_trip_with_differential_indexes() {
    let block = block(&BLOCK_MAINNET_1687107_BYTES);
    let mut compact = CompactBlock::from_block(&block, 9).unwrap();
    let last = block.transactions.len() - 1;
    assert!(last >= 3);
    // Prefill the transactions at absolute indexes 1 and `last`, so their short IDs go.
    compact.short_ids.remove(last - 1);
    compact.short_ids.remove(0);
    compact
        .prefilled
        .extend([1, last].map(|index| PrefilledTransaction {
            index,
            transaction: block.transactions[index].clone(),
        }));
    compact.validate().unwrap();

    let bytes = compact.zcash_serialize_to_vec().unwrap();
    let expected = raw_compact(
        &block.header,
        0,
        &[
            (0, &block.transactions[0]),
            (0, &block.transactions[1]),
            (last - 2, &block.transactions[last]),
        ],
    );
    // Compare the prefilled section, which follows the short IDs.
    let header_and_nonce = block.header.zcash_serialized_size() + 8;
    let short_ids = 1 + compact.short_ids.len() * size_of::<ShortId>();
    assert_eq!(
        bytes[header_and_nonce + short_ids..],
        expected[header_and_nonce + 1..]
    );

    let mut reader = &bytes[..];
    assert_eq!(
        CompactBlock::zcash_deserialize_bounded(&mut reader).unwrap(),
        compact
    );
    assert!(reader.is_empty());
    assert_eq!(compact.transaction_count(), block.transactions.len());
}

/// `validate`, encoding, and decoding reject every prefilled ordering and coinbase rule.
#[test]
fn prefilled_ordering_and_coinbase_rules() {
    let block = block(&BLOCK_MAINNET_1687107_BYTES);
    let compact = CompactBlock::from_block(&block, 11).unwrap();
    let coinbase = block.transactions[0].clone();
    let other = block.transactions[1].clone();
    let prefilled = |index: usize, transaction: &Arc<Transaction>| PrefilledTransaction {
        index,
        transaction: transaction.clone(),
    };

    let cases: [(&str, Vec<PrefilledTransaction>, &str); 6] = [
        ("no prefilled", vec![], "coinbase transaction at index 0"),
        (
            "first is not a coinbase",
            vec![prefilled(0, &other)],
            "coinbase transaction at index 0",
        ),
        (
            "coinbase is not at index 0",
            vec![prefilled(1, &coinbase)],
            "coinbase transaction at index 0",
        ),
        (
            "a later coinbase",
            vec![prefilled(0, &coinbase), prefilled(1, &coinbase)],
            "coinbase transaction after index 0",
        ),
        (
            "a repeated index",
            vec![prefilled(0, &coinbase), prefilled(0, &other)],
            "out of order or out of range",
        ),
        (
            "an index past the block",
            vec![prefilled(0, &coinbase), prefilled(usize::MAX, &other)],
            "out of order or out of range",
        ),
    ];
    for (case, prefilled, error) in cases {
        let mut invalid = compact.clone();
        // Keep the transaction count of the block.
        invalid
            .short_ids
            .truncate(block.transactions.len() - prefilled.len().max(1));
        invalid.prefilled = prefilled;
        assert_parse_error(invalid.validate(), error);
        assert!(invalid.zcash_serialize_to_vec().is_err(), "{case}");
    }

    // The wire format can't repeat an index, so check the decoder with other bodies.
    let header = &block.header;
    let decoded = [
        (
            raw_compact(header, 0, &[]),
            "coinbase transaction at index 0",
        ),
        (
            raw_compact(header, 1, &[(1, &coinbase)]),
            "coinbase transaction at index 0",
        ),
        (
            raw_compact(header, 0, &[(0, &coinbase), (0, &coinbase)]),
            "coinbase transaction after index 0",
        ),
        (
            raw_compact(header, 0, &[(0, &coinbase), (1, &other)]),
            "index is out of range",
        ),
    ];
    for (bytes, error) in decoded {
        assert_parse_error(decode_compact(&bytes), error);
    }
}

/// The decoder rejects each count above the limit, and a sum above it, before allocating.
#[test]
fn compact_block_counts_are_limited() {
    let block = block(&BLOCK_MAINNET_1687107_BYTES);
    let coinbase = &block.transactions[0];
    let header = &block.header;
    let limit = MAX_COMPACT_TRANSACTIONS;

    // The short ID count is checked before the short IDs are read.
    let mut too_many_short_ids = raw_compact(header, 0, &[]);
    too_many_short_ids.truncate(header.zcash_serialized_size() + 8);
    too_many_short_ids.extend(compact_size(limit + 1));
    assert_parse_error(decode_compact(&too_many_short_ids), "count exceeds");

    let mut too_many_prefilled = raw_compact(header, 0, &[]);
    too_many_prefilled.pop();
    too_many_prefilled.extend(compact_size(limit + 1));
    assert_parse_error(decode_compact(&too_many_prefilled), "count exceeds");

    assert_parse_error(
        decode_compact(&raw_compact(header, limit, &[(0, coinbase)])),
        "count exceeds",
    );

    let mut invalid = CompactBlock::from_block(&block, 1).unwrap();
    invalid.short_ids = vec![[0; 6]; limit];
    assert_parse_error(invalid.validate(), "count exceeds");

    // A count the remaining bytes can't hold fails before allocating for it.
    let mut short = raw_compact(header, 0, &[]);
    short.truncate(header.zcash_serialized_size() + 8);
    short.extend(compact_size(limit));
    assert_parse_error(decode_compact(&short), "short IDs exceed the message");

    let mut short = raw_compact(header, 0, &[]);
    short.pop();
    short.extend(compact_size(limit));
    assert_parse_error(decode_compact(&short), "prefilled transactions exceed");
}

/// The size bound counts the header, the transaction count, the prefilled transactions, and
/// 54 bytes for each short ID, and allows exactly 2,000,000 bytes.
#[test]
fn compact_block_size_bound_is_exact() {
    let block = block(&BLOCK_MAINNET_1687107_BYTES);
    let coinbase = &block.transactions[0];
    // The transaction count needs three bytes for every count used here.
    let fixed = block.header.zcash_serialized_size() + 3 + coinbase.zcash_serialized_size();
    let limit = usize::try_from(MAX_BLOCK_BYTES).unwrap();
    let short_ids = (limit - fixed) / 54;
    let spare = limit - fixed - short_ids * 54;
    assert!(short_ids + 1 < MAX_COMPACT_TRANSACTIONS);

    // Grow the coinbase transaction's first lock script to use the spare bytes, so the largest
    // message takes exactly the limit. The script stays short enough to keep its length in one
    // byte.
    let mut coinbase = coinbase.as_ref().clone();
    let output = &mut coinbase.outputs_mut()[0];
    let mut script = output.lock_script.as_raw_bytes().to_vec();
    script.resize(script.len() + spare, 0);
    assert!(script.len() < 0xfd);
    output.lock_script = transparent::Script::new(&script);
    assert_eq!(
        block.header.zcash_serialized_size()
            + 3
            + coinbase.zcash_serialized_size()
            + short_ids * 54,
        limit
    );

    let mut compact = CompactBlock::from_block(&block, 3).unwrap();
    compact.prefilled[0].transaction = Arc::new(coinbase.clone());
    compact.short_ids = vec![[1; 6]; short_ids];
    compact.validate().unwrap();
    let bytes = compact.zcash_serialize_to_vec().unwrap();
    assert_eq!(decode_compact(&bytes).unwrap(), compact);

    compact.short_ids.push([1; 6]);
    assert_parse_error(compact.validate(), "exceed the block size limit");
    assert!(compact.zcash_serialize_to_vec().is_err());
    let bytes = raw_compact(&block.header, short_ids + 1, &[(0, &coinbase)]);
    assert!(decode_compact(&bytes).is_err());
}

/// A compact block can't be made from a block without a coinbase transaction first, with too
/// many transactions, or above the block size limit.
#[test]
fn from_block_rejects_blocks_outside_the_limits() {
    let block = block(&BLOCK_MAINNET_1687107_BYTES);

    let mut empty = block.clone();
    empty.transactions.clear();
    assert!(CompactBlock::from_block(&empty, 0).is_err());

    let mut no_coinbase = block.clone();
    no_coinbase.transactions.remove(0);
    assert_parse_error(
        CompactBlock::from_block(&no_coinbase, 0),
        "coinbase transaction at index 0",
    );

    let mut too_many = block.clone();
    too_many
        .transactions
        .resize(MAX_COMPACT_TRANSACTIONS + 1, block.transactions[1].clone());
    assert_parse_error(CompactBlock::from_block(&too_many, 0), "count exceeds");

    let mut oversized = block.clone();
    let limit = usize::try_from(MAX_BLOCK_BYTES).unwrap();
    oversized.transactions[1] = with_unlock_script(&block.transactions[1], &vec![0; limit]);
    assert_parse_error(
        CompactBlock::from_block(&oversized, 0),
        "exceed the block size limit",
    );
}

/// `getblocktxn` indexes are differentially encoded, canonical, increasing, and below the
/// transaction limit.
#[test]
fn request_indexes_are_canonical_and_bounded() {
    let request = BlockTransactionsRequest {
        block_hash: block::Hash([1; 32]),
        indexes: vec![0, 1, 255],
    };
    let bytes = request.zcash_serialize_to_vec().unwrap();
    assert_eq!(bytes[32..], [3, 0, 0, 0xfd, 0xfd, 0]);
    assert_eq!(decode_request(&bytes).unwrap(), request);

    let last = MAX_COMPACT_TRANSACTIONS - 1;
    for indexes in [vec![], vec![last], vec![0, last]] {
        let request = BlockTransactionsRequest {
            indexes,
            ..request.clone()
        };
        let bytes = request.zcash_serialize_to_vec().unwrap();
        assert_eq!(decode_request(&bytes).unwrap(), request);
    }

    for indexes in [
        vec![1, 1],
        vec![2, 1],
        vec![MAX_COMPACT_TRANSACTIONS],
        vec![usize::MAX],
        vec![0; MAX_COMPACT_TRANSACTIONS + 1],
    ] {
        let request = BlockTransactionsRequest {
            indexes,
            ..request.clone()
        };
        assert!(request.zcash_serialize_to_vec().is_err());
    }

    let mut over_limit = vec![1; 32];
    over_limit.extend(compact_size(MAX_COMPACT_TRANSACTIONS + 1));
    over_limit.resize(over_limit.len() + MAX_COMPACT_TRANSACTIONS + 1, 0);
    let mut index_at_limit = vec![1; 32];
    index_at_limit.extend([1, 0xfd]);
    index_at_limit.extend(
        u16::try_from(MAX_COMPACT_TRANSACTIONS)
            .unwrap()
            .to_le_bytes(),
    );
    for (malformed, error) in [
        (vec![1, 0xfd, 0, 0], "CompactSize"),
        (vec![1, 0xff, 255, 255, 255, 255, 255, 255, 255, 255], ""),
        (vec![2, 0xfe, 0, 0, 1, 0, 0], ""),
        (vec![2, 0], ""),
    ] {
        let mut bytes = vec![1; 32];
        bytes.extend(malformed);
        let result = decode_request(&bytes);
        assert!(result.is_err());
        if !error.is_empty() {
            assert_parse_error(result, error);
        }
    }
    assert_parse_error(decode_request(&over_limit), "count exceeds");
    assert_parse_error(decode_request(&index_at_limit), "index is out of range");
}

/// `blocktxn` transactions round trip, and their total size is limited to exactly 2,000,000
/// bytes when encoding and decoding.
#[test]
fn response_transaction_bytes_are_limited() {
    let block = block(&BLOCK_MAINNET_1687107_BYTES);
    let response = BlockTransactions {
        block_hash: block.hash(),
        transactions: block.transactions.clone(),
    };
    let bytes = response.zcash_serialize_to_vec().unwrap();
    let mut reader = &bytes[..];
    assert_eq!(
        BlockTransactions::zcash_deserialize_bounded(&mut reader).unwrap(),
        response
    );
    assert!(reader.is_empty());

    // Keep the script length's `CompactSize` the same size in every transaction.
    let base = &block.transactions[1];
    let overhead = with_unlock_script(base, &[0; 100_000]).zcash_serialized_size() - 100_000;
    let half = usize::try_from(MAX_BLOCK_BYTES).unwrap() / 2;
    let first = with_unlock_script(base, &vec![0; half - overhead]);
    for (extra, fits) in [(0, true), (1, false)] {
        let second = with_unlock_script(base, &vec![0; half - overhead + extra]);
        let response = BlockTransactions {
            block_hash: block.hash(),
            transactions: vec![first.clone(), second.clone()],
        };
        let mut bytes = block.hash().zcash_serialize_to_vec().unwrap();
        bytes.push(2);
        first.zcash_serialize(&mut bytes).unwrap();
        second.zcash_serialize(&mut bytes).unwrap();

        if fits {
            assert_eq!(response.zcash_serialize_to_vec().unwrap(), bytes);
            assert_eq!(decode_response(&bytes).unwrap(), response);
        } else {
            assert!(response.zcash_serialize_to_vec().is_err());
            assert!(decode_response(&bytes).is_err());
        }
    }

    let mut over_limit = block.hash().zcash_serialize_to_vec().unwrap();
    over_limit.extend(compact_size(MAX_COMPACT_TRANSACTIONS + 1));
    assert_parse_error(decode_response(&over_limit), "count exceeds");

    let mut unfilled = block.hash().zcash_serialize_to_vec().unwrap();
    unfilled.extend(compact_size(100));
    unfilled.resize(unfilled.len() + 100, 0);
    assert_parse_error(decode_response(&unfilled), "exceed the message");

    let too_many = BlockTransactions {
        block_hash: block.hash(),
        transactions: vec![base.clone(); MAX_COMPACT_TRANSACTIONS + 1],
    };
    assert!(too_many.zcash_serialize_to_vec().is_err());
}
