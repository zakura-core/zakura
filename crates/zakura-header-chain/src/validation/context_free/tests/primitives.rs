use super::super::*;
use chrono::{TimeZone, Utc};
use zakura_chain::{
    block::{self, genesis::regtest_genesis_block},
    work::difficulty::{ExpandedDifficulty, U256},
};

#[test]
fn canonical_version_hash_link_and_height_boundaries() {
    let header = *regtest_genesis_block().header;
    let expected_hash = header.hash();
    assert_eq!(
        validate_encoding_version_hash(&header),
        Ok(expected_hash),
        "the shared validator hashes the complete canonical header"
    );

    let mut historical_non_four = header;
    historical_non_four.version = 5;
    assert!(validate_encoding_version_hash(&historical_non_four).is_ok());
    let mut too_old = header;
    too_old.version = 3;
    assert!(matches!(
        validate_encoding_version_hash(&too_old),
        Err(HeaderEncodingError::Version { version: 3, .. })
    ));
    let mut high_bit = header;
    high_bit.version = 1 << 31;
    assert!(validate_encoding_version_hash(&high_bit).is_err());

    let mut child = header;
    child.previous_block_hash = expected_hash;
    assert_eq!(
        validate_link(header.previous_block_hash, &[header, child]),
        Ok(())
    );
    child.previous_block_hash = block::Hash([9; 32]);
    assert!(matches!(
        validate_link(header.previous_block_hash, &[header, child]),
        Err(HeaderLinkError { offset: 1, .. })
    ));
    assert_eq!(
        infer_height(block::Height(7), Some(block::Height(8))),
        Ok(block::Height(8))
    );
    assert!(matches!(
        infer_height(block::Height(7), Some(block::Height(9))),
        Err(HeaderHeightError::PeerMismatch { .. })
    ));
    assert_eq!(
        infer_height(block::Height::MAX, None),
        Err(HeaderHeightError::Overflow(block::Height::MAX))
    );
}

#[test]
fn out_of_range_timestamps_are_rejected_before_hashing() {
    for timestamp in [-1, i64::from(u32::MAX) + 1] {
        let mut header = *regtest_genesis_block().header;
        header.time = Utc
            .timestamp_opt(timestamp, 0)
            .single()
            .expect("the test timestamp fits in chrono's supported range");

        assert_eq!(
            validate_encoding_version_hash(&header),
            Err(HeaderEncodingError::Timestamp { timestamp })
        );
    }
}

#[test]
fn hash_filter_accepts_equality_and_rejects_one_above() {
    let target = ExpandedDifficulty::from(U256::from(42));
    let mut equal_bytes = [0; 32];
    equal_bytes[0] = 42;
    assert_eq!(
        validate_hash_filter(block::Hash(equal_bytes), target),
        Ok(())
    );

    let mut above_bytes = equal_bytes;
    above_bytes[0] = 43;
    assert_eq!(
        validate_hash_filter(block::Hash(above_bytes), target),
        Err(HashFilterError {
            hash: block::Hash(above_bytes),
            target,
        })
    );
}

#[test]
fn future_time_accepts_two_hour_equality_and_rejects_one_second_above() {
    let mut header = *regtest_genesis_block().header;
    let now = header.time;
    let height = block::Height(1);
    let hash = header.hash();
    header.time = now + chrono::Duration::hours(2);
    assert!(validate_future_time(&header, now, height, hash).is_ok());
    header.time += chrono::Duration::seconds(1);
    assert!(validate_future_time(&header, now, height, hash).is_err());
}
