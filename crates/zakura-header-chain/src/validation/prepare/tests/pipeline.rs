use std::sync::Arc;

use chrono::{DateTime, Duration, TimeZone, Utc};
use zakura_chain::{
    block::{self, genesis::regtest_genesis_block},
    parameters::{testnet::RegtestParameters, Network},
};

use super::super::*;
use crate::{
    CheckpointSet, EngineMode, Frontier, HeaderContextFact, HeaderValidationState, TrustedAnchor,
    ValidationLease,
};

#[derive(Copy, Clone)]
struct FixedClock(DateTime<Utc>);

impl crate::Clock for FixedClock {
    fn now(&self) -> DateTime<Utc> {
        self.0
    }
}

fn fixture() -> (HeaderRules, ValidationLease, Arc<block::Header>) {
    let anchor_header = regtest_genesis_block().header.clone();
    let anchor = Frontier::new(block::Height(0), anchor_header.hash());
    let network = Network::new_regtest(RegtestParameters::default());
    let config = crate::EngineConfig::new(
        EngineMode::Integrated,
        network,
        TrustedAnchor {
            frontier: anchor,
            header: anchor_header.clone(),
        },
        CheckpointSet::default(),
    )
    .expect("the regtest anchor and release manifest are coherent");
    let rules = HeaderRules::from_engine_config(&config)
        .expect("authenticated regtest parameters define their PoW policy");
    let lease = ValidationLease::new(
        anchor,
        vec![HeaderContextFact {
            frontier: anchor,
            header: anchor_header.clone(),
        }],
        config.network.clone(),
        config.trust_anchor_digest(),
    );
    (rules, lease, anchor_header)
}

fn child(parent: Frontier, template: &block::Header, seconds: i64) -> Arc<block::Header> {
    Arc::new(block::Header {
        previous_block_hash: parent.hash,
        time: template.time + Duration::seconds(seconds),
        nonce: [u8::try_from(seconds).unwrap_or(u8::MAX); 32].into(),
        ..*template
    })
}

#[test]
fn complete_batch_is_sealed_to_lease_and_uses_internal_context() {
    let (rules, lease, anchor) = fixture();
    let first = child(lease.parent, &anchor, 1);
    let first_frontier = Frontier::new(block::Height(1), first.hash());
    let second = child(first_frontier, &first, 2);
    let headers = [first, second];

    let batch = prepare_headers(
        HeaderBatchInput::new(&headers),
        lease.parent(),
        &rules,
        &FixedClock(anchor.time + Duration::hours(1)),
    )
    .expect("the continuous custom-network batch is valid");

    assert_eq!(batch.receipt().parent(), lease.parent());
    assert_eq!(
        batch.receipt().trust_anchor_digest(),
        lease.trust_anchor_digest()
    );
    assert_eq!(batch.headers().len(), 2);
    assert_eq!(batch.headers()[0].height, block::Height(1));
    assert_eq!(batch.headers()[1].height, block::Height(2));
    assert_eq!(
        batch.headers()[1].header.previous_block_hash,
        headers[0].hash()
    );
}

#[test]
fn rebased_suffix_evidence_matches_fresh_preparation() {
    let (rules, lease, anchor) = fixture();
    let first = child(lease.parent, &anchor, 1);
    let first_frontier = Frontier::new(block::Height(1), first.hash());
    let second = child(first_frontier, &first, 2);
    let third = child(Frontier::new(block::Height(2), second.hash()), &second, 3);
    let headers = [first.clone(), second.clone(), third.clone()];
    let clock = FixedClock(anchor.time + Duration::hours(1));

    let mut prepared = prepare_headers(
        HeaderBatchInput::new(&headers),
        lease.parent(),
        &rules,
        &clock,
    )
    .expect("the continuous context-free batch is valid");

    prepared
        .rebase_after(first_frontier)
        .expect("the prepared path contains the finalized parent");

    let fresh_suffix = prepare_headers(
        HeaderBatchInput::new(&[second, third]),
        first_frontier,
        &rules,
        &clock,
    )
    .expect("fresh preparation of the same suffix succeeds");

    assert_eq!(prepared.receipt().parent(), first_frontier);
    assert_eq!(prepared.headers().len(), 2);
    assert_eq!(prepared.headers(), fresh_suffix.headers());
    assert_eq!(prepared.evidence(), fresh_suffix.evidence());
}

#[test]
fn future_time_is_deferred_but_deterministic_failures_are_rejected() {
    let (rules, lease, anchor) = fixture();
    let future = child(lease.parent, &anchor, 3 * 60 * 60);
    let now = anchor.time;
    let batch = prepare_headers(
        HeaderBatchInput::new(std::slice::from_ref(&future)),
        lease.parent(),
        &rules,
        &FixedClock(now),
    )
    .expect("local future time is admitted only as deferred");
    assert_eq!(
        batch.headers()[0].validation,
        HeaderValidationState::DeferredUntil(future.time - Duration::hours(2))
    );

    let mut disconnected = *future;
    disconnected.previous_block_hash = block::Hash([0x55; 32]);
    let disconnected = Arc::new(disconnected);
    prepare_headers(
        HeaderBatchInput::new(std::slice::from_ref(&disconnected)),
        lease.parent(),
        &rules,
        &FixedClock(now),
    )
    .expect("graph-independent preparation does not claim parent linkage");
}

#[test]
fn oversized_batch_is_rejected_before_header_validation() {
    let (rules, lease, anchor) = fixture();
    let header = child(lease.parent, &anchor, 1);
    let headers = vec![header; crate::MAX_HEADERS_PER_TRANSITION_V1 + 1];
    assert!(matches!(
        prepare_headers(
            HeaderBatchInput::new(&headers),
            lease.parent(),
            &rules,
            &FixedClock(anchor.time),
        ),
        Err(HeaderFailure::Oversized {
            actual,
            maximum: crate::MAX_HEADERS_PER_TRANSITION_V1,
        }) if actual == crate::MAX_HEADERS_PER_TRANSITION_V1 + 1
    ));
}

#[test]
fn context_free_receipt_excludes_parent_and_branch_context_claims() {
    let (rules, lease, anchor) = fixture();
    let mut disconnected = *child(lease.parent, &anchor, 0);
    disconnected.previous_block_hash = block::Hash([0x55; 32]);
    let disconnected = Arc::new(disconnected);

    let batch = prepare_headers(
        HeaderBatchInput::new(std::slice::from_ref(&disconnected)),
        lease.parent(),
        &rules,
        &FixedClock(anchor.time),
    )
    .expect("graph-independent preparation does not claim parent linkage or MTP");

    assert_eq!(batch.receipt().parent(), lease.parent());
    assert_eq!(
        batch.receipt().trust_anchor_digest(),
        rules.trust_anchor_digest()
    );
    assert_eq!(batch.headers()[0].hash, disconnected.hash());
    assert_eq!(batch.headers()[0].height, block::Height(1));
}

#[test]
fn invalid_version_is_rejected_before_link_hashing() {
    let (rules, lease, anchor) = fixture();
    let mut invalid = *child(lease.parent, &anchor, 1);
    invalid.version = 3;
    let invalid = Arc::new(invalid);

    assert!(matches!(
        prepare_headers(
            HeaderBatchInput::new(std::slice::from_ref(&invalid)),
            lease.parent(),
            &rules,
            &FixedClock(anchor.time),
        ),
        Err(HeaderFailure::Invalid {
            rule: HeaderRule::EncodingVersionHash,
            ..
        })
    ));
}

#[test]
fn out_of_range_timestamp_is_reported_as_an_encoding_failure() {
    let (rules, lease, anchor) = fixture();

    for timestamp in [-1, i64::from(u32::MAX) + 1] {
        let mut invalid = *child(lease.parent, &anchor, 1);
        invalid.time = Utc
            .timestamp_opt(timestamp, 0)
            .single()
            .expect("the test timestamp fits in chrono's supported range");
        let invalid = Arc::new(invalid);

        assert!(matches!(
            prepare_headers(
                HeaderBatchInput::new(std::slice::from_ref(&invalid)),
                lease.parent(),
                &rules,
                &FixedClock(anchor.time),
            ),
            Err(HeaderFailure::Invalid {
                offset: 0,
                rule: HeaderRule::EncodingVersionHash,
                ..
            })
        ));
    }
}

#[test]
fn empty_batch_is_rejected_before_header_validation() {
    let (rules, lease, anchor) = fixture();
    assert!(matches!(
        prepare_headers(
            HeaderBatchInput::new(&[]),
            lease.parent(),
            &rules,
            &FixedClock(anchor.time),
        ),
        Err(HeaderFailure::Empty)
    ));
}
