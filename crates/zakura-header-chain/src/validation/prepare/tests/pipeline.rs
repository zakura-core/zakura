use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use chrono::{DateTime, Duration, TimeZone, Utc};
use zakura_chain::{
    block::{self, genesis::regtest_genesis_block},
    parameters::{testnet::RegtestParameters, Network},
    work::{difficulty::CompactDifficulty, equihash},
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
        config.network().clone(),
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

/// Prepares `headers` on a 4-thread pool with a counting Equihash check that
/// rejects each header whose nonce is `rejected`'s. Returns the verdict, the
/// number of Equihash checks that ran, and the batch width.
fn prepare_counting(
    headers: &[Arc<block::Header>],
    rejected: block::Header,
) -> (
    Result<crate::PreparedHeaderBatch, HeaderFailure>,
    usize,
    usize,
) {
    let (rules, lease, anchor) = fixture();
    let checks = AtomicUsize::new(0);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .expect("the test pool starts");
    pool.install(|| {
        let verdict = super::super::pipeline::prepare_headers_checking_solutions(
            HeaderBatchInput::new(headers),
            lease.parent(),
            &rules,
            &FixedClock(anchor.time + Duration::hours(1)),
            &|header| {
                checks.fetch_add(1, Ordering::Relaxed);
                if header.nonce == rejected.nonce {
                    return Err(equihash::Error::InvalidSolutionSize {
                        network: rules.network().clone(),
                    });
                }
                Ok(())
            },
        );
        (
            verdict,
            checks.load(Ordering::Relaxed),
            super::super::pipeline::batch_width(),
        )
    })
}

/// hsync-9: a full batch whose first header fails Equihash runs at most one
/// batch width of Equihash checks, not all 4,000.
#[test]
fn an_invalid_first_header_runs_at_most_one_batch_of_equihash_checks() {
    let (_, lease, anchor) = fixture();
    let valid = child(lease.parent, &anchor, 1);
    let invalid = child(lease.parent, &anchor, 2);
    let mut headers = vec![valid; crate::MAX_HEADERS_PER_TRANSITION_V1];
    headers[0] = invalid.clone();

    let (verdict, checks, width) = prepare_counting(&headers, *invalid);

    assert!(matches!(
        verdict,
        Err(HeaderFailure::Invalid {
            offset: 0,
            rule: HeaderRule::Equihash,
            ..
        })
    ));
    assert!(width < headers.len(), "the page spans several batches");
    assert!(
        (1..=width).contains(&checks),
        "{checks} checks, width {width}"
    );
}

/// hsync-9: the verdict names the lowest failing offset and its rule, as a
/// run over every header does. Every header below the failure still runs,
/// and no batch after it starts.
#[test]
fn the_lowest_failing_offset_wins_across_batches() {
    let (_, lease, anchor) = fixture();
    let valid = child(lease.parent, &anchor, 1);
    let rejected = child(lease.parent, &anchor, 2);
    let mut bad_target = *valid;
    // A negative compact target has no expanded form.
    bad_target.difficulty_threshold = CompactDifficulty::from_le_bytes([0xff; 4]);
    let mut headers = vec![valid; crate::MAX_HEADERS_PER_TRANSITION_V1];
    let (.., width) = prepare_counting(&headers, *rejected);
    // A compact-target failure in the second batch, and Equihash failures at
    // higher offsets in the same and the last batch.
    let target_offset = width + width / 2;
    headers[target_offset] = Arc::new(bad_target);
    headers[target_offset + 1] = rejected.clone();
    *headers.last_mut().expect("the page is full") = rejected.clone();

    let (verdict, checks, _) = prepare_counting(&headers, *rejected);

    assert!(matches!(
        verdict,
        Err(HeaderFailure::Invalid {
            offset,
            rule: HeaderRule::CompactTarget,
            ..
        }) if offset == target_offset
    ));
    // Each header below the failure, and at most the rest of its batch.
    assert!(
        (target_offset..2 * width).contains(&checks),
        "{checks} checks, width {width}"
    );
}
