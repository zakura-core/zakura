//! Incremental legacy block discovery. The stream owns and cancels its peer requests.

use std::collections::{HashSet, VecDeque};

use eyre::{eyre, Report};
use futures::{
    future::{BoxFuture, FutureExt},
    stream::{self, BoxStream, FuturesUnordered, StreamExt},
};
use indexmap::IndexSet;
use tower::{timeout::Timeout, Service, ServiceExt};

use zakura_chain::block;
use zakura_network as zn;
use zakura_state as zs;

use super::{has_valid_tips_response_hash_count, CheckedTip, FANOUT, TIPS_RESPONSE_TIMEOUT};
use crate::BoxError;

/// A reply can discover a chain suffix, supply an unanchored announcement, or finish the round.
pub(super) enum Event {
    Batch(IndexSet<block::Hash>),
    Hint(block::Hash),
    Finished(HashSet<CheckedTip>),
}

pub(super) type Discovery = BoxStream<'static, Result<Event, Report>>;

type Reply = (Option<CheckedTip>, Result<zn::Response, BoxError>);

struct Round<ZN, ZS> {
    network: Timeout<ZN>,
    state: ZS,
    locators: VecDeque<(Vec<block::Hash>, Option<CheckedTip>)>,
    requests: FuturesUnordered<BoxFuture<'static, Reply>>,
    accepted: IndexSet<block::Hash>,
    tips: HashSet<CheckedTip>,
    is_regtest: bool,
    finished: bool,
    stage: &'static str,
    started: std::time::Instant,
}

/// Starts one bounded fanout at a time and yields each completely validated reply immediately.
///
/// Accepted hashes remain in the round even after their blocks commit, so later overlapping
/// replies are checked against the same discovery history rather than the advancing state tip.
pub(super) fn discover<ZN, ZS>(
    network: Timeout<ZN>,
    state: ZS,
    locators: Vec<(Vec<block::Hash>, Option<CheckedTip>)>,
    is_regtest: bool,
) -> Discovery
where
    ZN: Service<zn::Request, Response = zn::Response, Error = BoxError> + Send + Clone + 'static,
    ZN::Future: Send,
    ZS: Service<zs::Request, Response = zs::Response, Error = BoxError> + Send + Clone + 'static,
    ZS::Future: Send,
{
    let stage = if locators.first().is_some_and(|(_, tip)| tip.is_none()) {
        "obtain_tips"
    } else {
        "extend_tips"
    };
    let round = Round {
        network,
        state,
        locators: locators.into(),
        requests: FuturesUnordered::new(),
        accepted: IndexSet::new(),
        tips: HashSet::new(),
        is_regtest,
        finished: false,
        stage,
        started: std::time::Instant::now(),
    };
    stream::try_unfold(round, |mut round| async move {
        if round.finished {
            return Ok(None);
        }
        loop {
            if round.requests.is_empty() {
                let Some((known_blocks, expected)) = round.locators.pop_front() else {
                    round.finished = true;
                    metrics::histogram!("sync.stage.duration_seconds", "stage" => round.stage)
                        .record(round.started.elapsed().as_secs_f64());
                    let tips = std::mem::take(&mut round.tips);
                    return Ok(Some((Event::Finished(tips), round)));
                };
                for _ in 0..FANOUT {
                    let network = round.network.clone();
                    let known_blocks = known_blocks.clone();
                    round.requests.push(
                        async move {
                            let response = tokio::time::timeout(
                                TIPS_RESPONSE_TIMEOUT,
                                network.oneshot(zn::Request::FindBlocks {
                                    known_blocks,
                                    stop: None,
                                }),
                            )
                            .await
                            .map_err(BoxError::from)
                            .and_then(std::convert::identity);
                            (expected, response)
                        }
                        .boxed(),
                    );
                }
            }
            let (expected, response) = round.requests.next().await.expect("fanout is nonempty");
            let hashes = match response {
                Ok(zn::Response::BlockHashes(hashes)) => hashes,
                Ok(_) => unreachable!("FindBlocks only returns block hashes"),
                Err(error) => {
                    debug!(?error, "ignoring failed discovery request");
                    continue;
                }
            };
            let raw_hint = match (expected, hashes.as_slice()) {
                (Some(tip), [hash]) if *hash != tip.expected_next => Some(*hash),
                _ => None,
            };
            let Some(hashes) = round.validate(&hashes, expected).await? else {
                if let Some(hash) = raw_hint {
                    return Ok(Some((Event::Hint(hash), round)));
                }
                continue;
            };
            if let Some(end) = hashes.rchunks_exact(2).next() {
                let tip = CheckedTip {
                    tip: end[0],
                    expected_next: end[1],
                };
                if !round.accepted.contains(&tip.expected_next) {
                    round
                        .tips
                        .retain(|tip| !hashes.contains(&tip.expected_next));
                    round.tips.insert(tip);
                }
            }
            let mut batch = IndexSet::new();
            for hash in hashes {
                if round.accepted.insert(hash) {
                    batch.insert(hash);
                }
            }
            if !batch.is_empty() {
                let count = f64::from(
                    u32::try_from(batch.len()).expect("a reply is bounded to 500 hashes"),
                );
                let metric = if round.stage == "obtain_tips" {
                    "sync.obtain.response.hash.count"
                } else {
                    "sync.extend.response.hash.count"
                };
                metrics::histogram!(metric).record(count);
                return Ok(Some((Event::Batch(batch), round)));
            }
        }
    })
    .boxed()
}

impl<ZN, ZS> Round<ZN, ZS>
where
    ZS: Service<zs::Request, Response = zs::Response, Error = BoxError> + Send,
    ZS::Future: Send,
{
    async fn known_before_round(&mut self, hash: block::Hash) -> Result<bool, Report> {
        if self.accepted.contains(&hash) {
            return Ok(false);
        }
        match self
            .state
            .ready()
            .await
            .map_err(|e| eyre!(e))?
            .call(zs::Request::KnownBlock(hash))
            .await
            .map_err(|e| eyre!(e))?
        {
            zs::Response::KnownBlock(location) => Ok(location.is_some()),
            _ => unreachable!("KnownBlock only returns a known-block location"),
        }
    }

    async fn validate(
        &mut self,
        hashes: &[block::Hash],
        expected: Option<CheckedTip>,
    ) -> Result<Option<Vec<block::Hash>>, Report> {
        let hashes = if let Some(tip) = expected {
            let matched = match hashes {
                [first, ..] if *first == tip.expected_next => hashes,
                [_, second, ..] if *second == tip.expected_next => &hashes[1..],
                _ => return Ok(None),
            };
            let matched = match matched {
                [_, _] => matched,
                [rest @ .., _] => rest,
                [] => unreachable!("matched reply contains the expected hash"),
            };
            if matched.is_empty() {
                return Ok(None);
            }
            if !has_valid_tips_response_hash_count(&matched[1..]) {
                return Ok(None);
            }
            let mut seen = HashSet::new();
            if matched.iter().any(|hash| !seen.insert(*hash)) || matched[1..].contains(&tip.tip) {
                return Ok(None);
            }
            &matched[1..]
        } else if self.is_regtest {
            hashes
        } else {
            match hashes {
                [] | [_] => hashes,
                [rest @ .., _] => rest,
            }
        };
        if !has_valid_tips_response_hash_count(hashes) {
            return Ok(None);
        }
        let mut seen = HashSet::new();
        let mut first_unknown = None;
        for (index, &hash) in hashes.iter().enumerate() {
            if !seen.insert(hash) {
                return Ok(None);
            }
            let known = self.known_before_round(hash).await?;
            if known && (expected.is_some() || first_unknown.is_some()) {
                return Ok(None);
            }
            if !known && first_unknown.is_none() {
                first_unknown = Some(index);
            }
        }
        Ok(first_unknown.map(|index| hashes[index..].to_vec()))
    }
}
