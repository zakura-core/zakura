//! Runtime-owner histories over the real queue and an independent funding ledger.
//!
//! Decoder 1 reads at most 128 quadruples without padding the final input.
//! The first byte selects one of 25 operations. The remaining bytes select
//! bounded heights, request/receipt indices, ranges and estimates.
//! Operations cover extend, take, admit, publish, writer start/completion,
//! owner drop/cancellation, matched/competing/claimed receipt, receipt drop,
//! return, release, defer, estimate refresh, floor advance, reset, authority
//! refresh, range retention, clone/drop-clone, delivery failure and audit.
//! Operation 24 injects a funding-pool discrepancy while retaining the displaced
//! reservation. This explicit fault tests containment; it is not a peer action.
//! A final drain observes zero remaining charge before the queue is destroyed.

use std::sync::Arc;

use zakura_rt::sync::CancellationToken;

use super::*;
use crate::zakura::{
    block_sync::work_queue::{
        request_write::{RequestWritePhase, UnpublishedRequest},
        RequestWrite, TakenRequest, WorkQueue,
    },
    transport::{ByteBudget, ByteReservation, FrameWriteClaim},
};

const LIMIT: u64 = 1_000;
const PROPERTY: &str = "block/work-funding";

/// A request's retained runtime owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FundingRequestPhase {
    /// Provisional work without funding.
    Taken,
    /// Funding acquired before publication.
    Admitted,
    /// Published request awaiting the writer.
    Queued,
    /// Writer authorized to send the request.
    Started,
    /// Writer completed the request.
    Written,
    /// Request retired before the writer started.
    Skipped,
    /// No request owner remains.
    Dropped,
}

/// One request's actual or independently predicted retention.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FundingRequestView {
    /// Exact owner assigned by the take.
    pub owner: Option<BodyWorkOwner>,
    /// Heights retained by the request.
    pub heights: Vec<u32>,
    /// Request lifetime phase.
    pub phase: FundingRequestPhase,
    /// Strong request owners retained by the driver.
    pub copies: usize,
    /// Funding retained before publication.
    pub unpublished_bytes: u64,
    /// Whether the request's session token was cancelled.
    pub cancelled: bool,
}

/// A receipt reservation retained independently of the queue.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FundingReceiptView {
    /// Owner whose queue charge moved to this receipt.
    pub owner: BodyWorkOwner,
    /// Retained bytes, or zero after the receipt was dropped.
    pub bytes: u64,
}

/// Actual resource reads or independent model predictions at one boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkFundingView {
    /// Logical queue state.
    pub queue: WorkOwnershipView,
    /// Bytes owned by the queue's pooled reservation.
    pub funded: u64,
    /// The shared budget's actual retained charge.
    pub reserved: u64,
    /// Capacity currently available to ordinary admission.
    pub available: u64,
    /// Funding retained outside the pool after an explicit corruption injection.
    pub quarantined: u64,
    /// Whether accounting permanently closed admission.
    pub closed: bool,
    /// Request owners, including stale generations.
    pub requests: Vec<FundingRequestView>,
    /// Receipt owners, including receipts retained through reset.
    pub receipts: Vec<FundingReceiptView>,
}

/// One runtime-owner operation and its evidence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkFundingStep {
    /// One-based transition number.
    pub step: usize,
    /// Decoder-v1 quadruple; empty only for the final explicit drain.
    pub input: Vec<u8>,
    /// Whether this step drains all driver-owned resources before queue destruction.
    pub drain: bool,
    /// Independent reads of actual queue and reservation owners.
    pub actual: WorkFundingView,
    /// Predictions from the independent model.
    pub expected: WorkFundingView,
    /// First failed transition or invariant.
    pub failure: Option<WorkOwnershipFailure>,
}

/// Bounded semantic history from the runtime-owner driver.
#[derive(Debug)]
pub struct WorkFundingHistory {
    /// Every completed transition, including the final drain or first failure.
    pub steps: Vec<WorkFundingStep>,
}

enum RetainedRequest {
    Taken(TakenRequest),
    Admitted(UnpublishedRequest),
    Published(Arc<RequestWrite>),
}

struct ActualRequest {
    owner: Option<BodyWorkOwner>,
    heights: Vec<u32>,
    retained: Option<RetainedRequest>,
    clones: Vec<Arc<RequestWrite>>,
    cancel: CancellationToken,
}

impl ActualRequest {
    fn published(&self) -> Option<&Arc<RequestWrite>> {
        match &self.retained {
            Some(RetainedRequest::Published(claim)) => Some(claim),
            _ => self.clones.first(),
        }
    }

    fn view(&self) -> FundingRequestView {
        let (phase, copies, unpublished_bytes) = match &self.retained {
            Some(RetainedRequest::Taken(_)) => (FundingRequestPhase::Taken, 1, 0),
            Some(RetainedRequest::Admitted(request)) => {
                (FundingRequestPhase::Admitted, 1, request.bytes())
            }
            _ => match self.published() {
                Some(claim) => {
                    let phase = match claim.status().phase() {
                        RequestWritePhase::Unpublished => {
                            unreachable!("published owner has a write phase")
                        }
                        RequestWritePhase::Queued => FundingRequestPhase::Queued,
                        RequestWritePhase::Started => FundingRequestPhase::Started,
                        RequestWritePhase::Written => FundingRequestPhase::Written,
                        RequestWritePhase::Skipped => FundingRequestPhase::Skipped,
                    };
                    (phase, Arc::strong_count(claim), 0)
                }
                None => (FundingRequestPhase::Dropped, 0, 0),
            },
        };
        let (owner, heights) = match &self.retained {
            Some(RetainedRequest::Taken(taken)) => (
                taken
                    .items()
                    .first()
                    .and_then(|(_, item)| item.owner)
                    .or(self.owner),
                taken.items().iter().map(|(raw, _)| raw.0).collect(),
            ),
            Some(RetainedRequest::Admitted(request)) => (
                Some(request.owner()),
                request.items().iter().map(|(raw, _)| raw.0).collect(),
            ),
            _ => self.published().map_or_else(
                || (self.owner, self.heights.clone()),
                |claim| {
                    (
                        Some(claim.owner()),
                        claim.heights().map(|raw| raw.0).collect(),
                    )
                },
            ),
        };
        FundingRequestView {
            owner,
            heights,
            phase,
            copies,
            unpublished_bytes,
            cancelled: self.cancel.is_cancelled(),
        }
    }
}

struct ExpectedRequest {
    view: FundingRequestView,
    primary: bool,
    registered: bool,
    /// Admission uses the estimates captured by the take, not later retry hints.
    taken_estimates: Vec<u64>,
}

#[derive(Default)]
struct Estimate {
    observed: u64,
    retry: Option<u64>,
    retry_after: Option<u32>,
}

struct Driver {
    work: Arc<WorkQueue>,
    budget: ByteBudget,
    requests: Vec<ActualRequest>,
    receipts: Vec<(BodyWorkOwner, Option<ByteReservation>)>,
    model: Model,
    expected_requests: Vec<ExpectedRequest>,
    expected_receipts: Vec<FundingReceiptView>,
    estimates: BTreeMap<u32, Estimate>,
    quarantine: Vec<ByteReservation>,
    expected_quarantine: u64,
    closed: bool,
}

impl Driver {
    fn new() -> Self {
        let budget = ByteBudget::new(LIMIT);
        let work = Arc::new(WorkQueue::new(height(0), budget.clone()));
        work.lock().state.set_estimate_floor(1);
        Self {
            work,
            budget,
            requests: Vec::new(),
            receipts: Vec::new(),
            model: Model::default(),
            expected_requests: Vec::new(),
            expected_receipts: Vec::new(),
            estimates: BTreeMap::new(),
            quarantine: Vec::new(),
            expected_quarantine: 0,
            closed: false,
        }
    }

    fn actual(&self) -> WorkFundingView {
        let inner = self.work.lock();
        WorkFundingView {
            queue: actual_view(&inner.state),
            funded: inner.funding.bytes(),
            reserved: self.budget.reserved(),
            available: self.budget.available(),
            quarantined: self.quarantine.iter().map(ByteReservation::bytes).sum(),
            closed: self.budget.is_closed(),
            requests: self.requests.iter().map(ActualRequest::view).collect(),
            receipts: self
                .receipts
                .iter()
                .map(|(owner, receipt)| FundingReceiptView {
                    owner: *owner,
                    bytes: receipt.as_ref().map_or(0, ByteReservation::bytes),
                })
                .collect(),
        }
    }

    fn expected(&self) -> WorkFundingView {
        let queue = expected_view(&self.model);
        let requests: Vec<_> = self
            .expected_requests
            .iter()
            .map(|r| r.view.clone())
            .collect();
        let receipts = self.expected_receipts.clone();
        let funded = if self.closed { 0 } else { queue.reserved };
        let reserved = funded
            + self.expected_quarantine
            + requests.iter().map(|r| r.unpublished_bytes).sum::<u64>()
            + receipts.iter().map(|r| r.bytes).sum::<u64>();
        WorkFundingView {
            funded,
            reserved,
            available: if self.closed {
                0
            } else {
                LIMIT.saturating_sub(reserved)
            },
            quarantined: self.expected_quarantine,
            queue,
            closed: self.closed,
            requests,
            receipts,
        }
    }

    fn refresh_returned_estimates(&mut self) {
        self.estimates
            .retain(|raw, _| self.model.entries.contains_key(raw));
        for (raw, entry) in &mut self.model.entries {
            let estimate = self.estimates.entry(*raw).or_default();
            if !entry.in_flight {
                if let Some(retry) = estimate.retry.take() {
                    entry.estimate = retry.max(estimate.observed);
                }
            }
        }
    }

    /// The writer can skip a queued request as a whole when one height retires.
    fn return_model(&mut self, index: usize, mut heights: BTreeSet<u32>) {
        let request = &mut self.expected_requests[index];
        if request.registered {
            if request.view.phase == FundingRequestPhase::Queued {
                request.view.phase = FundingRequestPhase::Skipped;
            }
            if request.view.phase == FundingRequestPhase::Skipped && request.view.copies > 0 {
                heights.extend(request.view.heights.iter().copied());
            }
        }
        if let Some(owner) = request.view.owner {
            self.model.return_owned(owner, &heights);
        }
        self.refresh_returned_estimates();
    }

    fn rollback_model(&mut self, index: usize) {
        let request = &mut self.expected_requests[index];
        if let Some(owner) = request.view.owner {
            self.model.return_unpublished(owner, &request.view.heights);
        }
        request.view.phase = FundingRequestPhase::Dropped;
        request.view.copies = 0;
        request.view.unpublished_bytes = 0;
        request.primary = false;
        self.refresh_returned_estimates();
    }

    fn drop_model_copy(&mut self, index: usize, primary: bool) {
        let request = &mut self.expected_requests[index];
        if primary {
            if !request.primary {
                return;
            }
            request.primary = false;
        } else if request.view.copies <= usize::from(request.primary) {
            return;
        }
        request.view.copies -= 1;
        if request.view.copies != 0 {
            return;
        }
        match request.view.phase {
            FundingRequestPhase::Taken | FundingRequestPhase::Admitted => {
                self.rollback_model(index)
            }
            FundingRequestPhase::Written => {}
            FundingRequestPhase::Dropped => unreachable!("a dropped owner has no copies"),
            FundingRequestPhase::Queued
            | FundingRequestPhase::Started
            | FundingRequestPhase::Skipped => {
                if request.view.phase == FundingRequestPhase::Started {
                    request.view.cancelled = true;
                }
                let heights = request.view.heights.iter().copied().collect();
                self.return_model(index, heights);
            }
        }
        let request = &mut self.expected_requests[index];
        request.view.phase = FundingRequestPhase::Dropped;
        request.registered = false;
    }

    fn take(&mut self, a: u8, b: u8, c: u8, d: u8) {
        let low = u32::from(b) % MAX_HEIGHT + 1;
        let high = (low + u32::from(c % 8)).min(MAX_HEIGHT);
        let count = usize::from(c / 8 % 6) + 1;
        let bytes = u64::from(d) * 100;
        let session = u64::from(a / 25 % 3);
        let id = NonZeroU64::new(u64::try_from(self.requests.len() + 1).unwrap()).unwrap();
        let eligible_high = self
            .model
            .entries
            .range(low..=high)
            .find(|(raw, entry)| {
                !entry.in_flight
                    && self
                        .estimates
                        .get(raw)
                        .and_then(|estimate| estimate.retry_after)
                        .is_some_and(|tip| self.model.floor <= tip)
            })
            .map_or(high, |(raw, _)| raw - 1);
        let heights = if eligible_high < low {
            Vec::new()
        } else {
            self.model
                .take(low, eligible_high, count, bytes, (session, id))
        };
        let owner = heights
            .first()
            .and_then(|raw| self.model.entries[raw].owner);
        let taken_estimates = heights
            .iter()
            .map(|raw| self.model.entries[raw].estimate)
            .collect();
        self.expected_requests.push(ExpectedRequest {
            view: FundingRequestView {
                owner,
                heights,
                phase: FundingRequestPhase::Taken,
                copies: 1,
                unpublished_bytes: 0,
                cancelled: false,
            },
            primary: true,
            registered: false,
            taken_estimates,
        });
        let taken = self
            .work
            .take_request(height(low), height(high), count, bytes, session, id);
        self.requests.push(ActualRequest {
            owner: taken.items().first().and_then(|(_, item)| item.owner),
            heights: taken.items().iter().map(|(raw, _)| raw.0).collect(),
            retained: Some(RetainedRequest::Taken(taken)),
            clones: Vec::new(),
            cancel: CancellationToken::new(),
        });
    }

    fn admit(&mut self, index: usize, floor: u32) -> Result<(), String> {
        if self.expected_requests[index].view.phase != FundingRequestPhase::Taken {
            return Ok(());
        }
        let current = &self.expected_requests[index];
        let expected = if current.view.heights.is_empty() {
            Err("Empty")
        } else if self.closed {
            Err("AccountingClosed")
        } else if current.view.cancelled {
            Err("Cancelled")
        } else if !self
            .model
            .is_unpublished(current.view.owner.unwrap(), &current.view.heights)
        {
            Err("StaleClaim")
        } else if self.expected().reserved + current.taken_estimates.iter().sum::<u64>() > LIMIT
            && current.view.heights[0] > floor + 1
        {
            Err("Capacity")
        } else {
            Ok(())
        };
        if expected.is_ok() {
            let current = &mut self.expected_requests[index];
            current.view.phase = FundingRequestPhase::Admitted;
            current.view.unpublished_bytes = current.taken_estimates.iter().sum::<u64>();
        } else {
            self.rollback_model(index);
        }
        let Some(RetainedRequest::Taken(taken)) = self.requests[index].retained.take() else {
            return Err("admission lost its actual take".into());
        };
        let actual = match taken.admit(height(floor), self.requests[index].cancel.clone()) {
            Ok(request) => {
                self.requests[index].retained = Some(RetainedRequest::Admitted(request));
                Ok(())
            }
            Err(reason) => Err(format!("{reason:?}")),
        };
        equal(actual, expected.map_err(str::to_owned), "admission outcome")
    }

    fn publish(&mut self, index: usize) -> Result<(), String> {
        if self.expected_requests[index].view.phase != FundingRequestPhase::Admitted {
            return Ok(());
        }
        let request = &self.expected_requests[index];
        let expected = !self.closed
            && !request.view.cancelled
            && self
                .model
                .is_unpublished(request.view.owner.unwrap(), &request.view.heights);
        if expected {
            let request = &mut self.expected_requests[index];
            self.model
                .publish(request.view.owner.unwrap(), &request.view.heights);
            request.view.phase = FundingRequestPhase::Queued;
            request.view.unpublished_bytes = 0;
            request.registered = true;
        } else {
            self.rollback_model(index);
        }
        let Some(RetainedRequest::Admitted(request)) = self.requests[index].retained.take() else {
            return Err("publication lost its actual owner".into());
        };
        let published = request.publish(|_| {});
        let actual = published.is_some();
        self.requests[index].retained = published.map(RetainedRequest::Published);
        equal(actual, expected, "publication outcome")
    }

    fn start(&mut self, index: usize) -> Result<(), String> {
        if self.requests[index].published().is_none() {
            return Ok(());
        }
        let request = &mut self.expected_requests[index];
        let current = !self.closed
            && !request.view.cancelled
            && self
                .model
                .is_published(request.view.owner.unwrap(), &request.view.heights);
        let expected = current && request.view.phase == FundingRequestPhase::Queued;
        if expected {
            request.view.phase = FundingRequestPhase::Started;
        } else if !current && request.view.phase == FundingRequestPhase::Queued {
            request.view.phase = FundingRequestPhase::Skipped;
        }
        equal(
            self.requests[index].published().unwrap().try_start(),
            expected,
            "writer authorization",
        )
    }

    fn receipt(&mut self, index: usize, at: usize, kind: u8) -> Result<(), String> {
        let request = &self.expected_requests[index];
        let Some(owner) = request.view.owner else {
            return Ok(());
        };
        if request.view.heights.is_empty() {
            return Ok(());
        }
        let raw = request.view.heights[at % request.view.heights.len()];
        let before = expected_view(&self.model).reserved;
        let mut accepted = self.model.receive(owner, raw).is_some();
        if kind == 10 {
            accepted = self
                .model
                .entries
                .get(&raw)
                .is_some_and(|entry| entry.owner == Some(owner));
        }
        if accepted {
            self.return_model(index, BTreeSet::new());
        }
        let bytes = before - expected_view(&self.model).reserved;
        let actual = match kind {
            8 => self
                .work
                .begin_receipt_for_owner(owner, height(raw))
                .map(|r| r.map(|r| r.funding)),
            9 => self
                .work
                .begin_competing_receipt(owner, height(raw))
                .map(|r| r.map(|r| r.funding)),
            _ => self
                .work
                .claim_receipt_for_owner(owner, height(raw))
                .map(Some),
        };
        let actual = match actual {
            Err(error) if self.closed && bytes > 0 => {
                return equal(
                    format!("{error:?}"),
                    "Amount".to_owned(),
                    "quarantined receipt",
                );
            }
            Err(error) => return Err(format!("receipt transfer failed: {error:?}")),
            Ok(_) if self.closed && bytes > 0 => {
                return Err("receipt escaped an empty quarantined pool".into())
            }
            Ok(actual) => actual,
        };
        equal(
            actual.is_some(),
            accepted || kind == 10,
            "receipt acceptance",
        )?;
        if let Some(funding) = actual {
            self.receipts.push((owner, Some(funding)));
            self.expected_receipts
                .push(FundingReceiptView { owner, bytes });
        }
        Ok(())
    }

    fn apply(&mut self, input: &[u8]) -> Result<(), String> {
        let a = input[0];
        let b = input.get(1).copied().unwrap_or(0);
        let c = input.get(2).copied().unwrap_or(0);
        let d = input.get(3).copied().unwrap_or(0);
        let op = a % 25;
        let low = u32::from(b) % MAX_HEIGHT + 1;
        match op {
            0 => {
                let len = u32::from(c % 8);
                let size = u64::from(d) + 1;
                self.model.extend(low, len, size);
                for (raw, entry) in &mut self.model.entries {
                    if !entry.in_flight {
                        entry.estimate = entry
                            .estimate
                            .max(self.estimates.entry(*raw).or_default().observed);
                    }
                }
                self.work.extend(
                    self.model.authority(),
                    (low..=(low + len).min(MAX_HEIGHT))
                        .map(|raw| (height(raw), hash(raw), confirmed(size))),
                );
                return Ok(());
            }
            1 => {
                self.take(a, b, c, d);
                return Ok(());
            }
            11 => {
                if !self.receipts.is_empty() {
                    let index = usize::from(b) % self.receipts.len();
                    self.expected_receipts[index].bytes = 0;
                    drop(self.receipts[index].1.take());
                }
                return Ok(());
            }
            15 => {
                let size = u64::from(d) + 1;
                let scope = self.model.authority();
                if let Some(entry) = self.model.entries.get_mut(&low) {
                    let estimate = self.estimates.entry(low).or_default();
                    if entry.in_flight {
                        estimate.retry = Some(size.max(estimate.observed));
                    } else if entry.scope == scope {
                        entry.estimate = size.max(estimate.observed);
                    }
                }
                self.work.refresh_size_estimates(
                    self.model.authority(),
                    [(height(low), hash(low), confirmed(size))],
                );
                return Ok(());
            }
            16 | 17 => {
                let floor = u32::from(b) % (MAX_HEIGHT + 1);
                if op == 16 {
                    self.model.advance_floor(floor);
                    self.work.advance_floor(height(floor));
                } else {
                    for request in &mut self.expected_requests {
                        if request.registered && request.view.heights.iter().any(|raw| *raw > floor)
                        {
                            if request.view.phase == FundingRequestPhase::Queued {
                                request.view.phase = FundingRequestPhase::Skipped;
                            }
                            if request.view.phase == FundingRequestPhase::Started {
                                request.view.cancelled = true;
                            }
                            request.registered = false;
                        }
                    }
                    self.model.reset_above(floor);
                    self.work.reset_above(height(floor));
                }
                self.refresh_returned_estimates();
                return Ok(());
            }
            18 => {
                self.model.refresh_authority();
                self.work.refresh_authority(self.model.authority());
                return Ok(());
            }
            23 => {
                return equal(self.work.audit_funding(), !self.closed, "funding audit");
            }
            24 => {
                // Explicit fault input: move the real pool aside without changing
                // its ledger. The production audit must close admission while
                // the driver retains that funding for teardown verification.
                if !self.closed && expected_view(&self.model).reserved > 0 {
                    self.expected_quarantine = expected_view(&self.model).reserved;
                    self.closed = true;
                    let funding = {
                        let mut inner = self.work.lock();
                        std::mem::replace(&mut inner.funding, self.budget.empty_reservation())
                    };
                    self.quarantine.push(funding);
                    return equal(self.work.audit_funding(), false, "drift closes admission");
                }
                return Ok(());
            }
            _ => {}
        }
        if self.requests.is_empty() {
            return Ok(());
        }
        let index = usize::from(b) % self.requests.len();
        match op {
            2 => self.admit(index, u32::from(c) % (MAX_HEIGHT + 1))?,
            3 => self.publish(index)?,
            4 => self.start(index)?,
            5 => {
                if self.expected_requests[index].view.phase == FundingRequestPhase::Started {
                    self.expected_requests[index].view.phase = FundingRequestPhase::Written;
                    self.expected_requests[index].registered = false;
                    self.requests[index]
                        .published()
                        .ok_or("writer lost its owner")?
                        .written();
                }
            }
            6 => {
                self.drop_model_copy(index, true);
                drop(self.requests[index].retained.take());
            }
            7 => {
                self.expected_requests[index].view.cancelled = true;
                self.requests[index].cancel.cancel();
            }
            8..=10 => self.receipt(index, usize::from(c), op)?,
            12..=14 => {
                let request = &self.expected_requests[index];
                let Some(owner) = request.view.owner else {
                    return Ok(());
                };
                if request.view.heights.is_empty() {
                    return Ok(());
                }
                let raw = request.view.heights[usize::from(c) % request.view.heights.len()];
                match op {
                    12 => {
                        let heights: BTreeSet<_> = if d % 2 == 0 {
                            BTreeSet::from([raw])
                        } else {
                            request.view.heights.iter().copied().collect()
                        };
                        self.return_model(index, heights.clone());
                        self.work
                            .release_reserved_and_return_items_detailed_for_owner(
                                owner,
                                heights.into_iter().map(height),
                            );
                    }
                    13 => {
                        self.model.receive(owner, raw);
                        self.work
                            .release_reserved_heights_for_owner(owner, [height(raw)]);
                    }
                    _ => {
                        let expected = self.model.entries.get(&raw).is_some_and(|entry| {
                            entry.in_flight
                                && entry.owner == Some(owner)
                                && entry.charge == 0
                                && raw > self.model.floor
                        });
                        if expected {
                            let estimate = self.estimates.entry(raw).or_default();
                            estimate.observed = estimate.observed.max(u64::from(d) + 1);
                            estimate.retry_after = Some(self.model.floor);
                            let entry = self.model.entries.get_mut(&raw).unwrap();
                            entry.estimate = estimate
                                .retry
                                .take()
                                .unwrap_or(entry.estimate)
                                .max(estimate.observed);
                            entry.in_flight = false;
                            entry.owner = None;
                        }
                        equal(
                            self.work.defer_received_for_owner(
                                owner,
                                height(raw),
                                hash(raw),
                                u64::from(d) + 1,
                                height(self.model.floor),
                            ),
                            expected,
                            "deferred receipt",
                        )?;
                    }
                }
            }
            19 => {
                let request = &mut self.expected_requests[index];
                if request.view.phase == FundingRequestPhase::Taken {
                    let len = request.view.heights.len();
                    let start = usize::from(c) % (len + 1);
                    let end = start + usize::from(d) % (len - start + 1);
                    if let Some(owner) = request.view.owner {
                        self.model
                            .return_unpublished(owner, &request.view.heights[..start]);
                        self.model
                            .return_unpublished(owner, &request.view.heights[end..]);
                    }
                    request.view.heights = request.view.heights[start..end].to_vec();
                    request.taken_estimates = request.taken_estimates[start..end].to_vec();
                    let Some(RetainedRequest::Taken(taken)) = &mut self.requests[index].retained
                    else {
                        return Err("retained range lost its take".into());
                    };
                    taken.retain_range(start..end);
                    self.requests[index].heights =
                        taken.items().iter().map(|(raw, _)| raw.0).collect();
                    self.refresh_returned_estimates();
                }
            }
            20 => {
                if let Some(claim) = self.requests[index].published().cloned() {
                    self.requests[index].clones.push(claim);
                    self.expected_requests[index].view.copies += 1;
                }
            }
            21 => {
                self.drop_model_copy(index, false);
                drop(self.requests[index].clones.pop());
            }
            22 => {
                if self.requests[index].published().is_some() {
                    self.expected_requests[index].view.cancelled = true;
                    let heights = self.expected_requests[index]
                        .view
                        .heights
                        .iter()
                        .copied()
                        .collect();
                    self.return_model(index, heights);
                    self.requests[index].published().unwrap().delivery_failed();
                }
            }
            _ => unreachable!("all decoder operations have a transition"),
        }
        Ok(())
    }

    fn drain(&mut self) {
        for index in 0..self.requests.len() {
            self.drop_model_copy(index, true);
            drop(self.requests[index].retained.take());
            while !self.requests[index].clones.is_empty() {
                self.drop_model_copy(index, false);
                drop(self.requests[index].clones.pop());
            }
        }
        for ((_, receipt), expected) in self.receipts.iter_mut().zip(&mut self.expected_receipts) {
            expected.bytes = 0;
            drop(receipt.take());
        }
        self.model.advance_floor(MAX_HEIGHT);
        self.work.advance_floor(height(MAX_HEIGHT));
        self.expected_quarantine = 0;
        self.quarantine.clear();
    }
}

fn equal<T: std::fmt::Debug + PartialEq>(
    actual: T,
    expected: T,
    boundary: &str,
) -> Result<(), String> {
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "{PROPERTY}: {boundary}: expected {expected:?}; actual {actual:?}"
        ))
    }
}

fn compare_views(actual: &WorkFundingView, expected: &WorkFundingView) -> Result<(), String> {
    equal(actual.closed, expected.closed, "accounting closure")?;
    equal(actual.funded, expected.funded, "pooled funding")?;
    equal(actual.reserved, expected.reserved, "actual retained budget")?;
    equal(actual.available, expected.available, "available capacity")?;
    equal(
        actual.quarantined,
        expected.quarantined,
        "quarantined funding",
    )?;
    equal(actual.queue.floor, expected.queue.floor, "queue floor")?;
    equal(
        actual.queue.authority,
        expected.queue.authority,
        "queue authority",
    )?;
    equal(
        actual.queue.reserved,
        expected.queue.reserved,
        "logical charge",
    )?;
    equal(
        actual.queue.scanned,
        expected.queue.scanned,
        "scanned charge",
    )?;
    equal(
        actual.queue.above_floor,
        expected.queue.above_floor,
        "above-floor charge",
    )?;
    equal(
        actual.queue.items.len(),
        expected.queue.items.len(),
        "queue entries",
    )?;
    for (actual, expected) in actual.queue.items.iter().zip(&expected.queue.items) {
        equal(actual, expected, "queue item")?;
    }
    equal(
        actual.requests.len(),
        expected.requests.len(),
        "request records",
    )?;
    for (actual, expected) in actual.requests.iter().zip(&expected.requests) {
        equal(actual, expected, "request owner")?;
    }
    equal(
        actual.receipts.len(),
        expected.receipts.len(),
        "receipt records",
    )?;
    for (actual, expected) in actual.receipts.iter().zip(&expected.receipts) {
        equal(actual, expected, "receipt owner")?;
    }
    // Keep any future field visible until its specific comparison is added.
    equal(actual, expected, "runtime funding view")
}

/// Run decoder-v1 runtime-owner inputs and observe obligations before queue destruction.
/// This target is distinct from the preserved pure-state decoder and consumes at most 512 bytes.
pub fn trace_work_funding_bytes(data: &[u8]) -> WorkFundingHistory {
    let mut driver = Driver::new();
    let mut steps = Vec::new();
    let data = &data[..data.len().min(512)];
    for input in data.chunks(4).chain(std::iter::once(&[][..])) {
        let drain = input.is_empty();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if drain {
                driver.drain();
                Ok(())
            } else {
                driver.apply(input)
            }
        }));
        let actual = driver.actual();
        let expected = driver.expected();
        let failure = match result {
            Ok(result) => {
                let (phase, result) = match result {
                    Ok(()) => (
                        WorkOwnershipPhase::Invariant,
                        compare_views(&actual, &expected),
                    ),
                    Err(message) => (WorkOwnershipPhase::Transition, Err(message)),
                };
                result.err().map(|message| WorkOwnershipFailure {
                    property_failure: true,
                    phase,
                    message,
                })
            }
            Err(payload) => Some(WorkOwnershipFailure {
                property_failure: false,
                phase: WorkOwnershipPhase::Transition,
                message: payload
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_owned()))
                    .unwrap_or_else(|| "non-string panic payload".into()),
            }),
        };
        let failed = failure.is_some();
        steps.push(WorkFundingStep {
            step: steps.len() + 1,
            input: input.to_vec(),
            drain,
            actual,
            expected,
            failure,
        });
        if failed {
            break;
        }
    }
    WorkFundingHistory { steps }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(data: &[u8]) -> WorkFundingHistory {
        let history = trace_work_funding_bytes(data);
        if let Some(step) = history.steps.iter().find(|step| step.failure.is_some()) {
            panic!("{PROPERTY}: {:?}; input {data:?}", step.failure);
        }
        assert!(history.steps.last().unwrap().drain);
        assert_eq!(history.steps.last().unwrap().actual.reserved, 0);
        history
    }

    #[test]
    fn receipt_retains_funding_through_reset_and_replacement() {
        let history = check(&[
            0, 0, 1, 99, 1, 0, 9, 3, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0, 8, 0, 0, 0,
            17, 0, 0, 0, 0, 0, 1, 99, 1, 0, 9, 3, 2, 1, 0, 0, 3, 1, 0, 0, 12, 0, 0, 1, 11, 0, 0, 0,
        ]);
        assert_eq!(history.steps[6].actual.receipts[0].bytes, 100);
        assert_eq!(history.steps[7].actual.reserved, 100);
        assert_eq!(history.steps[11].actual.reserved, 300);
    }

    const PUBLISHED: &[u8] = &[0, 0, 3, 99, 1, 0, 27, 5, 2, 0, 0, 0, 3, 0, 0, 0];

    fn from_published(suffix: &[u8]) -> WorkFundingHistory {
        let mut data = PUBLISHED.to_vec();
        data.extend(suffix);
        check(&data)
    }

    #[test]
    fn queued_and_written_receipts_transfer_the_correct_funding() {
        for kind in [8, 9, 10] {
            let queued = from_published(&[kind, 0, 0, 0]);
            assert_eq!(queued.steps[4].actual.receipts[0].bytes, 400);
            assert_eq!(queued.steps[4].actual.funded, 0);
            let written = from_published(&[4, 0, 0, 0, 5, 0, 0, 0, kind, 0, 0, 0]);
            assert_eq!(written.steps[6].actual.receipts[0].bytes, 100);
            assert_eq!(written.steps[6].actual.funded, 300);
        }
    }

    #[test]
    fn last_request_clone_settles_and_released_heights_do_not_settle_twice() {
        let history = from_published(&[20, 0, 0, 0, 6, 0, 0, 0, 13, 0, 0, 0, 21, 0, 0, 0]);
        assert_eq!(history.steps[5].actual.reserved, 400);
        assert_eq!(history.steps[6].actual.reserved, 300);
        assert_eq!(history.steps[7].actual.reserved, 0);
    }

    #[test]
    fn deferred_receipt_uses_retry_estimates_without_recharging_its_old_owner() {
        let history = from_published(&[
            4, 0, 0, 0, 5, 0, 0, 0, 8, 0, 3, 0, 15, 3, 0, 149, 14, 0, 3, 199, 1, 3, 0, 5, 16, 1, 0,
            0, 1, 3, 0, 5, 2, 2, 1, 0,
        ]);
        assert_eq!(
            history.steps[8].actual.queue.items.last().unwrap().estimate,
            200
        );
        assert!(history.steps[9].actual.requests[1].heights.is_empty());
        assert_eq!(history.steps[12].actual.reserved, 500);
    }

    #[test]
    fn accounting_closure_quarantines_funding_through_rejected_replacement() {
        let history = from_published(&[
            4, 0, 0, 0, 5, 0, 0, 0, 8, 0, 0, 0, 24, 0, 0, 0, 17, 0, 0, 0, 0, 0, 0, 99, 1, 0, 0, 1,
            2, 1, 0, 0, 11, 0, 0, 0,
        ]);
        let closed = &history.steps[7].actual;
        assert!(closed.closed);
        assert_eq!(closed.quarantined, 300);
        assert_eq!(closed.reserved, 400);
        assert_eq!(
            history.steps[11].actual.requests[1].phase,
            FundingRequestPhase::Dropped
        );
        assert_eq!(history.steps[12].actual.reserved, 300);
        assert_eq!(history.steps.last().unwrap().actual.quarantined, 0);
    }

    #[test]
    fn stale_provisional_ranges_keep_their_take_estimates() {
        check(&[
            0, 0, 3, 99, 1, 0, 27, 5, 17, 0, 0, 0, 19, 0, 1, 2, 2, 0, 0, 0,
        ]);
    }

    #[test]
    fn stale_receipt_preserves_an_equal_charge_replacement() {
        let history = check(&[
            0, 0, 0, 99, 1, 0, 0, 1, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0, 17, 0, 0, 0,
            0, 0, 0, 99, 1, 0, 0, 1, 2, 1, 0, 0, 3, 1, 0, 0, 4, 1, 0, 0, 5, 1, 0, 0, 8, 0, 0, 0,
            12, 0, 0, 1, 6, 0, 0, 0,
        ]);
        for step in &history.steps[13..16] {
            assert_eq!(step.actual.reserved, 100);
            assert!(step.actual.receipts.is_empty());
        }
    }

    #[test]
    fn admission_refusal_floor_exception_and_cancelled_publication_keep_exact_charge() {
        let history = from_published(&[
            0, 4, 3, 249, 1, 4, 27, 20, 2, 1, 0, 0, 1, 4, 27, 20, 2, 2, 4, 0, 7, 2, 0, 0, 3, 2, 0,
            0,
        ]);
        assert_eq!(history.steps[6].actual.reserved, 400);
        assert_eq!(history.steps[8].actual.reserved, 1400);
        assert_eq!(history.steps[9].actual.reserved, 1400);
        assert_eq!(history.steps[10].actual.reserved, 400);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]
        #[test]
        fn runtime_funding_matches_independent_owners(data in prop::collection::vec(any::<u8>(), 0..=512)) {
            check(&data);
            for written in [false, true] {
                let mut seeded = vec![0, 0, 3, 99, 1, 0, 27, 5, 2, 0, 0, 0, 3, 0, 0, 0];
                if written { seeded.extend([4, 0, 0, 0, 5, 0, 0, 0]); }
                seeded.extend(&data);
                check(&seeded);
            }
        }
    }
}
