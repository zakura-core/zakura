//! The engine-backed header port's VCT repair: the context a repair reads
//! and the checks its completion passes before it applies, as zakura-state
//! makes them.

use std::{
    future::Future,
    num::NonZeroU64,
    pin::pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use zakura_chain::block;
use zakura_header_chain::{
    BodyWorkAuthority, BodyWorkOwner, ErrorCategory, Frontier, HeaderSyncWorkOwner, SourceId,
    TargetCompletion, VctRepairContext,
};
use zakura_node_services::header_chain::{
    ApplyHeaderTargetOutcome, Port, PrepareHeaderTarget, VctRepairContextReply,
};
use zakura_storage::header::{adapter::EnginePort, fixture::HeaderFixture};

/// Polls `future` once. Every adapter operation runs when first polled.
fn ready<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("the adapter completes each operation on its first poll"),
    }
}

/// A port whose selected chain is the main chain's first `len` headers,
/// with no tree records, resolving repairs up to `ceiling`.
fn chain(len: u32, ceiling: u32) -> (EnginePort, Vec<Arc<block::Header>>) {
    let fixture = HeaderFixture::regtest();
    let port = EnginePort::new(&fixture).with_checkpoint_ceiling(block::Height(ceiling));
    let headers = fixture.extend(fixture.anchor(), len, 0);
    let request = fixture.target_request(&port.snapshot(), fixture.anchor(), &headers, 1);
    let prepared = ready(port.prepare_header_target(request)).expect("fixture headers prepare");
    assert_eq!(
        ready(port.apply_header_target(prepared)).expect("fixture headers apply"),
        ApplyHeaderTargetOutcome::Applied
    );
    (port, headers)
}

/// A repair owner bound to `port`'s current snapshot.
fn owner(port: &EnginePort) -> BodyWorkOwner {
    BodyWorkAuthority::for_snapshot(&port.snapshot())
        .bind(1, NonZeroU64::new(1).expect("one is nonzero"))
}

/// `port`'s repair context at `height` for a current owner.
fn context(port: &EnginePort, height: u32) -> VctRepairContextReply {
    ready(port.vct_repair_context(owner(port), block::Height(height))).expect("the context reads")
}

/// The resolved context at `height`.
fn resolved(port: &EnginePort, height: u32) -> VctRepairContext {
    match context(port, height) {
        VctRepairContextReply::Resolved(context) => context,
        VctRepairContextReply::Stale => panic!("the repair at {height} resolves"),
    }
}

/// The prepare request a supplier `source` answers `context` with: its
/// selected range, each header with the fixture's record.
fn repair_request(
    port: &EnginePort,
    context: &VctRepairContext,
    headers: &[Arc<block::Header>],
    source: SourceId,
) -> PrepareHeaderTarget {
    let fixture = HeaderFixture::regtest();
    let parent = context.locator.entries()[0];
    // Heights start at 1, so a header's index is its height less one. A
    // fixture height is far below usize::MAX.
    let first = (context.target.height.0 - 1) as usize;
    let batch = &headers[first..first + context.selected_header_count()];
    let mut request =
        fixture.with_tree_aux(fixture.target_request(&port.snapshot(), parent, batch, 1));
    request.source = source;
    request.owner = HeaderSyncWorkOwner::BodyRepair(owner(port));
    request.completion = TargetCompletion::SelectedAuxiliaryRepair {
        common_ancestor: parent,
        selected_target: context.request_target(),
        episode: context.episode,
    };
    request
}

/// No repair resolves past the checkpoint ceiling, which a new port keeps
/// at 0, as `zakurad` does on Regtest.
#[test]
fn no_repair_resolves_past_the_checkpoint_ceiling() {
    let fixture = HeaderFixture::regtest();
    let (raised, _) = chain(3, 3);
    let port = EnginePort::new(&fixture);
    let headers = fixture.extend(fixture.anchor(), 3, 0);
    let request = fixture.target_request(&port.snapshot(), fixture.anchor(), &headers, 1);
    let prepared = ready(port.prepare_header_target(request)).expect("fixture headers prepare");
    ready(port.apply_header_target(prepared)).expect("fixture headers apply");
    for height in 1..=3 {
        assert_eq!(context(&port, height), VctRepairContextReply::Stale);
        assert!(matches!(
            context(&raised, height),
            VctRepairContextReply::Resolved(_)
        ));
    }
    let (low, _) = chain(3, 1);
    assert!(matches!(
        context(&low, 1),
        VctRepairContextReply::Resolved(_)
    ));
    assert_eq!(context(&low, 2), VctRepairContextReply::Stale);
}

/// A repair resolves only for a current owner and a height above finality
/// and at most the selected tip.
#[test]
fn a_repair_resolves_only_for_a_current_owner_and_height() {
    let (port, _) = chain(3, 10);
    assert_eq!(context(&port, 0), VctRepairContextReply::Stale);
    assert_eq!(context(&port, 4), VctRepairContextReply::Stale);
    let stale = BodyWorkAuthority::for_snapshot(&chain(2, 10).0.snapshot())
        .bind(1, NonZeroU64::new(1).expect("one is nonzero"));
    assert_eq!(
        ready(port.vct_repair_context(stale, block::Height(2))).expect("the context reads"),
        VctRepairContextReply::Stale
    );
}

/// With no rows that constrain it, a repair's range runs from its height to
/// the selected tip, the way production extends it, and names the header
/// before it as its locator.
#[test]
fn an_unconstrained_repair_covers_the_selected_suffix() {
    let (port, headers) = chain(3, 10);
    let context = resolved(&port, 2);
    let frontier = |height: u32| {
        // A fixture height is far below usize::MAX.
        Frontier::new(block::Height(height), headers[(height - 1) as usize].hash())
    };
    assert_eq!(context.target, frontier(2));
    assert_eq!(context.locator.entries(), &[frontier(1)]);
    assert!(context.matches_selected_range(&[frontier(2), frontier(3)]));
    assert_eq!(context.request_target(), frontier(3));
    assert_eq!(context.state_version, port.snapshot().state_version);
}

/// A supplier's records for the repair's range apply. The commit adds only
/// auxiliary rows: it moves the state version but no header generation, so
/// the same repair can't apply twice, and the header's new rooted row
/// constrains the next repair there to that one header.
#[test]
fn a_repair_applies_its_records_once() {
    let (port, headers) = chain(3, 10);
    let before = port.snapshot();
    let context = resolved(&port, 2);
    let source = SourceId::from_digest([0x51; 32]);
    let request = repair_request(&port, &context, &headers, source);
    let prepared = ready(port.prepare_header_target(request.clone())).expect("the repair prepares");
    assert_eq!(
        ready(port.apply_header_target(prepared)).expect("the repair applies"),
        ApplyHeaderTargetOutcome::Applied
    );
    let after = port.snapshot();
    assert!(after.state_version > before.state_version);
    assert_eq!(after.header_generation, before.header_generation);
    assert_eq!(after.verified_generation, before.verified_generation);
    assert_eq!(after.frontiers, before.frontiers);

    let again = ready(port.prepare_header_target(request)).expect("the repeat prepares");
    let error = ready(port.apply_header_target(again)).expect_err("a repeat is stale");
    assert_eq!(error.category, ErrorCategory::StaleTargetOrGeneration);

    let next = resolved(&port, 2);
    assert_eq!(next.selected_header_count(), 1);
    assert_ne!(next.episode, context.episode);
}

/// A repair whose episode no longer matches the engine's is stale, as a
/// repair prepared before another one committed is.
#[test]
fn a_repair_from_an_older_episode_is_stale() {
    let (port, headers) = chain(3, 10);
    let context = resolved(&port, 2);
    let first = repair_request(&port, &context, &headers, SourceId::from_digest([0x51; 32]));
    let second = repair_request(&port, &context, &headers, SourceId::from_digest([0x52; 32]));
    let first = ready(port.prepare_header_target(first)).expect("the first repair prepares");
    let second = ready(port.prepare_header_target(second)).expect("the second repair prepares");
    assert_eq!(
        ready(port.apply_header_target(first)).expect("the first repair applies"),
        ApplyHeaderTargetOutcome::Applied
    );
    let error = ready(port.apply_header_target(second)).expect_err("the second is stale");
    assert_eq!(error.category, ErrorCategory::StaleTargetOrGeneration);
}

/// A repair that brings a header without its record is stale.
#[test]
fn a_repair_without_records_is_stale() {
    let (port, headers) = chain(3, 10);
    let context = resolved(&port, 2);
    let mut request = repair_request(&port, &context, &headers, SourceId::from_digest([0x51; 32]));
    let mut entries = request.entries.into_vec();
    entries[0].tree_aux = None;
    request.entries = entries.try_into().expect("the entries still fit");
    let prepared = ready(port.prepare_header_target(request)).expect("the repair prepares");
    let error =
        ready(port.apply_header_target(prepared)).expect_err("a recordless repair is stale");
    assert_eq!(error.category, ErrorCategory::StaleTargetOrGeneration);
}

/// A repair's range stops at the checkpoint ceiling, even where the
/// selected chain and the bucket would let it run on.
#[test]
fn a_repair_range_stops_at_the_checkpoint_ceiling() {
    let (port, headers) = chain(3, 2);
    let context = resolved(&port, 2);
    assert_eq!(context.selected_header_count(), 1);
    assert_eq!(
        context.request_target(),
        Frontier::new(block::Height(2), headers[1].hash())
    );
}

/// A supplier that can serve only part of the range sends a shortened
/// context's one header, and that repair applies: the engine re-derives the
/// one-header range's own episode.
#[test]
fn a_shortened_repair_applies() {
    let (port, headers) = chain(3, 10);
    let context = resolved(&port, 2);
    assert_eq!(context.selected_header_count(), 2);
    let short = context
        .bounded_prefix(1)
        .expect("an unconstrained range shortens to its first header");
    let request = repair_request(&port, &short, &headers, SourceId::from_digest([0x51; 32]));
    let prepared = ready(port.prepare_header_target(request)).expect("the repair prepares");
    assert_eq!(
        ready(port.apply_header_target(prepared)).expect("the shortened repair applies"),
        ApplyHeaderTargetOutcome::Applied
    );
}

/// Once a source's rooted payload is retained at a header, a repair there
/// from the same source is stale even with a different payload, while
/// another source's different payload applies.
#[test]
fn a_repair_from_a_retained_source_is_stale() {
    let (port, headers) = chain(3, 10);
    let source = SourceId::from_digest([0x51; 32]);
    let first = repair_request(&port, &resolved(&port, 2), &headers, source);
    let prepared = ready(port.prepare_header_target(first)).expect("the first repair prepares");
    ready(port.apply_header_target(prepared)).expect("the first repair applies");

    let context = resolved(&port, 2);
    assert_eq!(context.selected_header_count(), 1);
    let different = |source| {
        let mut request = repair_request(&port, &context, &headers, source);
        let mut entries = request.entries.into_vec();
        let record = entries[0]
            .tree_aux
            .as_mut()
            .expect("the request has records");
        record.sapling_tx_count += 100;
        request.entries = entries.try_into().expect("the entries still fit");
        request
    };
    let again = ready(port.prepare_header_target(different(source))).expect("the repeat prepares");
    let error = ready(port.apply_header_target(again)).expect_err("a retained source is stale");
    assert_eq!(error.category, ErrorCategory::StaleTargetOrGeneration);

    let other = SourceId::from_digest([0x52; 32]);
    let prepared = ready(port.prepare_header_target(different(other))).expect("it prepares");
    assert_eq!(
        ready(port.apply_header_target(prepared)).expect("another source's payload applies"),
        ApplyHeaderTargetOutcome::Applied
    );
}
