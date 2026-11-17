//! Tests of the chain transition queue.

use std::{sync::Arc, time::Duration};

use zakura_chain::block;

use super::{
    chain_transitions, AlreadySubscribed, ChainTransitionSender, MAX_QUEUED_TRANSITION_BLOCKS,
};
use crate::mempool::port::{
    ChainEvent, ChainTip, ChainTransition, ConnectedBlock, Generation, GenerationRead,
};

/// The tip at `height`, with a hash made from `height` and `fork`.
fn tip(height: u32, fork: u8) -> ChainTip {
    let mut hash = [fork; 32];
    hash[..4].copy_from_slice(&height.to_le_bytes());
    ChainTip {
        hash: block::Hash(hash),
        height: block::Height(height),
    }
}

/// A connected block with no transactions.
fn connected(at: ChainTip) -> ConnectedBlock {
    ConnectedBlock {
        hash: at.hash,
        height: at.height,
        transactions: Vec::new(),
        transaction_hashes: Arc::from([]),
    }
}

/// Publish one change from the queue's tip to `after`, which disconnects
/// `disconnected` and connects `connected`, and return its generation.
fn change(
    sender: &mut ChainTransitionSender,
    disconnected: Vec<ChainTip>,
    connect: Vec<ChainTip>,
    after: ChainTip,
) -> Generation {
    sender.begin_change();
    sender.finish_change(after, |before, generation| {
        Some(ChainTransition {
            before,
            disconnected,
            connected: connect.into_iter().map(connected).collect(),
            after,
            generation,
            rules_change: false,
        })
    })
}

/// Extend the chain by one block on fork 0.
fn grow(sender: &mut ChainTransitionSender, height: u32) -> Generation {
    change(sender, Vec::new(), vec![tip(height, 0)], tip(height, 0))
}

/// The transition an event is, or a panic.
fn transition(event: Option<ChainEvent>) -> ChainTransition {
    match event {
        Some(ChainEvent::Transition(transition)) => transition,
        other => panic!("expected a transition, got {other:?}"),
    }
}

#[test]
fn nothing_is_built_or_queued_without_a_consumer() {
    let mut sender = chain_transitions(Some(tip(0, 0)));
    sender.begin_change();
    let generation = sender.finish_change(tip(1, 0), |_, _| {
        panic!("a transition is built only for a consumer")
    });
    assert_eq!(generation, Generation(1));
    assert!(!sender.is_subscribed());

    let (start, mut events) = sender.transitions().subscribe().expect("no consumer yet");
    assert_eq!(start.tip, Some(tip(1, 0)));
    assert_eq!(start.generation, Generation(1));
    assert!(events.try_next().is_none());
}

#[test]
fn transitions_arrive_in_order_with_their_generations() {
    let mut sender = chain_transitions(Some(tip(0, 0)));
    let (start, mut events) = sender.transitions().subscribe().expect("no consumer yet");
    assert_eq!(start.generation, Generation(0));

    for height in 1..=3 {
        assert_eq!(grow(&mut sender, height), Generation(u64::from(height)));
    }
    for height in 1..=3 {
        let transition = transition(events.try_next());
        assert_eq!(transition.before, Some(tip(height - 1, 0)));
        assert_eq!(transition.after, tip(height, 0));
        assert_eq!(transition.generation, Generation(u64::from(height)));
    }
    assert!(events.try_next().is_none());
}

#[test]
fn a_disconnect_right_after_subscribing_starts_from_the_subscribed_tip() {
    let mut sender = chain_transitions(Some(tip(5, 0)));
    let (start, mut events) = sender.transitions().subscribe().expect("no consumer yet");
    change(&mut sender, vec![tip(5, 0)], Vec::new(), tip(4, 0));

    let transition = transition(events.try_next());
    assert_eq!(transition.before, start.tip);
    assert_eq!(transition.disconnected, vec![tip(5, 0)]);
    assert!(transition.connected.is_empty());
    assert_eq!(transition.after, tip(4, 0));
    assert_eq!(transition.generation, Generation(start.generation.0 + 1));
}

#[test]
fn overflow_in_a_long_reorg_resyncs_at_the_tip_and_then_queues_again() {
    let mut sender = chain_transitions(Some(tip(0, 0)));
    let (_, mut events) = sender.transitions().subscribe().expect("no consumer yet");

    let filled = u32::try_from(MAX_QUEUED_TRANSITION_BLOCKS).expect("the bound fits in u32");
    for height in 1..=filled {
        grow(&mut sender, height);
    }
    // A reorg of the last ten blocks no longer fits.
    let disconnected = (filled - 9..=filled).rev().map(|h| tip(h, 0)).collect();
    let connect: Vec<_> = (filled - 9..=filled).map(|h| tip(h, 1)).collect();
    let reorged = change(&mut sender, disconnected, connect, tip(filled, 1));

    assert!(matches!(
        events.try_next(),
        Some(ChainEvent::Resync { tip: at, generation }) if at == tip(filled, 1) && generation == reorged
    ));
    assert!(events.try_next().is_none());

    grow(&mut sender, filled + 1);
    let next = transition(events.try_next());
    assert_eq!(next.after, tip(filled + 1, 0));
    assert_eq!(next.generation, Generation(reorged.0 + 1));
}

#[test]
fn one_transition_larger_than_the_bound_resyncs() {
    let mut sender = chain_transitions(Some(tip(0, 0)));
    let (_, mut events) = sender.transitions().subscribe().expect("no consumer yet");
    let last = u32::try_from(MAX_QUEUED_TRANSITION_BLOCKS + 1).expect("the bound fits in u32");
    let generation = change(
        &mut sender,
        Vec::new(),
        (1..=last).map(|h| tip(h, 0)).collect(),
        tip(last, 0),
    );
    assert!(matches!(
        events.try_next(),
        Some(ChainEvent::Resync { generation: at, .. }) if at == generation
    ));
}

#[test]
fn a_writer_that_cannot_name_the_blocks_sends_a_resync() {
    let mut sender = chain_transitions(Some(tip(0, 0)));
    let (_, mut events) = sender.transitions().subscribe().expect("no consumer yet");
    sender.begin_change();
    let generation = sender.finish_change(tip(1, 0), |_, _| None);
    assert!(matches!(
        events.try_next(),
        Some(ChainEvent::Resync { tip: at, generation: g }) if at == tip(1, 0) && g == generation
    ));
}

#[test]
fn readers_see_the_generation_change_between_begin_and_finish() {
    let mut sender = chain_transitions(Some(tip(0, 0)));
    let cell = sender.transitions().generation();
    assert_eq!(cell.read(), GenerationRead::Stable(Generation(0)));
    sender.begin_change();
    assert_eq!(cell.read(), GenerationRead::Changing);
    sender.finish_change(tip(1, 0), |_, _| None);
    assert_eq!(cell.read(), GenerationRead::Stable(Generation(1)));
}

#[test]
fn there_is_one_consumer_and_a_new_one_starts_fresh() {
    let mut sender = chain_transitions(Some(tip(0, 0)));
    let transitions = sender.transitions();
    let (_, events) = transitions.subscribe().expect("no consumer yet");
    assert_eq!(transitions.subscribe().err(), Some(AlreadySubscribed));
    grow(&mut sender, 1);
    drop(events);
    assert!(!sender.is_subscribed());
    grow(&mut sender, 2);

    let (start, mut events) = transitions.subscribe().expect("the first consumer left");
    assert_eq!(start.tip, Some(tip(2, 0)));
    assert_eq!(start.generation, Generation(2));
    assert!(events.try_next().is_none());
}

#[tokio::test]
async fn the_stream_wakes_for_a_transition_and_ends_with_the_sender() {
    let mut sender = chain_transitions(Some(tip(0, 0)));
    let (_, mut events) = sender.transitions().subscribe().expect("no consumer yet");

    let reader = tokio::spawn(async move {
        let first = events.next().await;
        let end = events.next().await;
        (first, end)
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    grow(&mut sender, 1);
    drop(sender);

    let (first, end) = tokio::time::timeout(Duration::from_secs(10), reader)
        .await
        .expect("the reader wakes")
        .expect("the reader does not panic");
    assert_eq!(transition(first).after, tip(1, 0));
    assert!(end.is_none());
}
