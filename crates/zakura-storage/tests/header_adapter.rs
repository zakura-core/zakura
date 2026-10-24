//! The engine-backed header port: fixture chains, preparation, apply,
//! locators, membership and the selected-chain watch (storage.md ST2).

use std::{
    future::Future,
    num::NonZeroU64,
    pin::pin,
    process::Command,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use sha2::{Digest, Sha256};
use zakura_chain::block;
use zakura_header_chain::{BodyWorkAuthority, ErrorCategory, Frontier};
use zakura_node_services::header_chain::{
    ApplyHeaderTargetOutcome, LocatorReadPermit, Port, PrepareHeaderTarget, PreparedHeaderTarget,
    SelectedChainMembership, VctRepairContextReply,
};
use zakura_storage::header::{
    adapter::EnginePort, fixture::HeaderFixture, HeaderReply, HeaderRequest,
};

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

/// The preparation request for `headers` on `parent`, owned from `port`'s
/// current snapshot.
fn request(
    port: &EnginePort,
    parent: Frontier,
    headers: &[Arc<block::Header>],
    request_id: u64,
) -> PrepareHeaderTarget {
    HeaderFixture::regtest().target_request(&port.snapshot(), parent, headers, request_id)
}

/// Prepares and applies `headers` on `parent`. Returns the new tip.
fn admit(
    port: &EnginePort,
    parent: Frontier,
    headers: &[Arc<block::Header>],
    request_id: u64,
) -> Frontier {
    let request = request(port, parent, headers, request_id);
    let target = request.target;
    let prepared = ready(port.prepare_header_target(request)).expect("fixture headers prepare");
    assert_eq!(
        ready(port.apply_header_target(prepared)).expect("fixture headers apply"),
        ApplyHeaderTargetOutcome::Applied
    );
    target
}

/// The frontier of each header in `headers`, which extend `parent`.
fn frontiers(parent: Frontier, headers: &[Arc<block::Header>]) -> Vec<Frontier> {
    (1..)
        .zip(headers)
        .map(|(offset, header)| {
            Frontier::new(block::Height(parent.height.0 + offset), header.hash())
        })
        .collect()
}

/// Where `hash` sits relative to the selected chain.
fn membership(port: &EnginePort, hash: block::Hash) -> SelectedChainMembership {
    ready(port.selected_chain_contains(hash)).expect("membership reads succeed")
}

/// A fresh locator permit.
fn permit() -> LocatorReadPermit {
    LocatorReadPermit::new(
        Arc::new(zakura_rt::sync::Semaphore::new(1))
            .try_acquire_owned()
            .expect("a fresh semaphore has a permit"),
    )
}

/// ST2.1: an extension then a heavier fork move the selected tip, the
/// locator and membership the way the fixture predicts.
#[test]
fn an_extension_and_a_heavier_fork_select_as_expected() {
    let fixture = HeaderFixture::regtest();
    let port = EnginePort::new(&fixture);
    let anchor = fixture.anchor();

    let main = fixture.extend(anchor, 3, 0);
    let main_tip = admit(&port, anchor, &main, 1);
    assert_eq!(port.snapshot().frontiers.header_best, main_tip);
    for frontier in frontiers(anchor, &main) {
        assert_eq!(
            membership(&port, frontier.hash),
            SelectedChainMembership::Selected(frontier)
        );
    }
    let locator = ready(port.continuation_locator(permit()))
        .expect("the locator reads")
        .expect("the engine always has a locator");
    assert_eq!(locator.entries().first(), Some(&main_tip));
    assert_eq!(locator.entries().last(), Some(&anchor));

    // Four headers from height 1 outweigh the two main headers above it.
    let fork_parent = frontiers(anchor, &main)[0];
    let fork = fixture.extend(fork_parent, 4, 1);
    let fork_tip = admit(&port, fork_parent, &fork, 2);
    assert_eq!(port.snapshot().frontiers.header_best, fork_tip);
    for frontier in &frontiers(anchor, &main)[1..] {
        assert_eq!(
            membership(&port, frontier.hash),
            SelectedChainMembership::NotSelected
        );
    }
    for frontier in frontiers(fork_parent, &fork) {
        assert_eq!(
            membership(&port, frontier.hash),
            SelectedChainMembership::Selected(frontier)
        );
    }
    assert_eq!(
        membership(&port, block::Hash([0xee; 32])),
        SelectedChainMembership::NotRetained
    );
    let locator = ready(port.continuation_locator(permit()))
        .expect("the locator reads")
        .expect("the engine always has a locator");
    assert_eq!(locator.entries().first(), Some(&fork_tip));
}

/// ST2.2: a target another adapter sealed fails with the contract's
/// rejection, `stale_target` on the owner's branch.
#[test]
fn a_foreign_seal_gets_the_contract_rejection() {
    let fixture = HeaderFixture::regtest();
    let ours = EnginePort::new(&fixture);
    let theirs = EnginePort::new(&fixture);
    let headers = fixture.extend(fixture.anchor(), 2, 0);
    let prepared: PreparedHeaderTarget =
        ready(theirs.prepare_header_target(request(&theirs, fixture.anchor(), &headers, 1)))
            .expect("the other adapter prepares");
    let apply = HeaderRequest::ApplyHeaderTarget {
        target: prepared.clone(),
    };
    let Some(HeaderReply::ApplyHeaderTarget(Err(expected))) =
        apply.rejection_reply(ours.adapter_key())
    else {
        panic!("the contract rejects a foreign seal");
    };
    let error = ready(ours.apply_header_target(prepared)).expect_err("a foreign seal fails");
    assert_eq!(error.category, expected.category);
    assert_eq!(error.subject, expected.subject);
    assert_eq!(ours.snapshot(), EnginePort::new(&fixture).snapshot());
}

/// ST2.2: work whose owner a later commit replaced fails as stale, and
/// commits nothing.
#[test]
fn an_expired_owner_is_stale() {
    let fixture = HeaderFixture::regtest();
    let port = EnginePort::new(&fixture);
    let anchor = fixture.anchor();
    let late = fixture.extend(anchor, 2, 1);
    // Prepared against the anchor snapshot, then overtaken.
    let prepared = ready(port.prepare_header_target(request(&port, anchor, &late, 1)))
        .expect("the late target prepares");
    admit(&port, anchor, &fixture.extend(anchor, 3, 0), 2);
    let before = port.snapshot();
    let error = ready(port.apply_header_target(prepared)).expect_err("the owner expired");
    assert_eq!(error.category, ErrorCategory::StaleTargetOrGeneration);
    assert_eq!(port.snapshot(), before);
}

/// ST2.2: preparation against a parent the engine doesn't retain, or at the
/// wrong height, fails as stale.
#[test]
fn invalid_preparation_context_is_stale() {
    let fixture = HeaderFixture::regtest();
    let port = EnginePort::new(&fixture);
    let anchor = fixture.anchor();
    let headers = fixture.extend(anchor, 2, 0);
    for parent in [
        Frontier::new(block::Height(0), block::Hash([0xab; 32])),
        Frontier::new(block::Height(5), anchor.hash),
    ] {
        let mut request = request(&port, anchor, &headers, 1);
        request.common_ancestor = parent;
        let error = ready(port.prepare_header_target(request)).expect_err("the context is stale");
        assert_eq!(
            error.category,
            ErrorCategory::StaleTargetOrGeneration,
            "{parent:?}"
        );
    }
}

/// ST2.2: a repair context, which a simulation can't resolve, fails
/// through the typed contract as `Stale`.
#[test]
fn a_repair_context_is_stale() {
    let fixture = HeaderFixture::regtest();
    let port = EnginePort::new(&fixture);
    let owner = BodyWorkAuthority::for_snapshot(&port.snapshot())
        .bind(1, NonZeroU64::new(1).expect("one is nonzero"));
    assert_eq!(
        ready(port.vct_repair_context(owner, block::Height(1))).expect("the context reads"),
        VctRepairContextReply::Stale
    );
}

/// ST2.3: a requester and a publisher holding one port share its state. The
/// watch changes only on a commit, holds only committed snapshots, and
/// membership is at least as new as it.
#[test]
fn requester_and_publisher_share_committed_state() {
    let fixture = HeaderFixture::regtest();
    let port = Arc::new(EnginePort::new(&fixture));
    let requester: Arc<dyn Port> = port.clone();
    let publisher: Arc<dyn Port> = port.clone();
    let mut watch = publisher.selected_chain();
    assert_eq!(*watch.borrow_and_update(), Some(port.snapshot()));

    let anchor = fixture.anchor();
    let headers = fixture.extend(anchor, 2, 0);
    let prepared = ready(requester.prepare_header_target(request(&port, anchor, &headers, 1)))
        .expect("the target prepares");
    // Preparation commits nothing, so the watch doesn't change.
    assert!(!watch.has_changed().expect("the port keeps the sender"));
    ready(requester.apply_header_target(prepared)).expect("the target applies");
    assert!(watch.has_changed().expect("the port keeps the sender"));
    let published = watch
        .borrow_and_update()
        .clone()
        .expect("a snapshot is published");
    assert_eq!(published, port.snapshot());
    let tip = published.frontiers.header_best;
    assert_eq!(
        ready(publisher.selected_chain_contains(tip.hash)).expect("membership reads"),
        SelectedChainMembership::Selected(tip)
    );

    // Replaying the same headers is a no-change apply, which publishes nothing.
    let replay = ready(requester.prepare_header_target(request(&port, anchor, &headers, 2)))
        .expect("the replay prepares");
    assert_eq!(
        ready(requester.apply_header_target(replay)).expect("the replay applies"),
        ApplyHeaderTargetOutcome::Applied
    );
    assert!(!watch.has_changed().expect("the port keeps the sender"));
}

/// A digest of a fixed scripted history: every snapshot, locator and
/// membership answer along an extension and a fork.
fn fingerprint() -> String {
    let fixture = HeaderFixture::regtest();
    let port = EnginePort::new(&fixture);
    let anchor = fixture.anchor();
    let mut hasher = Sha256::new();
    let mut record = |port: &EnginePort, hashes: &[block::Hash]| {
        hasher.update(format!("{:?}", port.snapshot()));
        let locator = ready(port.continuation_locator(permit())).expect("the locator reads");
        hasher.update(format!("{locator:?}"));
        for hash in hashes {
            hasher.update(format!("{:?}", membership(port, *hash)));
        }
    };
    let main = fixture.extend(anchor, 3, 0);
    let main_tip = admit(&port, anchor, &main, 1);
    record(&port, &[main_tip.hash]);
    let fork_parent = frontiers(anchor, &main)[0];
    let fork = fixture.extend(fork_parent, 4, 1);
    admit(&port, fork_parent, &fork, 2);
    record(
        &port,
        &main
            .iter()
            .chain(&fork)
            .map(|header| header.hash())
            .collect::<Vec<_>>(),
    );
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Set in the child process, which prints its fingerprint.
const CHILD: &str = "ZAKURA_STORAGE_FINGERPRINT_CHILD";

/// Marks the fingerprint in the child's output.
const MARKER: &str = "zakura-storage-fingerprint=";

/// Prints the fingerprint when run as the child, and does nothing otherwise.
#[test]
fn fresh_process_child() {
    if std::env::var_os(CHILD).is_some() {
        #[allow(
            clippy::print_stdout,
            reason = "the parent test reads the child's stdout"
        )]
        {
            println!("\n{MARKER}{}", fingerprint());
        }
    }
}

/// ST2.4: preparation and engine-visible results repeat in a fresh process
/// with the same fixtures. Fixture times sit far behind the scenario clock,
/// so the clock reading never changes a result.
#[test]
fn results_repeat_in_a_fresh_process() {
    let expected = fingerprint();
    assert_eq!(fingerprint(), expected, "the same process repeats");
    let output = Command::new(std::env::current_exe().expect("the test binary has a path"))
        .args(["--exact", "fresh_process_child", "--nocapture"])
        .env(CHILD, "1")
        .output()
        .expect("the test binary reruns itself");
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).expect("the child prints text");
    let at = stdout
        .find(MARKER)
        .unwrap_or_else(|| panic!("the child printed no fingerprint: {stdout}"))
        + MARKER.len();
    let child: String = stdout[at..]
        .chars()
        .take_while(char::is_ascii_hexdigit)
        .collect();
    assert_eq!(child, expected);
}
