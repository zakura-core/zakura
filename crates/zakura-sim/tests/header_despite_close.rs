//! hdr-14 item 2, gap 6: the despite-close case's companions on real runs.
//!
//! The planted case `commit-only-authorized/staged-despite-close` keeps the
//! subscriber's content fault but stages the rejected page anyway. Its row
//! names three companions, judged on its `Draft` run
//! ([`companions`](zakura_sim::harness::header_acceptance::companions)).
//! These tests judge them on the committed bad-content input unpatched,
//! where the subscriber refuses the unlinked page and nothing stages, and
//! move each key of each join on its own, with Rule 25's one template, to
//! show it doesn't match another page.
#![cfg(zakura_sim)]

mod support;

use std::path::Path;

use support::one_key_off::assert_one_key_off;
use zakura_sim::{
    check::FrameRef,
    harness::{
        self,
        header_acceptance::{
            companions::{
                fault_close, judge, path_break, rejected_page, run_draft, sent_page, staged_page,
                DraftEvidence, Refusal, SentPage, StagedPage, CONTENT_FAULT, FAULT_CLOSE,
                PATH_BREAK, STAGED,
            },
            refs::PageRef,
        },
        header_sync::{HeaderWire, WireScenario},
        Subsystem,
    },
    Observation,
};
use zakura_storage::header::fixture::HeaderFixture;

/// The committed bad-content input, the case's input.
fn input() -> Vec<u8> {
    let path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/header-wire-bad-content.bin");
    std::fs::read(&path).unwrap_or_else(|error| panic!("{}: {error}", path.display()))
}

/// The input's `Draft` run, unpatched.
fn evidence() -> DraftEvidence {
    run_draft(&input()).expect("the wire harness runs the committed input")
}

/// The fact staging `page`.
fn staged(page: &PageRef) -> StagedPage {
    StagedPage {
        peer: page.subscription.peer.clone(),
        session: page.subscription.session,
        subscription: page.subscription.subscription,
        frame: Some(page.frame),
        hashes: page.headers.clone(),
    }
}

/// What the peer sent as the input's rejected page, from its frame's
/// evidence.
fn sent(evidence: &DraftEvidence) -> SentPage {
    let page = rejected_page(&evidence.observations).expect("one page was rejected");
    sent_page(&page, &evidence.peers, &evidence.sent).expect("the peer sent the rejected page")
}

/// Unpatched, the node judges the unlinked page, its reader refuses it for
/// linkage and closes its connection with a fault, so the fault-close
/// companion holds, and nothing stages, so the other two don't: the catch
/// needs the patch. The companion joins the page's frame and its content
/// refusal's session, so each key of either moved on its own fails, and so
/// does the same close after a refusal for another fault.
#[test]
fn the_fault_close_holds_on_the_unpatched_rejection() {
    let evidence = evidence();
    let verdicts = judge(&evidence);
    let verdict = |name| {
        verdicts
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, verdict)| verdict.clone())
            .expect("every companion is judged")
    };
    assert_eq!(verdict(FAULT_CLOSE), Ok(()));
    assert!(verdict(STAGED).is_err());
    assert!(verdict(PATH_BREAK).is_err());

    let page = rejected_page(&evidence.observations).expect("one page was rejected");
    let held = |refusals: &[Refusal], page: &_| {
        fault_close(&evidence.observations, refusals, page, &evidence.peers).is_ok()
    };
    assert_one_key_off(&page.frame, |frame| {
        let page = zakura_sim::harness::header_acceptance::companions::RejectedPage {
            frame: frame.clone(),
            ..page.clone()
        };
        held(&evidence.refusals, &page)
    });

    let [refusal] = &evidence.refusals[..] else {
        panic!("one refusal: {:?}", evidence.refusals);
    };
    assert_eq!(
        (refusal.session, refusal.reason.as_str()),
        (page.session, CONTENT_FAULT)
    );
    // Sol P2: the same refusal with only its peer, or only its session,
    // changed is another session's, so it doesn't hold either.
    assert_one_key_off(&refusal.session(), |session| {
        let moved = Refusal {
            peer: session.peer.clone(),
            session: session.session,
            ..refusal.clone()
        };
        held(&[moved], &page)
    });
    let mut other = evidence.refusals.clone();
    other[0].reason = "over_credit".into();
    assert!(!held(&other, &page));
}

/// Unpatched, the rejected page never stages. The page staged from its
/// frame, with the peer and hashes the peer sent there, would hold, and the
/// same fact with any one key moved, its peer, session, subscription,
/// frame or a header, wouldn't.
#[test]
fn the_staging_companion_needs_the_rejected_page_staged() {
    let evidence = evidence();
    let page = rejected_page(&evidence.observations).expect("one page was rejected");
    let sent = sent(&evidence);
    assert!(!sent.hashes.is_empty(), "{sent:?}");
    assert!(evidence.staged.is_empty(), "{:?}", evidence.staged);
    assert!(staged_page(&evidence.staged, &page, &sent).is_err());
    assert_one_key_off(&page.page(&sent), |staged_ref| {
        staged_page(&[staged(staged_ref)], &page, &sent).is_ok()
    });
}

/// The chain peer `peer` (its harness name) serves in the input's
/// scenario, built from the scenario rather than from the run: each
/// header's hash in hex, and the hash in hex the same header gets with its
/// previous-block hash's first byte flipped, as the linkage fault sends it.
fn served_chain(peer: &str) -> Vec<(String, String)> {
    let entry = harness::find(HeaderWire::NAME).expect("the wire harness is registered");
    let scenario: WireScenario =
        serde_json::from_value(entry.scenario(&input())).expect("the input draws a scenario");
    let index: usize = peer
        .strip_prefix("peer-")
        .and_then(|index| index.parse().ok())
        .unwrap_or_else(|| panic!("{peer} is a scenario peer's name"));
    let length = scenario.peers[index]
        .honest
        .as_ref()
        .expect("the faulting peer serves an honest chain")
        .chain;
    let hex = |hash: zakura_chain::block::Hash| -> String {
        hash.0.iter().map(|byte| format!("{byte:02x}")).collect()
    };
    let fixture = HeaderFixture::regtest();
    fixture
        .extend(fixture.anchor(), length, 0)
        .iter()
        .map(|header| {
            let mut unlinked = **header;
            unlinked.previous_block_hash.0[0] ^= 1;
            (hex(header.hash()), hex(unlinked.hash()))
        })
        .collect()
}

/// The producer of `SentPages`: what it keeps for the rejected frame is
/// the page the peer sent there, as the scenario's chain and the linkage
/// fault give it: its first header unlinked, then the chain's next
/// headers. Every page it keeps sits at a headers frame the peer logged,
/// and is that page or a run of the chain's headers.
#[test]
fn the_sent_pages_store_holds_what_the_peer_sent() {
    let evidence = evidence();
    let page = rejected_page(&evidence.observations).expect("one page was rejected");
    let chain = served_chain(&page.peer);
    let stored = evidence
        .sent
        .page(&page.frame)
        .expect("the store holds the rejected frame's page");
    let start = chain
        .iter()
        .position(|(_, unlinked)| *unlinked == stored[0])
        .unwrap_or_else(|| {
            panic!("the page's first header is a chain header unlinked: {stored:?}")
        });
    let expected: Vec<String> = std::iter::once(chain[start].1.clone())
        .chain(
            chain[start + 1..]
                .iter()
                .take(stored.len() - 1)
                .map(|(hash, _)| hash.clone()),
        )
        .collect();
    assert_eq!(stored, expected, "{page:?}");

    let hashes: Vec<String> = chain.iter().map(|(hash, _)| hash.clone()).collect();
    let mut logged = 0;
    for obs in &evidence.observations {
        let Observation::PeerFrame {
            connection,
            stream,
            quic_stream: Some(quic_stream),
            seq,
            row: Some(row),
            ..
        } = obs
        else {
            continue;
        };
        let frame = FrameRef {
            connection: *connection,
            kind: stream.kind,
            version: stream.version,
            quic_stream: *quic_stream,
            inbound: true,
            seq: *seq,
        };
        let Some(kept) = evidence.sent.page(&frame) else {
            continue;
        };
        assert_eq!(row, "headers", "{frame:?}: {kept:?}");
        logged += 1;
        assert!(
            kept == expected || hashes.windows(kept.len()).any(|run| run == &kept[..]),
            "{frame:?}: {kept:?}"
        );
    }
    assert_eq!(
        logged,
        evidence.sent.len(),
        "every kept page is a logged frame's"
    );
}

/// Unpatched, `commit-only-authorized` holds, so no retained violation is
/// the rejected page's path break, even for a page staged from that frame.
#[test]
fn the_path_break_needs_a_retained_violation() {
    let evidence = evidence();
    let page = rejected_page(&evidence.observations).expect("one page was rejected");
    assert!(evidence.claims.is_array(), "{:?}", evidence.claims);
    assert!(path_break(
        &evidence.properties,
        &evidence.claims,
        &page,
        &sent(&evidence)
    )
    .is_err());
}
