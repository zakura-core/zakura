//! hdr-14 item 2, gap 6: the despite-close case's companions on real runs.
//!
//! The planted case `commit-only-authorized/staged-despite-close` keeps the
//! subscriber's content fault but stages the rejected page anyway. Its row
//! names three companions, judged on its `Draft` run
//! ([`companions`](zakura_sim::harness::header_acceptance::companions)).
//! These tests judge them on the committed bad-content input unpatched,
//! where the subscriber refuses the unlinked page and nothing stages, and
//! move one key of each join to show it doesn't match another page.
#![cfg(zakura_sim)]

use std::path::Path;

use zakura_sim::{
    harness::header_acceptance::companions::{
        fault_close, judge, path_break, rejected_page, run_draft, sent_page, staged_page,
        DraftEvidence, SentPage, StagedPage, CONTENT_FAULT, FAULT_CLOSE, PATH_BREAK, STAGED,
    },
    Observation,
};

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

/// A staged page of `sent`, from `session`'s `subscription` at `frame`.
fn staged(sent: &SentPage, session: u64, subscription: u64, frame: u64) -> StagedPage {
    StagedPage {
        peer: sent.peer.clone(),
        session,
        subscription,
        frame: Some(frame),
        hashes: sent.hashes.clone(),
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
/// needs the patch. The companion joins the page's connection and its
/// content refusal, so the same close moved to another connection fails,
/// and so does the same close after a refusal for another fault.
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
    let moved: Vec<Observation> = evidence
        .observations
        .iter()
        .cloned()
        .map(|mut obs| {
            if let Observation::ConnectionClosed { connection, .. } = &mut obs {
                if *connection == page.connection {
                    *connection += 1000;
                }
            }
            obs
        })
        .collect();
    assert!(fault_close(&moved, &evidence.refusals, &page).is_err());

    let [refusal] = &evidence.refusals[..] else {
        panic!("one refusal: {:?}", evidence.refusals);
    };
    assert_eq!(
        (refusal.session, refusal.reason.as_str()),
        (page.session, CONTENT_FAULT)
    );
    let mut other = evidence.refusals.clone();
    other[0].reason = "over_credit".into();
    assert!(fault_close(&evidence.observations, &other, &page).is_err());
}

/// Unpatched, the rejected page never stages. A page staged from its
/// session, subscription and frame, with the peer and hashes the peer sent
/// there, would hold, and another page on the same subscription, the same
/// frame on another session, or the same frame with only its peer or only
/// its hashes changed, wouldn't.
#[test]
fn the_staging_companion_needs_the_rejected_page_staged() {
    let evidence = evidence();
    let page = rejected_page(&evidence.observations).expect("one page was rejected");
    let sent = sent(&evidence);
    assert!(!sent.hashes.is_empty(), "{sent:?}");
    assert!(evidence.staged.is_empty(), "{:?}", evidence.staged);
    assert!(staged_page(&evidence.staged, &page, &sent).is_err());
    let own = staged(&sent, page.session, page.subscription, page.frame);
    assert_eq!(
        staged_page(std::slice::from_ref(&own), &page, &sent),
        Ok(())
    );
    assert!(staged_page(
        &[staged(
            &sent,
            page.session,
            page.subscription,
            page.frame + 1
        )],
        &page,
        &sent
    )
    .is_err());
    assert!(staged_page(
        &[staged(
            &sent,
            page.session + 1,
            page.subscription,
            page.frame
        )],
        &page,
        &sent
    )
    .is_err());
    let other_peer = StagedPage {
        peer: "00".repeat(32),
        ..own.clone()
    };
    let mut other_hashes = own.clone();
    other_hashes.hashes.reverse();
    other_hashes.hashes.push("00".repeat(32));
    for off in [other_peer, other_hashes] {
        assert!(
            staged_page(&[off.clone()], &page, &sent).is_err(),
            "{off:?}"
        );
    }
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
