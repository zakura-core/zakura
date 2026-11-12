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
use zakura_sim::harness::header_acceptance::{
    companions::{
        fault_close, judge, path_break, rejected_page, run_draft, sent_page, staged_page,
        DraftEvidence, Refusal, SentPage, StagedPage, CONTENT_FAULT, FAULT_CLOSE, PATH_BREAK,
        STAGED,
    },
    refs::PageRef,
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
