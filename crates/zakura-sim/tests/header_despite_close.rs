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
        fault_close, judge, path_break, rejected_page, run_draft, staged_page, DraftEvidence,
        StagedPage, FAULT_CLOSE, PATH_BREAK, STAGED,
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

/// A staged page carrying one header, from `session`'s `subscription` at
/// `frame`.
fn staged(session: u64, subscription: u64, frame: u64) -> StagedPage {
    StagedPage {
        peer: "ab".into(),
        session,
        subscription,
        frame: Some(frame),
        hashes: vec!["01".into()],
    }
}

/// Unpatched, the node judges the unlinked page and closes its connection
/// with a fault, so the fault-close companion holds, and nothing stages, so
/// the other two don't: the catch needs the patch. The companion joins the
/// page's connection, so the same close moved to another connection fails.
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
    assert!(fault_close(&moved, &page).is_err());
}

/// Unpatched, the rejected page never stages. A page staged from its
/// session, subscription and frame would hold, and another page on the
/// same subscription, or the same frame on another session, wouldn't.
#[test]
fn the_staging_companion_needs_the_rejected_page_staged() {
    let evidence = evidence();
    let page = rejected_page(&evidence.observations).expect("one page was rejected");
    assert!(evidence.staged.is_empty(), "{:?}", evidence.staged);
    assert!(staged_page(&evidence.staged, &page).is_err());
    let own = staged(page.session, page.subscription, page.frame);
    assert_eq!(staged_page(&[own.clone()], &page), Ok(own));
    assert!(staged_page(
        &[staged(page.session, page.subscription, page.frame + 1)],
        &page
    )
    .is_err());
    assert!(staged_page(
        &[staged(page.session + 1, page.subscription, page.frame)],
        &page
    )
    .is_err());
}

/// Unpatched, `commit-only-authorized` holds, so no retained violation is
/// the rejected page's path break, even for a page staged from that frame.
#[test]
fn the_path_break_needs_a_retained_violation() {
    let evidence = evidence();
    let page = rejected_page(&evidence.observations).expect("one page was rejected");
    assert!(evidence.claims.is_array(), "{:?}", evidence.claims);
    let own = staged(page.session, page.subscription, page.frame);
    assert!(path_break(&evidence.properties, &evidence.claims, &own).is_err());
}
