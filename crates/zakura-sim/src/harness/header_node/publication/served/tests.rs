//! The served-page rule, fed by hand.

use zakura_network::zakura::ZakuraPeerId;

use zakura_header_chain::Frontier;

use super::*;
use crate::observe::{StorageBranch, StorageLease, StoragePoint, WorkScope};

/// The test hash numbered `n`.
fn h(n: u8) -> block::Hash {
    block::Hash([n; 32])
}

/// The test mirror.
fn mirror() -> ZakuraPeerId {
    ZakuraPeerId::new(vec![9; 32]).expect("32 bytes is a valid peer ID")
}

/// The mirror's storage name.
fn name(_: &ZakuraPeerId) -> String {
    "mirror".to_owned()
}

/// A check that has seen read `op` of session 1, from `after` at `height`,
/// complete with headers `read`, each the child of the one before.
fn read(check: &mut ServedPages, op: u64, (after, height): (u8, u32), read: &[u8]) {
    let mut parent = h(after);
    let links: Vec<(block::Hash, block::Hash)> = read
        .iter()
        .map(|&n| {
            let link = (h(n), parent);
            parent = h(n);
            link
        })
        .collect();
    check.storage(&StorageFact::Submitted {
        id: op,
        request: StorageRequest::ReadHeaderPath {
            lease: Some(StorageLease {
                peer: "mirror".to_owned(),
                session_id: 1,
                lease_id: op,
            }),
            scope: WorkScope {
                header_generation: 1,
                branch: StorageBranch {
                    anchor: String::new(),
                    target: String::new(),
                },
            },
            after: hash(h(after)),
            max_headers: 10,
        },
        digest: String::new(),
        call: None,
    });
    let count = u32::try_from(links.len()).expect("a test read is short");
    check.page_reads(&[(op, links)]);
    check.storage(&StorageFact::Completed {
        id: op,
        outcome: "replied".to_owned(),
        result: StorageResult::Page {
            last: StoragePoint {
                height: height + count,
                hash: hash(parent),
            },
            count,
            complete: false,
        },
        delivered: true,
        generation: 1,
        state: String::new(),
    });
}

/// Subscription 1's page under lease `lease`, from `after` at `height`,
/// with `headers`, toward initial target `target`, marked `reaches`.
fn page(
    lease: u64,
    after: u8,
    height: u32,
    headers: &[u8],
    target: u8,
    reaches: bool,
) -> ReactorEvent {
    ReactorEvent::PageQueued {
        peer: mirror(),
        session_id: 1,
        subscription_id: 1,
        common_ancestor: Frontier::new(block::Height(height), h(after)),
        headers: headers.iter().map(|&n| h(n)).collect(),
        initial_target: h(target),
        reaches_target: reaches,
        lease,
    }
}

/// A subscription's pages, each its read, hold.
#[test]
fn pages_that_are_their_reads_hold() {
    let mut check = ServedPages::default();
    read(&mut check, 1, (0, 0), &[1, 2]);
    assert_eq!(
        check.queued(&page(1, 0, 0, &[1, 2], 4, false), name),
        Some(Vec::new())
    );
    read(&mut check, 2, (2, 2), &[3, 4]);
    assert_eq!(
        check.queued(&page(2, 2, 2, &[3, 4], 4, true), name),
        Some(Vec::new())
    );
    read(&mut check, 3, (4, 4), &[5]);
    assert_eq!(
        check.queued(&page(3, 4, 4, &[5], 4, false), name),
        Some(Vec::new())
    );
}

/// A page whose headers differ from its read's, a sibling or a swap,
/// breaks, and so do one that declares another start than its read's, by
/// hash or by height, and one whose lease completed no read.
#[test]
fn a_page_unlike_its_read_breaks() {
    for served in [&[9, 2][..], &[2, 1][..]] {
        let mut check = ServedPages::default();
        read(&mut check, 1, (0, 0), &[1, 2]);
        let breaks = check
            .queued(&page(1, 0, 0, served, 4, false), name)
            .unwrap();
        assert!(breaks[0].contains("not its read's"), "{breaks:?}");
    }
    for (after, height) in [(7, 0), (0, 99)] {
        let mut check = ServedPages::default();
        read(&mut check, 1, (0, 0), &[1, 2]);
        let breaks = check
            .queued(&page(1, after, height, &[1, 2], 4, false), name)
            .unwrap();
        assert!(breaks[0].contains("not its read's start"), "{breaks:?}");
    }
    let mut check = ServedPages::default();
    read(&mut check, 1, (0, 0), &[1, 2]);
    let breaks = check
        .queued(&page(2, 0, 0, &[1, 2], 4, false), name)
        .unwrap();
    assert!(breaks[0].contains("no page read"), "{breaks:?}");
}

/// An empty page breaks, even when its read was empty too.
#[test]
fn an_empty_page_breaks() {
    let mut check = ServedPages::default();
    read(&mut check, 1, (0, 0), &[]);
    let breaks = check.queued(&page(1, 0, 0, &[], 4, false), name).unwrap();
    assert_eq!(breaks.len(), 1, "{breaks:?}");
    assert!(breaks[0].contains("not its read's"), "{breaks:?}");
}

/// A page joins the read under its own lease, even with another read of
/// the session's still waiting, and a lease's read serves one page; any
/// other event isn't a page.
#[test]
fn a_page_joins_its_own_lease() {
    let mut check = ServedPages::default();
    read(&mut check, 1, (0, 0), &[1, 2]);
    read(&mut check, 2, (0, 0), &[1, 2, 3]);
    assert_eq!(
        check.queued(&page(2, 0, 0, &[1, 2, 3], 4, false), name),
        Some(Vec::new())
    );
    let breaks = check
        .queued(&page(2, 0, 0, &[1, 2, 3], 4, false), name)
        .unwrap();
    assert!(breaks[0].contains("no page read"), "{breaks:?}");
    assert_eq!(
        check.queued(
            &ReactorEvent::PublisherStarted {
                peer: mirror(),
                session_id: 1,
            },
            name
        ),
        None
    );
}
