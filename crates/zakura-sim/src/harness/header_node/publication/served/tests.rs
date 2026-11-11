//! The served-page rule, fed by hand.

use zakura_network::zakura::ZakuraPeerId;

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

/// The `Open` lookup for a mirror whose subscription 1 on session 1 asked
/// for target `target` from locator 0, and that opened nothing else.
fn opened(target: u8) -> impl Fn(&ZakuraPeerId, u64, u64) -> Option<Opened> {
    opened_from(target, &[0])
}

/// [`opened`], from `locators`. Header `n` is at height `n`.
fn opened_from(target: u8, locators: &[u8]) -> impl Fn(&ZakuraPeerId, u64, u64) -> Option<Opened> {
    let locators: Vec<block::Hash> = locators.iter().map(|&n| h(n)).collect();
    move |peer, session_id, subscription| {
        (*peer == mirror() && session_id == 1 && subscription == 1).then(|| Opened {
            target: h(target),
            target_height: block::Height(target.into()),
            locators: locators.clone(),
        })
    }
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

/// A subscription's two pages, each its read, the later from the cursor the
/// first left, the last marking its target, hold, and so does a following
/// page past it.
#[test]
fn pages_that_are_their_reads_hold() {
    let mut check = ServedPages::default();
    read(&mut check, 1, (0, 0), &[1, 2]);
    assert_eq!(
        check.queued(&page(1, 0, 0, &[1, 2], 4, false), name, opened(4)),
        Some(Vec::new())
    );
    read(&mut check, 2, (2, 2), &[3, 4]);
    assert_eq!(
        check.queued(&page(2, 2, 2, &[3, 4], 4, true), name, opened(4)),
        Some(Vec::new())
    );
    read(&mut check, 3, (4, 4), &[5]);
    assert_eq!(
        check.queued(&page(3, 4, 4, &[5], 4, false), name, opened(4)),
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
            .queued(&page(1, 0, 0, served, 4, false), name, opened(4))
            .unwrap();
        assert!(breaks[0].contains("not its read's"), "{breaks:?}");
    }
    for (after, height) in [(7, 0), (0, 99)] {
        let mut check = ServedPages::default();
        read(&mut check, 1, (0, 0), &[1, 2]);
        let breaks = check
            .queued(&page(1, after, height, &[1, 2], 4, false), name, opened(4))
            .unwrap();
        assert!(breaks[0].contains("not its read's start"), "{breaks:?}");
    }
    let mut check = ServedPages::default();
    read(&mut check, 1, (0, 0), &[1, 2]);
    let breaks = check
        .queued(&page(2, 0, 0, &[1, 2], 4, false), name, opened(4))
        .unwrap();
    assert!(breaks[0].contains("no page read"), "{breaks:?}");
}

/// A page whose target isn't its subscription's `Open` target breaks, even
/// with its marker matching the target it carries, and even when that
/// target is the page's own ancestor, as a subscriber that already held its
/// target would have it. So does a page whose subscription has no `Open`.
/// The first is also marked where the `Open`'s target isn't reached.
#[test]
fn a_page_off_its_open_target_breaks() {
    for (target, reaches) in [(2, true), (0, false)] {
        let mut check = ServedPages::default();
        read(&mut check, 1, (0, 0), &[1, 2]);
        let breaks = check
            .queued(&page(1, 0, 0, &[1, 2], target, reaches), name, opened(4))
            .unwrap();
        assert_eq!(breaks.len(), 1 + usize::from(reaches), "{breaks:?}");
        assert!(
            breaks[0].contains("not its subscription's `Open`"),
            "{breaks:?}"
        );
    }
    let mut check = ServedPages::default();
    read(&mut check, 1, (0, 0), &[1, 2]);
    let breaks = check
        .queued(
            &page(1, 0, 0, &[1, 2], 4, false),
            name,
            |_: &ZakuraPeerId, _, _| None,
        )
        .unwrap();
    assert!(breaks[0].contains("recorded no `Open`"), "{breaks:?}");
}

/// A first page from one of its `Open`'s lower locators holds, and one
/// from a start that isn't a locator breaks, even when it's its read's.
#[test]
fn a_first_page_starts_at_a_locator() {
    let mut check = ServedPages::default();
    read(&mut check, 1, (0, 0), &[1, 2]);
    assert_eq!(
        check.queued(
            &page(1, 0, 0, &[1, 2], 4, false),
            name,
            opened_from(4, &[3, 0])
        ),
        Some(Vec::new())
    );
    let mut check = ServedPages::default();
    read(&mut check, 1, (0, 0), &[1, 2]);
    let breaks = check
        .queued(
            &page(1, 0, 0, &[1, 2], 4, false),
            name,
            opened_from(4, &[3, 5]),
        )
        .unwrap();
    assert_eq!(breaks.len(), 1, "{breaks:?}");
    assert!(
        breaks[0].contains("isn't one of its `Open`'s locators"),
        "{breaks:?}"
    );
}

/// A page that serves one of its `Open`'s locators short of the target,
/// before its last header or as it, breaks, as from a lower locator than
/// the highest the node holds, while a page past the target through the
/// `Open`'s higher locators doesn't.
#[test]
fn a_page_past_a_locator_breaks() {
    let mut check = ServedPages::default();
    read(&mut check, 1, (0, 0), &[1, 2]);
    let breaks = check
        .queued(
            &page(1, 0, 0, &[1, 2], 4, false),
            name,
            opened_from(4, &[1, 0]),
        )
        .unwrap();
    assert_eq!(breaks.len(), 1, "{breaks:?}");
    assert!(
        breaks[0].contains("serves") && breaks[0].contains("short of its target"),
        "{breaks:?}"
    );
    let mut check = ServedPages::default();
    read(&mut check, 1, (0, 0), &[1, 2]);
    let breaks = check
        .queued(
            &page(1, 0, 0, &[1, 2], 4, false),
            name,
            opened_from(4, &[2, 0]),
        )
        .unwrap();
    assert_eq!(breaks.len(), 1, "{breaks:?}");
    assert!(
        breaks[0].contains("ends at") && breaks[0].contains("short of its target"),
        "{breaks:?}"
    );
    let mut check = ServedPages::default();
    read(&mut check, 1, (0, 0), &[1]);
    check.queued(
        &page(1, 0, 0, &[1], 1, true),
        name,
        opened_from(1, &[3, 2, 0]),
    );
    read(&mut check, 2, (1, 1), &[2, 3]);
    assert_eq!(
        check.queued(
            &page(2, 1, 1, &[2, 3], 1, false),
            name,
            opened_from(1, &[3, 2, 0])
        ),
        Some(Vec::new())
    );
}

/// When the `Open` names its target among its locators, its subscriber
/// already holds it: a first page from the target unmarked holds, and one
/// from below it breaks, marked or not.
#[test]
fn a_held_target_is_the_first_page_start() {
    let mut check = ServedPages::default();
    read(&mut check, 1, (2, 2), &[3]);
    assert_eq!(
        check.queued(
            &page(1, 2, 2, &[3], 2, false),
            name,
            opened_from(2, &[2, 1, 0])
        ),
        Some(Vec::new())
    );
    for marked in [true, false] {
        let mut check = ServedPages::default();
        read(&mut check, 1, (0, 0), &[1, 2]);
        let breaks = check
            .queued(
                &page(1, 0, 0, &[1, 2], 2, marked),
                name,
                opened_from(2, &[2, 1, 0]),
            )
            .unwrap();
        assert!(
            breaks[0].contains("isn't the target its `Open` named"),
            "{breaks:?}"
        );
        assert_eq!(breaks.len(), 1 + usize::from(marked), "{breaks:?}");
    }
}

/// A page that passes its target before its last header breaks, and so
/// does one that reaches the target's height elsewhere, on another branch,
/// and one that ends at its target at another height than the `Open`'s.
#[test]
fn a_page_through_or_beside_its_target_breaks() {
    let mut check = ServedPages::default();
    read(&mut check, 1, (0, 0), &[1, 2, 3]);
    let breaks = check
        .queued(&page(1, 0, 0, &[1, 2, 3], 2, false), name, opened(2))
        .unwrap();
    assert_eq!(breaks.len(), 1, "{breaks:?}");
    assert!(breaks[0].contains("passes its target"), "{breaks:?}");
    let mut check = ServedPages::default();
    read(&mut check, 1, (0, 0), &[5, 6]);
    let breaks = check
        .queued(&page(1, 0, 0, &[5, 6], 2, false), name, opened(2))
        .unwrap();
    assert_eq!(breaks.len(), 1, "{breaks:?}");
    assert!(breaks[0].contains("at or above its target's"), "{breaks:?}");
    let mut check = ServedPages::default();
    read(&mut check, 1, (0, 0), &[1, 3]);
    let breaks = check
        .queued(&page(1, 0, 0, &[1, 3], 3, true), name, opened(3))
        .unwrap();
    assert_eq!(breaks.len(), 1, "{breaks:?}");
    assert!(breaks[0].contains("not its target's"), "{breaks:?}");
}

/// An empty page breaks, even when its read was empty too.
#[test]
fn an_empty_page_breaks() {
    let mut check = ServedPages::default();
    read(&mut check, 1, (0, 0), &[]);
    let breaks = check
        .queued(&page(1, 0, 0, &[], 4, false), name, opened(4))
        .unwrap();
    assert_eq!(breaks.len(), 1, "{breaks:?}");
    assert!(breaks[0].contains("not its read's"), "{breaks:?}");
}

/// A later page that declares anything but its subscription's cursor
/// breaks, and so does a marker that doesn't match the last header.
#[test]
fn a_later_page_off_its_cursor_or_marker_breaks() {
    let mut check = ServedPages::default();
    read(&mut check, 1, (0, 0), &[1, 2]);
    check.queued(&page(1, 0, 0, &[1, 2], 4, false), name, opened(4));
    // The right hash at the wrong height, which its read agrees with, so
    // the target lands at the wrong height too.
    read(&mut check, 2, (2, 5), &[3, 4]);
    let breaks = check
        .queued(&page(2, 2, 5, &[3, 4], 4, true), name, opened(4))
        .unwrap();
    assert_eq!(breaks.len(), 2, "{breaks:?}");
    assert!(breaks[0].contains("not the cursor"), "{breaks:?}");
    assert!(breaks[1].contains("not its target's"), "{breaks:?}");

    let mut check = ServedPages::default();
    read(&mut check, 1, (0, 0), &[1, 2]);
    let breaks = check
        .queued(&page(1, 0, 0, &[1, 2], 2, false), name, opened(2))
        .unwrap();
    assert!(breaks[0].contains("target marker"), "{breaks:?}");
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
        check.queued(&page(2, 0, 0, &[1, 2, 3], 4, false), name, opened(4)),
        Some(Vec::new())
    );
    let breaks = check
        .queued(&page(2, 0, 0, &[1, 2, 3], 4, false), name, opened(4))
        .unwrap();
    assert!(breaks[0].contains("no page read"), "{breaks:?}");
    assert_eq!(
        check.queued(
            &ReactorEvent::PublisherStarted {
                peer: mirror(),
                session_id: 1,
            },
            name,
            opened(4)
        ),
        None
    );
}
