//! One conformance test per active `bpv1-close-N` row, plus the BPv1 close
//! properties that `close` states. Each test drives the production machines:
//! it runs the violation and checks the row and the sessions it names, then
//! runs the nearest conformant sequence and checks that it stays open.

use std::time::Duration;

use proptest::prelude::*;
use zakura_chain::block;
use zakura_quic::NodeId;
use zakura_rt::time::Instant;

use super::{
    blocks::{
        BlockConfig, BlockEvent, BlockInput, BlockOutput, BlockStep, BlockTable,
        HeaderAdmissionResult, MAX_PENDING_META_PER_PEER,
    },
    blocks_tests::{
        closed_sessions, commitment_ok, header_result, missing_parent, rejected_sessions,
        unknown_parent_checked,
    },
    close::Bpv1CloseRow,
    codec::encode_body,
    peer::{PeerConfig, PeerInput, PeerOutput, PeerState, RecvHave, RecvWantEnd, Retire},
    peer_tests::{
        admitted, assert_closed, have_with, pull, recv_meta, recv_want, session, tiny,
        tiny_part_prefix, tiny_session, verify_token,
    },
    profile::Profile,
    service::{control_input, data_input},
    test_support::{commit_work, make_envelope, node_id, LinkProfile, MachineMesh, TEST_TIP_HASH},
    unknown_parent::UnknownParentVerdict,
    wire::{HaveFlags, HaveForm, HAVE_FLAG_BITMAP, PROPAGATION_DEPTH},
};
use crate::zakura::{handshake::Frame, wire_codec::encode_frame};

// ============================================================================
// Rows 1 and 2: the frame decoders
// ============================================================================

fn frame(message: &super::wire::Bpv1Message) -> Frame {
    encode_frame(message).expect("the test message encodes")
}

fn plain_have() -> super::wire::Bpv1Message {
    super::wire::Bpv1Message::Have {
        block_id: block::Hash([0x51; 32]),
        flags: HaveFlags::Plain,
        form: HaveForm::List,
        indices: vec![0, 1],
    }
}

fn want(want_id: u32) -> super::wire::Bpv1Message {
    let mut requested = super::wire::WantBitmap::EMPTY;
    requested.set(0);
    super::wire::Bpv1Message::Want {
        want_id,
        block_id: block::Hash([0x52; 32]),
        stripe: 0,
        requested,
    }
}

/// `bpv1-close-1`: a `Have` flags byte with reserved bits closes 1, and a
/// message type on the wrong BPv1 stream closes 1. Every defined flags byte,
/// with or without the bitmap bit (decision-33), decodes.
#[test]
fn close_1_reserved_have_flags_and_wrong_stream_types() {
    // `Have` payload: `block_id` (32 bytes), then the flags byte.
    const FLAGS_AT: usize = 32;
    let defined: Vec<u8> = [HaveFlags::Plain, HaveFlags::Push, HaveFlags::Relay]
        .into_iter()
        .flat_map(|flags| [flags.bits(), flags.bits() | HAVE_FLAG_BITMAP])
        .collect();
    for &flags in &defined {
        // The list `[0, 1]` read as a bitmap still names two indices.
        let mut have = frame(&plain_have());
        have.payload[FLAGS_AT] = flags;
        assert!(
            data_input(have).is_ok(),
            "{flags:#04x} is a defined flags byte"
        );
    }
    for reserved in (0..=u8::MAX).filter(|b| !defined.contains(b)) {
        let mut have = frame(&plain_have());
        have.payload[FLAGS_AT] = reserved;
        assert_eq!(
            data_input(have).err(),
            Some(Bpv1CloseRow::Row1FrameHeader),
            "flags byte {reserved:#04x} is reserved"
        );
    }

    assert!(control_input(&frame(&want(1))).is_ok());
    assert_eq!(
        control_input(&frame(&plain_have())).err(),
        Some(Bpv1CloseRow::Row1FrameHeader),
        "a data message on the control stream"
    );
    assert_eq!(
        data_input(frame(&want(1))).err(),
        Some(Bpv1CloseRow::Row1FrameHeader),
        "a control message on the data stream"
    );
}

/// `bpv1-close-2`: truncation, trailing bytes, a zero `want_id`, an empty
/// `Want` bitmap, and a `HeaderMeta` header that does not parse each close
/// 2. The well-formed frames decode.
#[test]
fn close_2_decode_errors() {
    // `Want` payload: `want_id` (4 bytes), `block_id` (32), `stripe` (4),
    // then the 11-byte bitmap.
    const BITMAP_AT: usize = 40;

    let mut truncated = frame(&plain_have());
    truncated.payload.pop();
    let mut trailing = frame(&plain_have());
    trailing.payload.push(0);
    let mut zero_id = frame(&want(1));
    zero_id.payload[..4].fill(0);
    let mut empty_bitmap = frame(&want(1));
    empty_bitmap.payload[BITMAP_AT..].fill(0);
    let mut short_want = frame(&want(1));
    short_want.payload.pop();
    let bad_header = frame(&super::wire::Bpv1Message::HeaderMeta {
        header: vec![0xAA; 140],
        binding: super::commitment::CoinbaseBinding {
            coinbase: vec![0x01],
            txid_siblings: Vec::new(),
            auth_siblings: Vec::new(),
            light_client_root: [0; 32],
        },
        coding: tiny(),
        part_root: [0x11; 32],
        proposer_node: node_id(1),
    });

    for (name, input) in [
        ("truncated Have", data_input(truncated)),
        ("Have with a trailing byte", data_input(trailing)),
    ] {
        assert_eq!(input.err(), Some(Bpv1CloseRow::Row2Decode), "{name}");
    }
    for (name, input) in [
        ("zero want_id", control_input(&zero_id)),
        ("empty Want bitmap", control_input(&empty_bitmap)),
        ("truncated Want", control_input(&short_want)),
        ("unparsable header", control_input(&bad_header)),
    ] {
        assert_eq!(input.err(), Some(Bpv1CloseRow::Row2Decode), "{name}");
    }

    assert!(data_input(frame(&plain_have())).is_ok());
    assert!(control_input(&frame(&want(1))).is_ok());
}

// ============================================================================
// Rows 3 and 4: the block table's metadata checks
// ============================================================================

/// The sessions that send copies of one block's `HeaderMeta` in the row 3
/// and row 4 tests.
fn sessions() -> [(NodeId, u64); 4] {
    [
        (node_id(2), 1),
        (node_id(3), 7),
        (node_id(4), 2),
        (node_id(5), 9),
    ]
}

/// Steps `MetaReceived` for `copy` from `from`.
fn copy_from(
    table: &mut BlockTable,
    now: Instant,
    copy: Box<super::blocks::MetaEnvelope>,
    from: (NodeId, u64),
) -> BlockStep {
    table.step(
        now,
        BlockInput::MetaReceived {
            envelope: copy,
            from_peer: from.0,
            session_gen: from.1,
        },
    )
}

fn sorted(mut sessions: Vec<(NodeId, u64)>) -> Vec<(NodeId, u64)> {
    sessions.sort();
    sessions
}

/// Whether the step starts a commitment check.
fn verifies(step: &BlockStep) -> bool {
    step.outputs
        .iter()
        .any(|o| matches!(o, BlockOutput::VerifyCommitment { .. }))
}

fn commitment(table: &mut BlockTable, now: Instant, step: &BlockStep, ok: bool) -> BlockStep {
    table.step(
        now,
        BlockInput::CommitmentChecked {
            work_id: commit_work(step),
            ok,
            peers: Vec::new(),
            rng_seed: 0,
        },
    )
}

fn admits(step: &BlockStep, block_id: block::Hash) -> bool {
    step.outputs.iter().any(|o| {
        matches!(o, BlockOutput::PublishAdmitted { admitted, .. } if admitted.block_id == block_id)
    })
}

/// `bpv1-close-3`: a peer-invalid header closes every session that sent the
/// block, whatever binding each copy carried: the block id hashes the
/// header. An admitted header closes no session.
#[test]
fn close_3_invalid_header_closes_every_sender_of_any_copy() {
    let now = Instant::now();
    let coding = tiny();
    let block_id = block::Hash([0x33; 32]);
    let [a, b, c, d] = sessions();
    let receive_four = |table: &mut BlockTable| {
        let first = copy_from(
            table,
            now,
            make_envelope(block_id, coding, [0xA1; 32], node_id(1)),
            a,
        );
        copy_from(
            table,
            now,
            make_envelope(block_id, coding, [0xA1; 32], node_id(1)),
            b,
        );
        copy_from(
            table,
            now,
            make_envelope(block_id, coding, [0xA2; 32], node_id(1)),
            c,
        );
        copy_from(
            table,
            now,
            make_envelope(block_id, coding, [0xA3; 32], node_id(1)),
            d,
        );
        first
    };

    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let first = receive_four(&mut table);
    let refused = header_result(&mut table, now, &first, HeaderAdmissionResult::PeerInvalid);
    assert_eq!(
        closed_sessions(&refused, Bpv1CloseRow::Row3InvalidHeader),
        sorted(vec![a, b, c, d])
    );
    table.assert_invariants();

    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let first = receive_four(&mut table);
    let header = header_result(
        &mut table,
        now,
        &first,
        HeaderAdmissionResult::Admitted {
            height: 1,
            parent_hash: TEST_TIP_HASH,
        },
    );
    assert!(header
        .outputs
        .iter()
        .all(|o| !matches!(o, BlockOutput::ClosePeer { .. })));
    table.assert_invariants();
}

/// `bpv1-close-4`, layout: a later copy with a bad layout closes its own
/// sender only. The first copy's admission goes on.
#[test]
fn close_4_bad_later_layout_closes_only_its_sender() {
    let now = Instant::now();
    let block_id = block::Hash([0x34; 32]);
    let [a, b, ..] = sessions();
    let mut bad = tiny();
    bad.n = bad.k;
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let first = copy_from(
        &mut table,
        now,
        make_envelope(block_id, tiny(), [0xA1; 32], node_id(1)),
        a,
    );
    let later = copy_from(
        &mut table,
        now,
        make_envelope(block_id, bad, [0xA1; 32], node_id(1)),
        b,
    );
    assert_eq!(
        closed_sessions(&later, Bpv1CloseRow::Row4BadMetaOrCommitment),
        vec![b]
    );
    let header = header_result(
        &mut table,
        now,
        &first,
        HeaderAdmissionResult::Admitted {
            height: 1,
            parent_hash: TEST_TIP_HASH,
        },
    );
    assert!(admits(&commitment_ok(&mut table, now, &header), block_id));
    table.assert_invariants();
}

/// `bpv1-close-4`, commitment (bpv1-61): a failed commitment closes the
/// first sender and each session whose copy equals the failed envelope. A
/// differing copy gets its own check, in arrival order. If it passes, the
/// block is admitted and its senders stay open; if it fails, its senders
/// close too. `alternates_never_take_admitting_room_from_a_first_copy`
/// covers copies that the table did not keep.
#[test]
fn close_4_failed_commitment_closes_identical_copies_and_checks_the_alternate() {
    let now = Instant::now();
    let block_id = block::Hash([0x35; 32]);
    let [a, b, c, d] = sessions();
    let receive_four = |table: &mut BlockTable| {
        let first = copy_from(
            table,
            now,
            make_envelope(block_id, tiny(), [0xA1; 32], node_id(1)),
            a,
        );
        copy_from(
            table,
            now,
            make_envelope(block_id, tiny(), [0xA1; 32], node_id(1)),
            b,
        );
        copy_from(
            table,
            now,
            make_envelope(block_id, tiny(), [0xA2; 32], node_id(1)),
            c,
        );
        copy_from(
            table,
            now,
            make_envelope(block_id, tiny(), [0xA3; 32], node_id(1)),
            d,
        );
        header_result(
            table,
            now,
            &first,
            HeaderAdmissionResult::Admitted {
                height: 1,
                parent_hash: TEST_TIP_HASH,
            },
        )
    };

    // The alternate passes: only the sessions that sent the failed envelope
    // close, and the block is admitted.
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let header = receive_four(&mut table);
    let failed = commitment(&mut table, now, &header, false);
    assert_eq!(
        closed_sessions(&failed, Bpv1CloseRow::Row4BadMetaOrCommitment),
        sorted(vec![a, b])
    );
    assert!(rejected_sessions(&failed, block_id).is_empty());
    assert!(verifies(&failed), "the alternate gets its own check");
    let alternate = commitment(&mut table, now, &failed, true);
    assert!(admits(&alternate, block_id));
    assert!(alternate
        .outputs
        .iter()
        .all(|o| !matches!(o, BlockOutput::ClosePeer { .. })));
    table.assert_invariants();

    // Two differing copies fail before an honest one (F-884): each closes
    // its own sender, and the honest copy still gets its check.
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let header = receive_four(&mut table);
    let failed = commitment(&mut table, now, &header, false);
    let failed_again = commitment(&mut table, now, &failed, false);
    assert_eq!(
        closed_sessions(&failed_again, Bpv1CloseRow::Row4BadMetaOrCommitment),
        vec![c]
    );
    assert!(rejected_sessions(&failed_again, block_id).is_empty());
    assert!(verifies(&failed_again), "the next alternate gets its check");
    let honest = commitment(&mut table, now, &failed_again, true);
    assert!(admits(&honest, block_id));
    table.assert_invariants();

    // Every kept copy fails: the last sender closes too, and no check is
    // left.
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let header = receive_four(&mut table);
    let failed = commitment(&mut table, now, &header, false);
    let failed_again = commitment(&mut table, now, &failed, false);
    let failed_last = commitment(&mut table, now, &failed_again, false);
    assert_eq!(
        closed_sessions(&failed_last, Bpv1CloseRow::Row4BadMetaOrCommitment),
        vec![d]
    );
    assert!(!verifies(&failed_last));
    assert_eq!(table.admitting_meta_len(), 0);
    table.assert_invariants();

    // Without any differing copy, a passing commitment closes nobody.
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let first = copy_from(
        &mut table,
        now,
        make_envelope(block_id, tiny(), [0xA1; 32], node_id(1)),
        a,
    );
    copy_from(
        &mut table,
        now,
        make_envelope(block_id, tiny(), [0xA1; 32], node_id(1)),
        b,
    );
    let header = header_result(
        &mut table,
        now,
        &first,
        HeaderAdmissionResult::Admitted {
            height: 1,
            parent_hash: TEST_TIP_HASH,
        },
    );
    let ok = commitment(&mut table, now, &header, true);
    assert!(admits(&ok, block_id));
    assert!(ok
        .outputs
        .iter()
        .all(|o| !matches!(o, BlockOutput::ClosePeer { .. })));
}

/// `bpv1-close-4`, unknown parent (bpv1-61): a binding that fails the
/// unknown-parent checks closes the first sender and each session whose copy
/// equals it. The kept alternates get their own checks in arrival order,
/// and the first one that passes parks.
#[test]
fn close_4_failed_unknown_parent_binding_checks_the_alternate() {
    let now = Instant::now();
    let parent = block::Hash([0x38; 32]);
    let block_id = block::Hash([0x39; 32]);
    let [a, b, c, d] = sessions();
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let first = copy_from(
        &mut table,
        now,
        make_envelope(block_id, tiny(), [0xA1; 32], node_id(1)),
        a,
    );
    for (root, from) in [([0xA1; 32], b), ([0xA2; 32], c), ([0xA3; 32], d)] {
        copy_from(
            &mut table,
            now,
            make_envelope(block_id, tiny(), root, node_id(1)),
            from,
        );
    }
    let checking = header_result(
        &mut table,
        now,
        &first,
        HeaderAdmissionResult::MissingParent {
            parent_hash: parent,
        },
    );
    let failed = unknown_parent_checked(
        &mut table,
        now,
        &checking,
        UnknownParentVerdict::Invalid(Bpv1CloseRow::Row4BadMetaOrCommitment),
    );
    assert_eq!(
        closed_sessions(&failed, Bpv1CloseRow::Row4BadMetaOrCommitment),
        sorted(vec![a, b])
    );
    assert!(rejected_sessions(&failed, block_id).is_empty());
    table.assert_invariants();

    // `c`'s copy is the oldest alternate, so it is checked next; `d` waits.
    let failed = unknown_parent_checked(
        &mut table,
        now,
        &failed,
        UnknownParentVerdict::Invalid(Bpv1CloseRow::Row4BadMetaOrCommitment),
    );
    assert_eq!(
        closed_sessions(&failed, Bpv1CloseRow::Row4BadMetaOrCommitment),
        vec![c]
    );
    assert!(rejected_sessions(&failed, block_id).is_empty());
    table.assert_invariants();

    let parked = unknown_parent_checked(&mut table, now, &failed, UnknownParentVerdict::Park);
    assert!(parked
        .outputs
        .iter()
        .all(|o| !matches!(o, BlockOutput::ClosePeer { .. })));
    assert_eq!(table.pending_meta_len(), 1);
    table.assert_invariants();
}

/// An alternate whose sender already holds [`MAX_PENDING_META_PER_PEER`]
/// pending entries does not take over a failed unknown-parent binding
/// (`bpv1-11`). Its copy is rejected without fault, and the next alternate
/// takes the entry.
#[test]
fn close_4_failed_unknown_parent_binding_skips_an_alternate_at_the_pending_cap() {
    let now = Instant::now();
    let parent = block::Hash([0x3C; 32]);
    let block_id = block::Hash([0x3D; 32]);
    let [a, b, c, _] = sessions();
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    for i in (0u8..).take(MAX_PENDING_META_PER_PEER) {
        let held = copy_from(
            &mut table,
            now,
            make_envelope(
                block::Hash([0x80 + i; 32]),
                tiny(),
                [0xB0 + i; 32],
                node_id(1),
            ),
            b,
        );
        missing_parent(&mut table, now, &held, parent);
    }
    let first = copy_from(
        &mut table,
        now,
        make_envelope(block_id, tiny(), [0xC1; 32], node_id(1)),
        a,
    );
    for (root, from) in [([0xC2; 32], b), ([0xC3; 32], c)] {
        copy_from(
            &mut table,
            now,
            make_envelope(block_id, tiny(), root, node_id(1)),
            from,
        );
    }
    let checking = header_result(
        &mut table,
        now,
        &first,
        HeaderAdmissionResult::MissingParent {
            parent_hash: parent,
        },
    );
    let failed = unknown_parent_checked(
        &mut table,
        now,
        &checking,
        UnknownParentVerdict::Invalid(Bpv1CloseRow::Row4BadMetaOrCommitment),
    );
    assert_eq!(
        closed_sessions(&failed, Bpv1CloseRow::Row4BadMetaOrCommitment),
        vec![a]
    );
    assert_eq!(rejected_sessions(&failed, block_id), vec![b]);
    table.assert_invariants();

    // `c`'s copy took the entry, so its checks run and it parks.
    unknown_parent_checked(&mut table, now, &failed, UnknownParentVerdict::Park);
    assert_eq!(table.pending_meta_len(), MAX_PENDING_META_PER_PEER + 1);
    table.assert_invariants();
}

/// An alternate that takes over a failed unknown-parent binding keeps the
/// entry's best-header count. The best header moved during the first copy's
/// checks, and no wake reached the entry, so the alternate retries header
/// admission when its checks pass.
#[test]
fn close_4_unknown_parent_alternate_retries_after_the_best_header_moved() {
    let now = Instant::now();
    let parent = block::Hash([0x3A; 32]);
    let block_id = block::Hash([0x3B; 32]);
    let [a, b, ..] = sessions();
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let first = copy_from(
        &mut table,
        now,
        make_envelope(block_id, tiny(), [0xA1; 32], node_id(1)),
        a,
    );
    copy_from(
        &mut table,
        now,
        make_envelope(block_id, tiny(), [0xA2; 32], node_id(1)),
        b,
    );
    let checking = header_result(
        &mut table,
        now,
        &first,
        HeaderAdmissionResult::MissingParent {
            parent_hash: parent,
        },
    );
    table.step(
        now,
        BlockInput::ParentAdmitted {
            hash: parent,
            height: 1,
        },
    );
    let failed = unknown_parent_checked(
        &mut table,
        now,
        &checking,
        UnknownParentVerdict::Invalid(Bpv1CloseRow::Row4BadMetaOrCommitment),
    );
    assert_eq!(
        closed_sessions(&failed, Bpv1CloseRow::Row4BadMetaOrCommitment),
        vec![a]
    );

    let retried = unknown_parent_checked(&mut table, now, &failed, UnknownParentVerdict::Park);
    assert_eq!(table.pending_meta_len(), 0);
    assert!(retried
        .outputs
        .iter()
        .any(|o| matches!(o, BlockOutput::AdmitHeader { .. })));
    table.assert_invariants();
}

/// Header sync can reach a pending block's own height while its checks run.
/// The promoted alternate is then checked against the new best header and
/// gets `Drop`, but the header is now known, so it retries admission and
/// BPv1 still delivers the block (#2760).
#[test]
fn unknown_parent_drop_after_header_sync_reached_the_block_retries_admission() {
    let now = Instant::now();
    let parent = block::Hash([0x3C; 32]);
    let block_id = block::Hash([0x3D; 32]);
    let [a, b, ..] = sessions();
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    table.step(
        now,
        BlockInput::ParentAdmitted {
            hash: block::Hash([0x3E; 32]),
            height: 9,
        },
    );
    let first = copy_from(
        &mut table,
        now,
        make_envelope(block_id, tiny(), [0xA1; 32], node_id(1)),
        a,
    );
    copy_from(
        &mut table,
        now,
        make_envelope(block_id, tiny(), [0xA2; 32], node_id(1)),
        b,
    );
    let checking = header_result(
        &mut table,
        now,
        &first,
        HeaderAdmissionResult::MissingParent {
            parent_hash: parent,
        },
    );
    table.step(
        now,
        BlockInput::ParentAdmitted {
            hash: block_id,
            height: 11,
        },
    );
    let failed = unknown_parent_checked(
        &mut table,
        now,
        &checking,
        UnknownParentVerdict::Invalid(Bpv1CloseRow::Row4BadMetaOrCommitment),
    );
    assert!(failed
        .outputs
        .iter()
        .any(|o| matches!(o, BlockOutput::CheckUnknownParent { top: 11, .. })));

    let retried = unknown_parent_checked(&mut table, now, &failed, UnknownParentVerdict::Drop);
    assert!(rejected_sessions(&retried, block_id).is_empty());
    assert_eq!(table.pending_meta_len(), 0);
    let admitted = header_result(
        &mut table,
        now,
        &retried,
        HeaderAdmissionResult::Admitted {
            height: 11,
            parent_hash: parent,
        },
    );
    let committed = commitment_ok(&mut table, now, &admitted);
    assert!(committed
        .events
        .contains(&BlockEvent::BlockAdmitted { block_id }));
    table.assert_invariants();
}

/// The alternate envelope waits with its entry while the parent header is
/// unknown, and gets its check once the parent arrives.
#[test]
fn close_4_alternate_survives_a_wait_for_the_parent() {
    let now = Instant::now();
    let parent = block::Hash([0x36; 32]);
    let block_id = block::Hash([0x37; 32]);
    let [a, b, ..] = sessions();
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let first = copy_from(
        &mut table,
        now,
        make_envelope(block_id, tiny(), [0xA1; 32], node_id(1)),
        a,
    );
    missing_parent(&mut table, now, &first, parent);
    copy_from(
        &mut table,
        now,
        make_envelope(block_id, tiny(), [0xA2; 32], node_id(1)),
        b,
    );
    table.assert_invariants();
    assert_eq!(table.pending_meta_len(), 1);

    let woke = table.step(
        now,
        BlockInput::ParentAdmitted {
            hash: parent,
            height: 1,
        },
    );
    let header = header_result(
        &mut table,
        now,
        &woke,
        HeaderAdmissionResult::Admitted {
            height: 2,
            parent_hash: parent,
        },
    );
    let failed = commitment(&mut table, now, &header, false);
    assert_eq!(
        closed_sessions(&failed, Bpv1CloseRow::Row4BadMetaOrCommitment),
        vec![a]
    );
    assert!(admits(
        &commitment(&mut table, now, &failed, true),
        block_id
    ));
    table.assert_invariants();
}

/// Alternate envelopes take only room that no first copy needs: a first
/// copy that the budget fits without alternates sheds them, and an
/// alternate that does not fit is not kept. A shed alternate's sender is
/// rejected without a close when the first copy fails.
#[test]
fn alternates_never_take_admitting_room_from_a_first_copy() {
    const COINBASE: usize = 1_900_000;
    let now = Instant::now();
    // W2 allows a 2 MB block, so four envelopes fill most of the budget.
    let coding = Profile::W2.derive_coding(64, 2).unwrap();
    let big = |id: u8, root: u8| {
        let mut envelope = make_envelope(block::Hash([id; 32]), coding, [root; 32], node_id(1));
        envelope.binding.coinbase = vec![0x01; COINBASE];
        envelope
    };
    let [a, b, ..] = sessions();
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::W2));
    let first = copy_from(&mut table, now, big(0x40, 0xA1), a);
    copy_from(&mut table, now, big(0x41, 0xA1), a);
    copy_from(&mut table, now, big(0x42, 0xA1), a);
    // Three first copies and one alternate fill most of the budget.
    copy_from(&mut table, now, big(0x40, 0xA2), b);
    table.assert_invariants();
    // A fourth first copy fits only without the alternate.
    let fourth = copy_from(&mut table, now, big(0x43, 0xA1), a);
    assert!(fourth
        .outputs
        .iter()
        .any(|o| matches!(o, BlockOutput::AdmitHeader { .. })));
    assert!(rejected_sessions(&fourth, block::Hash([0x43; 32])).is_empty());
    assert_eq!(table.admitting_meta_len(), 4);
    // No room is left for another alternate.
    copy_from(&mut table, now, big(0x41, 0xA2), b);
    table.assert_invariants();

    let header = header_result(
        &mut table,
        now,
        &first,
        HeaderAdmissionResult::Admitted {
            height: 1,
            parent_hash: TEST_TIP_HASH,
        },
    );
    let failed = commitment(&mut table, now, &header, false);
    assert_eq!(
        closed_sessions(&failed, Bpv1CloseRow::Row4BadMetaOrCommitment),
        vec![a]
    );
    assert_eq!(rejected_sessions(&failed, block::Hash([0x40; 32])), vec![b]);
    assert!(!verifies(&failed));
    table.assert_invariants();
}

/// An alternate envelope waiting for a parent also gives way to a first
/// copy: the pending budget sheds it before it evicts any first copy.
#[test]
fn pending_alternates_give_way_before_a_first_copy_is_evicted() {
    const COINBASE: usize = 1_900_000;
    let now = Instant::now();
    let coding = Profile::W2.derive_coding(64, 2).unwrap();
    let big = |id: u8, root: u8| {
        let mut envelope = make_envelope(block::Hash([id; 32]), coding, [root; 32], node_id(1));
        envelope.binding.coinbase = vec![0x01; COINBASE];
        envelope
    };
    let parent = block::Hash([0x3F; 32]);
    let park = |table: &mut BlockTable, copy, from| {
        let received = copy_from(table, now, copy, from);
        missing_parent(table, now, &received, parent)
    };
    let [a, b, c, _] = sessions();
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::W2));
    for id in [0x40, 0x41, 0x42] {
        park(&mut table, big(id, 0xA1), a);
    }
    // Three first copies and one alternate fill most of the pending budget.
    copy_from(&mut table, now, big(0x40, 0xA2), b);
    table.assert_invariants();

    // A fourth first copy fits only without the alternate. Without the shed,
    // the table would evict a first copy of `a`, the heaviest peer.
    let parked = park(&mut table, big(0x43, 0xA1), c);
    assert!(parked
        .events
        .iter()
        .all(|e| !matches!(e, BlockEvent::MetaPendingDropped { .. })));
    for id in [0x40, 0x41, 0x42, 0x43] {
        assert!(rejected_sessions(&parked, block::Hash([id; 32])).is_empty());
    }
    assert_eq!(table.pending_meta_len(), 4);
    table.assert_invariants();
}

/// A first copy that the pending stage refuses takes no room, so it sheds
/// no alternate (F-886). The refused copy competes only with first copies:
/// the count cap is full and no other peer is heavier.
#[test]
fn a_refused_pending_copy_keeps_every_alternate() {
    let now = Instant::now();
    let coding = Profile::W2.derive_coding(64, 2).unwrap();
    let id = |peer: u8, index: u8| {
        let mut hash = [0x70; 32];
        hash[0] = peer;
        hash[1] = index;
        block::Hash(hash)
    };
    let copy = |block_id, root: u8, coinbase: usize| {
        let mut envelope = make_envelope(block_id, coding, [root; 32], node_id(1));
        envelope.binding.coinbase = vec![0x01; coinbase];
        envelope
    };
    let parent = block::Hash([0x6E; 32]);
    let alternate_parent = block::Hash([0x6F; 32]);
    let park = |table: &mut BlockTable, envelope, from: (NodeId, u64), parent_hash| {
        let received = copy_from(table, now, envelope, from);
        missing_parent(table, now, &received, parent_hash)
    };
    let heavy = (node_id(10), 10);
    let relay = (node_id(11), 11);
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::W2));
    // `heavy` parks three large first copies; the first waits on its own
    // parent, so it can be woken alone.
    park(
        &mut table,
        copy(id(10, 0), 0xA1, 1_900_000),
        heavy,
        alternate_parent,
    );
    for index in 1..3 {
        park(
            &mut table,
            copy(id(10, index), 0xA1, 1_900_000),
            heavy,
            parent,
        );
    }
    // Sixty-one small first copies fill the count cap.
    for peer in 20..35u8 {
        for index in 0..4 {
            park(
                &mut table,
                copy(id(peer, index), 0xA1, 3),
                (node_id(peer), u64::from(peer)),
                parent,
            );
        }
    }
    park(
        &mut table,
        copy(id(35, 0), 0xA1, 3),
        (node_id(35), 35),
        parent,
    );
    assert_eq!(table.pending_meta_len(), 64);
    // An honest relay's differing copy waits beside `heavy`'s first one.
    copy_from(&mut table, now, copy(id(10, 0), 0xA2, 1_500_000), relay);
    table.assert_invariants();

    // `heavy`'s fourth copy fits the bytes only without the alternate, but
    // the count cap refuses it.
    let refused = park(&mut table, copy(id(10, 3), 0xA1, 1_900_000), heavy, parent);
    assert_eq!(rejected_sessions(&refused, id(10, 3)), vec![heavy]);
    assert_eq!(table.pending_meta_len(), 64);
    table.assert_invariants();

    // The alternate is still kept: it gets its check when the first fails.
    let woke = table.step(
        now,
        BlockInput::ParentAdmitted {
            hash: alternate_parent,
            height: 1,
        },
    );
    let header = header_result(
        &mut table,
        now,
        &woke,
        HeaderAdmissionResult::Admitted {
            height: 2,
            parent_hash: alternate_parent,
        },
    );
    let failed = commitment(&mut table, now, &header, false);
    assert!(verifies(&failed), "the relay's copy gets its check");
    assert!(rejected_sessions(&failed, id(10, 0)).is_empty());
}

/// A session that ended leaves no record in metadata still in admission or
/// pending (F-883). The table frees an alternate that no live session sent,
/// and it keeps the replacement session of the same peer.
#[test]
fn an_ended_session_leaves_no_sender_record() {
    let now = Instant::now();
    let parent = block::Hash([0x6D; 32]);
    let block_id = block::Hash([0x6C; 32]);
    let [a, b, c, _] = sessions();
    let replacement = (c.0, c.1 + 100);
    let ended = |table: &mut BlockTable, (peer, session_gen): (NodeId, u64)| {
        table.step(now, BlockInput::MetaSessionEnded { peer, session_gen });
    };
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let first = copy_from(
        &mut table,
        now,
        make_envelope(block_id, tiny(), [0xA1; 32], node_id(1)),
        a,
    );
    missing_parent(&mut table, now, &first, parent);
    copy_from(
        &mut table,
        now,
        make_envelope(block_id, tiny(), [0xA2; 32], node_id(1)),
        b,
    );
    copy_from(
        &mut table,
        now,
        make_envelope(block_id, tiny(), [0xA1; 32], node_id(1)),
        c,
    );
    ended(&mut table, b);
    ended(&mut table, c);
    // The peer reconnects and sends again on its new session.
    copy_from(
        &mut table,
        now,
        make_envelope(block_id, tiny(), [0xA1; 32], node_id(1)),
        replacement,
    );
    table.assert_invariants();

    let woke = table.step(
        now,
        BlockInput::ParentAdmitted {
            hash: parent,
            height: 1,
        },
    );
    let header = header_result(
        &mut table,
        now,
        &woke,
        HeaderAdmissionResult::Admitted {
            height: 2,
            parent_hash: parent,
        },
    );
    let failed = commitment(&mut table, now, &header, false);
    assert_eq!(
        closed_sessions(&failed, Bpv1CloseRow::Row4BadMetaOrCommitment),
        sorted(vec![a, replacement])
    );
    assert!(!verifies(&failed), "no live session sent the alternate");
    assert!(rejected_sessions(&failed, block_id).is_empty());
    assert_eq!(table.admitting_meta_len(), 0);
    table.assert_invariants();
}

// ============================================================================
// Rows 5 to 20: the session machine
// ============================================================================

fn retire(block_id: block::Hash) -> PeerInput {
    PeerInput::Retire(Retire {
        block_id,
        stripes: tiny().stripes,
        n: tiny().n,
    })
}

/// `bpv1-close-5` (bpv1-4): a repeated `HeaderMeta` closes 5 while this
/// node keeps the block pending, admitted, or retired. A first `HeaderMeta`
/// never closes, also for a retired block.
#[test]
fn close_5_repeated_header_meta_closes_while_the_block_is_known() {
    let now = Instant::now();
    let block_id = block::Hash([0x55; 32]);
    let new_peer = || PeerState::new(PeerConfig::for_profile(Profile::TINY), node_id(2), 1);

    // Pending: the table has not admitted the block yet.
    let mut peer = new_peer();
    peer.step(now, recv_meta(block_id));
    assert!(peer.close_cause().is_none());
    peer.step(now, recv_meta(block_id));
    assert_closed(&peer, Bpv1CloseRow::Row5DuplicateHeaderMeta);

    // Admitted.
    let mut peer = tiny_session(block_id, now);
    peer.step(now, recv_meta(block_id));
    assert!(peer.close_cause().is_none());
    peer.step(now, recv_meta(block_id));
    assert_closed(&peer, Bpv1CloseRow::Row5DuplicateHeaderMeta);

    // Retired after the first copy, whether pending or admitted.
    for mut peer in [new_peer(), tiny_session(block_id, now)] {
        peer.step(now, recv_meta(block_id));
        peer.step(now, retire(block_id));
        assert!(peer.close_cause().is_none());
        peer.step(now, recv_meta(block_id));
        assert_closed(&peer, Bpv1CloseRow::Row5DuplicateHeaderMeta);
        peer.assert_invariants();
    }

    // Retired before the first copy: the first copy stays open, the repeat closes.
    let mut peer = new_peer();
    peer.step(now, retire(block_id));
    peer.step(now, recv_meta(block_id));
    assert!(peer.close_cause().is_none());
    peer.step(now, recv_meta(block_id));
    assert_closed(&peer, Bpv1CloseRow::Row5DuplicateHeaderMeta);
}

/// `EvictRetired` drops the session's tombstone of a retired block. A
/// `HeaderMeta` for the block then reaches the table, whose window refuses
/// it, instead of closing 5 as a repeat.
#[test]
fn evicted_tombstone_forgets_the_retired_block() {
    let now = Instant::now();
    let block_id = block::Hash([0x56; 32]);
    let mut peer = tiny_session(block_id, now);
    peer.step(now, recv_meta(block_id));
    peer.step(now, retire(block_id));
    peer.step(now, PeerInput::EvictRetired { block_id });
    peer.assert_invariants();

    let step = peer.step(now, recv_meta(block_id));
    assert!(peer.close_cause().is_none());
    assert!(step.outputs.iter().any(
        |o| matches!(o, PeerOutput::SubmitHeaderMeta { block_id: id, .. } if *id == block_id)
    ));
    peer.assert_invariants();
}

/// `bpv1-close-8`: a `Want` stripe or slot outside the admitted layout
/// closes 8. The last stripe and the last slot are in range.
#[test]
fn close_8_want_outside_the_layout() {
    let now = Instant::now();
    let block_id = block::Hash([0x58; 32]);
    let coding = tiny();
    for (stripe, slot, closes) in [
        (coding.stripes - 1, coding.n - 1, false),
        (coding.stripes, 0, true),
        (0, coding.n, true),
    ] {
        let mut peer = tiny_session(block_id, now);
        peer.step(now, recv_want(1, block_id, stripe, &[slot]));
        if closes {
            assert_closed(&peer, Bpv1CloseRow::Row8BadWantRange);
        } else {
            assert!(peer.close_cause().is_none(), "stripe {stripe} slot {slot}");
        }
    }
}

/// `bpv1-close-9` (bpv1-28): a `Want` stays live at the peer until this node
/// writes its `WantEnd`. Reusing its id or overlapping its set before then
/// closes 9; after the end is written, both are conformant.
#[test]
fn close_9_want_whose_end_is_unwritten_is_still_live() {
    let now = Instant::now();
    // An unknown block ends each `Want` at once with `UnknownBlock`, and a
    // retired one with `Retired`.
    let unknown = block::Hash([0x59; 32]);
    let retired = block::Hash([0x5A; 32]);
    let new_peer = || {
        let mut peer = PeerState::new(PeerConfig::for_profile(Profile::TINY), node_id(2), 1);
        peer.step(now, retire(retired));
        peer
    };
    for block_id in [unknown, retired] {
        let ends = |step: &super::peer::PeerStep| {
            step.outputs
                .iter()
                .any(|o| matches!(o, PeerOutput::SendWantEnd { want_id: 1, .. }))
        };
        // The same id again, before the end is written.
        let mut peer = new_peer();
        assert!(ends(&peer.step(now, recv_want(1, block_id, 0, &[0]))));
        peer.step(now, recv_want(1, block_id, 1, &[0]));
        assert_closed(&peer, Bpv1CloseRow::Row9WantReservationViolation);

        // An overlapping set under a new id, before the end is written.
        let mut peer = new_peer();
        peer.step(now, recv_want(1, block_id, 0, &[0, 1]));
        peer.step(now, recv_want(2, block_id, 0, &[1]));
        assert_closed(&peer, Bpv1CloseRow::Row9WantReservationViolation);

        // A disjoint set under a new id is conformant.
        let mut peer = new_peer();
        peer.step(now, recv_want(1, block_id, 0, &[0]));
        peer.step(now, recv_want(2, block_id, 0, &[1]));
        assert!(peer.close_cause().is_none());

        // Once the end is written, the peer may reuse both.
        let mut peer = new_peer();
        peer.step(now, recv_want(1, block_id, 0, &[0]));
        peer.step(now, PeerInput::WantEndWritten);
        peer.step(now, recv_want(1, block_id, 0, &[0]));
        assert!(peer.close_cause().is_none());
        peer.assert_invariants();
    }
}

/// `bpv1-close-10`: a `StripeDone` outside the layout or repeated closes 10.
/// One `StripeDone` per stripe is conformant.
#[test]
fn close_10_stripe_done_out_of_range_or_repeated() {
    let now = Instant::now();
    let block_id = block::Hash([0x5B; 32]);
    let done = |stripe| PeerInput::RecvStripeDone { block_id, stripe };
    let mut peer = tiny_session(block_id, now);
    for stripe in 0..tiny().stripes {
        peer.step(now, done(stripe));
    }
    assert!(peer.close_cause().is_none());
    peer.step(now, done(0));
    assert_closed(&peer, Bpv1CloseRow::Row10BadStripeDone);

    let mut peer = tiny_session(block_id, now);
    peer.step(now, done(tiny().stripes));
    assert_closed(&peer, Bpv1CloseRow::Row10BadStripeDone);
}

/// `bpv1-close-11`: a second `BlockDone` closes 11; the first does not.
#[test]
fn close_11_repeated_block_done() {
    let now = Instant::now();
    let block_id = block::Hash([0x5C; 32]);
    let mut peer = tiny_session(block_id, now);
    peer.step(now, PeerInput::RecvBlockDone { block_id });
    assert!(peer.close_cause().is_none());
    peer.step(now, PeerInput::RecvBlockDone { block_id });
    assert_closed(&peer, Bpv1CloseRow::Row11DuplicateBlockDone);
}

/// `bpv1-close-13`: a `Part` index outside the layout or a failed proof
/// closes 13. A wanted part with a valid proof stays open.
#[test]
fn close_13_part_index_or_proof() {
    let now = Instant::now();
    let block_id = block::Hash([0x5D; 32]);
    let total = tiny().total_parts().unwrap();

    let mut peer = tiny_session(block_id, now);
    peer.step(now, tiny_part_prefix(block_id, total));
    assert_closed(&peer, Bpv1CloseRow::Row13BadPartIndexOrProof);

    let encoded = encode_body(&Profile::TINY, &[0x5D; 64], 2).unwrap();
    for proof_ok in [true, false] {
        let mut peer = tiny_session(block_id, now);
        peer.step(now, pull(block_id, 0, &[0], now));
        let rx = peer.step(now, tiny_part_prefix(block_id, 0));
        let token = verify_token(&rx).expect("a wanted part starts its proof job");
        if proof_ok {
            peer.step(
                now,
                PeerInput::PartVerified {
                    token,
                    part: encoded.parts[0].clone(),
                },
            );
            assert!(peer.close_cause().is_none());
        } else {
            peer.step(now, PeerInput::PartFailed { token });
            assert_closed(&peer, Bpv1CloseRow::Row13BadPartIndexOrProof);
        }
    }
}

/// `bpv1-close-14`: a part nobody asked for, or the same part twice, closes
/// 14. A wanted part stays open.
#[test]
fn close_14_unwanted_or_repeated_part() {
    let now = Instant::now();
    let block_id = block::Hash([0x5E; 32]);
    let mut peer = tiny_session(block_id, now);
    peer.step(now, recv_meta(block_id));
    peer.step(now, tiny_part_prefix(block_id, 0));
    assert_closed(&peer, Bpv1CloseRow::Row14UnwantedOrDuplicatePart);

    let mut peer = tiny_session(block_id, now);
    peer.step(now, recv_meta(block_id));
    peer.step(now, pull(block_id, 0, &[0], now));
    peer.step(now, tiny_part_prefix(block_id, 0));
    assert!(peer.close_cause().is_none());
    peer.step(now, tiny_part_prefix(block_id, 0));
    assert_closed(&peer, Bpv1CloseRow::Row14UnwantedOrDuplicatePart);
}

/// `bpv1-close-15` (bpv1-21): after retirement, a session may still send
/// the parts and `Have` indices it has not sent; one more closes 15.
#[test]
fn close_15_retired_counters() {
    let now = Instant::now();
    let block_id = block::Hash([0x5F; 32]);
    let total = tiny().total_parts().unwrap();

    let mut peer = tiny_session(block_id, now);
    peer.step(now, have_with(block_id, HaveFlags::Plain, vec![0]));
    peer.step(now, retire(block_id));
    peer.step(
        now,
        have_with(block_id, HaveFlags::Plain, (1..total).collect()),
    );
    assert!(peer.close_cause().is_none());
    peer.step(now, have_with(block_id, HaveFlags::Plain, vec![0]));
    assert_closed(&peer, Bpv1CloseRow::Row15RetiredCounterExhausted);

    let mut peer = tiny_session(block_id, now);
    peer.step(now, retire(block_id));
    for index in 0..total {
        peer.step(now, tiny_part_prefix(block_id, index));
    }
    assert!(peer.close_cause().is_none());
    peer.step(now, tiny_part_prefix(block_id, 0));
    assert_closed(&peer, Bpv1CloseRow::Row15RetiredCounterExhausted);
}

/// `bpv1-close-16`: a `Have` index outside the layout, or one the peer
/// already announced, closes 16. Distinct indices stay open.
#[test]
fn close_16_have_index_out_of_range_or_repeated() {
    let now = Instant::now();
    let block_id = block::Hash([0x60; 32]);
    let total = tiny().total_parts().unwrap();

    let mut peer = tiny_session(block_id, now);
    peer.step(now, have_with(block_id, HaveFlags::Plain, vec![0, 1]));
    peer.step(now, have_with(block_id, HaveFlags::Plain, vec![2]));
    assert!(peer.close_cause().is_none());
    peer.step(now, have_with(block_id, HaveFlags::Plain, vec![1]));
    assert_closed(&peer, Bpv1CloseRow::Row16BadHaveIndex);

    let mut peer = tiny_session(block_id, now);
    peer.step(now, have_with(block_id, HaveFlags::Plain, vec![total]));
    assert_closed(&peer, Bpv1CloseRow::Row16BadHaveIndex);
}

/// An inbound `Have` of `indices` in `form`.
fn have_in(block_id: block::Hash, form: HaveForm, indices: Vec<u32>) -> PeerInput {
    PeerInput::RecvHave(RecvHave {
        block_id,
        flags: HaveFlags::Plain,
        form,
        frame_bytes: 33 + form.indices_len(indices.len()),
        indices,
    })
}

/// `bpv1-close-16` (decision-33): a `Have` bitmap must be exactly
/// `ceil(stripes * n / 8)` bytes, and a set padding bit names an index past
/// the layout. Each closes 16, also after the `Have` waited in the early
/// record for the block's admission. A bitmap that fits stays open, and an
/// index it repeats closes 16.
#[test]
fn close_16_have_bitmap_length_padding_or_repeat() {
    let now = Instant::now();
    let block_id = block::Hash([0x63; 32]);
    let total = tiny().total_parts().unwrap();
    assert!(!total.is_multiple_of(8), "the TINY layout has padding bits");
    // The TINY layout's bitmap is a single byte.
    let len = u16::try_from(total.div_ceil(8)).unwrap();
    let fits = HaveForm::Bitmap { len };

    let mut peer = tiny_session(block_id, now);
    peer.step(now, have_in(block_id, fits, vec![0, 2]));
    peer.step(now, have_in(block_id, fits, vec![1, total - 1]));
    assert!(peer.close_cause().is_none());
    peer.step(now, have_in(block_id, fits, vec![2]));
    assert_closed(&peer, Bpv1CloseRow::Row16BadHaveIndex);

    let mut peer = tiny_session(block_id, now);
    peer.step(now, have_in(block_id, fits, vec![total]));
    assert_closed(&peer, Bpv1CloseRow::Row16BadHaveIndex);

    let mut peer = tiny_session(block_id, now);
    peer.step(
        now,
        have_in(block_id, HaveForm::Bitmap { len: len + 1 }, vec![0]),
    );
    assert_closed(&peer, Bpv1CloseRow::Row16BadHaveIndex);

    // A `Have` before admission waits in the early record, then closes
    // when its replay meets the layout.
    let mut peer = PeerState::new(PeerConfig::for_profile(Profile::TINY), node_id(2), 1);
    peer.step(
        now,
        have_in(block_id, HaveForm::Bitmap { len: len + 1 }, vec![0]),
    );
    assert!(peer.close_cause().is_none());
    peer.step(now, admitted(block_id, tiny(), node_id(3)));
    assert_closed(&peer, Bpv1CloseRow::Row16BadHaveIndex);

    let mut peer = PeerState::new(PeerConfig::for_profile(Profile::TINY), node_id(2), 1);
    peer.step(now, have_in(block_id, fits, vec![0, total - 1]));
    peer.step(now, admitted(block_id, tiny(), node_id(3)));
    assert!(peer.close_cause().is_none());
}

/// `bpv1-close-18` (bpv1-29, bpv1-30): a `WantEnd` for no live `Want`, with
/// `sent` above the request, or with `sent` below the parts that arrived
/// closes 18. An end that counts exactly the parts sent stays open.
#[test]
fn close_18_want_end_without_a_live_want_or_with_a_wrong_count() {
    let now = Instant::now();
    let block_id = block::Hash([0x61; 32]);
    let end = |want_id, sent| PeerInput::RecvWantEnd(RecvWantEnd { want_id, sent });

    let mut peer = tiny_session(block_id, now);
    peer.step(now, end(1, 0));
    assert_closed(&peer, Bpv1CloseRow::Row18BadWantEnd);

    for (sent, closes) in [(1, false), (0, true), (3, true)] {
        let mut peer = tiny_session(block_id, now);
        peer.step(now, recv_meta(block_id));
        peer.step(now, pull(block_id, 0, &[0, 1], now));
        peer.step(now, tiny_part_prefix(block_id, 0));
        peer.step(now, end(1, sent));
        if closes {
            assert_closed(&peer, Bpv1CloseRow::Row18BadWantEnd);
        } else {
            assert!(peer.close_cause().is_none(), "sent {sent}");
        }
    }
    let mut peer = tiny_session(block_id, now);
    peer.step(now, pull(block_id, 0, &[0], now));
    peer.step(now, end(1, 0));
    peer.step(now, end(1, 0));
    assert_closed(&peer, Bpv1CloseRow::Row18BadWantEnd);
}

/// `bpv1-close-20` (decision-24): relay flags from a session that is not
/// the proposer, or push flags past `n − k` in one stripe, close 20. The
/// proposer may flag up to `n` indices per stripe, and a holder `n − k`.
#[test]
fn close_20_push_and_relay_flag_bounds() {
    let now = Instant::now();
    let block_id = block::Hash([0x62; 32]);
    let coding = tiny();
    let bound = coding.n - coding.k;
    // Node 3 proposed the block; node 2 is a holder.
    let holder = || session(node_id(2), now, admitted(block_id, coding, node_id(3)));
    let proposer = || session(node_id(3), now, admitted(block_id, coding, node_id(3)));

    let mut peer = holder();
    peer.step(
        now,
        have_with(block_id, HaveFlags::Push, (0..bound).collect()),
    );
    assert!(peer.close_cause().is_none());
    peer.step(now, have_with(block_id, HaveFlags::Push, vec![bound]));
    assert_closed(&peer, Bpv1CloseRow::Row20BadPushFlags);

    let mut peer = holder();
    peer.step(now, have_with(block_id, HaveFlags::Relay, vec![0]));
    assert_closed(&peer, Bpv1CloseRow::Row20BadPushFlags);

    let mut peer = proposer();
    peer.step(
        now,
        have_with(block_id, HaveFlags::Push, (0..coding.n).collect()),
    );
    assert!(peer.close_cause().is_none());

    let mut peer = proposer();
    peer.step(
        now,
        have_with(block_id, HaveFlags::Relay, (0..bound).collect()),
    );
    assert!(peer.close_cause().is_none());
    peer.step(now, have_with(block_id, HaveFlags::Relay, vec![bound]));
    assert_closed(&peer, Bpv1CloseRow::Row20BadPushFlags);
}

// ============================================================================
// Interleavings
// ============================================================================

proptest! {
    // 48 cases keep this test under a second; the fuzz mesh explores interleavings
    // at length.
    #![proptest_config(ProptestConfig::with_cases(48))]

    /// Three conformant nodes never close each other however their streams
    /// interleave. Node 0 proposes to both relays, which also forward the
    /// block's metadata, parts, and `Have`s to each other, so each table
    /// sees copies from two sessions and each session serves `Want`s whose
    /// ends race new `Want`s. One relay may retire the block mid-flight.
    #[test]
    fn conformant_relay_triangle_never_closes_under_any_interleaving(
        delays_ms in prop::array::uniform6(0u64..60),
        retire_at in prop::option::of((1usize..3, 0u64..200)),
    ) {
        let peer_cfg = PeerConfig::for_profile(Profile::TINY);
        let mut mesh = MachineMesh::new(3, BlockConfig::for_profile(Profile::TINY));
        let link = |ctrl: u64, data: u64| LinkProfile {
            ctrl_delay: Duration::from_millis(ctrl),
            data_delay: Duration::from_millis(data),
            ..LinkProfile::new(Duration::ZERO, 1_000_000)
        };
        let [d0, d1, d2, d3, d4, d5] = delays_ms;
        mesh.connect(0, 1, link(d0, d1), link(d1, d0), peer_cfg, None, None);
        mesh.connect(0, 2, link(d2, d3), link(d3, d2), peer_cfg, None, None);
        mesh.connect(1, 2, link(d4, d5), link(d5, d4), peer_cfg, None, None);

        let block_id = block::Hash([0x9A; 32]);
        let encoded = encode_body(&Profile::TINY, &[0x3A; 256], 2).unwrap();
        let envelope = make_envelope(block_id, encoded.coding, encoded.part_root, mesh.nodes[0].id);
        mesh.propose(0, envelope, encoded.parts);

        let start = mesh.now;
        if let Some((node, after)) = retire_at {
            mesh.run_until(start + Duration::from_millis(after));
            mesh.step_table(
                node,
                BlockInput::TipHeight {
                    height: super::test_support::MESH_HEIGHT + PROPAGATION_DEPTH + 1,
                    hash: TEST_TIP_HASH,
                },
            );
        }
        mesh.run_until(start + Duration::from_secs(3));

        mesh.assert_honest_links_open();
        for node in &mesh.nodes {
            for state in node.peers.values() {
                prop_assert_eq!(state.live_wants_out(), 0, "every Want ended");
            }
        }
        if retire_at.is_none() {
            prop_assert!(mesh.verified(1, &block_id));
            prop_assert!(mesh.verified(2, &block_id));
        }
    }
}
