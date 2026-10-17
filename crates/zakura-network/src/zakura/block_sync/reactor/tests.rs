use super::*;

#[test]
fn fork_repair_requires_matching_authority_and_acknowledged_reset() {
    let anchor = zakura_header_chain::Frontier::new(block::Height(1), block::Hash([1; 32]));
    let verified = zakura_header_chain::Frontier::new(block::Height(2), block::Hash([0xf2; 32]));
    let epoch = zakura_header_chain::BodyWorkEpoch::new(0);
    let repair = BodyForkRepair {
        body_work_epoch: epoch,
        verified,
        anchor,
        reset_epoch_before: 4,
    };
    let mut sequencer = initial_view(BlockSyncFrontiers {
        finalized_height: anchor.height,
        verified_block_tip: anchor.height,
        verified_block_hash: anchor.hash,
    });
    sequencer.reset_epoch = 4;

    assert!(
        !body_fork_repair_applied(None, epoch, Some(verified), anchor, sequencer),
        "a mirror still at the anchor can precede a queued fork advance"
    );
    assert!(
        !body_fork_repair_applied(Some(repair), epoch, Some(verified), anchor, sequencer),
        "queueing the reset does not acknowledge it"
    );
    sequencer.reset_epoch += 1;
    assert!(body_fork_repair_applied(
        Some(repair),
        epoch,
        Some(verified),
        anchor,
        sequencer
    ));

    let mut wrong_hash = sequencer;
    wrong_hash.verified_hash = block::Hash([0xa1; 32]);
    let mut advanced = sequencer;
    advanced.verified_tip = verified.height;
    advanced.verified_hash = verified.hash;
    let grown_fork = zakura_header_chain::Frontier::new(block::Height(3), block::Hash([0xf3; 32]));
    let other_fork = zakura_header_chain::Frontier::new(verified.height, block::Hash([0xe2; 32]));
    let other_anchor = zakura_header_chain::Frontier::new(anchor.height, block::Hash([0xa1; 32]));

    for (epoch, verified, anchor, sequencer) in [
        (epoch, Some(verified), anchor, wrong_hash),
        (epoch, Some(verified), anchor, advanced),
        (epoch, Some(grown_fork), anchor, sequencer),
        (epoch, Some(other_fork), anchor, sequencer),
        (epoch, Some(verified), other_anchor, sequencer),
        (epoch, None, anchor, sequencer),
        (
            zakura_header_chain::BodyWorkEpoch::new(1),
            Some(verified),
            anchor,
            sequencer,
        ),
    ] {
        assert!(!body_fork_repair_applied(
            Some(repair),
            epoch,
            verified,
            anchor,
            sequencer
        ));
    }
}
