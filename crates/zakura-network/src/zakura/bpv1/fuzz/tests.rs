//! The BPv1 fuzz drivers on generated inputs and their in-code seeds.

use proptest::prelude::*;

use super::{
    measure_bpv1_targets, replay_bpv1_codec_bytes, replay_bpv1_mesh_bytes, replay_bpv1_wire_bytes,
    seed_inputs,
};

fn no_meter(run: &mut dyn FnMut()) -> usize {
    run();
    0
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// The wire checks hold for arbitrary frames.
    #[test]
    fn wire_replay_holds(bytes in prop::collection::vec(any::<u8>(), 0..256)) {
        replay_bpv1_wire_bytes(&bytes, &mut no_meter);
    }

    /// The codec checks hold for arbitrary bodies and choices.
    #[test]
    fn codec_replay_holds(bytes in prop::collection::vec(any::<u8>(), 0..256)) {
        replay_bpv1_codec_bytes(&bytes);
    }

    /// Every target's checks hold near its seeds: each seed with a few
    /// bytes changed, which reorders deliveries and completions around a
    /// real block's lifecycle.
    #[test]
    fn seed_edits_hold(
        pick in any::<prop::sample::Index>(),
        edits in prop::collection::vec((any::<prop::sample::Index>(), any::<u8>()), 1..8),
    ) {
        let seeds = seed_inputs();
        let seed = &seeds[pick.index(seeds.len())];
        let mut bytes = seed.bytes.clone();
        for (at, value) in edits {
            let at = at.index(bytes.len());
            bytes[at] = value;
        }
        match seed.target {
            "bpv1_peer" => drop(crate::zakura::bpv1::peer::replay_bpv1_peer_bytes(&bytes)),
            "bpv1_block" => drop(crate::zakura::bpv1::blocks::replay_bpv1_block_bytes(&bytes)),
            "bpv1_mesh" => drop(replay_bpv1_mesh_bytes(&bytes)),
            "bpv1_wire" => drop(replay_bpv1_wire_bytes(&bytes, &mut no_meter)),
            _ => drop(replay_bpv1_codec_bytes(&bytes)),
        }
    }

    /// The mesh checks hold for arbitrary delivery orders and adversaries.
    #[test]
    fn mesh_replay_holds(bytes in prop::collection::vec(any::<u8>(), 0..512)) {
        replay_bpv1_mesh_bytes(&bytes);
    }
}

/// Every in-code seed replays without a failed check.
#[test]
fn seeds_replay() {
    let seeds = seed_inputs();
    assert!(!seeds.is_empty());
    let quality = measure_bpv1_targets(0, 0);
    for target in &quality {
        assert!(target.inputs > 0, "{} has no seed", target.target);
    }
}

/// The seeds alone reach every name each target expects, so a short run of
/// a target starts from every alternative it can reach.
#[test]
fn seeds_reach_every_expected_name() {
    for target in measure_bpv1_targets(0, 0) {
        assert!(
            target.missing().is_empty(),
            "{}: the seeds miss {:?}",
            target.target,
            target.missing()
        );
    }
}

/// A short generated run per target holds every check. It is a smoke test
/// of the drivers, not a campaign: a few hundred inputs each.
#[test]
fn generated_inputs_hold() {
    for target in measure_bpv1_targets(300, 0x5EED) {
        assert!(
            target.inputs >= 300,
            "{} replayed too few inputs",
            target.target
        );
    }
}
