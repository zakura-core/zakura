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

    /// The mesh checks hold for arbitrary delivery orders and adversaries.
    #[test]
    fn mesh_replay_holds(bytes in prop::collection::vec(any::<u8>(), 0..512)) {
        replay_bpv1_mesh_bytes(&bytes);
    }
}

// Each codec and seed-edit case runs real coding and whole block
// lifecycles, so these take fewer cases; the fuzz targets and the
// `bpv1_quality` example run many more.
proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

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
}

/// Every in-code seed replays without a failed check, and the seeds alone
/// reach every name each target expects, so a short run of a target starts
/// from every alternative it can reach.
#[test]
fn seeds_replay_and_reach_every_expected_name() {
    for target in measure_bpv1_targets(0, 0) {
        assert!(target.inputs > 0, "{} has no seed", target.target);
        assert!(
            target.missing().is_empty(),
            "{}: the seeds miss {:?}",
            target.target,
            target.missing()
        );
    }
}

/// The `Have` seeds double as close-16 tests (decision-33): a bitmap that
/// fits the layout stays open, and a repeated bitmap index, a padding bit,
/// a bitmap sized for another layout, and a list index past the layout
/// each close 16.
#[test]
fn have_form_seeds_close_16_only_when_malformed() {
    let seeds = seed_inputs();
    let closed_row = |name: &str| {
        let seed = seeds
            .iter()
            .find(|seed| seed.target == "bpv1_peer" && seed.name == name)
            .unwrap_or_else(|| panic!("no bpv1_peer seed {name}"));
        crate::zakura::bpv1::peer::replay_bpv1_peer_bytes(&seed.bytes).closed_row
    };
    assert_eq!(closed_row("have-bitmap"), None);
    assert_eq!(closed_row("have-bitmap-repeat"), Some(16));
    assert_eq!(closed_row("have-bitmap-padding"), Some(16));
    assert_eq!(closed_row("have-bitmap-misfit"), Some(16));
    assert_eq!(closed_row("have-past-layout"), Some(16));
}
