//! The response bound: invariants over random responses, and named cases for
//! where the cap and the ending reserve refuse a frame.

use proptest::prelude::*;

use super::*;

proptest! {
    /// Every accepted frame keeps the response within its cap and records
    /// what the test queued. A frame that is not the ending always leaves room
    /// for an ending of the reserved size. A refusal reports the progress
    /// before the frame and changes nothing.
    #[test]
    fn response_bound_keeps_room_for_the_ending(
        cap_frames in 0u32..6,
        cap_bytes in 0u64..200,
        ending_reserve in 0u64..40,
        frames in prop::collection::vec((0u64..60, any::<bool>()), 0..20),
    ) {
        let mut bound = ResponseBound::new(
            ResponseCap { frames: cap_frames, bytes: cap_bytes },
            ending_reserve,
        );
        bound.assert_invariants();
        prop_assert!(
            bound.admit(ending_reserve, true).is_ok(),
            "a fresh response has room for its largest ending"
        );
        // What the test queued so far.
        let mut queued = SinkProgress::default();
        for (len, ends) in frames {
            match bound.admit(len, ends) {
                Ok(progress) => {
                    queued.bytes += len;
                    queued.frames += u32::from(!ends);
                    prop_assert_eq!(progress, queued);
                    bound.progress = progress;
                    bound.assert_invariants();
                    if ends {
                        break;
                    }
                    prop_assert!(
                        bound.admit(ending_reserve, true).is_ok(),
                        "a queued frame left no room for the ending"
                    );
                }
                Err(error) => {
                    prop_assert_eq!(error, SinkError::OverCap { cap: bound.cap, sent: queued });
                    prop_assert_eq!(bound.progress, queued);
                }
            }
        }
    }
}

/// A frame, then whether the bound admits it.
type Case = (u64, bool, bool);

/// Feed `frames` to a bound with `cap` and `ending_reserve`, recording each
/// admitted frame, and check each admission against the case.
fn expect(cap: ResponseCap, ending_reserve: u64, frames: &[Case]) {
    let mut bound = ResponseBound::new(cap, ending_reserve);
    for (index, &(len, ends, admitted)) in frames.iter().enumerate() {
        let result = bound.admit(len, ends);
        assert_eq!(
            result.is_ok(),
            admitted,
            "frame {index}: {len} bytes, ends: {ends}, {result:?}"
        );
        if let Ok(progress) = result {
            bound.progress = progress;
        }
    }
}

#[test]
fn a_frame_may_not_take_the_ending_reserve() {
    let cap = ResponseCap {
        frames: 4,
        bytes: 100,
    };
    expect(
        cap,
        10,
        &[
            (91, false, false),
            (90, false, true),
            (1, false, false),
            (10, true, true),
        ],
    );
}

#[test]
fn the_ending_may_fill_the_cap_exactly() {
    let cap = ResponseCap {
        frames: 4,
        bytes: 100,
    };
    expect(
        cap,
        10,
        &[(80, false, true), (21, true, false), (20, true, true)],
    );
}

#[test]
fn the_frame_count_excludes_the_ending() {
    let cap = ResponseCap {
        frames: 2,
        bytes: 100,
    };
    expect(
        cap,
        0,
        &[
            (1, false, true),
            (1, false, true),
            (1, false, false),
            (1, true, true),
        ],
    );
}

#[test]
fn a_cap_below_the_ending_reserve_rises_to_hold_the_ending() {
    let cap = ResponseCap {
        frames: 0,
        bytes: 5,
    };
    expect(cap, 10, &[(0, false, false), (10, true, true)]);
    let bound = ResponseBound::new(cap, 10);
    assert_eq!(
        bound.cap,
        ResponseCap {
            frames: 0,
            bytes: 10
        }
    );
}
