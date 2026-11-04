//! The response bound checked against an independent model.

use proptest::prelude::*;

use super::*;

/// The model's record of one response: what it queued and whether it ended.
#[derive(Debug, Default)]
struct Model {
    frames: u32,
    bytes: u64,
    ended: bool,
}

proptest! {
    /// Every accepted frame keeps the response within its cap. A frame that
    /// is not the ending always leaves room for an ending of the reserved
    /// size, and the bound refuses exactly what the model refuses.
    #[test]
    fn response_bound_keeps_room_for_the_ending(
        cap_frames in 0u32..6,
        cap_bytes in 0u64..200,
        ending_reserve in 0u64..40,
        frames in prop::collection::vec((0u64..60, any::<bool>()), 0..20),
    ) {
        let cap = ResponseCap { frames: cap_frames, bytes: cap_bytes };
        let mut bound = ResponseBound::new(cap, ending_reserve);
        // The cap always holds the largest ending.
        let cap_bytes = cap_bytes.max(ending_reserve);
        let mut model = Model::default();
        for (len, ends) in frames {
            if model.ended {
                break;
            }
            let fits_bytes = if ends {
                model.bytes + len <= cap_bytes
            } else {
                model.bytes + len + ending_reserve <= cap_bytes
            };
            let fits_frames = ends || model.frames < cap_frames;
            let admitted = bound.admit(len, ends);
            prop_assert_eq!(admitted.is_ok(), fits_bytes && fits_frames);
            match admitted {
                Ok(progress) => {
                    model.bytes += len;
                    if ends {
                        model.ended = true;
                    } else {
                        model.frames += 1;
                    }
                    prop_assert_eq!(progress, SinkProgress { frames: model.frames, bytes: model.bytes });
                    bound.progress = progress;
                    prop_assert!(model.bytes <= cap_bytes);
                    if !ends {
                        prop_assert!(bound.admit(ending_reserve, true).is_ok());
                    }
                }
                Err(error) => {
                    // A refusal reports the progress before the frame and
                    // changes nothing.
                    let sent = SinkProgress { frames: model.frames, bytes: model.bytes };
                    let cap = ResponseCap { frames: cap_frames, bytes: cap_bytes };
                    prop_assert_eq!(error, SinkError::OverCap { cap, sent });
                }
            }
        }
    }
}
