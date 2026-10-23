//! The `Plan`: every choice a run makes.
//!
//! A run asks for two kinds of choice. A pick chooses one enabled action at
//! each step. A draw answers a random value that production code asked for,
//! such as an unbiased `select!` start. Both come from a [`PlanInput`]:
//! fuzz bytes, decoded lazily, or a readable [`Plan`] script.
//!
//! Once bytes run out, picks fall back to round-robin over the enabled set and
//! draws to a fair default, so an empty or truncated input is still a valid,
//! bounded run (M1.4).

use std::{collections::VecDeque, fmt};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{Category, Identity};

/// A `u64` that serializes as a lowercase `0x` hex string, so JSON tools keep
/// every bit. Only the canonical form deserializes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HexU64(pub u64);

impl fmt::Display for HexU64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:#x}", self.0)
    }
}

impl Serialize for HexU64 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for HexU64 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        text.strip_prefix("0x")
            .and_then(|digits| u64::from_str_radix(digits, 16).ok())
            .map(HexU64)
            .filter(|value| value.to_string() == text)
            .ok_or_else(|| serde::de::Error::custom(format!("not a canonical 0x hex u64: {text}")))
    }
}

/// A readable list of choices, one per real choice or draw, in run order.
///
/// A pick with one enabled action, or a draw with bound 1, isn't a real
/// choice and has no entry.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Plan {
    /// The choices, in the order the run asks for them.
    pub choices: Vec<PlanChoice>,
}

impl Plan {
    /// A plan input that decodes `bytes` as fuzz input.
    pub fn from_fuzz(bytes: &[u8]) -> PlanInput {
        PlanInput::Bytes(bytes.to_vec())
    }
}

/// One entry of a [`Plan`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PlanChoice {
    /// Pick the enabled action with this identity.
    Pick(String),
    /// Answer a draw with this value, reduced below the draw's bound.
    Draw(HexU64),
}

/// Where a run's choices come from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlanInput {
    /// Fuzz bytes, decoded one choice at a time.
    Bytes(Vec<u8>),
    /// A scripted plan that follows picks by identity. Test scaffolding only.
    /// Trace replay is separate (traces.md, Exact).
    Script(Plan),
}

/// What made a choice.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum By {
    /// Input bytes, or a [`PlanInput::Script`] entry, which has no offset.
    Bytes,
    /// The fallback, once the input ran out.
    Default,
    /// The only option.
    Forced,
}

/// A scripted plan that the run couldn't follow.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlanError {
    /// The script picks an action that isn't enabled.
    NotEnabled {
        /// The scripted identity.
        wanted: String,
    },
    /// The script has a pick where the run asked for a draw, or the reverse.
    Mismatch {
        /// What the run asked for: `pick` or `draw`.
        wanted: &'static str,
        /// The script's entry.
        found: PlanChoice,
    },
}

impl fmt::Display for PlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlanError::NotEnabled { wanted } => write!(f, "scripted pick {wanted} isn't enabled"),
            PlanError::Mismatch { wanted, found } => {
                write!(
                    f,
                    "the run asked for a {wanted}, but the script has {found:?}"
                )
            }
        }
    }
}

impl std::error::Error for PlanError {}

/// A pick: the chosen index into the enabled set, and what chose it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Decision {
    /// Index into the enabled set.
    pub(crate) index: usize,
    /// What made the choice.
    pub(crate) by: By,
    /// Where the choice's bytes start in the input, when `by` is `Bytes`.
    pub(crate) offset: Option<usize>,
}

/// A draw's answer, already below its bound, and what chose it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "the draw callback lands in the next PR")
)]
pub(crate) struct Answer {
    /// The value production code sees.
    pub(crate) value: u64,
    /// What made the choice.
    pub(crate) by: By,
    /// Where the choice's bytes start in the input, when `by` is `Bytes`.
    pub(crate) offset: Option<usize>,
}

/// The remaining input.
#[derive(Debug)]
enum Input {
    Bytes { bytes: Vec<u8>, offset: usize },
    Script(VecDeque<PlanChoice>),
}

/// Makes a run's choices from its [`PlanInput`].
#[derive(Debug)]
pub(crate) struct Decider {
    input: Input,
    /// The previous pick's `(category, key)`. Every pick moves it.
    cursor: Option<(Category, String)>,
    /// The real choices made so far, as a script that replays them.
    resolved: Vec<PlanChoice>,
}

impl Decider {
    /// A decider that reads `input` from its start.
    pub(crate) fn new(input: PlanInput) -> Self {
        let input = match input {
            PlanInput::Bytes(bytes) => Input::Bytes { bytes, offset: 0 },
            PlanInput::Script(plan) => Input::Script(plan.choices.into()),
        };
        Decider {
            input,
            cursor: None,
            resolved: Vec::new(),
        }
    }

    /// Picks one of `options`, which must be non-empty, sorted and free of
    /// duplicate identities.
    ///
    /// One option is forced and takes no input. Otherwise bytes choose
    /// `value % n`, a script entry chooses by identity, and an exhausted input
    /// takes the first option whose key sorts after the previous pick,
    /// wrapping to the start.
    pub(crate) fn pick(&mut self, options: &[Identity]) -> Result<Decision, PlanError> {
        assert!(!options.is_empty(), "a pick needs an enabled action");
        debug_assert!(
            options.windows(2).all(|w| w[0] < w[1]),
            "the enabled set is sorted and has no duplicates"
        );
        let n = options.len();
        let decision = if n == 1 {
            Decision {
                index: 0,
                by: By::Forced,
                offset: None,
            }
        } else {
            // usize always fits in u128.
            let n = n as u128;
            match self.next_input(width(n)) {
                Some(Next::Bytes { value, offset }) => Decision {
                    // The remainder is below n, which came from a usize.
                    index: (u128::from(value) % n) as usize,
                    by: By::Bytes,
                    offset: Some(offset),
                },
                Some(Next::Script(PlanChoice::Pick(wanted))) => Decision {
                    index: options
                        .iter()
                        .position(|o| o.to_string() == wanted)
                        .ok_or(PlanError::NotEnabled { wanted })?,
                    by: By::Bytes,
                    offset: None,
                },
                Some(Next::Script(found)) => {
                    return Err(PlanError::Mismatch {
                        wanted: "pick",
                        found,
                    })
                }
                None => Decision {
                    index: self.round_robin(options),
                    by: By::Default,
                    offset: None,
                },
            }
        };
        let picked = &options[decision.index];
        if decision.by == By::Bytes {
            self.resolved.push(PlanChoice::Pick(picked.to_string()));
        }
        self.cursor = Some((picked.category, picked.key.clone()));
        Ok(decision)
    }

    /// Picks one of `options` by round-robin alone, ignoring the input, as
    /// the drain after a budget does. The same preconditions as
    /// [`pick`](Self::pick) apply.
    pub(crate) fn pick_round_robin(&mut self, options: &[Identity]) -> Decision {
        assert!(!options.is_empty(), "a pick needs an enabled action");
        let decision = Decision {
            index: self.round_robin(options),
            by: if options.len() == 1 {
                By::Forced
            } else {
                By::Default
            },
            offset: None,
        };
        let picked = &options[decision.index];
        self.cursor = Some((picked.category, picked.key.clone()));
        decision
    }

    /// Answers a draw below `bound`, or any `u64` when `bound` is 0.
    ///
    /// `ordinal` counts this draw among its task's draws of the same kind,
    /// from 1. An exhausted input answers [`default_draw`] for it.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "the draw callback lands in the next PR")
    )]
    pub(crate) fn draw(&mut self, bound: u64, ordinal: u32) -> Result<Answer, PlanError> {
        if bound == 1 {
            return Ok(Answer {
                value: 0,
                by: By::Forced,
                offset: None,
            });
        }
        let range = match bound {
            0 => 1 << 64,
            bound => u128::from(bound),
        };
        let answer = match self.next_input(width(range)) {
            Some(Next::Bytes { value, offset }) => Answer {
                value: reduce(value, bound),
                by: By::Bytes,
                offset: Some(offset),
            },
            Some(Next::Script(PlanChoice::Draw(HexU64(value)))) => Answer {
                value: reduce(value, bound),
                by: By::Bytes,
                offset: None,
            },
            Some(Next::Script(found)) => {
                return Err(PlanError::Mismatch {
                    wanted: "draw",
                    found,
                })
            }
            None => Answer {
                value: default_draw(bound, ordinal),
                by: By::Default,
                offset: None,
            },
        };
        if answer.by == By::Bytes {
            self.resolved.push(PlanChoice::Draw(HexU64(answer.value)));
        }
        Ok(answer)
    }

    /// The real choices made so far, as a script that replays them. Choices
    /// after it ran out are the same defaults on replay.
    pub(crate) fn resolved(&self) -> Plan {
        Plan {
            choices: self.resolved.clone(),
        }
    }

    /// The next choice's input: `width` little-endian bytes, or a script
    /// entry. `None` once the input is exhausted.
    ///
    /// A read cut short by the end of the bytes is zero-padded and still
    /// counts as bytes. The input is exhausted after it.
    fn next_input(&mut self, width: usize) -> Option<Next> {
        match &mut self.input {
            Input::Bytes { bytes, offset } => {
                let start = *offset;
                let available = bytes.get(start..).unwrap_or_default();
                if available.is_empty() {
                    None
                } else {
                    let mut le = [0u8; 8];
                    let taken = available.len().min(width);
                    le[..taken].copy_from_slice(&available[..taken]);
                    *offset = start + taken;
                    Some(Next::Bytes {
                        value: u64::from_le_bytes(le),
                        offset: start,
                    })
                }
            }
            Input::Script(choices) => choices.pop_front().map(Next::Script),
        }
    }

    /// The round-robin default: the first option whose key sorts after the
    /// previous pick, wrapping to the start.
    ///
    /// It's fair only while the enabled set stays fixed: each of `k` options
    /// is then picked within `k` steps. A changing set can keep putting one
    /// off. With `{a, z}`, then `{b, z}`, then `{c, z}`, each pick lands on
    /// the new key after the previous one, and `z` waits.
    fn round_robin(&self, options: &[Identity]) -> usize {
        let Some((category, key)) = &self.cursor else {
            return 0;
        };
        options
            .iter()
            .position(|o| (o.category, &o.key) > (*category, key))
            .unwrap_or(0)
    }
}

/// One choice's raw input.
enum Next {
    Bytes { value: u64, offset: usize },
    Script(PlanChoice),
}

/// The fair default for an exhausted draw: `(ordinal - 1) % bound`, so
/// successive `select!` polls in one task rotate through their branches
/// instead of always starting at branch 0. A bound-0 draw, a seed, gets 0.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "the draw callback lands in the next PR")
)]
pub(crate) fn default_draw(bound: u64, ordinal: u32) -> u64 {
    debug_assert!(ordinal >= 1, "draw ordinals count from 1");
    match bound {
        0 => 0,
        bound => u64::from(ordinal.saturating_sub(1)) % bound,
    }
}

/// `value` reduced below `bound`, or kept whole when `bound` is 0.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "the draw callback lands in the next PR")
)]
fn reduce(value: u64, bound: u64) -> u64 {
    match bound {
        0 => value,
        bound => value % bound,
    }
}

/// How many input bytes choose among `range` values: 1 up to 256, 2 up to
/// 65,536, then 4, then 8.
fn width(range: u128) -> usize {
    match range {
        0..=0x100 => 1,
        0x101..=0x1_0000 => 2,
        0x1_0001..=0x1_0000_0000 => 4,
        _ => 8,
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use zakura_scheduler::{At, Op};

    use super::*;

    /// `n` task identities, `t0.1` to `t<n-1>.1`, sorted.
    fn tasks(n: usize) -> Vec<Identity> {
        let mut ids: Vec<Identity> = (0..n)
            .map(|i| {
                Identity::task(
                    format!("t{i:03}.1"),
                    At {
                        op: Op::Poll,
                        n: 1,
                        site: None,
                    },
                )
            })
            .collect();
        ids.sort();
        ids
    }

    /// A decider over fuzz `bytes`.
    fn bytes(bytes: &[u8]) -> Decider {
        Decider::new(Plan::from_fuzz(bytes))
    }

    #[test]
    fn empty_input_is_all_defaults() {
        let options = tasks(3);
        let mut d = bytes(&[]);
        let picks: Vec<_> = (0..6).map(|_| d.pick(&options).unwrap()).collect();
        assert!(picks
            .iter()
            .all(|p| p.by == By::Default && p.offset.is_none()));
        let indices: Vec<_> = picks.iter().map(|p| p.index).collect();
        assert_eq!(indices, [0, 1, 2, 0, 1, 2]);
        assert_eq!(d.draw(3, 1).unwrap().by, By::Default);
        assert!(d.resolved().choices.is_empty());
    }

    #[test]
    fn a_sole_option_is_forced_and_takes_no_bytes() {
        let mut d = bytes(&[1]);
        let forced = d.pick(&tasks(1)).unwrap();
        assert_eq!((forced.index, forced.by), (0, By::Forced));
        assert_eq!(d.draw(1, 1).unwrap().by, By::Forced);
        let next = d.pick(&tasks(2)).unwrap();
        assert_eq!((next.index, next.by, next.offset), (1, By::Bytes, Some(0)));
    }

    #[test]
    fn widths_follow_the_option_count() {
        // 256 options take one byte, 257 take two, little-endian.
        let mut d = bytes(&[255, 0x02, 0x01, 7]);
        assert_eq!(d.pick(&tasks(256)).unwrap().index, 255);
        let two = d.pick(&tasks(300)).unwrap();
        assert_eq!((two.index, two.offset), (0x0102, Some(1)));
        assert_eq!(d.draw(5, 1).unwrap().value, 2);
        // A bound-0 draw takes eight bytes and keeps the whole value.
        let mut d = bytes(&[1, 2, 3, 4, 5, 6, 7, 8]);
        let seed = d.draw(0, 1).unwrap();
        assert_eq!(seed.value, 0x0807_0605_0403_0201);
        assert_eq!((seed.by, seed.offset), (By::Bytes, Some(0)));
    }

    #[test]
    fn wide_bounds_take_four_and_eight_bytes() {
        let mut d = bytes(&[
            0x34, 0x12, // bound 0x1_0000: two bytes
            0x78, 0x56, 0x34, 0x12, // bound 0x1_0001: four bytes
            0xff, 0xff, 0xff, 0xff, // bound 2^32: four bytes
            1, 2, 3, 4, 5, 6, 7, 8, // bound 2^32 + 1: eight bytes
        ]);
        let cases = [
            (0x1_0000, 0x1234, 0),
            (0x1_0001, 0x1234_5678 % 0x1_0001, 2),
            (1 << 32, 0xffff_ffff, 6),
            ((1 << 32) + 1, 0x0807_0605_0403_0201 % ((1 << 32) + 1), 10),
        ];
        for (bound, value, offset) in cases {
            let answer = d.draw(bound, 1).unwrap();
            assert_eq!(
                (answer.value, answer.by, answer.offset),
                (value, By::Bytes, Some(offset)),
                "bound {bound:#x}"
            );
        }
        assert_eq!(d.draw(2, 1).unwrap().by, By::Default, "all 18 bytes used");
    }

    #[test]
    fn truncated_input_pads_then_runs_out() {
        // Two bytes wanted, one left: the value is 0x0001, still from bytes.
        let mut d = bytes(&[9, 1]);
        assert_eq!(d.pick(&tasks(4)).unwrap().index, 1);
        let short = d.pick(&tasks(300)).unwrap();
        assert_eq!(
            (short.index, short.by, short.offset),
            (1, By::Bytes, Some(1))
        );
        assert_eq!(d.pick(&tasks(4)).unwrap().by, By::Default);
        let seed = d.draw(0, 1).unwrap();
        assert_eq!((seed.value, seed.by), (0, By::Default));
    }

    #[test]
    fn default_draws_rotate_but_seeds_are_zero() {
        let values: Vec<_> = (1..=7).map(|n| default_draw(3, n)).collect();
        assert_eq!(values, [0, 1, 2, 0, 1, 2, 0]);
        assert_eq!(default_draw(0, 5), 0);
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "draw ordinals count from 1")]
    fn default_draw_rejects_ordinal_zero() {
        default_draw(3, 0);
    }

    #[test]
    fn round_robin_follows_keys_as_the_set_changes() {
        let all = tasks(4);
        let mut d = bytes(&[]);
        assert_eq!(d.pick(&all).unwrap().index, 0);
        // t000 left. The next key after it is t001.
        assert_eq!(d.pick(&all[1..]).unwrap().index, 0);
        // t002 left. The next key after t001 is t003.
        let without_t002 = [all[0].clone(), all[1].clone(), all[3].clone()];
        assert_eq!(d.pick(&without_t002).unwrap().index, 2);
        // Wrap to the start.
        assert_eq!(d.pick(&all).unwrap().index, 0);
    }

    #[test]
    fn script_follows_identities() {
        let options = tasks(3);
        let script = Plan {
            choices: vec![
                PlanChoice::Pick(options[2].to_string()),
                PlanChoice::Draw(HexU64(7)),
            ],
        };
        let mut d = Decider::new(PlanInput::Script(script));
        let pick = d.pick(&options).unwrap();
        assert_eq!((pick.index, pick.by, pick.offset), (2, By::Bytes, None));
        assert_eq!(d.draw(3, 1).unwrap().value, 1);
        // The script ran out, so round-robin goes on after t002.
        assert_eq!(d.pick(&options).unwrap().index, 0);
    }

    #[test]
    fn script_errors_name_the_problem() {
        let options = tasks(2);
        let missing = Plan {
            choices: vec![PlanChoice::Pick("task:gone.1:poll#1".into())],
        };
        assert_eq!(
            Decider::new(PlanInput::Script(missing)).pick(&options),
            Err(PlanError::NotEnabled {
                wanted: "task:gone.1:poll#1".into()
            })
        );
        let draw_first = Plan {
            choices: vec![PlanChoice::Draw(HexU64(1))],
        };
        assert!(matches!(
            Decider::new(PlanInput::Script(draw_first)).pick(&options),
            Err(PlanError::Mismatch { wanted: "pick", .. })
        ));
    }

    #[test]
    fn plans_and_hex_round_trip_as_json() {
        let plan = Plan {
            choices: vec![
                PlanChoice::Pick("task:root.1:poll#1".into()),
                PlanChoice::Draw(HexU64(u64::MAX)),
            ],
        };
        let json = serde_json::to_string(&plan).unwrap();
        assert_eq!(
            json,
            r#"{"choices":[{"pick":"task:root.1:poll#1"},{"draw":"0xffffffffffffffff"}]}"#
        );
        assert_eq!(serde_json::from_str::<Plan>(&json).unwrap(), plan);
        for bad in [r#""0X1""#, r#""0x01""#, r#""1""#, r#""0xA""#] {
            assert!(serde_json::from_str::<HexU64>(bad).is_err(), "{bad}");
        }
        assert!(serde_json::from_str::<Plan>(r#"{"choices":[],"extra":1}"#).is_err());
    }

    /// One choice a test run asks for: a pick among this many tasks, or a
    /// draw with this bound.
    #[derive(Clone, Debug)]
    enum Ask {
        Pick(usize),
        Draw(u64),
    }

    /// Runs `asks` through `d`, returning each pick's index or draw's value.
    fn answers(d: &mut Decider, asks: &[Ask]) -> Vec<u64> {
        asks.iter()
            .enumerate()
            .map(|(i, ask)| match ask {
                // usize always fits in u64 on supported targets.
                Ask::Pick(n) => d.pick(&tasks(*n)).unwrap().index as u64,
                // The test's ordinal only needs to differ between draws.
                Ask::Draw(bound) => d.draw(*bound, i as u32 + 1).unwrap().value,
            })
            .collect()
    }

    fn ask() -> impl Strategy<Value = Ask> {
        prop_oneof![
            (1usize..300).prop_map(Ask::Pick),
            prop_oneof![Just(0u64), 1u64..70_000].prop_map(Ask::Draw),
        ]
    }

    proptest! {
        #[test]
        fn round_robin_fair(k in 1usize..24, prefix in proptest::collection::vec(any::<u8>(), 0..16)) {
            // After any input runs out, each of k continuously enabled
            // actions is picked within every k consecutive steps.
            let options = tasks(k);
            let mut d = bytes(&prefix);
            for _ in 0..prefix.len() {
                d.pick(&options).unwrap();
            }
            let picks: Vec<usize> = (0..3 * k).map(|_| d.pick(&options).unwrap().index).collect();
            for window in picks.windows(k) {
                let mut seen = window.to_vec();
                seen.sort_unstable();
                seen.dedup();
                prop_assert_eq!(seen.len(), k);
            }
        }

        #[test]
        fn resolved_script_replays_bytes(
            input in proptest::collection::vec(any::<u8>(), 0..64),
            asks in proptest::collection::vec(ask(), 0..48),
        ) {
            let mut from_bytes = bytes(&input);
            let expected = answers(&mut from_bytes, &asks);
            let mut scripted = Decider::new(PlanInput::Script(from_bytes.resolved()));
            prop_assert_eq!(answers(&mut scripted, &asks), expected);
        }

        #[test]
        fn every_answer_is_in_range(
            input in proptest::collection::vec(any::<u8>(), 0..64),
            asks in proptest::collection::vec(ask(), 0..48),
        ) {
            let mut d = bytes(&input);
            for (value, ask) in answers(&mut d, &asks).into_iter().zip(&asks) {
                match ask {
                    // usize always fits in u64 on supported targets.
                    Ask::Pick(n) => prop_assert!(value < *n as u64),
                    Ask::Draw(0) => {}
                    Ask::Draw(bound) => prop_assert!(value < *bound),
                }
            }
        }
    }
}
