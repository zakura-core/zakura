//! Every place the sim-7 plan narrows the sim-7 or sched-4 spec's text,
//! each once, with its approval (the plan's Rule 10). The record lists
//! them all, and sim-7 and sched-4 close only when each is signed off.

/// One narrowing of a spec's text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Deviation {
    /// Its name, as the plan's decision or rule names it.
    pub id: &'static str,
    /// The spec's words it narrows, quoted exactly.
    pub spec: &'static str,
    /// What the plan claims instead.
    pub claim: &'static str,
    /// Adam's sign-off, and when, if given.
    pub approval: Option<&'static str>,
    /// Evan's sign-off, for a narrowing of the text he wrote.
    pub evan: Evan,
}

/// Whether a narrowing needs Evan's agreement as the spec's author, and
/// whether he gave it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Evan {
    /// It narrows nothing he must agree to.
    NotNeeded,
    /// He must agree, and hasn't yet.
    Pending,
    /// He agreed, as recorded.
    Agreed(&'static str),
}

impl Deviation {
    /// Whether every sign-off it needs is given.
    pub fn is_signed_off(&self) -> bool {
        self.approval.is_some() && !matches!(self.evan, Evan::Pending)
    }
}

/// Every narrowing, in the plan's order.
pub const DEVIATIONS: &[Deviation] = &[
    Deviation {
        id: "decision-2",
        spec: "update `crates/zakura-sim/tests/replay_exact.rs`, `tests/header_properties.rs`",
        claim: "no `tests/header_properties.rs`: hdr-14.6's despite-close case and hdr-14's fresh \
                replay of it give that evidence where hdr-14 puts them",
        approval: None,
        evan: Evan::NotNeeded,
    },
    Deviation {
        id: "decision-3",
        spec: "Fresh-process exact replay matches decisions, draws, datagram bytes, \
               observations, and violation witnesses.",
        claim: "a saved failure binds its recording to the fuzz iteration by property ID and \
                failure step; a fuzz iteration records no step digests, so a difference \
                confined to that first iteration isn't caught there",
        approval: None,
        evan: Evan::NotNeeded,
    },
    Deviation {
        id: "decision-7",
        spec: "The minimum per-change passing-witness and registry-completeness gate is \
               required through reg-2.",
        claim: "the gate runs nightly and on dispatch, since no simulator job runs on pull \
                requests",
        approval: None,
        evan: Evan::NotNeeded,
    },
    Deviation {
        id: "decision-8",
        spec: "Corrected fixtures pass those schedules.",
        claim: "the corrected version runs the same saved input and agrees with the affected \
                run up to their first differing step, which is at the fix's site (S4.4 and \
                S4.7); identical schedules wait on Recover replay",
        approval: None,
        evan: Evan::Pending,
    },
    Deviation {
        id: "s4.6-sim-cfg-scope",
        spec: "Both normal and sim configurations compile at the tested revision.",
        claim: "the sim configuration is certified for the packages and targets \
                `qa/suite.toml`'s `sim-package` rows name; `zakurad` doesn't build under the \
                sim cfg",
        approval: None,
        evan: Evan::Pending,
    },
    Deviation {
        id: "rule-9-release-and-archive",
        spec: "The fixed corpus passes in a local release build.",
        claim: "release covers the committed seed set, judged on its bytes, witnesses and end \
                records; hdr-14's histories count as recorded in the `planted` profile, and \
                its catches are rerunnable at S but not replayable byte for byte from the \
                archive",
        approval: Some("Adam, 2026-11-11 (Rule 9, decision 10)"),
        evan: Evan::NotNeeded,
    },
];
