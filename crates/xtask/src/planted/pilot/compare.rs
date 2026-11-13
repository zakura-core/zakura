//! The discovery comparison's arms, and what each case's seeds came to in
//! each arm (explorer plan, fuzz-14).
//!
//! Each arm runs the same campaigns with semantic feedback `on` or `off`.
//! The fuzz target's collector runs in both arms; the arm sets only whether
//! libFuzzer reads the collector's features. [`compare`] reports, for each
//! arm, the seeds that detected the intended property, the censored seeds
//! that didn't, the median time to detection, and the applicable runs and
//! unique abstract states of the original and mutant campaigns. It reports
//! only: no threshold decides a result.

use std::{collections::BTreeMap, fmt};

use serde::{Deserialize, Serialize};

use super::{
    classify::{CampaignFacts, Class},
    SeedResult,
};

/// A discovery arm: the semantic feedback mode its campaigns run with.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(super) enum Arm {
    /// libFuzzer reads the collector's features beside its code edges.
    On,
    /// The collector writes to counters libFuzzer doesn't read.
    Off,
}

impl Arm {
    /// The arm's `campaign start --feedback` mode, which the fuzzer logs at
    /// startup.
    pub(super) fn mode(self) -> &'static str {
        match self {
            Arm::On => "on",
            Arm::Off => "off",
        }
    }

    /// The arm whose mode is `text`.
    pub(super) fn parse(text: &str) -> Result<Arm, String> {
        match text {
            "on" => Ok(Arm::On),
            "off" => Ok(Arm::Off),
            other => Err(format!("an arm is `on` or `off`, not `{other}`")),
        }
    }
}

impl fmt::Display for Arm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.mode())
    }
}

/// What one case's seeds came to in one arm.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub(super) struct ArmSummary {
    pub(super) arm: Arm,
    /// Seeds that ran in the arm.
    pub(super) seeds: usize,
    /// Seeds whose mutant campaign found the intended property, and a fresh
    /// process confirmed and replayed it Exact.
    pub(super) detected: usize,
    /// Seeds whose valid mutant campaign ended without the intended
    /// finding: misses, whose time to detection exceeds the campaign's.
    pub(super) censored: usize,
    /// Seeds with no valid mutant campaign to time: harness failures,
    /// baseline failures, invalid mutants and equivalent mutants.
    pub(super) unscored: usize,
    /// The Kaplan-Meier median time to detection, in seconds, over the
    /// detected and censored seeds ([`median_detection`]).
    pub(super) median_detection_secs: Option<f64>,
    /// The original build's campaigns of the case's target.
    pub(super) baseline: CampaignTotals,
    /// The mutant's campaigns.
    pub(super) mutant: CampaignTotals,
}

/// What a set of campaigns showed about the intended property.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub(super) struct CampaignTotals {
    /// Campaigns that left facts.
    pub(super) campaigns: usize,
    /// Those where the intended property applied.
    pub(super) applicable: usize,
    /// Inputs where it applied, summed over the campaigns.
    pub(super) applicable_runs: u64,
    /// The median of the unique abstract states each campaign reached: the
    /// distinct protocol-state features its runs hit. `None` without a
    /// campaign.
    pub(super) features_reached: Option<f64>,
}

/// One seed's place in the comparison.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Scored {
    /// Detected after these seconds.
    Detected(f64),
    /// Not detected in the campaign's seconds.
    Censored(f64),
    /// No valid campaign to time.
    Unscored,
}

/// Where `seed` falls, by its class. Only an intended violation is a
/// detection. A detection without a crash the runner could match to the
/// intended input counts at the campaign's end, an upper bound. Every other
/// valid campaign is censored at its end: a survivor, a not-applicable run,
/// and another violation. Another violation stays censored even when the
/// campaign also saved an intended input, since the case's rules count that
/// campaign as another violation, not as a detection ([`super::classify::discovery`]).
/// The campaign stopped at that finding, so its time to detection is only
/// known to exceed its end.
fn score(seed: &SeedResult) -> Scored {
    let Some(mutant) = &seed.mutant else {
        return Scored::Unscored;
    };
    match seed.result.class {
        Class::IntendedViolation => {
            Scored::Detected(mutant.first_intended_secs.unwrap_or(mutant.worker_secs))
        }
        Class::SurvivedBudget | Class::NotApplicable | Class::OtherViolation => {
            Scored::Censored(mutant.worker_secs)
        }
        Class::BaselineFailure
        | Class::InvalidMutant
        | Class::HarnessFailure
        | Class::Equivalent => Scored::Unscored,
    }
}

/// Each arm's summary of `seeds`, in arm order.
pub(super) fn compare(seeds: &[SeedResult]) -> Vec<ArmSummary> {
    let mut arms: BTreeMap<Arm, Vec<&SeedResult>> = BTreeMap::new();
    for seed in seeds {
        arms.entry(seed.arm).or_default().push(seed);
    }
    arms.into_iter()
        .map(|(arm, seeds)| {
            let scored: Vec<Scored> = seeds.iter().map(|seed| score(seed)).collect();
            let times: Vec<(f64, bool)> = scored
                .iter()
                .filter_map(|scored| match scored {
                    Scored::Detected(secs) => Some((*secs, true)),
                    Scored::Censored(secs) => Some((*secs, false)),
                    Scored::Unscored => None,
                })
                .collect();
            let count = |want: fn(&Scored) -> bool| scored.iter().filter(|s| want(s)).count();
            ArmSummary {
                arm,
                seeds: seeds.len(),
                detected: count(|s| matches!(s, Scored::Detected(_))),
                censored: count(|s| matches!(s, Scored::Censored(_))),
                unscored: count(|s| matches!(s, Scored::Unscored)),
                median_detection_secs: median_detection(&times),
                baseline: totals(seeds.iter().filter_map(|seed| seed.baseline.as_ref())),
                mutant: totals(seeds.iter().filter_map(|seed| seed.mutant.as_ref())),
            }
        })
        .collect()
}

/// The totals of `campaigns`.
fn totals<'a>(campaigns: impl Iterator<Item = &'a CampaignFacts>) -> CampaignTotals {
    let mut totals = CampaignTotals::default();
    let mut features = Vec::new();
    for campaign in campaigns {
        totals.campaigns += 1;
        totals.applicable += usize::from(campaign.applied);
        totals.applicable_runs = totals
            .applicable_runs
            .saturating_add(campaign.applicable_runs);
        features.push(campaign.features_reached);
    }
    totals.features_reached = median(&mut features);
    totals
}

/// The median of `values`: the middle value, or the mean of the two middle
/// values. `None` if there are none.
pub(super) fn median(values: &mut [usize]) -> Option<f64> {
    values.sort_unstable();
    let half = values.len() / 2;
    match values.len() {
        0 => None,
        n if n % 2 == 1 => Some(as_f64(values[half])),
        _ => Some((as_f64(values[half - 1]) + as_f64(values[half])) / 2.0),
    }
}

/// The Kaplan-Meier median of `times`: each run's seconds, and whether it
/// detected then or was censored then, its detection only known to come
/// later. The estimated share of runs still undetected drops at each
/// detection time by the share of the runs at risk that detected then. A
/// run censored at a detection time stays at risk through it. The median is
/// the first detection time where the share reaches one half. `None` if it
/// never does, as when half the runs or more end censored.
pub(super) fn median_detection(times: &[(f64, bool)]) -> Option<f64> {
    /// Room for rounding, so a share of exactly one half reaches it.
    const ROUNDING: f64 = 1e-9;
    let mut sorted = times.to_vec();
    sorted.sort_by(|a, b| a.0.total_cmp(&b.0));
    let mut undetected = 1.0;
    let mut at_risk = sorted.len();
    let mut rest = sorted.as_slice();
    while let Some(&(secs, _)) = rest.first() {
        let tied = rest.iter().take_while(|(time, _)| *time == secs).count();
        let detected = rest[..tied]
            .iter()
            .filter(|(_, detected)| *detected)
            .count();
        if detected > 0 {
            undetected *= 1.0 - as_f64(detected) / as_f64(at_risk);
            if undetected <= 0.5 + ROUNDING {
                return Some(secs);
            }
        }
        at_risk -= tied;
        rest = &rest[tied..];
    }
    None
}

/// `value` as a float. Counts of features, runs and campaigns fit in a
/// `u32`, which a float holds exactly.
fn as_f64(value: usize) -> f64 {
    f64::from(u32::try_from(value).unwrap_or(u32::MAX))
}
