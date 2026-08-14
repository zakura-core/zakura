use crate::RuleId;

/// Stable preparation stage used for peer attribution and conformance diagnostics.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum HeaderRule {
    /// Canonical signed version and full-header hash.
    EncodingVersionHash,
    /// Exact parent and internal linkage.
    ParentLink,
    /// Checked local height inference.
    InferredHeight,
    /// Height-dependent commitment interpretation.
    CommitmentStructure,
    /// Compact target domain and network limit.
    CompactTarget,
    /// Header hash at or below its target.
    HashToTarget,
    /// Network-bound Equihash shape and proof.
    Equihash,
    /// Branch-local target adjustment and median-time rules.
    ContextualDifficultyAndTime,
    /// Local-clock future-header classification.
    LocalFutureTime,
    /// Exact durable validation lease and trust-anchor identity.
    ValidationLease,
    /// Exact per-block work calculation.
    Work,
}

impl HeaderRule {
    /// Return every normative rule implemented by this validation stage.
    pub const fn rule_ids(self) -> &'static [RuleId] {
        const ENCODING_VERSION_HASH: &[RuleId] = &[RuleId::new("LC-VAL-02")];
        const PARENT_LINK: &[RuleId] = &[RuleId::new("LC-VAL-03")];
        const INFERRED_HEIGHT: &[RuleId] = &[RuleId::new("LC-HEIGHT-01")];
        const COMMITMENT_STRUCTURE: &[RuleId] =
            &[RuleId::new("LC-COMMIT-01"), RuleId::new("LC-COMMIT-02")];
        const TARGET: &[RuleId] = &[RuleId::new("LC-VAL-05")];
        const EQUIHASH: &[RuleId] = &[RuleId::new("LC-VAL-04")];
        const CONTEXTUAL_DIFFICULTY_AND_TIME: &[RuleId] = &[
            RuleId::new("LC-VAL-06"),
            RuleId::new("LC-VAL-07"),
            RuleId::new("LC-TIME-01"),
        ];
        const LOCAL_FUTURE_TIME: &[RuleId] = &[RuleId::new("LC-VAL-08")];
        const VALIDATION_LEASE: &[RuleId] =
            &[RuleId::new("LC-ANCHOR-03"), RuleId::new("LC-VAL-11")];
        const WORK: &[RuleId] = &[RuleId::new("LC-VAL-10")];

        match self {
            Self::EncodingVersionHash => ENCODING_VERSION_HASH,
            Self::ParentLink => PARENT_LINK,
            Self::InferredHeight => INFERRED_HEIGHT,
            Self::CommitmentStructure => COMMITMENT_STRUCTURE,
            Self::CompactTarget | Self::HashToTarget => TARGET,
            Self::Equihash => EQUIHASH,
            Self::ContextualDifficultyAndTime => CONTEXTUAL_DIFFICULTY_AND_TIME,
            Self::LocalFutureTime => LOCAL_FUTURE_TIME,
            Self::ValidationLease => VALIDATION_LEASE,
            Self::Work => WORK,
        }
    }
}
