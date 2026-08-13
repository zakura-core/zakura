use super::super::stage::HeaderRule;
use crate::RuleId;

#[test]
fn validation_stages_expose_their_exact_normative_rule_ids() {
    let cases: &[(HeaderRule, &[RuleId])] = &[
        (HeaderRule::EncodingVersionHash, &[RuleId::new("LC-VAL-02")]),
        (HeaderRule::ParentLink, &[RuleId::new("LC-VAL-03")]),
        (HeaderRule::InferredHeight, &[RuleId::new("LC-HEIGHT-01")]),
        (
            HeaderRule::CommitmentStructure,
            &[RuleId::new("LC-COMMIT-01"), RuleId::new("LC-COMMIT-02")],
        ),
        (HeaderRule::CompactTarget, &[RuleId::new("LC-VAL-05")]),
        (HeaderRule::HashToTarget, &[RuleId::new("LC-VAL-05")]),
        (HeaderRule::Equihash, &[RuleId::new("LC-VAL-04")]),
        (
            HeaderRule::ContextualDifficultyAndTime,
            &[
                RuleId::new("LC-VAL-06"),
                RuleId::new("LC-VAL-07"),
                RuleId::new("LC-TIME-01"),
            ],
        ),
        (HeaderRule::LocalFutureTime, &[RuleId::new("LC-VAL-08")]),
        (
            HeaderRule::ValidationLease,
            &[RuleId::new("LC-ANCHOR-03"), RuleId::new("LC-VAL-11")],
        ),
        (HeaderRule::Work, &[RuleId::new("LC-VAL-10")]),
    ];

    for (stage, expected) in cases {
        assert_eq!(stage.rule_ids(), *expected, "{stage:?}");
    }
}
