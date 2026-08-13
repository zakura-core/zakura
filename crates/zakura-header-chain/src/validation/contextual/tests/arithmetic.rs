use super::*;

#[test]
fn custom_target_mean_does_not_overflow_u256() {
    let candidate_time =
        DateTime::from_timestamp(2_000_000_000, 0).expect("test timestamp is in range");
    let compact = ExpandedDifficulty::from(U256::MAX).to_compact();
    let expanded = compact
        .to_expanded()
        .expect("the maximum compact-representable target is valid");
    let context = vec![(compact, candidate_time - Duration::seconds(1)); 28];
    let adjustment = AdjustedDifficulty::new_from_header_time(
        candidate_time,
        block::Height(699_999),
        &Network::Mainnet,
        context,
    )
    .expect("the complete context is accepted");

    assert_eq!(adjustment.mean_target_difficulty(), expanded);
}
