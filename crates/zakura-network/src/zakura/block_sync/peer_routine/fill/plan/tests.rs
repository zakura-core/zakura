//! Machine tests for the pure fill planners.

use super::*;

fn reference_first_allowed_run(allowed: &[bool]) -> Option<std::ops::Range<usize>> {
    let start = allowed.iter().position(|allowed| *allowed)?;
    let len = allowed[start..]
        .iter()
        .take_while(|allowed| **allowed)
        .count();
    Some(start..start + len)
}

#[test]
fn retry_filter_retains_each_small_allowed_run() {
    for len in 0..=6 {
        for mask in 0..(1usize << len) {
            let allowed: Vec<_> = (0..len)
                .map(|index| mask & (1usize << index) != 0)
                .collect();
            let expected = reference_first_allowed_run(&allowed);
            let items: Vec<_> = (0..len).collect();
            let mut calls = vec![0usize; len];

            let keep = first_allowed_run(&items, |item| {
                calls[*item] += 1;
                allowed[*item]
            });

            assert_eq!(keep, expected, "len={len}, mask={mask:#08b}");

            let visited_len = match &expected {
                Some(range) if range.end < len => range.end + 1,
                Some(range) => range.end,
                None => len,
            };
            for (index, calls) in calls.into_iter().enumerate() {
                assert_eq!(
                    calls,
                    usize::from(index < visited_len),
                    "len={len}, mask={mask:#08b}, index={index}"
                );
            }

            let mut retained = items;
            let mut returned = Vec::new();
            match keep {
                Some(keep) => {
                    let keep_len = keep.len();
                    if keep.start > 0 {
                        returned.extend(retained.drain(..keep.start));
                    }
                    if keep_len < retained.len() {
                        returned.extend(retained.split_off(keep_len));
                    }
                }
                None => {
                    returned.extend(retained.iter().copied());
                    retained.clear();
                }
            }

            let expected_retained: Vec<_> = expected.clone().into_iter().flatten().collect();
            let expected_returned: Vec<_> = (0..len)
                .filter(|index| !expected.as_ref().is_some_and(|range| range.contains(index)))
                .collect();
            assert_eq!(retained, expected_retained, "len={len}, mask={mask:#08b}");
            assert_eq!(returned, expected_returned, "len={len}, mask={mask:#08b}");
            assert_eq!(
                retained.is_empty(),
                expected.is_none(),
                "len={len}, mask={mask:#08b}"
            );
        }
    }
}
