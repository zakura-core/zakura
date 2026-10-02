//! Seeded runs of the structure-aware generators through the differential checks.
//!
//! The coverage guards fail if a generator change stops producing accepted spends, rejected
//! spends, or accepted spends that depend on a successful signature check.

use arbitrary::Unstructured;
use rand::{rngs::StdRng, Rng as _, SeedableRng as _};
use zakura_script_oracle::{
    generate::{ScriptCase, TransactionCase},
    Verdict,
};

/// Counts of verdicts over generated cases.
#[derive(Debug, Default)]
struct Tally {
    accepted: usize,
    rejected: usize,
    /// Accepted cases whose acceptance requires a successful signature check.
    signed: usize,
}

impl Tally {
    fn add(&mut self, verdict: Verdict, needs_signature: bool) {
        match verdict {
            Verdict::Accepted => {
                self.accepted += 1;
                self.signed += usize::from(needs_signature);
            }
            Verdict::Rejected => self.rejected += 1,
        }
    }

    /// Asserts minimum percentages of `total` cases.
    fn assert_at_least(&self, total: usize, accepted: usize, rejected: usize, signed: usize) {
        println!("{self:?} of {total}");
        assert!(self.accepted * 100 >= total * accepted, "{self:?}");
        assert!(self.rejected * 100 >= total * rejected, "{self:?}");
        assert!(self.signed * 100 >= total * signed, "{self:?}");
    }
}

/// Runs `check` on `count` seeded inputs of up to 4 KiB.
fn seeded(seed: u64, count: usize, mut check: impl FnMut(&mut Unstructured)) {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut bytes = vec![0; 4_096];
    for _ in 0..count {
        let len = rng.gen_range(64..=bytes.len());
        rng.fill(&mut bytes[..len]);
        check(&mut Unstructured::new(&bytes[..len]));
    }
}

#[test]
fn generated_scripts_agree() {
    const CASES: usize = 20_000;
    let mut tally = Tally::default();
    seeded(0x5c21_9701, CASES, |u| {
        let case = ScriptCase::arbitrary(u).expect("generation succeeds on 64+ bytes");
        tally.add(case.check(), case.needs_signature());
    });
    tally.assert_at_least(CASES, 10, 40, 5);
}

#[test]
fn generated_transactions_agree() {
    const CASES: usize = 5_000;
    let (mut tally, mut inputs) = (Tally::default(), 0);
    seeded(0x7a11_0004, CASES, |u| {
        let case = TransactionCase::arbitrary(u).expect("generation succeeds on 64+ bytes");
        for (verdict, &needs_signature) in case
            .check()
            .into_iter()
            .flatten()
            .zip(case.needs_signature())
        {
            tally.add(verdict, needs_signature);
            inputs += 1;
        }
    });
    tally.assert_at_least(inputs, 10, 40, 5);
}
