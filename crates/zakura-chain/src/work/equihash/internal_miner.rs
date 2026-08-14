//! Fork-backed Equihash solving for the experimental internal miner.

use cpu_equihash_solver::tromp::{
    solve_200_9_cancellable, CancellableSolveOutcome, CancellationPoint,
};

use crate::{
    block::Header,
    serialization::{AtLeastOne, ZcashSerialize},
    shutdown::is_shutting_down,
};

use super::Solution;

/// The error type for Equihash solving.
#[derive(Copy, Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("solver was cancelled")]
pub struct SolverCancelled;

/// How a solver run should react to a change while it is in flight.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum SolverAction {
    /// Keep hashing.
    Continue,
    /// Finish the current nonce, then stop before starting another one.
    StopAtNonceBoundary,
    /// Stop at the next digit boundary and discard the current nonce.
    StopNow,
}

fn should_cancel_at(point: CancellationPoint, action: SolverAction) -> bool {
    matches!(
        (point, action),
        (_, SolverAction::StopNow)
            | (
                CancellationPoint::NonceBoundary,
                SolverAction::StopAtNonceBoundary,
            )
    )
}

impl Solution {
    /// Mines and returns one or more [`Solution`]s based on a template `header`.
    /// The returned header contains a valid `nonce` and solution.
    ///
    /// If `cancel_fn()` returns an error, returns early with [`SolverCancelled`].
    ///
    /// The `nonce` in the header template is taken as the starting nonce. If you are running multiple
    /// solvers at the same time, start them with different nonces.
    /// The solution in the header template is ignored.
    ///
    /// This method is CPU and memory-intensive. It uses 144 MB of RAM and one CPU core while running.
    /// It can run for minutes or hours if the network difficulty is high.
    #[allow(clippy::unwrap_in_result)]
    pub fn solve<F>(header: Header, mut cancel_fn: F) -> Result<AtLeastOne<Header>, SolverCancelled>
    where
        F: FnMut() -> Result<(), SolverCancelled>,
    {
        Self::solve_cancellable(header, || {
            if cancel_fn().is_ok() {
                SolverAction::Continue
            } else {
                SolverAction::StopAtNonceBoundary
            }
        })
    }

    /// Mines and returns one or more [`Solution`]s based on a template `header`.
    ///
    /// `solver_action()` can preserve the current nonce, stop before the next
    /// nonce, or discard the current nonce at the next digit boundary.
    #[allow(clippy::unwrap_in_result)]
    pub fn solve_cancellable<F>(
        mut header: Header,
        mut solver_action: F,
    ) -> Result<AtLeastOne<Header>, SolverCancelled>
    where
        F: FnMut() -> SolverAction,
    {
        let mut input = Vec::new();
        header
            .zcash_serialize(&mut input)
            .expect("serialization into a vec can't fail");
        // Take the part of the header before the nonce and solution.
        // This data is kept constant for this solver run.
        let input = &input[0..Solution::INPUT_LENGTH];

        while !is_shutting_down() {
            // Don't run the solver if we'd just cancel it anyway.
            if solver_action() != SolverAction::Continue {
                return Err(SolverCancelled);
            }

            let solve_result = solve_200_9_cancellable(
                input,
                || {
                    // This skips the first nonce, which doesn't matter in practice.
                    Self::next_nonce(&mut header.nonce);
                    Some(*header.nonce)
                },
                |point| {
                    if is_shutting_down() {
                        return true;
                    }

                    should_cancel_at(point, solver_action())
                },
            );

            let solutions = match solve_result.into_outcome() {
                CancellableSolveOutcome::Completed(solutions) => solutions,
                CancellableSolveOutcome::Cancelled => return Err(SolverCancelled),
            };

            // Give an invalidating update precedence over a solution that raced
            // with the callback after the final digit.
            if solver_action() == SolverAction::StopNow {
                return Err(SolverCancelled);
            }

            let mut valid_solutions = Vec::new();

            for solution in &solutions {
                header.solution = Self::from_bytes(solution)
                    .expect("unexpected invalid solution: incorrect length");

                // TODO: work out why we sometimes get invalid solutions here
                //
                // The solver only ever produces (200, 9) `Common` solutions,
                // so verify against those parameters directly rather than
                // binding to a network.
                if let Err(error) = header.solution.check_equihash(&header, 200, 9) {
                    info!(?error, "found invalid solution for header");
                    continue;
                }

                if Self::difficulty_is_valid(&header) {
                    valid_solutions.push(header);
                }
            }

            match valid_solutions.try_into() {
                Ok(at_least_one_solution) => return Ok(at_least_one_solution),
                Err(_is_empty_error) => debug!(
                    solutions = ?solutions.len(),
                    "found valid solutions which did not pass the validity or difficulty checks"
                ),
            }
        }

        Err(SolverCancelled)
    }

    /// Returns `true` if the `nonce` and solution in `header` meet the difficulty threshold.
    ///
    /// # Panics
    ///
    /// - If `header` contains an invalid difficulty threshold.
    fn difficulty_is_valid(header: &Header) -> bool {
        // Simplified from zakura_consensus::block::check::difficulty_is_valid().
        let difficulty_threshold = header
            .difficulty_threshold
            .to_expanded()
            .expect("unexpected invalid header template: invalid difficulty threshold");

        // TODO: avoid calculating this hash multiple times
        let hash = header.hash();

        // Note: this comparison is a u256 integer comparison, like zcashd and bitcoin. Greater
        // values represent *less* work.
        hash <= difficulty_threshold
    }

    /// Modifies `nonce` to be the next integer in big-endian order.
    /// Wraps to zero if the next nonce would overflow.
    fn next_nonce(nonce: &mut [u8; 32]) {
        let _ignore_overflow = crate::primitives::byte_array::increment_big_endian(&mut nonce[..]);
    }
}

#[cfg(test)]
mod tests {
    use super::{should_cancel_at, CancellationPoint, SolverAction};

    #[test]
    fn solver_actions_have_distinct_cancellation_boundaries() {
        assert!(!should_cancel_at(
            CancellationPoint::NonceBoundary,
            SolverAction::Continue
        ));
        assert!(!should_cancel_at(
            CancellationPoint::DigitBoundary,
            SolverAction::Continue
        ));
        assert!(should_cancel_at(
            CancellationPoint::NonceBoundary,
            SolverAction::StopAtNonceBoundary
        ));
        assert!(!should_cancel_at(
            CancellationPoint::DigitBoundary,
            SolverAction::StopAtNonceBoundary
        ));
        assert!(should_cancel_at(
            CancellationPoint::NonceBoundary,
            SolverAction::StopNow
        ));
        assert!(should_cancel_at(
            CancellationPoint::DigitBoundary,
            SolverAction::StopNow
        ));
    }
}
