//! Randomness, simulator build.
//!
//! Inside a run, [`rng`] values come from one SplitMix64 stream per run. The
//! stream's seed is a single draw from the run's `Plan`, taken by the first
//! value of the run, like the hash seed. So a run replays with its draws, the
//! fuzzer can still vary the stream, and values stay well spread when the
//! `Plan` answers 0. Outside a run it's rand's thread RNG.

use std::{cell::Cell, convert::Infallible, fmt, panic::Location};

use rand_10::{TryCryptoRng, TryRng};

use crate::sim::{executor, DrawKind, DrawSite};

thread_local! {
    /// The current run's stream state, once seeded.
    static RUN_STREAM: Cell<Option<u64>> = const { Cell::new(None) };
}

/// Forgets the run's stream. Called when a run starts and ends.
pub(crate) fn reset_run_stream() {
    let _ = RUN_STREAM.try_with(|state| state.set(None));
}

/// The next value of the current run's stream, seeding it from a `Plan` draw
/// at `site` on first use. `None` outside a run.
fn run_next(site: &'static Location<'static>) -> Option<u64> {
    let executor = executor()?;
    let state = match RUN_STREAM.with(Cell::get) {
        Some(state) => state,
        None => executor.draw(
            DrawSite {
                kind: DrawKind::Rng,
                site,
            },
            0,
        ),
    };
    // SplitMix64. The thread RNG isn't shared between tasks, so a value
    // needs no preemption point.
    let state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    RUN_STREAM.with(|cell| cell.set(Some(state)));
    let mut z = state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    Some(z ^ (z >> 31))
}

/// The thread RNG, as rand's `rng()`. Inside a run, values come from the
/// run's stream.
#[track_caller]
pub fn rng() -> ThreadRng {
    ThreadRng {
        site: Location::caller(),
        thread: None,
    }
}

/// The RNG [`rng`] returns. Each value comes from the run's stream if a run
/// is active when it's asked for, and from rand's thread RNG otherwise.
#[derive(Clone)]
pub struct ThreadRng {
    /// Where the RNG was made, reported if it seeds the run's stream.
    site: &'static Location<'static>,
    /// Rand's thread RNG, made on first use outside a run.
    thread: Option<rand_10::rngs::ThreadRng>,
}

impl ThreadRng {
    /// Rand's thread RNG, for values asked for outside a run.
    fn thread(&mut self) -> &mut rand_10::rngs::ThreadRng {
        self.thread.get_or_insert_with(rand_10::rng)
    }
}

impl TryRng for ThreadRng {
    type Error = Infallible;

    fn try_next_u32(&mut self) -> Result<u32, Infallible> {
        match run_next(self.site) {
            // The high half. The shift leaves 32 bits, so the cast keeps
            // them all.
            Some(value) => Ok((value >> 32) as u32),
            None => self.thread().try_next_u32(),
        }
    }

    fn try_next_u64(&mut self) -> Result<u64, Infallible> {
        match run_next(self.site) {
            Some(value) => Ok(value),
            None => self.thread().try_next_u64(),
        }
    }

    fn try_fill_bytes(&mut self, dst: &mut [u8]) -> Result<(), Infallible> {
        for chunk in dst.chunks_mut(8) {
            match run_next(self.site) {
                Some(value) => chunk.copy_from_slice(&value.to_le_bytes()[..chunk.len()]),
                None => self.thread().try_fill_bytes(chunk)?,
            }
        }
        Ok(())
    }
}

// Rand's thread RNG is a CSPRNG, and code may require that. The sim values
// aren't secret, which is fine for a test build.
impl TryCryptoRng for ThreadRng {}

impl fmt::Debug for ThreadRng {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ThreadRng").finish_non_exhaustive()
    }
}
