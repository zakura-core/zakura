//! Randomness, simulator build.
//!
//! Inside a run, every value [`rng`] produces is a draw from the run's
//! `Plan`, with a preemption point before it. So the fuzzer can steer each
//! value, and a run replays with its draws. Outside a run it's rand's thread
//! RNG.

use std::{convert::Infallible, fmt, panic::Location};

use rand_10::{TryCryptoRng, TryRng};

use crate::sim::{executor, point, DrawKind, DrawSite, Op};

/// The thread RNG, as rand's `rng()`. Inside a run, values come from the
/// `Plan`.
#[track_caller]
pub fn rng() -> ThreadRng {
    let inner = match executor() {
        Some(_) => Inner::Sim(Location::caller()),
        None => Inner::Thread(rand_10::rng()),
    };
    ThreadRng { inner }
}

/// The RNG [`rng`] returns. Whether it draws from the `Plan` is fixed when
/// it's made.
#[derive(Clone)]
pub struct ThreadRng {
    inner: Inner,
}

#[derive(Clone)]
enum Inner {
    Thread(rand_10::rngs::ThreadRng),
    Sim(&'static Location<'static>),
}

impl ThreadRng {
    /// One draw below `bound` (or any `u64` for 0) from the run, with its
    /// point. Zero once the run has ended.
    fn draw(site: &'static Location<'static>, bound: u64) -> u64 {
        point(Op::Draw, site);
        executor().map_or(0, |executor| {
            executor.draw(
                DrawSite {
                    kind: DrawKind::Rng,
                    site,
                },
                bound,
            )
        })
    }
}

impl TryRng for ThreadRng {
    type Error = Infallible;

    fn try_next_u32(&mut self) -> Result<u32, Infallible> {
        match &mut self.inner {
            Inner::Thread(rng) => rng.try_next_u32(),
            Inner::Sim(site) => {
                // The draw is below 2^32, so the cast keeps every bit.
                Ok(ThreadRng::draw(site, 1 << 32) as u32)
            }
        }
    }

    fn try_next_u64(&mut self) -> Result<u64, Infallible> {
        match &mut self.inner {
            Inner::Thread(rng) => rng.try_next_u64(),
            Inner::Sim(site) => Ok(ThreadRng::draw(site, 0)),
        }
    }

    fn try_fill_bytes(&mut self, dst: &mut [u8]) -> Result<(), Infallible> {
        match &mut self.inner {
            Inner::Thread(rng) => rng.try_fill_bytes(dst),
            Inner::Sim(site) => {
                for chunk in dst.chunks_mut(8) {
                    let bytes = ThreadRng::draw(site, 0).to_le_bytes();
                    chunk.copy_from_slice(&bytes[..chunk.len()]);
                }
                Ok(())
            }
        }
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
