//! Dedicated threads for trace filesystem work.

use std::{io, sync::Arc, thread};

use tokio::sync::Semaphore;

/// Runs filesystem jobs on their own threads, at most `limit` at once.
///
/// A job keeps its slot until it returns, even after the request that started
/// it is gone. A syscall that never returns therefore strands one of these
/// threads, never a thread of Tokio's blocking pool, whose stuck tasks would
/// also block runtime shutdown.
pub(super) struct FsThreads {
    slots: Arc<Semaphore>,
}

impl FsThreads {
    pub(super) fn new(limit: usize) -> Self {
        Self {
            slots: Arc::new(Semaphore::new(limit)),
        }
    }

    /// Start `job` on a new thread, or fail with `WouldBlock` when every slot is
    /// taken.
    pub(super) fn try_spawn(&self, job: impl FnOnce() + Send + 'static) -> io::Result<()> {
        let slot = self.slots.clone().try_acquire_owned().map_err(|_| {
            io::Error::new(io::ErrorKind::WouldBlock, "trace filesystem threads busy")
        })?;
        thread::Builder::new()
            .name("raw-trace-fs".to_owned())
            .spawn(move || {
                let _slot = slot;
                job();
            })
            .map(drop)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::mpsc, time::Duration};

    #[test]
    fn a_blocked_job_keeps_its_slot_after_its_caller_leaves() {
        let threads = FsThreads::new(1);
        let (release, blocked) = mpsc::sync_channel::<()>(0);
        let (started, entered) = mpsc::sync_channel(0);
        threads
            .try_spawn(move || {
                started.send(()).unwrap();
                let _ = blocked.recv(); // Models a syscall that cannot be cancelled.
            })
            .unwrap();
        entered.recv_timeout(Duration::from_secs(2)).unwrap();
        for _ in 0..100 {
            assert_eq!(
                threads.try_spawn(|| {}).unwrap_err().kind(),
                io::ErrorKind::WouldBlock
            );
        }
        drop(release);
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        let (done, finished) = mpsc::sync_channel(1);
        loop {
            let done = done.clone();
            if threads.try_spawn(move || done.send(()).unwrap()).is_ok() {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "slot was never released"
            );
            thread::yield_now();
        }
        finished.recv_timeout(Duration::from_secs(2)).unwrap();
    }

    #[test]
    fn a_panicking_job_releases_its_slot() {
        let threads = FsThreads::new(1);
        threads.try_spawn(|| panic!("job failed")).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while threads.try_spawn(|| {}).is_err() {
            assert!(
                std::time::Instant::now() < deadline,
                "slot was never released"
            );
            thread::yield_now();
        }
    }
}
