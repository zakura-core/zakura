//! Dedicated threads for trace filesystem work.

use std::{
    io,
    sync::{Arc, LazyLock},
    thread,
};

use tokio::sync::Semaphore;

/// Trace filesystem threads alive at once in the whole process.
pub(super) const MAX_FS_THREADS: usize = 4;

/// The pool every endpoint in this process shares.
static PROCESS: LazyLock<FsThreads> = LazyLock::new(|| FsThreads::new(MAX_FS_THREADS));

/// Runs filesystem jobs on their own threads, at most `limit` at once.
///
/// A job keeps its slot until it returns, even after the request that started
/// it is gone. A syscall that never returns therefore strands one of these
/// threads, never a thread of Tokio's blocking pool, whose stuck tasks would
/// also block runtime shutdown. Nothing can join such a thread, so the limit
/// bounds how many can accumulate.
#[derive(Clone)]
pub(super) struct FsThreads {
    slots: Arc<Semaphore>,
}

impl FsThreads {
    pub(super) fn new(limit: usize) -> Self {
        Self {
            slots: Arc::new(Semaphore::new(limit)),
        }
    }

    /// The process-wide pool. An embedding host can run several nodes, one
    /// after another, in one process; threads stranded by an earlier run still
    /// count against [`MAX_FS_THREADS`] in later runs.
    pub(super) fn process() -> Self {
        PROCESS.clone()
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
    fn every_process_pool_handle_shares_one_limit() {
        let earlier_run = FsThreads::process();
        let (release, blocked) = mpsc::sync_channel::<()>(0);
        let blocked = Arc::new(std::sync::Mutex::new(blocked));
        let (started, entered) = mpsc::sync_channel(MAX_FS_THREADS);
        for _ in 0..MAX_FS_THREADS {
            let (blocked, started) = (blocked.clone(), started.clone());
            earlier_run
                .try_spawn(move || {
                    started.send(()).unwrap();
                    let _ = blocked.lock().map(|blocked| blocked.recv());
                })
                .unwrap();
        }
        for _ in 0..MAX_FS_THREADS {
            entered.recv_timeout(Duration::from_secs(2)).unwrap();
        }
        assert_eq!(
            FsThreads::process().try_spawn(|| {}).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        drop(release);
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
