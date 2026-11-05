//! One dedicated filesystem worker, with admission owned by the actual job.

use std::{
    io,
    sync::{mpsc, Arc},
    thread,
};
use tokio::sync::Semaphore;

type Job = Box<dyn FnOnce() + Send>;

/// At most one queued or running filesystem job, even after its caller cancels.
/// A stuck syscall occupies this dedicated thread, never Tokio's blocking pool.
pub(super) struct Worker {
    jobs: mpsc::SyncSender<Job>,
    admission: Arc<Semaphore>,
}

impl Worker {
    pub(super) fn new() -> io::Result<Self> {
        let (jobs, receiver) = mpsc::sync_channel::<Job>(1);
        thread::Builder::new()
            .name("raw-trace-export".to_owned())
            .spawn(move || {
                while let Ok(job) = receiver.recv() {
                    job();
                }
            })?;
        Ok(Self {
            jobs,
            admission: Arc::new(Semaphore::new(1)),
        })
    }

    /// Refuse work while a previous job still owns admission. The permit is
    /// captured in the job, not held by the cancellable HTTP future.
    pub(super) fn try_execute(&self, work: impl FnOnce() + Send + 'static) -> io::Result<()> {
        let permit = self.admission.clone().try_acquire_owned().map_err(|_| {
            io::Error::new(io::ErrorKind::WouldBlock, "trace filesystem worker busy")
        })?;
        self.jobs
            .try_send(Box::new(move || {
                let _permit = permit;
                work();
            }))
            .map_err(|_| {
                io::Error::new(io::ErrorKind::BrokenPipe, "trace filesystem worker stopped")
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };
    use tokio::sync::oneshot;

    #[test]
    fn cancellation_does_not_release_a_blocked_job() {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap()
            .block_on(async {
                let worker = Worker::new().unwrap();
                let active = Arc::new(AtomicUsize::new(0));
                let (release, blocked) = mpsc::sync_channel(1);
                let (started, entered) = oneshot::channel();
                let (finished, response) = oneshot::channel::<()>();
                let running = active.clone();
                worker
                    .try_execute(move || {
                        running.fetch_add(1, Ordering::SeqCst);
                        started.send(()).unwrap();
                        blocked.recv().unwrap(); // Models a syscall that cannot be cancelled.
                        running.fetch_sub(1, Ordering::SeqCst);
                        let _ = finished.send(());
                    })
                    .unwrap();
                tokio::time::timeout(Duration::from_secs(2), entered)
                    .await
                    .unwrap()
                    .unwrap();
                // The caller times out and drops its response receiver, just like HTTP.
                assert!(tokio::time::timeout(Duration::from_millis(10), response)
                    .await
                    .is_err());
                for _ in 0..100 {
                    let (reply, disconnected) = oneshot::channel::<()>();
                    drop(disconnected);
                    assert_eq!(
                        worker
                            .try_execute(move || {
                                let _ = reply.send(());
                            })
                            .unwrap_err()
                            .kind(),
                        io::ErrorKind::WouldBlock
                    );
                    assert_eq!(active.load(Ordering::SeqCst), 1);
                }
                assert_eq!(
                    tokio::time::timeout(
                        Duration::from_secs(2),
                        tokio::task::spawn_blocking(|| 42)
                    )
                    .await
                    .unwrap()
                    .unwrap(),
                    42
                );
                release.send(()).unwrap();
                tokio::time::timeout(Duration::from_secs(2), async {
                    loop {
                        let (reply, response) = oneshot::channel();
                        if worker
                            .try_execute(move || {
                                let _ = reply.send(());
                            })
                            .is_ok()
                        {
                            response.await.unwrap();
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                assert_eq!(active.load(Ordering::SeqCst), 0);
            });
    }
}
