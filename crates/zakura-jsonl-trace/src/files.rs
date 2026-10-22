//! Bounded writer lock for trace directories.

use std::{
    fs::{File, OpenOptions},
    io,
    path::Path,
    time::{Duration, Instant},
};

pub(crate) struct TraceLock {
    _file: File,
}

impl TraceLock {
    pub(crate) fn acquire(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match file.try_lock() {
                Ok(()) => return Ok(Self { _file: file }),
                Err(std::fs::TryLockError::WouldBlock) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(std::fs::TryLockError::WouldBlock) => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "trace lock timed out",
                    ))
                }
                Err(std::fs::TryLockError::Error(error)) => return Err(error),
            }
        }
    }
}
