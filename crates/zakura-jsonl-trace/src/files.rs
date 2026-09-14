//! Regular-file access and bounded locks for trace storage.

use std::{
    fs::{self, File, OpenOptions},
    io,
    path::Path,
    time::{Duration, Instant},
};

/// Open a trace entry without following a symlink or blocking on a special file.
pub fn open_regular(path: &Path, options: &mut OpenOptions) -> io::Result<File> {
    for ancestor in path.ancestors().skip(1) {
        if !ancestor.as_os_str().is_empty()
            && fs::symlink_metadata(ancestor)?.file_type().is_symlink()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "trace directory must not be a symlink",
            ));
        }
    }
    if let Ok(metadata) = fs::symlink_metadata(path) {
        if !metadata.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "trace entry must be a regular file",
            ));
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        // FILE_FLAG_OPEN_REPARSE_POINT opens the link itself for validation.
        options.custom_flags(0x0020_0000);
    }
    let file = options.open(path)?;
    if !file.metadata()?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "trace entry must be a regular file",
        ));
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        // FILE_ATTRIBUTE_REPARSE_POINT includes symlinks and junctions.
        if file.metadata()?.file_attributes() & 0x400 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "trace entry must not be a reparse point",
            ));
        }
    }
    Ok(file)
}

pub(crate) struct TraceLock {
    _file: File,
}

impl TraceLock {
    pub(crate) fn acquire(path: &Path) -> io::Result<Self> {
        let file = open_regular(
            path,
            OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true),
        )?;
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
