//! Temporary files and directories that are removed when dropped.
//!
//! Names are unique because each one is created exclusively: an existing path is
//! skipped, never opened or replaced.

use std::{
    ffi::{OsStr, OsString},
    fs, io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

/// How many names to try before giving up on creating a temporary path.
const MAX_ATTEMPTS: u32 = 64;

/// Distinguishes temporary paths created by this process.
static NEXT_SUFFIX: AtomicU64 = AtomicU64::new(0);

/// Creates a new path in `dir` starting with `prefix`, using `create`, which must fail
/// with [`io::ErrorKind::AlreadyExists`] if the path exists.
fn create_unique<T>(
    dir: &Path,
    prefix: &OsStr,
    mut create: impl FnMut(&Path) -> io::Result<T>,
) -> io::Result<(PathBuf, T)> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.subsec_nanos());

    for _ in 0..MAX_ATTEMPTS {
        let suffix = NEXT_SUFFIX.fetch_add(1, Ordering::Relaxed);

        let mut name = OsString::from(prefix);
        name.push(format!("{}.{nanos}.{suffix}", std::process::id()));
        let path = dir.join(name);

        match create(&path) {
            Ok(created) => return Ok((path, created)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }

    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        format!(
            "could not find an unused temporary name in {}",
            dir.display()
        ),
    ))
}

/// A temporary file, readable and writable only by its owner on Unix.
///
/// It is removed when dropped, unless it has been [persisted](Self::persist).
#[derive(Debug)]
pub struct TempFile {
    path: PathBuf,
    file: fs::File,
    persisted: bool,
}

impl TempFile {
    /// Creates a new temporary file in `dir`, with a name starting with `prefix`.
    ///
    /// Create it in the same directory as its final path, so that
    /// [`persist`](Self::persist) is an atomic rename.
    pub fn new_in(dir: impl AsRef<Path>, prefix: impl AsRef<OsStr>) -> io::Result<Self> {
        let (path, file) = create_unique(dir.as_ref(), prefix.as_ref(), |path| {
            let mut options = fs::OpenOptions::new();
            options.read(true).write(true).create_new(true);

            #[cfg(unix)]
            std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);

            options.open(path)
        })?;

        Ok(Self {
            path,
            file,
            persisted: false,
        })
    }

    /// Returns the temporary file's current path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the open file.
    pub fn as_file(&self) -> &fs::File {
        &self.file
    }

    /// Returns the open file, for writing.
    pub fn as_file_mut(&mut self) -> &mut fs::File {
        &mut self.file
    }

    /// Atomically renames the file to `new_path`, replacing any file there.
    ///
    /// If the rename fails, the temporary file is removed.
    pub fn persist(mut self, new_path: impl AsRef<Path>) -> io::Result<()> {
        fs::rename(&self.path, new_path.as_ref())?;
        self.persisted = true;

        Ok(())
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        if !self.persisted {
            let _ = fs::remove_file(&self.path);
        }
    }
}

/// A temporary directory, accessible only by its owner on Unix.
///
/// It is removed, with its contents, when dropped.
#[derive(Debug)]
pub struct TempDir {
    path: PathBuf,
    kept: bool,
}

impl TempDir {
    /// Creates a new temporary directory in `dir`, with a name starting with `prefix`.
    pub fn new_in(dir: impl AsRef<Path>, prefix: impl AsRef<OsStr>) -> io::Result<Self> {
        let (path, ()) = create_unique(dir.as_ref(), prefix.as_ref(), |path| {
            let builder = fs::DirBuilder::new();

            #[cfg(unix)]
            let mut builder = builder;

            #[cfg(unix)]
            std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);

            builder.create(path)
        })?;

        Ok(Self { path, kept: false })
    }

    /// Returns the temporary directory's path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Keeps the directory, rather than removing it on drop, and returns its path.
    pub fn keep(mut self) -> PathBuf {
        self.kept = true;
        self.path.clone()
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        if !self.kept {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Seek, Write};

    use super::*;

    fn test_dir() -> TempDir {
        TempDir::new_in(std::env::temp_dir(), "zakura-temp-test-")
            .expect("a directory can be created in the system temporary directory")
    }

    #[test]
    fn unpersisted_file_is_removed_on_drop() {
        let dir = test_dir();
        let file = TempFile::new_in(dir.path(), ".tmp.").expect("temporary file is created");
        let path = file.path().to_path_buf();

        assert!(path.exists());
        drop(file);
        assert!(!path.exists());
    }

    #[test]
    fn persisted_file_replaces_the_destination() {
        let dir = test_dir();
        let destination = dir.path().join("data");
        fs::write(&destination, b"old").expect("destination is written");

        let mut file = TempFile::new_in(dir.path(), ".tmp.").expect("temporary file is created");
        file.as_file_mut()
            .write_all(b"new")
            .expect("temporary file is written");
        let temp_path = file.path().to_path_buf();
        file.persist(&destination)
            .expect("temporary file is persisted");

        assert_eq!(fs::read(&destination).expect("destination is read"), b"new");
        assert!(!temp_path.exists());
    }

    #[test]
    fn names_are_unique() {
        let dir = test_dir();
        let first = TempFile::new_in(dir.path(), ".tmp.").expect("first file is created");
        let second = TempFile::new_in(dir.path(), ".tmp.").expect("second file is created");

        assert_ne!(first.path(), second.path());
        assert!(first
            .path()
            .file_name()
            .expect("temporary files have names")
            .to_string_lossy()
            .starts_with(".tmp."));
    }

    #[test]
    fn file_is_readable_and_writable() {
        let dir = test_dir();
        let mut file = TempFile::new_in(dir.path(), ".tmp.").expect("temporary file is created");
        file.as_file_mut()
            .write_all(b"data")
            .expect("temporary file is written");
        file.as_file_mut()
            .rewind()
            .expect("temporary file is rewound");

        let mut contents = String::new();
        file.as_file_mut()
            .read_to_string(&mut contents)
            .expect("temporary file is read");
        assert_eq!(contents, "data");
    }

    #[cfg(unix)]
    #[test]
    fn only_the_owner_can_access_temporary_paths() {
        use std::os::unix::fs::PermissionsExt;

        let dir = test_dir();
        let file = TempFile::new_in(dir.path(), ".tmp.").expect("temporary file is created");

        let mode = |path: &Path| {
            fs::metadata(path)
                .expect("metadata is readable")
                .permissions()
                .mode()
                & 0o777
        };
        assert_eq!(mode(file.path()), 0o600);
        assert_eq!(mode(dir.path()), 0o700);
    }

    #[test]
    fn kept_directory_is_not_removed() {
        let parent = test_dir();
        let path = TempDir::new_in(parent.path(), "kept-")
            .expect("temporary directory is created")
            .keep();

        assert!(path.is_dir());
    }

    #[test]
    fn directory_is_removed_with_its_contents_on_drop() {
        let dir = test_dir();
        let path = dir.path().to_path_buf();
        fs::write(path.join("file"), b"data").expect("file is written");

        drop(dir);
        assert!(!path.exists());
    }
}
