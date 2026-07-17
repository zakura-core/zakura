//! Offline generator for the reviewed Mainnet VCT Sprout-history repair artifact.

#![allow(clippy::print_stderr, clippy::print_stdout)]

use std::{
    env, fs,
    io::{self, Write},
    path::{Path, PathBuf},
    process::ExitCode,
};

use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;
use thiserror::Error;
use zakura_state::{generate_mainnet_from_archive, Config};

#[derive(Debug, Error)]
enum OutputError {
    #[error("output path must name a file")]
    MissingFileName,
    #[error("could not resolve artifact path: {0}")]
    Io(#[from] io::Error),
    #[error("output path {output:?} is inside the source cache/database {source_cache:?}")]
    InsideSourceCache {
        output: PathBuf,
        source_cache: PathBuf,
    },
    #[error("output path already exists: {0:?}")]
    AlreadyExists(PathBuf),
}

fn main() -> ExitCode {
    let mut args = env::args_os().skip(1);
    let Some(cache_dir) = args.next() else {
        eprintln!("usage: generate-vct-sprout-artifact <mainnet-cache-dir> <output-file>");
        return ExitCode::FAILURE;
    };
    let Some(output_file) = args.next() else {
        eprintln!("usage: generate-vct-sprout-artifact <mainnet-cache-dir> <output-file>");
        return ExitCode::FAILURE;
    };
    if args.next().is_some() {
        eprintln!("usage: generate-vct-sprout-artifact <mainnet-cache-dir> <output-file>");
        return ExitCode::FAILURE;
    }

    let cache_dir = PathBuf::from(cache_dir);
    let output_file = PathBuf::from(output_file);
    let output_file = match validated_output_path(&cache_dir, &output_file) {
        Ok(output_file) => output_file,
        Err(error) => {
            eprintln!("invalid artifact output: {error}");
            return ExitCode::FAILURE;
        }
    };
    let config = Config {
        cache_dir,
        ..Config::default()
    };
    let bytes = match generate_mainnet_from_archive(&config) {
        Ok(bytes) => bytes,
        Err(error) => {
            eprintln!("artifact generation failed: {error}");
            return ExitCode::FAILURE;
        }
    };
    if let Err(error) = write_atomic_noclobber(&output_file, &bytes) {
        eprintln!("could not write artifact to {:?}: {error}", output_file);
        return ExitCode::FAILURE;
    }

    println!(
        "wrote {} bytes to {:?}; sha256={:x}",
        bytes.len(),
        output_file,
        Sha256::digest(&bytes)
    );
    ExitCode::SUCCESS
}

fn validated_output_path(cache_dir: &Path, output_file: &Path) -> Result<PathBuf, OutputError> {
    let source = fs::canonicalize(cache_dir)?;
    let file_name = output_file
        .file_name()
        .ok_or(OutputError::MissingFileName)?;
    let output_parent = output_file
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let output = fs::canonicalize(output_parent)?.join(file_name);

    if output.starts_with(&source) {
        return Err(OutputError::InsideSourceCache {
            output,
            source_cache: source,
        });
    }

    match fs::symlink_metadata(&output) {
        Ok(_) => return Err(OutputError::AlreadyExists(output)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(OutputError::Io(error)),
    }

    Ok(output)
}

fn write_atomic_noclobber(output_file: &Path, bytes: &[u8]) -> Result<(), OutputError> {
    let output_parent = output_file.parent().ok_or(OutputError::MissingFileName)?;
    let mut temporary = NamedTempFile::new_in(output_parent)?;
    temporary.write_all(bytes)?;
    temporary.as_file_mut().sync_all()?;
    temporary
        .persist_noclobber(output_file)
        .map_err(|error| OutputError::Io(error.error))?;
    #[cfg(unix)]
    fs::File::open(output_parent)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_output_inside_source_cache() {
        let source = tempfile::tempdir().expect("source tempdir is created");
        let output = source.path().join("artifact.bin");

        assert!(matches!(
            validated_output_path(source.path(), &output),
            Err(OutputError::InsideSourceCache { .. })
        ));
        assert!(!output.exists());
    }

    #[test]
    fn rejects_existing_output_without_modifying_it() {
        let source = tempfile::tempdir().expect("source tempdir is created");
        let destination = tempfile::tempdir().expect("destination tempdir is created");
        let output = destination.path().join("artifact.bin");
        fs::write(&output, b"existing").expect("fixture output is written");

        assert!(matches!(
            validated_output_path(source.path(), &output),
            Err(OutputError::AlreadyExists(_))
        ));
        assert_eq!(
            fs::read(output).expect("fixture output remains readable"),
            b"existing"
        );
    }

    #[test]
    fn atomically_creates_new_output_without_clobbering() {
        let source = tempfile::tempdir().expect("source tempdir is created");
        let destination = tempfile::tempdir().expect("destination tempdir is created");
        let output = validated_output_path(source.path(), &destination.path().join("artifact.bin"))
            .expect("safe output validates");

        write_atomic_noclobber(&output, b"artifact").expect("artifact is persisted");

        assert_eq!(
            fs::read(output).expect("artifact remains readable"),
            b"artifact"
        );
    }

    #[test]
    fn failed_noclobber_cleans_temporary_file() {
        let destination = tempfile::tempdir().expect("destination tempdir is created");
        let output = destination.path().join("artifact.bin");
        fs::write(&output, b"existing").expect("fixture output is written");

        assert!(matches!(
            write_atomic_noclobber(&output, b"replacement"),
            Err(OutputError::Io(_))
        ));
        let entries: Vec<_> = fs::read_dir(destination.path())
            .expect("destination directory remains readable")
            .map(|entry| entry.expect("directory entry is readable").file_name())
            .collect();
        assert_eq!(
            entries,
            [output.file_name().expect("output has a file name")]
        );
        assert_eq!(
            fs::read(output).expect("existing output remains readable"),
            b"existing"
        );
    }
}
