//! Offline generator for the reviewed Mainnet VCT Sprout-history repair artifact.

#![allow(clippy::print_stderr, clippy::print_stdout)]

use std::{
    env, fs,
    io::{self, Write},
    path::{Path, PathBuf},
    process::ExitCode,
    sync::Arc,
};

use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;
use thiserror::Error;
use zakura_state::{
    generate_mainnet_from_archive_with_options, Config, GeneratorOptions, GeneratorProgress,
};

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
    let (cache_dir, output_file, mut options) = match parse_args(env::args_os().skip(1)) {
        Ok(arguments) => arguments,
        Err(error) => {
            eprintln!("{error}");
            print_usage();
            return ExitCode::FAILURE;
        }
    };

    let output_file = match validated_output_path(&cache_dir, &output_file) {
        Ok(output_file) => output_file,
        Err(error) => {
            eprintln!("invalid artifact output: {error}");
            return ExitCode::FAILURE;
        }
    };
    if let Some(checkpoint_dir) = options.checkpoint_dir.as_mut() {
        match validated_checkpoint_path(&cache_dir, checkpoint_dir) {
            Ok(path) => *checkpoint_dir = path,
            Err(error) => {
                eprintln!("invalid checkpoint directory: {error}");
                return ExitCode::FAILURE;
            }
        }
    }
    let config = Config {
        cache_dir,
        ..Config::default()
    };
    options.progress = Some(Arc::new(print_progress));
    eprintln!(
        "scanning Mainnet archive with {} shards on {} workers and {} MiB readahead per scan",
        options.shards,
        options.workers,
        options.readahead_size / (1024 * 1024),
    );
    let bytes = match generate_mainnet_from_archive_with_options(&config, &options) {
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
    if let Some(checkpoint_dir) = &options.checkpoint_dir {
        if let Err(error) = cleanup_checkpoint_dir(checkpoint_dir) {
            eprintln!(
                "warning: artifact is complete, but checkpoint cleanup failed for {:?}: {error}",
                checkpoint_dir
            );
        }
    }

    println!(
        "wrote {} bytes to {:?}; sha256={:x}",
        bytes.len(),
        output_file,
        Sha256::digest(&bytes)
    );
    ExitCode::SUCCESS
}

fn print_usage() {
    eprintln!(
        "usage: generate-vct-sprout-artifact <mainnet-cache-dir> <output-file> \
         [--shards N] [--workers N] [--readahead-mib N] \
         [--checkpoint-dir PATH] [--resume]"
    );
}

fn parse_args(
    args: impl IntoIterator<Item = std::ffi::OsString>,
) -> Result<(PathBuf, PathBuf, GeneratorOptions), String> {
    let mut args = args.into_iter();
    let cache_dir = args
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| "missing Mainnet cache directory".to_string())?;
    let output_file = args
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| "missing output file".to_string())?;
    let mut options = GeneratorOptions::default();

    while let Some(argument) = args.next() {
        match argument.to_str() {
            Some("--shards") => {
                options.shards = parse_count(args.next(), "--shards")?;
            }
            Some("--workers") => {
                options.workers = parse_count(args.next(), "--workers")?;
            }
            Some("--readahead-mib") => {
                options.readahead_size = parse_count(args.next(), "--readahead-mib")?
                    .checked_mul(1024 * 1024)
                    .ok_or_else(|| "--readahead-mib is too large".to_string())?;
            }
            Some("--checkpoint-dir") => {
                options.checkpoint_dir =
                    Some(PathBuf::from(args.next().ok_or_else(|| {
                        "--checkpoint-dir requires a path".to_string()
                    })?));
            }
            Some("--resume") => options.resume = true,
            _ => return Err(format!("unknown generator argument {argument:?}")),
        }
    }

    Ok((cache_dir, output_file, options))
}

fn parse_count(value: Option<std::ffi::OsString>, option: &str) -> Result<usize, String> {
    let value = value.ok_or_else(|| format!("{option} requires a positive integer"))?;
    let count = value
        .to_str()
        .ok_or_else(|| format!("{option} is not valid UTF-8"))?
        .parse::<usize>()
        .map_err(|_| format!("{option} requires a positive integer"))?;
    if count == 0 {
        return Err(format!("{option} requires a positive integer"));
    }
    Ok(count)
}

fn print_progress(progress: GeneratorProgress) {
    let percent = progress.completed_heights as f64 * 100.0 / progress.total_heights as f64;
    let heights_per_second =
        progress.completed_heights as f64 / progress.elapsed.as_secs_f64().max(f64::EPSILON);
    let remaining = progress
        .total_heights
        .saturating_sub(progress.completed_heights);
    let eta_seconds = remaining as f64 / heights_per_second.max(f64::EPSILON);
    eprintln!(
        "scan {:>5.1}% ({}/{}) {:.0} heights/s elapsed {:.0}s ETA {:.0}s",
        percent,
        progress.completed_heights,
        progress.total_heights,
        heights_per_second,
        progress.elapsed.as_secs_f64(),
        eta_seconds,
    );
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

fn validated_checkpoint_path(
    cache_dir: &Path,
    checkpoint_dir: &Path,
) -> Result<PathBuf, OutputError> {
    let source = fs::canonicalize(cache_dir)?;
    let checkpoint = match fs::canonicalize(checkpoint_dir) {
        Ok(path) => path,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let name = checkpoint_dir
                .file_name()
                .ok_or(OutputError::MissingFileName)?;
            let parent = checkpoint_dir
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            fs::canonicalize(parent)?.join(name)
        }
        Err(error) => return Err(OutputError::Io(error)),
    };
    if checkpoint.starts_with(&source) {
        return Err(OutputError::InsideSourceCache {
            output: checkpoint,
            source_cache: source,
        });
    }
    Ok(checkpoint)
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

fn cleanup_checkpoint_dir(checkpoint_dir: &Path) -> io::Result<()> {
    for entry in fs::read_dir(checkpoint_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name == "manifest"
            || (name.starts_with("shard-") && name.ends_with(".bin"))
            || name.starts_with(".tmp")
        {
            fs::remove_file(entry.path())?;
        }
    }
    match fs::remove_dir(checkpoint_dir) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::DirectoryNotEmpty => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;

    #[test]
    fn parses_parallel_and_resume_options() {
        let (_, _, options) = parse_args(
            [
                "cache",
                "artifact.bin",
                "--shards",
                "12",
                "--workers",
                "24",
                "--readahead-mib",
                "32",
                "--checkpoint-dir",
                "progress",
                "--resume",
            ]
            .map(OsString::from),
        )
        .expect("valid generator options parse");

        assert_eq!(options.shards, 12);
        assert_eq!(options.workers, 24);
        assert_eq!(options.readahead_size, 32 * 1024 * 1024);
        assert_eq!(options.checkpoint_dir, Some(PathBuf::from("progress")));
        assert!(options.resume);
    }

    #[test]
    fn rejects_zero_parallelism() {
        assert!(
            parse_args(["cache", "artifact.bin", "--shards", "0"].map(OsString::from)).is_err()
        );
    }

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
    fn rejects_checkpoint_inside_source_cache() {
        let source = tempfile::tempdir().expect("source tempdir is created");
        let checkpoint = source.path().join("progress");

        assert!(matches!(
            validated_checkpoint_path(source.path(), &checkpoint),
            Err(OutputError::InsideSourceCache { .. })
        ));
        assert!(!checkpoint.exists());
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

    #[test]
    fn checkpoint_cleanup_preserves_unrelated_files() {
        let directory = tempfile::tempdir().expect("checkpoint parent is created");
        let checkpoint = directory.path().join("progress");
        fs::create_dir(&checkpoint).expect("checkpoint directory is created");
        fs::write(checkpoint.join("manifest"), b"manifest").expect("manifest is written");
        fs::write(checkpoint.join("shard-0000.bin"), b"shard").expect("shard is written");
        fs::write(checkpoint.join("keep.txt"), b"keep").expect("unrelated file is written");

        cleanup_checkpoint_dir(&checkpoint).expect("known checkpoint files are removed");

        assert!(checkpoint.join("keep.txt").exists());
        assert!(!checkpoint.join("manifest").exists());
        assert!(!checkpoint.join("shard-0000.bin").exists());
    }
}
