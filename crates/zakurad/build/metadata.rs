//! Emits Cargo, Rust compiler, and Git metadata for `zakurad`.

use std::{
    env,
    path::{Path, PathBuf},
    process::Command,
};

/// Emits all metadata consumed by `zakurad` diagnostics.
#[allow(clippy::print_stderr)]
pub fn emit() {
    println!("cargo:rerun-if-env-changed=SOURCE_DATE_EPOCH");

    emit_cargo_metadata();
    emit_rustc_metadata();

    if let Err(error) = emit_git_metadata() {
        // Source archives and `cargo install` builds do not have a `.git`
        // directory, so Git metadata is intentionally optional.
        eprintln!("git metadata unavailable: skipping git env vars: {error}");
    }
}

fn emit_cargo_metadata() {
    let mut features: Vec<_> = env::vars()
        .filter_map(|(name, _)| {
            name.strip_prefix("CARGO_FEATURE_")
                .map(|feature| feature.to_lowercase())
        })
        .collect();
    features.sort_unstable();

    emit_env("VERGEN_CARGO_FEATURES", features.join(","));
    emit_env("VERGEN_CARGO_TARGET_TRIPLE", required_env("TARGET"));
    emit_env("VERGEN_CARGO_OPT_LEVEL", required_env("OPT_LEVEL"));
    emit_env("VERGEN_CARGO_DEBUG", required_env("DEBUG"));
}

fn emit_rustc_metadata() {
    let rustc = env::var_os("RUSTC").expect("Cargo always sets RUSTC for build scripts");
    let output = Command::new(rustc)
        .arg("-vV")
        .output()
        .expect("rustc must be executable to compile zakurad");
    assert!(output.status.success(), "rustc -vV must succeed");

    let version = String::from_utf8(output.stdout).expect("rustc -vV output must be UTF-8");
    emit_env("VERGEN_RUSTC_SEMVER", rustc_field(&version, "release"));
    emit_env(
        "VERGEN_RUSTC_COMMIT_DATE",
        rustc_field(&version, "commit-date"),
    );
}

fn emit_git_metadata() -> Result<(), String> {
    if git(&["rev-parse", "--is-inside-work-tree"])? != "true" {
        return Err("not inside a Git worktree".to_string());
    }

    emit_git_rerun_triggers()?;

    let branch = git(&["rev-parse", "--abbrev-ref", "--symbolic-full-name", "HEAD"])?;
    let sha = git(&["rev-parse", "HEAD"])?;
    let timestamp = match env::var("SOURCE_DATE_EPOCH") {
        Ok(epoch) => format_source_date_epoch(&epoch)?,
        Err(env::VarError::NotPresent) => git(&["log", "-1", "--pretty=format:%cI"])?,
        Err(error) => return Err(format!("invalid SOURCE_DATE_EPOCH: {error}")),
    };

    let mut describe = git(&["describe", "--always", "--tags", "--match", "v*.*.*"])?;
    if !git(&["status", "--porcelain", "--untracked-files=no"])?.is_empty() {
        describe.push_str("-dirty");
    }

    emit_env("VERGEN_GIT_BRANCH", branch);
    emit_env("VERGEN_GIT_COMMIT_TIMESTAMP", timestamp);
    emit_env("VERGEN_GIT_DESCRIBE", describe);
    emit_env("VERGEN_GIT_SHA", sha);

    Ok(())
}

fn emit_git_rerun_triggers() -> Result<(), String> {
    let git_dir = PathBuf::from(git(&["rev-parse", "--git-dir"])?);
    emit_rerun_if_exists(&git_dir.join("HEAD"));

    if let Ok(reference) = git(&["symbolic-ref", "HEAD"]) {
        emit_rerun_if_exists(&git_dir.join(reference));
    }

    Ok(())
}

fn emit_rerun_if_exists(path: &Path) {
    if path.exists() {
        println!("cargo:rerun-if-changed={}", path.display());
    }
}

fn git(args: &[&str]) -> Result<String, String> {
    let output = Command::new("git")
        .args(args)
        .env("GIT_OPTIONAL_LOCKS", "0")
        .output()
        .map_err(|error| format!("could not run git: {error}"))?;

    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).trim().to_string());
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn format_source_date_epoch(epoch: &str) -> Result<String, String> {
    let seconds: i64 = epoch
        .parse()
        .map_err(|error| format!("SOURCE_DATE_EPOCH must be an integer: {error}"))?;
    let days = seconds.div_euclid(86_400);
    let seconds_of_day = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_date(days);
    let hour = seconds_of_day / 3_600;
    let minute = seconds_of_day % 3_600 / 60;
    let second = seconds_of_day % 60;

    Ok(format!(
        "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.000000000Z"
    ))
}

fn civil_date(days_since_unix_epoch: i64) -> (i64, i64, i64) {
    let days = days_since_unix_epoch + 719_468;
    let era = days.div_euclid(146_097);
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    (year, month, day)
}

fn rustc_field<'a>(version: &'a str, field: &str) -> &'a str {
    version
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{field}: ")))
        .unwrap_or_else(|| panic!("rustc -vV output must contain {field}"))
}

fn required_env(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("Cargo always sets {name} for build scripts"))
}

fn emit_env(name: &str, value: impl AsRef<str>) {
    println!("cargo:rustc-env={name}={}", value.as_ref());
}
