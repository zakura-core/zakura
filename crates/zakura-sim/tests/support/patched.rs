//! A planted case's patched simulator, built the way `cargo xtask planted`
//! builds one, for the tests that read a patched run's own recording.

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

/// Runs `command`, and panics with its output unless it exits 0.
pub fn run_ok(command: &mut Command) {
    let output = command.output().expect("the command runs");
    assert!(
        output.status.success(),
        "{command:?} exited with {}:\n{}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

/// `git` in `dir`.
pub fn git(dir: &Path) -> Command {
    let mut git = Command::new("git");
    git.arg("-C").arg(dir);
    git
}

/// Removes the scratch worktree at `worktree` from `repo`, if there is one.
fn remove_worktree(repo: &Path, worktree: &Path) {
    if worktree.exists() {
        run_ok(
            git(repo)
                .args(["worktree", "remove", "--force"])
                .arg(worktree),
        );
    }
}

/// A scratch worktree of the repository's `HEAD`, beside the planted
/// runner's, removed when dropped, so a failed assertion leaves none behind.
pub struct Worktree {
    repo: PathBuf,
    /// The worktree's root.
    pub path: PathBuf,
    /// The target directory its patched builds share.
    target: PathBuf,
}

impl Worktree {
    /// A fresh detached worktree of `HEAD` at `<work>/worktree`, replacing
    /// any a failed run left there, whose builds go to `<work>/target`.
    pub fn of_head(work: &Path) -> Self {
        let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .expect("the repository root resolves");
        let path = work.join("worktree");
        remove_worktree(&repo, &path);
        fs::create_dir_all(work).expect("the scratch directory is writable");
        run_ok(
            git(&repo)
                .args(["worktree", "add", "--detach"])
                .arg(&path)
                .arg("HEAD"),
        );
        Worktree {
            repo,
            path,
            target: work.join("target"),
        }
    }

    /// Resets the worktree to `HEAD`, applies `patch`, a path from the
    /// repository root, and builds `zakura-sim` with the simulator cfg and
    /// the `planted` profile, as the planted runner builds a patched
    /// simulator. Returns the built binary.
    pub fn build_patched(&self, patch: &str) -> PathBuf {
        run_ok(git(&self.path).args(["reset", "--hard", "--quiet", "HEAD"]));
        run_ok(git(&self.path).arg("apply").arg(patch));
        run_ok(
            Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
                .current_dir(&self.path)
                .env("RUSTFLAGS", "--cfg zakura_sim")
                .env("CARGO_TARGET_DIR", &self.target)
                // The patch can leave code unused, as the planted runner
                // allows.
                .env("CARGO_BUILD_WARNINGS", "warn")
                .args([
                    "build",
                    "--locked",
                    "--profile",
                    "planted",
                    "-p",
                    "zakura-sim",
                    "--bin",
                    "zakura-sim",
                ]),
        );
        self.target.join("planted").join("zakura-sim")
    }
}

impl Drop for Worktree {
    /// Removes the worktree.
    fn drop(&mut self) {
        remove_worktree(&self.repo, &self.path);
    }
}
