"""Work directories and detached worktrees for the revision comparison scripts.

`release-asm-diff.py` and `sim-schedule-diff.py` build two revisions in
detached `git worktree`s under one work directory, with cargo target
directories beside them, so neither touches the caller's tree or target
directory. `sim-schedule-diff.py` checks both out at once with `Worktrees`:

  <work>/base         <work>/head          the worktrees
  <work>/base-target  <work>/head-target   per-side target directories

`release-asm-diff.py` checks out one side at a time at one path with
`worktree`, so the two builds' paths are identical.
"""

import contextlib
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

# The exit status for a failed command, shared by both scripts.
EXIT_COMMAND_FAILED = 3

# The file that marks a work directory as this tool's, so it never deletes
# anything in a directory it didn't make.
MARKER = ".zakura-revision-diff"

# Caller settings that would change what cargo builds: compiler flags such as
# an inherited `--cfg zakura_sim`, profile overrides, a target or wrapper, and
# incremental builds.
BUILD_OVERRIDES = re.compile(
    r"^(RUSTFLAGS|CARGO_ENCODED_RUSTFLAGS|CARGO_BUILD_RUSTFLAGS|CARGO_TARGET_\w+_RUSTFLAGS"
    r"|CARGO_PROFILE_\w+|CARGO_BUILD_TARGET|CARGO_INCREMENTAL|RUSTC_WRAPPER"
    r"|RUSTC_WORKSPACE_WRAPPER|CARGO_BUILD_RUSTC_WRAPPER|CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER)$"
)


class CommandFailed(Exception):
    """A command that couldn't run or exited nonzero."""

    def __init__(self, command, detail):
        """Records the command and why it failed."""
        super().__init__(f"command failed ({detail}): {' '.join(map(str, command))}")


def run(command, cwd=None, env=None, capture=False):
    """Runs `command`, raising `CommandFailed` unless it exits 0.

    With `capture`, returns its stdout as text. Otherwise its output goes to
    this process's stdout and stderr.
    """
    try:
        result = subprocess.run(
            [str(part) for part in command],
            cwd=cwd,
            env=env,
            stdout=subprocess.PIPE if capture else None,
            text=True,
        )
    except OSError as error:
        raise CommandFailed(command, error) from error
    if result.returncode != 0:
        raise CommandFailed(command, f"exit status {result.returncode}")
    return result.stdout if capture else None


def repo_root(start):
    """The top of the git checkout that holds `start`."""
    return Path(run(["git", "-C", start, "rev-parse", "--show-toplevel"], capture=True).strip())


def resolve(repo, rev):
    """The full commit SHA that `rev` names in `repo`."""
    return run(["git", "-C", repo, "rev-parse", "--verify", f"{rev}^{{commit}}"], capture=True).strip()


def cargo_env(target_dir, extra=None):
    """This process's environment for a build, with `CARGO_TARGET_DIR` set to `target_dir`, plus `extra`.

    Drops every `BUILD_OVERRIDES` variable first, so both sides build exactly
    as the repository's own configuration says, whatever the caller's shell
    has set.
    """
    env = {name: value for name, value in os.environ.items() if not BUILD_OVERRIDES.match(name)}
    env["CARGO_TARGET_DIR"] = str(target_dir)
    env.update(extra or {})
    return env


@contextlib.contextmanager
def work_dir(path=None):
    """A context manager that yields a work directory, resolved.

    With no `path`, it makes a temporary one and deletes it on exit, build
    caches included. A given `path` is kept, so a later run with the same
    directory reuses its target directories. A given directory must be new,
    empty or marked with `MARKER` by an earlier run, so nothing else in it
    is ever deleted.
    """
    work = Path(path) if path else Path(tempfile.mkdtemp(prefix="zakura-revision-diff-"))
    work = work.resolve()
    work.mkdir(parents=True, exist_ok=True)
    if not (work / MARKER).exists():
        if any(work.iterdir()):
            raise CommandFailed(["--work-dir", work], "it isn't empty and no earlier run made it")
        (work / MARKER).write_text("made by scripts/lib/revision_worktrees.py\n")
    try:
        yield work
    finally:
        if not path:
            shutil.rmtree(work, ignore_errors=True)


@contextlib.contextmanager
def worktree(repo, rev, path):
    """A context manager that checks `rev` out at `path` as a detached worktree, and removes it on exit."""
    remove_worktree(repo, path)
    try:
        run(["git", "-C", repo, "worktree", "add", "--quiet", "--detach", "--force", path, rev])
        yield Path(path)
    finally:
        remove_worktree(repo, path)


def remove_worktree(repo, path):
    """Removes the worktree at `path`, if there is one.

    A registration left behind by a deleted directory doesn't matter, since
    `worktree add --force` reuses it.
    """
    path = Path(path)
    if path.exists():
        subprocess.run(
            ["git", "-C", str(repo), "worktree", "remove", "--force", str(path)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        shutil.rmtree(path, ignore_errors=True)


class Worktrees:
    """A context manager that checks out `base` and `head` side by side in a work directory.

    The work directory comes from `work_dir`, and both worktrees are always
    removed on exit.
    """

    def __init__(self, repo, base, head, path=None):
        """Records the revisions to check out, as SHAs, and the work directory's path."""
        self.repo = Path(repo)
        self.revs = {"base": base, "head": head}
        self._work = work_dir(path)
        self.work = None

    def __enter__(self):
        """Creates the work directory and both worktrees, and returns self."""
        self.work = self._work.__enter__()
        try:
            for side, rev in self.revs.items():
                remove_worktree(self.repo, self.tree(side))
                run(["git", "-C", self.repo, "worktree", "add", "--quiet", "--detach", "--force",
                     self.tree(side), rev])
        except BaseException:
            self.__exit__(None, None, None)
            raise
        return self

    def __exit__(self, *exc):
        """Removes both worktrees, and the work directory unless it was given."""
        for side in self.revs:
            remove_worktree(self.repo, self.tree(side))
        self._work.__exit__(None, None, None)
        return False

    def tree(self, side):
        """The worktree path for `side`, `base` or `head`."""
        return self.work / side

    def target(self, side):
        """The cargo target directory for `side`."""
        return self.work / f"{side}-target"
