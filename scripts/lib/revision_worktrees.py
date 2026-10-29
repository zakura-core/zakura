"""Detached base and head worktrees for the revision comparison scripts.

`release-asm-diff.py` and `sim-schedule-diff.py` build two revisions side by
side. Each revision gets its own detached `git worktree` under one work
directory, with cargo target directories beside them, so neither touches the
caller's tree or target directory:

  <work>/base         <work>/head          the worktrees
  <work>/base-target  <work>/head-target   per-side target directories

The two worktree paths have the same length, so a path a build embeds, such
as `CARGO_MANIFEST_DIR`, can't shift the layout of one side's binary.
"""

import os
import shutil
import subprocess
import tempfile
from pathlib import Path

# The exit status for a failed command, shared by both scripts.
EXIT_COMMAND_FAILED = 3


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
    """This process's environment with `CARGO_TARGET_DIR` set to `target_dir`, plus `extra`."""
    env = dict(os.environ)
    env["CARGO_TARGET_DIR"] = str(target_dir)
    env.update(extra or {})
    return env


class Worktrees:
    """A context manager that checks out `base` and `head` under a work directory.

    With no `work_dir`, it makes a temporary one and deletes it on exit, build
    caches included. A given `work_dir` is kept, so a later run with the same
    directory reuses its target directories. The worktrees themselves are
    always removed on exit.
    """

    def __init__(self, repo, base, head, work_dir=None):
        """Records the revisions to check out, as SHAs, and where."""
        self.repo = Path(repo)
        self.revs = {"base": base, "head": head}
        self.keep = work_dir is not None
        self.work = Path(work_dir) if work_dir else None

    def __enter__(self):
        """Creates the work directory and both worktrees, and returns self."""
        if self.work is None:
            self.work = Path(tempfile.mkdtemp(prefix="zakura-revision-diff-"))
        self.work = self.work.resolve()
        self.work.mkdir(parents=True, exist_ok=True)
        try:
            for side, rev in self.revs.items():
                tree = self.tree(side)
                self._remove(tree)
                run(["git", "-C", self.repo, "worktree", "add", "--quiet", "--detach", "--force", tree, rev])
        except BaseException:
            self.__exit__(None, None, None)
            raise
        return self

    def __exit__(self, *exc):
        """Removes both worktrees, and the work directory unless it was given."""
        for side in self.revs:
            self._remove(self.tree(side))
        if not self.keep:
            shutil.rmtree(self.work, ignore_errors=True)
        return False

    def tree(self, side):
        """The worktree path for `side`, `base` or `head`."""
        return self.work / side

    def target(self, side):
        """The cargo target directory for `side`."""
        return self.work / f"{side}-target"

    def _remove(self, tree):
        """Removes the worktree at `tree`, if there is one.

        A registration left behind by a deleted directory doesn't matter,
        since `worktree add --force` reuses it.
        """
        if tree.exists():
            subprocess.run(
                ["git", "-C", str(self.repo), "worktree", "remove", "--force", str(tree)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            shutil.rmtree(tree, ignore_errors=True)
