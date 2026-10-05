"""Bounded local and SSH command execution.

Every command runs in its own process group with stdin closed and stderr
discarded, so remote diagnostics (which may quote paths, logs or credentials)
never reach the watchdog's journal. Output is capped, and the whole group is
killed when the hard timeout expires.
"""

from __future__ import annotations

import os
import selectors
import shlex
import signal
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence


READ_CHUNK = 4096
# Reserve a small part of the overall deadline for killing and reaping.
MAX_REAP_SECONDS = 0.1


@dataclass(frozen=True)
class BoundedResult:
    """Command output; returncode is None if startup or bounded reaping failed."""

    returncode: int | None
    stdout: bytes
    timed_out: bool
    oversized: bool
    elapsed: float


def run_bounded(
    command: Sequence[str],
    timeout: float,
    max_output: int,
    env: dict[str, str] | None = None,
) -> BoundedResult:
    """Bound execution, output collection and cleanup by one overall deadline.

    Non-blocking pipe reads never wait for a detached descendant to close
    stdout. Timeout includes waiting for EOF, even if the direct child exited.
    Keep draining past the output cap so a chatty child cannot fill the pipe.
    """
    started = time.monotonic()
    budget = max(0.0, timeout)
    deadline = started + budget
    work_deadline = deadline - min(MAX_REAP_SECONDS, budget / 2)
    if budget == 0:
        return BoundedResult(None, b"", True, False, time.monotonic() - started)
    try:
        process = subprocess.Popen(
            list(command),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            env=env,
        )
    except OSError:
        return BoundedResult(None, b"", False, False, time.monotonic() - started)

    captured = bytearray()
    oversized = False
    timed_out = False
    returncode = None
    assert process.stdout is not None
    try:
        descriptor = process.stdout.fileno()
        os.set_blocking(descriptor, False)
        with selectors.DefaultSelector() as selector:
            selector.register(descriptor, selectors.EVENT_READ)
            while True:
                remaining = work_deadline - time.monotonic()
                if remaining <= 0:
                    raise subprocess.TimeoutExpired(command, timeout)
                if not selector.select(remaining):
                    continue
                try:
                    chunk = os.read(descriptor, READ_CHUNK)
                except BlockingIOError:
                    continue
                if not chunk:
                    break
                room = max_output - len(captured)
                oversized = oversized or len(chunk) > room
                if room > 0:
                    captured.extend(chunk[:room])
        # EOF alone does not mean the process finished.
        returncode = process.wait(timeout=max(0.0, work_deadline - time.monotonic()))
    except subprocess.TimeoutExpired:
        timed_out = True
    finally:
        if timed_out or process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                # The exited parent's group may already be gone or reused.
                pass
        try:
            returncode = process.wait(timeout=max(0.0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            # A process stuck in the kernel must not strand the probe worker.
            timed_out = True
        process.stdout.close()
    return BoundedResult(
        returncode, bytes(captured), timed_out, oversized,
        time.monotonic() - started,
    )


def ssh_command(
    target: str,
    remote_argv: Sequence[str],
    connect_timeout: int = 15,
    known_hosts: Path | None = None,
    identity_file: Path | None = None,
) -> list[str]:
    """Build an authenticated, non-interactive SSH command with pinned host keys."""
    command = [
        "ssh",
        "-o", "BatchMode=yes",
        "-o", f"ConnectTimeout={int(connect_timeout)}",
        "-o", "StrictHostKeyChecking=yes",
        "-o", "ServerAliveInterval=10",
        "-o", "ServerAliveCountMax=3",
        "-o", "LogLevel=ERROR",
    ]
    if known_hosts is not None:
        command += ["-o", f"UserKnownHostsFile={known_hosts}"]
    if identity_file is not None:
        command += ["-o", "IdentitiesOnly=yes", "-i", str(identity_file)]
    return [*command, "--", target, shlex.join(remote_argv)]
