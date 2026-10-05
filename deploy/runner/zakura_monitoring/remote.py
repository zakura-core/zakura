"""Bounded local and SSH command execution.

Every command runs in its own process group with stdin closed and stderr
discarded, so remote diagnostics (which may quote paths, logs or credentials)
never reach the watchdog's journal. Output is capped, and the whole group is
killed when the hard timeout expires.
"""

from __future__ import annotations

import os
import shlex
import signal
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence


READ_CHUNK = 4096
READER_JOIN_SECONDS = 5.0


@dataclass(frozen=True)
class BoundedResult:
    """What a bounded command produced. ``returncode`` is None if it never started."""

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
    """Run ``command`` for at most ``timeout`` seconds, keeping ``max_output`` bytes."""
    started = time.monotonic()
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
    oversized = threading.Event()

    def drain() -> None:
        # Keep draining past the cap so a chatty child cannot block on a full pipe.
        assert process.stdout is not None
        while True:
            chunk = process.stdout.read1(READ_CHUNK)
            if not chunk:
                return
            room = max_output - len(captured)
            if len(chunk) > room:
                oversized.set()
            if room > 0:
                captured.extend(chunk[:room])

    reader = threading.Thread(target=drain, name="bounded-reader", daemon=True)
    reader.start()
    timed_out = False
    try:
        returncode = process.wait(timeout=max(0.0, timeout))
    except subprocess.TimeoutExpired:
        timed_out = True
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        returncode = process.wait()
    reader.join(READER_JOIN_SECONDS)
    if reader.is_alive():
        # A descendant escaped the process group and still holds the pipe.
        oversized.set()
    try:
        process.stdout.close()
    except OSError:
        pass
    return BoundedResult(
        returncode, bytes(captured), timed_out, oversized.is_set(),
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
