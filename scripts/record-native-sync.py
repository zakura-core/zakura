#!/usr/bin/env python3
"""Record a local test node's progress and resource pressure without controlling it."""
import argparse
import gzip
import json
from pathlib import Path
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request

from native_sync_pressure import MAX_SAMPLE_BYTES

CGROUP_FIELDS = (
    "memory.current", "memory.peak", "memory.max", "memory.high", "memory.pressure",
    "memory.stat", "memory.events", "memory.swap.current", "pids.current", "pids.peak",
    "pids.events", "cpu.stat", "io.stat",
)
HOST_FIELDS = ("stat", "meminfo", "diskstats", "net/dev", "pressure/cpu", "pressure/io", "pressure/memory")
UNIT_FIELDS = ("MainPID", "ControlGroup", "ActiveState", "ExecMainStatus", "Result", "LoadState")
MAX_HTTP_BYTES = 2 * 1024 * 1024


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, new_url):
        raise urllib.error.URLError("local observation endpoint redirected")


def local_url(value):
    """The observer reads this host's test-node endpoints only."""
    parsed = urllib.parse.urlsplit(value)
    if (parsed.scheme != "http" or parsed.hostname not in ("127.0.0.1", "::1")
            or parsed.username is not None or parsed.password is not None or parsed.fragment):
        raise argparse.ArgumentTypeError("use an HTTP URL on 127.0.0.1 or [::1], without credentials")
    return value


def file_text(path):
    try:
        return Path(path).read_text()
    except OSError as error:
        return {"error": str(error)}


def read_url(url, payload=None):
    try:
        request = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
        # Do not forward local observations through ambient HTTP proxy settings.
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
        with opener.open(request, timeout=2) as response:
            body = response.read(MAX_HTTP_BYTES + 1)
        if len(body) > MAX_HTTP_BYTES:
            return {"error": "response exceeds observation byte limit"}
        return {"body": body.decode()}
    except Exception as error:
        return {"error": str(error)}


def observe(unit, metrics_url, rpc_url):
    """Bracket all reads so later analysis can retain timestamp uncertainty."""
    row = {"schema": 1, "clock": "monotonic_ns", "unit_name": unit,
           "sample_start_ns": time.monotonic_ns(), "utc_ns": time.time_ns()}
    raw = subprocess.check_output(
        ["systemctl", "show", "--property=" + ",".join(UNIT_FIELDS), "--", unit],
        text=True, timeout=5,
    )
    properties = dict(line.split("=", 1) for line in raw.splitlines() if "=" in line)
    if not all(field in properties for field in UNIT_FIELDS):
        raise ValueError("systemd did not return the required node unit properties")
    if properties["LoadState"] != "loaded":
        raise ValueError("the selected node unit is not loaded")
    row["unit"] = properties
    pid = properties["MainPID"]
    row["process"] = {name: file_text(f"/proc/{pid}/{name}") for name in ("stat", "status", "io")}
    try:
        row["open_file_descriptors"] = sum(1 for _ in Path(f"/proc/{pid}/fd").iterdir())
    except OSError as error:
        row["open_file_descriptors"] = {"error": str(error)}
    group = properties["ControlGroup"]
    row["cgroup"] = {name: file_text("/sys/fs/cgroup" + group + "/" + name)
                     for name in CGROUP_FIELDS} if group else {}
    row["host"] = {name: file_text("/proc/" + name) for name in HOST_FIELDS}
    row["boot_id"] = file_text("/proc/sys/kernel/random/boot_id")
    row["metrics"] = read_url(metrics_url)
    row["chain"] = read_url(rpc_url, b'{"jsonrpc":"2.0","id":1,"method":"getblockchaininfo","params":[]}')
    row["sample_end_ns"] = time.monotonic_ns()
    return row


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--unit", required=True, help="Existing test-node systemd service.")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--seconds", type=int, default=10800)
    parser.add_argument("--metrics-url", type=local_url, default="http://127.0.0.1:19999/metrics")
    parser.add_argument("--rpc-url", type=local_url, default="http://127.0.0.1:18232")
    args = parser.parse_args()
    if not 1 <= args.seconds <= 172800:
        parser.error("recording duration must be between one second and two days")
    if not args.unit.endswith(".service"):
        parser.error("the observed unit must be a systemd service")
    args.out.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + args.seconds
    with gzip.open(args.out, "xt") as output:
        while time.monotonic() < deadline:
            started = time.monotonic()
            row = observe(args.unit, args.metrics_url, args.rpc_url)
            encoded = json.dumps(row) + "\n"
            if len(encoded) > MAX_SAMPLE_BYTES:
                raise ValueError("combined sample exceeds the recording row limit")
            output.write(encoded)
            output.flush()
            if row["unit"]["ActiveState"] in ("inactive", "failed"):
                break
            time.sleep(max(0, min(2 - (time.monotonic() - started), deadline - time.monotonic())))


if __name__ == "__main__":
    main()
