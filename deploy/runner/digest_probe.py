#!/usr/bin/env python3
"""Read one node's public health and bounded performance rows; never return raw node logs."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.request

TRACE_DIR = Path("/var/log/zakura-digest-perf")
MAX_BYTES = 1024 * 1024


def read_rows(directory, cursor, maximum=MAX_BYTES):
    """Continue by inode across the fixed rotation ring; expose any lost cursor explicitly."""
    paths = [directory / f"events.jsonl.{index}" for index in (3, 2, 1)] + [directory / "events.jsonl"]
    paths = [path for path in paths if path.is_file()]
    if not paths:
        return [], cursor, 0
    inodes = [path.stat().st_ino for path in paths]
    lost = int(bool(cursor) and cursor.get("inode") not in inodes)
    if not cursor:
        # Starting collection does not ingest an earlier experiment's records.
        return [], {"inode": inodes[-1], "offset": paths[-1].stat().st_size}, 0
    start = inodes.index(cursor["inode"]) if cursor.get("inode") in inodes else 0
    rows = []
    consumed = 0
    result = dict(cursor)
    for index in range(start, len(paths)):
        offset = cursor.get("offset", 0) if inodes[index] == cursor.get("inode") else 0
        with paths[index].open("rb") as stream:
            stream.seek(offset)
            chunk = stream.read(maximum - consumed)
        boundary = chunk.rfind(b"\n") + 1
        complete = chunk[:boundary]
        for line in complete.splitlines():
            try:
                row = json.loads(line)
            except (ValueError, UnicodeDecodeError):
                lost += 1
                continue
            if row.get("event") in {"start", "finish", "phase", "checkpoint"}:
                rows.append(row)
            else:
                lost += 1
        result = {"inode": inodes[index], "offset": offset + boundary}
        consumed += len(chunk)
        if consumed >= maximum or boundary != len(chunk):
            break
    return rows, result, lost


def rpc_info():
    body = json.dumps({"jsonrpc": "2.0", "id": "digest-perf", "method": "getblockchaininfo", "params": []}).encode()
    request = urllib.request.Request("http://127.0.0.1:8232", data=body, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            reply = json.load(response)
        result = reply.get("result") or {}
        return {key: result.get(key) for key in ("chain", "blocks", "bestblockhash", "estimatedheight")}
    except Exception as error:
        # Error messages may include request headers; only export the exception class.
        return {"error": type(error).__name__}


def sample(cursor):
    pid = int(subprocess.check_output(["systemctl", "show", "zakurad", "-p", "MainPID", "--value"], text=True, timeout=5).strip())
    service = subprocess.check_output(["systemctl", "show", "zakurad", "-p", "ActiveState", "--value"], text=True, timeout=5).strip()
    result = {"at": time.time(), "pid": pid, "service": service, "rpc": rpc_info(), "load": os.getloadavg()}
    filesystem = os.statvfs("/mnt/data")
    result["disk_free"] = filesystem.f_bavail * filesystem.f_frsize
    trace_filesystem = os.statvfs(TRACE_DIR if TRACE_DIR.exists() else TRACE_DIR.parent)
    result["trace_disk_free"] = trace_filesystem.f_bavail * trace_filesystem.f_frsize
    previous_pid = cursor.get("pid")
    result["binary_sha256"] = cursor.get("binary_sha256")
    result["version"] = cursor.get("version")
    try:
        if pid:
            stat = Path(f"/proc/{pid}/stat").read_text().rsplit(") ", 1)[1].split()
            result["cpu_seconds"] = (int(stat[11]) + int(stat[12])) / os.sysconf("SC_CLK_TCK")
            status = Path(f"/proc/{pid}/status").read_text().splitlines()
            result["rss_kib"] = next(int(line.split()[1]) for line in status if line.startswith("VmRSS:"))
            if pid != previous_pid or not result["binary_sha256"]:
                with Path(f"/proc/{pid}/exe").open("rb") as binary:
                    result["binary_sha256"] = hashlib.file_digest(binary, "sha256").hexdigest()
                result["version"] = subprocess.check_output([f"/proc/{pid}/exe", "--version"], text=True, timeout=5).strip()
    except (FileNotFoundError, ProcessLookupError):
        result.update(pid=0, binary_sha256=None, process_changed_during_sample=True)
    rows, position, lost = read_rows(TRACE_DIR, cursor.get("position"))
    result.update(rows=rows, lost_segments=lost, cursor={"position": position, "pid": result["pid"], "binary_sha256": result["binary_sha256"], "version": result["version"]})
    return result


def backup(expected):
    if len(expected) != 64 or any(character not in "0123456789abcdef" for character in expected):
        raise ValueError("backup requires the recorded original binary checksum")
    source = Path("/usr/local/bin/zakurad")
    with source.open("rb") as binary:
        actual = hashlib.file_digest(binary, "sha256").hexdigest()
    if actual != expected:
        raise ValueError("installed binary changed since original identity was recorded")
    destination = source.with_name(f"zakurad.digest-original-{expected}")
    if not destination.exists():
        import shutil
        shutil.copy2(source, destination)
    with destination.open("rb") as binary:
        if hashlib.file_digest(binary, "sha256").hexdigest() != expected:
            raise ValueError("original binary backup checksum mismatch")
    print(json.dumps({"backup": str(destination), "sha256": expected}))


def restore(expected):
    """Restore only the checksum-verified original executable; configuration and state stay put."""
    if len(expected) != 64 or any(character not in "0123456789abcdef" for character in expected):
        raise ValueError("restore requires the recorded original checksum")
    import shutil
    executable = Path("/usr/local/bin/zakurad")
    original = executable.with_name(f"zakurad.digest-original-{expected}")
    with original.open("rb") as binary:
        if hashlib.file_digest(binary, "sha256").hexdigest() != expected:
            raise ValueError("original binary backup checksum mismatch")
    staging = executable.with_name("zakurad.digest-restore-staging")
    shutil.copy2(original, staging)
    staging.replace(executable)
    subprocess.run(["systemctl", "restart", "zakurad"], check=True, timeout=45)
    print(json.dumps({"restored_sha256": expected}))


def profile(phase):
    """Keep at most two 128-MiB profiles; record whether the bounded sample succeeded."""
    if phase not in {"phase1", "phase2"}:
        raise ValueError("unknown profile phase")
    import resource
    directory = TRACE_DIR / "profiles"
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"{phase}.data"
    pid = subprocess.check_output(["systemctl", "show", "zakurad", "-p", "MainPID", "--value"], text=True, timeout=5).strip()
    target.unlink(missing_ok=True)
    target.with_suffix(".data.old").unlink(missing_ok=True)
    start = time.time()
    def limit_file():
        resource.setrlimit(resource.RLIMIT_FSIZE, (128 * 1024 * 1024, 128 * 1024 * 1024))
    record = subprocess.run(["timeout", "75", "perf", "record", "-e", "cpu-clock", "-F", "49", "--call-graph", "dwarf,8192", "-p", pid, "-o", str(target), "--", "sleep", "60"], capture_output=True, text=True, timeout=85, preexec_fn=limit_file)
    report = subprocess.run(["perf", "report", "--stdio", "--no-children", "--percent-limit", "1", "-i", str(target)], capture_output=True, text=True, timeout=15) if record.returncode == 0 else None
    print(json.dumps({"phase": phase, "pid": int(pid), "start": start, "end": time.time(), "exit": record.returncode, "record_error": record.stderr[-800:] if record.returncode else "", "report_exit": report.returncode if report else None, "report": report.stdout[:16000] if report else "", "bytes": target.stat().st_size if target.exists() else 0}))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["sample", "backup", "restore", "profile"])
    parser.add_argument("value")
    args = parser.parse_args(argv)
    if args.operation == "sample":
        print(json.dumps(sample(json.loads(args.value))))
    elif args.operation == "backup":
        backup(args.value)
    elif args.operation == "restore":
        restore(args.value)
    else:
        profile(args.value)


if __name__ == "__main__":
    main()
