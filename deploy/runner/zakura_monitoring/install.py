"""Host-side install, cutover and rollback operations for monitoring packages.

This file is self-contained so deployments can stream it to a host with
``python3 - <operation> ...`` before any package is installed there. It runs as
root and only touches monitoring artifacts: package releases, the fleet
watchdog's links, env file, known_hosts, drop-in and state backups, and the
retired Rust watchdog's unit, binary and env file. It never stops, starts or
modifies zakurad, zcashd or their configuration.

Layout under a package root (``/opt/zakura-monitoring`` on zakura-compat,
``/opt/zakura-fleet-watchdog`` on us-east-0)::

    releases/<sha>/      one immutable, manifest-verified release per commit
    current -> releases/<sha>
    rollback.json        the release ``current`` pointed to before activation
    acceptance.json      soak evidence for the current cutover generation

Output is a single JSON object of paths, digests and states. Credential values
and env file contents are never printed.
"""

from __future__ import annotations

import argparse
import datetime
import gzip
import hashlib
import io
import json
import math
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import uuid
from pathlib import Path


SHA = re.compile(r"[0-9a-f]{40}")
LEGACY = re.compile(r"legacy-[0-9]{8}T[0-9]{6}Z")
MANIFEST = "MANIFEST.json"
EXECUTABLES = frozenset(
    {
        "zakura-cluster-watchdog.py",
        "zakura-compat-check",
        "zakura-monitoring-acceptance.py",
        "zakura-monitoring-deploy.py",
    }
)
MAX_PACKAGE_BYTES = 16 << 20
KEEP_RELEASES = 5
SLACK_KEYS = ("SLACK_WEB_HOOK", "SLACK_WEBHOOK_URL", "SLACK_WEBHOOK")
LANE_DROP_IN = Path(
    "/etc/systemd/system/zakura-fleet-watchdog.service.d/80-compat-monitoring.conf"
)
LANE_DROP_IN_TEXT = (
    "# Written by the monitoring cutover; removed by monitoring rollback.\n"
    "[Service]\n"
    "Environment=ZAKURA_COMPAT_MONITORING=1\n"
)
RUST_UNIT = "zakura-watchdog.service"
RUST_ARTIFACTS = (
    Path("/etc/systemd/system/zakura-watchdog.service"),
    Path("/usr/local/bin/zakura-watchdog"),
    Path("/etc/zakura-watchdog/env"),
)
HOST_KEY_LINE = re.compile(r"[A-Za-z0-9|+/=\[\]:.,_-]+ (ssh-ed25519|ecdsa-sha2-nistp[0-9]+|ssh-rsa) [A-Za-z0-9+/=]+")


# Bootstrap upload before this package exists on the remote host. mkdtemp
# creates a private directory atomically; exclusive creation rejects symlinks.
UPLOAD_SCRIPT = """import json, os, shutil, sys, tempfile
os.umask(0o077)
directory = tempfile.mkdtemp(prefix="zakura-monitoring-", dir=sys.argv[1])
try:
    limit = int(sys.argv[2])
    data = sys.stdin.buffer.read(limit + 1)
    if len(data) > limit:
        raise ValueError("monitoring package is oversized")
    tarball = directory + "/package.tar.gz"
    with open(tarball, "xb") as target:
        target.write(data)
    print(json.dumps({"tarball": tarball}))
except BaseException:
    shutil.rmtree(directory)
    raise
"""


class InstallError(RuntimeError):
    pass


def utc_stamp() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 16), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_write(path: Path, data: bytes, mode: int) -> None:
    """Write ``data`` to a sibling temp file with ``mode``, fsync, then rename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temp = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)
    except BaseException:
        try:
            os.unlink(temp)
        except FileNotFoundError:
            pass
        raise


def atomic_symlink(link: Path, target: str) -> None:
    temp = link.with_name(f".{link.name}.{os.getpid()}.tmp")
    try:
        temp.unlink()
    except FileNotFoundError:
        pass
    os.symlink(target, temp)
    os.replace(temp, link)


def read_manifest(release: Path) -> dict:
    try:
        manifest = json.loads((release / MANIFEST).read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise InstallError("release manifest is missing or invalid") from error
    if not isinstance(manifest, dict) or not isinstance(manifest.get("files"), dict):
        raise InstallError("release manifest is malformed")
    return manifest


def verify_release(release: Path, sha: str) -> dict:
    """Require exactly the manifest's files, each with its recorded digest."""
    manifest = read_manifest(release)
    if manifest.get("sha") != sha:
        raise InstallError("release manifest names a different commit")
    expected = manifest["files"]
    present = {
        path.relative_to(release).as_posix()
        for path in release.rglob("*")
        if path.is_file() and path.name != MANIFEST and "__pycache__" not in path.parts
    }
    if present != set(expected):
        raise InstallError("release files differ from its manifest")
    for name, digest in expected.items():
        if sha256_file(release / name) != digest:
            raise InstallError(f"release file digest mismatch: {name}")
    return manifest


PACKAGE_TOP_FILES = (
    "zakura-cluster-watchdog.py",
    "zakura-compat-check",
    "zakura-monitoring-acceptance.py",
    "fleet-watchdog.toml",
    "zakura-fleet-watchdog.service",
    "test_zakura_monitoring_compat.py",
    "test_zakura_monitoring_lane.py",
    "test_zakura_monitoring_install.py",
)


def package_files(source: Path) -> list[str]:
    """Every file a release contains, relative to ``deploy/runner`` (or a release)."""
    modules = sorted(
        path.relative_to(source).as_posix()
        for path in (source / "zakura_monitoring").glob("*.py")
    )
    return [*PACKAGE_TOP_FILES, *modules]


def build_package(source: Path, sha: str, out_dir: Path) -> dict:
    """Build a reproducible release tarball with a per-file digest manifest."""
    if not SHA.fullmatch(sha):
        raise InstallError("commit must be a full 40-character SHA")
    files = package_files(source)
    contents = {name: (source / name).read_bytes() for name in files}
    manifest = {
        "sha": sha,
        "files": {name: hashlib.sha256(data).hexdigest() for name, data in contents.items()},
    }
    contents[MANIFEST] = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode()
    out_dir.mkdir(parents=True, exist_ok=True)
    tarball = out_dir / f"zakura-monitoring-{sha}.tar.gz"
    with tarball.open("wb") as raw, gzip.GzipFile(
        filename="", fileobj=raw, mode="wb", mtime=0
    ) as compressed, tarfile.open(fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT) as archive:
        for name in sorted(contents):
            info = tarfile.TarInfo(name)
            info.size = len(contents[name])
            info.mode = 0o755 if Path(name).name in EXECUTABLES else 0o644
            info.mtime = 0
            archive.addfile(info, io.BytesIO(contents[name]))
    return {
        "tarball": str(tarball),
        "digest": sha256_file(tarball),
        "manifest_sha256": hashlib.sha256(contents[MANIFEST]).hexdigest(),
        "files": len(files),
    }


def safe_extract(tarball: Path, destination: Path) -> None:
    with tarfile.open(tarball, "r:gz") as archive:
        for member in archive.getmembers():
            name = Path(member.name)
            if (
                name.is_absolute()
                or ".." in name.parts
                or not (member.isfile() or member.isdir())
            ):
                raise InstallError("package contains an unsafe member")
        for member in archive.getmembers():
            target = destination / member.name
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            source = archive.extractfile(member)
            assert source is not None
            with source, target.open("wb") as handle:
                shutil.copyfileobj(source, handle)


def stage(root: Path, sha: str, tarball: Path, digest: str) -> dict:
    """Verify and unpack one release without changing what is active."""
    if not SHA.fullmatch(sha):
        raise InstallError("commit must be a full 40-character SHA")
    if tarball.stat().st_size > MAX_PACKAGE_BYTES:
        raise InstallError("package is too large")
    if sha256_file(tarball) != digest:
        raise InstallError("package digest mismatch")
    releases = root / "releases"
    releases.mkdir(parents=True, exist_ok=True)
    os.chmod(root, 0o755)
    os.chmod(releases, 0o755)
    final = releases / sha
    staging = Path(tempfile.mkdtemp(prefix=f".{sha}.", dir=releases))
    try:
        safe_extract(tarball, staging)
        manifest = verify_release(staging, sha)
        for path in sorted(staging.rglob("*")):
            os.chmod(path, 0o755 if path.is_dir() or path.name in EXECUTABLES else 0o644)
        os.chmod(staging, 0o755)
        check = subprocess.run(
            [sys.executable, "-I", str(staging / "zakura-compat-check"), "version"],
            capture_output=True, timeout=60, check=False,
        )
        if check.returncode != 0 or json.loads(check.stdout or b"{}").get("sha") != sha:
            raise InstallError("staged checker failed its import check")
        if final.exists():
            if read_manifest(final) != manifest:
                raise InstallError("an existing release for this commit differs")
            verify_release(final, sha)
            return {"release": str(final), "sha": sha, "reused": True}
        os.replace(staging, final)
        return {"release": str(final), "sha": sha, "reused": False}
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def current_release(root: Path) -> str | None:
    link = root / "current"
    if not link.is_symlink():
        return None
    return Path(os.readlink(link)).name


def preserve_legacy(root: Path, links: dict[str, str]) -> str | None:
    """Copy a pre-release (plain file) install aside so rollback can return to it."""
    legacy_files = [
        name for name in links if (root / name).is_file() and not (root / name).is_symlink()
    ]
    if not legacy_files:
        return None
    name = f"legacy-{utc_stamp()}"
    legacy = root / "releases" / name
    legacy.mkdir(parents=True)
    for file_name in legacy_files:
        # Store each file where its link will point, so links work unchanged.
        target = legacy / links[file_name]
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(root / file_name, target)
    os.chmod(legacy, 0o755)
    return name


def prune(root: Path, keep: set[str]) -> list[str]:
    releases = root / "releases"
    candidates = sorted(
        (
            path for path in releases.iterdir()
            if path.is_dir() and not path.name.startswith(".") and path.name not in keep
        ),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    removed = []
    for path in candidates[KEEP_RELEASES:]:
        shutil.rmtree(path)
        removed.append(path.name)
    return removed


def point_links(root: Path, links: dict[str, str]) -> None:
    for name, relative in links.items():
        atomic_symlink(root / name, f"current/{relative}")


def activate(root: Path, sha: str, links: dict[str, str]) -> dict:
    """Atomically switch ``current`` (and the named links) to an installed release."""
    release = root / "releases" / sha
    verify_release(release, sha)
    previous = current_release(root)
    legacy = None
    if previous is None:
        legacy = preserve_legacy(root, links)
        previous = legacy
    if previous != sha:
        atomic_write(
            root / "rollback.json",
            (json.dumps({"previous": previous, "active": sha, "at": time.time()},
                        sort_keys=True) + "\n").encode(),
            0o644,
        )
    atomic_symlink(root / "current", f"releases/{sha}")
    point_links(root, links)
    removed = prune(root, {sha, previous or ""})
    return {"active": sha, "previous": previous, "legacy_preserved": legacy, "pruned": removed}


def rollback(root: Path, links: dict[str, str]) -> dict:
    """Restore the pre-activation release; retries keep the same rollback target."""
    try:
        record = json.loads((root / "rollback.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"rolled_back": False, "reason": "no rollback record"}
    previous = record.get("previous")
    active = current_release(root)
    if previous is None:
        # Nothing was active before the first activation: deactivate.
        (root / "current").unlink(missing_ok=True)
        for name in links:
            if (root / name).is_symlink():
                (root / name).unlink()
        return {"rolled_back": True, "active": None, "previous": active}
    if not (isinstance(previous, str) and (SHA.fullmatch(previous) or LEGACY.fullmatch(previous))):
        raise InstallError("rollback record is malformed")
    if not (root / "releases" / previous).is_dir():
        raise InstallError("rollback release is missing")
    if SHA.fullmatch(previous):
        verify_release(root / "releases" / previous, previous)
    atomic_symlink(root / "current", f"releases/{previous}")
    point_links(root, links)
    # Keep the activation record intact, even if a later rollback step fails.
    # A subsequent activation writes a new record; rollback never swaps targets.
    return {"rolled_back": True, "active": previous, "previous": active}


def status(root: Path) -> dict:
    releases = root / "releases"
    try:
        record = json.loads((root / "rollback.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        record = None
    return {
        "root": str(root),
        "current": current_release(root),
        "releases": sorted(
            path.name for path in releases.iterdir()
            if path.is_dir() and not path.name.startswith(".")
        ) if releases.is_dir() else [],
        "rollback": record,
    }


def parse_env_lines(text: str) -> list[tuple[str | None, str]]:
    lines = []
    for raw in text.splitlines():
        stripped = raw.strip()
        key = None
        if stripped and not stripped.startswith("#") and "=" in stripped:
            key = stripped.split("=", 1)[0].strip().removeprefix("export ").strip()
        lines.append((key, raw))
    return lines


def fleet_env(env_file: Path, new_hook: str = "") -> dict:
    """Preserve the fleet env file, replacing only the Slack webhook when one is supplied.

    The new webhook arrives on stdin (never argv). Every other operator
    setting, for example Mac comparison flags, is kept verbatim.
    """
    new_hook = new_hook.strip()
    if "\n" in new_hook or "\r" in new_hook:
        raise InstallError("webhook must be a single line")
    try:
        existing = env_file.read_text(encoding="utf-8")
    except FileNotFoundError:
        existing = ""
    lines = parse_env_lines(existing)
    if new_hook:
        kept = [raw for key, raw in lines if key not in SLACK_KEYS]
        kept.append(f"SLACK_WEB_HOOK={new_hook}")
        outcome = "replaced" if any(key in SLACK_KEYS for key, _ in lines) else "added"
    else:
        kept = [raw for _key, raw in lines]
        outcome = "preserved" if any(key in SLACK_KEYS for key, _ in lines) else "missing"
    atomic_write(env_file, ("\n".join(kept) + "\n").encode() if kept else b"", 0o600)
    return {"env_file": str(env_file), "mode": "600", "slack_webhook": outcome}


def ensure_unit(unit: Path, template: Path) -> dict:
    """Install the template only on a host that has no unit; never replace a live one."""
    if unit.exists():
        return {"unit": str(unit), "installed": False}
    atomic_write(unit, template.read_bytes(), 0o644)
    return {"unit": str(unit), "installed": True}


def compat_env(release: Path, source: Path, destination: Path) -> dict:
    """Seed the checker's env file from the Rust watchdog's, keeping checker keys only."""
    if destination.exists():
        return {"env_file": str(destination), "seeded": False}
    sys.path.insert(0, str(release))
    from zakura_monitoring.compat import ENV_KEYS, read_env_file

    values = read_env_file(source) if source.exists() else {}
    body = "".join(
        f"{key}={values[key]}\n" for key in sorted(values) if key in ENV_KEYS
    )
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    atomic_write(destination, body.encode(), 0o600)
    return {
        "env_file": str(destination),
        "seeded": True,
        "keys": sorted(values),
        "source_present": source.exists(),
    }


def known_hosts(destination: Path, data: bytes) -> dict:
    lines = [line.strip() for line in data.decode("ascii").splitlines() if line.strip()]
    if not lines or not all(HOST_KEY_LINE.fullmatch(line) for line in lines):
        raise InstallError("known_hosts input is empty or malformed")
    atomic_write(destination, ("\n".join(lines) + "\n").encode(), 0o644)
    return {"known_hosts": str(destination), "entries": len(lines)}


def set_lane(enabled: bool, drop_in: Path = LANE_DROP_IN) -> dict:
    if enabled:
        atomic_write(drop_in, LANE_DROP_IN_TEXT.encode(), 0o644)
    else:
        drop_in.unlink(missing_ok=True)
    return {"drop_in": str(drop_in), "enabled": enabled}


def backup_state(state: Path, directory: Path) -> dict:
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    if not state.exists():
        return {"state_backup": None}
    target = directory / f"state-{utc_stamp()}.json"
    atomic_write(target, state.read_bytes(), 0o600)
    return {"state_backup": str(target), "sha256": sha256_file(target)}


def restore_state(backup: Path, state: Path) -> dict:
    data = backup.read_bytes()
    json.loads(data)
    atomic_write(state, data, 0o600)
    return {"state_restored_from": str(backup)}


VALIDATION_CHECKS = frozenset({
    "service_context_probe", "rust_python_parity", "synthetic_fleet", "synthetic_compat",
})


def validation(root: Path, sha: str, action: str, checks: dict | None = None) -> dict:
    """Bind successful pre-cutover checks to a verified staged release.

    Begin removes earlier success; pass requires all four checks. Require
    verifies the installed files again and rejects missing or stale evidence.
    The receipt is outside the release and never changes incident state.
    """
    if not SHA.fullmatch(sha):
        raise InstallError("validation commit must be a full SHA")
    release = root / "releases" / sha
    verify_release(release, sha)
    digest = sha256_file(release / MANIFEST)
    path = root / "validation" / (sha + ".json")
    if action == "require":
        try:
            record = json.loads(path.read_text())
        except (OSError, ValueError) as error:
            raise InstallError("successful installed validation is required before cutover") from error
        if (not isinstance(record, dict) or record.get("sha") != sha
                or record.get("manifest_sha256") != digest
                or not isinstance(record.get("checks"), dict)
                or set(record["checks"]) != VALIDATION_CHECKS
                or any(value is not True for value in record["checks"].values())):
            raise InstallError("validation proof is missing, failed or stale")
        return record
    if action not in ("begin", "pass"):
        raise InstallError("unknown validation operation")
    if action == "pass" and (not isinstance(checks, dict)
            or set(checks) != VALIDATION_CHECKS or any(v is not True for v in checks.values())):
        raise InstallError("all installed validation checks must pass")
    record = {"sha": sha, "manifest_sha256": digest, "checks": checks if action == "pass" else None}
    atomic_write(path, (json.dumps(record, sort_keys=True) + "\n").encode(), 0o600)
    return record


SOAK_MIN_SECONDS = 1800
SOAK_CHECKS = frozenset({
    "service_always_active", "probes_completed", "all_new_probes_passed",
    "no_unavailable_probes", "height_advanced", "no_open_incident",
})


def _finite_number(value: object) -> bool:
    return type(value) in (int, float) and math.isfinite(value)


def acceptance(root: Path, sha: str, action: str, generation: str | None = None,
               evidence: dict | None = None) -> dict:
    """Persist soak proof for one release/cutover, rejecting stale or failed runs.

    ``begin`` invalidates older evidence. ``soak`` accepts a passing report only
    for that generation and after at least 30 minutes. ``require`` fails until
    that proof exists; ``status`` supplies the generation before observation.
    These records are separate from fleet incidents and notification queues.
    """
    if not SHA.fullmatch(sha) or current_release(root) != sha:
        raise InstallError("acceptance release is not the active commit")
    path = root / "acceptance.json"
    if action == "begin":
        record = {"sha": sha, "generation": uuid.uuid4().hex,
                  "started_at": time.time(), "soak": None}
    else:
        try:
            record = json.loads(path.read_text())
        except (OSError, ValueError) as error:
            raise InstallError("cutover acceptance record is missing or invalid") from error
        if (not isinstance(record, dict) or record.get("sha") != sha
                or not isinstance(record.get("generation"), str)
                or not _finite_number(record.get("started_at"))):
            raise InstallError("cutover acceptance record belongs to another release")
        if generation is not None and generation != record["generation"]:
            raise InstallError("cutover changed during acceptance")
        if action == "soak":
            report = evidence or {}
            if not isinstance(report, dict) or not isinstance(report.get("release"), dict):
                raise InstallError("soak report is malformed")
            checks = report.get("checks")
            numbers = [report.get(key) for key in
                       ("duration", "elapsed", "started_at", "finished_at")]
            if not all(_finite_number(value) for value in numbers):
                raise InstallError("soak times must be finite numbers")
            duration, elapsed, started, finished = numbers
            if (generation is None or report.get("passed") is not True
                    or not isinstance(checks, dict) or set(checks) != SOAK_CHECKS
                    or any(value is not True for value in checks.values())
                    or report.get("release", {}).get("sha") != sha
                    or duration < SOAK_MIN_SECONDS or elapsed < SOAK_MIN_SECONDS
                    or started < record["started_at"] or finished < started + SOAK_MIN_SECONDS
                    or finished > time.time() + 5):
                raise InstallError("soak must pass for 30 minutes after this cutover")
            record["soak"] = {"started_at": started, "finished_at": finished, "elapsed": elapsed}
        elif action == "require":
            proof = record.get("soak")
            if (not isinstance(proof, dict)
                    or not all(_finite_number(proof.get(key)) for key in
                               ("started_at", "finished_at", "elapsed"))
                    or proof["elapsed"] < SOAK_MIN_SECONDS
                    or proof["started_at"] < record["started_at"]
                    or proof["finished_at"] < proof["started_at"] + SOAK_MIN_SECONDS):
                raise InstallError("a successful 30-minute soak is required before retirement")
        elif action != "status":
            raise InstallError("unknown acceptance operation")
    if action in ("begin", "soak"):
        atomic_write(path, (json.dumps(record, sort_keys=True) + "\n").encode(), 0o600)
    return record


def systemctl(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["systemctl", *args], capture_output=True, text=True, timeout=120, check=False
    )


def checked_systemctl(*args: str) -> None:
    """Fail the host operation when systemd rejects it, without exposing logs."""
    result = systemctl(*args)
    if result.returncode != 0:
        raise InstallError(f"systemctl {' '.join(args)} failed ({result.returncode})")


def retire_rust(backup_root: Path) -> dict:
    """Stop the Rust watchdog and move its artifacts aside (never delete them)."""
    present = [path for path in RUST_ARTIFACTS if path.exists()]
    if not present:
        return {"retired": False, "reason": "rust watchdog artifacts not present"}
    checked_systemctl("disable", "--now", RUST_UNIT)
    backup = backup_root / f"rust-watchdog-{utc_stamp()}"
    backup.mkdir(mode=0o700, parents=True)
    # Journal the complete plan before the first destructive move. Restoration
    # accepts matching originals for moves that had not happened yet.
    moved = {str(path): sha256_file(path) for path in present}
    atomic_write(
        backup / "manifest.json",
        (json.dumps({"moved": moved, "at": time.time()}, sort_keys=True) + "\n").encode(),
        0o600,
    )
    for path in present:
        destination = backup / path.relative_to("/")
        destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        shutil.move(str(path), destination)
    checked_systemctl("daemon-reload")
    return {"retired": True, "backup": str(backup), "moved": moved}


def restore_rust(backup_root: Path) -> dict:
    """Restore and start the latest retired watchdog, safely resuming retries."""
    backups = sorted(backup_root.glob("rust-watchdog-*")) if backup_root.is_dir() else []
    if not backups:
        return {"restored": False, "reason": "no rust watchdog backup"}
    backup = backups[-1]
    manifest = json.loads((backup / "manifest.json").read_text(encoding="utf-8"))
    restored = []
    for original, digest in manifest["moved"].items():
        source = backup / Path(original).relative_to("/")
        if Path(original).exists():
            # A previous attempt may have moved this file before failing later.
            if source.exists() or sha256_file(Path(original)) != digest:
                raise InstallError(f"refusing to overwrite existing {original}")
            restored.append(original)
            continue
        if sha256_file(source) != digest:
            raise InstallError(f"backup digest mismatch for {original}")
        Path(original).parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source), original)
        restored.append(original)
    checked_systemctl("daemon-reload")
    checked_systemctl("enable", "--now", RUST_UNIT)
    if not backup.name.endswith(".restored"):
        os.rename(backup, backup.with_name(backup.name + ".restored"))
    return {"restored": True, "files": restored, "service_started": True}


def parse_links(values: list[str]) -> dict[str, str]:
    links = {}
    for value in values or []:
        name, _, relative = value.partition("=")
        if not name or "/" in name or not relative or ".." in Path(relative).parts:
            raise InstallError("links must be NAME=RELATIVE_PATH")
        links[name] = relative
    return links


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="operation", required=True)
    package = sub.add_parser("build-package")
    package.add_argument("--source", type=Path, required=True)
    package.add_argument("--sha", required=True)
    package.add_argument("--out", type=Path, required=True)
    staged = sub.add_parser("stage")
    staged.add_argument("--root", type=Path, required=True)
    staged.add_argument("--sha", required=True)
    staged.add_argument("--tarball", type=Path, required=True)
    staged.add_argument("--digest", required=True)
    for name in ("activate", "rollback", "status"):
        command = sub.add_parser(name)
        command.add_argument("--root", type=Path, required=True)
        command.add_argument("--link", action="append", default=[])
        if name == "activate":
            command.add_argument("--sha", required=True)
    env = sub.add_parser("fleet-env")
    env.add_argument("--env-file", type=Path, required=True)
    env.add_argument("--webhook-stdin", action="store_true")
    unit = sub.add_parser("ensure-unit")
    unit.add_argument("--unit", type=Path, required=True)
    unit.add_argument("--template", type=Path, required=True)
    seed = sub.add_parser("compat-env")
    seed.add_argument("--release", type=Path, required=True)
    seed.add_argument("--source", type=Path, required=True)
    seed.add_argument("--destination", type=Path, required=True)
    hosts = sub.add_parser("known-hosts")
    hosts.add_argument("--destination", type=Path, required=True)
    for name in ("enable-lane", "disable-lane"):
        sub.add_parser(name)
    backup = sub.add_parser("backup-state")
    backup.add_argument("--state", type=Path, required=True)
    backup.add_argument("--directory", type=Path, required=True)
    restore = sub.add_parser("restore-state")
    restore.add_argument("--backup", type=Path, required=True)
    restore.add_argument("--state", type=Path, required=True)
    for name in ("retire-rust", "restore-rust"):
        command = sub.add_parser(name)
        command.add_argument("--backup-root", type=Path, required=True)
    validated = sub.add_parser("validation")
    validated.add_argument("--root", type=Path, required=True)
    validated.add_argument("--sha", required=True)
    validated.add_argument("--action", choices=("begin", "pass", "require"), required=True)
    validated.add_argument("--checks")
    accepted = sub.add_parser("acceptance")
    accepted.add_argument("--root", type=Path, required=True)
    accepted.add_argument("--sha", required=True)
    accepted.add_argument("--action", choices=("begin", "status", "soak", "require"), required=True)
    accepted.add_argument("--generation")
    args = parser.parse_args(argv)

    try:
        operation = args.operation
        if operation == "build-package":
            result = build_package(args.source, args.sha, args.out)
        elif operation == "stage":
            result = stage(args.root, args.sha, args.tarball, args.digest)
        elif operation == "activate":
            result = activate(args.root, args.sha, parse_links(args.link))
        elif operation == "rollback":
            result = rollback(args.root, parse_links(args.link))
        elif operation == "status":
            result = status(args.root)
        elif operation == "fleet-env":
            hook = sys.stdin.read(4096) if args.webhook_stdin else ""
            result = fleet_env(args.env_file, hook)
        elif operation == "ensure-unit":
            result = ensure_unit(args.unit, args.template)
        elif operation == "compat-env":
            result = compat_env(args.release, args.source, args.destination)
        elif operation == "known-hosts":
            result = known_hosts(args.destination, sys.stdin.buffer.read(1 << 16))
        elif operation in ("enable-lane", "disable-lane"):
            result = set_lane(operation == "enable-lane")
        elif operation == "backup-state":
            result = backup_state(args.state, args.directory)
        elif operation == "restore-state":
            result = restore_state(args.backup, args.state)
        elif operation == "validation":
            result = validation(args.root, args.sha, args.action,
                                json.loads(args.checks) if args.checks else None)
        elif operation == "acceptance":
            evidence = json.load(sys.stdin) if args.action == "soak" else None
            result = acceptance(args.root, args.sha, args.action, args.generation, evidence)
        elif operation == "retire-rust":
            result = retire_rust(args.backup_root)
        else:
            result = restore_rust(args.backup_root)
    except (InstallError, OSError, ValueError, subprocess.SubprocessError, tarfile.TarError) as error:
        print(json.dumps({"operation": args.operation, "error": type(error).__name__,
                          "detail": str(error) if isinstance(error, InstallError) else ""}))
        return 1
    print(json.dumps({"operation": args.operation, **result}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
