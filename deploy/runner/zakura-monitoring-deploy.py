#!/usr/bin/env python3
"""Install and stage compatibility monitoring from the mainnet deploy runner (us-east-0).

Used by ``.github/workflows/zakura-mainnet-deploy.yml``:

- ``fleet-deploy``: every regular deploy. Installs this commit's fleet watchdog
  release, preserving the live unit, its drop-ins, state and env file; once
  the lane is enabled it also refreshes the checker on zakura-compat.
- ``operation=monitoring`` stages, each run explicitly and in order:

  ``install``   checker release on zakura-compat (activated; inert until
                cutover) and fleet release on us-east-0 (staged only)
  ``validate``  service-context SSH probe, Rust/Python parity and the shipped
                synthetic tests from the installed paths on both hosts
  ``cutover``   back up fleet state, activate the fleet release, enable the
                lane drop-in and restart only zakura-fleet-watchdog
  ``soak``      30-minute read-only observation of the live lane
  ``slack-test`` opt-in labeled messages to #zakura-alerts (temporary state)
  ``finalize``  stop the Rust zakura-watchdog and move its artifacts aside
  ``rollback``  undo cutover/finalize; never touches node processes
  ``status``    read-only summary of both hosts

No stage builds, installs, stops or restarts zakurad or zcashd. Production SSH
credentials come from the workflow's agent; nothing secret is printed.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from zakura_monitoring import install, monitor  # noqa: E402

COMPAT_NAME = "zakura-compat"
COMPAT_TARGET = monitor.KNOWN_TARGETS[COMPAT_NAME]
FLEET_ROOT = Path("/opt/zakura-fleet-watchdog")
COMPAT_ROOT = Path("/opt/zakura-monitoring")
FLEET_LINKS = (
    "zakura-cluster-watchdog.py=zakura-cluster-watchdog.py",
    "fleets.toml=fleet-watchdog.toml",
)
FLEET_UNIT = "zakura-fleet-watchdog.service"
FLEET_UNIT_PATH = Path("/etc/systemd/system") / FLEET_UNIT
FLEET_ENV = Path("/etc/zakura-fleet-watchdog/env")
FLEET_STATE = Path("/var/lib/zakura-fleet-watchdog/state.json")
FLEET_BACKUPS = Path("/var/lib/zakura-fleet-watchdog/backups")
FLEET_KNOWN_HOSTS = Path(monitor.DEFAULT_KNOWN_HOSTS)
COMPAT_ENV = Path(monitor.DEFAULT_ENV_FILE)
RUST_ENV = Path("/etc/zakura-watchdog/env")
RUST_BACKUPS = Path("/var/backups/zakura-monitoring")
REMOTE_STAGING = "/var/tmp/zakura-monitoring"
CUTOVER_WAIT_SECONDS = 300
FRESH_PROBE_SECONDS = 300


class StageError(RuntimeError):
    pass


class Context:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.sha = args.sha
        self.target = args.target
        self.evidence: dict = {
            "stage": args.stage,
            "sha": args.sha,
            "target": args.target,
            "run_url": args.run_url,
            "started_at": time.time(),
            "steps": [],
        }
        self.package: dict | None = None

    def record(self, step: str, result: object) -> None:
        self.evidence["steps"].append({"step": step, "result": result})
        print(json.dumps({"step": step, "result": result}, sort_keys=True), flush=True)

    # Local root operations through the shipped installer.
    def local(self, *arguments: str, stdin: bytes | None = None, timeout: int = 300) -> dict:
        command = [sys.executable, "-I", str(HERE / "zakura_monitoring" / "install.py"), *arguments]
        if os.geteuid() != 0:
            command = ["sudo", "-n", *command]
        result = subprocess.run(command, input=stdin, capture_output=True, timeout=timeout,
                                check=False)
        return parse_report(result, f"local {arguments[0]}")

    def systemctl(self, *arguments: str, check: bool = True) -> str:
        command = ["systemctl", *arguments]
        if os.geteuid() != 0:
            command = ["sudo", "-n", *command]
        result = subprocess.run(command, capture_output=True, text=True, timeout=180,
                                check=False)
        if check and result.returncode != 0:
            raise StageError(f"systemctl {' '.join(arguments)} failed ({result.returncode})")
        return result.stdout.strip()

    def ssh(self, remote: str, stdin: bytes | None = None, timeout: int = 300):
        command = [
            "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=20",
            "-o", "StrictHostKeyChecking=yes", "-o", "ServerAliveInterval=10",
            "-o", "ServerAliveCountMax=3", "-o", "LogLevel=ERROR",
        ]
        if self.args.known_hosts:
            command += ["-o", f"UserKnownHostsFile={self.args.known_hosts}"]
        return subprocess.run([*command, "--", self.target, remote], input=stdin,
                              capture_output=True, timeout=timeout, check=False)

    def remote_install(self, *arguments: str, timeout: int = 300) -> dict:
        source = (HERE / "zakura_monitoring" / "install.py").read_bytes()
        result = self.ssh("python3 - " + shlex.join(arguments), stdin=source, timeout=timeout)
        return parse_report(result, f"remote {arguments[0]}")

    def remote_json(self, remote_argv: list[str], timeout: int) -> tuple[int, dict]:
        result = self.ssh(shlex.join(remote_argv), timeout=timeout)
        try:
            return result.returncode, json.loads(result.stdout)
        except ValueError:
            return result.returncode, {"unparsable_output": True}


def parse_report(result: subprocess.CompletedProcess, label: str) -> dict:
    try:
        report = json.loads(result.stdout.decode().strip().splitlines()[-1])
    except (ValueError, IndexError):
        report = {"unparsable_output": True}
    if result.returncode != 0:
        raise StageError(f"{label} failed with exit {result.returncode}: {json.dumps(report)}")
    return report


def build(ctx: Context) -> dict:
    if ctx.package is None:
        ctx.package = install.build_package(HERE, ctx.sha, Path(ctx.args.work))
        ctx.evidence["package"] = {
            key: ctx.package[key] for key in ("digest", "manifest_sha256", "files")
        }
        ctx.record("package", ctx.evidence["package"])
    return ctx.package


def validate_target(ctx: Context) -> None:
    if ctx.target != COMPAT_TARGET:
        raise StageError(f"compatibility target must be {COMPAT_TARGET}")
    targets = monitor.load_compatibility_targets(HERE / "fleet-watchdog.toml")
    if [(t.name, t.ssh_target) for t in targets] != [(COMPAT_NAME, COMPAT_TARGET)]:
        raise StageError("fleet-watchdog.toml does not name the compatibility target")
    if ctx.args.deploy_config:
        import tomllib

        with Path(ctx.args.deploy_config).open("rb") as config:
            nodes = tomllib.load(config).get("nodes", [])
        hosts = [node.get("ssh_string") for node in nodes if node.get("name") == COMPAT_NAME]
        if hosts != [COMPAT_TARGET]:
            raise StageError("deploy config disagrees about the compatibility host")
    ctx.record("target", {"name": COMPAT_NAME, "ssh_target": COMPAT_TARGET})


def install_compat(ctx: Context) -> None:
    package = build(ctx)
    remote_tarball = f"{REMOTE_STAGING}/{ctx.sha}.tar.gz"
    upload = ctx.ssh(
        f"umask 077 && mkdir -p {REMOTE_STAGING} && cat > {shlex.quote(remote_tarball)}",
        stdin=Path(package["tarball"]).read_bytes(),
    )
    if upload.returncode != 0:
        raise StageError(f"upload to {COMPAT_NAME} failed ({upload.returncode})")
    try:
        ctx.record("compat stage", ctx.remote_install(
            "stage", "--root", str(COMPAT_ROOT), "--sha", ctx.sha,
            "--tarball", remote_tarball, "--digest", package["digest"]))
        ctx.record("compat activate", ctx.remote_install(
            "activate", "--root", str(COMPAT_ROOT), "--sha", ctx.sha))
        ctx.record("compat env", ctx.remote_install(
            "compat-env", "--release", f"{COMPAT_ROOT}/releases/{ctx.sha}",
            "--source", str(RUST_ENV), "--destination", str(COMPAT_ENV)))
    finally:
        ctx.ssh(f"rm -f {shlex.quote(remote_tarball)}")


def install_known_hosts(ctx: Context) -> None:
    if not ctx.args.known_hosts:
        raise StageError("--known-hosts with the pinned compatibility host key is required")
    host = COMPAT_TARGET.split("@", 1)[1]
    found = subprocess.run(["ssh-keygen", "-F", host, "-f", ctx.args.known_hosts],
                           capture_output=True, text=True, timeout=30, check=False)
    lines = [line for line in found.stdout.splitlines() if line and not line.startswith("#")]
    if not lines:
        raise StageError("pinned known_hosts has no key for the compatibility host")
    ctx.record("fleet known_hosts", ctx.local(
        "known-hosts", "--destination", str(FLEET_KNOWN_HOSTS),
        stdin=("\n".join(lines) + "\n").encode()))


def stage_fleet(ctx: Context) -> str:
    package = build(ctx)
    with tempfile.TemporaryDirectory() as directory:
        staged = Path(directory) / Path(package["tarball"]).name
        staged.write_bytes(Path(package["tarball"]).read_bytes())
        os.chmod(directory, 0o755)
        os.chmod(staged, 0o644)
        ctx.record("fleet stage", ctx.local(
            "stage", "--root", str(FLEET_ROOT), "--sha", ctx.sha,
            "--tarball", str(staged), "--digest", package["digest"]))
    return str(FLEET_ROOT / "releases" / ctx.sha)


def activate_fleet(ctx: Context) -> None:
    links = [arg for link in FLEET_LINKS for arg in ("--link", link)]
    ctx.record("fleet activate", ctx.local(
        "activate", "--root", str(FLEET_ROOT), "--sha", ctx.sha, *links))
    ctx.record("fleet unit", ctx.local(
        "ensure-unit", "--unit", str(FLEET_UNIT_PATH),
        "--template", str(FLEET_ROOT / "releases" / ctx.sha / "zakura-fleet-watchdog.service")))


def lane_enabled() -> bool:
    return install.LANE_DROP_IN.exists()


def read_state(ctx: Context) -> dict:
    command = ["cat", str(FLEET_STATE)]
    if os.geteuid() != 0:
        command = ["sudo", "-n", *command]
    result = subprocess.run(command, capture_output=True, timeout=30, check=False)
    try:
        return json.loads(result.stdout) if result.returncode == 0 else {}
    except ValueError:
        return {}


def last_probe(ctx: Context) -> dict:
    record = read_state(ctx).get(monitor.COMPAT_PROBES, {}).get(COMPAT_NAME, {})
    return record.get("last", {})


# Stages.

def stage_fleet_deploy(ctx: Context) -> None:
    """Regular deploys: install the fleet release in place and restart only the watchdog."""
    stage_fleet(ctx)
    if lane_enabled():
        # Do not change the live fleet release until the checker refresh succeeds.
        validate_target(ctx)
        install_compat(ctx)
    activate_fleet(ctx)
    webhook = os.environ.get("SLACK_WEB_HOOK", "")
    ctx.record("fleet env", ctx.local(
        "fleet-env", "--env-file", str(FLEET_ENV), "--webhook-stdin",
        stdin=webhook.encode()))
    ctx.systemctl("daemon-reload")
    ctx.systemctl("enable", FLEET_UNIT)
    ctx.systemctl("restart", FLEET_UNIT)
    ctx.record("fleet service", ctx.systemctl("is-active", FLEET_UNIT, check=False))


def stage_install(ctx: Context) -> None:
    validate_target(ctx)
    install_compat(ctx)
    stage_fleet(ctx)
    install_known_hosts(ctx)


def stage_validate(ctx: Context) -> None:
    validate_target(ctx)
    failures = []
    status = ctx.remote_install("status", "--root", str(COMPAT_ROOT))
    ctx.record("compat release", status)
    if status.get("current") != ctx.sha:
        failures.append("compat current release is not the requested commit")

    release = FLEET_ROOT / "releases" / ctx.sha
    # The service runs as root with root's SSH identity and no agent.
    probe = ["env", "-u", "SSH_AUTH_SOCK", sys.executable, "-I",
             str(release / "zakura-monitoring-acceptance.py"), "probe",
             "--config", str(release / "fleet-watchdog.toml")]
    if os.geteuid() != 0:
        probe = ["sudo", "-n", *probe]
    result = subprocess.run(probe, capture_output=True, timeout=200, check=False)
    ctx.record("service-context probe", json_or_marker(result.stdout))
    if result.returncode != 0:
        failures.append("service-context probe did not pass")

    lane = monitor.load_compatibility_targets(release / "fleet-watchdog.toml")[0]
    code, parity = ctx.remote_json(
        ["python3", "-I", f"{COMPAT_ROOT}/releases/{ctx.sha}/zakura-monitoring-acceptance.py",
         "parity", "--height-max-drift", str(lane.height_max_drift)], timeout=400)
    ctx.record("rust/python parity", parity)
    if code != 0:
        failures.append("Rust/Python parity did not agree on a healthy check")

    for label, runner in (
        ("synthetic us-east-0", lambda: subprocess.run(
            [sys.executable, "-I", str(release / "zakura-monitoring-acceptance.py"),
             "synthetic"], capture_output=True, timeout=1000, check=False)),
        ("synthetic zakura-compat", lambda: ctx.ssh(shlex.join(
            ["python3", "-I", f"{COMPAT_ROOT}/releases/{ctx.sha}/zakura-monitoring-acceptance.py",
             "synthetic"]), timeout=1000)),
    ):
        outcome = runner()
        ctx.record(label, json_or_marker(outcome.stdout))
        if outcome.returncode != 0:
            failures.append(f"{label} failed")
    if failures:
        raise StageError("; ".join(failures))


def stage_cutover(ctx: Context) -> None:
    validate_target(ctx)
    if ctx.remote_install("status", "--root", str(COMPAT_ROOT)).get("current") != ctx.sha:
        raise StageError("install and validate this commit on zakura-compat first")
    if not (FLEET_ROOT / "releases" / ctx.sha).is_dir():
        raise StageError("install this commit's fleet release first")
    ctx.record("state backup", ctx.local(
        "backup-state", "--state", str(FLEET_STATE), "--directory", str(FLEET_BACKUPS)))
    activate_fleet(ctx)
    ctx.record("acceptance begin", ctx.local(
        "acceptance", "--root", str(FLEET_ROOT), "--sha", ctx.sha, "--action", "begin"))
    ctx.record("lane", ctx.local("enable-lane"))
    ctx.systemctl("daemon-reload")
    started = time.time()
    ctx.systemctl("restart", FLEET_UNIT)
    deadline = time.monotonic() + CUTOVER_WAIT_SECONDS
    while time.monotonic() < deadline:
        last = last_probe(ctx)
        if last.get("completed_at", 0) > started:
            ctx.record("first live probe", {
                key: last.get(key) for key in ("valid", "status", "predicate", "details")})
            ctx.record("fleet service", ctx.systemctl("is-active", FLEET_UNIT, check=False))
            if not last.get("valid") or last.get("status") != "pass":
                raise StageError("the first live probe did not pass; consider rollback")
            return
        time.sleep(10)
    raise StageError("no live probe completed after cutover; consider rollback")


def stage_soak(ctx: Context) -> None:
    validate_target(ctx)
    if ctx.args.soak_seconds < install.SOAK_MIN_SECONDS:
        raise StageError("deployment acceptance requires at least a 30-minute soak")
    generation = ctx.local("acceptance", "--root", str(FLEET_ROOT),
                           "--sha", ctx.sha, "--action", "status")["generation"]
    command = [sys.executable, "-I", str(FLEET_ROOT / "current" / "zakura-monitoring-acceptance.py"),
               "soak", "--duration", str(ctx.args.soak_seconds)]
    if os.geteuid() != 0:
        command = ["sudo", "-n", *command]
    result = subprocess.run(command, capture_output=True, timeout=ctx.args.soak_seconds + 300,
                            check=False)
    ctx.record("soak", json_or_marker(result.stdout))
    if result.returncode != 0:
        raise StageError("soak checks did not all pass")
    ctx.record("accepted soak", ctx.local(
        "acceptance", "--root", str(FLEET_ROOT), "--sha", ctx.sha,
        "--action", "soak", "--generation", generation, stdin=result.stdout))


def stage_slack_test(ctx: Context) -> None:
    command = [sys.executable, "-I",
               str(FLEET_ROOT / "releases" / ctx.sha / "zakura-monitoring-acceptance.py"),
               "slack-test", "--confirm-real-slack"]
    if os.geteuid() != 0:
        command = ["sudo", "-n", *command]
    result = subprocess.run(command, capture_output=True, timeout=300, check=False)
    ctx.record("slack test", json_or_marker(result.stdout))
    if result.returncode != 0:
        raise StageError("labeled Slack test messages were not accepted")


def stage_finalize(ctx: Context) -> None:
    validate_target(ctx)
    if not lane_enabled():
        raise StageError("cut over to the compatibility lane before retiring the Rust watchdog")
    for label, release in (
        ("fleet", ctx.local("status", "--root", str(FLEET_ROOT))),
        ("compat", ctx.remote_install("status", "--root", str(COMPAT_ROOT))),
    ):
        ctx.record(f"{label} release", release)
        if release.get("current") != ctx.sha:
            raise StageError(f"{label} active release is not the requested commit")
    last = last_probe(ctx)
    if not (last.get("valid") and last.get("status") == "pass"
            and time.time() - last.get("completed_at", 0) <= FRESH_PROBE_SECONDS):
        raise StageError("the live lane has no fresh passing probe; not retiring the Rust watchdog")
    ctx.record("acceptance proof", ctx.local(
        "acceptance", "--root", str(FLEET_ROOT), "--sha", ctx.sha, "--action", "require"))
    ctx.record("retire rust watchdog", ctx.remote_install(
        "retire-rust", "--backup-root", str(RUST_BACKUPS)))


def stage_rollback(ctx: Context) -> None:
    """Undo cutover and finalize. Each step is attempted and reported independently."""
    errors = []

    def attempt(step, action):
        try:
            ctx.record(step, action())
        except (StageError, OSError, subprocess.SubprocessError) as error:
            errors.append(f"{step}: {error}")
            ctx.record(step, {"error": str(error)})

    attempt("lane", lambda: ctx.local("disable-lane"))
    links = [arg for link in FLEET_LINKS for arg in ("--link", link)]
    attempt("fleet release", lambda: ctx.local("rollback", "--root", str(FLEET_ROOT), *links))
    if ctx.args.restore_state:
        attempt("fleet state", lambda: (
            ctx.systemctl("stop", FLEET_UNIT),
            ctx.local("restore-state", "--backup", ctx.args.restore_state,
                      "--state", str(FLEET_STATE)),
        )[1])
    attempt("fleet service", lambda: (
        ctx.systemctl("daemon-reload"), ctx.systemctl("restart", FLEET_UNIT),
        ctx.systemctl("is-active", FLEET_UNIT, check=False))[2])
    attempt("rust watchdog", lambda: ctx.remote_install(
        "restore-rust", "--backup-root", str(RUST_BACKUPS)))
    attempt("compat release", lambda: ctx.remote_install("rollback", "--root", str(COMPAT_ROOT)))
    if errors:
        raise StageError("; ".join(errors))


def stage_status(ctx: Context) -> None:
    ctx.record("fleet release", ctx.local("status", "--root", str(FLEET_ROOT)))
    ctx.record("fleet lane enabled", lane_enabled())
    ctx.record("fleet service", ctx.systemctl("is-active", FLEET_UNIT, check=False))
    last = last_probe(ctx)
    ctx.record("last probe", {key: last.get(key) for key in
                              ("completed_at", "valid", "status", "predicate", "details")})
    ctx.record("compat release", ctx.remote_install("status", "--root", str(COMPAT_ROOT)))
    rust = ctx.ssh("systemctl is-active zakura-watchdog || true")
    ctx.record("rust watchdog", rust.stdout.decode().strip() or "unknown")


def json_or_marker(stdout: bytes) -> object:
    try:
        return json.loads(stdout)
    except ValueError:
        return {"unparsable_output": True}


STAGES = {
    "fleet-deploy": stage_fleet_deploy,
    "install": stage_install,
    "validate": stage_validate,
    "cutover": stage_cutover,
    "soak": stage_soak,
    "slack-test": stage_slack_test,
    "finalize": stage_finalize,
    "rollback": stage_rollback,
    "status": stage_status,
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("stage", choices=sorted(STAGES))
    parser.add_argument("--sha", required=True, help="full commit SHA of this checkout")
    parser.add_argument("--target", default=COMPAT_TARGET)
    parser.add_argument("--known-hosts", help="pinned known_hosts used by the workflow")
    parser.add_argument("--deploy-config", help="nodes.ci.toml to cross-check the target")
    parser.add_argument("--work", default=tempfile.gettempdir())
    parser.add_argument("--evidence", type=Path)
    parser.add_argument("--run-url", default="")
    parser.add_argument("--soak-seconds", type=int, default=1800)
    parser.add_argument("--restore-state", help="state backup to restore during rollback")
    args = parser.parse_args(argv)
    if not install.SHA.fullmatch(args.sha):
        parser.error("--sha must be a full 40-character commit SHA")

    ctx = Context(args)
    code = 0
    try:
        STAGES[args.stage](ctx)
        ctx.evidence["result"] = "passed"
    except (StageError, install.InstallError, OSError, subprocess.SubprocessError) as error:
        ctx.evidence["result"] = f"failed: {error}"
        print(f"::error::monitoring {args.stage} failed: {error}", flush=True)
        code = 1
    ctx.evidence["finished_at"] = time.time()
    if args.evidence:
        args.evidence.write_text(json.dumps(ctx.evidence, indent=2, sort_keys=True) + "\n")
    return code


if __name__ == "__main__":
    raise SystemExit(main())
