"""Local zcashd-compat sync checker for the zakura-compat host.

One cycle checks, in order:

- a ``zakurad .*--zcashd-compat`` process is running,
- a sidecar ``zcashd .*-connect`` process is running,
- zcashd ``getconnectioncount`` is exactly one (the Zakura node it is pinned to),
- the absolute ``getblockcount`` drift between zakurad and zcashd is within the
  configured maximum.

``probe`` runs one cycle and always prints one JSON outcome (health failures
are valid outcomes). ``check`` retries until a cycle passes or the deadline
expires and exits 0 on pass, 1 on failed verification or deadline, and 2 on
invalid configuration. Neither mode reads Slack configuration, and ``check``
ignores deployment suppression.

Configuration comes from command-line flags, then the environment, then an
optional ``--env-file``, then defaults. Variable names and the cookie, config
file and user/password alternatives match the retired
``deploy/zcashd-compat/sync-check.sh``; an explicitly empty cookie path disables
cookie authentication. Credentials are only used to build the RPC
Authorization header: outcomes and errors report fixed predicates, error
categories and integers, never paths, URLs, response bodies or credentials.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import socket
import ssl
import subprocess
import sys
import time
import urllib.parse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Sequence

from . import suppression as suppression_marker


SCHEMA = "zakura-compat-outcome/1"
CHECK_NAME = "zcashd_compat_sync"
DEFAULT_PROBE_DEADLINE = 100
MAX_RESPONSE_BYTES = 1 << 20
MAX_SECRET_FILE_BYTES = 64 * 1024
MAX_PROCESS_QUERY_SECONDS = 10.0
NONCE = re.compile(r"[A-Za-z0-9_-]{1,64}")

PASS = "pass"
FAIL = "fail"

# Every predicate an outcome may name; ``in_sync`` is the only passing one.
PREDICATES = (
    "zakurad_process",
    "zcashd_process",
    "zcashd_getconnectioncount",
    "peer_pinning",
    "zakura_getblockcount",
    "zcashd_getblockcount",
    "height_drift",
    "deadline",
    "invalid_config",
    "in_sync",
)
ERROR_KINDS = (
    "auth_unavailable",
    "auth_malformed",
    "connection",
    "timeout",
    "http_status",
    "oversized_response",
    "malformed_json",
    "rpc_error",
    "missing_result",
    "invalid_result",
    "invalid_config",
)
DETAIL_KEYS = (
    "zcashd_connections",
    "zakura_height",
    "zcashd_height",
    "height_drift",
    "height_max_drift",
)


class ConfigError(ValueError):
    """Invalid checker configuration. The message names a setting, never a value."""


@dataclass(frozen=True)
class Setting:
    env: str
    default: str
    # Shell `${VAR-default}` semantics: an explicitly empty value is kept.
    empty_is_explicit: bool = False
    secret: bool = False


SETTINGS: dict[str, Setting] = {
    "zakura_rpc_url": Setting("ZAKURA_RPC_URL", "http://127.0.0.1:8232"),
    "zakura_cookie_file": Setting(
        "ZAKURA_COOKIE_FILE", "/root/.cache/zakura/.cookie", empty_is_explicit=True
    ),
    "zakura_rpc_conf": Setting("ZAKURA_RPC_CONF", ""),
    "zakura_rpc_user": Setting("ZAKURA_RPC_USER", "", secret=True),
    "zakura_rpc_password": Setting("ZAKURA_RPC_PASSWORD", "", secret=True),
    "zcashd_rpc_url": Setting("ZCASHD_RPC_URL", "http://[::1]:8232"),
    "zcashd_cookie_file": Setting(
        "ZCASHD_COOKIE_FILE", "/mnt/data/runtime/zcashd/.cookie", empty_is_explicit=True
    ),
    "zcashd_rpc_conf": Setting("ZCASHD_RPC_CONF", ""),
    "zcashd_rpc_user": Setting("ZCASHD_RPC_USER", "", secret=True),
    "zcashd_rpc_password": Setting("ZCASHD_RPC_PASSWORD", "", secret=True),
    "zakurad_process_pattern": Setting("ZAKURAD_PROCESS_PATTERN", "zakurad .*--zcashd-compat"),
    "zcashd_process_pattern": Setting("ZCASHD_PROCESS_PATTERN", "zcashd .*-connect"),
    # 30 blocks is about 12 minutes at NU7's 25-second spacing, the 10 blocks it was before.
    "height_max_drift": Setting("HEIGHT_MAX_DRIFT", "30"),
    "sync_check_timeout": Setting("SYNC_CHECK_TIMEOUT", "600"),
    "sync_check_interval": Setting("SYNC_CHECK_INTERVAL", "15"),
    "rpc_timeout": Setting("WATCHDOG_RPC_TIMEOUT", "30"),
    "deployment_suppression_file": Setting(
        "WATCHDOG_DEPLOYMENT_SUPPRESSION_FILE",
        str(suppression_marker.COMPAT_SUPPRESSION_FILE),
    ),
    "max_deployment_suppression": Setting(
        "WATCHDOG_MAX_DEPLOYMENT_SUPPRESSION",
        str(suppression_marker.COMPAT_MAX_SUPPRESSION_SECONDS),
    ),
}
ENV_KEYS = frozenset(setting.env for setting in SETTINGS.values())
UINT_SETTINGS = (
    "height_max_drift",
    "sync_check_timeout",
    "sync_check_interval",
    "rpc_timeout",
    "max_deployment_suppression",
)


@dataclass(frozen=True)
class Endpoint:
    """One JSON-RPC endpoint and the credentials sources it may use."""

    name: str
    url: str
    cookie_file: str
    conf_file: str
    user: str = field(repr=False)
    password: str = field(repr=False)


@dataclass(frozen=True)
class CompatConfig:
    zakura: Endpoint
    zcashd: Endpoint
    zakurad_process_pattern: str
    zcashd_process_pattern: str
    height_max_drift: int
    sync_check_timeout: int
    sync_check_interval: int
    rpc_timeout: int
    deployment_suppression_file: Path
    max_deployment_suppression: int


def read_env_file(path: Path) -> dict[str, str]:
    """Read whitelisted KEY=VALUE lines; other keys (for example SENTRY_DSN) are ignored."""
    try:
        with path.open("rb") as env_file:
            raw = env_file.read(MAX_SECRET_FILE_BYTES + 1)
    except FileNotFoundError:
        return {}
    except OSError as error:
        raise ConfigError("environment file is unreadable") from error
    if len(raw) > MAX_SECRET_FILE_BYTES:
        raise ConfigError("environment file is too large")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ConfigError("environment file is not UTF-8") from error

    values = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key.startswith("export "):
            key = key.removeprefix("export ").strip()
        if key not in ENV_KEYS:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key] = value
    return values


def resolve_settings(
    cli: dict[str, str | None],
    environ: dict[str, str],
    env_file: dict[str, str],
) -> dict[str, str]:
    resolved = {}
    for name, setting in SETTINGS.items():
        value = cli.get(name)
        if value is None:
            for source in (environ, env_file):
                if setting.env in source:
                    candidate = source[setting.env]
                    if candidate or setting.empty_is_explicit:
                        value = candidate
                        break
        resolved[name] = setting.default if value is None else value
    return resolved


def require_uint(name: str, value: str) -> int:
    if not re.fullmatch(r"[0-9]+", value or ""):
        raise ConfigError(f"{SETTINGS[name].env} must be a non-negative integer")
    return int(value)


def require_url(name: str, value: str) -> str:
    try:
        parsed = urllib.parse.urlsplit(value)
        parsed.port
    except ValueError as error:
        raise ConfigError(f"{SETTINGS[name].env} is not a valid URL") from error
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ConfigError(f"{SETTINGS[name].env} must be an http or https URL")
    if parsed.username is not None or parsed.password is not None:
        raise ConfigError(f"{SETTINGS[name].env} must not embed credentials")
    if any(ord(char) <= 32 or ord(char) == 127 for char in value):
        raise ConfigError(f"{SETTINGS[name].env} must not contain spaces or control characters")
    return value


def require_pattern(name: str, value: str) -> str:
    if not value:
        raise ConfigError(f"{SETTINGS[name].env} must not be empty")
    try:
        re.compile(value)
    except re.error as error:
        raise ConfigError(f"{SETTINGS[name].env} is not a valid pattern") from error
    return value


def build_config(values: dict[str, str]) -> CompatConfig:
    uints = {name: require_uint(name, values[name]) for name in UINT_SETTINGS}
    if uints["rpc_timeout"] < 1:
        raise ConfigError("WATCHDOG_RPC_TIMEOUT must be at least 1")
    if uints["max_deployment_suppression"] < 1:
        raise ConfigError("WATCHDOG_MAX_DEPLOYMENT_SUPPRESSION must be at least 1")
    if not values["deployment_suppression_file"]:
        raise ConfigError("WATCHDOG_DEPLOYMENT_SUPPRESSION_FILE must not be empty")

    def endpoint(prefix: str, name: str) -> Endpoint:
        return Endpoint(
            name=name,
            url=require_url(f"{prefix}_rpc_url", values[f"{prefix}_rpc_url"]),
            cookie_file=values[f"{prefix}_cookie_file"],
            conf_file=values[f"{prefix}_rpc_conf"],
            user=values[f"{prefix}_rpc_user"],
            password=values[f"{prefix}_rpc_password"],
        )

    return CompatConfig(
        zakura=endpoint("zakura", "zakurad"),
        zcashd=endpoint("zcashd", "zcashd"),
        zakurad_process_pattern=require_pattern(
            "zakurad_process_pattern", values["zakurad_process_pattern"]
        ),
        zcashd_process_pattern=require_pattern(
            "zcashd_process_pattern", values["zcashd_process_pattern"]
        ),
        deployment_suppression_file=Path(values["deployment_suppression_file"]),
        **uints,
    )


class RpcFailure(Exception):
    """An RPC predicate failed; ``kind`` is a fixed category from ERROR_KINDS."""

    def __init__(self, kind: str):
        super().__init__(kind)
        self.kind = kind


def read_small_file(path: str) -> str:
    with open(path, "rb") as secret_file:
        raw = secret_file.read(MAX_SECRET_FILE_BYTES + 1)
    if len(raw) > MAX_SECRET_FILE_BYTES:
        raise ValueError("file too large")
    return raw.decode("utf-8")


def conf_value(text: str, wanted: str) -> str:
    """Return the first ``key=value`` for ``wanted``, like the canonical shell check."""
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key.strip() == wanted:
            return value.strip()
    return ""


def authorization(endpoint: Endpoint) -> str | None:
    """Resolve the Basic credentials: cookie, else config file and user/password."""
    user, password = endpoint.user, endpoint.password
    if endpoint.cookie_file:
        if not os.path.isfile(endpoint.cookie_file):
            raise RpcFailure("auth_unavailable")
        try:
            cookie = read_small_file(endpoint.cookie_file).strip()
        except (OSError, ValueError):
            raise RpcFailure("auth_unavailable") from None
        if ":" not in cookie:
            raise RpcFailure("auth_malformed")
        credentials = cookie
    else:
        if endpoint.conf_file:
            if not os.path.isfile(endpoint.conf_file):
                raise RpcFailure("auth_unavailable")
            try:
                conf = read_small_file(endpoint.conf_file)
            except (OSError, ValueError):
                raise RpcFailure("auth_unavailable") from None
            user = user or conf_value(conf, "rpcuser")
            password = password or conf_value(conf, "rpcpassword")
        if not user and not password:
            return None
        credentials = f"{user}:{password}"
    return "Basic " + base64.b64encode(credentials.encode("utf-8")).decode("ascii")


def http_post(
    parsed: urllib.parse.SplitResult,
    path: str,
    headers: dict[str, str],
    body: bytes,
    left: Callable[[], float],
) -> tuple[int, bytes]:
    """POST over HTTP/1.0 and read to EOF, re-arming the socket timeout per read.

    HTTP/1.0 rules out chunked responses, and refreshing the timeout from
    ``left()`` before every operation bounds the whole exchange, which a
    per-operation socket timeout alone would not.
    """
    host = parsed.hostname or ""
    default_port = 443 if parsed.scheme == "https" else 80
    port = parsed.port or default_port
    sock: socket.socket = socket.create_connection((host, port), timeout=left())
    try:
        if parsed.scheme == "https":
            sock = ssl.create_default_context().wrap_socket(sock, server_hostname=host)
        host_header = f"[{host}]" if ":" in host else host
        if parsed.port:
            host_header += f":{parsed.port}"
        lines = [f"POST {path} HTTP/1.0", f"Host: {host_header}"]
        lines += [f"{key}: {value}" for key, value in headers.items()]
        lines.append(f"Content-Length: {len(body)}")
        request = ("\r\n".join(lines) + "\r\n\r\n").encode("latin-1") + body
        sock.settimeout(left())
        sock.sendall(request)
        response = bytearray()
        while True:
            sock.settimeout(left())
            chunk = sock.recv(65536)
            if not chunk:
                break
            response.extend(chunk)
            if len(response) > MAX_RESPONSE_BYTES + MAX_SECRET_FILE_BYTES:
                raise RpcFailure("oversized_response")
    finally:
        sock.close()

    head, separator, payload = bytes(response).partition(b"\r\n\r\n")
    status_line = head.split(b"\r\n", 1)[0].split()
    if not separator or len(status_line) < 2 or not status_line[0].startswith(b"HTTP/"):
        raise ValueError("malformed HTTP response")
    if len(payload) > MAX_RESPONSE_BYTES:
        raise RpcFailure("oversized_response")
    return int(status_line[1]), payload


def json_rpc(endpoint: Endpoint, method: str, deadline: float, rpc_timeout: float) -> int:
    """Call ``method`` and return its non-negative integer result.

    The call ends by ``min(now + rpc_timeout, deadline)`` regardless of how
    slowly the server trickles bytes.
    """
    call_deadline = min(deadline, time.monotonic() + rpc_timeout)

    def left() -> float:
        remaining = call_deadline - time.monotonic()
        if remaining <= 0:
            raise RpcFailure("timeout")
        return remaining

    left()
    auth = authorization(endpoint)
    parsed = urllib.parse.urlsplit(endpoint.url)
    path = parsed.path or "/"
    if parsed.query:
        path += "?" + parsed.query
    body = json.dumps(
        {"jsonrpc": "1.0", "id": "sync-check", "method": method, "params": []}
    ).encode("ascii")
    headers = {"Content-Type": "application/json", "Connection": "close"}
    if auth is not None:
        headers["Authorization"] = auth

    try:
        status, payload = http_post(parsed, path, headers, body, left)
    except RpcFailure:
        raise
    except (socket.timeout, TimeoutError):
        raise RpcFailure("timeout") from None
    except (OSError, ValueError):
        raise RpcFailure("connection") from None
    # Match `curl --fail`: an HTTP error status fails the predicate.
    if status >= 400:
        raise RpcFailure("http_status")

    try:
        data = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        raise RpcFailure("malformed_json") from None
    if not isinstance(data, dict):
        raise RpcFailure("malformed_json")
    if data.get("error") is not None:
        raise RpcFailure("rpc_error")
    if "result" not in data:
        raise RpcFailure("missing_result")
    result = data["result"]
    # JSON booleans are Python ints; neither they nor floats are block counts.
    if type(result) is not int or result < 0:
        raise RpcFailure("invalid_result")
    return result


def own_ancestry() -> set[int]:
    """This process and its ancestors, which may carry a pattern in their arguments."""
    pids = set()
    pid = os.getpid()
    while pid > 1 and pid not in pids:
        pids.add(pid)
        try:
            stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8", errors="replace")
            pid = int(stat.rsplit(")", 1)[1].split()[1])
        except (OSError, ValueError, IndexError):
            break
    pids.add(os.getppid())
    return pids


def process_running(pattern: str, timeout: float) -> bool:
    """Whether ``pgrep -f pattern`` matches a process other than this checker."""
    try:
        result = subprocess.run(
            ["pgrep", "-f", pattern],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=max(0.1, timeout),
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    if result.returncode != 0:
        return False
    pids = {int(token) for token in result.stdout.split() if token.isdigit()}
    return bool(pids - own_ancestry())


@dataclass(frozen=True)
class Outcome:
    """A typed, credential-free result of one check cycle."""

    status: str
    predicate: str
    summary: str
    details: dict[str, int]
    observed_at: float
    error_kind: str | None = None
    suppression: dict[str, object] | None = None

    @property
    def passed(self) -> bool:
        return self.status == PASS

    def as_dict(self, nonce: str | None = None) -> dict[str, object]:
        return {
            "schema": SCHEMA,
            "check": CHECK_NAME,
            "status": self.status,
            "predicate": self.predicate,
            "summary": self.summary,
            "details": dict(sorted(self.details.items())),
            "error_kind": self.error_kind,
            "observed_at": self.observed_at,
            "nonce": nonce,
            "suppression": self.suppression,
        }


def run_cycle(
    config: CompatConfig,
    deadline: float,
    process_query: Callable[[str, float], bool] = process_running,
    rpc: Callable[[Endpoint, str, float, float], int] = json_rpc,
    clock: Callable[[], float] = time.time,
) -> Outcome:
    """Run every predicate once, stopping at the first failure."""
    details = {"height_max_drift": config.height_max_drift}

    def fail(predicate: str, summary: str, error_kind: str | None = None) -> Outcome:
        return Outcome(FAIL, predicate, summary, details, clock(), error_kind)

    def remaining() -> float:
        return deadline - time.monotonic()

    for predicate, pattern, name in (
        ("zakurad_process", config.zakurad_process_pattern, "zakurad"),
        ("zcashd_process", config.zcashd_process_pattern, "zcashd"),
    ):
        if remaining() <= 0:
            return fail("deadline", "check deadline expired before the cycle completed", "timeout")
        if not process_query(pattern, min(MAX_PROCESS_QUERY_SECONDS, remaining())):
            return fail(predicate, f"{name} process is not running")

    try:
        peers = rpc(config.zcashd, "getconnectioncount", deadline, config.rpc_timeout)
    except RpcFailure as failure:
        return fail(
            "zcashd_getconnectioncount", "zcashd getconnectioncount RPC failed", failure.kind
        )
    details["zcashd_connections"] = peers
    if peers != 1:
        return fail(
            "peer_pinning",
            f"sidecar zcashd must peer with exactly one Zakura node, got {peers}",
        )

    try:
        zakura_height = rpc(config.zakura, "getblockcount", deadline, config.rpc_timeout)
    except RpcFailure as failure:
        return fail("zakura_getblockcount", "zakurad getblockcount RPC failed", failure.kind)
    try:
        zcashd_height = rpc(config.zcashd, "getblockcount", deadline, config.rpc_timeout)
    except RpcFailure as failure:
        return fail("zcashd_getblockcount", "zcashd getblockcount RPC failed", failure.kind)

    drift = abs(zakura_height - zcashd_height)
    details.update(zakura_height=zakura_height, zcashd_height=zcashd_height, height_drift=drift)
    if drift > config.height_max_drift:
        return fail(
            "height_drift", f"height drift {drift} exceeds maximum {config.height_max_drift}"
        )
    return Outcome(
        PASS,
        "in_sync",
        f"in sync: zakurad={zakura_height} zcashd={zcashd_height} drift={drift}",
        details,
        clock(),
    )


def check(
    config: CompatConfig,
    cycle: Callable[[CompatConfig, float], Outcome] = run_cycle,
    sleep: Callable[[float], None] = time.sleep,
    out=None,
    err=None,
) -> int:
    """Retry cycles until one passes or ``sync_check_timeout`` expires."""
    out = out or sys.stdout
    err = err or sys.stderr
    started = time.monotonic()
    deadline = started + config.sync_check_timeout
    attempt = 0
    while True:
        attempt += 1
        outcome = cycle(config, deadline)
        print(
            f"attempt {attempt}: {outcome.status} [{outcome.predicate}] {outcome.summary}"
            + (f" ({outcome.error_kind})" if outcome.error_kind else ""),
            file=out,
            flush=True,
        )
        if outcome.passed:
            print("zcashd-compat sync check passed", file=out, flush=True)
            return 0
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            elapsed = int(time.monotonic() - started)
            print(f"zcashd-compat sync check timed out after {elapsed}s", file=err, flush=True)
            return 1
        delay = min(config.sync_check_interval, remaining)
        print(f"Retrying in {delay:.0f}s...", file=out, flush=True)
        sleep(delay)
        if time.monotonic() >= deadline:
            elapsed = int(time.monotonic() - started)
            print(f"zcashd-compat sync check timed out after {elapsed}s", file=err, flush=True)
            return 1


def release_info() -> dict[str, object]:
    """The installed release manifest summary, if this package came from an installer."""
    manifest = Path(__file__).resolve().parent.parent / "MANIFEST.json"
    try:
        data = json.loads(manifest.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"sha": None, "installed": False}
    return {"sha": data.get("sha"), "installed": True}


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="zakura-compat-check",
        description="Check zakurad/zcashd-compat process, peer and height health.",
    )
    parser.add_argument("mode", choices=("probe", "check", "version"))
    parser.add_argument("--env-file", type=Path, help="optional KEY=VALUE configuration file")
    parser.add_argument("--nonce", help="probe identifier echoed in the outcome")
    parser.add_argument(
        "--deadline",
        default=str(DEFAULT_PROBE_DEADLINE),
        help="probe deadline in seconds (the caller's hard timeout must be longer)",
    )
    for name, setting in SETTINGS.items():
        if setting.secret:
            # Credentials stay out of argv, where other local users could read them.
            continue
        parser.add_argument("--" + name.replace("_", "-"), dest=name, default=None)
    return parser.parse_args(argv)


def load_config(args: argparse.Namespace, environ: dict[str, str]) -> CompatConfig:
    env_file = read_env_file(args.env_file) if args.env_file else {}
    cli = {name: getattr(args, name, None) for name in SETTINGS}
    return build_config(resolve_settings(cli, environ, env_file))


def emit(outcome: dict[str, object]) -> None:
    print(json.dumps(outcome, sort_keys=True, separators=(",", ":")), flush=True)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.mode == "version":
        emit({"schema": SCHEMA, **release_info()})
        return 0
    if args.nonce is not None and not NONCE.fullmatch(args.nonce):
        print("invalid --nonce", file=sys.stderr)
        return 2

    try:
        config = load_config(args, dict(os.environ))
        probe_deadline = int(args.deadline) if args.deadline.isdigit() else 0
        if args.mode == "probe" and probe_deadline < 1:
            raise ConfigError("--deadline must be a positive integer")
    except ConfigError as error:
        if args.mode == "check":
            print(f"invalid configuration: {error}", file=sys.stderr)
            return 2
        emit(Outcome(FAIL, "invalid_config", str(error), {}, time.time(), "invalid_config").as_dict(args.nonce))
        return 2

    if args.mode == "check":
        return check(config)

    outcome = run_cycle(config, time.monotonic() + probe_deadline)
    marker = suppression_marker.compat_suppression(
        config.deployment_suppression_file, time.time(), config.max_deployment_suppression
    )
    emit({**outcome.as_dict(args.nonce), "suppression": marker.as_dict()})
    return 0
