"""TOML configuration for the fork monitor.

`load_config` parses a TOML file into frozen dataclasses and validates every value
(types, numeric ranges, URL schemes, unknown keys, duplicate RPC names) so the
rest of the monitor can trust it. Any problem raises SystemExit naming the
offending key, like the other deploy/ tools. `with_overrides` applies the CLI's
`--db/--host/--port` on top of the file.

Beyond the keys in fork-monitor.testnet.toml, `[[rpc]]` entries also accept
`chaintips_interval`, `peerinfo_interval` and `timeout` (the RpcCollector and
RpcClient knobs), and a few defaults depend on `network`: `[p2p].dns_seeds`
falls back to that network's zcashd seeds, and CipherScan defaults to disabled
off testnet because its default API URL only serves testnet.
"""

from __future__ import annotations

import math
import re
import tomllib
import urllib.parse
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Callable

DEFAULT_DB = "/var/lib/zakura-fork-monitor/monitor.sqlite3"
NETWORKS = ("testnet", "mainnet")
# zcashd chainparams vSeeds, minus the gtank seeds that no longer answer.
DEFAULT_DNS_SEEDS = {
    "testnet": ("dnsseed.testnet.z.cash", "testnet.seeder.zfnd.org"),
    "mainnet": ("dnsseed.z.cash", "dnsseed.str4d.xyz", "mainnet.seeder.zfnd.org"),
}
DEFAULT_CIPHERSCAN_URL = "https://api.testnet.cipherscan.app"
RPC_KINDS = ("zakura", "zebra", "zcashd", "other")
# Caps on list lengths keep a typo'd or generated config from fanning out
# into thousands of collectors or connections.
MAX_RPC_ENDPOINTS = 64
MAX_DNS_SEEDS = 32
MAX_STATIC_PEERS = 1_000
MAX_PATH_LEN = 4_096
MAX_URL_LEN = 2_048

# RPC names become the "rpc:<name>" source key, so keep them short and plain.
_NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")
# Hostnames, IPv4 and bare IPv6 literals; brackets are handled by split_host_port.
_HOST_RE = re.compile(r"[A-Za-z0-9._:-]{1,253}")
_PORT_RE = re.compile(r"[0-9]{1,5}")


@dataclass(frozen=True, slots=True)
class HttpConfig:
    """Dashboard/API listen address (port 0 picks a free port)."""

    host: str = "127.0.0.1"
    port: int = 8093


@dataclass(frozen=True, slots=True)
class ChainConfig:
    """Block-tree sizing: startup backfill depth, in-memory window, settle depth."""

    backfill_blocks: int = 20_000
    memory_window: int = 30_000
    settle_depth: int = 3


@dataclass(frozen=True, slots=True)
class RpcEndpoint:
    """One JSON-RPC vantage point polled by an RpcCollector."""

    name: str
    url: str
    kind: str = "zakura"
    interval: float = 1.0
    fleet: bool = False
    backfill: bool = True
    chaintips_interval: float = 3.0
    peerinfo_interval: float = 120.0
    timeout: float = 10.0

    @property
    def source(self) -> str:
        """The `sources.source` key for this endpoint."""
        return f"rpc:{self.name}"

    @property
    def host(self) -> str:
        """Lowercased hostname or IP of the endpoint URL."""
        return (urllib.parse.urlsplit(self.url).hostname or "").lower()


@dataclass(frozen=True, slots=True)
class P2PConfig:
    """P2P observer settings; `static_peers` entries are "host[:port]" strings."""

    enabled: bool = True
    max_peers: int = 300
    connect_rate: float = 5.0
    poll_interval: float = 15.0
    probe_sample: float = 0.2
    dns_seeds: tuple[str, ...] = DEFAULT_DNS_SEEDS["testnet"]
    static_peers: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class CipherscanConfig:
    """Optional CipherScan orphan importer; `backfill_pages` = pages of 200 fetched at startup."""

    enabled: bool = True
    base_url: str = DEFAULT_CIPHERSCAN_URL
    interval: float = 60.0
    backfill_pages: int = 0


@dataclass(frozen=True, slots=True)
class RetentionConfig:
    """How long sightings, probes and tip changes are kept."""

    days: int = 30

    @property
    def seconds(self) -> float:
        """Retention window in seconds."""
        return self.days * 86_400.0


@dataclass(frozen=True, slots=True)
class Config:
    """Validated monitor configuration."""

    network: str = "testnet"
    db: str = DEFAULT_DB
    http: HttpConfig = field(default_factory=HttpConfig)
    chain: ChainConfig = field(default_factory=ChainConfig)
    rpc: tuple[RpcEndpoint, ...] = ()
    p2p: P2PConfig = field(default_factory=P2PConfig)
    cipherscan: CipherscanConfig = field(default_factory=CipherscanConfig)
    retention: RetentionConfig = field(default_factory=RetentionConfig)

    @property
    def fleet_hosts(self) -> frozenset[str]:
        """Hosts of the RPC endpoints marked `fleet = true`."""
        return frozenset(endpoint.host for endpoint in self.rpc if endpoint.fleet and endpoint.host)


class _Invalid(Exception):
    """A config value failed validation; converted to SystemExit at the API boundary."""


Check = Callable[[Any, str], Any]


def _bool(value: Any, where: str) -> bool:
    """Accept only a TOML boolean."""
    if not isinstance(value, bool):
        raise _Invalid(f"{where} must be true or false, got {value!r}")
    return value


def _int(lo: int, hi: int) -> Check:
    """Build a check for an integer (not a bool) in [lo, hi]."""

    def check(value: Any, where: str) -> int:
        """Validate one integer value."""
        if isinstance(value, bool) or not isinstance(value, int):
            raise _Invalid(f"{where} must be an integer, got {value!r}")
        if not lo <= value <= hi:
            raise _Invalid(f"{where} must be between {lo} and {hi}, got {value}")
        return value

    return check


def _float(lo: float, hi: float) -> Check:
    """Build a check for a finite number in [lo, hi], returned as float."""

    def check(value: Any, where: str) -> float:
        """Validate one numeric value."""
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise _Invalid(f"{where} must be a number, got {value!r}")
        if not lo <= value <= hi:
            raise _Invalid(f"{where} must be between {lo:g} and {hi:g}, got {value:g}")
        return float(value)

    return check


def _str(max_len: int, *, choices: tuple[str, ...] = (), pattern: re.Pattern[str] | None = None) -> Check:
    """Build a check for a non-empty string, optionally restricted to `choices` or `pattern`."""

    def check(value: Any, where: str) -> str:
        """Validate one string value."""
        if not isinstance(value, str) or not value:
            raise _Invalid(f"{where} must be a non-empty string, got {value!r}")
        if len(value) > max_len:
            raise _Invalid(f"{where} must be at most {max_len} characters")
        if choices and value not in choices:
            raise _Invalid(f"{where} must be one of {', '.join(choices)}, got {value!r}")
        if pattern is not None and not pattern.fullmatch(value):
            raise _Invalid(f"{where} has an invalid format: {value!r}")
        return value

    return check


def _url(value: Any, where: str) -> str:
    """Accept an absolute http(s) URL with a host."""
    text = _str(MAX_URL_LEN)(value, where)
    try:
        parts = urllib.parse.urlsplit(text)
        port = parts.port  # urllib only validates the port when it is read
    except ValueError as err:
        raise _Invalid(f"{where} is not a valid URL: {err}") from None
    if parts.scheme not in ("http", "https"):
        raise _Invalid(f"{where} must use http or https, got {text!r}")
    if not parts.hostname or port == 0:
        raise _Invalid(f"{where} must include a host and a non-zero port, got {text!r}")
    return text


def _peer(value: Any, where: str) -> str:
    """Accept a "host[:port]" peer string (checked with split_host_port)."""
    text = _str(260)(value, where)
    try:
        split_host_port(text, 1)
    except ValueError as err:
        raise _Invalid(f"{where}: {err}") from None
    return text


def _list(item: Check, max_items: int) -> Check:
    """Build a check for an array of at most `max_items` items, returned as a tuple."""

    def check(value: Any, where: str) -> tuple[Any, ...]:
        """Validate every item of one array value."""
        if not isinstance(value, list):
            raise _Invalid(f"{where} must be an array, got {value!r}")
        if len(value) > max_items:
            raise _Invalid(f"{where} may list at most {max_items} entries")
        return tuple(item(entry, f"{where}[{index}]") for index, entry in enumerate(value))

    return check


_PORT = _int(0, 65_535)
_HOST = _str(253, pattern=_HOST_RE)
_PATH = _str(MAX_PATH_LEN)

_TOP_KEYS = ("network", "db", "http", "chain", "rpc", "p2p", "cipherscan", "retention")
_HTTP_CHECKS: dict[str, Check] = {"host": _HOST, "port": _PORT}
_CHAIN_CHECKS: dict[str, Check] = {
    "backfill_blocks": _int(0, 2_000_000),
    "memory_window": _int(100, 5_000_000),
    "settle_depth": _int(0, 1_000),
}
_RPC_CHECKS: dict[str, Check] = {
    "name": _str(64, pattern=_NAME_RE),
    "url": _url,
    "kind": _str(16, choices=RPC_KINDS),
    "interval": _float(0.1, 3_600.0),
    "fleet": _bool,
    "backfill": _bool,
    "chaintips_interval": _float(0.1, 3_600.0),
    "peerinfo_interval": _float(1.0, 86_400.0),
    "timeout": _float(0.5, 300.0),
}
_P2P_CHECKS: dict[str, Check] = {
    "enabled": _bool,
    "max_peers": _int(0, 5_000),
    "connect_rate": _float(0.01, 1_000.0),
    "poll_interval": _float(1.0, 3_600.0),
    "probe_sample": _float(0.0, 1.0),
    "dns_seeds": _list(_HOST, MAX_DNS_SEEDS),
    "static_peers": _list(_peer, MAX_STATIC_PEERS),
}
_CIPHERSCAN_CHECKS: dict[str, Check] = {
    "enabled": _bool,
    "base_url": _url,
    "interval": _float(1.0, 86_400.0),
    "backfill_pages": _int(0, 10_000),
}
_RETENTION_CHECKS: dict[str, Check] = {"days": _int(1, 3_650)}


def _table(raw: Any, where: str, checks: dict[str, Check]) -> dict[str, Any]:
    """Validate one TOML table against `checks`, rejecting unknown keys."""
    if not isinstance(raw, dict):
        raise _Invalid(f"{where} must be a table")
    unknown = sorted(set(raw) - set(checks))
    if unknown:
        raise _Invalid(
            f"unknown key(s) in {where}: {', '.join(unknown)} (expected: {', '.join(sorted(checks))})"
        )
    return {key: checks[key](value, f"{where}.{key}") for key, value in raw.items()}


def _rpc_endpoints(raw: Any) -> tuple[RpcEndpoint, ...]:
    """Validate the `[[rpc]]` array: required keys and unique names."""
    if not isinstance(raw, list):
        raise _Invalid("rpc must be an array of tables ([[rpc]])")
    if len(raw) > MAX_RPC_ENDPOINTS:
        raise _Invalid(f"at most {MAX_RPC_ENDPOINTS} [[rpc]] entries are supported")
    endpoints = []
    seen: set[str] = set()
    for index, entry in enumerate(raw):
        values = _table(entry, f"rpc[{index}]", _RPC_CHECKS)
        for required in ("name", "url"):
            if required not in values:
                raise _Invalid(f"rpc[{index}] is missing required key {required!r}")
        if values["name"] in seen:
            raise _Invalid(f"duplicate rpc name: {values['name']}")
        seen.add(values["name"])
        endpoints.append(RpcEndpoint(**values))
    return tuple(endpoints)


def parse_config(data: dict[str, Any], origin: str = "config") -> Config:
    """Validate an already-decoded TOML document; SystemExit names `origin` and the bad key."""
    try:
        return _parse(data)
    except _Invalid as err:
        raise SystemExit(f"invalid config {origin}: {err}") from None


def _parse(data: dict[str, Any]) -> Config:
    """Build a Config from a TOML document, raising _Invalid on the first problem."""
    unknown = sorted(set(data) - set(_TOP_KEYS))
    if unknown:
        raise _Invalid(f"unknown top-level key(s): {', '.join(unknown)} (expected: {', '.join(_TOP_KEYS)})")
    network = _str(16, choices=NETWORKS)(data.get("network", "testnet"), "network")

    p2p_values = _table(data.get("p2p", {}), "p2p", _P2P_CHECKS)
    p2p_values.setdefault("dns_seeds", DEFAULT_DNS_SEEDS[network])
    cipherscan_values = _table(data.get("cipherscan", {}), "cipherscan", _CIPHERSCAN_CHECKS)
    if network != "testnet" and "base_url" not in cipherscan_values:
        cipherscan_values.setdefault("enabled", False)

    config = Config(
        network=network,
        db=_PATH(data.get("db", DEFAULT_DB), "db"),
        http=HttpConfig(**_table(data.get("http", {}), "http", _HTTP_CHECKS)),
        chain=ChainConfig(**_table(data.get("chain", {}), "chain", _CHAIN_CHECKS)),
        rpc=_rpc_endpoints(data.get("rpc", [])),
        p2p=P2PConfig(**p2p_values),
        cipherscan=CipherscanConfig(**cipherscan_values),
        retention=RetentionConfig(**_table(data.get("retention", {}), "retention", _RETENTION_CHECKS)),
    )
    if not config.rpc and not config.p2p.enabled:
        raise _Invalid("nothing to monitor: define at least one [[rpc]] endpoint or enable [p2p]")
    return config


def load_config(path: str | Path) -> Config:
    """Read and validate a TOML config file; SystemExit on any problem."""
    path = Path(path)
    try:
        with path.open("rb") as fh:
            data = tomllib.load(fh)
    except FileNotFoundError:
        raise SystemExit(f"config file not found: {path}") from None
    except OSError as err:
        raise SystemExit(f"cannot read config {path}: {err}") from None
    except tomllib.TOMLDecodeError as err:
        raise SystemExit(f"invalid TOML in {path}: {err}") from None
    return parse_config(data, str(path))


def with_overrides(
    config: Config, *, db: str | None = None, host: str | None = None, port: int | None = None
) -> Config:
    """Return `config` with the CLI's --db/--host/--port applied; None keeps the file's value."""
    try:
        http = config.http
        if host is not None:
            http = replace(http, host=_HOST(host, "--host"))
        if port is not None:
            http = replace(http, port=_PORT(port, "--port"))
        if db is not None:
            config = replace(config, db=_PATH(db, "--db"))
    except _Invalid as err:
        raise SystemExit(f"invalid command-line option: {err}") from None
    return replace(config, http=http)


def split_host_port(value: str, default_port: int) -> tuple[str, int]:
    """Split "host", "host:port", "[v6]" or "[v6]:port" (a bare IPv6 literal is all host).

    Raises ValueError on a malformed host or a port outside 1-65535.
    """
    text = value.strip()
    port_text: str | None = None
    if text.startswith("["):
        end = text.find("]")
        if end < 0:
            raise ValueError(f"unclosed '[' in {value!r}")
        host, rest = text[1:end], text[end + 1 :]
        if rest:
            if not rest.startswith(":"):
                raise ValueError(f"unexpected text after ']' in {value!r}")
            port_text = rest[1:]
    elif text.count(":") == 1:
        host, port_text = text.split(":")
    else:
        host = text
    if not _HOST_RE.fullmatch(host):
        raise ValueError(f"invalid host in {value!r}")
    if port_text is None:
        return host, default_port
    if not _PORT_RE.fullmatch(port_text) or not 1 <= int(port_text) <= 65_535:
        raise ValueError(f"invalid port in {value!r}")
    return host, int(port_text)
