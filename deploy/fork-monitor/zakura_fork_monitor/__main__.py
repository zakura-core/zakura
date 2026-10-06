"""Command line: `python3 -m zakura_fork_monitor {run,backfill,report,probe-peers} --config FILE [options]`.

- `run`: the long-running service (dashboard, collectors, P2P observer).
- `backfill --blocks N`: load the last N canonical blocks over RPC and exit.
- `report`: print a text summary of the database to stdout.
- `probe-peers --once`: refresh the tip and peer list over RPC, survey the
  P2P peers once and print the implementation groups (it writes what it
  learns to the database, so point `--db` elsewhere while `run` is active).
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import json
import logging
import sys
import time
from collections.abc import Sequence
from typing import Any

from . import __version__, analysis
from .config import Config, load_config, with_overrides
from .consensus import NETWORKS
from .service import Monitor, load_chain
from .store import Store

log = logging.getLogger("zakura_fork_monitor")

LOG_FORMAT = "%(asctime)s.%(msecs)03dZ %(levelname)s %(name)s: %(message)s"
LOG_DATEFMT = "%Y-%m-%dT%H:%M:%S"
MAX_BACKFILL = 2_000_000


def _count(low: int, high: int) -> Any:
    """Build an argparse type accepting an integer in [low, high]."""

    def parse(text: str) -> int:
        """Parse one bounded integer option."""
        try:
            value = int(text)
        except ValueError:
            raise argparse.ArgumentTypeError(f"expected an integer, got {text!r}") from None
        if not low <= value <= high:
            raise argparse.ArgumentTypeError(f"must be between {low} and {high}")
        return value

    return parse


def build_parser() -> argparse.ArgumentParser:
    """Return the argument parser; every subcommand takes the shared --config/--db/--log-level options."""
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--config", required=True, help="TOML config file (see fork-monitor.testnet.toml)")
    common.add_argument("--db", help="SQLite database path (overrides the config's db)")
    common.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR"))

    parser = argparse.ArgumentParser(prog="python3 -m zakura_fork_monitor", description=__doc__.splitlines()[0])
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    commands = parser.add_subparsers(dest="command", required=True)

    run = commands.add_parser("run", parents=[common], help="run the monitor service and dashboard")
    run.add_argument("--host", help="dashboard listen address (overrides [http].host)")
    run.add_argument("--port", type=_count(0, 65_535), help="dashboard port (overrides [http].port)")
    run.add_argument("--no-p2p", action="store_true", help="disable the P2P observer")
    run.add_argument("--no-cipherscan", action="store_true", help="disable the CipherScan importer")
    run.add_argument("--backfill-blocks", type=_count(0, MAX_BACKFILL),
                     help="canonical blocks to backfill at startup (overrides [chain].backfill_blocks)")

    backfill = commands.add_parser("backfill", parents=[common], help="backfill canonical blocks over RPC and exit")
    backfill.add_argument("--blocks", type=_count(1, MAX_BACKFILL),
                          help="how many blocks (default: [chain].backfill_blocks)")

    report = commands.add_parser("report", parents=[common], help="print a text summary of the database")
    report.add_argument("--forks", type=_count(0, 200), default=10, help="recent fork events to list")

    probe = commands.add_parser("probe-peers", parents=[common], help="survey the P2P peers once and print groups")
    probe.add_argument("--once", action="store_true", help="accepted for clarity; one sweep is the only mode")
    probe.add_argument("--limit", type=_count(1, 5_000), default=200, help="peers to dial")
    probe.add_argument("--json", action="store_true", help="print the per-peer results as JSON")
    return parser


def build_config(args: argparse.Namespace) -> Config:
    """Load the config file and apply the command-line overrides; SystemExit on a bad value."""
    config = load_config(args.config)
    config = with_overrides(config, db=args.db, host=getattr(args, "host", None), port=getattr(args, "port", None))
    if getattr(args, "no_p2p", False):
        config = dataclasses.replace(config, p2p=dataclasses.replace(config.p2p, enabled=False))
    if getattr(args, "no_cipherscan", False):
        config = dataclasses.replace(config, cipherscan=dataclasses.replace(config.cipherscan, enabled=False))
    blocks = getattr(args, "backfill_blocks", None)
    if blocks is not None:
        config = dataclasses.replace(config, chain=dataclasses.replace(config.chain, backfill_blocks=blocks))
    if not config.rpc and not config.p2p.enabled:
        raise SystemExit("nothing to monitor: no [[rpc]] endpoints and the P2P observer is disabled")
    return config


def cmd_run(config: Config) -> int:
    """Run the service until SIGINT/SIGTERM."""
    with Store(config.db) as store:
        monitor = Monitor(config, store)
        try:
            asyncio.run(monitor.run())
        except OSError as err:
            raise SystemExit(f"cannot start: {err}") from None
    return 0


def cmd_backfill(config: Config, blocks: int | None) -> int:
    """Backfill and print the result."""
    with Store(config.db) as store:
        monitor = Monitor(config, store)
        result = asyncio.run(monitor.backfill(blocks or config.chain.backfill_blocks))
        monitor.persist_best()
    if result is None:
        print("backfill failed: no [[rpc]] endpoint with backfill = true answered", file=sys.stderr)
        return 1
    print(
        f"backfill from {result['source']}: tip {result['tip_height']}, {result['fetched']} fetched, "
        f"{result['skipped']} already held, {result['failed']} failed in {result['elapsed']:.1f}s"
    )
    return 0


def cmd_report(config: Config, forks: int) -> int:
    """Print the text report for the database."""
    with Store(config.db) as store:
        chain = load_chain(
            store, NETWORKS[config.network], window=config.chain.memory_window, settle_depth=config.chain.settle_depth
        )
        sys.stdout.write(analysis.text_report(chain, store.reader(), forks=forks, config=config))
    return 0


def cmd_probe_peers(config: Config, limit: int, as_json: bool) -> int:
    """Sweep the P2P peers once and print the groups (or the raw results as JSON)."""
    with Store(config.db) as store:
        monitor = Monitor(config, store)
        results = asyncio.run(monitor.probe_peers(limit))
        if as_json:
            print(json.dumps(results, indent=2, sort_keys=True, default=str))
            return 0
        snap = analysis.live_snapshot(monitor, time.time())
    print(_probe_text(results, snap))
    return 0


def _probe_text(results: Sequence[dict[str, Any]], snap: dict[str, Any]) -> str:
    """Render the sweep: one line per implementation group, then one per peer."""
    tip = snap.get("tip") or {}
    answered = sum(1 for r in results if r.get("error") is None)
    lines = [f"best tip {tip.get('height')} {tip.get('hash')}; {answered} of {len(results)} peers answered", ""]
    lines.append(
        f"{'group':<16} {'active':>6} {'synced':>6} {'lag':>4} {'fork':>4} {'stuck':>5} {'old':>4} {'unk':>4}  branch"
    )
    for group in snap.get("groups", ()):
        states = group["states"]
        branch = group.get("branch")
        old_rules = group.get("old_rules_relation")
        if not branch:
            where = f"old-rules@{old_rules.get('fork_height')}" if old_rules else "-"
        else:
            where = "canonical" if branch["key"] == "canonical" else f"fork@{branch['fork_height']}"
        lines.append(
            f"{_clip(group['key'], 16):<16} {group['active']:>6} {states['synced']:>6} {states['lagging']:>4} "
            f"{states['fork']:>4} {states['stuck']:>5} {states.get('old-rules', 0):>4} {states['unknown']:>4}  {where}"
        )
    lines += ["", f"{'peer':<28} {'impl':<18} {'tip':>9} {'relation':<10} note"]
    order = sorted(results, key=lambda r: (r.get("error") is not None, r.get("impl") or "", r.get("source") or ""))
    for result in order:
        relation = result.get("relation") or {}
        impl = f"{result.get('impl') or '?'} {result.get('version') or ''}".strip()
        note = result.get("error") or result.get("tip_note") or ""
        lines.append(
            f"{_clip(result.get('source'), 28):<28} {_clip(impl, 18):<18} {str(result.get('tip_height') or '-'):>9} "
            f"{_clip(relation.get('kind') or '-', 10):<10} {_clip(note, 60)}"
        )
    return "\n".join(lines)


def _clip(value: Any, width: int) -> str:
    """Stringify and truncate a (possibly remote) value for a table cell."""
    text = "".join(ch if ch.isprintable() else "?" for ch in str(value if value is not None else "-"))
    return text if len(text) <= width else text[: width - 1] + "~"


def main(argv: Sequence[str] | None = None) -> int:
    """Parse arguments, configure logging and dispatch the subcommand."""
    args = build_parser().parse_args(argv)
    handler = logging.StreamHandler()
    formatter = logging.Formatter(LOG_FORMAT, LOG_DATEFMT)
    formatter.converter = time.gmtime  # UTC, like every timestamp the monitor stores
    handler.setFormatter(formatter)
    logging.basicConfig(level=args.log_level, handlers=[handler])
    config = build_config(args)
    if args.command == "run":
        return cmd_run(config)
    if args.command == "backfill":
        return cmd_backfill(config, args.blocks)
    if args.command == "report":
        return cmd_report(config, args.forks)
    return cmd_probe_peers(config, args.limit, args.json)


if __name__ == "__main__":
    sys.exit(main())
