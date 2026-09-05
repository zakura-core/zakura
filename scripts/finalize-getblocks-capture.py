#!/usr/bin/env python3
"""Save the final metrics boundary after the capture controller stops its clients."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from urllib.request import urlopen

from getblocks_boundary import await_quiescence
from getblocks_lifetimes import validate_stopped_clients


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run", type=Path, help="run directory containing the verified clients-stopped.json declaration")
    parser.add_argument("--metrics-url", default="http://127.0.0.1:19999/metrics")
    parser.add_argument("--timeout", type=float, default=240,
                        help="seconds to await drained sessions and owners (default: 240)")
    args = parser.parse_args()
    try:
        clients = (args.run / "clients-stopped.json").read_bytes()
        validate_stopped_clients(clients)
        metrics_path, boundary_path = args.run / "final-metrics.prom", args.run / "capture-boundary.json"
        if metrics_path.exists() or boundary_path.exists():
            raise FileExistsError("preserve the existing capture boundary")

        def scrape():
            with urlopen(args.metrics_url, timeout=3) as response:
                raw = response.read(16 * 1024 * 1024 + 1)
            if len(raw) > 16 * 1024 * 1024:
                raise ValueError("metrics response exceeds the capture size limit")
            return raw

        metrics, report = await_quiescence(scrape, args.timeout)
        report.update({
            "schema_version": 1,
            "observed_utc": datetime.now(timezone.utc).isoformat(),
            "clients_stopped_sha256": hashlib.sha256(clients).hexdigest(),
            "metrics_sha256": hashlib.sha256(metrics).hexdigest(),
        })
        with metrics_path.open("xb") as output:
            output.write(metrics)
        with boundary_path.open("x") as output:
            json.dump(report, output, indent=2)
            output.write("\n")
        print("Boundary saved. Keep clients disconnected, stop the server, then import its closed trace.")
    except (OSError, ValueError) as error:
        parser.exit(1, f"capture finalization failed: {error}\n")


if __name__ == "__main__":
    main()
