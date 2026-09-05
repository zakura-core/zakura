"""Check provenance, arithmetic invariants, and selected exact replays."""

import csv
import hashlib
import json
from pathlib import Path
import sys

from sim import Config, Simulation, WIRE


def main():
    result = Path(sys.argv[1])
    source = Path(__file__).resolve().parent
    environment = json.loads((result / "environment.json").read_text())
    for name, digest in environment["source_sha256"].items():
        assert hashlib.sha256((source / name).read_bytes()).hexdigest() == digest, name
    for name, digest in json.loads((result / "SHA256SUMS.json").read_text()).items():
        assert hashlib.sha256((result / name).read_bytes()).hexdigest() == digest, name
    rows = list(csv.DictReader((result / "codec.csv").open()))
    matched = {}
    for row in rows:
        if row["trace"] in ("insufficient", "invalid_codeword"):
            assert row["valid"] == "0"
        else:
            assert row["valid"] == "1"
            assert row["rank"] == row["k"]
            if row["codec"] == "rs16":
                assert row["arrivals"] == row["k"]
        assert float(row["tail_ms"]) >= 0
        assert 0 <= float(row["elimination_before_last_ms"]) <= float(row["elimination_ms"]) + .001
        key = tuple(row[k] for k in ("codec", "k", "n", "trace", "assemblies", "seed", "gap_ms"))
        matched.setdefault(key, {})[row["schedule"]] = row
    for group in matched.values():
        batch, online = group["batch"], group["online"]
        assert float(online["tail_ms"]) <= float(batch["tail_ms"]) + .001
        if batch["gap_ms"] == "0":
            assert abs(float(batch["tail_ms"]) - float(online["tail_ms"])) < .001
    routing = [json.loads(line) for line in (result / "routing.jsonl").read_text().splitlines()]
    sweeps = [json.loads(line) for line in (result / "sweeps.jsonl").read_text().splitlines()]
    for row in routing + sweeps:
        assert row["exploration_charged"] <= row["exploration_bound"]
        assert row["peak_link_queue_bytes"] <= row["config"]["queue_parts"] * WIRE
        assert row["completed"] <= row["total"]
        assert row["supplier_wire_to_body"] >= row["wire_to_body"] - 1e-9
        assert (row["p95_ms"] is None) == (row["completed"] == 0)
    for row in routing:
        if row["seed"] == 0 and row["policy"] == "budgeted":
            replay = Simulation(Config(**row["config"]), row["policy"], row["seed"]).run()
            # JSON normalizes dataclass tuples and movement tuples to lists.
            assert json.loads(json.dumps(replay)) == row, row["scenario"]
    graphs = json.loads((result / "graph.json").read_text())
    for row in graphs:
        if row["repair"] == "header_tree":
            assert row["completed"] == row["nodes"]
    if not environment["quick"]:
        assert (len(rows), len(routing), len(sweeps), len(graphs)) == (1368, 1080, 976, 3840)
    print(f"Verified source/result hashes, {len(rows)} codec rows, {len(routing) + len(sweeps)} routing runs, "
          f"{len(graphs)} graph closures, and selected exact replays.")


if __name__ == "__main__":
    main()
