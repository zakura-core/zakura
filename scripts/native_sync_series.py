"""Assess every planned native pair, retaining unavailable or failed trials."""
import copy
import hashlib
import json
import math
from pathlib import Path
import re
import statistics
import tomllib


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def read(path):
    return json.loads(path.read_text())


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}", value):
        raise ValueError("invalid run or host identifier")
    return value


def load_spec(root, witness):
    name = witness["file"]
    if Path(name).name != name or not name.endswith(".json"):
        raise ValueError("specification must be a local JSON basename")
    path = root / name
    if digest(path) != witness["sha256"]:
        raise ValueError("planned specification changed: " + name)
    spec = read(path)
    if type(spec.get("schema")) is not int or spec["schema"] != 1:
        raise ValueError("unsupported run specification schema")
    if name != identifier(spec["run"]) + ".json":
        raise ValueError("specification name differs from its run")
    for host in spec["hosts"]:
        identifier(host)
    roles = [host["role"] for host in spec["hosts"].values()]
    if (type(spec.get("client_count")) is not int or roles.count("server") != 1
            or roles.count("downloader") != spec["client_count"]
            or spec["client_count"] < 1 or set(roles) - {"server", "downloader", "source"}):
        raise ValueError("invalid native cohort")
    if "finite_client_pause" in spec:
        raise ValueError("recovery trials cannot be ordinary timing comparisons")
    return spec


def conditions(spec, *, allow_server_change=False):
    """Ignore provenance labels and three output paths, retaining unknown settings.

Only the serving binary, its regulation overrides and capture enablement may
vary within one pair. Across repetitions each side's full conditions must match.
"""
    result = copy.deepcopy(spec)
    for key in ("run", "claim", "acceptance", "derived_from", "repetition"):
        result.pop(key, None)
    if allow_server_change:
        result.pop("server_revision")
        result.pop("capture_application_lifetimes", None)
    for host in result["hosts"].values():
        raw = host.pop("config")
        if hashlib.sha256(raw.encode()).hexdigest() != host.pop("config_sha256"):
            raise ValueError("configuration differs from its recorded digest")
        config = tomllib.loads(raw)
        for path in (("network", "cache_dir"), ("network", "zakura", "trace_dir"),
                     ("state", "cache_dir")):
            section = config
            for key in path[:-1]:
                section = section.get(key, {})
            if path[-1] in section:
                section[path[-1]] = section[path[-1]].replace("/" + spec["run"] + "/", "/RUN/")
        if allow_server_change and host["role"] == "server":
            host.pop("binary")
            host.pop("binary_sha256")
            network = config["network"]["zakura"]
            block_sync = network.get("block_sync", {})
            block_sync.pop("get_blocks_regulation", None)
            if not block_sync:
                network.pop("block_sync", None)
        host["config"] = config
    return result


def run_evidence(root, spec):
    """Bind completed outcomes to their audited resources and archived bytes."""
    run = spec["run"]
    audit_path = root / (run + "-outcome-audit.json")
    if not audit_path.exists():
        return {"status": "unavailable", "reason": "completion audit is missing"}
    audit = read(audit_path)
    resources_path = root / (run + "-resources.json")
    resources = read(resources_path)
    if (audit["run"] != run or resources["run"] != run
            or set(audit["hosts"]) != set(spec["hosts"])
            or set(resources["hosts"]) != set(spec["hosts"])
            or audit["resources_sha256"] != digest(resources_path)):
        raise ValueError("audit/resource binding differs: " + run)
    failed = []
    archive_hashes = {}
    for host, observation in audit["hosts"].items():
        directory = root / "evidence" / run / host
        extracted = directory / "extracted"
        if (read(extracted / "run-spec.json") != spec
                or read(extracted / "run-outcome.json") != observation["run_outcome"]
                or observation["resources"] != resources["hosts"][host]
                or observation["role"] != spec["hosts"][host]["role"]):
            raise ValueError("archived metadata differs from audit: " + host)
        archive_hashes[host] = digest(directory / (run + ".tar.zst"))
        if archive_hashes[host] != observation["archive_sha256"]:
            raise ValueError("archived bytes differ from audit: " + host)
        outcome = observation["run_outcome"]
        if (outcome.get("run") != run or outcome.get("host") != host
                or outcome.get("native_completed") is not True
                or outcome.get("all_clients_reached_target") is not True
                or observation["resources"].get("recording_complete") is not True):
            failed.append(host)
    if failed:
        return {"status": "failed", "hosts": failed, "audit_sha256": digest(audit_path)}
    controller_path = root / (run + "-controller.json")
    controller = read(controller_path)
    if controller["run"] != run:
        raise ValueError("controller run differs")
    clients = {}
    for host, config in spec["hosts"].items():
        if config["role"] == "downloader":
            seconds = resources["hosts"][host]["completion_seconds"]
            if type(seconds) not in (int, float) or not math.isfinite(seconds) or seconds <= 0:
                raise ValueError("invalid completion duration: " + host)
            clients[host] = seconds
    return {"status": "complete", "clients": clients,
            "resources": resources["hosts"],
            "provenance": {key: controller[key] for key in ("host_environments", "remote_tool_hashes")},
            "sha256": {"audit": digest(audit_path), "resources": digest(resources_path),
                       "controller": digest(controller_path), "archives": archive_hashes}}


def report_series(plan_path):
    """Assess the whole frozen plan; completed subsets remain explicitly partial."""
    root, plan = plan_path.parent, read(plan_path)
    if type(plan.get("schema")) is not int or plan["schema"] != 1:
        raise ValueError("unsupported series schema")
    planned = [{"index": 1, "order": plan.get("first_pair_order"), "runs": plan["first_pair"]}, *plan["pairs"]]
    indices = [pair["index"] for pair in planned]
    if indices != list(range(1, len(planned) + 1)):
        raise ValueError("planned pairs must have consecutive unique indices")
    result = {"schema_version": 1, "plan_sha256": digest(plan_path), "pairs": [], "clients": {},
              "scope": "Descriptive paired sync timings. Clients share a server; neither p95 nor production qualification."}
    reference = {}
    reference_provenance = None
    seen_runs = set()
    changes = {}
    for pair in planned:
        if (set(pair["runs"]) != {"baseline", "candidate"}
                or pair["order"] is not None and sorted(pair["order"]) != ["baseline", "candidate"]):
            raise ValueError("each pair must contain both policies exactly once")
        specs = {label: load_spec(root, witness) for label, witness in pair["runs"].items()}
        if conditions(specs["baseline"], allow_server_change=True) != conditions(specs["candidate"], allow_server_change=True):
            raise ValueError("paired conditions differ beyond the serving policy")
        for label, spec in specs.items():
            if spec["run"] in seen_runs:
                raise ValueError("a run cannot supply multiple trials")
            seen_runs.add(spec["run"])
            current = conditions(spec)
            if label in reference and current != reference[label]:
                raise ValueError("repetition conditions changed: " + spec["run"])
            reference[label] = current
        observations = {label: run_evidence(root, spec) for label, spec in specs.items()}
        entry = {"index": pair["index"], "planned_order": pair["order"],
                 "runs": {label: spec["run"] for label, spec in specs.items()},
                 "observations": observations}
        complete = all(item["status"] == "complete" for item in observations.values())
        entry["status"] = "complete" if complete else "failed" if any(
            item["status"] == "failed" for item in observations.values()) else "unavailable"
        if complete:
            for observation in observations.values():
                provenance = observation["provenance"]
                if reference_provenance is not None and provenance != reference_provenance:
                    raise ValueError("runtime helpers or host environment changed between trials")
                reference_provenance = provenance
            entry["clients"] = {}
            for host, baseline in observations["baseline"]["clients"].items():
                candidate = observations["candidate"]["clients"][host]
                change = 100 * (candidate / baseline - 1)
                entry["clients"][host] = {"baseline_seconds": baseline, "candidate_seconds": candidate,
                                         "change_percent": change}
                changes.setdefault(host, []).append(change)
        result["pairs"].append(entry)
    result["pair_counts"] = {status: sum(pair["status"] == status for pair in result["pairs"])
                             for status in ("complete", "failed", "unavailable")}
    result["all_pairs_complete"] = result["pair_counts"]["complete"] == len(planned)
    result["conditions_sha256"] = {label: hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()
                                   for label, value in reference.items()}
    for host, values in changes.items():
        result["clients"][host] = {"completed_pairs": len(values), "paired_changes_percent": values,
                                   "median_change_percent": statistics.median(values),
                                   "minimum_change_percent": min(values), "maximum_change_percent": max(values)}
    return result
