#!/usr/bin/env python3
"""Check a fuzz campaign against C1's suite manifest and write one segment's
launch script.

A campaign directory holds a `manifest.toml`: the PR heads it integrates,
each target's shard, workers and per-input timeout, and each segment's
search budget. `zakura-sim suite --frozen --out <file>` writes the suite
manifest, which fixes the suite's fuzz targets. This tool refuses a
campaign whose targets differ from the suite manifest's, whose suite
manifest describes another checkout or another `qa/suite.toml`, or whose
budgets aren't bounded.

The launch script runs one shard of one segment. It builds every target of
the shard, then runs them together, each under libFuzzer's
`-max_total_time` and an outer `timeout`. Each run writes into a new
directory, `<root>/segments/<segment>[.<attempt>]/<shard>/<package>/<name>/`,
and every run of a target shares the corpus `<root>/corpus/<package>/<name>/`.
A later attempt of a segment is an explicit resume: it needs the previous
attempt's directory, and writes its own.

Usage:
  fuzz-campaign.py --campaign <dir> --suite-manifest <file> --check
  fuzz-campaign.py --campaign <dir> --suite-manifest <file>
      --segment <name> --shard <name> [--attempt <n>] [--target <id>]...
      [--root <dir>]

Exit status: 0 when the campaign checks clean (and, without --check, after
printing the script); 1 when the check finds problems; 2 on a usage error.
"""

import argparse
import hashlib
import json
import shlex
import subprocess
import sys
import tomllib
from pathlib import Path

SCHEMA = 2
REPO = Path(__file__).resolve().parents[1]
SUITE_FILE = "qa/suite.toml"
SIM_RUSTFLAGS = "--cfg zakura_sim"
# Seconds the outer `timeout` waits past the search budget and one input's
# timeout before it stops a run, and then before it kills one.
GRACE_SECS = 300


def load_toml(path):
    """The TOML file at `path`, parsed."""
    with open(path, "rb") as file:
        return tomllib.load(file)


def suite_targets(suite, manifest):
    """Each target's `qa/suite.toml` row, by the suite manifest's target ID,
    with the harness the suite manifest resolved for it."""
    harnesses = {row["id"]: row.get("harness") for row in manifest.get("targets", [])}
    rows = {}
    for row in suite.get("target", []):
        target_id = f"target/{row['package']}/{row['name']}"
        rows[target_id] = {**row, "harness": harnesses.get(target_id)}
    return rows


def check(campaign, suite_text, manifest, head, is_ancestor):
    """Every problem that keeps `campaign` from running at `head`.

    `suite_text` is `qa/suite.toml`'s bytes, `manifest` the parsed output of
    `zakura-sim suite`, and `is_ancestor(sha)` tells whether a commit is an
    ancestor of `head`.
    """
    problems = []
    if campaign.get("schema") != SCHEMA:
        problems.append(f"manifest.toml has schema {campaign.get('schema')}, not {SCHEMA}")

    build = manifest.get("build", {})
    if build.get("dirty", True):
        problems.append("the suite manifest comes from a dirty build; write it with `suite --frozen`")
    if build.get("git") != head:
        problems.append(f"the suite manifest describes {build.get('git')}, not the checkout's {head}")
    if manifest.get("suite_sha256") != hashlib.sha256(suite_text).hexdigest():
        problems.append(f"the suite manifest read another {SUITE_FILE}")
    toolchain = campaign.get("toolchain", {})
    formats = campaign.get("formats", {})
    for name, expected, actual in [
        ("toolchain.fuzz", toolchain.get("fuzz"), manifest.get("nightly")),
        ("formats.trace", formats.get("trace"), manifest.get("format")),
        ("formats.contract", formats.get("contract"), manifest.get("contract")),
    ]:
        if expected != actual:
            problems.append(f"manifest.toml's {name} is {expected}, but the suite manifest has {actual}")

    for name, sha in campaign.get("integration", {}).items():
        if not is_ancestor(sha):
            problems.append(f"integration.{name}, {sha}, isn't an ancestor of {head}")

    budgets = {}
    for segment in campaign.get("segment", []):
        if segment["name"] in budgets:
            problems.append(f"manifest.toml names the segment `{segment['name']}` twice")
        budgets[segment["name"]] = segment.get("budget-secs", 0)
        if budgets[segment["name"]] <= 0:
            problems.append(f"the segment `{segment['name']}` has no search budget")
    if not budgets:
        problems.append("manifest.toml defines no segment")

    expected = {row["id"] for row in manifest.get("targets", [])}
    seen = set()
    workers = {}
    for target in campaign.get("target", []):
        target_id = target["id"]
        if target_id in seen:
            problems.append(f"manifest.toml names `{target_id}` twice")
        seen.add(target_id)
        if target_id not in expected:
            problems.append(f"manifest.toml runs `{target_id}`, which the suite manifest doesn't map")
        if target.get("workers", 0) < 1:
            problems.append(f"`{target_id}` has no worker")
        timeout = target.get("input-timeout-secs", 0)
        if timeout < 1:
            problems.append(f"`{target_id}` has no per-input timeout")
        for name, budget in budgets.items():
            if 0 < budget <= timeout:
                problems.append(f"`{target_id}`'s per-input timeout isn't below the `{name}` budget")
        shard = target.get("shard")
        if not shard:
            problems.append(f"`{target_id}` has no shard")
        workers[shard] = workers.get(shard, 0) + target.get("workers", 0)
    for target_id in sorted(expected - seen):
        problems.append(f"the suite manifest maps `{target_id}`, and manifest.toml doesn't run it")
    cpus = campaign.get("cpus", 0)
    for shard, count in sorted(workers.items(), key=str):
        if count > cpus:
            problems.append(f"the shard `{shard}` has {count} workers on {cpus} vCPUs")
    return problems


def run_command(target, row, budget, corpus, evidence, toolchain, repo):
    """The shell command that runs one target for `budget` seconds."""
    package, name = row["package"], row["name"]
    workers = target["workers"]
    timeout = target["input-timeout-secs"]
    args = [f"-max_total_time={budget}", f"-timeout={timeout}"]
    if workers > 1:
        args += [f"-jobs={workers}", f"-workers={workers}"]
    args += row.get("args", [])
    args.append(f"-artifact_prefix={evidence}/artifacts/")
    env = list(row.get("env", []))
    if package == "sim":
        env += [f"RUSTFLAGS={SIM_RUSTFLAGS}", f"ZAKURA_SIM_ARTIFACTS={evidence}/sim"]
    words = ["env", *env, "timeout", f"--kill-after={GRACE_SECS}", str(budget + timeout + GRACE_SECS)]
    words += [
        "cargo", f"+{toolchain}", "fuzz", "run", "--fuzz-dir", str(repo / "qa/fuzz" / package),
        name, str(corpus), "--", *args,
    ]
    return shlex.join(words)


def segment_script(campaign, rows, suite_manifest, segment, shard, attempt, only, root, head, repo=REPO):
    """The bash script that runs `shard` of `segment`'s attempt `attempt`.

    `rows` maps each target ID to its `qa/suite.toml` row, and `only`, when
    not empty, limits the run to those target IDs.
    """
    budget = next(row["budget-secs"] for row in campaign["segment"] if row["name"] == segment)
    toolchain = campaign["toolchain"]["fuzz"]
    targets = [
        target for target in campaign["target"]
        if target["shard"] == shard and (not only or target["id"] in only)
    ]
    run = f"{segment}" if attempt == 1 else f"{segment}.{attempt}"
    parent = None if attempt == 1 else (f"{segment}" if attempt == 2 else f"{segment}.{attempt - 1}")
    out = root / "segments" / run / shard
    record = {
        "campaign": campaign["name"],
        "segment": segment,
        "attempt": attempt,
        "parent": parent,
        "shard": shard,
        "budget_secs": budget,
        "commit": head,
        "suite_manifest_sha256": hashlib.sha256(Path(suite_manifest).read_bytes()).hexdigest(),
        "targets": [target["id"] for target in targets],
    }
    q = shlex.quote
    lines = [
        "#!/usr/bin/env bash",
        f"# {campaign['name']}: segment {run}, shard {shard}, {budget} s per target.",
        "set -euo pipefail",
        f"cd {q(str(repo))}",
    ]
    if parent is not None:
        lines.append(
            f"test -d {q(str(root / 'segments' / parent / shard))} "
            f"|| {{ echo 'attempt {attempt} resumes {parent}, which has no {shard} run' >&2; exit 1; }}"
        )
    lines += [
        f"mkdir -p {q(str(root / 'segments' / run))}",
        f"mkdir {q(str(out))}",
        f"cp {q(str(suite_manifest))} {q(str(out / 'suite.json'))}",
        f"printf '%s\\n' {q(json.dumps(record, indent=2))} > {q(str(out / 'segment.json'))}",
        f"date -u +%FT%TZ > {q(str(out / 'started'))}",
    ]
    for target in targets:
        row = rows[target["id"]]
        package, name = row["package"], row["name"]
        corpus = root / "corpus" / package / name
        rustflags = f"RUSTFLAGS={q(SIM_RUSTFLAGS)} " if package == "sim" else ""
        lines.append(
            f"{rustflags}cargo +{toolchain} fuzz build --fuzz-dir {q(str(repo / 'qa/fuzz' / package))} {q(name)}"
        )
        harness = row.get("harness")
        seed = f" && cargo xtask sim-seeds --sim {q(harness)} --out {q(str(corpus))}" if harness else ""
        lines.append(f"test -d {q(str(corpus))} || {{ mkdir -p {q(str(corpus))}{seed}; }}")
    for target in targets:
        row = rows[target["id"]]
        evidence = out / row["package"] / row["name"]
        corpus = root / "corpus" / row["package"] / row["name"]
        command = run_command(target, row, budget, corpus, evidence, toolchain, repo)
        lines += [
            f"mkdir -p {q(str(evidence / 'artifacts'))} {q(str(evidence / 'sim'))}",
            f"( cd {q(str(evidence))} && status=0 && {{ {command} > run.log 2>&1 || status=$?; }} "
            f"&& echo \"$status\" > exit-status ) &",
        ]
    lines += ["wait", f"date -u +%FT%TZ > {q(str(out / 'finished'))}", ""]
    return "\n".join(lines)


def git_head(repo):
    """The checkout's commit."""
    return subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()


def git_is_ancestor(repo, head):
    """A function telling whether a commit is an ancestor of `head`."""
    def is_ancestor(sha):
        return subprocess.run(
            ["git", "-C", str(repo), "merge-base", "--is-ancestor", sha, head], capture_output=True
        ).returncode == 0
    return is_ancestor


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--campaign", type=Path, required=True)
    parser.add_argument("--suite-manifest", type=Path, required=True)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--segment")
    parser.add_argument("--shard")
    parser.add_argument("--attempt", type=int, default=1)
    parser.add_argument("--target", action="append", default=[])
    parser.add_argument("--root", type=Path)
    args = parser.parse_args(argv)

    campaign = load_toml(args.campaign / "manifest.toml")
    suite_text = (REPO / SUITE_FILE).read_bytes()
    manifest = json.loads(args.suite_manifest.read_text())
    head = git_head(REPO)
    problems = check(campaign, suite_text, manifest, head, git_is_ancestor(REPO, head))
    if problems:
        print(f"error: {len(problems)} problems keep {campaign.get('name')} from running:", file=sys.stderr)
        for problem in problems:
            print(problem, file=sys.stderr)
        return 1
    if args.check:
        print(f"{campaign['name']}: {len(campaign['target'])} targets match the suite manifest of {head}")
        return 0

    segments = {row["name"] for row in campaign["segment"]}
    shards = {row["shard"] for row in campaign["target"]}
    ids = {row["id"] for row in campaign["target"]}
    if args.segment not in segments:
        parser.error(f"--segment must be one of {', '.join(sorted(segments))}")
    if args.shard not in shards:
        parser.error(f"--shard must be one of {', '.join(sorted(shards))}")
    if args.attempt < 1:
        parser.error("--attempt starts at 1")
    if unknown := set(args.target) - ids:
        parser.error(f"unknown --target {', '.join(sorted(unknown))}")
    rows = suite_targets(tomllib.loads(suite_text.decode()), manifest)
    root = args.root or Path(campaign["artifacts_root"])
    sys.stdout.write(segment_script(
        campaign, rows, args.suite_manifest.resolve(), args.segment, args.shard, args.attempt,
        set(args.target), root, head,
    ))
    return 0


if __name__ == "__main__":
    sys.exit(main())
