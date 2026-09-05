"""Build, test, and record bounded experiments. Run from any directory."""

import argparse
import csv
from dataclasses import replace
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import resource
import subprocess
import sys
import time

from observations import experiment, parity_bounds
from graph import experiment as graph_experiment
from sim import Config, Simulation, scenarios


ROOT = Path(__file__).resolve().parent


def command(args):
    return subprocess.check_output(args, cwd=ROOT, text=True, stderr=subprocess.STDOUT)


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--quick", action="store_true")
    args = parser.parse_args()
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=True)
    if any(out.iterdir()):
        parser.error("output directory must be empty; preserve earlier runs")
    build = ROOT / "build"
    build.mkdir(exist_ok=True)
    binary = build / "codec"
    compile_args = ["g++", "-std=c++20", "-O3", "-Wall", "-Wextra", "-Werror",
                    "codec.cpp", "-lcrypto", "-o", str(binary)]
    started = time.time()
    command(compile_args)
    tests = command([str(binary), "--test"])
    tests += command([sys.executable, "-m", "unittest", "-v", "test_sim.py"])
    (out / "tests.txt").write_text(tests)
    print(tests, flush=True)
    codec_rows, calls = [], []
    cases = [(k, .25, trace, 1) for k in (8, 32, 128)
             for trace in ("systematic", "random", "parity_first", "withheld")]
    cases += [(32, ratio, "parity_first", 1) for ratio in (.125, .5, 1)]
    cases += [(32, .25, trace, 4) for trace in ("systematic", "parity_first")]
    cases += [(32, .25, trace, 1) for trace in ("insufficient", "invalid_codeword")]
    if args.quick:
        cases = [(32, .25, "parity_first", 1)]
    for k, ratio, trace, assemblies in cases:
        for codec in ("rs", "rlnc"):
            # First repeat warms this process. Preserve it with an explicit warmup field.
            argv = [str(binary), str(k), str(ratio), codec, trace, str(assemblies), "100", "4"]
            before = resource.getrusage(resource.RUSAGE_CHILDREN)
            start = time.perf_counter()
            output = command(argv)
            after = resource.getrusage(resource.RUSAGE_CHILDREN)
            calls.append(dict(argv=argv, wall_seconds=time.perf_counter() - start,
                              cpu_seconds=after.ru_utime + after.ru_stime - before.ru_utime - before.ru_stime))
            for row in csv.DictReader(io.StringIO(output)):
                row["warmup"] = int(row["seed"] == "100")
                codec_rows.append(row)
            print(f"codec {codec} k={k} parity={ratio} {trace} assemblies={assemblies}", flush=True)
    with (out / "codec.csv").open("w") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(codec_rows[0]))
        writer.writeheader()
        writer.writerows(codec_rows)
    write_json(out / "codec-processes.json", calls)

    policies = ("equal", "random", "global_best", "passive", "races", "budgeted")
    seeds = range(2 if args.quick else 12)
    configs = scenarios()[:2] if args.quick else scenarios()
    with (out / "routing.jsonl").open("w") as stream:
        for cfg in configs:
            for policy in policies:
                for seed in seeds:
                    stream.write(json.dumps(Simulation(cfg, policy, seed).run(), sort_keys=True) + "\n")
            print(f"routing {cfg.name}", flush=True)

    # Factor sweeps hold the other inputs fixed; interaction sweeps cover the main couplings.
    sweeps = []
    if not args.quick:
        for rho in (1 / 128, 1 / 32, 1 / 8):
            for width in (1, 2, 4):
                for interval in (0, 250, 2000):
                    for age in (5000, 20000, 80000):
                        for mode in ("alternating", "sparse"):
                            sweeps.append(replace(Config(), name="challenge_sweep", locality=True,
                                                  blocks=96, rho=rho, challenge_width=width,
                                                  min_interval_ms=interval, trial_age_ms=age,
                                                  proposer_mode=mode))
        for ratio in (.125, .25, .5, 1):
            for extra in (0, .125, .25, .5, 1):
                if extra > ratio:
                    continue
                for change in ("none", "withhold"):
                    sweeps.append(replace(Config(), name="parity_sweep", parity=ratio,
                                          subscribe_extra=extra, change=change))
        for budget in (8, 20, 64):
            for beta in (.5, .75, .9):
                for migration in (1, 4, 16):
                    sweeps.append(replace(Config(), name="budget_sweep", initial_budget_parts=budget,
                                          beta=beta, migration_parts=migration, change="bandwidth",
                                          rates=(80, 20, 10, 5), locality=True))
        for k in (8, 32, 128):
            for burst in (1, 2, 4):
                for rate in (5, 20, 80):
                    sweeps.append(replace(Config(), name="load_sweep", k=k, burst=burst,
                                          rates=(rate, rate, rate, rate)))
    with (out / "sweeps.jsonl").open("w") as stream:
        for num, cfg in enumerate(sweeps):
            for seed in range(4):
                stream.write(json.dumps(Simulation(cfg, "budgeted", seed).run(), sort_keys=True) + "\n")
            if num % 30 == 0:
                print(f"sweep {num}/{len(sweeps)}", flush=True)
    write_json(out / "observations.json", experiment(40 if args.quick else 400))
    write_json(out / "parity-bounds.json", parity_bounds())
    write_json(out / "graph.json", graph_experiment(4 if args.quick else 40))
    files = ("codec.cpp", "sim.py", "observations.py", "graph.py", "test_sim.py", "run.py", "summarize.py")
    write_json(out / "environment.json", dict(
        timestamp_utc=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(started)),
        wall_seconds=time.time() - started, platform=platform.platform(),
        python=sys.version, compiler=command(["g++", "--version"]),
        openssl=command(["pkg-config", "--modversion", "openssl"]),
        cpu=command(["lscpu"]), compile_argv=compile_args,
        invocation=sys.argv, quick=args.quick,
        source_sha256={f: hashlib.sha256((ROOT / f).read_bytes()).hexdigest() for f in files},
        notes="Single host, no CPU affinity or exclusive-core reservation. Arrival replay uses measured task wall times."))
    print(f"Finished: {out}", flush=True)


if __name__ == "__main__":
    main()
