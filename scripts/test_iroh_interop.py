#!/usr/bin/env python3
"""Exercise native handlers in independently built old/new network test binaries."""

import argparse
import json
import os
from pathlib import Path
import selectors
import subprocess
import time

TEST = "zakura::testkit::interop::native_process_peer"


def pair(server_binary, client_binary, output, label):
    """Require both processes to finish after an authenticated 1 MiB delivery."""
    args = ["--exact", TEST, "--ignored", "--nocapture", "--test-threads=1"]
    env = os.environ.copy()
    env.pop("ZAKURA_INTEROP_PEER", None)
    server = subprocess.Popen(
        [server_binary, *args], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env
    )
    server_output = bytearray()
    try:
        deadline = time.monotonic() + 10
        peer = None
        with selectors.DefaultSelector() as selector:
            selector.register(server.stdout, selectors.EVENT_READ)
            while peer is None and time.monotonic() < deadline:
                if not selector.select(timeout=max(0, deadline - time.monotonic())):
                    break
                chunk = os.read(server.stdout.fileno(), 65536)
                if not chunk:
                    break
                server_output.extend(chunk)
                for line in server_output.decode(errors="replace").splitlines():
                    if "ZAKURA_INTEROP_READY=" in line:
                        peer = line.split("ZAKURA_INTEROP_READY=", 1)[1].strip()
        if peer is None:
            raise RuntimeError("server did not advertise a loopback endpoint")
        env["ZAKURA_INTEROP_PEER"] = peer
        client = subprocess.run(
            [client_binary, *args], stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            env=env, timeout=25, check=False,
        )
        (output / f"{label}-client.log").write_bytes(client.stdout)
        remaining, _ = server.communicate(timeout=25)
        server_output.extend(remaining)
        passed = (
            client.returncode == 0 and server.returncode == 0
            and b"ZAKURA_INTEROP_OK" in client.stdout
            and b"ZAKURA_INTEROP_OK" in server_output
        )
        return {"pair": label, "passed": passed,
                "server_exit": server.returncode, "client_exit": client.returncode}
    except (RuntimeError, subprocess.TimeoutExpired) as error:
        return {"pair": label, "passed": False, "error": str(error)}
    finally:
        if server.poll() is None:
            server.kill()
            remaining, _ = server.communicate()
            server_output.extend(remaining)
        (output / f"{label}-server.log").write_bytes(server_output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--old-test-bin", required=True)
    parser.add_argument("--new-test-bin", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    binaries = {"old": args.old_test_bin, "new": args.new_test_bin}
    results = []
    for server, client in [("old", "old"), ("new", "new"), ("old", "new"), ("new", "old")]:
        result = pair(binaries[server], binaries[client], args.output_dir, f"{server}-{client}")
        results.append(result)
        print(json.dumps(result), flush=True)
    (args.output_dir / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    return 0 if all(result["passed"] for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
