#!/usr/bin/env python3
"""Exercise native and legacy connections in independently built old/new test binaries."""

import argparse
import json
import os
from pathlib import Path
import selectors
import subprocess
import tempfile
import time

TEST = "zakura::testkit::interop::native_process_peer"


def pair(server_binary, client_binary, output, label, test=TEST):
    """Require verified native payload delivery or a completed legacy ping/pong exchange."""
    args = ["--exact", test, "--ignored", "--nocapture", "--test-threads=1"]
    env = os.environ.copy()
    env.pop("ZAKURA_INTEROP_PEER", None)
    finish_dir = tempfile.TemporaryDirectory(prefix="zakura-interop-")
    finish = Path(finish_dir.name) / "finished"
    env["ZAKURA_INTEROP_FINISH"] = str(finish)
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
        finish.touch()
        remaining, _ = server.communicate(timeout=25)
        server_output.extend(remaining)
        passed = (
            client.returncode == 0 and server.returncode == 0
            and b"ZAKURA_INTEROP_OK" in client.stdout
            and b"ZAKURA_INTEROP_OK" in server_output
        )
        transport_rejected = (
            client.returncode == 101 and server.returncode == 101
            and b"Error: Elapsed(())" in server_output
            and (b"Error: Elapsed(())" in client.stdout
                 or b"peer doesn't support any known protocol" in client.stdout)
            and b"panicked at" not in client.stdout
            and b"panicked at" not in server_output
        )
        return {"pair": label, "passed": passed, "transport_rejected": transport_rejected,
                "server_exit": server.returncode, "client_exit": client.returncode}
    except (RuntimeError, subprocess.TimeoutExpired) as error:
        return {"pair": label, "passed": False, "error": str(error)}
    finally:
        if server.poll() is None:
            server.kill()
            remaining, _ = server.communicate()
            server_output.extend(remaining)
        (output / f"{label}-server.log").write_bytes(server_output)
        finish_dir.cleanup()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--old-test-bin", required=True)
    parser.add_argument("--new-test-bin", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--mode", choices=["native", "legacy"], default="native")
    parser.add_argument("--expect-mixed-rejection", action="store_true",
                        help="Native mixed pairs must not complete; verify legacy separately.")
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    binaries = {"old": args.old_test_bin, "new": args.new_test_bin}
    results = []
    pairs = [("old", "new"), ("new", "old")]
    test = "peer::handshake::tests::legacy_process_peer"
    if args.mode == "native":
        pairs = [("old", "old"), ("new", "new"), *pairs]
        test = TEST
    for server, client in pairs:
        result = pair(binaries[server], binaries[client], args.output_dir, f"{server}-{client}", test)
        result["completed"] = result["passed"]
        if args.mode == "native" and args.expect_mixed_rejection and server != client:
            result["expected"] = "native connection must not complete"
            result["passed"] = (not result["completed"] and "error" not in result
                                and result["transport_rejected"])
        results.append(result)
        print(json.dumps(result), flush=True)
    (args.output_dir / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    return 0 if all(result["passed"] for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
