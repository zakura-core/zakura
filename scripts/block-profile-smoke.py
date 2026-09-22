#!/usr/bin/env python3
"""Exercise the compiled profiler using synthetic blocks and private temporary storage."""
import json
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import urllib.request

ROOT = Path(__file__).resolve().parent.parent
BINARY = ROOT / "target/debug/zakura-profile-explorer"
EXAMPLE = ROOT / "target/debug/examples/block_profile"


def wait_socket(child, path):
    for _ in range(250):
        if child.poll() is not None:
            raise RuntimeError(f"collector exited with {child.returncode}")
        if path.exists():
            return
        time.sleep(0.02)
    raise TimeoutError("collector did not open its socket")


def main():
    children = []
    with tempfile.TemporaryDirectory(prefix="zakura-profile-smoke-") as directory:
        store = Path(directory)
        endpoint = store / "node.sock"

        def collect():
            child = subprocess.Popen([BINARY, "collect", "--store", store, "--socket", endpoint])
            children.append(child)
            wait_socket(child, endpoint)
            return child

        try:
            collector = collect()
            subprocess.run([EXAMPLE, endpoint], check=True, timeout=30)
            time.sleep(1.5)
            report = json.loads(subprocess.check_output([BINARY, "report", "--store", store], timeout=8))
            assert len(report["latest"]) == 10
            assert len(report["outliers"]) == 1
            assert report["counts"]["captured"] == 12
            assert report["counts"]["sealed_detail"] == 12
            assert report["runs"][0]["dropped"] == 0
            assert report["runs"][0]["sequence_gaps"] == 0
            collector.kill()
            collector.wait(timeout=5)
            assert endpoint.exists()
            collector = collect()
            time.sleep(0.2)
            assert collector.poll() is None
            duplicate = subprocess.run([BINARY, "collect", "--store", store, "--socket", endpoint], capture_output=True, timeout=5)
            assert duplicate.returncode != 0 and b"another collector" in duplicate.stderr
            collector.terminate()
            assert collector.wait(timeout=5) == 0
            assert not endpoint.exists()

            # A foreign socket must not be unlinked during startup.
            with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as foreign:
                foreign.bind(str(endpoint))
                endpoint.with_suffix(".lock").unlink()
                refused = subprocess.run([BINARY, "collect", "--store", store, "--socket", endpoint], capture_output=True, timeout=5)
                assert refused.returncode != 0 and endpoint.exists()

            with socket.socket() as reservation:
                reservation.bind(("127.0.0.1", 0))
                port = reservation.getsockname()[1]
            viewer = subprocess.Popen([BINARY, "serve", "--store", store, "--port", str(port)])
            children.append(viewer)
            origin = f"http://127.0.0.1:{port}"
            for _ in range(100):
                try:
                    with urllib.request.urlopen(origin + "/api/home", timeout=1) as response:
                        assert len(json.load(response)["latest"]) == 10
                    break
                except OSError:
                    assert viewer.poll() is None
                    time.sleep(0.05)
            else:
                raise TimeoutError("viewer did not respond")
            row = report["outliers"][0]
            suffix = f"/{row['run']}/{row['attempt']}"
            with urllib.request.urlopen(origin + "/api/attempt" + suffix, timeout=5) as response:
                detail = json.load(response)
            assert detail["complete"]
            assert max(span["end_us"] for span in detail["spans"]) > detail["summary"]["end_us"]
            stages = {span["stage"]: span for span in detail["spans"]}
            assert stages["finalized_commit"]["parent"] == stages["finalization"]["span"]
            assert stages["rocksdb_write"]["parent"] == stages["finalized_commit"]["span"]
            assert stages["rocksdb_write"]["start_us"] >= detail["summary"]["end_us"]
            with urllib.request.urlopen(origin + "/api/trace" + suffix, timeout=5) as response:
                assert json.load(response)["traceEvents"]
            print("Passed: 12 sealed profiles, recent/outlier queries, late writer detail, exports, crash recovery, and socket ownership")
        finally:
            for child in children:
                if child.poll() is None:
                    child.terminate()
                    try:
                        child.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        child.kill()
                        child.wait(timeout=5)


if __name__ == "__main__":
    main()
