#!/usr/bin/env python3
"""Exercise bake snapshot metadata and download recovery."""

import hashlib
import http.server
import io
import json
import os
import pathlib
import socket
import subprocess
import tarfile
import tempfile
import textwrap
import threading
import unittest

import do_provision


def shell_function(name):
    # Exercise the shipped function without installing or building a node.
    source = pathlib.Path(__file__).with_name("pr-node-bake.sh").read_text()
    return (
        f"{name}() {{"
        + source.split(f"{name}() {{", 1)[1].split("\n}\n", 1)[0]
        + "\n}\n"
    )


class SnapshotHeight(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = pathlib.Path(self.temp.name)
        binary = self.root / "target/release/zakurad"
        binary.parent.mkdir(parents=True)
        binary.write_text(
            "#!/usr/bin/env python3\n"
            "import json, os, pathlib, sys\n"
            "assert sys.argv[1] == '-c'\n"
            "assert pathlib.Path(sys.argv[2]).read_text() == "
            "'[state]\\nstorage_mode = \"pruned\"\\n'\n"
            "assert sys.argv[3:5] == ['tip-height', '--cache-dir']\n"
            "assert (pathlib.Path(sys.argv[5]) / 'restored').exists()\n"
            "assert sys.argv[6] == '--network'\n"
            "output, status = json.loads(os.environ['DB_RESULTS'])[sys.argv[7]]\n"
            "print(output, end='')\n"
            "sys.exit(status)\n"
        )
        binary.chmod(0o755)
        self.env = dict(os.environ, CARGO_TARGET_DIR=str(binary.parent.parent))

    def run_script(self, script, results):
        return subprocess.run(
            ["bash", "-c", "set -euo pipefail\n" + script],
            env=dict(self.env, DB_RESULTS=json.dumps(results)),
            cwd=self.root,
            capture_output=True,
            text=True,
            timeout=10,
        )

    def read_height(self, output, status=0):
        (self.root / "restored").touch()
        return self.run_script(
            shell_function("read_state_height") + "\nread_state_height . Mainnet\n",
            {"Mainnet": (output, status)},
        )

    def test_numeric_height_with_startup_logs(self):
        for height in (0, 3470916):
            with self.subTest(height=height):
                result = self.read_height(f"INFO opening database\n{height}\n")
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout, f"{height}\n")

    def test_unreadable_or_ambiguous_state_has_no_height(self):
        for output, status in (
            ("ERROR failed to read state\n", 0),
            ("3470916\n", 1),
            ("", 124),
            ("", 0),
            ("height=3470916\n", 0),
            ("-1\n", 0),
            ("3470916.0\n", 0),
            ("3470916\n3471916\n", 0),
        ):
            with self.subTest(output=output, status=status):
                result = self.read_height(output, status)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stdout, "")

    def bake_tips(self, results):
        # Exercise the shipped restore/measure/write order for both networks.
        source = pathlib.Path(__file__).with_name("pr-node-bake.sh").read_text()
        body = source.split("  # Mainnet tip:", 1)[1].split("\nfi\n\nsync", 1)[0]
        body = "  # Mainnet tip:" + body
        body = body.replace("/root/", str(self.root) + "/")
        (self.root / "mainnet.json").write_text(
            json.dumps(
                {
                    "height": 3471916,
                    "url": "mainnet.tar.zst",
                    "sha256": "mainnet-sha",
                }
            )
        )
        (self.root / "testnet.json").write_text(
            json.dumps(
                {
                    "snapshots": [
                        {
                            "enabled": True,
                            "kind": "pruned",
                            "published": "2026-09-04",
                            "height": "4128095",
                            "file": "testnet.tar.zst",
                            "sha256": "testnet-sha",
                        }
                    ]
                }
            )
        )
        return self.run_script(
            shell_function("read_state_height") + r"""
MAINNET_MNT="$PWD/mainnet"
TESTNET_MNT="$PWD/testnet"
TIP_MAINNET_LATEST_JSON=mainnet-latest
TESTNET_SNAPSHOTS_BASE=testnet-site
curl() {
  case "${@: -1}" in
    mainnet-latest) cat mainnet.json ;;
    testnet-site/snapshots.json) cat testnet.json ;;
    *) return 1 ;;
  esac
}
fetch_state() {
  mkdir -p "$3"
  touch "$3/restored"
}
""" + body,
            results,
        )

    def test_bake_measures_both_networks_before_checkpoint_selection(self):
        result = self.bake_tips(
            {
                "Mainnet": ("3470916\n", 0),
                "Testnet": ("4127095\n", 0),
            }
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        measured = (self.root / "mainnet-state-height").read_text().strip()
        self.assertEqual(measured, "3470916")
        self.assertEqual((self.root / "testnet-state-height").read_text(), "4127095\n")
        # Feed the measured files through the actual workflow naming step.
        workflow = pathlib.Path(__file__).parents[1] / "zakura-pr-node-bake.yml"
        step = workflow.read_text().split("      - name: Snapshot state volumes", 1)[1]
        body = textwrap.dedent(
            step.split("        run: |\n", 1)[1].split("\n      - name:", 1)[0]
        )
        body = body.replace("/root/", str(self.root) + "/")
        result = self.run_script(
            r"""
IP=example.invalid
MAINNET_VOL_ID=mainnet
TESTNET_VOL_ID=testnet
APPROACH_VOL_ID=approach
REBUILD_APPROACH_FROM_SANDBLAST=false
REGION=nyc1
STATE_PREFIX=zakura-pr-state
APPROACH_PREFIX=zakura-vct-approach
GITHUB_OUTPUT="$PWD/outputs"
ssh() {
  local request="${@: -1}"
  request="${request#cat }"
  cat "${request%% *}" 2>/dev/null
}
python3() {
  while [ "$1" != --name ]; do shift; done
  printf '%s\n' "$2" >> "$PWD/snapshot-names"
  echo fixture-id
}
""" + body,
            {},
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        names = (self.root / "snapshot-names").read_text().splitlines()
        self.assertEqual(len(names), 2)
        self.assertTrue(names[1].endswith("-finalized-h4127095"))
        states = [
            {
                "id": "approach",
                "name": "zakura-vct-approach-mainnet-old-h3418306",
                "regions": ["nyc1"],
            },
            {
                "id": "tip",
                "name": names[0],
                "regions": ["nyc1"],
            },
            {
                "id": "old-tip",
                "name": "zakura-pr-state-mainnet-old-h3471916",
                "regions": ["nyc1"],
            },
        ]
        for checkpoint, expected in (
            (3470171, "approach"),
            (3470916, "approach"),
            (3472489, "tip"),
        ):
            with self.subTest(checkpoint=checkpoint):
                selected = do_provision.select_state(
                    states,
                    "nyc1",
                    "mainnet",
                    "pre-checkpoint",
                    checkpoint,
                )
                self.assertEqual(selected["id"], expected)
                self.assertLess(do_provision.height(selected), checkpoint)
        self.assertEqual(do_provision.height(states[1]), int(measured))

    def test_bake_stops_without_publishing_an_unreadable_database_height(self):
        for network in ("Mainnet", "Testnet"):
            with self.subTest(network=network):
                results = {"Mainnet": ("3470916\n", 0), "Testnet": ("4127095\n", 0)}
                results[network] = ("ERROR failed to read state\n", 0)
                result = self.bake_tips(results)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(
                    (self.root / f"{network.lower()}-state-height").read_text(),
                    "",
                )

    def test_rebuilt_approach_requires_exact_readable_database_height(self):
        source = pathlib.Path(__file__).with_name("pr-node-bake.sh").read_text()
        body = (
            "  VERIFIED_APPROACH_H="
            + source.split("  VERIFIED_APPROACH_H=", 1)[1].split("\nelse\n", 1)[0]
        )
        body = body.replace("/root/", str(self.root) + "/")
        (self.root / "tip").mkdir()
        (self.root / "tip/restored").touch()
        marker = self.root / "mainnet-approach-height"
        for output in ("3472389\n", "3471389\n", "ERROR reopening fixture\n"):
            with self.subTest(output=output):
                marker.unlink(missing_ok=True)
                result = self.run_script(
                    shell_function("read_state_height")
                    + '\nAPPROACH_MNT="$PWD"\nAPPROACH_H=3472389\n'
                    + body,
                    {"Mainnet": (output, 0)},
                )
                if output == "3472389\n":
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(marker.read_text(), output)
                else:
                    self.assertNotEqual(result.returncode, 0)
                    self.assertFalse(marker.exists())


class BakeDownload(unittest.TestCase):
    def test_deadline_caps_connections_backoff_and_later_downloads(self):
        for budget, expected_timeouts in ((1000, ["600", "385"]), (605, ["600"])):
            with self.subTest(budget=budget):
                with tempfile.TemporaryDirectory() as tmp:
                    root = pathlib.Path(tmp)
                    (root / "clock").write_text("0")
                    script = (
                        r"""
        set -euo pipefail
        cd "$1"
        BAKE_DOWNLOAD_DEADLINE=$2
        date() { cat clock; }
        sleep() { echo "$(( $(cat clock) + $1 ))" > clock; }
        curl() {
          while [ "$1" != --max-time ]; do shift; done
          echo "$2" >> timeouts
          echo "$(( $(cat clock) + $2 ))" > clock
          return 28
        }
        """
                        + shell_function("fetch_state")
                        + r"""
        fetch_state https://example.invalid/first '' "$1/first" mainnet && exit 1
        fetch_state https://example.invalid/second '' "$1/second" mainnet && exit 1
        exit 0
        """
                    )
                    result = subprocess.run(
                        ["bash", "-c", script, "test", str(root), str(budget)],
                        capture_output=True,
                        text=True,
                        timeout=10,
                    )
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(
                        (root / "timeouts").read_text().splitlines(), expected_timeouts
                    )
                    self.assertEqual((root / "clock").read_text().strip(), str(budget))
                    self.assertEqual(
                        result.stderr.count("state download deadline reached"), 2
                    )
                    self.assertFalse((root / "first").exists())
                    self.assertFalse((root / "second").exists())

    def test_interrupted_download_resumes_and_verifies_archive(self):
        self.check_interrupted_download(1)

    def test_download_can_resume_more_than_nine_times(self):
        self.check_interrupted_download(10)

    def check_interrupted_download(self, interruptions):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            buffer = io.BytesIO()
            with tarfile.open(fileobj=buffer, mode="w") as archive:
                payload = b"validated chain fixture"
                info = tarfile.TarInfo("state/v1/mainnet/fixture")
                info.size = len(payload)
                archive.addfile(info, io.BytesIO(payload))
            data = subprocess.run(
                ["zstd", "-c"], input=buffer.getvalue(), capture_output=True, check=True
            ).stdout
            cut = len(data) // (interruptions + 1)
            ranges = []

            class Handler(http.server.BaseHTTPRequestHandler):
                def do_GET(self):
                    ranges.append(self.headers.get("Range"))
                    byte_range = self.headers.get("Range")
                    offset = int((byte_range or "bytes=0-").split("=")[1].split("-")[0])
                    self.send_response(206 if byte_range else 200)
                    self.send_header("Content-Length", str(len(data) - offset))
                    if byte_range:
                        self.send_header(
                            "Content-Range",
                            f"bytes {offset}-{len(data) - 1}/{len(data)}",
                        )
                    self.end_headers()
                    if len(ranges) <= interruptions:
                        self.wfile.write(data[offset : offset + cut])
                        self.wfile.flush()
                        self.connection.shutdown(socket.SHUT_RDWR)
                        self.connection.close()
                    else:
                        self.wfile.write(data[offset:])

                def log_message(self, *args):
                    pass

            server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
            self.addCleanup(server.server_close)
            self.addCleanup(server.shutdown)
            threading.Thread(target=server.serve_forever, daemon=True).start()
            script = (
                "set -euo pipefail\nsleep() { :; }\n"
                + "BAKE_DOWNLOAD_DEADLINE=$(( $(date +%s) + 60 ))\n"
                + shell_function("fetch_state")
                + '\nfetch_state "$1" "$2" "$3" mainnet\n'
            )
            result = subprocess.run(
                [
                    "bash",
                    "-c",
                    script,
                    "test",
                    f"http://127.0.0.1:{server.server_port}/fixture",
                    hashlib.sha256(data).hexdigest(),
                    str(root / "tip"),
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(
                ranges,
                [None] + [f"bytes={cut * n}-" for n in range(1, interruptions + 1)],
            )
            self.assertEqual(
                (root / "tip/state/v1/mainnet/fixture").read_bytes(), payload
            )
            self.assertFalse((root / "tip.tar.zst").exists())


if __name__ == "__main__":
    unittest.main()
