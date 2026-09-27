import importlib.util
import pathlib
import sys
import tempfile
import unittest


SCRIPT = pathlib.Path(__file__).parents[1] / "test-nsm-release-upgrade.py"
SPEC = importlib.util.spec_from_file_location("test_nsm_release_upgrade_script", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class AssignedPortsTest(unittest.TestCase):
    def test_config_uses_os_assigned_ports(self):
        with tempfile.TemporaryDirectory() as directory:
            node = MODULE.Node(
                sys.executable,
                pathlib.Path(directory) / "node",
                activate=True,
            )
            config = (node.directory / "zakura.toml").read_text()

        self.assertIn('listen_addr = "127.0.0.1:0"', config)
        self.assertEqual(config.count('listen_addr = "127.0.0.1:0"'), 2)
        self.assertIsNone(node.rpc_port)

    def test_rpc_discovery_ignores_endpoints_from_previous_starts(self):
        with tempfile.TemporaryDirectory() as directory:
            log_path = pathlib.Path(directory) / "node.log"
            log_path.write_bytes(b"Opened RPC endpoint at 127.0.0.1:18232\n")
            offset = log_path.stat().st_size

            self.assertIsNone(MODULE.assigned_rpc_port(log_path, offset))

            with log_path.open("ab") as log:
                log.write(b"Opened RPC endpoint at 127.0.0.1:29451\n")

            self.assertEqual(MODULE.assigned_rpc_port(log_path, offset), 29451)


if __name__ == "__main__":
    unittest.main()
