import importlib.util
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("session", Path(__file__).with_name("session.py"))
session = importlib.util.module_from_spec(spec)
spec.loader.exec_module(session)


class Teardown(unittest.TestCase):
    def droplet(self):
        return {"id": 7, "name": "zakura-profile-test", "tags": [session.TAG], "volume_ids": ["vol"], "networks": {"v4": [{"type": "public", "ip_address": "192.0.2.1"}]}}

    def test_failed_unmount_never_detaches_or_deletes(self):
        with patch.object(session, "resource", return_value=self.droplet()), patch.object(session, "retained_volume", return_value={"droplet_ids": [7]}), patch.object(session, "do") as do, patch.object(session.subprocess, "run", side_effect=subprocess.CalledProcessError(1, "ssh")):
            with self.assertRaises(subprocess.CalledProcessError):
                session.stop(7, "vol")
            do.assert_not_called()

    def test_still_attached_volume_prevents_droplet_delete(self):
        with patch.object(session, "resource", return_value=self.droplet()), patch.object(session, "retained_volume", return_value={"droplet_ids": [7]}), patch.object(session, "do") as do, patch.object(session.subprocess, "run"):
            with self.assertRaises(RuntimeError):
                session.stop(7, "vol")
            do.assert_called_once_with("compute", "volume-action", "detach", "vol", "7", "--wait")

    def test_ordered_teardown_has_no_volume_delete(self):
        with patch.object(session, "resource", return_value=self.droplet()), patch.object(session, "retained_volume", side_effect=[{"droplet_ids": [7]}, {"droplet_ids": []}]), patch.object(session, "do") as do, patch.object(session.subprocess, "run") as ssh, patch("builtins.print"):
            session.stop(7, "vol")
            self.assertIn("StrictHostKeyChecking=yes", ssh.call_args.args[0])
            self.assertEqual(do.call_args_list[-1].args, ("compute", "droplet", "delete", "7", "--force"))
            self.assertEqual(do.call_count, 2)

    def test_production_tag_refused_before_ssh(self):
        droplet = self.droplet()
        droplet["tags"] = ["production"]
        with patch.object(session, "resource", return_value=droplet), patch.object(session, "retained_volume", return_value={"droplet_ids": [7]}), patch.object(session.subprocess, "run") as ssh:
            with self.assertRaises(ValueError):
                session.stop(7, "vol")
            ssh.assert_not_called()


if __name__ == "__main__":
    unittest.main()
