import json
import dataclasses
from pathlib import Path
import shutil
import subprocess
import tempfile
import tomllib
import unittest
from unittest.mock import patch

from test_continuous_sync import ROOT, load_module, make_config, sync, deploy

candidate = load_module('genesis_candidate', ROOT/'deploy/continuous-sync/candidate.py')
host = load_module('genesis_candidate_host', ROOT/'deploy/continuous-sync/candidate-host.py')


class CandidateTests(unittest.TestCase):
    def test_owner_names_cannot_address_fleet_or_escape_shell(self):
        for run_id in ['', '0', '../1', '1\n2', 'temp-zakura-sync-test-1', '1;true']:
            with self.subTest(run_id=run_id), self.assertRaises(ValueError):
                candidate.names(run_id, 'baseline')
        self.assertEqual(candidate.names('123', 'candidate'), ('zakura-genesis-123-candidate', 'zakura-pr-genesis-123-candidate'))
        with self.assertRaises(ValueError): candidate.names('123', 'fleet')

    def test_ownership_ignores_fleet_and_rejects_wrong_attached_volume(self):
        fleet = {'name': 'temp-zakura-sync-test-1'}
        node = dict(name='zakura-genesis-123-baseline', id=1, tags=[candidate.TAG], volume_ids=['disk'])
        disk = dict(name='zakura-pr-genesis-123-baseline', id='disk', tags=[candidate.TAG], droplet_ids=[1])
        with patch.object(candidate, 'do', side_effect=[[fleet, node], [disk]]):
            self.assertEqual(candidate.owned('123')['baseline'], (node, disk))
        for bad_node, bad_disk in [(node | {'tags': []}, disk), (node | {'volume_ids': ['other']}, disk),
                                  (node, disk | {'droplet_ids': [2]}), (node, disk | {'tags': []})]:
            with patch.object(candidate, 'do', side_effect=[[bad_node], [bad_disk]]), self.assertRaises(ValueError):
                candidate.owned('123')

    def test_host_rejects_existing_fleet_before_any_configuration(self):
        with patch.object(host.socket, 'gethostname', return_value='temp-zakura-sync-test-1'), \
             patch.object(host, 'load_controller') as controller, self.assertRaises(ValueError):
            host.configure(dict(launch_run_id='123', leg='candidate'))
        controller.assert_not_called()

    def test_host_renders_real_controller_and_node_configs_for_both_modes(self):
        for mode, minimum_peers in [('dual', 1), ('zakura', 0)]:
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                bundle = root/'bundle'
                bundle.mkdir()
                for name in ['nodes.toml', 'continuous-sync.py']:
                    shutil.copy(ROOT/'deploy/continuous-sync'/name, bundle/name)
                shutil.copytree(ROOT/'deploy/continuous-sync/templates', bundle/'templates')
                def local_path(path):
                    return root/str(path).lstrip('/')
                owner = dict(launch_run_id='123', leg='candidate', mode=mode,
                             source_sha='a'*40, ip='192.0.2.1')
                with patch.object(host, 'ROOT', bundle), patch.object(host, 'Path', side_effect=local_path), \
                     patch.object(host.socket, 'gethostname', return_value='zakura-genesis-123-candidate'), \
                     patch.object(host.subprocess, 'run'):
                    local_path('/etc/systemd/system').mkdir(parents=True)
                    config = host.configure(owner)
                self.assertEqual(config.policy.pinned_sha, owner['source_sha'])
                self.assertEqual(config.policy.p2p_stack, mode)
                self.assertEqual(config.policy.ready_samples, 6)
                self.assertEqual(config.policy.ready_sample_interval_seconds, 30)
                self.assertEqual(str(config.paths.chain_state_dir), '/var/lib/zakura')
                self.assertTrue(local_path(config.paths.wipe_sentinel).exists())
                config.paths = dataclasses.replace(config.paths, **{
                    f.name: local_path(getattr(config.paths, f.name))
                    for f in dataclasses.fields(config.paths)
                })
                sync.render_config(config, root/'run')
                rendered = config.paths.zakurad_config.read_text()
                self.assertNotIn('{{', rendered)
                node = tomllib.loads(rendered)
                self.assertEqual(node['network']['p2p_stack'], mode)
                self.assertEqual(node['health']['min_connected_peers'], minimum_peers)
                self.assertTrue(node['consensus']['vct_fast_sync'])
                self.assertTrue(node['consensus']['checkpoint_sync'])
                self.assertEqual(node['state']['storage_mode'], 'pruned')
                self.assertNotIn('{{', local_path('/etc/systemd/system/zakura.service').read_text())

    def test_rates_match_daily_digest_and_reject_unconfirmed_counts(self):
        for height, seconds in [(0, 1), (3470000, 22697), (3470000, 25800)]:
            state = dict(phase='complete', last_success_end_height=height, last_success_duration_seconds=seconds)
            self.assertIn(f'{candidate.rate(state):.0f} blocks/sec', deploy.completion_run_text(dict(end_height=height, duration=seconds)))
        for change in [dict(phase='syncing'), dict(failed=True), dict(last_success_end_height=None),
                       dict(last_success_end_height=True), dict(last_success_end_height=2**32), dict(last_success_duration_seconds=0)]:
            state = dict(phase='complete', last_success_end_height=3470000, last_success_duration_seconds=22697) | change
            self.assertIsNone(candidate.rate(state))

    def test_one_cycle_exits_without_restart_or_cooldown(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = make_config(Path(tmp))
            with patch.object(sync, 'one_cycle', return_value={'phase': 'complete'}) as cycle, patch.object(sync.time, 'sleep') as sleep:
                self.assertEqual(sync.run_loop(config, Path(tmp)/'config', once=True), 0)
            cycle.assert_called_once(); sleep.assert_not_called()

    def test_one_cycle_failure_does_not_retry_disk_pressure(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = make_config(Path(tmp))
            with patch.object(sync, 'one_cycle', side_effect=sync.DiskPressure('full')) as cycle, \
                 patch.object(sync, 'stop_service'), patch.object(sync, 'cleanup_retention'), \
                 patch.object(sync, 'halt') as halt, patch.object(sync.time, 'sleep') as sleep:
                self.assertEqual(sync.run_loop(config, Path(tmp)/'config', once=True), 1)
            cycle.assert_called_once(); halt.assert_called_once(); sleep.assert_not_called()

    def test_pinned_revision_is_verified_instead_of_following_branch(self):
        with tempfile.TemporaryDirectory() as tmp:
            sha = 'a'*40
            config = make_config(Path(tmp), policy=sync.Policy(pinned_sha=sha))
            with patch.object(sync, 'run', return_value=subprocess.CompletedProcess([], 0, stdout=sha+'\n')) as run:
                self.assertEqual(sync.resolve_sha(config), sha)
                self.assertEqual(run.call_args_list[0].args[0], ['git', 'fetch', '--no-tags', 'origin', sha])
            with patch.object(sync, 'run', return_value=subprocess.CompletedProcess([], 0, stdout='b'*40+'\n')), self.assertRaises(sync.ControllerError):
                sync.resolve_sha(config)


if __name__ == '__main__':
    unittest.main()
