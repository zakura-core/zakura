"""Contracts for isolated resets and recovered activation countdowns."""
import argparse
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import network


class NetworkTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.config = dict(root=str(self.root), service_prefix='pq-testnet',
                           revision='a' * 40, seed_height=4420648,
                           seed_hash='b' * 64, seed_finalized_height=4419648,
                           seed_finalized_hash='d' * 64, miner_address='tmTestMiner',
                           public_peer='pq.valargroup.dev', seed_sha256='c' * 64,
                           seed_network_directory='nu7stagingv3', binary='/opt/zakurad')
        self.run = network.new_run(self.config)

    def test_both_nodes_and_participants_share_the_activation(self):
        import tomllib
        for index in (0, 1):
            config = tomllib.loads(network.node_config(self.config, self.run, index))
            params = config['network']['network']
            self.assertEqual(params['activation_heights']['NU7'], 4420649)
            self.assertEqual(params['pq_p2sh_activation_height'], 4420649)
            self.assertEqual(params['max_block_time_start_height'], 4420655)
            self.assertEqual(params['checkpoints'][1], [4419648, list(bytes.fromhex(self.config['seed_finalized_hash'])[::-1])])
            self.assertEqual(len(params['checkpoints'][0][1]), 32)
            self.assertFalse(config['mining']['internal_miner'])
        participant = tomllib.loads(network.participant_config(self.config, self.run))
        self.assertEqual(participant['network']['network'], params)
        self.assertEqual(participant['network']['initial_testnet_peers'], ['pq.valargroup.dev:18233'])
        self.assertNotIn('mining', participant)

    def test_reset_generations_have_distinct_identity(self):
        other = network.new_run(self.config)
        self.assertNotEqual(other['id'], self.run['id'])
        self.assertNotEqual(other['network'], self.run['network'])
        self.assertEqual(other['activation_height'], self.run['activation_height'])

    def controller(self, phase='countdown'):
        self.run.update(phase=phase, mining_at=300, ready_at=0)
        network.atomic_json(self.root / 'run.json', self.run)
        return network.Controller(self.config)

    def test_read_only_status_never_starts_mining(self):
        controller = self.controller()
        with patch.object(network, 'verify_nodes', return_value=[]), patch.object(network, 'command') as command:
            controller.sample()
        command.assert_not_called()
        self.assertEqual(json.loads((self.root / 'run.json').read_text())['phase'], 'countdown')

    def test_countdown_survives_restart_and_enables_one_named_miner(self):
        controller = self.controller()
        with patch.object(network, 'verify_nodes', return_value=[]), patch.object(network, 'command') as command, patch.object(network.time, 'time', return_value=299):
            controller.sample(advance=True)
            command.assert_not_called()
        with patch.object(network, 'verify_nodes', return_value=[]), patch.object(network, 'command') as command, patch.object(network.time, 'time', return_value=300), patch.object(network, 'wait_for_primary'):
            controller = network.Controller(self.config)
            controller.sample(advance=True)
            controller.sample(advance=True)
            self.assertEqual(command.call_count, 2)
            command.assert_has_calls([unittest.mock.call('systemctl', 'restart', 'pq-testnet-node0.service'), unittest.mock.call('systemctl', 'restart', 'pq-testnet-node1.service')])
        self.assertIn('internal_miner = true', (self.root / 'node0.toml').read_text())
        self.assertFalse((self.root / 'node1.toml').exists())

    def test_parameter_failure_prevents_mining(self):
        controller = self.controller()
        with patch.object(network, 'verify_nodes', side_effect=ValueError('wrong generation')), patch.object(network, 'command') as command:
            sample = controller.sample(advance=True)
        command.assert_not_called()
        self.assertFalse(sample['healthy'])
        self.assertEqual(sample['phase'], 'countdown')

    def test_activation_requires_two_validators_at_the_boundary(self):
        controller = self.controller('mining')
        for heights, expected in [((4420649, 4420648), 'mining'), ((4420649, 4420649), 'active')]:
            rows = [dict(height=h) for h in heights]
            with patch.object(network, 'verify_nodes', return_value=rows), patch.object(network, 'rpc', return_value='d' * 64):
                sample = controller.sample(advance=True)
            self.assertEqual(sample['phase'], expected)

    def test_bad_archive_checksum_does_not_stop_services(self):
        archive = self.root / 'seed.tar.zst'
        archive.write_bytes(b'wrong archive')
        self.config['seed_archive'] = str(archive)
        network.atomic_json(self.root / 'owner.json', {'root': str(self.root), 'service_prefix': 'pq-testnet'})
        with patch.object(network, 'command') as command:
            with self.assertRaisesRegex(ValueError, 'checksum'):
                network.reset(self.config, 300)
        command.assert_not_called()

    def test_archive_rejects_links_and_parent_paths(self):
        for name, kind in [('../escape', tarfile.REGTYPE), ('state/v29/fork/link', tarfile.SYMTYPE)]:
            payload = io.BytesIO()
            with tarfile.open(fileobj=payload, mode='w') as archive:
                member = tarfile.TarInfo(name)
                member.type = kind
                archive.addfile(member)
            process = unittest.mock.MagicMock()
            process.__enter__.return_value = process
            process.stdout = io.BytesIO(payload.getvalue())
            destination = self.root / ('restore' + str(kind))
            with patch.object(network.subprocess, 'Popen', return_value=process):
                with self.assertRaisesRegex(ValueError, 'unsafe|link'):
                    network.restore_archive('unused', destination, 'NewFork')

    def test_two_resets_restore_fresh_nodes_and_preserve_the_seed(self):
        import hashlib
        binary = self.root / 'zakurad'
        binary.write_bytes(b'compiled node')
        self.config.update(binary=str(binary), binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(), seed_archive='preserved')
        network.atomic_json(self.root / 'owner.json', {'root': str(self.root), 'service_prefix': 'pq-testnet'})
        def restore(archive, destination, name):
            destination.mkdir()
            (destination / 'identity').write_text(name)
        with patch.object(network, 'verify_archive'), patch.object(network, 'restore_archive', side_effect=restore), patch.object(network, 'verify_nodes', return_value=[]), patch.object(network, 'command') as command:
            first = network.reset(self.config, 300)
            second = network.reset(self.config, 300)
        self.assertNotEqual(first['network'], second['network'])
        self.assertEqual(second['phase'], 'countdown')
        self.assertTrue((self.root / 'history' / (first['id'] + '.json')).exists())
        self.assertEqual((self.root / 'node1/cache/identity').read_text(), second['network']['name'])
        self.assertFalse(any(call.args[:2] == ('systemctl', 'restart') for call in command.call_args_list))

    def test_failed_restore_stops_both_nodes_without_starting_mining(self):
        import hashlib
        binary = self.root / 'zakurad'
        binary.write_bytes(b'compiled node')
        self.config.update(binary=str(binary), binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(), seed_archive='preserved')
        network.atomic_json(self.root / 'owner.json', {'root': str(self.root), 'service_prefix': 'pq-testnet'})
        with patch.object(network, 'verify_archive'), patch.object(network, 'restore_archive', side_effect=ValueError('corrupt archive')), patch.object(network, 'command') as command:
            with self.assertRaisesRegex(ValueError, 'corrupt archive'):
                network.reset(self.config, 300)
        self.assertEqual(json.loads((self.root / 'run.json').read_text())['phase'], 'failed')
        for index in (0, 1):
            command.assert_any_call('systemctl', 'stop', f'pq-testnet-node{index}.service')
        self.assertFalse(any(call.args[:2] == ('systemctl', 'restart') for call in command.call_args_list))

    def test_reorg_withdraws_activation_and_refreshes_its_hash(self):
        controller = self.controller('active')
        self.run['activation_hash'] = 'old hash'
        network.atomic_json(self.root / 'run.json', self.run)
        with patch.object(network, 'verify_nodes', return_value=[dict(height=4420648)]):
            sample = controller.sample(advance=True)
        self.assertEqual(sample['phase'], 'mining')
        self.assertNotIn('activation_hash', sample)
        with patch.object(network, 'verify_nodes', return_value=[dict(height=4420649)]), patch.object(network, 'rpc', return_value='replacement hash'):
            sample = controller.sample(advance=True)
        self.assertEqual(sample['activation_hash'], 'replacement hash')

    def test_node_start_gate_rejects_old_generation_and_early_mining(self):
        self.controller()
        path = self.root / 'node0.toml'
        path.write_text(network.node_config(self.config, self.run, 0, mining=True))
        with patch.object(network.time, 'time', return_value=299):
            with self.assertRaisesRegex(ValueError, 'countdown'):
                network.check_node(self.config, 0)
        with patch.object(network.time, 'time', return_value=300):
            network.check_node(self.config, 0)
        other = network.new_run(self.config)
        path.write_text(network.node_config(self.config, other, 0))
        with self.assertRaisesRegex(ValueError, 'current run'):
            network.check_node(self.config, 0)
        self.run['phase'] = 'failed'
        network.atomic_json(self.root / 'run.json', self.run)
        path.write_text(network.node_config(self.config, self.run, 0))
        with self.assertRaisesRegex(ValueError, 'not permitted'):
            network.check_node(self.config, 0)

    def test_restore_renames_versioned_state_and_unversioned_backup(self):
        payload = io.BytesIO()
        with tarfile.open(fileobj=payload, mode='w') as archive:
            for name in ('state/v29/oldnet/', 'non_finalized_state/oldnet/'):
                member = tarfile.TarInfo(name)
                member.type = tarfile.DIRTYPE
                archive.addfile(member)
        process = unittest.mock.MagicMock()
        process.__enter__.return_value = process
        process.stdout = io.BytesIO(payload.getvalue())
        process.wait.return_value = 0
        destination = self.root / 'restored'
        with patch.object(network.subprocess, 'Popen', return_value=process):
            network.restore_archive('unused', destination, 'NewFork')
        self.assertTrue((destination / 'state/v29/newfork').is_dir())
        self.assertTrue((destination / 'non_finalized_state/newfork').is_dir())
        self.assertFalse((destination / 'non_finalized_state/oldnet').exists())

    def test_intervals_exclude_the_historical_seed_time_jump(self):
        controller = self.controller('mining')
        def response(port, method, params):
            if method == 'getblockhash':
                return str(params[0])
            height = int(params[0])
            return {'time': 100 if height < 4420649 else 2000000 + (height - 4420649) * 451, 'previousblockhash': str(height - 1)}
        with patch.object(network, 'rpc', side_effect=response):
            blocks, interval = controller.recent_blocks({'height': 4420650, 'hash': '4420650'}, 4420649)
        self.assertEqual(len(blocks), 6)
        self.assertEqual(interval, 451)

    def test_miner_readiness_requires_matching_activation_parameters(self):
        params = dict(network=self.run['network']['name'], pqP2shActivationHeight=4420649, nu7ActivationHeight=4420649)
        with patch.object(network, 'rpc', return_value=params):
            network.wait_for_primary(self.run)
        params['nu7ActivationHeight'] = 4465026
        with patch.object(network, 'rpc', return_value=params):
            with self.assertRaisesRegex(ValueError, 'does not match'):
                network.wait_for_primary(self.run)

    def test_recent_blocks_follow_the_sampled_tip_through_parent_links(self):
        controller = self.controller('active')
        headers = {
            'tip': {'time': 300, 'previousblockhash': 'parent'},
            'parent': {'time': 200, 'previousblockhash': 'genesis'},
            'genesis': {'time': 100, 'previousblockhash': 'none'},
        }
        def response(port, method, params):
            self.assertEqual(method, 'getblockheader')
            return headers[params[0]]
        with patch.object(network, 'rpc', side_effect=response):
            blocks, interval = controller.recent_blocks({'height': 2, 'hash': 'tip'}, 1)
        self.assertEqual([block['hash'] for block in blocks], ['genesis', 'parent', 'tip'])
        self.assertEqual(interval, 100)

    def test_activation_delay_is_bounded(self):
        self.assertEqual(network.activation_delay('5m'), 300)
        for invalid in ('0m', '-1s', '1h', '999999m'):
            with self.assertRaises(argparse.ArgumentTypeError):
                network.activation_delay(invalid)


if __name__ == '__main__':
    unittest.main()
