"""Archive preservation, source restart, and public-tip readiness contracts."""
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import archive_seed
import network


class ArchiveSeedTests(unittest.TestCase):
    def test_existing_archive_cannot_be_replaced(self):
        with tempfile.TemporaryDirectory() as temporary:
            archive = Path(temporary) / 'seed.tar.zst'
            archive.write_bytes(b'preserved first capture')
            with self.assertRaisesRegex(ValueError, 'immutable'):
                archive_seed.pack_state(Path(temporary), archive)
            self.assertEqual(archive.read_bytes(), b'preserved first capture')

    def document(self):
        return {'selectedProfile': 'public-testnet', 'generatedAt': 100,
                'status': {'network': {'name': 'Testnet', 'activationHeight': 4465026},
                           'nodes': [dict(name=name, height=4559785, hash='a' * 64, healthy=True)
                                     for name in ('one', 'two', 'three')]}}

    def reference(self, document):
        response = io.BytesIO(json.dumps(document).encode())
        with patch.object(archive_seed, 'urlopen', return_value=response), patch.object(archive_seed.time, 'time', return_value=110):
            return archive_seed.public_reference('https://reference.invalid')

    def test_reference_requires_fresh_distinct_agreeing_validators(self):
        self.assertEqual(self.reference(self.document())['height'], 4559785)
        for change in ('stale', 'disagree', 'duplicate', 'wrong network'):
            document = self.document()
            if change == 'stale':
                document['generatedAt'] = 1
            elif change == 'wrong network':
                document['selectedProfile'] = 'other'
            elif change == 'duplicate':
                for row in document['status']['nodes']:
                    row['name'] = 'same'
            else:
                for index, row in enumerate(document['status']['nodes']):
                    row['hash'] = str(index) * 64
            with self.assertRaises(ValueError):
                self.reference(document)

    def test_source_is_restarted_when_copy_fails(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'state').mkdir()
            calls = []

            def command(*args, **kwargs):
                calls.append(args)
                if args[0] == 'cp':
                    archive_seed.interrupt_operation(15, None)

            with patch.object(archive_seed, 'public_reference', return_value={'validators': ['zakura-testnet-eu', 'zakura-testnet-as']}), patch.object(archive_seed, 'verify_archive_history', return_value={}), patch.object(archive_seed, 'command', side_effect=command):
                with self.assertRaisesRegex(archive_seed.OperationCancelled, 'cancelled'):
                    archive_seed.capture(root, root / 'snapshot.tar.zst', 'source.service', 'unused')
            self.assertIn(('systemctl', 'start', 'source.service'), calls)
            self.assertFalse((root / 'snapshot.tar.capture').exists())

    def test_public_sync_requires_two_observations_and_archive_history(self):
        reference = {'height': 4559785, 'hash': 'a' * 64}
        infos = iter([{'blocks': 4559784, 'estimatedheight': 4559785, 'pruned': False},
                      {'blocks': 4559785, 'estimatedheight': 4559785, 'pruned': False},
                      {'blocks': 4559785, 'estimatedheight': 4559785, 'pruned': False}])

        def rpc(port, method, params=None):
            return next(infos) if method == 'getblockchaininfo' else 'a' * 64

        with patch.object(archive_seed, 'public_reference', return_value=reference), patch.object(archive_seed, 'rpc', side_effect=rpc), patch.object(archive_seed, 'verify_archive_history') as history, patch.object(archive_seed.time, 'sleep'):
            self.assertEqual(archive_seed.wait_for_sync('unused', timeout=1), reference)
        history.assert_called_once_with(18432)

    def test_cancellation_is_not_retried_by_preparation_readiness(self):
        with patch.object(archive_seed, 'rpc', side_effect=archive_seed.OperationCancelled):
            with self.assertRaises(archive_seed.OperationCancelled):
                archive_seed.wait_for_rpc('Testnet')
        with patch.object(archive_seed, 'public_reference', side_effect=archive_seed.OperationCancelled):
            with self.assertRaises(archive_seed.OperationCancelled):
                archive_seed.wait_for_sync('unused')

    def test_pruned_nodes_and_missing_old_bodies_are_rejected(self):
        with patch.object(archive_seed, 'rpc', return_value={'pruned': True}):
            with self.assertRaisesRegex(ValueError, 'archive'):
                archive_seed.verify_archive_history(18432)
        with patch.object(archive_seed, 'rpc', side_effect=[{'pruned': False, 'blocks': 4559785}, 'hash', '']):
            with self.assertRaisesRegex(ValueError, 'historical'):
                archive_seed.verify_archive_history(18432)

    def test_public_and_isolated_preparation_never_enable_mining(self):
        import tomllib
        public = tomllib.loads(archive_seed.preparation_config('/dedicated/root'))
        self.assertEqual(public['network']['network'], 'Testnet')
        probe = tomllib.loads(archive_seed.preparation_config('/dedicated/root', dict(name='Probe', magic=[1, 2, 3, 4], checkpoints=[])))
        self.assertEqual(probe['network']['initial_testnet_peers'], ['127.0.0.1:1'])
        for config in (public, probe):
            self.assertEqual(config['state']['storage_mode'], 'archive')
            self.assertFalse(config['mining']['internal_miner'])

    def test_running_pruned_validator_is_unhealthy(self):
        run = dict(network={'name': 'Fork'}, activation_height=4559786)
        params = dict(network='Fork', pqP2shActivationHeight=4559786, nu7ActivationHeight=4465026)
        with patch.object(network, 'rpc', side_effect=[params, {'pruned': True}]):
            with self.assertRaisesRegex(ValueError, 'archive'):
                network.verify_nodes({}, run)

    def test_post_nu7_archive_configuration_is_accepted_and_pruned_seed_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'config.json'
            config = dict(root=str(Path(temporary) / 'deployment'), service_prefix='pq-testnet', revision='a' * 40,
                          seed_height=4559785, seed_finalized_height=4558785, seed_storage_mode='archive',
                          seed_hash='b' * 64, seed_finalized_hash='c' * 64,
                          seed_sha256='d' * 64, binary_sha256='e' * 64)
            path.write_text(json.dumps(config))
            self.assertEqual(network.load_config(path), config)
            config['seed_storage_mode'] = 'pruned'
            path.write_text(json.dumps(config))
            with self.assertRaisesRegex(ValueError, 'archive seed'):
                network.load_config(path)


if __name__ == '__main__':
    unittest.main()
