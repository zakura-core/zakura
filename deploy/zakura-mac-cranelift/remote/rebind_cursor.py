"""Rebind only receipt identity during coordinated deployment or rollback."""
import hashlib
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, '/opt/zakura-mac-verifier')
from common import atomic_json


def rebind_cursor(path, receipt_path):
    stat = path.stat()
    state = json.loads(path.read_text())
    receipt = json.loads(receipt_path.read_text())
    state['receipt_digest'] = hashlib.sha256(json.dumps(receipt, sort_keys=True).encode()).hexdigest()
    atomic_json(path, state, mode=stat.st_mode & 0o777)
    os.chown(path, stat.st_uid, stat.st_gid)


if __name__ == '__main__':
    rebind_cursor(Path('/var/lib/zakura-mac-verifier/cursor.json'),
                  Path('/etc/zakura-mac-verifier/receipt.json'))
