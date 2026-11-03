"""Check the destination binary before replacing the running node."""
import hashlib
from pathlib import Path
import sys

with Path(sys.argv[1]).open('rb') as stream:
    if hashlib.file_digest(stream, 'sha256').hexdigest() != sys.argv[2]:
        raise ValueError('candidate binary digest differs')
