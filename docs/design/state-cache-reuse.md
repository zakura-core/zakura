# State cache reuse

Zakura preserves databases from released code. Stable releases through v1.4.0
use format 28.x, so the format-29 upgrade looks only in the version-28 directory.
It does not search older development formats or recover a different major format
stored under that directory. An existing version-29 database takes precedence.

Startup opens the previous database with RocksDB's exclusive lock and publishes
a checkpoint with its original format marker before running the existing
migrations. This avoids moving a database that a released binary is still using.
A locked or invalid source stops startup. A startup lock serializes checkpoint
publication with other writable opens.

The version-28 source remains in place. Checkpoints share immutable files on the
same filesystem, but later writes and compaction can consume additional space.
