PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA foreign_keys=ON;
PRAGMA auto_vacuum=INCREMENTAL;
PRAGMA wal_autocheckpoint=256;
PRAGMA journal_size_limit=2097152;
PRAGMA cache_size=-8192;
PRAGMA user_version=1;
CREATE TABLE IF NOT EXISTS runs (
 id TEXT PRIMARY KEY, metadata TEXT NOT NULL, utc_ms INTEGER NOT NULL,
 seen_ms INTEGER NOT NULL, sequence INTEGER NOT NULL DEFAULT 0,
 gaps INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0,
 dropped INTEGER NOT NULL DEFAULT 0, transport_dropped INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS attempts (
 run TEXT NOT NULL REFERENCES runs(id), attempt INTEGER NOT NULL,
 hash TEXT, height INTEGER, mode TEXT, transactions INTEGER,
 start_us INTEGER, end_us INTEGER, utc_ms INTEGER,
 outcome TEXT, dropped INTEGER NOT NULL DEFAULT 0, expected_spans INTEGER,
 received_spans INTEGER NOT NULL DEFAULT 0, expired INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(run,attempt)
);
CREATE INDEX IF NOT EXISTS recent ON attempts(run,mode,outcome,utc_ms DESC);
CREATE INDEX IF NOT EXISTS slow ON attempts(run,mode,outcome,end_us-start_us DESC);
CREATE INDEX IF NOT EXISTS hashes ON attempts(hash);
CREATE INDEX IF NOT EXISTS heights ON attempts(height);
CREATE TABLE IF NOT EXISTS chunks (
 id TEXT PRIMARY KEY, bytes INTEGER NOT NULL, created_ms INTEGER NOT NULL,
 deleting INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS details (
 run TEXT NOT NULL, attempt INTEGER NOT NULL, chunk TEXT NOT NULL REFERENCES chunks(id) ON DELETE CASCADE,
 PRIMARY KEY(run,attempt,chunk),
 FOREIGN KEY(run,attempt) REFERENCES attempts(run,attempt) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS chunk_details ON details(chunk);
CREATE TABLE IF NOT EXISTS cpu (
 id TEXT PRIMARY KEY, run TEXT NOT NULL REFERENCES runs(id),
 start_us INTEGER NOT NULL, end_us INTEGER NOT NULL,
 samples INTEGER NOT NULL, bytes INTEGER NOT NULL, metadata TEXT NOT NULL,
 deleting INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS cpu_window ON cpu(run,start_us,end_us);
CREATE TABLE IF NOT EXISTS status (
 id INTEGER PRIMARY KEY CHECK(id=1), updated_ms INTEGER NOT NULL,
 errors INTEGER NOT NULL, budget INTEGER NOT NULL, used INTEGER NOT NULL,
 discarded_spans INTEGER NOT NULL
);
