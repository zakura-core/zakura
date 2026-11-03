### Fixed

- Keep block profile storage accounting, restart recovery, and oldest-first CPU eviction from scanning large symbol metadata records and stalling collection. Preserve fresh CPU profiles after a restart by evicting older runs first. Clarify that stale collector health does not establish that the node is offline.
