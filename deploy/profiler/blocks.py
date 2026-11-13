"""When blocks landed on the node, read from the files the node already writes.

`FileFollower` reads only the bytes appended to a file since the last call, so following
a multi-gigabyte node log costs the size of what was written, not the size of the file.
`announcements` picks the block announcement lines out of the node log; each one marks
the moment a newly committed block was handed to gossip.
"""

import calendar
import os
import re

ANNOUNCEMENT_MARKER = b"sending block announcement"
ANNOUNCEMENT = re.compile(
    rb"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?Z\s.*"
    rb"sending block announcement.*?height=Height\((\d+)\).*?hash=block::Hash\(\"([0-9a-f]{64})\"\)"
)
# On the first read, look this far back so a restart of the profiler still knows the
# blocks of the last few minutes.
INITIAL_LOOKBACK_BYTES = 8 * 1024 * 1024
MAX_READ_BYTES = 256 * 1024 * 1024


class FileFollower:
    """Reads the complete lines appended to a file since the previous read."""

    def __init__(self, path, lookback_bytes=INITIAL_LOOKBACK_BYTES):
        self.path = path
        self._lookback_bytes = lookback_bytes
        self._offset = None
        self._inode = None
        self._partial = b""

    def read_new_lines(self):
        """The lines completed since the last call, without their newlines.

        A file that was replaced or truncated is read again from its start. A missing
        file yields nothing and is retried on the next call.
        """
        try:
            with open(self.path, "rb") as handle:
                stat = os.fstat(handle.fileno())
                if self._offset is None:
                    self._offset = max(0, stat.st_size - self._lookback_bytes)
                    # Start mid-file on a line boundary: drop the first, partial line.
                    skip_first = self._offset > 0
                elif stat.st_ino != self._inode or stat.st_size < self._offset:
                    self._offset, self._partial, skip_first = 0, b"", False
                else:
                    skip_first = False
                self._inode = stat.st_ino
                handle.seek(self._offset)
                data = handle.read(min(stat.st_size - self._offset, MAX_READ_BYTES))
        except OSError:
            return []
        self._offset += len(data)
        lines = (self._partial + data).split(b"\n")
        self._partial = lines.pop()
        if skip_first and lines:
            lines.pop(0)
        return lines


def parse_announcement(line):
    """`(unix_time, height, hash)` for a block announcement log line, else `None`."""
    if ANNOUNCEMENT_MARKER not in line:
        return None
    match = ANNOUNCEMENT.match(line)
    if match is None:
        return None
    year, month, day, hour, minute, second = (int(match.group(i)) for i in range(1, 7))
    fraction = match.group(7) or b"0"
    when = calendar.timegm((year, month, day, hour, minute, second)) + int(fraction) / (
        10 ** len(fraction)
    )
    return when, int(match.group(8)), match.group(9).decode()


def announcements(lines):
    """The block announcements among `lines`, oldest first."""
    found = []
    for line in lines:
        parsed = parse_announcement(line)
        if parsed is not None:
            found.append(parsed)
    return found
