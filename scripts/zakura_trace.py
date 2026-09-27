"""Shared bounded CSV decoder for trace validation and benchmark analysis."""

from contextlib import contextmanager
import csv
from dataclasses import dataclass
from datetime import datetime
import gzip
import io
import json
import math
import os
from pathlib import Path
import stat
import time


SCHEMA_PATH = Path(__file__).parent.parent / "crates/zakura-jsonl-trace/schema.json"
if not SCHEMA_PATH.is_file():
    SCHEMA_PATH = Path(__file__).with_name("schema.json")
SCHEMA = json.loads(SCHEMA_PATH.read_text())
HEADERS = {table: tuple(SCHEMA["envelope"] + columns + ["extra"])
           for table, columns in SCHEMA["tables"].items()}
INTEGER_FIELDS = set(SCHEMA["integer_fields"])
BOOLEAN_FIELDS = set(SCHEMA["boolean_fields"])
JSON_FIELDS = set(SCHEMA["json_fields"])
TEXT_FIELDS = set().union(*HEADERS.values()) - INTEGER_FIELDS - BOOLEAN_FIELDS - JSON_FIELDS - {"extra"}
TYPED_FIELDS = INTEGER_FIELDS | BOOLEAN_FIELDS | JSON_FIELDS
MAX_U64 = 2**64 - 1
MAX_RECORD_BYTES = 1024 * 1024
# Legacy sync stall snapshots can fill a record with one JSON task list.
MAX_FIELD_BYTES = MAX_RECORD_BYTES
MAX_STATUS_BYTES = 64 * 1024
MAX_JSON_DEPTH = 64
csv.field_size_limit(MAX_FIELD_BYTES)


class TraceInputError(ValueError):
    """The capture cannot supply trustworthy evidence."""


@dataclass
class Budget:
    bytes_left: int = 256 * 1024 * 1024
    rows_left: int = 500_000
    files_left: int = 256
    entries_left: int = 4096

    def file(self, size):
        if size > self.bytes_left or self.files_left <= 0:
            raise TraceInputError("trace byte/file budget exceeded")
        self.bytes_left -= size
        self.files_left -= 1

    def row(self):
        if self.rows_left <= 0:
            raise TraceInputError("trace row budget exceeded")
        self.rows_left -= 1


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise TraceInputError("duplicate JSON key")
        result[key] = value
    return result


def finite_float(text):
    value = float(text)
    if not math.isfinite(value):
        raise TraceInputError("nonfinite JSON number")
    return value


def invalid_constant(_):
    raise TraceInputError("nonfinite JSON number")


def load_json(text):
    depth = 0
    quoted = escaped = False
    for char in text:
        if escaped:
            escaped = False
        elif quoted and char == "\\":
            escaped = True
        elif char == '"':
            quoted = not quoted
        elif not quoted:
            if char in "[{":
                depth += 1
                if depth > MAX_JSON_DEPTH:
                    raise TraceInputError("JSON nesting budget exceeded")
            elif char in "]}":
                depth -= 1
    try:
        return json.loads(text, object_pairs_hook=unique_object, parse_float=finite_float, parse_constant=invalid_constant)
    except (ValueError, RecursionError) as error:
        raise TraceInputError("invalid or ambiguous JSON field") from error


# Descriptors opened under the writer lock, keyed by resolved directory.
_SNAPSHOTS = {}


def trusted_directory(path):
    """Resolve an operator-supplied directory once; symlinked ancestors are allowed."""
    return Path(os.path.realpath(path))


def snapshot_entry(path):
    path = Path(path)
    snapshot = _SNAPSHOTS.get(trusted_directory(path.parent))
    return None if snapshot is None else snapshot["files"].get(path.name)


@contextmanager
def regular_file(path):
    """Open a trace entry without following a symlink or blocking on a special file."""
    if os.name != "posix":
        raise TraceInputError("secure trace import requires POSIX file descriptors")
    snapshot = snapshot_entry(path)
    if snapshot is not None:
        with os.fdopen(os.dup(snapshot[0]), "rb") as handle:
            handle.seek(0)
            yield handle
        return
    path = Path(path)
    parent = os.open(trusted_directory(path.parent), os.O_RDONLY | os.O_DIRECTORY)
    try:
        fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
    finally:
        os.close(parent)
    with os.fdopen(fd, "rb") as handle:
        if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
            raise TraceInputError("trace entry must be a regular file")
        yield handle


def file_size(path, handle):
    """Return the snapshot size of `path`, or its current size outside a snapshot."""
    snapshot = snapshot_entry(path)
    return snapshot[1] if snapshot is not None else os.fstat(handle.fileno()).st_size


def is_snapshot_file(name):
    return ".csv" in name or (name.startswith("capture-") and name.endswith(".json"))


@contextmanager
def locked_directory(path):
    """Snapshot a trace directory without stalling the node's writer.

    The writer holds `.trace.lock` for each batch and drops the batch after two
    seconds. This holds a shared lock only while it lists the directory and
    opens descriptors. Trace files are append-only, and renames keep open
    descriptors valid, so reads after release see the recorded sizes.
    """
    import fcntl
    directory = trusted_directory(path)
    if directory in _SNAPSHOTS:
        yield
        return
    files = {}
    try:
        lock = directory / ".trace.lock"
        handle = None
        if os.path.lexists(lock):
            handle = open_entry(directory, lock.name)
            deadline = time.monotonic() + 2
            while True:
                try:
                    fcntl.flock(handle, fcntl.LOCK_SH | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        os.close(handle)
                        raise TraceInputError("trace snapshot lock timed out")
                    time.sleep(0.01)
        try:
            listing = entries(directory)
            for item in listing:
                if not is_snapshot_file(item.name):
                    continue
                try:
                    fd = open_entry(directory, item.name)
                except TraceInputError:
                    continue
                files[item.name] = (fd, os.fstat(fd).st_size)
        finally:
            if handle is not None:
                os.close(handle)
        _SNAPSHOTS[directory] = {"entries": listing, "files": files}
        yield
    finally:
        _SNAPSHOTS.pop(directory, None)
        for fd, _ in files.values():
            os.close(fd)


def open_entry(directory, name):
    """Open a regular file in `directory` without following a final symlink."""
    parent = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
    except OSError as error:
        raise TraceInputError(f"cannot open trace entry {name}: {type(error).__name__}") from error
    finally:
        os.close(parent)
    if not stat.S_ISREG(os.fstat(fd).st_mode):
        os.close(fd)
        raise TraceInputError("trace entry must be a regular file")
    return fd


def entries(path, budget=None):
    if Path(path).is_symlink():
        raise TraceInputError("trace directory must not be a symlink")
    snapshot = _SNAPSHOTS.get(trusted_directory(path))
    if snapshot is not None:
        listing = snapshot["entries"]
        if budget is not None:
            budget.entries_left -= len(listing)
            if budget.entries_left < 0:
                raise TraceInputError("trace discovery budget exceeded")
        return list(listing)
    result = []
    with os.scandir(path) as iterator:
        for entry in iterator:
            if budget is not None:
                budget.entries_left -= 1
                if budget.entries_left < 0:
                    raise TraceInputError("trace discovery budget exceeded")
            if len(result) >= 1024:
                raise TraceInputError("trace directory entry budget exceeded")
            result.append(Path(entry.path))
    return sorted(result)


def segments(path, budget=None):
    path = Path(path)
    retained = []
    for item in entries(path.parent, budget):
        # Writers keep plain numeric segments; the sync controller compresses them.
        suffix = item.name.removeprefix(path.name + ".").removesuffix(".gz")
        if item.name.startswith(path.name + ".") and suffix.isascii() and suffix.isdigit():
            if len(suffix) > 6:
                raise TraceInputError("invalid trace segment number")
            retained.append((int(suffix), item))
    retained.sort(reverse=True)
    result = [item for _, item in retained]
    if os.path.lexists(path):
        result.append(path)
    return result


def decode_field(name, value):
    if name not in TYPED_FIELDS:
        return value
    # Fast paths for the canonical forms the writer emits; JSON handles the rest.
    if (name in INTEGER_FIELDS and len(value) <= 20 and value.isascii() and value.isdigit()
            and (value == "0" or value[0] != "0")):
        return int(value)
    if name in BOOLEAN_FIELDS and value in ("true", "false"):
        return value == "true"
    return load_json(value)


def validate_header(table, header):
    if table not in HEADERS or tuple(header or ()) != HEADERS[table]:
        raise TraceInputError(f"CSV header does not match the {table} schema")
    return HEADERS[table]


def validate_row(row):
    if not isinstance(row.get("event"), str) or not row["event"]:
        raise TraceInputError("trace event must be a nonempty string")
    # Extra fields can carry fields declared by another table. Validate them too.
    for name, value in row.items():
        if name in INTEGER_FIELDS:
            if type(value) is not int or not 0 <= value <= MAX_U64:
                raise TraceInputError(f"{name} must be an unsigned 64-bit integer")
        elif name in BOOLEAN_FIELDS:
            if type(value) is not bool:
                raise TraceInputError(f"{name} must be a boolean")
        elif name in TEXT_FIELDS and not isinstance(value, str):
            raise TraceInputError(f"{name} must be a string")
    if row.get("trace_version") != 2:
        raise TraceInputError("trace requires version 2 process-wide clocks")
    if not row.get("process_trace_id") or not row.get("node") or "ts" not in row:
        raise TraceInputError("trace envelope is incomplete")
    wall = row.get("wall_ts", "")
    try:
        timestamp = datetime.fromisoformat(wall.replace("Z", "+00:00"))
        if timestamp.utcoffset() is None:
            raise ValueError("timezone missing")
    except ValueError as error:
        raise TraceInputError("wall_ts must be an absolute RFC 3339 timestamp") from error
    for field in SCHEMA["required_event_fields"].get(row["event"], []):
        if field not in row:
            raise TraceInputError(f"{row['event']} requires {field}")
    return row


def read_segment(path, table, budget):
    try:
        with regular_file(path) as raw:
            size = file_size(path, raw)
            budget.file(size)
            if snapshot_entry(path) is not None:
                # The writer may append after the snapshot; its size is the cut.
                data = raw.read(size)
            else:
                # The descriptor's initial size bounds growth by a noncooperating writer.
                data = raw.read(size + 1)
            if len(data) != size:
                raise TraceInputError("trace changed while reading")
        if Path(path).name.endswith(".gz"):
            data = gzip.GzipFile(fileobj=io.BytesIO(data)).read(budget.bytes_left + 1)
            if len(data) > budget.bytes_left:
                raise TraceInputError("trace byte/file budget exceeded")
            budget.bytes_left -= len(data)
        reader = csv.DictReader(io.StringIO(data.decode("utf-8"), newline=""), strict=True)
        header = validate_header(table, reader.fieldnames)
        for record in reader:
            budget.row()
            if None in record or None in record.values():
                raise TraceInputError("CSV row field count does not match header")
            # A character encodes to at most four UTF-8 bytes.
            if (sum(map(len, record.values())) * 4 > MAX_RECORD_BYTES
                    and sum(len(value.encode("utf-8")) for value in record.values()) > MAX_RECORD_BYTES):
                raise TraceInputError("trace record budget exceeded")
            row = {}
            extra = {}
            for name, value in record.items():
                if not value:
                    continue
                if name == "extra":
                    extra = load_json(value)
                else:
                    row[name] = decode_field(name, value)
            if not isinstance(extra, dict) or set(header).intersection(extra):
                raise TraceInputError("CSV extra must be an object with no declared columns")
            row.update(extra)
            yield validate_row(row)
    except (OSError, EOFError, UnicodeError, csv.Error) as error:
        raise TraceInputError(f"cannot decode trace segment {Path(path).name}: {type(error).__name__}") from error


def read_table(path, budget=None):
    path = Path(path)
    table = path.name.split(".csv", 1)[0]
    budget = budget if budget is not None else Budget()
    for segment in segments(path, budget):
        yield from read_segment(segment, table, budget)


def read_status(path, budget):
    with regular_file(path) as handle:
        size = file_size(path, handle)
        if size > MAX_STATUS_BYTES:
            raise TraceInputError("capture status budget exceeded")
        budget.file(size)
        return load_json(handle.read(size + 1).decode("utf-8"))
