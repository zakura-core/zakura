"""Shared CSV decoder for trace validation and benchmark analysis."""

from contextlib import contextmanager
import csv
from datetime import datetime
import gzip
import io
import json
import os
from pathlib import Path
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
# Legacy sync stall snapshots can fill a record with one JSON task list.
csv.field_size_limit(16 * 1024 * 1024)


class TraceInputError(ValueError):
    """The capture cannot supply trustworthy evidence."""


def load_json(text):
    try:
        return json.loads(text)
    except ValueError as error:
        raise TraceInputError("invalid JSON field") from error


# Descriptors opened under the writer lock, keyed by resolved directory.
_SNAPSHOTS = {}


def snapshot_entry(path):
    path = Path(path)
    snapshot = _SNAPSHOTS.get(path.parent.resolve())
    return None if snapshot is None else snapshot["files"].get(path.name)


@contextmanager
def open_trace(path):
    """Open a trace file, reusing the snapshot descriptor when one is held."""
    snapshot = snapshot_entry(path)
    if snapshot is not None:
        with os.fdopen(os.dup(snapshot[0]), "rb") as handle:
            handle.seek(0)
            yield handle
        return
    with open(path, "rb") as handle:
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
    directory = Path(path).resolve()
    if directory in _SNAPSHOTS:
        yield
        return
    files = {}
    try:
        lock = directory / ".trace.lock"
        handle = None
        if lock.exists():
            handle = os.open(lock, os.O_RDONLY)
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
                if not is_snapshot_file(item.name) or not item.is_file():
                    continue
                fd = os.open(item, os.O_RDONLY)
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


def entries(path):
    snapshot = _SNAPSHOTS.get(Path(path).resolve())
    if snapshot is not None:
        return list(snapshot["entries"])
    return sorted(Path(path).iterdir())


def segments(path):
    path = Path(path)
    retained = []
    for item in entries(path.parent):
        # Writers keep plain numeric segments; the sync controller compresses them.
        suffix = item.name.removeprefix(path.name + ".").removesuffix(".gz")
        if item.name.startswith(path.name + ".") and suffix.isascii() and suffix.isdigit():
            retained.append((int(suffix), item))
    retained.sort(reverse=True)
    result = [item for _, item in retained]
    if path.exists():
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


def read_segment(path, table):
    try:
        with open_trace(path) as raw:
            # The writer may append after a snapshot; its recorded size is the cut.
            data = raw.read(file_size(path, raw))
        if Path(path).name.endswith(".gz"):
            data = gzip.decompress(data)
        reader = csv.DictReader(io.StringIO(data.decode("utf-8"), newline=""), strict=True)
        header = validate_header(table, reader.fieldnames)
        for record in reader:
            if None in record or None in record.values():
                raise TraceInputError("CSV row field count does not match header")
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


def read_table(path):
    path = Path(path)
    table = path.name.split(".csv", 1)[0]
    for segment in segments(path):
        yield from read_segment(segment, table)


def read_status(path):
    with open_trace(path) as handle:
        return load_json(handle.read(file_size(path, handle)).decode("utf-8"))
