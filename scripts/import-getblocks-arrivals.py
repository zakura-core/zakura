#!/usr/bin/env python3
"""Import complete decode sessions, without claiming service-lifecycle coverage."""

import argparse
import hashlib
import json
from pathlib import Path


START = "block_decode_session_started"
MESSAGE = "block_message_received"
FINISH = "block_decode_session_finished"


class IncompleteCapture(ValueError):
    """The trace cannot establish a complete sequence of decoded arrivals."""


def integer(row, name, maximum=2**64 - 1):
    value = row.get(name)
    if type(value) is not int or not 0 <= value <= maximum:
        raise IncompleteCapture(f"invalid {name}: {value!r}")
    return value


def import_arrivals(lines):
    """Preserve decode order and reconnect identity; reject missing session rows.

    Timestamps are emitter-local monotonic microseconds. Each input must contain
    one process's block-sync table. Sequence numbers include non-GetBlocks
    messages so losing those rows also invalidates the capture.
    """
    process = None
    peers = {}
    sessions = {}
    requests = []
    source_hash = hashlib.sha256()

    for line_number, line in enumerate(lines, 1):
        source_hash.update(line)
        try:
            row = json.loads(line)
            if not isinstance(row, dict):
                raise IncompleteCapture("trace row is not an object")
            event = row.get("event")
            if event not in (START, MESSAGE, FINISH):
                continue
            current_process = row.get("process_trace_id")
            if not isinstance(current_process, str) or not current_process:
                raise IncompleteCapture("missing process identity")
            if process is None:
                process = current_process
            elif process != current_process:
                raise IncompleteCapture("split process restarts into separate captures")
            peer = row.get("peer")
            if not isinstance(peer, str) or not peer:
                raise IncompleteCapture("missing peer identity")
            generation = integer(row, "session_id")
            sequence = integer(row, "message_sequence")
            timestamp = integer(row, "ts")
            key = (peer, generation)
            if event == START:
                if integer(row, "capture_version") != 1:
                    raise IncompleteCapture("unsupported decode capture version")
                if key in sessions or sequence != 0:
                    raise IncompleteCapture("duplicate or nonempty session start")
                peer_index = peers.setdefault(peer, len(peers))
                sessions[key] = {
                    "peer": peer_index,
                    "session": len(sessions),
                    "start_us": timestamp,
                    "last_us": timestamp,
                    "messages": 0,
                    "end_us": None,
                }
                continue
            session = sessions.get(key)
            if session is None or session["end_us"] is not None:
                raise IncompleteCapture("message outside an open decode session")
            if timestamp < session["last_us"]:
                raise IncompleteCapture("session clock moved backwards")
            session["last_us"] = timestamp
            if event == FINISH:
                if integer(row, "capture_version") != 1:
                    raise IncompleteCapture("unsupported decode capture version")
                if sequence != session["messages"]:
                    raise IncompleteCapture("session tail messages are missing")
                session["end_us"] = timestamp
                continue
            if sequence != session["messages"] + 1:
                raise IncompleteCapture("missing or duplicate decoded message")
            session["messages"] = sequence
            kind = row.get("kind")
            if not isinstance(kind, str) or not kind:
                raise IncompleteCapture("missing decoded message kind")
            if kind == "get_blocks":
                requests.append({
                    "peer": session["peer"],
                    "session": session["session"],
                    "message_sequence": sequence,
                    "decoded_us": timestamp,
                    "start_height": integer(row, "range_start", 2**32 - 1),
                    "count": integer(row, "range_count", 2**32 - 1),
                })
        except (ValueError, TypeError) as error:
            raise IncompleteCapture(f"line {line_number}: {error}") from error

    if not sessions:
        raise IncompleteCapture("no decode sessions; older kind-only traces cannot be imported")
    if any(session["end_us"] is None for session in sessions.values()):
        raise IncompleteCapture("unfinished decode session or missing session footer")
    return {
        "version": 1,
        "observation_boundary": "peer_routine_decode",
        "time_unit": "microseconds",
        "source_sha256": source_hash.hexdigest(),
        "observed_sessions_complete": True,
        "capture_loss_verified": False,
        "service_lifecycles_complete": False,
        "peers": len(peers),
        "sessions": [
            {key: value for key, value in session.items() if key != "last_us"}
            for session in sessions.values()
        ],
        "requests": requests,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trace", type=Path, help="one process's block_sync.jsonl")
    parser.add_argument("output", type=Path, help="new arrival artifact; existing files are preserved")
    args = parser.parse_args()
    try:
        with args.trace.open("rb") as source:
            result = import_arrivals(source)
        with args.output.open("x") as output:
            json.dump(result, output, indent=2)
            output.write("\n")
    except (OSError, IncompleteCapture) as error:
        parser.exit(1, f"capture import failed: {error}\n")


if __name__ == "__main__":
    main()
