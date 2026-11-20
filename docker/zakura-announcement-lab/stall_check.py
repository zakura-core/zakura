#!/usr/bin/env python3
"""LAB ONLY: how long does O stay on the old tip while A withholds an announced block?

    stall_check.py <inst> <delay-ms> [trials]

A announces a 2 MB block and sits on its body for <delay-ms>. Honest B releases an empty block
600 ms later. O's tip is polled every 0.5 s. The instance must be up and settled.
"""
import sys
import threading
import time

import lab as lablib


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    lab = lablib.Lab(argv[0])
    delay = int(argv[1])
    lab.shape(100, 50)
    for trial in range(int(argv[2]) if len(argv) > 2 else 2):
        lab.reconcile()
        time.sleep(30)
        lab.knob("a", "body_delay_ms", delay)
        block_a = lab.prepare("a", 2_000_000)
        block_b = lab.prepare("b", 0)
        sent = {}
        start = time.time() + 0.3
        threads = [
            threading.Thread(target=lab.submit_at, args=("a", block_a, start, sent)),
            threading.Thread(target=lab.submit_at, args=("b", block_b, start + 0.6, sent)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        moved = None
        while time.time() - start < delay / 1000 + 90:
            tip = lab.tip("o")
            if tip in (block_a["hash"], block_b["hash"]):
                moved = ("a" if tip == block_a["hash"] else "b", round(time.time() - start, 1))
                break
            time.sleep(0.5)
        print({"trial": trial, "body_delay_ms": delay, "o_left_old_tip": moved}, flush=True)
    lab.knob("a", "body_delay_ms", None)


if __name__ == "__main__":
    sys.exit(main())
