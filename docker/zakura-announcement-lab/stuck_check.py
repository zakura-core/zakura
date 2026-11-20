#!/usr/bin/env python3
"""LAB ONLY: does a miner that kept its own block follow a longer chain built on its rival?

    LAB_SHAPE_RPC=1 [LAB_ZAKURAD=<binary>] stuck_check.py <inst> [trials]

A's 2 MB block is submitted through the shaped link, so B's header reaches A while A's own
submission is still uploading. A then keeps its block, O extends B, and A should reorganize.
The instance must be up on the dual stack and settled.
"""
import sys
import threading
import time

import lab as lablib


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    lab = lablib.Lab(argv[0])
    lab.shape(100, 50)
    for trial in range(int(argv[1]) if len(argv) > 1 else 3):
        lab.wait_same_tip(120)
        block_a = lab.prepare("a", 2_000_000)
        block_b = lab.prepare("b", 0)
        sent = {}
        start = time.time() + 0.3
        threads = [
            threading.Thread(target=lab.submit_at, args=("a", block_a, start, sent)),
            threading.Thread(target=lab.submit_at, args=("b", block_b, start + 0.1, sent)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        time.sleep(8)
        tips = {name: lab.tip(name) for name in lablib.NODES}
        kept_own = tips["a"] == block_a["hash"] and tips["o"] == block_b["hash"]
        lab.rpc("o", "generate", [1])
        began = time.time()
        followed = None
        while time.time() - began < 90:
            if lab.tip("a") == lab.tip("o"):
                followed = round(time.time() - began, 1)
                break
            time.sleep(1)
        print({"trial": trial, "a_kept_own_block_against_b": kept_own,
               "a_followed_longer_chain_after_s": followed}, flush=True)
        if followed is None:
            # Hand A the chain so the next trial starts from one tip.
            height = lab.rpc("o", "getblockcount", [])
            for at in range(height - 3, height + 1):
                try:
                    lab.rpc("a", "submitblock", [lab.rpc("o", "getblock", [str(at), 0])])
                except Exception:
                    pass
        time.sleep(40)


if __name__ == "__main__":
    sys.exit(main())
