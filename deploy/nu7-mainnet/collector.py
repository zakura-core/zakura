#!/usr/bin/env python3
"""NU7 mainnet developer dashboard collector.

Reads one local zakurad node and serves a public page plus a JSON API:

1. The metrics loop scrapes the node's Prometheus endpoint every few seconds
   and attributes the per-block fee, subsidy and operation counters to blocks
   (`nu7_chain.attribute_counters`), remembering the last snapshot in the
   store's `meta` table.
2. The block loop polls `getblockchaininfo`, detects reorgs by re-checking the
   last stored hash, and stores every new best-chain block from `getblock`
   with its subsidy, its miner (from the coinbase transaction), the difficulty
   consensus required for it (`nu7_daa.next_threshold`, a mismatch is an
   event) and the tip's NSM balance. After each new tip it refreshes the
   difficulty model: hashrate, the next block's difficulty, the expected block
   time and a projection of difficulty from current data; the first tip also
   freezes a projection from the activation height (`meta.projection_fixed`).
3. The sample loop records mempool, peer, latency and node-health samples.
4. The HTTP server answers `/` (the page), `/v1/dashboard` (status plus the
   series for a time window) and `/healthz`, on localhost behind Caddy.

Only the Python stdlib is used. Every node access goes through `NodeSources`,
so tests drive the same loops with a fake node.
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import nu7_chain as chain
import nu7_daa as daa
import nu7_miners as miners
from nu7_store import Store

LOG = logging.getLogger("nu7-mainnet")

METRICS_READ_LIMIT = 64 * 1024 * 1024
RPC_TIMEOUT_SECONDS = 20
METRICS_TIMEOUT_SECONDS = 20
MAX_BLOCKS_PER_PASS = 500
MAX_REORG_WALK = 100
RAW_MEMPOOL_MAX_TX = 5000
RAW_MEMPOOL_MAX_SECONDS = 2.0
RECENT_BLOCKS = 8
INTERVAL_SAMPLE_BLOCKS = 300
# Two weeks of 25 s blocks is far more; 2016 blocks is ~42 h before NU7, the miner baseline.
DEFAULT_BACKFILL = 2016
MINER_BASELINE_BLOCKS = 2016
MINER_RECENT_BLOCKS = 300
ANNOTATE_PER_PASS = 100
HASHRATE_BLOCKS = 120
PROJECTION_ROLLING_BLOCKS = 1500
PROJECTION_FIXED_BLOCKS = 3000
FIXED_HASHRATE_BLOCKS = 1008
MONTE_CARLO_RUNS = 300
PROJECTION_DETAIL_BLOCKS = 300
PROJECTION_THIN_STEP = 10
PAYLOAD_CACHE_SECONDS = 5.0
FRESHNESS_SECONDS = 60
WINDOWS = {"1h": 3600, "6h": 6 * 3600, "24h": 24 * 3600, "7d": 7 * 24 * 3600}
MAX_CONCURRENT_REQUESTS = 16
REQUEST_TIMEOUT_SECONDS = 10
RATE_LIMIT = 120
RATE_WINDOW = 60.0
RATE_CLIENT_LIMIT = 4096
TRUSTED_PROXIES = {"127.0.0.1", "::1"}


class NodeSources:
    """The one local node the collector reads: its JSON-RPC, metrics endpoint and systemd unit."""

    def __init__(self, rpc_url, metrics_url, service):
        self.rpc_url = rpc_url
        self.metrics_url = metrics_url
        self.service = service

    def rpc(self, method, params=None):
        body = json.dumps({
            "jsonrpc": "2.0", "id": "nu7-mainnet", "method": method, "params": list(params or []),
        }).encode()
        request = urllib.request.Request(
            self.rpc_url, data=body, headers={"Content-Type": "application/json"}, method="POST",
        )
        with urllib.request.urlopen(request, timeout=RPC_TIMEOUT_SECONDS) as response:
            payload = json.loads(response.read().decode())
        if payload.get("error"):
            raise RuntimeError(f"{method}: {payload['error']}")
        return payload.get("result")

    def metrics_text(self):
        with urllib.request.urlopen(self.metrics_url, timeout=METRICS_TIMEOUT_SECONDS) as response:
            return response.read(METRICS_READ_LIMIT).decode(errors="replace")

    def service_status(self):
        """ActiveState, MainPID and NRestarts of the node's unit, or None without systemd."""
        try:
            output = subprocess.run(
                ["systemctl", "show", self.service, "-p", "ActiveState", "-p", "MainPID", "-p", "NRestarts"],
                capture_output=True, text=True, timeout=5, check=False,
            ).stdout
        except (OSError, subprocess.SubprocessError):
            return None
        values = dict(line.split("=", 1) for line in output.splitlines() if "=" in line)
        try:
            return {
                "active_state": values.get("ActiveState"),
                "pid": int(values.get("MainPID", "0") or 0),
                "restarts": int(values.get("NRestarts", "0") or 0),
            }
        except ValueError:
            return None

    def rss_bytes(self, pid):
        if not pid:
            return None
        try:
            with open(f"/proc/{pid}/status", encoding="utf-8") as status:
                for line in status:
                    if line.startswith("VmRSS:"):
                        return int(line.split()[1]) * 1024
        except (OSError, ValueError, IndexError):
            return None
        return None


class Collector:
    """Holds the loops' shared state; each loop opens its own store connection."""

    def __init__(self, sources, db_path, activation_height=chain.NU7_ACTIVATION_HEIGHT,
                 backfill=INTERVAL_SAMPLE_BLOCKS, page_path=None, clock=time.time,
                 rolling_blocks=PROJECTION_ROLLING_BLOCKS, fixed_blocks=PROJECTION_FIXED_BLOCKS,
                 monte_carlo_runs=MONTE_CARLO_RUNS):
        self.sources = sources
        self.db_path = db_path
        self.activation_height = activation_height
        self.backfill = backfill
        self.clock = clock
        self.rolling_blocks = rolling_blocks
        self.fixed_blocks = fixed_blocks
        self.monte_carlo_runs = monte_carlo_runs
        self.model = {}
        self.fixed_projection = None
        self.monte_carlo_thread = None
        self.annotation_attempted = set()
        self.payload_cache = {}
        self.page = page_path.read_bytes() if page_path else b""
        self.lock = threading.Lock()
        self.metrics = {}
        self.metrics_at = 0.0
        self.scrape_ms = None
        self.rpc_ok_at = 0.0
        self.chain_info = None
        self.pending_attribution = {}
        self.started_at = clock()
        self.initial_tip = None
        self.stop = threading.Event()

    # -- metrics loop -------------------------------------------------------

    def scrape_once(self, store):
        """Scrape the node once and attribute any new per-block counter totals."""
        started = self.clock()
        text = self.sources.metrics_text()
        metrics = chain.parse_exposition(text)
        elapsed = (self.clock() - started) * 1000.0
        with self.lock:
            self.metrics = metrics
            self.metrics_at = self.clock()
            self.scrape_ms = elapsed
        self.attribute(store, metrics)
        return elapsed

    def counter_snapshot(self, metrics):
        height = chain.whole(chain.bare(metrics, chain.METRIC_NAMES["block_height"]))
        if height is None:
            return None
        totals = {}
        latest = {}
        for column, stem in chain.BLOCK_COUNTERS.items():
            total = chain.bare(metrics, stem + "_total")
            if total is not None:
                totals[column] = chain.whole(total)
                latest[column] = chain.whole(chain.bare(metrics, stem))
        return {"height": height, "totals": totals, "latest": latest} if totals else None

    def attribute(self, store, metrics):
        current = self.counter_snapshot(metrics)
        if current is None:
            return
        previous = store.meta("counter_snapshot")
        rows = chain.attribute_counters(previous, current)
        if rows == "reset":
            store.add_event("counter_reset", height=current["height"],
                            detail="per-block counters went backwards; the node restarted")
            rows = []
        for height, values, merged_from in rows:
            values = dict(values)
            values["fee_split_ok"] = chain.fee_split_ok(
                values.get("fees_zat"), values.get("nsm_fee_zat"), self.nu7_active_for(height, merged_from)
            )
            if merged_from is not None:
                values["merged_from"] = merged_from
            if store.block(height) is None:
                with self.lock:
                    self.pending_attribution[height] = values
            else:
                store.update_block(height, values)
        store.set_meta("counter_snapshot", current)

    def nu7_active_for(self, height, merged_from):
        """Whether NU7 rules applied to every block a row covers; None for a merged row that straddles activation."""
        first = height if merged_from is None else merged_from
        if first >= self.activation_height:
            return True
        if height < self.activation_height:
            return False
        return None

    def metrics_loop(self, base_interval):
        store = Store(self.db_path)
        interval = base_interval
        while not self.stop.is_set():
            try:
                elapsed = self.scrape_once(store)
                # An expensive exposition (many peer-labelled series) is scraped less often.
                interval = max(base_interval, 3 * elapsed / 1000.0)
            except Exception as error:  # noqa: BLE001 - a scrape failure must not stop the loop
                LOG.warning("metrics scrape failed: %s", error)
                interval = base_interval
            self.stop.wait(interval)

    # -- block loop ---------------------------------------------------------

    def ingest_once(self, store):
        """Store every new best-chain block, rolling back a reorg first."""
        info = self.sources.rpc("getblockchaininfo")
        with self.lock:
            self.chain_info = info
            self.rpc_ok_at = self.clock()
        tip_height = int(info["blocks"])
        if self.initial_tip is None:
            self.initial_tip = tip_height
        stored = store.tip()
        if stored is not None and stored["height"] <= tip_height:
            self.rollback_reorg(store, stored)
            stored = store.tip()
        if stored is None:
            start = max(1, tip_height - self.backfill)
        else:
            start = stored["height"] + 1
        end = min(tip_height, start + MAX_BLOCKS_PER_PASS - 1)
        if start > end:
            if not self.model:
                self.update_model(store)
            return 0
        if end < tip_height and stored is not None:
            store.add_event("gap", height=start, detail=f"ingesting {start}..{tip_height} in passes")
        previous = stored
        now = self.clock()
        for height in range(start, end + 1):
            row = self.block_row(height, previous, now)
            row.update(self.block_annotations(store, height, row))
            if height == tip_height:
                self.annotate_tip(row, info)
            if height >= self.activation_height and (previous is None or previous["height"] < self.activation_height):
                store.add_event("activation", height=height, detail="first NU7 block on the best chain")
            store.upsert_block(row)
            pending = None
            with self.lock:
                pending = self.pending_attribution.pop(height, None)
            if pending:
                store.update_block(height, pending)
            previous = row
        self.update_model(store)
        return end - start + 1

    def rollback_reorg(self, store, stored):
        """Drop stored rows that are no longer on the best chain and record the reorg."""
        if self.sources.rpc("getblockhash", [stored["height"]]) == stored["hash"]:
            return
        fork = stored["height"]
        walked = 0
        while fork > 0 and walked < MAX_REORG_WALK:
            fork -= 1
            walked += 1
            row = store.block(fork)
            if row is None or self.sources.rpc("getblockhash", [fork]) == row["hash"]:
                break
        depth = stored["height"] - fork
        store.add_event("reorg", height=stored["height"], depth=depth,
                        detail=f"best chain replaced {depth} block(s) above {fork}")
        store.rollback_above(fork)
        LOG.info("reorg: %s block(s) above %s replaced", depth, fork)

    def block_row(self, height, previous, now):
        block = self.sources.rpc("getblock", [str(height), 1])
        pools = {pool.get("id"): pool for pool in block.get("valuePools") or []}

        def pool_value(name):
            pool = pools.get(name) or {}
            return chain.whole(pool.get("chainValueZat"))

        previous_time = previous["time"] if previous else None
        previous_difficulty = previous["difficulty"] if previous else None
        live = self.initial_tip is not None and height > self.initial_tip
        row = {
            "height": height,
            "hash": block["hash"],
            "time": int(block["time"]),
            "observed_at": now if live else None,
            "interval_s": int(block["time"]) - previous_time if previous_time is not None else None,
            "bits": block.get("bits"),
            "difficulty": block.get("difficulty"),
            "difficulty_change_pct": chain.difficulty_change_pct(previous_difficulty, block.get("difficulty")),
            "size": block.get("size"),
            "n_tx": block.get("nTx", len(block.get("tx") or [])),
            "pool_transparent_zat": pool_value("transparent"),
            "pool_sprout_zat": pool_value("sprout"),
            "pool_sapling_zat": pool_value("sapling"),
            "pool_orchard_zat": pool_value("orchard"),
            "pool_ironwood_zat": pool_value("ironwood"),
            "pool_lockbox_zat": pool_value("lockbox"),
            "chain_supply_zat": chain.whole((block.get("chainSupply") or {}).get("chainValueZat")),
            "nu7_active": int(height >= self.activation_height),
            "source": "live" if live else "backfill",
            # Not stored: the block loop reads these to attribute the miner.
            "coinbase_txid": (block.get("tx") or [None])[0],
            "funding_stream_addresses": [],
        }
        try:
            subsidy = self.sources.rpc("getblocksubsidy", [height])
            row.update({
                "subsidy_total_zat": chain.zec_to_zat(subsidy.get("totalblocksubsidy")),
                "subsidy_miner_zat": chain.zec_to_zat(subsidy.get("miner")),
                "subsidy_fs_zat": chain.zec_to_zat(subsidy.get("fundingstreamstotal")),
                "subsidy_lockbox_zat": chain.zec_to_zat(subsidy.get("lockboxtotal")),
                "funding_stream_addresses": [stream.get("address") for stream in subsidy.get("fundingstreams") or []],
            })
        except Exception as error:  # noqa: BLE001 - the subsidy is decoration
            LOG.debug("getblocksubsidy %s failed: %s", height, error)
        return row

    def block_annotations(self, store, height, row):
        """The miner of a block and the difficulty model's view of it, as columns to store.

        The miner comes from the coinbase transaction (tag and payout address);
        `expected_bits` is what consensus required given the stored blocks
        below `height`, with a `daa_mismatch` event when the block differs;
        `hashrate_solps` is the solution rate over the previous 120 blocks and
        `expected_interval_s` the time this block was expected to take at it.
        """
        values = {}
        coinbase = None
        if row.get("coinbase_txid"):
            try:
                coinbase = self.sources.rpc("getrawtransaction", [row["coinbase_txid"], 1])
            except Exception as error:  # noqa: BLE001 - pruned or unavailable: the miner stays unknown
                LOG.debug("coinbase of %s unavailable: %s", height, error)
        if coinbase:
            inputs = coinbase.get("vin") or [{}]
            tag = miners.coinbase_tag(inputs[0].get("coinbase"))
            address = miners.miner_payout(coinbase.get("vout"), row.get("funding_stream_addresses"))
            values.update({"miner_tag": tag, "miner_address": address, "miner_id": miners.miner_id(tag, address)})
        context = store.headers_before(height, max(daa.CONTEXT_BLOCKS, HASHRATE_BLOCKS + 1))
        window, _, _ = daa.rule_at(height, self.activation_height)
        if row.get("bits") and len(context) >= window + daa.MEDIAN_TIME_SPAN:
            expected = daa.next_threshold(
                [daa.expand_compact(daa.parse_bits(bits)) for bits, _ in context],
                [t for _, t in context], height, self.activation_height,
            )
            values["expected_bits"] = f"{expected:08x}"
            values["daa_ok"] = int(expected == daa.parse_bits(row["bits"]))
            if not values["daa_ok"]:
                store.add_event("daa_mismatch", height=height,
                                detail=f"consensus requires bits {expected:08x}, the block has {row['bits']}")
        # The solution rate over the previous 120 blocks, or over what is stored when fewer.
        rate = daa.hashrate(context, min(HASHRATE_BLOCKS, len(context) - 1)) if len(context) > 1 else None
        if rate and row.get("bits"):
            values["hashrate_solps"] = rate
            values["expected_interval_s"] = daa.expected_interval(row["bits"], rate)
        return values

    def annotate_missing(self, store):
        """Fill the miner and model columns of rows stored by an older collector, a few per pass."""
        heights = [h for h in store.blocks_without_annotation(ANNOTATE_PER_PASS + len(self.annotation_attempted))
                   if h not in self.annotation_attempted][:ANNOTATE_PER_PASS]
        for height in heights:
            self.annotation_attempted.add(height)
            row = store.block(height)
            if row is None:
                continue
            try:
                block = self.sources.rpc("getblock", [str(height), 1])
                subsidy = self.sources.rpc("getblocksubsidy", [height])
            except Exception as error:  # noqa: BLE001 - leave the row for a later pass
                LOG.debug("annotation of %s failed: %s", height, error)
                continue
            row["coinbase_txid"] = (block.get("tx") or [None])[0]
            row["funding_stream_addresses"] = [stream.get("address") for stream in subsidy.get("fundingstreams") or []]
            values = self.block_annotations(store, height, row)
            if values:
                store.update_block(height, values)
        return len(heights)

    # -- difficulty model ---------------------------------------------------

    def update_model(self, store):
        """Refresh hashrate, next-block difficulty, expected block time and the rolling projection from the stored chain."""
        tip = store.tip()
        if not tip or not tip.get("bits"):
            return
        next_height = tip["height"] + 1
        context = store.headers_before(next_height, max(daa.CONTEXT_BLOCKS, FIXED_HASHRATE_BLOCKS + 1))
        targets = [daa.expand_compact(daa.parse_bits(bits)) for bits, _ in context[:daa.CONTEXT_BLOCKS]]
        times = [t for _, t in context[:daa.CONTEXT_BLOCKS]]
        _, spacing, _ = daa.rule_at(next_height, self.activation_height)
        projection_blocks = chain.daa_window(next_height, self.activation_height) if next_height > self.activation_height else INTERVAL_SAMPLE_BLOCKS
        rate = daa.hashrate(context, min(projection_blocks, len(context) - 1)) if len(context) > 1 else None
        rate120 = daa.hashrate(context, min(HASHRATE_BLOCKS, len(context) - 1)) if len(context) > 1 else None
        next_bits = daa.next_threshold(targets, times, next_height, self.activation_height)
        checked, matched = store.daa_check_counts()
        expected = {
            "hashrateSolps": rate120,
            "hashrateBlocks": HASHRATE_BLOCKS,
            "nodeSolps": tip.get("solps"),
            "projectionHashrateSolps": rate,
            "projectionHashrateBlocks": projection_blocks,
            "equilibriumDifficulty": daa.difficulty(daa.equilibrium_bits(rate, spacing)) if rate else None,
            "expectedIntervalSeconds": daa.expected_interval(tip["bits"], rate120) if rate120 else None,
            "nextBits": f"{next_bits:08x}",
            "nextDifficulty": daa.difficulty(next_bits),
            "modelChecked": checked,
            "modelMatched": matched,
        }
        # Before activation the projection reaches at least `rolling_blocks` past it.
        horizon = self.rolling_blocks + max(0, self.activation_height - next_height)
        rolling = daa.project(context, next_height, horizon, rate, self.activation_height) if rate else []
        self.ensure_fixed_projection(store, context, tip)
        with self.lock:
            self.model = {"expected": expected, "rolling": rolling,
                          "rollingFrom": next_height, "rollingHashrateSolps": rate}

    def ensure_fixed_projection(self, store, context, tip):
        """Freeze one projection from the activation height, the first time a tip is seen, and keep it.

        Before activation the seed is a synthetic chain at the tip's difficulty
        spaced by the expected block time at the hashrate of the last 1008
        blocks; after it, the real blocks below the activation height. A
        Monte Carlo band is added in the background once.
        """
        if self.fixed_projection is None:
            self.fixed_projection = store.meta("projection_fixed")
        if self.fixed_projection is not None:
            return
        if len(context) < 2:
            return
        if tip["height"] >= self.activation_height:
            seed = store.headers_before(self.activation_height, daa.CONTEXT_BLOCKS)
            if len(seed) < daa.CONTEXT_BLOCKS:
                seed = None
        else:
            seed = None
        rate_blocks = min(FIXED_HASHRATE_BLOCKS, len(context) - 1)
        rate = daa.hashrate(context, rate_blocks)
        if not rate:
            return
        bits0 = daa.parse_bits(tip["bits"])
        if seed is None:
            seed = daa.steady_context(bits0, rate, tip["time"])
            seed_kind = "synthetic"
        else:
            seed_kind = "chain"
        curve = daa.project(seed, self.activation_height, self.fixed_blocks, rate, self.activation_height)
        fixed = {
            "frozenAt": self.clock(), "frozenHeight": tip["height"], "bits0": f"{bits0:08x}",
            "difficulty0": daa.difficulty(bits0), "hashrate0": rate, "hashrateBlocks": rate_blocks,
            "seed": seed_kind, "fromHeight": self.activation_height, "curve": curve, "band": None,
            "summary": daa.convergence_summary(curve, chain.POST_NU7_TARGET_SPACING),
        }
        store.set_meta("projection_fixed", fixed)
        self.fixed_projection = fixed
        self.monte_carlo_thread = threading.Thread(
            target=self.monte_carlo_band, args=(seed, rate), daemon=True, name="monte-carlo")
        self.monte_carlo_thread.start()

    def monte_carlo_band(self, seed, rate):
        try:
            band = daa.monte_carlo(seed, self.activation_height, self.fixed_blocks, rate,
                                   self.activation_height, runs=self.monte_carlo_runs)
            with self.lock:
                fixed = dict(self.fixed_projection or {})
                fixed["band"] = band
                self.fixed_projection = fixed
            store = Store(self.db_path)
            try:
                store.set_meta("projection_fixed", fixed)
            finally:
                store.close()
        except Exception as error:  # noqa: BLE001 - the band is decoration; the curve stands without it
            LOG.warning("monte carlo band failed: %s", error)

    def annotate_tip(self, row, info):
        nsm = info.get("nsmValueBalanceZat")
        if isinstance(nsm, int):
            row["nsm_balance_zat"] = nsm
        try:
            row["solps"] = chain.whole(self.sources.rpc("getnetworksolps", [120, -1]))
        except Exception as error:  # noqa: BLE001 - the solution rate is decoration
            LOG.debug("getnetworksolps failed: %s", error)

    def block_loop(self, interval):
        store = Store(self.db_path)
        while not self.stop.is_set():
            try:
                self.ingest_once(store)
                self.annotate_missing(store)
            except Exception as error:  # noqa: BLE001 - an RPC failure must not stop the loop
                LOG.warning("block ingest failed: %s", error)
            self.stop.wait(interval)

    # -- sample loop --------------------------------------------------------

    def sample_once(self, store):
        with self.lock:
            metrics = self.metrics
            scrape_ms = self.scrape_ms
            info = self.chain_info
        m = chain.METRIC_NAMES
        sample = {
            "t": self.clock(),
            "height": int(info["blocks"]) if info else None,
            "peers": chain.whole(chain.bare(metrics, m["peers"])),
            "peers_obsolete": chain.whole(chain.bare(metrics, m["peers_obsolete"])),
            "sync_distance": chain.whole(chain.bare(metrics, m["sync_distance"])),
            "mempool_tx": chain.whole(chain.bare(metrics, m["mempool_tx"])),
            "mempool_bytes": chain.whole(chain.bare(metrics, m["mempool_bytes"])),
            "mempool_actions_paid": chain.whole(chain.bare(metrics, m["mempool_actions_paid"])),
            "mempool_actions_unpaid": chain.whole(chain.bare(metrics, m["mempool_actions_unpaid"])),
            "mempool_evicted_total": chain.whole(chain.bare(metrics, m["mempool_evicted"])),
            "node_fee_p50_zat": chain.quantile(metrics, m["tx_fee"], "0.5"),
            "node_fee_p90_zat": chain.quantile(metrics, m["tx_fee"], "0.9"),
            "node_fee_p99_zat": chain.quantile(metrics, m["tx_fee"], "0.99"),
            "verify_p50_ms": seconds_to_ms(chain.quantile(metrics, m["verify_duration"], "0.5")),
            "verify_p99_ms": seconds_to_ms(chain.quantile(metrics, m["verify_duration"], "0.99")),
            "commit_p50_ms": seconds_to_ms(chain.quantile(metrics, m["commit_duration"], "0.5")),
            "commit_p99_ms": seconds_to_ms(chain.quantile(metrics, m["commit_duration"], "0.99")),
            "rocksdb_p99_ms": seconds_to_ms(chain.quantile(metrics, m["rocksdb_duration"], "0.99")),
            "scrape_ms": scrape_ms,
        }
        try:
            mempool = self.sources.rpc("getmempoolinfo")
            sample["mempool_tx"] = mempool.get("size", sample["mempool_tx"])
            sample["mempool_bytes"] = mempool.get("bytes", sample["mempool_bytes"])
            if (mempool.get("size") or 0) <= RAW_MEMPOOL_MAX_TX:
                started = self.clock()
                raw = self.sources.rpc("getrawmempool", [True])
                if self.clock() - started <= RAW_MEMPOOL_MAX_SECONDS:
                    quantiles = chain.mempool_fee_quantiles(raw)
                    sample.update({
                        "mempool_fee_p10_zat": quantiles["p10"],
                        "mempool_fee_p50_zat": quantiles["p50"],
                        "mempool_fee_p90_zat": quantiles["p90"],
                        "mempool_feerate_p50": quantiles["feeRateP50"],
                        "mempool_count": quantiles["count"],
                    })
            tips = self.sources.rpc("getchaintips")
            sample["chain_tips"] = len(tips) if isinstance(tips, list) else None
        except Exception as error:  # noqa: BLE001 - mempool detail is optional
            LOG.debug("mempool sample incomplete: %s", error)
        status = self.sources.service_status()
        if status:
            sample["active_state"] = status["active_state"]
            sample["restarts"] = status["restarts"]
            sample["rss_bytes"] = self.sources.rss_bytes(status["pid"])
        store.insert_sample(sample)
        return sample

    def sample_loop(self, interval):
        store = Store(self.db_path)
        last_prune = 0.0
        while not self.stop.is_set():
            try:
                self.sample_once(store)
                if self.clock() - last_prune > 3600:
                    store.prune_samples()
                    last_prune = self.clock()
            except Exception as error:  # noqa: BLE001 - a sample failure must not stop the loop
                LOG.warning("sample failed: %s", error)
            self.stop.wait(interval)

    # -- API ----------------------------------------------------------------

    def healthy(self):
        now = self.clock()
        with self.lock:
            return (now - self.metrics_at) < FRESHNESS_SECONDS and (now - self.rpc_ok_at) < FRESHNESS_SECONDS

    def cached_payload(self, store, window="6h"):
        """`payload`, reused for a few seconds per window: the page polls every 15 s and the API is public."""
        now = self.clock()
        with self.lock:
            cached = self.payload_cache.get(window)
        if cached and now - cached[0] < PAYLOAD_CACHE_SECONDS:
            return cached[1]
        payload = self.payload(store, window)
        with self.lock:
            self.payload_cache[window] = (now, payload)
        return payload

    def payload(self, store, window="6h"):
        """The dashboard JSON: current status plus the series for `window`."""
        now = self.clock()
        window_seconds = WINDOWS.get(window, WINDOWS["6h"])
        with self.lock:
            metrics = self.metrics
            info = self.chain_info or {}
            scrape_ms = self.scrape_ms
            metrics_at = self.metrics_at
            model = self.model
            fixed = self.fixed_projection
        m = chain.METRIC_NAMES
        tip = store.tip()
        height = tip["height"] if tip else None
        intervals = store.intervals(INTERVAL_SAMPLE_BLOCKS)
        post_nu7 = [i for h, i in intervals if h >= self.activation_height]
        all_intervals = [i for _, i in intervals]
        spacing = chain.target_spacing(height, self.activation_height) if height else chain.PRE_NU7_TARGET_SPACING
        remaining, eta, basis = chain.activation_eta(
            height or 0, tip["time"] if tip else now, all_intervals[-102:], self.activation_height)
        activation_block = store.block(self.activation_height)
        latest_sample = store.latest_sample() or {}
        fees102 = store.fee_totals_since((height or 0) - 101)
        consensus = info.get("consensus") or {}
        upgrades = info.get("upgrades") or {}
        nu7_upgrade = upgrades.get(chain.NU7_BRANCH_ID) or {}
        base_subsidy = tip.get("subsidy_total_zat") if tip else None
        blocks = store.blocks_in_window(now - window_seconds)
        samples = store.samples_since(now - window_seconds)
        events = store.events_since(now - window_seconds)
        day_ago = now - 86400
        version = chain.label_value(metrics, m["build_info"], "version")
        nsm_balance = tip.get("nsm_balance_zat") if tip else None
        if nsm_balance is None and isinstance(info.get("nsmValueBalanceZat"), int):
            nsm_balance = info["nsmValueBalanceZat"]
        active = bool(height) and height >= self.activation_height
        baseline_end = self.activation_height if active else max(1, (height or 0) - MINER_RECENT_BLOCKS)
        baseline_rows = store.miner_rows(since_height=baseline_end - MINER_BASELINE_BLOCKS, until_height=baseline_end)
        recent_rows = store.miner_rows(since_height=max(1, (height or 0) - MINER_RECENT_BLOCKS + 1))
        week_rows = store.miner_rows(since_time=int(now) - miners.SEEN_WITHIN_SECONDS)
        baseline = miners.miner_shares(baseline_rows)
        recent = miners.miner_shares(recent_rows)
        week = miners.miner_shares(week_rows)
        return {
            "schemaVersion": 1,
            "generatedAt": now,
            "status": "live" if self.healthy() else "stale",
            "network": {
                "name": "Mainnet", "magic": chain.MAINNET_MAGIC,
                "activationHeight": self.activation_height, "nu7BranchId": chain.NU7_BRANCH_ID,
                "targetSpacingSeconds": chain.POST_NU7_TARGET_SPACING,
                "daaWindowBlocks": chain.POST_NU7_DAA_WINDOW,
                "nodeVersion": version,
                "nu7Status": nu7_upgrade.get("status"),
            },
            "activation": {
                "state": "active" if height and height >= self.activation_height else "pending",
                "height": self.activation_height,
                "blocksRemaining": remaining,
                "etaUnix": eta,
                "etaBasisIntervalSeconds": basis,
                "activatedAt": activation_block["time"] if activation_block else None,
                "blocksSinceActivation": (height - self.activation_height) if height and height >= self.activation_height else None,
            },
            "chain": {
                "height": height,
                "hash": tip["hash"] if tip else None,
                "blockTime": tip["time"] if tip else None,
                "tipAgeSeconds": max(0, int(now - tip["time"])) if tip else None,
                "difficulty": tip["difficulty"] if tip else None,
                "difficultyChangePct": tip["difficulty_change_pct"] if tip else None,
                "bits": tip["bits"] if tip else None,
                "daaWindow": chain.daa_window(height, self.activation_height) if height else None,
                "targetSpacingSeconds": spacing,
                "expectedMedianSeconds": chain.expected_median_interval(spacing),
                "intervals": {
                    "last102": chain.interval_stats(all_intervals, 102),
                    "last300": chain.interval_stats(all_intervals, 300),
                    "postNu7": chain.interval_stats(post_nu7, len(post_nu7)),
                },
                "solps": tip.get("solps") if tip else None,
                "chainTips": latest_sample.get("chain_tips"),
                "orchardActionsLast": tip.get("orchard_actions") if tip else None,
                "orchardActionLimit": chain.ORCHARD_BLOCK_ACTION_LIMIT,
                "sizeLast": tip.get("size") if tip else None,
                "txLast": tip.get("n_tx") if tip else None,
                "expected": model.get("expected"),
            },
            "projections": {
                "fixed": thin_projection(fixed),
                "rolling": {
                    "fromHeight": model.get("rollingFrom"),
                    "hashrateSolps": model.get("rollingHashrateSolps"),
                    "curve": thin_curve(model.get("rolling") or []),
                },
            },
            "miners": {
                "baseline": {"fromHeight": baseline_end - MINER_BASELINE_BLOCKS, "untilHeight": baseline_end,
                             "blocks": baseline["blocks"]},
                "recent": {"blocks": recent["blocks"], "lastBlocks": MINER_RECENT_BLOCKS},
                "week": {"blocks": week["blocks"], "seconds": miners.SEEN_WITHIN_SECONDS,
                         "activeSeconds": miners.ACTIVE_WITHIN_SECONDS},
                "table": miners.miner_table(baseline, recent, week, now),
            },
            "nsm": {
                "balanceZat": nsm_balance,
                "available": nsm_balance is not None,
                "lockboxZat": tip.get("pool_lockbox_zat") if tip else None,
                "lastBlockFeeZat": tip.get("nsm_fee_zat") if tip else None,
                "lastBlockDeltaZat": chain.whole(chain.bare(metrics, m["nsm_change"])),
                "fees102": {
                    "blocks": fees102.get("blocks") or 0,
                    "feesZat": fees102.get("fees"),
                    "nsmZat": fees102.get("nsm"),
                    "minerZat": fees102.get("miner"),
                    "reissuanceZat": fees102.get("reissuance"),
                    "splitOkBlocks": fees102.get("split_ok"),
                },
            },
            "observation": {
                "reorgs24h": store.count_events("reorg", day_ago),
                "blocks24h": store.blocks_observed_since(day_ago),
                "counterResets24h": store.count_events("counter_reset", day_ago),
                "since": self.started_at,
                "scope": "tip replacements observed on this node only",
            },
            "mempool": {
                "tx": latest_sample.get("mempool_tx"),
                "bytes": latest_sample.get("mempool_bytes"),
                "actionsPaid": latest_sample.get("mempool_actions_paid"),
                "actionsUnpaid": latest_sample.get("mempool_actions_unpaid"),
                "feeZat": {
                    "p10": latest_sample.get("mempool_fee_p10_zat"),
                    "p50": latest_sample.get("mempool_fee_p50_zat"),
                    "p90": latest_sample.get("mempool_fee_p90_zat"),
                },
                "feeRateP50": latest_sample.get("mempool_feerate_p50"),
                "nodeFeeZat": {
                    "p50": latest_sample.get("node_fee_p50_zat"),
                    "p99": latest_sample.get("node_fee_p99_zat"),
                },
            },
            "node": {
                "version": version,
                "activeState": latest_sample.get("active_state"),
                "restarts": latest_sample.get("restarts"),
                "rssBytes": latest_sample.get("rss_bytes"),
                "peers": latest_sample.get("peers"),
                "peersObsolete": latest_sample.get("peers_obsolete"),
                "syncDistance": latest_sample.get("sync_distance"),
                "scrapeMs": scrape_ms,
                "metricsAgeSeconds": int(now - metrics_at) if metrics_at else None,
                "metricsOptional": {"chainCounters": store.meta("counter_snapshot") is not None},
            },
            "rules": {
                "atTip": chain.rules_at(height or 0, consensus.get("chaintip"), base_subsidy, self.activation_height),
                "nextBlock": chain.rules_at((height or 0) + 1, consensus.get("nextblock"), base_subsidy, self.activation_height),
            },
            "recentBlocks": [recent_block(row) for row in store.latest_blocks(RECENT_BLOCKS)],
            "series": {
                "window": window,
                "blocks": columns(blocks, {
                    "height": "height", "time": "time", "intervalSeconds": "interval_s",
                    "difficulty": "difficulty", "difficultyChangePct": "difficulty_change_pct",
                    "nTx": "n_tx", "size": "size", "orchardActions": "orchard_actions",
                    "saplingIos": "sapling_ios", "feesZat": "fees_zat", "nsmFeeZat": "nsm_fee_zat",
                    "minerFeeZat": "miner_fee_zat", "reissuanceZat": "reissuance_zat",
                    "subsidyTotalZat": "subsidy_total_zat", "nsmBalanceZat": "nsm_balance_zat",
                    "lockboxZat": "pool_lockbox_zat", "mergedFrom": "merged_from",
                    "nu7Active": "nu7_active", "feeSplitOk": "fee_split_ok",
                    "bits": "bits", "expectedBits": "expected_bits", "daaOk": "daa_ok",
                    "minerId": "miner_id", "observedAt": "observed_at",
                    "hashrateSolps": "hashrate_solps", "expectedIntervalSeconds": "expected_interval_s",
                }),
                "minerBuckets": miners.miner_buckets(
                    [row for row in blocks if row.get("miner_id")], bucket_seconds(window_seconds)),
                "samples": columns(samples, {
                    "t": "t", "mempoolTx": "mempool_tx", "mempoolBytes": "mempool_bytes",
                    "mempoolFeeP10Zat": "mempool_fee_p10_zat", "mempoolFeeP50Zat": "mempool_fee_p50_zat",
                    "mempoolFeeP90Zat": "mempool_fee_p90_zat", "nodeFeeP50Zat": "node_fee_p50_zat",
                    "nodeFeeP99Zat": "node_fee_p99_zat", "peers": "peers",
                    "verifyP50Ms": "verify_p50_ms", "verifyP99Ms": "verify_p99_ms",
                    "commitP50Ms": "commit_p50_ms", "commitP99Ms": "commit_p99_ms",
                    "rocksdbP99Ms": "rocksdb_p99_ms", "rssBytes": "rss_bytes",
                }),
                "events": events,
            },
        }


def seconds_to_ms(value):
    return None if value is None else value * 1000.0


def bucket_seconds(window_seconds):
    """Miner chart buckets: 10 minutes for an hour, an hour for a day, 6 hours for a week."""
    if window_seconds <= 3600:
        return 600
    if window_seconds <= 6 * 3600:
        return 1800
    if window_seconds <= 24 * 3600:
        return 3600
    return 6 * 3600


def thin_curve(curve):
    """A projection with every point for the first 300 blocks and every tenth after, so the payload stays small."""
    return curve[:PROJECTION_DETAIL_BLOCKS] + curve[PROJECTION_DETAIL_BLOCKS::PROJECTION_THIN_STEP]


def thin_projection(fixed):
    if not fixed:
        return None
    thinned = {key: value for key, value in fixed.items() if key not in ("curve", "band")}
    thinned["curve"] = thin_curve(fixed.get("curve") or [])
    thinned["band"] = thin_curve(fixed["band"]) if fixed.get("band") else None
    return thinned


def columns(rows, mapping):
    """Row dicts as parallel arrays, keyed by the API name."""
    return {api: [row.get(column) for row in rows] for api, column in mapping.items()}


def recent_block(row):
    return {
        "height": row["height"], "hash": row["hash"], "time": row["time"],
        "intervalSeconds": row.get("interval_s"), "difficulty": row.get("difficulty"),
        "nTx": row.get("n_tx"), "size": row.get("size"), "feesZat": row.get("fees_zat"),
        "nsmFeeZat": row.get("nsm_fee_zat"), "orchardActions": row.get("orchard_actions"),
        "merged": row.get("merged_from") is not None,
    }


# -- HTTP -------------------------------------------------------------------


class RateLimiter:
    """A small per-client fixed-window limiter for the public endpoints."""

    def __init__(self, limit=RATE_LIMIT, window=RATE_WINDOW):
        self.limit = limit
        self.window = window
        self.lock = threading.Lock()
        self.events = {}

    def allow(self, client, now=None):
        now = time.time() if now is None else now
        cutoff = now - self.window
        with self.lock:
            if client not in self.events and len(self.events) >= RATE_CLIENT_LIMIT:
                self.events = {key: events for key, events in self.events.items()
                               if events and events[-1] > cutoff}
                if len(self.events) >= RATE_CLIENT_LIMIT:
                    return False
            events = self.events.setdefault(client, deque())
            while events and events[0] <= cutoff:
                events.popleft()
            if len(events) >= self.limit:
                return False
            events.append(now)
            return True


class BoundedHTTPServer(ThreadingHTTPServer):
    """A threading HTTP server that handles at most `max_concurrent` requests at once."""

    daemon_threads = True

    def __init__(self, address, handler, collector, max_concurrent=MAX_CONCURRENT_REQUESTS):
        super().__init__(address, handler)
        self.collector = collector
        self.limiter = RateLimiter()
        self.slots = threading.BoundedSemaphore(max_concurrent)
        self.local = threading.local()

    def store(self):
        store = getattr(self.local, "store", None)
        if store is None:
            store = self.local.store = Store(self.collector.db_path)
        return store

    def process_request(self, request, client_address):
        self.slots.acquire()
        try:
            super().process_request(request, client_address)
        except BaseException:
            self.slots.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self.slots.release()


class Handler(BaseHTTPRequestHandler):
    timeout = REQUEST_TIMEOUT_SECONDS

    def log_message(self, *args):
        pass

    def client(self):
        peer = self.client_address[0]
        if peer in TRUSTED_PROXIES:
            return (self.headers.get("X-Real-IP") or peer).strip()
        return peer

    def send_body(self, status, body, content_type, cache="no-store"):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", cache)
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        if body:
            self.wfile.write(body)

    def send_json(self, status, payload, cache="no-store"):
        body = json.dumps(payload, separators=(",", ":"), allow_nan=False).encode()
        self.send_body(status, body, "application/json; charset=utf-8", cache)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        collector = self.server.collector
        url = urllib.parse.urlparse(self.path)
        if not self.server.limiter.allow(self.client()):
            self.send_json(429, {"error": "rate limited"})
            return
        if url.path == "/healthz":
            healthy = collector.healthy()
            self.send_json(200 if healthy else 503, {"status": "ok" if healthy else "stale"})
        elif url.path == "/v1/dashboard":
            window = urllib.parse.parse_qs(url.query).get("window", ["6h"])[0]
            if window not in WINDOWS:
                self.send_json(400, {"error": "window must be one of " + ", ".join(WINDOWS)})
                return
            try:
                payload = collector.cached_payload(self.server.store(), window)
            except Exception as error:  # noqa: BLE001 - report instead of dropping the connection
                LOG.exception("dashboard payload failed")
                self.send_json(500, {"error": str(error)})
                return
            self.send_json(200, payload, cache="public, max-age=5")
        elif url.path == "/":
            self.send_body(200, collector.page, "text/html; charset=utf-8")
        else:
            self.send_json(404, {"error": "not found"})


def serve(collector, host, port, metrics_interval, block_interval, sample_interval):
    threads = [
        threading.Thread(target=collector.metrics_loop, args=(metrics_interval,), daemon=True, name="metrics"),
        threading.Thread(target=collector.block_loop, args=(block_interval,), daemon=True, name="blocks"),
        threading.Thread(target=collector.sample_loop, args=(sample_interval,), daemon=True, name="samples"),
    ]
    for thread in threads:
        thread.start()
    server = BoundedHTTPServer((host, port), Handler, collector)
    LOG.info("serving on %s:%s", host, port)
    try:
        server.serve_forever()
    finally:
        collector.stop.set()
        server.server_close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--rpc-url", default="http://127.0.0.1:8232/")
    parser.add_argument("--metrics-url", default="http://127.0.0.1:9999/metrics")
    parser.add_argument("--service", default="zakurad", help="systemd unit of the node")
    parser.add_argument("--db", default="dashboard.sqlite", help="SQLite database path")
    parser.add_argument("--page", default=str(Path(__file__).with_name("page.html")))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8095)
    parser.add_argument("--activation-height", type=int, default=chain.NU7_ACTIVATION_HEIGHT)
    parser.add_argument("--backfill", type=int, default=DEFAULT_BACKFILL,
                        help="blocks below the tip to load at startup (the miner baseline)")
    parser.add_argument("--metrics-interval", type=float, default=2.0)
    parser.add_argument("--block-interval", type=float, default=3.0)
    parser.add_argument("--sample-interval", type=float, default=10.0)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stderr)
    sources = NodeSources(args.rpc_url, args.metrics_url, args.service)
    collector = Collector(sources, args.db, args.activation_height, args.backfill, Path(args.page))
    serve(collector, args.host, args.port, args.metrics_interval, args.block_interval, args.sample_interval)


if __name__ == "__main__":
    main()
