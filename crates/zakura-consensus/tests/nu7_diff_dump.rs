//! The NU7 differential dump: Zakura's value of every quantity in the NU7 differential spec
//! (`qa/zcash/nu7-diff/SPEC.md` in the zcashd-compat fork), from Zakura's production
//! functions. The fork's `compare.py` checks zcashd's dump against this one, whose output it
//! commits as `zakura.jsonl.gz`. Run with
//!
//! ```sh
//! NU7_DIFF_OUT=zakura.jsonl cargo test --release -p zakura-consensus --test nu7_diff_dump -- --ignored
//! ```

use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
    time::Instant,
};

use chrono::{DateTime, TimeZone, Utc};
use zakura_chain::{
    amount::{Amount, NonNegative},
    block::Height,
    parameters::{
        subsidy::{
            block_subsidy, funding_stream_address_period, funding_stream_values, halving,
            halving_block_subsidy, height_for_halving, miner_fee_share, miner_subsidy,
            nsm_reissuance_height, FundingStreamReceiver,
        },
        testnet::{self, ConfiguredActivationHeights, RegtestParameters},
        ConsensusBranchId, Network, NetworkUpgrade,
    },
    work::difficulty::{CompactDifficulty, ParameterDifficulty as _},
};
use zakura_consensus::funding_stream_address;
use zakura_header_chain::{pow_adjustment_block_span_for_height, AdjustedDifficulty};

const RECEIVERS: [FundingStreamReceiver; 4] = [
    FundingStreamReceiver::Ecc,
    FundingStreamReceiver::ZcashFoundation,
    FundingStreamReceiver::MajorGrants,
    FundingStreamReceiver::Deferred,
];

const NSM_BALANCES: [i64; 7] = [
    0,
    1,
    100_000_000,
    1_000_000_000_000,
    21_000_000_000_000,
    500_000_000_000_000,
    1_050_000_000_000_000,
];

const FEES: [i64; 19] = [
    0,
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    99,
    100,
    101,
    999,
    12345,
    100000001,
    1099511627783,
];

/// One JSONL record.
struct Rec {
    q: &'static str,
    key: String,
    h: Option<u32>,
    v: String,
}

/// Escapes `s` for a JSON string.
fn esc(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', " ")
}

/// Runs `f`, turning a panic into an `ERR:panic:` value.
fn guard(f: impl FnOnce() -> String) -> String {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(v) => v,
        Err(e) => {
            let msg = e
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| e.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_default();
            format!("ERR:panic:{}", msg.chars().take(160).collect::<String>())
        }
    }
}

/// Formats an amount in zatoshis.
fn amt(a: Amount<NonNegative>) -> String {
    i64::from(a).to_string()
}

/// The receiver name used as a record key.
fn rname(r: FundingStreamReceiver) -> String {
    format!("{r:?}")
}

/// Cheap per-height values, in a fixed order of (quantity, key).
fn cheap(net: &Network, h: Height, is_testnet: bool) -> Vec<(&'static str, String, String)> {
    let mut out = Vec::with_capacity(16);
    out.push((
        "nu",
        String::new(),
        guard(|| format!("{}", NetworkUpgrade::current(net, h))),
    ));
    out.push((
        "branch_id",
        String::new(),
        guard(|| {
            ConsensusBranchId::current(net, h)
                .map(|b| format!("0x{:08x}", u32::from(b)))
                .unwrap_or_else(|| "none".to_string())
        }),
    ));
    out.push((
        "target_spacing",
        String::new(),
        guard(|| {
            NetworkUpgrade::target_spacing_for_height(net, h)
                .num_seconds()
                .to_string()
        }),
    ));
    out.push((
        "averaging_window",
        String::new(),
        guard(|| NetworkUpgrade::averaging_window_for_height(net, h).to_string()),
    ));
    out.push((
        "halving_index",
        String::new(),
        guard(|| halving(h, net).to_string()),
    ));

    // The block verifier calls `block_subsidy(height, network, nsm_value_balance)`; a zero
    // parent NSM balance makes the ZIP 234 bonus zero, so this is the halving subsidy.
    let subsidy = catch_unwind(AssertUnwindSafe(|| {
        block_subsidy(h, net, Some(Amount::zero()))
    }));
    let subsidy = match subsidy {
        Ok(Ok(s)) => Ok(s),
        Ok(Err(e)) => Err(format!("ERR:{e}")),
        Err(_) => Err("ERR:panic".to_string()),
    };
    out.push((
        "block_subsidy",
        String::new(),
        match &subsidy {
            Ok(s) => amt(*s),
            Err(e) => e.clone(),
        },
    ));

    let fs = match &subsidy {
        Ok(s) => {
            let s = *s;
            match catch_unwind(AssertUnwindSafe(|| funding_stream_values(h, net, s))) {
                Ok(Ok(m)) => Ok(m),
                Ok(Err(e)) => Err(format!("ERR:{e}")),
                Err(_) => Err("ERR:panic".to_string()),
            }
        }
        Err(e) => Err(e.clone()),
    };
    for r in RECEIVERS {
        out.push((
            "funding_stream_value",
            rname(r),
            match &fs {
                Ok(m) => m
                    .get(&r)
                    .map(|a| amt(*a))
                    .unwrap_or_else(|| "0".to_string()),
                Err(e) => e.clone(),
            },
        ));
    }
    out.push((
        "lockbox_value",
        String::new(),
        match &fs {
            Ok(m) => m
                .get(&FundingStreamReceiver::Deferred)
                .map(|a| amt(*a))
                .unwrap_or_else(|| "0".to_string()),
            Err(e) => e.clone(),
        },
    ));
    out.push((
        "miner_subsidy",
        String::new(),
        match &subsidy {
            Ok(s) => {
                let s = *s;
                guard(|| match miner_subsidy(h, net, s) {
                    Ok(a) => amt(a),
                    Err(e) => format!("ERR:{e}"),
                })
            }
            Err(e) => e.clone(),
        },
    ));
    if is_testnet {
        out.push((
            "testnet_min_difficulty_gap_secs",
            String::new(),
            guard(|| {
                NetworkUpgrade::minimum_difficulty_spacing_for_height(net, h)
                    .map(|d| d.num_seconds().to_string())
                    .unwrap_or_else(|| "none".to_string())
            }),
        ));
    }
    out
}

/// Expensive per-height values: funding stream addresses and NSM reissuance.
fn expensive(net: &Network, h: Height) -> Vec<Rec> {
    let mut out = Vec::new();
    for r in RECEIVERS {
        out.push(Rec {
            q: "funding_stream_address",
            key: rname(r),
            h: Some(h.0),
            v: guard(|| {
                funding_stream_address(h, net, r)
                    .map(|a| a.to_string())
                    .unwrap_or_else(|| "none".to_string())
            }),
        });
    }
    for b in NSM_BALANCES {
        out.push(Rec {
            q: "nsm_reissuance",
            key: b.to_string(),
            h: Some(h.0),
            v: guard(|| {
                let bal = Amount::<NonNegative>::try_from(b).expect("valid balance");
                let with = match block_subsidy(h, net, Some(bal)) {
                    Ok(a) => a,
                    Err(e) => return format!("ERR:{e}"),
                };
                let base = match halving_block_subsidy(h, net) {
                    Ok(a) => a,
                    Err(e) => return format!("ERR:{e}"),
                };
                (i64::from(with) - i64::from(base)).to_string()
            }),
        });
    }
    out
}

/// Runs `f` over `items` with a shared work queue on all cores.
fn par_map<T: Sync, R: Send>(items: &[T], f: impl Fn(&T) -> R + Sync) -> Vec<R> {
    let next = AtomicUsize::new(0);
    let results: Mutex<Vec<(usize, R)>> = Mutex::new(Vec::new());
    let threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    std::thread::scope(|s| {
        for _ in 0..threads {
            s.spawn(|| loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= items.len() {
                    break;
                }
                let r = f(&items[i]);
                results.lock().unwrap().push((i, r));
            });
        }
    });
    let mut v = results.into_inner().unwrap();
    v.sort_by_key(|(i, _)| *i);
    v.into_iter().map(|(_, r)| r).collect()
}

/// Returns the heights in `[lo, hi]` where `funding_stream_address_period` changes,
/// found by binary search (the period is non-decreasing).
fn period_boundaries(net: &Network, lo: u32, hi: u32) -> Vec<u32> {
    let p = |h: u32| funding_stream_address_period(Height(h), net);
    let mut out = Vec::new();
    let mut cur = lo;
    let mut pc = p(cur);
    loop {
        if cur >= hi || p(hi) == pc {
            break;
        }
        let (mut a, mut b) = (cur + 1, hi);
        while a < b {
            let m = a + (b - a) / 2;
            if p(m) > pc {
                b = m;
            } else {
                a = m + 1;
            }
        }
        out.push(a);
        cur = a;
        pc = p(a);
    }
    out
}

/// Builds public Testnet with NU7 at `a`, through the configured-network builder.
fn build_testnet(a: u32) -> Result<Network, String> {
    let def = Network::new_default_testnet();
    let mut h = ConfiguredActivationHeights::default();
    for (height, nu) in def.activation_list() {
        let v = Some(height.0);
        match nu {
            NetworkUpgrade::Genesis => {}
            NetworkUpgrade::BeforeOverwinter => h.before_overwinter = v,
            NetworkUpgrade::Overwinter => h.overwinter = v,
            NetworkUpgrade::Sapling => h.sapling = v,
            NetworkUpgrade::Blossom => h.blossom = v,
            NetworkUpgrade::Heartwood => h.heartwood = v,
            NetworkUpgrade::Canopy => h.canopy = v,
            NetworkUpgrade::Nu5 => h.nu5 = v,
            NetworkUpgrade::Nu6 => h.nu6 = v,
            NetworkUpgrade::Nu6_1 => h.nu6_1 = v,
            NetworkUpgrade::Nu6_2 => h.nu6_2 = v,
            NetworkUpgrade::Nu6_3 => h.nu6_3 = v,
            NetworkUpgrade::Nu7 => h.nu7 = v,
        }
    }
    h.nu7 = Some(a);
    testnet::Parameters::build()
        .with_activation_heights(h)
        .map_err(|e| e.to_string())?
        .to_network()
        .map_err(|e| e.to_string())
}

/// Builds Regtest with every upgrade through NU6.3 at 1 and NU7 at 300.
fn build_regtest() -> Result<Network, String> {
    let one = Some(1);
    let heights = ConfiguredActivationHeights {
        before_overwinter: one,
        overwinter: one,
        sapling: one,
        blossom: one,
        heartwood: one,
        canopy: one,
        nu5: one,
        nu6: one,
        nu6_1: one,
        nu6_2: one,
        nu6_3: one,
        nu7: Some(300),
    };
    testnet::Parameters::new_regtest(RegtestParameters {
        activation_heights: heights,
        ..Default::default()
    })
    .map(Network::new_configured_testnet)
    .map_err(|e| e.to_string())
}

/// Formats compact difficulty bits as hex.
fn compact_hex(c: CompactDifficulty) -> String {
    format!("0x{:08x}", u32::from_le_bytes(c.to_le_bytes()))
}

/// One dump scenario: a network, its NU7 height, and its section A scan range.
struct Scenario {
    name: &'static str,
    a: u32,
    lo: u32,
    hi: u32,
    is_testnet: bool,
    do_difficulty: bool,
}

/// Emits every section's records for scenario `sc` on `net`.
fn run_scenario(sc: &Scenario, net: &Network, out: &mut Vec<Rec>, notes: &mut Vec<String>) {
    let Scenario {
        name: scen,
        a,
        lo,
        hi,
        is_testnet,
        do_difficulty,
    } = *sc;
    let t0 = Instant::now();

    // ---- A. cheap change-point scan, chunked across threads.
    let chunk = 50_000u32;
    let chunks: Vec<(u32, u32)> = (lo..=hi)
        .step_by(chunk as usize)
        .map(|s| (s, (s + chunk - 1).min(hi)))
        .collect();
    let chunk_results = par_map(&chunks, |&(s, e)| {
        let mut recs = Vec::new();
        let mut changed = Vec::new();
        let mut prev = if s > lo {
            Some(cheap(net, Height(s - 1), is_testnet))
        } else {
            None
        };
        for h in s..=e {
            let cur = cheap(net, Height(h), is_testnet);
            let mut any = false;
            for (i, (q, k, v)) in cur.iter().enumerate() {
                let emit = match &prev {
                    None => true,
                    Some(p) => p[i].2 != *v,
                };
                if emit {
                    any = true;
                    recs.push(Rec {
                        q,
                        key: k.clone(),
                        h: Some(h),
                        v: v.clone(),
                    });
                }
            }
            if any && h != lo {
                changed.push(h);
            }
            prev = Some(cur);
        }
        (recs, changed)
    });
    let mut cheap_changes = BTreeSet::new();
    for (recs, changed) in chunk_results {
        out.extend(recs);
        cheap_changes.extend(changed);
    }
    notes.push(format!(
        "{scen}: cheap scan [{lo}, {hi}] took {:.1}s, {} change-point heights",
        t0.elapsed().as_secs_f64(),
        cheap_changes.len()
    ));

    // ---- Address-period boundaries (only where a funding stream is active).
    let t1 = Instant::now();
    // Only inside funding stream height ranges: Regtest has no streams and a 6-block interval.
    let stream_boundaries = |from: u32, to: u32| -> Vec<u32> {
        let mut v = BTreeSet::new();
        for fs in net.all_funding_streams() {
            let s = fs.height_range().start.0.max(from);
            let e = fs.height_range().end.0.saturating_sub(1).min(to);
            if s < e {
                v.extend(guard_vec(|| period_boundaries(net, s, e)));
            }
        }
        v.into_iter().collect::<Vec<u32>>()
    };
    let fs_boundaries: Vec<u32> = stream_boundaries(lo, hi);
    let boundaries = fs_boundaries.clone();

    // ---- Expensive heights.
    let mut exp_heights = BTreeSet::new();
    for &h in &cheap_changes {
        exp_heights.insert(h);
        if h > lo {
            exp_heights.insert(h - 1);
        }
    }
    let first_k = lo.div_ceil(1000) * 1000;
    for h in (first_k..=hi).step_by(1000) {
        exp_heights.insert(h);
    }
    for h in a.saturating_sub(10)..=a + 10 {
        if h >= lo && h <= hi {
            exp_heights.insert(h);
        }
    }
    for &b in &fs_boundaries {
        exp_heights.insert(b);
        if b > lo {
            exp_heights.insert(b - 1);
        }
    }
    exp_heights.insert(lo);
    // The derived ZIP 234 reissuance start is not a cheap change point, so add it explicitly.
    if let Some(s) = guard_opt(|| nsm_reissuance_height(net).map(|h| h.0)) {
        for h in [s.saturating_sub(1), s, s + 1] {
            if h >= lo && h <= hi {
                exp_heights.insert(h);
            }
        }
    }
    let exp_list: Vec<u32> = exp_heights.into_iter().collect();
    let exp_results = par_map(&exp_list, |&h| expensive(net, Height(h)));
    for r in exp_results {
        out.extend(r);
    }
    notes.push(format!(
        "{scen}: expensive quantities at {} heights (cheap change points and h-1, h%1000==0, [A-10,A+10], nsm_reissuance_height-1..=+1, {} funding-stream address-period boundaries b and b-1 (searched only inside funding stream ranges; {} total)), took {:.1}s",
        exp_list.len(),
        fs_boundaries.len(),
        boundaries.len(),
        t1.elapsed().as_secs_f64()
    ));

    // ---- B. Structural boundaries.
    out.push(Rec {
        q: "halving_heights",
        key: String::new(),
        h: None,
        v: guard(|| {
            (1..=6)
                .map(|i| {
                    height_for_halving(i, net)
                        .map(|h| h.0.to_string())
                        .unwrap_or_else(|| "none".to_string())
                })
                .collect::<Vec<_>>()
                .join(",")
        }),
    });
    for r in RECEIVERS {
        out.push(Rec {
            q: "funding_stream_ranges",
            key: rname(r),
            h: None,
            v: guard(|| {
                let v: Vec<String> = net
                    .all_funding_streams()
                    .iter()
                    .filter(|fs| fs.recipient(r).is_some())
                    .map(|fs| format!("{}..{}", fs.height_range().start.0, fs.height_range().end.0))
                    .collect();
                if v.is_empty() {
                    "none".to_string()
                } else {
                    v.join(",")
                }
            }),
        });
    }
    out.push(Rec {
        q: "nsm_reissuance_start_height",
        key: String::new(),
        h: None,
        v: guard(|| {
            nsm_reissuance_height(net)
                .map(|h| h.0.to_string())
                .unwrap_or_else(|| "none".to_string())
        }),
    });
    // Boundaries at/after A, over the whole u32 height space up to Height::MAX.
    let all_after_a = stream_boundaries(a, Height::MAX.0);
    for r in RECEIVERS {
        out.push(Rec {
            q: "address_period_boundaries",
            key: rname(r),
            h: None,
            v: guard(|| {
                if r == FundingStreamReceiver::Deferred {
                    return "none".to_string();
                }
                let v: Vec<String> = all_after_a
                    .iter()
                    .copied()
                    .filter(|&b| {
                        net.funding_streams(Height(b))
                            .is_some_and(|fs| fs.recipient(r).is_some())
                            && net
                                .funding_streams(Height(b - 1))
                                .is_some_and(|fs| fs.recipient(r).is_some())
                    })
                    .take(10)
                    .map(|b| b.to_string())
                    .collect();
                if v.is_empty() {
                    "none".to_string()
                } else {
                    v.join(",")
                }
            }),
        });
    }

    // ---- C. Fee split.
    for h in [a - 1, a, a + 1] {
        for f in FEES {
            let fee = Amount::<NonNegative>::try_from(f).expect("valid fee");
            let share = guard(|| amt(miner_fee_share(Height(h), net, fee)));
            let burn = match share.parse::<i64>() {
                Ok(s) => (f - s).to_string(),
                Err(_) => share.clone(),
            };
            out.push(Rec {
                q: "fee_burn",
                key: f.to_string(),
                h: Some(h),
                v: burn,
            });
            out.push(Rec {
                q: "miner_fee_share",
                key: f.to_string(),
                h: Some(h),
                v: share,
            });
        }
    }

    // ---- D. Difficulty.
    if do_difficulty {
        const PRE: [i64; 12] = [37, 75, 90, 60, 150, 20, 25, 25, 10, 500, 75, 30];
        const POST: [i64; 12] = [12, 25, 30, 20, 50, 25, 8, 40, 25, 480, 25, 460];
        let (start, end) = if is_testnet {
            (a - 200, a + 300)
        } else {
            (1, 600)
        };
        let start_bits = if is_testnet {
            CompactDifficulty::from_le_bytes(0x1f07ffffu32.to_le_bytes())
        } else {
            net.target_difficulty_limit().to_compact()
        };
        difficulty_chain(
            net,
            sc,
            "",
            start,
            end,
            start_bits,
            |h| {
                if h < a {
                    PRE[(h % 12) as usize]
                } else {
                    POST[(h % 12) as usize]
                }
            },
            out,
            notes,
        );

        // D2: near the target spacing and away from the PoW limit, with a slow and then a
        // fast stretch on each side of NU7 to reach the adjustment bounds.
        if is_testnet {
            const PRE2: [i64; 12] = [60, 90, 75, 70, 80, 75, 65, 85, 75, 72, 78, 75];
            const POST2: [i64; 12] = [20, 30, 25, 23, 27, 25, 22, 28, 25, 24, 26, 25];
            let d2_bits = CompactDifficulty::from_le_bytes(0x1d00ffffu32.to_le_bytes());
            difficulty_chain(
                net,
                sc,
                "_d2",
                start,
                end,
                d2_bits,
                |h| {
                    if h < a {
                        if (a - 100..a - 60).contains(&h) {
                            225
                        } else if (a - 60..a - 30).contains(&h) {
                            25
                        } else {
                            PRE2[(h % 12) as usize]
                        }
                    } else if (a + 100..a + 150).contains(&h) {
                        75
                    } else if (a + 150..a + 200).contains(&h) {
                        8
                    } else {
                        POST2[(h % 12) as usize]
                    }
                },
                out,
                notes,
            );
        }
    }

    // ---- E. Tx version validity: no public pure function.
    for h in [a - 1, a] {
        for k in [
            "v4",
            "v5",
            "v6",
            "v4-coinbase",
            "v5-coinbase",
            "v6-coinbase",
        ] {
            out.push(Rec {
                q: "tx_version_allowed",
                key: k.to_string(),
                h: Some(h),
                v: "MISSING:version-vs-upgrade checks are private Verifier::verify_v{4,5,6}_transaction_network_upgrade (zakura-consensus/src/transaction.rs:1017,1139,1226) taking a Transaction; no public pure function".to_string(),
            });
        }
    }
    notes.push(format!("{scen}: total {:.1}s", t0.elapsed().as_secs_f64()));
}

/// Builds section D's synthetic chain on `net` from `start` to `end`, whose first 29 blocks
/// have `start_bits` and whose later bits are Zakura's own expected thresholds, and emits
/// the expected bits (with `suffix` on the quantity names) from `a - 150`.
#[allow(clippy::too_many_arguments)]
fn difficulty_chain(
    net: &Network,
    sc: &Scenario,
    suffix: &'static str,
    start: u32,
    end: u32,
    start_bits: CompactDifficulty,
    interval: impl Fn(u32) -> i64,
    out: &mut Vec<Rec>,
    notes: &mut Vec<String>,
) {
    let (expected_q, mtp_q) = if suffix.is_empty() {
        ("expected_bits", "median_time_past")
    } else {
        ("expected_bits_d2", "median_time_past_d2")
    };
    const FIXED_SPAN: u32 = 28; // PoWAveragingWindow (17) + PoWMedianBlockSpan (11)
    let Scenario {
        name: scen,
        a,
        is_testnet,
        ..
    } = *sc;
    {
        notes.push(format!(
            "{scen}: difficulty{suffix} chain heights {start}..={end}, starting bits {} (fixed for heights {start}..={}), t({start}) = 1760000000",
            compact_hex(start_bits),
            start + FIXED_SPAN
        ));
        let mut times: BTreeMap<u32, i64> = BTreeMap::new();
        let mut bits: BTreeMap<u32, CompactDifficulty> = BTreeMap::new();
        times.insert(start, 1_760_000_000);
        for h in start + 1..=end {
            times.insert(h, times[&(h - 1)] + interval(h));
        }
        let dt = |s: i64| -> DateTime<Utc> { Utc.timestamp_opt(s, 0).single().unwrap() };
        for h in start..=end {
            if h <= start + FIXED_SPAN {
                bits.insert(h, start_bits);
                continue;
            }
            let span = pow_adjustment_block_span_for_height(net, Height(h)) as u32;
            let ctx: Vec<(CompactDifficulty, DateTime<Utc>)> = (h.saturating_sub(span)..h)
                .rev()
                .filter(|p| *p >= start)
                .map(|p| (bits[&p], dt(times[&p])))
                .collect();
            let adj =
                AdjustedDifficulty::new_from_header_time(dt(times[&h]), Height(h - 1), net, ctx);
            let emit = h + 150 >= a;
            match adj {
                Ok(adj) => {
                    let expected = adj.expected_difficulty_threshold();
                    bits.insert(h, expected);
                    if emit {
                        out.push(Rec {
                            q: expected_q,
                            key: String::new(),
                            h: Some(h),
                            v: compact_hex(expected),
                        });
                        out.push(Rec {
                            q: mtp_q,
                            key: String::new(),
                            h: Some(h),
                            v: adj.median_time_past().timestamp().to_string(),
                        });
                    }
                }
                Err(e) => {
                    bits.insert(h, start_bits);
                    if emit {
                        out.push(Rec {
                            q: expected_q,
                            key: String::new(),
                            h: Some(h),
                            v: format!("ERR:{e}"),
                        });
                    }
                }
            }
            if emit && is_testnet && suffix.is_empty() {
                out.push(Rec {
                    q: "is_min_difficulty_block",
                    key: String::new(),
                    h: Some(h),
                    v: NetworkUpgrade::is_testnet_min_difficulty_block(
                        net,
                        Height(h),
                        dt(times[&h]),
                        dt(times[&(h - 1)]),
                    )
                    .to_string(),
                });
            }
        }
    }
}

/// Runs `f`, turning a panic into `None`.
fn guard_opt(f: impl FnOnce() -> Option<u32>) -> Option<u32> {
    catch_unwind(AssertUnwindSafe(f)).ok().flatten()
}

/// Runs `f`, turning a panic into an empty list.
fn guard_vec(f: impl FnOnce() -> Vec<u32>) -> Vec<u32> {
    catch_unwind(AssertUnwindSafe(f)).unwrap_or_default()
}

/// Writes the dump to `NU7_DIFF_OUT` and its timing notes to `NU7_DIFF_OUT.runlog`, limited
/// to scenario `NU7_DIFF_ONLY` if it is set. Does nothing if `NU7_DIFF_OUT` is unset.
#[test]
#[ignore]
fn nu7_diff_dump() {
    let Ok(path) = std::env::var("NU7_DIFF_OUT") else {
        return;
    };
    std::panic::set_hook(Box::new(|_| {}));
    let t0 = Instant::now();
    let only = std::env::var("NU7_DIFF_ONLY").ok();
    let mut lines: Vec<String> = Vec::new();
    let mut notes: Vec<String> = Vec::new();

    let scenarios = [
        (
            Scenario {
                name: "testnet-A1",
                a: 4_200_000,
                lo: 4_198_000,
                hi: 16_200_000,
                is_testnet: true,
                do_difficulty: true,
            },
            build_testnet(4_200_000),
        ),
        (
            Scenario {
                name: "testnet-A2",
                a: 4_187_001,
                lo: 4_185_001,
                hi: 16_187_001,
                is_testnet: true,
                do_difficulty: false,
            },
            build_testnet(4_187_001),
        ),
        (
            Scenario {
                name: "testnet-real",
                a: 4_465_026,
                lo: 4_463_026,
                hi: 16_465_026,
                is_testnet: true,
                do_difficulty: true,
            },
            Ok(Network::new_default_testnet()),
        ),
        (
            Scenario {
                name: "regtest-R",
                a: 300,
                lo: 1,
                hi: 2_000_000,
                is_testnet: false,
                do_difficulty: true,
            },
            build_regtest(),
        ),
    ];

    for (sc, net) in scenarios {
        let scen = sc.name;
        if only.as_deref().is_some_and(|o| o != scen) {
            continue;
        }
        let net = match net {
            Ok(n) => n,
            Err(e) => {
                lines.push(format!(
                    "{{\"scenario\": \"{scen}\", \"quantity\": \"SCENARIO\", \"key\": \"\", \"height\": null, \"value\": \"ERR:{}\"}}",
                    esc(&e)
                ));
                continue;
            }
        };
        let mut recs = Vec::new();
        run_scenario(&sc, &net, &mut recs, &mut notes);
        for r in recs {
            let h =
                r.h.map(|h| h.to_string())
                    .unwrap_or_else(|| "null".to_string());
            lines.push(format!(
                "{{\"scenario\": \"{scen}\", \"quantity\": \"{}\", \"key\": \"{}\", \"height\": {h}, \"value\": \"{}\"}}",
                r.q,
                esc(&r.key),
                esc(&r.v)
            ));
        }
    }

    let mut f = std::fs::File::create(&path).expect("the output file can be created");
    for l in &lines {
        writeln!(f, "{l}").expect("the output file is writable");
    }
    let mut runlog =
        std::fs::File::create(format!("{path}.runlog")).expect("the run log can be created");
    for n in &notes {
        writeln!(runlog, "{n}").expect("the run log is writable");
    }
    writeln!(
        runlog,
        "rows: {}, total {:.1}s",
        lines.len(),
        t0.elapsed().as_secs_f64()
    )
    .expect("the run log is writable");
}
