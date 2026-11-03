//! Whole-process CPU and memory readings for manual load probes.
//!
//! Both read `/proc/self` and return `None` on hosts without it, such as macOS.

/// User plus system CPU seconds this process has used, from `/proc/self/stat`.
pub fn cpu_seconds() -> Option<f64> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    // Fields after the parenthesized command name; utime and stime are 14 and 15.
    let fields: Vec<&str> = stat.rsplit_once(')')?.1.split_whitespace().collect();
    let ticks = fields.get(11)?.parse::<f64>().ok()? + fields.get(12)?.parse::<f64>().ok()?;
    // Linux reports these in USER_HZ, which is 100 on supported targets.
    Some(ticks / 100.0)
}

/// Current and peak resident memory in KiB (`VmRSS`, `VmHWM`), from `/proc/self/status`.
pub fn memory_kib() -> Option<(u64, u64)> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let field = |name: &str| {
        status
            .lines()
            .find_map(|line| line.strip_prefix(name))?
            .trim()
            .trim_end_matches(" kB")
            .parse()
            .ok()
    };
    Some((field("VmRSS:")?, field("VmHWM:")?))
}
