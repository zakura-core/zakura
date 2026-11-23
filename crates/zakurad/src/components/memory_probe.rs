//! Host and cgroup memory limit probing (`02 §2`, `mem-8`, `03 §5`).
//!
//! Shared by both `[memory] budget = "auto"` and `zcashd-compat` preflight:
//!
//! - **Linux:** reads `MemTotal` from `/proc/meminfo`, walks every cgroup v2
//!   ancestor from the process's cgroup up to `/sys/fs/cgroup` reading both
//!   `memory.max` and `memory.high`, walks every cgroup v1 `memory` ancestor
//!   reading `memory.limit_in_bytes` and `memory.stat`'s
//!   `hierarchical_memory_limit` (ignoring `"max"` and unlimited sentinels),
//!   and returns the minimum finite limit. Ignores `MemAvailable` and
//!   `RLIMIT_AS`.
//! - **Other Unix targets:** queries physical RAM via `sysconf(_SC_PHYS_PAGES)
//!   * sysconf(_SC_PAGESIZE)`. No container detection.
//! - **Unreadable / unsupported:** returns an error naming `[memory] budget`.

#[cfg(target_os = "linux")]
use std::{
    fs, io,
    path::{Path, PathBuf},
};

use eyre::{eyre, Report};

/// Cgroup v1 unlimited sentinel threshold (`02 §2`, matching Linux kernel
/// `PAGE_COUNTER_MAX` / `LONG_MAX` rounded down to page alignment).
#[cfg(target_os = "linux")]
const CGROUP_UNLIMITED_SENTINEL: u64 = 0x7fff_ffff_ffff_f000;

/// Detect the tightest host or cgroup memory limit for this process, in bytes.
///
/// Any failure explains how to set `[memory] budget` explicitly (`02 §2`).
pub fn detect_host_memory_bytes() -> Result<u64, Report> {
    #[cfg(target_os = "linux")]
    {
        detect_linux_memory_bytes(&LinuxProbePaths::system())
    }

    #[cfg(all(unix, not(target_os = "linux")))]
    {
        detect_unix_sysconf_memory_bytes()
    }

    #[cfg(not(unix))]
    {
        Err(eyre!(
            "automatic host memory detection is not supported on this platform; \
             set [memory] budget explicitly in zakura.toml"
        ))
    }
}

/// Sample the current process's resident set size (VmRSS) in bytes on Linux
/// (`03 §5`: `zakura.memory.process_rss_bytes`, sampled every 10 s).
#[cfg(target_os = "linux")]
pub fn read_process_rss_bytes() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    parse_proc_status_vm_rss_bytes(&status)
}

#[cfg(target_os = "linux")]
fn parse_proc_status_vm_rss_bytes(status: &str) -> Option<u64> {
    let kib = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))?
        .split_whitespace()
        .next()?
        .parse::<u64>()
        .ok()?;
    Some(kib.saturating_mul(1024))
}

#[cfg(all(unix, not(target_os = "linux")))]
#[allow(unsafe_code)]
fn detect_unix_sysconf_memory_bytes() -> Result<u64, Report> {
    // SAFETY: `sysconf` takes only integer selector constants and returns a
    // scalar configuration value or `-1` on failure.
    let pages = unsafe { libc::sysconf(libc::_SC_PHYS_PAGES) };
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if pages <= 0 || page_size <= 0 {
        return Err(eyre!(
            "failed to detect physical memory via sysconf; \
             set [memory] budget explicitly in zakura.toml"
        ));
    }
    let pages = u64::try_from(pages).map_err(|_| {
        eyre!("invalid sysconf _SC_PHYS_PAGES; set [memory] budget explicitly in zakura.toml")
    })?;
    let page_size = u64::try_from(page_size).map_err(|_| {
        eyre!("invalid sysconf _SC_PAGESIZE; set [memory] budget explicitly in zakura.toml")
    })?;
    Ok(pages.saturating_mul(page_size))
}

#[cfg(target_os = "linux")]
#[derive(Clone, Copy, Debug)]
struct LinuxProbePaths<'a> {
    meminfo: &'a Path,
    self_cgroup: &'a Path,
    cgroup_v2_root: &'a Path,
    cgroup_v1_memory_root: &'a Path,
}

#[cfg(target_os = "linux")]
impl LinuxProbePaths<'static> {
    fn system() -> Self {
        Self {
            meminfo: Path::new("/proc/meminfo"),
            self_cgroup: Path::new("/proc/self/cgroup"),
            cgroup_v2_root: Path::new("/sys/fs/cgroup"),
            cgroup_v1_memory_root: Path::new("/sys/fs/cgroup/memory"),
        }
    }
}

#[cfg(target_os = "linux")]
fn detect_linux_memory_bytes(paths: &LinuxProbePaths<'_>) -> Result<u64, Report> {
    let mem_total = read_meminfo_total_bytes(paths.meminfo)?;
    let cgroup_limit = read_cgroup_memory_limit_bytes(paths)?;
    Ok(cgroup_limit.map_or(mem_total, |limit| limit.min(mem_total)))
}

#[cfg(target_os = "linux")]
fn read_meminfo_total_bytes(meminfo_path: &Path) -> Result<u64, Report> {
    let meminfo = fs::read_to_string(meminfo_path).map_err(|error| {
        eyre!(
            "failed to read {}: {error}; set [memory] budget explicitly in zakura.toml",
            meminfo_path.display()
        )
    })?;
    let mem_total_kib = meminfo
        .lines()
        .find_map(|line| line.strip_prefix("MemTotal:"))
        .and_then(|line| line.split_whitespace().next())
        .ok_or_else(|| {
            eyre!(
                "MemTotal field missing in {}; set [memory] budget explicitly in zakura.toml",
                meminfo_path.display()
            )
        })?
        .parse::<u64>()
        .map_err(|error| {
            eyre!(
                "failed to parse MemTotal from {}: {error}; \
                 set [memory] budget explicitly in zakura.toml",
                meminfo_path.display()
            )
        })?;

    if mem_total_kib == 0 {
        return Err(eyre!(
            "MemTotal in {} is 0; set [memory] budget explicitly in zakura.toml",
            meminfo_path.display()
        ));
    }

    Ok(mem_total_kib.saturating_mul(1024))
}

#[cfg(target_os = "linux")]
fn read_cgroup_memory_limit_bytes(paths: &LinuxProbePaths<'_>) -> Result<Option<u64>, Report> {
    let self_cgroup = match fs::read_to_string(paths.self_cgroup) {
        Ok(contents) => contents,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(eyre!(
                "failed to read {}: {error}; set [memory] budget explicitly in zakura.toml",
                paths.self_cgroup.display()
            ));
        }
    };

    let (v2_rel, v1_rel) = parse_self_cgroup_paths(&self_cgroup);
    let mut min_limit: Option<u64> = None;

    // cgroup v2: read `memory.max` and `memory.high` across EVERY directory
    // from the process's cgroup up to `/sys/fs/cgroup` inclusive (`02 §2`).
    if let Some(rel) = v2_rel.as_deref() {
        for dir in cgroup_ancestor_dirs(paths.cgroup_v2_root, rel) {
            for file in ["memory.max", "memory.high"] {
                if let Some(limit) = parse_cgroup_limit_file(&dir.join(file))? {
                    min_limit = Some(min_limit.map_or(limit, |cur| cur.min(limit)));
                }
            }
        }
    }

    // cgroup v1: read `memory.limit_in_bytes` and `memory.stat`'s
    // `hierarchical_memory_limit` across every ancestor directory (`02 §2`).
    if let Some(rel) = v1_rel.as_deref() {
        for dir in cgroup_ancestor_dirs(paths.cgroup_v1_memory_root, rel) {
            if let Some(limit) = parse_cgroup_limit_file(&dir.join("memory.limit_in_bytes"))? {
                min_limit = Some(min_limit.map_or(limit, |cur| cur.min(limit)));
            }
            if let Some(limit) = parse_cgroup_v1_hierarchical_limit(&dir.join("memory.stat"))? {
                min_limit = Some(min_limit.map_or(limit, |cur| cur.min(limit)));
            }
        }
    }

    Ok(min_limit)
}

#[cfg(target_os = "linux")]
fn parse_self_cgroup_paths(self_cgroup: &str) -> (Option<String>, Option<String>) {
    let mut v2_relative_path = None;
    let mut v1_memory_relative_path = None;

    for line in self_cgroup.lines() {
        let mut fields = line.splitn(3, ':');
        let _hierarchy_id = fields.next();
        let controllers = fields.next();
        let relative_path = fields.next();

        let (Some(controllers), Some(relative_path)) = (controllers, relative_path) else {
            continue;
        };

        if controllers.is_empty() {
            v2_relative_path = Some(relative_path.to_string());
        } else if controllers
            .split(',')
            .any(|controller| controller == "memory")
        {
            v1_memory_relative_path = Some(relative_path.to_string());
        }
    }

    (v2_relative_path, v1_memory_relative_path)
}

/// Returns every directory from `base` down to `base/<relative_path>`,
/// inclusive, skipping unsafe `.` or `..` components.
#[cfg(target_os = "linux")]
fn cgroup_ancestor_dirs(base: &Path, relative_path: &str) -> Vec<PathBuf> {
    let mut dirs = vec![base.to_path_buf()];
    let mut current = base.to_path_buf();
    for segment in relative_path.trim_matches('/').split('/') {
        if segment.is_empty() || segment == "." || segment == ".." {
            continue;
        }
        current.push(segment);
        dirs.push(current.clone());
    }
    dirs
}

#[cfg(target_os = "linux")]
fn parse_cgroup_limit_str(raw: &str, path: &Path) -> Result<Option<u64>, Report> {
    let trimmed = raw.trim();
    if trimmed.eq_ignore_ascii_case("max") {
        return Ok(None);
    }

    let parsed = trimmed.parse::<u64>().map_err(|error| {
        eyre!(
            "failed to parse cgroup memory limit from {}: {error}; \
             set [memory] budget explicitly in zakura.toml",
            path.display()
        )
    })?;

    if parsed >= CGROUP_UNLIMITED_SENTINEL {
        return Ok(None);
    }

    Ok(Some(parsed))
}

#[cfg(target_os = "linux")]
fn parse_cgroup_limit_file(path: &Path) -> Result<Option<u64>, Report> {
    let raw = match fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(eyre!(
                "failed to read {}: {error}; set [memory] budget explicitly in zakura.toml",
                path.display()
            ));
        }
    };
    parse_cgroup_limit_str(&raw, path)
}

#[cfg(target_os = "linux")]
fn parse_cgroup_v1_hierarchical_limit(stat_path: &Path) -> Result<Option<u64>, Report> {
    let raw = match fs::read_to_string(stat_path) {
        Ok(raw) => raw,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(eyre!(
                "failed to read {}: {error}; set [memory] budget explicitly in zakura.toml",
                stat_path.display()
            ));
        }
    };

    for line in raw.lines() {
        if let Some(value) = line.strip_prefix("hierarchical_memory_limit") {
            let token = value.trim();
            if token.is_empty() {
                continue;
            }
            return parse_cgroup_limit_str(token, stat_path);
        }
    }

    Ok(None)
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use tempfile::TempDir;

    const GIB: u64 = 1024 * 1024 * 1024;

    struct SyntheticTree {
        dir: TempDir,
    }

    impl SyntheticTree {
        fn new(mem_total_gib: u64, self_cgroup: &str) -> Self {
            let dir = TempDir::new().expect("tempdir");
            fs::create_dir_all(dir.path().join("proc/self")).unwrap();
            fs::create_dir_all(dir.path().join("sys/fs/cgroup/memory")).unwrap();
            // Include a small MemAvailable to confirm the probe ignores it (`02 §2`).
            let meminfo = format!(
                "MemTotal:       {} kB\nMemFree:        1024 kB\nMemAvailable:   2048 kB\n",
                mem_total_gib * 1024 * 1024
            );
            fs::write(dir.path().join("proc/meminfo"), meminfo).unwrap();
            fs::write(dir.path().join("proc/self/cgroup"), self_cgroup).unwrap();
            Self { dir }
        }

        fn write_v2(&self, rel_dir: &str, file: &str, value: &str) {
            let base = self.dir.path().join("sys/fs/cgroup");
            let target_dir = if rel_dir.trim_matches('/').is_empty() {
                base
            } else {
                base.join(rel_dir.trim_matches('/'))
            };
            fs::create_dir_all(&target_dir).unwrap();
            fs::write(target_dir.join(file), format!("{value}\n")).unwrap();
        }

        fn write_v1(&self, rel_dir: &str, file: &str, value: &str) {
            let base = self.dir.path().join("sys/fs/cgroup/memory");
            let target_dir = if rel_dir.trim_matches('/').is_empty() {
                base
            } else {
                base.join(rel_dir.trim_matches('/'))
            };
            fs::create_dir_all(&target_dir).unwrap();
            fs::write(target_dir.join(file), format!("{value}\n")).unwrap();
        }

        fn probe(&self) -> Result<u64, Report> {
            let meminfo = self.dir.path().join("proc/meminfo");
            let self_cgroup = self.dir.path().join("proc/self/cgroup");
            let v2_root = self.dir.path().join("sys/fs/cgroup");
            let v1_root = self.dir.path().join("sys/fs/cgroup/memory");
            detect_linux_memory_bytes(&LinuxProbePaths {
                meminfo: &meminfo,
                self_cgroup: &self_cgroup,
                cgroup_v2_root: &v2_root,
                cgroup_v1_memory_root: &v1_root,
            })
        }
    }

    /// mem-8: walks every nested cgroup v2 ancestor and takes the minimum of
    /// `MemTotal`, `memory.max`, and `memory.high`, ignoring `"max"`.
    #[test]
    fn mem8_walks_nested_cgroup_v2_ancestors_for_max_and_high() {
        let tree = SyntheticTree::new(64, "0::/system.slice/zakura.slice/worker.scope\n");
        // Root unlimited, intermediate ancestor sets tight `memory.high = 12 GiB`,
        // deeper parent sets `memory.max = 24 GiB`, leaf is `max`.
        tree.write_v2("/", "memory.max", "max");
        tree.write_v2("/system.slice", "memory.max", &(32 * GIB).to_string());
        tree.write_v2("/system.slice", "memory.high", &(12 * GIB).to_string());
        tree.write_v2(
            "/system.slice/zakura.slice",
            "memory.max",
            &(24 * GIB).to_string(),
        );
        tree.write_v2("/system.slice/zakura.slice", "memory.high", "max");
        tree.write_v2(
            "/system.slice/zakura.slice/worker.scope",
            "memory.max",
            "max",
        );
        tree.write_v2(
            "/system.slice/zakura.slice/worker.scope",
            "memory.high",
            "max",
        );

        assert_eq!(tree.probe().unwrap(), 12 * GIB);

        // Tightening leaf `memory.max` wins over the ancestor's `memory.high`.
        tree.write_v2(
            "/system.slice/zakura.slice/worker.scope",
            "memory.max",
            &(8 * GIB).to_string(),
        );
        assert_eq!(tree.probe().unwrap(), 8 * GIB);
    }

    /// mem-8: reads cgroup v1 `memory.limit_in_bytes` across every ancestor
    /// plus `memory.stat`'s `hierarchical_memory_limit`, ignoring v1 sentinels.
    #[test]
    fn mem8_reads_cgroup_v1_ancestors_and_hierarchical_limit_ignoring_sentinels() {
        let tree = SyntheticTree::new(64, "4:memory:/kubepods/burstable/pod123\n");
        // Unlimited sentinel at leaf and root, tighter limit at intermediate ancestor.
        tree.write_v1("/", "memory.limit_in_bytes", "9223372036854771712");
        tree.write_v1(
            "/kubepods",
            "memory.limit_in_bytes",
            &(20 * GIB).to_string(),
        );
        tree.write_v1(
            "/kubepods/burstable",
            "memory.limit_in_bytes",
            "9223372036854775807",
        );
        tree.write_v1(
            "/kubepods/burstable/pod123",
            "memory.limit_in_bytes",
            "9223372036854771712",
        );
        tree.write_v1(
            "/kubepods/burstable/pod123",
            "memory.stat",
            "cache 4096\nhierarchical_memory_limit 9223372036854771712\nrss 8192",
        );

        assert_eq!(tree.probe().unwrap(), 20 * GIB);

        // A finite `hierarchical_memory_limit` tighter than ancestor wins.
        tree.write_v1(
            "/kubepods/burstable/pod123",
            "memory.stat",
            &format!(
                "cache 4096\nhierarchical_memory_limit {}\nrss 8192",
                14 * GIB
            ),
        );
        assert_eq!(tree.probe().unwrap(), 14 * GIB);
    }

    /// mem-8: hybrid cgroup v1 + v2 + MemTotal takes the minimum finite value
    /// across all of them and ignores `MemAvailable`.
    #[test]
    fn mem8_takes_minimum_across_memtotal_v2_and_v1() {
        let tree = SyntheticTree::new(
            16,
            "0::/user.slice/app.scope\n2:cpu,memory:/containers/app\n",
        );
        tree.write_v2("/user.slice", "memory.max", &(32 * GIB).to_string());
        tree.write_v1(
            "/containers/app",
            "memory.limit_in_bytes",
            &(24 * GIB).to_string(),
        );
        // MemTotal (16 GiB) is tighter than both cgroups (and MemAvailable=2 MiB is ignored).
        assert_eq!(tree.probe().unwrap(), 16 * GIB);

        // Tightening v1 below MemTotal takes effect.
        tree.write_v1(
            "/containers",
            "memory.limit_in_bytes",
            &(10 * GIB).to_string(),
        );
        assert_eq!(tree.probe().unwrap(), 10 * GIB);
    }

    /// Unreadable `/proc/meminfo` fails and names `[memory] budget` (`02 §2`).
    #[test]
    fn unreadable_probe_names_memory_budget_setting() {
        let dir = TempDir::new().expect("tempdir");
        let missing = dir.path().join("no-meminfo");
        let self_cgroup = dir.path().join("no-cgroup");
        let err = detect_linux_memory_bytes(&LinuxProbePaths {
            meminfo: &missing,
            self_cgroup: &self_cgroup,
            cgroup_v2_root: dir.path(),
            cgroup_v1_memory_root: dir.path(),
        })
        .unwrap_err()
        .to_string();
        assert!(err.contains("[memory] budget"), "unexpected error: {err}");
    }

    #[test]
    fn parses_proc_status_vm_rss() {
        let sample = "Name:\tzakurad\nVmSize:\t  123456 kB\nVmRSS:\t  524288 kB\nThreads:\t8\n";
        assert_eq!(parse_proc_status_vm_rss_bytes(sample), Some(524_288 * 1024));
        assert_eq!(parse_proc_status_vm_rss_bytes("Name:\tzakurad\n"), None);
    }
}
