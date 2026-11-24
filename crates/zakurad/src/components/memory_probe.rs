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

use eyre::{eyre, Report};

#[cfg(target_os = "linux")]
pub use linux::read_process_rss_bytes;

/// Detect the tightest host or cgroup memory limit for this process, in bytes.
///
/// Any failure explains how to set `[memory] budget` explicitly (`02 §2`).
pub fn detect_host_memory_bytes() -> Result<u64, Report> {
    #[cfg(target_os = "linux")]
    {
        linux::detect_memory_bytes(&linux::ProbePaths::system())
    }

    #[cfg(all(unix, not(target_os = "linux")))]
    {
        detect_unix_sysconf_memory_bytes().map_err(with_budget_hint)
    }

    #[cfg(not(unix))]
    {
        Err(with_budget_hint(eyre!(
            "automatic host memory detection is not supported on this platform"
        )))
    }
}

/// `error`, with how to set `[memory] budget` explicitly instead.
fn with_budget_hint(error: Report) -> Report {
    eyre!("{error}; set [memory] budget explicitly in zakura.toml")
}

#[cfg(all(unix, not(target_os = "linux")))]
#[allow(unsafe_code)]
fn detect_unix_sysconf_memory_bytes() -> Result<u64, Report> {
    // SAFETY: `sysconf` takes only integer selector constants and returns a
    // scalar configuration value or `-1` on failure.
    let pages = unsafe { libc::sysconf(libc::_SC_PHYS_PAGES) };
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if pages <= 0 || page_size <= 0 {
        return Err(eyre!("failed to detect physical memory via sysconf"));
    }
    let pages = u64::try_from(pages).map_err(|_| eyre!("invalid sysconf _SC_PHYS_PAGES"))?;
    let page_size = u64::try_from(page_size).map_err(|_| eyre!("invalid sysconf _SC_PAGESIZE"))?;
    Ok(pages.saturating_mul(page_size))
}

#[cfg(target_os = "linux")]
mod linux {
    use std::{
        fs, io,
        path::{Path, PathBuf},
    };

    use eyre::{eyre, Report};

    use super::with_budget_hint;

    /// Cgroup v1 unlimited sentinel threshold (`02 §2`, matching Linux kernel
    /// `PAGE_COUNTER_MAX` / `LONG_MAX` rounded down to page alignment).
    const CGROUP_UNLIMITED_SENTINEL: u64 = 0x7fff_ffff_ffff_f000;

    /// Sample the current process's resident set size (VmRSS) in bytes
    /// (`03 §5`: `zakura.memory.process_rss_bytes`, sampled every 10 s).
    pub fn read_process_rss_bytes() -> Option<u64> {
        let status = fs::read_to_string("/proc/self/status").ok()?;
        proc_kib_field_bytes(&status, "VmRSS:")
    }

    /// The bytes of the first `/proc` field named `prefix`, which `/proc`
    /// reports in KiB.
    pub(super) fn proc_kib_field_bytes(contents: &str, prefix: &str) -> Option<u64> {
        let kib = contents
            .lines()
            .find_map(|line| line.strip_prefix(prefix))?
            .split_whitespace()
            .next()?
            .parse::<u64>()
            .ok()?;
        Some(kib.saturating_mul(1024))
    }

    #[derive(Clone, Copy, Debug)]
    pub(super) struct ProbePaths<'a> {
        pub(super) meminfo: &'a Path,
        pub(super) self_cgroup: &'a Path,
        pub(super) cgroup_v2_root: &'a Path,
        pub(super) cgroup_v1_memory_root: &'a Path,
    }

    impl ProbePaths<'static> {
        pub(super) fn system() -> Self {
            Self {
                meminfo: Path::new("/proc/meminfo"),
                self_cgroup: Path::new("/proc/self/cgroup"),
                cgroup_v2_root: Path::new("/sys/fs/cgroup"),
                cgroup_v1_memory_root: Path::new("/sys/fs/cgroup/memory"),
            }
        }
    }

    /// The smallest of `MemTotal` and every finite cgroup limit under `paths`.
    pub(super) fn detect_memory_bytes(paths: &ProbePaths<'_>) -> Result<u64, Report> {
        let detect = || {
            let mem_total = read_meminfo_total_bytes(paths.meminfo)?;
            let cgroup_limit = read_cgroup_memory_limit_bytes(paths)?;
            Ok(cgroup_limit.map_or(mem_total, |limit| limit.min(mem_total)))
        };
        detect().map_err(with_budget_hint)
    }

    fn read_meminfo_total_bytes(meminfo_path: &Path) -> Result<u64, Report> {
        let meminfo = fs::read_to_string(meminfo_path)
            .map_err(|error| eyre!("failed to read {}: {error}", meminfo_path.display()))?;
        match proc_kib_field_bytes(&meminfo, "MemTotal:") {
            Some(0) => Err(eyre!("MemTotal in {} is 0", meminfo_path.display())),
            Some(bytes) => Ok(bytes),
            None => Err(eyre!(
                "MemTotal field missing or unparsable in {}",
                meminfo_path.display()
            )),
        }
    }

    fn read_cgroup_memory_limit_bytes(paths: &ProbePaths<'_>) -> Result<Option<u64>, Report> {
        let Some(self_cgroup) = read_optional(paths.self_cgroup)? else {
            return Ok(None);
        };
        let (v2_rel, v1_rel) = parse_self_cgroup_paths(&self_cgroup);
        let mut min_limit: Option<u64> = None;

        // cgroup v2: read `memory.max` and `memory.high` across EVERY directory
        // from the process's cgroup up to `/sys/fs/cgroup` inclusive (`02 §2`).
        if let Some(rel) = v2_rel {
            for dir in cgroup_ancestor_dirs(paths.cgroup_v2_root, &rel) {
                for file in ["memory.max", "memory.high"] {
                    tighten(&mut min_limit, parse_cgroup_limit_file(&dir.join(file))?);
                }
            }
        }

        // cgroup v1: read `memory.limit_in_bytes` and `memory.stat`'s
        // `hierarchical_memory_limit` across every ancestor directory (`02 §2`).
        if let Some(rel) = v1_rel {
            for dir in cgroup_ancestor_dirs(paths.cgroup_v1_memory_root, &rel) {
                tighten(
                    &mut min_limit,
                    parse_cgroup_limit_file(&dir.join("memory.limit_in_bytes"))?,
                );
                tighten(
                    &mut min_limit,
                    parse_cgroup_v1_hierarchical_limit(&dir.join("memory.stat"))?,
                );
            }
        }

        Ok(min_limit)
    }

    /// Lowers `min_limit` to `limit` when `limit` is finite and smaller.
    fn tighten(min_limit: &mut Option<u64>, limit: Option<u64>) {
        if let Some(limit) = limit {
            *min_limit = Some(min_limit.map_or(limit, |current| current.min(limit)));
        }
    }

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

    /// The contents of `path`, or `None` when it doesn't exist.
    fn read_optional(path: &Path) -> Result<Option<String>, Report> {
        match fs::read_to_string(path) {
            Ok(contents) => Ok(Some(contents)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(eyre!("failed to read {}: {error}", path.display())),
        }
    }

    fn parse_cgroup_limit_str(raw: &str, path: &Path) -> Result<Option<u64>, Report> {
        let trimmed = raw.trim();
        if trimmed.eq_ignore_ascii_case("max") {
            return Ok(None);
        }
        let parsed = trimmed.parse::<u64>().map_err(|error| {
            eyre!(
                "failed to parse cgroup memory limit from {}: {error}",
                path.display()
            )
        })?;
        Ok((parsed < CGROUP_UNLIMITED_SENTINEL).then_some(parsed))
    }

    fn parse_cgroup_limit_file(path: &Path) -> Result<Option<u64>, Report> {
        match read_optional(path)? {
            Some(raw) => parse_cgroup_limit_str(&raw, path),
            None => Ok(None),
        }
    }

    fn parse_cgroup_v1_hierarchical_limit(stat_path: &Path) -> Result<Option<u64>, Report> {
        let Some(raw) = read_optional(stat_path)? else {
            return Ok(None);
        };
        let limit = raw.lines().find_map(|line| {
            let token = line.strip_prefix("hierarchical_memory_limit")?.trim();
            (!token.is_empty()).then_some(token)
        });
        match limit {
            Some(token) => parse_cgroup_limit_str(token, stat_path),
            None => Ok(None),
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use std::fs;

    use tempfile::TempDir;

    use super::linux::{detect_memory_bytes, proc_kib_field_bytes, ProbePaths};
    use super::*;

    const GIB: u64 = 1024 * 1024 * 1024;
    const V2: &str = "sys/fs/cgroup";
    const V1: &str = "sys/fs/cgroup/memory";

    struct SyntheticTree {
        dir: TempDir,
    }

    impl SyntheticTree {
        fn new(mem_total_gib: u64, self_cgroup: &str) -> Self {
            let dir = TempDir::new().expect("tempdir");
            fs::create_dir_all(dir.path().join("proc/self")).unwrap();
            fs::create_dir_all(dir.path().join(V1)).unwrap();
            // Include a small MemAvailable to confirm the probe ignores it (`02 §2`).
            let meminfo = format!(
                "MemTotal:       {} kB\nMemFree:        1024 kB\nMemAvailable:   2048 kB\n",
                mem_total_gib * 1024 * 1024
            );
            fs::write(dir.path().join("proc/meminfo"), meminfo).unwrap();
            fs::write(dir.path().join("proc/self/cgroup"), self_cgroup).unwrap();
            Self { dir }
        }

        /// Writes `value` to `file` in the cgroup `rel_dir` under the hierarchy
        /// `root` ([`V2`] or [`V1`]).
        fn write(&self, root: &str, rel_dir: &str, file: &str, value: &str) {
            let target_dir = self.dir.path().join(root).join(rel_dir.trim_matches('/'));
            fs::create_dir_all(&target_dir).unwrap();
            fs::write(target_dir.join(file), format!("{value}\n")).unwrap();
        }

        fn probe(&self) -> Result<u64, Report> {
            let meminfo = self.dir.path().join("proc/meminfo");
            let self_cgroup = self.dir.path().join("proc/self/cgroup");
            let v2_root = self.dir.path().join(V2);
            let v1_root = self.dir.path().join(V1);
            detect_memory_bytes(&ProbePaths {
                meminfo: &meminfo,
                self_cgroup: &self_cgroup,
                cgroup_v2_root: &v2_root,
                cgroup_v1_memory_root: &v1_root,
            })
        }
    }

    /// One cgroup file write: hierarchy root, cgroup directory, file, value.
    type Write = (&'static str, &'static str, &'static str, &'static str);

    /// One probe case: name, `MemTotal` in GiB, `/proc/self/cgroup`, and steps
    /// of writes, each followed by the probe's expected GiB.
    type Case = (
        &'static str,
        u64,
        &'static str,
        &'static [(&'static [Write], u64)],
    );

    const V2_LEAF: &str = "/system.slice/zakura.slice/worker.scope";
    const V2_PARENT: &str = "/system.slice/zakura.slice";
    const V1_LEAF: &str = "/kubepods/burstable/pod123";
    /// The cgroup v1 unlimited sentinel, and `i64::MAX`.
    const SENTINEL: &str = "9223372036854771712";
    const I64_MAX: &str = "9223372036854775807";

    const CASES: [Case; 3] = [
        // Root unlimited, an intermediate ancestor sets a tight `memory.high`,
        // a deeper parent sets `memory.max`, the leaf is `max`. Then
        // tightening the leaf's `memory.max` wins.
        (
            "nested v2",
            64,
            "0::/system.slice/zakura.slice/worker.scope\n",
            &[
                (
                    &[
                        (V2, "/", "memory.max", "max"),
                        (V2, "/system.slice", "memory.max", "32G"),
                        (V2, "/system.slice", "memory.high", "12G"),
                        (V2, V2_PARENT, "memory.max", "24G"),
                        (V2, V2_PARENT, "memory.high", "max"),
                        (V2, V2_LEAF, "memory.max", "max"),
                        (V2, V2_LEAF, "memory.high", "max"),
                    ],
                    12,
                ),
                (&[(V2, V2_LEAF, "memory.max", "8G")], 8),
            ],
        ),
        // Unlimited sentinels at the leaf and root, a tighter limit at an
        // intermediate ancestor. Then a finite `hierarchical_memory_limit`
        // tighter than the ancestor wins.
        (
            "nested v1",
            64,
            "4:memory:/kubepods/burstable/pod123\n",
            &[
                (
                    &[
                        (V1, "/", "memory.limit_in_bytes", SENTINEL),
                        (V1, "/kubepods", "memory.limit_in_bytes", "20G"),
                        (V1, "/kubepods/burstable", "memory.limit_in_bytes", I64_MAX),
                        (V1, V1_LEAF, "memory.limit_in_bytes", SENTINEL),
                        (V1, V1_LEAF, "memory.stat", SENTINEL),
                    ],
                    20,
                ),
                (&[(V1, V1_LEAF, "memory.stat", "14G")], 14),
            ],
        ),
        // `MemTotal` is tighter than both hierarchies. Then tightening a v1
        // ancestor below `MemTotal` takes effect.
        (
            "hybrid v1 + v2",
            16,
            "0::/user.slice/app.scope\n2:cpu,memory:/containers/app\n",
            &[
                (
                    &[
                        (V2, "/user.slice", "memory.max", "32G"),
                        (V1, "/containers/app", "memory.limit_in_bytes", "24G"),
                    ],
                    16,
                ),
                (&[(V1, "/containers", "memory.limit_in_bytes", "10G")], 10),
            ],
        ),
    ];

    /// mem-8: the probe takes the minimum of `MemTotal` and every finite
    /// limit across every cgroup v2 and v1 ancestor, ignoring `"max"`, the
    /// v1 unlimited sentinels and `MemAvailable`.
    #[test]
    fn mem8_probe_takes_the_minimum_across_memtotal_and_cgroup_ancestors() {
        for (name, mem_total_gib, self_cgroup, steps) in CASES {
            let tree = SyntheticTree::new(mem_total_gib, self_cgroup);
            for &(writes, expected_gib) in steps {
                for &(root, rel_dir, file, value) in writes {
                    // `N G` is N GiB. `memory.stat` holds the limit among
                    // other fields.
                    let value = match value.strip_suffix('G') {
                        Some(gib) => (gib.parse::<u64>().unwrap() * GIB).to_string(),
                        None => value.to_owned(),
                    };
                    let value = if file == "memory.stat" {
                        format!("cache 4096\nhierarchical_memory_limit {value}\nrss 8192")
                    } else {
                        value
                    };
                    tree.write(root, rel_dir, file, &value);
                }
                assert_eq!(tree.probe().unwrap(), expected_gib * GIB, "{name}");
            }
        }
    }

    /// Unreadable `/proc/meminfo` fails and names `[memory] budget` (`02 §2`).
    #[test]
    fn unreadable_probe_names_memory_budget_setting() {
        let dir = TempDir::new().expect("tempdir");
        let missing = dir.path().join("no-meminfo");
        let self_cgroup = dir.path().join("no-cgroup");
        let err = detect_memory_bytes(&ProbePaths {
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
        assert_eq!(proc_kib_field_bytes(sample, "VmRSS:"), Some(524_288 * 1024));
        assert_eq!(proc_kib_field_bytes("Name:\tzakurad\n", "VmRSS:"), None);
    }
}
