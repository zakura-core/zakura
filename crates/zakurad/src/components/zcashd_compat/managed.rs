use std::{
    fs::{self, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
    thread::sleep,
    time::{Duration, Instant},
};

use color_eyre::eyre::{eyre, Report};
use reqwest::{blocking::Client, redirect::Policy, Url};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;

use super::{
    Config, ConfigZcashdBinarySource, ZcashdReleaseManifest, EMBEDDED_ZCASHD_RELEASE_MANIFEST,
};
use crate::components::zcashd_compat::supervisor::is_command_resolvable;

/// Maximum time a managed binary download is allowed to take.
const MANAGED_DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(10 * 60);
/// Maximum number of redirects a managed binary download may follow.
const MANAGED_DOWNLOAD_MAX_REDIRECTS: usize = 5;
/// Largest managed binary download accepted, well above the pinned executable's size.
const MANAGED_BINARY_MAX_BYTES: u64 = 512 * 1024 * 1024;
/// Buffer size for streaming downloads and hashing cached binaries.
const MANAGED_BINARY_BUFFER_BYTES: usize = 64 * 1024;
/// Maximum time a process waits for another live process to finish a managed install.
const INSTALL_LOCK_WAIT_TIMEOUT: Duration = Duration::from_secs(15 * 60);
/// Stale age for legacy lock files that do not contain owner metadata.
const LEGACY_INSTALL_LOCK_STALE_AFTER: Duration = Duration::from_secs(30);
const INSTALL_LOCK_RETRY_DELAY: Duration = Duration::from_millis(250);

/// Effective `zcashd` source after local-path overrides are applied.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ZcashdBinarySource {
    /// Explicit local executable path.
    Path(PathBuf),
    /// Embedded release download and cache.
    Embedded,
}

/// Returns the current platform target triple used for managed release lookups.
pub fn zcashd_target_triple() -> Option<&'static str> {
    let target = match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") => Some("x86_64-pc-linux-gnu"),
        _ => None,
    }?;

    EMBEDDED_ZCASHD_RELEASE_MANIFEST
        .artifact_for_target(target)
        .map(|_| target)
}

/// Resolves the `zcashd` source selected by configuration.
pub fn effective_zcashd_source(config: &Config) -> Result<ZcashdBinarySource, Report> {
    match config.zcashd_source {
        ConfigZcashdBinarySource::Embedded => Ok(ZcashdBinarySource::Embedded),
        ConfigZcashdBinarySource::Path => config
            .zcashd_path
            .clone()
            .map(ZcashdBinarySource::Path)
            .ok_or_else(|| {
                eyre!(
                    "zcashd_compat.zcashd_source=path requires \
                     zcashd_compat.zcashd_path to be set"
                )
            }),
    }
}

/// Resolves and validates the `zcashd` executable path.
pub fn resolve_zcashd_binary_path(
    config: &Config,
    state_cache_dir: &Path,
) -> Result<PathBuf, Report> {
    match effective_zcashd_source(config)? {
        ZcashdBinarySource::Path(path) => {
            if !is_command_resolvable(&path) {
                return Err(eyre!(
                    "zcashd-compat mode could not resolve zcashd_path={}",
                    path.display()
                ));
            }
            Ok(path)
        }
        ZcashdBinarySource::Embedded => resolve_managed_zcashd_binary(state_cache_dir),
    }
}

/// Resolves the managed zcashd binary from the embedded release manifest,
/// downloading it into the cache if the cached binary is missing or stale.
pub fn resolve_managed_zcashd_binary(state_cache_dir: &Path) -> Result<PathBuf, Report> {
    let target = zcashd_target_triple().ok_or_else(|| {
        eyre!(
            "zcashd-compat managed downloads are unsupported for this platform ({}/{})",
            std::env::consts::OS,
            std::env::consts::ARCH
        )
    })?;

    install_managed_zcashd_binary(&EMBEDDED_ZCASHD_RELEASE_MANIFEST, target, state_cache_dir)
}

/// Returns the managed zcashd binary cache path without creating directories,
/// or `None` when managed downloads are unsupported for this target.
#[allow(dead_code)]
pub(super) fn managed_zcashd_binary_path(state_cache_dir: &Path) -> Option<PathBuf> {
    let target = zcashd_target_triple()?;

    Some(
        managed_zcashd_cache_dir(
            state_cache_dir,
            &EMBEDDED_ZCASHD_RELEASE_MANIFEST.release_tag,
            target,
        )
        .join("zcashd"),
    )
}

/// Returns whether the cached managed zcashd binary's contents match the
/// embedded manifest for this target, or `None` when managed downloads are
/// unsupported for this target.
#[allow(dead_code)]
pub(super) fn cached_managed_zcashd_binary_is_current(
    state_cache_dir: &Path,
) -> Result<Option<bool>, Report> {
    let Some(target) = zcashd_target_triple() else {
        return Ok(None);
    };
    let Some(artifact) = EMBEDDED_ZCASHD_RELEASE_MANIFEST.artifact_for_target(target) else {
        return Ok(None);
    };
    let Some(binary_path) = managed_zcashd_binary_path(state_cache_dir) else {
        return Ok(None);
    };

    cached_binary_matches(&binary_path, &artifact.runtime_binary_sha256).map(Some)
}

/// Installs the `target` executable pinned by `manifest` into the cache, unless
/// the cached executable already has the pinned contents, and returns its path.
///
/// The executable is streamed into a temporary file in the cache directory and
/// hashed as it is written. Only a verified, executable, synced file replaces
/// the cached binary, so a failed install leaves any previous binary in place.
fn install_managed_zcashd_binary(
    manifest: &ZcashdReleaseManifest,
    target: &str,
    state_cache_dir: &Path,
) -> Result<PathBuf, Report> {
    let artifact = manifest.artifact_for_target(target).ok_or_else(|| {
        eyre!(
            "no managed zcashd release is configured for target {target}; \
                 set zcashd_compat.zcashd_path to a local zcashd binary"
        )
    })?;

    let cache_dir = managed_zcashd_cache_dir(state_cache_dir, &manifest.release_tag, target);
    let binary_path = cache_dir.join("zcashd");
    if cached_binary_matches(&binary_path, &artifact.runtime_binary_sha256)? {
        return Ok(binary_path);
    }

    fs::create_dir_all(&cache_dir).map_err(|err| {
        eyre!(
            "failed to create managed zcashd cache directory {}: {err}",
            cache_dir.display()
        )
    })?;

    let _lock = acquire_lock(
        &cache_dir.join(".install.lock"),
        INSTALL_LOCK_WAIT_TIMEOUT,
        LEGACY_INSTALL_LOCK_STALE_AFTER,
    )?;

    // Another process may have installed the binary while this one waited.
    if cached_binary_matches(&binary_path, &artifact.runtime_binary_sha256)? {
        return Ok(binary_path);
    }

    let mut download = NamedTempFile::new_in(&cache_dir).map_err(|err| {
        eyre!(
            "failed to create a temporary file in managed zcashd cache directory {}: {err}",
            cache_dir.display()
        )
    })?;
    download_verified_binary(
        &artifact.runtime_binary_url,
        &artifact.runtime_binary_sha256,
        download.as_file_mut(),
    )?;
    make_executable(download.path())?;
    download.as_file().sync_all().map_err(|err| {
        eyre!(
            "failed to sync downloaded managed zcashd binary {}: {err}",
            download.path().display()
        )
    })?;
    download.persist(&binary_path).map_err(|err| {
        eyre!(
            "failed to persist managed zcashd binary {}: {}",
            binary_path.display(),
            err.error
        )
    })?;

    Ok(binary_path)
}

fn managed_zcashd_cache_dir(state_cache_dir: &Path, release_tag: &str, target: &str) -> PathBuf {
    state_cache_dir
        .join("zcashd-compat")
        .join("bin")
        .join(release_tag)
        .join(target)
}

/// Returns `true` if `path` is a file whose contents have SHA-256 digest
/// `expected_sha256`.
///
/// A missing file or different contents mean the cache is stale. Other
/// filesystem errors are returned, rather than treated as a stale cache.
/// Cache validity depends only on the binary's contents: legacy `zcashd.sha256`
/// sidecars from archive-based installs are ignored.
fn cached_binary_matches(path: &Path, expected_sha256: &str) -> Result<bool, Report> {
    let mut file = match fs::File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(err) => {
            return Err(eyre!(
                "failed to open cached managed zcashd binary {}: {err}",
                path.display()
            ))
        }
    };

    let is_file = file
        .metadata()
        .map_err(|err| {
            eyre!(
                "failed to inspect cached managed zcashd binary {}: {err}",
                path.display()
            )
        })?
        .is_file();
    if !is_file {
        return Ok(false);
    }

    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; MANAGED_BINARY_BUFFER_BYTES];
    loop {
        let n = file.read(&mut buf).map_err(|err| {
            eyre!(
                "failed to read cached managed zcashd binary {}: {err}",
                path.display()
            )
        })?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }

    Ok(hex::encode(hasher.finalize()) == expected_sha256)
}

/// Returns `true` if managed downloads may fetch `url`.
///
/// Production code accepts HTTPS URLs only. Tests may also use localhost HTTP.
fn download_url_is_allowed(url: &Url) -> bool {
    match url.scheme() {
        "https" => true,
        #[cfg(test)]
        "http" => matches!(url.host_str(), Some("127.0.0.1" | "localhost")),
        _ => false,
    }
}

/// Streams the managed binary at `url` into `out`, and checks that its
/// SHA-256 digest is `expected_sha256`.
///
/// The initial URL and every redirect must pass [`download_url_is_allowed`].
fn download_verified_binary(
    url: &str,
    expected_sha256: &str,
    out: &mut fs::File,
) -> Result<(), Report> {
    let parsed =
        Url::parse(url).map_err(|err| eyre!("invalid managed zcashd URL '{url}': {err}"))?;
    if !download_url_is_allowed(&parsed) {
        return Err(eyre!("managed zcashd URL must use https: {url}"));
    }

    // Retain the bundled trust roots and ring provider across reqwest upgrades.
    let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|err| eyre!("failed configuring managed zcashd TLS versions: {err}"))?
    .with_root_certificates(rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    })
    .with_no_client_auth();
    let redirect_policy = Policy::custom(|attempt| {
        // `previous()` includes the initial URL, matching `Policy::limited`.
        if attempt.previous().len() > MANAGED_DOWNLOAD_MAX_REDIRECTS {
            attempt.error(format!(
                "more than {MANAGED_DOWNLOAD_MAX_REDIRECTS} redirects"
            ))
        } else if !download_url_is_allowed(attempt.url()) {
            let error = format!("redirected to non-https URL {}", attempt.url());
            attempt.error(error)
        } else {
            attempt.follow()
        }
    });
    let client = Client::builder()
        .tls_backend_preconfigured(tls)
        .redirect(redirect_policy)
        .timeout(MANAGED_DOWNLOAD_TIMEOUT)
        .build()
        .map_err(|err| eyre!("failed building managed zcashd HTTP client: {err}"))?;
    let mut response = client.get(parsed).send().map_err(|err| {
        eyre!(
            "failed downloading managed zcashd binary from {url}: {}",
            error_chain(&err)
        )
    })?;
    if !response.status().is_success() {
        return Err(eyre!(
            "managed zcashd download from {url} failed with HTTP status {}",
            response.status()
        ));
    }

    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; MANAGED_BINARY_BUFFER_BYTES];
    let mut total_bytes: u64 = 0;
    loop {
        let n = response.read(&mut buf).map_err(|err| {
            eyre!(
                "failed downloading managed zcashd binary from {url}: {}",
                error_chain(&err)
            )
        })?;
        if n == 0 {
            break;
        }

        // `n` is at most the buffer length, which fits in a u64.
        total_bytes = total_bytes.saturating_add(n as u64);
        if total_bytes > MANAGED_BINARY_MAX_BYTES {
            return Err(eyre!(
                "managed zcashd download from {url} exceeded {MANAGED_BINARY_MAX_BYTES} bytes"
            ));
        }

        hasher.update(&buf[..n]);
        out.write_all(&buf[..n])
            .map_err(|err| eyre!("failed writing managed zcashd binary to cache: {err}"))?;
    }

    let actual_sha256 = hex::encode(hasher.finalize());
    if actual_sha256 != expected_sha256 {
        return Err(eyre!(
            "managed zcashd binary hash mismatch for {url}: expected {expected_sha256}, got {actual_sha256}"
        ));
    }

    Ok(())
}

/// Formats `err` and its sources, so that redirect and body errors keep their cause.
fn error_chain(err: &dyn std::error::Error) -> String {
    let mut message = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        message.push_str(": ");
        message.push_str(&cause.to_string());
        source = cause.source();
    }
    message
}

/// Makes `path` executable on Unix targets.
///
/// Non-Unix targets currently no-op because managed release targets are Linux.
fn make_executable(_path: &Path) -> Result<(), Report> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        fs::set_permissions(_path, fs::Permissions::from_mode(0o755)).map_err(|err| {
            eyre!(
                "failed to make managed zcashd binary {} executable: {err}",
                _path.display()
            )
        })?;
    }

    Ok(())
}

struct InstallLock {
    path: PathBuf,
}

impl Drop for InstallLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// Acquires an exclusive lock file, retrying until `timeout`.
///
/// This prevents concurrent zakurad processes from racing downloads and
/// replacing the same cached binary simultaneously.
fn acquire_lock(
    lock_path: &Path,
    wait_timeout: Duration,
    stale_after: Duration,
) -> Result<InstallLock, Report> {
    let started = Instant::now();
    loop {
        match OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(lock_path)
        {
            Ok(mut file) => {
                if let Err(error) = write_install_lock_owner(&mut file, lock_path) {
                    let _ = fs::remove_file(lock_path);
                    return Err(error);
                }

                return Ok(InstallLock {
                    path: lock_path.to_path_buf(),
                });
            }
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                if remove_stale_lock(lock_path, stale_after)? {
                    continue;
                }

                if started.elapsed() >= wait_timeout {
                    return Err(eyre!(
                        "timed out after {} seconds waiting for managed zcashd installation lock: {}",
                        wait_timeout.as_secs(),
                        lock_path.display(),
                    ));
                }

                sleep(INSTALL_LOCK_RETRY_DELAY);
            }
            Err(err) => {
                return Err(eyre!(
                    "failed to create managed zcashd installation lock {}: {err}",
                    lock_path.display()
                ))
            }
        }
    }
}

fn write_install_lock_owner(file: &mut fs::File, lock_path: &Path) -> Result<(), Report> {
    writeln!(file, "pid={}", std::process::id()).map_err(|err| {
        eyre!(
            "failed to write managed zcashd installation lock {}: {err}",
            lock_path.display()
        )
    })?;
    file.sync_all().map_err(|err| {
        eyre!(
            "failed to sync managed zcashd installation lock {}: {err}",
            lock_path.display()
        )
    })
}

fn remove_stale_lock(lock_path: &Path, stale_after: Duration) -> Result<bool, Report> {
    let content = match fs::read_to_string(lock_path) {
        Ok(content) => content,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(true),
        Err(err) => {
            return Err(eyre!(
                "failed to read managed zcashd installation lock {}: {err}",
                lock_path.display()
            ))
        }
    };

    if let Some(pid) = lock_owner_pid(&content) {
        if process_is_running(pid) {
            return Ok(false);
        }
    } else if !lock_file_is_older_than(lock_path, stale_after)? {
        return Ok(false);
    }

    match fs::remove_file(lock_path) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(err) => Err(eyre!(
            "failed to remove stale managed zcashd installation lock {}: {err}",
            lock_path.display()
        )),
    }
}

fn lock_owner_pid(content: &str) -> Option<u32> {
    content.lines().find_map(|line| {
        line.strip_prefix("pid=")
            .and_then(|pid| pid.trim().parse().ok())
    })
}

fn lock_file_is_older_than(lock_path: &Path, age: Duration) -> Result<bool, Report> {
    let metadata = match fs::metadata(lock_path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(true),
        Err(err) => {
            return Err(eyre!(
                "failed to inspect managed zcashd installation lock {}: {err}",
                lock_path.display()
            ))
        }
    };

    let Ok(modified_age) = metadata.modified()?.elapsed() else {
        return Ok(false);
    };

    Ok(modified_age >= age)
}

#[cfg(unix)]
fn process_is_running(pid: u32) -> bool {
    match super::unix::signal_process(pid, 0) {
        Ok(()) => true,
        Err(error) => error.raw_os_error() == Some(libc::EPERM),
    }
}

#[cfg(not(unix))]
fn process_is_running(_pid: u32) -> bool {
    true
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::TcpListener,
        path::{Path, PathBuf},
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        thread,
        time::Duration,
    };

    use sha2::{Digest, Sha256};
    use tempfile::tempdir;

    use super::{
        acquire_lock, cached_binary_matches, effective_zcashd_source,
        install_managed_zcashd_binary, managed_zcashd_cache_dir, zcashd_target_triple, Config,
        ZcashdBinarySource,
    };
    use crate::components::zcashd_compat::{
        ConfigZcashdBinarySource, ZcashdReleaseArtifact, ZcashdReleaseManifest,
        EMBEDDED_ZCASHD_RELEASE_MANIFEST,
    };

    const TARGET: &str = "x86_64-pc-linux-gnu";
    const RELEASE_TAG: &str = "test-release";
    const BINARY: &[u8] = b"#!/bin/sh\necho zcashd-compat-test\n";
    const PREVIOUS_BINARY: &[u8] = b"#!/bin/sh\necho previous-zcashd\n";
    /// An HTTPS URL that fails if a test unexpectedly downloads.
    const UNREACHABLE_URL: &str = "https://zcashd.invalid/zcashd";
    /// SHA-256 of the v1.1.0 runtime archive, which legacy installs wrote to `zcashd.sha256`.
    const LEGACY_ARCHIVE_SHA256: &str =
        "b131e901fb05782e047b9faa593a9da897092915eaa8f7a6cf2438c7634d2f06";

    fn sha256_hex(bytes: &[u8]) -> String {
        hex::encode(Sha256::digest(bytes))
    }

    fn manifest(url: &str, sha256: &str) -> ZcashdReleaseManifest {
        ZcashdReleaseManifest {
            schema_version: 2,
            release_tag: RELEASE_TAG.to_string(),
            artifacts: vec![ZcashdReleaseArtifact {
                target_triple: TARGET.to_string(),
                runtime_binary_url: url.to_string(),
                runtime_binary_sha256: sha256.to_string(),
            }],
        }
    }

    fn cache_dir(state_cache_dir: &Path) -> PathBuf {
        managed_zcashd_cache_dir(state_cache_dir, RELEASE_TAG, TARGET)
    }

    fn write_cached_binary(state_cache_dir: &Path, contents: &[u8]) -> PathBuf {
        let cache_dir = cache_dir(state_cache_dir);
        std::fs::create_dir_all(&cache_dir).expect("cache dir should be created");
        let binary_path = cache_dir.join("zcashd");
        std::fs::write(&binary_path, contents).expect("cached binary should be written");
        binary_path
    }

    /// Lists the cache directory, so tests can check that failures leave no temporary files.
    fn cache_dir_entries(state_cache_dir: &Path) -> Vec<String> {
        let mut entries: Vec<String> = std::fs::read_dir(cache_dir(state_cache_dir))
            .expect("cache dir should be readable")
            .map(|entry| {
                entry
                    .expect("cache dir entry should be readable")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        entries.sort();
        entries
    }

    fn ok_response(body: &[u8]) -> Vec<u8> {
        let mut response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .into_bytes();
        response.extend_from_slice(body);
        response
    }

    fn redirect_response(location: &str) -> Vec<u8> {
        format!(
            "HTTP/1.1 302 Found\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        )
        .into_bytes()
    }

    /// Serves one canned response per connection, in order, on a localhost HTTP
    /// listener. `responses` receives the server's base URL. Returns the base URL
    /// and the number of requests served so far.
    fn serve(responses: impl FnOnce(&str) -> Vec<Vec<u8>>) -> (String, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
        let base_url = format!(
            "http://{}",
            listener
                .local_addr()
                .expect("listener should have local address")
        );
        let responses = responses(&base_url);
        let requests = Arc::new(AtomicUsize::new(0));
        let served = requests.clone();

        thread::spawn(move || {
            for response in responses {
                let (mut stream, _) = listener.accept().expect("client should connect");
                let mut request = Vec::new();
                let mut buf = [0u8; 1024];
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    let n = stream.read(&mut buf).expect("request should be readable");
                    if n == 0 {
                        break;
                    }
                    request.extend_from_slice(&buf[..n]);
                }
                served.fetch_add(1, Ordering::SeqCst);
                let _ = stream.write_all(&response);
            }
        });

        (base_url, requests)
    }

    /// Runs an install that must fail, and checks that it kept `PREVIOUS_BINARY`
    /// and left no temporary files behind. Returns the error message.
    fn assert_failed_install_preserves_previous_binary(url: &str, sha256: &str) -> String {
        let temp = tempdir().expect("tempdir should exist");
        let binary_path = write_cached_binary(temp.path(), PREVIOUS_BINARY);

        let error = install_managed_zcashd_binary(&manifest(url, sha256), TARGET, temp.path())
            .expect_err("install should fail");

        assert_eq!(
            std::fs::read(&binary_path).expect("previous binary should remain"),
            PREVIOUS_BINARY
        );
        assert_eq!(cache_dir_entries(temp.path()), vec!["zcashd".to_string()]);
        format!("{error:#}")
    }

    #[test]
    fn embedded_source_ignores_explicit_zcashd_path() {
        let config = Config {
            zcashd_source: ConfigZcashdBinarySource::Embedded,
            zcashd_path: Some("/usr/local/bin/zcashd".into()),
            ..Default::default()
        };

        assert_eq!(
            effective_zcashd_source(&config).expect("source should resolve"),
            ZcashdBinarySource::Embedded
        );
    }

    #[test]
    fn path_source_uses_explicit_path() {
        let config = Config {
            zcashd_source: ConfigZcashdBinarySource::Path,
            zcashd_path: Some("/usr/local/bin/zcashd".into()),
            ..Default::default()
        };

        assert_eq!(
            effective_zcashd_source(&config).expect("source should resolve"),
            ZcashdBinarySource::Path("/usr/local/bin/zcashd".into())
        );
    }

    #[test]
    fn path_source_requires_explicit_path() {
        let config = Config {
            zcashd_source: ConfigZcashdBinarySource::Path,
            zcashd_path: None,
            ..Default::default()
        };

        let error = effective_zcashd_source(&config).expect_err("path source should fail");
        assert!(error.to_string().contains("zcashd_source=path"));
    }

    #[test]
    fn target_triple_is_configured_or_none() {
        if let Some(target) = zcashd_target_triple() {
            assert_eq!(
                target, "x86_64-pc-linux-gnu",
                "managed zcashd downloads are only published for x86_64"
            );
            assert!(
                EMBEDDED_ZCASHD_RELEASE_MANIFEST
                    .artifact_for_target(target)
                    .is_some(),
                "managed target triple is not configured in embedded manifest: {target}"
            );
        }
    }

    #[test]
    fn installs_downloaded_executable() {
        let temp = tempdir().expect("tempdir should exist");
        let (base_url, requests) = serve(|_| vec![ok_response(BINARY)]);

        let resolved = install_managed_zcashd_binary(
            &manifest(&format!("{base_url}/zcashd"), &sha256_hex(BINARY)),
            TARGET,
            temp.path(),
        )
        .expect("managed install should succeed");

        assert_eq!(resolved, cache_dir(temp.path()).join("zcashd"));
        assert_eq!(
            std::fs::read(&resolved).expect("installed binary should be readable"),
            BINARY
        );
        assert_eq!(requests.load(Ordering::SeqCst), 1);
        // No temporary file, lock or provenance sidecar is left behind.
        assert_eq!(cache_dir_entries(temp.path()), vec!["zcashd".to_string()]);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mode = std::fs::metadata(&resolved)
                .expect("installed binary metadata should be readable")
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o755);
        }
    }

    #[test]
    fn reuses_matching_cached_binary_offline_regardless_of_legacy_sidecar() {
        let binary_sha256 = sha256_hex(BINARY);

        for sidecar in [
            None,
            Some(LEGACY_ARCHIVE_SHA256),
            Some("not-a-sha256"),
            Some(binary_sha256.as_str()),
        ] {
            let temp = tempdir().expect("tempdir should exist");
            let binary_path = write_cached_binary(temp.path(), BINARY);
            let sidecar_path = binary_path.with_file_name("zcashd.sha256");
            if let Some(sidecar) = sidecar {
                std::fs::write(&sidecar_path, format!("{sidecar}\n"))
                    .expect("legacy sidecar should be written");
            }

            let resolved = install_managed_zcashd_binary(
                &manifest(UNREACHABLE_URL, &binary_sha256),
                TARGET,
                temp.path(),
            )
            .unwrap_or_else(|error| {
                panic!("cache with sidecar {sidecar:?} should be reused: {error:#}")
            });

            assert_eq!(resolved, binary_path);
            assert_eq!(
                std::fs::read(&resolved).expect("cached binary should be readable"),
                BINARY
            );
            assert_eq!(
                sidecar_path.exists(),
                sidecar.is_some(),
                "the installer must neither write nor remove sidecars"
            );
        }
    }

    #[test]
    fn replaces_corrupted_cached_binary_even_with_matching_sidecar() {
        let temp = tempdir().expect("tempdir should exist");
        let binary_sha256 = sha256_hex(BINARY);
        let binary_path = write_cached_binary(temp.path(), b"corrupted");
        std::fs::write(
            binary_path.with_file_name("zcashd.sha256"),
            format!("{binary_sha256}\n"),
        )
        .expect("sidecar should be written");
        let (base_url, requests) = serve(|_| vec![ok_response(BINARY)]);

        let resolved = install_managed_zcashd_binary(
            &manifest(&format!("{base_url}/zcashd"), &binary_sha256),
            TARGET,
            temp.path(),
        )
        .expect("corrupted cache should be repaired");

        assert_eq!(
            std::fs::read(resolved).expect("repaired binary should be readable"),
            BINARY
        );
        assert_eq!(requests.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn checksum_mismatch_preserves_previous_binary() {
        let (base_url, _) = serve(|_| vec![ok_response(b"tampered")]);

        let error = assert_failed_install_preserves_previous_binary(
            &format!("{base_url}/zcashd"),
            &sha256_hex(BINARY),
        );

        assert!(error.contains("hash mismatch"), "unexpected error: {error}");
    }

    #[test]
    fn http_error_preserves_previous_binary() {
        let (base_url, _) = serve(|_| {
            vec![b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec()]
        });

        let error = assert_failed_install_preserves_previous_binary(
            &format!("{base_url}/zcashd"),
            &sha256_hex(BINARY),
        );

        assert!(error.contains("500"), "unexpected error: {error}");
    }

    #[test]
    fn interrupted_download_preserves_previous_binary() {
        let (base_url, _) = serve(|_| {
            let mut response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                BINARY.len() + 100
            )
            .into_bytes();
            response.extend_from_slice(BINARY);
            vec![response]
        });

        assert_failed_install_preserves_previous_binary(
            &format!("{base_url}/zcashd"),
            &sha256_hex(BINARY),
        );
    }

    #[test]
    fn rejects_non_https_url() {
        let error = assert_failed_install_preserves_previous_binary(
            "http://example.com/zcashd",
            &sha256_hex(BINARY),
        );

        assert!(error.contains("https"), "unexpected error: {error}");
    }

    #[test]
    fn rejects_redirect_to_non_https_url() {
        let (base_url, requests) = serve(|_| vec![redirect_response("http://example.com/zcashd")]);

        let error = assert_failed_install_preserves_previous_binary(
            &format!("{base_url}/zcashd"),
            &sha256_hex(BINARY),
        );

        assert!(
            error.contains("redirected to non-https URL http://example.com/zcashd"),
            "unexpected error: {error}"
        );
        assert_eq!(requests.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn follows_allowed_redirects_up_to_the_limit() {
        let (base_url, requests) = serve(|base_url| {
            let mut responses: Vec<Vec<u8>> = (1..=5)
                .map(|hop| redirect_response(&format!("{base_url}/hop-{hop}")))
                .collect();
            responses.push(ok_response(BINARY));
            responses
        });
        let temp = tempdir().expect("tempdir should exist");

        let resolved = install_managed_zcashd_binary(
            &manifest(&format!("{base_url}/zcashd"), &sha256_hex(BINARY)),
            TARGET,
            temp.path(),
        )
        .expect("five redirects should be followed");

        assert_eq!(
            std::fs::read(resolved).expect("installed binary should be readable"),
            BINARY
        );
        assert_eq!(requests.load(Ordering::SeqCst), 6);
    }

    #[test]
    fn rejects_too_many_redirects() {
        let (base_url, _) = serve(|base_url| {
            (1..=6)
                .map(|hop| redirect_response(&format!("{base_url}/hop-{hop}")))
                .collect()
        });

        let error = assert_failed_install_preserves_previous_binary(
            &format!("{base_url}/zcashd"),
            &sha256_hex(BINARY),
        );

        assert!(
            error.contains("more than 5 redirects"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn concurrent_installers_download_once() {
        let temp = tempdir().expect("tempdir should exist");
        // A second download would find no listener and fail its installer.
        let (base_url, requests) = serve(|_| vec![ok_response(BINARY)]);
        let manifest = manifest(&format!("{base_url}/zcashd"), &sha256_hex(BINARY));

        let resolved: Vec<PathBuf> = thread::scope(|scope| {
            let installers: Vec<_> = (0..4)
                .map(|_| {
                    scope.spawn(|| install_managed_zcashd_binary(&manifest, TARGET, temp.path()))
                })
                .collect();
            installers
                .into_iter()
                .map(|installer| {
                    installer
                        .join()
                        .expect("installer thread should not panic")
                        .expect("every concurrent installer should succeed")
                })
                .collect()
        });

        for path in resolved {
            assert_eq!(
                std::fs::read(path).expect("installed binary should be readable"),
                BINARY
            );
        }
        assert_eq!(requests.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn rechecks_cache_after_waiting_for_install_lock() {
        let temp = tempdir().expect("tempdir should exist");
        let binary_path = write_cached_binary(temp.path(), b"stale");
        let lock = acquire_lock(
            &cache_dir(temp.path()).join(".install.lock"),
            Duration::ZERO,
            Duration::ZERO,
        )
        .expect("test should hold the install lock");
        let manifest = manifest(UNREACHABLE_URL, &sha256_hex(BINARY));

        let installer = {
            let state_cache_dir = temp.path().to_path_buf();
            thread::spawn(move || {
                install_managed_zcashd_binary(&manifest, TARGET, &state_cache_dir)
            })
        };
        // Let the installer find the stale binary and start waiting for the lock,
        // then install the binary as another process would before releasing it.
        thread::sleep(Duration::from_millis(500));
        std::fs::write(&binary_path, BINARY).expect("binary should be installed");
        drop(lock);

        let resolved = installer
            .join()
            .expect("installer thread should not panic")
            .expect("installer should reuse the binary installed while it waited");
        assert_eq!(resolved, binary_path);
    }

    #[test]
    fn cached_binary_matches_hashes_contents() {
        let temp = tempdir().expect("tempdir should exist");
        let path = temp.path().join("zcashd");
        let expected = sha256_hex(BINARY);

        assert!(!cached_binary_matches(&path, &expected).expect("missing binary is stale"));

        std::fs::write(&path, BINARY).expect("binary should be written");
        assert!(cached_binary_matches(&path, &expected).expect("binary should hash"));
        assert!(!cached_binary_matches(&path, &sha256_hex(b"other")).expect("binary should hash"));

        std::fs::create_dir(temp.path().join("dir")).expect("dir should be created");
        assert!(!cached_binary_matches(&temp.path().join("dir"), &expected)
            .expect("a directory is not a cached binary"));
    }

    #[cfg(unix)]
    #[test]
    fn cached_binary_matches_reports_unreadable_binary() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempdir().expect("tempdir should exist");
        let path = temp.path().join("zcashd");
        std::fs::write(&path, BINARY).expect("binary should be written");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000))
            .expect("permissions should be set");
        if std::fs::read(&path).is_ok() {
            // Running as root, which can read any file.
            return;
        }

        let error = cached_binary_matches(&path, &sha256_hex(BINARY))
            .expect_err("an unreadable binary is an error, not a stale cache");
        let error = error.to_string();
        assert!(
            error.contains(&path.display().to_string()) && error.contains("ermission denied"),
            "unexpected error: {error}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn acquire_lock_replaces_dead_owner_lock() {
        let temp = tempdir().expect("tempdir should exist");
        let lock_path = temp.path().join(".install.lock");
        std::fs::write(&lock_path, "pid=4294967295\n").expect("lock file should write");

        let lock = acquire_lock(&lock_path, Duration::ZERO, Duration::ZERO)
            .expect("dead owner lock should recover");
        let content = std::fs::read_to_string(&lock_path).expect("lock file should be readable");

        assert!(
            content.contains(&format!("pid={}\n", std::process::id())),
            "lock file should contain current process owner: {content}"
        );

        drop(lock);
        assert!(
            !lock_path.exists(),
            "dropping the recovered lock should remove the lock file"
        );
    }

    #[test]
    fn acquire_lock_replaces_legacy_stale_lock() {
        let temp = tempdir().expect("tempdir should exist");
        let lock_path = temp.path().join(".install.lock");
        std::fs::write(&lock_path, "").expect("legacy lock file should write");

        let lock = acquire_lock(&lock_path, Duration::ZERO, Duration::ZERO)
            .expect("legacy stale lock should recover");
        let content = std::fs::read_to_string(&lock_path).expect("lock file should be readable");

        assert!(
            content.contains(&format!("pid={}\n", std::process::id())),
            "lock file should contain current process owner: {content}"
        );

        drop(lock);
        assert!(
            !lock_path.exists(),
            "dropping the recovered lock should remove the lock file"
        );
    }

    #[test]
    fn acquire_lock_keeps_fresh_legacy_lock() {
        let temp = tempdir().expect("tempdir should exist");
        let lock_path = temp.path().join(".install.lock");
        std::fs::write(&lock_path, "").expect("legacy lock file should write");

        let error = match acquire_lock(&lock_path, Duration::ZERO, Duration::from_secs(3600)) {
            Ok(_) => panic!("fresh legacy lock should not be replaced"),
            Err(error) => error,
        };

        assert!(
            error.to_string().contains("timed out"),
            "unexpected error: {error}"
        );
        assert!(lock_path.exists(), "fresh legacy lock should remain");
    }

    #[cfg(unix)]
    #[test]
    fn acquire_lock_keeps_live_owner_lock() {
        let temp = tempdir().expect("tempdir should exist");
        let lock_path = temp.path().join(".install.lock");
        std::fs::write(&lock_path, format!("pid={}\n", std::process::id()))
            .expect("lock file should write");

        let error = match acquire_lock(&lock_path, Duration::ZERO, Duration::ZERO) {
            Ok(_) => panic!("live owner lock should not be replaced"),
            Err(error) => error,
        };

        assert!(
            error.to_string().contains("timed out"),
            "unexpected error: {error}"
        );
        assert!(
            lock_path.exists(),
            "live owner lock should remain for its owner"
        );
    }
}
