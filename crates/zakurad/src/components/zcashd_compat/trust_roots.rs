//! TLS trust roots for the managed `zcashd` download, read from the system CA bundle.
//!
//! The managed binary is only published for Linux, where every common distribution installs its
//! trusted CA certificates as one PEM bundle. Reading that bundle keeps root updates with the
//! operating system instead of compiling a root list into `zakurad`.
//!
//! Roots from organizations in [`DISTRUSTED_ORGANIZATIONS`] are dropped even when the bundle
//! carries them, because system bundles and `webpki-roots` still ship them.

use std::{
    ffi::OsString,
    path::{Path, PathBuf},
};

use eyre::{eyre, Report};
use tokio_rustls::rustls::{
    pki_types::{pem::PemObject, CertificateDer},
    RootCertStore,
};

/// The environment variable that names a PEM bundle to use instead of the system bundle.
///
/// OpenSSL and most TLS clients honor the same variable.
const CERT_FILE_ENV: &str = "SSL_CERT_FILE";

/// Where Linux distributions install their PEM CA bundle, most common first.
const SYSTEM_BUNDLE_PATHS: &[&str] = &[
    // Debian, Ubuntu, Arch, Gentoo, Alpine (`ca-certificates` package)
    "/etc/ssl/certs/ca-certificates.crt",
    // Fedora, RHEL 7+, CentOS
    "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
    // Older RHEL and CentOS
    "/etc/pki/tls/certs/ca-bundle.crt",
    // openSUSE
    "/etc/ssl/ca-bundle.pem",
    // Alpine, and the BSD and macOS layout
    "/etc/ssl/cert.pem",
];

/// Organizations whose roots are never trusted, matched ASCII case-insensitively against each
/// root's subject.
///
/// eMudhra operates the emSign roots. Its roots are excluded wherever they come from, so a
/// stale system bundle cannot reintroduce them.
const DISTRUSTED_ORGANIZATIONS: &[&[u8]] = &[b"eMudhra"];

/// Returns the trusted roots from `SSL_CERT_FILE` if it is set, or else from the first system CA
/// bundle that exists.
///
/// Fails if no bundle is found, or the bundle holds no usable certificate, so a host without CA
/// certificates gets an error that says how to fix it rather than a TLS handshake failure.
///
/// Roots from [`DISTRUSTED_ORGANIZATIONS`] are left out.
pub(super) fn system_root_certificates() -> Result<RootCertStore, Report> {
    let candidates: Vec<&Path> = SYSTEM_BUNDLE_PATHS.iter().map(Path::new).collect();
    root_certificates(std::env::var_os(CERT_FILE_ENV), &candidates)
}

/// Returns the trusted roots from `override_path` if it is set, or else from the first of
/// `candidates` that exists.
fn root_certificates(
    override_path: Option<OsString>,
    candidates: &[&Path],
) -> Result<RootCertStore, Report> {
    let bundle = match override_path {
        Some(path) => PathBuf::from(path),
        None => candidates
            .iter()
            .find(|path| path.is_file())
            .map(|path| path.to_path_buf())
            .ok_or_else(|| {
                eyre!(
                    "no system CA certificate bundle found at {candidates:?}: install the \
                     `ca-certificates` package, or set {CERT_FILE_ENV} to a PEM bundle"
                )
            })?,
    };

    let certificates = CertificateDer::pem_file_iter(&bundle)
        .map_err(|err| eyre!("failed reading CA certificate bundle {bundle:?}: {err}"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| eyre!("failed parsing CA certificate bundle {bundle:?}: {err}"))?;

    let mut roots = RootCertStore::empty();
    // Bundles can carry certificates rustls cannot use as anchors; those are skipped, as other
    // rustls clients do.
    roots.add_parsable_certificates(certificates);
    roots
        .roots
        .retain(|anchor| !is_distrusted_subject(anchor.subject.as_ref()));
    if roots.is_empty() {
        return Err(eyre!(
            "CA certificate bundle {bundle:?} holds no usable, trusted certificate"
        ));
    }

    Ok(roots)
}

/// Returns `true` if the DER-encoded `subject` names an organization in
/// [`DISTRUSTED_ORGANIZATIONS`].
fn is_distrusted_subject(subject: &[u8]) -> bool {
    DISTRUSTED_ORGANIZATIONS.iter().any(|organization| {
        subject
            .windows(organization.len())
            .any(|window| window.eq_ignore_ascii_case(organization))
    })
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use super::root_certificates;

    /// The ISRG Root X1 certificate, in PEM form.
    const ISRG_ROOT_X1: &str = include_str!("testdata/isrg_root_x1.pem");

    /// The eMudhra `emSign Root CA - C1` and `emSign Root CA - G1` certificates, in PEM form.
    ///
    /// Their subjects name two eMudhra organizations: `eMudhra Inc` and
    /// `eMudhra Technologies Limited`.
    const EMSIGN_ROOTS: &str = include_str!("testdata/emsign_roots.pem");

    #[test]
    fn emudhra_roots_are_dropped() {
        let dir = tempfile::tempdir().expect("a temporary directory can be created");
        let bundle = dir.path().join("bundle.pem");
        fs::write(&bundle, format!("{EMSIGN_ROOTS}{ISRG_ROOT_X1}"))
            .expect("the temporary directory is writable");

        let roots = root_certificates(Some(bundle.into()), &[])
            .expect("the bundle also holds a trusted root");

        assert_eq!(roots.len(), 1, "only ISRG Root X1 remains");
        assert!(roots
            .roots
            .iter()
            .all(|anchor| !String::from_utf8_lossy(anchor.subject.as_ref()).contains("eMudhra")));
    }

    #[test]
    fn bundle_with_only_emudhra_roots_is_an_error() {
        let dir = tempfile::tempdir().expect("a temporary directory can be created");
        let bundle = dir.path().join("bundle.pem");
        fs::write(&bundle, EMSIGN_ROOTS).expect("the temporary directory is writable");

        let err = root_certificates(Some(bundle.into()), &[])
            .expect_err("every root in the bundle is distrusted");

        assert!(
            err.to_string().contains("no usable, trusted certificate"),
            "{err}"
        );
    }

    /// The host's real bundle, when it has one, loads with no eMudhra root left in it.
    #[test]
    fn system_bundle_has_no_emudhra_roots() {
        let Ok(roots) = super::system_root_certificates() else {
            return;
        };

        assert!(roots
            .roots
            .iter()
            .all(|anchor| !super::is_distrusted_subject(anchor.subject.as_ref())));
    }

    #[test]
    fn override_path_wins_over_candidates() {
        let dir = tempfile::tempdir().expect("a temporary directory can be created");
        let bundle = dir.path().join("bundle.pem");
        fs::write(&bundle, format!("{ISRG_ROOT_X1}{ISRG_ROOT_X1}"))
            .expect("the temporary directory is writable");
        let missing = dir.path().join("missing.pem");

        let roots = root_certificates(Some(bundle.into()), &[Path::new(&missing)])
            .expect("the override bundle holds a valid root");

        assert_eq!(roots.len(), 2);
    }

    #[test]
    fn first_existing_candidate_is_used() {
        let dir = tempfile::tempdir().expect("a temporary directory can be created");
        let missing = dir.path().join("missing.pem");
        let bundle = dir.path().join("bundle.pem");
        fs::write(&bundle, ISRG_ROOT_X1).expect("the temporary directory is writable");

        let roots = root_certificates(None, &[missing.as_path(), bundle.as_path()])
            .expect("the second candidate holds a valid root");

        assert_eq!(roots.len(), 1);
    }

    #[test]
    fn missing_bundle_is_an_error() {
        let dir = tempfile::tempdir().expect("a temporary directory can be created");
        let missing = dir.path().join("missing.pem");

        let err = root_certificates(None, &[missing.as_path()]).expect_err("no candidate exists");

        assert!(
            err.to_string().contains("SSL_CERT_FILE"),
            "the error says how to fix it: {err}"
        );
    }

    #[test]
    fn bundle_without_certificates_is_an_error() {
        let dir = tempfile::tempdir().expect("a temporary directory can be created");
        let bundle = dir.path().join("bundle.pem");
        fs::write(&bundle, "not a certificate\n").expect("the temporary directory is writable");

        let err = root_certificates(Some(bundle.into()), &[])
            .expect_err("the bundle holds no certificate");

        assert!(
            err.to_string().contains("no usable, trusted certificate"),
            "{err}"
        );
    }
}
