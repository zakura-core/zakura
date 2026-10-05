//! Embedded zcashd compatibility release manifest.
//!
//! [`zakurad/zcashd-compat-manifest.json`](../../../zcashd-compat-manifest.json) is the
//! single source of truth for the zcashd compat pin: CI and Docker builds read it via
//! `scripts/resolve-zcashd-compat-manifest.sh`, and zakurad embeds it at compile time
//! and parses it here.

use std::{collections::HashSet, sync::LazyLock};

use color_eyre::eyre::{eyre, Report};
use serde::Deserialize;

/// Embedded manifest schema version.
///
/// Version 2 pins a standalone `zcashd` executable per target. Version 1 pinned
/// a runtime archive and an archive member path, and is rejected.
pub const EMBEDDED_MANIFEST_SCHEMA_VERSION: u32 = 2;

/// Embedded zcashd compatibility release manifest.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ZcashdReleaseManifest {
    /// Manifest schema version.
    pub schema_version: u32,
    /// Release tag from which artifacts were published.
    pub release_tag: String,
    /// Release artifacts by target triple.
    pub artifacts: Vec<ZcashdReleaseArtifact>,
}

/// One released zcashd compatibility artifact.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ZcashdReleaseArtifact {
    /// Rust-style target triple.
    pub target_triple: String,
    /// Fully-qualified HTTPS URL of the standalone `zcashd` executable.
    pub runtime_binary_url: String,
    /// Lowercase SHA-256 hex digest of the standalone `zcashd` executable.
    pub runtime_binary_sha256: String,
}

/// Embedded manifest used by managed zcashd downloads.
pub static EMBEDDED_ZCASHD_RELEASE_MANIFEST: LazyLock<ZcashdReleaseManifest> =
    LazyLock::new(|| {
        parse_manifest(include_str!("../../../zcashd-compat-manifest.json"))
            .expect("committed zcashd-compat-manifest.json is validated by unit tests and CI")
    });

impl ZcashdReleaseManifest {
    /// Returns the configured artifact for `target_triple`, if any.
    pub fn artifact_for_target(&self, target_triple: &str) -> Option<&ZcashdReleaseArtifact> {
        self.artifacts
            .iter()
            .find(|artifact| artifact.target_triple == target_triple)
    }
}

/// Parses `json` and checks the schema version, unique targets, HTTPS URLs and
/// SHA-256 digest shape.
fn parse_manifest(json: &str) -> Result<ZcashdReleaseManifest, Report> {
    let manifest: ZcashdReleaseManifest = serde_json::from_str(json)
        .map_err(|err| eyre!("invalid zcashd-compat manifest: {err}"))?;

    if manifest.schema_version != EMBEDDED_MANIFEST_SCHEMA_VERSION {
        return Err(eyre!(
            "unsupported zcashd-compat manifest schema version {}, expected {}",
            manifest.schema_version,
            EMBEDDED_MANIFEST_SCHEMA_VERSION
        ));
    }

    if manifest.release_tag.is_empty() {
        return Err(eyre!("zcashd-compat manifest release_tag is empty"));
    }

    let mut targets = HashSet::new();
    for artifact in &manifest.artifacts {
        let target = &artifact.target_triple;
        if !targets.insert(target.as_str()) {
            return Err(eyre!("duplicate zcashd-compat manifest target {target}"));
        }

        if !artifact.runtime_binary_url.starts_with("https://") {
            return Err(eyre!(
                "zcashd-compat manifest URL for target {target} must use https: {}",
                artifact.runtime_binary_url
            ));
        }

        // The installer compares this against a lowercase hex digest.
        let sha256 = &artifact.runtime_binary_sha256;
        if sha256.len() != 64
            || !sha256
                .chars()
                .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c))
        {
            return Err(eyre!(
                "zcashd-compat manifest SHA-256 for target {target} must be 64 lowercase hex characters: {sha256}"
            ));
        }
    }

    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use super::{parse_manifest, EMBEDDED_MANIFEST_SCHEMA_VERSION, EMBEDDED_ZCASHD_RELEASE_MANIFEST};

    const SHA256: &str = "fdfc488bd1a6df725b2997e455166e640d8c5ffc985a177f1dabe72020d5b2c3";

    fn manifest_json(schema_version: u32, artifacts: &str) -> String {
        format!(
            r#"{{"schema_version": {schema_version}, "release_tag": "v1.1.0", "artifacts": [{artifacts}]}}"#
        )
    }

    fn artifact_json(target: &str, url: &str, sha256: &str) -> String {
        format!(
            r#"{{"target_triple": "{target}", "runtime_binary_url": "{url}", "runtime_binary_sha256": "{sha256}"}}"#
        )
    }

    #[test]
    fn embedded_manifest_pins_the_linux_x86_64_standalone_executable() {
        assert_eq!(
            EMBEDDED_ZCASHD_RELEASE_MANIFEST.schema_version,
            EMBEDDED_MANIFEST_SCHEMA_VERSION
        );
        assert_eq!(EMBEDDED_ZCASHD_RELEASE_MANIFEST.artifacts.len(), 1);

        let artifact = EMBEDDED_ZCASHD_RELEASE_MANIFEST
            .artifact_for_target("x86_64-pc-linux-gnu")
            .expect("the embedded manifest pins the only managed target");
        assert_eq!(
            artifact.runtime_binary_url,
            format!(
                "https://github.com/valargroup/zcashd/releases/download/{tag}/zcashd-zebra-compat-{tag}-linux-x86_64",
                tag = EMBEDDED_ZCASHD_RELEASE_MANIFEST.release_tag
            )
        );
    }

    #[test]
    fn accepts_a_well_formed_manifest() {
        let json = manifest_json(
            2,
            &artifact_json("x86_64-pc-linux-gnu", "https://example.com/zcashd", SHA256),
        );

        let manifest = parse_manifest(&json).expect("well-formed manifest should parse");
        assert_eq!(
            manifest
                .artifact_for_target("x86_64-pc-linux-gnu")
                .map(|artifact| artifact.runtime_binary_sha256.as_str()),
            Some(SHA256)
        );
    }

    #[test]
    fn rejects_schema_version_1_archive_manifest() {
        let json = r#"{
            "schema_version": 1,
            "release_tag": "v1.1.0",
            "artifacts": [{
                "target_triple": "x86_64-pc-linux-gnu",
                "runtime_archive_url": "https://example.com/zcashd.tar.gz",
                "runtime_archive_sha256": "b131e901fb05782e047b9faa593a9da897092915eaa8f7a6cf2438c7634d2f06",
                "runtime_archive_member_binary_path": "./bin/zcashd"
            }]
        }"#;

        let error = parse_manifest(json).expect_err("schema 1 manifest should be rejected");
        assert!(
            error.to_string().contains("runtime_archive_url"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rejects_archive_fields_with_schema_version_2() {
        let artifact = format!(
            r#"{{"target_triple": "x86_64-pc-linux-gnu", "runtime_binary_url": "https://example.com/zcashd", "runtime_binary_sha256": "{SHA256}", "runtime_archive_member_binary_path": "./bin/zcashd"}}"#
        );

        let error = parse_manifest(&manifest_json(2, &artifact))
            .expect_err("archive fields should be rejected");
        assert!(
            error.to_string().contains("runtime_archive_member_binary_path"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rejects_other_schema_versions() {
        for version in [1, 3] {
            let json = manifest_json(
                version,
                &artifact_json("x86_64-pc-linux-gnu", "https://example.com/zcashd", SHA256),
            );

            let error = parse_manifest(&json).expect_err("schema version should be rejected");
            assert!(
                error.to_string().contains("schema version"),
                "unexpected error: {error}"
            );
        }
    }

    #[test]
    fn rejects_duplicate_targets() {
        let artifact = artifact_json("x86_64-pc-linux-gnu", "https://example.com/zcashd", SHA256);

        let error = parse_manifest(&manifest_json(2, &format!("{artifact}, {artifact}")))
            .expect_err("duplicate targets should be rejected");
        assert!(
            error.to_string().contains("duplicate"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rejects_non_https_urls() {
        let json = manifest_json(
            2,
            &artifact_json("x86_64-pc-linux-gnu", "http://example.com/zcashd", SHA256),
        );

        let error = parse_manifest(&json).expect_err("http URL should be rejected");
        assert!(
            error.to_string().contains("https"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rejects_malformed_sha256() {
        for sha256 in [
            SHA256[..63].to_string(),
            format!("{SHA256}0"),
            SHA256.to_uppercase(),
            format!("{}g", &SHA256[..63]),
        ] {
            let json = manifest_json(
                2,
                &artifact_json("x86_64-pc-linux-gnu", "https://example.com/zcashd", &sha256),
            );

            let error = parse_manifest(&json).expect_err("malformed SHA-256 should be rejected");
            assert!(
                error.to_string().contains("SHA-256"),
                "unexpected error for {sha256}: {error}"
            );
        }
    }
}
