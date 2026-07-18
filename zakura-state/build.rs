//! Selects the reviewed VCT Sprout-history artifact for this build.

use std::{
    env, fs,
    path::{Path, PathBuf},
};

const ARTIFACT_PATH: &str = "src/service/finalized_state/vct/mainnet-sprout-history.bin";
const REQUIRE_ARTIFACT_ENV: &str = "ZAKURA_REQUIRE_VCT_SPROUT_HISTORY";

fn main() {
    println!("cargo:rerun-if-changed={ARTIFACT_PATH}");
    println!("cargo:rerun-if-env-changed={REQUIRE_ARTIFACT_ENV}");
    println!("cargo:rustc-check-cfg=cfg(zakura_vct_sprout_history_embedded)");

    let manifest_dir =
        PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("Cargo sets CARGO_MANIFEST_DIR"));
    let source = manifest_dir.join(ARTIFACT_PATH);
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("Cargo sets OUT_DIR"));
    let generated = out_dir.join("vct_sprout_history_artifact.rs");

    if source.is_file() {
        embed_artifact(&source, &out_dir, &generated);
    } else {
        require_artifact_if_configured(&source);
        fs::write(generated, "const MAINNET_ARTIFACT: Option<&[u8]> = None;\n")
            .expect("generated artifact source is writable");
    }
}

fn embed_artifact(source: &Path, out_dir: &Path, generated: &Path) {
    let destination = out_dir.join("mainnet-sprout-history.bin");
    fs::copy(source, destination).expect("reviewed Sprout-history artifact is readable");
    fs::write(
        generated,
        "const MAINNET_ARTIFACT: Option<&[u8]> = Some(include_bytes!(concat!(env!(\"OUT_DIR\"), \"/mainnet-sprout-history.bin\")));\n",
    )
    .expect("generated artifact source is writable");
    println!("cargo:rustc-cfg=zakura_vct_sprout_history_embedded");
}

fn require_artifact_if_configured(source: &Path) {
    if env::var_os(REQUIRE_ARTIFACT_ENV).is_some_and(|value| value == "1") {
        panic!(
            "{REQUIRE_ARTIFACT_ENV}=1 but the reviewed Sprout-history artifact is missing at {}",
            source.display()
        );
    }
}
