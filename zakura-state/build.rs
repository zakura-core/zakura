//! Selects the reviewed VCT Sprout-history artifact for this build.

use std::{
    env, fs,
    path::{Path, PathBuf},
};

const ARTIFACT_PATH: &str = "src/service/finalized_state/vct/mainnet-sprout-history.bin";

fn main() {
    println!("cargo:rerun-if-changed={ARTIFACT_PATH}");
    println!("cargo:rustc-check-cfg=cfg(zakura_vct_sprout_history_embedded)");

    let manifest_dir =
        PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("Cargo sets CARGO_MANIFEST_DIR"));
    let source = manifest_dir.join(ARTIFACT_PATH);
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("Cargo sets OUT_DIR"));
    let generated = out_dir.join("vct_sprout_history_artifact.rs");

    if source.is_file() {
        embed_artifact(&source, &out_dir, &generated);
    } else {
        require_artifact(&source);
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

fn require_artifact(source: &Path) -> ! {
    // From the repository root:
    // curl -fL https://raw.githubusercontent.com/zakura-core/zakura/main/zakura-state/src/service/finalized_state/vct/mainnet-sprout-history.bin -o zakura-state/src/service/finalized_state/vct/mainnet-sprout-history.bin
    // echo "abf89ec7b9eacbe7a259be891a17059496f2c7c7c2144d3babb34f85f8098832  zakura-state/src/service/finalized_state/vct/mainnet-sprout-history.bin" | shasum -a 256 --check
    // cargo install --locked --path zakurad
    panic!(
        "the Sprout-history artifact is missing at {}. It was not included in the crates.io build due to size limit. Download it from https://github.com/zakura-core/zakura/blob/main/zakura-state/src/service/finalized_state/vct/mainnet-sprout-history.bin. Alternatively, sync from genesis or restore a snapshot from https://zakura.com/snapshots/",
        source.display()
    );
}
