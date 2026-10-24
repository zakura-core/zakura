//! Compile two copies of the real height-index implementation, changing only its map import.
use std::{env, fs, path::PathBuf};

fn main() {
    let source = "../../crates/zakura-state/src/service/non_finalized_state/chain/height_map.rs";
    println!("cargo:rerun-if-changed={source}");
    let original = fs::read_to_string(source).expect("height_map source exists in this checkout");
    // include! does not accept the source's inner module documentation.
    let original = original.replacen("//!", "//", 1)
        .replace("zakura_chain::block::Height", "crate::block::Height");
    let marker = "    collections::BTreeMap,\n";
    assert_eq!(original.matches(marker).count(), 1, "review changed map imports");
    let candidate = original.replace(marker, "");
    let candidate = format!("use bbtree::unsync::BBTreeMap as BTreeMap;\n{candidate}");
    let out = PathBuf::from(env::var_os("OUT_DIR").expect("Cargo provides OUT_DIR"));
    fs::write(out.join("std_height.rs"), original).unwrap();
    fs::write(out.join("bb_height.rs"), candidate).unwrap();
}
