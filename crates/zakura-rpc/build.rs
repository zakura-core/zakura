//! Copy checked-in protobuf artifacts into Cargo's output directory.
use std::{env, fs, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = env::var("OUT_DIR").map(PathBuf::from)?;
    let file_names = ["indexer_descriptor.bin", "zebra.indexer.rpc.rs"];

    for file_name in file_names {
        let out_path = out_dir.join(file_name);
        let generated_path = PathBuf::from("proto/__generated__").join(file_name);
        if fs::read(&out_path).ok() != Some(fs::read(&generated_path)?) {
            fs::copy(generated_path, out_path)?;
        }
    }

    Ok(())
}
