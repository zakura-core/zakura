//! Writes the BPv1 targets' in-code seed inputs under `corpus/<target>/`, or
//! under the directory given as the only argument.

use std::{env, fs, io, path::PathBuf};

fn main() -> io::Result<()> {
    let root = env::args_os()
        .nth(1)
        .map_or_else(|| PathBuf::from("corpus"), PathBuf::from);
    for seed in zakura_network::zakura::bpv1::fuzz::seed_inputs() {
        let dir = root.join(seed.target);
        fs::create_dir_all(&dir)?;
        fs::write(dir.join(seed.name), &seed.bytes)?;
    }
    Ok(())
}
