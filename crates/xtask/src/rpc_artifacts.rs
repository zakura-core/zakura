use std::{fs, path::Path};

use crate::BoxError;

const INDEXER_PROTO: &str = "crates/zakura-rpc/proto/indexer.proto";
const RPC_CRATE: &str = "crates/zakura-rpc";
const METHODS_SOURCE: &str = "crates/zakura-rpc/src/methods.rs";
const OPENRPC_ARTIFACT: &str = "crates/zakura-rpc/src/methods/rpc_openrpc.rs";

const GENERATED_FILES: [(&str, &str); 3] = [
    (
        "indexer_descriptor.bin",
        "crates/zakura-rpc/proto/__generated__/indexer_descriptor.bin",
    ),
    (
        "zebra.indexer.rpc.rs",
        "crates/zakura-rpc/proto/__generated__/zebra.indexer.rpc.rs",
    ),
    ("rpc_openrpc.rs", OPENRPC_ARTIFACT),
];

pub fn update(repo_root: &Path) -> Result<(), BoxError> {
    compare_generated_artifacts(repo_root, true)
}

pub fn check(repo_root: &Path) -> Result<(), BoxError> {
    compare_generated_artifacts(repo_root, false)
}

fn compare_generated_artifacts(repo_root: &Path, update: bool) -> Result<(), BoxError> {
    let first_dir = tempfile::tempdir()?;
    let second_dir = tempfile::tempdir()?;

    generate(repo_root, first_dir.path())?;
    generate(repo_root, second_dir.path())?;

    for (file_name, checked_in_path) in GENERATED_FILES {
        let first = fs::read(first_dir.path().join(file_name))?;
        let second = fs::read(second_dir.path().join(file_name))?;

        if first != second {
            return Err(
                format!("RPC artifact generation is not deterministic: {file_name}").into(),
            );
        }

        let checked_in_path = repo_root.join(checked_in_path);
        if fs::read(&checked_in_path).ok().as_deref() == Some(first.as_slice()) {
            println!("unchanged {}", checked_in_path.display());
        } else if !update {
            return Err(format!(
                "checked-in RPC artifact is stale: {}; run `cargo xtask generate-rpc-artifacts`",
                checked_in_path.display()
            )
            .into());
        } else {
            fs::write(&checked_in_path, first)?;
            println!("updated {}", checked_in_path.display());
        }
    }

    Ok(())
}

fn generate(repo_root: &Path, output_dir: &Path) -> Result<(), BoxError> {
    let proto_file = repo_root.join(INDEXER_PROTO);
    let rpc_crate = repo_root.join(RPC_CRATE);

    tonic_prost_build::configure()
        .emit_rerun_if_changed(false)
        .out_dir(output_dir)
        .type_attribute(".", "#[derive(serde::Deserialize, serde::Serialize)]")
        .file_descriptor_set_path(output_dir.join("indexer_descriptor.bin"))
        .compile_protos(&[proto_file], &[rpc_crate])?;

    let methods_source = repo_root.join(METHODS_SOURCE);
    let methods_source = methods_source
        .to_str()
        .ok_or("RPC methods source path should be valid UTF-8")?;
    openrpsee::generate_openrpc(methods_source, &["Rpc"], false, output_dir)?;

    verify_generated_files(output_dir)
}

fn verify_generated_files(output_dir: &Path) -> Result<(), BoxError> {
    for (file_name, _) in GENERATED_FILES {
        let path = output_dir.join(file_name);
        if !path.is_file() {
            return Err(format!("generator did not produce {}", path.display()).into());
        }
    }

    Ok(())
}
