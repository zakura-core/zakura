//! Compile proto files
use std::{
    env, fs,
    path::{Path, PathBuf},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    build_or_copy_proto()?;
    build_rpc_schema()?;

    Ok(())
}

fn build_or_copy_proto() -> Result<(), Box<dyn std::error::Error>> {
    const PROTO_FILE_PATH: &str = "proto/indexer.proto";

    let out_dir = env::var("OUT_DIR").map(PathBuf::from)?;
    let file_names = ["indexer_descriptor.bin", "zebra.indexer.rpc.rs"];

    let is_proto_file_available = Path::new(PROTO_FILE_PATH).exists();
    let is_protoc_available = env::var_os("PROTOC")
        .map(PathBuf::from)
        .or_else(|| which::which("protoc").ok())
        .is_some();

    if is_proto_file_available && is_protoc_available {
        tonic_prost_build::configure()
            .type_attribute(".", "#[derive(serde::Deserialize, serde::Serialize)]")
            .file_descriptor_set_path(out_dir.join("indexer_descriptor.bin"))
            .compile_protos(&[PROTO_FILE_PATH], &[""])?;

        for file_name in file_names {
            let out_path = out_dir.join(file_name);
            let generated_path = format!("proto/__generated__/{file_name}");
            if fs::read(&out_path).ok() != fs::read(&generated_path).ok() {
                fs::copy(out_path, generated_path)?;
            }
        }
    } else {
        for file_name in file_names {
            let out_path = out_dir.join(file_name);
            let generated_path = format!("proto/__generated__/{file_name}");
            if fs::read(&out_path).ok() != Some(fs::read(&generated_path)?) {
                fs::copy(generated_path, out_path)?;
            }
        }
    }

    Ok(())
}

fn build_rpc_schema() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = env::var("OUT_DIR").map(PathBuf::from)?;
    let json_rpc_methods_rs = "src/methods.rs";
    let trait_names = ["Rpc"];
    openrpsee::generate_openrpc(json_rpc_methods_rs, &trait_names, false, &out_dir)?;

    Ok(())
}
