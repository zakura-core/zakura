//! Header-chain fuzz artifact minimization and regression rendering.

use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::SystemTime,
};

use sha2::{Digest, Sha256};

use crate::{run_command, BoxError};

const PINNED_NIGHTLY: &str = "nightly-2026-07-15";
const TARGETS: [&str; 4] = [
    "fork_transitions",
    "header_codec",
    "header_pursuit",
    "recovery_rows",
];

pub(super) fn minimize(repo_root: &Path, artifact: &Path) -> Result<(), BoxError> {
    let artifact = artifact.canonicalize()?;
    if !artifact.is_file() {
        return Err(format!("fuzz artifact is not a file: {}", artifact.display()).into());
    }
    let target = infer_target(&artifact).ok_or_else(|| {
        format!(
            "cannot infer a header fuzz target from {}; expected one of {} in the path",
            artifact.display(),
            TARGETS.join(", ")
        )
    })?;
    let fuzz_dir = repo_root.join("fuzz").join("header-chain");
    let artifacts_dir = fuzz_dir.join("artifacts").join(target);
    let before = artifact_files(&artifacts_dir)?;

    run_command(
        Command::new("cargo")
            .arg(format!("+{PINNED_NIGHTLY}"))
            .arg("fuzz")
            .arg("tmin")
            .arg("--locked")
            .arg(target)
            .arg(&artifact)
            .current_dir(&fuzz_dir),
    )?;

    let minimized = find_minimized(&artifacts_dir, &before)?;
    let bytes = fs::read(&minimized)?;
    println!();
    println!("Minimized artifact: {}", minimized.display());
    println!("SHA-256: {}", sha256(&bytes));
    println!("Target: {target}");
    println!();
    println!("{}", render_regression(target, &bytes)?);
    Ok(())
}

fn infer_target(path: &Path) -> Option<&'static str> {
    path.components().find_map(|component| {
        let component = component.as_os_str().to_str()?;
        TARGETS.into_iter().find(|target| *target == component)
    })
}

fn artifact_files(directory: &Path) -> Result<BTreeSet<PathBuf>, BoxError> {
    if !directory.exists() {
        return Ok(BTreeSet::new());
    }
    let mut files = BTreeSet::new();
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        if path.is_file() {
            files.insert(path);
        }
    }
    Ok(files)
}

fn find_minimized(directory: &Path, before: &BTreeSet<PathBuf>) -> Result<PathBuf, BoxError> {
    let after = artifact_files(directory)?;
    let mut new_minimized: Vec<_> = after
        .difference(before)
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("minimized-from-"))
        })
        .cloned()
        .collect();
    new_minimized.sort();
    if let Some(path) = new_minimized.pop() {
        return Ok(path);
    }

    after
        .into_iter()
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("minimized-from-"))
        })
        .max_by_key(|path| {
            path.metadata()
                .and_then(|metadata| metadata.modified())
                .unwrap_or(SystemTime::UNIX_EPOCH)
        })
        .ok_or_else(|| {
            format!(
                "cargo fuzz tmin did not create a minimized artifact under {}",
                directory.display()
            )
            .into()
        })
}

fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn render_regression(target: &str, bytes: &[u8]) -> Result<String, BoxError> {
    let replay = match target {
        "fork_transitions" => "zakura_header_chain::replay_fork_transition_bytes",
        "header_pursuit" => "zakura_network::zakura::replay_header_pursuit_bytes",
        "recovery_rows" => "zakura_state::replay_recovery_rows_bytes",
        "header_codec" => return Ok(render_codec_regression(bytes)),
        _ => return Err(format!("unknown header fuzz target {target}").into()),
    };
    let operations = render_operations(target, bytes);
    Ok(format!(
        "// Target: {target}\n\
         // SHA-256: {}\n\
         // Fixed regtest configuration and manual/fixed clock are constructed by the replay helper.\n\
         {operations}\
         const REGRESSION: &[u8] = &{};\n\
         let first = {replay}(REGRESSION);\n\
         let second = {replay}(REGRESSION);\n\
         assert_eq!(first, second);\n",
        sha256(bytes),
        render_bytes(bytes),
    ))
}

fn render_codec_regression(bytes: &[u8]) -> String {
    format!(
        "// Target: header_codec\n\
         // SHA-256: {}\n\
         const REGRESSION: &[u8] = &{};\n\
         let codec = HeaderSyncCodec::new(\n\
             Network::Mainnet,\n\
             u32::try_from(MAX_HS_MESSAGE_BYTES).expect(\"protocol byte bound fits in u32\"),\n\
             MAX_HS_RANGE,\n\
             1,\n\
         );\n\
         let context = HeaderSyncDecodeContext {{\n\
             max_header_count: MAX_HS_RANGE,\n\
             requested_tree_aux_schema: AuxSchema::V1,\n\
         }};\n\
         if let Ok(message) = codec.decode(REGRESSION, Some(context)) {{\n\
             let canonical = codec.encode(&message).expect(\"decoded messages encode\");\n\
             let decoded = codec.decode(&canonical, Some(context)).expect(\"canonical messages decode\");\n\
             assert_eq!(decoded, message);\n\
         }}\n",
        sha256(bytes),
        render_bytes(bytes),
    )
}

fn render_operations(target: &str, bytes: &[u8]) -> String {
    let width = match target {
        "header_pursuit" | "recovery_rows" => 4,
        "fork_transitions" => 1,
        _ => return String::new(),
    };
    let mut rendered = String::from("// Decoded bounded operation bytes:\n");
    for (index, operation) in bytes.chunks(width).enumerate() {
        rendered.push_str(&format!("// {index:03}: {}\n", render_bytes(operation)));
    }
    rendered
}

fn render_bytes(bytes: &[u8]) -> String {
    let values = bytes
        .iter()
        .map(|byte| format!("0x{byte:02x}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{values}]")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_inference_requires_an_exact_path_component() {
        assert_eq!(
            infer_target(Path::new(
                "fuzz/header-chain/artifacts/header_pursuit/crash-deadbeef"
            )),
            Some("header_pursuit")
        );
        assert_eq!(
            infer_target(Path::new("tmp/header_pursuit-copy/crash")),
            None
        );
    }

    #[test]
    fn regression_rendering_is_stable_and_target_specific() {
        let bytes = [0, 1, 0xfe, 0xff];
        let pursuit =
            render_regression("header_pursuit", &bytes).expect("the known target renders");
        assert!(pursuit.contains("replay_header_pursuit_bytes"));
        assert!(pursuit.contains("// 000: [0x00, 0x01, 0xfe, 0xff]"));
        assert!(pursuit.contains(&sha256(&bytes)));

        let codec = render_regression("header_codec", &bytes).expect("the codec target renders");
        assert!(codec.contains("HeaderSyncCodec::new"));
        assert!(codec.contains("const REGRESSION: &[u8] = &[0x00, 0x01, 0xfe, 0xff];"));
    }
}
