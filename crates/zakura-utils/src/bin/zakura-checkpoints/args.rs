//! zakura-checkpoints arguments
//!
//! For usage please refer to the program help: `zakura-checkpoints --help`

use std::{
    env, fs,
    net::SocketAddr,
    path::{Component, Path, PathBuf},
    str::FromStr,
};

use clap::Parser;
use thiserror::Error;

use zakura_chain::block::Height;

/// The backend type the zakura-checkpoints utility will use to get data from.
///
/// This changes which RPCs the tool calls, and which fields it expects them to have.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Backend {
    /// Expect a Zebra-style backend with limited RPCs and fields.
    ///
    /// Calls these specific RPCs:
    /// - `getblock` with `verbose=0`, manually calculating `hash`, `height`, and `size`
    /// - `getblockchaininfo`, expecting a `blocks` field
    ///
    /// Supports both `zakurad` and `zcashd` nodes.
    Zakurad,

    /// Expect a `zcashd`-style backend with all available RPCs and fields.
    ///
    /// Calls these specific RPCs:
    /// - `getblock` with `verbose=1`, expecting `hash`, `height`, and `size` fields
    /// - `getblockchaininfo`, expecting a `blocks` field
    ///
    /// Currently only supported with `zcashd`.
    Zcashd,
}

impl FromStr for Backend {
    type Err = InvalidBackendError;

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        match string.to_lowercase().as_str() {
            "zakurad" => Ok(Backend::Zakurad),
            "zcashd" => Ok(Backend::Zcashd),
            _ => Err(InvalidBackendError(string.to_owned())),
        }
    }
}

/// An error indicating that the supplied string is not a valid [`Backend`] name.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error("Invalid backend: {0}")]
pub struct InvalidBackendError(String);

/// The transport used by the zakura-checkpoints utility to connect to the [`Backend`].
///
/// This changes how the tool makes RPC requests.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Transport {
    /// Launch the `zcash-cli` command in a subprocess, and read its output.
    ///
    /// The RPC name and parameters are sent as command-line arguments.
    /// Responses are read from the command's standard output.
    ///
    /// Requires the `zcash-cli` command, which is part of `zcashd`'s tools.
    /// Supports both `zakurad` and `zcashd` nodes.
    Cli,

    /// Connect directly to the node using TCP, and use the JSON-RPC protocol.
    ///
    /// Uses JSON-RPC over HTTP for sending the RPC name and parameters, and
    /// receiving responses.
    ///
    /// Always supports the `zakurad` node.
    /// Only supports `zcashd` nodes using a JSON-RPC TCP port with no authentication.
    Direct,
}

impl FromStr for Transport {
    type Err = InvalidTransportError;

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        match string.to_lowercase().as_str() {
            "cli" | "zcash-cli" | "zcashcli" | "zcli" | "z-cli" => Ok(Transport::Cli),
            "direct" => Ok(Transport::Direct),
            _ => Err(InvalidTransportError(string.to_owned())),
        }
    }
}

/// An error indicating that the supplied string is not a valid [`Transport`] name.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error("Invalid transport: {0}")]
pub struct InvalidTransportError(String);

/// zakura-checkpoints arguments
#[derive(Clone, Debug, Eq, PartialEq, Parser)]
#[command(version)]
pub struct Args {
    /// Backend type: the node we're connecting to.
    #[arg(default_value = "zakurad", short, long)]
    pub backend: Backend,

    /// Transport type: the way we connect.
    #[arg(default_value = "cli", short, long)]
    pub transport: Transport,

    /// Path or name of zcash-cli command.
    /// Only used if the transport is [`Cli`](Transport::Cli).
    #[arg(default_value = "zcash-cli", short, long)]
    pub cli: String,

    /// Address and port for RPC connections.
    /// Used for all transports.
    #[arg(short, long)]
    pub addr: Option<SocketAddr>,

    /// Start looking for checkpoints after this height.
    /// If there is no last checkpoint, we start looking at the Genesis block (height 0).
    #[arg(short, long)]
    pub last_checkpoint: Option<Height>,

    /// Offline mode: read a quiesced Zakura state cache directory instead of
    /// querying a node over RPC. Mainnet only.
    ///
    /// See the "Mainnet release-state" section of
    /// `docs/design/verified-commitment-trees.md` for the pipeline this feeds.
    #[arg(long)]
    pub state_cache_dir: Option<PathBuf>,

    /// Offline mode: write the VCT final-frontier artifact for the last
    /// emitted checkpoint height to this path.
    ///
    /// Requires `--state-cache-dir` and `--mainnet-subtree-output`.
    #[arg(long)]
    pub mainnet_frontier_output: Option<PathBuf>,

    /// Offline mode: write the completed-subtree artifact for the last emitted
    /// checkpoint height to this path.
    ///
    /// Requires `--state-cache-dir` and the other artifact output flags.
    #[arg(long)]
    pub mainnet_subtree_output: Option<PathBuf>,

    /// Offline mode: write the historical note commitment frontier grid covering
    /// everything below the last emitted checkpoint to this path.
    ///
    /// Serving nodes anchor on this grid to answer `z_gettreestate` across the
    /// heights a fast sync skipped. Requires `--state-cache-dir` and the other
    /// artifact output flags.
    #[arg(long)]
    pub mainnet_frontier_grid_output: Option<PathBuf>,

    /// Offline mode: resume the frontier grid from a previously published one.
    ///
    /// Carries that grid's entries forward instead of recomputing them, so a run scans only the
    /// blocks above its last entry rather than the whole chain. Every carried entry is re-checked
    /// against this database's authenticated roots first, so the input is never trusted. Omit it
    /// to build the grid from genesis, which is what the first run has to do.
    ///
    /// Requires `--mainnet-frontier-grid-output`.
    #[arg(long)]
    pub mainnet_frontier_grid_input: Option<PathBuf>,

    /// Offline mode: generate only the frontier grid, for an already-committed checkpoint.
    ///
    /// The normal export selects a new checkpoint above the embedded list and pins every
    /// artifact to it. This backfills a grid for a checkpoint the repository already ships,
    /// so the artifact can be committed without advancing `main-checkpoints.txt`. The height
    /// must be one of the embedded checkpoints.
    ///
    /// Requires `--state-cache-dir` and `--mainnet-frontier-grid-output`, and excludes the
    /// other artifact outputs, which would be generated for a newly selected checkpoint.
    #[arg(long)]
    pub mainnet_frontier_grid_checkpoint: Option<Height>,

    /// Offline mode: per-entry replay budget for the frontier grid, in milliseconds.
    ///
    /// Defaults to 2000 ms. The grid is spaced by estimated replay cost rather than
    /// evenly, so it bounds the slowest cold request a consumer can make rather than
    /// the average one. The estimate is a function of the chain, not of wall-clock
    /// timing, so generator runs stay byte-identical.
    #[arg(long, conflicts_with = "frontier_grid_spacing")]
    pub frontier_grid_target_cost_ms: Option<u64>,

    /// Offline mode: uniform height spacing for the frontier grid, in blocks.
    ///
    /// Not recommended. Replay cost varies by more than an order of magnitude across
    /// Mainnet, so a uniform grid cannot bound the worst-case cold request at a sane
    /// size. Prefer the default cost-weighted grid.
    #[arg(long, conflicts_with = "frontier_grid_target_cost_ms")]
    pub frontier_grid_spacing: Option<u32>,

    /// Offline mode: print the embedded Mainnet checkpoint list before the
    /// newly generated checkpoints, so stdout is a complete replacement
    /// `main-checkpoints.txt`.
    ///
    /// Requires `--state-cache-dir` and every artifact output flag; incompatible
    /// with `--last-checkpoint`.
    #[arg(long)]
    pub full_list: bool,

    /// Passthrough args for `zcash-cli`.
    /// Only used if the transport is [`Cli`](Transport::Cli).
    #[arg(last = true)]
    pub zcli_args: Vec<String>,
}

impl Args {
    /// Check that offline-mode flags are used coherently.
    ///
    /// Offline and RPC modes are mutually exclusive, and the full-list output
    /// only makes sense when extending the embedded checkpoint list.
    pub fn validate_mode(&self) -> Result<(), String> {
        if self.mainnet_frontier_grid_output.is_none()
            && (self.frontier_grid_spacing.is_some() || self.frontier_grid_target_cost_ms.is_some())
        {
            return Err(
                "--frontier-grid-spacing and --frontier-grid-target-cost-ms tune \
                 --mainnet-frontier-grid-output: add it, or remove them"
                    .to_string(),
            );
        }
        if self.mainnet_frontier_grid_output.is_none() && self.mainnet_frontier_grid_input.is_some()
        {
            return Err(
                "--mainnet-frontier-grid-input resumes a grid that has nowhere to go: add \
                 --mainnet-frontier-grid-output"
                    .to_string(),
            );
        }

        if self.state_cache_dir.is_some() && self.mainnet_frontier_grid_checkpoint.is_some() {
            return self.validate_grid_backfill_mode();
        }

        if self.state_cache_dir.is_some() {
            if self.addr.is_some() {
                return Err(
                    "--state-cache-dir reads the database directly: remove --addr".to_string(),
                );
            }
            if !self.zcli_args.is_empty() {
                return Err(
                    "--state-cache-dir reads the database directly: remove zcash-cli passthrough \
                     arguments"
                        .to_string(),
                );
            }
            if self.full_list && self.last_checkpoint.is_some() {
                return Err(
                    "--full-list extends the embedded checkpoint list: remove --last-checkpoint"
                        .to_string(),
                );
            }
            let supplied = self
                .artifact_outputs()
                .iter()
                .filter(|(_, path)| path.is_some())
                .count();
            if supplied != 0 && supplied != self.artifact_outputs().len() {
                return Err(
                    "release-state frontiers, subtree roots, and the frontier grid are one \
                     artifact set: provide --mainnet-frontier-output, --mainnet-subtree-output, \
                     and --mainnet-frontier-grid-output together"
                        .to_string(),
                );
            }
            self.reject_aliased_artifact_outputs()?;
            if self.full_list && supplied == 0 {
                return Err(
                    "--full-list emits a replacement main-checkpoints.txt, which must ship with \
                     its coupled release state: add every artifact output flag"
                        .to_string(),
                );
            }
        } else {
            for (flag, path) in self.artifact_outputs() {
                if path.is_some() {
                    return Err(format!("{flag} requires --state-cache-dir"));
                }
            }
            if self.full_list {
                return Err("--full-list requires --state-cache-dir".to_string());
            }
            if self.mainnet_frontier_grid_checkpoint.is_some() {
                return Err(
                    "--mainnet-frontier-grid-checkpoint requires --state-cache-dir".to_string(),
                );
            }
        }

        Ok(())
    }

    /// Check the flags of a grid-only backfill run.
    ///
    /// This mode exists to produce a grid for a checkpoint the repository already ships, so it
    /// emits no checkpoints and writes no other artifact. Pairing it with the flags that do
    /// either would silently pin those outputs to a different, newly selected checkpoint.
    fn validate_grid_backfill_mode(&self) -> Result<(), String> {
        if self.mainnet_frontier_grid_output.is_none() {
            return Err(
                "--mainnet-frontier-grid-checkpoint needs somewhere to write: add \
                 --mainnet-frontier-grid-output"
                    .to_string(),
            );
        }
        if self.mainnet_frontier_output.is_some() || self.mainnet_subtree_output.is_some() {
            return Err(
                "--mainnet-frontier-grid-checkpoint backfills the grid alone: remove \
                 --mainnet-frontier-output and --mainnet-subtree-output, which are only \
                 produced for a newly selected checkpoint"
                    .to_string(),
            );
        }
        if self.full_list {
            return Err(
                "--mainnet-frontier-grid-checkpoint emits no checkpoints: remove --full-list"
                    .to_string(),
            );
        }
        if self.last_checkpoint.is_some() {
            return Err(
                "--mainnet-frontier-grid-checkpoint selects no checkpoints: remove \
                 --last-checkpoint"
                    .to_string(),
            );
        }
        if self.addr.is_some() {
            return Err("--state-cache-dir reads the database directly: remove --addr".to_string());
        }
        if !self.zcli_args.is_empty() {
            return Err(
                "--state-cache-dir reads the database directly: remove zcash-cli passthrough \
                 arguments"
                    .to_string(),
            );
        }

        Ok(())
    }

    /// The artifact output flags and their paths, in the order errors report them.
    fn artifact_outputs(&self) -> [(&'static str, &Option<PathBuf>); 3] {
        [
            ("--mainnet-frontier-output", &self.mainnet_frontier_output),
            ("--mainnet-subtree-output", &self.mainnet_subtree_output),
            (
                "--mainnet-frontier-grid-output",
                &self.mainnet_frontier_grid_output,
            ),
        ]
    }

    /// Rejects two artifact outputs that resolve to the same destination.
    ///
    /// The artifacts are written independently, so two flags naming one file — directly, or
    /// through a symlinked or relative parent — would silently publish one artifact where the
    /// bundle expects two.
    fn reject_aliased_artifact_outputs(&self) -> Result<(), String> {
        let mut resolved: Vec<(&'static str, PathBuf)> = Vec::new();
        for (flag, path) in self.artifact_outputs() {
            let Some(path) = path else { continue };
            let destination = resolved_output_destination(path)?;
            if let Some((earlier, _)) = resolved
                .iter()
                .find(|(_, existing)| existing == &destination)
            {
                return Err(format!("{earlier} and {flag} must use different paths"));
            }
            resolved.push((flag, destination));
        }

        Ok(())
    }
}

/// Resolve the directory aliases that [`zakura_chain::common::atomic_write`] follows.
fn resolved_output_destination(path: &Path) -> Result<PathBuf, String> {
    let file_name = path
        .file_name()
        .ok_or_else(|| format!("artifact output path has no file name: {}", path.display()))?;
    let parent = path.parent().unwrap_or_else(|| Path::new(""));
    let absolute_parent = if parent.is_absolute() {
        parent.to_path_buf()
    } else {
        env::current_dir()
            .map_err(|error| format!("resolving {}: {error}", path.display()))?
            .join(parent)
    };

    let mut resolved_parent = PathBuf::new();
    for component in absolute_parent.components() {
        match component {
            Component::Prefix(_) | Component::RootDir => {
                resolved_parent.push(component.as_os_str());
            }
            Component::CurDir => {}
            Component::ParentDir => {
                resolved_parent.pop();
            }
            Component::Normal(name) => {
                let candidate = resolved_parent.join(name);
                match fs::canonicalize(&candidate) {
                    Ok(canonical) => resolved_parent = canonical,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        resolved_parent.push(name);
                    }
                    Err(error) => {
                        return Err(format!("resolving {}: {error}", path.display()));
                    }
                }
            }
        }
    }

    Ok(resolved_parent.join(file_name))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A baseline RPC-mode `Args` value for the mode-validation tests.
    fn rpc_args() -> Args {
        Args {
            backend: Backend::Zakurad,
            transport: Transport::Cli,
            cli: "zcash-cli".to_string(),
            addr: None,
            last_checkpoint: None,
            state_cache_dir: None,
            mainnet_frontier_output: None,
            mainnet_subtree_output: None,
            mainnet_frontier_grid_output: None,
            mainnet_frontier_grid_input: None,
            mainnet_frontier_grid_checkpoint: None,
            frontier_grid_target_cost_ms: None,
            frontier_grid_spacing: None,
            full_list: false,
            zcli_args: Vec::new(),
        }
    }

    /// A baseline offline-mode `Args` value with the complete artifact output set.
    fn offline_args() -> Args {
        Args {
            state_cache_dir: Some(PathBuf::from("state")),
            mainnet_frontier_output: Some(PathBuf::from("frontier.bin")),
            mainnet_subtree_output: Some(PathBuf::from("subtrees.bin")),
            mainnet_frontier_grid_output: Some(PathBuf::from("grid.bin")),
            full_list: true,
            ..rpc_args()
        }
    }

    /// A baseline grid-backfill `Args` value.
    fn backfill_args() -> Args {
        Args {
            state_cache_dir: Some(PathBuf::from("state")),
            mainnet_frontier_grid_output: Some(PathBuf::from("grid.bin")),
            mainnet_frontier_grid_checkpoint: Some(Height(3_449_371)),
            ..rpc_args()
        }
    }

    #[test]
    fn grid_backfill_flag_combinations() {
        assert_eq!(backfill_args().validate_mode(), Ok(()));

        let mut without_output = backfill_args();
        without_output.mainnet_frontier_grid_output = None;
        assert!(
            without_output.validate_mode().is_err(),
            "a backfill needs somewhere to write"
        );

        let mut with_frontier = backfill_args();
        with_frontier.mainnet_frontier_output = Some(PathBuf::from("frontier.bin"));
        assert!(
            with_frontier.validate_mode().is_err(),
            "the other artifacts belong to a newly selected checkpoint"
        );

        let mut with_full_list = backfill_args();
        with_full_list.full_list = true;
        assert!(
            with_full_list.validate_mode().is_err(),
            "a backfill emits no checkpoint list"
        );

        let mut with_last_checkpoint = backfill_args();
        with_last_checkpoint.last_checkpoint = Some(Height(100));
        assert!(
            with_last_checkpoint.validate_mode().is_err(),
            "a backfill selects no checkpoints"
        );

        let mut without_state = backfill_args();
        without_state.state_cache_dir = None;
        assert!(
            without_state.validate_mode().is_err(),
            "a backfill reads a state database"
        );
    }

    #[test]
    fn parses_defaults_and_zcash_cli_passthrough_args() {
        let args = Args::try_parse_from(["zakura-checkpoints", "--", "-testnet", "-rpcwait"])
            .expect("valid checkpoint CLI arguments");

        assert_eq!(
            args,
            Args {
                zcli_args: vec!["-testnet".to_string(), "-rpcwait".to_string()],
                ..rpc_args()
            }
        );
    }

    #[test]
    fn exposes_version_flag() {
        let error = Args::try_parse_from(["zakura-checkpoints", "--version"])
            .expect_err("--version exits after displaying version information");

        assert_eq!(error.kind(), clap::error::ErrorKind::DisplayVersion);
    }

    #[test]
    fn rpc_mode_flag_combinations() {
        assert_eq!(rpc_args().validate_mode(), Ok(()));

        let mut frontier_without_state = rpc_args();
        frontier_without_state.mainnet_frontier_output = Some(PathBuf::from("frontier.bin"));
        assert!(frontier_without_state.validate_mode().is_err());

        let mut subtrees_without_state = rpc_args();
        subtrees_without_state.mainnet_subtree_output = Some(PathBuf::from("subtrees.bin"));
        assert!(subtrees_without_state.validate_mode().is_err());

        let mut grid_without_state = rpc_args();
        grid_without_state.mainnet_frontier_grid_output = Some(PathBuf::from("grid.bin"));
        assert!(grid_without_state.validate_mode().is_err());

        let mut full_list_without_state = rpc_args();
        full_list_without_state.full_list = true;
        assert!(full_list_without_state.validate_mode().is_err());
    }

    #[test]
    fn offline_mode_flag_combinations() {
        let offline = offline_args();
        assert_eq!(offline.validate_mode(), Ok(()));

        let mut offline_with_addr = offline.clone();
        offline_with_addr.addr = Some("127.0.0.1:8232".parse().expect("valid address"));
        assert!(offline_with_addr.validate_mode().is_err());

        let mut offline_with_zcli_args = offline.clone();
        offline_with_zcli_args.zcli_args = vec!["-testnet".to_string()];
        assert!(offline_with_zcli_args.validate_mode().is_err());

        let mut full_list_with_last = offline.clone();
        full_list_with_last.last_checkpoint = Some(Height(100));
        assert!(full_list_with_last.validate_mode().is_err());

        let mut full_list_without_frontier = offline.clone();
        full_list_without_frontier.mainnet_frontier_output = None;
        assert!(
            full_list_without_frontier.validate_mode().is_err(),
            "a replacement checkpoint list must ship with every coupled artifact"
        );

        let mut full_list_without_subtrees = offline.clone();
        full_list_without_subtrees.mainnet_subtree_output = None;
        assert!(
            full_list_without_subtrees.validate_mode().is_err(),
            "a replacement checkpoint list must ship with every coupled artifact"
        );

        let mut full_list_without_grid = offline.clone();
        full_list_without_grid.mainnet_frontier_grid_output = None;
        assert!(
            full_list_without_grid.validate_mode().is_err(),
            "a replacement checkpoint list must ship with its frontier grid too"
        );

        let mut resume_without_grid_output = offline.clone();
        resume_without_grid_output.mainnet_frontier_grid_output = None;
        resume_without_grid_output.mainnet_frontier_output = None;
        resume_without_grid_output.mainnet_subtree_output = None;
        resume_without_grid_output.full_list = false;
        resume_without_grid_output.mainnet_frontier_grid_input = Some(PathBuf::from("old.bin"));
        assert!(
            resume_without_grid_output.validate_mode().is_err(),
            "resuming a grid needs somewhere to write the result"
        );

        let mut grid_tuning_without_grid_output = offline.clone();
        grid_tuning_without_grid_output.mainnet_frontier_grid_output = None;
        grid_tuning_without_grid_output.mainnet_frontier_output = None;
        grid_tuning_without_grid_output.mainnet_subtree_output = None;
        grid_tuning_without_grid_output.full_list = false;
        grid_tuning_without_grid_output.frontier_grid_target_cost_ms = Some(1_500);
        assert!(
            grid_tuning_without_grid_output.validate_mode().is_err(),
            "grid tuning flags without the grid output are a silent no-op"
        );

        let mut matching_artifact_paths = offline.clone();
        matching_artifact_paths.mainnet_subtree_output =
            matching_artifact_paths.mainnet_frontier_output.clone();
        assert_eq!(
            matching_artifact_paths.validate_mode(),
            Err(
                "--mainnet-frontier-output and --mainnet-subtree-output must use different paths"
                    .to_string()
            )
        );

        let mut resume_without_full_list = offline;
        resume_without_full_list.full_list = false;
        resume_without_full_list.last_checkpoint = Some(Height(100));
        assert_eq!(resume_without_full_list.validate_mode(), Ok(()));
    }

    #[test]
    fn rejects_artifact_output_path_aliases() {
        let mut offline = offline_args();
        offline.mainnet_subtree_output = Some(
            env::current_dir()
                .expect("current directory is available")
                .join("frontier.bin"),
        );
        assert!(offline.validate_mode().is_err());

        let temp = tempfile::tempdir().expect("temporary directory is created");
        offline.mainnet_frontier_output = Some(temp.path().join("frontier.bin"));
        offline.mainnet_subtree_output = Some(
            temp.path()
                .join("missing-directory")
                .join("..")
                .join("frontier.bin"),
        );
        assert!(offline.validate_mode().is_err());

        let mut aliased_grid = offline_args();
        aliased_grid.mainnet_frontier_grid_output = aliased_grid.mainnet_subtree_output.clone();
        assert_eq!(
            aliased_grid.validate_mode(),
            Err(
                "--mainnet-subtree-output and --mainnet-frontier-grid-output must use different \
                 paths"
                    .to_string()
            )
        );
    }

    #[cfg(unix)]
    #[test]
    fn rejects_artifact_outputs_through_symlinked_directories() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("temporary directory is created");
        let real_directory = temp.path().join("real");
        fs::create_dir(&real_directory).expect("real output directory is created");
        let alias_directory = temp.path().join("alias");
        symlink(&real_directory, &alias_directory).expect("directory symlink is created");

        let mut offline = offline_args();
        offline.mainnet_frontier_output = Some(real_directory.join("artifact.bin"));
        offline.mainnet_subtree_output = Some(alias_directory.join("artifact.bin"));

        assert!(offline.validate_mode().is_err());
    }
}
