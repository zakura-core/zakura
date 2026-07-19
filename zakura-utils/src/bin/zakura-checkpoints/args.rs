//! zakura-checkpoints arguments
//!
//! For usage please refer to the program help: `zakura-checkpoints --help`

use std::{net::SocketAddr, path::PathBuf, str::FromStr};

use structopt::StructOpt;
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
#[derive(Clone, Debug, Eq, PartialEq, StructOpt)]
pub struct Args {
    /// Backend type: the node we're connecting to.
    #[structopt(default_value = "zakurad", short, long)]
    pub backend: Backend,

    /// Transport type: the way we connect.
    #[structopt(default_value = "cli", short, long)]
    pub transport: Transport,

    /// Path or name of zcash-cli command.
    /// Only used if the transport is [`Cli`](Transport::Cli).
    #[structopt(default_value = "zcash-cli", short, long)]
    pub cli: String,

    /// Address and port for RPC connections.
    /// Used for all transports.
    #[structopt(short, long)]
    pub addr: Option<SocketAddr>,

    /// Start looking for checkpoints after this height.
    /// If there is no last checkpoint, we start looking at the Genesis block (height 0).
    #[structopt(short, long)]
    pub last_checkpoint: Option<Height>,

    /// Offline mode: read a quiesced Zakura state cache directory instead of
    /// querying a node over RPC. Mainnet only.
    ///
    /// See the "Mainnet release-state" section of
    /// `docs/design/verified-commitment-trees.md` for the pipeline this feeds.
    #[structopt(long, parse(from_os_str))]
    pub state_cache_dir: Option<PathBuf>,

    /// Offline mode: also write the VCT final-frontier artifact for the last
    /// emitted checkpoint height to this path.
    ///
    /// Requires `--state-cache-dir`.
    #[structopt(long, parse(from_os_str))]
    pub mainnet_frontier_output: Option<PathBuf>,

    /// Offline mode: print the embedded Mainnet checkpoint list before the
    /// newly generated checkpoints, so stdout is a complete replacement
    /// `main-checkpoints.txt`.
    ///
    /// Requires `--state-cache-dir`; incompatible with `--last-checkpoint`.
    #[structopt(long)]
    pub full_list: bool,

    /// Passthrough args for `zcash-cli`.
    /// Only used if the transport is [`Cli`](Transport::Cli).
    #[structopt(last = true)]
    pub zcli_args: Vec<String>,
}

impl Args {
    /// Check that offline-mode flags are used coherently.
    ///
    /// Offline and RPC modes are mutually exclusive, and the full-list output
    /// only makes sense when extending the embedded checkpoint list.
    pub fn validate_mode(&self) -> Result<(), String> {
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
        } else {
            if self.mainnet_frontier_output.is_some() {
                return Err("--mainnet-frontier-output requires --state-cache-dir".to_string());
            }
            if self.full_list {
                return Err("--full-list requires --state-cache-dir".to_string());
            }
        }

        Ok(())
    }
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
            full_list: false,
            zcli_args: Vec::new(),
        }
    }

    #[test]
    fn rpc_mode_flag_combinations() {
        assert_eq!(rpc_args().validate_mode(), Ok(()));

        let mut frontier_without_state = rpc_args();
        frontier_without_state.mainnet_frontier_output = Some(PathBuf::from("frontier.bin"));
        assert!(frontier_without_state.validate_mode().is_err());

        let mut full_list_without_state = rpc_args();
        full_list_without_state.full_list = true;
        assert!(full_list_without_state.validate_mode().is_err());
    }

    #[test]
    fn offline_mode_flag_combinations() {
        let mut offline = rpc_args();
        offline.state_cache_dir = Some(PathBuf::from("state"));
        offline.mainnet_frontier_output = Some(PathBuf::from("frontier.bin"));
        offline.full_list = true;
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

        let mut resume_without_full_list = offline;
        resume_without_full_list.full_list = false;
        resume_without_full_list.last_checkpoint = Some(Height(100));
        assert_eq!(resume_without_full_list.validate_mode(), Ok(()));
    }
}
