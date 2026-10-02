//! One probe node: `--impl quic` (zakura-quic) or `--impl ziroh` (zakura-iroh
//! 1.1.0-rc.1 configured like the previous Zakura release).

use std::{path::PathBuf, sync::Arc};

use anyhow::{bail, Context as _, Result};
use iroh_interop::{
    iroh_backend::{IrohNode, Profile},
    proto::{parse_args, run_agent, secret_bytes, NodeEndpoint},
    quic_backend::QuicNode,
};

#[tokio::main]
async fn main() -> Result<()> {
    iroh_interop::proto::init_logging();
    let args = parse_args();
    let one = |key: &str| args.get(key).and_then(|values| values.first()).cloned();
    let implementation = one("impl").context("--impl quic|ziroh")?;
    let secret = secret_bytes(&one("secret").context("--secret <64 hex>")?)?;
    let alpn = one("alpn")
        .unwrap_or_else(|| "p2p-v2/2".into())
        .into_bytes();
    let addrs = args
        .get("bind")
        .context("--bind <addr> (repeatable)")?
        .iter()
        .map(|addr| addr.parse())
        .collect::<Result<Vec<_>, _>>()?;
    let qlog = one("qlog").map(PathBuf::from);
    let endpoint: Arc<dyn NodeEndpoint> = match implementation.as_str() {
        "quic" => Arc::new(QuicNode::bind(secret, &addrs, alpn, qlog)?),
        "ziroh" => Arc::new(IrohNode::bind(secret, &addrs, alpn, Profile::Zakura, qlog).await?),
        other => bail!("unknown --impl {other}"),
    };
    run_agent(endpoint, &implementation).await?;
    std::process::exit(0);
}
