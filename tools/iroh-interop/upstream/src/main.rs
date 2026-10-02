//! One probe node backed by the newest released upstream Iroh 1.x, with Iroh's
//! own transport defaults, relay off and address lookup off.

use std::{path::PathBuf, sync::Arc};

use anyhow::{Context as _, Result};

#[path = "../../src/proto.rs"]
mod proto;

#[path = "../../src/iroh_backend.rs"]
mod iroh_backend;

use iroh_backend::{IrohNode, Profile};
use proto::{parse_args, run_agent, secret_bytes, NodeEndpoint};

#[tokio::main]
async fn main() -> Result<()> {
    proto::init_logging();
    let args = parse_args();
    let one = |key: &str| args.get(key).and_then(|values| values.first()).cloned();
    let secret = secret_bytes(&one("secret").context("--secret <64 hex>")?)?;
    let alpn = one("alpn").context("--alpn")?.into_bytes();
    let addrs = args
        .get("bind")
        .context("--bind <addr> (repeatable)")?
        .iter()
        .map(|addr| addr.parse())
        .collect::<Result<Vec<_>, _>>()?;
    let qlog = one("qlog").map(PathBuf::from);
    let endpoint: Arc<dyn NodeEndpoint> =
        Arc::new(IrohNode::bind(secret, &addrs, alpn, Profile::IrohDefaults, qlog).await?);
    run_agent(endpoint, "iroh-latest").await?;
    std::process::exit(0);
}
