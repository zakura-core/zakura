//! Opt-in, loopback-only HTTP access to the node's JSONL trace files.
//!
//! `GET /traces` lists the tables. `GET /traces/<table>` streams one table's
//! file with standard byte ranges, and its `ETag` names the file so that a
//! client can resume with `If-Range`. See `docs/raw-trace-endpoint.md`.

use std::{io, net::SocketAddr, path::PathBuf};

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[cfg(unix)]
mod file;
#[cfg(unix)]
mod fs_threads;
#[cfg(unix)]
mod request;
#[cfg(unix)]
mod server;

/// Start the endpoint when `address` is set. The returned task stops when
/// `shutdown` is cancelled.
///
/// A non-loopback address, a missing `directory`, a non-Unix platform or a bind
/// error fails node startup rather than exposing traces some other way.
pub(crate) async fn init(
    address: Option<SocketAddr>,
    directory: Option<PathBuf>,
    shutdown: CancellationToken,
) -> io::Result<Option<JoinHandle<()>>> {
    let Some(address) = address else {
        return Ok(None);
    };
    if !address.ip().is_loopback() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "raw trace endpoint must bind a loopback IP",
        ));
    }
    let directory = directory.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "raw trace endpoint requires network.zakura.trace_dir",
        )
    })?;
    #[cfg(unix)]
    {
        let fs_threads = fs_threads::FsThreads::process();
        let (task, _) = server::start(address, directory, fs_threads, shutdown).await?;
        Ok(Some(task))
    }
    #[cfg(not(unix))]
    {
        let _ = (directory, shutdown);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "raw trace endpoint requires Unix no-follow file opens",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn disabled_and_unsafe_configurations() {
        assert!(init(None, None, CancellationToken::new())
            .await
            .unwrap()
            .is_none());
        let refused = |address: &str, directory: Option<PathBuf>| {
            init(
                Some(address.parse().unwrap()),
                directory,
                CancellationToken::new(),
            )
        };
        assert_eq!(
            refused("0.0.0.0:0", Some(PathBuf::from(".")))
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            refused("127.0.0.1:0", None).await.unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert!(crate::components::tracing::Config::default()
            .raw_endpoint_addr
            .is_none());
    }
}
