//! Shared node agent for the interop probe.
//!
//! Every node binary (zakura-quic, the Iroh backend of the previous Zakura
//! release, and the newest upstream Iroh) runs this agent. The driver talks to
//! it over stdin and stdout, one command per line. Each command gets exactly
//! one reply line that starts with `OK ` or `ERR `. Lines that start with
//! `EVENT ` report asynchronous events, and the driver skips them.
//!
//! The upstream workspace includes this file with `#[path]`, so it depends
//! only on `anyhow`, `futures` and `tokio`.

use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{anyhow, bail, Context as _, Result};
use futures::future::BoxFuture;
use tokio::io::{
    AsyncBufReadExt as _, AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _,
};

/// A boxed send half of a bidirectional stream.
pub type BoxSend = Box<dyn AsyncWrite + Send + Unpin>;
/// A boxed receive half of a bidirectional stream.
pub type BoxRecv = Box<dyn AsyncRead + Send + Unpin>;

/// One authenticated connection, whatever the backend.
pub trait NodeConn: Send + Sync + 'static {
    fn open_bi(&self) -> BoxFuture<'_, Result<(BoxSend, BoxRecv)>>;
    fn accept_bi(&self) -> BoxFuture<'_, Result<(BoxSend, BoxRecv)>>;
    /// The TLS-proven remote node ID, as lowercase hex.
    fn remote_id(&self) -> String;
    /// The negotiated ALPN.
    fn alpn(&self) -> String;
    /// Backend-specific observations as `key=value` pairs.
    fn info(&self) -> String;
    /// Resolves with the close reason.
    fn closed(&self) -> BoxFuture<'static, String>;
    fn close_reason(&self) -> Option<String>;
}

/// One bound endpoint, whatever the backend.
pub trait NodeEndpoint: Send + Sync + 'static {
    fn id(&self) -> String;
    fn addrs(&self) -> Vec<SocketAddr>;
    fn dial(
        &self,
        id_hex: String,
        addrs: Vec<SocketAddr>,
        alpn: Vec<u8>,
    ) -> BoxFuture<'_, Result<Arc<dyn NodeConn>>>;
    /// The next inbound connection, or `None` when the endpoint stopped.
    fn accept(&self) -> BoxFuture<'_, Option<Result<Arc<dyn NodeConn>>>>;
}

/// Stream operation codes.
const OP_BIDI: u8 = 1;
const OP_ECHO: u8 = 2;
const CHUNK: usize = 64 * 1024;

/// Fills `buf` with the pattern for `seed`, starting at byte `offset` (a multiple of 8).
fn fill(buf: &mut [u8], seed: u64, offset: u64) {
    debug_assert_eq!(offset % 8, 0);
    let mut word = offset / 8;
    for chunk in buf.chunks_mut(8) {
        let value = (word ^ seed)
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            .rotate_left(29);
        chunk.copy_from_slice(&value.to_le_bytes()[..chunk.len()]);
        word += 1;
    }
}

async fn write_pattern(send: &mut BoxSend, seed: u64, len: u64) -> Result<()> {
    let mut buf = vec![0u8; CHUNK];
    let mut offset = 0u64;
    while offset < len {
        let n = (len - offset).min(CHUNK as u64) as usize;
        fill(&mut buf[..n], seed, offset);
        send.write_all(&buf[..n]).await.context("write")?;
        offset += n as u64;
    }
    Ok(())
}

async fn read_pattern(recv: &mut BoxRecv, seed: u64, len: u64) -> Result<()> {
    let mut buf = vec![0u8; CHUNK];
    let mut want = vec![0u8; CHUNK];
    let mut offset = 0u64;
    while offset < len {
        let n = (len - offset).min(CHUNK as u64) as usize;
        recv.read_exact(&mut buf[..n])
            .await
            .with_context(|| format!("read at offset {offset} of {len}"))?;
        fill(&mut want[..n], seed, offset);
        if buf[..n] != want[..n] {
            bail!("pattern mismatch in bytes {offset}..{}", offset + n as u64);
        }
        offset += n as u64;
    }
    Ok(())
}

/// Serves one peer-opened stream.
async fn serve_stream(mut send: BoxSend, mut recv: BoxRecv) -> Result<()> {
    let op = recv.read_u8().await.context("read op")?;
    let len = recv.read_u64().await.context("read len")?;
    let seed = recv.read_u64().await.context("read seed")?;
    match op {
        OP_BIDI => {
            // Send our pattern while reading theirs, then a status byte.
            let reader = async { read_pattern(&mut recv, seed, len).await };
            let writer = async { write_pattern(&mut send, seed.wrapping_add(1), len).await };
            let (read, write) = tokio::join!(reader, writer);
            write?;
            let ok = read.is_ok();
            send.write_u8(u8::from(ok)).await?;
            send.shutdown().await?;
            read
        }
        OP_ECHO => {
            tokio::io::copy(&mut recv, &mut send)
                .await
                .context("echo copy")?;
            send.shutdown().await?;
            Ok(())
        }
        other => bail!("unknown op {other}"),
    }
}

/// Runs one stream operation as the opener.
async fn run_stream(conn: &dyn NodeConn, op: u8, len: u64, seed: u64) -> Result<()> {
    let (mut send, mut recv) = conn.open_bi().await.context("open_bi")?;
    send.write_u8(op).await?;
    send.write_u64(len).await?;
    send.write_u64(seed).await?;
    let writer = async {
        write_pattern(&mut send, seed, len).await?;
        send.shutdown().await.context("finish")?;
        anyhow::Ok(())
    };
    let reader = async {
        match op {
            OP_BIDI => {
                read_pattern(&mut recv, seed.wrapping_add(1), len).await?;
                let status = recv.read_u8().await.context("read status")?;
                if status != 1 {
                    bail!("peer reported a pattern mismatch in our data");
                }
            }
            _ => read_pattern(&mut recv, seed, len).await?,
        }
        let mut rest = Vec::new();
        recv.read_to_end(&mut rest).await.context("read to end")?;
        if !rest.is_empty() {
            bail!("{} trailing bytes", rest.len());
        }
        anyhow::Ok(())
    };
    let (write, read) = tokio::join!(writer, reader);
    write.context("send side")?;
    read.context("receive side")?;
    Ok(())
}

type ConnTable = Arc<Mutex<BTreeMap<usize, Arc<dyn NodeConn>>>>;

pub fn emit(line: &str) {
    use std::io::Write as _;
    let mut out = std::io::stdout().lock();
    let _ = writeln!(out, "{line}");
    let _ = out.flush();
}

/// Registers a connection, serves the streams the peer opens and reports its close.
fn register(table: &ConnTable, conn: Arc<dyn NodeConn>, how: &str) -> usize {
    let index = {
        let mut table = table.lock().unwrap();
        let index = table.keys().next_back().map_or(0, |last| last + 1);
        table.insert(index, conn.clone());
        index
    };
    emit(&format!(
        "EVENT {how} conn={index} remote={} alpn={}",
        conn.remote_id(),
        conn.alpn()
    ));
    let server = conn.clone();
    tokio::spawn(async move {
        while let Ok((send, recv)) = server.accept_bi().await {
            tokio::spawn(async move {
                if let Err(error) = serve_stream(send, recv).await {
                    emit(&format!("EVENT stream_error conn={index} error={error:#}"));
                }
            });
        }
    });
    let watcher = conn.clone();
    tokio::spawn(async move {
        let reason = watcher.closed().await;
        emit(&format!("EVENT closed conn={index} reason={reason}"));
    });
    index
}

fn pick(table: &ConnTable, arg: Option<&str>) -> Result<(usize, Arc<dyn NodeConn>)> {
    let table = table.lock().unwrap();
    match arg {
        None | Some("last") => table
            .iter()
            .next_back()
            .map(|(index, conn)| (*index, conn.clone()))
            .ok_or_else(|| anyhow!("no connection")),
        Some(index) => {
            let index: usize = index.parse()?;
            table
                .get(&index)
                .map(|conn| (index, conn.clone()))
                .ok_or_else(|| anyhow!("no connection {index}"))
        }
    }
}

fn parse_addrs(list: &str) -> Result<Vec<SocketAddr>> {
    list.split(',')
        .filter(|item| !item.is_empty())
        .map(|item| item.parse().with_context(|| format!("bad address {item}")))
        .collect()
}

/// Runs the agent until stdin closes or `QUIT` arrives.
pub async fn run_agent(endpoint: Arc<dyn NodeEndpoint>, impl_name: &str) -> Result<()> {
    let table: ConnTable = Arc::default();

    let acceptor = endpoint.clone();
    let accept_table = table.clone();
    tokio::spawn(async move {
        while let Some(result) = acceptor.accept().await {
            match result {
                Ok(conn) => {
                    register(&accept_table, conn, "accepted");
                }
                Err(error) => emit(&format!("EVENT accept_error error={error:#}")),
            }
        }
    });

    let addrs: Vec<String> = endpoint.addrs().iter().map(ToString::to_string).collect();
    emit(&format!(
        "READY impl={impl_name} id={} addrs={} pid={}",
        endpoint.id(),
        addrs.join(","),
        std::process::id()
    ));

    let mut lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = lines.next_line().await? {
        let words: Vec<&str> = line.split_whitespace().collect();
        let Some(command) = words.first() else {
            continue;
        };
        let reply = match handle(&endpoint, &table, command, &words[1..]).await {
            Ok(Some(reply)) => format!("OK {reply}"),
            Ok(None) => break,
            Err(error) => format!("ERR {}", format!("{error:#}").replace('\n', " | ")),
        };
        emit(&reply);
    }
    Ok(())
}

async fn handle(
    endpoint: &Arc<dyn NodeEndpoint>,
    table: &ConnTable,
    command: &str,
    args: &[&str],
) -> Result<Option<String>> {
    Ok(Some(match command {
        // DIAL <id hex> <addr,addr> <alpn>
        "DIAL" => {
            let [id, addrs, alpn] = args else {
                bail!("usage: DIAL <id> <addrs> <alpn>")
            };
            let started = Instant::now();
            let conn = endpoint
                .dial(
                    id.to_string(),
                    parse_addrs(addrs)?,
                    alpn.as_bytes().to_vec(),
                )
                .await?;
            let ms = started.elapsed().as_millis();
            let remote = conn.remote_id();
            let alpn = conn.alpn();
            let index = register(table, conn, "dialed");
            format!("conn={index} remote={remote} alpn={alpn} ms={ms}")
        }
        // WAITCONN <secs> <min index>: wait for a live connection with index >= min.
        "WAITCONN" => {
            let secs: u64 = args.first().unwrap_or(&"10").parse()?;
            let min: usize = args.get(1).unwrap_or(&"0").parse()?;
            let deadline = Instant::now() + Duration::from_secs(secs);
            loop {
                let found = table
                    .lock()
                    .unwrap()
                    .range(min..)
                    .find(|(_, conn)| conn.close_reason().is_none())
                    .map(|(index, conn)| (*index, conn.clone()));
                if let Some((index, conn)) = found {
                    break format!(
                        "conn={index} remote={} alpn={}",
                        conn.remote_id(),
                        conn.alpn()
                    );
                }
                if Instant::now() > deadline {
                    bail!("no connection with index >= {min} within {secs} s");
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        // XFER <conn|last> <bidi|echo> <bytes per stream> <streams>
        "XFER" => {
            let [which, mode, bytes, streams] = args else {
                bail!("usage: XFER <conn> <bidi|echo> <bytes> <streams>")
            };
            let (index, conn) = pick(table, Some(which))?;
            let op = match *mode {
                "bidi" => OP_BIDI,
                "echo" => OP_ECHO,
                other => bail!("unknown mode {other}"),
            };
            let len: u64 = bytes.parse()?;
            let streams: u64 = streams.parse()?;
            let started = Instant::now();
            let runs = (0..streams).map(|stream| {
                let conn = conn.clone();
                let seed = started.elapsed().as_nanos() as u64 ^ (stream << 32) ^ 0xA5A5;
                async move { run_stream(conn.as_ref(), op, len, seed).await }
            });
            let results = futures::future::join_all(runs).await;
            for (stream, result) in results.into_iter().enumerate() {
                result.with_context(|| format!("stream {stream}"))?;
            }
            let secs = started.elapsed().as_secs_f64();
            // Bytes moved in both directions together.
            let moved = 2 * len * streams;
            format!(
                "conn={index} ms={:.0} mib_per_s_each_way={:.1} bytes_each_way={}",
                secs * 1000.0,
                (moved / 2) as f64 / secs / (1024.0 * 1024.0),
                len * streams
            )
        }
        // INFO <conn|last>
        "INFO" => {
            let (index, conn) = pick(table, args.first().copied())?;
            format!(
                "conn={index} remote={} alpn={} closed={} {}",
                conn.remote_id(),
                conn.alpn(),
                conn.close_reason()
                    .unwrap_or_else(|| "no".into())
                    .replace(' ', "_"),
                conn.info()
            )
        }
        // WAITCLOSED <conn> <secs>
        "WAITCLOSED" => {
            let (index, conn) = pick(table, args.first().copied())?;
            let secs: u64 = args.get(1).unwrap_or(&"10").parse()?;
            let started = Instant::now();
            match tokio::time::timeout(Duration::from_secs(secs), conn.closed()).await {
                Ok(reason) => format!(
                    "conn={index} ms={} reason={}",
                    started.elapsed().as_millis(),
                    reason.replace(' ', "_")
                ),
                Err(_) => bail!("conn={index} still open after {secs} s"),
            }
        }
        "PING" => "pong".into(),
        "QUIT" => return Ok(None),
        other => bail!("unknown command {other}"),
    }))
}

/// Parses `--flag value` pairs; repeated flags collect.
pub fn parse_args() -> BTreeMap<String, Vec<String>> {
    let mut args = BTreeMap::<String, Vec<String>>::new();
    let mut iter = std::env::args().skip(1);
    while let Some(flag) = iter.next() {
        let key = flag.trim_start_matches("--").to_string();
        let value = iter.next().unwrap_or_default();
        args.entry(key).or_default().push(value);
    }
    args
}

/// Decodes a 32-byte hex secret.
pub fn secret_bytes(hex_secret: &str) -> Result<[u8; 32]> {
    let mut out = [0u8; 32];
    if hex_secret.len() != 64 {
        bail!("secret must be 64 hex digits");
    }
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex_secret[2 * i..2 * i + 2], 16)?;
    }
    Ok(out)
}

/// Decodes a 32-byte hex node ID.
pub fn id_bytes(hex_id: &str) -> Result<[u8; 32]> {
    secret_bytes(hex_id)
}

/// Encodes bytes as lowercase hex.
pub fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Logs to stderr, filtered by `RUST_LOG` (default `warn`).
pub fn init_logging() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();
}
