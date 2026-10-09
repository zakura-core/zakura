//! Runs the COMPAT-1 and COMPAT-2 matrix with each node as its own OS process.
//!
//! Usage: `matrix --node-bin <interop-node> --latest-bin <iroh-latest-node>
//! [--out <dir>] [--only <substring>]... [--repeat <n>] [--sequential]`

use std::{
    collections::BTreeMap,
    fmt::Write as _,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        atomic::{AtomicU16, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use anyhow::{anyhow, bail, ensure, Context as _, Result};
use tokio::{
    io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader},
    process::{Child, ChildStdin, Command},
    sync::mpsc,
};

const MIB: u64 = 1024 * 1024;
const TEST_ALPN: &str = "zakura-interop/test/0";
const ZAKURA_ALPN: &str = "p2p-v2/2";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Kind {
    Quic,
    Ziroh,
    Latest,
}

impl Kind {
    fn label(self) -> &'static str {
        match self {
            Kind::Quic => "quic",
            Kind::Ziroh => "ziroh",
            Kind::Latest => "iroh-latest",
        }
    }
}

#[derive(Clone)]
struct Bins {
    node: PathBuf,
    latest: PathBuf,
}

/// One COMPAT pairing: zakura-quic against an Iroh flavor.
#[derive(Clone, Copy)]
struct Pair {
    label: &'static str,
    /// The node in the "quic" role: zakura-quic, or Zakura's Iroh backend for
    /// the C0 baseline.
    this: Kind,
    other: Kind,
    alpn: &'static str,
}

const PAIRS: [Pair; 3] = [
    Pair {
        label: "C1",
        this: Kind::Quic,
        other: Kind::Ziroh,
        alpn: ZAKURA_ALPN,
    },
    Pair {
        label: "C2",
        this: Kind::Quic,
        other: Kind::Latest,
        alpn: TEST_ALPN,
    },
    // Baseline: the previous release against itself, for the slow cells only.
    Pair {
        label: "C0",
        this: Kind::Ziroh,
        other: Kind::Ziroh,
        alpn: ZAKURA_ALPN,
    },
];

static NEXT_PORT: AtomicU16 = AtomicU16::new(47100);

fn port() -> u16 {
    NEXT_PORT.fetch_add(1, Ordering::Relaxed)
}

fn v4(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

fn v6(port: u16) -> SocketAddr {
    format!("[::1]:{port}").parse().expect("valid literal")
}

fn secret(tag: u8, salt: u64) -> String {
    let mut bytes = [tag; 32];
    bytes[..8].copy_from_slice(&salt.to_le_bytes());
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Reads `key=value` from a reply line.
fn kv<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    let prefix = format!("{key}=");
    line.split_whitespace()
        .find_map(|word| word.strip_prefix(prefix.as_str()))
}

fn bracket<'a>(line: &'a str, key: &str) -> &'a str {
    kv(line, key)
        .map(|value| value.trim_start_matches('[').trim_end_matches(']'))
        .unwrap_or("")
}

type EventLog = Arc<Mutex<Vec<(Instant, String)>>>;

struct Node {
    name: String,
    kind: Kind,
    secret: String,
    binds: Vec<SocketAddr>,
    alpn: String,
    qlog: Option<PathBuf>,
    dir: PathBuf,
    bins: Bins,
    generation: u32,
    child: Child,
    stdin: ChildStdin,
    replies: mpsc::UnboundedReceiver<String>,
    events: EventLog,
    id: String,
    addrs: Vec<SocketAddr>,
}

impl Node {
    #[allow(clippy::too_many_arguments)]
    async fn spawn(
        bins: &Bins,
        dir: &Path,
        name: &str,
        kind: Kind,
        secret: String,
        binds: Vec<SocketAddr>,
        alpn: &str,
        qlog: Option<PathBuf>,
    ) -> Result<Node> {
        let (child, stdin, replies, events, ready) = launch(
            bins,
            dir,
            name,
            0,
            kind,
            &secret,
            &binds,
            alpn,
            qlog.as_deref(),
        )
        .await?;
        let (id, addrs) = parse_ready(&ready)?;
        Ok(Node {
            name: name.into(),
            kind,
            secret,
            binds,
            alpn: alpn.into(),
            qlog,
            dir: dir.into(),
            bins: bins.clone(),
            generation: 0,
            child,
            stdin,
            replies,
            events,
            id,
            addrs,
        })
    }

    async fn cmd(&mut self, line: &str, timeout: Duration) -> Result<String> {
        self.stdin
            .write_all(format!("{line}\n").as_bytes())
            .await
            .with_context(|| format!("{}: write {line}", self.name))?;
        self.stdin.flush().await?;
        let reply = tokio::time::timeout(timeout, self.replies.recv())
            .await
            .map_err(|_| anyhow!("{}: no reply to `{line}` within {timeout:?}", self.name))?
            .ok_or_else(|| anyhow!("{}: process exited during `{line}`", self.name))?;
        if let Some(rest) = reply.strip_prefix("OK ") {
            Ok(rest.to_string())
        } else {
            bail!("{}: `{line}` -> {reply}", self.name)
        }
    }

    /// Sends a command without waiting; pair with [`Node::reply`].
    async fn send(&mut self, line: &str) -> Result<()> {
        self.stdin.write_all(format!("{line}\n").as_bytes()).await?;
        self.stdin.flush().await?;
        Ok(())
    }

    async fn reply(&mut self, what: &str, timeout: Duration) -> Result<String> {
        let reply = tokio::time::timeout(timeout, self.replies.recv())
            .await
            .map_err(|_| anyhow!("{}: no reply to `{what}` within {timeout:?}", self.name))?
            .ok_or_else(|| anyhow!("{}: process exited during `{what}`", self.name))?;
        match reply.strip_prefix("OK ") {
            Some(rest) => Ok(rest.to_string()),
            None => bail!("{}: `{what}` -> {reply}", self.name),
        }
    }

    async fn kill9(&mut self) -> Result<()> {
        // tokio's start_kill sends SIGKILL on Unix.
        self.child.start_kill()?;
        self.child.wait().await?;
        Ok(())
    }

    async fn restart(&mut self) -> Result<()> {
        self.generation += 1;
        let (child, stdin, replies, events, ready) = launch(
            &self.bins,
            &self.dir,
            &self.name,
            self.generation,
            self.kind,
            &self.secret,
            &self.binds,
            &self.alpn,
            self.qlog.as_deref(),
        )
        .await?;
        let (id, addrs) = parse_ready(&ready)?;
        ensure!(id == self.id, "restarted node has a different id");
        ensure!(
            addrs == self.addrs,
            "restarted node bound {addrs:?}, not {:?}",
            self.addrs
        );
        self.child = child;
        self.stdin = stdin;
        self.replies = replies;
        self.events = events;
        Ok(())
    }

    fn addr_list(&self) -> String {
        self.addrs
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",")
    }

    async fn dial(&mut self, other: &Node, addrs: &str) -> Result<(String, String)> {
        let line = format!("DIAL {} {addrs} {}", other.id, self.alpn);
        let reply = self.cmd(&line, Duration::from_secs(30)).await?;
        let conn = kv(&reply, "conn").context("conn")?.to_string();
        Ok((conn, reply))
    }

    /// Waits for an event line matching `pred`, returning when it arrived.
    async fn wait_event(
        &self,
        timeout: Duration,
        pred: impl Fn(&str) -> bool,
    ) -> Option<(Instant, String)> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(found) = self
                .events
                .lock()
                .unwrap()
                .iter()
                .find(|(_, line)| pred(line))
                .cloned()
            {
                return Some(found);
            }
            if Instant::now() > deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    fn event_lines(&self, pred: impl Fn(&str) -> bool) -> Vec<String> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, line)| pred(line))
            .map(|(_, line)| line.clone())
            .collect()
    }

    async fn quit(mut self) {
        let _ = self.send("QUIT").await;
        let _ = tokio::time::timeout(Duration::from_secs(5), self.child.wait()).await;
        let _ = self.child.start_kill();
    }
}

type Launched = (
    Child,
    ChildStdin,
    mpsc::UnboundedReceiver<String>,
    EventLog,
    String,
);

#[allow(clippy::too_many_arguments)]
async fn launch(
    bins: &Bins,
    dir: &Path,
    name: &str,
    generation: u32,
    kind: Kind,
    secret: &str,
    binds: &[SocketAddr],
    alpn: &str,
    qlog: Option<&Path>,
) -> Result<Launched> {
    let mut command = match kind {
        Kind::Quic | Kind::Ziroh => {
            let mut command = Command::new(&bins.node);
            command.args(["--impl", kind.label()]);
            command
        }
        Kind::Latest => Command::new(&bins.latest),
    };
    command.args(["--secret", secret, "--alpn", alpn]);
    for bind in binds {
        command.args(["--bind", &bind.to_string()]);
    }
    if let Some(qlog) = qlog {
        std::fs::create_dir_all(qlog)?;
        command.arg("--qlog").arg(qlog);
    }
    let stderr = std::fs::File::create(dir.join(format!("{name}.{generation}.stderr")))?;
    let log_path = dir.join(format!("{name}.{generation}.stdout"));
    command
        .env(
            "RUST_LOG",
            std::env::var("INTEROP_NODE_LOG").unwrap_or_else(|_| "warn,zakura_quic=debug".into()),
        )
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(stderr)
        .kill_on_drop(true);
    let mut child = command.spawn().with_context(|| format!("spawn {name}"))?;
    let stdin = child.stdin.take().context("stdin")?;
    let stdout = child.stdout.take().context("stdout")?;
    let (tx, mut rx) = mpsc::unbounded_channel();
    let events: EventLog = Arc::default();
    let task_events = events.clone();
    let started = Instant::now();
    tokio::spawn(async move {
        let mut log = std::fs::File::create(log_path).ok();
        let mut lines = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if let Some(log) = log.as_mut() {
                use std::io::Write as _;
                let _ = writeln!(log, "{:>9.3} {line}", started.elapsed().as_secs_f64());
            }
            if line.starts_with("EVENT ") {
                task_events.lock().unwrap().push((Instant::now(), line));
            } else {
                let _ = tx.send(line);
            }
        }
    });
    let ready = tokio::time::timeout(Duration::from_secs(20), rx.recv())
        .await
        .map_err(|_| anyhow!("{name}: no READY within 20 s"))?
        .ok_or_else(|| anyhow!("{name}: exited before READY (see {name}.{generation}.stderr)"))?;
    ensure!(ready.starts_with("READY "), "{name}: {ready}");
    Ok((child, stdin, rx, events, ready))
}

fn parse_ready(ready: &str) -> Result<(String, Vec<SocketAddr>)> {
    let id = kv(ready, "id").context("READY id")?.to_string();
    let addrs = kv(ready, "addrs")
        .context("READY addrs")?
        .split(',')
        .map(|addr| addr.parse())
        .collect::<Result<Vec<_>, _>>()?;
    Ok((id, addrs))
}

#[derive(Clone, Debug)]
struct Row {
    cell: String,
    pass: bool,
    timing: String,
    notes: String,
}

impl Row {
    fn ok(cell: impl Into<String>, timing: impl Into<String>, notes: impl Into<String>) -> Row {
        Row {
            cell: cell.into(),
            pass: true,
            timing: timing.into(),
            notes: notes.into(),
        }
    }

    fn fail(cell: impl Into<String>, notes: impl Into<String>) -> Row {
        Row {
            cell: cell.into(),
            pass: false,
            timing: String::new(),
            notes: notes.into(),
        }
    }
}

/// Collects rows; a failing step adds a failed row and the cell stops.
struct Cell {
    name: String,
    dir: PathBuf,
    rows: Vec<Row>,
}

impl Cell {
    fn new(out: &Path, name: &str) -> Result<Cell> {
        let dir = out.join(name.replace('/', "_"));
        std::fs::create_dir_all(&dir)?;
        Ok(Cell {
            name: name.into(),
            dir,
            rows: Vec::new(),
        })
    }

    fn row(&mut self, sub: &str, result: Result<(String, String)>) -> bool {
        let cell = if sub.is_empty() {
            self.name.clone()
        } else {
            format!("{}.{sub}", self.name)
        };
        match result {
            Ok((timing, notes)) => {
                self.rows.push(Row::ok(cell, timing, notes));
                true
            }
            Err(error) => {
                self.rows.push(Row::fail(cell, format!("{error:#}")));
                false
            }
        }
    }
}

async fn xfer(node: &mut Node, conn: &str, mode: &str, bytes: u64, streams: u64) -> Result<String> {
    let reply = node
        .cmd(
            &format!("XFER {conn} {mode} {bytes} {streams}"),
            Duration::from_secs(180),
        )
        .await?;
    Ok(format!(
        "{} ms ({} MiB/s each way)",
        kv(&reply, "ms").unwrap_or("?"),
        kv(&reply, "mib_per_s_each_way").unwrap_or("?")
    ))
}

/// Runs XFER on both nodes at once.
async fn xfer_both(
    a: &mut Node,
    a_conn: &str,
    b: &mut Node,
    b_conn: &str,
    bytes: u64,
    streams: u64,
) -> Result<String> {
    let started = Instant::now();
    a.send(&format!("XFER {a_conn} bidi {bytes} {streams}"))
        .await?;
    b.send(&format!("XFER {b_conn} bidi {bytes} {streams}"))
        .await?;
    let a_reply = a.reply("XFER", Duration::from_secs(180)).await;
    let b_reply = b.reply("XFER", Duration::from_secs(180)).await;
    let a_reply = a_reply?;
    let b_reply = b_reply?;
    Ok(format!(
        "{} ms total ({}: {} ms, {}: {} ms)",
        started.elapsed().as_millis(),
        a.name,
        kv(&a_reply, "ms").unwrap_or("?"),
        b.name,
        kv(&b_reply, "ms").unwrap_or("?")
    ))
}

async fn spawn_pair(
    bins: &Bins,
    cell: &Cell,
    pair: Pair,
    salt: u64,
    dual_stack: bool,
    qlog: bool,
) -> Result<(Node, Node)> {
    let bind = |port| {
        if dual_stack {
            vec![v4(port), v6(port)]
        } else {
            vec![v4(port)]
        }
    };
    let quic = Node::spawn(
        bins,
        &cell.dir,
        &format!("a-{}", pair.this.label()),
        pair.this,
        secret(0x51, salt),
        bind(port()),
        pair.alpn,
        qlog.then(|| cell.dir.join("qlog-quic")),
    )
    .await?;
    let other = Node::spawn(
        bins,
        &cell.dir,
        &format!("b-{}", pair.other.label()),
        pair.other,
        secret(0x1e, salt),
        bind(port()),
        pair.alpn,
        qlog.then(|| cell.dir.join("qlog-iroh")),
    )
    .await?;
    Ok((quic, other))
}

/// Dials from `dialer` to `acceptor` and checks both sides' view of the connection.
async fn connect(
    dialer: &mut Node,
    acceptor: &mut Node,
    addrs: &str,
    min: usize,
) -> Result<(String, String, String)> {
    let started = Instant::now();
    let (dial_conn, reply) = dialer.dial(acceptor, addrs).await?;
    let ms = started.elapsed().as_millis();
    ensure!(
        kv(&reply, "remote") == Some(acceptor.id.as_str()),
        "dialer {} saw remote {:?}, want {}",
        dialer.name,
        kv(&reply, "remote"),
        acceptor.id
    );
    let accepted = acceptor
        .cmd(&format!("WAITCONN 10 {min}"), Duration::from_secs(15))
        .await?;
    ensure!(
        kv(&accepted, "remote") == Some(dialer.id.as_str()),
        "acceptor {} saw remote {:?}, want {}",
        acceptor.name,
        kv(&accepted, "remote"),
        dialer.id
    );
    let alpn_d = kv(&reply, "alpn").unwrap_or("?").to_string();
    let alpn_a = kv(&accepted, "alpn").unwrap_or("?").to_string();
    ensure!(
        alpn_d == dialer.alpn && alpn_a == dialer.alpn,
        "ALPN {alpn_d}/{alpn_a}"
    );
    let accept_conn = kv(&accepted, "conn").context("conn")?.to_string();
    Ok((dial_conn, accept_conn, format!("{ms} ms")))
}

/// a + b: handshake in one direction, remote IDs, then bidirectional transfers.
async fn cell_direction(
    bins: Bins,
    out: PathBuf,
    pair: Pair,
    quic_dials: bool,
    salt: u64,
) -> Vec<Row> {
    let dir_label = if quic_dials {
        "quic-dials"
    } else {
        "iroh-dials"
    };
    let mut cell = match Cell::new(&out, &format!("{}.ab.{dir_label}", pair.label)) {
        Ok(cell) => cell,
        Err(error) => {
            return vec![Row::fail(
                format!("{}.ab", pair.label),
                format!("{error:#}"),
            )]
        }
    };
    let pair_result = spawn_pair(&bins, &cell, pair, salt, false, false).await;
    let Ok((mut quic, mut other)) = pair_result.map_err(|error| {
        cell.rows
            .push(Row::fail(cell.name.clone(), format!("spawn: {error:#}")));
    }) else {
        return cell.rows;
    };
    let (dialer, acceptor) = if quic_dials {
        (&mut quic, &mut other)
    } else {
        (&mut other, &mut quic)
    };
    let addrs = acceptor.addr_list();
    let connected = connect(dialer, acceptor, &addrs, 0).await;
    let Some((d_conn, a_conn)) = connected
        .map(|(d, a, ms)| {
            cell.rows.push(Row::ok(
                format!("{}.a.handshake+remote_id", cell.name),
                ms,
                "remote_id matches on both sides; ALPN matches on both sides",
            ));
            (d, a)
        })
        .map_err(|error| {
            cell.rows.push(Row::fail(
                format!("{}.a.handshake+remote_id", cell.name),
                format!("{error:#}"),
            ))
        })
        .ok()
    else {
        return cell.rows;
    };

    let steps: [(&str, bool, &str, u64, u64); 5] = [
        ("b.1MiB-bidi.dialer-opens", true, "bidi", MIB, 1),
        ("b.1MiB-bidi.acceptor-opens", false, "bidi", MIB, 1),
        (
            "b.64MiB-bidi.dialer-opens.1-stream",
            true,
            "bidi",
            64 * MIB,
            1,
        ),
        (
            "b.64MiB-bidi.acceptor-opens.4x16MiB",
            false,
            "bidi",
            16 * MIB,
            4,
        ),
        ("b.64MiB-echo.dialer-opens", true, "echo", 64 * MIB, 1),
    ];
    for (sub, from_dialer, mode, bytes, streams) in steps {
        let result = if from_dialer {
            xfer(dialer, &d_conn, mode, bytes, streams).await
        } else {
            xfer(acceptor, &a_conn, mode, bytes, streams).await
        };
        if !cell.row(
            sub,
            result.map(|timing| (timing, "bytes verified both ways".into())),
        ) {
            break;
        }
    }
    if cell.rows.iter().all(|row| row.pass) {
        let result = xfer_both(dialer, &d_conn, acceptor, &a_conn, 32 * MIB, 2).await;
        cell.row(
            "b.64MiB-bidi.both-sides-open-2x32MiB-concurrently",
            result.map(|timing| {
                (
                    timing,
                    "both nodes open 2 streams at once; 128 MiB each way".into(),
                )
            }),
        );
    }
    let d_info = dialer
        .cmd(&format!("INFO {d_conn}"), Duration::from_secs(5))
        .await;
    let a_info = acceptor
        .cmd(&format!("INFO {a_conn}"), Duration::from_secs(5))
        .await;
    let note = format!(
        "dialer: {} | acceptor: {}",
        d_info.unwrap_or_else(|e| format!("{e:#}")),
        a_info.unwrap_or_else(|e| format!("{e:#}"))
    );
    std::fs::write(cell.dir.join("info.txt"), &note).ok();
    quic.quit().await;
    other.quit().await;
    cell.rows
}

/// Reads `quic:parameters_set` events from the qlog files in `dir`.
fn qlog_params(dir: &Path) -> Result<Vec<(String, serde_json::Value)>> {
    let mut found = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        let text = std::fs::read_to_string(&path)?;
        for line in text.lines() {
            let line = line.trim_start_matches('\u{1e}');
            if !line.contains("parameters_set") {
                continue;
            }
            let value: serde_json::Value = serde_json::from_str(line)?;
            let initiator = value["data"]["initiator"]
                .as_str()
                .unwrap_or("?")
                .to_string();
            found.push((initiator, value["data"].clone()));
        }
    }
    Ok(found)
}

/// Transport parameters minus the per-connection and per-side values.
fn comparable_params(data: &serde_json::Value) -> BTreeMap<String, serde_json::Value> {
    const SKIP: [&str; 5] = [
        "initiator",
        "initial_source_connection_id",
        "original_destination_connection_id",
        "retry_source_connection_id",
        "stateless_reset_token",
    ];
    data.as_object()
        .map(|object| {
            object
                .iter()
                .filter(|(key, _)| !SKIP.contains(&key.as_str()))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect()
        })
        .unwrap_or_default()
}

fn param_diff(a: &serde_json::Value, b: &serde_json::Value) -> Vec<String> {
    let (a, b) = (comparable_params(a), comparable_params(b));
    let mut keys: Vec<&String> = a.keys().chain(b.keys()).collect();
    keys.sort();
    keys.dedup();
    keys.into_iter()
        .filter(|key| a.get(*key) != b.get(*key))
        .map(|key| {
            let show = |map: &BTreeMap<String, serde_json::Value>| {
                map.get(key)
                    .map_or_else(|| "absent".into(), ToString::to_string)
            };
            format!("{key}: quic={} iroh={}", show(&a), show(&b))
        })
        .collect()
}

fn summarize_params(data: &serde_json::Value) -> String {
    let field = |key: &str| match data.get(key) {
        Some(value) => value.to_string(),
        None => "absent".into(),
    };
    format!(
        "initial_max_path_id={} max_remote_nat_traversal_addresses={} max_datagram_frame_size={} \
         grease_quic_bit={} min_ack_delay={} max_idle_timeout={} initial_max_streams_bidi={} \
         initial_max_streams_uni={} initial_max_data={} initial_max_stream_data_bidi_remote={}",
        field("initial_max_path_id"),
        field("max_remote_nat_traversal_addresses"),
        field("max_datagram_frame_size"),
        field("grease_quic_bit"),
        field("min_ack_delay"),
        field("max_idle_timeout"),
        field("initial_max_streams_bidi"),
        field("initial_max_streams_uni"),
        field("initial_max_data"),
        field("initial_max_stream_data_bidi_remote"),
    )
}

/// Wire observations (item 3): transport parameters from qlog on both sides,
/// plus each side's view of multipath, datagrams and NAT traversal.
async fn cell_wire(bins: Bins, out: PathBuf, pair: Pair, quic_dials: bool, salt: u64) -> Vec<Row> {
    let dir_label = if quic_dials {
        "quic-dials"
    } else {
        "iroh-dials"
    };
    let mut cell = match Cell::new(&out, &format!("{}.wire.{dir_label}", pair.label)) {
        Ok(cell) => cell,
        Err(error) => {
            return vec![Row::fail(
                format!("{}.wire", pair.label),
                format!("{error:#}"),
            )]
        }
    };
    let result = async {
        let (mut quic, mut other) = spawn_pair(&bins, &cell, pair, salt, false, true).await?;
        let (q_conn, o_conn) = if quic_dials {
            let addrs = other.addr_list();
            let (d, a, _) = connect(&mut quic, &mut other, &addrs, 0).await?;
            (d, a)
        } else {
            let addrs = quic.addr_list();
            let (d, a, _) = connect(&mut other, &mut quic, &addrs, 0).await?;
            (a, d)
        };
        xfer(&mut quic, &q_conn, "bidi", MIB, 1).await?;
        let q_info = quic
            .cmd(&format!("INFO {q_conn}"), Duration::from_secs(5))
            .await?;
        let o_info = other
            .cmd(&format!("INFO {o_conn}"), Duration::from_secs(5))
            .await?;
        quic.quit().await;
        other.quit().await;
        let params = qlog_params(&cell.dir.join("qlog-quic"))?;
        let local_raw = params
            .iter()
            .find(|(who, _)| who == "local")
            .map(|(_, data)| data.clone())
            .context("no local parameters_set in the zakura-quic qlog")?;
        let remote_raw = params
            .iter()
            .find(|(who, _)| who == "remote")
            .map(|(_, data)| data.clone())
            .context("no remote parameters_set in the zakura-quic qlog")?;
        let local = summarize_params(&local_raw);
        let remote = summarize_params(&remote_raw);
        let diff = param_diff(&local_raw, &remote_raw);
        let iroh_params = qlog_params(&cell.dir.join("qlog-iroh")).unwrap_or_default();
        let iroh_local = iroh_params
            .iter()
            .find(|(who, _)| who == "local")
            .map(|(_, data)| summarize_params(data))
            .unwrap_or_else(|| "missing".into());
        let mut notes = String::new();
        let _ = write!(
            notes,
            "quic view: multipath={} max_datagram={} nat_local={} nat_remote={} alpn={}; \
             iroh view: max_datagram={} alpn={}; \
             zakura-quic sent: {local}; iroh sent (per quic qlog): {remote}; \
             iroh sent (per iroh qlog): {iroh_local}; all-parameter diff quic vs iroh: {}",
            kv(&q_info, "multipath").unwrap_or("?"),
            kv(&q_info, "max_datagram").unwrap_or("?"),
            kv(&q_info, "nat_local").unwrap_or("?"),
            kv(&q_info, "nat_remote").unwrap_or("?"),
            kv(&q_info, "alpn").unwrap_or("?"),
            kv(&o_info, "max_datagram").unwrap_or("?"),
            kv(&o_info, "alpn").unwrap_or("?"),
            if diff.is_empty() {
                "none".to_string()
            } else {
                diff.join(", ")
            },
        );
        ensure!(
            kv(&q_info, "multipath") == Some("true"),
            "multipath not negotiated: {notes}"
        );
        // zakura-quic's own parameters must match the WIRE profile.
        ensure!(
            local.contains("initial_max_path_id=7")
                && local.contains("max_remote_nat_traversal_addresses=absent")
                && local.contains("max_datagram_frame_size=absent")
                && local.contains("grease_quic_bit=false"),
            "zakura-quic parameters break WIRE-3/4/5/6: {notes}"
        );
        // Against Zakura's Iroh backend, both sides send the same profile, so
        // neither side may send datagrams and NAT traversal stays off.
        if pair.other == Kind::Ziroh {
            ensure!(
                diff.is_empty(),
                "parameters differ from the Iroh backend: {notes}"
            );
            ensure!(
                kv(&q_info, "max_datagram") == Some("None"),
                "datagrams on: {notes}"
            );
            ensure!(
                kv(&o_info, "max_datagram") == Some("None"),
                "datagrams on: {notes}"
            );
        }
        Ok(("".to_string(), notes))
    }
    .await;
    cell.row("", result);
    cell.rows
}

/// c: kill -9 one side mid-connection, restart it with the same key and port,
/// reconnect and transfer again.
async fn cell_kill(
    bins: Bins,
    out: PathBuf,
    pair: Pair,
    kill_quic: bool,
    restarted_redials: bool,
    salt: u64,
) -> Vec<Row> {
    let victim_label = if kill_quic { "quic" } else { "iroh" };
    let redialer = if restarted_redials {
        "restarted"
    } else {
        "survivor"
    };
    let name = format!("{}.c.kill9-{victim_label}.{redialer}-redials", pair.label);
    let mut cell = match Cell::new(&out, &name) {
        Ok(cell) => cell,
        Err(error) => return vec![Row::fail(name, format!("{error:#}"))],
    };
    let result = async {
        let (mut quic, mut other) = spawn_pair(&bins, &cell, pair, salt, false, false).await?;
        let addrs = other.addr_list();
        let (q_conn, o_conn, _) = connect(&mut quic, &mut other, &addrs, 0).await?;
        xfer(&mut quic, &q_conn, "bidi", 8 * MIB, 1).await?;

        let (victim, victim_conn, survivor, survivor_old) = if kill_quic {
            (&mut quic, q_conn.clone(), &mut other, o_conn.clone())
        } else {
            (&mut other, o_conn.clone(), &mut quic, q_conn.clone())
        };
        // Keep traffic flowing so the kill lands mid-transfer. The victim opens
        // the stream, so its reply dies with it and the survivor stays free.
        victim
            .send(&format!("XFER {victim_conn} bidi {} 1", 256 * MIB))
            .await?;
        tokio::time::sleep(Duration::from_millis(300)).await;
        let killed_at = Instant::now();
        victim.kill9().await?;
        victim.restart().await?;
        let restart_ms = killed_at.elapsed().as_millis();

        let min = survivor_old.parse::<usize>()? + 1;
        let mut attempts = Vec::new();
        let redial_started = Instant::now();
        let (r_node, r_conn, s_conn) = loop {
            let attempt = if restarted_redials {
                let addrs = survivor.addr_list();
                connect(victim, survivor, &addrs, min)
                    .await
                    .map(|(d, a, _)| (true, d, a))
            } else {
                let addrs = victim.addr_list();
                connect(survivor, victim, &addrs, 0)
                    .await
                    .map(|(d, a, _)| (false, d, a))
            };
            match attempt {
                Ok((restarted_dialed, dial_conn, accept_conn)) => {
                    break if restarted_dialed {
                        (true, dial_conn, accept_conn)
                    } else {
                        (false, accept_conn, dial_conn)
                    };
                }
                Err(error) => {
                    attempts.push(format!("{error:#}"));
                    if redial_started.elapsed() > Duration::from_secs(60) {
                        bail!("redial failed for 60 s: {attempts:?}");
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        };
        let _ = r_node;
        let reconnect_ms = killed_at.elapsed().as_millis();
        let (restarted_conn, survivor_conn) = (r_conn, s_conn);
        let big = xfer(victim, &restarted_conn, "bidi", 64 * MIB, 1).await?;
        let small = xfer(survivor, &survivor_conn, "bidi", MIB, 1).await?;

        // Detection: when the survivor's old connection closes.
        let old = format!("EVENT closed conn={survivor_old} ");
        let detected = survivor
            .wait_event(Duration::from_secs(200), |line| line.starts_with(&old))
            .await;
        let detect = match &detected {
            Some((at, line)) => format!(
                "{} ms ({})",
                at.duration_since(killed_at).as_millis(),
                line.split("reason=").nth(1).unwrap_or("?")
            ),
            None => "not within 200 s".into(),
        };
        // The new connection must survive the old one's close.
        let after = xfer(survivor, &survivor_conn, "bidi", MIB, 1).await;
        let survivor_info = survivor
            .cmd(&format!("INFO {survivor_conn}"), Duration::from_secs(5))
            .await
            .unwrap_or_default();
        quic.quit().await;
        other.quit().await;
        let after = after.context("transfer on the new connection after the old one closed")?;
        ensure!(
            detected.is_some(),
            "survivor never saw the old connection close"
        );
        Ok((
            format!(
                "restart {restart_ms} ms; reconnected {reconnect_ms} ms after kill; detect {detect}"
            ),
            format!(
                "64MiB on new conn: {big}; 1MiB from survivor: {small}; after old close: {after}; \
                 failed redials before success: {}; survivor new-conn paths=[{}]",
                attempts.len(),
                bracket(&survivor_info, "paths"),
            ),
        ))
    }
    .await;
    cell.row("", result);
    cell.rows
}

/// d: Iroh dials with both of the direct node's addresses (127.0.0.1 and ::1,
/// two sockets), then 32 s of traffic; reports the paths each side saw.
async fn cell_multipath(
    bins: Bins,
    out: PathBuf,
    pair: Pair,
    iroh_dials: bool,
    salt: u64,
) -> Vec<Row> {
    let label = if iroh_dials {
        "iroh-dials"
    } else {
        "quic-dials"
    };
    let name = format!("{}.d.two-addrs.{label}", pair.label);
    let mut cell = match Cell::new(&out, &name) {
        Ok(cell) => cell,
        Err(error) => return vec![Row::fail(name, format!("{error:#}"))],
    };
    let result = async {
        let (mut quic, mut other) = spawn_pair(&bins, &cell, pair, salt, true, false).await?;
        let (q_conn, o_conn) = if iroh_dials {
            let addrs = quic.addr_list();
            let (d, a, _) = connect(&mut other, &mut quic, &addrs, 0).await?;
            (a, d)
        } else {
            let addrs = other.addr_list();
            let (d, a, _) = connect(&mut quic, &mut other, &addrs, 0).await?;
            (d, a)
        };
        let started = Instant::now();
        let mut transfers = 0;
        let mut round = 0;
        while started.elapsed() < Duration::from_secs(32) {
            if round == 8 {
                xfer(&mut other, &o_conn, "bidi", 64 * MIB, 1)
                    .await
                    .context("64 MiB mid-run")?;
            } else if round % 2 == 0 {
                xfer(&mut other, &o_conn, "bidi", MIB, 1).await?;
            } else {
                xfer(&mut quic, &q_conn, "bidi", MIB, 1).await?;
            }
            transfers += 1;
            round += 1;
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        let q_info = quic
            .cmd(&format!("INFO {q_conn}"), Duration::from_secs(5))
            .await?;
        let o_info = other
            .cmd(&format!("INFO {o_conn}"), Duration::from_secs(5))
            .await?;
        std::fs::write(cell.dir.join("info.txt"), format!("{q_info}\n{o_info}\n"))?;
        ensure!(
            kv(&q_info, "closed") == Some("no"),
            "quic side closed: {q_info}"
        );
        ensure!(
            kv(&o_info, "closed") == Some("no"),
            "iroh side closed: {o_info}"
        );
        let q_paths = quic.event_lines(|line| line.starts_with("EVENT quic_path"));
        let o_paths = other.event_lines(|line| line.starts_with("EVENT iroh_path"));
        quic.quit().await;
        other.quit().await;
        let second = !bracket(&q_info, "path_established").is_empty();
        Ok((
            format!(
                "{transfers} transfers over {} s",
                started.elapsed().as_secs()
            ),
            format!(
                "second path opened: {second}; quic: paths_open={} paths=[{}] established=[{}] \
                 abandoned=[{}]; iroh: paths_open={} paths=[{}] opened=[{}] closed=[{}] \
                 selected=[{}]; quic events={q_paths:?}; iroh events={o_paths:?}",
                kv(&q_info, "paths_open").unwrap_or("?"),
                bracket(&q_info, "paths"),
                bracket(&q_info, "path_established"),
                bracket(&q_info, "path_abandoned"),
                kv(&o_info, "paths_open").unwrap_or("?"),
                bracket(&o_info, "paths"),
                bracket(&o_info, "path_opened"),
                bracket(&o_info, "path_closed"),
                bracket(&o_info, "path_selected"),
            ),
        ))
    }
    .await;
    cell.row("", result);
    cell.rows
}

/// d: two connections between the same pair, one over 127.0.0.1 and one over
/// ::1. Iroh applies its selected path to every connection to a remote, so it
/// opens a second path on one of them (and, as client, closes the redundant one).
async fn cell_two_conns(
    bins: Bins,
    out: PathBuf,
    pair: Pair,
    second_by_iroh: bool,
    salt: u64,
) -> Vec<Row> {
    let label = if second_by_iroh {
        "iroh-dials-v4-then-v6"
    } else {
        "iroh-dials-v4.quic-dials-v6"
    };
    let name = format!("{}.d.two-conns.{label}", pair.label);
    let mut cell = match Cell::new(&out, &name) {
        Ok(cell) => cell,
        Err(error) => return vec![Row::fail(name, format!("{error:#}"))],
    };
    let result = async {
        let (mut quic, mut other) = spawn_pair(&bins, &cell, pair, salt, true, false).await?;
        let q4 = quic.addrs[0].to_string();
        let q6 = quic.addrs[1].to_string();
        let o6 = other.addrs[1].to_string();
        let (a_o, a_q, _) = connect(&mut other, &mut quic, &q4, 0).await.context("conn A (iroh dials v4)")?;
        let min_q = a_q.parse::<usize>()? + 1;
        let min_o = a_o.parse::<usize>()? + 1;
        let (b_q, b_o) = if second_by_iroh {
            let (d, a, _) = connect(&mut other, &mut quic, &q6, min_q).await.context("conn B (iroh dials v6)")?;
            (a, d)
        } else {
            let (d, a, _) = connect(&mut quic, &mut other, &o6, min_o).await.context("conn B (quic dials v6)")?;
            (d, a)
        };
        let started = Instant::now();
        let mut transfers = 0;
        let mut round = 0u32;
        while started.elapsed() < Duration::from_secs(32) {
            let (o_conn, q_conn) = if round % 2 == 0 { (&a_o, &a_q) } else { (&b_o, &b_q) };
            if round == 6 || round == 7 {
                xfer(&mut other, o_conn, "bidi", 64 * MIB, 1)
                    .await
                    .with_context(|| format!("64 MiB from iroh on conn {o_conn}"))?;
            } else if round % 4 < 2 {
                xfer(&mut other, o_conn, "bidi", MIB, 1).await.with_context(|| format!("iroh conn {o_conn}"))?;
            } else {
                xfer(&mut quic, q_conn, "bidi", MIB, 1).await.with_context(|| format!("quic conn {q_conn}"))?;
            }
            transfers += 1;
            round += 1;
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        let mut infos = Vec::new();
        for conn in [&a_q, &b_q] {
            infos.push(quic.cmd(&format!("INFO {conn}"), Duration::from_secs(5)).await?);
        }
        for conn in [&a_o, &b_o] {
            infos.push(other.cmd(&format!("INFO {conn}"), Duration::from_secs(5)).await?);
        }
        std::fs::write(cell.dir.join("info.txt"), infos.join("\n"))?;
        for info in &infos {
            ensure!(kv(info, "closed") == Some("no"), "a connection closed: {info}");
        }
        let q_events = quic.event_lines(|line| line.starts_with("EVENT quic_path") || line.starts_with("EVENT iroh_path"));
        let o_events = other.event_lines(|line| line.starts_with("EVENT iroh_path"));
        quic.quit().await;
        other.quit().await;
        let describe = |info: &str| {
            format!(
                "conn {}: open=[{}] established=[{}] abandoned=[{}] opened=[{}] closed=[{}] selected=[{}]",
                kv(info, "conn").unwrap_or("?"),
                bracket(info, "paths"),
                bracket(info, "path_established"),
                bracket(info, "path_abandoned"),
                bracket(info, "path_opened"),
                bracket(info, "path_closed"),
                bracket(info, "path_selected"),
            )
        };
        let extra = infos[..2].iter().any(|info| !bracket(info, "path_established").is_empty())
            || infos.iter().any(|info| bracket(info, "path_opened").contains(','));
        Ok((
            format!("{transfers} transfers over {} s", started.elapsed().as_secs()),
            format!(
                "extra path seen: {extra}; {} side: {} / {}; {} side: {} / {}; {} events={q_events:?}; {} events={o_events:?}",
                pair.this.label(),
                describe(&infos[0]),
                describe(&infos[1]),
                pair.other.label(),
                describe(&infos[2]),
                describe(&infos[3]),
                pair.this.label(),
                pair.other.label(),
            ),
        ))
    }
    .await;
    cell.row("", result);
    cell.rows
}

/// d (netem pass): makes Iroh open a path on a live connection.
///
/// Iroh 1.x opens extra IP paths only as a client, and only to apply its
/// selected path (or for NAT traversal, which is off). The netem pass delays
/// packets to 127.0.0.1 by 20 ms each way. Once a second connection over a
/// faster address exists, Iroh selects that address and opens it on its
/// 127.0.0.1 client connection (conn A), then closes the redundant path 0.
/// The cell then idles 40 s and transfers again.
///
/// `same_socket = false`: both nodes bind 127.0.0.1 and ::1 (two sockets), conn
/// B runs over ::1, so the new path crosses address families.
/// `same_socket = true`: both nodes bind one wildcard socket `0.0.0.0:port`,
/// conn B runs to the netns dummy address 10.9.0.1, so the new path reaches
/// the same socket under a second IP, like a multi-homed production host.
async fn cell_iroh_opens_path(
    bins: Bins,
    out: PathBuf,
    pair: Pair,
    same_socket: bool,
    salt: u64,
) -> Vec<Row> {
    let variant = if same_socket {
        "same-socket-second-ip"
    } else {
        "cross-family"
    };
    let name = format!("{}.d.netem.iroh-opens-path.{variant}", pair.label);
    let mut cell = match Cell::new(&out, &name) {
        Ok(cell) => cell,
        Err(error) => return vec![Row::fail(name, format!("{error:#}"))],
    };
    let result = async {
        let (mut quic, mut other, q4, o6) = if same_socket {
            let quic = Node::spawn(
                &bins,
                &cell.dir,
                &format!("a-{}", pair.this.label()),
                pair.this,
                secret(0x51, salt),
                vec![SocketAddr::from(([0, 0, 0, 0], port()))],
                pair.alpn,
                None,
            )
            .await?;
            let other = Node::spawn(
                &bins,
                &cell.dir,
                &format!("b-{}", pair.other.label()),
                pair.other,
                secret(0x1e, salt),
                vec![SocketAddr::from(([0, 0, 0, 0], port()))],
                pair.alpn,
                None,
            )
            .await?;
            let q4 = v4(quic.addrs[0].port()).to_string();
            let o2 = SocketAddr::from(([10, 9, 0, 1], other.addrs[0].port())).to_string();
            (quic, other, q4, o2)
        } else {
            let (quic, other) = spawn_pair(&bins, &cell, pair, salt, true, false).await?;
            let q4 = quic.addrs[0].to_string();
            let o6 = other.addrs[1].to_string();
            (quic, other, q4, o6)
        };
        // Conn A: Iroh dials over IPv4 (40 ms RTT).
        let (a_o, a_q, _) = connect(&mut other, &mut quic, &q4, 0).await.context("conn A")?;
        xfer(&mut other, &a_o, "bidi", MIB, 1).await?;
        // Conn B: the other node dials Iroh over ::1 or 10.9.0.1 (no delay).
        let min_o = a_o.parse::<usize>()? + 1;
        let (b_q, b_o, _) = connect(&mut quic, &mut other, &o6, min_o).await.context("conn B")?;
        xfer(&mut quic, &b_q, "bidi", MIB, 1).await?;
        let started = Instant::now();
        let mut transfers = 2;
        let mut round = 0u32;
        while started.elapsed() < Duration::from_secs(32) {
            match round % 4 {
                0 => xfer(&mut other, &a_o, "bidi", MIB, 1).await.context("iroh on conn A")?,
                1 => xfer(&mut quic, &a_q, "bidi", MIB, 1).await.context("other on conn A")?,
                2 => xfer(&mut other, &b_o, "bidi", MIB, 1).await.context("iroh on conn B")?,
                _ => xfer(&mut quic, &b_q, "bidi", MIB, 1).await.context("other on conn B")?,
            };
            if round == 6 {
                xfer(&mut other, &a_o, "bidi", 64 * MIB, 1).await.context("64 MiB on conn A")?;
                transfers += 1;
            }
            transfers += 1;
            round += 1;
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        let mid_q = quic.cmd(&format!("INFO {a_q}"), Duration::from_secs(5)).await?;
        let mid_o = other.cmd(&format!("INFO {a_o}"), Duration::from_secs(5)).await?;
        tokio::time::sleep(Duration::from_secs(40)).await;
        let idle_a = xfer(&mut other, &a_o, "bidi", MIB, 1).await.context("conn A after 40 s idle")?;
        let idle_b = xfer(&mut quic, &a_q, "bidi", 64 * MIB, 1).await.context("conn A 64 MiB after idle")?;
        let mut infos = Vec::new();
        for conn in [&a_q, &b_q] {
            infos.push(quic.cmd(&format!("INFO {conn}"), Duration::from_secs(5)).await?);
        }
        for conn in [&a_o, &b_o] {
            infos.push(other.cmd(&format!("INFO {conn}"), Duration::from_secs(5)).await?);
        }
        std::fs::write(cell.dir.join("info.txt"), format!("{mid_q}\n{mid_o}\n{}", infos.join("\n")))?;
        for info in &infos {
            ensure!(kv(info, "closed") == Some("no"), "a connection closed: {info}");
        }
        let q_events = quic.event_lines(|line| line.contains("_path "));
        quic.quit().await;
        other.quit().await;
        let describe = |info: &str| {
            format!(
                "conn {}: open=[{}] established=[{}] abandoned=[{}]",
                kv(info, "conn").unwrap_or("?"),
                bracket(info, "paths"),
                bracket(info, "path_established"),
                bracket(info, "path_abandoned"),
            )
        };
        let opened = !bracket(&infos[0], "path_established").is_empty();
        Ok((
            format!("{transfers} transfers over 32 s, then 40 s idle, then {idle_a} / {idle_b}"),
            format!(
                "iroh opened a path on conn A: {opened}; {} side (end): {} / {}; {} side conn A paths (end)=[{}]; \
                 conn A after 32 s: {} paths=[{}] / {} paths=[{}]; {} path events={q_events:?}",
                pair.this.label(),
                describe(&infos[0]),
                describe(&infos[1]),
                pair.other.label(),
                bracket(&infos[2], "paths"),
                pair.this.label(),
                bracket(&mid_q, "paths"),
                pair.other.label(),
                bracket(&mid_o, "paths"),
                pair.this.label(),
            ),
        ))
    }
    .await;
    cell.row("", result);
    cell.rows
}

/// e: idle for 45 s, longer than Iroh's 15 s path idle timeout, then transfer.
async fn cell_idle(bins: Bins, out: PathBuf, pair: Pair, iroh_dials: bool, salt: u64) -> Vec<Row> {
    let label = if iroh_dials {
        "iroh-dials"
    } else {
        "quic-dials"
    };
    let name = format!("{}.e.idle45s.{label}", pair.label);
    let mut cell = match Cell::new(&out, &name) {
        Ok(cell) => cell,
        Err(error) => return vec![Row::fail(name, format!("{error:#}"))],
    };
    let result = async {
        let (mut quic, mut other) = spawn_pair(&bins, &cell, pair, salt, false, false).await?;
        let (q_conn, o_conn) = if iroh_dials {
            let addrs = quic.addr_list();
            let (d, a, _) = connect(&mut other, &mut quic, &addrs, 0).await?;
            (a, d)
        } else {
            let addrs = other.addr_list();
            let (d, a, _) = connect(&mut quic, &mut other, &addrs, 0).await?;
            (d, a)
        };
        xfer(&mut quic, &q_conn, "bidi", MIB, 1).await?;
        tokio::time::sleep(Duration::from_secs(45)).await;
        let q_info = quic
            .cmd(&format!("INFO {q_conn}"), Duration::from_secs(5))
            .await?;
        let o_info = other
            .cmd(&format!("INFO {o_conn}"), Duration::from_secs(5))
            .await?;
        ensure!(
            kv(&q_info, "closed") == Some("no"),
            "quic side closed while idle: {q_info}"
        );
        ensure!(
            kv(&o_info, "closed") == Some("no"),
            "iroh side closed while idle: {o_info}"
        );
        let a = xfer(&mut quic, &q_conn, "bidi", MIB, 1).await?;
        let b = xfer(&mut other, &o_conn, "bidi", MIB, 1).await?;
        let c = xfer(&mut other, &o_conn, "bidi", 64 * MIB, 1).await?;
        let q_events = quic.event_lines(|line| line.starts_with("EVENT quic_path"));
        let o_events = other.event_lines(|line| line.starts_with("EVENT iroh_path"));
        quic.quit().await;
        other.quit().await;
        Ok((
            "45 s idle".into(),
            format!(
                "after idle: quic 1MiB {a}; iroh 1MiB {b}; iroh 64MiB {c}; quic paths=[{}] \
                 abandoned=[{}]; iroh paths=[{}] closed=[{}]; quic events={q_events:?}; \
                 iroh events={o_events:?}",
                bracket(&q_info, "paths"),
                bracket(&q_info, "path_abandoned"),
                bracket(&o_info, "paths"),
                bracket(&o_info, "path_closed"),
            ),
        ))
    }
    .await;
    cell.row("", result);
    cell.rows
}

fn args() -> BTreeMap<String, Vec<String>> {
    let mut args = BTreeMap::<String, Vec<String>>::new();
    let mut iter = std::env::args().skip(1);
    while let Some(flag) = iter.next() {
        let key = flag.trim_start_matches("--").to_string();
        if key == "sequential" {
            args.entry(key).or_default().push("1".into());
            continue;
        }
        args.entry(key)
            .or_default()
            .push(iter.next().unwrap_or_default());
    }
    args
}

type CellFuture = std::pin::Pin<Box<dyn std::future::Future<Output = Vec<Row>> + Send>>;

#[tokio::main]
async fn main() -> Result<()> {
    let args = args();
    let one = |key: &str| args.get(key).and_then(|values| values.first()).cloned();
    let bins = Bins {
        node: one("node-bin").context("--node-bin")?.into(),
        latest: one("latest-bin").context("--latest-bin")?.into(),
    };
    let out: PathBuf = one("out").map_or_else(
        || {
            std::env::temp_dir().join("iroh-interop-runs").join(format!(
                "{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
            ))
        },
        PathBuf::from,
    );
    std::fs::create_dir_all(&out)?;
    let only = args.get("only").cloned().unwrap_or_default();
    let repeat: u32 = one("repeat").map_or(Ok(1), |n| n.parse())?;
    let sequential = args.contains_key("sequential");
    let selected =
        |name: &str| only.is_empty() || only.iter().any(|filter| name.contains(filter.as_str()));

    let mut rows = Vec::new();
    let mut salt = 0u64;
    for run in 0..repeat {
        let run_out = out.join(format!("run{run}"));
        std::fs::create_dir_all(&run_out)?;
        // Fast cells run one at a time so transfer timings don't share the CPU.
        let mut fast: Vec<(String, CellFuture)> = Vec::new();
        let mut slow: Vec<(String, CellFuture)> = Vec::new();
        for pair in PAIRS {
            for quic_dials in [true, false] {
                if pair.this != Kind::Quic {
                    break;
                }
                let dir = if quic_dials {
                    "quic-dials"
                } else {
                    "iroh-dials"
                };
                salt += 1;
                fast.push((
                    format!("{}.wire.{dir}", pair.label),
                    Box::pin(cell_wire(
                        bins.clone(),
                        run_out.clone(),
                        pair,
                        quic_dials,
                        salt,
                    )),
                ));
                salt += 1;
                fast.push((
                    format!("{}.ab.{dir}", pair.label),
                    Box::pin(cell_direction(
                        bins.clone(),
                        run_out.clone(),
                        pair,
                        quic_dials,
                        salt,
                    )),
                ));
            }
            for kill_quic in [true, false] {
                for restarted_redials in [true, false] {
                    let victim = if kill_quic { "quic" } else { "iroh" };
                    let who = if restarted_redials {
                        "restarted"
                    } else {
                        "survivor"
                    };
                    salt += 1;
                    slow.push((
                        format!("{}.c.kill9-{victim}.{who}-redials", pair.label),
                        Box::pin(cell_kill(
                            bins.clone(),
                            run_out.clone(),
                            pair,
                            kill_quic,
                            restarted_redials,
                            salt,
                        )),
                    ));
                }
            }
            for iroh_dials in [true, false] {
                let label = if iroh_dials {
                    "iroh-dials"
                } else {
                    "quic-dials"
                };
                salt += 1;
                slow.push((
                    format!("{}.d.two-addrs.{label}", pair.label),
                    Box::pin(cell_multipath(
                        bins.clone(),
                        run_out.clone(),
                        pair,
                        iroh_dials,
                        salt,
                    )),
                ));
                salt += 1;
                let two = if iroh_dials {
                    "iroh-dials-v4-then-v6"
                } else {
                    "iroh-dials-v4.quic-dials-v6"
                };
                slow.push((
                    format!("{}.d.two-conns.{two}", pair.label),
                    Box::pin(cell_two_conns(
                        bins.clone(),
                        run_out.clone(),
                        pair,
                        iroh_dials,
                        salt,
                    )),
                ));
                salt += 1;
                slow.push((
                    format!("{}.e.idle45s.{label}", pair.label),
                    Box::pin(cell_idle(
                        bins.clone(),
                        run_out.clone(),
                        pair,
                        iroh_dials,
                        salt,
                    )),
                ));
            }
        }
        if std::env::var("INTEROP_NETEM_V4").as_deref() == Ok("1") {
            // The netem pass runs only the cells that need the IPv4 delay.
            fast.clear();
            slow.clear();
            // C0 is left out: an Iroh dialer reuses its selected path, so two
            // Iroh nodes never reach the two-path state this cell builds.
            for pair in PAIRS.into_iter().filter(|pair| pair.this == Kind::Quic) {
                for same_socket in [false, true] {
                    let variant = if same_socket {
                        "same-socket-second-ip"
                    } else {
                        "cross-family"
                    };
                    salt += 1;
                    slow.push((
                        format!("{}.d.netem.iroh-opens-path.{variant}", pair.label),
                        Box::pin(cell_iroh_opens_path(
                            bins.clone(),
                            run_out.clone(),
                            pair,
                            same_socket,
                            salt,
                        )),
                    ));
                }
            }
        }
        for (name, cell) in fast {
            if selected(&name) {
                eprintln!("run {run}: {name}");
                rows.extend(cell.await.into_iter().map(|mut row| {
                    row.cell = format!("r{run} {}", row.cell);
                    row
                }));
            }
        }
        let slow: Vec<_> = slow
            .into_iter()
            .filter(|(name, _)| selected(name))
            .collect();
        if sequential {
            for (name, cell) in slow {
                eprintln!("run {run}: {name}");
                rows.extend(cell.await.into_iter().map(|mut row| {
                    row.cell = format!("r{run} {}", row.cell);
                    row
                }));
            }
        } else {
            eprintln!("run {run}: {} slow cells in parallel", slow.len());
            let results = futures::future::join_all(slow.into_iter().map(|(_, cell)| cell)).await;
            for row in results.into_iter().flatten() {
                rows.push(Row {
                    cell: format!("r{run} {}", row.cell),
                    ..row
                });
            }
        }
    }

    let mut table = String::from("| cell | result | timing | notes |\n| --- | --- | --- | --- |\n");
    for row in &rows {
        let _ = writeln!(
            table,
            "| {} | {} | {} | {} |",
            row.cell,
            if row.pass { "PASS" } else { "FAIL" },
            row.timing,
            row.notes.replace('|', "/")
        );
    }
    std::fs::write(out.join("results.md"), &table)?;
    println!("{table}");
    let failed = rows.iter().filter(|row| !row.pass).count();
    println!(
        "{} rows, {failed} failed; artifacts in {}",
        rows.len(),
        out.display()
    );
    if failed > 0 {
        std::process::exit(1);
    }
    Ok(())
}
