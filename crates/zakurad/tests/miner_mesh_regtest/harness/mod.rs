//! A miner mesh Regtest network of real `zakurad` processes on loopback.
//!
//! Each node is a `zakurad` child that runs only the v2 P2P stack, with a node
//! key the harness chose, a fixed loopback v2 port, a fixed RPC port and the
//! bootstrap peers its test names. A restarted node keeps its key, its ports
//! and its state directory.
//!
//! In-process v2 peers probe whether a child refuses an ordinary connection
//! at its capacity limit or when it is holding a place for a gateway.
//!
//! The harness counts the gateways named in blocks itself, from `getblock`
//! and the chain crate's coinbase parser. Every step that changes the net ends with a
//! check of the standing invariants on every running node, from its
//! `getminermeshinfo` report and that count (see [`mesh_view`]).
//!
//! Dropping a [`RegtestNet`], also while a test panics, kills every child and
//! then removes its directory.

use std::{
    collections::HashMap,
    fs,
    net::{Ipv4Addr, SocketAddr, TcpListener, UdpSocket},
    ops::RangeInclusive,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    time::{Duration, Instant},
};

use eyre::{eyre, Result, WrapErr};
use tempfile::TempDir;

use zakura_chain::{
    block::{self, Block, Height},
    miner_mesh::{self, MinerMeshCommitment},
    parameters::Network,
};
use zakura_network::{
    zakura::{zakura_trace_peer_label, ZakuraPeerId, CONN_TABLE},
    P2pStack,
};
use zakura_node_services::rpc_client::RpcRequestClient;
use zakura_quic::{NodeId, NodeSecretKey};
use zakura_test::{args, command::TestChild};
use zakurad::config::ZakuradConfig;

mod mesh_view;

pub use mesh_view::MeshView;

use mesh_view::{check_view, NodeFacts};

use crate::common::{
    config::{rpc_port_config, testdir},
    launch::ZakuradTestDirExt,
    regtest::MiningRpcMethods,
};

/// How long a child gets to start answering RPC requests.
const LAUNCH_TIMEOUT: Duration = Duration::from_secs(120);

/// How long one RPC request may take.
const RPC_TIMEOUT: Duration = Duration::from_secs(30);

/// How often the harness asks a node again while it waits for it.
const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// How long every running node gets to reach the same best block.
const SETTLE_TIMEOUT: Duration = Duration::from_secs(120);

/// How long the mining nodes get to count a new best chain's gateways.
const MESH_TIMEOUT: Duration = Duration::from_secs(60);

/// How many times a report that breaks an invariant is read before the
/// check fails.
const CHECK_ATTEMPTS: usize = 5;

/// How long the harness waits before reading a report again. Nodes form
/// their preferred set about once a second.
const CHECK_RETRY_INTERVAL: Duration = Duration::from_millis(500);

/// The blocks one `generate` call mines, so each call ends well within the
/// RPC timeout.
const BLOCKS_PER_GENERATE: u32 = 10;

/// How long a node gets to shut down before it is killed.
const STOP_TIMEOUT: Duration = Duration::from_secs(20);

/// How long a filler waits for a child to accept its connection.
#[cfg(feature = "prometheus")]
const FILLER_DIAL_TIMEOUT: Duration = Duration::from_secs(15);

/// Connections a child accepts from one IP address. Every node and filler
/// connects from 127.0.0.1, so the default of 16 would cap a larger cluster.
const MAX_CONNECTIONS_PER_IP: usize = 1_000;

/// The first seed for filler node keys. The testkit derives filler keys from
/// their seeds by hashing, so a filler's node ID never matches a child's.
#[cfg(feature = "prometheus")]
const FIRST_FILLER_SEED: u64 = 1_000;

/// The lines of a node's log a startup failure report includes.
const LOG_TAIL_LINES: usize = 40;

/// How long a peer's accepted connection record can take to reach the trace file.
const CONNECTION_TRACE_TIMEOUT: Duration = Duration::from_secs(10);

/// The blocks above a marker before the gateway it names counts.
pub const QUALIFYING_DEPTH: u32 = 10;

/// The fewest counted blocks that must name a gateway before a node ranks it.
pub const MIN_GATEWAY_BLOCKS: u32 = 10;

/// The heights a node counts, the newest at the qualifying depth: two days of
/// blocks at Regtest's 75 second spacing.
pub const WINDOW_BLOCKS: u32 = 2 * 24 * 60 * 60 / 75;

/// The first 20 bytes of a v2 node ID, as a coinbase names a gateway.
pub type GatewayId = [u8; 20];

/// A node's best block.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct Tip {
    pub height: Height,
    pub hash: block::Hash,
}

/// What a node does on the network.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Role {
    /// Mines, and names a gateway in every coinbase: its own node, or the
    /// node [`NodeSpec::names_gateway`] picks.
    Pool,
    /// Mines without naming a gateway.
    Miner,
    /// Relays blocks and does not mine.
    Relay,
    /// Relays blocks without mining and runs the miner mesh, as a pool's
    /// public relay node does: it prefers gateways, and a pool may name it.
    MeshRelay,
}

impl Role {
    /// Whether the node has a miner address.
    fn mines(self) -> bool {
        matches!(self, Self::Pool | Self::Miner)
    }

    /// Whether the node sets `mining.miner_mesh`.
    fn runs_miner_mesh(self) -> bool {
        matches!(self, Self::Pool | Self::MeshRelay)
    }
}

/// How to start one node of a [`RegtestNet`].
#[derive(Clone, Debug)]
pub struct NodeSpec {
    role: Role,
    bootstrap_from: Vec<usize>,
    names_gateway: Option<usize>,
    template_nodes: Vec<usize>,
    max_connections: Option<usize>,
    start_later: bool,
    trace_connections: bool,
}

impl NodeSpec {
    /// A node with `role`, no bootstrap peers and the default connection limit.
    pub fn new(role: Role) -> Self {
        Self {
            role,
            bootstrap_from: Vec::new(),
            names_gateway: None,
            template_nodes: Vec::new(),
            max_connections: None,
            start_later: false,
            trace_connections: false,
        }
    }

    /// Makes the node dial these nodes, by index, at startup.
    pub fn bootstrap_from(mut self, nodes: impl IntoIterator<Item = usize>) -> Self {
        self.bootstrap_from = nodes.into_iter().collect();
        self
    }

    /// Makes this pool's coinbases name `node`, by index, instead of itself.
    pub fn names_gateway(mut self, node: usize) -> Self {
        self.names_gateway = Some(node);
        self
    }

    /// Makes this gateway keep places for these template nodes, by index:
    /// `mining.miner_mesh_template_nodes`.
    pub fn template_nodes(mut self, nodes: impl IntoIterator<Item = usize>) -> Self {
        self.template_nodes = nodes.into_iter().collect();
        self
    }

    /// Sets the node's `network.zakura.max_connections`.
    pub fn max_connections(mut self, limit: usize) -> Self {
        self.max_connections = Some(limit);
        self
    }

    /// Leaves the node stopped when the net starts, until
    /// [`RegtestNet::start_node`].
    pub fn start_later(mut self) -> Self {
        self.start_later = true;
        self
    }

    /// Enables the node's trace so a test can identify its accepted connections.
    pub fn trace_connections(mut self) -> Self {
        self.trace_connections = true;
        self
    }
}

#[derive(serde::Deserialize)]
struct ConnectionTrace {
    event: String,
    peer: Option<String>,
    conn: u64,
}

/// One `zakurad` child.
struct RegtestNode {
    /// The running process, or `None` while the node is stopped. It is
    /// declared first so it is killed before its directory is removed.
    process: Option<TestChild<PathBuf>>,
    /// The config, state, node key and log of this node.
    dir: TempDir,
    /// The sockets that keep the node's ports from other processes until it
    /// first starts.
    reserved_ports: Option<ReservedPorts>,
    role: Role,
    node_id: NodeId,
    v2_addr: SocketAddr,
    rpc: RpcRequestClient,
    #[cfg(feature = "prometheus")]
    metrics: RpcRequestClient,
    facts: NodeFacts,
    /// The node's report at the last invariant check, if still available.
    last_view: Option<MeshView>,
}

impl RegtestNode {
    fn log_file(&self) -> PathBuf {
        log_file(self.dir.path())
    }

    /// Starts the process and waits until it answers RPC requests.
    async fn launch(&mut self, index: usize) -> Result<()> {
        // Release the ports just before the node binds them.
        drop(self.reserved_ports.take());
        let process = self
            .dir
            .path()
            .to_path_buf()
            .spawn_child(args!["start"])
            .wrap_err_with(|| format!("node {index} could not be spawned"))?;
        self.process = Some(process);

        let deadline = Instant::now() + LAUNCH_TIMEOUT;
        loop {
            self.is_running(index)?;
            if self.rpc.blockchain_info().await.is_ok() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(eyre!(
                    "node {index} did not answer RPC requests within {LAUNCH_TIMEOUT:?}\n{}",
                    log_tail(&self.log_file())
                ));
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    /// An absent process is intentional. An exited child is a test failure.
    fn is_running(&mut self, index: usize) -> Result<bool> {
        let Some(process) = &mut self.process else {
            return Ok(false);
        };
        let child = process.child.as_mut().expect("the harness owns the child");
        if let Some(status) = child
            .try_wait()
            .wrap_err_with(|| format!("checking whether node {index} exited"))?
        {
            return Err(eyre!(
                "node {index} exited unexpectedly ({status})\n{}",
                log_tail(&self.log_file())
            ));
        }
        Ok(true)
    }

    /// Asks the process to shut down, as an operator would, and waits until
    /// it exits, killing it if it is still running after [`STOP_TIMEOUT`].
    ///
    /// A node that shuts down closes its v2 connections, so its peers notice
    /// at once rather than after the QUIC idle timeout.
    async fn stop(&mut self) {
        if let Some(child) = self
            .process
            .as_mut()
            .and_then(|process| process.child.as_mut())
        {
            let terminated = Command::new("kill")
                .args(["-TERM", &child.id().to_string()])
                .status()
                .is_ok_and(|status| status.success());
            let deadline = Instant::now() + STOP_TIMEOUT;
            while terminated && Instant::now() < deadline && matches!(child.try_wait(), Ok(None)) {
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        }
        // Dropping a test child kills it if it still runs, and reads its
        // output to the end, which comes once its ports are released.
        drop(self.process.take());
    }

    async fn tip(&self) -> Result<Tip> {
        let info = self.rpc.blockchain_info().await?;
        Ok(Tip {
            height: info.blocks(),
            hash: info.best_block_hash(),
        })
    }

    async fn mesh_view(&self) -> Result<MeshView> {
        let info = self
            .rpc
            .json_result_from_call("getminermeshinfo", "[]")
            .await
            .map_err(|error| eyre!("getminermeshinfo failed: {error}"))?;
        Ok(MeshView { info })
    }
}

/// The receiving node's reason for refusing a filler.
#[cfg(feature = "prometheus")]
pub enum FillerRefusal {
    AdmissionLimit,
    HeldGatewayPlace,
}

/// A Regtest network of `zakurad` children.
pub struct RegtestNet {
    nodes: Vec<RegtestNode>,
    #[cfg(feature = "prometheus")]
    network: Network,
    #[cfg(feature = "prometheus")]
    next_filler_seed: u64,
}

impl RegtestNet {
    /// Starts one node per spec, in order, and waits until each answers RPC
    /// requests. A node made with [`NodeSpec::start_later`] waits for
    /// [`Self::start_node`].
    ///
    /// Every node's key and ports are chosen before the first one starts, so a
    /// node may bootstrap from a node with a higher index.
    pub async fn start(specs: Vec<NodeSpec>) -> Result<Self> {
        let network = Network::new_regtest(Default::default());
        let keys = (0..specs.len()).map(node_key).collect::<Result<Vec<_>>>()?;
        let mut reservations = (0..specs.len())
            .map(|_| ReservedPorts::bind())
            .collect::<Result<Vec<_>>>()?;
        let v2_addrs = reservations
            .iter()
            .map(|ports| ports.v2.local_addr())
            .collect::<std::io::Result<Vec<_>>>()?;
        let rpc_ports = reservations
            .iter()
            .map(|ports| Ok(ports.rpc.local_addr()?.port()))
            .collect::<Result<Vec<_>>>()?;

        let mut net = Self {
            #[cfg(feature = "prometheus")]
            network: network.clone(),
            nodes: Vec::with_capacity(specs.len()),
            #[cfg(feature = "prometheus")]
            next_filler_seed: FIRST_FILLER_SEED,
        };

        for ((index, spec), reserved_ports) in specs.iter().enumerate().zip(reservations.drain(..))
        {
            let bootstrap_peers = spec
                .bootstrap_from
                .iter()
                .map(|&peer| {
                    let id = keys.get(peer).ok_or_else(|| {
                        eyre!("node {index} bootstraps from node {peer}, which is not in the net")
                    })?;
                    Ok(format!("{}@{}", id.public(), v2_addrs[peer]))
                })
                .collect::<Result<Vec<_>>>()?;
            let gateway_id_of = |node: usize| {
                let key = keys.get(node).ok_or_else(|| {
                    eyre!("node {index} names node {node}, which is not in the net")
                })?;
                Ok::<_, eyre::Report>(miner_mesh::GatewayId::from_node_id(key.public().as_bytes()))
            };
            let named_gateway = spec
                .names_gateway
                .map(|gateway| {
                    eyre::ensure!(
                        spec.role == Role::Pool,
                        "node {index} names a gateway but is not a pool"
                    );
                    gateway_id_of(gateway)
                })
                .transpose()?;
            let mesh = MeshSettings {
                gateways: named_gateway.into_iter().collect(),
                template_nodes: spec
                    .template_nodes
                    .iter()
                    .map(|&node| gateway_id_of(node))
                    .collect::<Result<_>>()?,
            };

            let dir = testdir()?;
            let identity_dir = dir.path().join("identity");
            fs::create_dir_all(&identity_dir)?;
            fs::write(
                identity_dir.join("regtest.zakura-iroh-secret-key"),
                keys[index].to_hex(),
            )?;

            let mut config = rpc_port_config(rpc_ports[index], false, &network)?;
            #[cfg(feature = "prometheus")]
            let metrics_addr = reserved_ports.metrics.local_addr()?;
            #[cfg(feature = "prometheus")]
            {
                config.metrics.endpoint_addr = Some(metrics_addr);
            }
            configure_node(
                &mut config,
                spec,
                v2_addrs[index],
                identity_dir,
                log_file(dir.path()),
                bootstrap_peers,
                &mesh,
            );
            if spec.trace_connections {
                config.network.zakura.trace_dir = Some(dir.path().join("trace"));
            }
            dir.path().to_path_buf().with_config(&mut config)?;

            let rpc_addr = config
                .rpc
                .listen_addr
                .expect("rpc_port_config always sets the RPC address");
            let node_id = keys[index].public();
            let announced = match spec.role {
                Role::Pool => Some(
                    named_gateway
                        .unwrap_or_else(|| miner_mesh::GatewayId::from_node_id(node_id.as_bytes())),
                ),
                Role::Miner | Role::Relay | Role::MeshRelay => None,
            };
            let limits = zakura_network::zakura::ZakuraLocalLimits::from_config(&config.network);
            net.nodes.push(RegtestNode {
                process: None,
                dir,
                reserved_ports: Some(reserved_ports),
                role: spec.role,
                node_id,
                v2_addr: v2_addrs[index],
                rpc: RpcRequestClient::new_with_timeout(rpc_addr, RPC_TIMEOUT),
                #[cfg(feature = "prometheus")]
                metrics: RpcRequestClient::new_with_timeout(metrics_addr, RPC_TIMEOUT),
                facts: NodeFacts {
                    node_id: *node_id.as_bytes(),
                    mining: spec.role.mines(),
                    prefers_gateways: spec.role.mines() || spec.role.runs_miner_mesh(),
                    announced: announced.map(|gateway| *gateway.as_bytes()),
                    template_nodes: mesh
                        .template_nodes
                        .iter()
                        .map(|node| *node.as_bytes())
                        .collect(),
                    total_capacity: limits.total_connections(),
                    gateway_capacity: limits.max_preferred_connections,
                },
                last_view: None,
            });
        }

        for (index, spec) in specs.iter().enumerate() {
            if !spec.start_later {
                net.nodes[index].launch(index).await?;
            }
        }

        Ok(net)
    }

    /// The RPC client of `node`.
    pub fn rpc(&self, node: usize) -> &RpcRequestClient {
        &self.nodes[node].rpc
    }

    /// The v2 node ID of `node`, which a pool names as its gateway.
    pub fn node_id(&self, node: usize) -> NodeId {
        self.nodes[node].node_id
    }

    /// The gateway ID a coinbase names when it names `node`: the first 20
    /// bytes of its node ID.
    pub fn gateway_id(&self, node: usize) -> GatewayId {
        self.nodes[node].node_id.as_bytes()[..20]
            .try_into()
            .expect("a node ID is longer than a gateway ID")
    }

    /// The gateway IDs of `nodes`, in order.
    pub fn gateway_ids(&self, nodes: impl IntoIterator<Item = usize>) -> Vec<GatewayId> {
        nodes
            .into_iter()
            .map(|node| self.gateway_id(node))
            .collect()
    }

    /// Returns the most recently accepted connection ID for this peer.
    ///
    /// The trace writer flushes asynchronously, so wait for the initial record.
    /// When comparing IDs across a transition, hold the final peer state long
    /// enough for any replacement to be written before reading again.
    pub async fn accepted_connection_id(&self, node: usize, peer: usize) -> Result<u64> {
        let label = zakura_trace_peer_label(&ZakuraPeerId::from_node_id(self.node_id(peer)));
        let path = self.nodes[node]
            .dir
            .path()
            .join("trace")
            .join(CONN_TABLE.file_name());
        let deadline = Instant::now() + CONNECTION_TRACE_TIMEOUT;
        loop {
            match tokio::fs::read_to_string(&path).await {
                Ok(trace) => {
                    // A concurrent write can leave a partial last row.
                    let complete = trace.rsplit_once('\n').map_or("", |(rows, _)| rows);
                    for line in complete.lines().rev() {
                        let row: ConnectionTrace = serde_json::from_str(line)
                            .wrap_err_with(|| format!("reading {}", path.display()))?;
                        if row.event == "accepted" && row.peer.as_deref() == Some(label.as_str()) {
                            return Ok(row.conn);
                        }
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(error).wrap_err_with(|| format!("reading {}", path.display()))
                }
            }
            eyre::ensure!(
                Instant::now() < deadline,
                "node {node} did not trace an accepted connection to node {peer}"
            );
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    /// `node`'s report at the last invariant check.
    ///
    /// # Panics
    ///
    /// If `node` was not running at the last check.
    pub fn view(&self, node: usize) -> &MeshView {
        self.nodes[node]
            .last_view
            .as_ref()
            .unwrap_or_else(|| panic!("node {node} was not running at the last check"))
    }

    /// Mines `count` blocks on `node`, then [settles](Self::settle) the net.
    pub async fn mine(&mut self, node: usize, count: u32) -> Result<Tip> {
        self.generate(node, count).await?;
        self.settle().await
    }

    /// Mines `count` blocks on `node`, then [settles](Self::settle_island)
    /// `island` alone, which holds `node`. Nodes outside it may follow other
    /// chains.
    pub async fn mine_on_island(
        &mut self,
        node: usize,
        count: u32,
        island: &[usize],
    ) -> Result<Tip> {
        if !island.contains(&node) {
            return Err(eyre!("node {node} mines outside its island {island:?}"));
        }
        self.generate(node, count).await?;
        self.settle_island(island).await
    }

    /// Waits until every running node has the same best block and every
    /// mining node's gateway counts match the harness's own count of that
    /// chain, then checks the standing invariants. Returns the best block.
    pub async fn settle(&mut self) -> Result<Tip> {
        let everyone: Vec<usize> = (0..self.nodes.len()).collect();
        self.settle_island(&everyone).await
    }

    /// [Settles](Self::settle) the running nodes of `island` only: they reach
    /// one best block and count its gateways, whatever chains the other
    /// nodes follow. The standing invariants are still checked on every
    /// running node.
    pub async fn settle_island(&mut self, island: &[usize]) -> Result<Tip> {
        let tip = self.wait_for_one_tip(island).await?;
        self.wait_for_matching_counts(tip, island).await?;
        self.check_invariants().await?;
        Ok(tip)
    }

    /// Checks the standing invariants until `done` holds, or fails after
    /// `timeout`. `done` reads the reports through [`Self::view`].
    pub async fn wait_for(
        &mut self,
        what: &str,
        timeout: Duration,
        done: impl Fn(&Self) -> bool,
    ) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            self.check_invariants().await?;
            if done(self) {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(eyre!(
                    "{what} did not happen within {timeout:?}\n{}",
                    self.describe_failure()
                ));
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    /// Checks the standing invariants for `period`, and fails at the first
    /// check where `holds` does not.
    pub async fn hold_for(
        &mut self,
        what: &str,
        period: Duration,
        holds: impl Fn(&Self) -> bool,
    ) -> Result<()> {
        let deadline = Instant::now() + period;
        loop {
            self.check_invariants().await?;
            if !holds(self) {
                return Err(eyre!("{what} stopped holding\n{}", self.describe_failure()));
            }
            if Instant::now() >= deadline {
                return Ok(());
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    /// Starts `node`, which was made with [`NodeSpec::start_later`] or
    /// stopped, and waits until it answers RPC requests.
    ///
    pub async fn start_node(&mut self, node: usize) -> Result<()> {
        if self.nodes[node].is_running(node)? {
            return Err(eyre!("node {node} is already running"));
        }
        self.forget_views();
        self.nodes[node].launch(node).await
    }

    /// Shuts `node` down. It keeps its key, ports and state for
    /// [`Self::start_node`].
    pub async fn stop(&mut self, node: usize) {
        self.forget_views();
        self.nodes[node].stop().await;
    }

    /// `node`'s block at `height`.
    pub async fn block(&self, node: usize, height: u32) -> Result<Block> {
        let block = self
            .rpc(node)
            .get_block(i32::try_from(height)?)
            .await
            .map_err(|error| eyre!("node {node} could not read block {height}: {error}"))?
            .ok_or_else(|| eyre!("node {node} has no block at height {height}"))?;
        Ok(Arc::unwrap_or_clone(block))
    }

    /// Submits `block` to `node`, so it gets the block without a connection
    /// to the node that mined it.
    pub async fn submit_block(&self, node: usize, block: Block) -> Result<()> {
        self.rpc(node).submit_block(block).await
    }

    /// Counts, from `node`'s blocks at `heights`, how many blocks name each
    /// gateway.
    ///
    /// Reads each block with `getblock` so the count is independent of the node's tracker.
    pub async fn recount(
        &self,
        node: usize,
        heights: RangeInclusive<u32>,
    ) -> Result<HashMap<GatewayId, u32>> {
        let mut counts = HashMap::new();
        for height in heights {
            if let Some(gateway) = named_gateway(&self.block(node, height).await?) {
                *counts.entry(gateway).or_insert(0) += 1;
            }
        }
        Ok(counts)
    }

    /// Starts one filler and requires the receiving node to record the expected refusal.
    #[cfg(feature = "prometheus")]
    pub async fn expect_filler_refused(
        &mut self,
        node: usize,
        reason: FillerRefusal,
    ) -> Result<()> {
        let counter = match reason {
            FillerRefusal::AdmissionLimit => "zakura_p2p_conn_rejected_admission",
            FillerRefusal::HeldGatewayPlace => "zakura_p2p_conn_rejected_hold",
        };
        use zakura_network::zakura::testkit::ZakuraTestNode;
        use zakura_quic::NodeAddr;

        let before = self.counter(node, counter).await?;
        let seed = self.next_filler_seed;
        self.next_filler_seed += 1;
        let filler = ZakuraTestNode::builder(seed)
            .handshake_network(self.network.clone())
            .service_from_supervisor(serve_nothing)
            .spawn()
            .await
            .map_err(|error| eyre!("filler {seed} could not start: {error}"))?;
        let target = NodeAddr::with_addrs(self.nodes[node].node_id, [self.nodes[node].v2_addr]);
        let result = filler
            .connect_native_to_addr(target, FILLER_DIAL_TIMEOUT)
            .await;
        filler.shutdown().await;

        let Err(error) = result else {
            return Err(eyre!("node {node} accepted an additional filler"));
        };
        eyre::ensure!(
            self.counter(node, counter).await? > before,
            "node {node} did not record a {counter} refusal: {error}"
        );
        Ok(())
    }

    /// Reads the total for a Prometheus counter across all labels.
    #[cfg(feature = "prometheus")]
    pub async fn counter(&self, node: usize, name: &str) -> Result<u64> {
        let mut total = 0;
        for value in self.metric_values(node, name).await? {
            total += value.parse::<u64>()?;
        }
        Ok(total)
    }

    /// Reads a Prometheus gauge without labels, or `None` when the node has
    /// not set it.
    #[cfg(feature = "prometheus")]
    pub async fn gauge(&self, node: usize, name: &str) -> Result<Option<f64>> {
        match self.metric_values(node, name).await?.as_slice() {
            [] => Ok(None),
            [value] => Ok(Some(value.parse()?)),
            values => Err(eyre!("node {node} reports {name} {} times", values.len())),
        }
    }

    /// The value of every series of the Prometheus metric `name` on `node`.
    #[cfg(feature = "prometheus")]
    async fn metric_values(&self, node: usize, name: &str) -> Result<Vec<String>> {
        let response = self.nodes[node].metrics.get().await?;
        eyre::ensure!(
            response.status().is_success(),
            "metrics HTTP {}",
            response.status()
        );
        let body = std::str::from_utf8(response.body())?;
        Ok(body
            .lines()
            .filter_map(|line| line.split_once(' '))
            .filter(|(series, _)| series.split('{').next() == Some(name))
            .map(|(_, value)| value.trim().to_owned())
            .collect())
    }

    /// Mines `count` blocks on `node`, a few per RPC request.
    async fn generate(&self, node: usize, count: u32) -> Result<()> {
        if !self.nodes[node].role.mines() {
            return Err(eyre!("node {node} is a relay, so it has no miner address"));
        }

        let mut left = count;
        while left > 0 {
            let blocks = left.min(BLOCKS_PER_GENERATE);
            self.rpc(node)
                .generate(blocks)
                .await
                .wrap_err_with(|| format!("node {node} could not mine {blocks} blocks"))?;
            left -= blocks;
        }
        Ok(())
    }

    /// Waits until every running node of `island` has the same best block.
    async fn wait_for_one_tip(&mut self, island: &[usize]) -> Result<Tip> {
        let deadline = Instant::now() + SETTLE_TIMEOUT;
        loop {
            let mut tips = Vec::new();
            for &index in island {
                let node = &mut self.nodes[index];
                if node.is_running(index)? {
                    tips.push((index, node.tip().await.map_err(|error| error.to_string())));
                }
            }

            let first = tips.first().and_then(|(_, tip)| tip.as_ref().ok());
            if let Some(&tip) = first {
                if tips.iter().all(|(_, other)| other.as_ref() == Ok(&tip)) {
                    return Ok(tip);
                }
            }
            if Instant::now() >= deadline {
                return Err(eyre!(
                    "the nodes did not reach one best block within {SETTLE_TIMEOUT:?}: {tips:?}\n{}",
                    self.describe_failure()
                ));
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    /// Waits until every running node of `island` that prefers gateways has read its whole
    /// window at `tip`, and ranks each gateway the harness finds named in at
    /// least [`MIN_GATEWAY_BLOCKS`] of the same blocks, with that count, and
    /// its template nodes, and no other: standing invariant 6. A node never
    /// ranks its own ID.
    async fn wait_for_matching_counts(&mut self, tip: Tip, island: &[usize]) -> Result<()> {
        let newest = tip.height.0.checked_sub(QUALIFYING_DEPTH);
        let oldest = newest.map(|height| height.saturating_sub(WINDOW_BLOCKS - 1));
        let bounds = oldest.zip(newest);
        let checked = bounds.map_or(0, |(oldest, newest)| newest - oldest + 1);
        let checked = usize::try_from(checked).expect("the Regtest window fits in usize");
        let mut recounted = None;
        let deadline = Instant::now() + MESH_TIMEOUT;
        loop {
            let mut behind = Vec::new();
            for &index in island {
                if !self.nodes[index].is_running(index)?
                    || !self.nodes[index].facts.prefers_gateways
                {
                    continue;
                }
                let view = self.nodes[index].mesh_view().await?;
                let Some(window) = &view.info.window else {
                    behind.push(format!("node {index} reports no window"));
                    continue;
                };
                if window.tip != Some(tip.height.0)
                    || window.oldest != oldest
                    || window.newest != newest
                    || window.checked != checked
                    || window.coverage != 1.0
                {
                    behind.push(format!("node {index} has read {window:?}"));
                    continue;
                }
                let counts = match &recounted {
                    Some(counts) => counts,
                    None => recounted.insert(match bounds {
                        Some((oldest, newest)) => self.recount(index, oldest..=newest).await?,
                        None => HashMap::new(),
                    }),
                };
                let mut expected = counts.clone();
                expected.retain(|_, blocks| *blocks >= MIN_GATEWAY_BLOCKS);
                // A template node is listed with the blocks that name it, if any.
                for template_node in &self.nodes[index].facts.template_nodes {
                    expected.insert(
                        *template_node,
                        counts.get(template_node).copied().unwrap_or(0),
                    );
                }
                expected.remove(&self.gateway_id(index));
                if view.counts() != expected {
                    behind.push(format!(
                        "node {index} counts {:?}, the blocks name {expected:?}",
                        view.counts()
                    ));
                }
            }

            if behind.is_empty() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(eyre!(
                    "6: the gateway counts did not match the blocks at {tip:?} in {bounds:?} \
                     within {MESH_TIMEOUT:?}:\n{}\n{}",
                    behind.join("\n"),
                    self.describe_failure()
                ));
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    /// Checks the standing invariants on every running node and keeps each
    /// report for [`Self::view`].
    ///
    /// A report reads its peers and its slots one after the other, and the
    /// preferred set changes about once a second, so a check that fails is
    /// read again a few times before it counts as broken.
    async fn check_invariants(&mut self) -> Result<()> {
        let mut broken = Vec::new();
        for _ in 0..CHECK_ATTEMPTS {
            broken.clear();
            let mut views = Vec::new();
            for index in 0..self.nodes.len() {
                if !self.nodes[index].is_running(index)? {
                    continue;
                }
                let node = &self.nodes[index];
                let view = node.mesh_view().await?;
                broken.extend(
                    check_view(&view, &node.facts)
                        .into_iter()
                        .map(|rule| format!("node {index}: {rule}")),
                );
                views.push((index, view));
            }

            if broken.is_empty() {
                for node in &mut self.nodes {
                    node.last_view = None;
                }
                for (index, view) in views {
                    self.nodes[index].last_view = Some(view);
                }
                return Ok(());
            }
            tokio::time::sleep(CHECK_RETRY_INTERVAL).await;
        }

        Err(eyre!(
            "standing invariants broken:\n{}\n{}",
            broken.join("\n"),
            self.describe_failure()
        ))
    }

    /// Drops reports that can be stale after a start or stop.
    fn forget_views(&mut self) {
        for node in &mut self.nodes {
            node.last_view = None;
        }
    }

    /// What a failure report shows: every node's last kept report, then the
    /// last lines each running node logged, since the logs go with the
    /// node's directory when the test ends.
    fn describe_failure(&self) -> String {
        let mut text = self.describe_views();
        for (index, node) in self.nodes.iter().enumerate() {
            if node.process.is_some() {
                text.push_str(&format!("node {index} {}\n", log_tail(&node.log_file())));
            }
        }
        text
    }

    /// The last kept report of every node, with node IDs named by node index.
    fn describe_views(&self) -> String {
        let mut text = String::new();
        for (index, node) in self.nodes.iter().enumerate() {
            let Some(view) = &node.last_view else {
                continue;
            };
            let mut line = format!("node {index}: {}", view.describe());
            // Whole node IDs first, then the gateway IDs they start with.
            for (other, other_node) in self.nodes.iter().enumerate() {
                line = line.replace(
                    &hex::encode(other_node.node_id.as_bytes()),
                    &format!("node {other}"),
                );
            }
            for other in 0..self.nodes.len() {
                line = line.replace(
                    &hex::encode(self.gateway_id(other)),
                    &format!("gateway {other}"),
                );
            }
            text.push_str(&line);
            text.push('\n');
        }
        text
    }
}

/// The gateway named by the block's coinbase, if it names one.
fn named_gateway(block: &Block) -> Option<GatewayId> {
    let coinbase = block.transactions.first()?;
    MinerMeshCommitment::from_coinbase_outputs(coinbase.outputs())
        .map(|marker| *marker.gateway_id().as_bytes())
}

/// A filler's answer to every request: nothing, as a node with no blocks or
/// transactions answers.
///
/// Without a service that answers requests, the filler's own v2 stack closes
/// the connection the first time the child sends it one.
#[cfg(feature = "prometheus")]
fn serve_nothing(
    supervisor: zakura_network::zakura::ZakuraSupervisorHandle,
) -> Arc<dyn zakura_network::zakura::Service> {
    use std::future;
    use tower::{service_fn, BoxError};
    use zakura_network::{self as zn, zakura::LegacyGossipSink};

    Arc::new(LegacyGossipSink::spawn(
        service_fn(|_request: zn::Request| future::ready(Ok::<_, BoxError>(zn::Response::Nil))),
        supervisor,
    ))
}

/// Sets what makes a test config a node of this net with `spec`.
fn configure_node(
    config: &mut ZakuradConfig,
    spec: &NodeSpec,
    v2_addr: SocketAddr,
    identity_dir: PathBuf,
    log_file: PathBuf,
    bootstrap_peers: Vec<String>,
    mesh: &MeshSettings,
) {
    // A restarted node keeps its chain.
    config.state.ephemeral = false;
    config.tracing.log_file = Some(log_file);

    config.network.p2p_stack = P2pStack::Zakura;
    config.network.identity_dir = identity_dir;
    config.network.zakura.listen_addr = Some(v2_addr);
    config.network.zakura.bootstrap_peers = bootstrap_peers;
    config.network.zakura.max_connections_per_ip = MAX_CONNECTIONS_PER_IP;
    if let Some(limit) = spec.max_connections {
        config.network.zakura.max_connections = Some(limit);
    }

    // The test config names a miner address, which every mining role keeps.
    if !spec.role.mines() {
        config.mining.miner_address = None;
    }
    config.mining.miner_mesh = spec.role.runs_miner_mesh();
    config.mining.miner_mesh_gateways = mesh.gateways.clone();
    config.mining.miner_mesh_template_nodes = mesh.template_nodes.clone();
}

/// The gateway IDs a node's miner mesh settings list, resolved from the node
/// indexes its [`NodeSpec`] gives.
struct MeshSettings {
    /// `mining.miner_mesh_gateways`.
    gateways: Vec<miner_mesh::GatewayId>,
    /// `mining.miner_mesh_template_nodes`.
    template_nodes: Vec<miner_mesh::GatewayId>,
}

/// A fixed node key for node `index`, so its node ID is known before it starts.
fn node_key(index: usize) -> Result<NodeSecretKey> {
    // Seed byte 0 is left out, so every key differs from an all-zero key.
    let seed_byte = u8::try_from(index + 1).wrap_err("a net holds at most 254 nodes")?;
    Ok(NodeSecretKey::from_bytes(&[seed_byte; 32]))
}

fn log_file(dir: &Path) -> PathBuf {
    dir.join("zakurad.log")
}

/// The last lines of a node's log, for an error report.
fn log_tail(log_file: &Path) -> String {
    match fs::read_to_string(log_file) {
        Ok(log) => {
            let lines: Vec<&str> = log.lines().collect();
            let tail = &lines[lines.len().saturating_sub(LOG_TAIL_LINES)..];
            format!(
                "last log lines of {}:\n{}",
                log_file.display(),
                tail.join("\n")
            )
        }
        Err(error) => format!("no log at {}: {error}", log_file.display()),
    }
}

/// A free loopback UDP port for a node's v2 endpoint and a free TCP port for
/// its RPC server, each held by an open socket so no other process or node
/// takes it before the node starts.
struct ReservedPorts {
    v2: UdpSocket,
    rpc: TcpListener,
    #[cfg(feature = "prometheus")]
    metrics: TcpListener,
}

impl ReservedPorts {
    fn bind() -> Result<Self> {
        Ok(Self {
            v2: UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))?,
            rpc: TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?,
            #[cfg(feature = "prometheus")]
            metrics: TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?,
        })
    }
}

#[cfg(unix)]
mod tests {
    use super::*;

    /// A node that exits outside the harness must fail convergence, even when
    /// the remaining node has the expected best block.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "starts zakurad children: run with the miner-mesh-regtest nextest profile"]
    async fn an_unexpected_exit_fails_convergence() -> Result<()> {
        let _init_guard = zakura_test::init();
        let mut net =
            RegtestNet::start(vec![NodeSpec::new(Role::Relay), NodeSpec::new(Role::Relay)]).await?;
        net.settle().await?;
        net.rpc(1)
            .json_result_from_call::<String>("stop", "[]")
            .await
            .map_err(|error| eyre::eyre!("stopping child by RPC: {error}"))?;
        tokio::time::timeout(STOP_TIMEOUT, async {
            while net.rpc(1).blockchain_info().await.is_ok() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await?;
        let error = net
            .settle()
            .await
            .expect_err("the harness still expects both children");
        assert!(
            error.to_string().contains("node 1 exited unexpectedly"),
            "{error}"
        );
        Ok(())
    }
}
