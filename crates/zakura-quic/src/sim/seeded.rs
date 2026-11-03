//! [`SeededInputs`]: every [`EndpointInputs`] source and the node key, drawn
//! from one 32-byte scenario seed.
//!
//! Each source reads its own stream. Block `i` of a stream is
//! `SHA-256(seed ‖ node ‖ label ‖ i)`, with `node` and `i` big-endian and
//! `label` the source's fixed name, such as `quic/tls_random`. A draw takes
//! whole blocks, so adding draws to one source never shifts another.
//!
//! None of this is secret: anyone with the seed recomputes every key, which
//! is what lets a run replay.

use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex, MutexGuard, PoisonError, Weak},
    time::Duration,
};

use crate::{inputs::ProviderWatch, EndpointInputs, NodeSecretKey, KEY_LENGTH};
use curve25519_dalek::montgomery::MontgomeryPoint;
use quinn_proto::{ConnectionId, ConnectionIdGenerator, InvalidCid};
use ring::{digest, hkdf, hmac};
use rustls::{
    crypto::{
        ActiveKeyExchange, CryptoProvider, GetRandomFailed, SecureRandom, SharedSecret,
        SupportedKxGroup,
    },
    NamedGroup, PeerMisbehaved,
};

#[cfg(test)]
mod tests;

/// The node key's stream label. [`SeededInputs::node_key`] reads block 0.
const NODE_KEY_LABEL: &str = "quic/node_key";

/// Bytes in one stream block, the SHA-256 output length.
const BLOCK_LEN: usize = 32;

/// The connection ID nonce length, as quinn-proto's
/// `HashedConnectionIdGenerator` uses.
const CID_NONCE_LEN: usize = 3;

/// The connection ID length, as quinn-proto's `HashedConnectionIdGenerator`
/// uses. The bytes after the nonce authenticate it.
const CID_LEN: usize = 8;

/// The initial destination connection ID length. It matches the 20 bytes a
/// dial draws without an injected provider.
const INITIAL_DST_CID_LEN: usize = 20;

/// One randomness source of [`EndpointInputs`], with its own stream.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Source {
    /// The endpoint RNG seed, drawn once.
    RngSeed,
    /// The stateless-reset HMAC key, drawn once.
    ResetKey,
    /// Connection IDs: one key per generator, then one nonce per ID.
    Cid,
    /// Each dial's initial destination connection ID.
    InitialDstCid,
    /// The Retry token master key, drawn once.
    TokenKey,
    /// TLS handshake randoms.
    TlsRandom,
    /// X25519 ephemeral secrets, one per key exchange.
    KeyExchange,
}

impl Source {
    /// Every source, in stream index order.
    pub const ALL: [Source; 7] = [
        Source::RngSeed,
        Source::ResetKey,
        Source::Cid,
        Source::InitialDstCid,
        Source::TokenKey,
        Source::TlsRandom,
        Source::KeyExchange,
    ];

    /// The fixed label that names this source's stream.
    pub const fn label(self) -> &'static str {
        match self {
            Source::RngSeed => "quic/rng_seed",
            Source::ResetKey => "quic/reset_key",
            Source::Cid => "quic/cid",
            Source::InitialDstCid => "quic/initial_dst_cid",
            Source::TokenKey => "quic/token_key",
            Source::TlsRandom => "quic/tls_random",
            Source::KeyExchange => "quic/kx_x25519",
        }
    }

    /// This source's position in [`Source::ALL`].
    const fn index(self) -> usize {
        // A fieldless enum's discriminant is its declaration index, which
        // `ALL` follows, so it's below `ALL.len()`.
        self as usize
    }
}

/// What a scenario saves to replay a node's inputs: the seed, the node
/// index and how many draws each source has made so far.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InputRecord {
    /// The scenario seed.
    pub seed: [u8; 32],
    /// The node index.
    pub node: u32,
    /// Draws per source.
    pub draws: BTreeMap<Source, u64>,
}

/// Block `block` of the stream `label` for `node` under `seed`.
fn stream_block(seed: &[u8; 32], node: u32, label: &str, block: u64) -> [u8; BLOCK_LEN] {
    let mut ctx = digest::Context::new(&digest::SHA256);
    ctx.update(seed);
    ctx.update(&node.to_be_bytes());
    ctx.update(label.as_bytes());
    ctx.update(&block.to_be_bytes());
    ctx.finish()
        .as_ref()
        .try_into()
        .expect("SHA-256 output is 32 bytes")
}

/// One source's position in its stream.
#[derive(Clone, Copy, Debug, Default)]
struct Cursor {
    /// The next unread block.
    block: u64,
    /// Draws so far.
    draws: u64,
}

/// One node's streams, shared by its [`SeededInputs`], their endpoint
/// inputs and its TLS slot.
struct Streams {
    seed: [u8; 32],
    node: u32,
    cursors: Mutex<[Cursor; Source::ALL.len()]>,
    slot: &'static TlsSlot,
}

impl Streams {
    /// Locks the cursors. A panic elsewhere leaves them consistent, because
    /// each draw updates them under one lock.
    fn cursors(&self) -> MutexGuard<'_, [Cursor; Source::ALL.len()]> {
        self.cursors.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Fills `out` with the next whole blocks of `source`'s stream, as one
    /// draw.
    fn draw(&self, source: Source, out: &mut [u8]) {
        let mut cursors = self.cursors();
        let cursor = &mut cursors[source.index()];
        cursor.draws += 1;
        for chunk in out.chunks_mut(BLOCK_LEN) {
            let block = stream_block(&self.seed, self.node, source.label(), cursor.block);
            cursor.block += 1;
            chunk.copy_from_slice(&block[..chunk.len()]);
        }
    }

    /// `N` bytes from one draw of `source`.
    fn draw_array<const N: usize>(&self, source: Source) -> [u8; N] {
        let mut out = [0; N];
        self.draw(source, &mut out);
        out
    }
}

/// Seeded inputs for one node of a scenario.
///
/// [`SeededInputs::endpoint_inputs`] sets every input `bind_with` takes, and
/// [`SeededInputs::node_key`] gives the node key. The same seed and node give
/// the same values, and nothing reads the OS random source. Clones share
/// their streams.
#[derive(Clone)]
pub struct SeededInputs {
    streams: Arc<Streams>,
    lease: Arc<Lease>,
    rng_seed: [u8; 32],
    reset_key: hmac::Key,
    token_key: hkdf::Prk,
}

impl fmt::Debug for SeededInputs {
    /// Shows the seed and node. Keys don't print.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SeededInputs")
            .field("seed", &hex::encode(self.streams.seed))
            .field("node", &self.streams.node)
            .finish_non_exhaustive()
    }
}

impl SeededInputs {
    /// Starts node `node`'s streams under `seed`, every one at block 0.
    ///
    /// rustls takes the TLS random source and key-exchange group as
    /// `&'static`, so the node takes a leaked slot for them, reusing a free
    /// one when it can. A slot stays the node's while these inputs, their
    /// endpoint inputs or any TLS config built from them lives, so a process
    /// leaks at most one slot per node alive at once.
    pub fn new(seed: [u8; 32], node: u32) -> Self {
        Self::with_pool(seed, node, &TLS_SLOTS)
    }

    /// [`SeededInputs::new`], taking its TLS slot from `pool`.
    fn with_pool(seed: [u8; 32], node: u32, pool: &SlotPool) -> Self {
        let lease = pool.lease();
        let streams = Arc::new(Streams {
            seed,
            node,
            cursors: Mutex::default(),
            slot: lease.slot,
        });
        lease.slot.state().streams = Some(streams.clone());
        let rng_seed = streams.draw_array(Source::RngSeed);
        let reset_key = hmac::Key::new(
            hmac::HMAC_SHA256,
            &streams.draw_array::<BLOCK_LEN>(Source::ResetKey),
        );
        // quinn-proto's own token key is HKDF over 64 random bytes.
        let token_key = hkdf::Salt::new(hkdf::HKDF_SHA256, &[])
            .extract(&streams.draw_array::<64>(Source::TokenKey));
        Self {
            streams,
            lease: Arc::new(lease),
            rng_seed,
            reset_key,
            token_key,
        }
    }

    /// Node `node`'s key under `seed`, from its own stream. It's independent
    /// of every [`Source`].
    pub fn node_key(seed: [u8; 32], node: u32) -> NodeSecretKey {
        let bytes: [u8; KEY_LENGTH] = stream_block(&seed, node, NODE_KEY_LABEL, 0);
        NodeSecretKey::from_bytes(&bytes)
    }

    /// Endpoint inputs with every source set from this node's streams.
    /// Each call shares the same streams and keys. They hold the node's TLS
    /// slot even if a caller replaces every other input.
    pub fn endpoint_inputs(&self) -> EndpointInputs {
        self.inputs_without(None)
    }

    /// [`SeededInputs::endpoint_inputs`] with `source` left unset, so that
    /// one source keeps its production, OS-random default. Replay tests use
    /// it to show each source matters.
    pub fn endpoint_inputs_without(&self, source: Source) -> EndpointInputs {
        self.inputs_without(Some(source))
    }

    /// Endpoint inputs with every source but `omitted` set.
    fn inputs_without(&self, omitted: Option<Source>) -> EndpointInputs {
        let set = |source| omitted != Some(source);
        let mut inputs = EndpointInputs::default();
        if set(Source::RngSeed) {
            inputs = inputs.rng_seed(self.rng_seed);
        }
        if set(Source::ResetKey) {
            inputs = inputs.reset_key(Arc::new(self.reset_key.clone()));
        }
        if set(Source::Cid) {
            let cids = self.streams.clone();
            inputs = inputs.cid_generator(Arc::new(move || {
                Box::new(SeededCidGenerator::new(cids.clone())) as Box<dyn ConnectionIdGenerator>
            }));
        }
        if set(Source::InitialDstCid) {
            let initial = self.streams.clone();
            inputs = inputs.initial_dst_cid(Arc::new(move || {
                ConnectionId::new(&initial.draw_array::<INITIAL_DST_CID_LEN>(Source::InitialDstCid))
            }));
        }
        if set(Source::TokenKey) {
            inputs = inputs.token_key(Arc::new(self.token_key.clone()));
        }
        if set(Source::TlsRandom) {
            inputs = inputs.tls_random(self.streams.slot);
        }
        if set(Source::KeyExchange) {
            inputs = inputs.kx_groups(vec![self.streams.slot as &'static dyn SupportedKxGroup]);
        }
        inputs.provider_watch(self.lease.clone())
    }

    /// The seed, node and draw counts so far.
    pub fn record(&self) -> InputRecord {
        let cursors = *self.streams.cursors();
        InputRecord {
            seed: self.streams.seed,
            node: self.streams.node,
            draws: Source::ALL
                .into_iter()
                .map(|source| (source, cursors[source.index()].draws))
                .collect(),
        }
    }
}

/// An 8-byte connection ID generator in quinn-proto's
/// `HashedConnectionIdGenerator` shape: a nonce from the stream, then a keyed
/// hash that [`ConnectionIdGenerator::validate`] checks. IDs don't expire.
struct SeededCidGenerator {
    streams: Arc<Streams>,
    key: [u8; BLOCK_LEN],
}

impl SeededCidGenerator {
    /// A generator whose key is the next draw of the CID stream.
    fn new(streams: Arc<Streams>) -> Self {
        let key = streams.draw_array(Source::Cid);
        Self { streams, key }
    }

    /// The tag that follows `nonce` in an ID this generator made.
    fn tag(&self, nonce: &[u8]) -> [u8; CID_LEN - CID_NONCE_LEN] {
        let mut ctx = digest::Context::new(&digest::SHA256);
        ctx.update(&self.key);
        ctx.update(nonce);
        ctx.finish().as_ref()[..CID_LEN - CID_NONCE_LEN]
            .try_into()
            .expect("SHA-256 output is longer than the tag")
    }
}

impl ConnectionIdGenerator for SeededCidGenerator {
    /// Draws a nonce from the CID stream and appends its tag.
    fn generate_cid(&mut self) -> ConnectionId {
        let mut cid = [0; CID_LEN];
        self.streams.draw(Source::Cid, &mut cid[..CID_NONCE_LEN]);
        let tag = self.tag(&cid[..CID_NONCE_LEN]);
        cid[CID_NONCE_LEN..].copy_from_slice(&tag);
        ConnectionId::new(&cid)
    }

    /// Accepts only IDs whose tag matches their nonce under this key.
    fn validate(&self, cid: &ConnectionId) -> Result<(), InvalidCid> {
        if cid.len() != CID_LEN {
            return Err(InvalidCid);
        }
        let (nonce, tag) = cid.split_at(CID_NONCE_LEN);
        if self.tag(nonce)[..] == *tag {
            Ok(())
        } else {
            Err(InvalidCid)
        }
    }

    /// Always 8 bytes.
    fn cid_len(&self) -> usize {
        CID_LEN
    }

    /// IDs never expire, as with quinn-proto's default.
    fn cid_lifetime(&self) -> Option<Duration> {
        None
    }
}

/// The TLS random source and X25519 group of one node.
///
/// rustls holds both as `&'static`, so each slot is leaked once and reused.
/// A node takes a slot when it starts and keeps it while its
/// [`SeededInputs`], any [`EndpointInputs`] made from them, or any TLS
/// provider built on the slot is alive. Every rustls config and session
/// holds its provider, so a live provider stands for every TLS user. Until
/// then no other node gets the slot, and a draw always reads the node's own
/// stream.
struct TlsSlot {
    state: Mutex<SlotState>,
}

/// Who holds a slot.
struct SlotState {
    /// The node's streams. They stay until another node takes the slot, so
    /// a late draw still reads them.
    streams: Option<Arc<Streams>>,
    /// Whether a [`Lease`] is alive.
    leased: bool,
    /// The TLS providers built on the slot.
    users: Vec<Weak<CryptoProvider>>,
}

impl SlotState {
    /// Whether another node may take the slot.
    fn is_free(&self) -> bool {
        !self.leased && self.users.iter().all(|user| user.strong_count() == 0)
    }
}

impl TlsSlot {
    /// Locks the slot's state.
    fn state(&self) -> MutexGuard<'_, SlotState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The streams the slot draws from, once a node has taken it.
    fn streams(&self) -> Option<Arc<Streams>> {
        self.state().streams.clone()
    }
}

impl fmt::Debug for TlsSlot {
    /// Names the slot. Its streams don't print.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsSlot").finish_non_exhaustive()
    }
}

impl SecureRandom for TlsSlot {
    /// Fills `buf` with one draw of the TLS random stream.
    fn fill(&self, buf: &mut [u8]) -> Result<(), GetRandomFailed> {
        self.streams()
            .ok_or(GetRandomFailed)?
            .draw(Source::TlsRandom, buf);
        Ok(())
    }
}

impl SupportedKxGroup for TlsSlot {
    /// Starts an X25519 exchange whose secret is one draw of the key
    /// exchange stream.
    fn start(&self) -> Result<Box<dyn ActiveKeyExchange>, rustls::Error> {
        let streams = self
            .streams()
            .ok_or_else(|| rustls::Error::General("the TLS slot has no node yet".into()))?;
        let secret = streams.draw_array(Source::KeyExchange);
        let public = MontgomeryPoint::mul_base_clamped(secret).to_bytes();
        Ok(Box::new(X25519Exchange { secret, public }))
    }

    /// X25519, the group ring offers first.
    fn name(&self) -> NamedGroup {
        NamedGroup::X25519
    }
}

/// One X25519 exchange in progress (RFC 7748).
struct X25519Exchange {
    secret: [u8; 32],
    public: [u8; 32],
}

impl ActiveKeyExchange for X25519Exchange {
    /// The shared secret with `peer_pub_key`. A key share of the wrong
    /// length, or one that gives the all-zero secret, is refused, as RFC 8446
    /// §7.4.2 requires and ring does.
    fn complete(self: Box<Self>, peer_pub_key: &[u8]) -> Result<SharedSecret, rustls::Error> {
        let peer: [u8; 32] = peer_pub_key
            .try_into()
            .map_err(|_| PeerMisbehaved::InvalidKeyShare)?;
        let shared = MontgomeryPoint(peer).mul_clamped(self.secret).to_bytes();
        if shared.iter().all(|byte| *byte == 0) {
            return Err(PeerMisbehaved::InvalidKeyShare.into());
        }
        Ok(SharedSecret::from(&shared[..]))
    }

    /// This side's key share.
    fn pub_key(&self) -> &[u8] {
        &self.public
    }

    /// X25519.
    fn group(&self) -> NamedGroup {
        NamedGroup::X25519
    }
}

/// A node's hold on its slot, kept by its [`SeededInputs`] and the
/// [`EndpointInputs`] made from them. It also records the TLS providers
/// built on the slot.
struct Lease {
    slot: &'static TlsSlot,
}

impl Drop for Lease {
    /// Lets the slot go once its TLS providers are gone too.
    fn drop(&mut self) {
        self.slot.state().leased = false;
    }
}

impl ProviderWatch for Lease {
    /// Keeps the slot from another node while `provider` lives.
    fn built(&self, provider: &Arc<CryptoProvider>) {
        let mut state = self.slot.state();
        state.users.retain(|user| user.strong_count() > 0);
        state.users.push(Arc::downgrade(provider));
    }
}

/// Every TLS slot leaked so far.
struct SlotPool {
    slots: Mutex<Vec<&'static TlsSlot>>,
}

/// The process's TLS slots.
static TLS_SLOTS: SlotPool = SlotPool::new();

impl SlotPool {
    /// An empty pool.
    const fn new() -> Self {
        Self {
            slots: Mutex::new(Vec::new()),
        }
    }

    /// Locks the slot list.
    fn slots(&self) -> MutexGuard<'_, Vec<&'static TlsSlot>> {
        self.slots.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Takes a free slot, or leaks a new one if none is free. The caller
    /// gives it streams.
    fn lease(&self) -> Lease {
        let mut slots = self.slots();
        let free = slots.iter().copied().find(|slot| {
            let mut state = slot.state();
            if !state.is_free() {
                return false;
            }
            *state = SlotState {
                streams: None,
                leased: true,
                users: Vec::new(),
            };
            true
        });
        let slot = free.unwrap_or_else(|| {
            let slot: &'static TlsSlot = Box::leak(Box::new(TlsSlot {
                state: Mutex::new(SlotState {
                    streams: None,
                    leased: true,
                    users: Vec::new(),
                }),
            }));
            slots.push(slot);
            slot
        });
        Lease { slot }
    }

    /// Slots leaked so far.
    #[cfg(test)]
    fn leaked(&self) -> usize {
        self.slots().len()
    }
}
