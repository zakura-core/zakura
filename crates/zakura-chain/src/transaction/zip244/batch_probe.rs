//! Artifact-only deferred complete ZIP244 txid DAG, using established BLAKE2b.
use super::*;
use blake2b_simd::many::{hash_many, HashManyJob};
use std::cell::RefCell;
#[derive(Clone, Copy)]
enum Node {
    Constant([u8; 32]),
    Job(usize),
}
impl Node {
    fn as_bytes(&self) -> &Self {
        self
    }
}
struct Spec {
    personal: [u8; 16],
    bytes: Vec<u8>,
    refs: Vec<(usize, Node)>,
    depth: usize,
}
struct Graph {
    specs: RefCell<Vec<Spec>>,
}
struct State<'a> {
    graph: &'a Graph,
    personal: [u8; 16],
    bytes: Vec<u8>,
    refs: Vec<(usize, Node)>,
}
trait Input {
    fn append(&self, state: &mut State<'_>);
}
impl Input for [u8] {
    fn append(&self, state: &mut State<'_>) {
        state.bytes.extend_from_slice(self)
    }
}
impl<const N: usize> Input for [u8; N] {
    fn append(&self, state: &mut State<'_>) {
        self.as_slice().append(state)
    }
}
impl Input for Node {
    fn append(&self, state: &mut State<'_>) {
        match self {
            Node::Constant(b) => b.append(state),
            Node::Job(_) => {
                let offset = state.bytes.len();
                state.bytes.resize(offset + 32, 0);
                state.refs.push((offset, *self));
            }
        }
    }
}
impl State<'_> {
    fn update<T: Input + ?Sized>(&mut self, input: &T) -> &mut Self {
        input.append(self);
        self
    }
    fn finalize(self) -> Node {
        let mut specs = self.graph.specs.borrow_mut();
        let depth = self
            .refs
            .iter()
            .map(|(_, n)| match n {
                Node::Job(i) => specs[*i].depth + 1,
                Node::Constant(_) => 0,
            })
            .max()
            .unwrap_or(0);
        let id = specs.len();
        specs.push(Spec {
            personal: self.personal,
            bytes: self.bytes,
            refs: self.refs,
            depth,
        });
        Node::Job(id)
    }
}
impl Graph {
    fn hasher(&self, personal: &[u8; 16]) -> State<'_> {
        State {
            graph: self,
            personal: *personal,
            bytes: Vec::with_capacity(256),
            refs: Vec::with_capacity(4),
        }
    }
    fn evaluate(self, roots: &[Node], simd: bool) -> Vec<Hash> {
        let mut specs = self.specs.into_inner();
        let maxdepth = specs.iter().map(|s| s.depth).max().unwrap_or(0);
        let mut results = vec![[0u8; 32]; specs.len()];
        for depth in 0..=maxdepth {
            let ids: Vec<_> = specs
                .iter()
                .enumerate()
                .filter_map(|(i, s)| (s.depth == depth).then_some(i))
                .collect();
            for &i in &ids {
                let spec = &mut specs[i];
                for &(offset, node) in &spec.refs {
                    let value = match node {
                        Node::Constant(v) => v,
                        Node::Job(j) => results[j],
                    };
                    spec.bytes[offset..offset + 32].copy_from_slice(&value);
                }
            }
            let params: Vec<_> = ids
                .iter()
                .map(|&i| {
                    let mut p = Params::new();
                    p.hash_length(32).personal(&specs[i].personal);
                    p
                })
                .collect();
            if simd {
                let mut jobs: Vec<_> = ids
                    .iter()
                    .zip(&params)
                    .map(|(&i, p)| HashManyJob::new(p, &specs[i].bytes))
                    .collect();
                hash_many(&mut jobs);
                for (&i, job) in ids.iter().zip(jobs) {
                    results[i].copy_from_slice(job.to_hash().as_bytes());
                }
            } else {
                for (&i, p) in ids.iter().zip(&params) {
                    results[i].copy_from_slice(p.hash(&specs[i].bytes).as_bytes());
                }
            }
        }
        roots
            .iter()
            .map(|n| {
                Hash(match n {
                    Node::Constant(v) => *v,
                    Node::Job(i) => results[*i],
                })
            })
            .collect()
    }
}
fn finalize_node_hash(state: State<'_>) -> Node {
    state.finalize()
}
fn update_serialized<T: ZcashSerialize>(state: &mut State<'_>, value: &T) {
    value
        .zcash_serialize(&mut state.bytes)
        .expect("serialization into a byte vector cannot fail");
}
pub(crate) fn txids(txs: &[&Transaction], simd: bool) -> Vec<Hash> {
    let graph = Graph {
        specs: RefCell::new(Vec::with_capacity(txs.len() * 12)),
    };
    let roots: Vec<_> = txs
        .iter()
        .map(|tx| txid_inner(&graph, &zip244_parts(tx).expect("probe only accepts V5/V6")))
        .collect();
    graph.evaluate(&roots, simd)
}
fn hash_header(graph: &Graph, parts: &Zip244Parts) -> Node {
    let mut h = graph.hasher(ZCASH_HEADERS_HASH_PERSONALIZATION);
    h.update(&parts.version.header().to_le_bytes());
    h.update(&parts.version.version_group_id().to_le_bytes());
    h.update(&consensus_branch_id(parts).to_le_bytes());
    // lock_time and expiry_height are each a single LE u32.
    h.update(&parts.lock_time.to_le_bytes());
    h.update(&parts.expiry_height.0.to_le_bytes());
    h.finalize()
}

/// ZIP-244 §T.2a prevouts digest.
///
/// Reference implementation:
/// <https://github.com/zcash/librustzcash/blob/4367ba26ed57624544e2350f055a5df89079474a/zcash_primitives/src/transaction/txid.rs#L85>
fn hash_prevouts(graph: &Graph, inputs: &[transparent::Input]) -> Node {
    let mut h = graph.hasher(ZCASH_PREVOUTS_HASH_PERSONALIZATION);
    for input in inputs {
        match input {
            transparent::Input::PrevOut { outpoint, .. } => update_serialized(&mut h, outpoint),
            // A coinbase input commits to the null prevout, exactly as Zebra's
            // `Input` serialization writes it.
            transparent::Input::Coinbase { .. } => {
                h.update(&[0u8; 32]);
                h.update(&0xffff_ffff_u32.to_le_bytes());
            }
        }
    }
    h.finalize()
}

/// ZIP-244 §T.2b sequence digest.
///
/// Reference implementation:
/// <https://github.com/zcash/librustzcash/blob/4367ba26ed57624544e2350f055a5df89079474a/zcash_primitives/src/transaction/txid.rs#L71>
fn hash_sequence(graph: &Graph, inputs: &[transparent::Input]) -> Node {
    let mut h = graph.hasher(ZCASH_SEQUENCE_HASH_PERSONALIZATION);
    for input in inputs {
        h.update(&input.sequence().to_le_bytes());
    }
    h.finalize()
}

/// ZIP-244 §T.2c outputs digest.
///
/// Reference implementation:
/// <https://github.com/zcash/librustzcash/blob/4367ba26ed57624544e2350f055a5df89079474a/zcash_primitives/src/transaction/txid.rs#L85>
fn hash_outputs(graph: &Graph, outputs: &[transparent::Output]) -> Node {
    let mut h = graph.hasher(ZCASH_OUTPUTS_HASH_PERSONALIZATION);
    for output in outputs {
        update_serialized(&mut h, output);
    }
    h.finalize()
}

fn hash_transparent_txid_from_digests(
    graph: &Graph,
    bundle_present: bool,
    prevouts: &Node,
    sequence: &Node,
    outputs: &Node,
) -> Node {
    if !bundle_present {
        return Node::Constant(*EMPTY_TRANSPARENT_TXID_HASH);
    }

    let mut h = graph.hasher(ZCASH_TRANSPARENT_HASH_PERSONALIZATION);
    h.update(prevouts.as_bytes());
    h.update(sequence.as_bytes());
    h.update(outputs.as_bytes());
    finalize_node_hash(h)
}

/// ZIP-244 §T.2 transparent digest.
///
/// Reference implementation:
/// <https://github.com/zcash/librustzcash/blob/4367ba26ed57624544e2350f055a5df89079474a/zcash_primitives/src/transaction/txid.rs#L196>
fn hash_transparent_txid(
    graph: &Graph,
    inputs: &[transparent::Input],
    outputs: &[transparent::Output],
) -> Node {
    if inputs.is_empty() && outputs.is_empty() {
        return Node::Constant(*EMPTY_TRANSPARENT_TXID_HASH);
    }

    hash_transparent_txid_from_digests(
        graph,
        true,
        &hash_prevouts(graph, inputs),
        &hash_sequence(graph, inputs),
        &hash_outputs(graph, outputs),
    )
}

/// ZIP-244 §T.3a sapling spends digest.
///
/// Reference implementation:
/// <https://github.com/zcash/librustzcash/blob/4367ba26ed57624544e2350f055a5df89079474a/zcash_primitives/src/transaction/txid.rs#L100>
fn hash_sapling_spends(
    graph: &Graph,
    sapling: &sapling::ShieldedData<sapling::SharedAnchor>,
    version: Zip244Version,
) -> Node {
    let mut h = graph.hasher(ZCASH_SAPLING_SPENDS_HASH_PERSONALIZATION);
    if sapling.spends().next().is_some() {
        let mut ch = graph.hasher(ZCASH_SAPLING_SPENDS_COMPACT_HASH_PERSONALIZATION);
        let mut nh = graph.hasher(version.sapling_spends_noncompact_personalization());
        let anchor = version.sapling_spends_txid_includes_anchor().then(|| {
            <[u8; 32]>::from(
                sapling
                    .shared_anchor()
                    .expect("sapling spends share an anchor when present"),
            )
        });
        for spend in sapling.spends() {
            ch.update(&<[u8; 32]>::from(spend.nullifier));

            update_serialized(&mut nh, &spend.cv);
            if let Some(anchor) = &anchor {
                nh.update(anchor);
            }
            nh.update(&<[u8; 32]>::from(spend.rk.clone()));
        }
        h.update(ch.finalize().as_bytes());
        h.update(nh.finalize().as_bytes());
    }
    h.finalize()
}

/// ZIP-244 §T.3b sapling outputs digest.
///
/// Reference implementation:
/// <https://github.com/zcash/librustzcash/blob/4367ba26ed57624544e2350f055a5df89079474a/zcash_primitives/src/transaction/txid.rs#L132>
fn hash_sapling_outputs(
    graph: &Graph,
    sapling: &sapling::ShieldedData<sapling::SharedAnchor>,
) -> Node {
    let mut h = graph.hasher(ZCASH_SAPLING_OUTPUTS_HASH_PERSONALIZATION);
    if sapling.outputs().next().is_some() {
        let mut ch = graph.hasher(ZCASH_SAPLING_OUTPUTS_COMPACT_HASH_PERSONALIZATION);
        let mut mh = graph.hasher(ZCASH_SAPLING_OUTPUTS_MEMOS_HASH_PERSONALIZATION);
        let mut nh = graph.hasher(ZCASH_SAPLING_OUTPUTS_NONCOMPACT_HASH_PERSONALIZATION);
        for output in sapling.outputs() {
            ch.update(&output.cm_u.to_bytes());
            ch.update(&<[u8; 32]>::from(&output.ephemeral_key));
            ch.update(&output.enc_ciphertext.0[..52]);

            mh.update(&output.enc_ciphertext.0[52..564]);

            update_serialized(&mut nh, &output.cv);
            nh.update(&output.enc_ciphertext.0[564..]);
            nh.update(&output.out_ciphertext.0[..]);
        }
        h.update(ch.finalize().as_bytes());
        h.update(mh.finalize().as_bytes());
        h.update(nh.finalize().as_bytes());
    }
    h.finalize()
}

/// ZIP-244 §T.3 sapling digest.
///
/// Reference implementation:
/// <https://github.com/zcash/librustzcash/blob/4367ba26ed57624544e2350f055a5df89079474a/zcash_primitives/src/transaction/txid.rs#L209>
fn hash_sapling_txid(
    graph: &Graph,
    sapling: Option<&sapling::ShieldedData<sapling::SharedAnchor>>,
    version: Zip244Version,
) -> Node {
    let Some(sapling) = sapling else {
        return Node::Constant(*EMPTY_SAPLING_TXID_HASH);
    };

    let mut h = graph.hasher(ZCASH_SAPLING_HASH_PERSONALIZATION);
    // `ShieldedData` only exists with at least one spend or output, so this
    // matches librustzcash's "non-empty bundle" branch.
    if sapling.spends().next().is_some() {
        h.update(hash_sapling_spends(graph, sapling, version).as_bytes());
    } else {
        h.update(EMPTY_SAPLING_SPENDS_HASH);
    }
    if sapling.outputs().next().is_some() {
        h.update(hash_sapling_outputs(graph, sapling).as_bytes());
    } else {
        h.update(EMPTY_SAPLING_OUTPUTS_HASH);
    }
    h.update(&sapling.value_balance.zatoshis().to_le_bytes());
    finalize_node_hash(h)
}

/// ZIP-244 §T.4 Orchard-style bundle digest.
///
/// Mirrors `orchard::bundle::commitments::hash_bundle_txid_data`.
fn hash_bundle_txid(
    graph: &Graph,
    bundle: Option<&orchard::ShieldedData>,
    format: BundleCommitmentFormat,
) -> Node {
    let Some(bundle) = bundle else {
        return Node::Constant(*format.empty_txid_hash());
    };

    let personalizations = format.personalizations();
    let mut h = graph.hasher(personalizations.bundle);
    let mut ch = graph.hasher(personalizations.actions_compact);
    let mut mh = graph.hasher(personalizations.actions_memos);
    let mut nh = graph.hasher(personalizations.actions_noncompact);
    for action in bundle.actions() {
        ch.update(&<[u8; 32]>::from(action.nullifier));
        ch.update(&<[u8; 32]>::from(action.cm_x));
        update_serialized(&mut ch, &action.ephemeral_key);
        ch.update(&action.enc_ciphertext.0[..52]);

        mh.update(&action.enc_ciphertext.0[52..564]);

        update_serialized(&mut nh, &action.cv);
        nh.update(&<[u8; 32]>::from(action.rk));
        nh.update(&action.enc_ciphertext.0[564..]);
        nh.update(&action.out_ciphertext.0[..]);
    }
    h.update(ch.finalize().as_bytes());
    h.update(mh.finalize().as_bytes());
    h.update(nh.finalize().as_bytes());
    h.update(&[bundle.flags.bits()]);
    h.update(&bundle.value_balance.zatoshis().to_le_bytes());
    if format.includes_anchor_in_txid_digest() {
        h.update(&<[u8; 32]>::from(bundle.shared_anchor));
    }
    finalize_node_hash(h)
}

/// Combine the level-1 digests into the txid (ZIP-244 txid digest).
///
/// Reference implementation:
/// <https://github.com/zcash/librustzcash/blob/4367ba26ed57624544e2350f055a5df89079474a/zcash_primitives/src/transaction/txid.rs#L426>
fn combine_txid_digests(
    graph: &Graph,
    consensus_branch_id: u32,
    header: &Node,
    transparent: &Node,
    sapling: &Node,
    orchard: &Node,
    ironwood: Option<&Node>,
) -> Node {
    let mut personal = [0u8; 16];
    personal[..12].copy_from_slice(ZCASH_TX_PERSONALIZATION_PREFIX);
    personal[12..].copy_from_slice(&consensus_branch_id.to_le_bytes());

    // Commit the level-1 nodes in their ZIP-244 order.
    let mut h = graph.hasher(&personal);
    h.update(header.as_bytes());
    h.update(transparent);
    h.update(sapling);
    h.update(orchard);
    if let Some(ironwood) = ironwood {
        h.update(ironwood);
    }

    h.finalize()
}

fn txid_inner(graph: &Graph, parts: &Zip244Parts) -> Node {
    let header = hash_header(graph, parts);
    let transparent = hash_transparent_txid(graph, parts.inputs, parts.outputs);
    let sapling = hash_sapling_txid(graph, parts.sapling, parts.version);
    let orchard = hash_bundle_txid(graph, parts.orchard, parts.version.orchard_format());
    let ironwood = parts
        .version
        .has_ironwood()
        .then(|| hash_bundle_txid(graph, parts.ironwood, BundleCommitmentFormat::IronwoodV6));
    combine_txid_digests(
        graph,
        consensus_branch_id(parts),
        &header,
        &transparent,
        &sapling,
        &orchard,
        ironwood.as_ref(),
    )
}
