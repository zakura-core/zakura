//! Cross-service properties (properties.md, Cross-service sync): claims
//! about header sync and block sync together on one node, over one
//! engine.
//!
//! The checks judge the node's facts against a [`Dag`], the harness's own
//! record of every fixture block it made, never against the node's engine.
//! So a node that confuses two branches can't make its own check agree with
//! it.

use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

use zakura_header_chain::Frontier;
use zakura_storage::block::fixture::FixtureBlock;

use crate::{
    harness::block_node::view::BlockSyncView,
    property::{HasView, Property},
    storage::hash,
};

/// Every fixture block a run made: each one's height and parent, by hash,
/// above the anchor. Hashes are lowercase hex, as observations carry them.
#[derive(Clone, Debug)]
pub struct Dag {
    /// The anchor's hash, every chain's root.
    anchor: String,
    /// Each block's height and parent hash.
    parents: BTreeMap<String, (u32, String)>,
}

impl Dag {
    /// A DAG holding only `anchor`.
    pub fn new(anchor: Frontier) -> Self {
        Dag {
            anchor: hash(anchor.hash),
            parents: BTreeMap::new(),
        }
    }

    /// Adds `blocks`, a chain extending `parent`.
    pub fn extend(&mut self, parent: Frontier, blocks: &[FixtureBlock]) {
        let mut parent = hash(parent.hash);
        for block in blocks {
            let own = hash(block.hash());
            self.parents.insert(own.clone(), (block.height.0, parent));
            parent = own;
        }
    }

    /// The height of the block `hash`, if the run made it.
    pub fn height(&self, hash: &str) -> Option<u32> {
        if hash == self.anchor {
            return Some(0);
        }
        self.parents.get(hash).map(|(height, _)| *height)
    }

    /// The ancestor of the block `tip` at `height`, `tip` itself at its own
    /// height. `None` if the run never made `tip` or `height` is above it.
    pub fn ancestor(&self, tip: &str, height: u32) -> Option<&str> {
        let mut at = self.parents.get_key_value(tip).map(|(key, _)| key.as_str());
        if at.is_none() && tip == self.anchor {
            at = Some(self.anchor.as_str());
        }
        let mut at = at?;
        loop {
            let own = self.height(at)?;
            if own == height {
                return Some(at);
            }
            if own < height {
                return None;
            }
            at = self.parents.get(at).map(|(_, parent)| parent.as_str())?;
        }
    }

    /// Whether the chain ending at `tip` holds `ancestor`.
    pub fn descends(&self, tip: Frontier, ancestor: Frontier) -> bool {
        self.ancestor(&hash(tip.hash), ancestor.height.0) == Some(hash(ancestor.hash).as_str())
    }
}

/// What the cross checks read: the DAG.
#[derive(Debug)]
pub struct CrossModel {
    /// Every fixture block the run made.
    pub dag: Dag,
}

impl CrossModel {
    /// A model over `dag`.
    pub fn new(dag: Dag) -> Self {
        CrossModel { dag }
    }
}

/// A view that carries a [`CrossModel`] beside the node's block-sync view.
pub trait CrossView {
    /// The model, shared with every later view.
    fn cross(&self) -> Rc<RefCell<CrossModel>>;

    /// The node's block-sync view.
    fn block(&self) -> Rc<RefCell<BlockSyncView>>;
}

/// The cross properties `sync/storage` registers.
pub fn storage_properties<S>() -> Vec<Property<S>>
where
    S: HasView,
    S::View: CrossView,
{
    Vec::new()
}
