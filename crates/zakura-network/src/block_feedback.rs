//! Local rejection feedback for a block body supplied by one peer.

use crate::{PeerSource, Response};
use indexmap::IndexMap;
use std::sync::{Arc, Mutex};
use tokio::time::{Duration, Instant};
use zakura_chain::block;

const MAX_SUPPLIERS: usize = 4096;
const LIFETIME: Duration = Duration::from_secs(16 * 60);

/// A local capability identifying one block hash and its supplying peer.
/// Reject only after verification proves that the supplied body is invalid.
#[derive(Clone, Debug)]
pub struct BlockFeedback {
    suppliers: BlockSuppliers,
    hash: block::Hash,
    source: PeerSource,
}

impl PartialEq for BlockFeedback {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
            && self.source == other.source
            && Arc::ptr_eq(&self.suppliers.0, &other.suppliers.0)
    }
}
impl Eq for BlockFeedback {}

impl BlockFeedback {
    /// Excludes this supplier from subsequent requests for the same hash.
    pub fn reject(&self) {
        let mut entries = self
            .suppliers
            .0
            .lock()
            .expect("supplier registry is not poisoned");
        entries.retain(|_, at| at.elapsed() < LIFETIME);
        let key = (self.hash, self.source.clone());
        if !entries.contains_key(&key) && entries.len() >= MAX_SUPPLIERS {
            entries.shift_remove_index(0);
        }
        entries.insert(key, Instant::now());
    }

    pub(crate) fn attach(self, response: Response) -> Response {
        match response {
            Response::Blocks(blocks) if blocks.iter().any(|block| block.is_available()) => {
                Response::BlocksWithFeedback {
                    blocks,
                    feedback: self,
                }
            }
            response => response,
        }
    }
}

/// Stores only rejected suppliers. Successful downloads consume no registry capacity.
#[derive(Clone, Default, Debug)]
pub(crate) struct BlockSuppliers(Arc<Mutex<IndexMap<(block::Hash, PeerSource), Instant>>>);

impl BlockSuppliers {
    pub(crate) fn rejected(&self, hash: block::Hash, source: &PeerSource) -> bool {
        self.0
            .lock()
            .expect("supplier registry is not poisoned")
            .get(&(hash, source.clone()))
            .is_some_and(|at| at.elapsed() < LIFETIME)
    }

    pub(crate) fn feedback(&self, hash: block::Hash, source: PeerSource) -> BlockFeedback {
        BlockFeedback {
            suppliers: self.clone(),
            hash,
            source,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test(start_paused = true)]
    async fn rejection_is_shared_scoped_bounded_and_expires() {
        let suppliers = BlockSuppliers::default();
        let source = PeerSource::LegacySocket("127.0.0.1:8233".parse().unwrap());
        let other = PeerSource::LegacySocket("127.0.0.2:8233".parse().unwrap());
        let hash = block::Hash([1; 32]);
        let feedback = suppliers.feedback(hash, source.clone());
        assert!(suppliers.0.lock().unwrap().is_empty());
        feedback.clone().reject();
        assert!(suppliers.rejected(hash, &source));
        assert!(!suppliers.rejected(hash, &other));
        assert!(!suppliers.rejected(block::Hash([2; 32]), &source));
        tokio::time::advance(LIFETIME).await;
        assert!(!suppliers.rejected(hash, &source));
        for i in 0..MAX_SUPPLIERS + 10 {
            let mut bytes = [0; 32];
            bytes[..8].copy_from_slice(&u64::try_from(i).unwrap().to_le_bytes());
            suppliers
                .feedback(block::Hash(bytes), source.clone())
                .reject();
        }
        assert_eq!(suppliers.0.lock().unwrap().len(), MAX_SUPPLIERS);
    }
}
