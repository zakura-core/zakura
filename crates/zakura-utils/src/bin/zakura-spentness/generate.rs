//! Merge canonical output order with the exact-H UTXO set into membership bits.

use std::path::{Path, PathBuf};

use color_eyre::eyre::{bail, ensure, Result};
use zakura_chain::{
    block::{self, Block, Height},
    parameters::spentness_hints::{Commitment, Encoder, ParsedArtifact},
    transparent::{self, OrderedUtxo},
};
use zakura_state::{OutputLocation, ZakuraDb};

use super::{canonical_block, exact_boundary, open, write};

/// An untrusted artifact and the descriptor a reviewer checks before pinning it.
pub(super) struct Generated {
    pub(super) bytes: Vec<u8>,
    pub(super) commitment: Commitment,
    pub(super) survivors: u64,
}

/// Generate membership for an archive whose tip is exactly H/hash.
///
/// Every UTXO must match exactly one canonical output, with an identical entry.
/// Advancing H regenerates every bit, including bits for older outputs.
pub(super) fn generate(db: &ZakuraDb, height: u32, hash: block::Hash) -> Result<Generated> {
    let genesis = exact_boundary(db, height, hash)?;
    let mut encoder = Encoder::new(genesis, height, hash.0);
    let mut utxos = db.utxos_by_location().peekable();
    let mut survivors = 0;

    for h in 0..=height {
        let block = canonical_block(db, Height(h))?;
        for (location, output) in outputs_in_order(&block, Height(h)) {
            let retained = match utxos.next_if(|(next, _)| *next <= location) {
                None => false,
                Some((next, _)) if next != location => bail!("unmatched UTXO at {next:?}"),
                Some((_, entry)) => {
                    ensure!(h != 0, "genesis outputs must not enter the UTXO set");
                    let tx_index = location.transaction_index().as_usize();
                    ensure!(
                        entry == OrderedUtxo::new(output.clone(), Height(h), tx_index),
                        "UTXO entry at {location:?} differs from its creating transaction"
                    );
                    survivors += 1;
                    true
                }
            };
            encoder.push(retained)?;
        }
    }
    ensure!(
        utxos.next().is_none(),
        "UTXO set contains unmatched entries beyond canonical outputs"
    );

    let bytes = encoder.finish();
    let commitment = ParsedArtifact::read(bytes.as_slice())?.commitment().clone();
    ensure!(
        exact_boundary(db, height, hash)? == genesis,
        "archive changed during generation"
    );
    Ok(Generated {
        bytes,
        commitment,
        survivors,
    })
}

/// Every transparent output of `block` in canonical artifact order.
fn outputs_in_order(
    block: &Block,
    height: Height,
) -> impl Iterator<Item = (OutputLocation, &transparent::Output)> {
    block
        .transactions
        .iter()
        .enumerate()
        .flat_map(move |(tx_index, tx)| {
            tx.outputs()
                .iter()
                .enumerate()
                .map(move |(output_index, output)| {
                    let location = OutputLocation::from_usize(height, tx_index, output_index);
                    (location, output)
                })
        })
}

/// Write the artifact and its commitment, then print a summary for review.
pub(super) fn run(
    state: &Path,
    height: u32,
    block_hash: block::Hash,
    output: PathBuf,
    commitment_path: PathBuf,
) -> Result<()> {
    let Generated {
        bytes,
        commitment,
        survivors,
    } = generate(&open(state)?, height, block_hash)?;
    write(output, &bytes)?;
    write(commitment_path, &serde_json::to_vec_pretty(&commitment)?)?;
    println!(
        "outputs={} survivors={survivors} bytes={} sha256={}",
        commitment.output_count,
        commitment.byte_len,
        commitment.digest_hex()
    );
    Ok(())
}
