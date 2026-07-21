//! Internal mining in Zebra.
//!
//! # TODO
//! - pause mining if we have no peers, like `zcashd` does,
//!   and add a developer config that mines regardless of how many peers we have.
//!   <https://github.com/zcash/zcash/blob/6fdd9f1b81d3b228326c9826fa10696fc516444b/src/miner.cpp#L865-L880>
//! - move common code into zakura-chain or zakura-node-services and remove the RPC dependency.

use std::{cmp::min, sync::Arc, thread::available_parallelism, time::Duration};

use color_eyre::Report;
use futures::{stream::FuturesUnordered, StreamExt};
use thread_priority::{ThreadBuilder, ThreadPriority};
use tokio::{select, sync::watch, task::JoinHandle, time::sleep};
use tower::Service;
use tracing::{Instrument, Span};

use zakura_chain::{
    block::{self, Block},
    chain_sync_status::ChainSyncStatus,
    chain_tip::ChainTip,
    diagnostic::task::WaitForPanics,
    serialization::{AtLeastOne, ZcashSerialize},
    shutdown::is_shutting_down,
    work::equihash::{Solution, SolverCancelled},
};
use zakura_network::AddressBookPeers;
use zakura_node_services::mempool;
use zakura_rpc::{
    client::{
        BlockTemplateTimeSource,
        GetBlockTemplateCapability::{CoinbaseTxn, LongPoll},
        GetBlockTemplateParameters,
        GetBlockTemplateRequestMode::Template,
        HexData,
    },
    methods::{RpcImpl, RpcServer},
    proposal_block_from_template,
};
use zakura_state::WatchReceiver;

use crate::components::metrics::Config;

/// The amount of time we wait between block template retries.
pub const BLOCK_TEMPLATE_WAIT_TIME: Duration = Duration::from_secs(20);

/// A rate-limit for block template refreshes.
pub const BLOCK_TEMPLATE_REFRESH_LIMIT: Duration = Duration::from_secs(2);

/// How long we wait after mining a block, before expecting a new template.
///
/// This should be slightly longer than `BLOCK_TEMPLATE_REFRESH_LIMIT` to allow for template
/// generation.
pub const BLOCK_MINING_WAIT_TIME: Duration = Duration::from_secs(3);

/// Returns `true` if `new_header` should replace the internal miner's current
/// block template.
fn should_replace_mining_template(
    current_header: Option<block::Header>,
    new_header: block::Header,
    submit_old: Option<bool>,
) -> bool {
    current_header != Some(new_header) && (current_header.is_none() || submit_old != Some(true))
}

/// Cancels mining when the current template changes or becomes unavailable.
fn cancel_if_mining_template_changed(
    template_receiver: &mut WatchReceiver<Option<Arc<Block>>>,
    old_header: block::Header,
) -> Result<(), SolverCancelled> {
    match template_receiver.has_changed() {
        // Guard against `get_block_template()` providing an identical header.
        // This could happen if something irrelevant to the block data changes,
        // the time was within 1 second, or there is a spurious channel change.
        Ok(has_changed) => {
            template_receiver.mark_as_seen();

            // We only need to check header equality, because the block data is
            // bound to the header. An unavailable template has no header, so it
            // also cancels current work.
            if has_changed
                && Some(old_header) != template_receiver.cloned_watch_data().map(|b| *b.header)
            {
                Err(SolverCancelled)
            } else {
                Ok(())
            }
        }
        // If the sender was dropped, we're likely shutting down, so cancel the
        // solver.
        Err(_sender_dropped) => Err(SolverCancelled),
    }
}

/// Initialize the miner based on its config, and spawn a task for it.
///
/// This method is CPU and memory-intensive. It uses 144 MB of RAM and one CPU core per configured
/// mining thread.
///
/// See [`run_mining_solver()`] for more details.
pub fn spawn_init<Mempool, State, ReadState, Tip, AddressBook, BlockVerifierRouter, SyncStatus>(
    config: &Config,
    rpc: RpcImpl<Mempool, State, ReadState, Tip, AddressBook, BlockVerifierRouter, SyncStatus>,
) -> JoinHandle<Result<(), Report>>
// TODO: simplify or avoid repeating these generics (how?)
where
    Mempool: Service<
            mempool::Request,
            Response = mempool::Response,
            Error = zakura_node_services::BoxError,
        > + Clone
        + Send
        + Sync
        + 'static,
    Mempool::Future: Send,
    State: Service<
            zakura_state::Request,
            Response = zakura_state::Response,
            Error = zakura_state::BoxError,
        > + Clone
        + Send
        + Sync
        + 'static,
    <State as Service<zakura_state::Request>>::Future: Send,
    ReadState: Service<
            zakura_state::ReadRequest,
            Response = zakura_state::ReadResponse,
            Error = zakura_state::BoxError,
        > + Clone
        + Send
        + Sync
        + 'static,
    <ReadState as Service<zakura_state::ReadRequest>>::Future: Send,
    Tip: ChainTip + Clone + Send + Sync + 'static,
    BlockVerifierRouter: Service<
            zakura_consensus::Request,
            Response = block::Hash,
            Error = zakura_consensus::BoxError,
        > + Clone
        + Send
        + Sync
        + 'static,
    <BlockVerifierRouter as Service<zakura_consensus::Request>>::Future: Send,
    SyncStatus: ChainSyncStatus + Clone + Send + Sync + 'static,
    AddressBook: AddressBookPeers + Clone + Send + Sync + 'static,
{
    // TODO: spawn an entirely new executor here, so mining is isolated from higher priority tasks.
    tokio::spawn(init(config.clone(), rpc).in_current_span())
}

/// Initialize the miner based on its config.
///
/// This method is CPU and memory-intensive. It uses 144 MB of RAM and one CPU core per configured
/// mining thread.
///
/// See [`run_mining_solver()`] for more details.
pub async fn init<Mempool, State, ReadState, Tip, BlockVerifierRouter, SyncStatus, AddressBook>(
    _config: Config,
    rpc: RpcImpl<Mempool, State, ReadState, Tip, AddressBook, BlockVerifierRouter, SyncStatus>,
) -> Result<(), Report>
where
    Mempool: Service<
            mempool::Request,
            Response = mempool::Response,
            Error = zakura_node_services::BoxError,
        > + Clone
        + Send
        + Sync
        + 'static,
    Mempool::Future: Send,
    State: Service<
            zakura_state::Request,
            Response = zakura_state::Response,
            Error = zakura_state::BoxError,
        > + Clone
        + Send
        + Sync
        + 'static,
    <State as Service<zakura_state::Request>>::Future: Send,
    ReadState: Service<
            zakura_state::ReadRequest,
            Response = zakura_state::ReadResponse,
            Error = zakura_state::BoxError,
        > + Clone
        + Send
        + Sync
        + 'static,
    <ReadState as Service<zakura_state::ReadRequest>>::Future: Send,
    Tip: ChainTip + Clone + Send + Sync + 'static,
    BlockVerifierRouter: Service<
            zakura_consensus::Request,
            Response = block::Hash,
            Error = zakura_consensus::BoxError,
        > + Clone
        + Send
        + Sync
        + 'static,
    <BlockVerifierRouter as Service<zakura_consensus::Request>>::Future: Send,
    SyncStatus: ChainSyncStatus + Clone + Send + Sync + 'static,
    AddressBook: AddressBookPeers + Clone + Send + Sync + 'static,
{
    // TODO: change this to `config.internal_miner_threads` once mining tasks are cancelled when the best tip changes (#8797)
    let configured_threads = 1;
    // If we can't detect the number of cores, use the configured number.
    let available_threads = available_parallelism()
        .map(usize::from)
        .unwrap_or(configured_threads);

    // Use the minimum of the configured and available threads.
    let solver_count = min(configured_threads, available_threads);

    info!(
        ?solver_count,
        "launching mining tasks with parallel solvers"
    );

    let (template_sender, template_receiver) = watch::channel(None);
    let template_receiver = WatchReceiver::new(template_receiver);

    // Spawn these tasks, to avoid blocked cooperative futures, and improve shutdown responsiveness.
    // This is particularly important when there are a large number of solver threads.
    let mut abort_handles = Vec::new();

    let template_generator = tokio::task::spawn(
        generate_block_templates(rpc.clone(), template_sender).in_current_span(),
    );
    abort_handles.push(template_generator.abort_handle());
    let template_generator = template_generator.wait_for_panics();

    let mut mining_solvers = FuturesUnordered::new();
    for solver_id in 0..solver_count {
        // Assume there are less than 256 cores. If there are more, only run 256 tasks.
        let solver_id = min(solver_id, usize::from(u8::MAX))
            .try_into()
            .expect("just limited to u8::MAX");

        let solver = tokio::task::spawn(
            run_mining_solver(solver_id, template_receiver.clone(), rpc.clone()).in_current_span(),
        );
        abort_handles.push(solver.abort_handle());

        mining_solvers.push(solver.wait_for_panics());
    }

    // These tasks run forever unless there is a fatal error or shutdown.
    // When that happens, the first task to error returns, and the other JoinHandle futures are
    // cancelled.
    let first_result;
    select! {
        result = template_generator => { first_result = result; }
        result = mining_solvers.next() => {
            first_result = result
                .expect("stream never terminates because there is at least one solver task");
        }
    }

    // But the spawned async tasks keep running, so we need to abort them here.
    for abort_handle in abort_handles {
        abort_handle.abort();
    }

    // Any spawned blocking threads will keep running. When this task returns and drops the
    // `template_sender`, it cancels all the spawned miner threads. This works because we've
    // aborted the `template_generator` task, which owns the `template_sender`. (And it doesn't
    // spawn any blocking threads.)
    first_result
}

/// Generates block templates using `rpc`, and sends them to mining threads using `template_sender`.
#[instrument(skip(rpc, template_sender))]
pub async fn generate_block_templates<
    Mempool,
    State,
    ReadState,
    Tip,
    BlockVerifierRouter,
    SyncStatus,
    AddressBook,
>(
    rpc: RpcImpl<Mempool, State, ReadState, Tip, AddressBook, BlockVerifierRouter, SyncStatus>,
    template_sender: watch::Sender<Option<Arc<Block>>>,
) -> Result<(), Report>
where
    Mempool: Service<
            mempool::Request,
            Response = mempool::Response,
            Error = zakura_node_services::BoxError,
        > + Clone
        + Send
        + Sync
        + 'static,
    Mempool::Future: Send,
    State: Service<
            zakura_state::Request,
            Response = zakura_state::Response,
            Error = zakura_state::BoxError,
        > + Clone
        + Send
        + Sync
        + 'static,
    <State as Service<zakura_state::Request>>::Future: Send,
    ReadState: Service<
            zakura_state::ReadRequest,
            Response = zakura_state::ReadResponse,
            Error = zakura_state::BoxError,
        > + Clone
        + Send
        + Sync
        + 'static,
    <ReadState as Service<zakura_state::ReadRequest>>::Future: Send,
    Tip: ChainTip + Clone + Send + Sync + 'static,
    BlockVerifierRouter: Service<
            zakura_consensus::Request,
            Response = block::Hash,
            Error = zakura_consensus::BoxError,
        > + Clone
        + Send
        + Sync
        + 'static,
    <BlockVerifierRouter as Service<zakura_consensus::Request>>::Future: Send,
    SyncStatus: ChainSyncStatus + Clone + Send + Sync + 'static,
    AddressBook: AddressBookPeers + Clone + Send + Sync + 'static,
{
    // Request long polling so each update says whether current work remains valid.
    let mut parameters =
        GetBlockTemplateParameters::new(Template, None, vec![LongPoll, CoinbaseTxn], None, None);

    // Shut down the task when all the template receivers are dropped, or Zebra shuts down.
    while !template_sender.is_closed() && !is_shutting_down() {
        let template: Result<_, _> = rpc.get_block_template(Some(parameters.clone())).await;

        // Wait for the chain to sync so we get a valid template.
        let Ok(template) = template else {
            let active_template_invalidated = template_sender.send_if_modified(|template| {
                if template.is_none() {
                    return false;
                }

                *template = None;
                true
            });

            warn!(
                ?BLOCK_TEMPLATE_WAIT_TIME,
                ?template,
                ?active_template_invalidated,
                "waiting for a valid block template",
            );

            // Skip the wait if we got an error because we are shutting down.
            if !is_shutting_down() {
                sleep(BLOCK_TEMPLATE_WAIT_TIME).await;
            }

            continue;
        };

        // Convert from RPC GetBlockTemplate to Block
        let template = template
            .try_into_template()
            .expect("invalid RPC response: proposal in response to a template request");

        let height = template.height();
        let transaction_count = template.transactions().len();
        let submit_old = template.submit_old();

        // Tell the next get_block_template() call to wait until the template has changed.
        parameters = GetBlockTemplateParameters::new(
            Template,
            None,
            vec![LongPoll, CoinbaseTxn],
            Some(template.long_poll_id()),
            None,
        );

        let block = proposal_block_from_template(
            &template,
            BlockTemplateTimeSource::CurTime,
            rpc.network(),
        )?;

        // Only replace the current template when old work is no longer valid.
        // Mempool-only updates set `submit_old` to true, so cancelling for them
        // would discard useful solver work.
        let template_replaced = template_sender.send_if_modified(|old_block| {
            if !should_replace_mining_template(
                old_block.as_ref().map(|block| *block.header),
                *block.header,
                submit_old,
            ) {
                return false;
            }
            *old_block = Some(Arc::new(block));
            true
        });

        if template_replaced {
            info!(
                ?height,
                transactions = ?transaction_count,
                "mining with an updated block template",
            );
        } else {
            debug!(
                ?height,
                transactions = ?transaction_count,
                ?submit_old,
                "keeping valid work across a block template update",
            );
        }

        // If the blockchain is changing rapidly, limit how often we'll update the template.
        // But if we're shutting down, do that immediately.
        if !template_sender.is_closed() && !is_shutting_down() {
            sleep(BLOCK_TEMPLATE_REFRESH_LIMIT).await;
        }
    }

    Ok(())
}

/// Runs a single mining thread that gets blocks from the `template_receiver`, calculates equihash
/// solutions with nonces based on `solver_id`, and submits valid blocks to Zebra's block validator.
///
/// This method is CPU and memory-intensive. It uses 144 MB of RAM and one CPU core while running.
/// It can run for minutes or hours if the network difficulty is high. Mining uses a thread with
/// low CPU priority.
#[instrument(skip(template_receiver, rpc))]
pub async fn run_mining_solver<
    Mempool,
    State,
    ReadState,
    Tip,
    BlockVerifierRouter,
    SyncStatus,
    AddressBook,
>(
    solver_id: u8,
    mut template_receiver: WatchReceiver<Option<Arc<Block>>>,
    rpc: RpcImpl<Mempool, State, ReadState, Tip, AddressBook, BlockVerifierRouter, SyncStatus>,
) -> Result<(), Report>
where
    Mempool: Service<
            mempool::Request,
            Response = mempool::Response,
            Error = zakura_node_services::BoxError,
        > + Clone
        + Send
        + Sync
        + 'static,
    Mempool::Future: Send,
    State: Service<
            zakura_state::Request,
            Response = zakura_state::Response,
            Error = zakura_state::BoxError,
        > + Clone
        + Send
        + Sync
        + 'static,
    <State as Service<zakura_state::Request>>::Future: Send,
    ReadState: Service<
            zakura_state::ReadRequest,
            Response = zakura_state::ReadResponse,
            Error = zakura_state::BoxError,
        > + Clone
        + Send
        + Sync
        + 'static,
    <ReadState as Service<zakura_state::ReadRequest>>::Future: Send,
    Tip: ChainTip + Clone + Send + Sync + 'static,
    BlockVerifierRouter: Service<
            zakura_consensus::Request,
            Response = block::Hash,
            Error = zakura_consensus::BoxError,
        > + Clone
        + Send
        + Sync
        + 'static,
    <BlockVerifierRouter as Service<zakura_consensus::Request>>::Future: Send,
    SyncStatus: ChainSyncStatus + Clone + Send + Sync + 'static,
    AddressBook: AddressBookPeers + Clone + Send + Sync + 'static,
{
    // Shut down the task when the template sender is dropped, or Zebra shuts down.
    while template_receiver.has_changed().is_ok() && !is_shutting_down() {
        // Get the latest block template, and mark the current value as seen.
        // We mark the value first to avoid missed updates.
        template_receiver.mark_as_seen();
        let template = template_receiver.cloned_watch_data();

        let Some(template) = template else {
            if solver_id == 0 {
                info!(
                    ?solver_id,
                    ?BLOCK_TEMPLATE_WAIT_TIME,
                    "solver waiting for initial block template"
                );
            } else {
                debug!(
                    ?solver_id,
                    ?BLOCK_TEMPLATE_WAIT_TIME,
                    "solver waiting for initial block template"
                );
            }

            // Skip the wait if we didn't get a template because we are shutting down.
            if !is_shutting_down() {
                sleep(BLOCK_TEMPLATE_WAIT_TIME).await;
            }

            continue;
        };

        let height = template.coinbase_height().expect("template is valid");

        // Set up the cancellation conditions for the miner.
        let mut cancel_receiver = template_receiver.clone();
        let old_header = *template.header;
        let cancel_fn = move || cancel_if_mining_template_changed(&mut cancel_receiver, old_header);

        // Mine at least one block using the equihash solver.
        let Ok(blocks) = mine_a_block(solver_id, template, cancel_fn).await else {
            // If the solver was cancelled, we're either shutting down, or we have a new template.
            if solver_id == 0 {
                info!(
                    ?height,
                    ?solver_id,
                    new_template = ?template_receiver.has_changed(),
                    shutting_down = ?is_shutting_down(),
                    "solver cancelled: getting a new block template or shutting down"
                );
            } else {
                debug!(
                    ?height,
                    ?solver_id,
                    new_template = ?template_receiver.has_changed(),
                    shutting_down = ?is_shutting_down(),
                    "solver cancelled: getting a new block template or shutting down"
                );
            }

            // If the blockchain is changing rapidly, limit how often we'll update the template.
            // But if we're shutting down, do that immediately.
            if template_receiver.has_changed().is_ok() && !is_shutting_down() {
                sleep(BLOCK_TEMPLATE_REFRESH_LIMIT).await;
            }

            continue;
        };

        // Submit the newly mined blocks to the verifiers.
        //
        // TODO: if there is a new template (`cancel_fn().is_err()`), and
        //       GetBlockTemplate.submit_old is false, return immediately, and skip submitting the
        //       blocks.
        let mut any_success = false;
        for block in blocks {
            let data = block
                .zcash_serialize_to_vec()
                .expect("serializing to Vec never fails");

            match rpc.submit_block(HexData(data), None).await {
                Ok(success) => {
                    info!(
                        ?height,
                        hash = ?block.hash(),
                        ?solver_id,
                        ?success,
                        "successfully mined a new block",
                    );
                    any_success = true;
                }
                Err(error) => info!(
                    ?height,
                    hash = ?block.hash(),
                    ?solver_id,
                    ?error,
                    "validating a newly mined block failed, trying again",
                ),
            }
        }

        // Start re-mining quickly after a failed solution.
        // If there's a new template, we'll use it, otherwise the existing one is ok.
        if !any_success {
            // If the blockchain is changing rapidly, limit how often we'll update the template.
            // But if we're shutting down, do that immediately.
            if template_receiver.has_changed().is_ok() && !is_shutting_down() {
                sleep(BLOCK_TEMPLATE_REFRESH_LIMIT).await;
            }
            continue;
        }

        // Wait for the new block to verify, and the RPC task to pick up a new template.
        // But don't wait too long, we could have mined on a fork.
        tokio::select! {
            shutdown_result = template_receiver.changed() => shutdown_result?,
            _ = sleep(BLOCK_MINING_WAIT_TIME) => {}

        }
    }

    Ok(())
}

/// Mines one or more blocks based on `template`. Calculates equihash solutions, checks difficulty,
/// and returns as soon as it has at least one block. Uses a different nonce range for each
/// `solver_id`.
///
/// If `cancel_fn()` returns an error, returns early with `Err(SolverCancelled)`.
///
/// See [`run_mining_solver()`] for more details.
pub async fn mine_a_block<F>(
    solver_id: u8,
    template: Arc<Block>,
    cancel_fn: F,
) -> Result<AtLeastOne<Block>, SolverCancelled>
where
    F: FnMut() -> Result<(), SolverCancelled> + Send + Sync + 'static,
{
    // TODO: Replace with Arc::unwrap_or_clone() when it stabilises:
    // https://github.com/rust-lang/rust/issues/93610
    let mut header = *template.header;

    // Use a different nonce for each solver thread.
    // Change both the first and last bytes, so we don't have to care if the nonces are incremented in
    // big-endian or little-endian order. And we can see the thread that mined a block from the nonce.
    *header.nonce.first_mut().unwrap() = solver_id;
    *header.nonce.last_mut().unwrap() = solver_id;

    // Mine one or more blocks using the solver, in a low-priority blocking thread.
    let span = Span::current();
    let solved_headers =
        tokio::task::spawn_blocking(move || span.in_scope(move || {
            let miner_thread_handle = ThreadBuilder::default().name("zakura-miner").priority(ThreadPriority::Min).spawn(move |priority_result| {
                if let Err(error) = priority_result {
                    info!(?error, "could not set miner to run at a low priority: running at default priority");
                }

                Solution::solve(header, cancel_fn)
            }).expect("unable to spawn miner thread");

            miner_thread_handle.wait_for_panics()
        }))
        .wait_for_panics()
        .await?;

    // Modify the template into solved blocks.

    // TODO: Replace with Arc::unwrap_or_clone() when it stabilises
    let block = (*template).clone();

    let solved_blocks: Vec<Block> = solved_headers
        .into_iter()
        .map(|header| {
            let mut block = block.clone();
            block.header = Arc::new(header);
            block
        })
        .collect();

    Ok(solved_blocks
        .try_into()
        .expect("a 1:1 mapping of AtLeastOne produces at least one block"))
}

#[cfg(test)]
mod tests {
    use tower::buffer::Buffer;
    use zakura_chain::{
        chain_sync_status::MockSyncStatus, chain_tip::mock::MockChainTip, parameters::Network,
        serialization::ZcashDeserializeInto,
    };
    use zakura_network::address_book_peers::MockAddressBookPeers;
    use zakura_rpc::config::mining::{default_miner_address, MinerAddressType};
    use zakura_test::mock_service::MockService;

    use super::*;

    #[test]
    fn mining_template_updates_respect_submit_old() {
        let block = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into::<Arc<Block>>()
            .expect("block 1 deserializes");
        let current_header = *block.header;
        let mut changed_header = current_header;
        changed_header.nonce.0[0] ^= 1;

        assert!(should_replace_mining_template(None, current_header, None));
        assert!(!should_replace_mining_template(
            Some(current_header),
            current_header,
            Some(false),
        ));
        assert!(!should_replace_mining_template(
            Some(current_header),
            changed_header,
            Some(true),
        ));
        assert!(should_replace_mining_template(
            Some(current_header),
            changed_header,
            Some(false),
        ));
        assert!(should_replace_mining_template(
            Some(current_header),
            changed_header,
            None,
        ));
    }

    #[test]
    fn unavailable_mining_template_cancels_current_work() {
        let block = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into::<Arc<Block>>()
            .expect("block 1 deserializes");
        let old_header = *block.header;
        let (template_sender, template_receiver) = watch::channel(Some(block.clone()));
        let mut template_receiver = WatchReceiver::new(template_receiver);

        template_sender
            .send(Some(block))
            .expect("template receiver remains open");
        assert!(cancel_if_mining_template_changed(&mut template_receiver, old_header).is_ok());

        template_sender
            .send(None)
            .expect("template receiver remains open");
        assert!(matches!(
            cancel_if_mining_template_changed(&mut template_receiver, old_header),
            Err(SolverCancelled)
        ));
    }

    #[tokio::test]
    async fn template_generation_failure_invalidates_current_template() {
        let network = Network::Mainnet;
        let block = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into::<Arc<Block>>()
            .expect("block 1 deserializes");
        let (template_sender, mut template_receiver) = watch::channel(Some(block.clone()));

        let mining_config = zakura_rpc::config::mining::Config {
            miner_address: Some(
                default_miner_address(network.kind(), &MinerAddressType::Transparent)
                    .parse()
                    .expect("default Mainnet miner address is valid"),
            ),
            internal_miner: true,
            ..Default::default()
        };

        let (chain_tip, chain_tip_sender) = MockChainTip::new();
        chain_tip_sender.send_best_tip_height(block::Height(1));
        chain_tip_sender.send_best_tip_hash(block.hash());
        chain_tip_sender.send_estimated_distance_to_network_chain_tip(Some(0));

        let mut sync_status = MockSyncStatus::default();
        sync_status.set_is_close_to_tip(false);

        let mempool = Buffer::new(
            MockService::build().for_unit_tests::<
                mempool::Request,
                mempool::Response,
                zakura_node_services::BoxError,
            >(),
            1,
        );
        let state = Buffer::new(
            MockService::build().for_unit_tests::<
                zakura_state::Request,
                zakura_state::Response,
                zakura_state::BoxError,
            >(),
            1,
        );
        let read_state = Buffer::new(
            MockService::build().for_unit_tests::<
                zakura_state::ReadRequest,
                zakura_state::ReadResponse,
                zakura_state::BoxError,
            >(),
            1,
        );
        let block_verifier = Buffer::new(
            MockService::build().for_unit_tests::<
                zakura_consensus::Request,
                block::Hash,
                zakura_consensus::BoxError,
            >(),
            1,
        );
        let (_last_log_sender, last_log_receiver) = watch::channel(None);

        let (rpc, rpc_queue_task) = RpcImpl::new(
            network,
            mining_config,
            false,
            "0.0.1",
            "miner test",
            mempool,
            state,
            read_state,
            block_verifier,
            sync_status,
            chain_tip,
            MockAddressBookPeers::default(),
            last_log_receiver,
            None,
        );

        let template_generator = tokio::spawn(generate_block_templates(rpc, template_sender));

        tokio::time::timeout(Duration::from_secs(1), template_receiver.changed())
            .await
            .expect("template generation failure invalidates promptly")
            .expect("template sender remains open");
        assert!(
            template_receiver.borrow().is_none(),
            "an unsynced node must not retain its last valid mining template"
        );

        template_generator.abort();
        rpc_queue_task.abort();
    }
}
