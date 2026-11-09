use std::{
    future::Future,
    ops::Deref,
    pin::Pin,
    sync::{Arc, OnceLock},
};

use tracing::Instrument;

use zakura_chain::{transaction::Transaction, transparent};
use zakura_script::CachedFfiTransaction;

use crate::{primitives::spawn_fifo_and_convert, BoxError};

#[cfg(test)]
mod tests;

/// Asynchronous script verification.
///
/// The verifier asynchronously requests the UTXO a transaction attempts
/// to use as an input, and verifies the script as soon as it becomes
/// available.  This allows script verification to be performed
/// asynchronously, rather than requiring that the entire chain up to
/// the previous block is ready.
///
/// The asynchronous script verification design is documented in [RFC4].
///
/// [RFC4]: https://zebra.zfnd.org/dev/rfcs/0004-asynchronous-script-verification.html
#[derive(Debug, Clone, Default, Copy, PartialEq, Eq)]
pub struct Verifier;

/// A script verification request.
#[derive(Debug)]
pub struct Request<T = Arc<Transaction>> {
    /// A cached transaction, in the format required by the script verifier FFI interface.
    pub cached_ffi_transaction: Arc<CachedFfiTransaction<T>>,
    /// The index of an input in `cached_ffi_transaction`, used for verifying this request
    ///
    /// Coinbase inputs are rejected by the script verifier, because they do not spend a UTXO.
    pub input_index: usize,
    /// Reserve mempool work before dispatch; blocks use their cumulative budget instead.
    pub limit_verification_work: bool,
}

/// Keep the queued mempool signature work below one tenth of a PQ block budget.
const MEMPOOL_VERIFICATION_WORK: u32 = 15_000;

fn mempool_work_budget() -> &'static Arc<tokio::sync::Semaphore> {
    static BUDGET: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();
    BUDGET.get_or_init(|| {
        Arc::new(tokio::sync::Semaphore::new(
            usize::try_from(MEMPOOL_VERIFICATION_WORK).expect("work budget fits supported targets"),
        ))
    })
}

impl<T: Deref<Target = Transaction> + Send + Sync + 'static> tower::Service<Request<T>>
    for Verifier
{
    type Response = ();
    type Error = BoxError;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<T>) -> Self::Future {
        use futures_util::FutureExt;

        let Request {
            cached_ffi_transaction,
            input_index,
            limit_verification_work,
        } = req;

        let span = tracing::trace_span!("script");
        async move {
            let input = cached_ffi_transaction
                .inputs()
                .get(input_index)
                .ok_or_else(|| {
                    format!("cached_ffi_transaction missing input at index {input_index}")
                })?;

            match input {
                transparent::Input::PrevOut { outpoint, .. } => {
                    let outpoint = *outpoint;

                    let permit = if limit_verification_work {
                        let work = cached_ffi_transaction.input_verification_work(input_index)?;
                        if work > MEMPOOL_VERIFICATION_WORK {
                            return Err("input exceeds mempool verification work capacity".into());
                        }
                        Some(
                            mempool_work_budget()
                                .clone()
                                .acquire_many_owned(work)
                                .await?,
                        )
                    } else {
                        None
                    };

                    // The worker owns its permit: cancelling this future must not
                    // release capacity while its Rayon closure still consumes CPU.
                    spawn_fifo_and_convert(move || {
                        let _permit = permit;
                        cached_ffi_transaction.is_valid(input_index)
                    })
                    .await?;
                    tracing::trace!(?outpoint, "script verification succeeded");

                    Ok(())
                }
                transparent::Input::Coinbase { .. } => Err("unexpected coinbase input".into()),
            }
        }
        .instrument(span)
        .boxed()
    }
}
