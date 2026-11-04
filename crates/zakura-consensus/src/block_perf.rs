//! Enter synchronous diagnostic context on each poll, never across an await.

use std::future::Future;
use zakura_chain::diagnostic::block_perf::{Context, Stage};

pub(crate) async fn observe<F: Future>(
    context: Option<Context>,
    stage: Stage,
    future: F,
) -> F::Output {
    let mut future = std::pin::pin!(future);
    futures::future::poll_fn(|cx| {
        let _scope = Context::enter(context.clone(), stage);
        future.as_mut().poll(cx)
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use zakura_chain::{
        block::{Block, Hash},
        diagnostic::block_perf::{Operation, Session, Timer},
        serialization::ZcashDeserializeInto,
    };

    #[tokio::test]
    async fn context_is_reentered_after_yield_and_absent_between_polls() {
        let block: Block = zakura_test::vectors::BLOCK_MAINNET_1687107_BYTES
            .as_slice()
            .zcash_deserialize_into()
            .expect("the block fixture decodes");
        let session = Session::new(Hash([15; 32]), &block, "test");
        let context = session.context();
        observe(Some(context.clone()), Stage::Transactions, async {
            {
                let _operation = Timer::operation(Operation::Auth);
            }
            tokio::task::yield_now().await;
            {
                let _operation = Timer::operation(Operation::Auth);
            }
        })
        .await;
        assert!(Context::current().is_none());
        assert_eq!(context.count(Stage::Transactions, Operation::Auth), 2);
    }
}
