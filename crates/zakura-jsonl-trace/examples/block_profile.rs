//! Synthetic fixture for exercising IPC and the explorer. Never presented as real chain data.
use profiles::{begin, start, Block, Config, Mode, Outcome, Stage};
use std::{path::PathBuf, thread::sleep, time::Duration};
use zakura_jsonl_trace::block_profile as profiles;

fn main() -> std::io::Result<()> {
    let socket = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .expect("pass collector socket path");
    let _runtime = start(
        &Config {
            socket: Some(socket),
            node: "synthetic-demo".into(),
            session: "illustrative timings, not chain measurements".into(),
        },
        "synthetic".into(),
        "demo".into(),
        "pruned-demo".into(),
    )?;
    sleep(Duration::from_millis(100));
    for i in 1..=12u8 {
        let root = begin(Block {
            hash: [i; 32],
            parent: [i - 1; 32],
            height: Some(u32::from(i)),
            transactions: u32::from(i),
            mode: Mode::Semantic,
        })
        .expect("recorder is enabled");
        let context = root.context();
        {
            let _s = context.span(Stage::BlockChecks);
            sleep(Duration::from_millis(20));
        }
        {
            let _s = context.span(Stage::Transactions);
            sleep(Duration::from_millis(if i == 9 { 550 } else { 80 }));
        }
        {
            let _s = context.span(Stage::WriterQueue);
            sleep(Duration::from_millis(20));
        }
        {
            let _s = context.span(Stage::Contextual);
            sleep(Duration::from_millis(110));
        }
        root.finish(Outcome::Success);
        {
            let finalization = context.span(Stage::Finalization);
            let finalization_context = finalization.context();
            {
                let _prepare = finalization_context.span(Stage::FinalizedBatchPrepare);
                sleep(Duration::from_millis(15));
            }
            let commit = finalization_context.span(Stage::FinalizedCommit);
            let _write = commit.context().span(Stage::RocksdbWrite);
            sleep(Duration::from_millis(20));
        }
    }
    sleep(Duration::from_secs(2));
    Ok(())
}
