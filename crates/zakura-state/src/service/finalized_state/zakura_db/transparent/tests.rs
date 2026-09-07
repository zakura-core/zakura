//! Diagnostic comparison of UTXO request scheduling against an ephemeral database.

use futures::StreamExt;
use tower::{buffer::Buffer, ServiceExt};

use super::*;
use crate::{service::StateService, Config, Request, Response};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual warm-database timing comparison; run with --ignored --nocapture"]
#[allow(clippy::print_stdout)]
async fn utxo_state_path_timing() {
    let _init_guard = zakura_test::init();
    let (state, _, _, _) =
        StateService::new(Config::ephemeral(), &Network::Mainnet, Height::MAX, 0)
            .await
            .unwrap();
    let mut outpoints = Vec::new();
    let db = &state.read_service.db.db;
    let locations = db.cf_handle("tx_loc_by_hash").unwrap();
    let outputs = db.cf_handle("utxo_by_out_loc").unwrap();
    let mut write = DiskWriteBatch::new();
    // Seed only the indexes these informational reads use. This is not a valid chain.
    for index in 0u32..1001 {
        let mut hash = [0; 32];
        hash[..4].copy_from_slice(&index.to_le_bytes());
        let outpoint = transparent::OutPoint {
            hash: hash.into(),
            index: 0,
        };
        let location =
            TransactionLocation::from_usize(Height(1), usize::try_from(index).unwrap() + 1);
        write.zs_insert(&locations, outpoint.hash, location);
        write.zs_insert(
            &outputs,
            OutputLocation::from_outpoint(location, &outpoint),
            transparent::Output {
                value: Amount::<NonNegative>::try_from(u64::from(index) + 1).unwrap(),
                lock_script: transparent::Script::new(&[0x51]),
            },
        );
        outpoints.push(outpoint);
    }
    db.write(write).unwrap();
    let state = Buffer::new(state, 64);

    for (input_count, iterations) in [(1, 1000), (8, 1000), (64, 100), (1001, 20)] {
        for (name, width, concurrency) in [
            ("serial", 1, 1),
            ("per-input-64", 1, 64),
            ("batched-64", 64, 4),
        ] {
            // Warm the same keys before each measured mode.
            for outpoint in &outpoints[..input_count] {
                state
                    .clone()
                    .oneshot(Request::AwaitUtxo(*outpoint))
                    .await
                    .unwrap();
            }
            let start = std::time::Instant::now();
            for _ in 0..iterations {
                let requests = futures::stream::iter(outpoints[..input_count].chunks(width))
                    .map(|chunk| {
                        let request = if width == 1 {
                            Request::AwaitUtxo(chunk[0])
                        } else {
                            Request::AwaitUtxos(chunk.to_vec())
                        };
                        state.clone().oneshot(request)
                    })
                    .buffer_unordered(concurrency);
                futures::pin_mut!(requests);
                let mut count = 0;
                while let Some(response) = requests.next().await {
                    count += match response.unwrap() {
                        Response::Utxo(utxo) => {
                            std::hint::black_box(utxo);
                            1
                        }
                        Response::Utxos(utxos) => {
                            let count = utxos.len();
                            std::hint::black_box(utxos);
                            count
                        }
                        _ => unreachable!("UTXO request response"),
                    };
                }
                assert_eq!(count, input_count);
            }
            println!(
                "inputs={input_count} mode={name}: {:?}/set",
                start.elapsed() / iterations
            );
        }
    }
}
