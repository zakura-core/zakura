//! Standalone transport acceptance measurements. These are excluded from ordinary CI
//! even when that lane runs ignored tests.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "standalone 240-second impaired-link activation gate"]
async fn paired_download_completes_with_request_pressure_and_packet_loss() -> Result<(), BoxError> {
    eprintln!(
        "paired matched download, 32000 requests, 50ms RTT, 1% loss: {:?}",
        download_over_link(true, true).await?
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "standalone combined impairment and paused-service activation gate"]
async fn paired_download_completes_with_request_pressure_paused_service_and_loss(
) -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    eprintln!(
        "paired matched download with pressure, paused service, and loss: {:?}",
        run_download(Workload {
            pressure: true,
            impaired: true,
            paused_siblings: 1,
            ..Workload::default()
        })
        .await?
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "standalone saturation recovery gate using the default 32-second liveness deadline"]
async fn sustained_saturation_cleans_up_and_retries_on_a_fresh_peer() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    eprintln!(
        "matched retry on a fresh peer: {:?}",
        run_download(Workload {
            paused_siblings: 2,
            recover_on_fresh_peer: true,
            ..Workload::default()
        })
        .await?
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "standalone repeated saturation, cleanup, and matched retry measurement"]
async fn twenty_saturated_connections_release_capacity_and_complete_retries() -> Result<(), BoxError>
{
    let _guard = zakura_test::init();
    for round in 1..=20 {
        let elapsed = run_download(Workload {
            paused_siblings: 2,
            recover_on_fresh_peer: true,
            ..Workload::default()
        })
        .await?;
        eprintln!("matched saturation retry {round}/20: {elapsed:?}");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "transport acceptance gate: twenty matched downloads and real reopen backoff"]
async fn twenty_pair_reopens_complete_matched_downloads_under_request_pressure(
) -> Result<(), BoxError> {
    eprintln!(
        "twenty matched downloads under request pressure: {:?}",
        download_rounds(true, false, 20).await?
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "standalone twenty-round impaired-link activation gate"]
async fn twenty_pair_reopens_complete_matched_downloads_under_request_pressure_and_loss(
) -> Result<(), BoxError> {
    eprintln!(
        "twenty matched downloads under request pressure and loss: {:?}",
        download_rounds(true, true, 20).await?
    );
    Ok(())
}
