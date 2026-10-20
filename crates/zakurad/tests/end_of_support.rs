//! Testing the end of support feature.

#![allow(clippy::unwrap_in_result)]

use std::time::Duration;

use chrono::{TimeZone, Utc};
use eyre::Result;
use tracing::instrument::WithSubscriber;

#[path = "end_of_support/log_capture.rs"]
mod log_capture;

use zakura_chain::{
    block::Height,
    chain_tip::mock::MockChainTip,
    parameters::{Network, POST_BLOSSOM_POW_TARGET_SPACING},
};
use zakurad::components::sync::end_of_support::{
    self, EOS_WARN_MESSAGE_HEADER, ESTIMATED_BLOCKS_PER_DAY, ESTIMATED_RELEASE_HEIGHT,
};

/// The release supports this height and halts at the next block.
fn last_supported_height() -> Height {
    Height(3_541_074)
}

/// Test that the `end_of_support` function is working as expected.
#[test]
#[should_panic(expected = "Zakura refuses to run past the last supported Mainnet height")]
fn end_of_support_panic() {
    // We are in panic
    let panic = last_supported_height().0 + 1;

    end_of_support::check(Height(panic), &Network::Mainnet);
}

/// Test that the `end_of_support` function is working as expected.
#[test]
fn end_of_support_function() {
    let (logs, dispatch) = log_capture::capture_logs();
    let _log_guard = tracing::dispatcher::set_default(&dispatch);
    // We are away from warn or panic
    let no_warn = last_supported_height().0 - 3 * ESTIMATED_BLOCKS_PER_DAY;

    end_of_support::check(Height(no_warn), &Network::Mainnet);
    assert!(logs.contains("Checking if Zakura release is inside support range ..."));
    assert!(logs.contains("Zakura release is supported"));

    assert!(!logs.contains(EOS_WARN_MESSAGE_HEADER));

    // We are in warn range
    let warn = no_warn + 1;

    end_of_support::check(Height(warn), &Network::Mainnet);
    assert!(logs.contains("Checking if Zakura release is inside support range ..."));
    assert!(logs.contains("Your Zakura release is too old and it will stop running at block"));

    // Panic is tested in `end_of_support_panic`
}

/// Test that end of support is only reported and enforced on Mainnet.
#[test]
fn end_of_support_height_per_network() {
    let last_supported_height = last_supported_height();
    assert_eq!(
        end_of_support::end_of_support_height(&Network::Mainnet),
        Some(last_supported_height)
    );
    end_of_support::check(last_supported_height, &Network::Mainnet);

    let testnet = Network::new_default_testnet();
    assert_eq!(end_of_support::end_of_support_height(&testnet), None);
    end_of_support::check(Height::MAX, &testnet);
}

/// Test the end of support values reported to the metrics endpoint.
#[test]
fn end_of_support_remaining_blocks() {
    let last_supported_height = last_supported_height();
    let warning_height = Height(last_supported_height.0 - 3 * ESTIMATED_BLOCKS_PER_DAY);

    assert_eq!(
        end_of_support::remaining_blocks(Height(ESTIMATED_RELEASE_HEIGHT), &Network::Mainnet),
        Some(i64::from(
            last_supported_height.0 - ESTIMATED_RELEASE_HEIGHT
        )),
    );
    assert_eq!(
        end_of_support::remaining_blocks(warning_height, &Network::Mainnet),
        Some(i64::from(3 * ESTIMATED_BLOCKS_PER_DAY)),
    );
    assert_eq!(
        end_of_support::remaining_blocks(Height(warning_height.0 + 1), &Network::Mainnet),
        Some(i64::from(3 * ESTIMATED_BLOCKS_PER_DAY) - 1),
    );
    assert_eq!(
        end_of_support::remaining_blocks(last_supported_height, &Network::Mainnet),
        Some(0),
    );

    // The count goes negative at the height that halts the node.
    assert_eq!(
        end_of_support::remaining_blocks(Height(last_supported_height.0 + 1), &Network::Mainnet),
        Some(-1),
    );

    // Support is not enforced outside Mainnet, so there is nothing to report.
    assert_eq!(
        end_of_support::remaining_blocks(last_supported_height, &Network::new_default_testnet()),
        None,
    );
}

/// Test that we are never in end of support warning or panic.
#[test]
fn end_of_support_date() {
    let (logs, dispatch) = log_capture::capture_logs();
    let _log_guard = tracing::dispatcher::set_default(&dispatch);
    // Get the list of checkpoints.
    let list = Network::Mainnet.checkpoint_list();

    // Get the last one we have and use it as tip.
    let higher_checkpoint = list.max_height();

    end_of_support::check(higher_checkpoint, &Network::Mainnet);
    assert!(logs.contains("Checking if Zakura release is inside support range ..."));
    assert!(!logs.contains(EOS_WARN_MESSAGE_HEADER));
}

/// Keep Mainnet EOS within one block before November 5, 2026.
#[test]
fn mainnet_end_of_support_precedes_november_5() {
    let network = Network::Mainnet;
    // Fixed projection anchor from the September 22 Mainnet release-state
    // bundle's `mainnet-vct-manifest.json`. Keep this independent of release
    // height bumps and later bundle imports so they cannot move the deadline.
    let reference_time = Utc
        .with_ymd_and_hms(2026, 9, 22, 5, 46, 39)
        .single()
        .expect("the reference timestamp is valid");
    let reference_height = Height(3_490_665);
    let deadline = Utc
        .with_ymd_and_hms(2026, 11, 5, 0, 0, 0)
        .single()
        .expect("the EOS deadline is valid");
    let first_unsupported_height = end_of_support::end_of_support_height(&network)
        .expect("Mainnet enforces end of support")
        .next()
        .expect("the support window ends below the maximum height");
    let remaining_blocks = i64::from(first_unsupported_height.0) - i64::from(reference_height.0);
    let estimated_halt = reference_time
        + chrono::Duration::seconds(remaining_blocks * i64::from(POST_BLOSSOM_POW_TARGET_SPACING));

    assert!(
        estimated_halt < deadline
            && deadline - estimated_halt
                < chrono::Duration::seconds(i64::from(POST_BLOSSOM_POW_TARGET_SPACING)),
        "Mainnet EOS must be within one block before {deadline}; estimated halt \
         is {estimated_halt} at {first_unsupported_height:?}.",
    );
}

/// Check that the end of support task is working.
#[tokio::test(start_paused = true)]
async fn end_of_support_task() -> Result<()> {
    let (logs, dispatch) = log_capture::capture_logs();
    let (latest_chain_tip, latest_chain_tip_sender) = MockChainTip::new();
    latest_chain_tip_sender.send_best_tip_height(Height(10));

    let eos_future =
        end_of_support::start(Network::Mainnet, latest_chain_tip).with_subscriber(dispatch);

    tokio::time::timeout(Duration::from_secs(15), eos_future)
        .await
        .expect_err(
            "end of support task unexpectedly exited: it should keep running until Zakura exits",
        );

    assert!(logs.contains("Checking if Zakura release is inside support range ..."));

    assert!(logs.contains("Zakura release is supported"));

    Ok(())
}
