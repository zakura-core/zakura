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
    parameters::{
        Network, NetworkUpgrade, POST_BLOSSOM_POW_TARGET_SPACING, POST_NU7_POW_TARGET_SPACING,
    },
};
use zakurad::components::sync::end_of_support::{
    self, Config, SupportWindow, CHECK_INTERVAL, EOS_KEEPS_RUNNING_MESSAGE_HEADER,
    EOS_PANIC_MESSAGE_HEADER, EOS_UNSUPPORTED_MESSAGE_HEADER, EOS_UPGRADE_AHEAD_MESSAGE_HEADER,
    EOS_WARN_MESSAGE_HEADER, ESTIMATED_RELEASE_HEIGHT,
};

/// The release supports this height and halts at the next block.
fn last_supported_height() -> Height {
    Height(3_643_309)
}

/// The number of blocks per day at the halt height, which is past the ZIP 259
/// NU7 activation.
const EOS_BLOCKS_PER_DAY: u32 = 24 * 60 * 60 / POST_NU7_POW_TARGET_SPACING;

/// A config that keeps the node running past the end of its release's support.
const KEEPS_RUNNING: Config = Config { halt: false };

/// The support window of this release on Mainnet.
fn mainnet_window() -> SupportWindow {
    SupportWindow::of_release(&Network::Mainnet).expect("support ends on Mainnet")
}

/// Returns the height at which the last network upgrade of this release
/// activates on Mainnet.
fn last_mainnet_activation_height() -> Height {
    Network::Mainnet
        .full_activation_list()
        .last()
        .expect("Mainnet has network upgrades")
        .0
}

/// Returns a Mainnet window that ends `blocks` before the last network upgrade
/// of this release, so a tip past the window can still have that upgrade ahead.
fn window_ending_before_last_upgrade(blocks: u32) -> SupportWindow {
    SupportWindow::ending_at(
        Height(last_mainnet_activation_height().0 - blocks),
        &Network::Mainnet,
    )
}

/// Test that the `end_of_support` function is working as expected.
#[test]
#[should_panic(expected = "Zakura refuses to run past the last supported Mainnet height")]
fn end_of_support_panic() {
    // We are in panic
    let panic = last_supported_height().0 + 1;

    end_of_support::check(
        Height(panic),
        &mainnet_window(),
        &Network::Mainnet,
        Config::default(),
    );
}

/// Test that the `end_of_support` function is working as expected.
#[test]
fn end_of_support_function() {
    let (logs, dispatch) = log_capture::capture_logs();
    let _log_guard = tracing::dispatcher::set_default(&dispatch);
    // We are away from warn or panic
    let no_warn = last_supported_height().0 - 3 * EOS_BLOCKS_PER_DAY;

    end_of_support::check(
        Height(no_warn),
        &mainnet_window(),
        &Network::Mainnet,
        Config::default(),
    );
    assert!(logs.contains("Checking if Zakura release is inside support range ..."));
    assert!(logs.contains("Zakura release is supported"));

    assert!(!logs.contains(EOS_WARN_MESSAGE_HEADER));

    // We are in warn range
    let warn = no_warn + 1;

    end_of_support::check(
        Height(warn),
        &mainnet_window(),
        &Network::Mainnet,
        Config::default(),
    );
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
    end_of_support::check(
        last_supported_height,
        &mainnet_window(),
        &Network::Mainnet,
        Config::default(),
    );

    let testnet = Network::new_default_testnet();
    assert_eq!(end_of_support::end_of_support_height(&testnet), None);
    assert_eq!(SupportWindow::of_release(&testnet), None);
}

/// Test that a release past its support window keeps running when the halt is off.
#[test]
fn end_of_support_halt_can_be_turned_off() {
    let (logs, dispatch) = log_capture::capture_logs();
    let _log_guard = tracing::dispatcher::set_default(&dispatch);
    let window = mainnet_window();
    let last_supported_height = last_supported_height();

    // Close to the end of the window, the warning does not announce a halt.
    end_of_support::check(
        last_supported_height,
        &window,
        &Network::Mainnet,
        KEEPS_RUNNING,
    );
    assert!(logs.contains(EOS_KEEPS_RUNNING_MESSAGE_HEADER));
    assert!(!logs.contains(EOS_WARN_MESSAGE_HEADER));
    assert!(!logs.contains(EOS_UNSUPPORTED_MESSAGE_HEADER));

    // Past the window, the check warns where `end_of_support_panic` panics.
    end_of_support::check(
        Height(last_supported_height.0 + 1),
        &window,
        &Network::Mainnet,
        KEEPS_RUNNING,
    );
    assert!(logs.contains(EOS_UNSUPPORTED_MESSAGE_HEADER));
    assert!(logs.contains("It keeps running because end_of_support.halt is off"));
    assert!(!logs.contains(EOS_PANIC_MESSAGE_HEADER));
}

/// Test that a release that runs unsupported is warned about a network upgrade
/// ahead of its tip.
#[test]
fn unsupported_release_is_warned_about_an_upgrade_ahead() {
    let (logs, dispatch) = log_capture::capture_logs();
    let _log_guard = tracing::dispatcher::set_default(&dispatch);
    let window = window_ending_before_last_upgrade(1_000);
    let activation_height = last_mainnet_activation_height();

    // The upgrade is not reported once it has activated.
    end_of_support::check(activation_height, &window, &Network::Mainnet, KEEPS_RUNNING);
    assert!(logs.contains(EOS_UNSUPPORTED_MESSAGE_HEADER));
    assert!(!logs.contains(EOS_UPGRADE_AHEAD_MESSAGE_HEADER));

    end_of_support::check(
        Height(activation_height.0 - 100),
        &window,
        &Network::Mainnet,
        KEEPS_RUNNING,
    );
    assert!(logs.contains(EOS_UPGRADE_AHEAD_MESSAGE_HEADER));
    assert!(logs.contains(&format!(
        "activates at block {}, in 100 blocks",
        activation_height.0
    )));
}

/// Test the end of support values reported to the metrics endpoint.
#[test]
fn end_of_support_remaining_blocks() {
    let window = mainnet_window();
    let last_supported_height = last_supported_height();
    let warning_height = Height(last_supported_height.0 - 3 * EOS_BLOCKS_PER_DAY);

    assert_eq!(
        window.remaining_blocks(Height(ESTIMATED_RELEASE_HEIGHT)),
        i64::from(last_supported_height.0 - ESTIMATED_RELEASE_HEIGHT),
    );
    assert_eq!(
        window.remaining_blocks(warning_height),
        i64::from(3 * EOS_BLOCKS_PER_DAY),
    );
    assert_eq!(
        window.remaining_blocks(Height(warning_height.0 + 1)),
        i64::from(3 * EOS_BLOCKS_PER_DAY) - 1,
    );
    assert_eq!(window.remaining_blocks(last_supported_height), 0);

    // The count goes negative at the height that halts the node.
    assert_eq!(
        window.remaining_blocks(Height(last_supported_height.0 + 1)),
        -1,
    );

    // Support does not end outside Mainnet, so there is nothing to report.
    assert_eq!(
        SupportWindow::of_release(&Network::new_default_testnet()),
        None
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

    end_of_support::check(
        higher_checkpoint,
        &mainnet_window(),
        &Network::Mainnet,
        Config::default(),
    );
    assert!(logs.contains("Checking if Zakura release is inside support range ..."));
    assert!(!logs.contains(EOS_WARN_MESSAGE_HEADER));
}

/// Keep Mainnet EOS within one target-spaced block of December 5, 2026.
#[test]
fn mainnet_end_of_support_targets_december_5() {
    let network = Network::Mainnet;
    // Fixed projection anchor: Mainnet block 3,539,449, hash
    // 000000000038f877e08b47ee92a5d1d99856b160471d4697dad11344cdc712ba,
    // read from Mainnet RPC on November 3, 2026 (KST). Keep its block timestamp
    // independent of release height bumps and later bundle imports so they
    // cannot move the deadline.
    let reference_time = Utc
        .with_ymd_and_hms(2026, 11, 2, 15, 49, 5)
        .single()
        .expect("the reference timestamp is valid");
    let reference_height = Height(3_539_449);
    let deadline = Utc
        .with_ymd_and_hms(2026, 12, 5, 0, 0, 0)
        .single()
        .expect("the EOS deadline is valid");
    let nu7_activation_height = NetworkUpgrade::Nu7
        .activation_height(&network)
        .expect("NU7 activates on Mainnet");
    let first_unsupported_height = end_of_support::end_of_support_height(&network)
        .expect("Mainnet enforces end of support")
        .next()
        .expect("the support window ends below the maximum height");

    // Blocks arrive at the post-Blossom spacing through the last pre-NU7
    // height, and at the post-NU7 spacing from the ZIP 259 activation block.
    let last_pre_nu7_height = i64::from(nu7_activation_height.0) - 1;
    let pre_nu7_blocks = last_pre_nu7_height - i64::from(reference_height.0);
    let post_nu7_blocks = i64::from(first_unsupported_height.0) - last_pre_nu7_height;
    let estimated_halt = reference_time
        + chrono::Duration::seconds(pre_nu7_blocks * i64::from(POST_BLOSSOM_POW_TARGET_SPACING))
        + chrono::Duration::seconds(post_nu7_blocks * i64::from(POST_NU7_POW_TARGET_SPACING));

    assert!(
        estimated_halt >= deadline
            && estimated_halt - deadline
                < chrono::Duration::seconds(i64::from(POST_NU7_POW_TARGET_SPACING)),
        "Mainnet EOS must be within one target-spaced block after {deadline}; estimated halt \
         is {estimated_halt} at {first_unsupported_height:?}.",
    );
}

/// Check that the end of support task is working.
#[tokio::test(start_paused = true)]
async fn end_of_support_task() -> Result<()> {
    let (logs, dispatch) = log_capture::capture_logs();
    let (latest_chain_tip, latest_chain_tip_sender) = MockChainTip::new();
    latest_chain_tip_sender.send_best_tip_height(Height(10));

    let eos_future = end_of_support::start(
        Config::default(),
        Some(mainnet_window()),
        Network::Mainnet,
        latest_chain_tip,
    )
    .with_subscriber(dispatch);

    tokio::time::timeout(Duration::from_secs(15), eos_future)
        .await
        .expect_err(
            "end of support task unexpectedly exited: it should keep running until Zakura exits",
        );

    assert!(logs.contains("Checking if Zakura release is inside support range ..."));

    assert!(logs.contains("Zakura release is supported"));

    Ok(())
}

/// Check that the task of a release that runs unsupported warns about a network
/// upgrade ahead on every start, and again at every check.
#[tokio::test(start_paused = true)]
async fn unsupported_task_warns_about_an_upgrade_ahead_on_every_start() -> Result<()> {
    let window = window_ending_before_last_upgrade(1_000);
    let unsupported_tip = Height(last_mainnet_activation_height().0 - 100);

    // Each start of the node is a new task, so run it twice.
    for _ in 0..2 {
        let (logs, dispatch) = log_capture::capture_logs();
        let (latest_chain_tip, latest_chain_tip_sender) = MockChainTip::new();
        latest_chain_tip_sender.send_best_tip_height(unsupported_tip);

        let eos_future = end_of_support::start(
            KEEPS_RUNNING,
            Some(window),
            Network::Mainnet,
            latest_chain_tip,
        )
        .with_subscriber(dispatch);

        tokio::time::timeout(Duration::from_secs(15), eos_future)
            .await
            .expect_err("the task keeps running past the support window when the halt is off");

        assert_eq!(logs.count("end_of_support.halt is off: this node"), 1);
        assert_eq!(logs.count(EOS_UNSUPPORTED_MESSAGE_HEADER), 1);
        assert_eq!(logs.count(EOS_UPGRADE_AHEAD_MESSAGE_HEADER), 1);
    }

    // The warnings repeat at the next check.
    let (logs, dispatch) = log_capture::capture_logs();
    let (latest_chain_tip, latest_chain_tip_sender) = MockChainTip::new();
    latest_chain_tip_sender.send_best_tip_height(unsupported_tip);

    let eos_future = end_of_support::start(
        KEEPS_RUNNING,
        Some(window),
        Network::Mainnet,
        latest_chain_tip,
    )
    .with_subscriber(dispatch);

    tokio::time::timeout(CHECK_INTERVAL + Duration::from_secs(15), eos_future)
        .await
        .expect_err("the task keeps running past the support window when the halt is off");

    assert_eq!(logs.count(EOS_UNSUPPORTED_MESSAGE_HEADER), 2);
    assert_eq!(logs.count(EOS_UPGRADE_AHEAD_MESSAGE_HEADER), 2);

    Ok(())
}

/// Check that the first check runs soon after the state reports a tip, rather
/// than at the next hourly check.
#[tokio::test(start_paused = true)]
async fn end_of_support_task_checks_once_the_tip_is_known() -> Result<()> {
    let (logs, dispatch) = log_capture::capture_logs();
    let (latest_chain_tip, latest_chain_tip_sender) = MockChainTip::new();

    let eos_task = tokio::spawn(
        end_of_support::start(
            Config::default(),
            Some(mainnet_window()),
            Network::Mainnet,
            latest_chain_tip,
        )
        .with_subscriber(dispatch),
    );

    // The state has no tip yet, so there is nothing to check.
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert!(!logs.contains("Checking if Zakura release is inside support range ..."));

    latest_chain_tip_sender.send_best_tip_height(Height(10));
    tokio::time::sleep(Duration::from_secs(15)).await;
    assert!(logs.contains("Checking if Zakura release is inside support range ..."));
    assert!(logs.contains("Zakura release is supported"));

    assert!(
        !eos_task.is_finished(),
        "the task keeps running until Zakura exits"
    );
    eos_task.abort();

    Ok(())
}
