//! Config building for the zcashd-compat integration test suite.

use std::{net::SocketAddr, path::PathBuf, time::Duration};

use color_eyre::eyre::Result;
use zakura_chain::{
    amount::Amount,
    parameters::{
        testnet::{ConfiguredActivationHeights, ConfiguredLockboxDisbursement, RegtestParameters},
        Network, NetworkKind,
    },
};
use zakura_rpc::config::mining::MinerAddressType;
use zakura_test::net::random_known_port;
use zakurad::{
    components::{mempool, With},
    config::ZakuradConfig,
};

use super::TEST_ZCASHD_PATH;
use crate::common::config::default_test_config;

/// The regtest network upgrade schedule of a zcashd-compat test.
#[derive(Clone, Copy, Debug)]
pub enum RegtestProfile {
    /// Every upgrade through NU5 at height 1, which any sidecar supports.
    Nu5AtOne,
    /// Every upgrade through NU6.3 at height 1 and NU7 at the given height, which needs a
    /// sidecar with NU7 support.
    Nu7At(u32),
}

/// The NSM value balance before NU7 in the [`RegtestProfile::Nu7At`] profile, configured on
/// both sides so that neither derives it.
pub const NU7_TEST_INITIAL_NSM_VALUE_BALANCE: i64 = 1_000_000_000;

/// The address of the zero-value one-time lockbox disbursement that both sides expect in
/// the NU6.1 activation block of the [`RegtestProfile::Nu7At`] profile.
const NU6_1_LOCKBOX_MARKER_ADDRESS: &str = "t2RnBRiqrN1nW4ecZs1Fj3WWjNdnSs4kiX8";

/// Configuration produced by [`build_zcashd_compat_config`].
pub struct ZcashdCompatConfig {
    pub zakurad_config: ZakuradConfig,
    /// The regtest network both processes use.
    pub network: Network,
    /// Zcashd datadir prepared for managed regtest mode.
    pub zcashd_datadir: PathBuf,
    /// Zakurad's main (unauthenticated) RPC listen address.
    pub zakura_rpc_addr: SocketAddr,
    /// Zcashd's own RPC listen address (user/pass authenticated).
    pub zcashd_own_rpc_addr: SocketAddr,
}

/// Hardcoded test credentials injected into zcashd via `-rpcuser`/`-rpcpassword`.
pub const ZCASHD_TEST_RPC_USER: &str = "zcashd_test";
pub const ZCASHD_TEST_RPC_PASS: &str = "zakura_test_pass";

/// Deterministic regtest miner keypair (secp256k1 secret key = 1, compressed).
///
/// zakurad mines coinbase to this address; tx-flow tests import the private key
/// into zcashd's wallet so the mined funds become spendable there.
pub const MINER_T_ADDR: &str = "tmLPctKo9j49rtCSKpwEBpLBeykiTGomGQs";
pub const MINER_PRIV_WIF: &str = "cMahea7zqjxrtgAbB7LSGbcQUr1uX1ojuat9jZodMN87JcbXMTcA";

/// Builds a regtest zakurad config wired for zcashd-compat testing.
///
/// `work_dir` is the test scratch directory used for the supervised zcashd
/// datadir. In managed-spawn mode this is the testdir (kept alive by the
/// `TestChild`).
pub fn build_zcashd_compat_config(work_dir: PathBuf) -> Result<ZcashdCompatConfig> {
    build_zcashd_compat_config_for(work_dir, RegtestProfile::Nu5AtOne)
}

/// Builds a regtest zakurad config wired for zcashd-compat testing, with the network upgrade
/// schedule of `profile` on both sides. See [`build_zcashd_compat_config`].
pub fn build_zcashd_compat_config_for(
    work_dir: PathBuf,
    profile: RegtestProfile,
) -> Result<ZcashdCompatConfig> {
    let net = match profile {
        RegtestProfile::Nu5AtOne => Network::new_regtest(
            ConfiguredActivationHeights {
                nu5: Some(1),
                ..Default::default()
            }
            .into(),
        ),
        RegtestProfile::Nu7At(nu7_height) => Network::new_regtest(RegtestParameters {
            activation_heights: ConfiguredActivationHeights {
                overwinter: Some(1),
                sapling: Some(1),
                blossom: Some(1),
                heartwood: Some(1),
                canopy: Some(1),
                nu5: Some(1),
                nu6: Some(1),
                nu6_1: Some(1),
                nu6_2: Some(1),
                nu6_3: Some(1),
                nu7: Some(nu7_height),
                ..Default::default()
            },
            lockbox_disbursements: Some(vec![ConfiguredLockboxDisbursement {
                address: NU6_1_LOCKBOX_MARKER_ADDRESS.to_string(),
                amount: Amount::zero(),
            }]),
            initial_nsm_value_balance: Some(NU7_TEST_INITIAL_NSM_VALUE_BALANCE.try_into()?),
            ..Default::default()
        }),
    };

    let zakura_rpc_port = random_known_port();
    let zcashd_own_rpc_port = random_known_port();

    let zakura_rpc_addr: SocketAddr = format!("127.0.0.1:{zakura_rpc_port}").parse()?;
    let zcashd_own_rpc_addr: SocketAddr = format!("127.0.0.1:{zcashd_own_rpc_port}").parse()?;

    let mut config = default_test_config(&net).with(MinerAddressType::Transparent);

    // Mine to the deterministic test keypair so tests can spend coinbase
    // after importing MINER_PRIV_WIF into zcashd's wallet.
    config.mining.miner_address = Some(MINER_T_ADDR.parse().expect("valid miner address"));

    // Main RPC: no cookie auth, single-threaded for test determinism
    config.rpc.listen_addr = Some(zakura_rpc_addr);
    config.rpc.parallel_cpu_threads = 1;
    config.rpc.enable_cookie_auth = false;

    // Enable mempool from genesis so tx-flow tests work immediately
    config.mempool = mempool::Config {
        debug_enable_at_height: Some(0),
        ..config.mempool
    };

    // Zcashd-compat mode
    config.zcashd_compat.enabled = true;
    config.zcashd_compat.manage_zcashd = true;
    config.zcashd_compat.zcashd_source =
        zakurad::components::zcashd_compat::ConfigZcashdBinarySource::Embedded;
    // Skip startup delay in tests — supervisor spawns zcashd immediately
    config.zcashd_compat.startup_delay = Duration::ZERO;

    // Use a fresh datadir inside the testdir. The supervisor bootstraps the
    // datadir and minimal `zcash.conf` before spawning zcashd.
    let zcashd_datadir = work_dir.join("zcashd-datadir");
    config.zcashd_compat.zcashd_datadir = Some(zcashd_datadir.clone());

    // Use an explicit zcashd path if provided, else embedded download.
    // An empty value counts as unset (the make targets always export the var).
    if let Some(path) = std::env::var_os(TEST_ZCASHD_PATH).filter(|path| !path.is_empty()) {
        config.zcashd_compat.zcashd_source =
            zakurad::components::zcashd_compat::ConfigZcashdBinarySource::Path;
        config.zcashd_compat.zcashd_path = Some(PathBuf::from(path));
    }

    // Expose zcashd's own RPC on a known port with simple test credentials
    config.zcashd_compat.zcashd_extra_args = vec![
        format!("-rpcport={zcashd_own_rpc_port}"),
        format!("-rpcuser={ZCASHD_TEST_RPC_USER}"),
        format!("-rpcpassword={ZCASHD_TEST_RPC_PASS}"),
        "-rpcallowip=127.0.0.1".to_string(),
        // Match zakurad's regtest activation heights (NU5 at height 1), or
        // zcashd rejects zakurad's mined blocks with `AcceptBlock FAILED`.
        "-nuparams=5ba81b19:1".to_string(), // Overwinter
        "-nuparams=76b809bb:1".to_string(), // Sapling
        "-nuparams=2bb40e60:1".to_string(), // Blossom
        "-nuparams=f5b9230b:1".to_string(), // Heartwood
        "-nuparams=e9ff75a6:1".to_string(), // Canopy
        "-nuparams=c2d6d0b4:1".to_string(), // NU5
        // The wallet tests use `getnewaddress`, which is deny-by-default
        // deprecated in current zcashd.
        "-allowdeprecated=getnewaddress".to_string(),
        // Regtest blocks mined on top of the 2011 genesis inherit old
        // median-time-past timestamps, which would keep zcashd in initial
        // block download forever and disable its wallet RPCs. 100 years.
        "-maxtipage=3153600000".to_string(),
    ];
    if let RegtestProfile::Nu7At(nu7_height) = profile {
        config.zcashd_compat.zcashd_extra_args.extend([
            "-nuparams=c8e71055:1".to_string(),         // NU6
            "-nuparams=4dec4df0:1".to_string(),         // NU6.1
            "-nuparams=5437f330:1".to_string(),         // NU6.2
            "-nuparams=37a5165b:1".to_string(),         // NU6.3
            format!("-nuparams=77190ad9:{nu7_height}"), // NU7
            format!("-onetimelockboxdisbursement=0:4dec4df0:0:{NU6_1_LOCKBOX_MARKER_ADDRESS}"),
            format!("-regtestnsminitialbalance={NU7_TEST_INITIAL_NSM_VALUE_BALANCE}"),
        ]);
    }

    Ok(ZcashdCompatConfig {
        zakurad_config: config,
        network: net,
        zcashd_datadir,
        zakura_rpc_addr,
        zcashd_own_rpc_addr,
    })
}

/// Returns the expected zakurad `chain` field value for the given network.
pub fn expected_zakurad_chain_name(network: &Network) -> String {
    network.bip70_network_name()
}

/// Returns the expected zcashd `chain` field value for the given network.
pub fn expected_zcashd_chain_name(network: &Network) -> &'static str {
    match network.kind() {
        NetworkKind::Mainnet => "main",
        NetworkKind::Testnet => "test",
        NetworkKind::Regtest => "regtest",
    }
}

/// Reads `TEST_ZCASHD_COMPAT_NETWORK` and returns the corresponding
/// [`NetworkKind`].  Defaults to `Regtest` when absent.
///
/// Returns `Err` for unrecognised values.
pub fn read_test_network_kind() -> Result<NetworkKind> {
    match std::env::var(super::TEST_ZCASHD_COMPAT_NETWORK)
        .ok()
        .as_deref()
    {
        None | Some("") | Some("Regtest") => Ok(NetworkKind::Regtest),
        Some("Mainnet") => Ok(NetworkKind::Mainnet),
        Some("Testnet") => Ok(NetworkKind::Testnet),
        Some(other) => Err(color_eyre::eyre::eyre!(
            "unrecognised {}: {other:?} (expected Mainnet, Testnet, or Regtest)",
            super::TEST_ZCASHD_COMPAT_NETWORK,
        )),
    }
}
