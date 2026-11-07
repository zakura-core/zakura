use std::{cmp::max, fmt};

use zakura_chain::{
    block,
    parameters::{
        Network::{self, *},
        NetworkUpgrade::{self, *},
    },
};

use crate::constants;

#[cfg(any(test, feature = "proptest-impl"))]
use proptest_derive::Arbitrary;

/// A protocol version number.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct Version(pub u32);

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0.to_string())
    }
}

impl Version {
    /// Returns the minimum remote node network protocol version for `network` and
    /// `height`. Zebra disconnects from peers with lower versions.
    ///
    /// # Panics
    ///
    /// If we are incompatible with our own minimum remote protocol version.
    pub fn min_remote_for_height(
        network: &Network,
        height: impl Into<Option<block::Height>>,
    ) -> Version {
        let height = height.into().unwrap_or(block::Height(0));
        let min_spec = Version::min_specified_for_height(network, height);

        // shut down if our own version is too old
        assert!(
            constants::CURRENT_NETWORK_PROTOCOL_VERSION >= min_spec,
            "Zakura does not implement the minimum specified {:?} protocol version for {:?} at {:?}",
            NetworkUpgrade::current(network, height),
            network,
            height,
        );

        max(min_spec, Version::initial_min_for_network(network))
    }

    /// Returns the minimum supported network protocol version for `network`.
    ///
    /// This is the minimum peer version when Zebra is significantly behind current tip:
    /// - during the initial block download,
    /// - after Zebra restarts, and
    /// - after Zebra's local network is slow or shut down.
    fn initial_min_for_network(network: &Network) -> Version {
        *constants::INITIAL_MIN_NETWORK_PROTOCOL_VERSION
            .get(&network.kind())
            .expect("We always have a value for testnet or mainnet")
    }

    /// Returns the minimum specified network protocol version for `network` and
    /// `height`.
    ///
    /// This is the minimum peer version when Zebra is close to the current tip.
    fn min_specified_for_height(network: &Network, height: block::Height) -> Version {
        let network_upgrade = NetworkUpgrade::current(network, height);
        Version::min_specified_for_upgrade(network, network_upgrade)
    }

    /// Returns the minimum specified network protocol version for `network` and
    /// `network_upgrade`.
    ///
    /// ## ZIP-253
    ///
    /// > Nodes compatible with a network upgrade activation MUST advertise a network protocol
    /// > version that is greater than or equal to the MIN_NETWORK_PROTOCOL_VERSION for that
    /// > activation.
    ///
    /// <https://zips.z.cash/zip-0253>
    ///
    /// ### Notes
    ///
    /// - The citation above is a generalization of a statement in ZIP-253 since that ZIP is
    ///   concerned only with NU6 on Mainnet and Testnet.
    pub(crate) fn min_specified_for_upgrade(
        network: &Network,
        network_upgrade: NetworkUpgrade,
    ) -> Version {
        Version(match (network, network_upgrade) {
            (_, Genesis) | (_, BeforeOverwinter) => 170_002,
            (Testnet(_), Overwinter) => 170_003,
            (Mainnet, Overwinter) => 170_005,
            (Testnet(params), Sapling) if params.is_regtest() => 170_006,
            (Testnet(_), Sapling) => 170_007,
            (Mainnet, Sapling) => 170_007,
            (Testnet(_), Blossom) => 170_008,
            (Mainnet, Blossom) => 170_009,
            (Testnet(_), Heartwood) => 170_010,
            (Mainnet, Heartwood) => 170_011,
            (Testnet(_), Canopy) => 170_012,
            (Mainnet, Canopy) => 170_013,
            (Testnet(_), Nu5) => 170_050,
            (Mainnet, Nu5) => 170_100,
            (Testnet(_), Nu6) => 170_110,
            (Mainnet, Nu6) => 170_120,
            (Testnet(_), Nu6_1) => 170_130,
            (Mainnet, Nu6_1) => 170_140,
            (Testnet(_), Nu6_2) => 170_150,
            (Mainnet, Nu6_2) => 170_150,
            (Testnet(_), Nu6_3) => 170_160,
            (Mainnet, Nu6_3) => 170_160,
            // From NU7 onward, ZIP 204 assigns each upgrade a Testnet version of the form
            // 170_000 + 20·k and a Mainnet version 10 higher, so no two networks share a version.
            // Regtest and custom testnets use the Testnet version.
            (Testnet(_), Nu7) => 170_180,
            (Mainnet, Nu7) => 170_190,

            #[cfg(zcash_unstable = "zfuture")]
            (Testnet(_), ZFuture) => constants::CURRENT_NETWORK_PROTOCOL_VERSION.0,

            #[cfg(zcash_unstable = "zfuture")]
            (Mainnet, ZFuture) => {
                panic!("ZFuture network upgrade should not be active on Mainnet")
            }
        })
    }
}

bitflags! {
    /// A bitflag describing services advertised by a node in the network.
    ///
    /// Note that bits 24-31 are reserved for temporary experiments; other
    /// service bits should be allocated via the ZIP process.
    #[derive(Copy, Clone, Debug, Default, Hash, PartialEq, Eq, PartialOrd, Ord)]
    pub struct PeerServices: u64 {
        /// NODE_NETWORK means that the node is a full node capable of serving
        /// blocks, as opposed to a light client that makes network requests but
        /// does not provide network services.
        const NODE_NETWORK = 1;

        /// NODE_P2P_V2 means that the node can speak the experimental Zakura P2P v2 stack.
        ///
        /// This bit is in the temporary experiment range until a permanent service bit is assigned.
        const NODE_P2P_V2 = 1 << 24;
    }
}

/// A nonce used in the networking layer to identify messages.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
#[cfg_attr(any(test, feature = "proptest-impl"), derive(Arbitrary))]
pub struct Nonce(pub u64);

impl Default for Nonce {
    fn default() -> Self {
        use rand_10::{rng as thread_rng, RngExt};
        Self(thread_rng().random())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::constants::CURRENT_NETWORK_PROTOCOL_VERSION;

    #[test]
    fn version_extremes_mainnet() {
        version_extremes(&Mainnet)
    }

    #[test]
    fn version_extremes_testnet() {
        version_extremes(&Network::new_default_testnet())
    }

    /// Test the min_specified_for_upgrade and min_specified_for_height functions for `network` with
    /// extreme values.
    fn version_extremes(network: &Network) {
        let _init_guard = zakura_test::init();

        assert_eq!(
            Version::min_specified_for_height(network, block::Height(0)),
            Version::min_specified_for_upgrade(network, BeforeOverwinter),
        );

        // We assume that the last version we know about continues forever
        // (even if we suspect that won't be true)
        assert_ne!(
            Version::min_specified_for_height(network, block::Height::MAX),
            Version::min_specified_for_upgrade(network, BeforeOverwinter),
        );
    }

    #[test]
    fn version_consistent_mainnet() {
        version_consistent(&Mainnet)
    }

    #[test]
    fn version_consistent_testnet() {
        version_consistent(&Network::new_default_testnet())
    }

    /// Check that the min_specified_for_upgrade and min_specified_for_height functions
    /// are consistent for `network`.
    fn version_consistent(network: &Network) {
        let _init_guard = zakura_test::init();

        let highest_network_upgrade = NetworkUpgrade::current(network, block::Height::MAX);
        assert!(
            matches!(highest_network_upgrade, Nu6 | Nu6_1 | Nu6_2 | Nu6_3 | Nu7),
            "expected coverage of all network upgrades: \
            add the new network upgrade to the list in this test"
        );

        for &network_upgrade in &[
            BeforeOverwinter,
            Overwinter,
            Sapling,
            Blossom,
            Heartwood,
            Canopy,
            Nu5,
            Nu6,
            Nu6_1,
            Nu6_2,
            Nu6_3,
            Nu7,
        ] {
            let height = network_upgrade.activation_height(network);
            if let Some(height) = height {
                assert_eq!(
                    Version::min_specified_for_upgrade(network, network_upgrade),
                    Version::min_specified_for_height(network, height)
                );
            }
        }
    }

    /// Checks that Zakura advertises the Mainnet NU7 version, which meets the NU7
    /// minimum on every network.
    #[test]
    fn nu7_protocol_versions_match_current_version() {
        let _init_guard = zakura_test::init();

        assert_eq!(
            Version::min_specified_for_upgrade(&Mainnet, Nu7),
            CURRENT_NETWORK_PROTOCOL_VERSION
        );
        for network in [
            Network::new_default_testnet(),
            Network::new_regtest(Default::default()),
        ] {
            assert!(
                Version::min_specified_for_upgrade(&network, Nu7)
                    <= CURRENT_NETWORK_PROTOCOL_VERSION
            );
        }
    }

    /// Checks the NU7 protocol versions against the ZIP 204 assignment rule.
    #[test]
    fn nu7_protocol_versions_follow_zip_204() {
        let _init_guard = zakura_test::init();

        let testnet = Network::new_default_testnet();
        let regtest = Network::new_regtest(Default::default());

        assert_eq!(
            Version::min_specified_for_upgrade(&Mainnet, Nu7),
            Version(170_190)
        );
        assert_eq!(
            Version::min_specified_for_upgrade(&testnet, Nu7),
            Version(170_180)
        );
        assert_eq!(
            Version::min_specified_for_upgrade(&regtest, Nu7),
            Version(170_180)
        );

        // ZIP 204: the Testnet version is the least 170_000 + 20·k above the preceding
        // upgrade's Mainnet version, and the Mainnet version is 10 higher.
        let preceding_mainnet = Version::min_specified_for_upgrade(&Mainnet, Nu6_3).0;
        let base = 170_000 + ((preceding_mainnet - 170_000) / 20 + 1) * 20;
        assert_eq!(
            Version::min_specified_for_upgrade(&testnet, Nu7),
            Version(base)
        );
        assert_eq!(
            Version::min_specified_for_upgrade(&Mainnet, Nu7),
            Version(base + 10)
        );
    }

    #[test]
    fn custom_network_minimum_follows_activation_heights() {
        use zakura_chain::parameters::testnet::{ConfiguredActivationHeights, Parameters};

        let network = Parameters::build()
            .with_network_name("ProtocolMinimumTest")
            .unwrap()
            .with_activation_heights(ConfiguredActivationHeights {
                nu6_3: Some(10),
                nu7: Some(20),
                ..Default::default()
            })
            .unwrap()
            .clear_funding_streams()
            .to_network()
            .unwrap();
        let (mut tracked, tip) = crate::peer::MinimumPeerVersion::with_mock_chain_tip(&network);
        for (height, minimum) in [
            (0, 170_150),
            (9, 170_150),
            (10, 170_160),
            (19, 170_160),
            (20, 170_180),
            (21, 170_180),
        ] {
            tip.send_best_tip_height(Some(block::Height(height)));
            assert_eq!(tracked.current(), Version(minimum));
            let mut restarted =
                crate::peer::MinimumPeerVersion::new(tracked.chain_tip().clone(), &network);
            assert_eq!(restarted.current(), Version(minimum));
            assert_eq!(
                Version::min_remote_for_height(&network, block::Height(height)),
                Version(minimum)
            );
        }
        assert_eq!(
            Version::min_remote_for_height(&network, None),
            Version(170_150)
        );
        for upgrade in [
            Overwinter, Sapling, Blossom, Heartwood, Canopy, Nu5, Nu6, Nu6_1, Nu6_2, Nu6_3, Nu7,
        ] {
            assert_eq!(
                Version::min_specified_for_upgrade(&network, upgrade),
                Version::min_specified_for_upgrade(&Network::new_default_testnet(), upgrade)
            );
        }
        let minimum = Version::min_remote_for_height(&network, block::Height(20));
        assert!(Version(170_180) >= minimum);
        assert!(Version(170_179) < minimum);
    }

    #[test]
    fn peer_services_p2p_v2_bit_and_unknown_bits_are_compatible() {
        let _init_guard = zakura_test::init();

        assert_eq!(PeerServices::NODE_P2P_V2.bits(), 1 << 24);

        let decoded =
            PeerServices::from_bits_truncate(PeerServices::NODE_NETWORK.bits() | (1 << 25));
        assert_eq!(decoded, PeerServices::NODE_NETWORK);
    }
}
