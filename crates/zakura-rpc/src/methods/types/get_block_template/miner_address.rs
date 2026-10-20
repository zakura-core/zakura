//! The configured miner address, decoded into the receivers coinbase construction can use.

use zcash_address::{
    unified::{self, Container},
    ConversionError, TryFromAddress, ZcashAddress,
};
use zcash_protocol::consensus::{self, NetworkType};
use zcash_transparent::address::TransparentAddress;

/// A miner address that has been checked against the network and decoded into receivers.
///
/// Shielded receivers are validated while decoding, so a malformed configured address is
/// rejected when the miner parameters are built rather than when a coinbase is constructed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MinerAddress {
    /// A transparent P2PKH or P2SH address.
    Transparent(TransparentAddress),

    /// A Sapling payment address.
    Sapling(sapling_crypto::PaymentAddress),

    /// A [ZIP 316] Unified Address, reduced to the receivers Zakura understands.
    ///
    /// [ZIP 316]: https://zips.z.cash/zip-0316
    Unified {
        /// The Orchard receiver, which is payable through Ironwood after NU6.3.
        orchard: Option<orchard::Address>,
        /// The Sapling receiver.
        sapling: Option<sapling_crypto::PaymentAddress>,
        /// The transparent receiver.
        transparent: Option<TransparentAddress>,
    },

    /// A [ZIP 320] transparent-source-only P2PKH address, or "TEX address".
    ///
    /// [ZIP 320]: https://zips.z.cash/zip-0320
    Tex([u8; 20]),
}

impl MinerAddress {
    /// Decodes `zaddr`, rejecting addresses for networks other than `params`.
    pub fn try_from_zcash_address<P: consensus::Parameters>(
        params: &P,
        zaddr: ZcashAddress,
    ) -> Result<Self, ConversionError<&'static str>> {
        zaddr.convert_if_network(params.network_type())
    }

    /// Decodes an encoded address, returning `None` on any error.
    #[cfg(test)]
    pub(crate) fn decode<P: consensus::Parameters>(params: &P, s: &str) -> Option<Self> {
        Self::try_from_zcash_address(params, s.parse().ok()?).ok()
    }
}

impl From<TransparentAddress> for MinerAddress {
    fn from(addr: TransparentAddress) -> Self {
        Self::Transparent(addr)
    }
}

impl TryFromAddress for MinerAddress {
    type Error = &'static str;

    fn try_from_sapling(
        _net: NetworkType,
        data: [u8; 43],
    ) -> Result<Self, ConversionError<Self::Error>> {
        let addr = sapling_crypto::PaymentAddress::from_bytes(&data)
            .ok_or("Invalid Sapling payment address")?;
        Ok(Self::Sapling(addr))
    }

    fn try_from_unified(
        _net: NetworkType,
        ua: unified::Address,
    ) -> Result<Self, ConversionError<Self::Error>> {
        let mut orchard = None;
        let mut sapling = None;
        let mut transparent = None;

        // The unified decoder checks only receiver typecodes and lengths, so the receiver
        // bytes must be validated here instead of assumed canonical.
        for item in ua.items_as_parsed() {
            match item {
                unified::Receiver::Orchard(data) => {
                    orchard = Some(
                        Option::from(orchard::Address::from_raw_address_bytes(data))
                            .ok_or("Invalid Orchard receiver in Unified Address")?,
                    );
                }
                unified::Receiver::Sapling(data) => {
                    sapling = Some(
                        sapling_crypto::PaymentAddress::from_bytes(data)
                            .ok_or("Invalid Sapling receiver in Unified Address")?,
                    );
                }
                unified::Receiver::P2pkh(data) => {
                    transparent = Some(TransparentAddress::PublicKeyHash(*data));
                }
                unified::Receiver::P2sh(data) => {
                    transparent = Some(TransparentAddress::ScriptHash(*data));
                }
                unified::Receiver::Unknown { .. } => {}
            }
        }

        Ok(Self::Unified {
            orchard,
            sapling,
            transparent,
        })
    }

    fn try_from_transparent_p2pkh(
        _net: NetworkType,
        data: [u8; 20],
    ) -> Result<Self, ConversionError<Self::Error>> {
        Ok(Self::Transparent(TransparentAddress::PublicKeyHash(data)))
    }

    fn try_from_transparent_p2sh(
        _net: NetworkType,
        data: [u8; 20],
    ) -> Result<Self, ConversionError<Self::Error>> {
        Ok(Self::Transparent(TransparentAddress::ScriptHash(data)))
    }

    fn try_from_tex(
        _net: NetworkType,
        data: [u8; 20],
    ) -> Result<Self, ConversionError<Self::Error>> {
        Ok(Self::Tex(data))
    }
}
