//! The v7 encoding of [ZIP 248]: the common fields, the three maps, and the payload of
//! each registered bundle.
//!
//! Decoding enforces the ZIP 248 parsing rules, accepts only the registered pairs that a
//! [`Transaction::V7`] represents, in the maps the registry permits them in, and checks the
//! rules on each bundle's own encoding: exact payload lengths, canonical omission of
//! empty bundles, authorizing data present exactly when required, sighash variant 0 for
//! every signature, the coinbase fields, Orchard flags per pool, and the canonical Orchard
//! proof size. Rules that relate the parts of a transaction to each other or to the chain
//! are consensus checks.
//!
//! [ZIP 248]: https://zips.z.cash/zip-0248

use std::io::{self, Read as _};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use reddsa::{orchard::Binding, orchard::SpendAuth, Signature};

use crate::{
    amount::{Amount, NegativeAllowed, NonNegative},
    block::{self, MAX_BLOCK_BYTES},
    orchard,
    parameters::NetworkUpgrade,
    primitives::{Groth16Proof, Halo2Proof},
    sapling,
    serialization::{
        AtLeastOne, CompactSizeMessage, ReadZcashExt, SerializationError, TrustedPreallocate,
        ZcashDeserialize, ZcashReader, ZcashSerialize,
    },
    transaction::{serialize::orchard_allowed_flag_bits, LockTime, Transaction},
    transparent,
};

use super::{
    deltas::PoolDeltas,
    fields::{
        CoinbaseBundle, CoinbaseError, MinerPoll, MinerSignal, V7Fields, MAX_COINBASE_DATA_LEN,
    },
    registry::{self, BundleId},
    V7Ref,
};

/// The version group ID of v7 transactions, which ZIP 248 has not assigned yet.
pub(crate) const V7_VERSION_GROUP_ID: u32 = zcash_protocol::constants::V7_VERSION_GROUP_ID;

/// `sighashVariant` of the only variant ZIP 248 defines.
pub(super) const SIGHASH_VARIANT_0: u8 = 0;

/// The field encoding of a `sighashInfo` selecting sighash variant 0, which has no
/// associated data: `sizeSighashInfo = 1` followed by `sighashVariant = 0`.
pub(super) const SIGHASH_INFO_V0: [u8; 2] = [0x01, SIGHASH_VARIANT_0];

fn parse(message: &'static str) -> SerializationError {
    SerializationError::Parse(message)
}

// --- envelope ---------------------------------------------------------------

/// An `mValuePoolDeltas` entry, with its value not yet range-checked.
#[derive(Clone, Copy, Debug)]
pub(super) struct RawDelta {
    bundle: BundleId,
    value: i64,
}

/// An `mEffectBundles` or `mAuthBundles` entry, with its payload not yet decoded.
#[derive(Clone, Debug)]
pub(super) struct RawBundle {
    bundle: BundleId,
    data: Vec<u8>,
}

fn read_bundle_id<R: io::Read>(
    reader: &mut ZcashReader<R>,
) -> Result<BundleId, SerializationError> {
    let bundle_type: CompactSizeMessage = reader.read_value()?;
    let variant: CompactSizeMessage = reader.read_value()?;
    Ok((
        u32::try_from(usize::from(bundle_type))?,
        u32::try_from(usize::from(variant))?,
    ))
}

/// Writes a bundle's type and variant, which the encoding and every digest that names a
/// bundle prefix with.
pub(super) fn write_bundle_id<W: io::Write + ?Sized>(
    writer: &mut W,
    bundle: BundleId,
) -> io::Result<()> {
    for id in [bundle.0, bundle.1] {
        // A `u32` fits in the `usize` of every target Zakura builds for.
        CompactSizeMessage::try_from(id as usize)?.zcash_serialize(writer)?;
    }
    Ok(())
}

impl ZcashDeserialize for RawDelta {
    fn zcash_deserialize_from<R: io::Read>(
        reader: &mut ZcashReader<R>,
    ) -> Result<Self, SerializationError> {
        let bundle = read_bundle_id(reader)?;
        let value = reader.read_i64::<LittleEndian>()?;
        if value == 0 {
            return Err(parse("a zero value pool delta must be omitted"));
        }
        Ok(Self { bundle, value })
    }
}

/// A delta is at least a one-byte type, a one-byte variant, and an eight-byte value.
/// A map with more entries than registered bundle types cannot decode, so a longer one
/// is rejected before any entry is read.
impl TrustedPreallocate for RawDelta {
    fn min_serialized_size() -> u64 {
        10
    }

    fn max_allocation() -> u64 {
        registry::MAX_MAP_ENTRIES
    }
}

impl ZcashDeserialize for RawBundle {
    fn zcash_deserialize_from<R: io::Read>(
        reader: &mut ZcashReader<R>,
    ) -> Result<Self, SerializationError> {
        Ok(Self {
            bundle: read_bundle_id(reader)?,
            data: reader.read_value()?,
        })
    }
}

/// A bundle entry is at least a one-byte type, a one-byte variant, and an empty
/// payload's one-byte length. Like [`RawDelta`], a map holds at most one entry per
/// registered bundle type.
impl TrustedPreallocate for RawBundle {
    fn min_serialized_size() -> u64 {
        3
    }

    fn max_allocation() -> u64 {
        registry::MAX_MAP_ENTRIES
    }
}

/// Checks the ZIP 248 parsing rules that relate the entries of the three maps.
///
/// The fourth rule, that every delta is nonzero, is checked as each delta is read.
fn check_parsing_rules(
    deltas: &[RawDelta],
    effects: &[RawBundle],
    auths: &[RawBundle],
) -> Result<(), SerializationError> {
    let maps: [Vec<BundleId>; 3] = [
        deltas.iter().map(|d| d.bundle).collect(),
        effects.iter().map(|b| b.bundle).collect(),
        auths.iter().map(|b| b.bundle).collect(),
    ];

    // Each map has at most one entry per bundle type, in strictly increasing order.
    if maps
        .iter()
        .any(|ids| ids.windows(2).any(|pair| pair[1].0 <= pair[0].0))
    {
        return Err(parse(
            "v7 map entries must be in strictly increasing bundle type order",
        ));
    }

    // All entries for one bundle type use the same variant. With every map sorted, a
    // mismatch shows up as two entries for one type with different variants.
    let mut all: Vec<BundleId> = maps.iter().flatten().copied().collect();
    all.sort_unstable();
    if all
        .windows(2)
        .any(|pair| pair[0].0 == pair[1].0 && pair[0].1 != pair[1].1)
    {
        return Err(parse(
            "v7 entries for one bundle type must use the same variant",
        ));
    }

    // Authorizing data cannot exist without effecting data of the same bundle.
    if auths.iter().any(|auth| {
        effects
            .binary_search_by_key(&auth.bundle.0, |b| b.bundle.0)
            .is_err()
    }) {
        return Err(parse("v7 authorizing data needs effecting data"));
    }

    Ok(())
}

/// Checks that `bundle` is registered for v7 and permitted in the map `permitted`
/// selects.
fn check_registered(
    bundle: BundleId,
    permitted: impl FnOnce(&registry::Registration) -> bool,
) -> Result<(), SerializationError> {
    match registry::registration(bundle) {
        Some(registration) if permitted(&registration) => Ok(()),
        Some(_) => Err(parse(
            "the v7 bundle registry does not permit this map entry",
        )),
        None => Err(parse("the v7 bundle is not registered")),
    }
}

/// Returns the payload of `bundle` in `map`, if the map has an entry for it.
fn payload_of(map: &[RawBundle], bundle: BundleId) -> Option<&[u8]> {
    map.iter()
        .find(|entry| entry.bundle == bundle)
        .map(|entry| entry.data.as_slice())
}

/// Decodes a recognized bundle payload, which must consume exactly its declared length.
fn decode_payload<T>(
    data: &[u8],
    decode: impl FnOnce(&mut ZcashReader<&mut &[u8]>) -> Result<T, SerializationError>,
) -> Result<T, SerializationError> {
    let mut remaining = data;
    let value = decode(&mut ZcashReader::from_slice(&mut remaining))?;
    if !remaining.is_empty() {
        return Err(parse("trailing bytes in a v7 bundle payload"));
    }
    Ok(value)
}

/// Reads a `sighashInfo` field, requiring it to select sighash variant 0.
fn read_sighash_info_v0<R: io::Read>(
    reader: &mut ZcashReader<R>,
) -> Result<(), SerializationError> {
    let len: CompactSizeMessage = reader.read_value()?;
    if usize::from(len) != 1 || reader.read_u8()? != SIGHASH_VARIANT_0 {
        return Err(parse(
            "v7 signatures must use sighash variant 0 with no associated data",
        ));
    }
    Ok(())
}

/// Reads one signature, preceded by its `sighashInfo`, for each of `count` items that
/// were already decoded.
fn read_signatures<R: io::Read, T: ZcashDeserialize>(
    reader: &mut ZcashReader<R>,
    count: usize,
) -> Result<Vec<T>, SerializationError> {
    // `count` is the number of spends or actions already read from the payload, so it
    // is bounded by the input and safe to allocate for.
    let mut signatures = Vec::with_capacity(count);
    for _ in 0..count {
        read_sighash_info_v0(reader)?;
        signatures.push(reader.read_value()?);
    }
    Ok(signatures)
}

// --- decoding ---------------------------------------------------------------

/// Reads a v7 transaction whose `header` field has already been read.
pub(crate) fn read<R: io::Read>(
    reader: &mut ZcashReader<R>,
) -> Result<Transaction, SerializationError> {
    // Denoted as `nVersionGroupId` in the spec.
    if reader.read_u32::<LittleEndian>()? != V7_VERSION_GROUP_ID {
        return Err(parse("expected the v7 version group ID"));
    }
    // Denoted as `nConsensusBranchId` in the spec.
    let network_upgrade = NetworkUpgrade::try_from(reader.read_u32::<LittleEndian>()?)?;
    if network_upgrade < NetworkUpgrade::Nu7_1 {
        return Err(parse(
            "v7 transaction must have a NU7.1 or later consensus branch ID",
        ));
    }
    let lock_time: LockTime = reader.read_value()?;
    let expiry_height = block::Height(reader.read_u32::<LittleEndian>()?);

    let deltas: Vec<RawDelta> = reader.read_value()?;
    let effects: Vec<RawBundle> = reader.read_value()?;
    let auths: Vec<RawBundle> = reader.read_value()?;
    check_parsing_rules(&deltas, &effects, &auths)?;

    let mut pool_deltas = PoolDeltas::default();
    for delta in &deltas {
        check_registered(delta.bundle, |r| r.value_pool_delta)?;
        // Every registered pair has a delta field, which the registry tests check.
        *pool_deltas
            .get_mut(delta.bundle)
            .ok_or(parse("the v7 bundle has no value pool delta"))? = delta.value.try_into()?;
    }
    for effect in &effects {
        check_registered(effect.bundle, |r| r.effecting)?;
    }
    for auth in &auths {
        check_registered(auth.bundle, |r| r.authorizing)?;
    }
    let effect = |bundle| payload_of(&effects, bundle);
    let auth = |bundle| payload_of(&auths, bundle);

    let (inputs, outputs) = match effect(registry::TRANSPARENT) {
        Some(effect) => read_transparent(effect, auth(registry::TRANSPARENT))?,
        None => (Vec::new(), Vec::new()),
    };
    let coinbase = effect(registry::COINBASE)
        .map(|effect| decode_payload(effect, read_coinbase))
        .transpose()?;
    let is_coinbase = coinbase.is_some();
    let sapling_shielded_data = shielded_bundle(
        effect(registry::SAPLING),
        auth(registry::SAPLING),
        pool_deltas.sapling,
        |effect, auth, value_balance| read_sapling(effect, auth, value_balance, is_coinbase),
    )?;
    let orchard_shielded_data = shielded_bundle(
        effect(registry::ORCHARD),
        auth(registry::ORCHARD),
        pool_deltas.orchard,
        |effect, auth, value_balance| read_orchard(effect, auth, value_balance, Pool::Orchard),
    )?;
    let ironwood_shielded_data = shielded_bundle(
        effect(registry::IRONWOOD),
        auth(registry::IRONWOOD),
        pool_deltas.ironwood,
        |effect, auth, value_balance| read_orchard(effect, auth, value_balance, Pool::Ironwood),
    )?;

    Ok(Transaction::V7 {
        network_upgrade,
        lock_time,
        expiry_height,
        inputs,
        outputs,
        sapling_shielded_data: sapling_shielded_data.map(Box::new),
        orchard_shielded_data: orchard_shielded_data.map(Box::new),
        ironwood_shielded_data: ironwood_shielded_data.map(Box::new),
        v7_fields: Box::new(V7Fields {
            value_deltas: pool_deltas.unbundled,
            coinbase,
        }),
    })
}

/// Decodes a Sapling, Orchard, or Ironwood bundle, which has both effecting and
/// authorizing data, and whose balancing value is its entry in `mValuePoolDeltas`.
fn shielded_bundle<T>(
    effect: Option<&[u8]>,
    auth: Option<&[u8]>,
    value_balance: Amount<NegativeAllowed>,
    decode: impl FnOnce(&[u8], &[u8], Amount<NegativeAllowed>) -> Result<T, SerializationError>,
) -> Result<Option<T>, SerializationError> {
    match (effect, auth) {
        (Some(effect), Some(auth)) => decode(effect, auth, value_balance).map(Some),
        (Some(_), None) => Err(parse("a v7 shielded bundle needs authorizing data")),
        // The parsing rules already rule out authorizing data without effecting data.
        (None, _) if value_balance == Amount::<NegativeAllowed>::zero() => Ok(None),
        (None, _) => Err(parse(
            "a v7 shielded balancing value needs its effecting bundle",
        )),
    }
}

/// The effecting data of one transparent input: its previous output and sequence
/// number.
pub(super) struct InputEffects {
    outpoint: transparent::OutPoint,
    sequence: u32,
}

impl ZcashDeserialize for InputEffects {
    fn zcash_deserialize_from<R: io::Read>(
        reader: &mut ZcashReader<R>,
    ) -> Result<Self, SerializationError> {
        Ok(Self {
            outpoint: reader.read_value()?,
            sequence: reader.read_u32::<LittleEndian>()?,
        })
    }
}

/// An input's effecting data is a 36-byte previous output and a 4-byte sequence number.
impl TrustedPreallocate for InputEffects {
    fn min_serialized_size() -> u64 {
        40
    }

    fn max_allocation() -> u64 {
        MAX_BLOCK_BYTES / Self::min_serialized_size()
    }
}

/// Decodes the transparent bundle, whose authorizing data is present exactly when it
/// spends at least one input.
fn read_transparent(
    effect: &[u8],
    auth: Option<&[u8]>,
) -> Result<(Vec<transparent::Input>, Vec<transparent::Output>), SerializationError> {
    let (input_effects, outputs) = decode_payload(effect, |reader| {
        let inputs: Vec<InputEffects> = reader.read_value()?;
        let outputs: Vec<transparent::Output> = reader.read_value()?;
        Ok((inputs, outputs))
    })?;
    if input_effects.is_empty() && outputs.is_empty() {
        return Err(parse("an empty v7 transparent bundle must be omitted"));
    }
    let unlock_scripts = match (input_effects.is_empty(), auth) {
        (true, None) => Vec::new(),
        (false, Some(auth)) => decode_payload(auth, |reader| {
            let mut scripts = Vec::with_capacity(input_effects.len());
            for _ in 0..input_effects.len() {
                read_sighash_info_v0(reader)?;
                scripts.push(reader.read_value::<transparent::Script>()?);
            }
            Ok(scripts)
        })?,
        (true, Some(_)) => {
            return Err(parse(
                "a v7 transparent bundle without inputs has no authorizing data",
            ))
        }
        (false, None) => {
            return Err(parse(
                "a v7 transparent bundle with inputs needs authorizing data",
            ))
        }
    };
    let inputs = input_effects
        .into_iter()
        .zip(unlock_scripts)
        .map(|(effects, unlock_script)| transparent::Input::PrevOut {
            outpoint: effects.outpoint,
            unlock_script,
            sequence: effects.sequence,
        })
        .collect();
    Ok((inputs, outputs))
}

fn read_coinbase(
    reader: &mut ZcashReader<&mut &[u8]>,
) -> Result<CoinbaseBundle, SerializationError> {
    let height = block::Height(reader.read_u32::<LittleEndian>()?);
    let block_subsidy: Amount<NonNegative> = reader.read_value()?;
    let lockbox_value: Amount<NonNegative> = reader.read_value()?;
    let coinbase_data_len: CompactSizeMessage = reader.read_value()?;
    // Check the length before reading the data, so a long one allocates nothing.
    if usize::from(coinbase_data_len) > MAX_COINBASE_DATA_LEN {
        return Err(coinbase_error(CoinbaseError::CoinbaseDataTooLong));
    }
    let coinbase_data = reader.read_bytes(coinbase_data_len.into())?;
    let signal_count: CompactSizeMessage = reader.read_value()?;
    let signal_bytes = reader.read_bytes(usize::from(signal_count).saturating_mul(2))?;
    let miner_poll = if signal_bytes.is_empty() {
        None
    } else {
        let signals = signal_bytes
            .as_chunks::<2>()
            .0
            .iter()
            .map(|&[signal_code, level_of_support]| MinerSignal {
                signal_code,
                level_of_support,
            })
            .collect();
        let mut poll_digest = [0; 16];
        reader.read_exact(&mut poll_digest)?;
        Some(MinerPoll::new(signals, poll_digest).map_err(coinbase_error)?)
    };
    CoinbaseBundle::new(
        height,
        block_subsidy,
        lockbox_value,
        coinbase_data,
        miner_poll,
    )
    .map_err(coinbase_error)
}

fn coinbase_error(error: CoinbaseError) -> SerializationError {
    parse(error.message())
}

/// Decodes a v7 Sapling bundle from its effecting and authorizing payloads.
///
/// A coinbase transaction's spends are rejected before they are allocated, as for v5
/// and v6 transactions (GHSA-rgwx-8r98-p34c).
fn read_sapling(
    effect: &[u8],
    auth: &[u8],
    value_balance: Amount<NegativeAllowed>,
    is_coinbase: bool,
) -> Result<sapling::ShieldedData<sapling::SharedAnchor>, SerializationError> {
    let (spend_prefixes, output_prefixes) = decode_payload(effect, |reader| {
        let spend_count: CompactSizeMessage = reader.read_value()?;
        // # Consensus
        //
        // > A coinbase transaction MUST NOT have any Spend descriptions.
        //
        // <https://zips.z.cash/protocol/protocol.pdf#txnconsensus>
        if is_coinbase && usize::from(spend_count) > 0 {
            return Err(parse("coinbase transaction must not have Sapling spends"));
        }
        let spends: Vec<sapling::SpendPrefixInTransactionV5> =
            reader.read_external_count(spend_count.into())?;
        let outputs: Vec<sapling::OutputPrefixInTransactionV5> = reader.read_value()?;
        Ok((spends, outputs))
    })?;
    if spend_prefixes.is_empty() && output_prefixes.is_empty() {
        return Err(parse("an empty v7 Sapling bundle must be omitted"));
    }
    let (shared_anchor, spend_proofs, spend_sigs, output_proofs, binding_sig) =
        decode_payload(auth, |reader| {
            let shared_anchor: Option<sapling::tree::Root> = if spend_prefixes.is_empty() {
                None
            } else {
                Some(reader.read_value()?)
            };
            let spend_proofs: Vec<Groth16Proof> =
                reader.read_external_count(spend_prefixes.len())?;
            let spend_sigs: Vec<redjubjub::Signature<redjubjub::SpendAuth>> =
                read_signatures(reader, spend_prefixes.len())?;
            let output_proofs: Vec<Groth16Proof> =
                reader.read_external_count(output_prefixes.len())?;
            read_sighash_info_v0(reader)?;
            let binding_sig = reader.read_64_bytes()?.into();
            Ok((
                shared_anchor,
                spend_proofs,
                spend_sigs,
                output_proofs,
                binding_sig,
            ))
        })?;

    let spends: Vec<_> = spend_prefixes
        .into_iter()
        .zip(spend_proofs)
        .zip(spend_sigs)
        .map(|((prefix, proof), sig)| {
            sapling::Spend::<sapling::SharedAnchor>::from_v5_parts(prefix, proof, sig)
        })
        .collect();
    let outputs: Vec<_> = output_prefixes
        .into_iter()
        .zip(output_proofs)
        .map(|(prefix, proof)| sapling::Output::from_v5_parts(prefix, proof))
        .collect();
    let transfers = match shared_anchor {
        Some(shared_anchor) => sapling::TransferData::SpendsAndMaybeOutputs {
            shared_anchor,
            spends: spends.try_into()?,
            maybe_outputs: outputs,
        },
        None => sapling::TransferData::JustOutputs {
            outputs: outputs.try_into()?,
        },
    };
    Ok(sapling::ShieldedData {
        value_balance,
        transfers,
        binding_sig,
    })
}

/// The pool an Orchard-protocol bundle acts on, which fixes its flag grammar.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Pool {
    Orchard,
    Ironwood,
}

impl Pool {
    /// The flag bits a bundle of this pool may set. `enableCrossAddress` must be 0 in an
    /// Orchard bundle (ZIP 2006) and is unrestricted in an Ironwood bundle.
    fn allowed_flag_bits(self) -> u8 {
        orchard_allowed_flag_bits(self == Pool::Ironwood)
    }
}

/// Decodes a v7 Orchard or Ironwood bundle from its effecting and authorizing payloads.
fn read_orchard(
    effect: &[u8],
    auth: &[u8],
    value_balance: Amount<NegativeAllowed>,
    pool: Pool,
) -> Result<orchard::ShieldedData, SerializationError> {
    let (actions, flags) = decode_payload(effect, |reader| {
        // The actions' points are decompressed together once the whole list is read.
        let encodings: Vec<orchard::ActionEncoding> = reader.read_value()?;
        let actions = orchard::ActionEncoding::decompress(encodings)?;
        if actions.is_empty() {
            return Err(parse("an empty v7 Orchard-protocol bundle must be omitted"));
        }
        let bits = reader.read_u8()?;
        if bits & !pool.allowed_flag_bits() != 0 {
            return Err(parse("invalid reserved orchard flags"));
        }
        Ok((actions, orchard::Flags::from_bits_retain(bits)))
    })?;
    let (shared_anchor, proof, sigs, binding_sig) = decode_payload(auth, |reader| {
        let shared_anchor: orchard::tree::Root = reader.read_value()?;
        let proof: Halo2Proof = reader.read_value()?;
        if proof.0.len() != orchard::shielded_data::expected_proof_size(actions.len()) {
            return Err(SerializationError::NonCanonicalShieldedProofSize);
        }
        let sigs: Vec<Signature<SpendAuth>> = read_signatures(reader, actions.len())?;
        read_sighash_info_v0(reader)?;
        let binding_sig: Signature<Binding> = reader.read_value()?;
        Ok((shared_anchor, proof, sigs, binding_sig))
    })?;
    let actions: AtLeastOne<orchard::AuthorizedAction> = actions
        .into_iter()
        .zip(sigs)
        .map(|(action, sig)| orchard::AuthorizedAction::from_parts(action, sig))
        .collect::<Vec<_>>()
        .try_into()?;
    Ok(orchard::ShieldedData {
        flags,
        value_balance,
        shared_anchor,
        proof,
        actions,
        binding_sig,
    })
}

// --- encoding ---------------------------------------------------------------

/// Writes a v7 transaction after its `header` field.
pub(crate) fn write<W: io::Write + ?Sized>(tx: &V7Ref<'_>, writer: &mut W) -> io::Result<()> {
    if tx.network_upgrade < NetworkUpgrade::Nu7_1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "v7 transaction must have a NU7.1 or later consensus branch ID",
        ));
    }

    writer.write_u32::<LittleEndian>(V7_VERSION_GROUP_ID)?;
    writer.write_u32::<LittleEndian>(u32::from(tx.network_upgrade.branch_id().ok_or_else(
        || {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "NU7.1 has no consensus branch ID",
            )
        },
    )?))?;
    tx.lock_time.zcash_serialize(writer)?;
    writer.write_u32::<LittleEndian>(tx.expiry_height.0)?;

    let deltas = tx.pool_deltas().nonzero();
    CompactSizeMessage::try_from(deltas.len())?.zcash_serialize(writer)?;
    for (bundle, value) in deltas {
        write_bundle_id(writer, bundle)?;
        writer.write_i64::<LittleEndian>(value.zatoshis())?;
    }

    let (effects, auths) = tx.payloads()?;
    for map in [effects, auths] {
        CompactSizeMessage::try_from(map.len())?.zcash_serialize(writer)?;
        for (bundle, payload) in map {
            write_bundle_id(writer, bundle)?;
            CompactSizeMessage::try_from(payload.len())?.zcash_serialize(writer)?;
            writer.write_all(&payload)?;
        }
    }
    Ok(())
}

/// The payloads of the effecting and authorizing maps, in bundle type order.
type Payloads = (Vec<(BundleId, Vec<u8>)>, Vec<(BundleId, Vec<u8>)>);

impl V7Ref<'_> {
    /// Encodes every bundle's effecting and authorizing payloads.
    fn payloads(&self) -> io::Result<Payloads> {
        let mut effects = Vec::new();
        let mut auths = Vec::new();
        if self.has_transparent_effects() {
            effects.push((
                registry::TRANSPARENT,
                payload(|w| self.write_transparent_effects(w))?,
            ));
            if !self.inputs.is_empty() {
                auths.push((
                    registry::TRANSPARENT,
                    payload(|w| self.write_transparent_auth(w))?,
                ));
            }
        }
        if let Some(coinbase) = &self.fields.coinbase {
            effects.push((
                registry::COINBASE,
                payload(|w| write_coinbase(coinbase, w))?,
            ));
        }
        if let Some(sapling) = self.sapling {
            effects.push((
                registry::SAPLING,
                payload(|w| write_sapling_effects(sapling, w))?,
            ));
            auths.push((
                registry::SAPLING,
                payload(|w| write_sapling_auth(sapling, w))?,
            ));
        }
        for (bundle, data, pool) in [
            (registry::ORCHARD, self.orchard, Pool::Orchard),
            (registry::IRONWOOD, self.ironwood, Pool::Ironwood),
        ] {
            if let Some(data) = data {
                effects.push((bundle, payload(|w| write_orchard_effects(data, pool, w))?));
                auths.push((bundle, payload(|w| write_orchard_auth(data, w))?));
            }
        }
        Ok((effects, auths))
    }

    /// Writes the transparent effecting data: each input's previous output and sequence
    /// number, and the outputs.
    pub(super) fn write_transparent_effects<W: io::Write + ?Sized>(
        &self,
        writer: &mut W,
    ) -> io::Result<()> {
        CompactSizeMessage::try_from(self.inputs.len())?.zcash_serialize(writer)?;
        for input in self.inputs {
            let (outpoint, sequence) = prevout_input(input)?;
            outpoint.zcash_serialize(writer)?;
            writer.write_u32::<LittleEndian>(sequence)?;
        }
        CompactSizeMessage::try_from(self.outputs.len())?.zcash_serialize(writer)?;
        for output in self.outputs {
            output.zcash_serialize(writer)?;
        }
        Ok(())
    }

    /// Writes the transparent authorizing data, which ZIP 248's `transparent_auth_digest`
    /// hashes exactly.
    pub(super) fn write_transparent_auth<W: io::Write + ?Sized>(
        &self,
        writer: &mut W,
    ) -> io::Result<()> {
        for input in self.inputs {
            let transparent::Input::PrevOut { unlock_script, .. } = input else {
                return Err(coinbase_input());
            };
            writer.write_all(&SIGHASH_INFO_V0)?;
            unlock_script.zcash_serialize(writer)?;
        }
        Ok(())
    }
}

/// Returns a v7 input's previous output and sequence number. A v7 transaction has no
/// coinbase input: its coinbase bundle replaces it.
pub(super) fn prevout_input(
    input: &transparent::Input,
) -> io::Result<(&transparent::OutPoint, u32)> {
    match input {
        transparent::Input::PrevOut {
            outpoint, sequence, ..
        } => Ok((outpoint, *sequence)),
        transparent::Input::Coinbase { .. } => Err(coinbase_input()),
    }
}

fn coinbase_input() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "a v7 transaction has no coinbase input",
    )
}

fn payload(write: impl FnOnce(&mut Vec<u8>) -> io::Result<()>) -> io::Result<Vec<u8>> {
    let mut data = Vec::new();
    write(&mut data)?;
    Ok(data)
}

/// Writes the coinbase effecting data, which `coinbase_effects_digest` hashes exactly.
pub(super) fn write_coinbase<W: io::Write + ?Sized>(
    coinbase: &CoinbaseBundle,
    writer: &mut W,
) -> io::Result<()> {
    writer.write_u32::<LittleEndian>(coinbase.height().0)?;
    coinbase.block_subsidy().zcash_serialize(writer)?;
    coinbase.lockbox_value().zcash_serialize(writer)?;
    let coinbase_data = coinbase.coinbase_data();
    CompactSizeMessage::try_from(coinbase_data.len())?.zcash_serialize(writer)?;
    writer.write_all(coinbase_data)?;
    match coinbase.miner_poll() {
        None => CompactSizeMessage::try_from(0)?.zcash_serialize(writer),
        Some(poll) => {
            CompactSizeMessage::try_from(poll.signals().len())?.zcash_serialize(writer)?;
            for signal in poll.signals() {
                writer.write_all(&[signal.signal_code, signal.level_of_support])?;
            }
            writer.write_all(poll.poll_digest())
        }
    }
}

fn write_sapling_effects<W: io::Write + ?Sized>(
    sapling: &sapling::ShieldedData<sapling::SharedAnchor>,
    writer: &mut W,
) -> io::Result<()> {
    CompactSizeMessage::try_from(sapling.spends().count())?.zcash_serialize(writer)?;
    for spend in sapling.spends() {
        spend.zcash_serialize_v5_prefix(writer)?;
    }
    CompactSizeMessage::try_from(sapling.outputs().count())?.zcash_serialize(writer)?;
    for output in sapling.outputs() {
        output.zcash_serialize_v5_prefix(writer)?;
    }
    Ok(())
}

/// Writes the Sapling authorizing data, which `sapling_auth_digest` hashes exactly.
pub(super) fn write_sapling_auth<W: io::Write + ?Sized>(
    sapling: &sapling::ShieldedData<sapling::SharedAnchor>,
    writer: &mut W,
) -> io::Result<()> {
    if let Some(shared_anchor) = sapling.shared_anchor() {
        writer.write_all(&<[u8; 32]>::from(shared_anchor))?;
    }
    for spend in sapling.spends() {
        spend.zkproof.zcash_serialize(writer)?;
    }
    for spend in sapling.spends() {
        writer.write_all(&SIGHASH_INFO_V0)?;
        spend.spend_auth_sig.zcash_serialize(writer)?;
    }
    for output in sapling.outputs() {
        output.zkproof.zcash_serialize(writer)?;
    }
    writer.write_all(&SIGHASH_INFO_V0)?;
    writer.write_all(&<[u8; 64]>::from(sapling.binding_sig))
}

fn write_orchard_effects<W: io::Write + ?Sized>(
    bundle: &orchard::ShieldedData,
    pool: Pool,
    writer: &mut W,
) -> io::Result<()> {
    CompactSizeMessage::try_from(bundle.actions.len())?.zcash_serialize(writer)?;
    for authorized_action in bundle.actions.iter() {
        authorized_action.action.zcash_serialize(writer)?;
    }
    if bundle.flags.bits() & !pool.allowed_flag_bits() != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid reserved orchard flags",
        ));
    }
    writer.write_u8(bundle.flags.bits())
}

fn write_orchard_auth<W: io::Write + ?Sized>(
    bundle: &orchard::ShieldedData,
    writer: &mut W,
) -> io::Result<()> {
    bundle.shared_anchor.zcash_serialize(writer)?;
    bundle.proof.zcash_serialize(writer)?;
    for authorized_action in bundle.actions.iter() {
        writer.write_all(&SIGHASH_INFO_V0)?;
        authorized_action.spend_auth_sig.zcash_serialize(writer)?;
    }
    writer.write_all(&SIGHASH_INFO_V0)?;
    bundle.binding_sig.zcash_serialize(writer)
}
