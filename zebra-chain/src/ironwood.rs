//! Ironwood-related functionality.
//!
//! Ironwood uses the same action proof system and encoded action shape as
//! Orchard, but has distinct note commitment and nullifier state.

#![warn(missing_docs)]

pub use crate::orchard::{
    tree, Action, Address, AuthorizedAction, CommitmentRandomness, Diversifier, EncryptedNote,
    Flags, Note, NoteCommitment, Nullifier, ShieldedData, ValueCommitment, WrappedNoteKey,
};
