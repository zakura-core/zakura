//! Helpers for durable header-root authentication frontiers.

use super::events::HeaderRootAuthState;
use zakura_chain::block;

/// True when both frontiers exist and neither was rebased onto a different hash
/// at the same height.
///
/// Heights may stay put or advance. Same-height hash changes, and any
/// `None`↔`Some` transition, are incompatible with speculative root-auth work.
pub(super) fn root_auth_pipeline_compatible(
    previous: Option<HeaderRootAuthState>,
    next: Option<HeaderRootAuthState>,
) -> bool {
    match (previous, next) {
        (Some(old), Some(new)) => {
            frontier_not_rebased(
                old.authenticated_height,
                old.authenticated_hash,
                new.authenticated_height,
                new.authenticated_hash,
            ) && frontier_not_rebased(
                old.completed_checkpoint_height,
                old.completed_checkpoint_hash,
                new.completed_checkpoint_height,
                new.completed_checkpoint_hash,
            )
        }
        _ => false,
    }
}

fn frontier_not_rebased(
    old_height: block::Height,
    old_hash: block::Hash,
    new_height: block::Height,
    new_hash: block::Hash,
) -> bool {
    new_height > old_height || (new_height == old_height && new_hash == old_hash)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn auth(
        authenticated_height: u32,
        authenticated_hash: u8,
        completed_checkpoint_height: u32,
        completed_checkpoint_hash: u8,
    ) -> HeaderRootAuthState {
        HeaderRootAuthState {
            authenticated_height: block::Height(authenticated_height),
            authenticated_hash: block::Hash([authenticated_hash; 32]),
            completed_checkpoint_height: block::Height(completed_checkpoint_height),
            completed_checkpoint_hash: block::Hash([completed_checkpoint_hash; 32]),
        }
    }

    #[test]
    fn missing_either_side_is_incompatible() {
        let state = auth(1, 1, 4, 4);
        assert!(!root_auth_pipeline_compatible(None, None));
        assert!(!root_auth_pipeline_compatible(None, Some(state)));
        assert!(!root_auth_pipeline_compatible(Some(state), None));
    }

    #[test]
    fn advancing_or_unchanged_frontiers_are_compatible() {
        let previous = auth(1, 1, 4, 4);
        assert!(root_auth_pipeline_compatible(
            Some(previous),
            Some(auth(2, 2, 4, 4))
        ));
        assert!(root_auth_pipeline_compatible(
            Some(previous),
            Some(auth(1, 1, 6, 6))
        ));
        assert!(root_auth_pipeline_compatible(
            Some(previous),
            Some(previous)
        ));
    }

    #[test]
    fn same_height_hash_change_is_a_rebase() {
        let previous = auth(1, 1, 4, 4);
        assert!(!root_auth_pipeline_compatible(
            Some(previous),
            Some(auth(1, 9, 4, 4))
        ));
        assert!(!root_auth_pipeline_compatible(
            Some(previous),
            Some(auth(1, 1, 4, 9))
        ));
    }
}
