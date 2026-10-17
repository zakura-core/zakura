//! When this node closes a subscription: a `Close` the publisher never
//! answers drops the session at the request timeout.
//!
//! Tests that call an inner handler run
//! [`HeaderSyncReactor::schedule_cached_statuses`] afterwards, as the reactor
//! loop does after every event.

use super::{
    aux_fallback::{answer_locator, connect, end, fallback_reactor, next_update, pruned_status},
    *,
};

/// A `Close` the publisher never answers drops the session at the request
/// timeout, as an unresponsive request would, so the peer cannot stay parked
/// waiting for the outcome. An answered `Close` costs nothing.
#[test]
fn an_unanswered_close_drops_the_session_at_the_request_timeout() {
    for answered in [true, false] {
        let (mut reactor, mut actions, snapshot) = fallback_reactor();
        let (peer, mut outbound) = connect(&mut reactor, 0x71);
        let target = block::Hash([0x53; 32]);
        reactor.handle_status(peer.clone(), 0, pruned_status(&snapshot, target));
        let open = answer_locator(&mut reactor, &mut actions, &peer, &mut outbound, target);
        reactor.retire_peer_work(&peer, HeaderRequestTerminal::LocalError);
        assert_eq!(next_update(&mut outbound).operation, v9::Operation::Close);
        if answered {
            end(
                &mut reactor,
                &peer,
                &open,
                HeadersOutcomeCode::SubscriptionClosed,
            );
        }

        let session = reactor.peer_state[&peer].session.clone();
        reactor.retire_unanswered_closes(Instant::now());
        assert!(!session.cancel_token().is_cancelled());
        reactor.retire_unanswered_closes(Instant::now() + reactor.startup.request_timeout);
        assert_eq!(
            session.cancel_token().is_cancelled(),
            !answered,
            "only an unanswered Close drops the session"
        );
        assert_eq!(
            reactor.unproductive_peer_cooldowns.contains_key(&peer),
            !answered
        );
    }
}
