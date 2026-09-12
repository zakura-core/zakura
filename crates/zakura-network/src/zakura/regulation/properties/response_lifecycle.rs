//! Independent exchange histories over the shared requester lifecycle.

use super::*;
use crate::zakura::CloseCause;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug)]
struct ExpectedExchange {
    generation: usize,
    published: bool,
    sent: bool,
    ended: bool,
}

proptest! {
    #[test]
    fn response_histories_keep_old_writers_fenced(
        actions in prop::collection::vec((0u8..7, 0usize..8, any::<bool>()), 1..192),
    ) {
        let connection = CancellationToken::new();
        let cause = CloseCause::new();
        let mut scopes = vec![ResponseScope::new(connection.clone(), cause.clone())];
        let mut owners: [Option<ResponseAuthorization>; 8] = std::array::from_fn(|_| None);
        // Keep permissions after owner Drop, endings, and scope retirement.
        let mut writers: [Option<ResponseWritePermission>; 8] = std::array::from_fn(|_| None);
        let mut expected: [Option<ExpectedExchange>; 8] = [None; 8];
        let mut closed = false;

        for (action, index, work_available) in actions {
            let generation = scopes.len() - 1;
            match action {
                0 if owners[index].is_none() => {
                    let grant = scopes[generation].authorize();
                    prop_assert_eq!(grant.is_some(), !closed);
                    if let Some(grant) = grant {
                        if let Some(old_writer) = &writers[index] {
                            prop_assert!(!old_writer.try_start(|| true));
                        }
                        writers[index] = Some(grant.write_permission());
                        owners[index] = Some(grant);
                        expected[index] = Some(ExpectedExchange {
                            generation,
                            published: false,
                            sent: false,
                            ended: false,
                        });
                    }
                }
                1 => {
                    if let (Some(writer), Some(exchange)) = (&writers[index], &mut expected[index]) {
                        let allowed = owners[index].is_some() && !closed
                            && exchange.generation == generation && !exchange.published
                            && !exchange.ended;
                        let mut published = false;
                        let actual = writer.publish(|| published = true);
                        prop_assert_eq!(actual, allowed);
                        prop_assert_eq!(published, allowed);
                        exchange.published |= allowed;
                    }
                }
                2 => {
                    if let (Some(writer), Some(exchange)) = (&writers[index], &mut expected[index]) {
                        let eligible = owners[index].is_some() && !closed
                            && exchange.generation == generation && exchange.published
                            && !exchange.sent && !exchange.ended;
                        let mut claimed = false;
                        let actual = writer.try_start(|| { claimed = true; work_available });
                        prop_assert_eq!(claimed, eligible);
                        prop_assert_eq!(actual, eligible && work_available);
                        exchange.sent |= eligible && work_available;
                    }
                }
                3 => {
                    if let (Some(owner), Some(exchange)) = (&mut owners[index], &mut expected[index]) {
                        // The adapter has validated an ending for a sent exchange.
                        if exchange.sent {
                            owner.finish();
                            exchange.ended = true;
                        }
                    }
                }
                4 => {
                    if owners[index].is_some() {
                        closed |= expected[index].is_some_and(|exchange| exchange.sent && !exchange.ended);
                    }
                    drop(owners[index].take());
                }
                5 => {
                    // Only unfinished sent exchanges prevent same-connection replacement.
                    closed |= expected.iter().zip(&owners).any(|(exchange, owner)| {
                        owner.is_some() && exchange.is_some_and(|exchange| {
                            exchange.generation == generation && exchange.sent && !exchange.ended
                        })
                    });
                    prop_assert_eq!(scopes[generation].retire(), !closed);
                    prop_assert!(scopes[generation].authorize().is_none());
                    if !closed {
                        scopes.push(ResponseScope::new(connection.clone(), cause.clone()));
                    }
                }
                6 => {
                    connection.cancel();
                    closed = true;
                }
                _ => {}
            }
            prop_assert_eq!(connection.is_cancelled(), closed);
            // Every old generation stays fenced even while its writer and owner survive.
            for (writer, exchange) in writers.iter().zip(&expected) {
                if let (Some(writer), Some(exchange)) = (writer, exchange) {
                    if closed || exchange.generation < scopes.len() - 1 {
                        prop_assert!(!writer.try_start(|| panic!("retired writer reached local work")));
                    }
                }
            }
        }

        closed |= expected.iter().zip(&owners).any(|(exchange, owner)| {
            owner.is_some() && exchange.is_some_and(|exchange| exchange.sent && !exchange.ended)
        });
        drop(owners);
        prop_assert_eq!(connection.is_cancelled(), closed);
        for writer in writers.iter().flatten() {
            prop_assert!(!writer.try_start(|| panic!("owner Drop must fence its writer")));
        }
    }
}
