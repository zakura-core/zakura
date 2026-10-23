//! Tests for `zakura-rt`'s task names that build in both the production and
//! simulator builds. Outside a run, the simulator build delegates to tokio,
//! so these check that both behave as tokio does.

use std::future::pending;

use zakura_rt::task::{JoinSet, JoinSetExt};

#[tokio::test]
async fn join_set_spawn_named_spawns_into_the_set() {
    let mut set = JoinSet::new();
    set.spawn_named("worker", async { 1 });
    set.spawn_named("parked", pending::<i32>()).abort();
    assert_eq!(set.len(), 2);

    let mut joined = Vec::new();
    let mut aborted = 0;
    while let Some(result) = set.join_next().await {
        match result {
            Ok(value) => joined.push(value),
            Err(error) if error.is_cancelled() => aborted += 1,
            Err(error) => panic!("unexpected join error: {error}"),
        }
    }
    assert_eq!(joined, [1]);
    assert_eq!(aborted, 1);
}
