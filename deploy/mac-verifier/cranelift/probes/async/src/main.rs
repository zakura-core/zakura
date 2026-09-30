use futures_util::FutureExt;
use std::{
    panic::AssertUnwindSafe,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
struct Cleanup(Arc<AtomicUsize>);
impl Drop for Cleanup {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    std::panic::set_hook(Box::new(|_| {}));
    let cleaned = Arc::new(AtomicUsize::new(0));
    let lock = Arc::new(tokio::sync::Mutex::new(0u32));
    for _ in 0..100 {
        let task_cleaned = cleaned.clone();
        let task_lock = lock.clone();
        let peer = tokio::spawn(async move {
            AssertUnwindSafe(async move {
                let _cleanup = Cleanup(task_cleaned);
                let mut guard = task_lock.lock().await;
                *guard += 1;
                tokio::task::yield_now().await;
                panic!("isolated peer panic");
            })
            .catch_unwind()
            .await
            .is_err()
        });
        assert!(peer.await.unwrap());
        let survivor_lock = lock.clone();
        assert!(
            tokio::spawn(async move {
                let _guard = survivor_lock.lock().await;
                42
            })
            .await
            .unwrap()
                == 42
        );
    }
    assert_eq!(cleaned.load(Ordering::SeqCst), 100);
    assert_eq!(*lock.lock().await, 100);
    let failed = tokio::spawn(async { panic!("uncaught task panic") })
        .await
        .unwrap_err();
    assert!(failed.is_panic());
    assert_eq!(tokio::spawn(async { 777 }).await.unwrap(), 777);
    println!("PASS: 100 async peer panics, cleanup, async lock release, healthy task survival, Tokio JoinError");
}
