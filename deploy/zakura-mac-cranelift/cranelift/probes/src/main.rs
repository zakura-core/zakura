use futures_util::FutureExt;
use std::{
    panic::AssertUnwindSafe,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
struct DoublePanic;
impl Drop for DoublePanic {
    fn drop(&mut self) {
        panic!("second panic during cleanup");
    }
}
struct NestedCleanup(Arc<std::sync::Mutex<Vec<u32>>>, u32);
impl Drop for NestedCleanup {
    fn drop(&mut self) {
        self.0.lock().unwrap().push(self.1);
    }
}
#[inline(never)]
fn nested_panic(log: Arc<std::sync::Mutex<Vec<u32>>>) {
    let _inner = NestedCleanup(log, 2);
    std::panic::panic_any(1234u32);
}
struct Cleanup(Arc<AtomicUsize>);
impl Drop for Cleanup {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    std::panic::set_hook(Box::new(|_| {}));
    if std::env::args().any(|arg| arg == "double-panic") {
        let _guard = DoublePanic;
        panic!("first panic");
    }
    let log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let result = std::panic::catch_unwind({
        let log = log.clone();
        move || {
            let _outer = NestedCleanup(log.clone(), 1);
            nested_panic(log);
        }
    });
    assert_eq!(*result.unwrap_err().downcast::<u32>().unwrap(), 1234);
    assert_eq!(*log.lock().unwrap(), vec![2, 1]);
    let cleaned = Arc::new(AtomicUsize::new(0));
    let lock = Arc::new(tokio::sync::Mutex::new(0u32));
    for _ in 0..3 {
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
    assert_eq!(cleaned.load(Ordering::SeqCst), 3);
    assert_eq!(*lock.lock().await, 3);
    let failed = tokio::spawn(async { panic!("uncaught task panic") })
        .await
        .unwrap_err();
    assert!(failed.is_panic());
    assert_eq!(tokio::spawn(async { 777 }).await.unwrap(), 777);
    println!("PASS: nested cleanup, panic payload, async peer panics, cleanup, async lock release, healthy task survival, Tokio JoinError");
}
