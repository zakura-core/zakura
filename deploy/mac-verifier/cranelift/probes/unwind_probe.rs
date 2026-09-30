use std::sync::{Arc, Mutex};
struct Guard(Arc<Mutex<Vec<u32>>>, u32);
impl Drop for Guard {
    fn drop(&mut self) {
        self.0.lock().unwrap().push(self.1);
    }
}
#[inline(never)]
fn inner(log: Arc<Mutex<Vec<u32>>>) {
    let _guard = Guard(log, 2);
    std::panic::panic_any(1234u32);
}
#[inline(never)]
fn outer(log: Arc<Mutex<Vec<u32>>>) {
    let _guard = Guard(log.clone(), 1);
    inner(log);
}
fn main() {
    std::panic::set_hook(Box::new(|_| {}));
    let log = Arc::new(Mutex::new(Vec::new()));
    let result = std::panic::catch_unwind({
        let log = log.clone();
        move || outer(log)
    });
    assert_eq!(*result.unwrap_err().downcast::<u32>().unwrap(), 1234);
    assert_eq!(*log.lock().unwrap(), vec![2, 1]);
    let thread_log = log.clone();
    let joined = std::thread::spawn(move || {
        std::panic::catch_unwind(move || {
            let _guard = Guard(thread_log, 3);
            panic!("thread probe");
        })
        .is_err()
    })
    .join()
    .unwrap();
    assert!(joined);
    assert_eq!(*log.lock().unwrap(), vec![2, 1, 3]);
    println!("PASS: panic payload, nested cleanup order, thread catch, process survival");
}
