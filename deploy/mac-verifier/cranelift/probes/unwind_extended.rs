use std::sync::{Arc, Mutex};
struct Guard(Arc<Mutex<Vec<u32>>>, u32);
impl Drop for Guard {
    fn drop(&mut self) {
        self.0.lock().unwrap().push(self.1);
    }
}
struct DoublePanic;
impl Drop for DoublePanic {
    fn drop(&mut self) {
        panic!("second panic during cleanup");
    }
}
#[inline(never)]
fn deep(log: Arc<Mutex<Vec<u32>>>, depth: u32) {
    let _guard = Guard(log.clone(), depth);
    if depth == 0 {
        std::panic::panic_any(55u64)
    } else {
        deep(log, depth - 1)
    }
}
fn main() {
    std::panic::set_hook(Box::new(|_| {}));
    if std::env::args().any(|arg| arg == "double-panic") {
        let _guard = DoublePanic;
        panic!("first panic");
    }
    for _ in 0..1000 {
        let log = Arc::new(Mutex::new(Vec::new()));
        let result = std::panic::catch_unwind({
            let log = log.clone();
            move || deep(log, 16)
        });
        assert_eq!(*result.unwrap_err().downcast::<u64>().unwrap(), 55);
        assert_eq!(*log.lock().unwrap(), (0..=16).collect::<Vec<_>>());
    }
    let log = Arc::new(Mutex::new(Vec::new()));
    let resumed = std::panic::catch_unwind({
        let log = log.clone();
        move || {
            let _outer = Guard(log.clone(), 1);
            let caught = std::panic::catch_unwind(move || {
                let _inner = Guard(log, 2);
                panic!("resume");
            });
            std::panic::resume_unwind(caught.unwrap_err());
        }
    });
    assert!(resumed.is_err());
    assert_eq!(*log.lock().unwrap(), vec![2, 1]);
    let mutex = Mutex::new(0);
    assert!(std::panic::catch_unwind(|| {
        let mut guard = mutex.lock().unwrap();
        *guard = 42;
        panic!("locked");
    })
    .is_err());
    assert_eq!(**mutex.lock().unwrap_err().get_ref(), 42);
    let thread = std::thread::spawn(|| std::panic::panic_any(777u64));
    assert_eq!(*thread.join().unwrap_err().downcast::<u64>().unwrap(), 777);
    let dynamic: Box<dyn Fn() + std::panic::UnwindSafe> = Box::new(|| panic!("dynamic"));
    assert!(std::panic::catch_unwind(dynamic).is_err());
    println!("PASS: 1000 deep unwinds, resume_unwind, mutex cleanup/poisoning, thread join, dynamic call");
}
