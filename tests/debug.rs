//! `Debug` rings the bell.
//!
//! Formatting takes a real shared lock — `try_read()`, the same call
//! `parking_lot::RwLock` formats through — so it counts as a reader and its
//! release drains like any other. That makes `{:?}` a lock acquisition rather
//! than a free observation: it can run a callback, wait out a concurrent
//! `try_write_or_else` factory, and panic with a callback's payload.
//!
//! The alternative, a shared lock the bell could not see, stranded any
//! `try_write_or` that lost to a transient `{:?}`.

use std::{
    fmt,
    panic::{self, AssertUnwindSafe},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering::Relaxed},
    },
    thread::{self, ThreadId},
};

use lockbell::RwLockBell;

#[derive(Default)]
struct Gate(Mutex<bool>, Condvar);

impl Gate {
    fn open(&self) {
        *self.0.lock().unwrap() = true;
        self.1.notify_all();
    }

    fn wait(&self) {
        let mut open = self.0.lock().unwrap();
        while !*open {
            open = self.1.wait(open).unwrap();
        }
    }
}

/// Parks inside `Debug::fmt`, so the read guard formatting takes can be made to
/// outlive every other reader — which is what turns the format into the last
/// reader out, and therefore into the thread that drains.
struct Blocker {
    entered: Arc<Gate>,
    release: Arc<Gate>,
}

impl fmt::Debug for Blocker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.entered.open();
        self.release.wait();
        f.write_str("blocked")
    }
}

fn blocker() -> (Arc<RwLockBell<Blocker>>, Arc<Gate>, Arc<Gate>) {
    let entered = Arc::new(Gate::default());
    let release = Arc::new(Gate::default());
    let lock = Arc::new(RwLockBell::new(Blocker {
        entered: entered.clone(),
        release: release.clone(),
    }));
    (lock, entered, release)
}

#[test]
fn test_debug_shows_data_when_unlocked() {
    let lock = RwLockBell::new(42u64);
    assert_eq!(format!("{lock:?}"), "RwLockBell { data: 42 }");
}

#[test]
fn test_debug_shows_locked_under_a_writer() {
    let lock = RwLockBell::new(42u64);
    let _w = lock.write();
    assert_eq!(format!("{lock:?}"), "RwLockBell { data: <locked> }");
}

#[test]
fn test_debug_shows_data_under_a_reader() {
    let lock = RwLockBell::new(42u64);
    let _r = lock.read();
    assert_eq!(format!("{lock:?}"), "RwLockBell { data: 42 }");
}

#[test]
fn test_debug_does_not_ring_while_another_reader_holds() {
    let lock = Arc::new(RwLockBell::new(0u64));
    let called = Arc::new(AtomicBool::new(false));
    let called2 = called.clone();

    let r = lock.read();
    assert!(
        lock.try_write_or(move || called2.store(true, Relaxed))
            .is_none()
    );

    // Each format is a reader, but never the last one, so it defers the drain
    // to `r` exactly as any other reader would.
    for _ in 0..100 {
        let _ = format!("{lock:?}");
    }
    assert!(!called.load(Relaxed), "formatting is not the last reader");

    drop(r);
    assert!(called.load(Relaxed), "the last guard drop still drains");
}

#[test]
fn test_debug_does_not_disturb_the_reader_count() {
    let lock = Arc::new(RwLockBell::new(0u64));
    let called = Arc::new(AtomicBool::new(false));
    let called2 = called.clone();

    // Format between acquiring and releasing readers: each one must leave
    // `readers` exactly as it found it, or the drain below would fire early or
    // never fire at all.
    let r1 = lock.read();
    let _ = format!("{lock:?}");
    let r2 = lock.read();
    let _ = format!("{lock:?}");

    assert!(
        lock.try_write_or(move || called2.store(true, Relaxed))
            .is_none()
    );

    drop(r1);
    let _ = format!("{lock:?}");
    assert!(!called.load(Relaxed), "one reader is still live");

    drop(r2);
    assert!(called.load(Relaxed), "reader count must have reached zero");
}

/// The point of the whole thing: a format that is the last reader out drains
/// the queue and runs the callbacks itself.
#[test]
fn test_debug_rings_the_bell_when_it_is_the_last_reader() {
    let (lock, entered, release) = blocker();

    let r = lock.read();
    let rang_on: Arc<Mutex<Option<ThreadId>>> = Arc::new(Mutex::new(None));
    let rang_on2 = rang_on.clone();
    assert!(
        lock.try_write_or(move || *rang_on2.lock().unwrap() = Some(thread::current().id()))
            .is_none(),
        "a reader holds, so this must queue"
    );

    let lock_f = lock.clone();
    let formatter = thread::spawn(move || (thread::current().id(), format!("{lock_f:?}")));

    entered.wait(); // the format holds a reader of its own now
    drop(r); // readers 2 -> 1: defers the drain to the format
    assert!(
        rang_on.lock().unwrap().is_none(),
        "dropping a non-last reader must not ring"
    );
    release.open();

    let (id, out) = formatter.join().unwrap();
    assert_eq!(out, "RwLockBell { data: blocked }");
    assert_eq!(
        *rang_on.lock().unwrap(),
        Some(id),
        "the formatting thread must have rung the bell"
    );
}

/// And it carries that callback's panic, like any other draining release.
#[test]
fn test_debug_propagates_a_callback_panic() {
    let (lock, entered, release) = blocker();

    let r = lock.read();
    assert!(lock.try_write_or(|| panic!("callback boom")).is_none());

    let lock_f = lock.clone();
    let formatter = thread::spawn(move || {
        panic::catch_unwind(AssertUnwindSafe(|| format!("{lock_f:?}"))).is_err()
    });

    entered.wait();
    drop(r);
    release.open();

    assert!(
        formatter.join().unwrap(),
        "the callback's panic must propagate out of `fmt`"
    );
    // The raw lock is released before the drain, so the panic strands nothing.
    assert!(lock.try_write().is_some(), "lock stranded by the panic");
}
