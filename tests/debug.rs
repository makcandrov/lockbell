//! `Debug` must never ring the bell.
//!
//! `RwLockBell` is a newtype rather than an alias over `lock_api::RwLock`
//! precisely so that formatting can use a shared lock that bypasses the
//! callback machinery. With the alias, `{:?}` went through `try_read()` and
//! could run a queued callback — which meant it could block on a concurrent
//! `try_write_or_else` factory and could panic with that callback's payload.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering::Relaxed},
    },
    thread,
    time::{Duration, Instant},
};

use lockbell::RwLockBell;

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
fn test_debug_does_not_run_queued_callbacks() {
    let lock = Arc::new(RwLockBell::new(0u64));
    let called = Arc::new(AtomicBool::new(false));
    let called2 = called.clone();

    let r = lock.read();
    assert!(
        lock.try_write_or(move || called2.store(true, Relaxed))
            .is_none()
    );

    for _ in 0..100 {
        let _ = format!("{lock:?}");
    }
    assert!(!called.load(Relaxed), "formatting must not drain the queue");

    drop(r);
    assert!(called.load(Relaxed), "the real guard drop still drains");
}

#[test]
fn test_debug_does_not_disturb_the_reader_count() {
    let lock = Arc::new(RwLockBell::new(0u64));
    let called = Arc::new(AtomicBool::new(false));
    let called2 = called.clone();

    // Format between acquiring and releasing readers: the bell-free peek must
    // leave `readers` untouched, or the drain below would never fire.
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

#[test]
fn test_debug_never_runs_a_callback_or_waits_on_a_factory() {
    // With the old type alias this loop reliably ran a queued callback inside
    // `Debug::fmt` on the formatting thread — so it panicked with the
    // callback's payload — and blocked for the factory's full duration.
    const FACTORY: Duration = Duration::from_millis(50);

    let lock = Arc::new(RwLockBell::new(0u64));
    let stop = Arc::new(AtomicBool::new(false));
    let ran_here = Arc::new(AtomicBool::new(false));

    let formatter = thread::current().id();
    let lock_w = lock.clone();
    let stop_w = stop.clone();
    let ran_w = ran_here.clone();

    let writer = thread::spawn(move || {
        while !stop_w.load(Relaxed) {
            let ran = ran_w.clone();
            drop(lock_w.try_write_or_else(|| {
                // Long enough that a drainer waiting on it is unmistakable.
                thread::sleep(FACTORY);
                move || {
                    if thread::current().id() == formatter {
                        ran.store(true, Relaxed);
                    }
                }
            }));
        }
    });

    let mut worst = Duration::ZERO;
    let deadline = Instant::now() + Duration::from_millis(500);
    while Instant::now() < deadline {
        let start = Instant::now();
        let _ = format!("{lock:?}");
        worst = worst.max(start.elapsed());
    }

    stop.store(true, Relaxed);
    writer.join().unwrap();

    assert!(
        !ran_here.load(Relaxed),
        "Debug::fmt executed a queued callback on the formatting thread"
    );
    // Wall-clock only: under Miri a single `format!` already costs tens of ms.
    #[cfg(not(miri))]
    assert!(
        worst < FACTORY / 2,
        "Debug::fmt blocked for {worst:?}; it must not wait on a factory"
    );
    let _ = worst;
}
