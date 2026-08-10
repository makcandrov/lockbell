//! Guards and callbacks obtained through an `Arc`, with no lifetime attached.

#![cfg(feature = "arc_lock")]

use std::sync::Arc;

use lockbell::RwLockBell;

#[test]
fn test_read_arc_and_write_arc() {
    let lock = Arc::new(RwLockBell::new(0u64));

    let mut w = lock.write_arc();
    *w = 42;
    drop(w);

    let r = lock.read_arc();
    assert_eq!(*r, 42);
}

#[test]
fn test_arc_guards_outlive_the_borrow() {
    // The whole point of the Arc guards: no lifetime, so they can be stored
    // in an owning value that keeps the allocation alive by itself.
    struct Owned(lockbell::ArcRwLockBellReadGuard<u64>);

    let owned = {
        let lock = Arc::new(RwLockBell::new(7u64));
        Owned(lock.read_arc())
        // `lock` goes out of scope here; the guard keeps the lock alive.
    };
    assert_eq!(*owned.0, 7);
}

#[test]
fn test_owning_struct_dropped_as_last_reference() {
    // The same shape, but the owning struct is dropped *inside a call* while
    // its guard is the last reference to the lock. Building this out of a
    // borrowed guard with a laundered `'static` lifetime would be undefined
    // behaviour: the reference such a guard stores into the allocation is
    // retagged for the duration of the call, and freeing the allocation
    // inside that call violates the protector. An Arc guard stores no
    // reference, so this is well-defined.
    //
    // Checked with:
    //   cargo +nightly miri test --test basic
    //   MIRIFLAGS=-Zmiri-tree-borrows cargo +nightly miri test --test basic
    struct Owned {
        guard: lockbell::ArcRwLockBellReadGuard<u64>,
    }

    let owned = {
        let lock = Arc::new(RwLockBell::new(3u64));
        Owned {
            guard: lock.read_arc(),
        }
    };

    assert_eq!(*owned.guard, 3);
    drop(owned);
}

#[test]
fn test_arc_read_guard_rings_the_bell() {
    use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

    let lock = Arc::new(RwLockBell::new(0u64));
    let called = Arc::new(AtomicBool::new(false));
    let called2 = called.clone();

    let r = lock.read_arc();
    assert!(
        lock.try_write_or(move || called2.store(true, Relaxed))
            .is_none()
    );

    assert!(!called.load(Relaxed));
    drop(r);
    assert!(called.load(Relaxed), "last Arc reader must drain callbacks");
}

#[test]
fn test_arc_write_guard_rings_the_bell() {
    use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

    let lock = Arc::new(RwLockBell::new(0u64));
    let called = Arc::new(AtomicBool::new(false));
    let called2 = called.clone();

    let w = lock.write_arc();
    assert!(
        lock.try_write_or(move || called2.store(true, Relaxed))
            .is_none()
    );

    assert!(!called.load(Relaxed));
    drop(w);
    assert!(called.load(Relaxed), "Arc writer must drain callbacks");
}

#[test]
fn test_arc_and_borrowed_readers_share_one_count() {
    // Both guard flavours feed the same reader counter, so the drain only
    // happens once the last of them is gone.
    use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

    let lock = Arc::new(RwLockBell::new(0u64));
    let called = Arc::new(AtomicBool::new(false));
    let called2 = called.clone();

    let arc_reader = lock.read_arc();
    let borrowed_reader = lock.read();
    assert!(
        lock.try_write_or(move || called2.store(true, Relaxed))
            .is_none()
    );

    drop(arc_reader);
    assert!(!called.load(Relaxed), "a reader is still holding the lock");

    drop(borrowed_reader);
    assert!(called.load(Relaxed));
}

#[test]
fn test_arc_guards_account_for_exactly_one_strong_reference() {
    // `RwLockBell` reinterprets `Arc<Self>` as an `Arc` of the inner lock to
    // reach lock_api's Arc-guard constructors. Each guard must add exactly one
    // strong count, and give it back on drop.
    let lock = Arc::new(RwLockBell::new(0u64));
    assert_eq!(Arc::strong_count(&lock), 1);

    let r1 = lock.read_arc();
    assert_eq!(Arc::strong_count(&lock), 2);
    let r2 = lock.try_read_arc().expect("shared access");
    assert_eq!(Arc::strong_count(&lock), 3);
    drop((r1, r2));
    assert_eq!(Arc::strong_count(&lock), 1);

    let w = lock.write_arc();
    assert_eq!(Arc::strong_count(&lock), 2);
    drop(w);
    assert_eq!(Arc::strong_count(&lock), 1);

    let w = lock.try_write_arc().expect("lock is free");
    assert_eq!(Arc::strong_count(&lock), 2);
    drop(w);

    let w = lock.try_write_arc_or(|| {}).expect("lock is free");
    assert_eq!(Arc::strong_count(&lock), 2);
    drop(w);
    assert_eq!(Arc::strong_count(&lock), 1);

    // The failure path must not leak a count either.
    let held = lock.write();
    assert!(lock.try_write_arc_or(|| {}).is_none());
    assert!(lock.try_read_arc().is_none());
    drop(held);
    assert_eq!(Arc::strong_count(&lock), 1);
}

#[test]
fn test_arc_write_guard_is_last_reference() {
    // The guard is the sole owner when the handle goes away: dropping it must
    // release the lock and then free the allocation, in that order.
    let guard = {
        let lock = Arc::new(RwLockBell::new(1u64));
        lock.write_arc()
    };
    drop(guard);
}

#[test]
fn test_try_write_arc_or_success_returns_arc_guard() {
    use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

    let lock = Arc::new(RwLockBell::new(0u64));
    let called = Arc::new(AtomicBool::new(false));
    let called2 = called.clone();

    let mut w = lock
        .try_write_arc_or(move || called2.store(true, Relaxed))
        .expect("lock is free");
    *w = 5;
    drop(w);

    assert!(!called.load(Relaxed), "callback is discarded on success");
    assert_eq!(*lock.read(), 5);
}

#[test]
fn test_try_write_arc_or_queues_on_contention() {
    use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

    let lock = Arc::new(RwLockBell::new(0u64));
    let called = Arc::new(AtomicBool::new(false));
    let called2 = called.clone();

    let held = lock.write();
    assert!(
        lock.try_write_arc_or(move || called2.store(true, Relaxed))
            .is_none()
    );

    assert!(!called.load(Relaxed));
    drop(held);
    assert!(called.load(Relaxed));
}

#[test]
fn test_try_write_arc_or_guard_has_no_lifetime() {
    // The reason this method exists: the guard outlives the `Arc` handle.
    struct Owned(lockbell::ArcRwLockBellWriteGuard<u64>);

    let mut owned = {
        let lock = Arc::new(RwLockBell::new(1u64));
        Owned(lock.try_write_arc_or(|| {}).expect("lock is free"))
    };
    *owned.0 = 2;
    assert_eq!(*owned.0, 2);
}

#[test]
fn test_try_write_arc_or_else_lazy_not_called_on_success() {
    use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

    let lock = Arc::new(RwLockBell::new(0u64));
    let built = Arc::new(AtomicBool::new(false));
    let built2 = built.clone();

    let g = lock.try_write_arc_or_else(|| {
        built2.store(true, Relaxed);
        || {}
    });
    assert!(g.is_some());
    assert!(!built.load(Relaxed), "factory must not run on success");
}

#[test]
fn test_arc_and_borrowed_callbacks_share_one_queue() {
    use std::sync::{
        Mutex,
        atomic::{AtomicU64, Ordering::Relaxed},
    };

    let lock = Arc::new(RwLockBell::new(0u64));
    let order = Arc::new(Mutex::new(Vec::new()));
    let seq = Arc::new(AtomicU64::new(0));

    let held = lock.write();

    for i in 0..4u64 {
        let order = order.clone();
        let seq = seq.clone();
        let record = move || {
            let _ = seq.fetch_add(1, Relaxed);
            order.lock().unwrap().push(i);
        };
        // Alternate between the borrowed and the Arc entry point.
        if i % 2 == 0 {
            assert!(lock.try_write_or(record).is_none());
        } else {
            assert!(lock.try_write_arc_or(record).is_none());
        }
    }

    drop(held);
    assert_eq!(*order.lock().unwrap(), vec![0, 1, 2, 3], "one FIFO queue");
}
