use std::{
    panic::{self, AssertUnwindSafe},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering::Relaxed},
    },
};

use lockbell::RwLockBell;

// ─── read guard map ──────────────────────────────────────────────────────────

#[test]
fn test_map_read_guard() {
    let lock = RwLockBell::new((1u64, 2u64));
    let mapped = lock.read().map(|t| &t.0);
    assert_eq!(*mapped, 1);
}

#[test]
fn test_map_read_guard_fires_callbacks() {
    let lock = Arc::new(RwLockBell::new((1u64, 2u64)));
    let called = Arc::new(AtomicBool::new(false));
    let called2 = called.clone();

    let mapped = lock.read().map(|t| &t.1);
    assert!(
        lock.try_write_or(move || called2.store(true, Relaxed))
            .is_none()
    );

    assert!(!called.load(Relaxed));
    drop(mapped);
    assert!(called.load(Relaxed));
}

#[test]
fn test_try_map_read_guard_success() {
    let lock = RwLockBell::new((1u64, 2u64));
    let mapped = lock.read().try_map(|t| Some(&t.0));
    assert!(mapped.is_ok());
    assert_eq!(*mapped.unwrap(), 1);
}

#[test]
fn test_try_map_read_guard_failure() {
    let lock = RwLockBell::new((1u64, 2u64));
    let r = lock.read();
    let result = r.try_map(|_| None::<&u64>);
    assert!(result.is_err());
    // The original guard is returned.
    let original = result.unwrap_err();
    assert_eq!(*original, (1, 2));
}

#[test]
fn test_try_map_read_guard_failure_still_fires_callbacks() {
    let lock = Arc::new(RwLockBell::new((1u64, 2u64)));
    let called = Arc::new(AtomicBool::new(false));
    let called2 = called.clone();

    let r = lock.read();
    assert!(
        lock.try_write_or(move || called2.store(true, Relaxed))
            .is_none()
    );

    let result = r.try_map(|_| None::<&u64>);
    let original = result.unwrap_err();
    assert!(!called.load(Relaxed), "callback must not fire yet");
    drop(original);
    assert!(called.load(Relaxed), "callback must fire after drop");
}

#[test]
fn test_try_map_or_err_read_guard_success() {
    let lock = RwLockBell::new((1u64, 2u64));
    let mapped = lock.read().try_map_or_err(|t| Ok::<_, ()>(&t.1));
    assert!(mapped.is_ok());
    assert_eq!(*mapped.unwrap(), 2);
}

#[test]
fn test_try_map_or_err_read_guard_failure() {
    let lock = RwLockBell::new((1u64, 2u64));
    let r = lock.read();
    let result = r.try_map_or_err(|_| Err::<&u64, _>("oops"));
    assert!(result.is_err());
    let (original, err) = result.unwrap_err();
    assert_eq!(err, "oops");
    assert_eq!(*original, (1, 2));
}

// ─── write guard map ─────────────────────────────────────────────────────────

#[test]
fn test_map_write_guard() {
    let lock = RwLockBell::new((1u64, 2u64));
    let mut mapped = lock.write().map(|t| &mut t.0);
    *mapped = 99;
    drop(mapped);
    assert_eq!(lock.read().0, 99);
}

#[test]
fn test_map_write_guard_fires_callbacks() {
    let lock = Arc::new(RwLockBell::new((1u64, 2u64)));
    let called = Arc::new(AtomicBool::new(false));
    let called2 = called.clone();

    let mapped = lock.write().map(|t| &mut t.1);
    assert!(
        lock.try_write_or(move || called2.store(true, Relaxed))
            .is_none()
    );

    assert!(!called.load(Relaxed));
    drop(mapped);
    assert!(called.load(Relaxed));
}

#[test]
fn test_try_map_write_guard_success() {
    let lock = RwLockBell::new((1u64, 2u64));
    let mapped = lock.write().try_map(|t| Some(&mut t.0));
    assert!(mapped.is_ok());
    let mut m = mapped.unwrap();
    *m = 42;
    drop(m);
    assert_eq!(lock.read().0, 42);
}

#[test]
fn test_try_map_write_guard_failure() {
    let lock = RwLockBell::new((1u64, 2u64));
    let w = lock.write();
    let result = w.try_map(|_| None::<&mut u64>);
    assert!(result.is_err());
    let original = result.unwrap_err();
    assert_eq!(*original, (1, 2));
}

#[test]
fn test_try_map_write_guard_failure_still_fires_callbacks() {
    let lock = Arc::new(RwLockBell::new((1u64, 2u64)));
    let called = Arc::new(AtomicBool::new(false));
    let called2 = called.clone();

    let w = lock.write();
    assert!(
        lock.try_write_or(move || called2.store(true, Relaxed))
            .is_none()
    );

    let result = w.try_map(|_| None::<&mut u64>);
    let original = result.unwrap_err();
    assert!(!called.load(Relaxed));
    drop(original);
    assert!(called.load(Relaxed));
}

#[test]
fn test_try_map_err_write_guard_success() {
    let lock = RwLockBell::new((1u64, 2u64));
    let mapped = lock.write().try_map_err(|t| Ok::<_, ()>(&mut t.1));
    assert!(mapped.is_ok());
    let mut m = mapped.unwrap();
    assert_eq!(*m, 2);
    *m = 77;
    drop(m);
    assert_eq!(lock.read().1, 77);
}

#[test]
fn test_try_map_err_write_guard_failure() {
    let lock = RwLockBell::new((1u64, 2u64));
    let w = lock.write();
    let result = w.try_map_err(|_| Err::<&mut u64, _>("oops"));
    assert!(result.is_err());
    let (original, err) = result.unwrap_err();
    assert_eq!(err, "oops");
    assert_eq!(*original, (1, 2));
}

// ─── panic safety regressions ────────────────────────────────────────────────
//
// These verify that a panic in the user-supplied projection closure does not
// corrupt the lock's internal state.
//
// Today, every `map`-family method does:
//
//     let guard = self.guard.take().unwrap();
//     forget(self);
//     // (release of state bookkeeping is the responsibility of the *new* mapped
//     //  guard, which is only ever constructed if `f` returns successfully)
//     let mapped = RwLockReadGuard::map(guard, f);   // f may panic here
//
// If `f` panics, parking_lot's `map` drops its `s` parameter (releasing the
// underlying lock) but we never build the mapped guard, so:
//   - read side : `state.readers` is permanently leaked → the read-drop drain
//                 path can never reach `readers == 0` again → callbacks queued
//                 via `try_write_or` may stop firing forever.
//   - write side: the write-drop drain is skipped entirely → callbacks queued
//                 while this write guard was held are not flushed, even though
//                 the lock has actually been released.

// ─── read side: panic must not leak the `state.readers` counter ──────────────

#[test]
fn regression_read_map_panic_does_not_leak_readers() {
    let lock = Arc::new(RwLockBell::new((1u64, 2u64)));

    let r = lock.read();
    let result = panic::catch_unwind(AssertUnwindSafe(move || {
        let _ = r.map(|_| -> &u64 { panic!("intentional panic in map closure") });
    }));
    assert!(result.is_err(), "panic must propagate to caller");

    // If `state.readers` was leaked, the next read-guard drop will see
    // `readers > 0` and bail without draining — even though no other reader
    // exists.
    let called = Arc::new(AtomicBool::new(false));
    let called2 = called.clone();

    let r2 = lock.read();
    assert!(
        lock.try_write_or(move || called2.store(true, Relaxed))
            .is_none()
    );
    drop(r2);
    assert!(
        called.load(Relaxed),
        "callback must fire when the last read guard drops; \
         state.readers must not have been leaked by the panic"
    );
}

#[test]
fn regression_read_try_map_panic_does_not_leak_readers() {
    let lock = Arc::new(RwLockBell::new((1u64, 2u64)));

    let r = lock.read();
    let result = panic::catch_unwind(AssertUnwindSafe(move || {
        let _ = r.try_map(|_| -> Option<&u64> { panic!("intentional") });
    }));
    assert!(result.is_err());

    let called = Arc::new(AtomicBool::new(false));
    let called2 = called.clone();

    let r2 = lock.read();
    assert!(
        lock.try_write_or(move || called2.store(true, Relaxed))
            .is_none()
    );
    drop(r2);
    assert!(
        called.load(Relaxed),
        "callback must fire after last read drops"
    );
}

#[test]
fn regression_read_try_map_or_err_panic_does_not_leak_readers() {
    let lock = Arc::new(RwLockBell::new((1u64, 2u64)));

    let r = lock.read();
    let result = panic::catch_unwind(AssertUnwindSafe(move || {
        let _ = r.try_map_or_err(|_| -> Result<&u64, ()> { panic!("intentional") });
    }));
    assert!(result.is_err());

    let called = Arc::new(AtomicBool::new(false));
    let called2 = called.clone();

    let r2 = lock.read();
    assert!(
        lock.try_write_or(move || called2.store(true, Relaxed))
            .is_none()
    );
    drop(r2);
    assert!(
        called.load(Relaxed),
        "callback must fire after last read drops"
    );
}

// ─── write side: panic must still drain pending callbacks ────────────────────

#[test]
fn regression_write_map_panic_drains_pending_callbacks() {
    let lock = Arc::new(RwLockBell::new((1u64, 2u64)));
    let called = Arc::new(AtomicBool::new(false));
    let called2 = called.clone();

    let w = lock.write();
    assert!(
        lock.try_write_or(move || called2.store(true, Relaxed))
            .is_none()
    );

    let result = panic::catch_unwind(AssertUnwindSafe(move || {
        let _ = w.map(|_| -> &mut u64 { panic!("intentional") });
    }));
    assert!(result.is_err());

    // The parking_lot write lock was released as part of the unwind, so
    // contention has cleared — the queued callback must have fired.
    assert!(
        called.load(Relaxed),
        "callbacks queued under the panicked write guard must be drained \
         when the lock is released"
    );
}

#[test]
fn regression_write_try_map_panic_drains_pending_callbacks() {
    let lock = Arc::new(RwLockBell::new((1u64, 2u64)));
    let called = Arc::new(AtomicBool::new(false));
    let called2 = called.clone();

    let w = lock.write();
    assert!(
        lock.try_write_or(move || called2.store(true, Relaxed))
            .is_none()
    );

    let result = panic::catch_unwind(AssertUnwindSafe(move || {
        let _ = w.try_map(|_| -> Option<&mut u64> { panic!("intentional") });
    }));
    assert!(result.is_err());

    assert!(
        called.load(Relaxed),
        "queued callback must fire after the write guard's lock is released by unwind"
    );
}

#[test]
fn regression_write_try_map_err_panic_drains_pending_callbacks() {
    let lock = Arc::new(RwLockBell::new((1u64, 2u64)));
    let called = Arc::new(AtomicBool::new(false));
    let called2 = called.clone();

    let w = lock.write();
    assert!(
        lock.try_write_or(move || called2.store(true, Relaxed))
            .is_none()
    );

    let result = panic::catch_unwind(AssertUnwindSafe(move || {
        let _ = w.try_map_err(|_| -> Result<&mut u64, ()> { panic!("intentional") });
    }));
    assert!(result.is_err());

    assert!(
        called.load(Relaxed),
        "queued callback must fire after the write guard's lock is released by unwind"
    );
}
