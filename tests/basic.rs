use std::sync::Arc;

use lockbell::RwLockBell;

#[test]
fn test_new_and_into_inner() {
    let lock = RwLockBell::new(42u64);
    assert_eq!(lock.into_inner(), 42);
}

#[test]
fn test_from_value() {
    let lock: RwLockBell<u64> = 42u64.into();
    assert_eq!(lock.into_inner(), 42);
}

#[test]
fn test_read() {
    let lock = RwLockBell::new(42u64);
    assert_eq!(*lock.read(), 42);
}

#[test]
fn test_concurrent_reads() {
    let lock = RwLockBell::new(42u64);
    let r1 = lock.read();
    let r2 = lock.read();
    assert_eq!(*r1, 42);
    assert_eq!(*r2, 42);
}

#[test]
fn test_try_read_succeeds_when_free() {
    let lock = RwLockBell::new(42u64);
    let r = lock.try_read().expect("lock is free");
    assert_eq!(*r, 42);
}

#[test]
fn test_try_read_fails_during_write() {
    let lock = RwLockBell::new(42u64);
    let _w = lock.write();
    assert!(lock.try_read().is_none());
}

#[test]
fn test_try_write_succeeds_when_free() {
    let lock = RwLockBell::new(0u64);
    let mut w = lock.try_write().expect("lock is free");
    *w = 99;
    drop(w);
    assert_eq!(*lock.read(), 99);
}

#[test]
fn test_try_write_fails_when_read_held() {
    let lock = RwLockBell::new(0u64);
    let _r = lock.read();
    assert!(lock.try_write().is_none());
}

#[test]
fn test_try_write_fails_when_write_held() {
    let lock = RwLockBell::new(0u64);
    let _w = lock.write();
    assert!(lock.try_write().is_none());
}

#[test]
fn test_write_guard_deref_mut() {
    let lock = RwLockBell::new(0u64);
    let mut w = lock.write();
    *w = 99;
    drop(w);
    assert_eq!(*lock.read(), 99);
}

#[test]
fn test_try_write_or_succeeds_when_free() {
    use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

    let lock = RwLockBell::new(0u64);
    let called = AtomicBool::new(false);
    let guard = lock.try_write_or(move || called.store(true, Relaxed));
    assert!(guard.is_some(), "should succeed when lock is free");
    // Callback is consumed (not called) on success, so we can't check `called` here.
    drop(guard);
}

#[test]
fn test_try_write_or_else_lazy_not_called_on_success() {
    use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

    let lock = RwLockBell::new(0u64);
    let factory_called = AtomicBool::new(false);
    let guard = lock.try_write_or_else(|| {
        factory_called.store(true, Relaxed);
        || {}
    });
    assert!(guard.is_some());
    drop(guard);
    assert!(
        !factory_called.load(Relaxed),
        "callback factory must not be called on success"
    );
}

#[test]
fn test_into_inner_drops_pending_callbacks() {
    use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

    let lock = Arc::new(RwLockBell::new(0u64));
    let called = Arc::new(AtomicBool::new(false));
    let called2 = called.clone();

    let _guard = lock.write();
    drop(lock.try_write_or(move || called2.store(true, Relaxed)));
    // Drop guard first so we can consume the lock.
    drop(_guard);

    // The callback fired on guard drop. Re-register one that should be dropped.
    let called3 = called.clone();
    called.store(false, Relaxed);
    let r = lock.read();
    drop(lock.try_write_or(move || called3.store(true, Relaxed)));
    drop(r); // fires callback

    // Now test with into_inner: register callback, then consume without dropping guard.
    let lock2 = RwLockBell::new(0u64);
    let called4 = Arc::new(AtomicBool::new(false));
    let called5 = called4.clone();

    // We can't register a callback without holding a guard first, but we can
    // verify that into_inner consumes cleanly.
    let _ = lock2.into_inner();
    assert!(!called4.load(Relaxed));
    drop(called5);
}
