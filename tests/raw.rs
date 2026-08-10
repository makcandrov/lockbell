//! The escape hatch.
//!
//! `RawRwLockBell` lets you drive the bell through `lock_api` directly. That
//! also reinstates the operations `RwLockBell` deliberately keeps away from it,
//! so these tests pin what reaching for it costs.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering::Relaxed},
};

use lockbell::RawRwLockBell;
use parking_lot::lock_api;

type RawLock<T> = lock_api::RwLock<RawRwLockBell, T>;

#[test]
fn test_raw_lock_is_usable_through_lock_api() {
    let lock = RawLock::new(0u64);

    let mut w = lock.write();
    *w = 7;
    drop(w);

    assert_eq!(*lock.read(), 7);
}

#[test]
fn test_raw_lock_registers_and_fires_callbacks() {
    let lock = Arc::new(RawLock::new(0u64));
    let fired = Arc::new(AtomicBool::new(false));
    let fired2 = fired.clone();

    let held = lock.write();

    // SAFETY: used only to run the bell protocol; the call does not acquire
    // the lock here because `held` still owns it.
    let raw = unsafe { lock.raw() };
    let acquired = raw.try_lock_exclusive_or_else(|| move || fired2.store(true, Relaxed));
    assert!(!acquired, "the lock is held, so the callback is queued");

    assert!(!fired.load(Relaxed));
    drop(held);
    assert!(fired.load(Relaxed), "releasing the lock rings the bell");
}

#[test]
fn test_raw_try_lock_exclusive_or_queues_on_contention() {
    let lock = Arc::new(RawLock::new(0u64));
    let fired = Arc::new(AtomicBool::new(false));
    let fired2 = fired.clone();

    let held = lock.write();

    // SAFETY: used only to run the bell protocol; the call does not acquire
    // the lock here because `held` still owns it.
    let raw = unsafe { lock.raw() };
    let acquired = raw.try_lock_exclusive_or(move || fired2.store(true, Relaxed));
    assert!(!acquired, "the lock is held, so the callback is queued");

    assert!(!fired.load(Relaxed));
    drop(held);
    assert!(fired.load(Relaxed), "releasing the lock rings the bell");
}

#[test]
fn test_raw_try_lock_exclusive_or_success_discards_the_callback() {
    let lock = RawLock::new(0u64);

    // SAFETY: on success we own the exclusive lock and release it below.
    let raw = unsafe { lock.raw() };
    assert!(raw.try_lock_exclusive_or(|| unreachable!()));

    assert!(lock.is_locked_exclusive());
    // SAFETY: the exclusive lock was acquired just above and never handed out.
    unsafe { lock.force_unlock_write() };
    assert!(!lock.is_locked());
}

/// Both entry points share one queue and one FIFO order.
#[test]
fn test_raw_or_and_or_else_interleave_in_registration_order() {
    let lock = Arc::new(RawLock::new(0u64));
    let order = Arc::new(std::sync::Mutex::new(Vec::new()));

    let held = lock.write();
    // SAFETY: used only to run the bell protocol; `held` still owns the lock,
    // so neither call acquires it.
    let raw = unsafe { lock.raw() };

    let o1 = order.clone();
    assert!(!raw.try_lock_exclusive_or(move || o1.lock().unwrap().push(1)));
    let o2 = order.clone();
    assert!(!raw.try_lock_exclusive_or_else(|| move || o2.lock().unwrap().push(2)));
    let o3 = order.clone();
    assert!(!raw.try_lock_exclusive_or(move || o3.lock().unwrap().push(3)));

    drop(held);
    assert_eq!(*order.lock().unwrap(), vec![1, 2, 3]);
}

#[test]
fn test_raw_try_lock_exclusive_or_else_success_takes_the_lock() {
    let lock = RawLock::new(0u64);

    // SAFETY: on success we own the exclusive lock and release it below.
    let raw = unsafe { lock.raw() };
    assert!(raw.try_lock_exclusive_or_else(|| || unreachable!()));

    assert!(lock.is_locked_exclusive());
    // SAFETY: the exclusive lock was acquired just above and never handed out.
    unsafe { lock.force_unlock_write() };
    assert!(!lock.is_locked());
}

#[test]
fn test_raw_lock_debug_goes_through_the_bell() {
    // Documented consequence of the escape hatch: lock_api's `Debug` probes
    // with `try_read()`, which runs the full release path. `RwLockBell` uses a
    // bell-free peek instead.
    let lock = RawLock::new(42u64);
    assert_eq!(format!("{lock:?}"), "RwLock { data: 42 }");
}

#[test]
fn test_wrapper_and_raw_agree_on_layout() {
    // `RwLockBell` is a `#[repr(transparent)]` newtype over exactly this type.
    assert_eq!(
        size_of::<lockbell::RwLockBell<u64>>(),
        size_of::<RawLock<u64>>()
    );
    assert_eq!(
        align_of::<lockbell::RwLockBell<u64>>(),
        align_of::<RawLock<u64>>()
    );
}
