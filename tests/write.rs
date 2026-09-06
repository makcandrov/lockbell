use std::{
    hint::spin_loop,
    panic::{self, AssertUnwindSafe},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
    },
    thread,
    time::Duration,
};

use lockbell::RwLockBell;
use parking_lot::Mutex;

#[test]
fn test_callback() {
    let lock_callback = Arc::new(RwLockBell::new(12u64));

    let step = Arc::new(AtomicU64::new(0));
    let callback_allowed = Arc::new(AtomicBool::new(false));

    thread::scope(|s| {
        let step_1 = step.clone();
        let step_2 = step.clone();

        let lock_callback_1 = lock_callback.clone();
        let lock_callback_2 = lock_callback.clone();

        let callback_allowed_1 = callback_allowed.clone();
        let callback_allowed_2 = callback_allowed.clone();

        s.spawn(move || {
            let guard = lock_callback_1.try_write_or(|| {}).unwrap();
            step_1.fetch_add(1, Relaxed);

            while step_1.load(Relaxed) != 2 {
                spin_loop();
            }

            callback_allowed_1.store(true, Relaxed);

            drop(guard);

            callback_allowed_1.store(false, Relaxed);
        });

        s.spawn(move || {
            while step_2.load(Relaxed) != 1 {
                spin_loop();
            }

            let r = lock_callback_2.try_write_or(move || {
                assert!(callback_allowed_2.load(Relaxed));
            });
            assert!(r.is_none());
            step_2.fetch_add(1, Relaxed);
        });
    });
}

#[test]
fn test_multiple_callbacks() {
    let lock = Arc::new(RwLockBell::new(0u64));
    let order = Arc::new(Mutex::new(Vec::new()));

    let guard = lock.try_write_or(|| {}).unwrap();

    for i in 0..5u64 {
        let order = order.clone();
        drop(lock.try_write_or(move || order.lock().push(i)));
    }

    drop(guard);
    assert_eq!(*order.lock(), vec![0, 1, 2, 3, 4]);
}

#[test]
fn test_callback_can_call_try_write() {
    let lock = Arc::new(RwLockBell::new(0u64));
    let lock2 = lock.clone();

    let guard = lock.try_write_or(|| {}).unwrap();
    drop(lock.try_write_or(move || {
        drop(lock2.try_write_or(|| {}));
    }));
    drop(guard);
}

#[test]
fn test_write_triggers_callbacks() {
    let lock = Arc::new(RwLockBell::new(0u64));

    let called = Arc::new(AtomicBool::new(false));
    let called2 = called.clone();

    let guard = lock.write();
    drop(lock.try_write_or(move || called2.store(true, Relaxed)));
    drop(guard);

    assert!(called.load(Relaxed));
}

#[test]
fn test_callback_panic_does_not_skip_subsequent() {
    let lock = Arc::new(RwLockBell::new(0u64));

    let called = Arc::new(AtomicBool::new(false));
    let called2 = called.clone();

    let guard = lock.try_write().unwrap();
    drop(lock.try_write_or(|| panic!("intentional panic in callback")));
    drop(lock.try_write_or(move || called2.store(true, Relaxed)));

    let result = panic::catch_unwind(AssertUnwindSafe(|| drop(guard)));
    assert!(result.is_err(), "panic should have been re-raised");
    assert!(called.load(Relaxed));
}

// A zero-sized payload avoids allocating anything in the deliberately leaked
// secondary payload. Dropping it again would just keep producing more panics.
struct PanickingPayload;

impl Drop for PanickingPayload {
    fn drop(&mut self) {
        panic::panic_any(Self);
    }
}

#[test]
fn regression_payload_drop_panic_preserves_first_panic_and_remaining_callbacks() {
    struct DroppedPayload(Arc<AtomicBool>);
    impl Drop for DroppedPayload {
        fn drop(&mut self) {
            self.0.store(true, Relaxed);
        }
    }

    let lock = RwLockBell::new(0u64);
    let dropped = Arc::new(AtomicBool::new(false));
    let called = Arc::new(AtomicBool::new(false));
    let payload = DroppedPayload(dropped.clone());
    let called2 = called.clone();
    let guard = lock.write();

    assert!(
        lock.try_write_or(|| panic!("first callback panic"))
            .is_none()
    );
    assert!(
        lock.try_write_or(move || panic::panic_any(payload))
            .is_none()
    );
    assert!(
        lock.try_write_or(|| panic::panic_any(PanickingPayload))
            .is_none()
    );
    assert!(
        lock.try_write_or(move || called2.store(true, Relaxed))
            .is_none()
    );

    let result = panic::catch_unwind(AssertUnwindSafe(|| drop(guard)));
    let first = result.unwrap_err().downcast::<&'static str>().unwrap();
    assert_eq!(*first, "first callback panic");
    assert!(
        dropped.load(Relaxed),
        "ordinary suppressed payloads must be dropped"
    );
    assert!(
        called.load(Relaxed),
        "a payload destructor must not skip callbacks"
    );
    assert!(lock.try_write().is_some(), "the lock must remain usable");
}

#[test]
fn regression_payload_drop_panic_during_outer_unwind_does_not_abort() {
    let lock = RwLockBell::new(0u64);
    let called = Arc::new(AtomicBool::new(false));
    let called2 = called.clone();

    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        // Exercise the last-reader drain as well as the write drain above.
        let _guard = lock.read();
        assert!(
            lock.try_write_or(|| panic::panic_any(PanickingPayload))
                .is_none()
        );
        assert!(
            lock.try_write_or(move || called2.store(true, Relaxed))
                .is_none()
        );
        panic!("outer user panic");
    }));

    let outer = result.unwrap_err().downcast::<&'static str>().unwrap();
    assert_eq!(*outer, "outer user panic");
    assert!(called.load(Relaxed));
    assert!(lock.try_write().is_some(), "the lock must remain usable");
}

#[test]
fn test_write_guard_mutates_value() {
    let lock = RwLockBell::new(0u64);
    {
        let mut w = lock.write();
        *w = 42;
    }
    assert_eq!(*lock.read(), 42);
}

#[test]
fn test_callback_sees_updated_value() {
    let lock = Arc::new(RwLockBell::new(0u64));
    let lock2 = lock.clone();
    let seen = Arc::new(AtomicU64::new(0));
    let seen2 = seen.clone();

    let mut guard = lock.write();
    *guard = 99;
    drop(lock.try_write_or(move || {
        seen2.store(*lock2.read(), Relaxed);
    }));
    drop(guard);

    assert_eq!(seen.load(Relaxed), 99);
}

/// Spawns a watchdog that aborts the process if `stop` isn't set within `dur`.
/// Returns the watchdog handle and the stop flag — caller is responsible for
/// setting the flag and joining when its work is complete.
///
/// Used to flag potential deadlocks in tests for guards that are `!Send` and
/// therefore can't be dropped from a side thread.
fn spawn_watchdog(label: &'static str, dur: Duration) -> (thread::JoinHandle<()>, Arc<AtomicBool>) {
    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = stop.clone();
    let watchdog = thread::spawn(move || {
        let step = Duration::from_millis(50);
        let mut waited = Duration::ZERO;
        while waited < dur {
            if stop2.load(Relaxed) {
                return;
            }
            thread::sleep(step);
            waited += step;
        }
        eprintln!("[{label}] WATCHDOG fired — deadlock regression");
        std::process::abort();
    });
    (watchdog, stop)
}

/// Regression for the `try_write_or_else` factory-panic bug.
///
/// Previously, the factory was invoked between `locking += 1` and
/// `locking -= 1` while holding the state mutex. A panic from the factory
/// unwound out of the function without decrementing `locking`, permanently
/// breaking the lock — any subsequent guard drop would hang in
/// `while inner.locking != 0`.
///
/// The fix uses a drop guard so `locking` is always decremented, even on
/// unwind. This test verifies that after a panicking factory the lock is
/// fully usable.
#[test]
fn regression_try_write_or_else_factory_panic_does_not_leak_locking() {
    let lock = Arc::new(RwLockBell::new(0u64));

    // Hold a write guard so try_write_or_else takes the failure path.
    let guard = lock.write();

    // Panicking factory; catch the panic.
    let lock2 = lock.clone();
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        drop(lock2.try_write_or_else(|| -> fn() {
            panic!("factory panic");
        }));
    }));
    assert!(result.is_err(), "factory panic must propagate to caller");

    // Bounded watchdog: with the bug, drop(guard) hangs forever. The guard
    // is `!Send`, so we have to drop it on this thread and use a watchdog
    // thread to flag the hang.
    let (watchdog, stop) = spawn_watchdog(
        "regression_try_write_or_else_factory_panic_does_not_leak_locking",
        Duration::from_secs(5),
    );
    drop(guard);
    stop.store(true, Relaxed);
    watchdog.join().unwrap();

    // The lock must still be fully usable.
    let mut w = lock.write();
    *w = 42;
    drop(w);
    assert_eq!(*lock.read(), 42);

    // And try_write_or must still queue + fire callbacks normally.
    let fired = Arc::new(AtomicBool::new(false));
    let fired2 = fired.clone();
    let w = lock.write();
    assert!(
        lock.try_write_or(move || fired2.store(true, Relaxed))
            .is_none()
    );
    drop(w);
    assert!(fired.load(Relaxed));
}

/// Regression for the double-panic abort during a guard's drop.
///
/// If a user panic is already unwinding the stack and a write guard's drop
/// runs a callback that itself panics, re-raising the callback panic in the
/// middle of an active unwind triggers a process abort. The fix suppresses
/// the inner re-raise via `std::thread::panicking()`.
///
/// This test panics the user code while a guard is alive and a panicking
/// callback is queued; the outer panic must propagate (be caught by
/// `catch_unwind`) without aborting the process.
#[test]
fn regression_no_abort_when_callback_panics_during_user_unwind() {
    let lock = Arc::new(RwLockBell::new(0u64));

    let lock2 = lock.clone();
    let result = panic::catch_unwind(AssertUnwindSafe(move || {
        let _guard = lock2.write();
        drop(lock2.try_write_or(|| panic!("callback panic during unwind")));
        // _guard is dropped as part of unwinding from this panic. The drop
        // fires the queued callback, which panics. Without the fix the
        // double-panic aborts the process and this test never returns.
        panic!("outer user panic");
    }));

    assert!(result.is_err(), "outer panic must propagate");

    // Lock must still be in a usable, consistent state.
    let r = lock.read();
    assert_eq!(*r, 0);
    drop(r);

    let fired = Arc::new(AtomicBool::new(false));
    let fired2 = fired.clone();
    let w = lock.write();
    assert!(
        lock.try_write_or(move || fired2.store(true, Relaxed))
            .is_none()
    );
    drop(w);
    assert!(fired.load(Relaxed));
}

/// Sanity check: with no outer panic, a callback panic still propagates
/// (we only suppress when an outer unwind is already in progress).
#[test]
fn callback_panic_still_propagates_when_no_outer_panic() {
    let lock = Arc::new(RwLockBell::new(0u64));
    let guard = lock.write();
    drop(lock.try_write_or(|| panic!("callback panic")));

    let result = panic::catch_unwind(AssertUnwindSafe(|| drop(guard)));
    assert!(
        result.is_err(),
        "callback panic must still propagate in normal flow"
    );
}

/// A panicking destructor on the *discarded* callback used to strand the lock:
/// it dropped with the lock held and no guard yet built to release it.
#[test]
fn regression_discarded_callback_drop_panic_does_not_strand_the_lock() {
    struct PanicOnDrop;
    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            panic!("destructor boom");
        }
    }

    let lock = RwLockBell::new(0u64);

    let p = PanicOnDrop;
    let r = panic::catch_unwind(AssertUnwindSafe(|| {
        let _ = lock.try_write_or(move || {
            let _ = &p;
        });
    }));
    assert!(r.is_err(), "the destructor panic must propagate");
    assert!(
        lock.try_write().is_some(),
        "lock stranded: no guard existed to release it"
    );

    let p = PanicOnDrop;
    let r = panic::catch_unwind(AssertUnwindSafe(|| {
        let _ = lock.try_write_or_else(move || {
            let _ = &p;
            || {}
        });
    }));
    assert!(r.is_err());
    assert!(
        lock.try_write().is_some(),
        "lock stranded by the factory drop"
    );
}

/// The release on that path is the real one, not a bare raw unlock: the bell
/// bookkeeping survives, so the lock keeps ringing afterwards.
#[test]
fn regression_discarded_callback_drop_panic_leaves_the_bell_working() {
    struct PanicOnDrop;
    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            panic!("destructor boom");
        }
    }

    let lock = RwLockBell::new(0u64);

    let p = PanicOnDrop;
    let r = panic::catch_unwind(AssertUnwindSafe(|| {
        let _ = lock.try_write_or(move || {
            let _ = &p;
        });
    }));
    assert!(r.is_err());

    // A full cycle must still work: acquire, queue, drain, ring.
    let fired = Arc::new(AtomicBool::new(false));
    let f = fired.clone();
    let held = lock.write();
    assert!(
        lock.try_write_or(move || f.store(true, Relaxed)).is_none(),
        "the lock is held, so this must queue"
    );
    drop(held);
    assert!(
        fired.load(Relaxed),
        "bell wedged after the destructor panic"
    );
}
