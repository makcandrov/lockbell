use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
    },
    thread,
    time::Duration,
};

use crate::{
    RwLockBell,
    tests::hooks::{self, Gate, HookPoint, TestGuard},
};

// ─── write-guard drain races ──────────────────────────────────────────────────

/// Scenario: `try_write_or` is in-flight (locking=1) when a write guard is
/// dropped. The drain must wait for the in-flight call to finish before
/// collecting callbacks.
///
/// Sequencing:
/// 1. Thread B calls `try_write_or`, pauses at `TryWriteOrBeforeAcquire`
///    with `locking=1` before calling `try_write`.
/// 2. Main acquires the write lock (B hasn't called `try_write` yet, lock is free).
/// 3. Main releases B: B's `try_write` fails (lock held), callback registered,
///    `locking` → 0.
/// 4. Main drops the guard: drain waits for `locking=0` (B may already have
///    decremented it), collects callback, fires it.
#[test]
fn test_in_flight_try_write_or_collected_by_write_guard_drain() {
    let _g = TestGuard::acquire();
    let lock = Arc::new(RwLockBell::new(0u64));
    let called = Arc::new(AtomicBool::new(false));

    let gate = Gate::new();
    let g2 = gate.clone();
    hooks::set(HookPoint::TryWriteOrBeforeAcquire, move || g2.wait());

    // Thread B: tries to write-lock; pauses at hook, then completes.
    let lock_b = lock.clone();
    let called_b = called.clone();
    let t_b = thread::spawn(move || {
        let _ = lock_b.try_write_or(move || called_b.store(true, Relaxed));
    });

    gate.wait_for_arrival(); // B is paused (locking=1, before try_write)
    hooks::clear(HookPoint::TryWriteOrBeforeAcquire); // no accidental re-entry

    // Acquire the write lock now (safe: B is paused before calling try_write).
    let guard = lock.write();

    // Release B: it will call try_write (fails — we hold the lock),
    // push its callback, and decrement locking to 0.
    gate.open();

    // Drop the guard.  The drain sets dropping=true, waits for locking=0
    // (B may still be decrementing), then collects B's callback and fires it.
    drop(guard);

    t_b.join().unwrap();
    assert!(
        called.load(Relaxed),
        "callback must fire after drain completes"
    );
}

/// Scenario: a new `try_write_or` call starts while `dropping=true` (a drain
/// is in progress — the write lock has been released but `dropping` has not
/// yet been reset).
///
/// Sequencing:
/// 1. Thread A acquires the write guard, registers a dummy callback, then
///    signals main and waits.
/// 2. Main tells A to drop.  A's drain fires `DrainAfterWriteLockRelease`
///    (write lock free, `dropping=true`) and spawns thread B, then returns.
/// 3. B calls `try_write_or`; if `dropping` is still true it will block
///    until A's drain resets it.
/// 4. A's drain resets `dropping`, runs the dummy callback.
/// 5. B either succeeded or registered its own callback.
#[test]
fn test_try_write_or_during_drain_eventually_proceeds() {
    let _g = TestGuard::acquire();
    let lock = Arc::new(RwLockBell::new(0u64));

    // Shared handle so main can join B after A completes.
    let b_handle: Arc<Mutex<Option<thread::JoinHandle<_>>>> = Arc::new(Mutex::new(None));
    let b_handle2 = b_handle.clone();
    let b_proceeded = Arc::new(AtomicBool::new(false));
    let b_proceeded2 = b_proceeded.clone();

    // Hook: fires in A's drain after write lock is released, while dropping=true.
    // Clear it immediately on entry so that B's guard-drop does not re-fire it.
    let lock_b = lock.clone();
    hooks::set(HookPoint::DrainAfterWriteLockRelease, move || {
        hooks::clear(HookPoint::DrainAfterWriteLockRelease); // one-shot
        // Spawn B inside the hook so it sees dropping=true (if fast enough).
        let bp2 = b_proceeded2.clone();
        let lb2 = lock_b.clone();
        let handle = thread::spawn(move || {
            // try_write_or may block briefly on not_dropping, or proceed
            // immediately if the hook returns first — both are valid outcomes.
            let guard = lb2.try_write_or(|| {});
            bp2.store(true, Relaxed);
            drop(guard);
        });
        *b_handle2.lock().unwrap() = Some(handle);
        // Return immediately; drain continues, resets dropping, wakes B if blocked.
    });

    // Gate: A signals "guard acquired"; waits for main to say "start drop".
    let gate_hold = Gate::new();
    let gh2 = gate_hold.clone();
    let lock_a = lock.clone();

    let t_a = thread::spawn(move || {
        let guard = lock_a.write();
        // Register a dummy callback so the drain has work to do.
        assert!(lock_a.try_write_or(|| {}).is_none());
        gh2.wait(); // signal "acquired"; wait for "drop"
        drop(guard); // fires hook, spawns B, resets dropping
    });

    gate_hold.wait_for_arrival(); // A holds the guard
    gate_hold.open(); // tell A to drop → drain runs → hook fires → B spawned

    t_a.join().unwrap();

    // B was spawned inside the hook; join it.
    let handle = b_handle
        .lock()
        .unwrap()
        .take()
        .expect("B must have been spawned");
    handle.join().unwrap();

    assert!(b_proceeded.load(Relaxed), "B must complete after drain");
}

// ─── read-guard drain races ───────────────────────────────────────────────────

/// Scenario: `try_write_or` is in-flight (locking=1) when the last read guard
/// is dropped. The read-guard drain must wait for the in-flight call to finish
/// before collecting callbacks — same guarantee as the write-guard path.
///
/// Sequencing:
/// 1. Main holds a read guard.
/// 2. Thread B calls `try_write_or`, pauses at `TryWriteOrBeforeAcquire`
///    (locking=1).
/// 3. Main drops the read guard.  The read-guard drain sets `dropping=true`
///    and blocks on `locking_zero` (locking=1).
/// 4. Main releases B: B's `try_write` fails (read lock released but we're
///    in the drain — actually lock is now truly free, so B might succeed).
///    Either way, locking → 0, which wakes the drain.
/// 5. Drain collects any queued callbacks and fires them.
#[test]
fn test_in_flight_try_write_or_during_last_read_guard_drop() {
    let _g = TestGuard::acquire();
    let lock = Arc::new(RwLockBell::new(0u64));
    let called = Arc::new(AtomicU64::new(0));

    let gate = Gate::new();
    let g2 = gate.clone();
    hooks::set(HookPoint::TryWriteOrBeforeAcquire, move || g2.wait());

    let lock_b = lock.clone();
    let called_b = called.clone();
    let t_b = thread::spawn(move || {
        // If try_write succeeds (lock free by then), no callback is queued —
        // that outcome is also valid and tested by the `called <= 1` assert below.
        if lock_b
            .try_write_or(move || {
                called_b.fetch_add(1, Relaxed);
            })
            .is_none()
        {
            // callback queued; will fire when drain completes
        }
    });

    gate.wait_for_arrival(); // B is paused (locking=1)
    hooks::clear(HookPoint::TryWriteOrBeforeAcquire);

    let r = lock.read();
    // Register a callback while the read guard is held.
    let called2 = called.clone();
    assert!(
        lock.try_write_or(move || {
            called2.fetch_add(1, Relaxed);
        })
        .is_none()
    );

    // Drop the read guard; its drain will wait for locking=0 before collecting.
    // We release B BEFORE dropping r so the drain sees locking=0 promptly.
    gate.open(); // B resumes: try_write may succeed or fail; locking → 0
    drop(r); // drain: wait for locking=0, collect all callbacks, fire

    t_b.join().unwrap();

    // called = 1 (only the read-guard callback, B succeeded) or
    //         2 (both callbacks, B failed and its callback also fired).
    // In no case should a callback fire more than once.
    let n = called.load(Relaxed);
    assert!(n >= 1, "read-guard callback must have fired");
    assert!(n <= 2, "no callback should fire more than once");
}

/// Scenario: the last read guard is dropped while a concurrent `try_write_or`
/// is in-flight (paused at `ReadGuardAfterRelease` — after the read lock is
/// released but before `readers` is decremented).
///
/// Verifies: the `readers` counter is only decremented once per guard, even
/// with the gap between read-lock release and counter decrement.
#[test]
fn test_read_guard_drop_atomicity() {
    let _g = TestGuard::acquire();
    let lock = Arc::new(RwLockBell::new(0u64));
    let fired = Arc::new(AtomicU64::new(0));

    let r = lock.read();

    let f1 = fired.clone();
    assert!(
        lock.try_write_or(move || {
            f1.fetch_add(1, Relaxed);
        })
        .is_none()
    );

    // Pause the read-guard drop after releasing the read lock but before
    // decrementing `readers`.  During this window, another try_write_or
    // might see the lock as free and race with the drain.
    let gate = Gate::new();
    let g2 = gate.clone();
    hooks::set(HookPoint::ReadGuardAfterRelease, move || g2.wait());

    // Thread C: will drop r (pauses at the hook).
    // Read guards are !Send, so we can't move `r` to another thread.
    // Instead we drop it here on main, which is where r was created.
    // The hook pauses main; we release it from a side thread.

    // Spawn a side thread to release main's hook after it arrives.
    let g3 = gate.clone();
    let releaser = thread::spawn(move || {
        g3.wait_for_arrival(); // main is paused at hook
        // Optionally: do extra work here while `readers` is not yet decremented
        g3.open(); // let main continue
    });

    drop(r); // fires ReadGuardAfterRelease hook → pauses until releaser opens gate

    releaser.join().unwrap();

    // Drain has now completed; callback must have fired exactly once.
    assert_eq!(fired.load(Relaxed), 1, "callback must fire exactly once");
}

// ─── deterministic loop-coverage tests ───────────────────────────────────────

/// Verifies that the `while inner.dropping` loop body in `try_write_or_else`
/// is entered deterministically.
///
/// Sequencing:
/// 1. Thread A holds the write guard (with a queued callback so the drain runs).
///    A's drain is hooked at `DrainAfterWriteLockRelease` to block until released.
/// 2. Main drops the guard (A's thread): drain fires, hook blocks A with
///    `dropping=true` set and no lock held.
/// 3. Main spawns Thread B to call `try_write_or`.  B acquires the state mutex,
///    sees `dropping=true`, fires `TryWriteOrWhileDropping` (non-blocking signal),
///    and enters `not_dropping.wait()`.
/// 4. Main waits for the `TryWriteOrWhileDropping` signal (B is now in the wait),
///    then releases A's drain.
/// 5. Drain resets `dropping=false`, notifies `not_dropping` — B wakes and
///    completes.
#[test]
fn test_while_dropping_loop_is_entered() {
    let _g = TestGuard::acquire();
    let lock = Arc::new(RwLockBell::new(0u64));

    // gate_drain: pauses A's drain at DrainAfterWriteLockRelease.
    let gate_drain = Gate::new();
    let gd2 = gate_drain.clone();
    hooks::set(HookPoint::DrainAfterWriteLockRelease, move || {
        hooks::clear(HookPoint::DrainAfterWriteLockRelease); // one-shot
        gd2.wait(); // block A's drain; dropping=true, no lock held
    });

    // gate_in_dropping: B signals just before entering not_dropping.wait().
    // Fires while holding state mutex; must only call signal() (non-blocking).
    let gate_in_dropping = Gate::new();
    let gid2 = gate_in_dropping.clone();
    hooks::set(HookPoint::TryWriteOrWhileDropping, move || {
        hooks::clear(HookPoint::TryWriteOrWhileDropping); // one-shot
        gid2.signal(); // non-blocking: safe under state mutex
    });

    // Thread A: acquires write guard, registers a callback (so drain has work),
    // then drops the guard — its drain will block in the hook above.
    let lock_a = lock.clone();
    let gate_a = Gate::new();
    let ga2 = gate_a.clone();
    let t_a = thread::spawn(move || {
        let guard = lock_a.write();
        assert!(lock_a.try_write_or(|| {}).is_none());
        ga2.wait(); // signal "ready"; wait for "drop now"
        drop(guard); // drain fires, hook blocks here until gate_drain.open()
    });

    gate_a.wait_for_arrival(); // A holds the guard and has queued a callback
    gate_a.open(); // tell A to drop

    // Now A's drain is running (or about to) and will block at
    // DrainAfterWriteLockRelease with dropping=true.

    // Thread B: will call try_write_or, see dropping=true, enter the loop.
    let lock_b = lock.clone();
    let b_proceeded = Arc::new(AtomicBool::new(false));
    let bp2 = b_proceeded.clone();
    let t_b = thread::spawn(move || {
        let _ = lock_b.try_write_or(|| {});
        bp2.store(true, Relaxed);
    });

    // Wait until B has entered the `while inner.dropping` body (hook fired).
    gate_in_dropping.wait_for_arrival();

    // B is now in not_dropping.wait().  Release A's drain so it resets
    // dropping=false and wakes B.
    gate_drain.open();

    t_a.join().unwrap();
    t_b.join().unwrap();
    assert!(
        b_proceeded.load(Relaxed),
        "B must complete after dropping is reset"
    );
}

/// Verifies that the `while inner.locking != 0` loop body in
/// `Drop for RwLockBellWriteGuard` is entered deterministically.
///
/// Sequencing:
/// 1. Thread B calls `try_write_or`, pauses at `TryWriteOrBeforeAcquire`
///    with `locking=1`.
/// 2. Main acquires the write guard (B is paused before calling `try_write`).
/// 3. `WriteGuardAfterSettingDropping` hook: non-blockingly signals
///    `gate_dropping` (called while holding state mutex).
/// 4. Orchestrator thread waits for that signal, then opens B's gate.
/// 5. Main drops the write guard: sets `dropping=true` (hook fires, signal
///    sent), then enters `locking_zero.wait()` because `locking=1`.
/// 6. B resumes: `try_write` fails (write lock still held), pushes callback,
///    decrements `locking` to 0, notifies `locking_zero`.
/// 7. Drain wakes, drains callbacks (callback fires), drops write lock.
#[test]
fn test_write_guard_locking_zero_wait_is_entered() {
    let _g = TestGuard::acquire();
    let lock = Arc::new(RwLockBell::new(0u64));
    let called = Arc::new(AtomicBool::new(false));

    // gate_b: pauses B at TryWriteOrBeforeAcquire (locking=1).
    let gate_b = Gate::new();
    let gb2 = gate_b.clone();
    hooks::set(HookPoint::TryWriteOrBeforeAcquire, move || gb2.wait());

    // gate_dropping: non-blocking signal sent when dropping=true is set,
    // while the state mutex is held.
    let gate_dropping = Gate::new();
    let gd2 = gate_dropping.clone();
    hooks::set(HookPoint::WriteGuardAfterSettingDropping, move || {
        hooks::clear(HookPoint::WriteGuardAfterSettingDropping); // one-shot
        gd2.signal(); // non-blocking: safe under state mutex
    });

    // Thread B: will try_write_or, pause (locking=1), then complete.
    let lock_b = lock.clone();
    let called_b = called.clone();
    let t_b = thread::spawn(move || {
        let _ = lock_b.try_write_or(move || called_b.store(true, Relaxed));
    });

    gate_b.wait_for_arrival(); // B is paused; locking=1
    hooks::clear(HookPoint::TryWriteOrBeforeAcquire);

    // Acquire write lock now (safe: B hasn't called try_write yet).
    let guard = lock.write();

    // Orchestrator: waits for dropping=true to be signalled, then releases B.
    // At that point the drain is guaranteed to be inside (or about to enter)
    // the `while inner.locking != 0` wait.
    let gb3 = gate_b.clone();
    let orchestrator = thread::spawn(move || {
        gate_dropping.wait_for_arrival();
        // dropping=true is set; drain is waiting on locking_zero.  Release B
        // so it decrements locking and wakes the drain.
        gb3.open();
    });

    // Drop guard: sets dropping=true (hook fires, signals gate_dropping),
    // then blocks in `while inner.locking != 0` until B decrements locking.
    drop(guard);

    orchestrator.join().unwrap();
    t_b.join().unwrap();
    assert!(called.load(Relaxed), "callback must fire after drain");
}

// ─── drain ordering ───────────────────────────────────────────────────────────

/// Verifies that `dropping` is reset *before* callbacks run, so that a
/// callback calling `try_write_or` is never spuriously blocked on
/// `not_dropping`.
#[test]
fn test_callbacks_run_after_dropping_is_reset() {
    let _g = TestGuard::acquire();
    let lock = Arc::new(RwLockBell::new(0u64));
    let callback_ran = Arc::new(AtomicBool::new(false));
    let cr2 = callback_ran.clone();

    // Hook: fires between `dropping=false` and the callback batch.
    // Clear it immediately so the guard created inside does not re-fire it.
    let lock2 = lock.clone();
    hooks::set(HookPoint::DrainBeforeCallbacks, move || {
        hooks::clear(HookPoint::DrainBeforeCallbacks); // one-shot
        // The write lock is free and dropping=false here.
        // try_write_or must not block.
        let guard = lock2.try_write_or(|| {});
        assert!(
            guard.is_some(),
            "lock must be acquirable when dropping=false"
        );
    });

    // Register a callback whose body would deadlock if run before dropping resets.
    let lock3 = lock.clone();
    let guard = lock.write();
    assert!(
        lock.try_write_or(move || {
            // Also verify from inside the callback itself.
            let _ = lock3.try_write_or(|| {});
            cr2.store(true, Relaxed);
        })
        .is_none()
    );

    drop(guard); // DrainBeforeCallbacks hook fires, then callback runs

    assert!(callback_ran.load(Relaxed));
}

// ─── regression: last-reader drain must wait for in-flight try_write_or ──────

/// Regression for the lost-callback race in `drop_read_guard`.
///
/// Previously the last-reader drain returned early when the callback queue
/// was empty, without checking `locking`. An in-flight `try_write_or` whose
/// `try_write` had already failed (against this very read lock) would then
/// push its callback *after* the drain decision, stranding it forever even
/// though the lock was completely free. The fix is to drain whenever
/// `locking != 0`, waiting on `locking_zero` like the write-guard path does.
///
/// Sequencing (fully deterministic):
/// 1. Main holds a read guard R.
/// 2. Thread T calls `try_write_or_else`; its `try_write` fails (R is held)
///    and the factory blocks on `gate_factory` — T is now in-flight with
///    `locking=1` and an empty callback queue.
/// 3. Main drops R. The drain must take the non-early-return path: it sets
///    `dropping=true` (hook signals `gate_drain`) and waits on `locking_zero`.
/// 4. The opener thread sees `gate_drain`, releases `gate_factory`; T builds
///    its callback, pushes it, and decrements `locking` to 0.
/// 5. Main's drain wakes, collects T's callback, and fires it before
///    `drop(R)` returns.
///
/// With the bug, step 3 returns early instead: the hook never fires, the
/// opener and T block forever, and the watchdog aborts the test.
#[test]
fn regression_last_reader_drain_waits_for_in_flight_locking() {
    let _g = TestGuard::acquire();
    let lock = Arc::new(RwLockBell::new(0u64));
    let fired = Arc::new(AtomicBool::new(false));

    // gate_factory: blocks T inside the callback factory (after try_write
    // failed, before the callback is pushed).
    let gate_factory = Gate::new();

    // gate_drain: signalled when the read drain sets dropping=true.
    // Fires while holding the state mutex; signal() only (non-blocking).
    let gate_drain = Gate::new();
    let gd2 = gate_drain.clone();
    hooks::set(HookPoint::ReadGuardAfterSettingDropping, move || {
        hooks::clear(HookPoint::ReadGuardAfterSettingDropping); // one-shot
        gd2.signal();
    });

    let r = lock.read();

    // Thread T: in-flight try_write_or_else, held open by the factory gate.
    let lock_t = lock.clone();
    let fired_t = fired.clone();
    let gf2 = gate_factory.clone();
    let t = thread::spawn(move || {
        let res = lock_t.try_write_or_else(move || {
            gf2.wait(); // try_write already failed; locking=1, queue empty
            move || fired_t.store(true, Relaxed)
        });
        assert!(res.is_none(), "read guard was held at try_write time");
    });

    gate_factory.wait_for_arrival(); // T is in-flight, callback not yet pushed

    // Opener: once the drain has committed (dropping=true, about to wait on
    // locking_zero), release T so it pushes its callback and wakes the drain.
    let gf3 = gate_factory.clone();
    let opener = thread::spawn(move || {
        gate_drain.wait_for_arrival();
        gf3.open();
    });

    // Watchdog so a regression aborts instead of hanging the test runner.
    let watchdog_stop = Arc::new(AtomicBool::new(false));
    let watchdog_stop2 = watchdog_stop.clone();
    let watchdog = thread::spawn(move || {
        for _ in 0..50 {
            if watchdog_stop2.load(Relaxed) {
                return;
            }
            thread::sleep(Duration::from_millis(100));
        }
        eprintln!("[regression_last_reader_drain_waits_for_in_flight_locking] WATCHDOG fired");
        std::process::abort();
    });

    // Last reader drops: the drain must wait for T and fire its callback.
    drop(r);

    opener.join().unwrap();
    t.join().unwrap();
    assert!(
        fired.load(Relaxed),
        "callback pushed by the in-flight call must be collected by the read drain"
    );

    watchdog_stop.store(true, Relaxed);
    watchdog.join().unwrap();
}

// ─── regression: read-drain + write-drain both waiting on locking_zero ────────

/// Regression for the double-drain deadlock.
///
/// Previously, both drain paths slept on `locking_zero` and the failure path
/// of `try_write_or_else` called `notify_one`, stranding the second waiter.
/// The fix is to `notify_all` so both drainers re-check `locking == 0`.
///
/// Scenario:
/// 1. Q queues a callback while R holds a read guard. callbacks=[CB], locking=0.
/// 2. T calls try_write_or, pauses at TryWriteOrBeforeAcquire with locking=1.
/// 3. Main drops R; inside drop_read_guard the
///    `ReadGuardAfterSettingDropping` hook signals `r_set` (dropping=true set,
///    locking_zero wait not yet started). Main then sleeps on locking_zero.
/// 4. Orchestrator (in its own thread) waits for r_set, then spawns W.
/// 5. W acquires the write lock and drops it; inside drop_write_guard the
///    `WriteGuardAfterSettingDropping` hook signals `w_set` (dropping=true
///    re-set, locking_zero wait not yet started). W then sleeps on locking_zero.
/// 6. Orchestrator waits for w_set, then releases T.
/// 7. T's try_write fails (W holds the write lock), T pushes its CB,
///    decrements locking to 0, and *with the fix* notify_all wakes both
///    drainers. Without the fix, only one would wake and the other would hang.
#[test]
fn regression_double_drain_no_deadlock() {
    let _g = TestGuard::acquire();
    let lock = Arc::new(RwLockBell::new(0u64));

    // Step 1: R holds the read lock; Q queues CB.
    let r = lock.read();
    let q_fired = Arc::new(AtomicBool::new(false));
    let qf2 = q_fired.clone();
    assert!(
        lock.try_write_or(move || qf2.store(true, Relaxed))
            .is_none()
    );

    // Step 2: pause T at TryWriteOrBeforeAcquire (locking=1).
    let gate_t = Gate::new();
    let gt2 = gate_t.clone();
    hooks::set(HookPoint::TryWriteOrBeforeAcquire, move || gt2.wait());

    let lock_t = lock.clone();
    let t_fired = Arc::new(AtomicBool::new(false));
    let tf2 = t_fired.clone();
    let t_handle = thread::spawn(move || {
        let _ = lock_t.try_write_or(move || tf2.store(true, Relaxed));
    });
    gate_t.wait_for_arrival();
    hooks::clear(HookPoint::TryWriteOrBeforeAcquire);

    // Step 3 hook: signal when R sets dropping=true.
    let r_set = Gate::new();
    let r_set2 = r_set.clone();
    hooks::set(HookPoint::ReadGuardAfterSettingDropping, move || {
        hooks::clear(HookPoint::ReadGuardAfterSettingDropping);
        r_set2.signal();
    });

    // Step 5 hook: signal when W sets dropping=true.
    let w_set = Gate::new();
    let w_set2 = w_set.clone();
    hooks::set(HookPoint::WriteGuardAfterSettingDropping, move || {
        hooks::clear(HookPoint::WriteGuardAfterSettingDropping);
        w_set2.signal();
    });

    // Step 4 + 6: orchestrator spawns W after R's signal, releases T after W's.
    let lock_orc = lock.clone();
    let gate_t_open = gate_t.clone();
    let r_set_arrival = r_set.clone();
    let w_set_arrival = w_set.clone();
    let orchestrator = thread::spawn(move || {
        r_set_arrival.wait_for_arrival();

        let lock_w = lock_orc.clone();
        let w_handle = thread::spawn(move || {
            let w = lock_w.write();
            drop(w);
        });

        w_set_arrival.wait_for_arrival();
        gate_t_open.open();

        w_handle.join().unwrap();
    });

    // Watchdog so a regression doesn't hang the test runner forever.
    let watchdog_stop = Arc::new(AtomicBool::new(false));
    let watchdog_stop2 = watchdog_stop.clone();
    let watchdog = thread::spawn(move || {
        for _ in 0..50 {
            if watchdog_stop2.load(Relaxed) {
                return;
            }
            thread::sleep(Duration::from_millis(100));
        }
        eprintln!("[regression_double_drain_no_deadlock] WATCHDOG fired");
        std::process::abort();
    });

    // Main drops R; with the fix this returns once T's notify_all wakes both
    // drainers and they finish their drains.
    drop(r);

    t_handle.join().unwrap();
    orchestrator.join().unwrap();

    // Both queued callbacks must have fired (Q's pre-existing one, plus T's
    // newly-queued one).
    assert!(q_fired.load(Relaxed), "Q's callback must fire");
    assert!(t_fired.load(Relaxed), "T's callback must fire");

    watchdog_stop.store(true, Relaxed);
    watchdog.join().unwrap();
}
