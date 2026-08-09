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

/// A write-drain must wait out an in-flight `try_write_or` before taking its
/// batch, so the callback it is about to queue is not missed.
#[test]
fn test_in_flight_try_write_or_collected_by_write_guard_drain() {
    let _g = TestGuard::acquire();
    let lock = Arc::new(RwLockBell::new(0u64));
    let called = Arc::new(AtomicBool::new(false));

    let gate = Gate::new();
    let g2 = gate.clone();
    hooks::set(HookPoint::TryWriteOrBeforeAcquire, move || g2.wait());

    let lock_b = lock.clone();
    let called_b = called.clone();
    let t_b = thread::spawn(move || {
        drop(lock_b.try_write_or(move || called_b.store(true, Relaxed)));
    });

    gate.wait_for_arrival(); // B paused with locking=1, before try_write
    hooks::clear(HookPoint::TryWriteOrBeforeAcquire);

    let guard = lock.write();
    gate.open(); // B's try_write now fails against us and queues

    drop(guard);

    t_b.join().unwrap();
    assert!(
        called.load(Relaxed),
        "callback must fire after drain completes"
    );
}

/// A `try_write_or` starting while a drain is in flight must not hang: it
/// blocks on `not_draining` at most until the drain reopens the gate.
#[test]
fn test_try_write_or_during_drain_eventually_proceeds() {
    let _g = TestGuard::acquire();
    let lock = Arc::new(RwLockBell::new(0u64));

    let b_handle: Arc<Mutex<Option<thread::JoinHandle<_>>>> = Arc::new(Mutex::new(None));
    let b_handle2 = b_handle.clone();
    let b_proceeded = Arc::new(AtomicBool::new(false));
    let b_proceeded2 = b_proceeded.clone();

    // Spawn B from inside A's drain so it races the gate. Blocking or
    // proceeding immediately are both valid outcomes.
    let lock_b = lock.clone();
    hooks::set(HookPoint::DrainBeforeGateReopens, move || {
        hooks::clear(HookPoint::DrainBeforeGateReopens); // one-shot
        let bp2 = b_proceeded2.clone();
        let lb2 = lock_b.clone();
        let handle = thread::spawn(move || {
            let guard = lb2.try_write_or(|| {});
            bp2.store(true, Relaxed);
            drop(guard);
        });
        *b_handle2.lock().unwrap() = Some(handle);
    });

    let gate_hold = Gate::new();
    let gh2 = gate_hold.clone();
    let lock_a = lock.clone();

    let t_a = thread::spawn(move || {
        let guard = lock_a.write();
        assert!(lock_a.try_write_or(|| {}).is_none()); // give the drain work
        gh2.wait();
        drop(guard);
    });

    gate_hold.wait_for_arrival();
    gate_hold.open();

    t_a.join().unwrap();

    let handle = b_handle
        .lock()
        .unwrap()
        .take()
        .expect("B must have been spawned");
    handle.join().unwrap();

    assert!(b_proceeded.load(Relaxed), "B must complete after drain");
}

/// The read-drain waits out an in-flight `try_write_or` too, and nothing fires
/// twice whether that call ends up succeeding or queueing.
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
        drop(lock_b.try_write_or(move || {
            called_b.fetch_add(1, Relaxed);
        }));
    });

    gate.wait_for_arrival(); // B paused with locking=1
    hooks::clear(HookPoint::TryWriteOrBeforeAcquire);

    let r = lock.read();
    let called2 = called.clone();
    assert!(
        lock.try_write_or(move || {
            called2.fetch_add(1, Relaxed);
        })
        .is_none()
    );

    gate.open(); // release B first, so the drain sees locking=0 promptly
    drop(r);

    t_b.join().unwrap();

    // 1 if B acquired the lock, 2 if it queued. Never more.
    let n = called.load(Relaxed);
    assert!(n >= 1, "read-guard callback must have fired");
    assert!(n <= 2, "no callback should fire more than once");
}

/// Stretching the mid-release window — batch taken, shared lock still held —
/// changes nothing: the callback fires exactly once.
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

    let gate = Gate::new();
    let g2 = gate.clone();
    hooks::set(HookPoint::ReadGuardBeforeRawUnlock, move || g2.wait());

    // Read guards are !Send, so `r` has to be dropped here on main. The hook
    // pauses main; a side thread releases it.
    let g3 = gate.clone();
    let releaser = thread::spawn(move || {
        g3.wait_for_arrival(); // main is paused at the hook
        g3.open(); // let main continue
    });

    drop(r); // pauses at the hook until the releaser opens the gate

    releaser.join().unwrap();

    // Drain has now completed; callback must have fired exactly once.
    assert_eq!(fired.load(Relaxed), 1, "callback must fire exactly once");
}

/// The read-drain must snapshot its batch while it still holds the lock.
///
/// The release used to run first, leaving a gap where the lock was free but
/// `readers` still read non-zero. A writer could take it in that gap, a
/// `try_write_or` could then lose to *that* writer, and the late-arriving
/// read-drain would sweep the newcomer's callback into its own batch and ring
/// it while the writer still held. The writer's own release then found nothing
/// left — one early ring, and no correct one.
///
/// Here R parks mid-release with the batch taken, and W must fail to acquire.
#[test]
fn regression_read_drain_snapshots_before_releasing() {
    let _g = TestGuard::acquire();
    let lock = Arc::new(RwLockBell::new(0u64));
    let fired = Arc::new(AtomicBool::new(false));

    let r = lock.read();
    let f2 = fired.clone();
    assert!(lock.try_write_or(move || f2.store(true, Relaxed)).is_none());

    let r_parked = Gate::new();
    let r_release = Gate::new();
    let rp2 = r_parked.clone();
    let rr2 = r_release.clone();
    hooks::set(HookPoint::ReadGuardBeforeRawUnlock, move || {
        hooks::clear(HookPoint::ReadGuardBeforeRawUnlock); // one-shot
        rp2.signal();
        rr2.wait();
    });

    let w_holds = Arc::new(AtomicBool::new(false));
    let lock_w = lock.clone();
    let wh2 = w_holds.clone();
    let orchestrator = thread::spawn(move || {
        // R has taken its batch and is parked, still holding the shared lock.
        r_parked.wait_for_arrival();

        let w_handle = thread::spawn(move || {
            let w = lock_w.write();
            wh2.store(true, Relaxed);
            drop(w);
        });

        // W must still be blocked: with the old ordering it would already own
        // the lock here, and R's batch would be stale.
        thread::sleep(Duration::from_millis(100));
        assert!(
            !w_holds.load(Relaxed),
            "a writer took the lock before the read drain snapshotted its batch"
        );

        r_release.open();
        w_handle.join().unwrap();
    });

    let watchdog_stop = Arc::new(AtomicBool::new(false));
    let watchdog_stop2 = watchdog_stop.clone();
    let watchdog = thread::spawn(move || {
        for _ in 0..100 {
            if watchdog_stop2.load(Relaxed) {
                return;
            }
            thread::sleep(Duration::from_millis(100));
        }
        eprintln!("[regression_read_drain_snapshots_before_releasing] WATCHDOG fired");
        std::process::abort();
    });

    drop(r);

    orchestrator.join().unwrap();
    watchdog_stop.store(true, Relaxed);
    watchdog.join().unwrap();

    assert!(fired.load(Relaxed), "the queued callback must have rung");
}

/// Pins the `while inner.draining > 0` wait in `try_write_or_else`: B enters it
/// while A's drain is parked, and is woken when A reopens the gate.
#[test]
fn test_while_draining_loop_is_entered() {
    let _g = TestGuard::acquire();
    let lock = Arc::new(RwLockBell::new(0u64));

    let gate_drain = Gate::new();
    let gd2 = gate_drain.clone();
    hooks::set(HookPoint::DrainBeforeGateReopens, move || {
        hooks::clear(HookPoint::DrainBeforeGateReopens); // one-shot
        gd2.wait();
    });

    let gate_in_draining = Gate::new();
    let gid2 = gate_in_draining.clone();
    hooks::set(HookPoint::TryWriteOrWhileDraining, move || {
        hooks::clear(HookPoint::TryWriteOrWhileDraining); // one-shot
        gid2.signal();
    });

    let lock_a = lock.clone();
    let gate_a = Gate::new();
    let ga2 = gate_a.clone();
    let t_a = thread::spawn(move || {
        let guard = lock_a.write();
        assert!(lock_a.try_write_or(|| {}).is_none()); // give the drain work
        ga2.wait();
        drop(guard); // parks in the hook above
    });

    gate_a.wait_for_arrival();
    gate_a.open();

    let lock_b = lock.clone();
    let b_proceeded = Arc::new(AtomicBool::new(false));
    let bp2 = b_proceeded.clone();
    let t_b = thread::spawn(move || {
        drop(lock_b.try_write_or(|| {}));
        bp2.store(true, Relaxed);
    });

    gate_in_draining.wait_for_arrival(); // B is now in not_draining.wait()
    gate_drain.open();

    t_a.join().unwrap();
    t_b.join().unwrap();
    assert!(
        b_proceeded.load(Relaxed),
        "B must complete after the gate reopens"
    );
}

/// Pins the `while inner.locking != 0` wait in the write-drain: B is released
/// only once the drain has committed, so the drain must sit in the wait until
/// B pushes its callback.
#[test]
fn test_write_guard_locking_zero_wait_is_entered() {
    let _g = TestGuard::acquire();
    let lock = Arc::new(RwLockBell::new(0u64));
    let called = Arc::new(AtomicBool::new(false));

    let gate_b = Gate::new();
    let gb2 = gate_b.clone();
    hooks::set(HookPoint::TryWriteOrBeforeAcquire, move || gb2.wait());

    let gate_draining = Gate::new();
    let gd2 = gate_draining.clone();
    hooks::set(HookPoint::WriteGuardAfterEnteringDrain, move || {
        hooks::clear(HookPoint::WriteGuardAfterEnteringDrain); // one-shot
        gd2.signal();
    });

    let lock_b = lock.clone();
    let called_b = called.clone();
    let t_b = thread::spawn(move || {
        drop(lock_b.try_write_or(move || called_b.store(true, Relaxed)));
    });

    gate_b.wait_for_arrival(); // B paused with locking=1
    hooks::clear(HookPoint::TryWriteOrBeforeAcquire);

    let guard = lock.write();

    let gb3 = gate_b.clone();
    let orchestrator = thread::spawn(move || {
        gate_draining.wait_for_arrival();
        gb3.open();
    });

    drop(guard);

    orchestrator.join().unwrap();
    t_b.join().unwrap();
    assert!(called.load(Relaxed), "callback must fire after drain");
}

/// The gate reopens *before* callbacks run, so a callback calling
/// `try_write_or` is never blocked by the very drain running it.
#[test]
fn test_callbacks_run_after_drain_gate_reopens() {
    let _g = TestGuard::acquire();
    let lock = Arc::new(RwLockBell::new(0u64));
    let callback_ran = Arc::new(AtomicBool::new(false));
    let cr2 = callback_ran.clone();

    let lock2 = lock.clone();
    hooks::set(HookPoint::DrainBeforeCallbacks, move || {
        hooks::clear(HookPoint::DrainBeforeCallbacks); // one-shot
        let guard = lock2.try_write_or(|| {});
        assert!(
            guard.is_some(),
            "lock must be acquirable once the gate reopens"
        );
    });

    let lock3 = lock.clone();
    let guard = lock.write();
    assert!(
        lock.try_write_or(move || {
            drop(lock3.try_write_or(|| {}));
            cr2.store(true, Relaxed);
        })
        .is_none()
    );

    drop(guard);

    assert!(callback_ran.load(Relaxed));
}

// ─── regression: last-reader drain must wait for in-flight try_write_or ──────

/// The last-reader drain used to return early on an empty queue without
/// checking `locking`. An in-flight `try_write_or` that had already failed
/// against this very read lock would then push its callback after the decision,
/// stranding it forever even though the lock was free.
///
/// T is held inside its factory — in-flight, queue still empty — while the last
/// reader drops. The drain must wait for T rather than return.
#[test]
fn regression_last_reader_drain_waits_for_in_flight_locking() {
    let _g = TestGuard::acquire();
    let lock = Arc::new(RwLockBell::new(0u64));
    let fired = Arc::new(AtomicBool::new(false));

    let gate_factory = Gate::new();

    let gate_drain = Gate::new();
    let gd2 = gate_drain.clone();
    hooks::set(HookPoint::ReadGuardAfterEnteringDrain, move || {
        hooks::clear(HookPoint::ReadGuardAfterEnteringDrain); // one-shot
        gd2.signal();
    });

    let r = lock.read();

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

    let gf3 = gate_factory.clone();
    let opener = thread::spawn(move || {
        gate_drain.wait_for_arrival();
        gf3.open();
    });

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

// ─── the `locking_zero` wait runs under the drainer's own lock ───────────────

/// A drainer holds its lock across the whole `locking_zero` wait, so nobody
/// else can be inside a release at the same time.
///
/// This subsumes an older double-drain deadlock: back when the read path
/// released before its bookkeeping, both drains could sit in that wait at once
/// and `notify_one` would wake only one of them. Unreachable now — W here must
/// fail to acquire while R waits. (`notify_all` is kept anyway; see
/// `decrement_locking`.)
#[test]
fn regression_read_drain_holds_lock_through_locking_wait() {
    let _g = TestGuard::acquire();
    let lock = Arc::new(RwLockBell::new(0u64));

    let r = lock.read();
    let q_fired = Arc::new(AtomicBool::new(false));
    let qf2 = q_fired.clone();
    assert!(
        lock.try_write_or(move || qf2.store(true, Relaxed))
            .is_none()
    );

    let gate_t = Gate::new();
    let gt2 = gate_t.clone();
    hooks::set(HookPoint::TryWriteOrBeforeAcquire, move || gt2.wait());

    let lock_t = lock.clone();
    let t_fired = Arc::new(AtomicBool::new(false));
    let tf2 = t_fired.clone();
    let t_handle = thread::spawn(move || {
        drop(lock_t.try_write_or(move || tf2.store(true, Relaxed)));
    });
    gate_t.wait_for_arrival();
    hooks::clear(HookPoint::TryWriteOrBeforeAcquire);

    let r_set = Gate::new();
    let r_set2 = r_set.clone();
    hooks::set(HookPoint::ReadGuardAfterEnteringDrain, move || {
        hooks::clear(HookPoint::ReadGuardAfterEnteringDrain);
        r_set2.signal();
    });

    let lock_orc = lock.clone();
    let gate_t_open = gate_t.clone();
    let r_set_arrival = r_set.clone();
    let w_holds = Arc::new(AtomicBool::new(false));
    let wh2 = w_holds.clone();
    let orchestrator = thread::spawn(move || {
        r_set_arrival.wait_for_arrival();

        let lock_w = lock_orc.clone();
        let w_handle = thread::spawn(move || {
            let w = lock_w.write();
            wh2.store(true, Relaxed);
            drop(w);
        });

        // R is parked in `locking_zero.wait` holding the shared lock, so W
        // cannot be inside a release of its own.
        thread::sleep(Duration::from_millis(100));
        assert!(
            !w_holds.load(Relaxed),
            "a writer got the lock while the read drain was still waiting on locking_zero"
        );

        gate_t_open.open();
        w_handle.join().unwrap();
    });

    let watchdog_stop = Arc::new(AtomicBool::new(false));
    let watchdog_stop2 = watchdog_stop.clone();
    let watchdog = thread::spawn(move || {
        for _ in 0..100 {
            if watchdog_stop2.load(Relaxed) {
                return;
            }
            thread::sleep(Duration::from_millis(100));
        }
        eprintln!("[regression_read_drain_holds_lock_through_locking_wait] WATCHDOG fired");
        std::process::abort();
    });

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

// ─── regression: overlapping drains must not reopen the gate early ───────────

/// `draining` used to be a `bool`, cleared by whichever drain finished first.
/// But a drain still holds its lock between taking the batch and releasing. If
/// an overlapping drain cleared the flag inside that window, a `try_write_or`
/// would sail through, fail against the still-held lock, and queue a callback
/// into a batch already taken — never rung, though the lock went free moments
/// later. A count fixes it: only the last drain out reopens the gate.
///
/// Drains can only overlap tail-to-head, and that is what this builds: R parks
/// in its tail with the batch taken and the lock released, W parks in its head
/// still holding, R decrements into that window, and C then tries to register.
#[test]
fn regression_overlapping_drains_do_not_strand_callbacks() {
    let _g = TestGuard::acquire();
    let lock = Arc::new(RwLockBell::new(0u64));

    let registered = Arc::new(AtomicU64::new(0));
    let fired = Arc::new(AtomicU64::new(0));

    let bump = |fired: &Arc<AtomicU64>| {
        let f = fired.clone();
        move || {
            f.fetch_add(1, Relaxed);
        }
    };

    let r = lock.read();
    assert!(lock.try_write_or(bump(&fired)).is_none());
    registered.fetch_add(1, Relaxed);

    // R parks in its tail: batch taken, lock already released.
    let r_parked = Gate::new();
    let r_release = Gate::new();
    let rp2 = r_parked.clone();
    let rr2 = r_release.clone();
    hooks::set(HookPoint::DrainBeforeGateReopens, move || {
        hooks::clear(HookPoint::DrainBeforeGateReopens); // one-shot
        rp2.signal();
        rr2.wait();
    });

    // W parks in its head: batch taken, lock still held.
    let w_parked = Gate::new();
    let w_release = Gate::new();
    let wp2 = w_parked.clone();
    let wr2 = w_release.clone();
    hooks::set(HookPoint::WriteGuardBeforeRawUnlock, move || {
        hooks::clear(HookPoint::WriteGuardBeforeRawUnlock); // one-shot
        wp2.signal();
        wr2.wait();
    });

    // Fires once R has decremented `draining`.
    let first_drain_done = Gate::new();
    let fdd = first_drain_done.clone();
    hooks::set(HookPoint::DrainBeforeCallbacks, move || {
        hooks::clear(HookPoint::DrainBeforeCallbacks); // one-shot
        fdd.signal();
    });

    let lock_orc = lock.clone();
    let reg_c = registered.clone();
    let cb_c = bump(&fired);
    let orchestrator = thread::spawn(move || {
        // R is parked in its tail; the lock is free, so W can take it.
        r_parked.wait_for_arrival();
        let lock_w = lock_orc.clone();
        let w_handle = thread::spawn(move || drop(lock_w.write()));

        // W now holds the lock with its batch already taken. Let R finish
        // decrementing `draining` into that window.
        w_parked.wait_for_arrival();
        r_release.open();
        first_drain_done.wait_for_arrival();

        // The window: with the bug this returns `None` and strands CBC.
        let lock_c = lock_orc.clone();
        let c_handle = thread::spawn(move || {
            if lock_c.try_write_or(cb_c).is_none() {
                reg_c.fetch_add(1, Relaxed);
            }
        });

        // Give C time to reach its decision before letting W go. Correct code
        // has C blocked on `not_draining` for the whole nap.
        thread::sleep(Duration::from_millis(100));

        w_release.open();
        c_handle.join().unwrap();
        w_handle.join().unwrap();
    });

    let watchdog_stop = Arc::new(AtomicBool::new(false));
    let watchdog_stop2 = watchdog_stop.clone();
    let watchdog = thread::spawn(move || {
        for _ in 0..100 {
            if watchdog_stop2.load(Relaxed) {
                return;
            }
            thread::sleep(Duration::from_millis(100));
        }
        eprintln!("[regression_overlapping_drains_do_not_strand_callbacks] WATCHDOG fired");
        std::process::abort();
    });

    drop(r);

    orchestrator.join().unwrap();

    watchdog_stop.store(true, Relaxed);
    watchdog.join().unwrap();

    // Lock free, every release complete: the queue must be empty.
    assert_eq!(
        fired.load(Relaxed),
        registered.load(Relaxed),
        "every registered callback must have rung"
    );
}
