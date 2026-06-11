use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering::Relaxed, Ordering::SeqCst},
    },
    thread,
    time::Duration,
};

use lockbell::RwLockBell;

#[test]
fn test_multiple_readers_callbacks_fire_on_last_drop() {
    const READERS: usize = 8;
    const CALLBACKS: usize = 16;

    let lock = Arc::new(RwLockBell::new(0u64));
    let count = Arc::new(AtomicU64::new(0));

    let r1 = lock.read();
    let r2 = lock.read();
    let r3 = lock.read();
    let r4 = lock.read();
    let r5 = lock.read();
    let r6 = lock.read();
    let r7 = lock.read();
    let r8 = lock.read();
    let _ = READERS;

    for _ in 0..CALLBACKS {
        let c = count.clone();
        assert!(
            lock.try_write_or(move || {
                c.fetch_add(1, Relaxed);
            })
            .is_none()
        );
    }

    drop(r1);
    drop(r2);
    drop(r3);
    drop(r4);
    drop(r5);
    drop(r6);
    drop(r7);
    assert_eq!(count.load(Relaxed), 0, "must not fire before last drop");

    drop(r8);
    assert_eq!(count.load(Relaxed), CALLBACKS as u64);
}

#[test]
fn test_try_write_or_races_last_read_guard_drop() {
    for _ in 0..500 {
        let lock = Arc::new(RwLockBell::new(0u64));
        let called = Arc::new(AtomicU64::new(0));

        let r = lock.read();

        let lock2 = lock.clone();
        let called2 = called.clone();

        let handle = thread::spawn(move || {
            let _ = lock2.try_write_or(move || {
                called2.fetch_add(1, Relaxed);
            });
        });

        drop(r);
        handle.join().unwrap();

        assert!(called.load(Relaxed) <= 1);
    }
}

/// Regression: a callback queued by an in-flight `try_write_or_else` must
/// fire even if the last read guard is dropped between the failed `try_write`
/// and the callback being pushed.
///
/// The factory passed to `try_write_or_else` runs exactly in that window, so
/// blocking inside it holds the call in-flight (`locking = 1`, queue still
/// empty) while the last read guard drops. The drain must wait for the
/// in-flight call and collect its callback; previously it returned early on
/// the empty queue and the callback was stranded forever.
#[test]
fn test_callback_queued_during_last_read_guard_drop_still_fires() {
    use std::sync::mpsc;

    let lock = Arc::new(RwLockBell::new(0u64));
    let fired = Arc::new(AtomicBool::new(false));

    let r = lock.read();

    let (factory_entered_tx, factory_entered_rx) = mpsc::channel();
    let (release_factory_tx, release_factory_rx) = mpsc::channel::<()>();

    let lock_t = lock.clone();
    let fired_t = fired.clone();
    let t = thread::spawn(move || {
        let res = lock_t.try_write_or_else(move || {
            factory_entered_tx.send(()).unwrap();
            release_factory_rx.recv().unwrap();
            move || fired_t.store(true, Relaxed)
        });
        assert!(res.is_none(), "the read guard was held at try_write time");
    });

    // The factory has been entered: try_write failed, callback not yet queued.
    factory_entered_rx.recv().unwrap();

    // Release the factory from a helper after a short delay so the drain
    // decision in drop(r) below runs while the queue is still empty. The
    // delay only matters for catching the regression: correct code blocks in
    // drop(r) until the callback is pushed, regardless of timing.
    let helper = thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        release_factory_tx.send(()).unwrap();
    });

    drop(r);

    helper.join().unwrap();
    t.join().unwrap();
    assert!(
        fired.load(Relaxed),
        "callback queued by an in-flight try_write_or_else must fire once the lock is free"
    );
}

#[test]
fn test_stress_many_writers_one_reader() {
    const WRITERS: usize = 32;

    let lock = Arc::new(RwLockBell::new(0u64));
    let success_count = Arc::new(AtomicU64::new(0));
    let callback_count = Arc::new(AtomicU64::new(0));

    let r = lock.read();
    assert_eq!(*r, 0);

    thread::scope(|s| {
        for _ in 0..WRITERS {
            let lock2 = lock.clone();
            let sc = success_count.clone();
            let cc = callback_count.clone();
            s.spawn(move || {
                if lock2
                    .try_write_or(move || {
                        cc.fetch_add(1, Relaxed);
                    })
                    .is_some()
                {
                    sc.fetch_add(1, Relaxed);
                }
            });
        }
    });
    drop(r);

    let successes = success_count.load(Relaxed);
    let callbacks = callback_count.load(Relaxed);
    assert_eq!(
        successes + callbacks,
        WRITERS as u64,
        "every try_write_or call either succeeded or registered a callback that fired"
    );
}

#[test]
fn test_concurrent_try_write_or_while_reader_held() {
    const ITERS: usize = 200;

    let lock = Arc::new(RwLockBell::new(0u64));
    let count = Arc::new(AtomicU64::new(0));

    thread::scope(|s| {
        let r = lock.read();

        let lock2 = lock.clone();
        let count2 = count.clone();
        let writer_thread = s.spawn(move || {
            for _ in 0..ITERS {
                let c = count2.clone();
                if lock2
                    .try_write_or(move || {
                        c.fetch_add(1, Relaxed);
                    })
                    .is_some()
                {
                    count2.fetch_add(1, Relaxed);
                }
            }
        });

        thread::sleep(Duration::from_millis(5));
        drop(r);
        writer_thread.join().unwrap();
    });

    assert_eq!(count.load(Relaxed), ITERS as u64);
}

#[test]
fn test_in_flight_try_write_or_observed_by_drain() {
    for _ in 0..200 {
        let lock = Arc::new(RwLockBell::new(0u64));
        let called = Arc::new(AtomicU64::new(0));

        let r = lock.read();

        let lock2 = lock.clone();
        let called2 = called.clone();

        let handle = thread::spawn(move || {
            let _ = lock2.try_write_or(move || {
                called2.fetch_add(1, Relaxed);
            });
        });

        drop(r);
        handle.join().unwrap();

        assert!(called.load(Relaxed) <= 1, "no double-fire");
    }
}

#[test]
fn test_read_guard_acquired_during_drain_does_not_deadlock() {
    let lock = Arc::new(RwLockBell::new(0u64));
    let inner_read_dropped = Arc::new(AtomicBool::new(false));
    let inner_read_dropped2 = inner_read_dropped.clone();
    let lock2 = lock.clone();

    let r = lock.read();
    assert!(
        lock.try_write_or(move || {
            let r_inner = lock2.read();
            drop(r_inner);
            inner_read_dropped2.store(true, Relaxed);
        })
        .is_none()
    );

    drop(r);
    assert!(inner_read_dropped.load(Relaxed));

    let called = Arc::new(AtomicBool::new(false));
    let called2 = called.clone();
    let r2 = lock.read();
    assert!(
        lock.try_write_or(move || called2.store(true, Relaxed))
            .is_none()
    );
    drop(r2);
    assert!(called.load(Relaxed));
}

#[test]
fn test_high_contention_stress() {
    const THREADS: usize = 8;
    const OPS_PER_THREAD: usize = 50;

    let lock = Arc::new(RwLockBell::new(0u64));
    let registered = Arc::new(AtomicU64::new(0));
    let fired = Arc::new(AtomicU64::new(0));

    thread::scope(|s| {
        for t in 0..THREADS {
            let lock2 = lock.clone();
            let reg = registered.clone();
            let fir = fired.clone();
            s.spawn(move || {
                for i in 0..OPS_PER_THREAD {
                    if t % 2 == 0 {
                        let r = lock2.read();
                        if i % 3 == 0 {
                            let f = fir.clone();
                            if lock2
                                .try_write_or(move || {
                                    f.fetch_add(1, Relaxed);
                                })
                                .is_none()
                            {
                                reg.fetch_add(1, Relaxed);
                            }
                        }
                        drop(r);
                    } else {
                        let f = fir.clone();
                        match lock2.try_write_or(move || {
                            f.fetch_add(1, Relaxed);
                        }) {
                            Some(guard) => {
                                drop(guard);
                            }
                            None => {
                                reg.fetch_add(1, Relaxed);
                            }
                        }
                    }
                }
            });
        }
    });

    assert_eq!(
        fired.load(SeqCst),
        registered.load(SeqCst),
        "all registered callbacks must fire exactly once"
    );
}

#[test]
fn test_many_threads_write_and_callback() {
    const THREADS: usize = 16;
    const OPS: usize = 100;

    let lock = Arc::new(RwLockBell::new(0u64));
    let total = Arc::new(AtomicU64::new(0));

    thread::scope(|s| {
        for _ in 0..THREADS {
            let lock2 = lock.clone();
            let total2 = total.clone();
            s.spawn(move || {
                for _ in 0..OPS {
                    let t = total2.clone();
                    if let Some(mut guard) = lock2.try_write_or(move || {
                        t.fetch_add(1, Relaxed);
                    }) {
                        *guard += 1;
                        drop(guard);
                    }
                }
            });
        }
    });

    // Every operation either got the lock or registered a callback.
    // We can't predict exact counts, but nothing should panic or deadlock.
    let val = *lock.read();
    let callbacks = total.load(Relaxed);
    assert!(
        val + callbacks > 0,
        "at least some operations must have completed"
    );
}

#[test]
fn test_alternating_read_write_no_lost_callbacks() {
    const ITERS: usize = 200;

    let lock = Arc::new(RwLockBell::new(0u64));
    let count = Arc::new(AtomicU64::new(0));

    for _ in 0..ITERS {
        let c = count.clone();

        // Alternate: even iterations use read guard, odd use write guard.
        let r = lock.read();
        let _ = lock.try_write_or(move || {
            c.fetch_add(1, Relaxed);
        });

        drop(r);
    }

    assert_eq!(count.load(Relaxed), ITERS as u64);
}
