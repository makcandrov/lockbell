//! Bell bookkeeping shared by every guard flavour.
//!
//! [`LockState`] lives inside [`RawRwLockBell`](crate::RawRwLockBell) and is
//! guarded by its own mutex; it never grants or releases access to the
//! protected data.

use std::{
    fmt::Debug,
    mem::forget,
    panic::{self, AssertUnwindSafe},
};

use parking_lot::{Condvar, Mutex};

#[cfg(test)]
use crate::tests::hooks;

pub(crate) type Callback = Box<dyn FnOnce() + Send>;

// ─── LockStateInner ──────────────────────────────────────────────────────────

#[derive(Default)]
pub(crate) struct LockStateInner {
    /// Drains in progress, counted from committing to a drain to flushing its
    /// callbacks.
    ///
    /// A count rather than a flag: a read-drain and a write-drain can overlap,
    /// and the write-drain still holds the exclusive lock across its share of
    /// the window. Clearing on the first one to finish would let a
    /// `try_write_or_else` queue a callback that the other has already
    /// snapshotted past — and that callback would never ring.
    pub(crate) draining: u64,
    /// In-flight `try_write_or_else` calls between bumping the counter and resolving.
    pub(crate) locking: u64,
    /// Live shared-lock count.
    pub(crate) readers: u64,
    /// Callbacks queued by failed `try_write_or` calls, FIFO order.
    pub(crate) callbacks: Vec<Callback>,
}

impl LockStateInner {
    const NEW: Self = Self {
        draining: 0,
        locking: 0,
        readers: 0,
        callbacks: Vec::new(),
    };

    pub(crate) fn decrement_locking(&mut self, locking_zero: &Condvar) {
        self.locking -= 1;
        if self.locking == 0 {
            // `notify_all`: a read-drain and a write-drain can both wait on
            // `locking_zero` concurrently; `notify_one` would strand one of them.
            locking_zero.notify_all();
        }
    }
}

impl Debug for LockStateInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Inner")
            .field("draining", &self.draining)
            .field("locking", &self.locking)
            .field("readers", &self.readers)
            .field("callbacks", &"{callbacks}")
            .finish()
    }
}

// ─── LockState ───────────────────────────────────────────────────────────────

#[derive(Default)]
pub(crate) struct LockState {
    pub(crate) inner: Mutex<LockStateInner>,
    /// Signalled when `locking` reaches 0; drainers wait on this.
    pub(crate) locking_zero: Condvar,
    /// Signalled when `draining` reaches 0; `try_write_or_else` waits on this.
    pub(crate) not_draining: Condvar,
}

impl LockState {
    // Interior mutability is the point: this is the initial value copied into
    // each new lock, exactly like `RawRwLock::INIT`.
    #[allow(clippy::declare_interior_mutable_const)]
    pub(crate) const NEW: Self = Self {
        inner: Mutex::new(LockStateInner::NEW),
        locking_zero: Condvar::new(),
        not_draining: Condvar::new(),
    };

    pub(crate) fn decrement_locking(&self) {
        self.inner.lock().decrement_locking(&self.locking_zero);
    }
}

impl Debug for LockState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.inner.try_lock() {
            Some(inner) => Debug::fmt(&inner, f),
            None => f
                .debug_struct("LockState")
                .field("inner", &"<locked>")
                .finish(),
        }
    }
}

// ─── draining ────────────────────────────────────────────────────────────────

/// Closes this drain, waking [`try_write_or_else`] waiters once the *last*
/// concurrent drain is done, then runs every callback. Re-raises the first
/// callback panic (if any) once the queue is fully drained — but only if we
/// aren't already unwinding.
///
/// Caller must have bumped `draining` and taken the callback queue, and must
/// already have released the lock it held.
///
/// [`try_write_or_else`]: crate::RwLockBell::try_write_or_else
pub(crate) fn drain_and_run(state: &LockState, callbacks: Vec<Callback>) {
    #[cfg(test)]
    hooks::run(hooks::HookPoint::DrainAfterWriteLockRelease);

    {
        let mut inner = state.inner.lock();
        inner.draining -= 1;
        // Only the last drain out reopens the gate: an overlapping drain may
        // still be holding the lock, and a callback queued against it now
        // would be stranded.
        if inner.draining == 0 {
            state.not_draining.notify_all();
        }
    }

    #[cfg(test)]
    hooks::run(hooks::HookPoint::DrainBeforeCallbacks);

    // Run every callback, remember the first panic, re-raise after draining.
    // `.or(result)` (not `.or_else`) ensures `catch_unwind` is called for
    // every callback even after one has panicked.
    let first_panic = callbacks.into_iter().fold(None, |first, callback| {
        let result = panic::catch_unwind(AssertUnwindSafe(callback)).err();
        first.or(result)
    });

    if let Some(payload) = first_panic {
        // If we're already mid-unwind (e.g. the guard was dropped during
        // stack unwinding), re-raising would double-panic and abort. The
        // outer panic carries the primary failure — drop the inner payload.
        if std::thread::panicking() {
            drop(payload);
        } else {
            panic::resume_unwind(payload);
        }
    }
}

/// Calls `f`; if `f` panics, runs `on_panic` before the panic propagates.
pub(crate) fn catch_panic<T>(f: impl FnOnce() -> T, on_panic: impl FnOnce()) -> T {
    struct Guard<F: FnOnce()>(Option<F>);
    impl<F: FnOnce()> Drop for Guard<F> {
        fn drop(&mut self) {
            self.0.take().unwrap()()
        }
    }

    let guard = Guard(Some(on_panic));
    let res = f();
    forget(guard);
    res
}
