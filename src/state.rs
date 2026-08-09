//! Bell bookkeeping. Lives inside [`RawRwLockBell`](crate::RawRwLockBell) under
//! its own mutex; never grants or releases access to the protected data.

use std::{
    fmt::Debug,
    mem::forget,
    panic::{self, AssertUnwindSafe},
};

use parking_lot::{Condvar, Mutex};

#[cfg(test)]
use crate::tests::hooks;

pub(crate) type Callback = Box<dyn FnOnce() + Send>;

#[derive(Default)]
pub(crate) struct LockStateInner {
    /// Drains in flight. A count, not a flag: drains overlap, and one still
    /// holds the lock while another finishes. Reopening the gate on the first
    /// to finish would let a callback be queued into an already-taken batch.
    pub(crate) draining: u64,
    /// In-flight `try_write_or_else` calls, between bumping and resolving.
    pub(crate) locking: u64,
    /// Live shared-lock count.
    pub(crate) readers: u64,
    /// FIFO queue of callbacks from failed `try_write_or` calls.
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
            // Only one drainer can be parked here, so `notify_one` would do;
            // `notify_all` avoids depending on that argument.
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

#[derive(Default)]
pub(crate) struct LockState {
    pub(crate) inner: Mutex<LockStateInner>,
    /// Signalled when `locking` reaches 0; drainers wait on this.
    pub(crate) locking_zero: Condvar,
    /// Signalled when `draining` reaches 0; `try_write_or_else` waits on this.
    pub(crate) not_draining: Condvar,
}

impl LockState {
    // Interior mutability is the point, as with `RawRwLock::INIT`.
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

/// Closes this drain, then runs every callback, re-raising the first panic.
///
/// Caller must have bumped `draining`, taken the queue, and released its lock.
pub(crate) fn drain_and_run(state: &LockState, callbacks: Vec<Callback>) {
    #[cfg(test)]
    hooks::run(hooks::HookPoint::DrainBeforeGateReopens);

    {
        let mut inner = state.inner.lock();
        inner.draining -= 1;
        // Only the last drain out reopens the gate; an overlapping one may
        // still hold the lock.
        if inner.draining == 0 {
            state.not_draining.notify_all();
        }
    }

    #[cfg(test)]
    hooks::run(hooks::HookPoint::DrainBeforeCallbacks);

    // `.or(result)`, not `.or_else`: every callback must still run after one
    // has panicked.
    let first_panic = callbacks.into_iter().fold(None, |first, callback| {
        let result = panic::catch_unwind(AssertUnwindSafe(callback)).err();
        first.or(result)
    });

    if let Some(payload) = first_panic {
        // Re-raising mid-unwind would abort; the outer panic is the primary one.
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
