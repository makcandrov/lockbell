//! The raw lock that rings the bell.

use std::{fmt::Debug, mem::take};

use parking_lot::lock_api::RawRwLock;

use crate::state::{LockState, catch_panic, drain_and_run};
#[cfg(test)]
use crate::tests::hooks;

/// The [`RawRwLock`] behind [`RwLockBell`](crate::RwLockBell): a raw
/// reader-writer lock `R` plus the callback queue.
///
/// `R` defaults to [`parking_lot::RawRwLock`], which is what `RwLockBell` uses.
///
/// # Using it directly
///
/// Prefer [`RwLockBell`](crate::RwLockBell). In a bare
/// `lock_api::RwLock<RawRwLockBell, T>`, the `Debug` impl and
/// `force_unlock_read` / `force_unlock_write` all release a lock, so they run
/// queued callbacks — and can therefore block and panic.
///
/// # Requirements on `R`
///
/// [`RawRwLock`] alone is not enough. Substituting your own `R` also means:
///
/// - **Its acquire and release methods must not unwind.** Each runs with bell
///   bookkeeping already committed, so a panic strands `readers`, `locking` or
///   `draining` above zero and wedges the bell permanently: callbacks queue and
///   never ring, or every later drain blocks forever.
/// - **[`try_lock_shared`] and [`unlock_shared`] must not block or re-enter
///   this lock.** Both are called with the bell's own mutex held.
/// - **[`GuardMarker`] is `R`'s.** A `GuardSend` lock yields `Send` guards, so
///   the release — and with it the whole callback batch — can run on a thread
///   that never acquired.
///
/// [`parking_lot::RawRwLock`] satisfies all three.
///
/// # Extending it
///
/// Do **not** implement [`RawRwLockFair`], [`RawRwLockDowngrade`],
/// [`RawRwLockRecursive`] or [`RawRwLockUpgrade`] by plain delegation: each
/// bypasses [`lock_shared`]/[`unlock_shared`], so `unlock_shared_fair` would
/// leak a reader and `unlock_exclusive_fair` would skip the drain.
///
/// [`RawRwLockFair`]: parking_lot::lock_api::RawRwLockFair
/// [`RawRwLockDowngrade`]: parking_lot::lock_api::RawRwLockDowngrade
/// [`RawRwLockRecursive`]: parking_lot::lock_api::RawRwLockRecursive
/// [`RawRwLockUpgrade`]: parking_lot::lock_api::RawRwLockUpgrade
/// [`GuardMarker`]: parking_lot::lock_api::RawRwLock::GuardMarker
/// [`lock_shared`]: parking_lot::lock_api::RawRwLock::lock_shared
/// [`try_lock_shared`]: parking_lot::lock_api::RawRwLock::try_lock_shared
/// [`unlock_shared`]: parking_lot::lock_api::RawRwLock::unlock_shared
pub struct RawRwLockBell<R = parking_lot::RawRwLock> {
    raw: R,
    state: LockState,
}

impl<R: RawRwLock> Default for RawRwLockBell<R> {
    fn default() -> Self {
        <Self as RawRwLock>::INIT
    }
}

impl<R: RawRwLock> Debug for RawRwLockBell<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawRwLockBell")
            .field("locked", &self.raw.is_locked())
            .field("state", &self.state)
            .finish()
    }
}

// SAFETY: every method forwards its locking obligation to `self.raw`. The bell
// bookkeeping never grants or releases access to the protected data.
unsafe impl<R: RawRwLock> RawRwLock for RawRwLockBell<R> {
    const INIT: Self = Self {
        raw: <R as RawRwLock>::INIT,
        state: LockState::NEW,
    };

    type GuardMarker = <R as RawRwLock>::GuardMarker;

    fn lock_shared(&self) {
        // Counted before acquiring, so no window exists in which we hold the
        // lock uncounted and another reader's release mistakes itself for the
        // last one. Counting a waiter early only ever defers a drain to us.
        self.state.inner.lock().readers += 1;
        self.raw.lock_shared();
    }

    fn try_lock_shared(&self) -> bool {
        // Non-blocking, so the whole thing fits under the state mutex.
        let mut inner = self.state.inner.lock();
        if self.raw.try_lock_shared() {
            inner.readers += 1;
            true
        } else {
            false
        }
    }

    unsafe fn unlock_shared(&self) {
        // Bookkeeping before the release, as in `unlock_exclusive`. Releasing
        // first would let another thread take the lock while `readers` still
        // reads non-zero, and our batch could then sweep up callbacks that lost
        // to *that* thread — rung while it still holds, and never rung again.
        let mut inner = self.state.inner.lock();

        // Wrapping would park `readers` near `u64::MAX` and silently kill the
        // read-side bell for good.
        debug_assert!(inner.readers > 0, "unbalanced unlock_shared");
        inner.readers = inner.readers.saturating_sub(1);

        // Only the last reader drains. An in-flight drain either has not taken
        // the queue yet (and waits on `locking`) or has, leaving it empty until
        // `draining` hits 0.
        if inner.readers > 0
            || inner.draining > 0
            || (inner.callbacks.is_empty() && inner.locking == 0)
        {
            #[cfg(test)]
            hooks::run(hooks::HookPoint::ReadGuardBeforeSkippedDrainUnlock);

            // Released under the state mutex. Dropping it first would leave a
            // window where we still hold the lock having already declined to
            // drain: a `try_write_or` could fail against us and queue with
            // nobody left to ring it. Neither guard above covers that — the
            // drain we deferred to can finish inside the window, and the queue
            // we found empty can be filled inside it.
            //
            // SAFETY: forwarded from this method's own contract.
            unsafe { self.raw.unlock_shared() };
            drop(inner);
            return;
        }

        inner.draining += 1;

        #[cfg(test)]
        hooks::run(hooks::HookPoint::ReadGuardAfterEnteringDrain);

        // Wait out every in-flight `try_write_or`. We still hold the shared
        // lock, so they are all bound to fail against us and queue.
        while inner.locking != 0 {
            self.state.locking_zero.wait(&mut inner);
        }
        let callbacks = take(&mut inner.callbacks);
        drop(inner);

        #[cfg(test)]
        hooks::run(hooks::HookPoint::ReadGuardBeforeRawUnlock);

        // SAFETY: forwarded from this method's own contract.
        unsafe { self.raw.unlock_shared() };

        drain_and_run(&self.state, callbacks);
    }

    fn lock_exclusive(&self) {
        self.raw.lock_exclusive();
    }

    fn try_lock_exclusive(&self) -> bool {
        self.raw.try_lock_exclusive()
    }

    unsafe fn unlock_exclusive(&self) {
        #[cfg(test)]
        hooks::run(hooks::HookPoint::WriteGuardBeforeDrop);

        let callbacks = {
            let mut inner = self.state.inner.lock();
            inner.draining += 1;

            #[cfg(test)]
            hooks::run(hooks::HookPoint::WriteGuardAfterEnteringDrain);

            // Wait out every in-flight `try_write_or`.
            while inner.locking != 0 {
                self.state.locking_zero.wait(&mut inner);
            }
            take(&mut inner.callbacks)
        };

        #[cfg(test)]
        hooks::run(hooks::HookPoint::WriteGuardBeforeRawUnlock);

        // SAFETY: forwarded from this method's own contract.
        unsafe { self.raw.unlock_exclusive() };

        drain_and_run(&self.state, callbacks);
    }

    // The provided impls probe by acquiring and releasing, which would ring.
    fn is_locked(&self) -> bool {
        self.raw.is_locked()
    }

    fn is_locked_exclusive(&self) -> bool {
        self.raw.is_locked_exclusive()
    }
}

impl<R: RawRwLock> RawRwLockBell<R> {
    /// Attempts to acquire the exclusive lock; on failure, queues `callback`.
    ///
    /// The raw counterpart of
    /// [`RwLockBell::try_write_or`](crate::RwLockBell::try_write_or). On `true`
    /// the caller owns the exclusive lock and must release it exactly once, and
    /// `callback` is discarded.
    ///
    /// Not wait-free: it waits out any concurrent flush. Unlike
    /// [`try_lock_exclusive_or_else`], no user code runs inside that window.
    ///
    /// [`try_lock_exclusive_or_else`]: Self::try_lock_exclusive_or_else
    pub fn try_lock_exclusive_or<Callback>(&self, callback: Callback) -> bool
    where
        Callback: FnOnce() + Send + 'static,
    {
        // Check and bump under one lock, so the `draining` view can't go stale.
        let mut inner = self.state.inner.lock();

        while inner.draining > 0 {
            #[cfg(test)]
            hooks::run(hooks::HookPoint::TryWriteOrWhileDraining);

            self.state.not_draining.wait(&mut inner);
        }
        inner.locking += 1;
        drop(inner);

        #[cfg(test)]
        hooks::run(hooks::HookPoint::TryWriteOrBeforeAcquire);

        if self.raw.try_lock_exclusive() {
            self.state.decrement_locking();
            true
        } else {
            // Boxed only here, so an uncontended call never allocates. Nothing
            // between the failed acquire and the push can panic, so unlike
            // `try_lock_exclusive_or_else` there is no `locking` to protect.
            let cb = Box::new(callback);

            let mut inner = self.state.inner.lock();
            inner.callbacks.push(cb);
            inner.decrement_locking(&self.state.locking_zero);
            false
        }
    }

    /// Like [`try_lock_exclusive_or`], but builds the callback lazily:
    /// `factory` runs only on contention. On `true` the caller owns the
    /// exclusive lock and must release it exactly once.
    ///
    /// The raw counterpart of
    /// [`RwLockBell::try_write_or_else`](crate::RwLockBell::try_write_or_else).
    ///
    /// Same rules as there: `factory` runs inside the window a concurrent
    /// unlock waits on, so it must not touch this lock and must not block.
    ///
    /// [`try_lock_exclusive_or`]: Self::try_lock_exclusive_or
    pub fn try_lock_exclusive_or_else<Callback>(&self, factory: impl FnOnce() -> Callback) -> bool
    where
        Callback: FnOnce() + Send + 'static,
    {
        // Check and bump under one lock, so the `draining` view can't go stale.
        let mut inner = self.state.inner.lock();

        while inner.draining > 0 {
            #[cfg(test)]
            hooks::run(hooks::HookPoint::TryWriteOrWhileDraining);

            self.state.not_draining.wait(&mut inner);
        }
        inner.locking += 1;
        drop(inner);

        #[cfg(test)]
        hooks::run(hooks::HookPoint::TryWriteOrBeforeAcquire);

        if self.raw.try_lock_exclusive() {
            self.state.decrement_locking();
            true
        } else {
            // Decrement even if the factory panics, or drainers wait forever.
            let cb = catch_panic(factory, || self.state.decrement_locking());
            let cb = Box::new(cb);

            let mut inner = self.state.inner.lock();
            inner.callbacks.push(cb);
            inner.decrement_locking(&self.state.locking_zero);
            false
        }
    }

    /// Takes a shared lock without touching the bell: no reader accounting, no
    /// drain on release. Lets `Debug` format a lock without ringing it.
    pub(crate) fn peek(&self) -> Option<Peek<'_, R>> {
        self.raw.try_lock_shared().then(|| Peek(&self.raw))
    }
}

/// Holds a bell-free shared lock for as long as it lives.
pub(crate) struct Peek<'a, R: RawRwLock>(&'a R);

impl<R: RawRwLock> Drop for Peek<'_, R> {
    fn drop(&mut self) {
        // SAFETY: only constructed after a successful `try_lock_shared`.
        unsafe { self.0.unlock_shared() };
    }
}
