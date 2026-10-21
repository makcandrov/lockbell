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
/// `lock_api::RwLock<RawRwLockBell, T>`, `force_unlock_read` /
/// `force_unlock_write` release a lock, so they run queued callbacks — and can
/// therefore block and panic.
///
/// # Requirements on `R`
///
/// [`RawRwLock`] alone is not enough. Substituting your own `R` also means:
///
/// - **Its acquire and release methods must not unwind.** Each runs with bell
///   bookkeeping already committed, so a panic strands `readers`, `locking` or
///   `draining` above zero and wedges the bell permanently: callbacks queue and
///   never ring, or every later drain blocks forever.
/// - **[`try_lock_shared`], [`unlock_shared`] and [`unlock_exclusive`] must not
///   block or re-enter this lock.** All three are called with the bell's own
///   mutex held — `unlock_exclusive` on the branch that skips the drain.
/// - **[`try_lock_exclusive`] must not block either.** It runs between bumping
///   `locking` and resolving it, and a concurrent release sits in that window
///   waiting for `locking` to reach zero while still holding the lock. Blocking
///   there stalls every drain and every other `try_write_or`.
/// - **[`SharedGuard`] and [`ExclusiveGuard`] are `R`'s.** A lock whose guard
///   state is `Send` yields `Send` guards, so the release — and with it the
///   whole callback batch — can run on a thread that never acquired.
///
/// [`parking_lot::RawRwLock`] satisfies all four.
///
/// # Extending it
///
/// Do **not** implement [`RawRwLockFair`], [`RawRwLockDowngrade`] or
/// [`RawRwLockUpgrade`] by plain delegation: each bypasses
/// [`lock_shared`]/[`unlock_shared`], so `unlock_shared_fair` would leak a
/// reader and `unlock_exclusive_fair` would skip the drain.
///
/// [`RawRwLockFair`]: parking_lot::lock_api::RawRwLockFair
/// [`RawRwLockDowngrade`]: parking_lot::lock_api::RawRwLockDowngrade
/// [`RawRwLockUpgrade`]: parking_lot::lock_api::RawRwLockUpgrade
/// [`SharedGuard`]: parking_lot::lock_api::RawRwLock::SharedGuard
/// [`ExclusiveGuard`]: parking_lot::lock_api::RawRwLock::ExclusiveGuard
/// [`lock_shared`]: parking_lot::lock_api::RawRwLock::lock_shared
/// [`try_lock_shared`]: parking_lot::lock_api::RawRwLock::try_lock_shared
/// [`unlock_shared`]: parking_lot::lock_api::RawRwLock::unlock_shared
/// [`try_lock_exclusive`]: parking_lot::lock_api::RawRwLock::try_lock_exclusive
/// [`unlock_exclusive`]: parking_lot::lock_api::RawRwLock::unlock_exclusive
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
// bookkeeping never grants or releases access to the protected data. Unlocks
// unwind only from callbacks, after `self.raw` has released, as the trait allows.
unsafe impl<R: RawRwLock> RawRwLock for RawRwLockBell<R> {
    const INIT: Self = Self {
        raw: <R as RawRwLock>::INIT,
        state: LockState::NEW,
    };

    type SharedGuard = <R as RawRwLock>::SharedGuard;
    type ExclusiveGuard = <R as RawRwLock>::ExclusiveGuard;

    fn lock_shared(&self) -> Self::SharedGuard {
        // Counted before acquiring, so no window exists in which we hold the
        // lock uncounted and another reader's release mistakes itself for the
        // last one. Counting a waiter early only ever defers a drain to us.
        self.state.inner.lock().readers += 1;
        self.raw.lock_shared()
    }

    fn try_lock_shared(&self) -> Option<Self::SharedGuard> {
        // Non-blocking, so the whole thing fits under the state mutex.
        let mut inner = self.state.inner.lock();
        let guard = self.raw.try_lock_shared()?;
        inner.readers += 1;
        Some(guard)
    }

    unsafe fn unlock_shared(&self, guard: Self::SharedGuard) {
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
            unsafe { self.raw.unlock_shared(guard) };
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
        unsafe { self.raw.unlock_shared(guard) };

        drain_and_run(&self.state, callbacks);
    }

    fn lock_exclusive(&self) -> Self::ExclusiveGuard {
        self.raw.lock_exclusive()
    }

    fn try_lock_exclusive(&self) -> Option<Self::ExclusiveGuard> {
        self.raw.try_lock_exclusive()
    }

    unsafe fn unlock_exclusive(&self, guard: Self::ExclusiveGuard) {
        #[cfg(test)]
        hooks::run(hooks::HookPoint::WriteGuardBeforeDrop);

        let mut inner = self.state.inner.lock();

        // Nothing queued and nothing in flight: there is no batch to take, so
        // skip the drain and leave the gate open. Closing it here made every
        // concurrent `try_write_or` wait out a release that had nothing to
        // ring — the common case on a write-heavy lock.
        //
        // `draining > 0` deliberately still drains. It already implies an
        // empty queue and `locking == 0` — the gate is shut, so nothing can
        // have been pushed since — but keeping the bump preserves the rule
        // that a release overlapping a drain's tail holds the gate shut until
        // it is itself out.
        if inner.draining == 0 && inner.callbacks.is_empty() && inner.locking == 0 {
            #[cfg(test)]
            hooks::run(hooks::HookPoint::WriteGuardBeforeSkippedDrainUnlock);

            // Released under the state mutex, exactly as in `unlock_shared`.
            // Dropping it first would leave a window where we still hold the
            // lock having already declined to drain: a `try_write_or` could
            // fail against us and queue with nobody left to ring it.
            //
            // SAFETY: forwarded from this method's own contract.
            unsafe { self.raw.unlock_exclusive(guard) };
            drop(inner);
            return;
        }

        inner.draining += 1;

        #[cfg(test)]
        hooks::run(hooks::HookPoint::WriteGuardAfterEnteringDrain);

        // Wait out every in-flight `try_write_or`.
        while inner.locking != 0 {
            self.state.locking_zero.wait(&mut inner);
        }
        let callbacks = take(&mut inner.callbacks);
        drop(inner);

        #[cfg(test)]
        hooks::run(hooks::HookPoint::WriteGuardBeforeRawUnlock);

        // SAFETY: forwarded from this method's own contract.
        unsafe { self.raw.unlock_exclusive(guard) };

        drain_and_run(&self.state, callbacks);
    }

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
    /// [`RwLockBell::try_write_or`](crate::RwLockBell::try_write_or). On
    /// `Some(state)` the caller owns the exclusive lock and must release it
    /// exactly once with `state`, and `callback` is discarded.
    ///
    /// Not wait-free: it waits out any concurrent flush. Unlike
    /// [`try_lock_exclusive_or_else`], no user code runs inside that window.
    ///
    /// # Discarding the callback
    ///
    /// On success, `callback` is dropped here — **with the lock held**, before
    /// the caller can build a guard. So its captures' destructors must not
    /// touch this lock: doing so deadlocks. A destructor that panics is
    /// survivable, though — the lock is released before the panic propagates,
    /// so it is not stranded.
    ///
    /// [`try_lock_exclusive_or_else`]: Self::try_lock_exclusive_or_else
    pub fn try_lock_exclusive_or<Callback>(
        &self,
        callback: Callback,
    ) -> Option<<Self as RawRwLock>::ExclusiveGuard>
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

        if let Some(guard) = self.raw.try_lock_exclusive() {
            self.state.decrement_locking();

            // The discarded `callback` drops here, holding the lock with no
            // guard yet built to release it: a panicking destructor would
            // strand it for good. Release on the way out, exactly as the guard
            // the caller is about to build would have.
            let mut guard = Some(guard);
            // SAFETY: acquired just above and not yet handed to a guard, so we
            // own the release. Scoped to this branch: on the other one we hold
            // nothing, and releasing there would unlock a lock we never took.
            catch_panic(
                move || drop(callback),
                || unsafe { <Self as RawRwLock>::unlock_exclusive(self, guard.take().unwrap()) },
            );
            guard
        } else {
            // Boxed only here, so an uncontended call never allocates. Nothing
            // between the failed acquire and the push can panic, so unlike
            // `try_lock_exclusive_or_else` there is no `locking` to protect.
            let cb = Box::new(callback);

            let mut inner = self.state.inner.lock();
            inner.callbacks.push(cb);
            inner.decrement_locking(&self.state.locking_zero);
            None
        }
    }

    /// Like [`try_lock_exclusive_or`], but builds the callback lazily:
    /// `factory` runs only on contention. On `Some(state)` the caller owns the
    /// exclusive lock and must release it exactly once with `state`.
    ///
    /// The raw counterpart of
    /// [`RwLockBell::try_write_or_else`](crate::RwLockBell::try_write_or_else).
    ///
    /// Same rules as there: `factory` runs inside the window a concurrent
    /// unlock waits on, so it must not touch this lock and must not block.
    ///
    /// On success the unused `factory` is discarded under the same rule as the
    /// callback in [`try_lock_exclusive_or`]: it drops with the lock held, so
    /// its captures' destructors must not touch this lock.
    ///
    /// [`try_lock_exclusive_or`]: Self::try_lock_exclusive_or
    pub fn try_lock_exclusive_or_else<Callback, Factory>(
        &self,
        factory: Factory,
    ) -> Option<<Self as RawRwLock>::ExclusiveGuard>
    where
        Factory: FnOnce() -> Callback,
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

        if let Some(guard) = self.raw.try_lock_exclusive() {
            self.state.decrement_locking();

            // The unused `factory` drops here under the same rule as the
            // callback in `try_lock_exclusive_or`.
            let mut guard = Some(guard);
            // SAFETY: acquired just above and not yet handed to a guard.
            catch_panic(
                move || drop(factory),
                || unsafe { <Self as RawRwLock>::unlock_exclusive(self, guard.take().unwrap()) },
            );
            guard
        } else {
            // Decrement even if the factory panics, or drainers wait forever.
            let cb = catch_panic(factory, || self.state.decrement_locking());
            let cb = Box::new(cb);

            let mut inner = self.state.inner.lock();
            inner.callbacks.push(cb);
            inner.decrement_locking(&self.state.locking_zero);
            None
        }
    }
}
