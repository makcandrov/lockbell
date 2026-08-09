//! The raw lock that rings the bell.

use std::{fmt::Debug, mem::take};

use parking_lot::lock_api::RawRwLock as _;
use parking_lot::{RawRwLock, lock_api};

use crate::state::{LockState, catch_panic, drain_and_run};
#[cfg(test)]
use crate::tests::hooks;

/// The [`lock_api::RawRwLock`] behind [`RwLockBell`](crate::RwLockBell):
/// [`parking_lot::RawRwLock`] plus the callback queue.
///
/// # Using it directly
///
/// Prefer [`RwLockBell`](crate::RwLockBell). In a bare
/// `lock_api::RwLock<RawRwLockBell, T>`, the `Debug` impl and
/// `force_unlock_read` / `force_unlock_write` all release a lock, so they run
/// queued callbacks — and can therefore block and panic.
///
/// # Extending it
///
/// Do **not** implement [`RawRwLockFair`], [`RawRwLockDowngrade`],
/// [`RawRwLockRecursive`] or [`RawRwLockUpgrade`] by plain delegation: each
/// bypasses [`lock_shared`]/[`unlock_shared`], so `unlock_shared_fair` would
/// leak a reader and `unlock_exclusive_fair` would skip the drain.
///
/// [`RawRwLockFair`]: lock_api::RawRwLockFair
/// [`RawRwLockDowngrade`]: lock_api::RawRwLockDowngrade
/// [`RawRwLockRecursive`]: lock_api::RawRwLockRecursive
/// [`RawRwLockUpgrade`]: lock_api::RawRwLockUpgrade
/// [`lock_shared`]: lock_api::RawRwLock::lock_shared
/// [`unlock_shared`]: lock_api::RawRwLock::unlock_shared
pub struct RawRwLockBell {
    raw: RawRwLock,
    state: LockState,
}

impl Default for RawRwLockBell {
    fn default() -> Self {
        <Self as lock_api::RawRwLock>::INIT
    }
}

impl Debug for RawRwLockBell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawRwLockBell")
            .field("locked", &self.raw.is_locked())
            .field("state", &self.state)
            .finish()
    }
}

// SAFETY: every method forwards its locking obligation to `self.raw`. The bell
// bookkeeping never grants or releases access to the protected data.
unsafe impl lock_api::RawRwLock for RawRwLockBell {
    #[allow(clippy::declare_interior_mutable_const)]
    const INIT: Self = Self {
        raw: <RawRwLock as lock_api::RawRwLock>::INIT,
        state: LockState::NEW,
    };

    type GuardMarker = <RawRwLock as lock_api::RawRwLock>::GuardMarker;

    fn lock_shared(&self) {
        self.raw.lock_shared();
        self.state.inner.lock().readers += 1;
    }

    fn try_lock_shared(&self) -> bool {
        if self.raw.try_lock_shared() {
            self.state.inner.lock().readers += 1;
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
            drop(inner);

            // SAFETY: forwarded from this method's own contract.
            unsafe { self.raw.unlock_shared() };
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

impl RawRwLockBell {
    /// Attempts to acquire the exclusive lock; on failure, queues the callback
    /// produced by `factory`.
    ///
    /// The raw counterpart of
    /// [`RwLockBell::try_write_or_else`](crate::RwLockBell::try_write_or_else),
    /// and the reason to reach for this type. On `true` the caller owns the
    /// exclusive lock and must release it exactly once.
    ///
    /// Same rules as there: `factory` must not touch this lock or block, and
    /// the call is not wait-free — it waits out any concurrent flush.
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
            let cb: crate::state::Callback = Box::new(cb);

            let mut inner = self.state.inner.lock();
            inner.callbacks.push(cb);
            inner.decrement_locking(&self.state.locking_zero);
            false
        }
    }

    /// Takes a shared lock without touching the bell: no reader accounting, no
    /// drain on release. Lets `Debug` format a lock without ringing it.
    pub(crate) fn peek(&self) -> Option<Peek<'_>> {
        self.raw.try_lock_shared().then(|| Peek(&self.raw))
    }
}

/// Holds a bell-free shared lock for as long as it lives.
pub(crate) struct Peek<'a>(&'a RawRwLock);

impl Drop for Peek<'_> {
    fn drop(&mut self) {
        // SAFETY: only constructed after a successful `try_lock_shared`.
        unsafe { self.0.unlock_shared() };
    }
}
