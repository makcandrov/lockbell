//! The raw lock that rings the bell.

use std::{fmt::Debug, mem::take};

use parking_lot::lock_api::RawRwLock as _;
use parking_lot::{RawRwLock, lock_api};

use crate::state::{LockState, catch_panic, drain_and_run};
#[cfg(test)]
use crate::tests::hooks;

/// Raw reader-writer lock that fires queued callbacks when contention clears.
///
/// This is the [`lock_api::RawRwLock`] implementation behind
/// [`RwLockBell`](crate::RwLockBell); you rarely need to name it. It wraps
/// [`parking_lot::RawRwLock`] and adds the callback queue.
///
/// # Using it directly
///
/// Nothing stops you from building a `lock_api::RwLock<RawRwLockBell, T>`
/// yourself, but [`RwLockBell`](crate::RwLockBell) exists precisely to keep
/// the bell out of operations where firing it would surprise you. In
/// particular, `lock_api`'s `Debug` impl and `force_unlock_read` /
/// `force_unlock_write` all release a lock, so with the raw type they run
/// queued callbacks — which means they can block and can panic. Prefer
/// [`RwLockBell`](crate::RwLockBell).
///
/// # Extending it
///
/// Do **not** implement [`RawRwLockFair`], [`RawRwLockDowngrade`],
/// [`RawRwLockRecursive`] or [`RawRwLockUpgrade`] by plain delegation. Each of
/// them acquires or releases a lock without passing through
/// [`lock_shared`]/[`unlock_shared`], so the reader count would drift:
/// `unlock_shared_fair` would leak a reader (permanently disabling the
/// read-side drain) and `unlock_exclusive_fair` would skip the drain outright.
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

// SAFETY: every method forwards its locking obligation to `self.raw`, which is
// a correct `RawRwLock`. The bell bookkeeping in `state` is guarded by its own
// mutex and never grants or releases access to the protected data.
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
        // SAFETY: forwarded from this method's own contract.
        unsafe { self.raw.unlock_shared() };

        #[cfg(test)]
        hooks::run(hooks::HookPoint::ReadGuardAfterRelease);

        let callbacks = {
            let mut inner = self.state.inner.lock();
            inner.readers -= 1;
            // Only the last reader drains; skip if another drain is already in flight.
            if inner.readers > 0
                || inner.dropping
                || (inner.callbacks.is_empty() && inner.locking == 0)
            {
                return;
            }
            inner.dropping = true;

            #[cfg(test)]
            hooks::run(hooks::HookPoint::ReadGuardAfterSettingDropping);

            while inner.locking != 0 {
                self.state.locking_zero.wait(&mut inner);
            }
            take(&mut inner.callbacks)
        };

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
            inner.dropping = true;

            #[cfg(test)]
            hooks::run(hooks::HookPoint::WriteGuardAfterSettingDropping);

            // Wait until every in-flight `try_write_or` has either pushed its
            // callback or obtained the lock.
            while inner.locking != 0 {
                self.state.locking_zero.wait(&mut inner);
            }
            take(&mut inner.callbacks)
            // Mutex released here.
        };

        // SAFETY: forwarded from this method's own contract.
        unsafe { self.raw.unlock_exclusive() };

        drain_and_run(&self.state, callbacks);
    }

    // The provided implementations probe the lock by acquiring and releasing
    // it, which would fire the bell. Delegate instead.
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
    /// This is the raw counterpart of
    /// [`RwLockBell::try_write_or_else`](crate::RwLockBell::try_write_or_else)
    /// — the operation `lock_api` has no equivalent for, and the reason to
    /// reach for this type directly. On `true` the caller owns the exclusive
    /// lock and must release it exactly once; on `false` nothing was acquired
    /// and the callback is queued.
    ///
    /// The same deadlock and panic rules apply to `factory`: it runs while a
    /// concurrent unlock waits on it, so it must not touch this lock and must
    /// not block.
    pub fn try_lock_exclusive_or_else<Callback>(&self, factory: impl FnOnce() -> Callback) -> bool
    where
        Callback: FnOnce() + Send + 'static,
    {
        // Wait while a drain is running, then bump `locking` — both under the
        // same mutex so the `dropping` view can't go stale between the check
        // and the increment.
        let mut inner = self.state.inner.lock();

        while inner.dropping {
            #[cfg(test)]
            hooks::run(hooks::HookPoint::TryWriteOrWhileDropping);

            self.state.not_dropping.wait(&mut inner);
        }
        inner.locking += 1;
        drop(inner);

        #[cfg(test)]
        hooks::run(hooks::HookPoint::TryWriteOrBeforeAcquire);

        if self.raw.try_lock_exclusive() {
            self.state.decrement_locking();
            true
        } else {
            // Decrement `locking` even if the factory panics — otherwise
            // drainers would wait on `locking_zero` forever.
            let cb = catch_panic(factory, || self.state.decrement_locking());
            let cb: crate::state::Callback = Box::new(cb);

            let mut inner = self.state.inner.lock();
            inner.callbacks.push(cb);
            inner.decrement_locking(&self.state.locking_zero);
            false
        }
    }

    /// Takes a shared lock **without** touching the bell: no reader accounting,
    /// and no drain when the returned [`Peek`] is dropped.
    ///
    /// Used by [`RwLockBell`](crate::RwLockBell)'s `Debug` impl so that
    /// formatting a lock can never run a callback, block, or panic.
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
