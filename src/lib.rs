#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![doc = include_str!("../README.md")]

#[cfg(test)]
mod tests;
#[cfg(test)]
use tests::hooks;

use std::{
    fmt::Debug,
    mem::{forget, take},
    ops::{Deref, DerefMut},
    panic::{self, AssertUnwindSafe},
};

use parking_lot::{
    Condvar, MappedRwLockReadGuard, MappedRwLockWriteGuard, Mutex, RwLock, RwLockReadGuard,
    RwLockWriteGuard,
};

/// Drop guard: decrements `locking` and notifies `locking_zero` on zero.
/// Ensures `try_write_or_else` cannot leak the increment if the callback
/// factory panics.
struct LockingDec<'a>(&'a LockState);

impl Drop for LockingDec<'_> {
    fn drop(&mut self) {
        let mut inner = self.0.inner.lock();
        inner.locking -= 1;
        if inner.locking == 0 {
            // notify_all: read- and write-drains can wait concurrently — a
            // reader releases its lock before locking state, so a writer can
            // slip in. notify_one would strand the loser.
            self.0.locking_zero.notify_all();
        }
    }
}

#[derive(Default)]
struct Inner {
    dropping: bool,
    locking: u64,
    /// Number of live [`RwLockBellReadGuard`] instances.
    readers: u64,
    callbacks: Vec<Box<dyn FnOnce() + Send>>,
}

impl Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Inner")
            .field("dropping", &self.dropping)
            .field("locking", &self.locking)
            .field("readers", &self.readers)
            .field("callbacks", &"{callbacks}")
            .finish()
    }
}

#[derive(Default)]
struct LockState {
    inner: Mutex<Inner>,
    /// Notified when `locking` reaches zero (dropper is waiting on this).
    locking_zero: Condvar,
    /// Notified when `dropping` becomes false (try_write callers wait on this).
    not_dropping: Condvar,
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

/// An [`RwLock`] wrapper that fires registered callbacks when a write guard is released.
///
/// When [`try_write_or`] cannot acquire the lock, the provided callback is queued.
/// All queued callbacks are called in FIFO order, without holding any lock, when the
/// next write guard is dropped.
///
/// [`try_write_or`]: RwLockBell::try_write_or
#[derive(Debug)]
pub struct RwLockBell<T> {
    lock: RwLock<T>,
    state: LockState,
}

impl<T> RwLockBell<T> {
    /// Creates a new `RwLockBell` wrapping `value`.
    #[inline]
    #[must_use]
    pub fn new(value: T) -> Self {
        Self::from_lock(RwLock::new(value))
    }

    /// Creates a new `RwLockBell` from an existing [`RwLock`].
    #[inline]
    #[must_use]
    pub fn from_lock(lock: RwLock<T>) -> Self {
        Self {
            lock,
            state: LockState::default(),
        }
    }

    /// Consumes the lock and returns the inner value.
    ///
    /// Any callbacks pending in the queue are dropped without being called.
    #[inline]
    #[must_use]
    pub fn into_inner(self) -> T {
        self.lock.into_inner()
    }

    /// Locks for shared read access, blocking until it can be acquired.
    ///
    /// When the returned guard is dropped, any pending callbacks registered via
    /// [`try_write_or`] are flushed if this was the last reader holding the lock.
    ///
    /// [`try_write_or`]: RwLockBell::try_write_or
    #[must_use]
    pub fn read(&self) -> RwLockBellReadGuard<'_, T> {
        let guard = self.lock.read();
        self.state.inner.lock().readers += 1;
        RwLockBellReadGuard {
            guard: Some(guard),
            state: &self.state,
        }
    }

    /// Attempts to acquire shared read access without blocking.
    ///
    /// Returns `None` if a write lock is currently held.
    #[must_use]
    pub fn try_read(&self) -> Option<RwLockBellReadGuard<'_, T>> {
        let guard = self.lock.try_read()?;
        self.state.inner.lock().readers += 1;
        Some(RwLockBellReadGuard {
            guard: Some(guard),
            state: &self.state,
        })
    }

    /// Locks for exclusive write access, blocking until it can be acquired.
    ///
    /// Callbacks registered via [`try_write_or`] while this guard is held will be
    /// called when the guard is dropped.
    ///
    /// [`try_write_or`]: RwLockBell::try_write_or
    #[must_use]
    pub fn write(&self) -> RwLockBellWriteGuard<'_, T> {
        RwLockBellWriteGuard {
            guard: Some(self.lock.write()),
            state: &self.state,
        }
    }

    /// Attempts to acquire exclusive write access without blocking.
    ///
    /// Returns `None` if the lock is currently held. No callback is registered on
    /// failure; use [`try_write_or`] to register one.
    ///
    /// [`try_write_or`]: RwLockBell::try_write_or
    #[must_use]
    pub fn try_write(&self) -> Option<RwLockBellWriteGuard<'_, T>> {
        self.lock.try_write().map(|guard| RwLockBellWriteGuard {
            guard: Some(guard),
            state: &self.state,
        })
    }

    /// Attempts to acquire exclusive write access without blocking.
    ///
    /// - **Success** — returns `Some(guard)` and discards `callback`.
    /// - **Failure** — queues `callback` and returns `None`. The callback will be
    ///   called, without holding any lock, after the next write guard is dropped.
    ///
    /// Callbacks are called in FIFO registration order.
    #[inline]
    #[must_use]
    pub fn try_write_or<'a, Callback>(
        &'a self,
        callback: Callback,
    ) -> Option<RwLockBellWriteGuard<'a, T>>
    where
        Callback: FnOnce() + Send + 'static,
    {
        self.try_write_or_else(|| callback)
    }

    /// Attempts to acquire exclusive write access without blocking, lazily constructing
    /// the callback only if the lock is unavailable.
    ///
    /// - **Success** — returns `Some(guard)` and never calls `callback`.
    /// - **Failure** — calls `callback()` to produce the callback, queues it, and returns
    ///   `None`. The callback will be called, without holding any lock, after the next
    ///   write guard is dropped.
    ///
    /// Prefer this over [`try_write_or`] when constructing the callback is expensive
    /// or has side effects that should only occur on failure.
    ///
    /// Callbacks are called in FIFO registration order.
    ///
    /// [`try_write_or`]: Self::try_write_or
    #[must_use]
    pub fn try_write_or_else<'a, Callback>(
        &'a self,
        callback: impl FnOnce() -> Callback,
    ) -> Option<RwLockBellWriteGuard<'a, T>>
    where
        Callback: FnOnce() + Send + 'static,
    {
        // Atomically wait until not dropping, then increment locking.
        // Both steps happen under the same mutex, which eliminates the TOCTOU
        // that required SeqCst atomic ordering in the spin-based version.
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

        // `LockingDec`'s Drop performs the decrement and notify_all.
        let _dec = LockingDec(&self.state);

        if let Some(guard) = self.lock.try_write() {
            Some(RwLockBellWriteGuard {
                guard: Some(guard),
                state: &self.state,
            })
        } else {
            // Build the callback box *before* re-acquiring `inner`, and arm a
            // drop guard so that `locking` is decremented even if `callback()`
            // or `Box::new` panics. Without this, a panicking factory would
            // leak the increment and permanently deadlock future drains.
            let cb: Box<dyn FnOnce() + Send> = Box::new(callback());
            self.state.inner.lock().callbacks.push(cb);
            None
        }
    }
}

impl<T> From<T> for RwLockBell<T> {
    #[inline]
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T> From<RwLock<T>> for RwLockBell<T> {
    #[inline]
    fn from(lock: RwLock<T>) -> Self {
        Self::from_lock(lock)
    }
}

impl<T> From<RwLockBell<T>> for RwLock<T> {
    #[inline]
    fn from(lock: RwLockBell<T>) -> Self {
        lock.lock
    }
}

/// RAII read guard for [`RwLockBell`].
///
/// Provides shared read access to the protected value via [`Deref`].
/// When dropped, releases the read lock and, if this was the last active
/// read guard, immediately flushes any pending callbacks that were registered
/// via [`try_write_or`].
///
/// Obtained via [`RwLockBell::read`] or [`RwLockBell::try_read`].
///
/// [`try_write_or`]: RwLockBell::try_write_or
#[derive(Debug)]
pub struct RwLockBellReadGuard<'a, T> {
    guard: Option<RwLockReadGuard<'a, T>>,
    state: &'a LockState,
}

impl<'a, T> RwLockBellReadGuard<'a, T> {
    /// Transforms this guard into a [`MappedRwLockBellReadGuard`] that dereferences
    /// to a subfield of the protected value.
    pub fn map<U, F>(mut self, f: F) -> MappedRwLockBellReadGuard<'a, U>
    where
        F: FnOnce(&T) -> &U,
    {
        let guard = self.guard.take().unwrap();
        let state = self.state;
        forget(self);
        let map_guard = RwLockReadGuard::map(guard, f);
        MappedRwLockBellReadGuard {
            guard: Some(map_guard),
            state,
        }
    }

    /// Attempts to transform this guard into a [`MappedRwLockBellReadGuard`].
    ///
    /// Returns `Err(self)` if `f` returns `None`, giving the original guard back.
    pub fn try_map<U, F>(mut self, f: F) -> Result<MappedRwLockBellReadGuard<'a, U>, Self>
    where
        F: FnOnce(&T) -> Option<&U>,
    {
        let guard = self.guard.take().unwrap();
        let state = self.state;
        forget(self);
        match RwLockReadGuard::try_map(guard, f) {
            Ok(map_guard) => Ok(MappedRwLockBellReadGuard {
                guard: Some(map_guard),
                state,
            }),
            Err(guard) => Err(Self {
                guard: Some(guard),
                state,
            }),
        }
    }

    /// Attempts to transform this guard into a [`MappedRwLockBellReadGuard`].
    ///
    /// Returns `Err((self, error))` if `f` returns `Err`, giving the original guard
    /// and the error back.
    pub fn try_map_or_err<U, F, E>(
        mut self,
        f: F,
    ) -> Result<MappedRwLockBellReadGuard<'a, U>, (Self, E)>
    where
        F: FnOnce(&T) -> Result<&U, E>,
    {
        let guard = self.guard.take().unwrap();
        let state = self.state;
        forget(self);
        match RwLockReadGuard::try_map_or_err(guard, f) {
            Ok(map_guard) => Ok(MappedRwLockBellReadGuard {
                guard: Some(map_guard),
                state,
            }),
            Err((guard, err)) => Err((
                Self {
                    guard: Some(guard),
                    state,
                },
                err,
            )),
        }
    }
}

impl<'a, T> Deref for RwLockBellReadGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        Deref::deref(self.guard.as_ref().unwrap())
    }
}

impl<'a, T> Drop for RwLockBellReadGuard<'a, T> {
    fn drop(&mut self) {
        drop_read_guard(&mut self.guard, self.state)
    }
}

/// RAII read guard produced by [`RwLockBellReadGuard::map`] and related methods.
///
/// Provides shared read access to a subfield of the protected value via [`Deref`].
/// When dropped, releases the read lock and flushes pending callbacks just like
/// [`RwLockBellReadGuard`].
#[derive(Debug)]
pub struct MappedRwLockBellReadGuard<'a, T> {
    guard: Option<MappedRwLockReadGuard<'a, T>>,
    state: &'a LockState,
}

impl<'a, T> Deref for MappedRwLockBellReadGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        Deref::deref(self.guard.as_ref().unwrap())
    }
}

impl<'a, T> Drop for MappedRwLockBellReadGuard<'a, T> {
    fn drop(&mut self) {
        drop_read_guard(&mut self.guard, self.state)
    }
}

/// RAII write guard for [`RwLockBell`].
///
/// Provides exclusive write access to the protected value via [`Deref`] and [`DerefMut`].
/// When dropped, releases the lock and calls all callbacks that were registered via
/// [`try_write_or`] while this guard was held.
///
/// Obtained via [`RwLockBell::write`], [`RwLockBell::try_write`], or
/// [`RwLockBell::try_write_or`].
///
/// [`try_write_or`]: RwLockBell::try_write_or
#[derive(Debug)]
pub struct RwLockBellWriteGuard<'a, T> {
    guard: Option<RwLockWriteGuard<'a, T>>,
    state: &'a LockState,
}

impl<'a, T> RwLockBellWriteGuard<'a, T> {
    /// Transforms this guard into a [`MappedRwLockBellWriteGuard`] that dereferences
    /// to a subfield of the protected value.
    pub fn map<U, F>(mut self, f: F) -> MappedRwLockBellWriteGuard<'a, U>
    where
        F: FnOnce(&mut T) -> &mut U,
    {
        let guard = self.guard.take().unwrap();
        let state = self.state;
        forget(self);
        let map_guard = RwLockWriteGuard::map(guard, f);
        MappedRwLockBellWriteGuard {
            guard: Some(map_guard),
            state,
        }
    }

    /// Attempts to transform this guard into a [`MappedRwLockBellWriteGuard`].
    ///
    /// Returns `Err(self)` if `f` returns `None`, giving the original guard back.
    pub fn try_map<U, F>(mut self, f: F) -> Result<MappedRwLockBellWriteGuard<'a, U>, Self>
    where
        F: FnOnce(&mut T) -> Option<&mut U>,
    {
        let guard = self.guard.take().unwrap();
        let state = self.state;
        forget(self);
        match RwLockWriteGuard::try_map(guard, f) {
            Ok(map_guard) => Ok(MappedRwLockBellWriteGuard {
                guard: Some(map_guard),
                state,
            }),
            Err(guard) => Err(Self {
                guard: Some(guard),
                state,
            }),
        }
    }

    /// Attempts to transform this guard into a [`MappedRwLockBellWriteGuard`].
    ///
    /// Returns `Err((self, error))` if `f` returns `Err`, giving the original guard
    /// and the error back.
    pub fn try_map_err<U, F, E>(
        mut self,
        f: F,
    ) -> Result<MappedRwLockBellWriteGuard<'a, U>, (Self, E)>
    where
        F: FnOnce(&mut T) -> Result<&mut U, E>,
    {
        let guard = self.guard.take().unwrap();
        let state = self.state;
        forget(self);
        match RwLockWriteGuard::try_map_or_err(guard, f) {
            Ok(map_guard) => Ok(MappedRwLockBellWriteGuard {
                guard: Some(map_guard),
                state,
            }),
            Err((guard, err)) => Err((
                Self {
                    guard: Some(guard),
                    state,
                },
                err,
            )),
        }
    }
}

impl<'a, T> Deref for RwLockBellWriteGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        Deref::deref(self.guard.as_ref().unwrap())
    }
}

impl<'a, T> DerefMut for RwLockBellWriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        DerefMut::deref_mut(self.guard.as_mut().unwrap())
    }
}

impl<'a, T> Drop for RwLockBellWriteGuard<'a, T> {
    fn drop(&mut self) {
        drop_write_guard(&mut self.guard, self.state)
    }
}

/// RAII write guard produced by [`RwLockBellWriteGuard::map`] and related methods.
///
/// Provides exclusive write access to a subfield of the protected value via [`Deref`]
/// and [`DerefMut`]. When dropped, releases the lock and flushes pending callbacks
/// just like [`RwLockBellWriteGuard`].
#[derive(Debug)]
pub struct MappedRwLockBellWriteGuard<'a, T> {
    guard: Option<MappedRwLockWriteGuard<'a, T>>,
    state: &'a LockState,
}

impl<'a, T> Deref for MappedRwLockBellWriteGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        Deref::deref(self.guard.as_ref().unwrap())
    }
}

impl<'a, T> DerefMut for MappedRwLockBellWriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        DerefMut::deref_mut(self.guard.as_mut().unwrap())
    }
}

impl<'a, T> Drop for MappedRwLockBellWriteGuard<'a, T> {
    fn drop(&mut self) {
        drop_write_guard(&mut self.guard, self.state)
    }
}

fn drop_read_guard<G>(guard: &mut Option<G>, state: &LockState) {
    drop(guard.take());

    #[cfg(test)]
    hooks::run(hooks::HookPoint::ReadGuardAfterRelease);

    let callbacks = {
        let mut inner = state.inner.lock();
        inner.readers -= 1;
        // Only the last reader drains; also skip if a concurrent drain is
        // already running (`dropping = true`) — it would create a deadlock
        // because both sides would sleep on `locking_zero` but
        // `notify_one` only wakes a single waiter.
        if inner.readers > 0 || inner.callbacks.is_empty() || inner.dropping {
            return;
        }
        inner.dropping = true;

        #[cfg(test)]
        hooks::run(hooks::HookPoint::ReadGuardAfterSettingDropping);

        while inner.locking != 0 {
            state.locking_zero.wait(&mut inner);
        }
        take(&mut inner.callbacks)
    };

    // The read lock is already released; pass `()` so drain_and_run has
    // nothing to drop before resetting `dropping` and running callbacks.
    drain_and_run(state, callbacks);
}

fn drop_write_guard<G>(guard: &mut Option<G>, state: &LockState) {
    #[cfg(test)]
    hooks::run(hooks::HookPoint::WriteGuardBeforeDrop);

    let callbacks = {
        let mut inner = state.inner.lock();
        inner.dropping = true;

        #[cfg(test)]
        hooks::run(hooks::HookPoint::WriteGuardAfterSettingDropping);

        // Sleep until all in-flight try_write_or calls have either
        // registered their callback or obtained the lock.
        while inner.locking != 0 {
            state.locking_zero.wait(&mut inner);
        }
        take(&mut inner.callbacks)
        // Mutex released here — callbacks execute without holding any lock.
    };

    drop(guard.take().unwrap());

    drain_and_run(state, callbacks);
}

/// Resets `dropping` to `false`, notifies waiters, then runs all collected
/// callbacks, re-raising the first panic.
///
/// Callers must have already set `dropping = true` and drained the callback queue.
fn drain_and_run(state: &LockState, callbacks: Vec<Box<dyn FnOnce() + Send>>) {
    #[cfg(test)]
    hooks::run(hooks::HookPoint::DrainAfterWriteLockRelease);

    {
        let mut inner = state.inner.lock();
        inner.dropping = false;
        state.not_dropping.notify_all();
    }

    #[cfg(test)]
    hooks::run(hooks::HookPoint::DrainBeforeCallbacks);

    // Run every callback regardless of panics, then re-raise the first
    // panic (if any) after all callbacks have had a chance to execute.
    // `catch_unwind` is called unconditionally before `or` so that every
    // callback runs even if an earlier one panicked (`or_else` would
    // short-circuit and skip the remaining callbacks).
    let first_panic = callbacks.into_iter().fold(None, |first, callback| {
        let result = panic::catch_unwind(AssertUnwindSafe(callback)).err();
        first.or(result)
    });

    if let Some(payload) = first_panic {
        // If we are already unwinding from an outer panic (the guard was
        // dropped as part of stack-unwinding), re-raising would trigger a
        // double-panic and abort the process. Suppress the callback panic
        // in that case — the outer panic carries the primary failure.
        if std::thread::panicking() {
            drop(payload);
        } else {
            panic::resume_unwind(payload);
        }
    }
}
