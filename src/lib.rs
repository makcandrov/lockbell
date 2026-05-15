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

#[derive(Default)]
struct LockStateInner {
    /// A drain is in progress (between setting the flag and flushing callbacks).
    dropping: bool,
    /// In-flight `try_write_or_else` calls between bumping the counter and resolving.
    locking: u64,
    /// Live `RwLockBellReadGuard` count.
    readers: u64,
    /// Callbacks queued by failed `try_write_or` calls, FIFO order.
    callbacks: Vec<Box<dyn FnOnce() + Send>>,
}

impl LockStateInner {
    pub fn decrement_locking(&mut self, locking_zero: &Condvar) {
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
            .field("dropping", &self.dropping)
            .field("locking", &self.locking)
            .field("readers", &self.readers)
            .field("callbacks", &"{callbacks}")
            .finish()
    }
}

#[derive(Default)]
struct LockState {
    inner: Mutex<LockStateInner>,
    /// Signalled when `locking` reaches 0; drainers wait on this.
    locking_zero: Condvar,
    /// Signalled when `dropping` flips back to `false`; `try_write_or_else` waits on this.
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

impl LockState {
    fn decrement_locking(&self) {
        self.inner.lock().decrement_locking(&self.locking_zero);
    }
}

/// An [`RwLock`] that fires queued callbacks when contention clears.
///
/// When [`try_write_or`] cannot acquire the lock, the supplied callback is queued.
/// All queued callbacks fire in FIFO order, without holding any lock, when the
/// next write guard (or last reader) is dropped.
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
    /// Pending callbacks are dropped without being called.
    #[inline]
    #[must_use]
    pub fn into_inner(self) -> T {
        self.lock.into_inner()
    }

    /// Locks for shared read access, blocking until acquired.
    ///
    /// Dropping the guard flushes pending [`try_write_or`] callbacks if this was
    /// the last active reader.
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

    /// Locks for exclusive write access, blocking until acquired.
    ///
    /// Callbacks registered via [`try_write_or`] while this guard is held fire
    /// when the guard is dropped.
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
    /// Returns `None` if the lock is held. No callback is registered on failure;
    /// use [`try_write_or`] for that.
    ///
    /// [`try_write_or`]: RwLockBell::try_write_or
    #[must_use]
    pub fn try_write(&self) -> Option<RwLockBellWriteGuard<'_, T>> {
        self.lock.try_write().map(|guard| RwLockBellWriteGuard {
            guard: Some(guard),
            state: &self.state,
        })
    }

    /// Attempts to acquire exclusive write access; on failure, queues `callback`.
    ///
    /// - **Success** — returns `Some(guard)`; `callback` is discarded.
    /// - **Failure** — returns `None`; `callback` is queued and runs, without
    ///   holding any lock, after the next write guard (or last reader) is dropped.
    ///
    /// Callbacks fire in FIFO registration order.
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

    /// Like [`try_write_or`], but builds the callback lazily.
    ///
    /// On contention, `callback()` is called to produce the queued callback.
    /// Prefer this when constructing the callback is expensive or has side effects
    /// that should only run on failure.
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

        if let Some(guard) = self.lock.try_write() {
            self.state.decrement_locking();
            Some(RwLockBellWriteGuard {
                guard: Some(guard),
                state: &self.state,
            })
        } else {
            // Decrement `locking` even if the factory panics — otherwise
            // drainers would wait on `locking_zero` forever.
            let cb = catch_panic(callback, || self.state.decrement_locking());

            let cb: Box<dyn FnOnce() + Send> = Box::new(cb);

            let mut inner = self.state.inner.lock();
            inner.callbacks.push(cb);
            inner.decrement_locking(&self.state.locking_zero);
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
/// Provides shared read access via [`Deref`]. Dropping releases the read lock
/// and, if this was the last active reader, flushes pending [`try_write_or`]
/// callbacks.
///
/// Returned by [`RwLockBell::read`] and [`RwLockBell::try_read`].
///
/// [`try_write_or`]: RwLockBell::try_write_or
#[derive(Debug)]
pub struct RwLockBellReadGuard<'a, T> {
    guard: Option<RwLockReadGuard<'a, T>>,
    state: &'a LockState,
}

impl<'a, T> RwLockBellReadGuard<'a, T> {
    /// Maps this guard to a subfield of the protected value.
    pub fn map<U, F>(mut self, f: F) -> MappedRwLockBellReadGuard<'a, U>
    where
        F: FnOnce(&T) -> &U,
    {
        let guard = self.guard.take().unwrap();
        let state = self.state;
        let map_guard = RwLockReadGuard::map(guard, f);
        forget(self);
        MappedRwLockBellReadGuard {
            guard: Some(map_guard),
            state,
        }
    }

    /// Maps this guard to a subfield, returning `Err(self)` if `f` returns `None`.
    pub fn try_map<U, F>(mut self, f: F) -> Result<MappedRwLockBellReadGuard<'a, U>, Self>
    where
        F: FnOnce(&T) -> Option<&U>,
    {
        let guard = self.guard.take().unwrap();
        let state = self.state;
        let map_res = RwLockReadGuard::try_map(guard, f);
        forget(self);
        match map_res {
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

    /// Maps this guard to a subfield, returning `Err((self, e))` if `f` returns `Err(e)`.
    pub fn try_map_or_err<U, F, E>(
        mut self,
        f: F,
    ) -> Result<MappedRwLockBellReadGuard<'a, U>, (Self, E)>
    where
        F: FnOnce(&T) -> Result<&U, E>,
    {
        let guard = self.guard.take().unwrap();
        let state = self.state;
        let map_res = RwLockReadGuard::try_map_or_err(guard, f);
        forget(self);
        match map_res {
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

/// RAII read guard produced by [`RwLockBellReadGuard::map`] and friends.
///
/// Behaves like [`RwLockBellReadGuard`] but dereferences to a subfield.
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
/// Provides exclusive write access via [`Deref`] and [`DerefMut`]. Dropping
/// releases the lock and fires every callback registered via [`try_write_or`]
/// while this guard was held.
///
/// Returned by [`RwLockBell::write`], [`RwLockBell::try_write`], and
/// [`RwLockBell::try_write_or`].
///
/// [`try_write_or`]: RwLockBell::try_write_or
#[derive(Debug)]
pub struct RwLockBellWriteGuard<'a, T> {
    guard: Option<RwLockWriteGuard<'a, T>>,
    state: &'a LockState,
}

impl<'a, T> RwLockBellWriteGuard<'a, T> {
    /// Maps this guard to a subfield of the protected value.
    pub fn map<U, F>(mut self, f: F) -> MappedRwLockBellWriteGuard<'a, U>
    where
        F: FnOnce(&mut T) -> &mut U,
    {
        let guard = self.guard.take().unwrap();
        let state = self.state;
        let map_guard = RwLockWriteGuard::map(guard, f);
        forget(self);
        MappedRwLockBellWriteGuard {
            guard: Some(map_guard),
            state,
        }
    }

    /// Maps this guard to a subfield, returning `Err(self)` if `f` returns `None`.
    pub fn try_map<U, F>(mut self, f: F) -> Result<MappedRwLockBellWriteGuard<'a, U>, Self>
    where
        F: FnOnce(&mut T) -> Option<&mut U>,
    {
        let guard = self.guard.take().unwrap();
        let state = self.state;
        let map_res = RwLockWriteGuard::try_map(guard, f);
        forget(self);
        match map_res {
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

    /// Maps this guard to a subfield, returning `Err((self, e))` if `f` returns `Err(e)`.
    pub fn try_map_err<U, F, E>(
        mut self,
        f: F,
    ) -> Result<MappedRwLockBellWriteGuard<'a, U>, (Self, E)>
    where
        F: FnOnce(&mut T) -> Result<&mut U, E>,
    {
        let guard = self.guard.take().unwrap();
        let state = self.state;
        let map_res = RwLockWriteGuard::try_map_or_err(guard, f);
        forget(self);
        match map_res {
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

/// RAII write guard produced by [`RwLockBellWriteGuard::map`] and friends.
///
/// Behaves like [`RwLockBellWriteGuard`] but dereferences to a subfield.
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
    // `None` if a `map`-family method took the guard out and then panicked;
    // in that case parking_lot already released the lock during the unwind.
    drop(guard.take());

    #[cfg(test)]
    hooks::run(hooks::HookPoint::ReadGuardAfterRelease);

    let callbacks = {
        let mut inner = state.inner.lock();
        inner.readers -= 1;
        // Only the last reader drains; skip if another drain is already in
        // flight (the queue would be empty or stolen again — both pointless).
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

        // Wait until every in-flight `try_write_or` has either pushed its
        // callback or obtained the lock.
        while inner.locking != 0 {
            state.locking_zero.wait(&mut inner);
        }
        take(&mut inner.callbacks)
        // Mutex released here.
    };

    // `None` if a `map`-family method took the guard out and then panicked;
    // in that case parking_lot already released the lock during the unwind.
    drop(guard.take());

    drain_and_run(state, callbacks);
}

/// Clears `dropping`, wakes [`try_write_or_else`] waiters, then runs every
/// callback. Re-raises the first callback panic (if any) once the queue is
/// fully drained — but only if we aren't already unwinding.
///
/// Caller must have set `dropping = true` and taken the callback queue.
///
/// [`try_write_or_else`]: RwLockBell::try_write_or_else
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
fn catch_panic<T>(f: impl FnOnce() -> T, on_panic: impl FnOnce()) -> T {
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
