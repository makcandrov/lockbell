//! The lock itself.

use std::fmt;
#[cfg(feature = "arc")]
use std::{mem::ManuallyDrop, sync::Arc};

use parking_lot::lock_api;

#[cfg(feature = "arc")]
use crate::{ArcRwLockBellReadGuard, ArcRwLockBellWriteGuard};
use crate::{RwLockBellReadGuard, RwLockBellWriteGuard, raw::RawRwLockBell};

/// The `lock_api` lock we wrap. Kept private: exposing it would put every
/// `lock_api` operation — including the ones that ring the bell where you
/// would not expect it — back into the public API.
type RawLock<T> = lock_api::RwLock<RawRwLockBell, T>;

/// An [`RwLock`](parking_lot::RwLock) that fires queued callbacks when
/// contention clears.
///
/// When [`try_write_or`] cannot acquire the lock, the supplied callback is
/// queued. All queued callbacks fire in FIFO order, without the firing thread
/// holding any lock, when the next write guard (or last reader) is dropped.
///
/// [`try_write_or`]: Self::try_write_or
#[repr(transparent)]
pub struct RwLockBell<T: ?Sized>(RawLock<T>);

// ─── construction ────────────────────────────────────────────────────────────

impl<T> RwLockBell<T> {
    /// Creates a new `RwLockBell` wrapping `value`.
    ///
    /// This is a `const fn`, so a lock can live in a `static`:
    ///
    /// ```
    /// use lockbell::RwLockBell;
    ///
    /// static COUNTER: RwLockBell<u64> = RwLockBell::new(0);
    /// assert_eq!(*COUNTER.read(), 0);
    /// ```
    #[inline]
    #[must_use]
    pub const fn new(value: T) -> Self {
        Self(RawLock::new(value))
    }

    /// Consumes the lock and returns the protected value.
    ///
    /// Any callback still queued is dropped without being called.
    #[inline]
    #[must_use]
    pub fn into_inner(self) -> T {
        self.0.into_inner()
    }
}

impl<T: ?Sized> RwLockBell<T> {
    /// Returns a mutable reference to the protected value.
    ///
    /// No locking is needed: the `&mut self` borrow already proves exclusivity.
    #[inline]
    #[must_use]
    pub fn get_mut(&mut self) -> &mut T {
        self.0.get_mut()
    }
}

impl<T: Default> Default for RwLockBell<T> {
    #[inline]
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> From<T> for RwLockBell<T> {
    #[inline]
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

// ─── shared access ───────────────────────────────────────────────────────────

impl<T: ?Sized> RwLockBell<T> {
    /// Locks for shared access, blocking until acquired.
    ///
    /// Dropping the guard flushes pending [`try_write_or`] callbacks if this
    /// was the last active reader.
    ///
    /// [`try_write_or`]: Self::try_write_or
    #[inline]
    pub fn read(&self) -> RwLockBellReadGuard<'_, T> {
        RwLockBellReadGuard(self.0.read())
    }

    /// Attempts to acquire shared access without blocking.
    ///
    /// Returns `None` if the exclusive lock is currently held.
    #[inline]
    pub fn try_read(&self) -> Option<RwLockBellReadGuard<'_, T>> {
        self.0.try_read().map(RwLockBellReadGuard)
    }
}

// ─── exclusive access ────────────────────────────────────────────────────────

impl<T: ?Sized> RwLockBell<T> {
    /// Locks for exclusive access, blocking until acquired.
    ///
    /// Callbacks registered via [`try_write_or`] while this guard is held fire
    /// when it is dropped.
    ///
    /// [`try_write_or`]: Self::try_write_or
    #[inline]
    pub fn write(&self) -> RwLockBellWriteGuard<'_, T> {
        RwLockBellWriteGuard(self.0.write())
    }

    /// Attempts to acquire exclusive access without blocking.
    ///
    /// Returns `None` if the lock is held. No callback is registered on
    /// failure; use [`try_write_or`] for that.
    ///
    /// [`try_write_or`]: Self::try_write_or
    #[inline]
    pub fn try_write(&self) -> Option<RwLockBellWriteGuard<'_, T>> {
        self.0.try_write().map(RwLockBellWriteGuard)
    }

    /// Attempts to acquire exclusive access; on failure, queues `callback`.
    ///
    /// - **Success** — returns `Some(guard)`; `callback` is discarded.
    /// - **Failure** — returns `None`; `callback` is queued and runs, with the
    ///   firing thread holding no lock, after the next write guard (or last
    ///   reader) is dropped.
    ///
    /// Callbacks fire in FIFO registration order. A panicking callback does not
    /// prevent the rest of the queue from running; see [`try_write_or_else`]
    /// for the full panic and deadlock rules, which apply here too.
    ///
    /// [`try_write_or_else`]: Self::try_write_or_else
    #[inline]
    pub fn try_write_or<Callback>(&self, callback: Callback) -> Option<RwLockBellWriteGuard<'_, T>>
    where
        Callback: FnOnce() + Send + 'static,
    {
        self.try_write_or_else(|| callback)
    }

    /// Like [`try_write_or`], but builds the callback lazily.
    ///
    /// On contention, `factory()` is called to produce the queued callback.
    /// Prefer this when constructing the callback is expensive or has side
    /// effects that should only run on failure.
    ///
    /// # Deadlock
    ///
    /// `factory` runs inside the window that a concurrent unlock waits on, and
    /// that unlock still holds the exclusive lock while it waits. So `factory`
    /// **must not touch this lock and must not block**:
    ///
    /// ```no_run
    /// # use lockbell::RwLockBell;
    /// # let lock = RwLockBell::new(0u64);
    /// // Deadlocks: the writer we lost to cannot release until `factory` returns.
    /// let _ = lock.try_write_or_else(|| {
    ///     let value = *lock.read();
    ///     move || println!("{value}")
    /// });
    /// ```
    ///
    /// The queued callback itself has no such restriction — it runs after the
    /// lock has been released, and may freely re-acquire it.
    ///
    /// # Panics
    ///
    /// If `factory` panics, the panic propagates to the caller and no callback
    /// is queued; the lock's bookkeeping is left consistent.
    ///
    /// If a queued *callback* panics, the remaining callbacks still run and the
    /// first panic is re-raised afterwards from whichever thread was draining —
    /// unless that thread is already unwinding, in which case the payload is
    /// dropped to avoid an abort.
    ///
    /// [`try_write_or`]: Self::try_write_or
    pub fn try_write_or_else<Callback, Factory>(
        &self,
        factory: Factory,
    ) -> Option<RwLockBellWriteGuard<'_, T>>
    where
        Factory: FnOnce() -> Callback,
        Callback: FnOnce() + Send + 'static,
    {
        if self.raw().try_lock_exclusive_or_else(factory) {
            // SAFETY: the exclusive lock was just acquired above, and is
            // handed to exactly one guard.
            Some(RwLockBellWriteGuard(unsafe {
                self.0.make_write_guard_unchecked()
            }))
        } else {
            None
        }
    }
}

// ─── access through an `Arc` ─────────────────────────────────────────────────

#[cfg(feature = "arc")]
#[cfg_attr(docsrs, doc(cfg(feature = "arc")))]
impl<T: ?Sized> RwLockBell<T> {
    /// Locks for shared access through an [`Arc`], blocking until acquired.
    ///
    /// The returned guard carries no lifetime; see
    /// [`ArcRwLockBellReadGuard`].
    ///
    /// [`Arc`]: std::sync::Arc
    #[inline]
    pub fn read_arc(self: &Arc<Self>) -> ArcRwLockBellReadGuard<T> {
        ArcRwLockBellReadGuard(self.as_raw_arc().read_arc())
    }

    /// Attempts to acquire shared access through an [`Arc`] without blocking.
    ///
    /// [`Arc`]: std::sync::Arc
    #[inline]
    pub fn try_read_arc(self: &Arc<Self>) -> Option<ArcRwLockBellReadGuard<T>> {
        self.as_raw_arc().try_read_arc().map(ArcRwLockBellReadGuard)
    }

    /// Locks for exclusive access through an [`Arc`], blocking until acquired.
    ///
    /// [`Arc`]: std::sync::Arc
    #[inline]
    pub fn write_arc(self: &Arc<Self>) -> ArcRwLockBellWriteGuard<T> {
        ArcRwLockBellWriteGuard(self.as_raw_arc().write_arc())
    }

    /// Attempts to acquire exclusive access through an [`Arc`] without blocking.
    ///
    /// No callback is registered on failure; use [`try_write_arc_or`] for that.
    ///
    /// [`Arc`]: std::sync::Arc
    /// [`try_write_arc_or`]: Self::try_write_arc_or
    #[inline]
    pub fn try_write_arc(self: &Arc<Self>) -> Option<ArcRwLockBellWriteGuard<T>> {
        self.as_raw_arc()
            .try_write_arc()
            .map(ArcRwLockBellWriteGuard)
    }

    /// [`try_write_or`], returning a guard with no lifetime attached.
    ///
    /// [`try_write_or`]: Self::try_write_or
    #[inline]
    pub fn try_write_arc_or<Callback>(
        self: &Arc<Self>,
        callback: Callback,
    ) -> Option<ArcRwLockBellWriteGuard<T>>
    where
        Callback: FnOnce() + Send + 'static,
    {
        self.try_write_arc_or_else(|| callback)
    }

    /// [`try_write_or_else`], returning a guard with no lifetime attached.
    ///
    /// The same deadlock and panic rules apply to `factory`.
    ///
    /// [`try_write_or_else`]: Self::try_write_or_else
    pub fn try_write_arc_or_else<Callback, Factory>(
        self: &Arc<Self>,
        factory: Factory,
    ) -> Option<ArcRwLockBellWriteGuard<T>>
    where
        Factory: FnOnce() -> Callback,
        Callback: FnOnce() + Send + 'static,
    {
        if self.raw().try_lock_exclusive_or_else(factory) {
            // SAFETY: the exclusive lock was just acquired above, and is
            // handed to exactly one guard.
            Some(ArcRwLockBellWriteGuard(unsafe {
                self.as_raw_arc().make_arc_write_guard_unchecked()
            }))
        } else {
            None
        }
    }
}

// ─── inspection ──────────────────────────────────────────────────────────────

impl<T: ?Sized> RwLockBell<T> {
    /// Whether the lock is currently held in any mode.
    ///
    /// Does not ring the bell.
    #[inline]
    #[must_use]
    pub fn is_locked(&self) -> bool {
        self.0.is_locked()
    }

    /// Whether the lock is currently held exclusively.
    ///
    /// Does not ring the bell.
    #[inline]
    #[must_use]
    pub fn is_locked_exclusive(&self) -> bool {
        self.0.is_locked_exclusive()
    }
}

/// Formats the protected value if it can be read, `<locked>` otherwise.
///
/// Formatting never rings the bell: it takes a shared lock that bypasses the
/// callback machinery entirely, so `{:?}` cannot run a callback, block on a
/// concurrent [`try_write_or_else`] factory, or panic.
///
/// The trade-off is that a `try_write_or` losing a race to this transient lock
/// has its callback queued until the next real guard release, rather than
/// being drained here.
///
/// [`try_write_or_else`]: RwLockBell::try_write_or_else
impl<T: ?Sized + fmt::Debug> fmt::Debug for RwLockBell<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut out = f.debug_struct("RwLockBell");
        match self.raw().peek() {
            Some(_peek) => {
                // SAFETY: `_peek` holds a shared lock for as long as it is
                // alive, so the value cannot be mutated while we format it.
                let data = unsafe { &*self.0.data_ptr() };
                out.field("data", &data)
            }
            // `format_args!` avoids quoting `<locked>` in the output.
            None => out.field("data", &format_args!("<locked>")),
        }
        .finish()
    }
}

// ─── internals ───────────────────────────────────────────────────────────────

impl<T: ?Sized> RwLockBell<T> {
    /// The raw lock, for driving the bell protocol.
    #[inline]
    fn raw(&self) -> &RawRwLockBell {
        // SAFETY: used only to run the bell protocol and to acquire the
        // exclusive lock. Nothing here unlocks it, so no live guard's
        // invariant is disturbed.
        unsafe { self.0.raw() }
    }

    /// Borrows `Arc<Self>` as `Arc<RawLock<T>>` so that `lock_api`'s
    /// `Arc`-guard constructors — which only accept their own type — can be
    /// used to build our `Arc` guards.
    ///
    /// The result is a *borrow*: it must not be dropped as an owned `Arc`,
    /// hence the [`ManuallyDrop`]. The guard constructors take it by reference
    /// and clone it themselves, so exactly one strong count is added per guard.
    #[cfg(feature = "arc")]
    #[inline]
    fn as_raw_arc(self: &Arc<Self>) -> ManuallyDrop<Arc<RawLock<T>>> {
        let ptr = Arc::as_ptr(self) as *const RawLock<T>;
        // SAFETY: `RwLockBell` is `#[repr(transparent)]` over `RawLock<T>`, so
        // the pointer is valid for that type with the same metadata. Wrapping
        // in `ManuallyDrop` means the strong count borrowed from `self` is
        // never released, so `self` stays the sole owner of it.
        ManuallyDrop::new(unsafe { Arc::from_raw(ptr) })
    }
}
