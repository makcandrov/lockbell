//! The lock itself.

use std::fmt;
#[cfg(feature = "arc")]
use std::{mem::ManuallyDrop, sync::Arc};

use parking_lot::lock_api;

#[cfg(feature = "arc")]
use crate::{ArcRwLockBellReadGuard, ArcRwLockBellWriteGuard};
use crate::{RwLockBellReadGuard, RwLockBellWriteGuard, raw::RawRwLockBell};

/// Wrapped rather than aliased, so `RwLockBell` exposes only the operations
/// below — not every `lock_api` one, including those that ring the bell
/// unexpectedly. Spelling this type out is the opt-in for those.
type RawLock<T> = lock_api::RwLock<RawRwLockBell, T>;

/// An [`RwLock`](parking_lot::RwLock) that fires queued callbacks when
/// contention clears.
///
/// When [`try_write_or`] cannot acquire the lock, the supplied callback is
/// queued. All queued callbacks fire in FIFO order, without the firing thread
/// holding any lock, when the next write guard (or last reader) is dropped.
///
/// Both release paths snapshot the queue while still holding the lock, so a
/// batch only contains callbacks that lost their race to the guard now
/// releasing; none is rung while the holder that turned it away still holds. A
/// *different* thread may have taken the lock by then.
///
/// Dropping the lock, or calling [`into_inner`], discards whatever is still
/// queued without calling it.
///
/// [`try_write_or`]: Self::try_write_or
/// [`into_inner`]: Self::into_inner
#[repr(transparent)]
pub struct RwLockBell<T: ?Sized>(RawLock<T>);

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
    /// for the full blocking, panic and deadlock rules, which apply here too.
    ///
    /// [`try_write_or_else`]: Self::try_write_or_else
    #[inline]
    pub fn try_write_or<Callback>(&self, callback: Callback) -> Option<RwLockBellWriteGuard<'_, T>>
    where
        Callback: FnOnce() + Send + 'static,
    {
        if self.raw().try_lock_exclusive_or(callback) {
            // SAFETY: just acquired above, handed to exactly one guard.
            Some(RwLockBellWriteGuard(unsafe {
                self.0.make_write_guard_unchecked()
            }))
        } else {
            None
        }
    }

    /// Like [`try_write_or`], but builds the callback lazily: `factory` runs
    /// only on contention.
    ///
    /// # Deadlock
    ///
    /// `factory` runs inside the window a concurrent unlock waits on, and that
    /// unlock still holds the lock — shared or exclusive — while it waits. So
    /// `factory` **must not touch this lock and must not block**:
    ///
    /// ```no_run
    /// # use lockbell::RwLockBell;
    /// # let lock = RwLockBell::new(0u64);
    /// // Deadlocks: the guard we lost to cannot release until `factory` returns.
    /// let _ = lock.try_write_or_else(|| {
    ///     let value = *lock.write();
    ///     move || println!("{value}")
    /// });
    /// ```
    ///
    /// The queued callback has no such restriction: it runs after the release
    /// and may freely re-acquire the lock.
    ///
    /// # Blocking
    ///
    /// Unlike [`try_write`], this is not a wait-free probe: it waits out a
    /// concurrent flush before deciding. Without that wait the callback could
    /// land in an already-taken batch and never ring.
    ///
    /// The wait covers the bell bookkeeping and any concurrent `factory` —
    /// never the protected data, never a queued callback.
    ///
    /// [`try_write`]: Self::try_write
    ///
    /// # Panics
    ///
    /// A panicking `factory` propagates to the caller; no callback is queued and
    /// the bookkeeping stays consistent.
    ///
    /// A panicking *callback* does not stop the rest of the batch. The first
    /// panic is re-raised from the draining thread afterwards, unless that
    /// thread is already unwinding, in which case it is dropped to avoid an
    /// abort.
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
            // SAFETY: just acquired above, handed to exactly one guard.
            Some(RwLockBellWriteGuard(unsafe {
                self.0.make_write_guard_unchecked()
            }))
        } else {
            None
        }
    }
}

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
            // SAFETY: just acquired above, handed to exactly one guard.
            Some(ArcRwLockBellWriteGuard(unsafe {
                self.as_raw_arc().make_arc_write_guard_unchecked()
            }))
        } else {
            None
        }
    }
}

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
/// Never rings the bell: the shared lock it takes bypasses the callback
/// machinery, so `{:?}` cannot run a callback, block or panic. In exchange, a
/// `try_write_or` losing to this transient lock waits for the next real
/// release rather than being drained here.
impl<T: ?Sized + fmt::Debug> fmt::Debug for RwLockBell<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut out = f.debug_struct("RwLockBell");
        match self.raw().peek() {
            Some(_peek) => {
                // SAFETY: `_peek` holds a shared lock for the whole borrow.
                let data = unsafe { &*self.0.data_ptr() };
                out.field("data", &data)
            }
            // `format_args!` avoids quoting `<locked>`.
            None => out.field("data", &format_args!("<locked>")),
        }
        .finish()
    }
}

impl<T: ?Sized> RwLockBell<T> {
    /// The raw lock, for driving the bell protocol.
    #[inline]
    fn raw(&self) -> &RawRwLockBell {
        // SAFETY: only ever used to acquire, never to unlock, so no live
        // guard's invariant is disturbed.
        unsafe { self.0.raw() }
    }

    /// Borrows `Arc<Self>` as `Arc<RawLock<T>>`, which is all `lock_api`'s
    /// `Arc`-guard constructors accept. They clone it themselves, so exactly
    /// one strong count is added per guard.
    #[cfg(feature = "arc")]
    #[inline]
    fn as_raw_arc(self: &Arc<Self>) -> ManuallyDrop<Arc<RawLock<T>>> {
        let ptr = Arc::as_ptr(self) as *const RawLock<T>;
        // SAFETY: `#[repr(transparent)]`, so the pointer is valid for
        // `RawLock<T>` with the same metadata. `ManuallyDrop` keeps the strong
        // count borrowed from `self` from ever being released.
        ManuallyDrop::new(unsafe { Arc::from_raw(ptr) })
    }
}
