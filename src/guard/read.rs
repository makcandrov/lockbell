//! Shared guards.

use parking_lot::lock_api;

use crate::raw::RawRwLockBell;

type RawGuard<'a, T> = lock_api::RwLockReadGuard<'a, RawRwLockBell, T>;
type RawMapped<'a, T> = lock_api::MappedRwLockReadGuard<'a, RawRwLockBell, T>;
#[cfg(feature = "arc")]
type RawArc<T> = lock_api::ArcRwLockReadGuard<RawRwLockBell, T>;

/// RAII shared guard for [`RwLockBell`](crate::RwLockBell).
///
/// Dropping it releases the shared lock and, if this was the last active
/// reader, flushes pending [`try_write_or`] callbacks.
///
/// Returned by [`RwLockBell::read`] and [`RwLockBell::try_read`].
///
/// [`try_write_or`]: crate::RwLockBell::try_write_or
/// [`RwLockBell::read`]: crate::RwLockBell::read
/// [`RwLockBell::try_read`]: crate::RwLockBell::try_read
#[must_use = "if unused the lock is immediately released"]
pub struct RwLockBellReadGuard<'a, T: ?Sized>(pub(crate) RawGuard<'a, T>);

impl<'a, T: ?Sized> RwLockBellReadGuard<'a, T> {
    /// Maps this guard to a component of the protected value.
    ///
    /// An associated function so it cannot shadow a method on `T`.
    pub fn map<U: ?Sized, F>(s: Self, f: F) -> MappedRwLockBellReadGuard<'a, U>
    where
        F: FnOnce(&T) -> &U,
    {
        MappedRwLockBellReadGuard(lock_api::RwLockReadGuard::map(s.0, f))
    }

    /// Maps this guard to a component, returning `Err(s)` if `f` returns `None`.
    pub fn try_map<U: ?Sized, F>(s: Self, f: F) -> Result<MappedRwLockBellReadGuard<'a, U>, Self>
    where
        F: FnOnce(&T) -> Option<&U>,
    {
        match lock_api::RwLockReadGuard::try_map(s.0, f) {
            Ok(mapped) => Ok(MappedRwLockBellReadGuard(mapped)),
            Err(guard) => Err(Self(guard)),
        }
    }

    /// Maps this guard to a component, returning `Err((s, e))` if `f` returns `Err(e)`.
    pub fn try_map_or_err<U: ?Sized, F, E>(
        s: Self,
        f: F,
    ) -> Result<MappedRwLockBellReadGuard<'a, U>, (Self, E)>
    where
        F: FnOnce(&T) -> Result<&U, E>,
    {
        match lock_api::RwLockReadGuard::try_map_or_err(s.0, f) {
            Ok(mapped) => Ok(MappedRwLockBellReadGuard(mapped)),
            Err((guard, err)) => Err((Self(guard), err)),
        }
    }
}

forward_shared_traits!(RwLockBellReadGuard<'a>);

/// RAII shared guard produced by [`RwLockBellReadGuard::map`] and friends.
///
/// Behaves like [`RwLockBellReadGuard`] but dereferences to a component.
#[must_use = "if unused the lock is immediately released"]
pub struct MappedRwLockBellReadGuard<'a, T: ?Sized>(pub(crate) RawMapped<'a, T>);

impl<'a, T: ?Sized> MappedRwLockBellReadGuard<'a, T> {
    /// Maps this guard to a component of the protected value.
    pub fn map<U: ?Sized, F>(s: Self, f: F) -> MappedRwLockBellReadGuard<'a, U>
    where
        F: FnOnce(&T) -> &U,
    {
        MappedRwLockBellReadGuard(lock_api::MappedRwLockReadGuard::map(s.0, f))
    }

    /// Maps this guard to a component, returning `Err(s)` if `f` returns `None`.
    pub fn try_map<U: ?Sized, F>(s: Self, f: F) -> Result<MappedRwLockBellReadGuard<'a, U>, Self>
    where
        F: FnOnce(&T) -> Option<&U>,
    {
        match lock_api::MappedRwLockReadGuard::try_map(s.0, f) {
            Ok(mapped) => Ok(MappedRwLockBellReadGuard(mapped)),
            Err(guard) => Err(Self(guard)),
        }
    }

    /// Maps this guard to a component, returning `Err((s, e))` if `f` returns `Err(e)`.
    pub fn try_map_or_err<U: ?Sized, F, E>(
        s: Self,
        f: F,
    ) -> Result<MappedRwLockBellReadGuard<'a, U>, (Self, E)>
    where
        F: FnOnce(&T) -> Result<&U, E>,
    {
        match lock_api::MappedRwLockReadGuard::try_map_or_else(s.0, f) {
            Ok(mapped) => Ok(MappedRwLockBellReadGuard(mapped)),
            Err((guard, err)) => Err((Self(guard), err)),
        }
    }
}

forward_shared_traits!(MappedRwLockBellReadGuard<'a>);

/// Shared guard obtained through an [`Arc`], with no lifetime attached.
///
/// Holds the [`Arc`] itself rather than a reference into it, so it keeps the
/// allocation alive and can be stored in an owning struct without laundering
/// lifetimes.
///
/// Returned by [`RwLockBell::read_arc`] and [`RwLockBell::try_read_arc`].
///
/// [`Arc`]: std::sync::Arc
/// [`RwLockBell::read_arc`]: crate::RwLockBell::read_arc
/// [`RwLockBell::try_read_arc`]: crate::RwLockBell::try_read_arc
#[cfg(feature = "arc")]
#[cfg_attr(docsrs, doc(cfg(feature = "arc")))]
#[must_use = "if unused the lock is immediately released"]
pub struct ArcRwLockBellReadGuard<T: ?Sized>(pub(crate) RawArc<T>);

#[cfg(feature = "arc")]
forward_shared_traits!(ArcRwLockBellReadGuard);
