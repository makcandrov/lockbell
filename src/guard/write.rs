//! Exclusive guards.

use parking_lot::lock_api;

use crate::raw::RawRwLockBell;

type RawGuard<'a, T> = lock_api::RwLockWriteGuard<'a, RawRwLockBell, T>;
type RawMapped<'a, T> = lock_api::MappedRwLockWriteGuard<'a, RawRwLockBell, T>;
#[cfg(feature = "arc_lock")]
type RawArc<T> = lock_api::ArcRwLockWriteGuard<RawRwLockBell, T>;

/// RAII exclusive guard for [`RwLockBell`](crate::RwLockBell).
///
/// Dropping it releases the lock and fires every callback registered via
/// [`try_write_or`] while it was held.
///
/// Returned by [`RwLockBell::write`], [`RwLockBell::try_write`] and
/// [`try_write_or`].
///
/// [`try_write_or`]: crate::RwLockBell::try_write_or
/// [`RwLockBell::write`]: crate::RwLockBell::write
/// [`RwLockBell::try_write`]: crate::RwLockBell::try_write
#[must_use = "if unused the lock is immediately released"]
pub struct RwLockBellWriteGuard<'a, T: ?Sized>(pub(crate) RawGuard<'a, T>);

impl<'a, T: ?Sized> RwLockBellWriteGuard<'a, T> {
    /// Maps this guard to a component of the protected value.
    ///
    /// An associated function so it cannot shadow a method on `T`.
    pub fn map<U: ?Sized, F>(s: Self, f: F) -> MappedRwLockBellWriteGuard<'a, U>
    where
        F: FnOnce(&mut T) -> &mut U,
    {
        MappedRwLockBellWriteGuard(lock_api::RwLockWriteGuard::map(s.0, f))
    }

    /// Maps this guard to a component, returning `Err(s)` if `f` returns `None`.
    pub fn try_map<U: ?Sized, F>(s: Self, f: F) -> Result<MappedRwLockBellWriteGuard<'a, U>, Self>
    where
        F: FnOnce(&mut T) -> Option<&mut U>,
    {
        match lock_api::RwLockWriteGuard::try_map(s.0, f) {
            Ok(mapped) => Ok(MappedRwLockBellWriteGuard(mapped)),
            Err(guard) => Err(Self(guard)),
        }
    }

    /// Maps this guard to a component, returning `Err((s, e))` if `f` returns `Err(e)`.
    pub fn try_map_or_err<U: ?Sized, F, E>(
        s: Self,
        f: F,
    ) -> Result<MappedRwLockBellWriteGuard<'a, U>, (Self, E)>
    where
        F: FnOnce(&mut T) -> Result<&mut U, E>,
    {
        match lock_api::RwLockWriteGuard::try_map_or_err(s.0, f) {
            Ok(mapped) => Ok(MappedRwLockBellWriteGuard(mapped)),
            Err((guard, err)) => Err((Self(guard), err)),
        }
    }
}

forward_shared_traits!(RwLockBellWriteGuard<'a>);
forward_mut_traits!(RwLockBellWriteGuard<'a>);

/// RAII exclusive guard produced by [`RwLockBellWriteGuard::map`] and friends.
///
/// Behaves like [`RwLockBellWriteGuard`] but dereferences to a component.
#[must_use = "if unused the lock is immediately released"]
pub struct MappedRwLockBellWriteGuard<'a, T: ?Sized>(pub(crate) RawMapped<'a, T>);

impl<'a, T: ?Sized> MappedRwLockBellWriteGuard<'a, T> {
    /// Maps this guard to a component of the protected value.
    pub fn map<U: ?Sized, F>(s: Self, f: F) -> MappedRwLockBellWriteGuard<'a, U>
    where
        F: FnOnce(&mut T) -> &mut U,
    {
        MappedRwLockBellWriteGuard(lock_api::MappedRwLockWriteGuard::map(s.0, f))
    }

    /// Maps this guard to a component, returning `Err(s)` if `f` returns `None`.
    pub fn try_map<U: ?Sized, F>(s: Self, f: F) -> Result<MappedRwLockBellWriteGuard<'a, U>, Self>
    where
        F: FnOnce(&mut T) -> Option<&mut U>,
    {
        match lock_api::MappedRwLockWriteGuard::try_map(s.0, f) {
            Ok(mapped) => Ok(MappedRwLockBellWriteGuard(mapped)),
            Err(guard) => Err(Self(guard)),
        }
    }

    /// Maps this guard to a component, returning `Err((s, e))` if `f` returns `Err(e)`.
    pub fn try_map_or_err<U: ?Sized, F, E>(
        s: Self,
        f: F,
    ) -> Result<MappedRwLockBellWriteGuard<'a, U>, (Self, E)>
    where
        F: FnOnce(&mut T) -> Result<&mut U, E>,
    {
        match lock_api::MappedRwLockWriteGuard::try_map_or_err(s.0, f) {
            Ok(mapped) => Ok(MappedRwLockBellWriteGuard(mapped)),
            Err((guard, err)) => Err((Self(guard), err)),
        }
    }
}

forward_shared_traits!(MappedRwLockBellWriteGuard<'a>);
forward_mut_traits!(MappedRwLockBellWriteGuard<'a>);

/// Exclusive guard obtained through an [`Arc`], with no lifetime attached.
///
/// See [`ArcRwLockBellReadGuard`](crate::ArcRwLockBellReadGuard).
///
/// Returned by [`RwLockBell::write_arc`], [`RwLockBell::try_write_arc`] and
/// [`RwLockBell::try_write_arc_or`].
///
/// [`Arc`]: std::sync::Arc
/// [`RwLockBell::write_arc`]: crate::RwLockBell::write_arc
/// [`RwLockBell::try_write_arc`]: crate::RwLockBell::try_write_arc
/// [`RwLockBell::try_write_arc_or`]: crate::RwLockBell::try_write_arc_or
#[cfg(feature = "arc_lock")]
#[cfg_attr(docsrs, doc(cfg(feature = "arc_lock")))]
#[must_use = "if unused the lock is immediately released"]
pub struct ArcRwLockBellWriteGuard<T: ?Sized>(pub(crate) RawArc<T>);

#[cfg(feature = "arc_lock")]
forward_shared_traits!(ArcRwLockBellWriteGuard);
#[cfg(feature = "arc_lock")]
forward_mut_traits!(ArcRwLockBellWriteGuard);
