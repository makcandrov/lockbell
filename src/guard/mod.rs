//! RAII guards for [`RwLockBell`](crate::RwLockBell).
//!
//! Each is a newtype over the corresponding [`lock_api`] guard. None declares a
//! `Drop` impl: releasing — and ringing — happens in `RawRwLockBell` via
//! ordinary drop glue, which keeps the `map` family a plain move rather than a
//! `take`/`forget` dance.
//!
//! |           | borrowed                   | mapped                           | via `Arc` (`arc` feature)  |
//! |-----------|----------------------------|----------------------------------|----------------------------|
//! | shared    | [`RwLockBellReadGuard`]    | [`MappedRwLockBellReadGuard`]    | `ArcRwLockBellReadGuard`   |
//! | exclusive | [`RwLockBellWriteGuard`]   | [`MappedRwLockBellWriteGuard`]   | `ArcRwLockBellWriteGuard`  |
//!
//! [`lock_api`]: parking_lot::lock_api

/// Forwards `Deref`, `Debug` and `Display` to the wrapped guard.
macro_rules! forward_shared_traits {
    ($ty:ident $(<$lt:lifetime>)?) => {
        impl<$($lt,)? T: ?Sized> ::std::ops::Deref for $ty<$($lt,)? T> {
            type Target = T;

            #[inline]
            fn deref(&self) -> &T {
                &self.0
            }
        }

        impl<$($lt,)? T: ?Sized + ::std::fmt::Debug> ::std::fmt::Debug for $ty<$($lt,)? T> {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                ::std::fmt::Debug::fmt(&**self, f)
            }
        }

        impl<$($lt,)? T: ?Sized + ::std::fmt::Display> ::std::fmt::Display for $ty<$($lt,)? T> {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                ::std::fmt::Display::fmt(&**self, f)
            }
        }
    };
}

/// Forwards `DerefMut` to the wrapped guard.
macro_rules! forward_mut_traits {
    ($ty:ident $(<$lt:lifetime>)?) => {
        impl<$($lt,)? T: ?Sized> ::std::ops::DerefMut for $ty<$($lt,)? T> {
            #[inline]
            fn deref_mut(&mut self) -> &mut T {
                &mut self.0
            }
        }
    };
}

mod read;
mod write;

pub use read::{MappedRwLockBellReadGuard, RwLockBellReadGuard};
pub use write::{MappedRwLockBellWriteGuard, RwLockBellWriteGuard};

#[cfg(feature = "arc_lock")]
pub use read::ArcRwLockBellReadGuard;
#[cfg(feature = "arc_lock")]
pub use write::ArcRwLockBellWriteGuard;
