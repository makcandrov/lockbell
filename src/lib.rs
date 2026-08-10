#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]

mod guard;
mod raw;
mod rwlock;
mod state;

#[cfg(test)]
mod tests;

pub use guard::{
    MappedRwLockBellReadGuard, MappedRwLockBellWriteGuard, RwLockBellReadGuard,
    RwLockBellWriteGuard,
};
pub use raw::RawRwLockBell;
pub use rwlock::RwLockBell;

#[cfg(feature = "arc")]
pub use guard::{ArcRwLockBellReadGuard, ArcRwLockBellWriteGuard};
