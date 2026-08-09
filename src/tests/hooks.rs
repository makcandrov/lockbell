//! Test-hook infrastructure for deterministic race-condition testing.
//!
//! Only compiled when `cfg(test)` is active (i.e. when testing this crate).
//!
//! # Quick start
//!
//! ```ignore
//! use crate::tests::hooks::{self, Gate, HookPoint, TestGuard};
//!
//! #[test]
//! fn my_race_test() {
//!     let _g = TestGuard::acquire(); // serialise hook tests, auto-clears on drop
//!
//!     let gate = Gate::new();
//!     let g2   = gate.clone();
//!     hooks::set(HookPoint::TryWriteOrBeforeAcquire, move || g2.wait());
//!
//!     let t = thread::spawn(|| lock.try_write_or(|| {}));
//!     gate.wait_for_arrival(); // thread is now paused
//!     // … do concurrent work …
//!     gate.open();             // release the thread
//!     t.join().unwrap();
//! }
//! ```

use std::{
    collections::HashMap,
    sync::{Arc, Condvar, Mutex, OnceLock},
};

// ─── HookPoint ───────────────────────────────────────────────────────────────

/// A point in the library code where a hook can be inserted.
///
/// Every variant is reached with **no library locks held**, so hook functions
/// (including [`Gate::wait`]) may safely call back into [`RwLockBell`][crate::RwLockBell].
///
/// **Exception:** avoid calling `try_write_or` from a hook placed at
/// [`DrainAfterWriteLockRelease`][HookPoint::DrainAfterWriteLockRelease] or
/// [`WriteGuardBeforeRawUnlock`][HookPoint::WriteGuardBeforeRawUnlock], because
/// `draining` is still non-zero at those points and the call will block until
/// the drain finishes.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum HookPoint {
    /// [`try_write_or`][crate::RwLockBell::try_write_or]: the `locking`
    /// counter has been incremented and the state mutex released, but
    /// `try_write` has not yet been called.
    TryWriteOrBeforeAcquire,

    /// `RawRwLockBell::unlock_exclusive`: called before acquiring the state
    /// mutex to begin the drain sequence.
    WriteGuardBeforeDrop,

    /// `RawRwLockBell::unlock_shared`: the underlying read lock has been
    /// released, but the state mutex has not yet been acquired to decrement
    /// the `readers` counter.
    ReadGuardAfterRelease,

    /// `drain_and_run`: the lock has been released, but this drain has not yet
    /// decremented `draining`.
    DrainAfterWriteLockRelease,

    /// `drain_and_run`: `draining` has been decremented (and `not_draining`
    /// notified if it reached 0), but callbacks have not yet started.
    DrainBeforeCallbacks,

    /// `RawRwLockBell::unlock_exclusive`: `draining` has just been bumped; the
    /// `locking_zero` wait has not yet started.
    ///
    /// **⚠ Called while holding the library's internal state mutex.**
    /// The hook function **must not** block, call any [`RwLockBell`][crate::RwLockBell]
    /// method, or acquire any mutex that could form a cycle with the state
    /// mutex.  Safe operations: [`Gate::signal`], atomic stores, non-blocking
    /// channel sends.
    WriteGuardAfterEnteringDrain,

    /// `RawRwLockBell::unlock_shared`: `draining` has just been bumped (only
    /// fires when this release triggers the drain, i.e. when it is the last
    /// reader with pending callbacks or in-flight `try_write_or` calls); the
    /// `locking_zero` wait has not yet started.
    ///
    /// Same constraints as [`WriteGuardAfterEnteringDrain`].
    ReadGuardAfterEnteringDrain,

    /// `RawRwLockBell::unlock_exclusive`: the callback queue has been taken and
    /// the state mutex released, but the exclusive lock is **still held**.
    ///
    /// This is the window in which an overlapping drain must not reopen the
    /// `try_write_or` gate.
    WriteGuardBeforeRawUnlock,

    /// `try_write_or_else`: the `while inner.draining > 0` loop body has been
    /// entered, just before `not_draining.wait()`.
    ///
    /// **⚠ Called while holding the library's internal state mutex.**
    /// Same constraints as [`WriteGuardAfterEnteringDrain`].
    TryWriteOrWhileDraining,
}

// ─── Registry ────────────────────────────────────────────────────────────────

type HookFn = Arc<dyn Fn() + Send + Sync + 'static>;

fn registry() -> &'static Mutex<HashMap<HookPoint, HookFn>> {
    static REGISTRY: OnceLock<Mutex<HashMap<HookPoint, HookFn>>> = OnceLock::new();
    REGISTRY.get_or_init(Default::default)
}

/// Runs the hook registered for `point`, if any.
///
/// The registry lock is released **before** calling the hook function, so
/// hooks may freely call [`set`], [`clear`], or re-enter [`run`] without
/// deadlocking.
pub(crate) fn run(point: HookPoint) {
    let f = registry().lock().unwrap().get(&point).cloned();
    if let Some(f) = f {
        f();
    }
}

/// Registers `f` as the hook for `point`, replacing any existing hook.
pub fn set(point: HookPoint, f: impl Fn() + Send + Sync + 'static) {
    registry().lock().unwrap().insert(point, Arc::new(f));
}

/// Removes the hook for `point`.
pub fn clear(point: HookPoint) {
    registry().lock().unwrap().remove(&point);
}

/// Removes all registered hooks.
pub fn clear_all() {
    registry().lock().unwrap().clear();
}

// ─── TestGuard ───────────────────────────────────────────────────────────────

/// Ensures hook-based tests run serially and auto-clears all hooks on drop.
///
/// Acquire one at the start of every test that registers hooks:
///
/// ```ignore
/// let _g = TestGuard::acquire();
/// ```
pub struct TestGuard(#[allow(dead_code)] std::sync::MutexGuard<'static, ()>);

impl TestGuard {
    /// Acquires the global test serialisation lock and clears any leftover hooks.
    pub fn acquire() -> Self {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let guard = LOCK
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        clear_all();
        Self(guard)
    }
}

impl Drop for TestGuard {
    fn drop(&mut self) {
        clear_all();
    }
}

// ─── Gate ────────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
enum GateState {
    #[default]
    Idle,
    Arrived,
    Open,
}

/// Blocks a thread at a hook point until the test releases it.
///
/// Typical pattern:
///
/// ```ignore
/// let gate = Gate::new();
/// let g2   = gate.clone();
///
/// hooks::set(HookPoint::TryWriteOrBeforeAcquire, move || g2.wait());
///
/// let t = thread::spawn(|| lock.try_write_or(|| {}));
/// gate.wait_for_arrival(); // test blocks here until thread reaches the hook
/// // … perform concurrent work while the thread is paused …
/// gate.open();             // unblock the thread
/// t.join().unwrap();
/// ```
///
/// A `Gate` is single-use by default; call [`reset`][Gate::reset] to reuse it.
#[derive(Debug)]
pub struct Gate {
    state: Mutex<GateState>,
    cv: Condvar,
}

impl Gate {
    /// Creates a new gate wrapped in an [`Arc`] for easy cloning.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(GateState::Idle),
            cv: Condvar::new(),
        })
    }

    /// Blocks the calling thread until [`open`][Self::open] is called.
    ///
    /// Signals arrival before blocking so the test thread can detect it via
    /// [`wait_for_arrival`][Self::wait_for_arrival].
    pub fn wait(&self) {
        let mut s = self.state.lock().unwrap();
        *s = GateState::Arrived;
        self.cv.notify_all();
        while *s == GateState::Arrived {
            s = self.cv.wait(s).unwrap();
        }
    }

    /// Blocks until the thread calls [`wait`][Self::wait] (i.e., arrives at
    /// the hook point).
    pub fn wait_for_arrival(&self) {
        let mut s = self.state.lock().unwrap();
        while *s != GateState::Arrived {
            s = self.cv.wait(s).unwrap();
        }
    }

    /// Unblocks the thread waiting in [`wait`][Self::wait].
    pub fn open(&self) {
        let mut s = self.state.lock().unwrap();
        *s = GateState::Open;
        self.cv.notify_all();
    }

    /// Signals arrival non-blockingly, without waiting for [`open`][Self::open].
    ///
    /// Safe to call while holding the library's internal state mutex because it
    /// does not block.  Use this from hook points that are called under the
    /// mutex (e.g. [`HookPoint::WriteGuardAfterSettingDropping`]).
    pub fn signal(&self) {
        let mut s = self.state.lock().unwrap();
        *s = GateState::Arrived;
        self.cv.notify_all();
    }

    /// Resets the gate to its initial state so it can be used again.
    #[allow(unused)]
    pub fn reset(&self) {
        *self.state.lock().unwrap() = GateState::Idle;
    }
}
