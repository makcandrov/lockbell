//! Test hooks for deterministic race testing. `cfg(test)` only.
//!
//! ```ignore
//! let _g = TestGuard::acquire(); // serialise; auto-clears on drop
//!
//! let gate = Gate::new();
//! let g2   = gate.clone();
//! hooks::set(HookPoint::TryWriteOrBeforeAcquire, move || g2.wait());
//!
//! let t = thread::spawn(|| lock.try_write_or(|| {}));
//! gate.wait_for_arrival(); // thread is now paused
//! gate.open();             // release it
//! t.join().unwrap();
//! ```

use std::{
    collections::HashMap,
    sync::{Arc, Condvar, Mutex, OnceLock},
};

/// A point in the library code where a hook can be inserted.
///
/// Variants marked ⚠ run under the state mutex: **signal only** — no blocking,
/// no `RwLockBell` method, no mutex that could cycle with it. The rest hold no
/// library lock and may call back in freely, except that `try_write_or` blocks
/// at the three `*Before*Unlock`/`GateReopens` points, where `draining` is
/// still non-zero.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum HookPoint {
    /// `try_write_or`: `locking` bumped and mutex released, before `try_write`.
    TryWriteOrBeforeAcquire,

    /// `unlock_exclusive`: entry, before taking the state mutex.
    WriteGuardBeforeDrop,

    /// `unlock_shared`: batch taken, shared lock **still held**. The read-side
    /// mirror of [`WriteGuardBeforeRawUnlock`]; no other thread can hold the
    /// lock across this point.
    ReadGuardBeforeRawUnlock,

    /// `unlock_exclusive`: batch taken, exclusive lock **still held**. The
    /// window in which an overlapping drain must not reopen the gate.
    WriteGuardBeforeRawUnlock,

    /// `drain_and_run`: lock released, `draining` not yet decremented.
    DrainBeforeGateReopens,

    /// `drain_and_run`: `draining` decremented, before the callbacks run.
    DrainBeforeCallbacks,

    /// `unlock_exclusive`: `draining` bumped, before the `locking_zero` wait.
    WriteGuardAfterEnteringDrain,

    /// `unlock_shared`: same, and only when this release triggers the drain.
    ReadGuardAfterEnteringDrain,

    /// `try_write_or_else`: inside the `draining > 0` loop, before the wait.
    TryWriteOrWhileDraining,

    /// `unlock_shared`: non-draining branch, shared lock still held and the
    /// state mutex still taken. Anything reaching for the bell blocks here.
    ReadGuardBeforeSkippedDrainUnlock,
}

type HookFn = Arc<dyn Fn() + Send + Sync + 'static>;

fn registry() -> &'static Mutex<HashMap<HookPoint, HookFn>> {
    static REGISTRY: OnceLock<Mutex<HashMap<HookPoint, HookFn>>> = OnceLock::new();
    REGISTRY.get_or_init(Default::default)
}

/// Runs the hook registered for `point`, if any.
///
/// The registry lock is released before calling it, so hooks may re-enter.
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

/// Serialises hook-based tests and clears all hooks on drop. Acquire one at the
/// start of every test that registers hooks.
pub struct TestGuard(#[allow(dead_code)] std::sync::MutexGuard<'static, ()>);

impl TestGuard {
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

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
enum GateState {
    #[default]
    Idle,
    Arrived,
    Open,
}

/// Blocks a thread at a hook point until the test releases it. Single-use
/// unless [`reset`][Gate::reset].
#[derive(Debug)]
pub struct Gate {
    state: Mutex<GateState>,
    cv: Condvar,
}

impl Gate {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(GateState::Idle),
            cv: Condvar::new(),
        })
    }

    /// Signals arrival, then blocks until [`open`][Self::open].
    pub fn wait(&self) {
        let mut s = self.state.lock().unwrap();
        *s = GateState::Arrived;
        self.cv.notify_all();
        while *s == GateState::Arrived {
            s = self.cv.wait(s).unwrap();
        }
    }

    /// Blocks until the hooked thread arrives.
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

    /// Signals arrival without blocking — the only form usable from the ⚠ hook
    /// points, which run under the state mutex.
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
