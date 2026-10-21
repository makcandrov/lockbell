# lockbell

[![Crates.io](https://img.shields.io/crates/v/lockbell)](https://crates.io/crates/lockbell)
[![docs.rs](https://img.shields.io/docsrs/lockbell)](https://docs.rs/lockbell)
[![License](https://img.shields.io/crates/l/lockbell)](https://github.com/makcandrov/lockbell#license)

An [`RwLock`] that rings back when contention clears.

[`RwLock`]: https://docs.rs/parking_lot/latest/parking_lot/type.RwLock.html

## The problem

[`RwLock::try_write`] returns [`None`] when the lock is contended — and gives you no way to know when it becomes free. You're left choosing between spinning, blocking, or silently dropping work.

[`RwLock::try_write`]: https://docs.rs/parking_lot/latest/parking_lot/type.RwLock.html#method.try_write

[`None`]: https://doc.rust-lang.org/std/option/enum.Option.html#variant.None

## The solution

```rust
use lockbell::RwLockBell;

let lock = RwLockBell::new(0u32);

// Holds the lock — any concurrent try_write_or will register a callback
let guard = lock.write();

// Fails: lock is held. Callback is queued.
lock.try_write_or(|| println!("lock is free now"));

// Dropping the guard fires all queued callbacks.
drop(guard);
```

When [`try_write_or`] fails, the callback is queued. Queued callbacks are flushed — without holding any lock — when either:

- a write guard is dropped, or
- the last active read guard is dropped (if callbacks were queued while readers held the lock).

[`try_write_or_else`] builds the callback only on contention, so the success path skips the setup entirely. Keep the factory itself cheap and leave any expensive work to the callback it returns — see below.

[`try_write_or`]: https://docs.rs/lockbell/latest/lockbell/struct.RwLockBell.html#method.try_write_or
[`try_write_or_else`]: https://docs.rs/lockbell/latest/lockbell/struct.RwLockBell.html#method.try_write_or_else

## Callback semantics

- Callbacks run **after** the release that drains them — never from inside it, and never with the firing thread holding a lock.
- A release snapshots the queue while it still holds, then fires that batch in registration order once it has let go. Ordering is **per batch**, not global: a callback that re-acquires the lock drains whatever it queues before returning, so a later registration can run ahead of an earlier one still waiting its turn in the outer batch.
- A callback is a notification, not a lock acquisition — by the time it fires the lock may already be held again, including by a reader that joined mid-release and was itself one of the guards the call lost to.
- A reader registers with the bell *before* it acquires, so a reader still waiting on the lock already counts as active. The departing last holder therefore defers the flush to it, and a queued callback waits out that reader's whole hold rather than firing the moment the lock next goes free.
- Callbacks must be `FnOnce() + Send + 'static`.
- A panicking callback does not prevent subsequent callbacks from running.
- Suppressed panic payloads are dropped with panic containment. If a payload's
  destructor itself panics, that new panic payload is leaked to avoid recursive
  panics or an abort during unwinding.
- `try_write_or` is not wait-free despite the name: it waits out a concurrent
  flush before deciding, so that a callback can never land in a batch that has
  already been taken. It never waits on the protected data.
- The *factory* passed to `try_write_or_else` is different: it runs while a
  concurrent unlock is waiting on it, so it must not touch the lock and must
  not block. The callback it returns has no such restriction, so expensive
  construction belongs there rather than in the factory. `try_write_or` takes
  an already-built callback and so runs no code of yours in that window at all.
- Formatting a lock with `{:?}` **is** a lock acquisition: it takes a real read
  lock, counts as a reader, and rings the bell if it turns out to be the last
  reader out with callbacks queued. It can therefore run a callback, block on a
  concurrent factory, and panic with a callback's payload. Do not format a
  lock from a place that cannot take those.

## Installation

```toml
[dependencies]
lockbell = "0.3"
```

## Features

- **`arc_lock`** — `Arc`-based guards that carry no lifetime, and the
  `read_arc` / `write_arc` / `try_write_arc_or` family that produces them. They
  hold the `Arc<RwLockBell<T>>` itself rather than a reference into it, so they
  can be stored in an owning struct without laundering a lifetime to `'static`.
  Enables `parking_lot/arc_lock`.

## Use cases

- **Lazy invalidation**: a thread that loses the write race registers a callback to invalidate a local cache entry once the winner finishes.
- **Coalesced work**: multiple producers collapse concurrent write attempts into a single update, with each loser notified on completion.
- **Write-through coordination**: trigger downstream work (logging, metrics, replication) exactly once per write, regardless of contention.
