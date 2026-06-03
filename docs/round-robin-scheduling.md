# Round-Robin Work Scheduling

This note documents the gribjump worker-thread scheduling system,
covering the previous FIFO design, its limitations, and the new
round-robin scheduler that replaces it.

## Overview

Every public gribjump call (e.g. `Engine::extract`, `Engine::scan`) is
served by a `TaskGroup`. The engine splits the request into many `Task`
instances (one per file, typically), enqueues them on the shared
`WorkQueue` singleton, and blocks in `TaskGroup::waitForTasks()` until
every task has completed, errored, or been cancelled.

A pool of worker threads (`ConfigOptions::numThreads()`) pops tasks
from the `WorkQueue` and runs them.

## Before: single FIFO queue

The old `WorkQueue` wrapped a single `eckit::Queue<WorkItem>`:

```
TaskGroup A.enqueueTask ──┐
TaskGroup B.enqueueTask ──┼──►  [ A0 A1 A2 ... A999 B0 B1 ... ] ──► workers
TaskGroup C.enqueueTask ──┘             (single FIFO)
```

Properties:

- **Strict FIFO**: tasks were dispatched strictly in the order in which
  `WorkQueue::push(Task*)` was called.
- **Bounded**: the queue had a hard cap (`ConfigOptions::queueSize()`,
  default 1024) and producers blocked when it was full.

### The starvation problem

Because every group shared one FIFO, a very large `TaskGroup` could
completely block every later request. Concretely:

1. Client A submits an extraction touching 10 000 files. `TaskGroup A`
   pushes 10 000 tasks; the first 1 024 occupy the queue, the producer
   then blocks waiting for slots.
2. Client B submits a small extraction touching 5 files. `TaskGroup B`
   tries to push, but the queue is full *and* the slots will be reused
   by A's pending tasks as workers drain them.
3. B's 5 tasks cannot enter the queue until enough of A's tasks have
   been drained for A's producer to finish pushing, and even then B's
   tasks sit behind whichever A tasks are queued ahead.

As a result, small requests waited for large requests to finish — a
classic head-of-line blocking issue.

### Note on the old size cap

The old queue cap was not actually bounding any meaningful resource.
By the time `push()` was called, the producer had already allocated
every `ExtractionItem` and the entire `filemap` describing the
request. A queued `Task*` is a small handle on top of that state, so
capping the number of pending handles did not cap memory or I/O — it
only added complexity and contributed to the starvation problem above.
The cap (and the `GRIBJUMP_QUEUESIZE` / `gribjumpQueueSize` /
`ConfigOptions::queueSize()` knob that exposed it) has been removed.

## After: round-robin per-group scheduling

The new `WorkQueue` keeps a separate FIFO per active `TaskGroup` and
dispatches them in round-robin order:

```
                 ┌─► [ A0 A1 A2 ... A999 ]
TaskGroup A.push ┤
TaskGroup B.push ┼─► [ B0 B1 B2 B3 B4   ]   round-robin
TaskGroup C.push ┤                            └─► A0, B0, C0,
                 └─► [ C0 C1            ]          A1, B1, C1,
                                                    A2, B2, A3, ...  ──► workers
```

### Data structures

- `std::unordered_map<TaskGroup*, std::deque<Task*>> groupQueues_` —
  the per-group FIFO of pending tasks. A group is present only while
  it has at least one queued task.
- `std::list<TaskGroup*> rrOrder_` — the round-robin rotation. Each
  group appears at most once. The front is served next.
- A single mutex `mtx_` and condition variable `cv_` coordinate
  producers, consumers, and shutdown.

The queue is **unbounded** — `push` never blocks. See "Why no size
cap" above.

### Push (`WorkQueue::push(TaskGroup*, Task*)`)

1. Lock `mtx_`.
2. If the group is not yet in `groupQueues_`, insert an empty deque
   for it and append the group to the back of `rrOrder_`.
3. Append the task to the group's deque.
4. Unlock; signal `cv_`.

`push` is non-blocking and constant-time. A newcomer is always
admitted immediately into the rotation, so no producer can starve
another.

### Pop (worker thread)

1. Lock `mtx_`. Wait on `cv_` until `rrOrder_` is non-empty or the
   queue is closed.
2. Take the group `g` at the front of `rrOrder_`; pop one task from
   `groupQueues_[g]`.
3. If `g`'s queue is now empty, erase `g` from `groupQueues_`;
   otherwise re-append `g` to the back of `rrOrder_`. This is the
   round-robin rotation.
4. Unlock; execute the task outside the mutex.

Each task served = exactly one rotation step, so with `k` active
groups the worst-case latency for any one group is `k − 1` other
tasks ahead.

### Shutdown

Destruction sets `closed_ = true`, notifies the condition variable,
and joins the worker threads. Workers continue draining tasks until
`rrOrder_` is empty, after which they exit. `push` after close is a
programming error and asserts.

## API change

`WorkQueue::push` now takes the owning `TaskGroup*` as well as the
`Task*`:

```cpp
// before
WorkQueue::instance().push(task);

// after
WorkQueue::instance().push(this, task);   // called from TaskGroup::enqueueTask
```

`TaskGroup::enqueueTask` is the only caller of `WorkQueue::push`, so
the change is fully internal.

## Properties summary

| Property                              | Before (FIFO)           | After (round-robin)              |
|---------------------------------------|-------------------------|----------------------------------|
| Dispatch order                        | Strict global FIFO      | Round-robin across active groups |
| Head-of-line blocking between groups  | Yes                     | No                               |
| Worst-case wait for next group's task | All earlier tasks       | `(k − 1)` tasks (`k` groups)     |
| Producer backpressure                 | Blocks at `queueSize`   | None (push is non-blocking)      |
| `queueSize` / `GRIBJUMP_QUEUESIZE`    | Honoured (default 1024) | Removed                          |
| Per-group ordering                    | FIFO                    | FIFO (unchanged)                 |
| Push API                              | `push(Task*)`           | `push(TaskGroup*, Task*)`        |
| Cancellation behaviour                | Unchanged               | Unchanged                        |

## Known limitations / future work

- **No priority / weighting.** All groups are treated equally. Adding
  weighted round-robin (e.g. proportional to client quota) is a
  straightforward extension of the `rrOrder_` rotation.
- **Group identity is the `TaskGroup*` pointer.** This is safe because
  `TaskGroup::waitForTasks` guarantees the group outlives every one
  of its tasks in the queue, but the scheduler does not expose a
  stable external client/session identifier — distinct requests from
  the same client are still distinct groups.
- **No flow control on producers.** Removing the cap means a runaway
  producer could grow `groupQueues_` without bound. In practice this
  is gated by upstream limits (request size, FDB list latency), but
  if a future workload changes that assumption an explicit per-group
  or per-client cap would be the right place to add it back.
