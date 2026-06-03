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

A pool of worker threads (`ConfigOptions::numThreads()`) pops tasks from
the `WorkQueue` and runs them. The queue is bounded
(`ConfigOptions::queueSize()`, default 1024) so producers experience
backpressure when the system is saturated.

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
- **Bounded**: producers blocked when the global queue was full.

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
  the per-group FIFO of pending tasks. A group is present only while it
  has at least one queued task.
- `std::list<TaskGroup*> rrOrder_` — the round-robin rotation. Each
  group appears at most once. The front is served next.
- `size_t totalTasks_` and `size_t maxSize_` — preserve the existing
  global backpressure cap.

### Push (`WorkQueue::push(TaskGroup*, Task*)`)

1. If the group is **already** in `groupQueues_`, the producer waits on
   `cvPush_` while `totalTasks_ >= maxSize_` (standard backpressure),
   then appends the task to the group's deque.
2. If the group is **new** (not currently in the rotation), the push is
   admitted immediately — bypassing the global cap for the *first*
   task. The group is inserted in `groupQueues_` and pushed to the back
   of `rrOrder_`.
3. `totalTasks_` is incremented and `cvPop_` is signalled.

**Admission bypass** is a deliberate fairness guarantee: a saturating
producer cannot prevent a newcomer from entering the rotation. Once a
group is in the rotation, its tasks are served alongside everyone
else's.

### Pop (worker thread)

1. Wait on `cvPop_` until `rrOrder_` is non-empty or the queue is
   closed.
2. Take the group `g` at the front of `rrOrder_`; pop one task from
   `groupQueues_[g]`.
3. If `g`'s queue is now empty, erase `g` from `groupQueues_`;
   otherwise re-append `g` to the back of `rrOrder_`. This is the
   round-robin rotation.
4. Signal `cvPush_` so any blocked producers can re-check the cap.
5. Execute the task outside the mutex.

Each task served = exactly one rotation step, so with `k` active groups
the worst-case latency for any one group is `k - 1` other tasks ahead.

### Shutdown

Destruction sets `closed_ = true`, notifies both condition variables,
and joins the worker threads. Workers continue draining tasks until
`rrOrder_` is empty, after which they exit. `push` after close throws
`eckit::SeriousBug` (this only occurs at program teardown).

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
| Global capacity cap                   | `queueSize` (hard)      | `queueSize` (soft for new groups)|
| New group admission under saturation  | Blocked behind producer | Always admitted (first task)     |
| Per-group ordering                    | FIFO                    | FIFO (unchanged)                 |
| Push API                              | `push(Task*)`           | `push(TaskGroup*, Task*)`        |
| Cancellation behaviour                | Unchanged               | Unchanged                        |

## Known limitations / future work

- **Pre-admission fairness across many simultaneous producers.** Once
  the global cap is hit, multiple producers from already-admitted
  groups compete for slots via `cvPush_.notify_all()`; the OS thread
  scheduler decides who wins. Per-group fair admission queues would
  require a more elaborate design and are not implemented.
- **No priority / weighting.** All groups are treated equally. Adding
  weighted round-robin (e.g. proportional to client quota) is a
  straightforward extension of the `rrOrder_` rotation.
- **Group identity is the `TaskGroup*` pointer.** This is safe because
  `TaskGroup::waitForTasks` guarantees the group outlives every one of
  its tasks in the queue, but the scheduler does not expose a stable
  external client/session identifier — distinct requests from the same
  client are still distinct groups.
