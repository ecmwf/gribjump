# Round-Robin Work Scheduling

This note documents the GribJump worker-thread round-robin scheduling system.

## Overview

Calls to GribJump's engine/schedulign system (e.g. `Engine::extract`, `Engine::scan`) are served by a single
`TaskGroup`. The engine splits the request into many `Task` instances (one per file, typically), enqueues them
on the shared `WorkQueue` singleton, and blocks in `TaskGroup::waitForTasks()` until every task has completed,
errored, or been cancelled.

A pool of worker threads (`ConfigOptions::numThreads()`) pops tasks from the `WorkQueue` and runs them.

## Before: single FIFO queue

The old `WorkQueue` wrapped a single `eckit::Queue<WorkItem>`:

```
TaskGroup A.enqueueTask ──┐
TaskGroup B.enqueueTask ──┼──►  [ A0 A1 A2 ... A999 B0 B1 ... ] ──► workers
TaskGroup C.enqueueTask ──┘             (single FIFO)
```

Properties:

- **Strict FIFO**: tasks were dispatched strictly in the order in which `WorkQueue::push(Task*)` was called.
- **Bounded**: the queue had a hard cap (`ConfigOptions::queueSize()`, default 1024) and producers blocked
  when it was full.

### The starvation problem

Because every group shared one FIFO, a very large `TaskGroup` could completely block every later request.
Concretely:

1. Client A submits an extraction touching 10 000 files. `TaskGroup A` pushes 10 000 tasks; the first 1 024
   occupy the queue, the producer then blocks waiting for slots.
2. Client B submits a small extraction touching 5 files. `TaskGroup B` tries to push, but the queue is full
   _and_ the slots will be reused by A's pending tasks as workers drain them.
3. B's 5 tasks cannot enter the queue until enough of A's tasks have been drained for A's producer to finish
   pushing, and even then B's tasks sit behind whichever A tasks are queued ahead.

As a result, small requests waited for large requests to finish — a classic head-of-line blocking issue.

## Round-robin per-group scheduling

All GribJump versions before 0.13.0 used a single FIFO `WorkQueue` for all task groups.

```
TaskGroup A.enqueueTask ──┐
TaskGroup B.enqueueTask ──┼──►  [ A0 A1 A2 ... A999 B0 B1 ... ] ──► workers
TaskGroup C.enqueueTask ──┘             (single FIFO)
```

This meant that a very large job could completely block every later request. The new `WorkQueue` keeps a
separate FIFO per active `TaskGroup` and dispatches them in round-robin order:

```
                 ┌─► [ A0 A1 A2 ... A999 ]
TaskGroup A.push ┤
TaskGroup B.push ┼─► [ B0 B1 B2 B3 B4   ]   round-robin
TaskGroup C.push ┤                            └─► A0, B0, C0,
                 └─► [ C0 C1            ]          A1, B1, C1,
                                                    A2, B2, A3, ...  ──► workers
```

Which allows small requests to be handled while large requests are still in prgoress.

### Data structures

- `std::unordered_map<TaskGroup*, std::deque<Task*>> groupQueues_` — the per-group FIFO of pending tasks. A
  group is present only while it has at least one queued task.
- `std::list<TaskGroup*> rrOrder_` — the round-robin rotation. Each group appears at most once. The front is
  served next.
- A single mutex `mtx_` and condition variable `cv_` coordinate producers, consumers, and shutdown.

### Push (`WorkQueue::push(TaskGroup*, Task*)`)d

1. Lock `mtx_`.
2. If the group is not yet in `groupQueues_`, insert an empty deque for it and append the group to the back of
   `rrOrder_`.
3. Append the task to the group's deque.
4. Unlock; signal `cv_`.

`push` is non-blocking and constant-time. A newcomer is always admitted immediately into the rotation, so no
producer can starve another.

### Pop (worker thread)

1. Lock `mtx_`. Wait on `cv_` until `rrOrder_` is non-empty or the queue is closed.
2. Take the group `g` at the front of `rrOrder_`; pop one task from `groupQueues_[g]`.
3. If `g`'s queue is now empty, erase `g` from `groupQueues_`; otherwise re-append `g` to the back of
   `rrOrder_`. This is the round-robin rotation.
4. Unlock; execute the task outside the mutex.

Each task served = exactly one rotation step, so with `k` active groups the worst-case latency for any one
group is `k − 1` other tasks ahead.

### Shutdown

Destruction sets `closed_ = true`, notifies the condition variable, and joins the worker threads. Workers
continue draining tasks until `rrOrder_` is empty, after which they exit. `push` after close is a programming
error and asserts.

### Lifetime of the `TaskGroup*`

`TaskGroup::waitForTasks()` blocks until every task has notified completion, and tasks notify _after_ being
popped from the `WorkQueue` (popping removes the group's entry from `groupQueues_`/`rrOrder_` under the
queue's mutex, before the task runs). Therefore, by the time `waitForTasks()` returns, the `WorkQueue` no
longer holds the group's pointer in any data structure, and the group is safe to destroy. This is the only
lifetime requirement the round-robin scheduler imposes.

## Known limitations

- **No priority / weighting.** All groups are treated equally. Adding weighted round-robin (e.g. proportional
  to client quota) is a straightforward extension of the `rrOrder_` rotation.
- **No flow control on producers.** A runaway producer could grow `groupQueues_` without bound.
