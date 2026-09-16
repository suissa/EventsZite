# EventsZite v0.1 — technical review

Date: 2026-09-15

## Executive summary

EventsZite already had a useful embedded event-store core: SQLite WAL durability, expected-revision appends, stream/global reads, catch-up subscriptions, snapshots, metadata, projection checkpoints, a DCB-oriented schema extension, and a broad test layout.

The review found the highest risk in worker lifetime, subscription delivery semantics, configuration contracts, migration ordering and documentation accuracy rather than in the basic append/read path.

## Corrected in this review

### 1. Client shutdown lifetime

`Client.close()` previously had a bounded wait and could destroy SQLite/client memory while workers still existed. Shutdown is now deterministic: publish `closed`, wake waiters, wait for `active_workers == 0`, then release SQLite and the client.

### 2. Catch-up worker accounting

Worker cleanup is installed before waiter registration so allocator failure cannot leave `active_workers` permanently incremented.

### 3. Persistent worker stack lifetime

`connectPersistentSubscription()` previously spawned a worker with a pointer to a local `PersistentSubscription` value. Workers now receive a heap-owned `PSRunContext` with independent stream/group storage.

### 4. Queue ownership and loss

Catch-up queues are allocator-owned and sized from `SubscribeOptions.buffer_size` (`0` selects the documented default). When full, producers apply backpressure rather than silently discarding events. Persistent queues also stop dropping owned payloads silently and drain pending messages on close.

### 5. Scheduler-friendly waits and writer contention

Time-based polling no longer treats `std.atomic.spinLoopHint()` as elapsed milliseconds. `Waker.wait`, persistent polling, queue backpressure, receive loops and client shutdown use Zig 0.16 `std.Io.sleep` for wall-clock waiting.

`Client.writer_mu` was also changed from an unbounded spinlock to an adaptive lock: it spins only for a small bounded fast path, then sleeps/yields between retries. This matters because writer serialization can cover real SQLite/WAL I/O.

Short atomic queue/list critical sections still use spin acquisition where the protected region is in-memory and bounded.

### 6. Connection configuration contract

`OpenOptions.max_connections` was removed because v0.1 does not implement a connection pool. The actual model is one writer handle and optionally one dedicated subscription-read handle.

Plain SQLite `:memory:` plus `separate_read_connection = true` is rejected with `UnsupportedConfiguration`, because two normal `:memory:` handles are two independent databases. File-backed stores may use the second read handle under WAL.

### 7. Persistent checkpoint semantics

Schema version 3 adds `persistent_acks.event_number` and durable `acked` state. The event number is the stream coordinate; global `log_position` is retained separately.

`PersistentSubscription.ack()` now executes ACK mutation, contiguous-frontier calculation and checkpoint update inside one SQLite transaction under writer serialization.

The durable `last_position` is defined as the **next stream revision that is not durably complete**. Therefore:

- ACK 2 while 0 and 1 are incomplete leaves checkpoint 0;
- ACK 0 moves checkpoint to 1;
- duplicate ACK 0 remains checkpoint 1 and succeeds;
- ACK 1 closes the gap and moves checkpoint to 3;
- duplicate ACK 2 after the frontier reaches 3 still succeeds;
- NACK/park never advances the checkpoint;
- ACKed rows beyond a gap remain durable and are skipped during replay rather than reset to incomplete.

Completed ACK rows are intentionally retained in v0.1 so duplicate ACK remains idempotent and restart has durable completion evidence. Any future compaction mechanism must preserve those semantics first.

Tests now include both the in-process out-of-order sequence and a file-backed close/reopen sequence that proves the gap and later ACK survive an actual SQLite reopen.

### 8. Persistent worker shutdown

Persistent workers observe `Client.closed`, use real timed sleeps, and own their run context independently. `Client.close()` can therefore wait for them without destroying memory they still reference.

### 9. Schema migration ordering

The previous DDL structure could reference newer columns in indexes before an older database had run the migration that adds those columns. Version-specific indexes are now created only after their versioned columns exist.

The v3 migration also backfills `persistent_acks.event_number` from the canonical `events` row before creating the frontier index.

### 10. Missing CI stress source

`build.zig` referenced `tests/stress/separate_read_conn.zig`, but the file did not exist. The missing regression source was added and exercises a file-backed writer plus a dedicated WAL read connection with contiguous global positions.

## Documentation corrections

The product/repository name is **EventsZite**. The current Zig import/package compatibility surface still uses `eventstoredb` / `eventstoredb_zig`; this is a compatibility name, not the product identity.

The project implements an **EventStoreDB-inspired programming model over SQLite**. It is not wire-compatible with EventStoreDB and should not be described as a network drop-in replacement. `docs/EVENTSTOREDB-COMPATIBILITY.md` is the authority for that boundary.

## Current concurrency model

For v0.1:

- one canonical writer SQLite connection per `Client`;
- optional second read connection for subscription workers on file-backed stores;
- no generic connection pool;
- writer-side mutations are serialized by an adaptive writer lock;
- subscription queues are bounded and backpressured;
- time-based waits yield through `std.Io.sleep` rather than synthetic spin timing.

## Remaining roadmap items

The following are follow-up features rather than known correctness gaps in the paths corrected above:

1. Consider a shared-memory SQLite URI mode if multi-connection in-memory operation becomes a real requirement.
2. Define an explicit API for replaying/resolving parked persistent messages.
3. Design ACK-row compaction only if it can preserve duplicate-ACK idempotency and restart evidence.
4. Decide the versioned rename strategy from compatibility import `eventstoredb` to first-class `eventszite` without breaking consumers.
5. Keep Linux + Windows CI and `zig fmt --check` as mandatory merge gates.

## Merge criterion

This corrective branch is mergeable only when the current PR head passes the Linux and Windows test jobs plus formatting checks. Historical test counts are not sufficient; the changed worker/checkpoint/migration paths must compile and execute on the current head.
