# EventsZite architecture

EventsZite is an embedded event store written in Zig and backed by a vendored SQLite amalgamation. The current public import remains `eventstoredb` for source compatibility, but the product and repository identity is **EventsZite**.

See [`EVENTSTOREDB-COMPATIBILITY.md`](./EVENTSTOREDB-COMPATIBILITY.md) for the compatibility boundary.

## High-level shape

```text
Zig application
  |
  v
EventsZite Client
  |- append / read
  |- catch-up subscriptions
  |- persistent subscriptions
  |- snapshots / metadata / projections
  |
  v
SQLite WAL
  |- streams
  |- events
  |- committed_event_ids
  |- persistent_subscriptions
  |- persistent_acks
  |- snapshots
  |- projection_checkpoints
  `- schema_info
```

There is no required daemon. The library is embedded in the caller process. The CLI is an optional operational surface, not the durability authority.

## Storage model

The canonical schema is defined in [`src/schema.zig`](../src/schema.zig). Current schema version: **3**.

### `events`

Important coordinates are deliberately separate:

- `event_number`: zero-based revision inside one stream;
- `log_position`: global append order in the database;
- `transaction_position`: groups events committed by one append operation.

The DCB extension adds nullable `sequence`, `tags` and `dc_time` fields.

### Idempotency

`committed_event_ids` provides a direct lookup by caller-supplied event ID. Retrying the same logical append with the same ID does not require scanning the event log.

### Streams

`streams` stores the current revision plus metadata/truncation/tombstone state. Tombstoning prevents normal append/read operations while preserving historical rows for audit/recovery policy.

### Persistent subscriptions

`persistent_subscriptions.last_position` means:

> the next stream revision that is not durably complete.

`persistent_acks` stores durable delivery state for each consumer-group event, including:

- `event_id`;
- `event_number` (stream coordinate);
- `log_position` (global coordinate, retained separately);
- retry count;
- parked state;
- durable `acked` state.

Completed rows are retained in v0.1. This makes duplicate ACKs idempotent and preserves evidence needed to skip an already-completed event after a restart when it sits beyond an earlier gap. A future compaction design must preserve those semantics before deleting completion records.

## Schema migrations

Migrations are forward-only and versioned.

Version-specific indexes are created only after the columns they depend on exist. This is important for old databases: base DDL must not reference a v2/v3 column before the corresponding `ALTER TABLE` migration has run.

Version 2 adds the DCB event fields and indexes. Version 3 adds persistent `event_number` and `acked` state, backfills existing in-flight rows from `events`, and creates the persistent frontier index.

## Concurrency model

SQLite is the storage serialization boundary. EventsZite v0.1 deliberately uses a small connection model rather than pretending to expose a generic pool:

```text
1 canonical writer connection
+ optional 1 subscription-read connection for file-backed stores
```

There is no `max_connections` option because there is no generic connection pool to enforce it.

### Writer serialization

Writer-side operations use `Client.writer_mu` so expected-revision checks and their transaction cannot be interleaved by another same-process writer.

The lock is adaptive: it spins only for a short bounded fast path, then yields through `std.Io.sleep` while contention continues. This avoids burning a CPU core when the current owner is inside SQLite/WAL I/O.

Cross-process contention remains governed by SQLite and `PRAGMA busy_timeout`.

### WAL reads

For file-backed stores, `separate_read_connection = true` opens a second SQLite handle used by subscription workers. This lets WAL readers use a different handle from the serialized writer.

Plain SQLite `:memory:` is connection-local, so EventsZite rejects `:memory:` together with `separate_read_connection = true`. Two normal `:memory:` handles would otherwise be two unrelated databases presented as one `Client`.

## Catch-up subscriptions

Catch-up workers:

1. read from the durable cursor;
2. enqueue events in order;
3. advance only after ownership transfers to the queue;
4. immediately continue when a full page was read;
5. otherwise wait for append wakeup or poll timeout.

`SubscribeOptions.buffer_size` controls an allocator-owned bounded queue. `0` selects the default capacity of 256.

When the queue is full, the worker applies backpressure. Events are not silently discarded. Closing the subscription/client cancels the wait and frees any event ownership still held by the queue/producer path.

Time-based waits use real scheduler-friendly `std.Io.sleep`; `spinLoopHint()` is reserved for short atomic lock acquisition rather than used as a clock.

## Persistent subscription delivery

Before a persistent event is handed to the consumer, an in-flight row is durably recorded.

ACK processing is serialized and transactional:

1. locate the delivered event by `(group, stream, event_id)`;
2. mark it `acked = 1`;
3. find the first incomplete stream revision at or after the current checkpoint;
4. advance `last_position` only to that contiguous frontier;
5. commit ACK state and checkpoint atomically.

Example for delivered revisions `0, 1, 2`:

```text
ACK 2 -> checkpoint 0
ACK 0 -> checkpoint 1
ACK 0 -> checkpoint 1  (idempotent duplicate)
ACK 1 -> checkpoint 3
```

ACKed events beyond a gap remain durable. After restart they are skipped rather than redelivered or reset to incomplete. NACK/park state does not advance the contiguous checkpoint.

A file-backed reopen regression test exercises this behavior across actual client close/open cycles.

## Client lifetime

`Client.close()` is destructive and deterministic:

1. atomically publish `closed`;
2. wake/cancel subscription waiters;
3. wait until `active_workers == 0` using scheduler-friendly waits;
4. close the optional read connection;
5. close the writer connection;
6. release client-owned memory.

The implementation does not intentionally free SQLite/client memory while a registered worker may still dereference it.

## Failure model

| Failure | Contract |
| --- | --- |
| Process crash during append | SQLite/WAL preserves transaction atomicity. |
| Same-process concurrent writers | Serialized by `writer_mu`. |
| Cross-process writer contention | SQLite busy timeout applies. |
| Slow catch-up consumer | Bounded queue backpressures; no silent drop. |
| Persistent ACKs out of order | Durable checkpoint stops at first incomplete revision. |
| Restart after later ACK | Durable ACK row is preserved and replay skips that completed revision. |
| `:memory:` plus separate read handle | Rejected as `UnsupportedConfiguration`. |

## Compatibility boundary

EventsZite provides an EventStoreDB-inspired embedded programming model. It does not claim EventStoreDB wire-protocol, clustering, replication, gRPC-client, authentication or complete persistent-subscription parity.

See [`EVENTSTOREDB-COMPATIBILITY.md`](./EVENTSTOREDB-COMPATIBILITY.md) for the maintained statement.

## Extension points

Current roadmap-level extension points include:

- explicit parked-message replay/resolution;
- filter subscriptions;
- multi-stream transaction API;
- first-class DCB conditional append/read helpers;
- optional shared-memory URI mode if multi-connection in-memory operation is required;
- a future versioned package/import rename from `eventstoredb` to `eventszite` without silently breaking consumers.
