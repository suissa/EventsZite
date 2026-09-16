# EventsZite

> Embedded event store for Zig, powered by SQLite.

EventsZite provides an EventStoreDB-inspired programming model on top of a local SQLite database: streams, optimistic expected revisions, global log positions, catch-up subscriptions, persistent subscriptions, snapshots, stream metadata and projection checkpoints.

It is designed for embedded applications, agents, edge workloads, local development and tests where operating a separate event-store server would be unnecessary.

> **v0.1 — work in progress.** EventsZite is not wire-compatible with EventStoreDB and is not a network drop-in replacement. The current Zig compatibility import remains `@import("eventstoredb")` while the product/repository identity is EventsZite. See [`docs/EVENTSTOREDB-COMPATIBILITY.md`](docs/EVENTSTOREDB-COMPATIBILITY.md).

## Why EventsZite

- SQLite WAL storage with no separate database service.
- Vendored SQLite amalgamation, linked by Zig.
- Expected-revision appends for optimistic concurrency.
- Per-stream revisions plus a global append position.
- Catch-up subscriptions with bounded, configurable buffers.
- Backpressure instead of silent event loss when a catch-up consumer is slow.
- Persistent subscriptions with durable in-flight state and contiguous ACK checkpoints.
- Snapshots, stream metadata and projection checkpoints.
- Forward-only schema migrations.
- Zig 0.16 API and build system.

## Quick start

```zig
const std = @import("std");
const esdb = @import("eventstoredb");

pub fn main() !void {
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const client = try esdb.Client.open(allocator, .{
        .path = ":memory:",
    });
    defer client.close();

    const result = try esdb.appendToStream(
        client,
        allocator,
        "orders-1",
        .{ .expected_revision = .{ .no_stream = {} } },
        &[_]esdb.EventData{
            .{ .event_type = "OrderCreated", .data = "{\"id\":\"1\"}" },
            .{ .event_type = "OrderItemAdded", .data = "{\"sku\":\"A\"}" },
        },
    );
    defer esdb.freeEvents(allocator, result.events);

    const page = try esdb.readStream(
        client,
        allocator,
        "orders-1",
        .{ .from = .{ .start = {} }, .direction = .forward, .limit = 100 },
    );
    defer esdb.freeEvents(allocator, page.events);

    for (page.events) |event| {
        std.debug.print("rev={d} type={s}\n", .{ event.revision, event.event_type });
    }
}
```

More examples live under [`examples/`](examples/).

## Build and test

```bash
zig build
zig build test --summary all -j1
zig build examples
zig build run
```

Useful focused test targets include:

```bash
zig build test-unit-append
zig build test-unit-subscribe
zig build test-unit-persistent
zig build test-stress-separate-read-conn
zig build test-stress-concurrent
zig build test-bench-micro
```

The CI matrix runs on Linux and Windows with Zig 0.16 and also checks formatting with:

```bash
zig fmt --check src/ tests/
```

## Storage model

EventsZite stores the event log in one SQLite database. The current schema version is **3**.

Core tables:

```text
streams
  stream_id
  revision
  metadata / truncation / tombstone state

events
  event_id
  stream_id
  event_number
  log_position
  transaction_position
  event_type
  data / metadata
  sequence / tags / dc_time        # DCB fields

committed_event_ids                # idempotency
persistent_subscriptions           # durable group checkpoint
persistent_acks                    # delivered/in-flight/completed ACK state
snapshots
projection_checkpoints
schema_info
```

Schema migrations are forward-only and idempotent. Version-specific indexes are created only after the columns they depend on have been added, so older database files can migrate through the required versions in order.

SQLite opens with WAL-oriented pragmas including `journal_mode=WAL`, `synchronous=NORMAL` and a configurable `busy_timeout`.

## Append contract

An append accepts an `ExpectedRevision`:

```zig
.no_stream
.stream_exists
.{ .revision = n }
.any
```

The expected-revision check and append are protected by the canonical writer serialization so a competing local writer cannot interleave between validation and commit.

Caller-supplied event IDs provide retry idempotency. If an event ID has already been committed, EventsZite can identify the previous committed event instead of creating a duplicate logical write.

## Read contract

Events can be read by stream or through the global log:

```zig
esdb.readStream(...)
esdb.readAll(...)
```

Stream revision and global log position are different coordinate systems:

- `revision` / `event_number` is local to one stream;
- `log_position` is global to the database.

Persistent subscription checkpoint logic uses stream revision and never substitutes global `log_position` for it.

## Catch-up subscriptions

```zig
var sub = try esdb.subscribeToStream(client, allocator, "orders-1", .{
    .from = .{ .start = {} },
    .poll_interval_ms = 10,
    .buffer_size = 256,
});
defer sub.close();
```

`SubscribeOptions.buffer_size` is enforced per subscription. A value of `0` selects the default capacity of 256 events.

When the bounded queue is full, the producer applies backpressure until capacity is available or the subscription/client closes. Events are not silently discarded because a consumer is slow.

Polling and shutdown waits use scheduler-friendly Zig `std.Io` sleeps for time-based waits. Atomic spin loops are reserved for short lock acquisition rather than representing elapsed time.

## Persistent subscriptions

Persistent subscriptions store delivered/in-flight/completed state in SQLite and maintain a durable contiguous ACK frontier.

The meaning of `persistent_subscriptions.last_position` is:

> the next stream revision that is not durably complete.

This matters when ACKs arrive out of order. For delivered revisions `0, 1, 2`:

```text
ACK 2  -> checkpoint remains 0
ACK 0  -> checkpoint becomes 1
ACK 0  -> checkpoint remains 1 (idempotent duplicate)
ACK 1  -> checkpoint becomes 3
```

An ACK beyond a gap is stored durably but cannot move the checkpoint through an earlier incomplete revision. ACK mutation and checkpoint advancement happen in one SQLite transaction under writer serialization.

Completed ACK rows are retained in v0.1. This preserves duplicate-ACK idempotency and lets restart skip an event that was already completed beyond an earlier gap. Any future compaction mechanism must preserve that observable contract before deleting completion records.

NACK and parked state do not advance the checkpoint implicitly.

A file-backed regression test closes and reopens the database between out-of-order ACKs to verify that both the checkpoint and later completion state survive SQLite reopen.

## Connection and concurrency model

EventsZite v0.1 deliberately does **not** expose a generic connection pool.

A `Client` owns:

```text
1 canonical writer connection
+ optionally 1 dedicated subscription-read connection
```

The dedicated read connection is enabled with:

```zig
.separate_read_connection = true
```

This mode is intended for file-backed databases so WAL readers can operate independently from the writer handle.

### `:memory:` restriction

Plain SQLite `:memory:` databases are connection-local. Two ordinary `:memory:` connections are two different databases. Therefore EventsZite rejects:

```zig
.{
    .path = ":memory:",
    .separate_read_connection = true,
}
```

with `error.UnsupportedConfiguration` rather than silently giving the writer and subscription worker different stores.

If multi-connection in-memory operation is added later, it must use an explicit shared-memory URI contract.

## Public open options

```zig
pub const OpenOptions = struct {
    path: []const u8 = ":memory:",
    busy_timeout_ms: u32 = 5000,
    poll_interval_ms: u32 = 100,
    max_batch_size: u32 = 1024,
    separate_read_connection: bool = false,
};
```

There is intentionally no `max_connections` option in v0.1 because there is no connection pool to enforce such a value.

## Writer synchronization

Same-process writer mutations are serialized by an adaptive lock. It spins only through a short bounded fast path; if contention lasts longer, the waiter yields through `std.Io.sleep` before retrying. This avoids burning a CPU core while the current owner is waiting inside SQLite/WAL I/O.

Cross-process contention remains governed by SQLite and `busy_timeout`.

## Client lifetime

`Client.close()` is destructive. After calling it, the pointer must not be reused.

Shutdown is deterministic:

1. publish `closed`;
2. wake/cancel subscription waiters;
3. wait until registered background workers exit;
4. close the optional read connection;
5. close the writer connection;
6. free client-owned memory.

SQLite/client memory is never intentionally destroyed while a registered worker may still dereference it.

## DCB

The schema includes fields required by the Dynamic Consistency Boundary work (`sequence`, `tags`, `dc_time`). See [`docs/DCB.md`](docs/DCB.md) for the current design and test coverage.

First-class higher-level DCB helpers remain separate from the core append/read API until their semantics are stabilized.

## Compatibility boundary

EventsZite borrows concepts familiar to EventStoreDB users, including streams, expected revisions, positions, subscriptions and persistent consumer groups.

It currently does **not** claim:

- EventStoreDB wire-protocol compatibility;
- gRPC compatibility with official EventStoreDB clients;
- feature parity with the EventStoreDB server;
- transparent replacement of a remote EventStoreDB cluster.

The authoritative compatibility matrix is [`docs/EVENTSTOREDB-COMPATIBILITY.md`](docs/EVENTSTOREDB-COMPATIBILITY.md).

## Repository layout

```text
EventsZite/
├── build.zig
├── build.zig.zon
├── src/
│   ├── root.zig
│   ├── client.zig
│   ├── schema.zig
│   ├── append.zig
│   ├── read.zig
│   ├── subscribe.zig
│   ├── persistent.zig
│   ├── snapshot.zig
│   ├── meta.zig
│   ├── waker.zig
│   └── ...
├── tests/
│   ├── unit/
│   ├── load/
│   ├── stress/
│   ├── chaos/
│   ├── security/
│   ├── bench/
│   └── dcb/
├── examples/
├── docs/
│   ├── ARCHITECTURE.md
│   ├── DCB.md
│   ├── EVENTSTOREDB-COMPATIBILITY.md
│   └── TECHNICAL-REVIEW-v0.1.md
└── vendor/sqlite/
```

## Status

Implemented and exercised by the current suite:

- append with expected revision;
- idempotent event IDs;
- stream and global reads;
- catch-up subscriptions;
- configurable bounded subscription buffers with backpressure;
- persistent subscription create/connect/delete;
- durable contiguous ACK checkpoint semantics;
- duplicate persistent ACK idempotency;
- file-backed persistent checkpoint/reopen regression coverage;
- NACK/park state persistence;
- snapshots;
- stream metadata/tombstones;
- projection checkpoints;
- forward-only schema migrations;
- optional file-backed dedicated read connection;
- DCB schema support;
- Linux and Windows CI configuration.

Still evolving:

- filter subscriptions;
- multi-stream transaction API;
- network/HTTP protocol surface;
- parked-message replay/resolution API;
- first-class DCB read/conditional-append helpers;
- completion-record compaction that preserves idempotency/restart evidence;
- eventual package/import rename from compatibility name `eventstoredb` to `eventszite`.

## License

See [`LICENSE`](LICENSE). SQLite's amalgamation follows SQLite's own public-domain terms; EventsZite source follows the repository license.
