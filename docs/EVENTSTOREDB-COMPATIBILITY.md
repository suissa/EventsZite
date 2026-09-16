# EventStoreDB compatibility

EventsZite implements an EventStoreDB-inspired programming model on top of SQLite. It is **not** a wire-compatible EventStoreDB server and should not be described as a drop-in replacement for an EventStoreDB deployment.

## What is intentionally compatible

The public Zig API models familiar EventStoreDB concepts:

- named streams and per-stream revisions;
- append with expected revision checks;
- global log positions;
- stream and `$all` reads;
- catch-up subscriptions;
- embedded persistent consumer groups;
- stream metadata, snapshots, projection checkpoints and tombstones.

The Zig import name remains `eventstoredb` for source compatibility during the EventsZite rename.

## Persistent subscription semantics implemented in EventsZite

EventsZite persistent subscriptions are an embedded SQLite consumer-group implementation, not an implementation of the EventStoreDB network protocol.

The tested v0.1 contract includes:

- delivery state is persisted before the event is handed to the consumer;
- `persistent_subscriptions.last_position` is the next stream revision that is not durably complete;
- out-of-order ACKs are retained but cannot cross an earlier incomplete revision;
- duplicate ACK of a delivered/completed event is idempotent;
- ACK state and contiguous checkpoint advancement are committed transactionally;
- NACK/park state does not implicitly advance the checkpoint;
- ACKed events beyond an earlier gap remain durable and are skipped after reconnect/reopen;
- file-backed close/reopen regression tests verify the checkpoint and out-of-order ACK state survive SQLite reopen.

Completed ACK rows are retained in v0.1 to preserve duplicate-ACK idempotency and restart evidence. A future compaction mechanism must preserve the same observable contract.

## What is not compatible

EventsZite currently does not implement:

- EventStoreDB gRPC/TCP wire protocols;
- server clustering or quorum replication;
- official EventStoreDB authentication/authorization semantics;
- server-side projections;
- the complete EventStoreDB persistent-subscription protocol and management surface;
- transparent compatibility with official EventStoreDB clients.

## Concurrency model

SQLite remains the durability and serialization boundary. EventsZite uses one writer connection per `Client` and optionally one dedicated subscription-read connection for file-backed databases.

`separate_read_connection = true` is rejected for plain `:memory:` databases because separate SQLite `:memory:` connections are separate databases.

Writer contention uses a bounded adaptive lock: a short spin fast path followed by scheduler-friendly sleep while another writer remains inside SQLite.

## Compatibility policy

Compatibility claims in documentation should use the wording **"EventStoreDB-inspired API/model"** unless a behavior is covered by a regression test. Wire compatibility must never be implied unless an actual EventStoreDB protocol endpoint exists and is verified against official clients.
