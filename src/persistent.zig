//! Persistent subscriptions with durable in-flight state and a
//! contiguous ACK checkpoint. ACKs may arrive out of order but the
//! durable checkpoint never advances across an incomplete revision.

const std = @import("std");
const c = @import("c.zig").c;
const types = @import("types.zig");
const errors_mod = @import("errors.zig");
const schema_mod = @import("schema.zig");
const bind = @import("bind.zig");
const Client = @import("client.zig").Client;
const readStream = @import("read.zig").readStreamOnConn;

const PersistentMessageOrErr = union(enum) {
    message: types.PersistentMessage,
    err: errors_mod.Error,
    closed: void,
};

const Queue = struct {
    mu: std.atomic.Mutex = .unlocked,
    head: usize = 0,
    tail: usize = 0,
    buf: [4096]PersistentMessageOrErr = undefined,
    closed: bool = false,
    allocator: std.mem.Allocator,

    fn put(
        self: *Queue,
        item: PersistentMessageOrErr,
        done: *std.atomic.Value(bool),
        client_closed: *std.atomic.Value(bool),
    ) bool {
        while (true) {
            while (!self.mu.tryLock()) std.atomic.spinLoopHint();
            if (self.closed or done.load(.seq_cst) or client_closed.load(.seq_cst)) {
                self.mu.unlock();
                freeItem(self.allocator, item);
                return false;
            }
            const next_tail = (self.tail + 1) % self.buf.len;
            if (next_tail != self.head) {
                self.buf[self.tail] = item;
                self.tail = next_tail;
                self.mu.unlock();
                return true;
            }
            self.mu.unlock();
            sleepMs(1);
        }
    }

    fn get(self: *Queue) ?PersistentMessageOrErr {
        while (!self.mu.tryLock()) std.atomic.spinLoopHint();
        defer self.mu.unlock();
        if (self.head == self.tail) {
            if (self.closed) return .{ .closed = {} };
            return null;
        }
        const item = self.buf[self.head];
        self.head = (self.head + 1) % self.buf.len;
        return item;
    }

    fn close(self: *Queue) void {
        while (!self.mu.tryLock()) std.atomic.spinLoopHint();
        defer self.mu.unlock();
        self.closed = true;
    }

    fn drain(self: *Queue) void {
        while (!self.mu.tryLock()) std.atomic.spinLoopHint();
        defer self.mu.unlock();
        while (self.head != self.tail) {
            const item = self.buf[self.head];
            self.head = (self.head + 1) % self.buf.len;
            freeItem(self.allocator, item);
        }
    }
};

fn freeItem(allocator: std.mem.Allocator, item: PersistentMessageOrErr) void {
    if (item == .message) types.freeEvent(allocator, item.message.event);
}

fn sleepMs(ms: u32) void {
    if (ms == 0) return;
    var threaded: std.Io.Threaded = .init_single_threaded;
    const io = threaded.io();
    io.sleep(.fromMilliseconds(ms), .awake) catch {};
}

pub fn createPersistentSubscription(
    self: *Client,
    allocator: std.mem.Allocator,
    stream_id: []const u8,
    group_name: []const u8,
    opts: types.PersistentOptions,
    cfg: types.PersistentConfig,
    overwrite: bool,
) (errors_mod.Error || std.mem.Allocator.Error)!void {
    if (self.closed.load(.seq_cst)) return error.DatabaseClosed;
    if (stream_id.len == 0 or group_name.len == 0) return error.InvalidArgument;

    self.writer_mu.lock();
    defer self.writer_mu.unlock();

    if (!overwrite) {
        const exists = bind.prepare(self.conn.db, self.conn.allocator, "SELECT 1 FROM persistent_subscriptions WHERE group_name = ? AND stream_id = ?") catch return error.Sqlite;
        defer bind.finalize(self.conn.allocator, exists);
        _ = bind.bindText(exists, 1, group_name);
        _ = bind.bindText(exists, 2, stream_id);
        if (c.sqlite3_step(exists) == c.SQLITE_ROW) return error.PersistentSubscriptionExists;
    }

    const from_rev: i64 = switch (opts.from) {
        .start, .start_backward => 0,
        .end => blk: {
            const stmt = bind.prepare(self.conn.db, self.conn.allocator, "SELECT COALESCE(revision + 1, 0) FROM streams WHERE stream_id = ?") catch return error.Sqlite;
            defer bind.finalize(self.conn.allocator, stmt);
            _ = bind.bindText(stmt, 1, stream_id);
            break :blk if (c.sqlite3_step(stmt) == c.SQLITE_ROW) c.sqlite3_column_int64(stmt, 0) else 0;
        },
        .revision => |r| @intCast(r),
        .position => return error.InvalidArgument,
    };

    const cfg_buf = stringifyConfig(allocator, cfg) catch return error.Sqlite;
    defer allocator.free(cfg_buf);

    const sql =
        \\INSERT INTO persistent_subscriptions(group_name, stream_id, start_from, last_position, revision, config, status, created_at, updated_at)
        \\VALUES (?, ?, ?, ?, 1, ?, 'Live', ?, ?)
        \\ON CONFLICT(group_name, stream_id) DO UPDATE SET
        \\  start_from = excluded.start_from,
        \\  last_position = excluded.last_position,
        \\  config = excluded.config,
        \\  status = 'Live',
        \\  updated_at = excluded.updated_at
    ;
    const stmt = try bind.prepare(self.conn.db, self.conn.allocator, sql);
    defer bind.finalize(self.conn.allocator, stmt);
    _ = bind.bindText(stmt, 1, group_name);
    _ = bind.bindText(stmt, 2, stream_id);
    _ = bind.bindI64(stmt, 3, from_rev);
    _ = bind.bindI64(stmt, 4, from_rev);
    _ = bind.bindBlob(stmt, 5, cfg_buf);
    _ = bind.bindI64(stmt, 6, schema_mod.nowMs());
    _ = bind.bindI64(stmt, 7, schema_mod.nowMs());
    if (c.sqlite3_step(stmt) != c.SQLITE_DONE) return error.Sqlite;
}

pub fn deletePersistentSubscription(self: *Client, group_name: []const u8, stream_id: []const u8) errors_mod.Error!void {
    if (self.closed.load(.seq_cst)) return error.DatabaseClosed;
    if (group_name.len == 0 or stream_id.len == 0) return error.InvalidArgument;

    self.writer_mu.lock();
    defer self.writer_mu.unlock();

    try self.conn.exec("BEGIN IMMEDIATE");
    var tx_open = true;
    defer if (tx_open) self.conn.exec("ROLLBACK") catch {};

    const s1 = try bind.prepare(self.conn.db, self.conn.allocator, "DELETE FROM persistent_acks WHERE group_name = ? AND stream_id = ?");
    defer bind.finalize(self.conn.allocator, s1);
    _ = bind.bindText(s1, 1, group_name);
    _ = bind.bindText(s1, 2, stream_id);
    if (c.sqlite3_step(s1) != c.SQLITE_DONE) return error.Sqlite;

    const s2 = try bind.prepare(self.conn.db, self.conn.allocator, "DELETE FROM persistent_subscriptions WHERE group_name = ? AND stream_id = ?");
    defer bind.finalize(self.conn.allocator, s2);
    _ = bind.bindText(s2, 1, group_name);
    _ = bind.bindText(s2, 2, stream_id);
    if (c.sqlite3_step(s2) != c.SQLITE_DONE) return error.Sqlite;

    try self.conn.exec("COMMIT");
    tx_open = false;
}

const PSRunContext = struct {
    client: *Client,
    allocator: std.mem.Allocator,
    stream_id: []const u8,
    group_name: []const u8,
    queue: *Queue,
    done: *std.atomic.Value(bool),
    last_position: u64,
};

pub fn connectPersistentSubscription(
    self: *Client,
    allocator: std.mem.Allocator,
    stream_id: []const u8,
    group_name: []const u8,
) (errors_mod.Error || std.mem.Allocator.Error || std.Thread.SpawnError)!PersistentSubscription {
    if (self.closed.load(.seq_cst)) return error.DatabaseClosed;
    if (stream_id.len == 0 or group_name.len == 0) return error.InvalidArgument;

    _ = self.active_workers.fetchAdd(1, .seq_cst);
    errdefer _ = self.active_workers.fetchSub(1, .seq_cst);
    if (self.closed.load(.seq_cst)) return error.DatabaseClosed;

    const stmt = try bind.prepare(self.conn.db, self.conn.allocator, "SELECT last_position FROM persistent_subscriptions WHERE group_name = ? AND stream_id = ?");
    defer bind.finalize(self.conn.allocator, stmt);
    _ = bind.bindText(stmt, 1, group_name);
    _ = bind.bindText(stmt, 2, stream_id);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.PersistentSubscriptionNotFound;
    const last_pos = c.sqlite3_column_int64(stmt, 0);

    const queue = try allocator.create(Queue);
    errdefer allocator.destroy(queue);
    queue.* = .{ .allocator = allocator };
    const done = try allocator.create(std.atomic.Value(bool));
    errdefer allocator.destroy(done);
    done.* = std.atomic.Value(bool).init(false);

    const sub_stream_id = try allocator.dupe(u8, stream_id);
    errdefer allocator.free(sub_stream_id);
    const sub_group_name = try allocator.dupe(u8, group_name);
    errdefer allocator.free(sub_group_name);

    const ctx = try allocator.create(PSRunContext);
    errdefer allocator.destroy(ctx);
    const ctx_stream_id = try allocator.dupe(u8, stream_id);
    errdefer allocator.free(ctx_stream_id);
    const ctx_group_name = try allocator.dupe(u8, group_name);
    errdefer allocator.free(ctx_group_name);

    ctx.* = .{
        .client = self,
        .allocator = allocator,
        .stream_id = ctx_stream_id,
        .group_name = ctx_group_name,
        .queue = queue,
        .done = done,
        .last_position = @intCast(last_pos),
    };

    const thread = try std.Thread.spawn(.{}, runPS, .{ctx});
    return .{
        .client = self,
        .allocator = allocator,
        .stream_id = sub_stream_id,
        .group_name = sub_group_name,
        .queue = queue,
        .done = done,
        .thread = thread,
        .last_position = @intCast(last_pos),
    };
}

pub const PersistentSubscription = struct {
    client: *Client,
    allocator: std.mem.Allocator,
    stream_id: []const u8,
    group_name: []const u8,
    queue: *Queue,
    done: *std.atomic.Value(bool),
    thread: ?std.Thread = null,
    last_position: u64,
    closed: bool = false,

    pub fn receive(self: *PersistentSubscription) ?PersistentMessageOrErr {
        while (true) {
            if (self.closed) return null;
            if (self.queue.get()) |item| {
                if (item == .closed) {
                    self.closed = true;
                    return null;
                }
                return item;
            }
            if (self.done.load(.seq_cst)) return null;
            sleepMs(1);
        }
    }

    pub fn tryReceive(self: *PersistentSubscription) ?PersistentMessageOrErr {
        if (self.closed) return null;
        return self.queue.get();
    }

    /// Mark one delivered event complete and advance the durable checkpoint
    /// only through the highest contiguous completed revision.
    ///
    /// Completed rows are retained in v0.1 so a duplicate ACK remains
    /// idempotent even after the durable frontier has advanced past it.
    pub fn ack(self: *PersistentSubscription, ev_id: types.Uuid) errors_mod.Error!void {
        if (self.closed) return error.SubscriptionClosed;
        if (self.client.closed.load(.seq_cst)) return error.DatabaseClosed;

        self.client.writer_mu.lock();
        defer self.client.writer_mu.unlock();
        const conn = self.client.conn;

        try conn.exec("BEGIN IMMEDIATE");
        var tx_open = true;
        defer if (tx_open) conn.exec("ROLLBACK") catch {};

        _ = try findInflightEventNumber(conn, self.group_name, self.stream_id, ev_id);

        const mark = try bind.prepare(conn.db, conn.allocator, "UPDATE persistent_acks SET acked = 1 WHERE group_name = ? AND stream_id = ? AND event_id = ?");
        defer bind.finalize(conn.allocator, mark);
        _ = bind.bindText(mark, 1, self.group_name);
        _ = bind.bindText(mark, 2, self.stream_id);
        _ = bind.bindBlob(mark, 3, &ev_id);
        if (c.sqlite3_step(mark) != c.SQLITE_DONE) return error.Sqlite;

        const checkpoint = try readGroupCheckpoint(conn, self.group_name, self.stream_id);
        const first_incomplete = try queryFrontierScalar(
            conn,
            "SELECT MIN(event_number) FROM persistent_acks WHERE group_name = ? AND stream_id = ? AND event_number >= ? AND acked = 0",
            self.group_name,
            self.stream_id,
            checkpoint,
        );

        var frontier: i64 = checkpoint;
        if (first_incomplete) |gap| {
            frontier = gap;
        } else if (try queryFrontierScalar(
            conn,
            "SELECT MAX(event_number) FROM persistent_acks WHERE group_name = ? AND stream_id = ? AND event_number >= ?",
            self.group_name,
            self.stream_id,
            checkpoint,
        )) |max_tracked| {
            frontier = max_tracked + 1;
        }

        const update = try bind.prepare(conn.db, conn.allocator, "UPDATE persistent_subscriptions SET last_position = ?, updated_at = ? WHERE group_name = ? AND stream_id = ?");
        defer bind.finalize(conn.allocator, update);
        _ = bind.bindI64(update, 1, frontier);
        _ = bind.bindI64(update, 2, schema_mod.nowMs());
        _ = bind.bindText(update, 3, self.group_name);
        _ = bind.bindText(update, 4, self.stream_id);
        if (c.sqlite3_step(update) != c.SQLITE_DONE) return error.Sqlite;

        try conn.exec("COMMIT");
        tx_open = false;
        self.last_position = @intCast(frontier);
    }

    pub fn nack(self: *PersistentSubscription, ev_id: types.Uuid, park: bool) errors_mod.Error!void {
        if (self.closed) return error.SubscriptionClosed;
        if (self.client.closed.load(.seq_cst)) return error.DatabaseClosed;
        self.client.writer_mu.lock();
        defer self.client.writer_mu.unlock();

        const stmt = try bind.prepare(self.client.conn.db, self.client.conn.allocator, "UPDATE persistent_acks SET retry_count = retry_count + 1, parked = ?, acked = 0 WHERE group_name = ? AND stream_id = ? AND event_id = ?");
        defer bind.finalize(self.client.conn.allocator, stmt);
        _ = bind.bindI64(stmt, 1, if (park) 1 else 0);
        _ = bind.bindText(stmt, 2, self.group_name);
        _ = bind.bindText(stmt, 3, self.stream_id);
        _ = bind.bindBlob(stmt, 4, &ev_id);
        if (c.sqlite3_step(stmt) != c.SQLITE_DONE) return error.Sqlite;
    }

    pub fn close(self: *PersistentSubscription) void {
        if (self.closed) return;
        self.closed = true;
        self.queue.close();
        self.done.store(true, .seq_cst);
        if (self.thread) |t| t.join();
        self.queue.drain();
        self.allocator.free(self.stream_id);
        self.allocator.free(self.group_name);
        self.allocator.destroy(self.queue);
        self.allocator.destroy(self.done);
    }
};

const InflightState = struct {
    acked: bool,
    parked: bool,
    retry_count: u32,
};

fn findInflightEventNumber(conn: *schema_mod.Connection, group_name: []const u8, stream_id: []const u8, ev_id: types.Uuid) errors_mod.Error!i64 {
    const stmt = try bind.prepare(conn.db, conn.allocator, "SELECT event_number FROM persistent_acks WHERE group_name = ? AND stream_id = ? AND event_id = ?");
    defer bind.finalize(conn.allocator, stmt);
    _ = bind.bindText(stmt, 1, group_name);
    _ = bind.bindText(stmt, 2, stream_id);
    _ = bind.bindBlob(stmt, 3, &ev_id);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.NotFound;
    if (c.sqlite3_column_type(stmt, 0) == c.SQLITE_NULL) return error.Sqlite;
    return c.sqlite3_column_int64(stmt, 0);
}

fn readGroupCheckpoint(conn: *schema_mod.Connection, group_name: []const u8, stream_id: []const u8) errors_mod.Error!i64 {
    const stmt = try bind.prepare(conn.db, conn.allocator, "SELECT last_position FROM persistent_subscriptions WHERE group_name = ? AND stream_id = ?");
    defer bind.finalize(conn.allocator, stmt);
    _ = bind.bindText(stmt, 1, group_name);
    _ = bind.bindText(stmt, 2, stream_id);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.PersistentSubscriptionNotFound;
    return c.sqlite3_column_int64(stmt, 0);
}

fn queryFrontierScalar(
    conn: *schema_mod.Connection,
    sql: []const u8,
    group_name: []const u8,
    stream_id: []const u8,
    checkpoint: i64,
) errors_mod.Error!?i64 {
    const stmt = try bind.prepare(conn.db, conn.allocator, sql);
    defer bind.finalize(conn.allocator, stmt);
    _ = bind.bindText(stmt, 1, group_name);
    _ = bind.bindText(stmt, 2, stream_id);
    _ = bind.bindI64(stmt, 3, checkpoint);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.Sqlite;
    if (c.sqlite3_column_type(stmt, 0) == c.SQLITE_NULL) return null;
    return c.sqlite3_column_int64(stmt, 0);
}

fn readInflightState(conn: *schema_mod.Connection, group_name: []const u8, stream_id: []const u8, ev_id: types.Uuid) errors_mod.Error!?InflightState {
    const stmt = try bind.prepare(conn.db, conn.allocator, "SELECT acked, parked, retry_count FROM persistent_acks WHERE group_name = ? AND stream_id = ? AND event_id = ?");
    defer bind.finalize(conn.allocator, stmt);
    _ = bind.bindText(stmt, 1, group_name);
    _ = bind.bindText(stmt, 2, stream_id);
    _ = bind.bindBlob(stmt, 3, &ev_id);
    const rc = c.sqlite3_step(stmt);
    if (rc == c.SQLITE_DONE) return null;
    if (rc != c.SQLITE_ROW) return error.Sqlite;
    return .{
        .acked = c.sqlite3_column_int64(stmt, 0) != 0,
        .parked = c.sqlite3_column_int64(stmt, 1) != 0,
        .retry_count = @intCast(c.sqlite3_column_int64(stmt, 2)),
    };
}

fn runPS(ctx: *PSRunContext) void {
    defer ctx.allocator.destroy(ctx);
    defer ctx.allocator.free(ctx.group_name);
    defer ctx.allocator.free(ctx.stream_id);
    defer _ = ctx.client.active_workers.fetchSub(1, .seq_cst);

    var cursor: u64 = ctx.last_position;
    while (!ctx.done.load(.seq_cst) and !ctx.client.closed.load(.seq_cst)) {
        const res = readStream(
            ctx.client,
            ctx.allocator,
            ctx.client.read_conn,
            ctx.stream_id,
            .{ .from = .{ .revision = cursor }, .direction = .forward, .limit = ctx.client.max_batch_size },
        ) catch |err| {
            if (ctx.client.closed.load(.seq_cst)) break;
            _ = ctx.queue.put(.{ .err = err }, ctx.done, &ctx.client.closed);
            sleepMs(ctx.client.poll_interval_ms);
            continue;
        };

        const outer = res.events;
        if (outer.len == 0) {
            ctx.allocator.free(outer);
            sleepMs(ctx.client.poll_interval_ms);
            continue;
        }

        var stopped = false;
        for (outer) |ev| {
            if (stopped or ctx.done.load(.seq_cst) or ctx.client.closed.load(.seq_cst)) {
                types.freeEvent(ctx.allocator, ev);
                continue;
            }

            ctx.client.writer_mu.lock();
            const existing = readInflightState(ctx.client.conn, ctx.group_name, ctx.stream_id, ev.event_id) catch {
                ctx.client.writer_mu.unlock();
                types.freeEvent(ctx.allocator, ev);
                _ = ctx.queue.put(.{ .err = error.Sqlite }, ctx.done, &ctx.client.closed);
                continue;
            };

            if (existing) |state| {
                ctx.client.writer_mu.unlock();
                // Completed events beyond an earlier gap remain recorded so
                // replay can skip them and duplicate ACKs stay idempotent.
                if (state.acked or state.parked) {
                    cursor = ev.revision + 1;
                    types.freeEvent(ctx.allocator, ev);
                    continue;
                }
                if (!ctx.queue.put(.{ .message = .{ .event = ev, .retry_count = state.retry_count } }, ctx.done, &ctx.client.closed)) {
                    stopped = true;
                    continue;
                }
                cursor = ev.revision + 1;
                continue;
            }

            const stmt = bind.prepare(
                ctx.client.conn.db,
                ctx.client.conn.allocator,
                "INSERT INTO persistent_acks(group_name, stream_id, event_id, event_number, log_position, retry_count, parked, acked, enqueued_at) VALUES (?, ?, ?, ?, ?, 0, 0, 0, ?)",
            ) catch {
                ctx.client.writer_mu.unlock();
                types.freeEvent(ctx.allocator, ev);
                _ = ctx.queue.put(.{ .err = error.Sqlite }, ctx.done, &ctx.client.closed);
                continue;
            };
            _ = bind.bindText(stmt, 1, ctx.group_name);
            _ = bind.bindText(stmt, 2, ctx.stream_id);
            _ = bind.bindBlob(stmt, 3, &ev.event_id);
            _ = bind.bindI64(stmt, 4, @intCast(ev.revision));
            _ = bind.bindI64(stmt, 5, @intCast(ev.log_position));
            _ = bind.bindI64(stmt, 6, schema_mod.nowMs());
            const rc = c.sqlite3_step(stmt);
            bind.finalize(ctx.client.conn.allocator, stmt);
            ctx.client.writer_mu.unlock();

            if (rc != c.SQLITE_DONE) {
                types.freeEvent(ctx.allocator, ev);
                _ = ctx.queue.put(.{ .err = error.Sqlite }, ctx.done, &ctx.client.closed);
                continue;
            }

            if (!ctx.queue.put(.{ .message = .{ .event = ev, .retry_count = 0 } }, ctx.done, &ctx.client.closed)) {
                stopped = true;
                continue;
            }
            cursor = ev.revision + 1;
        }
        ctx.allocator.free(outer);
        if (stopped) break;
    }

    ctx.queue.close();
}

fn stringifyConfig(allocator: std.mem.Allocator, cfg: types.PersistentConfig) ![]u8 {
    var buf: [256]u8 = undefined;
    const slice = try std.fmt.bufPrint(&buf,
        \\{{"resolve_link_tos":{},"extra_statistics":{},"max_retry_count":{d},"check_point_after":{d},"min_check_point_count":{d},"max_check_point_count":{d},"live_buffer_size":{d},"read_batch_size":{d}}}
    , .{
        cfg.resolve_link_tos,
        cfg.extra_statistics,
        cfg.max_retry_count,
        cfg.check_point_after,
        cfg.min_check_point_count,
        cfg.max_check_point_count,
        cfg.live_buffer_size,
        cfg.read_batch_size,
    });
    return allocator.dupe(u8, slice);
}
