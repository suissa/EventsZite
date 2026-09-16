//! Catch-up subscriptions for one stream or the global log.
//! Queues are bounded per subscription and apply producer backpressure;
//! queue overflow never silently discards an event.

const std = @import("std");
const c = @import("c.zig").c;
const types = @import("types.zig");
const errors_mod = @import("errors.zig");
const bind = @import("bind.zig");
const Client = @import("client.zig").Client;
const Waker = @import("waker.zig").Waker;

const default_buffer_size: usize = 256;

const Queue = struct {
    mu: std.atomic.Mutex = .unlocked,
    head: usize = 0,
    tail: usize = 0,
    buf: []types.RecordedEventOrErr,
    closed: bool = false,
    allocator: std.mem.Allocator,

    fn init(allocator: std.mem.Allocator, requested: u32) !Queue {
        const logical_capacity: usize = if (requested == 0) default_buffer_size else @intCast(requested);
        if (logical_capacity == 0 or logical_capacity == std.math.maxInt(usize)) return error.OutOfMemory;
        // Ring buffers reserve one slot to distinguish full from empty.
        const storage = try allocator.alloc(types.RecordedEventOrErr, logical_capacity + 1);
        return .{ .buf = storage, .allocator = allocator };
    }

    fn put(
        self: *Queue,
        item: types.RecordedEventOrErr,
        done: *std.atomic.Value(bool),
        client_closed: *std.atomic.Value(bool),
    ) bool {
        while (true) {
            while (!self.mu.tryLock()) std.atomic.spinLoopHint();

            if (self.closed or done.load(.seq_cst) or client_closed.load(.seq_cst)) {
                self.mu.unlock();
                if (item == .event) freeEvent(self.allocator, &item.event);
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
            // Backpressure: preserve stream continuity rather than dropping.
            sleepMs(1);
        }
    }

    fn get(self: *Queue) ?types.RecordedEventOrErr {
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

    fn tryGet(self: *Queue) ?types.RecordedEventOrErr {
        while (!self.mu.tryLock()) std.atomic.spinLoopHint();
        defer self.mu.unlock();
        if (self.head == self.tail) return null;
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
            if (item == .event) freeEvent(self.allocator, &item.event);
        }
    }

    fn deinit(self: *Queue) void {
        self.drain();
        self.allocator.free(self.buf);
    }
};

fn freeEvent(allocator: std.mem.Allocator, ev: *const types.RecordedEvent) void {
    allocator.free(@constCast(ev.stream_id));
    allocator.free(@constCast(ev.event_type));
    allocator.free(@constCast(ev.data));
    if (ev.metadata) |m| allocator.free(@constCast(m));
}

fn sleepMs(ms: u32) void {
    if (ms == 0) return;
    var threaded: std.Io.Threaded = .init_single_threaded;
    const io = threaded.io();
    io.sleep(.fromMilliseconds(ms), .awake) catch {};
}

const RunContext = struct {
    client: *Client,
    stream_id: []const u8,
    allocator: std.mem.Allocator,
    from_rev: i64,
    from_all: bool,
    from_pos_commit: u64,
    poll_interval_ms: u32,
    queue: *Queue,
    done: *std.atomic.Value(bool),
};

pub fn subscribeToStream(
    self: *Client,
    allocator: std.mem.Allocator,
    stream_id: []const u8,
    opts: types.SubscribeOptions,
) (errors_mod.Error || std.mem.Allocator.Error || std.Thread.SpawnError)!Subscription {
    if (self.closed.load(.seq_cst)) return error.DatabaseClosed;
    if (stream_id.len == 0) return error.InvalidArgument;

    _ = self.active_workers.fetchAdd(1, .seq_cst);
    errdefer _ = self.active_workers.fetchSub(1, .seq_cst);
    if (self.closed.load(.seq_cst)) return error.DatabaseClosed;

    const from_rev: i64 = switch (opts.from) {
        .start, .start_backward => 0,
        .end => blk: {
            const conn = self.conn;
            const stmt = try bind.prepare(conn.db, conn.allocator, "SELECT revision FROM streams WHERE stream_id = ?");
            defer bind.finalize(conn.allocator, stmt);
            _ = bind.bindText(stmt, 1, stream_id);
            const rc = c.sqlite3_step(stmt);
            const v: i64 = if (rc == c.SQLITE_ROW) c.sqlite3_column_int64(stmt, 0) else -1;
            break :blk v + 1;
        },
        .revision => |r| @intCast(r),
        .position => 0,
    };

    const queue = try allocator.create(Queue);
    errdefer allocator.destroy(queue);
    queue.* = try Queue.init(allocator, opts.buffer_size);
    errdefer queue.deinit();

    const done = try allocator.create(std.atomic.Value(bool));
    errdefer allocator.destroy(done);
    done.* = std.atomic.Value(bool).init(false);

    const sub_stream_id = try allocator.dupe(u8, stream_id);
    errdefer allocator.free(sub_stream_id);
    const ctx_stream_id = try allocator.dupe(u8, stream_id);
    errdefer allocator.free(ctx_stream_id);
    const ctx = try allocator.create(RunContext);
    errdefer allocator.destroy(ctx);

    ctx.* = .{
        .client = self,
        .stream_id = ctx_stream_id,
        .allocator = allocator,
        .from_rev = from_rev,
        .from_all = false,
        .from_pos_commit = 0,
        .poll_interval_ms = if (opts.poll_interval_ms == 0) self.poll_interval_ms else opts.poll_interval_ms,
        .queue = queue,
        .done = done,
    };

    const thread = try std.Thread.spawn(.{}, runStream, .{ctx});
    return .{ .queue = queue, .thread = thread, .allocator = allocator, .stream_id = sub_stream_id, .done = done };
}

pub fn subscribeToAll(
    self: *Client,
    allocator: std.mem.Allocator,
    opts: types.SubscribeOptions,
) (errors_mod.Error || std.mem.Allocator.Error || std.Thread.SpawnError)!Subscription {
    if (self.closed.load(.seq_cst)) return error.DatabaseClosed;

    _ = self.active_workers.fetchAdd(1, .seq_cst);
    errdefer _ = self.active_workers.fetchSub(1, .seq_cst);
    if (self.closed.load(.seq_cst)) return error.DatabaseClosed;

    const from_pos_commit: u64 = switch (opts.from) {
        .start, .position, .start_backward => 0,
        .end, .revision => self.lastLogPosition() + 1,
    };

    const queue = try allocator.create(Queue);
    errdefer allocator.destroy(queue);
    queue.* = try Queue.init(allocator, opts.buffer_size);
    errdefer queue.deinit();

    const done = try allocator.create(std.atomic.Value(bool));
    errdefer allocator.destroy(done);
    done.* = std.atomic.Value(bool).init(false);

    const sub_stream_id = try allocator.dupe(u8, "$all");
    errdefer allocator.free(sub_stream_id);
    const ctx_stream_id = try allocator.dupe(u8, "$all");
    errdefer allocator.free(ctx_stream_id);
    const ctx = try allocator.create(RunContext);
    errdefer allocator.destroy(ctx);

    ctx.* = .{
        .client = self,
        .stream_id = ctx_stream_id,
        .allocator = allocator,
        .from_rev = 0,
        .from_all = true,
        .from_pos_commit = from_pos_commit,
        .poll_interval_ms = if (opts.poll_interval_ms == 0) self.poll_interval_ms else opts.poll_interval_ms,
        .queue = queue,
        .done = done,
    };

    const thread = try std.Thread.spawn(.{}, runAll, .{ctx});
    return .{ .queue = queue, .thread = thread, .allocator = allocator, .stream_id = sub_stream_id, .done = done };
}

pub const Subscription = struct {
    queue: *Queue,
    thread: ?std.Thread = null,
    allocator: std.mem.Allocator,
    stream_id: []const u8,
    done: *std.atomic.Value(bool),
    closed: bool = false,

    pub fn receive(self: *Subscription) ?types.RecordedEventOrErr {
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

    pub fn tryReceive(self: *Subscription) ?types.RecordedEventOrErr {
        if (self.closed) return null;
        return self.queue.tryGet();
    }

    pub fn close(self: *Subscription) void {
        if (self.closed) return;
        self.closed = true;
        self.queue.close();
        self.done.store(true, .seq_cst);
        if (self.thread) |t| t.join();
        self.queue.deinit();
        self.allocator.free(self.stream_id);
        self.allocator.destroy(self.queue);
        self.allocator.destroy(self.done);
    }
};

fn runStream(ctx: *RunContext) void {
    var cursor: i64 = ctx.from_rev;
    defer ctx.allocator.destroy(ctx);
    defer ctx.allocator.free(ctx.stream_id);
    defer _ = ctx.client.active_workers.fetchSub(1, .seq_cst);

    const waiter = ctx.client.waker.register() catch {
        ctx.done.store(true, .seq_cst);
        ctx.queue.close();
        return;
    };
    defer ctx.client.waker.unregister(waiter);

    while (!ctx.done.load(.seq_cst) and !ctx.client.closed.load(.seq_cst)) {
        const res = readStreamOnConn(
            ctx.client,
            ctx.allocator,
            ctx.client.read_conn,
            ctx.stream_id,
            .{ .from = .{ .revision = @intCast(@max(0, cursor)) }, .direction = .forward, .limit = ctx.client.max_batch_size },
        ) catch |err| {
            _ = ctx.queue.put(.{ .err = err }, ctx.done, &ctx.client.closed);
            continue;
        };

        const outer = res.events;
        if (outer.len == 0) {
            ctx.allocator.free(outer);
            if (res.is_end_of_stream) _ = Waker.wait(waiter, ctx.poll_interval_ms);
            continue;
        }

        var stopped = false;
        for (outer) |ev| {
            if (stopped) {
                freeEvent(ctx.allocator, &ev);
                continue;
            }
            if (!ctx.queue.put(.{ .event = ev }, ctx.done, &ctx.client.closed)) {
                stopped = true;
                continue;
            }
            cursor = @intCast(ev.revision + 1);
        }
        ctx.allocator.free(outer);

        if (stopped) break;
        if (outer.len >= ctx.client.max_batch_size) continue;
        _ = Waker.wait(waiter, ctx.poll_interval_ms);
    }
}

fn runAll(ctx: *RunContext) void {
    var cursor: u64 = ctx.from_pos_commit;
    defer ctx.allocator.destroy(ctx);
    defer ctx.allocator.free(ctx.stream_id);
    defer _ = ctx.client.active_workers.fetchSub(1, .seq_cst);

    const waiter = ctx.client.waker.register() catch {
        ctx.done.store(true, .seq_cst);
        ctx.queue.close();
        return;
    };
    defer ctx.client.waker.unregister(waiter);

    while (!ctx.done.load(.seq_cst) and !ctx.client.closed.load(.seq_cst)) {
        const res = readAllOnConn(
            ctx.client,
            ctx.allocator,
            ctx.client.read_conn,
            .{ .from = .{ .position = .{ .commit = cursor, .prepare = cursor } }, .direction = .forward, .limit = ctx.client.max_batch_size },
        ) catch |err| {
            _ = ctx.queue.put(.{ .err = err }, ctx.done, &ctx.client.closed);
            continue;
        };

        const outer = res.events;
        if (outer.len == 0) {
            ctx.allocator.free(outer);
            if (res.is_end_of_stream) _ = Waker.wait(waiter, ctx.poll_interval_ms);
            continue;
        }

        var stopped = false;
        for (outer) |ev| {
            if (stopped) {
                freeEvent(ctx.allocator, &ev);
                continue;
            }
            if (!ctx.queue.put(.{ .event = ev }, ctx.done, &ctx.client.closed)) {
                stopped = true;
                continue;
            }
            cursor = ev.log_position + 1;
        }
        ctx.allocator.free(outer);

        if (stopped) break;
        if (outer.len >= ctx.client.max_batch_size) continue;
        _ = Waker.wait(waiter, ctx.poll_interval_ms);
    }
}

const readStreamOnConn = @import("read.zig").readStreamOnConn;
const readAllOnConn = @import("read.zig").readAllOnConn;
