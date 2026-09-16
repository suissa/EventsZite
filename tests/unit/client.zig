//! Unit tests for the `Client` lifecycle and connection policy.

const std = @import("std");
const testing = std.testing;
const esdb = @import("eventstoredb");
const common = @import("common.zig");

test "client: open and close a fresh in-memory store" {
    const c = try common.newClient(testing.allocator);
    defer c.close();
    try testing.expectEqual(@as(u64, 0), c.lastLogPosition());
}

test "client: open applies pragmas (busy_timeout)" {
    const c = try esdb.Client.open(common.conn_alloc, .{ .path = ":memory:", .busy_timeout_ms = 1234 });
    defer c.close();
    try testing.expectEqual(@as(u64, 0), c.lastLogPosition());
}

test "client: lastLogPosition tracks the global log" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    try testing.expectEqual(@as(u64, 0), c.lastLogPosition());
    const res = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = "1" },
        .{ .event_type = "X", .data = "2" },
    });
    defer esdb.freeEvents(a, res.events);
    try testing.expectEqual(@as(u64, 2), c.lastLogPosition());
}

test "client: stats reflect the contents" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    const r1 = try esdb.appendToStream(c, a, "s1", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = "1" },
    });
    defer esdb.freeEvents(a, r1.events);

    const r2 = try esdb.appendToStream(c, a, "s2", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "Y", .data = "1" },
        .{ .event_type = "Y", .data = "2" },
    });
    defer esdb.freeEvents(a, r2.events);

    const stats = try c.stats();
    try testing.expectEqual(@as(i64, 2), stats.stream_count);
    try testing.expectEqual(@as(i64, 3), stats.event_count);
    try testing.expectEqual(@as(i64, 0), stats.tombstoned_streams);
    try testing.expect(stats.db_size_bytes >= 0);
    try testing.expectEqual(@as(u64, 3), stats.last_log_position);
}

test "client: close is destructive" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    const r = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = "1" },
    });
    defer esdb.freeEvents(a, r.events);
    c.close();
}

test "client: reopening on a stale path returns CannotOpenDatabase" {
    try testing.expectError(error.CannotOpenDatabase, esdb.Client.open(common.conn_alloc, .{
        .path = "/this/dir/really/does/not/exist/store.db",
    }));
}

test "client: plain memory store rejects separate read connection" {
    try testing.expectError(error.UnsupportedConfiguration, esdb.Client.open(common.conn_alloc, .{
        .path = ":memory:",
        .separate_read_connection = true,
    }));
}

test "client: file store supports separate read connection" {
    var threaded: std.Io.Threaded = .init_single_threaded;
    const io = threaded.io();
    const ts = std.Io.Clock.now(.real, io);

    var path_buf: [128]u8 = undefined;
    var wal_buf: [140]u8 = undefined;
    var shm_buf: [140]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "client-separate-read-{x}.db", .{@as(u64, @intCast(ts.nanoseconds))});
    const wal_path = try std.fmt.bufPrint(&wal_buf, "{s}-wal", .{path});
    const shm_path = try std.fmt.bufPrint(&shm_buf, "{s}-shm", .{path});

    defer std.Io.Dir.cwd().deleteFile(io, shm_path) catch {};
    defer std.Io.Dir.cwd().deleteFile(io, wal_path) catch {};
    defer std.Io.Dir.cwd().deleteFile(io, path) catch {};

    const c = try esdb.Client.open(common.conn_alloc, .{
        .path = path,
        .separate_read_connection = true,
    });
    const a = testing.allocator;
    const r = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = "1" },
    });
    defer esdb.freeEvents(a, r.events);

    var sub = try esdb.subscribeToAll(c, a, .{ .from = .{ .start = {} }, .poll_interval_ms = 5 });
    defer sub.close();

    const deadline: i128 = common.nowNs() + std.time.ns_per_s;
    var seen = false;
    while (!seen and common.nowNs() < deadline) {
        if (sub.tryReceive()) |item| {
            switch (item) {
                .event => |ev| {
                    seen = true;
                    esdb.freeEvent(a, ev);
                },
                .err => return item.err,
                .closed => break,
            }
        } else std.atomic.spinLoopHint();
    }
    try testing.expect(seen);
}
