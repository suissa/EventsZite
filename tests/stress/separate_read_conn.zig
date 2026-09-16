//! Stress/regression coverage for the optional dedicated SQLite read
//! connection. The writer and subscription worker must observe one
//! file-backed WAL database and deliver a contiguous global log.

const std = @import("std");
const testing = std.testing;
const esdb = @import("eventstoredb");

const event_count: u32 = 64;

test "stress: separate read connection observes every appended event" {
    const a = testing.allocator;

    var threaded: std.Io.Threaded = .init_single_threaded;
    const io = threaded.io();
    const stamp = std.Io.Clock.now(.real, io).nanoseconds;

    var path_buf: [128]u8 = undefined;
    var wal_buf: [140]u8 = undefined;
    var shm_buf: [140]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "stress-separate-read-{x}.db", .{@as(u64, @intCast(stamp))});
    const wal_path = try std.fmt.bufPrint(&wal_buf, "{s}-wal", .{path});
    const shm_path = try std.fmt.bufPrint(&shm_buf, "{s}-shm", .{path});

    defer std.Io.Dir.cwd().deleteFile(io, shm_path) catch {};
    defer std.Io.Dir.cwd().deleteFile(io, wal_path) catch {};
    defer std.Io.Dir.cwd().deleteFile(io, path) catch {};

    const client = try esdb.Client.open(a, .{
        .path = path,
        .separate_read_connection = true,
        .poll_interval_ms = 2,
        .max_batch_size = 16,
    });
    defer client.close();

    var sub = try esdb.subscribeToAll(client, a, .{
        .from = .{ .start = {} },
        .poll_interval_ms = 2,
        .buffer_size = 8,
    });
    defer sub.close();

    var i: u32 = 0;
    while (i < event_count) : (i += 1) {
        const result = try esdb.appendToStream(
            client,
            a,
            "separate-read-stream",
            .{ .expected_revision = .{ .any = {} } },
            &[_]esdb.EventData{.{ .event_type = "Tick", .data = "{}" }},
        );
        esdb.freeEvents(a, result.events);
    }

    var expected_position: u64 = 1;
    const deadline = std.Io.Clock.now(.boot, io).nanoseconds + 5 * std.time.ns_per_s;
    while (expected_position <= event_count and std.Io.Clock.now(.boot, io).nanoseconds < deadline) {
        if (sub.tryReceive()) |item| {
            switch (item) {
                .event => |ev| {
                    try testing.expectEqual(expected_position, ev.log_position);
                    expected_position += 1;
                    esdb.freeEvent(a, ev);
                },
                .err => return item.err,
                .closed => break,
            }
        } else {
            io.sleep(.fromMilliseconds(1), .awake) catch {};
        }
    }

    try testing.expectEqual(@as(u64, event_count + 1), expected_position);
    try testing.expectEqual(@as(u64, event_count), client.lastLogPosition());
}
