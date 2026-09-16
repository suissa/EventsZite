//! Public types: events, positions, expected revisions, options, etc.
//!
//! All types are allocator-aware where appropriate. The Client
//! is not (it owns a `*sqlite3` connection and is created with
//! `Client.open`). Owned slices are documented with their owner.

const std = @import("std");

pub const Uuid = [16]u8;

pub const EventData = struct {
    event_id: Uuid = .{0} ** 16,
    event_type: []const u8,
    data: []const u8,
    metadata: ?[]const u8 = null,
};

pub const RecordedEvent = struct {
    event_id: Uuid,
    stream_id: []const u8,
    event_type: []const u8,
    data: []const u8,
    metadata: ?[]const u8,
    revision: u64,
    log_position: u64,
    transaction_position: u64,
};

pub const AppendResult = struct {
    next_revision: u64,
    log_position: u64,
    transaction_position: u64,
    events: []const RecordedEvent,
};

pub const Position = struct {
    commit: u64 = 0,
    prepare: u64 = 0,

    pub fn isZero(self: Position) bool {
        return self.commit == 0 and self.prepare == 0;
    }

    pub const start_of_log: Position = .{};
    pub const end_of_log: Position = .{ .commit = std.math.maxInt(u64), .prepare = std.math.maxInt(u64) };
};

pub const ReadResult = struct {
    events: []const RecordedEvent,
    next_revision: u64,
    next_position: Position,
    is_end_of_stream: bool,
};

pub const ReadDirection = enum { forward, backward };

pub const From = union(enum) {
    start: void,
    start_backward: void,
    end: void,
    revision: u64,
    position: Position,
};

pub const ExpectedRevision = union(enum) {
    no_stream: void,
    stream_exists: void,
    revision: u64,
    any: void,
};

pub const AppendOptions = struct {
    expected_revision: ExpectedRevision = .{ .any = {} },
};

pub const ReadOptions = struct {
    from: From = .{ .start = {} },
    direction: ReadDirection = .forward,
    limit: u32 = 0,
};

pub const SubscribeOptions = struct {
    from: From = .{ .start = {} },
    poll_interval_ms: u32 = 0,
    /// Number of events that may be pending for the consumer.
    /// Zero selects the default capacity of 256.
    buffer_size: u32 = 0,
};

pub const PersistentOptions = struct {
    group_name: []const u8,
    from: From = .{ .start = {} },
    max_retries: u32 = 10,
    ack_timeout_ms: u32 = 30_000,
};

pub const PersistentConfig = struct {
    resolve_link_tos: bool = false,
    extra_statistics: bool = false,
    max_retry_count: u32 = 0,
    check_point_after: u32 = 0,
    min_check_point_count: u32 = 0,
    max_check_point_count: u32 = 0,
    live_buffer_size: u32 = 0,
    read_batch_size: u32 = 0,
};

pub const PersistentMessage = struct {
    event: RecordedEvent,
    retry_count: u32 = 0,
};

pub const StreamMetadata = struct {
    max_count: i64 = 0,
    truncate_before: u64 = 0,
    custom_metadata: ?[]const u8 = null,
};

pub const StreamInfo = struct {
    stream_id: []const u8,
    revision: u64,
    max_count: i64,
    truncate_before: u64,
    deleted: bool,
};

pub const Snapshot = struct {
    stream_id: []const u8,
    revision: u64,
    payload: []const u8,
    metadata: ?[]const u8,
};

pub const ProjectionState = struct {
    name: []const u8,
    last_position: u64,
    state: ?[]const u8,
};

pub const Stats = struct {
    stream_count: i64,
    event_count: i64,
    tombstoned_streams: i64,
    persistent_groups: i64,
    snapshots: i64,
    db_size_bytes: i64,
    last_log_position: u64,
};

pub const OpenOptions = struct {
    /// Database path; `:memory:` for a connection-local in-memory store.
    path: []const u8 = ":memory:",
    busy_timeout_ms: u32 = 5000,
    poll_interval_ms: u32 = 100,
    max_batch_size: u32 = 1024,

    /// Open one additional SQLite handle for subscription reads.
    /// This is supported only for stores where a second handle addresses
    /// the same physical database. Plain `:memory:` is rejected because
    /// each connection would otherwise receive an independent database.
    separate_read_connection: bool = false,
};

pub const RecordedEventOrErr = union(enum) {
    event: RecordedEvent,
    err: @import("errors.zig").Error,
    closed: void,
};

pub fn freeEvent(allocator: std.mem.Allocator, event: RecordedEvent) void {
    allocator.free(event.stream_id);
    allocator.free(event.event_type);
    allocator.free(event.data);
    if (event.metadata) |m| allocator.free(m);
}

pub fn freeEvents(allocator: std.mem.Allocator, events: []const RecordedEvent) void {
    for (events) |e| {
        allocator.free(e.stream_id);
        allocator.free(e.event_type);
        allocator.free(e.data);
        if (e.metadata) |m| allocator.free(m);
    }
    allocator.free(events);
}

pub fn freeSnapshots(allocator: std.mem.Allocator, snaps: []const Snapshot) void {
    for (snaps) |s| {
        allocator.free(s.stream_id);
        allocator.free(s.payload);
        if (s.metadata) |m| allocator.free(m);
    }
    allocator.free(snaps);
}

pub fn freeStreamInfo(allocator: std.mem.Allocator, infos: []const StreamInfo) void {
    for (infos) |i| allocator.free(i.stream_id);
    allocator.free(infos);
}

pub fn freeProjectionState(allocator: std.mem.Allocator, p: ProjectionState) void {
    allocator.free(p.name);
    if (p.state) |s| allocator.free(s);
}
