//! The `Client` type — top-level handle to a single event store
//! database. It owns the SQLite connection, the writer mutex,
//! the in-memory last-log-position cache, and the broadcast
//! waker that wakes catch-up subscribers after an append.

const std = @import("std");
const types = @import("types.zig");
const errors_mod = @import("errors.zig");
const schema_mod = @import("schema.zig");
const Waker = @import("waker.zig").Waker;
const spinlock = @import("spinlock.zig");

pub const Client = struct {
    conn: *schema_mod.Connection,
    allocator: std.mem.Allocator,

    /// Connection used by subscription workers (catch-up
    /// `runStream`/`runAll` and persistent `runPS`).
    ///
    /// By default this is the same pointer as `conn`. When
    /// `OpenOptions.separate_read_connection` is true, file-backed
    /// stores use a second connection so WAL readers do not share the
    /// writer handle. Plain `:memory:` is rejected with this option,
    /// because two SQLite `:memory:` handles are two different stores.
    read_conn: *schema_mod.Connection,
    read_conn_owned: bool = false,

    /// SQLite is single-writer per file. Serialize writes inside the
    /// process so expected-revision checks and their append commit are
    /// observed as one writer-critical section.
    writer_mu: spinlock.Spinlock = .{},

    last_log_pos: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    waker: Waker,
    closed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    active_workers: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    path: []const u8,
    poll_interval_ms: u32,
    max_batch_size: u32,

    pub fn open(allocator: std.mem.Allocator, opts: types.OpenOptions) errors_mod.Error!*Client {
        // A normal SQLite `:memory:` database belongs to one connection.
        // Opening a second handle would create an independent database and
        // make subscription workers appear to lose every appended event.
        if (opts.separate_read_connection and std.mem.eql(u8, opts.path, ":memory:")) {
            return error.UnsupportedConfiguration;
        }

        const conn = try schema_mod.Connection.open(allocator, opts.path, opts.busy_timeout_ms);
        errdefer conn.close();

        const client = try allocator.create(Client);
        errdefer allocator.destroy(client);

        const path_copy = try allocator.dupe(u8, opts.path);
        errdefer allocator.free(path_copy);

        const read_conn: *schema_mod.Connection = if (opts.separate_read_connection)
            try schema_mod.Connection.open(allocator, opts.path, opts.busy_timeout_ms)
        else
            conn;
        errdefer if (opts.separate_read_connection) read_conn.close();

        client.* = .{
            .conn = conn,
            .read_conn = read_conn,
            .read_conn_owned = opts.separate_read_connection,
            .allocator = allocator,
            .waker = Waker.init(allocator),
            .path = path_copy,
            .poll_interval_ms = opts.poll_interval_ms,
            .max_batch_size = opts.max_batch_size,
        };

        client.last_log_pos.store(@intCast(try conn.queryScalarI64("SELECT COALESCE(MAX(log_position), 0) FROM events")), .seq_cst);
        return client;
    }

    /// Shutdown is deterministic: publish closed, wake waiters and wait
    /// for all registered workers before releasing either SQLite handle.
    pub fn close(self: *Client) void {
        if (self.closed.swap(true, .seq_cst)) return;
        self.waker.deinit();

        var threaded: std.Io.Threaded = .init_single_threaded;
        const io = threaded.io();
        while (self.active_workers.load(.seq_cst) != 0) {
            io.sleep(.fromMilliseconds(1), .awake) catch {};
        }

        if (self.read_conn_owned) self.read_conn.close();
        self.conn.close();
        self.allocator.free(self.path);
        self.allocator.destroy(self);
    }

    pub fn lastLogPosition(self: *Client) u64 {
        return self.last_log_pos.load(.seq_cst);
    }

    pub fn stats(self: *Client) errors_mod.Error!types.Stats {
        if (self.closed.load(.seq_cst)) return error.DatabaseClosed;

        return .{
            .stream_count = try scalarI64(self.conn, "SELECT COUNT(*) FROM streams"),
            .event_count = try scalarI64(self.conn, "SELECT COUNT(*) FROM events"),
            .tombstoned_streams = try scalarI64(self.conn, "SELECT COUNT(*) FROM streams WHERE deleted_at IS NOT NULL"),
            .persistent_groups = try scalarI64(self.conn, "SELECT COUNT(*) FROM persistent_subscriptions"),
            .snapshots = try scalarI64(self.conn, "SELECT COUNT(*) FROM snapshots"),
            .db_size_bytes = try scalarI64(self.conn, "SELECT CAST(page_count AS INTEGER) * page_size FROM pragma_page_count(), pragma_page_size()"),
            .last_log_position = self.lastLogPosition(),
        };
    }
};

pub fn scalarI64(conn: *schema_mod.Connection, sql: []const u8) !i64 {
    return conn.queryScalarI64(sql);
}
