//! Public error sets and a few helper functions for converting
//! SQLite return codes into typed errors.

const std = @import("std");
const c = @import("c.zig").c;

/// All errors that can be returned by the public API.
pub const Error = error{
    /// `Open` was called on a path that already points at an
    /// unreadable file, or with invalid flags.
    CannotOpenDatabase,
    /// The stream does not exist and the operation requires it.
    StreamNotFound,
    /// The stream has been soft-deleted (tombstoned) and can
    /// no longer be appended to or read.
    StreamTombstoned,
    /// The expected revision did not match the current stream
    /// revision. The actual revision is attached to the error
    /// via `WrongExpectedVersionError`.
    WrongExpectedVersion,
    /// A multi-stream transaction failed; rolled back.
    InvalidTransaction,
    /// `Subscribe.close` was called; subsequent channel reads
    /// yield this error.
    SubscriptionClosed,
    /// A persistent subscription group was looked up but does
    /// not exist.
    PersistentSubscriptionNotFound,
    /// `CreatePersistentSubscription` was called for a group
    /// that already exists.
    PersistentSubscriptionExists,
    /// No snapshot exists for the requested stream/revision.
    SnapshotNotFound,
    /// Operation attempted on a closed client.
    DatabaseClosed,
    /// Caller-supplied argument is invalid.
    InvalidArgument,
    /// The requested option combination cannot be implemented
    /// safely by the current storage model.
    UnsupportedConfiguration,
    /// A query yielded no rows where at least one was expected.
    NotFound,
    /// `sqlite3_prepare_v2` returned SQLITE_OK but the handle
    /// is null (defensive — should not happen in practice).
    PrepareFailed,
    /// Catch-all for a SQLite C API call that returned an error
    /// code other than the ones we map explicitly.
    Sqlite,
    /// `std.heap.OutOfMemory` propagated from an allocator.
    OutOfMemory,
};

/// Carries the actual and expected revisions for an
/// `appendToStream` that failed because of a mismatch.
pub const WrongExpectedVersionError = struct {
    expected: []const u8,
    actual: u64,
    stream: []const u8,

    pub fn format(
        self: *const WrongExpectedVersionError,
        writer: *std.Io.Writer,
    ) std.Io.Writer.Error!void {
        try writer.print(
            "wrong expected version for stream {s} (expected {s}, actual {d})",
            .{ self.stream, self.expected, self.actual },
        );
    }
};

pub const AppendError = Error || std.mem.Allocator.Error;

pub fn fromSqlite(rc: c_int) ?Error {
    return switch (rc) {
        c.SQLITE_OK, c.SQLITE_ROW, c.SQLITE_DONE => null,
        c.SQLITE_BUSY, c.SQLITE_LOCKED => Error.Sqlite,
        else => Error.Sqlite,
    };
}

pub fn errorMessage(allocator: std.mem.Allocator, db: *c.sqlite3) ![]u8 {
    const cstr = c.sqlite3_errmsg(db);
    return allocator.dupe(u8, std.mem.span(cstr));
}
