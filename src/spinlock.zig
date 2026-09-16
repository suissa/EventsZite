//! Small adaptive lock built on `std.atomic.Mutex`.
//!
//! Very short contention spins briefly, which is cheaper than entering a
//! scheduler path. If the lock is still held after that bounded fast path,
//! the waiter sleeps for 1 ms through Zig's `std.Io` API before retrying.
//! This matters for `Client.writer_mu`, which can be held across a SQLite
//! transaction and therefore across real disk / WAL I/O.

const std = @import("std");

pub const Spinlock = struct {
    state: std.atomic.Mutex = .unlocked,

    pub fn lock(self: *Spinlock) void {
        const spin_budget: usize = 64;
        var spins: usize = 0;

        while (!self.state.tryLock()) {
            if (spins < spin_budget) {
                spins += 1;
                std.atomic.spinLoopHint();
                continue;
            }

            // Do not burn a CPU core while another writer is inside SQLite.
            // The public lock API stays synchronous, so the internal scheduler
            // handle is intentionally local to the contended slow path.
            var threaded: std.Io.Threaded = .init_single_threaded;
            const io = threaded.io();
            io.sleep(.fromMilliseconds(1), .awake) catch {};
            spins = 0;
        }
    }

    pub fn unlock(self: *Spinlock) void {
        self.state.unlock();
    }
};
