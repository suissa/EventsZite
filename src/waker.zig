//! Broadcast primitive used to wake catch-up subscriptions when a new
//! event is appended. Time-based waiting uses `std.Io.sleep` so a poll
//! interval represents wall-clock time instead of a synthetic spin count.

const std = @import("std");

pub const Waker = struct {
    const Waiter = struct {
        armed: std.atomic.Value(bool) = .init(false),
        closed: std.atomic.Value(bool) = .init(false),
        next: ?*Waiter = null,
    };

    mu: std.atomic.Mutex = .unlocked,
    head: ?*Waiter = null,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) Waker {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Waker) void {
        self.lock();
        var cur = self.head;
        while (cur) |w| {
            const next = w.next;
            w.closed.store(true, .release);
            cur = next;
        }
        self.head = null;
        self.unlock();
    }

    pub fn register(self: *Waker) !*Waiter {
        const w = try self.allocator.create(Waiter);
        w.* = .{};
        self.lock();
        w.next = self.head;
        self.head = w;
        self.unlock();
        return w;
    }

    pub fn unregister(self: *Waker, w: *Waiter) void {
        self.lock();
        var prev: ?*Waiter = null;
        var cur = self.head;
        while (cur) |node| {
            if (node == w) {
                if (prev) |p| {
                    p.next = node.next;
                } else {
                    self.head = node.next;
                }
                break;
            }
            prev = cur;
            cur = node.next;
        }
        self.unlock();
        w.closed.store(true, .release);
        _ = w.armed.swap(false, .acq_rel);
        self.allocator.destroy(w);
    }

    /// Wait until signaled, closed, or the real wall-clock timeout expires.
    /// One-millisecond slices keep close/signal latency bounded without
    /// burning a CPU core while the subscription is idle.
    pub fn wait(w: *Waiter, timeout_ms: u32) bool {
        if (w.armed.swap(false, .acq_rel)) return true;
        if (w.closed.load(.acquire)) return false;
        if (timeout_ms == 0) return false;

        var threaded: std.Io.Threaded = .init_single_threaded;
        const io = threaded.io();
        var remaining = timeout_ms;
        while (remaining != 0) : (remaining -= 1) {
            io.sleep(.fromMilliseconds(1), .awake) catch {};
            if (w.armed.swap(false, .acq_rel)) return true;
            if (w.closed.load(.acquire)) return false;
        }
        return false;
    }

    pub fn signal(self: *Waker) void {
        self.lock();
        var cur = self.head;
        while (cur) |w| {
            w.armed.store(true, .release);
            cur = w.next;
        }
        self.unlock();
    }

    fn lock(self: *Waker) void {
        while (!self.mu.tryLock()) std.atomic.spinLoopHint();
    }

    fn unlock(self: *Waker) void {
        self.mu.unlock();
    }
};
