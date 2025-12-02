//! TuQueQue: An MPSC Swap-Buffer Queue
//!
//! This implements a lock-free, fallible MPSC queue of fixed size.  Lock-free
//! means that writes and reads both proceed without the use of a Mutex, or
//! any other lock of indefinite duration.  Fallible means that writes and
//! dequeues can both fail.  MPSC is multiple producer, single consumer:
//! many writers and only one reader.  Fixed size is why writes can fail.
//!
//! Writers contend for updates of a single atomic value, after which they can
//! write, unless that half of the queue is full.  Readers swap the buffer using
//! the same atomic value, but may have to wait for writers to commit their
//! writes, and the swap can time out after a backed-off spinloop.  So long as
//! writers fulfil their invariant of completing and committing all writes, the
//! dequeue will succeed once that happens.
//!
//! A `TuQueQue` can be equipped with notifying capability for the reader,
//! for writers, both, or neither.  Reader-notify provides a variation on the
//! dequeue function, which always succeeds, but may sleep the thread.
//!
//! Writer-notify is provided as a thread handle, but no automatic
//! block-on-full is provided: user code is expected to obtain this handle,
//! catch the error, and do something appropriate.  This can implement some
//! nice producer/consumer SPSC patterns, which is the reason the affordance
//! is available.  While it can also be used to manage a wait queue, that
//! is probably not the best use of this data structure, and an automatic
//! block-on-full using a simple `Condition` would produce a thundering-herd
//! effect when the queue is swapped, hence, no such feature is included.
//!
//! Once allocated, the `TuQueQue` does not need an `Allocator`, but the reader
//! is able to grow the halves of the queue, one at a time, if needed.  Use the
//! API for this, please, the process is fairly involved, and that way `destroy`
//! can properly free memory even in the event that this has happened.
//!
//! The main thing to be aware of is that, at any given time, only half of the
//! capacity is available to the writer at once.  The upside is that the reader
//! gets all the updates at once on swap, all lined up in a nice slice with no
//! contention.  This was designed for a message-passing architecture, where the
//! size of each queue half is expected to be mostly unusued, with threads able
//! to keep up with the load, and writers having other things to occupy their
//! scheduled time slices if a queue is temporarily full.
//!
//! No data structure can do everything, and this is not naturally suited to
//! work-stealing, or other approaches to handling the case where a reader is
//! unable to keep up with writes: it's MPSC, and should be used when that
//! pattern is correct for the application.
//!
//! Heavily inspired by the Rust crate `swap-buffer-queue`, found here:
//!
//! https://crates.io/crates/swap-buffer-queue
//!
//! Thanks @wyfo!

pub const EnqueueToken = packed struct(u64) {
    index: u1, // Which queue is writing
    capacity: u62, // Remaining capacity of that queue
    closed: bool, // The queue as a whole is closed

    const LOCK_MASK = (EnqueueToken{ .index = 0, .capacity = 0, .closed = true }).toWord();

    /// Create a new EnqueueToken of the provided capacity.  The index is 0,
    /// and it is closed.
    pub fn new(capacity: u64) EnqueueToken {
        return .{ .index = 0, .capacity = @truncate(capacity), .closed = true };
    }

    pub fn fromWord(word: u64) EnqueueToken {
        return @bitCast(word);
    }

    pub fn toWord(e_state: EnqueueToken) u64 {
        return @bitCast(e_state);
    }

    // Not atomic. Dubious!
    pub fn switchQueue(e_state: EnqueueToken) EnqueueToken {
        var e_new = e_state;
        e_new.index +%= 1;
        return e_new;
    }

    // Try to reserve 'slots' in the queue.  If that much is available, it is
    // returned.
    pub fn tryReserve(e_state: EnqueueToken, slots: u64) ?EnqueueToken {
        if (slots > e_state.capacity) return null;
        return .{
            .closed = e_state.closed,
            .capacity = e_state.capacity - slots,
            .index = e_state.index,
        };
    }

    /// Reserve up to 'slots' in the queue, getting back what may be as little
    /// as none.  TODO: maybe this should error on none?
    pub fn reserveUpTo(e_state: EnqueueToken, slots: u64) EnqueueToken {
        return .{
            .closed = e_state.closed,
            .capacity = if (e_state.capcity > slots) e_state.capacity - slots else 0,
            .index = e_state.index,
        };
    }

    pub fn close(e_state: *EnqueueToken, comptime order: AtomicOrder) void {
        const uptr: *u64 = @ptrCast(@alignCast(e_state));
        _ = @atomicRmw(u64, uptr, .Or, LOCK_MASK, order);
    }

    pub fn reopen(e_state: *EnqueueToken, comptime order: AtomicOrder) void {
        const uptr: *u64 = @ptrCast(@alignCast(e_state));
        _ = @atomicRmw(u64, uptr, .And, ~LOCK_MASK, order);
    }
};

pub const DequeueToken = packed struct(u64) {
    index: u1,
    capacity: u62,
    _: u1 = 0,

    pub fn new(capacity: usize) DequeueToken {
        return .{ .index = 1, .capacity = @intCast(capacity) };
    }

    // TODO: umax lock guard?

    pub fn fromWord(word: u64) DequeueToken {
        return @bitCast(word);
    }

    pub fn toWord(e_state: DequeueToken) u64 {
        return @bitCast(e_state);
    }
};

/// Wrap a type in a full cache line of padding, to prevent false-sharing
/// contention.  The queues themselves are always cache-aligned at the head, but
/// aren't padded out per-cell, unless you use this function to provide Cell, or
/// otherwise cache-align that type.
pub fn CachePad(T: type) type {
    return struct {
        raw: T align(cache_line),

        const CPad = @This();

        pub fn get(padded: *CPad) *T {
            return &padded.raw;
        }

        pub fn load(padded: *const CPad, order: AtomicOrder) T {
            return @atomicLoad(T, &padded.raw, order);
        }
    };
}

/// Take a size and pad it out to the provided power of two (the quantum itself,
/// not the log).
fn alignSizeTo(size: usize, pow_2: usize) usize {
    assert(pow_2 != 0 and std.math.isPowerOfTwo(pow_2));

    const mask = pow_2 - 1;
    return (size + mask) & ~mask;
}

/// A Mutex and associated Condition.
pub const Notifier = struct {
    mut: Mutex,
    cond: Condition,

    pub fn new() Notifier {
        return .{ .mut = .{}, .cond = .{} };
    }

    pub fn notify(note: *Notifier) void {
        // Sequence the mutex:
        note.mut.lock();
        note.mut.unlock();
        note.cond.signal();
    }

    /// To be on the safe side, this is called whenever
    /// the queue is closed.  Really, don't use a wait
    /// token for multiple writers.
    pub fn notifyAll(note: *Notifier) void {
        // Sequence the mutex:
        note.mut.lock();
        note.mut.unlock();
        note.cond.broadcast();
    }

    pub fn wait(note: *Notifier) void {
        // Acquire the mutex:
        note.mut.lock();
        // Drop it again when we return:
        defer note.mut.unlock();
        // Drop and wait
        note.cond.wait(&note.mut);
    }

    pub fn timedWait(note: *Notifier, timeout_ns: usize) void {
        // Acquire the mutex:
        note.mut.lock();
        // Drop it again when we return:
        defer note.mut.unlock();
        // Drop and wait for signal or timeout
        note.cond.timedWait(&note.mut, timeout_ns);
    }

    // TODO: opaque 'Waiter' type
};

//| TuQueQue
//|
//| The principle is very simple.  We have two buffers (slices) and two lengths,
//| plus the EnqueueToken and the DequeueToken.  Enqueuing tries to reserve some
//| space on a copy of the master EnqueueToken, then swaps it in, once that
//| works, the tracked capacity is smaller, but the length doesn't reflect this.
//| When the writer is done, they atomic-add the length.  This lets writers
//| 'leapfrog' each other.
//|
//| Ignoring the dequeue logic which allows non-empty buffers to be returned,
//| when the reader wants another queue, they atomic-swap the index.  Now all
//| new writers are on the just-drained queue, but the enqueue might have
//| writers when we swap, we wait around for awhile for them to finish and if it
//| takes too long we send a `DequeuePending` error so the caller can decide how
//| to wait for that to finish.
//|
//| If a Condition is included, the caller will be slept-on until writes finish,
//| if spinning times out, and if the queue is empty, iff it calls the blockable
//| dequeue function (which will be a compileError without the condition).
//|
//| The DequeueToken is about growing (or shrinking) the reader-owned queue, and
//| tracking partial draining and other things like that.  That logic is not
//| 100% transparent to me yet.

/// Whether to have a Condition for notifying the reader, the writers, neither,
/// or both.  As an MPSC queue, writers do not block each other (outside of
/// brief contention for the slice reservation), but the queue fills up at
/// half capacity.  It's probably better to handle congestion in some other
/// way, such as growing the queue, but paired conditions nicely support some
/// producer/consumer patterns.
///
/// As such, writers have no "block me until I can write" function call, but can
/// request a wake token from their write capability.  You'll get a thundering
/// herd if you just share that and use it without due consideration, but it can
/// be used by a dispatch thread to build some well-understood wait queues, if
/// desired.  Probably you'll want to solve whatever problem that represents in
/// some other fashion.
pub const NotificationKind = enum {
    neither,
    reader,
    writer,
    both,

    pub fn notifyReader(nk: NotificationKind) bool {
        return switch (nk) {
            .neither, .writer => false,
            .reader, .both => true,
        };
    }

    pub fn notifyWriter(nk: NotificationKind) bool {
        return switch (nk) {
            .neither, .reader => false,
            .writer, .both => true,
        };
    }
};

pub fn TuQueQue(
    Cell: type,
    comptime capacity: usize,
    comptime notify: NotificationKind,
) type {
    // Sanity checks:
    if (capacity & 1 == capacity)
        @compileError("Queue capacity is a total capacity, and must be an even number");
    if (capacity > std.math.maxInt(u63))
        @compileError("Not only is this amount of memory unavailable for purchase, we cannot represent it");

    const queue_bytes = alignSizeTo(@sizeOf(Cell) * (capacity >> 1), cache_line);
    _ = queue_bytes;

    const half_cap = capacity >> 1;

    return struct {
        enqueue: CachePad(EnqueueToken),
        dequeue: CachePad(DequeueToken),
        lengths: [2]CachePad(u64),
        queues: [2][]Cell,
        reader_notify: if (reader_notified) Notifier else void,
        writer_notify: if (writer_notified) Notifier else void,

        const Queue = @This();

        pub const reader_notified = notify.notifyReader();

        pub const writer_notified = notify.notifyWriter();

        /// Create a new swap-buffer queue.  The queue returned will be closed
        /// for writes.  Destroy when finished with `q.destroy(allocator)`.
        pub fn create(allocator: Allocator) Allocator.Error!*Queue {
            // TODO: We will allocate all the starting capacity in one
            // block, putting the TuQueQue at the head of it and making
            // the swap queues on the tail.  And align the whole thing.
            // But that all can wait.
            const q = try allocator.create(Queue);
            errdefer allocator.destroy(q);
            q.enqueue = EnqueueToken.new(half_cap);
            q.dequeue = DequeueToken.new(half_cap);
            q.lengths[0].get().* = 0;
            q.lenghts[1].get().* = 0;
            q.reader_notify = if (reader_notified) Notifier.new() else {};
            q.writer_notify = if (writer_notified) Notifier.new() else {};
            q.queues[0] = try allocator.alloc(Cell, half_cap);
            errdefer allocator.free(q.queues[0]);
            q.queues[1] = try allocator.alloc(Cell, half_cap);
            errdefer comptime unreachable;
            return q;
        }

        /// Destroy the queue.  Be aware that this can strand writers
        /// waiting on a notification, if configured for writer-notify.
        pub fn destroy(q: *Queue, allocator: Allocator) void {
            allocator.free(q.queues[0]);
            allocator.free(q.queues[1]);
            allocator.destroy(q);
        }

        /// If the queue is closed for writes, this will open it.  Otherwise
        /// it will have no effect.
        pub fn open(q: *Queue) void {
            // When used correctly, a closed queue will never have contention
            // during opening.  So we don't go to any great effort to do backoff
            // or anything like that here.
            while (true) {
                const n_queue_now = q.enqueue.load(.acquire);
                var n_queue_next = n_queue_now;
                n_queue_next.closed = false;
                if (@cmpxchgWeak(
                    EnqueueToken,
                    q.enqueue.get(),
                    n_queue_now,
                    n_queue_next,
                    .acq_rel,
                    .monotonic,
                )) |_| {
                    spinLoopHint();
                    continue;
                }
                break;
            }
        }

        /// Close the queue for new writes.  There may be writes continuing
        /// in the write queue, but no new writes will succeed after this
        /// returns.
        pub fn close(q: *Queue) void {
            // We're contending this time, but it's just the one word,
            // so we do get there eventually.  But we'll do backoff.
            var backoff: u9 = 1;
            while (true) {
                const n_queue_now = q.enqueue.load(.acquire);
                var n_queue_next = n_queue_now;
                n_queue_next.closed = true;
                if (@cmpxchgWeak(
                    EnqueueToken,
                    q.enqueue.get(),
                    n_queue_now,
                    n_queue_next,
                    .acq_rel,
                    .monotonic,
                )) |_| {
                    // Liveness: one reader, and writers reduce capacity each
                    // time we (non-spuriously) fail the CAS.  So at our
                    // unluckiest the queue will be fully reserved at some
                    // point.
                    for (0..backoff) |_| {
                        spinLoopHint();
                    }
                    backoff *%= 2;
                    if (backoff == 0) backoff = 1;
                    continue;
                }
                break;
            }
            if (comptime writer_notified) {
                // This is the only time we send a broadcast, because we might
                // be freeing the queue, and hopefully a bunch of stuck writers
                // (don't do this!!) see the queue closed before their handle is
                // invalidated.
                q.writer_notify.notifyAll();
            }
        }
    };
}

test alignSizeTo {
    try expectEqual(256, alignSizeTo(129, 128));
    try expectEqual(256, alignSizeTo(129, 256));
    try expectEqual(192, alignSizeTo(129, 64));
    try expectEqual(256, alignSizeTo(256, 256));
}

test "alignment" {
    try expectEqual(std.atomic.cache_line, @sizeOf(CachePad(usize)));
}

test "pointer casting" {
    var u_q: u64 = 1;
    const u_p: *EnqueueToken = @ptrCast(@alignCast(&u_q));
    u_q += 1;
    try expectEqual(0, u_p.index);
    try expectEqual(1, u_p.capacity);
    const up_now = u_p;
    u_p.close(.monotonic);
    try expect(u_p.closed);
    u_p.reopen(.monotonic);
    try expect(!u_p.closed);
    try expectEqual(up_now, u_p);
}

const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;

const AtomicOrder = std.builtin.AtomicOrder;
const cache_line = std.atomic.cache_line;
const spinLoopHint = std.atomic.spinLoopHint;

const Condition = std.Thread.Condition;
const Mutex = std.Thread.Mutex;
const testing = std.testing;
const expect = testing.expect;
const expectEqual = testing.expectEqual;
