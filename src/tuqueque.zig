//! TuQueQue: An MPSC Swap-Buffer Queue
//!
//! This implements a lock-free, fallible MPSC queue of fixed size.  Lock-free
//! means that writes and reads both proceed without the use of a mutex, or
//! any other lock of indefinite duration.  Fallible means that enqueues and
//! dequeues can both fail.  MPSC is multiple producer, single consumer: many
//! writers and only one reader.  Fixed size is why writes can fail.
//!
//! Writers contend for updates of a single atomic value, after which they can
//! write, unless that half of the queue is full.  Readers swap the buffer using
//! the same atomic value, but may have to wait for writers to commit their
//! writes, and the swap can time out after a backed-off spinloop.  So long as
//! writers fulfil their invariant of completing and committing all writes, the
//! dequeue will succeed once that happens.
//!
//! The queues themselves are cache-aligned, and all contended values are
//! cache-padded onto their own line.  If cache-padded data structures are
//! used, or if enqueuing otherwise arranges to enqueue data in cache-aligned
//! blocks, no false sharing will occur.  Note that this is purely an efficiency
//! concern, not a matter of correctness: while you probably don't want 128
//! threads writing one `u8` apiece, it will work, the data structure is
//! suitable to use as a byte buffer in the normal case where the mean write is
//! substantially larger than one byte.
//!
//! A `TuQueQue` can be equipped with notifying capability for the reader,
//! for writers, both, or neither.  Reader-notify provides a variation on the
//! dequeue function, which always succeeds, but may sleep the thread.  Most
//! users will find the reader-notified variation the most useful of the four.
//!
//! Writer-notify is provided as a raw `Notify` type, but no automatic
//! block-on-full is provided: user code is expected to obtain this handle,
//! catch the error, and do something appropriate.  This can implement some
//! nice producer/consumer SPSC patterns, which is the reason the affordance
//! is available.  While it can also be used to manage a wait queue, that
//! is probably not the best use of this data structure, and an automatic
//! block-on-full using a simple `Condition` would produce a thundering-herd
//! effect when the queue is swapped.  Hence, no such feature is included.
//!
//! Once allocated, the `TuQueQue` does not need an `Allocator`, but the reader
//! is able to grow the halves of the queue, one at a time, if needed.  Use the
//! API for this, please, the process is fairly involved, and that way `destroy`
//! can properly free memory even in the event that this has happened.
//!
//! The main thing to be aware of is that, at any given time, only half of
//! the capacity is available to the writers at any given point.  The upside
//! is that the reader gets all the updates at once on swap, all lined up in
//! a nice slice with no contention.  This was chosen for a message-passing
//! architecture, where the size of each queue half is expected to be more than
//! adequate, with threads able to keep up with the load, and writers having
//! other things to occupy their scheduled time slices if a queue is temporarily
//! full.
//!
//! No data structure can do everything, and this is not naturally suited to
//! work-stealing, or other approaches to handling the case where a reader is
//! unable to keep up with writes.  It's MPSC, and should be used when that
//! pattern is correct for the application.
//!
//! Heavily inspired by the Rust crate `swap-buffer-queue`, found here:
//!
//! https://crates.io/crates/swap-buffer-queue
//!
//! Thanks @wyfo!
//!
//! ## Notes on Correct Use
//!
//! The most important invariant is that this is a single-consumer queue.  The
//! queue itself is assumed to be owned by the reader, that reader must live on
//! exactly one thread at any given point.  To facilitate this, writes should
//! come from a `WriteHandle`, this is an erased pointer to the queue which
//! provides only enqueuing functions.  There is also a dequeue lock which
//! prevents reader-side reëntrance: if you see a `DequeueLocked` error, that's
//! a correctness problem with your program.
//!
//! There are several ways a queue can get into an inconsistent state through
//! multithreaded use of the reader-side, not all of which are guarded against
//! by the implementation.
//!
//! A successful dequeue returns a slice of the buffer, and this slice is only
//! valid until another dequeue is attempted.  However, unusued portions of
//! the slice can be returned with `requeueFromIndex`, and the next successful
//! dequeue will give that back.  You'll either want to call a requeue at most
//! once per dequeue, or thoroughly understand what it does, and I suggest the
//! former.
//!
//! The queue must be opened in order to write to it, conversely, writers will
//! see an error writing to a closed queue.  However, the 'closed bit' lives on
//! the queue, so a writer trying to write to a _destroyed_ queue has committed
//! illegal behavior.  Correct user code must provide some mechanism for the
//! reader and the writers to agree on when the queue will be destroyed.
//!
//! To assist with this, a `TuQueQue` will, by default, keep a reference count
//! of `WriteHandle`s in play.  Zig being Zig, user code will need to call
//! `release()` on the handles for this to work (safe modes will panic if this
//! underflows).  The reader, holder of the queue proper, does not reference-
//! count itself, because the correct number of references to the reader queue
//! is one.  Calling `release` when reference counting is not enabled is
//! harmless, and will undefine the queue pointer of the handle.
//!
//! This library will panic if an attempt to destroy an open queue is made.
//! Always close the queue before destroying it.  Note that a closed queue is
//! closed to _new_ writes, but may have active writes incoming, so the reader
//! might have to pull several times before getting a `null` result: a closed
//! queue returning `null` will not have additional writes unless reopened, but
//! of course this does nothing to prevent threads from trying.

//| TuQueQue
//|
//| The principle is very simple.  We have two buffers (slices) and two lengths,
//| plus the EnqueueToken and the DequeueToken.  Enqueuing tries to reserve some
//| space on a copy of the master EnqueueToken, then swaps it in.  Once that
//| works, the tracked capacity is smaller, but the length doesn't reflect this.
//| When the writer is done, they atomic-add the length.  This lets writers
//| 'leapfrog' each other.
//|
//| Ignoring the dequeue logic which allows non-empty buffers to be requeued,
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

/// A Mutex and associated Condition.  This has value semantics,
/// copies will do the same thing as the original.
pub const Notifier = struct {
    mut: Mutex,
    cond: Condition,

    pub fn new() Notifier {
        return .{ .mut = .{}, .cond = .{} };
    }

    pub fn sequence(note: *Notifier) void {
        note.mut.lock();
        note.mut.unlock();
    }

    /// Notify any waiters that it's their turn.
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

    /// Await a notification.
    pub fn wait(note: *Notifier) void {
        // Acquire the mutex:
        note.mut.lock();
        // Drop it again when we return:
        defer note.mut.unlock();
        // Drop and wait
        note.cond.wait(&note.mut);
    }

    /// Await a notification for more-or-less `timeout_ns`.  When
    /// the thread actually awakens is out of our hands.
    pub fn timedWait(note: *Notifier, timeout_ns: usize) void {
        // Acquire the mutex:
        note.mut.lock();
        // Drop it again when we return:
        defer note.mut.unlock();
        // Drop and wait for signal or timeout
        note.cond.timedWait(&note.mut, timeout_ns) catch {};
    }
};

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

/// Errors which could be returned if a write fails
const EnqueueErrorSet = error{
    /// No one can write to the queue
    QueueIsClosed,
    /// There is no room left in the (half) queue at all
    QueueIsFull,
    /// There is not enough room to write the entire slice provided.
    NotEnoughRoom,
};

const DequeErrorSet = error{
    /// There are writes pending and we can't retrieve them
    /// until they're committed.
    DequeuePending,
    /// An attempt was made to resize the dequeue while contents
    /// remained on it.
    DequeueNotEmpty,
};

/// Options for configuring a `TueQueQue`.  Reasonable defaults
/// are provided.
pub const QueueOptions = struct {
    notify: NotificationKind = .reader,
    refcount: bool = true,
    secure_erase: bool = false,
};

/// Define a swap-buffer queue.  Capacity must be an even number, one-half of
/// which is available for writing at any given moment.  Specify whether you
/// want notifiers for the reader, the writers, both, or neither.
pub fn TuQueQue(
    Cell: type,
    comptime queue_capacity: usize,
    comptime opt: QueueOptions,
) type {
    // Sanity checks:
    if (queue_capacity & 1 == queue_capacity)
        @compileError("Queue capacity is a total capacity, and must be an even number");
    if (queue_capacity > std.math.maxInt(u63))
        @compileError("Not only is this amount of memory unavailable for purchase, we cannot represent it");

    const queue_bytes = alignSizeTo(@sizeOf(Cell) * (queue_capacity >> 1), cache_line);

    const half_cap = queue_capacity >> 1;

    // Inconveniently-short queues are useful for testing, not in production.
    const start_cap = if (is_test) half_cap else (queue_bytes / half_cap);

    return struct {
        /// The status word for enqueuing, always handled with
        /// .acquire / .release semantics.  All contention happens
        /// here.
        enqueuing: CachePad(EnqueueToken),
        /// The status word for dequeuing. This is handled monotonically,
        /// because it sees dual-use as a reëntrancy guard, ensuring that only
        /// one reader (this is a _single-consumer_ queue) ever does mutable
        /// things.  Monotonic ensures a consistent modification order is seen:
        /// one reader is in the wrong, and our code can't know which one that
        /// is.
        dequeuing: CachePad(DequeueToken),
        /// The lengths of committed writes.  Acquire-release so that read
        /// values are always current.  Add-only (except on reset, during the
        /// next dequeue), so no contention here.
        lengths: [2]CachePad(u62),
        /// The queues.  These are not atomic, because by construction, we
        /// carefully segregate who can write where.  This can lead to false
        /// sharing if the Cell type is not itself cache-aligned, but it's up to
        /// user code what it wants to do about that.
        queues: [2]CacheCells,
        /// A Notifier for the reader, if this queue notifies readers.
        reader_notify: if (reader_notified) Notifier else void,
        /// A Notifier for the writer, if this queue notifies writers.
        writer_notify: if (writer_notified) Notifier else void,
        /// A reference count of write handles, if this queue counts references.
        writer_refs: if (refcounted) CachePad(usize) else void,

        const Queue = @This();

        /// We align the head of both half-queues to the cache, so that even
        /// sub-cache-aligned data structures can take full advantage of cache-
        /// line isolation, if desired.
        const CacheCells = []align(cache_line) Cell;

        /// Errors visible to a writer (enqueuer)
        pub const EnqueueError = EnqueueErrorSet;
        /// Errors visible to a reader (dequeuer)
        pub const DequeueError = DequeErrorSet;

        /// Does this type notify readers?
        pub const reader_notified: bool = opt.notify.notifyReader();
        /// Does this type notify writers?
        pub const writer_notified: bool = opt.notify.notifyWriter();
        /// Does this type reference-count write handles?
        pub const refcounted: bool = opt.refcount;
        /// Does this type secure-erase reserved slices?
        pub const secure_erased: bool = opt.secure_erase;

        //| Reader API
        //|
        //| User code must preserve the **one reader invariant**.  Errors will
        //| be thrown if the invariant is violated in a detectable manner.

        /// Create a new swap-buffer queue.  The queue returned will be closed
        /// for writes.  Destroy when finished with `q.destroy(allocator)`.
        pub fn create(allocator: Allocator) Allocator.Error!*Queue {
            // TODO: We will allocate all the starting capacity in one
            // block, putting the TuQueQue at the head of it and making
            // the swap queues on the tail.  And align the whole thing.
            // But that all can wait until what we have is tested.
            const q = try allocator.create(Queue);
            errdefer allocator.destroy(q);
            q.enqueuing.raw = EnqueueToken.new(start_cap);
            q.dequeuing.raw = DequeueToken.new();
            q.lengths[0].raw = 0;
            q.lengths[1].raw = 0;
            q.reader_notify = if (reader_notified) Notifier.new() else {};
            q.writer_notify = if (writer_notified) Notifier.new() else {};
            q.writer_refs = if (refcounted) .{ .raw = 0 } else {};
            q.queues[0] = try allocator.alignedAlloc(Cell, cache_align, start_cap);
            // q.queues[0] = try allocator.alloc(Cell, start_cap);
            errdefer allocator.free(q.queues[0]);
            q.queues[1] = try allocator.alignedAlloc(Cell, cache_align, start_cap);
            // q.queues[1] = try allocator.alloc(Cell, start_cap);
            errdefer comptime unreachable;
            return q;
        }

        /// Destroy the queue.  Asserts that the queue has already been closed.
        /// If reference-counted, asserts that all writer references have been
        /// released.
        pub fn destroy(q: *Queue, allocator: Allocator) void {
            // Some sanity checks:
            _ = q.getAndLockDequeue();
            if (comptime refcounted) {
                const refs = q.writer_refs.load(.acquire);
                _ = refs; // Refcount debugging, it's a things
                assert(q.writer_refs.load(.acquire) == 0);
            }
            if (q.enqueuing.load(.acquire).closed) {
                allocator.free(q.queues[0]);
                allocator.free(q.queues[1]);
                allocator.destroy(q);
            } else {
                @panic("The queue must be closed before it can be destroyed");
            }
        }

        /// Destroy the queue without any sanity checks.  Meant to be used
        /// in an errdefer.
        pub fn errDestroy(q: *Queue, allocator: Allocator) void {
            allocator.free(q.queues[0]);
            allocator.free(q.queues[1]);
            allocator.destroy(q);
        }

        /// If the queue is closed for writes, this will open it.  Otherwise
        /// it will have no effect.
        pub fn open(q: *Queue) void {
            q.enqueuing.getPtr().open(.acq_rel);
            // Plausible someone might have seen us closed and waited:
            if (comptime writer_notified) {
                q.writer_notify.notify();
            }
        }

        /// Close the queue for new writes.  There may be writes continuing
        /// in the write queue, but no new writes will succeed after this
        /// returns.
        pub fn close(q: *Queue) void {
            q.enqueuing.getPtr().close(.acq_rel);
            if (comptime writer_notified) {
                // This is the only time we send a broadcast, because we might
                // be freeing the queue, and hopefully a bunch of stuck writers
                // (don't do this!!) see the queue closed before their handle is
                // invalidated.
                q.writer_notify.notifyAll();
            }
        }

        /// Obtain a write handle from the reader.  This is a shared reference
        /// to the queue, providing only the writer-appropriate actions.
        pub fn writeHandle(q: *Queue) WriteHandle {
            if (comptime refcounted) {
                const was = q.writer_refs.fetchAdd(1, .monotonic);
                _ = was; // refcount debugging again
            }
            return .{ .ptr = @ptrCast(q) };
        }

        /// Answer how many write handles remain.  This function requires that
        /// reference counting be enabled, and of course, writers must use
        /// .release correctly for the answer to be valid.
        pub fn writeHandlesRemaining(q: *Queue) usize {
            if (comptime !refcounted) @compileError("Reference counting is needed to answer this query");
            return q.writer_refs.load(.acquire);
        }

        /// Obtain a write notifier, if the queue is so configured.  If not it is
        /// a compile error to call this function.  A TuQueQue type can checked for
        /// this affordance with `TuQueQue(...).writer_notified`, if `true` then
        /// this function will compile when called.
        pub fn writeNotifier(q: *Queue) *Notifier {
            if (!writer_notified) @compileError("Queue is not configured with a write notifier");
            q.writer_notify.sequence();
            return &q.writer_notify;
        }

        /// Try a dequeue-and-swap.  This can be called whether or not the last
        /// one timed out.  It returns `null` if there are no writes to read, or
        /// a `DequeueError` if we spun for too long waiting, or the queue was
        /// already dequeuing (should never happen!), otherwise, it returns the
        /// message-containing slice from the queue.  This slice is only valid
        /// until the next call to `tryDequeue`, although some can be requeued
        /// if needed (see `requeueFromIndex`).
        pub fn dequeueAvailable(q: *Queue) !?[]Cell {
            // Invariant: our implementation always unlocks the dequeue when
            // the queue isn't actively accessed from the reader side (this differs
            // from the original).
            const dequeue_unlocked = q.getAndLockDequeue();
            defer if (is_debug) {
                assert(q.dequeuing.load(.monotonic) != DequeueToken.DEQUEUE_LOCK);
            };
            return q.dequeueAvailableInternal(dequeue_unlocked);
        }

        /// Dequeue messages from the queue, waiting on the Notifier as long as
        /// necessary.  This function is a compile error if reader notification is
        /// not configured.  To allow it to be error-free, this panics if it sees
        /// DequeueLocked (which should never happen).
        pub fn dequeue(q: *Queue) []Cell {
            if (!reader_notified) @compileError("This queue doesn't notify readers, use tryDequeue");
            if (q.dequeueAvailable()) |maybe_cells| if (maybe_cells) |cells| {
                return cells;
            } else { // Nothing yet, so we wait
                if (comptime writer_notified) {
                    q.writer_notify.notify();
                }
                q.reader_notify.wait();
                return q.dequeue();
            } else |err| {
                switch (err) {
                    DequeueError.DequeuePending => {
                        // Read not completed; we wait
                        q.reader_notify.wait();
                        return q.dequeue();
                    },
                    DequeueError.DequeueNotEmpty => unreachable,
                }
            }
        }

        /// Tries to dequeue, waits `timeout_ns` for a signal, then tries again with
        /// `tryDequeue`.  This is a compile error without reader notification configured.
        pub fn dequeueWithTimeout(q: *Queue, timeout_ns: usize) !?[]Cell {
            if (!reader_notified) @compileError("This queue doesn't notify readers, use tryDequeue");
            if (q.dequeueAvailable()) |maybe_cells| if (maybe_cells) |cells| {
                return cells;
            } else { // Nothing yet, so we wait
                if (comptime writer_notified) {
                    q.writer_notify.notify();
                }
                q.reader_notify.timedWait(timeout_ns);
                return q.dequeueAvailable();
            } else |err| {
                switch (err) {
                    DequeueError.DequeuePending => {
                        // Read not completed; we wait
                        q.reader_notify.timedWait(timeout_ns);
                        return q.dequeueAvailable();
                    },
                    DequeueError.DequeueNotEmpty => unreachable,
                }
            }
        }

        /// If you need to requeue some of a slice, you can.  If the
        /// unused portion of the slice would be `cells[i..]`, call
        /// `try q.requeueAfterIndex(i)`, and you'll get it back on
        /// the next dequeue.  Asserts that `i` is `<=` the length of
        /// the returned slice, the only error is if the queue is locked.
        pub fn requeueAfterIndex(q: *Queue, index: usize) void {
            var the_dequeue = q.getAndLockDequeue();
            const i: u62 = @intCast(index);
            the_dequeue.length_remaining = q.lengths[the_dequeue.index].load(.acquire) - i;
            // This also unlocks the dequeue
            q.dequeuing.store(the_dequeue, .monotonic);
        }

        /// Answers whether the writable half of the queue is full.  Uses a
        /// monotonic read, because 'true' stays that way until dequeued,
        /// and 'false' can become true after the call, so .acquire doesn't
        /// really help us here.
        pub fn queueIsFull(q: *Queue) bool {
            return q.enqueuing.load(.monotonic).capacity_remaining == 0;
        }

        /// Return the number of available (written) Cells on a best-effort,
        /// monotonic basis.  There is no guarantee that the next dequeue
        /// will return all of these messages, and it may return more.  Tries
        /// may error out due to DequeuePending, possibly more than once.
        /// A persistent reader will see this many messages after enough
        /// diligence, and before that happens, a blocking read with `dequeue`
        /// will not sleep on empty, just pending.
        pub fn availableDequeues(q: *Queue) usize {
            const the_dequeue = q.getAndLockDequeue();
            defer q.dequeuing.store(the_dequeue, .monotonic);
            const the_enqueue = q.enqueuing.load(.monotonic);
            const capacity = q.queues[the_enqueue.index].len;
            return (capacity - the_enqueue.capacity_remaining) + the_dequeue.length_remaining;
        }

        /// Return the number of Cell slots available before the queue will block.
        /// It's almost always better to just dequeue.
        pub fn remainingCapacity(q: *Queue) u62 {
            const the_dequeue = q.getAndLockDequeue();
            defer q.dequeuing.store(the_dequeue, .monotonic);
            return q.enqueuing.load(.monotonic).capacity_remaining;
        }

        /// Wait the thread without trying for new messages, before or after.
        /// Requires a reader notifier.
        pub fn wait(q: *Queue) void {
            if (!reader_notified) @compileError("Can't wait without a notifier");
            q.reader_notify.wait();
        }

        /// Wait the thread for about `timeout_ns`, without trying for new
        /// messages, before or after.  Requires a reader notifier.
        pub fn timedWait(q: *Queue, timeout_ns: usize) void {
            if (!reader_notified) @compileError("Can't wait without a notifier");
            q.reader_notify.timedWait(timeout_ns);
        }

        const WhichQueue = enum {
            dequeue,
            enqueue,
            both,

            fn countDequeue(wq: WhichQueue) bool {
                return switch (wq) {
                    .dequeue, .both => true,
                    .enqueue => false,
                };
            }

            fn countEnqueue(wq: WhichQueue) bool {
                return switch (wq) {
                    .enqueue, .both => true,
                    .dequeue => false,
                };
            }
        };

        /// Report the total capacity of either or both queues.  Mainly useful in
        /// tests, or deciding when to grow / shrink the queue, though a production
        /// reader may as well keep track of this itself and spare the cache a tiny
        /// bit of thrashing.
        pub fn queueTotalCapacity(q: *Queue, which: WhichQueue) usize {
            const the_dequeue = q.getAndLockDequeue();
            defer q.dequeuing.store(the_dequeue, .monotonic);
            const enq = if (which.countEnqueue()) q.queues[the_dequeue.index +% 1].len else 0;
            const deq = if (which.countDequeue()) q.queues[the_dequeue.index].len else 0;
            return enq + deq;
        }

        /// Resize the dequeued half of the queue pair.  This will error if that
        /// half is not completely empty, which would have been the result of a
        /// prior `DequeuePending` error.
        pub fn resizeDequeuedHalf(
            q: *Queue,
            allocator: Allocator,
            new_size: usize,
        ) (DequeueError || Allocator.Error)!void {
            const the_dequeue = q.getAndLockDequeue();
            defer q.dequeuing.store(the_dequeue, .monotonic);
            if (the_dequeue.length_remaining > 0) return DequeueError.DequeueNotEmpty;
            // TODO: we have to check if this is the posterior queue here
            q.queues[the_dequeue.index] = try allocator.realloc(q.queues[the_dequeue.index], new_size);
        }

        //| Writer API
        //|
        //| Writers should request and use a WriteHandle, in preference to sharing 'public'
        //| references to the queue itself (owned by the reader).
        //|
        //| However, these methods are public, because a read-thread enqueuing something for
        //| later is a coherent action, and not even an uncommon one.

        /// Attempt to enqueue one value of Cell.  Errors if there isn't room.
        pub fn enqueueValue(q: *Queue, val: Cell) EnqueueError!void {
            const v_slice: [1]Cell = .{val};
            return q.enqueueSlice(&v_slice);
        }

        /// Attempt to enqueue a slice of Cell values.  Errors if there isn't room,
        /// does not attempt to write some of the slice if there isn't room for
        /// all of it.  If you want that, use `enqueueUpToSlice`.
        pub fn enqueueSlice(q: *Queue, vals: []const Cell) EnqueueError!void {
            if (vals.len >= std.math.maxInt(u62)) return EnqueueError.NotEnoughRoom; // I mean...
            const v_len: u62 = @intCast(vals.len);
            const our_slice, const index = try q.reserveSliceInternal(v_len);
            @memcpy(our_slice, vals); // This asserts len == len for us
            // Now commit:
            q.commitEnqueue(v_len, index);
            return;
        }

        /// Attempt to enqueue as much as possible of `vals`.  Errors if there isn't enough
        /// room.  Returns what it didn't commit, which may be an empty slice.
        pub fn enqueueUpToSlice(q: *Queue, vals: []const Cell) EnqueueError![]const Cell {
            if (vals.len >= std.math.maxInt(u62)) return EnqueueError.NotEnoughRoom; // I mean...
            const v_len: u62 = @intCast(vals.len);
            const our_slice, const index, const rem = try q.reserveUpToSliceInternal(v_len);
            // Rem is strictly less than v_len:
            assert(rem < v_len);
            const our_half = vals[0 .. v_len - rem];
            const left_over = vals[v_len - rem ..];
            assert(our_slice.len == v_len - rem);
            @memcpy(our_slice, our_half);
            // Now commit:
            q.commitEnqueue(v_len - rem, index);
            return left_over;
        }

        /// A WriteHandle is the writer interface to our queue.
        pub const WriteHandle = struct {
            ptr: *anyopaque,
            reserve: Reservation = .None,

            // Zig doesn't have niche-optimizations for most values yet,
            // so we fake it:

            /// A reservation.  When a WriteHandle grabs a slice, the reservation
            /// is set, and used to commit the write to the queue.  Always commit!
            pub const Reservation = packed struct(u64) {
                index: u1,
                len: u62,
                something: bool,

                pub const None: Reservation = .{ .index = 0, .len = 0, .something = false };

                pub fn some(index: u1, len: usize) Reservation {
                    return .{ .index = index, .len = @intCast(len), .something = true };
                }
            };

            /// Try to enqueue a single value onto the queue.
            pub fn enqueueValue(wh: *WriteHandle, val: Cell) EnqueueError!void {
                const q: *Queue = @ptrCast(@alignCast(wh.ptr));
                return q.enqueueValue(val);
            }

            /// Attempt to enqueue a slice of Cell values.  Errors if there isn't room,
            /// does not attempt to write some of the slice if there isn't room for
            /// all of it.  If you want that, use `enqueueUpToSlice`.
            pub fn enqueueSlice(wh: *WriteHandle, vals: []const Cell) EnqueueError!void {
                const q: *Queue = @ptrCast(@alignCast(wh.ptr));
                return q.enqueueSlice(vals);
            }

            /// Attempt to enqueue as much as possible of `vals`.  Errors if there isn't enough
            /// room.  Returns what it didn't commit, which may be an empty slice.
            pub fn enqueueUpToSlice(wh: *WriteHandle, vals: []const Cell) EnqueueError![]const Cell {
                const q: *Queue = @ptrCast(@alignCast(wh.ptr));
                return q.enqueueUpToSlice(vals);
            }

            /// Try to reserve a slice of length `slots`.  When returned, there
            /// is an open transaction, and the reader cannot dequeue until the
            /// write is done and the WriteHandle calls `wh.commit()`.  Only one
            /// slot can be reserved at a time by any given writer. `[more]`
            ///
            /// Use of the function can leak prior contents of the queue, which
            /// can be a security risk in some applications.  If configured with
            /// the `secure_erase` option, the slice will always be filled with
            /// zeroes before returning it.
            ///
            /// There is no `reserveUpToSlice`, because it doesn't seem useful.
            /// Code should always prefer to enqueue by copying, instead of
            /// writing, to minimize contention with dequeuing.  This is mainly
            /// here to support serialization, and "I would like to serialize
            /// all, or maybe just some, of my payload" is not a coherent
            /// request to make.
            pub fn reserveSlice(wh: *WriteHandle, slots: usize) EnqueueError![]Cell {
                assert(wh.reserve.something == false);
                const q: *Queue = @ptrCast(@alignCast(wh.ptr));
                wh.reserve, const our_slice = try q.reserveSlice(slots);
                if (comptime secure_erased) {
                    std.crypto.secureZero(Cell, our_slice);
                }
                return our_slice;
            }

            /// Commit a write into a reserved slice.  Asserts that a slice was
            /// actually reserved.
            pub fn commit(wh: *WriteHandle) void {
                assert(wh.reserve.something == true);
                defer wh.reserve = Reservation.None;
                const q: *Queue = @ptrCast(@alignCast(wh.ptr));
                q.commitEnqueue(wh.reserve.len, wh.reserve.index);
            }

            /// Obtain a write Notifier.  Really there should be one of these, not
            /// many, but one writer is a valid use of a many-writer queue, so
            /// obtaining it from the WriteHandle is made possible.  Note that
            /// this is a compile error to call if write notification is not
            /// enabled for this queue type.
            pub fn writeNotifier(wh: *WriteHandle) Notifier {
                const q: *Queue = @ptrCast(@alignCast(wh.ptr));
                return q.writeNotifier();
            }

            /// Notify the reader.
            pub fn notifyReader(wh: *WriteHandle) void {
                if (comptime !reader_notified) @compileError("Reader is not configured to accept notification");
                const q: *Queue = @ptrCast(@alignCast(wh.ptr));
                q.reader_notify.notify();
            }

            /// Check whether the queue is full.  Usually it makes more sense to
            /// just try and enqueue, TOCTOU applies here in both directions.
            /// But it's a coherent writer-side query, so here it is.
            pub fn queueIsFull(wh: *WriteHandle) bool {
                const q: *Queue = @ptrCast(@alignCast(wh.ptr));
                return q.queueIsFull();
            }

            /// Returns true if the queue is closed.
            pub fn queueIsClosed(wh: *WriteHandle) bool {
                const q: *Queue = @ptrCast(@alignCast(wh.ptr));
                return q.enqueuing.load(.acquire).closed;
            }

            /// Call this function when a write handle is no longer in use.
            /// With reference counting enabled, it will decrement the count,
            /// but is safe to call either way.  In safe build modes, the
            /// pointer is made `undefined`.
            pub fn release(wh: *WriteHandle) void {
                // Probably want to ring the bell unconditionally
                var q: *Queue = @ptrCast(@alignCast(wh.ptr)); // if you see 'incorrect alignment'...
                defer if (comptime reader_notified) q.reader_notify.notify();
                if (comptime refcounted) {
                    const left = q.writer_refs.fetchSub(1, .release);
                    if (left == 1) {
                        _ = q.writer_refs.load(.acquire);
                    } else if (is_safe and left == std.math.maxInt(usize)) { // unreachable
                        @panic("Writer handle reference count underflow, too many releases");
                    }
                }
                wh.ptr = undefined; // this is why. one release!
            }
        };

        //| Private

        /// Correct use of this function involves the WriteHandle directly.  Rather than mess around
        /// with a reader-side-specific version of this function, which probably won't be needed,
        /// readers can always create a WriteHandle and call it that way (don't forget to `release`).
        fn reserveSlice(q: *Queue, slots: usize) EnqueueError!struct { WriteHandle.Reservation, []Cell } {
            const our_slice, const index = try q.reserveSliceInternal(@intCast(slots));
            return .{ WriteHandle.Reservation.some(index, slots), our_slice };
        }

        // Loads the dequeue token, erroring out if it's in the locked state.
        // If not, it swaps the locked state in, and returns the current
        // (actual) dequeue state to the caller.
        fn getAndLockDequeue(q: *Queue) DequeueToken {
            const the_dequeue = q.dequeuing.load(.monotonic);
            if (the_dequeue == DequeueToken.DEQUEUE_LOCK) dequeueLockError();
            return q.dequeuing.swap(DequeueToken.DEQUEUE_LOCK, .monotonic);
        }

        fn dequeueLockError() void { // no-coverage
            if (is_safe) {
                @panic("Dequeue was read-locked when reader-side function was called");
            }
        }

        fn dequeueAvailableInternal(q: *Queue, old_dequeue: DequeueToken) DequeueError!?[]Cell {
            assert(q.dequeuing.raw == DequeueToken.DEQUEUE_LOCK);
            if (old_dequeue.length_remaining > 0) {
                // This means we left something in the dequeue.  The canonical reason
                // this might happen is that we timed out on a previous try, but we
                // may also have requeued some of it.
                return try q.dequeueWait(.gte, old_dequeue.index, old_dequeue.length_remaining);
            } else {
                // We can swap the buffer.  As a correctness aid in development,
                // let's wipe out the queue we just finished reading.
                if (is_debug) {
                    @memset(q.queues[old_dequeue.index], undefined);
                }
            }
            // Get the capacity (resizing means this can change from `start_cap`).
            // This 'side' is never contended, and only we can change it, so no
            // atomic ordering is needed.
            const next_capacity = q.queues[old_dequeue.index].len;
            // Reset our to-be-enqueue length
            q.lengths[old_dequeue.index].store(0, .release);
            // We need 'closedness' from the existing enqueue, because this might
            // be a final cleanup pass:
            const new_enqueue: EnqueueToken = new: {
                const enqueue_at_first = q.enqueuing.load(.acquire);
                break :new .{
                    .index = old_dequeue.index,
                    .capacity_remaining = @intCast(next_capacity),
                    .closed = enqueue_at_first.closed,
                };
            };
            // This gets us the latest possible value, any writer who
            // 'happens-after' this is on the dequeue.  `swap-buffer-queue` does a
            // spin CAS here, to prevent overwriting a concurrent close: but we
            // use a reader-only close protocol, so that's not a problem for us.
            // We get the latest possible capacity reservation, and that's what
            // we need.
            const old_enqueue = q.enqueuing.swap(new_enqueue, .acq_rel);
            // Sanity check: we actually swapped the index:
            assert(old_enqueue.index != old_dequeue.index);
            // The swap immediately makes the other queue available, so that's
            // when we notify the writer side:
            if (comptime writer_notified) {
                q.writer_notify.notify();
            }
            // For clarity:
            const new_dq_index = old_enqueue.index;
            const total_capacity = q.queues[new_dq_index].len;
            if (total_capacity == old_enqueue.capacity_remaining) {
                // No writes, commit the dequeue length and bail.
                q.resetDequeue(new_dq_index);
                return null;
            }
            // Our capacity remaining must be less than the total capacity of the slice,
            // now strictly less, since we handled the equal case.
            assert(total_capacity > old_enqueue.capacity_remaining);
            // length + remaining_capacity == capacity when all writes are committed.
            const this_len = q.lengths[new_dq_index].load(.acquire);
            switch (order(this_len + old_enqueue.capacity_remaining, total_capacity)) {
                .lt => {
                    // We have some writes in-flight, deal with it
                    // We expect the exact length, which is: total capacity
                    // - capacity remaining.  We know this won't underflow.
                    const length_expected = total_capacity - old_enqueue.capacity_remaining;
                    return try q.dequeueWait(.eq, old_enqueue.index, @intCast(length_expected));
                },
                .eq => {
                    // We may take our slice, but first, commit the dequeue state.
                    q.resetDequeue(new_dq_index);
                    if (this_len == 0) {
                        unreachable; // We returned null already
                    } else {
                        return q.queues[new_dq_index][0..this_len];
                    }
                },
                .gt => {
                    @panic("length + capacity_remaining is over capacity, this shouldn't happen. transaction error?");
                },
            }
        }

        /// Set the dequeue to the provided index, and its token length-remaining
        /// and tracked q.lengths to zero.
        fn resetDequeue(q: *Queue, dq_index: u1) void {
            const next_dq: DequeueToken = .{ .index = dq_index, .length_remaining = 0 };
            q.dequeuing.store(next_dq, .monotonic);
        }

        /// Wait a while to see if enqueuing will complete in reasonable time, if not, send
        /// DequeuePending.
        fn dequeueWait(q: *Queue, comparison: Compare, index: u1, length_expected: u62) DequeueError![]Cell {
            // We want an exact match after we swapped, and accept greater-than when we
            // return, because we might have re-queued part of our dequeue buffer.  So
            // we pass in a comparison.
            for (0..SPIN_LIMIT) |_| {
                // We're waiting for the reported length to catch up to the
                // expected length (total capacity - capacity remaining).
                // Also, some of the queue may have been consumed, so the length
                // can be greater, not just equal.  Our `comparison` tells which.
                const committed_length = q.lengths[index].load(.acquire);
                if (compare(committed_length, comparison, length_expected)) {
                    // We got it: if length is the same as buffer len it's
                    // the whole slice, otherwise it's what we left from last time.
                    // Fortunately, the math is the same.
                    const start = committed_length - length_expected;
                    const our_slice = q.queues[index][start..committed_length];
                    // Unlock the dequeue state and set it to zero remaining.
                    q.resetDequeue(index);
                    return our_slice;
                } else if (comptime is_debug) {
                    if (comparison == .eq and committed_length > length_expected) { // no-coverage
                        @panic("Buffer length should never exceed stored length after a queue swap");
                    }
                    // If this bug slips through to production, it's (relatively!) harmless,
                    // because we'll accept `>` when we retry.  The correctness issue would
                    // remain, however.
                }
                spinLoopHint();
            }
            // TODO: In principle, we can just take what we have here, right?
            // I'm putting that off until I see how it interacts with requeuing,
            // I think it requires tracking one more variable, but that's fine.

            // Commit our expected length, and tell reader we aren't ready yet.
            const stored_dequeue: DequeueToken = .{ .index = index, .length_remaining = length_expected };
            q.dequeuing.store(stored_dequeue, .monotonic);

            return DequeueError.DequeuePending;
        }

        fn commitEnqueue(q: *Queue, slots: u62, index: u1) void {
            _ = @atomicRmw(u62, q.lengths[index].getPtr(), .Add, slots, .release);
            // All good!
            if (reader_notified) {
                q.reader_notify.notify();
            }
        }

        /// Try to reserve a slice of the queue.  If we succeed, returns both the slice,
        /// and the commit index to write the length at once we've used it.
        fn reserveSliceInternal(q: *Queue, slots: u62) EnqueueError!struct { []Cell, u1 } {
            var this_enqueue: EnqueueToken = undefined;
            var f_minus: usize = 1;
            var back_off: usize = 1;
            var log_backoff: usize = 0;
            // The contention can't fail, so we spin for it, but we can fail
            // to get our space, queue could be closed too.
            const en_q_ours: EnqueueToken = busy: while (true) {
                this_enqueue = q.enqueuing.load(.acquire);
                if (this_enqueue.closed) {
                    return EnqueueError.QueueIsClosed;
                } else if (this_enqueue.capacity_remaining == 0) {
                    if (reader_notified) {
                        // Race prevention:
                        q.reader_notify.notify();
                    }
                    return EnqueueError.QueueIsFull;
                }
                if (this_enqueue.tryReserve(slots)) |en_q_reserve| {
                    if (@cmpxchgWeak(
                        EnqueueToken,
                        q.enqueuing.getPtr(),
                        this_enqueue,
                        en_q_reserve,
                        .release,
                        .monotonic,
                    )) |_| {
                        for (0..back_off) |_| {
                            spinLoopHint();
                        }
                        if (log_backoff <= BACKOFF_LIMIT) { // LTE because we start at fib(2)
                            log_backoff += 1;
                            // Fibonacci back-off (no phase lock, slower gain):
                            const f_next = back_off;
                            back_off = back_off + f_minus;
                            f_minus = f_next;
                        }
                    } else {
                        break :busy en_q_reserve;
                    }
                } else {
                    if (reader_notified) {
                        // Race prevention:
                        q.reader_notify.notify();
                    }
                    return EnqueueError.NotEnoughRoom;
                }
            };
            assert(this_enqueue.index == en_q_ours.index);
            assert(this_enqueue.capacity_remaining - en_q_ours.capacity_remaining == slots);
            // We got our slots! We need a length, so we work backward
            // from our reservation.  Capacity (slice length) never changes
            // on the enqueue, so we can just read this:
            const capacity = q.queues[en_q_ours.index].len;
            // Our allocation starts at:
            const start = capacity - this_enqueue.capacity_remaining;
            return .{ q.queues[en_q_ours.index][start..][0..slots], en_q_ours.index };
        }

        /// Try to reserve up to `slots` on the queue, but take whatever we can get.
        fn reserveUpToSliceInternal(q: *Queue, slots: u62) EnqueueError!struct { []Cell, u1, u62 } {
            var this_enqueue: EnqueueToken = undefined;
            var f_minus: usize = 1;
            var back_off: usize = 1;
            var log_backoff: usize = 0;
            var rem: u62 = 0;
            // The contention can't fail, so we spin for it, but we can fail
            // to get our space, queue could be closed too.
            const en_q_ours: EnqueueToken = busy: while (true) {
                this_enqueue = q.enqueuing.load(.acquire);
                if (this_enqueue.closed) {
                    return EnqueueError.QueueIsClosed;
                } else if (this_enqueue.capacity_remaining == 0) {
                    return EnqueueError.QueueIsFull;
                }
                var en_q_reserve = this_enqueue;
                rem = en_q_reserve.reserveWithRemainder(slots);
                if (rem == slots) return EnqueueError.QueueIsFull;
                if (@cmpxchgWeak(
                    EnqueueToken,
                    q.enqueuing.getPtr(),
                    this_enqueue,
                    en_q_reserve,
                    .release,
                    .monotonic,
                )) |_| {
                    for (0..back_off) |_| {
                        spinLoopHint();
                    }
                    if (log_backoff <= BACKOFF_LIMIT) { // LTE because we start at fib(2)
                        log_backoff += 1;
                        // Fibonacci back-off (no phase lock, slower gain):
                        const f_next = back_off;
                        back_off = back_off + f_minus;
                        f_minus = f_next;
                    }
                } else {
                    break :busy en_q_reserve;
                }
            };
            assert(this_enqueue.index == en_q_ours.index);
            assert(this_enqueue.capacity_remaining - en_q_ours.capacity_remaining == slots - rem);
            // We got our slots! We need a length, so we work backward
            // from our reservation.  Capacity (slice length) never changes
            // on the enqueue, so we can just read this:
            const capacity = q.queues[en_q_ours.index].len;
            // Our allocation starts at:
            const start = capacity - this_enqueue.capacity_remaining;
            return .{ q.queues[en_q_ours.index][start..][0 .. slots - rem], en_q_ours.index, rem };
        }
    };
}

/// Atomic (cache-padded) word representing the enqueue state.
const EnqueueToken = packed struct(u64) {
    /// Which queue is the write queue (enqueue).
    index: u1,
    /// Remaining capacity of the enqueue.
    capacity_remaining: u62,
    /// The queue as a whole is closed.
    closed: bool,

    const LOCK_MASK: u64 = @bitCast(EnqueueToken{ .index = 0, .capacity_remaining = 0, .closed = true });

    /// Create a new EnqueueToken of the provided capacity.  The index is 0,
    /// and it is closed.
    pub fn new(capacity: u64) EnqueueToken {
        return .{ .index = 0, .capacity_remaining = @truncate(capacity), .closed = true };
    }

    /// Try to reserve 'slots' in the queue.  If that much is available,
    /// a token with that reservation is returned.
    pub fn tryReserve(e_state: EnqueueToken, slots: u62) ?EnqueueToken {
        if (slots > e_state.capacity_remaining) return null;
        return .{
            .closed = e_state.closed,
            .capacity_remaining = e_state.capacity_remaining - slots,
            .index = e_state.index,
        };
    }

    // / Mutably reserve slots on the EnqueueToken, returning the number of
    // / requested slots which were not able to be reserved.
    pub fn reserveWithRemainder(e_state: *EnqueueToken, slots: u62) u62 {
        if (slots > e_state.capacity_remaining) {
            const rem = slots - e_state.capacity_remaining;
            e_state.capacity_remaining = 0;
            return rem;
        } else {
            e_state.capacity_remaining -= slots;
            return 0;
        }
    }

    /// Bit-flip the enqueue closed.  Reserved enqueues will complete, but
    /// no further writes will succeed.
    pub fn close(e_state: *EnqueueToken, comptime atomic_order: AtomicOrder) void {
        const uptr: *u64 = @ptrCast(@alignCast(e_state));
        _ = @atomicRmw(u64, uptr, .Or, LOCK_MASK, atomic_order);
    }

    /// Bit-flip the enqueue open.
    pub fn open(e_state: *EnqueueToken, comptime atomic_order: AtomicOrder) void {
        const uptr: *u64 = @ptrCast(@alignCast(e_state));
        _ = @atomicRmw(u64, uptr, .And, ~LOCK_MASK, atomic_order);
    }
};

pub const DequeueToken = packed struct(u64) {
    /// Which index is the dequeue
    index: u1,
    /// How much is _not_ dequeued (up to capacity)
    length_remaining: u62,
    _: u1 = 0,

    pub const DEQUEUE_LOCK: DequeueToken = @bitCast(@as(u64, std.math.maxInt(u64)));

    /// Create a new DequeueToken.
    pub fn new() DequeueToken {
        return .{ .index = 1, .length_remaining = 0 };
    }

    pub fn getUnlocked(dqt: DequeueToken) error{DequeueLocked}!DequeueToken {
        if (dqt == DEQUEUE_LOCK) return DequeErrorSet.DequeueLocked;
        return dqt;
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

        pub fn pad(val: T) CPad {
            return .{ .raw = val };
        }

        /// Obtain a pointer to the raw address
        pub fn getPtr(padded: *CPad) *T {
            return &padded.raw;
        }

        /// Atomically load the padded value using the indicated order.
        pub fn load(padded: *const CPad, comptime atom_order: AtomicOrder) T {
            return @atomicLoad(T, &padded.raw, atom_order);
        }

        /// Atomically store a new value with the indicated order.
        pub fn store(padded: *CPad, val: T, comptime atom_order: AtomicOrder) void {
            @atomicStore(T, &padded.raw, val, atom_order);
        }

        /// Perform a fetchAdd on the padded value with the indicated order.
        pub fn fetchAdd(padded: *CPad, val: T, comptime atom_order: AtomicOrder) T {
            return @atomicRmw(T, &padded.raw, .Add, val, atom_order);
        }

        /// Perform a fetchSub on the padded value with the indicated order.
        pub fn fetchSub(padded: *CPad, val: T, comptime atom_order: AtomicOrder) T {
            return @atomicRmw(T, &padded.raw, .Sub, val, atom_order);
        }

        /// Swap in a new value using the indicated atomic order, returning
        /// the old one.
        pub fn swap(padded: *CPad, new: T, comptime atom_order: AtomicOrder) T {
            return @atomicRmw(T, &padded.raw, .Xchg, new, atom_order);
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

// The 15.2 version of order is inefficient, this produces
// optimal code.
inline fn order(a: anytype, b: anytype) Order {
    if (a < b)
        return .lt
    else if (a > b)
        return .gt
    else
        return .eq;
}

// The 15.2 version of compare is inefficient.  This produces
// optimal code, and you can see why.
inline fn compare(a: anytype, cmp: Compare, b: anytype) bool {
    const ord = @intFromEnum(order(a, b));
    return switch (cmp) {
        .lt => ord < 0,
        .lte => ord <= 0,
        .eq => ord == 0,
        .gte => ord >= 0,
        .gt => ord > 0,
        .neq => ord != 0,
    };
}

test order {
    try expectEqual(.lt, order(3, 4));
    try expectEqual(.eq, order(4, 4));
    try expectEqual(.gt, order(4, 3));
}

test compare {
    try expect(compare(3, .lt, 4));
    try expect(compare(3, .lte, 4));
    try expect(compare(4, .eq, 4));
    try expect(compare(4, .lte, 4));
    try expect(compare(4, .gte, 4));
    try expect(compare(4, .gt, 3));
    try expect(compare(4, .gte, 3));
}

const Compare = std.math.CompareOperator;

/// CPU-efficient verion of std.math.Order.
const Order = enum(i2) {
    lt = -1,
    eq,
    gt,
};

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
    try expectEqual(1, u_p.capacity_remaining);
    const up_now = u_p;
    u_p.close(.monotonic);
    try expect(u_p.closed);
    u_p.open(.monotonic);
    try expect(!u_p.closed);
    try expectEqual(up_now, u_p);
}

test "queue basics" {
    const EzQ = TuQueQue(u8, 128, .{ .notify = .neither, .refcount = false });
    var new_q = try EzQ.create(talloc);
    defer new_q.destroy(talloc);
    const heyyy = "hello world";
    new_q.open();
    new_q.enqueueSlice(heyyy) catch return expect(false);
    try expectEqual(heyyy.len, new_q.availableDequeues());
    const q_idx1 = new_q.enqueuing.raw.index;
    const hi = new_q.dequeueAvailable() catch return expect(false);
    const q_idx2 = new_q.enqueuing.raw.index;
    try expectEqual(0, new_q.availableDequeues());
    try expect(q_idx1 != q_idx2);
    try expectEqualStrings("hello world", hi.?);
    new_q.close();
}

test "queue full" {
    const SmolQ = TuQueQue(u8, 6, .{ .notify = .neither, .refcount = false });
    var new_q = try SmolQ.create(talloc);
    defer new_q.destroy(talloc);
    new_q.open();
    defer new_q.close();
    try expectEqual(6, new_q.queueTotalCapacity(.both));
    try expectEqual(3, new_q.queueTotalCapacity(.enqueue));
    try expectEqual(3, new_q.queueTotalCapacity(.dequeue));
    new_q.enqueueSlice("ab") catch return expect(false);
    try expectError(EnqueueErrorSet.NotEnoughRoom, new_q.enqueueSlice("cd"));
    try expect(!new_q.queueIsFull());
    try expectEqual(1, new_q.remainingCapacity());
    new_q.enqueueSlice("c") catch return expect(false);
    try expectError(EnqueueErrorSet.QueueIsFull, new_q.enqueueSlice("d"));
    try expectEqual(0, new_q.remainingCapacity());
    try expect(new_q.queueIsFull());
    try expectEqualStrings("abc", (new_q.dequeueAvailable() catch return expect(false)).?);
    try expectEqual(3, new_q.remainingCapacity());
}

test "WriteHandle" {
    const SmolQ = TuQueQue(u8, 6, .{ .notify = .neither, .refcount = true });
    var q = try SmolQ.create(talloc);
    var danger_zone = true;
    defer {
        if (danger_zone) {
            q.errDestroy(talloc); // no-coverage
        } else {
            q.destroy(talloc);
        }
    }
    q.open();
    try expectEqual(6, q.queueTotalCapacity(.both));
    try expectEqual(3, q.queueTotalCapacity(.enqueue));
    try expectEqual(3, q.queueTotalCapacity(.dequeue));
    var wh = q.writeHandle();
    try expectEqual(1, q.writeHandlesRemaining());
    wh.enqueueSlice("ab") catch return expect(false);
    try expectError(EnqueueErrorSet.NotEnoughRoom, q.enqueueSlice("cd"));
    try expect(!q.queueIsFull());
    try expectEqual(1, q.remainingCapacity());
    wh.enqueueValue('c') catch return expect(false);
    try expectError(EnqueueErrorSet.QueueIsFull, q.enqueueSlice("d"));
    try expectEqual(0, q.remainingCapacity());
    try expect(q.queueIsFull());
    try expectEqualStrings("abc", (q.dequeueAvailable() catch return expect(false)).?);
    try expectEqual(3, q.remainingCapacity());
    try expectEqual(null, (q.dequeueAvailable() catch return expect(false)));
    const slice = try wh.reserveSlice(3);
    @memcpy(slice, "def");
    wh.commit();
    try expect(wh.queueIsFull());
    try expectEqualStrings("def", (q.dequeueAvailable() catch return expect(false)).?);
    const remains = wh.enqueueUpToSlice("ghij") catch return expect(false);
    try expectEqualStrings("j", remains);
    try expect(wh.queueIsFull());
    const ghi = (q.dequeueAvailable() catch return expect(false)).?;
    try expectEqualStrings("ghi", ghi);
    q.requeueAfterIndex(1);
    const hi = (q.dequeueAvailable() catch return expect(false)).?;
    try expectEqualStrings("hi", hi);
    wh.enqueueValue('j') catch return expect(false);
    q.close();
    try expect(wh.queueIsClosed());
    try expectEqualStrings("j", (q.dequeueAvailable() catch return expect(false)).?);
    try expectEqual(EnqueueErrorSet.QueueIsClosed, wh.enqueueValue('?'));
    try expectEqual(EnqueueErrorSet.QueueIsClosed, q.enqueueValue('?'));
    try expectEqual(EnqueueErrorSet.QueueIsClosed, q.enqueueUpToSlice("????"));
    try expect(wh.queueIsClosed());
    q.open();
    const no_remainder = wh.enqueueUpToSlice("kl") catch return expect(false);
    try expectEqualStrings("", no_remainder);
    wh.release();
    q.close();
    try expectEqual(0, q.writeHandlesRemaining());
    danger_zone = false;
}

fn threadSmokeTests(
    thread_count: comptime_int,
    queue_cap: comptime_int,
    comptime which: []const u8,
) !void {
    const Message = struct {
        want: u64,
        got: *u64,
    };

    const Q64 = TuQueQue(Message, queue_cap, .{ .notify = .reader, .refcount = true });

    const KnockKnock = struct {
        want: u64,
        got: u64 = 0,
        wh: Q64.WriteHandle,

        const KnK = @This();

        pub fn new(want: u64, wh: Q64.WriteHandle) KnK {
            return .{ .want = want, .wh = wh };
        }

        pub fn enqueue(knocker: *KnK) void {
            // Wait some random amount of time
            const waiting = knocker.want % 256;
            sleep(waiting * 100); // up to 25 ms, seems reasonable
            const msg: Message = .{ .want = knocker.want, .got = &knocker.got };
            while (true) {
                if (knocker.wh.enqueueValue(msg)) |_| {
                    while (true) {
                        sleep(50);
                        const got = @atomicLoad(u64, &knocker.got, .monotonic);
                        if (got == knocker.want) {
                            knocker.wh.release();
                            return;
                        } else if (got != 0) { // no-coverage
                            logger.err("enqueue: knocker wanted {d} and got {d}\n", .{ knocker.want, knocker.got }); // no-coverage
                            knocker.wh.release(); // no-coverage
                            return; // no-coverage
                        }
                    }
                } else |err| {
                    switch (err) {
                        Q64.EnqueueError.NotEnoughRoom,
                        Q64.EnqueueError.QueueIsFull,
                        => {
                            sleep(100);
                        },
                        Q64.EnqueueError.QueueIsClosed => @panic("queue closed before knock-write"),
                    }
                }
            }
        }

        pub fn accumulate(knocker: *KnK) void {
            sleep(1000); // Give things a chance to catch up.
            const msg: Message = .{ .want = knocker.want, .got = &knocker.got };
            work: while (@atomicLoad(u64, &knocker.got, .monotonic) < knocker.want) {
                var slice: []const Message = &.{ msg, msg, msg };
                if (knocker.wh.enqueueUpToSlice(slice)) |rem| {
                    if (rem.len > 0) {
                        slice = rem;
                    } else {
                        continue :work;
                    }
                } else |err| {
                    switch (err) {
                        Q64.EnqueueError.NotEnoughRoom,
                        Q64.EnqueueError.QueueIsFull,
                        => {
                            sleep(100);
                        },
                        Q64.EnqueueError.QueueIsClosed => @panic("queue closed before knock-write"),
                    }
                }
            }
            knocker.wh.release();
        }

        pub fn reserve(knocker: *KnK) !void {
            sleep(1000); // Give things a chance to catch up.
            const msg: Message = .{ .want = knocker.want, .got = &knocker.got };
            while (true) {
                if (knocker.wh.reserveSlice(1)) |slice| {
                    {
                        try expect(knocker.wh.reserve.something);
                        defer knocker.wh.commit();
                        // Rudely hold on to the reservation.
                        sleep(150);
                        slice[0] = msg;
                    }
                    try expect(!knocker.wh.reserve.something);
                    while (true) {
                        sleep(50);
                        const got = @atomicLoad(u64, &knocker.got, .monotonic);
                        if (got == knocker.want) {
                            knocker.wh.release();
                            return;
                        } else if (got != 0) { // no-coverage
                            logger.err("reserve: knocker wanted {d} and got {d}\n", .{ knocker.want, knocker.got }); // no-coverage
                            knocker.wh.release(); // no-coverage
                            return; // no-coverage
                        }
                    }
                } else |err| {
                    switch (err) {
                        Q64.EnqueueError.NotEnoughRoom,
                        Q64.EnqueueError.QueueIsFull,
                        => {
                            sleep(100);
                        },
                        Q64.EnqueueError.QueueIsClosed => @panic("queue closed before knock-write"),
                    }
                }
            }
        }

        pub fn run(knocker: *KnK) !void {
            return @field(KnK, which)(knocker);
        }
    };

    const WhoThere = struct {
        knock_count: usize = 0,
        q: *Q64,

        const Wat = @This();

        pub fn enqueue(wat: *Wat) !void {
            dequeue: while (true) {
                if (wat.q.dequeueWithTimeout(10)) |m_knocks| {
                    if (m_knocks) |knocks| {
                        for (knocks) |knock| {
                            @atomicStore(u64, knock.got, knock.want, .monotonic);
                            wat.knock_count += 1;
                        }
                        if (wat.knock_count == thread_count) {
                            wat.q.close();
                            break :dequeue;
                        } else if (wat.knock_count > thread_count) {
                            return expectEqualStrings("More knocks", "than threads");
                        }
                    } else {
                        try Thread.yield();
                    }
                } else |err| {
                    switch (err) {
                        DequeErrorSet.DequeueNotEmpty => return expect(false),
                        DequeErrorSet.DequeuePending => {
                            sleep(10);
                        },
                    }
                }
            }
            try expectEqual(null, wat.q.dequeueAvailable() catch return expect(false));
            sleep(1000);
            try expectEqual(null, wat.q.dequeueAvailable() catch return expect(false));
            // Releases can't be counted on until after join
            return;
        }

        pub fn accumulate(wat: *Wat) !void {
            dequeue: while (true) {
                if (wat.q.dequeueAvailable()) |m_knocks| {
                    if (m_knocks) |knocks| {
                        for (knocks) |knock| {
                            const got = @atomicLoad(u64, knock.got, .monotonic);
                            if (got < knock.want) {
                                _ = @atomicRmw(u64, knock.got, .Add, 1, .monotonic);
                            }
                        }
                    } else {
                        if (wat.q.writeHandlesRemaining() == 0) break :dequeue;
                        try Thread.yield();
                    }
                } else |err| {
                    switch (err) {
                        DequeErrorSet.DequeueNotEmpty => return expect(false),
                        DequeErrorSet.DequeuePending => {
                            sleep(10);
                        },
                    }
                }
            }
            wat.q.close();
            try expectEqual(null, wat.q.dequeueAvailable() catch return expect(false));
            sleep(1000);
            try expectEqual(null, wat.q.dequeueAvailable() catch return expect(false));
            return;
        }

        pub const reserve = enqueue;

        pub fn run(wat: *Wat) !void {
            return @field(Wat, which)(wat);
        }
    };

    var prng = std.Random.DefaultPrng.init(blk: {
        var seed: u64 = undefined;
        try std.posix.getrandom(std.mem.asBytes(&seed));
        break :blk seed;
    });
    const rand = prng.random();

    var danger_zone: bool = true;
    var knockers: [thread_count]KnockKnock = undefined;
    const q = try Q64.create(talloc);
    defer {
        if (danger_zone) {
            q.errDestroy(talloc); // no-coverage
        } else {
            q.destroy(talloc);
        }
    }
    var wat: WhoThere = .{ .q = q, .knock_count = 0 };
    for (0..thread_count) |i| {
        const want: usize = roll: {
            while (true) {
                // Evade once-in-history spurious successes:
                const have = if (std.mem.eql(u8, which, "accumulate")) 100 else rand.int(u64);
                if (have != 0) break :roll have;
            }
        };
        knockers[i] = .{ .wh = wat.q.writeHandle(), .want = want, .got = 0 };
    }
    wat.q.open();
    var threads: [thread_count]Thread = undefined;
    for (0..thread_count) |i| {
        threads[i] = try Thread.spawn(
            .{ .stack_size = 1024 * 128 },
            KnockKnock.run,
            .{&knockers[i]},
        );
    }
    try wat.run();
    for (0..thread_count) |i| {
        threads[i].join();
    }

    for (0..thread_count) |i| {
        try expectEqual(knockers[i].want, knockers[i].got);
    }

    danger_zone = false;
}

test "smoke tests" {
    try threadSmokeTests(4, 16, "enqueue");
    try threadSmokeTests(256, 32, "enqueue");
    try threadSmokeTests(256, 4, "enqueue");
    try threadSmokeTests(4, 16, "reserve");
    try threadSmokeTests(256, 32, "reserve");
    try threadSmokeTests(256, 4, "reserve");
    try threadSmokeTests(4, 8, "accumulate");
    try threadSmokeTests(128, 8, "accumulate");
}

const fortuna =
    \\We the unwilling, led by the ungrateful, are doing the impossible.
    \\We've done so much, for so long, with so little,
    \\that we are now qualified to do something with nothing.
    \\
** 10;

fn producerConsumer(queue_cap: comptime_int, stride: comptime_int) !void {
    if (stride > queue_cap * 2) @compileError("stride must fit in half queue");
    const PcQ = TuQueQue(u8, queue_cap, .{ .notify = .both, .refcount = false, .secure_erase = true });

    var buf: [fortuna.len]u8 = undefined;

    const Reader = struct {
        q: *PcQ,
        i: usize = 0,

        const ReadMe = @This();

        pub fn run(r: *ReadMe, buffer: []u8) !void {
            dequeue: while (true) {
                if (r.q.dequeueWithTimeout(1000) catch continue :dequeue) |next| {
                    if (r.i + next.len <= buffer.len) {
                        @memcpy(buffer[r.i..][0..next.len], next);
                    }
                    r.i += next.len;
                    if (r.i >= buffer.len) {
                        break :dequeue;
                    }
                }
            }
            r.q.close();
        }
    };

    const Writer = struct {
        wh: PcQ.WriteHandle,
        i: usize = 0,
        w_note: *Notifier,

        const WriteMe = @This();

        pub fn run(w: *WriteMe) !void {
            var flip: usize = 0;
            enqueue: while (true) {
                defer flip += 1;
                const cut = @min(fortuna.len - w.i, stride);
                var next = fortuna[w.i..][0..cut];
                if (next.len == 0) break :enqueue;
                if (flip % 2 == 0) {
                    w.wh.enqueueSlice(next) catch |err| {
                        switch (err) {
                            PcQ.EnqueueError.QueueIsFull,
                            => {
                                w.w_note.timedWait(1000);
                                continue :enqueue;
                            },
                            PcQ.EnqueueError.NotEnoughRoom => {
                                w.w_note.timedWait(1000);
                                continue :enqueue;
                            },
                            PcQ.EnqueueError.QueueIsClosed => return,
                        }
                    };
                } else {
                    drain: while (next.len > 0) {
                        next = w.wh.enqueueUpToSlice(next) catch |err| {
                            switch (err) {
                                PcQ.EnqueueError.QueueIsFull,
                                => {
                                    w.w_note.timedWait(1000);
                                    continue :drain;
                                },
                                PcQ.EnqueueError.NotEnoughRoom => unreachable,
                                PcQ.EnqueueError.QueueIsClosed => return,
                            }
                        };
                    }
                }
                w.i += cut;
                if (w.i >= fortuna.len) break :enqueue;
            }
            w.wh.notifyReader();
        }
    };

    var q = try PcQ.create(talloc);
    var danger_zone = true;
    defer {
        if (danger_zone) {
            q.errDestroy(talloc); // no-coverage
        } else {
            q.destroy(talloc);
        }
    }
    q.open();
    var read: Reader = .{ .q = q };
    var write: Writer = .{ .wh = q.writeHandle(), .w_note = q.writeNotifier() };
    var r_thread = try Thread.spawn(.{ .stack_size = 1024 * 128 }, Reader.run, .{ &read, &buf });
    var w_thread = try Thread.spawn(.{ .stack_size = 1024 * 128 }, Writer.run, .{&write});
    r_thread.join();
    w_thread.join();
    write.wh.release();
    q.close();
    try expectEqualStrings(fortuna, &buf);
    danger_zone = false;
}

test "producer/consumer" {
    try producerConsumer(512, 17);
    try producerConsumer(256, 17);
    try producerConsumer(128, 17);
    try producerConsumer(64, 17);
    try producerConsumer(64, 31);
    try producerConsumer(64, 32);
}

fn waitSlam(threads: comptime_int, capacity: comptime_int, count: comptime_int) !void {
    const QSlam = TuQueQue(bool, capacity, .{ .notify = .reader, .secure_erase = true, .refcount = true });

    var threadies: [threads]Thread = undefined;
    var handles: [threads]QSlam.WriteHandle = undefined;

    const Slammed = struct {
        q: *QSlam,
        counter: usize = 0,

        pub fn run(slam: *@This()) !void {
            slam.q.wait();
            slam.q.timedWait(10);
            while (slam.counter < count) {
                for (slam.q.dequeue()) |_| {
                    slam.counter += 1;
                }
            }
            slam.q.close();
        }

        pub fn wh_run(wh: *QSlam.WriteHandle) !void {
            defer wh.release();
            while (true) {
                if (wh.enqueueValue(true)) |_| {} else |err| {
                    switch (err) {
                        QSlam.EnqueueError.QueueIsFull, QSlam.EnqueueError.NotEnoughRoom => {},
                        QSlam.EnqueueError.QueueIsClosed => return,
                    }
                }
            }
        }
    };
    var slammer: Slammed = .{ .q = try QSlam.create(talloc) };
    slammer.q.open();
    var danger_zone = true;
    defer {
        if (danger_zone) {
            slammer.q.errDestroy(talloc); // no-coverage
        } else {
            slammer.q.destroy(talloc);
        }
    }
    for (0..threads) |i| {
        handles[i] = slammer.q.writeHandle();
        threadies[i] = try Thread.spawn(.{ .stack_size = 1024 * 128 }, Slammed.wh_run, .{&handles[i]});
    }

    const r_handle = try Thread.spawn(.{ .stack_size = 1024 * 128 }, Slammed.run, .{&slammer});

    for (0..threads) |i| {
        threadies[i].join();
    }
    r_handle.join();

    while (slammer.q.writeHandlesRemaining() > 0) {
        sleep(5); // no-coverage
    }
    try expect(true);

    danger_zone = false;
}

test waitSlam {
    try waitSlam(128, 20, 100);
    try waitSlam(256, 20, 10000);
}

fn allocTest(allocator: Allocator) !void {
    // TODO: re-allocate the queues
    const SmolQ = TuQueQue(u8, 6, .{ .notify = .neither, .refcount = false });
    var new_q = try SmolQ.create(allocator);
    defer new_q.errDestroy(allocator);
    new_q.open();
    const dummy = try allocator.create(usize);
    defer allocator.destroy(dummy);
    defer new_q.close();
}

test "allocation failure" {
    try testing.checkAllAllocationFailures(talloc, allocTest, .{});
}

// 19th Fibonacci is 4181, seems reasonable: analogous to 2^12, 4096
const BACKOFF_LIMIT = if (is_test) 3 else 19;
const SPIN_LIMIT = if (is_test) 1 else 64;

const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;

const builtin = @import("builtin");
const is_debug = builtin.mode == .Debug;
const is_safe = is_debug or builtin.mode == .ReleaseSafe;
const is_test = builtin.is_test;

const logger = std.log.scoped(.tuqueue);

const AtomicOrder = std.builtin.AtomicOrder;
const cache_line = std.atomic.cache_line;
const cache_align = std.mem.Alignment.fromByteUnits(cache_line);
const spinLoopHint = std.atomic.spinLoopHint;
const Atom = std.atomic.Value; // I prefer it ¯\_(ツ)_/¯

const Thread = std.Thread;
const Condition = Thread.Condition;
const Mutex = Thread.Mutex;
const sleep = Thread.sleep;

const testing = std.testing;
const expect = testing.expect;
const expectEqual = testing.expectEqual;
const expectEqualStrings = testing.expectEqualStrings;
const expectError = testing.expectError;
const talloc = testing.allocator;
