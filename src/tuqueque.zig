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
//! blocks, no false sharing will occur.
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
//! TODO: guess I should provide an API for that, huh.
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
//! prevents reader-side reëntrance: if you see a `LockedDequeue` error, that's
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
        note.cond.timedWait(&note.mut, timeout_ns);
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
    /// The queue is locked, which only happens during a dequeue.
    /// The single-reader principle has been violated.
    DequeueLocked,
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
    _ = queue_bytes;

    const half_cap = queue_capacity >> 1;

    // Inconveniently-short queues are useful for testing, not in production.
    const start_cap = if (is_test) half_cap else alignSizeTo(half_cap, cache_line);

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
        /// The lengths of committed writes.  Acquire-release so that read values
        /// are always current.  Add-only (except on reset), so no contention here.
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
        writer_refs: if (refcounted) CachePad(usize) else void,

        const Queue = @This();

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
            _ = q.getAndLockDequeue() catch @panic("Queue left in locked state");
            if (comptime refcounted) {
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
                _ = q.writer_refs.fetchAdd(1, .monotonic);
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
        pub fn writeNotifier(q: *Queue) Notifier {
            if (!writer_notified) @compileError("Queue is not configured with a write notifier");
            return q.writer_notify;
        }

        /// Try a dequeue-and-swap.  This can be called whether or not the last
        /// one timed out.  It returns `null` if there are no writes to read, or
        /// a `DequeueError` if we spun for too long waiting, or the queue was
        /// already dequeuing (should never happen!), otherwise, it returns the
        /// message-containing slice from the queue.  This slice is only valid
        /// until the next call to `tryDequeue`, although some can be requeued
        /// if needed (see `requeueFromIndex`).
        pub fn tryDequeue(q: *Queue) !?[]Cell {
            // Invariant: our implementation always unlocks the dequeue when
            // the queue isn't actively accessed from the reader side (this differs
            // from the original).
            const dequeue_unlocked = try q.getAndLockDequeue();
            defer if (is_debug) {
                assert(q.dequeuing.load(.monotonic) != DequeueToken.DEQUEUE_LOCK);
            };
            return q.tryDequeueInternal(dequeue_unlocked);
        }

        /// Dequeue messages from the queue, waiting on the Notifier as long as
        /// necessary.  This function is a compile error if reader notification is
        /// not configured.  To allow it to be error-free, this panics if it sees
        /// DequeueLocked (which should never happen).
        pub fn dequeue(q: *Queue) []Cell {
            if (!reader_notified) @compileError("This queue doesn't notify readers, use tryDequeue");
            if (q.tryDequeue()) |maybe_cells| if (maybe_cells) |cells| {
                return cells;
            } else { // Nothing yet, so we wait
                q.reader_notify.wait();
                return q.dequeue();
            } else |err| {
                switch (err) {
                    DequeueError.DequeuePending => {
                        // Read not completed; we wait
                        q.reader_notify.wait();
                        return q.dequeuing();
                    },
                    DequeueError.DequeueLocked => @panic("Dequeue was locked within q.dequeue"),
                }
            }
        }

        /// Tries to dequeue, waits `timeout_ns` for a signal, then tries again with
        /// `tryDequeue`.  This is a compile error without reader notification configured.
        pub fn dequeueWithTimeout(q: *Queue, timeout_ns: usize) !?[]Cell {
            if (!reader_notified) @compileError("This queue doesn't notify readers, use tryDequeue");
            if (q.tryDequeue()) |maybe_cells| if (maybe_cells) |cells| {
                return cells;
            } else { // Nothing yet, so we wait
                q.reader_notify.timedWait(timeout_ns);
                return q.tryDequeue();
            } else |err| {
                switch (err) {
                    DequeueError.DequeuePending => {
                        // Read not completed; we wait
                        q.reader_notify.timedWait(timeout_ns);
                        return q.tryDequeue();
                    },
                    DequeueError.DequeueLocked => |e| return e,
                }
            }
        }

        /// If you need to requeue some of a slice, you can.  If the
        /// unused portion of the slice would be `cells[i..]`, call
        /// `try q.requeueAfterIndex(i)`, and you'll get it back on
        /// the next dequeue.  Asserts that `i` is `<=` the length of
        /// the returned slice, the only error is if the queue is locked.
        pub fn requeueAfterIndex(q: *Queue, index: usize) error{LockedDequeue}!void {
            var the_dequeue = try q.getAndLockDequeue();
            the_dequeue.length_remaining += index;
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

        /// Return the number of available Cells on a best-effort,
        /// monotonic basis.  There is no guarantee that the next dequeue
        /// will return all of these messages, and it may return more.
        /// Tries may error out due to DequeuePending, possibly more than
        /// once.  A persistent reader will see this many messages after
        /// enough diligence, and before that happens, a blocking read
        /// with `dequeue` will not sleep on empty, just pending.
        pub fn available(q: *Queue) usize {
            const the_dequeue = q.getAndLockDequeue() catch @panic("Locked dequeue in available()");
            defer q.dequeuing.store(the_dequeue, .monotonic);
            const the_enqueue = q.enqueuing.load(.monotonic);
            const capacity = q.queues[the_enqueue.index].len;
            return (capacity - the_enqueue.capacity_remaining) + the_dequeue.length_remaining;
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

        pub fn queueCapacity(q: *Queue, which: WhichQueue) usize {
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
            const the_dequeue = try q.getAndLockDequeue();
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
            return q.enqueueSlice(q, &v_slice);
        }

        /// Attempt to enqueue a slice of Cell values.  Errors if there isn't room,
        /// does not attempt to write some of the slice if there isn't room for
        /// all of it.
        pub fn enqueueSlice(q: *Queue, vals: []const Cell) EnqueueError!void {
            if (vals.len >= std.math.maxInt(u62)) return EnqueueError.NotEnoughRoom; // I mean...
            const v_len: u62 = @intCast(vals.len);
            const our_slice, const index = try q.reserveSliceInternal(v_len);
            @memcpy(our_slice, vals); // This asserts len == len for us
            // Now commit:
            q.commitEnqueue(v_len, index);
            return;
        }

        //| Private

        /// Correct use of this function involves the WriteHandle directly.  Rather than mess around
        /// with a reader-side-specific version of this function, which probably won't be needed,
        /// readers can always create a WriteHandle and call it that way.
        fn reserveSlice(q: *Queue, slots: usize) EnqueueError!struct { WriteHandle.Reservation, []Cell } {
            const our_slice, const index = try q.reserveSliceInternal(slots);
            return .{ WriteHandle.Reservation.some(index, slots), our_slice };
        }

        // Loads the dequeue token, erroring out if it's in the locked state.
        // If not, it swaps the locked state in, and returns the current
        // (actual) dequeue state to the caller.
        fn getAndLockDequeue(q: *Queue) error{LockedDequeue}!DequeueToken {
            const the_dequeue = q.dequeuing.load(.monotonic);
            if (the_dequeue == DequeueToken.DEQUEUE_LOCK) return error.LockedDequeue;
            return q.dequeuing.swap(DequeueToken.DEQUEUE_LOCK, .monotonic);
        }

        fn tryDequeueInternal(q: *Queue, old_dequeue: DequeueToken) DequeueError!?[]Cell {
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
                    @memset(q.queues[old_dequeue.index], 0xaa);
                }
            }
            // Get the capacity (resizing means this can change from half_cap).
            // This 'side' is never contended, and only we can change it, so no
            // atomic ordering is needed.
            const next_capacity = q.queues[old_dequeue.index].len;
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
                    return try q.dequeueWait(
                        .eq,
                        old_enqueue.index,
                        @intCast(total_capacity - old_enqueue.capacity_remaining),
                    );
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
            q.lengths[dq_index].store(0, .release);
            const next_dq: DequeueToken = .{ .index = dq_index, .length_remaining = 0 };
            q.dequeuing.store(next_dq, .monotonic);
        }

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
                    if (comparison == .eq and committed_length > length_expected) {
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
            var back_off: usize = 1;
            var log_backoff: usize = 1;
            // The contention can't fail, so we spin for it, but we can fail
            // to get our space, queue could be closed too.
            const en_q_ours: EnqueueToken = busy: while (true) {
                this_enqueue = q.enqueuing.load(.acquire);
                if (this_enqueue.closed) {
                    return EnqueueError.QueueIsClosed;
                } else if (this_enqueue.capacity_remaining == 0) {
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
                        if (log_backoff < BACKOFF_LIMIT) {
                            log_backoff += 1;
                            back_off *= 2;
                        }
                    } else {
                        break :busy en_q_reserve;
                    }
                } else {
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

            /// Try to enqueue a slice of values onto the queue.  All will be
            /// written, or none will be.
            pub fn enqueueSlice(wh: *WriteHandle, vals: []Cell) EnqueueError!void {
                const q: *Queue = @ptrCast(@alignCast(wh.ptr));
                return q.enqueueSlice(vals);
            }

            /// Try to reserve a slice of length `slots`.  When returned, there
            /// is an open transaction, and the reader cannot dequeue until the
            /// write is done and the WriteHandle calls `wh.commit()`.  Only one
            /// slot can be reserved at a time by any given writer.
            ///
            /// Use of the function can leak prior contents of the queue, which
            /// can be a security risk in some applications.  If configured with
            /// the `secure_erase` option, the slice will always be filled with
            /// zeroes before returning it.
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
                var q: *Queue = @ptrCast(@alignCast(wh.ptr));
                defer if (comptime reader_notified) q.reader_notify.notify();
                if (comptime refcounted) {
                    const left = q.writer_refs.fetchSub(1, .release);
                    if (left == 1) {
                        _ = q.writer_refs.load(.acquire);
                    } else if (is_safe and left == std.math.maxInt(usize)) {
                        @panic("Writer handle reference count underflow, too many releases");
                    }
                }
                wh.ptr = undefined;
            }
        };
    };
}

pub const EnqueueToken = packed struct(u64) {
    index: u1, // Which queue is writing
    capacity_remaining: u62, // Remaining capacity of that queue
    closed: bool, // The queue as a whole is closed

    const LOCK_MASK = (EnqueueToken{ .index = 0, .capacity_remaining = 0, .closed = true }).toWord();

    /// Create a new EnqueueToken of the provided capacity.  The index is 0,
    /// and it is closed.
    pub fn new(capacity: u64) EnqueueToken {
        return .{ .index = 0, .capacity_remaining = @truncate(capacity), .closed = true };
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
    pub fn tryReserve(e_state: EnqueueToken, slots: u62) ?EnqueueToken {
        if (slots > e_state.capacity_remaining) return null;
        return .{
            .closed = e_state.closed,
            .capacity_remaining = e_state.capacity_remaining - slots,
            .index = e_state.index,
        };
    }

    /// Reserve up to 'slots' in the queue, getting back what may be as little
    /// as none.  TODO: maybe this should error on none?
    pub fn reserveUpTo(e_state: EnqueueToken, slots: u64) EnqueueToken {
        return .{
            .closed = e_state.closed,
            .capacity_remaining = if (e_state.capcity > slots) e_state.capacity_remaining - slots else 0,
            .index = e_state.index,
        };
    }

    pub fn close(e_state: *EnqueueToken, comptime atomic_order: AtomicOrder) void {
        const uptr: *u64 = @ptrCast(@alignCast(e_state));
        _ = @atomicRmw(u64, uptr, .Or, LOCK_MASK, atomic_order);
    }

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

    pub fn getUnlocked(dqt: DequeueToken) error{LockedDequeue}!DequeueToken {
        if (dqt == DEQUEUE_LOCK) return error.LockedDequeue;
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
    const EzQ = TuQueQue(u8, 128, .{ .notify = .neither, .refcount = false, .secure_erase = false });
    var new_q = try EzQ.create(talloc);
    defer new_q.destroy(talloc);
    const heyyy = "hello world";
    new_q.open();
    new_q.enqueueSlice(heyyy) catch return expect(false);
    try expectEqual(heyyy.len, new_q.available());
    const q_idx1 = new_q.enqueuing.raw.index;
    const hi = new_q.tryDequeue() catch return expect(false);
    const q_idx2 = new_q.enqueuing.raw.index;
    try expectEqual(0, new_q.available());
    try expect(q_idx1 != q_idx2);
    try expectEqualStrings("hello world", hi.?);
    new_q.close();
}

const BACKOFF_LIMIT = 12; // Eventually configurable via option (for thread-san)
const SPIN_LIMIT = 64; // Likewise

const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;

const builtin = @import("builtin");
const is_debug = builtin.mode == .Debug;
const is_safe = is_debug or builtin.mode == .ReleaseSafe;
const is_test = builtin.is_test;

const AtomicOrder = std.builtin.AtomicOrder;
const cache_line = std.atomic.cache_line;
const cache_align = std.mem.Alignment.fromByteUnits(cache_line);
const spinLoopHint = std.atomic.spinLoopHint;
const Atom = std.atomic.Value; // I prefer it ¯\_(ツ)_/¯

const Condition = std.Thread.Condition;
const Mutex = std.Thread.Mutex;
const testing = std.testing;
const expect = testing.expect;
const expectEqual = testing.expectEqual;
const expectEqualStrings = testing.expectEqualStrings;
const talloc = testing.allocator;
