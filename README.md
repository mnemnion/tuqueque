# Tuqueque

This module provides `TuQueQue`[^†], a lock-free, fallible MPSC queue
of fixed size.  Lock-free means that writes and reads both proceed
without the use of a mutex, or any other lock of indefinite duration.
Fallible means that enqueues and dequeues can both fail.  MPSC is
multiple producer, single consumer: many writers and only one reader.
Fixed size is why writes can fail.  Writes are why dequeues can fail.

The mechanism is simple and efficient: swap buffers.  At any given
instant, one buffer is the write queue (the enqueue), the other is the
read queue (the dequeue).  Writers contend to reserve queue space,
using a single atomic word, write to their reservation, then commit
the write length.  This allows them to leapfrog each other — until the
queue is full.

At any instant, the reader (singular!) can dequeue, by swapping which
queue is which.  Writes may not be committed, in which case the reader
must wait until they are; this can time out.

Once it succeeds, the reader has a slice of whatever the queue carries.
It can do reader stuff with the slice, and (after reading, please!)
resize the dequeue half of the tuqueque, if desired.  It is not a
correctness issue for the halves to be unbalanced, temporarily or
otherwise.

Tuned appropriately, this is about as good as it gets.  All common
memory is handled atomically, and cache-isolated to eliminate false
sharing.

The most important invariant is that there **must** only be one
reader.  To facilitate this, writes are expected to proceed using a
`WriteHandle`, a type-erased pointer to the queue exposing only the
write-appropriate functions (and a bit of bookkeeping, it's two words
wide).

The library maintains a read lock and will panic in safe modes if two
reader-side functions ever overlap.  Seriously, don't do it.  There are
more ways to get the queue into an inconsistent state that way than the
assertions can detect and surface.

This library has 100% line coverage, but is hot off the presses and yet
to be used in anger[^‡].  Caveat hacker.

## Inspiration

`TuQueQue` is a port of a Rust library, [`swap-buffer-queue`][sbq],
many thanks to @wyfo!  It makes some different choices, in a way which
will be comprehensible to anyone familiar with both languages: the Zig
version puts somewhat more responsibility for correct use in the hands
of the user, and is able to pick up some small efficiencies in return.
In principle, at least: in addition to `NO WARRANTY`, `tuqueque` comes
with no benchmarks.

## Installation

<tk blah blah>

## Use

It's a queue, you queue things with it.

```zig
test MyQueue {
    var q = try MyQueue.create(allocator);
    errdefer q.errDestroy(allocator); // Doesn't check for e.g. closed queue
    q.open();
    q.enqueueValue('H') catch {}; // {} is two kinds of not-enough-room
    q.enqueueSlice("ello!") catch {};
    try testing.expectEqualStrings("Hello!", (q.dequeueAvailable() catch unreachable).?);
    q.close();
    q.destroy(allocator); // Does check
}
```

Unless resizing one side or the other of the queues, no allocation is
performed while the queue is in use.  Queues have object identity[^✢] and
will be pinned addresses when Zig gains this ability.

Notice that the queue is created in the closed state, and must be opened
before writes will be accepted.

**Only one reader**.

### Reading

A simple read loop might look something like this:

```zig
dequeue: while (true) {
    if (reader.q.dequeueAvailable()) |maybe_msgs| {
        if (msgs) |msgs| {
            for (msgs) |msg| {
                reader.doMessageThing(msg);
            }
            continue :dequeue;
        } else {
            // Nothing in the queue for the last millisecond
            reader.doSomethingElse();
            if (reader.isDone()) break :dequeue;
            continue :dequeue;
        }
    } else |err| {
        // Means writes haven't completed in time
        assert(err == error.DequeuePending);
        sleep(10);
        continue :dequeue;
    }
}
```

Or, with reader notification configured (the default):

```zig
dequeue: while (true) {
    if (reader.q.dequeueWithTimeout(1000) catch continue :dequeue) |msgs| {
        for (msgs) |msg| {
            reader.doMessageThings(msg);
        }
        if (reader.done()) break :dequeue;
    }
}
```

There is also `q.dequeueWait()`, which will block until it sees
messages.  Remember that it's always possible to check the queue, find
nothing, and block, while missing the notification from a concurrent
write!  Timeouts are to be preferred.  A writer can also ping the reader
with `w.notifyReader()`, and vice versa.

Writer notification is another configurable option (off by default),
but no "write or block" methods are provided.  This is an MPSC system,
which assumes many producers.  Blocking would be easy, but it would
most likely be wrong as well.  Thundering herds have a tendency to make
bottlenecking worse than it already is. `q.writeNotifier()` will return
a `Notifier`, a simple `Mutex`/`Condition` pair, which can be used to
build a more responsible write-awaiter, if desired.

When you dequeue a slice, it's good for precisely until the next call to
a `dequeue` function, after which, generally, it's getting stomped on by
writers.  If you need to put some of the slice back, you can, keep track
of the index:

```zig
dequeue: while (true) {
    if (reader.q.dequeueWithTimeout(1000) catch continue :dequeue) |msgs| {
        for (msgs, 0..) |msg, i| {
            if (reader.doInterruptableMessageThings(msg)) |interrupt| {
                reader.q.requeueAtIndex(i);
                // be right back
                return reader.dealWithInterrupt(interrupt);
            }
        }
        if (reader.done()) break :dequeue;
    }
}
```
You get the rest of it back next time you call `dequeue`, after that it
swaps again.  Unless you keep doing it.

### Writing

As shown, the reader (owner of the queue) can also write to it, but this
is not the ordinary course of events.

```zig
var w = q.writeHandle();
defer w.release();
```

Creates a `WriteHandle`.  This is an erased pointer to the queue, which
forwards the enqueue-side functions only.  It cannot open, close, nor
resize, nor, Heaven forfend, dequeue the queue.  It can, however,
enqueue, and that in several ways.

If reference counting is enabled (by default, no), a count of
`writeHandle` calls is kept, and `w.release()` will decrement it.
`q.destroy()` (but not `q.errDestroy()`) will panic if this count is not
zero.  This is intended as a useful building block, and sanity check,
but is not a complete, nor even necessarily a part of, a system to take
down a multithreaded program in an orderly manner.

Most functions which are configuration-specific are `@compileError`s to
call when not so configured. `release()` is always allowed, and will set
the handle pointer to `undefined` (for modes where this has an effect)
in all cases.

In addition to the already-illustrated value and slice writes, a
`WriteHandle` has `enqueueSliceUpTo`.  This is passed a slice, and
enqueues as much of it as there's room for, returning what's left over:
this will be an empty slice when all values are enqueued.  It will throw
`QueueIsFull` if unable to enqueue any value at all.

This variation has the nice property that it will eventually enqueue all
values of a slice larger than the enqueue itself, where `enqueueSlice`
will repeatedly fail[^※].  As such, the `UpTo` vesion should be
preferred for any collection of values where the upper bound is unknown.
Just remember that the iteration terminates on an empty slice, not `void`
or `null`.

For some applications, the writers may wish to write directly to
the queue, rather than creating and sending a copy.  For this, use
`reserveSlice`, which (on success) returns a slice of the queue of the
requested slice, then write to it and perform a `commit()`:

```zig
if (wh.reserveSlice(12)) |slice| {
    @memcpy(slice, "Hello there!");
    wh.commit();
} else |_| {
    // Not enough room, it's fine, I didn't
    // even want to say hello... baka
}
```

It is asserted that any given `WriteHandle` will not open another
reservation until the last one is committed.

Accomplishing the commit uses some space on the `WriteHandle`, and as
such, the reader-side cannot call this function directly.  It's cheap to
create a `WriteHandle` (`defer wh.release()`!) and use it, in the event
that the reader needs to do this kind of thing itself.

If your `TuQueQue` is of `u8`, this slice can be fed to an `Io.Writer`
for serialization, or other nefarious purposes.  Please note that there
is no way to give back any of the reservation, so if the length of the
write is unclear, some protocol must be established with the reader to
indicate the unused portion.  Zeroes perhaps.

Speaking of which!  This function can leak the prior contents of the
queue, which for some applications is a security risk.  If yours is
among them, configuring with `.secure_erase = true` will fill the
reservations with zeroes before handing it over.  This is, by default,
`false`, under two premises: expense should only be incurred when value
is obtained, and my users can be expected to RTFM and code responsibly.
Don't make me regret it.  The criterion is not "my code doesn't leak",
it's "there are no security implications for queue leakage whatsoever".

Let us observe that reserving a slice is a _grave_ responsibility.
Without the call to `commit()`, the reader will be stuck forever,
looping on `DequeuePending` and questioning its life choices.

Don't bum the reader out, writers.  Commit.

## Cleanup

As mentioned, the queue must be closed before `destroy` is called, and
all refcounts (when applicable) released.  After closing, writes may be
ongoing, but no new writes will be accepted.  After `dequeueAvailable()`
returns `null`, the only way the reader will see more writes is if it
calls `open`.

Be careful here!  Writers examine the queue itself to see if it's
closed, so some method _must_ be used to ensure that writers have
disposed of that capability, or at least won't use it, before the
queue is destroyed.  Reference counting is one of several ways to
arrange this.

## Suitability and Correct Use

The most important invariant is that this is a _single-consumer_ queue.
The queue itself is assumed to be owned by the reader, that reader must
live on exactly one thread at any given point.  This also circumscribes
the boundary within which using `tuqueque` might be a good choice.  No
data structure can do everything, and this is not naturally suited to
work-stealing, or other approaches to handling the case where a reader
is unable to keep up with writes.  It's MPSC, and should be used when
that pattern is correct for the application.

Another thing to be aware of is that, at any given time, only half of
the capacity is available to the writers at any given point.  The upside
is that the reader gets all the updates at once on swap, all lined up in
a nice slice with no contention, no pointer chasing, and a predictable
access pattern.  The sentiment is that on modern architectures, leaving
some space fallow in exchange for minimal waits and blocks is a great
trade-off to make.  This data structure was chosen for a message-passing
architecture, where the size of each queue half is expected to be more
than adequate, with threads able to keep up with the load, and writers
having other things to occupy their scheduled time slices if a queue is
temporarily full.  The design emphasizes throughput, and is willing to
tolerate some latency variance accordingly.

Any single-consumer system will start to behave poorly if the reader
cannot keep up with the incoming work.  It's straightforward to track
queue capacity against messages-per-dequeue and grow accordingly, it
is not straightforward at all to jimmy in some mechanism to farm out
work if growth is consistent.  For 'bursty' workloads, especially ones
which move housekeeping out of the hot path (examples: logging, database
transactions), it could perform very well.

A `TuQueQue` queque is quite well-suited to SPSC work, which is after
all just a special case of MPSC where `M == 1`.  Just don't rely on
the built-in `Condition` notifications alone, it takes more than that
to avoid deadlock races[^✤].  If you're certain the workload will be
SPSC, it may be that something more specialized to that purpose will
suit your needs better.  Then again, maybe not!  It's very good at what
it does.

Every atomic value in a `TuQueQue` is isolated on its own cache line,
to prevent false sharing.  The queues are also cache-aligned, one
consequence is that your starting capacity might be somewhat larger
than what was requested.  Writes can occur into the same cache line
however, in fact this would be normal for small writes, and depending
on specifics this might noticeably impact performance.  This can be
avoided, when suitable, by padding the data type used out to fill its
own cache line.  Since the queues themselves always start at the start
of a cache line, this will work.

There are a whole host of queries included, answering questions about
capacity, messages available, and so on.  These are debugging aids.
Using them in production produces read contention on critical atomic
values, and the right way to find out how many messages you can get is
to dequeue them.  It's no great challenge to track the capacity of the
queue halves locally, detect the case of most or all of it getting used,
then employ a simple state machine to grow / shrink both halves over the
ordinary course of message processing.

[^†]: Like the cognitive fallacy.  Pronounced _tu-**kyu**-kway_, if
possible.  You can also say _¿Tu que que?_, but you had better know who
you're saying it to.

[sbq]: https://github.com/wyfo/swap-buffer-queue

[^‡]: Likely this will no longer be true well before the next time the
README gets updated.  I did not write this library as an intellectual
exercise.

[^✢]: The implementation uses internal self-pointers.  Copying a queue
doesn't give you a queue clone, it gives you a serious headache later
when you try and figure out why things have gone wrong.

[^※]: There is no distinct error for an attempt to enqueue a slice so
large that it's guaranteed to fail.  Enqueuing capacity is a mutable
property: the dequeue might be resizing at the time of query, or already
larger, and so on, so this doesn't strike me as an actually-distinct
case from `error.QueueIsFull`.  Corollary: if you're not sure, use
`enqueueUpToSlice`.

[^✤]: Just as true for MPSC, but somehow less tempting.  Threads are
not coroutines!
