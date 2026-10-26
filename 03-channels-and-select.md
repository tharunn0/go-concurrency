# Part III: Channels and select

[← Part II](02-runtime.md) · [Index](README.md) · [Next: Part IV, sync Primitives →](04-sync-primitives.md)

- [Chapter 10: Channel Internals and Semantics](#chapter-10-channel-internals-and-semantics)
- [Chapter 11: select](#chapter-11-select)

---

## Chapter 10: Channel Internals and Semantics

### TL;DR

- A channel is a heap-allocated `hchan`: a lock, an optional ring buffer, and two wait queues (`sendq`, `recvq`) of parked goroutines (`sudog`s).
- When a receiver is already waiting, a send **copies directly into the receiver's stack** and readies it, skipping the buffer.
- **Closing is a broadcast**: all waiting receivers wake with zero values, and all waiting senders panic.
- **The sender closes, never the receiver.** With multiple senders, coordinate the close separately (WaitGroup + closer goroutine), or don't close at all and signal with a done channel or ctx.
- Buffer size is a **design decision**: 0 = synchronisation, 1 = latest or one-shot signal, N = a known bound. Large buffers hide backpressure problems; they don't fix them.

### Mental Model: the six axioms

| Operation | `nil` channel | open channel | closed channel |
|---|---|---|---|
| `ch <- v` | **blocks forever** | blocks until receiver / buffer space | **panic** |
| `<-ch` | **blocks forever** | blocks until sender / buffered value | returns buffered values, then **zero value, `ok=false`** immediately |
| `close(ch)` | **panic** | succeeds | **panic** |
| `len(ch)` / `cap(ch)` | 0 / 0 | current / capacity | remaining buffered / capacity |

These six behaviours (the nil and closed columns) are not quirks. They are tools:
- **nil blocks forever** → disables a `select` case.
- **closed returns immediately** → a broadcast "done" signal any number of goroutines can observe.

### Under the Hood (`runtime/chan.go`)

```go
// Abridged: Go 1.23+ also has `timer *timer` (timer channels) and 1.24+ a synctest bubble pointer.
type hchan struct {
	qcount   uint           // items currently in buffer
	dataqsiz uint           // buffer capacity
	buf      unsafe.Pointer // ring buffer of dataqsiz elements
	elemsize uint16
	closed   uint32
	elemtype *_type
	sendx    uint   // ring index for next send
	recvx    uint   // ring index for next receive
	recvq    waitq  // parked receivers (list of *sudog)
	sendq    waitq  // parked senders
	lock     mutex  // runtime mutex, not sync.Mutex
}
```

A **`sudog`** ("pseudo-g") represents a goroutine waiting on a channel. It holds the `g`, a pointer to the element to send or the slot to receive into, and (for `select`) which case it belongs to. One goroutine can be on many wait queues at once (in a `select`) via many sudogs.

#### `chansend(c, ep, block)`

```
if c == nil: block forever (or return false if non-blocking)
fast path (no lock): if non-blocking && not closed && full → return false
lock(c)
if closed: unlock; panic("send on closed channel")
if receiver waiting in recvq:
    send directly: copy *ep → receiver's stack slot; goready(receiver)   ← no buffer touch
    unlock; return
if qcount < dataqsiz:
    copy *ep → buf[sendx]; sendx++; qcount++; unlock; return
if !block: unlock; return false
enqueue sudog on sendq; gopark (unlocks c)
on wake: if woken because closed → panic
```

#### `chanrecv(c, ep, block)`

```
if c == nil: block forever
fast path: if non-blocking && empty: if !closed return false; else return zero
lock(c)
if closed && qcount == 0: zero *ep; unlock; return (true, ok=false)
if sender waiting in sendq:
    unbuffered: copy directly from sender's value; goready(sender)
    buffered (buffer must be full): take buf[recvx] for us, put sender's value at buf tail (preserves FIFO); goready(sender)
if qcount > 0: copy buf[recvx] → *ep; recvx++; qcount--; unlock
if !block: unlock; return false
enqueue sudog on recvq; gopark
```

#### `closechan(c)`

```
lock; if nil/closed → panic
closed = 1
release ALL receivers (they get zero values, ok=false)
release ALL senders (they will panic when they wake)
unlock; goready each released goroutine
```

> 💡 **Insight:** The "direct send" path means an unbuffered channel handoff copies the value **once**, straight from sender stack to receiver stack. That is the reason the runtime is allowed to write into another goroutine's stack (a rare exception, handled carefully with respect to stack copying).

> 💡 **Insight:** Channel ops are **FIFO for blocked goroutines**: parked senders and receivers are served in queue order. But which goroutine *reaches* the queue first is up to the scheduler.

### Practical Code

#### Closing correctly with multiple producers

```go
func fanIn[T any](ctx context.Context, producers ...func(context.Context, chan<- T)) <-chan T {
	out := make(chan T)
	var wg sync.WaitGroup
	for _, p := range producers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p(ctx, out) // producers never close out
		}()
	}
	go func() {
		wg.Wait()
		close(out) // the single closer: runs after every sender is finished
	}()
	return out
}
```

#### The nil-channel trick: merging until both inputs close

```go
func merge[T any](a, b <-chan T) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)
		for a != nil || b != nil {
			select {
			case v, ok := <-a:
				if !ok {
					a = nil // disable this case forever
					continue
				}
				out <- v
			case v, ok := <-b:
				if !ok {
					b = nil
					continue
				}
				out <- v
			}
		}
	}()
	return out
}
```

Without setting the closed channel to `nil`, the `select` would spin, picking the closed case (which is always ready) about half the time.

#### Request/response: channel of channels

```go
type request struct {
	key   string
	reply chan<- string // per-request reply channel, buffered(1)
}

func server(ctx context.Context, reqs <-chan request) {
	data := map[string]string{"a": "1"}
	for {
		select {
		case <-ctx.Done():
			return
		case r := <-reqs:
			r.reply <- data[r.key] // never blocks: reply is buffered(1)
		}
	}
}
```

The reply channel must be **buffered with capacity 1**, so the server never blocks on a client that gave up.

#### Choosing buffer size

| Size | Use it for | Smell |
|---|---|---|
| **0** | Synchronisation; handoff with two-way happens-before; rendezvous | None. This is the default. Justify anything else. |
| **1** | One-shot results (reply channel), "latest value" signal with non-blocking send, a mutex-like token | Using 1 to "make the deadlock go away" |
| **N (small, known)** | Semaphore (cap = concurrency), absorbing bursts of a *measured* size, exactly-N results (`make(chan T, len(jobs))`) | |
| **Large (1000+)** | Rarely justified | Hides a slow consumer; adds latency, and up to `cap` events are lost on crash |

#### "Latest value" coalescing signal

```go
// notify: non-blocking send to a cap-1 channel coalesces bursts into one wakeup.
func (w *Watcher) notify() {
	select {
	case w.changed <- struct{}{}:
	default: // a wakeup is already pending; it will see the latest state
	}
}
```

This is the standard way to say "something changed, go re-read the state" without queueing N redundant events.

### Pitfalls

> ⚠️ **Closing from the receiver side.** The receiver doesn't know whether a sender is mid-send. `close` then makes that sender panic. If the consumer wants producers to stop, it signals via a *separate* channel or ctx.

> ⚠️ **`sync.Once` to close a channel "safely" from many places.** It works mechanically, but it usually hides unclear ownership: senders may still panic sending to the closed channel. Fix ownership instead.

> ⚠️ **Range over a channel nobody closes.** `for v := range ch` blocks forever after the last value, which is a goroutine leak.

> ⚠️ **Checking `len(ch)` before sending or receiving.** It is a time-of-check/time-of-use race. Use `select` with `default`.

> ⚠️ **Sending pointers and then mutating.** A channel transfers ownership by convention only. `ch <- &buf; buf.Reset()` is a data race.

> 🤖 **AI note:** Generated code often closes a results channel inside the worker loop (`defer close(results)` in each of N workers), which panics on the second close. Another frequent mistake is buffering the channel to `len(input)` "to avoid deadlock" in streaming code where the input is unbounded.

### IRL Scenario: the 100,000-slot buffer

**Context:** An event pipeline's consumer occasionally fell behind. Someone raised the channel buffer from 100 to 100,000 "to absorb spikes". Months later: memory alarms, and after a crash, ~80k events were lost.

**Analysis:**
- Buffers absorb *transient* bursts whose size you know. They do nothing for a sustained rate mismatch: if λ_in > μ_out, any buffer fills; it only delays the failure.
- A huge buffer means huge **queueing latency** (Little: W = L/λ; 100k events / 2k per s = 50 s of latency) and **unacknowledged data in RAM**.

**Fix:** Buffer back to 256, plus explicit backpressure (the producer blocks with a ctx timeout, then sheds with a metric), plus more consumer parallelism, plus durable queueing (Kafka) for data that must not be lost.

### Review Drill

```go
func process(jobs []Job) []Result {
	results := make(chan Result)
	for _, j := range jobs {
		go func() {
			results <- handle(j)
		}()
	}
	var out []Result
	for r := range results {
		out = append(out, r)
		if len(out) == len(jobs) {
			close(results)
		}
	}
	return out
}
```

<details>
<summary>What's wrong?</summary>

1. The **receiver closes** the channel. It happens to be safe here only because all sends have completed by the time `len(out) == len(jobs)`, but it is fragile: any refactor (early return on error, a filter) turns it into a panic or a leak.
2. If `jobs` is empty, `range results` blocks forever (nobody closes it).
3. Unbounded goroutines for large `jobs`.

Better:

```go
func process(jobs []Job) []Result {
	out := make([]Result, len(jobs))
	var wg sync.WaitGroup
	for i, j := range jobs {
		wg.Go(func() { out[i] = handle(j) }) // [Go 1.25+]; distinct indices: no race
	}
	wg.Wait()
	return out
}
```

No channel needed at all, results stay in order, and adding a bound is easy (Chapter 19).

</details>

### Exercises

1. ⭐ What does `v, ok := <-ch` return for a closed channel that still has 2 buffered values?
2. ⭐⭐ Implement `OrDone[T](ctx, in <-chan T) <-chan T` that forwards values until `in` closes or ctx is done, with no leaks in either direction.
3. ⭐⭐⭐ Implement a `Broadcast[T]` type: `Subscribe(buf int) (<-chan T, func())` and `Publish(T)`, where slow subscribers don't block the publisher (drop for that subscriber and count it) and unsubscribe is race-free.

<details>
<summary>Solutions</summary>

1. The first two receives return the buffered values with `ok=true`. Only after that do receives return `zero, false`.
2. ```go
   func OrDone[T any](ctx context.Context, in <-chan T) <-chan T {
       out := make(chan T)
       go func() {
           defer close(out)
           for {
               select {
               case <-ctx.Done():
                   return
               case v, ok := <-in:
                   if !ok { return }
                   select { // the send must also be cancellable
                   case out <- v:
                   case <-ctx.Done():
                       return
                   }
               }
           }
       }()
       return out
   }
   ```
3. ```go
   type Broadcast[T any] struct {
       mu      sync.Mutex
       subs    map[chan T]struct{}
       Dropped atomic.Int64
   }
   func NewBroadcast[T any]() *Broadcast[T] { return &Broadcast[T]{subs: map[chan T]struct{}{}} }

   func (b *Broadcast[T]) Subscribe(buf int) (<-chan T, func()) {
       ch := make(chan T, buf)
       b.mu.Lock(); b.subs[ch] = struct{}{}; b.mu.Unlock()
       var once sync.Once
       return ch, func() {
           once.Do(func() {
               b.mu.Lock(); delete(b.subs, ch); b.mu.Unlock()
               close(ch) // safe: Publish only sends while holding mu, and ch is no longer in subs
           })
       }
   }

   func (b *Broadcast[T]) Publish(v T) {
       b.mu.Lock()
       defer b.mu.Unlock()
       for ch := range b.subs {
           select {
           case ch <- v:
           default:
               b.Dropped.Add(1)
           }
       }
   }
   ```
   The key point is that sends and the close are serialized by `mu`, so no send can hit a closed channel. Non-blocking sends mean the lock is held only briefly.

</details>

### Review Checklist

- [ ] For each channel: who sends, who receives, who closes, and is that documented?
- [ ] Is every non-zero buffer size justified?
- [ ] Can every blocking send or receive be cancelled?
- [ ] Is a channel being used where a slice + WaitGroup or a mutex would be simpler?

---

## Chapter 11: select

### TL;DR

- When several cases are ready, `select` picks **uniformly at random**. There is **no priority** and no source order.
- **All channel expressions and send values are evaluated once, on entry, in source order**, even for cases that are not chosen.
- `default` makes it non-blocking. `select {}` blocks forever.
- Implementation: lock every involved channel (in address order, to avoid deadlock), check for a ready case in random order, otherwise enqueue on all channels and park. Cost: O(n log n) to sort the lock order, plus O(n) channel locks per entry.
- `break` inside `select` breaks the `select`, not the enclosing `for`. Use a label.

### Mental Model

```
select {
case v := <-a:     ┐
case b <- x:       ├─ 1. evaluate a, b, x, c (in order, once)
case <-c:          ┘  2. any ready? pick one uniformly at random → do it
default:              3. none ready: default? run it. else park on ALL, wake on first.
}
```

### Under the Hood (`runtime/select.go` → `selectgo`)

1. **Compiler special cases** (no `selectgo` call):
   - `select {}` → `block()` forever.
   - One case, no default → plain channel op.
   - One case + `default` → `selectnbsend` / `selectnbrecv` (non-blocking fast path).
2. **pollorder:** a random permutation of the cases (via a cheap random generator). This gives the fairness.
3. **lockorder:** cases sorted by `hchan` address (heap sort), with duplicates locked once. Every `select` acquires channel locks in the same global order, so two selects over the same channels can't deadlock each other.
4. **Pass 1:** with all locks held, scan in pollorder for a ready case. If one is found, perform it, unlock all, and return.
5. **No case ready + default:** unlock, return default.
6. **Pass 2:** create a sudog per case and enqueue it on every channel's `sendq`/`recvq`. Park (`gopark` with `selparkcommit`, which unlocks all channels).
7. **Wake-up:** whoever readied us did so through one specific sudog. Relock all, **dequeue our sudogs from every other channel** (pass 3), unlock, and return the winning case.

> 💡 **Insight:** A 10-case `select` locks 10 channels on every entry. In hot paths a big `select` costs real time and causes lock contention on shared channels (such as a global `done` channel watched by thousands of goroutines). Even after `done` is closed, a multi-case `select` still locks it. Only a lone `select { case <-done: default: }` (or `ctx.Err()`) takes the lock-free fast path.

### Practical Code

#### The for-select loop with shutdown

```go
func (w *Worker) Run(ctx context.Context) error {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case job, ok := <-w.jobs:
			if !ok {
				return nil
			}
			if err := w.handle(ctx, job); err != nil {
				return err
			}
		case <-ticker.C:
			w.flush()
		}
	}
}
```

#### Priority: "shutdown wins over work"

Because the choice is random, a worker with a continuously ready `jobs` channel has only a ~50% chance of noticing `ctx.Done()` on each iteration once it's cancelled. That is usually fine (it notices within a few iterations). When it isn't, check explicitly:

```go
for {
	// 1. Check high-priority signal first, non-blocking.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	// 2. Then wait on everything.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case job := <-jobs:
		handle(job)
	}
}
```

Or, more simply, check `if ctx.Err() != nil { return ctx.Err() }` at the top of the loop.

**Priority between two data channels** (prefer `high` over `low`):

```go
for {
	select {
	case j := <-high:
		handle(j)
		continue
	default:
	}
	select {
	case j := <-high:
		handle(j)
	case j := <-low:
		handle(j)
	case <-ctx.Done():
		return
	}
}
```

This is *biased*, not strict: if both become ready while parked in the second select, either may win. True strict priority needs a single owner goroutine that drains both into a priority queue.

#### Timeouts: per-operation vs overall

```go
// Overall deadline for the whole loop: one timer, created once.
ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
defer cancel()

// Per-item idle timeout: reuse a single timer.
idle := time.NewTimer(30 * time.Second)
defer idle.Stop()
for {
	select {
	case msg := <-in:
		handle(msg)
		idle.Reset(30 * time.Second) // Go 1.23+ semantics (go.mod `go 1.23`+): Reset is safe without draining
	case <-idle.C:
		return errIdle
	case <-ctx.Done():
		return ctx.Err()
	}
}
```

#### Non-blocking send with drop and metric

```go
select {
case out <- ev:
default:
	dropped.Add(1) // explicit, observable load shedding
}
```

### Pitfalls

> ⚠️ **Eager evaluation of case expressions.**
> ```go
> select {
> case out <- expensiveEncode(v): // ALWAYS runs on select entry
> case <-ctx.Done():
> }
> ```
> `expensiveEncode` runs even if ctx is already done. Compute the value first (if you need it anyway), or check ctx before.

> ⚠️ **`break` doesn't leave the loop.**
> ```go
> for {
>     select {
>     case <-done:
>         break // only breaks the select!
>     }
> }
> ```
> Use `return`, or a labeled `break loop`.

> ⚠️ **Busy loop with `default`.** `for { select { case v := <-ch: ...; default: } }` spins at 100% CPU. Remove `default` to block.

> ⚠️ **`time.After` in a hot loop.** It creates a new timer every iteration. Since Go 1.23 (when go.mod declares `go 1.23` or later), unreferenced timers are garbage-collected, so it is no longer a *leak*, but it is still allocation and timer-heap churn. Reuse a `time.Timer`.

> 🤖 **AI note:** AI frequently writes priority logic that assumes `select` checks cases top-to-bottom ("put ctx.Done() first so it has priority"). Order in source has no effect.

### IRL Scenario: workers kept processing until SIGKILL after SIGTERM

**Context:** Kubernetes sends SIGTERM, the app cancels the root ctx, and workers should stop. The pod hit `terminationGracePeriodSeconds` (30 s) and got SIGKILLed mid-write, corrupting a batch.

**Diagnosis:** Workers looped `select { case <-ctx.Done(): return; case j := <-jobs: process(j) }`. `jobs` was fed from a 10k buffered channel, so it was always ready. Each iteration had a 50% chance of picking `Done`, which should have stopped them quickly… but `process(j)` itself took up to 2 s and **ignored ctx**. Worse, the dispatcher kept refilling `jobs` because it didn't watch ctx either.

**Fix:**
1. The dispatcher stops on ctx and closes `jobs`.
2. Workers check `ctx.Err()` before each job and pass ctx into `process`, which checks it between steps.
3. A proper drain: on shutdown, stop intake, finish in-flight jobs with a 20 s budget, then hard-cancel (Chapter 30).

### Review Drill

```go
func firstResponse(ctx context.Context, urls []string) (string, error) {
	ch := make(chan string)
	for _, u := range urls {
		go func() {
			body, err := fetch(ctx, u)
			if err == nil {
				ch <- body
			}
		}()
	}
	select {
	case b := <-ch:
		return b, nil
	case <-time.After(2 * time.Second):
		return "", errors.New("timeout")
	}
}
```

<details>
<summary>What's wrong?</summary>

1. **Leak:** after the first response (or timeout), every other successful fetcher blocks forever on `ch <- body`. Fix: `make(chan string, len(urls))`, or `select` on ctx in the sender.
2. **Wasted work:** losers keep fetching. Derive `ctx, cancel := context.WithCancel(ctx)` and `defer cancel()`, so the losers stop when we return.
3. **Hangs on all-fail:** if every fetch errors, we wait the full 2 s even though we could know immediately. Count failures, or send errors on the channel too.
4. **Caller ctx ignored** in the select: add `case <-ctx.Done()`.

</details>

### Exercises

1. ⭐ What does a `select` with only `case <-nilChan:` and no default do?
2. ⭐⭐ Write a test proving that `select` is random: two always-ready channels, 100k iterations, check that the distribution is ~50/50.
3. ⭐⭐⭐ Implement a strictly prioritised multiplexer over K input channels using one goroutine and a heap, with cancellation. Discuss its latency trade-off versus biased select.

<details>
<summary>Solutions</summary>

1. Blocks forever. If it's the only goroutine left, you get `fatal error: all goroutines are asleep - deadlock!`.
2. Fill two buffered channels (cap 1) before each select, count which case fires, and assert each count falls within, e.g., 49–51%.
3. A goroutine reads all inputs (with the nil-trick as they close) into a priority heap, and in the same select offers `heap.Peek()` on the output (using a nil output channel when the heap is empty). Trade-off: strict ordering among *buffered* items, but an extra hop and an internal buffer (bound it!).

</details>

### Review Checklist

- [ ] Does any logic assume `select` case order matters?
- [ ] Are case expressions side-effect free and cheap?
- [ ] Do loops use labeled `break` or `return` correctly?
- [ ] Are timers reused in loops rather than `time.After` per iteration?
- [ ] Does cancellation reach *inside* the work, not just the `select`?

---

[← Part II](02-runtime.md) · [Index](README.md) · [Next: Part IV, sync Primitives →](04-sync-primitives.md)
