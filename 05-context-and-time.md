# Part V: context and Time

[← Part IV](04-sync-primitives.md) · [Index](README.md) · [Next: Part VI, Patterns →](06-patterns.md)

- [Chapter 16: context in Depth](#chapter-16-context-in-depth)
- [Chapter 17: Timers and Tickers](#chapter-17-timers-and-tickers)

---

## Chapter 16: context in Depth

### TL;DR

- A `context.Context` is an **immutable node in a tree**. Cancelling a node cancels its whole subtree, never its parent.
- **Cancellation is cooperative.** `ctx.Done()` is just a channel that closes. Nothing stops your goroutine unless your code watches it.
- **Always call `cancel`** (`defer cancel()`). Otherwise the child stays registered with its parent (and, for timeout contexts, a timer stays alive) until the parent is cancelled or the deadline passes, whichever comes first. `go vet`'s `lostcancel` check catches many cases.
- Modern APIs: `WithCancelCause`/`Cause` `[1.20]`, `WithDeadlineCause`/`WithTimeoutCause`, `AfterFunc`, `WithoutCancel` `[1.21]`.
- Values are for **request-scoped data that crosses API boundaries** (trace IDs, auth principal), not for optional parameters. Lookup is a linked-list walk.

### Mental Model

```
Background
  └── server ctx (cancel on SIGTERM)
        └── request ctx (net/http: cancelled when client disconnects or handler returns)
              ├── WithTimeout(2s) → DB query
              ├── WithTimeout(500ms) → cache lookup
              └── WithoutCancel → audit log write (outlives the request, gets its own timeout)
```

Deadlines can only **shrink** down the tree. `WithTimeout(parent, 10s)` under a parent that expires in 1 s expires in 1 s.

### Under the Hood (`context/context.go`)

```go
type cancelCtx struct {
	Context                        // parent
	mu       sync.Mutex
	done     atomic.Value          // chan struct{}, created lazily on first Done()
	children map[canceler]struct{} // set to nil on cancel
	err      atomic.Value          // error, set by the first cancel (a plain error field in older releases)
	cause    error
}
```

**`propagateCancel(parent, child)`**, i.e. how a child learns that its parent was cancelled:
1. `parent.Done() == nil` (Background/TODO/WithoutCancel): never cancelled, nothing to do.
2. Parent already done: cancel the child immediately.
3. Parent is (or wraps) a standard `*cancelCtx`: register in `parent.children`. **No goroutine.**
4. Parent implements `AfterFunc` (the `afterFuncer` interface): register via that.
5. Otherwise (a custom Context type): **start a goroutine** that waits on `parent.Done()` and `child.Done()`.

**`cancel(removeFromParent, err, cause)`:** lock, set `err`/`cause`, close `done` (or store a pre-closed channel if `Done` was never called), cancel all children recursively, then remove itself from the parent's `children`.

**`timerCtx`** wraps a `cancelCtx` with a `time.AfterFunc` timer. `cancel` stops the timer. That is why a forgotten `cancel` keeps the timer, and the child's registration with its parent, alive until the deadline.

**`valueCtx`:** each `WithValue` adds a node. `Value(k)` walks up the parent chain, so lookup cost is O(depth). Fine for a handful of values. It is a smell if you have dozens.

> 💡 **Insight:** Because of step 5, **custom Context implementations cost a goroutine per derived child** unless they embed a standard context properly. Wrapping contexts in your own types is almost never worth it.

### Practical Code

#### Make blocking operations cancellable

```go
// Send with cancellation
func send[T any](ctx context.Context, ch chan<- T, v T) error {
	select {
	case ch <- v:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// Sleep with cancellation
func Sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// CPU-bound loop: poll ctx.Err() at a coarse granularity
func hashAll(ctx context.Context, files []string) error {
	for i, f := range files {
		if i%64 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		hashFile(f)
	}
	return nil
}
```

#### Causes: make "context canceled" debuggable

```go
var ErrClientGone = errors.New("client disconnected")
var ErrShutdown = errors.New("server shutting down")

ctx, cancel := context.WithCancelCause(parent)
defer cancel(nil) // nil cause → context.Canceled

go func() {
	select {
	case <-shutdownCh:
		cancel(ErrShutdown)
	case <-ctx.Done():
	}
}()

// Deep in the call stack:
if err := doWork(ctx); err != nil {
	log.Printf("work aborted: err=%v cause=%v", err, context.Cause(ctx))
	// err   = context.Canceled           (stable, for errors.Is checks)
	// cause = server shutting down       (the actual reason)
}

// Timeouts with a cause [1.21]
tctx, tcancel := context.WithTimeoutCause(ctx, 2*time.Second,
	fmt.Errorf("inventory lookup exceeded 2s budget"))
defer tcancel()
```

#### `AfterFunc`: bridge ctx to APIs that don't take one `[1.21+]`

```go
// Unblock a Read on a net.Conn when ctx is cancelled (pattern from the context package's example).
func readWithCtx(ctx context.Context, conn net.Conn, buf []byte) (int, error) {
	stopc := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		conn.SetReadDeadline(time.Now()) // wakes the blocked Read
		close(stopc)
	})
	n, err := conn.Read(buf)
	if !stop() {
		// f ran (or is running): wait for it, then clear the deadline so the conn stays usable.
		<-stopc
		conn.SetReadDeadline(time.Time{})
		return n, context.Cause(ctx)
	}
	return n, err
}
```

`stop()` returns `true` if it prevented `f` from running. `AfterFunc` runs `f` in its own goroutine, and `stop` does **not** wait for a running `f` to finish. That's why the code waits on `stopc` before clearing the deadline. Otherwise a late `f` could leave a past deadline on the conn and break every later `Read`.

`AfterFunc` + `sync.Cond` makes a Cond wait cancellable. This is the documented example in the `context` package:

```go
func waitOnCond(ctx context.Context, cond *sync.Cond, conditionMet func() bool) error {
	stopf := context.AfterFunc(ctx, func() {
		// Acquire cond.L so the Broadcast can't happen between the waiter's
		// ctx.Err() check and its cond.Wait (lost wakeup).
		cond.L.Lock()
		defer cond.L.Unlock()
		cond.Broadcast()
	})
	defer stopf()

	for !conditionMet() { // caller holds cond.L
		cond.Wait()
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return nil
}
```

#### `WithoutCancel`: work that must outlive the request `[1.21+]`

```go
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	order, err := h.placeOrder(r.Context(), r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, order)

	// Audit must complete even though the request ctx is cancelled when we return.
	// Keep values (trace ID) and drop the cancellation, then add our own bound.
	bg := context.WithoutCancel(r.Context())
	h.bgWG.Go(func() { // tracked so graceful shutdown can wait for it
		ctx, cancel := context.WithTimeout(bg, 5*time.Second)
		defer cancel()
		h.audit.Record(ctx, order)
	})
}
```

#### Typed context keys

```go
type ctxKey struct{} // unexported type: no collisions with other packages

func WithPrincipal(ctx context.Context, p *Principal) context.Context {
	return context.WithValue(ctx, ctxKey{}, p)
}

func PrincipalFrom(ctx context.Context) (*Principal, bool) {
	p, ok := ctx.Value(ctxKey{}).(*Principal)
	return p, ok
}
```

### Pitfalls

> ⚠️ **Background goroutines using the request ctx.** `go h.audit.Record(r.Context(), x)` fails with `context canceled` the moment the handler returns. Use `WithoutCancel` plus a timeout, and track the goroutine.

> ⚠️ **`context.Background()` in the middle of a call chain** cuts cancellation and deadlines off from everything below it. Grep your codebase for it. Outside `main`, init and tests, each occurrence needs a justification.

> ⚠️ **`WithTimeout` in a loop without `cancel`** accumulates timers and child registrations until they expire:
> ```go
> for _, item := range items {
>     ctx, cancel := context.WithTimeout(parent, time.Second)
>     process(ctx, item)
>     cancel() // not defer: defer would wait until the function returns
> }
> ```

> ⚠️ **Retrying without checking the deadline budget.** Retrying three times with 1 s per-attempt timeouts under a parent deadline of 1.5 s wastes attempts. Check `ctx.Deadline()` and skip attempts that can't finish.

> ⚠️ **Context in structs.** It's usually wrong: it ties a long-lived object's lifecycle to one call. Exceptions: a struct representing *one operation* (like `http.Request`), or a server's root ctx used only to derive per-operation contexts.

> 🤖 **AI note:** AI-generated handlers frequently spawn `go func() { ... }()` using `r.Context()` or `context.Background()` inside, with no tracking. That gives you either spurious cancellation or untracked work that dies at shutdown. The other classic is forgetting `defer cancel()`.

### IRL Scenario: cascading timeouts across microservices

**Context:** Gateway (timeout 3 s) → Orders (each call to Inventory: 2 s timeout, 3 retries) → Inventory. During an Inventory slowdown, the gateway timed out at 3 s, but Orders kept retrying Inventory for up to 6 s per request, **tripling** load on Inventory exactly when it was struggling. A retry storm.

**Root causes:**
1. Orders created its outbound timeouts from `context.Background()` instead of the incoming ctx (gRPC propagates deadlines in metadata, but they ignored it).
2. Retries didn't check the remaining budget.

**Fix:**
```go
func (c *InventoryClient) Reserve(ctx context.Context, req *Req) (*Resp, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		dl, hasDL := ctx.Deadline()
		if hasDL && time.Until(dl) < 200*time.Millisecond {
			break // not enough budget for a meaningful attempt
		}
		actx, cancel := context.WithTimeout(ctx, 800*time.Millisecond) // shrinks to parent's
		resp, err := c.rpc.Reserve(actx, req)
		cancel()
		if err == nil {
			return resp, nil
		}
		lastErr = err
		if !retryable(err) {
			return nil, err
		}
		if err := Sleep(ctx, backoff(attempt)); err != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("reserve failed: %w", lastErr)
}
```
Plus a **retry budget** (e.g., retries are at most 10% of requests, Chapter 30) and a circuit breaker.

### Exercises

1. ⭐ What do `ctx.Err()` and `context.Cause(ctx)` return after a `WithTimeout` deadline passes? After `cancel()` on the same ctx before the deadline?
2. ⭐⭐ Write a middleware that logs *why* each request's context ended (client gone, server timeout, shutdown) using causes.
3. ⭐⭐⭐ Implement `Merge(ctx1, ctx2) (context.Context, context.CancelFunc)`, a context that is done when *either* is done and carries values from ctx1. Use `AfterFunc` and avoid a goroutine per merge.

<details>
<summary>Solutions</summary>

1. After the deadline: `Err() == context.DeadlineExceeded`, and `Cause` is also `DeadlineExceeded` (or your cause, if you used `WithTimeoutCause`). After an early `cancel()`: `Err() == context.Canceled`, `Cause == context.Canceled`. The first cancellation wins and is sticky.
2. Wrap with `context.WithCancelCause`. Start `context.AfterFunc(r.Context(), ...)` to record "client gone" if the handler hasn't finished, use `WithTimeoutCause` for the server-side budget, and cancel with a shutdown cause from the server root. Log `context.Cause(ctx)` after the handler returns.
3. ```go
   func Merge(a, b context.Context) (context.Context, context.CancelFunc) {
       ctx, cancel := context.WithCancelCause(a) // a's cancellation propagates natively
       stop := context.AfterFunc(b, func() { cancel(context.Cause(b)) })
       return ctx, func() { stop(); cancel(context.Canceled) }
   }
   ```
   `AfterFunc` registers on `b`'s cancel tree (no goroutine if `b` is a standard context). The returned cancel must call `stop()` so the registration is removed. Limitations: `Deadline()` reports only `a`'s deadline, and when `b` times out, `Err()` is `Canceled` while `Cause` is `DeadlineExceeded`.

</details>

### Review Checklist

- [ ] Does every blocking operation accept and respect a ctx?
- [ ] Is every `With*` paired with `cancel`, called promptly in loops?
- [ ] Is there any `context.Background()` below the entry points? Is each one justified?
- [ ] Do background tasks started from requests use `WithoutCancel`, a timeout, and tracking?
- [ ] Are cancellation causes used so that "context canceled" in logs is diagnosable?
- [ ] Do retries respect the remaining deadline budget?

---

## Chapter 17: Timers and Tickers

### TL;DR

- **Go 1.23 changed timers** (when `go.mod` says `go 1.23` or later):
  1. Unreferenced `Timer`/`Ticker` values are **garbage-collected even if not stopped**, so `time.After` in a loop no longer leaks until expiry.
  2. Timer channels are **synchronous** (unbuffered). After `Stop` or `Reset` returns, you will **never** receive a stale value. The old "drain the channel" idiom is unnecessary.
- `time.AfterFunc(d, f)` runs `f` in **its own goroutine**. `Stop` does not wait for a running `f`.
- Tickers **drop ticks** for slow receivers. They don't queue them.
- Durations and timers use the **monotonic clock**, so wall-clock jumps (NTP, manual changes) don't affect them.
- Timers live in **per-P heaps** `[Go 1.14+]`, checked by the scheduler and integrated with the netpoller. They are cheap, but not free at millions per second.

### Under the Hood

- Each P keeps a heap of its timers. The scheduler runs expired timers (`(*timers).check` in `runtime/time.go`) from `findRunnable` and while stealing work. An idle M blocks in `netpoll` with a timeout equal to the next timer's deadline, so sleeping costs no polling.
- A timer channel's "send" now happens lazily, when a receiver is ready (this is how the 1.23 synchronous semantics work). `len(t.C)` and `cap(t.C)` report 0.
- To revert temporarily: `GODEBUG=asynctimerchan=1`. Behaviour is chosen by your main module's `go` line, so library code must work under both.

### Practical Code

#### Reset safely `[Go 1.23+]`

```go
t := time.NewTimer(time.Minute)
defer t.Stop()
// ...
t.Reset(5 * time.Second) // no Stop/drain dance needed; no stale fire will be delivered
```

Before 1.23 the "correct" sequence was `if !t.Stop() { <-t.C }; t.Reset(d)`, which itself deadlocked if the value had already been received. Library code that must support older module versions should track whether it has received from `t.C`.

#### Debounce: collapse bursts into one action

```go
// Debounce calls fn once, d after the last event. Run as a goroutine.
// Relies on Go 1.23+ timer semantics (main module go ≥ 1.23): with the old async
// channels, an expiry racing an event could leave a stale value and fire fn early.
func Debounce(ctx context.Context, events <-chan struct{}, d time.Duration, fn func()) {
	t := time.NewTimer(d)
	t.Stop() // start disarmed
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-events:
			if !ok {
				return
			}
			t.Reset(d) // each event pushes the deadline out
		case <-t.C:
			fn()
		}
	}
}
```

#### Throttle: at most once per interval, fire the latest

```go
func Throttle[T any](ctx context.Context, in <-chan T, every time.Duration, fn func(T)) {
	tick := time.NewTicker(every)
	defer tick.Stop()
	var latest T
	var pending bool
	for {
		select {
		case <-ctx.Done():
			return
		case v, ok := <-in:
			if !ok {
				if pending {
					fn(latest)
				}
				return
			}
			latest, pending = v, true
		case <-tick.C:
			if pending {
				fn(latest)
				pending = false
			}
		}
	}
}
```

#### Exponential backoff with full jitter

```go
func backoff(attempt int) time.Duration {
	const base, maxD = 100 * time.Millisecond, 10 * time.Second
	d := base << attempt // 100ms, 200ms, 400ms...
	if d > maxD || d <= 0 { // <= 0 guards shift overflow
		d = maxD
	}
	return rand.N(d) // math/rand/v2: uniform in [0, d): "full jitter"
}
```

Full jitter (AWS Architecture Blog) decorrelates retrying clients, preventing synchronized retry waves (the thundering herd).

#### AfterFunc: race-free stop

```go
type Session struct {
	mu      sync.Mutex
	expired bool
	timer   *time.Timer
}

func (s *Session) start(ttl time.Duration) {
	s.timer = time.AfterFunc(ttl, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.expired = true // runs in another goroutine: needs the lock
	})
}

func (s *Session) Touch(ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.expired {
		return
	}
	s.timer.Reset(ttl) // AfterFunc may already be running and blocked on mu;
	// it will then set expired=true right after we return. If that matters,
	// use a generation counter checked inside the callback.
}
```

### Pitfalls

> ⚠️ **Assuming `Stop` waits for `AfterFunc`'s callback.** It doesn't. If `Stop` returns false, the callback may be running right now. Synchronise inside the callback.

> ⚠️ **Ticker for "every N seconds of work".** A ticker fires on wall-time intervals, not after each piece of work completes. If work takes longer than the interval, ticks are dropped and the next work starts immediately. For "wait N after finishing", use `Sleep(ctx, d)` in the loop.

> ⚠️ **`time.Tick` in long-lived code.** It's fine since 1.23 (it's GC'd when unreferenced), but it can't be stopped. Prefer `NewTicker` + `defer Stop()` for clarity.

> ⚠️ **Thousands of timers for per-item timeouts.** A million connections each with its own `time.Timer`, all reset on every read, puts heavy load on the timer heaps. Use coarser timing wheels (e.g., check idle connections every second in one goroutine) for massive scale.

> ⚠️ **Library code assuming 1.23 semantics.** The behaviour depends on the *main module's* `go` version. Libraries must be correct under both old and new semantics.

### IRL Scenario: heartbeat storms after a deploy

**Context:** 5,000 agents each sent heartbeats every 30 s via `time.NewTicker(30*time.Second)`. All restarted together during a deploy, so every heartbeat landed in the same ~100 ms window every 30 s, spiking the collector to 50× baseline load.

**Fix:** Add a random initial delay (`rand.N(30*time.Second)`) before starting the ticker, plus ±10% jitter per beat (sleep-based loop with jitter instead of a fixed ticker). The load flattened.

**Lesson:** Any periodic behaviour in a *fleet* needs jitter: heartbeats, cache refreshes, cron-like jobs, retries.

### Exercises

1. ⭐ Why is `for { select { case <-time.After(time.Second): ...; case v := <-ch: ... } }` wasteful even after Go 1.23?
2. ⭐⭐ Implement `RateTicker` that ticks at a rate adjustable at runtime (`SetInterval(d)`) without leaking or skipping ticks.
3. ⭐⭐⭐ Implement a simple **hashed timing wheel** for 1M idle-connection timeouts with 1 s resolution. Compare memory and CPU with one `time.Timer` per connection.

<details>
<summary>Solutions</summary>

1. Every iteration allocates a new timer and channel and inserts into the timer heap. The timer also resets on every loop iteration, so if `ch` is busy the timeout never fires. That's an idle timeout, which may not be what you meant.
2. Use `time.Ticker.Reset(d)` (available since Go 1.15) from the owning goroutine, which receives `SetInterval` requests via a channel in its select loop.
3. Keep slots `[N][]connID` and a current index advanced by one goroutine each second. Each connection stores its slot and a `lastActive` time. On tick, scan only the current slot. Memory is O(connections) with tiny entries, and CPU is O(expiring) per tick, versus O(log n) heap work on every reset.

</details>

### Review Checklist

- [ ] What `go` version does the main module declare, and does library code avoid depending on timer semantics?
- [ ] Are timers in loops reused rather than recreated?
- [ ] Do AfterFunc callbacks synchronise their own state access?
- [ ] Does periodic fleet-wide behaviour have jitter?
- [ ] Do retries use capped exponential backoff with jitter?

---

[← Part IV](04-sync-primitives.md) · [Index](README.md) · [Next: Part VI, Patterns →](06-patterns.md)
