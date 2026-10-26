# Part XI: Mastery Drills

[← Part X](10-capstones.md) · [Index](README.md) · [Next: Appendices →](appendices.md)

- [Section A: Code-Review Gauntlet (18 snippets)](#section-a-code-review-gauntlet)
- [Section B: System-Design Drills (10 prompts)](#section-b-system-design-drills)

---

## Section A: Code-Review Gauntlet

**Rules:** For each snippet, before opening the answer, write down: (1) the bug, (2) a concrete interleaving or input that triggers it, (3) the fix. Score yourself: 2 points for the bug and a trigger, 1 for the bug alone. At 30+ out of 36 you're reviewing concurrent code at an advanced level.

Difficulty: 🟢 warm-up · 🟡 subtle · 🔴 nasty

---

### G1 🟢

```go
func (s *Store) UpdateAll(keys []string) {
	for _, k := range keys {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.data[k]++
	}
}
```

<details><summary>Answer</summary>

`defer` runs at **function** return, not at the end of each iteration. The second iteration's `Lock` deadlocks against the first iteration's still-held lock. Fix: lock once around the loop, or unlock explicitly per iteration (or move the body into a helper function with its own defer).

</details>

---

### G2 🔴

```go
func Enrich(ctx context.Context, ids []string) ([]Item, error) {
	g, ctx := errgroup.WithContext(ctx)
	items := make([]Item, len(ids))
	for i, id := range ids {
		g.Go(func() error {
			it, err := fetchItem(ctx, id)
			items[i] = it
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return items, audit.Log(ctx, "enriched", len(items))
}
```

<details><summary>Answer</summary>

The context returned by `errgroup.WithContext` is **cancelled when `Wait` returns**, even on success. `audit.Log(ctx, ...)` always gets a cancelled context and (if it respects ctx) always fails. The shadowing of `ctx` hides this. Fix: name it `gctx` and use the original `ctx` after `Wait`.

</details>

---

### G3 🟡

```go
type Registry struct {
	mu    sync.Mutex
	items []Item
}

func (r *Registry) Items() []Item {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.items
}

func (r *Registry) Add(it Item) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.items = append(r.items, it)
}
```

<details><summary>Answer</summary>

`Items` returns the internal slice, which shares the backing array. A caller ranging over it while `Add` appends (and writes into spare capacity of the same array) is a data race, and the lock ended when `Items` returned. Fix: return a copy (`slices.Clone(r.items)`), or switch to copy-on-write with an immutable published slice.

</details>

---

### G4 🟡

```go
// go.mod: go 1.21
func StartAll(handlers []Handler) {
	for _, h := range handlers {
		go func() {
			h.Start()
		}()
	}
}
```

<details><summary>Answer</summary>

Per-iteration loop variables depend on the **module's `go` version** (Go 1.22+). With `go 1.21` in `go.mod`, all goroutines share one `h` variable: they likely all start the *last* handler, and the variable is raced. The same source is correct under `go 1.22`. Fix: bump the `go` directive (and test!), or pass `h` as an argument. `go vet`'s loopclosure check flags this for older versions.

</details>

---

### G5 🟢

```go
var (
	once sync.Once
	db   *sql.DB
	err  error
)

func DB() (*sql.DB, error) {
	once.Do(func() {
		db, err = sql.Open("postgres", dsn())
		if err == nil {
			err = db.Ping()
		}
	})
	return db, err
}
```

<details><summary>Answer</summary>

If `Ping` fails once (e.g., the DB is briefly unavailable at startup), the error is cached forever and the process can never recover without a restart. Fix: retryable init (a mutex plus "initialised" flag that is only set on success), and don't make startup ping failure permanent.

</details>

---

### G6 🔴

```go
func handler(w http.ResponseWriter, r *http.Request) {
	results := make(chan string)
	go func() {
		results <- slowCompute(r.Context())
	}()
	select {
	case res := <-results:
		fmt.Fprint(w, res)
	case <-time.After(time.Second):
		go func() {
			res := <-results
			fmt.Fprint(w, "late: "+res)
		}()
		http.Error(w, "timeout", http.StatusGatewayTimeout)
	}
}
```

<details><summary>Answer</summary>

On timeout, a goroutine writes to `w` **after the handler returned**. The `ResponseWriter` is invalid after the handler returns, so this is a data race and may panic or corrupt another response on a reused connection. Also, `r.Context()` is cancelled when the handler returns, so `slowCompute` may return garbage. Fix: never touch `w` after return. Make `results` buffered(1) so the compute goroutine can finish and exit, and pass a ctx with the 1 s timeout into `slowCompute` so it stops.

</details>

---

### G7 🟡

```go
func (b *Batcher) Run(ctx context.Context) {
	for {
		select {
		case it := <-b.in:
			b.buf = append(b.buf, it)
		default:
			if len(b.buf) > 0 {
				b.flush()
			}
		}
		if ctx.Err() != nil {
			return
		}
	}
}
```

<details><summary>Answer</summary>

`default` turns the loop into a **busy spin**: 100% of a core when idle. It also flushes after almost every item (no real batching), and cancellation is only noticed between spins. Fix: block in select with `ctx.Done()`, `b.in`, and a ticker (or size threshold) for flushes.

</details>

---

### G8 🔴

```go
func Merge(ctx context.Context, a, b <-chan int) <-chan int {
	out := make(chan int)
	forward := func(in <-chan int) {
		for {
			select {
			case v, ok := <-in:
				if !ok {
					close(out)
					return
				}
				out <- v
			case <-ctx.Done():
				return
			}
		}
	}
	go forward(a)
	go forward(b)
	return out
}
```

<details><summary>Answer</summary>

1. **Two closers:** whichever input closes first closes `out`, and the other forwarder then panics sending to (or closing) a closed channel.
2. `out <- v` doesn't select on ctx, so it leaks if the consumer leaves.

Fix: a WaitGroup over both forwarders plus one closer goroutine, and `select { case out <- v: case <-ctx.Done(): return }`.

</details>

---

### G9 🟡

```go
type Conn struct {
	mu     sync.Mutex
	closed bool
}

type Pool struct {
	conns []Conn
}

func (p *Pool) CloseAll() {
	for _, c := range p.conns {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
	}
}
```

<details><summary>Answer</summary>

`range` copies each `Conn` (mutex included) into `c`. The code locks a copy and sets `closed` on the copy, so the pool's connections are never marked closed. `go vet` reports "range var c copies lock". Fix: `for i := range p.conns { c := &p.conns[i]; ... }`, or store `[]*Conn`.

</details>

---

### G10 🟡

```go
var lastErr atomic.Value

func record(err error) {
	lastErr.Store(err)
}

// elsewhere:
record(io.EOF)
record(fmt.Errorf("wrap: %w", io.EOF))
record(nil)
```

<details><summary>Answer</summary>

`atomic.Value` panics if you `Store` values of **different concrete types** (`*errors.errorString` vs `*fmt.wrapError`) and panics on `Store(nil)`. Fix: `atomic.Pointer[errBox]` with `type errBox struct{ err error }`, or a mutex-guarded `error` field.

</details>

---

### G11 🟢

```go
func ProcessAll(ctx context.Context, items []Item) {
	for _, it := range items {
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		process(ctx, it)
	}
}
```

<details><summary>Answer</summary>

**The bug:** `defer cancel()` in a loop. Every context and its timer stay alive until `ProcessAll` returns, so 100k items means 100k live timers and child registrations on the parent. Fix: call `cancel()` right after `process`, or move the body into a function that has its own `defer`.

**Red herring:** the shadowing `ctx, cancel := context.WithTimeout(ctx, ...)` looks as though each iteration might derive from the *previous* iteration's ctx, which would make the timeouts nest and shrink. It doesn't. A new variable's scope starts *after* its declaring statement, so the right-hand `ctx` is always the function parameter, and each iteration's `ctx` is a fresh variable scoped to that iteration. Recognising that this is *not* a bug is part of the drill.

</details>

---

### G12 🔴

```go
type Cache struct {
	mu sync.RWMutex
	m  map[string]*Entry
}

func (c *Cache) GetOrCreate(k string) *Entry {
	c.mu.RLock()
	e, ok := c.m[k]
	c.mu.RUnlock()
	if ok {
		return e
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e = newEntry(k)
	c.m[k] = e
	return e
}
```

<details><summary>Answer</summary>

There's no **re-check after acquiring the write lock**. Two goroutines can both miss, then each creates and stores an entry, and the second overwrites the first. Callers now hold different `*Entry` values for the same key, so updates to one are lost. Fix: after `Lock()`, check `c.m[k]` again before creating.

</details>

---

### G13 🟡

```go
func (s *Server) watchConfig(ctx context.Context) {
	go func() {
		t := time.NewTicker(10 * time.Second)
		for range t.C {
			s.reload()
		}
	}()
}
```

<details><summary>Answer</summary>

The goroutine **never exits**: `ctx` is ignored and `for range t.C` loops forever (ticker channels are never closed). In tests every Server leaks a goroutine, and in production reload continues after shutdown. Fix: `defer t.Stop()` and `for { select { case <-ctx.Done(): return; case <-t.C: s.reload() } }`, with the goroutine tracked (WaitGroup) so shutdown can wait.

</details>

---

### G14 🔴

```go
func (q *Queue) Pop() Item {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		q.cond.Wait()
	}
	it := q.items[0]
	q.items = q.items[1:]
	return it
}

func (q *Queue) Push(it Item) {
	q.mu.Lock()
	q.items = append(q.items, it)
	q.mu.Unlock()
	q.cond.Broadcast()
}
```

<details><summary>Answer</summary>

`if` instead of `for` around `cond.Wait()`. With `Broadcast` and several poppers, all of them wake. The first one takes the item, and the others proceed with an empty queue, so `q.items[0]` panics with index out of range. Fix: `for len(q.items) == 0 { q.cond.Wait() }`. (Also consider `Signal` for one item, and a channel-based design for cancellation.)

</details>

---

### G15 🟡

```go
func (w *Worker) Run(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	for _, chunk := range w.chunks { // 10,000 chunks, ~50ms each
		w.process(chunk)
	}
	return nil
}
```

<details><summary>Answer</summary>

Cancellation is checked **once**, at the start. After that, shutdown waits up to ~8 minutes. Fix: check `ctx.Err()` per chunk (or every N chunks), and pass ctx into `process` if it blocks.

</details>

---

### G16 🔴

```go
func (c *Cache) Get(k string) (V, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if v, ok := c.m[k]; ok {
		return v, nil
	}
	return c.load(k) // load takes c.mu.Lock() to store the result
}
```

<details><summary>Answer</summary>

An **RLock → Lock upgrade** in the same goroutine: `load` calls `Lock` while this goroutine still holds `RLock`, and it waits forever for readers (itself included) to leave. This is a deterministic self-deadlock on every miss. Go's RWMutex has no upgrade. Fix: release the read lock before loading (no defer, or a separate function), then re-check under the write lock (see G12).

</details>

---

### G17 🟡

```go
func StartWorkers(n int, jobs <-chan Job, wg sync.WaitGroup) {
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				j.Do()
			}
		}()
	}
}

// caller:
var wg sync.WaitGroup
StartWorkers(4, jobs, wg)
close(jobs)
wg.Wait() // returns immediately
```

<details><summary>Answer</summary>

The WaitGroup is **passed by value**. `Add` and `Done` operate on the copy inside `StartWorkers`, so the caller's `wg` stays at zero and `Wait` returns immediately while the workers are still running. `go vet` (copylocks) flags this. Fix: pass `*sync.WaitGroup`, or have `StartWorkers` return a wait function.

</details>

---

### G18 🔴

```go
type Server struct {
	handlers map[string]Handler // populated during setup
	ready    bool
}

func (s *Server) Register(name string, h Handler) { s.handlers[name] = h }

func (s *Server) Start() {
	go s.serve() // reads s.handlers and checks s.ready on each request
	s.Register("health", healthHandler)
	s.ready = true
}
```

<details><summary>Answer</summary>

`serve` starts **before** the remaining setup writes (`Register`, `ready = true`). Those writes race with `serve`'s reads: there's no happens-before edge after the `go` statement. A concurrent map read and write can also crash the program (`fatal error: concurrent map read and map write`). Fix: complete all setup **before** `go s.serve()` (rule 2 of the memory model: the `go` statement happens-before the goroutine starts, which publishes everything written before it). If late registration is needed, use a mutex or copy-on-write.

</details>

---

## Section B: System-Design Drills

**Format:** Spend 25–40 minutes per prompt. Produce: (1) clarifying questions, (2) a diagram, (3) a goroutine and ownership inventory, (4) primitives and why, (5) failure modes and the overload policy, (6) how you'd test and observe it. Then compare with the outline.

---

### D1: Per-tenant rate limiter at 1M req/s on 64 cores

<details><summary>Outline</summary>

- **Clarify:** strict vs approximate limits? How many tenants (10k? 10M)? Is the distribution skewed (a few huge tenants)? Is there a global limit across replicas?
- **Design:** a sharded map (256+ shards, `maphash`) of `tenant → limiter`, plus a lazy-refill token bucket (store `tokens, last` and compute the refill on access, so there are no timers). Hot tenants get **split budgets** (N sub-buckets at rate/N, chosen randomly) to kill single-key contention. Idle-entry eviction is incremental (per shard, a few keys per op, or a janitor that holds one shard at a time).
- **Distributed:** a local limit = global/replicas, periodically rebalanced from a central counter (Redis or a gossip protocol). Accept approximation.
- **Failure modes:** a hot key defeating shards, unbounded tenant cardinality (eviction), clock monotonicity (use `time.Now()`'s monotonic reading).
- **Test:** benchmarks with `-cpu=1..64` and skewed key distributions; synctest for refill math.
- **What's being assessed:** ability to name the hot-key problem and the approximate-vs-exact trade-off.

</details>

---

### D2: Log shipper (tail files → batch → ship), at-least-once, bounded memory

<details><summary>Outline</summary>

- **Pipeline:** per-file tailer goroutines → bounded channel → batcher (size/time triggers) → sender pool (N, with retries, backoff and a circuit breaker) → ack → checkpoint (file offset) persisted after ack.
- **Backpressure:** sender slow → batcher blocks → channel full → tailers *pause reading* (files are the durable buffer, so no drops are needed).
- **At-least-once:** checkpoint only offsets that have been acknowledged. On restart, re-ship from the checkpoint, and the receiver dedupes by (file, offset).
- **Ordering:** per-file ordering is preserved if each file's batches go through one sender lane (hash file → lane).
- **Shutdown:** stop tailers → flush batcher → wait for senders with a budget → persist checkpoints.
- **Failure modes:** file rotation/truncation (inode tracking), a remote outage (breaker, then pause), a poison record (dead-letter).
- **Observe:** lag per file (bytes behind), queue depth, send latency, retry rate.

</details>

---

### D3: Connection pool (like `database/sql`)

<details><summary>Outline</summary>

- **State (mutex-guarded):** `open count`, `idle []*Conn` (LIFO for warm conns), `waiters` (FIFO queue of `chan *Conn`, each buffered(1)), `closed`.
- **Get(ctx):** idle available → pop. Else if `open < max` → increment `open`, unlock, dial (outside the lock!), and on failure decrement. Else enqueue a waiter channel, unlock, `select { conn := <-w; <-ctx.Done() }`. On ctx done, remove the waiter under the lock. If a conn was delivered concurrently, **return it to the pool** (a classic race).
- **Put(conn):** if there's a waiter → hand off directly. Else if idle < maxIdle → push. Else close.
- **Health:** max lifetime and idle timeout via a janitor. Validate on checkout if stale.
- **Close:** mark closed, close idle conns, fail the waiters, and close in-use conns when they're returned.
- **What's being assessed:** dialing outside the lock, the waiter-cancellation-vs-handoff race, FIFO fairness.

</details>

---

### D4: WebSocket fan-out to 100k clients

<details><summary>Outline</summary>

- **Per connection:** a reader goroutine and a writer goroutine with a bounded outbound channel (e.g., 64). A slow client whose buffer fills gets disconnected (it resyncs on reconnect).
- **Rooms/topics:** sharded subscription maps (topic → set of conns). Publish does a non-blocking send to each subscriber's channel outside any long-held lock (snapshot subscribers, then send).
- **Memory:** 100k × (2 goroutines × ~8 KB + buffers) ≈ 2–3 GB. Consider epoll-based libraries (gnet/gobwas) for 1M+ connections, which avoid a goroutine per connection.
- **Encoding once:** serialize each message once and share the immutable `[]byte` across subscribers.
- **Heartbeats:** one ticker per shard (timing wheel), not per connection.
- **Shutdown:** send a close frame, drain with a short budget, then hard close.

</details>

---

### D5: Leader-only background job with a lease

<details><summary>Outline</summary>

- Leadership is a **context**: `leaderCtx` is cancelled when the lease is lost (renewal failed or a fencing token was superseded).
- The job runs `Run(leaderCtx)`, checks ctx between steps, and every external write carries the **fencing token**, so a stale leader's in-flight writes are rejected by the storage.
- **Renewal goroutine:** renew at TTL/3. If renewal fails, cancel `leaderCtx` *before* the lease actually expires (with a safety margin for clock skew).
- **Re-election:** a loop of acquire → run → wait for loss → back off.
- **What's being assessed:** knowing that ctx cancellation alone is **not enough** (a GC pause or network delay can let a stale leader act), which is why fencing tokens are needed.

</details>

---

### D6: Parallel indexer for 10M files

<details><summary>Outline</summary>

- **Stages:** walker (1–4 goroutines, `filepath.WalkDir`) → path channel → readers (bounded by I/O: ~32–128, a semaphore around open/read since file I/O blocks threads) → parsers (CPU: GOMAXPROCS) → per-parser **local index shards** → final merge.
- **Avoid contention:** each parser builds its own partial index (no shared map), and there's a k-way merge at the end, or periodic spill-to-disk plus merge (an external sort when memory-bound).
- **Bound memory:** bounded channels, a per-reader buffer pool, and a cap on file size or streaming parse.
- **Fault tolerance:** errors are collected per file (not fail-fast), with progress checkpoints.
- **Observe:** files/s per stage, which reveals the bottleneck stage (Amdahl).

</details>

---

### D7: DataLoader-style batching for DB lookups

<details><summary>Outline</summary>

- `Load(ctx, id)` registers in a pending batch (mutex-guarded map `id → []chan result`). The first registration in a window arms a timer (1–2 ms) or triggers at a max batch size.
- On flush, a swap-out batch under the lock → one `SELECT ... WHERE id IN (...)` → fan out results to each waiter's buffered channel. Missing IDs get not-found.
- **Per-caller ctx:** callers select on their result channel and their ctx. The batch query runs under a detached ctx with a timeout.
- Dedupe identical IDs within a batch, and optionally layer a request-scoped cache on top.
- **Trade-off:** adds up to the window's latency to each lookup, in exchange for fewer round-trips. Tune by load.

</details>

---

### D8: Refresh-ahead cache, 50k rps, slow origin (p99 2 s)

<details><summary>Outline</summary>

- A sharded LRU/TTL cache plus singleflight per key plus **stale-while-revalidate** (serve stale and refresh in the background) plus XFetch probabilistic early refresh plus a hard max-staleness (when exceeded, block or error).
- A refresh-worker pool with its own bulkhead (bounded concurrency to the origin) and a circuit breaker. When the breaker is open, keep serving stale and extend the TTL.
- **Negative caching** for not-found results (short TTL) to stop miss storms.
- **Fleet:** jitter TTLs per pod, and optionally a shared L2 (Redis) with its own singleflight per pod.
- **Observe:** hit ratio, stale-serve ratio, refresh latency, origin concurrency.

</details>

---

### D9: Hot config reload across many components

<details><summary>Outline</summary>

- `atomic.Pointer[Config]`, with immutable config snapshots. A reader loads once per request, getting a consistent snapshot for that request.
- **Validation before publish:** parse → validate → (optionally) dry-run components → swap. Keep the last good config on failure.
- Components that need to *react* (resize pools, reopen files) subscribe through a notifier (close-and-replace channel or coalescing cap-1 channel). Each one reloads in its own goroutine and reports success or failure.
- **Ordering:** if components depend on each other, reload in dependency order or use versioned configs that each component acks.
- **Pitfall:** a component caches `cfg := store.Load()` in a long-lived struct and never sees updates.

</details>

---

### D10: Order-matching engine (low latency, deterministic)

<details><summary>Outline</summary>

- A **single-threaded core** per instrument: one goroutine owns the order book, with no locks on the hot path. That gives determinism (replayable from the input log) and cache locality.
- **Concurrency at the edges:** network readers parse and validate in parallel, then enqueue to the instrument's inbox (bounded, SPSC/MPSC ring or channel). Output events go to publisher goroutines.
- **Sequencing:** an input journal (write-ahead) is assigned sequence numbers before processing, which enables replay, replication and recovery.
- **Latency:** pin the core goroutine (`LockOSThread`, plus CPU affinity at the process level), avoid allocation in the core loop (preallocated order pools), GOGC tuning or a memory arena approach, and busy-poll the inbox when ultra-low latency justifies burning a core.
- **What's being assessed:** recognising that the *best* concurrency design for the core is **none** (the LMAX insight), with parallelism only where work is independent.

</details>

---

[← Part X](10-capstones.md) · [Index](README.md) · [Next: Appendices →](appendices.md)
