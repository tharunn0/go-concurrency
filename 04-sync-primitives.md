# Part IV: sync Primitives, Deeply

[← Part III](03-channels-and-select.md) · [Index](README.md) · [Next: Part V, context & Time →](05-context-and-time.md)

- [Chapter 12: Mutex and RWMutex](#chapter-12-mutex-and-rwmutex)
- [Chapter 13: WaitGroup, Once, Cond](#chapter-13-waitgroup-once-cond)
- [Chapter 14: sync.Pool and sync.Map](#chapter-14-syncpool-and-syncmap)
- [Chapter 15: Atomics and Lock-Free Programming](#chapter-15-atomics-and-lock-free-programming)

---

## Chapter 12: Mutex and RWMutex

### TL;DR

- `sync.Mutex` is an 8-byte struct (two 32-bit fields: `state` and `sema`): a CAS fast path, brief spinning, then parking on a runtime semaphore.
- **Two modes.** *Normal* mode: woken waiters compete with newly arriving goroutines, and newcomers usually win (throughput). *Starvation* mode: entered when a waiter has waited **> 1 ms**; ownership is **handed off** directly in FIFO order (fairness).
- Mutexes are **not reentrant**. Locking twice in one goroutine deadlocks.
- `RWMutex` is **writer-preferring**: once a writer is waiting, new readers block. Recursive read-locking can therefore deadlock.
- `RWMutex` is not automatically faster. Readers write a shared counter, so for short critical sections on many cores, a plain `Mutex` often wins. Measure before choosing.

### Mental Model

```
Mutex.state (int32):
  bit 0: locked     bit 1: woken     bit 2: starving     bits 3..31: waiter count

Normal mode:                          Starvation mode (waiter waited > 1ms):
 Unlock → wake one waiter              Unlock → hand ownership directly to head waiter
 waiter must compete with newcomers    newcomers don't spin or grab; they queue at tail
 (newcomers are on CPU: they win)      exit when the waiter is last or waited < 1ms
 → high throughput, possible tail lat  → bounded tail latency, lower throughput
```

### Under the Hood (`internal/sync/mutex.go`, used by `sync.Mutex` since Go 1.24)

**Lock:**
1. **Fast path:** `CAS(state, 0, mutexLocked)`. Uncontended locking is one atomic op.
2. **Slow path (`lockSlow`):**
   - **Spin** up to 4 iterations (`runtime_canSpin`) *only if* multicore, GOMAXPROCS > 1, at least one other P is running, and the local run queue is empty. While spinning, set `mutexWoken` so `Unlock` doesn't bother waking someone.
   - Otherwise, increment the waiter count and park on the semaphore (`runtime_SemacquireMutex`).
   - A waiter that was woken but **lost** the race re-queues at the **front** (LIFO requeue, `queueLifo`), so it doesn't lose its place.
   - If it has waited more than `starvationThresholdNs = 1ms`, it sets `mutexStarving`.

**Unlock:**
1. **Fast path:** `atomic.Add(state, -mutexLocked)`. If the result is 0 (no waiters, no flags), done.
2. **Slow path:** in normal mode, wake one waiter if nobody is already woken or holding the lock. In starvation mode, `semrelease(handoff=true)` gives the lock *directly* to the next waiter and yields the P to it.

**RWMutex** (`sync/rwmutex.go`):

```go
type RWMutex struct {
	w           Mutex        // held by writers
	writerSem   uint32
	readerSem   uint32
	readerCount atomic.Int32 // number of pending readers; negative when writer pending
	readerWait  atomic.Int32 // departing readers the writer is waiting for
}
```
- `RLock`: `readerCount.Add(1)`. If the result is negative, a writer is pending, so park on `readerSem`.
- `Lock`: take `w`, then `readerCount.Add(-rwmutexMaxReaders)` to announce the writer (this makes new readers block), then wait for the active readers to drain.
- **Every `RLock`/`RUnlock` is an atomic write to `readerCount`.** On many cores with tiny read sections, this one cache line ping-pongs (high κ, Chapter 1).

### Practical Code

#### Idiomatic layout: the mutex sits above the fields it guards

```go
type Registry struct {
	name string // immutable after construction; no lock needed

	mu       sync.Mutex
	services map[string]*Service // guarded by mu
	version  uint64              // guarded by mu
}

func (r *Registry) Register(s *Service) {
	r.mu.Lock()
	defer r.mu.Unlock() // open-coded defers (Go 1.14+): ~free
	r.services[s.Name] = s
	r.version++
}
```

#### Keep I/O out of critical sections: snapshot, then work

```go
// ❌ Holds the lock during network I/O: every Register blocks behind a slow subscriber.
func (r *Registry) NotifyBad(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, s := range r.services {
		s.Ping(ctx) // network call under lock
	}
}

// ✅ Copy what you need under the lock, release, then do I/O.
func (r *Registry) Notify(ctx context.Context) {
	r.mu.Lock()
	snapshot := make([]*Service, 0, len(r.services))
	for _, s := range r.services {
		snapshot = append(snapshot, s)
	}
	r.mu.Unlock()

	for _, s := range snapshot {
		s.Ping(ctx)
	}
}
```

#### Lock with context or timeout (when you genuinely need it)

`sync.Mutex` can't be cancelled. If acquisition must respect ctx, use a 1-slot channel or `semaphore.Weighted`:

```go
type CtxMutex struct{ ch chan struct{} }

func NewCtxMutex() *CtxMutex { return &CtxMutex{ch: make(chan struct{}, 1)} }

func (m *CtxMutex) Lock(ctx context.Context) error {
	select {
	case m.ch <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *CtxMutex) Unlock() { <-m.ch }
```

### Pitfalls

> ⚠️ **Copying a mutex.** Value receivers, struct copies, or `range` over a slice of structs containing a mutex all copy the lock state, so you end up with two independent locks. `go vet` (`copylocks`) catches most cases. Run it in CI.
> ```go
> func (c Counter) Inc() { c.mu.Lock(); c.n++; c.mu.Unlock() } // value receiver: locks a copy
> ```

> ⚠️ **Recursive RLock with a pending writer: deadlock.**
> ```go
> func (c *Cache) Get(k string) V {
>     c.mu.RLock(); defer c.mu.RUnlock()
>     return c.getLocked(k) // fine
> }
> func (c *Cache) GetTwo(a, b string) (V, V) {
>     c.mu.RLock(); defer c.mu.RUnlock()
>     return c.Get(a), c.Get(b) // nested RLock: if a writer arrives between, everyone deadlocks
> }
> ```
> The writer waits for the outer reader, and the inner `RLock` waits for the writer. Convention: exported methods lock, and `xxxLocked` helpers assume the lock is held and never lock.

> ⚠️ **`TryLock` as a design tool.** It exists (`[Go 1.18+]`), and its docs say correct uses are rare. Usually it is a sign of a missing ownership design.

> ⚠️ **Unlock in a different goroutine.** It's legal, but very hard to review. Use a channel or semaphore if ownership truly crosses goroutines.

> 🤖 **AI note:** The most frequent AI mutex bugs are: (1) a value receiver on a struct with a mutex; (2) a write under `RLock` (e.g., updating a hit counter or LRU position); (3) an early `return` before a manual `Unlock` (no defer); (4) calling an exported (locking) method from inside another locked method.

### IRL Scenario: "RWMutex made it slower"

**Context:** A feature-flag service on 64 cores served 2M flag lookups/s from a `map` behind `sync.Mutex`. An engineer switched to `RWMutex` since "it's 99.99% reads". CPU went *up* 40% and p99 latency doubled.

**Diagnosis:** Each lookup's critical section is ~20 ns. With RWMutex, every lookup does two atomic RMWs on `readerCount` (RLock + RUnlock) on a single cache line shared by 64 cores. That coherency traffic dwarfed the critical section.

**Fix:** Flags change rarely, so copy-on-write: `atomic.Pointer[map[string]Flag]`. Reads are a single atomic load with no shared writes, and writers build a new map and swap it in. Lookup cost dropped to ~5 ns and scaled linearly.

**Rule of thumb:**
- Short critical section + many cores → `Mutex`, sharding, or copy-on-write.
- Long read section (scanning a big structure) + rare writes → `RWMutex` shines.
- Read-mostly with rare writes and small data → `atomic.Pointer` copy-on-write.

### Review Drill

```go
type Stats struct {
	mu    sync.Mutex
	count map[string]int
}

func (s *Stats) Incr(key string) {
	s.mu.Lock()
	if s.count == nil {
		s.count = make(map[string]int)
	}
	s.count[key]++
	if s.count[key] > 1000 {
		s.alert(key) // sends to Slack via HTTP
		return
	}
	s.mu.Unlock()
}
```

<details>
<summary>What's wrong?</summary>

1. **The lock is never released on the `> 1000` path.** The next `Incr` deadlocks forever. Use `defer s.mu.Unlock()`.
2. **HTTP call under the lock.** Even with defer, every `Incr` would wait for Slack. Decide under the lock (`shouldAlert := ...`), unlock, then alert.
3. The alert fires on *every* increment above 1000. You probably want it to fire once per threshold crossing.

</details>

### Exercises

1. ⭐ Why does `sync.Mutex` prefer newcomers in normal mode?
2. ⭐⭐ Write a benchmark comparing `Mutex`, `RWMutex`, and `atomic.Pointer` copy-on-write for a 99% read workload at critical-section sizes of 10 ns, 1 µs, and 50 µs, at GOMAXPROCS 1, 8, and NumCPU. Make a table of the results.
3. ⭐⭐⭐ Write a program that deadlocks via recursive `RLock` reliably (hint: force the writer in between with a channel), then make `go test` catch it with a timeout.

<details>
<summary>Solutions</summary>

1. A newcomer is already running on a CPU with a warm cache. Handing the lock to a parked goroutine costs a wake-up (µs) during which the lock sits idle. Letting the running goroutine take it maximises throughput, and starvation mode bounds the resulting unfairness.
2. Expected pattern: at 10 ns, COW ≫ Mutex > RWMutex on many cores. At 1 µs, RWMutex starts beating Mutex. At 50 µs, RWMutex wins clearly, and COW stays fastest for reads but has expensive writes.
3. Reader: `RLock`; signal "got outer"; wait for "writer started"; `time.Sleep(10ms)`; `RLock` again. Writer: wait for "got outer"; signal "writer started"; `Lock()`. Run with `go test -timeout 5s`. The panic dump shows `sync.(*RWMutex).RLock` and `sync.(*RWMutex).Lock` stacks.

</details>

### Review Checklist

- [ ] Is the lock scope minimal, with no I/O, no channel sends, and no callbacks into unknown code under the lock?
- [ ] Is the protected state documented ("guarded by mu")?
- [ ] Are there `xxxLocked` helpers instead of nested locking?
- [ ] Is RWMutex justified by measurement?
- [ ] Is `go vet` (copylocks) in CI?
- [ ] For multiple locks: is there a documented lock order?

---

## Chapter 13: WaitGroup, Once, Cond

### TL;DR

- **WaitGroup:** `Add` must *happen before* `Wait` (call it in the parent, not inside the goroutine). `wg.Go(f)` `[Go 1.25+]` removes that whole class of bugs. `go vet`'s `waitgroup` analyzer `[Go 1.25+]` flags `Add` inside the spawned goroutine.
- **Once:** `Do(f)` runs `f` exactly once, and every caller waits until it finishes. If `f` panics, the Once is still considered *done*. Prefer `sync.OnceFunc` / `OnceValue` / `OnceValues` `[Go 1.21+]`.
- **Cond:** rarely the right tool in Go. It can't be selected on, it can't be cancelled, and it's easy to misuse. Prefer channels (close-to-broadcast) unless you have many waiters on a condition over complex locked state and have measured.

### Under the Hood

- **WaitGroup:** `state atomic.Uint64` packs the counter (high 32 bits) and the waiter count (low bits; since 1.25 the top bit of the low half is a synctest bubble flag), plus a semaphore. `Wait` increments waiters and parks. When the counter hits 0 via `Done`, all waiters are released. A negative counter panics. Misuse is also detected: `Add` racing with `Wait` panics with "Add called concurrently with Wait", and reusing the group before a previous `Wait` has returned panics with "WaitGroup is reused before previous Wait has returned".
- **Once:** `done atomic.Bool` (it was a `uint32` in older releases) + `m Mutex`. The fast path is an atomic load of `done`. The slow path takes the mutex, re-checks, runs `f`, and sets `done` with a deferred store. That gives the guarantee "if `Do` returns, `f` has completed", which a CAS-only implementation would not.
- **OnceFunc / OnceValue:** wrappers around `Once` that additionally **re-panic with the same value on every call** if `f` panicked. That is much safer than `Once`'s silent "done but broken" state.

### Practical Code

#### WaitGroup: correct and modern

```go
// Pre-1.25 idiom
var wg sync.WaitGroup
for _, item := range items {
	wg.Add(1) // in the parent, before go
	go func() {
		defer wg.Done()
		process(item)
	}()
}
wg.Wait()

// Go 1.25+
var wg sync.WaitGroup
for _, item := range items {
	wg.Go(func() { process(item) })
}
wg.Wait()
```

When you need errors, cancellation, or a limit, you want `errgroup` (Chapter 19), not WaitGroup.

#### Lazy initialisation

```go
// Value + error, computed once, thread-safe, re-panics if loader panicked.
var loadSchema = sync.OnceValues(func() (*Schema, error) {
	return parseSchema("schema.json")
})

func Validate(doc []byte) error {
	s, err := loadSchema()
	if err != nil {
		return fmt.Errorf("schema unavailable: %w", err)
	}
	return s.Validate(doc)
}
```

> ⚠️ `OnceValues` caches the **error too**. If the load can fail transiently, you'll be stuck with the error forever. For retryable initialisation, use a mutex + "loaded" flag, or `singleflight` (Chapter 21).

#### Cond: correct use (and the channel alternative)

```go
// Bounded queue with Cond: correct pattern, always Wait in a loop.
type Queue struct {
	mu       sync.Mutex
	notEmpty *sync.Cond
	items    []int
}

func NewQueue() *Queue {
	q := &Queue{}
	q.notEmpty = sync.NewCond(&q.mu)
	return q
}

func (q *Queue) Put(v int) {
	q.mu.Lock()
	q.items = append(q.items, v)
	q.mu.Unlock()
	q.notEmpty.Signal()
}

func (q *Queue) Get() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.items) == 0 { // loop: spurious/competing wakeups
		q.notEmpty.Wait() // atomically unlocks mu, parks, relocks on wake
	}
	v := q.items[0]
	q.items = q.items[1:]
	return v
}
```

The problem is that `Get` can't time out or be cancelled. The cancellable alternative is a **"broadcast by close-and-replace"** channel:

```go
type Notifier struct {
	mu sync.Mutex
	ch chan struct{}
}

func NewNotifier() *Notifier { return &Notifier{ch: make(chan struct{})} }

// Wait returns a channel that closes on the next Broadcast.
func (n *Notifier) Wait() <-chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.ch
}

func (n *Notifier) Broadcast() {
	n.mu.Lock()
	close(n.ch)
	n.ch = make(chan struct{})
	n.mu.Unlock()
}

// Usage: check state, then wait for change OR cancellation.
for !condition() {
	select {
	case <-notifier.Wait():
	case <-ctx.Done():
		return ctx.Err()
	}
}
```

> ⚠️ There's a lost-wakeup window in the usage above: if the state changes between `condition()` and `Wait()`, you miss it. Fix it by grabbing `ch := notifier.Wait()` **before** checking `condition()`, then selecting on `ch`.

### Pitfalls

> ⚠️ **`wg.Add(1)` inside the goroutine.** The parent may reach `Wait` before any goroutine has run `Add`, so `Wait` returns immediately. Vet now flags this.

> ⚠️ **Passing a WaitGroup by value.** `func worker(wg sync.WaitGroup)` calls `Done` on a copy, so the real one never reaches zero.

> ⚠️ **Recursive `Once.Do`.** Calling `once.Do(f)` from inside `f` deadlocks (it waits on its own mutex).

> ⚠️ **`Cond.Wait` without a loop**, or **`Signal` when `Broadcast` is needed** (several waiters with different conditions). Both produce lost wakeups.

### IRL Scenario: the init that failed once and never recovered

**Context:** A service used `sync.Once` to connect to a secrets manager at first use. During a 20 s secrets-manager outage at startup, the first call failed, and every request afterwards got "secrets client is nil" until a restart, which took 6 hours to notice because health checks didn't exercise that path.

**Fix:**
```go
type lazyClient struct {
	mu sync.Mutex
	c  *Client
}

func (l *lazyClient) Get(ctx context.Context) (*Client, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.c != nil {
		return l.c, nil
	}
	c, err := Dial(ctx) // retried on the next call if it fails
	if err != nil {
		return nil, err
	}
	l.c = c
	return c, nil
}
```
Plus a readiness probe that actually calls `Get`. If concurrent first calls should share one dial, use `singleflight` around `Dial`.

### Exercises

1. ⭐ What happens if `f` passed to `once.Do` panics, then another goroutine calls `once.Do(f)`? What about `sync.OnceFunc(f)` called twice?
2. ⭐⭐ Implement `WaitTimeout(wg *sync.WaitGroup, d time.Duration) bool`. What resource does it leak on timeout, and is that acceptable?
3. ⭐⭐⭐ Implement a reusable barrier for N goroutines (all wait until N have arrived, then all proceed, and the barrier resets) using only `sync.Mutex` and channels.

<details>
<summary>Solutions</summary>

1. `once.Do`: the first panic propagates. The second call returns immediately without calling `f`, because the Once is done. `OnceFunc`: the first call panics, and **every** later call panics with the same value.
2. ```go
   func WaitTimeout(wg *sync.WaitGroup, d time.Duration) bool {
       done := make(chan struct{})
       go func() { wg.Wait(); close(done) }()
       t := time.NewTimer(d); defer t.Stop()
       select {
       case <-done: return true
       case <-t.C:  return false
       }
   }
   ```
   On timeout, the helper goroutine stays blocked until the WaitGroup eventually finishes, or forever if it never does. That's acceptable only if the group does finish eventually. Usually the better fix is to make the workers cancellable.
3. ```go
   type Barrier struct {
       mu    sync.Mutex
       n, c  int
       gate  chan struct{}
   }
   func NewBarrier(n int) *Barrier { return &Barrier{n: n, gate: make(chan struct{})} }
   func (b *Barrier) Await() {
       b.mu.Lock()
       gate := b.gate
       b.c++
       if b.c == b.n {
           b.c = 0
           b.gate = make(chan struct{}) // new generation
           close(gate)                  // release this generation
           b.mu.Unlock()
           return
       }
       b.mu.Unlock()
       <-gate
   }
   ```
   The *generation* channel is what makes it reusable: late wakers of generation k can't be confused with generation k+1.

</details>

### Review Checklist

- [ ] Is `wg.Go` used (Go 1.25+), or is `Add` called before `go`?
- [ ] Is failed one-time initialisation retryable when it needs to be?
- [ ] Is every `Cond.Wait` in a loop, and could a channel replace the Cond?
- [ ] Are WaitGroups and Onces never copied?

---

## Chapter 14: sync.Pool and sync.Map

### TL;DR

- **`sync.Pool`** is a per-P cache of reusable objects that **the GC clears** (objects survive about one extra cycle via the victim cache). Use it to cut allocation of short-lived, frequently created objects. It is **not** a connection pool or object lifecycle manager.
- Always **reset** objects before reuse, store **pointers**, and **don't return huge buffers** to the pool.
- **`sync.Map`** is optimised for (1) write-once/read-many keys or (2) goroutines working on disjoint key sets. Since Go 1.24 it is built on a concurrent hash-trie, which made it more competitive, but it is still untyped (`any`), so a typed `map` + `Mutex` or a sharded map is often clearer and just as fast.

### Under the Hood

#### sync.Pool (`sync/pool.go`, `poolqueue.go`)

```
Pool
 └── local [P]poolLocal  (padded to avoid false sharing)
       ├── private any          ← only the owning P; no sync needed (P is pinned)
       └── shared  poolChain    ← lock-free deque: owner pushes/pops head; others steal tail
 └── victim [P]poolLocal  ← previous generation, cleared at next GC
```

- **Get:** pin to the current P (disable preemption) → `private` → pop head of own `shared` → steal from other Ps' `shared` tails → check victim caches → `New()`.
- **Put:** pin → if `private` is empty, put it there, else push to the head of `shared`.
- **GC (`poolCleanup`, during STW):** `victim = local; local = empty`. Objects unused across two GCs are freed. This victim cache `[Go 1.13+]` removed the "pool is empty after every GC" latency cliff.

#### sync.Map

- **Go ≤1.23:** a read-only map (atomic, lock-free reads) plus a dirty map behind a mutex. Misses on the read map are counted, and after enough misses the dirty map is promoted. That was great for stable key sets and bad for churn.
- **Go 1.24+:** reimplemented on `internal/sync.HashTrieMap`, a concurrent hash-trie with lock-free reads and fine-grained locking on writes. It performs much better under mixed workloads. The API is the same.
- **API notes:** `LoadOrStore`, `LoadAndDelete`, `Swap`, `CompareAndSwap`, `CompareAndDelete` `[Go 1.20+]`, `Clear` `[Go 1.23+]`. `Range` is **not** a consistent snapshot.

### Practical Code

#### Buffer pool done right

```go
var bufPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) }, // pointer type
}

const maxPooledCap = 64 << 10 // don't keep buffers that grew past 64 KiB

func render(w io.Writer, v any) error {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset() // ALWAYS reset: previous user's data is still in there
	defer func() {
		if buf.Cap() <= maxPooledCap {
			bufPool.Put(buf)
		}
	}()

	if err := json.NewEncoder(buf).Encode(v); err != nil {
		return err
	}
	_, err := w.Write(buf.Bytes())
	return err
}
```

#### A typed generic Pool wrapper

```go
type Pool[T any] struct {
	p     sync.Pool
	reset func(*T)
}

func NewPool[T any](reset func(*T)) *Pool[T] {
	return &Pool[T]{
		p:     sync.Pool{New: func() any { return new(T) }},
		reset: reset,
	}
}

func (p *Pool[T]) Get() *T  { return p.p.Get().(*T) }
func (p *Pool[T]) Put(x *T) { p.reset(x); p.p.Put(x) }
```

#### Typed wrapper over sync.Map

```go
type SyncMap[K comparable, V any] struct{ m sync.Map }

func (s *SyncMap[K, V]) Load(k K) (V, bool) {
	v, ok := s.m.Load(k)
	if !ok {
		var zero V
		return zero, false
	}
	val, _ := v.(V) // comma-ok: a stored nil interface would make v.(V) panic
	return val, true
}

func (s *SyncMap[K, V]) LoadOrStore(k K, v V) (V, bool) {
	a, loaded := s.m.LoadOrStore(k, v)
	return a.(V), loaded
}

func (s *SyncMap[K, V]) Store(k K, v V) { s.m.Store(k, v) }
func (s *SyncMap[K, V]) Delete(k K)     { s.m.Delete(k) }
```

#### `LoadOrStore` with expensive values: avoid constructing per call

```go
// ❌ Builds a new expensive limiter on every call, even when one exists.
lim, _ := limiters.LoadOrStore(key, rate.NewLimiter(10, 20))

// ✅ Fast path Load; only construct on miss.
func limiterFor(key string) *rate.Limiter {
	if v, ok := limiters.Load(key); ok {
		return v.(*rate.Limiter)
	}
	v, _ := limiters.LoadOrStore(key, rate.NewLimiter(10, 20)) // may lose the race; that's fine
	return v.(*rate.Limiter)
}
```

### Pitfalls

> ⚠️ **Not resetting pooled objects: a data leak between requests.** A pooled struct that held user A's auth token, handed to user B's request, is a **security incident**. Reset in `Put` or right after `Get`, consistently.

> ⚠️ **Putting non-pointer values.** `pool.Put(buf)` where `buf` is a `[]byte` allocates a slice header to box it into `any` on every Put (staticcheck SA6002). Pool `*[]byte` or `*bytes.Buffer`.

> ⚠️ **Unbounded buffer growth.** One 50 MB request grows a pooled buffer to 50 MB, and the pool then keeps it alive indefinitely. Cap what you return.

> ⚠️ **Using Pool for connections or anything with a lifecycle.** The GC drops pooled objects without calling `Close`. Use a real pool (channel-based or `database/sql`-style).

> ⚠️ **Assuming Pool helps before profiling.** If allocation isn't in your profile, Pool adds complexity for nothing.

> ⚠️ **`sync.Map` as the default concurrent map.** You lose type safety, and `Range` + mutation semantics get subtle. Start with `map` + `Mutex`.

### IRL Scenario: the leaked tenant data

**Context:** A multi-tenant API pooled `*ResponseBuilder` objects, which had `headers map[string]string` and `body bytes.Buffer`. The `reset()` method cleared `body` but not `headers`. Under a specific error path, tenant B's response included tenant A's `X-Tenant-Token` header.

**Lessons:**
1. Reset **every field** (or build a fresh struct: `*b = ResponseBuilder{body: b.body}`, then `b.body.Reset()`).
2. Pool *only* plain data buffers on security-sensitive paths. Don't pool structs that carry identity.
3. Add a test that runs two "tenants" through a pooled path and asserts no cross-contamination.

### Exercises

1. ⭐ Why does `Get` "pin" the goroutine to a P?
2. ⭐⭐ Benchmark JSON encoding with and without a buffer pool using `-benchmem`, then add GC pressure (allocate garbage in parallel) and compare again.
3. ⭐⭐⭐ Compare `sync.Map`, `map`+`Mutex`, `map`+`RWMutex`, and a 64-shard map for (a) 99% reads on a stable key set, (b) 50/50 read/write on random keys, (c) each goroutine touching only its own keys.

<details>
<summary>Solutions</summary>

1. Pinning disables preemption so the goroutine stays on the same P while touching `local[pid].private` without locks. If it migrated mid-operation, two goroutines could access the same P's private slot concurrently.
2. Expect allocs/op to drop by roughly one large buffer per op. Under GC pressure, the victim cache keeps the hit rate reasonable. Before 1.13, pools emptied every cycle.
3. Typical: (a) sync.Map ≈ sharded > RWMutex > Mutex on many cores; (b) sharded wins, and the 1.24+ sync.Map is competitive; (c) sync.Map and sharded do well, while a single Mutex collapses.

</details>

### Review Checklist

- [ ] Is the Pool justified by an allocation profile?
- [ ] Are pooled objects fully reset? Is identity-bearing state never pooled?
- [ ] Is there a cap on pooled buffer size?
- [ ] Is `sync.Map` used only for its two documented workloads, ideally behind a typed wrapper?

---

## Chapter 15: Atomics and Lock-Free Programming

### TL;DR

- Use the **typed atomics** `[Go 1.19+]`: `atomic.Bool`, `Int32`, `Int64`, `Uint32`, `Uint64`, `Uintptr`, `Pointer[T]`. They guarantee alignment (even on 32-bit platforms) and `go vet` catches copies. (The older `atomic.Value`, from Go 1.4, is not vet-protected against copies.)
- Go atomics are **sequentially consistent**. There is no relaxed or acquire/release API, which keeps reasoning simple. Each atomic also acts as a publication point for the writes sequenced before it (Chapter 4).
- `And`/`Or` methods `[Go 1.23+]` make bit-flag updates atomic without hand-written CAS loops.
- Atomics protect **one word**. Invariants across several fields need a lock or a single owner, or you pack everything into one immutable value behind an `atomic.Pointer`.
- Lock-free does not mean fast. A hot CAS loop on one word has the same coherency problem as a mutex, plus retries.
- In Go, the **GC largely prevents ABA** for pointer-based lock-free structures, *unless you recycle nodes yourself* (e.g., with `sync.Pool`).

### Mental Model: progress guarantees

| Guarantee | Meaning | Go example |
|---|---|---|
| **Blocking** | A stalled thread can stop everyone | `sync.Mutex` |
| **Obstruction-free** | A thread running alone finishes | rare in practice |
| **Lock-free** | *Some* thread always makes progress (system-wide) | CAS-loop stack (Treiber) |
| **Wait-free** | *Every* thread finishes in bounded steps | `atomic.Int64.Add` (on x86 `LOCK XADD`), `atomic.Pointer.Load` |

### Under the Hood

- On amd64, `Load` is a plain `MOV` (x86 TSO already gives acquire semantics, and the compiler won't reorder around it), `Store` is `XCHG` (which carries an implicit full fence for seq-cst), and `Add` is `LOCK XADD`. On arm64, `Load`/`Store` are `LDAR`/`STLR`, and read-modify-write ops (Add, Swap, CAS, And/Or) use LSE instructions (`LDADDAL`, `SWPAL`, `CASAL`, `LDORAL`) when available, otherwise LDAXR/STLXR loops.
- Atomic operations are also **compiler barriers**: the compiler won't hoist or sink memory accesses across them in ways that break happens-before.
- `atomic.Value` stores an `interface{}` internally using a two-phase protocol. All `Store`s must use the **same concrete type**, or it panics, and `Store(nil)` panics. For new code, `atomic.Pointer[T]` is type-safe and simpler.

### Practical Code

#### Copy-on-write configuration (RCU-style)

```go
type Config struct {
	Timeout  time.Duration
	Backends []string
}

type ConfigStore struct{ p atomic.Pointer[Config] }

// NewConfigStore publishes an initial config. The zero ConfigStore would make Update dereference nil.
func NewConfigStore(initial *Config) *ConfigStore {
	s := &ConfigStore{}
	s.p.Store(initial)
	return s
}

func (s *ConfigStore) Load() *Config { return s.p.Load() } // wait-free read

// Update applies fn to a copy and publishes it. Writers are serialized by CAS retry,
// so fn may run more than once and must be free of side effects.
func (s *ConfigStore) Update(fn func(c *Config)) {
	for {
		old := s.p.Load()
		next := *old                                       // shallow copy
		next.Backends = append([]string(nil), old.Backends...) // deep-copy mutable parts!
		fn(&next)
		if s.p.CompareAndSwap(old, &next) {
			return
		}
	}
}
```

> ⚠️ The deep copy is essential. If `fn` appends to a slice shared with the published `old`, readers see mutation, which is a data race. Published values must be **immutable**.

#### State machine with CAS

```go
type State int32

const (
	Idle State = iota
	Running
	Stopped
)

type Service struct{ state atomic.Int32 }

func (s *Service) Start() error {
	if !s.state.CompareAndSwap(int32(Idle), int32(Running)) {
		return fmt.Errorf("cannot start from state %d", s.state.Load())
	}
	go s.run()
	return nil
}

func (s *Service) Stop() bool {
	return s.state.CompareAndSwap(int32(Running), int32(Stopped))
}
```

#### Atomic bit flags `[Go 1.23+]`

```go
const (
	flagDraining uint32 = 1 << iota
	flagReadOnly
)

var flags atomic.Uint32

func setDraining()     { flags.Or(flagDraining) }
func clearReadOnly()   { flags.And(^flagReadOnly) }
func isDraining() bool { return flags.Load()&flagDraining != 0 }
```

#### A lock-free stack (Treiber), and why Go makes it safe-ish

```go
type node[T any] struct {
	val  T
	next *node[T]
}

type Stack[T any] struct{ head atomic.Pointer[node[T]] }

func (s *Stack[T]) Push(v T) {
	n := &node[T]{val: v}
	for {
		old := s.head.Load()
		n.next = old
		if s.head.CompareAndSwap(old, n) {
			return
		}
	}
}

func (s *Stack[T]) Pop() (T, bool) {
	for {
		old := s.head.Load()
		if old == nil {
			var zero T
			return zero, false
		}
		if s.head.CompareAndSwap(old, old.next) {
			return old.val, true
		}
	}
}
```

**ABA problem:** in C, thread 1 reads `head = A`, gets preempted; thread 2 pops A, pops B, frees them, pushes a *new* node that reuses A's address; thread 1's CAS(A → B) succeeds and installs freed B. **In Go,** while thread 1 holds a reference to A, the GC can't free or reuse A, so the address can't come back. ABA returns as soon as you recycle nodes through a `sync.Pool` or a free list. Don't.

> 💡 **Insight:** This stack is lock-free but **not scalable**: every operation CASes the same `head` word. Under heavy contention it can be slower than `Mutex` + slice, because a mutex in starvation mode at least avoids wasted retries. Lock-free structures shine in progress guarantees (no thread can block others by being descheduled while holding a lock) more than in raw throughput.

#### Sharded counter: scalable atomics

```go
type ShardedCounter struct {
	shards [64]struct {
		n atomic.Int64
		_ [56]byte // pad to a cache line
	}
}

func (c *ShardedCounter) Add(delta int64) {
	// Cheap pseudo-random shard choice; any well-distributed source works.
	i := rand.Uint32() % uint32(len(c.shards)) // math/rand/v2 top-level funcs: lock-free runtime generator
	c.shards[i].n.Add(delta)
}

func (c *ShardedCounter) Load() int64 {
	var sum int64
	for i := range c.shards {
		sum += c.shards[i].n.Load()
	}
	return sum // not a point-in-time snapshot; fine for metrics
}
```

### Pitfalls

> ⚠️ **Mixing atomic and plain access to the same variable.** `atomic.AddInt64(&x, 1)` in one place and `x++` or `if x > 0` in another is a data race. Typed atomics make this impossible, which is the main reason to use them.

> ⚠️ **Check-then-act with separate atomics.**
> ```go
> if count.Load() < limit { count.Add(1); admit() } // two goroutines both pass the check
> ```
> Use a CAS loop: `for { c := count.Load(); if c >= limit { reject }; if count.CompareAndSwap(c, c+1) { admit; break } }`.

> ⚠️ **Multi-field invariants with multiple atomics.** `min.Store(x); max.Store(y)` lets readers see `min > max` in between. Put both in one immutable struct behind `atomic.Pointer`.

> ⚠️ **Old-style 64-bit atomics on 32-bit platforms.** `atomic.AddInt64(&s.n, 1)` panics on ARM32/386 if `n` isn't 64-bit aligned. Typed `atomic.Int64` is always aligned.

> 🤖 **AI note:** AI often "optimises" a mutex-protected struct by turning each field into an atomic, which silently breaks cross-field invariants. It also produces the check-then-act pattern above in rate limiters and connection counters.

### IRL Scenario: the connection limiter that let 3× through

**Context:** A proxy limited concurrent upstream connections to 100 with `if active.Load() >= 100 { reject }; active.Add(1)`. During a traffic spike, upstream saw ~300 concurrent connections and fell over.

**Fix options:**
1. CAS loop as shown above.
2. `semaphore.Weighted` with `TryAcquire(1)`.
3. A buffered channel of 100 tokens with non-blocking `select` acquire.

They chose (2) for readability and because it supports ctx-aware waiting with `Acquire` for the queued path.

### Review Drill

```go
type Metrics struct {
	requests atomic.Int64
	errors   atomic.Int64
}

func (m *Metrics) ErrorRate() float64 {
	r := m.requests.Load()
	if r == 0 {
		return 0
	}
	return float64(m.errors.Load()) / float64(r)
}
```

<details>
<summary>Is this a bug?</summary>

It isn't a data race, but the two loads aren't a consistent snapshot: `errors` may be read after more requests and errors have been counted, so in rare cases the rate is slightly off or even > 1 momentarily (e.g., if errors are incremented before requests). For metrics, that's usually acceptable. For **decisions** (circuit breaker thresholds), take a consistent snapshot: one mutex, or one atomic pointer to an immutable window struct, or derive both counts from the same windowed structure. Being able to say "racy-but-acceptable" versus "needs consistency", and why, is what this chapter is meant to teach.

</details>

### Exercises

1. ⭐ Rewrite the check-then-act limiter as a correct CAS loop with `TryAcquire()` and `Release()`.
2. ⭐⭐ Implement `atomic.Pointer`-based copy-on-write `map[string]string` with `Get`, `Set`, `Delete`. Benchmark read throughput against `RWMutex`.
3. ⭐⭐⭐ Implement a bounded single-producer single-consumer (SPSC) ring buffer using only atomics (head/tail indices). Explain why it's correct under the Go memory model and why it breaks with two producers.

<details>
<summary>Solutions</summary>

1. ```go
   type Limiter struct{ n atomic.Int64; max int64 }
   func (l *Limiter) TryAcquire() bool {
       for {
           c := l.n.Load()
           if c >= l.max { return false }
           if l.n.CompareAndSwap(c, c+1) { return true }
       }
   }
   func (l *Limiter) Release() { l.n.Add(-1) }
   ```
2. `Set` and `Delete` copy the whole map (O(n)) and CAS it in. That suits small, read-dominated maps. Reads are one atomic load plus a map lookup and beat RWMutex by a wide margin on many cores.
3. The producer writes `buf[tail%N]`, then `tail.Store(tail+1)`. The consumer `tail.Load()` (which observes the store, giving happens-before, so the slot write is visible), then reads the slot, then `head.Store(head+1)`. The producer checks `head.Load()` for space. It is correct because each index has exactly one writer. With two producers, both read the same `tail`, write the same slot, and one overwrites the other. You'd need a CAS to claim slots plus per-slot sequence numbers (the Vyukov MPMC queue).

</details>

### Review Checklist

- [ ] Are typed atomics used (no mixing of atomic and plain access)?
- [ ] Do any invariants span multiple atomics? (Then the design is wrong.)
- [ ] Is every check-then-act a single CAS?
- [ ] Is "lock-free" justified by a progress requirement or by measurement?
- [ ] Are values published via `atomic.Pointer` immutable afterwards?

---

[← Part III](03-channels-and-select.md) · [Index](README.md) · [Next: Part V, context & Time →](05-context-and-time.md)
