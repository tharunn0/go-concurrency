# Part VIII: Performance and Production

[← Part VII](07-failure-modes-and-debugging.md) · [Index](README.md) · [Next: Part IX, AI-Assisted Concurrency →](09-ai-assisted-concurrency.md)

- [Chapter 28: Contention Engineering](#chapter-28-contention-engineering)
- [Chapter 29: GOMAXPROCS, Containers and Tail Latency](#chapter-29-gomaxprocs-containers-and-tail-latency)
- [Chapter 30: Production Resilience](#chapter-30-production-resilience)

---

## Chapter 28: Contention Engineering

### TL;DR

- **Measure first:** `b.RunParallel`, `-cpu=1,2,4,8,16`, `benchstat`, mutex and block profiles. Contention problems only appear at realistic parallelism.
- **The ladder** (try in order): remove sharing → shrink critical sections → batch → shard/stripe → read-optimise (copy-on-write) → lock-free.
- The goal is not "no locks". The goal is **no hot shared cache lines on the critical path**.

### The contention ladder

| Rung | Technique | Example |
|---|---|---|
| 1 | **Remove sharing**: confine state to a goroutine, aggregate locally | Per-worker stats merged at end (Ch 1) |
| 2 | **Shrink the critical section**: compute outside, lock only to publish | Build the value, then `mu.Lock(); m[k]=v; mu.Unlock()` |
| 3 | **Batch**: amortise one lock acquisition over many ops | Buffer 100 updates locally, apply under one lock |
| 4 | **Shard / stripe**: N independent locks by key hash | 64-shard map |
| 5 | **Read-optimise**: copy-on-write with `atomic.Pointer` | Config, routing tables, feature flags |
| 6 | **Lock-free**: CAS-based structures | Rarely needed; Ch 15 caveats |

### Practical Code

#### Benchmarking contention properly

```go
func BenchmarkCache(b *testing.B) {
	for _, impl := range []struct {
		name string
		c    Cache
	}{
		{"mutex", NewMutexCache()},
		{"rwmutex", NewRWMutexCache()},
		{"sharded", NewShardedCache(64)},
	} {
		b.Run(impl.name, func(b *testing.B) {
			fill(impl.c, 10_000)
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) { // GOMAXPROCS goroutines by default
				r := rand.New(rand.NewPCG(rand.Uint64(), 0)) // per-goroutine RNG
				for pb.Next() {
					k := keys[r.IntN(len(keys))]
					if r.IntN(100) < 5 {
						impl.c.Set(k, "v")
					} else {
						impl.c.Get(k)
					}
				}
			})
		})
	}
}
```

```bash
go test -bench=Cache -cpu=1,4,16,64 -count=10 > new.txt
benchstat old.txt new.txt      # statistically meaningful comparison
```

#### Generic sharded map

```go
type ShardedMap[K comparable, V any] struct {
	seed   maphash.Seed
	shards []shard[K, V]
}

type shard[K comparable, V any] struct {
	mu sync.RWMutex
	m  map[K]V
	_  [96]byte // pad to 128 bytes: neighbouring shards' locks never share a cache line
}

func NewShardedMap[K comparable, V any](n int) *ShardedMap[K, V] {
	s := &ShardedMap[K, V]{seed: maphash.MakeSeed(), shards: make([]shard[K, V], n)}
	for i := range s.shards {
		s.shards[i].m = make(map[K]V)
	}
	return s
}

func (s *ShardedMap[K, V]) shardFor(k K) *shard[K, V] {
	h := maphash.Comparable(s.seed, k) // [Go 1.24+]
	return &s.shards[h%uint64(len(s.shards))]
}

func (s *ShardedMap[K, V]) Get(k K) (V, bool) {
	sh := s.shardFor(k)
	sh.mu.RLock()
	v, ok := sh.m[k]
	sh.mu.RUnlock()
	return v, ok
}

func (s *ShardedMap[K, V]) Set(k K, v V) {
	sh := s.shardFor(k)
	sh.mu.Lock()
	sh.m[k] = v
	sh.mu.Unlock()
}

// Update applies fn atomically for one key.
func (s *ShardedMap[K, V]) Update(k K, fn func(old V, ok bool) V) {
	sh := s.shardFor(k)
	sh.mu.Lock()
	old, ok := sh.m[k]
	sh.m[k] = fn(old, ok)
	sh.mu.Unlock()
}
```

> 🔬 `sync.RWMutex` is 24 bytes and a map header pointer is 8, so 96 bytes of padding makes each shard 128 bytes. Because the slice base isn't cache-line aligned, anything less than a full extra line (e.g. padding to 72 bytes) can still leave two shards' locks on one 64-byte line. 128 also covers CPUs with adjacent-line prefetch.

> ⚠️ Operations across shards (`Len`, `Range`, "transfer between keys") lose atomicity. Either accept eventually-consistent results or lock shards in index order.

#### Batching writes through an aggregator

```go
// Many goroutines report increments; one goroutine applies them in batches.
type Aggregator struct {
	in chan kv
}

type kv struct {
	k string
	n int64
}

func (a *Aggregator) Run(ctx context.Context, flush func(map[string]int64)) error {
	pending := make(map[string]int64)
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			flush(pending)
			return ctx.Err()
		case e := <-a.in:
			pending[e.k] += e.n
			if len(pending) >= 10_000 {
				flush(pending)
				pending = make(map[string]int64)
			}
		case <-t.C:
			if len(pending) > 0 {
				flush(pending)
				pending = make(map[string]int64)
			}
		}
	}
}
```

Caveats: `in` must be created by a constructor (a nil channel blocks forever); items still buffered in `in` when ctx is cancelled are lost unless you drain them non-blockingly before the final flush; and producers must send with a `select` on ctx, or they block forever once `Run` returns.

The channel is still one serialisation point. If *it* becomes hot, shard aggregators by key, or let producers pre-aggregate locally.

### Pitfalls

> ⚠️ **Benchmarking at `-cpu=1`** (or on a laptop with 8 cores when production has 64) hides contention completely.

> ⚠️ **Sharding with a bad hash** (e.g., `len(key) % n`) creates hot shards. Use `maphash`.

> ⚠️ **Sharding the lock but keeping a global counter** (`total++` under a global mutex in every op) brings the bottleneck right back.

### IRL Scenario: rate limiter at 1M req/s

**Context:** A per-API-key limiter (`map[string]*rate.Limiter` behind one mutex) became the top CPU consumer at 1M req/s on 48 cores. The mutex profile showed 70% of contention in `Allow`.

**Fix progression:**
1. Shard the map ×256 (rung 4): 4× throughput.
2. Each `rate.Limiter` has its own internal mutex, and hot keys (one giant customer) still contended: a single key got 300k req/s across all cores.
3. For hot keys, **split the budget**: each of N sub-limiters gets rate/N, and requests pick one at random (rung 4 at the key level). That's slightly less precise and removed the hotspot.

**Lesson:** Sharding by key fails when one key is hot. Then you shard *within* the key, accepting some approximation.

### Review Checklist

- [ ] Are concurrency benchmarks run at production-like `-cpu` values, with benchstat?
- [ ] Which rung of the ladder is this design on, and was a lower rung tried first?
- [ ] Are there hot keys that defeat sharding?
- [ ] Are cross-shard operations' consistency semantics documented?

---

## Chapter 29: GOMAXPROCS, Containers and Tail Latency

### TL;DR

- **CPU limits in containers are enforced by CFS quota over 100 ms periods.** If GOMAXPROCS exceeds the limit, the process can burn its whole quota early in a period and then be **throttled** for the rest, which shows up as periodic latency spikes of tens of ms.
- **Go 1.25+** (main module `go 1.25` or later) sets the default GOMAXPROCS to `min(logical CPUs, ceil(cgroup CPU limit))`, with the limit floored at 2, and **re-checks periodically** if the limit changes. Setting `GOMAXPROCS` explicitly (env var or `runtime.GOMAXPROCS`) disables this. Before 1.25, use `go.uber.org/automaxprocs`.
- CPU **requests** (shares) don't affect GOMAXPROCS. Only **limits** do.
- **Tail latency amplifies with fan-out:** if one backend call has p99 = 10 ms, a request that fans out to 100 backends hits a ≥10 ms call **63%** of the time.

### Mental Model: CFS throttling

```
Limit: 2 CPUs  →  quota 200ms CPU time per 100ms period
GOMAXPROCS=32 (node has 32 cores, old Go default):

 period 1 [0────────100ms]
   32 threads run in parallel → 200ms quota consumed in ~6ms
   ███████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ throttled for ~94ms
   ▲ any request arriving here waits until the next period

GOMAXPROCS=2: work spreads across the period, no throttling, smooth latency.
```

Metrics to check: the cgroup's `cpu.stat` (`nr_throttled`, `throttled_usec`) and the container-level throttling metrics in your platform (e.g., `container_cpu_cfs_throttled_periods_total`).

### Practical Code

#### Know what you're running with

```go
log.Printf("GOMAXPROCS=%d NumCPU=%d", runtime.GOMAXPROCS(0), runtime.NumCPU())
```

```bash
# Kubernetes: verify inside the pod
cat /sys/fs/cgroup/cpu.max        # cgroup v2: "200000 100000" = 2 CPUs
GODEBUG=containermaxprocs=0       # disable container-aware default (1.25+)
GODEBUG=updatemaxprocs=0          # keep the startup value; don't track limit changes
```

#### Fractional limits

With `limit: 1500m` (1.5 CPUs), Go 1.25+ picks `ceil(1.5) = 2`. That means some throttling is still possible at full load. For latency-critical services, use whole-number limits, or decide deliberately between under-using (GOMAXPROCS=1) and occasional throttling.

#### Tail latency math

```
P(request sees ≥1 slow call) = 1 − (1 − p_slow)^N

p_slow = 1% (p99), N = 1   → 1%
                   N = 10  → 9.6%
                   N = 100 → 63%
```

From Dean and Barroso, *The Tail at Scale*: at fan-out, **the tail is the common case**. Mitigations:
- **Hedged requests:** send a backup request after the p95 delay, and take the first response (Chapter 30).
- **Tied requests:** send to two replicas, with each told about the other so it can cancel when one starts.
- **Reduce fan-out:** coarser partitioning, caching.
- **Partial results:** return what you have at the deadline (search results, feeds).

#### Partial results by deadline

```go
func Search(ctx context.Context, shards []Shard, q Query) []Hit {
	ctx, cancel := context.WithTimeout(ctx, 80*time.Millisecond)
	defer cancel()

	results := make(chan []Hit, len(shards)) // buffered: late shards never block
	for _, s := range shards {
		go func() {
			hits, err := s.Search(ctx, q)
			if err != nil {
				hits = nil
			}
			results <- hits
		}()
	}

	var all []Hit
	for range shards {
		select {
		case hits := <-results:
			all = append(all, hits...)
		case <-ctx.Done():
			return all // partial: degrade rather than fail
		}
	}
	return all
}
```

### Pitfalls

> ⚠️ **Hardcoding `GOMAXPROCS`** in env for "performance" disables the 1.25 auto-tuning and goes stale when limits change.

> ⚠️ **Old toolchain go-line.** The container-aware default needs the *main module's* `go` directive to be ≥ 1.25. A service built with Go 1.26 but declaring `go 1.22` keeps the old behaviour.

> ⚠️ **Assuming GOMAXPROCS bounds threads or CPU.** Blocking syscalls and cgo add threads beyond it. GC workers come out of it.

### IRL Scenario: periodic 90 ms spikes after moving to Kubernetes

**Context:** A Go 1.21 service moved from VMs (8 cores) to k8s (limit 2 CPUs, nodes with 64 cores). p50 was fine, but p99 jumped from 15 ms to 110 ms, in a pattern that repeated every 100 ms under load.

**Diagnosis:** GOMAXPROCS=64. `nr_throttled` was climbing. The trace showed all Ps going idle simultaneously for ~90 ms, which is throttling.

**Fix:** Upgrading to Go 1.25+ (with `go 1.25` in go.mod) gave GOMAXPROCS=2. Alternatively, `automaxprocs`. p99 dropped to 14 ms.

### Review Checklist

- [ ] Does the service log its effective GOMAXPROCS at startup?
- [ ] Is CPU throttling monitored?
- [ ] Are CPU limits whole numbers for latency-critical services?
- [ ] Is GOMEMLIMIT set relative to the memory limit?
- [ ] Is fan-out tail amplification addressed (hedging, partial results, reduced fan-out)?

---

## Chapter 30: Production Resilience

### TL;DR

- **Graceful shutdown** is a sequence: fail readiness → wait for load balancer deregistration → stop accepting → drain in-flight (bounded) → stop background workers → flush → close resources in reverse dependency order.
- **Timeouts everywhere**, set as a **budget** that shrinks through the call chain. Never leave `http.Client{}` or `http.Server{}` without timeouts.
- **Retries need backoff, jitter and a budget**, or they amplify outages.
- **Circuit breakers** stop calling a failing dependency. **Bulkheads** (separate pools per dependency) stop one failing dependency from exhausting shared resources.
- **Hedging** cuts tail latency at the cost of a few percent extra load.

### Practical Code

#### Complete graceful shutdown

```go
func main() {
	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	var ready atomic.Bool
	db := mustOpenDB()
	defer db.Close() // deferred first, so it runs last

	// Requests and workers get contexts that WE cancel at the right step, not on SIGTERM.
	reqCtx, hardCancel := context.WithCancel(context.Background())
	defer hardCancel()
	workerCtx, stopWorkers := context.WithCancel(context.Background())
	defer stopWorkers()

	// Track handlers: srv.Close does not wait for them, and db must outlive them.
	var handlers sync.WaitGroup
	track := func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handlers.Add(1)
			defer handlers.Done()
			h.ServeHTTP(w, r)
		})
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !ready.Load() {
			http.Error(w, "draining", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.Handle("/", track(appHandler(db)))

	srv := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second, // Slowloris protection
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return reqCtx },
	}

	srvErr := make(chan error, 1)
	go func() { srvErr <- srv.ListenAndServe() }()
	workers := NewWorkerGroup(db) // background consumers; handlers may depend on them
	wrkErr := make(chan error, 1)
	go func() { wrkErr <- workers.Run(workerCtx) }()

	ready.Store(true)
	workersExited := false
	select {
	case <-sigCtx.Done():
	case err := <-srvErr:
		log.Printf("server failed: %v", err)
		srvErr <- err // put it back for the wait below (buffer is now empty)
	case err := <-wrkErr:
		log.Printf("workers failed: %v", err)
		workersExited = true
	}
	stop() // restore default signal handling: a second Ctrl-C/SIGTERM kills immediately
	log.Println("shutdown: begin")

	// 1. Fail readiness (matters for non-k8s LBs; k8s also removes terminating pods
	//    from EndpointSlices on its own).
	ready.Store(false)
	// 2. Wait for LB / endpoint / kube-proxy propagation. This sleep is what prevents 502s.
	time.Sleep(5 * time.Second)

	// 3. Stop accepting and drain in-flight requests, within budget.
	drainCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := srv.Shutdown(drainCtx); err != nil {
		log.Printf("drain incomplete: %v; hard-cancelling requests", err)
		hardCancel() // stragglers see ctx cancellation
		srv.Close()
	}
	<-srvErr // ListenAndServe has returned
	if !WaitTimeout(&handlers, 5*time.Second) { // Chapter 13; bounded
		log.Println("handlers still running after hard cancel; exiting anyway")
	}

	// 4. Only now stop background workers: handlers no longer need them.
	stopWorkers()
	if !workersExited {
		if err := <-wrkErr; err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("workers: %v", err)
		}
	}
	log.Println("shutdown: complete")
	// 5. Deferred: db.Close() and friends, in reverse order.
}
```

Budget check: 5 s propagation + 20 s drain + up to 5 s for stragglers + worker wind-down must fit within k8s `terminationGracePeriodSeconds` (default 30 s, which is too short here). Set it explicitly, e.g. 40 s.

Why this order: workers get their own context and stop only after HTTP has drained, because in-flight handlers may still enqueue to them. Handlers are tracked with a WaitGroup because neither `Shutdown` (on timeout) nor `Close` waits for running handlers, and the deferred `db.Close()` must not run underneath them.

> 💡 **Insight:** `srv.Shutdown` **does not** cancel request contexts. It closes listeners and idle connections, then waits for active ones. That's why `BaseContext` should return a ctx *you* control (`reqCtx` here) to hard-cancel stragglers.

#### Hedged requests

```go
// Hedged calls fn; if no response within `delay`, launches another attempt (up to max).
// Returns the first success. Losers are cancelled via ctx.
func Hedged[T any](ctx context.Context, delay time.Duration, max int,
	fn func(context.Context) (T, error)) (T, error) {

	if max < 1 {
		max = 1 // max ≤ 0 would make results unbuffered and leak the first attempt
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // cancels outstanding attempts when we return

	type result struct {
		v   T
		err error
	}
	results := make(chan result, max) // buffered: attempts never block after we return
	launched, inflight := 0, 0
	launch := func() {
		launched++
		inflight++
		go func() {
			v, err := fn(ctx)
			results <- result{v, err}
		}()
	}

	launch()
	t := time.NewTimer(delay)
	defer t.Stop()

	var zero T
	var lastErr error
	for {
		select {
		case r := <-results:
			inflight--
			if r.err == nil {
				return r.v, nil
			}
			lastErr = r.err
			if launched < max {
				launch() // failed fast: hedge immediately
				t.Reset(delay)
			} else if inflight == 0 {
				return zero, lastErr
			}
		case <-t.C:
			if launched < max {
				launch()
				t.Reset(delay)
			}
		case <-ctx.Done():
			return zero, ctx.Err()
		}
	}
}
```

Set `delay` to about the dependency's p95. Only hedge **idempotent** requests, and cap hedging at a small percentage of traffic, or a hedge storm will amplify an overload.

#### Retry budget

```go
// RetryBudget allows retries up to ratio of recent successful requests (e.g. 0.1 = +10% load).
type RetryBudget struct {
	mu     sync.Mutex
	tokens float64
	ratio  float64
	max    float64
}

func (b *RetryBudget) OnSuccess() {
	b.mu.Lock()
	b.tokens = min(b.max, b.tokens+b.ratio)
	b.mu.Unlock()
}

func (b *RetryBudget) TryRetry() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}
```

Initialise it with `max` > 0 (e.g. 10) and some starting tokens; the zero value never permits a retry. During a total outage, successes stop, tokens run out, and retries stop. Load on the failing dependency stays at about 1× instead of 3× (with 3 attempts).

#### Circuit breaker

```go
import (
	"errors"
	"sync"
	"time"
)

type BreakerState int32

const (
	Closed BreakerState = iota
	Open
	HalfOpen
)

// Outcome is how a call's result should count towards the breaker.
type Outcome int

const (
	Success Outcome = iota
	Failure
	Ignore // client errors (4xx), caller cancellation: neither success nor failure
)

var ErrOpen = errors.New("circuit open")

type Breaker struct {
	mu          sync.Mutex
	state       BreakerState
	gen         uint64 // bumped on every state change; stale results are ignored
	failures    int
	threshold   int
	openedAt    time.Time
	coolDown    time.Duration
	probeActive bool
}

func (b *Breaker) setState(s BreakerState) {
	b.state = s
	b.gen++
	b.failures = 0
	b.probeActive = false
	if s == Open {
		b.openedAt = time.Now()
	}
}

// Allow reports whether a call may proceed. On success it returns a ticket that
// must be passed to Done with the call's outcome.
func (b *Breaker) Allow() (ticket uint64, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case Open:
		if time.Since(b.openedAt) < b.coolDown {
			return 0, ErrOpen
		}
		b.setState(HalfOpen)
		fallthrough
	case HalfOpen:
		if b.probeActive {
			return 0, ErrOpen // only one probe at a time
		}
		b.probeActive = true
	}
	return b.gen, nil
}

func (b *Breaker) Done(ticket uint64, o Outcome) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if ticket != b.gen {
		return // admitted under an earlier state (e.g. before the breaker opened): stale
	}
	switch b.state {
	case HalfOpen:
		switch o {
		case Success:
			b.setState(Closed)
		case Failure:
			b.setState(Open)
		case Ignore:
			b.probeActive = false // inconclusive: let the next caller probe
		}
	case Closed:
		switch o {
		case Success:
			b.failures = 0
		case Failure:
			b.failures++
			if b.failures >= b.threshold {
				b.setState(Open)
			}
		}
	}
}

// Usage
func (c *Client) Get(ctx context.Context, k string) (V, error) {
	ticket, err := c.breaker.Allow()
	if err != nil {
		return V{}, err // fail fast; maybe serve from cache
	}
	v, err := c.get(ctx, k)
	c.breaker.Done(ticket, classify(err)) // Success / Failure / Ignore (4xx, ctx cancellation)
	return v, err
}
```

Two details matter. First, the **ticket**: a slow call admitted before the breaker opened can finish while a half-open probe is in flight, and without the generation check its success would wrongly close the breaker. Second, the **three-way outcome**: if "don't count" errors were reported as success, a cancelled probe would close the breaker.

A mutex is right here. The critical section is tiny, the state spans several fields (state, gen, failures, openedAt, probeActive), and atomics would break those invariants (Chapter 15). Production breakers usually use **sliding windows** (failure *rate* over the last N seconds), not consecutive-failure counts.

#### Bulkheads

```go
// Separate concurrency budgets per dependency: a slow payments API can't
// exhaust capacity needed for the inventory API.
type Bulkheads struct {
	payments  *semaphore.Weighted
	inventory *semaphore.Weighted
}

func (b *Bulkheads) CallPayments(ctx context.Context, f func(context.Context) error) error {
	waitCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond) // max queue wait
	err := b.payments.Acquire(waitCtx, 1)
	cancel()
	if err != nil {
		return fmt.Errorf("payments bulkhead full: %w", err)
	}
	defer b.payments.Release(1)
	return f(ctx) // the caller's ctx, not waitCtx (which is already cancelled)
}
```

> ⚠️ A classic bug: writing `ctx, cancel := context.WithTimeout(ctx, ...)` for the queue wait, then passing that shadowed, already-cancelled `ctx` to `f`. Give the wait context its own name.

#### Timeouts checklist for `net/http`

```go
client := &http.Client{
	Timeout: 10 * time.Second, // whole exchange including body read
	Transport: &http.Transport{
		DialContext:           (&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSHandshakeTimeout:   3 * time.Second,
		ResponseHeaderTimeout: 5 * time.Second,
		IdleConnTimeout:       90 * time.Second,
		MaxIdleConnsPerHost:   64, // default 2 is far too low for service-to-service traffic
		MaxConnsPerHost:       256, // a bulkhead at the transport level
	},
}
```

### Pitfalls

> ⚠️ **No sleep between failing readiness and `Shutdown`.** The LB keeps sending traffic for a few seconds, and those connections get refused, giving you 502s on every deploy.

> ⚠️ **Shutdown timeout longer than the orchestrator's grace period.** You get SIGKILL mid-drain.

> ⚠️ **Retrying non-idempotent operations** (payments!) without idempotency keys.

> ⚠️ **Counting cancellations and 4xx as breaker failures.** Clients going away then trips your breaker.

> 🤖 **AI note:** Generated shutdown code usually calls `srv.Shutdown` with a timeout and stops there: no readiness flip, no propagation delay, no handling of background workers, no hard-cancel path. Generated retry loops usually have no jitter and no budget.

### IRL Scenario: every deploy caused 0.5% errors

**Context:** Rolling deploys produced bursts of 502s from the ingress.

**Diagnosis:**
1. The app called `srv.Shutdown` immediately on SIGTERM. kube-proxy and the ingress still routed to the pod for ~2–5 s, and those connections were refused.
2. Long-polling requests (60 s) exceeded the 30 s grace period and got SIGKILLed.

**Fix:** The full sequence above: readiness flip, a 5 s sleep, a 20 s drain, a hard-cancel ctx for long-polls (which tell clients to reconnect elsewhere), and an explicit `terminationGracePeriodSeconds: 40`. Errors during deploys dropped to zero.

### Exercises

1. ⭐ Why does `Hedged` buffer `results` with capacity `max`?
2. ⭐⭐ Convert the circuit breaker to a sliding-window failure-rate breaker (e.g., open when ≥50% of the last 20 calls in the last 10 s failed, with a minimum of 10 calls).
3. ⭐⭐⭐ Write a synctest test for the shutdown sequence: start a server with a fake slow handler, trigger shutdown, and assert that in-flight requests complete, new requests are refused after readiness fails, and stragglers are hard-cancelled at the deadline. (Hint: use in-memory listeners.)

<details>
<summary>Solutions</summary>

1. After `Hedged` returns, nobody reads `results`. The losing attempts must still be able to send their results and exit. With capacity `max` and at most `max` launches, every send succeeds without blocking, so there's no leak.
2. Keep a ring buffer of `(time, ok)` outcomes under the mutex. On `Done`, append and prune entries older than 10 s. If the count is ≥ 10 and the failure ratio is ≥ 0.5, open. The half-open logic is unchanged.
3. A `net.Pipe`-based listener (implement `net.Listener` over a channel of `net.Conn`), handlers that block on bubble channels or `time.Sleep`, and assertions at `synctest.Wait()` quiescent points. The fake clock makes the 5 s/20 s windows instant and exact.

</details>

### Review Checklist

- [ ] Is the shutdown sequence complete, with timings that fit the orchestrator's grace period?
- [ ] Does every client and server have explicit timeouts?
- [ ] Are retries jittered, budgeted, and limited to idempotent operations?
- [ ] Are there circuit breakers and bulkheads around each critical dependency?
- [ ] Is hedging used for tail-sensitive idempotent reads, with a cap?

---

[← Part VII](07-failure-modes-and-debugging.md) · [Index](README.md) · [Next: Part IX, AI-Assisted Concurrency →](09-ai-assisted-concurrency.md)
