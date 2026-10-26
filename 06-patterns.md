# Part VI: Patterns and Composition

[← Part V](05-context-and-time.md) · [Index](README.md) · [Next: Part VII, Failure Modes & Debugging →](07-failure-modes-and-debugging.md)

- [Chapter 18: Pipelines, Fan-out and Fan-in](#chapter-18-pipelines-fan-out-and-fan-in)
- [Chapter 19: Bounded Parallelism and Worker Pools](#chapter-19-bounded-parallelism-and-worker-pools)
- [Chapter 20: Backpressure, Load Shedding and Rate Limiting](#chapter-20-backpressure-load-shedding-and-rate-limiting)
- [Chapter 21: singleflight and Request Coalescing](#chapter-21-singleflight-and-request-coalescing)
- [Chapter 22: Ownership, Actors and Pub/Sub](#chapter-22-ownership-actors-and-pubsub)
- [Chapter 23: Structured Concurrency](#chapter-23-structured-concurrency)
- [Chapter 24: Generics, Iterators and Concurrency](#chapter-24-generics-iterators-and-concurrency)

> Packages used: `golang.org/x/sync/errgroup`, `golang.org/x/sync/semaphore`, `golang.org/x/sync/singleflight`, `golang.org/x/time/rate`.

---

## Chapter 18: Pipelines, Fan-out and Fan-in

### TL;DR

- A **stage** is `func(ctx, in <-chan T) <-chan U`. It owns and **closes its output** and stops when `in` closes **or** ctx is done.
- **Every send in a stage must also select on `ctx.Done()`.** Otherwise one stalled consumer leaks the whole upstream.
- **Fan-out** loses ordering. If you need ordering, use **futures** (a channel of result channels) or index-tagged results with a reorder buffer.
- For most real systems, use **errgroup** to run stages so the first error cancels everything and `Wait` gives you the error.
- Don't build channel pipelines for in-memory slices you could process with a parallel loop. Pipelines earn their keep with **streams** (unbounded or large inputs, I/O at each stage).

### Mental Model

```
 source ──► [parse] ──► [enrich ×8] ──► [batch] ──► sink
   │         1 goroutine   fan-out         fan-in      │
   └──────────────── ctx cancels all ──────────────────┘
 Each arrow = channel owned (and closed) by its sender stage.
 Backpressure flows right-to-left automatically: a slow sink blocks batch, blocks enrich...
```

### Practical Code

#### Generic stage and fan-out/in

```go
// Stage applies fn to each input. Closes out when in closes or ctx is done.
func Stage[T, U any](ctx context.Context, in <-chan T, fn func(context.Context, T) (U, error)) (<-chan U, <-chan error) {
	out := make(chan U)
	errc := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errc)
		for v := range in {
			u, err := fn(ctx, v)
			if err != nil {
				errc <- err // buffered(1): never blocks
				return
			}
			select {
			case out <- u:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, errc
}
```

The `for v := range in` loop above can't be interrupted while it waits for input. That's fine *if* the upstream stage closes on cancellation (they all do in this design). If you can't guarantee that, use the `select`-receive form from Chapter 10's `OrDone`.

#### Production version: pipeline under errgroup

```go
func RunETL(ctx context.Context, src Source, dst Sink) error {
	g, ctx := errgroup.WithContext(ctx)

	raw := make(chan []byte, 64)
	recs := make(chan Record, 64)

	// Stage 1: read
	g.Go(func() error {
		defer close(raw)
		return src.Each(ctx, func(line []byte) error {
			select {
			case raw <- line:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	})

	// Stage 2: parse, fan-out ×N, with one closer
	const parsers = 8
	var parseWG sync.WaitGroup
	for range parsers {
		parseWG.Add(1)
		g.Go(func() error {
			defer parseWG.Done()
			for line := range raw {
				r, err := Parse(line)
				if err != nil {
					return fmt.Errorf("parse: %w", err)
				}
				select {
				case recs <- r:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		})
	}
	g.Go(func() error {
		parseWG.Wait()
		close(recs) // single closer after all parsers exit
		return nil
	})

	// Stage 3: batch + write (fan-in point)
	g.Go(func() error {
		batch := make([]Record, 0, 500)
		flush := func() error {
			if len(batch) == 0 {
				return nil
			}
			err := dst.Write(ctx, batch)
			batch = batch[:0]
			return err
		}
		for r := range recs {
			batch = append(batch, r)
			if len(batch) == cap(batch) {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		return flush()
	})

	return g.Wait()
}
```

Why this works:
- Any stage error cancels ctx. Every send selects on ctx, so all stages unwind. Each stage closes its output on exit, so downstream `range` loops end.
- `g.Wait()` returns only when **every** goroutine has exited: no leaks, structured lifetime (Chapter 23).
- The parse fan-out has exactly one closer.

> ⚠️ **Subtle point:** when a parse goroutine errors, the *others* still drain `raw` until stage 1 notices cancellation and closes it. Their sends then select on ctx and they exit. Trace through this unwind in your head for every pipeline you write.

#### Ordered fan-out with futures

```go
type Result[U any] struct {
	Val U
	Err error
}

// OrderedMap processes inputs with up to `workers` in flight, emitting results in input order.
func OrderedMap[T, U any](ctx context.Context, in <-chan T, workers int,
	fn func(context.Context, T) (U, error)) <-chan Result[U] {

	futures := make(chan chan Result[U], workers) // bounds in-flight work to ~workers+1 (the collector holds one)
	out := make(chan Result[U])

	go func() { // dispatcher
		defer close(futures)
		for v := range in {
			f := make(chan Result[U], 1) // buffered: worker never blocks
			select {
			case futures <- f:
			case <-ctx.Done():
				return
			}
			go func() {
				u, err := fn(ctx, v)
				f <- Result[U]{u, err}
			}()
		}
	}()

	go func() { // collector: preserves order by awaiting futures in sequence
		defer close(out)
		for f := range futures {
			var r Result[U]
			select {
			case r = <-f:
			case <-ctx.Done():
				return
			}
			select {
			case out <- r:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}
```

**Trade-off:** head-of-line blocking. One slow item delays all results behind it, but in-flight work continues up to the bound. If ordering is not a hard requirement, don't pay for it.

> 🔬 **Leak check:** when ctx is cancelled, the collector returns while workers may still be running. They write to their buffered `f` and exit, so nothing leaks. The dispatcher exits on ctx or when `in` closes. If `in` is never closed *and* never cancelled, the dispatcher waits forever, which is the caller's contract.

### Pitfalls

> ⚠️ **Stage sends without `ctx.Done()`.** This is the #1 pipeline leak.

> ⚠️ **Errors on a separate channel nobody reads.** If `errc` is unbuffered and the caller only reads `out`, the stage blocks forever on error. Use errgroup.

> ⚠️ **Over-staging.** Each stage hop costs ~100+ ns and a goroutine switch. Fusing cheap stages (parse + validate) into one function is often faster and clearer.

> 🤖 **AI note:** AI-generated pipelines commonly (1) omit ctx in sends, (2) close the output channel from each fan-out worker, (3) pass errors via `log.Println` inside stages instead of propagating them.

### IRL Scenario: the ETL job that never finished

**Context:** A nightly ETL occasionally hung forever. There was no error, CPU was at 0%, and the job had to be killed manually.

**Diagnosis:** A goroutine dump (`SIGQUIT`) showed: the writer stage had returned an error (DB unavailable) and exited. The 8 parsers were all blocked in `[chan send]` on `recs`. The reader was blocked in `[chan send]` on `raw`. `main` was waiting on a WaitGroup. The writer's error was logged and not propagated, and no send had a cancellation case.

**Fix:** The errgroup design above. A writer error now cancels ctx, everyone unwinds, and the job fails fast with the real error, so the orchestrator retries it.

### Exercises

1. ⭐ In `RunETL`, what happens if `dst.Write` panics?
2. ⭐⭐ Add a progress reporter goroutine (logs records/s every 10 s) to `RunETL` that exits cleanly when the pipeline finishes.
3. ⭐⭐⭐ Modify `OrderedMap` to stop dispatching new work after the first error, while still emitting all results *before* the error in order.

<details>
<summary>Solutions</summary>

1. The panic crashes the process. errgroup doesn't turn panics into errors. If you want that, wrap stage bodies with a recover that returns an error, and decide whether partially-written state is acceptable.
2. Add `g.Go(func() error { t := time.NewTicker(10*time.Second); defer t.Stop(); for { select { case <-t.C: log(count.Load()); case <-done: return nil; case <-ctx.Done(): return nil } } })`, where `done` is closed by the writer stage when it finishes successfully. Without `done`, `g.Wait()` would hang forever on the reporter.
3. Derive `ctx, cancel := context.WithCancel(ctx)`. In the collector, upon receiving a result with `Err != nil`, emit it, then `cancel()` and return. The dispatcher stops on ctx, in-flight workers write to buffered futures and exit, and the results before the error have already been emitted in order.

</details>

### Review Checklist

- [ ] Does every stage close its output and select on ctx in every send?
- [ ] Is there exactly one closer per channel?
- [ ] Do errors propagate (errgroup) instead of being logged and swallowed?
- [ ] Is ordering required? If not, is the code paying for it anyway?
- [ ] Could fewer, fused stages do the same job?

---

## Chapter 19: Bounded Parallelism and Worker Pools

### TL;DR

- **Always bound concurrency.** Unbounded fan-out overloads memory, file descriptors, and whatever is downstream.
- **errgroup + `SetLimit(n)`** is the default tool: bounded, cancels on first error, waits for all.
- **`semaphore.Weighted`** for weighted or ctx-aware acquisition (e.g., limit by bytes in flight, not by count).
- **Fixed worker pool** (N goroutines reading a jobs channel) when workers hold expensive per-worker state (connections, buffers, parsers) or for long-running streaming services.
- Choose N with **Little's Law** and the downstream capacity. For CPU-bound work N ≈ GOMAXPROCS. For I/O-bound work, measure.

### Mental Model

```
goroutine-per-item + semaphore        fixed worker pool
  for each item:                        N workers ← jobs chan ← producer
    acquire → go work → release         each worker: for job := range jobs
  + simple, no idle goroutines          + per-worker state, stable goroutine count
  - goroutine churn per item            - must manage jobs channel lifecycle
```

### Practical Code

#### errgroup with a limit (default choice)

```go
func FetchAll(ctx context.Context, urls []string) ([]Page, error) {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(16)

	pages := make([]Page, len(urls)) // each goroutine writes its own index: no lock needed
	for i, u := range urls {
		g.Go(func() error { // blocks here when 16 are in flight
			p, err := fetch(ctx, u)
			if err != nil {
				return fmt.Errorf("fetch %s: %w", u, err)
			}
			pages[i] = p
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return pages, nil
}
```

> ⚠️ `g.Go` blocks while the limit is reached and **does not watch ctx**. After an error, the loop keeps calling `g.Go` for the remaining items, and each new goroutine must notice the cancelled ctx quickly (here `fetch(ctx, ...)` returns immediately). For huge inputs, check `ctx.Err()` in the loop and break.

#### Collect all errors instead of failing fast

```go
func ValidateAll(ctx context.Context, docs []Doc) error {
	var (
		mu   sync.Mutex
		errs []error
	)
	g := new(errgroup.Group) // no WithContext: don't cancel siblings
	g.SetLimit(runtime.GOMAXPROCS(0))
	for _, d := range docs {
		g.Go(func() error {
			if err := validate(ctx, d); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("doc %s: %w", d.ID, err))
				mu.Unlock()
			}
			return nil
		})
	}
	g.Wait()
	return errors.Join(errs...)
}
```

#### Weighted semaphore: bound by bytes, not count

```go
var memBudget = semaphore.NewWeighted(512 << 20) // 512 MiB of in-flight uploads

func handleUpload(ctx context.Context, size int64, r io.Reader) error {
	if size > 512<<20 {
		return errTooLarge // would never acquire: fail fast
	}
	if err := memBudget.Acquire(ctx, size); err != nil {
		return err // ctx cancelled while waiting
	}
	defer memBudget.Release(size)
	return processInMemory(ctx, r, size)
}
```

`semaphore.Weighted` is FIFO: a large request at the head blocks smaller ones behind it, which avoids starving large requests.

#### Fixed worker pool with per-worker state

```go
type Pool struct {
	jobs chan Job
	wg   sync.WaitGroup
}

func NewPool(ctx context.Context, n int, newConn func() (*Conn, error)) (*Pool, error) {
	p := &Pool{jobs: make(chan Job)}
	for i := 0; i < n; i++ {
		conn, err := newConn() // expensive per-worker state
		if err != nil {
			close(p.jobs)
			p.wg.Wait()
			return nil, err
		}
		p.wg.Go(func() {
			defer conn.Close()
			for job := range p.jobs {
				job.Result <- conn.Do(ctx, job.Req) // Result is buffered(1)
			}
		})
	}
	return p, nil
}

// Submit blocks until a worker is free (backpressure) or ctx is done.
func (p *Pool) Submit(ctx context.Context, j Job) error {
	select {
	case p.jobs <- j:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close stops accepting jobs and waits for workers to finish in-flight ones.
// Callers must not Submit after Close (a send on a closed channel panics).
func (p *Pool) Close() {
	close(p.jobs)
	p.wg.Wait()
}
```

#### Choosing N

| Work type | Starting point | Then |
|---|---|---|
| CPU-bound | `runtime.GOMAXPROCS(0)` | Benchmark N/2..2N. Hyperthreads rarely help pure compute. |
| I/O to one downstream | Little's Law: target RPS × p50 latency, capped by downstream's limit | Load test, watch downstream p99 |
| Mixed | Separate pools for the CPU and I/O phases | |
| Unknown or variable downstream | **Adaptive concurrency** (AIMD, or gradient-based like Netflix's concurrency-limits) | Shrink on latency increase or errors, grow slowly |

#### Adaptive concurrency limit (AIMD sketch)

```go
type AIMD struct {
	mu       sync.Mutex
	limit    float64
	inflight int
	min, max float64
}

func (a *AIMD) TryAcquire() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if float64(a.inflight) >= a.limit {
		return false
	}
	a.inflight++
	return true
}

// Release reports the outcome: overloaded = timeout / 429 / 503 / latency above target.
func (a *AIMD) Release(overloaded bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.inflight--
	if overloaded {
		a.limit = max(a.min, a.limit*0.9) // multiplicative decrease
	} else {
		a.limit = min(a.max, a.limit+1/a.limit) // additive increase (~+1 per window)
	}
}
```

### Pitfalls

> ⚠️ **Writing to a shared slice with `append` from goroutines** is a race. Preallocate and write by index, or collect results through a channel or mutex.

> ⚠️ **Pool `Close` racing with `Submit`** panics (send on closed channel). Either guarantee ordering (all submitters done before Close), or guard with a `closing` channel that `Submit` selects on, and never close `jobs` (workers exit on `closing`).

> ⚠️ **Nested pools deadlocking.** A task in pool A submits subtasks to pool A and waits for them. With all workers busy waiting, nobody runs the subtasks. Use separate pools per level, or don't block a worker on its own pool.

> 🤖 **AI note:** AI frequently picks `runtime.NumCPU()` workers for I/O-bound work (far too few), or launches one goroutine per item with no bound at all ("goroutines are cheap").

### IRL Scenario: the migration that got the team rate-limited

**Context:** A one-off script migrated 2M user records through a partner API (limit: 100 req/s, 20 concurrent). It used `errgroup.SetLimit(200)`. Within seconds the partner returned 429s, the script retried immediately, and the account got temporarily banned.

**Fix:**
- Concurrency limit 20, matching the partner's documented limit.
- A `rate.Limiter(100, 10)` with `lim.Wait(ctx)` before each call.
- Honour `Retry-After` on 429, with exponential backoff plus jitter.
- Checkpoint progress, so a restart resumes rather than redoes the work.

**Lesson:** The bound comes from the *downstream's* capacity, not from what your machine can do.

### Exercises

1. ⭐ `FetchAll` returns on the first error. How many fetches might still be running when `g.Wait()` returns? (Trick question.)
2. ⭐⭐ Build a worker pool whose `Close` is safe to call concurrently with `Submit` (Submit returns `ErrClosed` instead of panicking).
3. ⭐⭐⭐ Implement a gradient-based adaptive limiter: track min RTT, compute `gradient = minRTT / currentRTT`, set `newLimit = limit × gradient + queueAllowance`. Simulate a downstream whose latency rises with concurrency and show the limit converge.

<details>
<summary>Solutions</summary>

1. Zero. `Wait` returns only after **all** goroutines started with `g.Go` have returned. That is the structured-concurrency guarantee. They may have been *cancelled*, but none are running.
2. ```go
   type Pool struct {
       jobs    chan Job
       closing chan struct{}
       once    sync.Once
       wg      sync.WaitGroup
   }
   func (p *Pool) Submit(ctx context.Context, j Job) error {
       select {
       case <-p.closing: return ErrClosed // fast-path check
       default:
       }
       select {
       case p.jobs <- j:  return nil
       case <-p.closing:  return ErrClosed
       case <-ctx.Done(): return ctx.Err()
       }
   }
   func (p *Pool) Close() { p.once.Do(func() { close(p.closing) }); p.wg.Wait() }
   // workers: for { select { case j := <-p.jobs: run(j); case <-p.closing: return } }
   ```
   `jobs` is never closed, so a send can never panic. Jobs racing with Close may or may not run. Document that, or drain `jobs` after `closing` if you need to.
3. Expect the limit to oscillate around the "knee" of the latency curve, near the downstream's real capacity (it rediscovers Little's Law empirically).

</details>

### Review Checklist

- [ ] Is every fan-out bounded, and is the bound derived from the downstream or from measurement?
- [ ] Fail-fast or collect-all: was that chosen deliberately?
- [ ] Can a pool's shutdown race with submission?
- [ ] Is there any risk of a task blocking on its own pool?
- [ ] Is rate limiting separate from concurrency limiting where the downstream needs both?

---

## Chapter 20: Backpressure, Load Shedding and Rate Limiting

### TL;DR

- When input rate > processing rate, you have four options: **block** (backpressure), **drop** (shed), **buffer** (delay the problem), **degrade** (do less per item). Choose one explicitly. Unbounded buffering is choosing to crash later.
- **Backpressure** propagates naturally through blocking channel sends and bounded semaphores. Make sure it reaches the edge (the client gets a 429/503) rather than piling up in memory.
- **Shed early and cheaply.** Reject at admission, before doing any work, with a fast error.
- **Rate limit** (requests per unit time; token bucket) and **concurrency limit** (in-flight requests) are different tools. Most services need concurrency limits more than rate limits.
- Under overload, **queue time is wasted time**: a request that waited 10 s in a queue is often already abandoned by the client. Bound queue *time*, not just queue length.

### Mental Model

```
             ┌─ admission control (concurrency limit + queue timeout) ─┐
client ───►  │  queue (bounded, timed)  ──►  workers (N)               │ ──► downstream
             └──────────── reject fast: 429/503 + Retry-After ─────────┘
             backpressure: downstream slow → workers busy → queue full → reject at edge
```

### Practical Code

#### Admission control middleware: concurrency limit with queue timeout

```go
type Admission struct {
	sem          chan struct{}
	queueTimeout time.Duration
	rejected     atomic.Int64
}

func NewAdmission(maxInFlight int, queueTimeout time.Duration) *Admission {
	return &Admission{sem: make(chan struct{}, maxInFlight), queueTimeout: queueTimeout}
}

func (a *Admission) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Fast path: slot available
		select {
		case a.sem <- struct{}{}:
			defer func() { <-a.sem }()
			next.ServeHTTP(w, r)
			return
		default:
		}
		// Slow path: wait a bounded time
		t := time.NewTimer(a.queueTimeout)
		defer t.Stop()
		select {
		case a.sem <- struct{}{}:
			defer func() { <-a.sem }()
			next.ServeHTTP(w, r)
		case <-t.C:
			a.rejected.Add(1)
			w.Header().Set("Retry-After", "1")
			http.Error(w, "server overloaded", http.StatusServiceUnavailable)
		case <-r.Context().Done():
			// client gave up while queued; nothing to write
		}
	})
}
```

#### Token bucket with `x/time/rate`

```go
lim := rate.NewLimiter(rate.Limit(100), 20) // 100 tokens/s, burst 20

// Non-blocking: reject when over the rate (API gateway style)
if !lim.Allow() {
	http.Error(w, "rate limited", http.StatusTooManyRequests)
	return
}

// Blocking: pace an outbound client (respects ctx)
if err := lim.Wait(ctx); err != nil {
	return err
}

// Reserve: know how long you'd wait, decide whether it's worth it
res := lim.Reserve()
if d := res.Delay(); d > 500*time.Millisecond {
	res.Cancel() // give the token back
	return errTooBusy
} else {
	time.Sleep(d) // accepted: you must still wait out the delay (or select on a timer and ctx)
}
```

#### Per-key limiters with eviction

```go
type KeyedLimiter struct {
	mu    sync.Mutex
	m     map[string]*keyed
	r     rate.Limit
	burst int
}

type keyed struct {
	lim      *rate.Limiter
	lastSeen time.Time
}

func NewKeyedLimiter(ctx context.Context, r rate.Limit, burst int, idle time.Duration) *KeyedLimiter {
	k := &KeyedLimiter{m: make(map[string]*keyed), r: r, burst: burst}
	go k.janitor(ctx, idle) // stopped by ctx; make sure the caller owns that lifecycle
	return k
}

func (k *KeyedLimiter) Allow(key string) bool {
	k.mu.Lock()
	e, ok := k.m[key]
	if !ok {
		e = &keyed{lim: rate.NewLimiter(k.r, k.burst)}
		k.m[key] = e
	}
	e.lastSeen = time.Now()
	k.mu.Unlock()
	return e.lim.Allow() // rate.Limiter is itself concurrency-safe
}

func (k *KeyedLimiter) janitor(ctx context.Context, idle time.Duration) {
	t := time.NewTicker(idle / 2)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			k.mu.Lock()
			for key, e := range k.m {
				if now.Sub(e.lastSeen) > idle {
					delete(k.m, key)
				}
			}
			k.mu.Unlock()
		}
	}
}
```

> ⚠️ The janitor holds the lock while scanning the whole map. At millions of keys, shard the map (Chapter 28) or evict incrementally.

#### Overload behaviour: LIFO and CoDel ideas

- **Adaptive LIFO** (popularised by Facebook): under overload, serve the *newest* queued requests first. Old ones are likely already timed out client-side, while new ones can still succeed. Switch back to FIFO when the queue drains.
- **CoDel** (controlled delay): if the *minimum* queueing delay over an interval exceeds a target (e.g., 5 ms over 100 ms), start dropping. It targets standing queues while tolerating bursts.

Both are implementable with a mutex-guarded deque in place of a channel. Channels are strictly FIFO.

### Pitfalls

> ⚠️ **Shedding after doing the expensive work** (parsing the body, auth, DB lookup) wastes the capacity you're trying to protect. Reject at the outermost layer.

> ⚠️ **Retrying 503s immediately** turns load shedding into a retry storm. Clients need backoff plus jitter plus retry budgets.

> ⚠️ **Rate limiting with `time.Sleep(time.Second / rps)`** in each worker ignores bursts, drifts, and multiplies by the worker count.

> ⚠️ **Per-key maps without eviction** are memory leaks driven by attacker-controlled keys (IPs, user IDs).

### IRL Scenario: the metastable failure

**Context:** A service at 70% capacity had a 30 s GC/network hiccup. Afterwards it stayed down at 100% CPU for an hour, even though incoming traffic was back to normal.

**Mechanism (a metastable failure):** During the hiccup, requests queued. Clients timed out at 2 s and retried, so effective load roughly doubled. The service burned all its capacity on requests whose clients had already given up (queue time > 2 s), so goodput was ~0 and the retries kept the load doubled. The system stayed in that bad state with no external trigger.

**Fix:**
1. Admission control with a queue timeout less than the client timeout (500 ms).
2. Check `r.Context().Err()` before expensive work, so abandoned requests are skipped.
3. Client retry budget (≤10% extra load) and jittered backoff.
4. Adaptive LIFO in the queue.

Recovery after the next incident took seconds.

### Exercises

1. ⭐ What is the difference between `rate.NewLimiter(10, 1)` and `rate.NewLimiter(10, 10)` for a client that sends 10 requests at once every second?
2. ⭐⭐ Extend `Admission` to export queue depth and queue-time histograms. Which of them is the better autoscaling signal?
3. ⭐⭐⭐ Implement a CoDel-style queue: `Push(ctx, req)`, `Pop() (req, bool)`, dropping when the minimum sojourn time over 100 ms exceeds 5 ms. Simulate a burst and a sustained overload and compare against a plain bounded FIFO.

<details>
<summary>Solutions</summary>

1. Burst 1: the first request passes, and the rest are spaced at 100 ms (Wait) or rejected (Allow). Burst 10: all 10 pass at once, then the bucket refills over the next second.
2. Queue time is usually the better signal: it captures both arrival rate and processing slowdown, and it relates directly to user-visible latency. Depth depends on concurrency settings.
3. Track each request's enqueue time. On each pop, compute its sojourn time. Keep the minimum over the current interval, and when an interval ends with min > target, enter dropping mode, dropping at increasing frequency (`interval/sqrt(count)`) until the minimum falls below target.

</details>

### Review Checklist

- [ ] For each queue: what happens when input > output? Is that the deliberate choice?
- [ ] Is load shed at the edge, before expensive work?
- [ ] Are queue *times* bounded below client timeouts?
- [ ] Are both concurrency limits and rate limits in place where needed?
- [ ] Do per-key structures have eviction?
- [ ] Do clients retry with budgets and jitter?

---

## Chapter 21: singleflight and Request Coalescing

### TL;DR

- `singleflight.Group` lets **concurrent callers with the same key share one execution** of a function: the first one runs it, and the others wait for its result.
- It is the standard defence against **cache stampedes / thundering herds** on hot keys.
- **Gotchas:** every waiter gets the **same error**; with `Do`, every waiter is stuck until the one execution finishes, and the work runs under the **first caller's ctx** (cancelling it fails everyone); results are **shared**, so they must be treated as immutable.
- Use `DoChan` + a detached context for the work, so each caller can abandon waiting independently.

### Under the Hood (`x/sync/singleflight`)

A mutex-guarded `map[string]*call`, where `call` holds a `WaitGroup`, the result, and a duplicate count. The first caller creates the call, runs `fn`, stores the result, calls `wg.Done()`, and deletes the key. Later callers find the call and `wg.Wait()`. `DoChan` callers get a buffered channel instead. `Forget(key)` deletes the key so the next caller starts a fresh execution. With `Do`, panics in `fn` are re-raised in the waiters, and `runtime.Goexit` is handled. With `DoChan`, a panic in `fn` **crashes the process unrecoverably** (singleflight re-panics in a fresh goroutine), so recover inside `fn` if the loader can panic.

### Practical Code: stampede-proof cache loader

```go
type entry[V any] struct {
	val     V
	expires time.Time
}

type LoadingCache[V any] struct {
	mu    sync.RWMutex
	items map[string]entry[V]
	g     singleflight.Group
	ttl   time.Duration
	load  func(ctx context.Context, key string) (V, error)
}

func (c *LoadingCache[V]) Get(ctx context.Context, key string) (V, error) {
	c.mu.RLock()
	e, ok := c.items[key]
	c.mu.RUnlock()
	if ok && time.Now().Before(e.expires) {
		return e.val, nil
	}

	ch := c.g.DoChan(key, func() (any, error) {
		// Detach from the first caller's cancellation (keep its values, e.g. trace IDs),
		// and bound the shared work independently.
		lctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		v, err := c.load(lctx, key)
		if err != nil {
			return v, err
		}
		c.mu.Lock()
		c.items[key] = entry[V]{val: v, expires: time.Now().Add(c.ttl)}
		c.mu.Unlock()
		return v, nil
	})

	select {
	case r := <-ch:
		if r.Err != nil {
			var zero V
			return zero, r.Err
		}
		v, _ := r.Val.(V) // comma-ok: a nil interface value would make r.Val.(V) panic
		return v, nil
	case <-ctx.Done(): // this caller gives up; shared work continues for others
		var zero V
		return zero, ctx.Err()
	}
}
```

#### Stale-while-revalidate: never block on refresh

```go
func (c *LoadingCache[V]) GetSWR(ctx context.Context, key string) (V, error) {
	c.mu.RLock()
	e, ok := c.items[key]
	c.mu.RUnlock()
	if ok {
		if time.Now().After(e.expires) {
			// Trigger a background refresh (deduplicated); serve stale now.
			c.g.DoChan(key, func() (any, error) { return c.refresh(key) })
		}
		return e.val, nil
	}
	return c.Get(ctx, key) // cold miss: must wait
}
```

`DoChan`'s result channel is buffered, so dropping it doesn't leak the goroutine. The refresh runs in singleflight's own goroutine.

#### Probabilistic early expiration (XFetch)

Instead of every node refreshing exactly at the TTL, each read refreshes early with a probability that increases as expiry approaches:

```go
// Refresh early if: now - delta*beta*ln(rand) >= expiry
// delta = how long the last recompute took; beta ≈ 1.0
func shouldRefreshEarly(expiry time.Time, delta time.Duration, beta float64) bool {
	jitter := time.Duration(-float64(delta) * beta * math.Log(rand.Float64()))
	return time.Now().Add(jitter).After(expiry)
}
```

This spreads refreshes over time across a fleet, where singleflight alone only deduplicates *within a process*.

### Pitfalls

> ⚠️ **Mutating the shared result.** All waiters receive the same pointer or slice. If one caller appends to it or modifies it, that's a data race with the others. Return immutable values or copy them per caller.

> ⚠️ **Caching errors by accident.** singleflight doesn't cache, but if your wrapper stores error results, a transient failure becomes sticky. Also, every concurrent waiter gets the failure, so consider `Forget` + retry for transient errors.

> ⚠️ **Key cardinality.** Keys must identify *identical* requests. Including the user ID when the data is per-user is right, while omitting it leaks data across users.

> ⚠️ **One slow execution blocks all waiters** (with `Do`). Use `DoChan` with per-caller ctx, plus a bounded timeout on the shared work.

### IRL Scenario: the homepage outage at midnight

**Context:** The homepage config was cached for 60 s. At peak (~30k req/s), each expiry sent ~3,000 concurrent requests to the config DB in the ~100 ms it took to reload. The DB connection pool saturated, latency rose, more requests piled up, and the outage lasted 4 minutes.

**Fix (layered):**
1. singleflight per key per process: 1 DB call per process per expiry.
2. Stale-while-revalidate: users never wait on a refresh.
3. XFetch-style early refresh with jitter, so 200 pods don't all refresh in the same second.

### Exercises

1. ⭐ What does the `shared` return value of `Do` tell you, and when is it useful?
2. ⭐⭐ Write a test proving that 100 concurrent `Get`s on a cold key call `load` exactly once.
3. ⭐⭐⭐ Implement a "batching coalescer": concurrent `Get(key)` calls within a 2 ms window are merged into one `loadMany(keys)` call (a DataLoader pattern). Handle partial failures and ctx cancellation per caller.

<details>
<summary>Solutions</summary>

1. `shared` is true if the result was given to multiple callers. Useful for metrics (the dedup ratio) and as a reminder to treat the value as immutable.
2. Use an `atomic.Int32` call counter in `load`, a `sync.WaitGroup` for 100 goroutines all released by closing a start channel, and make `load` block on a gate channel until all 100 are waiting. That last step is what makes the test deterministic. With `testing/synctest`, use `synctest.Wait()` to know when all of them are blocked.
3. A collector goroutine owns a pending map `key → []chan result`. The first request in a window starts a 2 ms timer. On fire (or when a max batch size is reached), call `loadMany` with the keys and fan results out to each waiter's buffered channel. Callers select on their channel and their ctx.

</details>

### Review Checklist

- [ ] Are hot-key cache misses deduplicated?
- [ ] Does the shared work run under a detached, bounded context?
- [ ] Are shared results immutable?
- [ ] Is fleet-wide refresh synchronisation (stampede across pods) addressed?

---

## Chapter 22: Ownership, Actors and Pub/Sub

### TL;DR

- **Confinement** is the cheapest form of thread safety: data that only one goroutine can reach needs no synchronisation.
- **Lexical confinement:** the goroutine that creates a channel owns writing to it and closing it, and returns a receive-only `<-chan T` to everyone else. The type system enforces the ownership.
- **Actors** (one goroutine owning state, driven by messages) fit state machines with timers and I/O. They need a mailbox bound, a lifecycle, and supervision.
- **Pub/sub** inside a process needs an explicit **slow-subscriber policy**: block, drop, or disconnect.

### Practical Code

#### Lexical confinement

```go
// Generate owns `out`: only it writes and closes. Callers can only receive.
func Generate(ctx context.Context, n int) <-chan int {
	out := make(chan int)
	go func() {
		defer close(out)
		for i := range n {
			select {
			case out <- i:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}
```

#### Actor with supervision

```go
type Actor struct {
	inbox chan func(*state) // closures as messages: flexible, type-safe
	done  chan struct{}
}

type state struct {
	conns map[string]*Conn
}

func StartActor(ctx context.Context, mailbox int) *Actor {
	a := &Actor{inbox: make(chan func(*state), mailbox), done: make(chan struct{})}
	go a.supervise(ctx)
	return a
}

func (a *Actor) supervise(ctx context.Context) {
	defer close(a.done)
	st := &state{conns: map[string]*Conn{}}
	for attempt := 0; ctx.Err() == nil; attempt++ {
		if crashed := a.run(ctx, st); !crashed {
			return
		}
		// State may be inconsistent after a panic: rebuild it.
		st = &state{conns: map[string]*Conn{}}
		if Sleep(ctx, backoff(attempt)) != nil {
			return
		}
	}
}

func (a *Actor) run(ctx context.Context, st *state) (crashed bool) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("actor panic: %v\n%s", r, debug.Stack())
			crashed = true
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return false
		case fn := <-a.inbox:
			fn(st)
		}
	}
}

// Ask sends a query and waits for the reply, respecting ctx and actor shutdown.
func Ask[R any](ctx context.Context, a *Actor, q func(*state) R) (R, error) {
	reply := make(chan R, 1)
	msg := func(s *state) { reply <- q(s) }
	var zero R
	select {
	case a.inbox <- msg:
	case <-a.done:
		return zero, ErrStopped
	case <-ctx.Done():
		return zero, ctx.Err()
	}
	select {
	case r := <-reply:
		return r, nil
	case <-a.done:
		return zero, ErrStopped
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}
```

> ⚠️ If `q` panics, `run` recovers and restarts the actor, but that message's `reply` is never written and `done` isn't closed, so `Ask` waits until its ctx ends (forever with `Background`). Recover inside `msg` and send an error reply, or always give `Ask` a deadline.

> ⚠️ With closures as messages, a careless message can capture and leak the `*state` pointer to another goroutine, which breaks confinement. Review closures for any escape of `s`. Typed message structs are stricter, at the cost of more code.

#### Pub/sub broker with slow-subscriber policy

```go
type Broker[T any] struct {
	mu   sync.RWMutex
	subs map[*sub[T]]struct{}
}

type sub[T any] struct {
	ch      chan T
	dropped atomic.Int64
	policy  Policy // DropNewest or Disconnect
	once    sync.Once
}

type Policy int

const (
	DropNewest Policy = iota
	Disconnect
)

func NewBroker[T any]() *Broker[T] { return &Broker[T]{subs: map[*sub[T]]struct{}{}} }

func (b *Broker[T]) Subscribe(buf int, p Policy) (<-chan T, func()) {
	s := &sub[T]{ch: make(chan T, buf), policy: p}
	b.mu.Lock()
	b.subs[s] = struct{}{}
	b.mu.Unlock()
	return s.ch, func() { b.remove(s) }
}

func (b *Broker[T]) remove(s *sub[T]) {
	s.once.Do(func() {
		b.mu.Lock()
		delete(b.subs, s)
		b.mu.Unlock()
		close(s.ch) // safe: no Publish can be sending (sends happen under RLock; we held Lock)
	})
}

func (b *Broker[T]) Publish(v T) {
	var slow []*sub[T]
	b.mu.RLock()
	for s := range b.subs {
		select {
		case s.ch <- v:
		default:
			s.dropped.Add(1)
			if s.policy == Disconnect {
				slow = append(slow, s)
			}
		}
	}
	b.mu.RUnlock()
	for _, s := range slow {
		b.remove(s) // outside RLock: remove takes the write lock
	}
}
```

Concurrent `Publish` calls can interleave, so different subscribers may see different orderings. If you need a **global order**, serialize publishing through a single goroutine.

### Pitfalls

> ⚠️ **Unbounded mailboxes** (a slice-backed queue that grows on demand) turn a slow actor into an OOM.

> ⚠️ **Actor calling Ask on itself** (directly or through a cycle of actors) deadlocks. Actor graphs need the same "no cycles while waiting" discipline as lock orders.

> ⚠️ **Publish holding a lock while blocking on a slow subscriber** stalls every publisher. Never do a blocking send under a lock.

### IRL Scenario: the WebSocket fan-out that froze

**Context:** A chat server broadcast each message to the room's subscribers with a blocking `conn.Write` in a loop, under the room mutex. One mobile client on a bad network stalled `Write` for 60 s, and the entire room froze, including joins and leaves that needed the mutex.

**Fix:** Each connection gets its own writer goroutine and a buffered outbound channel (64). The broadcast does a non-blocking send to each. If a client's buffer is full, the server disconnects it (the client reconnects and resyncs from history). The room lock now only guards the membership map and is held for microseconds.

### Review Checklist

- [ ] Is ownership enforced by types (`<-chan`, `chan<-`) where possible?
- [ ] Do actors have bounded mailboxes, done signals, and supervision with backoff?
- [ ] Is there a documented slow-consumer policy for every fan-out?
- [ ] Are there cycles in actor request graphs?

---

## Chapter 23: Structured Concurrency

### TL;DR

- **Principle:** a goroutine's lifetime should be **contained within a lexical scope or an owned object**, just as a function call's lifetime is. When a function returns, every goroutine it started has finished, or is owned by something with a `Close`/`Wait`.
- **"Never start a goroutine without knowing how it will stop."** — Dave Cheney
- In Go: `errgroup` is a nursery, a component exposes `Run(ctx) error` (blocking, returns when done), and no constructor starts background goroutines without a matching stop method.
- The benefits: no leaks, errors propagate, cancellation propagates, panics are attributable, and tests can verify that everything stopped.

### Mental Model

```
Unstructured:                     Structured:
 func Handle() {                   func Handle(ctx) error {
   go work()   ← escapes            g, ctx := errgroup.WithContext(ctx)
   return                           g.Go(func() error { return work(ctx) })
 }   work runs... until?            return g.Wait()   ← nothing escapes
                                   }
```

### Practical Code

#### Component pattern: `Run(ctx) error`

```go
type Component interface {
	Run(ctx context.Context) error // blocks until ctx is done or a fatal error occurs
}

// RunAll runs components together. The first failure (or ctx cancellation) stops all.
func RunAll(ctx context.Context, cs ...Component) error {
	g, ctx := errgroup.WithContext(ctx)
	for _, c := range cs {
		g.Go(func() error { return c.Run(ctx) })
	}
	return g.Wait()
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	err := RunAll(ctx,
		&HTTPServer{Addr: ":8080"},
		&Consumer{Topic: "orders"},
		&MetricsFlusher{Every: 10 * time.Second},
	)
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}
```

#### Adapting http.Server to `Run(ctx)`

```go
type HTTPServer struct {
	Addr    string
	Handler http.Handler
}

func (s *HTTPServer) Run(ctx context.Context) error {
	srv := &http.Server{
		Addr:        s.Addr,
		Handler:     s.Handler,
		BaseContext: func(net.Listener) context.Context { return ctx }, // request ctx derives from ours
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()

	select {
	case err := <-errc:
		return err // failed to start, or crashed
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 20*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return err
		}
		<-errc // ListenAndServe returns ErrServerClosed; wait for it so nothing escapes
		return nil
	}
}
```

> ⚠️ With `BaseContext` returning the run ctx, **in-flight requests are cancelled the moment shutdown starts**. That may be what you want (fast stop), or not (graceful drain). For a graceful drain, give `BaseContext` a separate ctx that you cancel only *after* `Shutdown` returns or times out (Chapter 30).

#### Objects that own goroutines: start in `Run` or provide `Close`

```go
// ❌ Constructor starts a goroutine; there is no way to stop it.
func NewCache() *Cache {
	c := &Cache{}
	go c.evictLoop() // leaks in every test that creates a Cache
	return c
}

// ✅ Owned lifecycle
func NewCache() *Cache {
	c := &Cache{stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(c.done)
		c.evictLoop(c.stop)
	}()
	return c
}

func (c *Cache) Close() error {
	c.closeOnce.Do(func() { close(c.stop) })
	<-c.done // wait: Close returns only after the goroutine has exited
	return nil
}
```

The `stop` + `done` pair is the minimal structured handle: one channel to signal, one to confirm. `Close` that doesn't wait is a half-measure, because tests and shutdown can still race the goroutine.

### Pitfalls

> ⚠️ **"Fire and forget" goroutines in request handlers.** They escape the request, ignore shutdown, and their errors vanish. Route them through a tracked background runner (a WaitGroup owned by the server, drained on shutdown) or a durable queue.

> ⚠️ **Returning before `Wait`.** Early `return err` inside a function that started goroutines, before `wg.Wait()`, leaks them and possibly races with the caller reusing data. Use errgroup, or `defer wg.Wait()` placed right after starting them.

> ⚠️ **Defer ordering with cancel + wait.** `defer cancel(); defer wg.Wait()` runs `Wait` **first** (LIFO), so if the goroutines wait on ctx, you deadlock. Write `defer func() { cancel(); wg.Wait() }()`.

### IRL Scenario: flaky tests from leaked goroutines

**Context:** A test suite showed random failures in unrelated tests: "log after test completed" panics, port-in-use errors, and race reports pointing at code from a *previous* test.

**Diagnosis:** Dozens of components started goroutines in constructors with no `Close`. Goroutines from test A kept running during test B, logged to A's (finished) `testing.T`, held ports, and mutated shared fakes.

**Fix:** Every component got `Run(ctx)` or `Close()`-that-waits. Tests used `t.Cleanup(func(){ c.Close() })` and `goleak.VerifyTestMain(m)`. The flakiness disappeared, and the leak checker now catches regressions.

### Exercises

1. ⭐ Rewrite `go sendAnalytics(event)` in a handler so it's tracked and drained at shutdown, with a bounded number in flight.
2. ⭐⭐ Implement a `Supervisor` component that runs a child `Component`, restarts it on error with backoff (unless ctx is done), and gives up after K consecutive failures within a window.

<details>
<summary>Solutions</summary>

1. ```go
   type Background struct {
   	mu     sync.Mutex
   	closed bool
   	sem    chan struct{}
   	wg     sync.WaitGroup
   	ctx    context.Context // server lifetime ctx, values-only
   }

   func (b *Background) Go(f func(ctx context.Context)) bool {
   	b.mu.Lock()
   	defer b.mu.Unlock() // mu orders every wg.Go before Drain's Wait
   	if b.closed {
   		return false // draining: refuse new work
   	}
   	select {
   	case b.sem <- struct{}{}:
   	default:
   		return false // shed: too much background work
   	}
   	b.wg.Go(func() {
   		defer func() { <-b.sem }()
   		ctx, cancel := context.WithTimeout(b.ctx, 5*time.Second)
   		defer cancel()
   		f(ctx)
   	})
   	return true
   }

   func (b *Background) Drain(ctx context.Context) error {
   	b.mu.Lock()
   	b.closed = true // no Go can start after this point
   	b.mu.Unlock()
   	done := make(chan struct{})
   	go func() { b.wg.Wait(); close(done) }()
   	select {
   	case <-done:
   		return nil
   	case <-ctx.Done():
   		return ctx.Err()
   	}
   }
   ```
   The mutex and `closed` flag matter: without them, `Go` can call `wg.Go` while `Drain` is already in `Wait`, which breaks the WaitGroup rule (a positive Add at zero must happen before Wait). The race detector flags it.
2. Loop: `err := child.Run(ctx)`; if ctx is done, return nil; record the failure timestamp; prune timestamps outside the window; if count ≥ K, return the wrapped error; otherwise `Sleep(ctx, backoff(n))` and continue. Reset the backoff after a long healthy run.

</details>

### Review Checklist

- [ ] For every `go` statement: who waits for it, and what stops it?
- [ ] Do constructors avoid starting goroutines, or do they provide a `Close` that waits?
- [ ] Do long-running components implement `Run(ctx) error`?
- [ ] Do tests use goleak or an equivalent?
- [ ] Is background work from requests tracked, bounded and drained?

---

## Chapter 24: Generics, Iterators and Concurrency

### TL;DR

- Generics make **reusable concurrency helpers** practical: `ParallelMap`, typed pools, typed sync maps, result types.
- **Range-over-func iterators** `[Go 1.23+]` are **synchronous**: `yield` runs the loop body on the iterator's goroutine, inline. Calling `yield` from another goroutine, concurrently, or after it returned `false` is a bug (the runtime panics on the "after false" case).
- To make an iterator **concurrent** (prefetching, parallel producers), bridge through a channel, and **stop the producer when the consumer breaks early** (cancel plus wait in the iterator's cleanup).
- `iter.Pull` uses coroutine switching, not parallelism, and its `next`/`stop` functions are **not safe for concurrent use**.

### Practical Code

#### Generic bounded parallel map (ordered results)

```go
func ParallelMap[T, U any](ctx context.Context, in []T, limit int,
	fn func(context.Context, T) (U, error)) ([]U, error) {

	out := make([]U, len(in))
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(limit)
	for i, v := range in {
		if ctx.Err() != nil {
			break // stop launching after the first failure
		}
		g.Go(func() error {
			u, err := fn(ctx, v)
			if err != nil {
				return err
			}
			out[i] = u
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return out, nil
}
```

#### Iterators: what goes wrong

```go
// ❌ Calls yield from multiple goroutines: data race on the loop body's state, undefined behaviour.
func ParallelBad[T any](items []T) iter.Seq[T] {
	return func(yield func(T) bool) {
		var wg sync.WaitGroup
		for _, it := range items {
			wg.Go(func() { yield(it) }) // NO
		}
		wg.Wait()
	}
}
```

The loop body is ordinary sequential code (`for x := range seq { total += x }`). The caller has every right to assume it runs on one goroutine, one iteration at a time.

#### ✅ Prefetching iterator: concurrent producer, sequential yield

```go
// Prefetch runs seq on a background goroutine, buffering up to buf items ahead.
// The consumer's loop body still runs sequentially on the caller's goroutine.
func Prefetch[T any](ctx context.Context, seq iter.Seq[T], buf int) iter.Seq[T] {
	return func(yield func(T) bool) {
		ctx, cancel := context.WithCancel(ctx)
		ch := make(chan T, buf)
		var wg sync.WaitGroup
		wg.Go(func() {
			defer close(ch)
			for v := range seq {
				select {
				case ch <- v:
				case <-ctx.Done():
					return // consumer broke out early, or ctx cancelled
				}
			}
		})
		// One defer: cancel FIRST, then wait. (Two separate defers would run in LIFO order: wait, then cancel, which deadlocks.)
		defer func() {
			cancel()
			wg.Wait() // structured: the producer has exited before the range loop ends
		}()

		for v := range ch {
			if !yield(v) {
				return // early break → deferred cancel + wait
			}
		}
	}
}

// Usage: page fetches overlap with processing.
for page := range Prefetch(ctx, apiPages(ctx, client), 2) {
	process(page)
	if done(page) {
		break // producer goroutine is stopped and awaited
	}
}
```

> ⚠️ The producer can only notice cancellation *between* elements of `seq`. If `seq` itself blocks (a slow page fetch), the break waits for that fetch. Passing the *caller's* ctx to `seq` (as `apiPages(ctx, ...)` does) only helps when the caller cancels. It doesn't interrupt an in-progress fetch on `break`. For a prompt early break, take a `func(context.Context) iter.Seq[T]` and call it with Prefetch's internal ctx.

#### Iterator over results of parallel work (unordered, as completed)

```go
func AsCompleted[T, U any](ctx context.Context, in []T, limit int,
	fn func(context.Context, T) U) iter.Seq[U] {
	return func(yield func(U) bool) {
		ctx, cancel := context.WithCancel(ctx)
		results := make(chan U)
		var wg sync.WaitGroup
		sem := make(chan struct{}, limit)
		go func() {
			defer func() { wg.Wait(); close(results) }()
			for _, v := range in {
				select {
				case sem <- struct{}{}:
				case <-ctx.Done():
					return
				}
				wg.Go(func() {
					defer func() { <-sem }()
					u := fn(ctx, v)
					select {
					case results <- u:
					case <-ctx.Done():
					}
				})
			}
		}()
		defer func() {
			cancel()
			for range results { // drain until the launcher closes results:
			} // guarantees every goroutine has exited
		}()
		for u := range results {
			if !yield(u) {
				return
			}
		}
	}
}
```

### Pitfalls

> ⚠️ **Holding a lock across `yield`.** An iterator that does `mu.Lock(); for ... { yield(v) }; mu.Unlock()` runs the caller's loop body *under your lock*. If the body calls back into your type, it deadlocks. Snapshot under the lock, then yield outside it (the same rule as callbacks).

> ⚠️ **Sharing an `iter.Pull` pair across goroutines.** `next` and `stop` must be called from one goroutine at a time.

> ⚠️ **Forgetting early-break cleanup** in channel-backed iterators leaks the producer on every `break`.

> 🤖 **AI note:** When asked to make an iterator concurrent, AI often produces the `ParallelBad` shape (yield from goroutines), or a channel bridge with no cancellation on break.

### Exercises

1. ⭐ Why is it acceptable for `Prefetch`'s producer goroutine to range over `seq`, given that only one goroutine may call a given `yield`?
2. ⭐⭐ Write a test that breaks out of `Prefetch` after 1 item and asserts (with goleak or a counter) that no goroutine remains.
3. ⭐⭐⭐ Implement `Merge[T](ctx, seqs ...iter.Seq[T]) iter.Seq[T]` that runs each input sequence on its own goroutine and yields items as they arrive, with correct cleanup on early break.

<details>
<summary>Solutions</summary>

1. The *inner* `seq`'s yield is called only by the producer goroutine (one goroutine), and the *outer* yield only by the consumer goroutine. Each yield has exactly one caller goroutine. That is the invariant that matters.
2. `defer goleak.VerifyNone(t)`; `for range Prefetch(ctx, infiniteSeq, 4) { break }`. goleak retries briefly, so the cleanup `wg.Wait()` makes it pass deterministically.
3. Like `AsCompleted`: one goroutine per seq sends to a shared channel with a ctx select, a WaitGroup plus closer goroutine closes the channel, and the deferred cleanup cancels and then drains the channel until it's closed.

</details>

### Review Checklist

- [ ] Is `yield` only ever called from the iterator's own goroutine, sequentially?
- [ ] Do concurrent iterators stop and await producers on early break?
- [ ] Are locks released before yielding or calling callbacks?
- [ ] Do generic helpers propagate ctx and errors rather than hiding them?

---

[← Part V](05-context-and-time.md) · [Index](README.md) · [Next: Part VII, Failure Modes & Debugging →](07-failure-modes-and-debugging.md)
