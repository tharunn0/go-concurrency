# Part VII: Failure Modes and Debugging

[← Part VI](06-patterns.md) · [Index](README.md) · [Next: Part VIII, Performance & Production →](08-performance-and-production.md)

- [Chapter 25: A Taxonomy of Concurrency Failures](#chapter-25-a-taxonomy-of-concurrency-failures)
- [Chapter 26: Tooling: Race Detector, pprof, Trace](#chapter-26-tooling-race-detector-pprof-trace)
- [Chapter 27: Testing Concurrent Code](#chapter-27-testing-concurrent-code)

---

## Chapter 25: A Taxonomy of Concurrency Failures

### TL;DR

- Know the failure classes by **symptom**. In production you see symptoms (hangs, growth, wrong data, latency), never the cause directly.
- **Data race ≠ race condition.** A data race is unsynchronized memory access (a memory-model violation). A race condition is a *logic* bug where the outcome depends on timing, and it can exist in code with perfect synchronisation.
- The Go runtime only detects **global** deadlock ("all goroutines are asleep"). **Partial deadlocks**, where some goroutines are stuck and others still run, are silent and look like leaks or hangs.
- Deadlock requires all four **Coffman conditions**. Break any one of them and the deadlock is impossible.

### The taxonomy

| Failure | Definition | Production symptom | How to find it | Typical fix |
|---|---|---|---|---|
| **Data race** | Concurrent unsynchronized access, at least one a write | Corrupted values, random crashes, "impossible" states, `concurrent map writes` | `-race` in tests/canary | Synchronise (mutex/channel/atomic) or confine |
| **Race condition** (logical) | Correctness depends on interleaving | Rare wrong results: double-booking, lost updates, over-admission | Reasoning, stress tests, linearizability checkers | Make check-and-act atomic (single critical section, CAS, DB transaction) |
| **Global deadlock** | Every goroutine blocked | `fatal error: all goroutines are asleep - deadlock!` | Crash message | Fix the cycle |
| **Partial deadlock** | A subset blocked in a cycle | Specific endpoints hang, goroutine count climbs | Goroutine dump: look for `sync.Mutex.Lock` / `chan send` with long wait durations | Lock ordering, no blocking under lock |
| **Goroutine leak** | Goroutine blocked forever (not a cycle) | Memory and goroutine count growth, eventual OOM | `NumGoroutine` metric, goroutine profile diff, goleak, `goroutineleak` profile (1.26 experiment) | Cancellation on every blocking op, ownership |
| **Livelock** | Goroutines active but making no progress (retrying in lockstep) | High CPU, zero throughput | CPU profile shows retry loops; trace shows busy, no progress | Randomised backoff, break symmetry |
| **Starvation** | Some goroutine never gets a resource | Tail-latency outliers, timeouts for some keys or tenants | Mutex/block profiles, per-tenant latency | Fair queues, starvation mode, separate pools |
| **Lost wakeup** | Signal sent before the waiter waits | Occasional hang until timeout | Reasoning, stress | Check condition under the lock / grab the channel before checking |
| **Panic: send on closed / close of closed** | Ownership violation | Crash | Stack trace | Single owner closes |
| **Priority inversion** | High-priority work waits on low-priority work holding a resource | Latency-critical path slow during batch jobs | Mutex profile + trace | Don't share locks across priority classes, keep critical sections short |
| **Lock convoy** | Many goroutines serialise behind a lock repeatedly | Throughput collapse with more cores | Mutex profile, `/sync/mutex/wait/total:seconds` | Shard, batch, copy-on-write |
| **Thundering herd** | Many waiters wake for one event, all contend | CPU spike, latency spike on cache expiry or reconnect | Correlate with events | singleflight, jitter, `Signal` vs `Broadcast` |

### Deadlock theory: Coffman conditions

All four must hold:
1. **Mutual exclusion:** resources can't be shared.
2. **Hold and wait:** holding one resource while waiting for another.
3. **No preemption:** resources can't be forcibly taken.
4. **Circular wait:** a cycle in the wait-for graph.

Practical ways to break them in Go:
- **Circular wait:** global lock order (by ID or by address), and document it.
- **Hold and wait:** never block (channel send, I/O, another lock, `wg.Wait`) while holding a mutex. Snapshot, unlock, then act.
- **No preemption:** acquire with a timeout or ctx (`semaphore.Weighted.Acquire(ctx)`, channel-based locks), then back off and retry.

> 💡 **Insight:** Channels participate in deadlocks just like locks. A goroutine that holds a mutex and does `ch <- v` is "holding and waiting". The receiver might need the same mutex.

### Reading a goroutine dump: a field guide

```
goroutine 1234 [sync.Mutex.Lock, 47 minutes]:
internal/sync.runtime_SemacquireMutex(...)     ← Go 1.24+ frames live in internal/sync
internal/sync.(*Mutex).lockSlow(...)
internal/sync.(*Mutex).Lock(...)
sync.(*Mutex).Lock(...)
main.(*Store).Update(0xc0001a0000, ...)        ← who's waiting, where
        /app/store.go:88
...
goroutine 77 [chan send, 47 minutes]:
main.(*Store).publish(...)                     ← who holds it? look for the same Store
        /app/store.go:131                         in a non-lock wait state
main.(*Store).Update(...)                      ← Update → publish → blocked on chan while holding mu
        /app/store.go:92
```

**Method:**
1. Get the aggregated view first: `curl .../debug/pprof/goroutine?debug=1` groups identical stacks with counts, so the outliers jump out (`3,412 @ ... store.go:88`).
2. Look at **wait reasons and durations**: `[chan send, 47 minutes]` means a leak or deadlock. `[IO wait]` is usually normal. `[select]` depends.
3. Find the **holder**: for a stuck lock, look for a goroutine inside the same object's methods *not* waiting on the lock. Its stack shows what it's blocked on while holding it.
4. Draw the wait-for graph for 2–3 goroutines. Cycles show up quickly.

Common wait reasons: `chan receive`, `chan send`, `select`, `select (no cases)`, `sync.Mutex.Lock`, `sync.RWMutex.RLock`, `sync.RWMutex.Lock`, `sync.WaitGroup.Wait`, `sync.Cond.Wait`, `semacquire`, `IO wait`, `sleep`, `syscall`, `GC assist wait`, `chan receive (nil chan)`, `chan send (nil chan)`. Inside a synctest bubble you will also see durable variants, e.g. `chan receive (durable)`, `sync.WaitGroup.Wait (durable)`.

> 💡 `chan receive (nil chan)` / `chan send (nil chan)` in a dump is almost always a bug: a channel field that was never initialised.

### IRL Scenario: the double-booking that passed all race tests

**Context:** A booking service checked availability, then inserted a reservation. Every access was properly mutex-protected, and `-race` was clean. Yet customers were occasionally double-booked.

**Diagnosis:** It wasn't in-process at all. Two *replicas* ran the check-then-insert against the DB concurrently. Each one's mutex protected only its own process. It was a logical race condition across processes.

**Fix:** Move atomicity to where the shared state lives: a unique constraint on `(room, date)` plus handling the conflict error, or `SELECT ... FOR UPDATE` in a transaction, or optimistic concurrency with a version column.

**Lesson:** In-process synchronisation only protects in-process state. In distributed systems, the race detector can't help you. Locks belong with the data.

### Review Checklist

- [ ] For each symptom class, is there a metric or alert (goroutines, mutex wait, thread count, latency per tenant)?
- [ ] Is there a documented lock order where several locks are held?
- [ ] Does anything block (send, I/O, lock, wait) while holding a mutex?
- [ ] Are check-then-act sequences atomic *at the layer that owns the state*?
- [ ] Can the team get a goroutine dump from production within minutes?

---

## Chapter 26: Tooling: Race Detector, pprof, Trace

### TL;DR

- **Race detector** (`-race`): ThreadSanitizer-based and happens-before precise, with no false positives in pure Go code. It only finds races **on paths that execute**. Cost: memory ~5–10×, execution time ~2–20×. Run it in CI on every test, and optionally in a canary.
- **pprof profiles** for concurrency: `goroutine` (what is everyone doing), `block` (where goroutines wait), `mutex` (where contention happens, attributed to the **unlocker**), `threadcreate`, plus CPU.
- **Execution trace** (`go tool trace`): the definitive view of scheduling, blocking, GC, syscalls and latency over time. Overhead is low since Go 1.21 (~1–2%). The **flight recorder** `[Go 1.25+]` keeps a rolling window in memory so you can dump it when something goes wrong.
- `runtime/metrics` for continuous, cheap production signals.

### Race detector

#### How it works

- Instrumented builds record every memory access and sync operation. Each goroutine has a **vector clock**. Sync operations (channel ops, mutexes, atomics, WaitGroup…) merge clocks to build happens-before.
- **Shadow memory** stores the last few accesses per 8-byte word, with goroutine ID and clock. A new access that conflicts with a stored one, where neither happens before the other, gets reported.
- Shadow memory keeps only a few cells per 8-byte word, so with many accesses to the same word, old conflicting accesses can be evicted and a race missed. Separately, `GORACE="history_size=7"` enlarges each goroutine's access history (more memory). That doesn't find more races. It fixes reports whose previous access says "failed to restore the stack".

#### Using it well

```bash
go test -race ./...                        # every CI run
go test -race -count=50 -run TestCache ./cache  # repeat to explore interleavings
go test -race -cpu=1,2,8 ./...             # vary GOMAXPROCS
GORACE="halt_on_error=1" ./app-race        # stop on first report (canary)
go build -race -o app-race .               # race-enabled binary for staging or canary
```

> ⚠️ A clean `-race` run proves nothing about code paths that didn't run, or about interleavings that didn't happen. Pair it with tests that **actually exercise concurrency**: many goroutines, tight loops, `-count`, the `stress` tool.

#### Reading a report

```
WARNING: DATA RACE
Write at 0x00c000124018 by goroutine 8:
  main.(*Cache).Get()  cache.go:22        ← the write (hits++ under RLock)
Previous read at 0x00c000124018 by goroutine 7:
  main.(*Cache).Get()  cache.go:22
Goroutine 8 (running) created at:
  main.main()  main.go:14                  ← where the goroutines came from
```

Both stacks matter. Fix the *design* (who owns `hits`?), not just the line.

### pprof for concurrency

#### Enabling the profiles

```go
import (
	"net/http"
	_ "net/http/pprof"
	"runtime"
)

func init() {
	runtime.SetMutexProfileFraction(10) // sample ~1/10 contention events
	runtime.SetBlockProfileRate(10_000) // ~1 sample per 10µs blocked; events ≥10µs always recorded (ns units)
}

func startDebugServer() {
	go http.ListenAndServe("localhost:6060", nil) // internal interface only!
}
```

Both profiles have a small, tunable cost. Mutex fraction 5–100 and block rate 10k–1M ns are typical production values. Measure the overhead in your own service.

#### What each profile tells you

| Profile | Question it answers | Gotcha |
|---|---|---|
| `goroutine` | What is every goroutine doing *now*? | `debug=1` aggregated, `debug=2` full stacks with wait times |
| `block` | Where do goroutines **wait** (chan, select, mutex, cond, wg)? Cumulative delay | Includes *intended* waits (idle workers on `select`). Filter by focus. |
| `mutex` | Where is lock contention? Cumulative delay **caused by the holder** | Stack is at **Unlock** (the holder's code that made others wait). Since 1.22 includes runtime-internal locks. |
| `threadcreate` | What created OS threads? | Spots blocking syscall or cgo explosions |
| CPU | Where is CPU spent, including spinning? | Look for `runtime.findRunnable`, `lock2`, `futex`, `procyield` |

```bash
go tool pprof -http=:8081 http://host:6060/debug/pprof/mutex
go tool pprof -http=:8081 'http://host:6060/debug/pprof/profile?seconds=30'   # CPU
go tool pprof -base before.pb.gz after.pb.gz       # diff goroutine profiles: what grew?
```

> 💡 **Insight:** For a mutex, the **block** profile shows the *waiters'* stacks ("I waited at Lock in handler X"), and the **mutex** profile shows the *holder's* stack at Unlock ("I made people wait from code Y"). You need both: the block profile tells you who suffers, the mutex profile tells you who is responsible.

### Execution tracer

#### Capturing

```go
// Programmatic, around a workload
f, _ := os.Create("trace.out")
_ = trace.Start(f)
runWorkload()
trace.Stop()
```

```bash
curl -o trace.out 'http://host:6060/debug/pprof/trace?seconds=5'   # from a live server
go test -trace=trace.out -run TestX                                  # from a test
go tool trace trace.out
```

#### Annotating your code

```go
ctx, task := trace.NewTask(ctx, "checkout")    // logical operation, can span goroutines
defer task.End()

trace.WithRegion(ctx, "price-quote", func() {   // time spans within a goroutine
	quote = pricing.Quote(ctx, cart)
})
trace.Log(ctx, "cart-size", strconv.Itoa(len(cart.Items)))
```

The trace UI then shows per-task latency distributions and lets you jump to the slow instances.

#### Flight recorder `[Go 1.25+]`: trace the incident that already happened

```go
fr := trace.NewFlightRecorder(trace.FlightRecorderConfig{
	MinAge:   10 * time.Second, // keep at least the last ~10s
	MaxBytes: 64 << 20,         // cap memory (takes precedence over MinAge)
})
if err := fr.Start(); err != nil {
	log.Fatal(err)
}

// When something interesting happens (slow request, SLO breach), snapshot it:
func onSlowRequest() {
	if !snapshotting.CompareAndSwap(false, true) {
		return // one snapshot at a time
	}
	defer snapshotting.Store(false)
	f, err := os.Create(fmt.Sprintf("/tmp/slow-%d.trace", time.Now().Unix()))
	if err != nil {
		return
	}
	defer f.Close()
	fr.WriteTo(f) // the last ~10s, including the slow request
}
```

This is the best way to debug rare latency outliers in production. You no longer have to be capturing a trace at the moment of the incident.

#### What to look for in a trace

| View | Look for |
|---|---|
| Per-P timeline | Gaps (idle Ps while work is queued → contention or GOMAXPROCS issue), dense tiny slices (too-fine work) |
| Goroutine analysis | Time split: execution / sync block / syscall / scheduler wait / GC assist |
| Scheduler latency profile | Long runnable→running waits → CPU saturation |
| Synchronization blocking profile | Which channel or mutex sites block the most |
| GC events | Mark assist on request goroutines, STW durations |

### Production metrics (`runtime/metrics`)

| Metric | Signal |
|---|---|
| `/sched/goroutines:goroutines` | Leaks (growth), load |
| `/sched/latencies:seconds` | CPU saturation, scheduler pressure |
| `/sync/mutex/wait/total:seconds` | Cumulative mutex wait (rate = contention) |
| `/sched/gomaxprocs:threads` | Effective parallelism (watch for container changes) |
| `/cpu/classes/gc/mark/assist:cpu-seconds` | GC-induced latency |
| `/sched/pauses/total/gc:seconds` | STW pauses |

### IRL Scenario: the 1-in-10,000 slow request

**Context:** p99.99 latency was 800 ms against a 20 ms p99. Logs showed nothing, and CPU profiles were dominated by normal work.

**Approach:** A flight recorder, plus a middleware that snapshots when a request exceeds 300 ms. Within an hour they had 6 traces. All showed the slow request's goroutine **runnable but not running** for ~700 ms, while every P was busy running goroutines from a cron-like "recompute all tenant quotas" job that fanned out 5,000 goroutines every 10 minutes.

**Fix:** Bound the recompute job's parallelism to 2 and spread it over the interval. p99.99 dropped to 40 ms. Without the flight recorder, this would have been almost impossible to catch.

### Exercises

1. ⭐ Run a program with a known race 10 times with and without `-race`. How often does each version show the bug?
2. ⭐⭐ Create a deliberate lock convoy (100 goroutines, one mutex, 50 µs critical section). Capture block and mutex profiles. Explain the difference in their stacks.
3. ⭐⭐⭐ Add a flight recorder to an HTTP service with a snapshot-on-slow-request middleware. Inject an occasional 500 ms CPU hog goroutine, then find it in the snapshot.

<details>
<summary>Solutions</summary>

1. Without `-race`, the bug manifests rarely or never (it depends on timing). With `-race`, it's reported every run on which the racy accesses execute, regardless of whether a bad interleaving actually happened. That is the strength of happens-before detection.
2. Block profile: the stacks are at `Lock()` call sites in the waiters. Mutex profile: the stacks are at `Unlock()` in the holder, i.e. the critical section that caused the delay. If they differ (e.g., a slow `Flush` holds the lock that `Get` waits on), the mutex profile points you to the code to fix.
3. Async preemption (Chapter 6) slices a lone hog into ~10 ms runs, so it shows up as one goroutine repeatedly occupying a P in ~10 ms chunks, not one solid 500 ms block. Request goroutines on that P's queue get turns between slices (or are stolen by idle Ps). Visible request latency appears when several hogs saturate all Ps. Inject `GOMAXPROCS` hogs to make the effect obvious in the snapshot.

</details>

### Review Checklist

- [ ] Is `-race` on in CI for all tests, with real concurrency in those tests?
- [ ] Are mutex and block profiling enabled at a production-safe rate?
- [ ] Is `net/http/pprof` reachable *internally* and never publicly?
- [ ] Are runtime/metrics exported (goroutines, sched latency, mutex wait, GC assist)?
- [ ] Is there a flight recorder (or a way to capture a trace on demand) for latency outliers?

---

## Chapter 27: Testing Concurrent Code

### TL;DR

- **`testing/synctest`** `[Go 1.25+]` runs a test in a **bubble** with a **fake clock**: time advances instantly when every goroutine in the bubble is durably blocked. Timeouts, retries, TTLs and tickers become fast and **deterministic**.
- `synctest.Wait()` blocks until every other goroutine in the bubble is durably blocked, so you can assert state at a quiescent point without sleeping.
- **Durably blocking:** channel ops on channels created in the bubble, a `select` where **every** case is a bubble channel, `sync.Cond.Wait`, `WaitGroup.Wait` (Add called in the bubble), `time.Sleep`. **Not durably blocking:** mutex locks, I/O, syscalls.
- Beyond synctest: stress (`-count`, `stress`, `-cpu`), leak checks (goleak), deadlock detection via `-timeout`, and linearizability checking (e.g., `porcupine`) for concurrent data structures.

### Practical Code

#### Before synctest: slow and flaky

```go
func TestCacheExpiry_Old(t *testing.T) {
	c := NewTTLCache(100 * time.Millisecond)
	c.Set("k", "v")
	time.Sleep(150 * time.Millisecond) // slow, and flaky on loaded CI
	if _, ok := c.Get("k"); ok {
		t.Fatal("expected expiry")
	}
}
```

#### With synctest: instant and deterministic

```go
func TestCacheExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewTTLCache(time.Hour) // the janitor goroutine starts inside the bubble
		defer c.Close()

		c.Set("k", "v")
		time.Sleep(59 * time.Minute) // fake clock: returns instantly
		if _, ok := c.Get("k"); !ok {
			t.Fatal("expired too early")
		}
		time.Sleep(2 * time.Minute)
		synctest.Wait() // let the janitor finish its pass
		if _, ok := c.Get("k"); ok {
			t.Fatal("expected expiry")
		}
	})
}
```

#### Testing a context timeout path

```go
func TestFetchTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		slow := make(chan struct{}) // bubble channel: blocking on it is durable
		svc := &Service{backend: func(ctx context.Context) error {
			select {
			case <-slow:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		start := time.Now()
		err := svc.Fetch(ctx)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("got %v, want deadline exceeded", err)
		}
		if got := time.Since(start); got != 2*time.Second { // exact: the fake clock is deterministic
			t.Fatalf("took %v", got)
		}
	})
}
```

#### Testing "nothing happens until X"

```go
func TestDebounce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		events := make(chan struct{})
		var calls atomic.Int32
		go Debounce(ctx, events, 100*time.Millisecond, func() { calls.Add(1) })

		for range 5 {
			events <- struct{}{}
			time.Sleep(50 * time.Millisecond) // within the debounce window
		}
		synctest.Wait()
		if calls.Load() != 0 {
			t.Fatal("fired during burst")
		}
		time.Sleep(100 * time.Millisecond)
		synctest.Wait()
		if calls.Load() != 1 {
			t.Fatalf("calls=%d, want 1", calls.Load())
		}
	})
}
```

#### synctest limitations

- **Real I/O isn't durable blocking.** Use in-memory fakes (`net.Pipe` is fine, since it's built on channels within the bubble).
- **Mutex waits aren't durable.** A goroutine blocked on a `Mutex` held by a bubble goroutine is assumed to be about to wake. That's fine for short critical sections and wrong if you hold locks across waits.
- Channels, timers and tickers created **inside** a bubble panic if used from outside it. Ones created outside *can* be used inside, but blocking on them isn't durable, so fake time won't advance. Construct everything inside `synctest.Test`.
- A **package-level** `var wg sync.WaitGroup` can't be associated with a bubble (a `*WaitGroup` can).
- If every goroutine is durably blocked and no timer can unblock them (or the test function has returned while bubble goroutines are still blocked, e.g. on a ticker that was never stopped), `Test` panics with a deadlock report. That is a feature: it reports deadlocks.

### Other techniques

#### Stress testing

```bash
go test -race -count=1000 -run TestQueue ./queue
go test -c -race -o queue.test ./queue && stress -p 16 ./queue.test -test.run TestQueue
go test -race -cpu=1,2,4,16 ./...
```

Design tests to **maximise contention**: release all goroutines at once with a start channel and use tiny critical sections in tight loops.

```go
func TestCounterConcurrent(t *testing.T) {
	var c Counter
	start := make(chan struct{})
	var wg sync.WaitGroup
	const G, N = 64, 1000
	for range G {
		wg.Go(func() {
			<-start // everyone starts together
			for range N {
				c.Inc()
			}
		})
	}
	close(start)
	wg.Wait()
	if got := c.Value(); got != G*N {
		t.Fatalf("got %d, want %d", got, G*N)
	}
}
```

#### Leak checking

```go
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m) // fails the package if goroutines remain after tests
}

func TestWorker(t *testing.T) {
	defer goleak.VerifyNone(t) // per-test check
	// ...
}
```

#### Deadlock detection

`go test -timeout 30s` panics with **all goroutine stacks** on timeout. For a suspected deadlock, run with a short timeout and read the dump (Chapter 25).

#### Linearizability checking

For concurrent data structures (queues, KV stores), record a history of `(call time, return time, op, result)` from many concurrent clients, then check with a linearizability checker (e.g., `github.com/anishathalye/porcupine`) that some sequential ordering, consistent with real-time order, explains all the results. This finds logical race conditions that `-race` can't.

#### Injecting interleavings

When a specific interleaving must be tested, add **test hooks**:

```go
var testHookBeforeCommit func() // nil in production

func (s *Store) Commit() {
	// ... prepare ...
	if testHookBeforeCommit != nil {
		testHookBeforeCommit() // test blocks here to force a concurrent op in between
	}
	// ... commit ...
}
```

The standard library uses this technique heavily (`net/http`).

### Pitfalls

> ⚠️ **`time.Sleep` to "let goroutines run"** in tests: slow and flaky. Use synctest, channels, or explicit synchronisation points.

> ⚠️ **Tests that only run one goroutine** and pass `-race` trivially. Make sure concurrent code is exercised concurrently.

> ⚠️ **`t.Fatal` from a non-test goroutine** is not allowed (it calls `runtime.Goexit` on the wrong goroutine). Use `t.Error` and return, or send errors back to the test goroutine.

> 🤖 **AI note:** AI-written tests for concurrent code overwhelmingly use `time.Sleep` for synchronisation and rarely assert on leaks. Ask for synctest-based tests explicitly.

### Exercises

1. ⭐ Convert a retry-with-backoff test that sleeps for real into a synctest test that asserts the exact backoff schedule using `time.Since(start)`.
2. ⭐⭐ Write a synctest test showing that a rate limiter allows exactly N events in a 1 s window.
3. ⭐⭐⭐ Build a tiny linearizability checker for a single register (read/write) and use it to find a bug in a deliberately broken "cached register" implementation.

<details>
<summary>Solutions</summary>

1. Inside `synctest.Test`, record `time.Since(start)` at each attempt in a slice, then assert it equals `[0, 100ms, 300ms, 700ms]` exactly. Make jitter injectable (seeded or disabled) for the test.
2. Run a goroutine that calls `lim.Allow()` every 1 ms (`time.Sleep(time.Millisecond)`) for 1 s of fake time, counting successes. Assert `== burst + rate×1s` (within the bucket math).
3. Brute force: enumerate permutations of the operations consistent with real-time precedence (if A returned before B started, A comes first), and check sequential register semantics. Exponential, but fine for histories of ~10 ops.

</details>

### Review Checklist

- [ ] Are time-dependent tests written with synctest (or an injected clock), with no real sleeps?
- [ ] Do concurrency tests maximise contention (start barrier, many goroutines)?
- [ ] Is goleak in `TestMain` for packages that start goroutines?
- [ ] Does CI run `-race` and a `-count` stress pass for concurrency-heavy packages?
- [ ] Are tricky interleavings covered by test hooks?

---

[← Part VI](06-patterns.md) · [Index](README.md) · [Next: Part VIII, Performance & Production →](08-performance-and-production.md)
