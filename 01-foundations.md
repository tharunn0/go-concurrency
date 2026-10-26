# Part I: Foundations of Concurrent Thinking

[← Index](README.md) · [Next: Part II, The Runtime →](02-runtime.md)

- [Chapter 1: Concurrency vs Parallelism, and the Laws of Scaling](#chapter-1-concurrency-vs-parallelism-and-the-laws-of-scaling)
- [Chapter 2: CSP, Actors and Shared Memory](#chapter-2-csp-actors-and-shared-memory)
- [Chapter 3: Hardware Reality](#chapter-3-hardware-reality)
- [Chapter 4: The Go Memory Model](#chapter-4-the-go-memory-model)

---

## Chapter 1: Concurrency vs Parallelism, and the Laws of Scaling

### TL;DR

- **Concurrency** is about *structure*: independently executing tasks composed together. **Parallelism** is about *execution*: tasks running at the same instant on different cores.
- Adding goroutines does not add throughput. Throughput is limited by the serial fraction (Amdahl), by contention, and by coherency cost (USL).
- **Little's Law** (`L = λ·W`) is the most useful equation for sizing worker pools, queues and connection pools.
- Past a certain point, *more parallelism makes things slower*. Measure where that point is; don't guess.

### Mental Model

```
Concurrency (structure)          Parallelism (execution)
┌──────────────────────┐         core0: ███ A ███ ███ A ███
│ A ─┐                 │         core1: ███ B ███ ███ C ███
│ B ─┼─► composed      │
│ C ─┘   program       │         Concurrency on 1 core:
└──────────────────────┘         core0: A B A C B A C ...
```

A concurrent program is *correct regardless of how many cores run it*. Parallelism is an optimisation the runtime may or may not give you. If your program is only correct with `GOMAXPROCS=1`, it has a bug.

### The three laws you need

#### Amdahl's Law: the ceiling of a fixed workload

If a fraction `p` of the work can be parallelised across `N` workers:

```
Speedup(N) = 1 / ((1 − p) + p/N)
```

| Parallel fraction `p` | N = 8 | N = 64 | N → ∞ |
|---|---|---|---|
| 50% | 1.78× | 1.97× | 2× |
| 90% | 4.7× | 8.8× | 10× |
| 99% | 7.5× | 39× | 100× |

> 💡 **Insight:** A 5% serial section (one mutex around a shared map, one single-threaded aggregator goroutine) caps you at 20× no matter how many cores you buy. Whenever you look at a parallel design, find the serial part first.

#### Gustafson's Law: the optimistic counterpart

Amdahl assumes a fixed problem size. In practice, more cores usually mean *bigger problems*: more requests, more data. If the serial part stays constant while the parallel part grows with N, speedup grows roughly linearly. That is why servers scale well: each request is mostly independent.

#### Universal Scalability Law (USL): why it gets *worse*

Neil Gunther's USL adds a **coherency** term to Amdahl:

```
X(N) = λN / (1 + σ(N − 1) + κN(N − 1))
         σ = contention (queueing for shared resources)
         κ = coherency (cost of keeping shared state consistent: cache-line ping-pong, crosstalk)
```

Because the `κ` term is quadratic, throughput has a **peak** and then *declines* as N grows. Typical Go symptoms of a high κ:

- Many goroutines doing atomic adds on the same counter.
- A heavily contended `sync.Mutex` (the cache line holding its state bounces between cores).
- `RWMutex` with lots of readers on many cores (readers all write to `readerCount`!).

```
throughput
   │        ┌──── peak (N*)
   │      ╱‾‾‾╲
   │    ╱       ╲___  ← coherency dominates
   │  ╱
   │╱
   └────────────────── N (workers / cores)
```

#### Little's Law: sizing anything with a queue

```
L = λ · W
L = average number of items in the system (in-flight requests, busy workers)
λ = average arrival rate
W = average time each item spends in the system
```

**Example:** a service receives 2,000 req/s and each request makes a DB call that takes 15 ms on average. In-flight DB calls = 2000 × 0.015 = **30**. A connection pool of 10 will queue; a pool of 200 is mostly idle (and may overload the DB). Size it at ~30 plus headroom for variance (e.g., 40–50), then validate with load tests.

> 💡 **Insight:** Little's Law holds for *any* stable system regardless of the distribution. That makes it your sanity check for every worker pool, semaphore and buffer size in this course.

### Practical Code: watching Amdahl and USL happen

```go
package main

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const total = 50_000_000

// Shared counter: every increment contends on one cache line (high κ).
func sharedCounter(workers int) time.Duration {
	var n atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < total/workers; i++ {
				n.Add(1)
			}
		}()
	}
	wg.Wait()
	return time.Since(start)
}

// Partitioned: each worker owns its partial sum; one serial merge at the end.
func partitioned(workers int) time.Duration {
	partials := make([]int64, workers)
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			var local int64 // register / stack: no sharing at all
			for i := 0; i < total/workers; i++ {
				local++
			}
			partials[w] = local // one write per worker
		}(w)
	}
	wg.Wait()
	var sum int64
	for _, p := range partials {
		sum += p
	}
	_ = sum
	return time.Since(start)
}

func main() {
	for _, w := range []int{1, 2, 4, 8, runtime.NumCPU()} {
		fmt.Printf("workers=%-3d shared=%-12v partitioned=%v\n",
			w, sharedCounter(w), partitioned(w))
	}
}
```

You will typically see the shared version get **slower** as you add workers, while the partitioned version scales almost linearly. The algorithm is the same in both. The only difference is the sharing pattern.

### Pitfalls

> ⚠️ **"Make it concurrent to make it fast."** For CPU-bound work on small inputs, goroutine creation, scheduling and the final merge often cost more than they save. Parallelise when per-item work is large compared with ~1 µs of coordination overhead, or when the work is I/O-bound.

> ⚠️ **Unbounded fan-out.** `for _, x := range items { go handle(x) }` with 1M items creates 1M goroutines (several GB of stacks in the worst case) and hammers whatever `handle` talks to. Concurrency needs a *bound* (Chapter 19).

### IRL Scenario: "We doubled the workers and throughput dropped 30%"

**Context:** A log-ingestion service parses JSON lines and updates in-memory aggregates (`map[string]*Stats` guarded by one `sync.Mutex`). They moved from 16 to 32 workers on a 32-core box, and throughput went *down*.

**Diagnosis:**
1. CPU profile: lots of time in `sync.(*Mutex).Lock` → `runtime.lock2`/`futex`.
2. Mutex profile (`runtime.SetMutexProfileFraction(5)`): 70% of contention at the aggregate update.
3. That is the USL in action: σ (queueing on the mutex) and κ (cache-line transfers of the mutex state and the map buckets) grow with N.

**Fix:**
- Each worker keeps a **local** `map[string]Stats` and flushes it to the global aggregate every 1 s, or every 10k lines (batching turns N tiny critical sections into a few big ones).
- Alternatively, shard the global map into 64 stripes by key hash (Chapter 28).
- Result: near-linear scaling to 32 workers.

**Lesson:** Find the shared write. Make it local, batch it, or shard it.

### Exercises

1. ⭐ Using Little's Law: a worker pool processes jobs that take 200 ms on average and arrive at 150/s. What is the minimum pool size that avoids unbounded queue growth?
2. ⭐⭐ Run the code above with `GOMAXPROCS=1`, then `=2`, `=NumCPU`. Explain the shared-counter numbers at `GOMAXPROCS=1`.
3. ⭐⭐⭐ Measure throughput of the shared-counter version at N = 1..2×NumCPU, fit the USL (σ, κ) using any curve-fitting tool, and predict N*.

<details>
<summary>Solutions</summary>

1. `L = 150 × 0.2 = 30` workers. That is the *average* number busy. With exactly 30, any variance builds a queue that never fully drains (utilisation ρ = 1). Size for ρ ≈ 0.7–0.8 → ~38–43 workers.
2. With `GOMAXPROCS=1`, goroutines are multiplexed on one P. Atomics are uncontended (only one core touches the line), so the shared version is about as fast as its single-threaded equivalent. The slowdown with more Ps is pure coherency cost, which is the hardware's doing and not the Go scheduler's.
3. USL peak: `N* = sqrt((1 − σ) / κ)`. Typical atomic-counter runs give a κ large enough that N* ≈ 1–4. That should change how you see "lock-free = scalable": it is lock-free, but it does not scale.

</details>

### Review Checklist

- [ ] What is the serial fraction of this design? Where is it?
- [ ] Is there a shared write on the hot path? Can it be made local, batched or sharded?
- [ ] Is every concurrency bound justified with Little's Law or a measurement?
- [ ] Has scaling been measured at 1, N/2, N, 2N workers, rather than just at "a lot"?

---

## Chapter 2: CSP, Actors and Shared Memory

### TL;DR

- Go supports **both** CSP-style message passing (channels) and shared memory (`sync`, `sync/atomic`). Neither is "the Go way" exclusively.
- *"Don't communicate by sharing memory; share memory by communicating"* is about **ownership transfer**, not a ban on mutexes.
- Use **channels** to transfer ownership, signal events and coordinate lifecycles. Use **mutexes** to protect state that has no natural single owner. Use **atomics** for simple independent counters and flags.
- Go channels are not actor mailboxes. Channels are first-class and anonymous, sends can block, and there is no built-in supervision.

### Mental Model

| Model | Unit of state | Communication | Synchronisation | Go mapping |
|---|---|---|---|---|
| **Shared memory** | Shared variables | Read/write memory | Locks, atomics | `sync.Mutex`, `atomic.*` |
| **CSP** (Hoare, 1978) | Process-local | Channels (anonymous, typed) | Rendezvous on channel ops | goroutines + channels |
| **Actor** (Hewitt, 1973; Erlang) | Actor-private | Async messages to *named* mailbox | Mailbox ordering | goroutine + inbox channel (by convention) |

#### What "share memory by communicating" actually means

```go
// Ownership transfer: after the send, the producer must not touch buf.
buf := make([]byte, 0, 4096)
buf = fill(buf)
out <- buf // ownership moves to the receiver
// buf must be treated as gone from here on
```

The channel send is a **happens-before edge** (Chapter 4) *and* a social contract about who may touch the data. The receiver can then use `buf` without locks because exactly one goroutine owns it at a time.

> 💡 **Insight:** The real value of channels is not "no locks". It is that **ownership becomes visible in the code**. Every send is an explicit handoff a reviewer can see. With a mutex, the protected state's ownership lives in comments and discipline.

### Under the Hood: channels *are* shared memory

A channel is a heap-allocated struct with a mutex inside (`runtime.hchan`, Chapter 10). Every blocking or value-transferring channel operation locks it (only non-blocking fast paths and `len`/`cap` skip the lock). Channels are therefore *not* faster than mutexes. They are a higher-level abstraction built on one. Rough uncontended costs on modern x86:

| Operation | Cost (order of magnitude) |
|---|---|
| `atomic.Int64.Add` (uncontended) | ~1–5 ns |
| `sync.Mutex` Lock+Unlock (uncontended) | ~10–20 ns |
| Buffered channel send+recv (no blocking) | ~20–60 ns |
| Unbuffered channel handoff (goroutine switch) | ~100–300 ns |

These numbers vary a lot with hardware and contention. Measure your own.

### Practical Code: one problem, three models

A bank account that supports deposits, withdrawals (no overdraft) and balance reads.

**1. Shared memory (mutex):** simplest and fastest. Usually the right answer for "protect a struct".

```go
type Account struct {
	mu      sync.Mutex
	balance int64
}

func (a *Account) Deposit(n int64) {
	a.mu.Lock()
	a.balance += n
	a.mu.Unlock()
}

func (a *Account) Withdraw(n int64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.balance < n {
		return errors.New("insufficient funds")
	}
	a.balance -= n
	return nil
}

func (a *Account) Balance() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.balance
}
```

**2. Actor / confinement (goroutine owns state):** useful when the state machine is complex, operations must be serialized with side effects (I/O), or you want a natural place for timers.

```go
type op struct {
	kind  int // 0 deposit, 1 withdraw, 2 balance
	n     int64
	reply chan result
}
type result struct {
	balance int64
	err     error
}

type ActorAccount struct{ ops chan op }

func NewActorAccount(ctx context.Context) *ActorAccount {
	a := &ActorAccount{ops: make(chan op)}
	go a.loop(ctx)
	return a
}

func (a *ActorAccount) loop(ctx context.Context) {
	var balance int64 // confined: only this goroutine touches it
	for {
		select {
		case <-ctx.Done():
			return
		case o := <-a.ops:
			switch o.kind {
			case 0:
				balance += o.n
				o.reply <- result{balance: balance}
			case 1:
				if balance < o.n {
					o.reply <- result{balance: balance, err: errors.New("insufficient funds")}
					continue
				}
				balance -= o.n
				o.reply <- result{balance: balance}
			case 2:
				o.reply <- result{balance: balance}
			}
		}
	}
}

func (a *ActorAccount) do(ctx context.Context, o op) (result, error) {
	o.reply = make(chan result, 1) // buffered: the actor never blocks on a gone caller
	select {
	case a.ops <- o:
	case <-ctx.Done():
		return result{}, ctx.Err()
	}
	select {
	case r := <-o.reply:
		return r, nil
	case <-ctx.Done():
		return result{}, ctx.Err()
	}
}
```

Notice how much more code this is, and how many lifecycle decisions it forces on you (what if the actor's ctx is cancelled while callers wait? Callers must also select on something that closes when the actor exits). Choose this model when it earns its cost.

**3. Atomic (lock-free CAS loop):** good for a *single word* of state with simple invariants.

```go
type AtomicAccount struct{ balance atomic.Int64 }

func (a *AtomicAccount) Withdraw(n int64) error {
	for {
		cur := a.balance.Load()
		if cur < n {
			return errors.New("insufficient funds")
		}
		if a.balance.CompareAndSwap(cur, cur-n) {
			return nil
		}
		// lost the race; retry with fresh value
	}
}
```

The moment the invariant spans two words (e.g., "transfer between two accounts atomically"), atomics stop being enough and you need a lock or a single owner.

### Decision guide

```
Is the data handed off between stages (producer → consumer)?   → channel (ownership transfer)
Is it an event/signal ("done", "ready", "stop")?               → close a channel / context
Is it a struct many goroutines read/modify in place?           → sync.Mutex (or RWMutex if read-heavy, measured)
Is it a single counter/flag/pointer with no cross-field rule?  → sync/atomic
Is it a complex state machine with timers and I/O?             → owning goroutine (actor)
```

### Pitfalls

> ⚠️ **Channels as locks.** `sem := make(chan struct{}, 1)` as a mutex works, but it is slower and gives up the mutex profile and race-detector clarity. Use it only when you need to `select` on acquisition (e.g., lock-with-timeout).

> ⚠️ **Actor without backpressure or lifecycle.** An actor goroutine that callers can block on forever (because it exited) is a goroutine-leak factory. Every request path must select on the actor's "done" signal or the caller's ctx.

> 🤖 **AI note:** Assistants often over-apply channels ("idiomatic Go uses channels") and produce actors for what should be a 10-line mutex-protected struct. The opposite also happens: a mutex wrapped around a section that does network I/O.

### IRL Scenario: the session store that became an actor

**Context:** A team built a session store as an actor (one goroutine, `map` confined inside, requests via channel) "to avoid locks". At 40k req/s, p99 latency was 80 ms.

**Diagnosis:** A single goroutine serializes *all* session operations, so the store is one queue with a single server. `go tool trace` showed requests queued on the `ops` channel. Amdahl: 100% serial.

**Fix:** A sharded map with 256 `sync.RWMutex`-protected shards. p99 dropped to 2 ms. The actor model was the wrong tool for a high-throughput key-value lookup with no complex per-key state.

**When the actor *was* right:** In the same system, the per-tenant "rate-limit + quota refresh + periodic flush to DB" logic stayed an actor per tenant. The state machine had timers and I/O, the per-tenant rate was low, and confinement made it easy to reason about.

### Exercises

1. ⭐ Add a `Transfer(to *Account, n int64)` to the mutex version. What deadlock risk appears, and how do you avoid it?
2. ⭐⭐ The actor version above has a leak when the actor's ctx is cancelled before a caller's ctx. Fix it.
3. ⭐⭐⭐ Benchmark the three versions with `b.RunParallel` at varied `GOMAXPROCS`. Explain the results using USL terms.

<details>
<summary>Solutions</summary>

1. Two concurrent transfers A→B and B→A, each locking `from` then `to`, can deadlock. Fix: **lock ordering**. Give each account a unique ID and always lock the lower ID first (and handle `from == to`).
   ```go
   func Transfer(from, to *Account, n int64) error {
       if from == to { return nil }
       first, second := from, to
       if to.id < from.id { first, second = to, from }
       first.mu.Lock(); defer first.mu.Unlock()
       second.mu.Lock(); defer second.mu.Unlock()
       if from.balance < n { return errors.New("insufficient funds") }
       from.balance -= n; to.balance += n
       return nil
   }
   ```
2. Add a `done chan struct{}` closed by `loop` when it returns (via `defer close(a.done)`), and include `case <-a.done: return result{}, ErrClosed` in **both** selects in `do`.
3. Typically atomic < mutex < actor at low contention. Under high contention atomic CAS loops degrade (retries plus coherency, high κ), the mutex enters starvation mode (Chapter 12), and the actor is capped at single-goroutine throughput (σ ≈ 1).

</details>

### Review Checklist

- [ ] Who owns each piece of mutable state? Is that written down?
- [ ] Do channels in the design transfer ownership or signal? If neither, why a channel?
- [ ] Does any actor goroutine have a documented lifecycle and a "done" signal callers can see?
- [ ] Are any cross-field invariants being "protected" by atomics? (They cannot be.)

---

## Chapter 3: Hardware Reality

### TL;DR

- Memory is not a flat array. It is a **hierarchy of caches** moved in **64-byte cache lines** (128 bytes effectively on some CPUs, such as Apple M-series and Intel with adjacent-line prefetch).
- Cores keep caches coherent with protocols like **MESI**. Writing to a line shared by another core costs an invalidation round-trip.
- **Both the CPU and the compiler reorder memory operations.** Without synchronisation, another goroutine may see your writes late, never, or in a different order.
- **False sharing**: independent variables on the same cache line slow each other down even though there is no logical sharing.

### Mental Model

```
┌────────── Core 0 ──────────┐   ┌────────── Core 1 ──────────┐
│ registers                   │   │ registers                   │
│ store buffer  ──┐           │   │ store buffer  ──┐           │
│ L1d (~32-64KB, ~1ns)        │   │ L1d                         │
│ L2  (~0.5-2MB, ~4ns)        │   │ L2                          │
└──────────────┬──────────────┘   └──────────────┬──────────────┘
               └──────── L3 (shared, ~10-40ns) ──┘
                              │
                        DRAM (~80-100ns)
```

Approximate latencies: L1 hit ~1 ns, L3 ~10–40 ns, DRAM ~80–100 ns, a cross-socket cache-line transfer 100+ ns. A contended cache line can cost about as much as a DRAM access **on every write**.

### Under the Hood

#### Cache coherence (MESI)

Each cache line in each core is in one of these states: **M**odified, **E**xclusive, **S**hared, **I**nvalid. To write, a core needs the line in M or E, which means invalidating every other core's copy. When two cores alternately write the same line, the line "ping-pongs" between them. This is the physical cause of the USL κ term.

#### Store buffers and reordering

A core does not wait for a store to reach the cache. It parks the store in a **store buffer** and keeps executing. Consequence: **a later load can complete before an earlier store becomes visible to other cores** (store→load reordering). x86 (TSO) allows *only* this reordering. ARM64 and POWER allow much more (load→load, store→store, ...).

The classic litmus test (Dekker / "store buffering"):

```
Initially x = y = 0
Core 0:  x = 1; r1 = y        Core 1:  y = 1; r2 = x
Can r1 == 0 && r2 == 0?   On x86: YES. On ARM: YES.
```

#### Compiler reordering

The Go compiler is free to reorder, hoist and eliminate memory accesses *as long as single-goroutine behaviour is preserved*. For example:

```go
for !done { }      // compiler may load `done` once and spin forever
```

That is why "it works on my x86 laptop" means nothing. The compiler alone can break unsynchronized code on any architecture.

> 💡 **Insight:** You never reason about hardware fences in Go. You reason about **happens-before** (Chapter 4). Synchronisation primitives (channels, mutexes, atomics) are where the toolchain inserts the right fences *and* stops compiler reordering. The hardware model tells you *why* the rules exist. The memory model tells you *what you can rely on*.

### Practical Code: false sharing, measured

```go
package falsesharing

import (
	"sync"
	"sync/atomic"
	"testing"
)

type Unpadded struct {
	a atomic.Int64
	b atomic.Int64 // same cache line as a
}

type Padded struct {
	a atomic.Int64
	_ [56]byte // 8 (a) + 56 = 64: b starts on the next line
	b atomic.Int64
	_ [56]byte
}

const iters = 10_000_000

func BenchmarkUnpadded(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var s Unpadded
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); for j := 0; j < iters; j++ { s.a.Add(1) } }()
		go func() { defer wg.Done(); for j := 0; j < iters; j++ { s.b.Add(1) } }()
		wg.Wait()
	}
}

func BenchmarkPadded(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var s Padded
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); for j := 0; j < iters; j++ { s.a.Add(1) } }()
		go func() { defer wg.Done(); for j := 0; j < iters; j++ { s.b.Add(1) } }()
		wg.Wait()
	}
}
```

Typical result: padded is **2–6× faster**, even though the two goroutines never touch each other's counter.

For portable padding, use `golang.org/x/sys/cpu.CacheLinePad`:

```go
import "golang.org/x/sys/cpu"

type shard struct {
	mu sync.Mutex
	m  map[string]int
	_  cpu.CacheLinePad
}
```

> 🔬 **Internals:** The Go runtime pads its own per-P structures, e.g. `sync.Pool`'s `poolLocal` is padded to 128 bytes for exactly this reason.

### Pitfalls

> ⚠️ **Padding everything.** Padding costs memory and cache capacity. Only pad *hot, independently-written* fields that a profile or benchmark shows are contended.

> ⚠️ **Arrays of per-worker counters.** `counts := make([]int64, workers)` where each worker increments `counts[w]` is a false-sharing machine: eight int64s per cache line. Keep a local variable and write the result once at the end (Chapter 1 code), or pad each element.

### IRL Scenario: metrics library on a 96-core box

**Context:** A homegrown metrics library used a `[]atomic.Int64` indexed by `runtime_procPin`-style shard IDs, which was "already sharded". On 96 cores, incrementing request counters took ~8% of CPU.

**Diagnosis:** `perf c2c` (cache-to-cache analysis on Linux) showed heavy HITM (hit-modified) events on the counter array. Eight shards shared each cache line, so the sharding barely helped.

**Fix:** Pad each shard to 128 bytes. Counter CPU dropped to <1%.

**Lesson:** Sharding without padding is often only half a fix.

### Exercises

1. ⭐ Why does the padded struct use 56 bytes and not 64?
2. ⭐⭐ Run the benchmark with `GOMAXPROCS=1`. Explain the result.
3. ⭐⭐⭐ Write the store-buffering litmus test in Go using plain variables and run it millions of times. Do you observe `r1 == r2 == 0`? Then rewrite it with `atomic.Int32`. What changes, and which rule from Chapter 4 explains it?

<details>
<summary>Solutions</summary>

1. `atomic.Int64` is 8 bytes. 8 + 56 = 64 puts `b` at offset 64. Note that this does not guarantee the struct *starts* on a line boundary. For strict isolation, pad before `a` too, or use `cpu.CacheLinePad` on both sides.
2. Both goroutines run on the same P (only one runs at a time), the line never bounces, and the difference vanishes. That shows the cost is coherency traffic, not instruction count.
3. With plain variables (a data race, so the program is invalid!) you will occasionally see `0, 0` on multi-core machines. With atomics, Go's atomics are **sequentially consistent**, so `0, 0` is impossible: in any total order of the four atomic ops, one of the stores precedes the other goroutine's load.

</details>

### Review Checklist

- [ ] Are hot counters or locks written by many cores sharing cache lines with each other?
- [ ] Is there a per-worker array where a local variable would do?
- [ ] Is padding justified by a benchmark or profile?
- [ ] Is correctness being argued from "x86 is strongly ordered"? (Invalid: the compiler reorders too.)

---

## Chapter 4: The Go Memory Model

> The most important chapter in this course. Everything else rests on it.

### TL;DR

- The Go memory model specifies when a read **is guaranteed to observe** a write by another goroutine. The answer is "when the write **happens before** the read", which is established *only* by synchronisation.
- **DRF-SC:** if your program has no data races, it behaves as if all goroutines' operations were interleaved on one core (sequential consistency). That is the deal: you remove races and Go gives you simple semantics.
- Go's atomics are **sequentially consistent**. There are no relaxed/acquire/release variants.
- A data race in Go is **not "benign"**. Races on multi-word values (interfaces, strings, slices) can tear, corrupting memory and crashing the program. Concurrent map operations are detected by the runtime and crash it outright.
- Since the 2022 revision (Go 1.19), the Go Memory Model document formally defines races and atomics and the synchronises-before relation.

### Mental Model

Think of each goroutine as a **timeline**. Synchronisation operations draw **arrows** between timelines. A read is guaranteed to see a write *only if there is a path of arrows* (plus program order within each goroutine) from the write to the read.

```
G1:  x = 42 ──► mu.Unlock() ─┐
                             │  synchronized-before
G2:                          └─► mu.Lock() ──► read x   ✅ sees 42

G1:  x = 42 ──► done = true
G2:  if done { read x }       ❌ no arrow: data race, no guarantee
```

### Under the Hood: the formal rules

**Definitions (simplified from the spec):**

- **Sequenced before:** program order within one goroutine.
- **Synchronized before:** an edge between synchronising operations in *different* goroutines, defined by the rules below.
- **Happens before** = transitive closure of sequenced-before ∪ synchronized-before.
- A **data race** is a write to a memory location happening concurrently with another read or write to that location, where at least one is not atomic and neither happens before the other.

**Synchronisation rules (memorise these):**

| # | Rule |
|---|---|
| 1 | **Initialization:** if package `p` imports `q`, `q`'s `init` completion is synchronized before any of `p`'s. All `init` completes before `main.main` starts. |
| 2 | **Goroutine creation:** the `go` statement is synchronized before the new goroutine starts executing. |
| 3 | **Goroutine exit:** is *not* synchronized before anything. You must use explicit sync to observe a goroutine's effects. |
| 4 | **Channel send:** a send is synchronized before the completion of the corresponding receive. |
| 5 | **Channel close:** close is synchronized before a receive that returns because the channel is closed. |
| 6 | **Unbuffered receive:** a receive from an unbuffered channel is synchronized before the completion of the corresponding send. |
| 7 | **Buffered capacity:** the *k*-th receive on a channel with capacity *C* is synchronized before the completion of the (*k+C*)-th send. (This is what makes a buffered channel a counting semaphore.) |
| 8 | **Mutex:** for `sync.Mutex`/`RWMutex` `l` and *n < m*, call *n* of `l.Unlock()` is synchronized before call *m* of `l.Lock()` returns. (RWMutex adds analogous rules for RLock/RUnlock.) |
| 9 | **Once:** the completion of the single call to `f()` in `once.Do(f)` is synchronized before the return of any `once.Do(f)`. |
| 10 | **Atomics:** if the effect of atomic op A is observed by atomic op B, A is synchronized before B. All atomic ops in a program behave as if executed in some sequentially consistent order. |
| 11 | **Finalizers:** `SetFinalizer(x, f)` is synchronized before the finalization call `f(x)`. |
| 12 | Other `sync` types (`WaitGroup`, `Cond`, `Map`, `Pool`) document their own guarantees (e.g., `wg.Done()` is synchronized before the `Wait` it unblocks returns). `context`'s `Done` is a channel close, so rule 5 applies. |

> 💡 **Insight on rule 6:** An unbuffered channel gives you a **two-way** edge: the sender knows the receiver has *started* receiving. A buffered channel only gives the sender→receiver direction. Code that relies on "after my send completes, the receiver has seen it" is only correct for unbuffered channels.

> 💡 **Insight on rule 3:** `go f(); time.Sleep(time.Second); read result` is a data race even though it "always works". Sleep is not synchronisation.

### Practical Code: correct and incorrect publication

**❌ Broken: flag busy-wait**

```go
var msg string
var done bool

func setup() {
	msg = "hello"
	done = true
}

func main() {
	go setup()
	for !done { // data race: may spin forever (hoisted load)
	}
	fmt.Println(msg) // may print "" even if the loop exits
}
```

Two independent failures: the compiler may hoist the `done` load out of the loop (infinite loop), and even if the loop exits, there is no happens-before from `msg = "hello"` to the read.

**✅ Fixed with a channel (preferred for one-shot events)**

```go
var msg string

func main() {
	done := make(chan struct{})
	go func() {
		msg = "hello"
		close(done) // rule 5
	}()
	<-done
	fmt.Println(msg) // guaranteed "hello"
}
```

**✅ Fixed with an atomic (when polling is genuinely needed)**

```go
var msg string
var done atomic.Bool

func setup() {
	msg = "hello"     // (a)
	done.Store(true)  // (b) a sequenced-before b
}

func main() {
	go setup()
	for !done.Load() { // (c) observes b ⇒ b synchronized-before c
		runtime.Gosched()
	}
	fmt.Println(msg) // (d) a → b → c → d: guaranteed "hello"
}
```

The atomic protects not just `done` but also **everything sequenced before the store**. This is the "publish" idiom.

**❌ Broken: double-checked locking**

```go
var instance *Config
var mu sync.Mutex

func Get() *Config {
	if instance == nil { // racy read, unsynchronized
		mu.Lock()
		if instance == nil {
			instance = load()
		}
		mu.Unlock()
	}
	return instance // may observe pointer before the pointee's fields are visible
}
```

A reader that sees `instance != nil` on the fast path has no happens-before edge to the writes *inside* `load()`. It can see a non-nil pointer to a partially initialised struct.

**✅ Fixed**

```go
var getConfig = sync.OnceValue(load) // [Go 1.21+]

func Get() *Config { return getConfig() }
```

Or, if the config must be hot-swappable:

```go
var current atomic.Pointer[Config]

func Get() *Config  { return current.Load() }
func Set(c *Config) { current.Store(c) } // c must be fully built before Store, and never mutated after
```

### Why races on multi-word values are catastrophic

A Go interface value is two words: `(type pointer, data pointer)`. A string is `(ptr, len)`. A slice is `(ptr, len, cap)`. Writes to these are **not atomic**. A racing reader can observe a **torn** value: half old, half new.

```go
package main

type Small struct{ a int }
type Big struct{ a, b, c, d [1 << 10]int }

func (s *Small) Get() int { return s.a }
func (b *Big) Get() int   { return b.d[len(b.d)-1] } // reads ~32KB past start

type Getter interface{ Get() int }

var sink int

func main() {
	var g Getter = &Small{}
	go func() {
		for {
			g = &Small{} // racy multi-word writes
			g = &Big{}
		}
	}()
	for {
		// may observe (type=*Big, data=*Small): reads far past the Small allocation
		sink += g.Get()
	}
}
```

Without `-race`, this can crash with a segfault or silently read garbage. **This is why the Go team says there are no benign data races.**

> 🔬 **Internals:** The 2022 memory model revision deliberately constrains the *compiler*: single-word races are treated more like Java's (a racy read of a single word returns some value that was actually written), but multi-word races may lead to "arbitrary memory corruption". In practice, assume any race is a crash waiting to happen.

### What the race detector can and cannot do

- It detects races **that actually occur in an execution**, using vector clocks over happens-before (Chapter 26). It does not find races in code paths you didn't run.
- It has **no false positives** in principle. If it reports a race, there is one (or you used `unsafe` in a way it cannot follow).
- It costs roughly 5–10× memory and 2–20× execution time. Run it in CI and in canaries rather than in all of production.

### Pitfalls

> ⚠️ **"I only read it, so it's fine."** A read racing with a write is still a race. Read-only data is safe only if *all writes happen before* the goroutines that read it start (rule 2) or are published via sync.

> ⚠️ **`sync.WaitGroup` as a publication mechanism, done wrong.** `wg.Done()` → `wg.Wait()` *is* an edge. But if a goroutine writes after calling `Done`, that write is unsynchronized.

> ⚠️ **Assuming `len(ch)` or `cap(ch)` synchronizes.** They are not synchronising operations, and `len(ch)` is stale the moment it returns.

> 🤖 **AI note:** Generated code frequently uses a plain `bool` "stopped" flag checked in a loop, or `time.Sleep` to "wait for the goroutine to start". Both are races. Ask: "where is the happens-before edge?"

### IRL Scenario: the config hot-reload crash

**Context:** A proxy reloads routing config on SIGHUP. The reload goroutine did `s.routes = newRoutes` (a `map[string]*Backend` field) while request goroutines read `s.routes[host]`. It "worked" for a year, then crashed in production with `fatal error: concurrent map read and map write`… except nobody was writing *to* the map.

**Diagnosis:** Reload *reused and mutated the old map* to build the next config, while request goroutines that had already loaded `s.routes` were still reading it. So someone was writing to the map after all, just not through the code path anyone looked at. On top of that, the racy field write gave readers no happens-before edge for the new map's contents. The runtime's map concurrency checker caught it. A worse outcome would have been silent corruption.

**Fix:**

```go
type Server struct {
	routes atomic.Pointer[map[string]*Backend]
}

// NewServer must Store an initial (possibly empty) map, or lookup dereferences nil.
func (s *Server) lookup(host string) *Backend {
	return (*s.routes.Load())[host]
}

func (s *Server) reload(cfg Config) {
	m := buildRoutes(cfg) // build a fresh map; never mutate after publishing
	s.routes.Store(&m)
}
```

This is the **copy-on-write + atomic pointer** pattern: readers are wait-free, writers build a complete new value, and immutability after publication removes the need for any further sync.

### Review Drill

```go
type Cache struct {
	mu   sync.RWMutex
	data map[string]string
	hits int
}

func (c *Cache) Get(k string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.data[k]
	if ok {
		c.hits++
	}
	return v, ok
}
```

<details>
<summary>What's wrong?</summary>

`c.hits++` is a **write** under a **read** lock. Multiple readers hold `RLock` concurrently, so `hits` is raced. Fix: make `hits` an `atomic.Int64`, or take the write lock (which kills read parallelism). This exact bug shows up constantly in human and AI-written code alike.

</details>

### Exercises

1. ⭐ For each of these, state whether there is a happens-before edge from A to B: (a) A: `ch <- 1` on a buffered chan; B: code after the corresponding `<-ch`. (b) A: code before `<-ch` on an unbuffered chan; B: code after the corresponding `ch <- 1` returns. (c) A: code before `wg.Done()`; B: code after `wg.Wait()`. (d) A: code in goroutine before it returns; B: code in main after `time.Sleep(time.Hour)`.
2. ⭐⭐ Explain, using rule 7, why `sem := make(chan struct{}, N)` with `sem <- struct{}{}` before work and `<-sem` after limits concurrency to N.
3. ⭐⭐⭐ Is this correct? Justify with the rules.
   ```go
   var a, b atomic.Int32
   go func() { a.Store(1); fmt.Println(b.Load()) }()
   go func() { b.Store(1); fmt.Println(a.Load()) }()
   ```
   Can both print 0?

<details>
<summary>Solutions</summary>

1. (a) Yes, rule 4. (b) Yes, rule 6: the receive is synchronized before the send completes. (c) Yes, documented `WaitGroup` guarantee. (d) **No**, rule 3. Sleep proves nothing.
2. The *k*-th receive (release) is synchronized before the (*k+N*)-th send (acquire) completes. So the (*k+N*)-th acquirer cannot proceed until the *k*-th holder released, which means at most N holders at any time.
3. No, both cannot print 0. Atomics are sequentially consistent: there is a total order of the four operations, consistent with program order. Whichever `Store` comes first in that order precedes the *other* goroutine's `Load`, so at least one load sees 1. (With plain ints this would be a race, and `0 0` is observable in practice.)

</details>

### Review Checklist

- [ ] For every piece of shared data: what is the happens-before edge from writer to reader?
- [ ] Is anything published by pointer before it is fully constructed? Is it mutated after publication?
- [ ] Are there `bool` flags, `time.Sleep`s, or `len(ch)` checks used as synchronisation?
- [ ] Do tests run with `-race` in CI, with enough concurrency to exercise the paths?
- [ ] Is anyone arguing a race is "benign"? (In Go, none are.)

---

[← Index](README.md) · [Next: Part II, The Runtime →](02-runtime.md)
