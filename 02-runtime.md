# Part II: The Runtime

[← Part I](01-foundations.md) · [Index](README.md) · [Next: Part III, Channels & select →](03-channels-and-select.md)

- [Chapter 5: The GMP Scheduler](#chapter-5-the-gmp-scheduler)
- [Chapter 6: Preemption](#chapter-6-preemption)
- [Chapter 7: Syscalls, the Netpoller and sysmon](#chapter-7-syscalls-the-netpoller-and-sysmon)
- [Chapter 8: Goroutine Lifecycle and Stacks](#chapter-8-goroutine-lifecycle-and-stacks)
- [Chapter 9: GC Interaction](#chapter-9-gc-interaction)

> 🔬 Everything in this part describes the *implementation* (as of Go 1.25/1.26), not the spec. It changes between releases. Learn it to build intuition and to read traces and profiles, never to justify correctness.

---

## Chapter 5: The GMP Scheduler

### TL;DR

- **G** = goroutine, **M** = OS thread ("machine"), **P** = processor: a scheduling context and the *right to run Go code*. `GOMAXPROCS` = number of Ps.
- Each P has a **local run queue** (256 slots) plus a **`runnext`** slot. There is also a **global run queue**.
- Idle Ps **steal half** of another P's local queue. That is how load balances without a central lock.
- An M must hold a P to run Go code. Ms in syscalls or cgo can exist *without* Ps, so thread count can exceed `GOMAXPROCS` by a lot.
- The scheduler is optimised for throughput and locality, not fairness or priorities. There are no goroutine priorities.

### Mental Model

```
              Global Run Queue  [G G G ...]   (lock-protected)
                      ▲    │
                      │    ▼  (checked every 61 schedticks, and when local is empty)
 ┌────────── P0 ──────────┐   ┌────────── P1 ──────────┐   ┌──── P2 (idle) ────┐
 │ runnext: [G7]          │   │ runnext: [ ]           │   │                    │
 │ local:  [G1 G2 G3 ...] │◄──┼── steal half ──────────┼───┤ looking for work   │
 │ mcache, timers, ...    │   │ local: [G9]            │   │ (spinning M)       │
 └──────────┬─────────────┘   └──────────┬─────────────┘   └────────────────────┘
            │ runs on                     │ runs on
          M0 (thread)                   M1 (thread)          M2 (spinning)   M3 (in syscall, no P)
```

### Under the Hood

#### The scheduling loop (`runtime.schedule` → `findRunnable`)

When an M needs a goroutine to run, it roughly does this (from `runtime/proc.go`):

1. **Fairness check:** every 61st scheduling tick, take one G from the global queue first, so the global queue can't be starved by busy local queues.
2. **Local queue:** take `runnext` if set, else pop from the local queue.
3. **Global queue:** grab a batch (and move some to the local queue).
4. **Netpoll (non-blocking):** any goroutines whose network I/O became ready?
5. **Steal:** try up to 4 rounds over other Ps in random order, stealing half of a victim's local queue (and possibly its `runnext`), plus ready timers.
6. **Nothing found:** release the P to the idle list and park the M (futex sleep). Before that, a blocking netpoll may be done by one M.

#### `runnext`: locality for producer/consumer pairs

When a goroutine readies another (e.g., an unbuffered channel send wakes a receiver, or `go f()`), the woken G goes into the current P's **`runnext`** slot. It runs *next* on the same P and **inherits the remaining time slice**. A ping-pong pair of goroutines therefore stays on one P with hot caches, which is excellent for latency. It is also why a tight channel ping-pong benchmark doesn't scale with cores.

#### Spinning Ms

When work appears (new G, ready netpoll) and there are idle Ps, the runtime wakes an M to **spin**, i.e. actively look for work before sleeping. To bound wasted CPU, it allows at most about half as many spinning Ms as busy Ps (`2*nmspinning < gomaxprocs - npidle`). Spinning trades a little CPU for much lower wake-up latency than a futex sleep/wake cycle.

> 💡 **Insight:** Spinning is why an idle-ish Go service shows *some* CPU usage in `top`, and why very bursty workloads with many tiny goroutines can burn CPU in `runtime.findRunnable`/`runtime.stealWork`. If a CPU profile shows heavy `findRunnable`, you are creating too-fine-grained work.

#### Goroutine creation

`go f()` → `runtime.newproc`: allocate (or reuse from a per-P free list) a `g`, set up a small stack, put it in `runnext` (kicking the old `runnext` to the local queue tail), and wake a P if there is idle capacity. Cost: **~250 ns to 1 µs** including the first schedule, and ~2–8 KB of memory.

#### `runtime.LockOSThread`

Wires the goroutine to its current M (the M runs only that G until unlocked). It is needed for thread-local OS state: some C libraries, OpenGL, Linux namespaces (`setns`), `setuid` per thread. If a goroutine exits while locked, the thread is **terminated** rather than reused, which is useful for cleaning up tainted thread state.

### Practical Code: observing the scheduler

**schedtrace:**

```bash
GODEBUG=schedtrace=1000 ./server
# SCHED 1004ms: gomaxprocs=8 idleprocs=6 threads=14 spinningthreads=1 needspinning=0
#               idlethreads=5 runqueue=0 [ 0 3 0 0 1 0 0 0 ] schedticks=[ ... ]
#   runqueue = global queue length; [...] = each P's local queue length
```

Add `scheddetail=1` to dump every G, M and P. That is heavy, but useful for "what is every goroutine doing right now?" in a staging box.

**Execution trace** (best tool by far):

```go
f, _ := os.Create("trace.out")
trace.Start(f)
defer trace.Stop()
// ... workload ...
```

```bash
go tool trace trace.out
# Look at: "Goroutine analysis", "Scheduler latency profile",
# the per-P timeline (gaps = idle Ps; dense short slices = too-fine work)
```

**Measuring scheduler latency directly:** the `/sched/latencies:seconds` metric in `runtime/metrics` is a histogram of time goroutines spent runnable before running.

```go
import "runtime/metrics"

func schedLatencyP99() float64 {
	s := []metrics.Sample{{Name: "/sched/latencies:seconds"}}
	metrics.Read(s)
	h := s[0].Value.Float64Histogram()
	var total uint64
	for _, c := range h.Counts {
		total += c
	}
	if total == 0 {
		return 0
	}
	target := uint64(float64(total) * 0.99)
	var acc uint64
	for i, c := range h.Counts {
		acc += c
		if acc >= target {
			return h.Buckets[i+1] // upper bound of the bucket
		}
	}
	return 0
}
```

Export this to your metrics system. A rising scheduler-latency p99 is an early signal of CPU saturation, `GOMAXPROCS` misconfiguration, or GC assist pressure.

### Pitfalls

> ⚠️ **Assuming FIFO or fairness between goroutines.** Run order is *not* creation order and is not fair. `runnext` deliberately lets a newly readied G jump the queue. Never encode ordering assumptions in goroutine start order.

> ⚠️ **Treating `runtime.Gosched()` as a fix.** It yields the P but puts the G on the *global* queue. It hides races and spin-loops rather than fixing them. Legitimate uses are very rare.

> ⚠️ **Millions of micro-goroutines for CPU-bound work.** Spawning a goroutine per array element makes the scheduler, not your work, the hot path. Chunk the work: roughly `GOMAXPROCS × 4` chunks is a good starting point.

### IRL Scenario: p99 latency doubled after adding a background job

**Context:** An API service (GOMAXPROCS=8) added an in-process report generator that spawns 8 CPU-bound goroutines every minute for ~10 s. During those windows, API p99 went from 20 ms to 45 ms.

**Diagnosis:** The trace showed all 8 Ps busy with report goroutines. Request goroutines sat in the run queues: `/sched/latencies` p99 jumped to ~10 ms per scheduling event, and a request is scheduled several times (each network wait → ready → run). Go has no priorities, so the batch work competed equally with the requests.

**Fix options (from best to worst):**
1. Move batch work to a separate process or deployment (hard isolation).
2. Limit batch parallelism to `GOMAXPROCS/4` with a semaphore, leaving most Ps for requests.
3. Make batch goroutines yield at chunk boundaries (process 1k rows → check a "pressure" signal → back off).

**Lesson:** In-process concurrency has **no QoS**. Priorities have to be built from admission control: semaphores, separate pools, separate processes.

### Exercises

1. ⭐ Why does an M in a blocking syscall not need a P?
2. ⭐⭐ Write a ping-pong benchmark over an unbuffered channel. Run it with `GOMAXPROCS=1` and `=8`. Explain why it barely changes (hint: `runnext`).
3. ⭐⭐⭐ Create 10k goroutines that each do 1 µs of CPU work and exit. Capture a trace and a CPU profile. Identify the scheduler overhead, then rewrite with chunked work and compare.

<details>
<summary>Solutions</summary>

1. A P represents the right to execute *Go code* (and owns Go-specific resources: mcache, run queue, timers). A thread blocked in the kernel isn't executing Go code, so its P can be handed to another M to keep running goroutines (Chapter 7).
2. Each send readies the receiver into `runnext` on the *same* P, which runs it immediately. Work never spreads across Ps. Both goroutines effectively share one P, so extra Ps sit idle.
3. Expect `runtime.newproc`, `runtime.schedule`, `runtime.findRunnable`, `runtime.gfget`/`gfput` in the profile. Chunking (e.g., 32 goroutines × 312 items each) removes nearly all of it.

</details>

### Review Checklist

- [ ] Does CPU-bound fan-out use chunks sized relative to `GOMAXPROCS`, not one goroutine per item?
- [ ] Does any design assume goroutine start order or fairness?
- [ ] Is latency-critical work protected from batch work through admission control?
- [ ] Is scheduler latency (`/sched/latencies:seconds`) exported as a metric?

---

## Chapter 6: Preemption

### TL;DR

- **Cooperative preemption:** at function prologues, a goroutine checks a flag (the stack guard) and yields if asked.
- **Asynchronous preemption** `[Go 1.14+]`: `sysmon` notices a G running more than **10 ms** and sends the thread a signal (`SIGURG` on Unix). The handler injects a call to `asyncPreempt` at a safe point.
- Before 1.14, a tight loop without function calls could block GC (STW) and starve other goroutines indefinitely. Today that mostly cannot happen.
- A side effect: syscalls can return `EINTR` more often. The standard library retries, but cgo code and raw `syscall` usage may need to as well.

### Mental Model

```
G running tight loop ─────────────────────────────────►
                    │                 │
               sysmon: >10ms?    send SIGURG to M
                                      │
                         signal handler: at async-safe point?
                           yes → inject asyncPreempt() → G back to run queue
                           no  → try again later
```

### Under the Hood

- **Cooperative path:** the runtime sets `g.stackguard0 = stackPreempt`, a value that makes the next function prologue's stack check fail. The goroutine enters `morestack`, which notices the preemption request and calls into the scheduler. Cheap, but it requires a function call.
- **Async path:** `preemptone` → `preemptM` → `signalM(mp, sigPreempt)`. In the handler, `wantAsyncPreempt` and `isAsyncSafePoint` check that the PC is somewhere the runtime can precisely describe registers and the stack. Some code is not async-safe: the runtime itself, some assembly, and code with `//go:nosplit`. Those wait for a cooperative point.
- **GC needs preemption** to stop the world and to scan stacks. Non-preemptible code directly delays STW. That shows up as long STW pauses in `GODEBUG=gctrace=1`.
- **Disable for debugging:** `GODEBUG=asyncpreemptoff=1`. Use it only to test whether a bug is related to preemption.

### Practical Code: the classic pre-1.14 hang

```go
package main

import (
	"fmt"
	"runtime"
	"time"
)

func main() {
	runtime.GOMAXPROCS(1)
	go func() {
		for { // no function calls: no cooperative preemption point
		}
	}()
	time.Sleep(10 * time.Millisecond) // main needs the only P back
	fmt.Println("reached") // Go ≤1.13: never prints. Go 1.14+: prints.
}
```

### Pitfalls

> ⚠️ **Relying on preemption for correctness.** Preemption bounds latency. It does not make busy-waiting correct (the loop above is still burning a core and, if it reads shared state without sync, it is still racy).

> ⚠️ **EINTR in cgo or raw syscalls.** Since 1.14, frequent `SIGURG` can interrupt blocking syscalls made outside the standard library. Retry on `EINTR`. Some C libraries mishandle signals, so check them.

> ⚠️ **Hot `//go:nosplit` or assembly loops.** These are not async-preemptible and can extend STW pauses.

### IRL Scenario: 300 ms GC pauses in a compression service

**Context:** `gctrace` showed occasional STW pauses of 300 ms. The heap was small.

**Diagnosis:** An execution trace showed STW waiting on one goroutine. It was running a long assembly-optimised compression routine (third-party, no safe points) over a 200 MB buffer.

**Fix:** Process the buffer in 1 MB chunks, calling the routine once per chunk, so a preemption point exists between chunks. STW dropped to <1 ms.

**Lesson:** Long-running non-preemptible regions turn into GC pauses. Chunk them.

### Exercises

1. ⭐ Why can't the runtime preempt at *any* instruction?
2. ⭐⭐ Run the hang example with `GODEBUG=asyncpreemptoff=1` on a modern Go. What happens, and why?

<details>
<summary>Solutions</summary>

1. GC needs to know exactly which registers and stack slots hold pointers. At arbitrary instructions (e.g., mid-sequence where a pointer is temporarily in an untracked register, or inside runtime critical sections) that information isn't available or consistent. Async safe points are PCs where it is.
2. It hangs like Go ≤1.13. Only cooperative preemption remains, and the loop makes no calls.

</details>

### Review Checklist

- [ ] Do long CPU-bound loops (especially assembly/cgo) have chunk boundaries?
- [ ] Does any cgo or raw-syscall code handle `EINTR`?
- [ ] Are STW pauses monitored (`/sched/pauses/total/gc:seconds` in `runtime/metrics`)?

---

## Chapter 7: Syscalls, the Netpoller and sysmon

### TL;DR

- **Network I/O** doesn't block threads. It goes through the **netpoller** (epoll/kqueue/IOCP). Goroutines park; the thread runs other Gs.
- **Blocking syscalls** (regular-file I/O, many `syscall.*` calls) and **cgo calls** *do* block a thread. The runtime hands that thread's P to another M so Go code keeps running, which can create many threads.
- **sysmon** is a runtime thread without a P. It retakes Ps from long syscalls, triggers async preemption, polls the network if nobody has, and forces a GC every 2 minutes if none ran.
- The default thread limit is 10,000 (`debug.SetMaxThreads`). Exceeding it **crashes** the program.

### Mental Model

```
Network read (EAGAIN):            Blocking file read / cgo:
  G parks on pollDesc               M enters syscall holding P "loosely"
  M runs other G's                  sysmon: syscall > ~20µs? → hand P to another M
  epoll says ready → G runnable     syscall returns → M tries to reacquire a P
                                    no P free → G to global queue, M parks
Threads stay ≈ GOMAXPROCS           Threads grow with concurrent blocking calls
```

### Under the Hood

#### Syscall path

- `entersyscall`: G state → `_Gsyscall`. In Go 1.26 the P stays `_Prunning` and attached to the M, and "P in a syscall" is identified from its G's state. (Go ≤1.25 used a separate `_Psyscall` P state, which is now defunct.)
- If the syscall returns quickly, `exitsyscall` grabs the same P again (fast path, no thread switch).
- If it doesn't, **sysmon's `retake`** spots a P whose G has been in `_Gsyscall` across a sysmon tick (starting at 20 µs). If there is other work, it blocks that G from leaving the syscall, takes the P, and calls `handoffp`, which starts or wakes another M to run that P.
- `entersyscallblock` is used for calls known to block, and hands the P off immediately.
- On return without a P available, the G goes to the global run queue and the M parks (idle threads are kept for reuse).

#### Netpoller

- `net.Conn.Read` → `poll.FD.Read` → syscall returns `EAGAIN` → `pollDesc.waitRead` → `gopark`. The G is now `_Gwaiting` and costs no thread.
- The scheduler polls `netpoll(0)` (non-blocking) inside `findRunnable`. When nothing else is runnable, one M does a blocking `netpoll` until the next timer fires.
- **Deadlines** (`SetReadDeadline`) are timers integrated with the poller, which is why they are cheap, and why the `net` package implements ctx cancellation of dials (and `net/http` some aborts) by setting a deadline in the past.
- **Regular files are not pollable** with epoll, so `os.File.Read` on disk is a blocking syscall. Pipes, sockets and some devices *are* polled.

#### sysmon

A dedicated M with no P, running `sysmon()` in a loop. It sleeps 20 µs. After ~1 ms of idleness it starts doubling, up to 10 ms. Each round:
- **retake:** hand off Ps stuck in syscalls; request preemption of Gs running >10 ms (Chapter 6).
- **netpoll:** if the network hasn't been polled for 10 ms, poll and inject ready Gs.
- **forced GC:** if no GC in 2 minutes (`forcegcperiod`).
- Wake the scavenger (returns memory to the OS).

### Practical Code: watching thread explosion

```go
package main

import (
	"fmt"
	"runtime/pprof"
	"sync"
	"syscall"
	"time"
)

func main() {
	var wg sync.WaitGroup
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// A blocking syscall that takes a while: each one pins an M.
			ts := syscall.Timespec{Sec: 1}
			syscall.Nanosleep(&ts, nil)
		}()
	}
	time.Sleep(200 * time.Millisecond)
	fmt.Println("threads created:", pprof.Lookup("threadcreate").Count())
	wg.Wait()
}
```

Compare with `time.Sleep(time.Second)` in the goroutines (timer-based, no thread pinned). The thread count stays around `GOMAXPROCS`.

**Bounding blocking work:** always put a semaphore around cgo- or file-I/O-heavy fan-out:

```go
var diskSem = make(chan struct{}, 64) // bound concurrent blocking file ops

func readFile(path string) ([]byte, error) {
	diskSem <- struct{}{}
	defer func() { <-diskSem }()
	return os.ReadFile(path)
}
```

### Pitfalls

> ⚠️ **Unbounded file I/O or cgo concurrency.** Each concurrent blocking call can cost a thread (a kernel thread plus a g0 stack: 16 KB in pure-Go Linux binaries, ~8 MB virtual with cgo). At 10k threads the process dies: `runtime: program exceeds 10000-thread limit`.

> ⚠️ **cgo DNS resolver.** With the cgo resolver (`GODEBUG=netdns=cgo`, or forced by some system configs), every concurrent lookup blocks a thread. A DNS slowdown can turn into a thread explosion. Prefer the pure-Go resolver (`netdns=go`) unless you need NSS features.

> ⚠️ **Slow NFS/FUSE mounts.** A hung filesystem turns every `os.Open` into a stuck thread.

### IRL Scenario: the service that crashed when the NFS server hiccupped

**Context:** An image service read originals from an NFS mount, one goroutine per request, no bound. During a 30 s NFS stall, the process crashed with the 10,000-thread limit.

**Diagnosis:** `threadcreate` profile and `/proc/<pid>/status` (Threads: ~9,800 just before the crash). Each blocked `read()` held an M. New requests kept arriving and kept spawning blocked threads.

**Fix:**
1. Semaphore of 128 around file reads, acquired with ctx (`semaphore.Weighted.Acquire(ctx, 1)`) so requests fail fast instead of piling up.
2. Load shedding: if the semaphore wait exceeds 100 ms, return 503.
3. Alert on the `threadcreate` count and on `/sched/threads/total:threads` versus `/sched/gomaxprocs:threads`.

### Exercises

1. ⭐ Why are network reads "free" in terms of threads but disk reads are not?
2. ⭐⭐ Run the thread explosion code with `debug.SetMaxThreads(100)`. What happens? Is crashing the right behaviour?
3. ⭐⭐⭐ Read `runtime.retake` in `proc.go`. Under what conditions does sysmon *not* hand off a P in a syscall?

<details>
<summary>Solutions</summary>

1. Sockets support readiness notification (epoll), so the runtime can park the goroutine and get notified. epoll refuses regular files (`EPERM`), and poll/select report them as always ready, so a read must be a blocking syscall on a thread (unless you use io_uring, which Go doesn't by default).
2. It crashes: `thread exhaustion`. Yes, by design. The runtime cannot make progress safely, and the limit exists to turn runaway growth into a visible failure instead of an OOM'd machine.
3. Roughly: if the P's run queue is empty, there are spinning or idle Ms/Ps to pick up new work, and the syscall has been in progress for less than ~10 ms, it leaves the P alone, because handing off would just create a thread with nothing to do.

</details>

### Review Checklist

- [ ] Is concurrency around file I/O, cgo and blocking syscalls bounded?
- [ ] Is the DNS resolver mode known and deliberate?
- [ ] Is thread count monitored and alerted on?
- [ ] Do blocking operations have timeouts (deadlines for net, ctx-aware semaphores for the rest)?

---

## Chapter 8: Goroutine Lifecycle and Stacks

### TL;DR

- Goroutines start with a small stack (**2 KB** minimum; since Go 1.19 the initial size adapts to the average observed stack usage) that **grows by copying** (doubling) and can **shrink** during GC.
- A goroutine costs ~2–8 KB plus a few hundred bytes for the `g` struct. 100k goroutines is fine; 10M probably isn't.
- **Leaked goroutines are never collected.** A goroutine blocked forever keeps its stack and everything it references alive.
- A **panic in any goroutine** without a `recover` in *that* goroutine crashes the whole process.
- There is no public goroutine ID, by design. Use pprof labels for observability.

### Mental Model: states

```
          go f()
            │
            ▼
       _Grunnable ◄──────────── ready (chan op, timer, netpoll, unlock)
            │ schedule                    ▲
            ▼                             │
        _Grunning ──── gopark ───► _Gwaiting  (chan, select, mutex, sleep, I/O)
            │   │                         
            │   └── syscall ───► _Gsyscall ──► back to _Grunning / _Grunnable
            │
            ▼ return / Goexit
         _Gdead (g struct reused via per-P free list)
```

`_Gwaiting` goroutines carry a **wait reason**, which is exactly what you see in a goroutine dump: `[chan receive]`, `[select]`, `[sync.Mutex.Lock]`, `[IO wait]`, `[sleep]`, plus durations like `[chan receive, 42 minutes]`.

### Under the Hood

- **Stack growth:** each function prologue compares SP to `stackguard0`. If there isn't enough room, it calls `morestack` → `newstack`, which allocates a stack twice the size, **copies** the frames, adjusts pointers into the stack, and frees the old one. That is why Go can't let you take raw pointers to stack memory across calls in cgo, and why `unsafe` stack-pointer tricks break.
- **Stack shrinking:** during GC, if a goroutine uses less than 1/4 of its stack, it is shrunk by half.
- **Max stack:** 1 GB on 64-bit (`debug.SetMaxStack`). Exceeding it is a fatal `goroutine stack exceeds` error, usually from infinite recursion.
- **Hot-path stack growth:** a function that repeatedly triggers growth (e.g., a big frame called in a loop by a goroutine that just started) pays a copy every time. It is usually negligible, but it shows up as `runtime.newstack` / `copystack` in profiles of goroutine-per-request servers with deep stacks.

### Practical Code

**Panic containment**: every goroutine you start in a server should have a policy:

```go
// Go runs f in a goroutine, converting panics to errors for the given handler.
func Go(f func(), onPanic func(any, []byte)) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				onPanic(r, debug.Stack())
			}
		}()
		f()
	}()
}
```

> ⚠️ Recovering is a *policy decision*. Recovering in a request-scoped goroutine is usually right (log, metric, 500). Recovering in a goroutine that owns shared state may leave that state half-modified. Sometimes crashing is the safer choice.

> 🔬 **Note:** `sync.WaitGroup.Go` `[Go 1.25+]` deliberately does **not** recover. If `f` panics, it re-panics *without* calling `Done`, so `Wait` cannot return and race the crash.

**pprof labels**: attribute goroutines and CPU to tenants or endpoints:

```go
pprof.Do(ctx, pprof.Labels("tenant", tenantID, "route", "/export"), func(ctx context.Context) {
	runExport(ctx) // goroutines started inside inherit the labels
})
```

Then: `go tool pprof -tagfocus tenant=acme cpu.pprof`, or view goroutine dumps grouped by label (`debug=1` goroutine profile shows labels).

**Goroutine dump on demand** (great in production):

```go
import _ "net/http/pprof" // on an internal-only listener!

// curl localhost:6060/debug/pprof/goroutine?debug=2   full stacks with wait reasons/durations
// curl localhost:6060/debug/pprof/goroutine?debug=1   aggregated by stack, with counts
```

Or send `SIGQUIT` (`kill -QUIT <pid>`) to dump all goroutines to stderr and exit. Set `GOTRACEBACK=all` (or `system`) so crashes include all goroutines.

### Pitfalls

> ⚠️ **Goroutine leaks.** The canonical leak: a goroutine blocked sending to a channel nobody will read.
> ```go
> func first(urls []string) string {
>     ch := make(chan string) // unbuffered
>     for _, u := range urls {
>         go func() { ch <- fetch(u) }()
>     }
>     return <-ch // the other len(urls)-1 goroutines block forever
> }
> ```
> Fix: `make(chan string, len(urls))`, or cancel the others with ctx and have them select on `ctx.Done()`.

> ⚠️ **Storing goroutine IDs** (parsed from `runtime.Stack`) to build goroutine-local storage. It's fragile, slow, and fights the design. Pass `context.Context` instead.

### IRL Scenario: memory grows 200 MB/day, heap profile looks clean

**Context:** RSS grows steadily and the pod is OOM-killed every ~5 days. The heap profile's top entries are ordinary.

**Diagnosis:**
1. `runtime.NumGoroutine()` metric: rising linearly, ~40k/day.
2. `goroutine?debug=1`: 38,000 goroutines in `[chan send]` at `notifier.go:88`, with wait times of hours.
3. Code: per-request goroutine sends an audit event to `auditCh`. When the audit consumer hits an error, it `return`s, and nobody reads `auditCh` ever again.

**Fix:**
- The producer uses `select { case auditCh <- ev: case <-ctx.Done(): case <-a.closed: default: dropped.Add(1) }`. Audit is best-effort, so drop and count rather than block.
- The consumer is supervised: restarted with backoff and never silently exits.
- Alert on goroutine count growth rate.

> 🔬 **Go 1.26 experiment:** building with `GOEXPERIMENT=goroutineleakprofile` adds a `goroutineleak` pprof profile, which uses the GC to find goroutines blocked on concurrency primitives that are *unreachable* from any runnable goroutine (i.e., provably leaked). Check your Go version's release notes for its status.

### Exercises

1. ⭐ Approximately how much memory do 1M idle goroutines take? Measure it with `runtime.ReadMemStats` (`StackInuse`).
2. ⭐⭐ Write a test helper that fails if the goroutine count after the test is higher than before (allowing for a short settle time). What are its weaknesses compared with `go.uber.org/goleak`?
3. ⭐⭐⭐ Explain why a goroutine blocked forever on `<-ch` keeps a 100 MB slice alive if it captured it in a closure, even though `ch` itself is unreachable from main.

<details>
<summary>Solutions</summary>

1. ≈ 2–3 GB (2 KB+ stack each, plus the `g` struct). This varies with the adaptive initial stack size.
2. Count-based checks are flaky: goroutines from other tests, runtime or library background goroutines, and slow shutdowns all interfere. goleak compares *stacks* against an ignore list, retries with backoff, and reports the leaked goroutine's stack.
3. All goroutine stacks are GC roots. The blocked goroutine's stack holds the closure, which holds the slice. The channel being unreachable from main doesn't matter, because the goroutine itself is a root. (That is exactly the case the experimental leak profile targets.)

</details>

### Review Checklist

- [ ] For every `go` statement: how does this goroutine end? (Chapter 23)
- [ ] Is there a panic policy for long-lived and request-scoped goroutines?
- [ ] Is `runtime.NumGoroutine()` exported and alerted on (level *and* growth)?
- [ ] Is `net/http/pprof` available on an internal listener in production?
- [ ] Is `GOTRACEBACK` set appropriately for crash forensics?

---

## Chapter 9: GC Interaction

### TL;DR

- Go's GC is **concurrent mark-sweep**, non-moving and non-generational, with two short STW phases (typically well under 1 ms).
- During marking, **25% of GOMAXPROCS** goes to background mark workers, and **allocating goroutines are forced into "mark assist"**, which shows up as request latency.
- **Write barriers** are on during marking, so pointer writes get slightly slower.
- `GOGC` sets the heap growth target. `GOMEMLIMIT` `[Go 1.19+]` sets a soft memory cap. Together they trade CPU for memory.
- Goroutine stacks are roots. More goroutines means more root-scanning work, and each stack scan briefly stops that goroutine.
- **Green Tea GC** (a span-oriented, cache-friendlier marking algorithm) was an experiment in 1.25 and is the **default in Go 1.26**.

### Mental Model: one GC cycle

```
 STW  ─┐ sweep termination, enable write barrier (~10-100µs)
       ▼
 MARK (concurrent): background workers (25% of Ps) + mark assists from allocators
       │   your goroutines run, but allocation-heavy ones pay "assist tax"
 STW  ─┤ mark termination (~10-100µs)
       ▼
 SWEEP (concurrent, lazy, done on allocation and by a background sweeper)
```

### Under the Hood

- **Pacer:** decides when to start a cycle so it finishes right when the heap reaches the target `live_heap × (1 + GOGC/100)`, plus GOGC% of stack and globals scan work since Go 1.18 (default GOGC=100 means roughly 2× live heap).
- **Mark assist:** if a goroutine allocates while marking is behind schedule, it must do marking work proportional to its allocation before it can proceed. A request handler that allocates 10 MB during a GC cycle can stall for milliseconds.
- **Write barrier:** the hybrid Yuasa/Dijkstra barrier `[Go 1.8+]` shades pointers on write during marking, so stacks don't need re-scanning at mark termination.
- **GOMEMLIMIT:** as the total Go memory approaches the limit, the GC runs more often. To prevent a death spiral, GC CPU is capped at around 50% over a time window, so the program may then exceed the limit rather than thrash.
- **Concurrency link:** GC CPU time comes out of the same Ps that run your goroutines. With GOMAXPROCS=8 that is 2 dedicated mark workers. With small GOMAXPROCS values where 25% doesn't round well (e.g. 1–3, 6), the runtime uses fractional workers instead: each P spends about 25% of its time on mark work. The proportion is the same, but with few Ps, one P busy marking is a large slice of total capacity, so small containers feel GC more.

### Practical Code: diagnosing GC-driven latency

```bash
GODEBUG=gctrace=1 ./server
# gc 42 @12.3s 4%: 0.031+2.1+0.045 ms clock, 0.25+1.2/4.1/0.5+0.36 ms cpu, 120->124->62 MB, 124 MB goal, 0 MB stacks, 0 MB globals, 8 P
#        ^ cumulative GC CPU%      ^ STW1 + concurrent mark + STW2
#                                  cpu: assist/background/idle breakdown in the middle term
```

Key `runtime/metrics` to export:

| Metric | Why |
|---|---|
| `/gc/cycles/total:gc-cycles` | GC frequency |
| `/cpu/classes/gc/mark/assist:cpu-seconds` | **Assist time**, which is latency your goroutines paid |
| `/cpu/classes/gc/total:cpu-seconds` | Total GC CPU |
| `/sched/pauses/total/gc:seconds` | STW pause histogram |
| `/gc/heap/goal:bytes` | Heap target |

**Reducing GC pressure in concurrent hot paths** (in order of effectiveness):
1. Allocate less: reuse buffers, avoid `[]byte`↔`string` round-trips, preallocate slices.
2. `sync.Pool` for short-lived, frequently allocated objects (Chapter 14).
3. Set `GOMEMLIMIT` to ~90% of the container limit and raise `GOGC` (or `GOGC=off` with a limit, which is risky) to trade memory for CPU.

### Pitfalls

> ⚠️ **GOMAXPROCS too low for GC.** With GOMAXPROCS=1, GC marking takes a quarter of your only P via a fractional worker that interleaves with your goroutines. Latency-sensitive services under-provisioned on CPU feel GC much more.

> ⚠️ **Huge pointer-rich heaps.** Mark time scales with *live pointers*. A 20 GB `map[string]*Struct` cache makes every cycle expensive. Use pointer-free representations (indexes into slices, `[]byte` blobs) for big caches.

> ⚠️ **Mistaking assist latency for lock contention.** In traces, look for "GC (mark assist)" slices on request goroutines before blaming mutexes.

### IRL Scenario: p99 spikes every 4 seconds

**Context:** A JSON API showed a sawtooth: p99 spiking to 60 ms every ~4 s, with p50 flat.

**Diagnosis:** The spikes lined up with `gctrace` cycles. The `mark/assist` CPU metric was high. The trace showed request goroutines in "mark assist" for 10–40 ms. The cause was decoding large JSON request bodies into `map[string]any`, which generated huge allocation bursts.

**Fix:** Decode into typed structs, pool the decode buffers, and set `GOMEMLIMIT=1800MiB` (2 GiB container) with `GOGC=200`. Cycles dropped to one every ~15 s, assist time fell 90%, and p99 dropped to 12 ms.

### Exercises

1. ⭐ With GOGC=100 and a live heap of 400 MB, what is the heap goal? With GOGC=50?
2. ⭐⭐ Explain why GC assists punish exactly the goroutines that allocate most. Is that fair? Is it desirable?
3. ⭐⭐⭐ Build a service with a 2 GB pointer-heavy cache and measure GC CPU. Convert the cache to a pointer-free layout and measure again.

<details>
<summary>Solutions</summary>

1. 800 MB at GOGC=100, 600 MB at GOGC=50.
2. It is a feedback mechanism: allocation is what makes GC necessary, so allocators pay the debt they create. That keeps the heap bounded even when allocation outpaces background marking. It is "fair" by cost causation, but it puts GC latency on your hottest request paths, which is why you minimise allocation there.
3. Expect a large drop. The GC still sees the big byte slices, but it doesn't scan inside pointer-free memory (noscan spans).

</details>

### Review Checklist

- [ ] Are GC assist time and STW pauses exported as metrics?
- [ ] Is `GOMEMLIMIT` set relative to the container limit?
- [ ] Are hot concurrent paths allocation-conscious?
- [ ] Are large caches pointer-free where possible?

---

[← Part I](01-foundations.md) · [Index](README.md) · [Next: Part III, Channels & select →](03-channels-and-select.md)
