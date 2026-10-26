# Appendices

[← Part XI](11-mastery-drills.md) · [Index](README.md)

- [A. Decision Matrix: Which Primitive?](#a-decision-matrix-which-primitive)
- [B. Cheat Sheets](#b-cheat-sheets)
- [C. Go Version Timeline (Concurrency-Relevant)](#c-go-version-timeline-concurrency-relevant)
- [D. Runtime Knobs and Diagnostics](#d-runtime-knobs-and-diagnostics)
- [E. Glossary](#e-glossary)
- [F. Reading List](#f-reading-list)

---

## A. Decision Matrix: Which Primitive?

### By problem

| Problem | First choice | Alternatives | Avoid |
|---|---|---|---|
| Run N tasks, wait for all, first error cancels | `errgroup.WithContext` | — | Hand-rolled WaitGroup + error channel |
| Run N tasks with a concurrency bound | `errgroup.SetLimit` | `semaphore.Weighted`, worker pool | Unbounded `go` per item |
| Wait for N goroutines, no errors | `sync.WaitGroup` (`wg.Go`) | errgroup | `time.Sleep` |
| Protect a struct with invariants | `sync.Mutex` | single-owner goroutine | Per-field atomics |
| Read-mostly, small, rarely updated data | `atomic.Pointer[T]` (copy-on-write) | `RWMutex` | Mutex on a 64-core hot path |
| Read-mostly with long read sections | `sync.RWMutex` | COW | — |
| Simple counter / flag | `atomic.Int64` / `atomic.Bool` | sharded counter if hot | Plain `int` + "it's just a counter" |
| Transfer ownership between stages | channel | — | Shared slice + mutex |
| One-shot broadcast signal | `close(ch)` / `context` | — | `sync.Cond` |
| Repeated broadcast with cancellation | close-and-replace channel | `Cond` + `context.AfterFunc` | `Cond` alone |
| Lazy one-time init | `sync.OnceValue(s)` | mutex + flag (if retryable) | Double-checked locking |
| Dedupe concurrent identical work | `singleflight` | — | Per-key mutex map without cleanup |
| Rate limit (per time) | `x/time/rate` | — | `time.Sleep(1/rps)` |
| Concurrency limit with cancellation | `semaphore.Weighted` / buffered chan | — | `sync.Mutex` (not cancellable) |
| Reuse short-lived allocations | `sync.Pool` (after profiling) | free list | Pooling connections |
| Concurrent map, general purpose | `map` + `Mutex` | sharded map | `sync.Map` by default |
| Concurrent map, stable keys / disjoint writers | `sync.Map` | sharded map | — |
| Complex state machine with timers and I/O | owning goroutine (actor) | mutex + careful design | Actor for a simple KV store |
| Cancellation and deadlines across APIs | `context.Context` | — | Custom done channels in public APIs |

### Channel vs mutex: quick test

```
Are you passing data (or ownership) from one goroutine to another?   → channel
Are you coordinating lifecycle / signalling an event?                  → channel / context
Are you guarding access to shared state in place?                      → mutex
Are you unsure?                                                        → mutex (simpler, easier to change later)
```

---

## B. Cheat Sheets

### B1. Channel operations

| | `nil` | open, empty | open, has data / receiver waiting | closed |
|---|---|---|---|---|
| `ch <- v` | block forever | block (unbuffered/full) or buffer | succeed | **panic** |
| `<-ch` | block forever | block | succeed | zero value, `ok=false` (after draining buffer) |
| `close(ch)` | **panic** | succeed | succeed (buffered values still receivable) | **panic** |

**Rules:** the sender closes; with multiple senders, one coordinator closes after all senders finish; never close from the receiver; closing is a broadcast; a nil channel disables a `select` case.

### B2. `select`

- Ready cases are chosen uniformly at random. Source order means nothing.
- All channel and value expressions are evaluated once, on entry, in source order.
- `default` = non-blocking. `select {}` = block forever.
- `break` exits the select only. Use labels or `return` to leave loops.
- Cancellation in every blocking select: `case <-ctx.Done():`.

### B3. Memory model: happens-before edges

| Operation A | is synchronized before |
|---|---|
| `go f()` statement | start of `f` |
| send on channel | completion of the matching receive |
| close of channel | a receive that returns because it's closed |
| receive on **unbuffered** channel | completion of the matching send |
| *k*-th receive on cap-*C* channel | completion of (*k+C*)-th send |
| `mu.Unlock()` (n-th) | return of `mu.Lock()` (m-th, m > n) |
| completion of `f` in `once.Do(f)` | return of any `once.Do(f)` |
| atomic write observed by atomic read | that read (and all atomics are seq-cst) |
| `wg.Done()` | return of the `wg.Wait()` it unblocks |
| *(goroutine exit)* | *nothing* |

### B4. `context`

- First parameter, named `ctx`. Never nil (use `context.TODO()`).
- Always `defer cancel()`, or call it promptly in loops.
- Cancellation only propagates down the tree. Deadlines only shrink.
- `context.Cause(ctx)` for the reason. `ctx.Err()` for the category.
- `WithoutCancel` for work that outlives a request, plus your own timeout and tracking.
- `AfterFunc` to bridge ctx to non-ctx APIs (deadlines, Cond).
- Values: request-scoped and cross-cutting only, with unexported key types.

### B5. sync primitives: the sharp edges

| Primitive | Sharp edge |
|---|---|
| `Mutex` | Not reentrant; don't copy; no I/O under lock |
| `RWMutex` | Writer-preferring → recursive RLock deadlocks; no upgrade; readers contend on a shared counter |
| `WaitGroup` | `Add` before `go` (or `wg.Go`); don't copy; don't reuse before `Wait` returns |
| `Once` | Panic or failure = done forever; recursive `Do` deadlocks |
| `OnceFunc/Value/Values` | Re-panics on every call if `f` panicked; caches errors |
| `Cond` | `Wait` in a `for` loop; can't select or cancel |
| `Pool` | Reset objects; pointers only; cap big buffers; GC clears it |
| `Map` | Untyped; `Range` isn't a snapshot; only for its two workloads |
| `atomic.Value` | Consistent concrete type; no nil |
| `atomic.*` | One word only; no cross-field invariants |

### B6. Timers `[Go 1.23+ semantics]`

- Unreferenced timers and tickers are GC'd even if not stopped.
- Timer channels are synchronous: no stale values after `Stop`/`Reset`, so no drain needed.
- `AfterFunc` callbacks run in their own goroutine; `Stop` doesn't wait for them.
- Semantics are chosen by the **main module's** `go` line (`GODEBUG=asynctimerchan=1` reverts).
- Tickers drop ticks for slow receivers.

### B7. The five review questions

1. **Lifecycle:** what stops each goroutine, and who waits for it?
2. **Ownership:** who sends, receives and closes each channel? What guards each shared variable?
3. **Happens-before:** which sync operation orders each cross-goroutine write before its read?
4. **Bounds:** what limits goroutines, memory, queue length and queue time?
5. **Failure:** what happens on error, cancel, slow or vanished consumer, and panic?

---

## C. Go Version Timeline (Concurrency-Relevant)

| Version | Changes |
|---|---|
| 1.5 | GOMAXPROCS defaults to NumCPU; concurrent GC |
| 1.7 | `context` in the standard library |
| 1.8 | Hybrid write barrier; mutex profile |
| 1.9 | `sync.Map`; mutex starvation mode |
| 1.13 | `sync.Pool` victim cache |
| 1.14 | **Async preemption**; per-P timers; open-coded defers |
| 1.18 | Generics; `Mutex.TryLock` |
| 1.19 | **Revised memory model**; typed atomics; `GOMEMLIMIT`; adaptive initial stack size |
| 1.20 | `context.WithCancelCause`/`Cause`; `sync.Map` `Swap`/`CompareAndSwap`/`CompareAndDelete`; `errors.Join` |
| 1.21 | `sync.OnceFunc`/`OnceValue`/`OnceValues`; `context.AfterFunc`/`WithoutCancel`/`WithDeadlineCause`/`WithTimeoutCause`; low-overhead tracing |
| 1.22 | **Per-iteration loop variables**; `math/rand/v2`; range over int; new trace format |
| 1.23 | **Timer/Ticker changes** (GC'd when unreferenced, synchronous channels); range-over-func iterators; `atomic` `And`/`Or`; `sync.Map.Clear` |
| 1.24 | `sync.Map` reimplemented (hash-trie); `testing/synctest` experiment; `maphash.Comparable`; `runtime.AddCleanup`; `weak` |
| 1.25 | **Container-aware GOMAXPROCS**; **`testing/synctest` GA**; `sync.WaitGroup.Go`; `trace.FlightRecorder`; vet `waitgroup` analyzer; Green Tea GC experiment |
| 1.26 | Green Tea GC on by default; `goroutineleak` profile experiment (`GOEXPERIMENT=goroutineleakprofile`) |

> Several of these depend on `go.mod`'s `go` directive, not on the toolchain version. Loop-var semantics follow **each module's own** `go` line. Timer and GOMAXPROCS behaviour follow the **main module's** `go` line (GODEBUG defaults).

---

## D. Runtime Knobs and Diagnostics

### Environment / GODEBUG

| Setting | Effect |
|---|---|
| `GOMAXPROCS=n` | Fix the number of Ps (disables container auto-tuning in 1.25+) |
| `GOGC=n` / `GOMEMLIMIT=…` | GC aggressiveness / soft memory limit |
| `GOTRACEBACK=all\|system\|crash` | How much is dumped on crash (`crash` also produces a core dump) |
| `GODEBUG=schedtrace=1000,scheddetail=1` | Scheduler state every second |
| `GODEBUG=gctrace=1` | One line per GC cycle |
| `GODEBUG=asyncpreemptoff=1` | Disable async preemption (debugging only) |
| `GODEBUG=asynctimerchan=1` | Pre-1.23 timer channel semantics |
| `GODEBUG=containermaxprocs=0` / `updatemaxprocs=0` | Disable container-aware / dynamically updated GOMAXPROCS (1.25+) |
| `GODEBUG=netdns=go\|cgo` | DNS resolver choice (cgo blocks threads) |
| `GORACE="halt_on_error=1 history_size=7"` | Race detector options |

### Commands

```bash
go test -race -count=100 -cpu=1,4,16 ./...       # race + interleaving exploration
go test -run X -trace=trace.out && go tool trace trace.out
go tool pprof -http=:8081 host:6060/debug/pprof/mutex     # or goroutine / block / profile (one per run)
curl 'host:6060/debug/pprof/goroutine?debug=1'     # aggregated goroutine dump
curl 'host:6060/debug/pprof/goroutine?debug=2'     # full dump with wait durations
kill -QUIT <pid>                                  # dump all goroutines and exit
go vet ./... && staticcheck ./...
dlv attach <pid>   →  goroutines -t / goroutine <id> bt
```

### Key `runtime/metrics`

`/sched/goroutines:goroutines` · `/sched/latencies:seconds` · `/sched/gomaxprocs:threads` · `/sync/mutex/wait/total:seconds` · `/gc/cycles/total:gc-cycles` · `/cpu/classes/gc/mark/assist:cpu-seconds` · `/sched/pauses/total/gc:seconds` · `/gc/heap/goal:bytes`

---

## E. Glossary

| Term | Meaning |
|---|---|
| **ABA problem** | A CAS succeeds because a value changed A→B→A, hiding the intermediate change. Largely prevented by GC in Go unless nodes are recycled. |
| **Actor** | A goroutine that exclusively owns state and processes messages from a mailbox. |
| **Backpressure** | A slow consumer causing upstream producers to slow down (by blocking) instead of buffering without bound. |
| **Bulkhead** | Isolating resources (pools, semaphores) per dependency so one failure can't exhaust shared capacity. |
| **Cache line** | The unit of cache coherence (usually 64 B). Independent data sharing a line causes false sharing. |
| **Confinement** | Making data reachable by only one goroutine, which removes the need for synchronisation. |
| **Data race** | Concurrent access to the same memory, at least one a write, at least one non-atomic, with no happens-before order. |
| **DRF-SC** | Data-race-free programs behave sequentially consistently. Go's core memory-model guarantee. |
| **Durably blocked** | (synctest) Blocked in a way only another goroutine in the same bubble can unblock. |
| **False sharing** | Cores contending on a cache line holding *different* variables. |
| **Fencing token** | A monotonically increasing token attached to writes so storage can reject a stale leader's writes. |
| **G / M / P** | Goroutine / OS thread / processor (scheduling context, count = GOMAXPROCS). |
| **Happens-before** | A partial order of events. If A happens before B, B observes A's effects. |
| **Hedged request** | A duplicate request sent after a delay to cut tail latency. The first response wins. |
| **Linearizability** | Each operation appears to take effect atomically at some instant between its call and return. |
| **Livelock** | Goroutines keep acting (retrying) but make no progress. |
| **Lock convoy** | Goroutines repeatedly queue behind a contended lock, collapsing throughput. |
| **Lost wakeup** | A signal sent before the waiter starts waiting, so it's missed. |
| **Mark assist** | GC work forced on allocating goroutines during marking. |
| **Metastable failure** | An overload state that sustains itself (e.g., via retries) after the trigger is gone. |
| **Netpoller** | Runtime integration with epoll/kqueue/IOCP that parks goroutines on network I/O without blocking threads. |
| **runnext** | A per-P slot that runs a just-readied goroutine next, inheriting the time slice. |
| **Singleflight** | Deduplicating concurrent calls for the same key into one execution. |
| **Starvation** | A goroutine repeatedly failing to acquire a resource others keep getting. |
| **Structured concurrency** | Goroutine lifetimes nested within lexical scopes or owned objects. |
| **sudog** | Runtime representation of a goroutine waiting on a channel (or sync object). |
| **sysmon** | A runtime monitor thread: retakes Ps from syscalls, preempts long-running Gs, forces GC, polls the network. |
| **Thundering herd** | Many waiters woken at once by one event, all contending. |
| **Work stealing** | Idle Ps taking half of another P's run queue. |

---

## F. Reading List

### Specs and official docs
- **The Go Memory Model**: <https://go.dev/ref/mem> (read it twice).
- **Diagnostics**: <https://go.dev/doc/diagnostics>.
- `testing/synctest`, `context`, `sync`, `sync/atomic`, `runtime/trace` package docs on pkg.go.dev.
- Go release notes for 1.19–1.26 (the concurrency sections).

### Essays and posts
- Russ Cox, *Memory Models* series (Hardware, Programming Language, and Updating the Go Memory Model): <https://research.swtch.com/mm>.
- Sameer Ajmani, *Go Concurrency Patterns: Pipelines and cancellation*: <https://go.dev/blog/pipelines>.
- Sameer Ajmani, *Go Concurrency Patterns: Context*: <https://go.dev/blog/context>.
- The Go Blog posts on `testing/synctest`, container-aware GOMAXPROCS, and the Green Tea GC.
- Dmitry Vyukov, *Scalable Go Scheduler Design Doc* (the original GMP design).
- Dave Cheney, *Channel Axioms* and *Never start a goroutine without knowing how it will stop*.

### Talks
- Rob Pike, *Concurrency is not Parallelism* (2012).
- Bryan C. Mills, *Rethinking Classical Concurrency Patterns* (GopherCon 2018). Essential: channels as resources, replacing Cond and worker pools.
- Kavya Joshi, *Understanding Channels* (GopherCon 2017).
- Rhys Hiltner, talks on execution traces and production profiling.

### Books
- Katherine Cox-Buday, *Concurrency in Go* (O'Reilly). Patterns are still valid; check the API details against modern Go.
- Maurice Herlihy & Nir Shavit, *The Art of Multiprocessor Programming*. Theory of locks, lock-freedom and linearizability.
- Paul McKenney, *Is Parallel Programming Hard, And, If So, What Can You Do About It?* (free). Hardware, RCU, memory ordering.
- Brendan Gregg, *Systems Performance*. CPU, scheduling, and cgroup throttling at the OS level.

### Papers
- Dean & Barroso, *The Tail at Scale* (CACM 2013).
- Bronson et al., *Metastable Failures in Distributed Systems* (HotOS 2021).
- Thompson et al., *LMAX Disruptor* technical paper.
- Gunther, *Universal Scalability Law* (Guerrilla Capacity Planning).
- Vattani, Chierichetti & Lowenstein, *Optimal Probabilistic Cache Stampede Prevention* (XFetch), VLDB 2015.

### Source files worth reading (`$(go env GOROOT)/src`)
- `runtime/proc.go`: scheduler (`schedule`, `findRunnable`, `sysmon`, `retake`)
- `runtime/chan.go`, `runtime/select.go`: channels and select
- `runtime/sema.go`: semaphores behind sync
- `internal/sync/mutex.go`, `sync/rwmutex.go`, `sync/waitgroup.go`, `sync/once.go`, `sync/pool.go`
- `context/context.go`
- `runtime/time.go`: timers
- `golang.org/x/sync/errgroup`, `semaphore`, `singleflight`: small and very readable

---

[← Part XI](11-mastery-drills.md) · [Index](README.md)
