# Go Concurrency Masterclass

> From "I know goroutines and channels" to "I can design, review, debug and tune concurrent Go systems in production."

**Audience:** Engineers who already write Go well and know the basics of goroutines, channels, `sync` and `context`. Nothing here re-teaches syntax.

**Target version:** Go 1.25+. Behaviour that depends on the version is tagged inline, e.g. `[Go 1.23+]`.

---

## Why this still matters when AI writes the code

Concurrency bugs do not fail at compile time and often do not fail in tests. They fail under production load, intermittently, and often in a way that looks like something else (latency spikes, memory growth, one corrupted record in a million). AI assistants are good at producing concurrent code that *looks* correct. The scarce skill is now **judging** that code:

- Recognising a missing happens-before edge.
- Knowing which goroutine owns a channel and who is allowed to close it.
- Reading a goroutine dump or trace and mapping symptoms to mechanisms.
- Choosing between channel / mutex / atomic / no concurrency at all.

So this course puts mental models, internals, review drills and debugging ahead of boilerplate. Chapter 31 deals specifically with working alongside AI on concurrent code.

---

## Table of Contents

| # | File | Chapters |
|---|------|----------|
| I | [01-foundations.md](01-foundations.md) | 1. Concurrency vs Parallelism, Scaling Laws · 2. CSP, Actors & Shared Memory · 3. Hardware Reality · 4. The Go Memory Model |
| II | [02-runtime.md](02-runtime.md) | 5. The GMP Scheduler · 6. Preemption · 7. Syscalls, Netpoller & sysmon · 8. Goroutine Lifecycle & Stacks · 9. GC Interaction |
| III | [03-channels-and-select.md](03-channels-and-select.md) | 10. Channel Internals & Semantics · 11. `select` |
| IV | [04-sync-primitives.md](04-sync-primitives.md) | 12. Mutex & RWMutex · 13. WaitGroup, Once, Cond · 14. Pool & sync.Map · 15. Atomics & Lock-Free |
| V | [05-context-and-time.md](05-context-and-time.md) | 16. `context` in Depth · 17. Timers & Tickers |
| VI | [06-patterns.md](06-patterns.md) | 18. Pipelines & Fan-out/in · 19. Bounded Parallelism · 20. Backpressure & Rate Limiting · 21. singleflight & Coalescing · 22. Ownership, Actors, Pub/Sub · 23. Structured Concurrency · 24. Generics, Iterators & Concurrency |
| VII | [07-failure-modes-and-debugging.md](07-failure-modes-and-debugging.md) | 25. Failure Taxonomy · 26. Tooling: race detector, pprof, trace · 27. Testing Concurrent Code (`synctest`) |
| VIII | [08-performance-and-production.md](08-performance-and-production.md) | 28. Contention Engineering · 29. GOMAXPROCS, Containers & Tail Latency · 30. Production Resilience |
| IX | [09-ai-assisted-concurrency.md](09-ai-assisted-concurrency.md) | 31. Working with AI on Concurrent Code |
| X | [10-capstones.md](10-capstones.md) | 3 capstone projects with reference designs |
| XI | [11-mastery-drills.md](11-mastery-drills.md) | Code-review gauntlet · System-design drills |
| — | [appendices.md](appendices.md) | Decision matrix · Cheat sheets · Glossary · Reading list |

---

## Chapter template

Every chapter has the same sections, so you can skim or dive in:

| Section | What you get |
|---|---|
| **TL;DR** | The points you should be able to recall cold |
| **Mental Model** | The core idea, often as a diagram |
| **Under the Hood** | Runtime and source-level mechanics |
| **Practical Code** | Idiomatic, production-shaped examples |
| **Pitfalls** | Broken code → why → fix |
| **IRL Scenario** | A realistic incident or design problem worked through |
| **Review Drill** | Plausible-looking code with a hidden bug, often the kind AI generates |
| **Exercises** | ⭐ (warm-up) → ⭐⭐⭐ (hard). Solutions are in collapsible blocks |
| **Review Checklist** | What to check in code and design review |

### Callout legend

> 💡 **Insight** a non-obvious idea worth internalising.

> ⚠️ **Pitfall** a common way to get it wrong.

> 🔬 **Internals** runtime and implementation detail. Correct for current Go, but not a spec guarantee.

> 🤖 **AI note** a pattern AI assistants frequently get wrong.

---

## How to use this course

1. **Read Part I fully.** The memory model chapter underpins everything else. If you only read one chapter, read Chapter 4.
2. **Read Parts II–V with the runtime source open.** Files worth having in a tab: `runtime/proc.go`, `runtime/chan.go`, `runtime/select.go`, `internal/sync/mutex.go` (sync.Mutex since 1.24), `context/context.go`. Browse at <https://cs.opensource.google/go/go>.
3. **Run every example with `-race`.** Make it a habit: `go test -race ./...`, `go run -race .`.
4. **Do the review drills before reading their answers.** That is where most of the learning happens.
5. **Build at least one capstone.** Then break it on purpose and diagnose it with pprof and trace.

### Setup

```bash
go version          # 1.25 or newer recommended
go install golang.org/x/tools/cmd/stress@latest   # stress-run flaky tests
go install golang.org/x/perf/cmd/benchstat@latest # compare benchmarks properly
```

Run examples:

```bash
go run -race main.go
go test -race -count=1 -run TestX ./...
stress -p 8 ./pkg.test -test.run TestFlaky   # after: go test -c -race -o pkg.test
```
