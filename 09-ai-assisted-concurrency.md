# Part IX: Working with AI on Concurrent Code

[← Part VIII](08-performance-and-production.md) · [Index](README.md) · [Next: Part X, Capstones →](10-capstones.md)

- [Chapter 31: AI-Assisted Concurrent Development](#chapter-31-ai-assisted-concurrent-development)
  - [Why AI struggles with concurrency](#why-ai-struggles-with-concurrency)
  - [The catalogue: 20 common AI concurrency mistakes](#the-catalogue-20-common-ai-concurrency-mistakes)
  - [Prompting for correct concurrent code](#prompting-for-correct-concurrent-code)
  - [The verification workflow](#the-verification-workflow)
  - [Worked example: reviewing an AI-generated worker pool](#worked-example-reviewing-an-ai-generated-worker-pool)
  - [Where AI is useful](#where-ai-is-useful)

---

## Chapter 31: AI-Assisted Concurrent Development

### TL;DR

- AI is good at producing concurrent code that **compiles, looks idiomatic, and passes naive tests**. Concurrency bugs live exactly in the gap between "looks right" and "is right".
- Your value is in **specifying** (ownership, lifecycle, bounds, error and cancellation policy) and **verifying** (happens-before reasoning, tools, adversarial tests).
- Use AI as a **fast drafter, test generator, and adversarial reviewer**. Don't treat it as the authority on correctness or runtime internals. Verify claims against source and tools.

---

### Why AI struggles with concurrency

1. **Correctness is non-local.** Whether a line is a race depends on *every other access* to that memory, anywhere in the program, and on the synchronisation between them. Generators work locally, a snippet at a time.
2. **The training data is full of subtly wrong examples.** Blog posts, Stack Overflow answers and tutorials often skip cancellation, leak goroutines, or use `time.Sleep` for sync. They "work" in a demo.
3. **Outdated idioms.** Models mix eras: `v := v` loop copies (unnecessary since 1.22), `if !t.Stop() { <-t.C }` drains (unnecessary since 1.23 semantics), `ioutil`, manual `wg.Add` (vs `wg.Go` in 1.25), `atomic.AddInt64` on plain fields (vs typed atomics).
4. **Tests confirm the happy interleaving.** A generated test with `time.Sleep(100ms)` passes on the author's machine and verifies nothing about other interleavings.
5. **Confident explanations.** Ask an AI how `sync.Mutex` starvation mode works and you'll get a fluent answer. It may be right, out of date, or invented. Runtime internals should be checked against the source.

---

### The catalogue: 20 common AI concurrency mistakes

| # | Mistake | Why it's wrong | Fix | See |
|---|---|---|---|---|
| 1 | `go func() { ... }()` with no stop mechanism | Leak; ignores shutdown; errors vanish | errgroup / Run(ctx) / tracked background runner | Ch 23 |
| 2 | Sends without `case <-ctx.Done()` | Leak when the consumer leaves | Select on ctx in every blocking send | Ch 18 |
| 3 | `wg.Add(1)` inside the goroutine | `Wait` may return early | `Add` before `go`, or `wg.Go` | Ch 13 |
| 4 | Workers each `defer close(results)` | Panic: close of closed channel | Single closer after `wg.Wait()` | Ch 10 |
| 5 | Unbounded `go` per item | Memory and downstream overload | `errgroup.SetLimit`, semaphore | Ch 19 |
| 6 | Write under `RLock` (counters, LRU updates) | Data race | Atomic or write lock | Ch 4, 12 |
| 7 | Value receiver on a struct with a mutex | Locks a copy | Pointer receivers; vet copylocks | Ch 12 |
| 8 | I/O or channel send while holding a mutex | Convoys, deadlocks | Snapshot, unlock, then act | Ch 12, 25 |
| 9 | `bool` flag for stop/ready | Data race; hoisted loads | `atomic.Bool`, channel close, ctx | Ch 4 |
| 10 | `time.Sleep` for synchronisation (code or tests) | No happens-before; flaky | Channels, WaitGroup; synctest in tests | Ch 4, 27 |
| 11 | Check-then-act with separate atomics | Over-admission | Single CAS loop | Ch 15 |
| 12 | Per-field atomics for multi-field invariants | Torn invariants | One mutex or `atomic.Pointer` to an immutable struct | Ch 15 |
| 13 | Assumes `select` case order = priority | Order is random | Nested select / explicit check | Ch 11 |
| 14 | `r.Context()` in a background goroutine | Cancelled when handler returns | `WithoutCancel` + timeout + tracking | Ch 16 |
| 15 | `context.Background()` deep in the call chain | Cuts cancellation and deadlines | Thread ctx through | Ch 16 |
| 16 | Missing `defer cancel()` | Timers and registrations linger | Always cancel; vet lostcancel | Ch 16 |
| 17 | Huge channel buffers "to avoid blocking" | Hides backpressure; latency; data loss | Small buffers + explicit overload policy | Ch 10, 20 |
| 18 | `sync.Map` as the default concurrent map | Untyped; usually not faster | `map` + `Mutex`, or sharded | Ch 14 |
| 19 | Retries without jitter, budget or idempotency | Retry storms, duplicate side effects | Backoff + jitter + budget + idempotency keys | Ch 30 |
| 20 | Shutdown = `srv.Shutdown(ctx)` only | 502s on deploy, killed work | Full shutdown sequence | Ch 30 |

---

### Prompting for correct concurrent code

Vague prompts get demo-quality code. **State the concurrency contract.** A template:

```text
Write a Go 1.25 function/component that <does X>.

Concurrency contract:
- Bounds: at most <N> concurrent <operations>; derive from <downstream limit>.
- Ownership: <who creates, sends, receives, closes each channel / owns each piece of state>.
- Cancellation: accepts ctx; every blocking operation must select on ctx.Done().
- Lifecycle: no goroutine may outlive the call (use errgroup), OR expose Run(ctx) error / Close() that waits.
- Errors: <fail-fast and cancel siblings | collect all with errors.Join>.
- Overload policy: when input exceeds capacity, <block with ctx | drop and count | reject with error>.
- Ordering: results <must | need not> preserve input order.
- Use modern APIs: wg.Go, typed atomics, context causes, errgroup.SetLimit. No time.Sleep for sync.

Also write:
- Tests using testing/synctest for timing behaviour, goleak.VerifyNone, and a high-contention test
  (start barrier, 64 goroutines) to run under -race.
- A short comment block listing each goroutine and how it terminates.
```

The last line is worth having on its own. Asking for **"a list of every goroutine and how it terminates"** forces the model (and you) to reason about lifecycle, and it makes review much faster.

#### Prompts for adversarial review

```text
Act as an adversarial reviewer. For the code below:
1. List every goroutine, what it blocks on, and every way it can terminate.
2. For each shared variable, name the happens-before edge between writer and reader, or flag a race.
3. Construct a concrete interleaving (step by step, goroutine by goroutine) that causes
   a deadlock, leak, panic, or wrong result, if one exists.
4. What happens when: ctx is cancelled mid-operation; the consumer stops reading;
   an operation returns an error; input is empty; input is huge?
Do not suggest style changes. Only correctness and resource issues.
```

Requiring a **concrete interleaving** filters out vague "this might be a race" answers and gives you something you can verify or turn into a test.

---

### The verification workflow

```
1. READ with the five questions        (minutes; catches most bugs)
2. STATIC: go vet, staticcheck         (seconds)
3. DYNAMIC: -race + contention tests   (CI)
4. DETERMINISTIC: synctest + goleak    (CI)
5. LOAD: profile under realistic load  (before production)
```

#### 1. The five questions (do these for every concurrent diff)

1. **Lifecycle:** for each `go` statement, what stops it, and who waits for it?
2. **Ownership:** for each channel, who sends, who receives, who closes? For each shared variable, what protects it?
3. **Happens-before:** for each cross-goroutine data flow, which sync operation orders the write before the read?
4. **Bounds:** what limits goroutines, memory, queue length and queue *time*?
5. **Failure paths:** what happens on error, on cancellation, on a slow or vanished consumer, and on panic?

If the code (or the AI's explanation) can't answer one of these clearly, that's where the bug is.

#### 2. Static analysis

```bash
go vet ./...            # copylocks, lostcancel, waitgroup (1.25+), loopclosure, ...
staticcheck ./...       # SA2000 (wg.Add in goroutine), SA6002 (Pool non-pointer), etc.
```

#### 3–4. Dynamic and deterministic testing

Ask the AI to generate these tests too, then review them for sleeps and for missing contention:
- `-race` with a start barrier and many goroutines.
- `testing/synctest` for every timeout, retry, TTL and ticker path.
- `goleak.VerifyNone(t)` in every test of a component that starts goroutines.
- Early-exit tests: cancel mid-way, consumer stops after 1 item, error on item 1, empty input.

#### 5. Load and profile

Run at production-like `GOMAXPROCS`. Check mutex and block profiles, goroutine count stability, and a trace snapshot.

---

### Worked example: reviewing an AI-generated worker pool

**Prompt given:** "Write a Go worker pool that processes URLs concurrently and returns results."

**Typical generated code:**

```go
type Result struct {
	URL    string
	Status int
	Err    error
}

func ProcessURLs(urls []string, workers int) []Result {
	jobs := make(chan string, len(urls))
	results := make(chan Result, len(urls))
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		go func() {
			wg.Add(1)                                     // (A)
			defer wg.Done()
			for url := range jobs {
				resp, err := http.Get(url)                // (B)
				if err != nil {
					results <- Result{URL: url, Err: err}
					continue
				}
				results <- Result{URL: url, Status: resp.StatusCode} // (C)
			}
		}()
	}

	for _, u := range urls {
		jobs <- u
	}
	close(jobs)

	go func() {
		time.Sleep(100 * time.Millisecond)               // (D)
		wg.Wait()
		close(results)
	}()

	var out []Result
	for r := range results {
		out = append(out, r)
	}
	return out
}
```

**Review:**

| Mark | Issue | Severity |
|---|---|---|
| (A) | `wg.Add` inside the goroutine. `Wait` can run before any `Add`, close `results` early, and the next send panics: "send on closed channel". The `Sleep` at (D) *hides* this most of the time. `go vet` (1.25+) flags it: "WaitGroup.Add called from inside new goroutine". | 🔴 crash |
| (B) | `http.Get` uses `DefaultClient`: **no timeout**. One hanging server hangs a worker forever, and with all workers hung, the function never returns. No ctx either. | 🔴 hang |
| (C) | `resp.Body` is never closed. That leaks connections and file descriptors, and the transport can't reuse connections. | 🔴 resource leak |
| (D) | `time.Sleep` as synchronisation. Not a happens-before edge, and it masks (A). | 🟠 |
| — | No cancellation: the caller can't abort. | 🟠 |
| — | Results are unordered, which may or may not be OK (unspecified). | 🟡 |
| — | `len(urls)` buffers: memory is O(n) twice over. Fine for small n, bad for streaming. | 🟡 |

**Fixed version** (with the contract made explicit):

```go
// ProcessURLs fetches each URL with at most `workers` concurrent requests.
// Results are returned in input order. Cancelling ctx aborts outstanding requests.
// Goroutines: one per in-flight URL (bounded by errgroup limit); all have exited when this returns.
func ProcessURLs(ctx context.Context, client *http.Client, urls []string, workers int) []Result {
	out := make([]Result, len(urls))
	g := new(errgroup.Group) // collect-all: one URL failing doesn't cancel others
	g.SetLimit(workers)

	for i, u := range urls {
		if ctx.Err() != nil {
			out[i] = Result{URL: u, Err: ctx.Err()}
			continue
		}
		g.Go(func() error {
			out[i] = fetch(ctx, client, u) // distinct index per goroutine: no race
			return nil
		})
	}
	g.Wait()
	return out
}

func fetch(ctx context.Context, client *http.Client, url string) Result {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Result{URL: url, Err: err}
	}
	resp, err := client.Do(req)
	if err != nil {
		return Result{URL: url, Err: err}
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20)) // drain (bounded) to allow conn reuse
	return Result{URL: url, Status: resp.StatusCode}
}
```

Note what disappeared: both channels, the WaitGroup, the closer goroutine, and the sleep. **The best concurrent code is often the code with the fewest concurrency primitives.**

**Tests to require:**

```go
func TestProcessURLs_Cancel(t *testing.T) {
	defer goleak.VerifyNone(t)
	hang := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done() // hang until client gives up
	}))
	defer hang.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(50 * time.Millisecond); cancel() }() // real network: synctest not applicable

	res := ProcessURLs(ctx, hang.Client(), []string{hang.URL, hang.URL, hang.URL}, 2)
	for _, r := range res {
		if r.Err == nil {
			t.Fatalf("expected error for %s", r.URL)
		}
	}
}
```

(That test uses real sockets, so it can't use `synctest` and needs a small real sleep. This is one of the legitimate exceptions, because the test only needs to cancel *at some point*, not at a precise interleaving.)

---

### Where AI is useful

| Task | How to use AI | Guardrail |
|---|---|---|
| **Drafting** components with a written contract | Use the prompt template above | The five questions; tools |
| **Generating tests** (synctest, goleak, contention) | Ask explicitly for each type | Reject tests that sleep, or that never run concurrently |
| **Adversarial review** | The interleaving prompt | Turn each claimed interleaving into a test that reproduces it |
| **Explaining goroutine dumps and traces** | Paste the aggregated dump (`debug=1`); ask for a wait-for graph | Verify against code. Dumps are ground truth, explanations are hypotheses. |
| **Exploring alternatives** | "Show three designs: channel pipeline, errgroup, mutex-protected, with trade-offs" | You choose using Ch 2's decision guide |
| **Migrating idioms** | "Modernise to Go 1.25: wg.Go, typed atomics, remove loop var copies, timer drains" | Re-run tests with `-race`; check the `go` line of the module being migrated (loop vars) and of the main module (GODEBUG-based defaults: timers, GOMAXPROCS) |
| **Runtime internals Q&A** | Quick orientation | **Verify against `$(go env GOROOT)/src`.** Internals change between releases. |

> 💡 **Insight:** The most reliable way to use AI on concurrency is to make it produce **checkable artifacts**: a goroutine inventory, a concrete interleaving, a failing test. Fluent prose about "thread safety" can't be checked, so don't rely on it.

### Exercises

1. ⭐ Take any concurrent function an AI wrote for you recently. Answer the five questions for it in writing.
2. ⭐⭐ Prompt an assistant for "a concurrent cache with TTL" with and without the contract template. Diff the results against the catalogue above.
3. ⭐⭐⭐ Use the adversarial-review prompt on the Review Drills from earlier chapters (without the answers). Score how many bugs it finds and how many false positives it raises, and turn each true positive into a failing test.

### Review Checklist (for AI-assisted PRs)

- [ ] Does the PR description (or code comment) include a goroutine inventory with termination conditions?
- [ ] Were the five questions answered by a human reviewer?
- [ ] Does the change use modern idioms (`wg.Go`, typed atomics, ctx causes) and no outdated ones?
- [ ] Do tests include synctest/goleak/contention tests, with no sleeps for sync?
- [ ] Were any runtime-behaviour claims in comments verified against source or docs?

---

[← Part VIII](08-performance-and-production.md) · [Index](README.md) · [Next: Part X, Capstones →](10-capstones.md)
