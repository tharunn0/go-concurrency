# Part X: Capstone Projects

[← Part IX](09-ai-assisted-concurrency.md) · [Index](README.md) · [Next: Part XI, Mastery Drills →](11-mastery-drills.md)

- [Capstone 1: Concurrent Rate-Limited Web Crawler](#capstone-1-concurrent-rate-limited-web-crawler)
- [Capstone 2: Sharded TTL+LRU Cache with Request Coalescing](#capstone-2-sharded-ttllru-cache-with-request-coalescing)
- [Capstone 3: Job Scheduler with Priorities, Retries and Graceful Shutdown](#capstone-3-job-scheduler-with-priorities-retries-and-graceful-shutdown)

> ⚠️ The reference implementations skip config validation to stay short. Production versions should reject `MaxInFlight`/`Workers`/`Shards` ≤ 0, `TTL` ≤ 0 and `LoadTimeout` ≤ 0, and the scheduler's `Run` may be called only once.

**How to do these:**
1. Read the requirements and **design it yourself first**: write the goroutine inventory and ownership table *before* reading the reference design.
2. Implement it. Test with `-race`, `synctest`, and `goleak`.
3. Compare with the reference. Differences are fine if you can defend them.
4. Do the **"Break it"** tasks: introduce each bug, and confirm your tests or tools catch it.

Dependencies: `golang.org/x/sync`, `golang.org/x/time`, `go.uber.org/goleak` (tests).

---

## Capstone 1: Concurrent Rate-Limited Web Crawler

### Requirements

- `Crawl(ctx, seed) (map[string]error, error)`: crawl pages reachable from `seed`, on the same host, up to `MaxDepth`.
- At most `MaxInFlight` concurrent fetches overall, and at most `PerHostRPS` requests/s per host.
- Each URL is fetched **at most once**.
- Returns when the crawl is **complete** (no pending or in-flight work) or ctx is cancelled. **No goroutine outlives the call.**
- Fetch errors are recorded per URL. They don't abort the crawl.

### The hard part: termination detection

Workers discover new work. When is the crawl done? "The queue is empty" isn't enough, because an in-flight fetch may add more. You need **queue empty AND in-flight == 0**, evaluated atomically with respect to new discoveries.

**Design options:**

| Option | Termination | Trade-off |
|---|---|---|
| A. Shared `visited` map + mutex, workers enqueue to channel, WaitGroup counts pending | `wg.Add` per discovered URL *before* enqueue; closer waits | Workers sending to a queue they also read from can deadlock if it's bounded |
| B. **Coordinator goroutine owns all state**; workers are stateless and report back | Coordinator checks `len(queue)==0 && inflight==0` | Single coordinator, but it only does map ops, which is very cheap compared with HTTP |

The reference uses **B**. Confinement removes every lock, and termination becomes a trivial local check.

### Goroutine inventory

| Goroutine | Blocks on | Terminates when |
|---|---|---|
| Coordinator (the caller of `Crawl`) | `results`, `ctx.Done()` | queue empty and inflight == 0, or ctx done |
| Fetch worker (≤ MaxInFlight) | rate limiter wait, HTTP | fetch completes or ctx cancelled. Its send to buffered `results` never blocks. |

### Reference implementation

```go
package crawler

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sync"

	"golang.org/x/time/rate"
)

type Crawler struct {
	MaxDepth    int
	MaxInFlight int
	PerHostRPS  float64
	// Fetch returns the absolute links on a page. Defaults to an HTTP fetcher.
	Fetch func(ctx context.Context, pageURL string) ([]string, error)
}

type task struct {
	url   string
	depth int
}

type result struct {
	task  task
	links []string
	err   error
}

// Crawl visits pages reachable from seed on the same host.
// All goroutines it starts have exited when it returns.
func (c *Crawler) Crawl(ctx context.Context, seed string) (map[string]error, error) {
	seedURL, err := url.Parse(seed)
	if err != nil {
		return nil, err
	}
	fetch := c.Fetch
	if fetch == nil {
		fetch = HTTPFetcher(http.DefaultClient)
	}

	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer func() {
		cancel()  // first: tell workers to stop
		wg.Wait() // then: wait for them (they never block on send; results is buffered)
	}()

	// Coordinator-owned state: no locks needed.
	visited := map[string]error{}
	seen := map[string]bool{seed: true}
	queue := []task{{url: seed, depth: 0}}
	limiters := map[string]*rate.Limiter{}
	results := make(chan result, c.MaxInFlight) // capacity ≥ max in-flight ⇒ sends never block
	inflight := 0

	for len(queue) > 0 || inflight > 0 {
		// Dispatch as much as the concurrency bound allows.
		for len(queue) > 0 && inflight < c.MaxInFlight {
			t := queue[0]
			queue = queue[1:]
			host := hostOf(t.url)
			lim, ok := limiters[host]
			if !ok {
				lim = rate.NewLimiter(rate.Limit(c.PerHostRPS), 1)
				limiters[host] = lim
			}
			inflight++
			wg.Go(func() {
				if err := lim.Wait(ctx); err != nil {
					results <- result{task: t, err: err}
					return
				}
				links, err := fetch(ctx, t.url)
				results <- result{task: t, links: links, err: err}
			})
		}

		select {
		case r := <-results:
			inflight--
			visited[r.task.url] = r.err
			if r.err != nil || r.task.depth >= c.MaxDepth {
				continue
			}
			for _, l := range r.links {
				if seen[l] || hostOf(l) != seedURL.Host {
					continue
				}
				seen[l] = true
				queue = append(queue, task{url: l, depth: r.task.depth + 1})
			}
		case <-ctx.Done():
			return visited, context.Cause(ctx)
		}
	}
	return visited, nil
}

func hostOf(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return u.Host
}

var hrefRE = regexp.MustCompile(`href="([^"#]+)"`)

// HTTPFetcher is a simple link extractor. Real crawlers should parse HTML properly.
func HTTPFetcher(client *http.Client) func(context.Context, string) ([]string, error) {
	return func(ctx context.Context, pageURL string) ([]string, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, pageURL, nil)
		if err != nil {
			return nil, err
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("status %d", resp.StatusCode)
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
		if err != nil {
			return nil, err
		}
		base, _ := url.Parse(pageURL)
		var links []string
		for _, m := range hrefRE.FindAllSubmatch(body, -1) {
			ref, err := url.Parse(string(m[1]))
			if err != nil {
				continue
			}
			links = append(links, base.ResolveReference(ref).String())
		}
		return links, nil
	}
}
```

### Tests

```go
package crawler

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"go.uber.org/goleak"
)

// fakeWeb: page → links. Fetch takes 100ms of (fake) time.
func fakeFetch(web map[string][]string, inflight, peak *atomic.Int32) func(context.Context, string) ([]string, error) {
	return func(ctx context.Context, u string) ([]string, error) {
		n := inflight.Add(1)
		defer inflight.Add(-1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return web[u], nil
	}
}

func TestCrawl_BoundsAndCompleteness(t *testing.T) {
	defer goleak.VerifyNone(t)
	synctest.Test(t, func(t *testing.T) {
		web := map[string][]string{
			"http://a/":  {"http://a/1", "http://a/2", "http://a/3", "http://b/x"},
			"http://a/1": {"http://a/", "http://a/4"},
			"http://a/2": {"http://a/4"},
			"http://a/3": {}, "http://a/4": {},
		}
		var inflight, peak atomic.Int32
		c := &Crawler{MaxDepth: 5, MaxInFlight: 2, PerHostRPS: 1000, Fetch: fakeFetch(web, &inflight, &peak)}

		got, err := c.Crawl(context.Background(), "http://a/")
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 5 { // a/, 1, 2, 3, 4 (b/x is off-host)
			t.Fatalf("visited %d pages: %v", len(got), got)
		}
		if peak.Load() > 2 {
			t.Fatalf("peak in-flight %d > 2", peak.Load())
		}
	})
}

func TestCrawl_Cancel(t *testing.T) {
	defer goleak.VerifyNone(t)
	synctest.Test(t, func(t *testing.T) {
		// Infinite web: every page links to two new pages.
		var n atomic.Int64
		fetch := func(ctx context.Context, u string) ([]string, error) {
			select {
			case <-time.After(10 * time.Millisecond):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			a, b := n.Add(1), n.Add(1)
			return []string{fmtURL(a), fmtURL(b)}, nil
		}
		c := &Crawler{MaxDepth: 1 << 30, MaxInFlight: 8, PerHostRPS: 1e6, Fetch: fetch}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := c.Crawl(ctx, "http://a/0")
		if err == nil {
			t.Fatal("expected cancellation error")
		}
		// goleak (deferred) verifies all workers exited.
	})
}

func fmtURL(i int64) string { return fmt.Sprintf("http://a/%d", i) }
```

### Break it (confirm your tests catch each)

1. Remove the `cancel()` from the deferred cleanup → **not caught**, and that's the lesson. It's currently redundant: the only early return happens after the *parent* ctx is done, so workers are already cancelled. Keep it anyway (for `lostcancel`, and for future early-return paths). Add a `MaxPages` limit that returns early while fetches are in flight: now removing `cancel()` leaks blocked workers, and the deferred `wg.Wait()` hangs (synctest reports a deadlock).
2. Make `results` unbuffered and return early on ctx → the deferred `wg.Wait()` deadlocks (workers block sending). synctest reports a deadlock.
3. Move `seen[l] = true` to when the result comes back instead of when it's enqueued → duplicate fetches. Add an assertion on the fetch count per URL.

### Extensions

- **Per-host head-of-line blocking:** a slow host's rate-limit wait occupies global in-flight slots. Fix it with per-host queues, dispatching only hosts whose limiter has a token ready (peek with `lim.Tokens() >= 1`. Note that `Reserve()` *consumes* a token, so a Reserve-based check must `Cancel()` when the delay is > 0). This only matters once you allow cross-host crawling: the reference follows same-host links only, so there is a single limiter.
- **robots.txt** with per-host singleflight-cached fetch.
- **Politeness under 429:** a per-host adaptive limiter (AIMD, Chapter 19).

---

## Capstone 2: Sharded TTL+LRU Cache with Request Coalescing

### Requirements

- `Cache[V]` with `Get(ctx, key) (V, error)`: on a miss, call `loader` **once per key across concurrent callers**, with the per-caller ctx respected.
- Entries expire after `TTL`. Each shard holds at most `PerShardCap` entries (LRU eviction).
- Scales across cores: sharded, with no global lock on the hot path.
- A background janitor removes expired entries. `Close()` stops it and **waits**.
- Stats: hits, misses, loads, evictions.

### Design decisions

| Decision | Choice | Why |
|---|---|---|
| Lock type per shard | `sync.Mutex`, **not** `RWMutex` | LRU `Get` mutates the recency list, so every Get is a write. `RLock` would be a data race (Chapter 12). |
| Coalescing | `singleflight.DoChan` + `WithoutCancel` + timeout | Per-caller cancellation; shared work bounded independently (Chapter 21) |
| Janitor | One goroutine, scans shards one at a time | Holds only one shard lock at a time. Scales with shard count. |
| Stats | `atomic.Int64` | Independent counters; consistency across them not required |
| Loaded values | Treated as immutable | Shared across waiters |

### Goroutine inventory

| Goroutine | Terminates when |
|---|---|
| Janitor | `Close()` closes `stop`; `Close` waits on `done` |
| singleflight execution (per in-flight key) | loader returns (bounded by `LoadTimeout`) |

### Reference implementation

```go
package cache

import (
	"container/list"
	"context"
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

type Config[V any] struct {
	Shards      int
	PerShardCap int
	TTL         time.Duration
	LoadTimeout time.Duration
	Loader      func(ctx context.Context, key string) (V, error)
}

type Stats struct{ Hits, Misses, Loads, Evictions int64 }

type Cache[V any] struct {
	cfg    Config[V]
	seed   maphash.Seed
	shards []*shard[V]
	group  singleflight.Group

	hits, misses, loads, evictions atomic.Int64

	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

type shard[V any] struct {
	mu    sync.Mutex
	items map[string]*list.Element // → *entry[V]
	lru   *list.List               // front = most recent
}

type entry[V any] struct {
	key     string
	val     V
	expires time.Time
}

func New[V any](cfg Config[V]) *Cache[V] {
	c := &Cache[V]{
		cfg:    cfg,
		seed:   maphash.MakeSeed(),
		shards: make([]*shard[V], cfg.Shards),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	for i := range c.shards {
		c.shards[i] = &shard[V]{items: make(map[string]*list.Element), lru: list.New()}
	}
	go c.janitor()
	return c
}

func (c *Cache[V]) shardFor(key string) *shard[V] {
	return c.shards[maphash.String(c.seed, key)%uint64(len(c.shards))]
}

// lookup returns a fresh value and promotes it in the LRU.
func (c *Cache[V]) lookup(key string) (V, bool) {
	s := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	if el, ok := s.items[key]; ok {
		e := el.Value.(*entry[V])
		if time.Now().Before(e.expires) {
			s.lru.MoveToFront(el)
			return e.val, true
		}
		s.lru.Remove(el)
		delete(s.items, key)
	}
	var zero V
	return zero, false
}

func (c *Cache[V]) Set(key string, v V) {
	s := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	exp := time.Now().Add(c.cfg.TTL)
	if el, ok := s.items[key]; ok {
		e := el.Value.(*entry[V])
		e.val, e.expires = v, exp
		s.lru.MoveToFront(el)
		return
	}
	s.items[key] = s.lru.PushFront(&entry[V]{key: key, val: v, expires: exp})
	for s.lru.Len() > c.cfg.PerShardCap {
		oldest := s.lru.Back()
		s.lru.Remove(oldest)
		delete(s.items, oldest.Value.(*entry[V]).key)
		c.evictions.Add(1)
	}
}

func (c *Cache[V]) Get(ctx context.Context, key string) (V, error) {
	if v, ok := c.lookup(key); ok {
		c.hits.Add(1)
		return v, nil
	}
	c.misses.Add(1)

	ch := c.group.DoChan(key, func() (any, error) {
		// Re-check: another flight may have just filled it.
		if v, ok := c.lookup(key); ok {
			return v, nil
		}
		c.loads.Add(1)
		lctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.cfg.LoadTimeout)
		defer cancel()
		v, err := c.cfg.Loader(lctx, key)
		if err != nil {
			return v, err // not cached: next caller retries
		}
		c.Set(key, v)
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
	case <-ctx.Done():
		var zero V
		return zero, context.Cause(ctx)
	}
}

func (c *Cache[V]) janitor() {
	defer close(c.done)
	t := time.NewTicker(c.cfg.TTL / 2)
	defer t.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-t.C:
			for _, s := range c.shards { // one shard lock at a time
				now := time.Now()
				s.mu.Lock()
				for k, el := range s.items {
					if !now.Before(el.Value.(*entry[V]).expires) {
						s.lru.Remove(el)
						delete(s.items, k)
					}
				}
				s.mu.Unlock()
			}
		}
	}
}

// Close stops the janitor and waits for it. Safe to call multiple times.
func (c *Cache[V]) Close() {
	c.closeOnce.Do(func() { close(c.stop) })
	<-c.done
}

func (c *Cache[V]) Stats() Stats {
	return Stats{c.hits.Load(), c.misses.Load(), c.loads.Load(), c.evictions.Load()}
}
```

### Tests

```go
func TestCoalescing(t *testing.T) {
	defer goleak.VerifyNone(t)
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		release := make(chan struct{})
		c := New(Config[string]{
			Shards: 8, PerShardCap: 100, TTL: time.Minute, LoadTimeout: 5 * time.Second,
			Loader: func(ctx context.Context, k string) (string, error) {
				calls.Add(1)
				<-release
				return "v:" + k, nil
			},
		})
		defer c.Close()

		var wg sync.WaitGroup
		for range 100 {
			wg.Go(func() {
				v, err := c.Get(context.Background(), "k")
				if err != nil || v != "v:k" {
					t.Errorf("got %q, %v", v, err)
				}
			})
		}
		synctest.Wait() // all 100 are now durably blocked waiting on the one flight
		close(release)
		wg.Wait()
		if n := calls.Load(); n != 1 {
			t.Fatalf("loader called %d times, want 1", n)
		}
	})
}

func TestTTL(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := New(Config[int]{Shards: 1, PerShardCap: 10, TTL: time.Minute, LoadTimeout: time.Second,
			Loader: func(context.Context, string) (int, error) { return 0, errors.New("no") }})
		defer c.Close()
		c.Set("a", 1)
		time.Sleep(59 * time.Second)
		if _, ok := c.lookup("a"); !ok {
			t.Fatal("expired early")
		}
		time.Sleep(2 * time.Second)
		if _, ok := c.lookup("a"); ok {
			t.Fatal("not expired")
		}
	})
}

func TestConcurrentHits(t *testing.T) {
	c := New(Config[int]{Shards: 4, PerShardCap: 100, TTL: time.Minute, LoadTimeout: time.Second,
		Loader: func(context.Context, string) (int, error) { return 0, errors.New("no") }})
	defer c.Close()
	keys := []string{"a", "b", "c", "d", "e"}
	for i, k := range keys {
		c.Set(k, i)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for i := range 1000 {
				if _, err := c.Get(context.Background(), keys[i%len(keys)]); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait() // run with -race: hits mutate the LRU list concurrently
}

func TestCallerCancelDoesNotFailOthers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		c := New(Config[int]{Shards: 1, PerShardCap: 10, TTL: time.Minute, LoadTimeout: time.Hour,
			Loader: func(ctx context.Context, _ string) (int, error) {
				select {
				case <-release:
					return 42, nil
				case <-ctx.Done():
					return 0, ctx.Err()
				}
			}})
		defer c.Close()

		ctx1, cancel1 := context.WithCancel(context.Background())
		errc := make(chan error, 1)
		go func() { _, err := c.Get(ctx1, "k"); errc <- err }() // first caller starts the flight
		synctest.Wait()

		resc := make(chan int, 1)
		go func() { v, _ := c.Get(context.Background(), "k"); resc <- v }()
		synctest.Wait()

		cancel1() // first caller gives up...
		if err := <-errc; !errors.Is(err, context.Canceled) {
			t.Fatalf("caller1: %v", err)
		}
		close(release) // ...but the shared load continues for caller 2
		if v := <-resc; v != 42 {
			t.Fatalf("caller2 got %d", v)
		}
	})
}
```

### Break it

1. Change the shard lock to `RWMutex` and use `RLock` in `lookup` → `-race` flags `MoveToFront`, **but only with a concurrent-hit test** (`TestConcurrentHits` below). The other tests only miss. A good reminder that the race detector only sees what your tests exercise.
2. Use `c.group.Do` with the caller's ctx in the loader → `TestCallerCancelDoesNotFailOthers` fails.
3. Make `Close` not wait for `done` → **no supplied test catches it**: the janitor exits promptly once `stop` closes, and synctest waits for bubble goroutines anyway. This class of bug needs a "nothing runs after Close returns" assertion. For example, give the janitor a test hook and assert it isn't called after `Close`.

### Extensions

- **Stale-while-revalidate** and **XFetch** early refresh (Chapter 21).
- **Hot-key detection**: per-key hit counters with a count-min sketch, and replicate hot entries across shards.
- **Memory bound** by bytes (a weighted LRU) instead of by count.
- Benchmark against `map+Mutex`, `sync.Map`, and the shard count {1, 8, 64, 256} at `-cpu=1,8,64`.

---

## Capstone 3: Job Scheduler with Priorities, Retries and Graceful Shutdown

### Requirements

- `Submit(ctx, Job) error`: enqueue a job with a priority (higher runs first) and `MaxAttempts`.
- At most `Workers` jobs run concurrently.
- A failed job is retried with exponential backoff and jitter (a **delayed** re-enqueue that doesn't block a worker) until `MaxAttempts` is reached.
- `Run(ctx) error` runs the scheduler. When ctx is cancelled: stop accepting jobs and stop starting new ones, give in-flight jobs `Grace` to finish, then **hard-cancel** their contexts and wait for them to return.
- `Submit` after shutdown returns `ErrStopped`. No goroutine leaks.

### Design

A **single dispatcher goroutine** (inside `Run`) owns two heaps:
- `ready`: max-heap by (priority, FIFO sequence).
- `delayed`: min-heap by `runAt` (retries waiting for their backoff).

One reusable `time.Timer` is armed for the earliest delayed job. Workers are goroutines (≤ `Workers`) that report outcomes on a buffered channel. Because the dispatcher owns all scheduling state, there are no locks, and priority is strict among ready jobs (unlike biased `select`, Chapter 11).

```
Submit ──► submitCh ──┐
                      ▼
            ┌──── dispatcher (owns heaps) ────┐
 timer ────►│ delayed ─(due)─► ready ─(slot)─►│──► worker goroutines ──► outcomes ──┐
            └────────────▲────────────────────┘                                     │
                         └───────── retry (runAt = now + backoff) ◄─────────────────┘
```

### Goroutine inventory

| Goroutine | Terminates when |
|---|---|
| Dispatcher (`Run`) | ctx cancelled → drain phase → inflight == 0 |
| Job worker | job's `Run` returns (job ctx is hard-cancelled after Grace) |

### Reference implementation

```go
package sched

import (
	"container/heap"
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"
)

var ErrStopped = errors.New("scheduler stopped")

type Job struct {
	ID          string
	Priority    int
	MaxAttempts int
	Run         func(ctx context.Context) error
}

type Outcome struct {
	JobID    string
	Attempts int
	Err      error // nil on success
}

type Scheduler struct {
	Workers int
	Grace   time.Duration
	Backoff func(attempt int) time.Duration // optional; default exponential with ±50% jitter
	OnDone  func(Outcome)                   // optional; called from the dispatcher goroutine

	initOnce sync.Once
	submitCh chan *item
	stopped  chan struct{} // closed when Run stops accepting
}

type item struct {
	job     Job
	attempt int
	seq     uint64
	runAt   time.Time
	index   int
}

func (s *Scheduler) init() {
	s.initOnce.Do(func() {
		s.submitCh = make(chan *item)
		s.stopped = make(chan struct{})
		if s.Backoff == nil {
			s.Backoff = func(a int) time.Duration {
				d := 100 * time.Millisecond << min(a, 10)
				return rand.N(d) + d/2 // jittered, never zero
			}
		}
	})
}

// Submit enqueues a job. It blocks until the dispatcher accepts it, ctx is done, or the scheduler stops.
func (s *Scheduler) Submit(ctx context.Context, j Job) error {
	s.init()
	if j.MaxAttempts < 1 {
		j.MaxAttempts = 1
	}
	select {
	case s.submitCh <- &item{job: j}:
		return nil
	case <-s.stopped:
		return ErrStopped
	case <-ctx.Done():
		return ctx.Err()
	}
}

type outcome struct {
	it  *item
	err error
}

// Run dispatches jobs until ctx is cancelled, then drains in-flight jobs (see Grace).
func (s *Scheduler) Run(ctx context.Context) error {
	s.init()
	jobCtx, hardCancel := context.WithCancel(context.WithoutCancel(ctx)) // outlives ctx until grace expires
	defer hardCancel()

	var (
		ready    readyHeap
		delayed  delayHeap
		seq      uint64
		inflight int
		results  = make(chan outcome, s.Workers) // ≥ max in-flight: workers never block
		timer    = time.NewTimer(time.Hour)
		wg       sync.WaitGroup
	)
	timer.Stop()
	defer timer.Stop()
	defer wg.Wait() // runs before hardCancel (LIFO), after drain logic ensures inflight == 0

	finish := func(it *item, err error) {
		if s.OnDone != nil {
			s.OnDone(Outcome{JobID: it.job.ID, Attempts: it.attempt, Err: err})
		}
	}
	handle := func(o outcome, retry bool) {
		inflight--
		if o.err != nil && retry && o.it.attempt < o.it.job.MaxAttempts {
			o.it.runAt = time.Now().Add(s.Backoff(o.it.attempt))
			heap.Push(&delayed, o.it)
			return
		}
		finish(o.it, o.err)
	}

	for {
		// 0. Cancellation is checked before anything else, so a queued job can never
		//    start after ctx is cancelled (select alone is random and could pick results).
		if ctx.Err() != nil {
			close(s.stopped) // Submit now returns ErrStopped
			return s.drain(results, &inflight, hardCancel, func(o outcome) { handle(o, false) })
		}
		// 1. Promote due delayed jobs.
		now := time.Now()
		for delayed.Len() > 0 && !delayed[0].runAt.After(now) {
			it := heap.Pop(&delayed).(*item)
			heap.Push(&ready, it)
		}
		// 2. Start as many ready jobs as slots allow (strict priority).
		for ready.Len() > 0 && inflight < s.Workers {
			it := heap.Pop(&ready).(*item)
			it.attempt++
			inflight++
			wg.Go(func() {
				results <- outcome{it: it, err: safeRun(jobCtx, it.job.Run)}
			})
		}
		// 3. Arm the timer for the next delayed job.
		var timerC <-chan time.Time
		if delayed.Len() > 0 {
			timer.Reset(time.Until(delayed[0].runAt)) // Go 1.23+: no stale fires after Reset
			timerC = timer.C
		}

		select {
		case it := <-s.submitCh:
			seq++
			it.seq = seq
			heap.Push(&ready, it)
		case o := <-results:
			handle(o, true)
		case <-timerC:
		case <-ctx.Done():
			// handled at the top of the loop
		}
	}
}

// drain waits for in-flight jobs: Grace first, then hard-cancel and wait for the rest.
// Queued (not started) jobs are dropped; a production scheduler would persist them.
func (s *Scheduler) drain(results <-chan outcome, inflight *int, hardCancel context.CancelFunc, handle func(outcome)) error {
	grace := time.NewTimer(s.Grace)
	defer grace.Stop()
	for *inflight > 0 {
		select {
		case o := <-results:
			handle(o)
		case <-grace.C:
			hardCancel() // jobs see ctx cancellation; keep collecting until all return
		}
	}
	return nil
}

func safeRun(ctx context.Context, f func(context.Context) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("job panicked") // include r and debug.Stack() in real code
		}
	}()
	return f(ctx)
}

// readyHeap: highest priority first; FIFO among equal priority.
type readyHeap []*item

func (h readyHeap) Len() int { return len(h) }
func (h readyHeap) Less(i, j int) bool {
	if h[i].job.Priority != h[j].job.Priority {
		return h[i].job.Priority > h[j].job.Priority
	}
	return h[i].seq < h[j].seq
}
func (h readyHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i]; h[i].index = i; h[j].index = j }
func (h *readyHeap) Push(x any)   { it := x.(*item); it.index = len(*h); *h = append(*h, it) }
func (h *readyHeap) Pop() any {
	old := *h
	it := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	return it
}

// delayHeap: earliest runAt first.
type delayHeap []*item

func (h delayHeap) Len() int           { return len(h) }
func (h delayHeap) Less(i, j int) bool { return h[i].runAt.Before(h[j].runAt) }
func (h delayHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i]; h[i].index = i; h[j].index = j }
func (h *delayHeap) Push(x any)        { it := x.(*item); it.index = len(*h); *h = append(*h, it) }
func (h *delayHeap) Pop() any {
	old := *h
	it := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	return it
}
```

**Points worth checking in the reference:**
- **Defer order:** `wg.Wait` is deferred *after* `hardCancel`, so it runs *first*. That's safe only because `drain` guarantees `inflight == 0` before returning, so every worker has already sent its result. Convince yourself of that, or restructure to a single explicit cleanup.
- **`OnDone` runs on the dispatcher goroutine**, so a slow callback stalls scheduling. Document it (or deliver outcomes via a channel to the user).
- **Jobs ignoring ctx** block shutdown forever after the hard cancel. This is unavoidable in-process. The orchestrator's SIGKILL is the backstop.

### Tests

```go
func TestPriorityAndRetry(t *testing.T) {
	defer goleak.VerifyNone(t)
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		var order []string
		var outcomes []Outcome
		s := &Scheduler{
			Workers: 1, Grace: time.Second,
			Backoff: func(int) time.Duration { return time.Second }, // deterministic
			OnDone:  func(o Outcome) { mu.Lock(); outcomes = append(outcomes, o); mu.Unlock() },
		}
		ctx, cancel := context.WithCancel(context.Background())
		runErr := make(chan error, 1)
		go func() { runErr <- s.Run(ctx) }()

		gate := make(chan struct{})
		rec := func(id string) func(context.Context) error {
			return func(context.Context) error { mu.Lock(); order = append(order, id); mu.Unlock(); return nil }
		}
		// Occupy the single worker so the next submissions queue up.
		s.Submit(ctx, Job{ID: "block", Run: func(context.Context) error { <-gate; return nil }})
		s.Submit(ctx, Job{ID: "low", Priority: 1, Run: rec("low")})
		s.Submit(ctx, Job{ID: "high", Priority: 9, Run: rec("high")})
		fails := 0
		s.Submit(ctx, Job{ID: "flaky", Priority: 5, MaxAttempts: 3, Run: func(context.Context) error {
			fails++
			if fails < 3 {
				return errors.New("transient")
			}
			mu.Lock(); order = append(order, "flaky"); mu.Unlock()
			return nil
		}})
		close(gate)
		time.Sleep(10 * time.Second) // fake time: retries' backoffs elapse instantly
		synctest.Wait()

		mu.Lock()
		got := fmt.Sprint(order)
		mu.Unlock()
		if got != "[high low flaky]" { // flaky's 1st attempt fails after high; retries are delayed; low runs meanwhile
			t.Fatalf("order = %s", got)
		}
		cancel()
		if err := <-runErr; err != nil {
			t.Fatal(err)
		}
	})
}

func TestShutdownHardCancel(t *testing.T) {
	defer goleak.VerifyNone(t)
	synctest.Test(t, func(t *testing.T) {
		s := &Scheduler{Workers: 2, Grace: 5 * time.Second}
		ctx, cancel := context.WithCancel(context.Background())
		runErr := make(chan error, 1)
		go func() { runErr <- s.Run(ctx) }()

		var sawCancel atomic.Bool
		s.Submit(ctx, Job{ID: "stubborn", Run: func(jctx context.Context) error {
			<-jctx.Done() // only stops when hard-cancelled
			sawCancel.Store(true)
			return jctx.Err()
		}})
		synctest.Wait()

		start := time.Now()
		cancel()
		<-runErr
		if d := time.Since(start); d != 5*time.Second {
			t.Fatalf("drain took %v, want exactly the 5s grace", d)
		}
		if !sawCancel.Load() {
			t.Fatal("job not hard-cancelled")
		}
		if err := s.Submit(context.Background(), Job{ID: "late"}); !errors.Is(err, ErrStopped) {
			t.Fatalf("late submit: %v", err)
		}
	})
}
```

### Break it

1. Set `results` capacity to 0 → a worker finishing during `drain` still works (drain receives), but think it through: is there any return path where workers could block? (Answer: no while drain loops until inflight == 0. An early-return bug would deadlock in `wg.Wait`.)
2. Replace the heaps with a `select` over per-priority channels → the priority test becomes flaky (biased, not strict, Chapter 11).
3. Remove `close(s.stopped)` → late `Submit` blocks forever, and the test hangs (synctest reports a deadlock).

### Extensions

- **Persistence:** write-ahead log of submissions and outcomes, and re-enqueue unfinished jobs on restart (at-least-once semantics, so jobs must be idempotent).
- **Per-tenant fairness:** weighted fair queueing (deficit round-robin) across tenants instead of pure priority.
- **Concurrency per job type** (bulkheads), and **rate limits** per type.
- **Cron:** recurring jobs scheduled through the delayed heap, with jitter.

---

[← Part IX](09-ai-assisted-concurrency.md) · [Index](README.md) · [Next: Part XI, Mastery Drills →](11-mastery-drills.md)
