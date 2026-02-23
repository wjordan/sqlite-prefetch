# sqlite-prefetch

A Go library for intelligently prefetching [SQLite database pages](https://www.sqlite.org/fileformat2.html#pages) from remote storage (S3, peer nodes, or any custom backend).

## The problem

SQLite stores data in fixed-size pages organized as [B-trees](https://www.sqlite.org/fileformat2.html#b_tree_pages). When the database lives on remote storage (object stores, distributed caches), every page fault becomes a network round-trip. A simple table scan that touches hundreds of pages serializes into hundreds of sequential fetches, each blocked on network latency.

Traditional prefetchers use sequential or stride heuristics to guess what pages come next. These work for flat files but break down on SQLite databases, where B-tree traversal jumps between non-contiguous pages and [VACUUM](https://www.sqlite.org/lang_vacuum.html)-fragmented databases have no predictable layout on disk.

## The approach

SQLite's own [B-tree interior pages](https://www.sqlite.org/fileformat2.html#b_tree_pages) already contain the answer: each interior page stores an array of child page pointers (typically ~450 per 4KB page). By parsing these pages as they're fetched, we can predict exactly which leaf pages will be needed next — no heuristics required. A single interior page hit yields all of its children, converging queries to 1-2 blocking faults per B-tree level regardless of fragmentation.

The library combines this B-tree prediction with scan detection, multi-source scheduling, and peer routing into a composable set of components:

| Component | Role |
|---|---|
| **Prefetcher** | Entry point. Deduplicates concurrent faults so parallel readahead and application reads share a single in-flight fetch per page. |
| **Scheduler** | Ranks multiple storage backends by estimated cost (`latency + size/bandwidth`) and issues [hedged requests](https://research.google/pubs/the-tail-at-scale/) to cut tail latency. |
| **ReadaheadEngine** | Detects scans via two-consecutive-sibling accesses under the same B-tree parent, then prefetches remaining siblings. Point selects (single child access) trigger no prefetch. Delegates prediction to the B-tree tracker and overflow chain prefetcher. |
| **btreeTracker** | Parses [interior B-tree pages](https://www.sqlite.org/fileformat2.html#b_tree_pages) (table `0x05` and index `0x02`) for exact child-page prediction, with multi-level lookahead to stay ahead of SQLite's descent. |
| **Overflow prefetcher** | Parses [leaf table pages](https://www.sqlite.org/fileformat2.html#b_tree_pages) (`0x0D`) to find first overflow page numbers, then cascades through the linked-list chain via next-page pointers. |
| **AvailabilityIndex** | Tracks which pages each peer has cached as [roaring64 bitmaps](https://github.com/RoaringBitmap/roaring) over a logical address space. Enables exact routing with zero convergence time. |

Each component is optional. You can use just the `Prefetcher` for deduplication, add the `Scheduler` for multi-source fetching, or enable the full stack for autonomous B-tree-aware prefetching.

## Install

```
go get github.com/wjordan/sqlite-prefetch
```

## Usage

```go
import "github.com/wjordan/sqlite-prefetch"

// Implement these interfaces for your storage layer.
var cache prefetch.PageCache   // Get/Put/Has for cached pages
var source prefetch.PageSource // GetPage from your backend (S3, disk, etc.)

// Create the prefetcher.
p := prefetch.New(source, cache)

// Optional: multi-source scheduling with hedged requests.
sched := prefetch.NewScheduler(prefetch.SchedulerConfig{})
sched.SetSources([]prefetch.Source{s3Source, peerSource})
p.SetScheduler(sched)

// Optional: readahead with B-tree awareness.
re := prefetch.NewReadaheadEngine(p, cache, prefetch.ReadaheadConfig{})
p.SetReadahead(re)

// Fetch pages — dedup, readahead, and B-tree prediction happen automatically.
data, err := p.GetPage(ctx, pageNo)
```

## Interfaces

Integrate with your storage layer by implementing two interfaces:

```go
// PageSource fetches page data from storage.
type PageSource interface {
    GetPage(ctx context.Context, pageNo int64) ([]byte, error)
}

// PageCache reads and writes cached pages.
type PageCache interface {
    Get(pageNo int64) ([]byte, bool)
    CopyTo(pageNo int64, dst []byte) (int, bool)
    Put(pageNo int64, data []byte)
    PutPrefetched(pageNo int64, data []byte) // tracks waste on eviction
    Has(pageNo int64) bool
}
```

For multi-source scheduling, implement `Source`:

```go
type Source interface {
    Name() string
    Latency() time.Duration
    Bandwidth() float64
    Completeness() float64
    EgressCost() float64
    HasPage(pageNo int64) bool
    GetPage(ctx context.Context, pageNo int64) ([]byte, error)
}
```

For peer-to-peer fetching, implement `PeerTransport`:

```go
type PeerTransport interface {
    OpenStream(ctx context.Context, addr string) (io.ReadWriteCloser, error)
}
```

## How it works

### B-tree prediction

SQLite interior pages (table flag `0x05`, index flag `0x02`) contain an array of child page pointers — typically ~450 children per 4KB page. Both page types share identical child-pointer layout. When the prefetcher fetches an interior page, the `btreeTracker` parses it and records the parent-to-child mapping. On subsequent leaf page accesses, `Predict()` returns all remaining siblings, letting the engine prefetch them before SQLite asks.

This covers both table scans and covering index scans (e.g., `SELECT indexed_col FROM t WHERE indexed_col BETWEEN x AND y`), where SQLite reads only from the index B-tree.

When remaining siblings drop below a threshold, multi-level lookahead prefetches the *next interior sibling* so its children are parsed before SQLite descends into them.

See: [SQLite file format: Interior Pages of Table B-Trees](https://www.sqlite.org/fileformat2.html#b_tree_pages)

### Scan detection

The `ReadaheadEngine` gates prefetch behind scan detection, analogous to the Linux kernel's `ondemand_readahead`. B-tree predictions are exact (interior pages encode their children), but prefetching all siblings on a single child access would be wasteful for point selects — a large interior page may have ~450 children, and a point select only needs one.

The detection is simple: when two consecutive sibling accesses occur under the same parent (child[i] followed by child[i+1]), a scan is detected and the remaining siblings are prefetched. This works on both cache misses (`GetPage`) and cache hits (`NotifyPageRead`), so the engine can detect scans even when pages are already warm.

Single-child accesses (point selects, random lookups) never trigger prefetch.

### Overflow chain prefetching

When a row's payload exceeds the [local storage limit](https://www.sqlite.org/fileformat2.html#ovflpgs), SQLite stores the excess on overflow pages — a linked list where each page's first 4 bytes point to the next. These pages aren't children of any interior page, so the B-tree tracker can't predict them.

The overflow prefetcher handles this by parsing leaf table pages (`0x0D`) when they're fetched, extracting the first overflow page number from each cell that exceeds `maxLocal`. Those pages are prefetched immediately. When an overflow page arrives, its next-page pointer is read and the next page in the chain is prefetched, cascading through the entire chain without blocking on SQLite's sequential reads.

### Hedged requests

The `Scheduler` implements [hedged requests](https://research.google/pubs/the-tail-at-scale/) from Google's "The Tail at Scale" (Dean & Barroso, 2013). It starts the best source immediately and fires a fallback after `1.5x best_latency`. The first successful response wins; the other is cancelled.

### Peer availability gossip

Peers exchange page availability using [roaring64 bitmaps](https://github.com/RoaringBitmap/roaring) over physical page numbers. The `AvailabilityIndex` stores one roaring64 bitmap per peer; `LocalAvailability` maintains a single bitmap for the local node. Peer removal is O(1) (delete the bitmap).

Exchange uses QUIC streams (reliable full snapshots on peer join, periodic resync) and datagrams (unreliable 7-byte deltas on cache changes). Since availability is known upfront, routing achieves 100% hit rate immediately — no convergence period.

## References

- [SQLite Database File Format](https://www.sqlite.org/fileformat2.html) -- page layout, B-tree structure, interior page format, overflow pages
- [The Tail at Scale](https://research.google/pubs/the-tail-at-scale/) (Dean & Barroso, 2013) -- hedged requests for tail latency

## Dependencies

- [roaring](https://github.com/RoaringBitmap/roaring) (Apache-2.0) — compressed bitmap data structure for peer availability tracking

## License

Apache 2.0
