// Package prefetch provides a B-tree-aware multi-source page prefetcher for
// SQLite databases served from remote storage (S3, peers, or any [Source]).
//
// The core insight is that SQLite interior B-tree pages encode the exact page
// numbers of their children. By parsing these pages as they are fetched, the
// [btreeTracker] can predict ~100 upcoming leaf page accesses from a single
// interior page hit — far more effective than sequential/stride heuristics on
// fragmented databases.
//
// # Architecture
//
// The system has five cooperating components:
//
//   - [Prefetcher] — In-flight map deduplication so concurrent faults and
//     readahead share a single fetch.
//   - [Scheduler] — Multi-source cost-model ranking (estimatedTime = latency +
//     bytes/bandwidth) with hedged requests per Google's "The Tail at Scale".
//   - [ReadaheadEngine] — Scan detection via two-consecutive-sibling accesses
//     under the same B-tree parent (analogous to Linux kernel ondemand_readahead).
//     Point selects touch a single child and trigger no prefetch; scans touch
//     consecutive siblings and trigger prefetch of all remaining siblings.
//   - btreeTracker — Parses SQLite interior pages (flags 0x05 and 0x02) to predict
//     exactly which leaf pages will be needed next. Multi-level lookahead prefetches
//     the next interior sibling before it is needed.
//   - Overflow prefetcher — Parses leaf table pages (0x0D) to find overflow page
//     chains and cascades prefetches through the linked list.
//   - [PeerRouter] — Reactive peer routing with EWMA hit rates and B-tree
//     sibling amplification.
//
// # Quick start
//
//	cache := yourCacheImpl{}   // implements prefetch.PageCache
//	source := yourSourceImpl{} // implements prefetch.PageSource
//
//	p := prefetch.New(source, cache)
//
//	// Optional: add multi-source scheduling with hedged requests.
//	sched := prefetch.NewScheduler(prefetch.SchedulerConfig{})
//	sched.SetSources([]prefetch.Source{s3Source, peerSource})
//	p.SetScheduler(sched)
//
//	// Optional: add readahead with B-tree awareness.
//	re := prefetch.NewReadaheadEngine(p, cache, prefetch.ReadaheadConfig{})
//	p.SetReadahead(re)
//
//	// Fetch pages — readahead and deduplication happen automatically.
//	data, err := p.GetPage(ctx, pageNo)
package prefetch
