// Package prefetch provides SQLite-specific readahead, scan detection, and
// peer availability tracking on top of the generic [pagefault] and [sqlitebtree]
// packages.
//
// The [ReadaheadEngine] detects scans via two-consecutive-sibling accesses
// under the same B-tree parent (analogous to Linux kernel ondemand_readahead),
// then prefetches remaining siblings. It implements [pagefault.FetchObserver]
// and delegates B-tree structure tracking to [sqlitebtree.Tracker].
//
// The [AvailabilityIndex] tracks which pages remote peers have cached as
// roaring64 bitmaps over physical page numbers. [LocalAvailability] maintains
// a single bitmap for the local node. Peers exchange availability via reliable
// snapshots and unreliable single-page deltas through [AvailabilityGossip].
//
// Source wrappers ([S3Source], [PeerSource]) implement [pagefault.Source]
// with EWMA-tracked latency and bandwidth.
package prefetch
