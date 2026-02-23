// Package prefetch provides SQLite-specific readahead, scan detection, and
// peer routing on top of the generic [pagefault] and [sqlitebtree] packages.
//
// The [ReadaheadEngine] detects scans via two-consecutive-sibling accesses
// under the same B-tree parent (analogous to Linux kernel ondemand_readahead),
// then prefetches remaining siblings. It implements [pagefault.FetchObserver]
// and delegates B-tree structure tracking to [sqlitebtree.Tracker].
//
// The [PeerRouter] learns which peer nodes cache which pages via fetch
// outcomes, using EWMA hit-rate tracking and B-tree sibling amplification.
//
// Source wrappers ([S3Source], [PeerSource]) implement [pagefault.Source]
// with EWMA-tracked latency and bandwidth.
package prefetch
