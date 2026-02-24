// Package prefetch provides SQLite-specific readahead and scan detection on
// top of the generic [pagefault] and [sqlitebtree] packages.
//
// The [ReadaheadEngine] detects scans via two-consecutive-sibling accesses
// under the same B-tree parent (analogous to Linux kernel ondemand_readahead),
// then prefetches remaining siblings. It also cascades overflow page chains
// from leaf table pages. It implements [pagefault.FetchObserver] and delegates
// B-tree structure tracking to [sqlitebtree.Tracker].
package prefetch
