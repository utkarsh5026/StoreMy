// Package lock implements page-level Two-Phase Locking (2PL) for StoreMy's
// concurrency control layer.
//
// # Overview
//
// The package enforces the standard 2PL protocol: a transaction acquires all
// locks it needs during the growing phase and releases them all at once during
// commit or abort (the shrinking phase). Locks are never released mid-transaction.
//
// Two lock modes are supported:
//
//   - [SharedLock]    — required to read a page; compatible with other shared locks.
//   - [ExclusiveLock] — required to write a page; incompatible with all other locks.
//
// A transaction holding a shared lock may upgrade it to exclusive ([LockManager.LockPage]
// with exclusive=true) provided no other transaction holds any lock on that page.
// Downgrading (exclusive → shared) is never permitted.
//
// # Components
//
// [LockManager] is the single public entry point. Callers use [LockManager.LockPage]
// to acquire locks and [LockManager.UnlockPage] / [LockManager.UnlockAllPages] to
// release them. Internally it coordinates four subsystems:
//
//   - [LockTable]       — dual-index tracking which pages each transaction holds
//     locks on, and which transactions hold locks on each page.
//   - [WaitQueue]       — per-page FIFO queues of pending [LockRequest] entries for
//     transactions that cannot be granted a lock immediately.
//   - [DependencyGraph] — directed wait-for graph used for deadlock detection. An edge
//     A→B means transaction A is waiting for a resource held by B. A cycle in this
//     graph indicates a deadlock.
//   - [LockGrantor]     — stateless logic for evaluating whether a lock can be granted
//     immediately or upgraded, and for performing the actual grant.
//
// # Lock Acquisition Flow
//
// When [LockManager.LockPage] is called:
//
//  1. If the transaction already holds a sufficient lock, return immediately.
//  2. If the lock can be granted without conflict, grant it and return.
//  3. If upgrading S→X is possible (sole holder), perform the upgrade and return.
//  4. Otherwise, enqueue the request in the [WaitQueue] and record wait-for edges
//     in the [DependencyGraph].
//  5. Run cycle detection on the dependency graph — if a cycle is found, remove the
//     request, clean up the graph, and return a deadlock error immediately.
//  6. Sleep with exponential backoff and retry from step 2. After the retry limit is
//     exhausted, return a timeout error.
//
// # Deadlock Detection
//
// [DependencyGraph.HasCycle] uses iterative depth-first search over the wait-for graph.
// The result is cached and invalidated on every structural change (edge addition or
// transaction removal). Detection runs inside the lock manager mutex, guaranteeing that
// the graph is checked before the caller blocks — satisfying the Deadlock Gate invariant.
//
// # Invariants
//
// The package upholds the following concurrency invariants (see CLAUDE.md for the
// full list):
//
//   - A transaction must hold a [SharedLock] to read a page and an [ExclusiveLock]
//     to write it.
//   - Locks may be upgraded S→X but never downgraded X→S.
//   - All locks are released only at commit or abort, never mid-transaction.
//   - Deadlock detection runs before the caller is put to wait; a detected cycle
//     returns an error immediately so the caller can abort and retry.
package lock
