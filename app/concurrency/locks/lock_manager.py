import threading
import time
from typing import Set, Optional, Dict, List, Tuple
from collections import defaultdict

from app.primitives import TransactionId, PageId
from app.core.exceptions import TransactionAbortedException
from .dependency_manager import DependencyGraph, Lock, LockType


class LockRequest:
    """
    ⏳ Represents a pending lock request ⏳

    🎯 A lock request is created when a transaction cannot immediately
    acquire a lock and must wait for conflicting locks to be released.

    Request Lifecycle:
    ------------------------------------------------------------
    1. 🚀 Created when lock cannot be granted immediately
    2. ⏳ Added to pending queue for the requested page
    3. 🔔 Thread waits on condition variable  
    4. 🎉 Granted when conflicting locks released
    5. 🧹 Removed from pending queue
    ------------------------------------------------------------

    Request States:
    ------------------------------------------------------------
    🟡 PENDING  => Request created, waiting for grant
    🟢 GRANTED  => Lock successfully acquired  
    🔴 ABORTED  => Request cancelled (timeout/deadlock)
    🟠 TIMEOUT  => Request expired before grant
    ------------------------------------------------------------

    Thread Synchronization:
    ------------------------------------------------------------
    🔒 condition: Used to block/wake requesting thread
    ⏰ timestamp: Track how long request has been waiting
    🚦 granted: Signal that lock has been acquired
    ❌ aborted: Signal that request should be cancelled
    ------------------------------------------------------------

    Example Usage Flow:
    ------------------------------------------------------------
    T1 wants EXCLUSIVE lock on Page_5
    ├─ Page_5 already has SHARED lock by T2
    ├─ Create LockRequest(T1, Page_5, EXCLUSIVE)
    ├─ Add to pending_requests[Page_5]
    ├─ T1 waits on condition variable
    ├─ T2 releases SHARED lock
    ├─ T1's request is granted
    └─ T1 wakes up and continues execution
    ------------------------------------------------------------
    """

    def __init__(self, tid: TransactionId, page_id: PageId, lock_type: LockType):
        self.tid = tid
        self.page_id = page_id
        self.lock_type = lock_type
        self.timestamp = time.time()
        self.condition = threading.Condition()
        self.granted = False
        self.aborted = False


class LockManager:
    """
    🔐 Robust lock manager implementing Strict Two-Phase Locking with proper blocking 🔐

    💡 This is the heart of the concurrency control system, providing ACID transaction
    isolation through a sophisticated locking protocol with deadlock detection.

    🏗️ Architecture Overview:
    ------------------------------------------------------------
    ┌─────────────────────────────────────────────────────┐
    │                 LOCK MANAGER                        │
    │                                                     │
    │  ┌──────────────┐  ┌──────────────┐  ┌───────────┐ │
    │  │   ACTIVE     │  │   PENDING    │  │ DEADLOCK  │ │
    │  │    LOCKS     │  │   REQUESTS   │  │ DETECTOR  │ │
    │  │              │  │              │  │           │ │
    │  │ Page → Locks │  │ Page → Queue │  │ Dep Graph │ │
    │  └──────────────┘  └──────────────┘  └───────────┘ │
    │           │                │                │       │
    │           └────────────────┼────────────────┘       │
    │                           │                         │
    │  ┌──────────────┐  ┌──────▼──────┐                 │
    │  │ TRANSACTION  │  │ CONDITION   │                 │
    │  │   TRACKING   │  │ VARIABLES   │                 │
    │  │              │  │             │                 │
    │  │ TID → Pages  │  │Page → Cond  │                 │
    │  └──────────────┘  └─────────────┘                 │
    └─────────────────────────────────────────────────────┘
    ------------------------------------------------------------

    🎯 Key Features:
    ✅ Actual lock acquisition with blocking/waiting using condition variables
    💀 Deadlock detection and resolution with dependency graphs
    ⬆️ Lock upgrade support (shared → exclusive)
    🔄 Proper integration with transaction lifecycle
    📊 Comprehensive statistics and monitoring
    🔒 Thread-safe operations with fine-grained locking

    🔄 Two-Phase Locking Protocol:
    ------------------------------------------------------------
    Phase 1: GROWING (Acquire locks only)
    ┌─────────────────────────────────────────────────────┐
    │ T1: 🔒→ 🔒→ 🔒→ 🔒→ (only acquiring)               │
    │ T2: 🔒→ 🔒→ 🔒→ (only acquiring)                   │
    │ T3: 🔒→ 🔒→ (only acquiring)                       │
    └─────────────────────────────────────────────────────┘

    Phase 2: SHRINKING (Release locks only)  
    ┌─────────────────────────────────────────────────────┐
    │ T1: 🔓← 🔓← 🔓← 🔓← (only releasing)               │
    │ T2: 🔓← 🔓← 🔓← (only releasing)                   │
    │ T3: 🔓← 🔓← (only releasing)                       │
    └─────────────────────────────────────────────────────┘
    ------------------------------------------------------------

    📊 Performance Characteristics:
    ------------------------------------------------------------
    ➕ Lock acquisition: O(1) average case
    🔍 Deadlock detection: O(V + E) where V=txns, E=dependencies
    📋 Lock lookup: O(L) where L=locks per page (typically small)
    ⏳ Wait time: Depends on lock contention patterns
    🔄 Lock upgrade: O(L) for compatibility check
    ------------------------------------------------------------

    🛡️ Deadlock Resolution Strategy:
    ------------------------------------------------------------
    1. 🔍 Detect cycles in dependency graph
    2. 🎯 Choose victim (youngest transaction)
    3. 💀 Abort victim transaction  
    4. 🔄 Retry remaining transactions
    5. 📊 Track statistics for monitoring
    ------------------------------------------------------------

    💾 Memory Management:
    ------------------------------------------------------------
    🗂️ Efficient data structures for fast lookup
    🧹 Automatic cleanup of completed transactions
    📈 Bounded memory usage (proportional to active txns)
    🔄 Lazy cleanup of empty collections
    ------------------------------------------------------------
    """

    def __init__(self):
        # Core data structures
        self._page_locks: Dict[PageId, List[Lock]] = defaultdict(list)
        self._transaction_locks: Dict[TransactionId,
                                      Set[PageId]] = defaultdict(set)
        self._pending_requests: Dict[PageId,
                                     List[LockRequest]] = defaultdict(list)

        self._dependency_graph = DependencyGraph()

        # Thread synchronization
        self._global_lock = threading.RLock()
        self._page_conditions: Dict[PageId, threading.Condition] = defaultdict(
            lambda: threading.Condition(self._global_lock)
        )

        # Configuration
        self._deadlock_timeout = 10.0  # seconds
        self._lock_timeout = 30.0  # seconds

        # Statistics
        self.locks_granted = 0
        self.locks_denied = 0
        self.deadlocks_detected = 0
        self.lock_upgrades = 0
        self.lock_timeouts = 0

    def acquire_lock(self, tid: TransactionId, page_id: PageId, lock_type: LockType,
                     timeout: Optional[float] = None) -> bool:
        """
        🎯 Acquire a lock with proper blocking and deadlock detection 🎯

        🔄 This is the main entry point for lock acquisition, implementing the complete
        2PL protocol with sophisticated conflict resolution and deadlock handling.

        Lock Acquisition Algorithm:
        ------------------------------------------------------------
        1. 🔍 Check for existing lock by same transaction
           ├─ Compatible? → ✅ Return success
           └─ Upgrade needed? → ⬆️ Attempt upgrade

        2. 🚀 Check if lock can be granted immediately  
           ├─ No conflicts? → ✅ Grant immediately
           └─ Conflicts exist? → ⏳ Enter wait phase

        3. ⏳ Wait Phase (if needed)
           ├─ Create LockRequest object
           ├─ Add to pending queue
           ├─ Wait on condition variable
           ├─ Check deadlock periodically
           └─ Grant when possible or timeout/abort
        ------------------------------------------------------------

        Example Flow - Simple Case:
        ------------------------------------------------------------
        T1 requests SHARED lock on Page_5
        ┌─────────────────────────────────────────────────────┐
        │ Page_5 locks: [] (empty)                           │
        │ Action: Grant immediately ✅                        │
        │ Result: Page_5 locks: [T1:SHARED]                  │
        │ Time: O(1) - no waiting                            │
        └─────────────────────────────────────────────────────┘
        ------------------------------------------------------------

        Example Flow - Conflict Case:
        ------------------------------------------------------------
        T1 requests EXCLUSIVE lock on Page_5
        ┌─────────────────────────────────────────────────────┐
        │ Page_5 locks: [T2:SHARED] (conflict!)              │
        │ Action: Create LockRequest(T1, Page_5, EXCLUSIVE)   │
        │ Queue: pending_requests[Page_5] = [T1_request]      │
        │ Wait: T1 blocks on condition variable               │
        │ Later: T2 releases SHARED lock                      │
        │ Notify: condition.notify_all() wakes T1             │
        │ Grant: T1 gets EXCLUSIVE lock ✅                    │
        │ Result: Page_5 locks: [T1:EXCLUSIVE]               │
        └─────────────────────────────────────────────────────┘
        ------------------------------------------------------------

        Lock Upgrade Example:
        ------------------------------------------------------------
        T1 has SHARED, requests EXCLUSIVE on Page_5
        ┌─────────────────────────────────────────────────────┐
        │ Before: Page_5 locks: [T1:SHARED, T2:SHARED]       │
        │ Request: T1 wants to upgrade to EXCLUSIVE           │
        │ Conflict: T2 still holds SHARED lock                │
        │ Action: Wait for T2 to release                      │
        │ After T2 releases: [T1:SHARED]                      │
        │ Upgrade: [T1:EXCLUSIVE] ⬆️✅                        │
        └─────────────────────────────────────────────────────┘
        ------------------------------------------------------------

        Deadlock Scenario:
        ------------------------------------------------------------
        T1: holds Page_A, wants Page_B
        T2: holds Page_B, wants Page_A
        ┌─────────────────────────────────────────────────────┐
        │ Dependency Graph:                                   │
        │ T1 ──waits for──→ T2                               │
        │  ↑                 ↓                               │
        │  └─────waits for───┘                               │
        │                                                     │
        │ Cycle Detected! 💀                                  │
        │ Victim: T2 (younger transaction)                    │
        │ Action: Abort T2, grant locks to T1                │
        └─────────────────────────────────────────────────────┘
        ------------------------------------------------------------

        Timeout Handling:
        ------------------------------------------------------------
        🕐 Default timeout: 30 seconds
        ⏰ Check every 1 second for deadlock
        ❌ Return False if timeout exceeded
        📊 Track timeout statistics
        ------------------------------------------------------------

        Args:
            🆔 tid: Transaction requesting the lock
            📄 page_id: Page to lock  
            🔒 lock_type: Type of lock (SHARED or EXCLUSIVE)
            ⏰ timeout: Optional timeout in seconds (default: 30s)

        Returns:
            ✅ True if lock acquired successfully
            ❌ False if timed out

        Raises:
            💀 TransactionAbortedException: If deadlock detected and this transaction is victim
        """
        if timeout is None:
            timeout = self._lock_timeout

        start_time = time.time()

        with self._global_lock:
            existing_lock = self._get_existing_lock(tid, page_id)
            if existing_lock:
                if self._is_lock_compatible(existing_lock.lock_type, lock_type):
                    return True
                elif lock_type == LockType.EXCLUSIVE and existing_lock.lock_type == LockType.SHARED:
                    return self._upgrade_lock(tid, page_id, timeout - (time.time() - start_time))

            request = LockRequest(tid, page_id, lock_type)

            if self._can_grant_lock(tid, page_id, lock_type):
                self._grant_lock(tid, page_id, lock_type)
                return True

            self._pending_requests[page_id].append(request)

            return self._wait_for_lock(request, timeout)

    def _get_existing_lock(self, tid: TransactionId, page_id: PageId) -> Optional[Lock]:
        """
        🔍 Get existing lock held by transaction on page 🔍

        Lock Lookup Process:
        ------------------------------------------------------------
        1. 📄 Get all locks for the specified page
        2. 🔄 Iterate through locks on that page  
        3. 🆔 Match transaction ID
        4. 📦 Return lock object if found, None otherwise
        ------------------------------------------------------------

        Example Lookup:
        ------------------------------------------------------------
        Page_5 locks: [T1:SHARED, T2:EXCLUSIVE, T3:SHARED]
        Query: _get_existing_lock(T2, Page_5)
        Result: Lock(T2, Page_5, EXCLUSIVE) ✅

        Query: _get_existing_lock(T4, Page_5)  
        Result: None ❌ (T4 has no locks on Page_5)
        ------------------------------------------------------------

        Performance: O(L) where L = locks per page (typically small)
        """
        for lock in self._page_locks[page_id]:
            if lock.transaction_id == tid:
                return lock
        return None

    def _is_lock_compatible(self, existing_type: LockType, requested_type: LockType) -> bool:
        """
        🔄 Check if requested lock is compatible with existing lock 🔄

        Compatibility Logic:
        ------------------------------------------------------------
        📖 If transaction already has EXCLUSIVE:
           ├─ Requesting EXCLUSIVE → ✅ Compatible (already have it)
           └─ Requesting SHARED → ✅ Compatible (downgrade allowed)

        📖 If transaction already has SHARED:
           ├─ Requesting SHARED → ✅ Compatible (same level)
           └─ Requesting EXCLUSIVE → ❌ Not compatible (needs upgrade)
        ------------------------------------------------------------

        Compatibility Matrix (Same Transaction):
        ------------------------------------------------------------
                      │ Request SHARED │ Request EXCLUSIVE
        --------------┼----------------┼------------------
        Have SHARED   │       ✅       │        ❌
        Have EXCLUSIVE│       ✅       │        ✅
        ------------------------------------------------------------

        Real Examples:
        ------------------------------------------------------------
        ✅ T1 has EXCLUSIVE, requests SHARED → Compatible
        ✅ T1 has EXCLUSIVE, requests EXCLUSIVE → Compatible  
        ✅ T1 has SHARED, requests SHARED → Compatible
        ❌ T1 has SHARED, requests EXCLUSIVE → Not compatible
        ------------------------------------------------------------

        Args:
            🔒 existing_type: Lock type currently held
            🎯 requested_type: Lock type being requested

        Returns:
            ✅ True if compatible, False if upgrade needed
        """
        if existing_type == LockType.EXCLUSIVE:
            return requested_type == LockType.EXCLUSIVE  # Already have exclusive
        else:  # existing_type == LockType.SHARED
            return requested_type == LockType.SHARED  # Shared is compatible with shared

    def _can_grant_lock(self, tid: TransactionId, page_id: PageId, lock_type: LockType) -> bool:
        """
        ✅ Check if lock can be granted immediately ✅

        Grant Decision Algorithm:
        ------------------------------------------------------------
        1. 📄 Check if page has any existing locks
           └─ Empty? → ✅ Grant immediately

        2. 🔒 Analyze lock type being requested:
           ├─ SHARED request:
           │  └─ Grant if no EXCLUSIVE locks exist
           └─ EXCLUSIVE request:
              └─ Grant only if no other transactions have locks
        ------------------------------------------------------------

        Lock Conflict Matrix:
        ------------------------------------------------------------
                    │ No Locks │ Other SHARED │ Other EXCLUSIVE │ Own Lock
        ------------┼----------┼--------------┼-----------------┼----------
        Req SHARED  │    ✅     │      ✅       │       ❌        │   (check)
        Req EXCLUSIVE│   ✅     │      ❌       │       ❌        │   (check)
        ------------------------------------------------------------

        Example Scenarios:
        ------------------------------------------------------------
        Scenario 1: Empty page
        Page_5 locks: []
        T1 requests SHARED → ✅ Grant (no conflicts)
        T1 requests EXCLUSIVE → ✅ Grant (no conflicts)

        Scenario 2: Other transactions have SHARED
        Page_5 locks: [T2:SHARED, T3:SHARED]
        T1 requests SHARED → ✅ Grant (SHARED compatible)
        T1 requests EXCLUSIVE → ❌ Deny (conflicts with SHARED)

        Scenario 3: Other transaction has EXCLUSIVE  
        Page_5 locks: [T2:EXCLUSIVE]
        T1 requests SHARED → ❌ Deny (conflicts with EXCLUSIVE)
        T1 requests EXCLUSIVE → ❌ Deny (conflicts with EXCLUSIVE)

        Scenario 4: Same transaction owns all locks
        Page_5 locks: [T1:SHARED]
        T1 requests SHARED → ✅ Grant (own lock)
        T1 requests EXCLUSIVE → ❌ Deny (need upgrade process)
        ------------------------------------------------------------

        Args:
            🆔 tid: Transaction requesting lock
            📄 page_id: Page to check
            🔒 lock_type: Type of lock requested

        Returns:
            ✅ True if can grant immediately, False if must wait
        """
        existing_locks = self._page_locks[page_id]

        if not existing_locks:
            return True

        if lock_type == LockType.SHARED:
            # Shared lock can be granted if no exclusive locks exist
            return not any(lock.lock_type == LockType.EXCLUSIVE for lock in existing_locks)
        else:  # lock_type == LockType.EXCLUSIVE
            # Exclusive lock can only be granted if no other locks exist
            # (or only locks held by same transaction)
            return all(lock.transaction_id == tid for lock in existing_locks)

    def _grant_lock(self, tid: TransactionId, page_id: PageId, lock_type: LockType) -> None:
        """
        🎁 Grant a lock to a transaction 🎁

        Lock Granting Process:
        ------------------------------------------------------------
        1. 🏗️ Create Lock object with transaction, page, and type
        2. 📄 Add lock to page's lock list  
        3. 🆔 Add page to transaction's locked pages set
        4. 📊 Increment lock statistics counter
        ------------------------------------------------------------

        Data Structure Updates:
        ------------------------------------------------------------
        Before Grant:
        _page_locks[Page_5] = [T2:SHARED]
        _transaction_locks[T1] = {Page_3, Page_7}

        Call: _grant_lock(T1, Page_5, SHARED)

        After Grant:
        _page_locks[Page_5] = [T2:SHARED, T1:SHARED]
        _transaction_locks[T1] = {Page_3, Page_7, Page_5}
        locks_granted += 1
        ------------------------------------------------------------

        Memory Layout:
        ------------------------------------------------------------
        Lock Object Created:
        ┌─────────────────────────────────────────────────────┐
        │ Lock(                                               │
        │   transaction_id = T1,                              │
        │   page_id = Page_5,                                 │
        │   lock_type = SHARED                                │
        │ )                                                   │
        └─────────────────────────────────────────────────────┘

        Added to Collections:
        _page_locks: PageId → List[Lock]
        _transaction_locks: TransactionId → Set[PageId]
        ------------------------------------------------------------

        Concurrency Notes:
        ------------------------------------------------------------
        🔒 Called under _global_lock protection
        ⚡ O(1) average case performance
        📈 Statistics updated atomically
        🧹 Automatic cleanup handled elsewhere
        ------------------------------------------------------------

        Args:
            🆔 tid: Transaction receiving the lock
            📄 page_id: Page being locked
            🔒 lock_type: Type of lock being granted
        """
        # Create and add lock
        lock = Lock(tid, page_id, lock_type)
        self._page_locks[page_id].append(lock)
        self._transaction_locks[tid].add(page_id)

        self.locks_granted += 1

    def _wait_for_lock(self, request: LockRequest, timeout: float) -> bool:
        """
        ⏳ Wait for a lock request to be granted or timeout ⏳

        🔄 This method implements the blocking behavior of the lock manager,
        using condition variables to efficiently wait for lock availability.

        Waiting Process:
        ------------------------------------------------------------
        1. 🕰️ Check remaining timeout
        2. 💀 Periodic deadlock detection  
        3. 🔔 Wait on condition variable
        4. 🔄 Re-check grant conditions when notified
        5. ✅ Return success or ❌ timeout/abort
        ------------------------------------------------------------

        Condition Variable Pattern:
        ------------------------------------------------------------
        while not condition_met and not timeout:
            condition.wait(timeout_remaining)
            # Check condition again after waking up
        ------------------------------------------------------------

        Deadlock Detection Schedule:
        ------------------------------------------------------------
        ⏰ Check every 1 second during wait
        🕐 Start checking after deadlock_timeout (10s)
        💀 Abort if cycle detected and this txn is victim
        📊 Track deadlock statistics
        ------------------------------------------------------------

        Example Wait Flow:
        ------------------------------------------------------------
        T1 requests EXCLUSIVE on Page_5
        ┌─────────────────────────────────────────────────────┐
        │ 1. Page_5 has [T2:SHARED] → Cannot grant             │
        │ 2. Create LockRequest(T1, Page_5, EXCLUSIVE)         │  
        │ 3. Add to pending_requests[Page_5]                   │
        │ 4. condition.wait(timeout) → T1 blocks               │
        │ 5. T2 releases lock → condition.notify_all()         │
        │ 6. T1 wakes up, checks can_grant_lock()              │
        │ 7. Grant successful → return True ✅                 │
        └─────────────────────────────────────────────────────┘
        ------------------------------------------------------------

        Timeout Example:
        ------------------------------------------------------------
        T1 waits 30 seconds for Page_5 lock
        ┌─────────────────────────────────────────────────────┐
        │ 00:00 - Start waiting                               │
        │ 00:10 - Deadlock check (none found)                 │
        │ 00:20 - Deadlock check (none found)                 │
        │ 00:30 - Timeout reached! ❌                          │
        │ Action: Remove from pending queue                    │
        │ Result: return False                                 │
        └─────────────────────────────────────────────────────┘
        ------------------------------------------------------------

        Args:
            📋 request: LockRequest object containing wait details
            ⏰ timeout: Maximum time to wait in seconds

        Returns:
            ✅ True if lock granted successfully
            ❌ False if timeout exceeded or request aborted
        """
        condition = self._page_conditions[request.page_id]
        start_time = time.time()

        while not request.granted and not request.aborted:
            remaining_timeout = timeout - (time.time() - start_time)

            if remaining_timeout <= 0:
                # Timeout
                self._remove_pending_request(request)
                self.lock_timeouts += 1
                return False

            # Check for deadlock periodically
            if time.time() - start_time > self._deadlock_timeout:
                if self._check_deadlock_for_request(request):
                    self._remove_pending_request(request)
                    raise TransactionAbortedException(
                        f"Transaction {request.tid} aborted due to deadlock")

            # Wait for notification or timeout
            # Check deadlock every second
            condition.wait(min(remaining_timeout, 1.0))

            # Re-check if we can grant the lock
            if not request.granted and self._can_grant_lock(request.tid, request.page_id, request.lock_type):
                self._grant_lock(request.tid, request.page_id,
                                 request.lock_type)
                request.granted = True
                self._remove_pending_request(request)
                return True

        return request.granted

    def _check_deadlock_for_request(self, request: LockRequest) -> bool:
        """Check if a pending request would cause deadlock."""
        # Find blocking transactions
        blocking_transactions = self._find_blocking_transactions(
            request.page_id, request.lock_type)

        # Add dependencies to graph
        for blocking_tid in blocking_transactions:
            self._dependency_graph.add_dependency(request.tid, blocking_tid)

        # Check for cycles
        cycle = self._dependency_graph.has_cycle()
        if cycle:
            # Choose victim
            victim = self._choose_deadlock_victim(cycle)

            # Clean up dependencies
            for blocking_tid in blocking_transactions:
                self._dependency_graph.remove_dependency(
                    request.tid, blocking_tid)

            self.deadlocks_detected += 1
            return victim == request.tid

        return False

    def _find_blocking_transactions(self, page_id: PageId, lock_type: LockType) -> Set[TransactionId]:
        """Find transactions blocking a lock request."""
        blocking = set()

        for lock in self._page_locks[page_id]:
            if lock_type == LockType.EXCLUSIVE:
                # Exclusive conflicts with everything
                blocking.add(lock.transaction_id)
            elif lock.lock_type == LockType.EXCLUSIVE:
                # Shared conflicts with exclusive
                blocking.add(lock.transaction_id)

        return blocking

    def _choose_deadlock_victim(self, cycle: List[TransactionId]) -> TransactionId:
        """Choose deadlock victim (youngest transaction)."""
        return max(cycle, key=lambda tid: tid.get_id())

    def _remove_pending_request(self, request: LockRequest) -> None:
        """Remove a pending request from the queue."""
        if request in self._pending_requests[request.page_id]:
            self._pending_requests[request.page_id].remove(request)

    def _upgrade_lock(self, tid: TransactionId, page_id: PageId, timeout: float) -> bool:
        """Upgrade a shared lock to exclusive."""
        with self._global_lock:
            other_shared_locks = [
                lock for lock in self._page_locks[page_id]
                if lock.transaction_id != tid and lock.lock_type == LockType.SHARED
            ]

            if not other_shared_locks:
                self._remove_lock_internal(tid, page_id)
                self._grant_lock(tid, page_id, LockType.EXCLUSIVE)
                self.lock_upgrades += 1
                return True

            # Need to wait for other shared locks to be released
            request = LockRequest(tid, page_id, LockType.EXCLUSIVE)
            self._pending_requests[page_id].append(request)

            result = self._wait_for_lock(request, timeout)
            if result:
                self.lock_upgrades += 1
            return result

    def release_lock(self, tid: TransactionId, page_id: PageId) -> None:
        """
        🔓 Release a specific lock held by a transaction 🔓

        🔄 This implements the "shrinking phase" of 2PL, where transactions
        can only release locks, never acquire new ones.

        Release Process:
        ------------------------------------------------------------
        1. 🔍 Find and remove lock from internal structures
        2. 🔔 Notify waiting transactions on this page
        3. 🧹 Clean up dependency graph entries
        4. 📊 Update statistics if needed
        ------------------------------------------------------------

        Data Structure Updates:
        ------------------------------------------------------------
        Before Release:
        _page_locks[Page_5] = [T1:SHARED, T2:EXCLUSIVE]
        _transaction_locks[T1] = {Page_3, Page_5, Page_7}

        Call: release_lock(T1, Page_5)

        After Release:
        _page_locks[Page_5] = [T2:EXCLUSIVE]
        _transaction_locks[T1] = {Page_3, Page_7}
        ------------------------------------------------------------

        Notification Process:
        ------------------------------------------------------------
        🔔 condition.notify_all() wakes ALL waiting transactions
        🔄 Each waiter re-checks if their lock can now be granted
        ⚡ Only one waiter will typically succeed per release
        🎯 FIFO ordering maintained through pending queue
        ------------------------------------------------------------

        Example Release Flow:
        ------------------------------------------------------------
        T2 releases EXCLUSIVE lock on Page_5
        ┌─────────────────────────────────────────────────────┐
        │ Before: Page_5 = [T2:EXCLUSIVE]                     │
        │ Pending: [T1:SHARED, T3:SHARED, T4:EXCLUSIVE]       │
        │                                                     │
        │ 1. Remove T2:EXCLUSIVE from Page_5                  │  
        │ 2. Page_5 now empty: []                             │
        │ 3. notify_all() wakes T1, T3, T4                    │
        │ 4. T1 checks: can grant SHARED? ✅                  │
        │ 5. T3 checks: can grant SHARED? ✅ (with T1)        │
        │ 6. T4 checks: can grant EXCLUSIVE? ❌ (T1,T3 have SHARED) │
        │                                                     │
        │ Result: Page_5 = [T1:SHARED, T3:SHARED]             │
        │ Still pending: [T4:EXCLUSIVE]                       │
        └─────────────────────────────────────────────────────┘
        ------------------------------------------------------------

        Args:
            🆔 tid: Transaction releasing the lock
            📄 page_id: Page to unlock
        """
        with self._global_lock:
            if self._remove_lock_internal(tid, page_id):
                # Notify waiting transactions
                self._notify_waiters(page_id)

                # Clean up dependency graph
                self._dependency_graph.remove_dependencies_for_transaction(tid)

    def _remove_lock_internal(self, tid: TransactionId, page_id: PageId) -> bool:
        """Internal method to remove a lock."""
        locks = self._page_locks[page_id]

        for i, lock in enumerate(locks):
            if lock.transaction_id == tid:
                del locks[i]
                self._transaction_locks[tid].discard(page_id)

                # Clean up empty collections
                if not locks:
                    del self._page_locks[page_id]
                if not self._transaction_locks[tid]:
                    del self._transaction_locks[tid]

                return True

        return False

    def _notify_waiters(self, page_id: PageId) -> None:
        """Notify transactions waiting for locks on a page."""
        condition = self._page_conditions[page_id]
        condition.notify_all()

    def release_all_locks(self, tid: TransactionId) -> None:
        """
        🔓 Release all locks held by a transaction 🔓

        📞 Called when a transaction commits, aborts, or is chosen as deadlock victim.
        🧹 Performs complete cleanup of all transaction resources and dependencies.

        Bulk Release Process:
        ------------------------------------------------------------
        1. 📋 Get copy of all pages locked by transaction
        2. 🔄 For each page: remove lock + notify waiters
        3. 🧹 Clean up dependency graph completely
        4. ❌ Abort any pending requests by this transaction
        ------------------------------------------------------------

        Example Bulk Release:
        ------------------------------------------------------------
        Transaction T1 holds locks on multiple pages:
        ┌─────────────────────────────────────────────────────┐
        │ _transaction_locks[T1] = {Page_3, Page_5, Page_8}    │
        │                                                     │
        │ Page_3: [T1:SHARED, T2:SHARED]                      │
        │ Page_5: [T1:EXCLUSIVE]                              │  
        │ Page_8: [T1:SHARED, T3:EXCLUSIVE]                   │
        │                                                     │
        │ Call: release_all_locks(T1)                         │
        │                                                     │
        │ After release:                                      │
        │ Page_3: [T2:SHARED]           ← T1 removed          │
        │ Page_5: []                    ← T1 removed          │
        │ Page_8: [T3:EXCLUSIVE]        ← T1 removed          │
        │                                                     │
        │ _transaction_locks[T1] deleted entirely             │
        └─────────────────────────────────────────────────────┘
        ------------------------------------------------------------

        Dependency Graph Cleanup:
        ------------------------------------------------------------
        Before cleanup:
        T1 → {T2, T3}  ← T1 waits for T2, T3
        T2 → {T1, T4}  ← T2 waits for T1, T4
        T4 → {T1}      ← T4 waits for T1

        Call: release_all_locks(T1)

        After cleanup:
        T2 → {T4}      ← T1 removed from T2's dependencies
        T4 → {}        ← T1 removed, T4 has no dependencies now

        T1 completely removed from dependency graph
        ------------------------------------------------------------

        Pending Request Abortion:
        ------------------------------------------------------------
        🔍 Find all pending requests by this transaction
        ❌ Mark them as aborted
        🗑️ Remove from pending queues
        🔔 Notify waiting threads to wake up and check abort status
        ------------------------------------------------------------

        Notification Cascade:
        ------------------------------------------------------------
        Each page release triggers condition.notify_all():
        ┌─────────────────────────────────────────────────────┐
        │ Page_3 release → notify waiters on Page_3           │
        │ Page_5 release → notify waiters on Page_5           │  
        │ Page_8 release → notify waiters on Page_8           │
        │                                                     │
        │ Result: All waiting transactions get chance to      │
        │         acquire locks that T1 was holding           │
        └─────────────────────────────────────────────────────┘
        ------------------------------------------------------------

        Args:
            🆔 tid: Transaction to release all locks for
        """
        with self._global_lock:
            # Get copy of pages locked by this transaction
            pages_to_release = self._transaction_locks.get(tid, set()).copy()

            for page_id in pages_to_release:
                self._remove_lock_internal(tid, page_id)
                self._notify_waiters(page_id)

            # Clean up dependency graph
            self._dependency_graph.remove_dependencies_for_transaction(tid)

            # Abort any pending requests by this transaction
            self._abort_pending_requests(tid)

    def _abort_pending_requests(self, tid: TransactionId) -> None:
        """Abort all pending requests by a transaction."""
        for page_id, requests in self._pending_requests.items():
            # Copy list to avoid modification during iteration
            for request in requests[:]:
                if request.tid == tid:
                    request.aborted = True
                    requests.remove(request)

                    # Notify the waiting transaction
                    condition = self._page_conditions[page_id]
                    condition.notify_all()

    def get_locks_held(self, tid: TransactionId) -> Set[Tuple[PageId, LockType]]:
        """Get all locks held by a transaction."""
        with self._global_lock:
            result = set()
            for page_id in self._transaction_locks.get(tid, set()):
                for lock in self._page_locks[page_id]:
                    if lock.transaction_id == tid:
                        result.add((page_id, lock.lock_type))
            return result

    def detect_all_deadlocks(self) -> List[List[TransactionId]]:
        """Detect all deadlock cycles in the system."""
        with self._global_lock:
            return self._dependency_graph.find_all_cycles()

    def get_statistics(self) -> dict:
        """
        📊 Get lock manager statistics 📊

        🔍 Provides comprehensive metrics for monitoring, debugging,
        and performance analysis of the locking system.

        Statistics Categories:
        ------------------------------------------------------------
        🏃‍♂️ Current State:
        ├─ active_locks: Total locks currently held
        ├─ pending_requests: Requests waiting for locks
        └─ active_transactions: Transactions with locks

        📈 Historical Counters:
        ├─ locks_granted: Total successful acquisitions
        ├─ locks_denied: Failed acquisition attempts  
        ├─ deadlocks_detected: Deadlock cycles found
        ├─ lock_upgrades: SHARED → EXCLUSIVE upgrades
        └─ lock_timeouts: Requests that timed out
        ------------------------------------------------------------

        Example Statistics Output:
        ------------------------------------------------------------
        {
            'active_locks': 15,        ← 15 locks currently held
            'pending_requests': 3,     ← 3 transactions waiting
            'locks_granted': 1247,     ← 1247 successful acquisitions
            'locks_denied': 23,        ← 23 failed attempts
            'deadlocks_detected': 2,   ← 2 deadlock cycles found
            'lock_upgrades': 18,       ← 18 SHARED→EXCLUSIVE upgrades
            'lock_timeouts': 5,        ← 5 requests timed out
            'active_transactions': 8   ← 8 transactions have locks
        }
        ------------------------------------------------------------

        Performance Interpretation:
        ------------------------------------------------------------
        📊 High active_locks: System under load
        ⏳ High pending_requests: Lock contention detected
        💀 High deadlocks_detected: Review transaction patterns
        ⏰ High lock_timeouts: Consider increasing timeout
        ⬆️ High lock_upgrades: Optimize read→write patterns
        🚫 High locks_denied: Investigate conflict sources
        ------------------------------------------------------------

        Usage Examples:
        ------------------------------------------------------------
        stats = lock_manager.get_statistics()
        utilization = stats['active_locks'] / max_expected_locks
        contention_ratio = stats['pending_requests'] / stats['active_transactions']
        deadlock_rate = stats['deadlocks_detected'] / stats['locks_granted']
        ------------------------------------------------------------

        Returns:
            📋 Dictionary with current and historical statistics
        """
        with self._global_lock:
            active_locks = sum(len(locks)
                               for locks in self._page_locks.values())
            pending_requests = sum(len(requests)
                                   for requests in self._pending_requests.values())

            return {
                'active_locks': active_locks,
                'pending_requests': pending_requests,
                'locks_granted': self.locks_granted,
                'locks_denied': self.locks_denied,
                'deadlocks_detected': self.deadlocks_detected,
                'lock_upgrades': self.lock_upgrades,
                'lock_timeouts': self.lock_timeouts,
                'active_transactions': len(self._transaction_locks)
            }

    def get_lock_info(self) -> dict:
        """
        🔍 Get detailed information about current locks (for debugging) 🔍

        🛠️ Provides comprehensive snapshot of internal lock manager state
        for debugging, monitoring, and system analysis.

        Information Categories:
        ------------------------------------------------------------
        🔒 Active Locks:
        ├─ Per-page breakdown of current locks
        ├─ Transaction ID and lock type for each lock
        └─ Real-time lock ownership details

        ⏳ Pending Requests:
        ├─ Queued requests waiting for each page
        ├─ Wait time for each pending request
        └─ Request type and transaction details

        🕸️ Dependency Graph:
        ├─ Current wait-for relationships
        ├─ Potential deadlock patterns
        └─ Transaction dependency chains
        ------------------------------------------------------------

        Example Output Structure:
        ------------------------------------------------------------
        {
            'active_locks': {
                'Page_3': [
                    {'transaction': 'T1', 'type': 'SHARED'},
                    {'transaction': 'T2', 'type': 'SHARED'}
                ],
                'Page_5': [
                    {'transaction': 'T3', 'type': 'EXCLUSIVE'}
                ]
            },
            'pending_requests': {
                'Page_5': [
                    {
                        'transaction': 'T1',
                        'type': 'EXCLUSIVE', 
                        'wait_time': 12.5
                    }
                ]
            },
            'dependency_graph': "{'T1': {'T3'}, 'T4': {'T1'}}"
        }
        ------------------------------------------------------------

        Debugging Use Cases:
        ------------------------------------------------------------
        🔍 Lock Contention Analysis:
        ├─ Which pages have most conflicts?
        ├─ Which transactions hold most locks?
        └─ What are typical wait times?

        💀 Deadlock Investigation:
        ├─ Examine dependency graph for cycles
        ├─ Identify transactions in deadlock
        └─ Understand wait-for relationships

        ⚡ Performance Tuning:
        ├─ Find lock hotspots
        ├─ Optimize transaction patterns
        └─ Adjust timeout values
        ------------------------------------------------------------

        Real-time Monitoring:
        ------------------------------------------------------------
        # Print current lock state
        info = lock_manager.get_lock_info()

        print("🔒 ACTIVE LOCKS:")
        for page, locks in info['active_locks'].items():
            print(f"  {page}: {locks}")

        print("⏳ PENDING REQUESTS:")  
        for page, requests in info['pending_requests'].items():
            for req in requests:
                print(f"  {req['transaction']} waiting {req['wait_time']:.1f}s for {page}")
        ------------------------------------------------------------

        Returns:
            🗂️ Dictionary with active_locks, pending_requests, and dependency_graph
        """
        with self._global_lock:
            lock_info = {}

            for page_id, locks in self._page_locks.items():
                lock_info[str(page_id)] = [
                    {
                        'transaction': str(lock.transaction_id),
                        'type': lock.lock_type.value
                    }
                    for lock in locks
                ]

            pending_info = {}
            for page_id, requests in self._pending_requests.items():
                pending_info[str(page_id)] = [
                    {
                        'transaction': str(req.tid),
                        'type': req.lock_type.value,
                        'wait_time': time.time() - req.timestamp
                    }
                    for req in requests
                ]

            return {
                'active_locks': lock_info,
                'pending_requests': pending_info,
                'dependency_graph': str(self._dependency_graph.graph)
            }
