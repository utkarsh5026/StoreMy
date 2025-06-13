import threading
from dataclasses import dataclass
from typing import Optional
from collections import defaultdict
from enum import Enum
from ...primitives import TransactionId, PageId


class LockType(Enum):
    """
    🔒 Types of locks that can be acquired 🔒

    📖 SHARED: Read lock - multiple transactions can hold simultaneously
    ✏️ EXCLUSIVE: Write lock - only one transaction can hold at a time

    Lock Compatibility:
    ┌─────────────┬─────────┬───────────┐
    │             │ SHARED  │ EXCLUSIVE │
    ├─────────────┼─────────┼───────────┤
    │ SHARED      │    ✅    │     ❌    │
    │ EXCLUSIVE   │    ❌    │     ❌    │
    └─────────────┴─────────┴───────────┘
    """
    SHARED = "SHARED"  # Read lock - multiple transactions can hold
    EXCLUSIVE = "EXCLUSIVE"  # Write lock - only one transaction can hold


@dataclass(frozen=True)
class Lock:
    """
    🔐 Represents a lock held by a transaction on a page 🔐

    💎 Immutable to ensure thread safety and proper hashing

    Structure:
    ┌─────────────────┬───────────────────────┐
    │ 🆔 Transaction  │ Which transaction     │
    │ 📄 Page         │ Which page locked     │
    │ 🔒 Lock Type     │ SHARED or EXCLUSIVE    │
    └─────────────────┴───────────────────────┘
    """
    transaction_id: TransactionId
    page_id: PageId
    lock_type: LockType

    def __str__(self) -> str:
        return f"Lock({self.transaction_id}, {self.page_id}, {self.lock_type.value})"


class DependencyGraph:
    """
    🕸️ Tracks transaction dependencies for deadlock detection 🕸️

    ⚠️ A dependency exists when Transaction A waits for Transaction B to release a lock.
    🔍 We detect deadlocks by finding cycles in this dependency graph using DFS.

    Graph Structure:
    ┌─────────────────────────────────────────────────────────┐
    │ Nodes: TransactionIds                                   │
    │ Edges: A → B means "A waits for B"                     │
    │                                                         │
    │ Example Deadlock Cycle:                                 │
    │                                                         │
    │     T1 ──────→ T2                                      │
    │     ↑           ↓                                       │
    │     T4 ←────── T3                                      │
    │                                                         │
    │ T1 waits for T2, T2 waits for T3,                     │
    │ T3 waits for T4, T4 waits for T1 = 💀 DEADLOCK!       │
    └─────────────────────────────────────────────────────────┘
    """

    def __init__(self):
        # adjacency list: transaction -> set of transactions it waits for
        self._graph: dict[TransactionId, set[TransactionId]] = defaultdict(set)
        self._lock = threading.RLock()

    @property
    def graph(self) -> dict[TransactionId, set[TransactionId]]:
        return self._graph

    def add_dependency(self, waiter: TransactionId, holder: TransactionId) -> None:
        """
        ➕ Add a dependency: waiter depends on holder ➕

        Flow:
        ┌─────────────┐    waits for    ┌─────────────┐
        │   Waiter    │ ──────────────→ │   Holder    │
        │      🕰️      │                 │      🔒      │
        └─────────────┘                 └─────────────┘

        Args:
            🕰️ waiter: Transaction that is waiting
            🔒 holder: Transaction that holds the conflicting lock
        """
        with self._lock:
            if waiter != holder:  # Prevent self-loops
                self._graph[waiter].add(holder)

    def remove_dependencies_for_transaction(self, tid: TransactionId) -> None:
        """
        🧹 Remove all dependencies involving a transaction 🧹

        📞 Called when transaction completes or aborts.

        Cleanup Process:
        ┌─────────────────────────────────────────────────────┐
        │ 1. Remove as waiter:                                │
        │    T1 → [T2, T3] becomes ∅                         │
        │                                                     │
        │ 2. Remove as holder:                                │
        │    T2 → [T1, T4] becomes T2 → [T4]                │
        │    T3 → [T1, T5] becomes T3 → [T5]                │
        └─────────────────────────────────────────────────────┘
        """
        with self._lock:
            # Remove as a waiter
            if tid in self._graph:
                del self._graph[tid]

            # Remove as a holder (from other transactions' wait lists)
            for waiter_deps in self._graph.values():
                waiter_deps.discard(tid)

    def has_cycle(self) -> Optional[list[TransactionId]]:
        """
        🔍 Detect if there's a cycle in the dependency graph using DFS 🔍

        Algorithm Flow:
        ┌──────────────────────────────────────────────────────┐
        │ 1. Start DFS from each unvisited node               │
        │    ┌─────┐                                          │
        │    │  T1 │ ──→ Check all neighbors                 │
        │    └─────┘                                          │
        │                                                     │
        │ 2. Track visited nodes and recursion stack          │
        │    Visited: {T1, T2}                               │
        │    RecStack: {T1} (currently exploring)            │
        │                                                     │
        │ 3. If neighbor is in recursion stack = CYCLE! 💀    │
        │    T1 → T2 → T3 → T1 (back to recursion stack)    │
        └──────────────────────────────────────────────────────┘

        Returns:
            📋 List of transaction IDs forming a cycle, or None if no cycle
        """
        with self._lock:
            visited = set()
            rec_stack = set()

            def dfs(tr_node: TransactionId, path: list[TransactionId]) -> Optional[list[TransactionId]]:
                visited.add(tr_node)
                rec_stack.add(tr_node)
                path.append(tr_node)

                for neighbor in self._graph.get(tr_node, set()):
                    if neighbor in rec_stack:
                        cycle_start = path.index(neighbor)
                        return path[cycle_start:] + [neighbor]

                    if neighbor not in visited:
                        is_cycle = dfs(neighbor, path.copy())
                        if is_cycle:
                            return is_cycle

                rec_stack.remove(tr_node)
                return None

            for node in self._graph:
                if node not in visited:
                    cycle = dfs(node, [])
                    if cycle:
                        return cycle

            return None
