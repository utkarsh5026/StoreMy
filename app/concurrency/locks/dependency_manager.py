import threading
from dataclasses import dataclass
from collections import defaultdict
from enum import Enum
from app.primitives import TransactionId, PageId


class LockType(Enum):
    """
    🔒 Types of locks that can be acquired 🔒

    📖 SHARED: Read lock - multiple transactions can hold simultaneously
    ✏️ EXCLUSIVE: Write lock - only one transaction can hold at a time

    Lock Compatibility Matrix:
    ------------------------------------------------------------
             │ SHARED  │ EXCLUSIVE 
    ---------┼---------┼-----------
    SHARED   │    ✅   │     ❌     
    EXCLUSIVE│    ❌   │     ❌     
    ------------------------------------------------------------

    🎯 Key Rules:
    ✅ Multiple SHARED locks can coexist (readers don't block readers)
    ❌ EXCLUSIVE locks conflict with everything (writers block all)
    🔄 SHARED can upgrade to EXCLUSIVE if no other locks exist
    """
    SHARED = "SHARED"
    EXCLUSIVE = "EXCLUSIVE"


@dataclass(frozen=True)
class Lock:
    """
    🔐 Immutable lock representation 🔐

    💎 Frozen dataclass ensures thread safety and proper hashing

    Lock Structure:
    ------------------------------------------------------------
    🆔 Transaction ID  => Which transaction holds this lock    
    📄 Page ID         => Which page is locked                 
    🔒 Lock Type        => SHARED or EXCLUSIVE                  
    ------------------------------------------------------------

    Example Lock Objects:
    ------------------------------------------------------------
    Lock(T1, Page_5, SHARED)    → T1 reading Page_5
    Lock(T2, Page_3, EXCLUSIVE) → T2 writing Page_3
    Lock(T1, Page_7, EXCLUSIVE) → T1 writing Page_7
    ------------------------------------------------------------

    🎯 Benefits of Immutability:
    🔒 Thread-safe by design
    🗂️ Can be used as dictionary keys
    ⚡ No synchronization needed for reads
    🛡️ Prevents accidental modifications
    """
    transaction_id: TransactionId
    page_id: PageId
    lock_type: LockType

    def __str__(self) -> str:
        return f"Lock({self.transaction_id}, {self.page_id}, {self.lock_type.value})"


class DependencyGraph:
    """
    🕸️ Dependency graph for deadlock detection with cycle finding 🕸️

    🔗 Maintains a directed graph where edges represent "waits-for" relationships.
    🔍 Provides efficient cycle detection using DFS (Depth-First Search).

    Graph Structure:
    ------------------------------------------------------------
    Nodes: TransactionIds (T1, T2, T3, ...)
    Edges: A → B means "A waits for B"

    Example Dependency Graph:

         T1 ──waits for──→ T2
         ↑                  ↓
         │                  │ waits for
         │                  ↓
         T4 ←──waits for── T3

    This forms a cycle: T1 → T2 → T3 → T4 → T1 = 💀 DEADLOCK!
    ------------------------------------------------------------

    🎯 Key Features:
    💀 Deadlock detection using cycle finding
    📊 Statistics tracking for monitoring
    🔄 Multiple cycle detection algorithms
    🧹 Automatic cleanup of empty dependencies
    🔒 Thread-safe operations with RLock

    🚀 Performance Characteristics:
    ➕ Add dependency: O(1) average case
    🗑️ Remove dependency: O(1) average case  
    🔍 Cycle detection: O(V + E) where V=transactions, E=dependencies
    📊 Statistics: O(V + E) for complete graph traversal
    """

    @dataclass(frozen=True)
    class Stats:
        """
        📊 Statistics about the dependency graph 📊

        Metrics Tracked:
        ------------------------------------------------------------
        🔢 nodes: Number of transactions with dependencies
        🔗 edges: Total number of wait-for relationships
        📈 avg_dependencies: Average dependencies per transaction
        ------------------------------------------------------------

        Usage Examples:
        ------------------------------------------------------------
        stats = graph.get_statistics()
        print(f"🔢 Active transactions: {stats.nodes}")
        print(f"🔗 Total dependencies: {stats.edges}")  
        print(f"📈 Avg deps per txn: {stats.avg_dependencies:.2f}")
        ------------------------------------------------------------
        """
        nodes: int
        edges: int
        avg_dependencies: float

    def __init__(self):
        # adjacency list: transaction -> set of transactions it waits for
        self._graph: defaultdict[TransactionId,
                                 set[TransactionId]] = defaultdict(set)
        self._lock = threading.RLock()

    @property
    def graph(self) -> dict[TransactionId, set[TransactionId]]:
        """
        🔍 Get a read-only view of the graph 🔍

        Returns a deep copy to prevent external modifications
        while allowing safe inspection of the graph structure.

        Return Format:
        ------------------------------------------------------------
        {
            T1: {T2, T3},    # T1 waits for T2 and T3
            T2: {T4},        # T2 waits for T4  
            T4: {T1}         # T4 waits for T1 (cycle!)
        }
        ------------------------------------------------------------

        🛡️ Thread Safety: Creates snapshot under lock protection
        💾 Memory: Creates copy to avoid lock contention on reads
        """
        with self._lock:
            return {tid: deps.copy() for tid, deps in self._graph.items()}

    def add_dependency(self, waiter: TransactionId, holder: TransactionId) -> None:
        """
        ➕ Add a dependency: waiter waits for holder ➕

        Dependency Creation Process:
        ------------------------------------------------------------
        1. 🔒 Acquire exclusive lock
        2. ✅ Validate waiter ≠ holder (prevent self-loops)
        3. ➕ Add holder to waiter's dependency set
        4. 🔓 Release lock
        ------------------------------------------------------------

        Visual Example:
        ------------------------------------------------------------
        Before: T1 → {T2}
        Call: add_dependency(T1, T3)  
        After:  T1 → {T2, T3}

        Meaning: T1 now waits for both T2 and T3
        ------------------------------------------------------------

        Self-Loop Prevention:
        ------------------------------------------------------------
        ❌ add_dependency(T1, T1) → IGNORED (no self-loops)
        ✅ add_dependency(T1, T2) → ADDED (valid dependency)
        ------------------------------------------------------------

        Args:
            🕰️ waiter: Transaction that is waiting
            🔒 holder: Transaction that holds the conflicting resource
        """
        with self._lock:
            if waiter != holder:
                self._graph[waiter].add(holder)

    def remove_dependency(self, waiter: TransactionId, holder: TransactionId) -> None:
        """
        ➖ Remove a specific dependency ➖

        Selective Removal Process:
        ------------------------------------------------------------
        1. 🔒 Acquire exclusive lock
        2. 🔍 Check if waiter exists in graph
        3. 🗑️ Remove holder from waiter's dependency set
        4. 🧹 Clean up empty dependency sets
        5. 🔓 Release lock
        ------------------------------------------------------------

        Visual Example:
        ------------------------------------------------------------
        Before: T1 → {T2, T3, T4}
        Call: remove_dependency(T1, T3)
        After:  T1 → {T2, T4}

        Only the T1→T3 dependency is removed
        ------------------------------------------------------------

        Cleanup Behavior:
        ------------------------------------------------------------
        Before: T1 → {T2}
        Call: remove_dependency(T1, T2)  
        After:  T1 → {} → DELETE T1 entry

        Empty dependency sets are automatically removed
        ------------------------------------------------------------

        Args:
            🕰️ waiter: Transaction to remove dependency from
            🔒 holder: Transaction to remove from dependency set
        """
        with self._lock:
            if waiter in self._graph:
                self._graph[waiter].discard(holder)
                if not self._graph[waiter]:
                    del self._graph[waiter]

    def remove_dependencies_for_transaction(self, tid: TransactionId) -> None:
        """
        🧹 Remove all dependencies involving a transaction 🧹

        📞 Called when transaction completes, aborts, or is chosen as deadlock victim.
        🔄 Performs complete cleanup of both outgoing and incoming dependencies.

        Complete Cleanup Process:
        ------------------------------------------------------------
        1. 🗑️ Remove as waiter (outgoing dependencies)
           T1 → {T2, T3} becomes ∅

        2. 🧹 Remove as holder (incoming dependencies)  
           T2 → {T1, T4} becomes T2 → {T4}
           T3 → {T1, T5} becomes T3 → {T5}

        3. 🔧 Clean up empty dependency sets
           Any transaction with empty set gets deleted
        ------------------------------------------------------------

        Before Cleanup Example:
        ------------------------------------------------------------
        Graph state:
        T1 → {T2, T3}  ← T1 waits for T2, T3
        T2 → {T1, T4}  ← T2 waits for T1, T4  
        T3 → {T1, T5}  ← T3 waits for T1, T5
        T4 → {T6}      ← T4 waits for T6

        Call: remove_dependencies_for_transaction(T1)
        ------------------------------------------------------------

        After Cleanup Example:
        ------------------------------------------------------------
        Graph state:
        T2 → {T4}      ← T2 now only waits for T4
        T3 → {T5}      ← T3 now only waits for T5  
        T4 → {T6}      ← T4 unchanged (didn't involve T1)

        Result: All traces of T1 removed from graph
        ------------------------------------------------------------

        Args:
            🆔 tid: Transaction ID to completely remove from graph
        """
        with self._lock:
            if tid in self._graph:
                del self._graph[tid]

            for waiter_deps in self._graph.values():
                waiter_deps.discard(tid)

            # Clean up empty entries
            empty_waiters = [waiter for waiter,
                             deps in self._graph.items() if not deps]
            for waiter in empty_waiters:
                del self._graph[waiter]

    def has_cycle(self) -> list[TransactionId] | None:
        """
        🔍 Detect if there's a cycle using DFS 🔍

        🧠 Uses Depth-First Search with recursion stack to detect back edges,
        which indicate cycles in directed graphs.

        DFS Cycle Detection Algorithm:
        ------------------------------------------------------------
        1. 🚀 Start DFS from each unvisited node
        2. 📝 Maintain visited set and recursion stack
        3. 🔄 For each neighbor:
           ├─ If in recursion stack → CYCLE FOUND! 💀
           ├─ If not visited → Continue DFS recursively  
           └─ If visited but not in stack → Skip
        4. 🏁 Return first cycle found, or None
        ------------------------------------------------------------

        Visual DFS Example:
        ------------------------------------------------------------
        Graph: T1 → T2 → T3 → T1 (cycle)

        Step 1: Start DFS from T1
        visited = {}, rec_stack = {}, path = []

        Step 2: Visit T1  
        visited = {T1}, rec_stack = {T1}, path = [T1]

        Step 3: Visit T2 (neighbor of T1)
        visited = {T1,T2}, rec_stack = {T1,T2}, path = [T1,T2]

        Step 4: Visit T3 (neighbor of T2)  
        visited = {T1,T2,T3}, rec_stack = {T1,T2,T3}, path = [T1,T2,T3]

        Step 5: Check T1 (neighbor of T3)
        T1 is in rec_stack! → CYCLE DETECTED! 💀
        Return path from T1: [T1, T2, T3, T1]
        ------------------------------------------------------------

        Performance Analysis:
        ------------------------------------------------------------
        ⏱️ Time Complexity: O(V + E)
           V = number of transactions (vertices)
           E = number of dependencies (edges)

        💾 Space Complexity: O(V)  
           For recursion stack and visited set

        🎯 Worst Case: Complete graph traversal
        ⚡ Best Case: Cycle found immediately
        ------------------------------------------------------------

        Returns:
            📋 List of transaction IDs forming a cycle, or None if no cycle exists

        Example Return Values:
        ------------------------------------------------------------
        ✅ No cycle: None
        💀 Simple cycle: [T1, T2, T1]  
        💀 Complex cycle: [T1, T2, T3, T4, T1]
        ------------------------------------------------------------
        """
        with self._lock:
            visited = set()
            rec_stack = set()

            def dfs(node: TransactionId, path: list[TransactionId]) -> list[TransactionId] | None:
                if node in rec_stack:
                    cycle_start = path.index(node)
                    return path[cycle_start:] + [node]

                if node in visited:
                    return None

                visited.add(node)
                rec_stack.add(node)
                path.append(node)

                for neighbor in self._graph.get(node, set()):
                    cycle = dfs(neighbor, path)
                    if cycle:
                        return cycle

                rec_stack.remove(node)
                path.pop()
                return None

            for node in self._graph:
                if node not in visited:
                    cycle = dfs(node, [])
                    if cycle:
                        return cycle

            return None

    def find_all_cycles(self) -> list[list[TransactionId]]:
        """
        🔍 Find all cycles in the graph 🔍

        🌐 Unlike has_cycle() which stops at first cycle, this method finds
        ALL cycles in the dependency graph for comprehensive analysis.

        Complete Cycle Detection Process:
        ------------------------------------------------------------
        1. 🚀 Start DFS from each unvisited node
        2. 📝 Track current path and path set for fast lookup
        3. 🔄 When back edge found:
           ├─ Extract cycle from path
           ├─ Add to cycles list  
           └─ Continue searching for more cycles
        4. 📋 Return all cycles found
        ------------------------------------------------------------

        Multiple Cycles Example:
        ------------------------------------------------------------
        Complex Graph:
        T1 → T2 → T3 → T1    (Cycle 1: T1→T2→T3→T1)
        T4 → T5 → T6 → T4    (Cycle 2: T4→T5→T6→T4)  
        T2 → T7 → T8 → T2    (Cycle 3: T2→T7→T8→T2)

        Result: [
            [T1, T2, T3],
            [T4, T5, T6], 
            [T2, T7, T8]
        ]
        ------------------------------------------------------------

        Overlapping Cycles Example:
        ------------------------------------------------------------
        Shared Dependencies:
        T1 → T2 → T3 → T1    (Cycle 1)
        T2 → T4 → T5 → T2    (Cycle 2, shares T2)

        Both cycles share transaction T2, creating complex
        dependency patterns that require careful resolution.
        ------------------------------------------------------------

        Performance Considerations:
        ------------------------------------------------------------
        ⏱️ Time Complexity: O(V + E) per cycle
        💾 Space Complexity: O(V) for path tracking
        🎯 Use Case: Comprehensive deadlock analysis
        ⚠️ Warning: Can be expensive on large graphs
        ------------------------------------------------------------

        Returns:
            📚 List of all cycles, each cycle is a list of TransactionIds
        """
        with self._lock:
            cycles: list[list[TransactionId]] = []
            visited = set()

            def dfs_all_cycles(node: TransactionId, path: list[TransactionId],
                               path_set: set[TransactionId]) -> None:
                if node in path_set:
                    cycle_start = path.index(node)
                    cycle = path[cycle_start:]
                    if len(cycle) > 1:
                        cycles.append(cycle)
                    return

                if node in visited:
                    return

                path.append(node)
                path_set.add(node)

                for neighbor in self._graph.get(node, set()):
                    dfs_all_cycles(neighbor, path, path_set)

                path.pop()
                path_set.remove(node)
                visited.add(node)

            for node in self._graph:
                if node not in visited:
                    dfs_all_cycles(node, [], set())

            return cycles

    def get_statistics(self) -> Stats:
        """
        📊 Get statistics about the dependency graph 📊

        Statistical Analysis:
        ------------------------------------------------------------
        🔢 Nodes: Count of transactions with dependencies
        🔗 Edges: Total number of wait-for relationships
        📈 Average: Mean dependencies per transaction
        ------------------------------------------------------------

        Calculation Details:
        ------------------------------------------------------------
        nodes = len(graph.keys())
        edges = sum(len(deps) for deps in graph.values())  
        avg = edges / max(1, nodes)  # Avoid division by zero
        ------------------------------------------------------------

        Example Statistics:
        ------------------------------------------------------------
        Graph: {
            T1: {T2, T3},     # T1 has 2 dependencies
            T2: {T4},         # T2 has 1 dependency  
            T4: {T1}          # T4 has 1 dependency
        }

        Stats:
        nodes = 3 (T1, T2, T4 have dependencies)
        edges = 4 (T1→T2, T1→T3, T2→T4, T4→T1)
        avg = 4/3 = 1.33 dependencies per transaction
        ------------------------------------------------------------

        Interpretation Guide:
        ------------------------------------------------------------
        📊 High node count: Many active transactions
        🔗 High edge count: Complex dependency patterns
        📈 High average: Potential for deadlock hotspots
        📉 Low average: Simpler dependency patterns
        ------------------------------------------------------------

        Performance Monitoring:
        ------------------------------------------------------------
        🚀 Use for capacity planning
        ⚠️ Monitor for deadlock-prone patterns
        📈 Track trends over time
        🎯 Optimize based on statistics
        ------------------------------------------------------------

        Returns:
            📊 Stats object with nodes, edges, and avg_dependencies
        """
        with self._lock:
            total_edges = sum(len(deps) for deps in self._graph.values())
            return self.Stats(
                nodes=len(self._graph),
                edges=total_edges,
                avg_dependencies=total_edges / max(1, len(self._graph))
            )
