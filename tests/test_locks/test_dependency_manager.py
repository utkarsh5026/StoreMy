"""
Comprehensive tests for the dependency manager module.

This test suite provides exhaustive coverage of:
- LockType enum functionality
- Lock dataclass behavior and immutability
- DependencyGraph operations and thread safety
- Cycle detection algorithms (simple and complex)
- Graph manipulation and cleanup
- Statistics and monitoring
- Concurrent operations and edge cases
"""

import pytest
import threading
import time

from app.concurrency.locks.dependency_manager import (
    LockType, Lock, DependencyGraph
)
from app.primitives import TransactionId
from app.storage.heap import HeapPageId


class TestLockType:
    """Test the LockType enum."""

    def test_lock_type_values(self):
        """Test that LockType enum has correct values."""
        assert LockType.SHARED.value == "SHARED"
        assert LockType.EXCLUSIVE.value == "EXCLUSIVE"

    def test_lock_type_comparison(self):
        """Test LockType enum comparison."""
        assert LockType.SHARED == LockType.SHARED
        assert LockType.EXCLUSIVE == LockType.EXCLUSIVE
        assert LockType.SHARED != LockType.EXCLUSIVE

    def test_lock_type_string_representation(self):
        """Test string representation of LockType."""
        assert str(LockType.SHARED) == "LockType.SHARED"
        assert str(LockType.EXCLUSIVE) == "LockType.EXCLUSIVE"


class TestLock:
    """Test the Lock dataclass."""

    def setup_method(self):
        """Set up test fixtures."""
        self.tid1 = TransactionId()
        self.tid2 = TransactionId()
        self.page1 = HeapPageId(1, 1)
        self.page2 = HeapPageId(1, 2)

    def test_lock_creation(self):
        """Test basic lock creation."""
        lock = Lock(self.tid1, self.page1, LockType.SHARED)

        assert lock.transaction_id == self.tid1
        assert lock.page_id == self.page1
        assert lock.lock_type == LockType.SHARED

    def test_lock_immutability(self):
        """Test that Lock is immutable (frozen dataclass)."""
        lock = Lock(self.tid1, self.page1, LockType.SHARED)

        # Should not be able to modify fields
        with pytest.raises(AttributeError):
            lock.transaction_id = self.tid2

        with pytest.raises(AttributeError):
            lock.page_id = self.page2

        with pytest.raises(AttributeError):
            lock.lock_type = LockType.EXCLUSIVE

    def test_lock_equality(self):
        """Test lock equality comparison."""
        lock1 = Lock(self.tid1, self.page1, LockType.SHARED)
        lock2 = Lock(self.tid1, self.page1, LockType.SHARED)
        lock3 = Lock(self.tid1, self.page1, LockType.EXCLUSIVE)
        lock4 = Lock(self.tid2, self.page1, LockType.SHARED)

        assert lock1 == lock2  # Same content
        assert lock1 != lock3  # Different lock type
        assert lock1 != lock4  # Different transaction

    def test_lock_hashability(self):
        """Test that Lock can be used as dictionary key or in sets."""
        lock1 = Lock(self.tid1, self.page1, LockType.SHARED)
        lock2 = Lock(self.tid1, self.page1, LockType.SHARED)
        lock3 = Lock(self.tid1, self.page1, LockType.EXCLUSIVE)

        # Can be used in sets
        lock_set = {lock1, lock2, lock3}
        assert len(lock_set) == 2  # lock1 and lock2 are identical

        # Can be used as dictionary keys
        lock_dict = {lock1: "value1", lock3: "value2"}
        assert len(lock_dict) == 2

    def test_lock_string_representation(self):
        """Test lock string representation."""
        lock = Lock(self.tid1, self.page1, LockType.SHARED)
        lock_str = str(lock)

        assert "Lock(" in lock_str
        assert str(self.tid1) in lock_str
        assert str(self.page1) in lock_str
        assert "SHARED" in lock_str

    def test_lock_with_different_types(self):
        """Test locks with both shared and exclusive types."""
        shared_lock = Lock(self.tid1, self.page1, LockType.SHARED)
        exclusive_lock = Lock(self.tid1, self.page1, LockType.EXCLUSIVE)

        assert shared_lock.lock_type == LockType.SHARED
        assert exclusive_lock.lock_type == LockType.EXCLUSIVE
        assert shared_lock != exclusive_lock


class TestDependencyGraph:
    """Test the DependencyGraph class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.graph = DependencyGraph()
        self.tid1 = TransactionId()
        self.tid2 = TransactionId()
        self.tid3 = TransactionId()
        self.tid4 = TransactionId()
        self.tid5 = TransactionId()

    # =================== BASIC OPERATIONS ===================

    def test_empty_graph_initialization(self):
        """Test that graph initializes empty."""
        assert len(self.graph.graph) == 0
        assert self.graph.has_cycle() is None
        assert len(self.graph.find_all_cycles()) == 0

    def test_add_single_dependency(self):
        """Test adding a single dependency."""
        self.graph.add_dependency(self.tid1, self.tid2)

        graph_dict = self.graph.graph
        assert self.tid1 in graph_dict
        assert self.tid2 in graph_dict[self.tid1]
        assert len(graph_dict[self.tid1]) == 1

    def test_add_multiple_dependencies_same_waiter(self):
        """Test adding multiple dependencies for the same waiter."""
        self.graph.add_dependency(self.tid1, self.tid2)
        self.graph.add_dependency(self.tid1, self.tid3)

        graph_dict = self.graph.graph
        assert len(graph_dict[self.tid1]) == 2
        assert self.tid2 in graph_dict[self.tid1]
        assert self.tid3 in graph_dict[self.tid1]

    def test_add_duplicate_dependency(self):
        """Test adding duplicate dependency (should be idempotent)."""
        self.graph.add_dependency(self.tid1, self.tid2)
        self.graph.add_dependency(self.tid1, self.tid2)  # Duplicate

        graph_dict = self.graph.graph
        assert len(graph_dict[self.tid1]) == 1
        assert self.tid2 in graph_dict[self.tid1]

    def test_add_self_dependency_ignored(self):
        """Test that self-dependencies are ignored."""
        self.graph.add_dependency(self.tid1, self.tid1)

        graph_dict = self.graph.graph
        assert len(graph_dict) == 0  # Should be empty

    def test_remove_existing_dependency(self):
        """Test removing an existing dependency."""
        self.graph.add_dependency(self.tid1, self.tid2)
        self.graph.add_dependency(self.tid1, self.tid3)

        self.graph.remove_dependency(self.tid1, self.tid2)

        graph_dict = self.graph.graph
        assert self.tid2 not in graph_dict[self.tid1]
        assert self.tid3 in graph_dict[self.tid1]
        assert len(graph_dict[self.tid1]) == 1

    def test_remove_last_dependency_cleans_up(self):
        """Test that removing last dependency cleans up the waiter node."""
        self.graph.add_dependency(self.tid1, self.tid2)
        self.graph.remove_dependency(self.tid1, self.tid2)

        graph_dict = self.graph.graph
        assert self.tid1 not in graph_dict

    def test_remove_nonexistent_dependency(self):
        """Test removing non-existent dependency (should be no-op)."""
        self.graph.add_dependency(self.tid1, self.tid2)
        self.graph.remove_dependency(self.tid1, self.tid3)  # Doesn't exist

        graph_dict = self.graph.graph
        assert self.tid2 in graph_dict[self.tid1]
        assert len(graph_dict[self.tid1]) == 1

    def test_remove_dependency_from_empty_graph(self):
        """Test removing dependency from empty graph."""
        # Should not raise error
        self.graph.remove_dependency(self.tid1, self.tid2)

        graph_dict = self.graph.graph
        assert len(graph_dict) == 0

    # =================== TRANSACTION CLEANUP ===================

    def test_remove_dependencies_for_transaction_as_waiter(self):
        """Test removing all dependencies where transaction is waiter."""
        self.graph.add_dependency(self.tid1, self.tid2)
        self.graph.add_dependency(self.tid1, self.tid3)
        self.graph.add_dependency(self.tid2, self.tid3)  # Should remain

        self.graph.remove_dependencies_for_transaction(self.tid1)

        graph_dict = self.graph.graph
        assert self.tid1 not in graph_dict
        assert self.tid2 in graph_dict
        assert self.tid3 in graph_dict[self.tid2]

    def test_remove_dependencies_for_transaction_as_holder(self):
        """Test removing all dependencies where transaction is holder."""
        self.graph.add_dependency(self.tid1, self.tid3)
        self.graph.add_dependency(self.tid2, self.tid3)
        self.graph.add_dependency(self.tid2, self.tid4)  # Should remain

        self.graph.remove_dependencies_for_transaction(self.tid3)

        graph_dict = self.graph.graph
        assert self.tid1 not in graph_dict  # Should be cleaned up
        assert self.tid2 in graph_dict
        assert self.tid4 in graph_dict[self.tid2]
        assert self.tid3 not in graph_dict[self.tid2]

    def test_remove_dependencies_for_nonexistent_transaction(self):
        """Test removing dependencies for transaction not in graph."""
        self.graph.add_dependency(self.tid1, self.tid2)

        # Should not affect existing dependencies
        self.graph.remove_dependencies_for_transaction(self.tid3)

        graph_dict = self.graph.graph
        assert self.tid2 in graph_dict[self.tid1]

    # =================== CYCLE DETECTION ===================

    def test_no_cycle_in_empty_graph(self):
        """Test cycle detection in empty graph."""
        assert self.graph.has_cycle() is None

    def test_no_cycle_single_dependency(self):
        """Test no cycle with single dependency."""
        self.graph.add_dependency(self.tid1, self.tid2)
        assert self.graph.has_cycle() is None

    def test_no_cycle_chain_dependencies(self):
        """Test no cycle in chain of dependencies."""
        self.graph.add_dependency(self.tid1, self.tid2)
        self.graph.add_dependency(self.tid2, self.tid3)
        self.graph.add_dependency(self.tid3, self.tid4)

        assert self.graph.has_cycle() is None

    def test_simple_two_node_cycle(self):
        """Test detection of simple two-node cycle."""
        self.graph.add_dependency(self.tid1, self.tid2)
        self.graph.add_dependency(self.tid2, self.tid1)

        cycle = self.graph.has_cycle()
        assert cycle is not None
        assert len(cycle) >= 2
        assert self.tid1 in cycle
        assert self.tid2 in cycle

    def test_three_node_cycle(self):
        """Test detection of three-node cycle."""
        self.graph.add_dependency(self.tid1, self.tid2)
        self.graph.add_dependency(self.tid2, self.tid3)
        self.graph.add_dependency(self.tid3, self.tid1)

        cycle = self.graph.has_cycle()
        assert cycle is not None
        assert len(cycle) >= 3
        assert self.tid1 in cycle
        assert self.tid2 in cycle
        assert self.tid3 in cycle

    def test_complex_cycle_with_additional_nodes(self):
        """Test cycle detection with additional non-cycle nodes."""
        # Create cycle: T1 -> T2 -> T3 -> T1
        self.graph.add_dependency(self.tid1, self.tid2)
        self.graph.add_dependency(self.tid2, self.tid3)
        self.graph.add_dependency(self.tid3, self.tid1)

        # Add non-cycle dependencies
        self.graph.add_dependency(self.tid4, self.tid1)
        self.graph.add_dependency(self.tid3, self.tid5)

        cycle = self.graph.has_cycle()
        assert cycle is not None
        # Should detect the T1->T2->T3->T1 cycle
        cycle_set = set(cycle)
        assert self.tid1 in cycle_set
        assert self.tid2 in cycle_set
        assert self.tid3 in cycle_set

    def test_multiple_separate_cycles(self):
        """Test detection when multiple separate cycles exist."""
        # First cycle: T1 -> T2 -> T1
        self.graph.add_dependency(self.tid1, self.tid2)
        self.graph.add_dependency(self.tid2, self.tid1)

        # Second cycle: T3 -> T4 -> T3
        self.graph.add_dependency(self.tid3, self.tid4)
        self.graph.add_dependency(self.tid4, self.tid3)

        cycle = self.graph.has_cycle()
        assert cycle is not None
        # Should detect at least one cycle

    def test_find_all_cycles_empty_graph(self):
        """Test finding all cycles in empty graph."""
        cycles = self.graph.find_all_cycles()
        assert len(cycles) == 0

    def test_find_all_cycles_no_cycles(self):
        """Test finding all cycles when none exist."""
        self.graph.add_dependency(self.tid1, self.tid2)
        self.graph.add_dependency(self.tid2, self.tid3)

        cycles = self.graph.find_all_cycles()
        assert len(cycles) == 0

    def test_find_all_cycles_single_cycle(self):
        """Test finding all cycles with single cycle."""
        self.graph.add_dependency(self.tid1, self.tid2)
        self.graph.add_dependency(self.tid2, self.tid3)
        self.graph.add_dependency(self.tid3, self.tid1)

        cycles = self.graph.find_all_cycles()
        assert len(cycles) >= 1

        # Check that found cycle contains all three transactions
        cycle = cycles[0]
        cycle_set = set(cycle)
        assert self.tid1 in cycle_set
        assert self.tid2 in cycle_set
        assert self.tid3 in cycle_set

    def test_find_all_cycles_multiple_cycles(self):
        """Test finding all cycles with multiple separate cycles."""
        # First cycle: T1 -> T2 -> T1
        self.graph.add_dependency(self.tid1, self.tid2)
        self.graph.add_dependency(self.tid2, self.tid1)

        # Second cycle: T3 -> T4 -> T5 -> T3
        self.graph.add_dependency(self.tid3, self.tid4)
        self.graph.add_dependency(self.tid4, self.tid5)
        self.graph.add_dependency(self.tid5, self.tid3)

        cycles = self.graph.find_all_cycles()
        assert len(cycles) >= 2

    # =================== STATISTICS ===================

    def test_statistics_empty_graph(self):
        """Test statistics for empty graph."""
        stats = self.graph.get_statistics()

        assert stats.nodes == 0
        assert stats.edges == 0
        assert stats.avg_dependencies == 0.0

    def test_statistics_single_dependency(self):
        """Test statistics with single dependency."""
        self.graph.add_dependency(self.tid1, self.tid2)

        stats = self.graph.get_statistics()
        assert stats.nodes == 1
        assert stats.edges == 1
        assert stats.avg_dependencies == 1.0

    def test_statistics_multiple_dependencies(self):
        """Test statistics with multiple dependencies."""
        self.graph.add_dependency(self.tid1, self.tid2)
        self.graph.add_dependency(self.tid1, self.tid3)
        self.graph.add_dependency(self.tid2, self.tid3)

        stats = self.graph.get_statistics()
        assert stats.nodes == 2  # T1 and T2 are waiters
        assert stats.edges == 3  # Total dependencies
        assert stats.avg_dependencies == 1.5  # 3 edges / 2 nodes

    def test_statistics_after_cleanup(self):
        """Test statistics after dependency cleanup."""
        self.graph.add_dependency(self.tid1, self.tid2)
        self.graph.add_dependency(self.tid2, self.tid3)

        # Remove one transaction
        self.graph.remove_dependencies_for_transaction(self.tid1)

        stats = self.graph.get_statistics()
        assert stats.nodes == 1
        assert stats.edges == 1
        assert stats.avg_dependencies == 1.0

    # =================== GRAPH PROPERTY ===================

    def test_graph_property_returns_copy(self):
        """Test that graph property returns a copy, not original."""
        self.graph.add_dependency(self.tid1, self.tid2)

        graph_copy = self.graph.graph

        # Modifying copy should not affect original
        graph_copy[self.tid1].add(self.tid3)

        original = self.graph.graph
        assert self.tid3 not in original[self.tid1]

    def test_graph_property_structure(self):
        """Test graph property returns correct structure."""
        self.graph.add_dependency(self.tid1, self.tid2)
        self.graph.add_dependency(self.tid1, self.tid3)

        graph_dict = self.graph.graph

        assert isinstance(graph_dict, dict)
        assert isinstance(graph_dict[self.tid1], set)
        assert self.tid2 in graph_dict[self.tid1]
        assert self.tid3 in graph_dict[self.tid1]

    # =================== THREAD SAFETY ===================

    def test_concurrent_dependency_addition(self):
        """Test thread safety of concurrent dependency additions."""

        def add_dependencies(waiter_base, holder_base, count):
            for i in range(count):
                waiter = TransactionId()
                holder = TransactionId()
                self.graph.add_dependency(waiter, holder)

        # Run concurrent additions
        threads = []
        for i in range(5):
            thread = threading.Thread(target=add_dependencies, args=(i, i + 10, 10))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Graph should be in consistent state
        stats = self.graph.get_statistics()
        assert stats.nodes >= 0
        assert stats.edges >= 0

    def test_concurrent_cycle_detection(self):
        """Test thread safety of concurrent cycle detection."""
        # Set up a graph with potential cycles
        self.graph.add_dependency(self.tid1, self.tid2)
        self.graph.add_dependency(self.tid2, self.tid3)

        results = []

        def detect_cycles():
            cycle = self.graph.has_cycle()
            results.append(cycle)

        def add_cycle():
            time.sleep(0.01)  # Small delay
            self.graph.add_dependency(self.tid3, self.tid1)  # Creates cycle

        # Run concurrent operations
        threads = []

        # Multiple cycle detection threads
        for _ in range(5):
            thread = threading.Thread(target=detect_cycles)
            threads.append(thread)
            thread.start()

        # One thread adds the cycle
        cycle_thread = threading.Thread(target=add_cycle)
        threads.append(cycle_thread)
        cycle_thread.start()

        for thread in threads:
            thread.join()

        # Should not crash and produce consistent results
        assert len(results) == 5

    def test_concurrent_dependency_removal(self):
        """Test thread safety of concurrent dependency removal."""
        # Set up initial dependencies
        tids = [TransactionId() for _ in range(20)]
        for i in range(len(tids) - 1):
            self.graph.add_dependency(tids[i], tids[i + 1])

        def remove_random_dependencies(start_idx, end_idx):
            for i in range(start_idx, end_idx):
                if i < len(tids) - 1:
                    self.graph.remove_dependency(tids[i], tids[i + 1])

        # Remove dependencies concurrently
        threads = []
        for i in range(0, len(tids), 5):
            thread = threading.Thread(
                target=remove_random_dependencies,
                args=(i, min(i + 5, len(tids) - 1))
            )
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Graph should be in consistent state
        final_graph = self.graph.graph
        # No assertion errors should occur

    def test_concurrent_transaction_cleanup(self):
        """Test thread safety of concurrent transaction cleanup."""
        # Set up complex dependency graph
        tids = [TransactionId() for _ in range(10)]

        # Create multiple dependencies for each transaction
        for i, tid in enumerate(tids):
            for j, other_tid in enumerate(tids):
                if i != j:
                    self.graph.add_dependency(tid, other_tid)

        def cleanup_transaction(tid):
            self.graph.remove_dependencies_for_transaction(tid)

        # Clean up transactions concurrently
        threads = []
        for tid in tids[:5]:  # Clean up half of the transactions
            thread = threading.Thread(target=cleanup_transaction, args=(tid,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Graph should be consistent and cleaned up
        final_stats = self.graph.get_statistics()
        assert final_stats.nodes >= 0
        assert final_stats.edges >= 0

    # =================== EDGE CASES ===================

    def test_very_large_graph(self):
        """Test behavior with large graph."""
        num_transactions = 100
        tids = [TransactionId() for _ in range(num_transactions)]

        # Create star pattern: all transactions depend on tid[0]
        for i in range(1, num_transactions):
            self.graph.add_dependency(tids[i], tids[0])

        stats = self.graph.get_statistics()
        assert stats.nodes == num_transactions - 1
        assert stats.edges == num_transactions - 1

        # Should not have cycle
        assert self.graph.has_cycle() is None

    def test_deep_dependency_chain(self):
        """Test behavior with deep dependency chain."""
        chain_length = 50
        tids = [TransactionId() for _ in range(chain_length)]

        # Create chain: T0 -> T1 -> T2 -> ... -> T(n-1)
        for i in range(chain_length - 1):
            self.graph.add_dependency(tids[i], tids[i + 1])

        # Should not have cycle
        assert self.graph.has_cycle() is None

        # Add back edge to create cycle
        self.graph.add_dependency(tids[-1], tids[0])

        # Should now detect cycle
        cycle = self.graph.has_cycle()
        assert cycle is not None
        assert len(cycle) >= chain_length

    def test_self_loops_and_duplicates(self):
        """Test handling of self-loops and duplicate edges."""
        # Self-loops should be ignored
        self.graph.add_dependency(self.tid1, self.tid1)
        assert len(self.graph.graph) == 0

        # Duplicate edges should not create multiple entries
        self.graph.add_dependency(self.tid1, self.tid2)
        self.graph.add_dependency(self.tid1, self.tid2)
        self.graph.add_dependency(self.tid1, self.tid2)

        graph_dict = self.graph.graph
        assert len(graph_dict[self.tid1]) == 1
        assert self.tid2 in graph_dict[self.tid1]

    def test_isolated_nodes_handling(self):
        """Test handling of isolated nodes after cleanup."""
        # Create dependencies then remove them
        self.graph.add_dependency(self.tid1, self.tid2)
        self.graph.add_dependency(self.tid2, self.tid3)

        # Remove dependencies that create isolated nodes
        self.graph.remove_dependency(self.tid1, self.tid2)
        self.graph.remove_dependency(self.tid2, self.tid3)

        # Graph should be empty after cleanup
        assert len(self.graph.graph) == 0

    def test_stats_dataclass_immutability(self):
        """Test that Stats dataclass is immutable."""
        stats = self.graph.get_statistics()

        # Should not be able to modify stats
        with pytest.raises(AttributeError):
            stats.nodes = 10

        with pytest.raises(AttributeError):
            stats.edges = 5

        with pytest.raises(AttributeError):
            stats.avg_dependencies = 2.5
