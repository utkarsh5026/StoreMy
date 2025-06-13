"""
Comprehensive tests for the dependency manager module.

This module tests the DependencyGraph, Lock, and LockType classes
with extensive coverage of edge cases and concurrency scenarios.
"""

import pytest
import threading

from app.concurrency.locks.dependency_manager import DependencyGraph, Lock, LockType
from app.primitives import TransactionId
from app.storage.heap import HeapPageId


class TestLockType:
    """Test the LockType enum."""

    def test_lock_type_values(self):
        """Test that lock types have correct values."""
        assert LockType.SHARED.value == "SHARED"
        assert LockType.EXCLUSIVE.value == "EXCLUSIVE"

    def test_lock_type_equality(self):
        """Test lock type equality comparisons."""
        assert LockType.SHARED == LockType.SHARED
        assert LockType.EXCLUSIVE == LockType.EXCLUSIVE
        assert LockType.SHARED != LockType.EXCLUSIVE

    def test_lock_type_string_representation(self):
        """Test string representation of lock types."""
        assert str(LockType.SHARED) == "LockType.SHARED"
        assert str(LockType.EXCLUSIVE) == "LockType.EXCLUSIVE"


class TestLock:
    """Test the Lock dataclass."""

    def setup_method(self):
        """Set up test fixtures."""
        self.tid1 = TransactionId()
        self.tid2 = TransactionId()
        self.page1 = HeapPageId(1, 0)
        self.page2 = HeapPageId(1, 1)

    def test_lock_creation(self):
        """Test basic lock creation."""
        lock = Lock(self.tid1, self.page1, LockType.SHARED)
        assert lock.transaction_id == self.tid1
        assert lock.page_id == self.page1
        assert lock.lock_type == LockType.SHARED

    def test_lock_immutability(self):
        """Test that locks are immutable (frozen dataclass)."""
        lock = Lock(self.tid1, self.page1, LockType.SHARED)

        # Attempting to modify should raise an error
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
        lock3 = Lock(self.tid2, self.page1, LockType.SHARED)
        lock4 = Lock(self.tid1, self.page2, LockType.SHARED)
        lock5 = Lock(self.tid1, self.page1, LockType.EXCLUSIVE)

        assert lock1 == lock2
        assert lock1 != lock3  # Different transaction
        assert lock1 != lock4  # Different page
        assert lock1 != lock5  # Different lock type

    def test_lock_hashability(self):
        """Test that locks can be used as dictionary keys and in sets."""
        lock1 = Lock(self.tid1, self.page1, LockType.SHARED)
        lock2 = Lock(self.tid1, self.page1, LockType.SHARED)
        lock3 = Lock(self.tid2, self.page1, LockType.SHARED)

        # Test in set
        lock_set = {lock1, lock2, lock3}
        assert len(lock_set) == 2  # lock1 and lock2 are equal

        lock_dict = {lock1: "value1", lock3: "value2"}
        assert len(lock_dict) == 2
        assert lock_dict[lock2] == "value1"

    def test_lock_string_representation(self):
        """Test string representation of locks."""
        lock = Lock(self.tid1, self.page1, LockType.SHARED)
        str_repr = str(lock)

        assert "Lock" in str_repr
        assert str(self.tid1) in str_repr
        assert "SHARED" in str_repr
        assert str(self.page1) in str_repr

    def test_lock_with_different_page_types(self):
        """Test locks with different page ID types."""
        heap_page = HeapPageId(1, 0)
        lock = Lock(self.tid1, heap_page, LockType.EXCLUSIVE)

        assert lock.page_id == heap_page
        assert lock.lock_type == LockType.EXCLUSIVE


class TestDependencyGraph:
    """Test the DependencyGraph class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.graph = DependencyGraph()
        self.tid1 = TransactionId()
        self.tid2 = TransactionId()
        self.tid3 = TransactionId()
        self.tid4 = TransactionId()

    def test_graph_initialization(self):
        """Test that graph initializes correctly."""
        assert len(self.graph.graph) == 0
        assert self.graph.has_cycle() is None

    def test_add_single_dependency(self):
        """Test adding a single dependency."""
        self.graph.add_dependency(self.tid1, self.tid2)

        assert self.tid1 in self.graph.graph
        assert self.tid2 in self.graph.graph[self.tid1]
        assert len(self.graph.graph[self.tid1]) == 1

    def test_add_multiple_dependencies(self):
        """Test adding multiple dependencies for the same transaction."""
        self.graph.add_dependency(self.tid1, self.tid2)
        self.graph.add_dependency(self.tid1, self.tid3)

        assert len(self.graph.graph[self.tid1]) == 2
        assert self.tid2 in self.graph.graph[self.tid1]
        assert self.tid3 in self.graph.graph[self.tid1]

    def test_prevent_self_dependency(self):
        """Test that self-dependencies are prevented."""
        self.graph.add_dependency(self.tid1, self.tid1)

        # Should not create a self-loop
        assert len(self.graph.graph) == 0

    def test_duplicate_dependency_handling(self):
        """Test that duplicate dependencies are handled correctly."""
        self.graph.add_dependency(self.tid1, self.tid2)
        self.graph.add_dependency(self.tid1, self.tid2)  # Duplicate

        # Should only have one dependency
        assert len(self.graph.graph[self.tid1]) == 1
        assert self.tid2 in self.graph.graph[self.tid1]

    def test_simple_cycle_detection(self):
        """Test detection of a simple 2-transaction cycle."""
        # Create cycle: tid1 -> tid2 -> tid1
        self.graph.add_dependency(self.tid1, self.tid2)
        self.graph.add_dependency(self.tid2, self.tid1)

        cycle = self.graph.has_cycle()
        assert cycle is not None
        assert len(cycle) >= 2

        # Check that both transactions are in the cycle
        cycle_set = set(cycle)
        assert self.tid1 in cycle_set
        assert self.tid2 in cycle_set

    def test_complex_cycle_detection(self):
        """Test detection of a complex multi-transaction cycle."""
        # Create cycle: tid1 -> tid2 -> tid3 -> tid1
        self.graph.add_dependency(self.tid1, self.tid2)
        self.graph.add_dependency(self.tid2, self.tid3)
        self.graph.add_dependency(self.tid3, self.tid1)

        cycle = self.graph.has_cycle()
        assert cycle is not None
        assert len(cycle) >= 3

        # Check that all transactions are in the cycle
        cycle_set = set(cycle)
        assert self.tid1 in cycle_set
        assert self.tid2 in cycle_set
        assert self.tid3 in cycle_set

    def test_no_cycle_detection(self):
        """Test that no cycle is detected when none exists."""
        # Create chain: tid1 -> tid2 -> tid3 -> tid4
        self.graph.add_dependency(self.tid1, self.tid2)
        self.graph.add_dependency(self.tid2, self.tid3)
        self.graph.add_dependency(self.tid3, self.tid4)

        cycle = self.graph.has_cycle()
        assert cycle is None

    def test_multiple_disconnected_cycles(self):
        """Test detection when multiple cycles exist."""
        # Create two separate cycles
        # Cycle 1: tid1 -> tid2 -> tid1
        self.graph.add_dependency(self.tid1, self.tid2)
        self.graph.add_dependency(self.tid2, self.tid1)

        # Cycle 2: tid3 -> tid4 -> tid3
        self.graph.add_dependency(self.tid3, self.tid4)
        self.graph.add_dependency(self.tid4, self.tid3)

        cycle = self.graph.has_cycle()
        assert cycle is not None
        # Should detect at least one cycle

    def test_remove_dependencies_for_transaction(self):
        """Test removing all dependencies for a transaction."""
        # Set up dependencies
        self.graph.add_dependency(self.tid1, self.tid2)
        self.graph.add_dependency(self.tid1, self.tid3)
        self.graph.add_dependency(self.tid2, self.tid1)
        self.graph.add_dependency(self.tid3, self.tid2)

        # Remove tid1's dependencies
        self.graph.remove_dependencies_for_transaction(self.tid1)

        # tid1 should no longer be a waiter
        assert self.tid1 not in self.graph.graph

        # tid1 should no longer be in other transactions' dependency lists
        for deps in self.graph.graph.values():
            assert self.tid1 not in deps

    def test_remove_nonexistent_transaction(self):
        """Test removing dependencies for a transaction that doesn't exist."""
        # Should not raise an error
        self.graph.remove_dependencies_for_transaction(self.tid1)
        assert len(self.graph.graph) == 0

    def test_cycle_detection_after_removal(self):
        """Test that cycle detection works correctly after dependency removal."""
        # Create a cycle
        self.graph.add_dependency(self.tid1, self.tid2)
        self.graph.add_dependency(self.tid2, self.tid3)
        self.graph.add_dependency(self.tid3, self.tid1)

        assert self.graph.has_cycle() is not None

        # Remove one transaction to break the cycle
        self.graph.remove_dependencies_for_transaction(self.tid2)

        assert self.graph.has_cycle() is None

    def test_thread_safety_basic(self):
        """Test basic thread safety of dependency graph operations."""
        def add_dependencies(start_tid, end_tid):
            for i in range(10):
                tid1 = TransactionId()
                tid2 = TransactionId()
                self.graph.add_dependency(tid1, tid2)

        # Run multiple threads concurrently
        threads = []
        for i in range(5):
            thread = threading.Thread(target=add_dependencies, args=(i, i+1))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Graph should be in a consistent state
        assert isinstance(self.graph.graph, dict)

    def test_concurrent_cycle_detection(self):
        """Test concurrent cycle detection operations."""
        # Set up a cycle
        self.graph.add_dependency(self.tid1, self.tid2)
        self.graph.add_dependency(self.tid2, self.tid1)

        results = []

        def detect_cycle():
            result = self.graph.has_cycle()
            results.append(result is not None)

        # Run multiple cycle detection operations concurrently
        threads = []
        for _ in range(10):
            thread = threading.Thread(target=detect_cycle)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # All should detect the cycle
        assert all(results)

    def test_concurrent_add_and_remove(self):
        """Test concurrent addition and removal of dependencies."""
        def add_remove_worker():
            for i in range(100):
                tid1 = TransactionId()
                tid2 = TransactionId()

                # Add dependency
                self.graph.add_dependency(tid1, tid2)

                # Immediately remove it
                self.graph.remove_dependencies_for_transaction(tid1)

        # Run multiple workers concurrently
        threads = []
        for _ in range(5):
            thread = threading.Thread(target=add_remove_worker)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Graph should be consistent (likely empty or with few remaining entries)
        assert isinstance(self.graph.graph, dict)

    def test_stress_cycle_detection(self):
        """Stress test cycle detection with many transactions."""
        # Create a large cycle
        transactions = [TransactionId() for _ in range(20)]

        # Create chain
        for i in range(len(transactions) - 1):
            self.graph.add_dependency(transactions[i], transactions[i + 1])

        # Close the cycle
        self.graph.add_dependency(transactions[-1], transactions[0])

        # Should detect the cycle
        cycle = self.graph.has_cycle()
        assert cycle is not None
        assert len(set(cycle)) >= 3  # At least 3 unique transactions in cycle

    def test_graph_property_access(self):
        """Test that the graph property provides access to internal state."""
        self.graph.add_dependency(self.tid1, self.tid2)

        graph_dict = self.graph.graph
        assert isinstance(graph_dict, dict)
        assert self.tid1 in graph_dict
        assert self.tid2 in graph_dict[self.tid1]

    def test_edge_case_empty_graph_operations(self):
        """Test operations on empty graph."""
        # Cycle detection on empty graph
        assert self.graph.has_cycle() is None

        # Removing from empty graph
        self.graph.remove_dependencies_for_transaction(self.tid1)
        assert len(self.graph.graph) == 0

        # Graph access on empty graph
        assert len(self.graph.graph) == 0

    def test_very_large_dependency_set(self):
        """Test with a transaction having many dependencies."""
        # Create one transaction that waits for many others
        waiters = [TransactionId() for _ in range(100)]

        for waiter in waiters:
            self.graph.add_dependency(self.tid1, waiter)

        assert len(self.graph.graph[self.tid1]) == 100

        # Remove all at once
        self.graph.remove_dependencies_for_transaction(self.tid1)
        assert self.tid1 not in self.graph.graph
