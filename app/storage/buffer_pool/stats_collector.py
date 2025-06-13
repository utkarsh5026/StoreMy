import threading
from dataclasses import dataclass
from typing import Any, Dict
from .cache_manager import CacheManager
from .page import PageLoader, PageWriter
from .lock_coordinator import LockCoordinator


class StatisticsCollector:
    """
    Collects and aggregates performance statistics.

    Responsibilities:
    - Aggregate statistics from all components
    - Provide performance metrics
    - Track trends over time
    - Generate reports
    """

    def __init__(self):
        self._operation_times: dict[str, list[float]] = {}
        self._counters: dict[str, int] = {}
        self._lock = threading.RLock()

    def record_operation(self, operation: str, duration: float) -> None:
        """Record the duration of an operation."""
        with self._lock:
            if operation not in self._operation_times:
                self._operation_times[operation] = []
            self._operation_times[operation].append(duration)

            # Keep only recent measurements (last 1000)
            if len(self._operation_times[operation]) > 1000:
                self._operation_times[operation] = self._operation_times[operation][-1000:]

    def increment_counter(self, counter: str, amount: int = 1) -> None:
        """Increment a counter."""
        with self._lock:
            self._counters[counter] = self._counters.get(counter, 0) + amount

    def get_aggregate_statistics(self, cache_mgr: CacheManager,
                                 page_loader: PageLoader, page_writer: PageWriter,
                                 lock_coord: LockCoordinator) -> Dict[str, Any]:
        """Get aggregated statistics from all components."""
        with self._lock:
            stats = {
                'cache': cache_mgr.get_statistics().__dict__,
                'cache_info': cache_mgr.get_cache_info(),
                'page_loading': page_loader.get_load_statistics(),
                'page_writing': page_writer.get_write_statistics(),
                'locking': lock_coord.get_lock_statistics(),
                'operations': {},
                'counters': self._counters.copy()
            }

            # Calculate operation statistics
            for operation, times in self._operation_times.items():
                if times:
                    import statistics
                    stats['operations'][operation] = {
                        'count': len(times),
                        'avg_time': statistics.mean(times),
                        'min_time': min(times),
                        'max_time': max(times),
                        'median_time': statistics.median(times)
                    }

            return stats
