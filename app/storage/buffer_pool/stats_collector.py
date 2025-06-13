import threading
import time
import statistics
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from collections import defaultdict, deque

from .cache_manager import CacheManager
from .page import PageLoader, PageWriter
from .transactions import LockCoordinator


@dataclass
class PerformanceMetrics:
    """Comprehensive performance metrics."""
    # Operation counts
    total_operations: int = 0
    successful_operations: int = 0
    failed_operations: int = 0

    # Timing statistics
    total_time: float = 0.0
    min_time: float = float('inf')
    max_time: float = 0.0
    avg_time: float = 0.0
    median_time: float = 0.0
    p95_time: float = 0.0
    p99_time: float = 0.0

    # Recent measurements for percentile calculations
    recent_times: deque = field(default_factory=lambda: deque(maxlen=1000))

    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        return self.successful_operations / max(1, self.total_operations)

    @property
    def failure_rate(self) -> float:
        """Calculate failure rate."""
        return self.failed_operations / max(1, self.total_operations)

    def update_times(self) -> None:
        """Update calculated timing statistics."""
        if self.recent_times:
            times = list(self.recent_times)
            self.avg_time = statistics.mean(times)
            self.median_time = statistics.median(times)

            if len(times) >= 20:  # Need sufficient data for percentiles
                self.p95_time = statistics.quantiles(
                    times, n=20)[18]  # 95th percentile
            if len(times) >= 100:
                self.p99_time = statistics.quantiles(
                    times, n=100)[98]  # 99th percentile


@dataclass
class SystemHealth:
    """System health indicators."""
    # Cache health
    cache_hit_rate: float = 0.0
    cache_utilization: float = 0.0
    dirty_page_ratio: float = 0.0

    # Lock health
    lock_contention_rate: float = 0.0
    deadlock_rate: float = 0.0
    avg_lock_wait_time: float = 0.0

    # Transaction health
    transaction_abort_rate: float = 0.0
    avg_transaction_duration: float = 0.0
    active_transaction_count: int = 0

    # Overall health score (0-100)
    overall_health_score: float = 100.0

    def calculate_health_score(self) -> float:
        """Calculate overall system health score."""
        score = 100.0

        # Penalize for poor cache performance
        if self.cache_hit_rate < 0.8:
            score -= (0.8 - self.cache_hit_rate) * 100

        # Penalize for high lock contention
        if self.lock_contention_rate > 0.1:
            score -= self.lock_contention_rate * 200

        # Penalize for deadlocks
        score -= self.deadlock_rate * 500

        # Penalize for high abort rate
        if self.transaction_abort_rate > 0.05:
            score -= (self.transaction_abort_rate - 0.05) * 400

        # Penalize for high cache utilization (>90%)
        if self.cache_utilization > 0.9:
            score -= (self.cache_utilization - 0.9) * 200

        return max(0.0, min(100.0, score))


class StatisticsCollector:
    """
    Enhanced statistics collector with comprehensive monitoring.

    Features:
    - Detailed performance metrics for all operations
    - System health monitoring with alerts
    - Trend analysis and anomaly detection
    - Configurable monitoring levels
    - Thread-safe operation tracking
    - Automatic cleanup of old data
    """

    def __init__(self, monitoring_level: str = 'standard'):
        """
        Initialize statistics collector.

        Args:
            monitoring_level: 'minimal', 'standard', or 'detailed'
        """
        # Core metrics
        self._operation_metrics: Dict[str, PerformanceMetrics] = defaultdict(
            PerformanceMetrics)
        self._counters: Dict[str, int] = defaultdict(int)
        self._gauges: Dict[str, float] = defaultdict(float)

        # System health
        self._health_history: deque = deque(
            maxlen=100)  # Keep last 100 health checks
        self._last_health_check: float = 0.0
        self._health_check_interval: float = 60.0  # Check every minute

        # Configuration
        self.monitoring_level = monitoring_level
        self._detailed_monitoring = monitoring_level == 'detailed'
        self._enable_health_monitoring = monitoring_level in [
            'standard', 'detailed']

        # Thread safety
        self._lock = threading.RLock()

        # Alerts
        self._alert_thresholds = {
            'cache_hit_rate_low': 0.7,
            'lock_contention_high': 0.2,
            'deadlock_rate_high': 0.01,
            'abort_rate_high': 0.1,
            'response_time_high': 1.0  # seconds
        }
        self._active_alerts: Set[str] = set()

        # Start background health monitoring if enabled
        if self._enable_health_monitoring:
            self._start_health_monitoring()

    def record_operation(self, operation: str, duration: float, success: bool = True) -> None:
        """
        Record an operation with comprehensive tracking.

        Args:
            operation: Operation name
            duration: Duration in seconds
            success: Whether operation succeeded
        """
        with self._lock:
            metrics = self._operation_metrics[operation]

            # Update counts
            metrics.total_operations += 1
            if success:
                metrics.successful_operations += 1
            else:
                metrics.failed_operations += 1

            # Update timing
            metrics.total_time += duration
            metrics.min_time = min(metrics.min_time, duration)
            metrics.max_time = max(metrics.max_time, duration)

            # Add to recent times for percentile calculation
            metrics.recent_times.append(duration)

            # Update calculated statistics
            metrics.update_times()

            # Check for performance alerts
            if self._enable_health_monitoring:
                self._check_performance_alerts(operation, duration, success)

    def increment_counter(self, counter: str, amount: int = 1) -> None:
        """Increment a counter."""
        with self._lock:
            self._counters[counter] += amount

    def set_gauge(self, gauge: str, value: float) -> None:
        """Set a gauge value."""
        with self._lock:
            self._gauges[gauge] = value

    def record_cache_operation(self, operation_type: str, hit: bool, duration: float) -> None:
        """Record cache-specific operations."""
        self.record_operation(f'cache_{operation_type}', duration, True)

        if hit:
            self.increment_counter('cache_hits')
        else:
            self.increment_counter('cache_misses')

    def record_lock_operation(self, operation_type: str, success: bool, wait_time: float) -> None:
        """Record lock-specific operations."""
        self.record_operation(f'lock_{operation_type}', wait_time, success)

        if success:
            self.increment_counter('locks_acquired')
        else:
            self.increment_counter('locks_denied')

    def record_transaction_operation(self, operation_type: str, duration: float, success: bool = True) -> None:
        """Record transaction-specific operations."""
        self.record_operation(
            f'transaction_{operation_type}', duration, success)

        if operation_type == 'commit' and success:
            self.increment_counter('transactions_committed')
        elif operation_type == 'abort':
            self.increment_counter('transactions_aborted')

    def get_aggregate_statistics(self, cache_mgr: CacheManager,
                                 page_loader: PageLoader, page_writer: PageWriter,
                                 lock_coord: LockCoordinator) -> Dict[str, Any]:
        """Get comprehensive aggregated statistics."""
        with self._lock:
            # Get component statistics
            cache_stats = cache_mgr.get_statistics()
            cache_info = cache_mgr.get_cache_info()
            load_stats = page_loader.get_load_statistics()
            write_stats = page_writer.get_write_statistics()
            lock_stats = lock_coord.get_lock_statistics()

            # Calculate derived metrics
            total_cache_requests = cache_stats.hits + cache_stats.misses
            cache_hit_rate = cache_stats.hits / max(1, total_cache_requests)

            # Compile comprehensive stats
            stats = {
                # Component statistics
                'cache': {
                    'hits': cache_stats.hits,
                    'misses': cache_stats.misses,
                    'hit_rate': cache_hit_rate,
                    'evictions': cache_stats.evictions,
                    'size': cache_info.size,
                    'capacity': cache_info.capacity,
                    'utilization': cache_info.utilization,
                    'dirty_pages': cache_info.dirty_pages,
                    'pinned_pages': cache_info.pinned_pages
                },

                'page_loading': {
                    'successful_loads': load_stats.successful_loads,
                    'failed_loads': load_stats.failed_loads,
                    'success_rate': load_stats.success_rate,
                    'avg_load_time': load_stats.avg_load_time,
                    'total_load_time': load_stats.total_load_time
                },

                'page_writing': {
                    'successful_writes': write_stats.successful_writes,
                    'failed_writes': write_stats.failed_writes,
                    'success_rate': write_stats.success_rate,
                    'avg_write_time': write_stats.avg_write_time,
                    'total_write_time': write_stats.total_write_time
                },

                'locking': {
                    'locks_acquired': lock_stats.locks_acquired,
                    'locks_denied': lock_stats.locks_denied,
                    'locks_timeout': lock_stats.locks_timeout,
                    'deadlocks_detected': lock_stats.deadlocks_detected,
                    'lock_upgrades': lock_stats.lock_upgrades,
                    'success_rate': lock_stats.success_rate,
                    'avg_wait_time': lock_stats.avg_wait_time
                },

                # Operation metrics
                'operations': self._get_operation_metrics(),

                # Simple counters and gauges
                'counters': dict(self._counters),
                'gauges': dict(self._gauges),

                # System health
                'health': self._calculate_current_health(
                    cache_hit_rate, cache_info, lock_stats
                ).__dict__ if self._enable_health_monitoring else None,

                # Monitoring configuration
                'monitoring': {
                    'level': self.monitoring_level,
                    'detailed_monitoring': self._detailed_monitoring,
                    'health_monitoring': self._enable_health_monitoring,
                    'active_alerts': list(self._active_alerts)
                }
            }

            return stats

    def _get_operation_metrics(self) -> Dict[str, Dict[str, Any]]:
        """Get formatted operation metrics."""
        metrics = {}

        for operation, perf_metrics in self._operation_metrics.items():
            metrics[operation] = {
                'total_operations': perf_metrics.total_operations,
                'successful_operations': perf_metrics.successful_operations,
                'failed_operations': perf_metrics.failed_operations,
                'success_rate': perf_metrics.success_rate,
                'failure_rate': perf_metrics.failure_rate,
                'total_time': perf_metrics.total_time,
                'avg_time': perf_metrics.avg_time,
                'min_time': perf_metrics.min_time if perf_metrics.min_time != float('inf') else 0.0,
                'max_time': perf_metrics.max_time,
                'median_time': perf_metrics.median_time,
                'p95_time': perf_metrics.p95_time,
                'p99_time': perf_metrics.p99_time
            }

        return metrics

    def _calculate_current_health(self, cache_hit_rate: float, cache_info, lock_stats) -> SystemHealth:
        """Calculate current system health."""
        health = SystemHealth()

        # Cache health
        health.cache_hit_rate = cache_hit_rate
        health.cache_utilization = cache_info.utilization
        health.dirty_page_ratio = cache_info.dirty_pages / \
            max(1, cache_info.size)

        # Lock health
        total_lock_requests = lock_stats.locks_acquired + \
            lock_stats.locks_denied + lock_stats.locks_timeout
        health.lock_contention_rate = (
            lock_stats.locks_denied + lock_stats.locks_timeout) / max(1, total_lock_requests)
        health.deadlock_rate = lock_stats.deadlocks_detected / \
            max(1, total_lock_requests)
        health.avg_lock_wait_time = lock_stats.avg_wait_time

        # Transaction health
        total_transactions = self._counters.get(
            'transactions_committed', 0) + self._counters.get('transactions_aborted', 0)
        health.transaction_abort_rate = self._counters.get(
            'transactions_aborted', 0) / max(1, total_transactions)

        # Get average transaction duration from operation metrics
        if 'transaction_commit' in self._operation_metrics:
            health.avg_transaction_duration = self._operation_metrics['transaction_commit'].avg_time

        # Calculate overall health score
        health.overall_health_score = health.calculate_health_score()

        return health

    def _check_performance_alerts(self, operation: str, duration: float, success: bool) -> None:
        """Check for performance alerts and anomalies."""
        alerts_triggered = []

        # Check response time
        if duration > self._alert_thresholds['response_time_high']:
            alert = f'high_response_time_{operation}'
            if alert not in self._active_alerts:
                alerts_triggered.append(alert)
                self._active_alerts.add(alert)

        # Check failure rate
        if not success:
            metrics = self._operation_metrics[operation]
            if metrics.total_operations >= 10:  # Need minimum sample size
                failure_rate = metrics.failure_rate
                if failure_rate > 0.1:  # More than 10% failure rate
                    alert = f'high_failure_rate_{operation}'
                    if alert not in self._active_alerts:
                        alerts_triggered.append(alert)
                        self._active_alerts.add(alert)

        # Log alerts (in production, this would send to monitoring system)
        for alert in alerts_triggered:
            print(
                f"ALERT: {alert} - {operation} duration: {duration:.3f}s, success: {success}")

    def _start_health_monitoring(self) -> None:
        """Start background health monitoring."""
        def health_monitor():
            while True:
                try:
                    time.sleep(self._health_check_interval)
                    self._periodic_health_check()
                except Exception as e:
                    print(f"Error in health monitoring: {e}")

        health_thread = threading.Thread(target=health_monitor, daemon=True)
        health_thread.start()

    def _periodic_health_check(self) -> None:
        """Perform periodic health checks."""
        current_time = time.time()

        # Only check if enough time has passed
        if current_time - self._last_health_check < self._health_check_interval:
            return

        with self._lock:
            # Clear old alerts that may have resolved
            self._clear_resolved_alerts()

            self._last_health_check = current_time

    def _clear_resolved_alerts(self) -> None:
        """Clear alerts that may have been resolved."""
        # This would implement logic to clear alerts that are no longer active
        # For now, we'll just clear all alerts periodically to prevent accumulation
        if len(self._active_alerts) > 10:
            self._active_alerts.clear()

    def get_health_history(self) -> List[Dict[str, Any]]:
        """Get system health history."""
        with self._lock:
            return list(self._health_history)

    def get_active_alerts(self) -> List[str]:
        """Get currently active alerts."""
        with self._lock:
            return list(self._active_alerts)

    def set_alert_threshold(self, alert_type: str, threshold: float) -> None:
        """Set custom alert threshold."""
        with self._lock:
            self._alert_thresholds[alert_type] = threshold

    def reset_statistics(self) -> None:
        """Reset all statistics."""
        with self._lock:
            self._operation_metrics.clear()
            self._counters.clear()
            self._gauges.clear()
            self._health_history.clear()
            self._active_alerts.clear()

    def export_metrics(self, format_type: str = 'json') -> str:
        """Export metrics in various formats."""
        if format_type == 'json':
            import json
            # This would get full stats and export as JSON
            # Simplified for now
            return json.dumps(dict(self._counters), indent=2)
        elif format_type == 'prometheus':
            # Export in Prometheus format
            lines = []
            for counter, value in self._counters.items():
                lines.append(f"buffer_pool_{counter} {value}")
            return '\n'.join(lines)
        else:
            raise ValueError(f"Unsupported export format: {format_type}")

    def enable_detailed_monitoring(self, enabled: bool) -> None:
        """Enable or disable detailed monitoring."""
        with self._lock:
            self._detailed_monitoring = enabled
            if enabled:
                self.monitoring_level = 'detailed'
            elif self.monitoring_level == 'detailed':
                self.monitoring_level = 'standard'
