# Index Performance Integration Tests

This document describes the integration tests that demonstrate the performance benefits of using indexes versus sequential scans in the StoreMy database.

## Overview

The `index_performance_test.go` file contains comprehensive tests that measure and compare query execution performance with and without indexes. These tests demonstrate that indexed queries can be significantly faster than sequential table scans, especially for selective queries.

## Test Scenarios

### 1. Equality Query Performance (`TestIndexPerformance_EqualityQuery`)

**Purpose:** Demonstrates the performance benefit of indexes for equality predicates (e.g., `WHERE email = 'value'`)

**Setup:**
- Creates a `users` table with 500 rows
- Columns: id (INT), email (STRING), age (INT), score (INT)

**Test Steps:**
1. Executes SELECT query with equality predicate WITHOUT index (sequential scan)
2. Creates a hash index on the `email` column
3. Executes the same SELECT query WITH index (index scan)
4. Compares execution times and calculates speedup

**Expected Result:** Index scan should be faster than sequential scan for single-row lookups.

---

### 2. Range Query Performance (`TestIndexPerformance_RangeQuery`)

**Purpose:** Shows how indexes improve range query performance (e.g., `WHERE price BETWEEN x AND y`)

**Setup:**
- Creates a `products` table with 500 rows
- Columns: id (INT), name (STRING), price (INT), category (STRING)

**Test Steps:**
1. Executes range query WITHOUT index on `price` column
2. Creates index on `price`
3. Executes same range query WITH index
4. Compares performance metrics

**Expected Result:** Index scan should significantly reduce execution time for range queries.

---

### 3. High Selectivity Query (`TestIndexPerformance_HighSelectivity`)

**Purpose:** Demonstrates index behavior when many rows match the query (high selectivity ~80%)

**Setup:**
- Creates an `orders` table with 500 rows
- 80% of rows have status = 'pending'
- 20% have status = 'completed'

**Test Steps:**
1. Query for 'pending' status WITHOUT index (matches ~400 rows)
2. Create index on `status`
3. Query for 'pending' status WITH index
4. Compare performance

**Key Insight:** When most rows match (high selectivity), indexes may provide less benefit because the database must still read most of the table. Sequential scan might be comparable or even faster in some cases.

---

### 4. Low Selectivity Query (`TestIndexPerformance_LowSelectivity`)

**Purpose:** Shows maximum index benefit when few rows match (low selectivity ~1%)

**Setup:**
- Creates an `events` table with 1,000 rows
- 99% of rows are 'view' events
- Only 1% are 'purchase' events

**Test Steps:**
1. Query for 'purchase' events WITHOUT index (matches ~10 rows)
2. Create index on `event_type`
3. Query for 'purchase' events WITH index
4. Measure speedup

**Expected Result:** **Maximum speedup** - This is where indexes shine! Only 1% of rows match, so the index can skip 99% of the table.

---

### 5. Comprehensive Benchmark (`TestIndexPerformance_Comprehensive`)

**Purpose:** Tests performance across multiple data sizes to show how indexes scale

**Setup:**
- Tests with 100, 250, 500, and 1000 rows
- Creates `benchmark` table with `search_key` column

**Test Steps:**
- For each data size:
  1. Measure sequential scan performance
  2. Create index
  3. Measure index scan performance
  4. Calculate speedup factor

**Output:** Generates comprehensive JSON and HTML reports showing performance trends across different data sizes.

---

## Running the Tests

### Run Individual Tests

```bash
# Test equality queries
go test -v -run TestIndexPerformance_EqualityQuery ./pkg/integration/

# Test range queries
go test -v -run TestIndexPerformance_RangeQuery ./pkg/integration/

# Test high selectivity
go test -v -run TestIndexPerformance_HighSelectivity ./pkg/integration/

# Test low selectivity
go test -v -run TestIndexPerformance_LowSelectivity ./pkg/integration/

# Run comprehensive benchmark (skip with -short flag)
go test -v -run TestIndexPerformance_Comprehensive ./pkg/integration/
```

### Run All Performance Tests

```bash
go test -v -run TestIndexPerformance ./pkg/integration/
```

### Skip Long-Running Tests

```bash
go test -short -v -run TestIndexPerformance ./pkg/integration/
```

---

## Performance Metrics

Each test measures:

- **Execution Time:** Wall-clock time to execute the query
- **Row Count:** Total rows in the table
- **Matched Rows:** Number of rows matching the WHERE clause
- **Selectivity:** Percentage of rows matched (matched/total)
- **Memory Allocation:** Memory used during query execution
- **Speedup Factor:** How much faster the index scan is compared to sequential scan

---

## Output Files

Test results are saved to `.benchmarks/` directory:

### JSON Files

```
.benchmarks/json/
├── index_equality_<timestamp>.json
├── index_equality_latest.json
├── index_range_<timestamp>.json
├── index_range_latest.json
├── index_high_selectivity_<timestamp>.json
├── index_low_selectivity_<timestamp>.json
└── index_comprehensive_<timestamp>.json
```

### HTML Reports

```
.benchmarks/html/
├── index_equality_latest.html
├── index_range_latest.html
├── index_high_selectivity_latest.html
├── index_low_selectivity_latest.html
└── index_comprehensive_latest.html
```

HTML reports include:
- Performance comparison charts (using Chart.js)
- Summary statistics (max speedup, average speedup)
- Detailed result tables
- Key insights and analysis

---

## Understanding Selectivity

**Selectivity** is the percentage of rows that match a query's WHERE clause:

- **Low Selectivity (1-10%):** Few rows match → **Indexes are HIGHLY beneficial**
  - Example: Finding users in a specific city from a global user table
  - Index can skip 90-99% of the table

- **Medium Selectivity (10-50%):** Moderate number of rows match → **Indexes are beneficial**
  - Example: Finding products in a specific price range
  - Index still provides good performance gains

- **High Selectivity (50-100%):** Most rows match → **Indexes provide less benefit**
  - Example: Finding active users when 80% are active
  - Sequential scan might be comparable since most rows are read anyway

---

## Known Limitations

### Hash Index Issues

As of the current implementation, there may be issues with hash index creation:
- Error: `unable to get dbFile for page HashPageID`
- This appears to be a bug in the hash index PageIO registration
- Tests gracefully handle index creation failures and log warnings
- The sequential scan performance is still measured and reported

### Index Type Selection

Currently, the database may use hash indexes by default. Future improvements could:
- Support explicit index type specification (HASH vs B-TREE)
- Implement query optimizer to choose between sequential and index scans
- Add cost-based optimization considering selectivity

---

## Performance Expectations

Based on typical database behavior:

| Selectivity | Expected Speedup | Use Case |
|-------------|------------------|----------|
| 1% (Low) | **10-100x faster** | Searching for rare events, specific IDs |
| 10% (Medium-Low) | **5-20x faster** | Category filters, date ranges |
| 50% (Medium-High) | **1-3x faster** | Common status values |
| 80%+ (High) | **0.5-2x faster** | May not benefit much from index |

---

## Example Test Output

```
=== Test 1: Sequential Scan (Low Selectivity - 1% match) ===
Sequential scan: 45.23ms, Matched: 10 rows (1.00% selectivity)

=== Creating Index on event_type ===
Index created in 234.56ms

=== Test 2: Index Scan (Low Selectivity - 1% match) ===
Index scan: 2.15ms, Matched: 10 rows (1.00% selectivity)

*** MAXIMUM SPEEDUP: 21.04x faster with index ***
Low selectivity queries (1.00%) benefit most from indexes!
```

---

## Future Improvements

1. **Add B-Tree Index Tests:** Test B-tree indexes alongside hash indexes
2. **Composite Index Tests:** Test multi-column indexes
3. **Index vs Full Table Scan Decision:** Implement query optimizer statistics
4. **Larger Datasets:** Test with 10K, 100K, 1M rows to show scaling
5. **Disk I/O Metrics:** Measure page reads to show I/O reduction
6. **UPDATE/DELETE Performance:** Test how indexes affect write operations
7. **Index Maintenance Overhead:** Measure index creation and update costs

---

## Contributing

When adding new performance tests:

1. Keep row counts reasonable (500-1000 rows) for fast test execution
2. Test both sequential and index scan paths
3. Measure and log:
   - Execution time
   - Row counts and selectivity
   - Memory usage
4. Handle index creation failures gracefully
5. Generate JSON and HTML reports for comparison
6. Document the test scenario in this file

---

## References

- Sequential Scan Implementation: [pkg/execution/query/seqscan.go](../../execution/query/seqscan.go)
- Index Scan Implementation: [pkg/execution/query/indexscan.go](../../execution/query/indexscan.go)
- Query Planner: [pkg/planner/query.go](../../planner/query.go)
- Integration Test Helpers: [helpers.go](./helpers.go)

---

*Generated for StoreMy Database System - Performance Testing Suite*
