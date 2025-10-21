# Cost Model

## Table of Contents
- [What is a Cost Model?](#what-is-a-cost-model)
- [Why Cost Estimation Matters](#why-cost-estimation-matters)
- [Core Concepts](#core-concepts)
- [Access Methods](#access-methods)
- [Join Algorithms](#join-algorithms)
- [Other Operators](#other-operators)
- [Advanced Features](#advanced-features)
- [How It Works](#how-it-works)
- [Examples](#examples)
- [API Reference](#api-reference)

## What is a Cost Model?

A **cost model** assigns a numeric **cost** to each query execution plan. The cost represents the estimated computational resources needed to execute the plan—primarily **I/O operations** (reading from disk) and **CPU operations** (processing data).

```
Cost = I/O Cost + CPU Cost
```

**I/O Cost:** Time spent reading data from disk (the slowest operation)
**CPU Cost:** Time spent processing data (comparisons, hashing, sorting)

For example:
- **Sequential scan of 1000 pages**: I/O cost = 1000 × 0.1 = 100 (sequential is cheap)
- **Random reads of 1000 pages**: I/O cost = 1000 × 1.0 = 1000 (random is expensive!)
- **Processing 100,000 tuples**: CPU cost = 100,000 × 0.01 = 1000

The optimizer uses these costs to **compare different query plans** and choose the cheapest one.

## Why Cost Estimation Matters

The cost model is the **decision-making engine** of the query optimizer. Even with perfect cardinality estimates, without accurate cost modeling, the optimizer will make poor choices.

### Example: Index vs. Table Scan

```sql
SELECT * FROM users WHERE age > 30
```

**Plan A: Sequential Scan**
```
Read all pages sequentially: 1000 pages × 0.1 = 100 cost
Process all rows: 100,000 × 0.01 = 1000 cost
Total: 1,100 cost
```

**Plan B: Index Scan**
```
Index lookup: 3 pages × 1.0 = 3 cost
Random table reads: 60,000 rows ÷ 100 per page = 600 pages × 1.0 = 600 cost
Process returned rows: 60,000 × 0.01 = 600 cost
Total: 1,203 cost
```

**Plan C: Index Scan (with better selectivity)**
```sql
SELECT * FROM users WHERE age > 90  -- Very selective!
```
```
Index lookup: 3 pages × 1.0 = 3 cost
Random table reads: 100 rows ÷ 100 per page = 1 page × 1.0 = 1 cost
Process returned rows: 100 × 0.01 = 1 cost
Total: 5 cost (200× cheaper than sequential scan!)
```

**The Lesson:** The best access method depends on:
- **Selectivity**: How many rows are returned?
- **Data distribution**: Are index entries clustered or scattered?
- **I/O patterns**: Sequential vs. random access

### Example: Join Algorithm Selection

```sql
SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id
```

**Plan A: Hash Join**
```
Build hash table on customers (10K rows): 10,000 × 0.02 = 200 cost
Probe with orders (100K rows): 100,000 × 0.015 = 1,500 cost
Total: 1,700 cost
```

**Plan B: Nested Loop Join**
```
Outer loop (customers): 10,000 × 0.01 = 100 cost
Inner loop (orders for each customer): 10,000 × 100,000 × 0.01 = 10,000,000 cost
Total: 10,000,100 cost (5900× more expensive!)
```

**Plan C: Sort-Merge Join**
```
Sort customers: 10,000 × log(10,000) × 0.01 = 1,330 cost
Sort orders: 100,000 × log(100,000) × 0.01 = 16,610 cost
Merge: 110,000 × 0.01 = 1,100 cost
Total: 19,040 cost
```

**The Winner:** Hash join (1,700 cost) is 11× cheaper than sort-merge and 5900× cheaper than nested loop!

### Impact of Wrong Cost Estimates

**Scenario: 10× cost underestimate**
```
Estimated: 1,000 cost → Chosen plan
Actual: 10,000 cost → Query 10× slower than expected
```

**Scenario: Choosing wrong join order**
```
Bad order: Build hash table on 1M rows, probe with 100 rows
  → Hash table uses 80MB memory, slow build phase

Good order: Build hash table on 100 rows, probe with 1M rows
  → Hash table uses 8KB memory, fast build phase

Difference: 1000× performance gap!
```

## Core Concepts

### 1. Cost Components

Every operation has two cost components:

**I/O Cost (Disk Access):**
- Reading pages from disk (or SSD)
- **Sequential I/O**: Reading consecutive pages (10× cheaper)
  - Disk: ~100 MB/s sequential read
  - Enables prefetching and read-ahead
- **Random I/O**: Reading scattered pages (expensive)
  - Disk: ~100 IOPS (reads per second)
  - Each seek takes ~10ms

**CPU Cost (Processing):**
- Tuple processing (filtering, projection)
- Comparisons (sorting, joining)
- Hashing (hash joins, aggregations)
- Expression evaluation

**Formula:**
```
TotalCost = (PageReads × IOCostPerPage) + (TuplesProcessed × CPUCostPerTuple)
```

### 2. Cost Parameters

The cost model uses tunable parameters to match hardware characteristics:

```go
// Default cost parameters
IoCostPerPage = 1.0              // Baseline I/O cost
SequentialIoCostFactor = 0.1     // Sequential is 10× cheaper
CPUCostPerTuple = 0.01           // CPU cost per tuple

// Memory parameters
DefaultMemoryPages = 1000        // ~8MB buffer pool
DefaultHashTableMemory = 500     // Half for hash operations
DefaultSortMemory = 500          // Half for sorting
```

**Calibration Example:**

For a system with:
- **Fast SSD**: Set `SequentialIoCostFactor = 0.5` (sequential only 2× cheaper than random)
- **Slow CPU**: Set `CPUCostPerTuple = 0.02` (double the CPU cost)
- **Large memory**: Set `DefaultMemoryPages = 10000` (80MB buffer pool)

### 3. Cost Units

Costs are in **arbitrary units**, not seconds or milliseconds. What matters is the **relative cost** between plans:

```
Plan A: Cost 1,000
Plan B: Cost 10,000
→ Plan A is 10× better than Plan B
```

The optimizer picks the plan with the **lowest cost**, regardless of the absolute values.

### 4. The Cost Estimation Pipeline

```
Query Plan Node
      ↓
  CostModel.EstimatePlanCost()
      ↓
   ├─> Get cardinality (from CardinalityEstimator)
   ├─> Estimate child costs (recursive)
   ├─> Calculate operator-specific cost
   │   ├─> I/O cost (page reads)
   │   └─> CPU cost (tuple processing)
   └─> Return total cost
```

## Access Methods

Access methods determine **how data is retrieved** from tables. The cost model supports three primary access methods:

### 1. Sequential Scan (Table Scan)

**How it works:**
- Read all pages of the table sequentially from start to end
- Process each tuple, applying WHERE predicates
- No index required

**Cost Formula:**
```
I/O Cost = PageCount × IOCostPerPage × SequentialIoCostFactor
CPU Cost = Cardinality × CPUCostPerTuple
Total = I/O Cost + CPU Cost
```

**Example:**
```sql
SELECT * FROM orders WHERE status = 'shipped'

Table: 10,000 pages, 1,000,000 rows
Predicate selectivity: 0.3 (30% match)

I/O Cost: 10,000 × 1.0 × 0.1 = 1,000
CPU Cost: 1,000,000 × 0.01 = 10,000
Total: 11,000 cost
```

**When to use:**
- **Small tables**: Index overhead not justified
- **High selectivity**: Accessing most/all rows (e.g., 50%+)
- **No suitable index**: No index on the filter columns
- **Parallel scan**: Can split work across multiple threads

**Advantages:**
- Sequential I/O is fast (disk prefetching)
- Simple, no index maintenance
- Predictable performance

**Disadvantages:**
- Reads entire table even if selecting few rows
- Doesn't benefit from indexes

### 2. Index Scan

**How it works:**
1. **B-tree traversal**: Navigate from root to leaf nodes
2. **Index leaf scan**: Read matching index entries
3. **Table lookups**: Follow pointers to fetch actual tuples from heap

**Cost Formula:**
```
Index Lookup Cost = BTreeHeight × IOCostPerPage
Table Access Cost = TablePages × IOCostPerPage × (ClusteringFactor + (1 - ClusteringFactor) × SequentialFactor)
CPU Cost = OutputRows × CPUCostPerTuple
Total = Index Lookup + Table Access + CPU
```

**Clustering Factor:**
- **0.0**: Index is perfectly ordered (table is sorted by index key)
  - All table reads are sequential
- **1.0**: Index is completely random (worst case)
  - Every table read is a random I/O
- **0.5**: Mixed (typical case)

**Example 1: Low Clustering (Random Access)**
```sql
SELECT * FROM users WHERE age = 25

Index: 3 levels deep
Result: 10,000 rows (out of 1,000,000)
Clustering factor: 0.9 (poor clustering)
Tuples per page: 100

Index Lookup: 3 × 1.0 = 3
Table Pages: 10,000 / 100 = 100 pages
Table Access: 100 × 1.0 × (0.9 + 0.1 × 0.1) = 91 (mostly random I/O)
CPU: 10,000 × 0.01 = 100
Total: 194 cost
```

**Example 2: High Clustering (Sequential Access)**
```sql
SELECT * FROM orders WHERE order_date = '2024-01-15'

Index on order_date (table is partitioned by date!)
Result: 10,000 rows
Clustering factor: 0.1 (excellent clustering)

Index Lookup: 3 × 1.0 = 3
Table Pages: 100 pages
Table Access: 100 × 1.0 × (0.1 + 0.9 × 0.1) = 19 (mostly sequential I/O)
CPU: 10,000 × 0.01 = 100
Total: 122 cost (37% cheaper than random case!)
```

**When to use:**
- **Low selectivity**: Returning small fraction of rows (< 10%)
- **Good clustering**: Index order matches table order
- **Covering predicates**: Index can filter effectively

**Advantages:**
- Skips irrelevant data
- Efficient for selective queries
- Can return results in sorted order

**Disadvantages:**
- Random I/O to table (if clustering is poor)
- Multiple I/Os (index + table)
- Slower for high selectivity

### 3. Index-Only Scan

**How it works:**
- Retrieve data **directly from index** without accessing table pages
- Only possible when all required columns are in the index
- Index must be a **covering index** for the query

**Cost Formula:**
```
Index Lookup Cost = BTreeHeight × IOCostPerPage
Index Scan Cost = IndexPages × IOCostPerPage × SequentialIoCostFactor
CPU Cost = OutputRows × CPUCostPerTuple × 0.5  (index entries simpler than tuples)
Total = Index Lookup + Index Scan + CPU
```

**Example:**
```sql
-- Index on (customer_id, order_date, amount)
SELECT customer_id, order_date, amount
FROM orders
WHERE customer_id = 12345

Index covers all columns (covering index!)
Result: 100 orders
Index entries per page: 200

Index Lookup: 3 × 1.0 = 3
Index Scan: (100 / 200) × 1.0 × 0.1 = 0.05
CPU: 100 × 0.01 × 0.5 = 0.5
Total: 3.55 cost

Compare to regular index scan:
  Index: 3
  Table access: (100/100) × 1.0 × 0.5 = 0.5
  CPU: 100 × 0.01 = 1
  Total: 4.5 cost (27% more expensive)
```

**When to use:**
- Index contains all needed columns
- Selective query (small result set)
- Read-heavy workload

**Advantages:**
- **No table access** (eliminates random I/O)
- Reads less data (only indexed columns)
- Sequential access through index leaves
- Often 2-10× faster than regular index scan

**Disadvantages:**
- Requires covering index
- Index overhead (storage, maintenance)
- Only works for specific queries

## Join Algorithms

The cost model supports three join algorithms, each with different performance characteristics:

### 1. Hash Join

**Algorithm:**
```
Phase 1: Build hash table on smaller relation
  For each tuple t in BuildRelation:
    hash_table[hash(t.join_key)] = t

Phase 2: Probe hash table with larger relation
  For each tuple s in ProbeRelation:
    bucket = hash_table[hash(s.join_key)]
    For each tuple t in bucket:
      If t.join_key == s.join_key:
        Output (t, s)
```

**Cost Formula (In-Memory):**
```
Build Cost = BuildRows × CPUCostPerTuple × 2.0  (hashing + insertion)
Probe Cost = ProbeRows × CPUCostPerTuple × 1.5  (hashing + lookup)
Total = Build Cost + Probe Cost
```

**Cost Formula (Grace Hash - Spills to Disk):**
```
Partition Cost = (BuildRows + ProbeRows) / TuplesPerPage × IOCostPerPage × 2.0
Build Cost = BuildRows × CPUCostPerTuple × 2.0
Probe Cost = ProbeRows × CPUCostPerTuple × 1.5
Total = Partition Cost + Build Cost + Probe Cost
```

**Example: In-Memory Hash Join**
```sql
SELECT * FROM customers c JOIN orders o ON c.id = o.customer_id

Customers: 10,000 rows (build relation - smaller)
Orders: 100,000 rows (probe relation - larger)
Hash table fits in memory (500 pages available)

Build: 10,000 × 0.01 × 2.0 = 200
Probe: 100,000 × 0.01 × 1.5 = 1,500
Total: 1,700 cost
```

**Example: Grace Hash Join (Spill to Disk)**
```sql
SELECT * FROM large_a a JOIN large_b b ON a.key = b.key

Table A: 1,000,000 rows
Table B: 2,000,000 rows
Hash table doesn't fit (needs 10,000 pages, only 500 available)

Partition I/O: (1M + 2M) / 100 × 1.0 × 2.0 = 60,000
Build: 1,000,000 × 0.01 × 2.0 = 20,000
Probe: 2,000,000 × 0.01 × 1.5 = 30,000
Total: 110,000 cost (65× more expensive than in-memory!)
```

**When to use:**
- **Large unsorted inputs**: No existing sort order
- **Sufficient memory**: Build relation fits in hash table
- **Equi-joins only**: Requires equality predicate (=)
- **High cardinality keys**: Many distinct join key values

**Advantages:**
- O(n + m) complexity (linear in total input size)
- Very fast when fits in memory
- Handles large inputs via grace hash

**Disadvantages:**
- Requires memory for hash table
- Doesn't produce sorted output
- Performance cliff when spilling to disk
- Only works for equi-joins

### 2. Sort-Merge Join

**Algorithm:**
```
Phase 1: Sort both inputs on join key
  LeftSorted = ExternalSort(LeftRelation, join_key)
  RightSorted = ExternalSort(RightRelation, join_key)

Phase 2: Merge sorted inputs
  left_cursor = LeftSorted.begin()
  right_cursor = RightSorted.begin()

  While not EOF:
    If left_cursor.key == right_cursor.key:
      Output all matching pairs
      Advance both cursors
    Else if left_cursor.key < right_cursor.key:
      Advance left_cursor
    Else:
      Advance right_cursor
```

**Cost Formula:**
```
Left Sort Cost = LeftRows × log2(LeftRows) × CPUCostPerTuple + External I/O
Right Sort Cost = RightRows × log2(RightRows) × CPUCostPerTuple + External I/O
Merge Cost = (LeftRows + RightRows) × CPUCostPerTuple
Total = Left Sort + Right Sort + Merge
```

**Example: Sort-Merge Join**
```sql
SELECT * FROM orders o JOIN shipments s ON o.id = s.order_id

Orders: 100,000 rows
Shipments: 100,000 rows

Left sort: 100,000 × log2(100,000) × 0.01 = 16,610
Right sort: 100,000 × log2(100,000) × 0.01 = 16,610
Merge: (100,000 + 100,000) × 0.01 = 2,000
Total: 35,220 cost
```

**When to use:**
- **Inputs already sorted**: Amortize sort cost across multiple operations
- **Hash table doesn't fit**: Large inputs that exceed memory
- **Sorted output needed**: Downstream ORDER BY can be eliminated
- **Range joins**: Supports <, >, <=, >= predicates

**Advantages:**
- Works with any join predicate (=, <, >, etc.)
- Produces sorted output (benefits downstream)
- Gracefully handles large inputs
- No memory requirements beyond sort buffers

**Disadvantages:**
- Expensive sorting phase (O(n log n))
- Slower than hash join for unsorted inputs
- Requires two full sorts

### 3. Nested Loop Join

**Algorithm:**
```
For each tuple t in OuterRelation:  # Outer loop
  For each tuple s in InnerRelation:  # Inner loop (scans entire inner!)
    If t.join_key == s.join_key:
      Output (t, s)
```

**Cost Formula:**
```
Outer Cost = OuterRows × CPUCostPerTuple
Inner Cost = OuterRows × InnerRows × CPUCostPerTuple  # O(n × m)!
Total = Outer Cost + Inner Cost
```

**Example: Nested Loop Join (Disaster!)**
```sql
SELECT * FROM customers c JOIN orders o ON c.id = o.customer_id

Customers: 10,000 rows (outer)
Orders: 100,000 rows (inner)

Outer: 10,000 × 0.01 = 100
Inner: 10,000 × 100,000 × 0.01 = 10,000,000
Total: 10,000,100 cost
```

**Example: Nested Loop with Tiny Inner (Acceptable)**
```sql
SELECT * FROM orders o JOIN order_priorities p ON o.priority_id = p.id

Orders: 100,000 rows (outer)
Priorities: 5 rows (inner - tiny lookup table!)

Outer: 100,000 × 0.01 = 1,000
Inner: 100,000 × 5 × 0.01 = 5,000
Total: 6,000 cost (reasonable for tiny inner)
```

**When to use:**
- **Tiny inner relation**: < 100 rows
- **No equi-join predicate**: Complex join conditions
- **Index on inner**: Index nested loop variant

**Advantages:**
- Simple, no memory requirements
- Works with any join predicate
- Can use indexes on inner relation

**Disadvantages:**
- **Catastrophically expensive** for large inputs (O(n × m))
- Rescans inner relation for every outer tuple
- Almost never the best choice for real data

## Other Operators

### 1. Filter (WHERE Clause)

**Cost Formula:**
```
Child Cost = Cost to produce input tuples
Filter Cost = InputRows × CPUCostPerTuple × FilterFactor × NumPredicates
Total = Child Cost + Filter Cost
```

**Example:**
```sql
SELECT * FROM orders WHERE status = 'shipped' AND amount > 100

Input: 1,000,000 rows
Predicates: 2

Child (scan): 11,000 cost
Filter: 1,000,000 × 0.01 × 1.0 × 2 = 20,000
Total: 31,000 cost
```

**Optimization:** Push filter as close to table scan as possible!

### 2. Projection (SELECT columns)

**Cost Formula:**
```
Child Cost = Cost to produce input tuples
Projection Cost = InputRows × CPUCostPerTuple × 0.5  (lighter than filtering)
Total = Child Cost + Projection Cost
```

**Example:**
```sql
SELECT id, name, email FROM users

Input: 100,000 rows

Child: 11,000 cost
Projection: 100,000 × 0.01 × 0.5 = 500
Total: 11,500 cost
```

### 3. Aggregation (GROUP BY)

**Simple Aggregation (No GROUP BY):**
```
Cost = Child Cost + (InputRows × CPUCostPerTuple × 1.0)
```

**Hash-Based GROUP BY (In-Memory):**
```
Cost = Child Cost + (InputRows × CPUCostPerTuple × 2.0)
```

**Hash-Based GROUP BY (Spills to Disk):**
```
Spill Cost = InputRows / TuplesPerPage × IOCostPerPage × 2.0
Cost = Child Cost + Group Cost + Spill Cost
```

**Example: In-Memory GROUP BY**
```sql
SELECT customer_id, COUNT(*), SUM(amount)
FROM orders
GROUP BY customer_id

Input: 1,000,000 orders
Output: 10,000 groups (distinct customers)
Hash table fits in memory

Child: 11,000 cost
Grouping: 1,000,000 × 0.01 × 2.0 = 20,000
Total: 31,000 cost
```

**Example: Spilling GROUP BY**
```sql
SELECT sku, SUM(quantity) FROM inventory GROUP BY sku

Input: 10,000,000 rows
Output: 1,000,000 distinct SKUs
Hash table needs 10,000 pages, only 500 available

Child: 110,000 cost
Grouping: 10,000,000 × 0.01 × 2.0 = 200,000
Spill: (10,000,000 / 100) × 1.0 × 2.0 = 200,000
Total: 510,000 cost (2.55× more expensive than in-memory!)
```

### 4. Sort (ORDER BY)

**In-Memory Sort:**
```
Cost = Child Cost + (N × log2(N) × CPUCostPerTuple)
```

**External Merge Sort:**
```
Passes = log_M(N / M)  where M = SortMemory
I/O Cost = TotalPages × IOCostPerPage × 2 × Passes
CPU Cost = N × log2(N) × CPUCostPerTuple
Total = Child Cost + I/O Cost + CPU Cost
```

**Example: In-Memory Sort**
```sql
SELECT * FROM users ORDER BY name

Input: 100,000 rows
Fits in memory (1,000 pages available, need 1,000 pages)

Child: 11,000 cost
Sort: 100,000 × log2(100,000) × 0.01 = 16,610
Total: 27,610 cost
```

**Example: External Merge Sort**
```sql
SELECT * FROM orders ORDER BY order_date

Input: 10,000,000 rows
Needs 100,000 pages, only 1,000 available

Initial runs: 100,000 / 1,000 = 100
Merge passes needed: log_1000(100) = ~1 pass

I/O: 100,000 × 1.0 × 2 × 1 = 200,000
CPU: 10,000,000 × log2(10,000,000) × 0.01 = 232,000
Child: 110,000 cost
Total: 542,000 cost
```

### 5. Limit

**Short-Circuit (Pipelined Operators):**
```
Cost = Child Cost × (Limit / ChildCardinality)
```

**No Short-Circuit (Blocking Operators):**
```
Cost = Child Cost  (must materialize entire input)
```

**Example: LIMIT with Sequential Scan (Short-Circuit)**
```sql
SELECT * FROM users LIMIT 10

Input: 1,000,000 rows
Limit: 10
Child is sequential scan (can stop early!)

Limit ratio: 10 / 1,000,000 = 0.00001
Full scan cost: 11,000
Short-circuit cost: 11,000 × 0.00001 = 0.11
Total: ~0.11 cost (99.999% savings!)
```

**Example: LIMIT with Sort (No Short-Circuit)**
```sql
SELECT * FROM users ORDER BY name LIMIT 10

Input: 1,000,000 rows
Limit: 10
Child is sort (blocking - must sort all rows!)

Child (sort): 182,610 cost
Limit: No savings (sort must complete)
Total: 182,610 cost
```

## Advanced Features

### 1. Buffer Pool Cache Modeling

Real databases have a **buffer pool** that caches recently accessed pages in memory. The cost model can account for this:

**Cache Model:**
- Track recent table accesses
- Apply discount factor for cached pages
- Simulate buffer pool eviction

**Example:**
```go
// Enable buffer pool caching
costModel.EnableBufferPoolCaching(
    5 * time.Second,  // Cache window
    0.1,              // 90% discount for cached pages
)

// First scan of table
firstScanCost := 10,000 × 1.0 = 10,000 cost

// Second scan (within 5 seconds)
secondScanCost := 10,000 × 0.1 = 1,000 cost (10× cheaper!)
```

**Use case: Self-joins**
```sql
SELECT * FROM users u1 JOIN users u2 ON u1.manager_id = u2.id

First scan of users: 10,000 cost
Second scan of users: 1,000 cost (cached!)
Total: 11,000 cost instead of 20,000 cost
```

### 2. Clustering Factor

The **clustering factor** measures how well index order matches table order:

**Perfect Clustering (0.0):**
```
Index order: [1, 2, 3, 4, 5, ...]
Table order: [1, 2, 3, 4, 5, ...]
→ Sequential I/O when using index
```

**No Clustering (1.0):**
```
Index order: [1, 2, 3, 4, 5, ...]
Table order: [5, 1, 4, 2, 3, ...]
→ Random I/O for every index entry
```

**Impact on cost:**
```
Good clustering (0.1): 100 pages × (0.1 + 0.9 × 0.1) = 19 cost
Poor clustering (0.9): 100 pages × (0.9 + 0.1 × 0.1) = 91 cost
Difference: 4.8× cost increase!
```

### 3. Memory-Based Optimization

The cost model adjusts for available memory:

**Hash Join:**
- **Fits in memory**: Fast, no I/O
- **Doesn't fit**: Grace hash with partitioning (slow)

**Sort:**
- **Fits in memory**: Quicksort, no I/O
- **Doesn't fit**: External merge sort (multiple passes)

**Aggregation:**
- **Fits in memory**: Single-pass hash aggregation
- **Doesn't fit**: Partition and aggregate (double I/O)

**Example:**
```
Hash join with 10K rows:
  Memory available: 500 pages
  Build relation: 100 pages (fits!)
  Cost: 1,700 (in-memory)

Hash join with 1M rows:
  Memory available: 500 pages
  Build relation: 10,000 pages (doesn't fit!)
  Cost: 110,000 (grace hash with spill)

Difference: 65× more expensive!
```

## How It Works

### End-to-End Example: Complex Query

```sql
SELECT c.name, COUNT(*) as order_count, SUM(o.amount) as total
FROM customers c
JOIN orders o ON c.id = o.customer_id
WHERE c.country = 'USA'
  AND o.status = 'completed'
  AND o.order_date >= '2024-01-01'
GROUP BY c.name
ORDER BY total DESC
LIMIT 10
```

**Query Plan:**
```
Limit (10)
  └─> Sort (total DESC)
       └─> Aggregate (GROUP BY name)
            └─> Hash Join (c.id = o.customer_id)
                 ├─> Filter (country = 'USA')
                 │    └─> Seq Scan (customers)
                 └─> Filter (status = 'completed' AND date >= '2024-01-01')
                      └─> Index Scan (orders on order_date)
```

**Step-by-Step Cost Calculation:**

**1. Seq Scan (customers):**
```
Table: 10,000 pages, 100,000 rows
I/O: 10,000 × 1.0 × 0.1 = 1,000
CPU: 100,000 × 0.01 = 1,000
Cost: 2,000
```

**2. Filter (country = 'USA'):**
```
Input: 100,000 rows
Selectivity: 0.4 (40% are USA)
Output: 40,000 rows

Filter CPU: 100,000 × 0.01 × 1.0 × 1 = 1,000
Total: 2,000 + 1,000 = 3,000
```

**3. Index Scan (orders on order_date):**
```
Index: 3 levels
Result: 500,000 orders since 2024-01-01 (out of 1,000,000)
Clustering: 0.3 (moderate)

Index lookup: 3 × 1.0 = 3
Table pages: 500,000 / 100 = 5,000
Table I/O: 5,000 × 1.0 × (0.3 + 0.7 × 0.1) = 1,850
CPU: 500,000 × 0.01 = 5,000
Cost: 6,853
```

**4. Filter (status = 'completed'):**
```
Input: 500,000 rows
Selectivity: 0.7 (70% completed)
Output: 350,000 rows

Filter CPU: 500,000 × 0.01 × 1.0 × 1 = 5,000
Total: 6,853 + 5,000 = 11,853
```

**5. Hash Join:**
```
Left (customers): 40,000 rows
Right (orders): 350,000 rows
Build on left (smaller)
Hash table fits in memory (400 pages needed, 500 available)

Build: 40,000 × 0.01 × 2.0 = 800
Probe: 350,000 × 0.01 × 1.5 = 5,250
Output: 350,000 matched rows
Output CPU: 350,000 × 0.01 = 3,500

Child costs: 3,000 + 11,853 = 14,853
Join cost: 800 + 5,250 + 3,500 = 9,550
Total: 24,403
```

**6. Aggregate (GROUP BY name):**
```
Input: 350,000 rows
Output: 35,000 groups (distinct customer names)
Hash table fits (350 pages needed, 500 available)

Group CPU: 350,000 × 0.01 × 2.0 = 7,000
Total: 24,403 + 7,000 = 31,403
```

**7. Sort (total DESC):**
```
Input: 35,000 groups
Fits in memory

Sort CPU: 35,000 × log2(35,000) × 0.01 = 5,399
Total: 31,403 + 5,399 = 36,802
```

**8. Limit (10):**
```
Input: 35,000 rows
Limit: 10
Child is sort (blocking - no short-circuit)

Total: 36,802 (no savings)
```

**Final Cost: 36,802**

### Alternative Plan: Different Join Order

What if we joined in the opposite order?

```
Hash Join
 ├─> Filter (status = 'completed')
 │    └─> Seq Scan (orders) [1M rows → 700K after filter]
 └─> Filter (country = 'USA')
      └─> Seq Scan (customers) [100K rows → 40K after filter]
```

**Alternative costs:**
- Scan orders: 110,000 cost
- Filter orders: 10,000 cost
- Scan customers: 2,000 cost
- Filter customers: 1,000 cost
- Hash join:
  - Build on customers: 40,000 × 0.01 × 2.0 = 800
  - Probe with orders: 700,000 × 0.01 × 1.5 = 10,500
  - **Build hash table on 40K rows, probe with 700K rows**

**Alternative total: ~135,300 cost**

**Comparison:**
- **Original plan**: 36,802 cost
- **Alternative plan**: 135,300 cost
- **Difference**: Alternative is 3.7× more expensive!

**Why?**
- Original uses index scan (cheaper than seq scan on large table)
- Original filters before join (reduces join input)
- Original builds hash table on already-filtered data

## Examples

### Example 1: Choosing Between Index and Sequential Scan

```go
costModel := NewCostModel(catalog, tx)

// Plan A: Sequential scan
scanNode := &plan.ScanNode{
    TableID: tableID,
    AccessMethod: "seqscan",
    Predicates: []plan.PredicateInfo{
        {Column: "age", Predicate: primitives.GreaterThan, Value: "30"},
    },
}
scanNode.SetCardinality(600000) // 60% of 1M rows

costA := costModel.EstimatePlanCost(scanNode)
// Result: ~11,000 cost

// Plan B: Index scan
indexScanNode := &plan.ScanNode{
    TableID: tableID,
    AccessMethod: "indexscan",
    IndexID: ageIndexID,
    Predicates: []plan.PredicateInfo{
        {Column: "age", Predicate: primitives.GreaterThan, Value: "30"},
    },
}
indexScanNode.SetCardinality(600000)

costB := costModel.EstimatePlanCost(indexScanNode)
// Result: ~6,100 cost (if clustering is good)

// Optimizer chooses Plan B (45% cheaper)
```

### Example 2: Comparing Join Algorithms

```go
// Hash Join
hashJoin := &plan.JoinNode{
    LeftChild: leftScan,    // 10,000 rows
    RightChild: rightScan,  // 100,000 rows
    JoinMethod: "hash",
}
hashJoin.SetCardinality(100000)

hashCost := costModel.EstimatePlanCost(hashJoin)
// Result: ~1,700 cost

// Sort-Merge Join
mergeJoin := &plan.JoinNode{
    LeftChild: leftScan,
    RightChild: rightScan,
    JoinMethod: "merge",
}
mergeJoin.SetCardinality(100000)

mergeCost := costModel.EstimatePlanCost(mergeJoin)
// Result: ~35,220 cost

// Nested Loop Join
nestedJoin := &plan.JoinNode{
    LeftChild: leftScan,
    RightChild: rightScan,
    JoinMethod: "nested",
}
nestedJoin.SetCardinality(100000)

nestedCost := costModel.EstimatePlanCost(nestedJoin)
// Result: ~10,000,100 cost

// Optimizer chooses hash join (20× cheaper than merge, 5900× cheaper than nested!)
```

### Example 3: Memory Impact on Hash Join

```go
costModel := NewCostModel(catalog, tx)

// Small memory: hash table spills
costModel.SetCostParameters(1.0, 0.01, 100) // Only 100 pages

hashJoin := &plan.JoinNode{
    LeftChild: bigScan,     // 1,000,000 rows (needs 10,000 pages)
    RightChild: hugerScan,  // 2,000,000 rows
    JoinMethod: "hash",
}

spillCost := costModel.EstimatePlanCost(hashJoin)
// Result: ~110,000 cost (grace hash with partitioning)

// Large memory: fits in RAM
costModel.SetCostParameters(1.0, 0.01, 15000) // 15,000 pages (enough!)

fitsInMemoryCost := costModel.EstimatePlanCost(hashJoin)
// Result: ~50,000 cost (in-memory hash join)

// Difference: 2.2× speedup with more memory!
```

### Example 4: LIMIT Optimization

```go
// LIMIT on scan (can short-circuit)
scanWithLimit := &plan.LimitNode{
    Child: scanNode,  // 1,000,000 rows
    Limit: 10,
}

scanCost := costModel.EstimatePlanCost(scanWithLimit)
// Result: ~11 cost (99.999% savings from short-circuit!)

// LIMIT on sort (cannot short-circuit)
sortWithLimit := &plan.LimitNode{
    Child: sortNode,  // Must sort all 1,000,000 rows
    Limit: 10,
}

sortCost := costModel.EstimatePlanCost(sortWithLimit)
// Result: ~182,610 cost (no savings - sort must complete)
```

## API Reference

### CostModel

Main struct for cost estimation:

```go
type CostModel struct {
    catalog              *catalog.SystemCatalog
    cardinalityEstimator *cardinality.CardinalityEstimator
    bufferCache          *BufferPoolCache
    tx                   *transaction.TransactionContext

    IOCostPerPage   float64  // Cost of one page read
    CPUCostPerTuple float64  // Cost of processing one tuple
    MemoryPages     int      // Available buffer pool pages
    HashTableMemory int      // Memory for hash operations
    SortMemory      int      // Memory for sorting
}
```

### Constructor

```go
func NewCostModel(
    cat *catalog.SystemCatalog,
    tx *transaction.TransactionContext,
) (*CostModel, error)
```

Creates a new cost model with default parameters.

### Core Methods

#### EstimatePlanCost

Main entry point for cost estimation.

```go
func (cm *CostModel) EstimatePlanCost(planNode plan.PlanNode) float64
```

**Supported node types:**
- ScanNode (sequential, index, index-only)
- JoinNode (hash, merge, nested loop)
- FilterNode
- ProjectNode
- AggregateNode
- SortNode
- LimitNode

**Returns:** Estimated cost in arbitrary units

#### SetCostParameters

Calibrate cost model for specific hardware.

```go
func (cm *CostModel) SetCostParameters(
    ioCostPerPage float64,
    cpuCostPerTuple float64,
    memoryPages int,
)
```

**Example:**
```go
// Fast SSD, slow CPU, large RAM
costModel.SetCostParameters(
    0.5,    // SSD is 2× faster than default
    0.02,   // CPU is 2× slower than default
    10000,  // 80MB buffer pool
)
```

### Access Method Costs

#### estimateSeqScanCost

```go
func (cm *CostModel) estimateSeqScanCost(stats *systemtable.TableStatistics) float64
```

**Formula:** `PageCount × IOCostPerPage × 0.1 + Cardinality × CPUCostPerTuple`

#### estimateIndexScanCost

```go
func (cm *CostModel) estimateIndexScanCost(
    node *plan.ScanNode,
    stats *systemtable.TableStatistics,
) float64
```

**Formula:** `TreeHeight × IOCost + TablePages × IOCost × ClusteringFactor + CPU`

#### estimateIndexOnlyScanCost

```go
func (cm *CostModel) estimateIndexOnlyScanCost(
    node *plan.ScanNode,
    tableStats *systemtable.TableStatistics,
) float64
```

**Formula:** `TreeHeight × IOCost + IndexPages × IOCost × 0.1 + CPU × 0.5`

### Join Costs

#### estimateHashJoinCost

```go
func (cm *CostModel) estimateHashJoinCost(leftCard, rightCard int64) float64
```

**Formula:**
- In-memory: `BuildCPU + ProbeCPU`
- Grace hash: `BuildCPU + ProbeCPU + PartitionIO × 2`

#### estimateSortMergeJoinCost

```go
func (cm *CostModel) estimateSortMergeJoinCost(leftCard, rightCard int64) float64
```

**Formula:** `LeftSortCost + RightSortCost + MergeCost`

#### estimateNestedLoopJoinCost

```go
func (cm *CostModel) estimateNestedLoopJoinCost(leftCard, rightCard int64) float64
```

**Formula:** `OuterCPU + (OuterRows × InnerRows × CPUCost)`

### Operator Costs

#### estimateFilterCost

```go
func (cm *CostModel) estimateFilterCost(node *plan.FilterNode) float64
```

**Formula:** `ChildCost + InputRows × CPUCost × NumPredicates`

#### estimateProjectCost

```go
func (cm *CostModel) estimateProjectCost(node *plan.ProjectNode) float64
```

**Formula:** `ChildCost + InputRows × CPUCost × 0.5`

#### estimateAggregateCost

```go
func (cm *CostModel) estimateAggregateCost(node *plan.AggregateNode) float64
```

**Formula:**
- Simple: `ChildCost + InputRows × CPUCost`
- Grouped (in-memory): `ChildCost + InputRows × CPUCost × 2`
- Grouped (spill): Above + `InputRows / TuplesPerPage × IOCost × 2`

#### estimateSortCost

```go
func (cm *CostModel) estimateSortCost(node *plan.SortNode) float64
```

**Formula:**
- In-memory: `ChildCost + N × log(N) × CPUCost`
- External: `ChildCost + N × log(N) × CPUCost + Pages × IOCost × 2 × Passes`

#### estimateLimitCost

```go
func (cm *CostModel) estimateLimitCost(node *plan.LimitNode) float64
```

**Formula:**
- Short-circuit: `ChildCost × (Limit / ChildCard)`
- No short-circuit: `ChildCost`

### Advanced Features

#### EnableBufferPoolCaching

Enable buffer pool cache modeling.

```go
func (cm *CostModel) EnableBufferPoolCaching(
    cacheWindow time.Duration,
    cacheDiscount float64,
)
```

**Example:**
```go
costModel.EnableBufferPoolCaching(
    5 * time.Second,  // Consider pages cached for 5 seconds
    0.1,              // 90% I/O discount for cached pages
)
```

#### GetCardinalityEstimator

Access the cardinality estimator.

```go
func (cm *CostModel) GetCardinalityEstimator() *cardinality.CardinalityEstimator
```

### Constants

```go
const (
    // I/O costs
    IoCostPerPage          = 1.0   // Baseline I/O cost
    SequentialIoCostFactor = 0.1   // Sequential is 10× cheaper

    // CPU costs
    CPUCostPerTuple        = 0.01  // Baseline CPU cost
    HashBuildCPUFactor     = 2.0   // Hash table building overhead
    HashProbeCPUFactor     = 1.5   // Hash lookup overhead
    ProjectionCPUFactor    = 0.5   // Projection is lighter
    FilterCPUFactor        = 1.0   // Filter evaluation
    SimpleAggCPUFactor     = 1.0   // Simple aggregation
    GroupAggCPUFactor      = 2.0   // Hash aggregation

    // Memory
    DefaultMemoryPages     = 1000  // ~8MB buffer pool
    DefaultHashTableMemory = 500   // Half for hash ops
    DefaultSortMemory      = 500   // Half for sorting

    // Other
    DefaultTuplesPerPage   = 100   // Default tuple density
    LimitShortCircuitRatio = 0.1   // LIMIT must be < 10% to short-circuit
)
```

## Key Takeaways

1. **Cost = I/O + CPU**: The cost model combines disk I/O (the slowest) and CPU processing
2. **Sequential is 10× cheaper than random I/O**: Access patterns matter enormously
3. **Memory is critical**: Hash joins and sorts perform vastly better when data fits in memory
4. **Join algorithm choice is crucial**: Hash join can be 5900× faster than nested loop!
5. **Index vs. scan depends on selectivity**: Indexes win for < 10%, sequential scan wins for > 50%
6. **Clustering factor matters**: Good index clustering can be 5× faster
7. **LIMIT can short-circuit**: But only with pipelined operators, not blocking ones
8. **Cost is relative**: Absolute values don't matter, only the ratio between plans

## Related Components

- [Selectivity Estimation](../selectivity/README.md): Provides predicate selectivity for cost calculation
- [Cardinality Estimation](../cardinality/README.md): Estimates row counts for each operator
- [Catalog Statistics](../../catalog/): Stores table/column/index statistics used for costing
