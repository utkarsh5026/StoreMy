# Cardinality Estimation

## Table of Contents
- [What is Cardinality?](#what-is-cardinality)
- [Why Does Cardinality Matter?](#why-does-cardinality-matter)
- [Core Concepts](#core-concepts)
- [Estimation Strategies by Operator](#estimation-strategies-by-operator)
- [Advanced Topics](#advanced-topics)
- [How It Works](#how-it-works)
- [Examples](#examples)
- [API Reference](#api-reference)

## What is Cardinality?

**Cardinality** is the number of rows (tuples) that an operation will produce. While selectivity tells us what *fraction* of rows match a condition, cardinality tells us the *actual number* of rows.

```
Cardinality = Input Rows × Selectivity
```

For example:
- **Table with 10,000 rows**, predicate with **selectivity 0.1** → **Cardinality = 1,000 rows**
- **Join of two tables** (100 rows × 200 rows), **join selectivity 0.01** → **Cardinality = 200 rows**

Cardinality is always a **whole number** (you can't have 3.5 rows!), representing the estimated result set size.

## Why Does Cardinality Matter?

Cardinality estimates are the **foundation of cost-based query optimization**. They directly impact:

### 1. Join Order Selection

Consider joining three tables:

```sql
SELECT * FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN products p ON o.product_id = p.id
```

**Scenario A: Bad join order**
```
orders (1M rows) ⋈ products (100K rows) = 10M rows (huge intermediate result!)
  └─> Then ⋈ customers (10K rows) = 500K rows final
```

**Scenario B: Good join order**
```
customers (10K rows) ⋈ orders (1M rows) = 100K rows
  └─> Then ⋈ products (100K rows) = 50K rows final
```

The optimizer uses cardinality estimates to choose Scenario B, avoiding the massive intermediate result.

### 2. Memory Allocation

- **Hash joins** need memory proportional to the smaller input's cardinality
- **Sort operations** need buffer space for the input cardinality
- **Aggregations** need hash table space for distinct groups

**Underestimating** cardinality → memory overflow → disk spilling → 100x slower
**Overestimating** cardinality → wasted memory → fewer concurrent queries

### 3. Index vs. Table Scan Decision

```sql
SELECT * FROM users WHERE age > 30
```

- **Cardinality estimate: 100 rows** (out of 1M) → Use index (0.01% of table)
- **Cardinality estimate: 900K rows** (out of 1M) → Use table scan (90% of table)

Reading 900K rows via index requires 900K random I/Os (slow!). Table scan is faster.

### 4. Physical Operator Selection

- **Small result set** → Nested loop join (good for small inner table)
- **Large result set** → Hash join or sort-merge join
- **Very large result set** → Consider materialization strategies

## Core Concepts

### 1. The Cardinality Estimation Pipeline

```
Query Plan Node
      ↓
   Cardinality Estimator
      ↓
   ├─> Get input cardinalities (recursive)
   ├─> Get selectivity estimates (from SelectivityEstimator)
   ├─> Apply operator-specific formula
   └─> Return output cardinality
```

### 2. Relationship to Selectivity

The cardinality package **uses** the selectivity package:

```
Selectivity: "What fraction of rows match?"  → 0.0 to 1.0
Cardinality: "How many rows match?"          → Integer count

Cardinality = Base Cardinality × Selectivity
```

**Example:**
```go
// Table has 10,000 rows
// Predicate: status = 'active' has selectivity 0.3
cardinality = 10000 × 0.3 = 3000 rows
```

### 3. Operator Categories

Different operators affect cardinality in different ways:

| Category | Operators | Cardinality Effect |
|----------|-----------|-------------------|
| **Pass-through** | Project, Sort | No change |
| **Filtering** | Filter, Scan with predicates | Reduces by selectivity |
| **Limiting** | Limit | Caps at limit value |
| **Multiplying** | Join, Cross Product | Multiplies inputs |
| **Aggregating** | GroupBy, Distinct | Reduces to distinct groups |
| **Set operations** | Union, Intersect, Except | Combines inputs |

### 4. Default Values

When statistics aren't available, the estimator uses conservative defaults:

```go
const (
    DefaultTableCardinality = 1000  // Assume tables have 1000 rows
    DefaultDistinctCount    = 100   // Assume columns have 100 distinct values
    DefaultJoinSelectivity  = 0.1   // Assume joins filter to 10%
    MinCardinality          = 1     // Never estimate 0 rows
)
```

These prevent catastrophically wrong estimates that would produce terrible query plans.

## Estimation Strategies by Operator

### 1. Scan (Table Access)

**Formula:**
```
Cardinality = TableRows × ∏(selectivity_i)^correlationFactor
```

**Intuition:** Start with the full table size, then apply predicate selectivity.

**Example:**
```go
// Table: users (10,000 rows)
// Predicate: age > 30 (selectivity = 0.6)

estimatedCardinality = 10000 × 0.6 = 6,000 rows
```

**With multiple predicates:**
```go
// Predicates:
//   - age > 30 (selectivity = 0.6)
//   - city = 'NYC' (selectivity = 0.1)

// Naive (assumes independence):
naive = 10000 × 0.6 × 0.1 = 600 rows

// With correlation correction (more realistic):
corrected = 10000 × (0.6 × 0.1)^0.63 ≈ 1,200 rows
```

The correlation correction prevents over-optimistic estimates when predicates might be correlated.

### 2. Filter

**Formula:**
```
Cardinality = ChildCardinality × ∏(selectivity_i)^correlationFactor
```

**Intuition:** Same as scan, but operates on child's output instead of base table.

**Example:**
```go
// Child outputs 5,000 rows
// Filter: status = 'active' (selectivity = 0.3)

estimatedCardinality = 5000 × 0.3 = 1,500 rows
```

**Visual Flow:**
```
Table Scan (10,000 rows)
    ↓
Filter: age > 30 (selectivity 0.6)
    ↓
6,000 rows
    ↓
Filter: status = 'active' (selectivity 0.3)
    ↓
1,800 rows
```

### 3. Join

**Formula:**
```
BaseCardinality = LeftRows × RightRows (Cartesian product)
JoinSelectivity = 1 / max(NDV_left, NDV_right)  (for equi-joins)
FilterSelectivity = ∏(filter_selectivity_i)^correlationFactor

OutputCardinality = BaseCardinality × JoinSelectivity × FilterSelectivity
```

**Intuition:** Start with worst case (cross product), reduce by join condition, reduce by filters.

#### Equi-Join Selectivity

The most sophisticated estimation is for equi-joins (`A.x = B.y`):

**Case 1: Similar Distinct Counts (Partial Overlap)**
```
Table A: 1,000 rows, column X has 500 distinct values
Table B: 1,000 rows, column Y has 700 distinct values

JoinSelectivity = 1 / 700 = 0.00143
Cardinality = 1000 × 1000 × 0.00143 ≈ 1,430 rows
```

**Reasoning:** Use the larger distinct count (more conservative). Assumes partial overlap.

**Case 2: Containment (One Side Much Larger)**
```
Orders: 100,000 rows, customer_id has 10,000 distinct values
Customers: 10,000 rows, id has 10,000 distinct values

Ratio: 10000 / 10000 = 1.0 (not containment)

But consider:
Orders: 100,000 rows, customer_id has 5,000 distinct values
Customers: 10,000 rows, id has 10,000 distinct values

Ratio: 10000 / 5000 = 2.0 → Triggers containment detection!
JoinSelectivity = 1 / 5,000 = 0.0002
Cardinality = 100000 × 10000 × 0.0002 = 200,000 rows
```

**Reasoning:** When `maxDistinct > 2 × minDistinct`, the smaller set is likely contained in the larger. Every value from the smaller side will match, so use its distinct count.

**Case 3: Foreign Key Join (Perfect Containment)**
```
Orders: 100,000 rows (each order has exactly one customer_id)
Customers: 10,000 rows (customer_id is primary key)

customer_id in Orders: all 10,000 distinct values present
customer_id in Customers: 10,000 distinct values

Result: ~100,000 rows (each order matches exactly one customer)
```

This is what happens in real databases with foreign keys!

#### Cross Join (No Join Condition)

**Formula:**
```
Cardinality = LeftRows × RightRows (full Cartesian product)
```

**Example:**
```sql
SELECT * FROM colors, sizes  -- No join condition!

Colors: 5 rows
Sizes: 3 rows
Result: 5 × 3 = 15 rows
```

### 4. Aggregation (GROUP BY)

**Formula:**
```
No GROUP BY:  Cardinality = 1
Single column GROUP BY: Cardinality = min(DistinctCount, InputRows)
Multiple columns GROUP BY: Cardinality = min(∏DistinctCount_i, InputRows)
```

**Intuition:** GROUP BY collapses rows into groups. Output cardinality = number of distinct groups.

**Examples:**

**No GROUP BY (global aggregation):**
```sql
SELECT COUNT(*), AVG(salary) FROM employees

Input: 10,000 rows
Output: 1 row (single aggregate result)
```

**Single column GROUP BY:**
```sql
SELECT department, COUNT(*) FROM employees GROUP BY department

Input: 10,000 rows
department has 50 distinct values
Output: 50 rows (one per department)
```

**Multiple column GROUP BY:**
```sql
SELECT department, location, COUNT(*)
FROM employees
GROUP BY department, location

department: 50 distinct values
location: 20 distinct values

Naive estimate: 50 × 20 = 1,000 distinct groups
Actual input: 10,000 rows

Output: min(1000, 10000) = 1,000 rows
```

**Important:** The multiplica tion assumes independence (departments and locations are uncorrelated). Real systems use multi-column statistics for better accuracy.

### 5. Distinct

**Formula:**
```
With statistics: Cardinality = min(DistinctCount, InputRows)
Without statistics: Cardinality = InputRows × 0.8  (assumes 20% duplicates)
```

**Intuition:** DISTINCT removes duplicates, reducing cardinality to the number of unique values.

**Examples:**

**With statistics:**
```sql
SELECT DISTINCT customer_id FROM orders

orders: 100,000 rows
customer_id: 10,000 distinct values

Output: 10,000 rows
```

**Without statistics:**
```sql
SELECT DISTINCT email FROM users

users: 10,000 rows
email: unknown distinct count

Output estimate: 10000 × 0.8 = 8,000 rows
```

The 80% factor is based on empirical studies showing typical tables have 70-90% unique rows.

### 6. Limit and Offset

**Formula:**
```
EffectiveRows = InputRows - Offset
Cardinality = min(Limit, max(0, EffectiveRows))
```

**Intuition:** OFFSET skips rows, LIMIT caps the result.

**Examples:**

**Simple LIMIT:**
```sql
SELECT * FROM users LIMIT 10

Input: 10,000 rows
Output: 10 rows
```

**LIMIT larger than input:**
```sql
SELECT * FROM users LIMIT 10000

Input: 100 rows
Output: 100 rows (can't produce more than input)
```

**LIMIT with OFFSET:**
```sql
SELECT * FROM users LIMIT 10 OFFSET 20

Input: 10,000 rows
After OFFSET: 10,000 - 20 = 9,980 rows
After LIMIT: min(10, 9980) = 10 rows
```

**OFFSET exceeds input:**
```sql
SELECT * FROM users LIMIT 10 OFFSET 20000

Input: 10,000 rows
After OFFSET: 10,000 - 20,000 = -10,000 → 0 rows
Output: 0 rows
```

### 7. Union

**UNION ALL (preserves duplicates):**
```
Cardinality = LeftRows + RightRows
```

**UNION (removes duplicates):**
```
Cardinality = LeftRows + RightRows - EstimatedOverlap

OverlapRatio depends on size ratio:
- SizeRatio < 0.1:  OverlapRatio = 0.50 (high containment)
- SizeRatio < 0.5:  OverlapRatio = 0.30 (moderate overlap)
- Otherwise:        OverlapRatio = 0.15 (low overlap)

where SizeRatio = min(Left, Right) / max(Left, Right)
```

**Examples:**

**UNION ALL:**
```sql
SELECT * FROM users_2023 UNION ALL SELECT * FROM users_2024

users_2023: 10,000 rows
users_2024: 12,000 rows
Output: 22,000 rows
```

**UNION with similar sizes:**
```sql
SELECT * FROM east_users UNION SELECT * FROM west_users

east_users: 10,000 rows
west_users: 9,000 rows
SizeRatio: 9000/10000 = 0.9 → OverlapRatio = 0.15

EstimatedOverlap = 9000 × 0.15 = 1,350 rows
Output: 10000 + 9000 - 1350 = 17,650 rows
```

**UNION with containment:**
```sql
SELECT * FROM all_users UNION SELECT * FROM premium_users

all_users: 100,000 rows
premium_users: 5,000 rows
SizeRatio: 5000/100000 = 0.05 → OverlapRatio = 0.50

EstimatedOverlap = 5000 × 0.50 = 2,500 rows
Output: 100000 + 5000 - 2500 = 102,500 rows
```

The containment logic recognizes that premium_users is likely a subset of all_users.

### 8. Intersect

**Formula:**
```
Cardinality = min(LeftRows, RightRows) × IntersectRatio

IntersectRatio (for INTERSECT ALL):
- SizeRatio < 0.1:  0.70 (high containment)
- SizeRatio < 0.5:  0.50 (moderate overlap)
- Otherwise:        0.30 (similar sizes)

IntersectRatio (for INTERSECT - with deduplication):
- SizeRatio < 0.1:  0.60
- SizeRatio < 0.5:  0.40
- Otherwise:        0.25
```

**Intuition:** INTERSECT returns only common rows. Result can't exceed the smaller input.

**Examples:**

**High containment:**
```sql
SELECT * FROM all_users INTERSECT SELECT * FROM premium_users

all_users: 100,000 rows
premium_users: 5,000 rows
SizeRatio: 5000/100000 = 0.05 → IntersectRatio = 0.60

Output: 5000 × 0.60 = 3,000 rows
```

**Similar sizes:**
```sql
SELECT * FROM users_2023 INTERSECT SELECT * FROM users_2024

users_2023: 10,000 rows
users_2024: 12,000 rows
SizeRatio: 10000/12000 = 0.83 → IntersectRatio = 0.25

Output: 10000 × 0.25 = 2,500 rows
```

### 9. Except (Minus)

**Formula:**
```
Cardinality = LeftRows - (LeftRows × RemovalRatio)

RemovalRatio:
- SizeRatio < 0.1:  0.20 (right is small, removes little)
- SizeRatio < 0.5:  0.40 (moderate removal)
- Otherwise:        0.60 (significant removal)

For EXCEPT (with deduplication):
DistinctLeft = LeftRows × 0.8
Cardinality = DistinctLeft - (DistinctLeft × RemovalRatio)
```

**Intuition:** EXCEPT returns left rows NOT in right. Larger right set → more removal.

**Examples:**

**Small right (removes little):**
```sql
SELECT * FROM all_users EXCEPT SELECT * FROM banned_users

all_users: 100,000 rows
banned_users: 500 rows
SizeRatio: 500/100000 = 0.005 → RemovalRatio = 0.20

Output: 100000 - (100000 × 0.20) = 80,000 rows
```

**Large right (removes much):**
```sql
SELECT * FROM all_orders EXCEPT SELECT * FROM fulfilled_orders

all_orders: 100,000 rows
fulfilled_orders: 90,000 rows
SizeRatio: 90000/100000 = 0.9 → RemovalRatio = 0.60

DistinctLeft: 100000 × 0.8 = 80,000
Output: 80000 - (80000 × 0.60) = 32,000 rows
```

### 10. Pass-Through Operators

Some operators don't change cardinality:

**Project (SELECT columns):**
```sql
SELECT name, email FROM users

Input: 10,000 rows
Output: 10,000 rows (just different columns)
```

**Sort (ORDER BY):**
```sql
SELECT * FROM users ORDER BY name

Input: 10,000 rows
Output: 10,000 rows (same rows, different order)
```

## Advanced Topics

### 1. Correlation Correction

When combining multiple predicates with AND, the **independence assumption** often fails:

**Independence Assumption (naive):**
```
P(A AND B) = P(A) × P(B)
```

**Problem:** Predicates often correlate!

**Example:**
```sql
WHERE age > 50 AND retired = true
```

These predicates are **highly correlated** (older people more likely to be retired). Naive multiplication:
```
age > 50:     selectivity = 0.2 (20%)
retired = true: selectivity = 0.1 (10%)
Combined (naive): 0.2 × 0.1 = 0.02 (2%)
```

But in reality, if you're over 50, you're much more likely to be retired! Actual selectivity might be 0.08 (8%).

**Correlation Correction Formula:**
```
corrected = product^(1 / (1 + log(n)))

where:
  product = naive product of selectivities
  n = number of predicates
```

**Why this formula?**
- **More predicates → stronger correction**: The exponent `1/(1+log(n))` decreases as `n` increases
- **Never more optimistic than naive**: The correction only increases the estimate
- **Empirically validated**: This formula approximates real-world correlation

**Example with correction:**
```go
// Three predicates with selectivity 0.1 each
naive = 0.1 × 0.1 × 0.1 = 0.001 (0.1%)
n = 3
exponent = 1 / (1 + log(3)) = 1 / (1 + 1.099) = 0.476
corrected = 0.001^0.476 = 0.0316 (3.16%)

// The correction increased the estimate by 30x!
```

**When to apply:**
- Multiple predicates in WHERE clause
- Multiple join conditions
- Filter predicates after joins

### 2. Containment Detection in Joins

**The Problem:**

Standard join formula assumes **independent, uniformly distributed values**:
```
JoinSelectivity = 1 / max(NDV_left, NDV_right)
```

But what if one side is a **subset** of the other?

**Example: Foreign Key Join**
```
Orders.customer_id → Customers.id

Orders: 100,000 rows, customer_id has 5,000 distinct values
Customers: 10,000 rows, id has 10,000 distinct values
```

Every `customer_id` in Orders **must exist** in Customers (foreign key constraint). The 5,000 distinct customer IDs in Orders are **contained** in the 10,000 IDs in Customers.

**Without containment detection:**
```
JoinSelectivity = 1 / 10,000 = 0.0001
Cardinality = 100000 × 10000 × 0.0001 = 100,000 rows
```

**With containment detection:**
```
Ratio: 10000 / 5000 = 2.0 > threshold (2.0)
→ Containment detected!
JoinSelectivity = 1 / 5,000 = 0.0002
Cardinality = 100000 × 10000 × 0.0002 = 200,000 rows
```

Actually, with perfect containment, the result should be ~100,000 rows (each order matches one customer). The estimator is conservative but closer than the naive approach.

**Containment Threshold:**

```go
if maxDistinct > minDistinct * 2.0 {
    // Likely containment: use smaller distinct count
    selectivity = 1.0 / minDistinct
} else {
    // Partial overlap: use larger distinct count (conservative)
    selectivity = 1.0 / maxDistinct
}
```

The **2.0 threshold** is empirically derived:
- Too low (e.g., 1.1) → Too aggressive, false positives
- Too high (e.g., 10.0) → Misses real containment
- 2.0 provides good balance

### 3. Bounding and Sanity Checks

All estimates include **sanity checks** to prevent nonsensical results:

**Join Cardinality Bounds:**
```go
// Can't exceed Cartesian product
result = min(result, leftRows * rightRows)

// Can't be zero (prevents division by zero downstream)
result = max(1, result)
```

**Aggregate Cardinality Bounds:**
```go
// Can't exceed input cardinality
result = min(distinctGroups, inputRows)

// Always at least 1 row
result = max(1, result)
```

**DISTINCT Cardinality Bounds:**
```go
// Can't have more distinct values than rows
result = min(distinctCount, inputRows)
```

**Why bother?**

Even with perfect statistics, rounding errors and edge cases can produce invalid estimates. Bounds ensure the optimizer never sees impossible values.

## How It Works

### End-to-End Example: Complex Query

Let's trace cardinality estimation through a realistic query:

```sql
SELECT u.name, COUNT(*) as order_count
FROM users u
JOIN orders o ON u.id = o.user_id
WHERE u.age > 30
  AND o.status = 'completed'
GROUP BY u.name
ORDER BY order_count DESC
LIMIT 10
```

**Query Plan:**
```
Limit (10)
  └─> Sort (order_count DESC)
       └─> Aggregate (GROUP BY name)
            └─> Filter (o.status = 'completed')
                 └─> Join (u.id = o.user_id)
                      ├─> Filter (u.age > 30)
                      │    └─> Scan (users)
                      └─> Scan (orders)
```

**Step-by-Step Cardinality Estimation:**

**1. Scan users:**
```
users table: 100,000 rows
Cardinality: 100,000 rows
```

**2. Filter (u.age > 30):**
```
Selectivity: 0.6 (60% of users are over 30)
Cardinality: 100,000 × 0.6 = 60,000 rows
```

**3. Scan orders:**
```
orders table: 500,000 rows
Cardinality: 500,000 rows
```

**4. Join (u.id = o.user_id):**
```
Left (filtered users): 60,000 rows
Right (orders): 500,000 rows
Base: 60,000 × 500,000 = 30,000,000,000 rows (cross product)

users.id: 100,000 distinct values (primary key)
orders.user_id: 60,000 distinct values (foreign key)

Containment: 100000 / 60000 = 1.67 < 2.0 (no containment)
JoinSelectivity: 1 / 100,000 = 0.00001

Cardinality: 30,000,000,000 × 0.00001 = 300,000 rows
```

**5. Filter (o.status = 'completed'):**
```
Selectivity: 0.7 (70% of orders are completed)
Cardinality: 300,000 × 0.7 = 210,000 rows
```

**6. Aggregate (GROUP BY name):**
```
name distinct count: ~95,000 (most names are unique)
Input: 210,000 rows

Cardinality: min(95000, 210000) = 95,000 rows
```

**7. Sort (order_count DESC):**
```
Pass-through operation
Cardinality: 95,000 rows
```

**8. Limit (10):**
```
Cardinality: min(10, 95000) = 10 rows
```

**Final Result: 10 rows**

### How This Affects the Query Plan

With these cardinality estimates, the optimizer can make informed decisions:

1. **Join Strategy**: Hash join (right table 500K rows → build hash table)
2. **Join Order**: Filter users first (reduces left input to 60K instead of 100K)
3. **Memory Allocation**:
   - Hash table: ~60K entries for filtered users
   - Sort buffer: ~95K rows for aggregation result
4. **Index Decision**: If there's an index on `age`, scan 60% of table → table scan better

**What if estimates were wrong?**

**Underestimate by 100x:**
```
Estimated 950 rows for aggregate, actually 95,000 rows
→ Hash table overflow
→ Disk spilling
→ Query 100x slower
```

**Overestimate by 100x:**
```
Estimated 9.5M rows for aggregate, actually 95,000 rows
→ Allocated 100x more memory than needed
→ Wasted resources
→ Fewer concurrent queries possible
```

Accurate estimates are critical!

## Examples

### Example 1: Simple Scan with Statistics

```go
// Table: products (10,000 rows)
// Predicate: category = 'Electronics'
// Statistics: category has 20 distinct values, uniform distribution

estimator := NewCardinalityEstimator(catalog, tx)

scan := &plan.ScanNode{
    TableID: productsTableID,
    Predicates: []plan.PredicateInfo{
        {
            Column:    "category",
            Predicate: primitives.Equals,
            Value:     "Electronics",
            Type:      plan.StandardPredicate,
        },
    },
}

cardinality, _ := estimator.EstimatePlanCardinality(scan)
// Result: ~500 rows (10,000 / 20 distinct values)
```

### Example 2: Join with Containment

```go
// Orders: 100,000 rows, user_id has 10,000 distinct values
// Customers: 10,000 rows, id has 10,000 distinct values (primary key)

leftScan := &plan.ScanNode{TableID: ordersTableID}
leftScan.SetCardinality(100000)

rightScan := &plan.ScanNode{TableID: customersTableID}
rightScan.SetCardinality(10000)

join := &plan.JoinNode{
    LeftChild:   leftScan,
    RightChild:  rightScan,
    LeftColumn:  "user_id",
    RightColumn: "id",
}

cardinality, _ := estimator.EstimateJoin(join)
// Result: ~100,000 rows (foreign key join, most orders match)
```

### Example 3: Complex Filter with Correlation Correction

```go
// Table: employees (50,000 rows)
// Predicates:
//   - age > 55 (selectivity 0.15)
//   - years_employed > 20 (selectivity 0.20)
//   - salary > 100000 (selectivity 0.25)

scan := &plan.ScanNode{
    TableID: employeesTableID,
    Predicates: []plan.PredicateInfo{
        {Column: "age", Predicate: primitives.GreaterThan, Value: "55"},
        {Column: "years_employed", Predicate: primitives.GreaterThan, Value: "20"},
        {Column: "salary", Predicate: primitives.GreaterThan, Value: "100000"},
    },
}

// Naive estimate: 50000 × 0.15 × 0.20 × 0.25 = 375 rows
// With correlation correction: ~1,200 rows (more realistic)

cardinality, _ := estimator.EstimatePlanCardinality(scan)
// Result: ~1,200 rows
```

**Why the correction?**

These predicates are correlated:
- Older employees (age > 55) likely have more years employed
- Employees with more years likely have higher salaries
- Naive multiplication assumes independence, drastically underestimates

### Example 4: Aggregation with Multiple GROUP BY Columns

```go
// Table: sales (1,000,000 rows)
// GROUP BY: region, product_category, month

agg := &plan.AggregateNode{
    Child: scanNode, // 1,000,000 rows
    GroupByExprs: []string{"region", "product_category", "month"},
}

// Statistics:
//   region: 4 distinct values (North, South, East, West)
//   product_category: 50 distinct values
//   month: 12 distinct values

// Estimated distinct groups: 4 × 50 × 12 = 2,400
// Bounded by input: min(2400, 1000000) = 2,400 rows

cardinality, _ := estimator.EstimateAggr(agg)
// Result: 2,400 rows
```

### Example 5: Set Operations

**UNION:**
```go
// active_users: 50,000 rows
// premium_users: 5,000 rows

union := &plan.UnionNode{
    LeftChild:  activeUsersNode,  // 50,000 rows
    RightChild: premiumUsersNode, // 5,000 rows
    UnionAll:   false,  // UNION (with deduplication)
}

// SizeRatio: 5000/50000 = 0.1 → OverlapRatio = 0.50
// EstimatedOverlap: 5000 × 0.50 = 2,500
// Result: 50000 + 5000 - 2500 = 52,500 rows

cardinality, _ := estimator.EstimateUnionCardinality(union)
// Result: 52,500 rows
```

**INTERSECT:**
```go
// All users: 50,000 rows
// Active users: 20,000 rows

intersect := &plan.IntersectNode{
    LeftChild:    allUsersNode,    // 50,000 rows
    RightChild:   activeUsersNode, // 20,000 rows
    IntersectAll: false,
}

// SizeRatio: 20000/50000 = 0.4 → IntersectRatio = 0.40
// Result: 20000 × 0.40 = 8,000 rows

cardinality, _ := estimator.EstimateIntersectCardinality(intersect)
// Result: 8,000 rows
```

### Example 6: Limit with Offset (Pagination)

```go
// Paginating through results: page 3, 50 items per page

limit := &plan.LimitNode{
    Child:  sortedResults, // 10,000 rows
    Limit:  50,
    Offset: 100, // Skip first 100 rows (pages 1-2)
}

// EffectiveRows: 10000 - 100 = 9,900
// Result: min(50, 9900) = 50 rows

cardinality, _ := estimator.EstimateLimit(limit)
// Result: 50 rows
```

## API Reference

### CardinalityEstimator

Main struct for estimating cardinality:

```go
type CardinalityEstimator struct {
    catalog *catalog.SystemCatalog
    tx      *transaction.TransactionContext
}
```

### Constructor

```go
func NewCardinalityEstimator(
    cat *catalog.SystemCatalog,
    tx *transaction.TransactionContext,
) (*CardinalityEstimator, error)
```

Returns error if catalog is nil.

### Core Methods

#### EstimatePlanCardinality

Main entry point for cardinality estimation. Recursively estimates cardinality for any plan node.

```go
func (ce *CardinalityEstimator) EstimatePlanCardinality(
    planNode plan.PlanNode,
) (int64, error)
```

**Supported node types:**
- ScanNode
- FilterNode
- ProjectNode
- JoinNode
- AggregateNode
- SortNode
- LimitNode
- DistinctNode
- UnionNode, IntersectNode, ExceptNode

**Returns:** Estimated cardinality (integer ≥ 0), or error if estimation fails

**Caching:** If node already has cardinality set, returns cached value

### Operator-Specific Methods

These are called internally by `EstimatePlanCardinality`:

#### estimateScan

```go
func (ce *CardinalityEstimator) estimateScan(node *plan.ScanNode) (int64, error)
```

Estimates cardinality for table scan with predicates.

**Formula:** `TableRows × ∏(selectivity_i)^correlationFactor`

#### estimateFilter

```go
func (ce *CardinalityEstimator) estimateFilter(node *plan.FilterNode) (int64, error)
```

Estimates cardinality for filter operation.

**Formula:** `ChildRows × ∏(selectivity_i)^correlationFactor`

#### estimateJoin

```go
func (ce *CardinalityEstimator) estimateJoin(node *plan.JoinNode) (int64, error)
```

Estimates cardinality for join operation with containment detection.

**Formula:** `LeftRows × RightRows × JoinSelectivity × FilterSelectivity`

#### estimateAggr

```go
func (ce *CardinalityEstimator) estimateAggr(node *plan.AggregateNode) (int64, error)
```

Estimates cardinality for aggregation/GROUP BY.

**Formula:**
- No GROUP BY: Returns 1
- With GROUP BY: Returns `min(∏DistinctCount_i, InputRows)`

#### estimateLimit

```go
func (ce *CardinalityEstimator) estimateLimit(node *plan.LimitNode) (int64, error)
```

Estimates cardinality for LIMIT/OFFSET.

**Formula:** `min(Limit, max(0, InputRows - Offset))`

#### estimateDistinct

```go
func (ce *CardinalityEstimator) estimateDistinct(node *plan.DistinctNode) (int64, error)
```

Estimates cardinality for DISTINCT operation.

**Formula:**
- With stats: `min(DistinctCount, InputRows)`
- Without stats: `InputRows × 0.8`

#### estimateUnionCardinality

```go
func (ce *CardinalityEstimator) estimateUnionCardinality(node *plan.UnionNode) (int64, error)
```

Estimates cardinality for UNION/UNION ALL.

**Formula:**
- UNION ALL: `LeftRows + RightRows`
- UNION: `LeftRows + RightRows - EstimatedOverlap`

#### estimateIntersectCardinality

```go
func (ce *CardinalityEstimator) estimateIntersectCardinality(node *plan.IntersectNode) (int64, error)
```

Estimates cardinality for INTERSECT.

**Formula:** `min(LeftRows, RightRows) × IntersectRatio`

#### estimateExceptCardinality

```go
func (ce *CardinalityEstimator) estimateExceptCardinality(node *plan.ExceptNode) (int64, error)
```

Estimates cardinality for EXCEPT (MINUS).

**Formula:** `LeftRows - (LeftRows × RemovalRatio)`

### Helper Functions

#### applyCorrelationCorrection

```go
func applyCorrelationCorrection(selectivities []float64) float64
```

Applies correlation correction to multiple predicate selectivities.

**Formula:** `product^(1 / (1 + log(n)))`

**Use:** Prevents over-optimistic estimates from correlated predicates

#### findBaseTableID

```go
func findBaseTableID(planNode plan.PlanNode) (tableID int, found bool)
```

Walks plan tree to find base table for statistics lookup.

**Use:** Needed when filter/predicate is above the scan in the plan tree

### Constants

```go
const (
    DefaultTableCardinality = 1000  // Default table size
    DefaultDistinctCount    = 100   // Default distinct values
    DefaultJoinSelectivity  = 0.1   // Default join selectivity (10%)
    MinCardinality          = 1     // Minimum cardinality (never 0)
)
```

## Key Takeaways

1. **Cardinality is the foundation** of cost-based optimization—wrong estimates lead to terrible query plans
2. **Uses selectivity estimates** from the selectivity package to calculate output row counts
3. **Different operators have different formulas**: Joins multiply, filters reduce, aggregates collapse
4. **Correlation correction** prevents over-optimistic estimates from correlated predicates
5. **Containment detection** improves join estimates for foreign key relationships
6. **Sanity checks and bounds** ensure estimates are always reasonable
7. **Conservative defaults** prevent catastrophic failures when statistics are missing
8. **Recursive estimation** builds up cardinality from leaves to root of plan tree

## Related Components

- [Selectivity Estimation](../selectivity/README.md): Provides predicate selectivity estimates used by cardinality estimation
- [Cost Model](../cost_model/README.md): Uses cardinality estimates to calculate query execution costs
- [Catalog Statistics](../../catalog/): Stores table/column statistics used for accurate cardinality estimation
