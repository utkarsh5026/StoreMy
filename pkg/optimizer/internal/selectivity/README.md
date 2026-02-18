# Selectivity Estimation

## Table of Contents
- [What is Selectivity?](#what-is-selectivity)
- [Why Does Selectivity Matter?](#why-does-selectivity-matter)
- [Core Concepts](#core-concepts)
- [Estimation Strategies](#estimation-strategies)
- [How It Works](#how-it-works)
- [Examples](#examples)
- [API Reference](#api-reference)

## What is Selectivity?

**Selectivity** is a number between 0.0 and 1.0 that represents the fraction of rows in a table that satisfy a given condition (predicate).

- **Selectivity = 0.0**: No rows match the condition
- **Selectivity = 0.5**: Half the rows match the condition
- **Selectivity = 1.0**: All rows match the condition

For example, if you have a table with 1,000 users and 100 of them are from "USA", then the selectivity of the predicate `country = 'USA'` is **0.1** (or 10%).

## Why Does Selectivity Matter?

Selectivity is **critical for query optimization**. The query optimizer uses selectivity estimates to:

1. **Choose the best join order**: Join smaller result sets first to minimize intermediate data
2. **Select optimal indexes**: Use indexes when predicates are highly selective (low selectivity)
3. **Estimate query costs**: Calculate I/O, CPU, and memory requirements
4. **Allocate resources**: Reserve appropriate buffer space and memory

### The Cost Impact

Consider joining two tables with different predicates:

```sql
-- Option A: Filter users first, then join
SELECT * FROM users u
JOIN orders o ON u.id = o.user_id
WHERE u.country = 'Luxembourg'  -- Very selective! (0.01)

-- Option B: Filter orders first, then join
SELECT * FROM users u
JOIN orders o ON u.id = o.user_id
WHERE o.status = 'pending'  -- Less selective (0.3)
```

If the optimizer knows that `country = 'Luxembourg'` is highly selective (only 1% of users), it can:
- Filter the users table down to 100 rows first
- Then join with orders (much cheaper join!)
- Instead of joining all 10,000 users with orders

**Without good selectivity estimates**, the optimizer might choose a terrible plan that's 100x slower.

## Core Concepts

### 1. Predicate Types

The selectivity package handles several types of predicates:

| Predicate Type | Example | Typical Selectivity |
|---------------|---------|-------------------|
| Equality | `age = 25` | Low (1/distinct_count) |
| Inequality | `age != 25` | High (1 - equality) |
| Range | `age > 18`, `price < 100` | Medium (~33%) |
| NULL checks | `email IS NULL` | Varies (from null_count) |
| Pattern matching | `name LIKE 'John%'` | Varies by pattern |
| Set membership | `status IN ('active', 'pending')` | Low to medium |
| Combined | `age > 18 AND country = 'USA'` | Product of individual selectivities |

### 2. Statistical Information

The estimator uses three types of statistics:

#### a) Distinct Count
The number of unique values in a column.

**Intuition**: If a column has 100 distinct values and we're looking for one specific value (equality), assuming uniform distribution, we'd expect to find about 1/100 = 1% of the rows.

```
Column: [1, 2, 2, 3, 3, 3, 4, 4, 4, 4]
Distinct Count: 4
Selectivity(col = 3): 1/4 = 0.25 (assuming uniform distribution)
Actual Selectivity: 3/10 = 0.3 (data is skewed!)
```

#### b) Most Common Values (MCV)
Tracks the most frequently occurring values and their exact frequencies.

**Intuition**: Real-world data is rarely uniform. Some values appear much more frequently than others. MCVs capture these "hot spots" for accurate estimation.

```
Column: country
MCV List:
  - "USA": 0.5 (50% of rows)
  - "UK": 0.2 (20% of rows)
  - "Canada": 0.15 (15% of rows)
Other values: 0.15 (15% of rows, distributed among 47 other countries)

Selectivity(country = 'USA'): 0.5 (exact!)
Selectivity(country = 'Luxembourg'): ~0.15/47 ≈ 0.003
```

#### c) Histograms
Divide the value range into buckets and track how many rows fall into each bucket.

**Intuition**: For range queries (`age > 30`), we need to know how values are distributed across the range. Histograms provide this by dividing the range into buckets.

```
Column: age (range 18-80)
Histogram with 4 buckets:

Bucket 1 [18-30]: 4000 rows (40%)
Bucket 2 [31-45]: 3000 rows (30%)
Bucket 3 [46-60]: 2000 rows (20%)
Bucket 4 [61-80]: 1000 rows (10%)

Selectivity(age > 45):
  = Rows in buckets 3 and 4 / Total rows
  = (2000 + 1000) / 10000
  = 0.3 (30%)
```

### 3. Fallback Strategy

When statistics aren't available, the estimator uses **empirically-derived default constants**:

| Predicate | Default | Reasoning |
|-----------|---------|-----------|
| Equality (`=`) | 0.01 (1%) | Equality is usually selective |
| Range (`>`, `<`) | 0.33 (33%) | Range typically matches ~1/3 of data |
| LIKE | 0.2 (20%) | Pattern matching is moderately selective |
| IN clause | 0.05 (5%) | Small sets are selective |
| IS NULL | 0.05 (5%) | Most columns have few nulls |

These defaults are **better than nothing** and prevent catastrophically bad query plans.

## Estimation Strategies

The estimator uses a **hierarchy of strategies**, preferring more accurate methods when available:

### Strategy Priority (Best to Worst)

1. **MCV Lookup** (Most Accurate)
   - Used for: Equality predicates when value is common
   - Accuracy: Exact frequency
   - Example: `country = 'USA'` → MCV lookup → 0.5

2. **Histogram Estimation** (Very Accurate)
   - Used for: Range predicates with histogram available
   - Accuracy: Good for ranges, approximated for point queries
   - Example: `age > 30` → Sum bucket frequencies → 0.6

3. **Distinct Count** (Moderate Accuracy)
   - Used for: When distinct count is known but no histogram/MCV
   - Accuracy: Assumes uniform distribution (often wrong!)
   - Example: `status = 'active'` → 1/distinct_count → 1/4 = 0.25

4. **Default Constants** (Low Accuracy)
   - Used for: No statistics available
   - Accuracy: Rough approximation
   - Example: `unknown_col = 'value'` → Default → 0.01

### Decision Flow

```
EstimateWithValue(predicate, table, column, value)
    |
    ├─> Can we get column statistics?
    |   └─> NO → Return default constant based on predicate type
    |
    ├─> Is this an EQUALITY or NOT EQUAL predicate?
    |   ├─> YES → Is value in MCV list?
    |   |   ├─> YES → Return MCV frequency (or 1 - frequency for NOT EQUAL)
    |   |   └─> NO → Continue to next strategy
    |   └─> NO → Continue to next strategy
    |
    ├─> Is this a comparison predicate (>, <, >=, <=) AND we have a histogram?
    |   ├─> YES → Use histogram to estimate selectivity
    |   └─> NO → Continue to next strategy
    |
    ├─> Is this an EQUALITY predicate AND we have distinct count?
    |   ├─> YES → Use equalityNoHist (considers MCVs + uniform for others)
    |   └─> NO → Continue to next strategy
    |
    └─> Return default constant
```

## How It Works

### 1. Basic Predicate Estimation

#### Equality with MCV

```go
// Estimate: WHERE country = 'USA'
estimator := NewSelectivityEstimator(catalog, tx)
sel := estimator.EstimateWithValue(
    primitives.Equals,
    tableID,
    "country",
    types.NewStringField("USA")
)
// Result: 0.5 (from MCV list)
```

**What happens internally:**
1. Fetch column statistics for "country"
2. Check if "USA" is in the MCV list
3. Found! Return its frequency: 0.5
4. This means 50% of rows have country = 'USA'

#### Range with Histogram

```go
// Estimate: WHERE age > 30
sel := estimator.EstimateWithValue(
    primitives.GreaterThan,
    tableID,
    "age",
    types.NewIntField(30)
)
// Result: 0.6 (from histogram)
```

**What happens internally:**
1. Fetch column statistics for "age"
2. Age has a histogram (not in MCV list)
3. Histogram calculates which buckets contain values > 30
4. Sum the frequencies of those buckets: 0.6
5. This means 60% of rows have age > 30

### 2. Combined Predicates

#### AND Combination

```go
// Estimate: WHERE age > 30 AND country = 'USA'
sel1 := 0.6  // age > 30
sel2 := 0.5  // country = 'USA'

combined := estimator.EstimateCombined(sel1, sel2, true) // true = AND
// Result: 0.6 × 0.5 = 0.3 (30%)
```

**Intuition**: For independent predicates, the probability that BOTH are true is the product of their individual probabilities.

If 60% of people are over 30, and 50% are from USA, then about 30% are both over 30 AND from USA (assuming independence).

#### OR Combination

```go
// Estimate: WHERE age > 60 OR country = 'USA'
sel1 := 0.1  // age > 60 (10%)
sel2 := 0.5  // country = 'USA' (50%)

combined := estimator.EstimateCombined(sel1, sel2, false) // false = OR
// Result: 0.1 + 0.5 - (0.1 × 0.5) = 0.55 (55%)
```

**Intuition**: We add the probabilities but subtract the overlap (people who are BOTH over 60 AND from USA) to avoid double-counting.

### 3. Special Cases

#### NULL Checks

```go
// Estimate: WHERE email IS NULL
sel := estimator.EstimateNull(tableID, "email", true)
// Result: 0.08 (8% of rows have null email)
```

**What happens:**
1. Get column statistics for "email"
2. Get table statistics to know total row count
3. Calculate: null_count / total_rows
4. Return exact null fraction

#### LIKE Patterns

```go
// Different LIKE patterns have different selectivities

sel1 := estimator.EstimateLike("John%")      // Prefix: 0.1 (10%)
sel2 := estimator.EstimateLike("%smith")     // Suffix: 0.3 (30%)
sel3 := estimator.EstimateLike("%foo%")      // Substring: 0.5 (50%)
sel4 := estimator.EstimateLike("exact")      // Exact: 0.01 (1%)
```

**Intuition**:
- **Prefix match** (`John%`): Database can use indexes efficiently, tends to be selective
- **Suffix match** (`%smith`): Can't use indexes well, less selective
- **Substring match** (`%foo%`): Requires full scan, least selective
- **Exact match**: No wildcards, very selective

#### IN Clauses

```go
// Estimate: WHERE status IN ('active', 'pending', 'processing')
sel := estimator.EstimateIn(tableID, "status", 3) // 3 values in the list
// If status has 10 distinct values: 3/10 = 0.3 (30%)
```

**Intuition**: If the column has 10 distinct values and we're looking for 3 of them, assuming uniform distribution, we'd expect to match 3/10 = 30% of rows.

### 4. Handling Non-MCV Values (Advanced)

When you query for a value that's NOT in the MCV list, the estimator uses a sophisticated approach:

```go
func (se *SelectivityEstimator) equalityNoHist(colStats *catalog.ColumnStatistics) float64 {
    // Calculate total frequency of all MCVs
    mcvTotalFreq := sum(colStats.MCVFreqs)  // e.g., 0.85 (85%)

    // Remaining probability mass for non-MCV values
    remainingFreq := 1.0 - mcvTotalFreq  // e.g., 0.15 (15%)

    // Number of distinct values NOT in MCV list
    nonMCVDistinct := distinctCount - len(MCVs)  // e.g., 100 - 3 = 97

    // Assume uniform distribution among non-MCV values
    return remainingFreq / nonMCVDistinct  // e.g., 0.15 / 97 ≈ 0.0015
}
```

**Example:**
```
Table: users (10,000 rows)
Column: country (100 distinct values)

MCV List:
  USA: 50% of rows
  UK: 20% of rows
  Canada: 15% of rows
  (Total MCV coverage: 85%)

Remaining data: 15% distributed among 97 other countries

Estimate for: WHERE country = 'Luxembourg'
  = 0.15 / 97
  ≈ 0.0015 (0.15%)
  ≈ 15 rows out of 10,000
```

**Why this matters:** This prevents over-estimating selectivity for rare values while still being more accurate than pure uniform distribution.

## Examples

### Example 1: Equality on High-Cardinality Column

```go
// Table: users (1,000,000 rows)
// Column: user_id (1,000,000 distinct values, PRIMARY KEY)

sel := estimator.EstimateWithValue(
    primitives.Equals,
    tableID,
    "user_id",
    types.NewIntField(12345)
)
// Result: 1/1,000,000 = 0.000001 (very selective!)
// Expected rows: 1
```

**Optimizer decision**: Use index on user_id (extremely selective)

### Example 2: Range Query on Age

```go
// Table: users (100,000 rows)
// Column: age, histogram shows:
//   [18-25]: 30%
//   [26-35]: 25%
//   [36-50]: 25%
//   [51-80]: 20%

sel := estimator.EstimateWithValue(
    primitives.GreaterThan,
    tableID,
    "age",
    types.NewIntField(35)
)
// Result: ~45% (buckets [36-50] and [51-80])
// Expected rows: 45,000
```

**Optimizer decision**: Might use index or table scan depending on other factors

### Example 3: Complex AND Condition

```go
// WHERE age > 30 AND country = 'USA' AND status = 'active'

sel1 := 0.5  // age > 30 (50%)
sel2 := 0.4  // country = 'USA' (40%)
sel3 := 0.3  // status = 'active' (30%)

// Combine with AND
combined := sel1 * sel2 * sel3
// Result: 0.5 × 0.4 × 0.3 = 0.06 (6%)
// If table has 100,000 rows: expect 6,000 rows
```

**Optimizer decision**: This is selective enough to use indexes on any of these columns

### Example 4: OR vs AND

```go
// Scenario A: WHERE country = 'USA' OR country = 'Canada'
sel_usa := 0.5
sel_canada := 0.15
sel_or := sel_usa + sel_canada - (sel_usa * sel_canada)
// = 0.5 + 0.15 - 0.075 = 0.575 (57.5%)

// Scenario B: WHERE country = 'USA' AND status = 'active'
sel_usa := 0.5
sel_active := 0.3
sel_and := sel_usa * sel_active
// = 0.5 × 0.3 = 0.15 (15%)
```

Notice how **OR makes selectivity higher** (less selective) while **AND makes it lower** (more selective).

## API Reference

### SelectivityEstimator

Main struct for estimating selectivity:

```go
type SelectivityEstimator struct {
    catalog *catalog.SystemCatalog
    tx      *transaction.TransactionContext
}
```

### Constructor

```go
func NewSelectivityEstimator(
    cat *catalog.SystemCatalog,
    tx *transaction.TransactionContext
) *SelectivityEstimator
```

### Core Methods

#### EstimateWithValue
Most accurate estimation when the comparison value is known.

```go
func (se *SelectivityEstimator) EstimateWithValue(
    pred primitives.Predicate,
    tableID int,
    columnName string,
    value types.Field
) float64
```

**Use when:** You know the specific value being compared (e.g., `age = 25`)

#### EstimatePredicateSelectivity
Estimation without knowing the specific value.

```go
func (se *SelectivityEstimator) EstimatePredicateSelectivity(
    pred primitives.Predicate,
    tableID int,
    columnName string
) float64
```

**Use when:** Predicate type is known but value isn't (e.g., during join estimation)

#### EstimateNull
Estimates NULL checks.

```go
func (se *SelectivityEstimator) EstimateNull(
    tableID int,
    columnName string,
    isNull bool
) float64
```

**Parameters:**
- `isNull`: `true` for IS NULL, `false` for IS NOT NULL

#### EstimateLike
Estimates LIKE pattern matching.

```go
func (se *SelectivityEstimator) EstimateLike(pattern string) float64
```

**Pattern types:**
- `"prefix%"`: Returns 0.1
- `"%suffix"`: Returns 0.3
- `"%substring%"`: Returns 0.5
- `"exact"`: Returns 0.01

#### EstimateIn
Estimates IN clause selectivity.

```go
func (se *SelectivityEstimator) EstimateIn(
    tableID int,
    columnName string,
    valueCount int
) float64
```

**Formula:** `min(valueCount / distinctCount, 1.0)`

#### EstimateCombined
Combines multiple predicates with AND/OR.

```go
func (se *SelectivityEstimator) EstimateCombined(
    s1, s2 float64,
    and bool
) float64
```

**Formulas:**
- AND: `s1 × s2`
- OR: `s1 + s2 - (s1 × s2)`

#### EstimateNot
Negates a predicate.

```go
func (se *SelectivityEstimator) EstimateNot(sel float64) float64
```

**Formula:** `1.0 - sel`

### Constants

Default selectivity values when statistics are unavailable:

```go
const (
    DefaultSelectivity  = 0.1   // 10% - unknown predicate
    EqualitySelectivity = 0.01  // 1% - equality without stats
    RangeSelectivity    = 0.33  // 33% - range predicate
    LikeSelectivity     = 0.2   // 20% - LIKE predicate
    InSelectivity       = 0.05  // 5% - IN predicate
    NullSelectivity     = 0.05  // 5% - IS NULL predicate
    MaxSelectivity      = 1.0   // 100% - all rows
)
```

## Key Takeaways

1. **Selectivity estimation is crucial** for the query optimizer to make good decisions
2. **Use statistics when available**: MCVs and histograms provide the most accurate estimates
3. **Fallback gracefully**: When stats aren't available, use reasonable defaults
4. **Independence assumption**: Combined predicates assume independence (may not be true in reality)
5. **Better estimates = faster queries**: Even rough estimates are better than no estimates

## Related Components

- [Cardinality Estimation](../cardinality/README.md): Uses selectivity to estimate result set sizes
- [Cost Model](../cost_model/README.md): Uses cardinality to estimate query costs
- [Catalog Statistics](../../catalog/): Stores the statistics used by selectivity estimation
