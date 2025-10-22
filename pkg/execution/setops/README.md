# Set Operations

## Table of Contents
- [What are Set Operations?](#what-are-set-operations)
- [Why Set Operations Matter](#why-set-operations-matter)
- [Core Concepts](#core-concepts)
- [Set Operation Types](#set-operation-types)
- [Implementation Details](#implementation-details)
- [Performance Characteristics](#performance-characteristics)
- [How It Works](#how-it-works)
- [Examples](#examples)
- [API Reference](#api-reference)

## What are Set Operations?

**Set operations** are SQL operators that combine or compare results from two query inputs, treating the result sets as mathematical sets. They allow you to:

- **Combine** results from multiple queries (UNION)
- **Find common** elements between queries (INTERSECT)
- **Find differences** between queries (EXCEPT)
- **Remove duplicates** from a single query (DISTINCT)

All set operations follow **SQL standard semantics** with two variants:
1. **Standard form**: Returns distinct tuples (set semantics)
2. **ALL form**: Preserves duplicates (bag semantics)

**Example:**
```sql
-- Combine customers from two regions
SELECT * FROM customers_east
UNION
SELECT * FROM customers_west

-- Find products sold in both stores
SELECT product_id FROM store_a_sales
INTERSECT
SELECT product_id FROM store_b_sales

-- Find customers who ordered but never cancelled
SELECT customer_id FROM orders
EXCEPT
SELECT customer_id FROM cancellations
```

## Why Set Operations Matter

Set operations are essential for many real-world queries where you need to combine, compare, or deduplicate data.

### 1. Data Integration

**Scenario:** Merge data from multiple sources

```sql
-- Combine user data from different platforms
SELECT user_id, email FROM web_users
UNION
SELECT user_id, email FROM mobile_users
UNION
SELECT user_id, email FROM api_users
```

**Without UNION:** Would need complex JOIN conditions or application-level merging

### 2. Finding Overlaps

**Scenario:** Identify customers who bought both product types

```sql
-- Customers who bought both electronics AND furniture
SELECT customer_id FROM purchases WHERE category = 'Electronics'
INTERSECT
SELECT customer_id FROM purchases WHERE category = 'Furniture'
```

**Without INTERSECT:** Would need self-join with complex conditions:
```sql
SELECT DISTINCT p1.customer_id
FROM purchases p1
JOIN purchases p2 ON p1.customer_id = p2.customer_id
WHERE p1.category = 'Electronics' AND p2.category = 'Furniture'
```

### 3. Finding Differences

**Scenario:** Identify inactive customers

```sql
-- Customers who registered but never made a purchase
SELECT customer_id FROM registrations
EXCEPT
SELECT customer_id FROM purchases
```

**Without EXCEPT:** Would need anti-join:
```sql
SELECT r.customer_id
FROM registrations r
LEFT JOIN purchases p ON r.customer_id = p.customer_id
WHERE p.customer_id IS NULL
```

### 4. Deduplication

**Scenario:** Remove duplicate records

```sql
-- Get unique email addresses
SELECT DISTINCT email FROM users
```

**Without DISTINCT:** Would need GROUP BY:
```sql
SELECT email FROM users GROUP BY email
```

## Core Concepts

### 1. Set vs. Bag Semantics

**Set Semantics (default):**
- Treats data as a mathematical set
- **Eliminates duplicates** automatically
- Each unique tuple appears **at most once** in the result

**Bag Semantics (ALL variant):**
- Treats data as a multiset (bag)
- **Preserves duplicates** from inputs
- Tuple count depends on operation-specific rules

**Example:**
```
Left:  {1, 2, 2, 3}
Right: {2, 3, 3, 4}

UNION:     {1, 2, 3, 4}           (set: distinct values)
UNION ALL: {1, 2, 2, 3, 2, 3, 3, 4} (bag: all values)
```

### 2. Schema Compatibility

All set operations require **compatible schemas**:

1. **Same number of columns**
2. **Matching column types** (in order)
3. **Column names don't need to match** (result uses left schema)

**Valid:**
```sql
SELECT id, name FROM users      -- (INT, VARCHAR)
UNION
SELECT emp_id, emp_name FROM employees  -- (INT, VARCHAR) ✓
```

**Invalid:**
```sql
SELECT id, name FROM users      -- (INT, VARCHAR)
UNION
SELECT product_id FROM products -- (INT) ✗ Different column count
```

### 3. Duplicate Handling

Each operation handles duplicates differently:

| Operation | Standard (Set) | ALL (Bag) |
|-----------|---------------|-----------|
| **UNION** | Removes all duplicates | Keeps all duplicates |
| **INTERSECT** | Returns distinct matches | Returns min(left_count, right_count) |
| **EXCEPT** | Returns distinct differences | Subtracts right counts from left |
| **DISTINCT** | Removes all duplicates | N/A (no ALL variant) |

### 4. Hash-Based Implementation

All operations use **hash tables** for efficiency:

- **Tuple hashing**: Compute hash from all field values
- **Collision detection**: Compare actual tuples when hashes match
- **Reference counting**: Track tuple occurrences for ALL variants

**Hash function:**
```go
func hashTuple(t *Tuple) uint32 {
    hash := 0
    for each field in t {
        hash = hash * 31 + field.Hash()
    }
    return hash
}
```

## Set Operation Types

### 1. UNION

Combines tuples from both inputs into a single result set.

**Mathematical Definition:**
```
A ∪ B = {x | x ∈ A OR x ∈ B}
```

**SQL Semantics:**

**UNION (without ALL):**
- Returns **distinct** tuples from both inputs
- Duplicates within each input are removed
- Duplicates between inputs are removed

**UNION ALL:**
- Returns **all** tuples from both inputs
- Preserves duplicates from both sides
- Simply concatenates both inputs

**Example:**
```
Left:  {1, 2, 2, 3}
Right: {2, 3, 3, 4}

UNION:     {1, 2, 3, 4}
UNION ALL: {1, 2, 2, 3, 2, 3, 3, 4}
```

**Use Cases:**
- Merging data from multiple sources
- Combining results from different conditions
- Building a master list from partitioned data

**Algorithm:**
```
UNION (set semantics):
  1. Stream all tuples from left input
     - Track each tuple in "seen" set
     - Emit if not previously seen
  2. Stream all tuples from right input
     - Check against "seen" set
     - Emit if not previously seen

UNION ALL (bag semantics):
  1. Stream all tuples from left input → emit all
  2. Stream all tuples from right input → emit all
```

**Performance:**
- **UNION**: O(n + m) time, O(distinct tuples) space
- **UNION ALL**: O(n + m) time, O(1) space

### 2. INTERSECT

Returns tuples that appear in **both** inputs.

**Mathematical Definition:**
```
A ∩ B = {x | x ∈ A AND x ∈ B}
```

**SQL Semantics:**

**INTERSECT (without ALL):**
- Returns **distinct** tuples present in both inputs
- A tuple appears once even if duplicated in both sides

**INTERSECT ALL:**
- Returns tuples preserving duplicates
- Count = **min(count_left, count_right)** for each unique tuple
- If left has 3 copies and right has 2 copies → output 2 copies

**Example:**
```
Left:  {1, 2, 2, 2, 3}
Right: {2, 2, 3, 3, 4}

INTERSECT:     {2, 3}        (distinct common tuples)
INTERSECT ALL: {2, 2, 3}     (min(3,2)=2 for value 2, min(1,2)=1 for value 3)
```

**Use Cases:**
- Finding common elements
- Identifying overlapping datasets
- Validating data consistency across sources

**Algorithm:**
```
INTERSECT:
  1. Build hash set from right input (with counts)
  2. Stream tuples from left input
  3. For each left tuple:
     - Check if exists in right set
     - If yes and not yet output: emit and mark as output
     - If no: skip

INTERSECT ALL:
  1. Build hash set from right input with reference counts
  2. Stream tuples from left input
  3. For each left tuple:
     - Check right count
     - If count > 0: emit and decrement right count
     - If count = 0: skip
```

**Performance:**
- **Time**: O(n + m) - build right set + scan left
- **Space**: O(m) - store right set

### 3. EXCEPT

Returns tuples from left input that **do NOT** appear in right input.

**Mathematical Definition:**
```
A - B = {x | x ∈ A AND x ∉ B}
```

Also known as **MINUS** in some databases (Oracle).

**SQL Semantics:**

**EXCEPT (without ALL):**
- Returns **distinct** tuples from left not in right
- Removes all duplicates from result

**EXCEPT ALL:**
- Returns tuples from left, removing matching occurrences from right
- For each unique tuple: output_count = **max(0, left_count - right_count)**
- If left has 5 copies and right has 2 copies → output 3 copies

**Example:**
```
Left:  {1, 2, 2, 2, 3}
Right: {2, 3, 3, 4}

EXCEPT:     {1}              (distinct tuples in left not in right)
EXCEPT ALL: {1, 2, 2}        (left has 3×'2', right has 1×'2' → output 2×'2')
```

**Use Cases:**
- Finding missing elements
- Identifying inactive users (registered but never active)
- Data cleanup (remove known bad records)

**Algorithm:**
```
EXCEPT:
  1. Build hash set from right input
  2. Stream tuples from left input
  3. For each left tuple:
     - Check if in right set OR already seen
     - If not: mark as seen and emit
     - If yes: skip

EXCEPT ALL:
  1. Build hash set from right input with reference counts
  2. Stream tuples from left input
  3. For each left tuple:
     - Check right count
     - If count > 0: decrement count and skip
     - If count = 0: emit (no more matches to remove)
```

**Performance:**
- **Time**: O(n + m) - build right set + scan left
- **Space**: O(m) - store right set

### 4. DISTINCT

Removes duplicate tuples from a **single** input.

**Mathematical Definition:**
```
DISTINCT(A) = {x | x ∈ A} (set conversion)
```

**SQL Semantics:**
- Returns only **unique** tuples
- First occurrence is kept (implementation-dependent)
- No ALL variant (doesn't make sense for single input)

**Example:**
```
Input:    {1, 2, 2, 3, 2, 1, 4}
DISTINCT: {1, 2, 3, 4}
```

**Use Cases:**
- Removing duplicate records
- Getting unique values (like unique email addresses)
- Deduplicating aggregation inputs

**Algorithm:**
```
DISTINCT:
  1. Initialize empty "seen" set
  2. For each input tuple:
     - Compute hash
     - Check if in seen set (with collision detection)
     - If not seen: add to set and emit
     - If seen: skip
```

**Performance:**
- **Time**: O(n) - single pass through input
- **Space**: O(distinct tuples)

## Implementation Details

### 1. TupleSet: Hash-Based Set

The core data structure for all set operations:

```go
type TupleSet struct {
    hashes      map[uint32]int              // Hash → count
    tuples      map[uint32][]*tuple.Tuple   // Hash → tuple list (collision detection)
    preserveAll bool                        // Set (false) or bag (true) semantics
}
```

**Key Features:**
- **Collision detection**: When hashes match, compares actual tuples
- **Reference counting**: Tracks tuple occurrences for ALL variants
- **Dual maps**: Hash map for speed, tuple map for correctness

**Hash Collision Handling:**
```go
// When hash matches, compare actual tuples field-by-field
func tuplesEqual(t1, t2 *Tuple) bool {
    if t1.NumFields() != t2.NumFields() {
        return false
    }

    for i := 0; i < t1.NumFields(); i++ {
        if !t1.GetField(i).Equals(t2.GetField(i)) {
            return false
        }
    }

    return true
}
```

**Why collision detection matters:**
```
Example collision:
  Tuple A: (1, 2, 3) → hash = 12345
  Tuple B: (3, 2, 1) → hash = 12345 (same hash, different tuple!)

Without collision detection: B would be treated as A (WRONG!)
With collision detection: Compares actual fields, sees they differ (CORRECT!)
```

### 2. Streaming vs. Hash-Building

Different operations use different execution strategies:

**Streaming (UNION, DISTINCT):**
- Process tuples **one at a time** as they arrive
- No need to materialize entire input
- Emit results immediately when determined unique
- Memory efficient for highly distinct data

**Hash-Building (INTERSECT, EXCEPT):**
- **Materialize right input** into hash set first
- Then stream left input against the hash set
- Two-phase execution: build phase + probe phase
- Memory requirement: O(right input size)

**Example: UNION vs. INTERSECT**
```
UNION (streaming):
  Time 0: Read left[0] → emit
  Time 1: Read left[1] → emit
  Time 2: Read right[0] → emit
  ... (incremental output)

INTERSECT (hash-building):
  Phase 1 (build): Read all of right input into hash set
  Phase 2 (probe): Read left, check against hash set, emit matches
```

### 3. Reference Counting for ALL Variants

When `preserveAll = true`, the TupleSet tracks **how many times** each tuple appears:

```go
// Example for INTERSECT ALL
Left:  {2, 2, 2, 3}
Right: {2, 2, 3, 3}

After building right hash set:
  hashes[hash(2)] = 2   // Value 2 appears 2 times
  hashes[hash(3)] = 2   // Value 3 appears 2 times

Processing left:
  Read 2 → count=2 → emit, decrement → count=1
  Read 2 → count=1 → emit, decrement → count=0
  Read 2 → count=0 → skip (no more matches in right)
  Read 3 → count=2 → emit, decrement → count=1

Output: {2, 2, 3}  (min of left and right counts)
```

### 4. Schema Validation

Before executing any set operation, schemas are validated:

```go
func validateSchemaCompatibility(left, right *TupleDescription) error {
    // Check field count
    if left.NumFields() != right.NumFields() {
        return error("field count mismatch")
    }

    // Check each field type
    for i := 0; i < left.NumFields(); i++ {
        if left.TypeAt(i) != right.TypeAt(i) {
            return error("type mismatch at field %d", i)
        }
    }

    return nil
}
```

**Why this matters:**
```sql
-- Valid: same types
SELECT id, name FROM users     -- (INT, VARCHAR)
UNION
SELECT id, title FROM products -- (INT, VARCHAR) ✓

-- Invalid: different types
SELECT id, name FROM users     -- (INT, VARCHAR)
UNION
SELECT name, id FROM products  -- (VARCHAR, INT) ✗
```

## Performance Characteristics

### Time Complexity

| Operation | Time Complexity | Notes |
|-----------|----------------|-------|
| UNION | O(n + m) | Single pass through both inputs |
| UNION ALL | O(n + m) | No hash set needed, pure streaming |
| INTERSECT | O(n + m) | Build right hash + scan left |
| INTERSECT ALL | O(n + m) | Build right hash + scan left |
| EXCEPT | O(n + m) | Build right hash + scan left |
| EXCEPT ALL | O(n + m) | Build right hash + scan left |
| DISTINCT | O(n) | Single pass with hash set |

Where:
- `n` = left input cardinality
- `m` = right input cardinality

### Space Complexity

| Operation | Space Complexity | Notes |
|-----------|-----------------|-------|
| UNION | O(distinct tuples) | Tracks seen tuples for deduplication |
| UNION ALL | O(1) | No hash set, pure streaming |
| INTERSECT | O(m + output) | Stores right set + distinct output tracking |
| INTERSECT ALL | O(m) | Stores right set with counts |
| EXCEPT | O(m + distinct left) | Stores right set + seen left tuples |
| EXCEPT ALL | O(m) | Stores right set with counts |
| DISTINCT | O(distinct tuples) | Stores seen set |

### Performance Tips

**1. UNION ALL is faster than UNION**
```
UNION:     Need to hash and track all tuples
UNION ALL: Simple concatenation, no hashing

Speed difference: 2-10× faster for UNION ALL
```

**2. Put smaller input on the right for INTERSECT/EXCEPT**
```
Good:  1M rows INTERSECT 100 rows
       → Build hash set of 100 rows
       → Scan 1M rows against small set

Bad:   100 rows INTERSECT 1M rows
       → Build hash set of 1M rows (slow!)
       → Scan 100 rows against huge set
```

**3. Pre-filter before set operations**
```
Slow:
  SELECT * FROM huge_table_a
  INTERSECT
  SELECT * FROM huge_table_b

Fast:
  SELECT * FROM huge_table_a WHERE country = 'USA'
  INTERSECT
  SELECT * FROM huge_table_b WHERE country = 'USA'
```

**4. Use DISTINCT sparingly**
```
Slow:
  SELECT DISTINCT * FROM (
    SELECT * FROM orders
    UNION ALL
    SELECT * FROM archived_orders
  )

Fast:
  SELECT * FROM orders
  UNION
  SELECT * FROM archived_orders
```
UNION already removes duplicates, no need for DISTINCT!

## How It Works

### End-to-End Example: UNION

**SQL Query:**
```sql
SELECT customer_id, name FROM active_customers
UNION
SELECT customer_id, name FROM archived_customers
```

**Execution Flow:**

**Step 1: Schema Validation**
```
Left schema:  (INT, VARCHAR)
Right schema: (INT, VARCHAR)
→ Compatible ✓
```

**Step 2: Initialize Tracking**
```go
seen := NewTupleSet(false) // Set semantics (no preserveAll)
leftDone := false
```

**Step 3: Stream Left Input**
```
Read tuple (1, "Alice"):
  hash = hash(1, "Alice") = 12345
  Check seen[12345] → not found
  Add to seen[12345] = 1
  → Emit (1, "Alice")

Read tuple (2, "Bob"):
  hash = hash(2, "Bob") = 67890
  Check seen[67890] → not found
  Add to seen[67890] = 1
  → Emit (2, "Bob")

Read tuple (1, "Alice"):  // Duplicate!
  hash = hash(1, "Alice") = 12345
  Check seen[12345] → found
  Compare tuples → equal
  → Skip (already seen)

Left input exhausted → leftDone = true
```

**Step 4: Stream Right Input**
```
Read tuple (2, "Bob"):  // Duplicate from left!
  hash = hash(2, "Bob") = 67890
  Check seen[67890] → found
  Compare tuples → equal
  → Skip (already seen)

Read tuple (3, "Charlie"):
  hash = hash(3, "Charlie") = 11111
  Check seen[11111] → not found
  Add to seen[11111] = 1
  → Emit (3, "Charlie")

Right input exhausted → Done
```

**Final Result:**
```
(1, "Alice")
(2, "Bob")
(3, "Charlie")
```

### End-to-End Example: INTERSECT ALL

**SQL Query:**
```sql
SELECT product_id FROM store_a_sales
INTERSECT ALL
SELECT product_id FROM store_b_sales
```

**Data:**
```
Store A: {101, 101, 101, 102, 103}
Store B: {101, 101, 102, 102, 104}
```

**Execution Flow:**

**Step 1: Build Right Hash Set**
```go
rightSet := NewTupleSet(true) // Bag semantics (preserveAll)

Process Store B:
  Read 101 → hash=H1 → hashes[H1]=1
  Read 101 → hash=H1 → hashes[H1]=2
  Read 102 → hash=H2 → hashes[H2]=1
  Read 102 → hash=H2 → hashes[H2]=2
  Read 104 → hash=H3 → hashes[H3]=1

Final state:
  hashes[H1]=2  (product 101 appears 2 times)
  hashes[H2]=2  (product 102 appears 2 times)
  hashes[H3]=1  (product 104 appears 1 time)
```

**Step 2: Probe with Left Input**
```
Read 101:
  hash=H1, count=2 → emit, decrement → count=1

Read 101:
  hash=H1, count=1 → emit, decrement → count=0

Read 101:
  hash=H1, count=0 → skip (no more matches in right)

Read 102:
  hash=H2, count=2 → emit, decrement → count=1

Read 103:
  hash=H4, not in right → skip
```

**Final Result:**
```
101  (appeared 3 times in A, 2 times in B → output min(3,2)=2)
101
102  (appeared 1 time in A, 2 times in B → output min(1,2)=1)
```

### Collision Detection Example

**Why we need collision detection:**

```
Tuple A: (1, 100)
Tuple B: (10, 10)

Naive hash: hash(A) = 1*31 + 100 = 131
            hash(B) = 10*31 + 10 = 320
            Different hashes → OK

But with different values:
Tuple C: (2, 50)
Tuple D: (5, 20)

hash(C) = 2*31 + 50 = 112
hash(D) = 5*31 + 20 = 175  // Different, but...

Better hash with 32-bit overflow:
Tuple E: (123, 456)
Tuple F: (789, 012)

hash(E) might equal hash(F) due to overflow!
```

**Collision handling:**
```go
// When hashes match, compare actual tuples
existingTuples := tuples[hash]
for _, existing := range existingTuples {
    if tuplesEqual(newTuple, existing) {
        // True duplicate
        return false
    }
}
// Hash collision, but different tuple
tuples[hash] = append(existingTuples, newTuple)
```

## Examples

### Example 1: Basic UNION

```go
// Two input iterators with compatible schemas
leftIter := createTableIterator(activeCustomers)  // (id INT, name VARCHAR)
rightIter := createTableIterator(archivedCustomers) // (id INT, name VARCHAR)

// Create UNION operator
union, err := NewUnion(leftIter, rightIter, false) // false = distinct
if err != nil {
    log.Fatal(err)
}

// Open and iterate
union.Open()
defer union.Close()

for {
    tuple, err := union.Next()
    if tuple == nil {
        break // No more tuples
    }
    fmt.Printf("Customer: %v\n", tuple)
}

// Output: All unique customers from both tables
```

### Example 2: UNION ALL for Performance

```go
// Merging large datasets without deduplication
leftIter := createTableIterator(sales2023)
rightIter := createTableIterator(sales2024)

// UNION ALL: no hash set needed, faster!
unionAll, _ := NewUnion(leftIter, rightIter, true) // true = preserve all

unionAll.Open()
defer unionAll.Close()

count := 0
for {
    tuple, _ := unionAll.Next()
    if tuple == nil {
        break
    }
    count++
}

fmt.Printf("Total records: %d\n", count)
// Output: Sum of both table sizes (no deduplication)
```

### Example 3: INTERSECT to Find Common Values

```go
// Find products sold in both stores
storeAIter := createQueryIterator("SELECT product_id FROM store_a_sales")
storeBIter := createQueryIterator("SELECT product_id FROM store_b_sales")

intersect, _ := NewIntersect(storeAIter, storeBIter, false) // Distinct

intersect.Open()
defer intersect.Close()

fmt.Println("Products sold in both stores:")
for {
    tuple, _ := intersect.Next()
    if tuple == nil {
        break
    }
    productID, _ := tuple.GetField(0)
    fmt.Printf("Product ID: %v\n", productID)
}
```

### Example 4: INTERSECT ALL with Counts

```go
// Find common purchases preserving duplicate counts
user1Purchases := createQueryIterator("SELECT item_id FROM user1_cart")
user2Purchases := createQueryIterator("SELECT item_id FROM user2_cart")

// INTERSECT ALL: min(count_user1, count_user2)
intersectAll, _ := NewIntersect(user1Purchases, user2Purchases, true)

intersectAll.Open()
defer intersectAll.Close()

items := make(map[int]int)
for {
    tuple, _ := intersectAll.Next()
    if tuple == nil {
        break
    }
    itemID, _ := tuple.GetField(0).ToInt()
    items[itemID]++
}

fmt.Println("Common items with counts:")
for id, count := range items {
    fmt.Printf("Item %d: %d times\n", id, count)
}
```

### Example 5: EXCEPT for Differences

```go
// Find customers who registered but never made a purchase
registeredIter := createQueryIterator("SELECT customer_id FROM registrations")
purchasedIter := createQueryIterator("SELECT customer_id FROM purchases")

except, _ := NewExcept(registeredIter, purchasedIter, false) // Distinct

except.Open()
defer except.Close()

fmt.Println("Inactive customers:")
for {
    tuple, _ := except.Next()
    if tuple == nil {
        break
    }
    customerID, _ := tuple.GetField(0)
    fmt.Printf("Customer %v never purchased\n", customerID)
}
```

### Example 6: EXCEPT ALL with Count Subtraction

```go
// Find inventory items after removing sold items
inventoryIter := createQueryIterator("SELECT sku FROM warehouse_inventory")
soldIter := createQueryIterator("SELECT sku FROM daily_sales")

// EXCEPT ALL: warehouse_count - sold_count
exceptAll, _ := NewExcept(inventoryIter, soldIter, true)

exceptAll.Open()
defer exceptAll.Close()

remaining := make(map[string]int)
for {
    tuple, _ := exceptAll.Next()
    if tuple == nil {
        break
    }
    sku, _ := tuple.GetField(0).ToString()
    remaining[sku]++
}

fmt.Println("Remaining inventory:")
for sku, count := range remaining {
    fmt.Printf("%s: %d units\n", sku, count)
}
```

### Example 7: DISTINCT for Deduplication

```go
// Get unique email addresses
emailsIter := createQueryIterator("SELECT email FROM users")

distinct, _ := NewDistinct(emailsIter)

distinct.Open()
defer distinct.Close()

uniqueEmails := []string{}
for {
    tuple, _ := distinct.Next()
    if tuple == nil {
        break
    }
    email, _ := tuple.GetField(0).ToString()
    uniqueEmails = append(uniqueEmails, email)
}

fmt.Printf("Found %d unique emails\n", len(uniqueEmails))
```

### Example 8: Complex Query with Multiple Set Operations

```go
// (A UNION B) INTERSECT (C EXCEPT D)

// Build left side: A UNION B
unionAB, _ := NewUnion(iterA, iterB, false)

// Build right side: C EXCEPT D
exceptCD, _ := NewExcept(iterC, iterD, false)

// Combine: (A UNION B) INTERSECT (C EXCEPT D)
finalResult, _ := NewIntersect(unionAB, exceptCD, false)

finalResult.Open()
defer finalResult.Close()

// Iterate result
for {
    tuple, _ := finalResult.Next()
    if tuple == nil {
        break
    }
    fmt.Printf("Result: %v\n", tuple)
}
```

## API Reference

### Union

Combines tuples from two inputs.

```go
type Union struct {
    *SetOp
}
```

#### NewUnion

```go
func NewUnion(
    left, right iterator.DbIterator,
    unionAll bool,
) (*Union, error)
```

**Parameters:**
- `left`: Left input iterator
- `right`: Right input iterator
- `unionAll`: If `true`, preserves duplicates (UNION ALL); if `false`, returns distinct

**Returns:** Union operator or error if schemas incompatible

**Errors:**
- Schema mismatch (different field counts or types)
- Nil iterator

### Intersect

Returns tuples appearing in both inputs.

```go
type Intersect struct {
    *SetOp
}
```

#### NewIntersect

```go
func NewIntersect(
    left, right iterator.DbIterator,
    intersectAll bool,
) (*Intersect, error)
```

**Parameters:**
- `left`: Left input iterator
- `right`: Right input iterator
- `intersectAll`: If `true`, preserves duplicates (min counts); if `false`, returns distinct

**Algorithm:** Builds hash set from right, probes with left

### Except

Returns tuples from left not in right.

```go
type Except struct {
    *SetOp
}
```

#### NewExcept

```go
func NewExcept(
    left, right iterator.DbIterator,
    exceptAll bool,
) (*Except, error)
```

**Parameters:**
- `left`: Left input iterator (source of potential results)
- `right`: Right input iterator (tuples to exclude)
- `exceptAll`: If `true`, subtracts counts; if `false`, returns distinct

**Algorithm:** Builds hash set from right, filters left against it

### Distinct

Removes duplicate tuples from input.

```go
type Distinct struct {
    base   *query.BaseIterator
    source *query.SourceIter
    seen   *TupleSet
}
```

#### NewDistinct

```go
func NewDistinct(child iterator.DbIterator) (*Distinct, error)
```

**Parameters:**
- `child`: Input iterator to deduplicate

**Returns:** Distinct operator or error

**Algorithm:** Streams input, tracks seen tuples in hash set

### TupleSet

Core hash-based set implementation.

```go
type TupleSet struct {
    hashes      map[uint32]int              // Hash → count
    tuples      map[uint32][]*tuple.Tuple   // Hash → tuple list
    preserveAll bool                        // Set or bag semantics
}
```

#### NewTupleSet

```go
func NewTupleSet(preserveAll bool) *TupleSet
```

**Parameters:**
- `preserveAll`: If `true`, tracks counts (bag); if `false`, only presence (set)

#### Key Methods

**Add:**
```go
func (ts *TupleSet) Add(t *tuple.Tuple) bool
```
Adds tuple to set. Returns `true` if added (set) or count incremented (bag).

**Contains:**
```go
func (ts *TupleSet) Contains(t *tuple.Tuple) bool
```
Checks if tuple exists in set with collision detection.

**GetCount:**
```go
func (ts *TupleSet) GetCount(t *tuple.Tuple) int
```
Returns reference count for tuple (1 for set, actual count for bag).

**Decrement:**
```go
func (ts *TupleSet) Decrement(t *tuple.Tuple) bool
```
Decrements count by 1. Returns `true` if count still > 0.

**Remove:**
```go
func (ts *TupleSet) Remove(t *tuple.Tuple)
```
Removes tuple completely (sets count to 0).

**Clear:**
```go
func (ts *TupleSet) Clear()
```
Empties the set.

### Common Methods (All Set Operations)

All set operation types implement the standard iterator interface:

#### Open

```go
func (op *SetOperation) Open() error
```
Initializes the operator and its children. Must be called before iteration.

#### Close

```go
func (op *SetOperation) Close() error
```
Releases resources and closes children.

#### HasNext

```go
func (op *SetOperation) HasNext() (bool, error)
```
Checks if more tuples are available.

#### Next

```go
func (op *SetOperation) Next() (*tuple.Tuple, error)
```
Returns next tuple from result set.

#### Rewind

```go
func (op *SetOperation) Rewind() error
```
Resets operator to beginning for re-iteration.

#### GetTupleDesc

```go
func (op *SetOperation) GetTupleDesc() *tuple.TupleDescription
```
Returns schema of result (uses left input's schema).

## Key Takeaways

1. **Set operations treat results as mathematical sets** with distinct or bag semantics
2. **ALL variants preserve duplicates**, standard form removes them
3. **Hash-based implementation** provides O(n + m) performance for all operations
4. **Collision detection ensures correctness** when tuples have the same hash
5. **Schema compatibility is required**: same field count and types
6. **UNION streams both inputs**, INTERSECT/EXCEPT build right hash set first
7. **UNION ALL is fastest** (no hash set, pure streaming)
8. **Put smaller input on right** for INTERSECT/EXCEPT to minimize memory
9. **Reference counting enables ALL variants** by tracking tuple occurrences
10. **DISTINCT is single-input UNION** with hash-based deduplication

## Related Components

- [Query Execution](../): Parent package containing the query execution engine
- [Aggregation](../aggregation/): GROUP BY operations that also deduplicate
- [Cardinality Estimation](../../optimizer/cardinality/): Estimates output sizes for set operations
