# Core Aggregation Package

The `core` package provides the foundational building blocks for implementing SQL aggregation operations (COUNT, SUM, AVG, MIN, MAX) with optional GROUP BY support. This package is designed to be the engine behind aggregation queries in the StoreOur database system.

## Table of Contents

- [Overview](#overview)
- [Key Concepts](#key-concepts)
- [Components](#components)
- [Getting Started](#getting-started)
- [Usage Examples](#usage-examples)
- [Thread Safety](#thread-safety)
- [Architecture](#architecture)

---

## Overview

This package implements a flexible aggregation system that separates the **aggregation logic** (how to iterate, manage groups) from the **calculation logic** (how to compute SUM, AVG, etc.). This separation makes it easy to add new aggregate functions without modifying the core iteration logic.

### What can you do with this package?

- **Simple aggregates**: `SELECT COUNT(*) FROM table`
- **Aggregates on specific fields**: `SELECT SUM(price) FROM orders`
- **Grouped aggregates**: `SELECT category, AVG(price) FROM products GROUP BY category`
- **Multiple aggregate types**: COUNT, SUM, AVG, MIN, MAX

---

## Key Concepts

### 1. **Aggregator** (BaseAggregator)

The aggregator is responsible for:
- Processing input tuples
- Managing groups (for GROUP BY operations)
- Delegating calculations to a calculator
- Providing iteration over results

Think of it as the **orchestrator** that handles the workflow.

### 2. **Calculator** (AggregateCalculator interface)

The calculator is responsible for:
- The actual math (e.g., adding numbers for SUM)
- Maintaining per-group state (e.g., count of items)
- Computing final results (e.g., average = sum / count)

Think of it as the **specialist** that knows how to compute specific aggregate functions.

### 3. **Iterator** (AggregatorIterator)

The iterator provides:
- Sequential access to aggregation results
- Snapshot-based iteration (results are stable during iteration)
- DbIterator interface compatibility

Think of it as the **reader** that lets you traverse results.

---

## Components

### Core Interfaces

#### `GroupAggregator`

The main interface for aggregators. Provides methods to:
- Get all group keys
- Retrieve aggregate values for specific groups
- Access result tuple descriptions
- Thread-safe read operations

**Location**: [interfaces.go](./interfaces.go)

#### `AggregateCalculator`

Interface for implementing specific aggregate calculations. Requires:
- `InitializeGroup(groupKey)` - Set up initial state
- `UpdateAggregate(groupKey, value)` - Process new values
- `GetFinalValue(groupKey)` - Retrieve computed result
- `ValidateOperation(op)` - Check if operation is supported
- `GetResultType(op)` - Return the result data type

**Location**: [interfaces.go](./interfaces.go)

### Core Types

#### `BaseAggregator`

The reference implementation of the aggregation system. It:
- Implements the `GroupAggregator` interface
- Handles both grouped and non-grouped aggregations
- Thread-safe with RWMutex protection
- Delegates calculations to an `AggregateCalculator`

**Key methods**:
- `Merge(tuple)` - Process a new tuple into the aggregate
- `Iterator()` - Create an iterator over results
- `InitializeDefault()` - Initialize default group for empty tables

**Location**: [base.go](./base.go)

#### `AggregatorIterator`

Iterator implementation for traversing aggregation results. Features:
- Snapshot-based iteration (stable results during iteration)
- DbIterator interface compliance
- Support for Rewind() to re-iterate
- Proper resource management (Open/Close)

**Key methods**:
- `Open()` - Take a snapshot and initialize iteration
- `HasNext()` - Check if more results are available
- `Next()` - Get the next result tuple
- `Rewind()` - Reset to beginning (reuses same snapshot)
- `Close()` - Release resources

**Location**: [iterator.go](./iterator.go)

### Constants and Types

#### `AggregateOp`

Enumeration of supported aggregate operations:

```go
const (
    OpCount AggregateOp = iota  // COUNT(*)
    OpSum                        // SUM(field)
    OpAvg                        // AVG(field)
    OpMin                        // MIN(field)
    OpMax                        // MAX(field)
)
```

#### `NoGrouping`

Constant (`-1`) representing aggregation without GROUP BY.

**Location**: [types.go](./types.go)

---

## Getting Started

### Basic Workflow

1. **Create a calculator** for your aggregate type (e.g., IntCalculator for integer operations)
2. **Create a BaseAggregator** with the calculator
3. **Feed tuples** to the aggregator using `Merge()`
4. **Iterate results** using the iterator

### Prerequisites

You need:
- A `tuple.TupleDescription` defining your input schema
- An `AggregateCalculator` implementation for your data type
- Input tuples to aggregate

---

## Usage Examples

### Example 1: Simple COUNT(*) Query

```go
// SELECT COUNT(*) FROM orders

// 1. Create a calculator (assuming you have IntCalculator)
calculator := NewIntCalculator(OpCount)

// 2. Create aggregator (NoGrouping = -1, no grouping field)
agg, err := NewBaseAggregator(
    NoGrouping,           // No GROUP BY
    types.IntType,        // Grouping type (unused)
    0,                    // Field to aggregate (unused for COUNT(*))
    OpCount,              // Operation
    calculator,           // Calculator
)
if err != nil {
    panic(err)
}

// 3. Initialize default group (ensures COUNT(*) returns 0 for empty tables)
agg.InitializeDefault()

// 4. Process tuples
for _, tuple := range inputTuples {
    if err := agg.Merge(tuple); err != nil {
        panic(err)
    }
}

// 5. Iterate results
iter := agg.Iterator()
defer iter.Close()

if err := iter.Open(); err != nil {
    panic(err)
}

for hasNext, _ := iter.HasNext(); hasNext; hasNext, _ = iter.HasNext() {
    tuple, err := iter.Next()
    if err != nil {
        panic(err)
    }

    // Get the count value
    countField, _ := tuple.GetField(0)
    fmt.Printf("Total count: %v\n", countField)
}
```

### Example 2: Grouped SUM Query

```go
// SELECT category, SUM(price) FROM products GROUP BY category

// Assume: field 0 = category (string), field 1 = price (int)

// 1. Create calculator
calculator := NewIntCalculator(OpSum)

// 2. Create aggregator with grouping
agg, err := NewBaseAggregator(
    0,                    // Group by field 0 (category)
    types.StringType,     // Category is a string
    1,                    // Aggregate field 1 (price)
    OpSum,                // SUM operation
    calculator,
)
if err != nil {
    panic(err)
}

// 3. Process tuples
for _, tuple := range productTuples {
    if err := agg.Merge(tuple); err != nil {
        panic(err)
    }
}

// 4. Iterate results
iter := agg.Iterator()
defer iter.Close()

if err := iter.Open(); err != nil {
    panic(err)
}

for hasNext, _ := iter.HasNext(); hasNext; hasNext, _ = iter.HasNext() {
    tuple, err := iter.Next()
    if err != nil {
        panic(err)
    }

    // Result tuple has 2 fields: [category, sum]
    category, _ := tuple.GetField(0)
    sum, _ := tuple.GetField(1)
    fmt.Printf("Category: %v, Total: %v\n", category, sum)
}
```

### Example 3: Implementing a Custom Calculator

```go
// Custom calculator for string operations (e.g., concatenation)

type StringCalculator struct {
    op     AggregateOp
    values map[string][]string  // Store all strings per group
}

func (sc *StringCalculator) InitializeGroup(groupKey string) {
    sc.values[groupKey] = []string{}
}

func (sc *StringCalculator) UpdateAggregate(groupKey string, field types.Field) error {
    strField, ok := field.(*types.StringField)
    if !ok {
        return fmt.Errorf("expected string field")
    }

    sc.values[groupKey] = append(sc.values[groupKey], strField.Value)
    return nil
}

func (sc *StringCalculator) GetFinalValue(groupKey string) (types.Field, error) {
    values := sc.values[groupKey]

    // Example: concatenate all strings
    result := strings.Join(values, ", ")
    return types.NewStringField(result, len(result)), nil
}

func (sc *StringCalculator) ValidateOperation(op AggregateOp) error {
    if op != OpConcat {
        return fmt.Errorf("string calculator only supports CONCAT")
    }
    return nil
}

func (sc *StringCalculator) GetResultType(op AggregateOp) types.Type {
    return types.StringType
}
```

---

## Thread Safety

### What's Protected?

- `BaseAggregator` uses `sync.RWMutex` to protect:
  - Group map modifications
  - Calculator state updates
  - Group key extraction

### Usage Guidelines

1. **Merge() is thread-safe**: Multiple goroutines can call `Merge()` concurrently
2. **Iterator is NOT thread-safe**: Each goroutine should have its own iterator
3. **RLock/RUnlock**: The iterator automatically uses RLock when reading aggregator state

### Safe Concurrent Pattern

```go
// Goroutine 1: Feeding tuples
go func() {
    for _, tuple := range batch1 {
        agg.Merge(tuple)  // Thread-safe
    }
}()

// Goroutine 2: Feeding tuples
go func() {
    for _, tuple := range batch2 {
        agg.Merge(tuple)  // Thread-safe
    }
}()

// Wait for all tuples to be processed...

// Main goroutine: Reading results
iter1 := agg.Iterator()  // Create separate iterator
defer iter1.Close()

// Each goroutine gets its own iterator if concurrent reading is needed
```

---

## Architecture

### Data Flow

```
Input Tuples
    ↓
[BaseAggregator.Merge()]
    ↓
Extract group key + aggregate field
    ↓
[AggregateCalculator.UpdateAggregate()]
    ↓
Store in calculator's internal state
    ↓
[AggregatorIterator.Open()]
    ↓
Snapshot group keys
    ↓
[AggregatorIterator.Next()]
    ↓
Fetch aggregate value from calculator
    ↓
Construct result tuple
    ↓
Output Tuples (group key + aggregate value)
```

### Result Tuple Format

#### Non-grouped Aggregates

```
Tuple: [aggregateValue]

Example: COUNT(*) = 42
Tuple: [IntField(42)]
```

#### Grouped Aggregates

```
Tuple: [groupKey, aggregateValue]

Example: SUM(price) GROUP BY category
Tuple: [StringField("Electronics"), IntField(15000)]
```

### Snapshot Behavior (Iterator)

The iterator takes a **snapshot** of group keys when `Open()` is called:

- ✅ **Stable iteration**: Groups won't change mid-iteration
- ✅ **Consistent results**: Same results if you iterate multiple times without reopening
- ❌ **New groups invisible**: Groups added after `Open()` won't appear in current iteration
- 🔄 **Refresh**: Call `Close()` + `Open()` to get a fresh snapshot

```go
// Snapshot example
iter.Open()           // Snapshot: [A, B, C]
agg.Merge(newTuple)   // Adds group D
iter.Next()           // Still iterates over [A, B, C]
iter.Close()
iter.Open()           // New snapshot: [A, B, C, D]
```

---

## File Reference

| File | Purpose |
|------|---------|
| [base.go](./base.go) | BaseAggregator implementation |
| [interfaces.go](./interfaces.go) | GroupAggregator and AggregateCalculator interfaces |
| [iterator.go](./iterator.go) | AggregatorIterator implementation |
| [types.go](./types.go) | AggregateOp enum and constants |
| [base_test.go](./base_test.go) | Tests for BaseAggregator |
| [iterator_test.go](./iterator_test.go) | Tests for AggregatorIterator |

---

## Testing

Run tests:

```bash
go test ./pkg/execution/aggregation/internal/core
```

Run tests with coverage:

```bash
go test -cover ./pkg/execution/aggregation/internal/core
```

Run specific test:

```bash
go test -run TestAggregatorIterator_Open ./pkg/execution/aggregation/internal/core
```

---

## Common Pitfalls

### 1. Forgetting to Initialize Default Group

```go
// ❌ Wrong: COUNT(*) on empty table returns no rows
agg, _ := NewBaseAggregator(NoGrouping, ...)
// No tuples merged
iter := agg.Iterator()
// No results!

// ✅ Correct: Initialize default group
agg, _ := NewBaseAggregator(NoGrouping, ...)
agg.InitializeDefault()  // Ensures COUNT(*) returns 0
iter := agg.Iterator()
// Returns 1 tuple with value 0
```

### 2. Not Opening Iterator Before Use

```go
// ❌ Wrong: Iterator must be opened
iter := agg.Iterator()
tuple, err := iter.Next()  // Error: iterator not opened

// ✅ Correct: Open before use
iter := agg.Iterator()
iter.Open()
tuple, err := iter.Next()  // Works!
```

### 3. Sharing Iterator Across Goroutines

```go
// ❌ Wrong: Iterator is not thread-safe
iter := agg.Iterator()
go func() { iter.Next() }()  // Race condition!
go func() { iter.Next() }()  // Race condition!

// ✅ Correct: Each goroutine gets its own iterator
iter1 := agg.Iterator()
iter2 := agg.Iterator()
go func() { iter1.Next() }()  // Safe
go func() { iter2.Next() }()  // Safe
```

### 4. Expecting Fresh Data After Rewind()

```go
// ❌ Wrong assumption
iter.Open()           // Snapshot: [A, B]
agg.Merge(newTuple)   // Adds C
iter.Rewind()         // Still sees [A, B], not [A, B, C]

// ✅ Correct: Close and reopen for fresh snapshot
iter.Open()           // Snapshot: [A, B]
agg.Merge(newTuple)   // Adds C
iter.Close()
iter.Open()           // New snapshot: [A, B, C]
```

---

## Performance Considerations

### Memory Usage

- **Group storage**: `O(number of unique groups)`
- **Iterator snapshot**: `O(number of groups)` per iterator
- **Calculator state**: Depends on calculator implementation

### Optimization Tips

1. **Reuse iterators**: Call `Close()` + `Open()` instead of creating new ones
2. **Use Rewind()**: More efficient than reopening if snapshot doesn't need refresh
3. **Batch processing**: Process multiple tuples before iterating
4. **Calculator design**: Keep calculator state minimal

---

## Future Enhancements

Potential improvements to consider:

- [ ] Support for DISTINCT aggregates (e.g., `COUNT(DISTINCT field)`)
- [ ] Support for filtered aggregates (e.g., `COUNT(*) FILTER (WHERE x > 10)`)
- [ ] Parallel aggregation for large datasets
- [ ] Spill-to-disk for aggregates that exceed memory
- [ ] Incremental aggregation (update results as new data arrives)

---

## Related Packages

- `pkg/tuple` - Tuple and TupleDescription types
- `pkg/types` - Field types (IntField, StringField, etc.)
- `pkg/iterator` - DbIterator interface
- `pkg/execution/aggregation/internal/calculators` - Calculator implementations

---

## Questions?

If you're new to this codebase and have questions:

1. Start with the [examples](#usage-examples)
2. Read the [interfaces](./interfaces.go) to understand the contracts
3. Check the [tests](./base_test.go) for more usage patterns
4. Review the inline documentation in each file

Happy aggregating! 🚀
