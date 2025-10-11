# Iterator Package

The `iterator` package provides a standardized set of interfaces and utility functions for iterating over tuples in the StoreMy database system. It enables efficient traversal of data from various sources such as tables, indexes, and intermediate query results.

## Overview

This package defines three main components:

1. **TupleIterator** - Minimal iteration interface
2. **DbIterator** - Full-featured iterator for the execution engine
3. **DbFileIterator** - Storage-level iterator for database files
4. **Utility Functions** - Functional-style operations on iterators

## Interfaces

### TupleIterator

The base interface that captures the essential iteration methods:

```go
type TupleIterator interface {
    HasNext() (bool, error)
    Next() (*tuple.Tuple, error)
}
```

Both `DbIterator` and `DbFileIterator` embed this interface, allowing generic utility functions to work with any iterator type.

### DbIterator

A comprehensive iterator interface used by the query execution engine. It extends `TupleIterator` with lifecycle management and schema information:

```go
type DbIterator interface {
    TupleIterator
    Open() error
    Rewind() error
    Close() error
    GetTupleDesc() *tuple.TupleDescription
}
```

**Methods:**
- `Open()` - Initializes the iterator before use
- `Rewind()` - Resets the iterator to the beginning
- `Close()` - Releases resources held by the iterator
- `GetTupleDesc()` - Returns schema information for the tuples

### DbFileIterator

A storage-level iterator for traversing tuples in database files (e.g., HeapFile). It extends `TupleIterator` with lifecycle methods but excludes schema information:

```go
type DbFileIterator interface {
    TupleIterator
    Open() error
    Rewind() error
    Close() error
}
```

## Utility Functions

The package provides a rich set of functional-style utility functions that work with any `TupleIterator`:

### Core Functions

#### Iterate
```go
func Iterate(iter TupleIterator, processFunc func(*tuple.Tuple) (continueLooping bool, err error)) error
```
Generic helper that encapsulates the common iteration pattern. The process function can control iteration flow by returning `false` to stop early.

#### ForEach
```go
func ForEach(iter TupleIterator, processFunc func(*tuple.Tuple) error) error
```
Applies a processing function to each tuple. Stops early if an error occurs.

### Transformation Functions

#### Map
```go
func Map(iter TupleIterator, transform func(*tuple.Tuple) (*tuple.Tuple, error)) ([]*tuple.Tuple, error)
```
Transforms each tuple using the provided function. Returns a slice of transformed tuples.

#### Filter
```go
func Filter(iter TupleIterator, predicate func(*tuple.Tuple) (bool, error)) ([]*tuple.Tuple, error)
```
Returns all tuples that satisfy the predicate function.

#### Reduce
```go
func Reduce[T any](iter TupleIterator, initial T, accumulator func(T, *tuple.Tuple) (T, error)) (T, error)
```
Accumulates a value by applying a function to each tuple. Generic function that works with any accumulator type.

### Collection Functions

#### Collect
```go
func Collect(iter TupleIterator) ([]*tuple.Tuple, error)
```
Returns all tuples from the iterator as a slice. **Note:** This consumes the entire iterator and loads all tuples into memory.

#### Count
```go
func Count(iter TupleIterator) (int, error)
```
Returns the total number of tuples in the iterator. **Note:** This consumes the entire iterator.

#### Take
```go
func Take(iter TupleIterator, n int) ([]*tuple.Tuple, error)
```
Returns up to `n` tuples from the iterator. If fewer tuples are available, returns all available tuples.

#### Skip
```go
func Skip(iter TupleIterator, n int) error
```
Consumes and discards `n` tuples from the iterator. Returns `nil` if the iterator exhausts before `n` tuples are skipped.

## Usage Examples

### Basic Iteration
```go
// Open the iterator
err := dbIter.Open()
if err != nil {
    return err
}
defer dbIter.Close()

// Iterate through all tuples
err = iterator.ForEach(dbIter, func(tup *tuple.Tuple) error {
    fmt.Println(tup)
    return nil
})
```

### Filtering Tuples
```go
// Find all tuples where the first field equals 42
filtered, err := iterator.Filter(dbIter, func(tup *tuple.Tuple) (bool, error) {
    field := tup.GetField(0)
    if intField, ok := field.(*IntField); ok {
        return intField.Value == 42, nil
    }
    return false, nil
})
```

### Transforming Data
```go
// Transform tuples by doubling the first integer field
transformed, err := iterator.Map(dbIter, func(tup *tuple.Tuple) (*tuple.Tuple, error) {
    field := tup.GetField(0)
    if intField, ok := field.(*IntField); ok {
        intField.Value *= 2
    }
    return tup, nil
})
```

### Aggregation
```go
// Calculate the sum of the first field across all tuples
sum, err := iterator.Reduce(dbIter, 0, func(acc int, tup *tuple.Tuple) (int, error) {
    if intField, ok := tup.GetField(0).(*IntField); ok {
        return acc + intField.Value, nil
    }
    return acc, nil
})
```

### Limiting Results
```go
// Get the first 10 tuples
firstTen, err := iterator.Take(dbIter, 10)

// Skip the first 5 tuples, then process the rest
err = iterator.Skip(dbIter, 5)
if err != nil {
    return err
}
// Continue using the iterator...
```

## Design Principles

1. **Abstraction**: The `TupleIterator` interface provides a minimal abstraction that allows utility functions to work with different iterator types.

2. **Composition**: Both `DbIterator` and `DbFileIterator` embed `TupleIterator`, enabling code reuse and polymorphism.

3. **Functional Style**: Utility functions follow functional programming patterns (map, filter, reduce) for expressive data processing.

4. **Error Handling**: All functions properly propagate errors, allowing robust error handling at the application level.

5. **Memory Awareness**: Functions that materialize results (like `Collect`, `Map`, `Filter`) explicitly document that they load data into memory.

## Files

- **[common.go](common.go)** - Contains `TupleIterator` interface and all utility functions
- **[db.go](db.go)** - Defines the `DbIterator` interface for the execution engine
- **[dbfile.go](dbfile.go)** - Defines the `DbFileIterator` interface for storage layer

## Notes

- Always call `Open()` before using an iterator and `Close()` when done to ensure proper resource management.
- Some utility functions (like `Count` and `Collect`) consume the entire iterator. Use `Rewind()` if you need to iterate again.
- Nil tuples are automatically skipped by the `Iterate` function.
- The iterator pattern is fundamental to query execution in StoreMy, enabling lazy evaluation and efficient memory usage.
