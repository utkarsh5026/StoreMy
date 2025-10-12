# Hash Index Implementation

This package provides a hash-based index implementation for the StoreMy database system.

## Overview

The hash index uses a **bucket-based hashing** approach with overflow pages to handle collisions. It implements the `Index` interface defined in `pkg/storage/index/index.go`.

## Architecture

### Components

1. **HashIndex** (`hash.go`): Main index structure implementing the `Index` interface
2. **HashFile** (`hash_file.go`): Manages persistent storage of hash index pages
3. **HashPage** (`hash_page.go`): Represents a bucket page containing key-value entries
4. **HashPageID** (`hash_pageid.go`): Unique identifier for hash pages

### Key Features

- **Fast lookups**: O(1) average-case search performance
- **Overflow handling**: Automatically creates overflow pages when buckets are full
- **Multiple key types**: Supports INT, STRING, BOOL, and FLOAT keys
- **Duplicate keys**: Allows multiple tuples with the same key value
- **Transaction support**: Integrates with the transaction system via TransactionID

## Usage

### Creating a Hash Index

```go
// Create a hash file
file, err := NewHashFile("index.dat", types.IntType, 256) // 256 buckets
if err != nil {
    return err
}

// Create the hash index
hashIndex := NewHashIndex(1, types.IntType, file)
```

### Insert

```go
key := types.NewIntField(42)
rid := &tuple.TupleRecordID{
    PageID:   pageID,
    TupleNum: 0,
}

err := hashIndex.Insert(tid, key, rid)
```

### Search

```go
key := types.NewIntField(42)
results, err := hashIndex.Search(tid, key)
// results contains all TupleRecordIDs for this key
```

### Delete

```go
err := hashIndex.Delete(tid, key, rid)
```

### Range Search

```go
startKey := types.NewIntField(10)
endKey := types.NewIntField(50)
results, err := hashIndex.RangeSearch(tid, startKey, endKey)
```

**Note**: Range search on hash indexes is inefficient (O(n)) as it requires scanning all buckets.

## File Structure

Each hash index is stored in a single file with the following structure:

- **Bucket pages**: Primary pages for each hash bucket (pages 0 to numBuckets-1)
- **Overflow pages**: Additional pages when buckets become full

### Page Layout

```
+-----------------+
| Bucket Number   | 4 bytes
| Num Entries     | 4 bytes
| Overflow Page   | 4 bytes (-1 if none)
+-----------------+
| Entry 1         |
| Entry 2         |
| ...             |
+-----------------+
```

### Entry Layout

```
+-----------------+
| Key Type        | 1 byte
| Key Data        | variable
| Table ID        | 4 bytes
| Page Number     | 4 bytes
| Tuple Number    | 4 bytes
+-----------------+
```

## Hash Function

The implementation uses FNV-1a hash function for distributing keys across buckets:

```go
bucketNum = hash(key) % numBuckets
```

## Performance Characteristics

| Operation | Average Case | Worst Case |
|-----------|--------------|------------|
| Insert    | O(1)         | O(n)*      |
| Search    | O(1)         | O(n)*      |
| Delete    | O(1)         | O(n)*      |
| Range     | O(n)         | O(n)       |

\* Worst case occurs with many collisions in a single bucket

## Comparison with B-Tree Index

| Feature           | Hash Index | B-Tree Index |
|-------------------|------------|--------------|
| Point queries     | O(1)       | O(log n)     |
| Range queries     | O(n)       | O(log n + k) |
| Ordered traversal | No         | Yes          |
| Memory efficiency | Good       | Better       |

## Limitations

1. **Range queries**: Inefficient for range searches (must scan all buckets)
2. **Ordering**: Does not maintain key order
3. **Iterator**: Currently not fully implemented for sparse buckets
4. **Fixed buckets**: Number of buckets is fixed at creation time

## Best Use Cases

- **Point queries**: When you frequently search for exact key matches
- **High cardinality**: Large number of distinct keys
- **Random access**: No need for ordered traversal or range queries

## Testing

Run tests with:

```bash
go test ./pkg/storage/index/hash -v
```

The test suite includes:
- Basic insert/search/delete operations
- String and numeric key types
- Overflow page handling
- Range search functionality
- Persistence/recovery
- Serialization/deserialization

## Future Improvements

1. **Dynamic resizing**: Automatically grow/shrink the number of buckets
2. **Better hash function**: Consider locality-sensitive hashing
3. **Iterator optimization**: Fix iterator for sparse bucket scenarios
4. **Statistics**: Track bucket fill rates and collision statistics
5. **Compression**: Compress overflow chains
