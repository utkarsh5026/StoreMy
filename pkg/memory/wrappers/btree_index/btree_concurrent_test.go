package btreeindex

import (
	"storemy/pkg/primitives"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"sync"
	"sync/atomic"
	"testing"
)

// Test: Massive concurrent reads (100 goroutines, 1000 searches each)
func TestBTree_MassiveConcurrent_Reads(t *testing.T) {
	bt, _, _, _, cleanup := setupTestBTree(t, types.IntType)
	defer cleanup()

	numEntries := 100
	pageID := page.NewPageDescriptor(1, 0)

	// Insert entries
	for i := 0; i < numEntries; i++ {
		key := types.NewIntField(int64(i))
		rid := tuple.NewTupleRecordID(pageID, primitives.SlotID(i))
		err := bt.Insert(key, rid)
		if err != nil {
			t.Fatalf("Failed to insert entry %d: %v", i, err)
		}
	}

	// Massive concurrent reads
	var wg sync.WaitGroup
	numGoroutines := 100
	searchesPerGoroutine := 1000
	var totalSearches atomic.Int64
	var failedSearches atomic.Int64

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for s := 0; s < searchesPerGoroutine; s++ {
				keyIdx := s % numEntries
				key := types.NewIntField(int64(keyIdx))
				results, err := bt.Search(key)

				totalSearches.Add(1)

				if err != nil {
					failedSearches.Add(1)
					t.Errorf("Goroutine %d: Failed to search for key %d: %v", goroutineID, keyIdx, err)
					return
				}

				if len(results) != 1 {
					failedSearches.Add(1)
					t.Errorf("Goroutine %d: Expected 1 result for key %d, got %d", goroutineID, keyIdx, len(results))
				}
			}
		}(g)
	}

	wg.Wait()

	t.Logf("Completed %d concurrent searches with %d failures", totalSearches.Load(), failedSearches.Load())

	if failedSearches.Load() > 0 {
		t.Errorf("Had %d failed searches out of %d total", failedSearches.Load(), totalSearches.Load())
	}
}

// Test: Concurrent range searches (50 goroutines, multiple range queries each)
func TestBTree_Concurrent_RangeSearches(t *testing.T) {
	bt, _, _, _, cleanup := setupTestBTree(t, types.IntType)
	defer cleanup()

	numEntries := 100
	pageID := page.NewPageDescriptor(1, 0)

	// Insert entries
	for i := 0; i < numEntries; i++ {
		key := types.NewIntField(int64(i))
		rid := tuple.NewTupleRecordID(pageID, primitives.SlotID(i))
		err := bt.Insert(key, rid)
		if err != nil {
			t.Fatalf("Failed to insert entry %d: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	numGoroutines := 50
	rangeSearchesPerGoroutine := 100

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for r := 0; r < rangeSearchesPerGoroutine; r++ {
				startIdx := r % (numEntries - 20)
				endIdx := startIdx + 20

				startKey := types.NewIntField(int64(startIdx))
				endKey := types.NewIntField(int64(endIdx))

				results, err := bt.RangeSearch(startKey, endKey)
				if err != nil {
					t.Errorf("Goroutine %d: Failed range search [%d, %d]: %v", goroutineID, startIdx, endIdx, err)
					return
				}

				expectedCount := endIdx - startIdx + 1
				if len(results) != expectedCount {
					t.Errorf("Goroutine %d: Range [%d, %d] expected %d results, got %d",
						goroutineID, startIdx, endIdx, expectedCount, len(results))
				}
			}
		}(g)
	}

	wg.Wait()
}

// Test: Mixed concurrent reads and writes (inserts only, no structure changes)
func TestBTree_Concurrent_MixedReadsAndInserts(t *testing.T) {
	bt, _, _, _, cleanup := setupTestBTree(t, types.IntType)
	defer cleanup()

	// Pre-populate with some entries
	numInitialEntries := 50
	pageID := page.NewPageDescriptor(1, 0)

	for i := 0; i < numInitialEntries; i++ {
		key := types.NewIntField(int64(i))
		rid := tuple.NewTupleRecordID(pageID, primitives.SlotID(i))
		err := bt.Insert(key, rid)
		if err != nil {
			t.Fatalf("Failed to insert initial entry %d: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	numReaders := 20
	numWriters := 5
	readsPerGoroutine := 500
	writesPerGoroutine := 20

	// Start readers
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()

			for i := 0; i < readsPerGoroutine; i++ {
				keyIdx := i % numInitialEntries
				key := types.NewIntField(int64(keyIdx))
				_, err := bt.Search(key)
				if err != nil {
					t.Errorf("Reader %d: Failed to search for key %d: %v", readerID, keyIdx, err)
					return
				}
			}
		}(r)
	}

	// Start writers (inserting new keys that don't exist yet)
	var nextKey atomic.Int64
	nextKey.Store(int64(numInitialEntries))

	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()

			for i := 0; i < writesPerGoroutine; i++ {
				keyVal := nextKey.Add(1)
				key := types.NewIntField(keyVal)
				rid := tuple.NewTupleRecordID(pageID, primitives.SlotID(keyVal))

				err := bt.Insert(key, rid)
				if err != nil {
					t.Errorf("Writer %d: Failed to insert key %d: %v", writerID, keyVal, err)
					return
				}
			}
		}(w)
	}

	wg.Wait()

	// Verify all initial entries still exist
	for i := 0; i < numInitialEntries; i++ {
		key := types.NewIntField(int64(i))
		results, err := bt.Search(key)
		if err != nil {
			t.Errorf("Final verification: Failed to search for key %d: %v", i, err)
		}
		if len(results) != 1 {
			t.Errorf("Final verification: Expected 1 result for key %d, got %d", i, len(results))
		}
	}
}

// Test: Concurrent point queries vs range queries
func TestBTree_Concurrent_PointVsRangeQueries(t *testing.T) {
	bt, _, _, _, cleanup := setupTestBTree(t, types.IntType)
	defer cleanup()

	numEntries := 100
	pageID := page.NewPageDescriptor(1, 0)

	// Insert entries
	for i := 0; i < numEntries; i++ {
		key := types.NewIntField(int64(i))
		rid := tuple.NewTupleRecordID(pageID, primitives.SlotID(i))
		err := bt.Insert(key, rid)
		if err != nil {
			t.Fatalf("Failed to insert entry %d: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	numPointQueryGoroutines := 25
	numRangeQueryGoroutines := 25
	queriesPerGoroutine := 200

	// Point query goroutines
	for g := 0; g < numPointQueryGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for q := 0; q < queriesPerGoroutine; q++ {
				keyIdx := q % numEntries
				key := types.NewIntField(int64(keyIdx))
				results, err := bt.Search(key)
				if err != nil {
					t.Errorf("Point query goroutine %d: Failed for key %d: %v", goroutineID, keyIdx, err)
					return
				}
				if len(results) != 1 {
					t.Errorf("Point query goroutine %d: Expected 1 result for key %d, got %d",
						goroutineID, keyIdx, len(results))
				}
			}
		}(g)
	}

	// Range query goroutines
	for g := 0; g < numRangeQueryGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for q := 0; q < queriesPerGoroutine; q++ {
				startIdx := q % (numEntries - 10)
				endIdx := startIdx + 10

				startKey := types.NewIntField(int64(startIdx))
				endKey := types.NewIntField(int64(endIdx))

				results, err := bt.RangeSearch(startKey, endKey)
				if err != nil {
					t.Errorf("Range query goroutine %d: Failed for range [%d, %d]: %v",
						goroutineID, startIdx, endIdx, err)
					return
				}

				expectedCount := endIdx - startIdx + 1
				if len(results) != expectedCount {
					t.Errorf("Range query goroutine %d: Range [%d, %d] expected %d results, got %d",
						goroutineID, startIdx, endIdx, expectedCount, len(results))
				}
			}
		}(g)
	}

	wg.Wait()
}

// Test: Concurrent searches with varying key distributions
func TestBTree_Concurrent_VariedDistribution(t *testing.T) {
	bt, _, _, _, cleanup := setupTestBTree(t, types.IntType)
	defer cleanup()

	numEntries := 100
	pageID := page.NewPageDescriptor(1, 0)

	// Insert entries
	for i := 0; i < numEntries; i++ {
		key := types.NewIntField(int64(i))
		rid := tuple.NewTupleRecordID(pageID, primitives.SlotID(i))
		err := bt.Insert(key, rid)
		if err != nil {
			t.Fatalf("Failed to insert entry %d: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	var errors atomic.Int64

	// Goroutines searching for sequential keys
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			key := types.NewIntField(int64(i % numEntries))
			_, err := bt.Search(key)
			if err != nil {
				errors.Add(1)
			}
		}
	}()

	// Goroutines searching for hot keys (frequently accessed)
	for g := 0; g < 20; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			hotKeys := []int64{10, 25, 50, 75, 90}
			for i := 0; i < 500; i++ {
				key := types.NewIntField(hotKeys[i%len(hotKeys)])
				_, err := bt.Search(key)
				if err != nil {
					errors.Add(1)
				}
			}
		}()
	}

	// Goroutines searching for random keys
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				// Pseudo-random using goroutine ID and iteration
				keyIdx := (id*997 + i*13) % numEntries
				key := types.NewIntField(int64(keyIdx))
				_, err := bt.Search(key)
				if err != nil {
					errors.Add(1)
				}
			}
		}(g)
	}

	wg.Wait()

	if errors.Load() > 0 {
		t.Errorf("Had %d errors during concurrent varied distribution searches", errors.Load())
	}
}

// Test: Stress test with massive concurrent operations
func TestBTree_Stress_MassiveConcurrency(t *testing.T) {
	bt, _, _, _, cleanup := setupTestBTree(t, types.IntType)
	defer cleanup()

	// Pre-populate
	numInitialEntries := 100
	pageID := page.NewPageDescriptor(1, 0)

	for i := 0; i < numInitialEntries; i++ {
		key := types.NewIntField(int64(i))
		rid := tuple.NewTupleRecordID(pageID, primitives.SlotID(i))
		err := bt.Insert(key, rid)
		if err != nil {
			t.Fatalf("Failed to insert initial entry %d: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	var searchCount, rangeSearchCount atomic.Int64
	var searchErrors, rangeSearchErrors atomic.Int64

	// 100 concurrent point query goroutines
	for g := 0; g < 100; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for i := 0; i < 1000; i++ {
				keyIdx := (id*11 + i*7) % numInitialEntries
				key := types.NewIntField(int64(keyIdx))
				_, err := bt.Search(key)

				searchCount.Add(1)
				if err != nil {
					searchErrors.Add(1)
				}
			}
		}(g)
	}

	// 50 concurrent range query goroutines
	for g := 0; g < 50; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for i := 0; i < 200; i++ {
				startIdx := (id*5 + i*3) % (numInitialEntries - 20)
				endIdx := startIdx + 20

				startKey := types.NewIntField(int64(startIdx))
				endKey := types.NewIntField(int64(endIdx))

				_, err := bt.RangeSearch(startKey, endKey)

				rangeSearchCount.Add(1)
				if err != nil {
					rangeSearchErrors.Add(1)
				}
			}
		}(g)
	}

	wg.Wait()

	t.Logf("Stress test results:")
	t.Logf("  Point searches: %d total, %d errors", searchCount.Load(), searchErrors.Load())
	t.Logf("  Range searches: %d total, %d errors", rangeSearchCount.Load(), rangeSearchErrors.Load())

	totalErrors := searchErrors.Load() + rangeSearchErrors.Load()
	if totalErrors > 0 {
		t.Errorf("Stress test had %d total errors", totalErrors)
	}
}

// Test: Concurrent searches on string keys
func TestBTree_Concurrent_StringKeys(t *testing.T) {
	bt, _, _, _, cleanup := setupTestBTree(t, types.StringType)
	defer cleanup()

	// Insert string entries
	pageID := page.NewPageDescriptor(1, 0)
	testStrings := []string{
		"apple", "banana", "cherry", "date", "elderberry",
		"fig", "grape", "honeydew", "kiwi", "lemon",
		"mango", "nectarine", "orange", "papaya", "quince",
		"raspberry", "strawberry", "tangerine", "ugli", "vanilla",
	}

	for i, str := range testStrings {
		key := types.NewStringField(str, 128)
		rid := tuple.NewTupleRecordID(pageID, primitives.SlotID(i))
		err := bt.Insert(key, rid)
		if err != nil {
			t.Fatalf("Failed to insert string %s: %v", str, err)
		}
	}

	var wg sync.WaitGroup
	var errors atomic.Int64

	// Multiple goroutines searching for strings
	for g := 0; g < 30; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for i := 0; i < 200; i++ {
				strIdx := (id + i) % len(testStrings)
				key := types.NewStringField(testStrings[strIdx], 128)
				results, err := bt.Search(key)

				if err != nil {
					errors.Add(1)
					t.Errorf("Goroutine %d: Failed to search for %s: %v", id, testStrings[strIdx], err)
					return
				}

				if len(results) != 1 {
					errors.Add(1)
					t.Errorf("Goroutine %d: Expected 1 result for %s, got %d", id, testStrings[strIdx], len(results))
				}
			}
		}(g)
	}

	wg.Wait()

	if errors.Load() > 0 {
		t.Errorf("Had %d errors during concurrent string key searches", errors.Load())
	}
}

// Test: Concurrent duplicate search attempts
func TestBTree_Concurrent_DuplicateSearches(t *testing.T) {
	bt, _, _, _, cleanup := setupTestBTree(t, types.IntType)
	defer cleanup()

	// Insert one entry
	key := types.NewIntField(42)
	pageID := page.NewPageDescriptor(1, 0)
	rid := tuple.NewTupleRecordID(pageID, 0)

	err := bt.Insert(key, rid)
	if err != nil {
		t.Fatalf("Failed to insert entry: %v", err)
	}

	// Have many goroutines search for the same key concurrently
	var wg sync.WaitGroup
	numGoroutines := 100
	searchesPerGoroutine := 100

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for i := 0; i < searchesPerGoroutine; i++ {
				results, err := bt.Search(key)
				if err != nil {
					t.Errorf("Goroutine %d: Failed to search: %v", id, err)
					return
				}

				if len(results) != 1 {
					t.Errorf("Goroutine %d: Expected 1 result, got %d", id, len(results))
				}

				if len(results) > 0 && !results[0].Equals(rid) {
					t.Errorf("Goroutine %d: Got wrong RID", id)
				}
			}
		}(g)
	}

	wg.Wait()
}

// Benchmark: Concurrent read performance
func BenchmarkBTree_ConcurrentReads(b *testing.B) {
	bt, _, _, _, cleanup := setupTestBTree(&testing.T{}, types.IntType)
	defer cleanup()

	numEntries := 1000
	pageID := page.NewPageDescriptor(1, 0)

	for i := 0; i < numEntries; i++ {
		key := types.NewIntField(int64(i))
		rid := tuple.NewTupleRecordID(pageID, primitives.SlotID(i))
		bt.Insert(key, rid)
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			keyIdx := i % numEntries
			key := types.NewIntField(int64(keyIdx))
			bt.Search(key)
			i++
		}
	})
}

// Benchmark: Concurrent range search performance
func BenchmarkBTree_ConcurrentRangeSearches(b *testing.B) {
	bt, _, _, _, cleanup := setupTestBTree(&testing.T{}, types.IntType)
	defer cleanup()

	numEntries := 1000
	pageID := page.NewPageDescriptor(1, 0)

	for i := 0; i < numEntries; i++ {
		key := types.NewIntField(int64(i))
		rid := tuple.NewTupleRecordID(pageID, primitives.SlotID(i))
		bt.Insert(key, rid)
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {

		i := 0
		for pb.Next() {
			startIdx := i % (numEntries - 100)
			endIdx := startIdx + 100

			startKey := types.NewIntField(int64(startIdx))
			endKey := types.NewIntField(int64(endIdx))
			bt.RangeSearch(startKey, endKey)
			i++
		}
	})
}
