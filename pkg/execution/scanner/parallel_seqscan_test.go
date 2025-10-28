package scanner

import (
	"fmt"
	"sort"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/execution/query"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

// TestParallelSeqScanBasic tests basic functionality
func TestParallelSeqScanBasic(t *testing.T) {
	setup := setupSeqScanTest(t, []types.Type{types.IntType, types.StringType}, []string{"id", "name"})
	defer setup.cleanup()

	numTuples := 100
	setup.insertTuples(t, numTuples, func(i int, td *tuple.TupleDescription) *tuple.Tuple {
		tup := tuple.NewTuple(td)
		tup.SetField(0, types.NewIntField(int64(i)))
		tup.SetField(1, types.NewStringField("tuple", 128))
		return tup
	})

	config := DefaultParallelConfig()
	parallelScan, err := NewParallelSeqScan(
		setup.tx,
		setup.heapFile.GetID(),
		setup.heapFile,
		setup.store,
		config,
	)
	if err != nil {
		t.Fatalf("Failed to create parallel scan: %v", err)
	}

	err = parallelScan.Open()
	if err != nil {
		t.Fatalf("Failed to open parallel scan: %v", err)
	}
	defer parallelScan.Close()

	count := 0
	for {
		hasNext, err := parallelScan.HasNext()
		if err != nil {
			t.Fatalf("HasNext failed: %v", err)
		}
		if !hasNext {
			break
		}

		tup, err := parallelScan.Next()
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}

		if tup == nil {
			break
		}

		count++
	}

	if count != numTuples {
		t.Errorf("Expected %d tuples, got %d", numTuples, count)
	}
}

// TestParallelSeqScanMultipleWorkers tests with different worker counts
func TestParallelSeqScanMultipleWorkers(t *testing.T) {
	setup := setupSeqScanTest(t, []types.Type{types.IntType}, []string{"id"})
	defer setup.cleanup()

	numTuples := 200
	setup.insertTuples(t, numTuples, func(i int, td *tuple.TupleDescription) *tuple.Tuple {
		tup := tuple.NewTuple(td)
		tup.SetField(0, types.NewIntField(int64(i)))
		return tup
	})

	workerCounts := []int{1, 2, 4, 8}

	for _, workers := range workerCounts {
		t.Run(fmt.Sprintf("workers_%d", workers), func(t *testing.T) {
			config := ParallelSeqScanConfig{
				NumWorkers:     workers,
				ResultChanSize: 100,
			}

			tx := transaction.NewTransactionContext(primitives.NewTransactionID())
			parallelScan, err := NewParallelSeqScan(
				tx,
				setup.heapFile.GetID(),
				setup.heapFile,
				setup.store,
				config,
			)
			if err != nil {
				t.Fatalf("Failed to create parallel scan: %v", err)
			}

			err = parallelScan.Open()
			if err != nil {
				t.Fatalf("Failed to open parallel scan: %v", err)
			}
			defer parallelScan.Close()

			count := 0
			for {
				hasNext, err := parallelScan.HasNext()
				if err != nil {
					t.Fatalf("HasNext failed: %v", err)
				}
				if !hasNext {
					break
				}

				_, err = parallelScan.Next()
				if err != nil {
					t.Fatalf("Next failed: %v", err)
				}
				count++
			}

			if count != numTuples {
				t.Errorf("Workers=%d: Expected %d tuples, got %d", workers, numTuples, count)
			}
		})
	}
}

// TestParallelSeqScanCorrectness verifies all tuples are returned correctly
func TestParallelSeqScanCorrectness(t *testing.T) {
	setup := setupSeqScanTest(t, []types.Type{types.IntType}, []string{"id"})
	defer setup.cleanup()

	numTuples := 100
	expectedIDs := make([]int, numTuples)

	setup.insertTuples(t, numTuples, func(i int, td *tuple.TupleDescription) *tuple.Tuple {
		tup := tuple.NewTuple(td)
		tup.SetField(0, types.NewIntField(int64(i)))
		expectedIDs[i] = i
		return tup
	})

	config := ParallelSeqScanConfig{NumWorkers: 4, ResultChanSize: 100}

	parallelScan, err := NewParallelSeqScan(
		setup.tx,
		setup.heapFile.GetID(),
		setup.heapFile,
		setup.store,
		config,
	)
	if err != nil {
		t.Fatalf("Failed to create parallel scan: %v", err)
	}

	err = parallelScan.Open()
	if err != nil {
		t.Fatalf("Failed to open parallel scan: %v", err)
	}
	defer parallelScan.Close()

	// Collect all IDs
	receivedIDs := make([]int, 0, numTuples)
	for {
		hasNext, err := parallelScan.HasNext()
		if err != nil {
			t.Fatalf("HasNext failed: %v", err)
		}
		if !hasNext {
			break
		}

		tup, err := parallelScan.Next()
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}

		idField, err := tup.GetField(0)
		if err != nil {
			t.Fatalf("Failed to get field: %v", err)
		}

		id := idField.(*types.IntField).Value
		receivedIDs = append(receivedIDs, int(id))
	}

	// Sort both slices and compare
	sort.Ints(expectedIDs)
	sort.Ints(receivedIDs)

	if len(receivedIDs) != len(expectedIDs) {
		t.Errorf("Expected %d tuples, got %d", len(expectedIDs), len(receivedIDs))
	}

	for i := range expectedIDs {
		if i >= len(receivedIDs) {
			break
		}
		if expectedIDs[i] != receivedIDs[i] {
			t.Errorf("ID mismatch at position %d: expected %d, got %d",
				i, expectedIDs[i], receivedIDs[i])
		}
	}
}

// TestParallelSeqScanRewindNotSupported verifies that Rewind returns an error
func TestParallelSeqScanRewindNotSupported(t *testing.T) {
	setup := setupSeqScanTest(t, []types.Type{types.IntType}, []string{"id"})
	defer setup.cleanup()

	numTuples := 50
	setup.insertTuples(t, numTuples, func(i int, td *tuple.TupleDescription) *tuple.Tuple {
		tup := tuple.NewTuple(td)
		tup.SetField(0, types.NewIntField(int64(i)))
		return tup
	})

	config := DefaultParallelConfig()
	parallelScan, err := NewParallelSeqScan(
		setup.tx,
		setup.heapFile.GetID(),
		setup.heapFile,
		setup.store,
		config,
	)
	if err != nil {
		t.Fatalf("Failed to create parallel scan: %v", err)
	}

	err = parallelScan.Open()
	if err != nil {
		t.Fatalf("Failed to open parallel scan: %v", err)
	}
	defer parallelScan.Close()

	// Try to rewind
	err = parallelScan.Rewind()
	if err == nil {
		t.Error("Expected Rewind to return an error, but it succeeded")
	}
}

// TestParallelSeqScanEmpty tests scanning an empty table
func TestParallelSeqScanEmpty(t *testing.T) {
	setup := setupSeqScanTest(t, []types.Type{types.IntType}, []string{"id"})
	defer setup.cleanup()

	// Don't insert any tuples

	config := DefaultParallelConfig()
	parallelScan, err := NewParallelSeqScan(
		setup.tx,
		setup.heapFile.GetID(),
		setup.heapFile,
		setup.store,
		config,
	)
	if err != nil {
		t.Fatalf("Failed to create parallel scan: %v", err)
	}

	err = parallelScan.Open()
	if err != nil {
		t.Fatalf("Failed to open parallel scan: %v", err)
	}
	defer parallelScan.Close()

	hasNext, err := parallelScan.HasNext()
	if err != nil {
		t.Fatalf("HasNext failed: %v", err)
	}

	if hasNext {
		t.Error("Expected no tuples in empty table")
	}
}

// TestParallelSeqScanWithFilter tests parallel scan with a filter operator
func TestParallelSeqScanWithFilter(t *testing.T) {
	setup := setupSeqScanTest(t, []types.Type{types.IntType}, []string{"id"})
	defer setup.cleanup()

	numTuples := 100
	setup.insertTuples(t, numTuples, func(i int, td *tuple.TupleDescription) *tuple.Tuple {
		tup := tuple.NewTuple(td)
		tup.SetField(0, types.NewIntField(int64(i)))
		return tup
	})

	config := DefaultParallelConfig()
	parallelScan, err := NewParallelSeqScan(
		setup.tx,
		setup.heapFile.GetID(),
		setup.heapFile,
		setup.store,
		config,
	)
	if err != nil {
		t.Fatalf("Failed to create parallel scan: %v", err)
	}

	// Filter: id > 50
	pred := query.NewPredicate(0, primitives.GreaterThan, types.NewIntField(50))

	filter, err := query.NewFilter(pred, parallelScan)
	if err != nil {
		t.Fatalf("Failed to create filter: %v", err)
	}

	err = filter.Open()
	if err != nil {
		t.Fatalf("Failed to open filter: %v", err)
	}
	defer filter.Close()

	count := 0
	for {
		hasNext, err := filter.HasNext()
		if err != nil {
			t.Fatalf("HasNext failed: %v", err)
		}
		if !hasNext {
			break
		}

		tup, err := filter.Next()
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}

		idField, _ := tup.GetField(0)
		id := idField.(*types.IntField).Value
		if id <= 50 {
			t.Errorf("Filter failed: got id=%d, expected > 50", id)
		}

		count++
	}

	expectedCount := 49 // IDs 51-99
	if count != expectedCount {
		t.Errorf("Expected %d filtered tuples, got %d", expectedCount, count)
	}
}

// TestParallelSeqScanInvalidConfig tests error handling for invalid configurations
func TestParallelSeqScanInvalidConfig(t *testing.T) {
	setup := setupSeqScanTest(t, []types.Type{types.IntType}, []string{"id"})
	defer setup.cleanup()

	tests := []struct {
		name   string
		config ParallelSeqScanConfig
	}{
		{
			name:   "zero_workers",
			config: ParallelSeqScanConfig{NumWorkers: 0, ResultChanSize: 100},
		},
		{
			name:   "negative_workers",
			config: ParallelSeqScanConfig{NumWorkers: -1, ResultChanSize: 100},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewParallelSeqScan(
				setup.tx,
				setup.heapFile.GetID(),
				setup.heapFile,
				setup.store,
				tt.config,
			)
			if err == nil {
				t.Error("Expected error for invalid config, got nil")
			}
		})
	}
}
