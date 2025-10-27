package query

import (
	"path/filepath"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/log/wal"
	"storemy/pkg/memory"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/heap"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"sync"
	"testing"
	"time"
)

// testSetup holds common test resources
type testSetup struct {
	heapFile *heap.HeapFile
	wal      *wal.WAL
	store    *memory.PageStore
	tx       *transaction.TransactionContext
	td       *tuple.TupleDescription
}

// setupSeqScanTest creates common test resources
func setupSeqScanTest(t *testing.T, fields []types.Type, fieldNames []string) *testSetup {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "seqscan_test.dat")

	td, err := tuple.NewTupleDesc(fields, fieldNames)
	if err != nil {
		t.Fatalf("Failed to create tuple description: %v", err)
	}

	heapFile, err := heap.NewHeapFile(primitives.Filepath(filePath), td)
	if err != nil {
		t.Fatalf("Failed to create heap file: %v", err)
	}

	walPath := filepath.Join(tempDir, "test.wal")
	wal, err := wal.NewWAL(walPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	store := memory.NewPageStore(wal)
	tx := transaction.NewTransactionContext(primitives.NewTransactionID())

	return &testSetup{
		heapFile: heapFile,
		wal:      wal,
		store:    store,
		tx:       tx,
		td:       td,
	}
}

// cleanup closes all resources
func (s *testSetup) cleanup() {
	if s.store != nil {
		s.store.Close()
	}
	if s.wal != nil {
		s.wal.Close()
	}
	if s.heapFile != nil {
		s.heapFile.Close()
	}
}

// insertTuples inserts tuples into the heap file
func (s *testSetup) insertTuples(t *testing.T, numTuples int, tupleFactory func(int, *tuple.TupleDescription) *tuple.Tuple) {
	for i := 0; i < numTuples; i++ {
		tup := tupleFactory(i, s.td)
		pageNum := i / 10
		pageID := page.NewPageDescriptor(s.heapFile.GetID(), primitives.PageNumber(pageNum))

		pg, err := s.store.GetPage(s.tx, s.heapFile, pageID, transaction.ReadWrite)
		if err != nil {
			t.Fatalf("Failed to get page: %v", err)
		}

		heapPage := pg.(*heap.HeapPage)
		err = heapPage.AddTuple(tup)
		if err != nil {
			t.Fatalf("Failed to add tuple %d: %v", i, err)
		}

		err = s.heapFile.WritePage(heapPage)
		if err != nil {
			t.Fatalf("Failed to write page: %v", err)
		}
	}
}

// TestSeqScanPrefetching verifies that prefetching is working correctly
func TestSeqScanPrefetching(t *testing.T) {
	setup := setupSeqScanTest(t, []types.Type{types.IntType, types.StringType}, []string{"id", "name"})
	defer setup.cleanup()

	totalTuples := 100
	setup.insertTuples(t, totalTuples, func(i int, td *tuple.TupleDescription) *tuple.Tuple {
		tup := tuple.NewTuple(td)
		tup.SetField(0, types.NewIntField(int64(i)))
		tup.SetField(1, types.NewStringField("tuple", 128))
		return tup
	})

	seqScan, err := NewSeqScan(setup.tx, setup.heapFile.GetID(), setup.heapFile, setup.store)
	if err != nil {
		t.Fatalf("Failed to create sequential scan: %v", err)
	}

	if !seqScan.prefetchEnabled {
		t.Error("Expected prefetching to be enabled by default")
	}

	err = seqScan.Open()
	if err != nil {
		t.Fatalf("Failed to open sequential scan: %v", err)
	}
	defer seqScan.Close()

	count := 0
	for {
		hasNext, err := seqScan.HasNext()
		if err != nil {
			t.Fatalf("HasNext failed: %v", err)
		}
		if !hasNext {
			break
		}

		tup, err := seqScan.Next()
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}

		if tup == nil {
			break
		}

		count++
	}

	if count != totalTuples {
		t.Errorf("Expected %d tuples, got %d", totalTuples, count)
	}

	select {
	case <-seqScan.prefetchDone:
		// Channel should be closed or ready
	case <-time.After(100 * time.Millisecond):
		t.Error("prefetchDone channel is blocking, possible goroutine leak")
	}
}

// TestSeqScanPrefetchingRewind tests that prefetching works correctly with Rewind
func TestSeqScanPrefetchingRewind(t *testing.T) {
	setup := setupSeqScanTest(t, []types.Type{types.IntType}, []string{"id"})
	defer setup.cleanup()

	numTuples := 50
	setup.insertTuples(t, numTuples, func(i int, td *tuple.TupleDescription) *tuple.Tuple {
		tup := tuple.NewTuple(td)
		tup.SetField(0, types.NewIntField(int64(i)))
		return tup
	})

	seqScan, err := NewSeqScan(setup.tx, setup.heapFile.GetID(), setup.heapFile, setup.store)
	if err != nil {
		t.Fatalf("Failed to create sequential scan: %v", err)
	}

	err = seqScan.Open()
	if err != nil {
		t.Fatalf("Failed to open sequential scan: %v", err)
	}
	defer seqScan.Close()

	count1 := 0
	for {
		hasNext, _ := seqScan.HasNext()
		if !hasNext {
			break
		}
		_, err := seqScan.Next()
		if err != nil {
			t.Fatalf("Next failed in first iteration: %v", err)
		}
		count1++
	}

	err = seqScan.Rewind()
	if err != nil {
		t.Fatalf("Rewind failed: %v", err)
	}

	select {
	case <-seqScan.prefetchDone:
		// Channel should be ready immediately after rewind
	case <-time.After(100 * time.Millisecond):
		t.Error("prefetchDone channel is blocking after rewind")
	}

	count2 := 0
	for {
		hasNext, _ := seqScan.HasNext()
		if !hasNext {
			break
		}
		_, err := seqScan.Next()
		if err != nil {
			t.Fatalf("Next failed in second iteration: %v", err)
		}
		count2++
	}

	if count1 != count2 {
		t.Errorf("Expected same number of tuples in both iterations: first=%d, second=%d", count1, count2)
	}

	if count1 != numTuples {
		t.Errorf("Expected %d tuples, got %d", numTuples, count1)
	}
}

// TestSeqScanPrefetchingClose tests that Close properly waits for pending prefetch
func TestSeqScanPrefetchingClose(t *testing.T) {
	setup := setupSeqScanTest(t, []types.Type{types.IntType}, []string{"id"})
	defer setup.cleanup()

	numTuples := 100
	setup.insertTuples(t, numTuples, func(i int, td *tuple.TupleDescription) *tuple.Tuple {
		tup := tuple.NewTuple(td)
		tup.SetField(0, types.NewIntField(int64(i)))
		return tup
	})

	seqScan, err := NewSeqScan(setup.tx, setup.heapFile.GetID(), setup.heapFile, setup.store)
	if err != nil {
		t.Fatalf("Failed to create sequential scan: %v", err)
	}

	err = seqScan.Open()
	if err != nil {
		t.Fatalf("Failed to open sequential scan: %v", err)
	}

	for i := 0; i < 10; i++ {
		hasNext, _ := seqScan.HasNext()
		if !hasNext {
			break
		}
		seqScan.Next()
	}

	done := make(chan struct{})
	go func() {
		seqScan.Close()
		close(done)
	}()

	select {
	case <-done:
		// Success - Close completed
	case <-time.After(5 * time.Second):
		t.Error("Close() did not complete within timeout, possible goroutine leak")
	}
}

// TestSeqScanPrefetchingConcurrency tests concurrent access patterns
func TestSeqScanPrefetchingConcurrency(t *testing.T) {
	setup := setupSeqScanTest(t, []types.Type{types.IntType, types.StringType}, []string{"id", "value"})
	defer setup.cleanup()

	numTuples := 200
	setup.insertTuples(t, numTuples, func(i int, td *tuple.TupleDescription) *tuple.Tuple {
		tup := tuple.NewTuple(td)
		tup.SetField(0, types.NewIntField(int64(i)))
		tup.SetField(1, types.NewStringField("value", 128))
		return tup
	})

	numScans := 5
	var wg sync.WaitGroup
	errors := make(chan error, numScans)

	for i := 0; i < numScans; i++ {
		wg.Add(1)
		go func(scanID int) {
			defer wg.Done()

			tx := transaction.NewTransactionContext(primitives.NewTransactionID())

			seqScan, err := NewSeqScan(tx, setup.heapFile.GetID(), setup.heapFile, setup.store)
			if err != nil {
				errors <- err
				return
			}

			err = seqScan.Open()
			if err != nil {
				errors <- err
				return
			}
			defer seqScan.Close()

			count := 0
			for {
				hasNext, err := seqScan.HasNext()
				if err != nil {
					errors <- err
					return
				}
				if !hasNext {
					break
				}

				_, err = seqScan.Next()
				if err != nil {
					errors <- err
					return
				}
				count++
			}

			if count != numTuples {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		if err != nil {
			t.Errorf("Concurrent scan error: %v", err)
		}
	}
}
