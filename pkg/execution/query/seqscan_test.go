package query

import (
	"path/filepath"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/log"
	"storemy/pkg/memory"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/heap"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"sync"
	"testing"
	"time"
)

// TestSeqScanPrefetching verifies that prefetching is working correctly
func TestSeqScanPrefetching(t *testing.T) {
	// Setup: Create a temporary database file with multiple pages
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "seqscan_test.dat")

	// Create tuple description
	td, err := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType},
		[]string{"id", "name"},
	)
	if err != nil {
		t.Fatalf("Failed to create tuple description: %v", err)
	}

	// Create heap file
	heapFile, err := heap.NewHeapFile(filePath, td)
	if err != nil {
		t.Fatalf("Failed to create heap file: %v", err)
	}
	defer heapFile.Close()

	// Create WAL and page store
	walPath := filepath.Join(tempDir, "test.wal")
	wal, err := log.NewWAL(walPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	store := memory.NewPageStore(wal)
	defer store.Close()

	// Create transaction
	tx := transaction.NewTransactionContext(primitives.NewTransactionID())

	// Insert multiple pages of data
	totalTuples := 100

	for i := 0; i < totalTuples; i++ {
		tup := tuple.NewTuple(td)
		tup.SetField(0, types.NewIntField(int64(i)))
		tup.SetField(1, types.NewStringField("tuple", 128))

		// Manually add tuples to pages
		pageNum := i / 10
		pageID := heap.NewHeapPageID(heapFile.GetID(), pageNum)

		pg, err := store.GetPage(tx, heapFile, pageID, transaction.ReadWrite)
		if err != nil {
			t.Fatalf("Failed to get page: %v", err)
		}

		heapPage := pg.(*heap.HeapPage)
		err = heapPage.AddTuple(tup)
		if err != nil {
			t.Fatalf("Failed to add tuple %d: %v", i, err)
		}

		// Write the page back
		err = heapFile.WritePage(heapPage)
		if err != nil {
			t.Fatalf("Failed to write page: %v", err)
		}
	}

	// Create sequential scan
	seqScan, err := NewSeqScan(tx, heapFile.GetID(), heapFile, store)
	if err != nil {
		t.Fatalf("Failed to create sequential scan: %v", err)
	}

	// Verify prefetching is enabled
	if !seqScan.prefetchEnabled {
		t.Error("Expected prefetching to be enabled by default")
	}

	// Open the scan
	err = seqScan.Open()
	if err != nil {
		t.Fatalf("Failed to open sequential scan: %v", err)
	}
	defer seqScan.Close()

	// Read all tuples and verify they are returned correctly
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

	// Verify we read all tuples
	if count != totalTuples {
		t.Errorf("Expected %d tuples, got %d", totalTuples, count)
	}

	// Verify the prefetch channel is properly managed
	select {
	case <-seqScan.prefetchDone:
		// Channel should be closed or ready
	case <-time.After(100 * time.Millisecond):
		t.Error("prefetchDone channel is blocking, possible goroutine leak")
	}
}

// TestSeqScanPrefetchingRewind tests that prefetching works correctly with Rewind
func TestSeqScanPrefetchingRewind(t *testing.T) {
	// Setup: Create a temporary database file
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "seqscan_rewind_test.dat")

	// Create tuple description
	td, err := tuple.NewTupleDesc(
		[]types.Type{types.IntType},
		[]string{"id"},
	)
	if err != nil {
		t.Fatalf("Failed to create tuple description: %v", err)
	}

	// Create heap file
	heapFile, err := heap.NewHeapFile(filePath, td)
	if err != nil {
		t.Fatalf("Failed to create heap file: %v", err)
	}
	defer heapFile.Close()

	// Create WAL and page store
	walPath := filepath.Join(tempDir, "test.wal")
	wal, err := log.NewWAL(walPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	store := memory.NewPageStore(wal)
	defer store.Close()

	// Create transaction
	tx := transaction.NewTransactionContext(primitives.NewTransactionID())

	// Insert some tuples
	numTuples := 50
	for i := 0; i < numTuples; i++ {
		tup := tuple.NewTuple(td)
		tup.SetField(0, types.NewIntField(int64(i)))

		pageNum := i / 10
		pageID := heap.NewHeapPageID(heapFile.GetID(), pageNum)

		pg, err := store.GetPage(tx, heapFile, pageID, transaction.ReadWrite)
		if err != nil {
			t.Fatalf("Failed to get page: %v", err)
		}

		heapPage := pg.(*heap.HeapPage)
		err = heapPage.AddTuple(tup)
		if err != nil {
			t.Fatalf("Failed to add tuple %d: %v", i, err)
		}

		err = heapFile.WritePage(heapPage)
		if err != nil {
			t.Fatalf("Failed to write page: %v", err)
		}
	}

	// Create sequential scan
	seqScan, err := NewSeqScan(tx, heapFile.GetID(), heapFile, store)
	if err != nil {
		t.Fatalf("Failed to create sequential scan: %v", err)
	}

	err = seqScan.Open()
	if err != nil {
		t.Fatalf("Failed to open sequential scan: %v", err)
	}
	defer seqScan.Close()

	// First iteration
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

	// Rewind
	err = seqScan.Rewind()
	if err != nil {
		t.Fatalf("Rewind failed: %v", err)
	}

	// Verify prefetch channel is reset after rewind
	select {
	case <-seqScan.prefetchDone:
		// Channel should be ready immediately after rewind
	case <-time.After(100 * time.Millisecond):
		t.Error("prefetchDone channel is blocking after rewind")
	}

	// Second iteration
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

	// Both iterations should return the same number of tuples
	if count1 != count2 {
		t.Errorf("Expected same number of tuples in both iterations: first=%d, second=%d", count1, count2)
	}

	if count1 != numTuples {
		t.Errorf("Expected %d tuples, got %d", numTuples, count1)
	}
}

// TestSeqScanPrefetchingClose tests that Close properly waits for pending prefetch
func TestSeqScanPrefetchingClose(t *testing.T) {
	// Setup: Create a temporary database file
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "seqscan_close_test.dat")

	// Create tuple description
	td, err := tuple.NewTupleDesc(
		[]types.Type{types.IntType},
		[]string{"id"},
	)
	if err != nil {
		t.Fatalf("Failed to create tuple description: %v", err)
	}

	// Create heap file
	heapFile, err := heap.NewHeapFile(filePath, td)
	if err != nil {
		t.Fatalf("Failed to create heap file: %v", err)
	}
	defer heapFile.Close()

	// Create WAL and page store
	walPath := filepath.Join(tempDir, "test.wal")
	wal, err := log.NewWAL(walPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	store := memory.NewPageStore(wal)
	defer store.Close()

	// Create transaction
	tx := transaction.NewTransactionContext(primitives.NewTransactionID())

	// Insert some tuples across multiple pages
	numTuples := 100
	for i := 0; i < numTuples; i++ {
		tup := tuple.NewTuple(td)
		tup.SetField(0, types.NewIntField(int64(i)))

		pageNum := i / 10
		pageID := heap.NewHeapPageID(heapFile.GetID(), pageNum)

		pg, err := store.GetPage(tx, heapFile, pageID, transaction.ReadWrite)
		if err != nil {
			t.Fatalf("Failed to get page: %v", err)
		}

		heapPage := pg.(*heap.HeapPage)
		err = heapPage.AddTuple(tup)
		if err != nil {
			t.Fatalf("Failed to add tuple %d: %v", i, err)
		}

		err = heapFile.WritePage(heapPage)
		if err != nil {
			t.Fatalf("Failed to write page: %v", err)
		}
	}

	// Create sequential scan
	seqScan, err := NewSeqScan(tx, heapFile.GetID(), heapFile, store)
	if err != nil {
		t.Fatalf("Failed to create sequential scan: %v", err)
	}

	err = seqScan.Open()
	if err != nil {
		t.Fatalf("Failed to open sequential scan: %v", err)
	}

	// Read a few tuples to trigger prefetching
	for i := 0; i < 10; i++ {
		hasNext, _ := seqScan.HasNext()
		if !hasNext {
			break
		}
		seqScan.Next()
	}

	// Close should wait for any pending prefetch and not hang
	done := make(chan struct{})
	go func() {
		seqScan.Close()
		close(done)
	}()

	// Wait for close with timeout
	select {
	case <-done:
		// Success - Close completed
	case <-time.After(5 * time.Second):
		t.Error("Close() did not complete within timeout, possible goroutine leak")
	}
}

// TestSeqScanPrefetchingConcurrency tests concurrent access patterns
func TestSeqScanPrefetchingConcurrency(t *testing.T) {
	// Setup: Create a temporary database file
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "seqscan_concurrent_test.dat")

	// Create tuple description
	td, err := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType},
		[]string{"id", "value"},
	)
	if err != nil {
		t.Fatalf("Failed to create tuple description: %v", err)
	}

	// Create heap file
	heapFile, err := heap.NewHeapFile(filePath, td)
	if err != nil {
		t.Fatalf("Failed to create heap file: %v", err)
	}
	defer heapFile.Close()

	// Create WAL and page store with larger buffer pool for concurrent access
	walPath := filepath.Join(tempDir, "test.wal")
	wal, err := log.NewWAL(walPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	store := memory.NewPageStore(wal)
	defer store.Close()

	// Create transaction
	tx := transaction.NewTransactionContext(primitives.NewTransactionID())

	// Insert data
	numTuples := 200
	for i := 0; i < numTuples; i++ {
		tup := tuple.NewTuple(td)
		tup.SetField(0, types.NewIntField(int64(i)))
		tup.SetField(1, types.NewStringField("value", 128))

		pageNum := i / 10
		pageID := heap.NewHeapPageID(heapFile.GetID(), pageNum)

		pg, err := store.GetPage(tx, heapFile, pageID, transaction.ReadWrite)
		if err != nil {
			t.Fatalf("Failed to get page: %v", err)
		}

		heapPage := pg.(*heap.HeapPage)
		err = heapPage.AddTuple(tup)
		if err != nil {
			t.Fatalf("Failed to add tuple %d: %v", i, err)
		}

		err = heapFile.WritePage(heapPage)
		if err != nil {
			t.Fatalf("Failed to write page: %v", err)
		}
	}

	// Create multiple sequential scans and run them concurrently
	numScans := 5
	var wg sync.WaitGroup
	errors := make(chan error, numScans)

	for i := 0; i < numScans; i++ {
		wg.Add(1)
		go func(scanID int) {
			defer wg.Done()

			// Each goroutine gets its own transaction
			tx := transaction.NewTransactionContext(primitives.NewTransactionID())

			seqScan, err := NewSeqScan(tx, heapFile.GetID(), heapFile, store)
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

	// Wait for all scans to complete before cleaning up
	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		if err != nil {
			t.Errorf("Concurrent scan error: %v", err)
		}
	}

	// Cleanup happens via defer statements
}
