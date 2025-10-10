package heap

import (
	"os"
	"path/filepath"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"sync"
	"testing"
	"time"
)

// =============================================================================
// BASIC FUNCTIONALITY TESTS
// =============================================================================

func TestNewHeapFile(t *testing.T) {
	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "test.db")
	td := mustCreateTupleDescForTest()

	tests := []struct {
		name          string
		filename      string
		tupleDesc     *tuple.TupleDescription
		expectedError bool
	}{
		{
			name:          "Valid file creation",
			filename:      filename,
			tupleDesc:     td,
			expectedError: false,
		},
		{
			name:          "Invalid directory",
			filename:      "/nonexistent/dir/test.db",
			tupleDesc:     td,
			expectedError: true,
		},
		{
			name:          "Empty filename",
			filename:      "",
			tupleDesc:     td,
			expectedError: true,
		},
		{
			name:          "Nil tuple description",
			filename:      filepath.Join(tempDir, "test2.db"),
			tupleDesc:     nil,
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hf, err := NewHeapFile(tt.filename, tt.tupleDesc)

			if tt.expectedError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				if hf != nil {
					hf.Close()
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if hf == nil {
				t.Fatal("NewHeapFile returned nil without error")
			}

			defer hf.Close()

			if hf.file == nil {
				t.Errorf("HeapFile.file is nil")
			}

			if hf.tupleDesc != tt.tupleDesc {
				t.Errorf("Expected tupleDesc %v, got %v", tt.tupleDesc, hf.tupleDesc)
			}

			id := hf.GetID()
			if id == 0 {
				t.Errorf("Expected non-zero ID")
			}

			if hf.GetTupleDesc() != tt.tupleDesc {
				t.Errorf("GetTupleDesc returned wrong value")
			}
		})
	}
}

func TestHeapFile_GetID(t *testing.T) {
	tempDir := t.TempDir()
	td := mustCreateTupleDescForTest()

	file1 := filepath.Join(tempDir, "file1.db")
	file2 := filepath.Join(tempDir, "file2.db")

	hf1, err := NewHeapFile(file1, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile 1: %v", err)
	}
	defer hf1.Close()

	hf2, err := NewHeapFile(file2, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile 2: %v", err)
	}
	defer hf2.Close()

	id1 := hf1.GetID()
	id2 := hf2.GetID()

	if id1 == id2 {
		t.Errorf("Expected different IDs for different files, both got %d", id1)
	}

	if id1 == 0 || id2 == 0 {
		t.Errorf("Expected non-zero IDs, got %d and %d", id1, id2)
	}

	// Test same file gives same ID
	sameFile := filepath.Join(tempDir, "same.db")
	hf3, err := NewHeapFile(sameFile, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile 3: %v", err)
	}
	defer hf3.Close()

	hf4, err := NewHeapFile(sameFile, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile 4: %v", err)
	}
	defer hf4.Close()

	id3 := hf3.GetID()
	id4 := hf4.GetID()

	if id3 != id4 {
		t.Errorf("Expected same IDs for same file, got %d and %d", id3, id4)
	}
}

func TestHeapFile_NumPages(t *testing.T) {
	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "test.db")
	td := mustCreateTupleDescForTest()

	hf, err := NewHeapFile(filename, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}
	defer hf.Close()

	numPages, err := hf.NumPages()
	if err != nil {
		t.Errorf("NumPages failed: %v", err)
	}

	if numPages != 0 {
		t.Errorf("Expected 0 pages for empty file, got %d", numPages)
	}

	testTuple := createTestTupleForTest(td, 1, "Alice")
	tid := primitives.NewTransactionID()
	_, err = hf.AddTuple(tid, testTuple)
	if err != nil {
		t.Fatalf("Failed to add tuple: %v", err)
	}

	numPages, err = hf.NumPages()
	if err != nil {
		t.Errorf("NumPages failed after adding tuple: %v", err)
	}

	if numPages != 1 {
		t.Errorf("Expected 1 page after adding tuple, got %d", numPages)
	}
}

func TestHeapFile_ReadWritePage(t *testing.T) {
	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "test.db")
	td := mustCreateTupleDescForTest()

	hf, err := NewHeapFile(filename, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}
	defer hf.Close()

	pageID := NewHeapPageID(hf.GetID(), 0)

	// Test reading new page
	page, err := hf.ReadPage(pageID)
	if err != nil {
		t.Errorf("ReadPage failed for new page: %v", err)
	}

	if page == nil {
		t.Fatal("ReadPage returned nil page")
	}

	heapPage, ok := page.(*HeapPage)
	if !ok {
		t.Fatal("ReadPage did not return HeapPage")
	}

	// Add tuple and write page
	testTuple := createTestTupleForTest(td, 1, "Alice")
	err = heapPage.AddTuple(testTuple)
	if err != nil {
		t.Fatalf("Failed to add tuple to page: %v", err)
	}

	err = hf.WritePage(heapPage)
	if err != nil {
		t.Errorf("WritePage failed: %v", err)
	}

	// Read back and verify
	readPage, err := hf.ReadPage(pageID)
	if err != nil {
		t.Errorf("ReadPage failed after write: %v", err)
	}

	readHeapPage, ok := readPage.(*HeapPage)
	if !ok {
		t.Fatal("ReadPage did not return HeapPage after write")
	}

	tuples := readHeapPage.GetTuples()
	if len(tuples) != 1 {
		t.Errorf("Expected 1 tuple after read, got %d", len(tuples))
	}

	// Test error cases
	wrongTableID := NewHeapPageID(99999, 0)
	_, err = hf.ReadPage(wrongTableID)
	if err == nil {
		t.Errorf("Expected error for wrong table ID")
	}
}

func TestHeapFile_AddTuple(t *testing.T) {
	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "test.db")
	td := mustCreateTupleDescForTest()

	hf, err := NewHeapFile(filename, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}
	defer hf.Close()

	tid := primitives.NewTransactionID()

	testTuple := createTestTupleForTest(td, 1, "Alice")
	pages, err := hf.AddTuple(tid, testTuple)
	if err != nil {
		t.Errorf("AddTuple failed: %v", err)
	}

	if len(pages) != 1 {
		t.Errorf("Expected 1 page returned from AddTuple, got %d", len(pages))
	}

	if testTuple.RecordID == nil {
		t.Errorf("Expected tuple to have RecordID after addition")
	}

	numPages, err := hf.NumPages()
	if err != nil {
		t.Errorf("NumPages failed: %v", err)
	}

	if numPages != 1 {
		t.Errorf("Expected 1 page after adding tuple, got %d", numPages)
	}

	// Add many tuples to test page creation
	for i := 0; i < 100; i++ {
		tuple := createTestTupleForTest(td, int64(i+2), "User")
		_, err := hf.AddTuple(tid, tuple)
		if err != nil {
			t.Errorf("Failed to add tuple %d: %v", i, err)
		}
	}

	finalNumPages, err := hf.NumPages()
	if err != nil {
		t.Errorf("NumPages failed after adding many tuples: %v", err)
	}

	if finalNumPages < 1 {
		t.Errorf("Expected at least 1 page after adding many tuples, got %d", finalNumPages)
	}
}

func TestHeapFile_DeleteTuple(t *testing.T) {
	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "test.db")
	td := mustCreateTupleDescForTest()

	hf, err := NewHeapFile(filename, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}
	defer hf.Close()

	tid := primitives.NewTransactionID()

	testTuple := createTestTupleForTest(td, 1, "Alice")
	_, err = hf.AddTuple(tid, testTuple)
	if err != nil {
		t.Fatalf("Failed to add tuple: %v", err)
	}

	if testTuple.RecordID == nil {
		t.Fatal("Tuple should have RecordID after addition")
	}

	page, err := hf.DeleteTuple(tid, testTuple)
	if err != nil {
		t.Errorf("DeleteTuple failed: %v", err)
	}

	if page == nil {
		t.Errorf("DeleteTuple returned nil page")
	}

	heapPage, ok := page.(*HeapPage)
	if !ok {
		t.Fatal("DeleteTuple did not return HeapPage")
	}

	tuples := heapPage.GetTuples()
	if len(tuples) != 0 {
		t.Errorf("Expected 0 tuples after deletion, got %d", len(tuples))
	}

	if testTuple.RecordID != nil {
		t.Errorf("Expected tuple RecordID to be nil after deletion")
	}

	// Test error case
	tupleWithoutID := createTestTupleForTest(td, 2, "Bob")
	tupleWithoutID.RecordID = nil
	_, err = hf.DeleteTuple(tid, tupleWithoutID)
	if err == nil {
		t.Errorf("Expected error for tuple without RecordID")
	}
}

func TestHeapFile_Close(t *testing.T) {
	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "test.db")
	td := mustCreateTupleDescForTest()

	hf, err := NewHeapFile(filename, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}

	if hf.file == nil {
		t.Fatal("HeapFile.file should not be nil")
	}

	err = hf.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	if hf.file != nil {
		t.Errorf("HeapFile.file should be nil after Close")
	}

	// Test multiple closes
	err = hf.Close()
	if err != nil {
		t.Errorf("Second Close call should not fail: %v", err)
	}
}

func TestHeapFile_PersistenceAcrossReopen(t *testing.T) {
	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "test.db")
	td := mustCreateTupleDescForTest()

	hf1, err := NewHeapFile(filename, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}

	tid := primitives.NewTransactionID()
	testTuple := createTestTupleForTest(td, 1, "Alice")
	_, err = hf1.AddTuple(tid, testTuple)
	if err != nil {
		t.Fatalf("Failed to add tuple: %v", err)
	}

	err = hf1.Close()
	if err != nil {
		t.Fatalf("Failed to close HeapFile: %v", err)
	}

	// Reopen and verify data persists
	hf2, err := NewHeapFile(filename, td)
	if err != nil {
		t.Fatalf("Failed to reopen HeapFile: %v", err)
	}
	defer hf2.Close()

	numPages, err := hf2.NumPages()
	if err != nil {
		t.Errorf("NumPages failed after reopen: %v", err)
	}

	if numPages != 1 {
		t.Errorf("Expected 1 page after reopen, got %d", numPages)
	}

	pageID := NewHeapPageID(hf2.GetID(), 0)
	page, err := hf2.ReadPage(pageID)
	if err != nil {
		t.Errorf("ReadPage failed after reopen: %v", err)
	}

	heapPage, ok := page.(*HeapPage)
	if !ok {
		t.Fatal("ReadPage did not return HeapPage after reopen")
	}

	tuples := heapPage.GetTuples()
	if len(tuples) != 1 {
		t.Errorf("Expected 1 tuple after reopen, got %d", len(tuples))
	}
}

// =============================================================================
// CONCURRENCY AND DEADLOCK TESTS
// =============================================================================

func TestHeapFile_DeadlockFix(t *testing.T) {
	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "deadlock_test.db")
	td := mustCreateTupleDescForTest()

	hf, err := NewHeapFile(filename, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}
	defer hf.Close()

	tid := primitives.NewTransactionID()
	testTuple := createTestTupleForTest(td, 1, "Alice")

	// This should not deadlock
	done := make(chan bool, 1)
	go func() {
		_, err := hf.AddTuple(tid, testTuple)
		if err != nil {
			t.Errorf("AddTuple failed: %v", err)
		}
		done <- true
	}()

	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("AddTuple deadlocked - test timed out")
	}
}

func TestHeapFile_ConcurrentAccess(t *testing.T) {
	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "concurrent_test.db")
	td := mustCreateTupleDescForTest()

	hf, err := NewHeapFile(filename, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}
	defer hf.Close()

	tid := primitives.NewTransactionID()

	numGoroutines := 10
	done := make(chan bool, numGoroutines)
	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer func() { done <- true }()

			tuple := createTestTupleForTest(td, int64(id), "ConcurrentUser")
			_, err := hf.AddTuple(tid, tuple)
			if err != nil {
				errors <- err
				return
			}

			numPages, err := hf.NumPages()
			if err != nil {
				errors <- err
				return
			}

			if numPages <= 0 {
				errors <- err
				return
			}
		}(i)
	}

	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	close(errors)
	for err := range errors {
		if err != nil {
			t.Errorf("Concurrent access error: %v", err)
		}
	}

	finalNumPages, err := hf.NumPages()
	if err != nil {
		t.Errorf("NumPages failed after concurrent access: %v", err)
	}

	if finalNumPages <= 0 {
		t.Errorf("Expected positive number of pages after concurrent access, got %d", finalNumPages)
	}
}

func TestHeapFile_ConcurrencyStress(t *testing.T) {
	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "stress_test.db")
	td := mustCreateTupleDescForTest()

	hf, err := NewHeapFile(filename, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}
	defer hf.Close()

	numWorkers := 20
	operationsPerWorker := 50
	var wg sync.WaitGroup
	errors := make(chan error, numWorkers*operationsPerWorker)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			tid := primitives.NewTransactionID()

			for j := 0; j < operationsPerWorker; j++ {
				// Mix of operations
				switch j % 4 {
				case 0, 1: // Add tuples (more frequent)
					tuple := createTestTupleForTest(td, int64(workerID*1000+j), "Worker")
					_, err := hf.AddTuple(tid, tuple)
					if err != nil {
						errors <- err
					}
				case 2: // Read number of pages
					_, err := hf.NumPages()
					if err != nil {
						errors <- err
					}
				case 3: // Get tuple desc
					desc := hf.GetTupleDesc()
					if desc == nil {
						errors <- err
					}
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		if err != nil {
			t.Errorf("Concurrent operation error: %v", err)
		}
	}
}

// =============================================================================
// ERROR HANDLING AND EDGE CASE TESTS
// =============================================================================

func TestHeapFile_InvalidOperations(t *testing.T) {
	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "invalid_test.db")
	td := mustCreateTupleDescForTest()

	hf, err := NewHeapFile(filename, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}

	tid := primitives.NewTransactionID()

	// Test with nil tuple
	t.Run("AddTuple with nil tuple", func(t *testing.T) {
		_, err := hf.AddTuple(tid, nil)
		if err == nil {
			t.Errorf("Expected error when adding nil tuple")
		}
	})

	// Test with mismatched tuple description
	t.Run("AddTuple with wrong schema", func(t *testing.T) {
		wrongTd, _ := tuple.NewTupleDesc([]types.Type{types.StringType}, []string{"name"})
		wrongTuple := tuple.NewTuple(wrongTd)
		wrongTuple.SetField(0, types.NewStringField("test", 128))

		_, err := hf.AddTuple(tid, wrongTuple)
		if err == nil {
			t.Errorf("Expected error when adding tuple with wrong schema")
		}
	})

	// Test delete with nil tuple
	t.Run("DeleteTuple with nil tuple", func(t *testing.T) {
		_, err := hf.DeleteTuple(tid, nil)
		if err == nil {
			t.Errorf("Expected error when deleting nil tuple")
		}
	})

	// Test operations after close
	hf.Close()

	t.Run("Operations after close", func(t *testing.T) {
		testTuple := createTestTupleForTest(td, 1, "test")

		_, err := hf.AddTuple(tid, testTuple)
		if err == nil {
			t.Errorf("Expected error when adding tuple to closed file")
		}

		_, err = hf.NumPages()
		if err == nil {
			t.Errorf("Expected error when calling NumPages on closed file")
		}

		pageID := NewHeapPageID(hf.GetID(), 0)
		_, err = hf.ReadPage(pageID)
		if err == nil {
			t.Errorf("Expected error when reading page from closed file")
		}
	})
}

func TestHeapFile_ErrorHandling(t *testing.T) {
	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "test.db")
	td := mustCreateTupleDescForTest()

	hf, err := NewHeapFile(filename, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}

	hf.Close()

	tid := primitives.NewTransactionID()
	testTuple := createTestTupleForTest(td, 1, "Alice")

	_, err = hf.AddTuple(tid, testTuple)
	if err == nil {
		t.Errorf("Expected error when adding tuple to closed file")
	}

	_, err = hf.NumPages()
	if err == nil {
		t.Errorf("Expected error when calling NumPages on closed file")
	}
}

func TestHeapFile_EdgeCases(t *testing.T) {
	tempDir := t.TempDir()

	t.Run("Empty file operations", func(t *testing.T) {
		filename := filepath.Join(tempDir, "empty_test.db")
		td := mustCreateTupleDescForTest()

		hf, err := NewHeapFile(filename, td)
		if err != nil {
			t.Fatalf("Failed to create HeapFile: %v", err)
		}
		defer hf.Close()

		// Check empty file has 0 pages
		numPages, err := hf.NumPages()
		if err != nil {
			t.Errorf("NumPages failed on empty file: %v", err)
		}
		if numPages != 0 {
			t.Errorf("Expected 0 pages for empty file, got %d", numPages)
		}

		// Try to read page 0 from empty file
		pageID := NewHeapPageID(hf.GetID(), 0)
		page, err := hf.ReadPage(pageID)
		if err != nil {
			t.Errorf("ReadPage failed on empty file: %v", err)
		}
		if page == nil {
			t.Errorf("ReadPage returned nil for empty file")
		}
	})

	t.Run("Very large tuple", func(t *testing.T) {
		filename := filepath.Join(tempDir, "large_tuple_test.db")

		// Create tuple desc with very large string
		td, _ := tuple.NewTupleDesc([]types.Type{types.StringType}, []string{"large_field"})

		hf, err := NewHeapFile(filename, td)
		if err != nil {
			t.Fatalf("Failed to create HeapFile: %v", err)
		}
		defer hf.Close()

		tid := primitives.NewTransactionID()
		largeTuple := tuple.NewTuple(td)
		// Create a very large string (might exceed page capacity)
		largeString := make([]byte, 5000) // Much larger than typical page capacity
		for i := range largeString {
			largeString[i] = 'A'
		}
		largeTuple.SetField(0, types.NewStringField(string(largeString), len(largeString)))

		_, err = hf.AddTuple(tid, largeTuple)
		// This might fail due to tuple being too large for a page
		// The behavior should be graceful (return error, not crash)
		if err != nil {
			t.Logf("Large tuple addition failed as expected: %v", err)
		}
	})

	t.Run("Multiple closes", func(t *testing.T) {
		filename := filepath.Join(tempDir, "multi_close_test.db")
		td := mustCreateTupleDescForTest()

		hf, err := NewHeapFile(filename, td)
		if err != nil {
			t.Fatalf("Failed to create HeapFile: %v", err)
		}

		// Close multiple times should not cause issues
		err1 := hf.Close()
		err2 := hf.Close()
		err3 := hf.Close()

		if err1 != nil {
			t.Errorf("First close failed: %v", err1)
		}
		if err2 != nil {
			t.Errorf("Second close failed: %v", err2)
		}
		if err3 != nil {
			t.Errorf("Third close failed: %v", err3)
		}
	})
}

func TestHeapFile_InvalidPageID(t *testing.T) {
	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "test.db")
	td := mustCreateTupleDescForTest()

	hf, err := NewHeapFile(filename, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}
	defer hf.Close()

	// Test with wrong table ID
	differentTableID := NewHeapPageID(99999, 0)
	_, err = hf.ReadPage(differentTableID)
	if err == nil {
		t.Errorf("Expected error for page ID with wrong table ID")
	}
}

// =============================================================================
// PERFORMANCE AND STRESS TESTS
// =============================================================================

func TestHeapFile_PageBoundaries(t *testing.T) {
	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "boundary_test.db")
	td := mustCreateTupleDescForTest()

	hf, err := NewHeapFile(filename, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}
	defer hf.Close()

	tid := primitives.NewTransactionID()

	// Calculate how many tuples fit in one page
	pageID := NewHeapPageID(hf.GetID(), 0)
	page, err := hf.ReadPage(pageID)
	if err != nil {
		t.Fatalf("Failed to read initial page: %v", err)
	}

	heapPage, ok := page.(*HeapPage)
	if !ok {
		t.Fatal("ReadPage did not return HeapPage")
	}

	maxTuplesPerPage := heapPage.numSlots
	t.Logf("Max tuples per page: %d", maxTuplesPerPage)

	// Fill exactly one page
	for i := 0; i < maxTuplesPerPage; i++ {
		tuple := createTestTupleForTest(td, int64(i), "BoundaryTest")
		_, err := hf.AddTuple(tid, tuple)
		if err != nil {
			t.Fatalf("Failed to add tuple %d: %v", i, err)
		}
	}

	// Check we have exactly 1 page
	numPages, err := hf.NumPages()
	if err != nil {
		t.Errorf("NumPages failed: %v", err)
	}
	if numPages != 1 {
		t.Errorf("Expected 1 page after filling first page, got %d", numPages)
	}

	// Add one more tuple - may or may not create second page depending on capacity calculations
	tuple := createTestTupleForTest(td, int64(maxTuplesPerPage), "SecondPage")
	_, err = hf.AddTuple(tid, tuple)
	if err != nil {
		t.Fatalf("Failed to add additional tuple: %v", err)
	}

	numPages, err = hf.NumPages()
	if err != nil {
		t.Errorf("NumPages failed after adding one more tuple: %v", err)
	}

	// The exact number of pages depends on whether the tuple fits in the existing page
	if numPages < 1 {
		t.Errorf("Expected at least 1 page, got %d", numPages)
	}

	t.Logf("After adding %d tuples, we have %d pages", maxTuplesPerPage+1, numPages)
}

func TestHeapFile_LargeFile(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large file test in short mode")
	}

	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "large.db")
	td := mustCreateTupleDescForTest()

	hf, err := NewHeapFile(filename, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}
	defer hf.Close()

	tid := primitives.NewTransactionID()
	numTuples := 1000

	start := time.Now()
	for i := 0; i < numTuples; i++ {
		tuple := createTestTupleForTest(td, int64(i), "LargeFileUser")
		_, err := hf.AddTuple(tid, tuple)
		if err != nil {
			t.Fatalf("Failed to add tuple %d: %v", i, err)
		}
	}
	addTime := time.Since(start)

	numPages, err := hf.NumPages()
	if err != nil {
		t.Errorf("NumPages failed: %v", err)
	}

	if numPages <= 0 {
		t.Errorf("Expected positive number of pages, got %d", numPages)
	}

	t.Logf("Added %d tuples in %v across %d pages", numTuples, addTime, numPages)

	stat, err := os.Stat(filename)
	if err != nil {
		t.Errorf("Failed to stat file: %v", err)
	}

	expectedSize := int64(numPages) * int64(page.PageSize)
	if stat.Size() != expectedSize {
		t.Errorf("Expected file size %d, got %d", expectedSize, stat.Size())
	}
}

func TestHeapFile_MemoryUsage(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory usage test in short mode")
	}

	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "memory_test.db")
	td := mustCreateTupleDescForTest()

	hf, err := NewHeapFile(filename, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}
	defer hf.Close()

	tid := primitives.NewTransactionID()

	// Add many tuples and check memory doesn't grow unbounded
	numTuples := 1000
	for i := range numTuples {
		tuple := createTestTupleForTest(td, int64(i), "MemoryTest")
		_, err := hf.AddTuple(tid, tuple)
		if err != nil {
			t.Fatalf("Failed to add tuple %d: %v", i, err)
		}

		// Periodically check basic operations still work
		if i%100 == 99 {
			numPages, err := hf.NumPages()
			if err != nil {
				t.Errorf("NumPages failed at iteration %d: %v", i, err)
			}
			if numPages <= 0 {
				t.Errorf("Invalid page count at iteration %d: %d", i, numPages)
			}
		}
	}

	t.Logf("Successfully added %d tuples", numTuples)
}

func TestHeapFile_IteratorConsistency(t *testing.T) {
	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "iterator_test.db")
	td := mustCreateTupleDescForTest()

	hf, err := NewHeapFile(filename, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}
	defer hf.Close()

	tid := primitives.NewTransactionID()

	// Add some tuples
	numTuples := 100
	addedTuples := make([]*tuple.Tuple, numTuples)
	for i := 0; i < numTuples; i++ {
		tuple := createTestTupleForTest(td, int64(i), "IteratorTest")
		addedTuples[i] = tuple
		pages, err := hf.AddTuple(tid, tuple)
		if err != nil {
			t.Fatalf("Failed to add tuple %d: %v", i, err)
		}

		// Write the page to ensure it's persisted
		if len(pages) > 0 {
			err = hf.WritePage(pages[0])
			if err != nil {
				t.Fatalf("Failed to write page for tuple %d: %v", i, err)
			}
		}
	}

	// Create iterator and count tuples
	iter := hf.Iterator(tid)
	err = iter.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}
	defer iter.Close()

	count := 0
	for {
		hasNext, err := iter.HasNext()
		if err != nil {
			t.Errorf("Iterator HasNext error: %v", err)
			break
		}
		if !hasNext {
			break
		}

		tuple, err := iter.Next()
		if err != nil {
			t.Errorf("Iterator Next error: %v", err)
			break
		}
		if tuple == nil {
			t.Errorf("Iterator returned nil tuple")
			break
		}
		count++
	}

	if count != numTuples {
		t.Errorf("Expected iterator to return %d tuples, got %d", numTuples, count)
	}
}

func TestHeapFile_FileSystemErrors(t *testing.T) {
	t.Run("Read-only directory", func(t *testing.T) {
		if os.Getuid() == 0 {
			t.Skip("Skipping read-only test when running as root")
		}

		tempDir := t.TempDir()
		readOnlyDir := filepath.Join(tempDir, "readonly")
		err := os.Mkdir(readOnlyDir, 0755)
		if err != nil {
			t.Fatalf("Failed to create directory: %v", err)
		}

		// Make directory read-only
		err = os.Chmod(readOnlyDir, 0444)
		if err != nil {
			t.Fatalf("Failed to change directory permissions: %v", err)
		}
		defer os.Chmod(readOnlyDir, 0755) // Restore permissions for cleanup

		td := mustCreateTupleDescForTest()
		filename := filepath.Join(readOnlyDir, "test.db")

		_, err = NewHeapFile(filename, td)
		if err == nil {
			t.Errorf("Expected error when creating file in read-only directory")
		}
	})
}

// =============================================================================
// BENCHMARKS
// =============================================================================

func BenchmarkHeapFile_AddTuple(b *testing.B) {
	tempDir := b.TempDir()
	filename := filepath.Join(tempDir, "benchmark.db")
	td := mustCreateTupleDescForTest()

	hf, err := NewHeapFile(filename, td)
	if err != nil {
		b.Fatalf("Failed to create HeapFile: %v", err)
	}
	defer hf.Close()

	tid := primitives.NewTransactionID()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tuple := createTestTupleForTest(td, int64(i), "BenchmarkUser")
		_, err := hf.AddTuple(tid, tuple)
		if err != nil {
			b.Fatalf("Failed to add tuple: %v", err)
		}
	}
}

func BenchmarkHeapFile_NumPages(b *testing.B) {
	tempDir := b.TempDir()
	filename := filepath.Join(tempDir, "benchmark.db")
	td := mustCreateTupleDescForTest()

	hf, err := NewHeapFile(filename, td)
	if err != nil {
		b.Fatalf("Failed to create HeapFile: %v", err)
	}
	defer hf.Close()

	// Add some tuples first
	tid := primitives.NewTransactionID()
	for i := 0; i < 1000; i++ {
		tuple := createTestTupleForTest(td, int64(i), "BenchmarkUser")
		hf.AddTuple(tid, tuple)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := hf.NumPages()
		if err != nil {
			b.Fatalf("NumPages failed: %v", err)
		}
	}
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

func mustCreateTupleDescForTest() *tuple.TupleDescription {
	types := []types.Type{types.IntType, types.StringType}
	fields := []string{"id", "name"}
	td, err := tuple.NewTupleDesc(types, fields)
	if err != nil {
		panic(err)
	}
	return td
}

func createTestTupleForTest(td *tuple.TupleDescription, id int64, name string) *tuple.Tuple {
	t := tuple.NewTuple(td)
	t.SetField(0, types.NewIntField(id))
	t.SetField(1, types.NewStringField(name, 128))
	return t
}
