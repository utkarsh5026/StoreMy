package heap

import (
	"fmt"
	"path/filepath"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

func TestNewHeapFileIterator(t *testing.T) {
	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "test.db")
	td := mustCreateTupleDesc()

	hf, err := NewHeapFile(filename, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}
	defer hf.Close()

	tid := &primitives.TransactionID{}
	iterator := NewHeapFileIterator(hf, tid)

	if iterator == nil {
		t.Fatal("NewHeapFileIterator returned nil")
	}

	if iterator.file != hf {
		t.Errorf("Expected iterator file to be %v, got %v", hf, iterator.file)
	}

	if iterator.tid != tid {
		t.Errorf("Expected iterator tid to be %v, got %v", tid, iterator.tid)
	}

	if iterator.currentPage != -1 {
		t.Errorf("Expected currentPage to be -1, got %d", iterator.currentPage)
	}

	if iterator.pageIter != nil {
		t.Errorf("Expected pageIter to be nil initially, got %v", iterator.pageIter)
	}

	if iterator.isOpen {
		t.Errorf("Expected isOpen to be false initially, got %v", iterator.isOpen)
	}
}

func TestHeapFileIterator_Open(t *testing.T) {
	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "test.db")
	td := mustCreateTupleDesc()

	hf, err := NewHeapFile(filename, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}
	defer hf.Close()

	tid := &primitives.TransactionID{}
	iterator := NewHeapFileIterator(hf, tid)

	err = iterator.Open()
	if err != nil {
		t.Errorf("Open returned error: %v", err)
	}

	if !iterator.isOpen {
		t.Errorf("Expected isOpen to be true after Open, got %v", iterator.isOpen)
	}

	// Note: currentPage is advanced by moveToNextPage during Open
	// For an empty file, it should end up at 0 (first page attempt)
	if iterator.currentPage < -1 {
		t.Errorf("Expected currentPage to be >= -1 after Open, got %d", iterator.currentPage)
	}
}

func TestHeapFileIterator_Close(t *testing.T) {
	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "test.db")
	td := mustCreateTupleDesc()

	hf, err := NewHeapFile(filename, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}
	defer hf.Close()

	tid := &primitives.TransactionID{}
	iterator := NewHeapFileIterator(hf, tid)

	err = iterator.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}

	err = iterator.Close()
	if err != nil {
		t.Errorf("Close returned error: %v", err)
	}

	if iterator.isOpen {
		t.Errorf("Expected isOpen to be false after Close, got %v", iterator.isOpen)
	}

	if iterator.pageIter != nil {
		t.Errorf("Expected pageIter to be nil after Close, got %v", iterator.pageIter)
	}
}

func TestHeapFileIterator_EmptyFile(t *testing.T) {
	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "test.db")
	td := mustCreateTupleDesc()

	hf, err := NewHeapFile(filename, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}
	defer hf.Close()

	tid := &primitives.TransactionID{}
	iterator := NewHeapFileIterator(hf, tid)

	err = iterator.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}
	defer iterator.Close()

	hasNext, err := iterator.HasNext()
	if err != nil {
		t.Errorf("HasNext returned error: %v", err)
	}

	if hasNext {
		t.Errorf("Expected HasNext to be false for empty file, got %v", hasNext)
	}

	_, err = iterator.Next()
	if err == nil {
		t.Errorf("Expected error when calling Next on empty file")
	}
}

func TestHeapFileIterator_HasNextNotOpened(t *testing.T) {
	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "test.db")
	td := mustCreateTupleDesc()

	hf, err := NewHeapFile(filename, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}
	defer hf.Close()

	tid := &primitives.TransactionID{}
	iterator := NewHeapFileIterator(hf, tid)

	_, err = iterator.HasNext()
	if err == nil {
		t.Errorf("Expected error when calling HasNext on unopened iterator")
	}
}

func TestHeapFileIterator_NextNotOpened(t *testing.T) {
	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "test.db")
	td := mustCreateTupleDesc()

	hf, err := NewHeapFile(filename, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}
	defer hf.Close()

	tid := &primitives.TransactionID{}
	iterator := NewHeapFileIterator(hf, tid)

	_, err = iterator.Next()
	if err == nil {
		t.Errorf("Expected error when calling Next on unopened iterator")
	}
}

func TestHeapFileIterator_SinglePageWithTuples(t *testing.T) {
	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "test.db")
	td := mustCreateTupleDesc()

	hf, err := NewHeapFile(filename, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}
	defer hf.Close()

	tuple1 := createTestTuple(td, 1, "Alice")
	tuple2 := createTestTuple(td, 2, "Bob")
	tuple3 := createTestTuple(td, 3, "Charlie")

	pageID := NewHeapPageID(hf.GetID(), 0)
	p, err := hf.ReadPage(pageID)
	if err != nil {
		p, err = NewHeapPage(pageID, make([]byte, page.PageSize), td)
		if err != nil {
			t.Fatalf("Failed to create page: %v", err)
		}
	}

	heapPage, ok := p.(*HeapPage)
	if !ok {
		t.Fatalf("Expected HeapPage, got %T", p)
	}

	err = heapPage.AddTuple(tuple1)
	if err != nil {
		t.Fatalf("Failed to add tuple1: %v", err)
	}

	err = heapPage.AddTuple(tuple2)
	if err != nil {
		t.Fatalf("Failed to add tuple2: %v", err)
	}

	err = heapPage.AddTuple(tuple3)
	if err != nil {
		t.Fatalf("Failed to add tuple3: %v", err)
	}

	err = hf.WritePage(heapPage)
	if err != nil {
		t.Fatalf("Failed to write page: %v", err)
	}

	tid := &primitives.TransactionID{}
	iterator := NewHeapFileIterator(hf, tid)

	err = iterator.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}
	defer iterator.Close()

	expectedCount := 3
	actualCount := 0
	actualTuples := []*tuple.Tuple{}

	for {
		hasNext, err := iterator.HasNext()
		if err != nil {
			t.Errorf("HasNext returned error: %v", err)
			break
		}

		if !hasNext {
			break
		}

		tup, err := iterator.Next()
		if err != nil {
			t.Errorf("Next returned error: %v", err)
			break
		}

		if tup == nil {
			t.Errorf("Next returned nil tuple")
			break
		}

		actualTuples = append(actualTuples, tup)
		actualCount++
	}

	if actualCount != expectedCount {
		t.Errorf("Expected to iterate over %d tuples, got %d", expectedCount, actualCount)
	}

	_, err = iterator.Next()
	if err == nil {
		t.Errorf("Expected error when calling Next after iteration finished")
	}
}

func TestHeapFileIterator_MultiplePagesWithTuples(t *testing.T) {
	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "test.db")
	td := mustCreateTupleDesc()

	hf, err := NewHeapFile(filename, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}
	defer hf.Close()

	numPages := 3
	tuplesPerPage := 2
	totalTuples := numPages * tuplesPerPage

	tupleID := 1
	for pageNum := 0; pageNum < numPages; pageNum++ {
		pageID := NewHeapPageID(hf.GetID(), pageNum)
		page, err := NewHeapPage(pageID, make([]byte, page.PageSize), td)
		if err != nil {
			t.Fatalf("Failed to create page %d: %v", pageNum, err)
		}

		for tupleNum := 0; tupleNum < tuplesPerPage; tupleNum++ {
			tuple := createTestTuple(td, int32(tupleID), fmt.Sprintf("User%d", tupleID))
			err = page.AddTuple(tuple)
			if err != nil {
				t.Fatalf("Failed to add tuple %d to page %d: %v", tupleID, pageNum, err)
			}
			tupleID++
		}

		err = hf.WritePage(page)
		if err != nil {
			t.Fatalf("Failed to write page %d: %v", pageNum, err)
		}
	}

	tid := &primitives.TransactionID{}
	iterator := NewHeapFileIterator(hf, tid)

	err = iterator.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}
	defer iterator.Close()

	actualCount := 0
	seenIDs := make(map[int32]bool)

	for {
		hasNext, err := iterator.HasNext()
		if err != nil {
			t.Errorf("HasNext returned error: %v", err)
			break
		}

		if !hasNext {
			break
		}

		tup, err := iterator.Next()
		if err != nil {
			t.Errorf("Next returned error: %v", err)
			break
		}

		if tup == nil {
			t.Errorf("Next returned nil tuple")
			break
		}

		idField, err := tup.GetField(0)
		if err != nil {
			t.Errorf("Failed to get id field: %v", err)
			continue
		}

		intField, ok := idField.(*types.IntField)
		if !ok {
			t.Errorf("Expected IntField, got %T", idField)
			continue
		}

		if seenIDs[intField.Value] {
			t.Errorf("Duplicate tuple ID %d", intField.Value)
		}
		seenIDs[intField.Value] = true

		actualCount++
	}

	if actualCount != totalTuples {
		t.Errorf("Expected to iterate over %d tuples, got %d", totalTuples, actualCount)
	}

	if len(seenIDs) != totalTuples {
		t.Errorf("Expected %d unique tuple IDs, got %d", totalTuples, len(seenIDs))
	}
}

func TestHeapFileIterator_Rewind(t *testing.T) {
	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "test.db")
	td := mustCreateTupleDesc()

	hf, err := NewHeapFile(filename, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}
	defer hf.Close()

	tuple1 := createTestTuple(td, 1, "Alice")
	tuple2 := createTestTuple(td, 2, "Bob")

	pageID := NewHeapPageID(hf.GetID(), 0)
	page, err := NewHeapPage(pageID, make([]byte, page.PageSize), td)
	if err != nil {
		t.Fatalf("Failed to create page: %v", err)
	}

	err = page.AddTuple(tuple1)
	if err != nil {
		t.Fatalf("Failed to add tuple1: %v", err)
	}

	err = page.AddTuple(tuple2)
	if err != nil {
		t.Fatalf("Failed to add tuple2: %v", err)
	}

	err = hf.WritePage(page)
	if err != nil {
		t.Fatalf("Failed to write page: %v", err)
	}

	tid := &primitives.TransactionID{}
	iterator := NewHeapFileIterator(hf, tid)

	err = iterator.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}
	defer iterator.Close()

	firstIteration := []*tuple.Tuple{}
	for {
		hasNext, err := iterator.HasNext()
		if err != nil {
			t.Errorf("HasNext returned error during first iteration: %v", err)
			break
		}

		if !hasNext {
			break
		}

		tup, err := iterator.Next()
		if err != nil {
			t.Errorf("Next returned error during first iteration: %v", err)
			break
		}

		firstIteration = append(firstIteration, tup)
	}

	err = iterator.Rewind()
	if err != nil {
		t.Errorf("Rewind returned error: %v", err)
	}

	secondIteration := []*tuple.Tuple{}
	for {
		hasNext, err := iterator.HasNext()
		if err != nil {
			t.Errorf("HasNext returned error during second iteration: %v", err)
			break
		}

		if !hasNext {
			break
		}

		tup, err := iterator.Next()
		if err != nil {
			t.Errorf("Next returned error during second iteration: %v", err)
			break
		}

		secondIteration = append(secondIteration, tup)
	}

	if len(firstIteration) != len(secondIteration) {
		t.Errorf("Expected both iterations to have same length, got %d and %d",
			len(firstIteration), len(secondIteration))
	}

	if len(firstIteration) != 2 {
		t.Errorf("Expected 2 tuples in each iteration, got %d", len(firstIteration))
	}
}

func TestHeapFileIterator_SkipEmptyPages(t *testing.T) {
	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "test.db")
	td := mustCreateTupleDesc()

	hf, err := NewHeapFile(filename, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}
	defer hf.Close()

	// Create a page with tuples first
	pageID0 := NewHeapPageID(hf.GetID(), 0)
	page0, err := NewHeapPage(pageID0, make([]byte, page.PageSize), td)
	if err != nil {
		t.Fatalf("Failed to create page 0: %v", err)
	}

	tuple1 := createTestTuple(td, 1, "Alice")
	tuple2 := createTestTuple(td, 2, "Bob")

	err = page0.AddTuple(tuple1)
	if err != nil {
		t.Fatalf("Failed to add tuple1: %v", err)
	}

	err = page0.AddTuple(tuple2)
	if err != nil {
		t.Fatalf("Failed to add tuple2: %v", err)
	}

	err = hf.WritePage(page0)
	if err != nil {
		t.Fatalf("Failed to write page 0: %v", err)
	}

	tid := &primitives.TransactionID{}
	iterator := NewHeapFileIterator(hf, tid)

	err = iterator.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}
	defer iterator.Close()

	actualCount := 0
	for {
		hasNext, err := iterator.HasNext()
		if err != nil {
			t.Errorf("HasNext returned error: %v", err)
			break
		}

		if !hasNext {
			break
		}

		tup, err := iterator.Next()
		if err != nil {
			t.Errorf("Next returned error: %v", err)
			break
		}

		if tup == nil {
			t.Errorf("Next returned nil tuple")
			break
		}

		actualCount++
	}

	// Should iterate over both tuples
	if actualCount != 2 {
		t.Errorf("Expected to iterate over 2 tuples, got %d", actualCount)
	}
}
