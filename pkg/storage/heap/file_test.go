package heap

import (
	"os"
	"path/filepath"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

// Test helper functions
func createTestTupleDesc() *tuple.TupleDescription {
	fieldTypes := []types.Type{types.IntType, types.StringType}
	fields := []string{"id", "name"}
	td, err := tuple.NewTupleDesc(fieldTypes, fields)
	if err != nil {
		panic(err)
	}
	return td
}

func createTempFile(t *testing.T, name string) (primitives.Filepath, func()) {
	t.Helper()
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, name)

	cleanup := func() {
		os.Remove(filePath)
	}

	return primitives.Filepath(filePath), cleanup
}

func createTestTupleForFile(td *tuple.TupleDescription, id int64, name string) *tuple.Tuple {
	t := tuple.NewTuple(td)
	t.SetField(0, types.NewIntField(id))
	t.SetField(1, types.NewStringField(name, 128))
	return t
}

func TestNewHeapFile(t *testing.T) {
	td := createTestTupleDesc()

	t.Run("Valid file creation", func(t *testing.T) {
		filePath, cleanup := createTempFile(t, "test.dat")
		defer cleanup()

		hf, err := NewHeapFile(filePath, td)
		if err != nil {
			t.Fatalf("Failed to create HeapFile: %v", err)
		}
		if hf == nil {
			t.Fatal("NewHeapFile returned nil")
		}
		defer hf.Close()

		if hf.tupleDesc != td {
			t.Error("HeapFile has incorrect tuple descriptor")
		}
	})

	t.Run("Empty filename", func(t *testing.T) {
		hf, err := NewHeapFile("", td)
		if err == nil {
			if hf != nil {
				hf.Close()
			}
			t.Fatal("Expected error with empty filename")
		}
	})

	t.Run("Create new file", func(t *testing.T) {
		filePath, cleanup := createTempFile(t, "newfile.dat")
		defer cleanup()

		// Remove the file first to test creation
		os.Remove(string(filePath))

		hf, err := NewHeapFile(filePath, td)
		if err != nil {
			t.Fatalf("Failed to create HeapFile with new file: %v", err)
		}
		defer hf.Close()

		if _, err := os.Stat(string(filePath)); os.IsNotExist(err) {
			t.Error("File was not created")
		}
	})
}

func TestHeapFile_GetID(t *testing.T) {
	td := createTestTupleDesc()
	filePath1, cleanup1 := createTempFile(t, "test1.dat")
	defer cleanup1()

	filePath2, cleanup2 := createTempFile(t, "test2.dat")
	defer cleanup2()

	hf1, err := NewHeapFile(filePath1, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile 1: %v", err)
	}
	defer hf1.Close()

	hf2, err := NewHeapFile(filePath2, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile 2: %v", err)
	}
	defer hf2.Close()

	id1 := hf1.GetID()
	id2 := hf2.GetID()

	if id1 == 0 {
		t.Error("Expected non-zero ID for HeapFile 1")
	}

	if id2 == 0 {
		t.Error("Expected non-zero ID for HeapFile 2")
	}

	if id1 == id2 {
		t.Error("Expected different IDs for different files")
	}

	// Test same file gives same ID
	hf1Again, err := NewHeapFile(filePath1, td)
	if err != nil {
		t.Fatalf("Failed to open HeapFile 1 again: %v", err)
	}
	defer hf1Again.Close()

	if hf1.GetID() != hf1Again.GetID() {
		t.Error("Expected same ID for same file path")
	}
}

func TestHeapFile_GetTupleDesc(t *testing.T) {
	td := createTestTupleDesc()
	filePath, cleanup := createTempFile(t, "test.dat")
	defer cleanup()

	hf, err := NewHeapFile(filePath, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}
	defer hf.Close()

	retrievedTD := hf.GetTupleDesc()
	if retrievedTD != td {
		t.Error("GetTupleDesc returned incorrect tuple descriptor")
	}

	if !retrievedTD.Equals(td) {
		t.Error("Retrieved tuple descriptor not equal to original")
	}
}

func TestHeapFile_NumPages(t *testing.T) {
	td := createTestTupleDesc()
	filePath, cleanup := createTempFile(t, "test.dat")
	defer cleanup()

	hf, err := NewHeapFile(filePath, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}
	defer hf.Close()

	t.Run("Empty file", func(t *testing.T) {
		numPages, err := hf.NumPages()
		if err != nil {
			t.Errorf("NumPages failed: %v", err)
		}

		if numPages != 0 {
			t.Errorf("Expected 0 pages for empty file, got %d", numPages)
		}
	})

	t.Run("After writing a page", func(t *testing.T) {
		pageID := page.NewPageDescriptor(hf.GetID(), 0)
		pageData := make([]byte, page.PageSize)
		heapPage, err := NewHeapPage(pageID, pageData, td)
		if err != nil {
			t.Fatalf("Failed to create HeapPage: %v", err)
		}

		err = hf.WritePage(heapPage)
		if err != nil {
			t.Fatalf("Failed to write page: %v", err)
		}

		numPages, err := hf.NumPages()
		if err != nil {
			t.Errorf("NumPages failed: %v", err)
		}

		if numPages != 1 {
			t.Errorf("Expected 1 page after writing, got %d", numPages)
		}
	})
}

func TestHeapFile_NumPages_ClosedFile(t *testing.T) {
	td := createTestTupleDesc()
	filePath, cleanup := createTempFile(t, "test.dat")
	defer cleanup()

	hf, err := NewHeapFile(filePath, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}

	hf.Close()

	_, err = hf.NumPages()
	if err == nil {
		t.Error("Expected error when calling NumPages on closed file")
	}
}

func TestHeapFile_ReadPage(t *testing.T) {
	td := createTestTupleDesc()
	filePath, cleanup := createTempFile(t, "test.dat")
	defer cleanup()

	hf, err := NewHeapFile(filePath, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}
	defer hf.Close()

	t.Run("Read non-existent page (EOF)", func(t *testing.T) {
		pageID := page.NewPageDescriptor(hf.GetID(), 0)
		p, err := hf.ReadPage(pageID)
		if err != nil {
			t.Errorf("ReadPage failed for non-existent page: %v", err)
		}

		if p == nil {
			t.Error("Expected page to be created for EOF")
		}
	})

	t.Run("Write and read page", func(t *testing.T) {
		pageID := page.NewPageDescriptor(hf.GetID(), 0)
		pageData := make([]byte, page.PageSize)
		heapPage, err := NewHeapPage(pageID, pageData, td)
		if err != nil {
			t.Fatalf("Failed to create HeapPage: %v", err)
		}

		// Add a tuple to the page
		testTuple := createTestTupleForFile(td, 1, "Alice")
		err = heapPage.AddTuple(testTuple)
		if err != nil {
			t.Fatalf("Failed to add tuple to page: %v", err)
		}

		// Write the page
		err = hf.WritePage(heapPage)
		if err != nil {
			t.Fatalf("Failed to write page: %v", err)
		}

		// Read the page back
		readPage, err := hf.ReadPage(pageID)
		if err != nil {
			t.Fatalf("Failed to read page: %v", err)
		}

		if readPage == nil {
			t.Fatal("ReadPage returned nil")
		}

		// Verify the page ID
		if !readPage.GetID().Equals(pageID) {
			t.Error("Read page has incorrect ID")
		}

		// Verify tuples
		heapPageRead := readPage.(*HeapPage)
		tuples := heapPageRead.GetTuples()
		if len(tuples) != 1 {
			t.Errorf("Expected 1 tuple in read page, got %d", len(tuples))
		}
	})

	t.Run("Invalid page ID - nil", func(t *testing.T) {
		_, err := hf.ReadPage(nil)
		if err == nil {
			t.Error("Expected error with nil page ID")
		}
	})

	t.Run("Invalid page ID - wrong table ID", func(t *testing.T) {
		wrongPageID := page.NewPageDescriptor(hf.GetID()+1, 0)
		_, err := hf.ReadPage(wrongPageID)
		if err == nil {
			t.Error("Expected error with wrong table ID")
		}
	})
}

func TestHeapFile_ReadPage_ClosedFile(t *testing.T) {
	td := createTestTupleDesc()
	filePath, cleanup := createTempFile(t, "test.dat")
	defer cleanup()

	hf, err := NewHeapFile(filePath, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}

	pageID := page.NewPageDescriptor(hf.GetID(), 0)
	hf.Close()

	_, err = hf.ReadPage(pageID)
	if err == nil {
		t.Error("Expected error when reading from closed file")
	}
}

func TestHeapFile_WritePage(t *testing.T) {
	td := createTestTupleDesc()
	filePath, cleanup := createTempFile(t, "test.dat")
	defer cleanup()

	hf, err := NewHeapFile(filePath, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}
	defer hf.Close()

	t.Run("Write valid page", func(t *testing.T) {
		pageID := page.NewPageDescriptor(hf.GetID(), 0)
		pageData := make([]byte, page.PageSize)
		heapPage, err := NewHeapPage(pageID, pageData, td)
		if err != nil {
			t.Fatalf("Failed to create HeapPage: %v", err)
		}

		err = hf.WritePage(heapPage)
		if err != nil {
			t.Errorf("WritePage failed: %v", err)
		}
	})

	t.Run("Write nil page", func(t *testing.T) {
		err := hf.WritePage(nil)
		if err == nil {
			t.Error("Expected error when writing nil page")
		}
	})

	t.Run("Write multiple pages", func(t *testing.T) {
		for i := 0; i < 3; i++ {
			pageID := page.NewPageDescriptor(hf.GetID(), primitives.PageNumber(i))
			pageData := make([]byte, page.PageSize)
			heapPage, err := NewHeapPage(pageID, pageData, td)
			if err != nil {
				t.Fatalf("Failed to create HeapPage %d: %v", i, err)
			}

			err = hf.WritePage(heapPage)
			if err != nil {
				t.Errorf("WritePage failed for page %d: %v", i, err)
			}
		}

		numPages, err := hf.NumPages()
		if err != nil {
			t.Fatalf("NumPages failed: %v", err)
		}

		if numPages != 3 {
			t.Errorf("Expected 3 pages after writing, got %d", numPages)
		}
	})
}

func TestHeapFile_WritePage_ClosedFile(t *testing.T) {
	td := createTestTupleDesc()
	filePath, cleanup := createTempFile(t, "test.dat")
	defer cleanup()

	hf, err := NewHeapFile(filePath, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}

	pageID := page.NewPageDescriptor(hf.GetID(), 0)
	pageData := make([]byte, page.PageSize)
	heapPage, err := NewHeapPage(pageID, pageData, td)
	if err != nil {
		t.Fatalf("Failed to create HeapPage: %v", err)
	}

	hf.Close()

	err = hf.WritePage(heapPage)
	if err == nil {
		t.Error("Expected error when writing to closed file")
	}
}

func TestHeapFile_Close(t *testing.T) {
	td := createTestTupleDesc()
	filePath, cleanup := createTempFile(t, "test.dat")
	defer cleanup()

	t.Run("Close open file", func(t *testing.T) {
		hf, err := NewHeapFile(filePath, td)
		if err != nil {
			t.Fatalf("Failed to create HeapFile: %v", err)
		}

		err = hf.Close()
		if err != nil {
			t.Errorf("Close failed: %v", err)
		}

	})

	t.Run("Close already closed file", func(t *testing.T) {
		hf, err := NewHeapFile(filePath, td)
		if err != nil {
			t.Fatalf("Failed to create HeapFile: %v", err)
		}

		err = hf.Close()
		if err != nil {
			t.Errorf("First close failed: %v", err)
		}

		err = hf.Close()
		if err != nil {
			t.Errorf("Second close should not error: %v", err)
		}
	})
}

func TestHeapFile_ReadWrite_Integration(t *testing.T) {
	td := createTestTupleDesc()
	filePath, cleanup := createTempFile(t, "test_integration.dat")
	defer cleanup()

	hf, err := NewHeapFile(filePath, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}
	defer hf.Close()

	// Create and write a page with tuples
	pageID := page.NewPageDescriptor(hf.GetID(), 0)
	pageData := make([]byte, page.PageSize)
	heapPage, err := NewHeapPage(pageID, pageData, td)
	if err != nil {
		t.Fatalf("Failed to create HeapPage: %v", err)
	}

	// Add multiple tuples
	testTuples := []struct {
		id   int64
		name string
	}{
		{1, "Alice"},
		{2, "Bob"},
		{3, "Charlie"},
	}

	for _, tt := range testTuples {
		tup := createTestTupleForFile(td, tt.id, tt.name)
		err = heapPage.AddTuple(tup)
		if err != nil {
			t.Fatalf("Failed to add tuple (%d, %s): %v", tt.id, tt.name, err)
		}
	}

	// Write the page
	err = hf.WritePage(heapPage)
	if err != nil {
		t.Fatalf("Failed to write page: %v", err)
	}

	// Read the page back
	readPage, err := hf.ReadPage(pageID)
	if err != nil {
		t.Fatalf("Failed to read page: %v", err)
	}

	// Verify tuples
	heapPageRead := readPage.(*HeapPage)
	tuples := heapPageRead.GetTuples()

	if len(tuples) != len(testTuples) {
		t.Fatalf("Expected %d tuples, got %d", len(testTuples), len(tuples))
	}

	// Verify tuple contents
	for i, expectedTuple := range testTuples {
		readTuple := tuples[i]

		idField, err := readTuple.GetField(0)
		if err != nil {
			t.Fatalf("Failed to get id field: %v", err)
		}

		nameField, err := readTuple.GetField(1)
		if err != nil {
			t.Fatalf("Failed to get name field: %v", err)
		}

		if idField.(*types.IntField).Value != expectedTuple.id {
			t.Errorf("Expected id %d, got %d", expectedTuple.id, idField.(*types.IntField).Value)
		}

		if nameField.(*types.StringField).Value != expectedTuple.name {
			t.Errorf("Expected name %s, got %s", expectedTuple.name, nameField.(*types.StringField).Value)
		}
	}
}

func TestHeapFile_MultiPage_ReadWrite(t *testing.T) {
	td := createTestTupleDesc()
	filePath, cleanup := createTempFile(t, "test_multipage.dat")
	defer cleanup()

	hf, err := NewHeapFile(filePath, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}
	defer hf.Close()

	numPages := 5
	tuplesPerPage := 3

	// Write multiple pages
	for pageNum := 0; pageNum < numPages; pageNum++ {
		pageID := page.NewPageDescriptor(hf.GetID(), primitives.PageNumber(pageNum))
		pageData := make([]byte, page.PageSize)
		heapPage, err := NewHeapPage(pageID, pageData, td)
		if err != nil {
			t.Fatalf("Failed to create HeapPage %d: %v", pageNum, err)
		}

		for i := 0; i < tuplesPerPage; i++ {
			tupleID := int64(pageNum*tuplesPerPage + i)
			tup := createTestTupleForFile(td, tupleID, "User")
			err = heapPage.AddTuple(tup)
			if err != nil {
				t.Fatalf("Failed to add tuple to page %d: %v", pageNum, err)
			}
		}

		err = hf.WritePage(heapPage)
		if err != nil {
			t.Fatalf("Failed to write page %d: %v", pageNum, err)
		}
	}

	// Verify number of pages
	numPagesRead, err := hf.NumPages()
	if err != nil {
		t.Fatalf("NumPages failed: %v", err)
	}

	if numPagesRead != primitives.PageNumber(numPages) {
		t.Errorf("Expected %d pages, got %d", numPages, numPagesRead)
	}

	// Read and verify each page
	for pageNum := 0; pageNum < numPages; pageNum++ {
		pageID := page.NewPageDescriptor(hf.GetID(), primitives.PageNumber(pageNum))
		readPage, err := hf.ReadPage(pageID)
		if err != nil {
			t.Fatalf("Failed to read page %d: %v", pageNum, err)
		}

		heapPageRead := readPage.(*HeapPage)
		tuples := heapPageRead.GetTuples()

		if len(tuples) != tuplesPerPage {
			t.Errorf("Page %d: expected %d tuples, got %d", pageNum, tuplesPerPage, len(tuples))
		}
	}
}

func TestHeapFile_ConcurrentReads(t *testing.T) {
	td := createTestTupleDesc()
	filePath, cleanup := createTempFile(t, "test_concurrent.dat")
	defer cleanup()

	hf, err := NewHeapFile(filePath, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}
	defer hf.Close()

	// Write a page
	pageID := page.NewPageDescriptor(hf.GetID(), 0)
	pageData := make([]byte, page.PageSize)
	heapPage, err := NewHeapPage(pageID, pageData, td)
	if err != nil {
		t.Fatalf("Failed to create HeapPage: %v", err)
	}

	tup := createTestTupleForFile(td, 1, "Alice")
	err = heapPage.AddTuple(tup)
	if err != nil {
		t.Fatalf("Failed to add tuple: %v", err)
	}

	err = hf.WritePage(heapPage)
	if err != nil {
		t.Fatalf("Failed to write page: %v", err)
	}

	// Concurrent reads
	numGoroutines := 10
	done := make(chan bool, numGoroutines)
	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer func() { done <- true }()

			p, err := hf.ReadPage(pageID)
			if err != nil {
				errors <- err
				return
			}

			if p == nil {
				errors <- err
				return
			}
		}()
	}

	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	close(errors)
	for err := range errors {
		if err != nil {
			t.Errorf("Concurrent read error: %v", err)
		}
	}
}

func TestHeapFile_ValidatePageID(t *testing.T) {
	td := createTestTupleDesc()
	filePath, cleanup := createTempFile(t, "test.dat")
	defer cleanup()

	hf, err := NewHeapFile(filePath, td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}
	defer hf.Close()

	tests := []struct {
		name          string
		pageID        *page.PageDescriptor
		expectedError bool
	}{
		{
			name:          "Valid HeapPageID",
			pageID:        page.NewPageDescriptor(hf.GetID(), 0),
			expectedError: false,
		},
		{
			name:          "Nil page ID",
			pageID:        nil,
			expectedError: true,
		},
		{
			name:          "Wrong table ID",
			pageID:        page.NewPageDescriptor(hf.GetID()+1, 0),
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := hf.ReadPage(tt.pageID)

			if tt.expectedError {
				if err == nil {
					t.Error("Expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
			}
		})
	}
}
