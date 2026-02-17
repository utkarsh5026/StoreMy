package heap

import (
	"bytes"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

func TestNewHeapPage(t *testing.T) {
	pageID := page.NewPageDescriptor(1, 2)
	td := mustCreateTupleDesc()
	validData := make([]byte, page.PageSize)

	tests := []struct {
		name          string
		pageID        *page.PageDescriptor
		data          []byte
		tupleDesc     *tuple.TupleDescription
		expectedError bool
	}{
		{
			name:          "Valid page creation",
			pageID:        pageID,
			data:          validData,
			tupleDesc:     td,
			expectedError: false,
		},
		{
			name:          "Invalid data size - too small",
			pageID:        pageID,
			data:          make([]byte, page.PageSize-1),
			tupleDesc:     td,
			expectedError: true,
		},
		{
			name:          "Invalid data size - too large",
			pageID:        pageID,
			data:          make([]byte, page.PageSize+1),
			tupleDesc:     td,
			expectedError: true,
		},
		{
			name:          "Empty data",
			pageID:        pageID,
			data:          []byte{},
			tupleDesc:     td,
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hp, err := NewHeapPage(tt.pageID, tt.data, tt.tupleDesc)

			if tt.expectedError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if hp == nil {
				t.Fatal("NewHeapPage returned nil")
			}

			if hp.pageID != tt.pageID {
				t.Errorf("Expected pageID %v, got %v", tt.pageID, hp.pageID)
			}

			if hp.tupleDesc != tt.tupleDesc {
				t.Errorf("Expected tupleDesc %v, got %v", tt.tupleDesc, hp.tupleDesc)
			}

			if hp.numSlots <= 0 {
				t.Errorf("Expected positive numSlots, got %d", hp.numSlots)
			}

			if len(hp.slotPointers) == 0 {
				t.Errorf("Expected non-empty slot pointers array")
			}

			if len(hp.tuples) != int(hp.numSlots) {
				t.Errorf("Expected tuples slice length %d, got %d", hp.numSlots, len(hp.tuples))
			}

			if len(hp.oldData) != page.PageSize {
				t.Errorf("Expected oldData size %d, got %d", page.PageSize, len(hp.oldData))
			}
		})
	}
}

func TestHeapPage_GetID(t *testing.T) {
	pageID := page.NewPageDescriptor(5, 10)
	td := mustCreateTupleDesc()
	data := make([]byte, page.PageSize)

	hp, err := NewHeapPage(pageID, data, td)
	if err != nil {
		t.Fatalf("Failed to create HeapPage: %v", err)
	}

	retrievedID := hp.GetID()
	if !retrievedID.Equals(pageID) {
		t.Errorf("Expected ID %v, got %v", pageID, retrievedID)
	}
}

func TestHeapPage_IsDirty_MarkDirty(t *testing.T) {
	pageID := page.NewPageDescriptor(1, 2)
	td := mustCreateTupleDesc()
	data := make([]byte, page.PageSize)

	hp, err := NewHeapPage(pageID, data, td)
	if err != nil {
		t.Fatalf("Failed to create HeapPage: %v", err)
	}

	if hp.IsDirty() != nil {
		t.Errorf("Expected clean page, but got dirty transaction: %v", hp.IsDirty())
	}

	tid := &primitives.TransactionID{}
	hp.MarkDirty(true, tid)

	if hp.IsDirty() != tid {
		t.Errorf("Expected dirty transaction %v, got %v", tid, hp.IsDirty())
	}

	hp.MarkDirty(false, nil)
	if hp.IsDirty() != nil {
		t.Errorf("Expected clean page after marking clean, got dirty transaction: %v", hp.IsDirty())
	}
}

func TestHeapPage_GetNumEmptySlots(t *testing.T) {
	pageID := page.NewPageDescriptor(1, 2)
	td := mustCreateTupleDesc()
	data := make([]byte, page.PageSize)

	hp, err := NewHeapPage(pageID, data, td)
	if err != nil {
		t.Fatalf("Failed to create HeapPage: %v", err)
	}

	initialEmpty := hp.GetNumEmptySlots()
	if initialEmpty != hp.numSlots {
		t.Errorf("Expected all slots to be empty initially, got %d empty out of %d total", initialEmpty, hp.numSlots)
	}

	tuple1 := createTestTuple(td, 1, "Alice")
	err = hp.AddTuple(tuple1)
	if err != nil {
		t.Fatalf("Failed to add tuple: %v", err)
	}

	emptyAfterAdd := hp.GetNumEmptySlots()
	if emptyAfterAdd != initialEmpty-1 {
		t.Errorf("Expected %d empty slots after adding one tuple, got %d", initialEmpty-1, emptyAfterAdd)
	}

	err = hp.DeleteTuple(tuple1)
	if err != nil {
		t.Fatalf("Failed to delete tuple: %v", err)
	}

	emptyAfterDelete := hp.GetNumEmptySlots()
	if emptyAfterDelete != initialEmpty {
		t.Errorf("Expected %d empty slots after deleting tuple, got %d", initialEmpty, emptyAfterDelete)
	}
}

func TestHeapPage_AddTuple(t *testing.T) {
	pageID := page.NewPageDescriptor(1, 2)
	td := mustCreateTupleDesc()
	data := make([]byte, page.PageSize)

	hp, err := NewHeapPage(pageID, data, td)
	if err != nil {
		t.Fatalf("Failed to create HeapPage: %v", err)
	}

	tests := []struct {
		name          string
		setupFunc     func() *tuple.Tuple
		expectedError bool
		errorMsg      string
	}{
		{
			name: "Valid tuple addition",
			setupFunc: func() *tuple.Tuple {
				return createTestTuple(td, 1, "Alice")
			},
			expectedError: false,
		},
		{
			name: "Tuple with wrong schema",
			setupFunc: func() *tuple.Tuple {
				wrongTd := mustCreateTupleDesc2()
				return createTestTuple(wrongTd, 1, "Bob")
			},
			expectedError: true,
			errorMsg:      "schema does not match",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testTuple := tt.setupFunc()
			initialEmpty := hp.GetNumEmptySlots()

			err := hp.AddTuple(testTuple)

			if tt.expectedError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if hp.GetNumEmptySlots() != initialEmpty-1 {
				t.Errorf("Expected empty slots to decrease by 1")
			}

			if testTuple.RecordID == nil {
				t.Errorf("Expected tuple to have RecordID after addition")
			}

			if testTuple.RecordID != nil && !testTuple.RecordID.PageID.Equals(pageID) {
				t.Errorf("Expected tuple RecordID to have correct page ID")
			}
		})
	}
}

func TestHeapPage_DeleteTuple(t *testing.T) {
	pageID := page.NewPageDescriptor(1, 2)
	td := mustCreateTupleDesc()
	data := make([]byte, page.PageSize)

	hp, err := NewHeapPage(pageID, data, td)
	if err != nil {
		t.Fatalf("Failed to create HeapPage: %v", err)
	}

	tuple1 := createTestTuple(td, 1, "Alice")
	err = hp.AddTuple(tuple1)
	if err != nil {
		t.Fatalf("Failed to add tuple for deletion test: %v", err)
	}

	tests := []struct {
		name          string
		tuple         *tuple.Tuple
		expectedError bool
		errorMsg      string
	}{
		{
			name:          "Valid tuple deletion",
			tuple:         tuple1,
			expectedError: false,
		},
		{
			name: "Tuple with no RecordID",
			tuple: func() *tuple.Tuple {
				t := createTestTuple(td, 2, "Bob")
				t.RecordID = nil
				return t
			}(),
			expectedError: true,
			errorMsg:      "no record ID",
		},
		{
			name: "Tuple from different page",
			tuple: func() *tuple.Tuple {
				t := createTestTuple(td, 3, "Charlie")
				differentPageID := page.NewPageDescriptor(2, 1)
				t.RecordID = tuple.NewTupleRecordID(differentPageID, 0)
				return t
			}(),
			expectedError: true,
			errorMsg:      "not on this page",
		},
		{
			name: "Invalid slot index",
			tuple: func() *tuple.Tuple {
				t := createTestTuple(td, 4, "David")
				t.RecordID = tuple.NewTupleRecordID(pageID, 9999)
				return t
			}(),
			expectedError: true,
			errorMsg:      "invalid tuple slot",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			initialEmpty := hp.GetNumEmptySlots()

			err := hp.DeleteTuple(tt.tuple)

			if tt.expectedError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if hp.GetNumEmptySlots() != initialEmpty+1 {
				t.Errorf("Expected empty slots to increase by 1")
			}

			if tt.tuple.RecordID != nil {
				t.Errorf("Expected tuple RecordID to be nil after deletion")
			}
		})
	}
}

func TestHeapPage_GetTuples(t *testing.T) {
	pageID := page.NewPageDescriptor(1, 2)
	td := mustCreateTupleDesc()
	data := make([]byte, page.PageSize)

	hp, err := NewHeapPage(pageID, data, td)
	if err != nil {
		t.Fatalf("Failed to create HeapPage: %v", err)
	}

	initialTuples := hp.GetTuples()
	if len(initialTuples) != 0 {
		t.Errorf("Expected 0 tuples initially, got %d", len(initialTuples))
	}

	tuple1 := createTestTuple(td, 1, "Alice")
	tuple2 := createTestTuple(td, 2, "Bob")

	err = hp.AddTuple(tuple1)
	if err != nil {
		t.Fatalf("Failed to add tuple1: %v", err)
	}

	err = hp.AddTuple(tuple2)
	if err != nil {
		t.Fatalf("Failed to add tuple2: %v", err)
	}

	allTuples := hp.GetTuples()
	if len(allTuples) != 2 {
		t.Errorf("Expected 2 tuples after adding two, got %d", len(allTuples))
	}

	for _, tup := range allTuples {
		if tup == nil {
			t.Errorf("GetTuples returned nil tuple")
		}
	}

	err = hp.DeleteTuple(tuple1)
	if err != nil {
		t.Fatalf("Failed to delete tuple1: %v", err)
	}

	remainingTuples := hp.GetTuples()
	if len(remainingTuples) != 1 {
		t.Errorf("Expected 1 tuple after deleting one, got %d", len(remainingTuples))
	}
}

func TestHeapPage_GetTupleAt(t *testing.T) {
	pageID := page.NewPageDescriptor(1, 2)
	td := mustCreateTupleDesc()
	data := make([]byte, page.PageSize)

	hp, err := NewHeapPage(pageID, data, td)
	if err != nil {
		t.Fatalf("Failed to create HeapPage: %v", err)
	}

	tests := []struct {
		name          string
		index         primitives.SlotID
		expectedError bool
		expectNil     bool
	}{
		{
			name:          "Valid index - empty slot",
			index:         0,
			expectedError: false,
			expectNil:     true,
		},
		{
			name:          "Index out of bounds",
			index:         hp.numSlots,
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tup, err := hp.GetTupleAt(tt.index)

			if tt.expectedError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if tt.expectNil && tup != nil {
				t.Errorf("Expected nil tuple, got %v", tup)
			}
		})
	}

	tuple1 := createTestTuple(td, 1, "Alice")
	err = hp.AddTuple(tuple1)
	if err != nil {
		t.Fatalf("Failed to add tuple: %v", err)
	}

	slotIndex := tuple1.RecordID.TupleNum
	retrievedTuple, err := hp.GetTupleAt(slotIndex)
	if err != nil {
		t.Errorf("Unexpected error retrieving tuple: %v", err)
	}

	if retrievedTuple != tuple1 {
		t.Errorf("Expected retrieved tuple to be the same as added tuple")
	}
}

func TestHeapPage_GetPageData(t *testing.T) {
	pageID := page.NewPageDescriptor(1, 2)
	td := mustCreateTupleDesc()
	data := make([]byte, page.PageSize)

	hp, err := NewHeapPage(pageID, data, td)
	if err != nil {
		t.Fatalf("Failed to create HeapPage: %v", err)
	}

	pageData := hp.GetPageData()
	if len(pageData) != page.PageSize {
		t.Errorf("Expected page data size %d, got %d", page.PageSize, len(pageData))
	}

	tuple1 := createTestTuple(td, 1, "Alice")
	err = hp.AddTuple(tuple1)
	if err != nil {
		t.Fatalf("Failed to add tuple: %v", err)
	}

	pageDataWithTuple := hp.GetPageData()
	if len(pageDataWithTuple) != page.PageSize {
		t.Errorf("Expected page data size %d after adding tuple, got %d", page.PageSize, len(pageDataWithTuple))
	}

	if bytes.Equal(pageData, pageDataWithTuple) {
		t.Errorf("Expected page data to change after adding tuple")
	}
}

func TestHeapPage_GetBeforeImage_SetBeforeImage(t *testing.T) {
	pageID := page.NewPageDescriptor(1, 2)
	td := mustCreateTupleDesc()
	data := make([]byte, page.PageSize)

	hp, err := NewHeapPage(pageID, data, td)
	if err != nil {
		t.Fatalf("Failed to create HeapPage: %v", err)
	}

	beforePage := hp.GetBeforeImage()
	if beforePage == nil {
		t.Fatal("GetBeforeImage returned nil")
	}

	tuple1 := createTestTuple(td, 1, "Alice")
	err = hp.AddTuple(tuple1)
	if err != nil {
		t.Fatalf("Failed to add tuple: %v", err)
	}

	hp.SetBeforeImage()

	beforePageAfterSet := hp.GetBeforeImage()
	if beforePageAfterSet == nil {
		t.Fatal("GetBeforeImage returned nil after SetBeforeImage")
	}

	beforeTuples := beforePageAfterSet.(*HeapPage).GetTuples()
	if len(beforeTuples) != 1 {
		t.Errorf("Expected 1 tuple in before image, got %d", len(beforeTuples))
	}
}

func TestHeapPage_AddTuple_FillPage(t *testing.T) {
	pageID := page.NewPageDescriptor(1, 2)
	td := mustCreateTupleDesc()
	data := make([]byte, page.PageSize)

	hp, err := NewHeapPage(pageID, data, td)
	if err != nil {
		t.Fatalf("Failed to create HeapPage: %v", err)
	}

	maxTuples := hp.numSlots
	var i primitives.SlotID
	for i = 0; i < maxTuples; i++ {
		tuple := createTestTuple(td, int64(i), "User")
		err := hp.AddTuple(tuple)
		if err != nil {
			t.Fatalf("Failed to add tuple %d: %v", i, err)
		}
	}

	if hp.GetNumEmptySlots() != 0 {
		t.Errorf("Expected 0 empty slots after filling page, got %d", hp.GetNumEmptySlots())
	}

	overflowTuple := createTestTuple(td, int64(maxTuples), "Overflow")
	err = hp.AddTuple(overflowTuple)
	if err == nil {
		t.Errorf("Expected error when adding tuple to full page")
	}
}

func TestHeapPage_DeleteTuple_AlreadyEmpty(t *testing.T) {
	pageID := page.NewPageDescriptor(1, 2)
	td := mustCreateTupleDesc()
	data := make([]byte, page.PageSize)

	hp, err := NewHeapPage(pageID, data, td)
	if err != nil {
		t.Fatalf("Failed to create HeapPage: %v", err)
	}

	tuple1 := createTestTuple(td, 1, "Alice")
	err = hp.AddTuple(tuple1)
	if err != nil {
		t.Fatalf("Failed to add tuple: %v", err)
	}

	err = hp.DeleteTuple(tuple1)
	if err != nil {
		t.Fatalf("Failed to delete tuple first time: %v", err)
	}

	err = hp.DeleteTuple(tuple1)
	if err == nil {
		t.Errorf("Expected error when deleting already deleted tuple")
	}
}

func TestHeapPage_ConcurrentAccess(t *testing.T) {
	pageID := page.NewPageDescriptor(1, 2)
	td := mustCreateTupleDesc()
	data := make([]byte, page.PageSize)

	hp, err := NewHeapPage(pageID, data, td)
	if err != nil {
		t.Fatalf("Failed to create HeapPage: %v", err)
	}

	tuple1 := createTestTuple(td, 1, "Alice")
	err = hp.AddTuple(tuple1)
	if err != nil {
		t.Fatalf("Failed to add tuple: %v", err)
	}

	numGoroutines := 10
	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer func() { done <- true }()

			_ = hp.GetNumEmptySlots()

			isDirty := hp.IsDirty()
			_ = isDirty
		}()
	}

	for i := 0; i < numGoroutines; i++ {
		<-done
	}
}

func mustCreateTupleDesc2() *tuple.TupleDescription {
	types := []types.Type{types.StringType, types.StringType, types.IntType}
	fields := []string{"first", "last", "age"}
	td, err := tuple.NewTupleDesc(types, fields)
	if err != nil {
		panic(err)
	}
	return td
}
