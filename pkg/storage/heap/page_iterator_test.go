package heap

import (
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

func TestNewHeapPageIterator(t *testing.T) {
	pageID := NewHeapPageID(1, 2)
	td := mustCreateTupleDesc()
	data := make([]byte, page.PageSize)

	hp, err := NewHeapPage(pageID, data, td)
	if err != nil {
		t.Fatalf("Failed to create HeapPage: %v", err)
	}

	iterator := NewHeapPageIterator(hp)

	if iterator == nil {
		t.Fatal("NewHeapPageIterator returned nil")
	}

	if iterator.page != hp {
		t.Errorf("Expected iterator page to be %v, got %v", hp, iterator.page)
	}

	if iterator.currentSlot != -1 {
		t.Errorf("Expected currentSlot to be -1, got %d", iterator.currentSlot)
	}

	if iterator.currentIndex != -1 {
		t.Errorf("Expected currentIndex to be -1, got %d", iterator.currentIndex)
	}

	if iterator.tuples != nil {
		t.Errorf("Expected tuples to be nil initially, got %v", iterator.tuples)
	}
}

func TestHeapPageIterator_Open(t *testing.T) {
	pageID := NewHeapPageID(1, 2)
	td := mustCreateTupleDesc()
	data := make([]byte, page.PageSize)

	hp, err := NewHeapPage(pageID, data, td)
	if err != nil {
		t.Fatalf("Failed to create HeapPage: %v", err)
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

	iterator := NewHeapPageIterator(hp)

	err = iterator.Open()
	if err != nil {
		t.Errorf("Open returned error: %v", err)
	}

	if iterator.currentIndex != -1 {
		t.Errorf("Expected currentIndex to be -1 after Open, got %d", iterator.currentIndex)
	}

	if len(iterator.tuples) != 2 {
		t.Errorf("Expected 2 tuples after Open, got %d", len(iterator.tuples))
	}
}

func TestHeapPageIterator_HasNext(t *testing.T) {
	pageID := NewHeapPageID(1, 2)
	td := mustCreateTupleDesc()
	data := make([]byte, page.PageSize)

	hp, err := NewHeapPage(pageID, data, td)
	if err != nil {
		t.Fatalf("Failed to create HeapPage: %v", err)
	}

	tests := []struct {
		name            string
		setupFunc       func() *HeapPageIterator
		expectedHasNext bool
	}{
		{
			name: "Empty page",
			setupFunc: func() *HeapPageIterator {
				iterator := NewHeapPageIterator(hp)
				iterator.Open()
				return iterator
			},
			expectedHasNext: false,
		},
		{
			name: "Page with one tuple - before iteration",
			setupFunc: func() *HeapPageIterator {
				tuple1 := createTestTuple(td, 1, "Alice")
				hp.AddTuple(tuple1)
				iterator := NewHeapPageIterator(hp)
				iterator.Open()
				return iterator
			},
			expectedHasNext: true,
		},
		{
			name: "Page with one tuple - after iteration",
			setupFunc: func() *HeapPageIterator {
				iterator := NewHeapPageIterator(hp)
				iterator.Open()
				iterator.Next()
				return iterator
			},
			expectedHasNext: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hp, _ = NewHeapPage(pageID, make([]byte, page.PageSize), td)
			iterator := tt.setupFunc()

			hasNext, err := iterator.HasNext()
			if err != nil {
				t.Errorf("HasNext returned error: %v", err)
			}

			if hasNext != tt.expectedHasNext {
				t.Errorf("Expected HasNext to be %v, got %v", tt.expectedHasNext, hasNext)
			}
		})
	}
}

func TestHeapPageIterator_Next(t *testing.T) {
	pageID := NewHeapPageID(1, 2)
	td := mustCreateTupleDesc()
	data := make([]byte, page.PageSize)

	hp, err := NewHeapPage(pageID, data, td)
	if err != nil {
		t.Fatalf("Failed to create HeapPage: %v", err)
	}

	tuple1 := createTestTuple(td, 1, "Alice")
	tuple2 := createTestTuple(td, 2, "Bob")
	tuple3 := createTestTuple(td, 3, "Charlie")

	err = hp.AddTuple(tuple1)
	if err != nil {
		t.Fatalf("Failed to add tuple1: %v", err)
	}

	err = hp.AddTuple(tuple2)
	if err != nil {
		t.Fatalf("Failed to add tuple2: %v", err)
	}

	err = hp.AddTuple(tuple3)
	if err != nil {
		t.Fatalf("Failed to add tuple3: %v", err)
	}

	iterator := NewHeapPageIterator(hp)
	err = iterator.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}

	expectedTuples := 3
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

		tuple, err := iterator.Next()
		if err != nil {
			t.Errorf("Next returned error: %v", err)
			break
		}

		if tuple == nil {
			t.Errorf("Next returned nil tuple")
			break
		}

		actualCount++
	}

	if actualCount != expectedTuples {
		t.Errorf("Expected to iterate over %d tuples, got %d", expectedTuples, actualCount)
	}

	_, err = iterator.Next()
	if err == nil {
		t.Errorf("Expected error when calling Next after iteration finished")
	}
}

func TestHeapPageIterator_NextEmptyPage(t *testing.T) {
	pageID := NewHeapPageID(1, 2)
	td := mustCreateTupleDesc()
	data := make([]byte, page.PageSize)

	hp, err := NewHeapPage(pageID, data, td)
	if err != nil {
		t.Fatalf("Failed to create HeapPage: %v", err)
	}

	iterator := NewHeapPageIterator(hp)
	err = iterator.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}

	_, err = iterator.Next()
	if err == nil {
		t.Errorf("Expected error when calling Next on empty page")
	}
}

func TestHeapPageIterator_Rewind(t *testing.T) {
	pageID := NewHeapPageID(1, 2)
	td := mustCreateTupleDesc()
	data := make([]byte, page.PageSize)

	hp, err := NewHeapPage(pageID, data, td)
	if err != nil {
		t.Fatalf("Failed to create HeapPage: %v", err)
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

	iterator := NewHeapPageIterator(hp)
	err = iterator.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}

	_, err = iterator.Next()
	if err != nil {
		t.Fatalf("Failed to get first tuple: %v", err)
	}

	_, err = iterator.Next()
	if err != nil {
		t.Fatalf("Failed to get second tuple: %v", err)
	}

	hasNext, err := iterator.HasNext()
	if err != nil {
		t.Errorf("HasNext returned error: %v", err)
	}

	if hasNext {
		t.Errorf("Expected no more tuples after iterating through all")
	}

	err = iterator.Rewind()
	if err != nil {
		t.Errorf("Rewind returned error: %v", err)
	}

	hasNext, err = iterator.HasNext()
	if err != nil {
		t.Errorf("HasNext returned error after rewind: %v", err)
	}

	if !hasNext {
		t.Errorf("Expected HasNext to be true after rewind")
	}

	if iterator.currentIndex != -1 {
		t.Errorf("Expected currentIndex to be -1 after rewind, got %d", iterator.currentIndex)
	}

	count := 0
	for {
		hasNext, err := iterator.HasNext()
		if err != nil {
			t.Errorf("HasNext returned error: %v", err)
			break
		}

		if !hasNext {
			break
		}

		_, err = iterator.Next()
		if err != nil {
			t.Errorf("Next returned error: %v", err)
			break
		}

		count++
	}

	if count != 2 {
		t.Errorf("Expected to iterate over 2 tuples after rewind, got %d", count)
	}
}

func TestHeapPageIterator_Close(t *testing.T) {
	pageID := NewHeapPageID(1, 2)
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

	iterator := NewHeapPageIterator(hp)
	err = iterator.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}

	if len(iterator.tuples) == 0 {
		t.Errorf("Expected tuples to be loaded after Open")
	}

	err = iterator.Close()
	if err != nil {
		t.Errorf("Close returned error: %v", err)
	}

	if iterator.tuples != nil {
		t.Errorf("Expected tuples to be nil after Close, got %v", iterator.tuples)
	}

	if iterator.currentIndex != -1 {
		t.Errorf("Expected currentIndex to be -1 after Close, got %d", iterator.currentIndex)
	}
}

func TestHeapPageIterator_IterateTwice(t *testing.T) {
	pageID := NewHeapPageID(1, 2)
	td := mustCreateTupleDesc()
	data := make([]byte, page.PageSize)

	hp, err := NewHeapPage(pageID, data, td)
	if err != nil {
		t.Fatalf("Failed to create HeapPage: %v", err)
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

	iterator := NewHeapPageIterator(hp)
	err = iterator.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}

	firstIteration := []*tuple.Tuple{}
	for {
		hasNext, err := iterator.HasNext()
		if err != nil {
			t.Errorf("HasNext returned error: %v", err)
			break
		}

		if !hasNext {
			break
		}

		tuple, err := iterator.Next()
		if err != nil {
			t.Errorf("Next returned error: %v", err)
			break
		}

		firstIteration = append(firstIteration, tuple)
	}

	err = iterator.Rewind()
	if err != nil {
		t.Fatalf("Failed to rewind iterator: %v", err)
	}

	secondIteration := []*tuple.Tuple{}
	for {
		hasNext, err := iterator.HasNext()
		if err != nil {
			t.Errorf("HasNext returned error: %v", err)
			break
		}

		if !hasNext {
			break
		}

		tuple, err := iterator.Next()
		if err != nil {
			t.Errorf("Next returned error: %v", err)
			break
		}

		secondIteration = append(secondIteration, tuple)
	}

	if len(firstIteration) != len(secondIteration) {
		t.Errorf("Expected both iterations to have same length, got %d and %d",
			len(firstIteration), len(secondIteration))
	}

	for i := 0; i < len(firstIteration); i++ {
		if firstIteration[i] != secondIteration[i] {
			t.Errorf("Expected tuple %d to be same in both iterations", i)
		}
	}
}

func TestHeapPageIterator_ModifiedPageAfterOpen(t *testing.T) {
	pageID := NewHeapPageID(1, 2)
	td := mustCreateTupleDesc()
	data := make([]byte, page.PageSize)

	hp, err := NewHeapPage(pageID, data, td)
	if err != nil {
		t.Fatalf("Failed to create HeapPage: %v", err)
	}

	tuple1 := createTestTuple(td, 1, "Alice")
	err = hp.AddTuple(tuple1)
	if err != nil {
		t.Fatalf("Failed to add tuple1: %v", err)
	}

	iterator := NewHeapPageIterator(hp)
	err = iterator.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}

	tuple2 := createTestTuple(td, 2, "Bob")
	err = hp.AddTuple(tuple2)
	if err != nil {
		t.Fatalf("Failed to add tuple2 after opening iterator: %v", err)
	}

	count := 0
	for {
		hasNext, err := iterator.HasNext()
		if err != nil {
			t.Errorf("HasNext returned error: %v", err)
			break
		}

		if !hasNext {
			break
		}

		_, err = iterator.Next()
		if err != nil {
			t.Errorf("Next returned error: %v", err)
			break
		}

		count++
	}

	if count != 1 {
		t.Errorf("Expected iterator to see only 1 tuple (snapshot at Open time), got %d", count)
	}
}

func TestHeapPageIterator_OpenMultipleTimes(t *testing.T) {
	pageID := NewHeapPageID(1, 2)
	td := mustCreateTupleDesc()
	data := make([]byte, page.PageSize)

	hp, err := NewHeapPage(pageID, data, td)
	if err != nil {
		t.Fatalf("Failed to create HeapPage: %v", err)
	}

	tuple1 := createTestTuple(td, 1, "Alice")
	err = hp.AddTuple(tuple1)
	if err != nil {
		t.Fatalf("Failed to add tuple1: %v", err)
	}

	iterator := NewHeapPageIterator(hp)

	err = iterator.Open()
	if err != nil {
		t.Errorf("First Open returned error: %v", err)
	}

	tuple2 := createTestTuple(td, 2, "Bob")
	err = hp.AddTuple(tuple2)
	if err != nil {
		t.Fatalf("Failed to add tuple2: %v", err)
	}

	err = iterator.Open()
	if err != nil {
		t.Errorf("Second Open returned error: %v", err)
	}

	count := 0
	for {
		hasNext, err := iterator.HasNext()
		if err != nil {
			t.Errorf("HasNext returned error: %v", err)
			break
		}

		if !hasNext {
			break
		}

		_, err = iterator.Next()
		if err != nil {
			t.Errorf("Next returned error: %v", err)
			break
		}

		count++
	}

	if count != 2 {
		t.Errorf("Expected iterator to see 2 tuples after second Open, got %d", count)
	}
}

func TestHeapPageIterator_TupleOrder(t *testing.T) {
	pageID := NewHeapPageID(1, 2)
	td := mustCreateTupleDesc()
	data := make([]byte, page.PageSize)

	hp, err := NewHeapPage(pageID, data, td)
	if err != nil {
		t.Fatalf("Failed to create HeapPage: %v", err)
	}

	expectedValues := []int32{10, 20, 30}
	expectedNames := []string{"First", "Second", "Third"}

	for i, val := range expectedValues {
		tuple := createTestTuple(td, val, expectedNames[i])
		err = hp.AddTuple(tuple)
		if err != nil {
			t.Fatalf("Failed to add tuple %d: %v", i, err)
		}
	}

	iterator := NewHeapPageIterator(hp)
	err = iterator.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}

	actualValues := []int32{}
	actualNames := []string{}

	for {
		hasNext, err := iterator.HasNext()
		if err != nil {
			t.Errorf("HasNext returned error: %v", err)
			break
		}

		if !hasNext {
			break
		}

		tuple, err := iterator.Next()
		if err != nil {
			t.Errorf("Next returned error: %v", err)
			break
		}

		idField, err := tuple.GetField(0)
		if err != nil {
			t.Errorf("Failed to get id field: %v", err)
			continue
		}

		nameField, err := tuple.GetField(1)
		if err != nil {
			t.Errorf("Failed to get name field: %v", err)
			continue
		}

		intField, ok := idField.(*types.IntField)
		if !ok {
			t.Errorf("Expected IntField, got %T", idField)
			continue
		}

		stringField, ok := nameField.(*types.StringField)
		if !ok {
			t.Errorf("Expected StringField, got %T", nameField)
			continue
		}

		actualValues = append(actualValues, intField.Value)
		actualNames = append(actualNames, stringField.Value)
	}

	if len(actualValues) != len(expectedValues) {
		t.Fatalf("Expected %d values, got %d", len(expectedValues), len(actualValues))
	}

	for i, expected := range expectedValues {
		if actualValues[i] != expected {
			t.Errorf("Expected value %d at position %d, got %d", expected, i, actualValues[i])
		}
	}

	for i, expected := range expectedNames {
		if actualNames[i] != expected {
			t.Errorf("Expected name %s at position %d, got %s", expected, i, actualNames[i])
		}
	}
}