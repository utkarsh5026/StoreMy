package query

import (
	"fmt"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

func TestNewBaseIterator(t *testing.T) {
	readFunc := func() (*tuple.Tuple, error) {
		return nil, nil
	}

	iterator := NewBaseIterator(readFunc)

	if iterator == nil {
		t.Fatal("NewBaseIterator returned nil")
	}

	if iterator.readNextFunc == nil {
		t.Error("Expected readNextFunc to be set")
	}

	if iterator.opened {
		t.Error("Expected iterator to start in closed state")
	}

	if iterator.nextTuple != nil {
		t.Error("Expected nextTuple to be nil initially")
	}
}

func TestBaseIterator_MarkOpened(t *testing.T) {
	readFunc := func() (*tuple.Tuple, error) {
		return nil, nil
	}

	iterator := NewBaseIterator(readFunc)

	iterator.MarkOpened()

	if !iterator.opened {
		t.Error("Expected iterator to be marked as opened")
	}

	if iterator.nextTuple != nil {
		t.Error("Expected nextTuple to be nil after MarkOpened")
	}
}

func TestBaseIterator_HasNext_NotOpened(t *testing.T) {
	readFunc := func() (*tuple.Tuple, error) {
		return nil, nil
	}

	iterator := NewBaseIterator(readFunc)

	hasNext, err := iterator.HasNext()
	if err == nil {
		t.Error("Expected error when calling HasNext on unopened iterator")
	}

	if hasNext {
		t.Error("Expected HasNext to return false when error occurs")
	}
}

func TestBaseIterator_HasNext_WithTuples(t *testing.T) {
	td := mustCreateTupleDesc()
	testTuple := createTestTuple(td, 1, "test")
	callCount := 0

	readFunc := func() (*tuple.Tuple, error) {
		callCount++
		if callCount == 1 {
			return testTuple, nil
		}
		return nil, nil // No more tuples
	}

	iterator := NewBaseIterator(readFunc)
	iterator.MarkOpened()

	// First call should return true and cache the tuple
	hasNext, err := iterator.HasNext()
	if err != nil {
		t.Errorf("HasNext returned error: %v", err)
	}
	if !hasNext {
		t.Error("Expected HasNext to return true when tuple is available")
	}
	if callCount != 1 {
		t.Errorf("Expected readFunc to be called once, got %d", callCount)
	}

	// Second call should return true without calling readFunc again (cached)
	hasNext, err = iterator.HasNext()
	if err != nil {
		t.Errorf("HasNext returned error: %v", err)
	}
	if !hasNext {
		t.Error("Expected HasNext to return true when tuple is cached")
	}
	if callCount != 1 {
		t.Errorf("Expected readFunc to not be called again, got %d calls", callCount)
	}
}

func TestBaseIterator_HasNext_NoTuples(t *testing.T) {
	readFunc := func() (*tuple.Tuple, error) {
		return nil, nil // No tuples available
	}

	iterator := NewBaseIterator(readFunc)
	iterator.MarkOpened()

	hasNext, err := iterator.HasNext()
	if err != nil {
		t.Errorf("HasNext returned error: %v", err)
	}
	if hasNext {
		t.Error("Expected HasNext to return false when no tuples available")
	}
}

func TestBaseIterator_HasNext_ReadError(t *testing.T) {
	expectedError := fmt.Errorf("read error")
	readFunc := func() (*tuple.Tuple, error) {
		return nil, expectedError
	}

	iterator := NewBaseIterator(readFunc)
	iterator.MarkOpened()

	hasNext, err := iterator.HasNext()
	if err != expectedError {
		t.Errorf("Expected error %v, got %v", expectedError, err)
	}
	if hasNext {
		t.Error("Expected HasNext to return false when read error occurs")
	}
}

func TestBaseIterator_Next_NotOpened(t *testing.T) {
	readFunc := func() (*tuple.Tuple, error) {
		return nil, nil
	}

	iterator := NewBaseIterator(readFunc)

	tuple, err := iterator.Next()
	if err == nil {
		t.Error("Expected error when calling Next on unopened iterator")
	}
	if tuple != nil {
		t.Error("Expected nil tuple when error occurs")
	}
}

func TestBaseIterator_Next_WithCachedTuple(t *testing.T) {
	td := mustCreateTupleDesc()
	testTuple := createTestTuple(td, 1, "test")
	callCount := 0

	readFunc := func() (*tuple.Tuple, error) {
		callCount++
		if callCount == 1 {
			return testTuple, nil
		}
		return nil, nil
	}

	iterator := NewBaseIterator(readFunc)
	iterator.MarkOpened()

	// Cache the tuple first
	iterator.HasNext()

	// Now call Next - should return cached tuple
	tuple, err := iterator.Next()
	if err != nil {
		t.Errorf("Next returned error: %v", err)
	}
	if tuple != testTuple {
		t.Errorf("Expected tuple %v, got %v", testTuple, tuple)
	}
	if callCount != 1 {
		t.Errorf("Expected readFunc to be called once, got %d", callCount)
	}

	// Cache should be cleared
	if iterator.nextTuple != nil {
		t.Error("Expected nextTuple cache to be cleared after Next")
	}
}

func TestBaseIterator_Next_WithoutCache(t *testing.T) {
	td := mustCreateTupleDesc()
	testTuple := createTestTuple(td, 1, "test")
	callCount := 0

	readFunc := func() (*tuple.Tuple, error) {
		callCount++
		if callCount == 1 {
			return testTuple, nil
		}
		return nil, nil
	}

	iterator := NewBaseIterator(readFunc)
	iterator.MarkOpened()

	// Call Next without calling HasNext first
	tuple, err := iterator.Next()
	if err != nil {
		t.Errorf("Next returned error: %v", err)
	}
	if tuple != testTuple {
		t.Errorf("Expected tuple %v, got %v", testTuple, tuple)
	}
	if callCount != 1 {
		t.Errorf("Expected readFunc to be called once, got %d", callCount)
	}
}

func TestBaseIterator_Next_NoTuples(t *testing.T) {
	readFunc := func() (*tuple.Tuple, error) {
		return nil, nil // No tuples available
	}

	iterator := NewBaseIterator(readFunc)
	iterator.MarkOpened()

	tuple, err := iterator.Next()
	if err == nil {
		t.Error("Expected error when no tuples available")
	}
	if tuple != nil {
		t.Error("Expected nil tuple when no tuples available")
	}
}

func TestBaseIterator_Next_ReadError(t *testing.T) {
	expectedError := fmt.Errorf("read error")
	readFunc := func() (*tuple.Tuple, error) {
		return nil, expectedError
	}

	iterator := NewBaseIterator(readFunc)
	iterator.MarkOpened()

	tuple, err := iterator.Next()
	if err != expectedError {
		t.Errorf("Expected error %v, got %v", expectedError, err)
	}
	if tuple != nil {
		t.Error("Expected nil tuple when read error occurs")
	}
}

func TestBaseIterator_Close(t *testing.T) {
	td := mustCreateTupleDesc()
	testTuple := createTestTuple(td, 1, "test")

	readFunc := func() (*tuple.Tuple, error) {
		return testTuple, nil
	}

	iterator := NewBaseIterator(readFunc)
	iterator.MarkOpened()

	// Cache a tuple first
	iterator.HasNext()
	if iterator.nextTuple == nil {
		t.Error("Expected tuple to be cached")
	}

	// Close the iterator
	err := iterator.Close()
	if err != nil {
		t.Errorf("Close returned error: %v", err)
	}

	if iterator.opened {
		t.Error("Expected iterator to be marked as closed")
	}

	if iterator.nextTuple != nil {
		t.Error("Expected cached tuple to be cleared")
	}
}

func TestBaseIterator_FullIteration(t *testing.T) {
	td := mustCreateTupleDesc()
	tuples := []*tuple.Tuple{
		createTestTuple(td, 1, "first"),
		createTestTuple(td, 2, "second"),
		createTestTuple(td, 3, "third"),
	}
	index := 0

	readFunc := func() (*tuple.Tuple, error) {
		if index >= len(tuples) {
			return nil, nil // No more tuples
		}
		tuple := tuples[index]
		index++
		return tuple, nil
	}

	iterator := NewBaseIterator(readFunc)
	iterator.MarkOpened()

	var retrievedTuples []*tuple.Tuple

	// Iterate using HasNext/Next pattern
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

		retrievedTuples = append(retrievedTuples, tuple)
	}

	if len(retrievedTuples) != len(tuples) {
		t.Errorf("Expected %d tuples, got %d", len(tuples), len(retrievedTuples))
	}

	for i, expectedTuple := range tuples {
		if i >= len(retrievedTuples) {
			t.Errorf("Missing tuple at index %d", i)
			continue
		}
		if retrievedTuples[i] != expectedTuple {
			t.Errorf("Expected tuple %v at index %d, got %v", expectedTuple, i, retrievedTuples[i])
		}
	}

	// Try to get next tuple after iteration is complete
	_, err := iterator.Next()
	if err == nil {
		t.Error("Expected error when calling Next after iteration is complete")
	}
}

func TestBaseIterator_AlternatingPattern(t *testing.T) {
	td := mustCreateTupleDesc()
	testTuple := createTestTuple(td, 1, "test")
	callCount := 0

	readFunc := func() (*tuple.Tuple, error) {
		callCount++
		if callCount == 1 {
			return testTuple, nil
		}
		return nil, nil
	}

	iterator := NewBaseIterator(readFunc)
	iterator.MarkOpened()

	// Call HasNext multiple times
	for i := 0; i < 3; i++ {
		hasNext, err := iterator.HasNext()
		if err != nil {
			t.Errorf("HasNext call %d returned error: %v", i+1, err)
		}
		if !hasNext {
			t.Errorf("HasNext call %d returned false", i+1)
		}
	}

	// Should only call readFunc once due to caching
	if callCount != 1 {
		t.Errorf("Expected readFunc to be called once, got %d", callCount)
	}

	// Get the tuple
	tuple, err := iterator.Next()
	if err != nil {
		t.Errorf("Next returned error: %v", err)
	}
	if tuple != testTuple {
		t.Errorf("Expected tuple %v, got %v", testTuple, tuple)
	}

	// Now HasNext should return false
	hasNext, err := iterator.HasNext()
	if err != nil {
		t.Errorf("HasNext after iteration returned error: %v", err)
	}
	if hasNext {
		t.Error("Expected HasNext to return false after iteration complete")
	}
}

func TestBaseIterator_ReopenAfterClose(t *testing.T) {
	td := mustCreateTupleDesc()
	testTuple := createTestTuple(td, 1, "test")
	callCount := 0

	readFunc := func() (*tuple.Tuple, error) {
		callCount++
		if callCount <= 2 {
			return testTuple, nil
		}
		return nil, nil
	}

	iterator := NewBaseIterator(readFunc)
	iterator.MarkOpened()

	// Get first tuple
	tuple1, err := iterator.Next()
	if err != nil {
		t.Errorf("First Next returned error: %v", err)
	}
	if tuple1 != testTuple {
		t.Errorf("Expected first tuple %v, got %v", testTuple, tuple1)
	}

	// Close iterator
	iterator.Close()

	// Reopen iterator
	iterator.MarkOpened()

	// Get second tuple
	tuple2, err := iterator.Next()
	if err != nil {
		t.Errorf("Second Next returned error: %v", err)
	}
	if tuple2 != testTuple {
		t.Errorf("Expected second tuple %v, got %v", testTuple, tuple2)
	}

	if callCount != 2 {
		t.Errorf("Expected readFunc to be called twice, got %d", callCount)
	}
}

// Helper functions

func mustCreateTupleDesc() *tuple.TupleDescription {
	td, err := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType},
		[]string{"id", "name"},
	)
	if err != nil {
		panic(fmt.Sprintf("Failed to create TupleDescription: %v", err))
	}
	return td
}

func createTestTuple(td *tuple.TupleDescription, id int64, name string) *tuple.Tuple {
	t := tuple.NewTuple(td)
	intField := types.NewIntField(id)
	stringField := types.NewStringField(name, 128)

	err := t.SetField(0, intField)
	if err != nil {
		panic(fmt.Sprintf("Failed to set int field: %v", err))
	}

	err = t.SetField(1, stringField)
	if err != nil {
		panic(fmt.Sprintf("Failed to set string field: %v", err))
	}

	return t
}
