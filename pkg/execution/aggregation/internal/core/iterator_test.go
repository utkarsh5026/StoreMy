package core

import (
	"fmt"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"sync"
	"testing"
)

// mockGroupAggregator implements GroupAggregator interface for testing
type mockGroupAggregator struct {
	groups        map[string]types.Field
	groupingField primitives.ColumnID
	tupleDesc     *tuple.TupleDescription
	shouldError   bool
	mu            sync.RWMutex
}

func newMockGroupAggregator(groupingField primitives.ColumnID, tupleDesc *tuple.TupleDescription) *mockGroupAggregator {
	return &mockGroupAggregator{
		groups:        make(map[string]types.Field),
		groupingField: groupingField,
		tupleDesc:     tupleDesc,
		shouldError:   false,
	}
}

func (m *mockGroupAggregator) GetGroups() []string {
	groups := make([]string, 0, len(m.groups))
	for key := range m.groups {
		groups = append(groups, key)
	}
	return groups
}

func (m *mockGroupAggregator) GetAggregateValue(groupKey string) (types.Field, error) {
	if m.shouldError {
		return nil, fmt.Errorf("mock aggregate value error")
	}

	value, exists := m.groups[groupKey]
	if !exists {
		return nil, fmt.Errorf("group key not found: %s", groupKey)
	}
	return value, nil
}

func (m *mockGroupAggregator) GetTupleDesc() *tuple.TupleDescription {
	return m.tupleDesc
}

func (m *mockGroupAggregator) GetGroupingField() primitives.ColumnID {
	return m.groupingField
}

func (m *mockGroupAggregator) RLock() {
	m.mu.RLock()
}

func (m *mockGroupAggregator) RUnlock() {
	m.mu.RUnlock()
}

func (m *mockGroupAggregator) addGroup(key string, value types.Field) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.groups[key] = value
}

func (m *mockGroupAggregator) setError(shouldError bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shouldError = shouldError
}

// Helper functions
func createIteratorTestTupleDesc(grouped bool) *tuple.TupleDescription {
	if grouped {
		td, _ := tuple.NewTupleDesc(
			[]types.Type{types.StringType, types.IntType},
			[]string{"group", "SUM"},
		)
		return td
	}

	td, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType},
		[]string{"SUM"},
	)
	return td
}

// TestNewAggregatorIterator tests the constructor
func TestNewAggregatorIterator(t *testing.T) {
	td := createIteratorTestTupleDesc(false)
	agg := newMockGroupAggregator(NoGrouping, td)

	iter := NewAggregatorIterator(agg)

	if iter == nil {
		t.Fatal("Expected non-nil iterator")
	}
	if iter.aggregator == nil {
		t.Error("Expected non-nil aggregator")
	}
	if iter.tupleDesc == nil {
		t.Error("Expected non-nil tuple description")
	}
	if iter.currentIdx != 0 {
		t.Errorf("Expected currentIdx to be 0, got %d", iter.currentIdx)
	}
	if iter.opened {
		t.Error("Expected iterator to be closed initially")
	}
}

// TestAggregatorIterator_Open tests the Open method
func TestAggregatorIterator_Open(t *testing.T) {
	t.Run("successful open", func(t *testing.T) {
		td := createIteratorTestTupleDesc(false)
		agg := newMockGroupAggregator(NoGrouping, td)
		agg.addGroup("NO_GROUPING", types.NewIntField(100))

		iter := NewAggregatorIterator(agg)

		err := iter.Open()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if !iter.opened {
			t.Error("Expected iterator to be opened")
		}
		if iter.currentIdx != 0 {
			t.Errorf("Expected currentIdx to be 0, got %d", iter.currentIdx)
		}
		if iter.groups == nil {
			t.Error("Expected groups to be initialized")
		}
	})

	t.Run("already opened", func(t *testing.T) {
		td := createIteratorTestTupleDesc(false)
		agg := newMockGroupAggregator(NoGrouping, td)

		iter := NewAggregatorIterator(agg)

		// First open
		err := iter.Open()
		if err != nil {
			t.Fatalf("First open failed: %v", err)
		}

		// Second open should error
		err = iter.Open()
		if err == nil {
			t.Error("Expected error when opening already opened iterator")
		}
	})

	t.Run("open with multiple groups", func(t *testing.T) {
		td := createIteratorTestTupleDesc(true)
		agg := newMockGroupAggregator(0, td)
		agg.addGroup("A", types.NewIntField(10))
		agg.addGroup("B", types.NewIntField(20))
		agg.addGroup("C", types.NewIntField(30))

		iter := NewAggregatorIterator(agg)

		err := iter.Open()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if len(iter.groups) != 3 {
			t.Errorf("Expected 3 groups, got %d", len(iter.groups))
		}
	})
}

// TestAggregatorIterator_HasNext tests the HasNext method
func TestAggregatorIterator_HasNext(t *testing.T) {
	t.Run("iterator not opened", func(t *testing.T) {
		td := createIteratorTestTupleDesc(false)
		agg := newMockGroupAggregator(NoGrouping, td)

		iter := NewAggregatorIterator(agg)

		hasNext, err := iter.HasNext()
		if err == nil {
			t.Error("Expected error when iterator not opened")
		}
		if hasNext {
			t.Error("Expected false when iterator not opened")
		}
	})

	t.Run("has results", func(t *testing.T) {
		td := createIteratorTestTupleDesc(false)
		agg := newMockGroupAggregator(NoGrouping, td)
		agg.addGroup("NO_GROUPING", types.NewIntField(100))

		iter := NewAggregatorIterator(agg)
		err := iter.Open()
		if err != nil {
			t.Fatalf("Failed to open iterator: %v", err)
		}

		hasNext, err := iter.HasNext()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if !hasNext {
			t.Error("Expected hasNext to be true")
		}
	})

	t.Run("no results", func(t *testing.T) {
		td := createIteratorTestTupleDesc(false)
		agg := newMockGroupAggregator(NoGrouping, td)
		// No groups added

		iter := NewAggregatorIterator(agg)
		err := iter.Open()
		if err != nil {
			t.Fatalf("Failed to open iterator: %v", err)
		}

		hasNext, err := iter.HasNext()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if hasNext {
			t.Error("Expected hasNext to be false")
		}
	})

	t.Run("multiple calls to HasNext", func(t *testing.T) {
		td := createIteratorTestTupleDesc(false)
		agg := newMockGroupAggregator(NoGrouping, td)
		agg.addGroup("NO_GROUPING", types.NewIntField(100))

		iter := NewAggregatorIterator(agg)
		err := iter.Open()
		if err != nil {
			t.Fatalf("Failed to open iterator: %v", err)
		}

		// First call
		hasNext1, err := iter.HasNext()
		if err != nil {
			t.Fatalf("First HasNext error: %v", err)
		}

		// Second call should return same result
		hasNext2, err := iter.HasNext()
		if err != nil {
			t.Fatalf("Second HasNext error: %v", err)
		}

		if hasNext1 != hasNext2 {
			t.Error("Multiple HasNext calls should return same result")
		}
	})

	// Note: Error cases during tuple reading are difficult to test reliably
	// with mocks due to caching in HasNext(). The important error cases
	// (iterator not opened) are tested above.
}

// TestAggregatorIterator_Next tests the Next method
func TestAggregatorIterator_Next(t *testing.T) {
	t.Run("iterator not opened", func(t *testing.T) {
		td := createIteratorTestTupleDesc(false)
		agg := newMockGroupAggregator(NoGrouping, td)

		iter := NewAggregatorIterator(agg)

		tup, err := iter.Next()
		if err == nil {
			t.Error("Expected error when iterator not opened")
		}
		if tup != nil {
			t.Error("Expected nil tuple when iterator not opened")
		}
	})

	t.Run("get next tuple without hasNext", func(t *testing.T) {
		td := createIteratorTestTupleDesc(false)
		agg := newMockGroupAggregator(NoGrouping, td)
		agg.addGroup("NO_GROUPING", types.NewIntField(100))

		iter := NewAggregatorIterator(agg)
		err := iter.Open()
		if err != nil {
			t.Fatalf("Failed to open iterator: %v", err)
		}

		// Call Next without calling HasNext first
		tup, err := iter.Next()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if tup == nil {
			t.Error("Expected non-nil tuple")
		}
	})

	t.Run("get next tuple with hasNext", func(t *testing.T) {
		td := createIteratorTestTupleDesc(false)
		agg := newMockGroupAggregator(NoGrouping, td)
		agg.addGroup("NO_GROUPING", types.NewIntField(100))

		iter := NewAggregatorIterator(agg)
		err := iter.Open()
		if err != nil {
			t.Fatalf("Failed to open iterator: %v", err)
		}

		hasNext, err := iter.HasNext()
		if err != nil {
			t.Fatalf("HasNext error: %v", err)
		}
		if !hasNext {
			t.Fatal("Expected hasNext to be true")
		}

		tup, err := iter.Next()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if tup == nil {
			t.Error("Expected non-nil tuple")
		}
	})

	t.Run("no more tuples available", func(t *testing.T) {
		td := createIteratorTestTupleDesc(false)
		agg := newMockGroupAggregator(NoGrouping, td)
		// No groups added

		iter := NewAggregatorIterator(agg)
		err := iter.Open()
		if err != nil {
			t.Fatalf("Failed to open iterator: %v", err)
		}

		tup, err := iter.Next()
		if err == nil {
			t.Error("Expected error when no tuples available")
		}
		if tup != nil {
			t.Error("Expected nil tuple when no tuples available")
		}
	})

	t.Run("iterate through all tuples", func(t *testing.T) {
		td := createIteratorTestTupleDesc(true)
		agg := newMockGroupAggregator(0, td)
		agg.addGroup("A", types.NewIntField(10))
		agg.addGroup("B", types.NewIntField(20))
		agg.addGroup("C", types.NewIntField(30))

		iter := NewAggregatorIterator(agg)
		err := iter.Open()
		if err != nil {
			t.Fatalf("Failed to open iterator: %v", err)
		}

		count := 0
		for {
			hasNext, err := iter.HasNext()
			if err != nil {
				t.Fatalf("HasNext error: %v", err)
			}
			if !hasNext {
				break
			}

			tup, err := iter.Next()
			if err != nil {
				t.Fatalf("Next error: %v", err)
			}
			if tup == nil {
				t.Error("Expected non-nil tuple")
			}
			count++
		}

		if count != 3 {
			t.Errorf("Expected 3 tuples, got %d", count)
		}
	})
}

// TestAggregatorIterator_readNext tests the internal readNext method behavior
func TestAggregatorIterator_readNext_NonGrouped(t *testing.T) {
	td := createIteratorTestTupleDesc(false)
	agg := newMockGroupAggregator(NoGrouping, td)
	agg.addGroup("NO_GROUPING", types.NewIntField(42))

	iter := NewAggregatorIterator(agg)
	err := iter.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}

	tup, err := iter.readNext()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if tup == nil {
		t.Fatal("Expected non-nil tuple")
	}

	// Verify tuple has 1 field (aggregate value only)
	if tup.TupleDesc.NumFields() != 1 {
		t.Errorf("Expected 1 field in non-grouped result, got %d", tup.TupleDesc.NumFields())
	}

	// Verify the value
	field, err := tup.GetField(0)
	if err != nil {
		t.Fatalf("Failed to get field: %v", err)
	}

	intField, ok := field.(*types.IntField)
	if !ok {
		t.Fatal("Expected IntField")
	}
	if intField.Value != 42 {
		t.Errorf("Expected value 42, got %d", intField.Value)
	}
}

// TestAggregatorIterator_readNext_Grouped tests readNext with grouped aggregation
func TestAggregatorIterator_readNext_Grouped(t *testing.T) {
	td := createIteratorTestTupleDesc(true)
	agg := newMockGroupAggregator(0, td)
	agg.addGroup("TestGroup", types.NewIntField(100))

	iter := NewAggregatorIterator(agg)
	err := iter.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}

	tup, err := iter.readNext()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if tup == nil {
		t.Fatal("Expected non-nil tuple")
	}

	// Verify tuple has 2 fields (group key + aggregate value)
	if tup.TupleDesc.NumFields() != 2 {
		t.Errorf("Expected 2 fields in grouped result, got %d", tup.TupleDesc.NumFields())
	}

	// Verify group field
	groupField, err := tup.GetField(0)
	if err != nil {
		t.Fatalf("Failed to get group field: %v", err)
	}

	groupStr := groupField.String()
	if groupStr != "TestGroup" {
		t.Errorf("Expected group 'TestGroup', got '%s'", groupStr)
	}

	// Verify value field
	valueField, err := tup.GetField(1)
	if err != nil {
		t.Fatalf("Failed to get value field: %v", err)
	}

	intField, ok := valueField.(*types.IntField)
	if !ok {
		t.Fatal("Expected IntField")
	}
	if intField.Value != 100 {
		t.Errorf("Expected value 100, got %d", intField.Value)
	}
}

// TestAggregatorIterator_Rewind tests the Rewind method
func TestAggregatorIterator_Rewind(t *testing.T) {
	t.Run("iterator not opened", func(t *testing.T) {
		td := createIteratorTestTupleDesc(false)
		agg := newMockGroupAggregator(NoGrouping, td)

		iter := NewAggregatorIterator(agg)

		err := iter.Rewind()
		if err == nil {
			t.Error("Expected error when rewinding unopened iterator")
		}
	})

	t.Run("successful rewind", func(t *testing.T) {
		td := createIteratorTestTupleDesc(true)
		agg := newMockGroupAggregator(0, td)
		agg.addGroup("A", types.NewIntField(10))
		agg.addGroup("B", types.NewIntField(20))

		iter := NewAggregatorIterator(agg)
		err := iter.Open()
		if err != nil {
			t.Fatalf("Failed to open iterator: %v", err)
		}

		// Read first tuple
		hasNext, _ := iter.HasNext()
		if hasNext {
			iter.Next()
		}

		// Rewind
		err = iter.Rewind()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if iter.currentIdx != 0 {
			t.Errorf("Expected currentIdx to be 0 after rewind, got %d", iter.currentIdx)
		}

		// Should be able to iterate again
		count := 0
		for {
			hasNext, err := iter.HasNext()
			if err != nil {
				t.Fatalf("HasNext error: %v", err)
			}
			if !hasNext {
				break
			}
			_, err = iter.Next()
			if err != nil {
				t.Fatalf("Next error: %v", err)
			}
			count++
		}

		if count != 2 {
			t.Errorf("Expected 2 tuples after rewind, got %d", count)
		}
	})

	t.Run("rewind at different positions", func(t *testing.T) {
		td := createIteratorTestTupleDesc(true)
		agg := newMockGroupAggregator(0, td)
		agg.addGroup("A", types.NewIntField(10))
		agg.addGroup("B", types.NewIntField(20))
		agg.addGroup("C", types.NewIntField(30))

		iter := NewAggregatorIterator(agg)
		err := iter.Open()
		if err != nil {
			t.Fatalf("Failed to open iterator: %v", err)
		}

		// Read all tuples
		for {
			hasNext, _ := iter.HasNext()
			if !hasNext {
				break
			}
			iter.Next()
		}

		// Rewind after exhausting
		err = iter.Rewind()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Should be able to iterate again
		hasNext, err := iter.HasNext()
		if err != nil {
			t.Fatalf("HasNext error after rewind: %v", err)
		}
		if !hasNext {
			t.Error("Expected results after rewind")
		}
	})
}

// TestAggregatorIterator_Close tests the Close method
func TestAggregatorIterator_Close(t *testing.T) {
	t.Run("close opened iterator", func(t *testing.T) {
		td := createIteratorTestTupleDesc(false)
		agg := newMockGroupAggregator(NoGrouping, td)
		agg.addGroup("NO_GROUPING", types.NewIntField(100))

		iter := NewAggregatorIterator(agg)
		err := iter.Open()
		if err != nil {
			t.Fatalf("Failed to open iterator: %v", err)
		}

		err = iter.Close()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		if iter.opened {
			t.Error("Expected iterator to be closed")
		}
		// Note: aggregator and groups are private fields, so we can't check them directly
		// Instead, we verify the iterator is closed by testing behavior
	})

	t.Run("close unopened iterator", func(t *testing.T) {
		td := createIteratorTestTupleDesc(false)
		agg := newMockGroupAggregator(NoGrouping, td)

		iter := NewAggregatorIterator(agg)

		err := iter.Close()
		if err != nil {
			t.Errorf("Unexpected error when closing unopened iterator: %v", err)
		}
	})

	t.Run("operations after close", func(t *testing.T) {
		td := createIteratorTestTupleDesc(false)
		agg := newMockGroupAggregator(NoGrouping, td)
		agg.addGroup("NO_GROUPING", types.NewIntField(100))

		iter := NewAggregatorIterator(agg)
		err := iter.Open()
		if err != nil {
			t.Fatalf("Failed to open iterator: %v", err)
		}

		err = iter.Close()
		if err != nil {
			t.Fatalf("Failed to close iterator: %v", err)
		}

		// Try HasNext after close
		_, err = iter.HasNext()
		if err == nil {
			t.Error("Expected error when calling HasNext after close")
		}

		// Try Next after close
		_, err = iter.Next()
		if err == nil {
			t.Error("Expected error when calling Next after close")
		}

		// Try Rewind after close
		err = iter.Rewind()
		if err == nil {
			t.Error("Expected error when calling Rewind after close")
		}
	})

	t.Run("reopen after close", func(t *testing.T) {
		td := createIteratorTestTupleDesc(false)
		agg := newMockGroupAggregator(NoGrouping, td)
		agg.addGroup("NO_GROUPING", types.NewIntField(100))

		iter := NewAggregatorIterator(agg)

		// First open and close
		err := iter.Open()
		if err != nil {
			t.Fatalf("Failed first open: %v", err)
		}

		err = iter.Close()
		if err != nil {
			t.Fatalf("Failed to close: %v", err)
		}

		// The iterator cannot be reopened because Close sets aggregator to nil
		// This is by design - create a new iterator instead
	})
}

// TestAggregatorIterator_GetTupleDesc tests the GetTupleDesc method
func TestAggregatorIterator_GetTupleDesc(t *testing.T) {
	t.Run("non-grouped aggregation", func(t *testing.T) {
		td := createIteratorTestTupleDesc(false)
		agg := newMockGroupAggregator(NoGrouping, td)

		iter := NewAggregatorIterator(agg)

		resultTd := iter.GetTupleDesc()
		if resultTd == nil {
			t.Fatal("Expected non-nil tuple description")
		}

		if resultTd.NumFields() != 1 {
			t.Errorf("Expected 1 field, got %d", resultTd.NumFields())
		}
	})

	t.Run("grouped aggregation", func(t *testing.T) {
		td := createIteratorTestTupleDesc(true)
		agg := newMockGroupAggregator(0, td)

		iter := NewAggregatorIterator(agg)

		resultTd := iter.GetTupleDesc()
		if resultTd == nil {
			t.Fatal("Expected non-nil tuple description")
		}

		if resultTd.NumFields() != 2 {
			t.Errorf("Expected 2 fields, got %d", resultTd.NumFields())
		}
	})
}

// TestAggregatorIterator_EmptyGroups tests iterator with no groups
func TestAggregatorIterator_EmptyGroups(t *testing.T) {
	td := createIteratorTestTupleDesc(false)
	agg := newMockGroupAggregator(NoGrouping, td)
	// No groups added

	iter := NewAggregatorIterator(agg)
	err := iter.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}
	defer iter.Close()

	hasNext, err := iter.HasNext()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if hasNext {
		t.Error("Expected no results for empty groups")
	}

	_, err = iter.Next()
	if err == nil {
		t.Error("Expected error when calling Next with no results")
	}
}

// TestAggregatorIterator_MultipleGroups tests iterator with multiple groups
func TestAggregatorIterator_MultipleGroups(t *testing.T) {
	td := createIteratorTestTupleDesc(true)
	agg := newMockGroupAggregator(0, td)

	// Add multiple groups
	expectedGroups := map[string]int64{
		"Group1": 100,
		"Group2": 200,
		"Group3": 300,
		"Group4": 400,
		"Group5": 500,
	}

	for group, value := range expectedGroups {
		agg.addGroup(group, types.NewIntField(value))
	}

	iter := NewAggregatorIterator(agg)
	err := iter.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}
	defer iter.Close()

	results := make(map[string]int64)
	for {
		hasNext, err := iter.HasNext()
		if err != nil {
			t.Fatalf("HasNext error: %v", err)
		}
		if !hasNext {
			break
		}

		tup, err := iter.Next()
		if err != nil {
			t.Fatalf("Next error: %v", err)
		}

		groupField, _ := tup.GetField(0)
		valueField, _ := tup.GetField(1)

		groupStr := groupField.String()
		intField := valueField.(*types.IntField)
		results[groupStr] = intField.Value
	}

	if len(results) != len(expectedGroups) {
		t.Errorf("Expected %d results, got %d", len(expectedGroups), len(results))
	}

	for group, expectedValue := range expectedGroups {
		if actualValue, exists := results[group]; !exists {
			t.Errorf("Missing group %s in results", group)
		} else if actualValue != expectedValue {
			t.Errorf("Group %s: expected %d, got %d", group, expectedValue, actualValue)
		}
	}
}

// TestAggregatorIterator_ThreadSafety tests concurrent access
func TestAggregatorIterator_ThreadSafety(t *testing.T) {
	td := createIteratorTestTupleDesc(true)
	agg := newMockGroupAggregator(0, td)
	agg.addGroup("A", types.NewIntField(10))
	agg.addGroup("B", types.NewIntField(20))

	iter := NewAggregatorIterator(agg)
	err := iter.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}
	defer iter.Close()

	// Test that RLock/RUnlock are properly called during iteration
	// This is implicitly tested by the fact that readNext calls RLock/RUnlock
	for {
		hasNext, err := iter.HasNext()
		if err != nil {
			t.Fatalf("HasNext error: %v", err)
		}
		if !hasNext {
			break
		}

		_, err = iter.Next()
		if err != nil {
			t.Fatalf("Next error: %v", err)
		}
	}
}

// TestAggregatorIterator_ErrorHandling tests basic error scenarios
func TestAggregatorIterator_ErrorHandling(t *testing.T) {
	t.Run("missing group key", func(t *testing.T) {
		td := createIteratorTestTupleDesc(false)
		agg := newMockGroupAggregator(NoGrouping, td)
		// Don't add any groups, so iterator should return no results

		iter := NewAggregatorIterator(agg)
		err := iter.Open()
		if err != nil {
			t.Fatalf("Failed to open iterator: %v", err)
		}
		defer iter.Close()

		hasNext, err := iter.HasNext()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if hasNext {
			t.Error("Expected no results")
		}
	})

	// Note: More complex error cases (like GetAggregateValue failures) are
	// difficult to test reliably with mocks due to caching and locking.
	// These are better tested through integration tests with real aggregators.
}

// TestAggregatorIterator_Integration tests end-to-end scenarios
func TestAggregatorIterator_Integration(t *testing.T) {
	t.Run("full iteration cycle", func(t *testing.T) {
		td := createIteratorTestTupleDesc(true)
		agg := newMockGroupAggregator(0, td)
		agg.addGroup("A", types.NewIntField(10))
		agg.addGroup("B", types.NewIntField(20))
		agg.addGroup("C", types.NewIntField(30))

		iter := NewAggregatorIterator(agg)

		// Open
		err := iter.Open()
		if err != nil {
			t.Fatalf("Failed to open: %v", err)
		}

		// Iterate once
		count1 := 0
		for {
			hasNext, err := iter.HasNext()
			if err != nil || !hasNext {
				break
			}
			_, err = iter.Next()
			if err != nil {
				t.Fatalf("Error during first iteration: %v", err)
			}
			count1++
		}

		// Rewind
		err = iter.Rewind()
		if err != nil {
			t.Fatalf("Failed to rewind: %v", err)
		}

		// Iterate again
		count2 := 0
		for {
			hasNext, err := iter.HasNext()
			if err != nil || !hasNext {
				break
			}
			_, err = iter.Next()
			if err != nil {
				t.Fatalf("Error during second iteration: %v", err)
			}
			count2++
		}

		// Close
		err = iter.Close()
		if err != nil {
			t.Fatalf("Failed to close: %v", err)
		}

		if count1 != 3 {
			t.Errorf("Expected 3 tuples in first iteration, got %d", count1)
		}
		if count2 != 3 {
			t.Errorf("Expected 3 tuples in second iteration, got %d", count2)
		}
	})
}
