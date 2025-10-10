package aggregation

import (
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

func TestAggregatorIterator_IntegerAggregator_NoGrouping(t *testing.T) {
	agg, err := NewIntAggregator(NoGrouping, types.StringType, 0, Sum)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}

	// Add test data
	td, err := tuple.NewTupleDesc([]types.Type{types.IntType}, []string{"value"})
	if err != nil {
		t.Fatalf("Failed to create tuple description: %v", err)
	}

	values := []int64{10, 20, 30}
	for _, value := range values {
		tup := tuple.NewTuple(td)
		err := tup.SetField(0, types.NewIntField(value))
		if err != nil {
			t.Fatalf("Failed to set field: %v", err)
		}
		err = agg.Merge(tup)
		if err != nil {
			t.Fatalf("Failed to merge tuple: %v", err)
		}
	}

	iter := agg.Iterator()

	t.Run("lifecycle operations", func(t *testing.T) {
		// Test operations on closed iterator
		_, err := iter.HasNext()
		if err == nil {
			t.Error("Expected error when calling HasNext on closed iterator")
		}

		_, err = iter.Next()
		if err == nil {
			t.Error("Expected error when calling Next on closed iterator")
		}

		err = iter.Rewind()
		if err == nil {
			t.Error("Expected error when calling Rewind on closed iterator")
		}

		// Test opening
		err = iter.Open()
		if err != nil {
			t.Fatalf("Failed to open iterator: %v", err)
		}

		// Test double open
		err = iter.Open()
		if err == nil {
			t.Error("Expected error when opening already opened iterator")
		}

		// Test normal operations
		hasNext, err := iter.HasNext()
		if err != nil {
			t.Fatalf("Error checking HasNext: %v", err)
		}
		if !hasNext {
			t.Error("Expected iterator to have next")
		}

		result, err := iter.Next()
		if err != nil {
			t.Fatalf("Failed to get next tuple: %v", err)
		}

		// Verify result
		field, err := result.GetField(0)
		if err != nil {
			t.Fatalf("Failed to get field: %v", err)
		}
		intField, ok := field.(*types.IntField)
		if !ok {
			t.Fatal("Expected IntField")
		}
		if intField.Value != 60 { // 10 + 20 + 30
			t.Errorf("Expected sum 60, got %d", intField.Value)
		}

		// Should have no more results
		hasNext, err = iter.HasNext()
		if err != nil {
			t.Fatalf("Error checking HasNext: %v", err)
		}
		if hasNext {
			t.Error("Expected no more results")
		}

		// Test rewind
		err = iter.Rewind()
		if err != nil {
			t.Fatalf("Failed to rewind: %v", err)
		}

		hasNext, err = iter.HasNext()
		if err != nil {
			t.Fatalf("Error checking HasNext after rewind: %v", err)
		}
		if !hasNext {
			t.Error("Expected iterator to have next after rewind")
		}

		// Test closing
		err = iter.Close()
		if err != nil {
			t.Fatalf("Failed to close iterator: %v", err)
		}

		// Test operations on closed iterator after close
		_, err = iter.HasNext()
		if err == nil {
			t.Error("Expected error when calling HasNext on closed iterator")
		}
	})
}

func TestAggregatorIterator_IntegerAggregator_WithGrouping(t *testing.T) {
	agg, err := NewIntAggregator(0, types.StringType, 1, Sum)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}

	// Add test data with grouping
	td, err := tuple.NewTupleDesc(
		[]types.Type{types.StringType, types.IntType},
		[]string{"group", "value"},
	)
	if err != nil {
		t.Fatalf("Failed to create tuple description: %v", err)
	}

	testData := []struct {
		group string
		value int64
	}{
		{"A", 10},
		{"B", 20},
		{"A", 15},
		{"C", 30},
		{"B", 25},
	}

	for _, data := range testData {
		tup := tuple.NewTuple(td)
		err := tup.SetField(0, types.NewStringField(data.group, len(data.group)))
		if err != nil {
			t.Fatalf("Failed to set group field: %v", err)
		}
		err = tup.SetField(1, types.NewIntField(data.value))
		if err != nil {
			t.Fatalf("Failed to set value field: %v", err)
		}
		err = agg.Merge(tup)
		if err != nil {
			t.Fatalf("Failed to merge tuple: %v", err)
		}
	}

	iter := agg.Iterator()
	err = iter.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}
	defer iter.Close()

	// Collect all results
	results := make(map[string]int64)
	for {
		hasNext, err := iter.HasNext()
		if err != nil {
			t.Fatalf("Error checking HasNext: %v", err)
		}
		if !hasNext {
			break
		}

		result, err := iter.Next()
		if err != nil {
			t.Fatalf("Failed to get next tuple: %v", err)
		}

		groupField, err := result.GetField(0)
		if err != nil {
			t.Fatalf("Failed to get group field: %v", err)
		}
		valueField, err := result.GetField(1)
		if err != nil {
			t.Fatalf("Failed to get value field: %v", err)
		}

		groupStr := groupField.String()
		intField, ok := valueField.(*types.IntField)
		if !ok {
			t.Fatal("Expected IntField for value")
		}

		results[groupStr] = intField.Value
	}

	// Verify expected results
	expectedResults := map[string]int64{
		"A": 25, // 10 + 15
		"B": 45, // 20 + 25
		"C": 30, // 30
	}

	if len(results) != len(expectedResults) {
		t.Errorf("Expected %d groups, got %d", len(expectedResults), len(results))
	}

	for group, expectedSum := range expectedResults {
		if actualSum, exists := results[group]; !exists {
			t.Errorf("Missing group %s", group)
		} else if actualSum != expectedSum {
			t.Errorf("Group %s: expected sum %d, got %d", group, expectedSum, actualSum)
		}
	}
}

func TestAggregatorIterator_EmptyAggregator(t *testing.T) {
	agg, err := NewIntAggregator(NoGrouping, types.StringType, 0, Sum)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}

	iter := agg.Iterator()
	err = iter.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}
	defer iter.Close()

	hasNext, err := iter.HasNext()
	if err != nil {
		t.Fatalf("Error checking HasNext: %v", err)
	}
	if hasNext {
		t.Error("Expected no results for empty aggregator")
	}

	_, err = iter.Next()
	if err == nil {
		t.Error("Expected error when calling Next on empty iterator")
	}
}

func TestAggregatorIterator_TupleDesc(t *testing.T) {
	t.Run("no grouping", func(t *testing.T) {
		agg, err := NewIntAggregator(NoGrouping, types.StringType, 0, Sum)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		iter := agg.Iterator()
		td := iter.GetTupleDesc()

		if td.NumFields() != 1 {
			t.Errorf("Expected 1 field, got %d", td.NumFields())
		}

		fieldType, err := td.TypeAtIndex(0)
		if err != nil {
			t.Fatalf("Failed to get field type: %v", err)
		}
		if fieldType != types.IntType {
			t.Errorf("Expected IntType, got %v", fieldType)
		}
	})

	t.Run("with grouping", func(t *testing.T) {
		agg, err := NewIntAggregator(0, types.StringType, 1, Sum)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		iter := agg.Iterator()
		td := iter.GetTupleDesc()

		if td.NumFields() != 2 {
			t.Errorf("Expected 2 fields, got %d", td.NumFields())
		}

		groupType, err := td.TypeAtIndex(0)
		if err != nil {
			t.Fatalf("Failed to get group field type: %v", err)
		}
		if groupType != types.StringType {
			t.Errorf("Expected StringType for group field, got %v", groupType)
		}

		aggType, err := td.TypeAtIndex(1)
		if err != nil {
			t.Fatalf("Failed to get aggregate field type: %v", err)
		}
		if aggType != types.IntType {
			t.Errorf("Expected IntType for aggregate field, got %v", aggType)
		}
	})
}

func TestAggregatorIterator_Operations_Multiple_Iterations(t *testing.T) {
	agg, err := NewIntAggregator(0, types.StringType, 1, Sum)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}

	// Add test data
	td, err := tuple.NewTupleDesc(
		[]types.Type{types.StringType, types.IntType},
		[]string{"group", "value"},
	)
	if err != nil {
		t.Fatalf("Failed to create tuple description: %v", err)
	}

	testData := []struct {
		group string
		value int64
	}{
		{"A", 10},
		{"B", 20},
		{"A", 15},
	}

	for _, data := range testData {
		tup := tuple.NewTuple(td)
		err := tup.SetField(0, types.NewStringField(data.group, len(data.group)))
		if err != nil {
			t.Fatalf("Failed to set group field: %v", err)
		}
		err = tup.SetField(1, types.NewIntField(data.value))
		if err != nil {
			t.Fatalf("Failed to set value field: %v", err)
		}
		err = agg.Merge(tup)
		if err != nil {
			t.Fatalf("Failed to merge tuple: %v", err)
		}
	}

	iter := agg.Iterator()
	err = iter.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}
	defer iter.Close()

	// First iteration - read all
	var firstResults []string
	for {
		hasNext, err := iter.HasNext()
		if err != nil {
			t.Fatalf("Error checking HasNext: %v", err)
		}
		if !hasNext {
			break
		}

		result, err := iter.Next()
		if err != nil {
			t.Fatalf("Failed to get next tuple: %v", err)
		}

		groupField, err := result.GetField(0)
		if err != nil {
			t.Fatalf("Failed to get group field: %v", err)
		}
		firstResults = append(firstResults, groupField.String())
	}

	if len(firstResults) != 2 {
		t.Errorf("Expected 2 results in first iteration, got %d", len(firstResults))
	}

	// Rewind and iterate again
	err = iter.Rewind()
	if err != nil {
		t.Fatalf("Failed to rewind: %v", err)
	}

	var secondResults []string
	for {
		hasNext, err := iter.HasNext()
		if err != nil {
			t.Fatalf("Error checking HasNext after rewind: %v", err)
		}
		if !hasNext {
			break
		}

		result, err := iter.Next()
		if err != nil {
			t.Fatalf("Failed to get next tuple after rewind: %v", err)
		}

		groupField, err := result.GetField(0)
		if err != nil {
			t.Fatalf("Failed to get group field after rewind: %v", err)
		}
		secondResults = append(secondResults, groupField.String())
	}

	if len(secondResults) != len(firstResults) {
		t.Errorf("Expected same number of results after rewind, got %d vs %d", len(secondResults), len(firstResults))
	}

	// Results should be the same (order may vary but should contain same elements)
	if len(firstResults) == len(secondResults) {
		found := make(map[string]bool)
		for _, result := range firstResults {
			found[result] = true
		}
		for _, result := range secondResults {
			if !found[result] {
				t.Errorf("Result %s not found in second iteration", result)
			}
		}
	}
}

func TestAggregatorIterator_HasNext_Next_Pattern(t *testing.T) {
	agg, err := NewIntAggregator(NoGrouping, types.StringType, 0, Sum)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}

	// Add one value
	td, err := tuple.NewTupleDesc([]types.Type{types.IntType}, []string{"value"})
	if err != nil {
		t.Fatalf("Failed to create tuple description: %v", err)
	}

	tup := tuple.NewTuple(td)
	err = tup.SetField(0, types.NewIntField(42))
	if err != nil {
		t.Fatalf("Failed to set field: %v", err)
	}
	err = agg.Merge(tup)
	if err != nil {
		t.Fatalf("Failed to merge tuple: %v", err)
	}

	iter := agg.Iterator()
	err = iter.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}
	defer iter.Close()

	// Test calling Next without HasNext
	result, err := iter.Next()
	if err != nil {
		t.Fatalf("Failed to get next tuple without HasNext: %v", err)
	}

	field, err := result.GetField(0)
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

	// Should be at end now
	hasNext, err := iter.HasNext()
	if err != nil {
		t.Fatalf("Error checking HasNext: %v", err)
	}
	if hasNext {
		t.Error("Expected no more results")
	}

	// Calling Next should fail
	_, err = iter.Next()
	if err == nil {
		t.Error("Expected error when calling Next at end of iteration")
	}
}

func TestAggregatorIterator_Multiple_HasNext_Calls(t *testing.T) {
	agg, err := NewIntAggregator(NoGrouping, types.StringType, 0, Sum)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}

	// Add one value
	td, err := tuple.NewTupleDesc([]types.Type{types.IntType}, []string{"value"})
	if err != nil {
		t.Fatalf("Failed to create tuple description: %v", err)
	}

	tup := tuple.NewTuple(td)
	err = tup.SetField(0, types.NewIntField(42))
	if err != nil {
		t.Fatalf("Failed to set field: %v", err)
	}
	err = agg.Merge(tup)
	if err != nil {
		t.Fatalf("Failed to merge tuple: %v", err)
	}

	iter := agg.Iterator()
	err = iter.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}
	defer iter.Close()

	// Call HasNext multiple times
	for i := 0; i < 3; i++ {
		hasNext, err := iter.HasNext()
		if err != nil {
			t.Fatalf("Error checking HasNext (call %d): %v", i+1, err)
		}
		if !hasNext {
			t.Errorf("Expected HasNext to return true (call %d)", i+1)
		}
	}

	// Now call Next - should still work
	result, err := iter.Next()
	if err != nil {
		t.Fatalf("Failed to get next tuple after multiple HasNext calls: %v", err)
	}

	field, err := result.GetField(0)
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

	// Multiple HasNext calls at end should return false
	for i := 0; i < 3; i++ {
		hasNext, err := iter.HasNext()
		if err != nil {
			t.Fatalf("Error checking HasNext at end (call %d): %v", i+1, err)
		}
		if hasNext {
			t.Errorf("Expected HasNext to return false at end (call %d)", i+1)
		}
	}
}

// General iterator interface tests that work with both types of iterators

func TestGeneralIteratorInterface_IntAggregator(t *testing.T) {
	testGeneralIteratorInterface(t, func() (Aggregator, *tuple.TupleDescription, []*tuple.Tuple) {
		agg, _ := NewIntAggregator(0, types.StringType, 1, Max)

		td, _ := tuple.NewTupleDesc(
			[]types.Type{types.StringType, types.IntType},
			[]string{"group", "value"},
		)

		testData := []struct {
			group string
			value int64
		}{
			{"A", 10},
			{"B", 20},
			{"A", 30},
		}

		var tuples []*tuple.Tuple
		for _, data := range testData {
			tup := tuple.NewTuple(td)
			tup.SetField(0, types.NewStringField(data.group, len(data.group)))
			tup.SetField(1, types.NewIntField(data.value))
			tuples = append(tuples, tup)
		}

		return agg, td, tuples
	})
}

func TestGeneralIteratorInterface_BoolAggregator(t *testing.T) {
	testGeneralIteratorInterface(t, func() (Aggregator, *tuple.TupleDescription, []*tuple.Tuple) {
		agg, _ := NewBooleanAggregator(0, types.StringType, 1, And)

		td, _ := tuple.NewTupleDesc(
			[]types.Type{types.StringType, types.BoolType},
			[]string{"group", "value"},
		)

		testData := []struct {
			group string
			value bool
		}{
			{"A", true},
			{"B", false},
			{"A", true},
		}

		var tuples []*tuple.Tuple
		for _, data := range testData {
			tup := tuple.NewTuple(td)
			tup.SetField(0, types.NewStringField(data.group, len(data.group)))
			tup.SetField(1, types.NewBoolField(data.value))
			tuples = append(tuples, tup)
		}

		return agg, td, tuples
	})
}

func TestGeneralIteratorInterface_StringAggregator(t *testing.T) {
	testGeneralIteratorInterface(t, func() (Aggregator, *tuple.TupleDescription, []*tuple.Tuple) {
		agg, _ := NewStringAggregator(0, types.StringType, 1, Min)

		td, _ := tuple.NewTupleDesc(
			[]types.Type{types.StringType, types.StringType},
			[]string{"group", "value"},
		)

		testData := []struct {
			group string
			value string
		}{
			{"A", "apple"},
			{"B", "zebra"},
			{"A", "banana"},
		}

		var tuples []*tuple.Tuple
		for _, data := range testData {
			tup := tuple.NewTuple(td)
			tup.SetField(0, types.NewStringField(data.group, len(data.group)))
			tup.SetField(1, types.NewStringField(data.value, len(data.value)))
			tuples = append(tuples, tup)
		}

		return agg, td, tuples
	})
}

func testGeneralIteratorInterface(t *testing.T, setupFunc func() (Aggregator, *tuple.TupleDescription, []*tuple.Tuple)) {
	agg, _, tuples := setupFunc()

	// Merge all tuples
	for _, tup := range tuples {
		err := agg.Merge(tup)
		if err != nil {
			t.Fatalf("Failed to merge tuple: %v", err)
		}
	}

	iter := agg.Iterator()

	t.Run("iterator_implements_interface", func(t *testing.T) {
		// Verify GetTupleDesc works before opening
		resultTd := iter.GetTupleDesc()
		if resultTd == nil {
			t.Error("GetTupleDesc returned nil")
		}
	})

	t.Run("iterator_lifecycle", func(t *testing.T) {
		// Open iterator
		err := iter.Open()
		if err != nil {
			t.Fatalf("Failed to open iterator: %v", err)
		}

		// Verify we can get tuple description after opening
		resultTd := iter.GetTupleDesc()
		if resultTd == nil {
			t.Error("GetTupleDesc returned nil after opening")
		}

		// Test that iterator can be iterated
		resultCount := 0
		for {
			hasNext, err := iter.HasNext()
			if err != nil {
				t.Fatalf("Error checking HasNext: %v", err)
			}
			if !hasNext {
				break
			}

			result, err := iter.Next()
			if err != nil {
				t.Fatalf("Failed to get next tuple: %v", err)
			}

			if result == nil {
				t.Error("Next returned nil tuple")
			}

			resultCount++

			// Basic validation that result has expected structure
			if resultTd.NumFields() == 1 {
				// No grouping case
				field, err := result.GetField(0)
				if err != nil {
					t.Fatalf("Failed to get aggregate field: %v", err)
				}
				if field == nil {
					t.Error("Aggregate field is nil")
				}
			} else if resultTd.NumFields() == 2 {
				// With grouping case
				groupField, err := result.GetField(0)
				if err != nil {
					t.Fatalf("Failed to get group field: %v", err)
				}
				if groupField == nil {
					t.Error("Group field is nil")
				}

				aggField, err := result.GetField(1)
				if err != nil {
					t.Fatalf("Failed to get aggregate field: %v", err)
				}
				if aggField == nil {
					t.Error("Aggregate field is nil")
				}
			} else {
				t.Errorf("Unexpected number of fields in result: %d", resultTd.NumFields())
			}
		}

		if resultCount == 0 {
			t.Error("Iterator produced no results")
		}

		// Test rewind
		err = iter.Rewind()
		if err != nil {
			t.Fatalf("Failed to rewind iterator: %v", err)
		}

		// Verify we can iterate again after rewind
		rewindCount := 0
		for {
			hasNext, err := iter.HasNext()
			if err != nil {
				t.Fatalf("Error checking HasNext after rewind: %v", err)
			}
			if !hasNext {
				break
			}

			_, err = iter.Next()
			if err != nil {
				t.Fatalf("Failed to get next tuple after rewind: %v", err)
			}
			rewindCount++
		}

		if rewindCount != resultCount {
			t.Errorf("Expected same result count after rewind: %d vs %d", rewindCount, resultCount)
		}

		// Close iterator
		err = iter.Close()
		if err != nil {
			t.Fatalf("Failed to close iterator: %v", err)
		}

		// Verify operations fail after close
		_, err = iter.HasNext()
		if err == nil {
			t.Error("Expected error when calling HasNext on closed iterator")
		}

		_, err = iter.Next()
		if err == nil {
			t.Error("Expected error when calling Next on closed iterator")
		}

		err = iter.Rewind()
		if err == nil {
			t.Error("Expected error when calling Rewind on closed iterator")
		}
	})
}

func TestIteratorInterface_ConsistentBehavior(t *testing.T) {
	// Test that both iterator types behave consistently
	iteratorTypes := []struct {
		name    string
		factory func() (Aggregator, *tuple.TupleDescription)
	}{
		{
			name: "IntegerAggregator",
			factory: func() (Aggregator, *tuple.TupleDescription) {
				agg, _ := NewIntAggregator(NoGrouping, types.StringType, 0, Sum)
				td, _ := tuple.NewTupleDesc([]types.Type{types.IntType}, []string{"value"})
				return agg, td
			},
		},
		{
			name: "BooleanAggregator",
			factory: func() (Aggregator, *tuple.TupleDescription) {
				agg, _ := NewBooleanAggregator(NoGrouping, types.StringType, 0, Count)
				td, _ := tuple.NewTupleDesc([]types.Type{types.BoolType}, []string{"value"})
				return agg, td
			},
		},
		{
			name: "StringAggregator",
			factory: func() (Aggregator, *tuple.TupleDescription) {
				agg, _ := NewStringAggregator(NoGrouping, types.StringType, 0, Count)
				td, _ := tuple.NewTupleDesc([]types.Type{types.StringType}, []string{"value"})
				return agg, td
			},
		},
	}

	for _, iterType := range iteratorTypes {
		t.Run(iterType.name, func(t *testing.T) {
			agg, td := iterType.factory()

			// Add some test data
			for i := 0; i < 3; i++ {
				tup := tuple.NewTuple(td)
				if iterType.name == "IntegerAggregator" {
					tup.SetField(0, types.NewIntField(int64(i+1)))
				} else if iterType.name == "BooleanAggregator" {
					tup.SetField(0, types.NewBoolField(true))
				} else {
					tup.SetField(0, types.NewStringField("test", 4))
				}
				agg.Merge(tup)
			}

			iter := agg.Iterator()

			// Test consistent lifecycle behavior
			err := iter.Open()
			if err != nil {
				t.Fatalf("Failed to open %s iterator: %v", iterType.name, err)
			}

			hasNext, err := iter.HasNext()
			if err != nil {
				t.Fatalf("%s HasNext failed: %v", iterType.name, err)
			}
			if !hasNext {
				t.Errorf("%s iterator should have results", iterType.name)
			}

			result, err := iter.Next()
			if err != nil {
				t.Fatalf("%s Next failed: %v", iterType.name, err)
			}
			if result == nil {
				t.Errorf("%s Next returned nil", iterType.name)
			}

			hasNext, err = iter.HasNext()
			if err != nil {
				t.Fatalf("%s HasNext failed after Next: %v", iterType.name, err)
			}
			if hasNext {
				t.Errorf("%s iterator should not have more results", iterType.name)
			}

			err = iter.Rewind()
			if err != nil {
				t.Fatalf("%s Rewind failed: %v", iterType.name, err)
			}

			hasNext, err = iter.HasNext()
			if err != nil {
				t.Fatalf("%s HasNext failed after Rewind: %v", iterType.name, err)
			}
			if !hasNext {
				t.Errorf("%s iterator should have results after rewind", iterType.name)
			}

			err = iter.Close()
			if err != nil {
				t.Fatalf("%s Close failed: %v", iterType.name, err)
			}

			// Verify error behavior is consistent
			_, err = iter.HasNext()
			if err == nil {
				t.Errorf("%s HasNext should fail on closed iterator", iterType.name)
			}
		})
	}
}

func TestIteratorInterface_ErrorHandling(t *testing.T) {
	// Test error handling behavior across different iterator types
	testCases := []struct {
		name    string
		factory func() (Aggregator, *tuple.TupleDescription)
	}{
		{
			name: "IntegerAggregator_Empty",
			factory: func() (Aggregator, *tuple.TupleDescription) {
				agg, _ := NewIntAggregator(NoGrouping, types.StringType, 0, Sum)
				td, _ := tuple.NewTupleDesc([]types.Type{types.IntType}, []string{"value"})
				return agg, td
			},
		},
		{
			name: "BooleanAggregator_Empty",
			factory: func() (Aggregator, *tuple.TupleDescription) {
				agg, _ := NewBooleanAggregator(NoGrouping, types.StringType, 0, Count)
				td, _ := tuple.NewTupleDesc([]types.Type{types.BoolType}, []string{"value"})
				return agg, td
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			agg, _ := tc.factory()
			// Don't add any data - test empty iterator

			iter := agg.Iterator()
			err := iter.Open()
			if err != nil {
				t.Fatalf("Failed to open iterator: %v", err)
			}
			defer iter.Close()

			hasNext, err := iter.HasNext()
			if err != nil {
				t.Fatalf("HasNext failed on empty iterator: %v", err)
			}
			if hasNext {
				t.Error("Empty iterator should not have results")
			}

			_, err = iter.Next()
			if err == nil {
				t.Error("Next should fail on empty iterator")
			}

			// Rewind should still work on empty iterator
			err = iter.Rewind()
			if err != nil {
				t.Fatalf("Rewind failed on empty iterator: %v", err)
			}

			hasNext, err = iter.HasNext()
			if err != nil {
				t.Fatalf("HasNext failed after rewind on empty iterator: %v", err)
			}
			if hasNext {
				t.Error("Empty iterator should not have results after rewind")
			}
		})
	}
}
