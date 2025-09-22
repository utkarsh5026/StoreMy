package aggregation

import (
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"sync"
	"testing"
)

func TestNewBooleanAggregator(t *testing.T) {
	tests := []struct {
		name        string
		gbField     int
		gbFieldType types.Type
		aField      int
		op          AggregateOp
		expectError bool
	}{
		{
			name:        "valid aggregator without grouping - Count",
			gbField:     NoGrouping,
			gbFieldType: types.StringType,
			aField:      0,
			op:          Count,
			expectError: false,
		},
		{
			name:        "valid aggregator with grouping - And",
			gbField:     0,
			gbFieldType: types.StringType,
			aField:      1,
			op:          And,
			expectError: false,
		},
		{
			name:        "valid aggregator - Or",
			gbField:     NoGrouping,
			gbFieldType: types.StringType,
			aField:      0,
			op:          Or,
			expectError: false,
		},
		{
			name:        "valid aggregator - Sum",
			gbField:     NoGrouping,
			gbFieldType: types.StringType,
			aField:      0,
			op:          Sum,
			expectError: false,
		},
		{
			name:        "unsupported operation - Min",
			gbField:     NoGrouping,
			gbFieldType: types.StringType,
			aField:      0,
			op:          Min,
			expectError: true,
		},
		{
			name:        "unsupported operation - Max",
			gbField:     NoGrouping,
			gbFieldType: types.StringType,
			aField:      0,
			op:          Max,
			expectError: true,
		},
		{
			name:        "unsupported operation - Avg",
			gbField:     NoGrouping,
			gbFieldType: types.StringType,
			aField:      0,
			op:          Avg,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agg, err := NewBooleanAggregator(tt.gbField, tt.gbFieldType, tt.aField, tt.op)
			
			if tt.expectError && err == nil {
				t.Error("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if !tt.expectError && agg == nil {
				t.Error("Expected aggregator but got nil")
			}
		})
	}
}

func TestBooleanAggregator_NoGrouping_Operations(t *testing.T) {
	operations := []struct {
		op       AggregateOp
		values   []bool
		expected interface{}
	}{
		{And, []bool{true, true, true}, true},
		{And, []bool{true, false, true}, false},
		{And, []bool{false, false, false}, false},
		{Or, []bool{false, false, false}, false},
		{Or, []bool{false, true, false}, true},
		{Or, []bool{true, true, true}, true},
		{Sum, []bool{true, false, true, true}, int32(3)}, // count of true values
		{Sum, []bool{false, false, false}, int32(0)},
		{Count, []bool{true, false, true, false, true}, int32(5)}, // total count
	}

	for _, op := range operations {
		t.Run(op.op.String()+"_NoGrouping", func(t *testing.T) {
			agg, err := NewBooleanAggregator(NoGrouping, types.StringType, 0, op.op)
			if err != nil {
				t.Fatalf("Failed to create aggregator: %v", err)
			}

			td, err := tuple.NewTupleDesc([]types.Type{types.BoolType}, []string{"value"})
			if err != nil {
				t.Fatalf("Failed to create tuple description: %v", err)
			}

			for _, value := range op.values {
				tup := tuple.NewTuple(td)
				err := tup.SetField(0, types.NewBoolField(value))
				if err != nil {
					t.Fatalf("Failed to set field: %v", err)
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

			hasNext, err := iter.HasNext()
			if err != nil {
				t.Fatalf("Error checking HasNext: %v", err)
			}
			if !hasNext {
				t.Fatal("Expected at least one result")
			}

			result, err := iter.Next()
			if err != nil {
				t.Fatalf("Failed to get next tuple: %v", err)
			}

			field, err := result.GetField(0)
			if err != nil {
				t.Fatalf("Failed to get result field: %v", err)
			}

			switch expectedVal := op.expected.(type) {
			case bool:
				boolField, ok := field.(*types.BoolField)
				if !ok {
					t.Fatal("Result field is not a boolean")
				}
				if boolField.Value != expectedVal {
					t.Errorf("Expected %v, got %v", expectedVal, boolField.Value)
				}
			case int32:
				intField, ok := field.(*types.IntField)
				if !ok {
					t.Fatal("Result field is not an integer")
				}
				if intField.Value != expectedVal {
					t.Errorf("Expected %d, got %d", expectedVal, intField.Value)
				}
			}

			hasNext, err = iter.HasNext()
			if err != nil {
				t.Fatalf("Error checking HasNext: %v", err)
			}
			if hasNext {
				t.Error("Expected no more results")
			}
		})
	}
}

func TestBooleanAggregator_WithGrouping(t *testing.T) {
	agg, err := NewBooleanAggregator(0, types.StringType, 1, And)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}

	td, err := tuple.NewTupleDesc(
		[]types.Type{types.StringType, types.BoolType},
		[]string{"group", "value"},
	)
	if err != nil {
		t.Fatalf("Failed to create tuple description: %v", err)
	}

	testData := []struct {
		group string
		value bool
	}{
		{"A", true},
		{"B", false},
		{"A", true},
		{"C", true},
		{"B", true},
		{"A", false}, // A: true AND true AND false = false
	}

	for _, data := range testData {
		tup := tuple.NewTuple(td)
		err := tup.SetField(0, types.NewStringField(data.group, len(data.group)))
		if err != nil {
			t.Fatalf("Failed to set group field: %v", err)
		}
		err = tup.SetField(1, types.NewBoolField(data.value))
		if err != nil {
			t.Fatalf("Failed to set value field: %v", err)
		}

		err = agg.Merge(tup)
		if err != nil {
			t.Fatalf("Failed to merge tuple: %v", err)
		}
	}

	expectedResults := map[string]bool{
		"A": false, // true AND true AND false = false
		"B": false, // false AND true = false
		"C": true,  // true
	}

	iter := agg.Iterator()
	err = iter.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}
	defer iter.Close()

	results := make(map[string]bool)
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
		boolField, ok := valueField.(*types.BoolField)
		if !ok {
			t.Fatal("Value field is not a boolean")
		}

		results[groupStr] = boolField.Value
	}

	if len(results) != len(expectedResults) {
		t.Errorf("Expected %d groups, got %d", len(expectedResults), len(results))
	}

	for group, expectedResult := range expectedResults {
		if actualResult, exists := results[group]; !exists {
			t.Errorf("Missing group %s", group)
		} else if actualResult != expectedResult {
			t.Errorf("Group %s: expected %v, got %v", group, expectedResult, actualResult)
		}
	}
}

func TestBooleanAggregator_OrOperation(t *testing.T) {
	agg, err := NewBooleanAggregator(0, types.StringType, 1, Or)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}

	td, err := tuple.NewTupleDesc(
		[]types.Type{types.StringType, types.BoolType},
		[]string{"group", "value"},
	)
	if err != nil {
		t.Fatalf("Failed to create tuple description: %v", err)
	}

	testData := []struct {
		group string
		value bool
	}{
		{"A", false},
		{"A", false},
		{"A", true}, // A: false OR false OR true = true
		{"B", false},
		{"B", false}, // B: false OR false = false
	}

	for _, data := range testData {
		tup := tuple.NewTuple(td)
		err := tup.SetField(0, types.NewStringField(data.group, len(data.group)))
		if err != nil {
			t.Fatalf("Failed to set group field: %v", err)
		}
		err = tup.SetField(1, types.NewBoolField(data.value))
		if err != nil {
			t.Fatalf("Failed to set value field: %v", err)
		}

		err = agg.Merge(tup)
		if err != nil {
			t.Fatalf("Failed to merge tuple: %v", err)
		}
	}

	expectedResults := map[string]bool{
		"A": true,  // false OR false OR true = true
		"B": false, // false OR false = false
	}

	iter := agg.Iterator()
	err = iter.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}
	defer iter.Close()

	results := make(map[string]bool)
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
		boolField, ok := valueField.(*types.BoolField)
		if !ok {
			t.Fatal("Value field is not a boolean")
		}

		results[groupStr] = boolField.Value
	}

	for group, expectedResult := range expectedResults {
		if actualResult, exists := results[group]; !exists {
			t.Errorf("Missing group %s", group)
		} else if actualResult != expectedResult {
			t.Errorf("Group %s: expected %v, got %v", group, expectedResult, actualResult)
		}
	}
}

func TestBooleanAggregator_SumOperation(t *testing.T) {
	agg, err := NewBooleanAggregator(0, types.StringType, 1, Sum)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}

	td, err := tuple.NewTupleDesc(
		[]types.Type{types.StringType, types.BoolType},
		[]string{"group", "value"},
	)
	if err != nil {
		t.Fatalf("Failed to create tuple description: %v", err)
	}

	testData := []struct {
		group string
		value bool
	}{
		{"A", true},
		{"A", false},
		{"A", true},  // A: 2 true values
		{"B", false},
		{"B", false}, // B: 0 true values
		{"C", true},  // C: 1 true value
	}

	for _, data := range testData {
		tup := tuple.NewTuple(td)
		err := tup.SetField(0, types.NewStringField(data.group, len(data.group)))
		if err != nil {
			t.Fatalf("Failed to set group field: %v", err)
		}
		err = tup.SetField(1, types.NewBoolField(data.value))
		if err != nil {
			t.Fatalf("Failed to set value field: %v", err)
		}

		err = agg.Merge(tup)
		if err != nil {
			t.Fatalf("Failed to merge tuple: %v", err)
		}
	}

	expectedSums := map[string]int32{
		"A": 2, // 2 true values
		"B": 0, // 0 true values
		"C": 1, // 1 true value
	}

	iter := agg.Iterator()
	err = iter.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}
	defer iter.Close()

	results := make(map[string]int32)
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
			t.Fatal("Value field is not an integer")
		}

		results[groupStr] = intField.Value
	}

	for group, expectedSum := range expectedSums {
		if actualSum, exists := results[group]; !exists {
			t.Errorf("Missing group %s", group)
		} else if actualSum != expectedSum {
			t.Errorf("Group %s: expected sum %d, got %d", group, expectedSum, actualSum)
		}
	}
}

func TestBooleanAggregator_EdgeCases(t *testing.T) {
	t.Run("empty aggregation", func(t *testing.T) {
		agg, err := NewBooleanAggregator(NoGrouping, types.StringType, 0, And)
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
			t.Error("Expected no results for empty aggregation")
		}
	})

	t.Run("invalid aggregate field type", func(t *testing.T) {
		agg, err := NewBooleanAggregator(NoGrouping, types.StringType, 0, And)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

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
		if err == nil {
			t.Error("Expected error when merging non-boolean field")
		}
	})

	t.Run("invalid group field index", func(t *testing.T) {
		agg, err := NewBooleanAggregator(5, types.StringType, 0, And)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		td, err := tuple.NewTupleDesc([]types.Type{types.BoolType}, []string{"value"})
		if err != nil {
			t.Fatalf("Failed to create tuple description: %v", err)
		}

		tup := tuple.NewTuple(td)
		err = tup.SetField(0, types.NewBoolField(true))
		if err != nil {
			t.Fatalf("Failed to set field: %v", err)
		}

		err = agg.Merge(tup)
		if err == nil {
			t.Error("Expected error when accessing invalid group field index")
		}
	})

	t.Run("invalid aggregate field index", func(t *testing.T) {
		agg, err := NewBooleanAggregator(NoGrouping, types.StringType, 5, And)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		td, err := tuple.NewTupleDesc([]types.Type{types.BoolType}, []string{"value"})
		if err != nil {
			t.Fatalf("Failed to create tuple description: %v", err)
		}

		tup := tuple.NewTuple(td)
		err = tup.SetField(0, types.NewBoolField(true))
		if err != nil {
			t.Fatalf("Failed to set field: %v", err)
		}

		err = agg.Merge(tup)
		if err == nil {
			t.Error("Expected error when accessing invalid aggregate field index")
		}
	})
}

func TestBooleanAggregator_Iterator_Operations(t *testing.T) {
	agg, err := NewBooleanAggregator(NoGrouping, types.StringType, 0, And)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}

	td, err := tuple.NewTupleDesc([]types.Type{types.BoolType}, []string{"value"})
	if err != nil {
		t.Fatalf("Failed to create tuple description: %v", err)
	}

	tup := tuple.NewTuple(td)
	err = tup.SetField(0, types.NewBoolField(true))
	if err != nil {
		t.Fatalf("Failed to set field: %v", err)
	}
	err = agg.Merge(tup)
	if err != nil {
		t.Fatalf("Failed to merge tuple: %v", err)
	}

	iter := agg.Iterator()

	t.Run("operations on closed iterator", func(t *testing.T) {
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
	})

	t.Run("double open", func(t *testing.T) {
		err := iter.Open()
		if err != nil {
			t.Fatalf("Failed to open iterator: %v", err)
		}

		err = iter.Open()
		if err == nil {
			t.Error("Expected error when opening already opened iterator")
		}

		iter.Close()
	})

	t.Run("rewind functionality", func(t *testing.T) {
		newIter := agg.Iterator()
		err := newIter.Open()
		if err != nil {
			t.Fatalf("Failed to open iterator: %v", err)
		}
		defer newIter.Close()

		hasNext, err := newIter.HasNext()
		if err != nil || !hasNext {
			t.Fatal("Expected tuple to be available")
		}

		_, err = newIter.Next()
		if err != nil {
			t.Fatalf("Failed to get next tuple: %v", err)
		}

		hasNext, err = newIter.HasNext()
		if err != nil {
			t.Fatalf("Error checking HasNext: %v", err)
		}
		if hasNext {
			t.Error("Expected no more tuples")
		}

		err = newIter.Rewind()
		if err != nil {
			t.Fatalf("Failed to rewind: %v", err)
		}

		hasNext, err = newIter.HasNext()
		if err != nil || !hasNext {
			t.Fatal("Expected tuple to be available after rewind")
		}
	})
}

func TestBooleanAggregator_Concurrent(t *testing.T) {
	agg, err := NewBooleanAggregator(0, types.StringType, 1, Sum)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}

	td, err := tuple.NewTupleDesc(
		[]types.Type{types.StringType, types.BoolType},
		[]string{"group", "value"},
	)
	if err != nil {
		t.Fatalf("Failed to create tuple description: %v", err)
	}

	const numGoroutines = 10
	const valuesPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			
			for j := 0; j < valuesPerGoroutine; j++ {
				tup := tuple.NewTuple(td)
				group := "group1"
				value := true

				err := tup.SetField(0, types.NewStringField(group, len(group)))
				if err != nil {
					t.Errorf("Failed to set group field: %v", err)
					return
				}
				err = tup.SetField(1, types.NewBoolField(value))
				if err != nil {
					t.Errorf("Failed to set value field: %v", err)
					return
				}

				err = agg.Merge(tup)
				if err != nil {
					t.Errorf("Failed to merge tuple: %v", err)
					return
				}
			}
		}(i)
	}

	wg.Wait()

	iter := agg.Iterator()
	err = iter.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}
	defer iter.Close()

	hasNext, err := iter.HasNext()
	if err != nil || !hasNext {
		t.Fatal("Expected at least one result")
	}

	result, err := iter.Next()
	if err != nil {
		t.Fatalf("Failed to get result: %v", err)
	}

	valueField, err := result.GetField(1)
	if err != nil {
		t.Fatalf("Failed to get value field: %v", err)
	}

	intField, ok := valueField.(*types.IntField)
	if !ok {
		t.Fatal("Value field is not an integer")
	}

	expected := int32(numGoroutines * valuesPerGoroutine)
	if intField.Value != expected {
		t.Errorf("Expected sum %d, got %d", expected, intField.Value)
	}
}

func TestBooleanAggregator_TupleDesc(t *testing.T) {
	t.Run("no grouping tuple desc - boolean result", func(t *testing.T) {
		agg, err := NewBooleanAggregator(NoGrouping, types.StringType, 0, And)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		td := agg.GetTupleDesc()
		if td.NumFields() != 1 {
			t.Errorf("Expected 1 field, got %d", td.NumFields())
		}

		fieldType, err := td.TypeAtIndex(0)
		if err != nil {
			t.Fatalf("Failed to get field type: %v", err)
		}
		if fieldType != types.BoolType {
			t.Errorf("Expected BoolType, got %v", fieldType)
		}

		fieldName, err := td.GetFieldName(0)
		if err != nil {
			t.Fatalf("Failed to get field name: %v", err)
		}
		if fieldName != "AND" {
			t.Errorf("Expected field name 'AND', got '%s'", fieldName)
		}
	})

	t.Run("no grouping tuple desc - integer result", func(t *testing.T) {
		agg, err := NewBooleanAggregator(NoGrouping, types.StringType, 0, Count)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		td := agg.GetTupleDesc()
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

		fieldName, err := td.GetFieldName(0)
		if err != nil {
			t.Fatalf("Failed to get field name: %v", err)
		}
		if fieldName != "COUNT" {
			t.Errorf("Expected field name 'COUNT', got '%s'", fieldName)
		}
	})

	t.Run("with grouping tuple desc", func(t *testing.T) {
		agg, err := NewBooleanAggregator(0, types.StringType, 1, Or)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		td := agg.GetTupleDesc()
		if td.NumFields() != 2 {
			t.Errorf("Expected 2 fields, got %d", td.NumFields())
		}

		groupFieldType, err := td.TypeAtIndex(0)
		if err != nil {
			t.Fatalf("Failed to get group field type: %v", err)
		}
		if groupFieldType != types.StringType {
			t.Errorf("Expected StringType for group field, got %v", groupFieldType)
		}

		aggFieldType, err := td.TypeAtIndex(1)
		if err != nil {
			t.Fatalf("Failed to get aggregate field type: %v", err)
		}
		if aggFieldType != types.BoolType {
			t.Errorf("Expected BoolType for aggregate field, got %v", aggFieldType)
		}

		groupFieldName, err := td.GetFieldName(0)
		if err != nil {
			t.Fatalf("Failed to get group field name: %v", err)
		}
		if groupFieldName != "group" {
			t.Errorf("Expected group field name 'group', got '%s'", groupFieldName)
		}

		aggFieldName, err := td.GetFieldName(1)
		if err != nil {
			t.Fatalf("Failed to get aggregate field name: %v", err)
		}
		if aggFieldName != "OR" {
			t.Errorf("Expected aggregate field name 'OR', got '%s'", aggFieldName)
		}
	})
}

// Additional comprehensive iterator tests for BooleanAggregatorIterator

func TestBooleanAggregatorIterator_Lifecycle_Detailed(t *testing.T) {
	agg, err := NewBooleanAggregator(NoGrouping, types.StringType, 0, And)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}

	// Add test data
	td, err := tuple.NewTupleDesc([]types.Type{types.BoolType}, []string{"value"})
	if err != nil {
		t.Fatalf("Failed to create tuple description: %v", err)
	}

	values := []bool{true, true, false}
	for _, value := range values {
		tup := tuple.NewTuple(td)
		err := tup.SetField(0, types.NewBoolField(value))
		if err != nil {
			t.Fatalf("Failed to set field: %v", err)
		}
		err = agg.Merge(tup)
		if err != nil {
			t.Fatalf("Failed to merge tuple: %v", err)
		}
	}

	iter := agg.Iterator()

	t.Run("operations_on_unopened_iterator", func(t *testing.T) {
		// Test GetTupleDesc on unopened iterator (should work)
		td := iter.GetTupleDesc()
		if td == nil {
			t.Error("Expected tuple description even on unopened iterator")
		}

		// Test operations that should fail on unopened iterator
		_, err := iter.HasNext()
		if err == nil {
			t.Error("Expected error when calling HasNext on unopened iterator")
		}

		_, err = iter.Next()
		if err == nil {
			t.Error("Expected error when calling Next on unopened iterator")
		}

		err = iter.Rewind()
		if err == nil {
			t.Error("Expected error when calling Rewind on unopened iterator")
		}
	})

	t.Run("normal_lifecycle", func(t *testing.T) {
		// Open iterator
		err = iter.Open()
		if err != nil {
			t.Fatalf("Failed to open iterator: %v", err)
		}

		// Test double open
		err = iter.Open()
		if err == nil {
			t.Error("Expected error when opening already opened iterator")
		}

		// Test normal iteration
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

		// Verify result (AND of true, true, false = false)
		field, err := result.GetField(0)
		if err != nil {
			t.Fatalf("Failed to get field: %v", err)
		}
		boolField, ok := field.(*types.BoolField)
		if !ok {
			t.Fatal("Expected BoolField")
		}
		if boolField.Value != false {
			t.Errorf("Expected AND result false, got %v", boolField.Value)
		}

		// Should have no more results
		hasNext, err = iter.HasNext()
		if err != nil {
			t.Fatalf("Error checking HasNext: %v", err)
		}
		if hasNext {
			t.Error("Expected no more results")
		}

		// Test close
		err = iter.Close()
		if err != nil {
			t.Fatalf("Failed to close iterator: %v", err)
		}

		// Test operations on closed iterator
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

func TestBooleanAggregatorIterator_WithGrouping_Comprehensive(t *testing.T) {
	agg, err := NewBooleanAggregator(0, types.StringType, 1, Or)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}

	// Add test data with multiple groups
	td, err := tuple.NewTupleDesc(
		[]types.Type{types.StringType, types.BoolType},
		[]string{"group", "value"},
	)
	if err != nil {
		t.Fatalf("Failed to create tuple description: %v", err)
	}

	testData := []struct {
		group string
		value bool
	}{
		{"A", false},
		{"B", true},
		{"A", false},
		{"C", false},
		{"B", false},
		{"A", true}, // A: false OR false OR true = true
		{"D", true}, // D: true
	}

	for _, data := range testData {
		tup := tuple.NewTuple(td)
		err := tup.SetField(0, types.NewStringField(data.group, len(data.group)))
		if err != nil {
			t.Fatalf("Failed to set group field: %v", err)
		}
		err = tup.SetField(1, types.NewBoolField(data.value))
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

	// Test GetTupleDesc on opened iterator
	td = iter.GetTupleDesc()
	if td.NumFields() != 2 {
		t.Errorf("Expected 2 fields in tuple desc, got %d", td.NumFields())
	}

	// Collect all results - test multiple iterations
	results := make(map[string]bool)
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

		resultCount++

		groupField, err := result.GetField(0)
		if err != nil {
			t.Fatalf("Failed to get group field: %v", err)
		}
		valueField, err := result.GetField(1)
		if err != nil {
			t.Fatalf("Failed to get value field: %v", err)
		}

		groupStr := groupField.String()
		boolField, ok := valueField.(*types.BoolField)
		if !ok {
			t.Fatal("Expected BoolField for value")
		}

		results[groupStr] = boolField.Value
	}

	// Verify expected results
	expectedResults := map[string]bool{
		"A": true,  // false OR false OR true = true
		"B": true,  // true OR false = true
		"C": false, // false
		"D": true,  // true
	}

	if len(results) != len(expectedResults) {
		t.Errorf("Expected %d groups, got %d", len(expectedResults), len(results))
	}

	for group, expectedResult := range expectedResults {
		if actualResult, exists := results[group]; !exists {
			t.Errorf("Missing group %s", group)
		} else if actualResult != expectedResult {
			t.Errorf("Group %s: expected %v, got %v", group, expectedResult, actualResult)
		}
	}

	// Test rewind and re-iteration
	err = iter.Rewind()
	if err != nil {
		t.Fatalf("Failed to rewind: %v", err)
	}

	rewindResults := make(map[string]bool)
	rewindCount := 0

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

		rewindCount++

		groupField, err := result.GetField(0)
		if err != nil {
			t.Fatalf("Failed to get group field after rewind: %v", err)
		}
		valueField, err := result.GetField(1)
		if err != nil {
			t.Fatalf("Failed to get value field after rewind: %v", err)
		}

		groupStr := groupField.String()
		boolField, ok := valueField.(*types.BoolField)
		if !ok {
			t.Fatal("Expected BoolField for value after rewind")
		}

		rewindResults[groupStr] = boolField.Value
	}

	if rewindCount != resultCount {
		t.Errorf("Expected same number of results after rewind: %d vs %d", rewindCount, resultCount)
	}

	// Results should be identical
	for group, expectedResult := range results {
		if actualResult, exists := rewindResults[group]; !exists {
			t.Errorf("Missing group %s after rewind", group)
		} else if actualResult != expectedResult {
			t.Errorf("Group %s after rewind: expected %v, got %v", group, expectedResult, actualResult)
		}
	}
}

func TestBooleanAggregatorIterator_EmptyAggregator(t *testing.T) {
	agg, err := NewBooleanAggregator(NoGrouping, types.StringType, 0, Or)
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
		t.Fatalf("Error checking HasNext on empty aggregator: %v", err)
	}
	if hasNext {
		t.Error("Expected no results for empty aggregator")
	}

	_, err = iter.Next()
	if err == nil {
		t.Error("Expected error when calling Next on empty iterator")
	}

	// Test rewind on empty iterator
	err = iter.Rewind()
	if err != nil {
		t.Fatalf("Failed to rewind empty iterator: %v", err)
	}

	hasNext, err = iter.HasNext()
	if err != nil {
		t.Fatalf("Error checking HasNext after rewind on empty aggregator: %v", err)
	}
	if hasNext {
		t.Error("Expected no results for empty aggregator after rewind")
	}
}

func TestBooleanAggregatorIterator_HasNext_Next_Patterns(t *testing.T) {
	agg, err := NewBooleanAggregator(NoGrouping, types.StringType, 0, Count)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}

	// Add test data
	td, err := tuple.NewTupleDesc([]types.Type{types.BoolType}, []string{"value"})
	if err != nil {
		t.Fatalf("Failed to create tuple description: %v", err)
	}

	values := []bool{true, false, true}
	for _, value := range values {
		tup := tuple.NewTuple(td)
		err := tup.SetField(0, types.NewBoolField(value))
		if err != nil {
			t.Fatalf("Failed to set field: %v", err)
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

	t.Run("next_without_hasNext", func(t *testing.T) {
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
			t.Fatal("Expected IntField for Count operation")
		}
		if intField.Value != 3 {
			t.Errorf("Expected count 3, got %d", intField.Value)
		}

		// Should be at end now
		hasNext, err := iter.HasNext()
		if err != nil {
			t.Fatalf("Error checking HasNext: %v", err)
		}
		if hasNext {
			t.Error("Expected no more results")
		}
	})

	t.Run("multiple_hasNext_calls", func(t *testing.T) {
		// Rewind for fresh test
		err = iter.Rewind()
		if err != nil {
			t.Fatalf("Failed to rewind: %v", err)
		}

		// Call HasNext multiple times
		for i := 0; i < 5; i++ {
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
			t.Fatal("Expected IntField for Count operation")
		}
		if intField.Value != 3 {
			t.Errorf("Expected count 3, got %d", intField.Value)
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
	})
}

func TestBooleanAggregatorIterator_FieldTypes_Verification(t *testing.T) {
	tests := []struct {
		name         string
		op           AggregateOp
		values       []bool
		expectedType string
		expectedVal  interface{}
	}{
		{
			name:         "And_operation_boolean_result",
			op:           And,
			values:       []bool{true, true},
			expectedType: "bool",
			expectedVal:  true,
		},
		{
			name:         "Or_operation_boolean_result",
			op:           Or,
			values:       []bool{false, true},
			expectedType: "bool",
			expectedVal:  true,
		},
		{
			name:         "Sum_operation_int_result",
			op:           Sum,
			values:       []bool{true, false, true},
			expectedType: "int",
			expectedVal:  int32(2),
		},
		{
			name:         "Count_operation_int_result",
			op:           Count,
			values:       []bool{true, false, true},
			expectedType: "int",
			expectedVal:  int32(3),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			agg, err := NewBooleanAggregator(NoGrouping, types.StringType, 0, test.op)
			if err != nil {
				t.Fatalf("Failed to create aggregator: %v", err)
			}

			// Add test data
			td, err := tuple.NewTupleDesc([]types.Type{types.BoolType}, []string{"value"})
			if err != nil {
				t.Fatalf("Failed to create tuple description: %v", err)
			}

			for _, value := range test.values {
				tup := tuple.NewTuple(td)
				err := tup.SetField(0, types.NewBoolField(value))
				if err != nil {
					t.Fatalf("Failed to set field: %v", err)
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

			hasNext, err := iter.HasNext()
			if err != nil {
				t.Fatalf("Error checking HasNext: %v", err)
			}
			if !hasNext {
				t.Fatal("Expected at least one result")
			}

			result, err := iter.Next()
			if err != nil {
				t.Fatalf("Failed to get next tuple: %v", err)
			}

			field, err := result.GetField(0)
			if err != nil {
				t.Fatalf("Failed to get field: %v", err)
			}

			switch test.expectedType {
			case "bool":
				boolField, ok := field.(*types.BoolField)
				if !ok {
					t.Fatalf("Expected BoolField, got %T", field)
				}
				expectedBool := test.expectedVal.(bool)
				if boolField.Value != expectedBool {
					t.Errorf("Expected %v, got %v", expectedBool, boolField.Value)
				}
			case "int":
				intField, ok := field.(*types.IntField)
				if !ok {
					t.Fatalf("Expected IntField, got %T", field)
				}
				expectedInt := test.expectedVal.(int32)
				if intField.Value != expectedInt {
					t.Errorf("Expected %d, got %d", expectedInt, intField.Value)
				}
			default:
				t.Fatalf("Unknown expected type: %s", test.expectedType)
			}
		})
	}
}