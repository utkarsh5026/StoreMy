package aggregation

import (
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"sync"
	"testing"
)

func TestNewIntAggregator(t *testing.T) {
	tests := []struct {
		name        string
		gbField     int
		gbFieldType types.Type
		aField      int
		op          AggregateOp
		expectError bool
	}{
		{
			name:        "valid aggregator without grouping",
			gbField:     NoGrouping,
			gbFieldType: types.StringType,
			aField:      0,
			op:          Sum,
			expectError: false,
		},
		{
			name:        "valid aggregator with grouping",
			gbField:     0,
			gbFieldType: types.StringType,
			aField:      1,
			op:          Max,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agg, err := NewIntAggregator(tt.gbField, tt.gbFieldType, tt.aField, tt.op)
			
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

func TestIntegerAggregator_NoGrouping_Operations(t *testing.T) {
	operations := []struct {
		op       AggregateOp
		values   []int32
		expected int32
	}{
		{Min, []int32{5, 2, 8, 1, 9}, 1},
		{Max, []int32{5, 2, 8, 1, 9}, 9},
		{Sum, []int32{5, 2, 8, 1, 9}, 25},
		{Avg, []int32{10, 20, 30}, 20},
		{Count, []int32{5, 2, 8, 1, 9}, 5},
	}

	for _, op := range operations {
		t.Run(op.op.String(), func(t *testing.T) {
			agg, err := NewIntAggregator(NoGrouping, types.StringType, 0, op.op)
			if err != nil {
				t.Fatalf("Failed to create aggregator: %v", err)
			}

			td, err := tuple.NewTupleDesc([]types.Type{types.IntType}, []string{"value"})
			if err != nil {
				t.Fatalf("Failed to create tuple description: %v", err)
			}

			for _, value := range op.values {
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

			intField, ok := field.(*types.IntField)
			if !ok {
				t.Fatal("Result field is not an integer")
			}

			if intField.Value != op.expected {
				t.Errorf("Expected %d, got %d", op.expected, intField.Value)
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

func TestIntegerAggregator_WithGrouping(t *testing.T) {
	agg, err := NewIntAggregator(0, types.StringType, 1, Sum)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}

	td, err := tuple.NewTupleDesc(
		[]types.Type{types.StringType, types.IntType},
		[]string{"group", "value"},
	)
	if err != nil {
		t.Fatalf("Failed to create tuple description: %v", err)
	}

	testData := []struct {
		group string
		value int32
	}{
		{"A", 10},
		{"B", 20},
		{"A", 15},
		{"C", 30},
		{"B", 25},
		{"A", 5},
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

	expectedSums := map[string]int32{
		"A": 30, // 10 + 15 + 5
		"B": 45, // 20 + 25
		"C": 30, // 30
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

	if len(results) != len(expectedSums) {
		t.Errorf("Expected %d groups, got %d", len(expectedSums), len(results))
	}

	for group, expectedSum := range expectedSums {
		if actualSum, exists := results[group]; !exists {
			t.Errorf("Missing group %s", group)
		} else if actualSum != expectedSum {
			t.Errorf("Group %s: expected sum %d, got %d", group, expectedSum, actualSum)
		}
	}
}

func TestIntegerAggregator_Average(t *testing.T) {
	agg, err := NewIntAggregator(0, types.StringType, 1, Avg)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}

	td, err := tuple.NewTupleDesc(
		[]types.Type{types.StringType, types.IntType},
		[]string{"group", "value"},
	)
	if err != nil {
		t.Fatalf("Failed to create tuple description: %v", err)
	}

	testData := []struct {
		group string
		value int32
	}{
		{"A", 10},
		{"A", 20},
		{"A", 30}, // Average: 20
		{"B", 5},
		{"B", 15}, // Average: 10
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

	expectedAvgs := map[string]int32{
		"A": 20, // (10 + 20 + 30) / 3
		"B": 10, // (5 + 15) / 2
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

	for group, expectedAvg := range expectedAvgs {
		if actualAvg, exists := results[group]; !exists {
			t.Errorf("Missing group %s", group)
		} else if actualAvg != expectedAvg {
			t.Errorf("Group %s: expected average %d, got %d", group, expectedAvg, actualAvg)
		}
	}
}

func TestIntegerAggregator_EdgeCases(t *testing.T) {
	t.Run("empty aggregation", func(t *testing.T) {
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
			t.Error("Expected no results for empty aggregation")
		}
	})

	t.Run("invalid aggregate field", func(t *testing.T) {
		agg, err := NewIntAggregator(NoGrouping, types.StringType, 0, Sum)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		td, err := tuple.NewTupleDesc([]types.Type{types.StringType}, []string{"text"})
		if err != nil {
			t.Fatalf("Failed to create tuple description: %v", err)
		}

		tup := tuple.NewTuple(td)
		err = tup.SetField(0, types.NewStringField("not_an_int", 10))
		if err != nil {
			t.Fatalf("Failed to set field: %v", err)
		}

		err = agg.Merge(tup)
		if err == nil {
			t.Error("Expected error when merging non-integer field")
		}
	})

	t.Run("invalid group field index", func(t *testing.T) {
		agg, err := NewIntAggregator(5, types.StringType, 0, Sum)
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
			t.Error("Expected error when accessing invalid group field index")
		}
	})
}

func TestIntegerAggregator_Iterator_Operations(t *testing.T) {
	agg, err := NewIntAggregator(NoGrouping, types.StringType, 0, Sum)
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
		// Create a fresh iterator for this test
		newIter := agg.Iterator()
		err := newIter.Open()
		if err != nil {
			t.Fatalf("Failed to open iterator: %v", err)
		}
		defer newIter.Close()

		// Read the first tuple
		hasNext, err := newIter.HasNext()
		if err != nil || !hasNext {
			t.Fatal("Expected tuple to be available")
		}

		_, err = newIter.Next()
		if err != nil {
			t.Fatalf("Failed to get next tuple: %v", err)
		}

		// Should be at end now
		hasNext, err = newIter.HasNext()
		if err != nil {
			t.Fatalf("Error checking HasNext: %v", err)
		}
		if hasNext {
			t.Error("Expected no more tuples")
		}

		// Rewind and check again
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

func TestIntegerAggregator_Concurrent(t *testing.T) {
	agg, err := NewIntAggregator(0, types.StringType, 1, Sum)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}

	td, err := tuple.NewTupleDesc(
		[]types.Type{types.StringType, types.IntType},
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
				value := int32(1)

				err := tup.SetField(0, types.NewStringField(group, len(group)))
				if err != nil {
					t.Errorf("Failed to set group field: %v", err)
					return
				}
				err = tup.SetField(1, types.NewIntField(value))
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

func TestIntegerAggregator_TupleDesc(t *testing.T) {
	t.Run("no grouping tuple desc", func(t *testing.T) {
		agg, err := NewIntAggregator(NoGrouping, types.StringType, 0, Sum)
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
		if fieldName != "SUM" {
			t.Errorf("Expected field name 'SUM', got '%s'", fieldName)
		}
	})

	t.Run("with grouping tuple desc", func(t *testing.T) {
		agg, err := NewIntAggregator(0, types.StringType, 1, Max)
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
		if aggFieldType != types.IntType {
			t.Errorf("Expected IntType for aggregate field, got %v", aggFieldType)
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
		if aggFieldName != "MAX" {
			t.Errorf("Expected aggregate field name 'MAX', got '%s'", aggFieldName)
		}
	})
}