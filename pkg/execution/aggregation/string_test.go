package aggregation

import (
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"sync"
	"testing"
)

func TestNewStringAggregator(t *testing.T) {
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
			name:        "valid aggregator with grouping - Min",
			gbField:     0,
			gbFieldType: types.StringType,
			aField:      1,
			op:          Min,
			expectError: false,
		},
		{
			name:        "valid aggregator - Max",
			gbField:     NoGrouping,
			gbFieldType: types.StringType,
			aField:      0,
			op:          Max,
			expectError: false,
		},
		{
			name:        "unsupported operation - Sum",
			gbField:     NoGrouping,
			gbFieldType: types.StringType,
			aField:      0,
			op:          Sum,
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
		{
			name:        "unsupported operation - And",
			gbField:     NoGrouping,
			gbFieldType: types.StringType,
			aField:      0,
			op:          And,
			expectError: true,
		},
		{
			name:        "unsupported operation - Or",
			gbField:     NoGrouping,
			gbFieldType: types.StringType,
			aField:      0,
			op:          Or,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agg, err := NewStringAggregator(tt.gbField, tt.gbFieldType, tt.aField, tt.op)
			
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

func TestStringAggregator_NoGrouping_Operations(t *testing.T) {
	operations := []struct {
		op       AggregateOp
		values   []string
		expected interface{}
	}{
		{Count, []string{"apple", "banana", "cherry"}, int32(3)},
		{Count, []string{"hello", "world"}, int32(2)},
		{Count, []string{}, int32(0)}, // empty case handled by iterator
		{Min, []string{"zebra", "apple", "banana"}, "apple"},
		{Min, []string{"charlie", "alpha", "beta"}, "alpha"},
		{Min, []string{"z", "a", "m"}, "a"},
		{Max, []string{"zebra", "apple", "banana"}, "zebra"},
		{Max, []string{"charlie", "alpha", "beta"}, "charlie"},
		{Max, []string{"z", "a", "m"}, "z"},
	}

	for _, op := range operations {
		if len(op.values) == 0 {
			continue // Skip empty test for now
		}
		
		t.Run(op.op.String()+"_NoGrouping", func(t *testing.T) {
			agg, err := NewStringAggregator(NoGrouping, types.StringType, 0, op.op)
			if err != nil {
				t.Fatalf("Failed to create aggregator: %v", err)
			}

			td, err := tuple.NewTupleDesc([]types.Type{types.StringType}, []string{"value"})
			if err != nil {
				t.Fatalf("Failed to create tuple description: %v", err)
			}

			for _, value := range op.values {
				tup := tuple.NewTuple(td)
				err := tup.SetField(0, types.NewStringField(value, len(value)))
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
			case string:
				stringField, ok := field.(*types.StringField)
				if !ok {
					t.Fatal("Result field is not a string")
				}
				if stringField.Value != expectedVal {
					t.Errorf("Expected %v, got %v", expectedVal, stringField.Value)
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

func TestStringAggregator_WithGrouping(t *testing.T) {
	agg, err := NewStringAggregator(0, types.StringType, 1, Min)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}

	td, err := tuple.NewTupleDesc(
		[]types.Type{types.StringType, types.StringType},
		[]string{"group", "value"},
	)
	if err != nil {
		t.Fatalf("Failed to create tuple description: %v", err)
	}

	testData := []struct {
		group string
		value string
	}{
		{"fruits", "apple"},
		{"colors", "red"},
		{"fruits", "banana"},
		{"animals", "zebra"},
		{"colors", "blue"},
		{"fruits", "cherry"}, // fruits: min("apple", "banana", "cherry") = "apple"
		{"animals", "ant"},   // animals: min("zebra", "ant") = "ant"
	}

	for _, data := range testData {
		tup := tuple.NewTuple(td)
		err := tup.SetField(0, types.NewStringField(data.group, len(data.group)))
		if err != nil {
			t.Fatalf("Failed to set group field: %v", err)
		}
		err = tup.SetField(1, types.NewStringField(data.value, len(data.value)))
		if err != nil {
			t.Fatalf("Failed to set value field: %v", err)
		}

		err = agg.Merge(tup)
		if err != nil {
			t.Fatalf("Failed to merge tuple: %v", err)
		}
	}

	expectedResults := map[string]string{
		"fruits":  "apple",  // min("apple", "banana", "cherry") = "apple"
		"colors":  "blue",   // min("red", "blue") = "blue"
		"animals": "ant",    // min("zebra", "ant") = "ant"
	}

	iter := agg.Iterator()
	err = iter.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}
	defer iter.Close()

	results := make(map[string]string)
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
		stringField, ok := valueField.(*types.StringField)
		if !ok {
			t.Fatal("Value field is not a string")
		}

		results[groupStr] = stringField.Value
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

func TestStringAggregator_MaxOperation(t *testing.T) {
	agg, err := NewStringAggregator(0, types.StringType, 1, Max)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}

	td, err := tuple.NewTupleDesc(
		[]types.Type{types.StringType, types.StringType},
		[]string{"group", "value"},
	)
	if err != nil {
		t.Fatalf("Failed to create tuple description: %v", err)
	}

	testData := []struct {
		group string
		value string
	}{
		{"A", "apple"},
		{"A", "zebra"},
		{"A", "banana"}, // A: max("apple", "zebra", "banana") = "zebra"
		{"B", "cat"},
		{"B", "dog"}, // B: max("cat", "dog") = "dog"
	}

	for _, data := range testData {
		tup := tuple.NewTuple(td)
		err := tup.SetField(0, types.NewStringField(data.group, len(data.group)))
		if err != nil {
			t.Fatalf("Failed to set group field: %v", err)
		}
		err = tup.SetField(1, types.NewStringField(data.value, len(data.value)))
		if err != nil {
			t.Fatalf("Failed to set value field: %v", err)
		}

		err = agg.Merge(tup)
		if err != nil {
			t.Fatalf("Failed to merge tuple: %v", err)
		}
	}

	expectedResults := map[string]string{
		"A": "zebra", // max("apple", "zebra", "banana") = "zebra"
		"B": "dog",   // max("cat", "dog") = "dog"
	}

	iter := agg.Iterator()
	err = iter.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}
	defer iter.Close()

	results := make(map[string]string)
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
		stringField, ok := valueField.(*types.StringField)
		if !ok {
			t.Fatal("Value field is not a string")
		}

		results[groupStr] = stringField.Value
	}

	for group, expectedResult := range expectedResults {
		if actualResult, exists := results[group]; !exists {
			t.Errorf("Missing group %s", group)
		} else if actualResult != expectedResult {
			t.Errorf("Group %s: expected %v, got %v", group, expectedResult, actualResult)
		}
	}
}

func TestStringAggregator_CountOperation(t *testing.T) {
	agg, err := NewStringAggregator(0, types.StringType, 1, Count)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}

	td, err := tuple.NewTupleDesc(
		[]types.Type{types.StringType, types.StringType},
		[]string{"group", "value"},
	)
	if err != nil {
		t.Fatalf("Failed to create tuple description: %v", err)
	}

	testData := []struct {
		group string
		value string
	}{
		{"A", "apple"},
		{"A", "banana"},
		{"A", "cherry"}, // A: 3 items
		{"B", "dog"},
		{"B", "cat"}, // B: 2 items
		{"C", "fish"}, // C: 1 item
	}

	for _, data := range testData {
		tup := tuple.NewTuple(td)
		err := tup.SetField(0, types.NewStringField(data.group, len(data.group)))
		if err != nil {
			t.Fatalf("Failed to set group field: %v", err)
		}
		err = tup.SetField(1, types.NewStringField(data.value, len(data.value)))
		if err != nil {
			t.Fatalf("Failed to set value field: %v", err)
		}

		err = agg.Merge(tup)
		if err != nil {
			t.Fatalf("Failed to merge tuple: %v", err)
		}
	}

	expectedCounts := map[string]int32{
		"A": 3, // 3 items
		"B": 2, // 2 items
		"C": 1, // 1 item
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

	for group, expectedCount := range expectedCounts {
		if actualCount, exists := results[group]; !exists {
			t.Errorf("Missing group %s", group)
		} else if actualCount != expectedCount {
			t.Errorf("Group %s: expected count %d, got %d", group, expectedCount, actualCount)
		}
	}
}

func TestStringAggregator_EdgeCases(t *testing.T) {
	t.Run("empty aggregation", func(t *testing.T) {
		agg, err := NewStringAggregator(NoGrouping, types.StringType, 0, Count)
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
		agg, err := NewStringAggregator(NoGrouping, types.StringType, 0, Count)
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
			t.Error("Expected error when merging non-string field")
		}
	})

	t.Run("invalid group field index", func(t *testing.T) {
		agg, err := NewStringAggregator(5, types.StringType, 0, Count)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		td, err := tuple.NewTupleDesc([]types.Type{types.StringType}, []string{"value"})
		if err != nil {
			t.Fatalf("Failed to create tuple description: %v", err)
		}

		tup := tuple.NewTuple(td)
		err = tup.SetField(0, types.NewStringField("test", 4))
		if err != nil {
			t.Fatalf("Failed to set field: %v", err)
		}

		err = agg.Merge(tup)
		if err == nil {
			t.Error("Expected error when accessing invalid group field index")
		}
	})

	t.Run("invalid aggregate field index", func(t *testing.T) {
		agg, err := NewStringAggregator(NoGrouping, types.StringType, 5, Count)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		td, err := tuple.NewTupleDesc([]types.Type{types.StringType}, []string{"value"})
		if err != nil {
			t.Fatalf("Failed to create tuple description: %v", err)
		}

		tup := tuple.NewTuple(td)
		err = tup.SetField(0, types.NewStringField("test", 4))
		if err != nil {
			t.Fatalf("Failed to set field: %v", err)
		}

		err = agg.Merge(tup)
		if err == nil {
			t.Error("Expected error when accessing invalid aggregate field index")
		}
	})

	t.Run("single value min/max", func(t *testing.T) {
		for _, op := range []AggregateOp{Min, Max} {
			t.Run(op.String(), func(t *testing.T) {
				agg, err := NewStringAggregator(NoGrouping, types.StringType, 0, op)
				if err != nil {
					t.Fatalf("Failed to create aggregator: %v", err)
				}

				td, err := tuple.NewTupleDesc([]types.Type{types.StringType}, []string{"value"})
				if err != nil {
					t.Fatalf("Failed to create tuple description: %v", err)
				}

				tup := tuple.NewTuple(td)
				err = tup.SetField(0, types.NewStringField("single", 6))
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

				stringField, ok := field.(*types.StringField)
				if !ok {
					t.Fatal("Result field is not a string")
				}
				if stringField.Value != "single" {
					t.Errorf("Expected 'single', got %v", stringField.Value)
				}
			})
		}
	})
}

func TestStringAggregator_Iterator_Operations(t *testing.T) {
	agg, err := NewStringAggregator(NoGrouping, types.StringType, 0, Count)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}

	td, err := tuple.NewTupleDesc([]types.Type{types.StringType}, []string{"value"})
	if err != nil {
		t.Fatalf("Failed to create tuple description: %v", err)
	}

	tup := tuple.NewTuple(td)
	err = tup.SetField(0, types.NewStringField("test", 4))
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

func TestStringAggregator_Concurrent(t *testing.T) {
	agg, err := NewStringAggregator(0, types.StringType, 1, Count)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}

	td, err := tuple.NewTupleDesc(
		[]types.Type{types.StringType, types.StringType},
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
				value := "test"

				err := tup.SetField(0, types.NewStringField(group, len(group)))
				if err != nil {
					t.Errorf("Failed to set group field: %v", err)
					return
				}
				err = tup.SetField(1, types.NewStringField(value, len(value)))
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
		t.Errorf("Expected count %d, got %d", expected, intField.Value)
	}
}

func TestStringAggregator_TupleDesc(t *testing.T) {
	t.Run("no grouping tuple desc - count result", func(t *testing.T) {
		agg, err := NewStringAggregator(NoGrouping, types.StringType, 0, Count)
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

	t.Run("no grouping tuple desc - min result", func(t *testing.T) {
		agg, err := NewStringAggregator(NoGrouping, types.StringType, 0, Min)
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
		if fieldType != types.StringType {
			t.Errorf("Expected StringType, got %v", fieldType)
		}

		fieldName, err := td.GetFieldName(0)
		if err != nil {
			t.Fatalf("Failed to get field name: %v", err)
		}
		if fieldName != "MIN" {
			t.Errorf("Expected field name 'MIN', got '%s'", fieldName)
		}
	})

	t.Run("with grouping tuple desc", func(t *testing.T) {
		agg, err := NewStringAggregator(0, types.StringType, 1, Max)
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
		if aggFieldType != types.StringType {
			t.Errorf("Expected StringType for aggregate field, got %v", aggFieldType)
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

func TestStringAggregator_LexicographicOrdering(t *testing.T) {
	t.Run("case sensitive ordering", func(t *testing.T) {
		values := []string{"apple", "Apple", "APPLE", "zebra", "Zebra"}
		
		// Test Min
		agg, err := NewStringAggregator(NoGrouping, types.StringType, 0, Min)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		td, err := tuple.NewTupleDesc([]types.Type{types.StringType}, []string{"value"})
		if err != nil {
			t.Fatalf("Failed to create tuple description: %v", err)
		}

		for _, value := range values {
			tup := tuple.NewTuple(td)
			err := tup.SetField(0, types.NewStringField(value, len(value)))
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
		if err != nil || !hasNext {
			t.Fatal("Expected result")
		}

		result, err := iter.Next()
		if err != nil {
			t.Fatalf("Failed to get next tuple: %v", err)
		}

		field, err := result.GetField(0)
		if err != nil {
			t.Fatalf("Failed to get result field: %v", err)
		}

		stringField, ok := field.(*types.StringField)
		if !ok {
			t.Fatal("Result field is not a string")
		}

		// In lexicographic order: "APPLE" < "Apple" < "Zebra" < "apple" < "zebra"
		expected := "APPLE"
		if stringField.Value != expected {
			t.Errorf("Expected min value '%s', got '%s'", expected, stringField.Value)
		}
	})
}