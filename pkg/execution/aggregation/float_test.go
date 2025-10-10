package aggregation

import (
	"math"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"sync"
	"testing"
)

func TestNewFloatAggregator(t *testing.T) {
	tests := []struct {
		name        string
		gbField     int
		gbFieldType types.Type
		aField      int
		op          AggregateOp
	}{
		{
			name:        "valid aggregator without grouping - Count",
			gbField:     NoGrouping,
			gbFieldType: types.FloatType,
			aField:      0,
			op:          Count,
		},
		{
			name:        "valid aggregator with grouping - Min",
			gbField:     0,
			gbFieldType: types.StringType,
			aField:      1,
			op:          Min,
		},
		{
			name:        "valid aggregator - Max",
			gbField:     NoGrouping,
			gbFieldType: types.FloatType,
			aField:      0,
			op:          Max,
		},
		{
			name:        "valid aggregator - Sum",
			gbField:     NoGrouping,
			gbFieldType: types.FloatType,
			aField:      0,
			op:          Sum,
		},
		{
			name:        "valid aggregator - Avg",
			gbField:     NoGrouping,
			gbFieldType: types.FloatType,
			aField:      0,
			op:          Avg,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agg, err := NewFloatAggregator(tt.gbField, tt.gbFieldType, tt.aField, tt.op)
			if err != nil {
				t.Fatalf("Failed to create aggregator: %v", err)
			}

			if agg == nil {
				t.Error("Expected aggregator but got nil")
			}
			if agg.gbField != tt.gbField {
				t.Errorf("Expected gbField %d, got %d", tt.gbField, agg.gbField)
			}
			if agg.aField != tt.aField {
				t.Errorf("Expected aField %d, got %d", tt.aField, agg.aField)
			}
			if agg.op != tt.op {
				t.Errorf("Expected op %v, got %v", tt.op, agg.op)
			}
		})
	}
}

func TestFloatAggregator_NoGrouping_Operations(t *testing.T) {
	operations := []struct {
		op       AggregateOp
		values   []float64
		expected interface{}
	}{
		{Count, []float64{1.5, 2.5, 3.5}, int64(3)},
		{Count, []float64{10.0, 20.0}, int64(2)},
		{Min, []float64{5.5, 1.2, 3.8}, 1.2},
		{Min, []float64{-10.5, -5.2, -8.1}, -10.5},
		{Min, []float64{100.0, 50.0, 75.0}, 50.0},
		{Max, []float64{5.5, 1.2, 3.8}, 5.5},
		{Max, []float64{-10.5, -5.2, -8.1}, -5.2},
		{Max, []float64{100.0, 50.0, 75.0}, 100.0},
		{Sum, []float64{1.5, 2.5, 3.0}, 7.0},
		{Sum, []float64{-1.0, 1.0, 0.5}, 0.5},
		{Sum, []float64{10.25, 20.75}, 31.0},
		{Avg, []float64{2.0, 4.0, 6.0}, 4.0},
		{Avg, []float64{1.0, 2.0, 3.0, 4.0}, 2.5},
		{Avg, []float64{10.5, 20.5}, 15.5},
	}

	for _, op := range operations {
		if len(op.values) == 0 {
			continue // Skip empty test for now
		}

		t.Run(op.op.String()+"_NoGrouping", func(t *testing.T) {
			agg, err := NewFloatAggregator(NoGrouping, types.FloatType, 0, op.op)
			if err != nil {
				t.Fatalf("Failed to create aggregator: %v", err)
			}
			if agg == nil {
				t.Fatal("Failed to create aggregator")
			}

			td, err := tuple.NewTupleDesc([]types.Type{types.FloatType}, []string{"value"})
			if err != nil {
				t.Fatalf("Failed to create tuple description: %v", err)
			}

			for _, value := range op.values {
				tup := tuple.NewTuple(td)
				err := tup.SetField(0, types.NewFloat64Field(value))
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
			case float64:
				floatField, ok := field.(*types.Float64Field)
				if !ok {
					t.Fatal("Result field is not a float")
				}
				if math.Abs(floatField.Value-expectedVal) > 1e-9 {
					t.Errorf("Expected %v, got %v", expectedVal, floatField.Value)
				}
			case int64:
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

func TestFloatAggregator_WithGrouping(t *testing.T) {
	agg, err := NewFloatAggregator(0, types.StringType, 1, Min)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}
	if agg == nil {
		t.Fatal("Failed to create aggregator")
	}

	td, err := tuple.NewTupleDesc(
		[]types.Type{types.StringType, types.FloatType},
		[]string{"group", "value"},
	)
	if err != nil {
		t.Fatalf("Failed to create tuple description: %v", err)
	}

	testData := []struct {
		group string
		value float64
	}{
		{"numbers", 5.5},
		{"scores", 85.0},
		{"numbers", 2.3},
		{"temps", 98.6},
		{"scores", 92.5},
		{"numbers", 7.8}, // numbers: min(5.5, 2.3, 7.8) = 2.3
		{"temps", 101.2}, // temps: min(98.6, 101.2) = 98.6
	}

	for _, data := range testData {
		tup := tuple.NewTuple(td)
		err := tup.SetField(0, types.NewStringField(data.group, len(data.group)))
		if err != nil {
			t.Fatalf("Failed to set group field: %v", err)
		}
		err = tup.SetField(1, types.NewFloat64Field(data.value))
		if err != nil {
			t.Fatalf("Failed to set value field: %v", err)
		}

		err = agg.Merge(tup)
		if err != nil {
			t.Fatalf("Failed to merge tuple: %v", err)
		}
	}

	expectedResults := map[string]float64{
		"numbers": 2.3,  // min(5.5, 2.3, 7.8) = 2.3
		"scores":  85.0, // min(85.0, 92.5) = 85.0
		"temps":   98.6, // min(98.6, 101.2) = 98.6
	}

	iter := agg.Iterator()
	err = iter.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}
	defer iter.Close()

	results := make(map[string]float64)
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
		floatField, ok := valueField.(*types.Float64Field)
		if !ok {
			t.Fatal("Value field is not a float")
		}

		results[groupStr] = floatField.Value
	}

	if len(results) != len(expectedResults) {
		t.Errorf("Expected %d groups, got %d", len(expectedResults), len(results))
	}

	for group, expectedResult := range expectedResults {
		if actualResult, exists := results[group]; !exists {
			t.Errorf("Missing group %s", group)
		} else if math.Abs(actualResult-expectedResult) > 1e-9 {
			t.Errorf("Group %s: expected %v, got %v", group, expectedResult, actualResult)
		}
	}
}

func TestFloatAggregator_SumOperation(t *testing.T) {
	agg, err := NewFloatAggregator(0, types.StringType, 1, Sum)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}
	if agg == nil {
		t.Fatal("Failed to create aggregator")
	}

	td, err := tuple.NewTupleDesc(
		[]types.Type{types.StringType, types.FloatType},
		[]string{"group", "value"},
	)
	if err != nil {
		t.Fatalf("Failed to create tuple description: %v", err)
	}

	testData := []struct {
		group string
		value float64
	}{
		{"A", 1.5},
		{"A", 2.5},
		{"A", 3.0}, // A: sum(1.5, 2.5, 3.0) = 7.0
		{"B", 10.0},
		{"B", 20.0}, // B: sum(10.0, 20.0) = 30.0
	}

	for _, data := range testData {
		tup := tuple.NewTuple(td)
		err := tup.SetField(0, types.NewStringField(data.group, len(data.group)))
		if err != nil {
			t.Fatalf("Failed to set group field: %v", err)
		}
		err = tup.SetField(1, types.NewFloat64Field(data.value))
		if err != nil {
			t.Fatalf("Failed to set value field: %v", err)
		}

		err = agg.Merge(tup)
		if err != nil {
			t.Fatalf("Failed to merge tuple: %v", err)
		}
	}

	expectedResults := map[string]float64{
		"A": 7.0,  // sum(1.5, 2.5, 3.0) = 7.0
		"B": 30.0, // sum(10.0, 20.0) = 30.0
	}

	iter := agg.Iterator()
	err = iter.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}
	defer iter.Close()

	results := make(map[string]float64)
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
		floatField, ok := valueField.(*types.Float64Field)
		if !ok {
			t.Fatal("Value field is not a float")
		}

		results[groupStr] = floatField.Value
	}

	for group, expectedResult := range expectedResults {
		if actualResult, exists := results[group]; !exists {
			t.Errorf("Missing group %s", group)
		} else if math.Abs(actualResult-expectedResult) > 1e-9 {
			t.Errorf("Group %s: expected %v, got %v", group, expectedResult, actualResult)
		}
	}
}

func TestFloatAggregator_AvgOperation(t *testing.T) {
	agg, err := NewFloatAggregator(0, types.StringType, 1, Avg)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}
	if agg == nil {
		t.Fatal("Failed to create aggregator")
	}

	td, err := tuple.NewTupleDesc(
		[]types.Type{types.StringType, types.FloatType},
		[]string{"group", "value"},
	)
	if err != nil {
		t.Fatalf("Failed to create tuple description: %v", err)
	}

	testData := []struct {
		group string
		value float64
	}{
		{"A", 2.0},
		{"A", 4.0},
		{"A", 6.0}, // A: avg(2.0, 4.0, 6.0) = 4.0
		{"B", 10.0},
		{"B", 20.0}, // B: avg(10.0, 20.0) = 15.0
	}

	for _, data := range testData {
		tup := tuple.NewTuple(td)
		err := tup.SetField(0, types.NewStringField(data.group, len(data.group)))
		if err != nil {
			t.Fatalf("Failed to set group field: %v", err)
		}
		err = tup.SetField(1, types.NewFloat64Field(data.value))
		if err != nil {
			t.Fatalf("Failed to set value field: %v", err)
		}

		err = agg.Merge(tup)
		if err != nil {
			t.Fatalf("Failed to merge tuple: %v", err)
		}
	}

	expectedResults := map[string]float64{
		"A": 4.0,  // avg(2.0, 4.0, 6.0) = 4.0
		"B": 15.0, // avg(10.0, 20.0) = 15.0
	}

	iter := agg.Iterator()
	err = iter.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}
	defer iter.Close()

	results := make(map[string]float64)
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
		floatField, ok := valueField.(*types.Float64Field)
		if !ok {
			t.Fatal("Value field is not a float")
		}

		results[groupStr] = floatField.Value
	}

	for group, expectedResult := range expectedResults {
		if actualResult, exists := results[group]; !exists {
			t.Errorf("Missing group %s", group)
		} else if math.Abs(actualResult-expectedResult) > 1e-9 {
			t.Errorf("Group %s: expected %v, got %v", group, expectedResult, actualResult)
		}
	}
}

func TestFloatAggregator_CountOperation(t *testing.T) {
	agg, err := NewFloatAggregator(0, types.StringType, 1, Count)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}
	if agg == nil {
		t.Fatal("Failed to create aggregator")
	}

	td, err := tuple.NewTupleDesc(
		[]types.Type{types.StringType, types.FloatType},
		[]string{"group", "value"},
	)
	if err != nil {
		t.Fatalf("Failed to create tuple description: %v", err)
	}

	testData := []struct {
		group string
		value float64
	}{
		{"A", 1.0},
		{"A", 2.0},
		{"A", 3.0}, // A: count = 3
		{"B", 10.0},
		{"B", 20.0}, // B: count = 2
	}

	for _, data := range testData {
		tup := tuple.NewTuple(td)
		err := tup.SetField(0, types.NewStringField(data.group, len(data.group)))
		if err != nil {
			t.Fatalf("Failed to set group field: %v", err)
		}
		err = tup.SetField(1, types.NewFloat64Field(data.value))
		if err != nil {
			t.Fatalf("Failed to set value field: %v", err)
		}

		err = agg.Merge(tup)
		if err != nil {
			t.Fatalf("Failed to merge tuple: %v", err)
		}
	}

	expectedResults := map[string]int64{
		"A": 3, // count = 3
		"B": 2, // count = 2
	}

	iter := agg.Iterator()
	err = iter.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}
	defer iter.Close()

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
			t.Fatal("Value field is not an integer")
		}

		results[groupStr] = intField.Value
	}

	for group, expectedResult := range expectedResults {
		if actualResult, exists := results[group]; !exists {
			t.Errorf("Missing group %s", group)
		} else if actualResult != expectedResult {
			t.Errorf("Group %s: expected %v, got %v", group, expectedResult, actualResult)
		}
	}
}

func TestFloatAggregator_EdgeCases(t *testing.T) {
	t.Run("invalid aggregate field type", func(t *testing.T) {
		agg, err := NewFloatAggregator(NoGrouping, types.FloatType, 0, Sum)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		td, err := tuple.NewTupleDesc([]types.Type{types.StringType}, []string{"value"})
		if err != nil {
			t.Fatalf("Failed to create tuple description: %v", err)
		}

		tup := tuple.NewTuple(td)
		err = tup.SetField(0, types.NewStringField("not a float", 11))
		if err != nil {
			t.Fatalf("Failed to set field: %v", err)
		}

		err = agg.Merge(tup)
		if err == nil {
			t.Error("Expected error for non-float field, got none")
		}
	})

	t.Run("invalid group field index", func(t *testing.T) {
		agg, err := NewFloatAggregator(5, types.StringType, 0, Sum) // index 5 doesn't exist
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		td, err := tuple.NewTupleDesc([]types.Type{types.FloatType}, []string{"value"})
		if err != nil {
			t.Fatalf("Failed to create tuple description: %v", err)
		}

		tup := tuple.NewTuple(td)
		err = tup.SetField(0, types.NewFloat64Field(1.0))
		if err != nil {
			t.Fatalf("Failed to set field: %v", err)
		}

		err = agg.Merge(tup)
		if err == nil {
			t.Error("Expected error for invalid group field index, got none")
		}
	})

	t.Run("invalid aggregate field index", func(t *testing.T) {
		agg, err := NewFloatAggregator(NoGrouping, types.FloatType, 5, Sum) // index 5 doesn't exist
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		td, err := tuple.NewTupleDesc([]types.Type{types.FloatType}, []string{"value"})
		if err != nil {
			t.Fatalf("Failed to create tuple description: %v", err)
		}

		tup := tuple.NewTuple(td)
		err = tup.SetField(0, types.NewFloat64Field(1.0))
		if err != nil {
			t.Fatalf("Failed to set field: %v", err)
		}

		err = agg.Merge(tup)
		if err == nil {
			t.Error("Expected error for invalid aggregate field index, got none")
		}
	})

	t.Run("single value operations", func(t *testing.T) {
		operations := []AggregateOp{Min, Max, Sum, Avg, Count}
		for _, op := range operations {
			t.Run(op.String(), func(t *testing.T) {
				agg, err := NewFloatAggregator(NoGrouping, types.FloatType, 0, op)
				if err != nil {
					t.Fatalf("Failed to create aggregator: %v", err)
				}

				td, err := tuple.NewTupleDesc([]types.Type{types.FloatType}, []string{"value"})
				if err != nil {
					t.Fatalf("Failed to create tuple description: %v", err)
				}

				tup := tuple.NewTuple(td)
				err = tup.SetField(0, types.NewFloat64Field(42.5))
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

				switch op {
				case Count:
					intField, ok := field.(*types.IntField)
					if !ok {
						t.Fatal("Result field is not an integer")
					}
					if intField.Value != 1 {
						t.Errorf("Expected count 1, got %d", intField.Value)
					}
				default:
					floatField, ok := field.(*types.Float64Field)
					if !ok {
						t.Fatal("Result field is not a float")
					}
					if math.Abs(floatField.Value-42.5) > 1e-9 {
						t.Errorf("Expected 42.5, got %v", floatField.Value)
					}
				}
			})
		}
	})
}

func TestFloatAggregator_Iterator_Operations(t *testing.T) {
	t.Run("operations on closed iterator", func(t *testing.T) {
		agg, err := NewFloatAggregator(NoGrouping, types.FloatType, 0, Sum)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}
		iter := agg.Iterator()

		// Try operations on closed iterator
		_, err = iter.HasNext()
		if err == nil {
			t.Error("Expected error for HasNext on closed iterator")
		}

		_, err = iter.Next()
		if err == nil {
			t.Error("Expected error for Next on closed iterator")
		}
	})

	t.Run("double open", func(t *testing.T) {
		agg, err := NewFloatAggregator(NoGrouping, types.FloatType, 0, Sum)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}
		iter := agg.Iterator()

		err = iter.Open()
		if err != nil {
			t.Fatalf("Failed to open iterator: %v", err)
		}
		defer iter.Close()

		err = iter.Open()
		if err == nil {
			t.Error("Expected error for double open")
		}
	})

	t.Run("rewind functionality", func(t *testing.T) {
		agg, err := NewFloatAggregator(NoGrouping, types.FloatType, 0, Count)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		td, err := tuple.NewTupleDesc([]types.Type{types.FloatType}, []string{"value"})
		if err != nil {
			t.Fatalf("Failed to create tuple description: %v", err)
		}

		// Add some data
		for i := 0; i < 3; i++ {
			tup := tuple.NewTuple(td)
			err = tup.SetField(0, types.NewFloat64Field(float64(i)))
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

		// Read once
		hasNext, err := iter.HasNext()
		if err != nil {
			t.Fatalf("Error checking HasNext: %v", err)
		}
		if !hasNext {
			t.Fatal("Expected at least one result")
		}

		_, err = iter.Next()
		if err != nil {
			t.Fatalf("Failed to get next tuple: %v", err)
		}

		// Should be at end now
		hasNext, err = iter.HasNext()
		if err != nil {
			t.Fatalf("Error checking HasNext: %v", err)
		}
		if hasNext {
			t.Error("Expected no more results")
		}

		// Rewind
		iter.Rewind()

		// Should be able to read again
		hasNext, err = iter.HasNext()
		if err != nil {
			t.Fatalf("Error checking HasNext: %v", err)
		}
		if !hasNext {
			t.Fatal("Expected result after rewind")
		}
	})
}

func TestFloatAggregator_Concurrent(t *testing.T) {
	agg, err := NewFloatAggregator(0, types.StringType, 1, Sum)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}

	td, err := tuple.NewTupleDesc(
		[]types.Type{types.StringType, types.FloatType},
		[]string{"group", "value"},
	)
	if err != nil {
		t.Fatalf("Failed to create tuple description: %v", err)
	}

	var wg sync.WaitGroup
	numGoroutines := 10
	valuesPerGoroutine := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < valuesPerGoroutine; j++ {
				tup := tuple.NewTuple(td)
				err := tup.SetField(0, types.NewStringField("group", 5))
				if err != nil {
					t.Errorf("Failed to set group field: %v", err)
					return
				}
				err = tup.SetField(1, types.NewFloat64Field(1.0))
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

	// Verify result
	expectedSum := float64(numGoroutines * valuesPerGoroutine)

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

	valueField, err := result.GetField(1)
	if err != nil {
		t.Fatalf("Failed to get value field: %v", err)
	}

	floatField, ok := valueField.(*types.Float64Field)
	if !ok {
		t.Fatal("Value field is not a float")
	}

	if math.Abs(floatField.Value-expectedSum) > 1e-9 {
		t.Errorf("Expected sum %v, got %v", expectedSum, floatField.Value)
	}
}

func TestFloatAggregator_TupleDesc(t *testing.T) {
	t.Run("no grouping tuple desc - count result", func(t *testing.T) {
		agg, err := NewFloatAggregator(NoGrouping, types.FloatType, 0, Count)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}
		desc := agg.GetTupleDesc()

		if desc.NumFields() != 1 {
			t.Errorf("Expected 1 field, got %d", desc.NumFields())
		}

		fieldType, err := desc.TypeAtIndex(0)
		if err != nil {
			t.Fatalf("Failed to get field type: %v", err)
		}
		if fieldType != types.IntType {
			t.Errorf("Expected IntType, got %v", fieldType)
		}

		fieldName, err := desc.GetFieldName(0)
		if err != nil {
			t.Fatalf("Failed to get field name: %v", err)
		}
		if fieldName != "COUNT" {
			t.Errorf("Expected field name 'COUNT', got '%s'", fieldName)
		}
	})

	t.Run("no grouping tuple desc - sum result", func(t *testing.T) {
		agg, err := NewFloatAggregator(NoGrouping, types.FloatType, 0, Sum)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}
		desc := agg.GetTupleDesc()

		if desc.NumFields() != 1 {
			t.Errorf("Expected 1 field, got %d", desc.NumFields())
		}

		fieldType, err := desc.TypeAtIndex(0)
		if err != nil {
			t.Fatalf("Failed to get field type: %v", err)
		}
		if fieldType != types.FloatType {
			t.Errorf("Expected FloatType, got %v", fieldType)
		}

		fieldName, err := desc.GetFieldName(0)
		if err != nil {
			t.Fatalf("Failed to get field name: %v", err)
		}
		if fieldName != "SUM" {
			t.Errorf("Expected field name 'SUM', got '%s'", fieldName)
		}
	})

	t.Run("with grouping tuple desc", func(t *testing.T) {
		agg, err := NewFloatAggregator(0, types.StringType, 1, Avg)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}
		desc := agg.GetTupleDesc()

		if desc.NumFields() != 2 {
			t.Errorf("Expected 2 fields, got %d", desc.NumFields())
		}

		groupFieldType, err := desc.TypeAtIndex(0)
		if err != nil {
			t.Fatalf("Failed to get group field type: %v", err)
		}
		if groupFieldType != types.StringType {
			t.Errorf("Expected StringType for group field, got %v", groupFieldType)
		}

		aggFieldType, err := desc.TypeAtIndex(1)
		if err != nil {
			t.Fatalf("Failed to get aggregate field type: %v", err)
		}
		if aggFieldType != types.FloatType {
			t.Errorf("Expected FloatType for aggregate field, got %v", aggFieldType)
		}

		groupFieldName, err := desc.GetFieldName(0)
		if err != nil {
			t.Fatalf("Failed to get group field name: %v", err)
		}
		if groupFieldName != "group" {
			t.Errorf("Expected group field name 'group', got '%s'", groupFieldName)
		}

		aggFieldName, err := desc.GetFieldName(1)
		if err != nil {
			t.Fatalf("Failed to get aggregate field name: %v", err)
		}
		if aggFieldName != "AVG" {
			t.Errorf("Expected aggregate field name 'AVG', got '%s'", aggFieldName)
		}
	})
}

func TestFloatAggregator_SpecialFloatValues(t *testing.T) {
	t.Run("infinity values", func(t *testing.T) {
		agg, err := NewFloatAggregator(NoGrouping, types.FloatType, 0, Max)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		td, err := tuple.NewTupleDesc([]types.Type{types.FloatType}, []string{"value"})
		if err != nil {
			t.Fatalf("Failed to create tuple description: %v", err)
		}

		values := []float64{1.0, math.Inf(1), 2.0, math.Inf(-1)}
		for _, value := range values {
			tup := tuple.NewTuple(td)
			err := tup.SetField(0, types.NewFloat64Field(value))
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

		floatField, ok := field.(*types.Float64Field)
		if !ok {
			t.Fatal("Result field is not a float")
		}

		if !math.IsInf(floatField.Value, 1) {
			t.Errorf("Expected +Inf, got %v", floatField.Value)
		}
	})

	t.Run("NaN values", func(t *testing.T) {
		agg, err := NewFloatAggregator(NoGrouping, types.FloatType, 0, Sum)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		td, err := tuple.NewTupleDesc([]types.Type{types.FloatType}, []string{"value"})
		if err != nil {
			t.Fatalf("Failed to create tuple description: %v", err)
		}

		values := []float64{1.0, math.NaN(), 2.0}
		for _, value := range values {
			tup := tuple.NewTuple(td)
			err := tup.SetField(0, types.NewFloat64Field(value))
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

		floatField, ok := field.(*types.Float64Field)
		if !ok {
			t.Fatal("Result field is not a float")
		}

		if !math.IsNaN(floatField.Value) {
			t.Errorf("Expected NaN, got %v", floatField.Value)
		}
	})
}
