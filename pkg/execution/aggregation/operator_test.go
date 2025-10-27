package aggregation

import (
	"fmt"
	"storemy/pkg/iterator"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

// mockIterator implements DbIterator for testing
type mockIterator struct {
	tuples   []*tuple.Tuple
	index    int
	isOpen   bool
	hasError bool
	td       *tuple.TupleDescription
}

func newMockIterator(tuples []*tuple.Tuple, td *tuple.TupleDescription) *mockIterator {
	return &mockIterator{
		tuples: tuples,
		index:  -1,
		td:     td,
	}
}

func (m *mockIterator) Open() error {
	if m.hasError {
		return fmt.Errorf("mock open error")
	}
	m.isOpen = true
	m.index = -1
	return nil
}

func (m *mockIterator) Close() error {
	m.isOpen = false
	return nil
}

func (m *mockIterator) HasNext() (bool, error) {
	if !m.isOpen {
		return false, fmt.Errorf("iterator not open")
	}
	if m.hasError {
		return false, fmt.Errorf("mock has next error")
	}
	return m.index+1 < len(m.tuples), nil
}

func (m *mockIterator) Next() (*tuple.Tuple, error) {
	if !m.isOpen {
		return nil, fmt.Errorf("iterator not open")
	}
	if m.hasError {
		return nil, fmt.Errorf("mock next error")
	}
	m.index++
	if m.index >= len(m.tuples) {
		return nil, fmt.Errorf("no more tuples")
	}
	return m.tuples[m.index], nil
}

func (m *mockIterator) GetTupleDesc() *tuple.TupleDescription {
	return m.td
}

func (m *mockIterator) Rewind() error {
	if !m.isOpen {
		return fmt.Errorf("iterator not open")
	}
	if m.hasError {
		return fmt.Errorf("mock rewind error")
	}
	m.index = -1
	return nil
}

func (m *mockIterator) setError(hasError bool) {
	m.hasError = hasError
}

// Helper functions
func createTestTupleDesc() *tuple.TupleDescription {
	td, _ := tuple.NewTupleDesc(
		[]types.Type{types.StringType, types.IntType},
		[]string{"group", "value"},
	)
	return td
}

func createTestTuples() []*tuple.Tuple {
	td := createTestTupleDesc()
	var tuples []*tuple.Tuple

	testData := []struct {
		group string
		value int64
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
		tup.SetField(0, types.NewStringField(data.group, len(data.group)))
		tup.SetField(1, types.NewIntField(data.value))
		tuples = append(tuples, tup)
	}

	return tuples
}

func TestNewAggregateOperator(t *testing.T) {
	td := createTestTupleDesc()
	tuples := createTestTuples()
	source := newMockIterator(tuples, td)

	tests := []struct {
		name        string
		source      iterator.DbIterator
		aField      primitives.ColumnID
		gField      primitives.ColumnID
		op          AggregateOp
		expectError bool
		errorMsg    string
	}{
		{
			name:        "valid operator with grouping",
			source:      source,
			aField:      1,
			gField:      0,
			op:          Sum,
			expectError: false,
		},
		{
			name:        "valid operator without grouping",
			source:      source,
			aField:      1,
			gField:      NoGrouping,
			op:          Max,
			expectError: false,
		},
		{
			name:        "nil source",
			source:      nil,
			aField:      1,
			gField:      0,
			op:          Sum,
			expectError: true,
			errorMsg:    "source iterator cannot be nil",
		},
		{
			name:        "invalid aggregate field",
			source:      source,
			aField:      5,
			gField:      0,
			op:          Sum,
			expectError: true,
			errorMsg:    "invalid aggregate field index",
		},
		{
			name:        "invalid group field",
			source:      source,
			aField:      1,
			gField:      5,
			op:          Sum,
			expectError: true,
			errorMsg:    "invalid group field index",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op, err := NewAggregateOperator(tt.source, tt.aField, tt.gField, tt.op)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				} else if tt.errorMsg != "" && err.Error() != tt.errorMsg && len(err.Error()) < len(tt.errorMsg) {
					// Check if error message contains expected substring
					found := false
					for i := 0; i <= len(err.Error())-len(tt.errorMsg); i++ {
						if err.Error()[i:i+len(tt.errorMsg)] == tt.errorMsg {
							found = true
							break
						}
					}
					if !found {
						t.Errorf("Expected error containing '%s', got '%s'", tt.errorMsg, err.Error())
					}
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if op == nil {
					t.Error("Expected operator but got nil")
				}
			}
		})
	}
}

func TestAggregateOperator_UnsupportedFieldType(t *testing.T) {
	// Create tuple desc with an invalid field type (use a type value that doesn't exist)
	td := &tuple.TupleDescription{
		Types:      []types.Type{types.Type(999)}, // Invalid type
		FieldNames: []string{"value"},
	}

	tup := tuple.NewTuple(td)

	source := newMockIterator([]*tuple.Tuple{tup}, td)

	_, err := NewAggregateOperator(source, 0, NoGrouping, Sum)
	if err == nil {
		t.Error("Expected error for unsupported field type")
	}
}

func TestAggregateOperator_StringUnsupportedOperation(t *testing.T) {
	// Create tuple desc with string field type but unsupported operation
	td, _ := tuple.NewTupleDesc(
		[]types.Type{types.StringType, types.StringType},
		[]string{"group", "text"},
	)

	tup := tuple.NewTuple(td)
	tup.SetField(0, types.NewStringField("A", 1))
	tup.SetField(1, types.NewStringField("text", 4))

	source := newMockIterator([]*tuple.Tuple{tup}, td)

	_, err := NewAggregateOperator(source, 1, 0, Sum)
	if err == nil {
		t.Error("Expected error for unsupported string operation")
	}
}

func TestAggregateOperator_BooleanAggregation_WithGrouping(t *testing.T) {
	// Create tuple desc with boolean field
	td, _ := tuple.NewTupleDesc(
		[]types.Type{types.StringType, types.BoolType},
		[]string{"group", "value"},
	)

	// Create test data
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

	var tuples []*tuple.Tuple
	for _, data := range testData {
		tup := tuple.NewTuple(td)
		tup.SetField(0, types.NewStringField(data.group, len(data.group)))
		tup.SetField(1, types.NewBoolField(data.value))
		tuples = append(tuples, tup)
	}

	source := newMockIterator(tuples, td)

	op, err := NewAggregateOperator(source, 1, 0, And)
	if err != nil {
		t.Fatalf("Failed to create operator: %v", err)
	}

	err = op.Open()
	if err != nil {
		t.Fatalf("Failed to open operator: %v", err)
	}
	defer op.Close()

	// Expected results: A=false, B=false, C=true
	expectedResults := map[string]bool{
		"A": false, // true AND true AND false = false
		"B": false, // false AND true = false
		"C": true,  // true
	}

	results := make(map[string]bool)
	for {
		hasNext, err := op.HasNext()
		if err != nil {
			t.Fatalf("Error checking HasNext: %v", err)
		}
		if !hasNext {
			break
		}

		result, err := op.Next()
		if err != nil {
			t.Fatalf("Error getting next result: %v", err)
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

func TestAggregateOperator_BooleanAggregation_NoGrouping(t *testing.T) {
	// Create tuple desc with boolean field
	td, _ := tuple.NewTupleDesc(
		[]types.Type{types.BoolType},
		[]string{"value"},
	)

	tests := []struct {
		op       AggregateOp
		values   []bool
		expected interface{}
	}{
		{And, []bool{true, true, true}, true},
		{And, []bool{true, false, true}, false},
		{Or, []bool{false, false, false}, false},
		{Or, []bool{false, true, false}, true},
		{Sum, []bool{true, false, true, true}, int64(3)},          // count of true values
		{Count, []bool{true, false, true, false, true}, int64(5)}, // total count
	}

	for _, test := range tests {
		t.Run(test.op.String()+"_Boolean", func(t *testing.T) {
			// Create test tuples
			var tuples []*tuple.Tuple
			for _, value := range test.values {
				tup := tuple.NewTuple(td)
				tup.SetField(0, types.NewBoolField(value))
				tuples = append(tuples, tup)
			}

			source := newMockIterator(tuples, td)
			op, err := NewAggregateOperator(source, 0, NoGrouping, test.op)
			if err != nil {
				t.Fatalf("Failed to create operator: %v", err)
			}

			err = op.Open()
			if err != nil {
				t.Fatalf("Failed to open operator: %v", err)
			}
			defer op.Close()

			hasNext, err := op.HasNext()
			if err != nil {
				t.Fatalf("Error checking HasNext: %v", err)
			}
			if !hasNext {
				t.Fatal("Expected at least one result")
			}

			result, err := op.Next()
			if err != nil {
				t.Fatalf("Failed to get result: %v", err)
			}

			field, err := result.GetField(0)
			if err != nil {
				t.Fatalf("Failed to get result field: %v", err)
			}

			switch expectedVal := test.expected.(type) {
			case bool:
				boolField, ok := field.(*types.BoolField)
				if !ok {
					t.Fatal("Result field is not a boolean")
				}
				if boolField.Value != expectedVal {
					t.Errorf("Expected %v, got %v", expectedVal, boolField.Value)
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

			// Should have no more results
			hasNext, err = op.HasNext()
			if err != nil {
				t.Fatalf("Error checking HasNext: %v", err)
			}
			if hasNext {
				t.Error("Expected no more results")
			}
		})
	}
}

func TestAggregateOperator_StringAggregation_WithGrouping(t *testing.T) {
	// Create tuple desc with string field
	td, _ := tuple.NewTupleDesc(
		[]types.Type{types.StringType, types.StringType},
		[]string{"group", "value"},
	)

	// Create test data
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
	}

	var tuples []*tuple.Tuple
	for _, data := range testData {
		tup := tuple.NewTuple(td)
		tup.SetField(0, types.NewStringField(data.group, len(data.group)))
		tup.SetField(1, types.NewStringField(data.value, len(data.value)))
		tuples = append(tuples, tup)
	}

	source := newMockIterator(tuples, td)

	op, err := NewAggregateOperator(source, 1, 0, Min)
	if err != nil {
		t.Fatalf("Failed to create operator: %v", err)
	}

	err = op.Open()
	if err != nil {
		t.Fatalf("Failed to open operator: %v", err)
	}
	defer op.Close()

	// Expected results
	expectedResults := map[string]string{
		"fruits":  "apple", // min("apple", "banana", "cherry") = "apple"
		"colors":  "blue",  // min("red", "blue") = "blue"
		"animals": "zebra", // min("zebra") = "zebra"
	}

	results := make(map[string]string)
	for {
		hasNext, err := op.HasNext()
		if err != nil {
			t.Fatalf("Error checking HasNext: %v", err)
		}
		if !hasNext {
			break
		}

		result, err := op.Next()
		if err != nil {
			t.Fatalf("Error getting next result: %v", err)
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

func TestAggregateOperator_StringAggregation_NoGrouping(t *testing.T) {
	// Create tuple desc with string field
	td, _ := tuple.NewTupleDesc(
		[]types.Type{types.StringType},
		[]string{"value"},
	)

	tests := []struct {
		op       AggregateOp
		values   []string
		expected interface{}
	}{
		{Count, []string{"apple", "banana", "cherry"}, int64(3)},
		{Min, []string{"zebra", "apple", "banana"}, "apple"},
		{Max, []string{"zebra", "apple", "banana"}, "zebra"},
	}

	for _, test := range tests {
		t.Run(test.op.String()+"_String", func(t *testing.T) {
			// Create test tuples
			var tuples []*tuple.Tuple
			for _, value := range test.values {
				tup := tuple.NewTuple(td)
				tup.SetField(0, types.NewStringField(value, len(value)))
				tuples = append(tuples, tup)
			}

			source := newMockIterator(tuples, td)
			op, err := NewAggregateOperator(source, 0, NoGrouping, test.op)
			if err != nil {
				t.Fatalf("Failed to create operator: %v", err)
			}

			err = op.Open()
			if err != nil {
				t.Fatalf("Failed to open operator: %v", err)
			}
			defer op.Close()

			hasNext, err := op.HasNext()
			if err != nil {
				t.Fatalf("Error checking HasNext: %v", err)
			}
			if !hasNext {
				t.Fatal("Expected at least one result")
			}

			result, err := op.Next()
			if err != nil {
				t.Fatalf("Failed to get result: %v", err)
			}

			field, err := result.GetField(0)
			if err != nil {
				t.Fatalf("Failed to get result field: %v", err)
			}

			switch expectedVal := test.expected.(type) {
			case string:
				stringField, ok := field.(*types.StringField)
				if !ok {
					t.Fatal("Result field is not a string")
				}
				if stringField.Value != expectedVal {
					t.Errorf("Expected %v, got %v", expectedVal, stringField.Value)
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

			// Should have no more results
			hasNext, err = op.HasNext()
			if err != nil {
				t.Fatalf("Error checking HasNext: %v", err)
			}
			if hasNext {
				t.Error("Expected no more results")
			}
		})
	}
}

func TestAggregateOperator_FloatAggregation_WithGrouping(t *testing.T) {
	// Create tuple desc with float field
	td, _ := tuple.NewTupleDesc(
		[]types.Type{types.StringType, types.FloatType},
		[]string{"group", "value"},
	)

	// Create test data
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

	var tuples []*tuple.Tuple
	for _, data := range testData {
		tup := tuple.NewTuple(td)
		tup.SetField(0, types.NewStringField(data.group, len(data.group)))
		tup.SetField(1, types.NewFloat64Field(data.value))
		tuples = append(tuples, tup)
	}

	source := newMockIterator(tuples, td)

	op, err := NewAggregateOperator(source, 1, 0, Min)
	if err != nil {
		t.Fatalf("Failed to create operator: %v", err)
	}

	err = op.Open()
	if err != nil {
		t.Fatalf("Failed to open operator: %v", err)
	}
	defer op.Close()

	// Expected results
	expectedResults := map[string]float64{
		"numbers": 2.3,  // min(5.5, 2.3, 7.8) = 2.3
		"scores":  85.0, // min(85.0, 92.5) = 85.0
		"temps":   98.6, // min(98.6, 101.2) = 98.6
	}

	results := make(map[string]float64)
	for {
		hasNext, err := op.HasNext()
		if err != nil {
			t.Fatalf("Error checking HasNext: %v", err)
		}
		if !hasNext {
			break
		}

		result, err := op.Next()
		if err != nil {
			t.Fatalf("Error getting next result: %v", err)
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
		} else if actualResult != expectedResult {
			t.Errorf("Group %s: expected %v, got %v", group, expectedResult, actualResult)
		}
	}
}

func TestAggregateOperator_FloatAggregation_NoGrouping(t *testing.T) {
	// Create tuple desc with float field
	td, _ := tuple.NewTupleDesc(
		[]types.Type{types.FloatType},
		[]string{"value"},
	)

	tests := []struct {
		op       AggregateOp
		values   []float64
		expected interface{}
	}{
		{Count, []float64{1.5, 2.5, 3.5}, int64(3)},
		{Min, []float64{5.5, 1.2, 3.8}, 1.2},
		{Max, []float64{5.5, 1.2, 3.8}, 5.5},
		{Sum, []float64{1.5, 2.5, 3.0}, 7.0},
		{Avg, []float64{2.0, 4.0, 6.0}, 4.0},
	}

	for _, test := range tests {
		t.Run(test.op.String()+"_Float", func(t *testing.T) {
			// Create test tuples
			var tuples []*tuple.Tuple
			for _, value := range test.values {
				tup := tuple.NewTuple(td)
				tup.SetField(0, types.NewFloat64Field(value))
				tuples = append(tuples, tup)
			}

			source := newMockIterator(tuples, td)
			op, err := NewAggregateOperator(source, 0, NoGrouping, test.op)
			if err != nil {
				t.Fatalf("Failed to create operator: %v", err)
			}

			err = op.Open()
			if err != nil {
				t.Fatalf("Failed to open operator: %v", err)
			}
			defer op.Close()

			hasNext, err := op.HasNext()
			if err != nil {
				t.Fatalf("Error checking HasNext: %v", err)
			}
			if !hasNext {
				t.Fatal("Expected at least one result")
			}

			result, err := op.Next()
			if err != nil {
				t.Fatalf("Failed to get result: %v", err)
			}

			field, err := result.GetField(0)
			if err != nil {
				t.Fatalf("Failed to get result field: %v", err)
			}

			switch expectedVal := test.expected.(type) {
			case float64:
				floatField, ok := field.(*types.Float64Field)
				if !ok {
					t.Fatal("Result field is not a float")
				}
				if floatField.Value != expectedVal {
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

			// Should have no more results
			hasNext, err = op.HasNext()
			if err != nil {
				t.Fatalf("Error checking HasNext: %v", err)
			}
			if hasNext {
				t.Error("Expected no more results")
			}
		})
	}
}

func TestAggregateOperator_FloatUnsupportedOperation(t *testing.T) {
	td, _ := tuple.NewTupleDesc(
		[]types.Type{types.FloatType},
		[]string{"value"},
	)

	// Create test tuple
	tup := tuple.NewTuple(td)
	tup.SetField(0, types.NewFloat64Field(1.0))
	tuples := []*tuple.Tuple{tup}

	source := newMockIterator(tuples, td)

	// Try to create operator with unsupported operation for float (And)
	_, err := NewAggregateOperator(source, 0, NoGrouping, And)
	if err == nil {
		t.Error("Expected error for unsupported float operation And, got none")
	}
}

func TestAggregateOperator_Lifecycle(t *testing.T) {
	td := createTestTupleDesc()
	tuples := createTestTuples()
	source := newMockIterator(tuples, td)

	op, err := NewAggregateOperator(source, 1, 0, Sum)
	if err != nil {
		t.Fatalf("Failed to create operator: %v", err)
	}

	t.Run("operations on closed operator", func(t *testing.T) {
		_, err := op.HasNext()
		if err == nil {
			t.Error("Expected error when calling HasNext on closed operator")
		}

		_, err = op.Next()
		if err == nil {
			t.Error("Expected error when calling Next on closed operator")
		}

		err = op.Rewind()
		if err == nil {
			t.Error("Expected error when calling Rewind on closed operator")
		}
	})

	t.Run("double open", func(t *testing.T) {
		err := op.Open()
		if err != nil {
			t.Fatalf("Failed to open operator: %v", err)
		}

		err = op.Open()
		if err == nil {
			t.Error("Expected error when opening already opened operator")
		}

		op.Close()
	})

	t.Run("normal operation", func(t *testing.T) {
		// Re-create operator since previous tests may have altered state
		source = newMockIterator(tuples, td)
		op, err = NewAggregateOperator(source, 1, 0, Sum)
		if err != nil {
			t.Fatalf("Failed to create operator: %v", err)
		}

		err = op.Open()
		if err != nil {
			t.Fatalf("Failed to open operator: %v", err)
		}
		defer op.Close()

		// Should have results
		hasNext, err := op.HasNext()
		if err != nil {
			t.Fatalf("Error checking HasNext: %v", err)
		}
		if !hasNext {
			t.Error("Expected results from aggregation")
		}

		// Read all results
		var results []*tuple.Tuple
		for {
			hasNext, err := op.HasNext()
			if err != nil {
				t.Fatalf("Error checking HasNext: %v", err)
			}
			if !hasNext {
				break
			}

			result, err := op.Next()
			if err != nil {
				t.Fatalf("Error getting next result: %v", err)
			}
			results = append(results, result)
		}

		if len(results) == 0 {
			t.Error("Expected at least one result")
		}
	})
}

func TestAggregateOperator_Aggregation_WithGrouping(t *testing.T) {
	td := createTestTupleDesc()
	tuples := createTestTuples()
	source := newMockIterator(tuples, td)

	op, err := NewAggregateOperator(source, 1, 0, Sum)
	if err != nil {
		t.Fatalf("Failed to create operator: %v", err)
	}

	err = op.Open()
	if err != nil {
		t.Fatalf("Failed to open operator: %v", err)
	}
	defer op.Close()

	// Expected sums: A=30 (10+15+5), B=45 (20+25), C=30
	expectedSums := map[string]int64{
		"A": 30,
		"B": 45,
		"C": 30,
	}

	results := make(map[string]int64)
	for {
		hasNext, err := op.HasNext()
		if err != nil {
			t.Fatalf("Error checking HasNext: %v", err)
		}
		if !hasNext {
			break
		}

		result, err := op.Next()
		if err != nil {
			t.Fatalf("Error getting next result: %v", err)
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

func TestAggregateOperator_Aggregation_NoGrouping(t *testing.T) {
	td := createTestTupleDesc()
	tuples := createTestTuples()

	tests := []struct {
		op       AggregateOp
		expected int64
	}{
		{Min, 5},   // minimum value
		{Max, 30},  // maximum value
		{Sum, 105}, // 10+20+15+30+25+5
		{Avg, 17},  // 105/6 = 17 (integer division)
		{Count, 6}, // 6 tuples
	}

	for _, test := range tests {
		t.Run(test.op.String(), func(t *testing.T) {
			source := newMockIterator(tuples, td)
			op, err := NewAggregateOperator(source, 1, NoGrouping, test.op)
			if err != nil {
				t.Fatalf("Failed to create operator: %v", err)
			}

			err = op.Open()
			if err != nil {
				t.Fatalf("Failed to open operator: %v", err)
			}
			defer op.Close()

			hasNext, err := op.HasNext()
			if err != nil {
				t.Fatalf("Error checking HasNext: %v", err)
			}
			if !hasNext {
				t.Fatal("Expected at least one result")
			}

			result, err := op.Next()
			if err != nil {
				t.Fatalf("Failed to get result: %v", err)
			}

			field, err := result.GetField(0)
			if err != nil {
				t.Fatalf("Failed to get result field: %v", err)
			}

			intField, ok := field.(*types.IntField)
			if !ok {
				t.Fatal("Result field is not an integer")
			}

			if intField.Value != test.expected {
				t.Errorf("Expected %d, got %d", test.expected, intField.Value)
			}

			// Should have no more results
			hasNext, err = op.HasNext()
			if err != nil {
				t.Fatalf("Error checking HasNext: %v", err)
			}
			if hasNext {
				t.Error("Expected no more results")
			}
		})
	}
}

func TestAggregateOperator_EmptySource(t *testing.T) {
	td := createTestTupleDesc()
	source := newMockIterator([]*tuple.Tuple{}, td)

	op, err := NewAggregateOperator(source, 1, 0, Sum)
	if err != nil {
		t.Fatalf("Failed to create operator: %v", err)
	}

	err = op.Open()
	if err != nil {
		t.Fatalf("Failed to open operator: %v", err)
	}
	defer op.Close()

	hasNext, err := op.HasNext()
	if err != nil {
		t.Fatalf("Error checking HasNext: %v", err)
	}
	if hasNext {
		t.Error("Expected no results from empty source")
	}
}

func TestAggregateOperator_SourceErrors(t *testing.T) {
	td := createTestTupleDesc()
	tuples := createTestTuples()

	t.Run("source open error", func(t *testing.T) {
		source := newMockIterator(tuples, td)
		source.setError(true)

		op, err := NewAggregateOperator(source, 1, 0, Sum)
		if err != nil {
			t.Fatalf("Failed to create operator: %v", err)
		}

		err = op.Open()
		if err == nil {
			t.Error("Expected error when source open fails")
		}
	})

	t.Run("source hasNext error during aggregation", func(t *testing.T) {
		source := newMockIterator(tuples, td)
		op, err := NewAggregateOperator(source, 1, 0, Sum)
		if err != nil {
			t.Fatalf("Failed to create operator: %v", err)
		}

		// Open source first, then set error
		source.Open()
		source.setError(true)

		err = op.Open()
		if err == nil {
			t.Error("Expected error when source hasNext fails during aggregation")
		}
	})
}

func TestAggregateOperator_Rewind(t *testing.T) {
	td := createTestTupleDesc()
	tuples := createTestTuples()
	source := newMockIterator(tuples, td)

	op, err := NewAggregateOperator(source, 1, 0, Sum)
	if err != nil {
		t.Fatalf("Failed to create operator: %v", err)
	}

	err = op.Open()
	if err != nil {
		t.Fatalf("Failed to open operator: %v", err)
	}
	defer op.Close()

	// Read first result
	hasNext, err := op.HasNext()
	if err != nil || !hasNext {
		t.Fatal("Expected result to be available")
	}

	_, err = op.Next()
	if err != nil {
		t.Fatalf("Failed to get first result: %v", err)
	}

	// Rewind
	err = op.Rewind()
	if err != nil {
		t.Fatalf("Failed to rewind: %v", err)
	}

	// Should be able to read results again
	hasNext, err = op.HasNext()
	if err != nil || !hasNext {
		t.Fatal("Expected result to be available after rewind")
	}
}

func TestAggregateOperator_TupleDesc(t *testing.T) {
	td := createTestTupleDesc()
	tuples := createTestTuples()

	t.Run("with grouping", func(t *testing.T) {
		source := newMockIterator(tuples, td)
		op, err := NewAggregateOperator(source, 1, 0, Sum)
		if err != nil {
			t.Fatalf("Failed to create operator: %v", err)
		}

		resultTd := op.GetTupleDesc()
		if resultTd.NumFields() != 2 {
			t.Errorf("Expected 2 fields, got %d", resultTd.NumFields())
		}

		groupType, err := resultTd.TypeAtIndex(0)
		if err != nil {
			t.Fatalf("Failed to get group field type: %v", err)
		}
		if groupType != types.StringType {
			t.Errorf("Expected StringType for group field, got %v", groupType)
		}

		aggType, err := resultTd.TypeAtIndex(1)
		if err != nil {
			t.Fatalf("Failed to get aggregate field type: %v", err)
		}
		if aggType != types.IntType {
			t.Errorf("Expected IntType for aggregate field, got %v", aggType)
		}
	})

	t.Run("without grouping", func(t *testing.T) {
		source := newMockIterator(tuples, td)
		op, err := NewAggregateOperator(source, 1, NoGrouping, Sum)
		if err != nil {
			t.Fatalf("Failed to create operator: %v", err)
		}

		resultTd := op.GetTupleDesc()
		if resultTd.NumFields() != 1 {
			t.Errorf("Expected 1 field, got %d", resultTd.NumFields())
		}

		aggType, err := resultTd.TypeAtIndex(0)
		if err != nil {
			t.Fatalf("Failed to get aggregate field type: %v", err)
		}
		if aggType != types.IntType {
			t.Errorf("Expected IntType for aggregate field, got %v", aggType)
		}
	})
}

func TestAggregateOperator_NilSourceTupleDesc(t *testing.T) {
	// Create a mock iterator with nil tuple description
	source := &mockIterator{
		tuples: []*tuple.Tuple{},
		td:     nil,
	}

	_, err := NewAggregateOperator(source, 1, 0, Sum)
	if err == nil {
		t.Error("Expected error for nil source tuple description")
	}
	if err.Error() != "source tuple description cannot be nil" {
		t.Errorf("Expected nil tuple description error, got: %v", err)
	}
}
