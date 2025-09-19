package join

import (
	"storemy/pkg/execution"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

// ============================================================================
// JOIN PREDICATE TESTS
// ============================================================================

// TestNewJoinPredicate tests the construction of join predicates
func TestNewJoinPredicate(t *testing.T) {
	tests := []struct {
		name      string
		field1    int
		field2    int
		op        execution.PredicateOp
		expectErr bool
	}{
		{
			name:      "valid predicate",
			field1:    0,
			field2:    1,
			op:        execution.Equals,
			expectErr: false,
		},
		{
			name:      "negative field1",
			field1:    -1,
			field2:    0,
			op:        execution.Equals,
			expectErr: true,
		},
		{
			name:      "negative field2",
			field1:    0,
			field2:    -1,
			op:        execution.Equals,
			expectErr: true,
		},
		{
			name:      "both fields negative",
			field1:    -1,
			field2:    -2,
			op:        execution.Equals,
			expectErr: true,
		},
		{
			name:      "valid predicate with GreaterThan",
			field1:    2,
			field2:    3,
			op:        execution.GreaterThan,
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jp, err := NewJoinPredicate(tt.field1, tt.field2, tt.op)

			if tt.expectErr {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				if jp != nil {
					t.Errorf("expected nil predicate on error")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if jp == nil {
					t.Errorf("expected non-nil predicate")
				}
				if jp.field1 != tt.field1 {
					t.Errorf("expected field1=%d, got %d", tt.field1, jp.field1)
				}
				if jp.field2 != tt.field2 {
					t.Errorf("expected field2=%d, got %d", tt.field2, jp.field2)
				}
				if jp.op != tt.op {
					t.Errorf("expected op=%v, got %v", tt.op, jp.op)
				}
			}
		})
	}
}

// TestJoinPredicateGetters tests the getter methods
func TestJoinPredicateGetters(t *testing.T) {
	field1, field2 := 1, 2
	op := execution.LessThan

	jp, err := NewJoinPredicate(field1, field2, op)
	if err != nil {
		t.Fatalf("failed to create join predicate: %v", err)
	}

	if jp.GetField1() != field1 {
		t.Errorf("GetField1() = %d, want %d", jp.GetField1(), field1)
	}

	if jp.GetField2() != field2 {
		t.Errorf("GetField2() = %d, want %d", jp.GetField2(), field2)
	}

	if jp.GetOP() != op {
		t.Errorf("GetOP() = %v, want %v", jp.GetOP(), op)
	}
}

// TestJoinPredicateString tests the string representation
func TestJoinPredicateString(t *testing.T) {
	jp, err := NewJoinPredicate(1, 2, execution.Equals)
	if err != nil {
		t.Fatalf("failed to create join predicate: %v", err)
	}

	expected := "JoinPredicate(field1=1 = field2=2)"
	if jp.String() != expected {
		t.Errorf("String() = %q, want %q", jp.String(), expected)
	}
}

// Helper function to create a test tuple
func createTestTuple(fieldTypes []types.Type, values []any) *tuple.Tuple {
	td, _ := tuple.NewTupleDesc(fieldTypes, nil)
	tup := tuple.NewTuple(td)

	for i, val := range values {
		var field types.Field
		switch v := val.(type) {
		case int32:
			field = types.NewIntField(v)
		case string:
			field = types.NewStringField(v, types.StringMaxSize)
		}
		tup.SetField(i, field)
	}

	return tup
}

// TestJoinPredicateFilter tests the filter functionality
func TestJoinPredicateFilter(t *testing.T) {
	tests := []struct {
		name       string
		field1     int
		field2     int
		op         execution.PredicateOp
		tuple1Data []interface{}
		tuple2Data []interface{}
		expected   bool
		expectErr  bool
	}{
		{
			name:       "integers equal",
			field1:     0,
			field2:     0,
			op:         execution.Equals,
			tuple1Data: []any{int32(10)},
			tuple2Data: []any{int32(10)},
			expected:   true,
			expectErr:  false,
		},
		{
			name:       "integers not equal",
			field1:     0,
			field2:     0,
			op:         execution.Equals,
			tuple1Data: []interface{}{int32(10)},
			tuple2Data: []interface{}{int32(20)},
			expected:   false,
			expectErr:  false,
		},
		{
			name:       "integer less than",
			field1:     0,
			field2:     0,
			op:         execution.LessThan,
			tuple1Data: []interface{}{int32(5)},
			tuple2Data: []interface{}{int32(10)},
			expected:   true,
			expectErr:  false,
		},
		{
			name:       "integer greater than",
			field1:     0,
			field2:     0,
			op:         execution.GreaterThan,
			tuple1Data: []interface{}{int32(15)},
			tuple2Data: []interface{}{int32(10)},
			expected:   true,
			expectErr:  false,
		},
		{
			name:       "string comparison equal",
			field1:     0,
			field2:     0,
			op:         execution.Equals,
			tuple1Data: []interface{}{"hello"},
			tuple2Data: []interface{}{"hello"},
			expected:   true,
			expectErr:  false,
		},
		{
			name:       "string comparison not equal",
			field1:     0,
			field2:     0,
			op:         execution.NotEqual,
			tuple1Data: []interface{}{"hello"},
			tuple2Data: []interface{}{"world"},
			expected:   true,
			expectErr:  false,
		},
		{
			name:       "different field indices",
			field1:     0,
			field2:     1,
			op:         execution.Equals,
			tuple1Data: []interface{}{int32(10), int32(20)},
			tuple2Data: []interface{}{int32(30), int32(10)},
			expected:   true,
			expectErr:  false,
		},
		{
			name:       "field index out of bounds - tuple1",
			field1:     5,
			field2:     0,
			op:         execution.Equals,
			tuple1Data: []interface{}{int32(10)},
			tuple2Data: []interface{}{int32(10)},
			expected:   false,
			expectErr:  true,
		},
		{
			name:       "field index out of bounds - tuple2",
			field1:     0,
			field2:     5,
			op:         execution.Equals,
			tuple1Data: []interface{}{int32(10)},
			tuple2Data: []interface{}{int32(10)},
			expected:   false,
			expectErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jp, err := NewJoinPredicate(tt.field1, tt.field2, tt.op)
			if err != nil {
				t.Fatalf("failed to create join predicate: %v", err)
			}

			// Create tuples based on data types
			var fieldTypes1, fieldTypes2 []types.Type
			for _, val := range tt.tuple1Data {
				switch val.(type) {
				case int32:
					fieldTypes1 = append(fieldTypes1, types.IntType)
				case string:
					fieldTypes1 = append(fieldTypes1, types.StringType)
				}
			}
			for _, val := range tt.tuple2Data {
				switch val.(type) {
				case int32:
					fieldTypes2 = append(fieldTypes2, types.IntType)
				case string:
					fieldTypes2 = append(fieldTypes2, types.StringType)
				}
			}

			tuple1 := createTestTuple(fieldTypes1, tt.tuple1Data)
			tuple2 := createTestTuple(fieldTypes2, tt.tuple2Data)

			result, err := jp.Filter(tuple1, tuple2)

			if tt.expectErr {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if result != tt.expected {
					t.Errorf("Filter() = %v, want %v", result, tt.expected)
				}
			}
		})
	}
}

// TestJoinPredicateFilterNilTuples tests error handling for nil tuples
func TestJoinPredicateFilterNilTuples(t *testing.T) {
	jp, err := NewJoinPredicate(0, 0, execution.Equals)
	if err != nil {
		t.Fatalf("failed to create join predicate: %v", err)
	}

	tuple1 := createTestTuple([]types.Type{types.IntType}, []any{int32(10)})

	tests := []struct {
		name   string
		tuple1 *tuple.Tuple
		tuple2 *tuple.Tuple
	}{
		{"both nil", nil, nil},
		{"first nil", nil, tuple1},
		{"second nil", tuple1, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jp.Filter(tt.tuple1, tt.tuple2)
			if err == nil {
				t.Errorf("expected error for nil tuples")
			}
			if result {
				t.Errorf("expected false result for nil tuples")
			}
		})
	}
}

// TestJoinPredicateFilterNullFields tests handling of null fields
func TestJoinPredicateFilterNullFields(t *testing.T) {
	jp, err := NewJoinPredicate(0, 0, execution.Equals)
	if err != nil {
		t.Fatalf("failed to create join predicate: %v", err)
	}

	// Create tuples with null fields (empty tuples)
	td, _ := tuple.NewTupleDesc([]types.Type{types.IntType}, nil)
	tuple1 := tuple.NewTuple(td)
	tuple2 := tuple.NewTuple(td)

	result, err := jp.Filter(tuple1, tuple2)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if result {
		t.Errorf("expected false for null fields comparison")
	}
}

// TestJoinPredicateAllOperations tests all predicate operations
func TestJoinPredicateAllOperations(t *testing.T) {
	operations := []struct {
		op       execution.PredicateOp
		val1     int32
		val2     int32
		expected bool
	}{
		{execution.Equals, 10, 10, true},
		{execution.Equals, 10, 20, false},
		{execution.LessThan, 5, 10, true},
		{execution.LessThan, 15, 10, false},
		{execution.GreaterThan, 15, 10, true},
		{execution.GreaterThan, 5, 10, false},
		{execution.LessThanOrEqual, 10, 10, true},
		{execution.LessThanOrEqual, 5, 10, true},
		{execution.LessThanOrEqual, 15, 10, false},
		{execution.GreaterThanOrEqual, 10, 10, true},
		{execution.GreaterThanOrEqual, 15, 10, true},
		{execution.GreaterThanOrEqual, 5, 10, false},
		{execution.NotEqual, 10, 20, true},
		{execution.NotEqual, 10, 10, false},
	}

	for _, test := range operations {
		t.Run(test.op.String(), func(t *testing.T) {
			jp, err := NewJoinPredicate(0, 0, test.op)
			if err != nil {
				t.Fatalf("failed to create join predicate: %v", err)
			}

			tuple1 := createTestTuple([]types.Type{types.IntType}, []interface{}{test.val1})
			tuple2 := createTestTuple([]types.Type{types.IntType}, []interface{}{test.val2})

			result, err := jp.Filter(tuple1, tuple2)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if result != test.expected {
				t.Errorf("Filter(%d %s %d) = %v, want %v",
					test.val1, test.op.String(), test.val2, result, test.expected)
			}
		})
	}
}
