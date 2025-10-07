package tuple

import (
	"storemy/pkg/primitives"
	"storemy/pkg/types"
	"testing"
)

func TestNewTuple(t *testing.T) {
	td := mustCreateTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"id", "name"})

	tuple := NewTuple(td)

	if tuple == nil {
		t.Fatal("NewTuple returned nil")
	}

	if tuple.TupleDesc != td {
		t.Errorf("Expected TupleDesc to be %v, got %v", td, tuple.TupleDesc)
	}

	if len(tuple.fields) != 2 {
		t.Errorf("Expected 2 fields, got %d", len(tuple.fields))
	}

	for i, field := range tuple.fields {
		if field != nil {
			t.Errorf("Expected field %d to be nil, got %v", i, field)
		}
	}

	if tuple.RecordID != nil {
		t.Errorf("Expected RecordID to be nil, got %v", tuple.RecordID)
	}
}

func TestTuple_SetField(t *testing.T) {
	td := mustCreateTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"id", "name"})
	tuple := NewTuple(td)

	intField := types.NewIntField(42)
	stringField := types.NewStringField("test", 128)

	tests := []struct {
		name          string
		index         int
		field         types.Field
		expectedError bool
	}{
		{
			name:          "Valid int field at index 0",
			index:         0,
			field:         intField,
			expectedError: false,
		},
		{
			name:          "Valid string field at index 1",
			index:         1,
			field:         stringField,
			expectedError: false,
		},
		{
			name:          "Invalid negative index",
			index:         -1,
			field:         intField,
			expectedError: true,
		},
		{
			name:          "Invalid index out of bounds",
			index:         2,
			field:         intField,
			expectedError: true,
		},
		{
			name:          "Type mismatch - string field at int index",
			index:         0,
			field:         stringField,
			expectedError: true,
		},
		{
			name:          "Type mismatch - int field at string index",
			index:         1,
			field:         intField,
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tuple.SetField(tt.index, tt.field)

			if tt.expectedError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			retrievedField, _ := tuple.GetField(tt.index)
			if retrievedField != tt.field {
				t.Errorf("Expected field %v at index %d, got %v", tt.field, tt.index, retrievedField)
			}
		})
	}
}

func TestTuple_GetField(t *testing.T) {
	td := mustCreateTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"id", "name"})
	tuple := NewTuple(td)

	intField := types.NewIntField(42)
	stringField := types.NewStringField("test", 128)

	tuple.SetField(0, intField)
	tuple.SetField(1, stringField)

	tests := []struct {
		name          string
		index         int
		expectedField types.Field
		expectedError bool
	}{
		{
			name:          "Valid index 0",
			index:         0,
			expectedField: intField,
			expectedError: false,
		},
		{
			name:          "Valid index 1",
			index:         1,
			expectedField: stringField,
			expectedError: false,
		},
		{
			name:          "Invalid negative index",
			index:         -1,
			expectedError: true,
		},
		{
			name:          "Invalid index out of bounds",
			index:         2,
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field, err := tuple.GetField(tt.index)

			if tt.expectedError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if field != tt.expectedField {
				t.Errorf("Expected field %v, got %v", tt.expectedField, field)
			}
		})
	}
}

func TestTuple_GetFieldUninitialized(t *testing.T) {
	td := mustCreateTupleDesc([]types.Type{types.IntType}, []string{"id"})
	tuple := NewTuple(td)

	field, err := tuple.GetField(0)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if field != nil {
		t.Errorf("Expected nil field, got %v", field)
	}
}

func TestTuple_String(t *testing.T) {
	tests := []struct {
		name           string
		setupFunc      func() *Tuple
		expectedString string
	}{
		{
			name: "Tuple with int and string fields",
			setupFunc: func() *Tuple {
				td := mustCreateTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"id", "name"})
				tuple := NewTuple(td)
				tuple.SetField(0, types.NewIntField(42))
				tuple.SetField(1, types.NewStringField("test", 128))
				return tuple
			},
			expectedString: "42\ttest\n",
		},
		{
			name: "Tuple with nil fields",
			setupFunc: func() *Tuple {
				td := mustCreateTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"id", "name"})
				return NewTuple(td)
			},
			expectedString: "null\tnull\n",
		},
		{
			name: "Tuple with mixed nil and initialized fields",
			setupFunc: func() *Tuple {
				td := mustCreateTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"id", "name"})
				tuple := NewTuple(td)
				tuple.SetField(0, types.NewIntField(123))
				return tuple
			},
			expectedString: "123\tnull\n",
		},
		{
			name: "Single field tuple",
			setupFunc: func() *Tuple {
				td := mustCreateTupleDesc([]types.Type{types.IntType}, []string{"id"})
				tuple := NewTuple(td)
				tuple.SetField(0, types.NewIntField(999))
				return tuple
			},
			expectedString: "999\n",
		},
		{
			name: "Empty string field",
			setupFunc: func() *Tuple {
				td := mustCreateTupleDesc([]types.Type{types.StringType}, []string{"name"})
				tuple := NewTuple(td)
				tuple.SetField(0, types.NewStringField("", 128))
				return tuple
			},
			expectedString: "\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tuple := tt.setupFunc()
			result := tuple.String()

			if result != tt.expectedString {
				t.Errorf("Expected string %q, got %q", tt.expectedString, result)
			}
		})
	}
}

func TestCombineTuples(t *testing.T) {
	tests := []struct {
		name           string
		setupTuple1    func() *Tuple
		setupTuple2    func() *Tuple
		expectedError  bool
		expectedFields int
		validateResult func(*testing.T, *Tuple)
	}{
		{
			name: "Combine two valid tuples",
			setupTuple1: func() *Tuple {
				td := mustCreateTupleDesc([]types.Type{types.IntType}, []string{"id"})
				tuple := NewTuple(td)
				tuple.SetField(0, types.NewIntField(1))
				return tuple
			},
			setupTuple2: func() *Tuple {
				td := mustCreateTupleDesc([]types.Type{types.StringType}, []string{"name"})
				tuple := NewTuple(td)
				tuple.SetField(0, types.NewStringField("Alice", 128))
				return tuple
			},
			expectedError:  false,
			expectedFields: 2,
			validateResult: func(t *testing.T, result *Tuple) {
				field0, _ := result.GetField(0)
				field1, _ := result.GetField(1)

				intField, ok := field0.(*types.IntField)
				if !ok || intField.Value != 1 {
					t.Errorf("Expected first field to be IntField with value 1, got %v", field0)
				}

				stringField, ok := field1.(*types.StringField)
				if !ok || stringField.Value != "Alice" {
					t.Errorf("Expected second field to be StringField with value 'Alice', got %v", field1)
				}
			},
		},
		{
			name: "Combine tuples with multiple fields each",
			setupTuple1: func() *Tuple {
				td := mustCreateTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"id", "first_name"})
				tuple := NewTuple(td)
				tuple.SetField(0, types.NewIntField(2))
				tuple.SetField(1, types.NewStringField("Bob", 128))
				return tuple
			},
			setupTuple2: func() *Tuple {
				td := mustCreateTupleDesc([]types.Type{types.StringType, types.IntType}, []string{"last_name", "age"})
				tuple := NewTuple(td)
				tuple.SetField(0, types.NewStringField("Smith", 128))
				tuple.SetField(1, types.NewIntField(25))
				return tuple
			},
			expectedError:  false,
			expectedFields: 4,
			validateResult: func(t *testing.T, result *Tuple) {
				expectedValues := []interface{}{int32(2), "Bob", "Smith", int32(25)}

				for i, expected := range expectedValues {
					field, _ := result.GetField(i)

					switch v := expected.(type) {
					case int32:
						intField, ok := field.(*types.IntField)
						if !ok || intField.Value != v {
							t.Errorf("Expected field %d to be IntField with value %d, got %v", i, v, field)
						}
					case string:
						stringField, ok := field.(*types.StringField)
						if !ok || stringField.Value != v {
							t.Errorf("Expected field %d to be StringField with value %s, got %v", i, v, field)
						}
					}
				}
			},
		},
		{
			name: "First tuple is nil",
			setupTuple1: func() *Tuple {
				return nil
			},
			setupTuple2: func() *Tuple {
				td := mustCreateTupleDesc([]types.Type{types.IntType}, []string{"id"})
				return NewTuple(td)
			},
			expectedError: true,
		},
		{
			name: "Second tuple is nil",
			setupTuple1: func() *Tuple {
				td := mustCreateTupleDesc([]types.Type{types.IntType}, []string{"id"})
				return NewTuple(td)
			},
			setupTuple2: func() *Tuple {
				return nil
			},
			expectedError: true,
		},
		{
			name: "Both tuples are nil",
			setupTuple1: func() *Tuple {
				return nil
			},
			setupTuple2: func() *Tuple {
				return nil
			},
			expectedError: true,
		},
		{
			name: "Combine tuples with some nil fields",
			setupTuple1: func() *Tuple {
				td := mustCreateTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"id", "name"})
				tuple := NewTuple(td)
				tuple.SetField(0, types.NewIntField(3))
				return tuple
			},
			setupTuple2: func() *Tuple {
				td := mustCreateTupleDesc([]types.Type{types.StringType}, []string{"email"})
				tuple := NewTuple(td)
				tuple.SetField(0, types.NewStringField("test@example.com", 128))
				return tuple
			},
			expectedError:  false,
			expectedFields: 3,
			validateResult: func(t *testing.T, result *Tuple) {
				field0, _ := result.GetField(0)
				field1, _ := result.GetField(1)
				field2, _ := result.GetField(2)

				intField, ok := field0.(*types.IntField)
				if !ok || intField.Value != 3 {
					t.Errorf("Expected first field to be IntField with value 3, got %v", field0)
				}

				if field1 != nil {
					t.Errorf("Expected second field to be nil, got %v", field1)
				}

				stringField, ok := field2.(*types.StringField)
				if !ok || stringField.Value != "test@example.com" {
					t.Errorf("Expected third field to be StringField with value 'test@example.com', got %v", field2)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tuple1 := tt.setupTuple1()
			tuple2 := tt.setupTuple2()

			result, err := CombineTuples(tuple1, tuple2)

			if tt.expectedError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if result == nil {
				t.Fatal("Expected non-nil result")
			}

			if result.TupleDesc.NumFields() != tt.expectedFields {
				t.Errorf("Expected %d fields, got %d", tt.expectedFields, result.TupleDesc.NumFields())
			}

			if tt.validateResult != nil {
				tt.validateResult(t, result)
			}
		})
	}
}

func TestTuple_RecordID(t *testing.T) {
	td := mustCreateTupleDesc([]types.Type{types.IntType}, []string{"id"})
	tuple := NewTuple(td)

	if tuple.RecordID != nil {
		t.Errorf("Expected RecordID to be nil for new tuple, got %v", tuple.RecordID)
	}

	mockPageID := &mockPageID{tableID: 1, pageNo: 2}
	recordID := NewTupleRecordID(mockPageID, 5)
	tuple.RecordID = recordID

	if tuple.RecordID != recordID {
		t.Errorf("Expected RecordID to be %v, got %v", recordID, tuple.RecordID)
	}
}

type mockPageID struct {
	tableID int
	pageNo  int
}

func (m *mockPageID) GetTableID() int {
	return m.tableID
}

func (m *mockPageID) PageNo() int {
	return m.pageNo
}

func (m *mockPageID) Serialize() []int {
	return []int{m.tableID, m.pageNo}
}

func (m *mockPageID) Equals(other primitives.PageID) bool {
	if other == nil {
		return false
	}
	return m.tableID == other.GetTableID() && m.pageNo == other.PageNo()
}

func (m *mockPageID) String() string {
	return "mockPageID(1,2)"
}

func (m *mockPageID) HashCode() int {
	return m.tableID*31 + m.pageNo
}
