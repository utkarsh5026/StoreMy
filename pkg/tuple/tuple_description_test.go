package tuple

import (
	"storemy/pkg/primitives"
	"storemy/pkg/types"
	"testing"
)

func TestNewTupleDesc(t *testing.T) {
	tests := []struct {
		name           string
		fieldTypes     []types.Type
		fieldNames     []string
		expectedError  bool
		expectedLength primitives.ColumnID
	}{
		{
			name:           "Valid tuple with types and names",
			fieldTypes:     []types.Type{types.IntType, types.StringType},
			fieldNames:     []string{"id", "name"},
			expectedError:  false,
			expectedLength: 2,
		},
		{
			name:           "Valid tuple with types only",
			fieldTypes:     []types.Type{types.IntType, types.StringType},
			fieldNames:     nil,
			expectedError:  false,
			expectedLength: 2,
		},
		{
			name:          "Empty field types",
			fieldTypes:    []types.Type{},
			fieldNames:    []string{},
			expectedError: true,
		},
		{
			name:          "Mismatched types and names length",
			fieldTypes:    []types.Type{types.IntType, types.StringType},
			fieldNames:    []string{"id"},
			expectedError: true,
		},
		{
			name:           "Single field",
			fieldTypes:     []types.Type{types.IntType},
			fieldNames:     []string{"id"},
			expectedError:  false,
			expectedLength: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			td, err := NewTupleDesc(tt.fieldTypes, tt.fieldNames)

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

			if td.NumFields() != tt.expectedLength {
				t.Errorf("Expected %d fields, got %d", tt.expectedLength, td.NumFields())
			}

			if len(td.Types) != len(tt.fieldTypes) {
				t.Errorf("Expected %d types, got %d", len(tt.fieldTypes), len(td.Types))
			}

			for i, expectedType := range tt.fieldTypes {
				if td.Types[i] != expectedType {
					t.Errorf("Expected type %v at index %d, got %v", expectedType, i, td.Types[i])
				}
			}

			if tt.fieldNames != nil {
				if len(td.FieldNames) != len(tt.fieldNames) {
					t.Errorf("Expected %d field names, got %d", len(tt.fieldNames), len(td.FieldNames))
				}
				for i, expectedName := range tt.fieldNames {
					if td.FieldNames[i] != expectedName {
						t.Errorf("Expected field name %s at index %d, got %s", expectedName, i, td.FieldNames[i])
					}
				}
			}
		})
	}
}

func TestTupleDescription_NumFields(t *testing.T) {
	td, _ := NewTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"id", "name"})

	if td.NumFields() != 2 {
		t.Errorf("Expected 2 fields, got %d", td.NumFields())
	}
}

func TestTupleDescription_GetFieldName(t *testing.T) {
	tests := []struct {
		name          string
		td            *TupleDescription
		index         primitives.ColumnID
		expectedName  string
		expectedError bool
	}{
		{
			name:          "Valid index with names",
			td:            mustCreateTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"id", "name"}),
			index:         0,
			expectedName:  "id",
			expectedError: false,
		},
		{
			name:          "Valid index without names",
			td:            mustCreateTupleDesc([]types.Type{types.IntType, types.StringType}, nil),
			index:         0,
			expectedName:  "",
			expectedError: false,
		},
		{
			name:          "Invalid index out of bounds",
			td:            mustCreateTupleDesc([]types.Type{types.IntType}, []string{"id"}),
			index:         1,
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, err := tt.td.GetFieldName(tt.index)

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

			if name != tt.expectedName {
				t.Errorf("Expected field name %s, got %s", tt.expectedName, name)
			}
		})
	}
}

func TestTupleDescription_TypeAtIndex(t *testing.T) {
	td := mustCreateTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"id", "name"})

	tests := []struct {
		name          string
		index         primitives.ColumnID
		expectedType  types.Type
		expectedError bool
	}{
		{
			name:          "Valid index 0",
			index:         0,
			expectedType:  types.IntType,
			expectedError: false,
		},
		{
			name:          "Valid index 1",
			index:         1,
			expectedType:  types.StringType,
			expectedError: false,
		},
		{
			name:          "Invalid index out of bounds",
			index:         2,
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			typ, err := td.TypeAtIndex(tt.index)

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

			if typ != tt.expectedType {
				t.Errorf("Expected type %v, got %v", tt.expectedType, typ)
			}
		})
	}
}

func TestTupleDescription_GetSize(t *testing.T) {
	tests := []struct {
		name         string
		fieldTypes   []types.Type
		expectedSize uint32
	}{
		{
			name:         "Int field only",
			fieldTypes:   []types.Type{types.IntType},
			expectedSize: 8, // int64 is 8 bytes
		},
		{
			name:         "String field only",
			fieldTypes:   []types.Type{types.StringType},
			expectedSize: 4 + 256, // 4 for length + 256 for max size (StringMaxSize)
		},
		{
			name:         "Int and String fields",
			fieldTypes:   []types.Type{types.IntType, types.StringType},
			expectedSize: 8 + 260, // 8 (int) + 260 (string)
		},
		{
			name:         "Multiple fields",
			fieldTypes:   []types.Type{types.IntType, types.IntType, types.StringType},
			expectedSize: 8 + 8 + 260, // 8 (int) + 8 (int) + 260 (string)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			td := mustCreateTupleDesc(tt.fieldTypes, nil)
			size := td.GetSize()

			if size != tt.expectedSize {
				t.Errorf("Expected size %d, got %d", tt.expectedSize, size)
			}
		})
	}
}

func TestTupleDescription_Equals(t *testing.T) {
	td1 := mustCreateTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"id", "name"})
	td2 := mustCreateTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"user_id", "username"})
	td3 := mustCreateTupleDesc([]types.Type{types.IntType}, []string{"id"})
	td4 := mustCreateTupleDesc([]types.Type{types.StringType, types.IntType}, []string{"name", "id"})

	tests := []struct {
		name     string
		td1      *TupleDescription
		td2      *TupleDescription
		expected bool
	}{
		{
			name:     "Equal tuple descriptions",
			td1:      td1,
			td2:      td2,
			expected: true,
		},
		{
			name:     "Different number of fields",
			td1:      td1,
			td2:      td3,
			expected: false,
		},
		{
			name:     "Different field types order",
			td1:      td1,
			td2:      td4,
			expected: false,
		},
		{
			name:     "Comparison with nil",
			td1:      td1,
			td2:      nil,
			expected: false,
		},
		{
			name:     "Same reference",
			td1:      td1,
			td2:      td1,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.td1.Equals(tt.td2)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestTupleDescription_String(t *testing.T) {
	tests := []struct {
		name           string
		fieldTypes     []types.Type
		fieldNames     []string
		expectedString string
	}{
		{
			name:           "With field names",
			fieldTypes:     []types.Type{types.IntType, types.StringType},
			fieldNames:     []string{"id", "name"},
			expectedString: "INT_TYPE(id),STRING_TYPE(name)",
		},
		{
			name:           "Without field names",
			fieldTypes:     []types.Type{types.IntType, types.StringType},
			fieldNames:     nil,
			expectedString: "INT_TYPE(null),STRING_TYPE(null)",
		},
		{
			name:           "Single field with name",
			fieldTypes:     []types.Type{types.IntType},
			fieldNames:     []string{"id"},
			expectedString: "INT_TYPE(id)",
		},
		{
			name:           "Single field without name",
			fieldTypes:     []types.Type{types.StringType},
			fieldNames:     nil,
			expectedString: "STRING_TYPE(null)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			td := mustCreateTupleDesc(tt.fieldTypes, tt.fieldNames)
			result := td.String()

			if result != tt.expectedString {
				t.Errorf("Expected string %s, got %s", tt.expectedString, result)
			}
		})
	}
}

func TestCombine(t *testing.T) {
	td1 := mustCreateTupleDesc([]types.Type{types.IntType}, []string{"id"})
	td2 := mustCreateTupleDesc([]types.Type{types.StringType}, []string{"name"})
	td3 := mustCreateTupleDesc([]types.Type{types.IntType}, nil)
	td4 := mustCreateTupleDesc([]types.Type{types.StringType}, nil)

	tests := []struct {
		name               string
		td1                *TupleDescription
		td2                *TupleDescription
		expectedTypes      []types.Type
		expectedFieldNames []string
		expectedHasNames   bool
	}{
		{
			name:               "Combine two descriptions with names",
			td1:                td1,
			td2:                td2,
			expectedTypes:      []types.Type{types.IntType, types.StringType},
			expectedFieldNames: []string{"id", "name"},
			expectedHasNames:   true,
		},
		{
			name:             "Combine two descriptions without names",
			td1:              td3,
			td2:              td4,
			expectedTypes:    []types.Type{types.IntType, types.StringType},
			expectedHasNames: false,
		},
		{
			name:               "Combine description with names and without names",
			td1:                td1,
			td2:                td4,
			expectedTypes:      []types.Type{types.IntType, types.StringType},
			expectedFieldNames: []string{"id", ""},
			expectedHasNames:   true,
		},
		{
			name:               "Combine description without names and with names",
			td1:                td3,
			td2:                td2,
			expectedTypes:      []types.Type{types.IntType, types.StringType},
			expectedFieldNames: []string{"", "name"},
			expectedHasNames:   true,
		},
		{
			name:          "Combine with first nil",
			td1:           nil,
			td2:           td2,
			expectedTypes: []types.Type{types.StringType},
		},
		{
			name:          "Combine with second nil",
			td1:           td1,
			td2:           nil,
			expectedTypes: []types.Type{types.IntType},
		},
		{
			name: "Combine both nil",
			td1:  nil,
			td2:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Combine(tt.td1, tt.td2)

			if tt.td1 == nil && tt.td2 == nil {
				if result != nil {
					t.Errorf("Expected nil result when both inputs are nil")
				}
				return
			}

			if tt.td1 == nil {
				if result != tt.td2 {
					t.Errorf("Expected result to be td2 when td1 is nil")
				}
				return
			}

			if tt.td2 == nil {
				if result != tt.td1 {
					t.Errorf("Expected result to be td1 when td2 is nil")
				}
				return
			}

			if len(result.Types) != len(tt.expectedTypes) {
				t.Errorf("Expected %d types, got %d", len(tt.expectedTypes), len(result.Types))
			}

			for i, expectedType := range tt.expectedTypes {
				if result.Types[i] != expectedType {
					t.Errorf("Expected type %v at index %d, got %v", expectedType, i, result.Types[i])
				}
			}

			if tt.expectedHasNames {
				if len(result.FieldNames) != len(tt.expectedFieldNames) {
					t.Errorf("Expected %d field names, got %d", len(tt.expectedFieldNames), len(result.FieldNames))
				}
				for i, expectedName := range tt.expectedFieldNames {
					if result.FieldNames[i] != expectedName {
						t.Errorf("Expected field name %s at index %d, got %s", expectedName, i, result.FieldNames[i])
					}
				}
			} else {
				if result.FieldNames != nil {
					t.Errorf("Expected no field names, but got %v", result.FieldNames)
				}
			}
		})
	}
}

func mustCreateTupleDesc(fieldTypes []types.Type, fieldNames []string) *TupleDescription {
	td, err := NewTupleDesc(fieldTypes, fieldNames)
	if err != nil {
		panic(err)
	}
	return td
}
