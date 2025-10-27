package tuple

import (
	"storemy/pkg/primitives"
	"storemy/pkg/types"
	"testing"
	"time"
)

func TestNewBuilder(t *testing.T) {
	td := mustCreateTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"id", "name"})

	builder := NewBuilder(td)

	if builder == nil {
		t.Fatal("NewBuilder returned nil")
	}

	if builder.tuple == nil {
		t.Error("Builder tuple is nil")
	}

	if builder.currentIndex != 0 {
		t.Errorf("Expected currentIndex to be 0, got %d", builder.currentIndex)
	}

	if builder.err != nil {
		t.Errorf("Expected err to be nil, got %v", builder.err)
	}

	if builder.tuple.TupleDesc != td {
		t.Errorf("Expected TupleDesc to be %v, got %v", td, builder.tuple.TupleDesc)
	}
}

func TestBuilder_AddInt(t *testing.T) {
	td := mustCreateTupleDesc([]types.Type{types.IntType}, []string{"value"})

	tuple, err := NewBuilder(td).AddInt(42).Build()

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	field, err := tuple.GetField(0)
	if err != nil {
		t.Fatalf("Failed to get field: %v", err)
	}

	intField, ok := field.(*types.IntField)
	if !ok {
		t.Fatalf("Expected IntField, got %T", field)
	}

	if intField.Value != 42 {
		t.Errorf("Expected value 42, got %d", intField.Value)
	}
}

func TestBuilder_AddInt32(t *testing.T) {
	td := mustCreateTupleDesc([]types.Type{types.Int32Type}, []string{"value"})

	tuple, err := NewBuilder(td).AddInt32(42).Build()

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	field, err := tuple.GetField(0)
	if err != nil {
		t.Fatalf("Failed to get field: %v", err)
	}

	int32Field, ok := field.(*types.Int32Field)
	if !ok {
		t.Fatalf("Expected Int32Field, got %T", field)
	}

	if int32Field.Value != 42 {
		t.Errorf("Expected value 42, got %d", int32Field.Value)
	}
}

func TestBuilder_AddInt64(t *testing.T) {
	td := mustCreateTupleDesc([]types.Type{types.Int64Type}, []string{"value"})

	tuple, err := NewBuilder(td).AddInt64(9223372036854775807).Build()

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	field, err := tuple.GetField(0)
	if err != nil {
		t.Fatalf("Failed to get field: %v", err)
	}

	int64Field, ok := field.(*types.Int64Field)
	if !ok {
		t.Fatalf("Expected Int64Field, got %T", field)
	}

	if int64Field.Value != 9223372036854775807 {
		t.Errorf("Expected value 9223372036854775807, got %d", int64Field.Value)
	}
}

func TestBuilder_AddUint32(t *testing.T) {
	td := mustCreateTupleDesc([]types.Type{types.Uint32Type}, []string{"value"})

	tuple, err := NewBuilder(td).AddUint32(4294967295).Build()

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	field, err := tuple.GetField(0)
	if err != nil {
		t.Fatalf("Failed to get field: %v", err)
	}

	uint32Field, ok := field.(*types.Uint32Field)
	if !ok {
		t.Fatalf("Expected Uint32Field, got %T", field)
	}

	if uint32Field.Value != 4294967295 {
		t.Errorf("Expected value 4294967295, got %d", uint32Field.Value)
	}
}

func TestBuilder_AddUint64(t *testing.T) {
	td := mustCreateTupleDesc([]types.Type{types.Uint64Type}, []string{"value"})

	tuple, err := NewBuilder(td).AddUint64(18446744073709551615).Build()

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	field, err := tuple.GetField(0)
	if err != nil {
		t.Fatalf("Failed to get field: %v", err)
	}

	uint64Field, ok := field.(*types.Uint64Field)
	if !ok {
		t.Fatalf("Expected Uint64Field, got %T", field)
	}

	if uint64Field.Value != 18446744073709551615 {
		t.Errorf("Expected value 18446744073709551615, got %d", uint64Field.Value)
	}
}

func TestBuilder_AddString(t *testing.T) {
	td := mustCreateTupleDesc([]types.Type{types.StringType}, []string{"name"})

	tuple, err := NewBuilder(td).AddString("Hello World").Build()

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	field, err := tuple.GetField(0)
	if err != nil {
		t.Fatalf("Failed to get field: %v", err)
	}

	stringField, ok := field.(*types.StringField)
	if !ok {
		t.Fatalf("Expected StringField, got %T", field)
	}

	if stringField.Value != "Hello World" {
		t.Errorf("Expected value 'Hello World', got '%s'", stringField.Value)
	}
}

func TestBuilder_AddFloat(t *testing.T) {
	td := mustCreateTupleDesc([]types.Type{types.FloatType}, []string{"value"})

	tuple, err := NewBuilder(td).AddFloat(3.14159).Build()

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	field, err := tuple.GetField(0)
	if err != nil {
		t.Fatalf("Failed to get field: %v", err)
	}

	floatField, ok := field.(*types.Float64Field)
	if !ok {
		t.Fatalf("Expected Float64Field, got %T", field)
	}

	if floatField.Value != 3.14159 {
		t.Errorf("Expected value 3.14159, got %f", floatField.Value)
	}
}

func TestBuilder_AddBool(t *testing.T) {
	tests := []struct {
		name     string
		value    bool
		expected bool
	}{
		{
			name:     "true value",
			value:    true,
			expected: true,
		},
		{
			name:     "false value",
			value:    false,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			td := mustCreateTupleDesc([]types.Type{types.BoolType}, []string{"flag"})

			tuple, err := NewBuilder(td).AddBool(tt.value).Build()

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			field, err := tuple.GetField(0)
			if err != nil {
				t.Fatalf("Failed to get field: %v", err)
			}

			boolField, ok := field.(*types.BoolField)
			if !ok {
				t.Fatalf("Expected BoolField, got %T", field)
			}

			if boolField.Value != tt.expected {
				t.Errorf("Expected value %v, got %v", tt.expected, boolField.Value)
			}
		})
	}
}

func TestBuilder_AddTimestamp(t *testing.T) {
	td := mustCreateTupleDesc([]types.Type{types.IntType}, []string{"created_at"})

	now := time.Date(2024, 1, 15, 12, 30, 45, 0, time.UTC)

	tuple, err := NewBuilder(td).AddTimestamp(now).Build()

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	field, err := tuple.GetField(0)
	if err != nil {
		t.Fatalf("Failed to get field: %v", err)
	}

	intField, ok := field.(*types.IntField)
	if !ok {
		t.Fatalf("Expected IntField, got %T", field)
	}

	expectedUnix := now.Unix()
	if intField.Value != expectedUnix {
		t.Errorf("Expected timestamp %d, got %d", expectedUnix, intField.Value)
	}
}

func TestBuilder_AddFloatAsScaledInt(t *testing.T) {
	td := mustCreateTupleDesc([]types.Type{types.IntType}, []string{"value"})

	tuple, err := NewBuilder(td).AddFloatAsScaledInt(3.14159, 1000000).Build()

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	field, err := tuple.GetField(0)
	if err != nil {
		t.Fatalf("Failed to get field: %v", err)
	}

	intField, ok := field.(*types.IntField)
	if !ok {
		t.Fatalf("Expected IntField, got %T", field)
	}

	expected := int64(3141590)
	if intField.Value != expected {
		t.Errorf("Expected value %d, got %d", expected, intField.Value)
	}
}

func TestBuilder_AddField(t *testing.T) {
	td := mustCreateTupleDesc([]types.Type{types.IntType}, []string{"id"})

	customField := types.NewIntField(999)

	tuple, err := NewBuilder(td).AddField(customField).Build()

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	field, err := tuple.GetField(0)
	if err != nil {
		t.Fatalf("Failed to get field: %v", err)
	}

	intField, ok := field.(*types.IntField)
	if !ok {
		t.Fatalf("Expected IntField, got %T", field)
	}

	if intField.Value != 999 {
		t.Errorf("Expected value 999, got %d", intField.Value)
	}
}

func TestBuilder_BuildChaining(t *testing.T) {
	td := mustCreateTupleDesc(
		[]types.Type{types.IntType, types.StringType, types.FloatType, types.BoolType},
		[]string{"id", "name", "score", "active"},
	)

	tuple, err := NewBuilder(td).
		AddInt(1).
		AddString("Alice").
		AddFloat(95.5).
		AddBool(true).
		Build()

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Verify field 0 (id)
	field0, _ := tuple.GetField(0)
	if intField, ok := field0.(*types.IntField); !ok || intField.Value != 1 {
		t.Errorf("Expected field 0 to be IntField with value 1")
	}

	// Verify field 1 (name)
	field1, _ := tuple.GetField(1)
	if strField, ok := field1.(*types.StringField); !ok || strField.Value != "Alice" {
		t.Errorf("Expected field 1 to be StringField with value 'Alice'")
	}

	// Verify field 2 (score)
	field2, _ := tuple.GetField(2)
	if floatField, ok := field2.(*types.Float64Field); !ok || floatField.Value != 95.5 {
		t.Errorf("Expected field 2 to be Float64Field with value 95.5")
	}

	// Verify field 3 (active)
	field3, _ := tuple.GetField(3)
	if boolField, ok := field3.(*types.BoolField); !ok || boolField.Value != true {
		t.Errorf("Expected field 3 to be BoolField with value true")
	}
}

func TestBuilder_Build_IncompleteTuple(t *testing.T) {
	td := mustCreateTupleDesc(
		[]types.Type{types.IntType, types.StringType},
		[]string{"id", "name"},
	)

	// Only add one field when two are expected
	_, err := NewBuilder(td).AddInt(1).Build()

	if err == nil {
		t.Fatal("Expected error for incomplete tuple, got nil")
	}

	expectedErrMsg := "incomplete tuple"
	if err.Error()[:len(expectedErrMsg)] != expectedErrMsg {
		t.Errorf("Expected error message to start with '%s', got '%s'", expectedErrMsg, err.Error())
	}
}

func TestBuilder_Build_NoFields(t *testing.T) {
	td := mustCreateTupleDesc([]types.Type{types.IntType}, []string{"id"})

	// Don't add any fields
	_, err := NewBuilder(td).Build()

	if err == nil {
		t.Fatal("Expected error for tuple with no fields added, got nil")
	}
}

func TestBuilder_TypeMismatch(t *testing.T) {
	td := mustCreateTupleDesc([]types.Type{types.StringType}, []string{"name"})

	// Try to add int field when string is expected
	_, err := NewBuilder(td).AddInt(42).Build()

	if err == nil {
		t.Fatal("Expected error for type mismatch, got nil")
	}

	expectedErrMsg := "field 0:"
	if err.Error()[:len(expectedErrMsg)] != expectedErrMsg {
		t.Errorf("Expected error message to start with '%s', got '%s'", expectedErrMsg, err.Error())
	}
}

func TestBuilder_ErrorPropagation(t *testing.T) {
	td := mustCreateTupleDesc(
		[]types.Type{types.StringType, types.IntType},
		[]string{"name", "id"},
	)

	// First add causes error, subsequent adds should not execute
	builder := NewBuilder(td).
		AddInt(42).        // This will fail due to type mismatch
		AddString("Alice") // This should not execute

	if builder.err == nil {
		t.Fatal("Expected error to be set in builder")
	}

	_, err := builder.Build()
	if err == nil {
		t.Fatal("Expected Build to return error, got nil")
	}
}

func TestBuilder_MustBuild_Success(t *testing.T) {
	td := mustCreateTupleDesc([]types.Type{types.IntType}, []string{"id"})

	// Should not panic
	tuple := NewBuilder(td).AddInt(42).MustBuild()

	if tuple == nil {
		t.Fatal("MustBuild returned nil")
	}

	field, _ := tuple.GetField(0)
	if intField, ok := field.(*types.IntField); !ok || intField.Value != 42 {
		t.Error("Expected field to be IntField with value 42")
	}
}

func TestBuilder_MustBuild_Panic(t *testing.T) {
	td := mustCreateTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"id", "name"})

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected MustBuild to panic, but it didn't")
		}
	}()

	// This should panic due to incomplete tuple
	NewBuilder(td).AddInt(1).MustBuild()
}

func TestBuilder_AllIntegerTypes(t *testing.T) {
	td := mustCreateTupleDesc(
		[]types.Type{types.Int32Type, types.Int64Type, types.Uint32Type, types.Uint64Type, types.IntType},
		[]string{"int32", "int64", "uint32", "uint64", "int"},
	)

	tuple, err := NewBuilder(td).
		AddInt32(32).
		AddInt64(64).
		AddUint32(132).
		AddUint64(164).
		AddInt(42).
		Build()

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Verify all fields
	tests := []struct {
		index        primitives.ColumnID
		expectedType string
		checkValue   func(field types.Field) bool
	}{
		{
			index:        0,
			expectedType: "*types.Int32Field",
			checkValue: func(field types.Field) bool {
				f, ok := field.(*types.Int32Field)
				return ok && f.Value == 32
			},
		},
		{
			index:        1,
			expectedType: "*types.Int64Field",
			checkValue: func(field types.Field) bool {
				f, ok := field.(*types.Int64Field)
				return ok && f.Value == 64
			},
		},
		{
			index:        2,
			expectedType: "*types.Uint32Field",
			checkValue: func(field types.Field) bool {
				f, ok := field.(*types.Uint32Field)
				return ok && f.Value == 132
			},
		},
		{
			index:        3,
			expectedType: "*types.Uint64Field",
			checkValue: func(field types.Field) bool {
				f, ok := field.(*types.Uint64Field)
				return ok && f.Value == 164
			},
		},
		{
			index:        4,
			expectedType: "*types.IntField",
			checkValue: func(field types.Field) bool {
				f, ok := field.(*types.IntField)
				return ok && f.Value == 42
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.expectedType, func(t *testing.T) {
			field, err := tuple.GetField(tt.index)
			if err != nil {
				t.Fatalf("Failed to get field %d: %v", tt.index, err)
			}

			if !tt.checkValue(field) {
				t.Errorf("Field %d failed validation", tt.index)
			}
		})
	}
}

func TestBuilder_EmptyString(t *testing.T) {
	td := mustCreateTupleDesc([]types.Type{types.StringType}, []string{"value"})

	tuple, err := NewBuilder(td).AddString("").Build()

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	field, _ := tuple.GetField(0)
	if stringField, ok := field.(*types.StringField); !ok || stringField.Value != "" {
		t.Error("Expected empty string field")
	}
}

func TestBuilder_ZeroValues(t *testing.T) {
	td := mustCreateTupleDesc(
		[]types.Type{types.IntType, types.FloatType, types.BoolType},
		[]string{"int", "float", "bool"},
	)

	tuple, err := NewBuilder(td).
		AddInt(0).
		AddFloat(0.0).
		AddBool(false).
		Build()

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Verify zero int
	field0, _ := tuple.GetField(0)
	if intField, ok := field0.(*types.IntField); !ok || intField.Value != 0 {
		t.Error("Expected zero int field")
	}

	// Verify zero float
	field1, _ := tuple.GetField(1)
	if floatField, ok := field1.(*types.Float64Field); !ok || floatField.Value != 0.0 {
		t.Error("Expected zero float field")
	}

	// Verify false bool
	field2, _ := tuple.GetField(2)
	if boolField, ok := field2.(*types.BoolField); !ok || boolField.Value != false {
		t.Error("Expected false bool field")
	}
}

func TestBuilder_NegativeNumbers(t *testing.T) {
	td := mustCreateTupleDesc(
		[]types.Type{types.Int32Type, types.Int64Type, types.IntType, types.FloatType},
		[]string{"int32", "int64", "int", "float"},
	)

	tuple, err := NewBuilder(td).
		AddInt32(-32).
		AddInt64(-64).
		AddInt(-42).
		AddFloat(-3.14).
		Build()

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Verify negative int32
	field0, _ := tuple.GetField(0)
	if f, ok := field0.(*types.Int32Field); !ok || f.Value != -32 {
		t.Error("Expected negative int32 field")
	}

	// Verify negative int64
	field1, _ := tuple.GetField(1)
	if f, ok := field1.(*types.Int64Field); !ok || f.Value != -64 {
		t.Error("Expected negative int64 field")
	}

	// Verify negative int
	field2, _ := tuple.GetField(2)
	if f, ok := field2.(*types.IntField); !ok || f.Value != -42 {
		t.Error("Expected negative int field")
	}

	// Verify negative float
	field3, _ := tuple.GetField(3)
	if f, ok := field3.(*types.Float64Field); !ok || f.Value != -3.14 {
		t.Error("Expected negative float field")
	}
}

func TestBuilder_MultipleBuildsFromSameDesc(t *testing.T) {
	td := mustCreateTupleDesc(
		[]types.Type{types.IntType, types.StringType},
		[]string{"id", "name"},
	)

	// Build first tuple
	tuple1, err := NewBuilder(td).AddInt(1).AddString("Alice").Build()
	if err != nil {
		t.Fatalf("First build failed: %v", err)
	}

	// Build second tuple with different values
	tuple2, err := NewBuilder(td).AddInt(2).AddString("Bob").Build()
	if err != nil {
		t.Fatalf("Second build failed: %v", err)
	}

	// Verify they are different
	field1, _ := tuple1.GetField(0)
	field2, _ := tuple2.GetField(0)

	if field1.(*types.IntField).Value == field2.(*types.IntField).Value {
		t.Error("Expected different values in separate builds")
	}
}
