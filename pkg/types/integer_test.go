package types

import (
	"bytes"
	"storemy/pkg/primitives"
	"testing"
)

func TestNewIntField(t *testing.T) {
	value := int64(42)
	field := NewIntField(value)

	if field.Value != value {
		t.Errorf("Expected value %d, got %d", value, field.Value)
	}
}

func TestIntField_Type(t *testing.T) {
	field := NewIntField(42)

	if field.Type() != IntType {
		t.Errorf("Expected type %v, got %v", IntType, field.Type())
	}
}

func TestIntField_String(t *testing.T) {
	field := NewIntField(42)
	expected := "42"

	if field.String() != expected {
		t.Errorf("Expected string %s, got %s", expected, field.String())
	}
}

func TestIntField_Length(t *testing.T) {
	field := NewIntField(42)
	expected := uint32(8)

	if field.Length() != expected {
		t.Errorf("Expected length %d, got %d", expected, field.Length())
	}
}

func TestIntField_Equals(t *testing.T) {
	field1 := NewIntField(42)
	field2 := NewIntField(42)
	field3 := NewIntField(24)
	stringField := NewStringField("test", 10)

	if !field1.Equals(field2) {
		t.Error("Expected equal fields to return true")
	}

	if field1.Equals(field3) {
		t.Error("Expected unequal fields to return false")
	}

	if field1.Equals(stringField) {
		t.Error("Expected different field types to return false")
	}
}

func TestIntField_Hash(t *testing.T) {
	field := NewIntField(42)
	hash, err := field.Hash()

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// FNV-1a hash of 42 (as 8 bytes in little-endian)
	if hash != primitives.HashCode(3990555855) {
		t.Errorf("Expected hash %d, got %d", primitives.HashCode(3990555855), hash)
	}
}

func TestIntField_Hash_Consistency(t *testing.T) {
	// Test that hashing the same value produces consistent results
	field1 := NewIntField(42)
	field2 := NewIntField(42)

	hash1, err1 := field1.Hash()
	hash2, err2 := field2.Hash()

	if err1 != nil || err2 != nil {
		t.Fatalf("Unexpected errors: %v, %v", err1, err2)
	}

	if hash1 != hash2 {
		t.Errorf("Hash should be consistent for same value: got %d and %d", hash1, hash2)
	}

	// Test that different values produce different hashes
	field3 := NewIntField(100)
	hash3, _ := field3.Hash()

	if hash1 == hash3 {
		t.Error("Hash should be different for different values (42 vs 100)")
	}

	// Test negative numbers
	field4 := NewIntField(-42)
	hash4, _ := field4.Hash()

	if hash1 == hash4 {
		t.Error("Hash should be different for 42 and -42")
	}
}

func TestIntField_Serialize(t *testing.T) {
	field := NewIntField(42)
	var buf bytes.Buffer

	err := field.Serialize(&buf)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if buf.Len() != 8 {
		t.Errorf("Expected 8 bytes, got %d", buf.Len())
	}
}

func TestIntField_Compare(t *testing.T) {
	field1 := NewIntField(10)
	field2 := NewIntField(20)
	field3 := NewIntField(10)
	stringField := NewStringField("test", 10)

	tests := []struct {
		op       primitives.Predicate
		other    Field
		expected bool
	}{
		{primitives.Equals, field3, true},
		{primitives.Equals, field2, false},
		{primitives.LessThan, field2, true},
		{primitives.LessThan, field3, false},
		{primitives.GreaterThan, field2, false},
		{primitives.GreaterThan, NewIntField(5), true},
		{primitives.LessThanOrEqual, field2, true},
		{primitives.LessThanOrEqual, field3, true},
		{primitives.LessThanOrEqual, NewIntField(5), false},
		{primitives.GreaterThanOrEqual, field3, true},
		{primitives.GreaterThanOrEqual, NewIntField(5), true},
		{primitives.GreaterThanOrEqual, field2, false},
		{primitives.NotEqual, field2, true},
		{primitives.NotEqual, field3, false},
		{primitives.Equals, stringField, false},
	}

	for _, test := range tests {
		result, err := field1.Compare(test.op, test.other)
		if err != nil {
			t.Errorf("Unexpected error for %v: %v", test.op, err)
		}

		if result != test.expected {
			t.Errorf("Compare(%v, %v) = %v, expected %v",
				test.op, test.other, result, test.expected)
		}
	}
}

// ===== Int32Field Tests =====

func TestNewInt32Field(t *testing.T) {
	value := int32(42)
	field := NewInt32Field(value)

	if field.Value != value {
		t.Errorf("Expected value %d, got %d", value, field.Value)
	}
}

func TestInt32Field_Type(t *testing.T) {
	field := NewInt32Field(42)

	if field.Type() != Int32Type {
		t.Errorf("Expected type %v, got %v", Int32Type, field.Type())
	}
}

func TestInt32Field_String(t *testing.T) {
	tests := []struct {
		value    int32
		expected string
	}{
		{42, "42"},
		{-42, "-42"},
		{0, "0"},
		{2147483647, "2147483647"},
		{-2147483648, "-2147483648"},
	}

	for _, test := range tests {
		field := NewInt32Field(test.value)
		if field.String() != test.expected {
			t.Errorf("Expected string %s, got %s", test.expected, field.String())
		}
	}
}

func TestInt32Field_Length(t *testing.T) {
	field := NewInt32Field(42)
	expected := uint32(4)

	if field.Length() != expected {
		t.Errorf("Expected length %d, got %d", expected, field.Length())
	}
}

func TestInt32Field_Equals(t *testing.T) {
	field1 := NewInt32Field(42)
	field2 := NewInt32Field(42)
	field3 := NewInt32Field(24)
	stringField := NewStringField("test", 10)

	if !field1.Equals(field2) {
		t.Error("Expected equal fields to return true")
	}

	if field1.Equals(field3) {
		t.Error("Expected unequal fields to return false")
	}

	if field1.Equals(stringField) {
		t.Error("Expected different field types to return false")
	}
}

func TestInt32Field_Hash(t *testing.T) {
	field1 := NewInt32Field(42)
	field2 := NewInt32Field(42)

	hash1, err1 := field1.Hash()
	hash2, err2 := field2.Hash()

	if err1 != nil || err2 != nil {
		t.Fatalf("Unexpected errors: %v, %v", err1, err2)
	}

	if hash1 != hash2 {
		t.Errorf("Hash should be consistent for same value: got %d and %d", hash1, hash2)
	}

	field3 := NewInt32Field(100)
	hash3, _ := field3.Hash()

	if hash1 == hash3 {
		t.Error("Hash should be different for different values (42 vs 100)")
	}

	field4 := NewInt32Field(-42)
	hash4, _ := field4.Hash()

	if hash1 == hash4 {
		t.Error("Hash should be different for 42 and -42")
	}
}

func TestInt32Field_Serialize(t *testing.T) {
	field := NewInt32Field(42)
	var buf bytes.Buffer

	err := field.Serialize(&buf)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if buf.Len() != 4 {
		t.Errorf("Expected 4 bytes, got %d", buf.Len())
	}
}

func TestInt32Field_Compare(t *testing.T) {
	field1 := NewInt32Field(10)
	field2 := NewInt32Field(20)
	field3 := NewInt32Field(10)
	stringField := NewStringField("test", 10)

	tests := []struct {
		op       primitives.Predicate
		other    Field
		expected bool
	}{
		{primitives.Equals, field3, true},
		{primitives.Equals, field2, false},
		{primitives.LessThan, field2, true},
		{primitives.LessThan, field3, false},
		{primitives.GreaterThan, field2, false},
		{primitives.GreaterThan, NewInt32Field(5), true},
		{primitives.LessThanOrEqual, field2, true},
		{primitives.LessThanOrEqual, field3, true},
		{primitives.LessThanOrEqual, NewInt32Field(5), false},
		{primitives.GreaterThanOrEqual, field3, true},
		{primitives.GreaterThanOrEqual, NewInt32Field(5), true},
		{primitives.GreaterThanOrEqual, field2, false},
		{primitives.NotEqual, field2, true},
		{primitives.NotEqual, field3, false},
		{primitives.Equals, stringField, false},
	}

	for _, test := range tests {
		result, err := field1.Compare(test.op, test.other)
		if err != nil {
			t.Errorf("Unexpected error for %v: %v", test.op, err)
		}

		if result != test.expected {
			t.Errorf("Compare(%v, %v) = %v, expected %v",
				test.op, test.other, result, test.expected)
		}
	}
}

func TestInt32Field_EdgeValues(t *testing.T) {
	tests := []int32{
		0,
		1,
		-1,
		2147483647,  // max int32
		-2147483648, // min int32
	}

	for _, value := range tests {
		field := NewInt32Field(value)

		if field.Value != value {
			t.Errorf("Expected value %d, got %d", value, field.Value)
		}

		var buf bytes.Buffer
		err := field.Serialize(&buf)
		if err != nil {
			t.Errorf("Failed to serialize %d: %v", value, err)
		}

		if buf.Len() != 4 {
			t.Errorf("Expected 4 bytes for %d, got %d", value, buf.Len())
		}
	}
}

// ===== Int64Field Tests =====

func TestNewInt64Field(t *testing.T) {
	value := int64(42)
	field := NewInt64Field(value)

	if field.Value != value {
		t.Errorf("Expected value %d, got %d", value, field.Value)
	}
}

func TestInt64Field_Type(t *testing.T) {
	field := NewInt64Field(42)

	if field.Type() != Int64Type {
		t.Errorf("Expected type %v, got %v", Int64Type, field.Type())
	}
}

func TestInt64Field_String(t *testing.T) {
	tests := []struct {
		value    int64
		expected string
	}{
		{42, "42"},
		{-42, "-42"},
		{0, "0"},
		{9223372036854775807, "9223372036854775807"},   // max int64
		{-9223372036854775808, "-9223372036854775808"}, // min int64
	}

	for _, test := range tests {
		field := NewInt64Field(test.value)
		if field.String() != test.expected {
			t.Errorf("Expected string %s, got %s", test.expected, field.String())
		}
	}
}

func TestInt64Field_Length(t *testing.T) {
	field := NewInt64Field(42)
	expected := uint32(8)

	if field.Length() != expected {
		t.Errorf("Expected length %d, got %d", expected, field.Length())
	}
}

func TestInt64Field_Equals(t *testing.T) {
	field1 := NewInt64Field(42)
	field2 := NewInt64Field(42)
	field3 := NewInt64Field(24)
	stringField := NewStringField("test", 10)

	if !field1.Equals(field2) {
		t.Error("Expected equal fields to return true")
	}

	if field1.Equals(field3) {
		t.Error("Expected unequal fields to return false")
	}

	if field1.Equals(stringField) {
		t.Error("Expected different field types to return false")
	}
}

func TestInt64Field_Hash(t *testing.T) {
	field1 := NewInt64Field(42)
	field2 := NewInt64Field(42)

	hash1, err1 := field1.Hash()
	hash2, err2 := field2.Hash()

	if err1 != nil || err2 != nil {
		t.Fatalf("Unexpected errors: %v, %v", err1, err2)
	}

	if hash1 != hash2 {
		t.Errorf("Hash should be consistent for same value: got %d and %d", hash1, hash2)
	}

	field3 := NewInt64Field(100)
	hash3, _ := field3.Hash()

	if hash1 == hash3 {
		t.Error("Hash should be different for different values (42 vs 100)")
	}

	field4 := NewInt64Field(-42)
	hash4, _ := field4.Hash()

	if hash1 == hash4 {
		t.Error("Hash should be different for 42 and -42")
	}
}

func TestInt64Field_Serialize(t *testing.T) {
	field := NewInt64Field(42)
	var buf bytes.Buffer

	err := field.Serialize(&buf)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if buf.Len() != 8 {
		t.Errorf("Expected 8 bytes, got %d", buf.Len())
	}
}

func TestInt64Field_Compare(t *testing.T) {
	field1 := NewInt64Field(10)
	field2 := NewInt64Field(20)
	field3 := NewInt64Field(10)
	stringField := NewStringField("test", 10)

	tests := []struct {
		op       primitives.Predicate
		other    Field
		expected bool
	}{
		{primitives.Equals, field3, true},
		{primitives.Equals, field2, false},
		{primitives.LessThan, field2, true},
		{primitives.LessThan, field3, false},
		{primitives.GreaterThan, field2, false},
		{primitives.GreaterThan, NewInt64Field(5), true},
		{primitives.LessThanOrEqual, field2, true},
		{primitives.LessThanOrEqual, field3, true},
		{primitives.LessThanOrEqual, NewInt64Field(5), false},
		{primitives.GreaterThanOrEqual, field3, true},
		{primitives.GreaterThanOrEqual, NewInt64Field(5), true},
		{primitives.GreaterThanOrEqual, field2, false},
		{primitives.NotEqual, field2, true},
		{primitives.NotEqual, field3, false},
		{primitives.Equals, stringField, false},
	}

	for _, test := range tests {
		result, err := field1.Compare(test.op, test.other)
		if err != nil {
			t.Errorf("Unexpected error for %v: %v", test.op, err)
		}

		if result != test.expected {
			t.Errorf("Compare(%v, %v) = %v, expected %v",
				test.op, test.other, result, test.expected)
		}
	}
}

func TestInt64Field_EdgeValues(t *testing.T) {
	tests := []int64{
		0,
		1,
		-1,
		9223372036854775807,  // max int64
		-9223372036854775808, // min int64
	}

	for _, value := range tests {
		field := NewInt64Field(value)

		if field.Value != value {
			t.Errorf("Expected value %d, got %d", value, field.Value)
		}

		var buf bytes.Buffer
		err := field.Serialize(&buf)
		if err != nil {
			t.Errorf("Failed to serialize %d: %v", value, err)
		}

		if buf.Len() != 8 {
			t.Errorf("Expected 8 bytes for %d, got %d", value, buf.Len())
		}
	}
}

// ===== Uint32Field Tests =====

func TestNewUint32Field(t *testing.T) {
	value := uint32(42)
	field := NewUint32Field(value)

	if field.Value != value {
		t.Errorf("Expected value %d, got %d", value, field.Value)
	}
}

func TestUint32Field_Type(t *testing.T) {
	field := NewUint32Field(42)

	if field.Type() != Uint32Type {
		t.Errorf("Expected type %v, got %v", Uint32Type, field.Type())
	}
}

func TestUint32Field_String(t *testing.T) {
	tests := []struct {
		value    uint32
		expected string
	}{
		{42, "42"},
		{0, "0"},
		{4294967295, "4294967295"}, // max uint32
	}

	for _, test := range tests {
		field := NewUint32Field(test.value)
		if field.String() != test.expected {
			t.Errorf("Expected string %s, got %s", test.expected, field.String())
		}
	}
}

func TestUint32Field_Length(t *testing.T) {
	field := NewUint32Field(42)
	expected := uint32(4)

	if field.Length() != expected {
		t.Errorf("Expected length %d, got %d", expected, field.Length())
	}
}

func TestUint32Field_Equals(t *testing.T) {
	field1 := NewUint32Field(42)
	field2 := NewUint32Field(42)
	field3 := NewUint32Field(24)
	stringField := NewStringField("test", 10)

	if !field1.Equals(field2) {
		t.Error("Expected equal fields to return true")
	}

	if field1.Equals(field3) {
		t.Error("Expected unequal fields to return false")
	}

	if field1.Equals(stringField) {
		t.Error("Expected different field types to return false")
	}
}

func TestUint32Field_Hash(t *testing.T) {
	field1 := NewUint32Field(42)
	field2 := NewUint32Field(42)

	hash1, err1 := field1.Hash()
	hash2, err2 := field2.Hash()

	if err1 != nil || err2 != nil {
		t.Fatalf("Unexpected errors: %v, %v", err1, err2)
	}

	if hash1 != hash2 {
		t.Errorf("Hash should be consistent for same value: got %d and %d", hash1, hash2)
	}

	field3 := NewUint32Field(100)
	hash3, _ := field3.Hash()

	if hash1 == hash3 {
		t.Error("Hash should be different for different values (42 vs 100)")
	}
}

func TestUint32Field_Serialize(t *testing.T) {
	field := NewUint32Field(42)
	var buf bytes.Buffer

	err := field.Serialize(&buf)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if buf.Len() != 4 {
		t.Errorf("Expected 4 bytes, got %d", buf.Len())
	}
}

func TestUint32Field_Compare(t *testing.T) {
	field1 := NewUint32Field(10)
	field2 := NewUint32Field(20)
	field3 := NewUint32Field(10)
	stringField := NewStringField("test", 10)

	tests := []struct {
		op       primitives.Predicate
		other    Field
		expected bool
	}{
		{primitives.Equals, field3, true},
		{primitives.Equals, field2, false},
		{primitives.LessThan, field2, true},
		{primitives.LessThan, field3, false},
		{primitives.GreaterThan, field2, false},
		{primitives.GreaterThan, NewUint32Field(5), true},
		{primitives.LessThanOrEqual, field2, true},
		{primitives.LessThanOrEqual, field3, true},
		{primitives.LessThanOrEqual, NewUint32Field(5), false},
		{primitives.GreaterThanOrEqual, field3, true},
		{primitives.GreaterThanOrEqual, NewUint32Field(5), true},
		{primitives.GreaterThanOrEqual, field2, false},
		{primitives.NotEqual, field2, true},
		{primitives.NotEqual, field3, false},
		{primitives.Equals, stringField, false},
	}

	for _, test := range tests {
		result, err := field1.Compare(test.op, test.other)
		if err != nil {
			t.Errorf("Unexpected error for %v: %v", test.op, err)
		}

		if result != test.expected {
			t.Errorf("Compare(%v, %v) = %v, expected %v",
				test.op, test.other, result, test.expected)
		}
	}
}

func TestUint32Field_EdgeValues(t *testing.T) {
	tests := []uint32{
		0,
		1,
		4294967295, // max uint32
	}

	for _, value := range tests {
		field := NewUint32Field(value)

		if field.Value != value {
			t.Errorf("Expected value %d, got %d", value, field.Value)
		}

		var buf bytes.Buffer
		err := field.Serialize(&buf)
		if err != nil {
			t.Errorf("Failed to serialize %d: %v", value, err)
		}

		if buf.Len() != 4 {
			t.Errorf("Expected 4 bytes for %d, got %d", value, buf.Len())
		}
	}
}

// ===== Uint64Field Tests =====

func TestNewUint64Field(t *testing.T) {
	value := uint64(42)
	field := NewUint64Field(value)

	if field.Value != value {
		t.Errorf("Expected value %d, got %d", value, field.Value)
	}
}

func TestUint64Field_Type(t *testing.T) {
	field := NewUint64Field(42)

	if field.Type() != Uint64Type {
		t.Errorf("Expected type %v, got %v", Uint64Type, field.Type())
	}
}

func TestUint64Field_String(t *testing.T) {
	tests := []struct {
		value    uint64
		expected string
	}{
		{42, "42"},
		{0, "0"},
		{18446744073709551615, "18446744073709551615"}, // max uint64
	}

	for _, test := range tests {
		field := NewUint64Field(test.value)
		if field.String() != test.expected {
			t.Errorf("Expected string %s, got %s", test.expected, field.String())
		}
	}
}

func TestUint64Field_Length(t *testing.T) {
	field := NewUint64Field(42)
	expected := uint32(8)

	if field.Length() != expected {
		t.Errorf("Expected length %d, got %d", expected, field.Length())
	}
}

func TestUint64Field_Equals(t *testing.T) {
	field1 := NewUint64Field(42)
	field2 := NewUint64Field(42)
	field3 := NewUint64Field(24)
	stringField := NewStringField("test", 10)

	if !field1.Equals(field2) {
		t.Error("Expected equal fields to return true")
	}

	if field1.Equals(field3) {
		t.Error("Expected unequal fields to return false")
	}

	if field1.Equals(stringField) {
		t.Error("Expected different field types to return false")
	}
}

func TestUint64Field_Hash(t *testing.T) {
	field1 := NewUint64Field(42)
	field2 := NewUint64Field(42)

	hash1, err1 := field1.Hash()
	hash2, err2 := field2.Hash()

	if err1 != nil || err2 != nil {
		t.Fatalf("Unexpected errors: %v, %v", err1, err2)
	}

	if hash1 != hash2 {
		t.Errorf("Hash should be consistent for same value: got %d and %d", hash1, hash2)
	}

	field3 := NewUint64Field(100)
	hash3, _ := field3.Hash()

	if hash1 == hash3 {
		t.Error("Hash should be different for different values (42 vs 100)")
	}
}

func TestUint64Field_Serialize(t *testing.T) {
	field := NewUint64Field(42)
	var buf bytes.Buffer

	err := field.Serialize(&buf)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if buf.Len() != 8 {
		t.Errorf("Expected 8 bytes, got %d", buf.Len())
	}
}

func TestUint64Field_Compare(t *testing.T) {
	field1 := NewUint64Field(10)
	field2 := NewUint64Field(20)
	field3 := NewUint64Field(10)
	stringField := NewStringField("test", 10)

	tests := []struct {
		op       primitives.Predicate
		other    Field
		expected bool
	}{
		{primitives.Equals, field3, true},
		{primitives.Equals, field2, false},
		{primitives.LessThan, field2, true},
		{primitives.LessThan, field3, false},
		{primitives.GreaterThan, field2, false},
		{primitives.GreaterThan, NewUint64Field(5), true},
		{primitives.LessThanOrEqual, field2, true},
		{primitives.LessThanOrEqual, field3, true},
		{primitives.LessThanOrEqual, NewUint64Field(5), false},
		{primitives.GreaterThanOrEqual, field3, true},
		{primitives.GreaterThanOrEqual, NewUint64Field(5), true},
		{primitives.GreaterThanOrEqual, field2, false},
		{primitives.NotEqual, field2, true},
		{primitives.NotEqual, field3, false},
		{primitives.Equals, stringField, false},
	}

	for _, test := range tests {
		result, err := field1.Compare(test.op, test.other)
		if err != nil {
			t.Errorf("Unexpected error for %v: %v", test.op, err)
		}

		if result != test.expected {
			t.Errorf("Compare(%v, %v) = %v, expected %v",
				test.op, test.other, result, test.expected)
		}
	}
}

func TestUint64Field_EdgeValues(t *testing.T) {
	tests := []uint64{
		0,
		1,
		18446744073709551615, // max uint64
	}

	for _, value := range tests {
		field := NewUint64Field(value)

		if field.Value != value {
			t.Errorf("Expected value %d, got %d", value, field.Value)
		}

		var buf bytes.Buffer
		err := field.Serialize(&buf)
		if err != nil {
			t.Errorf("Failed to serialize %d: %v", value, err)
		}

		if buf.Len() != 8 {
			t.Errorf("Expected 8 bytes for %d, got %d", value, buf.Len())
		}
	}
}
