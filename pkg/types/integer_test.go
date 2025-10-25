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
	if hash != uint32(3990555855) {
		t.Errorf("Expected hash %d, got %d", uint32(3990555855), hash)
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
