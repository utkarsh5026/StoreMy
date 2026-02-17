package types

import (
	"bytes"
	"storemy/pkg/primitives"
	"testing"
)

func TestNewBoolField(t *testing.T) {
	tests := []struct {
		name  string
		value bool
	}{
		{"true value", true},
		{"false value", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field := NewBoolField(tt.value)
			if field.Value != tt.value {
				t.Errorf("Expected value %v, got %v", tt.value, field.Value)
			}
		})
	}
}

func TestBoolField_Type(t *testing.T) {
	field := NewBoolField(true)

	if field.Type() != BoolType {
		t.Errorf("Expected type %v, got %v", BoolType, field.Type())
	}
}

func TestBoolField_String(t *testing.T) {
	tests := []struct {
		name     string
		value    bool
		expected string
	}{
		{"true value", true, "true"},
		{"false value", false, "false"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field := NewBoolField(tt.value)
			if field.String() != tt.expected {
				t.Errorf("Expected string %s, got %s", tt.expected, field.String())
			}
		})
	}
}

func TestBoolField_Length(t *testing.T) {
	field := NewBoolField(true)
	expected := uint32(1)

	if field.Length() != expected {
		t.Errorf("Expected length %d, got %d", expected, field.Length())
	}
}

func TestBoolField_Equals(t *testing.T) {
	fieldTrue1 := NewBoolField(true)
	fieldTrue2 := NewBoolField(true)
	fieldFalse := NewBoolField(false)
	intField := NewIntField(42)

	if !fieldTrue1.Equals(fieldTrue2) {
		t.Error("Expected equal fields to return true")
	}

	if fieldTrue1.Equals(fieldFalse) {
		t.Error("Expected different fields to return false")
	}

	if fieldTrue1.Equals(intField) {
		t.Error("Expected comparison with different type to return false")
	}
}

func TestBoolField_Hash(t *testing.T) {
	tests := []struct {
		name     string
		value    bool
		expected primitives.HashCode
	}{
		{"true value", true, primitives.HashCode(67918732)},   // FNV-1a hash of byte{1}
		{"false value", false, primitives.HashCode(84696351)}, // FNV-1a hash of byte{0}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field := NewBoolField(tt.value)
			hash, err := field.Hash()
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if hash != tt.expected {
				t.Errorf("Expected hash %d, got %d", tt.expected, hash)
			}
		})
	}
}

func TestBoolField_Hash_Consistency(t *testing.T) {
	// Test that hashing the same value produces consistent results
	field1 := NewBoolField(true)
	field2 := NewBoolField(true)

	hash1, _ := field1.Hash()
	hash2, _ := field2.Hash()

	if hash1 != hash2 {
		t.Errorf("Hash should be consistent for same value: got %d and %d", hash1, hash2)
	}

	// Test that different values produce different hashes
	field3 := NewBoolField(false)
	hash3, _ := field3.Hash()

	if hash1 == hash3 {
		t.Error("Hash should be different for true and false")
	}
}

func TestBoolField_Serialize(t *testing.T) {
	tests := []struct {
		name     string
		value    bool
		expected byte
	}{
		{"true value", true, 1},
		{"false value", false, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field := NewBoolField(tt.value)
			var buf bytes.Buffer

			err := field.Serialize(&buf)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if buf.Len() != 1 {
				t.Errorf("Expected buffer length 1, got %d", buf.Len())
			}

			if buf.Bytes()[0] != tt.expected {
				t.Errorf("Expected byte %d, got %d", tt.expected, buf.Bytes()[0])
			}
		})
	}
}

func TestBoolField_Compare(t *testing.T) {
	tests := []struct {
		name      string
		field1    *BoolField
		op        primitives.Predicate
		field2    *BoolField
		expected  bool
		expectErr bool
	}{
		// Equals tests
		{"true equals true", NewBoolField(true), primitives.Equals, NewBoolField(true), true, false},
		{"true equals false", NewBoolField(true), primitives.Equals, NewBoolField(false), false, false},
		{"false equals false", NewBoolField(false), primitives.Equals, NewBoolField(false), true, false},

		// NotEqual tests
		{"true not equal false", NewBoolField(true), primitives.NotEqual, NewBoolField(false), true, false},
		{"true not equal true", NewBoolField(true), primitives.NotEqual, NewBoolField(true), false, false},

		// LessThan tests (false < true)
		{"false less than true", NewBoolField(false), primitives.LessThan, NewBoolField(true), true, false},
		{"true less than false", NewBoolField(true), primitives.LessThan, NewBoolField(false), false, false},
		{"true less than true", NewBoolField(true), primitives.LessThan, NewBoolField(true), false, false},
		{"false less than false", NewBoolField(false), primitives.LessThan, NewBoolField(false), false, false},

		// GreaterThan tests (true > false)
		{"true greater than false", NewBoolField(true), primitives.GreaterThan, NewBoolField(false), true, false},
		{"false greater than true", NewBoolField(false), primitives.GreaterThan, NewBoolField(true), false, false},
		{"true greater than true", NewBoolField(true), primitives.GreaterThan, NewBoolField(true), false, false},
		{"false greater than false", NewBoolField(false), primitives.GreaterThan, NewBoolField(false), false, false},

		// LessThanOrEqual tests
		{"false less than or equal true", NewBoolField(false), primitives.LessThanOrEqual, NewBoolField(true), true, false},
		{"true less than or equal false", NewBoolField(true), primitives.LessThanOrEqual, NewBoolField(false), false, false},
		{"true less than or equal true", NewBoolField(true), primitives.LessThanOrEqual, NewBoolField(true), true, false},
		{"false less than or equal false", NewBoolField(false), primitives.LessThanOrEqual, NewBoolField(false), true, false},

		// GreaterThanOrEqual tests
		{"true greater than or equal false", NewBoolField(true), primitives.GreaterThanOrEqual, NewBoolField(false), true, false},
		{"false greater than or equal true", NewBoolField(false), primitives.GreaterThanOrEqual, NewBoolField(true), false, false},
		{"true greater than or equal true", NewBoolField(true), primitives.GreaterThanOrEqual, NewBoolField(true), true, false},
		{"false greater than or equal false", NewBoolField(false), primitives.GreaterThanOrEqual, NewBoolField(false), true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.field1.Compare(tt.op, tt.field2)
			if tt.expectErr && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !tt.expectErr && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected result %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestBoolField_Compare_InvalidType(t *testing.T) {
	boolField := NewBoolField(true)
	intField := NewIntField(42)

	_, err := boolField.Compare(primitives.Equals, intField)
	if err == nil {
		t.Error("Expected error when comparing with different field type")
	}
}

func TestBoolField_Compare_UnsupportedPredicate(t *testing.T) {
	field1 := NewBoolField(true)
	field2 := NewBoolField(false)

	// Using an invalid predicate value
	_, err := field1.Compare(primitives.Predicate(999), field2)
	if err == nil {
		t.Error("Expected error for unsupported predicate")
	}
}
