package types

import (
	"bytes"
	"testing"
)

func TestNewIntField(t *testing.T) {
	value := int32(42)
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
	expected := uint32(4)
	
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
	
	if hash != uint32(42) {
		t.Errorf("Expected hash %d, got %d", uint32(42), hash)
	}
}

func TestIntField_Serialize(t *testing.T) {
	field := NewIntField(42)
	var buf bytes.Buffer
	
	err := field.Serialize(&buf)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	
	if buf.Len() != 4 {
		t.Errorf("Expected 4 bytes, got %d", buf.Len())
	}
}

func TestIntField_Compare(t *testing.T) {
	field1 := NewIntField(10)
	field2 := NewIntField(20)
	field3 := NewIntField(10)
	stringField := NewStringField("test", 10)
	
	tests := []struct {
		op       Predicate
		other    Field
		expected bool
	}{
		{Equals, field3, true},
		{Equals, field2, false},
		{LessThan, field2, true},
		{LessThan, field3, false},
		{GreaterThan, field2, false},
		{GreaterThan, NewIntField(5), true},
		{LessThanOrEqual, field2, true},
		{LessThanOrEqual, field3, true},
		{LessThanOrEqual, NewIntField(5), false},
		{GreaterThanOrEqual, field3, true},
		{GreaterThanOrEqual, NewIntField(5), true},
		{GreaterThanOrEqual, field2, false},
		{NotEqual, field2, true},
		{NotEqual, field3, false},
		{Equals, stringField, false},
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