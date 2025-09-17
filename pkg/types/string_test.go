package types

import (
	"bytes"
	"testing"
)

func TestNewStringField(t *testing.T) {
	value := "hello"
	maxSize := 10
	field := NewStringField(value, maxSize)
	
	if field.Value != value {
		t.Errorf("Expected value %s, got %s", value, field.Value)
	}
	
	if field.MaxSize != maxSize {
		t.Errorf("Expected maxSize %d, got %d", maxSize, field.MaxSize)
	}
}

func TestNewStringField_Truncation(t *testing.T) {
	value := "this is a very long string"
	maxSize := 10
	field := NewStringField(value, maxSize)
	
	expected := value[:maxSize]
	if field.Value != expected {
		t.Errorf("Expected truncated value %s, got %s", expected, field.Value)
	}
}

func TestStringField_Type(t *testing.T) {
	field := NewStringField("test", 10)
	
	if field.Type() != StringType {
		t.Errorf("Expected type %v, got %v", StringType, field.Type())
	}
}

func TestStringField_String(t *testing.T) {
	value := "hello"
	field := NewStringField(value, 10)
	
	if field.String() != value {
		t.Errorf("Expected string %s, got %s", value, field.String())
	}
}

func TestStringField_Length(t *testing.T) {
	field := NewStringField("test", 10)
	expected := uint32(4 + StringMaxSize)
	
	if field.Length() != expected {
		t.Errorf("Expected length %d, got %d", expected, field.Length())
	}
}

func TestStringField_Equals(t *testing.T) {
	field1 := NewStringField("hello", 10)
	field2 := NewStringField("hello", 10)
	field3 := NewStringField("world", 10)
	field4 := NewStringField("hello", 20)
	intField := NewIntField(42)
	
	if !field1.Equals(field2) {
		t.Error("Expected equal fields to return true")
	}
	
	if field1.Equals(field3) {
		t.Error("Expected fields with different values to return false")
	}
	
	if field1.Equals(field4) {
		t.Error("Expected fields with different max sizes to return false")
	}
	
	if field1.Equals(intField) {
		t.Error("Expected different field types to return false")
	}
}

func TestStringField_Hash(t *testing.T) {
	field := NewStringField("test", 10)
	hash, err := field.Hash()
	
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	
	if hash == 0 {
		t.Error("Expected non-zero hash")
	}
	
	field2 := NewStringField("test", 10)
	hash2, err := field2.Hash()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	
	if hash != hash2 {
		t.Error("Expected same hash for same string values")
	}
}

func TestStringField_Serialize(t *testing.T) {
	field := NewStringField("test", 10)
	var buf bytes.Buffer
	
	err := field.Serialize(&buf)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	
	expectedLength := 4 + field.MaxSize
	if buf.Len() != expectedLength {
		t.Errorf("Expected %d bytes, got %d", expectedLength, buf.Len())
	}
}

func TestStringField_Compare(t *testing.T) {
	field1 := NewStringField("apple", 10)
	field2 := NewStringField("banana", 10)
	field3 := NewStringField("apple", 10)
	intField := NewIntField(42)
	
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
		{GreaterThan, NewStringField("aaa", 10), true},
		{LessThanOrEqual, field2, true},
		{LessThanOrEqual, field3, true},
		{LessThanOrEqual, NewStringField("aaa", 10), false},
		{GreaterThanOrEqual, field3, true},
		{GreaterThanOrEqual, NewStringField("aaa", 10), true},
		{GreaterThanOrEqual, field2, false},
		{NotEqual, field2, true},
		{NotEqual, field3, false},
		{Like, NewStringField("app", 10), true},
		{Like, field2, false},
		{Equals, intField, false},
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