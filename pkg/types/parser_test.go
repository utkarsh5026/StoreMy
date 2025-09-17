package types

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestParseField_IntField(t *testing.T) {
	original := NewIntField(42)

	var buf bytes.Buffer
	err := original.Serialize(&buf)
	if err != nil {
		t.Fatalf("Failed to serialize: %v", err)
	}

	parsed, err := ParseField(&buf, IntType)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	parsedInt, ok := parsed.(*IntField)
	if !ok {
		t.Fatalf("Expected *IntField, got %T", parsed)
	}

	if parsedInt.Value != original.Value {
		t.Errorf("Expected value %d, got %d", original.Value, parsedInt.Value)
	}
}

func TestParseField_StringField(t *testing.T) {
	original := NewStringField("hello", StringMaxSize)

	var buf bytes.Buffer
	err := original.Serialize(&buf)
	if err != nil {
		t.Fatalf("Failed to serialize: %v", err)
	}

	parsed, err := ParseField(&buf, StringType)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	parsedString, ok := parsed.(*StringField)
	if !ok {
		t.Fatalf("Expected *StringField, got %T", parsed)
	}

	if parsedString.Value != original.Value {
		t.Errorf("Expected value %s, got %s", original.Value, parsedString.Value)
	}

	if parsedString.MaxSize != original.MaxSize {
		t.Errorf("Expected maxSize %d, got %d", original.MaxSize, parsedString.MaxSize)
	}
}

func TestParseField_RoundTrip_IntField(t *testing.T) {
	tests := []int32{0, 42, -42, 2147483647, -2147483648}

	for _, value := range tests {
		original := NewIntField(value)

		var buf bytes.Buffer
		err := original.Serialize(&buf)
		if err != nil {
			t.Fatalf("Failed to serialize %d: %v", value, err)
		}

		parsed, err := ParseField(&buf, IntType)
		if err != nil {
			t.Fatalf("Failed to parse %d: %v", value, err)
		}

		if !original.Equals(parsed) {
			t.Errorf("Round trip failed for %d", value)
		}
	}
}

func TestParseField_RoundTrip_StringField(t *testing.T) {
	tests := []string{
		"",
		"hello",
		"world",
		"a very long string that should be truncated",
		"special chars: !@#$%^&*()",
	}

	for _, value := range tests {
		original := NewStringField(value, StringMaxSize)

		var buf bytes.Buffer
		err := original.Serialize(&buf)
		if err != nil {
			t.Fatalf("Failed to serialize %s: %v", value, err)
		}

		parsed, err := ParseField(&buf, StringType)
		if err != nil {
			t.Fatalf("Failed to parse %s: %v", value, err)
		}

		if !original.Equals(parsed) {
			t.Errorf("Round trip failed for %s", value)
		}
	}
}

func TestParseField_UnsupportedType(t *testing.T) {
	var buf bytes.Buffer
	buf.Write([]byte{1, 2, 3, 4})

	invalidType := Type(999)
	_, err := ParseField(&buf, invalidType)

	if err == nil {
		t.Error("Expected error for unsupported type")
	}

	if !strings.Contains(err.Error(), "invalid field type size") && !strings.Contains(err.Error(), "unsupported field type") {
		t.Errorf("Expected type error, got: %v", err)
	}
}

func TestParseField_InvalidTypeSize(t *testing.T) {
	var buf bytes.Buffer
	buf.Write([]byte{1, 2, 3, 4})

	invalidType := Type(999)
	_, err := ParseField(&buf, invalidType)

	if err == nil {
		t.Error("Expected error for invalid type size")
	}
}

func TestParseField_InsufficientData_IntField(t *testing.T) {
	var buf bytes.Buffer
	buf.Write([]byte{1, 2})

	_, err := ParseField(&buf, IntType)

	if err == nil {
		t.Error("Expected error for insufficient data")
	}
}

func TestParseField_InsufficientData_StringField(t *testing.T) {
	var buf bytes.Buffer
	buf.Write([]byte{0, 0, 0, 5})

	_, err := ParseField(&buf, StringType)

	if err == nil {
		t.Error("Expected error for insufficient string data")
	}

	if err != io.EOF && err != io.ErrUnexpectedEOF {
		t.Errorf("Expected EOF error, got: %v", err)
	}
}

func TestParseField_EmptyReader_IntField(t *testing.T) {
	var buf bytes.Buffer

	_, err := ParseField(&buf, IntType)

	if err == nil {
		t.Error("Expected error for empty reader")
	}

	if err != io.EOF {
		t.Errorf("Expected EOF error, got: %v", err)
	}
}

func TestParseField_EmptyReader_StringField(t *testing.T) {
	var buf bytes.Buffer

	_, err := ParseField(&buf, StringType)

	if err == nil {
		t.Error("Expected error for empty reader")
	}

	if err != io.EOF {
		t.Errorf("Expected EOF error, got: %v", err)
	}
}

func TestParseIntField_MaxValues(t *testing.T) {
	tests := []int32{2147483647, -2147483648}

	for _, value := range tests {
		original := NewIntField(value)

		var buf bytes.Buffer
		err := original.Serialize(&buf)
		if err != nil {
			t.Fatalf("Failed to serialize %d: %v", value, err)
		}

		parsed, err := parseIntField(&buf, 4)
		if err != nil {
			t.Fatalf("Failed to parse %d: %v", value, err)
		}

		if parsed.Value != value {
			t.Errorf("Expected %d, got %d", value, parsed.Value)
		}
	}
}

func TestParseStringField_LengthHandling(t *testing.T) {
	original := NewStringField("hello world", StringMaxSize)

	var buf bytes.Buffer
	err := original.Serialize(&buf)
	if err != nil {
		t.Fatalf("Failed to serialize: %v", err)
	}

	parsed, err := parseStringField(&buf)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if parsed.Value != "hello world" {
		t.Errorf("Expected 'hello world', got '%s'", parsed.Value)
	}
}
