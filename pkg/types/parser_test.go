package types

import (
	"bytes"
	bytes2 "bytes"
	"encoding/binary"
	"io"
	"math"
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
	tests := []int64{0, 42, -42, 2147483647, -2147483648}

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

func TestParseField_BoolField(t *testing.T) {
	tests := []struct {
		name  string
		value bool
	}{
		{"true value", true},
		{"false value", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := NewBoolField(tt.value)

			var buf bytes.Buffer
			err := original.Serialize(&buf)
			if err != nil {
				t.Fatalf("Failed to serialize: %v", err)
			}

			parsed, err := ParseField(&buf, BoolType)
			if err != nil {
				t.Fatalf("Failed to parse: %v", err)
			}

			parsedBool, ok := parsed.(*BoolField)
			if !ok {
				t.Fatalf("Expected *BoolField, got %T", parsed)
			}

			if parsedBool.Value != original.Value {
				t.Errorf("Expected value %v, got %v", original.Value, parsedBool.Value)
			}
		})
	}
}

func TestParseField_RoundTrip_BoolField(t *testing.T) {
	tests := []bool{true, false}

	for _, value := range tests {
		original := NewBoolField(value)

		var buf bytes.Buffer
		err := original.Serialize(&buf)
		if err != nil {
			t.Fatalf("Failed to serialize %v: %v", value, err)
		}

		parsed, err := ParseField(&buf, BoolType)
		if err != nil {
			t.Fatalf("Failed to parse %v: %v", value, err)
		}

		if !original.Equals(parsed) {
			t.Errorf("Round trip failed for %v", value)
		}
	}
}

func TestParseBoolField_DirectFunction(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected bool
	}{
		{"zero byte as false", []byte{0}, false},
		{"one byte as true", []byte{1}, true},
		{"any non-zero byte as true", []byte{42}, true},
		{"255 byte as true", []byte{255}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(tt.data)
			field, err := parseBoolField(buf)
			if err != nil {
				t.Fatalf("Failed to parse: %v", err)
			}

			if field.Value != tt.expected {
				t.Errorf("Expected value %v, got %v", tt.expected, field.Value)
			}
		})
	}
}

func TestParseField_InsufficientData_BoolField(t *testing.T) {
	var buf bytes.Buffer

	_, err := ParseField(&buf, BoolType)

	if err == nil {
		t.Error("Expected error for insufficient data")
	}

	if err != io.EOF {
		t.Errorf("Expected EOF error, got: %v", err)
	}
}

func TestParseField_EmptyReader_BoolField(t *testing.T) {
	var buf bytes.Buffer

	_, err := parseBoolField(&buf)

	if err == nil {
		t.Error("Expected error for empty reader")
	}

	if err != io.EOF {
		t.Errorf("Expected EOF error, got: %v", err)
	}
}

func TestParseField_Float64Field(t *testing.T) {
	tests := []struct {
		name  string
		value float64
	}{
		{"positive value", 42.5},
		{"negative value", -42.5},
		{"zero", 0.0},
		{"very small", 1e-10},
		{"very large", 1e10},
		{"max float64", math.MaxFloat64},
		{"min float64", -math.MaxFloat64},
		{"positive infinity", math.Inf(1)},
		{"negative infinity", math.Inf(-1)},
		{"NaN", math.NaN()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := NewFloat64Field(tt.value)

			var buf bytes.Buffer
			err := original.Serialize(&buf)
			if err != nil {
				t.Fatalf("Failed to serialize: %v", err)
			}

			parsed, err := ParseField(&buf, FloatType)
			if err != nil {
				t.Fatalf("Failed to parse: %v", err)
			}

			parsedFloat, ok := parsed.(*Float64Field)
			if !ok {
				t.Fatalf("Expected *Float64Field, got %T", parsed)
			}

			// Special handling for NaN since NaN != NaN
			if math.IsNaN(tt.value) {
				if !math.IsNaN(parsedFloat.Value) {
					t.Errorf("Expected NaN, got %v", parsedFloat.Value)
				}
			} else if parsedFloat.Value != original.Value {
				t.Errorf("Expected value %v, got %v", original.Value, parsedFloat.Value)
			}
		})
	}
}

func TestParseField_RoundTrip_Float64Field(t *testing.T) {
	tests := []float64{0.0, 42.5, -42.5, 1e-10, 1e10, math.Pi, math.E}

	for _, value := range tests {
		original := NewFloat64Field(value)

		var buf bytes.Buffer
		err := original.Serialize(&buf)
		if err != nil {
			t.Fatalf("Failed to serialize %v: %v", value, err)
		}

		parsed, err := ParseField(&buf, FloatType)
		if err != nil {
			t.Fatalf("Failed to parse %v: %v", value, err)
		}

		if !original.Equals(parsed) {
			t.Errorf("Round trip failed for %v", value)
		}
	}
}

func TestParseFloat64Field_DirectFunction(t *testing.T) {
	tests := []struct {
		name  string
		value float64
	}{
		{"positive value", 42.5},
		{"negative value", -42.5},
		{"zero", 0.0},
		{"max float64", math.MaxFloat64},
		{"min float64", -math.MaxFloat64},
		{"smallest positive", math.SmallestNonzeroFloat64},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a buffer with the serialized float64
			bits := math.Float64bits(tt.value)
			bytes := make([]byte, 8)
			binary.BigEndian.PutUint64(bytes, bits)
			buf := bytes2.NewBuffer(bytes)

			field, err := parseFloat64Field(buf, 8)
			if err != nil {
				t.Fatalf("Failed to parse: %v", err)
			}

			if field.Value != tt.value {
				t.Errorf("Expected value %v, got %v", tt.value, field.Value)
			}
		})
	}
}

func TestParseField_InsufficientData_Float64Field(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"empty buffer", []byte{}},
		{"partial data 1 byte", []byte{1}},
		{"partial data 4 bytes", []byte{1, 2, 3, 4}},
		{"partial data 7 bytes", []byte{1, 2, 3, 4, 5, 6, 7}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes2.NewBuffer(tt.data)

			_, err := ParseField(buf, FloatType)

			if err == nil {
				t.Error("Expected error for insufficient data")
			}

			if err != io.EOF && err != io.ErrUnexpectedEOF {
				t.Errorf("Expected EOF error, got: %v", err)
			}
		})
	}
}

func TestParseField_EmptyReader_Float64Field(t *testing.T) {
	var buf bytes.Buffer

	_, err := parseFloat64Field(&buf, 8)

	if err == nil {
		t.Error("Expected error for empty reader")
	}

	if err != io.EOF {
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
	tests := []int64{2147483647, -2147483648, 9223372036854775807, -9223372036854775808}

	for _, value := range tests {
		original := NewIntField(value)

		var buf bytes.Buffer
		err := original.Serialize(&buf)
		if err != nil {
			t.Fatalf("Failed to serialize %d: %v", value, err)
		}

		parsed, err := parseIntField(&buf, 8)
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
