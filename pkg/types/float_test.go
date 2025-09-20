package types

import (
	"bytes"
	"math"
	"testing"
)

func TestNewFloat64Field(t *testing.T) {
	tests := []struct {
		name  string
		value float64
	}{
		{"positive value", 42.5},
		{"negative value", -42.5},
		{"zero", 0.0},
		{"very small", 1e-10},
		{"very large", 1e10},
		{"pi", math.Pi},
		{"max float64", math.MaxFloat64},
		{"smallest positive", math.SmallestNonzeroFloat64},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field := NewFloat64Field(tt.value)
			if field.Value != tt.value {
				t.Errorf("Expected value %v, got %v", tt.value, field.Value)
			}
		})
	}
}

func TestFloat64Field_Type(t *testing.T) {
	field := NewFloat64Field(42.5)
	
	if field.Type() != FloatType {
		t.Errorf("Expected type %v, got %v", FloatType, field.Type())
	}
}

func TestFloat64Field_String(t *testing.T) {
	tests := []struct {
		name     string
		value    float64
		expected string
	}{
		{"positive integer", 42.0, "42"},
		{"positive decimal", 42.5, "42.5"},
		{"negative", -42.5, "-42.5"},
		{"zero", 0.0, "0"},
		{"very small", 0.0001, "0.0001"},
		{"scientific notation avoided", 1234567.0, "1234567"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field := NewFloat64Field(tt.value)
			if field.String() != tt.expected {
				t.Errorf("Expected string %s, got %s", tt.expected, field.String())
			}
		})
	}
}

func TestFloat64Field_Length(t *testing.T) {
	field := NewFloat64Field(42.5)
	expected := uint32(8)
	
	if field.Length() != expected {
		t.Errorf("Expected length %d, got %d", expected, field.Length())
	}
}

func TestFloat64Field_Equals(t *testing.T) {
	tests := []struct {
		name     string
		field1   *Float64Field
		field2   Field
		expected bool
	}{
		{"same values", NewFloat64Field(42.5), NewFloat64Field(42.5), true},
		{"different values", NewFloat64Field(42.5), NewFloat64Field(43.5), false},
		{"within epsilon", NewFloat64Field(1.0), NewFloat64Field(1.0 + 1e-10), true},
		{"outside epsilon", NewFloat64Field(1.0), NewFloat64Field(1.0 + 1e-8), false},
		{"negative values", NewFloat64Field(-42.5), NewFloat64Field(-42.5), true},
		{"zero equality", NewFloat64Field(0.0), NewFloat64Field(0.0), true},
		{"different type", NewFloat64Field(42.5), NewIntField(42), false},
		{"NaN values", NewFloat64Field(math.NaN()), NewFloat64Field(math.NaN()), false}, // NaN != NaN
		{"positive infinity", NewFloat64Field(math.Inf(1)), NewFloat64Field(math.Inf(1)), false}, // Inf - Inf = NaN, which is not < epsilon
		{"negative infinity", NewFloat64Field(math.Inf(-1)), NewFloat64Field(math.Inf(-1)), false}, // -Inf - -Inf = NaN, which is not < epsilon
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.field1.Equals(tt.field2)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestFloat64Field_Hash(t *testing.T) {
	tests := []struct {
		name   string
		value  float64
	}{
		{"positive value", 42.5},
		{"negative value", -42.5},
		{"zero", 0.0},
		{"very large", 1e10},
		{"very small", 1e-10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field := NewFloat64Field(tt.value)
			hash, err := field.Hash()
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			
			// Verify same values produce same hash
			field2 := NewFloat64Field(tt.value)
			hash2, err := field2.Hash()
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			
			if hash != hash2 {
				t.Errorf("Same values should produce same hash: %d != %d", hash, hash2)
			}
		})
	}
	
	// Test that different values produce different hashes (most of the time)
	field1 := NewFloat64Field(42.5)
	field2 := NewFloat64Field(43.5)
	
	hash1, _ := field1.Hash()
	hash2, _ := field2.Hash()
	
	if hash1 == hash2 {
		t.Errorf("Different values should generally produce different hashes")
	}
}

func TestFloat64Field_Serialize(t *testing.T) {
	tests := []struct {
		name  string
		value float64
	}{
		{"positive value", 42.5},
		{"negative value", -42.5},
		{"zero", 0.0},
		{"max float64", math.MaxFloat64},
		{"min float64", -math.MaxFloat64},
		{"positive infinity", math.Inf(1)},
		{"negative infinity", math.Inf(-1)},
		{"NaN", math.NaN()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field := NewFloat64Field(tt.value)
			var buf bytes.Buffer
			
			err := field.Serialize(&buf)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			
			if buf.Len() != 8 {
				t.Errorf("Expected buffer length 8, got %d", buf.Len())
			}
		})
	}
}

func TestFloat64Field_Compare(t *testing.T) {
	tests := []struct {
		name      string
		field1    *Float64Field
		op        Predicate
		field2    *Float64Field
		expected  bool
		expectErr bool
	}{
		// Equals tests
		{"equals same", NewFloat64Field(42.5), Equals, NewFloat64Field(42.5), true, false},
		{"equals different", NewFloat64Field(42.5), Equals, NewFloat64Field(43.5), false, false},
		{"equals within epsilon", NewFloat64Field(1.0), Equals, NewFloat64Field(1.0 + 1e-10), true, false},
		{"equals outside epsilon", NewFloat64Field(1.0), Equals, NewFloat64Field(1.0 + 1e-8), false, false},
		
		// NotEqual tests
		{"not equal different", NewFloat64Field(42.5), NotEqual, NewFloat64Field(43.5), true, false},
		{"not equal same", NewFloat64Field(42.5), NotEqual, NewFloat64Field(42.5), false, false},
		{"not equal within epsilon", NewFloat64Field(1.0), NotEqual, NewFloat64Field(1.0 + 1e-10), false, false},
		{"not equal outside epsilon", NewFloat64Field(1.0), NotEqual, NewFloat64Field(1.0 + 1e-8), true, false},
		
		// LessThan tests
		{"less than true", NewFloat64Field(42.5), LessThan, NewFloat64Field(43.5), true, false},
		{"less than false", NewFloat64Field(43.5), LessThan, NewFloat64Field(42.5), false, false},
		{"less than equal", NewFloat64Field(42.5), LessThan, NewFloat64Field(42.5), false, false},
		{"negative less than", NewFloat64Field(-43.5), LessThan, NewFloat64Field(-42.5), true, false},
		
		// GreaterThan tests
		{"greater than true", NewFloat64Field(43.5), GreaterThan, NewFloat64Field(42.5), true, false},
		{"greater than false", NewFloat64Field(42.5), GreaterThan, NewFloat64Field(43.5), false, false},
		{"greater than equal", NewFloat64Field(42.5), GreaterThan, NewFloat64Field(42.5), false, false},
		
		// LessThanOrEqual tests
		{"less than or equal less", NewFloat64Field(42.5), LessThanOrEqual, NewFloat64Field(43.5), true, false},
		{"less than or equal equal", NewFloat64Field(42.5), LessThanOrEqual, NewFloat64Field(42.5), true, false},
		{"less than or equal greater", NewFloat64Field(43.5), LessThanOrEqual, NewFloat64Field(42.5), false, false},
		
		// GreaterThanOrEqual tests
		{"greater than or equal greater", NewFloat64Field(43.5), GreaterThanOrEqual, NewFloat64Field(42.5), true, false},
		{"greater than or equal equal", NewFloat64Field(42.5), GreaterThanOrEqual, NewFloat64Field(42.5), true, false},
		{"greater than or equal less", NewFloat64Field(42.5), GreaterThanOrEqual, NewFloat64Field(43.5), false, false},
		
		// Special values tests
		{"infinity greater than max", NewFloat64Field(math.Inf(1)), GreaterThan, NewFloat64Field(math.MaxFloat64), true, false},
		{"negative infinity less than min", NewFloat64Field(math.Inf(-1)), LessThan, NewFloat64Field(-math.MaxFloat64), true, false},
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

func TestFloat64Field_Compare_CrossType(t *testing.T) {
	tests := []struct {
		name      string
		floatVal  float64
		op        Predicate
		intVal    int32
		expected  bool
	}{
		{"float equals int", 42.0, Equals, 42, true},
		{"float not equals int decimal", 42.5, Equals, 42, false},
		{"float less than int", 41.9, LessThan, 42, true},
		{"float greater than int", 42.1, GreaterThan, 42, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			floatField := NewFloat64Field(tt.floatVal)
			intField := NewIntField(tt.intVal)
			
			result, err := floatField.Compare(tt.op, intField)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected result %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestFloat64Field_Compare_InvalidType(t *testing.T) {
	floatField := NewFloat64Field(42.5)
	stringField := NewStringField("test", 10)
	
	_, err := floatField.Compare(Equals, stringField)
	if err == nil {
		t.Error("Expected error when comparing with string field")
	}
}

func TestFloat64Field_Compare_UnsupportedPredicate(t *testing.T) {
	field1 := NewFloat64Field(42.5)
	field2 := NewFloat64Field(43.5)
	
	// Using an invalid predicate value
	_, err := field1.Compare(Predicate(999), field2)
	if err == nil {
		t.Error("Expected error for unsupported predicate")
	}
}

func TestFloat64Field_Compare_NaN(t *testing.T) {
	tests := []struct {
		name     string
		field1   *Float64Field
		op       Predicate
		field2   *Float64Field
		expected bool
	}{
		{"NaN equals NaN", NewFloat64Field(math.NaN()), Equals, NewFloat64Field(math.NaN()), false},
		{"NaN not equals NaN", NewFloat64Field(math.NaN()), NotEqual, NewFloat64Field(math.NaN()), false}, // NaN - NaN = NaN, and NaN is not >= epsilon
		{"NaN less than value", NewFloat64Field(math.NaN()), LessThan, NewFloat64Field(42.5), false},
		{"value less than NaN", NewFloat64Field(42.5), LessThan, NewFloat64Field(math.NaN()), false},
		{"NaN greater than value", NewFloat64Field(math.NaN()), GreaterThan, NewFloat64Field(42.5), false},
		{"value greater than NaN", NewFloat64Field(42.5), GreaterThan, NewFloat64Field(math.NaN()), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.field1.Compare(tt.op, tt.field2)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected result %v, got %v", tt.expected, result)
			}
		})
	}
}