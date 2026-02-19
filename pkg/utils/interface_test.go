package utils

import (
	"testing"
)

// mockInterface is a test interface
type mockInterface interface {
	DoSomething()
}

// mockStruct implements mockInterface
type mockStruct struct{}

func (m *mockStruct) DoSomething() {}

func TestIsNilInterface(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		expected bool
	}{
		{
			name:     "plain nil",
			value:    nil,
			expected: true,
		},
		{
			name:     "nil pointer wrapped in interface",
			value:    mockInterface((*mockStruct)(nil)),
			expected: true,
		},
		{
			name:     "nil pointer (concrete type as any)",
			value:    (*mockStruct)(nil),
			expected: true,
		},
		{
			name:     "valid pointer in interface",
			value:    mockInterface(&mockStruct{}),
			expected: false,
		},
		{
			name:     "valid pointer (concrete type as any)",
			value:    &mockStruct{},
			expected: false,
		},
		{
			name:     "nil slice",
			value:    ([]int)(nil),
			expected: true,
		},
		{
			name:     "empty slice",
			value:    []int{},
			expected: false,
		},
		{
			name:     "nil map",
			value:    (map[string]int)(nil),
			expected: true,
		},
		{
			name:     "empty map",
			value:    map[string]int{},
			expected: false,
		},
		{
			name:     "nil channel",
			value:    (chan int)(nil),
			expected: true,
		},
		{
			name:     "value type (int)",
			value:    42,
			expected: false,
		},
		{
			name:     "value type (string)",
			value:    "test",
			expected: false,
		},
		{
			name:     "value type (struct)",
			value:    mockStruct{},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsNilInterface(tt.value)
			if result != tt.expected {
				t.Errorf("IsNilInterface() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

// TestIsNilInterface_RealWorldScenario tests the exact bug scenario
func TestIsNilInterface_RealWorldScenario(t *testing.T) {
	// Simulate the bug: nil concrete pointer passed to interface parameter
	var nilPtr *mockStruct = nil
	var iface mockInterface = nilPtr

	// Direct comparison fails (the bug)
	if iface == nil {
		t.Error("iface == nil should be false (this is the bug!)")
	}

	// Helper function succeeds
	if !IsNilInterface(iface) {
		t.Error("IsNilInterface should return true for nil-wrapped interface")
	}

	// Valid pointer case
	validPtr := &mockStruct{}
	var validIface mockInterface = validPtr

	if validIface == nil {
		t.Error("validIface == nil should be false")
	}

	if IsNilInterface(validIface) {
		t.Error("IsNilInterface should return false for valid interface")
	}
}
