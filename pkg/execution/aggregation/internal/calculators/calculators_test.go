package calculators

import (
	"math"
	"storemy/pkg/execution/aggregation/internal/core"
	"storemy/pkg/types"
	"testing"
)

// ====================================
// FloatCalculator Tests
// ====================================

func TestFloatCalculator_NewFloatCalculator(t *testing.T) {
	calc := NewFloatCalculator(core.Sum)
	if calc == nil {
		t.Fatal("Expected non-nil FloatCalculator")
	}
	if calc.op != core.Sum {
		t.Errorf("Expected operation Sum, got %v", calc.op)
	}
	if calc.groupToAgg == nil {
		t.Error("Expected non-nil groupToAgg map")
	}
	if calc.groupToCount == nil {
		t.Error("Expected non-nil groupToCount map")
	}
}

func TestFloatCalculator_ValidateOperation(t *testing.T) {
	calc := NewFloatCalculator(core.Sum)

	tests := []struct {
		name      string
		op        core.AggregateOp
		expectErr bool
	}{
		{"MIN is valid", core.Min, false},
		{"MAX is valid", core.Max, false},
		{"SUM is valid", core.Sum, false},
		{"AVG is valid", core.Avg, false},
		{"COUNT is valid", core.Count, false},
		{"AND is invalid", core.And, true},
		{"OR is invalid", core.Or, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := calc.ValidateOperation(tt.op)
			if tt.expectErr && err == nil {
				t.Errorf("Expected error for operation %v", tt.op)
			}
			if !tt.expectErr && err != nil {
				t.Errorf("Unexpected error for operation %v: %v", tt.op, err)
			}
		})
	}
}

func TestFloatCalculator_GetResultType(t *testing.T) {
	calc := NewFloatCalculator(core.Sum)

	tests := []struct {
		name         string
		op           core.AggregateOp
		expectedType types.Type
	}{
		{"COUNT returns IntType", core.Count, types.IntType},
		{"SUM returns FloatType", core.Sum, types.FloatType},
		{"AVG returns FloatType", core.Avg, types.FloatType},
		{"MIN returns FloatType", core.Min, types.FloatType},
		{"MAX returns FloatType", core.Max, types.FloatType},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resultType := calc.GetResultType(tt.op)
			if resultType != tt.expectedType {
				t.Errorf("Expected type %v, got %v", tt.expectedType, resultType)
			}
		})
	}
}

func TestFloatCalculator_InitializeGroup(t *testing.T) {
	tests := []struct {
		name          string
		op            core.AggregateOp
		expectedValue float64
	}{
		{"MIN initializes to +Inf", core.Min, math.Inf(1)},
		{"MAX initializes to -Inf", core.Max, math.Inf(-1)},
		{"SUM initializes to 0", core.Sum, 0.0},
		{"AVG initializes to 0", core.Avg, 0.0},
		{"COUNT initializes to 0", core.Count, 0.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calc := NewFloatCalculator(tt.op)
			calc.InitializeGroup("testGroup")

			value, exists := calc.groupToAgg["testGroup"]
			if !exists {
				t.Fatal("Group was not initialized")
			}
			if value != tt.expectedValue {
				t.Errorf("Expected initial value %v, got %v", tt.expectedValue, value)
			}

			if tt.op == core.Avg {
				count, exists := calc.groupToCount["testGroup"]
				if !exists {
					t.Error("Count was not initialized for AVG operation")
				}
				if count != 0 {
					t.Errorf("Expected initial count 0, got %d", count)
				}
			}
		})
	}
}

func TestFloatCalculator_UpdateAggregate_MIN(t *testing.T) {
	calc := NewFloatCalculator(core.Min)
	calc.InitializeGroup("group1")

	values := []float64{5.5, 3.2, 8.7, 1.1, 9.9}
	for _, val := range values {
		err := calc.UpdateAggregate("group1", types.NewFloat64Field(val))
		if err != nil {
			t.Fatalf("Failed to update aggregate: %v", err)
		}
	}

	result := calc.groupToAgg["group1"]
	expected := 1.1
	if result != expected {
		t.Errorf("Expected MIN %v, got %v", expected, result)
	}
}

func TestFloatCalculator_UpdateAggregate_MAX(t *testing.T) {
	calc := NewFloatCalculator(core.Max)
	calc.InitializeGroup("group1")

	values := []float64{5.5, 3.2, 8.7, 1.1, 9.9}
	for _, val := range values {
		err := calc.UpdateAggregate("group1", types.NewFloat64Field(val))
		if err != nil {
			t.Fatalf("Failed to update aggregate: %v", err)
		}
	}

	result := calc.groupToAgg["group1"]
	expected := 9.9
	if result != expected {
		t.Errorf("Expected MAX %v, got %v", expected, result)
	}
}

func TestFloatCalculator_UpdateAggregate_SUM(t *testing.T) {
	calc := NewFloatCalculator(core.Sum)
	calc.InitializeGroup("group1")

	values := []float64{1.5, 2.5, 3.0}
	for _, val := range values {
		err := calc.UpdateAggregate("group1", types.NewFloat64Field(val))
		if err != nil {
			t.Fatalf("Failed to update aggregate: %v", err)
		}
	}

	result := calc.groupToAgg["group1"]
	expected := 7.0
	if math.Abs(result-expected) > 0.0001 {
		t.Errorf("Expected SUM %v, got %v", expected, result)
	}
}

func TestFloatCalculator_UpdateAggregate_AVG(t *testing.T) {
	calc := NewFloatCalculator(core.Avg)
	calc.InitializeGroup("group1")

	values := []float64{2.0, 4.0, 6.0}
	for _, val := range values {
		err := calc.UpdateAggregate("group1", types.NewFloat64Field(val))
		if err != nil {
			t.Fatalf("Failed to update aggregate: %v", err)
		}
	}

	sum := calc.groupToAgg["group1"]
	count := calc.groupToCount["group1"]

	if sum != 12.0 {
		t.Errorf("Expected sum 12.0, got %v", sum)
	}
	if count != 3 {
		t.Errorf("Expected count 3, got %d", count)
	}
}

func TestFloatCalculator_UpdateAggregate_COUNT(t *testing.T) {
	calc := NewFloatCalculator(core.Count)
	calc.InitializeGroup("group1")

	values := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	for _, val := range values {
		err := calc.UpdateAggregate("group1", types.NewFloat64Field(val))
		if err != nil {
			t.Fatalf("Failed to update aggregate: %v", err)
		}
	}

	result := calc.groupToAgg["group1"]
	expected := 5.0
	if result != expected {
		t.Errorf("Expected COUNT %v, got %v", expected, result)
	}
}

func TestFloatCalculator_UpdateAggregate_InvalidFieldType(t *testing.T) {
	calc := NewFloatCalculator(core.Sum)
	calc.InitializeGroup("group1")

	err := calc.UpdateAggregate("group1", types.NewIntField(10))
	if err == nil {
		t.Error("Expected error when passing non-FloatField")
	}
}

func TestFloatCalculator_GetFinalValue(t *testing.T) {
	t.Run("SUM returns FloatField", func(t *testing.T) {
		calc := NewFloatCalculator(core.Sum)
		calc.InitializeGroup("group1")
		calc.UpdateAggregate("group1", types.NewFloat64Field(10.5))
		calc.UpdateAggregate("group1", types.NewFloat64Field(20.5))

		field, err := calc.GetFinalValue("group1")
		if err != nil {
			t.Fatalf("Failed to get final value: %v", err)
		}

		floatField, ok := field.(*types.Float64Field)
		if !ok {
			t.Fatal("Expected Float64Field")
		}
		if floatField.Value != 31.0 {
			t.Errorf("Expected value 31.0, got %v", floatField.Value)
		}
	})

	t.Run("AVG computes average", func(t *testing.T) {
		calc := NewFloatCalculator(core.Avg)
		calc.InitializeGroup("group1")
		calc.UpdateAggregate("group1", types.NewFloat64Field(10.0))
		calc.UpdateAggregate("group1", types.NewFloat64Field(20.0))
		calc.UpdateAggregate("group1", types.NewFloat64Field(30.0))

		field, err := calc.GetFinalValue("group1")
		if err != nil {
			t.Fatalf("Failed to get final value: %v", err)
		}

		floatField, ok := field.(*types.Float64Field)
		if !ok {
			t.Fatal("Expected Float64Field")
		}
		expected := 20.0
		if math.Abs(floatField.Value-expected) > 0.0001 {
			t.Errorf("Expected average %v, got %v", expected, floatField.Value)
		}
	})

	t.Run("COUNT returns IntField", func(t *testing.T) {
		calc := NewFloatCalculator(core.Count)
		calc.InitializeGroup("group1")
		calc.UpdateAggregate("group1", types.NewFloat64Field(10.0))
		calc.UpdateAggregate("group1", types.NewFloat64Field(20.0))

		field, err := calc.GetFinalValue("group1")
		if err != nil {
			t.Fatalf("Failed to get final value: %v", err)
		}

		intField, ok := field.(*types.IntField)
		if !ok {
			t.Fatal("Expected IntField for COUNT")
		}
		if intField.Value != 2 {
			t.Errorf("Expected count 2, got %d", intField.Value)
		}
	})

	t.Run("AVG with zero count", func(t *testing.T) {
		calc := NewFloatCalculator(core.Avg)
		calc.InitializeGroup("group1")

		field, err := calc.GetFinalValue("group1")
		if err != nil {
			t.Fatalf("Failed to get final value: %v", err)
		}

		floatField, ok := field.(*types.Float64Field)
		if !ok {
			t.Fatal("Expected Float64Field")
		}
		if floatField.Value != 0.0 {
			t.Errorf("Expected 0.0 for AVG with no values, got %v", floatField.Value)
		}
	})
}

func TestFloatCalculator_MultipleGroups(t *testing.T) {
	calc := NewFloatCalculator(core.Sum)

	groups := map[string][]float64{
		"A": {1.0, 2.0, 3.0},
		"B": {4.0, 5.0, 6.0},
		"C": {7.0, 8.0, 9.0},
	}

	for group, values := range groups {
		calc.InitializeGroup(group)
		for _, val := range values {
			calc.UpdateAggregate(group, types.NewFloat64Field(val))
		}
	}

	expectedSums := map[string]float64{
		"A": 6.0,
		"B": 15.0,
		"C": 24.0,
	}

	for group, expectedSum := range expectedSums {
		field, err := calc.GetFinalValue(group)
		if err != nil {
			t.Fatalf("Failed to get final value for group %s: %v", group, err)
		}

		floatField := field.(*types.Float64Field)
		if math.Abs(floatField.Value-expectedSum) > 0.0001 {
			t.Errorf("Group %s: expected sum %v, got %v", group, expectedSum, floatField.Value)
		}
	}
}

// ====================================
// IntCalculator Tests
// ====================================

func TestIntCalculator_NewIntCalculator(t *testing.T) {
	calc := NewIntCalculator(core.Sum)
	if calc == nil {
		t.Fatal("Expected non-nil IntCalculator")
	}
	if calc.op != core.Sum {
		t.Errorf("Expected operation Sum, got %v", calc.op)
	}
	if calc.groupToAgg == nil {
		t.Error("Expected non-nil groupToAgg map")
	}
	if calc.groupToCount == nil {
		t.Error("Expected non-nil groupToCount map")
	}
}

func TestIntCalculator_ValidateOperation(t *testing.T) {
	calc := NewIntCalculator(core.Sum)

	tests := []struct {
		name      string
		op        core.AggregateOp
		expectErr bool
	}{
		{"MIN is valid", core.Min, false},
		{"MAX is valid", core.Max, false},
		{"SUM is valid", core.Sum, false},
		{"AVG is valid", core.Avg, false},
		{"COUNT is valid", core.Count, false},
		{"AND is invalid", core.And, true},
		{"OR is invalid", core.Or, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := calc.ValidateOperation(tt.op)
			if tt.expectErr && err == nil {
				t.Errorf("Expected error for operation %v", tt.op)
			}
			if !tt.expectErr && err != nil {
				t.Errorf("Unexpected error for operation %v: %v", tt.op, err)
			}
		})
	}
}

func TestIntCalculator_GetResultType(t *testing.T) {
	calc := NewIntCalculator(core.Sum)

	resultType := calc.GetResultType(core.Sum)
	if resultType != types.IntType {
		t.Errorf("Expected IntType, got %v", resultType)
	}
}

func TestIntCalculator_InitializeGroup(t *testing.T) {
	tests := []struct {
		name          string
		op            core.AggregateOp
		expectedValue int64
	}{
		{"MIN initializes to MaxInt32", core.Min, math.MaxInt32},
		{"MAX initializes to MinInt32", core.Max, math.MinInt32},
		{"SUM initializes to 0", core.Sum, 0},
		{"AVG initializes to 0", core.Avg, 0},
		{"COUNT initializes to 0", core.Count, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calc := NewIntCalculator(tt.op)
			calc.InitializeGroup("testGroup")

			value, exists := calc.groupToAgg["testGroup"]
			if !exists {
				t.Fatal("Group was not initialized")
			}
			if value != tt.expectedValue {
				t.Errorf("Expected initial value %v, got %v", tt.expectedValue, value)
			}

			if tt.op == core.Avg {
				count, exists := calc.groupToCount["testGroup"]
				if !exists {
					t.Error("Count was not initialized for AVG operation")
				}
				if count != 0 {
					t.Errorf("Expected initial count 0, got %d", count)
				}
			}
		})
	}
}

func TestIntCalculator_UpdateAggregate_MIN(t *testing.T) {
	calc := NewIntCalculator(core.Min)
	calc.InitializeGroup("group1")

	values := []int64{5, 3, 8, 1, 9}
	for _, val := range values {
		err := calc.UpdateAggregate("group1", types.NewIntField(val))
		if err != nil {
			t.Fatalf("Failed to update aggregate: %v", err)
		}
	}

	result := calc.groupToAgg["group1"]
	expected := int64(1)
	if result != expected {
		t.Errorf("Expected MIN %v, got %v", expected, result)
	}
}

func TestIntCalculator_UpdateAggregate_MAX(t *testing.T) {
	calc := NewIntCalculator(core.Max)
	calc.InitializeGroup("group1")

	values := []int64{5, 3, 8, 1, 9}
	for _, val := range values {
		err := calc.UpdateAggregate("group1", types.NewIntField(val))
		if err != nil {
			t.Fatalf("Failed to update aggregate: %v", err)
		}
	}

	result := calc.groupToAgg["group1"]
	expected := int64(9)
	if result != expected {
		t.Errorf("Expected MAX %v, got %v", expected, result)
	}
}

func TestIntCalculator_UpdateAggregate_SUM(t *testing.T) {
	calc := NewIntCalculator(core.Sum)
	calc.InitializeGroup("group1")

	values := []int64{1, 2, 3, 4, 5}
	for _, val := range values {
		err := calc.UpdateAggregate("group1", types.NewIntField(val))
		if err != nil {
			t.Fatalf("Failed to update aggregate: %v", err)
		}
	}

	result := calc.groupToAgg["group1"]
	expected := int64(15)
	if result != expected {
		t.Errorf("Expected SUM %v, got %v", expected, result)
	}
}

func TestIntCalculator_UpdateAggregate_AVG(t *testing.T) {
	calc := NewIntCalculator(core.Avg)
	calc.InitializeGroup("group1")

	values := []int64{10, 20, 30}
	for _, val := range values {
		err := calc.UpdateAggregate("group1", types.NewIntField(val))
		if err != nil {
			t.Fatalf("Failed to update aggregate: %v", err)
		}
	}

	sum := calc.groupToAgg["group1"]
	count := calc.groupToCount["group1"]

	if sum != 60 {
		t.Errorf("Expected sum 60, got %v", sum)
	}
	if count != 3 {
		t.Errorf("Expected count 3, got %d", count)
	}
}

func TestIntCalculator_UpdateAggregate_COUNT(t *testing.T) {
	calc := NewIntCalculator(core.Count)
	calc.InitializeGroup("group1")

	values := []int64{1, 2, 3, 4, 5}
	for _, val := range values {
		err := calc.UpdateAggregate("group1", types.NewIntField(val))
		if err != nil {
			t.Fatalf("Failed to update aggregate: %v", err)
		}
	}

	result := calc.groupToAgg["group1"]
	expected := int64(5)
	if result != expected {
		t.Errorf("Expected COUNT %v, got %v", expected, result)
	}
}

func TestIntCalculator_UpdateAggregate_InvalidFieldType(t *testing.T) {
	calc := NewIntCalculator(core.Sum)
	calc.InitializeGroup("group1")

	err := calc.UpdateAggregate("group1", types.NewFloat64Field(10.5))
	if err == nil {
		t.Error("Expected error when passing non-IntField")
	}
}

func TestIntCalculator_GetFinalValue(t *testing.T) {
	t.Run("SUM returns correct value", func(t *testing.T) {
		calc := NewIntCalculator(core.Sum)
		calc.InitializeGroup("group1")
		calc.UpdateAggregate("group1", types.NewIntField(10))
		calc.UpdateAggregate("group1", types.NewIntField(20))

		field, err := calc.GetFinalValue("group1")
		if err != nil {
			t.Fatalf("Failed to get final value: %v", err)
		}

		intField, ok := field.(*types.IntField)
		if !ok {
			t.Fatal("Expected IntField")
		}
		if intField.Value != 30 {
			t.Errorf("Expected value 30, got %v", intField.Value)
		}
	})

	t.Run("AVG computes average", func(t *testing.T) {
		calc := NewIntCalculator(core.Avg)
		calc.InitializeGroup("group1")
		calc.UpdateAggregate("group1", types.NewIntField(10))
		calc.UpdateAggregate("group1", types.NewIntField(20))
		calc.UpdateAggregate("group1", types.NewIntField(30))

		field, err := calc.GetFinalValue("group1")
		if err != nil {
			t.Fatalf("Failed to get final value: %v", err)
		}

		intField, ok := field.(*types.IntField)
		if !ok {
			t.Fatal("Expected IntField")
		}
		expected := int64(20)
		if intField.Value != expected {
			t.Errorf("Expected average %v, got %v", expected, intField.Value)
		}
	})

	t.Run("AVG with zero count", func(t *testing.T) {
		calc := NewIntCalculator(core.Avg)
		calc.InitializeGroup("group1")

		field, err := calc.GetFinalValue("group1")
		if err != nil {
			t.Fatalf("Failed to get final value: %v", err)
		}

		intField, ok := field.(*types.IntField)
		if !ok {
			t.Fatal("Expected IntField")
		}
		if intField.Value != 0 {
			t.Errorf("Expected 0 for AVG with no values, got %v", intField.Value)
		}
	})
}

// ====================================
// BooleanCalculator Tests
// ====================================

func TestBooleanCalculator_NewBooleanCalculator(t *testing.T) {
	calc := NewBooleanCalculator(core.And)
	if calc == nil {
		t.Fatal("Expected non-nil BooleanCalculator")
	}
	if calc.op != core.And {
		t.Errorf("Expected operation And, got %v", calc.op)
	}
	if calc.groupToAgg == nil {
		t.Error("Expected non-nil groupToAgg map")
	}
	if calc.groupToCount == nil {
		t.Error("Expected non-nil groupToCount map")
	}
}

func TestBooleanCalculator_ValidateOperation(t *testing.T) {
	calc := NewBooleanCalculator(core.And)

	tests := []struct {
		name      string
		op        core.AggregateOp
		expectErr bool
	}{
		{"COUNT is valid", core.Count, false},
		{"AND is valid", core.And, false},
		{"OR is valid", core.Or, false},
		{"SUM is valid", core.Sum, false},
		{"MIN is invalid", core.Min, true},
		{"MAX is invalid", core.Max, true},
		{"AVG is invalid", core.Avg, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := calc.ValidateOperation(tt.op)
			if tt.expectErr && err == nil {
				t.Errorf("Expected error for operation %v", tt.op)
			}
			if !tt.expectErr && err != nil {
				t.Errorf("Unexpected error for operation %v: %v", tt.op, err)
			}
		})
	}
}

func TestBooleanCalculator_GetResultType(t *testing.T) {
	tests := []struct {
		name         string
		op           core.AggregateOp
		expectedType types.Type
	}{
		{"AND returns BoolType", core.And, types.BoolType},
		{"OR returns BoolType", core.Or, types.BoolType},
		{"COUNT returns IntType", core.Count, types.IntType},
		{"SUM returns IntType", core.Sum, types.IntType},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calc := NewBooleanCalculator(tt.op)
			resultType := calc.GetResultType(tt.op)
			if resultType != tt.expectedType {
				t.Errorf("Expected type %v, got %v", tt.expectedType, resultType)
			}
		})
	}
}

func TestBooleanCalculator_InitializeGroup(t *testing.T) {
	tests := []struct {
		name          string
		op            core.AggregateOp
		expectedValue any
	}{
		{"AND initializes to true", core.And, true},
		{"OR initializes to false", core.Or, false},
		{"COUNT initializes to 0", core.Count, int64(0)},
		{"SUM initializes to 0", core.Sum, int64(0)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calc := NewBooleanCalculator(tt.op)
			calc.InitializeGroup("testGroup")

			value, exists := calc.groupToAgg["testGroup"]
			if !exists {
				t.Fatal("Group was not initialized")
			}
			if value != tt.expectedValue {
				t.Errorf("Expected initial value %v, got %v", tt.expectedValue, value)
			}

			count, exists := calc.groupToCount["testGroup"]
			if !exists {
				t.Error("Count was not initialized")
			}
			if count != 0 {
				t.Errorf("Expected initial count 0, got %d", count)
			}
		})
	}
}

func TestBooleanCalculator_UpdateAggregate_AND(t *testing.T) {
	calc := NewBooleanCalculator(core.And)
	calc.InitializeGroup("group1")

	t.Run("all true", func(t *testing.T) {
		calc.InitializeGroup("allTrue")
		values := []bool{true, true, true}
		for _, val := range values {
			err := calc.UpdateAggregate("allTrue", types.NewBoolField(val))
			if err != nil {
				t.Fatalf("Failed to update aggregate: %v", err)
			}
		}

		result := calc.groupToAgg["allTrue"].(bool)
		if !result {
			t.Error("Expected AND of all true to be true")
		}
	})

	t.Run("mixed values", func(t *testing.T) {
		calc.InitializeGroup("mixed")
		values := []bool{true, false, true}
		for _, val := range values {
			err := calc.UpdateAggregate("mixed", types.NewBoolField(val))
			if err != nil {
				t.Fatalf("Failed to update aggregate: %v", err)
			}
		}

		result := calc.groupToAgg["mixed"].(bool)
		if result {
			t.Error("Expected AND with false to be false")
		}
	})
}

func TestBooleanCalculator_UpdateAggregate_OR(t *testing.T) {
	calc := NewBooleanCalculator(core.Or)

	t.Run("all false", func(t *testing.T) {
		calc.InitializeGroup("allFalse")
		values := []bool{false, false, false}
		for _, val := range values {
			err := calc.UpdateAggregate("allFalse", types.NewBoolField(val))
			if err != nil {
				t.Fatalf("Failed to update aggregate: %v", err)
			}
		}

		result := calc.groupToAgg["allFalse"].(bool)
		if result {
			t.Error("Expected OR of all false to be false")
		}
	})

	t.Run("mixed values", func(t *testing.T) {
		calc.InitializeGroup("mixed")
		values := []bool{false, true, false}
		for _, val := range values {
			err := calc.UpdateAggregate("mixed", types.NewBoolField(val))
			if err != nil {
				t.Fatalf("Failed to update aggregate: %v", err)
			}
		}

		result := calc.groupToAgg["mixed"].(bool)
		if !result {
			t.Error("Expected OR with true to be true")
		}
	})
}

func TestBooleanCalculator_UpdateAggregate_SUM(t *testing.T) {
	calc := NewBooleanCalculator(core.Sum)
	calc.InitializeGroup("group1")

	values := []bool{true, false, true, true, false}
	for _, val := range values {
		err := calc.UpdateAggregate("group1", types.NewBoolField(val))
		if err != nil {
			t.Fatalf("Failed to update aggregate: %v", err)
		}
	}

	result := calc.groupToAgg["group1"].(int64)
	expected := int64(3) // Three true values
	if result != expected {
		t.Errorf("Expected SUM %v, got %v", expected, result)
	}
}

func TestBooleanCalculator_UpdateAggregate_COUNT(t *testing.T) {
	calc := NewBooleanCalculator(core.Count)
	calc.InitializeGroup("group1")

	values := []bool{true, false, true, true, false}
	for _, val := range values {
		err := calc.UpdateAggregate("group1", types.NewBoolField(val))
		if err != nil {
			t.Fatalf("Failed to update aggregate: %v", err)
		}
	}

	result := calc.groupToAgg["group1"].(int64)
	expected := int64(5)
	if result != expected {
		t.Errorf("Expected COUNT %v, got %v", expected, result)
	}
}

func TestBooleanCalculator_UpdateAggregate_InvalidFieldType(t *testing.T) {
	calc := NewBooleanCalculator(core.And)
	calc.InitializeGroup("group1")

	err := calc.UpdateAggregate("group1", types.NewIntField(1))
	if err == nil {
		t.Error("Expected error when passing non-BoolField")
	}
}

func TestBooleanCalculator_GetFinalValue(t *testing.T) {
	t.Run("AND returns BoolField", func(t *testing.T) {
		calc := NewBooleanCalculator(core.And)
		calc.InitializeGroup("group1")
		calc.UpdateAggregate("group1", types.NewBoolField(true))
		calc.UpdateAggregate("group1", types.NewBoolField(true))

		field, err := calc.GetFinalValue("group1")
		if err != nil {
			t.Fatalf("Failed to get final value: %v", err)
		}

		boolField, ok := field.(*types.BoolField)
		if !ok {
			t.Fatal("Expected BoolField")
		}
		if !boolField.Value {
			t.Error("Expected true for AND of all true")
		}
	})

	t.Run("COUNT returns IntField", func(t *testing.T) {
		calc := NewBooleanCalculator(core.Count)
		calc.InitializeGroup("group1")
		calc.UpdateAggregate("group1", types.NewBoolField(true))
		calc.UpdateAggregate("group1", types.NewBoolField(false))

		field, err := calc.GetFinalValue("group1")
		if err != nil {
			t.Fatalf("Failed to get final value: %v", err)
		}

		intField, ok := field.(*types.IntField)
		if !ok {
			t.Fatal("Expected IntField for COUNT")
		}
		if intField.Value != 2 {
			t.Errorf("Expected count 2, got %d", intField.Value)
		}
	})
}

// ====================================
// StringCalculator Tests
// ====================================

func TestStringCalculator_NewStringCalculator(t *testing.T) {
	calc := NewStringCalculator(core.Count)
	if calc == nil {
		t.Fatal("Expected non-nil StringCalculator")
	}
	if calc.op != core.Count {
		t.Errorf("Expected operation Count, got %v", calc.op)
	}
	if calc.groupToAgg == nil {
		t.Error("Expected non-nil groupToAgg map")
	}
	if calc.groupToCount == nil {
		t.Error("Expected non-nil groupToCount map")
	}
}

func TestStringCalculator_ValidateOperation(t *testing.T) {
	calc := NewStringCalculator(core.Count)

	tests := []struct {
		name      string
		op        core.AggregateOp
		expectErr bool
	}{
		{"COUNT is valid", core.Count, false},
		{"MIN is valid", core.Min, false},
		{"MAX is valid", core.Max, false},
		{"SUM is invalid", core.Sum, true},
		{"AVG is invalid", core.Avg, true},
		{"AND is invalid", core.And, true},
		{"OR is invalid", core.Or, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := calc.ValidateOperation(tt.op)
			if tt.expectErr && err == nil {
				t.Errorf("Expected error for operation %v", tt.op)
			}
			if !tt.expectErr && err != nil {
				t.Errorf("Unexpected error for operation %v: %v", tt.op, err)
			}
		})
	}
}

func TestStringCalculator_GetResultType(t *testing.T) {
	tests := []struct {
		name         string
		op           core.AggregateOp
		expectedType types.Type
	}{
		{"COUNT returns IntType", core.Count, types.IntType},
		{"MIN returns StringType", core.Min, types.StringType},
		{"MAX returns StringType", core.Max, types.StringType},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calc := NewStringCalculator(tt.op)
			resultType := calc.GetResultType(tt.op)
			if resultType != tt.expectedType {
				t.Errorf("Expected type %v, got %v", tt.expectedType, resultType)
			}
		})
	}
}

func TestStringCalculator_InitializeGroup(t *testing.T) {
	tests := []struct {
		name          string
		op            core.AggregateOp
		expectedValue any
	}{
		{"COUNT initializes to 0", core.Count, int64(0)},
		{"MIN initializes to empty string", core.Min, ""},
		{"MAX initializes to empty string", core.Max, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calc := NewStringCalculator(tt.op)
			calc.InitializeGroup("testGroup")

			value, exists := calc.groupToAgg["testGroup"]
			if !exists {
				t.Fatal("Group was not initialized")
			}
			if value != tt.expectedValue {
				t.Errorf("Expected initial value %v, got %v", tt.expectedValue, value)
			}

			count, exists := calc.groupToCount["testGroup"]
			if !exists {
				t.Error("Count was not initialized")
			}
			if count != 0 {
				t.Errorf("Expected initial count 0, got %d", count)
			}
		})
	}
}

func TestStringCalculator_UpdateAggregate_MIN(t *testing.T) {
	calc := NewStringCalculator(core.Min)
	calc.InitializeGroup("group1")

	values := []string{"banana", "apple", "cherry", "apricot"}
	for _, val := range values {
		err := calc.UpdateAggregate("group1", types.NewStringField(val, len(val)))
		if err != nil {
			t.Fatalf("Failed to update aggregate: %v", err)
		}
	}

	result := calc.groupToAgg["group1"].(string)
	expected := "apple"
	if result != expected {
		t.Errorf("Expected MIN %v, got %v", expected, result)
	}
}

func TestStringCalculator_UpdateAggregate_MAX(t *testing.T) {
	calc := NewStringCalculator(core.Max)
	calc.InitializeGroup("group1")

	values := []string{"banana", "apple", "cherry", "apricot"}
	for _, val := range values {
		err := calc.UpdateAggregate("group1", types.NewStringField(val, len(val)))
		if err != nil {
			t.Fatalf("Failed to update aggregate: %v", err)
		}
	}

	result := calc.groupToAgg["group1"].(string)
	expected := "cherry"
	if result != expected {
		t.Errorf("Expected MAX %v, got %v", expected, result)
	}
}

func TestStringCalculator_UpdateAggregate_COUNT(t *testing.T) {
	calc := NewStringCalculator(core.Count)
	calc.InitializeGroup("group1")

	values := []string{"a", "b", "c", "d", "e"}
	for _, val := range values {
		err := calc.UpdateAggregate("group1", types.NewStringField(val, len(val)))
		if err != nil {
			t.Fatalf("Failed to update aggregate: %v", err)
		}
	}

	result := calc.groupToAgg["group1"].(int64)
	expected := int64(5)
	if result != expected {
		t.Errorf("Expected COUNT %v, got %v", expected, result)
	}
}

func TestStringCalculator_UpdateAggregate_InvalidFieldType(t *testing.T) {
	calc := NewStringCalculator(core.Count)
	calc.InitializeGroup("group1")

	err := calc.UpdateAggregate("group1", types.NewIntField(10))
	if err == nil {
		t.Error("Expected error when passing non-StringField")
	}
}

func TestStringCalculator_UpdateAggregate_UnsupportedOperation(t *testing.T) {
	calc := NewStringCalculator(core.Sum)
	calc.InitializeGroup("group1")

	err := calc.UpdateAggregate("group1", types.NewStringField("test", 4))
	if err == nil {
		t.Error("Expected error for unsupported operation")
	}
}

func TestStringCalculator_GetFinalValue(t *testing.T) {
	t.Run("MIN returns StringField", func(t *testing.T) {
		calc := NewStringCalculator(core.Min)
		calc.InitializeGroup("group1")
		calc.UpdateAggregate("group1", types.NewStringField("zebra", 5))
		calc.UpdateAggregate("group1", types.NewStringField("apple", 5))

		field, err := calc.GetFinalValue("group1")
		if err != nil {
			t.Fatalf("Failed to get final value: %v", err)
		}

		stringField, ok := field.(*types.StringField)
		if !ok {
			t.Fatal("Expected StringField")
		}
		if stringField.Value != "apple" {
			t.Errorf("Expected value 'apple', got '%s'", stringField.Value)
		}
	})

	t.Run("COUNT returns IntField", func(t *testing.T) {
		calc := NewStringCalculator(core.Count)
		calc.InitializeGroup("group1")
		calc.UpdateAggregate("group1", types.NewStringField("a", 1))
		calc.UpdateAggregate("group1", types.NewStringField("b", 1))

		field, err := calc.GetFinalValue("group1")
		if err != nil {
			t.Fatalf("Failed to get final value: %v", err)
		}

		intField, ok := field.(*types.IntField)
		if !ok {
			t.Fatal("Expected IntField for COUNT")
		}
		if intField.Value != 2 {
			t.Errorf("Expected count 2, got %d", intField.Value)
		}
	})
}

func TestStringCalculator_GetInitValue(t *testing.T) {
	tests := []struct {
		name     string
		op       core.AggregateOp
		expected any
	}{
		{"COUNT init value", core.Count, int64(0)},
		{"MIN init value", core.Min, ""},
		{"MAX init value", core.Max, ""},
		{"default init value", core.Sum, int64(0)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calc := NewStringCalculator(tt.op)
			value := calc.getInitValue()
			if value != tt.expected {
				t.Errorf("Expected init value %v, got %v", tt.expected, value)
			}
		})
	}
}

// ====================================
// GetCalculator Tests
// ====================================

func TestGetCalculator(t *testing.T) {
	tests := []struct {
		name         string
		fieldType    types.Type
		op           core.AggregateOp
		expectError  bool
		expectedType any
	}{
		{"IntType returns IntCalculator", types.IntType, core.Sum, false, &IntCalculator{}},
		{"FloatType returns FloatCalculator", types.FloatType, core.Avg, false, &FloatCalculator{}},
		{"BoolType returns BooleanCalculator", types.BoolType, core.And, false, &BooleanCalculator{}},
		{"StringType returns StringCalculator", types.StringType, core.Count, false, &StringCalculator{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calc, err := GetCalculator(tt.fieldType, tt.op)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if calc == nil {
				t.Fatal("Expected non-nil calculator")
			}

			// Check type
			switch tt.expectedType.(type) {
			case *IntCalculator:
				if _, ok := calc.(*IntCalculator); !ok {
					t.Errorf("Expected IntCalculator, got %T", calc)
				}
			case *FloatCalculator:
				if _, ok := calc.(*FloatCalculator); !ok {
					t.Errorf("Expected FloatCalculator, got %T", calc)
				}
			case *BooleanCalculator:
				if _, ok := calc.(*BooleanCalculator); !ok {
					t.Errorf("Expected BooleanCalculator, got %T", calc)
				}
			case *StringCalculator:
				if _, ok := calc.(*StringCalculator); !ok {
					t.Errorf("Expected StringCalculator, got %T", calc)
				}
			}
		})
	}
}

// ====================================
// Edge Cases and Integration Tests
// ====================================

func TestCalculators_EmptyGroups(t *testing.T) {
	t.Run("FloatCalculator empty group", func(t *testing.T) {
		calc := NewFloatCalculator(core.Sum)
		calc.InitializeGroup("empty")

		field, err := calc.GetFinalValue("empty")
		if err != nil {
			t.Fatalf("Failed to get final value: %v", err)
		}

		floatField := field.(*types.Float64Field)
		if floatField.Value != 0.0 {
			t.Errorf("Expected 0.0 for empty group, got %v", floatField.Value)
		}
	})

	t.Run("IntCalculator empty group MIN", func(t *testing.T) {
		calc := NewIntCalculator(core.Min)
		calc.InitializeGroup("empty")

		field, err := calc.GetFinalValue("empty")
		if err != nil {
			t.Fatalf("Failed to get final value: %v", err)
		}

		intField := field.(*types.IntField)
		if intField.Value != math.MaxInt32 {
			t.Errorf("Expected MaxInt32 for empty MIN group, got %v", intField.Value)
		}
	})
}

func TestCalculators_SingleValue(t *testing.T) {
	t.Run("FloatCalculator single value", func(t *testing.T) {
		calc := NewFloatCalculator(core.Sum)
		calc.InitializeGroup("single")
		calc.UpdateAggregate("single", types.NewFloat64Field(42.5))

		field, _ := calc.GetFinalValue("single")
		floatField := field.(*types.Float64Field)
		if floatField.Value != 42.5 {
			t.Errorf("Expected 42.5, got %v", floatField.Value)
		}
	})
}

func TestCalculators_NegativeValues(t *testing.T) {
	t.Run("IntCalculator negative values", func(t *testing.T) {
		calc := NewIntCalculator(core.Sum)
		calc.InitializeGroup("negatives")

		values := []int64{-5, -10, -15}
		for _, val := range values {
			calc.UpdateAggregate("negatives", types.NewIntField(val))
		}

		field, _ := calc.GetFinalValue("negatives")
		intField := field.(*types.IntField)
		if intField.Value != -30 {
			t.Errorf("Expected -30, got %v", intField.Value)
		}
	})

	t.Run("FloatCalculator negative values", func(t *testing.T) {
		calc := NewFloatCalculator(core.Min)
		calc.InitializeGroup("negatives")

		values := []float64{-5.5, -10.2, -1.1, -15.8}
		for _, val := range values {
			calc.UpdateAggregate("negatives", types.NewFloat64Field(val))
		}

		field, _ := calc.GetFinalValue("negatives")
		floatField := field.(*types.Float64Field)
		if floatField.Value != -15.8 {
			t.Errorf("Expected -15.8, got %v", floatField.Value)
		}
	})
}

func TestCalculators_LargeNumbers(t *testing.T) {
	t.Run("IntCalculator large sum", func(t *testing.T) {
		calc := NewIntCalculator(core.Sum)
		calc.InitializeGroup("large")

		for i := 0; i < 1000; i++ {
			calc.UpdateAggregate("large", types.NewIntField(1))
		}

		field, _ := calc.GetFinalValue("large")
		intField := field.(*types.IntField)
		if intField.Value != 1000 {
			t.Errorf("Expected 1000, got %v", intField.Value)
		}
	})
}

func TestCalculators_SpecialFloatValues(t *testing.T) {
	t.Run("FloatCalculator with infinity", func(t *testing.T) {
		calc := NewFloatCalculator(core.Max)
		calc.InitializeGroup("inf")

		calc.UpdateAggregate("inf", types.NewFloat64Field(100.0))
		calc.UpdateAggregate("inf", types.NewFloat64Field(math.Inf(1)))
		calc.UpdateAggregate("inf", types.NewFloat64Field(50.0))

		field, _ := calc.GetFinalValue("inf")
		floatField := field.(*types.Float64Field)
		if !math.IsInf(floatField.Value, 1) {
			t.Errorf("Expected +Inf, got %v", floatField.Value)
		}
	})

	t.Run("FloatCalculator with negative infinity", func(t *testing.T) {
		calc := NewFloatCalculator(core.Min)
		calc.InitializeGroup("neginf")

		calc.UpdateAggregate("neginf", types.NewFloat64Field(100.0))
		calc.UpdateAggregate("neginf", types.NewFloat64Field(math.Inf(-1)))
		calc.UpdateAggregate("neginf", types.NewFloat64Field(50.0))

		field, _ := calc.GetFinalValue("neginf")
		floatField := field.(*types.Float64Field)
		if !math.IsInf(floatField.Value, -1) {
			t.Errorf("Expected -Inf, got %v", floatField.Value)
		}
	})
}

func TestCalculators_StringOrdering(t *testing.T) {
	t.Run("StringCalculator lexicographic ordering", func(t *testing.T) {
		calc := NewStringCalculator(core.Min)
		calc.InitializeGroup("order")

		// Test that comparison is case-sensitive and lexicographic
		values := []string{"Banana", "apple", "Cherry", "APPLE"}
		for _, val := range values {
			calc.UpdateAggregate("order", types.NewStringField(val, len(val)))
		}

		field, _ := calc.GetFinalValue("order")
		stringField := field.(*types.StringField)
		expected := "APPLE" // Uppercase letters come before lowercase in ASCII
		if stringField.Value != expected {
			t.Errorf("Expected MIN '%s', got '%s'", expected, stringField.Value)
		}
	})
}
