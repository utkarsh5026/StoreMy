package aggregation

import (
	"fmt"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

type mockCalculator struct {
	groups      map[string]bool
	values      map[string]types.Field
	resultType  types.Type
	validOps    map[AggregateOp]bool
	shouldError bool
}

func newMockCalculator(resultType types.Type, validOps []AggregateOp) *mockCalculator {
	validOpsMap := make(map[AggregateOp]bool)
	for _, op := range validOps {
		validOpsMap[op] = true
	}
	
	return &mockCalculator{
		groups:     make(map[string]bool),
		values:     make(map[string]types.Field),
		resultType: resultType,
		validOps:   validOpsMap,
	}
}

func (m *mockCalculator) InitializeGroup(groupKey string) {
	m.groups[groupKey] = true
	switch m.resultType {
	case types.IntType:
		m.values[groupKey] = types.NewIntField(0)
	case types.StringType:
		m.values[groupKey] = types.NewStringField("", types.StringMaxSize)
	case types.BoolType:
		m.values[groupKey] = types.NewBoolField(false)
	}
}

func (m *mockCalculator) UpdateAggregate(groupKey string, fieldValue types.Field) error {
	if m.shouldError {
		return fmt.Errorf("mock error")
	}
	
	if !m.groups[groupKey] {
		return fmt.Errorf("group not initialized")
	}
	
	switch m.resultType {
	case types.IntType:
		if intField, ok := fieldValue.(*types.IntField); ok {
			currentVal := m.values[groupKey].(*types.IntField).Value
			m.values[groupKey] = types.NewIntField(currentVal + intField.Value)
		}
	case types.StringType:
		if strField, ok := fieldValue.(*types.StringField); ok {
			m.values[groupKey] = strField
		}
	}
	
	return nil
}

func (m *mockCalculator) GetFinalValue(groupKey string) (types.Field, error) {
	if !m.groups[groupKey] {
		return nil, fmt.Errorf("group not found")
	}
	return m.values[groupKey], nil
}

func (m *mockCalculator) ValidateOperation(op AggregateOp) error {
	if !m.validOps[op] {
		return fmt.Errorf("unsupported operation")
	}
	return nil
}

func (m *mockCalculator) GetResultType(op AggregateOp) types.Type {
	return m.resultType
}

func TestNewBaseAggregator(t *testing.T) {
	tests := []struct {
		name        string
		gbField     int
		gbFieldType types.Type
		aField      int
		op          AggregateOp
		calculator  AggregateCalculator
		expectError bool
	}{
		{
			name:        "valid aggregator without grouping",
			gbField:     NoGrouping,
			gbFieldType: types.StringType,
			aField:      0,
			op:          Sum,
			calculator:  newMockCalculator(types.IntType, []AggregateOp{Sum, Count}),
			expectError: false,
		},
		{
			name:        "valid aggregator with grouping",
			gbField:     0,
			gbFieldType: types.StringType,
			aField:      1,
			op:          Sum,
			calculator:  newMockCalculator(types.IntType, []AggregateOp{Sum, Count}),
			expectError: false,
		},
		{
			name:        "invalid operation",
			gbField:     NoGrouping,
			gbFieldType: types.StringType,
			aField:      0,
			op:          Min,
			calculator:  newMockCalculator(types.IntType, []AggregateOp{Sum, Count}),
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agg, err := NewBaseAggregator(tt.gbField, tt.gbFieldType, tt.aField, tt.op, tt.calculator)
			
			if tt.expectError && err == nil {
				t.Error("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if !tt.expectError && agg == nil {
				t.Error("Expected aggregator but got nil")
			}
		})
	}
}

func TestBaseAggregator_CreateTupleDesc(t *testing.T) {
	tests := []struct {
		name             string
		gbField          int
		gbFieldType      types.Type
		op               AggregateOp
		expectedTypes    []types.Type
		expectedFields   []string
	}{
		{
			name:           "no grouping",
			gbField:        NoGrouping,
			gbFieldType:    types.StringType,
			op:             Sum,
			expectedTypes:  []types.Type{types.IntType},
			expectedFields: []string{"SUM"},
		},
		{
			name:           "with grouping",
			gbField:        0,
			gbFieldType:    types.StringType,
			op:             Count,
			expectedTypes:  []types.Type{types.StringType, types.IntType},
			expectedFields: []string{"group", "COUNT"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calculator := newMockCalculator(types.IntType, []AggregateOp{Sum, Count})
			agg, err := NewBaseAggregator(tt.gbField, tt.gbFieldType, 0, tt.op, calculator)
			if err != nil {
				t.Fatalf("Failed to create aggregator: %v", err)
			}

			tupleDesc := agg.GetTupleDesc()
			if tupleDesc == nil {
				t.Fatal("Expected tuple description but got nil")
			}

			if len(tupleDesc.Types) != len(tt.expectedTypes) {
				t.Errorf("Expected %d types but got %d", len(tt.expectedTypes), len(tupleDesc.Types))
			}

			for i, expectedType := range tt.expectedTypes {
				if i < len(tupleDesc.Types) && tupleDesc.Types[i] != expectedType {
					t.Errorf("Expected type %v at index %d but got %v", expectedType, i, tupleDesc.Types[i])
				}
			}

			if len(tupleDesc.FieldNames) != len(tt.expectedFields) {
				t.Errorf("Expected %d fields but got %d", len(tt.expectedFields), len(tupleDesc.FieldNames))
			}

			for i, expectedField := range tt.expectedFields {
				if i < len(tupleDesc.FieldNames) && tupleDesc.FieldNames[i] != expectedField {
					t.Errorf("Expected field %s at index %d but got %s", expectedField, i, tupleDesc.FieldNames[i])
				}
			}
		})
	}
}

func TestBaseAggregator_Merge(t *testing.T) {
	calculator := newMockCalculator(types.IntType, []AggregateOp{Sum})
	agg, err := NewBaseAggregator(0, types.StringType, 1, Sum, calculator)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}

	tupleDesc, err := tuple.NewTupleDesc(
		[]types.Type{types.StringType, types.IntType},
		[]string{"group", "value"},
	)
	if err != nil {
		t.Fatalf("Failed to create tuple desc: %v", err)
	}

	tup := tuple.NewTuple(tupleDesc)

	tup.SetField(0, types.NewStringField("group1", types.StringMaxSize))
	tup.SetField(1, types.NewIntField(10))

	err = agg.Merge(tup)
	if err != nil {
		t.Errorf("Unexpected error during merge: %v", err)
	}

	groups := agg.GetGroups()
	if len(groups) != 1 {
		t.Errorf("Expected 1 group but got %d", len(groups))
	}

	if groups[0] != "group1" {
		t.Errorf("Expected group 'group1' but got '%s'", groups[0])
	}
}

func TestBaseAggregator_MergeNoGrouping(t *testing.T) {
	calculator := newMockCalculator(types.IntType, []AggregateOp{Sum})
	agg, err := NewBaseAggregator(NoGrouping, types.StringType, 0, Sum, calculator)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}

	tupleDesc, err := tuple.NewTupleDesc(
		[]types.Type{types.IntType},
		[]string{"value"},
	)
	if err != nil {
		t.Fatalf("Failed to create tuple desc: %v", err)
	}

	tup := tuple.NewTuple(tupleDesc)

	tup.SetField(0, types.NewIntField(10))

	err = agg.Merge(tup)
	if err != nil {
		t.Errorf("Unexpected error during merge: %v", err)
	}

	groups := agg.GetGroups()
	if len(groups) != 1 {
		t.Errorf("Expected 1 group but got %d", len(groups))
	}

	if groups[0] != "NO_GROUPING" {
		t.Errorf("Expected group 'NO_GROUPING' but got '%s'", groups[0])
	}
}

func TestBaseAggregator_GetAggregateValue(t *testing.T) {
	calculator := newMockCalculator(types.IntType, []AggregateOp{Sum})
	agg, err := NewBaseAggregator(0, types.StringType, 1, Sum, calculator)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}

	tupleDesc, err := tuple.NewTupleDesc(
		[]types.Type{types.StringType, types.IntType},
		[]string{"group", "value"},
	)
	if err != nil {
		t.Fatalf("Failed to create tuple desc: %v", err)
	}

	tup := tuple.NewTuple(tupleDesc)

	tup.SetField(0, types.NewStringField("group1", types.StringMaxSize))
	tup.SetField(1, types.NewIntField(10))

	err = agg.Merge(tup)
	if err != nil {
		t.Fatalf("Failed to merge tuple: %v", err)
	}

	value, err := agg.GetAggregateValue("group1")
	if err != nil {
		t.Errorf("Unexpected error getting aggregate value: %v", err)
	}

	if value == nil {
		t.Error("Expected aggregate value but got nil")
	}
}

func TestBaseAggregator_GetGroupingField(t *testing.T) {
	calculator := newMockCalculator(types.IntType, []AggregateOp{Sum})
	agg, err := NewBaseAggregator(5, types.StringType, 1, Sum, calculator)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}

	groupingField := agg.GetGroupingField()
	if groupingField != 5 {
		t.Errorf("Expected grouping field 5 but got %d", groupingField)
	}
}

func TestBaseAggregator_Iterator(t *testing.T) {
	calculator := newMockCalculator(types.IntType, []AggregateOp{Sum})
	agg, err := NewBaseAggregator(0, types.StringType, 1, Sum, calculator)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}

	iterator := agg.Iterator()
	if iterator == nil {
		t.Error("Expected iterator but got nil")
	}
}

func TestBaseAggregator_ExtractGroupKey(t *testing.T) {
	tests := []struct {
		name           string
		gbField        int
		expectedKey    string
		expectError    bool
	}{
		{
			name:        "no grouping",
			gbField:     NoGrouping,
			expectedKey: "NO_GROUPING",
			expectError: false,
		},
		{
			name:        "with grouping field",
			gbField:     0,
			expectedKey: "test_group",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calculator := newMockCalculator(types.IntType, []AggregateOp{Sum})
			agg, err := NewBaseAggregator(tt.gbField, types.StringType, 1, Sum, calculator)
			if err != nil {
				t.Fatalf("Failed to create aggregator: %v", err)
			}

			var tupleDesc *tuple.TupleDescription
			var tup *tuple.Tuple

			if tt.gbField == NoGrouping {
				tupleDesc, err = tuple.NewTupleDesc(
					[]types.Type{types.IntType},
					[]string{"value"},
				)
				if err != nil {
					t.Fatalf("Failed to create tuple desc: %v", err)
				}

				tup = tuple.NewTuple(tupleDesc)

				tup.SetField(0, types.NewIntField(10))
			} else {
				tupleDesc, err = tuple.NewTupleDesc(
					[]types.Type{types.StringType, types.IntType},
					[]string{"group", "value"},
				)
				if err != nil {
					t.Fatalf("Failed to create tuple desc: %v", err)
				}

				tup = tuple.NewTuple(tupleDesc)

				tup.SetField(0, types.NewStringField("test_group", types.StringMaxSize))
				tup.SetField(1, types.NewIntField(10))
			}

			key, err := agg.extractGroupKey(tup)
			
			if tt.expectError && err == nil {
				t.Error("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if !tt.expectError && key != tt.expectedKey {
				t.Errorf("Expected key '%s' but got '%s'", tt.expectedKey, key)
			}
		})
	}
}

func TestBaseAggregator_ThreadSafety(t *testing.T) {
	calculator := newMockCalculator(types.IntType, []AggregateOp{Sum})
	agg, err := NewBaseAggregator(0, types.StringType, 1, Sum, calculator)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}

	tupleDesc, err := tuple.NewTupleDesc(
		[]types.Type{types.StringType, types.IntType},
		[]string{"group", "value"},
	)
	if err != nil {
		t.Fatalf("Failed to create tuple desc: %v", err)
	}

	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func(id int) {
			defer func() { done <- true }()
			
			tup := tuple.NewTuple(tupleDesc)

			tup.SetField(0, types.NewStringField("group1", types.StringMaxSize))
			tup.SetField(1, types.NewIntField(1))

			err = agg.Merge(tup)
			if err != nil {
				t.Errorf("Failed to merge tuple: %v", err)
			}
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	groups := agg.GetGroups()
	if len(groups) != 1 {
		t.Errorf("Expected 1 group but got %d", len(groups))
	}
}