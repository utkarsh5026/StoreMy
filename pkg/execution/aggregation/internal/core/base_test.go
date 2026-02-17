package core

import (
	"fmt"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"sync"
	"testing"
)

// mockCalculator implements AggregateCalculator for testing
type mockCalculator struct {
	groups          map[string]*mockGroupState
	validOperations map[AggregateOp]bool
	resultType      types.Type
	shouldError     bool
	mu              sync.RWMutex
}

type mockGroupState struct {
	initialized bool
	count       int
	value       types.Field
}

func newMockCalculator(resultType types.Type, validOps ...AggregateOp) *mockCalculator {
	calc := &mockCalculator{
		groups:          make(map[string]*mockGroupState),
		validOperations: make(map[AggregateOp]bool),
		resultType:      resultType,
	}
	for _, op := range validOps {
		calc.validOperations[op] = true
	}
	return calc
}

func newBaseAggregator(gbField primitives.ColumnID, gbFieldType types.Type, aField primitives.ColumnID, op AggregateOp, calculator AggregateCalculator) (*BaseAggregator, error) {
	conf := &AggregatorConfig{
		Operation:   op,
		GbField:     gbField,
		GbFieldType: gbFieldType,
		AggrField:   aField,
	}
	return NewBaseAggregator(conf, calculator)
}

func (m *mockCalculator) InitializeGroup(groupKey string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.groups[groupKey] = &mockGroupState{
		initialized: true,
		count:       0,
		value:       types.NewIntField(0),
	}
}

func (m *mockCalculator) UpdateAggregate(groupKey string, fieldValue types.Field) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.shouldError {
		return fmt.Errorf("mock update error")
	}

	state, exists := m.groups[groupKey]
	if !exists {
		return fmt.Errorf("group not initialized: %s", groupKey)
	}

	state.count++
	state.value = fieldValue
	return nil
}

func (m *mockCalculator) GetFinalValue(groupKey string) (types.Field, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.shouldError {
		return nil, fmt.Errorf("mock get value error")
	}

	state, exists := m.groups[groupKey]
	if !exists {
		return nil, fmt.Errorf("group not found: %s", groupKey)
	}

	return state.value, nil
}

func (m *mockCalculator) ValidateOperation(op AggregateOp) error {
	if !m.validOperations[op] {
		return fmt.Errorf("unsupported operation: %s", op.String())
	}
	return nil
}

func (m *mockCalculator) GetResultType(op AggregateOp) types.Type {
	return m.resultType
}

func (m *mockCalculator) setError(shouldError bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shouldError = shouldError
}

// Helper functions
func createBaseTestTupleDesc() *tuple.TupleDescription {
	td, _ := tuple.NewTupleDesc(
		[]types.Type{types.StringType, types.IntType},
		[]string{"group", "value"},
	)
	return td
}

func createBaseTestTuple(group string, value int64) *tuple.Tuple {
	td := createBaseTestTupleDesc()
	tup := tuple.NewTuple(td)
	tup.SetField(0, types.NewStringField(group, len(group)))
	tup.SetField(1, types.NewIntField(value))
	return tup
}

// TestNewBaseAggregator tests the constructor
func TestNewBaseAggregator(t *testing.T) {
	t.Run("valid aggregator with grouping", func(t *testing.T) {
		calc := newMockCalculator(types.IntType, Sum, Count)
		agg, err := newBaseAggregator(0, types.StringType, 1, Sum, calc)

		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if agg == nil {
			t.Fatal("Expected non-nil aggregator")
		}
		if agg.GbField != 0 {
			t.Errorf("Expected GbField 0, got %d", agg.GbField)
		}
		if agg.AggrField != 1 {
			t.Errorf("Expected AggrField 1, got %d", agg.AggrField)
		}
		if agg.Operation != Sum {
			t.Errorf("Expected operation Sum, got %v", agg.Operation)
		}
		if agg.tupleDesc == nil {
			t.Error("Expected non-nil tuple description")
		}
		if agg.groups == nil {
			t.Error("Expected non-nil groups map")
		}
	})

	t.Run("valid aggregator without grouping", func(t *testing.T) {
		calc := newMockCalculator(types.IntType, Count)
		agg, err := newBaseAggregator(NoGrouping, types.IntType, 0, Count, calc)

		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if agg == nil {
			t.Fatal("Expected non-nil aggregator")
		}
		if agg.GbField != NoGrouping {
			t.Errorf("Expected GbField NoGrouping, got %d", agg.GbField)
		}
	})

	t.Run("invalid operation", func(t *testing.T) {
		calc := newMockCalculator(types.IntType, Sum) // Only Sum is valid
		agg, err := newBaseAggregator(0, types.StringType, 1, Count, calc)

		if err == nil {
			t.Error("Expected error for invalid operation")
		}
		if agg != nil {
			t.Error("Expected nil aggregator on error")
		}
	})
}

// TestBaseAggregator_createTupleDesc tests tuple description creation
func TestBaseAggregator_createTupleDesc(t *testing.T) {
	t.Run("non-grouped aggregation", func(t *testing.T) {
		calc := newMockCalculator(types.IntType, Sum)
		agg, err := newBaseAggregator(NoGrouping, types.IntType, 0, Sum, calc)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		td := agg.GetTupleDesc()
		if td.NumFields() != 1 {
			t.Errorf("Expected 1 field, got %d", td.NumFields())
		}

		fieldType, _ := td.TypeAtIndex(0)
		if fieldType != types.IntType {
			t.Errorf("Expected IntType, got %v", fieldType)
		}

		fieldName := td.FieldNames[0]
		if fieldName != "SUM" {
			t.Errorf("Expected field name 'SUM', got '%s'", fieldName)
		}
	})

	t.Run("grouped aggregation", func(t *testing.T) {
		calc := newMockCalculator(types.IntType, Sum)
		agg, err := newBaseAggregator(0, types.StringType, 1, Sum, calc)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		td := agg.GetTupleDesc()
		if td.NumFields() != 2 {
			t.Errorf("Expected 2 fields, got %d", td.NumFields())
		}

		// First field should be group field
		groupType, _ := td.TypeAtIndex(0)
		if groupType != types.StringType {
			t.Errorf("Expected StringType for group field, got %v", groupType)
		}

		groupName := td.FieldNames[0]
		if groupName != "group" {
			t.Errorf("Expected field name 'group', got '%s'", groupName)
		}

		// Second field should be aggregate result
		aggType, _ := td.TypeAtIndex(1)
		if aggType != types.IntType {
			t.Errorf("Expected IntType for aggregate field, got %v", aggType)
		}

		aggName := td.FieldNames[1]
		if aggName != "SUM" {
			t.Errorf("Expected field name 'SUM', got '%s'", aggName)
		}
	})
}

// TestBaseAggregator_GetGroups tests group retrieval
func TestBaseAggregator_GetGroups(t *testing.T) {
	t.Run("empty groups", func(t *testing.T) {
		calc := newMockCalculator(types.IntType, Sum)
		agg, err := newBaseAggregator(0, types.StringType, 1, Sum, calc)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		groups := agg.GetGroups()
		if len(groups) != 0 {
			t.Errorf("Expected 0 groups, got %d", len(groups))
		}
	})

	t.Run("single group after merge", func(t *testing.T) {
		calc := newMockCalculator(types.IntType, Sum)
		agg, err := newBaseAggregator(0, types.StringType, 1, Sum, calc)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		tup := createBaseTestTuple("A", 10)
		err = agg.Merge(tup)
		if err != nil {
			t.Fatalf("Failed to merge tuple: %v", err)
		}

		groups := agg.GetGroups()
		if len(groups) != 1 {
			t.Errorf("Expected 1 group, got %d", len(groups))
		}
		if groups[0] != "A" {
			t.Errorf("Expected group 'A', got '%s'", groups[0])
		}
	})

	t.Run("multiple groups", func(t *testing.T) {
		calc := newMockCalculator(types.IntType, Sum)
		agg, err := newBaseAggregator(0, types.StringType, 1, Sum, calc)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		tuples := []*tuple.Tuple{
			createBaseTestTuple("A", 10),
			createBaseTestTuple("B", 20),
			createBaseTestTuple("C", 30),
		}

		for _, tup := range tuples {
			err = agg.Merge(tup)
			if err != nil {
				t.Fatalf("Failed to merge tuple: %v", err)
			}
		}

		groups := agg.GetGroups()
		if len(groups) != 3 {
			t.Errorf("Expected 3 groups, got %d", len(groups))
		}

		// Check all groups exist
		groupMap := make(map[string]bool)
		for _, g := range groups {
			groupMap[g] = true
		}
		if !groupMap["A"] || !groupMap["B"] || !groupMap["C"] {
			t.Error("Not all expected groups found")
		}
	})
}

// TestBaseAggregator_GetAggregateValue tests aggregate value retrieval
func TestBaseAggregator_GetAggregateValue(t *testing.T) {
	t.Run("get value for existing group", func(t *testing.T) {
		calc := newMockCalculator(types.IntType, Sum)
		agg, err := newBaseAggregator(0, types.StringType, 1, Sum, calc)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		tup := createBaseTestTuple("A", 42)
		err = agg.Merge(tup)
		if err != nil {
			t.Fatalf("Failed to merge tuple: %v", err)
		}

		value, err := agg.GetAggregateValue("A")
		if err != nil {
			t.Fatalf("Failed to get aggregate value: %v", err)
		}

		intField, ok := value.(*types.IntField)
		if !ok {
			t.Fatal("Expected IntField")
		}
		if intField.Value != 42 {
			t.Errorf("Expected value 42, got %d", intField.Value)
		}
	})

	t.Run("get value for non-existing group", func(t *testing.T) {
		calc := newMockCalculator(types.IntType, Sum)
		agg, err := newBaseAggregator(0, types.StringType, 1, Sum, calc)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		_, err = agg.GetAggregateValue("NonExistent")
		if err == nil {
			t.Error("Expected error for non-existent group")
		}
	})
}

// TestBaseAggregator_GetTupleDesc tests tuple description retrieval
func TestBaseAggregator_GetTupleDesc(t *testing.T) {
	calc := newMockCalculator(types.IntType, Sum)
	agg, err := newBaseAggregator(0, types.StringType, 1, Sum, calc)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}

	td := agg.GetTupleDesc()
	if td == nil {
		t.Fatal("Expected non-nil tuple description")
	}
	if td.NumFields() != 2 {
		t.Errorf("Expected 2 fields, got %d", td.NumFields())
	}
}

// TestBaseAggregator_GetGroupingField tests grouping field retrieval
func TestBaseAggregator_GetGroupingField(t *testing.T) {
	t.Run("with grouping", func(t *testing.T) {
		calc := newMockCalculator(types.IntType, Sum)
		agg, err := newBaseAggregator(0, types.StringType, 1, Sum, calc)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		GbField := agg.GetGroupingField()
		if GbField != 0 {
			t.Errorf("Expected grouping field 0, got %d", GbField)
		}
	})

	t.Run("without grouping", func(t *testing.T) {
		calc := newMockCalculator(types.IntType, Sum)
		agg, err := newBaseAggregator(NoGrouping, types.IntType, 0, Sum, calc)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		GbField := agg.GetGroupingField()
		if GbField != NoGrouping {
			t.Errorf("Expected grouping field NoGrouping, got %d", GbField)
		}
	})
}

// TestBaseAggregator_RLockRUnlock tests read locking
func TestBaseAggregator_RLockRUnlock(t *testing.T) {
	calc := newMockCalculator(types.IntType, Sum)
	agg, err := newBaseAggregator(0, types.StringType, 1, Sum, calc)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}

	// Should not panic - verify RLock/RUnlock work without deadlock
	agg.RLock()
	locked := true //nolint:staticcheck
	agg.RUnlock()
	_ = locked
}

// TestBaseAggregator_Iterator tests iterator creation
func TestBaseAggregator_Iterator(t *testing.T) {
	calc := newMockCalculator(types.IntType, Sum)
	agg, err := newBaseAggregator(0, types.StringType, 1, Sum, calc)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}

	iter := agg.Iterator()
	if iter == nil {
		t.Fatal("Expected non-nil iterator")
	}

	// Check that iterator has correct type
	_, ok := iter.(*AggregatorIterator)
	if !ok {
		t.Error("Expected AggregatorIterator type")
	}
}

// TestBaseAggregator_Merge tests tuple merging
func TestBaseAggregator_Merge(t *testing.T) {
	t.Run("merge single tuple with grouping", func(t *testing.T) {
		calc := newMockCalculator(types.IntType, Sum)
		agg, err := newBaseAggregator(0, types.StringType, 1, Sum, calc)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		tup := createBaseTestTuple("A", 10)
		err = agg.Merge(tup)
		if err != nil {
			t.Fatalf("Failed to merge tuple: %v", err)
		}

		groups := agg.GetGroups()
		if len(groups) != 1 {
			t.Errorf("Expected 1 group, got %d", len(groups))
		}
	})

	t.Run("merge multiple tuples same group", func(t *testing.T) {
		calc := newMockCalculator(types.IntType, Sum)
		agg, err := newBaseAggregator(0, types.StringType, 1, Sum, calc)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		tuples := []*tuple.Tuple{
			createBaseTestTuple("A", 10),
			createBaseTestTuple("A", 20),
			createBaseTestTuple("A", 30),
		}

		for _, tup := range tuples {
			err = agg.Merge(tup)
			if err != nil {
				t.Fatalf("Failed to merge tuple: %v", err)
			}
		}

		groups := agg.GetGroups()
		if len(groups) != 1 {
			t.Errorf("Expected 1 group, got %d", len(groups))
		}

		// Check that calculator's UpdateAggregate was called 3 times
		state := calc.groups["A"]
		if state.count != 3 {
			t.Errorf("Expected 3 updates, got %d", state.count)
		}
	})

	t.Run("merge multiple tuples different groups", func(t *testing.T) {
		calc := newMockCalculator(types.IntType, Sum)
		agg, err := newBaseAggregator(0, types.StringType, 1, Sum, calc)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		tuples := []*tuple.Tuple{
			createBaseTestTuple("A", 10),
			createBaseTestTuple("B", 20),
			createBaseTestTuple("C", 30),
		}

		for _, tup := range tuples {
			err = agg.Merge(tup)
			if err != nil {
				t.Fatalf("Failed to merge tuple: %v", err)
			}
		}

		groups := agg.GetGroups()
		if len(groups) != 3 {
			t.Errorf("Expected 3 groups, got %d", len(groups))
		}
	})

	t.Run("merge without grouping", func(t *testing.T) {
		calc := newMockCalculator(types.IntType, Count)
		td, _ := tuple.NewTupleDesc(
			[]types.Type{types.IntType},
			[]string{"value"},
		)
		tup1 := tuple.NewTuple(td)
		tup1.SetField(0, types.NewIntField(10))

		agg, err := newBaseAggregator(NoGrouping, types.IntType, 0, Count, calc)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		err = agg.Merge(tup1)
		if err != nil {
			t.Fatalf("Failed to merge tuple: %v", err)
		}

		groups := agg.GetGroups()
		if len(groups) != 1 {
			t.Errorf("Expected 1 group, got %d", len(groups))
		}
		if groups[0] != "NO_GROUPING" {
			t.Errorf("Expected group 'NO_GROUPING', got '%s'", groups[0])
		}
	})

	t.Run("merge with invalid aggregate field", func(t *testing.T) {
		calc := newMockCalculator(types.IntType, Sum)
		agg, err := newBaseAggregator(0, types.StringType, 5, Sum, calc)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		tup := createBaseTestTuple("A", 10)
		err = agg.Merge(tup)
		if err == nil {
			t.Error("Expected error for invalid aggregate field")
		}
	})

	t.Run("merge with invalid grouping field", func(t *testing.T) {
		calc := newMockCalculator(types.IntType, Sum)
		agg, err := newBaseAggregator(5, types.StringType, 1, Sum, calc)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		tup := createBaseTestTuple("A", 10)
		err = agg.Merge(tup)
		if err == nil {
			t.Error("Expected error for invalid grouping field")
		}
	})
}

// TestBaseAggregator_extractGroupKey tests group key extraction
func TestBaseAggregator_extractGroupKey(t *testing.T) {
	t.Run("extract group key with grouping", func(t *testing.T) {
		calc := newMockCalculator(types.IntType, Sum)
		agg, err := newBaseAggregator(0, types.StringType, 1, Sum, calc)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		tup := createBaseTestTuple("TestGroup", 10)
		groupKey, err := agg.extractGroupKey(tup)
		if err != nil {
			t.Fatalf("Failed to extract group key: %v", err)
		}
		if groupKey != "TestGroup" {
			t.Errorf("Expected group key 'TestGroup', got '%s'", groupKey)
		}
	})

	t.Run("extract group key without grouping", func(t *testing.T) {
		calc := newMockCalculator(types.IntType, Count)
		agg, err := newBaseAggregator(NoGrouping, types.IntType, 0, Count, calc)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		td, _ := tuple.NewTupleDesc(
			[]types.Type{types.IntType},
			[]string{"value"},
		)
		tup := tuple.NewTuple(td)
		tup.SetField(0, types.NewIntField(10))

		groupKey, err := agg.extractGroupKey(tup)
		if err != nil {
			t.Fatalf("Failed to extract group key: %v", err)
		}
		if groupKey != "NO_GROUPING" {
			t.Errorf("Expected group key 'NO_GROUPING', got '%s'", groupKey)
		}
	})

	t.Run("extract group key with invalid field", func(t *testing.T) {
		calc := newMockCalculator(types.IntType, Sum)
		agg, err := newBaseAggregator(5, types.StringType, 1, Sum, calc)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		tup := createBaseTestTuple("A", 10)
		_, err = agg.extractGroupKey(tup)
		if err == nil {
			t.Error("Expected error for invalid grouping field")
		}
	})
}

// TestBaseAggregator_InitializeDefault tests default initialization
func TestBaseAggregator_InitializeDefault(t *testing.T) {
	t.Run("initialize default for non-grouped aggregate", func(t *testing.T) {
		calc := newMockCalculator(types.IntType, Count)
		agg, err := newBaseAggregator(NoGrouping, types.IntType, 0, Count, calc)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		err = agg.InitializeDefault()
		if err != nil {
			t.Fatalf("Failed to initialize default: %v", err)
		}

		groups := agg.GetGroups()
		if len(groups) != 1 {
			t.Errorf("Expected 1 group, got %d", len(groups))
		}
		if groups[0] != "NO_GROUPING" {
			t.Errorf("Expected group 'NO_GROUPING', got '%s'", groups[0])
		}

		// Verify group was initialized in calculator
		state, exists := calc.groups["NO_GROUPING"]
		if !exists {
			t.Error("Expected group to be initialized in calculator")
		}
		if !state.initialized {
			t.Error("Expected group to be marked as initialized")
		}
	})

	t.Run("initialize default for grouped aggregate does nothing", func(t *testing.T) {
		calc := newMockCalculator(types.IntType, Sum)
		agg, err := newBaseAggregator(0, types.StringType, 1, Sum, calc)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		err = agg.InitializeDefault()
		if err != nil {
			t.Fatalf("Failed to initialize default: %v", err)
		}

		groups := agg.GetGroups()
		if len(groups) != 0 {
			t.Errorf("Expected 0 groups for grouped aggregate, got %d", len(groups))
		}
	})

	t.Run("initialize default twice is idempotent", func(t *testing.T) {
		calc := newMockCalculator(types.IntType, Count)
		agg, err := newBaseAggregator(NoGrouping, types.IntType, 0, Count, calc)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		err = agg.InitializeDefault()
		if err != nil {
			t.Fatalf("First initialize failed: %v", err)
		}

		err = agg.InitializeDefault()
		if err != nil {
			t.Fatalf("Second initialize failed: %v", err)
		}

		groups := agg.GetGroups()
		if len(groups) != 1 {
			t.Errorf("Expected 1 group, got %d", len(groups))
		}
	})
}

// TestBaseAggregator_ThreadSafety tests concurrent access
func TestBaseAggregator_ThreadSafety(t *testing.T) {
	calc := newMockCalculator(types.IntType, Sum)
	agg, err := newBaseAggregator(0, types.StringType, 1, Sum, calc)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}

	// Concurrent writes
	var wg sync.WaitGroup
	numGoroutines := 10
	tuplesPerGoroutine := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < tuplesPerGoroutine; j++ {
				tup := createBaseTestTuple(fmt.Sprintf("Group%d", id%3), int64(j))
				agg.Merge(tup)
			}
		}(i)
	}

	wg.Wait()

	// Verify all groups were created
	groups := agg.GetGroups()
	if len(groups) == 0 {
		t.Error("Expected at least one group after concurrent merges")
	}
}

// TestBaseAggregator_Integration tests end-to-end scenarios
func TestBaseAggregator_Integration(t *testing.T) {
	t.Run("complete aggregation workflow", func(t *testing.T) {
		calc := newMockCalculator(types.IntType, Sum)
		agg, err := newBaseAggregator(0, types.StringType, 1, Sum, calc)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		// Merge tuples
		tuples := []*tuple.Tuple{
			createBaseTestTuple("A", 10),
			createBaseTestTuple("B", 20),
			createBaseTestTuple("A", 15),
			createBaseTestTuple("C", 30),
			createBaseTestTuple("B", 25),
		}

		for _, tup := range tuples {
			err = agg.Merge(tup)
			if err != nil {
				t.Fatalf("Failed to merge tuple: %v", err)
			}
		}

		// Verify groups
		groups := agg.GetGroups()
		if len(groups) != 3 {
			t.Errorf("Expected 3 groups, got %d", len(groups))
		}

		// Verify values can be retrieved
		for _, group := range groups {
			_, err := agg.GetAggregateValue(group)
			if err != nil {
				t.Errorf("Failed to get value for group %s: %v", group, err)
			}
		}

		// Verify iterator works
		iter := agg.Iterator()
		err = iter.Open()
		if err != nil {
			t.Fatalf("Failed to open iterator: %v", err)
		}
		defer iter.Close()

		count := 0
		for {
			hasNext, err := iter.HasNext()
			if err != nil || !hasNext {
				break
			}
			_, err = iter.Next()
			if err != nil {
				t.Fatalf("Failed to get next from iterator: %v", err)
			}
			count++
		}

		if count != 3 {
			t.Errorf("Expected 3 results from iterator, got %d", count)
		}
	})

	t.Run("non-grouped aggregation with empty table", func(t *testing.T) {
		calc := newMockCalculator(types.IntType, Count)
		agg, err := newBaseAggregator(NoGrouping, types.IntType, 0, Count, calc)
		if err != nil {
			t.Fatalf("Failed to create aggregator: %v", err)
		}

		// Initialize default so COUNT returns 0
		err = agg.InitializeDefault()
		if err != nil {
			t.Fatalf("Failed to initialize default: %v", err)
		}

		groups := agg.GetGroups()
		if len(groups) != 1 {
			t.Errorf("Expected 1 group, got %d", len(groups))
		}

		value, err := agg.GetAggregateValue("NO_GROUPING")
		if err != nil {
			t.Fatalf("Failed to get value: %v", err)
		}
		if value == nil {
			t.Error("Expected non-nil value")
		}
	})
}
