package join

import (
	"fmt"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// ============================================================================
// JOIN TESTS
// ============================================================================

// Mock iterator for testing Join operator
type mockIterator struct {
	tuples     []*tuple.Tuple
	tupleDesc  *tuple.TupleDescription
	index      int
	isOpen     bool
	hasError   bool
	rewindable bool
}

func newMockIterator(tuples []*tuple.Tuple, tupleDesc *tuple.TupleDescription) *mockIterator {
	return &mockIterator{
		tuples:     tuples,
		tupleDesc:  tupleDesc,
		index:      -1,
		rewindable: true,
	}
}

func (m *mockIterator) Open() error {
	if m.hasError {
		return fmt.Errorf("mock open error")
	}
	m.isOpen = true
	m.index = -1
	return nil
}

func (m *mockIterator) HasNext() (bool, error) {
	if !m.isOpen {
		return false, fmt.Errorf("iterator not open")
	}
	if m.hasError {
		return false, fmt.Errorf("mock has next error")
	}
	return m.index+1 < len(m.tuples), nil
}

func (m *mockIterator) Next() (*tuple.Tuple, error) {
	if !m.isOpen {
		return nil, fmt.Errorf("iterator not open")
	}
	if m.hasError {
		return nil, fmt.Errorf("mock next error")
	}
	m.index++
	if m.index >= len(m.tuples) {
		return nil, fmt.Errorf("no more tuples")
	}
	return m.tuples[m.index], nil
}

func (m *mockIterator) Rewind() error {
	if !m.isOpen {
		return fmt.Errorf("iterator not open")
	}
	if !m.rewindable {
		return fmt.Errorf("rewind not supported")
	}
	m.index = -1
	return nil
}

func (m *mockIterator) Close() error {
	m.isOpen = false
	return nil
}

func (m *mockIterator) GetTupleDesc() *tuple.TupleDescription {
	return m.tupleDesc
}

// Helper functions for creating test data

func createTestTupleDesc(fieldTypes []types.Type, fieldNames []string) *tuple.TupleDescription {
	td, _ := tuple.NewTupleDesc(fieldTypes, fieldNames)
	return td
}

func createJoinTestTuple(tupleDesc *tuple.TupleDescription, values []interface{}) *tuple.Tuple {
	tup := tuple.NewTuple(tupleDesc)
	for i, val := range values {
		var field types.Field
		switch v := val.(type) {
		case int32:
			field = types.NewIntField(v)
		case string:
			field = types.NewStringField(v, types.StringMaxSize)
		}
		tup.SetField(i, field)
	}
	return tup
}
