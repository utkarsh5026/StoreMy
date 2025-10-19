package query

import (
	"fmt"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

func mustCreateLimitTupleDesc() *tuple.TupleDescription {
	td, err := tuple.NewTupleDesc(
		[]types.Type{types.IntType},
		[]string{"id"},
	)
	if err != nil {
		panic(fmt.Sprintf("Failed to create TupleDescription: %v", err))
	}
	return td
}

func createLimitTestTuple(td *tuple.TupleDescription, id int64) *tuple.Tuple {
	t := tuple.NewTuple(td)
	intField := types.NewIntField(id)

	err := t.SetField(0, intField)
	if err != nil {
		panic(fmt.Sprintf("Failed to set int field: %v", err))
	}

	return t
}

func TestNewLimitOperator(t *testing.T) {
	td := mustCreateLimitTupleDesc()
	child := newMockChildIterator([]*tuple.Tuple{}, td)

	tests := []struct {
		name      string
		child     iterator.DbIterator
		limit     int
		offset    int
		expectErr bool
	}{
		{
			name:      "Valid parameters",
			child:     child,
			limit:     5,
			offset:    2,
			expectErr: false,
		},
		{
			name:      "Zero limit",
			child:     child,
			limit:     0,
			offset:    0,
			expectErr: false,
		},
		{
			name:      "Zero offset",
			child:     child,
			limit:     10,
			offset:    0,
			expectErr: false,
		},
		{
			name:      "Nil child",
			child:     nil,
			limit:     5,
			offset:    2,
			expectErr: true,
		},
		{
			name:      "Negative limit",
			child:     child,
			limit:     -1,
			offset:    2,
			expectErr: true,
		},
		{
			name:      "Negative offset",
			child:     child,
			limit:     5,
			offset:    -1,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op, err := NewLimitOperator(tt.child, tt.limit, tt.offset)

			if tt.expectErr {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				if op != nil {
					t.Errorf("Expected nil operator when error occurs")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if op == nil {
					t.Errorf("Expected valid operator but got nil")
				}
				if op != nil {
					if op.limit != tt.limit {
						t.Errorf("Expected limit %d, got %d", tt.limit, op.limit)
					}
					if op.offset != tt.offset {
						t.Errorf("Expected offset %d, got %d", tt.offset, op.offset)
					}
					if op.child != tt.child {
						t.Errorf("Child iterator not set correctly")
					}
				}
			}
		})
	}
}

func TestLimitOperatorOpen(t *testing.T) {
	td := mustCreateLimitTupleDesc()

	tuples := []*tuple.Tuple{
		createLimitTestTuple(td, 1),
		createLimitTestTuple(td, 2),
		createLimitTestTuple(td, 3),
		createLimitTestTuple(td, 4),
		createLimitTestTuple(td, 5),
	}

	t.Run("Open with offset 0", func(t *testing.T) {
		child := newMockChildIterator(tuples, td)
		op, _ := NewLimitOperator(child, 3, 0)

		err := op.Open()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		if op.count != 0 {
			t.Errorf("Expected count 0 after open, got %d", op.count)
		}
		if child.index != -1 {
			t.Errorf("Expected child index -1 after open with offset 0, got %d", child.index)
		}
	})

	t.Run("Open with offset 2", func(t *testing.T) {
		child := newMockChildIterator(tuples, td)
		op, _ := NewLimitOperator(child, 2, 2)

		err := op.Open()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		if op.count != 0 {
			t.Errorf("Expected count 0 after open, got %d", op.count)
		}
		if child.index != 1 {
			t.Errorf("Expected child index 1 after offset 2, got %d", child.index)
		}
	})

	t.Run("Open with offset larger than available tuples", func(t *testing.T) {
		child := newMockChildIterator(tuples, td)
		op, _ := NewLimitOperator(child, 2, 10)

		err := op.Open()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		if child.index != 4 {
			t.Errorf("Expected child index 4 after consuming all tuples, got %d", child.index)
		}
	})

	t.Run("Open with child error", func(t *testing.T) {
		child := newMockChildIterator(tuples, td)
		child.hasError = true
		op, _ := NewLimitOperator(child, 2, 0)

		err := op.Open()
		if err == nil {
			t.Error("Expected error when child open fails")
		}
	})
}

func TestLimitOperatorIteration(t *testing.T) {
	td := mustCreateLimitTupleDesc()

	tuples := []*tuple.Tuple{
		createLimitTestTuple(td, 1),
		createLimitTestTuple(td, 2),
		createLimitTestTuple(td, 3),
		createLimitTestTuple(td, 4),
		createLimitTestTuple(td, 5),
	}

	t.Run("Limit 3 with no offset", func(t *testing.T) {
		child := newMockChildIterator(tuples, td)
		op, _ := NewLimitOperator(child, 3, 0)
		op.Open()

		count := 0
		for {
			hasNext, err := op.HasNext()
			if err != nil {
				t.Errorf("Unexpected error in HasNext: %v", err)
				break
			}
			if !hasNext {
				break
			}

			tup, err := op.Next()
			if err != nil {
				t.Errorf("Unexpected error in Next: %v", err)
				break
			}

			expectedValue := int64(count + 1)
			field, _ := tup.GetField(0)
			actualValue := field.(*types.IntField).Value
			if actualValue != expectedValue {
				t.Errorf("Expected value %d, got %d", expectedValue, actualValue)
			}
			count++
		}

		if count != 3 {
			t.Errorf("Expected 3 tuples, got %d", count)
		}
	})

	t.Run("Limit 2 with offset 2", func(t *testing.T) {
		child := newMockChildIterator(tuples, td)
		op, _ := NewLimitOperator(child, 2, 2)
		op.Open()

		count := 0
		expectedValues := []int64{3, 4}
		for {
			hasNext, err := op.HasNext()
			if err != nil {
				t.Errorf("Unexpected error in HasNext: %v", err)
				break
			}
			if !hasNext {
				break
			}

			tup, err := op.Next()
			if err != nil {
				t.Errorf("Unexpected error in Next: %v", err)
				break
			}

			expectedValue := expectedValues[count]
			field, _ := tup.GetField(0)
			actualValue := field.(*types.IntField).Value
			if actualValue != expectedValue {
				t.Errorf("Expected value %d, got %d", expectedValue, actualValue)
			}
			count++
		}

		if count != 2 {
			t.Errorf("Expected 2 tuples, got %d", count)
		}
	})

	t.Run("Limit larger than available tuples", func(t *testing.T) {
		child := newMockChildIterator(tuples, td)
		op, _ := NewLimitOperator(child, 10, 0)
		op.Open()

		count := 0
		for {
			hasNext, err := op.HasNext()
			if err != nil {
				t.Errorf("Unexpected error in HasNext: %v", err)
				break
			}
			if !hasNext {
				break
			}

			_, err = op.Next()
			if err != nil {
				t.Errorf("Unexpected error in Next: %v", err)
				break
			}
			count++
		}

		if count != 5 {
			t.Errorf("Expected 5 tuples (all available), got %d", count)
		}
	})
}

func TestLimitOperatorRewind(t *testing.T) {
	td := mustCreateLimitTupleDesc()

	tuples := []*tuple.Tuple{
		createLimitTestTuple(td, 1),
		createLimitTestTuple(td, 2),
		createLimitTestTuple(td, 3),
		createLimitTestTuple(td, 4),
		createLimitTestTuple(td, 5),
	}

	t.Run("Rewind after partial iteration", func(t *testing.T) {
		child := newMockChildIterator(tuples, td)
		op, _ := NewLimitOperator(child, 3, 1)
		op.Open()

		op.Next()

		err := op.Rewind()
		if err != nil {
			t.Errorf("Unexpected error in Rewind: %v", err)
		}

		if op.count != 0 {
			t.Errorf("Expected count 0 after rewind, got %d", op.count)
		}

		hasNext, _ := op.HasNext()
		if !hasNext {
			t.Error("Expected HasNext to return true after rewind")
		}

		tup, _ := op.Next()
		expectedValue := int64(2)
		field, _ := tup.GetField(0)
		actualValue := field.(*types.IntField).Value
		if actualValue != expectedValue {
			t.Errorf("Expected first value after rewind to be %d, got %d", expectedValue, actualValue)
		}
	})

	t.Run("Rewind with child error", func(t *testing.T) {
		child := newMockChildIterator(tuples, td)
		op, _ := NewLimitOperator(child, 3, 0)
		op.Open()

		child.hasError = true
		err := op.Rewind()
		if err == nil {
			t.Error("Expected error when child rewind fails")
		}
	})
}

func TestLimitOperatorErrorHandling(t *testing.T) {
	td := mustCreateLimitTupleDesc()

	t.Run("HasNext without open", func(t *testing.T) {
		child := newMockChildIterator([]*tuple.Tuple{}, td)
		op, _ := NewLimitOperator(child, 3, 0)

		_, err := op.HasNext()
		if err == nil {
			t.Error("Expected error when calling HasNext without opening")
		}
	})

	t.Run("Next without open", func(t *testing.T) {
		child := newMockChildIterator([]*tuple.Tuple{}, td)
		op, _ := NewLimitOperator(child, 3, 0)

		_, err := op.Next()
		if err == nil {
			t.Error("Expected error when calling Next without opening")
		}
	})

	t.Run("GetTupleDesc", func(t *testing.T) {
		child := newMockChildIterator([]*tuple.Tuple{}, td)
		op, _ := NewLimitOperator(child, 3, 0)

		result := op.GetTupleDesc()
		if result != td {
			t.Error("GetTupleDesc should return child's tuple description")
		}
	})

	t.Run("Close", func(t *testing.T) {
		child := newMockChildIterator([]*tuple.Tuple{}, td)
		op, _ := NewLimitOperator(child, 3, 0)
		op.Open()

		err := op.Close()
		if err != nil {
			t.Errorf("Unexpected error in Close: %v", err)
		}

		if child.isOpen {
			t.Error("Child iterator should be closed")
		}
	})
}

func TestLimitOperatorEdgeCases(t *testing.T) {
	td := mustCreateLimitTupleDesc()

	t.Run("Zero limit", func(t *testing.T) {
		tuples := []*tuple.Tuple{
			createLimitTestTuple(td, 1),
		}
		child := newMockChildIterator(tuples, td)
		op, _ := NewLimitOperator(child, 0, 0)
		op.Open()

		hasNext, _ := op.HasNext()
		if hasNext {
			t.Error("Expected no tuples with zero limit")
		}
	})

	t.Run("Empty child iterator", func(t *testing.T) {
		child := newMockChildIterator([]*tuple.Tuple{}, td)
		op, _ := NewLimitOperator(child, 5, 0)
		op.Open()

		hasNext, _ := op.HasNext()
		if hasNext {
			t.Error("Expected no tuples from empty child iterator")
		}
	})
}
