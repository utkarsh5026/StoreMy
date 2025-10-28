package query

import (
	"fmt"
	"sort"
	"storemy/pkg/iterator"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
)

// Sort operator orders tuples by a specified field in ascending or descending order.
//
// Implementation:
//   - Materializes all tuples from input (blocking operator)
//   - Sorts tuples in memory based on field value comparison
//   - Streams sorted tuples in order
//   - Memory usage: O(n) where n = number of tuples
//
// Performance Characteristics:
//   - Time: O(n log n) for sorting, where n = tuples
//   - Space: O(n) to store all tuples
//   - Blocking: Must read all input before producing first output
type Sort struct {
	base         *iterator.BaseIterator
	child        iterator.DbIterator
	sorted       *iterator.SliceIterator[*tuple.Tuple]
	sortField    primitives.ColumnID // Index of field to sort by
	ascending    bool                // Sort direction: true = ASC, false = DESC
	opened       bool
	materialized bool // Flag to track if tuples have been materialized
}

// NewSort creates a new Sort operator that orders tuples by the specified field.
//
// Parameters:
//   - child: Input iterator providing tuples to sort
//   - sortField: Index of field to sort by (0-based)
//   - ascending: true for ASC, false for DESC
//
// Returns:
//   - *Sort operator ready to be opened and iterated
//   - error if parameters are invalid
func NewSort(child iterator.DbIterator, sortField primitives.ColumnID, ascending bool) (*Sort, error) {
	if child == nil {
		return nil, fmt.Errorf("child operator cannot be nil")
	}

	td := child.GetTupleDesc()
	if td == nil {
		return nil, fmt.Errorf("child operator has nil tuple descriptor")
	}

	if sortField >= td.NumFields() {
		return nil, fmt.Errorf("sort field index %d out of bounds (schema has %d fields)",
			sortField, td.NumFields())
	}

	s := &Sort{
		child:     child,
		sortField: sortField,
		ascending: ascending,
	}

	s.base = iterator.NewBaseIterator(s.readNext)
	return s, nil
}

// materializeTuples reads all tuples from source and sorts them.
// This is called once during Open() to prepare the sorted data.
func (s *Sort) materializeTuples() error {
	if s.materialized {
		return nil
	}

	tuples := make([]*tuple.Tuple, 0)
	for {
		hasNext, err := s.child.HasNext()
		if err != nil {
			return fmt.Errorf("error checking if child has next: %w", err)
		}
		if !hasNext {
			break
		}

		t, err := s.child.Next()
		if err != nil {
			return fmt.Errorf("error fetching tuple from source: %w", err)
		}

		if t == nil {
			break
		}

		tuples = append(tuples, t)
	}

	if err := s.sortTuples(tuples); err != nil {
		return fmt.Errorf("error sorting tuples: %w", err)
	}

	s.sorted = iterator.NewSliceIterator(tuples)
	s.sorted.Open()
	s.materialized = true
	return nil
}

// sortTuples sorts the slice of tuples by the sort field using comparison.
func (s *Sort) sortTuples(tuples []*tuple.Tuple) error {
	var sortErr error

	sort.Slice(tuples, func(i, j int) bool {
		if sortErr != nil {
			return false
		}

		field1, err := tuples[i].GetField(s.sortField)
		if err != nil || field1 == nil {
			sortErr = fmt.Errorf("failed to get sort field from tuple %d: %w", i, err)
			return false
		}

		field2, err := tuples[j].GetField(s.sortField)
		if err != nil || field2 == nil {
			sortErr = fmt.Errorf("failed to get sort field from tuple %d: %w", j, err)
			return false
		}

		var lessThan bool
		if s.ascending {
			lessThan, err = field1.Compare(primitives.LessThan, field2)
		} else {
			lessThan, err = field2.Compare(primitives.LessThan, field1)
		}

		if err != nil {
			sortErr = fmt.Errorf("failed to compare fields: %w", err)
			return false
		}

		return lessThan
	})

	return sortErr
}

// readNext returns the next tuple from the sorted slice.
func (s *Sort) readNext() (*tuple.Tuple, error) {
	if !s.materialized {
		if err := s.materializeTuples(); err != nil {
			return nil, err
		}
	}

	if s.sorted.Remaining() == 0 {
		return nil, nil
	}

	return s.sorted.Next()
}

// Open initializes the Sort operator and materializes all tuples.
//
// This is a blocking operation that reads all input tuples and sorts them
// before returning. Must be called before HasNext/Next can be used.
func (s *Sort) Open() error {
	if err := s.child.Open(); err != nil {
		return fmt.Errorf("failed to open child operator: %w", err)
	}

	s.opened = true
	s.materialized = false

	s.base.MarkOpened()
	return nil
}

// Close releases resources held by the Sort operator and its child.
func (s *Sort) Close() error {
	s.opened = false
	s.materialized = false

	if s.sorted != nil {
		s.sorted.Close()
	}

	if err := s.child.Close(); err != nil {
		return fmt.Errorf("failed to close source operator: %w", err)
	}

	return s.base.Close()
}

// HasNext checks if there are more sorted tuples available.
func (s *Sort) HasNext() (bool, error) {
	if !s.opened {
		return false, fmt.Errorf("sort operator not opened")
	}
	return s.base.HasNext()
}

// Next returns the next sorted tuple.
func (s *Sort) Next() (*tuple.Tuple, error) {
	if !s.opened {
		return nil, fmt.Errorf("sort operator not opened")
	}
	return s.base.Next()
}

// Rewind resets the Sort operator to the beginning of the sorted results.
// Does not re-sort; just resets the read position.
func (s *Sort) Rewind() error {
	if !s.opened {
		return fmt.Errorf("sort operator not opened")
	}

	if s.sorted != nil {
		s.sorted.Rewind()
	}

	return s.base.Rewind()
}

// GetTupleDesc returns the tuple descriptor (schema) from the source.
// Sort does not modify the schema, only the order of tuples.
func (s *Sort) GetTupleDesc() *tuple.TupleDescription {
	return s.child.GetTupleDesc()
}
