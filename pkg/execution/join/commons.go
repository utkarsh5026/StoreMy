package join

import (
	"fmt"
	"sort"
	"storemy/pkg/iterator"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
)

// BaseJoin provides common functionality for all join implementations.
type BaseJoin struct {
	leftChild   iterator.DbIterator
	rightChild  iterator.DbIterator
	predicate   *JoinPredicate
	stats       *JoinStatistics
	matchBuffer *JoinMatchBuffer
	initialized bool
}

// NewBaseJoin creates a new base join with common initialization.
func NewBaseJoin(left, right iterator.DbIterator, pred *JoinPredicate, stats *JoinStatistics) BaseJoin {
	return BaseJoin{
		leftChild:   left,
		rightChild:  right,
		predicate:   pred,
		stats:       stats,
		matchBuffer: NewJoinMatchBuffer(),
		initialized: false,
	}
}

// Close releases common resources.
func (bj *BaseJoin) Close() error {
	bj.matchBuffer.Reset()
	bj.initialized = false
	return nil
}

// IsInitialized checks if the join has been initialized.
func (bj *BaseJoin) IsInitialized() bool {
	return bj.initialized
}

// SetInitialized marks the join as initialized.
func (bj *BaseJoin) SetInitialized() {
	bj.initialized = true
}

// GetMatchFromBuffer returns next match if available, nil otherwise.
func (bj *BaseJoin) GetMatchFromBuffer() *tuple.Tuple {
	if bj.matchBuffer.HasNext() {
		return bj.matchBuffer.Next()
	}
	return nil
}

// ResetCommon resets common state for all join types.
func (bj *BaseJoin) ResetCommon() error {
	bj.matchBuffer.Reset()
	return nil
}

// ExtractJoinKey extracts and stringifies the join key from a tuple.
func extractJoinKey(t *tuple.Tuple, fieldIndex int) (string, error) {
	field, err := t.GetField(fieldIndex)
	if err != nil || field == nil {
		return "", fmt.Errorf("invalid join key at field %d", fieldIndex)
	}
	return field.String(), nil
}

// CompareTuples compares two tuples at specified fields using the given operator.
func compareTuples(t1, t2 *tuple.Tuple, field1, field2 int, op primitives.Predicate) (bool, error) {
	f1, err := t1.GetField(field1)
	if err != nil || f1 == nil {
		return false, fmt.Errorf("invalid field %d in left tuple", field1)
	}

	f2, err := t2.GetField(field2)
	if err != nil || f2 == nil {
		return false, fmt.Errorf("invalid field %d in right tuple", field2)
	}

	return f1.Compare(op, f2)
}

// LoadAndSort loads all tuples from an iterator and sorts by field index.
func LoadAndSort(iter iterator.DbIterator, fieldIndex int) ([]*tuple.Tuple, error) {
	tuples, err := iterator.LoadAllTuples(iter)
	if err != nil {
		return nil, err
	}

	// Use Go's built-in slices.SortFunc (Go 1.21+) or sort.Slice
	sortTuplesByField(tuples, fieldIndex)
	return tuples, nil
}

// sortTuplesByField sorts tuples by comparing values at the specified field.
func sortTuplesByField(tuples []*tuple.Tuple, fieldIndex int) {
	// Using Go's sort.Slice with panic recovery for robustness
	defer func() {
		if r := recover(); r != nil {
			// Sorting failure shouldn't crash the system
		}
	}()

	sort.Slice(tuples, func(i, j int) bool {
		f1, err1 := tuples[i].GetField(fieldIndex)
		f2, err2 := tuples[j].GetField(fieldIndex)

		if err1 != nil || err2 != nil || f1 == nil || f2 == nil {
			return false
		}

		result, err := f1.Compare(primitives.LessThan, f2)
		return err == nil && result
	})
}

// CalculateDefaultBlockSize determines optimal block size based on available memory.
func calculateDefaultBlockSize(stats *JoinStatistics) int {
	const (
		defaultBlockSize = 100
		tuplesPerPage    = 100
	)

	if stats == nil || stats.MemorySize <= 0 {
		return defaultBlockSize
	}

	return stats.MemorySize * tuplesPerPage
}

// CombineAndBuffer combines matching tuples and adds to buffer.
func combineAndBuffer(buffer *JoinMatchBuffer, left, right *tuple.Tuple) error {
	combined, err := tuple.CombineTuples(left, right)
	if err != nil {
		return err
	}
	buffer.Add(combined)
	return nil
}
