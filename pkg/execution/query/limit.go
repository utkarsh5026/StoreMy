package query

import (
	"fmt"
	"storemy/pkg/iterator"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
)

// LimitOperator implements SQL LIMIT and OFFSET functionality.
// It restricts the number of tuples returned by a query and allows
// skipping a specified number of tuples from the beginning.
//
// Example: SELECT * FROM users LIMIT 10 OFFSET 5
// Returns 10 tuples starting from the 6th tuple.
type LimitOperator struct {
	*iterator.UnaryOperator
	limit  primitives.RowID // Maximum number of tuples to return
	offset primitives.RowID // Number of tuples to skip from the beginning
	count  primitives.RowID // Number of tuples returned so far
}

// NewLimitOperator creates a new LimitOperator instance.
//
// Parameters:
//   - child: The underlying iterator that provides tuples
//   - limit: Maximum number of tuples to return (must be non-negative)
//   - offset: Number of tuples to skip from the beginning (must be non-negative)
//
// Returns:
//   - *LimitOperator: The initialized limit operator
//   - error: If child is nil, or limit/offset are negative
func NewLimitOperator(child iterator.DbIterator, limit, offset primitives.RowID) (*LimitOperator, error) {
	if child == nil {
		return nil, fmt.Errorf("child operator cannot be nil")
	}

	lo := &LimitOperator{
		limit:  limit,
		offset: offset,
	}

	unaryOp, err := iterator.NewUnaryOperator(child, lo.readNext)
	if err != nil {
		return nil, err
	}
	lo.UnaryOperator = unaryOp

	return lo, nil
}

// Open initializes the limit operator and skips the offset tuples.
// This method must be called before fetching any tuples.
//
// Returns:
//   - error: If opening the child operator fails or if an error occurs while skipping offset tuples
func (lo *LimitOperator) Open() error {
	if err := lo.UnaryOperator.Open(); err != nil {
		return err
	}

	lo.count = 0
	return lo.skipOffset()
}

// readNext retrieves the next tuple within the limit range.
// It returns nil when the limit has been reached.
//
// Returns:
//   - *tuple.Tuple: The next tuple, or nil if limit is reached or no more tuples available
//   - error: If an error occurs while fetching the next tuple
func (lo *LimitOperator) readNext() (*tuple.Tuple, error) {
	if lo.count >= lo.limit {
		return nil, nil
	}

	t, err := lo.FetchNext()
	if err != nil || t == nil {
		return t, err
	}

	lo.count++
	return t, nil
}

// Rewind resets the limit operator to its initial state.
// After rewinding, the operator will skip offset tuples again
// and start returning tuples from the beginning.
//
// Returns:
//   - error: If rewinding the child operator fails or if an error occurs while skipping offset tuples
func (lo *LimitOperator) Rewind() error {
	lo.count = 0

	if err := lo.UnaryOperator.Rewind(); err != nil {
		return err
	}

	return lo.skipOffset()
}

// skipOffset advances the child operator by the number of tuples specified
// by the offset value, discarding those tuples. This prepares the limit
// operator so the next retrieved tuple is the first one after the offset.
// If there are fewer tuples than the offset value, it stops early.
//
// Returns:
//   - error: If an error occurs while fetching the next tuple from the child operator.
func (lo *LimitOperator) skipOffset() error {
	var i primitives.RowID
	for i = 0; i < lo.offset; i++ {
		t, err := lo.FetchNext()
		if err != nil {
			return err
		}
		if t == nil {
			break
		}
	}
	return nil
}
