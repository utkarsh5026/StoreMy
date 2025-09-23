package aggregation

import (
	"fmt"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// AggregateOperator is the main aggregation operator that computes aggregates over tuples.
//
// The operator follows a two-phase execution model:
// 1. Open phase: Consumes all input tuples and builds aggregation state
// 2. Iteration phase: Returns computed aggregate results
type AggregateOperator struct {
	source        iterator.DbIterator     // Source iterator providing input tuples
	aField        int                     // Index of the field to aggregate on
	gField        int                     // Index of the grouping field (NoGrouping if no grouping)
	op            AggregateOp             // Type of aggregation operation to perform
	aggregator    Aggregator              // Type-specific aggregator that computes results
	aggIterator   iterator.DbIterator     // Iterator over computed aggregate results
	tupleDesc     *tuple.TupleDescription // Schema description of result tuples
	opened        bool                    // Tracks whether the operator has been opened
	nextTuple     *tuple.Tuple            // Cached tuple for hasNext/next pattern
	hasNextCalled bool                    // Tracks if hasNext was called before next
}

// NewAggregateOperator creates a new aggregate operator with the specified configuration.
// The operator will aggregate values from aField, optionally grouping by gField.
//
// Parameters:
//   - source: The source iterator providing input tuples (must not be nil)
//   - aField: Index of the field to aggregate (must be valid field index)
//   - gField: Index of the grouping field, or NoGrouping for no grouping
//   - op: The aggregation operation to perform (COUNT, SUM, AVG, MIN, MAX)
//
// Returns:
//   - *AggregateOperator: Configured aggregate operator ready for use
//   - error: Error if parameters are invalid or aggregator creation fails
func NewAggregateOperator(source iterator.DbIterator, aField, gField int, op AggregateOp) (*AggregateOperator, error) {
	if source == nil {
		return nil, fmt.Errorf("source iterator cannot be nil")
	}

	sourceDesc := source.GetTupleDesc()
	if sourceDesc == nil {
		return nil, fmt.Errorf("source tuple description cannot be nil")
	}

	if aField < 0 || aField >= len(sourceDesc.Types) {
		return nil, fmt.Errorf("invalid aggregate field index: %d", aField)
	}

	if gField != NoGrouping && (gField < 0 || gField >= len(sourceDesc.Types)) {
		return nil, fmt.Errorf("invalid group field index: %d", gField)
	}

	aggOp := &AggregateOperator{
		source: source,
		aField: aField,
		gField: gField,
		op:     op,
		opened: false,
	}

	aggFieldType := sourceDesc.Types[aField]
	var gbFieldType types.Type
	if gField != NoGrouping {
		gbFieldType = sourceDesc.Types[gField]
	}

	switch aggFieldType {
	case types.IntType:
		var err error
		aggOp.aggregator, err = NewIntAggregator(gField, gbFieldType, aField, op)
		if err != nil {
			return nil, err
		}

	case types.BoolType:
		var err error
		aggOp.aggregator, err = NewBooleanAggregator(gField, gbFieldType, aField, op)
		if err != nil {
			return nil, err
		}

	case types.StringType:
		var err error
		aggOp.aggregator, err = NewStringAggregator(gField, gbFieldType, aField, op)
		if err != nil {
			return nil, err
		}

	case types.FloatType:
		var err error
		aggOp.aggregator, err = NewFloatAggregator(gField, gbFieldType, aField, op)
		if err != nil {
			return nil, err
		}

	default:
		return nil, fmt.Errorf("unsupported field type for aggregation: %v", aggFieldType)
	}

	aggOp.tupleDesc = aggOp.aggregator.GetTupleDesc()
	return aggOp, nil
}

// Close releases all resources associated with the aggregate operator.
// This includes closing the source iterator, aggregate result iterator,
// and resetting internal state.
//
// Returns:
//   - error: Always returns nil, but maintains interface compatibility
//
// Note: Close can be called multiple times safely
func (agg *AggregateOperator) Close() error {
	if agg.source != nil {
		agg.source.Close()
	}
	if agg.aggIterator != nil {
		agg.aggIterator.Close()
	}

	agg.opened = false
	agg.nextTuple = nil
	agg.hasNextCalled = false

	return nil
}

// GetTupleDesc returns the schema description of the result tuples.
//
// Returns:
//   - *tuple.TupleDescription: Schema of result tuples
func (agg *AggregateOperator) GetTupleDesc() *tuple.TupleDescription {
	return agg.tupleDesc
}

// Rewind resets the aggregate result iterator to the beginning.
// This allows re-reading the aggregate results without recomputing them.
//
// Returns:
//   - error: Error if operator is not opened or rewind fails
//
// Note: Rewind does not recompute aggregates, only resets result iteration
func (agg *AggregateOperator) Rewind() error {
	if !agg.opened {
		return fmt.Errorf("aggregate operator not opened")
	}

	agg.nextTuple = nil
	agg.hasNextCalled = false

	if agg.aggIterator != nil {
		return agg.aggIterator.Rewind()
	}

	return nil
}

// Open initializes the aggregate operator and computes all aggregate results.
// This method:
// 1. Opens the source iterator
// 2. Consumes all input tuples and feeds them to the aggregator
// 3. Creates an iterator over the computed results
// 4. Prepares the operator for result iteration
//
// Returns:
//   - error: Error if operator is already opened, source fails, or aggregation fails
func (agg *AggregateOperator) Open() error {
	if agg.opened {
		return fmt.Errorf("aggregate operator already opened")
	}

	if err := agg.source.Open(); err != nil {
		return fmt.Errorf("failed to open source iterator: %v", err)
	}

	for {
		hasNext, err := agg.source.HasNext()
		if err != nil {
			return fmt.Errorf("error checking child iterator: %v", err)
		}
		if !hasNext {
			break
		}

		tup, err := agg.source.Next()
		if err != nil {
			return fmt.Errorf("error reading from child iterator: %v", err)
		}

		if err := agg.aggregator.Merge(tup); err != nil {
			return fmt.Errorf("error merging tuple: %v", err)
		}
	}

	agg.aggIterator = agg.aggregator.Iterator()
	if err := agg.aggIterator.Open(); err != nil {
		return fmt.Errorf("failed to open aggregate iterator: %v", err)
	}

	agg.opened = true
	agg.nextTuple = nil
	agg.hasNextCalled = false

	return nil
}

// HasNext checks if there are more aggregate result tuples available.
// Uses internal caching to support the hasNext/next pattern efficiently.
//
// Returns:
//   - bool: True if more tuples are available, false otherwise
//   - error: Error if operator is not opened or reading fails
func (agg *AggregateOperator) HasNext() (bool, error) {
	if !agg.opened {
		return false, fmt.Errorf("aggregate operator not opened")
	}

	if !agg.hasNextCalled {
		var err error
		agg.nextTuple, err = agg.readNext()
		if err != nil {
			return false, fmt.Errorf("error reading next tuple: %v", err)
		}
		agg.hasNextCalled = true
	}

	return agg.nextTuple != nil, nil
}

// Next returns the next aggregate result tuple.
// Must be called after HasNext() returns true, or will automatically
// check for tuple availability.
//
// Returns:
//   - *tuple.Tuple: Next aggregate result tuple
//   - error: Error if operator not opened, no tuples available, or reading fails
func (agg *AggregateOperator) Next() (*tuple.Tuple, error) {
	if !agg.opened {
		return nil, fmt.Errorf("aggregate operator not opened")
	}

	if !agg.hasNextCalled {
		hasNext, err := agg.HasNext()
		if err != nil {
			return nil, err
		}
		if !hasNext {
			return nil, fmt.Errorf("no more tuples available")
		}
	}

	result := agg.nextTuple
	agg.nextTuple = nil
	agg.hasNextCalled = false

	if result == nil {
		return nil, fmt.Errorf("no more tuples available")
	}

	return result, nil
}

// readNext is an internal helper method that reads the next tuple from
// the aggregate result iterator.
//
// Returns:
//   - *tuple.Tuple: Next tuple from aggregate iterator, or nil if no more tuples
//   - error: Error if reading from aggregate iterator fails
func (agg *AggregateOperator) readNext() (*tuple.Tuple, error) {
	if agg.aggIterator == nil {
		return nil, nil
	}

	hasNext, err := agg.aggIterator.HasNext()
	if err != nil {
		return nil, err
	}
	if !hasNext {
		return nil, nil
	}

	return agg.aggIterator.Next()
}
