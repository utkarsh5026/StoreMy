package aggregation

import (
	"fmt"
	"storemy/pkg/execution/aggregation/internal/calculators"
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
	source         iterator.DbIterator
	aggregateField int
	groupByField   int
	op             AggregateOp
	aggregator     Aggregator
	aggIterator    iterator.DbIterator
	tupleDesc      *tuple.TupleDescription
	opened         bool
	nextTuple      *tuple.Tuple
	hasNextCalled  bool
}

// NewAggregateOperator creates a new aggregate operator with the specified configuration.
// The operator will aggregate values from aggregateField, optionally grouping by groupByField.
func NewAggregateOperator(source iterator.DbIterator, aggregateField, groupByField int, op AggregateOp) (*AggregateOperator, error) {
	if err := validateInputs(source, aggregateField, groupByField); err != nil {
		return nil, err
	}

	sourceDesc := source.GetTupleDesc()
	aggOp := &AggregateOperator{
		source:         source,
		aggregateField: aggregateField,
		groupByField:   groupByField,
		op:             op,
		opened:         false,
	}

	aggFieldType := sourceDesc.Types[aggregateField]
	var gbFieldType types.Type
	if groupByField != NoGrouping {
		gbFieldType = sourceDesc.Types[groupByField]
	}

	var err error
	aggOp.aggregator, err = createAggregator(aggFieldType, groupByField, gbFieldType, aggregateField, op)
	if err != nil {
		return nil, err
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
func (agg *AggregateOperator) GetTupleDesc() *tuple.TupleDescription {
	return agg.tupleDesc
}

// Rewind resets the aggregate result iterator to the beginning.
// This allows re-reading the aggregate results without recomputing them.
func (agg *AggregateOperator) Rewind() error {
	if err := agg.ensureOpened(); err != nil {
		return err
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
// 3. For non-grouped aggregates with no input tuples, initializes a default group
// 4. Creates an iterator over the computed results
// 5. Prepares the operator for result iteration
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

	tupleCount := 0
	iterator.ForEach(agg.source, func(t *tuple.Tuple) error {
		tupleCount++
		if err := agg.aggregator.Merge(t); err != nil {
			return fmt.Errorf("error merging tuple: %v", err)
		}
		return nil
	})

	// For non-grouped aggregates (e.g., COUNT(*) with no GROUP BY),
	// we need to return a single row even if there are no input tuples.
	if tupleCount == 0 && agg.groupByField == NoGrouping {
		if err := agg.aggregator.InitializeDefault(); err != nil {
			return fmt.Errorf("failed to initialize default group: %v", err)
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
func (agg *AggregateOperator) HasNext() (bool, error) {
	if err := agg.ensureOpened(); err != nil {
		return false, err
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
func (agg *AggregateOperator) Next() (*tuple.Tuple, error) {
	if err := agg.ensureOpened(); err != nil {
		return nil, err
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

// ensureOpened checks if the operator is opened and returns an error if not
func (agg *AggregateOperator) ensureOpened() error {
	if !agg.opened {
		return fmt.Errorf("aggregate operator not opened")
	}
	return nil
}

// validateInputs validates the constructor parameters
func validateInputs(source iterator.DbIterator, aggregateField, groupByField int) error {
	if source == nil {
		return fmt.Errorf("source iterator cannot be nil")
	}

	sourceDesc := source.GetTupleDesc()
	if sourceDesc == nil {
		return fmt.Errorf("source tuple description cannot be nil")
	}

	if aggregateField < 0 || aggregateField >= len(sourceDesc.Types) {
		return fmt.Errorf("invalid aggregate field index: %d", aggregateField)
	}

	if groupByField != NoGrouping && (groupByField < 0 || groupByField >= len(sourceDesc.Types)) {
		return fmt.Errorf("invalid group field index: %d", groupByField)
	}

	return nil
}

// createAggregator creates the appropriate type-specific aggregator based on the field type
func createAggregator(fieldType types.Type, groupByField int, gbFieldType types.Type, aggregateField int, op AggregateOp) (Aggregator, error) {
	switch fieldType {
	case types.IntType:
		return calculators.NewIntAggregator(groupByField, gbFieldType, aggregateField, op)
	case types.BoolType:
		return calculators.NewBooleanAggregator(groupByField, gbFieldType, aggregateField, op)
	case types.StringType:
		return calculators.NewStringAggregator(groupByField, gbFieldType, aggregateField, op)
	case types.FloatType:
		return calculators.NewFloatAggregator(groupByField, gbFieldType, aggregateField, op)
	default:
		return nil, fmt.Errorf("unsupported field type for aggregation: %v", fieldType)
	}
}
