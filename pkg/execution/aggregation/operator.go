package aggregation

import (
	"fmt"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// AggregateOperator is the main aggregation operator that computes aggregates
// It uses composition to manage iteration state and delegates to the underlying aggregator
type AggregateOperator struct {
	source        iterator.DbIterator     // Source of tuples
	aField        int                     // Field to aggregate
	gField        int                     // Grouping field
	op            AggregateOp             // Aggregation operation
	aggregator    Aggregator              // Underlying aggregator
	aggIterator   iterator.DbIterator     // Iterator over results
	tupleDesc     *tuple.TupleDescription // Result tuple description
	opened        bool
	nextTuple     *tuple.Tuple // Cache for hasNext/next pattern
	hasNextCalled bool
}

// NewAggregateOperator creates a new aggregate operator using composition
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
		aggOp.aggregator, _ = NewIntAggregator(gField, gbFieldType, aField, op)

	case types.BoolType:
		aggOp.aggregator, _ = NewBooleanAggregator(gField, gbFieldType, aField, op)

	case types.StringType:
		var err error
		aggOp.aggregator, err = NewStringAggregator(gField, gbFieldType, aField, op)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported field type for aggregation: %v", aggFieldType)
	}

	aggOp.tupleDesc = aggOp.aggregator.GetTupleDesc()
	return aggOp, nil
}

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

func (agg *AggregateOperator) GetTupleDesc() *tuple.TupleDescription {
	return agg.tupleDesc
}

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

func (agg *AggregateOperator) Next() (*tuple.Tuple, error) {
	if !agg.opened {
		return nil, fmt.Errorf("aggregate operator not opened")
	}

	// If hasNext hasn't been called, call it to cache the next tuple
	if !agg.hasNextCalled {
		hasNext, err := agg.HasNext()
		if err != nil {
			return nil, err
		}
		if !hasNext {
			return nil, fmt.Errorf("no more tuples available")
		}
	}

	// Return the cached tuple and reset the cache
	result := agg.nextTuple
	agg.nextTuple = nil
	agg.hasNextCalled = false

	if result == nil {
		return nil, fmt.Errorf("no more tuples available")
	}

	return result, nil
}

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
