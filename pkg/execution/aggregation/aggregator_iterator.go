package aggregation

import (
	"fmt"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

type AggregatorIterator struct {
	aggregator    GroupAggregator
	tupleDesc     *tuple.TupleDescription
	groups        []string
	currentIdx    int
	opened        bool
	nextTuple     *tuple.Tuple
	hasNextCalled bool
}

func NewAggregatorIterator(agg GroupAggregator) *AggregatorIterator {
	return &AggregatorIterator{
		aggregator: agg,
		tupleDesc:  agg.GetTupleDesc(),
		currentIdx: 0,
		opened:     false,
	}
}

func (iter *AggregatorIterator) Open() error {
	if iter.opened {
		return fmt.Errorf("iterator already opened")
	}

	iter.aggregator.RLock()
	defer iter.aggregator.RUnlock()

	iter.groups = iter.aggregator.GetGroups()

	iter.currentIdx = 0
	iter.opened = true
	iter.nextTuple = nil
	iter.hasNextCalled = false
	return nil
}

func (iter *AggregatorIterator) HasNext() (bool, error) {
	if !iter.opened {
		return false, fmt.Errorf("iterator not opened")
	}

	if !iter.hasNextCalled {
		var err error

		iter.nextTuple, err = iter.readNext()
		if err != nil {
			return false, fmt.Errorf("error reading next tuple: %v", err)
		}

		iter.hasNextCalled = true
	}

	return iter.nextTuple != nil, nil
}

func (iter *AggregatorIterator) readNext() (*tuple.Tuple, error) {
	if iter.currentIdx >= len(iter.groups) {
		return nil, nil // No more tuples
	}

	iter.aggregator.RLock()
	defer iter.aggregator.RUnlock()

	groupKey := iter.groups[iter.currentIdx]
	iter.currentIdx++

	aggValue, err := iter.aggregator.GetAggregateValue(groupKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get aggregate value: %v", err)
	}

	resultTuple := tuple.NewTuple(iter.tupleDesc)

	if iter.aggregator.GetGroupingField() == NoGrouping {
		// Single aggregate value
		if err := resultTuple.SetField(0, aggValue); err != nil {
			return nil, fmt.Errorf("failed to set aggregate field: %v", err)
		}
	} else {
		// Group value + aggregate value
		groupField := types.NewStringField(groupKey, len(groupKey))
		if err := resultTuple.SetField(0, groupField); err != nil {
			return nil, fmt.Errorf("failed to set group field: %v", err)
		}
		if err := resultTuple.SetField(1, aggValue); err != nil {
			return nil, fmt.Errorf("failed to set aggregate field: %v", err)
		}
	}

	return resultTuple, nil
}

func (iter *AggregatorIterator) Next() (*tuple.Tuple, error) {
	if !iter.opened {
		return nil, fmt.Errorf("iterator not opened")
	}

	if !iter.hasNextCalled {
		hasNext, err := iter.HasNext()
		if err != nil {
			return nil, err
		}
		if !hasNext {
			return nil, fmt.Errorf("no more tuples available")
		}
	}

	result := iter.nextTuple
	iter.nextTuple = nil
	iter.hasNextCalled = false

	if result == nil {
		return nil, fmt.Errorf("no more tuples available")
	}

	return result, nil
}

func (iter *AggregatorIterator) Rewind() error {
	if !iter.opened {
		return fmt.Errorf("iterator not opened")
	}

	iter.currentIdx = 0
	iter.nextTuple = nil
	iter.hasNextCalled = false
	return nil
}

func (iter *AggregatorIterator) Close() error {
	iter.opened = false
	iter.aggregator = nil
	iter.groups = nil
	iter.nextTuple = nil
	iter.hasNextCalled = false
	return nil
}

func (iter *AggregatorIterator) GetTupleDesc() *tuple.TupleDescription {
	return iter.tupleDesc
}
