package aggregation

import (
	"fmt"
	"math"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"sync"
)

// IntAggregator handles aggregation over integer fields
// It maintains internal state for each group and supports all aggregate operations
type IntegerAggregator struct {
	groupByField   int
	groupFieldType types.Type
	aggrField      int
	op             AggregateOp
	groupToAgg     map[string]int32
	groupToCount   map[string]int32
	tupleDesc      *tuple.TupleDescription
	mutex          sync.RWMutex
}

// NewIntAggregator creates a new integer aggregator
func NewIntAggregator(gbField int, gbFieldType types.Type, aField int, op AggregateOp) (*IntegerAggregator, error) {
	agg := &IntegerAggregator{
		groupByField:   gbField,
		groupFieldType: gbFieldType,
		aggrField:      aField,
		op:             op,
		groupToAgg:     make(map[string]int32),
		groupToCount:   make(map[string]int32),
	}

	td, err := agg.createTupleDesc()
	if err != nil {
		return nil, fmt.Errorf("Error creating IntegerAggregator %v", err)
	}
	agg.tupleDesc = td
	return agg, nil
}

func (ia *IntegerAggregator) createTupleDesc() (*tuple.TupleDescription, error) {
	if ia.groupByField == NoGrouping {
		return tuple.NewTupleDesc(
			[]types.Type{types.IntType},
			[]string{ia.op.String()},
		)
	}

	return tuple.NewTupleDesc(
		[]types.Type{ia.groupFieldType, types.IntType},
		[]string{"group", ia.op.String()},
	)
}

func (ia *IntegerAggregator) GetTupleDesc() *tuple.TupleDescription {
	return ia.tupleDesc
}

func (ia *IntegerAggregator) Merge(tup *tuple.Tuple) error {
	ia.mutex.Lock()
	defer ia.mutex.Unlock()

	var groupKey string

	if ia.groupByField == NoGrouping {
		groupKey = "NO_GROUPING"
	} else {
		groupField, err := tup.GetField(ia.groupByField)
		if err != nil {
			return fmt.Errorf("failed to get grouping field: %v", err)
		}
		groupKey = groupField.String()
	}

	aggField, err := tup.GetField(ia.aggrField)
	if err != nil {
		return fmt.Errorf("failed to get aggregate field: %v", err)
	}

	intField, ok := aggField.(*types.IntField)
	if !ok {
		return fmt.Errorf("aggregate field is not an integer")
	}

	aggValue := intField.Value
	ia.initializeGroupIfNeeded(groupKey)
	return ia.updateAggregate(groupKey, aggValue)
}

func (ia *IntegerAggregator) getInitValue() int32 {
	switch ia.op {
	case Min:
		return math.MaxInt32
	case Max:
		return math.MinInt32
	case Sum, Avg, Count:
		return 0
	default:
		return 0
	}
}

func (ia *IntegerAggregator) updateAggregate(groupKey string, aggValue int32) error {
	currentAgg := ia.groupToAgg[groupKey]

	switch ia.op {
	case Min:
		if aggValue < currentAgg {
			ia.groupToAgg[groupKey] = aggValue
		}

	case Max:
		if aggValue > currentAgg {
			ia.groupToAgg[groupKey] = aggValue
		}

	case Sum:
		ia.groupToAgg[groupKey] = currentAgg + aggValue

	case Avg:
		ia.groupToAgg[groupKey] = currentAgg + aggValue
		ia.groupToCount[groupKey]++

	case Count:
		ia.groupToAgg[groupKey]++

	default:
		return fmt.Errorf("unsupported operation: %v", ia.op)
	}

	return nil
}

// initializeGroupIfNeeded initializes a new group with default values if it doesn't exist.
func (ia *IntegerAggregator) initializeGroupIfNeeded(groupKey string) {
	if _, exists := ia.groupToAgg[groupKey]; exists {
		return
	}

	ia.groupToAgg[groupKey] = ia.getInitValue()

	if ia.op == Avg {
		ia.groupToCount[groupKey] = 0
	}
}
