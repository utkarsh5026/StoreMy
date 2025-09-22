package aggregation

import (
	"fmt"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"sync"
)

// BaseAggregator contains all common aggregation functionality
// This eliminates code duplication across all aggregator types
type BaseAggregator struct {
	gbField     int                     // Index of grouping field
	gbFieldType types.Type              // Type of grouping field
	aField      int                     // Index of field to aggregate
	op          AggregateOp             // Aggregation operation
	tupleDesc   *tuple.TupleDescription // Description of result tuples
	mutex       sync.RWMutex            // Protects concurrent access
	groups      map[string]bool         // Track which groups exist
	calculator  AggregateCalculator     // Type-specific logic
}

func NewBaseAggregator(gbField int, gbFieldType types.Type, aField int, op AggregateOp, calculator AggregateCalculator) (*BaseAggregator, error) {
	if err := calculator.ValidateOperation(op); err != nil {
		return nil, err
	}

	agg := &BaseAggregator{
		gbField:     gbField,
		gbFieldType: gbFieldType,
		aField:      aField,
		op:          op,
		groups:      make(map[string]bool),
		calculator:  calculator,
	}

	tupleDesc, err := agg.createTupleDesc()
	if err != nil {
		return nil, err
	}
	agg.tupleDesc = tupleDesc
	return agg, nil
}

func (ba *BaseAggregator) createTupleDesc() (*tuple.TupleDescription, error) {
	resultType := ba.calculator.GetResultType(ba.op)

	if ba.gbField == NoGrouping {
		return tuple.NewTupleDesc(
			[]types.Type{resultType},
			[]string{ba.op.String()},
		)
	} else {
		return tuple.NewTupleDesc(
			[]types.Type{ba.gbFieldType, resultType},
			[]string{"group", ba.op.String()},
		)
	}
}

func (ba *BaseAggregator) GetGroups() []string {
	groups := make([]string, 0, len(ba.groups))
	for groupKey := range ba.groups {
		groups = append(groups, groupKey)
	}
	return groups
}

func (ba *BaseAggregator) GetAggregateValue(groupKey string) (types.Field, error) {
	return ba.calculator.GetFinalValue(groupKey)
}

func (ba *BaseAggregator) GetTupleDesc() *tuple.TupleDescription {
	return ba.tupleDesc
}

func (ba *BaseAggregator) GetGroupingField() int {
	return ba.gbField
}

func (ba *BaseAggregator) RLock() {
	ba.mutex.RLock()
}

func (ba *BaseAggregator) RUnlock() {
	ba.mutex.RUnlock()
}

func (ba *BaseAggregator) Iterator() iterator.DbIterator {
	return NewAggregatorIterator(ba)
}

// Merge processes a new tuple into the aggregate - common logic for all types
func (ba *BaseAggregator) Merge(tup *tuple.Tuple) error {
	ba.mutex.Lock()
	defer ba.mutex.Unlock()

	groupKey, err := ba.extractGroupKey(tup)
	if err != nil {
		return err
	}

	aggField, err := tup.GetField(ba.aField)
	if err != nil {
		return fmt.Errorf("failed to get aggregate field: %v", err)
	}

	if !ba.groups[groupKey] {
		ba.calculator.InitializeGroup(groupKey)
		ba.groups[groupKey] = true
	}

	return ba.calculator.UpdateAggregate(groupKey, aggField)
}

func (ba *BaseAggregator) extractGroupKey(tup *tuple.Tuple) (string, error) {
	if ba.gbField == NoGrouping {
		return "NO_GROUPING", nil
	}

	groupField, err := tup.GetField(ba.gbField)
	if err != nil {
		return "", fmt.Errorf("failed to get grouping field: %v", err)
	}

	return groupField.String(), nil
}
