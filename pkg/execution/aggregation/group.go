package aggregation

import (
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// GroupAggregator interface defines what each aggregator must provide to the generic iterator
type GroupAggregator interface {
	// GetGroups returns all group keys for iteration
	GetGroups() []string

	// GetAggregateValue returns the aggregated field value for a specific group
	GetAggregateValue(groupKey string) (types.Field, error)

	// GetTupleDesc returns the tuple description for results
	GetTupleDesc() *tuple.TupleDescription

	// GetGroupingField returns the grouping field index (-1 for no grouping)
	GetGroupingField() int

	// Lock/Unlock for thread safety during iteration
	RLock()
	RUnlock()
}
