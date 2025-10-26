package common

import (
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
)

// BaseJoin provides common functionality for all join implementations.
// Fields are exported for testing purposes within the internal package.
type BaseJoin struct {
	LeftChildField   iterator.DbIterator
	RightChildField  iterator.DbIterator
	PredicateField   JoinPredicate
	StatsField       *JoinStatistics
	MatchBufferField *JoinMatchBuffer
	Initialized      bool
}

// NewBaseJoin creates a new base join with common initialization.
func NewBaseJoin(left, right iterator.DbIterator, pred JoinPredicate, stats *JoinStatistics) BaseJoin {
	return BaseJoin{
		LeftChildField:   left,
		RightChildField:  right,
		PredicateField:   pred,
		StatsField:       stats,
		MatchBufferField: NewJoinMatchBuffer(),
		Initialized:      false,
	}
}

// Close releases common resources.
func (bj *BaseJoin) Close() error {
	bj.MatchBufferField.Reset()
	bj.Initialized = false
	return nil
}

// IsInitialized checks if the join has been initialized.
func (bj *BaseJoin) IsInitialized() bool {
	return bj.Initialized
}

// SetInitialized marks the join as initialized.
func (bj *BaseJoin) SetInitialized() {
	bj.Initialized = true
}

// GetMatchFromBuffer returns next match if available, nil otherwise.
func (bj *BaseJoin) GetMatchFromBuffer() *tuple.Tuple {
	if bj.MatchBufferField.HasNext() {
		return bj.MatchBufferField.Next()
	}
	return nil
}

// ResetCommon resets common state for all join types.
func (bj *BaseJoin) ResetCommon() error {
	bj.MatchBufferField.Reset()
	return nil
}

// LeftChild returns the left child iterator
func (bj *BaseJoin) LeftChild() iterator.DbIterator {
	return bj.LeftChildField
}

// RightChild returns the right child iterator
func (bj *BaseJoin) RightChild() iterator.DbIterator {
	return bj.RightChildField
}

// Predicate returns the join predicate
func (bj *BaseJoin) Predicate() JoinPredicate {
	return bj.PredicateField
}

// Stats returns the join statistics
func (bj *BaseJoin) Stats() *JoinStatistics {
	return bj.StatsField
}

// MatchBuffer returns the match buffer
func (bj *BaseJoin) MatchBuffer() *JoinMatchBuffer {
	return bj.MatchBufferField
}
