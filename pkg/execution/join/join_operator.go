package join

import (
	"fmt"
	"storemy/pkg/execution/query"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
	"sync"
)

type JoinOperator struct {
	base        *query.BaseIterator
	leftChild   iterator.DbIterator
	rightChild  iterator.DbIterator
	predicate   *JoinPredicate
	tupleDesc   *tuple.TupleDescription
	algorithm   JoinAlgorithm
	strategy    *JoinStrategy
	initialized bool
	mutex       sync.RWMutex
}

// NewJoinOperator creates a new join operator with automatic algorithm selection
func NewJoinOperator(predicate *JoinPredicate, leftChild, rightChild iterator.DbIterator) (*JoinOperator, error) {
	if err := validateInputs(predicate, leftChild, rightChild); err != nil {
		return nil, err
	}

	combinedTupleDesc, err := createCombinedSchema(leftChild, rightChild)
	if err != nil {
		return nil, err
	}

	jo := &JoinOperator{
		leftChild:  leftChild,
		rightChild: rightChild,
		predicate:  predicate,
		tupleDesc:  combinedTupleDesc,
	}

	jo.base = query.NewBaseIterator(jo.readNext)
	return jo, nil
}

func (jo *JoinOperator) Open() error {
	jo.mutex.Lock()
	defer jo.mutex.Unlock()

	if jo.initialized {
		return nil
	}

	if err := jo.leftChild.Open(); err != nil {
		return fmt.Errorf("failed to open left child: %v", err)
	}
	if err := jo.rightChild.Open(); err != nil {
		return fmt.Errorf("failed to open right child: %v", err)
	}

	stats, err := GetStatistics(jo.leftChild, jo.rightChild)
	if err != nil {
		stats = &JoinStatistics{
			LeftCardinality:  1000,
			RightCardinality: 1000,
			LeftSize:         10,
			RightSize:        10,
			MemorySize:       100,
			Selectivity:      0.1,
		}
	}

	jo.strategy = NewJoinStrategy(jo.leftChild, jo.rightChild, jo.predicate, stats)
	jo.algorithm, err = jo.strategy.SelectBestAlgorithm(jo.predicate)
	if err != nil {
		return err
	}

	if err := jo.algorithm.Initialize(); err != nil {
		return fmt.Errorf("failed to initialize join algorithm: %v", err)
	}

	jo.initialized = true
	jo.base.MarkOpened()
	return nil
}

func (jo *JoinOperator) readNext() (*tuple.Tuple, error) {
	jo.mutex.RLock()
	defer jo.mutex.RUnlock()

	if !jo.initialized {
		return nil, fmt.Errorf("join operator not initialized")
	}

	return jo.algorithm.Next()
}

func (jo *JoinOperator) Rewind() error {
	jo.mutex.Lock()
	defer jo.mutex.Unlock()

	if err := jo.leftChild.Rewind(); err != nil {
		return err
	}
	if err := jo.rightChild.Rewind(); err != nil {
		return err
	}
	if err := jo.algorithm.Reset(); err != nil {
		return err
	}

	jo.base.ClearCache()
	return nil
}

func (jo *JoinOperator) Close() error {
	jo.mutex.Lock()
	defer jo.mutex.Unlock()

	if jo.leftChild != nil {
		jo.leftChild.Close()
	}

	if jo.rightChild != nil {
		jo.rightChild.Close()
	}

	if jo.algorithm != nil {
		jo.algorithm.Close()
	}

	jo.initialized = false
	return jo.base.Close()
}

func (jo *JoinOperator) GetTupleDesc() *tuple.TupleDescription {
	return jo.tupleDesc
}

func (jo *JoinOperator) HasNext() (bool, error) {
	return jo.base.HasNext()
}

func (jo *JoinOperator) Next() (*tuple.Tuple, error) {
	return jo.base.Next()
}

// Helper functions remain the same
func validateInputs(predicate *JoinPredicate, leftChild, rightChild iterator.DbIterator) error {
	if predicate == nil {
		return fmt.Errorf("join predicate cannot be nil")
	}
	if leftChild == nil {
		return fmt.Errorf("left child operator cannot be nil")
	}
	if rightChild == nil {
		return fmt.Errorf("right child operator cannot be nil")
	}
	return nil
}

func createCombinedSchema(leftChild, rightChild iterator.DbIterator) (*tuple.TupleDescription, error) {
	leftTupleDesc := leftChild.GetTupleDesc()
	rightTupleDesc := rightChild.GetTupleDesc()

	if leftTupleDesc == nil || rightTupleDesc == nil {
		return nil, fmt.Errorf("child operators must have valid tuple descriptors")
	}

	combinedTupleDesc := tuple.Combine(leftTupleDesc, rightTupleDesc)
	if combinedTupleDesc == nil {
		return nil, fmt.Errorf("failed to combine tuple descriptors")
	}

	return combinedTupleDesc, nil
}
