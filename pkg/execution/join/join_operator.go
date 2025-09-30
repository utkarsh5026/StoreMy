package join

import (
	"fmt"
	"storemy/pkg/execution/query"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
	"sync"
)

// JoinOperator is the main interface for executing joins in the database engine.
// It abstracts the underlying join algorithm and provides automatic algorithm selection
// based on data characteristics and available resources.
type JoinOperator struct {
	base *query.BaseIterator // Handles common iterator operations (caching, state management)

	leftChild  iterator.DbIterator // Left (outer) relation
	rightChild iterator.DbIterator // Right (inner) relation

	predicate *JoinPredicate          // Join condition (e.g., R.id = S.id)
	tupleDesc *tuple.TupleDescription // Schema of the result tuples
	algorithm JoinAlgorithm           // Selected join algorithm implementation
	strategy  *JoinStrategy           // Algorithm selection strategy

	initialized bool         // Whether the operator has been opened and initialized
	mutex       sync.RWMutex // Protects concurrent access to operator state
}

// NewJoinOperator creates a new join operator with automatic algorithm selection.
func NewJoinOperator(predicate *JoinPredicate, leftChild, rightChild iterator.DbIterator) (*JoinOperator, error) {
	if err := validateInputs(predicate, leftChild, rightChild); err != nil {
		return nil, fmt.Errorf("invalid join operator inputs: %w", err)
	}
	combinedTupleDesc, err := createCombinedSchema(leftChild, rightChild)
	if err != nil {
		return nil, fmt.Errorf("failed to create result schema: %w", err)
	}

	jo := &JoinOperator{
		leftChild:   leftChild,
		rightChild:  rightChild,
		predicate:   predicate,
		tupleDesc:   combinedTupleDesc,
		initialized: false,
	}

	jo.base = query.NewBaseIterator(jo.readNext)
	return jo, nil
}

// Open initializes the join operator and selects the optimal join algorithm.
//
// This method:
//  1. Opens both child operators
//  2. Gathers statistics about the input relations
//  3. Uses JoinStrategy to select the best algorithm
//  4. Initializes the selected algorithm
func (jo *JoinOperator) Open() error {
	jo.mutex.Lock()
	defer jo.mutex.Unlock()

	if jo.initialized {
		return nil
	}

	if err := jo.openChildOperators(); err != nil {
		return err
	}

	stats, err := jo.gatherStatistics()
	if err != nil {
		stats = jo.getDefaultStatistics()
	}

	if err := jo.selectAndInitializeAlgorithm(stats); err != nil {
		return err
	}

	jo.initialized = true
	jo.base.MarkOpened()
	return nil
}

// openChildOperators opens both left and right child operators
func (jo *JoinOperator) openChildOperators() error {
	if err := jo.leftChild.Open(); err != nil {
		return fmt.Errorf("failed to open left child: %w", err)
	}

	if err := jo.rightChild.Open(); err != nil {
		return fmt.Errorf("failed to open right child: %w", err)
	}

	return nil
}

// gatherStatistics collects statistics about the input relations
func (jo *JoinOperator) gatherStatistics() (*JoinStatistics, error) {
	return GetStatistics(jo.leftChild, jo.rightChild)
}

// getDefaultStatistics provides fallback statistics when gathering fails
func (jo *JoinOperator) getDefaultStatistics() *JoinStatistics {
	return &JoinStatistics{
		LeftCardinality:  1000, // Assume moderate cardinality
		RightCardinality: 1000,
		LeftSize:         10, // Assume small relations
		RightSize:        10,
		MemorySize:       100, // Conservative memory estimate
		Selectivity:      0.1, // Assume 10% selectivity
	}
}

// selectAndInitializeAlgorithm chooses and sets up the optimal join algorithm
func (jo *JoinOperator) selectAndInitializeAlgorithm(stats *JoinStatistics) error {
	jo.strategy = NewJoinStrategy(jo.leftChild, jo.rightChild, jo.predicate, stats)

	algorithm, err := jo.strategy.SelectBestAlgorithm(jo.predicate)
	if err != nil {
		return fmt.Errorf("failed to select join algorithm: %w", err)
	}
	jo.algorithm = algorithm

	if err := jo.algorithm.Initialize(); err != nil {
		return fmt.Errorf("failed to initialize join algorithm: %w", err)
	}
	return nil
}

// readNext is the internal function that retrieves the next tuple from the join.
// This method is called by the base iterator to implement the iterator interface.
//
// Returns:
//   - Next joined tuple, or nil when no more tuples exist
//   - Error if the operator is not initialized or algorithm fails
func (jo *JoinOperator) readNext() (*tuple.Tuple, error) {
	jo.mutex.RLock()
	defer jo.mutex.RUnlock()

	if !jo.initialized {
		return nil, fmt.Errorf("join operator not initialized - call Open() first")
	}

	return jo.algorithm.Next()
}

// Rewind resets the join operator to start iteration from the beginning.
//
// This method:
//  1. Rewinds both child operators
//  2. Resets the join algorithm state
//  3. Clears any cached results
func (jo *JoinOperator) Rewind() error {
	jo.mutex.Lock()
	defer jo.mutex.Unlock()

	if err := jo.leftChild.Rewind(); err != nil {
		return fmt.Errorf("failed to rewind left child: %w", err)
	}

	if err := jo.rightChild.Rewind(); err != nil {
		return fmt.Errorf("failed to rewind right child: %w", err)
	}

	if err := jo.algorithm.Reset(); err != nil {
		return fmt.Errorf("failed to reset join algorithm: %w", err)
	}

	jo.base.ClearCache()
	return nil
}

// Close releases all resources held by the join operator.
//
// This method:
//  1. Closes both child operators
//  2. Closes the join algorithm (freeing memory, hash tables, etc.)
//  3. Closes the base iterator
//  4. Marks the operator as uninitialized
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

// GetTupleDesc returns the schema description of the result tuples.
func (jo *JoinOperator) GetTupleDesc() *tuple.TupleDescription {
	return jo.tupleDesc
}

// HasNext checks if more joined tuples are available.
func (jo *JoinOperator) HasNext() (bool, error) {
	return jo.base.HasNext()
}

// Next returns the next joined tuple from the join operation.
func (jo *JoinOperator) Next() (*tuple.Tuple, error) {
	return jo.base.Next()
}

// validateInputs checks that all required inputs are valid.
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

// createCombinedSchema creates the result tuple schema by combining
// the schemas of the left and right child operators.
//
// The combined schema contains:
//  1. All fields from the left relation (in original order)
//  2. All fields from the right relation (in original order)
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
