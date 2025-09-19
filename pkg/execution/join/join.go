package join

import (
	"fmt"
	"storemy/pkg/execution"
	"storemy/pkg/tuple"
	"sync"
)

// Join implements the relational join operation between two input streams.
// It supports both nested loop joins (for any predicate) and hash joins (for equality predicates).
// The join operator combines tuples from two child operators that satisfy the join predicate.
type Join struct {
	base       *execution.BaseIterator // Handles iterator caching logic
	predicate  *JoinPredicate          // Join condition
	leftChild  execution.DbIterator    // Left input operator
	rightChild execution.DbIterator    // Right input operator
	tupleDesc  *tuple.TupleDescription // Combined schema of output tuples

	// Hash join specific fields (used when predicate is equality)
	hashTable      map[string][]*tuple.Tuple // Hash table for right side tuples
	currentMatches []*tuple.Tuple            // Current matching tuples from hash table
	currentLeft    *tuple.Tuple              // Current left tuple being processed
	matchIndex     int                       // Index in current matches
	isHashJoin     bool                      // Whether to use hash join optimization

	// Nested loop join specific fields
	hasCurrentLeft bool // Whether we have a current left tuple

	mutex sync.RWMutex // Protects concurrent access
}

// NewJoin creates a new Join operator with the specified predicate and child operators.
// It automatically determines whether to use hash join (for equality predicates) or
// nested loop join (for other predicates).
//
// Parameters:
//   - predicate: The join condition that determines which tuple pairs to combine
//   - leftChild: The left input operator (outer relation)
//   - rightChild: The right input operator (inner relation)
//
// Returns:
//   - *Join: A new join operator instance
//   - error: An error if validation fails or tuple descriptors cannot be combined
func NewJoin(predicate *JoinPredicate, leftChild, rightChild execution.DbIterator) (*Join, error) {
	if predicate == nil {
		return nil, fmt.Errorf("join predicate cannot be nil")
	}
	if leftChild == nil {
		return nil, fmt.Errorf("left child operator cannot be nil")
	}
	if rightChild == nil {
		return nil, fmt.Errorf("right child operator cannot be nil")
	}

	leftTupleDesc := leftChild.GetTupleDesc()
	rightTupleDesc := rightChild.GetTupleDesc()
	if leftTupleDesc == nil || rightTupleDesc == nil {
		return nil, fmt.Errorf("child operators must have valid tuple descriptors")
	}

	combinedTupleDesc := tuple.Combine(leftTupleDesc, rightTupleDesc)
	if combinedTupleDesc == nil {
		return nil, fmt.Errorf("failed to combine tuple descriptors")
	}

	isHashJoin := predicate.GetOP() == execution.Equals
	j := &Join{
		predicate:  predicate,
		leftChild:  leftChild,
		rightChild: rightChild,
		tupleDesc:  combinedTupleDesc,
		isHashJoin: isHashJoin,
		hashTable:  make(map[string][]*tuple.Tuple),
		matchIndex: -1,
	}

	j.base = execution.NewBaseIterator(j.readNext)
	return j, nil
}

// Open initializes the join operator for execution.
// For hash joins, it builds the hash table from the right child.
// For nested loop joins, it positions the left child at the first tuple.
func (j *Join) Open() error {
	if err := j.leftChild.Open(); err != nil {
		return fmt.Errorf("failed to open left child: %v", err)
	}
	if err := j.rightChild.Open(); err != nil {
		return fmt.Errorf("failed to open right child: %v", err)
	}

	j.mutex.Lock()
	defer j.mutex.Unlock()

	if j.isHashJoin {
		if err := j.buildHashTable(); err != nil {
			return fmt.Errorf("failed to build hash table: %v", err)
		}
	} else {
		if err := j.initializeNestedLoop(); err != nil {
			return fmt.Errorf("failed to initialize nested loop: %v", err)
		}
	}

	j.base.MarkOpened()
	return nil
}

// Rewind resets the join operator to its initial state.
func (j *Join) Rewind() error {
	j.mutex.Lock()
	defer j.mutex.Unlock()

	// Rewind both children
	if err := j.leftChild.Rewind(); err != nil {
		return err
	}
	if err := j.rightChild.Rewind(); err != nil {
		return err
	}

	j.currentMatches = nil
	j.currentLeft = nil
	j.matchIndex = -1
	j.hasCurrentLeft = false

	if j.isHashJoin {
		// Hash table is already built, just reset left side
		j.currentLeft = nil
		j.currentMatches = nil
		j.matchIndex = -1
	} else {
		// Re-initialize nested loop
		if err := j.initializeNestedLoop(); err != nil {
			return err
		}
	}

	j.base.ClearCache()
	return nil
}

// Close releases resources held by the join operator and its children.
func (j *Join) Close() error {
	j.mutex.Lock()
	defer j.mutex.Unlock()

	j.hashTable = make(map[string][]*tuple.Tuple)
	j.currentMatches = nil
	j.currentLeft = nil

	// Close children
	if j.leftChild != nil {
		j.leftChild.Close()
	}
	if j.rightChild != nil {
		j.rightChild.Close()
	}

	return j.base.Close()
}

// GetTupleDesc returns the tuple description for the joined tuples.
// This combines the schemas from both input operators.
func (j *Join) GetTupleDesc() *tuple.TupleDescription {
	return j.tupleDesc
}

// HasNext checks if there are more joined tuples available.
func (j *Join) HasNext() (bool, error) { return j.base.HasNext() }

// Next returns the next joined tuple.
func (j *Join) Next() (*tuple.Tuple, error) { return j.base.Next() }

// buildHashTable constructs a hash table from the right child for equality joins.
func (j *Join) buildHashTable() error {
	rightFieldIndex := j.predicate.GetField2()

	for {
		hasNext, err := j.rightChild.HasNext()
		if err != nil {
			return err
		}
		if !hasNext {
			break
		}

		rightTuple, err := j.rightChild.Next()
		if err != nil {
			return err
		}
		if rightTuple == nil {
			continue
		}

		joinField, err := rightTuple.GetField(rightFieldIndex)
		if err != nil {
			continue // Skip tuples with invalid fields
		}
		if joinField == nil {
			continue // Skip tuples with null join fields
		}

		key := joinField.String()
		j.hashTable[key] = append(j.hashTable[key], rightTuple)
	}

	return nil
}

// initializeNestedLoop sets up the nested loop join by getting the first left tuple.
func (j *Join) initializeNestedLoop() error {
	hasNext, err := j.leftChild.HasNext()
	if err != nil {
		return err
	}

	if hasNext {
		j.currentLeft, err = j.leftChild.Next()
		if err != nil {
			return err
		}
		j.hasCurrentLeft = j.currentLeft != nil
	} else {
		j.hasCurrentLeft = false
	}

	return nil
}

// readNext implements the core join logic, delegating to hash join or nested loop join.
func (j *Join) readNext() (*tuple.Tuple, error) {
	j.mutex.Lock()
	defer j.mutex.Unlock()

	if j.isHashJoin {
		return j.readNextHashJoin()
	}
	return j.readNextNestedLoop()
}

// readNextNestedLoop implements nested loop join logic for general predicates.
func (j *Join) readNextNestedLoop() (*tuple.Tuple, error) {
	for j.hasCurrentLeft {
		for {
			hasNextRight, err := j.rightChild.HasNext()
			if err != nil {
				return nil, err
			}

			if !hasNextRight {
				break
			}

			rightTuple, err := j.rightChild.Next()
			if err != nil {
				return nil, err
			}

			if rightTuple == nil {
				continue
			}

			matches, err := j.predicate.Filter(j.currentLeft, rightTuple)
			if err != nil {
				continue // Skip on error
			}

			if matches {
				return tuple.CombineTuples(j.currentLeft, rightTuple)
			}
		}

		hasNextLeft, err := j.leftChild.HasNext()
		if err != nil {
			return nil, err
		}

		if !hasNextLeft {
			j.hasCurrentLeft = false
			return nil, nil // No more tuples
		}

		j.currentLeft, err = j.leftChild.Next()
		if err != nil {
			return nil, err
		}

		j.hasCurrentLeft = j.currentLeft != nil
		if err := j.rightChild.Rewind(); err != nil {
			return nil, err
		}
	}

	return nil, nil // No more tuples
}

// readNextHashJoin implements hash join logic for equality predicates.
func (j *Join) readNextHashJoin() (*tuple.Tuple, error) {
	leftFieldIndex := j.predicate.GetField1()

	if j.currentMatches != nil && j.matchIndex >= 0 && j.matchIndex < len(j.currentMatches) {
		joinedTuple, err := tuple.CombineTuples(j.currentLeft, j.currentMatches[j.matchIndex])
		j.matchIndex++

		// If we've exhausted current matches, clear them
		if j.matchIndex >= len(j.currentMatches) {
			j.currentMatches = nil
			j.currentLeft = nil
			j.matchIndex = -1
		}

		return joinedTuple, err
	}

	for {
		hasNextLeft, err := j.leftChild.HasNext()
		if err != nil {
			return nil, err
		}
		if !hasNextLeft {
			return nil, nil // No more tuples
		}

		leftTuple, err := j.leftChild.Next()
		if err != nil {
			return nil, err
		}
		if leftTuple == nil {
			continue
		}

		// Get join key from left tuple
		joinField, err := leftTuple.GetField(leftFieldIndex)
		if err != nil {
			continue
		}
		if joinField == nil {
			continue
		}

		key := joinField.String()

		// Look for matches in hash table
		if matches, exists := j.hashTable[key]; exists && len(matches) > 0 {
			j.currentLeft = leftTuple
			j.currentMatches = matches
			j.matchIndex = 0

			// Return first match
			joinedTuple, err := tuple.CombineTuples(leftTuple, matches[0])
			j.matchIndex++

			return joinedTuple, err
		}
	}
}
