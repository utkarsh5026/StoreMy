package join

import (
	"fmt"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
)

// NestedLoopJoin implements a block nested loop join algorithm.
//
// This is a more efficient variant of the simple nested loop join that processes
// tuples in blocks rather than one at a time. The algorithm works by:
//  1. Loading a block of tuples from the left (outer) relation into memory
//  2. For each tuple in the right (inner) relation, checking it against all
//     tuples in the current left block
//  3. When the right relation is exhausted, loading the next left block
//
// Block nested loop join is particularly effective when:
// - No indexes are available on join columns
// - The join predicate is not an equality condition
// - Memory is limited but some buffering is possible
//
// Time complexity: O(|R| * |S| / B) where B is the block size
// Space complexity: O(B) for the left block buffer
type NestedLoopJoin struct {
	leftChild   iterator.DbIterator
	rightChild  iterator.DbIterator
	predicate   *JoinPredicate
	leftBlock   []*tuple.Tuple
	blockSize   int
	blockIndex  int
	initialized bool
	stats       *JoinStatistics
	matchBuffer *JoinMatchBuffer
}

// NewNestedLoopJoin creates a new block nested loop join operator.
// The block size is determined based on available memory statistics.
// If no statistics are provided, a default block size of 100 tuples is used.
//
// Returns a new NestedLoopJoin instance ready for initialization.
func NewNestedLoopJoin(left, right iterator.DbIterator, pred *JoinPredicate, stats *JoinStatistics) *NestedLoopJoin {
	blockSize := 100 // Default block size
	if stats != nil && stats.MemorySize > 0 {
		blockSize = stats.MemorySize * 100 // Assume 100 tuples per page
	}

	return &NestedLoopJoin{
		leftChild:   left,
		rightChild:  right,
		predicate:   pred,
		blockSize:   blockSize,
		blockIndex:  0,
		stats:       stats,
		matchBuffer: NewJoinMatchBuffer(),
	}
}

// Next returns the next joined tuple from the block nested loop join operation.
// Must call Initialize() before first use.
//
// The method maintains several levels of state:
// 1. Match buffer: Contains pre-computed matches for the current right tuple
// 2. Block processing: Processes all tuples in the current left block
// 3. Block loading: Loads new left blocks when current block is exhausted
func (nl *NestedLoopJoin) Next() (*tuple.Tuple, error) {
	if !nl.initialized {
		return nil, fmt.Errorf("block nested loop join not initialized")
	}

	if nl.matchBuffer.HasNext() {
		return nl.matchBuffer.Next(), nil
	}

	nl.matchBuffer.StartNew()

	for {
		if nl.blockIndex >= len(nl.leftBlock) {
			hasMore, err := nl.leftChild.HasNext()
			if err != nil {
				return nil, err
			}
			if !hasMore {
				return nil, nil
			}

			if err := nl.loadNextBlock(); err != nil {
				return nil, err
			}
			if len(nl.leftBlock) == 0 {
				return nil, nil
			}

			if err := nl.rightChild.Rewind(); err != nil {
				return nil, err
			}
		}

		if err := nl.processNextRightTuple(); err != nil {
			return nil, err
		}

		if result := nl.matchBuffer.GetFirstAndAdvance(); result != nil {
			return result, nil
		}
	}
}

// Initialize prepares the block nested loop join for execution.
// This must be called before any Next() operations.
// The method is idempotent - multiple calls are safe.
//
// Loads the first block of left tuples into memory and sets up
// the initial state for iteration.
//
// Returns error if the first block cannot be loaded.
func (nl *NestedLoopJoin) Initialize() error {
	if nl.initialized {
		return nil
	}

	if err := nl.loadNextBlock(); err != nil {
		return err
	}

	nl.initialized = true
	return nil
}

// loadNextBlock reads the next block of tuples from the left iterator.
// The block size is determined by the blockSize field, which can be
// configured based on available memory.
//
// This method:
// 1. Clears the current left block
// 2. Reads up to blockSize tuples from the left iterator
// 3. Resets the block index to 0
//
// Returns error if reading from the left iterator fails.
func (nl *NestedLoopJoin) loadNextBlock() error {
	nl.leftBlock = make([]*tuple.Tuple, 0, nl.blockSize)
	nl.blockIndex = 0

	for i := 0; i < nl.blockSize; i++ {
		hasNext, err := nl.leftChild.HasNext()
		if err != nil {
			return err
		}
		if !hasNext {
			break
		}

		t, err := nl.leftChild.Next()
		if err != nil {
			return err
		}
		if t != nil {
			nl.leftBlock = append(nl.leftBlock, t)
		}
	}

	return nil
}

// Close releases all resources held by the nested loop join operator.
// This includes clearing the left block buffer, match buffer, and
// resetting all state variables.
func (nl *NestedLoopJoin) Close() error {
	nl.leftBlock = nil
	nl.matchBuffer.Reset()
	nl.initialized = false
	return nil
}

// EstimateCost returns the estimated cost of executing this block nested loop join.
//
// Uses the formula: |R| + (|R| / BlockSize) * |S|
// Where:
//   - |R| is the size of the left relation
//   - |S| is the size of the right relation
//   - BlockSize is the number of tuples per block
//
// This represents:
//   - |R|: Cost of reading the left relation once
//   - (|R| / BlockSize): Number of blocks in the left relation
//   - * |S|: Right relation is scanned once per left block
//
// Returns a high default cost if no statistics are available.
func (nl *NestedLoopJoin) EstimateCost() float64 {
	if nl.stats == nil {
		return 1000000
	}

	// Cost = |R| + (|R| / BlockSize) * |S|
	numBlocks := float64(nl.stats.LeftSize) / float64(nl.blockSize)
	if numBlocks < 1 {
		numBlocks = 1
	}

	return float64(nl.stats.LeftSize) + numBlocks*float64(nl.stats.RightSize)
}

// SupportsPredicateType checks if this nested loop join can handle the given predicate.
func (nl *NestedLoopJoin) SupportsPredicateType(predicate *JoinPredicate) bool {
	return true // Block nested loop supports all predicate types
}

// Reset rewinds the nested loop join to the beginning, allowing re-iteration.
// This resets all iteration state and reloads the first left block.
//
// The method:
// 1. Resets block and buffer indices
// 2. Clears the match buffer
// 3. Rewinds both child iterators
// 4. Reloads the first left block
//
// Returns error if rewinding child iterators or loading the first block fails.
func (nl *NestedLoopJoin) Reset() error {
	nl.blockIndex = 0
	nl.matchBuffer.Reset()

	if err := nl.leftChild.Rewind(); err != nil {
		return err
	}

	if err := nl.rightChild.Rewind(); err != nil {
		return err
	}

	return nl.loadNextBlock()
}

// processNextRightTuple gets the next tuple from the right iterator and
// finds all matches with the current left block.
//
// The method handles two scenarios:
// 1. If more right tuples exist: processes the next right tuple
// 2. If right iterator is exhausted: moves to the next left block
//
// When a right tuple is found, it's compared against all tuples in the
// current left block, and matches are added to the match buffer.
//
// Returns error if iterator operations fail.
func (nl *NestedLoopJoin) processNextRightTuple() error {
	hasNext, err := nl.rightChild.HasNext()
	if err != nil {
		return err
	}

	if !hasNext {
		nl.blockIndex = len(nl.leftBlock)
		return nl.rightChild.Rewind()
	}

	rightTuple, err := nl.rightChild.Next()
	if err != nil {
		return err
	}
	if rightTuple == nil {
		return nil
	}

	return nl.findMatchesInCurrentBlock(rightTuple)
}

// findMatchesInCurrentBlock compares a right tuple against all tuples
// in the current left block and adds matches to the match buffer.
//
// For each left tuple in the current block:
// 1. Applies the join predicate to test for a match
// 2. If the predicate succeeds, combines the tuples and adds to buffer
// 3. Continues processing even if individual predicate evaluations fail
func (nl *NestedLoopJoin) findMatchesInCurrentBlock(rightTuple *tuple.Tuple) error {
	for i := nl.blockIndex; i < len(nl.leftBlock); i++ {
		leftTuple := nl.leftBlock[i]

		matches, err := nl.predicate.Filter(leftTuple, rightTuple)
		if err != nil {
			continue
		}

		if matches {
			if err := nl.addMatchToBuffer(leftTuple, rightTuple); err != nil {
				continue
			}
		}
	}

	return nil
}

// addMatchToBuffer combines two matching tuples and adds the result
// to the match buffer for later retrieval.
func (nl *NestedLoopJoin) addMatchToBuffer(leftTuple, rightTuple *tuple.Tuple) error {
	joinedTuple, err := tuple.CombineTuples(leftTuple, rightTuple)
	if err != nil {
		return err
	}

	nl.matchBuffer.Add(joinedTuple)
	return nil
}
