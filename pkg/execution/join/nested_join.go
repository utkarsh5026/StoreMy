package join

import (
	"fmt"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
)

// NestedLoopJoin implements block nested loop join algorithm
// More efficient than simple nested loop by reading blocks of tuples at a time
type NestedLoopJoin struct {
	leftChild   iterator.DbIterator
	rightChild  iterator.DbIterator
	predicate   *JoinPredicate
	leftBlock   []*tuple.Tuple
	blockSize   int
	blockIndex  int
	initialized bool
	stats       *JoinStatistics
	matchBuffer []*tuple.Tuple
	bufferIndex int
}

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
		bufferIndex: -1,
	}
}

func (nl *NestedLoopJoin) Next() (*tuple.Tuple, error) {
	if !nl.initialized {
		return nil, fmt.Errorf("block nested loop join not initialized")
	}

	if nl.hasBufferedResults() {
		return nl.getNextBufferedResult(), nil
	}

	nl.matchBuffer = make([]*tuple.Tuple, 0)
	nl.bufferIndex = 0

	for {
		if nl.blockIndex >= len(nl.leftBlock) {
			hasMore, err := nl.leftChild.HasNext()
			if err != nil {
				return nil, err
			}
			if !hasMore {
				return nil, nil // No more tuples
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
		hasNext, err := nl.rightChild.HasNext()
		if err != nil {
			return nil, err
		}

		if !hasNext {
			nl.blockIndex = len(nl.leftBlock)
			if err := nl.rightChild.Rewind(); err != nil {
				return nil, err
			}
			continue
		}

		rightTuple, err := nl.rightChild.Next()
		if err != nil {
			return nil, err
		}

		if rightTuple == nil {
			continue
		}

		for i := nl.blockIndex; i < len(nl.leftBlock); i++ {
			matches, err := nl.predicate.Filter(nl.leftBlock[i], rightTuple)
			if err != nil {
				continue
			}

			if matches {
				joined, err := tuple.CombineTuples(nl.leftBlock[i], rightTuple)
				if err == nil {
					nl.matchBuffer = append(nl.matchBuffer, joined)
				}
			}
		}

		if len(nl.matchBuffer) > 0 {
			nl.bufferIndex = 1
			return nl.matchBuffer[0], nil
		}
	}
}

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

func (nl *NestedLoopJoin) Close() error {
	nl.leftBlock = nil
	nl.matchBuffer = nil
	nl.initialized = false
	return nil
}

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

func (nl *NestedLoopJoin) SupportsPredicateType(predicate *JoinPredicate) bool {
	return true // Block nested loop supports all predicate types
}

func (nl *NestedLoopJoin) Reset() error {
	nl.blockIndex = 0
	nl.matchBuffer = nil
	nl.bufferIndex = -1

	if err := nl.leftChild.Rewind(); err != nil {
		return err
	}

	if err := nl.rightChild.Rewind(); err != nil {
		return err
	}

	return nl.loadNextBlock()
}

func (nl *NestedLoopJoin) hasBufferedResults() bool {
	return nl.bufferIndex >= 0 && nl.bufferIndex < len(nl.matchBuffer)
}

func (nl *NestedLoopJoin) getNextBufferedResult() *tuple.Tuple {
	result := nl.matchBuffer[nl.bufferIndex]
	nl.bufferIndex++
	return result
}
