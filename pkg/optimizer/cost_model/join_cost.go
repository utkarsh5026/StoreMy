package costmodel

import (
	"math"
	"storemy/pkg/plan"
)

// estimateJoinCost estimates the cost of a join operation based on the specified join method.
//
// Join Cost Components:
//  1. Input production cost: Cost to produce left and right child relations
//  2. Join method cost: Algorithm-specific cost (hash/merge/nested loop)
//  3. Output materialization: Cost to produce result tuples
//
// Supported Join Methods:
//   - "hash": Hash join - best for large unsorted inputs with sufficient memory
//   - "merge": Sort-merge join - efficient for pre-sorted inputs or when hash table doesn't fit
//   - "nested": Nested loop join - simple but expensive O(n*m), only for tiny inputs
//
// The optimizer chooses join methods based on:
//   - Input cardinalities and available memory
//   - Sort order of inputs
//   - Selectivity of join predicate
//
// Parameters:
//   - node: Join plan node containing join method, predicate, and child nodes
//
// Returns:
//   - float64: Total estimated cost (input + join + output)
func (cm *CostModel) estimateJoinCost(node *plan.JoinNode) float64 {
	// First, estimate costs of child nodes
	leftCost := cm.EstimatePlanCost(node.LeftChild)
	rightCost := cm.EstimatePlanCost(node.RightChild)
	leftCard := node.LeftChild.GetCardinality()
	rightCard := node.RightChild.GetCardinality()

	// Base cost includes producing both inputs
	baseCost := leftCost + rightCost

	// Choose join method cost based on specified method
	var joinMethodCost float64
	switch node.JoinMethod {
	case "hash":
		joinMethodCost = cm.estimateHashJoinCost(leftCard, rightCard)
	case "merge":
		joinMethodCost = cm.estimateSortMergeJoinCost(leftCard, rightCard)
	case "nested":
		joinMethodCost = cm.estimateNestedLoopJoinCost(leftCard, rightCard)
	default:
		// Default to nested loop (simplest, usually worst)
		joinMethodCost = cm.estimateNestedLoopJoinCost(leftCard, rightCard)
	}

	// Cost of materializing output tuples (CPU overhead for result construction)
	outputCost := float64(node.Cardinality) * cm.CPUCostPerTuple

	return baseCost + joinMethodCost + outputCost
}

// estimateHashJoinCost estimates the cost of a hash join operation.
//
// Hash Join Algorithm:
//  1. Build Phase: Create in-memory hash table on smaller relation
//     - Hash each tuple and insert into bucket
//     - O(n) time for n tuples in build relation
//  2. Probe Phase: Scan larger relation and probe hash table
//     - Hash each tuple to find matching bucket
//     - Compare with tuples in bucket
//     - O(m) time for m tuples in probe relation
//  3. Grace Hash Join: If hash table doesn't fit in memory
//     - Partition both relations into disk-resident partitions
//     - Join partitions one at a time
//     - Adds 2× I/O cost (write + read partitions)
//
// Memory Considerations:
//   - Optimal when build relation fits in HashTableMemory pages
//   - Performance degrades significantly with disk partitioning
//   - Build on smaller relation to minimize memory footprint
//
// Cost Formula:
//   - In-memory: BuildCost + ProbeCost
//   - Grace hash: BuildCost + ProbeCost + 2× PartitionI/O
//
// Best suited for:
//   - Large unsorted inputs
//   - Equijoin predicates
//   - Sufficient memory for build relation
//
// Parameters:
//   - leftCard: Cardinality of left input relation
//   - rightCard: Cardinality of right input relation
//
// Returns:
//   - float64: Total hash join cost (CPU + potential I/O)
func (cm *CostModel) estimateHashJoinCost(leftCard, rightCard int64) float64 {
	// Choose smaller relation for build phase to minimize memory usage
	buildSize := float64(min(leftCard, rightCard))
	probeSize := float64(max(leftCard, rightCard))

	// Build phase: hash computation and hash table insertion
	// HashBuildCPUFactor accounts for hash function + insertion overhead
	buildCost := buildSize * cm.CPUCostPerTuple * HashBuildCPUFactor

	// Probe phase: hash lookup and equality comparison
	// HashProbeCPUFactor accounts for hash function + bucket scan
	probeCost := probeSize * cm.CPUCostPerTuple * HashProbeCPUFactor

	// Check if hash table fits in memory
	// Calculate pages needed for build relation
	buildPages := estimatePagesForTuples(int64(buildSize), DefaultTuplesPerPage)

	if buildPages > float64(cm.HashTableMemory) {
		// Hash table doesn't fit: need grace hash join with partitioning
		// Must write and re-read both relations (2× I/O per relation)
		// Total I/O: 2 writes + 2 reads = 4× data volume
		partitionIOCost := (buildSize + probeSize) / DefaultTuplesPerPage * cm.IOCostPerPage * 2.0
		return buildCost + probeCost + partitionIOCost
	}

	// Hash table fits in memory - optimal case
	return buildCost + probeCost
}

// estimateSortMergeJoinCost estimates the cost of a sort-merge join operation.
//
// Sort-Merge Join Algorithm:
//  1. Sort Phase: Sort both input relations on join key
//     - External merge sort if inputs don't fit in memory
//     - O(n log n) CPU + potential I/O for external sort
//  2. Merge Phase: Single linear pass through sorted inputs
//     - Advance pointers through both sorted lists
//     - Output matching tuples
//     - O(n + m) time complexity
//
// Advantages:
//   - No memory requirements beyond sort memory
//   - Produces sorted output (beneficial for downstream operators)
//   - Efficient when inputs are already sorted
//   - Gracefully handles large inputs via external merge sort
//
// Cost Components:
//   - Left sort cost: Depends on memory availability (in-memory vs external)
//   - Right sort cost: Same as left
//   - Merge cost: Linear scan of both sorted relations
//
// Best suited for:
//   - Inputs already sorted on join key
//   - Large inputs that don't fit in hash table memory
//   - When sorted output is beneficial
//
// Parameters:
//   - leftCard: Cardinality of left input relation
//   - rightCard: Cardinality of right input relation
//
// Returns:
//   - float64: Total sort-merge join cost (sort + merge)
func (cm *CostModel) estimateSortMergeJoinCost(leftCard, rightCard int64) float64 {
	// Sort both inputs on join key
	// Uses external merge sort if data exceeds SortMemory
	leftSortCost := cm.estimateSortCostInternal(leftCard)
	rightSortCost := cm.estimateSortCostInternal(rightCard)

	// Merge phase: linear scan of both sorted relations
	// Each tuple processed once for comparison
	mergeCost := float64(leftCard+rightCard) * cm.CPUCostPerTuple

	return leftSortCost + rightSortCost + mergeCost
}

// estimateNestedLoopJoinCost estimates the cost of a nested loop join operation.
//
// Nested Loop Join Algorithm:
//
//	For each tuple t in outer relation:
//	  For each tuple s in inner relation:
//	    If t and s match join predicate:
//	      Output (t, s)
//
// Characteristics:
//   - Simplest join algorithm (no sorting, no hash table)
//   - O(n × m) time complexity - quadratic behavior
//   - No memory requirements beyond tuple buffers
//   - Catastrophically expensive for large inputs
//
// Cost Formula:
//   - Outer loop: Process each outer tuple once
//   - Inner loop: For each outer tuple, scan all inner tuples
//   - Total comparisons: leftCard × rightCard
//
// When to use:
//   - One relation is very small (< 10 tuples)
//   - No suitable index exists
//   - Join predicate is not equality-based
//   - Last resort when other methods fail
//
// Optimization note:
//   - Always use smaller relation as outer to minimize iterations
//   - Consider block nested loop for better cache performance
//
// Parameters:
//   - leftCard: Cardinality of left (outer) input relation
//   - rightCard: Cardinality of right (inner) input relation
//
// Returns:
//   - float64: Total nested loop cost (outer + inner loop processing)
func (cm *CostModel) estimateNestedLoopJoinCost(leftCard, rightCard int64) float64 {
	// Outer loop cost: process each outer tuple once
	outerCost := float64(leftCard) * cm.CPUCostPerTuple

	// Inner loop: for each outer tuple, process all inner tuples
	// This is the expensive O(n*m) component
	innerCost := float64(leftCard) * float64(rightCard) * cm.CPUCostPerTuple

	return outerCost + innerCost
}

// estimateSortCostInternal estimates the cost of sorting N tuples using external merge sort.
//
// Sorting Algorithm:
//
//	In-Memory Sort (data fits in SortMemory):
//	  - Quicksort with O(n log n) comparisons
//	  - No I/O cost, purely CPU-bound
//
//	External Merge Sort (data exceeds SortMemory):
//	  - Phase 1: Create initial sorted runs in memory
//	  - Phase 2: Multi-way merge of runs
//	  - Each pass reads and writes entire dataset
//	  - Number of passes = log_k(N/M) where k = merge factor, M = memory
//
// Cost Components:
//   - CPU cost: O(n log n) comparisons in all cases
//   - I/O cost: 2 × passes × dataSize (read + write per pass)
//
// Memory Model:
//   - SortMemory pages available for sorting
//   - Can merge up to SortMemory runs simultaneously
//   - Larger memory = fewer merge passes
//
// Example:
//   - 1000 pages, 100 pages memory
//   - Initial runs: 10 (1000/100)
//   - Pass 1: Merge 10 runs into 1
//   - Total I/O: 2 × 1000 = 2000 page I/Os
//
// Parameters:
//   - n: Number of tuples to sort
//
// Returns:
//   - float64: Total sort cost (CPU + I/O)
//   - Returns 0.0 for empty input
func (cm *CostModel) estimateSortCostInternal(n int64) float64 {
	if n <= 0 {
		return 0.0
	}

	nFloat := float64(n)

	// Calculate pages needed to store n tuples
	sortPages := estimatePagesForTuples(n, DefaultTuplesPerPage)

	if sortPages <= float64(cm.SortMemory) {
		// In-memory quicksort: O(n log n) comparisons
		// No I/O cost, purely CPU-bound
		return nFloat * math.Log2(nFloat) * cm.CPUCostPerTuple
	}

	// External merge sort required
	// Number of runs we can merge simultaneously (limited by memory)
	runsPerMerge := float64(cm.SortMemory)

	// Initial number of sorted runs = total pages / memory
	totalRuns := sortPages

	// Count merge passes needed
	passes := 0.0
	for totalRuns > 1.0 {
		// Each pass reduces runs by factor of runsPerMerge
		totalRuns = math.Ceil(totalRuns / runsPerMerge)
		passes++
	}

	// I/O cost: read and write entire dataset per pass
	// 2× factor accounts for reading input runs and writing merged output
	ioCost := sortPages * cm.IOCostPerPage * 2.0 * passes

	// CPU cost: O(n log n) comparison cost regardless of merge strategy
	cpuCost := nFloat * math.Log2(nFloat) * cm.CPUCostPerTuple

	return ioCost + cpuCost
}
