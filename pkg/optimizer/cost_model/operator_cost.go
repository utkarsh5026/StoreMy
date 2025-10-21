package costmodel

import (
	"storemy/pkg/plan"
)

// estimateFilterCost estimates the cost of a filter (WHERE clause) operation.
//
// Filter Operation:
//   - Evaluates boolean predicates on each input tuple
//   - Passes through tuples that satisfy all predicates
//   - Short-circuits on first false predicate (not modeled in cost)
//
// Cost Components:
//   - Child cost: Cost to produce input tuples
//   - CPU cost: Predicate evaluation per tuple
//   - Multiple predicates multiply the cost (AND/OR evaluation)
//   - Complex predicates (subqueries, functions) cost more
//
// Cost Formula:
//   - Total = ChildCost + (InputCardinality × CPUCostPerTuple × FilterFactor × NumPredicates)
//
// Performance Characteristics:
//   - Pure CPU operation (no I/O beyond child)
//   - Linear in input cardinality
//   - Benefits from predicate pushdown optimization
//
// Best practices:
//   - Push filters close to table scans for early elimination
//   - Order predicates by selectivity (most selective first)
//   - Consider index scans to avoid filtering entirely
//
// Parameters:
//   - node: Filter plan node containing predicates and child operator
//
// Returns:
//   - float64: Total estimated cost (child production + filtering)
func (cm *CostModel) estimateFilterCost(node *plan.FilterNode) float64 {
	childCost := cm.EstimatePlanCost(node.Child)
	childCard := node.Child.GetCardinality()

	tuplesCost := float64(childCard) * cm.CPUCostPerTuple
	filterCost := tuplesCost * FilterCPUFactor * float64(len(node.Predicates))

	return childCost + filterCost
}

// estimateProjectCost estimates the cost of a projection operation.
//
// Projection Operation:
//   - Selects specific columns from input tuples
//   - Evaluates computed expressions (column arithmetic, functions)
//   - Constructs new output tuples with selected schema
//
// Cost Components:
//   - Child cost: Cost to produce input tuples
//   - CPU cost: Column extraction and tuple construction
//   - ProjectionCPUFactor is lower than filter cost (simpler operation)
//   - Expression evaluation adds overhead (not separately modeled)
//
// Cost Formula:
//   - Total = ChildCost + (InputCardinality × CPUCostPerTuple × ProjectionFactor)
//
// Performance Characteristics:
//   - Lightweight CPU operation
//   - Linear in input cardinality
//   - Memory efficient (no intermediate storage)
//
// Optimization notes:
//   - Column pruning eliminates unnecessary projections
//   - Combine adjacent projections into single operation
//   - Push down projections to reduce data movement
//
// Parameters:
//   - node: Project plan node containing column expressions and child operator
//
// Returns:
//   - float64: Total estimated cost (child production + projection)
func (cm *CostModel) estimateProjectCost(node *plan.ProjectNode) float64 {
	// Cost to produce input tuples from child operator
	childCost := cm.EstimatePlanCost(node.Child)
	childCard := node.Child.GetCardinality()

	// Projection is lighter than full tuple processing
	// ProjectionCPUFactor accounts for column selection overhead
	projectionCost := float64(childCard) * cm.CPUCostPerTuple * ProjectionCPUFactor
	return childCost + projectionCost
}

// estimateAggregateCost estimates the cost of aggregation operations (GROUP BY, COUNT, SUM, etc.).
//
// Aggregation Types:
//
//  1. Simple Aggregation (no GROUP BY):
//     - Single-pass accumulation (e.g., SELECT COUNT(*) FROM table)
//     - Maintains running aggregates in memory
//     - O(n) complexity
//
//  2. Hash-Based GROUP BY:
//     - Builds hash table of groups while scanning input
//     - Each tuple hashed by grouping columns
//     - Updates aggregate values for matching group
//     - O(n) average case, requires memory for hash table
//
// Cost Components:
//   - Child cost: Cost to produce input tuples
//   - Aggregation cost: Depends on grouping:
//   - Simple: Single accumulator update per tuple
//   - Grouped: Hash lookup + aggregate update per tuple
//   - Spill cost: If hash table exceeds memory, partition to disk
//
// Memory Considerations:
//   - Hash table size = number of groups × group entry size
//   - If exceeds HashTableMemory: external aggregation with partitioning
//   - External agg adds 2× I/O cost (write partitions + read back)
//
// Cost Formula:
//   - Simple: ChildCost + (n × CPUCostPerTuple × SimpleAggFactor)
//   - Grouped (in-memory): ChildCost + (n × CPUCostPerTuple × GroupAggFactor)
//   - Grouped (external): Above + (n/TuplesPerPage × IOCostPerPage × 2)
//
// Best suited for:
//   - Simple aggregation: Any cardinality (very efficient)
//   - Grouped: When number of groups fits in memory
//   - Sort-based alternative when hash table doesn't fit
//
// Parameters:
//   - node: Aggregate plan node with grouping expressions and aggregate functions
//
// Returns:
//   - float64: Total estimated cost (child + aggregation + potential spill)
func (cm *CostModel) estimateAggregateCost(node *plan.AggregateNode) float64 {
	// Cost to produce input tuples from child operator
	childCost := cm.EstimatePlanCost(node.Child)
	childCard := node.Child.GetCardinality()

	if len(node.GroupByExprs) == 0 {
		// Simple aggregation (no GROUP BY): single-pass accumulation
		// e.g., SELECT COUNT(*) FROM table
		// Maintains single set of aggregate values (one accumulator per function)
		aggCost := float64(childCard) * cm.CPUCostPerTuple * SimpleAggCPUFactor
		return childCost + aggCost
	}

	// GROUP BY aggregation: hash-based approach
	// Build hash table mapping group keys → aggregate values
	// For each input tuple:
	//   1. Hash the grouping columns
	//   2. Probe hash table for matching group
	//   3. Update aggregate values for that group
	groupCost := float64(childCard) * cm.CPUCostPerTuple * GroupAggCPUFactor

	// Check if hash table fits in memory
	numGroups := float64(node.Cardinality) // Output cardinality = number of groups
	groupPages := estimatePagesForTuples(int64(numGroups), DefaultTuplesPerPage)

	if groupPages > float64(cm.HashTableMemory) {
		// Hash table doesn't fit: external aggregation required
		// Strategy: Partition input by hash of group key
		//   - Write partitions to disk
		//   - Aggregate each partition independently
		//   - Ensures each partition fits in memory
		// I/O cost: write all tuples + read them back = 2× data volume
		spillCost := float64(childCard) / DefaultTuplesPerPage * cm.IOCostPerPage * 2.0
		return childCost + groupCost + spillCost
	}

	// In-memory case: hash table fits, no spill needed
	return childCost + groupCost
}

// estimateSortCost estimates the cost of a sort operation (ORDER BY).
//
// Sort Operation:
//   - Orders tuples by one or more sort keys
//   - Uses in-memory quicksort or external merge sort
//   - Materializes entire input before returning first tuple
//
// Algorithm Selection:
//   - In-memory: Quicksort when data fits in SortMemory
//   - External: Multi-way merge sort for larger datasets
//
// Cost Components:
//   - Child cost: Cost to produce input tuples
//   - Sort cost: Depends on algorithm (see estimateSortCostInternal)
//   - In-memory: O(n log n) comparisons, no I/O
//   - External: O(n log n) + multi-pass I/O
//
// Performance Characteristics:
//   - Blocking operation (must consume all input)
//   - Memory-intensive (buffers entire input or runs)
//   - Benefits downstream operators requiring sorted input
//
// Optimization opportunities:
//   - Eliminate sort if input already sorted (index scan)
//   - Use sort-merge join to amortize sort cost
//   - Apply LIMIT pushdown to reduce sort effort
//
// Parameters:
//   - node: Sort plan node with sort keys and child operator
//
// Returns:
//   - float64: Total estimated cost (child production + sorting)
func (cm *CostModel) estimateSortCost(node *plan.SortNode) float64 {
	// Cost to produce input tuples from child operator
	childCost := cm.EstimatePlanCost(node.Child)
	childCard := node.Child.GetCardinality()

	// Delegate to internal sort cost estimator
	// Handles both in-memory and external merge sort cases
	sortCost := cm.estimateSortCostInternal(childCard)

	return childCost + sortCost
}

// estimateLimitCost estimates the cost of a LIMIT operation.
//
// LIMIT Operation:
//   - Returns first N tuples from input
//   - Can short-circuit child operators in some cases
//   - Most effective with pipelined operators (scans, filters)
//
// Short-Circuit Optimization:
//   - If LIMIT is small relative to input, may stop early
//   - Only works with pipelined operators (not blocking like sort)
//   - Checked via canShortCircuit() for operator type
//
// Cost Models:
//
//  1. Short-circuit possible:
//     - Partial child cost = ChildCost × (LimitCard / ChildCard)
//     - Significant savings when LIMIT << ChildCard
//
//  2. No short-circuit (blocking operators):
//     - Full child cost (must materialize entire input)
//     - LIMIT only affects output, not computation
//
// Short-Circuit Threshold:
//   - LimitShortCircuitRatio determines when to apply optimization
//   - Default: LIMIT must be < ratio × child cardinality
//
// Examples:
//   - SELECT * FROM large_table LIMIT 10
//     → Short-circuits scan after 10 tuples
//   - SELECT * FROM (SELECT * FROM t ORDER BY x) LIMIT 10
//     → No short-circuit (sort must complete first)
//
// Parameters:
//   - node: Limit plan node with limit count and child operator
//
// Returns:
//   - float64: Total estimated cost (full or partial child cost)
func (cm *CostModel) estimateLimitCost(node *plan.LimitNode) float64 {
	childCard := node.Child.GetCardinality()
	limitCard := int64(node.Limit)

	// Check if short-circuit optimization applies
	// Condition 1: LIMIT is small relative to input
	// Condition 2: Child operator supports early termination
	if limitCard < int64(float64(childCard)*LimitShortCircuitRatio) {
		// Check if child operator supports short-circuiting
		childType := node.Child.GetNodeType()
		if canShortCircuit(childType) {
			// Estimate partial child cost based on limit ratio
			// This is a simplification: actual savings depend on operator internals
			// Example: Sequential scan can stop after reading N tuples
			ratio := float64(limitCard) / float64(childCard)
			partialCost := cm.EstimatePlanCost(node.Child) * ratio
			return partialCost
		}
	}

	// No short-circuit possible: child must fully execute
	// Common with blocking operators (sort, aggregate, some joins)
	return cm.EstimatePlanCost(node.Child)
}
