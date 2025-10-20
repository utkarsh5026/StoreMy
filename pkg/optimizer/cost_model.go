package optimizer

import (
	"math"
	"storemy/pkg/catalog"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/optimizer/cardinality"
	"storemy/pkg/planner"
)

const (
	IoCostPerPage   = 1.0
	CPUCostPerTuple = 0.01
)

// CostModel provides cost estimation for query plan nodes
// Costs are measured in arbitrary units representing I/O and CPU time
type CostModel struct {
	catalog              *catalog.SystemCatalog
	cardinalityEstimator *cardinality.CardinalityEstimator

	// Cost parameters (tunable based on hardware)
	IOCostPerPage   float64 // Cost of reading one page from disk
	CPUCostPerTuple float64 // Cost of processing one tuple
	MemoryPages     int     // Available buffer pool pages
	HashTableMemory int     // Memory available for hash tables (in pages)
	SortMemory      int     // Memory available for sorting (in pages)
}

// NewCostModel creates a new cost model with default parameters
func NewCostModel(cat *catalog.SystemCatalog) *CostModel {
	cardEst, _ := cardinality.NewCardinalityEstimator(cat)
	return &CostModel{
		catalog:              cat,
		cardinalityEstimator: cardEst,
		IOCostPerPage:        IoCostPerPage,   // Baseline I/O cost
		CPUCostPerTuple:      CPUCostPerTuple, // CPU cost is cheaper than I/O
		MemoryPages:          1000,            // 1000 pages = ~8MB with 8KB pages
		HashTableMemory:      500,             // Half of buffer pool for hash tables
		SortMemory:           500,             // Half of buffer pool for sorting
	}
}

// EstimatePlanCost estimates the total cost of executing a plan node
func (cm *CostModel) EstimatePlanCost(
	tx *transaction.TransactionContext,
	plan planner.PlanNode,
) float64 {
	if plan == nil {
		return 0.0
	}

	// First ensure cardinality is estimated
	if plan.GetCardinality() == 0 {
		card, err := cm.cardinalityEstimator.EstimatePlanCardinality(tx, plan)
		if err != nil {
			// On error, use a default cardinality
			card = cardinality.DefaultTableCardinality
		}
		plan.SetCardinality(card)
	}

	var cost float64
	switch node := plan.(type) {
	case *planner.ScanNode:
		cost = cm.estimateScanCost(tx, node)
	case *planner.JoinNode:
		cost = cm.estimateJoinCost(tx, node)
	case *planner.FilterNode:
		cost = cm.estimateFilterCost(tx, node)
	case *planner.ProjectNode:
		cost = cm.estimateProjectCost(tx, node)
	case *planner.AggregateNode:
		cost = cm.estimateAggregateCost(tx, node)
	case *planner.SortNode:
		cost = cm.estimateSortCost(tx, node)
	case *planner.LimitNode:
		cost = cm.estimateLimitCost(tx, node)
	default:
		cost = 0.0
	}

	// Set the cost on the node
	plan.SetCost(cost)
	return cost
}

// estimateScanCost estimates the cost of a table scan
func (cm *CostModel) estimateScanCost(
	tx *transaction.TransactionContext,
	node *planner.ScanNode,
) float64 {
	stats, err := cm.catalog.GetTableStatistics(tx, node.TableID)
	if err != nil || stats == nil {
		return 100.0 * cm.IOCostPerPage
	}

	switch node.AccessMethod {
	case "seqscan":
		return cm.estimateSeqScanCost(stats)
	case "indexscan":
		return cm.estimateIndexScanCost(tx, node, stats)
	default:
		return cm.estimateSeqScanCost(stats)
	}
}

// estimateSeqScanCost estimates cost of sequential scan
func (cm *CostModel) estimateSeqScanCost(stats *catalog.TableStatistics) float64 {
	ioCost := float64(stats.PageCount) * cm.IOCostPerPage
	cpuCost := float64(stats.Cardinality) * cm.CPUCostPerTuple

	return ioCost + cpuCost
}

func (cm *CostModel) estimateIndexScanCost(
	tx *transaction.TransactionContext,
	node *planner.ScanNode,
	tableStats *catalog.TableStatistics,
) float64 {
	indexStats, err := cm.catalog.GetIndexStatistics(tx, node.IndexID)
	if err != nil || indexStats == nil {
		return cm.estimateSeqScanCost(tableStats)
	}

	treeHeight := float64(indexStats.Height)
	indexLookupCost := treeHeight * cm.IOCostPerPage

	outputRows := float64(node.Cardinality)
	tablePageAccess := outputRows * (1.0 - indexStats.ClusteringFactor)
	tableAccessCost := tablePageAccess * cm.IOCostPerPage

	cpuCost := outputRows * cm.CPUCostPerTuple
	return indexLookupCost + tableAccessCost + cpuCost
}

// estimateJoinCost estimates the cost of a join operation
func (cm *CostModel) estimateJoinCost(
	tx *transaction.TransactionContext,
	node *planner.JoinNode,
) float64 {
	leftCost := cm.EstimatePlanCost(tx, node.LeftChild)
	rightCost := cm.EstimatePlanCost(tx, node.RightChild)
	leftCard := node.LeftChild.GetCardinality()
	rightCard := node.RightChild.GetCardinality()

	// Base cost includes child costs
	baseCost := leftCost + rightCost

	var joinMethodCost float64
	switch node.JoinMethod {
	case "hash":
		joinMethodCost = cm.estimateHashJoinCost(leftCard, rightCard)
	case "merge":
		joinMethodCost = cm.estimateSortMergeJoinCost(leftCard, rightCard)
	case "nested":
	default:
		joinMethodCost = cm.estimateNestedLoopJoinCost(leftCard, rightCard)
	}

	outputCost := float64(node.Cardinality) * cm.CPUCostPerTuple
	return baseCost + joinMethodCost + outputCost
}

// estimateHashJoinCost estimates cost of hash join
func (cm *CostModel) estimateHashJoinCost(leftCard, rightCard int64) float64 {
	buildSize := float64(min(leftCard, rightCard))
	buildCost := buildSize * cm.CPUCostPerTuple * 2.0

	probeSize := float64(max(leftCard, rightCard))
	probeCost := probeSize * cm.CPUCostPerTuple * 1.5 // Hash lookup

	// If hash table doesn't fit in memory, add partitioning cost
	buildPages := buildSize / 100.0 // Assume ~100 tuples per page
	if buildPages > float64(cm.HashTableMemory) {
		// Need to partition to disk
		partitionCost := (buildSize + probeSize) * cm.IOCostPerPage * 0.1
		return buildCost + probeCost + partitionCost
	}

	return buildCost + probeCost
}

// estimateSortMergeJoinCost estimates cost of sort-merge join
func (cm *CostModel) estimateSortMergeJoinCost(leftCard, rightCard int64) float64 {
	leftSortCost := cm.estimateSortCost2(leftCard)
	rightSortCost := cm.estimateSortCost2(rightCard)

	// Merge cost: linear scan of both relations
	mergeCost := float64(leftCard+rightCard) * cm.CPUCostPerTuple

	return leftSortCost + rightSortCost + mergeCost
}

// estimateNestedLoopJoinCost estimates cost of nested loop join
func (cm *CostModel) estimateNestedLoopJoinCost(leftCard, rightCard int64) float64 {
	outerCost := float64(leftCard) * cm.CPUCostPerTuple
	innerCost := float64(leftCard) * float64(rightCard) * cm.CPUCostPerTuple
	return outerCost + innerCost
}

// estimateFilterCost estimates cost of a filter operation
func (cm *CostModel) estimateFilterCost(
	tx *transaction.TransactionContext,
	node *planner.FilterNode,
) float64 {
	childCost := cm.EstimatePlanCost(tx, node.Child)
	childCard := node.Child.GetCardinality()

	filterCost := float64(childCard) * cm.CPUCostPerTuple * float64(len(node.Predicates))

	return childCost + filterCost
}

// estimateProjectCost estimates cost of a projection
func (cm *CostModel) estimateProjectCost(
	tx *transaction.TransactionContext,
	node *planner.ProjectNode,
) float64 {
	childCost := cm.EstimatePlanCost(tx, node.Child)
	childCard := node.Child.GetCardinality()

	projectionCost := float64(childCard) * cm.CPUCostPerTuple * 0.5

	return childCost + projectionCost
}

// estimateAggregateCost estimates cost of aggregation
func (cm *CostModel) estimateAggregateCost(
	tx *transaction.TransactionContext,
	node *planner.AggregateNode,
) float64 {
	childCost := cm.EstimatePlanCost(tx, node.Child)
	childCard := node.Child.GetCardinality()

	if len(node.GroupByExprs) == 0 {
		// No GROUP BY: single pass aggregation
		aggCost := float64(childCard) * cm.CPUCostPerTuple
		return childCost + aggCost
	}

	// With GROUP BY: hash-based or sort-based aggregation
	// Assume hash-based: cost is linear
	groupCost := float64(childCard) * cm.CPUCostPerTuple * 2.0 // Hash + aggregate

	return childCost + groupCost
}

// estimateSortCost estimates cost of a sort operation
func (cm *CostModel) estimateSortCost(
	tx *transaction.TransactionContext,
	node *planner.SortNode,
) float64 {
	childCost := cm.EstimatePlanCost(tx, node.Child)
	childCard := node.Child.GetCardinality()

	sortCost := cm.estimateSortCost2(childCard)

	return childCost + sortCost
}

// estimateSortCost2 estimates the cost of sorting N tuples
func (cm *CostModel) estimateSortCost2(n int64) float64 {
	if n <= 0 {
		return 0.0
	}

	// Check if sort fits in memory
	sortPages := float64(n) / 100.0 // Assume ~100 tuples per page

	if sortPages <= float64(cm.SortMemory) {
		// In-memory quicksort: O(n log n)
		return float64(n) * math.Log2(float64(n)) * cm.CPUCostPerTuple
	}

	// External merge sort: need multiple passes
	passes := math.Ceil(math.Log2(sortPages / float64(cm.SortMemory)))

	// I/O cost: read and write data for each pass
	ioCost := sortPages * cm.IOCostPerPage * 2.0 * passes

	// CPU cost: comparisons during merge
	cpuCost := float64(n) * math.Log2(float64(n)) * cm.CPUCostPerTuple

	return ioCost + cpuCost
}

// estimateLimitCost estimates cost of a LIMIT operation
func (cm *CostModel) estimateLimitCost(
	tx *transaction.TransactionContext,
	node *planner.LimitNode,
) float64 {
	// LIMIT can often short-circuit execution
	// Estimate cost based on limited output rather than full child cost

	childCard := node.Child.GetCardinality()
	limitCard := int64(node.Limit)

	// If limit is much smaller than child output, we can stop early
	if limitCard < childCard/10 {
		// Estimate partial child cost
		// This is a simplification - actual cost depends on child operator type
		ratio := float64(limitCard) / float64(childCard)
		partialCost := cm.EstimatePlanCost(tx, node.Child) * ratio
		return partialCost
	}

	// Otherwise, need to scan most/all of child
	return cm.EstimatePlanCost(tx, node.Child)
}

func (cm *CostModel) SetCostParameters(
	ioCostPerPage,
	cpuCostPerTuple float64,
	memoryPages int,
) {
	cm.IOCostPerPage = ioCostPerPage
	cm.CPUCostPerTuple = cpuCostPerTuple
	cm.MemoryPages = memoryPages
	cm.HashTableMemory = memoryPages / 2
	cm.SortMemory = memoryPages / 2
}
