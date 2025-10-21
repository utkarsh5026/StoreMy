package costmodel

import (
	"fmt"
	"storemy/pkg/catalog"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/optimizer/cardinality"
	"storemy/pkg/plan"
)

// CostModel provides cost estimation for query plan nodes.
// It estimates both I/O and CPU costs to help the optimizer choose efficient query plans.
//
// Cost estimation considers:
// - I/O costs: Reading pages from disk (sequential vs random access)
// - CPU costs: Processing tuples, comparisons, hashing
// - Memory constraints: Hash tables, sort buffers
// - Access methods: Sequential scans, index scans, index-only scans
// - Join algorithms: Hash join, sort-merge join, nested loop join
type CostModel struct {
	catalog              *catalog.SystemCatalog
	cardinalityEstimator *cardinality.CardinalityEstimator
	bufferCache          *BufferPoolCache // Optional: models buffer pool cache effects
	tx                   *transaction.TransactionContext

	// Tunable cost parameters based on hardware characteristics
	IOCostPerPage   float64 // Cost of reading one page from disk
	CPUCostPerTuple float64 // Cost of processing one tuple
	MemoryPages     int     // Available buffer pool pages
	HashTableMemory int     // Memory available for hash tables (in pages)
	SortMemory      int     // Memory available for sorting (in pages)
}

// NewCostModel creates a new cost model with default parameters.
// The transaction context is stored and used for all statistics lookups.
// Returns an error if the cardinality estimator cannot be initialized.
func NewCostModel(cat *catalog.SystemCatalog, tx *transaction.TransactionContext) (*CostModel, error) {
	cardEst, err := cardinality.NewCardinalityEstimator(cat, tx)
	if err != nil {
		return nil, fmt.Errorf("failed to create cardinality estimator: %w", err)
	}

	return &CostModel{
		catalog:              cat,
		cardinalityEstimator: cardEst,
		tx:                   tx,
		IOCostPerPage:        IoCostPerPage,
		CPUCostPerTuple:      CPUCostPerTuple,
		MemoryPages:          DefaultMemoryPages,
		HashTableMemory:      DefaultHashTableMemory,
		SortMemory:           DefaultSortMemory,
	}, nil
}

// EstimatePlanCost estimates the total cost of executing a plan node.
// This is the main entry point for cost estimation.
//
// The cost includes:
// - Cost of child nodes (recursively)
// - Cost of the operation itself (scan, join, filter, etc.)
// - I/O and CPU costs combined into a single metric
//
// Returns the estimated cost in arbitrary units.
func (cm *CostModel) EstimatePlanCost(planNode plan.PlanNode) float64 {
	if planNode == nil {
		return 0.0
	}

	if planNode.GetCardinality() == 0 {
		card, err := cm.cardinalityEstimator.EstimatePlanCardinality(planNode)
		if err != nil {
			card = DefaultTableCardinality
		}
		planNode.SetCardinality(card)
	}

	var cost float64
	switch node := planNode.(type) {
	case *plan.ScanNode:
		cost = cm.estimateScanCost(node)
	case *plan.JoinNode:
		cost = cm.estimateJoinCost(node)
	case *plan.FilterNode:
		cost = cm.estimateFilterCost(node)
	case *plan.ProjectNode:
		cost = cm.estimateProjectCost(node)
	case *plan.AggregateNode:
		cost = cm.estimateAggregateCost(node)
	case *plan.SortNode:
		cost = cm.estimateSortCost(node)
	case *plan.LimitNode:
		cost = cm.estimateLimitCost(node)
	default:
		cost = 0.0
	}

	planNode.SetCost(cost)
	return cost
}

// SetCostParameters allows tuning cost model parameters for specific hardware.
// This can be used to calibrate the cost model based on actual system performance.
//
// Parameters:
// - ioCostPerPage: Relative cost of reading one page (adjust based on disk speed)
// - cpuCostPerTuple: Relative cost of processing one tuple (adjust based on CPU speed)
// - memoryPages: Total buffer pool size in pages
func (cm *CostModel) SetCostParameters(
	ioCostPerPage,
	cpuCostPerTuple float64,
	memoryPages int,
) {
	cm.IOCostPerPage = ioCostPerPage
	cm.CPUCostPerTuple = cpuCostPerTuple
	cm.MemoryPages = memoryPages
	cm.HashTableMemory = memoryPages / 2 // Allocate half for hash joins
	cm.SortMemory = memoryPages / 2      // Allocate half for sorting
}

// GetCardinalityEstimator returns the cardinality estimator used by this cost model.
// This allows other optimizer components to access cardinality estimation directly.
func (cm *CostModel) GetCardinalityEstimator() *cardinality.CardinalityEstimator {
	return cm.cardinalityEstimator
}
