package costmodel

import (
	"storemy/pkg/catalog"
	"storemy/pkg/plan"
	"storemy/pkg/storage/page"
)

// estimateScanCost estimates the cost of a table scan
func (cm *CostModel) estimateScanCost(node *plan.ScanNode) float64 {
	stats, err := cm.catalog.GetTableStatistics(cm.tx, node.TableID)
	if err != nil || stats == nil {
		return 100.0 * cm.IOCostPerPage
	}

	switch node.AccessMethod {
	case "seqscan":
		return cm.estimateSeqScanCost(stats)
	case "indexscan":
		return cm.estimateIndexScanCost(node, stats)
	case "indexonlyscan":
		return cm.estimateIndexOnlyScanCost(node, stats)
	default:
		// Default to sequential scan
		return cm.estimateSeqScanCost(stats)
	}
}

// estimateSeqScanCost estimates cost of sequential table scan
// Sequential scans read all pages sequentially, which is cheaper than random I/O
func (cm *CostModel) estimateSeqScanCost(stats *catalog.TableStatistics) float64 {
	ioCost := float64(stats.PageCount) * cm.IOCostPerPage * SequentialIoCostFactor

	ioCost = cm.applyCacheFactor(stats.TableID, ioCost)

	cm.recordTableAccess(stats.TableID)

	cpuCost := float64(stats.Cardinality) * cm.CPUCostPerTuple

	return ioCost + cpuCost
}

// estimateIndexScanCost estimates cost of index scan with table lookups
// This involves:
// 1. Traversing the B-tree index to find entries
// 2. Following pointers to access table pages (potentially random I/O)
// 3. Processing the resulting tuples
func (cm *CostModel) estimateIndexScanCost(
	node *plan.ScanNode,
	tableStats *catalog.TableStatistics,
) float64 {
	indexStats, err := cm.catalog.GetIndexStatistics(cm.tx, node.IndexID)
	if err != nil || indexStats == nil {
		return cm.estimateSeqScanCost(tableStats)
	}

	// 1. Index traversal cost: navigate down B-tree
	treeHeight := float64(indexStats.Height)
	indexLookupCost := treeHeight * cm.IOCostPerPage

	// 2. Table page access cost
	// The clustering factor determines how many random I/Os we need
	// ClusteringFactor ranges from 0.0 (perfectly ordered) to 1.0 (random)
	// - Low clustering factor (close to 0): table is ordered by index, sequential I/O
	// - High clustering factor (close to 1): random access pattern
	outputRows := float64(node.Cardinality)

	randomIOFactor := indexStats.ClusteringFactor
	sequentialIOFactor := 1.0 - indexStats.ClusteringFactor

	tuplesPerPage := calculateTuplesPerPage(tableStats, page.PageSize)
	tablePages := outputRows / tuplesPerPage

	tableAccessCost := tablePages * cm.IOCostPerPage *
		(randomIOFactor + sequentialIOFactor*SequentialIoCostFactor)

	tableAccessCost = cm.applyCacheFactor(tableStats.TableID, tableAccessCost)

	cm.recordTableAccess(tableStats.TableID)

	cpuCost := outputRows * cm.CPUCostPerTuple
	return indexLookupCost + tableAccessCost + cpuCost
}

// estimateIndexOnlyScanCost estimates cost of index-only scan
// This is cheaper than regular index scan because we don't access table pages
// Only works when all needed columns are in the index
func (cm *CostModel) estimateIndexOnlyScanCost(
	node *plan.ScanNode,
	tableStats *catalog.TableStatistics,
) float64 {
	indexStats, err := cm.catalog.GetIndexStatistics(cm.tx, node.IndexID)
	if err != nil || indexStats == nil {

		return cm.estimateSeqScanCost(tableStats)
	}

	treeHeight := float64(indexStats.Height)
	indexLookupCost := treeHeight * cm.IOCostPerPage

	// 2. Sequential scan of index leaf pages
	// Index-only scan reads index pages sequentially
	outputRows := float64(node.Cardinality)
	indexPagesScanned := outputRows / DefaultTuplesPerPage // Index entries are typically smaller
	indexScanCost := indexPagesScanned * cm.IOCostPerPage * SequentialIoCostFactor

	// 3. CPU cost for processing index entries
	cpuCost := outputRows * cm.CPUCostPerTuple * 0.5 // Cheaper: no table tuple materialization

	return indexLookupCost + indexScanCost + cpuCost
}
