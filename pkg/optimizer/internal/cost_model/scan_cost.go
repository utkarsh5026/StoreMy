package costmodel

import (
	"storemy/pkg/catalog/systable"
	"storemy/pkg/plan"
	"storemy/pkg/storage/page"
)

// estimateScanCost estimates the cost of a table scan operation based on the access method.
// It retrieves table statistics from the catalog and delegates to specific cost estimation
// functions based on the scan type (sequential, index, or index-only).
//
// Parameters:
//   - node: The scan plan node containing table ID, access method, and cardinality information
//
// Returns:
//   - float64: Estimated cost in cost units (combines I/O and CPU costs)
//   - Falls back to 100.0 * IOCostPerPage if statistics are unavailable
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
		return cm.estimateSeqScanCost(stats)
	}
}

// estimateSeqScanCost calculates the cost of a full sequential table scan.
//
// Cost Model:
//   - I/O Cost: PageCount × IOCostPerPage × SequentialIoCostFactor
//     Sequential I/O is cheaper than random I/O due to disk prefetching
//   - CPU Cost: Cardinality × CPUCostPerTuple (processing each tuple)
//   - Cache Factor: Applied to reduce cost for recently accessed tables
//
// Sequential scans are efficient for:
//   - Small tables where index overhead isn't justified
//   - Queries accessing most/all rows (high selectivity)
//   - Tables without suitable indexes
//
// Parameters:
//   - stats: Table statistics containing page count and row cardinality
//
// Returns:
//   - float64: Total estimated cost (I/O + CPU)
func (cm *CostModel) estimateSeqScanCost(stats *systable.TableStatistics) float64 {
	ioCost := float64(stats.PageCount) * cm.IOCostPerPage * SequentialIoCostFactor

	ioCost = cm.applyCacheFactor(stats.TableID, ioCost)

	cm.recordTableAccess(stats.TableID)

	cpuCost := float64(stats.Cardinality) * cm.CPUCostPerTuple

	return ioCost + cpuCost
}

// estimateIndexScanCost calculates the cost of an index scan with table lookups.
//
// Index Scan Process:
//  1. B-tree traversal: Navigate from root to leaf nodes (tree height I/Os)
//  2. Index leaf scan: Read matching index entries
//  3. Table lookups: Follow pointers to fetch actual tuples from heap pages
//
// Cost Components:
//   - Index Lookup: BTreeHeight × IOCostPerPage (tree traversal)
//   - Table Access: Depends on clustering factor:
//   - ClusteringFactor = 0.0: Perfectly ordered, sequential I/O
//   - ClusteringFactor = 1.0: Random order, many random I/Os
//   - Mixed clustering uses weighted combination
//   - CPU Cost: Processing returned tuples
//
// Best suited for:
//   - Low selectivity queries (returning small fraction of rows)
//   - Queries with good index clustering
//
// Parameters:
//   - node: Scan node with index ID and estimated cardinality
//   - stats: Table statistics for the scanned table
//
// Returns:
//   - float64: Total estimated cost (index + table I/O + CPU)
//   - Falls back to sequential scan cost if index statistics unavailable
func (cm *CostModel) estimateIndexScanCost(node *plan.ScanNode, stats *systable.TableStatistics) float64 {
	indexStats, err := cm.catalog.GetIndexStatistics(cm.tx, node.IndexID)
	if err != nil || indexStats == nil {
		return cm.estimateSeqScanCost(stats)
	}

	treeHeight := float64(indexStats.BTreeHeight)
	indexLookupCost := treeHeight * cm.IOCostPerPage

	// 2. Table page access cost
	// The clustering factor determines how many random I/Os we need
	// ClusteringFactor ranges from 0.0 (perfectly ordered) to 1.0 (random)
	// - Low clustering factor (close to 0): table is ordered by index, sequential I/O
	// - High clustering factor (close to 1): random access pattern
	outputRows := float64(node.Cardinality)

	randomIOFactor := indexStats.ClusteringFactor
	sequentialIOFactor := 1.0 - indexStats.ClusteringFactor

	tuplesPerPage := calculateTuplesPerPage(stats, page.PageSize)
	tablePages := outputRows / tuplesPerPage

	tableAccessCost := tablePages * cm.IOCostPerPage *
		(randomIOFactor + sequentialIOFactor*SequentialIoCostFactor)

	tableAccessCost = cm.applyCacheFactor(stats.TableID, tableAccessCost)

	cm.recordTableAccess(stats.TableID)

	cpuCost := outputRows * cm.CPUCostPerTuple
	return indexLookupCost + tableAccessCost + cpuCost
}

// estimateIndexOnlyScanCost calculates the cost of an index-only scan.
//
// Index-Only Scan:
//   - Retrieves data directly from index without accessing table pages
//   - Only possible when all required columns are present in the index
//   - Significantly cheaper than regular index scan (no table lookup overhead)
//
// Cost Components:
//  1. B-tree traversal: Navigate to leaf level (tree height I/Os)
//  2. Leaf page scan: Sequential scan of index leaf pages
//  3. CPU cost: Reduced by 50% as index entries are simpler than full tuples
//
// Advantages:
//   - Eliminates random I/O to table pages
//   - Reads less data (only indexed columns)
//   - Sequential access pattern through index leaves
//
// Parameters:
//   - node: Scan node with index ID and cardinality
//   - tableStats: Table statistics (for fallback scenarios)
//
// Returns:
//   - float64: Total estimated cost (index I/O + reduced CPU)
//   - Falls back to sequential scan if index statistics unavailable
func (cm *CostModel) estimateIndexOnlyScanCost(node *plan.ScanNode, tableStats *systable.TableStatistics) float64 {
	indexStats, err := cm.catalog.GetIndexStatistics(cm.tx, node.IndexID)
	if err != nil || indexStats == nil {
		return cm.estimateSeqScanCost(tableStats)
	}

	// 1. B-tree traversal cost (root to leaf)
	treeHeight := float64(indexStats.BTreeHeight)
	indexLookupCost := treeHeight * cm.IOCostPerPage

	// 2. Sequential scan of index leaf pages
	// Index-only scan reads index pages sequentially (no table access needed)
	outputRows := float64(node.Cardinality)

	// Index entries are typically smaller than full tuples
	indexPagesScanned := outputRows / DefaultTuplesPerPage
	indexScanCost := indexPagesScanned * cm.IOCostPerPage * SequentialIoCostFactor

	// 3. Reduced CPU cost (processing index entries is cheaper than full tuples)
	cpuCost := outputRows * cm.CPUCostPerTuple * 0.5

	return indexLookupCost + indexScanCost + cpuCost
}
