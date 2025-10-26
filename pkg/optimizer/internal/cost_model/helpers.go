package costmodel

import (
	"math"
	"storemy/pkg/catalog/systemtable"
)

// calculateTuplesPerPage estimates how many tuples fit in a page
// based on average tuple size from table statistics
func calculateTuplesPerPage(stats *systemtable.TableStatistics, pageSize int) float64 {
	if stats == nil || stats.AvgTupleSize <= 0 {
		return DefaultTuplesPerPage
	}

	// Account for page header overhead (assume ~24 bytes per page)
	usablePageSize := float64(pageSize - 24)
	tuplesPerPage := usablePageSize / float64(stats.AvgTupleSize)

	if tuplesPerPage < 1.0 {
		return 1.0 // At least one tuple per page
	}

	return tuplesPerPage
}

// estimatePagesForTuples calculates how many pages are needed for N tuples
func estimatePagesForTuples(numTuples int64, tuplesPerPage float64) float64 {
	if tuplesPerPage <= 0 {
		tuplesPerPage = DefaultTuplesPerPage
	}
	return math.Ceil(float64(numTuples) / tuplesPerPage)
}

// canShortCircuit determines if an operator can stop early with LIMIT
// Operators like Sort, Aggregate (with GROUP BY), and some Joins cannot short-circuit
func canShortCircuit(nodeType string) bool {
	switch nodeType {
	case "Sort", "Aggregate", "Hash Join":
		return false // Must process all input before producing output
	case "Scan", "Filter", "Project", "Nested Loop Join":
		return true // Can stop early
	default:
		return false // Conservative: assume cannot short-circuit
	}
}
