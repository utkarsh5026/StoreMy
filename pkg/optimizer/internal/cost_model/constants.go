package costmodel

import "storemy/pkg/optimizer/internal/cardinality"

const (
	// I/O cost parameters
	IoCostPerPage          = 1.0  // Cost of reading one page from disk
	SequentialIoCostFactor = 0.1  // Sequential I/O is ~10x cheaper than random
	CPUCostPerTuple        = 0.01 // Cost of processing one tuple

	// Memory parameters (default values)
	DefaultMemoryPages     = 1000 // 1000 pages = ~8MB with 8KB pages
	DefaultHashTableMemory = 500  // Half of buffer pool for hash tables
	DefaultSortMemory      = 500  // Half of buffer pool for sorting

	// Estimation parameters
	DefaultTuplesPerPage                            = 100  // Default tuples per page if not calculable
	DefaultTableCardinality cardinality.Cardinality = 1000 // Default cardinality when statistics are missing

	// Hash join parameters
	HashBuildCPUFactor = 2.0 // Building hash table is more expensive
	HashProbeCPUFactor = 1.5 // Hash lookup overhead

	// Projection/filter parameters
	ProjectionCPUFactor = 0.5 // Projection is lighter than full tuple processing
	FilterCPUFactor     = 1.0 // Filter evaluation cost multiplier

	// Aggregation parameters
	SimpleAggCPUFactor = 1.0 // No GROUP BY
	GroupAggCPUFactor  = 2.0 // With GROUP BY (hash + aggregate)

	// Short-circuit threshold for LIMIT optimization
	LimitShortCircuitRatio = 0.1 // If limit < 10% of output, assume short-circuit
)
