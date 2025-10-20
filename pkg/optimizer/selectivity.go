package optimizer

import (
	"math"
	"storemy/pkg/catalog"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/primitives"
	"storemy/pkg/types"
)

// Default selectivity constants
const (
	DefaultSelectivity  = 0.1  // 10% - general unknown predicate
	EqualitySelectivity = 0.01 // 1% - equality without statistics
	RangeSelectivity    = 0.33 // 33% - range predicate without statistics
	LikeSelectivity     = 0.2  // 20% - LIKE predicate
	InSelectivity       = 0.05 // 5% - IN predicate
	NullSelectivity     = 0.05 // 5% - IS NULL predicate
	MaxSelectivity      = 1.0  // 100% - all rows
)

// SelectivityEstimator estimates the selectivity of predicates
// Selectivity is a value between 0.0 and 1.0 representing the fraction
// of rows that satisfy a predicate
type SelectivityEstimator struct {
	catalog *catalog.SystemCatalog
}

// NewSelectivityEstimator creates a new selectivity estimator
func NewSelectivityEstimator(cat *catalog.SystemCatalog) *SelectivityEstimator {
	return &SelectivityEstimator{
		catalog: cat,
	}
}

// EstimatePredicateSelectivity estimates the selectivity of a predicate
func (se *SelectivityEstimator) EstimatePredicateSelectivity(
	tx *transaction.TransactionContext,
	pred primitives.Predicate,
	tableID int,
	columnName string,
) float64 {
	colStats, err := se.catalog.GetColumnStatistics(tx, tableID, columnName)
	if err != nil || colStats == nil {
		return se.getDefaultSelectivityForOp(pred)
	}

	if colStats.Histogram != nil {
		// For now, return default until we have value extraction
		// In real usage, you'd extract the comparison value
		return se.estimateSelectivityFromDistinctCount(pred, colStats)
	}
	return se.estimateSelectivityFromDistinctCount(pred, colStats)
}

// estimateSelectivityFromDistinctCount estimates selectivity based on distinct count
func (se *SelectivityEstimator) estimateSelectivityFromDistinctCount(
	pred primitives.Predicate,
	colStats *catalog.ColumnStatistics,
) float64 {
	if colStats.DistinctCount == 0 {
		return 0.0
	}

	switch pred {
	case primitives.Equals:
		return 1.0 / float64(colStats.DistinctCount)

	case primitives.NotEqual:
		return 1.0 - (1.0 / float64(colStats.DistinctCount))

	case primitives.GreaterThan, primitives.GreaterThanOrEqual, primitives.LessThan, primitives.LessThanOrEqual:
		return RangeSelectivity

	default:
		return DefaultSelectivity
	}
}

// EstimateHistogramSelectivity uses histogram for accurate selectivity
func (se *SelectivityEstimator) EstimateHistogramSelectivity(
	histogram *catalog.Histogram,
	pred primitives.Predicate,
	value types.Field,
) float64 {
	if histogram == nil {
		return DefaultSelectivity
	}

	return histogram.EstimateSelectivity(pred, value)
}

// EstimateNullSelectivity estimates selectivity for NULL checks
func (se *SelectivityEstimator) EstimateNullSelectivity(
	tx *transaction.TransactionContext,
	tableID int,
	columnName string,
	isNull bool,
) float64 {
	colStats, err := se.catalog.GetColumnStatistics(tx, tableID, columnName)

	nullify := func(val float64) float64 {
		if isNull {
			return val
		}
		return 1.0 - val
	}

	if err != nil || colStats == nil {
		return nullify(NullSelectivity)
	}

	tableStats, err := se.catalog.GetTableStatistics(tx, tableID)
	if err != nil || tableStats == nil || tableStats.Cardinality == 0 {
		return nullify(NullSelectivity)
	}

	nullFraction := float64(colStats.NullCount) / float64(tableStats.Cardinality)
	return nullify(nullFraction)
}

// EstimateCombinedSelectivity estimates selectivity for combined predicates
func (se *SelectivityEstimator) EstimateCombinedSelectivity(
	sel1, sel2 float64,
	isAnd bool,
) float64 {
	if isAnd {
		// sel(A AND B) = sel(A) * sel(B) (assuming independence)
		return sel1 * sel2
	}
	// sel(A OR B) = sel(A) + sel(B) - sel(A) * sel(B)
	return sel1 + sel2 - (sel1 * sel2)
}

// EstimateNotSelectivity estimates selectivity for NOT predicate
func (se *SelectivityEstimator) EstimateNotSelectivity(sel float64) float64 {
	return 1.0 - sel
}

func (se *SelectivityEstimator) getDefaultSelectivityForOp(pred primitives.Predicate) float64 {
	switch pred {
	case primitives.Equals:
		return EqualitySelectivity
	case primitives.NotEqual:
		return 1.0 - EqualitySelectivity
	case primitives.GreaterThan, primitives.GreaterThanOrEqual, primitives.LessThan, primitives.LessThanOrEqual:
		return RangeSelectivity
	default:
		return DefaultSelectivity
	}
}

// EstimateLikeSelectivity estimates selectivity for LIKE predicates
func (se *SelectivityEstimator) EstimateLikeSelectivity(pattern string) float64 {
	if pattern == "" {
		return LikeSelectivity
	}

	if len(pattern) > 0 && pattern[0] != '%' && pattern[len(pattern)-1] == '%' {
		// Prefix match: relatively selective
		return 0.1
	} else if len(pattern) > 0 && pattern[0] == '%' && pattern[len(pattern)-1] != '%' {
		// Suffix match: less selective
		return 0.3
	} else if len(pattern) > 1 && pattern[0] == '%' && pattern[len(pattern)-1] == '%' {
		// Substring match: least selective
		return 0.5
	} else {
		// Exact match or no wildcards: very selective
		return 0.01
	}
}

// EstimateInSelectivity estimates selectivity for IN predicates
func (se *SelectivityEstimator) EstimateInSelectivity(
	tx *transaction.TransactionContext,
	tableID int,
	columnName string,
	valueCount int,
) float64 {
	colStats, err := se.catalog.GetColumnStatistics(tx, tableID, columnName)
	if err != nil || colStats == nil || colStats.DistinctCount == 0 {
		return InSelectivity
	}

	if valueCount == 0 {
		return 0.0
	}

	selectivity := float64(valueCount) / float64(colStats.DistinctCount)
	return math.Min(selectivity, MaxSelectivity)
}
