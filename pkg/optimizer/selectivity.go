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
// when the comparison value is not available. Falls back to distinct count
// or default selectivity estimates.
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

	return se.estimateSelectivityFromDistinctCount(pred, colStats)
}

// EstimatePredicateSelectivityWithValue estimates the selectivity of a predicate
// when the comparison value is known. Uses MCV (Most Common Values) and histogram
// statistics for accurate estimation when available.
func (se *SelectivityEstimator) EstimatePredicateSelectivityWithValue(
	tx *transaction.TransactionContext,
	pred primitives.Predicate,
	tableID int,
	columnName string,
	value types.Field,
) float64 {
	colStats, err := se.catalog.GetColumnStatistics(tx, tableID, columnName)
	if err != nil || colStats == nil {
		return se.getDefaultSelectivityForOp(pred)
	}

	switch pred {
	case primitives.Equals:
		if mcvFreq, found := se.findMCVFrequency(colStats, value); found {
			return mcvFreq
		}
	case primitives.NotEqual, primitives.NotEqualsBracket:
		if mcvFreq, found := se.findMCVFrequency(colStats, value); found {
			return 1.0 - mcvFreq
		}
	}

	if colStats.Histogram != nil && isComparisonPredicate(pred) {
		sel := se.estimateHistogramSelectivityWithMCV(colStats, pred, value)
		return math.Max(0.0, math.Min(1.0, sel))
	}

	// Fall back to distinct count for equality when histogram unavailable
	if pred == primitives.Equals && colStats.DistinctCount > 0 {
		return se.estimateEqualityWithoutHistogram(colStats)
	}

	return se.getDefaultSelectivityForOp(pred)
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

// isComparisonPredicate checks if a predicate is a comparison that can use histograms
func isComparisonPredicate(pred primitives.Predicate) bool {
	switch pred {
	case primitives.Equals,
		primitives.NotEqual,
		primitives.NotEqualsBracket,
		primitives.GreaterThan,
		primitives.GreaterThanOrEqual,
		primitives.LessThan,
		primitives.LessThanOrEqual:
		return true
	default:
		return false
	}
}

// findMCVFrequency searches for a value in the Most Common Values list
// and returns its frequency if found
func (se *SelectivityEstimator) findMCVFrequency(
	colStats *catalog.ColumnStatistics,
	value types.Field,
) (float64, bool) {
	if len(colStats.MostCommonVals) == 0 {
		return 0.0, false
	}

	for i, mcv := range colStats.MostCommonVals {
		if i >= len(colStats.MCVFreqs) {
			break
		}

		equal, err := value.Compare(primitives.Equals, mcv)
		if err == nil && equal {
			return colStats.MCVFreqs[i], true
		}
	}

	return 0.0, false
}

// estimateHistogramSelectivityWithMCV uses histogram for selectivity but accounts for MCVs
// MCVs are typically tracked separately from histograms, so we use histogram directly
func (se *SelectivityEstimator) estimateHistogramSelectivityWithMCV(
	colStats *catalog.ColumnStatistics,
	pred primitives.Predicate,
	value types.Field,
) float64 {
	// For range predicates, histogram handles MCVs naturally as they're part of the distribution
	// For equality, we already checked MCVs above, so if we're here, value is not an MCV
	return se.EstimateHistogramSelectivity(colStats.Histogram, pred, value)
}

// estimateEqualityWithoutHistogram estimates equality selectivity without histogram
// but accounts for MCVs by distributing remaining probability among non-MCV values
func (se *SelectivityEstimator) estimateEqualityWithoutHistogram(
	colStats *catalog.ColumnStatistics,
) float64 {
	if colStats.DistinctCount == 0 {
		return 0.0
	}

	mcvTotalFreq := 0.0
	for _, freq := range colStats.MCVFreqs {
		mcvTotalFreq += freq
	}

	remainingFreq := 1.0 - mcvTotalFreq
	if remainingFreq < 0.0 {
		remainingFreq = 0.0
	}

	nonMCVDistinct := colStats.DistinctCount - int64(len(colStats.MostCommonVals))
	if nonMCVDistinct <= 0 {
		return 1.0 / float64(colStats.DistinctCount)
	}

	return remainingFreq / float64(nonMCVDistinct)
}
