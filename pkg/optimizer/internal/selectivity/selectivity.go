package selectivity

import (
	"math"
	"storemy/pkg/catalog/catalogmanager"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/optimizer/statistics"
	"storemy/pkg/primitives"
	"storemy/pkg/types"
)

type Selectivity float64

// Default selectivity constants used when statistical information is unavailable.
// Selectivity represents the fraction of rows (0.0 to 1.0) that satisfy a predicate.
const (
	DefaultSelectivity  Selectivity = 0.1  // 10% - general unknown predicate without statistics
	EqualitySelectivity Selectivity = 0.01 // 1% - equality predicate without statistics
	RangeSelectivity    Selectivity = 0.33 // 33% - range predicate (>, <, >=, <=) without statistics
	LikeSelectivity     Selectivity = 0.2  // 20% - LIKE predicate without statistics
	InSelectivity       Selectivity = 0.05 // 5% - IN predicate without statistics
	NullSelectivity     Selectivity = 0.05 // 5% - IS NULL predicate without statistics
	MaxSelectivity      Selectivity = 1.0  // 100% - all rows satisfy the predicate
)

// SelectivityEstimator estimates the selectivity of predicates for query optimization.
// It uses column statistics including histograms, most common values (MCV), and distinct
// counts to provide accurate estimates. Selectivity estimates are crucial for the query
// optimizer to choose efficient execution plans.
//
// The estimator supports:
//   - Basic predicates (=, !=, <, >, <=, >=)
//   - NULL checks (IS NULL, IS NOT NULL)
//   - Pattern matching (LIKE)
//   - Set membership (IN)
//   - Combined predicates (AND, OR, NOT)
type SelectivityEstimator struct {
	catalog *catalogmanager.CatalogManager
	tx      *transaction.TransactionContext
}

// NewSelectivityEstimator creates a new selectivity estimator.
//
// Parameters:
//   - cat: CatalogManager containing table and column statistics
//   - tx: Transaction context for accessing catalog statistics
//
// Returns:
//   - *SelectivityEstimator: A new selectivity estimator instance
func NewSelectivityEstimator(cat *catalogmanager.CatalogManager, tx *transaction.TransactionContext) *SelectivityEstimator {
	return &SelectivityEstimator{
		catalog: cat,
		tx:      tx,
	}
}

// EstimatePredicateSelectivity estimates the selectivity of a predicate when the
// comparison value is not available. Falls back to distinct count or default
// selectivity estimates.
//
// This method is used when the optimizer knows the predicate type but not the
// specific value being compared (e.g., during join selectivity estimation).
//
// Parameters:
//   - pred: The predicate operator (=, !=, <, >, etc.)
//   - tableID: ID of the table containing the column
//   - columnName: Name of the column in the predicate
//
// Returns:
//   - Selectivity: Estimated selectivity between 0.0 and 1.0
func (se *SelectivityEstimator) EstimatePredicateSelectivity(pred primitives.Predicate, tableID primitives.FileID, columnName string) Selectivity {
	if se.catalog == nil {
		return se.defaultSel(pred)
	}

	colStats, err := se.catalog.GetColumnStatistics(se.tx, tableID, columnName)
	if err != nil || colStats == nil {
		return se.defaultSel(pred)
	}

	return se.fromDistinct(pred, colStats)
}

// EstimateWithValue estimates the selectivity of a predicate
// when the comparison value is known. Uses MCV (Most Common Values) and histogram
// statistics for accurate estimation when available.
//
// This is the most accurate estimation method as it can leverage:
//   - MCV frequencies for common values
//   - Histogram buckets for range predicates
//   - Distinct count for uniform distribution assumptions
//
// Parameters:
//   - pred: The predicate operator (=, !=, <, >, etc.)
//   - tableID: ID of the table containing the column
//   - columnName: Name of the column in the predicate
//   - value: The specific value being compared against
//
// Returns:
//   - Selectivity: Estimated selectivity between 0.0 and 1.0
func (se *SelectivityEstimator) EstimateWithValue(pred primitives.Predicate, tableID primitives.FileID, columnName string, value types.Field) Selectivity {
	if se.catalog == nil {
		return se.defaultSel(pred)
	}

	colStats, err := se.catalog.GetColumnStatistics(se.tx, tableID, columnName)
	if err != nil || colStats == nil {
		return se.defaultSel(pred)
	}

	switch pred {
	case primitives.Equals:
		if mcvFreq, found := se.checkMCV(colStats, value); found {
			return mcvFreq
		}
	case primitives.NotEqual, primitives.NotEqualsBracket:
		if mcvFreq, found := se.checkMCV(colStats, value); found {
			return MaxSelectivity - mcvFreq
		}
	}

	if colStats.Histogram != nil && isComparisonPredicate(pred) {
		sel := se.estimateHist(colStats.Histogram, pred, value)
		return Selectivity(math.Max(0.0, math.Min(1.0, float64(sel))))
	}

	if pred == primitives.Equals && colStats.DistinctCount > 0 {
		return se.equalityNoHist(colStats)
	}

	return se.defaultSel(pred)
}

// fromDistinct estimates selectivity based on distinct count,
// assuming uniform distribution of values.
//
// Parameters:
//   - pred: The predicate operator
//   - colStats: Column statistics containing distinct count
//
// Returns:
//   - Selectivity: Estimated selectivity between 0.0 and 1.0
func (se *SelectivityEstimator) fromDistinct(pred primitives.Predicate, stats *catalogmanager.ColumnStatistics) Selectivity {
	dc := stats.DistinctCount
	if dc == 0 {
		return 0.0
	}

	switch pred {
	case primitives.Equals:
		return Selectivity(1.0 / float64(dc))

	case primitives.NotEqual:
		return Selectivity(1.0 - (1.0 / float64(dc)))

	case primitives.GreaterThan, primitives.GreaterThanOrEqual, primitives.LessThan, primitives.LessThanOrEqual:
		return RangeSelectivity

	default:
		return DefaultSelectivity
	}
}

// estimateHist uses histogram statistics for accurate selectivity
// estimation of comparison predicates.
//
// Histograms divide the value range into buckets and track the number of rows
// in each bucket, enabling accurate range query selectivity estimation.
//
// Parameters:
//   - h: The h statistics for the column
//   - pred: The predicate operator
//   - value: The value being compared against
//
// Returns:
//   - Selectivity: Estimated selectivity between 0.0 and 1.0
func (se *SelectivityEstimator) estimateHist(h *statistics.Histogram, pred primitives.Predicate, value types.Field) Selectivity {
	if h == nil {
		return DefaultSelectivity
	}

	return Selectivity(h.EstimateSelectivity(pred, value))
}

// EstimateNull estimates selectivity for NULL checks (IS NULL / IS NOT NULL).
//
// Uses the null count from column statistics to calculate the exact fraction
// of null values in the column.
//
// Parameters:
//   - tableID: ID of the table containing the column
//   - columnName: Name of the column being checked
//   - isNull: true for IS NULL, false for IS NOT NULL
//
// Returns:
//   - Selectivity: Estimated selectivity between 0.0 and 1.0
func (se *SelectivityEstimator) EstimateNull(tableID primitives.FileID, columnName string, isNull bool) Selectivity {
	nullify := func(val Selectivity) Selectivity {
		if isNull {
			return val
		}
		return MaxSelectivity - val
	}

	if se.catalog == nil {
		return nullify(NullSelectivity)
	}

	colStats, err := se.catalog.GetColumnStatistics(se.tx, tableID, columnName)
	if err != nil || colStats == nil {
		return nullify(NullSelectivity)
	}

	tableStats, err := se.catalog.GetTableStatistics(se.tx, tableID)
	if err != nil || tableStats == nil || tableStats.Cardinality == 0 {
		return nullify(NullSelectivity)
	}

	nullFraction := Selectivity(float64(colStats.NullCount) / float64(tableStats.Cardinality))
	return nullify(nullFraction)
}

// EstimateCombined estimates selectivity for combined predicates (AND/OR).
//
// Uses probability theory assuming predicate independence:
//   - AND: sel(A AND B) = sel(A) × sel(B)
//   - OR:  sel(A OR B) = sel(A) + sel(B) - sel(A) × sel(B)
//
// Parameters:
//   - s1: Selectivity of first predicate
//   - s2: Selectivity of second predicate
//   - and: true for AND, false for OR
//
// Returns:
//   - Selectivity: Combined selectivity between 0.0 and 1.0
func (se *SelectivityEstimator) EstimateCombined(s1, s2 Selectivity, and bool) Selectivity {
	if and {
		return s1 * s2
	}
	return s1 + s2 - (s1 * s2)
}

// EstimateNot estimates selectivity for NOT predicate.
//
// Simply inverts the selectivity: sel(NOT A) = 1 - sel(A)
//
// Parameters:
//   - sel: Selectivity of the inner predicate
//
// Returns:
//   - Selectivity: Inverted selectivity between 0.0 and 1.0
func (se *SelectivityEstimator) EstimateNot(sel Selectivity) Selectivity {
	return MaxSelectivity - sel
}

// defaultSel returns a default selectivity estimate based on
// the predicate operator when no statistics are available.
//
// Parameters:
//   - pred: The predicate operator
//
// Returns:
//   - Selectivity: Default selectivity estimate
func (se *SelectivityEstimator) defaultSel(pred primitives.Predicate) Selectivity {
	switch pred {
	case primitives.Equals:
		return EqualitySelectivity
	case primitives.NotEqual:
		return MaxSelectivity - EqualitySelectivity
	case primitives.GreaterThan, primitives.GreaterThanOrEqual, primitives.LessThan, primitives.LessThanOrEqual:
		return RangeSelectivity
	default:
		return DefaultSelectivity
	}
}

// EstimateLike estimates selectivity for LIKE predicates based on
// the pattern structure.
//
// Different wildcard patterns have different selectivities:
//   - 'prefix%': 0.1 (prefix match is selective)
//   - '%suffix': 0.3 (suffix match is less selective)
//   - '%substring%': 0.5 (substring match is least selective)
//   - 'exact' or no wildcards: 0.01 (very selective)
//
// Parameters:
//   - pattern: The LIKE pattern string
//
// Returns:
//   - Selectivity: Estimated selectivity between 0.0 and 1.0
func (se *SelectivityEstimator) EstimateLike(pattern string) Selectivity {
	if pattern == "" {
		return LikeSelectivity
	}

	if len(pattern) > 0 && pattern[0] != '%' && pattern[len(pattern)-1] == '%' {
		return 0.1
	} else if len(pattern) > 0 && pattern[0] == '%' && pattern[len(pattern)-1] != '%' {
		return 0.3
	} else if len(pattern) > 1 && pattern[0] == '%' && pattern[len(pattern)-1] == '%' {
		return 0.5
	} else {

		return 0.01
	}
}

// EstimateIn estimates selectivity for IN predicates.
//
// Assumes each value in the IN list is equally likely and uses the ratio
// of IN list size to distinct count as the selectivity estimate.
//
// Parameters:
//   - tableID: ID of the table containing the column
//   - columnName: Name of the column being checked
//   - valueCount: Number of values in the IN list
//
// Returns:
//   - Selectivity: Estimated selectivity between 0.0 and 1.0
func (se *SelectivityEstimator) EstimateIn(tableID primitives.FileID, columnName string, valueCount int) Selectivity {
	if se.catalog == nil {
		return InSelectivity
	}

	colStats, err := se.catalog.GetColumnStatistics(se.tx, tableID, columnName)
	if err != nil || colStats == nil || colStats.DistinctCount == 0 {
		return InSelectivity
	}

	if valueCount == 0 {
		return 0.0
	}

	selectivity := Selectivity(float64(valueCount) / float64(colStats.DistinctCount))
	if selectivity > MaxSelectivity {
		return MaxSelectivity
	}
	return selectivity
}

// isComparisonPredicate checks if a predicate is a comparison operator that
// can benefit from histogram statistics.
//
// Parameters:
//   - pred: The predicate operator to check
//
// Returns:
//   - bool: true if the predicate is a comparison operator
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

// checkMCV searches for a value in the Most Common Values (MCV) list
// and returns its frequency if found.
//
// MCVs track the most frequently occurring values and their exact frequencies,
// enabling accurate selectivity for common values.
//
// Parameters:
//   - colStats: Column statistics containing MCV list
//   - value: The value to search for
//
// Returns:
//   - Selectivity: Frequency of the value (0.0 to 1.0)
//   - bool: true if the value was found in the MCV list
func (se *SelectivityEstimator) checkMCV(colStats *catalogmanager.ColumnStatistics, value types.Field) (freq Selectivity, found bool) {
	if len(colStats.MostCommonVals) == 0 {
		return 0.0, false
	}

	for i, mcv := range colStats.MostCommonVals {
		if i >= len(colStats.MCVFreqs) {
			break
		}

		equal, err := value.Compare(primitives.Equals, mcv)
		if err == nil && equal {
			return Selectivity(colStats.MCVFreqs[i]), true
		}
	}

	return 0.0, false
}

// equalityNoHist estimates equality selectivity without histogram
// but accounts for MCVs by distributing remaining probability among non-MCV values.
//
// This assumes MCVs account for a portion of the data, and the remaining data is
// uniformly distributed among non-MCV distinct values.
//
// Parameters:
//   - colStats: Column statistics with MCV information
//
// Returns:
//   - Selectivity: Estimated selectivity for equality on non-MCV value
func (se *SelectivityEstimator) equalityNoHist(colStats *catalogmanager.ColumnStatistics) Selectivity {
	if colStats.DistinctCount == 0 {
		return 0.0
	}

	mcvTotalFreq := 0.0
	for _, freq := range colStats.MCVFreqs {
		mcvTotalFreq += freq
	}

	remainingFreq := math.Max(1.0-mcvTotalFreq, 0)

	nonMCVDistinct := int(colStats.DistinctCount) - len(colStats.MostCommonVals)
	if nonMCVDistinct <= 0 {
		return Selectivity(1.0 / float64(colStats.DistinctCount))
	}

	return Selectivity(remainingFreq / float64(nonMCVDistinct))
}
