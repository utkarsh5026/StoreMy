package plan

type SelectPlan struct {
	groupByField string // GROUP BY field (only one supported)
	hasAgg       bool   // Whether query has aggregation
	aggOp        string // Aggregation operation
	aggField     string // Field being aggregated
	hasOrderBy   bool   // Whether query has ORDER BY
	orderByAsc   bool   // ORDER BY direction
	orderByField string // ORDER BY field
	query        string // Original SQL query string
}

func NewSelectPlan() *SelectPlan {
	return &SelectPlan{}
}
