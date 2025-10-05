package statements

import "storemy/pkg/parser/plan"

// SelectStatement represents a SQL SELECT statement with its execution plan
type SelectStatement struct {
	BaseStatement
	Plan *plan.SelectPlan
}

// NewSelectStatement creates a new SELECT statement with the given plan
func NewSelectStatement(plan *plan.SelectPlan) *SelectStatement {
	return &SelectStatement{
		BaseStatement: NewBaseStatement(Select),
		Plan:          plan,
	}
}

// GetPlan returns the execution plan for this SELECT statement
func (ss *SelectStatement) GetPlan() *plan.SelectPlan {
	return ss.Plan
}

// IsSelectAll returns true if the statement uses SELECT *
func (ss *SelectStatement) IsSelectAll() bool {
	return ss.Plan.SelectAll()
}

// HasAggregation returns true if the statement contains aggregation functions
func (ss *SelectStatement) HasAggregation() bool {
	return ss.Plan.HasAgg()
}

// HasGroupBy returns true if the statement has a GROUP BY clause
func (ss *SelectStatement) HasGroupBy() bool {
	return ss.Plan.GroupByField() != ""
}

// HasOrderBy returns true if the statement has an ORDER BY clause
func (ss *SelectStatement) HasOrderBy() bool {
	return ss.Plan.HasOrderBy()
}

// TableCount returns the number of tables in the FROM clause
func (ss *SelectStatement) TableCount() int {
	return len(ss.Plan.Tables())
}

// JoinCount returns the number of JOIN clauses
func (ss *SelectStatement) JoinCount() int {
	return len(ss.Plan.Joins())
}

// FilterCount returns the number of WHERE conditions
func (ss *SelectStatement) FilterCount() int {
	return len(ss.Plan.Filters())
}

// Validate checks if the statement is valid
func (ss *SelectStatement) Validate() error {
	if ss.Plan == nil {
		return NewValidationError(Select, "Plan", "plan cannot be nil")
	}

	if len(ss.Plan.Tables()) == 0 {
		return NewValidationError(Select, "Tables", "at least one table is required in FROM clause")
	}

	return nil
}

// String returns a string representation of the SELECT statement
func (ss *SelectStatement) String() string {
	return ss.Plan.String()
}
