package planner

import (
	"encoding/json"
	"fmt"
	"storemy/pkg/parser/statements"
)

// ExplainPlan orchestrates the execution of an EXPLAIN statement.
// It generates a query execution plan without necessarily executing the query.
type ExplainPlan struct {
	ctx       DbContext
	tx        TransactionCtx
	statement *statements.ExplainStatement
}

// NewExplainPlan creates a new EXPLAIN query execution plan.
func NewExplainPlan(stmt *statements.ExplainStatement, tx TransactionCtx, ctx DbContext) *ExplainPlan {
	return &ExplainPlan{
		ctx:       ctx,
		tx:        tx,
		statement: stmt,
	}
}

// Execute generates the query execution plan for the underlying statement.
//
// Execution flow:
//  1. Parse and plan the underlying statement
//  2. Generate the plan explanation in the requested format
//  3. If ANALYZE is specified, also execute the query and collect actual statistics
func (p *ExplainPlan) Execute() (Result, error) {
	underlyingStmt := p.statement.GetStatement()
	options := p.statement.GetOptions()

	planner := NewQueryPlanner(p.ctx)
	plan, err := planner.Plan(underlyingStmt, p.tx)
	if err != nil {
		return nil, fmt.Errorf("failed to plan statement: %w", err)
	}

	var explanation string
	if selectPlan, ok := plan.(*SelectPlan); ok {
		explanation, err = p.explainSelectPlan(selectPlan, options)
		if err != nil {
			return nil, err
		}
	} else {
		explanation = fmt.Sprintf("Statement Type: %s\n", underlyingStmt.GetType())
		explanation += fmt.Sprintf("SQL: %s\n", underlyingStmt.String())

		if options.Analyze {
			result, err := plan.Execute()
			if err != nil {
				return nil, fmt.Errorf("failed to execute statement for ANALYZE: %w", err)
			}
			explanation += fmt.Sprintf("\nExecution Result: %s\n", result.String())
		}
	}

	return &ExplainResult{
		Plan:    explanation,
		Format:  options.Format,
		Analyze: options.Analyze,
	}, nil
}

// explainSelectPlan generates an explanation for a SELECT query plan
func (p *ExplainPlan) explainSelectPlan(selectPlan *SelectPlan, options statements.ExplainOptions) (string, error) {
	planTree := selectPlan.statement.Plan

	var explanation string
	switch options.Format {
	case "TEXT", "":
		explanation = "Query Plan:\n"
		explanation += "===========\n\n"

		explanation += "Statement Type: SELECT\n"
		explanation += fmt.Sprintf("Tables: %d\n", len(planTree.Tables()))

		for i, table := range planTree.Tables() {
			explanation += fmt.Sprintf("  %d. %s (ID: %d)\n", i+1, table.TableName, table.TableID)
			if table.Alias != "" && table.Alias != table.TableName {
				explanation += fmt.Sprintf("     Alias: %s\n", table.Alias)
			}
		}

		if len(planTree.Joins()) > 0 {
			explanation += fmt.Sprintf("\nJoins: %d\n", len(planTree.Joins()))
			for i, join := range planTree.Joins() {
				explanation += fmt.Sprintf("  %d. %s JOIN on %s = %s\n",
					i+1, join.JoinType, join.LeftColumn, join.RightColumn)
			}
		}

		if len(planTree.Filters()) > 0 {
			explanation += fmt.Sprintf("\nFilters (WHERE clause): %d conditions\n", len(planTree.Filters()))
			for i, filter := range planTree.Filters() {
				explanation += fmt.Sprintf("  %d. %s\n", i+1, filter.String())
			}
		}

		if planTree.SelectAll() {
			explanation += "\nProjection: SELECT *\n"
		} else {
			explanation += fmt.Sprintf("\nProjection: %d columns\n", len(planTree.SelectList()))
			for i, col := range planTree.SelectList() {
				explanation += fmt.Sprintf("  %d. %s\n", i+1, col)
			}
		}

		if planTree.GroupByField() != "" {
			explanation += fmt.Sprintf("\nGroup By: %s\n", planTree.GroupByField())
		}

		if planTree.HasAgg() {
			explanation += "\nAggregation: Yes\n"
		}

		if planTree.HasOrderBy() {
			direction := "ASC"
			if !planTree.OrderByAsc() {
				direction = "DESC"
			}
			explanation += fmt.Sprintf("\nOrder By: %s %s\n", planTree.OrderByField(), direction)
		}

		if planTree.IsDistinct() {
			explanation += "\nDistinct: Yes\n"
		}

	case "JSON":
		jsonData, err := p.planToJSON(planTree)
		if err != nil {
			return "", fmt.Errorf("failed to convert plan to JSON: %w", err)
		}
		explanation = string(jsonData)

	default:
		return "", fmt.Errorf("unsupported format: %s", options.Format)
	}

	if options.Analyze {
		explanation += "\n\n=== ANALYZE Results ===\n"
		result, err := selectPlan.Execute()
		if err != nil {
			return "", fmt.Errorf("failed to execute query for ANALYZE: %w", err)
		}

		if selectResult, ok := result.(*SelectQueryResult); ok {
			explanation += fmt.Sprintf("Actual rows returned: %d\n", len(selectResult.Tuples))
		}
	}

	return explanation, nil
}

// planToJSON converts a SelectPlan to JSON format
func (p *ExplainPlan) planToJSON(planTree interface{}) ([]byte, error) {
	planMap := map[string]any{
		"statement_type": "SELECT",
	}

	return json.MarshalIndent(planMap, "", "  ")
}
