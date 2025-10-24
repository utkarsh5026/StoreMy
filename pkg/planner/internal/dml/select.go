package dml

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/execution/aggregation"
	"storemy/pkg/execution/join"
	"storemy/pkg/execution/query"
	"storemy/pkg/execution/setops"
	"storemy/pkg/iterator"
	"storemy/pkg/parser/statements"
	"storemy/pkg/plan"
	"storemy/pkg/planner/internal/metadata"
	"storemy/pkg/planner/internal/result"
	"storemy/pkg/planner/internal/scan"
	"storemy/pkg/registry"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"strings"
)

// SelectPlan orchestrates the execution of a SELECT statement.
// Implements the Planner → Executor pipeline by building an operator tree
// that follows the iterator pattern for query execution.
type SelectPlan struct {
	ctx       *registry.DatabaseContext
	tx        *transaction.TransactionContext
	statement *statements.SelectStatement
}

// NewSelectPlan creates a new SELECT query execution plan.
func NewSelectPlan(stmt *statements.SelectStatement, tx *transaction.TransactionContext, ctx *registry.DatabaseContext) *SelectPlan {
	return &SelectPlan{
		ctx:       ctx,
		tx:        tx,
		statement: stmt,
	}
}

// Execute builds and runs the query operator tree, materializing results.
//
// Execution flow:
//  1. Check if this is a set operation (UNION, INTERSECT, EXCEPT)
//  2. If set operation: recursively execute left and right, then apply set operator
//  3. Otherwise: Build base scan with WHERE filter
//  4. Apply JOINs (if any)
//  5. Apply aggregation/GROUP BY (if any)
//  6. Apply projection/SELECT list (if no aggregation)
//  7. Apply DISTINCT (if specified and no aggregation)
//  8. Apply ORDER BY (if specified)
//  9. Materialize all results via collectAllTuples()
func (p *SelectPlan) Execute() (result.Result, error) {
	if p.statement.Plan.IsSetOperation() {
		return p.executeSetOperation()
	}

	currentOp, err := p.buildScanOperator()
	if err != nil {
		return nil, err
	}

	currentOp, err = p.applyJoinsIfNeeded(currentOp)
	if err != nil {
		return nil, err
	}

	currentOp, err = p.applyAggregationIfNeeded(currentOp)
	if err != nil {
		return nil, err
	}

	if !p.statement.Plan.HasAgg() {
		currentOp, err = p.applyProjectionIfNeeded(currentOp)
		if err != nil {
			return nil, err
		}
	}

	if p.statement.Plan.IsDistinct() && !p.statement.Plan.HasAgg() {
		currentOp, err = p.applyDistinctIfNeeded(currentOp)
		if err != nil {
			return nil, err
		}
	}

	currentOp, err = p.applySortIfNeeded(currentOp)
	if err != nil {
		return nil, err
	}

	results, err := metadata.CollectAllTuples(currentOp)
	if err != nil {
		return nil, err
	}

	return &result.SelectQueryResult{
		TupleDesc: currentOp.GetTupleDesc(),
		Tuples:    results,
	}, nil
}

// buildScanOperator creates the base scan operator with optional WHERE filter.
// This is the foundation of the query execution tree - all other operators build on this.
//
// Process:
//  1. Resolve first table from FROM clause via TableManager
//  2. Extract WHERE filter conditions (if present)
//  3. Create SeqScan operator (acquires page locks via LockManager)
//  4. Wrap in Filter operator if WHERE clause exists
//
// Returns iterator.DbIterator ready to produce tuples from base table.
// Errors if table doesn't exist or scan creation fails.
func (p *SelectPlan) buildScanOperator() (iterator.DbIterator, error) {
	tables := p.statement.Plan.Tables()
	if len(tables) == 0 {
		return nil, fmt.Errorf("SELECT requires at least one table in FROM clause")
	}

	firstTable := tables[0]
	metadata, err := metadata.ResolveTableMetadata(firstTable.TableName, p.tx, p.ctx)
	if err != nil {
		return nil, err
	}

	var filter *plan.FilterNode
	filters := p.statement.Plan.Filters()
	if len(filters) > 0 {
		filter = filters[0]
	}

	scanOp, err := scan.BuildScanWithFilter(p.tx, metadata.TableID, filter, p.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create table scan: %w", err)
	}

	return scanOp, nil
}

// applyProjectionIfNeeded applies the SELECT clause projection if not SELECT *.
// Skipped if query has aggregation (aggregation defines output schema instead).
func (p *SelectPlan) applyProjectionIfNeeded(input iterator.DbIterator) (iterator.DbIterator, error) {
	if p.statement.Plan.SelectAll() {
		return input, nil
	}

	fields := p.statement.Plan.SelectList()
	if len(fields) == 0 {
		return input, nil
	}

	return buildProjection(input, fields)
}

// buildProjection constructs a Project operator from the SELECT field list.
// Maps each field name to its position in the input schema.
//
// Process:
//  1. For each field in SELECT list, find its index in input schema
//  2. Extract field type from input schema
//  3. Create Project operator with field indices and types
func buildProjection(input iterator.DbIterator, selectFields []*plan.SelectListNode) (iterator.DbIterator, error) {
	fieldIndices := make([]int, 0, len(selectFields))
	fieldTypes := make([]types.Type, 0, len(selectFields))
	tupleDesc := input.GetTupleDesc()

	for _, field := range selectFields {
		unqualifiedName := extractFieldName(field.FieldName)
		idx, err := tupleDesc.FindFieldIndex(unqualifiedName)
		if err != nil {
			return nil, err
		}

		fieldIndices = append(fieldIndices, idx)
		fieldType, _ := tupleDesc.TypeAtIndex(idx)
		fieldTypes = append(fieldTypes, fieldType)
	}

	pr, err := query.NewProject(fieldIndices, fieldTypes, input)
	if err != nil {
		return nil, fmt.Errorf("failed to create projection: %v", err)
	}

	return pr, nil
}

// applyJoinsIfNeeded applies all JOIN operations to the input operator.
// Builds a left-deep join tree where each join becomes the left input to the next join.
//
// Join execution:
//  1. Current operator (left) starts as base scan
//  2. For each JOIN clause:
//     a. Build scan for right table
//     b. Construct join predicate from ON condition
//     c. Create JoinOperator wrapping left and right
//     d. Current operator becomes this join (for next iteration)
//
// Example query flow:
//
//	FROM users u JOIN orders o ON u.id = o.user_id JOIN products p ON o.product_id = p.id
//	→ (users JOIN orders) JOIN products
func (p *SelectPlan) applyJoinsIfNeeded(input iterator.DbIterator) (iterator.DbIterator, error) {
	joins := p.statement.Plan.Joins()
	if len(joins) == 0 {
		return input, nil
	}

	currentOp := input
	for _, joinNode := range joins {
		rightOp, err := p.buildJoinRightSide(joinNode)
		if err != nil {
			return nil, fmt.Errorf("failed to build right side of join: %w", err)
		}

		pred, err := p.buildJoinPredicate(joinNode, currentOp, rightOp)
		if err != nil {
			return nil, fmt.Errorf("failed to build join predicate: %w", err)
		}

		joinOp, err := join.NewJoinOperator(pred, currentOp, rightOp)
		if err != nil {
			return nil, fmt.Errorf("failed to create join operator: %w", err)
		}

		currentOp = joinOp
	}

	return currentOp, nil
}

// buildJoinRightSide creates a scan operator for the right side of a JOIN.
// Each join's right side is a fresh scan of a table (no filter optimization currently).
func (p *SelectPlan) buildJoinRightSide(joinNode *plan.JoinNode) (iterator.DbIterator, error) {
	table := joinNode.RightTable
	md, err := metadata.ResolveTableMetadata(table.TableName, p.tx, p.ctx)
	if err != nil {
		return nil, err
	}

	scanOp, err := scan.BuildScanWithFilter(p.tx, md.TableID, nil, p.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create scan for table %s: %w", table.TableName, err)
	}

	return scanOp, nil
}

// buildJoinPredicate constructs a JoinPredicate from the parsed JOIN ON clause.
// Maps field names to tuple positions in left and right operators.
//
// Process:
//  1. Resolve left field name to index in left operator's schema
//  2. Resolve right field name to index in right operator's schema
//  3. Convert parser predicate type to join predicate operation
//
// Example: ON users.id = orders.user_id
//
//	→ leftIndex=0 (users.id at position 0), rightIndex=2 (orders.user_id at position 2)
func (p *SelectPlan) buildJoinPredicate(node *plan.JoinNode, l, r iterator.DbIterator) (*join.JoinPredicate, error) {
	li, err := getJoinFieldIndex(node.LeftField, l.GetTupleDesc())
	if err != nil {
		return nil, err
	}

	ri, err := getJoinFieldIndex(node.RightField, r.GetTupleDesc())
	if err != nil {
		return nil, err
	}

	return join.NewJoinPredicate(li, ri, node.Predicate)
}

// applyAggregationIfNeeded applies aggregation/GROUP BY to the input operator.
// Only executes if the query contains aggregate functions (COUNT, SUM, AVG, MIN, MAX).
//
// Aggregation process:
//  1. Identify aggregation field from SELECT clause (e.g., COUNT(ID))
//  2. Identify GROUP BY field if present
//  3. Create AggregateOperator that:
//     - Groups tuples by GROUP BY field (or single group if no GROUP BY)
//     - Applies aggregate function to each group
//     - Outputs one tuple per group
//
// Example: SELECT dept, COUNT(id) FROM employees GROUP BY dept
//
//	→ Groups by dept field, counts id field per group
//
// Returns AggregateOperator, or input unchanged if no aggregation.
func (p *SelectPlan) applyAggregationIfNeeded(input iterator.DbIterator) (iterator.DbIterator, error) {
	if !p.statement.Plan.HasAgg() {
		return input, nil
	}

	td := input.GetTupleDesc()

	aggFieldName := extractFieldName(p.statement.Plan.AggField())
	aggFieldIndex, err := td.FindFieldIndex(aggFieldName)
	if err != nil {
		return nil, fmt.Errorf("aggregate field %s not found: %w", p.statement.Plan.AggField(), err)
	}

	groupByIndex := aggregation.NoGrouping
	if p.statement.Plan.GroupByField() != "" {
		groupByFieldName := extractFieldName(p.statement.Plan.GroupByField())
		groupByIndex, err = td.FindFieldIndex(groupByFieldName)
		if err != nil {
			return nil, fmt.Errorf("group by field %s not found: %w", p.statement.Plan.GroupByField(), err)
		}
	}

	aggOp, err := parseAggregateOp(p.statement.Plan.AggOp())
	if err != nil {
		return nil, err
	}

	aggOperator, err := aggregation.NewAggregateOperator(input, aggFieldIndex, groupByIndex, aggOp)
	if err != nil {
		return nil, fmt.Errorf("failed to create aggregate operator: %w", err)
	}

	return aggOperator, nil
}

// applyDistinctIfNeeded applies DISTINCT deduplication to the input operator.
// Only executes if the query specifies SELECT DISTINCT.
//
// DISTINCT process:
//  1. Wraps input in Distinct operator
//  2. Distinct operator uses hash-based TupleSet for deduplication
//  3. Hash collisions are properly handled via tuple comparison
//  4. Outputs only unique tuples from input stream
//
// Example: SELECT DISTINCT name FROM employees
//
//	→ Returns unique names only (duplicates removed)
//
// Returns Distinct operator wrapping input, or input unchanged if not DISTINCT.
func (p *SelectPlan) applyDistinctIfNeeded(input iterator.DbIterator) (iterator.DbIterator, error) {
	distinctOp, err := setops.NewDistinct(input)
	if err != nil {
		return nil, fmt.Errorf("failed to create distinct operator: %w", err)
	}

	return distinctOp, nil
}

// applySortIfNeeded applies ORDER BY sorting to the input operator.
// Only executes if the query specifies ORDER BY clause.
//
// Sorting process:
//  1. Wraps input in Sort operator
//  2. Sort operator materializes all tuples and sorts them in memory
//  3. Outputs tuples in sorted order (ASC or DESC)
//
// Example: SELECT * FROM employees ORDER BY age DESC
//
//	→ Returns employees sorted by age in descending order
//
// Returns Sort operator wrapping input, or input unchanged if no ORDER BY.
func (p *SelectPlan) applySortIfNeeded(input iterator.DbIterator) (iterator.DbIterator, error) {
	if !p.statement.Plan.HasOrderBy() {
		return input, nil
	}

	td := input.GetTupleDesc()
	orderByFieldName := extractFieldName(p.statement.Plan.OrderByField())

	orderByFieldIdx, err := td.FindFieldIndex(orderByFieldName)
	if err != nil {
		return nil, fmt.Errorf("order by field %s not found: %w", p.statement.Plan.OrderByField(), err)
	}

	sortOp, err := query.NewSort(input, orderByFieldIdx, p.statement.Plan.OrderByAsc())
	if err != nil {
		return nil, fmt.Errorf("failed to create sort operator: %w", err)
	}

	return sortOp, nil
}

// parseAggregateOp converts an aggregate operation string to AggregateOp enum.
func parseAggregateOp(opStr string) (aggregation.AggregateOp, error) {
	switch strings.ToUpper(opStr) {
	case "MIN":
		return aggregation.Min, nil
	case "MAX":
		return aggregation.Max, nil
	case "SUM":
		return aggregation.Sum, nil
	case "AVG":
		return aggregation.Avg, nil
	case "COUNT":
		return aggregation.Count, nil
	case "AND":
		return aggregation.And, nil
	case "OR":
		return aggregation.Or, nil
	default:
		return 0, fmt.Errorf("unsupported aggregate operation: %s", opStr)
	}
}

// getJoinFieldIndex resolves a join field name to its index in the tuple schema.
// Handles qualified names (table.field) by extracting just the field part.
func getJoinFieldIndex(fieldName string, td *tuple.TupleDescription) (int, error) {
	name := extractFieldName(fieldName)
	idx, err := td.FindFieldIndex(name)
	if err != nil {
		return -1, fmt.Errorf("join field %s not found: %w", fieldName, err)
	}

	return idx, nil
}

// extractFieldName extracts the field name from a qualified name.
// Handles both simple (field) and qualified (table.field) names.
func extractFieldName(qualifiedName string) string {
	parts := strings.Split(qualifiedName, ".")
	return parts[len(parts)-1]
}

// executeSetOperation handles execution of set operations (UNION, INTERSECT, EXCEPT).
// Recursively executes left and right SELECT statements, then applies the appropriate set operator.
//
// Process:
//  1. Execute left SELECT statement to get left iterator
//  2. Execute right SELECT statement to get right iterator
//  3. Create appropriate set operation operator (Union, Intersect, or Except)
//  4. Materialize and return results
func (p *SelectPlan) executeSetOperation() (result.Result, error) {
	leftStmt := statements.NewSelectStatement(p.statement.Plan.LeftPlan())
	rightStmt := statements.NewSelectStatement(p.statement.Plan.RightPlan())

	leftPlan := NewSelectPlan(leftStmt, p.tx, p.ctx)
	rightPlan := NewSelectPlan(rightStmt, p.tx, p.ctx)

	// Execute left side
	leftResult, err := leftPlan.Execute()
	if err != nil {
		return nil, fmt.Errorf("failed to execute left side of set operation: %v", err)
	}
	leftQueryResult := leftResult.(*result.SelectQueryResult)

	// Execute right side
	rightResult, err := rightPlan.Execute()
	if err != nil {
		return nil, fmt.Errorf("failed to execute right side of set operation: %v", err)
	}
	rightQueryResult := rightResult.(*result.SelectQueryResult)

	// Convert results to iterators
	leftIter := tuple.NewIteratorWithDesc(leftQueryResult.Tuples, leftQueryResult.TupleDesc)
	rightIter := tuple.NewIteratorWithDesc(rightQueryResult.Tuples, rightQueryResult.TupleDesc)

	// Create appropriate set operation operator
	var setOp iterator.DbIterator
	isAll := p.statement.Plan.SetOpAll()

	switch p.statement.Plan.SetOpType() {
	case plan.UnionOp:
		setOp, err = setops.NewUnion(leftIter, rightIter, isAll)
		if err != nil {
			return nil, fmt.Errorf("failed to create UNION operator: %v", err)
		}
	case plan.IntersectOp:
		setOp, err = setops.NewIntersect(leftIter, rightIter, isAll)
		if err != nil {
			return nil, fmt.Errorf("failed to create INTERSECT operator: %v", err)
		}
	case plan.ExceptOp:
		setOp, err = setops.NewExcept(leftIter, rightIter, isAll)
		if err != nil {
			return nil, fmt.Errorf("failed to create EXCEPT operator: %v", err)
		}
	default:
		return nil, fmt.Errorf("unsupported set operation type")
	}

	// Materialize results
	results, err := metadata.CollectAllTuples(setOp)
	if err != nil {
		return nil, err
	}

	return &result.SelectQueryResult{
		TupleDesc: setOp.GetTupleDesc(),
		Tuples:    results,
	}, nil
}
