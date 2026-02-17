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
	"storemy/pkg/primitives"
	"storemy/pkg/registry"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
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
//  9. Apply LIMIT/OFFSET (if specified)
//
// 10. Materialize all results via collectAllTuples()
func (p *SelectPlan) Execute() (result.Result, error) {
	if p.statement.Plan.IsSetOperation() {
		return p.executeSetOperation()
	}

	iter, err := p.ExecuteIterator()
	if err != nil {
		return nil, err
	}

	results, err := metadata.CollectAllTuples(iter)
	if err != nil {
		return nil, err
	}

	return &result.SelectQueryResult{
		TupleDesc: iter.GetTupleDesc(),
		Tuples:    results,
	}, nil
}

// ExecuteIterator builds the query operator tree and returns an iterator without materializing results.
// This allows streaming execution and avoids intermediate materialization.
//
// Execution flow (same as Execute but returns iterator instead of materialized results):
//  1. Build base scan with WHERE filter
//  2. Apply JOINs (if any)
//  3. Apply aggregation/GROUP BY (if any)
//  4. Apply projection/SELECT list (if no aggregation)
//  5. Apply DISTINCT (if specified and no aggregation)
//  6. Apply ORDER BY (if specified)
//  7. Apply LIMIT/OFFSET (if specified)
//
// Returns iterator.DbIterator ready to produce tuples on demand.
func (p *SelectPlan) ExecuteIterator() (iterator.DbIterator, error) {
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

	currentOp, err = p.applyLimitIfNeeded(currentOp)
	if err != nil {
		return nil, err
	}

	return currentOp, nil
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
	fieldIndices := make([]primitives.ColumnID, 0, len(selectFields))
	fieldTypes := make([]types.Type, 0, len(selectFields))
	tupleDesc := input.GetTupleDesc()

	for _, field := range selectFields {
		idx, err := findFieldIndex(field.FieldName, tupleDesc)
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

		li, ri, predOp, err := p.buildJoinPredicateFields(joinNode, currentOp, rightOp)
		if err != nil {
			return nil, fmt.Errorf("failed to build join predicate: %w", err)
		}

		joinOp, err := join.NewJoinOperator(li, ri, predOp, currentOp, rightOp)
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

// buildJoinPredicateFields extracts join predicate fields from the parsed JOIN ON clause.
// Maps field names to tuple positions in left and right operators.
//
// Process:
//  1. Resolve left field name to index in left operator's schema
//  2. Resolve right field name to index in right operator's schema
//  3. Return field indexes and predicate operation for join operator
//
// Example: ON users.id = orders.user_id
//
//	→ leftIndex=0 (users.id at position 0), rightIndex=2 (orders.user_id at position 2)
func (p *SelectPlan) buildJoinPredicateFields(node *plan.JoinNode, l, r iterator.DbIterator) (leftIdx, rightIdx primitives.ColumnID, pred primitives.Predicate, err error) {
	li, err := findFieldIndex(node.LeftField, l.GetTupleDesc())
	if err != nil {
		return 0, 0, 0, err
	}

	ri, err := findFieldIndex(node.RightField, r.GetTupleDesc())
	if err != nil {
		return 0, 0, 0, err
	}

	return li, ri, node.Predicate, nil
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
	pl := p.statement.Plan
	if !pl.HasAgg() {
		return input, nil
	}

	aggFieldIndex, err := p.parseAggregationIndex(input.GetTupleDesc())
	if err != nil {
		return nil, err
	}

	groupIndex := aggregation.NoGrouping
	if pl.GroupByField() != "" {
		groupIndex, err = findFieldIndex(pl.GroupByField(), input.GetTupleDesc())
		if err != nil {
			return nil, fmt.Errorf("group by field %s not found: %w", p.statement.Plan.GroupByField(), err)
		}
	}

	aggOp, err := aggregation.ParseAggregateOp(pl.AggOp())
	if err != nil {
		return nil, err
	}

	aggOperator, err := aggregation.NewAggregateOperator(input, aggFieldIndex, groupIndex, aggOp)
	if err != nil {
		return nil, fmt.Errorf("failed to create aggregate operator: %w", err)
	}

	return aggOperator, nil
}

func (p *SelectPlan) parseAggregationIndex(td *tuple.TupleDescription) (primitives.ColumnID, error) {
	aggFieldName := extractFieldName(p.statement.Plan.AggField())
	var aggFieldIndex primitives.ColumnID
	var err error

	if aggFieldName == "*" {
		if td.NumFields() == 0 {
			return 0, fmt.Errorf("cannot perform COUNT(*) on table with no fields")
		}
		aggFieldIndex = 0
	} else {
		aggFieldIndex, err = td.FindFieldIndex(aggFieldName)
		if err != nil {
			return 0, fmt.Errorf("aggregate field %s not found: %w", p.statement.Plan.AggField(), err)
		}
	}

	return aggFieldIndex, nil
}

// applyDistinctIfNeeded applies DISTINCT deduplication to the input operator.
// Only executes if the query specifies SELECT DISTINCT.
//
// Example: SELECT DISTINCT name FROM employees
//
//	→ Returns unique names only (duplicates removed)
//
// Returns Distinct operator wrapping input, or input unchanged if not DISTINCT.
func (p *SelectPlan) applyDistinctIfNeeded(input iterator.DbIterator) (iterator.DbIterator, error) {
	dnt, err := setops.NewDistinct(input)
	if err != nil {
		return nil, fmt.Errorf("failed to create distinct operator: %w", err)
	}

	return dnt, nil
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
	plan := p.statement.Plan
	if !plan.HasOrderBy() {
		return input, nil
	}

	fieldIdx, err := findFieldIndex(plan.OrderByField(), input.GetTupleDesc())
	if err != nil {
		return nil, fmt.Errorf("order by field %s not found: %w", plan.OrderByField(), err)
	}

	sortOp, err := query.NewSort(input, fieldIdx, plan.OrderByAsc())
	if err != nil {
		return nil, fmt.Errorf("failed to create sort operator: %w", err)
	}

	return sortOp, nil
}

// applyLimitIfNeeded applies LIMIT/OFFSET to the input operator.
// Only executes if the query specifies LIMIT clause.
//
// LIMIT/OFFSET process:
//  1. Wraps input in Limit operator
//  2. Limit operator skips first OFFSET tuples
//  3. Then returns at most LIMIT tuples
//  4. Remaining tuples are discarded
//
// Example: SELECT * FROM employees LIMIT 10 OFFSET 5
//
//	→ Skips first 5 rows, returns next 10 rows
//
// Returns Limit operator wrapping input, or input unchanged if no LIMIT.
func (p *SelectPlan) applyLimitIfNeeded(input iterator.DbIterator) (iterator.DbIterator, error) {
	plan := p.statement.Plan
	if !plan.HasLimit() {
		return input, nil
	}

	lm, err := query.NewLimitOperator(input, plan.Limit(), plan.Offset())
	if err != nil {
		return nil, fmt.Errorf("failed to create limit operator: %w", err)
	}

	return lm, nil
}

// executeSetOperation handles execution of set operations (UNION, INTERSECT, EXCEPT).
// Uses ExecuteIterator to stream results without intermediate materialization.
//
// Process:
//  1. Get left iterator directly (no materialization)
//  2. Get right iterator directly (no materialization)
//  3. Create appropriate set operation operator (Union, Intersect, or Except)
//  4. Materialize final results once
//
// This approach avoids the wasteful Iterator → Array → Iterator → Array conversion.
func (p *SelectPlan) executeSetOperation() (result.Result, error) {
	pl := p.statement.Plan

	leftIter, err := p.createPlanIter(pl.LeftPlan())
	if err != nil {
		return nil, fmt.Errorf("failed to build left side iterator: %v", err)
	}

	rightIter, err := p.createPlanIter(pl.RightPlan())
	if err != nil {
		return nil, fmt.Errorf("failed to build right side iterator: %v", err)
	}

	setOp, err := p.createSetOp(leftIter, rightIter)
	if err != nil {
		return nil, fmt.Errorf("failed to create set operation iterator: %v", err)
	}

	results, err := metadata.CollectAllTuples(setOp)
	if err != nil {
		return nil, err
	}

	return &result.SelectQueryResult{
		TupleDesc: setOp.GetTupleDesc(),
		Tuples:    results,
	}, nil
}

func (p *SelectPlan) createPlanIter(pl *plan.SelectPlan) (iterator.DbIterator, error) {
	stmt := statements.NewSelectStatement(pl)
	plan := NewSelectPlan(stmt, p.tx, p.ctx)

	iter, err := plan.ExecuteIterator()
	if err != nil {
		return nil, fmt.Errorf("failed to build iterator: %v", err)
	}

	return iter, nil
}

func (p *SelectPlan) createSetOp(l, r iterator.DbIterator) (iterator.DbIterator, error) {
	var setOp iterator.DbIterator
	var err error

	isAll := p.statement.Plan.SetOpAll()
	switch p.statement.Plan.SetOpType() {
	case plan.UnionOp:
		setOp, err = setops.NewUnion(l, r, isAll)
		if err != nil {
			return nil, fmt.Errorf("failed to create UNION operator: %v", err)
		}
	case plan.IntersectOp:
		setOp, err = setops.NewIntersect(l, r, isAll)
		if err != nil {
			return nil, fmt.Errorf("failed to create INTERSECT operator: %v", err)
		}
	case plan.ExceptOp:
		setOp, err = setops.NewExcept(l, r, isAll)
		if err != nil {
			return nil, fmt.Errorf("failed to create EXCEPT operator: %v", err)
		}
	default:
		return nil, fmt.Errorf("unsupported set operation type")
	}

	return setOp, nil
}
