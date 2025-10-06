package planner

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/execution/aggregation"
	"storemy/pkg/execution/join"
	"storemy/pkg/execution/query"
	"storemy/pkg/iterator"
	"storemy/pkg/parser/plan"
	"storemy/pkg/parser/statements"
	"storemy/pkg/registry"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"strings"
)

type QueryResult struct {
	TupleDesc *tuple.TupleDescription
	Tuples    []*tuple.Tuple
}

type SelectPlan struct {
	ctx       *registry.DatabaseContext
	tid       *transaction.TransactionID
	statement *statements.SelectStatement
}

func NewSelectPlan(stmt *statements.SelectStatement, tid *transaction.TransactionID, ctx *registry.DatabaseContext) *SelectPlan {
	return &SelectPlan{
		ctx:       ctx,
		tid:       tid,
		statement: stmt,
	}
}

func (p *SelectPlan) Execute() (any, error) {
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

	results, err := collectAllTuples(currentOp)
	if err != nil {
		return nil, err
	}

	return &QueryResult{
		TupleDesc: currentOp.GetTupleDesc(),
		Tuples:    results,
	}, nil
}

// buildScanOperator creates the base scan operator with optional filters
func (p *SelectPlan) buildScanOperator() (iterator.DbIterator, error) {
	tables := p.statement.Plan.Tables()
	if len(tables) == 0 {
		return nil, fmt.Errorf("SELECT requires at least one table in FROM clause")
	}

	firstTable := tables[0]
	metadata, err := resolveTableMetadata(firstTable.TableName, p.ctx)
	if err != nil {
		return nil, err
	}

	var filter *plan.FilterNode
	filters := p.statement.Plan.Filters()
	if len(filters) > 0 {
		filter = filters[0]
	}

	scanOp, err := buildScanWithFilter(p.tid, metadata.TableID, filter, p.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create table scan: %w", err)
	}

	return scanOp, nil
}

func (p *SelectPlan) applyProjectionIfNeeded(input iterator.DbIterator) (iterator.DbIterator, error) {
	if p.statement.Plan.SelectAll() {
		return input, nil
	}

	selectFields := p.statement.Plan.SelectList()
	if len(selectFields) == 0 {
		return input, nil
	}

	projectionOp, err := p.buildProjection(input, selectFields)
	if err != nil {
		return nil, err
	}

	return projectionOp, nil
}

func (p *SelectPlan) buildProjection(input iterator.DbIterator, selectFields []*plan.SelectListNode) (iterator.DbIterator, error) {
	fieldIndices := make([]int, 0, len(selectFields))
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

// applyJoinsIfNeeded applies all join operations to the input operator
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

// buildJoinRightSide creates a scan operator for the right side of a join
func (p *SelectPlan) buildJoinRightSide(joinNode *plan.JoinNode) (iterator.DbIterator, error) {
	table := joinNode.RightTable
	md, err := resolveTableMetadata(table.TableName, p.ctx)
	if err != nil {
		return nil, err
	}

	scanOp, err := query.NewSeqScan(p.tid, md.TableID, p.ctx.TableManager())
	if err != nil {
		return nil, fmt.Errorf("failed to create scan for table %s: %w", table.TableName, err)
	}

	return scanOp, nil
}

// buildJoinPredicate constructs a JoinPredicate from a JoinNode
func (p *SelectPlan) buildJoinPredicate(joinNode *plan.JoinNode, leftOp, rightOp iterator.DbIterator) (*join.JoinPredicate, error) {
	leftTupleDesc := leftOp.GetTupleDesc()
	rightTupleDesc := rightOp.GetTupleDesc()

	leftFieldName := extractFieldName(joinNode.LeftField)
	leftFieldIndex, err := findFieldIndex(leftFieldName, leftTupleDesc)
	if err != nil {
		return nil, fmt.Errorf("left join field %s not found: %w", joinNode.LeftField, err)
	}

	rightFieldName := extractFieldName(joinNode.RightField)
	rightFieldIndex, err := findFieldIndex(rightFieldName, rightTupleDesc)
	if err != nil {
		return nil, fmt.Errorf("right join field %s not found: %w", joinNode.RightField, err)
	}

	predicateOp, err := getPredicateOperation(joinNode.Predicate)
	if err != nil {
		return nil, fmt.Errorf("failed to convert predicate: %w", err)
	}

	return join.NewJoinPredicate(leftFieldIndex, rightFieldIndex, predicateOp)
}

// extractFieldName extracts the field name from a qualified name (e.g., "table.field" -> "field")
func extractFieldName(qualifiedName string) string {
	parts := strings.Split(qualifiedName, ".")
	return parts[len(parts)-1]
}

// applyAggregationIfNeeded applies aggregation to the input operator if the query has aggregations
func (p *SelectPlan) applyAggregationIfNeeded(input iterator.DbIterator) (iterator.DbIterator, error) {
	if !p.statement.Plan.HasAgg() {
		return input, nil
	}

	tupleDesc := input.GetTupleDesc()

	aggFieldName := extractFieldName(p.statement.Plan.AggField())
	aggFieldIndex, err := findFieldIndex(aggFieldName, tupleDesc)
	if err != nil {
		return nil, fmt.Errorf("aggregate field %s not found: %w", p.statement.Plan.AggField(), err)
	}

	groupByIndex := aggregation.NoGrouping
	if p.statement.Plan.GroupByField() != "" {
		groupByFieldName := extractFieldName(p.statement.Plan.GroupByField())
		groupByIndex, err = findFieldIndex(groupByFieldName, tupleDesc)
		if err != nil {
			return nil, fmt.Errorf("group by field %s not found: %w", p.statement.Plan.GroupByField(), err)
		}
	}

	aggOp, err := p.parseAggregateOp(p.statement.Plan.AggOp())
	if err != nil {
		return nil, err
	}

	aggOperator, err := aggregation.NewAggregateOperator(input, aggFieldIndex, groupByIndex, aggOp)
	if err != nil {
		return nil, fmt.Errorf("failed to create aggregate operator: %w", err)
	}

	return aggOperator, nil
}

// parseAggregateOp converts an aggregate operation string to AggregateOp type
func (p *SelectPlan) parseAggregateOp(opStr string) (aggregation.AggregateOp, error) {
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
