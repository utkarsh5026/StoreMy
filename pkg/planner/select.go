package planner

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/execution/query"
	"storemy/pkg/iterator"
	"storemy/pkg/parser/plan"
	"storemy/pkg/parser/statements"
	"storemy/pkg/registry"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
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

	currentOp, err = p.applyProjectionIfNeeded(currentOp)
	if err != nil {
		return nil, err
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
