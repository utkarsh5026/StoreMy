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
	tables := p.statement.Plan.Tables()
	if len(tables) == 0 {
		return nil, fmt.Errorf("SELECT requires at least one table in FROM clause")
	}

	firstTable := tables[0]
	metadata, err := resolveTableMetadata(firstTable.TableName, p.ctx)
	if err != nil {
		return nil, err
	}

	scanOp, err := query.NewSeqScan(p.tid, metadata.TableID, p.ctx.TableManager())
	if err != nil {
		return nil, fmt.Errorf("failed to create table scan: %v", err)
	}

	var currentOp iterator.DbIterator = scanOp
	filters := p.statement.Plan.Filters()
	if len(filters) > 0 {
		currentOp, err = buildScanWithFilter(p.tid, metadata.TableID, filters[0], p.ctx)
		if err != nil {
			return nil, err
		}
	} else {
		currentOp, err = buildScanWithFilter(p.tid, metadata.TableID, nil, p.ctx)
		if err != nil {
			return nil, err
		}
	}

	// If SELECT * is used, skip projection
	if !p.statement.Plan.SelectAll() {
		selectFields := p.statement.Plan.SelectList()
		if len(selectFields) > 0 {
			currentOp, err = p.buildProjection(currentOp, selectFields)
			if err != nil {
				return nil, err
			}
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
