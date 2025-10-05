package planner

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/execution/query"
	"storemy/pkg/iterator"
	"storemy/pkg/memory"
	"storemy/pkg/parser/statements"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

type QueryResult struct {
	TupleDesc *tuple.TupleDescription
	Tuples    []*tuple.Tuple
}

type SelectPlan struct {
	tableManager *memory.TableManager
	pageStore    *memory.PageStore
	tid          *transaction.TransactionID
	statement    *statements.SelectStatement
}

func NewSelectPlan(stmt *statements.SelectStatement, tm *memory.TableManager, ps *memory.PageStore, tid *transaction.TransactionID) *SelectPlan {
	return &SelectPlan{
		tableManager: tm,
		pageStore:    ps,
		tid:          tid,
		statement:    stmt,
	}
}

func (p *SelectPlan) Execute() (any, error) {
	tables := p.statement.Plan.Tables()
	if len(tables) == 0 {
		return nil, fmt.Errorf("SELECT requires at least one table in FROM clause")
	}

	firstTable := tables[0]
	tableID, err := p.tableManager.GetTableID(firstTable.TableName)
	if err != nil {
		return nil, fmt.Errorf("table %s not found", firstTable.TableName)
	}

	scanOp, err := query.NewSeqScan(p.tid, tableID, p.tableManager)
	if err != nil {
		return nil, fmt.Errorf("failed to create table scan: %v", err)
	}

	var currentOp iterator.DbIterator = scanOp
	filters := p.statement.Plan.Filters()
	if len(filters) > 0 {
		predicate, err := buildPredicateFromFilterNode(filters[0], scanOp.GetTupleDesc())
		if err != nil {
			return nil, fmt.Errorf("failed to build WHERE predicate: %v", err)
		}

		filterOp, err := query.NewFilter(predicate, currentOp)
		if err != nil {
			return nil, fmt.Errorf("failed to create filter: %v", err)
		}
		currentOp = filterOp
	}

	// If SELECT * is used, skip projection
	if !p.statement.Plan.SelectAll() {
		selectFields := p.statement.Plan.SelectList()
		if len(selectFields) > 0 {
			fieldIndices := make([]int, 0, len(selectFields))
			fieldTypes := make([]types.Type, 0, len(selectFields))

			tupleDesc := currentOp.GetTupleDesc()

			for _, field := range selectFields {
				found := false
				for i := 0; i < tupleDesc.NumFields(); i++ {
					name, _ := tupleDesc.GetFieldName(i)
					if name == field.FieldName {
						fieldIndices = append(fieldIndices, i)
						fieldType, _ := tupleDesc.TypeAtIndex(i)
						fieldTypes = append(fieldTypes, fieldType)
						found = true
						break
					}
				}
				if !found {
					return nil, fmt.Errorf("column %s not found", field.FieldName)
				}
			}

			projectOp, err := query.NewProject(fieldIndices, fieldTypes, currentOp)
			if err != nil {
				return nil, fmt.Errorf("failed to create projection: %v", err)
			}
			currentOp = projectOp
		}
	}

	if err := currentOp.Open(); err != nil {
		return nil, fmt.Errorf("failed to open query: %v", err)
	}
	defer currentOp.Close()

	results := make([]*tuple.Tuple, 0)
	for {
		hasNext, err := currentOp.HasNext()
		if err != nil {
			return nil, fmt.Errorf("error during query execution: %v", err)
		}
		if !hasNext {
			break
		}

		t, err := currentOp.Next()
		if err != nil {
			return nil, fmt.Errorf("error fetching tuple: %v", err)
		}
		results = append(results, t)
	}

	return &QueryResult{
		TupleDesc: currentOp.GetTupleDesc(),
		Tuples:    results,
	}, nil
}
