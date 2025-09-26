package planner

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/execution/query"
	"storemy/pkg/iterator"
	"storemy/pkg/memory"
	"storemy/pkg/parser/statements"
	"storemy/pkg/tuple"
)

type DeletePlan struct {
	statement    *statements.DeleteStatement
	pageStore    *memory.PageStore
	tableManager *memory.TableManager
	tid          *transaction.TransactionID
}

func NewDeletePlan(
	stmt *statements.DeleteStatement,
	ps *memory.PageStore,
	tid *transaction.TransactionID,
	tm *memory.TableManager) *DeletePlan {
	return &DeletePlan{
		statement:    stmt,
		pageStore:    ps,
		tableManager: tm,
		tid:          tid,
	}
}

func (p *DeletePlan) Execute() (any, error) {
	tableID, err := p.getTableID()
	if err != nil {
		return nil, err
	}

	query, err := p.createQuery(tableID)
	if err != nil {
		return nil, err
	}

	tuplesToDelete, err := p.collectTuplesToDelete(query)
	if err != nil {
		return nil, err
	}

	err = p.deleteTuples(tuplesToDelete)
	if err != nil {
		return nil, err
	}

	return &DMLResult{
		RowsAffected: len(tuplesToDelete),
		Message:      fmt.Sprintf("%d row(s) deleted", len(tuplesToDelete)),
	}, nil
}

func (p *DeletePlan) getTableID() (int, error) {
	tableName := p.statement.TableName
	tableID, err := p.tableManager.GetTableID(tableName)
	if err != nil {
		return 0, fmt.Errorf("table %s not found", tableName)
	}
	return tableID, nil
}

func (p *DeletePlan) createTableScan(tableID int) (iterator.DbIterator, error) {
	scanOp, err := query.NewSeqScan(p.tid, tableID, p.tableManager)
	if err != nil {
		return nil, fmt.Errorf("failed to create table scan: %v", err)
	}
	return scanOp, nil
}

func (p *DeletePlan) addWhereFilter(scanOp iterator.DbIterator) (iterator.DbIterator, error) {
	predicate, err := buildPredicateFromFilterNode(p.statement.WhereClause, scanOp.GetTupleDesc())
	if err != nil {
		return nil, fmt.Errorf("failed to build WHERE predicate: %v", err)
	}

	filterOp, err := query.NewFilter(predicate, scanOp)
	if err != nil {
		return nil, fmt.Errorf("failed to create filter: %v", err)
	}

	return filterOp, nil
}

func (p *DeletePlan) collectTuplesToDelete(queryPlan iterator.DbIterator) ([]*tuple.Tuple, error) {
	if err := queryPlan.Open(); err != nil {
		return nil, fmt.Errorf("failed to open delete query: %v", err)
	}
	defer queryPlan.Close()

	var tuplesToDelete []*tuple.Tuple

	for {
		hasNext, err := queryPlan.HasNext()
		if err != nil {
			return nil, fmt.Errorf("error during delete scan: %v", err)
		}

		if !hasNext {
			break
		}

		tuple, err := queryPlan.Next()
		if err != nil {
			return nil, fmt.Errorf("error fetching tuple to delete: %v", err)
		}

		tuplesToDelete = append(tuplesToDelete, tuple)
	}

	return tuplesToDelete, nil
}

func (p *DeletePlan) deleteTuples(tuplesToDelete []*tuple.Tuple) error {
	for i, tupleToDelete := range tuplesToDelete {
		if err := p.pageStore.DeleteTuple(p.tid, tupleToDelete); err != nil {
			return fmt.Errorf("failed to delete tuple %d: %v", i+1, err)
		}
	}
	return nil
}

func (p *DeletePlan) createQuery(tableID int) (iterator.DbIterator, error) {
	scanOp, err := p.createTableScan(tableID)
	if err != nil {
		return nil, err
	}

	var currentOp iterator.DbIterator
	currentOp = scanOp

	if p.statement.WhereClause != nil {
		filter, err := p.addWhereFilter(scanOp)
		if err != nil {
			return nil, err
		}
		currentOp = filter
	}

	return currentOp, nil
}
