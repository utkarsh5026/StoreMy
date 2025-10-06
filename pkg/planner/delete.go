package planner

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/execution/query"
	"storemy/pkg/iterator"
	"storemy/pkg/parser/statements"
	"storemy/pkg/registry"
	"storemy/pkg/tuple"
)

// DeletePlan implements the execution plan for DELETE statements.
type DeletePlan struct {
	statement *statements.DeleteStatement
	ctx       *registry.DatabaseContext
	tid       *transaction.TransactionID
}

// NewDeletePlan creates a new DELETE execution plan
func NewDeletePlan(
	stmt *statements.DeleteStatement,
	tid *transaction.TransactionID,
	ctx *registry.DatabaseContext) *DeletePlan {
	return &DeletePlan{
		statement: stmt,
		tid:       tid,
		ctx:       ctx,
	}
}

// Execute performs the DELETE operation following these steps:
// 1. Resolve table name to table ID
// 2. Build query plan with optional WHERE filtering
// 3. Collect all tuples matching the criteria
// 4. Delete the collected tuples
func (p *DeletePlan) Execute() (any, error) {
	tableID, err := p.getTableID()
	if err != nil {
		return nil, err
	}

	query, err := p.createQuery(tableID)
	if err != nil {
		return nil, err
	}

	tuplesToDelete, err := collectAllTuples(query)
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

// getTableID resolves the table name from the DELETE statement to its internal table ID.
func (p *DeletePlan) getTableID() (int, error) {
	metadata, err := resolveTableMetadata(p.statement.TableName, p.ctx)
	if err != nil {
		return -1, err
	}
	return metadata.TableID, nil
}

// createTableScan creates a sequential scan iterator for the target table.
func (p *DeletePlan) createTableScan(tableID int) (iterator.DbIterator, error) {
	scanOp, err := query.NewSeqScan(p.tid, tableID, p.ctx.TableManager())
	if err != nil {
		return nil, fmt.Errorf("failed to create table scan: %v", err)
	}
	return scanOp, nil
}

// addWhereFilter wraps the scan iterator with a filter based on the WHERE clause.
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

// deleteTuples performs the actual deletion of collected tuples.
func (p *DeletePlan) deleteTuples(tuplesToDelete []*tuple.Tuple) error {
	for i, tupleToDelete := range tuplesToDelete {
		if err := p.ctx.PageStore().DeleteTuple(p.tid, tupleToDelete); err != nil {
			return fmt.Errorf("failed to delete tuple %d: %v", i+1, err)
		}
	}
	return nil
}

// createQuery builds the complete query execution plan for the DELETE operation.
// The plan consists of:
// - A sequential scan of the target table
// - An optional filter if WHERE clause is present
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
