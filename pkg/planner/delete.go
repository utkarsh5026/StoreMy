package planner

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
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
	tableID, err := resolveTableID(p.statement.TableName, p.ctx)
	if err != nil {
		return nil, err
	}

	query, err := buildScanWithFilter(p.tid, tableID, p.statement.WhereClause, p.ctx)
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

// deleteTuples performs the actual deletion of collected tuples.
func (p *DeletePlan) deleteTuples(tuplesToDelete []*tuple.Tuple) error {
	for i, tupleToDelete := range tuplesToDelete {
		if err := p.ctx.PageStore().DeleteTuple(p.tid, tupleToDelete); err != nil {
			return fmt.Errorf("failed to delete tuple %d: %v", i+1, err)
		}
	}
	return nil
}
