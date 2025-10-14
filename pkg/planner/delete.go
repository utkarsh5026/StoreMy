package planner

import (
	"fmt"
	"storemy/pkg/parser/statements"
	"storemy/pkg/tuple"
)

// DeletePlan implements the execution plan for DELETE statements.
type DeletePlan struct {
	statement *statements.DeleteStatement
	ctx       DbContext
	tx        TransactionCtx
}

// NewDeletePlan creates a new DELETE execution plan
func NewDeletePlan(
	stmt *statements.DeleteStatement,
	tx TransactionCtx,
	ctx DbContext) *DeletePlan {
	return &DeletePlan{
		statement: stmt,
		tx:        tx,
		ctx:       ctx,
	}
}

// Execute performs the DELETE operation following these steps:
// 1. Resolve table name to table ID
// 2. Build query plan with optional WHERE filtering
// 3. Collect all tuples matching the criteria
// 4. Delete the collected tuples
func (p *DeletePlan) Execute() (any, error) {
	tableID, err := resolveTableID(p.statement.TableName, p.tx, p.ctx)
	if err != nil {
		return nil, err
	}

	query, err := buildScanWithFilter(p.tx, tableID, p.statement.WhereClause, p.ctx)
	if err != nil {
		return nil, err
	}

	tuplesToDelete, err := collectAllTuples(query)
	if err != nil {
		return nil, err
	}

	err = p.deleteTuples(tuplesToDelete, tableID)
	if err != nil {
		return nil, err
	}

	return &DMLResult{
		RowsAffected: len(tuplesToDelete),
		Message:      fmt.Sprintf("%d row(s) deleted", len(tuplesToDelete)),
	}, nil
}

// deleteTuples performs the actual deletion of collected tuples.
func (p *DeletePlan) deleteTuples(ts []*tuple.Tuple, tableID int) error {
	ctm := p.ctx.CatalogManager()
	tupleMgr := p.ctx.TupleManager()
	dbFile, err := ctm.GetTableFile(tableID)
	if err != nil {
		return err
	}

	for i, t := range ts {
		if err := tupleMgr.DeleteTuple(p.tx, dbFile, t); err != nil {
			return fmt.Errorf("failed to delete tuple %d: %v", i+1, err)
		}
	}
	return nil
}
