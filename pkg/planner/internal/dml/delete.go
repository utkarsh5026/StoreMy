package dml

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/parser/statements"
	"storemy/pkg/planner/internal/result"
	"storemy/pkg/planner/internal/scan"
	"storemy/pkg/registry"
	"storemy/pkg/tuple"
)

// DeletePlan implements the execution plan for DELETE statements.
// It handles the deletion of rows from a table based on optional WHERE clause conditions.
//
// The execution follows a two-phase approach:
// 1. Query Phase: Identifies all tuples matching the WHERE clause (or all tuples if no WHERE clause)
// 2. Delete Phase: Removes the identified tuples from the table
//
// This approach ensures consistency by determining the full set of tuples to delete
// before performing any modifications.
type DeletePlan struct {
	statement *statements.DeleteStatement     // The parsed DELETE statement to execute
	ctx       *registry.DatabaseContext       // Database context providing access to catalog and storage managers
	tx        *transaction.TransactionContext // Transaction context for ensuring ACID properties
}

// NewDeletePlan creates a new DELETE execution plan.
//
// Parameters:
//   - stmt: The parsed DELETE statement containing table name and optional WHERE clause
//   - tx: Transaction context for coordinating the delete operation
//   - ctx: Database context providing access to system catalogs and storage
//
// Returns:
//   - A new DeletePlan instance ready for execution
func NewDeletePlan(
	stmt *statements.DeleteStatement,
	tx *transaction.TransactionContext,
	ctx *registry.DatabaseContext) *DeletePlan {
	return &DeletePlan{
		statement: stmt,
		tx:        tx,
		ctx:       ctx,
	}
}

// Execute performs the DELETE operation following these steps:
// 1. Resolve table name to table ID using the catalog
// 2. Build query plan with optional WHERE filtering
// 3. Collect all tuples matching the criteria
// 4. Delete the collected tuples from the table
//
// Returns:
//   - DMLResult containing the number of rows deleted
//   - error if any step fails (table not found, invalid WHERE clause, deletion failure, etc.)
func (p *DeletePlan) Execute() (result.Result, error) {
	tableID, err := resolveTableID(p.statement.TableName, p.tx, p.ctx)
	if err != nil {
		return nil, err
	}

	query, err := scan.BuildScanWithFilter(p.tx, tableID, p.statement.WhereClause, p.ctx)
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

	return &result.DMLResult{
		RowsAffected: len(tuplesToDelete),
		Message:      fmt.Sprintf("%d row(s) deleted", len(tuplesToDelete)),
	}, nil
}

// deleteTuples performs the actual deletion of collected tuples from the table.
// It iterates through each tuple and removes it using the tuple manager.
//
// Parameters:
//   - ts: Slice of tuples to delete
//   - tableID: ID of the table from which to delete tuples
//
// Returns:
//   - error if tuple deletion fails, including the index of the failed tuple
func (p *DeletePlan) deleteTuples(ts []*tuple.Tuple, tableID int) error {
	ctm := p.ctx.CatalogManager()
	tm := p.ctx.TupleManager()
	dbFile, err := ctm.GetTableFile(tableID)
	if err != nil {
		return err
	}

	for i, t := range ts {
		if err := tm.DeleteTuple(p.tx, dbFile, t); err != nil {
			return fmt.Errorf("failed to delete tuple %d: %v", i+1, err)
		}
	}
	return nil
}
